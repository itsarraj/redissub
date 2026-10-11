use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use regex::Regex;
use std::collections::HashMap;
use std::process;

use redissub::{
    matches_filter, OutputFormat, RedisConnection,
};

#[derive(Parser, Debug)]
#[command(
    name = "redissub",
    about = "Modern CLI stream processor and event harvester for Redis Pub/Sub, Keyspace Notifications, and Streams",
    version
)]
pub struct Cli {
    /// Redis host to connect to
    #[arg(short = 'H', long, default_value = "127.0.0.1", global = true)]
    pub host: String,

    /// Redis port to connect to
    #[arg(short = 'p', long, default_value_t = 6379, global = true)]
    pub port: u16,

    /// Optional Redis password (AUTH)
    #[arg(short = 'a', long, global = true)]
    pub auth: Option<String>,

    /// Optional Redis username (ACL AUTH)
    #[arg(short = 'u', long, global = true)]
    pub user: Option<String>,

    /// Redis database index (SELECT)
    #[arg(short = 'd', long, global = true)]
    pub db: Option<u32>,

    /// Output format: pretty, json, raw
    #[arg(short = 'f', long, default_value = "pretty", global = true)]
    pub format: OutputFormat,

    /// Extract field from JSON payload using dot notation (e.g. data.user.id)
    #[arg(short = 'e', long, global = true)]
    pub extract: Option<String>,

    /// Filter payload matching regex pattern
    #[arg(long, global = true)]
    pub filter: Option<String>,

    /// Filter payload containing substring
    #[arg(long, global = true)]
    pub contains: Option<String>,

    /// Stop after receiving N messages
    #[arg(short = 'c', long, global = true)]
    pub count: Option<usize>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Subscribe to one or more Redis Pub/Sub channels
    Sub {
        /// Channel names to subscribe to
        #[arg(required = true)]
        channels: Vec<String>,
    },

    /// Pattern subscribe to Redis Pub/Sub channels (glob patterns like 'events.*')
    Psub {
        /// Patterns to subscribe to
        #[arg(required = true)]
        patterns: Vec<String>,
    },

    /// Listen to Redis Keyspace / Keyevent notifications
    Keyspace {
        /// Target database index for keyspace events (default: 0)
        #[arg(long, default_value_t = 0)]
        db: u32,

        /// Event types to listen to (e.g., "expired,set,del" or "*" for all)
        #[arg(short = 'E', long, default_value = "*")]
        events: String,

        /// Specific key to observe
        #[arg(short = 'k', long)]
        key: Option<String>,

        /// Run CONFIG SET notify-keyspace-events KEA on server before listening
        #[arg(long)]
        enable_config: bool,
    },

    /// Tail Redis Streams using XREAD BLOCK
    Stream {
        /// Redis stream key names
        #[arg(required = true)]
        streams: Vec<String>,

        /// Stream ID offsets (defaults to "$" for new messages only, or "0-0" for all from beginning)
        #[arg(long, default_value = "$")]
        id: String,

        /// Block timeout in milliseconds (0 for indefinite block)
        #[arg(long, default_value_t = 5000)]
        block: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);

    let regex_filter = if let Some(ref pat) = cli.filter {
        Some(Regex::new(pat).context("Invalid regex pattern in --filter")?)
    } else {
        None
    };
    let contains_filter = cli.contains.as_deref();
    let extract_path = cli.extract.as_deref();

    let mut conn = match RedisConnection::connect(
        &addr,
        cli.auth.as_deref(),
        cli.user.as_deref(),
        cli.db,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{} Failed to connect to {}: {}", "error:".bold().red(), addr, e);
            process::exit(1);
        }
    };

    let mut received_count = 0usize;

    match cli.command {
        Commands::Sub { channels } => {
            if cli.format == OutputFormat::Pretty {
                eprintln!(
                    "{} Connected to {} | Subscribed to: {}",
                    "✓".bold().green(),
                    addr.cyan(),
                    channels.join(", ").yellow()
                );
            }
            conn.subscribe(&channels)
                .await
                .context("Failed to subscribe to channels")?;

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                    res = conn.read_pubsub_event() => {
                        match res {
                            Ok(event) => {
                                if !matches_filter(&event.payload, regex_filter.as_ref(), contains_filter) {
                                    continue;
                                }
                                println!("{}", event.format(cli.format, extract_path));
                                received_count += 1;
                                if let Some(max) = cli.count {
                                    if received_count >= max {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("{} {}", "error:".bold().red(), e);
                                break;
                            }
                        }
                    }
                }
            }
        }
        Commands::Psub { patterns } => {
            if cli.format == OutputFormat::Pretty {
                eprintln!(
                    "{} Connected to {} | Pattern-subscribed to: {}",
                    "✓".bold().green(),
                    addr.cyan(),
                    patterns.join(", ").yellow()
                );
            }
            conn.psubscribe(&patterns)
                .await
                .context("Failed to pattern-subscribe")?;

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                    res = conn.read_pubsub_event() => {
                        match res {
                            Ok(event) => {
                                if !matches_filter(&event.payload, regex_filter.as_ref(), contains_filter) {
                                    continue;
                                }
                                println!("{}", event.format(cli.format, extract_path));
                                received_count += 1;
                                if let Some(max) = cli.count {
                                    if received_count >= max {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("{} {}", "error:".bold().red(), e);
                                break;
                            }
                        }
                    }
                }
            }
        }
        Commands::Keyspace {
            db,
            events,
            key,
            enable_config,
        } => {
            if enable_config {
                if let Err(e) = conn.enable_keyspace_events().await {
                    eprintln!(
                        "{} Could not set CONFIG notify-keyspace-events: {}",
                        "warn:".bold().yellow(),
                        e
                    );
                } else if cli.format == OutputFormat::Pretty {
                    eprintln!("{} Configured Redis notify-keyspace-events = KEA", "✓".bold().green());
                }
            }

            let mut targets = Vec::new();
            let is_pattern;

            if let Some(target_key) = key {
                targets.push(format!("__keyspace@{}__:{}", db, target_key));
                is_pattern = false;
            } else if events == "*" {
                targets.push(format!("__keyevent@{}__:*", db));
                is_pattern = true;
            } else {
                for ev in events.split(',') {
                    let ev_clean = ev.trim();
                    if !ev_clean.is_empty() {
                        targets.push(format!("__keyevent@{}__:{}", db, ev_clean));
                    }
                }
                is_pattern = false;
            }

            if cli.format == OutputFormat::Pretty {
                eprintln!(
                    "{} Connected to {} | Listening to keyspace targets: {}",
                    "✓".bold().green(),
                    addr.cyan(),
                    targets.join(", ").yellow()
                );
            }

            if is_pattern {
                conn.psubscribe(&targets)
                    .await
                    .context("Failed to psubscribe to keyspace events")?;
            } else {
                conn.subscribe(&targets)
                    .await
                    .context("Failed to subscribe to keyspace events")?;
            }

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                    res = conn.read_pubsub_event() => {
                        match res {
                            Ok(mut event) => {
                                event.kind = "keyspace".into();
                                if !matches_filter(&event.payload, regex_filter.as_ref(), contains_filter) {
                                    continue;
                                }
                                println!("{}", event.format(cli.format, extract_path));
                                received_count += 1;
                                if let Some(max) = cli.count {
                                    if received_count >= max {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("{} {}", "error:".bold().red(), e);
                                break;
                            }
                        }
                    }
                }
            }
        }
        Commands::Stream {
            streams,
            id,
            block,
        } => {
            if cli.format == OutputFormat::Pretty {
                eprintln!(
                    "{} Connected to {} | Tailing streams: {} (offset: {})",
                    "✓".bold().green(),
                    addr.cyan(),
                    streams.join(", ").yellow(),
                    id.dimmed()
                );
            }

            // Map each stream to its latest id
            let mut stream_ids: HashMap<String, String> = HashMap::new();
            for s in &streams {
                stream_ids.insert(s.clone(), id.clone());
            }

            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                    res = async {
                        let current_ids: Vec<String> = streams
                            .iter()
                            .map(|s| stream_ids.get(s).cloned().unwrap_or_else(|| "$".to_string()))
                            .collect();
                        conn.xread(&streams, &current_ids, block).await
                    } => {
                        match res {
                            Ok(entries) => {
                                for entry in entries {
                                    if let (Some(ref st), Some(ref entry_id)) = (&entry.stream, &entry.id) {
                                        stream_ids.insert(st.clone(), entry_id.clone());
                                    }
                                    if !matches_filter(&entry.payload, regex_filter.as_ref(), contains_filter) {
                                        continue;
                                    }
                                    println!("{}", entry.format(cli.format, extract_path));
                                    received_count += 1;
                                    if let Some(max) = cli.count {
                                        if received_count >= max {
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("{} {}", "error:".bold().red(), e);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
