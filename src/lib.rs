pub mod resp;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use colored::*;
use regex::Regex;
use resp::{decode_resp, encode_command, RespValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Pretty,
    Json,
    Raw,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pretty" => Ok(OutputFormat::Pretty),
            "json" | "ndjson" => Ok(OutputFormat::Json),
            "raw" => Ok(OutputFormat::Raw),
            other => Err(format!("Unknown output format: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEvent {
    pub timestamp: String,
    pub kind: String, // "message", "pmessage", "stream", "keyspace"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub payload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<HashMap<String, String>>,
}

impl MessageEvent {
    pub fn new_pubsub(channel: String, payload: String) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            kind: "message".into(),
            channel: Some(channel),
            pattern: None,
            stream: None,
            id: None,
            payload,
            fields: None,
        }
    }

    pub fn new_pmessage(pattern: String, channel: String, payload: String) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            kind: "pmessage".into(),
            channel: Some(channel),
            pattern: Some(pattern),
            stream: None,
            id: None,
            payload,
            fields: None,
        }
    }

    pub fn new_keyspace(channel: String, key_or_event: String) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339(),
            kind: "keyspace".into(),
            channel: Some(channel),
            pattern: None,
            stream: None,
            id: None,
            payload: key_or_event,
            fields: None,
        }
    }

    pub fn new_stream_entry(
        stream: String,
        id: String,
        fields_map: HashMap<String, String>,
    ) -> Self {
        let payload = serde_json::to_string(&fields_map).unwrap_or_default();
        Self {
            timestamp: Utc::now().to_rfc3339(),
            kind: "stream".into(),
            channel: None,
            pattern: None,
            stream: Some(stream),
            id: Some(id),
            payload,
            fields: Some(fields_map),
        }
    }

    pub fn format(
        &self,
        format: OutputFormat,
        extract_path: Option<&str>,
    ) -> String {
        let display_payload = if let Some(path) = extract_path {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&self.payload) {
                extract_json_path(&v, path).unwrap_or_else(|| self.payload.clone())
            } else {
                self.payload.clone()
            }
        } else {
            self.payload.clone()
        };

        match format {
            OutputFormat::Json => {
                let mut copy = self.clone();
                copy.payload = display_payload;
                serde_json::to_string(&copy).unwrap_or_default()
            }
            OutputFormat::Raw => display_payload,
            OutputFormat::Pretty => {
                let ts = self.timestamp.dimmed();
                let kind_tag = match self.kind.as_str() {
                    "message" => "PUB/SUB".bold().green(),
                    "pmessage" => "P-SUB".bold().cyan(),
                    "keyspace" => "KEYSPACE".bold().yellow(),
                    "stream" => "STREAM".bold().magenta(),
                    _ => self.kind.bold().normal(),
                };

                let target = if let Some(ref ch) = self.channel {
                    if let Some(ref pat) = self.pattern {
                        format!("{} (pattern {})", ch.bold().blue(), pat.dimmed())
                    } else {
                        ch.bold().blue().to_string()
                    }
                } else if let Some(ref st) = self.stream {
                    if let Some(ref id) = self.id {
                        format!("{} [{}]", st.bold().blue(), id.dimmed())
                    } else {
                        st.bold().blue().to_string()
                    }
                } else {
                    "".to_string()
                };

                format!("[{}] [{}] {} -> {}", ts, kind_tag, target, display_payload)
            }
        }
    }
}

/// JSON Dot-Path extractor supporting nested keys and array index (`foo.bar.0.id`)
pub fn extract_json_path(value: &serde_json::Value, path: &str) -> Option<String> {
    let mut current = value;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        if let Ok(idx) = segment.parse::<usize>() {
            if let Some(arr) = current.as_array() {
                current = arr.get(idx)?;
                continue;
            }
        }
        current = current.get(segment)?;
    }

    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

pub struct RedisConnection {
    stream: TcpStream,
    read_buf: Vec<u8>,
}

impl RedisConnection {
    pub async fn connect(
        addr: &str,
        password: Option<&str>,
        username: Option<&str>,
        db: Option<u32>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("Failed to connect to Redis at {}", addr))?;

        let mut conn = Self {
            stream,
            read_buf: Vec::with_capacity(8192),
        };

        if let Some(pass) = password {
            if let Some(user) = username {
                conn.send_command(&["AUTH", user, pass]).await?;
            } else {
                conn.send_command(&["AUTH", pass]).await?;
            }
            let resp = conn.read_frame().await?;
            if let RespValue::Error(err) = resp {
                return Err(anyhow!("Redis AUTH failed: {}", err));
            }
        }

        if let Some(db_num) = db {
            let db_str = db_num.to_string();
            conn.send_command(&["SELECT", &db_str]).await?;
            let resp = conn.read_frame().await?;
            if let RespValue::Error(err) = resp {
                return Err(anyhow!("Redis SELECT {} failed: {}", db_num, err));
            }
        }

        Ok(conn)
    }

    pub async fn send_command<T: AsRef<[u8]>>(&mut self, args: &[T]) -> Result<()> {
        let encoded = encode_command(args);
        self.stream
            .write_all(&encoded)
            .await
            .context("Failed to write command to Redis stream")?;
        self.stream.flush().await.context("Failed to flush command")?;
        Ok(())
    }

    pub async fn read_frame(&mut self) -> Result<RespValue> {
        loop {
            if !self.read_buf.is_empty() {
                match decode_resp(&self.read_buf)? {
                    Some((val, consumed)) => {
                        self.read_buf.drain(..consumed);
                        return Ok(val);
                    }
                    None => {
                        // Incomplete frame, need more bytes
                    }
                }
            }

            let mut chunk = [0u8; 4096];
            let n = self
                .stream
                .read(&mut chunk)
                .await
                .context("Failed to read from Redis socket")?;
            if n == 0 {
                return Err(anyhow!("Redis server closed connection unexpectedly"));
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
    }

    pub async fn subscribe(&mut self, channels: &[String]) -> Result<()> {
        let mut args: Vec<&str> = Vec::with_capacity(channels.len() + 1);
        args.push("SUBSCRIBE");
        for ch in channels {
            args.push(ch.as_str());
        }
        self.send_command(&args).await
    }

    pub async fn psubscribe(&mut self, patterns: &[String]) -> Result<()> {
        let mut args: Vec<&str> = Vec::with_capacity(patterns.len() + 1);
        args.push("PSUBSCRIBE");
        for pat in patterns {
            args.push(pat.as_str());
        }
        self.send_command(&args).await
    }

    pub async fn read_pubsub_event(&mut self) -> Result<MessageEvent> {
        loop {
            let frame = self.read_frame().await?;
            if let RespValue::Array(Some(arr)) = frame {
                if arr.is_empty() {
                    continue;
                }
                let kind = arr[0].as_str().unwrap_or("");
                match kind {
                    "message" => {
                        if arr.len() >= 3 {
                            let channel = arr[1].as_str().unwrap_or("").to_string();
                            let payload = arr[2].as_str().unwrap_or("").to_string();
                            return Ok(MessageEvent::new_pubsub(channel, payload));
                        }
                    }
                    "pmessage" => {
                        if arr.len() >= 4 {
                            let pattern = arr[1].as_str().unwrap_or("").to_string();
                            let channel = arr[2].as_str().unwrap_or("").to_string();
                            let payload = arr[3].as_str().unwrap_or("").to_string();
                            return Ok(MessageEvent::new_pmessage(pattern, channel, payload));
                        }
                    }
                    "subscribe" | "psubscribe" | "unsubscribe" | "punsubscribe" => {
                        // Status confirmation frames, ignore and wait for message
                        continue;
                    }
                    _ => {}
                }
            } else if let RespValue::Error(err) = frame {
                return Err(anyhow!("Redis error: {}", err));
            }
        }
    }

    pub async fn enable_keyspace_events(&mut self) -> Result<()> {
        self.send_command(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]).await?;
        let resp = self.read_frame().await?;
        if let RespValue::Error(err) = resp {
            return Err(anyhow!("Failed to enable keyspace events via CONFIG SET: {}", err));
        }
        Ok(())
    }

    /// Run XREAD BLOCK <block_ms> STREAMS <streams...> <ids...>
    pub async fn xread(
        &mut self,
        streams: &[String],
        ids: &[String],
        block_ms: u64,
    ) -> Result<Vec<MessageEvent>> {
        let block_str = block_ms.to_string();
        let mut cmd: Vec<&str> = vec!["XREAD", "BLOCK", &block_str, "STREAMS"];
        for s in streams {
            cmd.push(s.as_str());
        }
        for id in ids {
            cmd.push(id.as_str());
        }

        self.send_command(&cmd).await?;
        let resp = self.read_frame().await?;

        let mut events = Vec::new();

        match resp {
            RespValue::Array(None) => {
                // Timeout / nil array
                return Ok(events);
            }
            RespValue::Array(Some(streams_arr)) => {
                // streams_arr is an array of [stream_name, entries]
                for stream_item in streams_arr {
                    if let RespValue::Array(Some(pair)) = stream_item {
                        if pair.len() >= 2 {
                            let stream_name = pair[0].as_str().unwrap_or("").to_string();
                            if let RespValue::Array(Some(entries)) = &pair[1] {
                                for entry in entries {
                                    if let RespValue::Array(Some(entry_pair)) = entry {
                                        if entry_pair.len() >= 2 {
                                            let id = entry_pair[0].as_str().unwrap_or("").to_string();
                                            let mut fields_map = HashMap::new();
                                            if let RespValue::Array(Some(kv_pairs)) = &entry_pair[1] {
                                                let mut i = 0;
                                                while i + 1 < kv_pairs.len() {
                                                    let k = kv_pairs[i].as_str().unwrap_or("").to_string();
                                                    let v = kv_pairs[i + 1].as_str().unwrap_or("").to_string();
                                                    fields_map.insert(k, v);
                                                    i += 2;
                                                }
                                            }
                                            events.push(MessageEvent::new_stream_entry(
                                                stream_name.clone(),
                                                id,
                                                fields_map,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            RespValue::Error(err) => return Err(anyhow!("XREAD failed: {}", err)),
            other => return Err(anyhow!("Unexpected XREAD response: {}", other)),
        }

        Ok(events)
    }
}

pub fn matches_filter(payload: &str, regex: Option<&Regex>, contains: Option<&str>) -> bool {
    if let Some(r) = regex {
        if !r.is_match(payload) {
            return false;
        }
    }
    if let Some(sub) = contains {
        if !payload.contains(sub) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_path() {
        let json_str = r#"{"user":{"id":123,"profile":{"name":"Alice"}},"tags":["rust","systems"]}"#;
        let val: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(extract_json_path(&val, "user.id").unwrap(), "123");
        assert_eq!(extract_json_path(&val, "user.profile.name").unwrap(), "Alice");
        assert_eq!(extract_json_path(&val, "tags.0").unwrap(), "rust");
        assert_eq!(extract_json_path(&val, "tags.1").unwrap(), "systems");
        assert_eq!(extract_json_path(&val, "user.nonexistent"), None);
    }

    #[test]
    fn test_filter_matching() {
        let re = Regex::new(r"error|fail").unwrap();
        assert!(matches_filter("system error occurred", Some(&re), None));
        assert!(!matches_filter("system success", Some(&re), None));
        assert!(matches_filter("payment processed", None, Some("payment")));
        assert!(!matches_filter("login attempt", None, Some("payment")));
    }

    #[test]
    fn test_format_output() {
        let evt = MessageEvent::new_pubsub("orders".into(), r#"{"id":42}"#.into());
        let raw = evt.format(OutputFormat::Raw, None);
        assert_eq!(raw, r#"{"id":42}"#);

        let extracted = evt.format(OutputFormat::Raw, Some("id"));
        assert_eq!(extracted, "42");

        let json = evt.format(OutputFormat::Json, None);
        assert!(json.contains(r#""channel":"orders""#));
    }
}
