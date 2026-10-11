# redissub

A lightweight, zero-dependency async CLI stream processor and event harvester for Redis Pub/Sub channels, Keyspace Notifications (`__keyevent@<db>__`), and Redis Streams (`XREAD BLOCK`).

Status: Verified against Redis RESP v2/v3 mock socket servers; supports real-time Pub/Sub subscription (`SUBSCRIBE`), glob pattern subscription (`PSUBSCRIBE`), keyspace event tapping (`KEA` notifications), and continuous stream tailing (`XREAD BLOCK`). SSL/TLS wrapping deferred to `redissub` v0.2.

---

## Features

- **Direct Async RESP Engine**: Built-in, high-performance Redis Serialization Protocol (RESP v2/v3) parser and command encoder with zero heavy external Redis library dependencies.
- **Pub/Sub Streaming**: Subscribe to one or multiple standard Redis channels (`sub channel1 channel2`).
- **Pattern Subscriptions**: Wildcard and glob pattern event harvesting (`psub "orders.*"`).
- **Keyspace / Keyevent Watcher**: Automated listening on Redis database lifecycle events (`expired`, `set`, `del`, etc.) with optional server-side configuration (`--enable-config`).
- **Redis Streams Consumer**: Real-time tailing of Redis Streams with automatic ID tracking and configurable blocking timeout (`stream mystream --id $ --block 5000`).
- **Real-Time Extraction & Filtering**:
  - Extract deeply nested fields from JSON payloads via dot-notation (`-e user.profile.id`).
  - Filter payloads by regular expression (`--filter "^CRITICAL"`) or substring (`--contains "timeout"`).
- **Multiple Output Formats**:
  - `pretty`: Colorized, human-readable console feed with timestamps and channel tags.
  - `json` / `ndjson`: Structured JSON stream suited for piping into `jq`, ClickHouse, Vector, or Kafka.
  - `raw`: Emits only the bare message payload per line.
- **Bounded Ingestion**: Graceful termination after receiving $N$ messages (`-c 100`).

---

## Installation

```bash
cargo install --path .
```

Or build the release binary directly:

```bash
cargo build --release
cp target/release/redissub /usr/local/bin/
```

---

## Usage

### 1. Basic Pub/Sub

```bash
# Listen to live orders and telemetry channels
redissub -H 127.0.0.1 -p 6379 sub orders telemetry

# Pipe raw JSON payloads directly into jq
redissub -f raw sub sensor_readings | jq '.temperature'
```

### 2. Pattern Matching

```bash
# Tap into all regional user events
redissub psub "users.*.events"
```

### 3. Keyspace & Key Expiration Notifications

```bash
# Listen to all key expiration events on DB 0
redissub keyspace --db 0 --events "expired"

# Automatically enable keyspace events on Redis server and monitor key sets & deletes
redissub keyspace --enable-config --events "set,del"
```

### 4. Redis Streams Tailing

```bash
# Tail a Redis stream from current time onwards
redissub stream orders_stream --id $ --block 5000

# Replay and stream all historical entries from beginning
redissub -f json stream orders_stream --id 0-0
```

### 5. Filtering and Extraction

```bash
# Filter JSON messages containing "error" and extract the error code
redissub --filter "error" -e error.code -f raw sub payment_pipeline
```

---

## Command Line Options

```text
Usage: redissub [OPTIONS] <COMMAND>

Commands:
  sub       Subscribe to one or more Redis Pub/Sub channels
  psub      Pattern subscribe to Redis Pub/Sub channels (glob patterns like 'events.*')
  keyspace  Listen to Redis Keyspace / Keyevent notifications
  stream    Tail Redis Streams using XREAD BLOCK
  help      Print this message or the help of the given subcommand(s)

Options:
  -H, --host <HOST>          Redis host to connect to [default: 127.0.0.1]
  -p, --port <PORT>          Redis port to connect to [default: 6379]
  -a, --auth <AUTH>          Optional Redis password (AUTH)
  -u, --user <USER>          Optional Redis username (ACL AUTH)
  -d, --db <DB>              Redis database index (SELECT)
  -f, --format <FORMAT>      Output format: pretty, json, raw [default: pretty]
  -e, --extract <EXTRACT>    Extract field from JSON payload using dot notation
      --filter <FILTER>      Filter payload matching regex pattern
      --contains <CONTAINS>  Filter payload containing substring
  -c, --count <COUNT>        Stop after receiving N messages
  -h, --help                 Print help
  -V, --version              Print version
```

---

## License

MIT (c) 2026 Animesh Raj. See [LICENSE](LICENSE) for details.
