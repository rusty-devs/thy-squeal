# thy-squeal PRD - Product Requirements Document

## 1. Project Overview

### Project Name
**thy-squeal** - A lightweight SQL server with HTTP JSON API and Redis-like capabilities

### Project Type
Distributed in-memory database with SQL and HTTP interfaces

### Core Feature Summary
A MySQL-compatible SQL server with dual-protocol support (SQL over TCP + HTTP JSON API), featuring full-text search, dynamic caching, and Redis-like key-value operations. Includes an interactive JavaScript REPL client.

### Target Users
- Developers needing a lightweight embedded database
- Applications requiring flexible search (like Elasticsearch)
- Teams wanting Redis-like key-value storage with SQL querying
- Systems needing unified SQL + HTTP + KV access patterns

---

## 2. Architecture Overview

### Binary Distribution
| Binary | Port | Purpose |
|--------|------|---------|
| `thy-squeal` | 3306 (SQL), 9200 (HTTP) | Server daemon |
| `thy-squeal-client` | CLI | Interactive JS REPL + CLI tool |

### Component Diagram
```
┌─────────────────────────────────────────────────────────┐
│                    thy-squeal (Server)                  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ SQL Parser  │  │ HTTP API    │  │ KV Store        │  │
│  │ (Pest)      │  │ (Axum)      │  │ (DashMap)       │  │
│  └──────┬──────┘  └──────┬──────┘  └────────┬────────┘  │
│         │                │                  │           │
│         └────────────────┼──────────────────┘           │
│                          ▼                              │
│               ┌─────────────────────┐                   │
│               │   Storage Engine    │                   │
│               │  (In-Memory Cache)  │                   │
│               └─────────────────────┘                   │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│              thy-squeal-client (CLI)                    │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │ SQL Client  │  │ HTTP Client │  │ JS Runtime      │  │
│  │ (TCP)       │  │ (reqwest)   │  │ (QuickJS)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Functional Requirements

### 3.1 SQL Server ( thy-squeal )

#### 3.1.1 SQL Dialect
- MySQL-compatible syntax (simplified subset)
- Support for: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX
- JOINs: INNER JOIN, LEFT JOIN (no RIGHT JOIN initially)
- Subqueries in WHERE clauses
- Aggregations: COUNT, SUM, AVG, MIN, MAX with GROUP BY
- ORDER BY and LIMIT/OFFSET
- DISTINCT keyword
- Aliases for tables and columns

#### 3.1.2 Data Types

| Type | Aliases | Description |
|------|---------|-------------|
| `INT` | `INTEGER`, `SMALLINT`, `BIGINT` | Signed integer (32-bit default) |
| `FLOAT` | `DOUBLE`, `REAL` | Floating point |
| `BOOL` | `BOOLEAN` | True/False |
| `DATE` | - | Date (YYYY-MM-DD) |
| `DATETIME` | - | DateTime (YYYY-MM-DD HH:MM:SS) |
| `VARCHAR(n)` | `TEXT`, `STRING` | Variable-length string |
| `BLOB` | `BINARY`, `BYTEA` | Binary data |
| `JSON` | `JSONB` | JSON object/array |

#### 3.1.3 Full-Text Search
- CREATE FULLTEXT INDEX on VARCHAR columns
- MATCH AGAINST syntax: `SELECT * FROM t WHERE MATCH(col) AGAINST('query')`
- Support for boolean operators: +, -, *, ", "
- Rank results by relevance
- Index tokenization (whitespace, punctuation)

#### 3.1.4 In-Memory Cache Configuration
- Per-table cache size configuration (row count or memory in MB)
- Cache eviction policy: LRU, LFU, FIFO (configurable)
- Dynamic cache resize at runtime via SQL or HTTP API
- View caching with materialization options
- Cache statistics exposed via HTTP endpoint

```sql
-- Cache configuration examples
CREATE TABLE users (...) WITH (cache_size = 10000, eviction = 'LRU');
ALTER TABLE users SET cache_size = 50000;
CREATE VIEW user_stats AS SELECT ... WITH (cache_ttl = 300);
```

#### 3.1.5 Key-Value Storage (Redis-like)
- Direct KV operations via SQL:
  ```sql
  SET @key = 'value';
  GET @key;
  DEL @key;
  SET @key = 'value' EX 60;  -- TTL in seconds
  INCR @counter;
  HSET @hash field value;
  HGET @hash field;
  ```
- Key patterns: `strings:`, `hashes:`, `lists:`, `sets:` namespaces
- Pub/Sub support for real-time notifications
- Persistence: optional snapshot to disk (RDB-like)

### 3.2 HTTP JSON API (Elasticsearch-style)

#### 3.2.1 REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Server info |
| GET | `/health` | Health check |
| GET | `/_stats` | Storage statistics |
| POST | `/_query` | Execute SQL via HTTP |
| GET | `/<database>/_search` | Full-text search |
| GET | `/<database>/<table>` | List rows |
| GET | `/<database>/<table>/<id>` | Get row by ID |
| POST | `/<database>/<table>` | Insert row |
| PUT | `/<database>/<table>/<id>` | Update row |
| DELETE | `/<database>/<table>/<id>` | Delete row |
| GET | `/kv/<key>` | Get KV value |
| PUT | `/kv/<key>` | Set KV value |
| DELETE | `/kv/<key>` | Delete KV value |
| GET | `/kv` | List KV keys (with pattern) |

#### 3.2.2 Search API
```json
POST /mydb/users/_search
{
  "query": "john",
  "fields": ["name", "email"],
  "from": 0,
  "size": 10,
  "highlight": true
}
```

### 3.3 Client ( thy-squeal-client )

#### 3.3.1 Connection Modes
- TCP SQL connection (MySQL wire protocol subset)
- HTTP connection (REST API)
- URI format: `thy-sql://host:port` or `thy-http://host:port`

#### 3.3.2 JavaScript REPL
- Embedded QuickJS runtime
- Native client API exposed as JS objects:
  ```javascript
  const client = require('thy-squeal');
  const conn = client.connect('thy-sql://localhost:3306');

  // SQL queries
  const result = conn.query('SELECT * FROM users WHERE age > ?', [18]);

  // KV operations
  client.kv.set('session:123', { user: 'john' });
  const session = client.kv.get('session:123');

  // Full-text search
  const hits = conn.search('users', 'john doe');
  ```
- Multi-line input support
- Tab completion for SQL keywords and table names
- History (arrow keys)
- Load scripts from files: `.load script.js`

#### 3.3.3 CLI Commands
```bash
# Connect and run SQL
thy-squeal-client -h localhost -p 3306 -e "SELECT * FROM users"

# HTTP mode
thy-squeal-client --http localhost:9200 -e "SELECT * FROM users"

# Interactive REPL
thy-squeal-client

# Execute script file
thy-squeal-client script.js

# Import/export
thy-squeal-client --export data.json
thy-squeal-client --import data.json
```

---

## 4. Non-Functional Requirements

### 4.1 Performance
- Query latency: < 10ms for simple SELECT on cached tables
- HTTP API response: < 50ms (p99)
- KV operations: < 1ms for basic ops
- Support for 1M+ rows in memory (depending on available RAM)

### 4.2 Scalability
- Single-node initially (no clustering in v1)
- Configurable memory limits per table
- Connection pooling for HTTP clients

### 4.3 Reliability
- Graceful shutdown (save caches to disk)
- Crash recovery from persistent KV snapshots
- Request timeouts (configurable)

### 4.4 Security
- Optional authentication (username/password)
- TLS support for connections
- SQL injection prevention (parameterized queries)

---

## 5. Configuration

### Server Configuration (thy-squeal.yaml)
```yaml
server:
  host: "0.0.0.0"
  sql_port: 3306
  http_port: 9200

storage:
  max_memory_mb: 4096
  default_cache_size: 10000
  default_eviction: "LRU"
  snapshot_interval_sec: 300
  data_dir: "./data"

security:
  auth_enabled: false
  tls_enabled: false

logging:
  level: "info"
```

### Client Configuration (~/.thy-squeal/config.yaml)
```yaml
connection:
  default_host: "localhost"
  default_port: 3306

repl:
  history_size: 1000
  auto_indent: true
```

---

## 6. Recommended Libraries

### Server Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `pest` | ^2.0 | PEG parser generator (already in use) |
| `pest_derive` | ^2.0 | Pest derive macro |
| `axum` | ^0.7 | HTTP server (recommended over actix-web) |
| `tokio` | ^1.0 | Async runtime |
| `dashmap` | ^5.0 | Concurrent HashMap for KV store |
| `sled` | ^0.34 | Embedded KV DB with persistence |
| `tantivy` | ^0.22 | Full-text search engine |
| `moka` | ^0.12 | Cache with LRU/LFU/expire |
| `serde` | ^1.0 | Serialization framework |
| `serde_json` | ^1.0 | JSON serialization |
| `chrono` | ^0.4 | DateTime handling |
| `tracing` | ^0.1 | Structured logging |
| `thiserror` | ^1.0 | Error handling |
| `anyhow` | ^1.0 | Context-aware error handling |
| `uuid` | ^1.0 | UUID generation |
| `async-trait` | ^0.1 | Async trait support |
| `tower` | ^0.4 | HTTP middleware |
| `tower-http` | ^0.5 | HTTP utilities (CORS, etc.) |

### Client Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `reqwest` | ^0.12 | HTTP client |
| `quickjs-rs` | ^0.5 | QuickJS JavaScript runtime |
| `boa` | ^0.19 | Alternative: Pure Rust JS engine |
| `tokio` | ^1.0 | Async runtime |
| `serde` | ^1.0 | Serialization |
| `serde_json` | ^1.0 | JSON handling |
| `tracing` | ^0.1 | Logging |
| `clap` | ^4.0 | CLI argument parsing |
| `rustyline` | ^14.0 | REPL line editing |
| `nucleotide` | ^0.2 | Alternative: Rust REPL library |

### Serialization

| Crate | Version | Purpose |
|-------|---------|---------|
| `serde` | ^1.0 | Serialization framework |
| `serde_json` | ^1.0 | JSON |
| `serde_yaml` | ^0.9 | YAML config parsing |
| `bincode` | ^1.3 | Binary serialization |

---

## 7. File Structure

```
thy-squeal/                          # Cargo workspace
├── Cargo.toml                       # Workspace config
├── thy-squeal.yaml.example          # Server config template
├── server/                          # Server crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                  # Server entry (Axum HTTP)
│       ├── config.rs                # YAML config loading
│       └── sql.pest                 # SQL grammar
├── client/                          # Client crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                  # Client CLI (Clap)
│       ├── config.rs                # Client config
│       ├── http.rs                  # HTTP client
│       └── repl.rs                  # REPL (rustyline)
├── docs/
│   ├── PRD.md
│   ├── TODO.md
│   └── features/
│       └── *.md
├── examples/
│   └── *.sql
└── LICENSE, README.md
```

### Current Status
- [x] Workspace setup
- [x] Server binary with Axum HTTP on port 9200
- [x] Client binary with REPL
- [x] YAML config loading

---

## 8. Phases

### Phase 1: Foundation (v0.1)
- [x] Set up workspace with Cargo workspace
- [x] Server binary with Axum HTTP (port 9200)
- [x] Client binary with REPL
- [x] Basic SQL parser (SELECT, INSERT, CREATE TABLE, DROP TABLE)
- [x] In-memory table storage
- [ ] TCP server (SQL protocol)

### Phase 2: HTTP API (v0.2)
- [x] HTTP JSON API (basic Axum server running)
- [x] POST /_query endpoint
- [ ] CRUD endpoints for tables (REST)
- [ ] GET /health, /_stats endpoints

### Phase 3: Advanced SQL (v0.3)
- [ ] JOINs, aggregations, GROUP BY
- [ ] UPDATE, DELETE support
- [ ] WHERE clause filtering
- [ ] Indexes

### Phase 4: Search & KV (v0.4)
- [ ] Full-text search
- [ ] Key-value store
- [ ] Redis-like commands

### Phase 5: Client (v0.5)
- [x] thy-squeal-client CLI
- [x] REPL with rustyline
- [ ] JavaScript REPL (QuickJS)

### Phase 6: Production (v1.0)
- [ ] Authentication
- [ ] TLS
- [ ] Persistence
- [ ] Performance tuning
