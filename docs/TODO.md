# TODO - Implementation Tasks

## Feature Specifications
See [docs/features/](./features/) for detailed specifications.

| Feature | File |
|---------|------|
| SQL Parser | [features/sql-parser.md](./features/sql-parser.md) |
| TCP Protocol | [features/tcp-protocol.md](./features/tcp-protocol.md) |
| HTTP API | [features/http-api.md](./features/http-api.md) |
| Key-Value Store | [features/key-value-store.md](./features/key-value-store.md) |
| Full-Text Search | [features/full-text-search.md](./features/full-text-search.md) |
| JS REPL Client | [features/js-repl-client.md](./features/js-repl-client.md) |
| Table Caching | [features/table-caching.md](./features/table-caching.md) |
| Authentication | [features/authentication.md](./features/authentication.md) |

## Project Setup

- [ ] Set up workspace with Cargo workspace
- [ ] Create server binary (`thy-squeal`)
- [ ] Create client binary (`thy-squeal-client`)
- [ ] Set up logging with `tracing`
- [ ] Add config loading (YAML)

## Phase 1: Foundation (v0.1)
See: [sql-parser.md](./features/sql-parser.md), [tcp-protocol.md](./features/tcp-protocol.md)

### SQL Parser
- [ ] Extend Pest grammar for SELECT
- [ ] Add INSERT support
- [ ] Add UPDATE support
- [ ] Add DELETE support
- [ ] Implement parser AST

### Storage
- [ ] Create in-memory table struct
- [ ] Implement row storage (Vec/HashMap)
- [ ] Add basic CRUD operations
- [ ] Add row ID generation

### TCP Server
- [ ] Set up Tokio runtime
- [ ] Create TCP listener on port 3306
- [ ] Implement simple wire protocol
- [ ] Handle client connections

**Milestone v0.1**: Basic SQL server running, can execute SELECT/INSERT/UPDATE/DELETE

## Phase 2: HTTP API (v0.2)
See: [http-api.md](./features/http-api.md)

### HTTP Server
- [ ] Set up Axum on port 9200
- [ ] Add CORS middleware
- [ ] Create health endpoint (`/health`)
- [ ] Add server info endpoint (`/`)

### REST Endpoints
- [ ] POST `/_query` - Execute SQL
- [ ] GET `/<db>/<table>` - List rows
- [ ] GET `/<db>/<table>/<id>` - Get row
- [ ] POST `/<db>/<table>` - Insert row
- [ ] PUT `/<db>/<table>/<id>` - Update row
- [ ] DELETE `/<db>/<table>/<id>` - Delete row

**Milestone v0.2**: HTTP JSON API working alongside TCP SQL

## Phase 3: Advanced SQL (v0.3)
See: [sql-parser.md](./features/sql-parser.md)

### Query Features
- [ ] Add JOIN support (INNER, LEFT)
- [ ] Add WHERE clause operators (=, !=, >, <, >=, <=, LIKE, IN, BETWEEN, IS NULL)
- [ ] Add ORDER BY
- [ ] Add LIMIT/OFFSET
- [ ] Add DISTINCT
- [ ] Add column aliases

### Aggregations
- [ ] Add COUNT, SUM, AVG, MIN, MAX
- [ ] Add GROUP BY
- [ ] Add HAVING

### Schema
- [ ] Add CREATE TABLE
- [ ] Add DROP TABLE
- [ ] Add column type validation
- [ ] Add DESCRIBE TABLE
- [ ] Implement indexes (B-tree)
- [ ] Add EXPLAIN query plan

**Milestone v0.3**: Full SQL query capabilities

## Phase 4: Search & KV (v0.4)
See: [key-value-store.md](./features/key-value-store.md), [full-text-search.md](./features/full-text-search.md)

### Key-Value Store
- [ ] Implement DashMap-based KV storage
- [ ] Add SET/GET/DEL commands
- [ ] Add TTL support (EX option)
- [ ] Add INCR/DECR
- [ ] Add HSET/HGET/HDEL (hashes)
- [ ] Add LPUSH/RPUSH/LPOP/RPOP (lists)
- [ ] Add SADD/SMEMBERS (sets)

### Full-Text Search
- [ ] Integrate Tantivy
- [ ] Add CREATE FULLTEXT INDEX
- [ ] Implement MATCH AGAINST syntax
- [ ] Add search ranking
- [ ] Add highlighting

### HTTP KV Endpoints
- [ ] GET `/kv/<key>`
- [ ] PUT `/kv/<key>`
- [ ] DELETE `/kv/<key>`
- [ ] GET `/kv` - List with pattern

**Milestone v0.4**: Redis-like KV and Elasticsearch-like search

## Phase 5: Client (v0.5)
See: [js-repl-client.md](./features/js-repl-client.md)

### CLI
- [ ] Set up Clap for argument parsing
- [ ] Add `-e` / `--execute` flag
- [ ] Add `-h` / `--host` flag
- [ ] Add `--http` flag for HTTP mode
- [ ] Add `--export` / `--import`

### REPL
- [ ] Integrate rustyline
- [ ] Add history (arrow keys)
- [ ] Add tab completion
- [ ] Add `.load` command
- [ ] Add `.help` command

### JavaScript Runtime
- [ ] Integrate quickjs-rs
- [ ] Expose JS API for connections
- [ ] Add `client.query()` function
- [ ] Add `client.kv` namespace
- [ ] Add `conn.search()` function
- [ ] Support multi-line input

**Milestone v0.5**: Full client with JS REPL

## Phase 6: Production (v1.0)
See: [authentication.md](./features/authentication.md), [table-caching.md](./features/table-caching.md)

### Security
- [ ] Add authentication (username/password)
- [ ] Add TLS support
- [ ] Implement parameterized queries (SQL injection prevention)

### Reliability
- [ ] Add graceful shutdown
- [ ] Implement KV persistence (sled)
- [ ] Add snapshot/restore
- [ ] Add request timeouts

### Caching
- [ ] Implement configurable cache per table
- [ ] Add LRU eviction
- [ ] Add LFU eviction
- [ ] Add FIFO eviction
- [ ] Expose cache stats via HTTP

### Observability
- [ ] Add metrics endpoint
- [ ] Add query logging
- [ ] Add performance profiling

**Milestone v1.0**: Production-ready release

---

## Backlog (Future)

- [ ] Clustering support
- [ ] RIGHT JOIN
- [ ] Subqueries in SELECT
- [ ] Window functions
- [ ] Transactions
- [ ] Views with materialization
- [ ] Pub/Sub
- [ ] GraphQL API
- [ ] Protobuf protocol
