# TODO - Implementation Tasks

See [MVP-ARCHITECTURE.md](./MVP-ARCHITECTURE.md) for recommended architecture changes to reach MVP.

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

- [x] Set up workspace with Cargo workspace
- [x] Create server binary (`thy-squeal`)
- [x] Create client binary (`thy-squeal-client`)
- [x] Set up logging with `tracing`
- [x] Add config loading (YAML)

### Current Status
- Server runs on HTTP port 9200 (Axum)
- Client REPL exists (rustyline); SQL execution in REPL wired to HTTP API
- Config loads from `thy-squeal.yaml`
- GET /, GET /health, POST /_query, CORS working

## Phase 1: Foundation (v0.1)
See: [sql-parser.md](./features/sql-parser.md), [tcp-protocol.md](./features/tcp-protocol.md)

### SQL Parser
- [x] Simple SQL parser for SELECT (using Pest grammar)
- [x] INSERT support
- [x] CREATE TABLE, DROP TABLE
- [x] UPDATE support
- [x] DELETE support
- [x] Wire Pest grammar (sql.pest) into executor

### Storage
- [x] In-memory table struct
- [x] Row storage (Vec)
- [x] Basic CRUD operations (insert, select, update, delete at Table level)
- [x] Row ID generation (UUID)
- [x] WHERE clause filtering (implemented via evaluate_condition)

### HTTP Server
- [x] Set up Axum on port 9200
- [x] POST /_query endpoint
- [x] GET /, GET /health
- [ ] Add more REST endpoints (/_stats, CRUD)

### Current Status
- Server running on http://localhost:9200
- SQL execution works via POST /_query
- CREATE TABLE, DROP TABLE, INSERT, SELECT, UPDATE, DELETE supported
- WHERE clause support with basic operators (=, !=, >, <, >=, <=, LIKE, IS NULL)

**Milestone v0.1**: Basic SQL server running; SELECT/INSERT/UPDATE/DELETE/CREATE/DROP work with WHERE clause support.

## Phase 2: HTTP API (v0.2)
See: [http-api.md](./features/http-api.md)

### HTTP Server
- [x] Set up Axum on port 9200
- [x] Add CORS middleware
- [x] Create health endpoint (`/health`)
- [x] Add server info endpoint (`/`)
- [ ] GET `/_stats` - Storage/cache statistics

### REST Endpoints
- [x] POST `/_query` - Execute SQL
- [ ] GET `/<db>/<table>` - List rows
- [ ] GET `/<db>/<table>/<id>` - Get row
- [ ] POST `/<db>/<table>` - Insert row
- [ ] PUT `/<db>/<table>/<id>` - Update row
- [ ] DELETE `/<db>/<table>/<id>` - Delete row

**Milestone v0.2**: HTTP JSON API working; CRUD endpoints and /_stats pending

## Phase 3: Advanced SQL (v0.3)
See: [sql-parser.md](./features/sql-parser.md)

### Query Features
- [ ] Add JOIN support (INNER, LEFT)
- [x] Add WHERE clause operators (=, !=, >, <, >=, <=, LIKE, IS NULL, IS NOT NULL)
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
- [x] Set up Clap for argument parsing
- [x] Add `-e` / `--execute` flag
- [x] Add `-h` / `--host` flag
- [x] Add `--http` flag for HTTP mode
- [ ] Add `--export` / `--import`
- [ ] Wire `Query` subcommand to execute SQL

### REPL
- [x] Integrate rustyline
- [x] Add history (arrow keys)
- [x] Add `.help`, `.quit` / `.exit`
- [x] Wire SQL input to HTTP client execution
- [ ] Add tab completion
- [ ] Add `.load` command

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
