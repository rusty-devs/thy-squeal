# TODO - Implementation Tasks

See [MVP-ARCHITECTURE.md](./MVP-ARCHITECTURE.md) for recommended architecture changes to reach MVP.

## Feature Specifications
See [docs/features/](./features/) for detailed specifications.

| Feature | Status | Priority |
|---------|--------|----------|
| SQL Parser (Pest) | ✅ Implemented | High |
| Storage Engine | ✅ Implemented | High |
| HTTP API | 🚧 Partial | High |
| CLI Client | ✅ Implemented | Medium |
| JS REPL | ❌ Not Started | Low |
| TCP Protocol | ❌ Not Started | Medium |

---

## Phase 1: Foundation (v0.1)
See: [sql-parser.md](./features/sql-parser.md), [tcp-protocol.md](./features/tcp-protocol.md)

### SQL Parser
- [x] Simple SQL parser for SELECT (using Pest grammar)
- [x] INSERT support
- [x] CREATE TABLE, DROP TABLE
- [x] UPDATE support
- [x] DELETE support
- [x] Wire Pest grammar (sql.pest) into executor
- [x] Decompose parser into modular submodules

### Storage
- [x] In-memory table struct
- [x] Row storage (Vec)
- [x] Basic CRUD operations (insert, select, update, delete at Table level)
- [x] Row ID generation (UUID)
- [x] WHERE clause filtering (implemented via evaluate_condition)
- [x] Decompose storage into modular submodules

### HTTP Server
- [x] Set up Axum on port 9200
- [x] POST /_query endpoint
- [x] GET /, GET /health
- [ ] Add more REST endpoints (/_stats, CRUD)

### Milestone v0.1: Foundation
- [x] Wired Pest parser into the Executor
- [x] Implemented WHERE, UPDATE, DELETE
- [x] Structured Error Handling (SqlError enum with JSON mapping)
- [x] Unit tests for SQL operations
- [x] Integration tests (end-to-end via HTTP)
- [x] REPL executes SQL over HTTP
- [x] Modularized codebase for maintainability

### Current Status
- Server running on http://localhost:9200
- SQL execution works via POST /_query
- CREATE TABLE, DROP TABLE, INSERT, SELECT, UPDATE, DELETE supported
- WHERE clause support with basic operators
- ORDER BY and LIMIT/OFFSET support
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY and HAVING support
- Column aliases
- Structured errors returned in JSON response

**Milestone v0.1**: Basic SQL server running; Full SQL CRUD with filtering, sorting, and grouping.

## Phase 2: HTTP API (v0.2)
See: [http-api.md](./features/http-api.md)

### HTTP Server
- [ ] GET /_stats - Storage/cache statistics
- [ ] Implement REST CRUD endpoints

### Security
- [ ] Basic Authentication
- [ ] API Key support
- [ ] TLS support

## Phase 3: Advanced SQL (v0.3)
See: [sql-parser.md](./features/sql-parser.md)

### Query Features
- [x] Add JOIN support (INNER, LEFT)
- [x] Add WHERE clause operators
- [x] Add ORDER BY
- [x] Add LIMIT/OFFSET
- [x] Add DISTINCT
- [x] Add column aliases

### Aggregations
- [x] Add COUNT, SUM, AVG, MIN, MAX
- [x] Add GROUP BY
- [x] Add HAVING

## Phase 4: Search & KV (v0.4)
See: [full-text-search.md](./features/full-text-search.md), [key-value-store.md](./features/key-value-store.md)

- [ ] Full-text search engine
- [ ] Redis-compatible KV commands
- [ ] Table-level caching

## Phase 5: Client (v0.5)
See: [js-repl-client.md](./features/js-repl-client.md)

### REPL
- [x] Integrate rustyline
- [x] Add history (arrow keys)
- [x] Add `.help`, `.quit` / `.exit`
- [x] Wire SQL input to HTTP client execution
- [ ] Add tab completion

### JavaScript Runtime
- [ ] Integrate quickjs-rs
- [ ] Add `conn.query(sql)` bridge to JS

## Phase 6: Production (v1.0)

- [ ] Persistence: snapshot to disk (RDB-like)
- [ ] WAL (Write Ahead Log) for durability
- [ ] Distributed mode (Raft consensus)
