# thy-squeal PRD - Product Requirements Document

## 1. Project Overview

### Project Name
**thy-squeal** - A lightweight SQL server with HTTP JSON API and Redis-like capabilities

### Project Type
Distributed in-memory database with SQL and HTTP interfaces

### Core Feature Summary
A MySQL-compatible SQL server with dual-protocol support (SQL over TCP + HTTP JSON API), featuring full-text search, dynamic caching, and Redis-like key-value operations. Includes an interactive JavaScript REPL client.

---

## 2. Architecture Overview

### Binary Distribution
| Binary | Port | Purpose |
|--------|------|---------|
| `thy-squeal` | 3306 (SQL), 9200 (HTTP) | Server daemon |
| `thy-squeal-client` | CLI | Interactive JS REPL + CLI tool |

---

## 3. Functional Requirements

### 3.1 SQL Server ( thy-squeal )

#### 3.1.1 SQL Dialect
- MySQL-compatible syntax (simplified subset)
- Support for: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE
- Transactions: BEGIN, COMMIT, ROLLBACK (Pending)
- JOINs: INNER JOIN, LEFT JOIN
- Aggregations: COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
- ORDER BY and LIMIT/OFFSET
- Aliases for columns
- Subqueries: Correlated and non-correlated (IN clause)
- **EXPLAIN**: Query execution plan visualization
- **Data Export/Import**: `.dump` and `.restore` commands (SQL script format)
- **Information Schema**: Metadata querying (tables, columns, statistics)

#### 3.1.2 Performance & Reliability
- **Indexes**: B-Tree or Hash indexes for fast lookups
- **Explain Plan**: Visualizing query execution strategy
- **Write-Ahead Logging (WAL)**: Guaranteed durability for every write
- **Schema Evolution**: `ALTER TABLE` support for non-destructive schema changes

---

## 7. File Structure

```
thy-squeal/                          # Cargo workspace
├── Cargo.toml                       # Workspace config
├── server/                          # Server crate
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                  # Server entry (Axum HTTP, /, /health, /_query)
│       ├── config.rs                # YAML config loading
│       ├── storage/                 # Modular storage engine
│       │   ├── mod.rs               # Database struct
│       │   ├── table.rs             # Table, Column, Row
│       │   ├── value.rs             # Value enum & impls
│       │   ├── types.rs             # DataType enum
│       │   └── error.rs             # StorageError
│       ├── sql/                     # SQL engine
│       │   ├── mod.rs               # SQL module entry
│       │   ├── ast.rs               # Abstract Syntax Tree
│       │   ├── eval.rs              # Expression/Condition evaluator
│       │   ├── error.rs             # SqlError enum
│       │   ├── parser/              # Pest-based parser (modular)
│       │   └── executor/            # SQL statement execution
│       └── sql.pest                 # SQL grammar (Pest)
├── client/                          # Client crate
├── docs/
└── LICENSE, README.md
```

### Current Status (as of v0.1)
- [x] Workspace setup
- [x] Server binary with Axum HTTP on port 9200
- [x] Client binary with REPL
- [x] YAML config loading
- [x] GET /, GET /health, POST /_query endpoints
- [x] SQL grammar (`sql.pest`) — Modular Pest parser
- [x] In-memory storage: CREATE TABLE, DROP TABLE, INSERT, SELECT, UPDATE, DELETE
- [x] WHERE clause, ORDER BY, LIMIT support
- [x] Aggregations (COUNT, SUM, AVG, MIN, MAX)
- [x] GROUP BY and HAVING support
- [x] Column aliases
- [x] DISTINCT support
- [x] INNER and LEFT JOIN support
- [x] Subquery support (correlated and IN)
- [x] EXPLAIN support (execution plan visualization)
- [x] Structured Error Handling (SqlError)
- [x] Integration testing suite
- [x] REPL SQL execution (wired via HTTP)
- [x] Persistence via `sled` snapshots

---

## 8. Phases

### Phase 1: Foundation (v0.1) - ✅ COMPLETE
- [x] Set up workspace with Cargo workspace
- [x] Server binary with Axum HTTP (port 9200)
- [x] Client binary with REPL
- [x] SQL parser using Pest (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, WHERE)
- [x] In-memory table storage
- [x] Integration tests
- [x] Persistence (Sled snapshots)

### Phase 2: HTTP API (v0.2)
- [x] HTTP JSON API (basic Axum server running)
- [x] POST /_query endpoint
- [x] GET /, GET /health
- [ ] Transactions support (transaction_id)
- [ ] GET /_stats (Storage/cache statistics)
- [ ] CRUD endpoints for tables (REST)

### Phase 3: Advanced SQL (v0.3)
- [x] Wire Pest parser into executor
- [x] WHERE clause filtering
- [x] UPDATE, DELETE support
- [x] Aggregations, GROUP BY, HAVING
- [x] ORDER BY, LIMIT/OFFSET
- [x] Subqueries (correlated and IN)
- [ ] Multi-table JOINs (optimized)
- [x] Indexes (B-Tree for range scans)

### Phase 4: Reliability & Tooling (v0.4)
- [ ] **SQL Dump/Restore**: Export/Import SQL scripts
- [ ] **Information Schema**: Metadata discoverability
- [x] **Explain Plan**: Query execution transparency (Completed)
- [ ] **Write-Ahead Logging (WAL)**: Durability beyond snapshots

### Phase 5: Ecosystem & Client (v0.5)
- [ ] **MySQL Protocol Compatibility**: Support standard MySQL clients
- [x] Client CLI (Clap)
- [x] REPL with history and HTTP execution
- [ ] JavaScript REPL (QuickJS)
- [ ] Parameterized Queries (Prepared statements)

### Phase 6: Production (v1.0)
- [ ] Authentication & Role-Based Access Control (RBAC)
- [ ] TLS for both HTTP and SQL protocols
- [ ] Prometheus/OpenTelemetry metrics
- [ ] Distributed mode (Raft consensus)
- [ ] Schema Evolution (`ALTER TABLE`)
