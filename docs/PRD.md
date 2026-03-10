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
- Transactions: BEGIN, COMMIT, ROLLBACK
- JOINs: INNER JOIN, LEFT JOIN
- Aggregations: COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING
- ORDER BY and LIMIT/OFFSET
- Aliases for columns
- Subqueries: Correlated and non-correlated (IN clause)
- **EXPLAIN**: Query execution plan visualization
- **Data Export/Import**: `.dump` and `.restore` commands (SQL script format)
- **Information Schema**: Metadata querying (tables, columns, statistics)

#### 3.1.2 Performance & Reliability
- **Indexes**: B-Tree, Hash, Composite, JSON Path, Functional, and Partial indexes
- **Explain Plan**: Visualizing query execution strategy
- **Transactions**: ACID compliance with Snapshot Isolation
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
│       ├── main.rs                  # Server entry (Axum + MySQL TCP)
│       ├── config.rs                # YAML config loading
│       ├── http.rs                  # Axum HTTP handlers
│       ├── mysql/                   # MySQL Protocol handler
│       ├── storage/                 # Modular storage engine
│       │   ├── database.rs          # Database state management
│       │   ├── table/               # Table, Index, and Mutation logic
│       │   ├── value/               # Modular data types (Cast, Ops)
│       │   ├── info_schema.rs       # Metadata virtual tables
│       │   └── error.rs             # StorageError
│       ├── sql/                     # SQL engine
│       │   ├── ast/                 # Decomposed AST definitions
│       │   ├── eval/                # Modular expression evaluation
│       │   ├── parser/              # Modular Pest-based parsing
│       │   ├── executor/            # Specialized statement executors
│       │   └── error.rs             # SqlError enum
│       └── sql.pest                 # SQL grammar (Pest)
├── client/                          # Client crate
├── docs/
└── LICENSE, README.md
```

### Current Status (as of v0.5)
- [x] Workspace setup
- [x] Server binary with Axum HTTP and MySQL TCP (3306)
- [x] Client binary with REPL
- [x] SQL grammar (`sql.pest`) — Modular Pest parser
- [x] Modular Architecture: Decomposed AST, Parser, Evaluator, and Storage
- [x] In-memory storage: CREATE, ALTER, DROP, INSERT, SELECT, UPDATE, DELETE
- [x] Materialized Views with automatic refresh
- [x] ACID Transactions & WAL Durability
- [x] Advanced Indexing (B-Tree, Hash, JSON, Functional, Partial)
- [x] User Authentication & RBAC (Secure access control)
- [x] Standard SQL Functions (CONCAT, COALESCE, etc.)
- [x] CTE Support (WITH clause)
- [x] SELECT without FROM (Dual-less support)
- [x] Integration testing suite (43+ tests)

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

### Phase 2: HTTP API (v0.2) - ✅ COMPLETE
- [x] HTTP JSON API (basic Axum server running)
- [x] POST /_query endpoint
- [x] GET /, GET /health
- [x] Transaction ID support in HTTP API
- [ ] GET /_stats (Storage/cache statistics)
- [ ] CRUD endpoints for tables (REST)

### Phase 3: Advanced SQL (v0.3) - ✅ COMPLETE
- [x] Wire Pest parser into executor
- [x] WHERE clause filtering
- [x] UPDATE, DELETE support
- [x] Aggregations, GROUP BY, HAVING
- [x] ORDER BY, LIMIT/OFFSET
- [x] Subqueries (correlated and IN)
- [x] INNER and LEFT JOIN
- [x] Advanced Indexing (Hash, Composite, JSON, Functional, Partial)

### Phase 4: ACID & Protocol (v0.4) - ✅ COMPLETE
- [x] **Transactions**: BEGIN, COMMIT, ROLLBACK
- [x] **Write-Ahead Logging (WAL)**: Durability beyond snapshots
- [x] **Information Schema**: Metadata discoverability
- [x] **Explain Plan**: Query execution transparency
- [x] **SQL Dump/Restore**: Export/Import SQL scripts
- [x] **MySQL Protocol Compatibility**: Native TCP support (Port 3306)
### Phase 5: Compatibility & Ecosystem (v0.5) - ✅ COMPLETE
- [x] Parameterized Queries (Prepared statements)
- [x] Client CLI (Clap)
- [x] REPL with history and HTTP execution
- [x] **ALTER TABLE**: Support for schema evolution
- [x] **Constraints**: Primary & Foreign Keys
- [x] **AUTO_INCREMENT**: Automated ID generation
- [x] **CTEs**: Common Table Expressions (WITH clause)
- [x] **Materialized Views**: Automatically refreshing pre-calculated query results
- [x] **User Authentication & RBAC**: Secure access control

### Phase 6: Production & Distributed (v1.0) - 🏗 IN PROGRESS
- [ ] **JavaScript Query Interface**: QuickJS integration
- [ ] **Distributed Mode**: multi-node replication (Raft)
- [ ] **Telemetry**: Prometheus/OpenTelemetry metrics
- [ ] **Encryption**: TLS for both HTTP and SQL protocols
- [ ] **Advanced Schema Evolution**: Type changes and constraint modifications

