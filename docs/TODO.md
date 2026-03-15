# thy-squeal Implementation TODO

## Phase 1: Foundation (v0.1) - ✅ COMPLETE
- [x] Project workspace setup (Cargo)
- [x] Basic in-memory storage (Database, Table, Row, Value)
- [x] Axum HTTP server with `/` and `/health`
- [x] Simple SQL parser (string-based)
- [x] Basic execution logic (CREATE, INSERT, SELECT)
- [x] Client CLI with REPL

## Phase 2: HTTP & Persistence (v0.2) - ✅ COMPLETE
- [x] Move to Pest-based parser for robust SQL
- [x] Map SQL errors to HTTP responses
- [x] Implement JSON API `POST /_query`
- [x] Implement Sled-based persistence (Snapshots)
- [x] Periodic and DML-triggered background saving

## Phase 3: Advanced SQL (v0.3) - ✅ COMPLETE
- [x] Implement WHERE clause with complex logic
- [x] Implement ORDER BY and LIMIT/OFFSET
- [x] Implement DISTINCT
- [x] Implement Aggregations (COUNT, SUM, etc.)
- [x] Implement GROUP BY and HAVING
- [x] Implement INNER and LEFT JOIN
- [x] Implement Correlated Subqueries
- [x] Implement EXPLAIN plan
- [x] Full-Text Search (Tantivy integration)
- [x] B-Tree Indexes (Range & Equality)
- [x] Hash Indexes (O(1) equality lookups)
- [x] Composite Indexes (Multi-column)
- [x] JSON Path Indexes (Indexing nested fields)
- [x] Functional Indexes (Expression-based indexing)
- [x] Partial Indexes (Conditional indexing)
- [x] Unique Constraints / Indexes

## Phase 4: ACID & Protocol (v0.4) - ✅ COMPLETE
- [x] Transactions (BEGIN, COMMIT, ROLLBACK)
- [x] Write-Ahead Logging (WAL) for durability
- [x] Information Schema (tables, columns metadata)
- [x] SQL Dump/Restore (.sql script export)
- [x] MySQL Protocol Compatibility (TCP 3306)
- [x] Parameterized Queries (Prepared Statements)

## Code Quality & Refactoring - ✅ COMPLETE
- [x] Decompose `eval.rs` (Expression vs Condition logic)
- [x] Decompose `executor/aggregate.rs` (Grouping vs Functions)
- [x] Decompose `parser/expr.rs` (Literals vs Logic)
- [x] Decompose `executor/dml.rs` (Insert/Update/Delete modules)
- [x] Move WAL recovery logic to `storage/wal.rs`
- [x] Decompose `storage/table.rs` (Index and Mutation logic)
- [x] Modularize test suite (`tests/` and `executor/tests/` directories)

## Phase 5: Compatibility & Ecosystem (v0.5) - ✅ COMPLETE
- [x] **ALTER TABLE**: Support for `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `RENAME TABLE`
- [x] **Constraints**: Proper `PRIMARY KEY` and `FOREIGN KEY` (Referential Integrity)
- [x] **AUTO_INCREMENT / SERIAL**: Automated ID generation for integer columns
- [x] **Standard SQL Functions**: `CONCAT`, `SUBSTRING`, `COALESCE`, `NOW()`, `DATE_FORMAT`, `CAST(x AS type)`
- [x] **CTEs (WITH clause)**: Common Table Expressions for complex query readability
- [x] **Information Schema Expansion**: `statistics`, `key_column_usage`, `schemata` tables
- [x] Secondary Index optimization (using index only if selective)
- [x] Materialized Views
- [x] User Authentication & RBAC

## Phase 6: Key-Value Storage (v0.6) - ✅ COMPLETE
- [x] **Redis Protocol Compatibility**: Support for RESP protocol on port 6379
- [x] **Core Commands**: GET, SET, DEL, EXISTS, EXPIRE, TTL, KEYS
- [x] **Data Structures**: Hash (HSET/HGET/HDEL/HGETALL), Lists (LPUSH/RPUSH/LRANGE/LPOP/RPOP/LLEN), Sets (SADD/SREM/SMEMBERS/SISMEMBER), Sorted Sets (ZADD/ZRANGE/ZRANGEBYSCORE/ZREM)
- [x] **Streams (XADD, XREAD, etc.)**: XADD, XRANGE, XLEN
- [x] **Persistence**: RDB-style snapshots and AOF (Append Only File) integration with existing WAL
- [x] **Pub/Sub**: Basic message queuing and notification system
- [ ] **Pub/Sub**: Basic message queuing and notification system
- [x] **SQL Integration**: Querying Key-Value data via SQL virtual tables

## Phase 7: Production & Distributed (v1.0) - 🏗 IN PROGRESS
- [x] **JSqueal**: JSON-based query language (direct IR mapping, bypassing Pest parser)
- [ ] **Distributed Mode**: Multi-node replication via Raft consensus
- [ ] **Telemetry**: Prometheus metrics and OpenTelemetry tracing
- [ ] **Encryption**: TLS support for HTTP and MySQL TCP protocols
- [ ] **Advanced Schema Evolution**: Type changes and constraint modifications
- [ ] **Query Optimizer Phase 2**: Cost-based Join ordering
- [ ] JavaScript Query Interface (QuickJS)

## High-Impact Refactorings - ✅ COMPLETE
- [x] **Squeal IR**: Introduce an internal query representation layer to decouple parser from executor.
- [x] **Command Pattern Dispatcher**: Split `exec_stmt` into specialized `StatementExecutor` structs.
- [x] **Session Management**: Introduce a `Session` struct to encapsulate user, transaction, and settings state.
- [x] **Evaluator Decomposition**: Split monolithic evaluators into specialized, chainable components.
- [x] **Storage Decoupling**: Separate `Table` into `TableSchema`, `TableData`, and `TableIndexes`.
- [x] **Error Handling Unification**: Streamline `SqlError` and `StorageError` hierarchy.
