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

## Phase 4: ACID & Protocol (v0.4) - 🏗 IN PROGRESS
- [x] Transactions (BEGIN, COMMIT, ROLLBACK)
- [x] Write-Ahead Logging (WAL) for durability
- [x] Information Schema (tables, columns metadata)
- [ ] SQL Dump/Restore (.sql script export) - **CURRENT FOCUS**
- [ ] MySQL Protocol Compatibility (TCP 3306)
- [ ] Parameterized Queries (Prepared Statements)

## Phase 5: Advanced Features (v0.5)
- [ ] Secondary Index optimization (using index only if selective)
- [ ] Materialized Views
- [ ] User Authentication & RBAC
- [ ] Distributed Mode (Raft consensus)
- [ ] JavaScript Query Interface (QuickJS)
