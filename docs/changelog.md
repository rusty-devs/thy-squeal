# thy-squeal Changelog

## [v0.6.1] - 2026-03-15
### Added
- **Redis Protocol Compatibility**: RESP protocol support on port 6379 with comprehensive command implementation.
- **Core KV Commands**: GET, SET, DEL, EXISTS, EXPIRE, TTL, KEYS
- **Hash Commands**: HSET, HGET, HDEL, HGETALL
- **List Commands**: LPUSH, RPUSH, LRANGE, LPOP, RPOP, LLEN
- **Set Commands**: SADD, SREM, SMEMBERS, SISMEMBER
- **Sorted Set Commands**: ZADD, ZRANGE, ZRANGEBYSCORE, ZREM
- **Stream Commands**: XADD, XRANGE, XLEN
- **Pub/Sub Commands**: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PUBSUB
- **JSqueal KV Support**: Full KV data structure operations via Squeal IR (KvSet, KvGet, KvHashSet, KvListPush, KvSetAdd, KvZSetAdd, KvStreamAdd, PubSubPublish)
- **Enhanced Persistence**: Snapshot-based persistence including all KV data structures (hash, list, set, zset, stream) alongside WAL replay

## [v0.6.0] - 2026-03-12
### Added
- **Squeal IR (Internal Representation)**: Introduced a unified, strongly-typed internal query representation. This layer decouples the surface syntax (SQL or JSON) from the execution engine, providing a more robust foundation for optimization and cross-protocol support.
- **JSqueal (JSON Query Language)**: Added a structured JSON-based query interface accessible via `POST /_jsqueal`. This allows for programmatic query construction and bypassing the SQL parsing stage.
- **Bidirectional IR Conversions**: Full support for converting between SQL AST and Squeal IR, ensuring consistency across all query paths.

### Refactored
- **Storage Decoupling**: Deep refactoring of the storage engine (`Database`, `Table`, `WalRecord`, `TableIndex`) to natively use Squeal IR instead of the SQL AST. This eliminates circular dependencies and allows the storage layer to operate independently of the parser.
- **Unified Schema Types**: Unified `ForeignKey` and `Column` definitions into `crate::storage`, simplifying type management across the AST, IR, and persistence layers.
- **HTTP Handler Modernization**: Updated the Axum-based HTTP server to support the new unified execution pipeline.

## [v0.5.0] - 2026-03-10
### Added
- **Authentication & RBAC**: Implementation of `CREATE USER`, `DROP USER`, `GRANT`, and `REVOKE`. Secure password hashing via `bcrypt`. Fine-grained permission enforcement for all SQL operations.
- **Cost-Based Index Selection**: Optimizer now uses real-time index statistics (selectivity) to decide between Index Lookup and Full Table Scan.
- **Materialized Views**: Support for `CREATE MATERIALIZED VIEW` with automatic background data refresh on underlying table mutations (`INSERT`, `UPDATE`, `DELETE`).
- **Information Schema Expansion**: Added `statistics`, `key_column_usage`, and `schemata` tables for improved metadata discoverability and tool compatibility.
- **CTEs (WITH clause)**: Support for non-recursive Common Table Expressions.
- **Constraints**: Support for `PRIMARY KEY` and `FOREIGN KEY` (Referential Integrity). Automatic unique index creation for PKs.
- **Standard SQL Functions**: Added variadic function support and implemented `CONCAT`, `COALESCE`, `NOW`, and `REPLACE`.
- **SELECT without FROM**: Support for dual-less selects (e.g., `SELECT NOW()`).
- **ALTER TABLE**: Support for schema evolution via `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `RENAME TO` (table renaming).

### Refactored
- **Architectural Modularization**: Deep decomposition of the codebase for better maintainability:
    - `sql/ast/`: Split into specialized modules for expressions, conditions, and statements.
    - `sql/parser/`: Modularized into `ddl/`, `dml/`, and `select/` subdirectories.
    - `sql/eval/`: Decomposed expression evaluation into `binary`, `function`, and `subquery` modules.
    - `storage/table/`: Unified table structure, indexing, and mutation logic into a dedicated module.
    - `storage/value/`: Split core enum from casting and comparison logic.
    - `mysql/`: Fully decoupled protocol handling from the main server logic.
- **Improved Robustness**: Refactored SQL join and alias parsing to be more resilient to optional keywords and complex syntax.

## [v0.4.0] - 2026-03-10
### Added
- **MySQL Protocol Compatibility**: Native TCP support on port 3306. Allows standard MySQL clients (CLI, DBeaver, etc.) to connect.
- **Prepared Statements**: Full support for `PREPARE`, `EXECUTE`, and `DEALLOCATE` commands.
- **Parameterized Queries**: Support for `?` and `$1`, `$2` style placeholders in SELECT, INSERT, UPDATE, and DELETE.
- **Enhanced INSERT**: Added support for specifying target columns in INSERT statements (e.g., `INSERT INTO table (col1, col2) VALUES (...)`).
- **SQL Dump & Restore**: Capabilities to export the entire database state as a SQL script and restore it.
- **Information Schema**: Added virtual `information_schema` tables (`tables`, `columns`) for database metadata introspection.
- **Write-Ahead Logging (WAL)**: Implemented Sled-based WAL for durability and crash recovery.
- **Transactions**: Full support for ACID transactions with `BEGIN`, `COMMIT`, and `ROLLBACK`.

## [v0.3.0] - 2026-03-07
### Added
- **Advanced Indexing**:
    - B-Tree and Hash indexes.
    - Composite indexes (multi-column).
    - JSON Path indexes for nested data.
    - Functional/Expression-based indexes.
    - Partial/Conditional indexes.
    - Unique constraints and indexes.
- **Complex SQL Support**:
    - Correlated Subqueries.
    - INNER and LEFT JOINs.
    - `GROUP BY` and `HAVING` clauses.
    - Aggregation functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`).
    - `DISTINCT` keyword support.
    - `ORDER BY`, `LIMIT`, and `OFFSET`.
    - `WHERE` clause with complex logical operations.
- **Full-Text Search**: Integrated Tantivy-powered search with a custom `SEARCH` command.
- **Query Observability**: `EXPLAIN` command for query plan visualization.

### Refactored
- Significant codebase modularization:
    - Decoupled Expression evaluation from Condition logic.
    - Separated Grouping from Aggregate functions.
    - Modularized DML (Insert/Update/Delete) and DDL (Create/Drop) executors.
    - Moved storage mutation logic into dedicated modules.

## [v0.2.0] - 2026-03-07
### Added
- **Pest Parser**: Migration to a robust Pest-based grammar for SQL parsing.
- **HTTP JSON API**: Implementation of `POST /_query` endpoint using Axum.
- **Persistence**: Hybrid in-memory storage with Sled-based background snapshotting.
- **Error Mapping**: Structured mapping of SQL and Storage errors to HTTP responses.

## [v0.1.0] - 2026-03-06
### Added
- **Core Engine**: Initial in-memory storage implementation (Database, Table, Row, Value).
- **Basic SQL**: Simple string-based parser for `CREATE`, `INSERT`, and `SELECT`.
- **CLI Client**: Interactive REPL for database management.
- **Foundation**: Project workspace setup and basic project documentation (PRD, Architecture).
