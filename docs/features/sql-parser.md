# SQL Parser

## Overview
Pest-based SQL parser for thy-squeal, supporting a MySQL-compatible dialect.

## Implementation Status

- **Grammar** (`server/src/sql/sql.pest`): ✅ Integrated (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX, WHERE, expressions, subqueries, etc.)
- **Executor**: Uses **Modular Pest-based parser** to produce AST. Supported: CREATE TABLE, DROP TABLE, CREATE INDEX, SELECT, INSERT, UPDATE, DELETE, WHERE, ORDER BY, LIMIT, Aggregations, GROUP BY, HAVING, DISTINCT, INNER/LEFT JOIN, Subqueries, and Advanced Indexing.
- **Explain Plan**: ✅ Supported for `SELECT` statements.

## Parser Architecture

The parser is decomposed into submodules for maintainability:
- `parser/mod.rs`: Main entry and top-level statement dispatch.
- `parser/expr.rs`: Expression, Condition, and Literal parsing (including aggregates, scalar functions, and subqueries).
- `parser/select.rs`: SELECT specific clauses (JOIN, GROUP BY, HAVING, ORDER BY, LIMIT).
- `parser/dml.rs`: INSERT, UPDATE, DELETE parsing.
- `parser/ddl.rs`: CREATE TABLE, DROP TABLE, CREATE INDEX parsing.
- `parser/utils.rs`: Shared helper functions.

## Supported SQL Statements

### Data Query Language (DQL)
- `SELECT` with columns, expressions, aliases
- `FROM` with table references
- `WHERE` conditions (basic operators + `IN` subquery)
- `ORDER BY`
- `LIMIT` / `OFFSET`
- `Aggregations` (COUNT, SUM, AVG, MIN, MAX)
- `GROUP BY`
- `HAVING`
- `DISTINCT`
- `INNER JOIN`
- `LEFT JOIN`
- `EXPLAIN` (Prefix for `SELECT`)
- `SEARCH` (Full-Text Search)

### Data Manipulation Language (DML)
- `INSERT INTO ... VALUES ...`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`

### Data Definition Language (DDL)
- `CREATE TABLE ... (columns, types)`
- `DROP TABLE`
- `CREATE [UNIQUE] INDEX [name] ON [table] (expr1, expr2, ...) [USING BTREE|HASH] [WHERE condition]`

## Advanced Indexing

thy-squeal supports several advanced indexing techniques:

### 1. Composite Indexes
Index multiple columns or expressions together.
```sql
CREATE INDEX idx_name ON users (last_name, first_name);
```

### 2. Hash Indexes
Fast $O(1)$ equality lookups.
```sql
CREATE INDEX idx_id ON users (id) USING HASH;
```

### 3. JSON Path Indexes
Index specific fields inside a JSON column.
```sql
CREATE INDEX idx_user_id ON events (data.user.id);
```

### 4. Functional Indexes
Index the result of an expression.
```sql
CREATE INDEX idx_lower_email ON users (LOWER(email));
```

### 5. Partial Indexes
Index only a subset of rows matching a condition.
```sql
CREATE UNIQUE INDEX idx_active_orders ON orders (id) WHERE status = 'pending';
```

## Explain Plan

The `EXPLAIN` keyword can be prefixed to any `SELECT` statement to see the execution strategy:

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
```

It returns a table showing:
- **Scan Type**: `Full Table Scan`, `Index Lookup (BTree)`, or `Index Lookup (Hash)`
- **Index Used**: The name of the index being utilized (if any)
- **Filters**: Conditions being applied
- **Aggregation**: Grouping or aggregate functions being calculated
