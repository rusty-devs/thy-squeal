# SQL Parser

## Overview
Pest-based SQL parser for thy-squeal, supporting a MySQL-compatible dialect.

## Implementation Status

- **Grammar** (`server/src/sql.pest`): ✅ Integrated (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, WHERE, expressions, etc.)
- **Executor**: Uses **Modular Pest-based parser** to produce AST. Supported: CREATE TABLE, DROP TABLE, SELECT, INSERT, UPDATE, DELETE, WHERE, ORDER BY, LIMIT, Aggregations, GROUP BY, and HAVING.
- **Next steps**: Implement JOINs and Indexes in the executor.

## Parser Architecture

The parser is decomposed into submodules for maintainability:
- `parser/mod.rs`: Main entry and top-level statement dispatch.
- `parser/expr.rs`: Expression, Condition, and Literal parsing (including aggregates).
- `parser/select.rs`: SELECT specific clauses (GROUP BY, HAVING, ORDER BY, LIMIT).
- `parser/dml.rs`: INSERT, UPDATE, DELETE parsing.
- `parser/ddl.rs`: CREATE TABLE, DROP TABLE parsing.
- `parser/utils.rs`: Shared helper functions (e.g., identifiers).

## Supported SQL Statements

### Data Query Language (DQL)
- `SELECT` with columns, expressions, aliases
- `FROM` with table references
- `WHERE` conditions (basic operators)
- `ORDER BY`
- `LIMIT` / `OFFSET`
- `Aggregations` (COUNT, SUM, AVG, MIN, MAX)
- `GROUP BY`
- `HAVING`
- `DISTINCT`
- `INNER JOIN`
- `LEFT JOIN`
- `Subqueries` (correlated and IN)

### Data Manipulation Language (DML)
- `INSERT INTO ... VALUES ...`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`

### Data Definition Language (DDL)
- `CREATE TABLE ... (columns, types)`
- `DROP TABLE`
- [ ] `CREATE INDEX`
- [ ] `CREATE FULLTEXT INDEX`

## Grammar Rules

```
select = { "SELECT" ~ distinct? ~ columns ~ "FROM" ~ table ~ join* ~ where? ~ group_by? ~ having? ~ order_by? ~ limit? }
insert = { "INSERT" ~ "INTO" ~ table_name ~ "VALUES" ~ "(" ~ values ~ ")" }
update = { "UPDATE" ~ table_name ~ "SET" ~ assignments ~ where? }
delete = { "DELETE" ~ "FROM" ~ table_name ~ where? }

columns = { "*" | column_list }
column_list = { column ~ ("," ~ column)* }
column = { expression ~ alias? }

expression = { ... }
condition = { ... }
```

## Data Types

| Type | Aliases | Storage |
|------|---------|---------|
| INT | INTEGER, SMALLINT, BIGINT | i32/i64 |
| FLOAT | DOUBLE, REAL | f64 |
| BOOL | BOOLEAN | bool |
| DATE | - | NaiveDate |
| DATETIME | - | NaiveDateTime |
| VARCHAR(n) | TEXT, STRING | String |
| BLOB | BINARY | Vec<u8> |
| JSON | JSONB | serde_json::Value |

## Operators

### Comparison
- `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- `LIKE`, `NOT LIKE`
- `IN`, `NOT IN`
- `BETWEEN`, `NOT BETWEEN`
- `IS NULL`, `IS NOT NULL`

### Logical
- `AND`, `OR`, `NOT`

### Arithmetic
- `+`, `-`, `*`, `/`, `%`

## Full-Text Search

```sql
SELECT * FROM users WHERE MATCH(name, bio) AGAINST('developer');
SELECT * FROM posts WHERE MATCH(content) AGAINST('+rust -python' IN BOOLEAN MODE);
```

## Parser Output

Returns AST (Abstract Syntax Tree) for executor:
- `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`
- `CreateTableStmt`, `DropTableStmt`
- `Expression`, `Condition`, `Join`, etc.
