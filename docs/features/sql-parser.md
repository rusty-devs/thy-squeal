# SQL Parser

## Overview
Pest-based SQL parser for thy-squeal, supporting a MySQL-compatible dialect.

## Implementation Status

- **Grammar** (`server/src/sql/sql.pest`): ✅ Integrated (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX, WHERE, expressions, subqueries, etc.)
- **Executor**: Uses **Modular Pest-based parser** to produce AST. Supported: CREATE TABLE, DROP TABLE, CREATE INDEX, SELECT, INSERT, UPDATE, DELETE, WHERE, ORDER BY, LIMIT, Aggregations, GROUP BY, HAVING, DISTINCT, INNER/LEFT JOIN, Subqueries, and ACID Transactions.
- **Explain Plan**: ✅ Supported for `SELECT` statements.
- **Information Schema**: ✅ Query metadata via virtual tables.

## Parser Architecture

The parser is decomposed into submodules for maintainability:
- `parser/mod.rs`: Main entry and top-level statement dispatch.
- `parser/expr/`: Modular expression parsing.
    - `literal.rs`: String, Number, Boolean, and Null literals.
    - `functions.rs`: Aggregate and Scalar function calls.
    - `condition.rs`: WHERE/HAVING logic, comparisons, and subqueries.
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
- `INSERT INTO table [(col1, col2, ...)] VALUES (val1, val2, ...)`
- `UPDATE ... SET ... WHERE ...`
- `DELETE FROM ... WHERE ...`

### Data Definition Language (DDL)
- `CREATE TABLE ... (columns, types [AUTO_INCREMENT | PRIMARY KEY])`
- `ALTER TABLE table [ADD COLUMN col def | DROP COLUMN col | RENAME COLUMN col TO new | RENAME TO new_table]`
- `DROP TABLE`
- `CREATE [UNIQUE] INDEX [name] ON [table] (expr1, expr2, ...) [USING BTREE|HASH] [WHERE condition]`

**Example Table with Auto-Increment:**
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT
);
-- or
CREATE TABLE users (
    id INT AUTO_INCREMENT,
    name TEXT
);
```

### Transaction Control
- `BEGIN` / `START TRANSACTION`
- `COMMIT`
- `ROLLBACK`

### Prepared Statements
thy-squeal supports server-side prepared statements for query reuse and performance:

- `PREPARE name FROM 'sql_query'`
- `EXECUTE name [USING val1, val2, ...]`
- `DEALLOCATE PREPARE name`

**Example:**
```sql
PREPARE inst FROM 'INSERT INTO users (id, name) VALUES (?, ?)';
EXECUTE inst USING 1, 'Alice';
EXECUTE inst USING 2, 'Bob';
DEALLOCATE PREPARE inst;
```

### Parameterized Queries
thy-squeal supports parameterized queries to prevent SQL injection and improve performance:

#### Positional Placeholders (`?`)
```sql
SELECT * FROM users WHERE id = ? AND status = ?
```
Parameters are passed as a JSON array in the `params` field.

#### Named Placeholders (`$1`, `$2`, etc.)
```sql
SELECT * FROM users WHERE id = $1 AND name = $2
```
Parameters are matched by index (1-based).

**Example via HTTP API:**
```bash
POST /_query
Content-Type: application/json

{
  "sql": "SELECT * FROM users WHERE id = $1 AND status = ?",
  "params": [1, "active"]
}
```

### System Metadata
thy-squeal supports the standard `information_schema` for discovering database metadata.

**Query Tables:**
```sql
SELECT * FROM information_schema.tables;
```

**Query Columns:**
```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users';
```

**Query Indexes:**
```sql
SELECT index_name, is_unique FROM information_schema.indexes WHERE table_name = 'orders';
```

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
