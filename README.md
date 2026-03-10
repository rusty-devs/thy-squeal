# thy-squeal

A lightweight, MySQL-compatible SQL server with dual-protocol support (SQL over TCP + HTTP JSON API), featuring full-text search, dynamic caching, and Redis-like key-value capabilities.

![Logo](./images/logo.png)

## Features

- **SQL Engine**: Pest-based parser supporting SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, JOINs, Subqueries, Aggregations, and more.
- **MySQL Compatible**: Native TCP support on port 3306. Connect via standard `mysql` CLI or GUI tools.
- **Full-Text Search**: Integrated Tantivy-powered search with `SEARCH` command.
- **Persistence**: Hybrid in-memory storage with Sled-based snapshotting.
- **HTTP API**: Axum-based JSON API for easy integration.
- **Backup & Restore**: Export and import full database state as SQL scripts.
- **REPL**: Interactive CLI client for manual querying and management.
- **Observability**: Built-in `EXPLAIN` support for query plan visualization.

## Quick Start

### Build and Run Server
```bash
# Start the server (default HTTP port 9200)
cargo run -p thy-squeal
```

### Run Client
```bash
# Start the interactive REPL
cargo run -p thy-squeal-client

# Export database to a file
cargo run -p thy-squeal-client -- --export backup.sql

# Import database from a file
cargo run -p thy-squeal-client -- --import backup.sql
```

### Example Queries
```sql
-- Create a table
CREATE TABLE users (id INT, name TEXT, email TEXT);

-- Insert data
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');

-- Search data
SEARCH users 'alice';

-- Complex query
SELECT name, (SELECT COUNT(*) FROM posts WHERE user_id = users.id) as post_count
FROM users
WHERE id = 1;
```

## Documentation

- [MVP Architecture](./docs/MVP-ARCHITECTURE.md)
- [Product Requirements (PRD)](./docs/PRD.md)
- [Comparison with other Engines](./docs/COMPARISON.md)
- [SQL Parser Details](./docs/features/sql-parser.md)
- [Implementation TODO](./docs/TODO.md)

## Development

See [AGENTS.md](./AGENTS.md) for development guidelines, commands, and project structure.

## License

MIT
