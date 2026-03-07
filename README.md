# thy-squeal

A lightweight SQL server with HTTP JSON API, built in Rust.

## Current Features

- **SQL Database** - In-memory relational storage with a SQL subset: CREATE TABLE, DROP TABLE, SELECT, INSERT, UPDATE, DELETE, WHERE clause, ORDER BY, LIMIT, aggregations, GROUP BY, HAVING, DISTINCT, INNER JOIN, and LEFT JOIN support
- **HTTP API** - JSON API on port 9200: `GET /`, `GET /health`, `POST /_query` for SQL execution
- **CLI Client** - `thy-squeal-client` with interactive SQL REPL and `--http -e "SQL"` for one-off queries
- **Configuration** - YAML config (`thy-squeal.yaml`) for server, storage, security, and logging

## Roadmap (see [PRD](./docs/PRD.md))

- MySQL-compatible SQL dialect (WHERE, JOINs, UPDATE, DELETE, aggregations)
- TCP SQL protocol
- Key-value store (Redis-like)
- Full-text search
- JavaScript REPL client

## Quick Start

```bash
# Build
cargo build

# Run server (HTTP on port 9200)
cargo run -p thy-squeal

# Run client
cargo run -p thy-squeal-client

# Or run in release mode
cargo run --release -p thy-squeal
```

## Documentation

- [PRD](./docs/PRD.md) - Product requirements and architecture
- [MVP Architecture](./docs/MVP-ARCHITECTURE.md) - Suggested changes for a minimal viable release
- [TODO](./docs/TODO.md) - Implementation tasks
- [Features](./docs/features/) - Detailed feature specifications

## Binaries

| Binary | Description |
|--------|-------------|
| `thy-squeal` | SQL server (HTTP on port 9200; TCP SQL port 3306 planned) |
| `thy-squeal-client` | CLI client with REPL (`--http` mode for server connection) |

## License

MIT
