# thy-squeal

A lightweight SQL server with HTTP JSON API and Redis-like key-value storage.

## Features

- **SQL Database** - MySQL-compatible SQL dialect with full-text search
- **HTTP API** - Elasticsearch-style REST endpoints
- **Key-Value Store** - Redis-compatible in-memory storage
- **JavaScript REPL** - Interactive client with embedded JS runtime
- **Dynamic Caching** - Configurable per-table LRU/LFU/FIFO cache

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
- [TODO](./docs/TODO.md) - Implementation tasks
- [Features](./docs/features/) - Detailed feature specifications

## Binaries

| Binary | Description |
|--------|-------------|
| `thy-squeal` | SQL server (ports 3306 SQL, 9200 HTTP) |
| `thy-squeal-client` | CLI client with JavaScript REPL |

## License

MIT
