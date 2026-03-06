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

# Run server
cargo run --release

# Run client
cargo run --release --package thy-squeal-client
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
