# TCP SQL Protocol

## Overview
MySQL-compatible TCP wire protocol for SQL connections.

## Connection

### Handshake
```
Client -> Server: HandshakeResponse41
Server -> Client: OKPacket (auth success)
             or ErrorPacket (auth failed)
```

### Connection URI
```bash
thy-squeal-client thy-sql://localhost:3306
thy-squeal-client 127.0.0.1:3306
```

## Packet Format

### Header (4 bytes)
```
[0-1] Length (little-endian)
[2]   Packet number
[3]   Reserved
```

### Packet Types

#### OK Packet
```
[0]   0x00 (OK)
[1-4] Affected rows
[5-8] Insert ID
[2]   Status flags
[2]   Warnings
[n]   Message (optional)
```

#### Error Packet
```
[0]   0xFF (ERROR)
[1-2] Error code
[1]   SQL state marker ('#')
[5]   SQL state (5 chars)
[n]   Error message
```

#### Result Set
```
[4]   Column count (little-endian)
...   Column definitions
[0xFF] Row data marker
...   Rows
```

## Commands

### COM_QUERY
```
[0]   0x03
[n]   Query string
```

### COM_STMT_PREPARE
```
[0]   0x16
[n]   SQL statement
```

### COM_STMT_EXECUTE
```
[0]   0x17
[1-4] Statement ID
[1]   Flags
[1-4] Iteration count
[n]   Parameters
```

### COM_PING
```
[0]   0x0E
```

## Message Types

| Code | Name | Description |
|------|------|-------------|
| 0x00 | OK | Success |
| 0xFF | ERR | Error |
| 0xFB | LOCAL_INFILE | Local infile |
| 0xFE | EOF | End of results |
| 0xFF | COLUMN_DEFINITION | Column def |

## Status Flags

| Flag | Value | Description |
|------|-------|-------------|
| SERVER_STATUS_IN_TRANS | 0x0001 | In transaction |
| SERVER_STATUS_AUTOCOMMIT | 0x0002 | Auto-commit mode |
| SERVER_MORE_RESULTS_EXISTS | 0x0008 | More results |

## Example Session

```
Client: [0x16] "SELECT * FROM users WHERE id = ?"
Server: [0x01] [0x01] columns: 3
        [table] "users"
        [name] "id"
        [type] 0x03 (INT)
Server: [0x00] [0x01] [0x01] [0x00]

Client: [0x17] [0x00][0x00][0x00][0x00] [0x00] [0x01][0x00][0x00][0x00] [0x01]
Server: [0x00] [0x01][0x00][0x00][0x00]
        [0x01] [0x01] [0x00]
        [0x0A] "Alice"
Server: [0xFE]
```

## Implementation

### Rust Types
```rust
enum Packet {
    Handshake,
    HandshakeResponse,
    OK,
    Error,
    Query,
    ResultSet,
    Row,
    EOF,
}
```

### Async Processing
- Use `tokio` for async I/O
- Per-connection state machine
- Connection pooling for efficiency
- Keep-alive ping every 30s

## Performance

- Binary protocol (faster than HTTP)
- Prepared statements caching
- Compression option
- Connection multiplexing (future)
