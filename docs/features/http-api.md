# HTTP JSON API

## Overview
REST API for thy-squeal, Elasticsearch-style endpoints for data access.

## Implementation Status

| Endpoint | Status |
|----------|--------|
| GET `/` | ✅ Implemented |
| GET `/health` | ✅ Implemented |
| POST `/_query` | ✅ Implemented |
| GET `/_dump` | ✅ Implemented |
| POST `/_restore` | ✅ Implemented |
| GET `/_stats` | ❌ Not implemented |
| REST CRUD (`/<db>/<table>`, etc.) | ❌ Not implemented |
| Search, KV | ❌ Not implemented |

## Server Configuration
```yaml
server:
  host: "0.0.0.0"
  http_port: 9200

logging:
  level: "info"
```

## Endpoints

### Server
| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Server info (version, uptime) |
| GET | `/health` | Health check |

### Statistics
| Method | Path | Description |
|--------|------|-------------|
| GET | `/_stats` | Storage & cache statistics |

### Query
| Method | Path | Description |
|--------|------|-------------|
| POST | `/_query` | Execute SQL query |
| GET | `/_dump` | Export full database as SQL script |
| POST | `/_restore` | Import database from SQL script |

### Database
| Method | Path | Description |
|--------|------|-------------|
| GET | `/<db>` | List tables in database |

### Table CRUD
| Method | Path | Description |
|--------|------|-------------|
| GET | `/<db>/<table>` | List rows (paginated) |
| GET | `/<db>/<table>/<id>` | Get row by ID |
| POST | `/<db>/<table>` | Insert row |
| PUT | `/<db>/<table>/<id>` | Update row |
| DELETE | `/<db>/<table>/<id>` | Delete row |

### Search
| Method | Path | Description |
|--------|------|-------------|
| POST | `/<db>/<table>/_search` | Full-text search |

### Key-Value
| Method | Path | Description |
|--------|------|-------------|
| GET | `/kv` | List keys (query param: `pattern`) |
| GET | `/kv/<key>` | Get value |
| PUT | `/kv/<key>` | Set value |
| DELETE | `/kv/<key>` | Delete key |

## Request/Response Examples

### Execute SQL
```bash
POST /_query
Content-Type: application/json

{
  "sql": "SELECT id, name FROM users WHERE id = 1"
}
```

### Execute Parameterized SQL
```bash
POST /_query
Content-Type: application/json

{
  "sql": "SELECT * FROM users WHERE id = $1 AND status = ?",
  "params": [1, "active"]
}
```

### Response
```json
{
  "success": true,
  "columns": ["id", "name"],
  "data": [
    {"id": 1, "name": "Alice"}
  ],
  "rows_affected": 0,
  "execution_time_ms": 5
}
```

### Error Response
```json
{
  "success": false,
  "columns": [],
  "data": [],
  "rows_affected": 0,
  "execution_time_ms": 2,
  "error": {
    "type": "TableNotFound",
    "details": "users"
  }
}
```

### Search
```bash
POST /mydb/users/_search
Content-Type: application/json

{
  "query": "developer",
  "fields": ["name", "bio"],
  "from": 0,
  "size": 10,
  "highlight": true
}
```

### Search Response
```json
{
  "hits": {
    "total": 15,
    "results": [
      {
        "id": "1",
        "score": 2.5,
        "data": {"name": "Alice", "bio": "Senior developer"},
        "highlight": {"bio": ["Senior <em>developer</em>"]}
      }
    ]
  }
}
```
