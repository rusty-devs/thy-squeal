# HTTP JSON API

## Overview
REST API for thy-squeal, Elasticsearch-style endpoints for data access.

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
  "sql": "SELECT * FROM users WHERE age > ?",
  "params": [18]
}
```

### Response
```json
{
  "success": true,
  "data": [
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30}
  ],
  "rows_affected": 0,
  "execution_time_ms": 5
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

## Error Response
```json
{
  "success": false,
  "error": {
    "code": "PARSE_ERROR",
    "message": "Syntax error at line 1",
    "position": 15
  }
}
```
