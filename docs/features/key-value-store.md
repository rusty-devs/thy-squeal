# Key-Value Store

## Overview
Redis-compatible in-memory key-value store with multiple data structures.

## Storage Backend
- Primary: `DashMap` for concurrent access
- Persistence: `sled` for optional disk persistence

## Configuration
```yaml
storage:
  data_dir: "./data"
  snapshot_interval_sec: 300
```

## Data Types

### Strings
```sql
SET @key = 'value';
SET @key = 'value' EX 60;     -- TTL 60 seconds
GET @key;
DEL @key;
```

### Numbers (Counters)
```sql
SET @counter = 0;
INCR @counter;    -- Returns new value
DECR @counter;    -- Returns new value
INCRBY @counter 5;
DECRBY @counter 3;
```

### Hashes
```sql
HSET @user:1 name 'Alice' email 'alice@example.com';
HGET @user:1 name;
HGETALL @user:1;
HDEL @user:1 email;
HKEYS @user:1;
HVALS @user:1;
HLEN @user:1;
```

### Lists
```sql
LPUSH @queue 'task1';
RPUSH @queue 'task2';
LPOP @queue;
RPOP @queue;
LRANGE @queue 0 -1;
LLEN @queue;
```

### Sets
```sql
SADD @tags 'rust' 'database';
SMEMBERS @tags;
SISMEMBER @tags 'rust';
SREM @tags 'database';
SCARD @tags;
```

## HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/kv` | List keys (query: `pattern=*`, `type=string`) |
| GET | `/kv/<key>` | Get value |
| PUT | `/kv/<key>` | Set value (body: `{"value": "...", "ttl": 60}`) |
| DELETE | `/kv/<key>` | Delete key |

## Key Naming Convention
- Strings: `strings:<name>`
- Hashes: `hashes:<name>`
- Lists: `lists:<name>`
- Sets: `sets:<name>`

Example keys: `session:abc123`, `user:1:profile`, `cache:products`

## TTL
- Use `EX` seconds or `PX` milliseconds
- `SET key value EX 60`
- `EXPIRE key 30`
- `TTL key` - returns remaining seconds
- `-1` = no expiry, `-2` = key doesn't exist

## Pub/Sub (Future)
```sql
PUBLISH channel message;
SUBSCRIBE channel;
```
