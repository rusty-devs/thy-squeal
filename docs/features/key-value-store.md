# Key-Value Store

## Overview
Redis-compatible in-memory key-value store with multiple data structures. Accessible via Redis protocol (RESP on port 6379) and JSqueal (JSON API).

## Storage Backend
- Primary: In-memory hash maps for concurrent access
- Persistence: Sled-based snapshots and WAL for durability

## Redis Protocol (Port 6379)

### Strings
```
SET key value
GET key
DEL key
EXISTS key
EXPIRE key seconds
TTL key
KEYS pattern
```

### Hashes
```
HSET key field value
HGET key field
HGETALL key
HDEL key field [field ...]
```

### Lists
```
LPUSH key value [value ...]
RPUSH key value [value ...]
LRANGE key start stop
LPOP key [count]
RPOP key [count]
LLEN key
```

### Sets
```
SADD key member [member ...]
SMEMBERS key
SISMEMBER key member
SREM key member [member ...]
```

### Sorted Sets
```
ZADD key score member [score member ...]
ZRANGE key start stop [WITHSCORES]
ZRANGEBYSCORE key min max [WITHSCORES]
ZREM key member [member ...]
```

### Streams
```
XADD key [ID] field value [field value ...]
XRANGE key start stop [COUNT n]
XLEN key
```

### Pub/Sub
```
PUBLISH channel message
SUBSCRIBE channel [channel ...]
UNSUBSCRIBE [channel ...]
PUBSUB CHANNELS
```

## JSqueal API

Access via `POST /_jsqueal`:

```json
{
  "squeal": {
    "KvSet": {
      "key": "mykey",
      "value": {"Text": "hello"},
      "expiry": 60
    }
  }
}
```

```json
{
  "squeal": {
    "KvGet": {
      "key": "mykey"
    }
  }
}
```

## TTL
- Use `EXPIRE key seconds` to set expiration
- `TTL key` returns:
  - `-1` = no expiry
  - `-2` = key doesn't exist
  - `> 0` = seconds remaining

## Key Naming Convention
Example keys: `session:abc123`, `user:1:profile`, `cache:products`
