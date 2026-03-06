# Table Caching

## Overview
Dynamic in-memory cache for tables and views with configurable eviction policies.

## Configuration

### Server Config
```yaml
storage:
  max_memory_mb: 4096
  default_cache_size: 10000
  default_eviction: "LRU"
```

### Per-Table Config
```sql
CREATE TABLE users (...) WITH (
  cache_size = 10000,
  eviction = 'LRU'
);

CREATE TABLE logs (...) WITH (
  cache_size = 100000,
  eviction = 'FIFO'
);

ALTER TABLE users SET cache_size = 50000;
```

## Eviction Policies

### LRU (Least Recently Used)
- Evicts least recently accessed rows
- Good for typical access patterns

### LFU (Least Frequently Used)
- Evicts least frequently accessed rows
- Good for hot/cold data

### FIFO (First In First Out)
- Evicts oldest rows first
- Good for time-series data

## Cache Operations

### View Caching
```sql
CREATE VIEW user_stats AS
SELECT status, COUNT(*) as cnt
FROM users
GROUP BY status
WITH (cache_ttl = 300);  -- Refresh every 5 minutes
```

### Manual Cache Control
```sql
-- Prefetch table into cache
CACHE TABLE users;

-- Clear table cache
UNCACHE TABLE users;

-- Warm up cache
SELECT * FROM users;  -- Auto-caches
```

## Statistics

### HTTP Endpoint
```bash
GET /_stats
```

```json
{
  "cache": {
    "tables": {
      "users": {
        "rows_cached": 5000,
        "cache_size": 10000,
        "eviction_policy": "LRU",
        "hits": 15000,
        "misses": 200,
        "hit_ratio": 0.987
      }
    },
    "memory_usage_mb": 256,
    "max_memory_mb": 4096
  }
}
```

### SQL Access
```sql
SELECT * FROM information_schema.cache_stats;
```

## Implementation

- Use `moka` crate for cache backend
- Per-table cache instances
- Background eviction threads
- Async cache updates on writes

## Monitoring

### Metrics
- Cache hit/miss ratio
- Memory usage per table
- Eviction count
- Average access time

### Alerts (Future)
- Low hit ratio warning
- Memory threshold exceeded
- Cache miss spike
