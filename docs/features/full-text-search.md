# Full-Text Search

## Overview
Tantivy-powered full-text search engine with MySQL-compatible syntax.

## Index Creation

```sql
CREATE FULLTEXT INDEX idx_name_bio ON users(name, bio);
CREATE FULLTEXT INDEX idx_content ON posts(title, content);
```

## Search Syntax

### Basic Search
```sql
SELECT * FROM users WHERE MATCH(name, bio) AGAINST('developer');
SELECT * FROM posts WHERE MATCH(content) AGAINST('rust programming');
```

### Boolean Mode
```sql
-- Must contain 'rust'
SELECT * FROM posts WHERE MATCH(content) AGAINST('+rust' IN BOOLEAN MODE);

-- Must contain 'rust', must not contain 'python'
SELECT * FROM posts WHERE MATCH(content) AGAINST('+rust -python' IN BOOLEAN MODE);

-- Contains 'rust' or 'python' (default)
SELECT * FROM posts WHERE MATCH(content) AGAINST('rust python' IN BOOLEAN MODE);

-- Exact phrase
SELECT * FROM posts WHERE MATCH(content) AGAINST('"rust programming"' IN BOOLEAN MODE);

-- Wildcard
SELECT * FROM posts WHERE MATCH(content) AGAINST('prog*' IN BOOLEAN MODE);
```

### Natural Language Mode (Default)
```sql
SELECT * FROM posts WHERE MATCH(content) AGAINST('rust database');
-- Orders by relevance by default
```

## HTTP Search API

```bash
POST /mydb/users/_search
{
  "query": "developer",
  "fields": ["name", "bio"],
  "from": 0,
  "size": 10,
  "highlight": true,
  "filter": {"age": {"$gt": 18}}
}
```

## Response

```json
{
  "hits": {
    "total": 25,
    "max_score": 3.14,
    "results": [
      {
        "id": "42",
        "score": 3.14,
        "data": {
          "name": "Alice Developer",
          "bio": "Experienced developer"
        },
        "highlight": {
          "bio": ["Experienced <em>developer</em>"]
        }
      }
    ]
  },
  "took_ms": 5
}
```

## Indexing

- Auto-index on INSERT/UPDATE
- Index tokenization: whitespace, punctuation
- Lowercasing for case-insensitivity
- Stop words: the, a, an, is, are, etc. (configurable)

## Ranking

- TF-IDF based scoring
- Field boost: title field weighted higher than body
- Document frequency considered

## Performance

- Index in-memory for fast queries
- Incremental index updates
- Segment-based for concurrency
