# Authentication & Security

## Overview
User authentication and transport security for thy-squeal server.

## Configuration
```yaml
security:
  auth_enabled: true
  tls_enabled: false
  users:
    - username: "admin"
      password_hash: "$2b$12$..."  # bcrypt
      roles: ["admin"]
    - username: "readonly"
      password_hash: "$2b$12$..."
      roles: ["read"]
```

## Authentication Methods

### Password (SQL)
```sql
-- Authenticate user
AUTH 'username' 'password';

-- Create user
CREATE USER 'alice' IDENTIFIED BY 'secret123';

-- Grant privileges
GRANT SELECT, INSERT ON mydb.* TO 'alice';
GRANT ALL ON *.* TO 'admin';
```

### Token (HTTP)
```bash
# Login
POST /_auth/login
{"username": "admin", "password": "..."}

# Response
{"token": "eyJhbGciOiJIUzI1NiIs...", "expires_in": 3600}

# Use token
GET /mydb/users -H "Authorization: Bearer eyJ..."
```

## Roles & Privileges

| Privilege | Description |
|-----------|-------------|
| SELECT | Read data |
| INSERT | Insert rows |
| UPDATE | Update rows |
| DELETE | Delete rows |
| CREATE | Create tables/indexes |
| DROP | Drop tables |
| ADMIN | All privileges |

## TLS/SSL

### Server Certificate
```yaml
security:
  tls_enabled: true
  cert_file: "./certs/server.crt"
  key_file: "./certs/server.key"
```

### Connection URIs
```bash
# SQL with TLS
thy-squeal-client thy-sqls://localhost:3306

# HTTP with TLS
curl https://localhost:9200/health
```

## SQL Injection Prevention

### Parameterized Queries (Recommended)
```javascript
// Safe - parameters escaped automatically
conn.query('SELECT * FROM users WHERE id = ?', [userId]);
```

### Raw Query (Avoid)
```javascript
// DANGEROUS - SQL injection vulnerable
conn.query(`SELECT * FROM users WHERE name = '${name}'`);
```

### Whitelist
- Use stored procedures
- Validate input types
- Escape special characters

## Rate Limiting

```yaml
security:
  rate_limit:
    enabled: true
    requests_per_minute: 100
    burst: 20
```

## Audit Log

```sql
-- Query audit
SELECT * FROM system.audit_log
WHERE user = 'alice'
AND timestamp > NOW() - INTERVAL '1 day';
```

## Best Practices

1. Always use TLS in production
2. Use parameterized queries
3. Rotate passwords regularly
4. Enable audit logging
5. Implement IP allowlist
6. Use minimum required privileges
