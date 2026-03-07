# JavaScript REPL Client

## Overview
Interactive CLI client with embedded JavaScript runtime for scripting.

## Binary
`thy-squeal-client` - CLI tool with JS REPL

## Installation
```bash
cargo install thy-squeal-client
```

## Usage

```bash
# Interactive REPL
thy-squeal-client

# Execute SQL
thy-squeal-client -h localhost -p 3306 -e "SELECT * FROM users"

# HTTP mode
thy-squeal-client --http localhost:9200 -e "SELECT * FROM users"

# Run script
thy-squeal-client script.js

# Import/Export
thy-squeal-client --import data.json
thy-squeal-client --export data.json
```

## JavaScript API

### Connection
```javascript
const thy = require('thy-squeal');

// TCP SQL connection
const conn = thy.connect('thy-sql://localhost:3306');

// HTTP connection
const conn = thy.connect('thy-http://localhost:9200');
```

### Query
```javascript
// Simple query
const result = conn.query('SELECT * FROM users');

// With parameters
const result = conn.query('SELECT * FROM users WHERE age > ?', [18]);

// Get rows
for (const row of result.rows) {
  console.log(row.name, row.age);
}

// Get affected count
console.log(`Deleted ${result.affected} rows`);
```

### Key-Value
```javascript
// Set value
thy.kv.set('session:123', { user: 'alice', exp: 3600 });

// Get value
const session = thy.kv.get('session:123');

// Delete
thy.kv.del('session:123');

// Increment
thy.kv.incr('counter');
```

### Full-Text Search
```javascript
const hits = conn.search('users', 'developer', {
  fields: ['name', 'bio'],
  limit: 10
});

for (const hit of hits) {
  console.log(hit.id, hit.score, hit.data);
}
```

### Transaction (Future)
```javascript
const tx = conn.begin();
try {
  tx.query('INSERT INTO orders VALUES (?, ?)', [1, 100]);
  tx.query('UPDATE stock SET qty = qty - 1 WHERE id = ?', [5]);
  tx.commit();
} catch (e) {
  tx.rollback();
}
```

## REPL Features

### Commands
```
.help           Show help
.load script.js Load and execute JS file
.quit           Exit
.clear          Clear screen
.tables         List tables
```

### Keyboard Shortcuts
- `Ctrl+C` - Cancel current input
- `Ctrl+D` - Exit REPL
- `Up/Down` - History navigation
- `Tab` - Autocomplete

### Configuration
```yaml
# ~/.thy-squeal/config.yaml
connection:
  default_host: "localhost"
  default_port: 9200

repl:
  history_size: 1000
  auto_indent: true
  prompt: "thy> "
```

## Script Example
```javascript
// batch.js - Import users from JSON file
const fs = require('fs');
const data = JSON.parse(fs.readFileSync('users.json'));

for (const user of data) {
  conn.query(
    'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
    [user.name, user.email, user.age]
  );
}

console.log(`Imported ${data.length} users`);
```
