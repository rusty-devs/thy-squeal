# Transactions

## Overview
Support for ACID transactions in thy-squeal, allowing multiple SQL statements to be executed as a single atomic unit.

## Implementation Plan

### 1. SQL Grammar & AST
Add the following statements to the grammar and AST:
- `BEGIN` or `START TRANSACTION`: Initiates a new transaction.
- `COMMIT`: Persists all changes made during the transaction.
- `ROLLBACK`: Discards all changes made during the transaction.

### 2. Session Management (HTTP API)
Since HTTP is stateless, multi-request transactions require a session or transaction ID:
- **`POST /_query`**: If a transaction is started, the response will include a `transaction_id`.
- **`QueryRequest`**: Will be updated to include an optional `transaction_id`.
- **Server State**: The server will maintain a map of active `transaction_id -> TransactionState`.
- **Timeout**: Active transactions will have a configurable timeout (default: 30s) after which they are automatically rolled back.

### 3. Execution Strategy (In-Memory)
For the initial implementation, we will use a **Copy-on-Write (CoW)** approach:
- **`BEGIN`**: Clones the current `DatabaseState` into a new `TransactionState` associated with a `transaction_id`.
- **DML Operations**: If a `transaction_id` is provided, the `Executor` applies changes to the cloned `DatabaseState` instead of the global one.
- **`COMMIT`**: 
    1. Acquire a write lock on the global `Database`.
    2. Replace the global `DatabaseState` with the `TransactionState`.
    3. Trigger a persistence snapshot (`db.save()`).
    4. Release the lock and remove the transaction session.
- **`ROLLBACK`**: Simply remove the transaction session.

### 4. Isolation Levels
- **Initial Support**: `Serializable` (via global locking on COMMIT) or `Read Committed` (depending on how we handle the initial clone).
- **Concurrency**: Since we are in-memory, we can start with a simple model where one transaction can be active at a time per session, but multiple read-only queries can still hit the global state.

## Example Workflow

1. **Start Transaction**
   ```bash
   POST /_query { "sql": "BEGIN" }
   # Response: { "success": true, "transaction_id": "tx_123" }
   ```

2. **Execute Statements**
   ```bash
   POST /_query { "sql": "INSERT INTO users...", "transaction_id": "tx_123" }
   POST /_query { "sql": "UPDATE account...", "transaction_id": "tx_123" }
   ```

3. **Commit**
   ```bash
   POST /_query { "sql": "COMMIT", "transaction_id": "tx_123" }
   ```
