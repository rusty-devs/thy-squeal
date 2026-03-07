use crate::sql::eval::{Evaluator, RecoveryEvaluator};
use crate::storage::{DatabaseState, Table, WalRecord, StorageError};
use std::collections::HashMap;

pub fn replay_logs(
    state: &mut DatabaseState,
    logs: Vec<WalRecord>,
) -> Result<(), StorageError> {
    if logs.is_empty() {
        return Ok(());
    }

    tracing::info!("Replaying {} WAL records", logs.len());
    let recovery_eval = RecoveryEvaluator;

    // Buffer for in-progress transactions
    let mut pending_txs: HashMap<String, Vec<WalRecord>> = HashMap::new();

    for record in logs {
        match record {
            WalRecord::Begin { tx_id } => {
                pending_txs.insert(tx_id, Vec::new());
            }
            WalRecord::Commit { tx_id } => {
                if let Some(records) = pending_txs.remove(&tx_id) {
                    for r in records {
                        apply_record(state, &recovery_eval, r)?;
                    }
                }
            }
            WalRecord::Rollback { tx_id } => {
                pending_txs.remove(&tx_id);
            }
            r => {
                // Check if it's part of a transaction
                let tx_id_opt = match &r {
                    WalRecord::CreateTable { tx_id, .. } => tx_id,
                    WalRecord::DropTable { tx_id, .. } => tx_id,
                    WalRecord::Insert { tx_id, .. } => tx_id,
                    WalRecord::Update { tx_id, .. } => tx_id,
                    WalRecord::Delete { tx_id, .. } => tx_id,
                    WalRecord::CreateIndex { tx_id, .. } => tx_id,
                    _ => &None,
                };

                if let Some(id) = tx_id_opt {
                    if let Some(v) = pending_txs.get_mut(id) {
                        v.push(r);
                    }
                } else {
                    // Autocommit record
                    apply_record(state, &recovery_eval, r)?;
                }
            }
        }
    }
    Ok(())
}

pub fn apply_record(
    state: &mut DatabaseState,
    evaluator: &dyn Evaluator,
    record: WalRecord,
) -> Result<(), StorageError> {
    match record {
        WalRecord::CreateTable { name, columns, .. } => {
            state.tables.insert(name.clone(), Table::new(name, columns));
        }
        WalRecord::DropTable { name, .. } => {
            state.tables.remove(&name);
        }
        WalRecord::Insert { table, values, .. } => {
            let db_state = state.clone();
            if let Some(t) = state.get_table_mut(&table) {
                t.insert(evaluator, values, &db_state)?;
            }
        }
        WalRecord::Update {
            table,
            id,
            values,
            ..
        } => {
            let db_state = state.clone();
            if let Some(t) = state.get_table_mut(&table) {
                t.update(evaluator, &id, values, &db_state)?;
            }
        }
        WalRecord::Delete { table, id, .. } => {
            let db_state = state.clone();
            if let Some(t) = state.get_table_mut(&table) {
                t.delete(evaluator, &id, &db_state)?;
            }
        }
        WalRecord::CreateIndex {
            table,
            name,
            expressions,
            unique,
            use_hash,
            where_clause,
            ..
        } => {
            let db_state = state.clone();
            if let Some(t) = state.get_table_mut(&table) {
                t.create_index(
                    evaluator,
                    name,
                    expressions,
                    unique,
                    use_hash,
                    where_clause,
                    &db_state,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}
