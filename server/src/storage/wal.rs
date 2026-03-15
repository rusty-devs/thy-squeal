use crate::sql::eval::{Evaluator, RecoveryEvaluator};
use crate::storage::{DatabaseState, StorageError, Table, WalRecord};
use std::collections::HashMap;

pub fn replay_logs(state: &mut DatabaseState, logs: Vec<WalRecord>) -> Result<(), StorageError> {
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
                    WalRecord::CreateMaterializedView { tx_id, .. } => tx_id,
                    WalRecord::AlterTable { tx_id, .. } => tx_id,
                    WalRecord::DropTable { tx_id, .. } => tx_id,
                    WalRecord::Insert { tx_id, .. } => tx_id,
                    WalRecord::Update { tx_id, .. } => tx_id,
                    WalRecord::Delete { tx_id, .. } => tx_id,
                    WalRecord::CreateIndex { tx_id, .. } => tx_id,
                    WalRecord::KvSet { tx_id, .. } => tx_id,
                    WalRecord::KvDelete { tx_id, .. } => tx_id,
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
        WalRecord::CreateTable {
            name,
            columns,
            primary_key,
            foreign_keys,
            ..
        } => {
            state.tables.insert(
                name.clone(),
                Table::new(name, columns, primary_key, foreign_keys),
            );
        }
        WalRecord::CreateMaterializedView { name, query, .. } => {
            state.materialized_views.insert(name, *query);
        }
        WalRecord::AlterTable { table, action, .. } => {
            use crate::squeal::AlterAction;
            if let Some(t) = state.get_table_mut(&table) {
                match action {
                    AlterAction::AddColumn(col) => t.add_column(col)?,
                    AlterAction::DropColumn(name) => t.drop_column(&name)?,
                    AlterAction::RenameColumn { old_name, new_name } => {
                        t.rename_column(&old_name, &new_name)?
                    }
                    AlterAction::RenameTable(new_name) => {
                        let mut t = state.tables.remove(&table).unwrap();
                        t.rename_table(new_name.clone());
                        state.tables.insert(new_name, t);
                    }
                }
            }
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
            table, id, values, ..
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
        WalRecord::KvSet { key, value, .. } => {
            state.kv.insert(key, value);
        }
        WalRecord::KvDelete { key, .. } => {
            state.kv.remove(&key);
        }
        WalRecord::KvExpire { key, expiry, .. } => {
            state.kv_expiry.insert(key, expiry);
        }
        WalRecord::KvHashSet {
            key, field, value, ..
        } => {
            state.kv_hash.entry(key).or_default().insert(field, value);
        }
        WalRecord::KvHashDelete { key, fields, .. } => {
            if let Some(hash) = state.kv_hash.get_mut(&key) {
                for field in fields {
                    hash.remove(&field);
                }
            }
        }
        WalRecord::KvListPush {
            key, values, left, ..
        } => {
            let list = state.kv_list.entry(key).or_default();
            if left {
                let mut vals = values;
                vals.reverse();
                list.splice(0..0, vals);
            } else {
                list.extend(values);
            }
        }
        WalRecord::KvSetAdd { key, members, .. } => {
            let set = state.kv_set.entry(key).or_default();
            for member in members {
                set.insert(member);
            }
        }
        WalRecord::KvSetRemove { key, members, .. } => {
            if let Some(set) = state.kv_set.get_mut(&key) {
                for member in members {
                    set.remove(&member);
                }
            }
        }
        WalRecord::KvZSetAdd { key, members, .. } => {
            let zset = state.kv_zset.entry(key).or_default();
            for (score, member) in members {
                if let Some(existing) = zset.iter_mut().find(|(_, m)| *m == member) {
                    existing.0 = score;
                } else {
                    zset.push((score, member));
                }
            }
            zset.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        }
        WalRecord::KvZSetRemove { key, members, .. } => {
            if let Some(zset) = state.kv_zset.get_mut(&key) {
                zset.retain(|(_, m)| !members.contains(m));
            }
        }
        WalRecord::KvStreamAdd {
            key, id, fields, ..
        } => {
            let stream = state.kv_stream.entry(key.clone()).or_default();
            let last_id = state.kv_stream_last_id.entry(key).or_default();
            *last_id = id;
            stream.push((id, fields));
        }
        _ => {}
    }
    Ok(())
}
