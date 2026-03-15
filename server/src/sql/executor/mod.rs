pub mod aggregate;
pub mod ddl;
pub mod dispatch;
pub mod dml;
pub mod dump;
pub mod explain;
pub mod result;
pub mod search;
pub mod select;
#[cfg(test)]
mod tests;
pub mod tx;
pub mod user;

use super::error::{SqlError, SqlResult};
use crate::squeal;
use crate::squeal::{Select, Squeal};
use crate::storage::{Database, DatabaseState, Privilege, Row, Table, Value, WalRecord};
use dashmap::DashMap;
use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

pub use result::QueryResult;

/// A user session containing authentication and transaction state.
#[derive(Clone, Debug)]
pub struct Session {
    pub username: String,
    pub transaction_id: Option<String>,
}

impl Session {
    pub fn new(username: Option<String>, transaction_id: Option<String>) -> Self {
        Self {
            username: username.unwrap_or_else(|| "root".to_string()),
            transaction_id,
        }
    }

    pub fn root() -> Self {
        Self::new(None, None)
    }
}

/// Context for statement execution
pub struct ExecutionContext {
    pub params: Vec<Value>,
    pub session: Session,
}

impl ExecutionContext {
    pub fn new(params: Vec<Value>, session: Session) -> Self {
        Self { params, session }
    }
}

/// A builder-style plan for executing a SELECT query.
/// Reduces argument count in internal executor functions.
pub struct SelectQueryPlan<'a> {
    pub stmt: Select,
    pub outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    pub params: &'a [Value],
    pub db_state: &'a DatabaseState,
    pub session: Session,
}

impl<'a> SelectQueryPlan<'a> {
    pub fn new(stmt: Select, db_state: &'a DatabaseState, session: Session) -> Self {
        Self {
            stmt,
            outer_contexts: &[],
            params: &[],
            db_state,
            session,
        }
    }

    pub fn with_outer_contexts(
        mut self,
        contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    ) -> Self {
        self.outer_contexts = contexts;
        self
    }

    pub fn with_params(mut self, params: &'a [Value]) -> Self {
        self.params = params;
        self
    }
}

pub struct Executor {
    pub(crate) db: Arc<RwLock<Database>>,
    pub(crate) transactions: DashMap<String, DatabaseState>,
    pub(crate) prepared_statements: DashMap<String, Squeal>, // name -> stmt
    pub(crate) data_dir: Option<String>,
}

impl Executor {
    pub fn new(db: Arc<RwLock<Database>>) -> Self {
        Self {
            db,
            transactions: DashMap::new(),
            prepared_statements: DashMap::new(),
            data_dir: None,
        }
    }

    pub fn with_data_dir(mut self, data_dir: String) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: Vec<Value>,
        transaction_id: Option<String>,
        username: Option<String>,
    ) -> SqlResult<QueryResult> {
        // Workflow: SQL string -> AST (Pest) -> Squeal (IR) -> Executor
        let ast = super::parser::parse(sql)?;
        let squeal = Squeal::from(ast);
        self.exec_squeal(squeal, params, transaction_id, username)
            .await
    }

    pub async fn execute_squeal(
        &self,
        squeal: Squeal,
        params: Vec<Value>,
        transaction_id: Option<String>,
        username: Option<String>,
    ) -> SqlResult<QueryResult> {
        self.exec_squeal(squeal, params, transaction_id, username)
            .await
    }

    pub fn check_privilege(
        &self,
        username: &str,
        table: Option<&str>,
        privilege: Privilege,
        db_state: &DatabaseState,
    ) -> SqlResult<()> {
        let user = db_state
            .users
            .get(username)
            .ok_or_else(|| SqlError::Runtime(format!("User {} not found", username)))?;

        // root always has All
        if user.global_privileges.contains(&Privilege::All) {
            return Ok(());
        }

        if let Some(t) = table
            && let Some(privs) = user.table_privileges.get(t)
            && (privs.contains(&Privilege::All) || privs.contains(&privilege))
        {
            return Ok(());
        }

        if user.global_privileges.contains(&privilege) {
            return Ok(());
        }

        Err(SqlError::PermissionDenied(format!(
            "User {} does not have {:?} privilege{}",
            username,
            privilege,
            table
                .map(|t| format!(" on table {}", t))
                .unwrap_or_default()
        )))
    }

    pub fn refresh_materialized_views(&self, state: &mut DatabaseState) -> SqlResult<()> {
        let views = state.materialized_views.clone();
        for (name, query) in views {
            let plan = SelectQueryPlan::new(query, state, Session::root());
            let res = futures::executor::block_on(self.exec_select_recursive(plan))?;

            if let Some(table) = state.tables.get_mut(&name) {
                table.data.rows = res
                    .rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, values)| Row {
                        id: format!("mv_{}_{}", name, i),
                        values,
                    })
                    .collect();
            }
        }
        Ok(())
    }

    pub async fn kv_set(&self, key: String, value: Value, tx_id: Option<&str>) -> SqlResult<()> {
        self.mutate_state(tx_id, |state| {
            state.kv.insert(key.clone(), value.clone());
            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await?;

        let db = self.db.read().await;
        db.log_operation(&WalRecord::KvSet {
            tx_id: tx_id.map(|s| s.to_string()),
            key,
            value,
        })
        .map_err(SqlError::Storage)?;
        Ok(())
    }

    pub async fn kv_get(&self, key: &str, tx_id: Option<&str>) -> SqlResult<Option<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv.get(key).cloned())
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv.get(key).cloned())
        }
    }

    pub async fn kv_del(&self, key: String, tx_id: Option<&str>) -> SqlResult<()> {
        self.mutate_state(tx_id, |state| {
            state.kv.remove(&key);
            state.kv_expiry.remove(&key);
            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await?;

        let db = self.db.read().await;
        db.log_operation(&WalRecord::KvDelete {
            tx_id: tx_id.map(|s| s.to_string()),
            key,
        })
        .map_err(SqlError::Storage)?;
        Ok(())
    }

    pub async fn kv_exists(&self, key: &str, tx_id: Option<&str>) -> SqlResult<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            if let Some(expiry) = state.kv_expiry.get(key)
                && *expiry < now
            {
                return Ok(false);
            }
            Ok(state.kv.contains_key(key))
        } else {
            let db = self.db.read().await;
            let state = db.state();
            if let Some(expiry) = state.kv_expiry.get(key)
                && *expiry < now
            {
                return Ok(false);
            }
            Ok(state.kv.contains_key(key))
        }
    }

    pub async fn kv_expire(
        &self,
        key: String,
        seconds: u64,
        tx_id: Option<&str>,
    ) -> SqlResult<bool> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let expiry = now + (seconds * 1000);

        let exists = self.kv_exists(&key, tx_id).await?;
        if !exists {
            return Ok(false);
        }

        self.mutate_state(tx_id, |state| {
            state.kv_expiry.insert(key.clone(), expiry);
            Ok(())
        })
        .await?;

        let db = self.db.read().await;
        db.log_operation(&WalRecord::KvExpire {
            tx_id: tx_id.map(|s| s.to_string()),
            key,
            expiry,
        })
        .map_err(SqlError::Storage)?;
        Ok(true)
    }

    pub async fn kv_ttl(&self, key: &str, tx_id: Option<&str>) -> SqlResult<i64> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let (exists, expiry) = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            let exists = state.kv.contains_key(key);
            let expiry = state.kv_expiry.get(key).copied();
            (exists, expiry)
        } else {
            let db = self.db.read().await;
            let state = db.state();
            let exists = state.kv.contains_key(key);
            let expiry = state.kv_expiry.get(key).copied();
            (exists, expiry)
        };

        if !exists {
            return Ok(-2);
        }

        match expiry {
            Some(exp) if exp >= now => Ok(((exp - now) / 1000) as i64),
            Some(_) => Ok(-2),
            None => Ok(-1),
        }
    }

    pub async fn kv_keys(&self, pattern: &str, tx_id: Option<&str>) -> SqlResult<Vec<String>> {
        let keys: Vec<String> = if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            state.kv.keys().cloned().collect()
        } else {
            let db = self.db.read().await;
            db.state().kv.keys().cloned().collect()
        };

        if pattern.is_empty() || pattern == "*" {
            return Ok(keys);
        }

        let regex_pattern = pattern.replace("?", ".").replace("*", ".*");

        let re = regex::Regex::new(&format!("^{}$", regex_pattern))
            .map_err(|e| SqlError::Runtime(format!("Invalid pattern: {}", e)))?;

        Ok(keys.into_iter().filter(|k| re.is_match(k)).collect())
    }

    pub async fn kv_hash_set(
        &self,
        key: String,
        field: String,
        value: Value,
        tx_id: Option<&str>,
    ) -> SqlResult<()> {
        self.mutate_state(tx_id, |state| {
            state
                .kv_hash
                .entry(key)
                .or_insert_with(HashMap::new)
                .insert(field, value);
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn kv_hash_get(
        &self,
        key: &str,
        field: &str,
        tx_id: Option<&str>,
    ) -> SqlResult<Option<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_hash.get(key).and_then(|h| h.get(field).cloned()))
        } else {
            let db = self.db.read().await;
            Ok(db
                .state()
                .kv_hash
                .get(key)
                .and_then(|h| h.get(field).cloned()))
        }
    }

    pub async fn kv_hash_get_all(
        &self,
        key: &str,
        tx_id: Option<&str>,
    ) -> SqlResult<HashMap<String, Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_hash.get(key).cloned().unwrap_or_default())
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv_hash.get(key).cloned().unwrap_or_default())
        }
    }

    pub async fn kv_hash_del(
        &self,
        key: String,
        fields: Vec<String>,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = fields.len();
        self.mutate_state(tx_id, |state| {
            if let Some(hash) = state.kv_hash.get_mut(&key) {
                for field in fields {
                    hash.remove(&field);
                }
            }
            Ok(())
        })
        .await?;
        Ok(count)
    }

    pub async fn kv_list_push(
        &self,
        key: String,
        values: Vec<Value>,
        left: bool,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = values.len();
        self.mutate_state(tx_id, |state| {
            let list = state.kv_list.entry(key).or_insert_with(Vec::new);
            if left {
                let mut vals = values;
                vals.reverse();
                list.splice(0..0, vals);
            } else {
                list.extend(values);
            }
            Ok(())
        })
        .await?;
        Ok(count)
    }

    pub async fn kv_list_range(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        tx_id: Option<&str>,
    ) -> SqlResult<Vec<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            let list = state.kv_list.get(key).cloned().unwrap_or_default();
            Ok(Self::range_slice(&list, start, stop))
        } else {
            let db = self.db.read().await;
            let list = db.state().kv_list.get(key).cloned().unwrap_or_default();
            Ok(Self::range_slice(&list, start, stop))
        }
    }

    fn range_slice(list: &[Value], start: i64, stop: i64) -> Vec<Value> {
        let len = list.len() as i64;
        let start = if start < 0 { len + start } else { start };
        let stop = if stop < 0 { len + stop } else { stop };
        let start = start.max(0) as usize;
        let stop = (stop + 1).min(len) as usize;
        if start >= stop {
            return vec![];
        }
        list[start..stop].to_vec()
    }

    pub async fn kv_list_pop(
        &self,
        key: String,
        count: usize,
        left: bool,
        tx_id: Option<&str>,
    ) -> SqlResult<Vec<Value>> {
        let result = self
            .mutate_state(tx_id, |state| {
                let mut vals = vec![];
                if let Some(list) = state.kv_list.get_mut(&key) {
                    for _ in 0..count {
                        let val = if left {
                            if !list.is_empty() {
                                Some(list.remove(0))
                            } else {
                                None
                            }
                        } else {
                            list.pop()
                        };
                        if let Some(v) = val {
                            vals.push(v);
                        } else {
                            break;
                        }
                    }
                }
                Ok(vals)
            })
            .await?;
        Ok(result)
    }

    pub async fn kv_list_len(&self, key: &str, tx_id: Option<&str>) -> SqlResult<usize> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state.kv_list.get(key).map(|l| l.len()).unwrap_or(0))
        } else {
            let db = self.db.read().await;
            Ok(db.state().kv_list.get(key).map(|l| l.len()).unwrap_or(0))
        }
    }

    pub async fn kv_set_add(
        &self,
        key: String,
        members: Vec<String>,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            let set = state.kv_set.entry(key).or_insert_with(HashSet::new);
            for member in members {
                set.insert(member);
            }
            Ok(())
        })
        .await?;
        Ok(count)
    }

    pub async fn kv_set_members(&self, key: &str, tx_id: Option<&str>) -> SqlResult<Vec<String>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state
                .kv_set
                .get(key)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default())
        } else {
            let db = self.db.read().await;
            Ok(db
                .state()
                .kv_set
                .get(key)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default())
        }
    }

    pub async fn kv_set_is_member(
        &self,
        key: &str,
        member: &str,
        tx_id: Option<&str>,
    ) -> SqlResult<bool> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            Ok(state
                .kv_set
                .get(key)
                .map(|s| s.contains(member))
                .unwrap_or(false))
        } else {
            let db = self.db.read().await;
            Ok(db
                .state()
                .kv_set
                .get(key)
                .map(|s| s.contains(member))
                .unwrap_or(false))
        }
    }

    pub async fn kv_set_remove(
        &self,
        key: String,
        members: Vec<String>,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            if let Some(set) = state.kv_set.get_mut(&key) {
                for member in members {
                    set.remove(&member);
                }
            }
            Ok(())
        })
        .await?;
        Ok(count)
    }

    pub async fn kv_zset_add(
        &self,
        key: String,
        members: Vec<(f64, String)>,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            let zset = state.kv_zset.entry(key).or_insert_with(Vec::new);
            for (score, member) in members {
                if let Some(existing) = zset.iter_mut().find(|(_, m)| *m == member) {
                    existing.0 = score;
                } else {
                    zset.push((score, member));
                }
            }
            zset.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            Ok(())
        })
        .await?;
        Ok(count)
    }

    pub async fn kv_zset_range(
        &self,
        key: &str,
        start: i64,
        stop: i64,
        with_scores: bool,
        tx_id: Option<&str>,
    ) -> SqlResult<Vec<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            let zset = state.kv_zset.get(key).cloned().unwrap_or_default();
            Ok(Self::zset_range(zset, start, stop, with_scores))
        } else {
            let db = self.db.read().await;
            let zset = db.state().kv_zset.get(key).cloned().unwrap_or_default();
            Ok(Self::zset_range(zset, start, stop, with_scores))
        }
    }

    pub async fn kv_zsetrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        with_scores: bool,
        tx_id: Option<&str>,
    ) -> SqlResult<Vec<Value>> {
        if let Some(id) = tx_id {
            let state = self
                .transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            let zset = state.kv_zset.get(key).cloned().unwrap_or_default();
            Ok(Self::zset_filter_by_score(zset, min, max, with_scores))
        } else {
            let db = self.db.read().await;
            let zset = db.state().kv_zset.get(key).cloned().unwrap_or_default();
            Ok(Self::zset_filter_by_score(zset, min, max, with_scores))
        }
    }

    fn zset_range(
        zset: Vec<(f64, String)>,
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Vec<Value> {
        let len = zset.len() as i64;
        let start = start.max(0) as usize;
        let stop = if stop < 0 {
            len as usize
        } else {
            stop as usize
        };

        let mut result = vec![];
        for (i, (score, member)) in zset.into_iter().enumerate() {
            if i >= start && i <= stop {
                result.push(Value::Text(member));
                if with_scores {
                    result.push(Value::Float(score));
                }
            }
            if i > stop {
                break;
            }
        }
        result
    }

    fn zset_filter_by_score(
        zset: Vec<(f64, String)>,
        min: f64,
        max: f64,
        with_scores: bool,
    ) -> Vec<Value> {
        let mut result = vec![];
        for (score, member) in zset {
            if score >= min && score <= max {
                result.push(Value::Text(member));
                if with_scores {
                    result.push(Value::Float(score));
                }
            }
        }
        result
    }

    pub async fn kv_zset_remove(
        &self,
        key: String,
        members: Vec<String>,
        tx_id: Option<&str>,
    ) -> SqlResult<usize> {
        let count = members.len();
        self.mutate_state(tx_id, |state| {
            if let Some(zset) = state.kv_zset.get_mut(&key) {
                zset.retain(|(_, m)| !members.contains(m));
            }
            Ok(())
        })
        .await?;
        Ok(count)
    }

    pub async fn exec_kv_set(
        &self,
        kv: squeal::KvSet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let key = kv.key.clone();
        self.kv_set(kv.key, kv.value, tx_id).await?;
        if let Some(exp) = kv.expiry {
            self.kv_expire(key, exp, tx_id).await?;
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_get(
        &self,
        kv: squeal::KvGet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let value = self.kv_get(&kv.key, tx_id).await?;
        let row = match &value {
            Some(v) => vec![v.clone()],
            None => vec![],
        };
        Ok(QueryResult {
            columns: vec!["value".to_string()],
            rows: if value.is_some() { vec![row] } else { vec![] },
            rows_affected: 0,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_del(
        &self,
        kv: squeal::KvDel,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let mut count = 0;
        for key in kv.keys {
            if self.kv_get(&key, tx_id).await?.is_some() {
                self.kv_del(key, tx_id).await?;
                count += 1;
            }
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_hash_set(
        &self,
        kv: squeal::KvHashSet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        self.kv_hash_set(kv.key, kv.field, kv.value, tx_id).await?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_hash_get(
        &self,
        kv: squeal::KvHashGet,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let value = self.kv_hash_get(&kv.key, &kv.field, tx_id).await?;
        let row = match &value {
            Some(v) => vec![v.clone()],
            None => vec![],
        };
        Ok(QueryResult {
            columns: vec!["value".to_string()],
            rows: if value.is_some() { vec![row] } else { vec![] },
            rows_affected: 0,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_list_push(
        &self,
        kv: squeal::KvListPush,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let count = self.kv_list_push(kv.key, kv.values, kv.left, tx_id).await?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count as u64,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_list_range(
        &self,
        kv: squeal::KvListRange,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let values = self
            .kv_list_range(&kv.key, kv.start, kv.stop, tx_id)
            .await?;
        let rows: Vec<Vec<Value>> = values.into_iter().map(|v| vec![v]).collect();
        Ok(QueryResult {
            columns: vec!["value".to_string()],
            rows,
            rows_affected: 0,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_set_add(
        &self,
        kv: squeal::KvSetAdd,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let count = self.kv_set_add(kv.key, kv.members, tx_id).await?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count as u64,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_set_members(
        &self,
        kv: squeal::KvSetMembers,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let members = self.kv_set_members(&kv.key, tx_id).await?;
        let rows: Vec<Vec<Value>> = members.into_iter().map(|m| vec![Value::Text(m)]).collect();
        Ok(QueryResult {
            columns: vec!["member".to_string()],
            rows,
            rows_affected: 0,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_zset_add(
        &self,
        kv: squeal::KvZSetAdd,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let count = self.kv_zset_add(kv.key, kv.members, tx_id).await?;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count as u64,
            transaction_id: None,
        })
    }

    pub async fn exec_kv_zset_range(
        &self,
        kv: squeal::KvZSetRange,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let values = self
            .kv_zset_range(&kv.key, kv.start, kv.stop, kv.with_scores, tx_id)
            .await?;
        let rows: Vec<Vec<Value>> = values
            .chunks(if kv.with_scores { 2 } else { 1 })
            .map(|chunk| chunk.to_vec())
            .collect();
        Ok(QueryResult {
            columns: if kv.with_scores {
                vec!["member".to_string(), "score".to_string()]
            } else {
                vec!["member".to_string()]
            },
            rows,
            rows_affected: 0,
            transaction_id: None,
        })
    }
}

impl crate::sql::eval::Evaluator for Executor {
    fn exec_select_internal<'a>(
        &'a self,
        stmt: Select,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        let plan = SelectQueryPlan::new(stmt, db_state, Session::root())
            .with_outer_contexts(outer_contexts)
            .with_params(params);
        self.exec_select_recursive(plan)
    }
}
