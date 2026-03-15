#[cfg(test)]
mod tests {
    use crate::sql::executor::Executor;
    use crate::squeal::{Expression, Insert, Select, SelectColumn, Squeal};
    use crate::squeal::{
        KvDel, KvGet, KvHashGet, KvHashSet, KvListPush, KvListRange, KvSet, KvSetAdd, KvSetMembers,
        KvZSetAdd, KvZSetRange,
    };
    use crate::storage::{Database, Value};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_jsqueal_select_simple() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        // CREATE TABLE users (id INT, name TEXT)
        executor
            .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None, None)
            .await
            .unwrap();
        executor
            .execute(
                "INSERT INTO users (id, name) VALUES (1, 'Alice')",
                vec![],
                None,
                None,
            )
            .await
            .unwrap();

        // SELECT name FROM users WHERE id = 1
        let query = Squeal::Select(Select {
            with_clause: None,
            columns: vec![SelectColumn {
                expr: Expression::Column("name".to_string()),
                alias: None,
            }],
            table: "users".to_string(),
            table_alias: None,
            distinct: false,
            joins: vec![],
            where_clause: Some(crate::squeal::Condition::Comparison(
                Expression::Column("id".to_string()),
                crate::squeal::ComparisonOp::Eq,
                Expression::Literal(Value::Int(1)),
            )),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
        });

        let result = executor
            .execute_squeal(query, vec![], None, None)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_jsqueal_insert_and_select() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        executor
            .execute(
                "CREATE TABLE items (id INT, price FLOAT)",
                vec![],
                None,
                None,
            )
            .await
            .unwrap();

        // INSERT INTO items (id, price) VALUES (10, 19.99)
        let insert = Squeal::Insert(Insert {
            table: "items".to_string(),
            columns: Some(vec!["id".to_string(), "price".to_string()]),
            values: vec![
                Expression::Literal(Value::Int(10)),
                Expression::Literal(Value::Float(19.99)),
            ],
        });

        executor
            .execute_squeal(insert, vec![], None, None)
            .await
            .unwrap();

        let result = executor
            .execute("SELECT price FROM items WHERE id = 10", vec![], None, None)
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Float(19.99));
    }

    #[tokio::test]
    async fn test_jsqueal_kv_set_get() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        let kv_set = Squeal::KvSet(KvSet {
            key: "mykey".to_string(),
            value: Value::Text("myvalue".to_string()),
            expiry: None,
        });
        executor
            .execute_squeal(kv_set, vec![], None, None)
            .await
            .unwrap();

        let kv_get = Squeal::KvGet(KvGet {
            key: "mykey".to_string(),
        });
        let result = executor
            .execute_squeal(kv_get, vec![], None, None)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("myvalue".to_string()));
    }

    #[tokio::test]
    async fn test_jsqueal_kv_hash() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        let kv_set = Squeal::KvHashSet(KvHashSet {
            key: "myhash".to_string(),
            field: "field1".to_string(),
            value: Value::Text("value1".to_string()),
        });
        executor
            .execute_squeal(kv_set, vec![], None, None)
            .await
            .unwrap();

        let kv_get = Squeal::KvHashGet(KvHashGet {
            key: "myhash".to_string(),
            field: "field1".to_string(),
        });
        let result = executor
            .execute_squeal(kv_get, vec![], None, None)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Text("value1".to_string()));
    }

    #[tokio::test]
    async fn test_jsqueal_kv_list() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        let kv_push = Squeal::KvListPush(KvListPush {
            key: "mylist".to_string(),
            values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
            left: false,
        });
        executor
            .execute_squeal(kv_push, vec![], None, None)
            .await
            .unwrap();

        let kv_range = Squeal::KvListRange(KvListRange {
            key: "mylist".to_string(),
            start: 0,
            stop: -1,
        });
        let result = executor
            .execute_squeal(kv_range, vec![], None, None)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_jsqueal_kv_set() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        let kv_add = Squeal::KvSetAdd(KvSetAdd {
            key: "myset".to_string(),
            members: vec!["member1".to_string(), "member2".to_string()],
        });
        executor
            .execute_squeal(kv_add, vec![], None, None)
            .await
            .unwrap();

        let kv_members = Squeal::KvSetMembers(KvSetMembers {
            key: "myset".to_string(),
        });
        let result = executor
            .execute_squeal(kv_members, vec![], None, None)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_jsqueal_kv_zset() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Executor::new(db);

        let kv_add = Squeal::KvZSetAdd(KvZSetAdd {
            key: "myzset".to_string(),
            members: vec![
                (1.0, "a".to_string()),
                (2.0, "b".to_string()),
                (3.0, "c".to_string()),
            ],
        });
        executor
            .execute_squeal(kv_add, vec![], None, None)
            .await
            .unwrap();

        let kv_range = Squeal::KvZSetRange(KvZSetRange {
            key: "myzset".to_string(),
            start: 0,
            stop: -1,
            with_scores: false,
        });
        let result = executor
            .execute_squeal(kv_range, vec![], None, None)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 3);
    }
}
