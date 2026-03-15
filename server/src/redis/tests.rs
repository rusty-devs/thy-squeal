#[cfg(test)]
mod tests {
    use crate::redis::resp::{RespValue, read_value};
    use crate::sql::Executor;
    use crate::storage::{Database, Value};
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_resp_simple_string() {
        let mut data = Cursor::new(b"+OK\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(val, RespValue::SimpleString("OK".to_string()));

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b"+OK\r\n");
    }

    #[tokio::test]
    async fn test_resp_integer() {
        let mut data = Cursor::new(b":1000\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(val, RespValue::Integer(1000));

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b":1000\r\n");
    }

    #[tokio::test]
    async fn test_resp_bulk_string() {
        let mut data = Cursor::new(b"$6\r\nfoobar\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(val, RespValue::BulkString(Some(b"foobar".to_vec())));

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b"$6\r\nfoobar\r\n");
    }

    #[tokio::test]
    async fn test_resp_array() {
        let mut data = Cursor::new(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let val = read_value(&mut data).await.unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"foo".to_vec())),
                RespValue::BulkString(Some(b"bar".to_vec()))
            ]))
        );

        let mut buf = Vec::new();
        val.write(&mut buf).await.unwrap();
        assert_eq!(buf, b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[tokio::test]
    async fn test_kv_set_get() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_set("key1".to_string(), Value::Text("value1".to_string()), None)
            .await
            .unwrap();
        let result = executor.kv_get("key1", None).await.unwrap();

        assert_eq!(result, Some(Value::Text("value1".to_string())));
    }

    #[tokio::test]
    async fn test_kv_exists() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_set("key1".to_string(), Value::Text("value1".to_string()), None)
            .await
            .unwrap();

        let exists = executor.kv_exists("key1", None).await.unwrap();
        assert!(exists);

        let not_exists = executor.kv_exists("nonexistent", None).await.unwrap();
        assert!(!not_exists);
    }

    #[tokio::test]
    async fn test_kv_expire_ttl() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_set("key1".to_string(), Value::Text("value1".to_string()), None)
            .await
            .unwrap();

        let ttl_before = executor.kv_ttl("key1", None).await.unwrap();
        assert_eq!(ttl_before, -1);

        executor
            .kv_expire("key1".to_string(), 10, None)
            .await
            .unwrap();

        let ttl_after = executor.kv_ttl("key1", None).await.unwrap();
        assert!(ttl_after >= 0 && ttl_after <= 10);
    }

    #[tokio::test]
    async fn test_kv_keys() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_set("foo1".to_string(), Value::Text("v1".to_string()), None)
            .await
            .unwrap();
        executor
            .kv_set("foo2".to_string(), Value::Text("v2".to_string()), None)
            .await
            .unwrap();
        executor
            .kv_set("bar1".to_string(), Value::Text("v3".to_string()), None)
            .await
            .unwrap();

        let keys = executor.kv_keys("*", None).await.unwrap();
        assert_eq!(keys.len(), 3);

        let foo_keys = executor.kv_keys("foo*", None).await.unwrap();
        assert_eq!(foo_keys.len(), 2);
    }

    #[tokio::test]
    async fn test_kv_hash() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_hash_set(
                "myhash".to_string(),
                "field1".to_string(),
                Value::Text("value1".to_string()),
                None,
            )
            .await
            .unwrap();
        executor
            .kv_hash_set(
                "myhash".to_string(),
                "field2".to_string(),
                Value::Text("value2".to_string()),
                None,
            )
            .await
            .unwrap();

        let val = executor
            .kv_hash_get("myhash", "field1", None)
            .await
            .unwrap();
        assert_eq!(val, Some(Value::Text("value1".to_string())));

        let all = executor.kv_hash_get_all("myhash", None).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_kv_list() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_list_push(
                "mylist".to_string(),
                vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
                false,
                None,
            )
            .await
            .unwrap();

        let range = executor.kv_list_range("mylist", 0, -1, None).await.unwrap();
        assert_eq!(range.len(), 2);

        let len = executor.kv_list_len("mylist", None).await.unwrap();
        assert_eq!(len, 2);
    }

    #[tokio::test]
    async fn test_kv_set() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_set_add(
                "myset".to_string(),
                vec!["member1".to_string(), "member2".to_string()],
                None,
            )
            .await
            .unwrap();

        let members = executor.kv_set_members("myset", None).await.unwrap();
        assert_eq!(members.len(), 2);

        let is_member = executor
            .kv_set_is_member("myset", "member1", None)
            .await
            .unwrap();
        assert!(is_member);

        let not_member = executor
            .kv_set_is_member("myset", "member3", None)
            .await
            .unwrap();
        assert!(!not_member);
    }

    #[tokio::test]
    async fn test_kv_zset() {
        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        executor
            .kv_zset_add(
                "myzset".to_string(),
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
                None,
            )
            .await
            .unwrap();

        let range = executor
            .kv_zset_range("myzset", 0, -1, false, None)
            .await
            .unwrap();
        assert_eq!(range.len(), 3);

        let with_scores = executor
            .kv_zset_range("myzset", 0, -1, true, None)
            .await
            .unwrap();
        assert_eq!(with_scores.len(), 6);

        let by_score = executor
            .kv_zsetrangebyscore("myzset", 1.0, 2.0, false, None)
            .await
            .unwrap();
        assert_eq!(by_score.len(), 2);
    }

    #[tokio::test]
    async fn test_kv_stream() {
        use std::collections::HashMap;

        let db = Arc::new(RwLock::new(Database::new()));
        let executor = Arc::new(Executor::new(db));

        let mut fields1 = HashMap::new();
        fields1.insert("field1".to_string(), Value::Text("value1".to_string()));

        let id1 = executor
            .kv_stream_add("mystream".to_string(), None, fields1, None)
            .await
            .unwrap();
        assert_eq!(id1, "1");

        let mut fields2 = HashMap::new();
        fields2.insert("field2".to_string(), Value::Text("value2".to_string()));

        let id2 = executor
            .kv_stream_add("mystream".to_string(), None, fields2, None)
            .await
            .unwrap();
        assert_eq!(id2, "2");

        let range = executor
            .kv_stream_range("mystream", "-", "+", None, None)
            .await
            .unwrap();
        assert_eq!(range.len(), 2);

        let len = executor.kv_stream_len("mystream", None).await.unwrap();
        assert_eq!(len, 2);
    }
}
