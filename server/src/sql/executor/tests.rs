#[cfg(test)]
mod tests {
    use crate::sql::{Executor, SqlError};

    #[tokio::test]
    async fn test_create_table_insert_select() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (a INT, b TEXT, c FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO t (a, b, c) VALUES (1, 'hello', 3.14)")
            .await
            .unwrap();

        let r = exec.execute("SELECT * FROM t").await.unwrap();
        assert_eq!(r.columns, vec!["a", "b", "c"]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
        assert_eq!(r.rows[0][1].as_text(), Some("hello"));
    }

    #[tokio::test]
    async fn test_select_columns() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (a INT, b TEXT, c FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO t (a, b, c) VALUES (1, 'hello', 3.14)")
            .await
            .unwrap();

        let r = exec.execute("SELECT a, c FROM t").await.unwrap();
        assert_eq!(r.columns, vec!["a", "c"]);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].len(), 2);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
    }

    #[tokio::test]
    async fn test_select_where() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE users (id INT, name TEXT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (2, 'bob')")
            .await
            .unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (3, 'charlie')")
            .await
            .unwrap();

        let r = exec.execute("SELECT * FROM users WHERE id = 2").await.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1].as_text(), Some("bob"));

        let r = exec.execute("SELECT name FROM users WHERE id > 1").await.unwrap();
        assert_eq!(r.rows.len(), 2);

        let r = exec.execute("SELECT * FROM users WHERE name = 'alice'").await.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
    }

    #[tokio::test]
    async fn test_update() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (id INT, v TEXT)").await.unwrap();
        exec.execute("INSERT INTO t (id, v) VALUES (1, 'old')").await.unwrap();
        
        let r = exec.execute("UPDATE t SET v = 'new' WHERE id = 1").await.unwrap();
        assert_eq!(r.rows_affected, 1);
        
        let r = exec.execute("SELECT v FROM t WHERE id = 1").await.unwrap();
        assert_eq!(r.rows[0][0].as_text(), Some("new"));
    }

    #[tokio::test]
    async fn test_delete() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (id INT)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (2)").await.unwrap();
        
        let r = exec.execute("DELETE FROM t WHERE id = 1").await.unwrap();
        assert_eq!(r.rows_affected, 1);
        
        let r = exec.execute("SELECT * FROM t").await.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_int(), Some(2));
    }

    #[tokio::test]
    async fn test_order_by() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (id INT)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (3)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
        exec.execute("INSERT INTO t (id) VALUES (2)").await.unwrap();

        let r = exec.execute("SELECT * FROM t ORDER BY id ASC").await.unwrap();
        assert_eq!(r.rows[0][0].as_int(), Some(1));
        assert_eq!(r.rows[1][0].as_int(), Some(2));
        assert_eq!(r.rows[2][0].as_int(), Some(3));

        let r = exec.execute("SELECT * FROM t ORDER BY id DESC").await.unwrap();
        assert_eq!(r.rows[0][0].as_int(), Some(3));
        assert_eq!(r.rows[1][0].as_int(), Some(2));
        assert_eq!(r.rows[2][0].as_int(), Some(1));
    }

    #[tokio::test]
    async fn test_limit_offset() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (id INT)").await.unwrap();
        for i in 1..=10 {
            exec.execute(&format!("INSERT INTO t (id) VALUES ({})", i))
                .await
                .unwrap();
        }

        let r = exec.execute("SELECT * FROM t ORDER BY id ASC LIMIT 3")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0].as_int(), Some(1));
        assert_eq!(r.rows[2][0].as_int(), Some(3));

        let r = exec.execute("SELECT * FROM t ORDER BY id ASC LIMIT 3 OFFSET 2")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0].as_int(), Some(3));
        assert_eq!(r.rows[2][0].as_int(), Some(5));
    }

    #[tokio::test]
    async fn test_aggregations_and_aliases() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE sales (id INT, amount FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO sales (id, amount) VALUES (1, 100.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO sales (id, amount) VALUES (2, 200.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO sales (id, amount) VALUES (3, 150.0)")
            .await
            .unwrap();

        let r = exec.execute("SELECT COUNT(*) AS total_count, SUM(amount) AS total_amount FROM sales").await.unwrap();
        assert_eq!(r.columns, vec!["total_count", "total_amount"]);
        assert_eq!(r.rows[0][0].as_int(), Some(3));
        assert_eq!(r.rows[0][1].as_float(), Some(450.0));

        let r = exec.execute("SELECT MIN(amount), MAX(amount), AVG(amount) FROM sales").await.unwrap();
        assert_eq!(r.rows[0][0].as_float(), Some(100.0));
        assert_eq!(r.rows[0][1].as_float(), Some(200.0));
        assert_eq!(r.rows[0][2].as_float(), Some(150.0));
    }

    #[tokio::test]
    async fn test_group_by_having() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE orders (id INT, customer TEXT, amount FLOAT)")
            .await
            .unwrap();
        exec.execute("INSERT INTO orders (id, customer, amount) VALUES (1, 'alice', 100.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO orders (id, customer, amount) VALUES (2, 'bob', 200.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO orders (id, customer, amount) VALUES (3, 'alice', 50.0)")
            .await
            .unwrap();
        exec.execute("INSERT INTO orders (id, customer, amount) VALUES (4, 'charlie', 300.0)")
            .await
            .unwrap();

        let r = exec.execute("SELECT customer, SUM(amount) FROM orders GROUP BY customer ORDER BY customer ASC")
            .await
            .unwrap();
        
        assert_eq!(r.rows.len(), 3);
        
        let alice = r.rows.iter().find(|row| row[0].as_text() == Some("alice")).unwrap();
        assert_eq!(alice[1].as_float(), Some(150.0));

        let r = exec.execute("SELECT customer, SUM(amount) FROM orders GROUP BY customer HAVING SUM(amount) > 200")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_text(), Some("charlie"));
        assert_eq!(r.rows[0][1].as_float(), Some(300.0));
    }

    #[tokio::test]
    async fn test_inner_join() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE users (id INT, name TEXT)").await.unwrap();
        exec.execute("CREATE TABLE posts (id INT, user_id INT, title TEXT)").await.unwrap();
        
        // Create index on user_id
        exec.execute("CREATE INDEX idx_user_id ON posts (user_id)").await.unwrap();
        
        exec.execute("INSERT INTO users (id, name) VALUES (1, 'alice')").await.unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (2, 'bob')").await.unwrap();
        
        exec.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'p1')").await.unwrap();
        exec.execute("INSERT INTO posts (id, user_id, title) VALUES (11, 1, 'p2')").await.unwrap();
        exec.execute("INSERT INTO posts (id, user_id, title) VALUES (12, 2, 'p3')").await.unwrap();

        let r = exec.execute("SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id ORDER BY posts.id ASC")
            .await
            .unwrap();
        
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0].as_text(), Some("alice"));
        assert_eq!(r.rows[0][1].as_text(), Some("p1"));
        assert_eq!(r.rows[2][0].as_text(), Some("bob"));
        assert_eq!(r.rows[2][1].as_text(), Some("p3"));
    }

    #[tokio::test]
    async fn test_left_join() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE users (id INT, name TEXT)").await.unwrap();
        exec.execute("CREATE TABLE posts (user_id INT, title TEXT)").await.unwrap();
        
        exec.execute("INSERT INTO users (id, name) VALUES (1, 'alice')").await.unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (2, 'bob')").await.unwrap();
        
        exec.execute("INSERT INTO posts (user_id, title) VALUES (1, 'p1')").await.unwrap();

        // Alice has a post, Bob does not. LEFT JOIN should show Alice with p1 and Bob with NULL.
        let r = exec.execute("SELECT users.name, posts.title FROM users LEFT JOIN posts ON users.id = posts.user_id ORDER BY users.id ASC")
            .await
            .unwrap();
        
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0].as_text(), Some("alice"));
        assert_eq!(r.rows[0][1].as_text(), Some("p1"));
        assert_eq!(r.rows[1][0].as_text(), Some("bob"));
        assert_eq!(r.rows[1][1], crate::storage::Value::Null);
    }

    #[tokio::test]
    async fn test_subqueries() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE users (id INT, name TEXT)").await.unwrap();
        exec.execute("CREATE TABLE posts (id INT, user_id INT, title TEXT)").await.unwrap();
        
        exec.execute("INSERT INTO users (id, name) VALUES (1, 'alice')").await.unwrap();
        exec.execute("INSERT INTO users (id, name) VALUES (2, 'bob')").await.unwrap();
        
        exec.execute("INSERT INTO posts (id, user_id, title) VALUES (10, 1, 'p1')").await.unwrap();

        // 1. IN subquery
        let r = exec.execute("SELECT name FROM users WHERE id IN (SELECT user_id FROM posts)")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_text(), Some("alice"));

        // 2. Scalar subquery in SELECT
        let r = exec.execute("SELECT name, (SELECT title FROM posts WHERE user_id = users.id) AS post_title FROM users ORDER BY id ASC")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0].as_text(), Some("alice"));
        assert_eq!(r.rows[0][1].as_text(), Some("p1"));
        assert_eq!(r.rows[1][0].as_text(), Some("bob"));
        assert_eq!(r.rows[1][1], crate::storage::Value::Null);
    }

    #[tokio::test]
    async fn test_distinct() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE t (name TEXT)").await.unwrap();
        exec.execute("INSERT INTO t (name) VALUES ('alice')").await.unwrap();
        exec.execute("INSERT INTO t (name) VALUES ('bob')").await.unwrap();
        exec.execute("INSERT INTO t (name) VALUES ('alice')").await.unwrap();

        let r = exec.execute("SELECT name FROM t").await.unwrap();
        assert_eq!(r.rows.len(), 3);

        let r = exec.execute("SELECT DISTINCT name FROM t").await.unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_drop_table() {
        let exec = Executor::new(crate::storage::Database::new());
        exec.execute("CREATE TABLE x (id INT)").await.unwrap();
        exec.execute("DROP TABLE x").await.unwrap();
        let err = exec.execute("SELECT * FROM x").await.unwrap_err();
        assert!(matches!(err, SqlError::TableNotFound(_)));
    }
}
