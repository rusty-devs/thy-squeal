#[cfg(test)]
mod tests {
    use crate::sql::executor::Executor;
    use crate::sql::squeal::{Expression, Insert, Select, SelectColumn, Squeal};
    use crate::storage::{Database, Value};

    #[tokio::test]
    async fn test_jsqueal_select_simple() {
        let db = Database::new();
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
            where_clause: Some(crate::sql::squeal::Condition::Comparison(
                Expression::Column("id".to_string()),
                crate::sql::squeal::ComparisonOp::Eq,
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
        let db = Database::new();
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
}
