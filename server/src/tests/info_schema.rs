use crate::sql::Executor;
use crate::storage::Database;
use std::sync::Arc;
use super::common::setup;

#[tokio::test]
async fn test_info_schema() {
    setup();
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE info_test (id INT, name TEXT)", None).await.unwrap();
    executor.execute("CREATE UNIQUE INDEX idx_info_id ON info_test (id)", None).await.unwrap();

    // 1. Check tables
    let r = executor.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'info_test'", None).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_text(), Some("info_test"));
    assert_eq!(r.rows[0][1].as_text(), Some("BASE TABLE"));

    // 2. Check columns
    let r = executor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'info_test' ORDER BY ordinal_position", None).await.unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0].as_text(), Some("id"));
    assert_eq!(r.rows[0][1].as_text(), Some("INT"));
    assert_eq!(r.rows[1][0].as_text(), Some("name"));
    assert_eq!(r.rows[1][1].as_text(), Some("TEXT"));

    // 3. Check indexes
    let r = executor.execute("SELECT index_name, is_unique FROM information_schema.indexes WHERE table_name = 'info_test'", None).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_text(), Some("idx_info_id"));
    assert_eq!(r.rows[0][1].as_bool(), Some(true));
}
