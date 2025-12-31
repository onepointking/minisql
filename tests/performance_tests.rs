use minisql::executor::{Executor, Session};
use minisql::parser::Parser;
use minisql::storage::StorageEngine;
use minisql::engines::granite::TransactionManager;
use std::time::Instant;
use tempfile::{tempdir, TempDir};

fn setup_perf_test() -> (Executor, Session, TempDir) {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage, txn_manager);
    let session = Session::new();
    (executor, session, dir)
}

fn populate_table(executor: &Executor, session: &mut Session, table_name: &str, row_count: usize) {
    let create_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER, description TEXT)",
        table_name
    );
    executor.execute(Parser::parse(&create_sql).unwrap(), session).unwrap();

    let categories = ["A", "B", "C", "D", "E"];
    for i in 1..=row_count {
        let category = categories[i % categories.len()];
        let amount = (i * 10) as i64;
        let desc = format!("Description for row {}", i);
        let insert_sql = format!(
            "INSERT INTO {} VALUES ({}, '{}', {}, '{}')",
            table_name, i, category, amount, desc
        );
        executor.execute(Parser::parse(&insert_sql).unwrap(), session).unwrap();
    }
}

#[test]
fn test_full_scan_performance() {
    let (executor, mut session, _dir) = setup_perf_test();
    let row_count = 1000;
    populate_table(&executor, &mut session, "items", row_count);

    let start = Instant::now();
    let stmt = Parser::parse("SELECT * FROM items WHERE description = 'Description for row 500'").unwrap();
    let _res = executor.execute(stmt, &mut session).unwrap();
    let duration = start.elapsed();

    println!("Full scan of {} rows took: {:?}", row_count, duration);
    // Threshold: 100ms for 1000 rows (generous buffer)
    assert!(duration.as_millis() < 100, "Full scan was too slow: {:?}", duration);
}

#[test]
fn test_index_scan_performance() {
    let (executor, mut session, _dir) = setup_perf_test();
    let row_count = 1000;
    populate_table(&executor, &mut session, "items", row_count);

    // Create index
    executor.execute(Parser::parse("CREATE INDEX idx_id ON items(id)").unwrap(), &mut session).unwrap();

    let start = Instant::now();
    let stmt = Parser::parse("SELECT * FROM items WHERE id = 500").unwrap();
    let _res = executor.execute(stmt, &mut session).unwrap();
    let duration = start.elapsed();

    println!("Index scan of {} rows took: {:?}", row_count, duration);
    // Threshold: 10ms (should be near-instant)
    assert!(duration.as_millis() < 10, "Index scan was too slow: {:?}", duration);
}

#[test]
fn test_aggregate_performance() {
    let (executor, mut session, _dir) = setup_perf_test();
    let row_count = 1000;
    populate_table(&executor, &mut session, "items", row_count);

    let start = Instant::now();
    let stmt = Parser::parse("SELECT category, SUM(amount) FROM items GROUP BY category").unwrap();
    let _res = executor.execute(stmt, &mut session).unwrap();
    let duration = start.elapsed();

    println!("Aggregation of {} rows took: {:?}", row_count, duration);
    // Threshold: 150ms
    assert!(duration.as_millis() < 150, "Aggregation was too slow: {:?}", duration);
}

#[test]
fn test_join_performance() {
    let (executor, mut session, _dir) = setup_perf_test();
    let row_count = 500;
    
    // Create and populate two tables
    executor.execute(Parser::parse("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val1 TEXT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val2 TEXT)").unwrap(), &mut session).unwrap();

    for i in 1..=row_count {
        executor.execute(Parser::parse(&format!("INSERT INTO t1 VALUES ({}, 'val{}')", i, i)).unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse(&format!("INSERT INTO t2 VALUES ({}, 'val{}')", i, i)).unwrap(), &mut session).unwrap();
    }

    let start = Instant::now();
    let stmt = Parser::parse("SELECT t1.val1, t2.val2 FROM t1 JOIN t2 ON t1.id = t2.id").unwrap();
    let _res = executor.execute(stmt, &mut session).unwrap();
    let duration = start.elapsed();

    println!("Join of {}x{} rows took: {:?}", row_count, row_count, duration);
    // Threshold: 500ms (Join is currently O(N*M) or O(N) if it uses index, but let's be safe)
    assert!(duration.as_millis() < 500, "Join was too slow: {:?}", duration);
}
