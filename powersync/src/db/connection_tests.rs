use super::{SqliteConnection, TransactionMode, run_transaction};
use crate::error::PowerSyncError;
use powersync_sqlite_nostd::Connection;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn connection(tag: &str) -> (SqliteConnection, PathBuf) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "powersync-transaction-{tag}-{}-{nonce}.db",
        std::process::id()
    ));
    let connection = rusqlite::Connection::open(&path).expect("connection");
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;",
        )
        .expect("WAL");
    (SqliteConnection::from(connection), path)
}

fn autocommit(connection: &SqliteConnection) -> bool {
    unsafe { connection.handle().get_autocommit() }
}

fn assert_wal_reclaimed(connection: &SqliteConnection) {
    let result: (i64, i64, i64) = connection
        .rusqlite_connection()
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("checkpoint");
    assert_eq!(
        result,
        (0, 0, 0),
        "transaction exit must leave no WAL read mark"
    );
}

fn remove_db(connection: SqliteConnection, path: PathBuf) {
    drop(connection);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

#[test]
fn success_commits_and_error_rolls_back() {
    let (mut connection, path) = connection("success-error");
    connection
        .exec(c"CREATE TABLE t(id INTEGER)")
        .expect("table");

    run_transaction(&mut connection, TransactionMode::Write, |transaction| {
        transaction.execute("INSERT INTO t VALUES (1)")
    })
    .expect("commit");

    let error = run_transaction(&mut connection, TransactionMode::Write, |transaction| {
        transaction.execute("INSERT INTO t VALUES (2)")?;
        Err::<(), _>(PowerSyncError::argument_error("expected operation error"))
    })
    .expect_err("operation error");
    assert!(error.to_string().contains("expected operation error"));
    assert!(autocommit(&connection));

    let count: i64 = connection
        .rusqlite_connection()
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 1);
    assert_wal_reclaimed(&connection);
    remove_db(connection, path);
}

#[test]
fn panic_rolls_back_on_unwind() {
    let (mut connection, path) = connection("panic");
    connection
        .exec(c"CREATE TABLE t(id INTEGER)")
        .expect("table");

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = run_transaction(
            &mut connection,
            TransactionMode::Write,
            |transaction| -> Result<(), PowerSyncError> {
                transaction.execute("INSERT INTO t VALUES (1)")?;
                panic!("fixture panic");
            },
        );
    }));
    assert!(panic.is_err());
    assert!(autocommit(&connection));

    let count: i64 = connection
        .rusqlite_connection()
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 0);
    assert_wal_reclaimed(&connection);
    remove_db(connection, path);
}

#[test]
fn commit_failure_rolls_back_and_releases_the_wal() {
    let (mut connection, path) = connection("commit-failure");
    connection
        .rusqlite_connection()
        .execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE parent(id INTEGER PRIMARY KEY);
             CREATE TABLE child(
                 parent_id INTEGER,
                 FOREIGN KEY(parent_id) REFERENCES parent(id)
                     DEFERRABLE INITIALLY DEFERRED
             );",
        )
        .expect("schema");

    run_transaction(&mut connection, TransactionMode::Write, |transaction| {
        transaction.execute("INSERT INTO child(parent_id) VALUES (999)")
    })
    .expect_err("deferred foreign-key violation must fail at commit");

    assert!(
        autocommit(&connection),
        "failed COMMIT must still roll back"
    );
    let count: i64 = connection
        .rusqlite_connection()
        .query_row("SELECT COUNT(*) FROM child", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 0);
    assert_wal_reclaimed(&connection);
    remove_db(connection, path);
}

#[test]
fn transaction_control_is_owned_by_the_scope() {
    let (mut connection, path) = connection("owned-exit");
    connection
        .exec(c"CREATE TABLE t(id INTEGER)")
        .expect("table");

    for sql in [
        "BEGIN",
        " commit",
        "-- deliberately obscured\nROLLBACK",
        "/* deliberately obscured */ SAVEPOINT nested",
        "\u{feff}END",
        "RELEASE nested",
    ] {
        let error = run_transaction(&mut connection, TransactionMode::Write, |transaction| {
            transaction.execute("INSERT INTO t VALUES (1)")?;
            transaction.execute(sql)
        })
        .expect_err("transaction control must be rejected");
        assert!(
            error.to_string().contains("Transaction control is owned"),
            "{sql}: {error}"
        );
    }

    let count: i64 = connection
        .rusqlite_connection()
        .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
        .expect("count");
    assert_eq!(count, 0);
    assert_wal_reclaimed(&connection);
    remove_db(connection, path);
}

#[test]
fn read_mode_releases_a_real_snapshot() {
    let (mut reader, path) = connection("read-snapshot");
    reader.exec(c"CREATE TABLE t(id INTEGER)").expect("table");
    reader.exec(c"INSERT INTO t VALUES (1)").expect("seed");
    let writer = rusqlite::Connection::open(&path).expect("second writer");

    run_transaction(&mut reader, TransactionMode::Read, |transaction| {
        transaction.with_statement("SELECT id FROM t", |statement| {
            assert_eq!(statement.step()?, powersync_sqlite_nostd::ResultCode::ROW);
            writer
                .execute("INSERT INTO t VALUES (2)", [])
                .map_err(PowerSyncError::from)?;
            Ok(())
        })
    })
    .expect("read transaction");

    assert!(autocommit(&reader));
    assert_wal_reclaimed(&reader);
    drop(writer);
    remove_db(reader, path);
}
