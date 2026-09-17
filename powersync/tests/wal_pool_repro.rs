use powersync::{ConnectionPool, env::PowerSyncEnvironment};
use rusqlite::ffi::{
    SQLITE_OK, SQLITE_ROW, sqlite3_finalize, sqlite3_prepare_v2, sqlite3_step, sqlite3_stmt,
    sqlite3_stmt_busy,
};
use std::{
    ffi::CString,
    path::PathBuf,
    ptr,
    time::{SystemTime, UNIX_EPOCH},
};

fn temp_db() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "powersync-pool-wal-repro-{}-{nonce}.db",
        std::process::id()
    ))
}

fn checkpoint(connection: &rusqlite::Connection) -> (i64, i64, i64) {
    connection
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .expect("checkpoint")
}

unsafe fn step_without_reset(connection: &rusqlite::Connection, sql: &str) -> *mut sqlite3_stmt {
    let sql = CString::new(sql).expect("SQL has no NUL");
    let mut statement = ptr::null_mut();
    let prepared = unsafe {
        sqlite3_prepare_v2(
            connection.handle(),
            sql.as_ptr(),
            -1,
            &mut statement,
            ptr::null_mut(),
        )
    };
    assert_eq!(prepared, SQLITE_OK, "prepare");
    assert_eq!(unsafe { sqlite3_step(statement) }, SQLITE_ROW, "step");
    statement
}

#[test]
fn returned_reader_must_not_keep_a_busy_statement_or_pin_the_wal() {
    PowerSyncEnvironment::powersync_auto_extension().expect("register PowerSync extension");
    let path = temp_db();
    let writer = rusqlite::Connection::open(&path).expect("writer");
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO items(value) VALUES ('snapshot');",
        )
        .expect("fixture");
    let reader = rusqlite::Connection::open(&path).expect("reader");
    let pool = ConnectionPool::wrap_connections(writer, [reader]);

    let reader = pool.reader_sync();
    let statement = unsafe { step_without_reset(&reader, "SELECT * FROM items") };
    assert!(
        reader.is_autocommit(),
        "a stepped SELECT can pin WAL state while SQLite still reports autocommit"
    );
    drop(reader);

    let writer = pool.writer_sync();
    writer
        .execute_batch(
            "BEGIN IMMEDIATE;
             WITH RECURSIVE n(x) AS (
               VALUES(1) UNION ALL SELECT x + 1 FROM n WHERE x < 2000
             )
             INSERT INTO items(value) SELECT printf('value-%04d', x) FROM n;
             COMMIT;",
        )
        .expect("grow WAL behind snapshot");
    let (_, log_frames, checkpointed_frames) = checkpoint(&writer);
    drop(writer);

    let returned_reader = pool.reader_sync();
    let still_busy = unsafe { sqlite3_stmt_busy(statement) } != 0;
    unsafe {
        sqlite3_finalize(statement);
    }
    drop(returned_reader);

    let writer = pool.writer_sync();
    let (_, final_log_frames, final_checkpointed_frames) = checkpoint(&writer);

    assert!(
        !still_busy,
        "pool returned a reader with a stepped statement still busy: \
         log={log_frames}, checkpointed={checkpointed_frames}; \
         after explicit finalize log={final_log_frames}, checkpointed={final_checkpointed_frames}"
    );
    assert_eq!(
        log_frames, checkpointed_frames,
        "a released reader lease must not strand WAL frames"
    );
}

#[test]
fn released_leases_roll_back_explicit_transactions() {
    PowerSyncEnvironment::powersync_auto_extension().expect("register PowerSync extension");
    let path = temp_db();
    let writer = rusqlite::Connection::open(&path).expect("writer");
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT);",
        )
        .expect("fixture");
    let reader = rusqlite::Connection::open(&path).expect("reader");
    let pool = ConnectionPool::wrap_connections(writer, [reader]);

    let reader = pool.reader_sync();
    reader.execute_batch("BEGIN").expect("reader begin");
    assert!(!reader.is_autocommit());
    drop(reader);
    assert!(
        pool.reader_sync().is_autocommit(),
        "reader transaction survived lease release"
    );

    let writer = pool.writer_sync();
    writer.execute_batch("BEGIN").expect("writer begin");
    assert!(!writer.is_autocommit());
    drop(writer);
    assert!(
        pool.writer_sync().is_autocommit(),
        "writer transaction survived lease release"
    );
}
