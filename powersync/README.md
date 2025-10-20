<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

_[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres, MongoDB or MySQL on the server-side._

# PowerSync Rust

## Setup

PowerSync is implemented over local SQLite databases. The main entrypoint for this library, `PowerSyncDatabase`,
requires three external dependencies to be provided:

1. An HTTP client implementation from the `http-client` crate.
   - The `async-h1` implementation is known not to work with PowerSync, we recommend the `IsahcClient` instead.
2. A `ConnectionPool` of SQLite connections.
   - Create one with `ConnectionPool::open(path)`.
   - For in-memory databases, use `ConnectionPool::single_connection()`.
3. A timer implementation, used to delay reconnects when a sync connection gets interrupted.

These three external dependencies are bundled into the `PowerSyncEnvironment` class. At the moment, all three of them
need to provided manually via `PowerSyncEnvironment::custom`. We may offer a default configuration in the future.

After obtaining a `PowerSyncEnvironment`, construct a `Schema` instance describing the local schema of your database.
The PowerSync SDK will create and auto-migrate schemas.
Finally, create a database with `PowerSyncDatabase::new`.

### Async runtimes

This crate provides asynchronous APIs to:

1. Manage concurrent access to SQLite connections.
2. Request tokens to authenticate against the PowerSync service.
3. Stream changes from the PowerSync service to your local database.
4. Upload local writes to your backend.

For maximum flexibility, the `powersync` crate is executor-agnostic and can run on async runtime. All tasks that run
concurrently to the main application need to be spawned before starting PowerSync:

```Rust
#[tokio::main]
async fn main() {
    let db = PowerSyncDatabase::new(env, schema);
    db.async_tasks().spawn_with(|future| {
        tokio::spawn(future);
    });
}
```

The crate generally operates on a bring-your-own-runtime assumption, although optional features are available for
popular runtimes. The above snippet can be simplified to:

```Rust
#[tokio::main]
async fn main() {
    let db = PowerSyncDatabase::new(env, schema);
    db.async_tasks().spawn_with_tokio();
}
```

### Running queries

There is no dedicated query API as part of this SDK. Instead, the SDK gives out scoped access to `sqlite3*` connection
pointers (or the `rusqlite::Connection` struct for safe Rust).

```Rust
async fn main() {
   let db = PowerSyncDatabase::new(...);

   {
      let writer = db.writer().await.unwrap();
      writer.execute("INSERT INTO users VALUES ...", params![]);
   }

   {
      let reader = db.reader().await.unwrap();
      // Use rusqlite query APIs here
   }
}
```

Because contents of the database can change at any time (for instance, due to new data being synced), it's convenient
to get notifications about updates on tables.
PowerSync exposes the `watch_tables` method returning a `futures_core::stream::Stream` emitting an event whenever a
watched table changes. This is useful as a building block for auto-updating queries.
Note that unlike the other PowerSync SDKs, `watch_tables` is never throttled! You'd have to implement that logic as a
stream transformer.

### Connecting

To automatically keep the local SQLite database in-sync with a backend process, call `PowerSyncDatabase::connect`.
This requires passing your own `BackendConnector` implementation, see the examples for a possible implementation.

### Sync status

PowerSync exposes the current sync status through the `PowerSyncDatabase::status()` method. This status instance can be
used to query for current sync progress or errors.

Similarly, `PowerSyncDatabase::watch_status` returns a stream of status data that re-emits whenver it changes.

### Sync streams

The native SDK is built under the assumption that data to sync is specified in [sync streams](https://docs.powersync.com/usage/sync-streams).

To subscribe to a stream, use `PowerSyncDatabase::sync_stream(db, name, params).subscribe()`. The stream will be synced
as long as the returned subscription handle is active (and for a configured TTL afterwards).

## Limitations

This SDK is in development. Some items that are still being worked on are:

1. Token prefetching and caching.
2. Unit tests for CRUD uploads.

Also, this crate's `build.rs` dynamically downloads a binary
(the [PowerSync core extension](https://github.com/powersync-ja/powersync-sqlite-core/)) to link into the final
executable. The reason is that, while this library works with stable Rust, the core extension requires a nightly build.
We'll work towards making the core extension a regular Rust crate supporting stable compilers in the future.
