<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

_[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres, MongoDB or MySQL on the server-side._

# PowerSync Rust

> [!NOTE]
> This SDK is currently in a [beta state](https://docs.powersync.com/resources/feature-status): It is production-ready
> for tested use cases, breaking changes will be clearly communicated.

## Setup

PowerSync is implemented over local SQLite databases. The main entrypoint for this library, `PowerSyncDatabase`,
requires three external dependencies to be provided:

1. An HTTP client implementation.
   - PowerSync accepts `reqwest::Client` instances when the `reqwest` feature is enabled.
2. A `ConnectionPool` of SQLite connections.
   - Create one with `ConnectionPool::open(path)`.
   - For in-memory databases, use `ConnectionPool::single_connection()`.
3. An async runtime like Tokio or smol, used to spawn background tasks for the sync client.
   Support for these runtimes can be enabled with the `tokio` and `smol` features, respectively.

These three external dependencies are bundled into the `PowerSyncEnvironment` class. At the moment, all three of them
need to provided manually via `PowerSyncEnvironment::custom`. We may offer a default configuration in the future.

After obtaining a `PowerSyncEnvironment`, construct a `Schema` instance describing the local schema of your database.
The PowerSync SDK will create and auto-migrate schemas.
Finally, create a database with `PowerSyncDatabase::new`.

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
