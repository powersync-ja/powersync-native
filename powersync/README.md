<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

_[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres, MongoDB or MySQL on the server-side._

# PowerSync Rust

## Setup

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

## Limitations

This SDK is in development. Some items that are still being worked on are:

1. Token prefetching and caching.
2. Unit tests for CRUD uploads.

Also, this crate's `build.rs` dynamically downloads a binary
(the [PowerSync core extension](https://github.com/powersync-ja/powersync-sqlite-core/)) to link into the final
executable. The reason is that, while this library works with stable Rust, the core extension requires a nightly build.
We'll work towards making the core extension a regular Rust crate supporting stable compilers in the future.
