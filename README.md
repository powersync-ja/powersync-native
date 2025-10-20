<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

_[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres, MongoDB or MySQL on the server-side._

## PowerSync Native

This repository contains code used to build a PowerSync SDK for native development.

PowerSync is available as a Rust crate in `powersync/`. Note that this crate is not currently published to crates.io,
and that it downloads additional binaries in its `build.rs`. This is something we're looking to address in the future.
At the moment, we are primarily targeting C++ development with this SDK (although the SDK is implemented in Rust).

## Running the examples

To start an example:

1. Run the [NodeJS demo](https://github.com/powersync-ja/self-host-demo/tree/main/demos/nodejs) without
   the sync service: `docker compose up --scale powersync=0`
2. Start a sync service instance with sync streams configured (see sync rules below).
3. Compile and run an example here: `cargo run -p egui_todolist`.

```yaml
# Sync-rule docs: https://docs.powersync.com/usage/sync-rules
streams:
  lists:
    query: SELECT * FROM lists #WHERE owner_id = auth.user_id()
    auto_subscribe: true
  todos:
    query: SELECT * FROM todos WHERE list_id = subscription.parameter('list') #AND list_id IN (SELECT id FROM lists WHERE owner_id = auth.user_id())

config:
  edition: 2
```
