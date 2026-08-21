<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

_[PowerSync](https://www.powersync.com) keeps a client-side SQLite database in sync with your backend database. Changes appear across users and devices in real-time, user interactions feel instant and your app will continue to work even when offline. Supports Postgres, MongoDB, MySQL, and SQL Server. Client SDKs are available for a wide range of environments including web, mobile, desktop, headless and embedded._

## PowerSync Native

> [!NOTE]
> This SDK is currently in an [alpha state](https://docs.powersync.com/resources/feature-status), intended for external testing and public feedback.
> Expect breaking changes and instability as development continues.

This repository contains code used to build a PowerSync SDK for native development.
PowerSync is available as a Rust crate in `powersync/`, and on crates.io as the `powersync` crate.

## Running the examples

To start an example:

1. Run the [NodeJS demo](https://github.com/powersync-ja/self-host-demo/tree/main/demos/nodejs) without
   the sync service: `docker compose up --scale powersync=0`
2. Start a sync service instance with sync streams configured (see sync rules below).
3. Compile and run an example here: `cargo run -p egui_todolist`.

```yaml
# Sync Streams docs: https://docs.powersync.com/sync/streams/overview
streams:
  lists:
    query: SELECT * FROM lists #WHERE owner_id = auth.user_id()
    auto_subscribe: true
  todos:
    query: SELECT * FROM todos WHERE list_id = subscription.parameter('list') #AND list_id IN (SELECT id FROM lists WHERE owner_id = auth.user_id())

config:
  edition: 3
```
