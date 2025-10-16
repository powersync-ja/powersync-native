Experiment for a native PowerSync SDK.

- `powersync/` contains a small Rust SDK for PowerSync.
- `libpowersync/` wraps the Rust SDK for C and C++ projects.

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
