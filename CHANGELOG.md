## 0.0.6 (unreleased)

- Skip creating `ps_crud` entries when clearing raw tables.

## 0.0.5

- __Breaking__: Remove the `http-client` crate dependency. Instead, implement the `HttpClient` trait
  from the `powersync` crate directly.
- __Breaking__: The `Timer` passed to `PowerSyncEnvironment` is now a `&'static` reference instead of a `Box`.
- __Breaking__: `LeasedConnection` is a struct instead of a trait now.
- Add `PowerSyncDatabase::watch_all_updates` to emit updates of all changed tables.

## 0.0.4

- Update PowerSync core extension to version 0.4.11.
- Improvements for raw tables:
  - The `put` and `delete` statements are optional now.
  - The `RawTableSchema` struct represents a raw table in the local database, and can be used
    to create triggers forwarding writes to the CRUD upload queue and to infer statements used
    to sync data into raw tables.

## 0.0.3

- Add `PowerSyncDatabase::watch_statement` to get an auto-updating stream of query results.

## 0.0.2

- Configure automated publishing to crates.io.

## 0.0.1

- Initial release.
