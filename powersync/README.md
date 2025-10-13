## Async runtimes

The `powersync` crate is executor-agnostic and can run on any async runtime.

## Todos

For this SDK to reach alpha-quality, items that still need to be done are:

1. Easier spawning of tasks on popular runtimes (tokio / smol).
2. Token prefetching and caching.
3. Unit tests for CRUD uploads.
