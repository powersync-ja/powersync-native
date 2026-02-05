## Preparing releases

First, bump the version in `powersync/Cargo.toml`. Run a build to ensure it's also updated in `Cargo.lock`.
Also ensure all changes are described in `CHANGELOG.md`.

Next, open a PR with these changes and wait for it to get approved and merged.

Finally, [create a release](https://github.com/powersync-ja/powersync-native/releases) on GitHub.
Creating the associated tag will trigger a workflow to publish to crates.io.
