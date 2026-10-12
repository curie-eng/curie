# Forge fixtures

Local stand-ins for code hosts and trackers, driven by the factory end to end
harness. They are started and stopped through the CLI:

```
curie dev forge-fixture <kind> up|self-test|down --state-dir <dir> [--advertise-host <host>] [--port <port>]
```

## Layout

Each fixture is one directory, `tools/forge-fixtures/<kind>/`, with an entry
point `fixture.py`. The entry point takes the action as its first argument,
one of `up`, `self-test`, or `down`, followed by `--state-dir <dir>` and the
optional `--advertise-host <host>` and `--port <port>`. `cli/scripts/forge-fixture.sh`
runs it with `python3`. All fixture state lives under the state directory.

The reserved kinds are `gitlab` and `bitbucket-dc`. `gitlab` is installed; see
`gitlab/README.md`. `bitbucket-dc` is not installed yet: until its `fixture.py`
exists, `curie dev forge-fixture` for that kind exits nonzero with a "not
installed" message and creates no state.

## Adding a fixture

Add the `tools/forge-fixtures/<kind>/fixture.py` directory and file for the
forge. If the kind is not already `gitlab` or `bitbucket-dc`, also add it to
`ForgeFixtureKind` in `cli/src/args/dev.rs` and regenerate the command
manifest. Fixtures use placeholder site, workspace, and account names only.
