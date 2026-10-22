# docker-scenario fixtures

Offline inputs for `tools/factory-e2e/docker_scenario.py` and its tests.

## Real recordings

`raw/` holds output recorded from a Docker-in-Docker daemon built from
`apps/docker-daemon` with the `curie-authz` authorization plugin loaded. Each
`<name>.out` is the combined stdout and stderr of one command and `<name>.rc` its
exit code. The recording nonce is `e8ce1a8c`.

1. `info-runner` is `docker info --format '{{.ID}} {{.Name}}'` from the client
   side and `info-daemon` the same command inside the daemon container.
2. `holder` starts the labelled `alpine:3.20 sleep 300` container; `holder-rm`
   and `volume-rm` are the probe's cleanup.
3. `escape-<name>` is each of the eight escape attempts, all refused by the
   plugin with the rule named in the output.
4. `egress-metadata` and `egress-testnet` are `nc -z` probes to blocked targets
   (exit 1); `egress-public` is the reachable control (exit 0). `pull` is the
   image pull.
5. `events` is the daemon-side `docker events` stream for the window in
   `window` (`T0 T1`), filtered on the nonce label.

`github/` holds real GitHub REST response shapes, anonymized: the branch rules
for the base branch, the check runs of one fully green head, and that head's
combined status. The combined status reads `pending` with zero statuses even
though every check run is green; that is how GitHub reports a head that only
has check runs.

## Derived data in `passing-envelope.json`

1. Run `a` is built by hand from `raw/`: ids, names, events, refusal rules and
   exit codes are copied literally. Egress maps `egress-metadata` to `metadata`
   and `egress-testnet` to `cluster-api` and `curie-api`, with ClusterIP-style
   targets. The daemon pod name is the recorded `docker info` name.
2. Run `b` is an edited copy of run `a`: a distinct claim, daemon pod, daemon
   id and container ids, with its window and event times shifted by nine
   seconds so the two windows overlap. It was not recorded separately.
3. `cleanup` and `control` are exactly what `from_lifecycle` produces from
   `../daemon-lifecycle/passing-envelope.json`, a recorded live run.
4. `upgrade.plan_lines` are composed from the plan-line format strings in
   `cli/src/ops/upgrade.rs` for one catalog repository and one custom
   repository; the image references are illustrative digest-pinned refs.
5. `ci` is derived from `github/`: the required contexts from the rules file
   and one check per check run.

6. `runs.a.probe.cleanup` and `runs.b.probe.cleanup` (`holder_removed`,
   `volume_absent`) are derived fields: both are true because the recording's
   `holder-rm` and `volume-rm` exited 0 (the probe additionally verifies with
   `docker ps -aq` and `docker volume ls -q` listings that print nothing).

Failing envelopes are not stored here. Each failure case is a mutation of the
passing envelope made inside `tests/test_docker_scenario.py`.

7. `upgrade.custom.reply` (and `custom-reply.txt`, the file the `upgrade` CLI test
   passes as `--custom-reply`) is the reply of a turn on the custom repository
   after the upgrade. It is composed from the format strings in
   `apps/worker/src/curie_worker/kernel/attempt.py` ("This agent cannot start:
   {exc}. An operator has to make that change.") and the pinned-platform
   `stale_base` message of `RepoRunnerPoolRefused` in
   `apps/worker/src/curie_worker/sandbox/types.py`, with placeholder registry,
   namespace, release and context values.
8. `lifecycle` is `{"schema": "daemon-lifecycle/v1", "failures": []}`, what
   `from_lifecycle` returns for the committed daemon-lifecycle passing envelope
   (whose own judge reports no failures).
