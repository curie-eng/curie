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

## compose and skill rung recordings

`compose-passing.json`, `compose-failing.json`, `skill-rung-passing.json` and
`skill-rung-failing.json` are each one section exactly as the `compose` or
`skill-rung` collector wrote it with `--out`; nothing in them was edited. They
were recorded on the default Docker engine against a Docker-in-Docker daemon
built from `apps/docker-daemon`, with the `curie-authz` authorization plugin
loaded and user namespace remapping on (`docker info` listed `curie-authz`
under authorization plugins and `userns` under security options).

1. The daemon container ran `dockerd -H unix:///var/run/docker.sock -H
   tcp://0.0.0.0:2375 --tls=false` on its own network with an explicitly
   allocated subnet that overlapped no route in the daemon container, no route
   on the engine host and no existing engine network.
2. The collectors ran in a second container on that network with
   `DOCKER_HOST=tcp://<daemon container>:2375` and
   `DOCKER_PUBLISHED_HOST=<daemon container>`, so published ports were dialed
   at the daemon container's name. That container held the Docker CLI with the
   compose and buildx plugins, Python, a curie CLI built from the recorded
   source revision, and that checkout. Everything reached it by build context
   and `docker cp`; no host path was bind mounted anywhere.
3. compose used `--image` set to the digest-pinned Python base image of
   `runner/Dockerfile` (the stack needs `python3`). The failing compose
   recording passed `--published-host 127.0.0.1`, an address where the
   daemon's published port does not listen, so the published check was
   refused while the in-stack reach still worked.
4. skill-rung used `--source` set to that checkout and the built CLI as
   `CURIE_BIN`. `curie-runner` was built inside the daemon from the uploaded
   checkout, the ladder ran `CURIE_E2E_TIERS=skill` on the fake model and
   printed `LADDER PASS (tiers: skill)`, and the image was removed afterwards.
   The failing recording passed `--curie-bin /bin/false`, so the ladder
   stopped at `"$BIN" --version` with exit 1 and no `LADDER PASS`, and the
   collector recorded an empty CLI version. Both recordings show nothing left
   behind: no `curie-runner` image and no container, network or volume that
   was not on the daemon before the collector's snapshot.

9. `compose` and `skill_rung` in `passing-envelope.json` are
   `compose-passing.json` and `skill-rung-passing.json` with one edit: `info`
   (and `skill_rung.image.daemon_id`) carry run `a`'s recorded daemon identity
   instead of the recording daemon's, because the judge requires both sections
   to come from run `a`'s or run `b`'s daemon and those sections were recorded
   on a different daemon from the one run `a` was recorded on. The test
   `test_recorded_passing_envelope_embeds_the_recorded_sections` pins that
   this identity is the only difference.

## Scripted model transcript

`transcript.json` is the scripted-model transcript the docker scenario driver
(`tools/factory-e2e/docker_driver.py`) serves through `ModelScript`. Every
exchange uses the lane key of `../daemon-lifecycle/transcript.json`, and the
exchanges are in this order:

1. Run `a`'s lane, then run `b`'s: ordinals 0 to 5 are each one Bash tool use
   (timeout 600000 ms) that holds until `/tmp/fd-release` exists, for at most
   580 s, while `docker info` answers, and ordinal 6 is the end of turn text
   "Docker scenario run finished.". Six holds keep a turn alive for about an
   hour, long enough for the skill rung; once the release file exists every
   remaining hold returns at once, so the turn uses all seven exchanges.
2. Every exchange of `../daemon-lifecycle/transcript.json`, verbatim and in
   its order, for the lifecycle scenarios that follow the concurrent runs.

`ModelScript` consumes the first unused exchange for each lane and ordinal, so
the order matters: runs `a` and `b` run before any lifecycle turn and must take
the two Docker scenario lanes, and the lifecycle turns then find their own
exchanges untouched. The file was generated with a short Python snippet that
built the fourteen lane exchanges, appended the daemon-lifecycle exchanges
unchanged and wrote the result with `json.dumps(..., indent=2)` plus a trailing
newline, the formatting of the daemon-lifecycle transcript. Regenerate it the
same way after changing either part rather than editing it by hand.
