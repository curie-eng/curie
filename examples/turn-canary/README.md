# Turn canary example

`turn_canary.py` runs one opt-in probe cycle. It reads current agent, deployment,
and channel binding rows, checks the selected Slack routes, and appends one
validated `QueuedTurn` per eligible route to the configured worker stream. The
reply arrives through the authenticated cluster message relay. The checker
requests a reset of each probe's own scoped thread and waits for confirmed
release before moving to another route.

The operator supplies all connections and route triples. Run from the repository
root with the Python workspace installed:

```sh
TURN_CANARY_DATABASE_URL='postgresql+asyncpg://...' \
TURN_CANARY_VALKEY_URL='redis://...' \
TURN_CANARY_API_URL='https://platform.example.invalid' \
TURN_CANARY_API_KEY='...' \
TURN_CANARY_STREAM='curie:runs' \
TURN_CANARY_ROUTES='[["slack","C-EXAMPLE-1","default"]]' \
TURN_CANARY_STATE_PATH='/var/lib/turn-canary/state' \
TURN_CANARY_READ_ONLY_QUALIFIED=1 \
uv run python examples/turn-canary/turn_canary.py
```

`TURN_CANARY_READ_ONLY_QUALIFIED=1` is an operator assertion that the deployed
worker enforces `tool_access=read-only` and its runner advertises that mode. An
unset flag refuses all turns. A local unit test does not establish that runtime
qualification. Use a disposable route and verify a real delivered nonce, the
protected runner trace, and reset completion before enabling periodic use.

The state file must be on persistent storage shared by repeated invocations.
A separate `.lock` file holds the process lock while atomic state replacement
keeps the previous complete state readable through an interrupted write. An
existing empty or corrupt state file refuses all probes. Before enqueue, the
checker fsyncs a `pending` record with the exact agent ID and scoped thread key.
It removes that record only after the matching reset is confirmed. A crash or
unconfirmed reset leaves it in place, and later invocations refuse to enqueue.
An operator must verify that exact route has been released before clearing
`pending` and changing `cleanup_blocked` to `false`. Never treat an absent route
as confirmation.
The file also retains the capacity skip counter and last success timestamps.
The script does not resolve approval cards.

`TURN_CANARY_CYCLE_INTERVAL`, `TURN_CANARY_TURN_DEADLINE`,
`TURN_CANARY_CLEANUP_DEADLINE`, and `TURN_CANARY_POLL_PERIOD` are seconds,
defaulting to 300, 90, 60, and 2. Schedule the one-shot command at the
configured interval, or use the exporter mode below.
`TURN_CANARY_MAX_TARGETS` defaults to 5. All are positive and bounded.
`TURN_CANARY_DB_SCHEMA` defaults to `curie`. `TURN_CANARY_METRICS_PATH` writes
Prometheus exposition after each cycle. Logs contain only phase, outcome, HTTP
status, and exception type.

## Standing monitor options

Each option below is off by default; [design.md](design.md) holds the contract
(TURN-CANARY-7 through TURN-CANARY-10).

- `TURN_CANARY_DISCOVER=1` replaces `TURN_CANARY_ROUTES`. Each cycle reads the
  live bindings and turns one Slack route per agent, rotating deterministically
  across cycles with a counter kept in the state file. At most
  `TURN_CANARY_MAX_TARGETS` agents are turned per cycle, in a rotating window.
  `TURN_CANARY_DISCOVER_EXCLUDE` takes a JSON list of exact triples that are
  never turned. Setting both `TURN_CANARY_ROUTES` and discovery refuses to
  start.
- `TURN_CANARY_BINDING_CHECKS=1` gives every binding that this cycle does not
  turn a model-free check through `GET /channel-identities`: an active
  deployment and one active identity with a nonblank credential reference. It
  needs the platform API key and never resolves the secret.
  `TURN_CANARY_IDENTITY_KINDS` (a JSON list, default `["slack"]`) names the
  binding kinds checked against channel identities; other kinds are checked
  for an active deployment only.
- `TURN_CANARY_ADAPTER_HEALTH` takes a JSON object from adapter name to
  health URL, for example
  `{"discord":"http://discord-adapter.example.invalid:8080/healthz"}`. Each
  cycle sends one unauthenticated `GET` per adapter and reports 2xx as up.
- `TURN_CANARY_LISTEN=0.0.0.0:9464` runs a long-lived exporter. It runs one
  cycle per `TURN_CANARY_CYCLE_INTERVAL`, never overlapping, and serves
  `/metrics` and `/healthz`. `/healthz` reports whether the monitor loop is
  current, not whether the platform is healthy, so a platform outage shows in
  the metrics without restarting the monitor.
- `TURN_CANARY_METRIC_PREFIX` replaces the `turn_canary` metric name prefix in
  both the metrics file and the exporter, to keep existing series names.

The exporter adds these series beside the TURN-CANARY-6 set, shown with the
default prefix: `turn_canary_binding_check_success{route,outcome}`,
`turn_canary_adapter_up{adapter,outcome}`, `turn_canary_cycle_overruns_total`,
and `turn_canary_rotation`.

For an optional ResourceQuota observation, set `TURN_CANARY_QUOTA_URL` to the
explicit Kubernetes quota list endpoint, `TURN_CANARY_QUOTA_TOKEN` to its bearer
token, and optionally `TURN_CANARY_QUOTA_RESOURCE` and
`TURN_CANARY_QUOTA_MIN_FREE`. The default resource is `count/pods` and the
minimum free count is 1. An unreadable quota skips enqueue. This check is a
snapshot, not a reservation or a cluster wide sandbox limit.
