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
checker fsyncs a record with the exact agent ID and scoped thread key under the
route's label in `pending_routes`. It removes that record only after the
matching reset confirms release. A crash or unconfirmed reset leaves it in
place, and later cycles skip that route only; other routes keep turning. Each
later cycle re-requests the reset for at most `TURN_CANARY_MAX_TARGETS` pending
records, least recently checked first, and clears a record once the worker
drains that reset: either it released the route, or at least one cycle after
the probe it found no route under the exact owned key. The route is then
turned again without operator action. Right after the probe, an absent route
is never treated as confirmation, because the queued turn may not have claimed
it yet.

`cleanup_blocked: true` still holds every route. The checker no longer sets it.
A state file written by an earlier version may carry a single `pending` record
with `cleanup_blocked: true`; that record names no route, so it holds every
route until the same re-check clears it, and clearing it also sets
`cleanup_blocked` to `false`. A `cleanup_blocked: true` without any pending
record is an operator hold, cleared only by an operator.
The file also retains the capacity skip counter and last success timestamps.
The script does not resolve approval cards.

Each probe's conversation ID is `eval:turn-canary-` followed by a UUIDv4, so a
log filter on that prefix selects canary threads and nothing else.
`TURN_CANARY_CONVERSATION_PREFIX` replaces the prefix; it must match
`eval:[A-Za-z0-9._-]{0,48}` because the worker isolates conversations by the
`eval:` prefix. Event IDs start with `EvCANARY-` and the author is
`turn-canary`.

`TURN_CANARY_CYCLE_INTERVAL`, `TURN_CANARY_TURN_DEADLINE`,
`TURN_CANARY_CLEANUP_DEADLINE`, and `TURN_CANARY_POLL_PERIOD` are seconds,
defaulting to 300, 90, 60, and 2. Schedule the one-shot command at the
configured interval, or use the exporter mode below.
`TURN_CANARY_MAX_TARGETS` defaults to 5. All are positive and bounded.
`TURN_CANARY_DB_SCHEMA` defaults to `curie`. `TURN_CANARY_METRICS_PATH` writes
Prometheus exposition after each cycle. Logs contain only phase, outcome, HTTP
status, and exception type.

The exposition carries these series, shown with the default prefix:

| Series | Meaning |
|---|---|
| `turn_canary_cycle_success` | 1 when inventory and admission succeeded, no all-route hold remains, at least one selected route was turned or skipped for capacity, no turned route failed, and no cleanup in the cycle was left unconfirmed. Capacity skips and routes held by an earlier pending cleanup are not failures. |
| `turn_canary_last_cycle_timestamp_seconds` | End of the last cycle. |
| `turn_canary_last_success_timestamp_seconds` | Last successful cycle. |
| `turn_canary_cleanup_degraded` | 1 while any route has a pending cleanup or an all-route hold remains. |
| `turn_canary_capacity_skips_total` | Persisted count of capacity skips. |
| `turn_canary_target_success{route}` | Last turn result of the route. Published only for a route whose turn was attempted; a capacity-skipped route publishes no sample in that cycle, and the exporter keeps its previous value. |
| `turn_canary_target_last_run_timestamp_seconds{route}` | Last cycle that selected the route for a turn or a capacity check. |
| `turn_canary_target_last_success_timestamp_seconds{route}` | Last delivered turn of the route. |
| `turn_canary_target_capacity_skipped{route}` | Added. 1 when the route was skipped for capacity in the cycle, 0 when it was turned. |
| `turn_canary_target_cleanup_degraded{route}` | Added. 1 while the route has a pending cleanup and is not turned, 0 once its cleanup is confirmed or cleared. |

Before this change, a capacity skip published `target_success` 0 and
`cycle_success` 0, and one unconfirmed cleanup held every route until an
operator edited the state file.

For the optional ResourceQuota check below, quota `hard` and `used` values and
`TURN_CANARY_QUOTA_MIN_FREE` are Kubernetes quantities: plain integers and
decimals, `m` and the other decimal suffixes (`k` or `K`, `M`, `G`, `T`, `P`,
`E`, `n`, `u`), and the binary suffixes `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei`. A
used `1500m` against a hard `16` CPU leaves 14.5 free, and `512Mi` used against
`2Gi` hard parses as bytes. Set `TURN_CANARY_QUOTA_MIN_FREE` in the resource's
own unit, for example `500m` for `limits.cpu` or `1Gi` for `limits.memory`.

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

In exporter mode `TURN_CANARY_METRICS_PATH` is not written; scrape `/metrics`
instead. These options add series beside the TURN-CANARY-6 set, shown with the
default prefix: `turn_canary_binding_check_success{route,outcome}`,
`turn_canary_adapter_up{adapter,outcome}`, `turn_canary_cycle_overruns_total`
(exporter only),
and `turn_canary_rotation`.

For an optional ResourceQuota observation, set `TURN_CANARY_QUOTA_URL` to the
explicit Kubernetes quota list endpoint, its bearer token through exactly one of
`TURN_CANARY_QUOTA_TOKEN` (a static token) or `TURN_CANARY_QUOTA_TOKEN_FILE` (a
path to a file holding the token), and optionally `TURN_CANARY_QUOTA_RESOURCE`
and `TURN_CANARY_QUOTA_MIN_FREE`. Setting both token options refuses to start.
The checker reads the token file again for every quota request and keeps
nothing past that request, so a token that the kubelet rotates is picked up on
the next request. Inside a pod, the usual value is the projected service
account token, `/var/run/secrets/kubernetes.io/serviceaccount/token`; it is
not a default and must be set explicitly. A missing, unreadable, or blank token
file makes the quota unreadable, and the token is never logged. The default resource is `count/pods` and the
minimum free amount is 1. An unreadable quota skips enqueue, reported as a
capacity skip rather than a turn failure. This check is a
snapshot, not a reservation or a cluster wide sandbox limit.
