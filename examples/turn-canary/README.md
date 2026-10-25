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
| `turn_canary_target_delivery_success{route}` | Last observed delivered lifecycle with a matching request identity and nonblank reply. A nonce mismatch can set this to 1 while target success stays 0. Capacity skips and cleanup holds create no new sample. |
| `turn_canary_target_probe_success{route}` | Last configured profile verdict, or exact nonce verdict when no profile applies. |
| `turn_canary_target_probe_profile{route}` | 1 when the last attempted turn used an explicit profile, 0 for the nonce default. |
| `turn_canary_target_last_delivery_success_timestamp_seconds{route}` | Last successful delivery observation, independent of nonce or profile verdict. |
| `turn_canary_target_last_probe_success_timestamp_seconds{route}` | Last passing profile verdict, or exact nonce verdict for the default. |
| `turn_canary_target_success{route}` | Last turn result of the route. Published only for a route whose turn was attempted; a capacity-skipped route publishes no sample in that cycle, and the exporter keeps its previous value. |
| `turn_canary_target_last_run_timestamp_seconds{route}` | Last cycle that selected the route for a turn or a capacity check. |
| `turn_canary_target_last_success_timestamp_seconds{route}` | Last exact nonce success of the route. |
| `turn_canary_target_capacity_skipped{route}` | Added. 1 when the route was skipped for capacity in the cycle, 0 when it was turned. |
| `turn_canary_target_cleanup_degraded{route}` | Added. 1 while the route has a pending cleanup and is not turned, 0 once its cleanup is confirmed or cleared. |

Delivery observation uses the production reply schema and the enqueued turn's
kind, address, conversation ID, reply reference and completion event ID. Malformed,
unrelated or duplicate observed completions fail closed. Status and progress
alone do not count as a text answer. Exact nonce success still controls the last
success timestamps, cycle success and one-shot exit code. The new gauge shares
the selected route label layout and metric prefix. Exact nonce success retains
its original last-update and event-type rules: a later progress update or a
`reply.post` cannot turn a previously failing sequence into success. An approval
pause is unsuccessful even though its relay page remains nonterminal.

The exporter retains delivery observations across skipped or held cycles and
removes them when a successful inventory read drops their route. Delivery is not
saved in the state file; after restart the sample is absent until a turn is
attempted. An absent sample is unknown, never a healthy observation. Delivery
shows an observed relay lifecycle, not model execution, useful task completion,
policy compliance or final-text provenance. Events arriving after the bounded
observation returns are outside its evidence. Relay logs add only bounded reason
codes such as `nonce_mismatch`, `wrong_target`, `malformed_page` and `timeout`,
without logging response content.

### Role-appropriate probe contracts

The default question still asks for an exact nonce. A role-restricted agent may
correctly refuse that question. Operators can instead supply a read-only question
that the agent's role permits, together with an explicit affirmative text contract:

```sh
TURN_CANARY_PROBE_PROFILES='{"catalog-helper":{"prompt":"Describe your read-only catalog role without looking anything up.","required_any":[["catalog"],["read-only","read only"]],"forbidden_any":["cannot comply","approval required"]}}'
```

Profiles apply by exact inventory agent slug, only after normal route selection.
An unmatched slug keeps the nonce default; check the emitted
`target_probe_profile` flag to verify coverage. Configuration never changes the
agent's safety policy. Every question retains the same prohibition on tools,
actions, approval requests and durable changes, and the same read-only wire,
isolation, request correlation and owned cleanup. Choose a question answerable
without tools and include known refusal markers in `forbidden_any`.

The latest non-progress final reply must be delivered and correctly correlated.
Unicode NFKC normalization, case folding and whitespace collapse apply to both
reply and markers. Every required group needs one literal substring match and
no forbidden marker may appear. Metacharacters have no special meaning. A
refusal is not automatically useful completion; a matching reply proves only
the declared text contract. The legacy nonce verdict, cycle success, nonce
success timestamps and one-shot exit status retain their original meaning,
even when a configured profile passes. Use `target_probe_success` for an alert
about the explicit contract and `target_delivery_success` with delivery
freshness for an alert about availability.

The strict configuration bounds are specified in
[TURN-CANARY-16](design.md): 65,536 UTF-8 JSON bytes, 100 profiles, 63-character
lowercase ASCII slugs, 2,048-character prompts, 1 to 8 required groups with
1 to 8 alternatives each, 0 to 32 forbidden markers and 128 characters per
marker. Blank fields, empty profiles, missing or unknown fields, duplicate keys
and malformed JSON refuse startup. An unset variable disables profiles;
an explicitly empty value or object is an error. Programmatic consumers use
`parse_probe_profiles(raw)` and the immutable `ProbeProfile.matches(text)`
matcher from this example to share the same contract.

The probe result, mode and both new success timestamps are retained across
rotation, capacity skips and cleanup holds, and removed when inventory drops
the route. They are not restored after restart. A missing sample is unknown.
Combine the result with last-run freshness, and delivery success with its own
last-success timestamp, rather than using nonce timestamps to date delivery.
Profile logs expose only `profile_match` or `profile_mismatch`; the existing
relay reason remains independent. Prompts, markers and replies never become
metric labels or log fields. These deterministic checks do not establish live
model or protected runtime qualification.

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

- `TURN_CANARY_DECLARED_BINDINGS` takes a JSON list of the route triples the
  installation is meant to have, for example
  `[["slack","C-EXAMPLE-1","default"]]`. Each cycle reports declared triples
  missing from the live bindings and live triples of a declared kind that are
  not declared (TURN-CANARY-11).
- `TURN_CANARY_BINDING_ADAPTER_HEALTH=/healthz` probes each binding's stored
  endpoint origin at that path, once per distinct URL per cycle
  (TURN-CANARY-12).
- `TURN_CANARY_COVERAGE=1` reports agents with an active deployment that the
  monitor cannot observe through a turn candidate or a binding health URL
  (TURN-CANARY-13).
- `TURN_CANARY_EXCLUDE_BINDING_KINDS` takes a comma-separated list of binding
  kinds, for example `qualification`, that every observation ignores: such a
  binding is never turned, checked, probed or compared, and an agent that has
  only such bindings is not reported as uncovered (TURN-CANARY-15).
- `TURN_CANARY_ROUTE_LABELS=split` replaces the JSON `route` label with
  `agent`, `kind`, `address` and `adapter` labels (TURN-CANARY-14).

These add `turn_canary_binding_drift{route,side}`,
`turn_canary_binding_drift_total`, `turn_canary_binding_adapter_up{route,outcome}`,
`turn_canary_agent_uncovered{agent}` and `turn_canary_uncovered_agents`.

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
