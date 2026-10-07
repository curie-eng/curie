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
A process lock prevents overlapping cycles on that file. If a reset is not
confirmed, its `cleanup_blocked` field is `true` and later invocations refuse
to enqueue. An operator must inspect and resolve the outstanding route before
changing that field to `false`. Never treat an absent route as confirmation.
The file also retains the capacity skip counter and last success timestamps.
The script does not resolve approval cards.

`TURN_CANARY_CYCLE_INTERVAL`, `TURN_CANARY_TURN_DEADLINE`,
`TURN_CANARY_CLEANUP_DEADLINE`, and `TURN_CANARY_POLL_PERIOD` are seconds,
defaulting to 300, 90, 60, and 2. Schedule the one-shot command at the
configured interval.
`TURN_CANARY_MAX_TARGETS` defaults to 5. All are positive and bounded.
`TURN_CANARY_DB_SCHEMA` defaults to `curie`. `TURN_CANARY_METRICS_PATH` writes
Prometheus exposition after each cycle. Logs contain only phase, outcome, HTTP
status, and exception type.

For an optional ResourceQuota observation, set `TURN_CANARY_QUOTA_URL` to the
explicit Kubernetes quota list endpoint, `TURN_CANARY_QUOTA_TOKEN` to its bearer
token, and optionally `TURN_CANARY_QUOTA_RESOURCE` and
`TURN_CANARY_QUOTA_MIN_FREE`. The default resource is `count/pods` and the
minimum free count is 1. An unreadable quota skips enqueue. This check is a
snapshot, not a reservation or a cluster wide sandbox limit.
