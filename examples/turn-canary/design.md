# Opt-in read-only turn canary

The operator selects concrete Slack binding routes to probe, or opts in to
discovery from the live binding rows. This example is not installed by default
and never infers a target from an agent name alone.
It uses the existing queued-turn stream, disconnected cluster-message reply
relay, and scoped thread-reset API. It does not add a worker, runner, admission,
or channel adapter contract. The checker proves a bounded synthetic turn for
each selected route; it does not prove that Slack ingress or any other channel
adapter is connected.

## Contract

- **TURN-CANARY-1: inventory and selection.** Each cycle reads the live
  `agents`, `deployments`, and `agent_channels` tables. A target is eligible only
  when its agent has an active deployment and its exact `(kind, address,
  adapter)` route exists. In explicit mode the operator supplies a nonempty
  explicit list of these three-part routes; TURN-CANARY-7 defines discovery. The checker rejects missing, duplicate, ambiguous,
  inactive, or non-Slack selections before enqueue. It does not infer bindings
  from agent names, email addresses, manifests, or any tenant convention.
  An inventory read failure is a failed cycle, never an empty healthy cycle.

- **TURN-CANARY-2: isolated request.** Each probe mints a fresh event ID,
  `eval:`-prefixed conversation ID, canonical UUIDv4 relay reply ref, and random
  nonce. Its prompt asks only for that nonce and explicitly forbids tools,
  actions, approval requests, and durable changes. It sets `tool_access` to
  `read-only`, `source` to `slack`, and empty attachments and hook run. The
  `ReplyHandle` retains the selected Slack address, uses the reserved
  `curie-cluster-message` adapter for internal delivery, and names the selected
  binding identity in `identity`. The placeholder is the reply ref. The
  producer validates and serializes the complete request through the installed
  `aci_protocol.QueuedTurn` schema before one stream enqueue. It rejects a
  platform that cannot honor read-only tool access before sending a turn.

- **TURN-CANARY-3: completion.** The checker polls the authenticated
  `/cluster-message-replies/{reply_ref}` API with a monotonic cursor and a
  finite deadline. Success needs a `turn.completed` event with outcome
  `delivered` and a reply update whose entire text equals the nonce. A reply
  alone, an approval pause, dropped or escalated completion, a mismatched
  nonce, malformed event, or a timeout fails. No reply is posted to a channel.

- **TURN-CANARY-4: owned cleanup.** After every enqueued probe, including
  timeout and error paths, the checker computes the worker thread key with
  `channel_protocol.scoped_conversation_id` and the selected binding identity.
  Before the stream enqueue can begin, it durably records the exact selected
  agent ID and owned thread key in its persistent state. A crash or cancellation
  leaves this intent in place; a fresh invocation refuses another enqueue until
  an operator has confirmed the old route's release. The intent clears only
  after the matching reset is confirmed, never merely after a reset request.
  State updates use a separate stable process lock and an atomic replacement
  of a synced temporary file, followed by syncing the containing directory.
  An existing empty or corrupt state file fails closed; only a genuinely absent
  state file is a first-run empty state. An interrupted write cannot turn a
  recorded cleanup intent into a healthy empty state.
  `cleanup_blocked` must be a JSON boolean; numeric 0 or 1 is corrupt state,
  even though Python compares them equal to false or true. A journaled cycle
  honors a true cleanup hold directly, without relying on a caller to copy it
  into the platform. Persisted capacity skips must be a nonnegative JSON
  integer, and persisted success timestamps must be finite positive JSON
  numbers, never booleans. Invalid types or values fail before enqueue or
  metric export.
  It submits that key to `POST /agents/{agent_id}/threads/{thread_key}/reset`
  and polls the matching reset status endpoint until `requested` is false and
  `route_existed` is true. It never resets an unowned or guessed conversation.
  A false or unknown `route_existed`, pending reset beyond the deadline, or any
  unknown status leaves
  cleanup unconfirmed. The cycle marks the target degraded and stops before
  another probe. It does not treat absence of a route as proof of release.

- **TURN-CANARY-5: bounded operation.** Probes run serially. Cycle interval,
  turn deadline, cleanup deadline, poll period, and maximum selected targets
  are positive, bounded settings with documented defaults. A cycle is also
  bounded by those limits. A new cycle cannot begin while cleanup from the
  preceding cycle is unconfirmed. An optional ResourceQuota headroom check is
  an observation immediately before enqueue: insufficient or unreadable
  headroom skips the turn with an explicit outcome. It is not an atomic
  reservation and cannot promise a global sandbox cap. Quota `hard` and `used`
  values and the configured minimum free amount are Kubernetes quantities: a
  decimal number with an optional decimal (`m`, `k`, `M`, `G`, `T`, `P`, `E`)
  or binary (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei`) suffix, so a CPU limit such
  as `500m` is read as 0.5. A value that is not such a quantity makes the
  quota unreadable, never zero.

- **TURN-CANARY-6: diagnostics.** Export a portable Prometheus target success
  gauge, last run and last success timestamps, cleanup degraded gauge, capacity
  skip counter, cycle success gauge, and last cycle timestamp. Structured logs
  record outcome codes and phase without credentials, prompts, reply text,
  nonces, raw exception strings, or full HTTP bodies. HTTP failures expose only
  status and error type. Metric labels are bounded to selected route identity
  fields and outcome codes. Values such as namespace, image, resource quota,
  endpoint, and operator identity come from configuration; no installation
  value is compiled into this example.

## Standing monitor

The contract above describes one cycle. The capabilities below let the same
checker run continuously. Each is opt-in and off by default, and with all of
them off TURN-CANARY-1 through TURN-CANARY-6 behave as before. None of them
writes to the platform: discovery and model-free checks only read binding and
identity rows, adapter probes only send HTTP `GET`, and the exporter only
serves the last observation. Only the turn path of TURN-CANARY-2 through
TURN-CANARY-4 enqueues work, and it keeps its read-only request and owned
cleanup.

- **TURN-CANARY-7: discovery and rotation.** In discovery mode the operator
  supplies no route list. Each cycle derives candidates from the inventory
  read of TURN-CANARY-1. A binding is a turn candidate only when its kind is
  `slack`, its agent has an active deployment, its agent ID, address, and
  adapter are nonblank, its exact triple occurs once in the inventory, and
  the triple is not on the optional exclusion list. A triple that occurs more
  than once is ambiguous; it is never turned and is logged as a discovery
  outcome. Candidates are grouped by agent ID, agents are ordered by agent ID,
  and each agent's routes are ordered by `(kind, address, adapter)`. With `n`
  agents, a cycle fills `m = min(max_targets, n)` slots. Slot
  `s = rotation * m + i`, for `i` from 0 to `m - 1`, turns agent
  `agents[s % n]` on its route at index `(s // n) % len(routes)`. A cycle
  therefore turns at most one route per agent, a window of agents rotates when
  more agents qualify than the maximum targets, and each full pass over the
  agents advances every agent to its next route. `rotation` is a nonnegative
  JSON integer persisted in the state file and advanced by one after each
  discovery cycle that selected turn targets, so consecutive cycles walk every
  route of every agent deterministically and a restart resumes the walk. A missing rotation field is zero; any other type
  or a negative value is corrupt state. A discovery cycle with no turn
  candidate is a failed cycle with outcome `discovery`/`empty`, never an empty
  healthy cycle. Explicit routes and discovery are mutually exclusive;
  configuring both, or neither, refuses to start.

- **TURN-CANARY-8: model-free binding checks.** When enabled, every binding in
  the cycle's inventory that the cycle does not turn receives a model-free
  check, reported as its own result kind, separate from turn success. This
  covers bindings of every kind, routes excluded from turns, routes not
  chosen by this cycle's rotation, and all bindings of a cycle whose turns are
  held by unconfirmed cleanup. The check needs no model, enqueues nothing, and
  resolves no secret. For a binding whose kind is in the configured identity
  kinds, `slack` by default, it reads the existing platform-key
  `GET /channel-identities?provider={kind}` resource at most once per kind per
  cycle and looks for the identity whose `name` equals the binding's adapter.
  Passing needs an active deployment, exactly one such identity, identity
  status `active`, `installation_mismatch` false, and a `credential_ref` that
  is a nonblank string. The check reports the first failing condition, in that
  order, as `deployment_inactive`, `identity_missing`, `identity_ambiguous`,
  `identity_inactive`, `installation_mismatch`, or `credential_missing`, and
  `ok` when all hold. A blank adapter matches no identity. A binding of any
  other kind is checked for an active deployment only. A failed or malformed
  identity read reports `read_failed` for that kind's bindings. When several
  inventory rows share one route label, the label reports the first failing
  outcome among them. A present reference proves
  configuration, not that the referenced secret resolves or that the provider
  accepts it. Binding checks do not change cycle success. Diagnostics name
  only route labels and outcome codes; a reference value is never logged or
  exported.

- **TURN-CANARY-9: adapter health.** The operator may name adapter health
  endpoints as a JSON object from an adapter name to an absolute HTTP(S) URL
  without user information or fragment. At most 20 adapters are named, and a
  name matches `[a-z0-9][a-z0-9-]{0,62}`. Each cycle sends one `GET` per
  endpoint with a finite timeout, no credential header, and no redirect
  following. Any 2xx status is up; any other status, a transport error, or a
  timeout is down, with outcome `ok`, `http_status` (with the code), or
  `unreachable`. The endpoint URL and response body never appear in logs or
  metrics. Adapter health is reported per adapter, separate from turn and
  binding results, and does not change cycle success. It suits channel
  adapters that serve an HTTP health route; it does not prove that a Slack
  socket session or any adapter's provider connection is delivering events.

- **TURN-CANARY-10: exporter mode.** When a listen address is configured, the
  process runs as a long-lived exporter instead of a one-shot command. One
  loop runs one cycle at a time: a cycle starts at most once per configured
  interval and never while the previous cycle, including its cleanup, is still
  running. A cycle that overruns its interval is followed immediately by the
  next and increments an overrun counter; cycles never overlap or queue. Each
  cycle opens the state journal under its process lock and releases it at the
  end, so a concurrent one-shot invocation on the same state file is refused,
  not run alongside. Route, binding, and adapter observations persist across
  cycles, so a route turned by an earlier rotation keeps its last series until
  a successful inventory read no longer lists it. The server answers
  `GET /metrics` with the last exposition in Prometheus text format and
  `GET /healthz`. Health reports the monitor itself, not the platform: 200
  while the last cycle finished within twice the interval plus the maximum
  cycle duration (maximum targets times the sum of turn and cleanup
  deadlines), counted from process start before the first cycle, and 503
  otherwise. A failing platform therefore shows in the metrics without
  restarting the monitor. Any other path is 404, any other method is 405, and
  a request head beyond a fixed size or read timeout is closed. Responses
  contain no configuration value. A metric name prefix matching
  `[a-zA-Z_:][a-zA-Z0-9_:]*`, defaulting to `turn_canary`, replaces the
  leading `turn_canary` of every exported name in both the file and HTTP
  outputs, so an operator can keep existing series names.

- **TURN-CANARY-11: binding drift.** The operator may declare the bindings an
  installation is meant to have, as a JSON list of `(kind, address, adapter)`
  triples. After each successful inventory read, every declared triple that no
  inventory row carries is reported with side `declared_only`, and every
  inventory triple of a declared kind that is not declared is reported with
  side `live_only`. Kinds absent from the declaration are not compared. Drift
  is reported per triple and as a total, separate from turn, binding and
  adapter results, and does not change cycle success. A failed inventory read
  reports no drift for that cycle; it never reports an empty drift as healthy.

- **TURN-CANARY-12: binding adapter health.** The operator may name one health
  path, beginning with `/`. Each cycle then derives one health URL per
  inventory binding whose agent has an active deployment and whose stored
  `endpoint` is an absolute HTTP(S) URL without user information, query or
  fragment: the endpoint's scheme and authority followed by the configured
  path. Each distinct URL is probed once per cycle under TURN-CANARY-9's rules,
  at most 100 per cycle, and the outcome is reported for every binding that
  shares it. A binding whose endpoint is present but not such a URL reports
  `endpoint_invalid` without a request; a binding without an endpoint is not
  reported. URLs never appear in logs or metrics. The result does not change
  cycle success.

- **TURN-CANARY-13: coverage.** When enabled, each successful inventory read
  reports every agent that has an active deployment but neither a binding that
  TURN-CANARY-7 would accept as a turn candidate nor, when TURN-CANARY-12 is
  enabled, a binding with a derived health URL. Such an agent is reported as
  uncovered by its agent name, with a count. Coverage describes what the
  monitor can observe, not whether the agent works, and does not change cycle
  success.

- **TURN-CANARY-14: label layout.** By default each route series carries one
  `route` label holding the JSON `(kind, address, adapter)` object. With the
  split layout, route series instead carry separate `agent`, `kind`,
  `address` and `adapter` labels, where `agent` is the agent name from the
  inventory and is empty for a declared-only drift triple. The layout applies
  to target, binding check, binding adapter and drift series in both the file
  and HTTP outputs; adapter series of TURN-CANARY-9 keep their `adapter` label.

## Verification

Unit tests under `examples/tests` exercise the selected binding and
`QueuedTurn` wire, relay completion, exact scoped reset key, failure and
cleanup stop cases, limits, and safe diagnostics. They also exercise
deterministic discovery and rotation, the model-free binding outcomes, adapter
probe outcomes against a local HTTP server, the exporter routes and
non-overlapping loop, the metric prefix, quantity parsing, drift, binding
adapter health, coverage and the split label layout, all with synthetic rows. Isolated runtime
qualification uses a disposable agent and selected binding with a real model
and protected read-only runner, verifies a delivered nonce, inspects the
runner trace for no action or approval, confirms the reset result, and checks
the negative path with an intentionally mismatched nonce. This proof is
recorded for the exact candidate and costs an explicitly approved budget.
No unit or fake-model test alone establishes that runtime qualification.
