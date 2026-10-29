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
  conversation ID, canonical UUIDv4 relay reply ref, and random nonce. The
  conversation ID is a fixed canary prefix followed by a fresh UUIDv4, so an
  operator's log filters can tell canary threads from every other eval
  conversation. The prefix defaults to `eval:turn-canary-`; an operator may
  replace it with any value matching `eval:[A-Za-z0-9._-]{0,48}`, which keeps
  the worker's `eval:` isolate prefix that every conversation-ID check of this
  example and the worker requires. Any other prefix refuses to start. By default its prompt asks only for that nonce; TURN-CANARY-16 permits an
  explicit role-appropriate question. Every prompt explicitly forbids tools,
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
  finite deadline. Without a profile, cycle qualification needs a `turn.completed`
  event with outcome `delivered`, observed delivery as defined below, and a reply
  update whose entire text equals the nonce. A reply
  alone, an approval pause, dropped or escalated completion, a mismatched
  nonce, malformed event, or a timeout fails. No reply is posted to a channel.
  Independently, delivery is observed only when the production `ReplyEvent`
  parser accepts every observed event, every target matches the enqueued
  turn's kind, address, conversation ID and reply reference, exactly one
  completion matches its event ID and has outcome `delivered`, and the latest
  text update is nonblank. Text may be streamed before completion; a later
  status clear is allowed. Placeholder, status and progress events alone are
  insufficient. Duplicate completions and malformed or unrelated events fail
  both observations closed. Cursors must be nonnegative integers excluding
  booleans and advance by the page event count; terminal flags must be booleans
  consistent with the observed completion. The checker assesses only events
  observed before return, without sealing the relay against later writes.
  Delivery is lifecycle evidence, not proof of model execution, useful task
  completion, policy compliance or final-text provenance. The legacy target nonce
  result and target last-success timestamp retain their meaning: exact success
  uses the last `reply.update` literally, including a
  progress edit, and accepts only `reply.update`, `turn.status` and
  `turn.completed` event types. A valid `reply.post` therefore cannot broaden
  exact success. An `awaiting-approval` completion uses the relay's nonterminal
  page shape and fails the probe as `not_delivered`.

- **TURN-CANARY-4: owned cleanup.** After every enqueued probe, including
  timeout and error paths, the checker computes the worker thread key with
  `channel_protocol.scoped_conversation_id` and the selected binding identity.
  Before the stream enqueue can begin, it durably records the exact selected
  agent ID and owned thread key in its persistent state, under the selected
  route's `(kind, address, adapter)` label in `pending_routes`. A crash or
  cancellation leaves this intent in place. While a route has a pending
  cleanup, no cycle turns that route; other routes keep turning. The intent
  clears only after a matching reset confirms release as defined below, never
  merely after a reset request.
  State updates use a separate stable process lock and an atomic replacement
  of a synced temporary file, followed by syncing the containing directory.
  An existing empty or corrupt state file fails closed; only a genuinely absent
  state file is a first-run empty state. An interrupted write cannot turn a
  recorded cleanup intent into a healthy empty state.
  `cleanup_blocked` must be a JSON boolean; numeric 0 or 1 is corrupt state,
  even though Python compares them equal to false or true. A true
  `cleanup_blocked` holds every route. The checker never sets it; it is an
  operator hold, or the companion of a single `pending` record written by an
  earlier version of this checker. Such a legacy `pending` record names no
  route, so it also holds every route until it clears, and when it clears the
  checker sets `cleanup_blocked` to false with it. A journaled cycle honors
  these holds directly, without relying on a caller to copy them into the
  platform. `pending_routes` must be a JSON object from a route label to an
  object with string `agent_id` and `thread_key` and an optional finite
  positive `checked_at`. Persisted capacity skips must be a nonnegative JSON
  integer, and persisted success timestamps must be finite positive JSON
  numbers, never booleans. Invalid types or values fail before enqueue or
  metric export.
  It submits that key to `POST /agents/{agent_id}/threads/{thread_key}/reset`
  and polls the matching reset status endpoint until `requested` is false and
  `route_existed` is true. It never resets an unowned or guessed conversation.
  Within the probe's own cycle, a false or unknown `route_existed`, a pending
  reset beyond the deadline, or any unknown status leaves cleanup unconfirmed:
  the absence of a route right after the turn is not proof of release, because
  the queued turn may not have claimed its route yet. The cycle marks that
  route's cleanup degraded, keeps its pending record, and continues with the
  next selected route. Each route therefore holds at most one unconfirmed
  probe at a time.

  Each later cycle re-checks pending cleanups before selecting targets: at
  most the configured maximum targets per cycle, least recently checked first.
  A re-check submits the recorded key to the same reset endpoint and polls the
  reset status within the cleanup deadline. The record clears when the reset
  drains, that is `requested` is false, with `route_existed` true (the worker
  released the route) or false (the worker found no route under the exact owned
  key at a drain at least one cycle after the probe, so it holds nothing for
  this probe). A pending reset beyond the deadline, an unknown `route_existed`,
  or an error keeps the record and stamps `checked_at`. A cleared route is
  turned again from the same cycle on.

- **TURN-CANARY-5: bounded operation.** Probes run serially. Cycle interval,
  turn deadline, cleanup deadline, poll period, and maximum selected targets
  are positive, bounded settings with documented defaults. A cycle is also
  bounded by those limits, including its pending-cleanup re-checks. No cycle
  turns a route whose cleanup from an earlier probe is unconfirmed, and no
  cycle turns any route while a hold on every route remains (TURN-CANARY-4).
  An optional ResourceQuota headroom check is an observation immediately
  before enqueue: insufficient or unreadable headroom skips the turn with an
  explicit `capacity` outcome. Its bearer token comes from exactly one of a
  static configured token or a configured token file path; configuring both,
  or a quota URL with neither, refuses to start. There is no implicit token
  path. A token file is read again for each quota request and its contents
  are kept no longer than that request, so a rotated projected service
  account token is used on the next request. A missing, unreadable, or blank
  token file makes that observation unreadable, and the token never appears
  in logs or metrics. A capacity skip is not a turn failure. It is
  not an atomic reservation and cannot promise a global sandbox cap. Quota
  `hard` and `used` values, and the configured minimum free amount, are read
  as Kubernetes quantities: a plain integer or decimal, an optional exponent,
  and an optional decimal suffix (`n`, `u`, `m`, `k` or `K`, `M`, `G`, `T`,
  `P`, `E`) or binary suffix (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei`), so
  `limits.cpu`, `limits.memory`, `count/pods`, and `pods` all compare
  correctly. A value that is not a quantity makes the quota unreadable.

- **TURN-CANARY-6: diagnostics.** Export a portable Prometheus target success
  gauge, last run and last success timestamps, cleanup degraded gauge, capacity
  skip counter, cycle success gauge, and last cycle timestamp. An independent
  `target_delivery_success` gauge records delivery observation per attempted
  route. A delivered nonblank mismatch sets delivery to 1 and exact nonce
  success to 0. An error or timeout sets delivery to 0. Capacity skips and
  cleanup holds create no delivery sample; the standing exporter retains a
  previous observation until a successful inventory read removes its route.
  Restart does not restore delivery observations from persistent state and
  missing samples never imply healthy delivery. Logs expose only bounded
  assessment reason codes, without adding content or identity labels.
  The target
  success gauge is published only for a route whose turn was attempted: a
  capacity-skipped route publishes no target success sample in that cycle, and
  the exporter keeps the route's previous value. A per-route capacity skipped
  gauge is 1 for a route skipped for capacity in the cycle and 0 for a route
  turned in it. A per-route cleanup degraded gauge is 1 for every route with a
  pending cleanup and 0 for a route whose cleanup was confirmed or cleared in
  the cycle. The global cleanup degraded gauge is 1 while any route has a
  pending cleanup or a hold on every route remains. Cycle success is 1 only
  when the inventory read and admission succeed, no hold on every route
  remains, at least one selected route was turned or skipped for capacity, no
  turned route failed, and no cleanup in the cycle was left unconfirmed.
  Capacity skips and routes held by an earlier pending cleanup therefore never
  count as turn failures; a cycle whose every selected route is held fails. Structured logs
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
  cycle duration (maximum targets times the sum of the turn deadline and
  twice the cleanup deadline: one cleanup for each probe and one for each
  pending-cleanup re-check), counted from process start before the first
  cycle, and 503 otherwise. A failing platform therefore shows in the metrics without
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
  to every per-route series, including target, cleanup, capacity, binding
  check, binding adapter and drift series, in both the file and HTTP outputs;
  adapter series of TURN-CANARY-9 keep their `adapter` label.

- **TURN-CANARY-15: binding kind exclusion.** The operator may name binding
  kinds the monitor ignores, as a comma-separated list of distinct nonblank
  kinds, empty by default. After each inventory read, rows of an excluded kind
  are removed before every per-binding observation: TURN-CANARY-1 selection,
  TURN-CANARY-7 discovery, TURN-CANARY-8 binding checks, TURN-CANARY-11 drift,
  TURN-CANARY-12 binding health and TURN-CANARY-13 coverage. An excluded
  binding is therefore never turned, checked, probed or compared, and an agent
  whose only bindings are of excluded kinds is not reported as uncovered. Its
  earlier per-route series are dropped like those of any route the inventory no
  longer lists. Exclusion matches the stored kind exactly; agent names,
  addresses and adapters are never exclusion criteria. With the list empty,
  every cycle behaves as before.

- **TURN-CANARY-16: explicit probe profiles.** Nonce-only probes remain the
  default. An operator may set `TURN_CANARY_PROBE_PROFILES` to a nonempty JSON
  object from exact inventory agent slugs to objects containing the required
  `prompt`, `required_any`, and `forbidden_any` fields, with the optional
  prefix exclusion defined by TURN-CANARY-18. The parser rejects duplicate
  keys, unknown or missing fields, malformed JSON, and empty profiles before
  any enqueue. The JSON is at most 65,536 UTF-8 bytes and names at most 100
  profiles. Slugs are 1 to 63 lowercase ASCII letters, digits or hyphens,
  beginning with a letter or digit. Prompts are nonblank strings of at most
  2,048 characters. `required_any` contains 1 to 8 nonempty lists of 1 to 8
  marker strings; `forbidden_any` contains 0 to 32 marker strings. Each marker
  is 1 to 128 characters and nonblank after normalization. These bounds also
  apply to programmatic construction. An omitted environment variable means
  no profiles; an explicitly empty value or object is invalid.

  Only a selected route whose exact inventory agent slug has a profile uses
  its prompt. Missing or unmatched slugs keep the original nonce question.
  A profile does not select routes or change read-only runtime qualification, isolation, wire
  correlation, timeout, capacity, cleanup, or agent safety policy. Its question
  receives the same explicit no-tools, no-actions, no-approvals and
  no-durable-changes suffix as the nonce question. Operators choose a
  role-appropriate question answerable without tools; profiles cannot grant
  permissions or require the agent to override its role.

  A profile passes only after TURN-CANARY-3 establishes delivery of a nonblank
  final non-progress reply update correlated to the enqueued turn. Normalize
  the final text and every marker using Unicode NFKC, case folding, and
  whitespace collapse. Every required group must have at least one literal
  substring match, and no forbidden marker may occur. No regular expression
  is evaluated. Progress text cannot satisfy a marker. Missing, blank,
  malformed, foreign, non-delivered or incomplete replies fail. A refusal
  is not automatically accepted: the configured affirmative markers must all
  be present and its forbidden markers absent. Matching is a deterministic
  text contract, not a semantic judgment or proof of useful work beyond that
  contract. Operators should include known refusal markers in forbidden_any.

  `target_probe_success` independently reports this result, or exact nonce
  success for an unprofiled route. `target_probe_profile` is 1 for a configured
  profile and 0 for the nonce default. Both have only the existing route labels;
  profile prompts and marker text never become labels or logs. Profile logs
  use only `profile_match` or `profile_mismatch` in the probe phase, while
  relay lifecycle reason codes remain unchanged. Enqueue and relay failures
  publish probe failure. Capacity skips and cleanup holds publish neither
  probe field, retaining previous observations in standing mode.

  `target_last_probe_success_timestamp_seconds` records an independently
  passing configured probe (or exact nonce default).
  `target_last_delivery_success_timestamp_seconds` records actual delivery
  success, independently of nonce or profile matching. Both timestamps are retained across
  exporter rotations until inventory removes the route, but is not restored
  after restart; a missing timestamp is not healthy delivery. Existing
  exact_nonce, target_success and target nonce-success timestamps retain their
  original meaning. Cycle qualification instead requires both delivery_success
  and probe_success on every attempted route, so the configured profile replaces
  the diagnostic nonce only for that route's cycle verdict. A passing profile
  with a different nonce can qualify; an exact nonce cannot hide a failed
  configured profile. Enqueue, delivery, selected-probe or owned-cleanup failure
  keeps the cycle failed even when another route subsequently passes. Capacity
  skip and held-route rules remain those of TURN-CANARY-6. Global cycle
  last-success and one-shot exit status follow this qualified cycle result.
  Turn-phase logs describe actual delivery; probe-phase logs describe the
  configured text verdict, and relay reason codes remain independent. Alerts
  use delivery timestamps plus last-run freshness rather than interpreting
  the legacy nonce result as delivery or profile qualification.

- **TURN-CANARY-17: standalone profile schema and source assembly.**
  `probe_profiles.py`, beside `turn_canary.py`, is the single owner of the
  profile schema, normalization, literal matcher, strict JSON parser and
  programmatic mapping validation from TURN-CANARY-16. It depends only on the
  Python standard library. A consumer can load this helper file alone by
  `importlib.util.spec_from_file_location` and call `ProbeProfile`,
  `ProbeProfile.matches`, `parse_probe_profiles` and `validate_probe_profiles`
  without importing ACI, channel protocol, Pydantic or the runnable checker.
  Validation, bounds and matching behavior remain TURN-CANARY-16's contract.

  The runnable checker consumes this sibling and re-exports its existing
  `ProbeProfile` and `parse_probe_profiles` APIs. Both script execution and
  file-based dynamic import work from a clean source directory containing the
  two files, with the checker's normal runtime dependencies available. Sibling
  resolution uses the checker's own source directory, without mutating global
  import search paths, extracting source fragments, installing dependency
  substitutes or falling back to another checkout's helper. Shipping the
  checker requires both files in the same directory. A previously imported
  helper is reused only when its registered module resolves to this exact
  sibling file, preserving profile class identity across helper and checker
  imports. Normal helper imports and file-spec imports registered in
  `sys.modules` before execution are supported. Repeated checker imports share
  that sibling; different source directories stay isolated. If the canonical
  `probe_profiles` module name is vacant, the checker registers the exact
  sibling under that name too, preserving profile class identity for normal
  helper imports before or after checker loading. An occupied canonical name
  is never overwritten. Arbitrary duplicate file-spec executions under new
  names retain Python's normal independent module identities. A missing helper fails
  before any platform operation. Factoring the helper changes no prompt,
  default, wire, lifecycle, cleanup, log or metric behavior.

- **TURN-CANARY-18: optional response-prefix exclusions.** A profile may add
  `forbidden_prefix_any`, defaulting to an empty list in JSON and an empty
  immutable tuple in `ProbeProfile`. Existing three-field JSON and three-argument
  programmatic construction remain compatible. The optional field contains
  0 to 32 marker strings with TURN-CANARY-16's existing nonblank, 128-character
  marker bound and immutable canonicalization. Invalid collection types,
  non-string, empty, blank or overbound markers refuse parsing/construction.
  The strict parser accepts the three required fields plus this optional field
  only; all other unknown fields, duplicate keys and the existing total JSON
  byte bound remain rejected.

  Apply TURN-CANARY-16's Unicode NFKC, case folding and whitespace collapse to
  the response and prefix markers. A response fails when its normalized text
  starts with any normalized prefix marker. The same literal wording later
  in the response does not trigger this prefix exclusion. Required marker
  groups still all apply, and `forbidden_any` continues to reject matching
  text anywhere. No regex, language inference or sentence parsing is used.
  The standalone schema module remains the sole owner. Source assembly,
  module identity, default nonce behavior, wire and lifecycle validation,
  cleanup, logs and metrics retain TURN-CANARY-16/17's meanings.

## Verification

Unit tests under `examples/tests` exercise the selected binding and
`QueuedTurn` wire, relay completion, exact scoped reset key, failure and
per-route cleanup holds and their recovery, capacity skips, Kubernetes
quantity parsing, limits, and safe diagnostics. They also exercise
deterministic discovery and rotation, the model-free binding outcomes, adapter
probe outcomes against a local HTTP server, the exporter routes and
non-overlapping loop, the metric prefix, quantity parsing, drift, binding
adapter health, coverage, the split label layout and binding kind
exclusion, all with synthetic rows. Isolated runtime
qualification uses a disposable agent and selected binding with a real model
and protected read-only runner, verifies a delivered nonce, inspects the
runner trace for no action or approval, confirms the reset result, and checks
the negative path with an intentionally mismatched nonce. This proof is
recorded for the exact candidate and costs an explicitly approved budget.
No unit or fake-model test alone establishes that runtime qualification.
