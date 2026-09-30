# 159. A canary is a weighted second active deployment and every trace carries its version

Date: 2026-09-01

Status: Draft

This Draft records an aspirational architecture decision ahead of any
implementation. Nothing described here exists on `next` today, and this
document does not authorize building it.

## Context

The Curie UI is converging on a plant-manager view: one card per agent showing
success and error rate, cost, and volume over a window. The manager's loop is
glance, decide, delegate — spot a problem on the card, take a coarse action,
and send a runner (a coding agent) down to do the detailed analysis and report
back. A canary release is the representative member of that class of
higher-level manager actions: the human decides "ship 1.3.0 to 1% of threads
for seven days" at a glance, the platform runs the split and the measurement,
and the agent later answers "look at all of the canary traces and make sure
they are looking good." The decisions below — a version dimension on every
trace, a comparison contract the UI can read, a human who decides and an agent
who reports — are the template for that whole class, not a one-off for canary.

Three facts about the current system frame the decision.

First, there is no traffic splitting anywhere. `Deployment` in
`apps/api/src/curie_api/models.py` carries `agent_id`, `version_id`,
`environment`, `commit_sha`, `workspace_enabled`, a plain-string `status`, and
`deployed_at` — no weight, percentage, or split of any kind.

Second, "the current version" is already an ordering over multiple live rows,
not a single-row invariant. There is no unique constraint on active
deployments per `(agent, environment)`; git-flow's `process_push`
(`apps/api/src/curie_api/gitflow.py`) appends a new active row per push and
leaves older rows untouched. The worker's binding resolver
(`apps/worker/src/curie_worker/binding.py`, `_RESOLVE_SQL`) joins agents to
their active deployments and orders by `(environment='prod') DESC,
deployed_at DESC, id DESC` — newest active wins. That resolution runs inside
the per-event turn loop (`kernel.py`), so every turn re-resolves; no thread is
pinned to a version today.

Third, and the deeper blocker: the metrics plane cannot distinguish versions.
`agent_trace_filter()` in `apps/api/src/curie_api/metrics.py` selects an
agent's traces by a `contains` match on the Langfuse trace name, which the
runner sets to `curie-run:agent-<agent_id>-thread-<ts>`
(`runner/src/curie_runner/__main__.py`). No version appears in the name, and
product-run traces carry no serving-version or deployment dimension in tags
or metadata either — the worker's eval recorder
(`apps/worker/src/curie_worker/eval/recorder.py`) already tags *eval* traces
with `version:` and `suite:`, which proves the mechanism, but the traces the
manager's card is built from set nothing of the kind. Traffic splitting
without a version dimension on traces would produce a canary that cannot be
measured — the split would exist and the card comparing canary to base could
never be drawn.

## Decision

### A canary is a second active deployment row with a rollout weight

`Deployment` gains a rollout weight (a percentage; the exact column name is an
implementation detail). Starting a canary creates a second active `Deployment`
row for the same `(agent, environment)` pointing at the canary
`AgentVersion`, with its weight set to the canary share. The base row is not
modified; its share is the remainder. Ending a canary — either direction —
ends one of the two rows.

The canary names its base. The database already holds more than one active
row per `(agent, environment)` — `process_push` appends one per push and the
resolver joins them all — so "sample among the active rows" would expose
historical versions that today take no traffic only because newest-wins
ordering shadows them. Starting a canary therefore binds the canary row to
the specific base deployment the resolver currently selects, and weighted
sampling happens strictly between that pair. Every other active row remains
outside the split and continues to receive nothing, exactly as it does
today; whether such shadowed rows should also be retired is an
implementation question this ADR does not decide.

For v1 a canary must be connector-compatible with its base: the two bundles
declare the same connector surface. Connector reconciliation
(`connector_loop._TARGETS_SQL` in `apps/worker`) collapses an agent's active
deployments to a single version, and its pinned invariant matches the
reconciled connectors to the sandbox version — two live versions with
different connector sets would leave one cohort's connectors pruned or
replaced underneath it. Version-scoped connector ownership is real work with
its own blast radius; it is deferred, and until it exists the platform
refuses a canary whose connector surface differs from its base's.

This follows the grain of the existing model rather than cutting across it.
Multiple active rows per `(agent, environment)` are already the system's
shape, and the resolver already arbitrates between them; the delta is that
arbitration becomes weighted sampling instead of newest-wins. A canary flag or
percentage on the single base row was rejected (see Alternatives) because one
row cannot carry two versions' lifecycles: the canary must be stoppable,
promotable, and attributable independently of the base, and each side's traces
must attribute to its own deployment id.

### Threads are the unit of splitting, and a thread's assignment is sticky

The canary weight is a share of *threads*, not of turns or channels. When a
new thread first reaches an agent that has a canary, the platform samples once
against the weight, assigns the thread to the canary or the base deployment,
and persists that assignment. Every subsequent turn of the thread is served by
the assigned deployment.

Stickiness is load-bearing for a conversational agent. A thread is one
conversation with one human; switching versions mid-thread would hand the
conversation to an agent with different behavior, different prompts, possibly
a different tool surface, silently and repeatedly. It would also contaminate
the comparison: a thread served by both versions belongs to neither cohort,
and the manager's later instruction — "look at all of the canary traces" — has
no clean answer. Per-turn and per-channel splitting are rejected in
Alternatives.

The persisted per-thread assignment has an established precedent:
`ThreadWorkspace` (`models.py`) is already a per-`(agent_id, conversation_id)`
record created on first contact, with `selected_by_deployment_id` recording
which deployment made the selection. The canary pin is a new record of the
same shape, created on the thread's first turn — with one correction to the
key. An adapter conversation id is unique only within its channel, and the
worker documents the channel kind, address, and conversation id together as
the load-bearing thread identity (`kernel.py`); a pin keyed by the bare
`(agent_id, conversation_id)` pair could collide across unrelated
conversations on an agent bound to multiple addresses. The pin is keyed by
that full scoped thread identity.

### The worker's binding resolver evaluates the split and persists the pin

The split is decided where deployment selection already happens: the worker's
`BindingResolver`. On a thread's first turn it samples against the weights of
the active rows and writes the pin; on later turns it reads the pin and
resolves to the pinned deployment while that deployment is still active.

The alternatives put the decision in the wrong place. The dispatcher routes
events but knows nothing about deployments, so teaching it to split would
duplicate deployment knowledge into a component that has none. Resolving in
the API would move selection away from the one component that already performs
it per event, and every consumer of "current deployment" would have to agree
on the new semantics. The resolver is the single existing seam through which
every served turn already passes; the split lives there or the system grows a
second resolution authority.

### Every trace carries its serving version as a structured Langfuse tag

The runner tags every trace with the serving `AgentVersion` (its
`version_label`) and the serving `Deployment` id, as Langfuse tags, leaving
the trace name unchanged. The worker passes the resolved identity into the
runner's boot configuration alongside what it already passes.

The trace name was the tempting shortcut: today's metrics filter is a
`contains` match on the name, so appending `-version-<label>` would have
worked without touching `_filters()`. It is rejected because it bakes an
ever-growing, order-sensitive encoding into a string that other code already
substring-matches — each new dimension would make the name longer and every
parser more fragile. Tags are a queryable dimension Langfuse supports
natively; the honest cost is extending `agent_trace_filter()` / `_filters()`
in `apps/api/src/curie_api/metrics.py` to combine the existing name filter
with a tag filter, and that cost is paid once.

Passing the identity through boot configuration is necessary but not
sufficient. The runner's traces flow through the closed OTel attribute
registry decided by ADR-0076 (`packages/telemetry-schema`), and the export
processor (`runner/src/curie_runner/otel.py`) strips attributes the registry
does not name. Realizing this decision therefore extends that registry with
the serving-version and deployment attributes — an explicit amendment in the
ADR-0076 chain that fixes the attribute names and value types — rather than
smuggling values past it.

This tag is the reusable asset of the whole decision. Any future manager
action that compares cohorts — canary vs base, before vs after a prompt
change, channel A vs channel B — needs traces that carry structured
dimensions rather than encoded names. Canary is merely the first consumer.

### Promote ends the base, rollback ends the canary, and pins never outlive their deployment

Promotion stops the base row and the canary row becomes the sole active
deployment at full weight — the same row, same deployment id, so the canary
cohort's history remains attributable after promotion. No new deployment row
is minted for a promote; the version was already deployed, observed, and
judged, and re-deploying it would sever the trace lineage the judgment was
based on.

Rollback stops the canary row. Threads pinned to a stopped deployment are not
stranded: a pin is honored only while its deployment is active, so the next
turn of a canary-pinned thread re-resolves to the base. This is a deliberate
breach of stickiness and the correct one. Stickiness protects the
conversation from arbitrary version churn; a rollback is an operator's
emergency judgment that the canary version must stop serving humans now, and
that judgment outranks continuity. The mid-thread switch is the cost of the
override, accepted explicitly.

Re-resolving the pin is not by itself enough to move the thread. The sandbox
substrate reuses a running thread-affine sandbox
(`apps/worker/src/curie_worker/sandbox/substrate.py`), and on adoption the
kernel deliberately ignores the newly resolved boot environment — so a
canary-pinned thread with a live sandbox would keep executing the rolled-back
version inside it. Serving a thread whose pinned deployment has stopped
therefore requires comparing the live sandbox's deployment identity against
the newly resolved one and replacing the sandbox at a fenced turn boundary,
the mechanism ADR-0136 established for late workspace handoffs. The same
applies to base-pinned threads after a promotion.

### The UI reads one comparison contract built from MetricsSummary

The metrics endpoints (`/observability/metrics/summary` and
`/observability/metrics/series`, `routers/observability.py`) gain a
version/deployment filter dimension backed by the trace tag, and a compare
shape returns one `MetricsSummary` per side —
`{base: MetricsSummary, canary: MetricsSummary}` keyed by deployment — over a
caller-chosen window. `MetricsSummary` (`schemas.py`) already carries
everything the card needs per side: runs, latency p95, tokens, cost,
cost-known, error rate. The comparison contract is composition, not a new
metrics vocabulary.

The same filter dimension is the runner-investigation handoff: when the
manager sends the coding agent down to inspect the canary, "all of the canary
traces" is a single tag-filtered query, not a heuristic over trace names and
timestamps.

### A human promotes; the platform measures; the agent reports

Automated promotion or rollback on a metric threshold is explicitly out of
scope, as a product position rather than a deferral. The manager-action model
is that the human decides at a glance and delegates analysis, not that the
plant runs itself. Error rate, latency, and cost are proxies; for a
conversational agent the judgment "the canary is looking good" includes
reading how conversations actually went, which is exactly the analysis the
manager delegates to an agent. The agent summarizes and recommends; the
promote and rollback verbs remain human-invoked.

## Current implementation state

Nothing in this ADR is implemented. An implementation would touch, at
minimum: the `Deployment` weight column and its migration; weighted sampling
and the per-thread pin record in the worker's `BindingResolver`; the
connector-compatibility refusal at canary start; the ADR-0076 registry
amendment and version/deployment tagging in the runner's trace setup;
sandbox replacement at the fenced turn boundary when a pin's deployment
stops; the tag-aware extension of the metrics filters; the comparison
contract in the observability API; the promote/rollback verbs; and the UI
card that renders the comparison. Those are touch points, not a plan —
issues will decompose and track the work if this Draft is accepted.

## Consequences

The manager gains the canary loop as one glanceable action: start a 1% canary,
watch one card compare the cohorts, send an agent to read the canary's traces,
promote or roll back. Each piece — the weighted second row, the pin, the tag,
the compare contract — is also independently useful to the next manager
action of the same shape.

Every trace gains a structured version dimension. Cohort comparison stops
depending on what can be parsed out of a trace name, and the metrics filter
grows its first tag-based dimension, which later dimensions can follow.

A real user in a real channel can be served by a bad canary turn. The exposure
is bounded by design: the weight caps how many threads are exposed,
stickiness caps it to whole conversations rather than scattering bad turns
across many, the version tag makes every exposed conversation findable, and
rollback stops the bleeding on the next turn of every pinned thread. That
bounded live exposure is also precisely the information no offline gate can
produce — how the new version behaves with real users, real channels, and
real distribution.

The resolver becomes the single owner of split-and-pin semantics. Any future
consumer of "which deployment serves this thread" must go through it rather
than reimplementing the ordering, which hardens a boundary that today is only
a comment-level agreement between `binding.py` and the connector loop.

Rollback's pin override is a documented behavior, not an edge case: operators
can be told exactly what canary-pinned threads experience when they pull the
lever.

## Alternatives considered

### Gate promotion on the eval suite instead of running a canary

Rejected as a replacement; endorsed as a complement. The eval mechanism
exists (`POST /evals/trigger`; git-flow fans out eval jobs on dev pushes) and
gating promotion on it is cheaper than any live-traffic mechanism, catches
regressions before any user sees them, and should remain the first line. But
evals replay a curated distribution. They cannot surface how a new version
behaves with real users, live channels, drifted traffic, and the tool-call
side effects of genuine conversations — which is the specific information the
manager's canary question asks for. An eval gate and a canary answer
different questions; the second is this ADR's subject.

### Shadow or mirror runs on copied live traffic

Rejected as the primary mechanism. Shadowing is attractive because no user
ever sees a bad turn, but a conversational agent acts — it posts to channels
and calls tools with side effects. A shadow either performs those actions
(double side effects) or suppresses them, at which point it is no longer
executing the behavior being evaluated, and its divergence from the real
version grows with every suppressed action. It also doubles cost on mirrored
traffic. For a strictly read-only agent shadowing could be a useful
complement, but the platform cannot assume its agents are read-only.

### Split per turn

Rejected. Per-turn sampling churns a single conversation across versions,
which is visible and incoherent to the human in the thread, and it destroys
cohort attribution — a thread served by both versions belongs to neither.

### Split per channel

Rejected. Channels are too coarse to realize a 1% share for most agents, and
channel assignment confounds the comparison: channels differ systematically
in traffic, tone, and task mix, so version effects and channel effects cannot
be separated.

### Encode the version in the trace name

Rejected. It fits today's `contains` filter without code changes, but it
turns the trace name into an accreting positional encoding that every
consumer must substring-parse, and the next cohort dimension makes it worse.
Tags pay a one-time filter-extension cost for a clean, queryable dimension.

### A canary weight or flag on the single active deployment row

Rejected. One row cannot represent two versions with independent lifecycles.
Promote, rollback, pin attribution, and per-side metrics all need each
version to be its own deployment identity; a flag on the base row would smear
the canary's identity across a row whose `version_id` says otherwise.

### More than two live versions, and time-boxed auto-expiry

Deferred, not decided. Weighted sampling generalizes to N rows and a canary
could carry an expiry that ends it automatically after the observation
window, but both add operator surface before the two-version loop has been
proven. The seven-day window in the motivating scenario is an observation
period the manager watches, not a timer the platform enforces, until
experience says otherwise.

### Automated promotion on a metric threshold

Rejected as a product position, recorded in the Decision. The human decides;
the platform measures; the agent analyzes and reports.
