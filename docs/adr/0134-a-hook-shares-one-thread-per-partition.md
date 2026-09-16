# 134. A hook shares one thread per partition

Date: 2026-08-29

Status: Draft

**Amends [ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md)** by narrowing
the thread identity of the ingress its Decision 1 authorized: one thread per
hook becomes one thread per *partition* of a hook, opt-in per hook and off by
default. ADR-0079's own decisions stand unchanged — the API still accepts
inbound triggers, a trigger is still a placeholder-less event kind whose output
the kernel posts, and a job still waits for idle rather than steering a live
interactive session. Only the granularity of "the thread" moves. The per-hook
reasoning actually being narrowed was never written in 0079's text; when
ADR-0079 was implemented it lived in the `_conversation_id` docstring of
`apps/api/src/curie_api/routers/hooks.py`, and it now lives in
`curie_api.hook_partition.conversation_id`, which this ADR restates and
amends.

## Context

A hook fires once and finds N independent things: a sweep over open pull
requests finds three PRs, a triage sweep finds five issues, a reconciler finds
seven drifted resources. Each is separate work with its own history and its own
conclusion, and nothing orders them against each other.

Today all N collapse into one thread. The ingress mints
`hook:<agent_id>:<hook>` once per hook, the worker keys a sandbox claim on it,
and the second and third deliveries defer behind the first with
`ThreadBusyError`. That was the right default and remains the right default: per
*delivery* would claim a fresh sandbox for every event and let two firings of
the same hook race with no ordering at all, which is exactly what ADR-0079's
"jobs are outputs, not steering inputs" rules out. The defect is not that the
hook shares a thread. The defect is that "the hook" is the only granularity
available, so a hook whose deliveries genuinely are about different things has
no way to say so.

The maintainer settled the shape on 2026-08-27 as **partition now, delegation
later**. A hook that finds N things could instead delegate N sub-turns to other
agents; that is [ADR-0115](0115-agents-call-each-other-with-no-third-party.md)'s
question, it is Draft, it needs a credential no sandbox holds, and it is
explicitly **not** this decision.

A 2026-08-28 mechanism spike against the real kernel, `SandboxSubstrate`,
`RunnerClient` and a real Valkey observed three partitioned deliveries claiming
three sandboxes and running three turns concurrently with intra-partition
serialization intact, and an unpartitioned hook still serializing into one. The
mechanism is not in question here; the granularity policy is.

## Decision

**A hook's conversation id gains an optional fourth segment.** The id becomes
`hook:<agent_id>:<hook>:<partition>` for a hook that opts in, and stays
`hook:<agent_id>:<hook>` for every hook that does not. An unpartitioned hook
mints a byte-identical and SHA-256-identical string to the one it mints today,
which is what keeps every artifact keyed on that id — Valkey key namespaces,
claim names, transcripts, boot env — unchanged for every agent that does not
configure this.

**The partition source is an operator-owned column on the agent row**, an
`agents.hook_partitions` JSONB map of hook name to
`{"pointer": "<RFC 6901 pointer>"}`. The operator decides both *that* a hook
fans out and *which* field of the delivery document identifies the thing each
delivery is about. The upstream supplies the value, but only through a field the
operator named.

An `X-Curie-Partition` request header was rejected. It would sit outside the
bytes the HMAC covers, so the value that decides sandbox cardinality would be
unauthenticated even on a correctly signed delivery; and it would hand any
holder of the derived hook secret direct control of this agent's concurrent
sandbox count, which is a resource-amplification surface the operator never
opted into.

Be precise about what the column does and does not buy. It does **not** stop a
holder of the derived hook secret from choosing the partition values: whoever
can sign a delivery still picks what goes in the field the pointer names, and
therefore still drives the resulting cardinality. What the column changes is who
enables that at all. The operator selects an authenticated field — one inside
the bytes the HMAC covers — and by configuring it makes a deliberate, per-hook
decision to let the sender drive cardinality for that hook. The header
alternative gave the sender that power *without* the operator enabling anything,
on every hook, by default. That is the distinction the column exists for.

Draft [ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md)'s
bundle-declared triggers are the long-term home for this declaration. That is
unbuilt. The agent row is the smallest thing that works under the write surface
that exists, and moving the declaration into the bundle manifest later is a
follow-up (FU-2), not a different decision.

**A misconfigured partitioned delivery refuses with 422 and never falls back.**
If the body is not JSON, the pointer does not resolve, or the resolved value is
not a bounded scalar, the delivery is refused naming both the hook and the
pointer. Falling back to the unpartitioned id would collapse N intended threads
into one *silently*: the operator would see a hook returning 2xx, producing
turns, and quietly no longer fanning out. A configuration error must not resolve
into a plausible-looking degraded state, so this one resolves into a loud one.
The derivation runs after signature verification and after the delivery-id
check, so an unsigned caller learns nothing about a hook's configuration, and
before the delivery claim, so a refused delivery holds no claim and a corrected
retry is not deduplicated away.

**422 is terminal for an unchanged delivery.** A retry is meaningful only after
the payload or the operator's pointer has been corrected; resending identical
bytes against unchanged configuration will refuse identically, forever. An
upstream that retries every non-2xx unchanged therefore pays, per attempt, one
signature verification, one lookup of the agent and its channel bindings, and
one bounded parse of the body — the same exposure any authenticated 4xx already
carries on this route — and touches no delivery claim, so it neither consumes
the backlog window nor blocks a corrected delivery behind it. The cost of that loop is that
it produces no run record at all: a hook stuck in it is visible only in the
ingress log, not in anything an operator lists as a failed turn.

**A partition value matches `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`** — one to 63
characters. Uppercase is permitted because the identifiers this exists to carry
are mixed case (Slack ids, ticket keys, branch names). `:` is forbidden so a
four-segment id can never be read as three. `/` is forbidden as a
canonical-identity policy and as defense in depth, **not** because an encoding
is missing: the worker percent-encodes the whole thread key before it becomes a
`CURIE_HISTORY_REF` path segment, so a slash would already be safe on the wire.
It is excluded so that the id an operator reads in a receipt, a log line, or a
dashboard is the same string everywhere and never has a second readable form. A
leading `.`, `-` or `_` is excluded, which
also makes a bare `..` unrepresentable. Unicode is excluded entirely: a
partition derived from a non-ASCII identifier refuses rather than being
transliterated into something that no longer names the thing.

The bound is defense in depth, not the only control. The worker already
percent-encodes each component of its thread key, so a longer or stranger
conversation id becomes one encoded segment and cannot forge another thread's
key. On length, the partition adds at most 64 characters (the value plus its
separator) to the conversation id and therefore to `CURIE_SESSION_ID`, which
stays in the low hundreds of characters; the Kubernetes claim name is
`curie-thread-<sha256(thread_key)[:10]>-<nonce>`, a fixed length whatever the id
does, and so cannot overflow. A pure-function test pins the arithmetic so the
bound cannot drift without someone re-checking it.

**No wire change and no worker production change are needed, and that is
structural rather than lucky.** `QueuedTurn.conversation_id` is a plain `str`,
so a four-segment id round-trips the queued-event contract untouched; and the
kernel's `_thread_key_for` builds its key as
`quote(kind):quote(address):quote(conversation_id)`, comparing the result and
never parsing it. The conversation id is one opaque encoded segment to every
consumer downstream of the ingress.

**The caller learns the id it landed on.** The hook receipt carries
`conversation_id` on every path — the enqueue, both duplicate paths, and the
202 no-claim path — and the ingress info log names it server-side. Without that,
a partitioned hook's operator would have no way to name the thread for the
`GET /approvals?conversation_id=` filter or to compose the key the reset verb
wants, because the id now depends on payload content the operator never sees.

**The receipt's id is an input to the reset verb, not its argument.** The reset
surface is `POST /agents/{agent_id}/threads/{thread_key}/reset`, driven by
`curie … reset-thread <agent> --thread-key <key>`, which passes `--thread-key`
through verbatim. What it consumes is the *worker's* composed thread key, not a
conversation id: `_thread_key_for` joins `quote(kind)`, `quote(address)` and
`quote(conversation_id)` with `:`, percent-encoding each segment with no safe
characters. So the `:` separators inside a hook conversation id are themselves
encoded. A hook delivery replying on Slack channel `C0EXAMPLE1` whose receipt
reads `hook:0f2b7c1e-4a9d-4f2c-9d31-5b6e8a7c0d12:pr-sweep:1481` resets under the
thread key
`slack:C0EXAMPLE1:hook%3A0f2b7c1e-4a9d-4f2c-9d31-5b6e8a7c0d12%3Apr-sweep%3A1481`.
None of this is new to partitioning: it is equally true of an unpartitioned
hook's three-segment id today. What partitioning changes is how often an
operator needs to do it, since a value they never chose now decides which
thread they are naming.

## What the conversation id keys

Fifteen artifacts derive from the conversation id and therefore change shape for
a partitioned hook. Each row records the decision, not just the effect.

| # | Artifact | Call site | What changes | Decision |
|---|---|---|---|---|
| 1 | Sandbox affinity route | `sandbox/affinity.py`, `<prefix>:route:<thread_key>` | one route per partition | **Intended.** This is the partition key itself; everything else follows from it |
| 2 | Claim name + `curietech.ai/thread-hash` label | `sandbox/types.py`, `curie-thread-<sha256[:10]>-<nonce>` | one hook now emits N thread hashes | **Accepted.** Fixed-length SHA, length-safe at any partition; dashboards filtered on a single hash under-report (FU-3) |
| 3 | Valkey thread lock | `worker/config.py`, `curie:worker:lock:<thread_key>` | one lock per partition | **Intended.** This is what makes fan-out work *and* what keeps two deliveries on one partition serialized |
| 4 | In-process order lock | `kernel.py` `_order_locks` | one lock per partition | **Intended.** Already keyed per thread key; two partitions were never serialized against each other |
| 5 | Kill-switch fan-out set | `kernel.py` `interrupt_agent` / `_active_by_agent` | one hook contributes N thread keys | **Accepted.** Kill cost is proportional to fan-out; each interrupt is individually bounded, so correctness is unchanged and only latency scales |
| 6 | `CURIE_HISTORY_REF` | `binding.py`, `.../state/transcript/<encoded thread_key>` | the transcript **splits** per partition | **Intended**, and the reason for the stability requirement below: each partition owns a transcript |
| 7 | `CURIE_SESSION_ID` | `binding.py`, `agent-<id>-thread-<thread_key>` | longer value only | **Accepted**, bounded by the charset cap |
| 8 | `SandboxHandle.session_id` fallback | `sandbox/substrate.py`, `session_id or thread-<hash>` | follows the claim hash | **Accepted**, fixed length by construction |
| 9 | Thread-reset chain | `apps/api` `threadreset.py` + `POST /agents/{agent_id}/threads/{thread_key}/reset`, worker consumer, `Kernel.release_thread` / `interrupt_thread`, CLI `reset-thread --thread-key` | takes the **composed thread key** verbatim and never parses it | **Accepted.** The verb's argument is `<kind>:<address>:<url-encoded conversation id>`, not the receipt's id, so an operator resets one partition by composing that key themselves — true of an unpartitioned hook today, and now needed per partition. No verb resets every partition of a hook (FU-1) |
| 10 | `Approval.conversation_id`, the approvals filter, resume | `apps/api` approvals router | approvals bind to the partition, not the hook | **Correct by construction** for resume; the filter needs the full id, which the receipt now supplies |
| 11 | Behavior-pack shimmer seed | worker behavior pack | a different seed per partition | **Accepted**, cosmetic |
| 12 | `ReplyTarget.conversation_id` on emitted replies | worker reply events | carries the four-segment id | **Harmless today** (the Slack sink ignores it, see below); a future adapter that threads replies on it would fan replies per partition |
| 13 | `ThreadWorkspace` repository selection | `models.py` `ThreadWorkspace`, unique on `(agent_id, conversation_id)`; `crud.get_thread_workspace` / `select_thread_workspace` | one immutable repository selection per partition instead of per hook | **Intended.** A partition *is* a thread, and "one immutable, independent repository selection per conversation" applies to it exactly as to any other thread. The selection is normally made from an opening thread message, and a hook partition has no human to make it, but the kernel's `_route_and_start` extracts a single root repository URL from the hook-rendered event text and submits it for selection, so a payload naming one allowed repository can establish a partition's selection on its own. Selection is nullable: with neither an existing selection nor a usable URL, the partition takes the generic claim path and no repository credential is redeemed. Draft [ADR 0137](0137-coding-tools-are-built-in-and-an-initial-repository-url-selects-the-workspace.md) proposes this clarification and the removal of deployment-time coding enablement; it does not change this Draft's status or constitute acceptance of either decision. |
| 14 | Worker workspace ownership ledger and retained base objects | `workspace.py`, `_WorkspaceOwnership` keyed by `thread_key`, plus the sanitized `PreparedWorkspace` base object it names | durable per-partition retained state, so retained bytes scale with partition count rather than with hook count | **Accepted, but on a shorter lease than the transcript it sits beside.** `_WorkspaceOwnership` rows carry `expires_at_epoch` and are reaped by the existing TTL reaper (`enumerate_expired` → `begin_expired_reap` → `finish_expired_reap`) with no new mechanism, so a partition's workspace base is bounded and eventually released. The transcript is not: it lives in `workflow_state_entries` (`apps/api/src/curie_api/models.py`), which has no expiry column and no reaper, so it persists indefinitely. Both still depend on the partition value being a stable identity, since a per-delivery value retains a fresh workspace base for every delivery until its lease expires, and grows the transcript store without bound in the meantime |
| 15 | `agent_actions.conversation_id` audit/undo records | `models.py` `AgentAction` (indexed `conversation_id`), `crud.list_actions` | actions record the partition, not the hook | **Correct by construction.** The action belongs to the thread that took it, and an undo must not reach across partitions. The consequence is on the query side: an operator listing a hook's actions by conversation needs the full four-segment id, exactly as the approvals filter does |

Eight artifacts are load-bearing and deliberately **unchanged**, several of them
in ways that will look like oversights to a later reader.

| Artifact | Why it does not take the partition |
|---|---|
| Completion outbox and pending set | Keyed per **event id**, and the event id is unchanged |
| Done and side-effect markers | Same: per event id, so a partitioned delivery is still applied at most once |
| Delivery claim key `curie:hook:delivery:<agent>:<hook>:<sha16>` | **Must not** gain the partition. One upstream delivery id must run at most once whatever partition it names; folding the partition in would let a redelivery run a second time under a different partition |
| Backlog key `curie:hook:backlog:<agent>` | Metered per agent, by construction (see the quota below) |
| `CURIE_MEMORY_REF` | Agent-scoped, not thread-scoped. Memory does **not** fragment when threads do — that is the point of the split between the two |
| The reply handle in `_mint_turn` | Built wholly from the channel binding row; where a reply goes has nothing to do with which partition produced it |
| The turn author, `hook:<hook>` | The author is the hook. A partition is a thread, not an identity |
| Slack sink `_thread_ts` | Matches a Slack timestamp against the whole id; a `hook:`-prefixed id has never matched and a four-segment one still does not, so hook replies stay channel-level posts |

## The quota, as a fact and not a gate

The agent backlog quota admits `hook_backlog_limit` (64) deliveries per agent
per `hook_backlog_window_s` (60) seconds, configured in
`apps/api/src/curie_api/config.py` and enforced on the ingress claim. It is
counted per agent and **never reads the conversation id**, so admission is
exactly what it was before this ADR.

What changes is what that same number now bounds. Before, 64 admitted
deliveries produced at most one concurrent sandbox for a hook; now they can
produce up to 64. The window is an admission counter, not a concurrency
semaphore, so sustained fan-out is not bounded by any fixed ceiling at all — it
is bounded by admission rate multiplied by turn duration. The real backstop is
the substrate `ResourceQuota`, whose rejection is terminal without retry, which
means an over-fanned sweep fails loudly and terminally rather than queueing up
behind itself.

**No platform-side fan-out cap is added, and neither setting changes.** The
maintainer rejected both on 2026-08-27: a platform cap would refuse work for a
reason the sweep author cannot see or reason about, and lowering the admission
limit would penalise every unpartitioned hook for a capability it never used.
The sweep owns its own bound.

## A partition value must be a stable identity

A partition **is** a thread. It owns a transcript, an affinity route, a lock,
and a claim, with exactly the lifetime and retention a Slack thread ts has
today. So the value a pointer resolves to must be a stable identity of the thing
the delivery is about — a pull request number, a ticket key, a Slack thread ts —
and never a run id, a delivery id, or a timestamp. A value that changes per
delivery makes every transcript single-use and grows the state store without
bound while delivering none of the continuity the split is for.

This is a documentation and review control, not a runtime one. The bound cannot
tell `1481` from `1756425600`, and a heuristic that tried would refuse
legitimate numeric identifiers. It is stated in the column's own comment, in the
config schema, and here.

No new retention story is invented for partitions, and none is needed: none
exists for Slack threads either, and a partition is the same kind of object.

## Consequences

An agent-wide kill now signals N runners for a hook that used to have one.
Correctness is unchanged — the fan-out kill already iterated a set of thread
keys — but the cost is proportional to the fan-out. A saved dashboard filtered
on a single `curietech.ai/thread-hash` under-reports for the same reason: one
hook now emits a hash per partition (FU-3).

There is no fan-out reset verb, and the single-partition path is not as direct
as a receipt makes it look. The receipt and the ingress log name the minted
conversation id, but the reset verb takes the worker's composed thread key, so
an operator resetting one partition must build
`<kind>:<address>:<url-encoded conversation id>` themselves. FU-1 is therefore
not merely "a verb that resets every partition": it is a reset surface that
accepts a hook's conversation id — or a hook name, fanning out over every live
partition of it — and composes the thread key on the operator's behalf.

Nothing in the platform bounds or displays how many partitions a hook has
created. That is an **accepted risk**, not an oversight: the maintainer declined
a platform-side cap on 2026-08-27 for the reasons recorded under the quota
above, and no gate is added here. The gap that remains is observability rather
than control — an operator cannot currently answer "how many live partitions
does this hook have," which is the number they would need to notice a pointer
aimed at a delivery id before the retained per-partition state grows. An
operator-observable count of live partitions per hook is **FU-5**.

The transcript splits per partition, which is the intended benefit and the
reason for the stability requirement above.

A future channel adapter that threads its replies on
`ReplyTarget.conversation_id` would fan replies per partition. Slack does not,
so nothing is broken today, but that adapter's author needs to decide it rather
than discover it.

A delivery retried after the operator edits `hook_partitions` derives a
different partition than the original request did. That does not matter to
enqueue: the owner-checked enqueue script turns a pending lease into a
permanent stream-id receipt and permits exactly one `XADD` per delivery id, so
one delivery id enqueues at most once, on whichever partition the winning
request derived — a late former owner either re-claims before any successor or
observes the successor and does not `XADD`. A retry carrying a different
partition value, or arriving after the operator changed the pointer, is a
duplicate that mints nothing. Because the receipt cannot be re-derived from the
retry's own body, `HookAccepted.conversation_id` on a duplicate is read back
from the queued turn instead, and is null whenever the landing thread is not
knowable from that request — a pending twin still mid-flight, a stream an
operator has trimmed, or the 202 case.

A partition derived from a non-ASCII identifier refuses rather than being
transliterated, and the 422 names the pointer so the operator can see why.

Configuring a pointer enables sender-driven partition cardinality for that hook.
A holder of the derived hook secret chooses the values, and therefore how many
partitions exist; what the operator holds is the decision to enable that at all,
on this hook, through a field the HMAC covers. The header alternative would have
given the sender the same power without the operator enabling anything.

Follow-ups: **FU-1** a reset surface that takes a hook's conversation id (or
fans out over a hook's partitions) and composes the thread key itself; **FU-2**
moving the declaration into a bundle manifest under ADR-0099; **FU-3**
dashboards keyed on `curietech.ai/thread-hash`; **FU-4**
`docs/interfaces/triggers/INTERFACE.md` still states the per-agent webhook
ingress runtime is not built, which has been false since the ingress landed;
**FU-5** an operator-observable count of live partitions per hook.

## Alternatives considered

**An `X-Curie-Partition` request header.** Rejected by the maintainer on
2026-08-27. It sits outside the HMAC's covered bytes and hands the upstream this
agent's sandbox cardinality directly, turning an authenticated sender into an
unauthenticated capacity dial.

**A bundle trigger declaration under Draft ADR-0099.** Rejected for now on
2026-08-27 as the right long-term home for the wrong moment: 0099 is Draft and
its runtime is unbuilt, so building the declaration there first would block a
working slice on an unstarted decision. Recorded as FU-2.

**A platform-side fan-out cap.** Rejected by the maintainer on 2026-08-27. A
ceiling the sweep author cannot see would refuse work for an invisible reason,
and the substrate `ResourceQuota` already provides a terminal backstop.

**Silent fallback to the unpartitioned id on a misconfigured pointer.** Rejected
in design. It would collapse N threads into one while returning 2xx, which is
the ambiguous-signal failure the loud refusal exists to prevent.

## Realizing code path

`apps/api/src/curie_api/hook_partition.py` derives the partition and mints the
id, migration `apps/api/alembic/versions/0036_agents_hook_partitions.py` adds
the operator-owned column (revision `0036` skips `0035`, already claimed by
another in-flight migration on this release train), and
`apps/worker/tests/kernel/test_hook_partition.py` is the durable regression
guard for fan-out and intra-partition serialization.

`0036` currently revises `0034` because `0035` is claimed by that in-flight
migration, so whichever of the two merges **second** must re-parent its own
migration onto the other's head before merging. A merged migration's
`down_revision` is never rewritten afterwards: a database already stamped at
`0036` would not retroactively run a `0035` inserted beneath it, and the static
one-head check cannot see that hole.

This ADR is **Draft** and authorizes nothing by itself. Under
[ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md) as amended
by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md),
acceptance is a maintainer act: either this ADR is published Accepted, or the
coordinated exception is recorded with explicit maintainer approval naming the
realizing code path above.
