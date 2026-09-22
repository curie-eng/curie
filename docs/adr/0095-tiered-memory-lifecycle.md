# 95. One tiered memory lifecycle for agent and channel memory

Date: 2026-08-04

Status: Draft

**Folded into [ADR-0167](0167-two-memory-tiers-written-by-the-agent-capped-by-the-platform-and-policed-by-the-bundle.md). Not proposed for acceptance on its own.** ADR-0167
carries forward the two tiers, the hard cap enforced at the API, and the trust
posture, and decides them there. It drops the backlog bootstrap and the
operator instructions layer, leaves the escape valve with
[ADR-0100](0100-agents-search-their-own-surface-through-the-channel-port.md),
and sequences compaction behind the cap. When ADR-0167 is Accepted, this line
becomes `Status: Superseded by ADR-0167` and the body below stays as the record
of the earlier thinking.

Partially superseded by [ADR-0111](0111-the-default-memory-compaction-algorithm.md)
for the incremental compaction mechanism only. This ADR remains current for
durable storage and session injection.

Supersedes [ADR-0025](0025-memory-port-and-first-loader.md) (the per-agent
memory port and first loader) **upon acceptance**: agent memory becomes one
tier of the lifecycle defined here rather than a separate system, and
ADR-0025's status flips to Superseded with a back link at that point, per
ADR-0045/0085. Composes with [ADR-0029](0029-conversation-history-port-and-first-loader.md)
(per-thread transcripts), which remains its own port: the thread tier of the
context hierarchy is conversation replay, not managed memory. Designed against
[ADR-0020](0020-message-port-rendering-free-channel-interface.md) (the
rendering-free channel port) and [ADR-0012](0012-substrate-and-channel-agnostic-core.md)
(the runner never learns its channel), and sequenced behind
[ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md) (inbound triggers as a
new event kind), whose `source` field and placeholder-less output path this
lifecycle builds on. Distinct from [ADR-0030](0030-proactive-within-episode-memory-agent.md)
(within-episode memory), which addresses behavioral decay inside one run; this
ADR addresses context continuity across runs.

## Narrowing recorded 2026 08 17

The 2026 08 17 architecture review narrowed this ADR to durable storage and
session injection. [ADR-0111](0111-the-default-memory-compaction-algorithm.md)
supersedes the incremental compaction mechanism described below. The storage
and injection decisions in this ADR remain current.

## Context

Threaded work is the product's unit of execution: every mention boots a fresh
session (ADR-0003, stateless-first). The context a session should carry forms
a hierarchy of scopes, narrowest to broadest:

- **thread context**: this conversation (the transcript, ADR-0029);
- **channel context**: what the place knows, running projects, conventions,
  decisions, people;
- **agent context**: lessons the agent carries across every channel it works
  in;

with the bundle `systemPrompt` above all of them as the authored identity.
Today the hierarchy is incomplete and inconsistent. The thread tier works
(transcript replay). The agent tier is partially built: ADR-0025 landed the
`MemoryStore` port, the state-store loader, the boot preamble, and the
explicit `remember()` write path (#264, with the inspection UI of #267), but
the automatic extraction it deferred never landed and its consolidation
pipeline (`runner/src/curie_runner/memory.py`) has no trigger, so in practice
agent memory is a write-rarely append log. The channel tier does not exist at
all: nothing carries what the channel knows into a new thread, so a thread
either lacks that context or would have to re-read the channel backlog per
turn, which does not scale in tokens or latency.

These are not two problems. Channel memory and agent memory are the same
concept at different scopes, and building the channel tier as a second,
separate system would double the record shapes, the update stories, and the
namespaces for one idea. The fix used by every comparable product is a
per-scope memory document with one lifecycle: consume a backlog once where
one exists, keep a condensed durable note, inject it into every session,
update it continuously, and compact it periodically.

Three facts about the current system shape the design:

1. **The seams already exist.** Runner boot composes preambles in one place
   (`runner/src/curie_runner/__main__.py`, `_compose_system_prompt`). The
   worker mints memory and history refs in one place (`binding.py`,
   `boot_env`). Storage is the durable state store (`WorkflowStateEntry`:
   agent-scoped, namespaced, size-capped, CAS-versioned) with reserved
   namespaces `memory` and `transcript`.

2. **"Per channel" and "per agent" are the same granularity today.** An agent
   binds exactly one Slack channel (`agents.slack_channel`, 1:1; multi-channel
   is deliberately deferred to epic #27). The tiers must nevertheless be
   distinct now, because they diverge the moment an agent has two channels:
   channel memory stays behind the place's access boundary, agent memory
   travels with the agent. The limits the 1:1 binding still imposes are named
   in Consequences rather than papered over.

3. **There is no custom-instructions concept and no batch-job infrastructure.**
   The only model-facing instruction surface is the bundle `systemPrompt`,
   versioned and immutable per deployment, not operator-editable at runtime.
   The only periodic-work pattern is the API's in-process asyncio loops (the
   expiry sweeper's wait-first template); there is no CronJob anywhere.

External evidence (surveyed 2026-08-04) converges hard on a few points, and
the closest comparable product, Claude in Slack, is a direct precedent:

- **Memory is a curated note, not a transcript.** Every production system that
  ships to non-developers stores short, stable, human-readable prose and
  rejects transcript accumulation; the industry has visibly retreated from
  vector stores for this job (Letta's MemFS ships markdown with no vector
  index by default).
- **Two tiers with a hard budget.** A small always-injected core plus
  on-demand detail, with the injected tier's size enforced by the harness on
  every write (Claude Code: measure on write, warn near the limit, error over
  it), not left to model judgment. Context-rot measurements justify the cap.
- **Hybrid update triggers.** In-turn writes for explicit "remember this"
  (users expect immediacy), background consolidation for everything else
  (better recall, no hot-path latency). OpenAI and Letta independently shipped
  idle-time consolidation and both named it "dreaming".
- **Scope follows the access boundary.** "Memory follows places the same way
  access does": channel memory is scoped to the channel, never attached to an
  individual. Claude in Slack keeps memory by channel, isolates private
  channels, and orphans a private channel's memory if it goes public
  (fail-closed over continuity).
- **Standing instructions outrank learned memory.** Claude in Slack layers
  operator-authored custom instructions above channel memory explicitly,
  because learned memory is the lowest-trust layer: nobody deliberately wrote
  it.
- **Memory poisoning is a studied attack surface.** The write channels this
  feature opens (agent-judged saves, backlog consumption, compaction) are
  three of the four channels in the published poisoning taxonomy
  (arXiv 2606.04329), on a surface where any channel member is an author.

## Decision

**Memory is one tiered system: a single lifecycle (bootstrap where a backlog
exists, injection, hybrid update with scheduled compaction, and a bounded
escape valve into raw history the platform does not store) applied per
scope, with an agent tier and a channel tier over the existing `memory`
namespace of the durable state store.** The thread tier of the context
hierarchy remains the transcript port (ADR-0029), replayed rather than
curated; the bundle `systemPrompt` remains the authored layer above
everything. Slack is the first surface implementing the channel tier; nothing
below names Slack except its adapter section.

### The surface obligation (the interface)

A surface adapter owes the platform one identity and, optionally, two
capabilities, and nothing else:

1. **A scope key.** Every inbound turn carries a stable, opaque
   `scope_key` identifying the place the conversation happens in (Slack: the
   channel id, already present as `reply_handle.channel`). The platform treats
   it as an opaque string, per ADR-0012. Each future surface defines its own
   mapping (email: likely the mailbox or recurring thread; decided in that
   surface's adapter ADR, not here).
2. **Optionally, a bootstrap trigger.** A surface that has a readable backlog
   may emit a one-time instigating event carrying a bounded backlog payload
   the adapter fetched. A surface with no backlog (email) emits nothing; its
   memory starts empty and accretes through the update path. Bootstrap is an
   optimization, not a requirement of the interface.
3. **Optionally, a history read capability.** A surface that retains its own
   raw history may serve two channel-neutral operations behind the platform:
   **dereference** (read the message or thread a provenance pointer names)
   and, where the surface supports it, **scoped search** (a keyword query
   with capped results). Both are served platform-side, so the runner never
   holds surface credentials (ADR-0012); a surface without the capability
   degrades to document-only memory. This is the escape valve of the read
   path, defined below. The capability itself, its verbs, bounds, and
   enablement model, is specified by ADR-0100 (agents search their own
   surface through the channel port); this ADR only requires that memory
   turns can call it where enabled.

The agent tier needs none of the optional capabilities: its scope is the
agent itself, it has no backlog, and its raw history is the platform's own
transcripts.

### Storage

- **Everything lives under the existing reserved `memory` namespace.** No new
  namespace: tiers are key prefixes, mirroring how `transcript/<thread_key>`
  scopes threads. Per tier and scope the keys are:
  - `agent/document`, `agent/notes`, `agent/watermark` for the agent tier;
  - `channel/<scope_key>/document`, `.../notes`, `.../instructions`,
    `.../watermark` for the channel tier.
- The key roles, identical across tiers:
  - the **document**: one curated markdown profile, the always-injected core.
    It is rewritten in place only by the bootstrap and compaction turns,
    always under compare-and-set. Claims in the document keep their surface
    references inline, so the document doubles as an index into the raw
    history it was distilled from;
  - the **notes log**: an append log of provenance-stamped records
    (ADR-0025's `MemoryRecord` shape, which this ADR carries forward,
    extended with an optional **surface reference**: a permalink or message
    timestamp, pointer metadata and never copied content). In-turn saves
    append here; notes are injected after the document until compaction
    folds them in;
  - the **instructions** (channel tier only): an operator-authored plain-text
    note, editable at runtime through the API/console (extending the memory
    inspection UI of #267), never written by the agent. **Writable with the
    platform key only**: the state router refuses writes to this key from
    `state`-scoped and app tokens, so no sandbox credential can author it.
    This restriction is enforced API-side and mirrored in the runner's
    reserved-namespace table;
  - the **watermark**: the last-compacted position, its own machine-read key,
    never embedded in the prose document.
- **Migration from ADR-0025 is a rename, not a rewrite.** The existing
  agent-memory log key becomes `agent/notes` with an empty `agent/document`;
  the first compaction pass folds the accumulated records into the document.
  No record shape changes and no provenance is lost.
- Storage remains agent-scoped (`WorkflowStateEntry` has no channel table),
  so the scope key buys forward compatibility for the artifacts, not
  channel-portability across agents; see Consequences for the rebinding and
  multi-channel limits.
- The state store's per-value and per-namespace caps and scoped-token auth
  (ADR-0033) apply unchanged. **Concurrency is explicit, not inherited**: CAS
  in the state store is opt-in, so this ADR requires it. Document rewrites
  carry `expected_version` and, on conflict, re-read and re-merge (or
  re-enqueue the compaction turn). In-turn saves only append to the notes
  log, never rewrite the document, so an explicit user save can never be
  discarded by a concurrent compaction; compaction clears only the notes it
  consumed, and a note landing mid-compaction survives to the next pass.
- The `MemoryStore` port generalizes from ADR-0025's two methods to a
  tier-scoped load/append/replace; the dormant consolidation helpers
  (`consolidate_records`, `SupportsReplace.replace`) become the compaction
  turn's write-back shape.

### Injection

- The worker mints one `CURIE_MEMORY_REF` (the namespace root, as today) in
  `binding.boot_env`, and `boot_env` gains the scope key so the runner can
  resolve both tiers (the kernel already holds it as `reply_handle.channel`;
  today only `thread_key` is threaded through, so the signature widens by one
  opaque string).
- At runner boot the ref resolves as today (URL-shaped, scoped token,
  transient failure degrades to "no memory" without blocking boot), and
  `_compose_system_prompt` composes the hierarchy, one labeled block per
  tier: **channel instructions, channel memory (document then unfolded
  notes), agent memory (document then unfolded notes), conversation
  preamble, bundle `systemPrompt`**. The authority ladder is stated in the
  rendered headers: the bundle prompt and custom instructions are authored
  guidance and outrank learned memory at every tier; memory is learned
  context, not instruction. The channel block's rendered header also names
  the dereference tool and when to reach for it, so the model knows the
  document is a distillation with an escape valve, not the whole record.
  Instructions and memory are read once at session boot; a running thread
  keeps the set it booted with.
- **The injected tier has a hard budget, enforced at the API state router on
  every write.** Each tier's document and notes log are measured against a
  fixed per-tier cap (Claude Code's 200-line / 25KB class of limit; exact
  numbers are implementation). Near the cap, the write succeeds and the
  response carries a warning instructing consolidation; over the cap, the
  write is refused with an error instructing a rewrite. Enforcement lives in
  the API, not the runner, so console edits and any future writer pass
  through the same gate; the cap is a platform decision, not a model
  judgment, because injection bloat degrades every session at once.
- **Sibling paths are disposed of, not discovered**: eval runs stay hermetic
  (ADR-0051's fresh-conversation default), so an eval session gets no memory
  ref, or a fresh throwaway scope when a case explicitly exercises memory.
  The `curie local` CLI stub channel mints a stub scope key and accretes
  channel memory like any scope, which is desirable for testing and confined
  to the local store. The fake tier remains plumbing-only and injects
  nothing.

### The memory turns and their kernel obligations

Bootstrap and compaction run as turns through the existing stream/kernel/
runner path, so they inherit routing, model selection, observability, and
cost attribution. They ride ADR-0079's queued-event extension (`source`
field, optional `placeholder_ts`) and add a second axis: a turn **kind**
(`interactive` today, plus `memory_bootstrap` and `memory_compaction`),
carried in the shared contract in `packages/`, never dispatcher-owned. That
is deliberately more than ADR-0079 grants, and the delta is named here as
sacred-kernel, single-owner work:

- **Output disposition: silent.** ADR-0079's placeholder-less path posts its
  reply to the channel; a memory turn must not. Its product is a state-store
  write, and the kernel discards its conversational output.
- **Transcript disposition: none.** Memory turns append nothing to any
  thread transcript (ADR-0029's write side is for conversational turns), and
  they carry a reserved conversation identity, not a user thread's
  `conversation_id`.
- **Ordering: locked per scope.** A memory turn takes the same per-thread
  order lock family as interactive turns (keyed by its reserved identity) so
  two compaction turns for one scope cannot interleave, and a compaction
  turn waits behind live interactive work rather than competing with it
  (ADR-0079's jobs-wait-for-idle semantics).
- **Approvals: inapplicable by construction.** Memory turns run with no
  side-effecting tools beyond the state API; they must not be able to reach
  an approval gate. If a memory turn requests an approval, that is a bug,
  and the kernel fails the turn rather than parking it.

### Bootstrap (channel tier only)

- **Triggers, concretely.** The common case is control-plane, not Slack: an
  agent is bound to a channel when `agents.slack_channel` is set at
  create/update/deploy time, which emits no Slack event, so **the API emits
  the bootstrap trigger on binding**. The Slack-side event (the bot invited
  to a channel it is bound to) additionally requires the dispatcher to
  subscribe to `member_joined_channel`; the backlog fetch itself requires
  new read scopes (`channels:history`, `groups:history`, `pins:read`). The
  adapter fetches a bounded backlog (recent history plus pinned items) and
  enqueues the `memory_bootstrap` turn with the backlog as opaque text; the
  runner never calls a surface API (ADR-0012).
- **Idempotency lives in the turn, not the trigger.** The dispatcher has no
  state-store access and does not gain one; the bootstrap turn's first act
  is to check the scope's document and end silently if one exists (the same
  "channels that already have memory" suppression Claude in Slack
  documents). Duplicate triggers are therefore harmless.
- **The seed is conservative**: stable facts, named projects, conventions,
  and people, not a digest of the backlog. (Claude in Slack's join-time scan
  is documented as producing an intro post, not a memory write; the
  closest-comparable product chose caution here, and the poisoning
  taxonomy's compaction-channel attacks argue the same way.)

### Update (both tiers)

The lifecycle is a hybrid, matching the convergent industry pattern:

- **In-turn writes land immediately.** An explicit user ask ("remember for
  this channel: ..." or "remember this everywhere: ...") and agent-initiated
  saves during work append provenance-stamped notes to the addressed tier
  through the state API mid-session, durable and injected for the next
  session the moment they land. Every write passes the budget gate above.
  The channel tier is the default destination; the agent tier is for lessons
  that should travel across channels.
- **Supersession note.** [ADR-0111](0111-the-default-memory-compaction-algorithm.md)
  supersedes the incremental compaction mechanism described in the following
  historical paragraph.
- **Compaction is a scheduled background turn, not new infrastructure.** An
  API-side scheduler loop (the expiry sweeper's wait-first template,
  including its multi-replica single-writer guard so two API replicas cannot
  double-enqueue) walks scopes on an interval (nightly by default) and, for
  each scope, in either tier, with new transcript activity or unfolded notes
  since its watermark, enqueues a `memory_compaction` turn. The turn reads
  the scope's inputs since the watermark (transcripts and notes for the
  channel tier; notes for the agent tier), plus the current document,
  rewrites the document in place (fold notes in, merge duplicates, correct
  stale facts, resolve conflicts newest-wins, union provenance, drop what
  the work has outgrown), clears the consumed notes, and advances the
  watermark, all under CAS as specified in Storage. A scope with no new
  activity is skipped, so cost is bounded by real usage, not channel count.
  This is the trigger ADR-0025 deferred and never grew.

### The escape valve: compaction is not the only read path

Compaction is lossy by design, and the best available evidence (TierMem,
arXiv 2602.17913) puts numbers on both halves of that trade: a summary tier
answering alone loses roughly twelve points on needle-recall against reading
retained raw history, while its dominant failure mode is the reader *not
noticing* the summary is insufficient. An always-injected document with no
way to look deeper would carry that blind spot permanently. The lifecycle
therefore keeps the raw history reachable, without storing it:

- **Dereference is the default valve.** In a session, the agent may follow
  the surface references already pinned in the memory document and notes,
  reading the message or thread a pointer names through the surface's
  history read capability, under a per-session call budget. It cannot browse
  or query beyond the pins: the only raw content reachable by default is
  content a bootstrap or compaction pass already certified as load-bearing.
  This bounds cost (explicit dereferences of known targets), context
  pollution (no open-ended exploration mid-task), and the poisoning surface
  (arbitrary channel text cannot be pulled into working context by a
  planted instruction).
- **Scoped search is an operator opt-in, per agent, off by default**: a
  keyword query with capped result counts and the same per-session budget,
  for channels whose work genuinely needs recall beyond the pins.
  Open-ended exploration is not shipped at all.
- **The valve feeds the document.** A fact recovered through the valve that
  proves load-bearing is saved as a note like any other and folded in at the
  next compaction, so escalation gets rarer as the document matures; the
  broad reads stay where they belong, in the bootstrap and compaction turns,
  off the hot path.

The raw stores behind the valve are the surface's own retained history
(Slack keeps the channel log; the pointers are permalinks into it) and the
platform's own per-thread transcripts (ADR-0029). The platform never copies
the surface's raw history into its own store to make this work.

### Trust posture

- **Memory is context, never an enforcement boundary.** Nothing an agent must
  not do may rely on memory or custom instructions; approvals and policy
  gates ([ADR-0010](0010-approval-gates-and-human-in-the-loop.md),
  [ADR-0056](0056-operator-opt-in-for-policy-gate-grantability.md)) remain
  the enforcement surface, unaffected by memory content.
- Every memory write carries provenance back to the session and traces it was
  learned from; the documents are human-readable and operator-editable
  through the console (the #267 surface), so audit and correction are
  first-class.
- The poisoning exposure is named and accepted with mitigations, not ignored:
  the bootstrap and compaction prompts distill toward stable facts under
  scope-limited write criteria, provenance identifies the contaminating
  session when poisoning is suspected, and the instructions layer (which no
  sandbox credential can write, enforced at the state router as specified in
  Storage) outranks the memory tiers (which the agent can write). The agent
  tier deserves particular care in the compaction prompt: it is the tier a
  poisoned channel could try to escape into, so promotion of a fact from
  channel to agent scope must be conservative.
- **The retention posture is declared, not discovered.** What the platform
  stores: the curated documents, the notes, the operator instructions,
  pointer metadata (permalinks, timestamps), and its own per-thread
  transcripts (ADR-0029). What it never stores: copies of the surface's raw
  history (the bootstrap turn reads the backlog transiently and discards
  it once distilled) and no embedding or vector index over any of it. The
  surface remains the system of record for its own content, under its own
  retention and access policy; deployments hold the memory artifacts in
  their own database. This is the same posture Claude in Slack publishes
  (retention named explicitly, with the consequence that zero-data-retention
  configurations exclude the feature), and any compliance story quotes this
  paragraph rather than reverse-engineering the store.

### Validation gate (before maintainer acceptance)

Per ADR-0001, evidence before promotion. This Draft is accepted only with:
(1) an eval case proving a fact saved in thread A appears in a fresh thread
B's context in the same scope, and does not appear in another scope; (2) a
compaction case proving a named load-bearing fact survives the rewrite while
a planted stale fact is corrected; (3) unit tests on the budget gate at the
API enforcement point (warn near, refuse over) and on the CAS conflict path;
(4) one red-team case from the poisoning taxonomy (a backlog or thread
message that instructs the agent to write an instruction-shaped memory)
showing the write lands, is visible with provenance in the console, and
cannot reach the instructions key; (5) a migration check proving an existing
ADR-0025 agent-memory log is still injected, unmodified, after the rename to
`agent/notes` and before any compaction runs; (6) an escape-valve case
proving a fact absent from the document but named by a pinned surface
reference is recovered through the dereference operation, and that the
default valve refuses a read of any target not pinned in the document or
notes.

## Alternatives considered

- **Build channel memory as a second system beside ADR-0025's agent memory.**
  Rejected, and this ADR's supersession of ADR-0025 is the point: the two
  are one concept at different scopes, and two systems means two record
  shapes, two update stories, and namespace sprawl. The first draft of this
  ADR took the two-system shape (a new `channel-memory` namespace beside
  `memory`) and review moved it here.
- **Fold the thread tier under managed memory too.** Rejected: the
  transcript is replay of what was said, not a curated distillation, with
  its own lifecycle (append per turn, windowed at boot, ADR-0029) and no
  compaction semantics. It is the hierarchy's narrowest tier but not a
  memory store; the compaction turn consumes it as input instead.
- **Raw history as the memory: mirror the surface's logs into the platform
  and expose grep/read tools over them.** Rejected. It duplicates the
  surface's content into a second store, which is the largest possible
  retention footprint for a feature that needs the smallest (the surface
  already retains the raw log under the customer's own policy); it pays the
  raw path's tokens and latency on queries a curated document answers for
  hundreds of tokens; and the strongest study cited for it (TierMem) in
  fact recommends the opposite default, showing routed escalation over
  retained raw recovering nearly all of raw-only accuracy at half the cost,
  with its raw-vs-routed margin smaller than the benchmark's known
  answer-key error rate. The property the proposal is really after, that no
  fact is irrecoverably lost to compaction, is delivered by the escape
  valve without storing anything.
- **A retrieval layer (vector store or knowledge graph) instead of curated
  documents.** Rejected: the unit is one bounded document per scope, well
  within a context window; the surveyed industry trajectory is away from
  vector stores for this job; and the state store gives durability, scoping,
  caps, and audit for free. An embedding index is also a compliance step
  backward, a second derived copy of surface content with no clean deletion
  propagation (purging a message from the surface does not purge its
  vectors), for retrieval quality the recent literature shows keyword
  search over raw text matching or beating on this workload. A retrieval
  tier can be added later without changing this decision.
- **An unrestricted search tool by default.** Rejected: open-ended agentic
  exploration of a channel mid-task is a measured failure mode twice over,
  context pollution (irrelevant history crowding the working context) and
  the C2 poisoning channel (planted text pulled into context by the agent's
  own search). The valve ladder (dereference by default, scoped search as
  an option enabled per ADR-0100, open exploration never) keeps the
  accuracy benefit while bounding both.
- **Real-time-only updates (no compaction job).** Rejected: accretion without
  consolidation produces the documented staleness and bloat failure modes,
  and the budget gate alone would then force the model to consolidate during
  user-facing turns, moving maintenance cost onto the hot path.
- **Batch-only updates (no in-turn writes).** Rejected: users expect
  "remember this" to take effect immediately; every surveyed system honors
  explicit asks in-turn.
- **A full-digest bootstrap.** Rejected in favor of the conservative seed:
  the digest inflates the injected tier on day one, and backlog text is the
  least-trusted input in the poisoning taxonomy.
- **Dedicated compaction infrastructure (a CronJob, a job queue).** Rejected:
  compaction is LLM work and belongs in the runner path where routing,
  observability, and cost attribution already live; the only new piece is a
  small enqueue-only scheduler loop following an existing in-process
  template. This repeats #29's "no new execution machinery" principle.
- **Custom instructions inside the bundle `systemPrompt`.** Rejected: the
  bundle prompt is versioned with the deployment and owner-authored;
  operators and channel members need a runtime-editable, per-scope layer
  without a redeploy. The two compose; they are not the same knob.
- **A single memory artifact with in-place writes from the hot path.**
  Rejected: concurrent thread sessions and the compaction turn would race on
  one document, and a blind last-write-wins could silently discard an
  explicit user save. The document-plus-notes-log split keeps the hot path
  append-only and gives the document a single writer class.

## Consequences

- **ADR-0025 is superseded upon acceptance.** Its state-store namespace, its
  `MemoryRecord` provenance shape, its boot-preamble delivery, and its
  boot-resilience rules all carry forward into this lifecycle; what changes
  is the storage layout (document plus notes per tier instead of one log),
  the generalized port, and the arrival of the update lifecycle it deferred.
  The migration is the key rename in Storage plus the first compaction pass.
  ADR-0029 is untouched.
- **Sequencing.** ADR-0079 is Accepted but its contract and kernel side are
  unimplemented (`QueuedTurn` today carries no `source`, and the placeholder
  is required). This ADR sequences behind that work and adds to it: the turn
  `kind` axis, the silent output path, the no-transcript rule, and the
  reserved conversation identity are kernel changes under the sacred-kernel
  single-owner review rule. Per the contract rules in `packages/`, a new
  kind enum is a breaking change (minor under 0.x); the optional fields are
  patches.
- `binding.boot_env` widens to carry the scope key. `RESERVED_NAMESPACES` is
  unchanged (the `memory` namespace already exists), but the
  platform-key-only rule on the instructions key is a new, key-granular
  restriction the state router does not have today. The runner's memory
  resolver grows tier awareness; the composition seam gains one block per
  tier.
- The Slack adapter gains a `member_joined_channel` subscription, three read
  scopes, and a backlog fetch path: a deliberate, confined widening of its
  "ack, dedupe, placeholder, enqueue" discipline. The new OAuth scopes are a
  reinstall/consent event for existing workspaces. The dereference valve
  rides those same read scopes (it reads channels the bot is already a
  member of); scoped search is gated behind ADR-0100's enablement option.
  The history read capability is ADR-0100's platform-served tool surface,
  and surface credentials stay out of the sandbox in line with the
  platform's credential boundary.
- Cost is one bootstrap turn per channel ever, plus one compaction turn per
  active scope per interval (agent tiers included), skipped for idle scopes.
  Both are attributable per agent through existing observability.
- **Rebinding orphans channel memory, by choice.** Channel memory is stored
  under the agent that learned it; rebinding that agent to a different
  channel strands the old scope's artifacts (readable to operators, injected
  nowhere), and the new channel bootstraps fresh. Agent-tier memory, by
  contrast, follows the agent through a rebinding: that is the difference
  between the tiers doing its job. A platform-level memory move is future
  work, as is any handling of a Slack channel's own privacy transitions.
- **Multi-channel (#27) has a named prerequisite here.** The tiered key
  layout needs no migration (channel keys already carry the scope key), but
  transcript keys carry only the thread ts, so "this scope's transcripts
  since the watermark" is computable only while the 1:1 binding makes agent
  and scope coincide. Attributing transcripts to a scope key is #27 work.
- Custom instructions become a product concept for the first time. The v1
  editing surface is the state API and console; richer scoping (workspace or
  org tiers above the channel, as Claude in Slack layers them) is future
  work and slots into the hierarchy as additional tiers without changing
  the storage or injection shape.
- The email surface (and any backlog-less surface) is served by the same
  lifecycle minus bootstrap: memory starts empty and accretes through
  in-turn writes and compaction. Its scope-key mapping is deliberately
  undecided here.
- Known limits, stated rather than discovered later: conflict resolution in
  a prose document is model rewrite plus human edit, with no algorithmic
  contradiction detector; forgetting happens only at compaction and the
  budget gate, with no time-based decay; per-thread transcripts remain the
  compaction job's input and stay subject to their own growth limitation
  (ADR-0029). ADR-0030's within-episode layer, if accepted, composes with
  this one and neither replaces the other.
