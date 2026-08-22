# 115. Agents call each other directly, with no third party in the path

Date: 2026-08-20

Status: Draft

## Context

Curie has no way for one agent to ask another agent to do something. Pillar 4
in [`docs/pillars.md`](../pillars.md) already says agents communicate "with
people and with other agents", and nothing implements the second half.

This is not a missing feature so much as a missing decision. Turn-minting
machinery landed recently, which makes it tempting to describe the gap as almost
closed. It is not: **an agent cannot reach any of it.** The gap is total, and the
recent work changes only what a solution can be built out of.

### What exists today, and why none of it is reachable

**The blocking fact first.** A sandbox is handed exactly one API credential:
`CURIE_STATE_TOKEN`, a `state.app`-scoped ADR-0033 token minted per claim
(`apps/worker/src/curie_worker/binding.py`). The API accepts a scoped token on
the state router and nowhere else -- "a scoped token is rejected everywhere else"
(`apps/api/CLAUDE.md`) -- and the platform key is deliberately never placed in
the sandbox. So an agent holds no credential that any turn-minting route will
accept. There is no partial path, no lossy path, and nothing to widen by a
configuration change. Everything below is about which surface a *new* credential
should authorize, not about a surface an agent can use today.

Since [#269](https://github.com/curie-eng/curie/issues/269) closed,
[ADR-0079](0079-inbound-triggers-as-a-new-event-kind.md) is implemented. A turn
can start from something other than a person: `QueuedTurn.source` distinguishes
a person's message from a job, `ReplyHandle.placeholder` is nullable, and
`POST /hooks/{agent_id}/{hook}` (`apps/api/src/curie_api/routers/hooks.py`)
turns a signed external event into a turn on the same stream a mention feeds.
[#1459](https://github.com/curie-eng/curie/issues/1459) then landed the channel
port from [ADR-0096](0096-port-adapters-are-deployed-services.md), so
`POST /channels/turns` mints a turn for a binding named in the body, holding a
`chn` token scoped to one binding row rather than the platform key.

So two ingress paths mint turns today, and both are gated on the platform key:
`POST /channels/token` mints a `chn` token under `require_api_key`, and a hook
signature derives from `settings.api_key`. Handing either credential to a sandbox
is not a shortcut to this feature, because each surface also has properties that
make it the wrong thing to open up -- properties that are deliberate, and load
bearing where they sit:

- **The hook ingress cannot carry a caller, by construction.** `_mint_turn` sets
  `author=f"hook:{hook}"`, and the code says why: an upstream-supplied identity
  there "would let a hook impersonate one to anything downstream that reads the
  field". The reply route comes wholly from the target's binding row, so the
  answer goes to the target's channel and a caller never sees it. And
  `_conversation_id` is per hook, so two calls would share one thread and the
  second would defer behind the first. A caller, a private answer, and
  concurrency are the three things a call needs, and this surface refuses all
  three on purpose.
- **The channel ingress is for adapters, not agents.** A `chn` token is scoped
  to a binding row so an ingress adapter can enqueue for that binding. It says
  nothing about who asked, and the same reply-routing property applies: the
  platform mints the route from the binding, never from the request.
- **Slack is closed on purpose.** `is_user_message` in
  `apps/dispatcher/src/curie_dispatcher/handlers.py` drops any event carrying a
  `bot_id`. An agent posting into a channel cannot wake another agent. Without
  that filter the dispatcher's own placeholder would re-trigger it, so the filter
  is load-bearing and not an oversight to remove.

Two other things exist and are worth naming because they are often mistaken for
this feature. Subagents inside a single turn already work: the bundle manifest
carries an `agents` field (`packages/plugin-format/src/plugin_format/models.py`)
that the harness picks up. That is one agent identity, one pod, one credential
set, one approval policy -- decomposition, not two agents talking. And the
`curie-state` MCP server ([ADR-0073](0073-agentos-state-mcp-server-and-state-boot-env.md))
is namespaced per agent with a token bound to that agent, so two agents cannot
even use it as a shared blackboard.

### Two decisions already made that this one has to fit

ADR-0112 renames the person-message value of `source` from `slack` to `message`.
It is a Draft and is not on `main` yet, so it is cited here by number rather than
by relative link ([PR #1672](https://github.com/curie-eng/curie/pull/1672)). Its
rejection of `human` names this case explicitly: "ADR-0079's own agent-to-agent
case, and the Twin agent pattern, both produce turns caused by an agent speaking as a
participant rather than by a job firing. Those are messages and are not human."
That settles the enum question here before it is asked. An agent asking another
agent something is a message, not a job, and needs no fourth `source` value.

[ADR-0099](0099-hooks-are-bundle-declared-turns-the-system-starts.md) (Draft)
covers the other half of "a turn no user started": bundle-declared background
work on a schedule. It is easy to assume delegation belongs there. It does not.
A hook fire is a job with no caller and no audience; a delegated call has both.
This ADR is therefore not sequenced behind ADR-0099, and the two should not be
merged into one mechanism.

### Two consumers are already waiting

[ADR-0044](0044-workflow-controlled-agents-callable-substrate.md) (Accepted)
committed to shape C as its phase-1 build: a developer's LangGraph or CrewAI
graph stays the outer loop and calls into Curie at the steps that need
open-ended AI, folding the result back into graph state. That ADR assumes the
developer can "call the existing Curie API to run a sandboxed, approval-gated
turn". No such call exists. The hook and channel ingresses mint a turn and
return a receipt; neither returns a result. Discussion
[#1372](https://github.com/curie-eng/curie/discussions/1372) (LangChain support)
wants the same primitive.

So the missing piece is one primitive with two callers: an external workflow, and
another agent. The authorization question is much sharper in the second case,
which is why this ADR is scoped to it and the external caller inherits the answer.

### The question this ADR has to answer

Discussion [#1049](https://github.com/curie-eng/curie/discussions/1049) (agent
identity, invocation authorization, and caller/owner attribution) is the
prerequisite conversation, and it frames the hard part. Its requirement 3 is that
every action be traceable to an accountable person -- the caller when a human
asked, the registered owner when a schedule fired -- and "never to just 'the
agent' with no accountable person behind it". Its requirement 1 is that not
everyone may invoke every agent. It explicitly lists delegated, on-behalf-of
impersonation as a non-goal.

A call between two agents is where those requirements bite hardest, because a
naive implementation quietly does three things nobody decided:

1. **It launders authority.** If B's turn runs with no record of who asked, then
   a caller with no permission to do something can ask an agent that has it. The
   approval machinery ([ADR-0010](0010-approval-gates-and-human-in-the-loop.md),
   [ADR-0034](0034-approval-authorizers-resolve-membership-in-the-api.md),
   [ADR-0035](0035-one-shot-post-approval-allowance.md)) resolves approvers
   server-side precisely so authority cannot be asserted from inside a sandbox.
   A delegation path that drops the caller reopens that from the other end.
2. **It loses accountability.** #1049's requirement 3 fails at the first hop
   unless the accountable principal is carried, not recomputed.
3. **It can loop.** ADR-0099 already names the pathology for background work: a
   refusal becoming a recursive spawn trigger, and no-progress loops discovered
   from the bill. A calling B calling A makes that a cycle rather than a single
   runaway, and every hop spends real tokens.

## Decision

**One agent may ask another agent to do something, over a surface the platform
owns end to end, with no third party in the path.** No interop protocol to
adopt, no external broker, no orchestration framework, no bundle-shipped MCP
server, no relay holding the platform key, and no round trip through a human
channel.

That is the capability. The rest of this decision is how it is made safe, and the
constraint that governs all of it: **a call must never widen what the target may
do, and must never lose who asked.** Everything below follows from those two
sentences.

Six parts.

### 1. A first-party scoped surface, reached through an auto-mounted MCP server

The call goes to a new first-party API route, authenticated by a scoped token
minted per turn, and reaches bundle code as an auto-mounted MCP server
(`curie-delegate`) rather than something a bundle ships.

This is the pattern ADR-0073 established for durable state and
[ADR-0107](0107-an-agent-reads-its-own-runs-through-a-first-party-scoped-surface.md)
follows for an agent reading its own runs: a narrow scope on the
[ADR-0033](0033-scoped-sandbox-state-token.md) token, a first-party surface, and
an in-process server the runner wires into every real session. Reusing it here
means no new datastore, no new credential shape, and no per-bundle
reimplementation of the auth handling.

The three rejected mechanisms -- the hook ingress, the channel ingress, and Slack
-- are covered under Alternatives.

**First-party is the point, and it is worth stating plainly.** Nothing in the
path is a third party: no agent-interop protocol to adopt, no external message
broker to run, no orchestration framework in the middle, no MCP server a bundle
has to ship, and no round trip through a human channel. The call reuses machinery
the platform already owns -- the `curie:runs` stream, the resume path, the
ADR-0033 token, and the runner's existing MCP mounting -- so a second agent is
reachable with no new infrastructure and no new dependency to trust. That is also
a constraint rather than only a selling point: the moment this decision requires
a third party, it is the wrong decision and should be revisited rather than
extended.

### 2. A call rides the message lane, not the job lane

Mechanical, and recorded because an implementer would otherwise have to derive
it. `QueuedTurn.source` answers what caused a turn, and `is_job` collapses it to
one predicate -- "not a person's message" -- which drives exactly one behavior:
whether the turn may **steer a live session**. A person speaking mid-run may
redirect the agent. A job (`cron`, `webhook`) is an output and must not hijack a
run in progress, so it waits for idle.

A call belongs in the message lane, for one reason that is not about steering:
the job lane's only ingress is the hook path, which pins `author` to the platform
and so discards the caller by construction. Losing the caller is the one thing
this decision cannot accept, so the lane follows from that.

The minted turn therefore carries `source=message` per ADR-0112, and needs no new
`source` value. The target's turn is otherwise ordinary: its own conversation,
model resolution, tools, and approval policy. Only the reply route differs, below.

The cost of this choice is explicit: because a delegated turn is a `message`,
`is_job` is False, and the existing steering rule would let a caller interrupt a
target mid-run. That is wrong, and it is a kernel change rather than something
this lane choice fixes on its own -- see part 4 and Consequences.

### 3. The caller suspends and resumes; it never blocks

The caller does not wait inline. The delegate tool returns a correlation handle
and the caller's turn ends; when the target's turn settles, the result is
delivered back as a turn that resumes the caller's conversation.

This reuses the approval suspend/resume path rather than inventing a second one.
`apps/api/src/curie_api/resumequeue.py` already appends a normal `QueuedTurn`
onto `curie:runs` so a resume "walks the identical consumer -> kernel -> claim
path a Slack mention takes", with the kernel rehydrating the suspended thread per
[ADR-0003](0003-stateless-first-rehydrate-on-resume.md) and a deterministic
`event_id` deduping a double-enqueue.

Blocking is not a viable alternative. The target's turn may suspend for hours on
an approval gate, and a caller holding a sandbox open across that would burn the
resource envelope [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md)
bounds, for a wait whose length is set by when a human clicks a button.

The return route is expressed as a `ReplyHandle` with a new first-party `kind`
(`delegation`) whose address is the caller's conversation, delivered by a
first-party in-cluster sink that re-enqueues the reply as the caller's next turn.
`kind` is already an open vocabulary under ADR-0096, and the worker already
selects a sink by kind, so this is a new value rather than a new mechanism.
Per ADR-0096's rule that a reply endpoint never receives any credential other
than its own, this sink is first-party and carries its own.

### 4. Authority never widens, and the accountable principal is carried

The target runs with **its own** credentials, model resolution, egress
allowlist, and approval policy. Never the caller's, and never a union of the two.
A call is a request to an agent, not a grant of the caller's authority to it.

The turn carries two attribution fields, not one:

- the **immediate caller** -- the calling agent's structured identity;
- the **accountable principal** -- the human the chain traces back to, propagated
  unchanged from the root turn through every hop.

Two fields rather than one because #1049's requirement 3 cannot be satisfied by
either alone: the immediate caller is what a cycle check and an authorization
check need, and the accountable principal is what an audit trail needs. Deriving
one from the other at read time means walking a chain that may no longer exist.

Three things follow, stated because each is a place an implementer would
otherwise have to guess:

- **A delegation never satisfies an approval gate.** If the target's turn reaches
  a gated tool it posts its card and suspends exactly as a user-started turn
  does. The caller is not an approver, and the accountable principal being
  carried is not consent -- it is attribution. ADR-0035's one-shot allowance is
  minted by an approval resolution and by nothing else.
- **This is not on-behalf-of impersonation.** The target does not act as the
  accountable principal downstream. #1049 lists that as a non-goal and it stays
  one.
- **The caller cannot steer the target's live session.** ADR-0079's rule that a
  job is an output and not a steering input was about jobs, but the same
  reasoning applies to a second agent: a call arriving mid-run defers rather than
  interrupting. Note this means `is_job` is *not* the predicate the kernel can
  use here, since a delegated turn is a `message`. The steering rule needs to
  key on the caller being an agent, which is a kernel change and is called out in
  Consequences.

There is a real gap under this part. #1049's requirement 2 -- each agent
authenticating as its own IdP-issued identity -- is not built, and neither is a
structured agent identity in telemetry (ADR-0107 records that the closed span
attribute enum carries `curie.session_id` and `curie.sandbox_id` but no agent
identity key). This ADR therefore specifies attribution in the turn payload and
the run record, which is where it can hold today, and does not claim an IdP
story it cannot deliver. That sequencing is stated in Consequences.

### 5. Default closed: the bundle declares, the operator arms

A bundle declares the agents it intends to call. An operator opt-in arms it. With
no operator grant, no call is possible, and the tool is not mounted.

Declaration alone is not authorization, following
[ADR-0056](0056-operator-opt-in-for-policy-gate-grantability.md) (operator opt-in
for gate grantability) and ADR-0098's rule that a knob whose blast radius the
operator carries is not a bundle's to set. The same split as
[ADR-0086](0086-bundles-declare-connectors-the-platform-hosts-them.md) and
[ADR-0087](0087-the-api-renders-connector-objects-the-cli-applies-them.md): the
bundle states intent, the platform decides.

The allowlist is directional. A grant that A may call B does not let B call A.

### 6. Bounded depth, refused cycles, one recorded refusal

Every delegated turn carries the chain of agent identities that produced it and
its depth. The API refuses a call that would revisit an agent already in the
chain, or exceed a configured maximum depth, and records the refusal.

The refusal is returned to the calling model as a plain tool error and is
recorded in the run record. Following ADR-0099's treatment of a failed hook run,
it is a hard stop and never an input the agent may plan around -- the documented
pathology is a denial becoming a recursive spawn trigger.

Fan-out within one turn is bounded by the same mechanism, and the bound is per
root chain rather than per agent, so a caller cannot multiply its allowance by
spreading calls across targets.

### Explicitly out of scope for v1

- **Parallel fan-out with a join.** One call per tool invocation, resumed
  independently. A caller wanting three answers makes three calls and gets three
  resumes. A join primitive is a later ADR if a real consumer asks for one.
- **Cross-tenant calls.** Multi-tenancy is Epic
  [#158](https://github.com/curie-eng/curie/issues/158) and per-tenant identity
  is [#82](https://github.com/curie-eng/curie/issues/82). Until a tenant object
  exists, a call stays within one installation.
- **On-behalf-of impersonation**, per #1049's non-goals.
- **A synchronous variant.** Not until something needs it that the resume path
  cannot serve.

## Consequences

- **This sequences behind ADR-0112.** Delegation adds a fifth producer of
  `QueuedTurn`, and ADR-0112 states that "every producer added before the rename
  widens" its blast radius. Landing delegation first means renaming six producers
  instead of five, for no benefit. It does *not* sequence behind ADR-0099.
- **The protocol takes a minor bump.** New attribution and chain fields on a
  closed schema, plus a new `ReplyHandle.kind` value, under the change-class table
  in [`packages/CLAUDE.md`](../../packages/CLAUDE.md) and the reader policy in
  [ADR-0036](0036-aci-semver-and-reader-policy.md). The tri-language artifacts
  regenerate with it ([ADR-0017](0017-tri-language-contract-codegen.md)), and
  [ADR-0101](0101-schema-compatibility-for-closed-schemas.md) and
  [ADR-0103](0103-previous-schema-shape-gate.md) govern the compatibility gate.
- **The kernel's steering rule needs a second predicate.** A delegated turn is a
  `message`, so `source.is_job` returns False, and the existing rule would let a
  call steer a live session. This is a change in the single-owner kernel area
  (`apps/worker/CLAUDE.md`) and cannot be made from the API lane.
- **Attribution is payload-level, not identity-level, until #1049 is built.** The
  chain and the accountable principal are as trustworthy as the platform minting
  them, which is sufficient because the sandbox never mints them itself -- but it
  is not the IdP-issued agent identity #1049 asks for, and the run record should
  not be described as if it were.
- **Every tier answers the verb**, per
  [ADR-0041](0041-every-verb-is-answered-at-every-tier.md). The skill tier has no
  API and no second agent, so it answers with a clear refusal rather than a
  simulated call. That is a tier-parity obligation and needs stating before an
  implementer improvises a local fake.
- **Cost becomes harder to attribute and easier to run up.** Each agent spends
  its own credentials, so a call moves spend onto the target's keys. The depth
  and fan-out bounds are what keep that from being a runaway, and the run record
  is what makes it visible. Epic #158 must revisit this before anyone else pays
  for the tokens.
- **ADR-0044's phase 1 becomes buildable**, and the external-workflow caller
  reaches the same surface with a scoped token instead of the delegate tool.
- **This is a Draft.** Per [ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md)
  it authorizes no implementation until a maintainer accepts it.

## Alternatives considered

**Let an agent call the target's `/hooks/{agent_id}/{hook}`.** The tempting
option, because the ingress is built and hardened. Rejected on four counts, each
of which is a property the hook ingress deliberately has: the secret derives from
the platform key, which is kept out of the sandbox; `author` is set to
`hook:{hook}` specifically so an upstream cannot supply an identity; the reply
goes to the target's channel binding, so the caller never receives the result;
and the conversation is per hook, so concurrent calls serialize behind one
thread. Fixing all four would leave a different route wearing the hook ingress's
name, which is worse than adding one.

**Let an agent post in Slack and let the mention wake the target.** Requires
removing the `bot_id` filter in `handlers.py`, which exists so the dispatcher's
own placeholder does not re-trigger it. It also makes every delegation public in
a human channel, carries no caller attribution, and gives the loop problem an
unbounded surface. Rejected.

**Use the channel ingress with a `chn` token.** Closest existing surface, and
still wrong: a `chn` token is scoped to a binding row, not to a caller, so it
answers "which binding may this enqueue for" and not "who is asking". The reply
still routes to the binding. It would work as plumbing and would record nothing
this ADR exists to record.

**A synchronous, blocking call.** Simplest to reason about and simplest to
program against, which is why ADR-0044's phase 1 reads as though it wanted one.
Rejected because the target's turn can suspend on an approval gate for an
unbounded human-shaped interval, and a caller holding a sandbox open across that
burns the envelope ADR-0059 bounds. The resume path already solves exactly this
for approvals.

**Model delegation as a hook fire on the target.** Would fold this into
ADR-0099 and reuse its scheduler, claim, and run record. Rejected because a hook
is a job with no caller and no audience, and a call has both. Forcing a call
through the job path is what drops the caller -- the specific failure this ADR
exists to prevent -- and `is_job` would then be wrong about it in the kernel.

**One attribution field instead of two.** Carry only the accountable principal
and let the immediate caller be inferred from the chain, or the reverse. Rejected
because the two fields answer different questions at different times: the
authorization and cycle checks need the immediate caller at call time, and the
audit trail needs the accountable principal at read time, when the chain may be
gone.

**A shared state namespace as a mailbox.** Two agents write to one namespace
and poll it. Requires breaking the per-agent token binding in ADR-0073, which is the
property that makes the state store safe to hand a sandbox, and replaces a turn
with a polling loop nothing schedules. Rejected.

**Let the caller's authority flow to the target.** Would make a call behave like
a function call in one trust domain, and is what most naive implementations do by
accident. Rejected: it is precisely the laundering path in the Context, and it
inverts ADR-0034's decision to resolve approvers server-side rather than trust an
assertion from inside a sandbox.

**Do nothing and let subagents cover it.** Subagents inside one turn are real and
already work, and for decomposition inside one agent's own capability they are the
right tool. They cannot express two agents with different owners, credentials,
approval policies, and channel bindings collaborating, which is the case pillar 4
names and the case #1049 is about.
