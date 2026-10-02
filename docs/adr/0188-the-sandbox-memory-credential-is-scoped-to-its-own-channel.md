# 188. The sandbox memory credential is scoped to its own channel

Date: 2026-10-01

Status: Draft

Tracked in [#3623](https://github.com/curie-eng/curie/issues/3623), which
widens [#3394](https://github.com/curie-eng/curie/issues/3394).

This ADR, once Accepted, supersedes in part
[ADR-0033](0033-scoped-sandbox-state-token.md), for the `memory` namespace only.
It builds on [ADR-0167](0167-agent-and-channel-memory-are-written-by-the-agent-guided-by-editable-guidance.md)
and makes its promise that "the model cannot set the author" hold at the API
rather than only in the tools.

## Context

[ADR-0033](0033-scoped-sandbox-state-token.md) gave the sandbox a signed state
token in place of the platform key. The token carries three claims: `agent`,
`scope` and `exp`. The API accepts it on any path under that agent's state. So
the boundary is the agent, and nothing smaller.

[ADR-0167](0167-agent-and-channel-memory-are-written-by-the-agent-guided-by-editable-guidance.md)
then added channel memory. Each channel binding gets its own partition, keyed
by `binding_scope`, under
`/agents/<id>/state/bindings/<kind>/<address>/memory`. The worker hands the
sandbox the path for its own channel in `CURIE_CHANNEL_MEMORY_REF`, and the
same broad `state` token as `CURIE_MEMORY_TOKEN`.

The two decisions do not fit together. The API treats a binding path as a
partition key, not a permission. `_binding_scope` in
`apps/api/src/curie_api/routers/state.py` says so in its docstring: it checks
that the binding exists for this agent, and calls that "not a security
boundary". The #1525 follow-up rejected adding a binding claim to the token for
the same reason. That was sound while every partition held the agent's own
data. It stops being sound once one partition is a direct message.

The probe on #3623, run from inside a live sandbox at `fd865dee5`, found:

1. **Cross-channel reach.** Editing the channel in `CURIE_CHANNEL_MEMORY_REF`
   gave 200 on GET and PUT against another channel's memory of the same agent.
   A DM's memory is readable from any channel the agent serves.
2. **Forged authors.** A raw PUT stores any `author`. The tools take the author
   from the turn (`MemoryTurn` in `runner/src/curie_runner/memory_facts.py`),
   but the API takes whatever the body says.
3. **Writes off is not read-only.** With memory writes off, a sandbox still
   overwrote `memory/guidance` and planted an agent fact that loaded in the next
   session.

Cross-agent access was refused, as ADR-0033 intends. And when asked in chat,
the model declined to run the cross-channel read. So today this rests on the
model's judgment, not on a control.

Two more facts shape the options:

- **The token lives as long as the sandbox, not the turn.** The worker mints it
  in `boot_env` (`apps/worker/src/curie_worker/binding.py`) with a 24 hour
  expiry. A warm sandbox serves many turns in a thread, and the sender changes
  from turn to turn. A claim on this token cannot name the sender.
- **Hiding the token from Bash has landed, but is not a boundary.** Since
  [#3612](https://github.com/curie-eng/curie/pull/3612) (after the probe), hooks
  and other subprocesses get an env without `CURIE_*TOKEN*` names, and Bash
  unsets them in a root-owned prelude (`runner/src/curie_runner/subprocess_env.py`).
  But the runner process still holds the token, and Bash runs as the same user
  (uid 1000) in the same container. Code that runs as that user can look for
  it there. This narrows casual reach. It cannot be what the API relies on.

## Decision

The API, not the sandbox, decides what the memory credential can do. Each
point below is checked on the server for every request that is not made with
the platform key.

### 1. Scoped to its own agent and its own channel

The sandbox's memory credential carries a new `binding` claim: the
`kind:address` of the channel binding the sandbox was booted for. On the
`memory` namespace the API allows:

- agent memory: `/agents/<agent>/state/memory`, where `<agent>` matches the
  `agent` claim, as today;
- channel memory: `/agents/<agent>/state/bindings/<kind>/<address>/memory`,
  only when `<kind>:<address>` equals the `binding` claim.

Any other binding's memory gets 403. A credential minted with no binding (an
eval or a turn with no channel) reaches agent memory only.

This makes the binding a security boundary for the memory namespaces. It
supersedes, for the `memory` namespace only, these parts of
[ADR-0033](0033-scoped-sandbox-state-token.md)'s Decision:

- the opening sentence, "The token grants access to exactly one agent's state
  namespace and nothing else", where "namespace" meant the whole agent;
- the **Payload claims** list, which had only `agent`, `scope` and `exp`;
- the **Verification (API)** rule, which checked only that `agent` matches the
  path, `scope` equals `state` and `exp` is in the future.

It also reverses the #1525 rejection of a binding claim, for memory. The rest
of ADR-0033 stands: the HMAC signing with `api_key`, the token format, the
24 hour expiry, the duplicated module and its golden vector, the platform key's
full access, and the token's reach on transcripts and general state.

### 2. With memory writes off, read-only

The credential carries a `memory` claim, `read` or `write`. With writes off it
is `read`, and the API refuses PUT, POST and DELETE on the `memory` namespace.
Under decision 4 the long-lived credential is always `read`; only the per-turn
credential can write.

### 3. Never the guidance

Only fact keys (`fact-` followed by 32 hex characters, the shape the tools
mint) are writable with a sandbox credential. The reserved keys in agent
memory, `guidance` and the legacy `log`, are writable only with the platform
key, which is what `curie cluster memory` uses. The sandbox can still read
them, since the runner loads them at boot.

### 4. Authors are stamped by the API, from a per-turn credential

For each turn on an agent with memory writes on, the worker mints a second,
short-lived credential: `memory` scope `write`, with claims `agent`, `binding`,
`sender` (the turn's `event.user`, or `<no person>` for a job or eval) and
`turn` (the run id, one per attempt: the event id plus a suffix). The worker
mints it just before the runner request that opens the turn, so it expires
when that request's stream times out: the turn's time limit. A message steered
into a live turn gets its own credential, naming its own sender, which expires
no later than the live turn's time limit when the same worker opened that
turn. If another worker opened it, the steering message's own time limit
applies. A turn with no time left gets no credential. The credential is
refused once its turn ends (#3776). The worker sends it with the turn on the
ACI `Event`, as a new optional field, not in the sandbox env. The runner's
memory tools present it for writes.

On a write, the API ignores any `author` in the body and stores the `sender`
claim instead. Writes with the platform key keep the body's author, since the
operator is trusted to say who they are.

This is the recommended option of three. The other two are under Alternatives.
It is the only one where the API can prove who sent the turn without trusting
the sandbox and without a database lookup. The cost is an additive field on a
frozen contract, which gets its own issue before code, as ADR-0167 did for
`CURIE_CHANNEL_MEMORY_REF`.

What this proves, and what it doesn't: a stored author is the sender of the
turn the write happened in, while that turn's credential is held. It does not
prove that person said the fact. Code in the sandbox that gets hold of a turn's
credential can write a fact attributed to that turn's real sender while the
turn lasts. It cannot name anyone else, write to another channel, or write
after the turn ends: when an attempt at a turn ends, on any outcome, the worker
reports its turn to the API (`POST /v1/internal/memory/closed-turns`, worker
token), and the API refuses writes with that turn's credential from then on,
before it expires ([#3776](https://github.com/curie-eng/curie/issues/3776)).
Reads with it still work. A steer's credential is closed when the live turn it
joined ends. If the worker cannot reach the API, the credential falls back to
expiring at the turn's time limit (for a steered message, the live turn's, or
the message's own when another worker opened that turn).

### 5. Keep the credential out of Bash and hooks, as defense in depth

Keep the scrub from #3612 and make sure the per-turn credential is covered by
it too. It should not travel in any env var, which is one reason it rides on
the ACI event. Moving the credential out of reach of the sandbox user entirely,
for example a separate process user for the runner or a broker that signs
requests on its behalf, is a separate decision. It changes the runner image and
the sandbox's process model, and this ADR does not depend on it: decisions 1
to 4 hold even if code in the sandbox reads every credential the runner has.

## Consequences

- **Version skew.** The token verifier ignores claims it does not know, so a
  new worker talking to an old API keeps working as today, with today's reach.
  A new API seeing an old token (one with no `binding` or `memory` claim)
  treats it as read-only on `memory`, reaching agent memory and no channel
  memory, and logs a warning. Memory tools on such a sandbox report their
  writes as refused, which the default guidance already tells the agent to say.
  Old tokens age out within the 24 hour expiry once the worker is upgraded.
  An old runner ignores the new `Event` field and writes with its env token,
  which is refused for the same reason.
- **Migration.** No stored data changes. Facts saved before this lands keep the
  author the runner wrote, and nothing can check those now. An operator who
  is worried about planted facts can review them once listing exists
  ([#3393](https://github.com/curie-eng/curie/issues/3393)). The golden test
  vector for the duplicated token module changes, in both apps.
- **What an operator sees.** Nothing changes for normal use: the tools work,
  facts load, `curie cluster memory` works. A request outside the credential's
  reach gets 403 with a reason, and the API logs a warning naming the agent and
  the refused path. With writes off, there are no tools and no way to write.
  The interim warning in `docs/operations.md` can be removed when this ships.
- **Performance.** The new checks are string comparisons on claims the API
  already decodes, plus one HMAC verify per request, as today. Decision 4 adds
  no database lookup. The binding check reuses the existing
  `agent_holds_channel_pair` query that `_binding_scope` already runs.
- **Transcripts are covered by #3767.** Transcript keys are thread keys, not
  binding paths, so scoping them is a separate change,
  [#3767](https://github.com/curie-eng/curie/issues/3767). It uses the
  `binding` claim added here: the API maps a transcript key back to its binding
  and holds the sandbox credential to its own channel's threads. A token minted
  before this ADR keeps its transcript reach until it expires, with a warning.
- **One more credential to mint per turn.** It is an HMAC over a few claims and
  costs nothing measurable, but it is a second thing the runner must route to
  the right tool call.

## Alternatives considered

- **The API records the author from a turn id it looks up.** The write carries
  a run id; the API reads the run row and takes the sender from it. This needs
  no contract change. But the sandbox chooses which run id to send, so it can
  cite any earlier turn in its channel and borrow that turn's sender. Closing
  that means checking the run is the one live in this sandbox right now, which
  needs live-run state the API does not keep, and a database read on every
  write. Rejected in favour of a signed claim, which carries the same facts
  without the lookup.
- **Keep runner-stamped authors and mark API-written facts unverified.** The
  cheapest option: no new credential, and the stored value gains a flag. But
  the API cannot tell the runner's write from Bash's, since both hold the same
  token. Every sandbox write would be marked unverified, which tells an
  operator nothing. Rejected, though it is a fair fallback if the ACI change is
  refused.
- **Put the sender on the long-lived token.** Rejected: the token outlives the
  turn, and the next turn's sender is someone else.
- **Mint the long-lived token per turn instead.** Rejected: the env is fixed
  when the sandbox boots, so a fresh token per turn has no way in without the
  same ACI change, and an env var is what Bash and hooks are most likely to
  reach.
- **Rely on the env scrub alone (#3394's first option).** Rejected as the
  boundary, for the reason in Context: the runner holds the token as the same
  user. Kept as defense in depth (decision 5).
- **Separate URLs instead of claims.** The worker already hands each sandbox
  only its own channel's path. Rejected: the probe shows a sandbox can edit a
  path. A path is a request, not a permission.
