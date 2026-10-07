# 106. An approver is an authenticated principal, never a caller-asserted string

Date: 2026-08-14

Status: Accepted

Amended by [ADR 0202](0202-a-test-installation-admits-a-listed-bots-actions-and-approval-replies.md): one new principal kind, `test_driver`, which only an
explicit approver list accepts.

Raised by [#1531](https://github.com/curie-eng/curie/issues/1531) (findings 1
and 2) and by the 2026-08-14 comment on
[#1495](https://github.com/curie-eng/curie/issues/1495).

Extends [ADR 0010](0010-approval-gates-and-human-in-the-loop.md) and
[ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md) on the
identity axis neither of them settles. ADR 0010 decided **where** the approval
decision runs (server side, at resolution time). ADR 0034 decided **which set**
answers "is this actor a member". Neither decides **what an actor is**, and the
whole authorizer stands on that unanswered question. This ADR also supersedes
ADR 0034's universal self-approval prohibition: set membership remains the
authorization boundary, but requester inequality is no longer a global
precondition.

Consumes the canonical principal that
[Discussion 1049](https://github.com/curie-eng/curie/discussions/1049)
establishes and that [ADR 0088](0088-per-user-delegated-oauth-for-mcp.md)
already depends on. Closes the residual
[ADR 0033](0033-scoped-sandbox-state-token.md) named and
deferred.

## Context

Three callers reach `POST /approvals/{id}/resolve` today, and **all three put
the approver's name in the request body**:

| Caller | What it sends as `resolved_by` | What authenticates the call |
|---|---|---|
| Slack card click (dispatcher) | `body["user"]["id"]` from the interaction payload (`approval_actions.py:617`, `:802`) | the shared platform API key |
| Console (`RealApprovals.tsx:139-149`) | a free text box, remembered in the browser | the shared platform API key |
| `curie cluster approvals --resolve ID --as NAME` (`cli/src/main.rs:1122`, `:1138`) | whatever the operator typed | the shared platform API key |

The wire contract makes it explicit. `ApprovalResolve` is
`resolved_by: str = Field(min_length=1)` plus an optional `actor_channel`
(`apps/api/src/curie_api/schemas.py:663-674`), and the router is guarded by
`require_api_key` and nothing else. Its own module docstring states the position
without hedging (`apps/api/src/curie_api/routers/approvals.py:10-13`):

> Authorization today is the shared API key, like every router. WHO may resolve
> (channel membership, self-approval block) is the server-side authorizer of
> #246 and slots in at this endpoint.

So does the authorizer, which has carried the honest limit in its docstring
since #420 (`apps/api/src/curie_api/authorizer.py:31-32`):

> One honest limit on that evidence: it proves the ASSERTED identity satisfied
> policy at click time, not who actually clicked (the ADR-0033 residual, tracked
> separately).

Everything downstream of that string is built well and rests on nothing. The
authorizer refuses self-approval, resolves membership through a fail closed
`ApproverSet`, and writes an append only audit row snapshotting the actor, the
channel, the authorizer that decided, and the membership evidence
(`apps/api/src/curie_api/models.py:221-251`). The name in every one of those
`actor` columns is a value the caller chose. The resume turn then feeds the same
string back into the agent's prompt as the authority for continuing
(`apps/api/src/curie_api/resumequeue.py:134-138`), so an asserted identity is
not only stored, it is model visible.

Only one of the three callers has any authentication behind the name at all.
The dispatcher runs in Socket Mode, where interactions arrive over Slack's own
authenticated websocket (`apps/dispatcher/src/curie_dispatcher/app.py:24-27`),
so Slack really did authenticate the click and the dispatcher really is relaying
a proven user id. But the API cannot tell that relayed id apart from a typed
one, because both arrive as the same field on the same credential. **The one
trustworthy path is indistinguishable from the two untrustworthy ones.**

ADR 0033 is often read as having closed this. It did not. It removed the
sandbox's copy of the resolve capable platform key, which stopped an agent
approving its own gate. It left untouched the fact that **any** holder of that
key, which today includes the dispatcher, the CLI, the console's backend, and
every operator, can name any approver they like. ADR 0033 said so and deferred
it; this ADR is that deferral coming due.

### Why now: the gate started blocking real writes

The 2026-08-14 comment on #1495 reports the connector gate fix validated end to
end on a single node k3s cluster: deploy validation accepts
`mcp__<connector>__<tool>` gates, the runtime gate fires on the live tool call,
a durable approval is created, and the one shot grant executes exactly once. The
full write loop ran twice, and the write in question was **a deployment restart
behind a human approval**. Before that fix, gates on connector tools armed
nothing (the manifest path failed open) or bricked the agent (the operator
path), so the audit trail recorded very little of consequence. It now records
who authorized a production change, and it records it as a string someone typed.

### The single operator dead end

#1531 finding 1 reports the other end of the same defect. Self-approval is
blocked structurally and deliberately: the actor who authored the turn may not
resolve it, whatever channel they click from, under every approver set, and no
set is ever asked (`authorizer.py`, the AC2 design). That treats every approval
as a two-person separation-of-duties control. Curie's ordinary approval gate is
instead an explicit confirmation: an authenticated requester who belongs to the
configured approver set is authorized to confirm the action. In a workspace
where the requester is the only human in the channel, the current ordering makes
the write path unusable despite that authorization.

The reporter got past the dead end by resolving through the CLI as a second
actor. There was no second actor. That assertion hole turns the audit trail into
a record of what someone typed. The correct repair separates identity from
policy: authenticate the principal, then ask the configured approver set whether
that principal may approve. Requester equality neither grants nor denies that
membership.

### What this ADR does not decide

#1531 finding 3 (the resolve hint prints the stub channel `C0LOCALDEV` instead
of the route's bound channel) is a bug, not an identity decision, and stays in
#1531. The undocumented rule that two `grantableViaPolicy` gates cannot share
one route is a docs note, likewise. Neither is settled here.

## Decision

**An approver is a principal the platform authenticated. `resolved_by` stops
being an input and becomes a value the server derives from the credential that
made the call.**

Three principal kinds, one per caller class, each with its own proof:

1. **`chat`.** The dispatcher is a **trusted attester**, not an asserter. It
   authenticates with its own credential carrying the claim "may attest chat
   identities for workspace W", and the Slack user id it relays is accepted as
   an attested identity because the platform knows which component attested it
   and what that component is entitled to attest. This is the path that already
   works; the change is that the API can now tell it apart from the others.
2. **`console`.** The console session (#630, and the CLI minted login codes
   proposed in PR #1029) already knows who is logged in. The "who is resolving
   this approval" text box is **deleted**, and the session subject becomes the
   approver.
3. **`operator`.** `--as` on the shared platform key is **removed**. The
   platform key is a machine credential, and a machine is not an approver. An
   operator who resolves from a terminal presents a per operator credential:
   a signed principal token minted exactly the way ADR 0033 mints the sandbox
   state token (HMAC SHA256 over `api_key`, compact self describing claims,
   here `sub`, `scope: approve`, `exp`), so this needs no new shared secret, no
   new config plumbing, and no principal table to start.

The platform key alone resolves nothing. That is the load bearing half of this
decision: today it resolves anything, as anyone.

**`actor_channel` follows the same rule, or the hole simply moves.** It is
membership evidence, so an asserted channel defeats a channel bound approver set
just as an asserted name defeats the identity boundary. A `chat` principal's
channel is attested by the dispatcher alongside its user id. An `operator`
principal **asserts no channel at all** and is authorized only by appearing in a
route's explicit approver list (`ExplicitUsers`, ADR 0034), never by claiming to
stand in a channel it cannot be seen in.

**The audit row records the proof, not only the actor.** Two columns:
`principal_kind` (`chat` / `console` / `operator`) and `authenticated: bool`.
Rows written before this decision migrate as `authenticated: false` with a null
kind. They are not retro-labeled as anything, because what they actually record
is an assertion and the trail should keep saying so.

### An authorized requester may approve

**The approver set is the authorization boundary.** The authorizer always asks
the selected set whether the authenticated principal may approve, including
when that principal requested the turn. Membership permits the approval;
requester equality does not bypass membership and does not veto it. The audit
row records the same authenticated principal as requester and approver when that
is what happened.

This supersedes only ADR 0034's rule that the self-approval check runs before
the set is consulted. Its set-selection precedence, fail-closed membership
resolution, and server-side placement remain unchanged. A deployment that needs
two-person separation of duties must declare that as a distinct policy; it is
not implied by an ordinary approval gate and is not introduced by this ADR.

## Consequences

**The audit trail becomes answerable.** "Who approved the restart" gets an
answer with a proof attached, which is the property #1495's now working write
loop needs and does not have.

**Every resolve caller changes, so this cannot land as one patch.** The wire
contract moves (`resolved_by` leaves `ApprovalResolve`), which is a stop and
escalate contract change under `AGENTS.md` and lands on its own before the
dependent lanes. Then the dispatcher credential, the console session subject,
and the CLI credential, in that order. The API must accept both shapes only for
the length of that sequence and not one release longer; a permanent dual mode
here is the compatibility path that keeps the hole open behind a green gate.

**`curie cluster approvals --resolve --as` breaks for existing operators.**
Deliberate. It is the exact affordance #1531 finding 2 reports, and preserving
it under any name preserves the finding. The replacement (mint an operator
credential once, then resolve without naming yourself) is a strictly smaller
thing to type.

**Solo installs remain usable without fictional identities.** A single operator
who is an authenticated member of the configured approver set may request and
approve an action. The audit trail says plainly that the same principal did
both; it does not manufacture a second actor or silently claim two-person
review.

**Skill tier is unaffected and stays unaffected.** ADR 0077 already holds that
durable approvals are unavailable there, so there is no principal to invent for
a tier that has no approvals.

**This is one principal, not a second one.** Discussion 1049 needs a canonical
caller identity for invocation authorization, and ADR 0088 consumes that same
principal to select a delegated MCP credential. Approvals reaching it first is
sequencing, not a parallel identity system. If approvals instead invented an
approver specific identity, 1049 would arrive later and have to reconcile two.

**Not decided here: where principals come from at enterprise scale.** The signed
token above is deliberately the ADR 0033 shape, which needs no storage and no
lookup. An IdP issued principal (Discussion 1049's requirement 2) replaces the
minting without changing anything this ADR decides, because what this ADR
decides is that the server derives the approver from a credential. Which
credential is a later ADR.

## Alternatives considered

**Chat is the only approver, and non chat callers lose the ability to resolve.**
The tempting minimal answer: Slack already authenticates the click, so make the
card the only trustworthy resolution and delete `--as` with no replacement.
Rejected. It buys the trail with no token machinery at all, but it makes Slack a
hard dependency of the write path for every install, strands the console
entirely, leaves the single operator dead end permanently unrecoverable with no
non chat recourse, and hands the same gap to the first party email channel work
(#1515) on arrival. Curie's channel port exists precisely so the platform does
not fuse a capability to one provider, and fusing the approval identity to Slack
would put the fusion back one layer down.

**Keep assertion, and document it (status quo, made explicit).** Write down that
the audit trail records an assertion, keep `--as`, and rely on operational
control of the platform key. Rejected. It costs nothing and it is at least
honest, but it is honest about a control that does not control anything: one key
is held by the worker, the dispatcher, the CLI, and every operator, so **no**
audit row is attributable to a human, and a compromised dispatcher approves as
anyone. #1531 shows the further problem with documenting it: the workaround is
so natural that the reporter used it for every approval in the test without
treating it as a compromise. An approval gate whose trail cannot be trusted is a
compliance artifact, not a control, and it is worse than no gate because it
reads as one.

**Add a solo-mode bypass around the approver set.** Rejected. Authorized
self-approval is not an override: the requester passes the same authenticated
membership check as every other approver. A flag that skips that check would
recreate the assertion hole with a nicer name. Two-person separation of duties,
when required, belongs in an explicit policy rather than an install-wide bypass
or an unconditional rule for every gate.

**Bind approvers to Slack identity in the API and drop the concept of a
principal.** Store Slack user ids as the identity type and have the API verify
them against the workspace directly. Rejected as the same fusion as the first
alternative, one layer deeper, and it also puts a Slack Web API call on the
resolve path that ADR 0034 deliberately kept behind an `ApproverSet` port with
its connection pool discipline.

**Do nothing until Discussion 1049 lands.** Defensible on sequencing, and
rejected on exposure. 1049 is scoped to invocation authorization and explicitly
leaves downstream delegation out; it has no dated plan, while the connector gate
fix in #1495 means approvals are gating real infrastructure writes now. The
principal shape decided here is a subset of what 1049 needs, so this is early
delivery of that work on the path where the exposure is, not a competing design.
