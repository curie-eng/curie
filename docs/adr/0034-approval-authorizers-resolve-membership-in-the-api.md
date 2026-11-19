# 34. Approval authorizers resolve membership in the API

Date: 2026-07-15

Status: Accepted

**Partially amended by [ADR 0271](0271-per-request-approver-sets-and-separation-of-duties.md)**
(back link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
ADR 0271 amends decision 2 for per-request selection. It stores selected people
for dynamic routes while preserving current authority and missing-route refusal.

**Partially superseded by [ADR 0106](0106-an-approver-is-an-authenticated-principal.md).**
ADR 0106 supersedes only the universal pre-membership self-approval prohibition;
the server-side, fail-closed approver-set design remains in force.

Implements [#420](https://github.com/curie-eng/curie/issues/420). Supersedes
ADR-0010's framing of the authorizer line: where ADR-0010 named channel
membership, user-group, explicit user-list, and platform-RBAC as four
`Authorizer` implementations, this decision makes them four APPROVER SETS behind
a single authorizer. Platform-RBAC becomes the fourth set, not the fourth
authorizer. Nothing else in ADR-0010 is reversed: the approval primitive, the
server-side placement, and the sequence the line is built in all stand.

## Context

[ADR-0010](0010-approval-gates-and-human-in-the-loop.md) (Accepted) established
the approval primitive and named the authorizer line it intended to build:
channel membership first, then user-group, explicit user-list, and
platform-RBAC, all behind one server-side check at resolution time. Only the
first landed. This ADR extends that line with the next two, and reshapes what
"implementation" means for all four (see the supersession note above).

Today "who may approve" is fused to "where the card posts". The only
implementation is `ChannelMembershipAuthorizer`
(`apps/api/src/curie_api/authorizer.py`), and the approver set is whoever can
see the channel the worker routed the card into. That forces a choice no
operator should have to make: post the card somewhere visible and accept a broad
approver set, or narrow the approver set by hiding the conversation. Unfusing the
two axes means the route binding keeps `channel` as the *where* and gains an
optional `approvers` block as the *who*.

The moment an approver set is a Slack user group rather than a channel, the
platform must answer a question it has never had to answer: how does the server
learn who is in that group? That question is cross-cutting. It decides whether
the API grows an outbound dependency on the Slack Web API and a credential it
has never held, and it decides whether the authorization decision stays
trustworthy or quietly becomes something the caller asserts. It is not a
detail of one endpoint.

It is also a first. `apps/api` has never made an outbound call to Slack: the
dispatcher and the worker hold that relationship, and the API's only Slack
knowledge until now was the channel IDs it stored and handed back. So this
change is not one more call added to an existing client -- it opens a door, and
what comes through that door lands in whatever module is holding it.

One framing correction underlies the shape below. It is tempting to read channel
membership as the neutral baseline and the user group as the Slack feature bolted
beside it. That is wrong. Both are Slack: they are two ways Slack expresses "who
is in the authorized set" -- everyone in this room, and everyone on this list
Slack maintains. Only the explicit user list owes Slack nothing. Reading the
channel as neutral is what produced the fusion of *where* and *who* in the first
place, and repeating it would have put Slack's two shapes on opposite sides of
the abstraction.

## Decision

### 1. The API resolves user-group membership itself

The API calls Slack's `usergroups.users.list` with its own bot token
(`SLACK_BOT_TOKEN`, needing the `usergroups:read` scope) and caches the result
in process. The decision is made from membership the server fetched, never from
membership a caller supplied.

### 2. The route binding is read fresh at resolve time

The resolver reads `agents.approval_routes[<route>]` at the moment of the click,
using the approval's `agent_id` and `route`. Nothing about the approver set is
snapshotted onto the `Approval` record at creation time.

### 3. Fail closed, precisely scoped

When a binding read at resolve time DOES declare an `approvers` spec, every
lookup and config error denies: no bot token configured, an HTTP error, a
network error, an `ok: false` body, a malformed body, or malformed approvers
JSON. Each denies with a could-not-verify reason and an audit row. None of them
falls back to channel membership. Failures are never cached.

This does NOT mean the absence of a declaration fails closed. A binding with no
`approvers` spec means channel membership, by design: that is the zero-setup
default and the behavior every existing deployment already has.

### 4. One authorizer, four approver sets behind an async port

`ApproverSet` (`approvers.py`) is the line this ADR draws:
`async contains(actor, actor_channel) -> MembershipVerdict`, plus an
`audit_name`. A set answers ONLY "is this actor in the set". There are four:
`SlackChannelMembers` and `SlackUserGroupMembers` (`slack_approvers.py`),
`ExplicitUsers` (`approvers.py`, the one that owes Slack nothing), and
`InvalidApprovers` (`approvers.py`) for a declared block the platform cannot
read. That last one is a set rather than a special path on purpose: an unreadable
block is not the absence of policy, it is policy nothing can evaluate, so it
admits nobody and determines nothing. Modelling it as a set means the authorizer
has exactly one code path and never learns that config errors exist.

Membership lookup sits behind a second, narrower port, `GroupMembershipSource`
(`usergroups.py`): `async members(group_id) -> UserGroupMembership`, raising
`UserGroupLookupError` for every mode that yields no member set.
`SlackUserGroupClient` (`slack_usergroups.py`) implements it.

Selection is `ApproverSetSelector` (`approvers.py`), implemented by
`SlackApproverSetSelector` (`slack_approvers.py`). It lives on the Slack side
deliberately: reading a binding means parsing `ApprovalApprovers`, whose schema
validates `S...` usergroup IDs and `C...` channel IDs, so the code that reads a
binding is Slack-aware by nature. Putting selection in the authorizer would have
forced the authorization decision to import the provider's classes in order to
choose between them, which is the coupling this decision exists to prevent.
`main.py` is the composition root and the only module that names Slack to build
the selector; `authorizer.py` contains no Slack at all.

The two Slack sets are not symmetrical, and the port does not pretend otherwise.
`SlackChannelMembers` performs no lookup: the click's channel IS the proof, which
is why `contains` takes `actor_channel` at all. `SlackUserGroupMembers` must go
and ask, so it is the only set that can come back `undetermined` -- a third state
that is NOT `member=False`, because "you are not in the set" and "we could not
find out" deny for different reasons and must stay distinct.

**This reverses the synchronous-pure-port position** taken earlier in this ADR's
own life. That argument was that lifting the fetch into the resolver keeps every
implementation pure and the port unchanged. What changed the decision: a set that
owns its own lookup deletes the resolver's fetch-then-post-attach-evidence dance.
With a sync port the resolver had to fetch, construct the authorizer with a
member set, catch the lookup error, and then splice `fetched_at`, `cache_age_s`,
and `error` onto evidence the authorizer could not know -- a helper existed
purely to paper over the seam being in the wrong place. Async moves the fetch to
the only object that knows what it is fetching and lets each set report its own
complete evidence. The purity the old argument protected is still there; it just
lives in `ExplicitUsers`, which never awaits anything.

### 5. Precedence, and self-approval as a structural invariant

Explicit `users` wins over `group`, which wins over channel membership.

Self-approval is enforced ONCE, in `authorize_approval`, after set selection and
before the set is consulted. A set is never asked and cannot skip it. This
replaces three implementations each promising in a docstring to re-check
self-approval themselves, held together by nothing but that promise: an
authorizer that forgot the check would have been silently unsafe and every test
of it would still have passed. Now no set can be unsafe about self-approval,
because no set participates in it. The check sitting before `contains` also keeps
its old I/O property: a self-attempt spends no rate-limit budget and cannot be
used to probe who is in a group.

The check sits after set selection, because the audit row names the set that
would have decided and that requires having selected one. Selection performs no
I/O, so "after selection" and "before any lookup" are the same instant.

One consequence, accepted: an author self-clicking a route whose approvers block
is unreadable is now told "self-approval is blocked" rather than "could not
verify approvers". Both deny, nothing is widened, and the audit row still records
`InvalidApproversSpec`, so an operator reading the trail still sees the block was
unreadable. Nothing in production can observe the change: `InvalidApproversSpec`
is introduced by #420 and does not exist on main.

### 6. Audit records the authority, not just the actor

Audit rows gain a structured `evidence` object naming the basis of the decision:
the channel pair for channel membership; the group ID, the actor's membership
verdict, the member count, and the fetch time for a user group; the list and the
actor's presence in it for a user list; the failure class for a lookup failure.
The full member list is deliberately not stored: a 500-member group would bloat
an append-only table on every click, and the group ID plus the verdict is the
fact worth keeping.

## Alternatives considered and rejected

1. **The dispatcher resolves membership and passes it to the API as evidence.**
   Rejected, and this is the door this ADR is mainly here to close. The
   dispatcher already holds a bot token and could attach "this actor is in group
   S123" to the resolve call. But the resolve endpoint authenticates with the
   platform API key, so that field is asserted by whoever holds the key, not
   proven. Any platform-key holder could forge group membership, which makes the
   authorizer a formality rather than a control. An authorization port whose
   input the caller controls is not an authorization port. The API must learn
   membership from Slack directly, at the cost of a token and a scope it did not
   previously need.

2. **Snapshot the approvers spec onto the `Approval` row at creation time.**
   Rejected. Approvals pend for hours to days. A snapshot lets a user removed
   from the approver group yesterday resolve a stale pending approval today.
   Evaluating against current policy at the decision point is the correct TOCTOU
   direction for authorization. A snapshot would also need a new column, a worker
   change to populate it, and an edit inside the kernel's pause path, none of
   which the fresh read requires.

3. **Keep a synchronous `Authorizer` port and lift the fetch into the resolver.**
   Considered first and rejected on review; see decision 4 for what changed the
   position. In short: it keeps the implementations pure at the cost of making
   the resolver assemble evidence on their behalf, which is the seam being in the
   wrong place rather than an absence of I/O.

4. **Keep one authorizer per approver kind.** Rejected, and this is the change
   review actually forced. Three `Authorizer` implementations each re-checking
   self-approval means the invariant is a convention, not a mechanism; it also
   put Slack's two shapes (a channel, a user group) on opposite sides of the
   abstraction while pretending the channel was neutral. One authorizer over a
   membership-only port makes self-approval unskippable and puts both Slack
   shapes where they belong, next to each other and behind the port.

5. **Fall back to channel membership when a group lookup fails.** Rejected. It
   converts a Slack outage or a missing scope into a silent widening of the
   approver set, which is the exact failure mode an approval gate exists to
   prevent. Denying a legitimate approver during an outage is recoverable; a
   silently widened approver set is not visible until after the fact.

6. **Accept usergroup handles (`@deal-desk-approvers`) in bindings.** Rejected
   here, following the channel-ID precedent: names never route, they fail
   silently when renamed, and the binding schema takes IDs. Handle-to-ID
   resolution is admin UX and is tracked separately.

## Consequences

- **The API gains an outbound Slack dependency and a credential.** The chart
  reuses the existing shared `slackBotToken` secret key that the dispatcher and
  worker already consume, so there is no new secret material, but the API now
  reads it. Note the rollout gotcha: a `secretKeyRef` env resolves once at pod
  start, so rotating the token needs a pod rollout of the API, not just a
  `helm upgrade`. Slack-free installs set nothing and lose nothing.

- **The ports relocate work; they do not shrink it.** Three ports and four set
  classes across five modules replace three authorizer classes in one, and they
  save nothing today: there is no second provider, and the binding schema is still
  Slack-shaped (`schemas.py` validates `S...` usergroup IDs and `C...` channel
  IDs), so a non-Slack provider would need a schema change as well as an adapter
  and a selector. This narrows the coupling; it does not make the path
  provider-neutral. What was bought is dependency direction -- keeping the
  authorization decision free of a vendor client on the first call that could have
  put one there -- and the AC2 invariant, which is now unskippable rather than
  promised three times. Read as future savings it is oversold. Read as where Slack
  is allowed to live and as what makes self-approval structural, it is the point.

- **The audit vocabulary is frozen, and now reads oddly.** Each set's
  `audit_name` pins the class name it had before this decision, so
  `approval_audit.authorizer` still records `ChannelMembershipAuthorizer`,
  `ExplicitUserListAuthorizer`, and `UserGroupAuthorizer` even though those are
  now sets and no such classes exist. The column is append-only history: rows
  already on main carry these values, and renaming the vocabulary mid-stream
  would make every old row lie about what decided. The oddness is the cost of a
  truthful trail, and it is the right trade. New vocabulary needs a new column,
  not a redefinition of this one. `InvalidApproversSpec` and `ExpirySweeper` are
  frozen for the same reason.

- **`usergroups:read` requires reinstalling the Slack app.** An existing
  installation keeps its old grant until the app is reinstalled to the
  workspace. Until then a route declaring an approver group denies with the
  could-not-verify reason, exactly as designed. Channel and user-list approvers
  are unaffected.

- **Unbinding a route mid-pend widens authority, and this is accepted.** Under
  fresh-read semantics, deleting or renaming a binding that carried `approvers`
  while an approval is pending widens the approver set from the declared group or
  list to card-channel membership. The resolver cannot distinguish "never bound"
  from "was bound, now unbound"; distinguishing them would require the snapshot
  rejected above. This is intrinsic to the design, not a bug, and it is accepted
  because: mutating `approval_routes` goes through the agent PATCH endpoint,
  i.e. the same platform key that can already resolve any approval directly, so
  no new escalation path exists; fail-closing unbound routes would reject
  approvals that resolve fine today, breaking the guarantee that a deployment
  declaring no approvers is unchanged; and the audit row records the widened
  basis actually used, so the trail stays truthful.

- **A 60s membership cache trades revocation lag for rate-limit headroom.**
  `usergroups.users.list` is a Tier 2 Slack method (roughly 20 requests per
  minute). An uncached per-click fetch would let a click storm or a busy
  workspace hit that limit, and a rate limit under a fail-closed rule converts
  into false denials of legitimate approvers, which is a worse outcome than the
  lag it would remove. The cost is that a revoked member can still approve for up
  to the TTL, and an added member waits up to the TTL. Against an hours-to-days
  human flow, 60s is negligible. Failures are never cached, so an outage does not
  stick. Operators who want strict semantics set `SLACK_USERGROUP_CACHE_TTL_S=0`
  for a per-resolve fetch and accept the rate-limit exposure.

- **Evidence proves policy, not identity, and this limit must stay written
  down.** The audit evidence proves that the ASSERTED identity satisfied policy
  at click time. It does not prove who clicked. Identity is dispatcher-verified
  on the Slack path: the dispatcher populates the actor from Slack's
  authenticated interaction payload
  (`apps/dispatcher/src/curie_dispatcher/approval_actions.py:146`). On the
  platform-API-key path it remains caller-asserted, the named residual from
  [ADR-0033](0033-scoped-sandbox-state-token.md) and a tracked follow-up. Richer
  evidence makes a forged resolution look MORE legitimate in the trail than
  today's thinner rows do, which is precisely why the limit belongs in the record
  rather than left implicit. This change keeps the existing trust model; it does
  not claim to fix it.

- **One extra DB read per resolution.** The resolver loads the agent's route
  bindings on each attempt. This is the cost of fresh-read semantics and is
  bounded by the same request that already writes the audit row.

- **Route bindings stay deployment config.** `approvers` lives on the per-agent
  binding, never in the bundle manifest, preserving ADR-0010's split between
  versioned gate points and workspace-bound routing.
