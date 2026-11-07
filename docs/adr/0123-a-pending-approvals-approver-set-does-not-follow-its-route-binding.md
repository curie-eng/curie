# 123. A pending approval's approver set does not follow its route binding

Date: 2026-08-25

Status: Accepted

**Partially amended by [ADR 0273](0273-approval-routes-own-expiry-and-reminders.md)**
(back link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
ADR 0273 makes the route deadline an explicit choice, including none, while
preserving missing-binding refusal and recovery by rejection.

**Partially amended by [ADR 0271](0271-per-request-approver-sets-and-separation-of-duties.md)**
(back link added under [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)):
ADR 0271 amends the no-snapshot clause for per-request selection. It stores
selected people for dynamic routes while preserving current authority and
missing-route refusal.

Raised by [#1081](https://github.com/curie-eng/curie/issues/1081).

Supersedes the one consequence [ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md)
accepted on the strength of a rationale that does not hold for every approver set,
and extends [ADR 0046](0046-converged-approval-gates-and-durable-provenance.md) and
the #544 decision it records from approval CREATION time to approval RESOLVE time.

## The escalation, in three steps

Nothing below needs an attacker, a race, or an unusual configuration. It is what
the shipped code does today.

1. An agent has the route `deal_desk` bound to
   `{"channel": "C0MANAGERS", "approvers": {"group": "S0FINANCE"}}`. A gated action
   raises an approval on that route. Only members of the `S0FINANCE` user group can
   resolve it, and the API proves that membership server side by asking Slack.

2. Someone holding the platform API key, who is **not** in `S0FINANCE`, rewrites
   the agent's route map without that route. A write is a full replacement, so
   `--clear-routes`, a `--routes-from` file that omits the route, and even
   `--route other=C0OTHER` all do it.

3. That same person now resolves the **already-pending** approval by asserting
   `actor_channel: "C0MANAGERS"`.

It succeeds. The approver set for a pending approval is selected fresh at resolve
time, and with no `approvers` block on the binding `SlackApproverSetSelector`
falls back to `SlackChannelMembers(card_channel)`. Self-approval blocking does not
help, because it compares only against `approval.author` and this is a second
identity.

The audit row records `ChannelMembershipAuthorizer`. It does not record that a
`UserGroupAuthorizer` was in force minutes earlier, and it does not record who
removed the binding.

## Why this is not merely policy being dropped

Removing a route binding sounds like it should widen the approver set to the
channel, and for one of the three sets that reading is correct. The other two make
it a change of KIND, not of width.

`SlackUserGroupMembers` and `ExplicitUsers` are server-enforced: the API resolves
membership itself, from Slack or from stored config, and the caller cannot
influence the answer. `SlackChannelMembers` is **caller-asserted** on the axis that
matters, because the channel it checks membership in is the `actor_channel` the
resolver supplied. The API compares the caller's claimed channel against the
approval's own channel and then asks Slack whether the actor is in it.

So step 2 does not loosen a check. It swaps a check the server performs for one
the caller half-supplies. A holder of the platform key can therefore choose which
kind of check applies to an approval that is already waiting for a decision.

## What ADR 0034 actually accepted

ADR 0034 recorded the resolve-time selection as accepted, and its rationale was
that no new escalation path is created: whoever can rewrite a binding can already
act on the agent, so letting a rewrite change the approver set grants nothing they
did not have.

That rationale is sound for the channel-members default and unsound for the other
two sets, which arrived later. A group-bound or list-bound route exists precisely
to name a smaller set than "people with platform access", and #420 added those
sets so an operator could express that. Once they exist, a rewrite is not a
no-op: it converts a route whose approvers were named into a route whose approvers
are whoever is in a channel.

The repository's ADR rule makes an Accepted decision immutable, so this is a
superseding ADR rather than an edit to 0034. Only that one consequence is
superseded; 0034's decision that membership resolves in the API, and its AC4
zero-setup default, both stand.

#544 already took the opposite position at the other end of the lifecycle. Its
Decision B has the kernel escalate and create **no approval at all** for a route
the manifest named but nobody bound. Creating no approval for an unbound named
route while allowing a pending approval on a named route to silently become
channel-bound is one rule applied at creation and abandoned at resolution.

## Decision

**A pending approval that named a route is resolvable only through that route's
binding. If the binding is gone, the approval is not resolvable and no fallback
applies.**

Concretely, `SlackApproverSetSelector` splits the no-approvers case on whether the
approval named a route:

- The approval names a route and the binding is present with no `approvers` block:
  `SlackChannelMembers`, unchanged. This is ADR 0034 AC4's zero-setup default and
  the case its rationale was written for.
- The approval names a route and the binding is **absent**: an approver set that
  refuses every actor and reports its reason, in the shape `InvalidApprovers`
  already uses. Fail closed.
- The approval names **no** route: `SlackChannelMembers`, unchanged. A routeless
  approval never had a narrower set to lose, which is the #420 AC4 default.

The audit row for a refusal under the second case names the missing binding as its
reason, so an operator reading the trail sees why the approval stopped being
resolvable rather than only that somebody was refused.

No snapshot of the approver set is taken at creation time. That is deliberate and
is discussed under the rejected alternatives.

## Consequences

An operator who clears a route map while an approval is pending on it now has a
stuck approval instead of a widened one. The recovery is to restore the binding,
after which the approval resolves normally; it is also the behavior an operator
would predict from #544, which refuses to create an approval for an unbound route
in the first place.

An approval can therefore become unresolvable through operator action. The SLA is
what bounds it: past `expires_at` a resolve attempt flips the record to expired and
enqueues the expiry resume turn, so the suspended session wakes down its timeout
branch rather than waiting forever. An approval with no `expires_at` and a deleted
binding waits indefinitely, which is a real gap and is not created by this decision
-- it is the pre-existing consequence of `expires_at` being optional. It is worth
its own issue rather than a rule smuggled in here.

Revoking an approver still takes effect on the next attempt, because the binding is
still read fresh at resolve time. Nothing here caches authority; the change is only
what an ABSENT binding means.

The route rewrite path itself is untouched. This ADR does not make a rewrite
require anything it does not require today, because the widening it closes is at
the reading end, and a permission model for who may rewrite a route map is a
different decision that ADR 0106's canonical principals have to land first.

## Alternatives rejected

**Snapshot the approver set onto the approval at creation time.** This removes the
resolve-time question entirely and is the first thing anyone proposes. It is
rejected because it breaks something 0034 chose on purpose and #420 depends on: a
revoked approver must lose their authority on the next click, not at the next
restart. A snapshot makes membership as of creation the answer forever, so removing
somebody from a finance group would leave them able to resolve every approval
raised before the removal. Trading a fail-closed gap for a stale-authority gap is
not an improvement, and stale authority is the harder one to notice.

**Refuse the route rewrite while an approval is pending on that route.** Attractive
because it stops the sequence at step 2, but it makes an ordinary operator action
fail for a reason the operator cannot see without listing approvals first, and it
does nothing about a rewrite that races a creation. The reading end is where the
decision is made, so it is where the rule belongs.

**Leave it and rely on the audit trail.** Rejected on the strength of what the trail
actually records: `ChannelMembershipAuthorizer` and nothing about the
`UserGroupAuthorizer` that was in force minutes earlier, nor who removed the
binding. Reconstructing this from the trail requires already suspecting it.

**Treat the missing binding as a routeless approval.** This is the current behavior
restated, and it is precisely the substitution the escalation depends on: it lets a
route the operator narrowed be read as one they never narrowed.

## References

- [#1081](https://github.com/curie-eng/curie/issues/1081), the finding, with the
  reachability analysis and the sequence above.
- [#420](https://github.com/curie-eng/curie/issues/420), which introduced the
  `approvers` block and therefore the two server-enforced sets.
- [#544](https://github.com/curie-eng/curie/issues/544), Decision B, the same rule
  at creation time.
- [ADR 0034](0034-approval-authorizers-resolve-membership-in-the-api.md), whose
  resolve-time consequence this supersedes.
- [ADR 0046](0046-converged-approval-gates-and-durable-provenance.md), which
  refuses an unbound route outright.
- [ADR 0106](0106-an-approver-is-an-authenticated-principal.md), Draft, which
  settles what an actor IS. This ADR settles which SET is asked and does not depend
  on 0106 landing first, but a permission model for route rewrites does.
