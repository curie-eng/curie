# 177. An approval is answered where it was asked, including by email

Date: 2026-09-25

Status: Accepted

Accepted with explicit maintainer approval from Brian Conn in the review of
[#3252](https://github.com/curie-eng/curie/pull/3252), the pull request that
published this status, before implementation.

**Amended 2026-10-01** by the [amendment](#amendment-email-approver-lists) at the end, at the request of Brian Conn, the maintainer, in the [review of #3584](https://github.com/curie-eng/curie/pull/3584#pullrequestreview-5378690059), who asked for a small change to this ADR instead of a separate one. It replaces decision 3, the requester-only set in decision 4, the last sentence of decision 7 and rejected alternative 2. The text between this note and the amendment is unchanged. This edit goes beyond the status line and back-link that [ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md) allows, because the maintainer asked for it. The amendment is accepted with his explicit approval on merge of [#3584](https://github.com/curie-eng/curie/pull/3584), and is realized by [#3585](https://github.com/curie-eng/curie/pull/3585) (platform) and [#3450](https://github.com/curie-eng/curie/pull/3450) (mail adapter).

When a bot pauses for a person's approval, it can only be answered by a click
in Slack. A request raised in an email thread therefore can never be approved:
it waits until it expires. This ADR lets an approval raised on any channel an
adapter serves, such as email, be answered on that same channel, by replying
with approve or reject and an optional note. It does not let a request shown in
one place be answered from another, and no frozen contract changes.

This ADR builds on [ADR-0010](0010-approval-gates-and-human-in-the-loop.md),
[ADR-0106](0106-an-approver-is-an-authenticated-principal.md),
[ADR-0156](0156-adapter-principal-with-a-scoped-credential.md) and
[ADR-0166](0166-tenant-boundary-and-principal-identity-land-together.md), and
supersedes nothing.

## Terms

- **Approval**: a paused turn waiting for a person to approve or reject it.
- **Card**: the message that shows an approval and takes the answer. In Slack it
  has Approve and Reject buttons.
- **Route**: a named level of authority a bundle sends an approval to, such as
  "finance". The operator binds each route to a place the card is shown and,
  optionally, a list of **approvers**.
- **Routeless approval**: one that names no route. Its card is shown in the
  conversation that asked.
- **Adapter**: a service outside the platform that brings a channel, such as
  email, in through the channel port. Its **adapter principal** (ADR-0156) is a
  credential scoped to the places it serves, which lets it resolve an approval
  on behalf of a sender it authenticated.

## Context

Where a card is shown today:

- A routeless approval's card goes into the conversation that asked, on
  whatever channel that is. In Slack, anyone in that channel may answer it.
- A routed approval's card goes to the Slack channel the operator bound. The
  conversation that asked gets only a text notice. A route cannot be bound
  anywhere except Slack.

For an email thread this fails in three places. The mail adapter sends the
card's text but drops its Approve and Reject intent, so there is nothing to
answer. The API serves an adapter only routed approvals, so it can never see a
routeless one. And the approvers of a routeless approval are "members of the
channel", which only a Slack click can prove. Routed gates are no better: their
card can only go to Slack.

Approver lists hold Slack user ids, and there is not yet one identity that links
a person's Slack id to their email address. ADR-0166 plans that (principals and
identity links, #2910). Until it lands, nothing can say that an email sender is a
listed approver.

## Decision

**An approval is answered where its card is shown, and nowhere else. The card is
shown where the request was asked, unless an operator routes it to a fixed Slack
channel, which works as it does today.**

### 1. A route may say "answer where it was asked"

A route's card target may be either a fixed Slack channel, as today, or
`{"mode": "requesting_surface"}`: show the card in the conversation that asked,
on whatever channel that is. Anything else, or a mix of both forms, is refused.
The mode cannot carry a separate notification, since there is nothing to notify
beyond the thread that asked. A fixed target stays Slack-only. Routeless
approvals already behave like the mode.

### 2. Slack behaves as today

A card shown in Slack is answered in Slack, by the same people as today:
channel members, a user group, or the route's listed approvers.

### 3. On any other channel, only the person who asked may answer, for now

A card shown in an email thread, or on another adapter's channel, is answered by
the requester alone: the sender the adapter authenticated when the request came
in. In a one-to-one conversation that person is the only member anyone
verified, and ADR-0106 already lets an authorized requester confirm their own
action. There, an approval is a confirmation step, not a second person's
sign-off. People copied on the thread cannot answer.

**This is an interim.** A route that lists approvers and resolves to a non-Slack
channel is escalated when the approval is raised, with a message saying why,
instead of creating an approval nobody there can answer. When approvers become
principals linked to every channel identity (ADR-0166, #2910), approver lists
apply on every channel and this rule is replaced. This ADR adds no new way to
write an approver down.

### 4. The adapter carries the answer

- The API serves an adapter an approval whose card went to one of that adapter's
  own bindings, routed or not.
- A new approver set, "the requester only", is used for those approvals. It
  admits the serving adapter's sender when it equals the approval's author, and
  admits no operator, console or Slack principal, since none of them can prove
  they are that person.

### 5. What the request and the reply look like

**The request.** Nothing changes on the wire. The worker already sends the card
into the thread as a channel-neutral Approve and Reject intent carrying the
approval id, with a note allowed. The adapter renders it for its channel. For
email: "Reply with APPROVE or REJECT on the first line. Anything after it is
your note." The message carries a random single-use reference the adapter
generates and keeps, linking a reply to this approval. The reference is not
proof of identity, because every reply quotes it.

**The reply.** The adapter treats a message as an answer only when all of these
hold:

- it passes the adapter's normal sender checks (SPF, DKIM and DMARC for email);
- it carries a live reference for this approval;
- it was not sent automatically (an auto-reply, out-of-office or bounce);
- the first line of the new text, above any quote, is one decision word.

The adapter then calls resolve with its credential, the sender as the actor, and
the decision and note. The API decides. A reply in a pending thread that is not
an answer gets the instructions back. An answer never starts a turn (ADR-0106).

### 6. What happens to the card when the approval ends

There is one card per approval, so no second card is left behind. Whatever
ends the approval, the resume settles that card, best effort:

- **Slack.** The card is edited in place: the buttons go, and it shows the
  decision, who made it and the note. That happens the same way whether the
  answer was a click, the console or the command line. On expiry it shows
  "Approval expired" with no buttons. If an edit fails, a later click gets
  "already resolved by" whoever answered.
- **Email.** A sent email cannot be edited, so the adapter sends one short
  follow-up in the thread saying approved, rejected or expired, and spends the
  reference. A later reply gets "already resolved".

A routed approval's text notice in the asking thread carries no buttons, so
there is nothing to remove there.

### 7. Protections

A forged sender is stopped by the adapter's sender checks, which ADR-0156 names
as the trust boundary; the audit row names both the adapter and the sender. A
replayed reply loses, because the first answer wins. There are no links in the
message, so a mail scanner that opens every link approves nothing. A forwarded
message does not help anyone else answer, because only the requester's address
is accepted.

## Consequences

- Any bot can have routeless approvals, and routes in the new mode, answered by
  email or another adapter channel, without the platform key on the adapter.
- On those channels only the requester may answer until approvers are linked
  identities. A bot that needs a second person's sign-off keeps a fixed Slack
  route.
- Each adapter must render the intent and follow the reply rules. Until one
  does, its approvals still only expire, as today.
- No frozen contract changes. The approval record already stores the asking
  channel's kind and address, which is all the served check needs.

## A separate finding: fix on its own

An adapter principal that serves a Slack binding can resolve a Slack route today
by naming any listed Slack user id, and its tests do exactly that
(`apps/api/tests/test_adapter_principal.py`). Under ADR-0106 only the Slack
dispatcher vouches for Slack ids. This ADR does not use that path. It should be
closed as its own security fix, refusing an adapter principal on Slack approver
sets, without waiting for this ADR.

## Where this lands in the code

1. `apps/api/src/curie_api/schemas.py`: the resolution target union.
2. `apps/api/src/curie_api/crud.py`: `_approval_served` serves the asking
   binding.
3. `apps/api/src/curie_api/slack_approvers.py` and
   `apps/api/src/curie_api/approvers.py`: the requester-only set.
4. `apps/worker/src/curie_worker/kernel.py`: `_parse_approval_targets` accepts
   the mode, and the escalation in decision 3.
5. `apps/mail-adapter/src/curie_mail_adapter/`: render the intent, keep
   references, read replies, send the follow-up.
6. `cli/src/commands.rs` and `cli/src/api.rs`: route validation.
7. `docs/interfaces/approval/INTERFACE.md` and
   `docs/interfaces/port-adapter-service/INTERFACE.md`: the reply rules.

Tests use real Postgres and Valkey, fake only Slack and the mail provider, and
prove each rule, including that a copied person, an auto-reply, a quoted answer
and a spent reference are refused. End to end, at the tier `AGENTS.md` requires,
a requester approves by replying to a real email and the bot resumes.

## Acceptance conditions

- Explicit maintainer approval, published as `Status: Accepted`, per
  [`docs/adr/AGENTS.md`](AGENTS.md).
- The maintainer confirms the requester-only interim and the new route mode.

## Alternatives considered

1. **Answer from any channel, whatever channel shows the card.** Rejected. It
   needs one identity across channels, which does not exist yet, and a card
   could be answered where nobody can see it settle.
2. **Write email addresses into approver lists now.** Rejected. It invents a
   second identifier scheme that ADR-0166's identity links would have to
   replace.
3. **Approve and Reject links in the email.** Rejected. Mail scanners open every
   link, and a click proves only that someone held the email.
4. **Only routeless approvals, no new route mode.** Rejected. A gate that names
   a route could then never be answered by email.

## Amendment: email approver lists

Added 2026-10-01. An approval shown in an email thread is answered only by an address the route lists.

- **A1. A route may list approver emails.** `approvers.emails` holds exact, bare addresses (no display name, wildcard or domain), stored and compared lowercase. It is allowed only on a `requesting_surface` route, and may sit beside `users` or `group`: a Slack card reads only the Slack entries, an email card reads only `emails`. An empty list is refused when written. This reverses rejected alternative 2; the identity links of ADR-0166 (#2910) will absorb the list when they land.
- **A2. Who may answer.** A reply counts only when all of these hold, checked in this order: (1) it passes the adapter's inbound gate (SPF, DKIM and DMARC); (2) the binding's `allowed_callers` admit its sender; (3) it follows the reply rules of decision 5; (4) the sender's address is on `emails`. Only the adapter that serves the thread's binding can carry the answer; no operator, console or Slack principal can answer an email card. The inbound gate proves the sending domain, not the mailbox, and that limitation is accepted.
- **A3. No list, nobody.** The requester-only default is retired. An email card whose route lists no addresses, including a routeless approval, admits nobody, so the worker escalates it when it is raised instead of letting it expire. Any other non-Slack channel is treated the same way.
- **A4. Slack is unchanged.** An address never answers a Slack card, and a route that lists only `emails` admits nobody on Slack.
- **A5. The requester adds approvers.** The request email names who can approve and says which of them are already on the thread. If none is, the requester replies all and adds any number of listed approvers; the bot never emails an approver who is not on the thread. The first answer decides and is final, and later answers are told it was already answered. A listed requester may approve their own request, as on Slack under [ADR-0106](0106-an-approver-is-an-authenticated-principal.md). The outcome always reaches the requester: it is sent reply all to the winning answer, and directly to the requester when they are not on it.
