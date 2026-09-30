# 183. An email approval is answered only by a listed address

Date: 2026-09-29

Status: Draft

Today, when a bot asks for approval in an email thread, the only person who can
answer is the person who asked. This ADR replaces that with a list: a route
names the email addresses that may approve, the same way a Slack route names
the Slack users who may approve. A reply counts only when it passes the mail adapter's inbound gate and its
sender address is on the list. The gate checks provider verdicts; it does not
prove control of the named mailbox. With no list, nobody can
approve by email, and the bot flags the request for a human instead of waiting.

This ADR amends [ADR-0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md):
it replaces decision 3 ("only the person who asked may answer, for now"), the
requester-only set in decision 4, the last sentence of decision 7, and reverses
its rejected alternative 2 ("Write email addresses into approver lists now").
Every other part of ADR-0177 stands: an approval is still answered where its
card is shown, the route mode, the reply rules and the follow-up are unchanged.
It builds on [ADR-0106](0106-an-approver-is-an-authenticated-principal.md),
[ADR-0156](0156-adapter-principal-with-a-scoped-credential.md) and
[ADR-0175](0175-a-bot-may-limit-who-can-talk-to-it.md).

## Terms

- **Approver list**: the `approvers` block on a route (ADR-0034, #420). Today it
  holds Slack user ids (`users`) or a Slack user group (`group`).
- **Inbound allowlist**: who may talk to the bot at all. That is the binding's
  `allowed_callers` (ADR-0175), and the mail adapter's own allowed senders.
- **Verified sender**: shorthand here for the bare address carried by a message
  that passed the mail adapter's inbound gate: the provider's SPF, DKIM and DMARC
  verdict, or the equivalent check a mail adapter documents as its inbound gate.
  These verdicts authenticate the sending domain, not control of the named
  mailbox. Curie performs no additional sender authentication. This ADR adds no
  new check; accepting that limitation is an explicit acceptance condition.

## Context

ADR-0177 made email approvals answerable by the requester alone, as an interim
until approvers become principals linked to every channel identity (ADR-0166,
#2910). It rejected writing email addresses into approver lists because that
would be a second identifier scheme for the identity links to replace.

The maintainer has since ruled on what an email approval needs:

> The user must be verified to be that email (the sender must be who they say
> they are) and that email address (not the Slack address) must be in the
> approved list of emails.

The product owner confirmed that the approver list for email is separate from
the inbound allowlist, exactly as a Slack route's approvers are separate from
who may talk to the bot in that channel. Being allowed to talk to a bot is not
the same as being allowed to approve what it does.

The requester-only rule does not meet the ruling. The requester's message passes
the inbound gate, but nobody wrote their address down as an approver.

## Decision

**On an email thread, an approval is answered only by a verified sender whose
address is on the route's approver email list.**

### 1. A route may list approver email addresses

A route's `approvers` block may carry `emails`, a list of exact, bare email
addresses:

```json
{
  "resolution": {"mode": "requesting_surface"},
  "approvers": {"emails": ["approver@example.com", "second.approver@example.com"]}
}
```

Each entry is one address, with no display name, no wildcard and no whole
domain, the same shape `allowed_callers` accepts for an email binding. Entries
are stored lowercase and compared lowercase. An empty list is refused when it
is written, because it can only mean "nobody", and a list that is somehow empty
when read admits nobody.

`emails` is allowed only on a route in `requesting_surface` mode, since only
that mode shows a card in an email thread. A route may carry `emails` beside
`users` or `group`: a card shown in Slack reads only the Slack entries, and a
card shown in email reads only `emails`.

### 2. The order of checks

A reply in an email thread with a pending approval goes through these checks,
in this order, and stops at the first that fails:

1. **The inbound gate.** The adapter checks the provider's verdicts, exactly as
   for any message to the bot. This does not prove control of the named mailbox.
   A message that fails is dropped before anything else.
2. **The inbound allowlist.** A sender the mailbox does not admit is refused
   before any approval logic runs. The adapter applies its own list, and the
   platform applies the binding's `allowed_callers` again when the answer
   arrives, so an answer cannot reach a bot its sender could not write to.
3. **The reply rules of ADR-0177 decision 5.** A live reference for this
   approval, not sent automatically, a decision word on the first line.
4. **The approver email list.** The adapter passes the verified sender, never a
   display name, as the actor. The platform admits it only when that address is
   on the route's `emails`, and only from an adapter principal that serves the
   thread's binding. No operator, console or Slack principal can answer an
   email card, whatever address it names.

Anyone on the list may answer, not only the person who asked. The person who
asked can answer only if their own address is on the list.

### 3. No list, no email answer

The requester-only set is retired rather than kept as a default. An approval
shown in an email thread whose route lists no approver emails, including a
routeless approval, has nobody who may answer it. The worker escalates it when
it is raised, with a message saying why, instead of creating an approval that
can only expire, as ADR-0177 decision 3 already does for a route whose
approvers cannot be verified there. An approval already pending under the old
rule admits nobody and expires.

A bot that wants the requester to confirm their own action lists the
requester's address. Any other non-Slack channel has no approver list it can
verify yet, so it is treated the same way: escalated when raised, answered by
nobody.

### 4. Slack is unchanged

A card shown in Slack is answered by the same people as today. An email address
never satisfies a Slack card, and an adapter principal is still refused on
every Slack approver set (ADR-0177, "A separate finding"). A Slack card whose
route lists only `emails` has no Slack approvers, so it admits nobody rather
than falling back to channel membership.

## Consequences

- An email approval now needs a second, deliberate operator step: listing who
  may approve. The inbound allowlist alone never grants it.
- A copied or forwarded approver on the list can answer, which ADR-0177's
  decision 7 ruled out. That is the point of a list, and the audit row records
  the adapter and the verified address that answered.
- Nothing already answered by email changes: the mail adapter's approval half
  has not shipped. A bot that relied on routeless approvals in email threads
  must name a route with an approver list.
- The list is an identifier scheme the identity links of ADR-0166 (#2910) will
  have to absorb. When they land, a later ADR can map each listed address to a
  principal; the rule "verified, then listed" stays the same.
- DMARC and SPF verify the sending domain, not the mailbox. Anyone who can send
  authenticated mail for a listed address's domain, and has seen the thread's
  reference, can answer as that address. This is the same trust the bot already
  places in that sender when they talk to it, and it is recorded here rather
  than solved here.

## Where this lands in the code

1. `apps/api/src/curie_api/schemas.py`: `ApprovalApprovers.emails`, and the
   rule that `emails` needs a `requesting_surface` route.
2. `apps/api/src/curie_api/approvers.py` and
   `apps/api/src/curie_api/slack_approvers.py`: an email approver set in place
   of `RequesterOnly`.
3. `apps/api/src/curie_api/routers/approvals.py`: the binding's
   `allowed_callers` checked for an adapter's answer before the approver set.
4. `apps/worker/src/curie_worker/kernel.py`: the raise-time escalation.
5. `apps/mail-adapter/src/curie_mail_adapter/adapter.py`: pass the verified
   sender; drop the requester-only filter.
6. `cli/src/api.rs` and `cli/src/commands.rs`: route validation.
7. `docs/approvals.md`, `docs/interfaces/approval/INTERFACE.md` and
   `docs/interfaces/port-adapter-service/INTERFACE.md`.

Tests use real Postgres and Valkey and fake only the mail provider. They prove
that a listed verified sender is admitted, and that each of these is refused:
an unlisted sender the inbox admits, a listed address the inbound gate did not
verify, an empty list, a sender outside `allowed_callers`, and an adapter
principal on a Slack approver set.

## Acceptance conditions

- Explicit maintainer approval, published as `Status: Accepted`, per
  [`docs/adr/AGENTS.md`](AGENTS.md).
- The maintainer confirms that the requester-only rule is retired rather than
  kept as the default when no list is given.
- The maintainer explicitly accepts the domain-authentication limitation above
  for approval answers, or requires stronger mailbox verification in a revised
  Draft before acceptance. The quoted ruling alone does not decide this tradeoff.

## Alternatives considered

1. **Keep "the requester only" as the default when a route lists no emails.**
   Rejected. The ruling requires the address to be on a list, and a default
   that admits someone nobody listed is exactly what the ruling closes. Listing
   the requester's address gives the same result on purpose.
2. **Use the inbound allowlist as the approver list.** Rejected. Who may talk
   to a bot and who may approve its actions are different questions, and Slack
   already keeps them apart.
3. **Require the approver to be both the requester and listed.** Rejected. It
   would make the list a filter on one person, not a set of approvers, unlike
   every Slack approver set.
4. **Wait for identity links (ADR-0166, #2910).** Rejected by the ruling:
   email approvals are needed before that lands. Whether the current inbound
   gate provides enough identity assurance remains an explicit acceptance question.
