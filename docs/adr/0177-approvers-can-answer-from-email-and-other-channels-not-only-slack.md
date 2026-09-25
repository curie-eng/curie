# 177. Approvers can answer from email and other channels, not only Slack

Date: 2026-09-25

Status: Draft

When a bot pauses for a person's approval, today only a Slack click can answer
it, and a request raised in an email thread can never be approved at all: it
waits until it expires. This ADR lets a person approve or reject, with an
optional note, by replying through any channel an adapter serves, such as email.
The adapter proves who replied and the API decides whether that person may
approve, the same way it decides for a Slack click. No frozen contract changes.

This ADR builds on [ADR-0010](0010-approval-gates-and-human-in-the-loop.md),
[ADR-0096](0096-port-adapters-are-deployed-services.md),
[ADR-0106](0106-an-approver-is-an-authenticated-principal.md),
[ADR-0156](0156-adapter-principal-with-a-scoped-credential.md),
[ADR-0166](0166-tenant-boundary-and-principal-identity-land-together.md),
[ADR-0168](0168-one-installation-hosts-several-bot-identities.md) and
[ADR-0175](0175-a-bot-may-limit-who-can-talk-to-it.md), and supersedes nothing.

## Terms

- **Approval**: a paused turn waiting for a person to approve or reject it.
- **Route**: a named level of authority a bundle sends an approval to, such as
  "finance". The operator binds each route to a **resolution target**, the one
  place the request is shown for answering, and optionally a list of
  **approvers**.
- **Routeless approval**: one that names no route. It is shown in the
  conversation that raised it, and in Slack anyone in that channel may answer.
- **Binding**: the link between a bot and one place it listens: a Slack channel,
  its direct messages, or an email inbox.
- **Adapter**: a service outside the platform that brings a channel, such as
  email, in through the channel port. Its **adapter principal** (ADR-0156) is a
  credential scoped to the bindings it serves, and lets it resolve an approval
  on behalf of a sender it authenticated.
- **Served**: an approval is served by an adapter when it belongs to one of that
  adapter's bindings. An adapter sees and resolves only approvals it serves.

## Context

ADR-0156 gave adapters the scoped, verified identity that the Slack-only rule
was waiting for, but three things still block an answer from any other channel:

1. **The resolution target must be Slack.** The schema allows only `slack`, and
   its comment says this holds "until a second adapter can present the scoped
   verified identity". The worker refuses any other kind on its own.
2. **Approvers must be Slack user ids.** No email address can be listed.
3. **Routeless approvals are served by no adapter.** Their approvers are "members
   of the channel", which only a Slack click can prove. So a routeless approval
   raised in an email thread reaches the requester as plain text, with the
   Approve and Reject intent dropped, and can only expire.

There is also a gap today: an adapter principal that serves a Slack binding can
resolve a Slack route by naming any listed Slack user id, and its tests do
exactly that. Under ADR-0106 only the Slack dispatcher vouches for Slack ids.

Email also has hazards Slack does not: scanners open every link, replies quote
the thread, out-of-office replies arrive unasked, and mail is forwarded.

## Decision

**A person may answer an approval by replying through any channel an adapter
serves. The adapter vouches for who replied; the API decides with the route's
approvers, exactly as for a Slack click.**

### 1. A resolution target may be any channel the bot is bound to

`resolution.kind` may name any channel kind. For a kind other than Slack:

- `(kind, address)` must be one of the agent's own bindings, compared on the
  full route including its identity (ADR-0168), or the write is refused (422).
  The worker sends the request through that binding's own transport.
- The route must list at least one approver of that kind (422 otherwise),
  because there is no channel membership to fall back to.

Slack targets behave exactly as today.

### 2. Approver entries say which channel they belong to

A bare entry stays a Slack user id, so every stored route reads as it does
today. Any other entry is written `kind:id`, such as `email:alice@example.com`,
and checked by its kind as ADR-0175 checks caller lists: for email one bare
address, stored lowercase; for any other kind, non-empty with no spaces. Entries
are exact, with no domains or wildcards.

### 3. An adapter vouches only for people in its own channel

When an adapter resolves, the API judges its sender as `kind:sender`, where
`kind` is the kind of the binding the approval is served through. That never
matches a bare Slack entry, so only the Slack dispatcher vouches for Slack ids,
as ADR-0106 intends. This closes the gap above. Slack user groups and channel
membership stay answerable only by a Slack click.

### 4. A routeless approval answers to the person who asked

A routeless approval raised on a non-Slack binding is served by the adapter that
holds that binding, found from the reply channel stored on the approval. Its
only approver is the requester, as the adapter authenticated them when the
message came in: in a one-to-one conversation the requester is the only member
anyone verified, and ADR-0106 already lets an authorized requester confirm their
own action. People copied on the thread cannot answer; an operator who wants
them to declares a route. Routeless Slack approvals are unchanged.

### 5. What the request and the reply look like

**The request.** The worker already sends each approval card as a channel-neutral
Approve and Reject intent carrying the approval id, with a note allowed. A
routeless request goes into the conversation that raised it. A routed request
belongs to no conversation, so the adapter asks the API whom to send it to
through a new read, open only to an adapter principal for an approval it serves,
which returns the route's approvers of the adapter's kind. The adapter sends one
message per person, so approvers never see each other's addresses. The message
says: reply with APPROVE or REJECT on the first line; anything after it becomes
your note.

**The reference.** Each message carries a random, single-use reference that the
adapter generates and records against the approval and the recipient. It links a
reply to its request. It is never proof of identity, because every reply quotes
it and every forward carries it.

**The reply.** The adapter treats a message as a decision only when all of these
hold:

- it passes the adapter's normal sender checks (SPF, DKIM and DMARC for email);
- it carries a live reference issued to this same sender;
- it was not sent automatically (an auto-reply, out-of-office or bounce);
- the first line of the new text, above any quote, is one decision word.

The adapter then calls resolve with its credential, the sender as the actor,
and the decision and note. After any final answer (recorded, not an approver,
not found, already resolved, expired) it spends the reference and tells the
sender the outcome in one line. A reply to a pending request that is not a
decision gets the instructions back. An answer is never a turn (ADR-0106), so
the binding's caller list (ADR-0175) does not apply; the approver list decides.

### 6. Protections

A forged sender is stopped by the adapter's sender checks, which ADR-0156 names
as the trust boundary; the audit row names the adapter and the sender, so a
failure traces to one adapter. A replayed reply gets "already resolved", since
the first answer wins. The reference ties a reply to one approval, and the
served check stops it crossing bindings. No link approves anything, so scanners
and forwards cannot. Approvers are read fresh at resolve time, so someone
removed after the message went out is refused.

### 7. What stays Slack-only

Slack user groups, channel-membership approvers, the card buttons and note
dialog, and the dispatcher as the only voucher for Slack ids. Notifications stay
text-only on every kind. Console and command-line resolution are unchanged.

### 8. No frozen contract changes

The approval record in `packages/aci-protocol` keeps only the card's address;
its kind comes from the route, read fresh, as the served check already does. The
channel port's reply format is unchanged, since recipients come from the API. A
recipients field on the reply target instead would be a wire change needing its
own review.

## Consequences

- Any bot's approvals can be answered by email or another adapter channel,
  without the platform key on the adapter, and routeless approvals in email
  threads, which today can only expire, become answerable by their requester.
- An adapter that relied on vouching for Slack ids stops working, and the
  adapter principal tests move to email. This is intended.
- Each adapter must follow the reply rules above. Until one does, it never
  resolves anything, so nothing becomes weaker.
- Approver entries hold raw provider ids until ADR-0166's identity links (#2914)
  replace them, as ADR-0175's caller lists do.

## Where this lands in the code

1. `apps/api/src/curie_api/schemas.py`: `ApprovalResolutionTarget.kind`,
   `ApprovalApprovers.users` entry checks, and the binding and approver rules in
   decision 1.
2. `apps/api/src/curie_api/crud.py`: `_approval_served` gains the routeless
   case.
3. `apps/api/src/curie_api/slack_approvers.py`, `apps/api/src/curie_api/approvers.py`
   and `apps/api/src/curie_api/authorizer.py`: the requester-only set, and
   matching by kind.
4. `apps/api/src/curie_api/approval_auth.py` and
   `apps/api/src/curie_api/routers/approvals.py`: the served binding's kind
   passed to the authorizer, and the new recipients read.
5. `apps/worker/src/curie_worker/kernel.py`: `POLICY_CARD_KIND` and
   `_parse_approval_targets` accept a bound non-Slack target and send through
   its transport.
6. `apps/mail-adapter/src/curie_mail_adapter/adapter.py`: send requests, keep
   references, read replies.
7. `cli/src/commands.rs` and `cli/src/api.rs`: route validation and the new
   entry form.
8. `docs/interfaces/approval/INTERFACE.md` and
   `docs/interfaces/port-adapter-service/INTERFACE.md`: the reply rules every
   adapter follows.

Tests use real Postgres and Valkey, fake only Slack and the mail provider, and
prove each rule above, including that an adapter naming a Slack id, a copied
person, an auto-reply, a quoted decision and a spent reference are all refused.
End to end, at the tier `AGENTS.md` requires, a listed approver answers a routed
request and a requester answers a routeless one by replying to a real email,
and each bot resumes.

## Acceptance conditions

- Explicit maintainer approval, published as `Status: Accepted`, per
  [`docs/adr/AGENTS.md`](AGENTS.md).
- The maintainer confirms the `kind:id` entry form, the recipients read rather
  than a reply wire field, and requester-only routeless approvals.

## Alternatives considered

1. **Approve and Reject links in the email.** Rejected. Security scanners open
   every link, so requests would approve themselves, and a click proves only
   that someone held the email.
2. **A token from the API in the message as the proof.** Rejected. Every reply
   quotes it and every forward carries it, so holding the email would become
   the authority, which is the caller-asserted identity ADR-0106 removed.
3. **Answer only in Slack or the console, and use email only to notify.**
   Rejected. That is today: routeless email approvals stay unanswerable, and
   approvers who work in email must switch tools.
4. **Put recipients on the channel port's reply target.** Rejected for now. It
   changes a wire that third-party adapters parse strictly, and the API read
   keeps the list where the authority lives and reads it fresh.
5. **Let a routeless approval be answered by the binding's caller list, or by
   anyone on the thread.** Rejected. Who may use a bot is not who may approve
   its actions, and nothing the platform sees authenticates the people copied
   on a thread.
