# 273. Approval routes own expiry and reminders

Date: 2026-11-05

Status: Accepted

Accepted on 2026-11-07 with explicit maintainer approval from Brian Conn,
before implementation. Implementation follows
[ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md) and
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing changes and required evidence below remain required.

This ADR is independent of ADRs 0270, 0271 and 0272. It can be accepted and
shipped first on existing fixed-channel and requesting-surface routes. It
requires neither requester transport, dynamic approvers nor DM delivery.

Extends [ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md)
decision 5 with optional non-interactive reminders on the stored card surface,
and decision 6 with reference retention for the whole pending lifetime.
Extends [ADR 0123](0123-a-pending-approvals-approver-set-does-not-follow-its-route-binding.md)'s
SLA consequence: expiry is an explicit route choice, including no deadline;
its missing-binding refusal still applies and can leave a no-deadline approval
pending until rejection/recovery. These are partial amendments. No decision
about who can answer or where the actionable card lives is superseded, and
no ADR is superseded in full.

Source inspection: `origin/next` at
`678a5665d8fe7f10e78fdb282aee8d9210b37021`, fetched on 2026-11-05.
This is source evidence, not scheduler or delivery qualification.

## Context

The session worker always requests a 24-hour approval duration. The shared
approval request and database already allow a null deadline. Operators cannot
currently choose that duration per route. `_StoredWithoutNulls` drops every
null member when serializing route models, so adding a nullable route field
alone would silently turn an explicit no-deadline choice back into the default.

The API already sweeps pending approvals for expiry and publishes work to the
worker stream. The worker already remembers one card and knows how to send
replies below it. Reminders need a due time and a notification path, not another
approval workflow. A reminder grants nothing and does not need fresh approver
eligibility. The current 14-day card-reference TTL must not erase a still-pending
request's destination.

## Decision

### 1. Persist a route's explicit deadline once

`@spec APPROVAL-SCHEDULE-1`

Add route `expires_in_seconds`: a positive integer selects a duration and
explicit null selects no decision deadline. Reject booleans, zero and negative
values. If the field is omitted, or the approval is routeless, honor the
producer's existing `ApprovalRequest.expires_in_seconds` unchanged, including
null. The session worker still supplies its existing 24-hour default when no
route override is present; other producers retain their own durations.

Both API creation owners derive `expires_at` from authoritative creation time
and the current validated route's explicit override when present, regardless
of a conflicting worker duration. Without an override, use the request's
duration with its existing frozen meaning.
Persist that deadline once. Retrying an identical creation, editing the route
or restarting a worker cannot move it. Publication and remediation approval
purposes retain their separately decided deadline policies.

Preserve omitted versus explicit null through validation, storage, API reads,
CLI writes and read/write round trips. Change `ApprovalRouteBinding`'s serializer
to preserve explicit null for this field based on field presence, while keeping
the existing omission behavior for unrelated optional route fields. An output
model must not synthesize `expires_in_seconds: null` for an omitted input.
Adding `int | None` without these changes does not realize the decision.

The frozen approval request already carries a nullable duration; no new ACI
field is needed. The worker uses an explicit route duration when present and
otherwise retains its producer default; the API independently enforces only
an explicit override. For malformed named routes, refuse rather than substituting
the default. The existing expiry sweeper and late-answer owner retain their
single-winner transition and resume behavior. Null deadlines never expire.

No deadline is a retention choice, not continuing identity authority. Actual
answers and approved resumes still recheck their existing authorization.
A removed binding or inactive actor can strand a no-deadline approval.
Administrative recovery may reject it and cannot approve on another's behalf.

### 2. Claim reminders with one due-time column

`@spec APPROVAL-SCHEDULE-2`

Add route `remind_every_s`: omitted/null disables reminders; a positive integer
enables them. Reject booleans, zero and negative values. At creation persist
its chosen interval and `next_remind_at`, initially creation time plus interval.
No deadline and reminders may be selected independently. Route edits do not
reschedule existing approvals, and existing rows start with reminders disabled.

In the existing sweeper loop, select bounded batches of due, still-pending,
unexpired approvals using database time. Claim each with a conditional update
of `next_remind_at` from its observed value to now plus interval, returning the
claimed row. Competing replicas cannot claim the same due value. After an
outage, schedule from now; never replay every missed interval.

Publish the claimed reminder onto the existing runs stream in a dedicated
`curie_approval_reminder_v1` side-field containing the versioned internal
`ApprovalReminderV1` JSON: approval id and claimed due-time/event identity.
Follow `curie_identity_context_v1`'s precedent for internal metadata outside
the frozen payload. Normal turn entries retain their unchanged `payload`
containing exactly the existing `QueuedTurn` JSON. Reminder entries carry
the reminder side-field and ordinary tracing/delivery metadata, with no
`payload`; never create a fake turn just to pass its parser.

Inside the existing delivery lease, dispatch the reminder side-field before
`from_stream_fields` or any model-turn admission. Strictly validate it and
reject entries mixing reminder and turn payloads. A malformed reminder takes
the existing bounded failure/dead-letter path; it never falls through to turn
execution. The worker reads the stored card and state, not a supplied
destination or message body. Add no frozen `QueuedTurn`/`ApprovalRequest`
field, variant or payload encoding. The frozen stream payload contract is
unchanged; the new notification discriminator lives in `curie-internal`.
There is no outbox, reminder-specific stream, delivery ledger or dead-letter
owner; existing shared delivery limits and fencing apply.

Deploy reminder-capable workers to every consumer in the runs group before
enabling the API sweeper's reminder publications. Old workers parse every
entry as a `QueuedTurn` and would dead-letter these entries; a side-field
does not make them understand reminders. For rollback, stop reminder
publication and drain pending reminder entries before restoring old workers.
No model turn or approval resume is started by notification dispatch.

The database claim and stream publish are not an atomic cross-store operation.
A crash or failed publish after claim may lose that interval's reminder; the
persisted next due time permits a later interval. Record the failure and do not
promise exactly-once delivery or introduce an outbox to repair this accepted
notification gap. A stream redelivery can also duplicate a non-interactive
message after an uncertain send. It cannot duplicate an approval or effect.

### 3. Reply under the one existing card without eligibility lookup

`@spec APPROVAL-SCHEDULE-3`

The worker loads current pending status and the existing card reference before
sending. Skip terminal or expired approvals and missing card references; a
missing destination is visible operationally and may be tried at a later
interval, but never rerouted or repaired by posting another actionable card.
Use a non-interactive reply under the stored card, through its stored transport
identity and endpoint. For email, use that existing thread's reply transport.
If ADR 0272 is installed, the same rule naturally targets its one DM card.

The reminder refers to the pending decision and existing stored description.
It contains no Approve/Reject intent, email answer reference, replacement
arguments or bearer link, and names no newly selected recipient. Use existing
`ApprovalCardRef.ts` only to place a reply below the card. Do not recheck
canonical membership, exemption or eligibility for a notification that grants
nothing. Ordinary transport/credential validity still applies.

Stop scheduling on approval, rejection, expiry or administrative recovery;
terminal status makes due rows ineligible and terminal writers clear
`next_remind_at`. Check status again before delivery. A decision racing an
already issued provider send may leave one final non-interactive reminder;
no platform transaction can retract it. Actual answers retain all their
existing identity checks, independently of reminder delivery.

For an email-thread resolution card, replying to a reminder can answer the
original approval only through
[ADR 0267](0267-an-email-reply-answers-an-approval-under-the-http-approver-role.md)'s
existing email answer path: the reply must include the original card's live
email approval reference, use the existing decision syntax, and receive a
fresh signed email answer proof and current authority checks. Quoted original
card text may supply that reference; the reminder itself issues none. A reply
without a matching live reference gets the existing instructions and does
not decide the approval. A fixed-channel or approver-DM resolution cannot be
answered by replying to an email notification/reminder. This preserves
ADR 0267's original answer surface and adds no reminder answer authority.

### 4. Keep pending references until settlement

`@spec APPROVAL-SCHEDULE-4`

Use the existing single `ApprovalCardStore`. Card, notice and reply-placement
references have no TTL while pending, including finite approvals that have not
yet reached a terminal state. Enable TTL-free pending writes only after every
card writer and settlement consumer supports this retention contract. After the
last old worker drains, reconcile still-present references against current
approval state: remove TTLs for pending approvals and apply bounded terminal
retention to settled approvals. Rollback must retain compatible reference owners
until pending approvals settle. Do not claim expired/lost references can be
reconstructed.

At terminal settlement, start a new 14-day TTL on surviving references before
attempting card delivery; the current store instead starts its 14-day TTL at
registration. Successful settlement can consume the exact ref immediately;
failed card edits retain it within that bounded window. Preserve the
late-registration terminal check. No new delivery collection or seal type is
required. This retention primitive also serves ADR 0272, which depends on this
decision. This ADR is its sole owner; the DM implementation reuses it without
duplicating the contract or migration.

The reminder interval and `next_remind_at` are notification scheduling data,
outside the authorization digest. Scheduling updates do not invalidate HTTP
approval seals. The authoritative decision deadline and status remain covered
by the existing digest and change through the sealed expiry and answer owners.

## Realizing code paths and required evidence

The named proposed owners are:

1. `apps/api/src/curie_api/schemas/approvals.py::_StoredWithoutNulls`,
   `ApprovalRouteBinding` and `ApprovalRouteBindingOut`,
   `routers/agents.py::update_agent` and
   `crud/agents.py::update_agent_approval_routes` preserve field presence/null.
   `cli/src/api.rs` and `cli/src/commands/approvals.rs::approvals` preserve it
   through operator JSON configuration/readback. Regenerate OpenAPI/client
   schemas and command manifests as applicable, preserving console parity.
2. New `apps/api/src/curie_api/approval_schedule.py::resolve_route_schedule`
   serves `routers/approvals.py::create_approval`,
   `identity/http_approval.py::create_http_approval` and
   `crud/approvals.py::insert_approval`. `models.py::Approval` and a new
   Alembic migration add the persisted interval and indexed `next_remind_at`;
   allocate the migration number at implementation. Neither reminder field
   changes `identity/http_approval.py::_RECORD_FIELDS`.
3. New `apps/api/src/curie_api/crud/approvals.py::claim_due_reminders` and
   `sweeper.py::sweep_due_reminders` run beside `sweep_expired_approvals` in
   `run_expiry_sweeper`, wired by `main.py::lifespan`. Existing
   `identity/http_approval.py::expire_http_approval`,
   `crud/approvals.py::claim_approval_resolution`, `expire_approval` and
   `recover_approval_atomic` clear the cursor on terminal transitions.
4. New `packages/curie-internal/src/curie_internal/approval_reminder.py::ApprovalReminderV1`
   defines only the internal `curie_approval_reminder_v1` side-field. New
   `apps/api/src/curie_api/approval_reminders.py::enqueue_reminder` publishes
   it without a turn payload to the existing runs stream, enabled only after
   all consumers support it. `apps/worker/src/curie_worker/consumer.py::Consumer`
   dispatches it inside existing `stream_consumer.py::StreamConsumer`
   delivery-lease/bounded handling before `from_stream_fields` or kernel turn
   start, following the internal identity side-field pattern. No frozen ACI
   schema or normal turn payload changes.
   New `apps/worker/src/curie_worker/approval_reminders.py::deliver_reminder`
   reads approval/card state and calls the existing reply sink. This needs no
   new model-turn authority or reminder-specific dead-letter implementation.
5. `apps/worker/src/curie_worker/kernel/approval.py::_pause_for_approval` uses
   an explicit route duration when present and otherwise keeps
   `kernel/constants.py::_SESSION_APPROVAL_EXPIRES_IN_SECONDS`.
   `approval_cards.py::ApprovalCardStore` and
   `kernel/approval.py::_settle_remembered_card`/
   `_settle_if_decided_before_registration` own retention and settlement.
   `reply_sink.py::ReplySinkRouter` and `slack_sink.py` deliver the reply.
   `packages/channel-gateway/src/curie_channel_gateway/mail/adapter.py::MailAdapter._handle_approval_reply`
   retains its original-reference matching; reminders add no new answer parser.

At implementation record exact commands, candidate commit and outcomes:

1. Through operator API and CLI round trips, prove omitted, positive and null
   deadlines remain distinct; reject invalid numbers/booleans and prove unrelated
   null fields keep their existing omission behavior. Create through legacy
   and HTTP owners and verify actual deadlines, including a mismatched worker
   duration when an override exists. On omitted fields and routeless creation,
   preserve each producer's finite or null duration; the session default stays
   24 hours. Dedupe and later route edits do not change stored schedules.
2. With real Postgres/Valkey and controlled time, run the actual sweeper and
   worker notification path. Observe repeated reminders, competing replica
   claims, restart behavior, null-deadline reminders and stopping for each
   terminal outcome. No model turn or approval resume is started.
   Execute reminder side-field dispatch before the unchanged turn parser,
   normal identity-bearing turn consumption, and malformed/mixed-entry
   refusal under the shared delivery cap. Verify the consumer-first rollout
   and publication-stop/drain rollback sequence against the actual runs group.
3. Fail publish after a successful claim and observe the documented lost
   interval followed by a later due interval. Exercise send/decision races
   without claiming exactly-once delivery. No eligibility lookup is made for
   reminders, while a real answer still requires its normal checks.
4. Deliver through a real fixed-channel card and existing requesting-surface
   transport. Verify reminders are replies below that card, contain no
   interaction, and use the stored identity, including after route edits.
   Reply to an email reminder with the original live reference and a signed
   answer proof: an eligible decision resolves the original email-card
   approval. Missing references receive instructions; forged proofs, an
   ineligible sender or an email answer to a non-email card refuse.
5. Advance logical time beyond 14 days for an unexpired pending card, restart
   readers, then answer/settle and verify bounded terminal retention. Exercise
   no-deadline missing-route refusal and ordinary administrative rejection.
   Run applicable external transport qualification; this independent change
   does not require ADR 0270's requester/live-model connector acceptance.

No runtime acceptance evidence is claimed.

## Consequences

1. Operators can choose route-specific finite or indefinite waiting without
   changing who may approve. Explicit null survives every configuration writer.
2. Reminders add a due cursor and best-effort notifications to existing owners.
   A lost interval is an accepted consequence of avoiding a cross-store outbox.
3. No-deadline pending approvals retain their single card reference indefinitely;
   they may require administrative rejection when policy makes them unresolvable.
4. The work can ship before requester attribution or dynamic approvers and
   later reuse the same reminder path for a selected-recipient DM.

## Alternatives considered

1. Keep only the global 24-hour worker constant. Rejected because it cannot
   express route-specific timing or an explicit no-deadline choice.
2. Add a nullable field without changing serialization/readback. Rejected
   because existing null-dropping writers would silently restore the default.
3. Add an outbox and recipient eligibility checks. Rejected because reminders
   grant no authority and an occasional lost/duplicate notification is accepted.
4. Ask a model to schedule reminders or post another actionable card each time.
   Rejected because timing belongs to the sweeper and the existing card suffices.

## ADR 0045 back-links on acceptance

Keep Accepted statuses and bodies. Add partial-amendment back-links after status:

1. ADR 0177 decisions 5 and 6: ADR 0273 adds non-interactive reminder replies
   and pending-lifetime reference retention, with bounded terminal retention.
2. ADR 0123's SLA consequence: ADR 0273 makes the route deadline explicit,
   including none, while preserving missing-binding refusal and recovery by rejection.
