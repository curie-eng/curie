# 272. Approver DM delivery uses one selected recipient

Date: 2026-11-05

Status: Accepted

Accepted on 2026-11-07 with explicit maintainer approval from Brian Conn,
before implementation. Implementation follows
[ADR 0085](0085-acceptance-not-implementation-authorizes-an-adr.md) and
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing changes and evidence below remain required.

Depends on [ADR 0271](0271-per-request-approver-sets-and-separation-of-duties.md)
for a verified per-request canonical selection and its authority checks, and
therefore transitively on ADR 0270. It also depends on
[ADR 0273](0273-approval-routes-own-expiry-and-reminders.md) decision 4's
pending-reference retention primitive. That primitive must land first;
route expiry and reminder delivery are not required for DM enablement.

Partially supersedes [ADR 0177](0177-an-approval-is-answered-where-it-was-asked-including-by-email.md)'s
opening location rule and decisions 1 and 2 only to add one selected approver's
DM as a resolution surface. Extends decision 6's existing single-card lifecycle
to that DM; it does not introduce several cards per approval. Amends
[ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
decision 10's card-location clause. Amends
[ADR 0267](0267-an-email-reply-answers-an-approval-under-the-http-approver-role.md)
decisions 1 and 5 for an email-origin request routed to this DM. Existing
email-thread cards and replies remain under their existing policy. These are
partial amendments; no ADR is superseded in full.

Source inspection: `origin/next` at
`678a5665d8fe7f10e78fdb282aee8d9210b37021`, fetched on 2026-11-05.
This is source evidence, not DM delivery qualification.

## Context

The platform already separates a request's reply handle from the card's
resolution channel. It stores one `Approval.card_channel` and one
`ApprovalCardRef` per approval. The HTTP answer proof already identifies the
interaction channel, actor and receiving installation. No message timestamp
is present in `ApprovalAnswerProofV1` or needed for this decision.

One selected canonical person needs one actionable DM card. A collection of
deliveries, fan-out settlement, per-delivery signatures and a new answer-proof
version would add contracts that this use case does not need. The current
14-day card-reference TTL is insufficient for a request still pending longer
than that, including a no-deadline approval.

## Decision

### 1. A dynamic route may choose exactly one approver's DM

`@spec APPROVER-DM-1`

Add resolution `{"mode":"approver_dm","identity":"default"}`. The identity
is operator-selected and names a configured bot installation with a registered
receiver. Enable it only on an ADR 0271 dynamic route. Creation requires exactly
one selected canonical approver with one candidate native user account for
that installation/namespace, verified through the HTTP identity owner.
Zero or several selected recipients refuse. Static routes, group expansion
and multi-recipient delivery are deferred.

Who may answer remains the canonical selected person intersected with every
ADR 0271 ceiling, role and separation check. A DM does not grant membership,
requester invocation authority, or a new agent channel binding. No delivery
failure falls back to the requester's thread or a broad channel.

### 2. Post one card and register the returned DM channel

`@spec APPROVER-DM-2`

Use the existing worker bot transport to call `chat.postMessage` with the
verified recipient's native user id as `channel`. The provider documents this
under [Begin a conversation in a user's App Home](https://docs.slack.dev/reference/methods/chat.postMessage/).
This uses the existing `chat:write` permission; it does not assume
`conversations.open` or a new `im:write` grant. Qualify the actual supported
installation before acceptance.

Settling a DM through the dispatcher's
`approval_actions.py::_fetch_card_message` reads the existing card with
`conversations.replies` and needs `im:history`. Require that scope on the
selected installation and qualify the actual read and outcome edit; posting
with `chat:write` alone does not qualify settlement. Keep the worker's stored
card settlement path and the dispatcher's best-effort stamp behavior.

Creation persists one approval and selected recipient before posting. Its
DM `card_channel` is initially unset. For this mode, unset means delivery is
not registered and every answer refuses; it never falls back to
`reply_channel`. Return the server-verified single recipient and selected bot
identity to the worker in the versioned internal creation response.

After posting, the worker records the actual DM channel returned by the
provider in that approval's existing `card_channel`, through an authenticated
internal card-registration operation. A user id sent as the post target is
not a DM channel id. The operation binds approval id, original creation
context, configured bot/installation and selected recipient. It permits one
channel registration, makes an identical retry idempotent and refuses a
conflicting registration. Model/runner arguments cannot perform this write.

`card_channel` already participates in the HTTP record digest. Its API owner
updates that digest and the existing continuity seal atomically; there is no
new per-delivery seal or delivery-row collection. Preserve the original
creation fingerprint and requester association. Registration racing a terminal
decision returns that status so the worker settles the known card immediately.

Keep the provider message timestamp only in the existing `ApprovalCardRef`
for editing or replying under the card. Do not add it to the answer proof or
use it as answer authority. Unknown send/ack outcomes are delivery failures,
not proof that nothing posted; do not automatically create another actionable
card after an uncertain post. No exactly-once provider delivery is promised.

The original requesting surface receives a non-interactive pending notice
and the eventual outcome through its original reply handle. For email origin,
that notice has no email approval reference; an email reply cannot answer a
card whose resolution surface is the approver DM.

### 3. Match the existing proof's DM channel and selected actor

`@spec APPROVER-DM-3`

Preserve the existing authenticated interaction, receiver proof and exact-body
checks for buttons and note modals. Require interaction channel equal to the
registered DM `card_channel`, receiver/bot installation equal to the selected
identity, and current canonical actor equal to the one selected person.
ADR 0271 checks still apply at answer, final commit and approved resume.
Operator principals are refused, including when the underlying approver-set
kind ordinarily permits them; a CLI operator token is not proof of the
selected canonical person in this DM.

A different DM, installation or actor refuses. A copied card in another channel
or presented by another person supplies no authority. A copy in the same DM
used by the same selected actor is not a separate security boundary and needs
no message-timestamp check. First authorized decision wins the existing CAS.

Classify `approver_dm` as a Slack card even when the invocation originated in
email. Extend target and token-identity selection, preserving the existing
registered-origin restriction for Slack origins and ADR 0267's stored-card
association for email origins. Keep the original binding as invocation origin;
receiving a DM is not permission to invoke the agent there.

Reuse ADR 0273 decision 4 for all pending-reference retention, migration and
terminal settlement behavior. This ADR adds no second retention contract,
delivery ledger or `ApprovalCardStore` lifecycle. Fan-out and reconciliation
of several actionable cards remain deferred.

## Realizing code paths and required evidence

The named proposed owners are:

1. `apps/api/src/curie_api/schemas/approvals.py::ApprovalRouteBinding` and
   `ApprovalRouteBindingOut` gain the strict resolution mode. New
   `ApprovalApproverDmTarget` in that file validates it.
   `routers/agents.py::update_agent` and
   `crud/agents.py::update_agent_approval_routes` validate operator writes.
2. `apps/api/src/curie_api/identity/http_approval.py::_route_snapshot`,
   `create_http_approval`, `_response` and `approval_record_digest` own initial
   DM-pending state and existing continuity. Add `register_approval_card` in
   that module and an internal endpoint in
   `routers/identity_approvals.py`, with a versioned internal registration
   request/response in
   `packages/curie-internal/src/curie_internal/identity_approval.py`.
   This adds no message timestamp to `ApprovalAnswerProofV1` and no new seal type.
3. `apps/api/src/curie_api/identity/http_approval_answer.py::_authenticate_actor`,
   `_serves_origin` and `_route_decision`,
   `approvers.py::card_on_requesting_surface` and
   `slack_approvers.py::approval_token_identity` handle DM classification and
   answer matching. `identity/http_resume.py::_check_resume` retains both
   original-requester and actor rechecks.
4. `apps/worker/src/curie_worker/kernel/approval.py::_parse_approval_targets`,
   `_pause_for_approval`, `_settle_remembered_card` and
   `_settle_if_decided_before_registration`, plus
   `approvals.py::ApprovalClient`, post/register/settle the one card.
   `approval_cards.py::ApprovalCardStore` uses the prerequisite retention
   implementation owned solely by ADR 0273 decision 4.
   `reply_sink.py::ReplySinkRouter` and `slack_sink.py::SlackReplyAdapter` carry the selected bot and
   returned channel through the existing transport.
5. `apps/dispatcher/src/curie_dispatcher/approval_actions.py` keeps the existing
   button/modal answer-proof path. Its integration tests must exercise DM
   channels and `_fetch_card_message`'s `conversations.replies` read using
   `im:history`; no new proof field is assumed. Operator JSON configuration flows
   through `cli/src/commands/approvals.rs::approvals` and `cli/src/api.rs`.
   Regenerate API/internal/client artifacts and manifests where applicable.

At implementation record exact commands, candidate commit and observed outcomes:

1. Reject static DM routes, zero/multiple selected people, wrong-installation
   accounts, unresolved account links and missing receivers through real API
   boundaries. An unregistered pending DM cannot be answered on its origin.
2. With a real bot installation, create one request, receive its DM and verify
   the returned DM channel is the stored `card_channel`. Submit both a button
   and a note modal on separate requests, complete an actual approved effect,
   and observe one-card settlement. These are genuine Slack interactions: the
   dispatcher receives them over Socket Mode and the modal needs Slack's
   short-lived `trigger_id`, so no API call or scripted receiver proof
   substitutes for them. Drive them with an unattended browser driver holding
   authenticated Slack test-user sessions for the selected actor and an
   ineligible actor, and reuse that driver for item 3's live interaction
   controls. If no such session is available or a login challenge appears,
   record this transport qualification as not run and complete it as one
   human-driven check before the release that ships this ADR; every other item
   stays unattended. Repeat with email origin, retaining its original reply
   handle and refusing an email answer to the notice. Prove the actual DM card
   read and outcome edit with `im:history`; a missing scope must be reported as
   failed settlement qualification, not a passing send.
3. Exercise wrong DM/actor/installation and copied-card controls through actual
   interaction authentication, plus concurrent answers. Observe no unauthorized
   effect and one decision. Refuse operator-principal resolution even on an
   otherwise operator-eligible set. Use HTTP identity and scripted receiver
   proofs for unattended policy tests, plus real signed button/modal
   interactions for transport qualification. Source parsing alone is not DM qualification.
4. Reuse ADR 0273 decision 4's retention implementation and evidence, and
   exercise its DM integration beyond 14 days while pending. Cover post/registration
   racing a decision, identical registration retries and conflicting channels.
   After settlement verify bounded retention and successful exact deletion.
5. Run the required live-provider and external-integration tiers for changed
   hosted-connector/DM paths. Keep shared approval-action and principal vectors
   on both API/dispatcher readers. No runtime evidence is claimed.

## Consequences

1. A dynamic approval has one selected recipient, one DM channel and one card
   reference. There is no multi-card storage or fan-out settlement contract.
2. Pending references can outlive any finite deadline, at the cost of storage
   until a terminal decision, under ADR 0273 decision 4. That prerequisite
   owns the migration and bounded terminal retention.
3. The existing receiver proof is sufficient with channel and canonical actor
   matching; message timestamps remain presentation/editing data.
4. A missing or uncertain delivery needs visible operational recovery. It
   cannot grant approval, choose a substitute or silently change surface.

## Alternatives considered

1. Deliver several cards or support static-route fan-out. Deferred because
   exactly one selected recipient meets this contract.
2. Add message timestamps or per-delivery seals to answer proofs. Rejected
   because the registered DM channel and authenticated selected actor suffice.
3. Use a longer fixed pending TTL. Rejected because no finite TTL covers a
   deliberately unbounded pending approval.
4. Open a separate conversation first. Not selected initially because the
   documented user-id posting path uses the existing scope; live qualification
   remains required rather than assumed.

## ADR 0045 back-links on acceptance

Keep Accepted statuses and bodies. Add partial-amendment back-links after status:

1. ADR 0177: ADR 0272 adds `approver_dm` to the opening rule and decisions 1/2,
   and extends decision 6's single-card lifecycle to that surface.
2. ADR 0220 decision 10: ADR 0272 adds one selected approver's DM card location.
3. ADR 0267 decisions 1 and 5: ADR 0272 permits an email-origin approval's card
   in the selected approver's DM while preserving its original email reply handle.
