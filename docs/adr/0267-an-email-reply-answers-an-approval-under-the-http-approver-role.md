# 267. An email reply answers an approval under the HTTP approver role

Date: 2026-11-03

Status: Accepted

Accepted 2026-11-03 under ADR 0102 coordinated acceptance with explicit
approval by Junwon Jung (jw3329), recorded as "Yes, next + ADR", including
merging alongside its realizing code path in one combined pull request. His
correction of the intended design, translated from Korean, is recorded
verbatim: "I never said the approve button must be pressed in Slack. Keep it
the same as before, but when approving, check the Okta group: if the person is
in the group they pass, if not they are rejected. That is what I wanted." The
realizing code path is `curie_api.identity.http_approval.create_http_approval`
through `_original`,
`curie_api.identity.http_approval_answer.answer_http_approval` through
`_authenticate_email_actor`, `_authenticate_actor` and `_preserved_actor`,
`curie_api.identity.http_invocation.turn_invocation_source`,
`curie_api.identity.http_resume._check_resume`,
`curie_internal.identity_approval.EmailApprovalAnswerProofV1` with
`sign_email_approval_answer_proof` and `verify_email_approval_answer_proof`,
and the worker's email approval pause in `curie_worker.kernel.approval`.

Amends [ADR 0260](0260-http-identity-admits-an-email-sender-only-on-a-signed-email-receiver-proof.md)
decision 6. It supersedes in part exactly one sentence of that decision:
"Approval answers by email reply are not added." Everything else in ADR 0260
stands.

## Context

Before HTTP identity, an email turn that reaches an approval gate posts its
card in the email thread (or in a fixed Slack channel when its route names
one), and an approver answers by replying to the mail. The mail adapter
forwards the decision under its adapter credential, naming the sender it
authenticated, and the route's approver email list decides who may answer
(ADR 0154, ADR 0177).

Under ADR 0260 an email binding that needs HTTP facts admits a sender only on
a signed email receiver proof. The approval continuity owner, however, accepts
only a Slack reply handle and only a Slack answer proved by a Slack receiver.
An email turn that reaches an approval gate is refused, so an agent that
serves email requesters cannot complete any gated action once its binding
requires HTTP identity.

The intent is the earlier behaviour, with the answer authorised the way HTTP
identity authorises everything else: the agent's approver role, typically a
migrated role whose filter names an identity provider group, evaluated fresh
on the answering person.

## Decision

### 1. An email turn may raise an approval

The internal approval creation owner accepts an email-origin turn: one whose
sealed context names an email receiver registration and whose reply handle is
email. Its card follows its route as before: in the email thread when the
route resolves to the requesting surface or names no route, or in a fixed
Slack channel when the route names one. Every other creation check is
unchanged. The worker no longer refuses such an approval for lacking a route
approver email list, because the approver role decides who may answer.

### 2. An email reply answers through a signed email approval answer proof

The mail adapter forwards an email answer to the existing resolve route under
its adapter credential and the sender it authenticated, and adds an email
approval answer proof signed with that mailbox's email receiver key. The proof
names its purpose (`approval_answer`), the operation (`resolve`), the receiver
registration, the email binding, the approval id, the lowercased sender
address and the SHA-256 of the exact resolve body, which carries the decision.
It uses a signed prefix of its own, so it never verifies as an invocation
proof, and an invocation proof never verifies as an answer proof. It binds one
decision to one approval: it cannot be moved to another approval, decision or
sender, and a resolved approval accepts no second answer.

Only a card shown in the email thread may be answered by email. A card in a
fixed Slack channel is answered in Slack, as today.

### 3. The answering sender is judged by the approver role, fresh

The API reconstructs the answering sender from the email registration exactly
as it reconstructs an email requester, and runs the shared approval answer
composition: a fresh provider lookup for the sender with purpose `approval`,
the agent's approver role evaluated on those facts, and a fresh check that the
original requester still holds invocation authority. A sender in the role
approves or rejects; a sender outside it is refused with 403
`caller_not_allowed` and the approval stays pending. A sender the provider
does not resolve is refused the same way. An unavailable provider refuses
with 503 `authority_unavailable`. Nothing is inferred from the adapter
credential alone.

A route approver email list, when declared, still narrows who may answer; it
never widens the role. A migrated approver role with no route restriction is
the whole answer authority.

### 4. The original invoker stays the email sender

Creation, answer, resume publication and the resume current check
reconstruct the original invoker from its email registration. The resumed
turn replies through the original email reply handle.

### 5. Slack answers are unchanged

A Slack answer keeps its Slack receiver proof and the registration
restriction it already has. For an email-origin approval whose card is in a
fixed Slack channel, no Slack registration lists the email binding, so the
answering Slack registration is bound instead to that stored card.

## Consequences

An agent behind HTTP identity can again complete gated actions requested by
email, with the approver answering by email reply, authorised by the identity
provider group its approver role names.

A mail adapter that answers approvals must sign the email approval answer
proof. An adapter that does not keeps working for agents on the legacy path;
its answers to an HTTP-selected approval are refused.

## Alternatives considered

1. **Answer only in Slack.** Rejected: the earlier behaviour answers in the
   email thread, and the approver may not use Slack.
2. **Trust the adapter credential and the sender header.** Rejected: whoever
   holds the platform key can mint an adapter credential and name any sender,
   the weakness ADR 0260 removed for invocation.
3. **Reuse the email invocation proof.** Rejected: an invocation proof could
   then be replayed as an answer. A purpose-bound proof under its own prefix
   keeps the two apart.
4. **Keep the route approver email list as the only authority.** Rejected: it
   duplicates, by hand, the group membership the identity provider already
   owns.
