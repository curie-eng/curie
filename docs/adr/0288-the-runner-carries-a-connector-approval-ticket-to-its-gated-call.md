# 288. The runner carries a connector's approval ticket to its gated call

Date: 2026-11-19

Status: Accepted

Accepted on 2026-11-20 alongside implementation, with explicit maintainer
approval from Brian Conn recorded in
[the review of #5876](https://github.com/curie-eng/curie/pull/5876#pullrequestreview-5991710461),
under the coordinated exception of
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The realizing code paths are named below. Required evidence item 5, the live
run behind the caller proxy, remains required.

Partially supersedes [ADR 0271](0271-per-request-approver-sets-and-separation-of-duties.md)
decision 3, only its opaque, unparsed transport of the ticket: "Carry the opaque
ticket" and the sentence that the runner "does not parse" the ticket it
transports. Everything else in ADR 0271 stands: the ticket shape, the signed
facts, the creation verifier, the nonce, the route lock and the proxy's spent
grant are unchanged. Narrowly amends the silent terminal refusal of
`HTTP-APPROVAL-CONTINUITY-3` and `HTTP-INVOCATION-EXECUTION-31` for four ticket
refusal codes (decision 3 below).

Source inspection: `origin/main` at
`25408019f947d6217a65dfa188a4ebc5ded5ce1a` and `origin/next` at
`5249832af2a8fef903f7df81275d7b1bfe1a5ef6`, both fetched on 2026-11-19.
Neither branch has a ticket reference, a runner ticket reader or hook
`updatedInput` support. This is source evidence, not runtime qualification.

## Context

Under ADR 0271 a hosted connector's preparation tool returns a signed
`cat1.<payload>.<signature>` ticket, and the model must copy it, unchanged,
into the top-level `approval_ticket` argument of the gated tool. The runner
records the model's exact arguments as the pending approval
(`runner/src/curie_runner/approval.py::ApprovalGate._record_pending`), the
worker sends them as `granted_arguments`, and the API reads the ticket only
from `arguments["approval_ticket"]`
(`apps/api/src/curie_api/approver_tickets.py::verify_ticket`).

A ticket is about 1,000 to 1,500 characters of base64url, and every member is
required. On a live deployment test about 2 in 10 requests failed because the
model dropped or changed part of the ticket. In one failure it dropped exactly
one encoded `"tenant_id":"00000000-0000-0000-0000-000000000000",` member, 68
characters: with the default all-zero tenant id, the payload repeats long
identical base64 runs, and models are prone to skip one copy of repeated text.
The rate therefore depends on the data, not only on the model.

The API refuses such a creation with a coded 422. The worker treats every coded
creation refusal as the HTTP terminal refusal
(`apps/worker/src/curie_worker/kernel/approval.py`, `ApprovalContinuityRefused`
to `HttpIdentityRefused`), marks the turn done and posts nothing, so the
requester is left with the placeholder and no explanation.

A bundle cannot work around this. `runner/src/curie_runner/hooks.py` keeps only
deny, ask, block and stop from a bundle hook and drops `updatedInput`. Bundle
PreToolUse hooks run concurrently with the approval hook, so a bundle hook that
denies a damaged ticket does not stop the pending approval from being recorded.
The connector cannot fill the ticket itself, because the gate stops the call
before it reaches the caller proxy. Shortening the ticket saves a few percent at
most, and the recipient accounts that make it long are required for
`approver_dm` delivery.

The model also copies the ticket a second time, on the approved call after
resume. A damaged copy there already meets the grant-mismatch refusal inside the
turn (#3174), and the model may retry, but the copy can fail again.

The decisions below mitigate this failure (#5847); they do not guarantee that
the gated call receives the issued ticket. They remove the observed failure when
the runner remembers exactly one eligible source for the copy: a copy that keeps
one half of the ticket intact and no longer parses. The cases in consequence 1,
and a payload edit that still parses, still send a copy that differs from the
issued ticket; decision 3 turns the API's refusal of such a copy into a visible
request to ask again.

## Decision

### 1. Record the connector's own ticket when the approval is created

The runner remembers, per conversation process, the well-formed tickets that a
hosted connector returns. It observes each tool result through an in-process
`PostToolUse` callback, which the CLI runs before the model sees the result, so
a ticket is remembered before any call that copies it is decided.

Only a reply from a server in `ApprovalGate.proxied_connector_servers` (the
hosted entries the caller proxy fronts) is read. Each `cat1` framing in the
reply is parsed with the shared bounded parser
(`curie_internal.approver_ticket.unverified_claims`, the same checks as
`unverified_key_id`) without verifying the signature. A ticket is selectable
only when its claimed `connector` equals the server that returned it; a ticket
naming another connector is kept only as evidence (below). The scan reads string
keys and values and tries every `cat1.` start on its own, within bounds on
characters, JSON nodes, depth and candidates; a framing cut short by the
character bound is discarded rather than remembered as a shorter token. The
runner keeps the token and three UNTRUSTED claims: connector, tool and
`arguments_sha256`. Memory is bounded and is lost on suspend, which ADR-0003
already makes a cold rehydrate.

When the model calls a gated tool `mcp__<connector>__<tool>` of a proxied server,
the runner looks for the ticket the model's `approval_ticket` was copied from,
using evidence in the copy itself. The copy must be a damaged copy: a string
that starts with `cat1.`, is no longer than a ticket may be, no longer parses as
a complete ticket, and either starts with its source's exact `cat1.<payload>.`
(the signature was damaged) or ends with its source's exact `.<signature>` (the
payload was damaged, the common case). Both matches are anchored at an end of
the copy: the source's payload or signature appearing anywhere else in the copy
is no evidence. The source is the one remembered ticket, across every
connector and tool, that the copy is a damaged copy of; it must name this
connector and tool. The runner then requires the source's `arguments_sha256` to
equal `execution_arguments_sha256` of the call's other arguments, the production
rule the signer and the API share, and records the arguments with the source
ticket instead of the copy.

In every other case it records the model's arguments unchanged, which is today's
behavior: no damaged copy (a copy that still parses cannot be told apart from a
different ticket), an ungated or non-connector tool, no remembered source or two
of them, a source for another connector or tool, or a source issued for other
arguments. Because the source is named by the copy and checked against the
call, a ticket the runner never saw is never chosen. A string the runner
forgot, or could not read (a reply larger than the scan bounds, a value that is
not JSON, or an observation that was malformed or failed), could be a second
possible source it can no longer count, so any of these stops the carry for the
rest of the conversation; an ordinary conversation reaches none of them. A
ticket one connector returns that names another connector is never chosen, but
it counts as a possible source, so it can make a match ambiguous. Freshness is
left to the API.

The runner reads these claims only to recover the unique matching ticket that
connector returned in this conversation, which the copy names and which claims
exactly these arguments. It gains no authority: the API still verifies
signature, key, scope, requester, freshness, digest, ceiling and nonce, and the
connector checks again at apply. A remembered string that is not a genuine
ticket (a forgery or a corrupted echo that still parses) can at worst be chosen
when it is the only match, and its signature then fails at the API, which is the
refusal a damaged copy gets today. Conversely, a damaged copy of such a string
may be restored to the genuine ticket with the identical payload; that is the
same signed request.

### 2. Run the approved call with the grant's exact ticket

On a resume with a grant, if the model's call names the granted tool on a
proxied hosted connector, carries a damaged copy (same rule as decision 1) of
the grant's own ticket, which must itself parse and name this connector and
tool, and is canonically equal to the grant's stored arguments in every other
member, the gate spends the one-shot grant and allows the call with the stored
arguments: `updatedInput` on the approval hook's PreToolUse allow, and
`updated_input` on the permission callback's allow. Both are declared by the
pinned SDK. The connector then receives the exact ticket the API verified, and
the proxy's argument-bound grant is expected to match; required evidence item 5
is the run that observes it.

A call that differs in any other argument, omits `approval_ticket`, carries a
different complete ticket or a non-string, or carries a differing
`approval_ticket` on a tool outside the proxied hosted connectors (where that
argument is ordinary data and an exact call is granted as before), still meets
the existing grant-mismatch refusal, and the grant stays unspent.

The stored arguments are the only trust anchor: they are the trusted resume boot
input and carry the exact ticket the API verified. The repair reads no runner
memory. It does not scan the rehydrated transcript, it does not consult any
ticket observed before or since the resume, and a stopped carry does not affect
it, so it behaves the same whether or not the process restarted. No other check
is needed, because the repaired call is the approved call byte for byte: even
when the model's copy could also be a damaged copy of another string, the only
call that can run is the one the approver approved.

### 3. Tell the requester when the prepared ticket is refused

When identity approval creation is refused with one of the four codes below,
the worker ends the turn exactly as before and then replaces the placeholder
with this fixed notice, which names no code, person or reason:

> The approval request for this action could not be created. Please ask again,
> and a new request will be prepared.

The worker sends exactly this text
(`apps/worker/src/curie_worker/approvals.py::TICKET_RETRY_REPLY`). The four
codes are:

- `approval_ticket_invalid_ticket`
- `approval_ticket_arguments_mismatch`
- `approval_ticket_preparation_expired`
- `approval_ticket_replayed`

Each one is about the prepared ticket (damaged, stale, already used or prepared
for other details), and a fresh request makes a fresh ticket.
`approval_ticket_arguments_unstable` is excluded: the stored arguments changed
form in the database, and asking again for the same details fails the same way.
None says anything about who asked or who may approve, so the notice does not
disclose an identity refusal, which is the reason invocation criteria 18 and 31
and `HTTP-APPROVAL-CONTINUITY-3` keep refusals silent. Every other coded refusal,
including `invalid_approval_context`, `caller_not_allowed` and
`approval_ticket_requester_mismatch`, stays silent.

The lifecycle owner delivers the notice, after the lease check, the run close
and the done marker, and checks the lease again just before sending, so an owner
whose fenced lease (ADR-0131) is lost or past its confirmed window by then sends
nothing, and a redelivery of the settled event is skipped before it could send a
second one. A leaseless direct caller or a consumer without a lease store has no
fence to check; the kernel already treats each as the delivery's only owner. It
is best effort: a failed delivery is logged and the terminal outcome, marker and
ACK are unchanged. The relay adapter keeps its existing single refusal reply
(HTTP-INVOCATION-EXECUTION-31) and gets no notice; targetless and factory turns
post nothing.

The notice has the same authority basis as any reply the turn would otherwise
post. The API verifies the ticket before its current-caller check, so a caller
whose authority is revoked during the turn and whose ticket is also damaged would
receive the notice; that caller would equally have received the turn's ordinary
answer had no approval been needed, because the worker does not repeat the
current-authority check before a final reply.

## Realizing code paths and required evidence

The named owners are:

1. `runner/src/curie_runner/ticket_carry.py::TicketCarry` (observe and restore)
   and `live_pair`, which repeats the worker's
   `kernel/claim.py::_connector_tool_grant` parsing rule.
2. `runner/src/curie_runner/approval.py::ApprovalGate.observe_tool_result`,
   `carried_input`, `granted_with_ticket` and `_decide_gate`, shared by both
   interception points.
3. `runner/src/curie_runner/harness/claude/approval.py::build_approval_hook`
   (PreToolUse `updatedInput`, built as before; only the PostToolUse observer
   depends on a proxied connector) and `build_can_use_tool`, with
   `runner/src/curie_runner/__main__.py::build_runner`, which sets
   `proxied_connector_servers` on the gate before the hooks are built.
4. `packages/curie-internal/src/curie_internal/approver_ticket.py::unverified_claims`.
5. `apps/worker/src/curie_worker/approvals.py::ApprovalContinuityRefused.code`,
   `TICKET_RETRY_CODES`, the refusal branch of
   `apps/worker/src/curie_worker/kernel/approval.py::_pause_for_approval`,
   `TICKET_RETRY_REPLY`, `kernel/identity.py::HttpIdentityRefused.notice`, the
   refusal branch of `kernel/lifecycle.py::process_event`,
   `kernel/routing.py::_notice_after_refusal` and its `Kernel` binding in
   `kernel/core.py`.
6. `docs/superpowers/specs/2026-10-26-http-approval-continuity-v1.md`, criterion
   3 (`HTTP-APPROVAL-CONTINUITY-3`), and
   `docs/superpowers/specs/2026-10-26-http-invocation-execution-v1.md`, criterion
   31 (`HTTP-INVOCATION-EXECUTION-31`): both name the decision 3 exception, so
   neither spec describes every definitive creation refusal as silent.

Required evidence before Accepted is claimed for runtime:

1. Runner tests with synthetic tickets signed by the shared fixture key: a
   damaged copy is recorded with the connector's exact ticket and passes
   `verify_preparation`; a copy of one request's ticket sent with another
   request's details, two possible sources, other-tool, other-connector,
   non-proxied, ticketless, still-parsing and forgotten cases are left
   unchanged, and so is a copy whose source's payload or signature appears only
   away from its ends; the scan finds tickets in keys and after malformed
   prefixes and stops at its bounds; the shared vector corpus proves the
   runner's digest equals the signer's.
2. Runner tests that the grant path returns `updatedInput` and `updated_input`
   with the stored ticket, that nothing observed since the resume (a string
   sharing the evidence, an echo, a failed observation) changes that, and that
   other differences and non-proxied tools keep the refusal with the grant
   unspent.
3. Runner tests on the real bundled Claude Code CLI with a stdio MCP connector
   and a local model stand-in: the PostToolUse observer receives the preparation
   result and the held approval carries the exact ticket; after a grant the
   connector receives the stored ticket through `updatedInput`. Each fails when
   its half of the mechanism is removed.
4. Worker tests through the real consumer, kernel and Valkey: the four codes
   post the notice after the done marker and end terminal; the worker's text
   equals the quoted notice; other codes stay silent; an undelivered notice
   keeps the terminal outcome; the relay adapter gets no notice.
5. One live run with a hosted connector behind the caller proxy, so the
   proxy's argument-bound grant is seen to accept the restored arguments. Item 3
   uses a stdio connector and does not cover the proxy.

## Consequences

1. This is a mitigation, not a guarantee. The model no longer has to reproduce a
   long signed value exactly when the copy keeps the source's payload prefix or
   signature suffix and no longer parses. Damage to both, a payload edit that
   still parses, a same-length change inside the signature that still parses, a
   process restart between preparation and the gated call, and a conversation
   that exceeds the memory or scan bounds leave the model's copy as written at
   creation, as today. The API then decides: an exact copy still succeeds, and a
   damaged one is refused, which decision 3 makes visible for its four codes. On
   the approved call, a differing copy that does not qualify for repair meets
   the existing grant-mismatch refusal inside the turn.
2. The runner now reads ticket claims. They are never trusted: they only select
   among tickets the same connector returned. A ticket that one connector
   returns for another connector is never chosen; it only counts as a possible
   source, so it can make a match ambiguous.
3. When the API refuses a ticket with one of the four codes, the worker tries,
   best effort, to replace the placeholder with the fixed notice, when the turn
   has a person-facing target and the lease check in decision 3 allows it.
   Refusals about people, routes, configuration and availability remain silent.
4. The approved call can be rewritten by the platform's own hook. That is the
   first runner use of `updatedInput`; bundle hooks still cannot emit it.
5. The accepted #1852 race now also covers a repaired call: the approval hook
   spends the grant and returns `updatedInput`, and a concurrently dispatched
   bundle PreToolUse hook that denies the same call (it sees the damaged copy)
   leaves the call unexecuted with the grant spent. Recovery is a fresh approval,
   as for an exact call today.
6. Any reply from the same hosted connector can supply a ticket, not only a
   designated preparation tool, because no contract names that tool. An echoed
   ticket names that connector and was returned by it, is used only when the
   model's copy matches it, and must pass every API check for the current
   requester.

## Alternatives considered

1. Stronger prompt wording. Rejected: it cannot be proven, and the failure rate
   depends on the data as well as the model.
2. A shorter ticket. Rejected: every member is required, and the saving is a
   few percent while the repeated runs remain.
3. Let bundle hooks repair or refuse the ticket. Rejected: honoring a bundle
   hook's `updatedInput` would let bundle code rewrite any call, and a bundle
   refusal runs concurrently with the approval hook, so it cannot stop the
   record.
4. A short ticket reference that the platform resolves. Rejected for now: it
   changes the creation contract and reopens ADR 0271 alternative 5, which
   rejected a connector result-read API.
5. Carry without parsing: use the ticket only when it is the single ticket that
   connector returned in the current turn. Rejected as the primary rule because
   it cannot tell two tickets apart in one turn and loses a ticket prepared in
   the previous turn; it remains a fallback if reading claims is not accepted.
6. Choose the newest of several matching tickets, or pick the one remembered
   ticket whose digest matches the call when the copy names none. Rejected:
   the arguments alone do not say which ticket the model was copying, and a
   ticket the runner never saw or forgot could be the real source. The copy's
   own surviving payload or signature is the evidence; without it, do nothing.
7. Post a notice for every creation refusal. Rejected: identity refusals must
   stay silent on a public thread.
8. Name the preparation tool in the bundle so only its replies are read.
   Rejected for now: it changes the frozen plugin format for a marginal gain
   (consequence 6).
9. Repeat the current-authority check before the notice. Rejected for now: the
   versioned internal `TurnAuthorityRequestV1` contract has no stage for a
   notice, adding one changes the API and the worker together, and an ordinary
   final reply does not repeat the check either.

## ADR 0045 back-links on acceptance

Keep ADR 0271's Accepted status and body. Add after its status line a
back-link saying that decision 3's opaque, unparsed transport of the ticket
("Carry the opaque ticket" and "it does not parse") is partially superseded by
ADR 0288, which reads three untrusted claims to carry the connector's own
ticket, and that the ticket shape, its signed facts and every verification step
are unchanged.
