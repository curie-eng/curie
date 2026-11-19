# 288. The runner carries a connector's approval ticket to its gated call

Date: 2026-11-19

Status: Draft

Partially supersedes [ADR 0271](0271-per-request-approver-sets-and-separation-of-duties.md)
decision 3, only its sentence that the runner "does not parse" the ticket it
transports. Everything else in ADR 0271 stands: the ticket shape, the signed
facts, the creation verifier, the nonce, the route lock and the proxy's spent
grant are unchanged. Narrowly amends the silent terminal refusal of
`HTTP-APPROVAL-CONTINUITY-3` for five ticket refusal codes (decision 3 below).

Source inspection: `origin/main` at
`25408019f947d6217a65dfa188a4ebc5ded5ce1a` and `origin/next` at
`5249832af2a8fef903f7df81275d7b1bfe1a5ef6`, both fetched on 2026-11-19.
Neither branch has a ticket reference, a runner ticket reader or hook
`updatedInput` support. This is source evidence, not runtime qualification.
This Draft authorizes no implementation.

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

## Decision

### 1. Record the connector's own ticket when the approval is created

The runner remembers, per conversation process, the well-formed tickets that a
hosted connector returns. It observes each tool result through an in-process
`PostToolUse` callback, which the CLI runs before the model sees the result, so
a ticket is remembered before any call that copies it is decided.

Only a reply from a server in `ApprovalGate.proxied_connector_servers` (the
hosted entries the caller proxy fronts) is read. Each `cat1` framing in the reply
is parsed with the shared bounded parser
(`curie_internal.approver_ticket.unverified_claims`, the same checks as
`unverified_key_id`) without verifying the signature, and is kept only when its
claimed `connector` equals the server that returned it. The scan reads string
keys and values and tries every `cat1.` start on its own, within bounds on
characters, JSON nodes, depth and candidates; a framing cut short by the
character bound is discarded rather than remembered as a shorter token. The runner keeps the token and
three UNTRUSTED claims: connector, tool and `arguments_sha256`. Memory is bounded
and is lost on suspend, which ADR-0003 already makes a cold rehydrate.

When the model calls a gated tool `mcp__<connector>__<tool>` of a proxied server,
the runner looks for the ticket the model's `approval_ticket` was copied from,
using evidence in the copy itself. The copy must be a damaged copy: a string
that starts with `cat1.`, is no longer than a ticket may be, no longer parses as
a complete ticket, and still carries its source's exact `cat1.<payload>` part
(the signature was damaged) or its exact signature segment (the payload was
damaged, the common case). The source is the one remembered ticket, across every
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
call, a ticket the runner never saw simply has no match. A string the runner
forgot, or could not read in a reply larger than the scan bounds, could be a
second possible source it can no longer count, so either event stops the carry
for the rest of the conversation; an ordinary conversation reaches neither
bound. Freshness is left to the API.

The runner reads these claims only to recover the ticket the connector itself
issued in this conversation, which the copy names and which fits exactly these
arguments. It gains no authority: the API still verifies signature, key, scope,
requester, freshness, digest, ceiling and nonce, and the connector checks again
at apply. A remembered string that is not a genuine ticket (a forgery or a
corrupted echo that still parses) can at worst be chosen when it is the only
match, and its signature then fails at the API, which is the refusal a damaged
copy gets today. Conversely, a damaged copy of such a string may be restored to
the genuine ticket with the identical payload; that is the same signed request.

### 2. Run the approved call with the grant's exact ticket

On a resume with a grant, if the model's call names the granted tool on a
proxied hosted connector, carries a damaged copy (same rule as decision 1) of
the grant's own ticket, which must itself parse and name this connector and
tool, matches no other string the resumed conversation has seen (and the carry
has not stopped), and is canonically equal to the grant's stored arguments in
every other member, the gate spends the
one-shot grant and allows the call with the stored arguments: `updatedInput` on
the approval hook's PreToolUse allow, and `updated_input` on the permission
callback's allow. Both are declared by the pinned SDK. The connector then
receives the exact ticket the API verified, and the proxy's argument-bound grant
matches.

A call that differs in any other argument, omits `approval_ticket`, carries a
different complete ticket or a non-string, or targets a tool outside the proxied
hosted connectors (where an `approval_ticket` argument is ordinary data), still
meets the existing grant-mismatch refusal, and the grant stays unspent. This needs no runner memory: the stored arguments are
the trusted resume boot input.

### 3. Tell the requester when the prepared ticket is refused

When identity approval creation is refused with one of these codes, the worker
ends the turn exactly as before and then replaces the placeholder with one fixed
sentence that names no code, person or reason:

- `approval_ticket_invalid_ticket`
- `approval_ticket_arguments_mismatch`
- `approval_ticket_preparation_expired`
- `approval_ticket_replayed`

Each one is about the prepared ticket (damaged, stale, already used or prepared
for other details), and a fresh request makes a fresh ticket.
`approval_ticket_arguments_unstable` is excluded: the stored arguments changed
form in the database, and asking again for the same details fails the same way. None says anything
about who asked or who may approve, so the notice does not disclose an identity
refusal, which is the reason invocation criterion 18 and
`HTTP-APPROVAL-CONTINUITY-3` keep refusals silent. Every other coded refusal,
including `invalid_approval_context`, `caller_not_allowed` and
`approval_ticket_requester_mismatch`, stays silent.

The lifecycle owner delivers the notice, after the lease check, the run close and
the done marker, so a lost lease sends nothing and a redelivery of the settled
event is skipped before it could send a second one. It is best effort: a failed
delivery is logged and the terminal outcome, marker and ACK are unchanged. The
relay adapter keeps its existing single refusal reply
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
   (PreToolUse `updatedInput`, PostToolUse observer, registered only when a
   proxied connector exists) and `build_can_use_tool`.
4. `packages/curie-internal/src/curie_internal/approver_ticket.py::unverified_claims`.
5. `apps/worker/src/curie_worker/approvals.py::ApprovalContinuityRefused.code`,
   `TICKET_RETRY_CODES`, the refusal branch of
   `apps/worker/src/curie_worker/kernel/approval.py::_pause_for_approval`,
   `kernel/identity.py::HttpIdentityRefused.notice`, the refusal branch of
   `kernel/lifecycle.py::process_event` and
   `kernel/routing.py::_notice_after_refusal`.

Required evidence before Accepted is claimed for runtime:

1. Runner tests with synthetic tickets signed by the shared fixture key: a
   damaged copy is recorded with the connector's exact ticket and passes
   `verify_preparation`; a copy of one request's ticket sent with another
   request's details, two possible sources, other-tool, other-connector,
   non-proxied, ticketless, still-parsing and forgotten cases are left
   unchanged; the scan finds tickets in keys and after malformed prefixes and
   stops at its bounds; the shared vector corpus proves the runner's digest
   equals the signer's.
2. Runner tests that the grant path returns `updatedInput` and `updated_input`
   with the stored ticket, and that other differences and non-proxied tools keep
   the refusal with the grant unspent.
3. Runner tests on the real bundled Claude Code CLI with a stdio MCP connector
   and a local model stand-in: the PostToolUse observer receives the preparation
   result and the held approval carries the exact ticket; after a grant the
   connector receives the stored ticket through `updatedInput`. Each fails when
   its half of the mechanism is removed.
4. Worker tests through the real consumer, kernel and Valkey: the four codes
   post the sentence after the done marker and end terminal; other codes stay
   silent; an undelivered notice keeps the terminal outcome; the relay adapter
   gets no notice.
5. One live run with a hosted connector behind the caller proxy, so the
   proxy's argument-bound grant is seen to accept the restored arguments. Item 3
   uses a stdio connector and does not cover the proxy.

## Consequences

1. The model no longer has to reproduce a long signed value exactly when the
   damage leaves either the payload or the signature intact. Damage to both, a
   same-length change inside the signature, a process restart between
   preparation and the gated call, and a conversation that exceeds the memory
   or scan bounds fall back to today's behavior.
2. The runner now reads ticket claims. They are never trusted: they only select
   among tickets the same connector returned. A connector that returns a ticket
   for another connector is ignored.
3. A requester whose ticket is refused sees one plain sentence instead of the
   placeholder. Refusals about people, routes, configuration and availability
   remain silent.
4. The approved call can be rewritten by the platform's own hook. That is the
   first runner use of `updatedInput`; bundle hooks still cannot emit it.
5. The accepted #1852 race now also covers a repaired call: the approval hook
   spends the grant and returns `updatedInput`, and a concurrently dispatched
   bundle PreToolUse hook that denies the same call (it sees the damaged copy)
   leaves the call unexecuted with the grant spent. Recovery is a fresh approval,
   as for an exact call today.
6. Any reply from the same hosted connector can supply a ticket, not only a
   designated preparation tool, because no contract names that tool. An echoed
   ticket is still that connector's own ticket, is used only when the model's
   copy names it, and must pass every API check for the current requester.

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

Keep ADR 0271's Accepted status and body. Add after its status: "Decision 3's
sentence that the runner does not parse the ticket is partially superseded by
ADR 0288, which reads three untrusted claims to carry the connector's own
ticket."
