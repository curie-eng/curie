# Requester display across chained approvals

A person can ask for several actions in one turn. Each action can have its own
approval, and each resolution resumes the same task before it reaches the next
gate. Every card in that uninterrupted chain must identify the person who asked
for the task, even when another person approved an earlier gate.

## Separate the identities

The display requester is the person who started the current approval chain.
The queued resume author remains the resolver; an expiry resume remains authored
by the system. The durable approval author continues to identify the author of
the turn that raised that particular gate. Resolution uses the same authenticated
principal and current policy as before. Requester display must never participate
in authorization, requester-only eligibility, admission, grants, or action-ledger
attribution.

## Acceptance criteria

<!-- @spec WORKER-REQUESTER-1 -->
For three approvals raised from one person's request and its automatic approval
resumes, all live and settled cards show the original display requester.
Decision attribution continues to show the actual resolver and recorded time.
The display requester is remembered separately from the current resume actor.

<!-- @spec WORKER-REQUESTER-2 -->
Derive display identity from durable approval lineage, not model prose, notes,
summary text, thread-wide last-user memory, or the identity of the latest
resolver. A normal fresh human turn starts a new chain, including one in the
same thread. A follow-up after a rejected or expired approval must not inherit
permission or treat the old decision as approval for a new action.

<!-- @spec WORKER-REQUESTER-3 -->
Lineage derivation is bounded and cycle-safe. Every preceding approval must
exist and match the agent, bare conversation, reply kind, reply channel, reply
adapter identity, and reply endpoint of the requesting surface. Reply placeholders
are not origin identity: the resumed answer can legitimately post below a card.
Approver route and card channel can differ between gates and do not identify the
requesting surface. Each link names a resolved or expired predecessor and the
expected resume actor; a pending predecessor cannot prove a continuation.
A missing, mismatched, cyclic, or excessive chain yields unavailable display
attribution. It must not borrow another agent's or conversation's requester.

<!-- @spec WORKER-REQUESTER-4 -->
Derivation uses the persisted row on both fresh creation and an idempotent
creation replay. Altering a replay request body cannot rewrite its attribution.
A worker or API restart and consumption of a prior card reference do not erase
the identity of the current chain. A concurrent repeated delivery still creates
and renders the same approval without changing the resolver or principal.

<!-- @spec WORKER-REQUESTER-5 -->
Transport actor and authorization remain unchanged. Existing resolve/expiry
author assertions and requester-only resolver eligibility must continue to pass.
No field is added to the frozen queued turn, approval request, or reply contract.
If a continuation's display identity cannot be established, the card omits the
requester line rather than labeling the resolver as the original requester.
The gate remains actionable under its existing policy.

## Proposed response seam

The API computes a display-only requester for the approval creation response,
using a bounded walk of the persisted dedupe lineage. The existing ordinary
approval create response gains an optional requester field through an API-local
creation response model. Listing, reading and resolving approval records retain
their existing record shape and author semantics. No database column or migration
is required. An explicit unavailable result is different from a response from an
older API that does not implement derivation.

The worker consumes that creation result in its internal DTO and uses the display
requester consistently for the live card, the remembered settled card identity,
and notification metadata. For an older API response, a fresh ordinary turn can
use its actual author; an automatic approval resume cannot assume its resolver
is the original requester and therefore leaves display attribution unavailable.
The Slack renderer omits the requester context block when the identity is empty,
as the settled renderer already does. This changes display only.

A bounded walk follows approval ids from resume event ids, validates every link,
and ends only at a non-resume event. A current resolved link must be authored by
its predecessor's recorded resolver; an expired link must be system-authored.
The walk has a maximum of 64 predecessor reads, independent of chain contents.
The derived identity cannot authorize anything or reclassify a tool as read-only.

## Verification

Tests use real Postgres and Valkey for lineage and worker/card state. Cover a
three-gate chain, a new person's fresh request in the same thread, different
requesting surfaces and agents, pending or missing predecessors, cycles and
bounds, idempotent replay, worker restart and consumed card memory, resolution
notes, rejection and expiry. Preserve resolver author, durable author and
requester-only eligibility assertions. Capture actual isolated Slack card posts
and settles for a three-action replay after deploying the final candidate.
