# 136. A late workspace handoff replaces the sandbox at a fenced turn boundary

Date: 2026-08-31

Status: Accepted

Explicit maintainer direction on 2026-08-31 authorizes this ADR to be published
Accepted and its realizing implementation to proceed as a stacked change under
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).

## Context

[ADR 0125](0125-managed-repository-workspaces-and-approval-gated-publication.md)
keeps clone and publication credentials in trusted platform components.
[ADR 0126](0126-runtime-repository-selection-is-sticky-authorized-thread-state.md)
makes the first authorized repository selected for an agent conversation
durable and permanent. Their realizing path prepares a workspace before the
conversation's first sandbox claim.

That path does not cover a conversation which already has a live generic
sandbox. A workspace-enabled deployment currently requires a repository before
letting the first turn run, and the workspace claim path adopts any live route
without distinguishing a generic runner from a workspace runner. Letting a
later message hot-mount storage or inject a GitHub credential would cross the
model-controlled boundary. Silently starting a second Curie conversation would
lose the Slack thread's history, approvals, steering, and delivery identity.

The missing operation is therefore a controlled replacement: retain the
logical conversation and its durable authority, but move it to a new sandbox
whose workspace was prepared outside that sandbox. Replacement must survive a
worker crash at every step without leaving two authoritative runners or losing
the original one before its successor is ready.

## Decision

**A workspace-capable conversation may begin on a generic sandbox and acquire
its one sticky repository later. The worker performs that acquisition only
between turns by preparing a workspace, cold-claiming a replacement sandbox,
and atomically fencing the old route.**

### No repository remains an ordinary generic turn

`workspace_enabled` declares capability, not a requirement that every
conversation already have a repository. Before routing every human turn, the
trusted worker parses the optional repository fact and asks the
worker-authenticated API to establish or read ADR 0126's selection. When both
the message and durable selection contain no repository, the turn follows the
ordinary generic-sandbox path. It does not redeem a clone credential, prepare
an archive, or touch the workspace object store.

An unauthorized or ambiguous repository is refused before sandbox adoption,
steering, workspace preparation, or model execution. Once a repository wins,
the existing `(agent_id, conversation_id)` row remains sticky. The selecting
author is the authenticated ingress principal carried by the worker; the API
derives the agent from the deployment and trusts no caller-supplied agent or
origin. A conflicting repository remains a terminal refusal and creates no
checkout.

### The boundary is terminal, durable, locked, and fenced

The repository-bearing delivery is a new turn, never a steer into the generic
runner. The worker holds the existing distributed thread lock and its current
renewable delivery lease before inspecting or changing the route. A live turn
defers the repository-bearing delivery without acknowledging it; redelivery
re-enters through the same sticky selection and deadline after the owning turn
finishes.

The generic runner is safe to replace only when its authenticated status says:

1. no turn can accept a steer;
2. the most recently completed turn's structured transcript append succeeded;
   and
3. no approval suspension or unresolved side-effect boundary is active.

An unreadable or incomplete status fails closed and retains the old route.
Steers accepted by the completed turn are part of that turn's structured
message history, so the durable check covers them. A steer folded into a failed
turn is not promoted into history and cannot authorize a handoff; the original
delivery retry semantics remain authoritative.

### Replacement is a compare-and-swap, not suspend then hope

The worker renders the same deterministic session id, history reference,
scoped history token, bundle reference, approvals, and other boot facts it
would render for a fresh claim. The trusted workspace coordinator prepares and
verifies the sanitized repository archive first. It then asks the sandbox
substrate to create a new claim with a fresh generation while the old claim and
route remain intact.

After the replacement sandbox is ready, the affinity store atomically replaces
the route only if it still names the old claim and fencing generation. Losing
that compare-and-swap deletes the losing new claim and adopts no authority.
Winning it makes the new claim the sole route; only then may the old claim be
deleted. The repository-bearing delivery starts on the replacement runner
after the route swap, so its model context is the replayed logical history plus
the triggering message exactly once.

The route records whether its sandbox was claimed with a workspace and the
canonical repository, but carries no credential. A live workspace route for
the same selected repository is reusable. A generic route, a different
repository, or a route without trustworthy workspace metadata is never treated
as an already-complete handoff.

### Crash recovery follows the route fence

Before the route compare-and-swap, failure or cancellation deletes the
candidate claim and candidate workspace object and leaves the generic route
authoritative. If a process dies, the existing orphan reaper removes the
unrouted candidate while the generic route remains live.

After the compare-and-swap, the replacement route is authoritative even if
deleting the old claim or replying to the channel fails. The existing reaper
removes the now-unrouted old claim. A retry reads the sticky repository and
workspace-marked route, adopts the replacement, and does not clone or swap
again. An uncertain affinity write is reconciled by reading the exact route:
old claim means retry is safe, new claim means adopt, and any other generation
means fail closed for its current owner to resolve.

Workspace ownership follows the same transaction boundary. A failed candidate
restores the previous ownership ledger; a winning route retains exactly the
archive referenced by the replacement. Cleanup is idempotent and never deletes
an archive still named by an authoritative route.

### Credentials do not cross with the conversation

Workspace preparation retains ADRs 0125 and 0126 unchanged: the API authorizes
the sticky repository before credential resolution; the worker uses a clone
credential only as ephemeral process-scoped Git configuration; the sanitized
remote is verified before archiving; and the sandbox receives only the signed
workspace reference and digest. The replacement claim receives neither a
GitHub token nor the internal worker credential, object-store credential,
authenticated clone URL, or process-scoped Git configuration.

Verification must search the replacement claim environment, materialized
workspace, runner events, and worker/runner logs for a unique synthetic
credential marker and find none. The negative unauthorized, ambiguous, and
conflicting repository paths must prove that no candidate claim, archive, or
credential redemption was created.

### Existing interfaces are sufficient

The existing ACI session id and event frame, plus the platform-owned history
and workspace boot fields, carry everything the replacement needs. This
decision authorizes no change to `packages/aci-protocol` or
`packages/plugin-format`. Handoff generation, workspace route metadata,
durability status, and affinity compare-and-swap are internal platform
contracts. If implementation discovers that a new ACI field is required, it
must stop under the frozen-contract rule rather than widening this decision.

The realizing paths are:

- `apps/api/src/curie_api/routers/workspaces.py`, `schemas.py`, and the existing
  workspace selection CRUD for optional no-selection responses and sticky
  principal/repository authorization;
- `apps/worker/src/curie_worker/kernel.py`, `workspace.py`, and
  `runner_client.py` for pre-steer selection, safe-boundary checks, preparation,
  replay, and recovery;
- `apps/worker/src/curie_worker/sandbox/affinity.py`, `substrate.py`, and
  `types.py` for workspace-marked routes and fenced replacement;
- `runner/src/curie_runner/session.py` and `server.py` for authenticated
  terminal transcript-durability status; and
- worker, API, runner, and disposable cluster-runtime tests which prove the
  positive replacement, every pre/post-fence recovery side, repository
  refusals, history and steering continuity, credential absence, and cleanup.

## Consequences

A Slack conversation can investigate without a repository, accept one later,
and continue as the same logical Curie thread in a fresh workspace sandbox.
The replacement costs one cold claim and one repository preparation; it is not
a hot path for conversations which start with a repository or never select
one.

The affinity route becomes security and recovery state rather than only a
dial target. Its compare-and-swap must stay atomic with respect to ordinary
claim, resume, release, and reaping. Route metadata remains internal and may be
rolled forward without changing the ACI wire.

Transcript durability becomes observable at the runner boundary. A history
store outage can delay workspace acquisition even after a reply was delivered;
preserving context is safer than replacing the runner with a known stale
history prefix.

The selecting principal aims an operator-owned repository identity under the
current Slack membership boundary. Per-user GitHub OAuth remains separate and
can replace credential resolution without changing the sandbox handoff.

## Alternatives considered

### Start a new Slack or Curie thread

Rejected. It loses the logical conversation, steering context, approvals, and
the user-facing same-thread experience this capability exists to preserve.

### Mount a workspace into the live sandbox

Rejected. A hot mount bypasses claim-time verification, creates a second
workspace delivery mechanism, and makes failure recovery depend on mutating a
running pod.

### Give the generic sandbox a credential and clone in place

Rejected. It exposes operator authority to prompt-controlled code and undoes
the credential boundary established by ADR 0125.

### Suspend or delete the old sandbox before preparing its replacement

Rejected. Clone, archive, object-store, quota, or readiness failure would turn
a recoverable handoff into avoidable downtime. Prepare and cold-claim first;
the route compare-and-swap is the cutover.

### Reuse any live route after repository selection

Rejected. A live generic runner has no verified workspace. Reuse is permitted
only when authoritative route metadata names the same sticky repository.

### Replay only a prose summary

Rejected. Structured history, tool results, approvals, and accepted steering
already have a durable representation. Flattening them would lose semantics and
prompt-cache continuity exactly at the transition.
