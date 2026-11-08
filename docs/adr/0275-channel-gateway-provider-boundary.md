# 275. A channel gateway owns provider authentication and I/O

Date: 2026-11-07

Status: Draft

## Context

Slack connection, intake, rendering and provider credentials currently span the
dispatcher, API and worker. Mail has its own standalone adapter. Consolidating
provider I/O must preserve the existing authority, storage and visible behavior.
The kernel also needs synchronous delivery capabilities before running a model;
an HTTP emitter alone cannot supply those answers.

## Proposed boundary

`packages/channel-gateway` owns provider connections, authentication, intake,
rendering, clicks, replies and provider reads. Its `curie_channel_gateway.ports`
package depends on neutral channel contracts and the standard library. The
import-linter contract in `pyproject.toml` forbids provider SDK, API and worker
imports from these ports. It is exercised with violating imports as well as the
valid package. No production composition uses the new ports in this slice.

The gateway authenticates Slack via signature verification or Socket Mode. It
mints the chat-attester principal and signs receiver answer proofs over exact
resolve bytes and that principal. The API verifies those signatures and the
evidence shape, derives the principal from the evidence, and authorizes the
operation. It cannot independently establish that Slack delivered the event.
Whoever holds the platform key and attester secret can assert any user.

The API retains admission authority, verified-principal policy, authorization,
resolve-once and mandatory uncached group membership reads. The worker retains
resume enqueue, stream consumption, throttling and completion/outbox ownership.
API admission state, Valkey dedupe/cache and stream ownership, and mail's local
SQLite delivery state remain with their existing owners. The gateway adds no
Postgres store or platform coordination contract.

The reply contract retains the existing channel reply wire and acknowledgement.
`AuthenticatedReplyTransport` additionally declares `edits_in_place` and
`undeliverable_reason` for each kind and route. `DeclaredReplyTransport` attaches
an immutable, complete provider configuration snapshot to an HTTP emitter.
Composition supplies and refreshes the declarations as provider identities change;
unknown declarations raise instead of falling back to buffered or deliverable.
The declarations remain outside the reply event wire.

`AdmissionClient` wraps the existing admission endpoint; `TurnSubmission` wraps
the existing channel-turn endpoint; `ApprovalResolution` wraps approval resolve.
Lane adapters fix their channel kind, never taking it from a delivery. A returned
connect URL is single-use and must never be cached or shared.

## Consequences and extraction sequence

This is a behavior-neutral extraction. U2 moves Slack intake and mail while
retaining the standalone mail entrypoint and stock Slack admission, dedupe claim,
placeholder, enqueue ordering. U3 moves replies, cards and clicks after promoting
the general `Reply` value to `channel-protocol` and migrating all callers. U4
moves provider reads and credentials while the API continues executing evidence
validation and authorization, including uncached `fresh_members`.

Every move deletes the duplicated protocol and migrates its callers. In
particular, replace dispatcher `AdmissionAnswer`, API `GroupMembershipSource`,
`ChannelReader` and `CanvasReader`; keep no parallel definition or compatibility
alias. Shared neutral value records belong in `channel-protocol` when needed.
The executable regression selection is `packages/channel-gateway/behavior.pytest`.
It includes the selected-click acknowledgement and exactly-once submission check.
Extraction must retain all assertions and controls.

This Draft records the proposed durable boundary for review. It does not accept
the decision or authorize subsequent runtime rewiring by itself.

## Rejected alternatives

1. Keep provider SDK imports in the neutral ports. This couples callers to the
   implementation and lets provider credentials cross the intended boundary.
2. Make HTTP reachability imply that a route is deliverable or buffered. This
   loses Slack's in-place sibling notice and missing-identity early refusal.
3. Give the gateway admission, approval or shared-storage authority. That changes
   the control plane and delivery semantics instead of extracting provider I/O.
4. Keep both old and new protocols during each move. Their independent evolution
   permits callers to retain the wrong contract and hides incomplete migration.
