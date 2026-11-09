# 275. A channel gateway owns provider authentication and I/O

Date: 2026-11-07

Status: Accepted

Approved by the maintainer on 2026-11-09.

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
valid package. Provider composition lives in
`curie_channel_gateway.slack.read_server.application`.

Slack intake is fully owned by `curie_channel_gateway.slack`: admission caching,
Redis/Valkey construction, process heartbeat and the one-shot CLI producer live
there alongside the handlers. The dispatcher image starts
`python -m curie_channel_gateway.slack.run`; its Python package retains only
deployment metadata. The shared stream codec, publication and dedupe transport
live in `curie_internal.turn_queue`, so worker consumers need no provider import.
The `QueueConfig` protocol consumes the existing stream, dedupe key and TTL
settings without importing Slack configuration. Admission rules, cache and queue
keys, heartbeat timings and Redis/Valkey construction are preserved.

The import-linter independence contract forbids imports in either direction
between `curie_channel_gateway` and `curie_dispatcher`, including indirect and
`TYPE_CHECKING` imports. The existing lint and pre-push paths run it. Executable
controls exercise both directions and a valid shared-package dependency.

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

The source extraction preserves provider-visible operations, but moving token
custody into the gateway changes runtime configuration and credential access.
U2 moves Slack intake and mail while
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

Existing Helm Slack installations keep their values and Secret objects. The
chart automatically deploys the gateway, preserves bot-token Secret references,
removes raw tokens from API and worker environments, and gives those callers
separate gateway credentials. Chart-managed TLS certificates persist through
Helm lookup; operator-managed certificates remain available. Explicit gateway
disable without an external URL refuses rendering. Optional new capabilities
remain off until configured. `charts/curie/templates/_channel-gateway.tpl` and
`charts/curie/templates/channel-gateway-tls.yaml` in the chart realize that upgrade wiring.

Protected-worker process files require the offline
`curie migrate-protected-process` migration from v1 to v2. Runtime accepts
only v2 and names the migration when rejecting v1. Custom launchers must move
provider credentials into the gateway and configure their caller credentials.
This is not a behavior-neutral runtime upgrade for those launchers.

## Rejected alternatives

1. Keep provider SDK imports in the neutral ports. This couples callers to the
   implementation and lets provider credentials cross the intended boundary.
2. Make HTTP reachability imply that a route is deliverable or buffered. This
   loses Slack's in-place sibling notice and missing-identity early refusal.
3. Give the gateway admission, approval or shared-storage authority. That changes
   the control plane and delivery semantics instead of extracting provider I/O.
4. Keep both old and new protocols during each move. Their independent evolution
   permits callers to retain the wrong contract and hides incomplete migration.
