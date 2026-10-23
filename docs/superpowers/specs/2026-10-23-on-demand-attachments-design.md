# On-demand attachment delivery without application byte ceilings

Date: 2026-10-23

Status: Proposed; awaiting approval of the written design

## Intent and authorization

The maintainer selected removal of both per-file and per-thread byte ceilings,
then selected on-demand retrieval with streaming and caching instead of eagerly
downloading every attachment before an agent starts. This replaces the earlier
proposal that removed byte ceilings while preserving eager delivery. Approval
of the written design and its new ADR remains pending.

The agent starts with a catalog, downloads only files it needs, and reuses
verified local copies. File size alone does not reject or omit a catalog entry.
A text-only follow-up does not download the thread's attachment history.

## Existing constraints

`apps/worker/src/curie_worker/attachments.py` already streams channel bytes
through `WorkspaceObjectPort.put_stream`. The adapter in
`apps/worker/src/curie_worker/workspace.py` can assemble all remaining bytes
when `_IteratorReader.read(-1)` is called; bounded memory must be verified
through consumers, not inferred from the iterator signature.

`apps/worker/src/curie_worker/kernel/attachments.py` resolves content before
claim/handoff. The API ledger requires an actual digest, so it cannot register
an unfetched file. `apps/worker/src/curie_worker/sandbox/docker.py` and the
init program in `charts/curie/templates/agent-sandbox.yaml` materialize files
before runner startup. Runner manifests and notices describe files already on
disk. Lazy references cannot be passed through that old manifest unchanged.

`apps/api/src/curie_api/routers/thread_attachments.py` is worker-only. Preserve
that restriction: a sandbox cannot plant arbitrary file IDs in a ledger to
make the worker fetch them with channel credentials.

## Approaches

1. Raise/remove numeric limits, retaining eager preparation: rejected because
   cold-boot work and complete local materialization remain.
2. Intercept filesystem reads through a mount layer: rejected for this change
   because it adds runtime/privilege dependencies across both substrates and
   hides retrieval errors behind ordinary reads.
3. Explicit platform tools with streamed retrieval and local caching: selected
   product direction. The component choices below are implementation proposals.

## Agent interface

Expose `list_attachments` and `fetch_attachment` on the platform tool server.
Listing returns safe names, opaque IDs, current/earlier-message provenance,
MIME type, reported/verified size when known and local cache status. Paginate
metadata; metadata response bounds are not file byte ceilings.

Fetching takes only a catalog-issued ID, never a URL, channel file ID, agent ID,
thread key or destination path. It returns a verified absolute path and actual
size after complete transfer. Notices distinguish a remote reference from an
existing local file. Respect runner tool policy and approval; names alone grant
no exemption. Fake-model runs stay offline and mount no network attachment tool.

This version retrieves the complete selected file. It does not add generic
range reads, file parsing or automatic file-content insertion into model text.
Some file formats/tools still require a complete local copy.

## Reference catalog and integrity

Add a separate API-owned reference catalog with an additive schema migration;
preserve the legacy digest-required ledger/readers during rollout. Scope each
record to agent, canonical thread, event and ordinal. Assign a stable opaque ID
and collision-safe filename. Register trusted inbound references before the
model starts without downloading content; replay is idempotent. Store route
kind, adapter and identity, never credentials, endpoints or provider URLs.
Registration failure is explicit.

An unfetched reference has no verified digest, and any initial size is marked
provider-reported. First successful fetch atomically binds actual digest/size.
Use a version/CAS predicate: on conflict, re-read and accept only the identical
digest; otherwise refuse. Subsequent fetches must match. Never invent a digest
for an unfetched file. Import legacy references idempotently with their digest
and filename. Catalog retention, thread reset and deletion follow transcript
scope. Existing metadata/file-count bounds remain, without total-byte filtering.

## Authority and streamed transfer

Keep channel credentials in the worker. Add a private worker attachment broker
using existing file clients, behind a scoped API facade. The facade verifies
runner authority and resolves the registered reference; a purpose-bound internal
request authorizes the broker to fetch exactly it. The runner cannot call the
broker or internal ledger directly. Resolve current bindings, endpoints and
credential identity at request time. Do not copy channel credentials to the API
or runner. Revoked routes refuse even when a local copy exists.

The API facade streams the broker response with backpressure, without buffering
the complete body, spooling another copy or returning provider/object-store URLs
to the model. Bound concurrent transfers per sandbox and globally, control-body
sizes, retries and transfer time. No per-file or thread-total byte cutoff.
Bound buffering to a fixed small number of chunks per active transfer. Client
disconnect, cancellation and expired authority promptly close upstream reads.
Catalog listing and rejected requests read no provider file bodies.

Use a transfer ID and completion record: EOF alone is not success. The broker
reports digest and actual size through an authenticated completion path and
commits catalog integrity before reporting completion. The runner hashes its
bytes, verifies the completion descriptor, and only then publishes the local
file. Premature EOF, changed digest or a failed integrity commit produces no
usable final path. A bounded status request can recover a lost completion
response within the same transfer deadline.

Revalidate active turn/session, route generation, agent/thread scope, reference
membership and transcript lifetime before provider access and before completion.
Turn end, cancellation, route replacement and reset revoke subsequent access.
Different agents/threads/sessions cannot reuse each other's capability or cache.
Capabilities, internal tickets, provider URLs and channel IDs never enter model
text, tool results, logs or portable history.

## Cache and resources

Use the attachments volume as a sandbox-local cache keyed by authorized
reference and expected digest. Stream into an exclusively created staging file;
atomic rename follows verification. Assign filenames server-side and defend
against traversal, symlinks and collisions with safe filesystem operations.
Serialize simultaneous fetches of one ID. A verified cache hit avoids provider
download, but still checks authorization and local integrity before returning.

Cache lifetime is the sandbox's. A cold sandbox refetches only requested files.
This change adds no persistent shared blob cache and does not pre-stage all
content in the object store. Legacy staged objects retain their cleanup rules.

Keep finite sandbox volume size, ephemeral-storage limits, namespace quotas,
concurrency, deadlines and retention. Cache exhaustion can evict unpinned entries;
a file that cannot fit fails explicitly. Unlimited size policy is not unlimited
capacity. Never truncate silently or return a successful partial path.

## Frozen protocol and rollout

Propose an optional BootEnv feature flag for attachment tool availability and
an optional per-event scoped attachment context analogous to channel-read
context. Replace authority on a new logical turn; a steer renews the live
opener's authority rather than revoking its in-flight reads. Track authority
by logical turn, including late completion from an older turn; boot authority
must not become thread-wide read authority. Context carries a
platform origin and opaque capability; the server derives scope. Do not
repurpose the version-1 disk-present, names-only attachment manifest.

The frozen-ACI dependency is raised in
[#5005](https://github.com/curie-eng/curie/issues/5005).
Land a separate reviewed compatible
contract PR with schema/TS/Rust generation and the appropriate independent
protocol version before dependent lanes merge. Once its head is pinned,
dependent implementation may use stacked draft PRs. Do not bump Curie's Cargo
or Helm release versions for schema compatibility.

Lazy mode is explicit and needs compatible API, worker, runner and broker
capability negotiation. Mixed versions refuse activation with an actionable
error rather than silently using eager downloads or ignoring context. Preserve
legacy eager mode during migration and the existing attachment-lane off switch;
do not enable any deployment in this task. Lazy mode ignores application byte
caps; legacy mode retains its positive limits. Document transition and check
that HTTP proxy/client settings do not silently restore the old byte cutoff.

## Architecture approval and ownership

A new Draft ADR proposes partial amendments to ADR 0153's eager complete-set
handoff and ADR 0205's rebuild/mandatory-byte-budget/disk-present rules. Register
current-message metadata together; deliver each requested file completely or
report failure. Do not describe every reference as delivered. ADR 0059's finite
sandbox resource envelope stands.

Realizing areas: worker coordination and private broker; API catalog, scoped
facade and migration; runner attachment tools/cache/notices; optional ACI fields;
chart and local networking/volumes. The attachment routing change in
`apps/worker/src/curie_worker/kernel/attachments.py` is intended scope. One owner changes that module and
preserves locks, steer/finish behavior and side-effect retry classification.
Require its adversarial review before merge.

Approval requested: approve this written design and authorize its amendment ADR
to land Accepted alongside its realizing implementation under the coordinated
exception. This does not waive separate ACI contract review, exact-head CI,
release ownership or deployment authorization.

## Acceptance evidence

Use production wiring and synthetic external-provider fixtures. Do not mock
Postgres or Valkey; isolate test resources and clean up only task-owned resources.

1. File-carrying messages start after registration with zero provider content
   reads. Listing and text-only follow-ups read zero file bodies.
2. Fetching one ID reads no unrelated files; a repeat uses verified local cache.
   A cold sandbox downloads only files requested again.
3. A generated file above 32 MiB succeeds. Catalogs above 256 MiB are not
   filtered, and requested transfers above the former aggregate budget work.
   Measure bounded buffering through slow production consumers/producers,
   without launching an unbounded resource campaign.
4. Cross-thread/agent/session, forged, expired or stale-route contexts, reset
   transcripts and revoked bindings refuse before provider body reads.
   Cancellation stops slow streams within the same transfer budget.
5. Partial streams, deleted files, cache exhaustion, traversal/symlinks,
   concurrent fetches, digest changes and stale-version CAS conflicts cannot
   produce a wrong-scope or accepted partial path.
6. Warm/steered/cold turn authority reaches the active tools. Mixed-version
   activation refuses. Fake-model mode stays offline. Shared vectors exercise
   actual Docker and Kubernetes behavior.
7. Focused behavior tests fail before implementation and pass afterward.
   Run applicable local lint/type/import/docs/chart/contract checks. Exact-head
   PR CI proves broader suites; no production or Slack testing is authorized.

## Builder decisions

Choose route names, internal port, opaque-ID encoding, capability encoding,
chunk sizes, bounded concurrency defaults and pagination sizes from existing
conventions. Select additive migration IDs from current train heads and honor
the schema window. These choices must preserve the outcomes above.
