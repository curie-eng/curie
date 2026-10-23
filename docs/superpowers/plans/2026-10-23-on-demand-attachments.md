# On-demand Attachments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Register attachments without reading content, then stream and cache only requested files without application byte ceilings.

**Architecture:** A separately reviewed optional ACI context carries per-logical-turn authority. An API catalog/facade authorizes requests to a private worker broker; runner tools verify and atomically publish downloaded files in a sandbox-local cache. Legacy eager mode remains isolated during rollout.

**Tech Stack:** Python, Pydantic, FastAPI/aiohttp, SQLAlchemy/Postgres, Valkey, Helm, generated Rust/TypeScript.

**Spec:** [Approved design](../specs/2026-10-23-on-demand-attachments-design.md), [ADR 0243](../../adr/0243-attachments-are-fetched-on-demand-through-scoped-platform-tools.md).

## Global Constraints

- No per-file or per-thread application byte ceiling in lazy mode.
- Preserve finite capacity, time, concurrency and metadata budgets.
- Channel credentials remain in the worker; context is in-memory per turn.
- The reviewed ACI contract PR precedes dependent merges; do not bump Curie release versions.
- Preserve legacy eager behavior and existing attachment-lane off switch.
- Use synthetic external-provider fixtures and isolated real Postgres/Valkey.
- No deployment, release preparation or Slack posting.

## Review Focus

- A steer renews the same logical turn; a late completed older turn cannot clear current authority (Tasks 1, 4).
- Response EOF without durable digest completion never publishes a path (Tasks 2, 3).
- A revoked route invalidates cache access as well as fresh transfer (Tasks 2, 3).
- Slow or disconnected clients close upstream reads within bounded memory/time (Tasks 2, 3).
- Older components explicitly refuse lazy activation rather than dropping context (Tasks 1, 4).

### Task 1: Independently reviewed ACI contract

**Files:** ACI events/session/exporters/version/public exports and their existing tests/generated artifacts.

**Interfaces:** Produce `AttachmentAccessCapability(url: str, token: str)`, optional `Event.attachment_access`, optional `BootEnv.attachment_access_enabled`, and `ATTACHMENT_ACCESS_STATUS_FIELD = "attachment_access"`. This contract implements no broker or runtime enforcement.

- [ ] Add failing codec/validation tests for missing/null context, valid pairs, wrong types/URLs, hidden tokens and tolerant old readers. Add BootEnv round-trip and reserved-name tests.
- [ ] Run focused ACI tests and record expected failures.
- [ ] Implement the optional fields, independent protocol patch bump, generated redacted Rust debug, and explicit lifecycle documentation.
- [ ] Regenerate schema, wire lock, env catalog and Rust/TS; run ACI tests, formatting/lint/type/export checks and generated-language compilation.
- [ ] Inspect all outgoing commits/text, open the separate contract PR against next and record its pinned head. This unlocks dependent development, not merge.

### Task 2: Catalog, scoped API facade and worker broker

**Files:** New API `attachment_access` modules/router/schema/migration and worker `attachment_broker.py`; existing registration/cleanup/config/run wiring.

**Interfaces:** Catalog-issued opaque IDs; active authority bound to agent/thread/logical-turn/route generation; authenticated internal broker request for one registered ref; streamed bytes plus separately authenticated completion descriptor `(transfer_id, sha256, size_bytes)`.

- [ ] Write failing production-path tests: registration reads zero bodies; paginated listing; idempotent registration; first digest bind/CAS conflict; cross-scope/refusal/reset; disconnect/timeout; incomplete completion. Use real isolated stores.
- [ ] Add additive catalog schema and lifecycle cleanup without changing legacy ledger shape. Implement facade authorization/leases, private broker current-binding fetch and streaming completion with bounded concurrency/backpressure.
- [ ] Verify zero provider reads for unauthorized requests/listing, bounded buffering with slow clients and stale-version conflicts. Keep API/worker credentials out of responses/logs.
- [ ] Run focused tests and applicable static/schema gates, record evidence and commit only task files.

### Task 3: Runner tools and local verified cache

**Files:** New runner attachment access client/cache/tool module; existing runner attachments/main/session/side-effect/tool-policy wiring and focused tests.

**Interfaces:** Consume Task 1 optional context and Task 2 catalog/transfer/completion. Produce platform `list_attachments`/`fetch_attachment` tools and verified local paths only.

- [ ] Add failing tests for lazy notice, zero boot body reads, single selected-file transfer above 32 MiB, verified cache reuse, malformed completion, partial stream, symlink/traversal, concurrent same-ID fetch and authority replacement/steer lifecycle.
- [ ] Implement a per-logical-turn capability holder; list with bounded metadata; stream into exclusive staging files, hash, validate completion, atomic publish and clean cancellation. Validate authority even on cache hits. Respect tool policy; keep fake model offline.
- [ ] Run focused runner tests, lint/type gates and commit. Do not claim live-model qualification from synthetic fixtures.

### Task 4: Worker delivery and Docker/Kubernetes activation

**Files:** Worker attachment coordination/kernel routing and capability lifecycle; runner status; chart worker/broker service/sandbox/values; local Compose/CLI seams where required; shared behavior vectors.

**Interfaces:** Consume Task 1 context/status and Tasks 2/3 runtime; feature activation requires compatible API/broker/runner. Legacy references never enter the lazy manifest unchanged.

- [ ] Add failing tests proving metadata-only startup on cold/warm/workspace/steered paths, zero unrequested content reads, no byte-based catalog omission above 256 MiB and explicit mixed-version refusal.
- [ ] Wire metadata registration/context mint and revocation while preserving kernel lock/finish/retry invariants. One owner changes kernel files.
- [ ] Add private broker networking and writable cache setup consistently in Docker/Kubernetes, finite storage/time/concurrency unchanged. Avoid eager init-container downloads in lazy mode.
- [ ] Exercise both substrates through shared vectors and isolated product wiring; run focused/static/docs/contract checks. Document opt-in transition and actual resource limitations.
- [ ] Pin each coherent dependent PR, retarget after contract merge and rerun exact-head required gates before any authorized merge.

### Task 5: Candidate review and completion evidence

- [ ] Run final-candidate focused tests, repository static/docs/contract gates and required fix pins if applicable; rely on exact-head CI for broad suites.
- [ ] Obtain fresh whole-candidate correctness/security review and scoped adversarial kernel review. Resolve important findings with a failing test then fix; record rulings/minor deferrals.
- [ ] Inspect complete outgoing history and PR bodies for private information, unrelated changes and release-version changes.
- [ ] Record implementation, local coverage, exact-head CI, merge state and deployment separately. Do not mark the feature complete while required integration evidence is missing.
