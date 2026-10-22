# Conditional read-only adoption for authenticated hooks

Related: #3603. This is producer adoption of the existing TOOL-ACCESS contract
in `docs/interfaces/aci-producer/INTERFACE.md`, implemented by #3582, #3590
and #3591. It introduces no ACI or plugin-format member.

`POST /hooks/{agent_id}/{hook}` accepts optional query `tool_access=read-only`.
Omission keeps the existing null policy and ordinary approval behavior. The
signed hook sender selects this restriction outside the untrusted request body;
payload text cannot select it. A narrowed query is an opt-in request, not a
mandatory source policy. The existing signature authenticates the body, so
operators must protect the configured request URL and opt in on every delivery.

A newly accepted delivery carries the selected value on `QueuedTurn.tool_access`
and returns it as `HookAccepted.tool_access`. The receipt proves the queued
policy, not investigation, runtime enforcement, or reply delivery. A completed
duplicate returns the original stored policy only when it matches the retry's
policy. A mismatch returns 409 without changing the first claim or enqueueing.
For a legacy delivery with neither retained receipt metadata nor its original
queued payload, no policy can be attested and the retry returns 409. Pending restricted retries
also return 409; existing ordinary pending retries retain their 202 receipt,
whose null policy and unknown conversation do not attest an accepted turn.

Before submitting a restricted turn, the operator must establish homogeneous
worker artifacts implementing TOOL-ACCESS-6 and compatible runner artifacts.
API OpenAPI support, ACI protocol version and consumer heartbeat capability are
not worker enforcement evidence. The worker checks the exact target runner's
advertisement before delivering the restricted event. Old or mixed worker
fleets remain an installation blocker: this change does not automatically
identify or exclude them before enqueue, and does not close #3603. Automatic
fleet admission and mandatory source policy require separate reviewed design.

Tests first verify authenticated selection, default null, ignored payload
policy, invalid policy, bad signature, completed duplicate agreement and both
mismatch directions, and unknown original-policy refusal. Tests use isolated
real Postgres and Valkey. Runtime verification must exercise the local hook,
queue, worker and runner path, plus cluster CI. Skill is not applicable because
it bypasses API ingress; local-release is not applicable because no released
artifact or install command changes. Live-provider and external integration
are not applicable to this narrow producer change: it changes neither model
invocation nor PreToolUse/MCP classification or provider request shape. Existing
runner enforcement evidence remains a prerequisite, not new evidence from this
API receipt. A draft PR records any outstanding runtime gates explicitly.


## Retained ordinary receipts (ADR 0237)

Maintainer Junwon Jung accepted [ADR 0237](../../adr/0237-ordinary-hook-receipts-preserve-policy-after-stream-retention.md)
on 2026-10-19. The following requirements amend only ordinary hook receipts;
protected admission, authentication, source activation exclusion, delivery IDs
and quota accounting retain their existing contracts.

- `HOOK-RECEIPT-1`: An ordinary successful owned enqueue writes its existing
  permanent stream-ID claim and permanent receipt metadata in one atomic Valkey
  operation with XADD. The metadata key is the claim key plus `:receipt`.
  Channels and other callers that request no metadata preserve their existing
  enqueue behavior and create no metadata key. No producer trims its stream.
- `HOOK-RECEIPT-2`: Metadata is one strict UTF-8 JSON object with exactly
  `version` (integer 1), `stream_id` (canonical decimal Valkey stream ID),
  `conversation_id` (nonempty string), and `tool_access` (explicit null or
  `read-only`). Unknown or duplicate members, missing members, booleans as a
  version, nonfinite numbers and invalid UTF-8 refuse. Each metadata record is
  at most 16384 bytes, including the eventual stream ID. Encoding checks a
  conservative maximum stream-ID allowance before claiming or enqueueing;
  an overlarge receipt returns 422 without claim, quota or queue mutation.
  No raw payload, header, credential, source generation or transcript is stored.
- `HOOK-RECEIPT-3`: A completed duplicate atomically observes the current claim
  and bounded metadata. It requires that claim still equal the observed held
  stream ID and that the parsed metadata stream ID equal the claim. A malformed,
  overlarge or non-string metadata key, non-string or changed claim, or policy
  mismatch returns 409 without changing claim, metadata, quota or stream. The
  metadata size is checked before loading and parsing it. Valid retained proof
  returns the original conversation and policy even after settled retention.
- `HOOK-RECEIPT-4`: Absent legacy metadata uses the original queue entry as before.
  A missing or unverifiable original entry refuses with 409. Legacy metadata
  is never reconstructed from a retry. Pending semantics remain ordinary 202
  and restricted 409. An orphan metadata key cannot be overwritten to admit a
  fresh delivery when its claim is absent or pending; it refuses without XADD.
- `HOOK-RECEIPT-5`: Concurrent winners, expired leases, foreign successors and
  lost successful enqueue responses preserve one enqueue and one immutable
  receipt. Claim and metadata are set together; no metadata is exposed as proof
  without a matching permanent claim. Failed attempts preserve completed claims
  and their quota charge. Storage errors never become successful acknowledgements.
- `HOOK-RECEIPT-6`: Regression tests cover real signed HTTP requests, acknowledged
  entries removed by the production settled-retention algorithm, both policy
  mismatch directions, strict metadata refusal, legacy fallback and refusal,
  pending claims, authentication, and shared channel/protected enqueue paths.
  A duplicate proves prior acceptance only; it never schedules a periodic
  investigation of a still firing alert.
