# HTTP invocation through current execution authority v1

Realization of [ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md), tracked in [#4890](https://github.com/curie-eng/curie/issues/4890). The initial receiver is the existing ordinary non-Grid Slack app-mention path. Caller integration, transport provenance, worker checks and activation form one supported path. Pure facts, role matches, signatures and successful lifecycle writes individually grant nothing.

## 1. Runtime ownership

<!-- @spec HTTP-INVOCATION-EXECUTION-1 -->

The operator initializer supplies immutable provider, receiver and core-context registrations. Configuration is startup authority, never request input or a platform-key registration endpoint. Provider registrations use the existing ProviderCredentialRegistry. Receiver registrations pin tenant, ChannelIdentity, ProviderInstallation, namespace descriptor, permitted binding IDs and independent source-account kind authority. Use exact existing IDs and opaque strings without normalization. An explicit human source-account entry is operator authority for that exact receiving scope; user profile flags, email, source=SLACK and first observation do not establish it. A missing entry supplies no independent human proof. Definitive resolved provider facts can separately establish their explicitly configured canonical kind authority.

Receiver signing and core context sealing use separate, independently generated 32-byte HMAC-SHA256 keys with explicit key ID and positive generation. They never reuse or derive from platform, worker, provider, approval-chat or snapshot credentials. Only the API receives core sealing keys. Only the particular receiver and API receive that receiver's key. Worker, runner, bundle and ordinary agent-secret paths receive neither. Initializers read protected operator files and feed validated in-memory registrations; no HTTP path can read, write, replace or mint their contents. Filesystem custody and effective mounts require separate qualification before activation. The runtime factory accepts immutable typed registrations and has no lazy request-time filesystem reads. Absent initialization makes configured HTTP mode unavailable.

Snapshots do not claim distributed revocation. Effective registration changes require coordinated process replacement under the participating-writer/cutover boundary. Generations are immutable; explicit retained verification keys may remain until affected work drains. Retired or missing keys refuse affected work. No hot reload or key-management service is introduced.

## 2. Receiver proof

<!-- @spec HTTP-INVOCATION-EXECUTION-2 -->

Require both the existing channel-enqueue credential and an independent receiver proof for HTTP invocation. The channel token supplies binding ID/generation/scope; current server rows own the tenant, agent, route and namespace. A platform-key-minted token alone cannot attest a sender. Platform-key-only, channel-token-only, missing/invalid receiver evidence and unsupported receiving scopes refuse before provider lookup, claim, quota or placeholder.

The bounded receiver proof binds its schema version, receiver ID/generation, exact receiving identity/installation, binding ID/generation, native Slack event ID, conversation/channel, existing SlackEvidence, and SHA-256 of the exact submitted TurnIn bytes. Capture evidence from the actual configured Socket Mode connection and native delivery. Reuse the existing Slack sender derivation, including positive non-Grid evidence, shared-channel and conflicting-team refusals. Independently pinned receiving scope and observed connection identity must agree. Platform-key-written installation reports alone cannot establish receiver trust. No request-supplied human flag, endpoint or namespace may replace the registered authority.

A proof is purpose-separated from a core context by its signed prefix. Parsing is strict and bounded: reject unknown versions/fields, duplicate JSON keys, nonfinite numbers, invalid UTF-8/surrogates, noncanonical base64url, invalid field types and oversized encodings. MAC comparison is constant-time. Fixed refusals never include supplied proof/key/provider bytes. Signing and parsing read no clock, network, datastore or environment. Repeated identical input is deterministic; a signature proves integrity under its registered purpose, not current permission.

## 3. Receiver lane and delivery identity

<!-- @spec HTTP-INVOCATION-EXECUTION-3 -->

An operator-selected HTTP receiver lane reaches API-owned enqueue before the legacy cached admission, dedupe, placeholder and direct-XADD path. It never falls through to cached open/stale decisions, API route-miss compatibility or direct enqueue. Initially support ordinary app_mention only; other turn-starting lanes on that selected receiver refuse explicitly until qualified. Existing unselected receiver behavior and its claim, placeholder, enqueue ordering remain unchanged. Do not introduce a second Socket Mode owner.

Preserve TurnIn and the frozen QueuedTurn contract. Send empty reply_ref in this lane, then let the verified API producer mint ReplyHandle.placeholder=None and the worker send its normal fresh reply. The adopted lane deliberately omits the courtesy placeholder. The API derives routing fields from the current binding and compares body.author with the verified native sender. Sign the full submitted content, including attachment references, without putting text or attachment contents in the proof.

Retries use identical submitted bytes, receiver evidence and channel generation. At most two submissions may resolve an ambiguous transport outcome within one fixed receiver deadline; never retry a definitive denial/unavailable answer or refresh evidence to salvage an old attempt. ACK-before-work remains the existing Socket Mode contract. This adds no intake outbox or guaranteed recovery of every failure before enqueue. Once queued, existing bounded worker delivery recovery applies.

## 4. Core caller composition

<!-- @spec HTTP-INVOCATION-EXECUTION-4 -->

Capture current authority from server-derived scope and resolve its exact immutable provider credential. Perform the existing two-second total-budget lookup outside SQL transactions/fences. Keep the original accepted response bytes. Under the prescribed global then tenant/IdP then agent/policy/binding/principal boundary, compare configuration, credentials, binding generation, policy and current activity. Apply HTTP principal lifecycle effects in the caller's transaction and positively acknowledge COMMIT before any enqueue authority is issued. Flush or an unknown COMMIT result is insufficient. Do not use the console active-human guard in place of the lifecycle owner; that would erase the independent raw-account exception.

After the acknowledged commit, acquire a new reader boundary. Re-read current configuration/lifecycle/policy/binding/registration scope, revalidate the same accepted bytes with fresh UTC time and public T1, and recompute current permission. No phase-two SQL writes or recaptured facts. Any changed capture, malformed state, unavailable provider, uncertain writer lifetime or expired observation refuses. Enqueue uses the qualified stable fence, exact complete T1 interval and original backlog reservation in the guarded delivery owner. Participating writer lifetime is a prerequisite, not inferred from a Lua comparison.

## 5. Roles and original association

<!-- @spec HTTP-INVOCATION-EXECUTION-5 -->

Use the stored versioned invocation filter and explicit role migration, retaining applicable team and binding/route restrictions. The pure role evaluator still reports permission=not_evaluated. Current inactive tenant/agent, invalid scope and forbidden kind remain global denials. A migrated exact surface-account selector can independently grant the same authenticated source account after definitive inactive/unresolved facts, with independent human proof and retained restrictions. Null/unrestricted is insufficient for unresolved facts, and provider unavailability never falls through. Distinguish an exact raw selector using public typed selector fields, not private evaluator helpers or a generic any-match result.

Resolved/inactive canonical facts must explicitly identify a human before lifecycle application or canonical/group permission. Service, agent, unknown or conflicting canonical kind supplies no such grant. Unresolved facts can use only the independently registered human source account and exact migrated raw selector. Independent source proof never upgrades an unknown canonical identity into a canonical/group grant.

Preserve the original PrincipalRef and canonical association as provenance. Re-evaluate current versions; do not require every recorded version to remain unchanged forever or treat console-session revocation as a raw-account global denial. A replacement canonical subject cannot inherit the old turn's canonical/group-only grant in this version; refuse that continuation until its policy is separately specified. A valid independent raw grant for the same authenticated source account remains applicable. Invocation permission supplies no approver, undo, recovery or later privileged-action authority.

## 6. Core context

<!-- @spec HTTP-INVOCATION-EXECUTION-6 -->

Seal a bounded immutable core context only after the complete caller check succeeds. Bind schema/purpose/key ID/generation; event and original invocation IDs; tenant/agent and exact binding/generation; receiving identity/installation and namespace; original sender and independent-human authority registration; receiver/provider credential references/generations; provider issuer/configuration and role-policy versions; resolution outcome; original canonical/principal association when established; and SHA-256 of the exact original queued payload bytes. Record no text, provider body or secret material. There is no generic worker/platform-key signing oracle.

The stream field is curie_identity_context_v1. The context remains provenance, never an offline permit. Every later boundary verifies it through core and rechecks current authority. Preserve the original wire digest separately from worker placeholder rewrites. Invalid/missing contexts cannot become legacy work. Contexts and keys are excluded from logs and model-visible fields; QueuedTurn and PrincipalRef acquire no new field.

## 7. Guarded transport v2

<!-- @spec HTTP-INVOCATION-EXECUTION-7 -->

Extend the delivery owner with explicit identity_guard_v2, preserving v1 and legacy/hook layouts. The five keys are claim, stream, fence, original reservation marker and a dedicated retained receipt. The first fourteen arguments retain v1 positions except the explicit v2 mode; arguments fifteen through seventeen are the context, a 64-lowercase-hex intent digest and a 64-lowercase-hex context digest. The fixed context field is appended with payload and optional existing traceparent in one XADD. No arbitrary metadata map or second context publish.

Validate the whole layout, bounded context, digests and five distinct keys before mutation. Before an append require the same fence/marker/TIME conditions as v1. Atomically retain the permanent stream receipt and canonical JSON metadata {schema_version:1,intent_digest,context_digest,stream_id} using the existing MSET receipt pattern. Intent binds original signed receiver claims and body, so same-delivery conflicting content cannot masquerade as a newly accepted retry. An exact historical receipt is a duplicate, not fresh permission. Missing, conflicting or malformed v2 receipt metadata refuses rather than replacing it. Unknown datastore outcomes retain existing settlement duties. This adds no SQL admission ledger.

## 8. Current core operation

<!-- @spec HTTP-INVOCATION-EXECUTION-8 -->

Publish strict internal DTOs in curie-internal and a core-owned POST /v1/internal/identity/turn-authority/check operation using the existing internal-worker credential. The credential authorizes asking core; it cannot attest users, register trust or mint contexts. A request binds fresh request ID, stage (prepare, claim_or_resume, start, steer), opaque context, exact original payload digest, current event ID, expected tenant/agent/binding/generation and optional approval ID. Routing values are equality expectations, never caller authority.

Respond with continue, bounded refuse or unavailable, echoing request/stage/event/context digest and exact binding scope. Continue applies only to that boundary invocation. Verify the context and current registered source, then run the same fresh core composition; use no positive decision cache. Recorded versions establish provenance, while current policy/configuration is evaluated afresh. A change during the current check refuses, not a salvaged old result. The worker client uses one total five-second deadline, one attempt and no redirects. Malformed/mismatched bodies, 401/404, transport errors or timeout start nothing and never mean legacy allow. Caller cancellation remains cancellation.

## 9. Worker enforcement

<!-- @spec HTTP-INVOCATION-EXECUTION-9 -->

Inject one supported client through existing Kernel construction; worker imports no API implementation. Carry per-event verified context beside the original QueuedTurn, never one allow per conversation. Extend the binding owner with exact binding identity/generation/tenant and the authoritative HTTP-context-required discriminator. SQL NULL absence differs from JSON null or malformed configuration; unavailable classification refuses. A known adopted binding never reaches a sibling/generic fallback, and recognized contexts always require core even after mode changes.

Check before boot_env, workspace/attachment credentials or approval grants; at actual claim/resume after preparation/wait/retry; at the top of the common start helper before memory/channel/attachment capabilities; and before the distinct steer capability/start path. Common start covers attachment handoff and steer-409 new-start fallback. Reclaim and capacity redelivery keep the same original context. Preserve lease/thread ownership, side-effect flags, existing max_attempts/max_delivery and substrate behavior. Definitive refusal uses the existing terminal outcome and only a verified reply destination. Unavailable leaves execution unstarted and uses existing bounded delivery/dead-letter handling, without an unlimited retry or successful ACK. A network check is not atomic with remote allocation or scheduling and does not interrupt already executing actions.

## 10. Suspension continuity

<!-- @spec HTTP-INVOCATION-EXECUTION-10 -->

Retain one nullable private sealed-context field on existing Approval, verified against dedupe_key, agent, conversation and route at creation. Identical retries retain it; conflicting provenance refuses. No editable approval authority or admission ledger. Core builds a fresh flat notification-resume context from committed approval status and new payload/event, preserving the original invoker independently of resolved_by/system attribution.

The initial invocation path refuses approved/grant-bearing HTTP resumes before grants or boot. Rejected/expired notifications can continue only after independently qualified current invoker checks and carry no approval grant. Current answering-actor authority and exact effect scope require a separate realizing specification before approved resumes activate. Legacy approvals keep their behavior. Capacity waits preserve complete fields. Payload-changing successors need core rebinding; unsupported HTTP successors refuse rather than dropping context.

## 11. Activation

<!-- @spec HTTP-INVOCATION-EXECUTION-11 -->

HTTP activation requires explicit policy migration, qualified provider mapping/kind and independent identity/membership source freshness, credential and receiver/core-key custody, participating writers and all enabled receiver/worker/resume paths. Clear existing provenance-free queued, pending, capacity-wait, suspended and in-flight work before switching. No automatic cancellation, approval conversion or fabricated backfill. A coordinated quiescent transition must prevent old producers/consumers from racing a single empty-state snapshot. Start controlled qualification with an owned all-new installation; this does not claim mixed-old-binary or arbitrary online migration support.

Unsupported surfaces, unavailable qualification or missing registrations refuse. Expose exact effective scope and remaining unsupported grant paths in readback. Neither module tests, fake-model replies nor code merge qualify actual provider freshness or a production installation.

## 12. Independent verification

<!-- @spec HTTP-INVOCATION-EXECUTION-12 -->

Independent tests precede source. Use real owned PostgreSQL/Valkey for lifecycle COMMIT/read-fence/append races, both refund orders, exact/conflicting receipts, context transport, capacity and approval persistence. External Slack/provider/runner boundaries may be synthetic and labeled accordingly. Prove the platform-key mint oracle is closed, every signed field/body digest is bound, key purposes and receiving scopes differ, and malformed/unknown/retired contexts never downgrade.

Cover current policy/configuration/binding/credential changes during lookup and preparation; independent raw grants under inactive/unresolved/replacement facts; retained restriction denials; canonical/group replacement refusal; every named worker gate including attachment, steer/409 and retries; unsupported approval grants; notification restart continuity; missing/corrupt mode classification; and existing legacy placeholder/hook/channel behavior. Keep original independent assertions through integration. Focused local checks and exact-head required CI are separate from the owned real-provider/receiver qualification. Report caller, approver, later privileged-action and production activation evidence separately.

## 13. First codec boundary

<!-- @spec HTTP-INVOCATION-EXECUTION-13 -->

The shared curie-internal identity_context module owns the following pure codec. It imports no application implementation. All wire models are strict, extra-forbidden, frozen and revalidated, with immutable nested values. Every declared field is present on the wire, including explicit nulls. Integers are actual integers rather than booleans, with range 1 through 2^63-1 unless stated otherwise. IDs named UUID use canonical lowercase UUID spelling. Other identifiers preserve their original Unicode strings, reject unpaired surrogates and have one through 256 characters, with the following explicit exceptions: conversation_id has at most 1,024, namespace authority has zero through 2,048 and issuer has one through 2,048. The empty authority is the existing ordinary non-Grid Slack value. No string normalization.

Use the workspace's existing Pydantic dependency and declare it directly for curie-internal without upgrading resolved dependencies. An owned probe of Pydantic 2.13.5, `BaseModel` with `ConfigDict(strict=True)` and a `Literal[1]` field, observed that model_validate({'version':True}) accepts 1. Therefore schema_version needs an explicit actual-integer check; strict model configuration alone does not meet this contract.

ReceiverProofV1 fields:

| Field | Type |
| --- | --- |
| schema_version | integer literal 1 |
| receiver_id, channel_identity_id, provider_installation_id, binding_id | UUID strings |
| receiver_generation, binding_generation | positive bounded integers |
| delivery_id, native_channel_id | identifiers |
| conversation_id | identifier with its stated bound |
| body_sha256 | exactly 64 lowercase hexadecimal characters |
| slack | ReceiverSlackEvidenceV1 |

ReceiverSlackEvidenceV1 has the existing SlackEvidence field names: delivery is an identifier; user_id, event_user_team, event_team and interaction_user_team_id are nullable identifiers; is_ext_shared_channel is strict boolean or null; enterprise_ids is an immutable tuple of at most 32 identifiers. The codec verifies grammar, not Slack sender derivation or human kind. API composition still uses the existing Slack owner.

InvocationScopeV1 has tenant_id, agent_id, binding_id, channel_identity_id and provider_installation_id as UUID strings; binding_generation as a positive bounded integer; provider and native_id as identifiers; and namespace as ContextNamespaceV1. ContextNamespaceV1 has UUID id, bounded authority, kind and key. ContextCanonicalV1 has bounded issuer and subject identifier. ContextPrincipalV1 has UUID principal_id and positive bounded authorization_version.

InvocationContextV1 fields are schema_version=1, purpose='invocation', key_id, key_generation, event_id, original_event_id, scope, receiver_id, receiver_generation, independent_human, provider_issuer, provider_version, provider_credential_id, provider_credential_generation, lifecycle_generation, role_policy_version, outcome, canonical, principal and payload_sha256. key_id is one through 64 ASCII letters/digits/underscore/hyphen; key_generation and all version/generation fields are positive bounded integers. scope is InvocationScopeV1, receiver_id is a UUID, independent_human is strict boolean, provider_issuer uses the issuer bound, event/original IDs and credential ID use identifier bounds, and payload_sha256 is a lowercase digest. outcome is resolved/inactive/unresolved; canonical is ContextCanonicalV1 or null, principal is ContextPrincipalV1 or null. Resolved/inactive requires canonical; unresolved requires both associations null. A principal requires canonical. An invocation's original_event_id equals event_id. No notification or grant purpose is accepted by this first codec.

IdentityKeyring is an opaque immutable, nonserializable, redacted object, not a boolean permission. build_identity_keyring(purpose, registrations) takes purpose 'receiver' or 'context' and an actual tuple of at most 256 (key_id, generation, material) tuples. Lookup pairs (key_id, generation) must be unique; reject identical duplicates and conflicting material for the same pair. Key IDs use the grammar above; material is an actual bytes value of exactly 32 bytes. Preserve immutable key generations, detach registrations and reject all invalid inputs with fixed ValueError('invalid_identity_keyring'), without supplied values or exception chaining. Empty keyrings are valid and cannot sign or verify. A ring exposes no material or serialized registration inventory. Receiver proof signing/verification selects only the exact (receiver_id, receiver_generation) pair; the receiver UUID doubles as its key ID. Context signing/verification selects only (key_id, key_generation). There is no implicit current key, try-all or generation fallback.

sign_receiver_proof(proof, keyring) and seal_invocation_context(context, keyring) return the deterministic token or raise fixed ValueError('invalid_receiver_proof') / ValueError('invalid_identity_context'). verify_receiver_proof(token, keyring) and open_invocation_context(token, keyring) return their validated model or ContextRefusal with the corresponding code. ContextRefusal, keyrings and returned proof/context models reject boolean conversion because none is a permission. Builders/signers revalidate supplied model instances, including unchecked model_copy results. Wrong object types, wrong ring purpose, absent key/generation and parsing/verification defects use those fixed outcomes; no key/token/parser content is included.

The token has three ASCII segments: purpose prefix, unpadded canonical base64url of canonical JSON, and unpadded canonical base64url HMAC-SHA256. Receiver prefix is curie-receiver-v1, context prefix is curie-context-v1. MAC input is the ASCII prefix, a dot and the payload segment. Use the existing shared base64url and signature primitives where applicable; material stays bytes and HMAC uses it directly. Canonical JSON is UTF-8, sorted keys, compact separators and ensure_ascii=False, containing the complete validated wire model. Verify exact canonical spelling, including payload and signature base64url. Maximum whole token is 16,384 ASCII bytes inclusive; reject nonstrings and larger encodings before decoding. Reject malformed segment counts, invalid alphabets, duplicate JSON keys, nonfinite values, unknown fields/versions/purposes and noncanonical alternate JSON. MAC comparison uses compare_digest. No clock, environment, network or datastore reads, and no expiry or permission claim, belong in this codec. Runtime ownership, current registration scope and all current authority checks remain the consuming core's obligation.
