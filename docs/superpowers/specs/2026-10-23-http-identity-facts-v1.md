# HTTP identity facts v1

Facts-only realization of [Accepted ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md),
tracked in [#4890](https://github.com/curie-eng/curie/issues/4890).
This contract publishes representations and pure validation; it enables no
identity provider or authorization consumer.

## Request and exact scope

<!-- @spec HTTP-IDENTITY-V1-1 -->
All fields below are required; null is permitted only where stated. Objects
reject unknown fields. Integers are JSON integers, never booleans, strings or
fractional numbers. UUIDs are lowercase hyphenated canonical UUID strings;
no particular UUID version is required. The entire string must match, including
rejection of trailing newline characters. Opaque identifiers are nonempty,
case-sensitive strings, compared without trimming, case folding or inference.
No identifier or collection ceilings are added beyond the response-byte bound.

```text
LookupRequestV1 = {
  schema_version: 1,
  request_id: UUID,
  purpose: "invocation" | "approval",
  authority_version: integer >= 1,
  scope: {
    tenant_id: UUID,
    expected_issuer: issuer,
    sender: {
      provider: opaque,
      channel_identity_id: UUID,
      provider_installation_id: UUID,
      namespace: {id: UUID, authority: string, kind: opaque, key: opaque},
      native_id: opaque
    }
  }
}
```

`authority_version` is the caller-captured authority configuration generation,
not a provider-chosen revision or an administrative write schema. The issuer
uses the existing directory issuer grammar: HTTPS with a hostname, no
userinfo/query/fragment or surrounding whitespace. It remains an exact string,
without URL normalization. Namespace authority may be empty, as existing
trusted installations permit; it is still echoed exactly.

The caller supplies the authenticated, server-scoped sender and captured
configuration. A response must echo request_id, purpose, authority_version and
the complete scope exactly, comparing object members independent of JSON key
order. A provider cannot substitute tenant, issuer, installation, namespace or
native account. The request contains no message text, display author, local
principal UUID, grant or credential. T1 does not derive or authenticate sender
evidence; eventual consumers must establish it before constructing a request.

## Response and outcome union

<!-- @spec HTTP-IDENTITY-V1-2 -->
```text
LookupResponseV1 = {
  schema_version: 1,
  request_id: UUID,
  purpose: "invocation" | "approval",
  authority_version: integer >= 1,
  scope: LookupRequestV1.scope,
  outcome: "resolved" | "inactive" | "unresolved" | "unavailable",
  identity: CanonicalIdentityV1 | null,
  sender_kind: "human" | "service" | "agent" | "unknown",
  identity_observation: ObservationV1 | null,
  membership: MembershipV1 | null
}
CanonicalIdentityV1 = {
  issuer: issuer, subject: opaque,
  kind: "human" | "service" | "agent",
  account_status: "active" | "disabled" | "revoked"
}
ObservationV1 = {as_of: UTC timestamp, max_source_lag_ms: integer >= 0}
MembershipV1 = {
  complete: true,
  groups: [{issuer: issuer, id: opaque}, ...],
  observation: ObservationV1
}
```

| Outcome | Identity | Sender kind | Identity observation | Membership |
| --- | --- | --- | --- | --- |
| resolved | Active canonical identity | Exactly identity.kind | Required | Required complete set, including an empty set |
| inactive | Disabled or revoked canonical identity | Exactly identity.kind | Required | null |
| unresolved | null | human, service, agent or unknown | Required negative-lookup observation | null |
| unavailable | null | unknown | null | null |

Canonical and group issuers must equal scope.expected_issuer exactly. Group
pairs `(issuer, id)` are unique; order conveys no meaning. Partial, paginated
or truncated membership has no representation. Contradictory or omitted union
fields are invalid. All structurally valid human/service/agent facts are
accepted irrespective of purpose; unknown is valid only in the two union
rows permitting it. Kind validation grants no capability or policy eligibility.
An unresolved result creates no canonical identity. Unavailable is a
structurally valid envelope with no usable identity or membership.

## Strict bytes and validation result

<!-- @spec HTTP-IDENTITY-V1-3 -->
The pure validator accepts a byte sequence, a validated captured request,
required freshness limits and caller-supplied UTC `now`. It performs no I/O.
At most 65,536 decoded entity-body bytes are accepted, including JSON
whitespace; 65,537 is invalid. HTTP decompression/streaming is outside T1.
Parse strict UTF-8 JSON without a BOM: reject duplicate keys at every nesting
level, non-finite numbers, unpaired Unicode surrogates, invalid syntax and
trailing non-whitespace. Do not coerce scalar types, UUIDs or enum values.
Schema version is the integer 1 on both request and response.

The existing facts schema module owns the shared internal byte-decoding stage
`decode_json_bytes(data: bytes, *, max_bytes: int) -> Any`. Its explicit byte
bound is inclusive and must be an actual nonnegative integer; a boolean,
noninteger or negative programmer argument raises ValueError before decoding.
Zero admits no valid JSON document. Preserve the existing bytes-instance
acceptance and strict decoding grammar above. Decimal/exponent tokens retain
an opaque noninteger marker until the caller's version/model stage; the marker
is not a public value API. Size-fitting integer tokens retain arbitrary integer
precision without changing the process-wide digit guard. The helper returns
untrusted decoded values and may raise raw decoding exceptions; it performs
no version, context, model or refusal classification. The facts validator uses
its unchanged 65,536-byte bound and validates caller context before invoking
this stage. Each consuming boundary owns its existing sanitized refusal and
validation order; exposing this helper creates no HTTP surface or schema change.

Validation order is byte/JSON validity, supported version, strict shape/union,
request correspondence, then identity freshness and membership freshness.
Failures produce `FactsRefusal` with only a stable code: `invalid_response`,
`unsupported_version`, `scope_mismatch`, `identity_stale`,
`membership_stale` or `unavailable`. A structurally valid unavailable envelope
returns FactsRefusal(code="unavailable") after structural and correspondence
validation. A noninteger version is invalid_response; another integer
version is unsupported_version. No provider body, sender, credential or raw
exception is returned. Invalid caller context is a caller error raised before
response parsing, not an unresolved provider outcome.

Success produces `ValidatedFacts`: the validated request context, outcome,
optional canonical identity, sender kind, optional complete group set,
observations and applicable expiry instants for a definitive outcome.
Neither result contains `allowed`, a role match, local principal reference or
an authorization decision. Unavailable never produces ValidatedFacts.

## Independent freshness and expiry

<!-- @spec HTTP-IDENTITY-V1-4 -->
The caller must supply positive integer max_identity_age_ms and
max_membership_age_ms, nonnegative integer max_clock_skew_ms and a valid UTC
clock instant. There are no product TTL defaults. These are internal
validation inputs, not an authority administration resource.

The facts schema module owns public `utc_clock_instant(now: datetime) -> Fraction`
for this existing caller-clock contract. Accept the existing datetime instances
with a defined zero UTC offset; reject non-datetime, naive and nonzero-offset
inputs with ValueError. Return exact signed epoch seconds including the full
microsecond component across datetime's year 0001 through 9999 domain, using
Fraction arithmetic. Do not read a clock, normalize a supplied offset, use a
floating-point timestamp or narrow the domain. Facts validation and pure role
calculation reuse this public owner rather than importing a private helper.
This exposes the existing conversion without changing freshness or error order.

The facts schema owner exposes the pure internal helper `utc_instant(value: str) -> Fraction` over its existing exact timestamp parser. Accept actual strings in the unchanged calendar, UTC suffix and fractional grammar below, preserving all decimal precision and the complete year 0001 through 9999 domain. Reject nonstrings and invalid instants with fixed ValueError text `invalid_utc_instant` and no sensitive input or chained exception. This helper reads no clock, performs no I/O and grants no authority. Existing T1 parser call paths and wire acceptance remain unchanged.

An observation's as_of is the backing-source observation instant, not the
HTTP response creation time. Timestamps are valid RFC3339 calendar instants
with uppercase T, seconds 00–59, optional decimal fractional seconds and UTC
suffix Z or +00:00. Reject other offsets, missing timezone and leap seconds;
retain fractional precision in comparisons without rounding that extends
validity. For each required observation, let L be its independent age limit,
S the skew limit and G its nonnegative source lag, in milliseconds:

```text
reject as stale if as_of > now + S
age = max(0, now - as_of) + G + S
fresh iff age <= L
expires_at = as_of + (L - G - S)
```

When G + S exceeds L, the observation is already stale. Equality at the age
limit is fresh; any later instant is expired. Identity freshness applies to
resolved, inactive and unresolved facts. Membership freshness is additional
for resolved facts, with its own observation, limit and expiry; one fresh
observation cannot repair the other. Unavailable has neither observation and
requires no invented freshness. Facts are usable only while every required
observation remains valid at the supplied clock; pure revalidation at a later
clock must expose expiry. No positive cache is introduced.

Validation establishes consistency against declared metadata, not that the
provider actually observed the upstream source or bounded its lag. Real
mapping, suspension, membership freshness and invalidation qualification
remain separate evidence.

## Standalone artifacts and synthetic conformance

<!-- @spec HTTP-IDENTITY-V1-5 -->
The typed facts models own field shapes; this specification owns semantics.
A deterministic API-owned exporter generates standalone provider OpenAPI 3.1
and JSON Schema artifacts. Independently authored synthetic facts vectors
exercise the contract. The proposed later
interface pointer is docs/interfaces/http-identity/INTERFACE.md, with generated
http-identity-v1.openapi.json and http-identity-v1.schema.json beside it.
Every canonical UUID occurrence must retain its complete, whole-string UUID
grammar in generated model schemas and both standalone artifacts. This is an
expressible structural constraint, including lowercase, hyphenation and true
end-of-string matching; it is not a semantic-only exception. Derive it from the
shared annotated UUID type rather than copying identifier grammars into each
exporter. Opaque account, subject and group identifiers remain opaque.
The artifacts describe a provider-hosted reference operation POST /lookup;
deployment maps the configured lookup URL to that operation. It is not a
Curie API route; URL configuration is outside this facts contract.

Extend the existing `curie dev contracts` machinery to invoke the exporter;
no new CLI subcommand, dummy runtime route or loose script is required.
Exporter output has stable ordering and formatting, no timestamps, and a drift
test compares freshly rendered bytes against both committed artifacts without
DB/app startup. The runtime API OpenAPI and its derivatives remain unchanged.
Synthetic vectors cover every union/kind, request echo, malformed and duplicate
JSON, strict types/version, exact byte boundary, scope mismatch and independent
freshness/expiry boundary. Deliberately broken vectors must fail for the
intended reason. Exported shape validation and pure semantic validation agree;
constraints beyond JSON Schema's expressiveness remain explicit vector tests.

The provider OpenAPI records ADR 0246's two-second total deadline, no retries,
64 KiB response bound, no redirects and no positive core identity cache as
transport obligations. T1 implements no transport, credential handling or
HTTP status handling and proves none of those runtime obligations.

## Later-slice gates and completion

<!-- @spec HTTP-IDENTITY-V1-6 -->
R1 gates canonical principal status/type ownership and stale-result ordering.
R2 gates unresolved/null role eligibility, kind authority and provenance
integration. R3 gates HTTP activation with retained legacy teams. This facts
contract rules none of them: no authority mode/admin/credential API, role or
migration schema, filter evaluation, principal persistence, runtime HTTP I/O,
invocation/approval integration, worker/provenance change or activation ships
in T1. A facts result never selects a provider role or grants permission.

Completion requires test-first pure parser/schema/vector and exporter-drift
checks, relevant static/docs checks and required exact-head CI. No database,
running provider, model, shared installation or real directory is needed for
this contract slice. Contracts and synthetic conformance do not establish the
complete gateway, provider freshness, artifact publication or deployment.
