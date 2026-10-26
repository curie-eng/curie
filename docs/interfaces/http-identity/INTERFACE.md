---
seam: HTTP identity facts
kind: NONE
impls: 0 active providers; facts-only v1 contract and synthetic vectors
grade: not separately graded
epics:
  - "#4890"
order: 32
---
# INTERFACE: HTTP identity facts

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** NONE &nbsp;·&nbsp; **Implementations today:** 0 active providers; facts-only v1 contract and synthetic vectors &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

## Contract owners

<!-- @spec HTTP-IDENTITY-V1-1 HTTP-IDENTITY-V1-2 HTTP-IDENTITY-V1-3 HTTP-IDENTITY-V1-4 -->
The [HTTP identity facts v1 specification](../../superpowers/specs/2026-10-23-http-identity-facts-v1.md)
owns semantics. The strict models in
`apps/api/src/curie_api/schemas/http_identity.py` own field shapes and the pure
`validate_lookup` entry point. The [provider OpenAPI 3.1](http-identity-v1.openapi.json)
and [JSON Schema](http-identity-v1.schema.json) describe those shapes. Every nullable
wire field remains required. Provider facts are bound to the exact caller-captured
request and complete sender scope; they contain no permission decision.

Callers supply a validated `LookupRequestV1`, required `FreshnessLimits` and an
aware UTC `datetime` to `validate_lookup`. Invalid caller inputs raise before
provider parsing. The result is `ValidatedFacts` or a `FactsRefusal` containing
only its stable code. Identity and membership expiries are exact `Fraction`
values measured in UTC epoch seconds, retaining arbitrary timestamp fractional
precision. `groups=None` carries no membership; an empty tuple is a complete
empty set. Group order has no meaning.

## Provider artifacts and conformance

<!-- @spec HTTP-IDENTITY-V1-5 -->
The reference operation is provider-hosted `POST /lookup`. Deployment maps a
configured lookup URL to that operation; it is not a Curie API route.
`apps/api/src/curie_api/http_identity_export.py` renders both artifacts without
loading the runtime app or connecting to any service. `curie dev contracts`
regenerates them and checks drift alongside existing contracts. Independently
authored synthetic vectors live in `tests/vectors/http-identity-v1.json`, with
pure parser, shape and drift checks in `tests/http_identity`.

JSON Schema represents required fields, strict scalar shapes, outcome unions
and complete unique group sets. Exact request correspondence, issuer equality
across objects, duplicate JSON keys, decoded byte length and freshness require
the pure validator. Issuer patterns represent HTTPS structure and bracketed
IPv6, scoped IPv6 and IPvFuture hosts, preserving the existing parser's exact
string compatibility, including controls it removes only while parsing.
Unicode NFKC netloc checks require the pure validator: standard JSON Schema
does not normalize strings, and structural schema validation alone does not
prove full URL validity. Enable `date-time` format assertions to validate real
calendar dates; timestamp patterns retain UTC syntax and arbitrary fractional
precision. The specification remains the semantic reference.

The OpenAPI's `x-curie-transport` publishes the two-second total deadline, no
retries, 64 KiB response bound, no redirects and no positive core identity
cache required by
[ADR 0246](../../adr/0246-common-admission-resolves-an-optional-http-identity-before-authorized-enqueue.md).
Those are transport obligations, not runtime behavior implemented by these artifacts.
The [HTTP identity transport v1 specification](../../superpowers/specs/2026-10-23-http-identity-transport-v1.md)
defines the standalone bounded client, verified trust, identity-only streaming
and cancellation/drain contract; its publication does not activate a consumer.

## Completion boundary

<!-- @spec HTTP-IDENTITY-V1-6 -->
This seam has no active HTTP provider or consumer. Synthetic conformance proves
representation and pure validation, not real directory freshness, HTTP transport,
publication or deployment. Authority administration, credentials, principal
persistence, role eligibility, migration and invocation/approval integration
remain outside this contract. R1, R2 and R3 in the specification gate their
dependent slices. Accepted nonhuman facts grant no capability.
