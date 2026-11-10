# 231. Remediation policy bind and arm need an authority the platform key cannot derive

Date: 2026-10-15

Status: Accepted

Supersedes in part [ADR 0203](0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md) REMEDIATION-13 only. Nothing else in ADR 0203 changes. ADR 0203 stays Accepted. Per [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md), ADR 0203 gains a superseded-in-part back-link only once this ADR is Accepted. This Draft does not authorize implementation.

Tracked in [#4607](https://github.com/curie-eng/curie/issues/4607). Realizing issue: [#4231](https://github.com/curie-eng/curie/issues/4231). Realizing code path, after acceptance: `apps/api/src/curie_api/routers/remediation_policy.py`.

## Context

[ADR 0203](0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md) REMEDIATION-13 says the existing platform administrative authority binds a remediation policy. [#4231](https://github.com/curie-eng/curie/issues/4231) reproduced that this authority is the platform API key every worker already holds, so binding and arming a policy is not a separate boundary from the worker.

[ADR 0191](0191-protected-hook-delivery-authority.md) already records the same shape for protected delivery: workers hold the platform administrative API key, and a credential minted from that key is not an independent boundary. The same fact applies here. A remediation authority derived from the platform key is held by every worker that holds the key.

ADR 0045 freezes an Accepted ADR's body. Changing REMEDIATION-13 is a new ADR that supersedes that clause in part. The rest of ADR 0203 stands. The back-link on ADR 0203 is written when this ADR is Accepted, not in this Draft.

## Decision

Binding (`PUT`) and arming (`POST .../arm`) a remediation policy require a separate remediation authority key.

1. The key is `CURIE_REMEDIATION_AUTHORITY_KEY`. The chart value is `api.remediationAuthorityKey`. It is mounted into the API only. It is never mounted into the worker, the dispatcher, adapters, or the sandbox.
2. The chart and `Settings` refuse a value equal to the platform key.
3. The caller presents the key in the header `X-Curie-Remediation-Authority`. The API compares it with `hmac.compare_digest`.
4. A missing header, a wrong header, or an unset key is `403 remediation_authority_required`.
5. `DELETE` and disarm keep today's authority. They only reduce what runs automatically.
6. The operator principal stays for `bound_by` attribution only. It is not the bind or arm authority.

This rule replaces REMEDIATION-13 and nothing else in ADR 0203. Realization is [#4231](https://github.com/curie-eng/curie/issues/4231) against `apps/api/src/curie_api/routers/remediation_policy.py`, and only after this ADR is Accepted. This Draft does not edit ADR 0203, the automated remediation spec, or any code.

## Consequences

* Bind and arm fail closed unless the caller presents the separate remediation authority key to the API.
* A worker that holds only the platform key cannot bind or arm a policy, and cannot mint an equivalent credential from that key.
* Unbind and disarm stay on today's authority, so reducing automatic remediation does not require the new key.
* `bound_by` still records the operator principal. That record is attribution, not authorization.
* Until this ADR is Accepted, REMEDIATION-13 remains the published rule and ADR 0203 is not edited.

## Rejected alternatives

* A `remediation.administer` scope on the existing principal ([#4228](https://github.com/curie-eng/curie/issues/4228)). Rejected because that token is still signed with the platform key and is mintable from it. ADR 0191 already treats a credential minted from the platform key as not an independent boundary.
* Keeping REMEDIATION-13 and relying on the operator principal. Rejected because `POST /approvals/principals/operator` mints an operator principal for any platform key holder, so the principal is not a boundary the worker lacks.
