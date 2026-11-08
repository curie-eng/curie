# Adopt an external HTTP identity authority

An operator uses the platform API key to administer an authority and the
`invocation` or `approver` role of an agent. Console sessions, worker tokens,
channel tokens and directory ingestion credentials do not grant this authority.
The Rust CLI exposes these operations through `cluster identity-authority` and
`cluster role-filter`. Both accept the ordinary cluster connection flags,
`--api-url`, `--api-key`, `--dry-run` and global `--json`.

## Prepare the provider and runtime

The external identity provider must serve the existing
[HTTP identity lookup contract](../apps/api/src/curie_api/schemas/http_identity.py)
over HTTPS. Its response must echo the request scope and authority version,
report a verified human identity, and return complete, fresh group observations
with the required lag evidence. An unverified group claim in a turn body grants
nothing. A nonmember receives the fixed `caller_not_allowed` refusal.

Provision the bearer through the protected API runtime file, outside a bundle
or request body. `CURIE_HTTP_IDENTITY_DIR` selects a directory containing
`api-runtime.json`. Its `providers` entries each contain a complete
`configuration` and a `bearer`. A configuration contains `schema_version: 1`,
`tenant_id`, `version`, and the `selection` object shown below. The
`credential_ref` identifies that protected registration by ID and generation;
the registry also binds it to the tenant, issuer and exact lookup URL. The
operator API accepts only this reference, never a literal bearer.

The same runtime document must contain the installation's `receivers`,
`context_keys`, and `signing_key`, or its supported `email_receivers`. Use the
[initializer](../apps/api/src/curie_api/identity/http_initializer.py) and
[receiver registration models](../apps/api/src/curie_api/identity/http_invocation.py)
for the exact document shapes. Protect the directory and files using the
[operator file rules](../packages/curie-internal/src/curie_internal/operator_file.py).
The API reads the document once at startup. Replace the process after changing
it. For a private CA, put `provider-ca.pem` beside it; TLS hostname checks remain
mandatory. The chart mounts an operator supplied Secret through
`api.httpIdentity.existingSecret`; enable `api.httpIdentity.providerCaBundle`
when providing the private CA. Slack receivers also need the matching protected
dispatcher registration through `dispatcher.httpIdentity.existingSecret`.

Register the credential before adoption. Otherwise the persisted authority has
no usable bearer and admission refuses as `identity_authority_unavailable`.
This workflow requires configuration files and operator API calls, not custom
application code. The [chart installation reference](../charts/curie/README.md#http-identity-operator-files)
describes the protected files and activation wiring.

## Adopt and inspect

Create an adoption request with a fresh UUID `request_id`. An absent authority
starts with expected version and lifecycle generation zero. If replacing a
local directory authority, read its current version and generation first and
use those values. Adoption preserves the issuer and advances both counters.
The directory configuration read exposes its version; an existing local
projection has lifecycle generation zero before its first HTTP adoption.

```json
{
  "request_id": "00000000-0000-4000-8000-000000000010",
  "expected_version": 0,
  "expected_lifecycle_generation": 0,
  "selection": {
    "mode": "http",
    "issuer": "https://identity.example.com",
    "lookup_url": "https://identity.example.com/lookup",
    "credential_ref": {"id": "acme-provider", "generation": 1},
    "max_identity_age_ms": 5000,
    "max_membership_age_ms": 5000,
    "max_clock_skew_ms": 1000
  }
}
```

```bash
curie cluster identity-authority adopt --tenant "$TENANT_ID" --apply authority.json --dry-run --json
curie cluster identity-authority adopt --tenant "$TENANT_ID" --apply authority.json --json
curie cluster identity-authority get --tenant "$TENANT_ID" --json
curie cluster identity-authority list --json
```

The API routes are `PUT /identity/authorities/{tenant_id}` for adoption,
`GET /identity/authorities/{tenant_id}` for the current authority, and
`GET /identity/authorities` for a bounded list. Continue a list with `--after`
and its `next_cursor`. Dry runs print the HTTP operation and path, without
connection credentials or request contents.

## Restrict a role to issuer groups

Inspect the current role policy before writing. A missing policy has version
zero. If a legacy team policy exists, its version is still the CAS token even
when no role filters have been set.

```bash
curie cluster role-filter list --tenant "$TENANT_ID" --agent "$AGENT_ID" --json
```

Create the filter request:

```json
{
  "request_id": "00000000-0000-4000-8000-000000000011",
  "expected_version": 0,
  "groups": [{"issuer": "https://identity.example.com", "id": "operators"}]
}
```

```bash
curie cluster role-filter set --tenant "$TENANT_ID" --agent "$AGENT_ID" --role invocation --apply groups.json --json
```

`PUT /identity/roles/{tenant_id}/{agent_id}/{role}` replaces that role's filter
with issuer group selectors. Any listed group may match. The other role,
legacy caller restrictions, approval route eligibility, and existing migration
state remain in force. `approver` restricts approval resolution for that agent;
it does not independently grant approval permission. These are agent role
filters, not separate per route ACLs.

Drive the actual ingress with two independently authenticated senders. Confirm
that a verified member queues a turn and a nonmember receives
`caller_not_allowed` with no new queued delivery. Repeat after changing provider
membership to establish that current observations control admission.

To remove a filter, write a request containing a new `request_id` and the
current `expected_version`, without `groups`, then run:

```bash
curie cluster role-filter remove --tenant "$TENANT_ID" --agent "$AGENT_ID" --role invocation --apply removal.json --json
```

This calls `DELETE /identity/roles/{tenant_id}/{agent_id}/{role}` and restores
the role's unrestricted filter while preserving its other policy settings.

## Rotate, retire and recover

For rotation, provision the new protected credential generation and restart the
API, then submit the full new selection with the current authority version and
lifecycle generation to `cluster identity-authority replace`. This calls
`POST /identity/authorities/{tenant_id}/replace` and increments both counters.
It preserves the issuer. Sender scopes using the old authority version refuse.

Retirement uses a request with `request_id`, `expected_version`, and
`expected_lifecycle_generation`, without `selection`:

```bash
curie cluster identity-authority retire --tenant "$TENANT_ID" --apply retirement.json --json
```

`POST /identity/authorities/{tenant_id}/retire` increments both counters and
stores a disabled selection. It retains the ownership boundary so retirement
cannot reopen legacy admission. HTTP identity dependent turns refuse with
`identity_authority_unavailable`. Replacement with a current version can
reactivate the same issuer after its credential is provisioned.

Every successful mutation records an `identity.operator_request` audit event
with actor `platform`. Repeating the same request UUID and payload returns its
original result without another mutation or audit event. That result is a
receipt, so use `get` or `list` for current state. Reusing the UUID with different
content is `operator_request_conflict`. A stale version or generation returns
409 with a fixed, nonempty `detail`. Read current state and issue a new request
UUID after resolving the conflict. Validation returns 422 without reflecting
submitted values; missing platform authority returns 401.
