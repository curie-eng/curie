# Hosted connector endpoints

The worker selects hosted connector endpoints from operator deployment
configuration. Bundle connector declarations, deploy target allowlists, caller
restrictions and proxy authorization still apply. A bundle's `unhosted_url` does
not override a hosted endpoint.

## Static configuration version 1

Set `CURIE_CONNECTOR_ENDPOINTS` on the worker to this JSON shape:

```json
{
  "version": 1,
  "agents": {
    "acme-dev": {
      "reviewer": {
        "base_url": "https://connectors.example.com/acme-dev/reviewer",
        "upload_url": "https://uploads.example.com/acme-dev/reviewer/v1/upload"
      }
    }
  }
}
```

This is the version 1 schema for static connector serving and packaging,
implemented by `packages/curie-internal/src/curie_internal/connector_endpoints.py`.
`version` and `agents` are required. Each agent maps declared connector names to
objects with required `base_url`, optional `upload_url` (default absent), and
optional `insecure_http` (a boolean, default `false`). `version` must be the
integer `1`; booleans, floats and duplicate JSON keys are rejected. JSON
loaders use the shared `parse_endpoint_json` before model validation.
Unknown object fields and versions are rejected. An empty map selects static
mode and fails a claim for an absent agent. Unset configuration selects the
existing Kubernetes resolver; there is no fallback from a static map.

`base_url` is the caller proxy's base, including any service routing prefix.
The MCP URL is `base_url` plus `/mcp`; protected route paths are relative to the
same base. Put the scheme in each URL. HTTPS is required except for loopback
hosts (`localhost`, `127.0.0.0/8`, `::1`) or an explicit per-connector
`insecure_http: true`. That opt-in applies to both URLs and permits sending
credentials in cleartext; use it only on a trusted transport. User information,
any `?` or `#` delimiter (including empty query or fragment), backslashes,
whitespace, non-ASCII or percent-encoded hosts, empty ports and port zero are
rejected. URLs are normalized before storage.

HTTPS verifies certificates and hostnames on both transports. The Python boot
probe and action executor use only the image's trust store and an optional
`CURIE_CONNECTOR_CA_FILE`: an absolute path to a CA PEM file supplied by the
runner deployment. Mount that file read-only outside the bundle and workspace.
Hosted Python clients ignore environment proxy settings and generic TLS trust
variables, including `SSL_CERT_FILE` and `SSL_CERT_DIR`.

The turn-time Claude CLI runs its own HTTP transport. For a private CA, mount
the CA PEM into the runner and set `NODE_EXTRA_CA_CERTS` to its absolute container
path in the runner's deployment environment before the CLI starts. Set both
`CURIE_CONNECTOR_CA_FILE` and `NODE_EXTRA_CA_CERTS` when both clients need that CA.
Bundle secrets cannot supply proxy or trust settings: `HTTP_PROXY`,
`HTTPS_PROXY`, `ALL_PROXY`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `NODE_EXTRA_CA_CERTS`,
`NODE_TLS_REJECT_UNAUTHORIZED` and `NODE_OPTIONS` are reserved in both cases.
There is no endpoint setting that disables certificate verification.

The worker binds this map using its resolved agent identity, never an agent
name supplied to a tool. Runs and evals receive only that agent's projection in
`CURIE_HOSTED_CONNECTOR_ENDPOINTS`:

```json
{"version":1,"agent":"acme-dev","connectors":{"reviewer":{"base_url":"https://connectors.example.com/acme-dev/reviewer"}}}
```

This variable is worker-owned operational configuration, outside the frozen ACI
contract. Do not set it in bundles. Static claims require the existing
`CURIE_CONNECTOR_CALLER_SIGNING_KEY`. A missing declared hosted connector fails
runner boot even if it has an `unhosted_url`. Static mode needs neither a
release nor a Kubernetes namespace. The map changes endpoint selection only:
`X-Curie-Caller`, conditional `X-Curie-Connector-Grant`, one-shot grant spending
and caller-scoped route token transfer retain their existing enforcement.
Every static endpoint is trusted with the agent's caller token. The token is
per agent, so a connector host holding it can replay it at a sibling connector
that admits the same agent. Endpoint selection does not add per-connector
audience claims. Only configure endpoint hosts within that shared trust boundary.

The action executor is unavailable with static endpoints. Its serving attestation
checks Kubernetes Deployment digest, rollout and gating, which cannot attest a
static server; the worker rejects this configuration at startup.

## Upload-only capability

After applying the bundle's connector allowlist, the runner exposes optional
upload URLs as `CURIE_CONNECTOR_UPLOAD_ENDPOINTS`, a JSON object mapping each
connector to its `upload_url`. An absent capability is omitted. Bundle code
reads this map instead of synthesizing a Service name or port. This map contains
URLs only and never caller, grant or route tokens; upload endpoints are not
registered as MCP servers or added to the protected route pipe.

The connector service must serve only its upload operation at `upload_url`,
refuse MCP and general connector operations there, and enforce any separate
upload-specific authentication it requires. An upload URL grants no MCP
credential. Do not reuse the MCP listener as an upload endpoint or embed a
credential in either URL. Migrating downstream bundle upload clients and
implementing that server boundary are separate from endpoint resolution.
