# Serve a static connector container

A connector can run as a long-running container on a managed container service
without Kubernetes. The container runs the existing caller proxy and the
unchanged connector implementation in one task. Only the proxy ports are public.
Bind the implementation to task loopback. Publishing its port bypasses caller
and grant verification.

Build the base image from the repository root with
`docker build -f apps/worker/Connector.Dockerfile -t connector-host .`.
Use the Docker endpoint supplied by your environment. A derived image installs
the connector and its dependencies, preserving the entrypoint and non-root user:

```dockerfile
FROM connector-host
COPY --chown=65532:65532 connector/ /opt/connector/
```

The image uses `tini` as PID 1. The host starts the implementation directly from
an argument array, drains the HTTP listeners, forwards SIGTERM and SIGINT to its
process group, and waits up to ten seconds for the implementation before killing
it. Unexpected implementation exit stops the task with a failure status. Keep
the service's termination grace period above twenty seconds. The connector must
handle SIGTERM, bind its servers to loopback, and need no root privileges.

## Configuration version 1

Mount a read-only JSON file and set `CURIE_CONNECTOR_SERVE_CONFIG` to its absolute
container path. Set `CURIE_CONNECTOR_GRANT_STORE_PASSWORD` from the container
service's secret store. The host validates the complete file before starting the
implementation or opening listeners. Unknown fields, duplicate JSON keys, wrong
types, unsupported versions, invalid keys, colliding listener ports, and unsafe
route paths refuse startup. The executable schema is
`apps/worker/src/curie_connector_proxy/hosting.py::HostConfig`; its
`model_json_schema()` returns the JSON Schema for configuration tooling.

This example serves the connector named `reviewer` for `acme-dev`:

```json
{
  "version": 1,
  "connector": "reviewer",
  "listener": {
    "host": "0.0.0.0",
    "port": 8080,
    "tls": {
      "cert_file": "/run/connector/tls/cert.pem",
      "key_file": "/run/connector/tls/key.pem"
    }
  },
  "implementation": {
    "command": ["python3", "/opt/connector/server.py"],
    "port": 8000,
    "upstream_host": "127.0.0.1"
  },
  "public_keys": ["REPLACE_WITH_BASE64_ED25519_PUBLIC_KEY"],
  "admits": ["acme-dev"],
  "gated_tools": ["reviewer/write*", "reviewer/delete*"],
  "routes": [
    {"path": "/v1/review", "methods": ["POST"], "max_body_bytes": 1048576}
  ],
  "grant_store": {
    "host": "valkey.example.com",
    "port": 6379,
    "username": "reviewer",
    "ca_file": "/run/connector/valkey-ca.pem"
  },
  "upload": {
    "listener": {
      "host": "0.0.0.0",
      "port": 8081,
      "tls": {
        "cert_file": "/run/connector/tls/cert.pem",
        "key_file": "/run/connector/tls/key.pem"
      }
    },
    "upstream_port": 8001,
    "path": "/v1/upload",
    "max_body_bytes": 16777216
  }
}
```

Replace the public key placeholder with the worker's standard-base64 Ed25519
verification key. Rotation accepts several public keys. The same verification
keys check callers, tool grants, declared routes, and upload capabilities. Never
give this task the worker's private caller or grant minting key. The TLS private
key belongs to the listener certificate and grants no token-minting authority.

The environment for this example is:

```text
CURIE_CONNECTOR_SERVE_CONFIG=/run/connector/serve.json
CURIE_CONNECTOR_GRANT_STORE_PASSWORD=<secret supplied by the container service>
```

`listener.host` is an IP bind address, including IPv6. Omit `tls` when a trusted
ingress terminates TLS. Restrict ingress to that gateway and configure the
external endpoint with HTTPS. Direct non-loopback HTTP requires the explicit
endpoint opt-in described in [hosted connector endpoints](connector-endpoints.md).
Never expose cleartext credentials to an untrusted network.

`implementation.port` defaults to 8000. Connections always target `127.0.0.1`;
`upstream_host` controls only the forwarded Host header for implementations with
an allowed-host check. Its allowed values are `127.0.0.1`, `localhost`, and `::1`.
Public listener ports must differ from implementation ports, including the
upload upstream. An implementation may serve uploads on its MCP port by setting
`upload.upstream_port` to that port.

`gated_tools` is required, including an explicit empty list when no tools are
gated. Patterns match `reviewer/tool` and `mcp__reviewer__tool`. `admits` is an
explicit nonempty agent list. An admitted caller still needs a live grant
matching the connector, tool, agent, and canonical arguments for a gated call.
A successful spend happens once, across replicas sharing the grant store.

`routes` defaults to an empty list. Each declared route accepts only its
connector-scoped `ccr` capability, with the existing streaming body limit.
Methods are POST or PUT; paths are canonical lowercase slash-separated segments
of letters, digits, underscores, or hyphens. `/mcp` is reserved. Unknown routes
retain the existing caller-proxy behavior and one MiB body limit.

## Valkey grant spending

Portable serving always uses TLS with certificate and hostname verification.
`grant_store.ca_file` optionally supplies a private CA; otherwise the image's
trust store applies. There is no plaintext or verification-disable setting.
Provision a dedicated ACL user, with a unique password, for this connector:

```text
user reviewer on >REPLACE_WITH_SECRET ~connector-grant:reviewer:* +set
```

Disable the default user and restrict other users according to the deployment's
needs. The proxy authenticates as the configured username and uses only
`SET connector-grant:reviewer:<grant-id> 1 NX EX <ttl>` in database zero. Do not
grant read, delete, administrative, or other connector key access. Every replica
for the same connector must use the same Valkey store and key namespace. A TLS,
authentication, ACL, or availability failure refuses the gated call before it
reaches the implementation. The host requires a password even when the current
gate list is empty.

## Upload-only listener

Omit `upload` when the connector has no upload capability. Otherwise configure
an explicit listener and one exact POST path. There is no synthesized hostname.
The worker's endpoint configuration for the example is:

```json
{
  "version": 1,
  "agents": {
    "acme-dev": {
      "reviewer": {
        "base_url": "https://connectors.example.com",
        "upload_url": "https://uploads.example.com/v1/upload"
      }
    }
  }
}
```

Route the first origin to port 8080 and the second to port 8081, preserving
paths. `upload_url` must resolve to the configured upload listener and path;
an ingress must not route it to the MCP listener. Upload clients send a signed
`ccu` capability in `X-Curie-Caller`, never a general `cct` token. The worker's
`apps/worker/src/curie_worker/caller_token.py::mint_upload` creates this
capability using its private key, agent, connector, and absolute expiry. Its
signed claims are exactly `agent`, `connector`, and `exp`. Keep expiry short and
deliver the capability only to the authorized upload client. Endpoint projection
contains URLs only; it does not distribute upload credentials automatically.

The upload listener verifies the signature, expiry, admitted agent, and connector
before streaming the bounded upload to task loopback. It rejects `/mcp`, other
paths or methods, caller tokens, route tokens, tool grants, and any request with
`X-Curie-Connector-Grant`, even alongside an upload capability. The MCP listener
refuses the configured upload path and its alternate spellings, including when
the implementation serves MCP and uploads on the same port. The MCP listener
and declared routes reject `ccu`. Curie capability headers stop at the proxy; the
implementation receives the verified agent header. The upload capability is
reusable until expiry and authorizes this connector's one upload operation.

The optional per-connector audience for ordinary `cct` tokens is not implemented.
Their existing per-agent trust boundary still applies, as documented in
[hosted connector endpoints](connector-endpoints.md).

## Container acceptance check

Run `uv run pytest cli/tests/local/test_static_connector_host.py -q -s --reruns 0`
with Docker configured by the caller. The test builds this recipe, runs one
trivial connector container and one TLS Valkey container, drives HTTPS requests,
and checks implementation execution counts, upload scope, non-root execution,
and graceful SIGTERM. Resources carry one unique ownership label; cleanup removes
and verifies every owned container, network, and image. No bind mount assumes
that the Docker daemon shares the test host's filesystem.
