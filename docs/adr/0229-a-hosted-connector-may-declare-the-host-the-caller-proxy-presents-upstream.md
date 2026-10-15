# 229. A hosted connector may declare the Host the caller proxy presents upstream

Date: 2026-10-14

Status: Draft

This ADR would amend [ADR-0178](0178-the-connector-caller-token-names-the-run-and-work-item.md)
decision 5, which says the caller proxy "changes nothing else about the
forwarded request". It keeps decision 6 as decided: the proxy still forwards
on loopback, and a connector still trusts the identity headers only on that
connection. A Draft does not authorize implementation, and ADR-0178 gains no
back link until this is Accepted
([ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)).

Tracked in [#4598](https://github.com/curie-eng/curie/issues/4598). The
defect it addresses is [#4597](https://github.com/curie-eng/curie/issues/4597).

## Context

With a caller key, every hosted connector gets a caller proxy (ADR-0168
decision 7). The proxy checks the caller token's signature, its expiry and the
agent against `admits:`. It then forwards the request to the server on
`127.0.0.1:<port>` and keeps the caller's `Host`, which is a name of the
connector's Service. The Service targets the proxy's port. A second Service,
`<connector object>-direct`, targets the server's own port for callers that
are not agents, and only an operator-applied ingress policy admits anyone to
it.

Servers protect themselves from DNS rebinding in two ways, and the proxy
treats them differently:

- **An allowed-hosts list** (mcp-grafana's `-allowed-hosts`). It checks only
  `Host`. Curie supplies every Service name through `${CURIE_ALLOWED_HOSTS}`
  (ADR-0086), so keeping the caller's `Host` is what makes these servers work
  behind the proxy.
- **A loopback-listener guard** (the MCP Go SDK's Streamable HTTP handler,
  used by `kubernetes-mcp-server`). It refuses a non-loopback `Host` only on a
  connection that arrived on a loopback address, because that is the case a
  browser on the same machine can be tricked into. Without the proxy, a
  request reaches the server on the pod IP and the guard does not apply. With
  the proxy, every admitted request arrives on loopback carrying a Service
  name, and the server answers `403 Forbidden: invalid Host header`.

So turning on a caller key breaks every server of the second kind, while
servers of the first kind keep working. The released `kubernetes-mcp-server`
(v0.0.67 and earlier) has no option to relax the guard or list allowed hosts.
Its main branch adds `disable_localhost_protection` for exactly this
reverse-proxy shape (merged upstream on 2026-09-23), but that is
unreleased, and the same line replaces every command-line flag with a
configuration file. Other servers built on the same SDK behave the same way,
and nothing in Curie lets a bundle author fix it.

## Decision

### 1. An opt-in declaration on the hosted connector

`ConnectorSpec` gains one optional hosted-form field, `proxy_upstream_host`.
Its value is a loopback host: `localhost`, a literal IPv4 loopback address
such as `127.0.0.1`, or `::1`. Validation refuses anything else, refuses the
field on a remote (`url:`) connector, and refuses a port or any other
characters in the value.

Omitted, nothing changes: the proxy forwards the caller's `Host` exactly as it
does today. A connector whose server keys on `Host` alone keeps today's
behavior by default and never needs the field.

### 2. Rendered into the proxy, never into the server

`connector_render` renders the value as one more literal in the proxy
container's environment, `CURIE_CALLER_PROXY_UPSTREAM_HOST`, only when the
field is set and the connector is rendered with a proxy. The server container,
the Service, the `-direct` Service and both NetworkPolicies render exactly as
they do now. Without a caller key there is no proxy, and the field has no
effect.

### 3. The proxy presents the declared Host only on admitted requests

After a request passes every admission check (signature, expiry, `admits:`,
and the connector grant for a gated tool call), the proxy replaces `Host` with
`<declared host>:<server port>`, with IPv6 in brackets. A refused request never
reaches the server, so it is never rewritten. Nothing else changes from
ADR-0178 decision 5: the token stays stripped, the identity headers are set
from the verified claims, and every other header and the body are relayed as
received. The forward still goes to `127.0.0.1`, so decision 6's loopback
trust is intact.

### 4. Why presenting loopback is safe here, and only here

The loopback-listener guard exists so a request that a browser was tricked
into sending to a local port is refused. Behind the proxy, that question has
already been answered more strictly than the guard can answer it: the request
carries a token signed outside the sandbox, unexpired, naming an admitted
agent. The declared `Host` attests only that this request was admitted by the
proxy in the server's own pod, which is true.

That attestation must not reach requests the proxy did not admit. The
`-direct` Service reaches the server's own port without the proxy, so those
requests keep their original `Host`, and whatever check the server applies to
them still applies. This is the boundary any alternative has to keep: a Host
rewrite placed in front of the server's port, rather than inside the proxy's
admitted path, would vouch for direct callers that nothing verified.

### 5. Contract change

The field changes the frozen `packages/plugin-format` contract and its Rust
mirror (`ConnectorSpecDecl` in `cli/src/connector_build.rs`), so it lands as
its own reviewed contract change per AGENTS.md "Frozen contracts". It is a new
optional field. `ConnectorSpec` forbids unknown keys and so does the Rust
mirror, so a CLI or API from before the field refuses a bundle that sets it. A
bundle should use it only on a release line that has it.

## Consequences

- A bundle can host a server that guards its loopback listener, such as the
  pinned `kubernetes-mcp-server`, behind the caller proxy with one line in
  `connectors.yaml`. The SRE example's Kubernetes connector is the first user.
- The proxy grows one more fixed responsibility: on admitted requests, set one
  header from rendered configuration.
- A server's own `Host` check no longer constrains admitted proxied requests
  for a connector that opts in. That check has nothing to add there, because
  admission already verified the caller. Direct requests are unchanged.
- An author can opt in for a server that does not need it. The cost is that the
  server sees `localhost` instead of its Service name on proxied requests. That
  matters only to a server that builds absolute URLs from `Host`, and the field
  is off by default.
- Old CLIs and APIs reject bundles that use the field (decision 5).

## Alternatives considered

1. **Forward to the pod IP instead of `127.0.0.1`.** That would make the proxy
   transparent to the server with no new field: the guard would not fire, and
   allowed-hosts servers would see the same `Host`. Rejected: ADR-0178
   decision 6 makes a connector trust the identity headers only on the proxy's
   loopback connection, and a pod-IP forward is indistinguishable from a
   direct caller.
2. **Always present a loopback `Host`.** Rejected: it breaks every
   allowed-hosts server whose list names only the Service, which is the
   default Curie renders (ADR-0086).
3. **A per-bundle front in the connector image.** A small reverse proxy inside
   the connector image, listening on the connector's port and forwarding to the
   server on loopback with a loopback `Host`, needs no Curie change. Rejected:
   the front sits on the server's own port, which the `-direct` Service also
   targets. It would present a loopback `Host` for direct requests too, which
   nothing verified, and the server would see every request on loopback. So it
   switches off any `Host` check the server applies, on exactly the path that
   has no caller verification. That defeats the boundary ADR-0178 draws. A
   loopback-listener guard happens to ignore pod-IP traffic anyway, but the
   front would make that a property of one server rather than of the
   platform. Every bundle would also re-implement and re-publish the same
   component.
4. **Wait for an upstream server option.** `kubernetes-mcp-server` main has
   one, but it is unreleased, and adopting it also means replacing the
   example's command-line flags with a configuration file. It also fixes only
   that server, not the class.
5. **A free-form `Host` value.** Rejected: the proxy forwards only to
   loopback, and a non-loopback value would present an authority the
   connection does not have. A loopback-only value keeps the declaration
   truthful.

## Tracking

[#4598](https://github.com/curie-eng/curie/issues/4598) carries the contract
change. The realizing paths are `ConnectorSpec` and its Rust mirror,
`connector_render._proxy_container`, `curie_connector_proxy.server`, and the
SRE example's `connectors.yaml`. They land only after this ADR is Accepted.
