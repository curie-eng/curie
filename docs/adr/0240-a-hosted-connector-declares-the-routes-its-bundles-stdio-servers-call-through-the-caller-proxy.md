# 240. A hosted connector declares the routes its bundle's stdio servers call through the caller proxy

Date: 2026-10-22

Status: Accepted

Accepted on 2026-10-22 with explicit maintainer approval from Junwon Jung,
before implementation.

This ADR amends [ADR-0168](0168-one-installation-hosts-several-bot-identities.md)
decision 7 and [ADR-0178](0178-the-connector-caller-token-names-the-run-and-work-item.md)
decision 4. Under ADR-0168 decision 7, the `cct` caller token is the only
credential the caller proxy admits. Under ADR-0178 decision 4, the proxy
refuses any claim beyond `agent`, `exp`, `run` and `work_item`. This ADR keeps
both rules for the `cct` token and adds a second token type that the proxy
admits only on declared routes. Both ADRs carry a back link under
[ADR-0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

Tracked in [#4917](https://github.com/curie-eng/curie/issues/4917).

## Context

A hosted connector behind the caller proxy can be reached only by a request
that carries a `cct` token, and the proxy buffers an admitted POST in memory
before forwarding it. Both properties were chosen for MCP traffic from the
bundled CLI. Neither fits a class of bundle that exists today: a bundle whose
stdio plugin server (declared in the bundle's `.mcp.json` or its manifest's
`mcpServers`) has a tool that sends a large file to an HTTP route on a hosted
connector declared by the same bundle. One example is a tool that reviews a
document of tens of MiB by POSTing the raw file to `/v1/review`. The route is
not MCP, and the file never passes through the model.

Behind the proxy, that request fails for two independent reasons.

1. **Bundle code has no caller token, by design.** `derive_mcp_servers`
   (`runner/src/curie_runner/connectors.py`) puts the `X-Curie-Caller`
   placeholder only on the derived hosted entries.
   `materialize_connector_caller_headers` expands it only there and removes
   `CURIE_CONNECTOR_CALLER_TOKEN` from every env mapping it is given
   ([#3550](https://github.com/curie-eng/curie/issues/3550)).
   `platform_credential_names` (`runner/src/curie_runner/subprocess_env.py`)
   keeps every `CURIE_*TOKEN*` name out of shell and hook envs. The runner and
   CLI parent environs are locked against same-uid readers
   (`runner/src/curie_runner/proc_dumpable.c`). The rules that keep the token
   from Bash and hooks are the same rules that keep it from a bundle's stdio
   server. The proxy therefore refuses the POST `403 outcome=missing`.
2. **The body cap.** An admitted POST is read whole by `request.read()` in
   `apps/worker/src/curie_connector_proxy/server.py`, and a body over
   `_MAX_BODY_BYTES` (1 MiB) is refused with `outcome=too_large`. The buffer
   exists because the grant check (`_grant_refused`) parses JSON-RPC
   `tools/call` bodies. An admitted request with any other method is streamed to the server unread, with no size check at all.

The `cct` token cannot simply be handed to the stdio server. It names an agent
and nothing else, so any connector that admits that agent accepts it on every
path, `/mcp` included. Bundle code that held it could call every MCP tool of
every connector the agent may reach and skip the runner's approval gate on
each call. ADR-0178 decision 5 already treats a leaked `cct` as replayable
against other connectors.

The NetworkPolicy-bounded second port that #4917 describes as today's way out
does not fit either. The stdio server runs in the sandbox. Opening a port to
the sandbox opens it to Bash and hooks, with no credential at all.

These requirements govern any answer.

- **R1.** The #3550 boundary holds. Bash and hooks never receive a credential,
  including a narrower one, by env, argv, file or inherited descriptor.
- **R2.** A credential the platform gives bundle code is scoped to the stdio
  plugin servers the bundle declares as callers, and to one hosted connector.
  It is useless on any other connector and on the connector's MCP path.
- **R3.** Body limits are declared per route. The MCP path and every
  undeclared path keep the small buffered limit.

## Decision

### 1. `routes:` on a hosted connector

`ConnectorSpec` gains one optional hosted-form field, `routes:`, a list of
entries:

```yaml
connectors:
  reviewer:
    image: ghcr.io/example/reviewer@sha256:...
    port: 8000
    routes:
      - path: /v1/review
        methods: [POST]
        max_body_bytes: 67108864
        callers: [review-tools]
```

- `path` is matched exactly against the request's raw path, with the query
  string ignored. It must start with `/`, contain only unreserved URI
  characters and `/`, and contain no empty, `.` or `..` segment and no percent
  encoding. It must not equal or sit under the connector's MCP path, and it
  must not equal `readiness_path`. Two entries must not share a path.
- `methods` is a non-empty subset of `POST` and `PUT`. It defaults to
  `[POST]`.
- `max_body_bytes` is required. It is a positive integer no larger than the
  platform ceiling, 256 MiB. There is no default, so an author who declares a
  route also states its size.
- `callers` is a non-empty list of stdio server names. Each must be a server
  this bundle declares in its `.mcp.json` or its manifest's `mcpServers`, with
  the stdio transport. A remote, HTTP or SSE server cannot be a caller,
  because it does not run under the runner's custody (decision 4).

Validation refuses `routes:` on a remote (`url:`) connector. A route token is
admitted against the connector's `admits:` list, like a `cct`. Omitted,
nothing changes.

### 2. A route token, scoped to one connector

The worker mints a second token type next to `cct`. It uses the same keys and
the same encoding as ADR-0178 decision 4, under the prefix `ccr`, with exactly
these claims:

- `agent`, `exp`, and the optional `run` and `work_item` pair, with the same
  meaning and lifetime as the sandbox's `cct`.
- `connector`, the name of the one hosted connector the token is good for.

The worker mints one `ccr` per hosted connector that both declares `routes:`
and is mounted for this agent under ADR-0168 decision 8. It reads those
declarations from the version's stored bundle, as it already does for an
action executor boot, and caches the answer by `bundle_ref`, because a stored
bundle never changes. It mints none for any other connector, and none when it
holds no caller key or cannot read the bundle. In that case the route is
refused `missing`, which the caller sees, rather than the turn failing.

The tokens travel as one env entry, `CURIE_CONNECTOR_ROUTE_TOKENS`, a JSON
object from connector name to token. The worker writes it next to the
`BootEnv` render, the way it writes `CURIE_CONNECTOR_TOOL_GRANT`, so the ACI
contract does not change. The name matches `CURIE_*TOKEN*`, so
`platform_credential_names` already keeps it out of every shell and hook env.
`claim_tokens` lists it with the other claim tokens, so on Kubernetes it is
delivered by the per-claim Secret and never stored on the claim. A `cct` and a `ccr` are never interchangeable. The proxy refuses a
`cct` on a declared route and a `ccr` everywhere else (decision 3).

### 3. The proxy admits a `ccr` on declared routes only, and streams them

`connector_render` renders the declared routes into the proxy container as one
more literal, `CURIE_CALLER_PROXY_ROUTES`, a JSON list of `{path, methods,
max_body_bytes}`. `callers` is not rendered, because the proxy cannot see
which process sent a request. Caller scoping is enforced in the sandbox
(decision 4). The server container, the Service, the `-direct` Service and
both NetworkPolicies render as they do now.

For a request whose method and path match a declared route:

- The proxy admits it only if `X-Curie-Caller` carries a `ccr` that verifies,
  has not expired, names an admitted agent, and names this connector. A
  missing token is `missing`. A `cct`, a malformed token or a `ccr` for
  another connector is `invalid`. The refusal vocabulary and the frozen
  refusal body are unchanged.
- The body is never buffered. A `Content-Length` above `max_body_bytes` is
  refused before the server is contacted. A body without one is counted as it
  is relayed. When the count passes the limit, the proxy aborts the upstream
  request and answers the caller, if it has not answered yet. Either refusal
  is `413` with `outcome=too_large route=<path>` in the log line.
- There is no grant check. A route body is not JSON-RPC, and a route is not a
  connector tool. The approval boundary for this traffic is the runner's gate
  on the stdio tool call that sends it, which applies today under the bundle's
  own policy.
- The token is stripped and the identity headers are set from its verified
  claims, exactly as for a `cct` (ADR-0178 decision 5), and ADR-0229's
  declared `Host` applies the same way.

A request on a declared route's path with a method the route does not
declare, or on another spelling of that path, is refused `invalid` whatever
token it carries. Another spelling is one a lenient server could route to the
same handler: a percent encoding, a repeated or trailing slash, a dot segment,
or another letter case. Without this, a `cct` could reach a route's handler
through a server that merges slashes.

For every other request, behavior is today's with one change. A `ccr` is
`invalid` there, so a route token cannot reach `/mcp`, OAuth discovery, or an
undeclared path. The 1 MiB limit now applies to the body of every method, not
only POST. Non-POST bodies are still streamed rather than buffered, but they
are counted against the same limit. Without this, the limit could be avoided
by changing the method, and R3 would mean nothing.

### 4. The runner hosts a declared caller and hands it the token on a descriptor

Today the CLI spawns a bundle's stdio servers from the `--mcp-config` payload
the runner builds (`runner/src/curie_runner/plugin.py::bundle_mcp_servers`),
with an env the runner does not fully control. Putting a token in that
payload's `env` block would copy it into the CLI's configuration path and into
whatever env the CLI gives its children. So a stdio server named in some
route's `callers` is not given to the CLI as a stdio entry. Instead:

- The runner spawns the server itself, with the same command, args and
  `${VAR}` and `${VAR:-default}` expansion it would have had. The executable
  is resolved at boot, and the process starts in the bundle root, never in a
  directory Bash writes. Its env is the shell and hook env with no platform
  credentials, plus its own expanded `env` block. That is the env the runner's
  capability probe already gives the same server. The runner also adds the
  environ lock library to `LD_PRELOAD`, so the process is not dumpable and
  same-uid peers cannot read its environ or memory.
- The runner starts the caller once, on the first session, which comes before
  any turn's Bash can run, and keeps it for the runner's life. A later session
  reuses it, and a repeated `initialize` is answered from the caller's first
  answer, so the caller is never respawned while Bash could be waiting for it.
  A caller that exits is not restarted, and its tools fail for the rest of the
  runner's life.
- The runner mounts that process on the CLI as an in-process SDK MCP server,
  under the name the CLI would have used (`plugin:<bundle>:<server>`). The SDK
  server is a JSON-RPC relay over the child's stdio. It remaps request ids, so
  two sessions cannot collide, and it settles a request the CLI cancels. The
  SDK's bridge drops what an in-process server sends on its own, its
  notifications and its requests to the client, so a caller that relies on
  them does not get them. Tool names, `toolPolicy`, approval routes and
  capability probing are unchanged.
- The route credentials reach the child on one inherited pipe, whose number is
  in `CURIE_CONNECTOR_ROUTES_FD`. The runner writes to it only after it has
  observed the child locked: the kernel refuses `/proc/<pid>/environ` to a
  same-uid reader exactly when the process is not dumpable. A caller never
  observed locked, such as a static binary that ignores `LD_PRELOAD`, gets an
  empty document. The runner writes one JSON document and closes its end:
  `{"<connector>": {"url": "<base URL>", "token": "ccr...."}}`. The document
  holds only the connectors whose routes list this server, and the base URL
  comes from the same `mcp_entry` derivation as the hosted entry, so the
  author writes no URL. The child reads the pipe once at start. After that the
  pipe is empty, and nothing the child execs can read a value from the env or
  the descriptor. A caller started any other way, such as by the runner's
  capability probe or on a tier with no connector scope, finds the name unset
  and has no routes, so it must start without them.
- The runner removes `CURIE_CONNECTOR_ROUTE_TOKENS` from every env mapping in
  the same step as `materialize_connector_caller_headers`. The token's only
  copies in the sandbox are then the runner's memory and the declared
  caller's memory.

A server that is not named as a caller is mounted exactly as today and
receives nothing, and so is a caller with no route this boot (no token, or no
connector scope) or with no lock library to load, since then it holds nothing
to protect. A bundle that declares no `routes:` boots exactly as today.

### 5. Contract change and rollout

`routes:` changes the frozen `packages/plugin-format` contract and its Rust
mirror (`ConnectorSpecDecl` in `cli/src/connector_build.rs`), as ADR-0229's
`proxy_upstream_host` did. The field is additive, but `ConnectorSpec` forbids
unknown keys, so a CLI or API from before the field refuses a bundle that sets
it. `aci-protocol` does not change (decision 2).

The proxy runs from the worker image, so the proxy that understands `ccr` and
`CURIE_CALLER_PROXY_ROUTES` ships with the worker that mints a `ccr`. A worker
mints one only for a bundle that declares `routes:`, and only an API that has
the field can deploy that bundle. A proxy from before this change refuses a
`ccr` as `invalid`, which is correct.

## Consequences

- A bundle can send a large file from its own stdio tool to its own hosted
  connector behind the caller proxy, with a size it declares, and with no
  credential reaching Bash or hooks.
- A leaked `ccr` can do less than a leaked `cct`. It reaches one connector's
  declared routes until `exp`, and never MCP.
- The proxy gains route matching, a second token type and counted streaming.
  The undeclared-path limit tightens for non-POST methods. That is a behavior
  change for a connector that took large PUT bodies through the proxy today.
  Such a connector would declare a route.
- Declared callers stop being CLI-spawned. The runner owns their lifecycle,
  so a crash or a hang in one surfaces as a connector capability failure
  through the runner's probe, as for any other mounted server.
- A declared caller shares the sandbox's uid with Bash. The runner keeps the
  token out of every env, argument and file Bash can read, writes it only to
  a process it has observed locked, and starts that process before any turn's
  Bash exists, from the bundle root. It cannot stop a same-uid process writing
  files that the caller's runtime reads later on its own, such as a module it
  imports lazily from a per-user directory. A caller should load what it needs
  at start.
- The bundle's code is trusted with the token once the child has read it.
  Nothing stops a declared caller from writing the token to a file or passing
  it to a subprocess. That is the bundle author's code, in the same way a
  hosted connector's own credential is its author's. The platform's promise
  is that the token reaches only the processes the bundle declared, and only
  by the descriptor.
- The routes do not appear in the action digest or the capability probe. They
  are not tools, and no model reads them.

## Alternatives considered

1. **Raise or remove the global cap.** Rejected under R3. The cap protects the
   proxy's memory and bounds the grant parse, and a larger global number
   leaves every connector's MCP path open to the same body size. It also fixes
   nothing for this bundle shape, which is refused at admission first.
2. **A declared per-connector limit with no route** (#4917 direction 1). This
   solves the size for MCP-shaped uploads, but on its own it gives bundle code
   no credential, and it sizes `/mcp` along with the upload path.
3. **Give the stdio server the `cct` token.** Rejected under R2. The token is
   good on every connector that admits the agent and on every path,
   including gated MCP tools, which the runner's approval gate would no longer
   see.
4. **Put a token in the stdio server's `--mcp-config` env block.** This is the
   smallest change. Rejected under R1. It adds a copy of the token to the
   CLI's configuration path, and it leaves the token in the server's environ,
   where every process the server execs inherits it and loses the environ
   lock on exec.
5. **A runner-side relay on loopback or a Unix socket.** In this design the
   runner attaches the token, and bundle code sends to a local endpoint.
   Rejected under R1. Bash runs as the same user in the same network
   namespace, so it can reach any endpoint the stdio server can reach. Any
   secret that gates the endpoint is itself a credential that bundle code
   must hold.
6. **The connector pulls the file from platform storage.** This avoids a
   large upload through the proxy, but it needs a new store, a new credential
   for the connector and a new credential for the stdio tool to write with.
   It is a larger surface than a declared route and still has to solve R1 for
   the write.
7. **The NetworkPolicy-bounded second port as the sanctioned shape** (#4917
   direction 3, second branch). Rejected under R1 and R2. The sandbox is the
   only peer, and the sandbox includes Bash.
8. **Let the proxy authenticate the calling process.** The proxy sees a TCP
   peer in another pod. It cannot tell one sandbox process from another, so
   caller scoping has to be enforced where the processes are, in the runner.

## Measurements

These were measured before acceptance on `claude-agent-sdk` 0.2.163 with its
bundled CLI 2.1.286, against a local stand-in for `/v1/messages`, with
`strict_mcp_config` set as the runner sets it:

- An in-process SDK MCP server registered as `plugin:b:review-tools` is
  offered to the model as `mcp__plugin_b_review-tools__review`. A stdio server
  with the same name and tool is offered under exactly the same name.
- A `tools/call` that takes 330 seconds completes and returns its result, on
  the SDK server and on the stdio server alike. Neither mount imposes a
  shorter timeout. The SDK's own 60 second default applies only to control
  requests the SDK sends, not to the MCP messages it answers.

## Tracking

[#4917](https://github.com/curie-eng/curie/issues/4917) carries the problem.
The realizing paths are `ConnectorSpec` and its Rust
mirror, `connector_render._proxy_container`, `curie_connector_proxy.server`
and `curie_connector_proxy.caller`, the worker's token minting in
`curie_worker.binding`, `BootEnv` in `aci-protocol`, and the runner's
`plugin.bundle_mcp_servers` and `connectors.materialize_connector_caller_headers`.
The tests that pin today's starting point are
`apps/worker/tests/test_connector_proxy.py::test_an_admitted_post_over_the_body_limit_never_reaches_the_server`
and
`runner/tests/test_connectors.py::test_a_bundle_stdio_server_cannot_ask_for_the_caller_token`.
The realizing work changes or extends both.
