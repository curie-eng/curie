---
seam: Connector host (bundle-declared MCP servers)
kind: CLEAN
impls: 1 (Kubernetes) + in-memory fake
grade: not separately graded
epics:
  - "#1063"
  - "#1184"
order: 20
---
# INTERFACE: Connector host (bundle-declared MCP servers)

> Part of the Curie swappable-seam catalog — see the [seam index](../../interfaces.md).
<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 (Kubernetes) + in-memory fake &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

A bundle **declares** the MCP servers an agent needs; the platform **hosts**
them (ADR-0086). The author writes intent in `connectors.yaml` and never writes
a Deployment, a Service, a Secret reference, container hardening or a
NetworkPolicy again. Under ADR-0087 the API renders those objects but never
applies them, and under ADR-0090 an in-cluster reconciler applies them so an
agent repository needs no CLI and no `kubectl`-capable operator standing next to
the cluster.

The swappable thing is the **workload host**: the half that mutates sits behind
the `ConnectorClient` `Protocol`, and a second implementation substitutes Docker
or compose for Kubernetes behind the same verbs. The decision half — what to
create, what to prune, what has drifted — stays opinionated core and never
imports the cluster module, which is exactly what lets the dangerous logic be
tested against an in-memory fake. A second, narrower port sits beside it:
`ManifestSource`, where the rendered objects come from, so the reconcile step is
testable without an API.

Two orderings and one refusal are load-bearing above the line and a second host
inherits all three: apply before delete, so a part-way failure leaves inert
extra objects rather than a missing Deployment; delete one at a time, continuing
past failures, so one undeletable object cannot strand the rest as orphans; and
refuse to act on any object not carrying this agent's owner label.

## Current contract

A second host implements `ConnectorClient`
(`apps/worker/src/curie_worker/connector_apply.py::ConnectorClient`). It is
deliberately narrow — a wider seam invites the reconciler to grow cluster access
it does not need, and the RBAC behind it is scoped to exactly the object kinds a
connector consists of. Three methods:

- `list_owned(namespace, owner) -> list[dict[str, Any]]`
  (`apps/worker/src/curie_worker/connector_apply.py::ConnectorClient.list_owned`)
  — every connector object labelled for this owner. Each returned object **must**
  carry `kind` and `metadata.name`, because ownership and every prune decision is
  keyed on that pair by `identity`
  (`apps/worker/src/curie_worker/connector_reconcile.py::identity`).
- `apply(namespace, obj) -> None`
  (`apps/worker/src/curie_worker/connector_apply.py::ConnectorClient.apply`) —
  create or update one object.
- `delete(namespace, kind, name) -> None`
  (`apps/worker/src/curie_worker/connector_apply.py::ConnectorClient.delete`) —
  remove one object; missing is success, not an error.

The driver is `execute`
(`apps/worker/src/curie_worker/connector_apply.py::execute`), which consumes a
`ReconcilePlan`
(`apps/worker/src/curie_worker/connector_reconcile.py::ReconcilePlan`) built by
`plan` (`apps/worker/src/curie_worker/connector_reconcile.py::plan`) and returns
an `ApplyReport`
(`apps/worker/src/curie_worker/connector_apply.py::ApplyReport`). Ownership is
the label `OWNER_LABEL`
(`apps/worker/src/curie_worker/connector_reconcile.py::OWNER_LABEL`), and drift
is detected against a hash stamped in `HASH_ANNOTATION`
(`apps/worker/src/curie_worker/connector_reconcile.py::HASH_ANNOTATION`) by
`stamp_hash`
(`apps/worker/src/curie_worker/connector_reconcile.py::stamp_hash`), with
`owner_of` (`apps/worker/src/curie_worker/connector_reconcile.py::owner_of`) the
single ownership reader.

Where the desired objects come from is the second port, `ManifestSource`
(`apps/worker/src/curie_worker/connector_agent.py::ManifestSource`); one agent's
pass is `reconcile_agent`
(`apps/worker/src/curie_worker/connector_agent.py::reconcile_agent`) when the
in-force version has a stored bundle, or `prune_agent`
(`apps/worker/src/curie_worker/connector_agent.py::prune_agent`) when it does
not (`apps/worker/src/curie_worker/connector_loop.py`). `prune_agent` never
renders, treats desired as empty, and deletes no Secret of any name. The
owner label is stamped by `own`
(`apps/worker/src/curie_worker/connector_agent.py::own`).

The declaration side is frozen in the bundle format: `CONNECTORS_FILE`
(`packages/plugin-format/src/plugin_format/connectors.py::CONNECTORS_FILE`) is
`connectors.yaml`, parsed into `ConnectorsFile`
(`packages/plugin-format/src/plugin_format/connectors.py::ConnectorsFile`), a
mapping of name to `ConnectorSpec`
(`packages/plugin-format/src/plugin_format/connectors.py::ConnectorSpec`). A spec
is one of three mutually exclusive forms — hosted by reference (`image`), hosted
from source (`build`, ADR-0113, a `ConnectorBuild`
(`packages/plugin-format/src/plugin_format/connectors.py::ConnectorBuild`) naming
a bundle-relative `context`, its `dockerfile` and the `platforms` to build), or
remote (`url`) — and unlike the rest of
that package it forbids unknown keys: `connectors.yaml` is Curie's own file with
no external producer, so an unrecognised key is a typo rather than a Claude Code
extension to tolerate. Credentials are declared by **name** in three holder
shapes — resolved by Curie, referenced out of band via `SecretRef`
(`packages/plugin-format/src/plugin_format/connectors.py::SecretRef`), or carried
sealed by the bundle (see [sealed-credential](../sealed-credential/INTERFACE.md)).

Rendering is a pure function outside the port entirely: `render`
(`packages/plugin-format/src/plugin_format/connector_render.py::render`), driven
by the API through `render_connector_manifests`
(`apps/api/src/curie_api/bundles.py::render_connector_manifests`) and served at
the version subresource `read_version_connectors`
(`apps/api/src/curie_api/routers/agents.py::read_version_connectors`), with
`read_connectors` (`apps/api/src/curie_api/bundles.py::read_connectors`) parsing
the file and `connector_mcp_entries`
(`apps/api/src/curie_api/bundles.py::connector_mcp_entries`) deriving the
`.mcp.json` the sandbox dials.

A `build` spec never reaches `render` unresolved. `read_version_connectors` puts
the declaration and the bundle's `connectors.lock.yaml` (parsed by
`read_connector_lock`,
`apps/api/src/curie_api/bundles.py::read_connector_lock`) through `apply_lock`
(`packages/plugin-format/src/plugin_format/connector_lock.py::apply_lock`), which
rewrites every `build` connector into an ordinary `image` one carrying the locked
digest — so `render`, the MCP entry derivation and `is_hosted` are untouched by
the new form, and there is exactly one place a mutable tag can be refused.
`apply_lock` reads a recorded fact and never resolves or builds one, which is what
keeps the API a pure renderer under ADR-0087, and `render` raises rather than
emitting a null image if it is ever handed an unresolved `build` spec.

The reconciler is off by default. `WorkerConfig`
(`apps/worker/src/curie_worker/config.py::WorkerConfig`) reads
`CURIE_CONNECTOR_RECONCILE` (default false), `CURIE_CONNECTOR_RECONCILE_INTERVAL_S`
(default 60 seconds) and `CURIE_CONNECTOR_APP_NAME`, and reuses `CURIE_RELEASE`
and `CURIE_NAMESPACE`, which must agree with the values the runner's connector
scope is built from — the runner dials a Service by the name they produce and
the reconciler creates the Service by the same name. Enabling the flag is what
makes the chart grant the worker create, patch and delete on the connector
object kinds, so a worker that is not reconciling does not hold that grant.

Who may call a hosted connector is decision 7 of
[ADR-0168](../../adr/0168-one-installation-hosts-several-bot-identities.md).
When `CURIE_CONNECTOR_CALLER_SIGNING_KEY` is set, the worker's `boot_env`
(`apps/worker/src/curie_worker/binding.py::BindingResolver.boot_env`) signs the
resolved agent's name with `mint`
(`apps/worker/src/curie_worker/caller_token.py::mint`), and `render_worker`
(`packages/aci-protocol/src/aci_protocol/session.py::BootEnv.render_worker`)
emits it as `CURIE_CONNECTOR_CALLER_TOKEN` with the connector scope. The runner's
`derive_mcp_servers`
(`runner/src/curie_runner/connectors.py::derive_mcp_servers`) gives each hosted
entry the header `X-Curie-Caller`
(`runner/src/curie_runner/connectors.py::CALLER_HEADER`) with the token's
placeholder, and gives none to a remote or fallback URL. The signing key never
enters a sandbox
(`apps/worker/src/curie_worker/sandbox/types.py::HOST_APPLICATION_CREDENTIAL_ENV_NAMES`).

Hosted rendering requires a caller proxy with a valid public key: `render`
refuses a hosted connector without one with `hosted_connector_requires_caller_key`.
A remote connector returns no objects and needs no caller key. Every hosted
connector that renders has the proxy in front of it
(`packages/plugin-format/src/plugin_format/connector_render.py::ConnectorProxy`).
The proxy runs from the worker image as `python -m curie_connector_proxy`, pulled
with the worker's pull policy and pull secrets, and holds the public keys, the
connector's `admits` list, resolved at render
(`packages/plugin-format/src/plugin_format/connector_render.py::resolved_admits`),
the connector's gated tool patterns, its name and the grant store URL
(`apps/worker/src/curie_connector_proxy/server.py::ProxyConfig`).
The Service keeps the connector's port, so the URL and the allowed hosts do not
move, and lands it on the proxy's port. Both rendered NetworkPolicies open only
that port, so nothing Curie renders opens the server's own port.

Callers that are not agents, such as a keep-alive Job, carry no token. For them
the render adds a second Service, `<connector Service>-direct`
(`packages/plugin-format/src/plugin_format/connector_render.py::direct_service_name`),
which selects the same pods and targets the server's own port
(`packages/plugin-format/src/plugin_format/connector_render.py::render_direct_service`).
No rendered policy opens that port, so a sandbox cannot use it. It admits only
the peers of an operator-applied ingress policy naming the server's port,
because policies are additive. An install with no caller key cannot render a
hosted connector, including its `-direct` Service.

The proxy admits a request whose token either configured key
verifies, that has not expired and that names a listed agent
(`apps/worker/src/curie_connector_proxy/caller.py::decide`) and, for a POST,
that passes the grant check below. It strips the header and forwards the
request to the server over loopback with its `Host` unchanged and its body as
sent
(`apps/worker/src/curie_connector_proxy/server.py::make_app`). A connector that
declares `proxy_upstream_host` (ADR 0229) gets that loopback host and the
server's port as `Host` instead, on admitted requests only
(`apps/worker/src/curie_connector_proxy/server.py::ProxyConfig`). It is for a
server that refuses a non-loopback `Host` on a loopback connection. A refused
request never reaches the server, and a `-direct` caller keeps its own `Host`.
On that forward
it sets `X-Curie-Agent` from the verified token. When the token also carries
`run` and `work_item` (ADR 0178), it sets `X-Curie-Run` and
`X-Curie-Work-Item` from those claims. Inbound copies of the three headers
are dropped first, so a caller cannot supply them. A token without the pair
sets no run headers. A connector trusts the identity headers only on this
loopback forward. The `-direct` Service skips the proxy, so a connector
reached there has no run. It checks every
path the same way, the MCP client's OAuth discovery and registration requests
included. Its log line quotes the path, and aiohttp's own error and access logs
redact a caller token.

An admitted POST is also read before it is forwarded. When its body holds a
`tools/call` for a gated tool, the proxy needs exactly one
`X-Curie-Connector-Grant` header whose grant verifies against the same keys,
has not expired, and names this agent, this connector, this tool and the call's
canonical arguments
(`apps/worker/src/curie_connector_proxy/server.py::_grant_refused`). The grant's
`jti` is then spent once in Valkey with `SET NX EX`, so a replayed grant, a
missing grant store or a failed spend refuses the call. A body with two or more
gated calls is refused before any grant is spent. The grant header is stripped
before the forward. A refused grant check answers with the JSON-RPC refusal
below, reason `grant_required`
(`apps/worker/src/curie_connector_proxy/caller.py::GRANT_REQUIRED`). On a path
the connector does not declare as a route, a body over 1 MiB is refused with a
plain-text 403, not the JSON-RPC refusal, and never reaches the server. That
holds for every method: a POST is read first, and any other method is streamed
but counted as it is relayed.

Only the runner holds a caller token, and it gives the token only to the
derived hosted entries
(`runner/src/curie_runner/connectors.py::materialize_connector_caller_headers`).
A bundle's own stdio server, its hooks and Bash never receive it.

A hosted connector may declare `routes:`, non-MCP routes its bundle's own
stdio servers call through the proxy
([ADR 0240](../../adr/0240-a-hosted-connector-declares-the-routes-its-bundles-stdio-servers-call-through-the-caller-proxy.md)).
The render gives the proxy each route's path, methods and `max_body_bytes` as
`CURIE_CALLER_PROXY_ROUTES` and leaves `callers` out
(`packages/plugin-format/src/plugin_format/connector_render.py::_proxy_container`).
A request whose method and exact raw path match a route is admitted only with
a `ccr` route token that verifies, has not expired, names an admitted agent
and names this connector
(`apps/worker/src/curie_connector_proxy/caller.py::decide_route`). A `cct`
there is `invalid`, a `ccr` anywhere else is `invalid`, and so is any token on
the route's path with another method or on another spelling of it
(`apps/worker/src/curie_connector_proxy/server.py::_shadows_route`). A route
body is never buffered. A `Content-Length` past the route's limit is refused
before the server is contacted, and a body without one is counted as it is
relayed, so the caller gets a plain-text `413` unless the server answered
before the limit was reached
(`apps/worker/src/curie_connector_proxy/server.py::_CountedBody`). A route has
no grant check: the runner's gate on the stdio tool call that sends it is the
approval boundary.

The worker reads which mounted hosted connectors declare routes from the
version's stored bundle, cached by bundle and agent, and only when it holds a
caller key (`apps/worker/src/curie_worker/kernel/route_tokens.py::route_connectors`).
It mints one `ccr` per such connector with the caller token's claims and
expiry into `CURIE_CONNECTOR_ROUTE_TOKENS`
(`apps/worker/src/curie_worker/binding.py::CONNECTOR_ROUTE_TOKENS_ENV`), which
the per-claim Secret delivers
(`apps/worker/src/curie_worker/sandbox/claim_tokens.py::WORKER_TOKEN_ENVS`).
No caller key, no connector scope or an unreadable bundle mints none, and the
route then refuses `missing`. The runner removes the name from every env at
boot (`runner/src/curie_runner/route_callers.py::take_route_tokens`). It starts
each declared caller itself, at boot and before it serves a turn, from the
bundle root, and mounts it behind an in-process SDK MCP relay under the name
the plugin loader would have used
(`runner/src/curie_runner/route_callers.py::CallerRelay`). The caller gets the
shell and hook env plus its own `env` block and the environ lock library in
`LD_PRELOAD`. Its routes arrive on one inherited pipe named by
`CURIE_CONNECTOR_ROUTES_FD`, written only once `/proc` shows the caller is no
longer dumpable (`runner/src/curie_runner/route_callers.py::proc_locked`). A
caller with no route this boot, or with no lock library to load, is left to
the CLI with no routes.

A request the token check does not admit gets a 403 carrying a JSON-RPC error and no
challenge, and never reaches the server; the shape is frozen in
`tests/vectors/connector-caller-refusal.json`, and the runner reports it as the
connector refusing this sandbox
(`runner/src/curie_runner/mcp_tool_capability.py::ConnectorCapabilityFailure`).
The token wire is frozen in `tests/vectors/connector-caller-token.json`. An API
with no public key refuses hosted rendering rather than relying only on network
policy. A runner booted by a worker that held no key carries no
token, and every proxied connector refuses its calls until the next turn on its
thread that a worker holding the key takes, which claims a fresh one
(`apps/worker/src/curie_worker/kernel/claim.py::_boots_differently`). The minting
worker and the proxy ship in the same release, so during the upgrade that gives
an install its key this covers every runner from before it and any runner a
worker pod the roll has not replaced yet boots; `charts/curie/README.md` says
how long that window lasts.

A `cct` may also carry `principal`, the verified human who made the request
([ADR 0270](../../adr/0270-hosted-connectors-receive-the-verified-requester-as-a-signed-caller-claim.md)
decision 2). It is an object with exactly `tenant_id`, a lowercase hyphenated
UUID, `issuer`, 1 to 2048 characters, and `subject`, 1 to 256 characters. Each
is a string that encodes to UTF-8 (a lone surrogate is refused), compared
exactly. The `cct` key sets are closed: `agent` and `exp`, optionally the `run` and `work_item` pair, and
optionally `principal`. A principal is complete or absent, so a null, empty,
partial, extra-key or wrongly typed one, and a duplicate key at any depth of
the payload, is `invalid`
(`apps/worker/src/curie_connector_proxy/caller.py::claims`). The worker
refuses before boot to build a principal that breaks these rules or to mint a
token past the verifier's 4096 characters
(`apps/worker/src/curie_worker/caller_token.py::CallerPrincipal`,
`apps/worker/src/curie_worker/caller_token.py::MAX_TOKEN_CHARS`). The proxy
returns the principal only on an admitted decision
(`apps/worker/src/curie_connector_proxy/caller.py::Principal`), never on an
`expired` or `not_admitted` refusal, and never logs it. Its decision log line
ends `principal=present` or `principal=absent` on an admitted `cct` and has no
principal field on a refusal or a `ccr`. A requester adds no
admission: `admits` still judges only the agent. A `ccr` never carries a
principal, and one that does is `invalid`
(`apps/worker/src/curie_connector_proxy/caller.py::decide_route`), so a route
holds no human authority.

The worker mints a principal only for an agent whose operator flag
`connector_requester_identity` is on. It is off by default, and every other
agent's token keeps its exact bytes. The proxy drops every inbound
`X-Curie-Principal` header
(`apps/worker/src/curie_connector_proxy/caller.py::PRINCIPAL_HEADER`), in any
letter case and however often it repeats, on `cct` and `ccr` paths alike,
before the forward
(`apps/worker/src/curie_connector_proxy/server.py::_NOT_FORWARDED`). This
release does not yet set that header from the verified claim, so a connector
sees no principal header at all; decision 4's projection is a later change.

Enable `connector_requester_identity` only after the api, the worker and the
agent's connector proxies all run this release, and redeploy the agent first
if its connector proxies predate it. A mixed state fails closed rather than
leaking or widening authority: an older worker cannot read the turn authority
answer that carries the requester and refuses the turn, and an older proxy
refuses a principal-bearing `cct` as `invalid`, so the agent's connector calls
are refused until the rollout completes.

## Implementations today

One production host plus a fake:

- **Kubernetes:** `KubernetesConnectorClient`
  (`apps/worker/src/curie_worker/connector_k8s.py::KubernetesConnectorClient`),
  in-cluster or kubeconfig auth. Every Kubernetes API call made through the
  reconciler's `ConnectorClient` stops in that module; Deployment-shaped
  observations still reach the probe logic described below. Writes are **server-side applies** with `force=True`
  under a stable field manager, `FIELD_MANAGER`
  (`apps/worker/src/curie_worker/connector_k8s.py::FIELD_MANAGER`): not
  create-then-replace, because a rendered Service declares no `clusterIP` and
  replacing a live one without it is rejected outright, and because server-side
  apply can remove a field we previously set and no longer declare, which a merge
  patch cannot express. `force=True` is what makes a hand-edited field come back,
  which is the drift correction ADR-0090 asks for. The object kinds are
  `CONNECTOR_KINDS`
  (`apps/worker/src/curie_worker/connector_k8s.py::CONNECTOR_KINDS`) — Deployment,
  Service, Secret and NetworkPolicy — and anything else raises `UnsupportedKind`
  (`apps/worker/src/curie_worker/connector_k8s.py::UnsupportedKind`).
- **Fake:** an in-memory client in `apps/worker/tests/reconcile/test_connector_apply.py`,
  which is what lets the half that can delete a live connector be tested without
  a cluster. There is no second production host behind the port, but a dev-tier
  Docker host exists in the CLI, outside it (see Known leakage).

The loop that drives it is `ConnectorReconcileLoop`
(`apps/worker/src/curie_worker/connector_loop.py::ConnectorReconcileLoop`) over
`HttpManifestSource`
(`apps/worker/src/curie_worker/connector_loop.py::HttpManifestSource`), which is
the `ManifestSource` implementation that fetches rendered objects from the API.

## Known leakage

The port is a real `Protocol`, and the values crossing it are Kubernetes:

The capability probe consumes Deployment details beyond the identity and owner
fields used by reconciliation. `_ObservingClient`
(`apps/worker/src/curie_worker/connector_loop.py::_ObservingClient`) retains the
Deployments returned by `list_owned`; `ProbeTrigger`
(`apps/worker/src/curie_worker/connector_probe.py::ProbeTrigger`) reads the caller
proxy container's connector-name env entry and calls `action_digest.observe`
(`apps/worker/src/curie_worker/action_digest.py::observe`). That helper requires
Deployment generation, observed generation, desired, updated, available and
total replica counts, plus the server container's pinned image. These reads
make no additional Kubernetes API call, but a second host must supply that
Deployment shape for capability probes to become eligible.

- **The action executor reads Deployments around the port.** The action executor
  (`apps/worker/src/curie_worker/action_executor_loop.py`) and the digest recorder
  (`apps/worker/src/curie_worker/action_digest.py::DigestAttributingRecorder`) each
  call `read_namespaced_deployment` by name on the raw `AppsV1Api` from
  `apps/worker/src/curie_worker/connector_k8s.py::connector_deployments_api`, not
  through `ConnectorClient`. `apps/worker/src/curie_worker/run.py` composes both
  whenever `WorkerConfig.executor_reads_connectors` is on and an internal worker
  token is configured (without one, neither the executor nor the recorder wrapper
  starts), with or without the connector reconciler. A second host therefore needs a Deployment get API too:
  without a readable Deployment the executor refuses with
  `connector_digest_unavailable` before any sandbox claim, and the recorder
  records a null digest.

- **The exchanged type is raw cluster wire JSON.** Every verb trades
  `dict[str, Any]` holding `kind`, `apiVersion` and `metadata`, and reads come
  back as unparsed JSON on purpose — the rest of the reconciler compares against
  what the API rendered, which is camelCase, and the generated typed models would
  hand back snake_case attributes and quietly compare unequal on every field. A
  Docker or compose host would have to synthesize Kubernetes-shaped dictionaries
  to satisfy a port that never mentions Kubernetes in its own signatures.
- **Rendering is Kubernetes-specific and sits outside the port.** `render`
  (`packages/plugin-format/src/plugin_format/connector_render.py::render`) emits a
  Deployment, a Service and two NetworkPolicies, plus the `-direct` Service
  behind a caller proxy, and it runs in the API, not
  behind `ConnectorClient`. Swapping the host therefore swaps only the applier;
  the second host needs a second renderer too, and no port covers that half.
- **There is no selector.** Unlike the substrate seam's `CURIE_SANDBOX_SUBSTRATE`,
  nothing chooses a connector host at runtime: `_build_connector_loop`
  (`apps/worker/src/curie_worker/run.py::_build_connector_loop`) imports and
  constructs `KubernetesConnectorClient` directly. The flag that exists switches
  the reconciler on and off, not which host it drives, so a second implementation
  starts by editing the composition root.
- **Server-side-apply semantics are assumed, not stated.** Drift correction
  depends on field-manager ownership and on apply being able to remove an
  undeclared field; prune depends on server-side label selection, applied by the
  API server rather than client-side because a filter that can be forgotten is one
  that prunes another agent's connectors. None of that is expressible in the three
  method signatures, so a host without those semantics satisfies the `Protocol`
  and still behaves differently.
- **There are three appliers, only one of them behind the port, and they
  disagree.** The third is a dev-tier Docker host: for the skill and local
  tiers the CLI starts each hosted connector as a plain container
  (`ConnectorStartSpec` in `cli/src/docker.rs`), tags it with its own
  `CONNECTOR_COMPONENT_LABEL` and reaps undesired ones by that label, then
  blocks on its own readiness wait, `wait_for_connectors_ready`, called from
  `bring_up_local` and `start_skill_connectors` in `cli/src/commands/connector_images.rs`. That
  path goes around both `render` and `ConnectorClient`: it neither consumes the
  rendered Kubernetes objects nor implements the port, so it is leakage, not a
  second implementation. Of the two cluster appliers, the CLI applies the same
  rendered objects on the `cluster deploy`
  path (`cli/src/connectors.rs`) with a plain client-side `kubectl apply`, prunes
  in one bulk labelled delete, mints and deletes the owned Secret, stamps the pod
  template's `curie.dev/connector-secret-digest` rollout annotation
  (`CONNECTOR_SECRET_DIGEST`), and stamps no drift hash; the reconciler neither
  renders nor stamps that annotation and only tolerates it, because its
  containment drift check ignores keys the live object adds. The reconciler
  server-side applies, prunes one object at a time,
  never touches that Secret, and stamps a hash on every apply — so a
  CLI-created object reads as drift and is adopted on the reconciler's first
  pass. The ownership label that decides what gets deleted is also two
  hand-maintained copies of one string, `OWNER_LABEL`
  (`apps/worker/src/curie_worker/connector_reconcile.py::OWNER_LABEL`) and a Rust
  constant in `cli/src/connectors.rs`, with no codegen or drift gate tying them
  together the way the ACI contract is tied. The derived Bearer selection rule is
  another Python/Rust copy of one rule, the same class as `OWNER_LABEL`. The
  renderer (`packages/plugin-format/src/plugin_format/connector_render.py::_derived_headers`)
  and the CLI (`cli/src/connector_build.rs`) both accept an explicit
  `bearer_secret` or one plain-string secret and exclude an implicit `SecretRef`.
  Unlike `OWNER_LABEL`, the cases are frozen in
  `tests/vectors/connector-derived-bearer.json`, which the API's independence check
  (`apps/api/src/curie_api/remediation_verifier.py::header_secret_names`) reads as a third copy of
  the same rule. A hosted connector may also declare `no_client_auth: <reason>` beside
  `bearer_secret`, stating why its server authenticates no client. Every copy then derives no
  `Authorization` header, including for the single plain-secret shape, and the CLI binds no secret
  name into the sandbox, while every declared secret keeps its pod-side `secretKeyRef` delivery
  and the rendered Deployment, Service and NetworkPolicy stay byte-identical because the reason is
  never rendered. The reason must be non-empty, it cannot sit beside `bearer_secret`, and a `url`
  connector cannot declare it. For example:

  ```yaml
  connectors:
    docs:
      image: ghcr.io/example/docs-mcp:1
      port: 8000
      secrets: [UPSTREAM_CLIENT_ID, UPSTREAM_CLIENT_SECRET]
      no_client_auth: >-
        In-cluster MCP server reached over the pod network; it authenticates no
        client and the boundary is its ingress NetworkPolicy.
  ```
- **The Docker dev-tier host enforces no caller.** Its connector containers share
  one network with no policy and no caller proxy, so any container on it can
  reach a connector's port, the trade ADR-0009 makes for the local substrate.
- **The port's own docstring miscounts itself.** `ConnectorClient` is introduced
  as "deliberately four verbs" while declaring three; the cluster module states
  the true shape, four object kinds and three verbs. A second implementer reading
  the port first will look for a fourth method that was never there.

## Cross-links

- **Related seam:** [substrate](../substrate/INTERFACE.md) — `SandboxClient` is the other Kubernetes-behind-a-Protocol seam, and it is the discipline this one copies; it covers the runner runtime, not connector workloads.
- **Related seam:** [bundle-format](../bundle-format/INTERFACE.md) — `connectors.yaml` is an additive, platform-facing declaration in the bundle; declaring intent is a bundle's job, being the platform's implementation is not.
- **Related seam:** [sealed-credential](../sealed-credential/INTERFACE.md) — the third credential holder shape a connector spec can declare.
- **Epic(s):** #1063 — bundles declare connectors and the platform derives the objects; #1184 — the in-cluster connector reconciler and its RBAC
- **Vision doc:** [architecture-vision.md](../../architecture-vision.md) — connector hosting is not one of the six swap-readiness Jobs; not separately graded
- **ADR(s):** [ADR-0086](../../adr/0086-bundles-declare-connectors-the-platform-hosts-them.md) — bundles declare connectors, the platform hosts them; [ADR-0087](../../adr/0087-the-api-renders-connector-objects-the-cli-applies-them.md) — the API renders connector objects and never applies them; [ADR-0090](../../adr/0090-a-reconciler-applies-connectors-so-agent-repos-need-no-cli.md) — a reconciler applies them, so agent repos need no CLI; [ADR-0009](../../adr/0009-per-agent-connector-auth.md) — per-agent secrets and connector credentials

## Per-request approval tickets (ADR 0271)

This section freezes the wire contract for
[ADR 0271](../../adr/0271-per-request-approver-sets-and-separation-of-duties.md).
The pure signer and verifier are available; approval creation, policy, nonce
storage and proxy header wiring are separate implementation slices. These
contracts do not claim that those paths already enforce this policy.

The connector helper is
`packages/curie-internal/src/curie_internal/approver_ticket.py`.
`sign_ticket(payload, signing_key=...)` takes a closed payload and an Ed25519
signing key. `execution_arguments_sha256(arguments)` hashes execution data.
The API entry point `verify_ticket` in
`apps/api/src/curie_api/approver_tickets.py` takes the registered public key,
key id, complete execution arguments and an injected integer Unix clock.
It reads only `arguments["approval_ticket"]`; a separate token cannot select
another approver set. A missing or non-string carried member refuses.

These helpers perform no database, route, current identity, key registration,
requester-association, ceiling or nonce-spend checks. Creation owners must add
those checks atomically, including comparison of the signed tenant, agent,
connector, exact tool and route with the verified creation context. A parsed
ticket is not execution authorization. JSONB numeric round-trip refusal belongs
to the creation slice, not this pure contract.

### Ticket bytes and payload

Carry the opaque ticket in `granted_arguments.approval_ticket` as
`cat1.<payload>.<signature>`. Encode the canonical ASCII JSON payload bytes in
unpadded base64url. Ed25519 signs exactly the ASCII bytes `cat1.<payload>`.
Encode its 64-byte signature in unpadded base64url. Reject padding, alternate
base64 spellings including nonzero trailing pad bits, non-ASCII ticket text,
incorrect segment counts, duplicate or unknown keys at every object depth,
non-finite numbers, wrong types and noncanonical JSON spellings. The entire
ticket is at most 16,384 ASCII characters.

The top-level object has exactly these required members:

| Member | Wire value |
| --- | --- |
| `version` | Integer `1`; booleans refuse. |
| `purpose` | String `approval_request`. |
| `tenant_id` | Canonical lowercase hyphenated tenant UUID. |
| `agent_id` | Canonical lowercase hyphenated agent UUID. |
| `connector` | Exact registered connector name, 1 through 256 UTF-8 bytes. |
| `tool` | Exact upstream execution tool name, 1 through 256 UTF-8 bytes. |
| `route` | Exact named approval route, 1 through 256 UTF-8 bytes. |
| `key_id` | Exact registered selector key id, 1 through 256 UTF-8 bytes. |
| `requester` | Original requester's canonical principal object. |
| `nonce` | Unpadded base64url of 32 cryptographically random bytes, single use within the registered selector scope. |
| `issued_at` | Integer Unix seconds in `[0, 9223372036854775807]`. |
| `expires_at` | Integer Unix seconds, strictly after issue and at most 300 seconds later. |
| `arguments_sha256` | 64 lowercase SHA-256 hexadecimal characters over execution arguments as described below. |
| `approvers` | Array of 1 through 16 distinct selected canonical principals. |

Each principal has exactly `tenant_id`, `issuer` and `subject`; its tenant
must equal the payload tenant. The canonical identity types come directly from
`packages/curie-internal/src/curie_internal/identity_context.py`:
`CanonicalUUID`, `Issuer` and `Identifier`. A UUID is its exact lowercase
hyphenated 36-character spelling. Issuer is 1 through 2,048 Unicode code points;
subject is 1 through 256 Unicode code points. Count a non-BMP character once,
not as two UTF-16 code units or its UTF-8 byte count. Values must encode as
UTF-8, so lone surrogates refuse. Overall ticket and grant limits still apply
when individual fields fit their own bounds.

Compare issuer and subject exactly, without case folding, trimming or Unicode
normalization. Selection uniqueness uses `(tenant_id, issuer, subject)`, ignoring
recipient metadata. Array order is signed and preserved; membership is a set.

### Candidate recipient accounts use the identity owner's scope

Only an approver may additionally have `recipient_accounts`, an array of 1
through 16 closed objects. Omit the optional member when absent; null refuses.
Each account has exactly these members, using the same vocabulary and limits as
`InvocationScopeV1` and `ContextNamespaceV1` in the identity context owner:

| Member | Meaning and limit |
| --- | --- |
| `provider` | The identity sender/namespace provider identifier, as in `InvocationScopeV1.provider`, such as `slack` or `email`; 1 through 256 Unicode code points. It is not the canonical person's IdP issuer. |
| `installation_id` | The canonical UUID in `InvocationScopeV1.provider_installation_id`, naming the platform `ProviderInstallation.id` row, not a provider's external account id or application label. |
| `namespace` | The closed object `{id, authority, kind, key}` described below, not a string workspace label. |
| `account_id` | The provider-native person/account id, as in `InvocationScopeV1.native_id` and `IdentityLink.provider_native_id`; 1 through 256 Unicode code points. |

The namespace object has exactly `id` (canonical UUID of the tenant's
`IdentityNamespace` row), `authority` (0 through 2,048 Unicode code points),
`kind` and `key` (each 1 through 256 Unicode code points). Authority is the
receiving installation's endpoint authority; empty is valid for a global
provider. Kind is the identity owner's exact namespace kind, such as
`slack_workspace`, `slack_enterprise` or `email_domain`; key is that namespace's
exact native key, such as a workspace id or canonical email domain. These are
opaque identity values, not strings to reinterpret or normalize.

Before delivery, the identity owner must verify the entire candidate scope:
tenant, provider, installation UUID, namespace UUID and its authority/kind/key,
and native account id. It must establish the active account-to-person link to
the selected canonical tenant/issuer/subject and check current installation and
namespace lifecycle. An unknown, inconsistent, inactive, revoked or unavailable
association refuses. Candidate metadata itself proves none of those facts.
Provider-specific account canonicalization belongs to the identity owner.

### Canonical JSON and string hygiene for other implementations

The ticket JSON encoding is Python's current production JSON spelling, frozen
by the shared vectors. Its rules are:

1. Object keys at every depth sort lexicographically by Unicode code point
   sequence, not UTF-16 code unit order, locale or normalized text. Arrays retain
   input order. Use one comma between members and one colon between key and
   value, with no whitespace outside strings. JSON literals are lowercase.
2. Quote strings with double quotes. Escape double quote and reverse solidus
   with a reverse-solidus prefix. Use the conventional one-letter escapes
   `b`, `t`, `n`, `f`, `r` for backspace, tab, line feed, form feed and carriage
   return. Other C0 values use lowercase `\u00xx` escapes.
3. Ticket payloads are ASCII: every code point from U+007F upward uses lowercase
   hexadecimal `\uXXXX` spelling. Non-BMP scalar values use a lowercase UTF-16
   surrogate pair, high surrogate first; U+1D11E is `\ud834\udd1e`. Reject lone
   surrogates and alternate spellings, including uppercase hexadecimal digits.
4. Do not escape solidus `/`, less-than `<`, greater-than `>`, ampersand `&` or
   single quote. Do not use optional escapes for ordinary ASCII characters.
   Thus an escaped solidus or HTML-style Unicode escape refuses even if it
   would decode to the same string. Re-encoding must reproduce every payload
   byte exactly. Raw UTF-8 non-ASCII and a raw BOM before the JSON object refuse.

All ticket text obeys this deliberate hygiene rule, in addition to its length
and grammar: refuse U+0000 through U+001F and U+007F through U+009F anywhere,
including DEL U+007F. Also refuse leading or trailing code points from exactly
this whitespace set: U+0009 through U+000D, U+001C through U+0020, U+0085,
U+00A0, U+1680, U+2000 through U+200A, U+2028, U+2029, U+202F, U+205F and
U+3000. The overlap with C0/C1 is intentional. Empty namespace authority is
permitted. Internal whitespace outside C0/C1 is preserved.

BOM U+FEFF and zero-width space U+200B are deliberately accepted in opaque
identity text, including principal strings, and preserved even at either end;
they are not whitespace in this rule. U+2028 and U+2029 are accepted internally
but refused at either end. Ticket encoding escapes all of these; do not strip
or normalize them when comparing identity facts. Shared positive and negative
vectors cover these choices.

### Execution argument digest and numeric spelling

Remove only the top-level `approval_ticket` member. Nested members with that
name remain execution data. Serialize the remaining object using production
canonical arguments, then SHA-256 its UTF-8 bytes. Object sorting, separators,
quote/reverse-solidus escaping and C0 escaping are as above. Unlike the ticket
payload, argument JSON leaves DEL U+007F and all valid non-ASCII scalars as
literal UTF-8, including BOM, zero-width space, U+2028/U+2029 and non-BMP values.
Do not ASCII-escape, HTML-escape or normalize them. Ticket text hygiene does not
restrict execution argument string contents.

Integers use decimal with no plus sign or redundant leading zeroes. Floating
values use finite IEEE 754 binary64, preserving their numeric type and negative
zero. Use the shortest decimal significand that round-trips to the same
binary64 value under round-to-nearest/even; choose the nearest such decimal,
with an even final digit for a tie. For decimal exponent below -4 or at least
16, use one digit before the decimal point and lowercase `e`; retain a negative
exponent sign or an explicit positive `+`, with at least two exponent digits.
Otherwise use fixed-point notation. Fixed-point integral floats retain `.0`;
an integral scientific mantissa omits it. Examples are `1` versus `1.0`, `-0.0`,
`0.0001`, `1e-05`, `1000000000000000.0`, `1e+16`, `1e-07` and
`0.30000000000000004`. Reject non-finite numbers rather than emitting NaN or
Infinity. Do not substitute another language's default JSON float formatter.

The existing `tests/vectors/action-canonical-arguments.json` corpus freezes
these bytes, including key sorting across the BMP boundary and float thresholds.
Both new readers exercise it. `tests/vectors/approver-ticket.json` freezes
payloads, signatures, character boundaries, namespace scope, refusal codes and
size cases. The complete grant argument object still includes the ticket;
only the preparation digest excludes it.

### Key selection and preparation expiry

`unverified_key_id(ticket)` reads a bounded, canonical, closed payload and
returns its `key_id` without checking the signature or registration. This value
is untrusted lookup input only. A well-shaped forged ticket can return any
well-shaped key id. Select the current registered key within the expected
selector/tenant/agent scope, then verify the original ticket with that key and
compare its key id. Never use the peek result as proof of identity, selection,
registration, freshness or execution authority. Unknown registrations refuse.
The ordinary `parse_ticket` path verifies the Ed25519 signature before parsing
claims; only the explicitly untrusted key-selection helper parses first.

Preparation verification accepts exactly `issued_at <= now < expires_at` with
an integer clock and no clock-skew allowance. Preparation expiry is distinct
from approval expiry. After accepted creation, connectors may use `parse_ticket`
for signature and shape without freshness, then check scope, requester, argument
digest and current local policy without reapplying preparation expiry. Creation
dedupe and single-use nonce enforcement belong to the database transaction.

### Frozen refusal codes

Every `TicketError` exposes its stable `code`; no supplied ticket content is
included in the error. Evaluate framing/signature/shape before key-id comparison,
then preparation time, then argument digest. The API validates argument data and
the carried member before calling that verification path.

| Code | Condition |
| --- | --- |
| `invalid_ticket` | Bad framing/encoding, parser or canonical-byte failure, bad signature, closed shape/type/bound violation, or missing/non-string carried ticket. Parsing an oversized ticket also uses this code. |
| `invalid_arguments` | Execution arguments are not a JSON object or cannot be canonically encoded as finite UTF-8 JSON. |
| `key_mismatch` | The signed key id differs from the expected registered key id supplied to the pure verifier. |
| `preparation_expired` | The injected clock is not an integer, precedes issue, or is at or after preparation expiry. |
| `arguments_mismatch` | The signed digest differs from the actual execution-argument digest. |
| `ticket_too_large` | The signer would emit a ticket over 16,384 ASCII characters. |
| `arguments_too_large` | The complete canonical execution arguments exceed 16,384 UTF-8 bytes. |
| `grant_too_large` | The conservative complete `ccg` encoding exceeds 4,096 ASCII characters. |

Each negative ticket vector carries its expected `code`, asserted by both
readers. Validly signed malformed-payload cases also check their signature
precondition independently, so a signature error cannot hide the intended
parser refusal. Size and untrusted-key vectors assert their own named codes.

### Executable size budget and realistic approver ceiling

Before creating an approval, call `check_execution_size` with the complete
persisted arguments, final ticket included, and the exact worker agent name,
connector and upstream tool. It rejects canonical arguments over 16,384 UTF-8
bytes or an eventual `ccg` over 4,096 ASCII characters. The calculation includes
JSON escaping of the complete canonical argument string, base64url expansion,
prefix, separators and 86 signature characters. It reserves the largest signed
64-bit expiry and a 36-character canonical UUID `jti`, so the returned length is
an upper bound on the actual grant. Nothing widens either existing limit.

Sixteen is a shape ceiling, not a promise that sixteen approvers can execute.
The shared capacity vectors use the same compact execution arguments, agent
`acme-bot`, connector `access` and tool `execute`, with these observed budgets:

| Principal shape | Largest fitting set and reserved grant length | First refusal |
| --- | --- | --- |
| Short synthetic issuer (22 characters), short subjects | 12 approvers, 3,943 characters | 13 approvers, 4,141 characters |
| Okta-shaped issuer (39 characters), 21-character subjects | 10 approvers, 4,090 characters | 11 approvers, 4,335 characters |
| Entra-shaped issuer (76 characters), UUID subjects | 7 approvers, 4,094 characters | 8 approvers, 4,431 characters |
| Short principals, each with one full recipient account | 3 approvers, 3,481 characters | 4 approvers, 4,110 characters |

These are examples, not provider limits: Entra-shaped sets are practically about
six to seven in this compact example; seven leaves only two reserved characters.
Larger arguments, longer scope strings, different key ids or recipient accounts
reduce the ceiling further. Non-ASCII identity text also expands under ASCII
escaping. The creation refusal is `grant_too_large`, before any pending approval
is committed. Always compute the actual budget rather than relying on a count.

### Verified spent-grant header

`X-Curie-Grant-Verified` carries the spent grant's canonical lowercase hyphenated
UUID `jti`, never an approval row id or an approval boolean. The proxy must strip
every inbound occurrence in every casing, including repeated headers. Only after
successful signature, expiry, agent, connector, exact tool and complete argument
verification and a successful single-use spend may it set exactly one header.
Never emit it for a failed spend, a grant-less call, a batch or a non-tool route.
An attested request must contain exactly one `tools/call`.

A static `X-Curie-Connector-Grant` header can accompany unrelated calls after
resume or warm reuse. For a gated tool, missing, invalid, mismatched or spent
grants refuse. For an ungated tool, consider attestation only if the grant's
signature, tool and canonical arguments match this call, then require all
remaining checks and spend. A matching expired or spent grant refuses. Otherwise
strip the incidental grant and forward under ordinary rules without attestation
or spend. Batches never receive an attestation.

The connector trusts this header only on its own proxy connection and reads it
from per-request context. Direct callers cannot establish the attestation.
A protected execution handler must refuse its absence even if the tool is absent
from the proxy's gated list. The connector also checks its ticket and current
local policy before the effect. API answer and resume checks own selected actor
and separation policy; this header supplies no second approval policy or result
lookup API. An uncertain upstream outcome never permits replay of a spent grant.
