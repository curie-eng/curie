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
Today the rendered ingress policy, `render_ingress_networkpolicy`
(`packages/plugin-format/src/plugin_format/connector_render.py::render_ingress_networkpolicy`),
is still the whole check: it admits every sandbox of the release. What this
release adds is the caller's identity, carried but not yet checked. When
`CURIE_CONNECTOR_CALLER_SIGNING_KEY` is set, the worker's `boot_env`
(`apps/worker/src/curie_worker/binding.py::BindingResolver.boot_env`) signs the
resolved agent's name with `mint`
(`apps/worker/src/curie_worker/caller_token.py::mint`), and `render_worker`
(`packages/aci-protocol/src/aci_protocol/session.py::BootEnv.render_worker`)
emits it as `CURIE_CONNECTOR_CALLER_TOKEN` with the connector scope. The runner's
`derive_mcp_servers`
(`runner/src/curie_runner/connectors.py::derive_mcp_servers`) then gives each
hosted entry the header `CALLER_HEADER`
(`runner/src/curie_runner/connectors.py::CALLER_HEADER`) with the token's
placeholder, and gives none to a remote or fallback URL. The token stays in the
sandbox env, because the MCP client expands the header from it; the signing key
never enters a sandbox
(`apps/worker/src/curie_worker/sandbox/types.py::HOST_APPLICATION_CREDENTIAL_ENV_NAMES`).
The wire is frozen in `tests/vectors/connector-caller-token.json`. With no key
set, nothing is minted and the boot env is unchanged.

## Implementations today

One production host plus a fake:

- **Kubernetes:** `KubernetesConnectorClient`
  (`apps/worker/src/curie_worker/connector_k8s.py::KubernetesConnectorClient`),
  in-cluster or kubeconfig auth. Everything Kubernetes-shaped in the reconciler
  stops in that module. Writes are **server-side applies** with `force=True`
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

- **The exchanged type is raw cluster wire JSON.** Every verb trades
  `dict[str, Any]` holding `kind`, `apiVersion` and `metadata`, and reads come
  back as unparsed JSON on purpose — the rest of the reconciler compares against
  what the API rendered, which is camelCase, and the generated typed models would
  hand back snake_case attributes and quietly compare unequal on every field. A
  Docker or compose host would have to synthesize Kubernetes-shaped dictionaries
  to satisfy a port that never mentions Kubernetes in its own signatures.
- **Rendering is Kubernetes-specific and sits outside the port.** `render`
  (`packages/plugin-format/src/plugin_format/connector_render.py::render`) emits a
  Deployment, a Service and two NetworkPolicies, and it runs in the API, not
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
  `bring_up_local` and `start_skill_connectors` in `cli/src/commands.rs`. That
  path goes around both `render` and `ConnectorClient`: it neither consumes the
  rendered Kubernetes objects nor implements the port, so it is leakage, not a
  second implementation. Of the two cluster appliers, the CLI applies the same
  rendered objects on the `cluster deploy`
  path (`cli/src/connectors.rs`) with a plain client-side `kubectl apply`, prunes
  in one bulk labelled delete, mints and deletes the owned Secret, and stamps no
  drift hash; the reconciler server-side applies, prunes one object at a time,
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
  Unlike `OWNER_LABEL`, the four cases are frozen in
  `tests/vectors/connector-derived-bearer.json`.
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
