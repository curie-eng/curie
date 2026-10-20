# 87. The API renders connector objects; the CLI applies them

Date: 2026-07-30
Status: Accepted

Partially superseded by [ADR 0239](0239-explicit-shared-connector-applications-resolve-credentials-for-each-authenticated-caller.md)
for explicit shared applications: application-owned objects replace per-agent
object ownership. The rendering/apply authority split and dedicated mode stand.

Realized by `read_version_connectors` in `apps/api/src/curie_api/routers/agents.py`,
which renders the objects and applies none of them, and by `version_connectors` in
`cli/src/api.rs` with the apply and prune in `cli/src/connectors.rs`, which are
what reach the cluster.

## Context

[ADR 0086](0086-bundles-declare-connectors-the-platform-hosts-them.md) decided
that a bundle declares connectors and the platform hosts them. It did not say
**which component** turns a declaration into running Kubernetes objects, and
that gap has a security consequence, so it is recorded here rather than left to
whichever pull request happened to answer it first.

Three components could plausibly own it, and each already holds some of what
the job needs:

- The **API** holds the bundle. It is the only component that can read
  `connectors.yaml` for an arbitrary stored version.
- The **worker** already creates pods, so it holds cluster-write authority.
- The **CLI** already runs `helm` and `kubectl` under the operator's own
  credentials.

The API is also the component that receives webhooks from the internet. Its
RBAC today is deliberately, narrowly read-only: `pods: list` and
`pods/log: get`, and `apps/api/CLAUDE.md` names keeping it that way as a
load-bearing invariant. Giving the API the ability to create Deployments,
Services, NetworkPolicies, and Secrets would widen the blast radius of any
authentication or deserialization flaw in the one service most exposed to
hostile input — to *create a pod running an attacker-named image with a
mounted credential*.

Deploy-time expansion also has to answer where the connector's credential comes
from. `connectors.yaml` carries secret **names** only (ADR 0086), so something
must resolve them to values at deploy time and put them where the connector pod
can read them.

## Decision

**Rendering and applying are split across two components: the API renders, the
CLI applies.**

`GET /agents/{id}/versions/{vid}/connectors` returns the Kubernetes objects
derived from that version's `connectors.yaml`, plus the `.mcp.json` entries
derived from the Services it defines. It computes and returns; it never applies.

The CLI applies the returned objects with `kubectl`, under the operator's own
credentials, and prunes objects the bundle no longer declares.

Three properties follow, and they are the reason for the split:

1. **Rendering is a pure function**, so the API needs no cluster access to do
   it. Its read-only RBAC is untouched. This is the whole point: the component
   exposed to the internet gains no new authority.
2. **Cluster-write authority stays where it already was.** The operator running
   `curie cluster deploy` already holds `kubectl` credentials sufficient to
   install the chart. Applying connector objects grants nothing new.
3. **Credentials never reach the API.** The CLI resolves each declared secret
   name locally — environment first, then the host vault — and writes the value
   straight into a Kubernetes Secret. The rendered manifest carries a
   `secretKeyRef`, never a literal.

Three consequences of the split are load-bearing and worth naming:

**Deployment context travels as request parameters.** `release`, `namespace`,
and `app_name` are install-time facts the API does not know — the Helm release
name and `nameOverride` live with whoever ran `cluster up`, not in the bundle.
The caller supplies them. `app_name` is *read from the cluster* rather than
re-derived from chart values, because it is the label Rail 1's default-deny
egress selects on ([ADR 0067](0067-controller-networkpolicymanagement-unmanaged-for-rail-1.md));
taking it from the objects the chart actually rendered makes the connector's
allow-rule match by construction. A second derivation could disagree, and a
NetworkPolicy that selects nothing fails silently.

**Applying is not a bare `kubectl apply`.** Every object carries a label naming
the agent that declared it, and each deploy deletes owned objects the bundle no
longer declares — Secrets included. Without the prune, removing a connector
from `connectors.yaml` leaves a pod running with a credential mounted and
nothing referencing it: nothing breaks, so nobody notices.

**Object names are scoped to the agent, not the release.** Curie runs many
agents per release, so a release-scoped name lets two agents that each declare
`grafana` render byte-identical objects and silently overwrite one another,
including the shared credential (#1116). The failure is a dev-tier agent
acquiring a prod token with no error and no log.

## Consequences

Connector hosting works only where a `kubectl`-capable caller drives the
deploy. Git-flow pushes create a Version through the API but do not apply its
connectors; the connectors of the most recent CLI-driven deploy remain in
force. Closing that requires an in-cluster reconciler with cluster-write
authority — a decision this one deliberately defers rather than smuggling into
the API.

Two components must agree on the derived Secret's name. The CLI reads it back
off the rendered `secretKeyRef` rather than re-deriving it, so there is one
source of that rule and no second copy to drift.

The `.mcp.json` entries the endpoint returns are not yet injected into the
sandbox, so an author still hand-writes the connector URL that ADR 0086 set out
to remove (#1118). The split does not cause this, but it does mean the fix
lands in the worker rather than in either component named here.

## Alternatives considered

- **The API renders and applies.** Rejected: it widens the internet-facing
  service's RBAC from read-only to pod-creating, which is the specific outcome
  `apps/api/CLAUDE.md` names as load-bearing to avoid. The convenience is real
  and the exposure is not worth it.
- **The worker renders and applies.** Rejected as the *first* step, not on
  principle. The worker already holds cluster-write authority, so it is the
  natural home for the reconciler that would close the git-flow gap above. But
  it does not run on a deploy — it runs on a turn — so making it the deploy-time
  path would mean a connector appears only when someone next messages the agent.
  Deferred deliberately.
- **The CLI reads `connectors.yaml` and renders locally.** Rejected: the CLI
  would need its own copy of the rendering rules, in a second language, drifting
  from `packages/plugin-format`. It also would not work for a version deployed
  from a git push, where no local checkout of that sha exists.
- **A Helm subchart per connector.** Rejected: it puts connector lifecycle on
  the release's upgrade cycle rather than the agent's deploy cycle, so adding a
  connector to one agent means a chart change and a release upgrade for
  everyone.
