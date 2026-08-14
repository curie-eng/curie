# 105. One release owns a cluster's shared singletons; every other release declares what it needs from them

Date: 2026-08-14

Status: Draft

Implements [#1535](https://github.com/curie-eng/curie/issues/1535).

Builds on [ADR-0023](0023-controller-networkpolicy-rbac-cluster-read-namespace-mutate.md)
(the controller's NetworkPolicy RBAC split), [ADR-0059](0059-sandbox-is-a-bounded-resource-envelope.md)
decision 5 (the two PriorityClasses), and [ADR-0067](0067-controller-networkpolicymanagement-unmanaged-for-rail-1.md)
(`networkPolicyManagement: Unmanaged`). Each of those decided what a single
install renders. None of them decided who owns the result when a second install
lands on the same cluster.

## Context

`charts/curie` is a single umbrella chart, and the mental model behind it is one
release per cluster. That model is already false in practice: issue #1535
reports two installs of different chart versions side by side on one k3s node
(an rc.3 era release and an rc.4 era release), and the same shape shows up
whenever an operator runs a staging and a production namespace, or an evaluator
keeps a scratch install next to a real one.

The chart renders three classes of object, and only the first is safe to
duplicate.

| Class | Examples | Scope |
| --- | --- | --- |
| Per release | `SandboxTemplate`, `SandboxWarmPool`, Deployments, Services, Secrets, the Rail 1 NetworkPolicies | namespaced, prefixed `<release>-` |
| Cluster singleton, chart rendered | the vendored agent-sandbox controller and its `agent-sandbox-system` namespace, its ClusterRoles and ClusterRoleBindings, `agent-sandbox-controller-networkpolicies-read`, the `curie-platform` and `curie-sandbox` PriorityClasses | cluster scoped, fixed names |
| Cluster singleton, outside Helm entirely | the four `crds/` CustomResourceDefinitions, and the image tags resolved by the node container runtime | cluster scoped, never templated with release identity |

Every object in the second and third rows carries a name that is a constant, not
a function of `.Release.Name`, so two releases cannot both hold one. Helm 3
refuses to install an object another release owns, which is the loud half of the
problem and is already worked around with `create: false` and
`controller.deploy: false` values. The quiet half is what #1535 is actually
about, and it has two independent instances.

**Instance one: the grants a shared controller needs follow the wrong release.**
`templates/agent-sandbox.yaml` renders the controller and all four of its
NetworkPolicy RBAC objects inside one `{{- if .Values.agentSandbox.controller.deploy }}`
guard (lines 29 and 130), while the runner `SandboxTemplate` renders under the
separate `agentSandbox.deploy` guard (line 131). Rendering the chart at
`main` (`0.7.0-rc.4`) as a consumer proves the split:

```
helm template c2 charts/curie -n curie-b --set agentSandbox.controller.deploy=false
  kind: SandboxTemplate            1 occurrence
  agent-sandbox-controller-networkpolicies*   0 occurrences
```

The consumer therefore asks a cluster shared controller to reconcile objects in
`curie-b` while granting it nothing in `curie-b`. The namespaced
`Role`/`RoleBinding` that ADR-0023 requires renders only into the owner's
`.Release.Namespace`. The cluster wide read half is worse: it renders only from
the owner's chart version, so an rc.3 owner supplies the rc.3 grant set to every
consumer regardless of what the consumer's own templates assume. That is exactly
the reported failure. An rc.3 controller lacked the RBAC rc.4 templates assume,
rc.4 `SandboxClaim`s never bound, and nothing surfaced an error; the operator
recovered it by hand applying a namespace `Role` and `RoleBinding` for
`networkpolicies`.

The silence is structural, not incidental. A claim that never binds is
indistinguishable from a claim that is slow, so the observable is a run that
times out rather than a rejection. The same silence is available through the
CRDs: Helm installs `crds/` before any template and never upgrades or deletes
them, so the first install on a cluster pins the CRD schema for every later one,
and the apiserver prunes unknown fields against a structural schema. A newer
template writing a field an older CRD does not declare is accepted and then
silently dropped.

**Instance two: an imported image tag is a cluster scoped mutable name.** The
GHCR path is already versioned; the chart renders
`ghcr.io/curie-eng/curie-runner:0.7.0-rc.4` from `Chart.AppVersion`. The offline
path is not. `values-dev.yaml` and the chart README both hardcode `curie-api:local`,
`curie-dispatcher:local`, `curie-worker:local`, `curie-ui:local`, and
`curie-runner:latest`, imported into the node runtime with
`docker save "$img" | ssh <node> 'sudo k3s ctr images import -'`. The node image
store is one namespace shared by every release on the cluster, so the second
install's import silently rebinds the first install's tags. In #1535 that
produced an ACI 0.2.9 against 0.3.0 protocol skew that persisted until the images
were rebuilt and the tags restored.

PriorityClasses are the mild case: the name collision is loud, and the workaround
(`create: false`, repoint `name`) is already documented in `values.yaml`. They
are in scope here only because the ownership question is the same one, and
because `resourceQuota.sandboxPriorityClassName` binds a release's quota
`scopeSelector` to a class name the release may not own.

## Decision

**A cluster has exactly one owner release for its shared singletons. Every other
release is a consumer: it renders nothing cluster scoped, and it declares the
contract revision it needs from the owner, which is preflighted and fails closed
on skew.**

### 1. One value declares ownership, and it gates the whole owner set

Ownership today is inferred from three unrelated flags
(`agentSandbox.controller.deploy`, `priorityClasses.platform.create`,
`priorityClasses.sandbox.create`) that an operator can set in any combination,
including combinations that render an incoherent cluster. It becomes one
declaration, `clusterSingletons.owner` (default `true`, so the single release
case is unchanged), and the owner set is defined as exactly:

- the vendored agent-sandbox controller, its `agent-sandbox-system` namespace,
  its webhook Service, and every ClusterRole and ClusterRoleBinding it needs,
  including `agent-sandbox-controller-networkpolicies-read`;
- the `curie-platform` and `curie-sandbox` PriorityClasses;
- the four agent-sandbox CRDs, with the caveat in decision 4.

A PriorityClass is a cluster wide ranking, so two releases holding different
opinions about it is not a coherent state to support; the singleton form is
correct and only the ownership needed naming. The existing per flag values stay
readable for an operator who wants finer control, but the owner flag is the
supported surface, and a consumer release setting it to `false` gets a coherent
render rather than a combination.

### 2. Grants follow the consumer, not the owner

Any namespace scoped permission the shared controller needs in order to serve a
release renders in **that release's** namespace, gated on `agentSandbox.deploy`,
never on ownership. Concretely the ADR-0023 namespaced
`Role`/`RoleBinding` for `networkpolicies` moves out of the
`controller.deploy` block and renders wherever a `SandboxTemplate` renders.

This is the load bearing half of the decision, and it strictly improves the
ADR-0023 posture rather than relaxing it. ADR-0023's guarantee is no cluster wide
mutate, and that is untouched: mutating verbs stay confined to namespaces that
actually contain Curie sandboxes, and each such namespace grants them for itself
rather than inheriting them from whichever release happened to install the
controller.

### 3. A contract revision, a declared floor, and a fixed upgrade order

The skew contract between chart templates and the shared controller is a single
monotonically increasing integer, `clusterContract`, carried by the chart.

- The owner release **stamps** what it installed: labels on the
  `agent-sandbox-system` namespace and the controller Deployment recording the
  contract revision, the upstream controller version, and the owning release and
  namespace.
- Every chart version **declares** the minimum revision its templates require.
  The declaration is bumped whenever templates begin to depend on something the
  shared install must provide: a new grant, a newer upstream controller, a CRD
  field, or a changed controller side default.
- Every install and upgrade **preflights** the stamp by `lookup` and fails
  closed when the installed revision is below what it requires, naming the owner
  release and the required order. An absent stamp means an externally managed
  controller, which is a supported configuration and requires an explicit
  acknowledgement value; the operator owns compatibility from there.
- The **order is owner first**. The owner's chart version is at least every
  consumer's, and a consumer is never permitted to run ahead of the contract.

The revision is deliberately not the chart version. Most chart versions change
nothing a shared controller must provide, and tying the two would make every
patch release a cluster wide upgrade event.

### 4. CRD upgrades are an explicit cluster operation, covered by the same contract

Helm's `crds/` handling means the owner release installs the CRDs and no
subsequent `helm upgrade` touches them, so the chart cannot honestly claim to
manage their lifecycle. It is stated as what it is: CRD upgrade is an explicit
operator action on the owner release, surfaced through `curie` rather than a
copied `kubectl apply`, and the contract revision covers CRD schema so a
consumer needing a newer field fails its preflight instead of writing a field the
apiserver prunes.

### 5. An image tag entering a shared runtime carries release identity

Because the node image store is cluster scoped and its tags are mutable, no
first party image Curie imports or references may resolve through a name that is
constant across releases. The GHCR defaults already satisfy this by deriving
from `Chart.AppVersion`. `values-dev.yaml` and the chart README's import
instructions move to the same derivation (`curie-<service>:<appVersion>`), and
the fixed `:local` and `curie-runner:latest` tags are removed from the documented
path. A multi release cluster pulling from GHCR should pin digests, which the
chart's `digest` field already supports.

### 6. The failure mode must be loud

The class in #1535 is silent by construction, so a decision that only fixes the
mechanics would leave the next instance just as hard to diagnose. Skew is
therefore surfaced in three places: the preflight in decision 3, the cluster
status and doctor output (installed revision, required revision, owner release),
and the claim timeout diagnostic, which names the missing grant or the contract
gap instead of reporting a generic timeout.

## Alternatives rejected

**Split the chart into a `curie-platform` cluster chart and a `curie` release
chart.** Architecturally the cleanest expression of the decision: ownership stops
being a value and becomes a package, `helm uninstall` on a release can no longer
remove a singleton another release depends on, and the version skew contract
becomes an ordinary chart dependency constraint. Rejected for now on cost and on
narrative. It is a breaking packaging change for every existing install, it
splits the one command install that the evaluation path depends on, and it cuts
against ADR-0097's one file declares an installation. The consequence of
rejecting it is accepted and real: under this ADR, `helm uninstall` on the owner
release is a cluster level operation that removes the controller, the RBAC, and
the PriorityClasses out from under every consumer, and only documentation and the
owner stamp stand between an operator and that outcome. If multi release clusters
become a supported product configuration rather than an operational reality, this
alternative is the natural successor ADR.

**Keep the current flags and document the multi release procedure.** The cheapest
option, and the status quo plus a runbook. Rejected because the two instances in
#1535 are both silent, and a runbook cannot make a silent failure loud. It also
leaves decision 2 unaddressed: no combination of existing values renders the
namespaced grant a consumer needs, so the documented procedure would have to end
in hand applied YAML, which is exactly how the reporter recovered.

**Let each release run its own namespaced controller.** Would make the whole
question disappear. Not available: the upstream agent-sandbox controller LISTs
and WATCHes at cluster scope with no namespace flag (ADR-0023 and issue #350
established that a namespaced Role alone can never satisfy its informer), and the
CRDs it serves are cluster scoped objects regardless. Rejected as not
implementable against the vendored upstream.

**Use Helm resource adoption so a second release can take over a singleton.**
Rejected because adoption transfers ownership rather than sharing it. The
singleton would then be deleted by whichever release last claimed it, converting
a loud install time collision into a quiet uninstall time outage, which is the
wrong direction for the failure class this ADR exists to close.

## Consequences

- The single release install, which is every documented install today, renders
  identically. `clusterSingletons.owner` defaults to `true`, and the contract
  preflight passes trivially against a stamp the same release just wrote.
- A consumer release gains the namespaced RBAC it never had, so the
  reported "claims silently never bind" state is not reachable through the
  missing grant path, at matched or skewed versions.
- Version skew becomes an install time failure with a named remedy instead of a
  runtime silence. The cost is that a consumer install can now be refused for a
  reason outside its own namespace, which is a new class of install failure an
  operator has to understand.
- The owner release becomes load bearing for the cluster. Uninstalling it breaks
  every consumer, and nothing enforces that beyond the stamp and the
  documentation. This is the accepted cost of not splitting the chart, and it is
  the trigger to revisit that alternative.
- The offline dev path changes shape: imported tags become version derived, so
  existing local scripts and any operator muscle memory around `curie-*:local`
  break once, deliberately.
- The contract revision is a new artifact that has to be maintained honestly. A
  template change that needs a new grant and does not bump it reintroduces the
  exact class this ADR closes, which argues for tying the bump to a gate rather
  than to reviewer memory.
- Nothing here makes Curie multi tenant. It makes multi release clusters fail
  honestly. Real tenancy remains issue #158.
