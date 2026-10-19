# 129. One release owns a cluster's shared singletons; every other release declares what it needs from them

Date: 2026-08-14

Status: Accepted

Tracked by [#1535](https://github.com/curie-eng/curie/issues/1535).

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
NetworkPolicy RBAC objects inside one
`{{- if .Values.agentSandbox.controller.deploy }}` guard, while the runner
`SandboxTemplate` renders under the separate `agentSandbox.deploy` guard.
The same render was re-run after rebasing this Draft onto `next`; the consumer
still receives one `SandboxTemplate` and neither controller NetworkPolicy role:

```
helm template c2 charts/curie -n curie-b --set agentSandbox.controller.deploy=false
  kind: SandboxTemplate            1 occurrence
  ClusterRole/agent-sandbox-controller-networkpolicies-read   0 occurrences
  Role/agent-sandbox-controller-networkpolicies               0 occurrences
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
`curie-dispatcher:local`, `curie-mail-adapter:local`, `curie-worker:local`,
`curie-ui:local`, and `curie-runner:latest`, imported into the node runtime with
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
release is a consumer: it renders no owner-scoped cluster templates, installs no
CRDs, and declares the contract it needs from the owner. Compatibility is
preflighted and fails closed on skew.**

### 1. One value declares ownership and gates the templated owner set

Ownership today is inferred from three unrelated flags
(`agentSandbox.controller.deploy`, `priorityClasses.platform.create`,
`priorityClasses.sandbox.create`) that an operator can set in any combination,
including combinations that render an incoherent cluster. It becomes one
declaration, `clusterSingletons.owner` (default `true`, so the single release
case is unchanged), and the templated owner set is defined as exactly:

- the vendored agent-sandbox controller, its `agent-sandbox-system` namespace,
  its webhook Service, and every ClusterRole and ClusterRoleBinding it needs,
  including `agent-sandbox-controller-networkpolicies-read`;
- the `curie-platform` and `curie-sandbox` PriorityClasses.

A PriorityClass is a cluster wide ranking, so two releases holding different
opinions about it is not a coherent state to support; the singleton form is
correct and only the ownership needed naming. The existing per flag values stay
readable for an operator who wants finer control, but the owner flag is the
supported surface, and a consumer release setting it to `false` gets a coherent
template render rather than a combination. The four agent-sandbox CRDs are also
owner-only, but Helm processes `crds/` before values and cannot gate them with
this flag. Decision 4 therefore makes `--skip-crds` part of the supported
consumer install contract instead of pretending the value controls them.

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

### 3. A versioned contract range and a fixed upgrade order

The skew contract between chart templates and the shared controller is an epoch
plus monotonically increasing revision, `clusterContract`, carried by the chart.
Compatible additions increment the revision; a change that cannot serve older
consumers increments the epoch. Within one epoch, a newer owner is required to
remain backward compatible down to the floor it stamps.

- The owner release **stamps** what it installed: labels on the
  `agent-sandbox-system` namespace and the controller Deployment recording the
  contract epoch, current revision, oldest compatible consumer revision, the
  upstream controller version, and the owning release and namespace.
- Every chart version **declares** its contract epoch and required revision. The
  revision is bumped whenever templates begin to depend on something the shared
  install must provide: a new grant, a newer upstream controller, a CRD field,
  or a changed controller-side default.
- Every install and upgrade **preflights** the stamp by `lookup` and fails
  closed unless the epochs match and the consumer's required revision falls
  between the owner's compatibility floor and current revision. The error names
  both ranges, the owner release, and the required order.
- An owner upgrade also reads every consumer registration from decision 6 and
  refuses any proposed epoch or compatibility floor that would exclude one of
  them. Raising the floor is therefore safe only after every registered
  consumer has upgraded into the retained range.
- An absent stamp has two explicit branches. `owner=true` may bootstrap only
  when no foreign fixed-name singleton exists and the installed CRDs equal the
  packaged schemas; an existing singleton without a stamp is a loud adoption
  conflict. `owner=false` never turns absence into an acknowledgement bypass:
  an external manager must publish the same verifiable contract stamp and pass
  live controller capability, RBAC, and CRD-schema probes before the consumer
  installs.
- For a compatible revision, the **order is owner first**. The owner's contract
  revision is at or above every consumer's required revision within the same
  epoch, and a consumer is never permitted to run ahead of that range. An epoch
  transition is not an ordinary owner-first Helm upgrade: `curie cluster
  contract migrate` drains every registered consumer, proves no active claim or
  sandbox remains, upgrades the owner and CRDs, upgrades and re-registers every
  consumer in the new epoch, then reopens admission. No old- and new-epoch
  consumers run concurrently; a failed migration leaves admission closed and
  reports the exact incomplete step.

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

A supported consumer install always passes `--skip-crds`; `curie` adds it when
`clusterSingletons.owner=false`, and the documented direct-Helm command includes
it. CI executes both owner and consumer commands and fails if the consumer path
submits any CRD. Helm cannot infer this flag from chart values, so omitting it is
an unsupported invocation that `curie doctor` reports rather than a protection
the chart claims to enforce.

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

Every consumer also writes a namespaced registration naming its release,
namespace, owner, epoch, and required revision. An owner pre-delete hook lists
those registrations through read-only cluster RBAC and refuses uninstall while
another consumer remains. `curie cluster owner handoff` transfers the stamp and
registrations to a compatible replacement first. A separate explicit force path
may remove an owner during disaster recovery, but it names the affected
consumers and is never the default `helm uninstall` behavior.

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
release is a cluster-level operation, so decision 6 must refuse it while
consumers remain and provide an explicit handoff. The value and hook are still
more moving parts than a separate package. If multi-release clusters become a
supported product configuration rather than an operational reality, this
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

- The single-release topology and default remain unchanged: the one release is
  the owner because `clusterSingletons.owner` defaults to `true`. Its rendered
  product resources gain ownership metadata and lifecycle hooks; the
  absent-stamp first-owner preflight verifies no foreign singleton and matching
  CRDs before permitting the owner to stamp what it installs.
- A consumer release gains the namespaced RBAC it never had, so the
  reported "claims silently never bind" state is not reachable through the
  missing grant path, at matched or skewed versions.
- Version skew becomes an install time failure with a named remedy instead of a
  runtime silence. The cost is that a consumer install can now be refused for a
  reason outside its own namespace, which is a new class of install failure an
  operator has to understand.
- The owner release becomes load bearing for the cluster. Its pre-delete hook
  refuses removal while consumers remain; planned replacement uses the handoff
  path, and only an explicit disaster-recovery force can bypass it. This is still
  more operational coupling than a split platform chart and remains the trigger
  to revisit that alternative.
- The offline dev path changes shape: imported tags become version derived, so
  existing local scripts and any operator muscle memory around `curie-*:local`
  break once, deliberately.
- The contract revision is a new artifact that has to be maintained honestly. A
  template change that needs a new grant and does not bump it reintroduces the
  exact class this ADR closes, which argues for tying the bump to a gate rather
  than to reviewer memory.
- Consumer installs have a non-value requirement because Helm cannot condition
  `crds/`: they use `--skip-crds`, preferably through `curie`, and the direct
  Helm path is documented and tested with that flag.
- Nothing here makes Curie multi tenant. It makes multi release clusters fail
  honestly. Real tenancy remains issue #158.
