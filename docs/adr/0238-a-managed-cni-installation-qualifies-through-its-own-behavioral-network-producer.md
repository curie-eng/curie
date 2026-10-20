# 238. A managed CNI installation qualifies through its own behavioral network producer

Date: 2026-10-19

Status: Accepted

Accepted on 2026-10-20 with explicit maintainer approval.

This decides the managed cloud CNI producer and the production
installation form that
[ADR 0236](0236-the-commissioning-operator-splits-provisioner-and-control-processes.md)
leaves open.

Ref [#4557](https://github.com/curie-eng/curie/issues/4557).

It extends two clauses of ADR 0236 and supersedes none:
- "Network producers are selected per CNI": the sentence "Until such a producer
  exists, the command qualifies only disposable local installations" stops
  applying to an installation whose CNI has a producer accepted under this ADR.
- "Command surface": this ADR defines the production installation form that
  sentence anticipates ("A `curie cluster` form follows once a production CNI
  producer exists").

It applies, without changing, the per-installation campaign rule of
[ADR 0224](0224-protected-runtime-commissioning-precedes-installation-acceptance.md),
the provisioning authority of
[ADR 0191](0191-protected-hook-delivery-authority.md), the source invariants of
[ADR 0190](0190-automated-hook-sources-cannot-widen-their-tool-access.md) and
`PROTECTED-HOOK-LANE-8` of the
[delivery lane specification](../superpowers/specs/2026-10-02-protected-hook-lane.md).

## Context

ADR 0224 requires every installation intended for production to run its own
commissioning campaign while real-source admission remains closed. Success in
one installation never qualifies another. ADR 0236 makes the provisioner measure
preventive network enforcement with a producer for the installation's actual CNI.
Its first producer is the local Cilium producer. An installation whose CNI has
no producer refuses before the campaign starts.

Read together, these rules mean no production cluster on a managed cloud CNI can
obtain Qualification or Readiness. An example is a cloud provider's VPC CNI with
its network-policy agent. Many production clusters run such a CNI, so automated
remediation under
[ADR 0203](0203-automated-remediation-is-a-pre-qualified-action-the-platform-executes-and-verifies.md)
cannot use the protected lane on them.

A managed CNI differs from the local Cilium case in three ways that matter for
measurement.
- **The engine is opaque.** The policy agent is installed and upgraded by the
  cloud provider. The provisioner cannot read its endpoint identities or policy
  maps the way it reads Cilium state. Only its behavior is observable.
- **Pod addresses come from the cloud network.** Addresses are recycled
  quickly from warm pools. Node-originated traffic and new pods may be treated
  differently from steady-state pods. A policy agent may also admit flows that
  were established before a policy change.
- **The cluster is not disposable.** It runs other workloads, including the
  ordinary Curie release. Its administrators will not give the measuring
  identity cluster-admin, and the lane must not depend on it.

`PROTECTED-HOOK-LANE-8` already requires verification of actual CNI policy and
forbids treating additive NetworkPolicy objects as a restrictive intersection.
It also requires a separate cluster or node authority domain when ordinary
identities keep node-capable rights. ADR 0236 states the minimum for a
managed CNI producer: source-identity and spoof measurement, and refusal of
retained connections after the original workload is replaced.

## Proposed decision

### Measurement is behavioral; configuration is identity

A managed CNI producer decides enforcement only from **active probes against
owned listeners**. Each probe asserts a denied direction is blocked before any
allowed direction is trusted. A configuration read never counts as an enforcement
observation, whether it comes from the add-on, the agent, a NetworkPolicy object
or provider documentation.

The producer still reads configuration, but only to build the **CNI identity**
that binds and invalidates the measurement (see below). An observation the
producer cannot make is `unavailable`. Under ADR 0236, any unavailable required
observation refuses.

### Required observations

The producer must measure each case below against this installation's own
protected namespace, broker, mediator and runner pod template. It must measure
every IP family the cluster serves.

1. **Non-vacuity.** Before any other case, a probe that no policy allows cannot
   reach an owned listener in the protected namespace. If that probe succeeds,
   the producer reports that enforcement is absent and the campaign refuses.
2. **Source-identity enforcement.** The broker and mediator accept connections
   only from the pods their policy selects. The producer probes from:
   - an unselected pod in the protected namespace;
   - a pod in an owned foreign namespace;
   - a pod in the ordinary release namespace.

   Each probe runs on the same node as the target and on a different node.
   The producer also records that an ordinary identity cannot create a pod
   carrying the protected selector labels in the protected namespace. Label
   forgery is the label-based policy's spoof path, so that refusal comes from
   the guard and is part of this case.
3. **Address spoofing refusal.** A probe pod with only a raw-socket capability
   sends traffic to the broker and mediator. The traffic carries the source
   address of a currently allowed protected pod and of a recently terminated
   one. The owned listener records that no spoofed packet was delivered, both
   from the same node and from another node.
4. **First-packet denial.** A new unselected pod is refused from its first
   packet, including when it reuses an address that a terminated protected pod
   held. The probe sends as its container starts, and its pod creation is
   repeated across a bounded window. A new pod that a policy agent admits until
   policy propagates fails this case. The producer reports address reuse it
   could not provoke within the bound as `unavailable`; it never treats that as
   a pass.
5. **Retained-connection refusal.** First, a connection established while
   allowed passes no further data within a fixed bound after the original
   workload is replaced. Second, it passes no further data after a policy change
   removes the allowance. The engine may keep connection-tracking state. If it
   does and the established flow keeps passing data, this case fails. Broker
   session termination under ADR 0191 is still required, but it does not
   substitute for this network observation.
6. **Egress denial.** A pod with the runner's policy reaches only its declared
   egress peers. It is refused toward:
   - the internet;
   - the node's instance metadata endpoint;
   - the Kubernetes API server;
   - node addresses;
   - other cloud network addresses outside the declared peers;
   - external DNS resolvers.
7. **Node-local traffic.** The producer records whether traffic from host-network
   pods or the node itself reaches protected pods. If it does, the producer must
   also show that no ordinary identity can obtain host-network, privileged,
   host-path or node-proxy execution on any node eligible for protected pods.
   That evidence comes from effective-RBAC and admission refusals. If it cannot
   show this, the campaign refuses. The operator must then use a separate node
   or cluster authority domain, as `PROTECTED-HOOK-LANE-8` requires.
8. **Policy authority.** No ordinary identity can create, change or delete a
   NetworkPolicy, a provider-specific policy object or the policy agent's
   configuration that affects the protected namespace. The producer runs every
   probe above with all existing policies present.

These are the network, hostile-peer and retained-peer facts, and the CNI and Node
identity, that ADR 0236 assigns to provisioner producers. The local Cilium
producer remains a separate producer with its own realization.

### The measuring identity holds no cluster-admin

This ADR separates installation from measurement.

**Installation** is a one-time step a cluster administrator performs from the
provisioner-owned bootstrap. It creates:
- the protected and probe namespaces;
- the admission guard;
- the RBAC and NetworkPolicies;
- the measuring identity's bindings.

**Measurement** uses a separate identity with only these rights:
- create, relabel and delete pods, and read their status and logs, in owned
  probe namespaces;
- create probe pods in the protected namespace only through a guard-admitted
  probe template that carries no protected credential;
- create probe pods in the ordinary release namespace for the campaign only,
  through a binding that is revoked at cleanup;
- create, change and delete NetworkPolicies in owned probe namespaces only;
- read Nodes, the CNI and policy-agent workloads, and admission configuration;
- create SubjectAccessReviews to measure effective RBAC.

Probe namespaces may carry a pod-security exception for a raw-socket
capability. They never permit host network, host path, privileged containers or
node proxy access. If the provider's add-on configuration is readable only
through the cloud API, the CNI identity read may use a read-only cloud principal
scoped to describe that cluster and its add-ons. That principal grants no
measurement authority.

The verifier also proves this least-privilege property. A SubjectAccessReview
showing the measuring identity holds any right beyond this list refuses the
campaign.

### Same channel, same verifier, same records

The producer runs in the ADR 0236 provisioner process. Its facts enter the
existing signed request and reply channel as the network, hostile, retained-peer
and CNI digests the control process already binds. The provisioner re-observes
them on every guard call.

The independent verifier, the Qualification and Readiness publishers and their
record grammar are unchanged. The same is true of the permit, the bootstrap and
the manifest. The CNI fact names its producer kind and the identity below. The
realizing specification defines that internal fact's shape. It is evidence
bound into reply digests, not a new closed record.

### A production installation runs its own sealed campaign

The production form of the operator command qualifies a fresh protected
installation inside the target cluster. It uses the ADR 0224 rules unchanged:
- its broker, trust roots, principals, runtime identity and namespaces are
  created for it;
- its final tuple is selected before the campaign;
- the only sources are owned synthetic sources;
- the only target is an owned read-only fixture.

Real-source admission stays closed throughout. No real source policy is bound to
the protected runtime, and no production Qualification or Readiness exists until
the verifier accepts. The ordinary release keeps running and is a hostile peer
for the campaign, not a participant.

The provisioner process runs outside the cluster under the measuring identity
and the provisioner signing key. Cleanup removes probe workloads, probe
bindings, campaign targets and candidate authority. The measured protected
controls are preserved for activation under ADR 0224.

### What invalidates the qualification

The CNI identity is a digest over the following:
- CNI plugin and policy-agent image digests;
- the agent's enforcement mode and managed add-on configuration;
- the kube-proxy or dataplane mode;
- the control-plane version;
- the admission guard configuration;
- for every node eligible for protected pods, its node image, kernel and
  kubelet identity.

A change to any of these invalidates the campaign. So does a new eligible node
whose identity was not measured, or a change to the eligible node set. The
installation then needs a new campaign before Readiness can be issued again.

The provisioner re-observes the identity at each Readiness issuance and on every
guard call. A mismatch refuses, so the bounded Readiness lifetime is the longest
exposure to an unobserved change. Installation observations are never reused.

Operators must hold managed automatic upgrades of the CNI, the policy agent and
eligible node images while the lane is active. Otherwise they accept lane
closure until a new campaign passes.

### Out of scope

- Any substrate other than Kubernetes.
- Connector-bearing profiles; the profile stays `qualified_connectors=[]`.
- Compromise of the cloud provider control plane, of cluster administrators or
  of a node.
- Clusters where ordinary identities keep node-capable rights on eligible nodes.
  These refuse.
- Serverless pod runtimes and Windows nodes, which refuse until a producer
  measures them.
- Data exfiltration through allowed peers or the cluster resolver.
- Activation itself, which remains the ADR 0224 transition.

Each additional managed CNI needs its own producer realization and evidence
under this contract. The first producer does not qualify another provider's CNI.

## Consequences

Production clusters on a managed CNI gain a defined path to Qualification and
Readiness without a CNI migration, cluster-admin measurement or a new record
format. The cost is a heavier campaign: probes span nodes and IP families, and
bounded address-reuse provoking adds time. The command now also has a production
form that must be qualified on a real cluster before any claim of support.

Some managed policy agents keep established flows after a policy change, or
admit new pods before policy propagates. These fail cases 4 or 5 and cannot
qualify. Their agents may offer a stricter enforcement mode; an installation
that enables it is measured under that mode's identity. A cluster that cannot
pass remains unqualified, and that outcome is explicit.

Pinning node images and the CNI version during lane operation conflicts with
managed auto-upgrade defaults. It is an operating cost the operator accepts
explicitly.

## Alternatives considered

- **Require a self-managed CNI such as Cilium on the production cluster.**
  Rejected as a precondition. Replacing or chaining a managed cluster's CNI is a
  disruptive change to the whole cluster, outside Curie's authority. A Cilium
  measurement on one cluster would still not qualify another. Where an operator
  already runs Cilium, its producer applies to the production form.
- **A dedicated node pool with its own policy enforcement.** An example is a
  tainted pool whose pods use cloud network security groups or subnet controls.
  Deferred. It moves the measured boundary into cloud network controls that need
  a cloud API producer and broader cloud credentials. It also does not remove the
  need for in-cluster probes. It stays the documented remedy when case 7 refuses,
  and it may become its own producer under a later ADR.
- **A separate cluster for the protected lane.** Not required by default because
  of its cost and operating burden. It remains the fallback that
  `PROTECTED-HOOK-LANE-8` names when ordinary identities keep node-capable rights.
- **Qualify from configuration and provider documentation.** Rejected. A policy
  object or an enabled flag does not prove enforcement. A non-enforcing CNI once
  passed every allow-direction check in this repository's own probes, which is
  why `scripts/check-netpol-enforcement.sh` asserts denial first.
- **Qualify a disposable clone cluster with the same CNI and reuse its
  evidence.** Rejected. ADR 0224 and ADR 0236 forbid one installation qualifying
  another, and engines can differ with node image and add-on configuration.
- **Let the broker's session termination stand in for retained-connection
  refusal.** Rejected. It protects only the broker. A flow to the mediator or
  any other peer would remain unmeasured.
- **Give the measuring identity cluster-admin.** Rejected. Production
  administrators should not grant it. It would also let the measured authority
  change the boundary it measures.
