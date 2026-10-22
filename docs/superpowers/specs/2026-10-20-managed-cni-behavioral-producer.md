# Managed CNI behavioral network producer

Realizing specification for
[ADR 0238](../../adr/0238-a-managed-cni-installation-qualifies-through-its-own-behavioral-network-producer.md).
Tracked in [#4557](https://github.com/curie-eng/curie/issues/4557). It applies
[ADR 0236](../../adr/0236-the-commissioning-operator-splits-provisioner-and-control-processes.md)
and the per-installation campaign rule of
[ADR 0224](../../adr/0224-protected-runtime-commissioning-precedes-installation-acceptance.md)
without changing them, and it shares clauses with the
[protected runtime commissioning](2026-10-19-protected-runtime-commissioning.md)
specification (`PROTECTED-COMMISSIONING-1` to `9`). Where both documents speak
to the same object, the commissioning specification's clause is cited and not
restated.

This specification changes no code, no record format and no ADR. It claims no
qualification, provisioning or activation evidence, and no managed cluster has
been measured under it. ADR 0238 states that no producer code may start before
this specification exists. Its acceptance, not its merge, is what lets the
producer slices begin.

## Scope

In scope: one producer kind, the managed behavioral producer, for a Kubernetes
cluster whose pod network and policy enforcement come from a managed cloud CNI
and its network-policy agent. This covers:

- the CNI identity fact and how it binds into the ADR 0236 reply digests
  (clause 1);
- the probe kit, listeners and verdict rules shared by every case (clause 2);
- IP family coverage (clause 3);
- cases 1 to 8 of ADR 0238 with their pass, fail and unavailable rules
  (clause 4);
- the measuring identity's exact RBAC as a manifest listing (clause 5);
- the administrator-run installation bootstrap (clause 6);
- the `curie cluster` command surface (clause 7);
- cleanup, and revocation of the release-namespace probe binding (clause 8);
- reobservation at Readiness issuance and on every guard call (clause 9).

Each item that ADR 0238, ADR 0236 or ADR 0224 does not authorize is listed
under [Out of scope](#out-of-scope). A point where this specification reads
an ADR sentence in a way review should confirm is listed under
[Review points](#review-points). Each review point fails closed: if review
rejects the reading, the affected observation is `unavailable` and the
campaign refuses. No review point widens a right or turns a refusal into a
pass.

The producer slices land in this order. A slice cannot claim that a later
slice passed, and none enables production.

1. Probe kit, non-vacuity, source identity, egress denial and policy authority
   (clauses 2 to 4, cases 1, 2, 6 and 8).
2. Spoofing, first-packet and address reuse, retained connections and
   node-local traffic (cases 3, 4, 5 and 7).
3. CNI identity, invalidation and the measuring identity's least-privilege
   self-check (clauses 1, 5 and 9).
4. The `curie cluster` production form, the installation bootstrap and
   cleanup (clauses 6 to 8).

## Terms

- **Installation**: one protected installation in the target cluster, created
  for one final tuple as ADR 0224 and `PROTECTED-COMMISSIONING-2` define.
- **Protected namespace**: the namespace holding the installation's broker,
  mediator, protected worker and runner pods. The control namespace of ADR
  0236 is included when it is separate.
- **Probe namespaces**: namespaces created by the installation bootstrap only
  for probes. At least two exist: `probe-a` hosts probe senders and receivers,
  and `probe-foreign` is the owned foreign namespace of case 2. Both carry the
  installation's ownership labels. Neither holds a protected credential.
- **Release namespace**: the namespace of the ordinary Curie release in the
  same cluster. It is a hostile peer, never a participant.
- **Eligible nodes**: the nodes on which the protected pod templates measured
  by the guard configuration G (`PROTECTED-COMMISSIONING-2`) can be scheduled.
  The set is computed from Node labels and taints against the templates' node
  selectors, affinity and tolerations.
- **Ordinary identity**: any Kubernetes subject except (a) the protected
  installation's own identities measured by G, (b) the measuring identity and
  (c) the administrative and provider system set the bootstrap records
  (clause 6). Compromise of set (c) is outside ADR 0238.
- **Target**: an endpoint a case protects. The targets are the broker and
  mediator Services of the protected namespace, their twins (clause 2), and the
  declared egress peers of the runner policy.

## The CNI identity fact

<!-- @spec MANAGED-CNI-1 -->
The producer's output is `ManagedCniFact`, an internal evidence value. It is
not a closed record. No broker key stores it, no production reader parses it,
and no record grammar of `PROTECTED-COMMISSIONING-1` or `-7` gains a member
for it. It exists in provisioner memory, is hashed into the reply digests the
ADR 0236 channel already carries, and is written into the campaign evidence
whose digest the `commissioning_acceptance` record names
(`PROTECTED-COMMISSIONING-7`).

The fact uses the LANE-2 canonical encoding for hashing: canonical ASCII JSON,
sorted keys, no floats, duplicate member refusal. Its members are:

| Member | Content |
| --- | --- |
| `fact_kind` | Literal `managed_cni_fact`. |
| `fact_version` | Literal `1`. |
| `producer_kind` | Literal `managed-behavioral-v1`. A change to any producer constant of clause 2 is a new producer kind. |
| `producer_constants_digest` | sha256 of the canonical constants table of clause 2. |
| `runtime_id`, `runtime_generation` | The installation's final tuple (`PROTECTED-COMMISSIONING-1`). |
| `guard_config_digest` | `sha256(G)`. |
| `identity` | The identity object below. |
| `identity_digest` | sha256 of the canonical `identity` object. |
| `observations` | Sorted array of observation entries, one per case, IP family, placement and source kind. |

The `identity` object holds exactly what ADR 0238 lists under "What
invalidates the qualification":

| Member | Source |
| --- | --- |
| `cni_plugin_images` | Sorted `{workload, container, image_digest}` for every container of the CNI plugin workloads. A tag without a resolved digest is `unavailable`. |
| `policy_agent_images` | The same, for the network-policy agent workloads. |
| `enforcement_mode` | The agent's enforcement mode, read from the agent workload's arguments and environment and, where the provider exposes it only there, from the cloud describe call. |
| `addon_configuration_digest` | sha256 of the canonical managed add-on configuration as the provider reports it, with secrets and provider request IDs removed. |
| `dataplane_mode` | The kube-proxy or replacement dataplane mode, read from its workload configuration. |
| `control_plane_version` | The API server's `/version` `gitVersion` and `platform`. |
| `ip_families` | Sorted served IP families (clause 3). |
| `eligible_nodes` | Sorted `{node_name, node_uid, node_image, kernel_version, kubelet_version, container_runtime_version}` for each eligible node. `node_image` is the provider-reported image ID where the cloud describe call gives one, otherwise the Node's `osImage`. |

A Node's UID is part of its identity, so a replaced node with the same name is
a new, unmeasured node. Boot ID is not part of the identity: ADR 0238 does not
list it, and kernel or image changes are already covered.

Each observation entry has exactly `case` (`1` to `8`), `sub_case` (a literal
from clause 4), `family` (`ipv4`, `ipv6` or `none` for cases that are not
per-address), `placement` (`same_node`, `cross_node` or `none`),
`source_kind` (a literal from clause 4), `outcome` (`pass`, `fail` or
`unavailable`), `reason` (a literal reason code) and `evidence_digest` (sha256
of the probe's recorded evidence, clause 2).

The fact binds into the ADR 0236 request and reply channel as four digests,
each the sha256 of the canonical array of the listed entries:

| Channel digest (ADR 0236) | Content |
| --- | --- |
| network | Entries for cases 1, 6, 7 and 8. |
| hostile | Entries for cases 2, 3 and 4. |
| retained-peer | Entries for case 5. |
| CNI | `producer_kind`, `producer_constants_digest` and `identity_digest`. |

The verifier accepts only when every required entry is present with outcome
`pass` (clause 4 names the required entries). Any `fail` or `unavailable`
entry, a missing entry, or a duplicate entry refuses. The outcome literals
exist so that evidence explains a refusal; they never make one positive.

The fact's `producer_kind` is what lets ADR 0236's per-CNI selection pick this
producer. The provisioner selects it only when the observed CNI plugin and
agent workloads match a provider entry in the producer's own provider table,
which ships with the producer slice and names, per provider, the workload
names and namespaces to read. An unknown CNI has no producer and refuses
before the campaign starts, as ADR 0236 states. The local Cilium producer
keeps its own realization and is never selected for a managed CNI.

## Probe kit and verdict rules

<!-- @spec MANAGED-CNI-2 -->
All measurement is behavioral. A configuration read builds only the identity
of clause 1. It never decides an outcome.

**Probe pods.** Every probe pod is created from one of the probe templates the
bootstrap installs (clause 6). Each template has no Secret volume, no projected
service account token (`automountServiceAccountToken: false`), no host
namespace, no host path, no privileged container, read-only root filesystem,
and a pinned probe image by digest. The only capability any template may add
is `NET_RAW`, and only the spoofing template in `probe-a` adds it. The
measuring identity chooses only the node (by `nodeName`), the labels the
template allows, the probe arguments, and, for the kubelet-probe template of
case 7, the probe's target host and port.

**Owned listeners.** A listener is a probe pod that accepts TCP and UDP on the
target's port numbers. It records, per event, the peer address and port, the
protocol, the receive time and the probe nonce carried in the payload. It also
samples its own network namespace's socket table (`/proc/net/tcp`,
`/proc/net/tcp6`, `/proc/net/udp`, `/proc/net/udp6`) and listen-overflow
counter. Reading these needs no capability. A socket entry in `SYN_RECV` or
`ESTABLISHED` whose peer equals a probe's source address counts as delivery
even when no payload arrived.

**Twins.** A probe cannot record at the real broker or mediator, which run
protected code. For each of the broker and the mediator the bootstrap
installs a twin listener: a probe pod in the protected namespace, created from
the guard-admitted twin template, that carries the same policy role label the
protected NetworkPolicies select for that target and nothing else of the
target's identity. G's network policy recipes select targets by that role label
alone, so a twin is selected by exactly the same policy objects as its target.
Likewise an allowed-sender twin carries the role label the target's ingress
rule admits. Twins hold no credential, so the guard admits them only from the
twin templates and only for the measuring identity. Every denial case probes
both the real target (client-side observation) and its twin (listener-side
observation), and either side's delivery is a delivery.

**Verdict of one denial probe.** A probe sends a fixed number of attempts, each
carrying a fresh 128-bit nonce, and then waits for the listener window.

- `pass` when the listener recorded no event and no socket entry for any of the
  probe's nonces or source addresses, the client observed no completed TCP
  handshake and no reply, and the paired allowed control probe (below)
  succeeded.
- `fail` when the listener recorded any delivery, or the client completed a
  handshake or received a reply.
- `unavailable` when the paired allowed control probe failed, the listener's
  overflow counter moved during the window, the listener or probe pod did not
  reach the expected phase in time, the probe pod received no address in the
  case's family, or the pod landed on a node other than the one requested.

**Allowed control probe.** Every denial verdict is paired with a probe from a
pod the policy does allow, to the same listener, on the same node placement,
run immediately after the denial window. It shows the listener was reachable,
so a denial is not explained by a dead listener. ADR 0238 requires the denied
direction to be asserted before any allowed direction is trusted. The denial
runs first. The allowed control is never evidence of enforcement on its own.

**Evidence.** Per probe the producer records the pod UIDs, nodes and addresses
of sender and listener, the nonces, the client result, the listener events and
socket samples, the window bounds and the reason code. Evidence holds no
credential value and no payload other than nonces.

**Producer constants.** These are fixed in the producer kind. Changing one is a
new producer kind and a new `producer_constants_digest`.

| Constant | Value |
| --- | --- |
| Attempts per denial probe | 5 TCP connects and 5 UDP datagrams, 200 ms apart |
| Client connect timeout | 2 s |
| Listener window after the last attempt | 5 s |
| Pod phase timeout (Running, or container started) | 120 s |
| First-packet send interval from container start | 50 ms, for 10 s |
| First-packet repetitions per placement and family | 10 pod creations |
| Address-reuse bound | 40 pod creations or 15 minutes per placement and family, whichever comes first |
| Retained-connection heartbeat interval | 100 ms |
| Retained-connection bound after the triggering event | 10 s |
| Per-call probe set deadline (clause 9) | 4 s |

## IP family coverage

<!-- @spec MANAGED-CNI-3 -->
The served families are the union of:

- the families of the protected pods' `status.podIPs`;
- the families of the protected Services' `spec.ipFamilies`;
- the families of every eligible node's `InternalIP` addresses.

Every per-address case runs in each served family. A case runs in a family
only when both its sender and its listener hold an address in that family; if
either does not, that entry is `unavailable`. A single-family cluster records
one family, and the producer also records that no protected pod or eligible
node holds an address in the other family. If one does, that family is served.

Egress (case 6) probes each destination in every family the destination
resolves to or is defined in. Instance metadata is probed at the provider's
documented IPv4 and IPv6 metadata addresses whenever the node holds an address
in that family.

## Required cases

<!-- @spec MANAGED-CNI-4 -->
Each case lists its required entries. Unless a case states otherwise, each
entry is a denial probe judged by clause 2, run in every served family and
in both placements: `same_node` places sender and listener on one eligible
node, and `cross_node` on two different eligible nodes. A cluster with one
eligible node cannot produce `cross_node` entries, so they are `unavailable`
and the campaign refuses.

Case 1 runs first. If case 1 does not pass in every family and placement, the
producer runs no other case and the campaign refuses with reason
`enforcement_absent` or `non_vacuity_unavailable`.

### Case 1: non-vacuity

A listener in the protected namespace, selected by a policy that admits no
peer for its port, is probed from a pod in `probe-foreign` and from an
unselected pod in the protected namespace. Source kinds: `foreign_namespace`,
`protected_unselected`. `pass` only by clause 2. Delivery is `fail` with
reason `enforcement_absent`. This case exists because a non-enforcing CNI
passes every allow-direction check, which is why
`scripts/check-netpol-enforcement.sh` asserts denial first.

### Case 2: source identity

The broker, the mediator and their twins are probed from three source kinds:
`protected_unselected` (a probe pod in the protected namespace without the
allowed-sender role label), `foreign_namespace` (`probe-foreign`) and
`release_namespace` (a probe pod in the release namespace, created through the
campaign's release probe binding of clause 8).

Sub-case `label_forgery` is not per-address. It has two required entries:

- `guard_refusal`: a server-side dry-run create, by the measuring identity, of
  a pod in the protected namespace that carries the broker's and mediator's
  selected labels but is not one of the guard-admitted templates. `pass` when
  the guard refuses it. Admission by the guard, or refusal by RBAC before the
  guard decides, is `fail`, because then the guard decision was not observed.
- `rbac_refusal`: a SubjectAccessReview per ordinary identity for `create`,
  `update` and `patch` on `pods` in the protected namespace, and
  for `create` on every pod-synthesizing controller resource (Deployments,
  ReplicaSets, StatefulSets, DaemonSets, Jobs, CronJobs,
  ReplicationControllers) and on `pods/ephemeralcontainers` there. `pass` when
  every review is denied.

### Case 3: address spoofing

A sender in `probe-a`, created from the spoofing template with `NET_RAW`,
sends TCP SYN segments and UDP datagrams carrying nonces to the broker twin
and the mediator twin on their allowed ports. Two sub-cases set the forged
source address:

- `spoof_live`: the address of a currently running allowed-sender twin;
- `spoof_terminated`: the address of an allowed-sender twin that the
  producer terminated and confirmed absent less than 30 s earlier.

Both run in both placements and every family. `pass` when the twin recorded no
datagram from the forged address and no socket entry with the forged address
as peer. The `spoof_terminated` sub-case is `unavailable`, not `pass`, if the
forged address was reassigned to any pod before the probe, because the probe
then no longer measures a recently terminated address.

### Case 4: first-packet denial

Sub-case `new_pod`: the producer creates a probe pod without any allowed role
label, in the protected namespace and in `probe-foreign`, whose container
sends to the broker twin from its first instant at the first-packet interval.
The creation repeats the configured number of times per placement and family.
`pass` only if no repetition delivered any packet. A delivery from any
repetition is `fail` with reason `admitted_before_propagation`.

Sub-case `address_reuse`: the producer terminates an allowed-sender twin,
confirms its absence, and then creates unselected probe pods on the same node
until one receives the terminated twin's address or the address-reuse bound is
reached. The reusing pod sends from its first instant as in `new_pod`. `pass`
when reuse was provoked and nothing was delivered. `fail` on delivery.
`unavailable` with reason `reuse_not_provoked` when the bound was reached
without reuse. This entry is required, so an unprovoked reuse refuses the
campaign. That is ADR 0238's explicit outcome.

### Case 5: retained-connection refusal

Both sub-cases use a TCP connection with a heartbeat of sequence-numbered
nonces, established while allowed, from an allowed-sender to a listener.

- `workload_replaced`: the sender is the allowed-sender twin in the protected
  namespace and the listener is the broker twin. The producer terminates the
  sender and confirms its absence. It then provokes reuse of the sender's
  address as in case 4. The reusing pod is created in `probe-a` from the
  spoofing template, and it sends segments in the original connection's
  four-tuple with the last recorded sequence state, plus a fresh SYN from the
  original source port. `pass` when the listener receives no data, no new
  socket entry and no heartbeat after the termination plus the retained bound.
  `unavailable` with reason `reuse_not_provoked` when reuse was not provoked.
- `policy_changed`: the measuring identity can change NetworkPolicies only in
  probe namespaces (clause 5). The sender and listener are probe pods in
  `probe-a` under a probe policy built from the same selector shapes, port
  forms and protocol as the protected ingress policy. After the connection is
  established, the producer changes the probe policy to remove the allowance.
  `pass` when no heartbeat with a sequence number sent later than the change
  plus the retained bound reaches the listener, and the established socket
  passes no further data in either direction.

Both sub-cases run in both placements and every family. If the engine keeps
connection-tracking state and the established flow keeps passing data, the
entry is `fail` with reason `retained_flow`. Broker session termination under
ADR 0191 stays required by the commissioning campaign and is never recorded
as this case.

### Case 6: egress denial

A probe pod in the protected namespace, created from the runner-policy
template, carries the runner's policy role label and so receives the runner
egress policy. Each destination below is a sub-case, with source kind
`runner_policy`:

- `internet`: two fixed public test addresses per family, TCP 443 and 80;
- `instance_metadata`: the provider's metadata addresses;
- `kubernetes_api`: the API server's Service address and every endpoint
  address it publishes;
- `node_addresses`: every eligible node's addresses, on the kubelet port and
  on a closed high port;
- `cloud_network`: one address per pod and node subnet the provider reports
  that is outside the declared peers;
- `external_dns`: two fixed public resolvers per family, UDP and TCP 53.

These destinations have no owned listener, so the outcome is judged at the
client: `pass` when no handshake completed and no reply arrived within the
window, and the runner-policy pod's allowed control (a declared egress peer)
succeeded. Sub-case `declared_peers` additionally probes each declared peer and
is `pass` when each is reachable. Placement is `none` for external
destinations and both placements for node addresses.

### Case 7: node-local traffic

Sub-case `node_origin`: a pod in `probe-a`, created from the kubelet-probe
template on each eligible node, carries a readiness probe whose target host is
a protected twin's address. Kubelet then connects to the twin from the node
itself. The twin's record of a connection from a node address is the
observation. The template cannot request host network, so this is the only
node-originated source the measuring identity can produce. This entry records a
fact rather than a denial: it is `pass` with reason `reaches` or `blocked` when
the observation was made, and `unavailable` when it could not be made. Most
CNIs admit node-to-local-pod traffic for kubelet health checks, so `reaches` is
the expected result on the same node.

When any eligible node records `reaches`, three further entries are required:

- `rbac_node_capability`: SubjectAccessReviews per ordinary identity for
  `create` on `pods`, on each pod-synthesizing controller resource of case 2
  and on `pods/ephemeralcontainers` in every namespace, and for `get` and
  `create` on `nodes/proxy`. `pass` when `nodes/proxy` is denied for every
  ordinary identity. Allowed pod-creation rights are recorded; the next entry
  must show that admission refuses node-capable pods from any of them.
- `admission_node_capability`: the bootstrap's eligible-node fence (clause 6)
  is observed by server-side dry-run creates by the measuring identity in
  `probe-a` of a pod that targets an eligible node and requests, one at a
  time, host network, host PID, host IPC, a host path volume, a privileged
  container, and the eligible-node taint toleration. `pass` when the guard
  refuses each one.
- `running_node_capability`: the pods currently bound to eligible nodes are
  only protected-installation pods, probe pods and pods of the recorded system
  set. Any other pod with host network, host PID, host IPC, a host path or a
  privileged container on an eligible node is `fail`.

If any of these is not `pass`, the campaign refuses with reason
`separate_authority_domain_required`, and the operator must use a separate
node or cluster authority domain as `PROTECTED-HOOK-LANE-8` requires.

### Case 8: policy authority

Sub-case `rbac_policy`: SubjectAccessReviews per ordinary identity for
`create`, `update`, `patch`, `delete` and `deletecollection` on
`networkpolicies` in the protected namespace, on every provider-specific policy
resource the provider table names, on the policy agent's configuration objects,
and on `update` and `patch` of the protected and probe Namespace objects
(labels select namespaces). `pass` when every review is denied.

Sub-case `all_policies_present`: the producer records the set of
NetworkPolicy and provider policy objects that select the protected namespace
at the start and end of each case, by name, UID and resource version, using
the reads of clause 5. `pass` when the set did not change during the campaign
except for the producer's own probe-namespace policies. This entry proves that
every probe above ran with all existing policies present.

## The measuring identity's RBAC

<!-- @spec MANAGED-CNI-5 -->
The measuring identity is one Kubernetes ServiceAccount created by the
installation bootstrap in the control namespace. The out-of-cluster
provisioner uses a short-lived token for it. Its rights are exactly the
manifest below, with `<protected>`, `<control>`, `<probe-a>`, `<probe-foreign>`
and `<release>` substituted by the bootstrap. Every rule is a Role in one
namespace except the three marked ClusterRole. No rule uses a wildcard verb,
resource or API group.

```yaml
# Role in <probe-a> and, identically, in <probe-foreign>
- apiGroups: [""]
  resources: [pods]
  verbs: [create, get, list, watch, patch, delete]   # patch is relabel
- apiGroups: [""]
  resources: [pods/log, pods/status]
  verbs: [get]
- apiGroups: [networking.k8s.io]
  resources: [networkpolicies]
  verbs: [create, get, list, watch, update, patch, delete]

# Role in <protected>
- apiGroups: [""]
  resources: [pods]
  verbs: [create, get, list, watch, delete]   # guard admits only probe and twin templates
- apiGroups: [""]
  resources: [pods/log, pods/status]
  verbs: [get]

# Role in <release>, bound only for one campaign (clause 8)
- apiGroups: [""]
  resources: [pods]
  verbs: [create, get, list, watch, delete]   # guard admits only the release probe template
- apiGroups: [""]
  resources: [pods/log]
  verbs: [get]
- apiGroups: [rbac.authorization.k8s.io]
  resources: [rolebindings]
  resourceNames: [curie-release-probe-<campaign_id>]
  verbs: [delete]   # self-revocation at cleanup

# Role in <control>: the ADR 0236 request and reply channel
- apiGroups: [<channel API group>]
  resources: [<request kind>]
  verbs: [get, list, watch]
- apiGroups: [<channel API group>]
  resources: [<reply kind>]
  verbs: [create, get]

# ClusterRole: reads for identity and effective RBAC
- apiGroups: [""]
  resources: [nodes, namespaces]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]   # case 7 running_node_capability, case 8 and CNI workloads
- apiGroups: [apps]
  resources: [daemonsets, deployments]
  verbs: [get, list, watch]   # CNI, policy agent and dataplane workloads
- apiGroups: [networking.k8s.io]
  resources: [networkpolicies]
  verbs: [get, list, watch]
- apiGroups: [<provider policy groups from the provider table>]
  resources: [<provider policy resources>]
  verbs: [get, list, watch]
- apiGroups: [admissionregistration.k8s.io]
  resources: [validatingwebhookconfigurations, mutatingwebhookconfigurations,
              validatingadmissionpolicies, validatingadmissionpolicybindings]
  verbs: [get, list, watch]
- apiGroups: [rbac.authorization.k8s.io]
  resources: [roles, rolebindings, clusterroles, clusterrolebindings]
  verbs: [get, list, watch]   # enumerates the subjects SubjectAccessReviews test
- nonResourceURLs: [/version]
  verbs: [get]

# ClusterRole: effective RBAC measurement
- apiGroups: [authorization.k8s.io]
  resources: [subjectaccessreviews]
  verbs: [create]
```

The ConfigMaps the CNI and agent workloads mount are read only by the names the
provider table lists, through an additional Role in the CNI namespace with
`resourceNames` and verb `get`.

The identity has no right to create, change or delete NetworkPolicies outside
the probe namespaces; to create any RBAC object; to impersonate; to read
Secrets anywhere; to exec, attach or port-forward; to use `nodes/proxy`; or to
create pods with host network, host path, privileged containers or added
capabilities other than `NET_RAW` in `probe-a`.

A read-only cloud principal scoped to describe this cluster, its node groups
and its add-ons is permitted only when the provider exposes the add-on
configuration or node image ID nowhere else. It is used for clause 1 identity
reads and grants no measurement authority.

**Least-privilege self-check.** Before the campaign and on every guard call
(clause 9), the producer runs SubjectAccessReviews for the measuring identity
over a fixed probe list: every verb on Secrets, `nodes/proxy`, `pods/exec`,
`pods/attach`, `pods/portforward`, `pods/ephemeralcontainers`, RBAC writes,
`impersonate` on users, groups and service accounts, NetworkPolicy writes in
the protected and release namespaces, and pod creation in every namespace not
listed above. It also checks the identity's RoleBindings and
ClusterRoleBindings by subject and requires that their rules equal this
manifest. Any allowed review outside the manifest, or any extra binding, refuses
the campaign. Clause 9 explains why the release Role is allowed only while a
campaign holds its binding.

## Installation bootstrap

<!-- @spec MANAGED-CNI-6 -->
A cluster administrator runs the installation bootstrap once per installation
with their own credentials. The provisioner renders it from the pinned profile,
projection and images, so the provisioner stays the only author of the
protected lane (commissioning design decision 1); the administrator only
applies it. The rendered set is content-addressed. Its digest is recorded in
the bootstrap result and covered by G.

It creates:

1. The protected, control and probe namespaces, with the installation's
   ownership labels. The probe namespaces carry the Pod Security `privileged`
   level, which is the only way to permit `NET_RAW`. The guard (item 2) then
   restricts them to the probe templates.
2. The admission guard. Its configuration includes, in addition to the
   protected-namespace rules of `PROTECTED-HOOK-LANE-8`:
   - probe-namespace rules admitting only the probe templates of clause 2, and
     refusing host network, host PID, host IPC, host path, privileged
     containers and every added capability other than `NET_RAW`;
   - protected-namespace rules admitting probe and twin pods only from the
     measuring identity and only from the twin and runner-policy templates;
   - a release-namespace rule that admits pods from the measuring identity
     only from the release probe template;
   - the eligible-node fence: a `NoSchedule` eligible-node taint applied by
     this step, and a cluster-wide rule refusing any pod that tolerates that
     taint, or names an eligible node, unless it is a protected-installation
     pod, a probe pod, or a pod of the recorded system set. Pods already
     running on eligible nodes are not evicted; case 7
     `running_node_capability` observes them.
3. The protected lane's RBAC and NetworkPolicies, with policies that select
   targets by role label as clause 2 requires.
4. The measuring identity's ServiceAccount, the Roles and ClusterRoles of
   clause 5 and their bindings, except the release-namespace binding.
5. The protected installation workloads, broker and credential Secrets that
   `PROTECTED-COMMISSIONING-2` and `-6` place before the campaign. The
   measuring identity cannot create them.

It also records the system set: the subjects the administrator declares as
administrative or provider-owned (for example `system:masters`, the control
plane components, the provider's node and add-on identities and the
`kube-system` service accounts). The record is part of the bootstrap result
and of the campaign evidence, so a reviewer sees every exclusion from
"ordinary identity". An empty or wildcard system set refuses.

The bootstrap never grants the measuring identity cluster-admin, never edits
the ordinary release's objects, and never changes the CNI, the agent or node
configuration other than the eligible-node taint. It refuses unless the
cluster context was passed explicitly.

## Command surface

<!-- @spec MANAGED-CNI-7 -->
The production form is a `curie cluster protected` command group. Every
command takes an explicit `--context` and refuses without it. None reads the
kubeconfig current context, and none accepts a caller-supplied observation,
success record, outcome or phase flag (ADR 0236).

| Command | Run by | Effect |
| --- | --- | --- |
| `curie cluster protected install` | Administrator | Renders and applies the bootstrap of clause 6; `--render-only` writes the rendered set and its digest to a directory for review and applies nothing. |
| `curie cluster protected grant-release-probe` | Administrator | Creates the campaign's release probe RoleBinding (clause 8) for one named campaign ID. |
| `curie cluster protected qualify` | Provisioner, as the measuring identity | Selects the producer, runs clause 4 in order, then the commissioning campaign of `PROTECTED-COMMISSIONING-5`, then cleanup. Exits nonzero on every refusal and names the first refusing entry. |
| `curie cluster protected status` | Anyone with read access | Reports the installation's identity digest, the last campaign's outcome and whether cleanup and revocation were observed. Read-only. |
| `curie cluster protected cleanup` | Provisioner | Resumes cleanup from recorded ownership after an interrupted campaign. |
| `curie cluster protected revoke-release-probe` | Administrator | Deletes a release probe binding the provisioner could not revoke. |

The local `curie dev` form of ADR 0236 is unchanged. `qualify` runs the
provisioner outside the cluster. It holds the provisioner signing key and the
measuring identity token, and no other credential.

## Cleanup and release probe revocation

<!-- @spec MANAGED-CNI-8 -->
**Release probe binding.** ADR 0238 lets the measuring identity create probe
pods in the release namespace "for the campaign only, through a binding that is
revoked at cleanup". The measuring identity holds no right to create RBAC
objects. So the binding is created per campaign by
`grant-release-probe`, named `curie-release-probe-<campaign_id>` and annotated
with the campaign ID and installation ID. The measuring identity can delete
only that binding, by name. `qualify` refuses to start when:

- no binding exists for its campaign ID;
- any binding to the release Role exists for another campaign ID;
- the binding's subject is not the measuring identity.

**Cleanup order.** Cleanup registers before any probe pod is created, as
`PROTECTED-COMMISSIONING-8` requires for primitive probes, and follows the
same ownership rules in the provisioner's separate probe registry. Every
probe pod and probe policy is recorded as `creating` before it is created and
bound to its UID afterwards. Cleanup runs in this order:

1. Delete every recorded probe pod in the release namespace and confirm
   absence by UID.
2. Delete the release probe binding, then confirm by SubjectAccessReview that
   the measuring identity can no longer create pods in the release namespace.
3. Delete every recorded probe pod and twin in the protected namespace that is
   not part of the measured controls, and confirm absence by UID.
4. Delete every recorded probe pod and probe policy in the probe namespaces
   and confirm absence.
5. Hand over to the commissioning cleanup of ADR 0224 and
   `PROTECTED-COMMISSIONING-6` for campaign targets and candidate authority.

Cleanup never deletes by prefix or label selector alone, and never adopts a
same-name object it did not record. The measured protected controls (the
namespaces, guard, RBAC, NetworkPolicies, measuring identity and its
non-release bindings, and the installation workloads) are preserved for
activation under ADR 0224. The probe namespaces stay, empty, because Readiness
re-issuance (clause 9) probes from them again.

Revocation is observed only by the step 2 SubjectAccessReview. Uncertain
ownership, an unconfirmed absence or a still-allowed review blocks acceptance
(ADR 0236: "revocation has been observed") and is reported by `status`. If the
provisioner cannot delete the binding, `revoke-release-probe` deletes it, and
`cleanup` then reruns the step 2 observation.

## Reobservation

<!-- @spec MANAGED-CNI-9 -->
ADR 0238 requires the provisioner to re-observe the producer's facts on every
guard call, and the CNI identity at each Readiness issuance. Installation
observations are never reused (`PROTECTED-COMMISSIONING-9`).

**On every guard call.** Before it signs a reply, the provisioner:

1. recomputes the `identity` object from live reads and compares its digest
   with the campaign's `identity_digest`;
2. recomputes the eligible node set and refuses on any added, removed or
   replaced node;
3. runs the per-call probe set: case 1 in every family, plus the case 2
   `foreign_namespace` probe against the broker twin in both placements, from
   probe pods started at campaign start and kept running for the campaign;
4. reruns the least-privilege self-check of clause 5;
5. confirms that the policy object set of case 8 `all_policies_present` is
   unchanged.

Every step must complete within the per-call deadline. A mismatch, a
`fail`, an `unavailable` entry or a missed deadline refuses that guard call.
The heavy cases 3, 4, 5, 6 and 7 and the remainder of case 2 are measured in
the campaign's premeasurement and enter the reply digests by their observation
entries. Step 1 and the per-call probes show on each call that the engine they
measured is unchanged and still enforcing. This reading is review point 1.

**At Readiness issuance.** Before the verifier's Readiness publisher writes,
the provisioner runs steps 1, 2, 4 and 5, and step 3 from probe pods it creates
for that issuance and then removes under clause 8's ownership rules. The
release namespace is not probed at issuance, and no release binding is
granted for it. A mismatch refuses the issuance. The installation then needs a
new campaign before Readiness can be issued again, as ADR 0238 states. The
bounded Readiness lifetime is the longest exposure to an unobserved change.

**Invalidation.** A change to any identity member, a new or replaced eligible
node, a change in served IP families, a new producer kind or a change of
`sha256(G)` invalidates the campaign. Operators must hold managed automatic
upgrades of the CNI, the policy agent and eligible node images while the lane
is active, or accept lane closure until a new campaign passes.

## Verification boundary

Unit tests prove only the fact encoding, digest binding and verdict rules.
A multi-node local cluster with an enforcing CNI proves the probe kit and the
refusal paths. A non-enforcing CNI must be shown to make case 1 refuse. Cases a
local cluster cannot provoke are recorded as `unavailable`, never as `pass`.
Only a campaign on a real, disposable, upstream-owned managed cluster can show
that this producer qualifies that provider's CNI. A failing case 4 or 5 is a
valid, explicit outcome. No slice claims qualification of a provider's CNI
from local results, and no slice runs against a customer or shared cluster.

## Review points

Each point is a reading of an ADR sentence that review should confirm. If
review rejects one, the observation it supports is `unavailable` and the
campaign refuses until a revised decision exists.

1. **Per-call reobservation scope.** ADR 0238 says the provisioner re-observes
   the producer's facts on every guard call. Clause 9 re-observes the identity,
   non-vacuity and a source-identity subset on each call, and binds the heavy
   cases from the premeasurement. Rerunning cases 3 to 5 on each call takes
   minutes and cannot meet the guard latency ADR 0236 budgets. If review reads
   the ADR as requiring every case per call, this producer cannot meet it, and
   the specification returns to ADR review.
2. **Reads beyond the ADR's named list.** ADR 0238 lists reading Nodes, the
   CNI and policy-agent workloads and admission configuration, and creating
   SubjectAccessReviews. Clause 5 also grants read-only access to Namespaces,
   Pods cluster-wide, NetworkPolicies and provider policy objects, and RBAC
   objects. These serve case 7 `running_node_capability`, case 8
   `all_policies_present`, and the enumeration of ordinary identities that
   SubjectAccessReviews need. Reading Pods cluster-wide exposes Pod specs,
   including any inline environment values.
3. **Lifecycle of the measuring identity's own probe pods.** The ADR names
   "create" for the protected and release namespaces. Clause 5 adds `get`,
   `list`, `watch` and `delete` there, because the ADR also requires reading
   probe status and logs and removing probe workloads at cleanup. It also adds
   self-revocation of the release binding.
4. **The channel's object rights.** The provisioner runs under the measuring
   identity, and the ADR 0236 channel needs it to read request objects and
   create reply objects in the control namespace. Clause 5 lists those rights.
5. **Twins.** Clause 2 measures listener-side delivery at twins that share the
   target's policy role label. This relies on G's policy recipes selecting
   targets by role label alone. A recipe that also selects by any other label
   breaks the equivalence, and the twin entries are then `unavailable`.
6. **Policy-change retention in probe namespaces.** The measuring identity
   cannot change protected NetworkPolicies, so case 5 `policy_changed` runs
   under a probe policy with the protected policy's shapes on the same engine
   and nodes.
7. **Installation contents.** ADR 0238 lists namespaces, guard, RBAC,
   NetworkPolicies and measuring bindings as installation. Clause 6 also places
   the protected workloads, broker and credential Secrets in the installation
   step, because the measuring identity cannot create them.
8. **Per-campaign administrator grant.** Clause 8 makes the release probe
   binding an administrator action per campaign, so that the measuring identity
   never holds an RBAC write.
9. **Eligible-node fence.** Case 7 needs admission refusals of node-capable
   pods on eligible nodes. Clause 6 realizes them with an eligible-node taint
   and a cluster-wide guard rule. The ADR names the admission guard as part of
   installation but does not name a node taint or a cluster-wide rule.

## Out of scope

These are not authorized by ADR 0238, ADR 0236 or ADR 0224, or are named
there as excluded. No slice of this specification implements them.

- **How the out-of-cluster provisioner reaches the protected broker.** The
  commissioning specification gives the provisioner principal broker ACL
  administration (`PROTECTED-COMMISSIONING-6`, steps 2 and 5). No accepted ADR
  decides the network path from outside the cluster, and clause 5 grants no
  port-forward or exec. This blocks slice 4 and needs its own decision; it
  does not block slices 1 to 3.
- Post-activation guard reobservation outside a commissioning campaign, beyond
  Readiness issuance.
- Any substrate other than Kubernetes; serverless pod runtimes and Windows
  nodes, which refuse.
- Connector-bearing profiles; the profile stays `qualified_connectors=[]`.
- A dedicated node pool with cloud network controls, a cloud API producer, and
  a separate-cluster topology. These are the documented remedies when case 7
  refuses, and each needs its own ADR.
- Any second managed CNI. Each one needs its own provider table entry,
  realization and evidence. The first provider's evidence qualifies no other.
- Compromise of the cloud provider control plane, of cluster administrators,
  of the recorded system set or of a node.
- Data exfiltration through allowed peers or the cluster resolver.
- Enforcing, as opposed to requiring, the operator's hold on managed automatic
  upgrades.
- Activation, which remains the ADR 0224 transition of
  `PROTECTED-COMMISSIONING-6`.
- Any change to the closed record grammars of `PROTECTED-COMMISSIONING-1` and
  `-7`, or to the production Qualification and Readiness formats.
