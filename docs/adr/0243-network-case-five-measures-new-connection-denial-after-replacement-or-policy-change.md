# 243. Network case five measures new connection denial after replacement or policy change

Date: 2026-10-23

Status: Draft

Tracked under [#4557](https://github.com/curie-eng/curie/issues/4557).
The [realizing amendment](../superpowers/specs/2026-10-23-network-case-five-new-connections.md)
defines the measurement. This Draft authorizes no implementation or changed
qualification. Current producers retain their current refusal behavior.

Upon explicit maintainer acceptance, this decision partially supersedes only:

- case 5, **Retained-connection refusal**, of
  [ADR 0238](0238-a-managed-cni-installation-qualifies-through-its-own-behavioral-network-producer.md);
- the **retained-connection refusal after the original workload is replaced**
  requirement under "Network producers are selected per CNI" in
  [ADR 0236](0236-the-commissioning-operator-splits-provisioner-and-control-processes.md).

All other observations, per-installation qualification, authority separation,
revocation and cleanup obligations remain in force. In particular, this does not
change SDK/runner/broker session termination or the actual join witnesses of
[ADR 0241](0241-a-commissioning-run-separates-destructive-trials-from-final-acceptance.md)
and [ADR 0242](0242-commissioning-session-scope-makes-the-last-observer-finite.md).

## Context

The current case asks a network-policy engine to terminate established TCP
traffic after sender replacement and after removal of an allowance. Kubernetes
leaves the effect of policy changes on existing connections to the network
implementation; see the official
[NetworkPolicy existing-connection documentation](https://kubernetes.io/docs/concepts/services-networking/network-policies/#networkpolicys-impact-on-existing-connections).
This is a stronger requirement than the portable policy API promises.

The existing producer also cannot establish the replacement predicate by
replaying TCP segments without original sequence state. It correctly reports
`sequence_state_unavailable`, rather than counting a rejected stale segment as
a network-policy success. Neither observation is repaired by changing a report
flag or disconnecting an application client.

The choice is therefore a change to the qualification contract, not a fix that
makes the old established-flow guarantee pass. Existing-flow termination by the
CNI will no longer be promised under case 5. Application authority revocation
and genuine session termination remain mandatory independently.

## Decision

### Measure new connections against the changed authority

Case 5 requires fresh TCP connections to be denied after two observed triggers:

1. An allowed sender is terminated, its original UID is observed absent, and an
   unselected owned replacement actually receives that sender's address.
2. A probe-namespace policy removes an allowed sender's allowance. As in the
   existing specification, the policy reproduces the protected selector/port/
   protocol shapes on the actual engine; the measuring identity does not gain
   permission to edit protected policies.

Both subcases run for every served IP family, on the same node and across
nodes. Failure to provoke address reuse, unavailable cross-node placement,
unknown trigger timing, incomplete listener observations or failed controls is
`unavailable` and refuses qualification. A one-node layout does not silently
omit the cross-node entry.

The realizing amendment fixes a finite post-trigger probe schedule within a
120-second total deadline, including startup and observations. There is no
claim of a 10-second policy-convergence guarantee. Every attempted connection
uses a new 5-tuple;
none reuses the established connection's tuple or TCP sequence state. Completed
forbidden handshakes, replies and listener-side delivery during the required
denial window fail. Paired fresh allowed connections must also demonstrate
receiver liveness and a nonce-bound round trip at the owned listener. An absent
listener, one-way control or a client timeout alone cannot prove enforcement.
Every completed forbidden attempt in the measured post-trigger set fails; no
early successful attempt is discarded as propagation. Case 4's immediate
first-packet denial remains unchanged. Case 5's startup allowance supplies no
continuous observation of an earlier interval and is not a grace period for
case 4.

A surviving established flow is recorded separately when observed. It neither
passes nor fails the replacement case-5 predicate and is never described as
terminated. Existing broker revocation/termination and SDK/runner stop/join
proof must still succeed through their own actual producers before acceptance.

### New semantics have new measurement identity

The managed producer uses `managed-behavioral-v2`; the local Cilium composition
uses `local-cilium-v2`. Their constants and all affected evidence digests bind
the new semantics. Old v1 observations cannot be relabelled, mixed into a v2
result or interpreted through a new verdict function. Missing version binding
refuses. The existing `retained_peer` fact-channel name remains a compatibility
identifier; it is not a claim that established conntrack state was removed.
The twelve-fact signed channel gains no member or receipt authority.

Implementation is tracked in #4557. The realizing code paths are
`apps/worker/src/curie_worker/managed_cni_timing.py`,
`apps/worker/src/curie_worker/managed_cni_probe_kit.py`,
`apps/worker/src/curie_worker/managed_cni_probe_programs.py`,
`apps/worker/src/curie_worker/managed_cni_identity.py` and
`apps/worker/src/curie_worker/commissioning_local_network.py`.
They must remain unavailable for the new claim until this decision is Accepted
and the amendment's actual measurement and version checks are implemented.

## Consequences

A CNI can qualify without a promise to flush established connections on policy
change. This intentionally withdraws that prior network guarantee. A successful
case proves only the bounded fresh-connection predicate on the measured
installation; it is not universal packet-loss prevention or runtime acceptance.

The replacement measurement no longer needs privileged recovery of original TCP
sequence state. It still needs actual address reuse and complete placements, so
some installations can remain unavailable. The amendment adds only a closed, credential-free probe barrier and its owned
probe-namespace port policy to establish actual post-trigger causality. No
raw-socket, node, protected-policy-edit, credential, RBAC or production
authority is added by this decision.

## Rejected alternatives

- **Keep the existing predicate.** This preserves the stronger guarantee, but
  the current producer cannot prove replacement-flow refusal and policy engines
  may validly preserve established flows. It would keep qualification refused.
- **Flush conntrack or repair the measured system.** Administrative intervention
  changes the system under test and supplies no observation of its own policy
  enforcement. It also requires authority outside the existing probe contract.
- **Use broker disconnect as the network proof.** Mandatory application cleanup
  is valuable, but it says nothing about network denial of a fresh connection.
- **Treat fresh SYN or stale-sequence non-delivery as a pass.** Either can fail
  because TCP state is unsuitable or the receiver is dead. The new predicate
  requires genuine fresh connect attempts, listener corroboration and allowed
  controls, rather than reinterpreting existing raw injections.
