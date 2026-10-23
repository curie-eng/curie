# Case five: new connections after sender replacement or policy change

Realizing amendment for
[Draft ADR 0243](../../adr/0243-network-case-five-measures-new-connection-denial-after-replacement-or-policy-change.md),
tracked in [#4557](https://github.com/curie-eng/curie/issues/4557).
This document is a proposal. Neither its merge nor existing component tests
change the current producer or establish qualification. Implementation requires
explicit acceptance of that ADR first.

## Amendment boundary

<!-- @spec CNI-FRESH-FLOW-1 -->
On acceptance, this replaces only case 5 of
[MANAGED-CNI-4](2026-10-20-managed-cni-behavioral-producer.md) and its
retained-connection-specific constants. Common probe ownership, least privilege,
policy equivalence, denied-before-allowed controls, cleanup, CNI invalidation and
cases 1–4 and 6–8 stay unchanged. It applies to the actual managed producer and
the local Cilium composition that shares its behavioral cases. No proof from
one CNI, installation or producer version qualifies another.

The old established-flow refusal guarantee is withdrawn. The replacement is a
finite, observed fresh-connection denial predicate, not proof that conntrack
was flushed, that a policy converged within 10 seconds, or that all traffic
throughout the interval after a trigger was observed. SDK, runner and broker
revocation, actual joins, original-container termination and independent remote
absence remain separate mandatory commissioning predicates.

## Exact matrix and triggers

<!-- @spec CNI-FRESH-FLOW-2 -->
Keep the two case-5 subcase identifiers `workload_replaced` and `policy_changed`,
source kind `retained_peer`, every served IP family and both `same_node` and
`cross_node` placements. Bind actual sender/receiver UID, address and node.
Duplicate, missing or extra required entries refuse. One eligible node makes
each cross-node entry `unavailable`; it does not reduce the required matrix.

For `workload_replaced`:

1. Establish the old allowed connection and observe its actual endpoint tuple
   at the original allowed sender and broker twin; this is setup evidence only.
2. Terminate that sender and observe absence of its exact UID. Provoke an
   unselected owned replacement in the existing probe namespace that actually
   receives the same address, using the existing 40-creations/15-minute bound.
   Unknown, unprovoked or mismatched reuse is `unavailable`.
3. After both observations, trigger the address-reusing workload itself through
   the closed causal barrier below. It remains the actual fresh-connection
   sender. No later replacement may silently change its UID or address.
   Probe both the real broker and its policy-equivalent owned twin. Apply the
   existing twin equivalence check; an unmeasurable target refuses the entry.

For `policy_changed`, use only owned probe-namespace sender/listener workloads
and a probe policy with the protected ingress selector, port and protocol
shapes, as the existing specification requires. Observe the initial allowed
connection, remove the allowance, and freshly read the exact resulting policy
set and original endpoint UIDs. That confirmed read is the trigger. Never
change the protected policy or repair the measured final tuple. Use the same
original sender Pod, UID and address for the fresh connections, started by the
causal barrier below. Its labels remain the removed sender role; a separate
allowed control role retains its allowance.

A trigger time is the trusted producer's monotonic time when it has confirmed
all required trigger observations. It starts the total deadline below. The
barrier below establishes attempt causality; unrelated node wall clocks are
not compared to the host clock.

## Finite fresh probes and controls

<!-- @spec CNI-FRESH-FLOW-3 -->
The v2 constants are fixed before measurement:

| Constant | Value |
| --- | --- |
| Total deadline from confirmed trigger through all attempt, listener and control observations | 120 s |
| Fresh denial attempts per endpoint | 5 TCP connects |
| Connect timeout per attempt | 2 s |
| Delay between completed attempts | 200 ms |
| Listener observation after the last attempt | 5 s |

The 120-second deadline includes workload startup, polling, both target sides,
controls and transport delay. Completion after it is `unavailable`, even if a
later read reports all denials. It neither extends on retry nor grants positive
work after the enclosing campaign deadline. The existing per-call timeout and
aggregate elapsed/resource bounds remain additional ceilings.

Each attempt uses a new TCP 5-tuple, explicitly bound to the actual sender
address with a fresh source port. Exclude every earlier attempt tuple and the
old established tuple. Record the ports and outcomes; kernel-selected ephemeral
ports without observed distinctness do not suffice. Do not forge a source
address, replay an old segment, reuse established sequence state, set TCP_REPAIR
or add capability/RBAC rights. Failure to obtain the required distinct tuples
is `unavailable`.

All denied attempts must begin after the confirmed trigger. There is no
unrecorded propagation grace or selective retry: any completed forbidden
handshake or received reply in this set fails. Listener corroboration records
the matching fresh peer tuples, socket samples and challenge nonces. SYN_RECV/ESTABLISHED observations for those fresh tuples count as delivery even
without a payload. For this case only, correlate listener/socket evidence by
exact fresh tuple and challenge; matching the sender address alone is
insufficient because the explicitly excluded old setup connection can remain
established. Retain old-flow events separately rather than dropping them or
counting them as new-flow denial evidence.
Record every attempt, including success, refusal and timeout, in order. An
unspecified exception, connection reset or malformed result cannot be silently
counted as policy denial; corroborate the existing common denial criteria.

The listener must already be ready before denial attempts. It must cover the
whole attempt/window interval, preserve complete nonoverflowing observations,
and retain exact original identity. The producer then runs a paired, separately
allowed **fresh** control on the same placement/family and same listener. The
owned twin/probe listener echoes a fresh challenge; the control sender must
receive that matching response, and the listener must record receipt. This
measures both request and return directions. A one-way send or an unrelated
response cannot establish liveness. The actual broker side additionally needs
the existing allowed client-side connection control; no broker credential or
arbitrary broker command is introduced by the nonce echo, which is confined to
owned probe listeners. The denial is measured before this allowed control is
trusted. Old allowed setup traffic never replaces the post-denial control.

### Closed causal barrier within owned probes

Both subcases keep their measured fresh-connection sender alive until its
attempts finish: the address-reusing Pod for replacement, or the original
allowed Pod for policy change. A fixed, credential-free case-5 sender program opens its setup connection
when required, then waits at a one-use TCP barrier on port 42001. Targets,
excluded tuples, attempt count and deadlines are fixed in its measured launch
arguments. The barrier cannot choose a target, execute a command, change a
policy or open any other transport. It accepts one line exactly `START ` plus
32 lowercase hexadecimal characters and a newline, at most 39 bytes; all other
input refuses the case. A second trigger refuses rather than restarting work.
The program returns an acknowledgment bound to that challenge, then performs
exactly the fixed fresh attempts and emits challenge-bound complete results.

Only after confirming the trigger does the trusted producer generate that
fresh unpredictable 128-bit challenge. It then creates an owned one-shot trigger
Pod whose sole operation is sending that line to the original sender's barrier
and receiving the matching acknowledgment. The producer observes both exact
Pod identities and their matching challenge-bound results before the deadline.
The sender's recorded challenge must equal this independently generated value.
A pre-trigger first-packet log, an earlier arbitrary trigger, missing/mismatched
acknowledgment or a report-supplied challenge never satisfies this barrier. An
unexpected earlier trigger poisons the case. The nonce is causal probe data,
not a credential or a new authority token; even a copy learned after its
creation cannot cause a matching attempt before the confirmed trigger.

This is a new closed probe program/template, specified here rather than assumed
to exist. It uses no Kubernetes credential, exec, patch, proxy or new RBAC verb.
Both receivers are in the owned probe namespace; the deleted original allowed
sender is not given this endpoint in the protected namespace. A fixed owned
probe policy permits only the trigger role to the case-5 sender role on TCP
42001. The barrier port must differ from every measured target port; otherwise
the case is unavailable. The producer verifies the gate policy cannot allow
any measured target port or select the protected namespace. It is never merged
into the policy whose allowance is removed. The one-shot trigger, gate policy
and sender are all owned resources with normal bounded cleanup. A missing or
unimplemented barrier remains unavailable; there is no caller flag fallback.

## Verdict, identity and implementation gates

<!-- @spec CNI-FRESH-FLOW-4 -->
`fail` means any forbidden handshake, reply, matching listener delivery or
matching socket observation. Preserve such a positive leak finding even if a
later control or cleanup is unavailable. Acceptance still refuses when cleanup
is missing; a leak finding never authorizes continued work.

`pass` requires the exact matrix, independently confirmed triggers and identities,
all five fresh denial attempts per endpoint without delivery, complete listener
coverage, no overflow, successful nonce-bound round-trip controls and cleanup.
All other missing or uncertain proof is `unavailable` and refuses. A client
timeout or an all-true report cannot supply the required observations.

The managed kind is `managed-behavioral-v2`; local kind is `local-cilium-v2`.
Bind these kinds, exact constants and the case-5 interpretation into the actual
measurement identity and affected digests. Version selection is measured from
the producer/artifact, never caller-supplied. Keep v1 interpretation closed;
reject mixed v1/v2 observations or absent bindings. Preserve the existing twelve
signed fact names, including `retained_peer`; do not add report/receipt authority
to the guard channel. A hash is only a binding, not proof of producer execution.

Implementation under #4557 must cover `managed_cni_timing.py`,
`managed_cni_probe_programs.py`, `managed_cni_probe_kit.py`,
`managed_cni_identity.py` and `commissioning_local_network.py` under
`apps/worker/src/curie_worker/`. Add focused behavioral tests for actual fresh
connect success, listener-only delivery, one-way/dead controls, tuple reuse,
missing/replaced UIDs, unprovoked address reuse, deadline overrun, pre-trigger/mismatched/repeated barrier challenges, gate
policy overlap, version substitution and single-node cross-node refusal. An owned two-node actual CNI
experiment must corroborate fresh denial and a falsifiable unfiltered control;
fake cluster fixtures do not qualify the installation. No paid provider, cloud,
production, release or deployment action is authorized here.
