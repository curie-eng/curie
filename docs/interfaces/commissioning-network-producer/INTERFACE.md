---
seam: Commissioning network producer
kind: CLEAN
impls: ProbeKit (managed-behavioral-v1) and LocalCiliumNetworkProducer (local-cilium-v1)
grade: not separately graded
epics:
  - ADR-0236
  - ADR-0238
  - "#4557"
order: 31
---

# INTERFACE: Commissioning network producer

> Part of the Curie swappable-seam catalog. See the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** ProbeKit (managed-behavioral-v1) and LocalCiliumNetworkProducer (local-cilium-v1) &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol`/typed port class · SOFT = swap via env/URL/prefix/wire, no code interface · NONE = not built yet.

## The black line

A commissioning campaign measures preventive network enforcement by calling one producer. The typed line is `apps/worker/src/curie_worker/managed_cni_probe_kit.py::BehavioralNetworkProducer`. It exposes `producer_kind` and `measure`, and `measure` returns `apps/worker/src/curie_worker/managed_cni_probe_kit.py::Measurement`. Callers select the producer for the installation's CNI. [ADR-0236](../../adr/0236-the-commissioning-operator-splits-provisioner-and-control-processes.md) states that selection: the local Cilium producer is the first one, and an installation whose CNI has no producer refuses before the campaign starts. [ADR-0238](../../adr/0238-a-managed-cni-installation-qualifies-through-its-own-behavioral-network-producer.md) is the managed behavioral producer.

## Current contract

`BehavioralNetworkProducer` is a `Protocol`. `producer_kind` is a `str` property. `measure` is async and returns `Measurement`, a frozen pair of `apps/worker/src/curie_worker/managed_cni_probe_kit.py::ObservationEntry` values and an optional refusal string. The managed kind constant is `apps/worker/src/curie_worker/managed_cni_probe_kit.py::PRODUCER_KIND` (`managed-behavioral-v1`). The local kind constant is `apps/worker/src/curie_worker/commissioning_local_network.py::LOCAL_PRODUCER_KIND` (`local-cilium-v1`).

Neither class subclasses the protocol. Both match it structurally: `apps/worker/src/curie_worker/managed_cni_cases.py::ProbeKit` and `apps/worker/src/curie_worker/commissioning_local_network.py::LocalCiliumNetworkProducer`.

`apps/worker/src/curie_worker/commissioning_local_network.py::LocalNetworkFact` is the fact adapter on the local producer. It matches `packages/protected-hooks/src/curie_protected_hooks/commissioning_producers.py::FactProducer` (`fact` and `observe`) for the `network` fact. A call reobserves the Cilium engine identity and the policy set that bound the last measurement, then returns `apps/worker/src/curie_worker/managed_cni_probe_kit.py::channel_digest`.

## Implementations today

Two producers. Production composition roots in this tree do not construct either one. The classes are imported from their modules and from tests.

1. Managed behavioral kit. `ProbeKit` runs cases 1, 2, 6 and 8 against `apps/worker/src/curie_worker/managed_cni_cases.py::ProbeCluster`. Its `producer_kind` is `PRODUCER_KIND`.
2. Local Cilium. `LocalCiliumNetworkProducer` takes a `apps/worker/src/curie_worker/commissioning_local_network.py::LocalCluster`, which is `ProbeCluster` plus `apps/worker/src/curie_worker/commissioning_local_cilium.py::CiliumReader`. It runs a `ProbeKit` (or a caller supplied kit with the same `measure` shape), drops the instance metadata sub-case when that sub-case was unobserved, and refuses when the Cilium identity or the policy set changes across the run. Its `producer_kind` is `LOCAL_PRODUCER_KIND`. `LocalNetworkFact` is built from `LocalCiliumNetworkProducer.fact`.

## Related narrow ports

These protocols feed the same commissioning flow. Each is its own typed port with a narrower job than `BehavioralNetworkProducer`.

1. `apps/worker/src/curie_worker/managed_cni_cases.py::ProbeCluster` is the cluster surface the cases call under the measuring identity: create, read, log and delete pods, dry-run create, access review, and list items.
2. `apps/worker/src/curie_worker/commissioning_local_cilium.py::CiliumReader` adds fixed read-only `read` and `list_items` paths for the local Cilium engine.
3. `packages/protected-hooks/src/curie_protected_hooks/commissioning_producers.py::FactProducer` is one ADR 0236 fact, observed again on every call. `packages/protected-hooks/src/curie_protected_hooks/commissioning_producers.py::AclUserReader` is the broker session that claims to be the read-only ACL observer.
4. `packages/protected-hooks/src/curie_protected_hooks/commissioning_operator_guard.py::ObservationTransport` is the signed channel that returns only after a verified reply for the campaign digests.
5. `packages/protected-hooks/src/curie_protected_hooks/commissioning_provisioner.py::BrokerAclRecipes` returns one principal's measured ACL rule tokens.

## Known leakage

1. **The shared result types live in the managed CNI module.** `Measurement` and `ObservationEntry` are defined in `managed_cni_probe_kit.py`. `LocalCiliumNetworkProducer` imports both, and also `apps/worker/src/curie_worker/managed_cni_probe_kit.py::PolicySet`, `apps/worker/src/curie_worker/managed_cni_probe_kit.py::ProbeKitRefused`, `apps/worker/src/curie_worker/managed_cni_probe_kit.py::accepts`, `channel_digest`, `apps/worker/src/curie_worker/managed_cni_probe_kit.py::refuse`, `apps/worker/src/curie_worker/managed_cni_probe_kit.py::require` and `apps/worker/src/curie_worker/managed_cni_probe_kit.py::PRODUCER_CONSTANTS`. A second producer that returns `Measurement` imports that module.
2. **The local producer reaches into the kit's case module, including a private runner.** `commissioning_local_network.py` imports `ProbeKit`, `ProbeCluster`, `apps/worker/src/curie_worker/managed_cni_cases.py::ProbeLayout`, `apps/worker/src/curie_worker/managed_cni_cases.py::policy_snapshot`, `apps/worker/src/curie_worker/managed_cni_cases.py::required_entries`, `apps/worker/src/curie_worker/managed_cni_cases.py::EntryKey`, `apps/worker/src/curie_worker/managed_cni_cases.py::KitResult` and `apps/worker/src/curie_worker/managed_cni_cases.py::_Run`. Policy snapshots are taken by constructing `_Run`.

## Cross-links

**ADR(s):** [ADR-0236](../../adr/0236-the-commissioning-operator-splits-provisioner-and-control-processes.md) selects a network producer per CNI and tracks [#4557](https://github.com/curie-eng/curie/issues/4557). [ADR-0238](../../adr/0238-a-managed-cni-installation-qualifies-through-its-own-behavioral-network-producer.md) specifies the managed behavioral producer.

**Sibling seams:** [protected-hook-source-authority](../protected-hook-source-authority/INTERFACE.md) is the protected source resolver beside this commissioning flow. The sandbox [substrate](../substrate/INTERFACE.md) and the work [queue-stream](../queue-stream/INTERFACE.md) are separate lines. This producer measures an installation CNI through `ProbeCluster` and `CiliumReader`.

**Vision doc:** [architecture-vision.md](../../architecture-vision.md). This seam is outside the graded swap-readiness rows, so it is not separately graded.
