"""The local Cilium network producer and its per-call network fact, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. A fake probe kit and a fake API stand in for
the cluster. These tests prove the composition only: the engine identity
brackets the kit's behavioral measurement, the local required set, and that
the network fact is reobserved on every call and refuses on any change. No
cluster is measured here and nothing here is qualification evidence.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from curie_worker.commissioning_local_cilium import CILIUM_AGENT_OBJECTS, CILIUM_POLICY_COLLECTIONS
from curie_worker.commissioning_local_network import (
    FACTS,
    LOCAL_PRODUCER_KIND,
    NOT_APPLICABLE_SUB_CASES,
    LocalCiliumNetworkProducer,
    local_channel_digest,
    local_required_entries,
)
from curie_worker.managed_cni_cases import (
    Destination,
    KitResult,
    ProbeLayout,
    Subject,
    Target,
    required_entries,
)
from curie_worker.managed_cni_probe_kit import (
    Measurement,
    ObservationEntry,
    ProbeKitRefused,
)

sys.path.insert(0, str(Path(__file__).parent))
from test_commissioning_local_cilium import (  # noqa: E402
    NODES,
    OPERATOR_RS,
    PROFILE,
    PROT,
    PROXY_CONFIG,
    PROXY_DS,
    REVISION,
    VERSION,
    FakeReader,
    _agents,
    _config,
    installation,
)

DIGEST = "cd" * 32
FIXTURE = Destination("192.168.49.9", 18081, "tcp")


def layout(**changes: Any) -> ProbeLayout:
    values: dict[str, Any] = {
        "campaign": "d8",
        "protected_namespace": PROT,
        "probe_namespace": "probe-a",
        "foreign_namespace": "probe-foreign",
        "release_namespace": "release",
        "eligible_nodes": NODES,
        "families": ("ipv4",),
        "targets": (
            Target("broker", {"r": "broker"}, {"r": "bc"}, 16379, ("tcp",), 16380, ("10.0.0.1",)),
            Target("mediator", {"r": "m"}, {"r": "mc"}, 18443, ("tcp",), 18444, ("10.0.0.2",)),
        ),
        "runner_labels": {"r": "runner"},
        "declared_peers": (Destination("10.0.0.3", 18080, "tcp"),),
        "supplied_egress": {
            "kubernetes_api": (Destination("10.96.0.1", 443, "tcp"),),
            "cloud_network": (FIXTURE,),
        },
        "guard_names": ("guard.example",),
        "system_set": frozenset({Subject("User", "system:kube-scheduler")}),
        "excluded": frozenset(),
        "provider_policy_collections": CILIUM_POLICY_COLLECTIONS,
        "policy_agent_objects": CILIUM_AGENT_OBJECTS,
    }
    values.update(changes)
    return ProbeLayout(**values)


def entry(key: tuple[int, str, str, str, str], outcome: str = "pass", reason: str = "") -> Any:
    case, sub_case, family, placement, source = key
    return ObservationEntry(
        case=case,
        sub_case=sub_case,
        family=family,
        placement=placement,
        source_kind=source,
        outcome=outcome,  # type: ignore[arg-type]
        reason=reason or ("denied" if outcome == "pass" else "delivered"),
        evidence_digest=DIGEST,
    )


def kit_entries(probe_layout: ProbeLayout) -> list[ObservationEntry]:
    """What the kit reports on a local install: every entry passes but instance metadata."""
    entries = []
    for key in sorted(required_entries(probe_layout)):
        if key[1] == "instance_metadata":
            entries.append(entry(key, "unavailable", "destination_unknown"))
        else:
            reason = "reachable" if key[1] == "declared_peers" else ""
            entries.append(entry(key, reason=reason))
    return entries


class FakeKit:
    def __init__(
        self,
        entries: list[ObservationEntry],
        refusal: str | None = "destination_unknown",
        leftovers: tuple[str, ...] = (),
        during: Callable[[], None] | None = None,
    ) -> None:
        self.entries, self.refusal, self.leftovers, self.during = (
            entries,
            refusal,
            leftovers,
            during,
        )
        self.last: KitResult | None = None
        self.calls = 0

    async def measure(self) -> Measurement:
        self.calls += 1
        if self.during is not None:
            self.during()
        from curie_worker.managed_cni_probe_kit import PRODUCER_KIND, producer_constants_digest
        from curie_worker.managed_cni_probe_programs import probe_kit_digest

        measurement = Measurement(tuple(self.entries), self.refusal, PRODUCER_KIND,
                                  producer_constants_digest(), probe_kit_digest())
        self.last = KitResult(measurement, ({"kit": "evidence"},), self.leftovers)
        return measurement


class FakeCluster(FakeReader):
    def __init__(self, state: dict[str, Any]) -> None:
        super().__init__(state)
        self.policies: dict[str, list[dict[str, Any]] | None] = {
            "networkpolicies:" + PROT: [self.policy("default-deny", "1")],
            CILIUM_POLICY_COLLECTIONS[0]: [],
            CILIUM_POLICY_COLLECTIONS[1]: [],
        }

    @staticmethod
    def policy(name: str, version: str, namespace: str = PROT) -> dict[str, Any]:
        meta = {"name": name, "uid": "uid-" + name, "resourceVersion": version}
        return {"metadata": meta | ({"namespace": namespace} if namespace else {})}

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        if collection == "nodes":
            return await super().list_items(collection)
        value = self.policies[collection]
        return None if value is None else [dict(item) for item in value]


def bound(entries: Any) -> Measurement:
    from curie_worker.managed_cni_probe_kit import PRODUCER_KIND, producer_constants_digest
    from curie_worker.managed_cni_probe_programs import probe_kit_digest

    return Measurement(
        tuple(entries), None, PRODUCER_KIND, producer_constants_digest(), probe_kit_digest()
    )


def producer(
    cluster: FakeCluster | None = None, kit: FakeKit | None = None, **changes: Any
) -> tuple[LocalCiliumNetworkProducer, FakeCluster, FakeKit]:
    probe_layout = layout(**changes)
    cluster = cluster or FakeCluster(installation())
    kit = kit or FakeKit(kit_entries(probe_layout))
    made = LocalCiliumNetworkProducer(cluster, probe_layout, profile=PROFILE, kit=kit)
    return made, cluster, kit


def measured(**changes: Any) -> tuple[LocalCiliumNetworkProducer, FakeCluster, FakeKit]:
    made, cluster, kit = producer(**changes)
    assert asyncio.run(made.measure()).refusal is None
    return made, cluster, kit


def test_the_local_producer_has_its_own_kind() -> None:
    made, _, _ = producer()
    assert made.producer_kind == LOCAL_PRODUCER_KIND == "local-cilium-v2"


def test_the_local_required_set_is_an_allowlist_of_the_three_peer_facts() -> None:
    probe_layout = layout()
    assert NOT_APPLICABLE_SUB_CASES == frozenset({"instance_metadata"})
    local = local_required_entries(probe_layout)
    # Exactly the cases of the network (1, 6, 8), hostile (2, 3, 4) and
    # retained-peer (5) facts, never instance metadata.
    assert {key[0] for key in local} == {1, 2, 3, 4, 5, 6, 8}
    assert (6, "instance_metadata", "ipv4", "none", "runner_policy") not in local
    # Case 7 is not wired locally and is dropped, like any later kit case.
    dropped = required_entries(probe_layout) - local
    assert {key[0] for key in dropped} == {6, 7}
    assert {key for key in dropped if key[0] == 6} == {
        (6, "instance_metadata", "ipv4", "none", "runner_policy")
    }
    assert FACTS == {
        "network": ("network", (1, 6, 8)),
        "hostile_peer": ("hostile", (2, 3, 4)),
        "retained_peer": ("retained_peer", (5,)),
    }


def test_a_clean_local_measurement_passes_every_local_entry() -> None:
    made, _, kit = producer()
    measurement = asyncio.run(made.measure())
    assert measurement.refusal is None and kit.calls == 1
    assert {item.key for item in measurement.entries} == local_required_entries(layout())
    assert {item.outcome for item in measurement.entries} == {"pass"}
    assert made.last is not None and made.last.identity is not None


@pytest.mark.parametrize(
    "changes",
    [
        # The local layout reads Cilium's policy collections and protects its objects.
        {"provider_policy_collections": ()},
        {"provider_policy_collections": CILIUM_POLICY_COLLECTIONS[:1]},
        {"policy_agent_objects": CILIUM_AGENT_OBJECTS[1:]},
        # A local installation has no metadata service; supplying one is a mistake.
        {
            "supplied_egress": {
                "instance_metadata": (Destination("169.254.169.254", 80, "tcp"),),
                "cloud_network": (FIXTURE,),
            }
        },
        # Off-cluster egress needs the owned fixture on the node network.
        {"supplied_egress": {"kubernetes_api": (Destination("10.96.0.1", 443, "tcp"),)}},
        {"eligible_nodes": NODES[:1]},
    ],
)
def test_a_layout_the_local_producer_cannot_measure_refuses(changes: dict[str, Any]) -> None:
    with pytest.raises(ProbeKitRefused):
        producer(**changes)


def test_the_kit_never_runs_when_the_engine_cannot_be_bound() -> None:
    state = installation()
    _config(state)["policy-audit-mode"] = "true"
    made, _, kit = producer(cluster=FakeCluster(state))
    measurement = asyncio.run(made.measure())
    assert (measurement.entries, measurement.refusal, kit.calls) == ((), "cni_unavailable", 0)


def test_families_other_than_the_engine_serves_refuse_before_the_kit() -> None:
    made, _, kit = producer(families=("ipv4", "ipv6"))
    measurement = asyncio.run(made.measure())
    assert (measurement.refusal, kit.calls) == ("families_unserved", 0)


def test_an_engine_change_during_the_measurement_refuses() -> None:
    cluster = FakeCluster(installation())

    def restart() -> None:
        _agents(cluster.state)[0]["status"]["containerStatuses"][0]["restartCount"] = 1

    made, _, _ = producer(cluster=cluster, kit=FakeKit(kit_entries(layout()), during=restart))
    assert asyncio.run(made.measure()).refusal == "cni_changed"


def test_a_policy_change_during_the_measurement_refuses() -> None:
    cluster = FakeCluster(installation())

    def edit() -> None:
        cluster.policies[CILIUM_POLICY_COLLECTIONS[1]] = [FakeCluster.policy("allow-all", "9", "")]

    made, _, _ = producer(cluster=cluster, kit=FakeKit(kit_entries(layout()), during=edit))
    assert asyncio.run(made.measure()).refusal == "policy_set_changed"


@pytest.mark.parametrize(
    "refusal,leftovers,expected",
    [
        ("enforcement_absent", (), "enforcement_absent"),
        ("non_vacuity_unavailable", (), "non_vacuity_unavailable"),
        # The kit folds an unconfirmed cleanup behind an earlier refusal; keep it.
        ("destination_unknown", ("probe-a/x",), "cleanup_unconfirmed"),
    ],
)
def test_kit_refusals_survive_the_local_filter(
    refusal: str, leftovers: tuple[str, ...], expected: str
) -> None:
    kit = FakeKit(kit_entries(layout()), refusal=refusal, leftovers=leftovers)
    made, _, _ = producer(kit=kit)
    assert asyncio.run(made.measure()).refusal == expected


@pytest.mark.parametrize(
    "outcome,reason",
    [("unavailable", "destination_unreached"), ("fail", "reply_received")],
)
def test_an_instance_metadata_entry_with_any_observation_is_kept(outcome: str, reason: str) -> None:
    entries = [
        entry(item.key, outcome, reason) if item.sub_case == "instance_metadata" else item
        for item in kit_entries(layout())
    ]
    made, _, _ = producer(kit=FakeKit(entries))
    measurement = asyncio.run(made.measure())
    assert measurement.refusal == reason
    assert any(item.sub_case == "instance_metadata" for item in measurement.entries)


def test_a_missing_or_failed_local_entry_refuses() -> None:
    entries = kit_entries(layout())
    node = next(i for i, item in enumerate(entries) if item.sub_case == "node_addresses")
    failed = list(entries)
    failed[node] = entry(entries[node].key, "fail", "reply_received")
    made, _, _ = producer(kit=FakeKit(failed))
    assert asyncio.run(made.measure()).refusal == "reply_received"
    made, _, _ = producer(kit=FakeKit(entries[:node] + entries[node + 1 :]))
    assert asyncio.run(made.measure()).refusal == "incomplete"


def network(made: LocalCiliumNetworkProducer) -> str:
    fact = made.fact("network")
    assert fact.fact == "network"
    return asyncio.run(fact.observe())


def test_the_network_fact_is_the_channel_digest_of_the_network_cases() -> None:
    made, _, _ = measured()
    assert made.last is not None
    expected = local_channel_digest(bound(made.last.measurement.entries), "network")
    assert network(made) == network(made) == expected


def test_the_network_fact_covers_only_network_cases() -> None:
    made, _, _ = measured()
    assert made.last is not None
    entries = made.last.measurement.entries
    assert (local_channel_digest(bound([i for i in entries if i.case != 2]), "network")
            == network(made))


def test_other_kit_cases_change_neither_the_fact_digest_nor_the_verdict() -> None:
    # A kit that also reports case 7 produces the same facts and the same (clean)
    # verdict as a kit that reports only the local cases: case 7 is inert here.
    probe_layout = layout()
    full, _, _ = measured()  # kit_entries reports every case 1..8
    full_digest = network(full)
    local_keys = local_required_entries(probe_layout)
    local_only = [item for item in kit_entries(probe_layout) if item.key in local_keys]
    lean, _, _ = producer(kit=FakeKit(local_only))
    assert asyncio.run(lean.measure()).refusal is None
    assert network(lean) == full_digest
    for name in ("hostile_peer", "retained_peer"):
        assert observe(lean, name) == observe(full, name)


def test_a_non_pass_dropped_case_neither_refuses_nor_changes_the_digest() -> None:
    # An extra non-pass case-7 entry (an unknown case to this producer) is dropped
    # before the verdict, so it does not refuse the measurement or move the digest.
    probe_layout = layout()
    base_digest = network(measured()[0])
    entries, flipped = [], False
    for item in kit_entries(probe_layout):
        if item.case == 7 and not flipped:
            item, flipped = entry(item.key, "unavailable", "node_probe_unavailable"), True
        entries.append(item)
    assert flipped
    made, _, _ = producer(kit=FakeKit(entries))
    assert asyncio.run(made.measure()).refusal is None
    assert network(made) == base_digest


def test_a_missing_required_local_sub_case_still_refuses() -> None:
    # Dropping a required case-1 entry leaves the local required set incomplete.
    probe_layout = layout()
    missing = (1, "non_vacuity", "ipv4", "same_node", "foreign_namespace")
    entries = [item for item in kit_entries(probe_layout) if item.key != missing]
    made, _, _ = producer(kit=FakeKit(entries))
    assert asyncio.run(made.measure()).refusal is not None


def test_exactly_the_three_peer_facts_exist() -> None:
    made, _, _ = measured()
    for name in ("network", "hostile_peer", "retained_peer"):
        assert made.fact(name).fact == name
    for name in ("cni", "permit", "hostile", "node_local"):
        with pytest.raises(ProbeKitRefused):
            made.fact(name)


def test_the_network_fact_refuses_before_a_measurement() -> None:
    made, _, _ = producer()
    with pytest.raises(ProbeKitRefused):
        network(made)


def test_the_network_fact_refuses_after_a_refused_measurement() -> None:
    made, _, _ = producer(kit=FakeKit(kit_entries(layout()), refusal="enforcement_absent"))
    asyncio.run(made.measure())
    with pytest.raises(ProbeKitRefused):
        network(made)


@pytest.mark.parametrize(
    "change",
    [
        lambda c: _agents(c.state)[1]["metadata"].update({"uid": "uid-replaced"}),
        lambda c: _config(c.state).update({"enable-policy": "never"}),
        lambda c: c.state["nodes"].append(c.state["nodes"][0] | {"metadata": {"name": "new"}}),
        lambda c: c.state[VERSION].update({"gitVersion": "v1.35.2"}),
        lambda c: c.state[PROXY_CONFIG]["data"].update({"config.conf": 'mode: "ipvs"\n'}),
        lambda c: c.state[PROXY_DS]["metadata"].update({"uid": "proxy-replaced"}),
        lambda c: c.state[OPERATOR_RS]["items"][0]["metadata"]["annotations"].update(
            {REVISION: "3"}
        ),
        lambda c: c.policies["networkpolicies:" + PROT].append(FakeCluster.policy("allow", "2")),
        lambda c: c.policies["networkpolicies:" + PROT][0]["metadata"].update(
            {"resourceVersion": "7"}
        ),
        lambda c: c.policies.update({CILIUM_POLICY_COLLECTIONS[0]: None}),
    ],
)
def test_every_call_reobserves_and_refuses_on_any_change(
    change: Callable[[FakeCluster], None],
) -> None:
    made, cluster, _ = measured()
    network(made)
    change(cluster)
    with pytest.raises(ProbeKitRefused):
        network(made)


def test_reobservation_never_reruns_the_behavioral_cases() -> None:
    made, _, kit = measured()
    for _ in range(3):
        network(made)
    assert kit.calls == 1


class StubFact:
    def __init__(self, fact: str) -> None:
        self.fact = fact

    async def observe(self) -> str:
        return "ab" * 32


def test_the_network_fact_serves_the_commissioning_reobservation_set() -> None:
    producers = importlib.import_module("curie_protected_hooks.commissioning_producers")
    made, cluster, _ = measured()
    facts = tuple(
        made.fact(name) if name == "network" else StubFact(name)
        for name in producers.OBSERVATION_FACTS
    )
    reobservation = producers.ReobservationSet(facts)
    assert asyncio.run(reobservation.reobserve())["network"] == network(made)
    _config(cluster.state)["enable-policy"] = "never"
    with pytest.raises(producers.ProducerRefused):
        asyncio.run(reobservation.reobserve())


def observe(made: LocalCiliumNetworkProducer, name: str) -> str:
    return asyncio.run(made.fact(name).observe())


def with_outcome(sub_case: str, outcome: str, reason: str) -> list[ObservationEntry]:
    entries = kit_entries(layout())
    return [
        entry(item.key, outcome, reason) if item.sub_case == sub_case else item for item in entries
    ]


def test_each_peer_fact_is_the_channel_digest_of_its_own_cases() -> None:
    made, _, _ = measured()
    assert made.last is not None
    entries = made.last.measurement.entries
    assert observe(made, "hostile_peer") == local_channel_digest(bound(entries), "hostile")
    assert observe(made, "retained_peer") == local_channel_digest(bound(entries), "retained_peer")
    hostile = [item for item in entries if item.case in (2, 3, 4)]
    assert local_channel_digest(bound(hostile), "hostile") == observe(made, "hostile_peer")
    assert len({network(made), observe(made, "hostile_peer"), observe(made, "retained_peer")}) == 3


@pytest.mark.parametrize(
    "sub_case,reason,refused,kept",
    [
        # Case 5 workload_replaced cannot pass from user space; it refuses only its fact.
        (
            "workload_replaced",
            "sequence_state_unavailable",
            "retained_peer",
            ("network", "hostile_peer"),
        ),
        # Address reuse a local cluster could not provoke is unavailable, never a pass.
        ("address_reuse", "reuse_not_provoked", "hostile_peer", ("network", "retained_peer")),
        ("spoof_terminated", "transport_unavailable", "hostile_peer", ("network", "retained_peer")),
    ],
)
def test_an_unavailable_peer_case_refuses_its_fact_and_the_measurement(
    sub_case: str, reason: str, refused: str, kept: tuple[str, ...]
) -> None:
    made, _, _ = producer(kit=FakeKit(with_outcome(sub_case, "unavailable", reason)))
    measurement = asyncio.run(made.measure())
    # The producer's own verdict is never positive while any local entry is not pass.
    assert measurement.refusal == reason
    with pytest.raises(ProbeKitRefused):
        observe(made, refused)
    # Facts judge only their own channel, so the evidence shows which one refused.
    clean, _, _ = measured()
    for name in kept:
        assert observe(made, name) == observe(clean, name)


@pytest.mark.parametrize(
    "sub_case,reason,refused",
    [
        ("spoof_live", "delivered", "hostile_peer"),
        ("new_pod", "delivered", "hostile_peer"),
        ("policy_changed", "retained_flow", "retained_peer"),
        ("workload_replaced", "retained_flow", "retained_peer"),
    ],
)
def test_a_peer_delivery_fails_its_fact(sub_case: str, reason: str, refused: str) -> None:
    made, _, _ = producer(kit=FakeKit(with_outcome(sub_case, "fail", reason)))
    assert asyncio.run(made.measure()).refusal == reason
    with pytest.raises(ProbeKitRefused):
        observe(made, refused)


@pytest.mark.parametrize("missing", ["spoof_live", "new_pod", "policy_changed"])
def test_a_missing_peer_entry_refuses_its_fact(missing: str) -> None:
    entries = [item for item in kit_entries(layout()) if item.sub_case != missing]
    made, _, _ = producer(kit=FakeKit(entries))
    assert asyncio.run(made.measure()).refusal == "incomplete"
    fact = "retained_peer" if missing == "policy_changed" else "hostile_peer"
    with pytest.raises(ProbeKitRefused):
        observe(made, fact)
    assert network(made)


@pytest.mark.parametrize("name", ["network", "hostile_peer", "retained_peer"])
@pytest.mark.parametrize(
    "refusal,leftovers,expected",
    [
        # The engine identity is also rechecked per call; these two are not, so only
        # the bracket guard can refuse a peer fact after them.
        ("enforcement_absent", (), "enforcement_absent"),
        (None, ("probe-a/cni-d8-1",), "cleanup_unconfirmed"),
        ("non_vacuity_unavailable", (), "non_vacuity_unavailable"),
    ],
)
def test_a_bracket_refusal_refuses_every_fact(
    name: str, refusal: str | None, leftovers: tuple[str, ...], expected: str
) -> None:
    # Every entry passes, so no per-fact entry check could refuse on its own.
    kit = FakeKit(kit_entries(layout()), refusal=refusal, leftovers=leftovers)
    made, _, _ = producer(kit=kit)
    assert asyncio.run(made.measure()).refusal == expected
    with pytest.raises(ProbeKitRefused):
        observe(made, name)


@pytest.mark.parametrize("name", ["network", "hostile_peer", "retained_peer"])
def test_an_engine_change_during_the_measurement_refuses_every_fact(name: str) -> None:
    cluster = FakeCluster(installation())

    def restart() -> None:
        _agents(cluster.state)[0]["status"]["containerStatuses"][0]["restartCount"] = 1

    made, _, _ = producer(cluster=cluster, kit=FakeKit(kit_entries(layout()), during=restart))
    assert asyncio.run(made.measure()).refusal == "cni_changed"
    with pytest.raises(ProbeKitRefused):
        observe(made, name)


@pytest.mark.parametrize("name", ["hostile_peer", "retained_peer"])
def test_every_peer_fact_call_reobserves_the_engine_and_policies(name: str) -> None:
    made, cluster, kit = measured()
    observe(made, name)
    _config(cluster.state)["enable-policy"] = "never"
    with pytest.raises(ProbeKitRefused):
        observe(made, name)
    _config(cluster.state)["enable-policy"] = "default"
    observe(made, name)
    cluster.policies[CILIUM_POLICY_COLLECTIONS[0]] = [FakeCluster.policy("allow", "3", PROT)]
    with pytest.raises(ProbeKitRefused):
        observe(made, name)
    assert kit.calls == 1


def test_all_three_peer_facts_serve_the_commissioning_reobservation_set() -> None:
    producers = importlib.import_module("curie_protected_hooks.commissioning_producers")
    made, _, _ = measured()
    mine = ("network", "hostile_peer", "retained_peer")
    facts = tuple(
        made.fact(name) if name in mine else StubFact(name) for name in producers.OBSERVATION_FACTS
    )
    reobserved = asyncio.run(producers.ReobservationSet(facts).reobserve())
    assert {name: reobserved[name] for name in mine} == {name: observe(made, name) for name in mine}


@pytest.mark.parametrize(
    "extra,fact",
    [
        ((3, "spoof_live", "ipv6", "same_node", "raw_socket"), "hostile_peer"),
        ((5, "policy_changed", "ipv6", "cross_node", "retained_peer"), "retained_peer"),
    ],
)
def test_an_extra_unrequired_peer_entry_refuses(
    extra: tuple[int, str, str, str, str], fact: str
) -> None:
    # A pass entry for a family the layout does not serve is not evidence of it.
    made, _, _ = producer(kit=FakeKit([*kit_entries(layout()), entry(extra)]))
    assert asyncio.run(made.measure()).refusal == "incomplete"
    with pytest.raises(ProbeKitRefused):
        observe(made, fact)


@pytest.mark.parametrize(
    "change",
    [
        {},
        {"producer_kind": "managed-behavioral-v1"},
        {"producer_constants_digest": "0" * 64},
        {"probe_kit_digest": "0" * 64},
    ],
)
def test_missing_or_mixed_behavioral_version_cannot_qualify_local_facts(
    change: dict[str, Any],
) -> None:
    from dataclasses import replace

    class WrongKit(FakeKit):
        async def measure(self) -> Measurement:
            measurement = await super().measure()
            if not change:
                return Measurement(measurement.entries, measurement.refusal)
            return replace(measurement, **change)

    made, _, _ = producer(kit=WrongKit(kit_entries(layout())))
    assert asyncio.run(made.measure()).refusal == "producer_binding_unavailable"
    with pytest.raises(ProbeKitRefused):
        observe(made, "retained_peer")


def test_an_earlier_cleanup_refusal_blocks_every_local_fact_even_after_later_absence() -> None:
    made, _, _ = producer(kit=FakeKit(kit_entries(layout()), refusal="cleanup_unconfirmed"))
    assert asyncio.run(made.measure()).refusal == "cleanup_unconfirmed"
    for name in FACTS:
        with pytest.raises(ProbeKitRefused):
            observe(made, name)
