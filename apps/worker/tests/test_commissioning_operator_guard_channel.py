"""The commissioning guard over the actual signed observation channel, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. A fake ConfigMap API behind the bounded HTTPS
client, real Ed25519 signatures and fake producers on both sides. This proves
that the guard's transport is the merged channel and that a stale or unsigned
reply refuses; nothing here observes an installation or is campaign evidence.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from urllib.parse import urlsplit

import pytest

from .test_commissioning_observation_channel import DIGESTS, Store, api, pair, serving

OTHER = "f" * 64


def _ref(name):
    return {"id": "example/" + name, "generation": "1"}


def bootstrap():
    """An anonymous B; the guard's ACL baseline must belong to it."""
    parse = importlib.import_module("curie_protected_hooks.commissioning_bootstrap")
    value = {
        "schema_version": 1,
        "record_type": "commissioning_bootstrap",
        "runtime_id": "66666666-6666-4666-8666-666666666666",
        "runtime_generation": "1",
        "execution_config_digest": "7" * 64,
        "entry_points": {"production": "composition/p", "commissioning": "composition/c"},
        "production_credentials": {n: _ref(n) for n in ("enqueue", "worker", "verifier")},
        "candidate_credentials": {"enqueue": _ref("c-enqueue"), "worker": _ref("c-worker")},
        **{
            n + "_identity": _ref(n)
            for n in (
                "issuer",
                "control",
                "qualification_publisher",
                "control_reader",
                "source_writer",
                "mediator",
                "gateway",
            )
        },
        "source_scope": [
            {
                "source_id": "77777777-7777-4777-8777-777777777777",
                "generation": "1",
                "policy_fingerprint": "8" * 64,
            }
        ],
        "target_scope": ["fixture/example-readonly"],
        "max_permit_ms": "1000",
    }
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    return parse.parse_commissioning_bootstrap(raw)


def product():
    return (
        importlib.import_module("curie_protected_hooks.commissioning_operator_guard"),
        importlib.import_module("curie_protected_hooks.commissioning_producers"),
    )


class Fact:
    def __init__(self, fact, facts, value=None):
        self.fact, self.facts, self.value, self.calls = fact, facts, value, 0

    async def observe(self):
        self.calls += 1
        return self.facts[self.fact] if self.value is None else self.value(self.calls)


OBSERVER = "example/acl-observer"


class Reader:
    def __init__(self):
        self.reply = {"flags": ["on"], "passwords": ["a" * 64]}
        self.own = {
            "flags": ["on"],
            "passwords": ["b" * 64],
            "commands": "-@all +auth +hello +ping +client|setname +client|setinfo +acl|getuser",
            "keys": "",
            "channels": "",
            "selectors": [],
        }

    async def getuser(self, username):
        return self.own if username == OBSERVER else self.reply

    async def key_read_denied(self, key):
        return True


def compose(store, *, provisioner=None, timeout_s=2.0):
    guard_m, producers = product()
    b = bootstrap()
    facts = DIGESTS | {"bootstrap": b.digest}
    control = {name: Fact(name, facts) for name in producers.OBSERVATION_FACTS}
    remote = {
        name: Fact(name, facts, (provisioner or {}).get(name))
        for name in producers.OBSERVATION_FACTS
    }
    reobservation = producers.ReobservationSet(tuple(remote.values()))
    requester, responder, _, key = pair(store, observed=None, timeout_s=timeout_s)
    responder._reobserve, responder._facts = reobservation.reobserve, facts
    state = producers.AclPrincipalState(frozenset({"a" * 64}), True)
    baseline = producers.AclBaseline(b, {name: state for name in producers.ACL_PRINCIPALS})
    guard = guard_m.ChannelCampaignGuard(
        premeasured=facts,
        control=producers.ReobservationSet(tuple(control.values())),
        acl=producers.AclPrincipalStateProducer(
            reader=Reader(),
            baseline=baseline,
            observer_username=OBSERVER,
            provisioner_username="example/provisioner",
        ),
        phase=lambda: producers.PhaseExpectation("campaign", {}),
        transport=requester,
        deadline_s=timeout_s + 1,
    )
    return guard_m, guard, requester, responder, control, remote, key


def test_the_guard_and_the_channel_bind_the_same_fact_set():
    _, producers = product()
    channel = importlib.import_module("curie_worker.commissioning_observation_channel")
    assert producers.OBSERVATION_FACTS == channel.OBSERVATION_FACTS


def test_each_guard_call_needs_a_fresh_signed_provisioner_reobservation():
    store = Store()
    _, guard, requester, responder, control, remote, _ = compose(store)

    async def body():
        for _ in range(3):
            await guard.require_current()

    asyncio.run(serving(responder, body))
    assert requester.calls == 3 and len(responder.answered) == 3
    assert all(p.calls == 3 for p in (*control.values(), *remote.values()))
    assert len(guard.latencies_ms) == 3 and store.objects == {}


@pytest.mark.parametrize("fact", ["network", "cni", "installation", "retained_peer"])
def test_provisioner_side_drift_is_never_signed_and_the_guard_refuses(fact):
    store = Store()
    guard_m, guard, _, responder, _, _, _ = compose(
        store, provisioner={fact: lambda n: OTHER}, timeout_s=0.3
    )

    async def body():
        with pytest.raises(guard_m.OperatorGuardRefused):
            await guard.require_current()

    channel = importlib.import_module("curie_worker.commissioning_observation_channel")
    with pytest.raises(channel.ObservationChannelRefused):  # the responder fences on drift
        asyncio.run(serving(responder, body))
    assert responder.answered == []


def test_a_stale_reply_from_an_earlier_call_refuses():
    store = Store()
    guard_m, guard, requester, responder, _, _, _ = compose(store, timeout_s=0.5)

    async def first():
        await guard.require_current()

    asyncio.run(serving(responder, first))
    stale = next(o for o in store.created if o["metadata"]["name"].startswith("observation-reply-"))

    def replay(request):
        response = store.handle(request)
        if request.method == "POST" and urlsplit(str(request.url)).path.endswith("/configmaps"):
            name = json.loads(request.content)["metadata"]["name"]
            nonce = name.removeprefix("observation-request-")
            planted = json.loads(json.dumps(stale))
            planted["metadata"]["name"] = "observation-reply-" + nonce
            store.objects[planted["metadata"]["name"]] = planted
        return response

    requester._api = api(store, replay)

    with pytest.raises(guard_m.OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert requester.calls == 1 and len(guard.latencies_ms) == 1


def test_an_unsigned_reply_refuses():
    store = Store()
    guard_m, guard, requester, _, _, _, _ = compose(store, timeout_s=0.5)

    def forge(request):
        response = store.handle(request)
        if request.method == "POST":
            value = json.loads(request.content)
            nonce = value["metadata"]["name"].removeprefix("observation-request-")
            body = json.loads(value["data"]["request"]) | {"request_uid": "x"}
            store.objects["observation-reply-" + nonce] = {
                "metadata": {"name": "observation-reply-" + nonce, "uid": "u"},
                "immutable": True,
                "data": {"observation": json.dumps(body), "signature": ""},
            }
        return response

    requester._api = api(store, forge)
    with pytest.raises(guard_m.OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert requester.calls == 0


# These deadline regressions use genuine observer ACL replies from the owned
# TLS broker. Fact digests remain the existing channel plumbing placeholders.
from .test_commissioning_worker_entry import _name, pworker_service  # noqa: E402

_acl_broker = importlib.import_module(_name + ".test_commissioning_acl_observer_identity")
installed = _acl_broker.installed
__all__ = ["installed", "pworker_service"]


@pytest.mark.parametrize("slow_acl", [False, True, "suppresses"])
def test_acl_stage_budget_is_distinct_from_aggregate_guard_budget(
    pworker_service, installed, monkeypatch, slow_acl
):
    from curie_protected_hooks.commissioning_producers import AclPrincipalStateProducer
    from curie_worker import commissioning_control_process as process

    guard_m, producers = product()
    acl, client = _acl_broker._producer(pworker_service, installed, "observer")
    facts = DIGESTS | {"bootstrap": acl.baseline.bootstrap_digest}
    reader = _acl_broker.BrokerReader(client)

    class ScheduledReader:
        async def getuser(self, username):
            try:
                await asyncio.sleep(0.2 if slow_acl else 0.02)
            except asyncio.CancelledError:
                if slow_acl != "suppresses":
                    raise
            return await reader.getuser(username)

        async def key_read_denied(self, key):
            await asyncio.sleep(0.02)
            return await reader.key_read_denied(key)

    class ScheduledFact(Fact):
        async def observe(self):
            await asyncio.sleep(0.06)
            return await super().observe()

    store = Store()
    requester, responder, _, _ = pair(store, observed=None, timeout_s=1)
    responder._facts = facts
    remote = producers.ReobservationSet(tuple(Fact(name, facts) for name in facts))
    responder._reobserve = remote.reobserve
    monkeypatch.setitem(process.PRODUCER_CONSTANTS, "per_call_deadline_ms", 100)
    guard = guard_m.ChannelCampaignGuard(
        premeasured=facts,
        control=producers.ReobservationSet(
            tuple(process.BoundedProducer(ScheduledFact(name, facts)) for name in facts)
        ),
        acl=AclPrincipalStateProducer(
            reader=ScheduledReader(),
            baseline=acl.baseline,
            observer_username=installed["observer"].username,
            provisioner_username=installed["provisioner"].username,
        ),
        phase=lambda: producers.PhaseExpectation("campaign", {}),
        transport=requester,
        deadline_s=1,
        acl_deadline_s=0.1,
    )

    async def body():
        if slow_acl:
            with pytest.raises(guard_m.OperatorGuardRefused):
                await guard.require_current()
            with pytest.raises(guard_m.OperatorGuardRefused):
                await guard.require_current()
            assert requester.calls == 0
        else:
            await guard.require_current()
            assert guard.latencies_ms[0] > 100
            assert requester.calls == 1

    try:
        asyncio.run(serving(responder, body))
    finally:
        client.close()
