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


def product():
    return (
        importlib.import_module("curie_protected_hooks.commissioning_operator_guard"),
        importlib.import_module("curie_protected_hooks.commissioning_producers"),
    )


class Fact:
    def __init__(self, fact, value=None):
        self.fact, self.value, self.calls = fact, value, 0

    async def observe(self):
        self.calls += 1
        return DIGESTS[self.fact] if self.value is None else self.value(self.calls)


class Reader:
    def __init__(self):
        self.reply = {"flags": ["on"], "passwords": ["a" * 64]}

    async def getuser(self, username):
        return self.reply


def compose(store, *, provisioner=None, timeout_s=2.0):
    guard_m, producers = product()
    control = {name: Fact(name) for name in producers.OBSERVATION_FACTS}
    remote = {
        name: Fact(name, (provisioner or {}).get(name)) for name in producers.OBSERVATION_FACTS
    }
    reobservation = producers.ReobservationSet(tuple(remote.values()))
    requester, responder, _, key = pair(store, observed=None, timeout_s=timeout_s)
    responder._reobserve = reobservation.reobserve
    state = producers.AclPrincipalState(frozenset({"a" * 64}), True)
    baseline = producers.AclBaseline(
        {name: ("user-" + name, state) for name in producers.ACL_PRINCIPALS}
    )
    guard = guard_m.ChannelCampaignGuard(
        premeasured=DIGESTS,
        control=producers.ReobservationSet(tuple(control.values())),
        acl=producers.AclPrincipalStateProducer(reader=Reader(), baseline=baseline),
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
