"""Per-call commissioning guard over producer Protocols, ADR 0236.

@spec PROTECTED-COMMISSIONING-4 step 5, @spec PROTECTED-COMMISSIONING-5 and
design decision 7. Only fakes implement the producers and the channel here.
These tests prove refusal and no-caching only; nothing here is campaign evidence.
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from curie_protected_hooks.commissioning_operator_guard import (
    ChannelCampaignGuard,
    OperatorGuardRefused,
)
from curie_protected_hooks.commissioning_producers import (
    ACL_PRINCIPALS,
    OBSERVATION_FACTS,
    AclBaseline,
    AclPrincipalState,
    AclPrincipalStateProducer,
    PhaseExpectation,
    ProducerRefused,
    ReobservationSet,
    parse_getuser_reply,
)

DIGESTS = {name: hashlib.sha256(name.encode()).hexdigest() for name in OBSERVATION_FACTS}
OTHER = hashlib.sha256(b"drifted").hexdigest()
USERS = {name: "user-" + name.replace("_", "-") for name in ACL_PRINCIPALS}


def hashes(name: str) -> frozenset[str]:
    return frozenset({hashlib.sha256(("credential-" + name).encode()).hexdigest()})


BASELINE = AclBaseline(
    {name: (USERS[name], AclPrincipalState(hashes(name), True)) for name in ACL_PRINCIPALS}
)
CAMPAIGN = PhaseExpectation("campaign", {})


class Fact:
    """Fake producer: returns a fresh digest per call, counts calls."""

    def __init__(self, fact: str, value: object = None) -> None:
        self.fact, self.value, self.calls = fact, value, 0

    async def observe(self) -> str:
        self.calls += 1
        value = self.value
        if callable(value):
            value = value(self.calls)
        if isinstance(value, BaseException):
            raise value
        return DIGESTS[self.fact] if value is None else value  # type: ignore[return-value]


class Reader:
    """Fake read-only ACL observer: ACL GETUSER replies keyed by username."""

    def __init__(self) -> None:
        self.replies: dict[str, object] = {
            USERS[name]: {
                "flags": ["on"],
                "passwords": sorted(hashes(name)),
                "commands": "-@all",
            }
            for name in ACL_PRINCIPALS
        }
        self.calls = 0

    async def getuser(self, username: str) -> object:
        self.calls += 1
        reply = self.replies[username]
        if isinstance(reply, BaseException):
            raise reply
        return reply


class Transport:
    """Fake signed channel: records the exact digests it was asked to bind."""

    def __init__(self, refuse: bool = False) -> None:
        self.sent: list[dict[str, str]] = []
        self.refuse = refuse

    async def require(self, facts) -> None:
        self.sent.append(dict(facts))
        if self.refuse:
            raise RuntimeError("stale or foreign reply")


def build(*, values=None, reader=None, transport=None, phase=None, deadline_s=5.0):
    producers = {name: Fact(name, (values or {}).get(name)) for name in OBSERVATION_FACTS}
    reader = reader or Reader()
    transport = transport or Transport()
    guard = ChannelCampaignGuard(
        premeasured=DIGESTS,
        control=ReobservationSet(tuple(producers.values())),
        acl=AclPrincipalStateProducer(reader=reader, baseline=BASELINE),
        phase=phase or (lambda: CAMPAIGN),
        transport=transport,
        deadline_s=deadline_s,
    )
    return guard, producers, reader, transport


def test_each_call_reobserves_every_producer_acl_and_channel_without_caching():
    guard, producers, reader, transport = build()

    async def body():
        for _ in range(3):
            await guard.require_current()

    asyncio.run(body())
    assert all(p.calls == 3 for p in producers.values())
    assert reader.calls == 3 * len(ACL_PRINCIPALS)
    assert transport.sent == [DIGESTS] * 3
    assert guard.calls == 3 and len(guard.latencies_ms) == 3
    assert all(type(ms) is int and ms >= 0 for ms in guard.latencies_ms)


@pytest.mark.parametrize("fact", OBSERVATION_FACTS)
def test_every_digest_mismatch_refuses_before_the_channel(fact):
    guard, _, _, transport = build(values={fact: OTHER})
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert transport.sent == []


@pytest.mark.parametrize("fact", OBSERVATION_FACTS)
def test_drift_on_a_later_call_refuses_and_fences(fact):
    guard, _, _, transport = build(values={fact: lambda n: DIGESTS[fact] if n == 1 else OTHER})

    async def body():
        await guard.require_current()
        with pytest.raises(OperatorGuardRefused):
            await guard.require_current()
        with pytest.raises(OperatorGuardRefused):
            await guard.require_current()  # fenced, no reobservation

    asyncio.run(body())
    assert len(transport.sent) == 1


@pytest.mark.parametrize(
    "value",
    ["", "A" * 64, "0" * 63, b"0" * 64, 1.5, 1, ProducerRefused(), RuntimeError("x")],
    ids=["empty", "upper", "short", "bytes", "float", "int", "refused", "error"],
)
def test_a_malformed_or_unavailable_producer_observation_refuses(value):
    guard, _, _, transport = build(values={"cni": (lambda _n: value)})
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert transport.sent == []


def test_a_refused_or_stale_channel_reply_refuses_and_fences():
    guard, producers, _, transport = build(transport=Transport(refuse=True))

    async def body():
        with pytest.raises(OperatorGuardRefused):
            await guard.require_current()
        transport.refuse = False
        with pytest.raises(OperatorGuardRefused):
            await guard.require_current()

    asyncio.run(body())
    assert len(transport.sent) == 1 and producers["permit"].calls == 1


def test_a_timed_out_call_refuses():
    class Slow(Transport):
        async def require(self, facts) -> None:
            await asyncio.sleep(1)

    guard, _, _, _ = build(transport=Slow(), deadline_s=0.05)
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())


def _state(reader: Reader, name: str, **change) -> None:
    reply = dict(reader.replies[USERS[name]])  # type: ignore[call-overload]
    reply.update(change)
    reader.replies[USERS[name]] = reply


@pytest.mark.parametrize("name", ACL_PRINCIPALS)
@pytest.mark.parametrize(
    "change",
    [
        {"passwords": []},
        {"passwords": ["f" * 64]},
        "added",
        {"flags": ["off"]},
        {"flags": []},
        {"flags": ["on", "off"]},
    ],
    ids=["no-password", "rotated", "extra-password", "disabled", "no-flag", "both-flags"],
)
def test_acl_state_drift_refuses_for_every_principal(name, change):
    reader = Reader()
    if change == "added":
        _state(reader, name, passwords=sorted(hashes(name) | {"e" * 64}))
    else:
        _state(reader, name, **change)
    guard, _, _, transport = build(reader=reader)
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert transport.sent == []


@pytest.mark.parametrize("name", ACL_PRINCIPALS)
@pytest.mark.parametrize(
    "reply",
    [None, RuntimeError("NOPERM"), {"flags": ["on"]}, {"passwords": []}, "on", [b"flags"]],
    ids=["absent-user", "error", "no-passwords", "no-flags", "text", "odd-pairs"],
)
def test_an_unavailable_acl_observation_refuses(name, reply):
    reader = Reader()
    reader.replies[USERS[name]] = reply
    guard, _, _, transport = build(reader=reader)
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert transport.sent == []


def test_the_phase_expected_enabled_state_is_checked_each_call():
    activated = PhaseExpectation(
        "activated", {"candidate_enqueue": False, "candidate_worker": False}
    )
    phases = [CAMPAIGN, activated, activated]
    reader = Reader()
    guard, _, _, transport = build(reader=reader, phase=lambda: phases[guard.calls])

    async def body():
        await guard.require_current()  # campaign: baseline state
        # Activation step 2 disabled the candidates; enabled state follows the phase.
        _state(reader, "candidate_enqueue", flags=["off"])
        _state(reader, "candidate_worker", flags=["off"])
        await guard.require_current()
        # A candidate re-enabled after activation is drift.
        _state(reader, "candidate_worker", flags=["on"])
        with pytest.raises(OperatorGuardRefused):
            await guard.require_current()

    asyncio.run(body())
    assert len(transport.sent) == 2


def test_still_enabled_candidates_refuse_once_the_phase_expects_them_off():
    activated = PhaseExpectation("activated", {"candidate_worker": False})
    guard, _, _, transport = build(phase=lambda: activated)
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert transport.sent == []


def test_resp2_and_resp3_getuser_replies_parse_to_the_same_state():
    resp3 = {b"flags": [b"on", b"sanitize-payload"], b"passwords": [b"a" * 64], b"keys": b""}
    resp2 = [b"flags", [b"on", b"sanitize-payload"], b"passwords", [b"a" * 64], b"keys", b""]
    expected = AclPrincipalState(frozenset({"a" * 64}), True)
    assert parse_getuser_reply(resp3) == parse_getuser_reply(resp2) == expected


@pytest.mark.parametrize(
    "reply",
    [
        {"flags": ["on"], "passwords": ["A" * 64]},
        {"flags": ["on"], "passwords": ["a" * 64, "a" * 64]},
        {"flags": ["on"], "passwords": "a" * 64},
        {"flags": "on", "passwords": []},
        [b"flags", [b"on"], b"flags", [b"off"], b"passwords", []],
    ],
    ids=["upper-hash", "duplicate-hash", "hash-text", "flags-text", "repeated"],
)
def test_malformed_getuser_replies_refuse(reply):
    with pytest.raises(ProducerRefused):
        parse_getuser_reply(reply)


def test_construction_refuses_an_incomplete_or_foreign_binding():
    good = {name: Fact(name) for name in OBSERVATION_FACTS}
    with pytest.raises(ProducerRefused):
        ReobservationSet(tuple(p for n, p in good.items() if n != "artifact"))
    with pytest.raises(ProducerRefused):
        ReobservationSet((*good.values(), Fact("cni")))
    with pytest.raises(ProducerRefused):
        AclBaseline({name: BASELINE.principals[name] for name in ACL_PRINCIPALS[1:]})
    with pytest.raises(ProducerRefused):
        AclBaseline(
            {
                name: (USERS["worker"], AclPrincipalState(hashes(name), True))
                for name in ACL_PRINCIPALS
            }
        )
    with pytest.raises(ProducerRefused):
        PhaseExpectation("activated", {"provisioner": False})
    with pytest.raises(OperatorGuardRefused):
        ChannelCampaignGuard(
            premeasured={**DIGESTS, "extra": OTHER},
            control=ReobservationSet(tuple(good.values())),
            acl=AclPrincipalStateProducer(reader=Reader(), baseline=BASELINE),
            phase=lambda: CAMPAIGN,
            transport=Transport(),
        )


def test_a_cancelled_call_fences_and_propagates():
    class Blocked(Transport):
        async def require(self, facts) -> None:
            await asyncio.Event().wait()

    guard, _, _, _ = build(transport=Blocked())

    async def body():
        task = asyncio.create_task(guard.require_current())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(OperatorGuardRefused):
            await guard.require_current()

    asyncio.run(body())


@pytest.mark.parametrize(
    "value", ["A" * 64, "0" * 63, b"0" * 64, 1], ids=["upper", "short", "bytes", "int"]
)
def test_the_reobservation_set_itself_refuses_a_malformed_digest(value):
    # The provisioner uses the set directly as the channel responder's reobserve.
    producers = tuple(Fact(n, value if n == "artifact" else None) for n in OBSERVATION_FACTS)
    with pytest.raises(ProducerRefused):
        asyncio.run(ReobservationSet(producers).reobserve())
