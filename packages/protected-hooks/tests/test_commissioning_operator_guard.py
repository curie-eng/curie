"""Per-call commissioning guard over producer Protocols, ADR 0236.

@spec PROTECTED-COMMISSIONING-4 step 5, @spec PROTECTED-COMMISSIONING-5 and
design decision 7. Only fakes implement the producers and the channel here.
These tests prove refusal and no-caching only; nothing here is campaign evidence.
"""

from __future__ import annotations

import asyncio
import hashlib

import pytest
from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
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
from curie_protected_hooks.commissioning_provisioner import BROKER_PRINCIPALS

from .test_commissioning_bootstrap import bootstrap_value, encoded

B = parse_commissioning_bootstrap(encoded(bootstrap_value()))
DIGESTS = {name: hashlib.sha256(name.encode()).hexdigest() for name in OBSERVATION_FACTS}
DIGESTS["bootstrap"] = B.digest
OTHER = hashlib.sha256(b"drifted").hexdigest()
_b = B.as_dict()
USERS = {
    **{name: ref["id"] for name, ref in _b["production_credentials"].items()},
    "issuer": _b["issuer_identity"]["id"],
    "control": _b["control_identity"]["id"],
    "candidate_enqueue": _b["candidate_credentials"]["enqueue"]["id"],
    "candidate_worker": _b["candidate_credentials"]["worker"]["id"],
}
OBSERVER, PROVISIONER = "commissioning/example-acl-observer", "commissioning/example-provisioner"


def hashes(name: str) -> frozenset[str]:
    return frozenset({hashlib.sha256(("credential-" + name).encode()).hexdigest()})


STATES = {name: AclPrincipalState(hashes(name), True) for name in ACL_PRINCIPALS}
BASELINE = AclBaseline(B, STATES)
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


def observer(reader, *, baseline=BASELINE, name=OBSERVER, provisioner=PROVISIONER):
    return AclPrincipalStateProducer(
        reader=reader, baseline=baseline, observer_username=name, provisioner_username=provisioner
    )


def build(*, values=None, reader=None, transport=None, phase=None, deadline_s=5.0):
    producers = {name: Fact(name, (values or {}).get(name)) for name in OBSERVATION_FACTS}
    reader = reader or Reader()
    transport = transport or Transport()
    guard = ChannelCampaignGuard(
        premeasured=DIGESTS,
        control=ReobservationSet(tuple(producers.values())),
        acl=observer(reader),
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

    guard, producers, _, transport = build(transport=Slow(), deadline_s=0.05)
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    transport.__class__ = Transport  # the channel would answer now; the guard stays fenced
    with pytest.raises(OperatorGuardRefused):
        asyncio.run(guard.require_current())
    assert producers["permit"].calls == 1 and guard.calls == 0


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
        AclBaseline(B, {name: STATES[name] for name in ACL_PRINCIPALS[1:]})
    with pytest.raises(ProducerRefused):
        AclBaseline(B, {**STATES, "provisioner": STATES["worker"]})
    with pytest.raises(ProducerRefused):
        AclBaseline(bootstrap_value(), STATES)  # type: ignore[arg-type]
    with pytest.raises(ProducerRefused):
        PhaseExpectation("activated", {"provisioner": False})
    with pytest.raises(OperatorGuardRefused):
        ChannelCampaignGuard(
            premeasured={**DIGESTS, "extra": OTHER},
            control=ReobservationSet(tuple(good.values())),
            acl=observer(Reader()),
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


def test_the_acl_principals_are_the_provisioners_broker_principals():
    # One tuple: a principal added to G cannot be skipped by the state check.
    assert ACL_PRINCIPALS is BROKER_PRINCIPALS


def test_the_baseline_usernames_are_bs_credential_references():
    assert {name: row[0] for name, row in BASELINE.principals.items()} == USERS
    assert BASELINE.bootstrap_digest == B.digest


def test_a_baseline_for_another_installation_cannot_redirect_the_check():
    other = bootstrap_value()
    other["runtime_generation"] = str(int(other["runtime_generation"]) + 1)
    foreign = AclBaseline(parse_commissioning_bootstrap(encoded(other)), STATES)
    with pytest.raises(OperatorGuardRefused):
        ChannelCampaignGuard(
            premeasured=DIGESTS,
            control=ReobservationSet(tuple(Fact(n) for n in OBSERVATION_FACTS)),
            acl=observer(Reader(), baseline=foreign),
            phase=lambda: CAMPAIGN,
            transport=Transport(),
        )


@pytest.mark.parametrize(
    "name, provisioner",
    [
        *((USERS[p], PROVISIONER) for p in ACL_PRINCIPALS),
        *((OBSERVER, USERS[p]) for p in ACL_PRINCIPALS),
        (OBSERVER, OBSERVER),
        ("", PROVISIONER),
        (OBSERVER, ""),
        (OBSERVER.encode(), PROVISIONER),
    ],
)
def test_the_observer_must_be_its_own_principal(name, provisioner):
    # The checked parties never read their own evidence (design decision 7).
    with pytest.raises(ProducerRefused):
        observer(Reader(), name=name, provisioner=provisioner)


def test_resp3_flags_as_a_set_parse_like_a_list():
    # Valkey replies with ``flags`` as a RESP3 set; ``passwords`` stays an array.
    expected = AclPrincipalState(frozenset({"a" * 64}), False)
    for flags in ({b"off"}, frozenset({"off", "allchannels"}), [b"off"]):
        assert parse_getuser_reply({"flags": flags, "passwords": ["a" * 64]}) == expected
    with pytest.raises(ProducerRefused):
        parse_getuser_reply({"flags": ["on"], "passwords": {"a" * 64}})
