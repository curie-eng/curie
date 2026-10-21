"""Commissioning producer Protocols and the reobserved fact set, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. A producer returns a fresh SHA-256 digest of
one fact on every call; ``ReobservationSet`` runs exactly one producer per fact
of ADR 0236's list and never reuses a result. The same set serves the control
process (the digests it asks the channel to bind) and the provisioner (the
``reobserve`` callable of the channel responder).

The network, hostile-peer and retained-peer facts are the per-channel digests
of a behavioral network producer's observation entries. For a managed CNI the
probe kit's ``BehavioralNetworkProducer`` and ``channel_digest`` define them;
this module adds no second network measurement interface. A measurement with a
refusal or any entry that is not ``pass`` has no digest: its fact producer
raises.

Design decision 7: the ACL principal state is observed, not hashed. The
read-only ACL observer reads each commissioning and production principal's
password-hash set and enabled state with ``ACL GETUSER``. Every reobservation
compares them with the administrator-recorded installation baseline and with
the enabled state the current campaign phase expects. Any difference or
unavailable observation refuses.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import NoReturn, Protocol

# ADR 0236, decision step 1. The signed channel binds the same tuple.
OBSERVATION_FACTS = (
    "permit",
    "manifest",
    "bootstrap",
    "control_profile",
    "projection",
    "primitive_completions",
    "network",
    "hostile_peer",
    "retained_peer",
    "installation",
    "cni",
    "artifact",
)
# Design decision 7: every commissioning and production broker principal. The
# provisioner (the checked party) and the observer (the checker) are not in it.
ACL_PRINCIPALS = (
    "issuer",
    "control",
    "candidate_enqueue",
    "candidate_worker",
    "enqueue",
    "worker",
    "verifier",
)
_DIGEST = re.compile(r"[0-9a-f]{64}")
_USERNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_PHASE = re.compile(r"[a-z][a-z0-9-]{0,62}")


class ProducerRefused(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Commissioning producer refused")


def _refuse() -> NoReturn:
    raise ProducerRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


def _digest(value: object) -> str:
    _require(type(value) is str and _DIGEST.fullmatch(str(value)) is not None)
    return str(value)


class FactProducer(Protocol):
    """One ADR 0236 fact, freshly observed on every call."""

    @property
    def fact(self) -> str: ...

    async def observe(self) -> str:
        """Lowercase SHA-256 hex of the current observation; raises if unavailable."""
        ...


class ReobservationSet:
    """Exactly one producer per fact. Each call observes every fact again."""

    def __init__(self, producers: Sequence[FactProducer]) -> None:
        _require(type(producers) is tuple)
        facts = [producer.fact for producer in producers]
        _require(sorted(facts) == sorted(OBSERVATION_FACTS))
        self._producers = {producer.fact: producer for producer in producers}

    async def reobserve(self) -> dict[str, str]:
        try:
            values = await asyncio.gather(
                *(self._producers[name].observe() for name in OBSERVATION_FACTS)
            )
            return {
                name: _digest(value) for name, value in zip(OBSERVATION_FACTS, values, strict=True)
            }
        except ProducerRefused:
            raise
        except Exception:  # noqa: BLE001 -- an unavailable observation is a refusal.
            _refuse()


@dataclass(frozen=True, slots=True)
class AclPrincipalState:
    """What ``ACL GETUSER`` shows for one principal: password hashes and ``on``/``off``."""

    password_hashes: frozenset[str]
    enabled: bool

    def __post_init__(self) -> None:
        _require(type(self.password_hashes) is frozenset and type(self.enabled) is bool)
        for item in self.password_hashes:
            _digest(item)

    def __repr__(self) -> str:
        return f"AclPrincipalState(<{len(self.password_hashes)} hashes>, {self.enabled})"


class AclBaseline:
    """The administrator-recorded installation baseline: username and state per principal.

    The installation bootstrap records it before any campaign and only that step
    updates it; the provisioner never writes it.
    """

    def __init__(self, principals: Mapping[str, tuple[str, AclPrincipalState]]) -> None:
        _require(isinstance(principals, Mapping) and set(principals) == set(ACL_PRINCIPALS))
        rows = {}
        for name in ACL_PRINCIPALS:
            row = principals[name]
            _require(type(row) is tuple and len(row) == 2)
            username, state = row
            _require(type(username) is str and _USERNAME.fullmatch(username) is not None)
            _require(type(state) is AclPrincipalState)
            rows[name] = (username, state)
        _require(len({username for username, _ in rows.values()}) == len(rows))
        self.principals: Mapping[str, tuple[str, AclPrincipalState]] = rows


class PhaseExpectation:
    """Enabled state the current campaign phase changes from the baseline.

    Principals the phase does not name keep their baseline state; for example,
    after activation step 2 the candidate principals are expected disabled.
    """

    def __init__(self, name: str, enabled: Mapping[str, bool]) -> None:
        _require(type(name) is str and _PHASE.fullmatch(name) is not None)
        _require(isinstance(enabled, Mapping) and set(enabled) <= set(ACL_PRINCIPALS))
        _require(all(type(value) is bool for value in enabled.values()))
        self.name, self.enabled = name, dict(enabled)


class AclUserReader(Protocol):
    """The read-only ACL observer principal: ``ACL GETUSER`` and nothing else."""

    async def getuser(self, username: str) -> object: ...


def _text(value: object) -> str:
    if type(value) is bytes:
        value = value.decode("ascii")
    _require(type(value) is str)
    return str(value)


def parse_getuser_reply(reply: object) -> AclPrincipalState:
    """Closed reading of a RESP2 (flat pairs) or RESP3 (map) ``ACL GETUSER`` reply."""
    try:
        if type(reply) is list:
            _require(len(reply) % 2 == 0)
            pairs = [(_text(reply[i]), reply[i + 1]) for i in range(0, len(reply), 2)]
        else:
            _require(type(reply) is dict)
            assert isinstance(reply, dict)
            pairs = [(_text(key), value) for key, value in reply.items()]
        fields = dict(pairs)
        _require(len(fields) == len(pairs) and {"flags", "passwords"} <= fields.keys())
        flags, passwords = fields["flags"], fields["passwords"]
        _require(type(flags) is list and type(passwords) is list)
        assert isinstance(flags, list) and isinstance(passwords, list)
        names = {_text(flag) for flag in flags}
        _require(("on" in names) != ("off" in names))
        hashes = [_digest(_text(item)) for item in passwords]
        _require(len(set(hashes)) == len(hashes))
        return AclPrincipalState(frozenset(hashes), "on" in names)
    except ProducerRefused:
        raise
    except Exception:  # noqa: BLE001 -- reply diagnostics never escape.
        _refuse()


class AclPrincipalStateProducer:
    """Design decision 7 as a producer: refuses on any drift from the expected state."""

    def __init__(self, *, reader: AclUserReader, baseline: AclBaseline) -> None:
        _require(type(baseline) is AclBaseline and callable(getattr(reader, "getuser", None)))
        self._reader, self._baseline = reader, baseline

    async def require(self, phase: PhaseExpectation) -> None:
        """Observe every principal now; equal hash sets and phase-expected enabled state."""
        try:
            _require(type(phase) is PhaseExpectation)
            rows = self._baseline.principals
            replies = await asyncio.gather(
                *(self._reader.getuser(rows[name][0]) for name in ACL_PRINCIPALS)
            )
            for name, reply in zip(ACL_PRINCIPALS, replies, strict=True):
                observed = parse_getuser_reply(reply)
                recorded = rows[name][1]
                _require(observed.password_hashes == recorded.password_hashes)
                _require(observed.enabled is phase.enabled.get(name, recorded.enabled))
        except ProducerRefused:
            raise
        except Exception:  # noqa: BLE001 -- an unavailable observation is a refusal.
            _refuse()


PhaseSource = Callable[[], PhaseExpectation]
