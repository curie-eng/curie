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

from .commissioning_acl import commissioning_acl_rules
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_provisioner import BROKER_PRINCIPALS

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
# Design decision 7: every commissioning and production broker principal, the
# same tuple G records rules for. The provisioner (the checked party) and the ACL
# observer (the checker) are not in it.
ACL_PRINCIPALS = BROKER_PRINCIPALS
_DIGEST = re.compile(r"[0-9a-f]{64}")
_USERNAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}")
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


def _usernames(bootstrap: CommissioningBootstrap) -> dict[str, str]:
    """Each principal's ACL username is its credential reference ID in B."""
    b = bootstrap.as_dict()
    refs = {
        **b["production_credentials"],
        "issuer": b["issuer_identity"],
        "control": b["control_identity"],
        "candidate_enqueue": b["candidate_credentials"]["enqueue"],
        "candidate_worker": b["candidate_credentials"]["worker"],
        "qualification_publisher": b["qualification_publisher_identity"],
        "control_reader": b["control_reader_identity"],
        "source_writer": b["source_writer_identity"],
    }
    return {name: refs[name]["id"] for name in ACL_PRINCIPALS}


class AclBaseline:
    """The administrator-recorded installation baseline, bound to that installation's B.

    The installation bootstrap records each principal's state before any campaign,
    and only that step updates it; the provisioner never writes it. Usernames are
    never supplied here: they are the credential references of the bootstrap B whose
    digest the guard's premeasured tuple binds, so a baseline cannot name other
    users or another installation.
    """

    def __init__(
        self, bootstrap: CommissioningBootstrap, states: Mapping[str, AclPrincipalState]
    ) -> None:
        try:
            _require(type(bootstrap) is CommissioningBootstrap)
            _require(isinstance(states, Mapping) and set(states) == set(ACL_PRINCIPALS))
            _require(all(type(states[name]) is AclPrincipalState for name in ACL_PRINCIPALS))
            usernames = _usernames(bootstrap)
            _require(all(_USERNAME.fullmatch(u) is not None for u in usernames.values()))
            _require(len(set(usernames.values())) == len(usernames))
        except ProducerRefused:
            raise
        except Exception:  # noqa: BLE001
            _refuse()
        self.bootstrap_digest = bootstrap.digest
        self.principals: Mapping[str, tuple[str, AclPrincipalState]] = {
            name: (usernames[name], states[name]) for name in ACL_PRINCIPALS
        }


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
    """A broker session that claims to be the read-only ACL observer principal."""

    async def getuser(self, username: str) -> object:
        """The raw ``ACL GETUSER`` reply; raises on any refusal or error."""
        ...

    async def key_read_denied(self, key: str) -> bool:
        """``True`` only when the broker answers a ``GET`` of ``key`` with ``NOPERM``.

        ``False`` when the read succeeds; raises on anything else.
        """
        ...


# Any key: the observer recipe grants none, so its read must be refused.
OBSERVER_PROBE_KEY = "protected:control:manifest:acl-observer-probe"
# Recipe actions the broker applies but never reports back as rules.
_ACTIONS = frozenset({"resetkeys", "resetchannels", "clearselectors"})
_OBSERVER_COMMANDS = frozenset(
    token for token in commissioning_acl_rules("observer") if token not in _ACTIONS
)
# The pinned Valkey reports exactly ``on`` for the installed observer recipe;
# any other flag (``nopass``, ``allkeys``, ``allchannels``, ``off``) refuses.
_OBSERVER_FLAGS = frozenset({"on"})


def _fields(reply: object) -> dict[str, object]:
    if type(reply) is list:
        _require(len(reply) % 2 == 0)
        pairs = [(_text(reply[i]), reply[i + 1]) for i in range(0, len(reply), 2)]
    else:
        _require(type(reply) is dict)
        assert isinstance(reply, dict)
        pairs = [(_text(key), value) for key, value in reply.items()]
    fields = dict(pairs)
    _require(len(fields) == len(pairs))
    return fields


def _require_observer_rules(reply: object) -> None:
    """The session's own user reports exactly the observer recipe and nothing more."""
    fields = _fields(reply)
    _require({"flags", "commands", "keys", "channels", "selectors"} <= fields.keys())
    flags = fields["flags"]
    _require(type(flags) in (list, set, frozenset))
    assert isinstance(flags, (list, set, frozenset))
    names = {_text(flag) for flag in flags}
    _require(names == _OBSERVER_FLAGS)
    commands = _text(fields["commands"]).split(" ")
    _require(len(set(commands)) == len(commands) and set(commands) == _OBSERVER_COMMANDS)
    _require(_text(fields["keys"]) == "" and _text(fields["channels"]) == "")
    _require(type(fields["selectors"]) is list and fields["selectors"] == [])


def _text(value: object) -> str:
    if type(value) is bytes:
        value = value.decode("ascii")
    _require(type(value) is str)
    return str(value)


def parse_getuser_reply(reply: object) -> AclPrincipalState:
    """Closed reading of a RESP2 (flat pairs) or RESP3 (map) ``ACL GETUSER`` reply."""
    try:
        fields = _fields(reply)
        _require({"flags", "passwords"} <= fields.keys())
        flags, passwords = fields["flags"], fields["passwords"]
        # RESP3 sends ``flags`` as a set and ``passwords`` as an array.
        _require(type(flags) in (list, set, frozenset) and type(passwords) is list)
        assert isinstance(flags, (list, set, frozenset)) and isinstance(passwords, list)
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

    def __init__(
        self,
        *,
        reader: AclUserReader,
        baseline: AclBaseline,
        observer_username: str,
        provisioner_username: str,
    ) -> None:
        _require(type(baseline) is AclBaseline and callable(getattr(reader, "getuser", None)))
        checked = {username for username, _ in baseline.principals.values()}
        for name in (observer_username, provisioner_username):
            _require(type(name) is str and _USERNAME.fullmatch(name) is not None)
            _require(name not in checked)
        # The checked parties never read or report their own evidence.
        _require(observer_username != provisioner_username)
        self._reader, self._baseline = reader, baseline
        self.observer_username = observer_username

    @property
    def baseline(self) -> AclBaseline:
        return self._baseline

    async def require(self, phase: PhaseExpectation) -> None:
        """Observe every principal now; equal hash sets and phase-expected enabled state."""
        try:
            _require(type(phase) is PhaseExpectation)
            await self._require_observer()
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

    async def _require_observer(self) -> None:
        """The session reading the evidence is the observer, without ``ACL WHOAMI``.

        Design decision 7 grants the observer ``ACL GETUSER`` only, so identity
        is established from what that session can and cannot do. Under the
        merged recipes only the observer holds ``acl|getuser``: every other
        principal, the provisioner included, is refused ``GETUSER``, so a
        successful read already excludes them. A default or administrative user
        also holds it, but can read keys: the probe read must be refused. Last,
        the bound observer user must report exactly the observer recipe.
        """
        _require_observer_rules(await self._reader.getuser(self.observer_username))
        _require((await self._reader.key_read_denied(OBSERVER_PROBE_KEY)) is True)


PhaseSource = Callable[[], PhaseExpectation]
