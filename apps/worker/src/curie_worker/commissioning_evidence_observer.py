"""Provisioner-only retained observations, @spec COMMISSIONING-SERIES-3/6/8.

Reads are neither causal witnesses nor cleanup, acceptance or dispatch authority.
Original campaign epochs survive historical reads; actual live epochs are pinned
separately. Session roster composition and final observer teardown remain outside
this component.
"""

from __future__ import annotations

import asyncio
import copy
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.commissioning_evidence_records import (
    EvidenceInventory,
    RetainedCampaignState,
    decode_retained_state,
    retained_keys,
)
from curie_protected_hooks.commissioning_journal import CommissioningSession
from curie_protected_hooks.commissioning_records import CommissioningPermit
from curie_protected_hooks.commissioning_series_bootstrap import SeriesBootstrap
from curie_protected_hooks.commissioning_series_guard import verify_series_tuple
from curie_protected_hooks.worker_transport import WorkerCredential

from .managed_cni_probe_kit import PRODUCER_CONSTANTS

if TYPE_CHECKING:
    from .commissioning_registered_observer import _OperatorRegistration

_DEADLINE = PRODUCER_CONSTANTS["per_call_deadline_ms"] / 1000
_MAX_INT = 9223372036854775807


class EvidenceObservationRefused(ValueError):
    """One redacted refusal for an unusable observation."""

    def __init__(self) -> None:
        super().__init__("retained evidence observation refused")


def _require(condition: bool) -> None:
    if not condition:
        raise EvidenceObservationRefused()


@dataclass(frozen=True, slots=True, repr=False)
class SeriesEvidenceBinding:
    """Exact B2/G2/M inputs, not installation measurement or authority."""

    bootstrap: SeriesBootstrap
    guard_config: bytes
    manifest: Manifest

    def __post_init__(self) -> None:
        try:
            verify_series_tuple(self.bootstrap, self.guard_config, self.manifest)
        except (ValueError, TypeError, RecursionError, OverflowError):
            raise EvidenceObservationRefused() from None

    def __repr__(self) -> str:
        return "SeriesEvidenceBinding()"


def _permit(binding: SeriesEvidenceBinding, permit: CommissioningPermit) -> None:
    _require(type(permit) is CommissioningPermit)
    b, m, p = binding.bootstrap.as_dict(), binding.manifest.as_dict(), permit.as_dict()
    for key in ("runtime_id", "runtime_generation", "execution_config_digest"):
        _require(p[key] == m[key] == b[key])
    for key in ("broker_identity", "guard_identity"):
        _require(p[key] == m[key])
    for key in ("issuer_identity", "control_identity", "candidate_credentials"):
        _require(p[key] == b[key])
    _require(p["manifest_digest"] == binding.manifest.digest)
    _require(p["bootstrap_digest"] == binding.bootstrap.digest)
    _require(all(source in b["source_scope"] for source in p["sources"]))
    _require(set(p["targets"]) <= set(b["target_scope"]))
    _require(0 < int(p["expires_at_ms"]) - int(p["issued_at_ms"]) <= int(b["max_permit_ms"]))


@dataclass(frozen=True, slots=True, repr=False)
class BrokerClient:
    """Observed CLIENT LIST id/user only, not a registry or join receipt."""

    client_id: str
    username: str

    def __repr__(self) -> str:
        return "BrokerClient()"


def _positive(value: Any) -> str:
    _require(type(value) is str and re.fullmatch(r"[1-9][0-9]{0,18}", value) is not None)
    _require(int(value) <= _MAX_INT)
    return str(value)


def _clients(raw: Any) -> tuple[BrokerClient, ...]:
    # Actual id/user fields: https://valkey.io/commands/client-list/
    _require(type(raw) is str and 0 < len(raw) <= 1048576)
    rows = raw.splitlines()
    _require(0 < len(rows) <= 4096)
    result = []
    seen: set[str] = set()
    for line in rows:
        _require(0 < len(line) <= 4096)
        fields = {}
        for item in line.split(" "):
            _require("=" in item)
            key, value = item.split("=", 1)
            _require(bool(key) and key not in fields)
            fields[key] = value
        identifier = _positive(fields.get("id"))
        username = fields.get("user")
        _require(type(username) is str and 0 < len(username) <= 1024)
        _require(identifier not in seen)
        seen.add(identifier)
        result.append(BrokerClient(identifier, str(username)))
    return tuple(result)


def _hello(raw: Any) -> str:
    # HELLO2's RESP2 alternating array, id and proto are documented here:
    # https://valkey.io/commands/hello/ ; verified on actual pinned Valkey.
    _require(type(raw) is list and 0 < len(raw) <= 64 and len(raw) % 2 == 0)
    fields = {}
    for key, value in zip(raw[::2], raw[1::2], strict=True):
        _require(type(key) is str and key not in fields)
        fields[key] = value
    _require(type(fields.get("proto")) is int and fields["proto"] == 2)
    _require(type(fields.get("id")) is int)
    return _positive(str(fields["id"]))


@dataclass(frozen=True, slots=True, repr=False)
class ClientObservation:
    """Immutable presence and time facts; absence alone is not cleanup proof."""

    _identity: bytes
    clients: tuple[BrokerClient, ...]
    broker_time_ms: int

    @property
    def actual_broker_identity(self) -> dict[str, Any]:
        import json

        result: dict[str, Any] = json.loads(self._identity)
        return result

    def __repr__(self) -> str:
        return "ClientObservation()"


@dataclass(frozen=True, slots=True, repr=False)
class EvidenceObservation(ClientObservation):
    """Retained state plus actual observations, never a passing receipt."""

    state: RetainedCampaignState

    def __repr__(self) -> str:
        return "EvidenceObservation()"


class RetainedEvidenceObserver:
    """One private read-only session; any failure permanently closes it."""

    __slots__ = (
        "__binding",
        "__session",
        "__epoch",
        "__client",
        "__last_time",
        "__lock",
        "__closed",
        "__registration",
    )
    __binding: SeriesEvidenceBinding
    __session: CommissioningSession
    __epoch: str
    __client: str
    __last_time: int
    __lock: asyncio.Lock
    __closed: bool
    __registration: _OperatorRegistration | None

    def __new__(cls) -> Self:
        raise EvidenceObservationRefused()

    def __repr__(self) -> str:
        return "RetainedEvidenceObserver()"

    @property
    def client_id(self) -> str:
        return self.__client

    @classmethod
    async def connect(
        cls,
        binding: SeriesEvidenceBinding,
        credential: WorkerCredential,
        ca_pem: str,
        *,
        _registration: _OperatorRegistration | None = None,
    ) -> Self:
        session = None
        expiry = None
        deadline = asyncio.get_running_loop().time() + _DEADLINE
        try:
            if _registration is not None:
                from .commissioning_registered_observer import _OperatorRegistration

                _require(type(_registration) is _OperatorRegistration)
                deadline = min(deadline, asyncio.get_running_loop().time() + _registration.budget())
            async with asyncio.timeout_at(deadline):
                _require(type(binding) is SeriesEvidenceBinding)
                binding.__post_init__()
                session = await CommissioningSession.connect(
                    binding.manifest,
                    credential,
                    ca_pem,
                    principal=binding.bootstrap.as_dict()["evidence_observer_identity"],
                    terminal=True,
                    deadline=deadline,
                )
                if _registration is not None:
                    _registration.session = session
                expiry = asyncio.get_running_loop().call_at(
                    deadline, _registration.abort if _registration is not None else session.abort
                )
                observer = object.__new__(cls)
                observer.__binding = binding
                observer.__session = session
                observer.__epoch = await observer.__read_epoch()
                observer.__client = _hello(await session.execute("HELLO", 2))
                observer.__last_time = -1
                observer.__lock = asyncio.Lock()
                observer.__closed = False
                observer.__registration = _registration
                if _registration is not None:
                    await _registration.bind_and_measure(
                        session, observer.__epoch, observer.__client
                    )
                await observer.__presence()
                return observer
        except BaseException as error:  # noqa: BLE001  Suppress credential/transport detail.
            if session is not None:
                await session.close(nowait=True)
            if not isinstance(error, Exception):
                raise
            raise EvidenceObservationRefused() from None
        finally:
            if expiry is not None:
                expiry.cancel()

    @asynccontextmanager
    async def __bounded(self) -> AsyncIterator[None]:
        deadline = asyncio.get_running_loop().time() + _DEADLINE
        if self.__registration is not None:
            deadline = min(
                deadline, asyncio.get_running_loop().time() + self.__registration.budget()
            )
        expiry = asyncio.get_running_loop().call_at(
            deadline,
            self.__registration.abort if self.__registration is not None else self.__session.abort,
        )
        try:
            if self.__registration is not None:
                self.__registration.check_active()
            async with asyncio.timeout_at(deadline):
                yield
                if self.__registration is not None:
                    self.__registration.check_active()
        finally:
            expiry.cancel()

    async def __read_epoch(self) -> str:
        info = await self.__session.execute("INFO", "server")
        _require(type(info) is dict)
        value = info.get("run_id")
        _require(type(value) is str and re.fullmatch(r"[0-9a-f]{40}", value) is not None)
        return str(value)

    async def __presence(self) -> ClientObservation:
        import json

        _require(await self.__read_epoch() == self.__epoch)
        clients = _clients(await self.__session.execute("CLIENT", "LIST"))
        principal = self.__binding.bootstrap.as_dict()["evidence_observer_identity"]["id"]
        _require(any(c.client_id == self.__client and c.username == principal for c in clients))
        clock = await self.__session.execute("TIME")
        _require(type(clock) is tuple and len(clock) == 2 and all(type(v) is int for v in clock))
        seconds, micros = clock
        _require(0 <= seconds <= _MAX_INT // 1000 and 0 <= micros < 1000000)
        now = seconds * 1000 + micros // 1000
        _require(self.__last_time <= now <= _MAX_INT)
        _require(await self.__read_epoch() == self.__epoch)
        self.__last_time = now
        identity = copy.deepcopy(self.__binding.manifest.as_dict()["broker_identity"])
        identity["run_id"] = self.__epoch
        return ClientObservation(json.dumps(identity, sort_keys=True).encode("ascii"), clients, now)

    async def __values(self, keys: tuple[str, ...]) -> tuple[bytes | None, ...]:
        result: list[bytes | None] = []
        for key in keys:
            kind = await self.__session.execute("TYPE", key)
            _require(kind in ("string", "none"))
            value = await self.__session.execute("GET", key)
            if value is None:
                _require(kind == "none")
                result.append(None)
            else:
                _require(kind == "string" and type(value) is str and len(value) <= 16384)
                raw = value.encode("utf-8")
                _require(len(raw) <= 16384)
                result.append(raw)
        return tuple(result)

    async def clients(self) -> ClientObservation:
        """Fresh connection presence, no registry-completeness assertion."""
        try:
            async with self.__bounded(), self.__lock:
                _require(not self.__closed)
                if self.__registration is not None:
                    await self.__registration.measure()
                return await self.__presence()
        except BaseException as error:  # noqa: BLE001  Poison all failed/cancelled observations.
            await self.close()
            if not isinstance(error, Exception):
                raise
            raise EvidenceObservationRefused() from None

    async def read(
        self, permit: CommissioningPermit, inventory: EvidenceInventory
    ) -> EvidenceObservation:
        """Read twice and correlate unchanged retained bytes under one live epoch."""
        try:
            async with self.__bounded(), self.__lock:
                _require(not self.__closed)
                if self.__registration is not None:
                    await self.__registration.measure()
                _permit(self.__binding, permit)
                keys = retained_keys(permit, inventory)
                first = await self.__values(keys)
                presence = await self.__presence()
                second = await self.__values(keys)
                _require(first == second and await self.__read_epoch() == self.__epoch)
                state = decode_retained_state(permit, inventory, second)
                return EvidenceObservation(
                    presence._identity, presence.clients, presence.broker_time_ms, state
                )
        except BaseException as error:  # noqa: BLE001  Poison all failed/cancelled observations.
            await self.close()
            if not isinstance(error, Exception):
                raise
            raise EvidenceObservationRefused() from None

    async def close(self) -> None:
        """Idempotently end this session without a reconnect path."""
        self.__closed = True
        if self.__registration is not None:
            self.__registration.abort()
            async with self.__lock:
                await self.__registration.close_transports(self.__session)
            return
        await self.__session.close(nowait=True)
