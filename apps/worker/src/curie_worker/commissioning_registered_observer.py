"""Measured operator observer composition, not qualification, @spec COMMISSIONING-SERIES-8.

No trial session is relabelled or exempted here. Complete membership, genuine
SDK joins, checkpoints, final decisions and PC-6 publication remain unavailable.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import sys
from collections.abc import Coroutine
from typing import Any, Self, TypeVar

from curie_protected_hooks.commissioning_evidence_acl import evidence_observer_acl_rules
from curie_protected_hooks.commissioning_evidence_records import EvidenceInventory
from curie_protected_hooks.commissioning_journal import (
    CommissioningSession,
    _DeadlineLifetime,
    _TerminalConnection,
)
from curie_protected_hooks.commissioning_operator_custody import OperatorRunCustody
from curie_protected_hooks.commissioning_producers import _fields, _text
from curie_protected_hooks.commissioning_records import CommissioningPermit
from curie_protected_hooks.worker_acl import _HANDSHAKE
from curie_protected_hooks.worker_transport import WorkerCredential, _AsyncPool
from redis import asyncio as aioredis

from .commissioning_evidence_observer import (
    ClientObservation,
    EvidenceObservation,
    RetainedEvidenceObserver,
    SeriesEvidenceBinding,
    _clients,
    _hello,
)


class RegisteredObservationRefused(ValueError):
    """No identity, credentials or retained contents in diagnostics."""

    def __init__(self) -> None:
        super().__init__("registered evidence observation refused")


_T = TypeVar("_T")


def _require(value: bool) -> None:
    if not value:
        raise RegisteredObservationRefused()


def _host_process() -> dict[str, str]:
    # proc_pid_stat(5): field 22 is starttime; the comm (field 2) may contain
    # spaces and ')'. proc_pid_ns(5) and random(4) define namespace/boot IDs.
    _require(sys.platform == "linux")
    pid = os.getpid()
    with open(f"/proc/{pid}/stat", encoding="ascii") as stream:
        raw = stream.read(4097)
    _require(len(raw) <= 4096 and raw.split(" ", 1)[0] == str(pid))
    tail = raw.rsplit(")", 1)[1].split()
    with open("/proc/sys/kernel/random/boot_id", encoding="ascii") as stream:
        boot = stream.read(128).strip()
    return {
        "pid": str(pid),
        "start_ticks": tail[19],
        "boot_id": boot,
        "pid_namespace_inode": str(os.stat(f"/proc/{pid}/ns/pid").st_ino),
    }


def _words(value: object) -> frozenset[str]:
    words = _text(value).split()
    _require(len(words) == len(set(words)))
    return frozenset(words)


def _rights(reply: object, credential: WorkerCredential, *, evidence: bool) -> None:
    # The actual GETUSER response is compared semantically against the fixed
    # product recipes, including selector key permissions. No wrapper/username
    # or successful GET alone supplies this independent rights observation.
    fields = _fields(reply)
    _require(set(fields) == {"flags", "passwords", "commands", "keys", "channels", "selectors"})
    flags, passwords = fields["flags"], fields["passwords"]
    _require(type(flags) in {list, set, frozenset})
    assert isinstance(flags, (list, set, frozenset))
    _require({_text(flag) for flag in flags} == {"on"})
    _require(type(passwords) is list)
    assert isinstance(passwords, list)
    _require(
        [_text(value) for value in passwords]
        == [hashlib.sha256(credential.password.encode()).hexdigest()]
    )
    handshake = {"-@all", *_HANDSHAKE}
    expected = handshake | (
        {"+acl|whoami", "+info|server", "+time", "+client|list"} if evidence else {"+acl|getuser"}
    )
    _require(_words(fields["commands"]) == expected)
    _require(_text(fields["keys"]) == "" and _text(fields["channels"]) == "")
    selectors = fields["selectors"]
    _require(type(selectors) is list)
    assert isinstance(selectors, list)
    if not evidence:
        _require(selectors == [])
        return
    _require(len(selectors) == 1)
    selector = _fields(selectors[0])
    _require(set(selector) == {"commands", "keys", "channels"})
    _require(_words(selector["commands"]) == {"-@all", "+get", "+type"})
    rule = evidence_observer_acl_rules()[-1][1:-1].split()
    _require(_words(selector["keys"]) == {word for word in rule if word.startswith("%R~")})
    _require(_text(selector["channels"]) == "")


class _AclSession:
    """Only authenticated HELLO identification and independent GETUSER."""

    def __init__(self) -> None:
        self.lifetime = _DeadlineLifetime()
        self.pool: _AsyncPool | None = None
        self.redis: aioredis.Redis | None = None

    async def connect(self, registration: _OperatorRegistration) -> str:
        identity = registration.binding.manifest.as_dict()["broker_identity"]
        self.pool = _AsyncPool(
            connection_class=_TerminalConnection,
            max_connections=1,
            identity=identity,
            credential=registration.acl_credential,
            ca_pem=registration.ca_pem,
            lifetime=self.lifetime,
            protocol=2,
            decode_responses=True,
        )
        self.redis = aioredis.Redis(connection_pool=self.pool)
        return _hello(await self.redis.execute_command("HELLO", 2))  # type: ignore[no-untyped-call]

    async def getuser(self, username: str) -> object:
        _require(self.redis is not None)
        assert self.redis is not None
        self.lifetime.check()
        return await self.redis.execute_command("ACL", "GETUSER", username)  # type: ignore[no-untyped-call]

    async def close_joined(self) -> None:
        self.lifetime.fail()
        if self.pool is not None:
            await self.pool.disconnect()
        await self.lifetime.join_closed()


class _OperatorRegistration:
    """One fresh factory's composition; no portable or reusable registration."""

    def __init__(
        self,
        run: OperatorRunCustody,
        binding: SeriesEvidenceBinding,
        credential: WorkerCredential,
        acl_credential: WorkerCredential,
        ca_pem: str,
    ) -> None:
        _require(type(run) is OperatorRunCustody and type(binding) is SeriesEvidenceBinding)
        _require(type(credential) is WorkerCredential and type(acl_credential) is WorkerCredential)
        self.run, self.binding = run, binding
        self.credential, self.acl_credential, self.ca_pem = credential, acl_credential, ca_pem
        self.evidence_ticket: str | None = None
        self.acl_ticket: str | None = None
        self.session: CommissioningSession | None = None
        self.acl: _AclSession | None = None
        self.epoch: str | None = None
        self.client: str | None = None
        self.acl_client: str | None = None
        self.closed = False
        self.failed_close = False
        self.finalized = False
        binding.__post_init__()
        view = run.view()
        _require(view.plan is not None)
        assert view.plan is not None
        static = view.plan.as_dict()["static_binding"]
        _require(static["bootstrap"] == binding.bootstrap.digest)
        _require(static["guard"] == hashlib.sha256(binding.guard_config).hexdigest())
        _require(
            static["execution_config"] == binding.manifest.as_dict()["execution_config_digest"]
        )
        operator_view = run.operator_view()
        latest = {
            event.as_dict()["intent_id"]: event.as_dict()["kind"] for event in operator_view.events
        }
        _require(all(kind == "closed" for kind in latest.values()))
        roles = {row["role"]: row["credential"] for row in operator_view.recipe.as_dict()["roles"]}
        b, g = binding.bootstrap.as_dict(), json.loads(binding.guard_config)
        _require(roles["evidence-observer"] == b["evidence_observer_identity"])
        _require(roles["acl-observer"] == g["broker_acl"]["observer"]["credential"])
        _require(credential.username == roles["evidence-observer"]["id"])
        _require(acl_credential.username == roles["acl-observer"]["id"])
        self.evidence_ticket = run.intend_observer("evidence-observer")

    def check_active(self) -> None:
        _require(not self.closed)
        self.check_process_custody()
        with self.run._operation(positive=True):
            self.run._elapsed(positive=True)

    def check_process_custody(self) -> None:
        if self.session is not None:
            self.session.check_process_custody()
        if self.acl is not None:
            self.acl.lifetime.require_unforked()

    def budget(self) -> float:
        self.check_active()
        return min(4.0, (self.run._limit_ns - (self.run._last_ns - self.run._started_ns)) / 1e9)

    def abort(self) -> None:
        if self.session is not None:
            self.session.abort()
        if self.acl is not None:
            self.acl.lifetime.fail()

    async def bind_and_measure(
        self, session: CommissioningSession, epoch: str, client: str
    ) -> None:
        self.session, self.epoch, self.client = session, epoch, client
        _require(self.evidence_ticket is not None)
        assert self.evidence_ticket is not None
        identity = copy.deepcopy(self.binding.manifest.as_dict()["broker_identity"])
        identity["run_id"] = epoch
        self.run._record_transport(
            self.evidence_ticket,
            kind="bound",
            identity={
                "client_id": client,
                "broker_identity": identity,
                "credential": self.binding.bootstrap.as_dict()["evidence_observer_identity"],
                "host_process": _host_process(),
            },
        )
        self.acl_ticket = self.run.intend_observer("acl-observer")
        self.acl = _AclSession()
        async with asyncio.timeout(self.budget()):
            self.acl_client = await self.acl.connect(self)
            self.run._record_transport(
                self.acl_ticket,
                kind="bound",
                identity={
                    "client_id": self.acl_client,
                    "broker_identity": identity,
                    "credential": json.loads(self.binding.guard_config)["broker_acl"]["observer"][
                        "credential"
                    ],
                    "host_process": _host_process(),
                },
            )
            await self.measure()

    async def measure(self) -> None:
        _require(self.session is not None and self.acl is not None)
        assert self.session is not None and self.acl is not None
        deadline = asyncio.get_running_loop().time() + self.budget()
        timer = asyncio.get_running_loop().call_at(deadline, self.abort)
        try:
            async with asyncio.timeout_at(deadline):
                _require((await self.session.execute("INFO", "server"))["run_id"] == self.epoch)
                _rights(
                    await self.acl.getuser(self.acl_credential.username),
                    self.acl_credential,
                    evidence=False,
                )
                _rights(
                    await self.acl.getuser(self.credential.username), self.credential, evidence=True
                )
                clients = _clients(await self.session.execute("CLIENT", "LIST"))
                _require(
                    any(
                        c.client_id == self.client and c.username == self.credential.username
                        for c in clients
                    )
                )
                _require(
                    any(
                        c.client_id == self.acl_client
                        and c.username == self.acl_credential.username
                        for c in clients
                    )
                )
                _require((await self.session.execute("INFO", "server"))["run_id"] == self.epoch)
                self.check_active()
        finally:
            timer.cancel()

    async def close_transports(self, session: CommissioningSession | None = None) -> None:
        if self.closed:
            _require(not self.failed_close)
            return
        self.abort()
        self.closed = True
        try:
            async with asyncio.timeout(4.0):
                joins = []
                if self.acl is not None:
                    joins.append(self.acl.close_joined())
                current = session or self.session
                if current is not None:
                    joins.append(current.close_joined())
                results = await asyncio.gather(*joins, return_exceptions=True)
                _require(not any(isinstance(result, BaseException) for result in results))
        except BaseException:  # noqa: BLE001  Keep teardown uncertainty irreversible.
            self.failed_close = True
            # Retain original unresolved identities. No fabricated passing close
            # or automatic retry repairs an unavailable join.
            raise RegisteredObservationRefused() from None

    def finish_records(self) -> None:
        # Only the outer owner calls this after its tracked child observation
        # tasks have actually finished. A child's own close cannot join itself.
        _require(self.closed and not self.failed_close)
        self.check_process_custody()
        if self.finalized:
            return
        events = self.run.operator_view().events
        for ticket in (self.acl_ticket, self.evidence_ticket):
            if ticket is None:
                continue
            last = next(e.as_dict() for e in reversed(events) if e.as_dict()["intent_id"] == ticket)
            self.run._record_transport(
                ticket, kind="closed" if last["kind"] == "bound" else "uncertain"
            )
        self.finalized = True


class RegisteredEvidenceObserver:
    """Closed paired composition; no command, permit or publisher surface."""

    _observer: RetainedEvidenceObserver
    _registration: _OperatorRegistration
    _tasks: set[asyncio.Task[Any]]
    _close_task: asyncio.Task[None] | None
    _closed: bool

    def __new__(cls) -> Self:
        raise RegisteredObservationRefused()

    def __repr__(self) -> str:
        return "RegisteredEvidenceObserver()"

    @classmethod
    async def connect(
        cls,
        run: OperatorRunCustody,
        binding: SeriesEvidenceBinding,
        credential: WorkerCredential,
        acl_credential: WorkerCredential,
        ca_pem: str,
    ) -> Self:
        registration = None
        try:
            registration = _OperatorRegistration(run, binding, credential, acl_credential, ca_pem)
            observer = await RetainedEvidenceObserver.connect(
                binding, credential, ca_pem, _registration=registration
            )
            result = object.__new__(cls)
            result._observer, result._registration = observer, registration
            result._tasks, result._close_task, result._closed = set(), None, False
            return result
        except BaseException as error:
            if type(run) is OperatorRunCustody:
                run._poisoned = True
            if registration is not None:
                await registration.close_transports()
                registration.finish_records()
            if not isinstance(error, Exception):
                raise
            raise RegisteredObservationRefused() from None

    async def read(
        self, permit: CommissioningPermit, inventory: EvidenceInventory
    ) -> EvidenceObservation:
        """Fresh rights then retained observations, never a verdict."""
        _require(not self._closed)
        return await self._perform(self._observer.read(permit, inventory))

    async def clients(self) -> ClientObservation:
        """Fresh actual membership; no completeness assertion."""
        _require(not self._closed)
        return await self._perform(self._observer.clients())

    async def _perform(self, operation: Coroutine[Any, Any, _T]) -> _T:
        task = asyncio.create_task(operation)
        self._tasks.add(task)
        try:
            return await task
        except BaseException as error:  # noqa: BLE001  Join before returning a redacted refusal/cancel.
            self._registration.run._poisoned = True
            if self._close_task is None:
                await self.close()
            if not isinstance(error, Exception):
                raise
            raise RegisteredObservationRefused() from None
        finally:
            self._tasks.discard(task)

    async def _finish(self) -> None:
        self._registration.abort()
        try:
            async with asyncio.timeout(4.0):
                tasks = tuple(self._tasks)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self._observer.close()
                self._registration.finish_records()
        except BaseException as error:  # noqa: BLE001  Never turn uncertain teardown into success.
            self._registration.run._poisoned = True
            if not isinstance(error, Exception):
                raise
            raise RegisteredObservationRefused() from None

    async def close(self) -> None:
        """Actual paired close/task joins before durable closure records."""
        if self._close_task is None:
            self._closed = True
            # Make shutdown irreversible before any scheduling boundary. The
            # caller may be cancelled before the owned task's first step.
            self._registration.abort()
            self._close_task = asyncio.create_task(self._finish())
        interrupted = False
        while True:
            try:
                await asyncio.shield(self._close_task)
                break
            except asyncio.CancelledError:
                if self._close_task.done() and self._close_task.cancelled():
                    self._registration.run._poisoned = True
                    raise RegisteredObservationRefused() from None
                # Repeated caller cancellation never cancels or detaches the
                # same finite owned teardown. Its actual failure still raises.
                interrupted = True
        if interrupted:
            raise asyncio.CancelledError
