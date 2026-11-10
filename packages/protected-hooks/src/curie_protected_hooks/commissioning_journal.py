"""Authenticated commissioning journal and campaign floor, @spec PROTECTED-COMMISSIONING-3.

The commissioning issuer writes each permit once with ``SETNX``. The
commissioning control principal moves the campaign journal through
``prepared``, ``running``, ``closing`` and then ``closed`` or ``failed``, each
step a compare and set on the expected generation inside one broker script.
No step returns to ``running``. ``prepare`` advances the durable per-runtime
campaign floor in the same atomic step, so a campaign generation is never
reused. Nothing here sets a TTL or deletes a key: permits, journals and floors
are retained until the installation is retired.

A session is pinned to the manifest's broker identity, including its
``run_id``, and authenticates as one named principal. After a broker restart
the pinned session refuses; a terminal session keeps the TLS pin without the
``run_id`` equality and exposes only status, close and fail, which is all the
specification leaves a campaign whose binding no longer holds.

This module charges nothing, forwards nothing and issues no acceptance.
"""

from __future__ import annotations

import asyncio
import copy
import math
import os
from dataclasses import dataclass, field
from threading import get_ident
from typing import Any, Self

from redis import asyncio as aioredis
from redis.exceptions import ConnectionError

from .authority_records import _CREDENTIAL, Manifest, _object
from .broker_transport import trusted_ca_pem
from .commissioning_acl import JOURNAL, PERMIT
from .commissioning_bootstrap import CommissioningBootstrap
from .commissioning_records import (
    BrokerObservation,
    CommissioningPermit,
    CommissioningRecordInvalid,
    validate_commissioning_binding,
)
from .commissioning_scripts import JOURNAL_SCRIPT
from .worker_transport import (
    _ERROR,
    WorkerCredential,
    _AsyncConnection,
    _AsyncPool,
    _Lifetime,
)

__all__ = [
    "COUNTERS",
    "CampaignKeys",
    "CommissioningJournal",
    "CommissioningJournalRefused",
    "CommissioningSession",
    "JournalState",
    "campaign_keys",
    "issue_permit",
]

COUNTERS = ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")
_STATES = ("prepared", "running", "closing", "closed", "failed")
# Fixed refusal reasons. None names a member, a value or a credential.
_REASONS = (
    "absent",
    "binding",
    "charge",
    "claimed",
    "corrupt",
    "exists",
    "floor",
    "generation",
    "limit",
    "mismatch",
    "ownership",
    "principal",
    "run_id",
    "state",
    "terminal",
    "time",
    "unavailable",
    "verb",
)


class CommissioningJournalRefused(Exception):
    """Redacted refusal carrying one fixed reason, @spec PROTECTED-COMMISSIONING-3."""

    def __init__(self, reason: str = "unavailable") -> None:
        """@spec PROTECTED-COMMISSIONING-3."""
        if reason not in _REASONS:
            raise ValueError("unknown commissioning journal refusal")
        super().__init__("Commissioning journal refused: " + reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class CampaignKeys:
    """Broker key names derived from the permit, never caller supplied."""

    permit: str
    journal: str
    floor: str


def campaign_keys(permit: CommissioningPermit) -> CampaignKeys:
    """Keys inside the issuer's and control's ACL families, @spec PROTECTED-COMMISSIONING-3.

    The floor is per runtime and lives in the journal family, so every campaign
    generation of one runtime shares it.
    """
    if type(permit) is not CommissioningPermit:
        raise ValueError("commissioning permit required")
    p = permit.as_dict()
    suffix = p["campaign_id"] + ":" + p["campaign_generation"]
    return CampaignKeys(PERMIT + suffix, JOURNAL + suffix, JOURNAL + "floor:" + p["runtime_id"])


@dataclass(frozen=True, slots=True)
class JournalState:
    """One observed journal, with the broker ``TIME`` the script read."""

    state: str
    generation: int
    counters: dict[str, int]
    exhausted: str | None
    broker_time_ms: int = field(compare=False)


def _decimal(value: Any, *, zero: bool) -> int:
    if (
        type(value) is not str
        or not value.isascii()
        or not value.isdigit()
        or (len(value) > 1 and value[0] == "0")
        or (value == "0" and not zero)
        or int(value) > 2**63 - 1
    ):
        raise CommissioningJournalRefused("corrupt")
    return int(value)


def _journal_state(reply: list[Any]) -> JournalState:
    """Strict decoding of the script's journal reply."""
    if len(reply) != 10 or reply[1] not in _STATES or reply[8] not in ("", *COUNTERS):
        raise CommissioningJournalRefused("corrupt")
    return JournalState(
        reply[1],
        _decimal(reply[2], zero=False),
        {
            name: _decimal(value, zero=True)
            for name, value in zip(COUNTERS, reply[3:8], strict=True)
        },
        reply[8] or None,
        _decimal(reply[9], zero=True),
    )


class _TerminalConnection(_AsyncConnection):
    """TLS name and SPKI pin without the ``run_id`` equality, @spec PROTECTED-COMMISSIONING-3.

    Only a terminal session uses it, so that a campaign whose broker restarted
    can still close or fail. The scripts refuse every positive step on a
    ``run_id`` change regardless of the session.
    """

    async def on_connect_check_health(self, check_health: bool = True) -> None:
        """RESP2 HELLO with no legacy AUTH fallback, @spec PROTECTED-COMMISSIONING-3."""
        self._parser.on_connect(self)  # type: ignore[no-untyped-call]
        await self.send_command(
            "HELLO", 2, "AUTH", self.username, self.password, check_health=False
        )
        hello = await self.read_response()
        if (
            not isinstance(hello, list)
            or dict(zip(hello[::2], hello[1::2], strict=True)).get("proto") != 2
        ):
            raise ConnectionError(_ERROR)


_TRANSPORT_FORK_GENERATION = 0


def _parent_transport_forked() -> None:
    global _TRANSPORT_FORK_GENERATION
    _TRANSPORT_FORK_GENERATION += 1


os.register_at_fork(after_in_parent=_parent_transport_forked)


class _DeadlineLifetime(_Lifetime):
    """Every failure hard-closes before any inherited cleanup detaches its writer."""

    def __init__(self) -> None:
        super().__init__()
        self.writers: set[asyncio.StreamWriter] = set()
        self.sockets: dict[asyncio.StreamWriter, Any] = {}
        self._owner_pid = os.getpid()
        self._fork_generation = _TRANSPORT_FORK_GENERATION

    def require_unforked(self) -> None:
        # Opt-in registered observations/join witnesses use this check. Default
        # sessions retain their existing behavior. A joined child cannot undo
        # the fact that original socket ownership became ambiguous on fork.
        if self._owner_pid != os.getpid() or self._fork_generation != _TRANSPORT_FORK_GENERATION:
            raise CommissioningJournalRefused()

    def fail(self) -> None:
        for connection in self.async_connections:
            writer = connection._writer
            if writer is not None and writer not in self.writers:
                self.writers.add(writer)
                # SSLProtocolTransport detaches its protocol at connection_lost;
                # retain the original socket before requesting close.
                try:
                    self.sockets[writer] = writer.get_extra_info("socket")
                except AttributeError:
                    self.sockets[writer] = None
        super().fail()
        for connection in self.async_connections:
            if connection._writer is not None:
                abort = connection._writer.transport.abort
                if get_ident() == self.loop_thread:
                    abort()
                elif not self.loop.is_closed():
                    self.loop.call_soon_threadsafe(abort)

    async def join_closed(self) -> None:
        """Join retained TLS writers after failure, @spec COMMISSIONING-SERIES-8."""
        self.fail()
        results = await asyncio.gather(
            *(writer.wait_closed() for writer in self.writers), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException) and not isinstance(
                result, (OSError, ConnectionError)
            ):
                raise CommissioningJournalRefused()
        for writer in self.writers:
            socket = self.sockets[writer]
            if not writer.is_closing() or socket is None or socket.fileno() != -1:
                raise CommissioningJournalRefused()
        self.require_unforked()


class CommissioningSession:
    """One authenticated, non-reconnecting session as one named principal.

    @spec PROTECTED-COMMISSIONING-1/3.
    """

    __slots__ = (
        "__lifetime",
        "__lock",
        "__pool",
        "__principal",
        "__identity",
        "__redis",
        "__run_id",
        "__terminal",
    )
    __lifetime: _Lifetime
    __lock: asyncio.Lock
    __pool: _AsyncPool
    __principal: dict[str, Any]
    __identity: dict[str, Any]
    __redis: aioredis.Redis
    __run_id: str
    __terminal: bool

    def __new__(cls) -> Self:
        """Only the verified factory constructs a session, @spec PROTECTED-COMMISSIONING-3."""
        raise CommissioningJournalRefused()

    def __repr__(self) -> str:
        """No credential in representations, @spec PROTECTED-COMMISSIONING-3."""
        return "CommissioningSession()"

    @classmethod
    async def connect(
        cls,
        manifest: Manifest,
        credential: WorkerCredential,
        ca_pem: str,
        *,
        principal: dict[str, Any],
        terminal: bool = False,
        deadline: float | None = None,
    ) -> Self:
        """Pin TLS and epoch, then prove ``ACL WHOAMI`` names the principal.

        @spec PROTECTED-COMMISSIONING-1/3.
        """
        lifetime = _Lifetime() if deadline is None else _DeadlineLifetime()
        pool: _AsyncPool | None = None
        expiry: asyncio.TimerHandle | None = None
        try:
            if deadline is not None:
                if (
                    type(deadline) is not float
                    or not math.isfinite(deadline)
                    or terminal is not True
                ):
                    raise CommissioningJournalRefused()
                expiry = asyncio.get_running_loop().call_at(deadline, lifetime.fail)
            if (
                type(manifest) is not Manifest
                or type(credential) is not WorkerCredential
                or type(terminal) is not bool
            ):
                raise CommissioningJournalRefused()
            _object(principal, _CREDENTIAL)
            if credential.username != principal["id"]:
                raise CommissioningJournalRefused("principal")
            credential.__post_init__()
            identity = Manifest(manifest.canonical_bytes).as_dict()["broker_identity"]
            if identity["endpoint"]["host"] != identity["tls_server_name"]:
                raise CommissioningJournalRefused()
            trusted_ca_pem(ca_pem)
            pool = _AsyncPool(
                connection_class=_TerminalConnection if terminal else _AsyncConnection,
                max_connections=1,
                identity=identity,
                credential=credential,
                ca_pem=ca_pem,
                lifetime=lifetime,
                protocol=2,
                decode_responses=True,
            )
            redis = aioredis.Redis(connection_pool=pool)
            whoami = await redis.execute_command("ACL", "WHOAMI")  # type: ignore[no-untyped-call]
            if whoami != credential.username:
                raise CommissioningJournalRefused("principal")
            session = object.__new__(cls)
            session.__lifetime = lifetime
            session.__lock = asyncio.Lock()
            session.__pool = pool
            session.__principal = copy.deepcopy(principal)
            session.__redis = redis
            session.__identity = identity
            session.__run_id = identity["run_id"]
            session.__terminal = terminal
            return session
        except BaseException as error:  # noqa: BLE001  Close the socket before suppressing transport details.
            lifetime.fail()
            if pool is not None:
                if deadline is None:
                    await pool.disconnect()
                else:
                    lifetime.fail()
                    await asyncio.gather(
                        *(
                            connection.disconnect(nowait=True)
                            for connection in lifetime.async_connections
                        )
                    )
            if not isinstance(error, Exception):
                raise
            if isinstance(error, CommissioningJournalRefused):
                raise CommissioningJournalRefused(error.reason) from None
            raise CommissioningJournalRefused() from None
        finally:
            if expiry is not None:
                expiry.cancel()

    @property
    def principal(self) -> dict[str, Any]:
        """Fresh copy of the authenticated credential reference."""
        return copy.deepcopy(self.__principal)

    @property
    def terminal(self) -> bool:
        """Whether this session may only read, close or fail."""
        return self.__terminal

    async def execute(self, *args: Any) -> Any:
        """One command at a time on the single pinned connection.

        A transport loss ends the session; nothing reconnects. ACL refusals
        and script errors become the one redacted refusal.
        """
        async with self.__lock:
            try:
                self.__lifetime.check()
                return await self.__redis.execute_command(*args)  # type: ignore[no-untyped-call]
            except Exception:  # noqa: BLE001  The session surface has one redacted refusal.
                raise CommissioningJournalRefused() from None

    async def broker_time_ms(self) -> int:
        """Broker ``TIME``, after checking the live ``run_id`` on a pinned session.

        A terminal session reads time without the ``run_id`` equality; the
        scripts still refuse every positive step on a changed ``run_id``.
        @spec PROTECTED-COMMISSIONING-3.
        """
        if not self.__terminal:
            info = await self.execute("INFO", "server")
            if type(info) is not dict or info.get("run_id") != self.__run_id:
                raise CommissioningJournalRefused("run_id")
        clock = await self.execute("TIME")
        try:
            seconds, micros = (int(value) for value in clock)
        except (TypeError, ValueError):
            raise CommissioningJournalRefused() from None
        if seconds < 0 or not 0 <= micros < 1000000:
            raise CommissioningJournalRefused()
        return seconds * 1000 + micros // 1000

    def pinned_to(self, manifest: Manifest) -> bool:
        """Whether this session was pinned to ``manifest``'s broker identity and epoch."""
        return (
            type(manifest) is Manifest and manifest.as_dict()["broker_identity"] == self.__identity
        )

    def abort(self) -> None:
        """Immediately poison sockets before cancelled parser draining can delay closure."""
        self.__lifetime.fail()

    async def close(self, *, nowait: bool = False) -> None:
        """End the session without a reconnect path, @spec PROTECTED-COMMISSIONING-3."""
        self.__lifetime.fail()
        if nowait:
            self.abort()
            await asyncio.gather(
                *(
                    connection.disconnect(nowait=True)
                    for connection in self.__lifetime.async_connections
                )
            )
        else:
            await self.__pool.disconnect()

    async def close_joined(self) -> None:
        """Actual writer closure for the finite host witness, @spec COMMISSIONING-SERIES-8.

        The owning adapter must first join its command tasks. This method alone
        supplies no registry completeness, SDK join or independent broker absence.
        """
        if type(self.__lifetime) is not _DeadlineLifetime:
            raise CommissioningJournalRefused()
        self.__lifetime.fail()
        await self.close(nowait=True)
        await self.__lifetime.join_closed()

    def check_process_custody(self) -> None:
        """Registered process/fork fence, no broker verdict, @spec COMMISSIONING-SERIES-8."""
        if type(self.__lifetime) is not _DeadlineLifetime:
            raise CommissioningJournalRefused()
        self.__lifetime.require_unforked()


def _binding(
    session: CommissioningSession,
    permit: CommissioningPermit,
    principal: dict[str, Any],
    manifest: Manifest,
    time_ms: int,
    trusted: dict[str, Any],
) -> None:
    """Validate the permit binding as the named principal at ``time_ms``."""
    if type(session) is not CommissioningSession or type(permit) is not CommissioningPermit:
        raise CommissioningJournalRefused()
    if session.principal != principal:
        raise CommissioningJournalRefused("principal")
    if not session.pinned_to(manifest):
        raise CommissioningJournalRefused("run_id")
    # The session is pinned to the manifest's broker, so its identity is the manifest's.
    observed = BrokerObservation(identity=manifest.as_dict()["broker_identity"], time_ms=time_ms)
    try:
        validate_commissioning_binding(permit, manifest=manifest, broker=observed, **trusted)
    except CommissioningRecordInvalid:
        raise CommissioningJournalRefused("binding") from None


async def issue_permit(
    session: CommissioningSession,
    permit: CommissioningPermit,
    *,
    manifest: Manifest,
    bootstrap: CommissioningBootstrap,
    trusted_issuer: dict[str, Any],
    trusted_control: dict[str, Any],
    max_permit_ms: int,
) -> None:
    """Write the permit once with ``SETNX`` as the commissioning issuer.

    The binding is validated at live broker time first, and the permit's
    campaign generation must be above the runtime's durable floor. The control
    principal's ``prepare`` checks the floor again atomically; this read only
    keeps a permit that can never start from being written. Identical bytes
    already stored are a replay; different bytes refuse.
    @spec PROTECTED-COMMISSIONING-1/3.
    """
    if type(session) is not CommissioningSession:
        raise CommissioningJournalRefused()
    if session.terminal:
        raise CommissioningJournalRefused("terminal")
    trusted = dict(
        bootstrap=bootstrap,
        trusted_issuer=trusted_issuer,
        trusted_control=trusted_control,
        max_permit_ms=max_permit_ms,
    )
    now = await session.broker_time_ms()
    _binding(session, permit, trusted_issuer, manifest, now, trusted)
    keys = campaign_keys(permit)
    floor = await session.execute("GET", keys.floor)
    current = 0 if floor is None else _floor(floor)
    if int(permit.as_dict()["campaign_generation"]) <= current:
        raise CommissioningJournalRefused("floor")
    raw = permit.canonical_bytes.decode("ascii")
    if await session.execute("SETNX", keys.permit, raw) == 1:
        return
    if await session.execute("GET", keys.permit) != raw:
        raise CommissioningJournalRefused("exists")


def _floor(value: Any) -> int:
    try:
        return _decimal(value, zero=True)
    except CommissioningJournalRefused:
        raise CommissioningJournalRefused("corrupt") from None


class CommissioningJournal:
    """The control principal's journal for one campaign, @spec PROTECTED-COMMISSIONING-3.

    Every method is one atomic broker script. The script, not this object,
    decides: it reads the issuer-written permit, broker ``TIME`` and the live
    ``run_id`` and refuses when any of them disagrees. No state is cached
    here, so a restarted control process resumes from the durable journal.
    """

    __slots__ = ("__keys", "__permit", "__session")
    __keys: CampaignKeys
    __permit: CommissioningPermit
    __session: CommissioningSession

    def __new__(cls) -> Self:
        """Only ``open`` constructs a journal, @spec PROTECTED-COMMISSIONING-3."""
        raise CommissioningJournalRefused()

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-3."""
        return "CommissioningJournal()"

    @classmethod
    async def open(
        cls,
        session: CommissioningSession,
        permit: CommissioningPermit,
        *,
        manifest: Manifest,
        bootstrap: CommissioningBootstrap,
        trusted_issuer: dict[str, Any],
        trusted_control: dict[str, Any],
        max_permit_ms: int,
    ) -> Self:
        """Bind a control session to one permit; the session closes with the journal.

        The binding is validated at broker time clamped into the permit
        interval, so a restarted control process can still read, close or fail
        an expired campaign. Live time decides every positive step inside the
        scripts. @spec PROTECTED-COMMISSIONING-1/3.
        """
        try:
            if type(session) is not CommissioningSession:
                raise CommissioningJournalRefused()
            if type(permit) is not CommissioningPermit:
                raise CommissioningJournalRefused("binding")
            p = permit.as_dict()
            now = await session.broker_time_ms()
            clamped = min(max(now, int(p["issued_at_ms"])), int(p["expires_at_ms"]) - 1)
            trusted = dict(
                bootstrap=bootstrap,
                trusted_issuer=trusted_issuer,
                trusted_control=trusted_control,
                max_permit_ms=max_permit_ms,
            )
            _binding(session, permit, trusted_control, manifest, clamped, trusted)
        except BaseException:
            if type(session) is CommissioningSession:
                await session.close()
            raise
        journal = object.__new__(cls)
        journal.__keys = campaign_keys(permit)
        journal.__permit = permit
        journal.__session = session
        return journal

    async def __script(self, verb: str, expected: int = 0) -> JournalState:
        if type(expected) is not int or not 0 <= expected <= 2**63 - 1:
            raise CommissioningJournalRefused("generation")
        if self.__session.terminal and verb in ("prepare", "start", "require_running"):
            raise CommissioningJournalRefused("terminal")
        return _reply(await self._eval(JOURNAL_SCRIPT, (), verb, str(expected)))

    @property
    def keys(self) -> CampaignKeys:
        """The permit, journal and floor keys of this campaign."""
        return self.__keys

    @property
    def permit(self) -> CommissioningPermit:
        """The immutable permit this journal is bound to."""
        return self.__permit

    @property
    def terminal(self) -> bool:
        """Whether the bound session may only read, close or fail."""
        return self.__session.terminal

    async def _eval(self, script: str, keys: tuple[str, ...], verb: str, *args: str) -> Any:
        """Run one commissioning script over this campaign's prelude keys and permit.

        For the reservation and ownership scripts of the same campaign; the
        script, not the caller, checks every key it is given.
        """
        return await self.__session.execute(
            "EVAL",
            script,
            3 + len(keys),
            self.__keys.permit,
            self.__keys.journal,
            self.__keys.floor,
            *keys,
            verb,
            self.__permit.canonical_bytes.decode("ascii"),
            self.__permit.digest,
            *args,
        )

    async def prepare(self) -> JournalState:
        """Advance the runtime floor and create the ``prepared`` journal."""
        return await self.__script("prepare")

    async def status(self) -> JournalState:
        """Read the journal; available after expiry and after a broker restart."""
        return await self.__script("status")

    async def require_running(self) -> JournalState:
        """Refuse unless ``running``, inside the permit interval and on the bound broker."""
        return await self.__script("require_running")

    async def start(self, expected_generation: int) -> JournalState:
        """``prepared`` to ``running``, only with live authority."""
        return await self.__script("start", expected_generation)

    async def begin_close(self, expected_generation: int) -> JournalState:
        """``prepared`` or ``running`` to ``closing``."""
        return await self.__script("begin_close", expected_generation)

    async def confirm_closed(self, expected_generation: int) -> JournalState:
        """``closing`` to terminal ``closed``."""
        return await self.__script("confirm_closed", expected_generation)

    async def fail(self, expected_generation: int) -> JournalState:
        """Any live state to terminal ``failed``."""
        return await self.__script("fail", expected_generation)

    async def close(self) -> None:
        """End the session; the journal itself is retained."""
        await self.__session.close()


def _reply(reply: Any) -> JournalState:
    """Strict decoding of one journal script reply."""
    if type(reply) is not list or not reply:
        raise CommissioningJournalRefused()
    if reply[0] == "refused" and len(reply) == 2:
        reason = reply[1] if reply[1] in _REASONS else "unavailable"
        raise CommissioningJournalRefused(reason)
    if reply[0] != "journal":
        raise CommissioningJournalRefused()
    return _journal_state(reply)
