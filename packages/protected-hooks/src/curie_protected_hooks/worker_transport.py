"""Pinned private worker sessions, @spec PROTECTED-HOOK-LANE-3/6/7."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ssl
from dataclasses import dataclass, field
from threading import Lock, get_ident
from typing import Any, cast

from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from redis import Redis
from redis import asyncio as aioredis
from redis._parsers.helpers import parse_info
from redis.asyncio.connection import SSLConnection as AsyncSSLConnection
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import NoBackoff
from redis.connection import ConnectionPool, SSLConnection
from redis.exceptions import ConnectionError, ResponseError
from redis.maint_notifications import MaintNotificationsConfig
from redis.retry import Retry

from curie_protected_hooks.authority_records import Manifest, _scalar
from curie_protected_hooks.broker_metadata import (
    _CONTROL_KEY,
    BrokerMetadataUnavailable,
    BrokerObservation,
)
from curie_protected_hooks.broker_transport import trusted_ca_pem

_ERROR = "Broker metadata unavailable"


@dataclass(frozen=True, slots=True)
class WorkerCredential:
    """Named provisioner credential, @spec PROTECTED-HOOK-LANE-3."""

    username: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        """Refuse missing or default credentials, @spec PROTECTED-HOOK-LANE-3."""
        if (
            not isinstance(self.username, str)
            or not self.username
            or self.username == "default"
            or not isinstance(self.password, str)
            or not self.password
        ):
            raise BrokerMetadataUnavailable() from None


@dataclass(frozen=True, slots=True)
class LaneConnections:
    """Only the unchanged worker components receive these handles, @spec PROTECTED-HOOK-LANE-7."""

    runs: aioredis.Redis = field(repr=False)
    affinity: Redis = field(repr=False)


class _Lifetime:
    """One irreversible lifetime shared by every session, @spec PROTECTED-HOOK-LANE-3."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self.dead = False
        self.loop = asyncio.get_running_loop()
        self.loop_thread = get_ident()
        self.async_connections: list[_AsyncConnection] = []
        self.sync_connections: list[_SyncConnection] = []

    def check(self) -> None:
        """An observed closed reader invalidates pool growth too, @spec PROTECTED-HOOK-LANE-3."""
        for connection in self.async_connections:
            if connection._opened and (
                not connection.is_connected
                or (connection._reader is not None and connection._reader.at_eof())
            ):
                self.fail()
        for sync_connection in self.sync_connections:
            if not sync_connection._opened or not sync_connection._probe_lock.acquire(
                blocking=False
            ):
                continue
            try:
                if not sync_connection._busy:
                    if sync_connection._sock is None:
                        self.fail()
                    else:
                        try:
                            sync_connection._parser.can_read(0)
                        except (ConnectionError, OSError):
                            self.fail()
            finally:
                sync_connection._probe_lock.release()
        if self.dead:
            raise ConnectionError(_ERROR) from None

    def fail(self) -> None:
        """Close all retained sockets without reconnecting, @spec PROTECTED-HOOK-LANE-3."""
        self.dead = True
        for connection in self.async_connections:
            if connection._writer is not None:
                if get_ident() == self.loop_thread:
                    connection._writer.close()
                elif not self.loop.is_closed():
                    self.loop.call_soon_threadsafe(connection._writer.close)
        for sync_connection in self.sync_connections:
            # Bypass our override: it would reenter this whole-client shutdown.
            SSLConnection.disconnect(sync_connection)  # type: ignore[no-untyped-call]


def _pin(certificate: bytes | None, expected: str) -> None:
    """Check SPKI before any credential is transmitted, @spec PROTECTED-HOOK-LANE-3."""
    if not certificate:
        raise ConnectionError(_ERROR)
    spki = (
        x509.load_der_x509_certificate(certificate)
        .public_key()
        .public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    )
    if not hmac.compare_digest(hashlib.sha256(spki).hexdigest(), expected):
        raise ConnectionError(_ERROR)


def _options(identity: dict[str, Any], credential: WorkerCredential, ca_pem: str) -> dict[str, Any]:
    """Shared endpoint and bounded socket policy, @spec PROTECTED-HOOK-LANE-3/7."""
    return dict(
        host=identity["endpoint"]["host"],
        port=identity["endpoint"]["port"],
        db=0,
        username=credential.username,
        password=credential.password,
        ssl_ca_data=ca_pem,
        ssl_cert_reqs=ssl.CERT_REQUIRED,
        ssl_check_hostname=True,
        socket_timeout=2.0,
        socket_connect_timeout=2.0,
        retry_on_error=[],
        retry_on_timeout=False,
        protocol=2,
        decode_responses=True,
        health_check_interval=0,
        client_name=None,
        driver_info=None,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )


class _AsyncConnection(AsyncSSLConnection):
    """Each pool member verifies TLS and live epoch, @spec PROTECTED-HOOK-LANE-3/6."""

    def __init__(
        self,
        identity: dict[str, Any],
        credential: WorkerCredential,
        ca_pem: str,
        lifetime: _Lifetime,
        **kwargs: Any,
    ) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self._identity = identity
        self._lifetime = lifetime
        self._opened = False
        super().__init__(**_options(identity, credential, ca_pem), retry=AsyncRetry(NoBackoff(), 0))
        lifetime.async_connections.append(self)

    async def _connect(self) -> None:
        """CA/name and pin precede the HELLO handshake, @spec PROTECTED-HOOK-LANE-3."""
        await super()._connect()  # type: ignore[no-untyped-call]
        secured = self._writer.get_extra_info("ssl_object") if self._writer else None
        _pin(
            secured.getpeercert(binary_form=True) if secured else None,
            self._identity["tls_spki_sha256"],
        )

    async def connect_check_health(self, *args: Any, **kwargs: Any) -> None:
        """Never reconnect any opened session, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.check()
        if self._opened and not self.is_connected:
            self._lifetime.fail()
            raise ConnectionError(_ERROR)
        try:
            await super().connect_check_health(*args, **kwargs)
            self._opened = True
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            self._lifetime.fail()
            await super().disconnect(nowait=True)
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    async def on_connect_check_health(self, check_health: bool = True) -> None:
        """RESP2 HELLO has no legacy AUTH fallback, @spec PROTECTED-HOOK-LANE-3/6."""
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
        await self.send_command("INFO", "server", check_health=False)
        if parse_info(await self.read_response()).get("run_id") != self._identity["run_id"]:  # type: ignore[no-untyped-call]
            raise ConnectionError(_ERROR)

    async def send_packed_command(self, *args: Any, **kwargs: Any) -> None:
        """Guard every command including pipelines, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.check()
        try:
            await super().send_packed_command(*args, **kwargs)
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            self._lifetime.fail()
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    async def read_response(self, *args: Any, **kwargs: Any) -> Any:
        """ACL replies preserve sessions, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.check()
        # Shield the parser, not the command: cancellation must consume exactly its
        # pending frame before the pool can lend this connection again. The ordinary
        # socket deadline still bounds draining; any real loss poisons the lifetime.
        response = asyncio.create_task(super().read_response(*args, **kwargs))
        try:
            return await asyncio.shield(response)
        except asyncio.CancelledError:
            try:
                await response
            except ResponseError:
                pass  # A consumed ACL error is still a complete frame.
            except BaseException:  # noqa: BLE001  Never leave a pending reader task behind.
                self._lifetime.fail()
                if not response.done():
                    response.cancel()
            raise
        except ResponseError:
            raise
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            self._lifetime.fail()
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    async def disconnect(self, *args: Any, **kwargs: Any) -> None:
        """Any pool member closing ends the client's lifetime, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.fail()
        await super().disconnect(*args, **kwargs)


class _SyncConnection(SSLConnection):
    """Affinity uses the same identity and terminal lifetime, @spec PROTECTED-HOOK-LANE-7."""

    def __init__(
        self,
        identity: dict[str, Any],
        credential: WorkerCredential,
        ca_pem: str,
        lifetime: _Lifetime,
        **kwargs: Any,
    ) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self._identity = identity
        self._lifetime = lifetime
        self._opened = False
        self._probe_lock = Lock()
        self._busy = False
        super().__init__(**_options(identity, credential, ca_pem), retry=Retry(NoBackoff(), 0))
        lifetime.sync_connections.append(self)

    def can_read(self, timeout: float = 0) -> bool:
        """Pool checks and passive probes share parser ownership, @spec PROTECTED-HOOK-LANE-3."""
        with self._probe_lock:
            return super().can_read(timeout)

    def _wrap_socket_with_ssl(self, sock: Any) -> ssl.SSLSocket:
        """Validate pin before the handshake, @spec PROTECTED-HOOK-LANE-3."""
        secured: ssl.SSLSocket = super()._wrap_socket_with_ssl(sock)  # type: ignore[no-untyped-call]
        try:
            _pin(secured.getpeercert(binary_form=True), self._identity["tls_spki_sha256"])
            return secured
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            secured.close()
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    def connect_check_health(self, *args: Any, **kwargs: Any) -> None:
        """An established connection may never reopen, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.check()
        if self._opened and self._sock is None:
            self._lifetime.fail()
            raise ConnectionError(_ERROR)
        try:
            super().connect_check_health(*args, **kwargs)
            self._opened = True
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            self._lifetime.fail()
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    def on_connect_check_health(self, check_health: bool = True) -> None:
        """Same RESP2 named handshake and epoch gate, @spec PROTECTED-HOOK-LANE-3/6."""
        self._parser.on_connect(self)
        self.send_command("HELLO", 2, "AUTH", self.username, self.password, check_health=False)  # type: ignore[no-untyped-call]
        hello = self.read_response()
        if (
            not isinstance(hello, list)
            or dict(zip(hello[::2], hello[1::2], strict=True)).get("proto") != 2
        ):
            raise ConnectionError(_ERROR)
        self.send_command("INFO", "server", check_health=False)  # type: ignore[no-untyped-call]
        if parse_info(self.read_response()).get("run_id") != self._identity["run_id"]:  # type: ignore[no-untyped-call]
            raise ConnectionError(_ERROR)

    def send_packed_command(self, *args: Any, **kwargs: Any) -> None:
        """Guard pipeline and ordinary writes, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.check()
        try:
            super().send_packed_command(*args, **kwargs)  # type: ignore[no-untyped-call]
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            self._lifetime.fail()
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    def read_response(self, *args: Any, **kwargs: Any) -> Any:
        """A NOPERM is a reply, not a lost session, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.check()
        try:
            return super().read_response(*args, **kwargs)
        except ResponseError:
            raise
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            self._lifetime.fail()
            if not isinstance(error, Exception):
                raise
            raise ConnectionError(_ERROR) from None

    def disconnect(self, *args: Any, **kwargs: Any) -> None:
        """Terminal shutdown, @spec PROTECTED-HOOK-LANE-3."""
        self._lifetime.fail()
        super().disconnect(*args, **kwargs)  # type: ignore[no-untyped-call]


class _AsyncPool(aioredis.ConnectionPool):
    """Do not expose provisioner material in handle logs, @spec PROTECTED-HOOK-LANE-3."""

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "ProtectedWorkerAsyncPool()"


class _SyncPool(ConnectionPool):
    """Redacted affinity handle representation, @spec PROTECTED-HOOK-LANE-3."""

    def get_connection(self, *args: Any, **kwargs: Any) -> _SyncConnection:
        """Reserve the parser for a synchronous command, @spec PROTECTED-HOOK-LANE-3."""
        connection = cast(_SyncConnection, super().get_connection(*args, **kwargs))
        with connection._probe_lock:
            connection._busy = True
        return connection

    def release(self, connection: Any) -> None:
        """Permit passive probes after response parsing, @spec PROTECTED-HOOK-LANE-3."""
        with connection._probe_lock:
            connection._busy = False
        super().release(connection)

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-3."""
        return "ProtectedWorkerAffinityPool()"


class AuthenticatedWorkerClient:
    """Closed metadata surface plus private lane handles, @spec PROTECTED-HOOK-LANE-3/7."""

    __slots__ = ("__lifetime", "__lane", "__run_id")
    __lifetime: _Lifetime
    __lane: LaneConnections
    __run_id: str

    def __new__(cls) -> AuthenticatedWorkerClient:
        """Only a verified factory can construct the client, @spec PROTECTED-HOOK-LANE-3."""
        raise BrokerMetadataUnavailable()

    def __repr__(self) -> str:
        """No provisioner secrets in representations, @spec PROTECTED-HOOK-LANE-3."""
        return "AuthenticatedWorkerClient()"

    @classmethod
    async def connect(
        cls, manifest: Manifest, credential: WorkerCredential, ca_pem: str
    ) -> AuthenticatedWorkerClient:
        """Verify both sessions before export, @spec PROTECTED-HOOK-LANE-3/6/7."""
        lifetime = _Lifetime()
        async_pool: aioredis.ConnectionPool | None = None
        sync_pool: Any = None
        try:
            if type(manifest) is not Manifest or type(credential) is not WorkerCredential:
                raise BrokerMetadataUnavailable()
            credential.__post_init__()
            identity = Manifest(manifest.canonical_bytes).as_dict()["broker_identity"]
            if identity["endpoint"]["host"] != identity["tls_server_name"]:
                raise BrokerMetadataUnavailable()
            trusted_ca_pem(ca_pem)
            options = dict(
                identity=identity,
                credential=credential,
                ca_pem=ca_pem,
                lifetime=lifetime,
                protocol=2,
                decode_responses=True,
            )
            async_pool = _AsyncPool(connection_class=_AsyncConnection, **options)
            connection = await async_pool.get_connection()  # type: ignore[no-untyped-call]
            await async_pool.release(connection)
            sync_pool = _SyncPool(connection_class=_SyncConnection, max_connections=1, **options)
            sync_connection = sync_pool.get_connection()
            sync_pool.release(sync_connection)
            client = object.__new__(cls)
            client.__lifetime = lifetime
            client.__lane = LaneConnections(
                aioredis.Redis(connection_pool=async_pool), Redis(connection_pool=sync_pool)
            )
            client.__run_id = identity["run_id"]
            return client
        except BaseException as error:  # noqa: BLE001  Close all sockets before suppressing transport details.
            lifetime.fail()
            if async_pool is not None:
                await async_pool.disconnect()
            if sync_pool is not None:
                sync_pool.disconnect()
            if not isinstance(error, Exception):
                raise
            raise BrokerMetadataUnavailable() from None

    def lane(self) -> LaneConnections:
        """Retain the handles even after terminal loss, @spec PROTECTED-HOOK-LANE-7."""
        return self.__lane

    async def read_control(self, key: str) -> bytes | None:
        """Bounded control key syntax before network, @spec PROTECTED-HOOK-LANE-3."""
        if type(key) is not str or _CONTROL_KEY.fullmatch(key) is None:
            raise BrokerMetadataUnavailable() from None
        return await self.__read(key)

    async def __read(self, key: str) -> bytes | None:
        """Preserve exact bytes despite decoded lane handles, @spec PROTECTED-HOOK-LANE-3."""
        try:
            self.__lifetime.check()
            value = await self.__lane.runs.execute_command(  # type: ignore[no-untyped-call]
                "GET", key, NEVER_DECODE=True
            )
            if value is not None and type(value) is not bytes:
                raise BrokerMetadataUnavailable()
            return value
        except Exception:  # noqa: BLE001  The metadata surface has one redacted refusal.
            raise BrokerMetadataUnavailable() from None

    async def read_binding(self, event_id: str) -> bytes | None:
        """Read one immutable opaque event binding, @spec PROTECTED-HOOK-LANE-6."""
        try:
            _scalar(event_id, "opaque_ref")
            raw = await self.__read("protected:admission:binding:" + event_id)
            if raw is not None and len(raw) > 16384:
                raise BrokerMetadataUnavailable()
            return raw
        except Exception:  # noqa: BLE001  The metadata surface has one redacted refusal.
            raise BrokerMetadataUnavailable() from None

    async def observe(self) -> BrokerObservation:
        """Live epoch and time on the verified session, @spec PROTECTED-HOOK-LANE-6."""
        try:
            self.__lifetime.check()
            info = await self.__lane.runs.info("server")
            if info.get("run_id") != self.__run_id:
                self.__lifetime.fail()
                raise BrokerMetadataUnavailable()
            seconds, micros = await self.__lane.runs.time()
            if type(seconds) is not int or type(micros) is not int or not 0 <= micros < 1000000:
                raise BrokerMetadataUnavailable()
            return BrokerObservation(self.__run_id, seconds * 1000 + micros // 1000)
        except Exception:  # noqa: BLE001  The metadata surface has one redacted refusal.
            raise BrokerMetadataUnavailable() from None

    async def close(self) -> None:
        """End every session without a reconnect path, @spec PROTECTED-HOOK-LANE-3."""
        self.__lifetime.fail()
        await self.__lane.runs.connection_pool.disconnect()
        self.__lane.affinity.connection_pool.disconnect()
