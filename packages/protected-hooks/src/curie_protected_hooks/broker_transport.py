"""Authenticated metadata transport, @spec PROTECTED-HOOK-LANE-2/3/SOURCE-6."""

from __future__ import annotations

import hashlib
import hmac
import re
import ssl
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from socket import SHUT_RDWR, socket
from threading import Lock
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from redis._parsers.helpers import parse_info
from redis.backoff import NoBackoff
from redis.connection import SSLConnection
from redis.exceptions import ConnectionError
from redis.maint_notifications import MaintNotificationsConfig
from redis.retry import Retry

from curie_protected_hooks.authority_records import Manifest
from curie_protected_hooks.broker_metadata import (
    _CONTROL_KEY,
    BrokerMetadataUnavailable,
    BrokerObservation,
)
from curie_protected_hooks.source_fence import (
    SourceFence,
    SourceFenceConflict,
    SourceFenceExhausted,
    SourceFenceInvalid,
    SourceState,
    _decode_source,
    _source_key,
)

# @spec PROTECTED-HOOK-LANE-2/3 @spec PROTECTED-HOOK-SOURCE-9
_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"
_SPACE = frozenset(" \t\n\r\f\v")
_BODY = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\r\n")
_EOL = frozenset("\r\n")


def _pem_body(body: str) -> bool:
    """Whether ``body`` is whitespace then base64 lines, @spec PROTECTED-HOOK-SOURCE-9.

    The language of ``\\s+[A-Za-z0-9+/=\\r\\n]+`` decided in one pass: a nonempty
    whitespace prefix, then body characters only. When the body is all
    whitespace, the classes share only CR and LF, so it must end in one of them
    after at least one other character.
    """
    prefix = 0
    while prefix < len(body) and body[prefix] in _SPACE:
        prefix += 1
    if prefix == 0:
        return False
    if prefix < len(body):
        return all(character in _BODY for character in body[prefix:])
    return len(body) >= 2 and body[-1] in _EOL


def _pem_certificates(text: str) -> bool:
    """Whether ``text`` is one or more certificate PEM blocks, in linear time.

    Accepts exactly what the former pattern
    ``\\s*(?:-----BEGIN CERTIFICATE-----\\s+[A-Za-z0-9+/=\\r\\n]+-----END
    CERTIFICATE-----\\s*)+`` (ASCII) accepted, without backtracking: neither
    class contains ``-``, so each body ends at the next ``-``.
    @spec PROTECTED-HOOK-LANE-2/3 @spec PROTECTED-HOOK-SOURCE-9.
    """
    index, size, blocks = 0, len(text), 0
    while True:
        while index < size and text[index] in _SPACE:
            index += 1
        if index == size:
            return blocks > 0
        if not text.startswith(_BEGIN, index):
            return False
        index += len(_BEGIN)
        end = text.find("-", index)
        if end < 0 or not _pem_body(text[index:end]) or not text.startswith(_END, end):
            return False
        index = end + len(_END)
        blocks += 1


def trusted_ca_pem(ca_pem: str) -> str:
    """Validated broker CA certificates or the single safe refusal.

    @spec PROTECTED-HOOK-LANE-2/3 @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        if not isinstance(ca_pem, str) or not _pem_certificates(ca_pem):
            raise BrokerMetadataUnavailable()
        if not x509.load_pem_x509_certificates(ca_pem.encode("ascii")):
            raise BrokerMetadataUnavailable()
    except Exception:
        raise BrokerMetadataUnavailable() from None
    return ca_pem


_BUDGET = threading.local()


@contextmanager
def metadata_reader_budget(seconds: float) -> Iterator[None]:
    """Bound every reader this thread connects inside the block to one deadline.

    The deadline covers connecting and every later operation of those readers:
    each command checks it first, and a watchdog shuts the socket down when it
    passes, so a broker that answers slowly byte by byte cannot outlast it. A
    reader over budget fails with its single safe error and stays unusable.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3.
    """
    if type(seconds) not in (int, float) or not 0 < seconds <= 3600:
        raise ValueError("invalid metadata reader budget")
    previous = getattr(_BUDGET, "deadline", None)
    deadline = time.monotonic() + seconds
    _BUDGET.deadline = deadline if previous is None else min(previous, deadline)
    try:
        yield
    finally:
        _BUDGET.deadline = previous


class _Watchdog:
    """One reader's deadline, @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-2/3."""

    def __init__(self, deadline: float) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        self._deadline = deadline
        self._lock = Lock()
        self._shadow: socket | None = None
        self._done = False
        self._timer = threading.Timer(max(0.0, deadline - time.monotonic()), self._expire)
        self._timer.daemon = True
        self._timer.start()

    def remaining(self) -> float:
        """Seconds left, refusing once spent or released, @spec PROTECTED-HOOK-SOURCE-9."""
        left = self._deadline - time.monotonic()
        if self._done or left <= 0:
            raise BrokerMetadataUnavailable()
        return left

    def watch(self, sock: socket) -> None:
        """Hold a duplicate of the connected socket to shut it down at the deadline.

        A duplicate survives the TLS wrap detaching the original, and shutting
        it down ends the one kernel socket both name. @spec PROTECTED-HOOK-SOURCE-9.
        """
        with self._lock:
            if self._done:
                raise BrokerMetadataUnavailable()
            if self._shadow is not None:
                self._shadow.close()
            self._shadow = sock.dup()

    def _expire(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        with self._lock:
            self._done = True
            if self._shadow is not None:
                try:
                    self._shadow.shutdown(SHUT_RDWR)
                except OSError:
                    pass

    def release(self) -> None:
        """Stop watching for good, @spec PROTECTED-HOOK-SOURCE-9."""
        self._timer.cancel()
        with self._lock:
            self._done = True
            if self._shadow is not None:
                try:
                    self._shadow.close()
                except OSError:
                    pass
                self._shadow = None


@dataclass(frozen=True, slots=True)
class MetadataReaderCredential:
    """Explicit provisioning credential, @spec PROTECTED-HOOK-LANE-2/3."""

    username: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        if (
            not isinstance(self.username, str)
            or not self.username
            or self.username == "default"
            or not isinstance(self.password, str)
            or not self.password
        ):
            raise BrokerMetadataUnavailable() from None


@dataclass(frozen=True, slots=True)
class SourceWriterCredential:
    """Provisioner supplied source writer principal, @spec PROTECTED-HOOK-SOURCE-6."""

    username: str = field(repr=False)
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3."""
        if (
            not isinstance(self.username, str)
            or not self.username
            or self.username == "default"
            or not isinstance(self.password, str)
            or not self.password
        ):
            raise BrokerMetadataUnavailable() from None


class _PinnedConnection(SSLConnection):
    """Socket identity enforcement, @spec PROTECTED-HOOK-LANE-2/3."""

    def __init__(
        self,
        identity: dict[str, Any],
        credential: MetadataReaderCredential | SourceWriterCredential,
        ca_pem: str,
    ) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        self._expected_pin = identity["tls_spki_sha256"]
        self._expected_run_id = identity["run_id"]
        deadline = getattr(_BUDGET, "deadline", None)
        self._watchdog = _Watchdog(deadline) if deadline is not None else None
        timeout = 2.0 if self._watchdog is None else min(2.0, self._watchdog.remaining())
        super().__init__(
            host=identity["endpoint"]["host"],
            port=identity["endpoint"]["port"],
            db=0,
            username=credential.username,
            password=credential.password,
            ssl_ca_data=ca_pem,
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            ssl_check_hostname=True,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            retry=Retry(NoBackoff(), 0),
            retry_on_error=[],
            retry_on_timeout=False,
            protocol=3,
            decode_responses=False,
            health_check_interval=0,
            client_name=None,
            driver_info=None,
            maint_notifications_config=MaintNotificationsConfig(enabled=False),
        )

    def _wrap_socket_with_ssl(self, sock: socket) -> ssl.SSLSocket:
        """@spec PROTECTED-HOOK-LANE-2/3 @spec PROTECTED-HOOK-SOURCE-9."""
        if self._watchdog is not None:
            try:
                self._watchdog.watch(sock)
            except Exception:
                raise ConnectionError("Broker metadata unavailable") from None
        secured: ssl.SSLSocket = super()._wrap_socket_with_ssl(sock)  # type: ignore[no-untyped-call]
        try:
            certificate = secured.getpeercert(binary_form=True)
            if not certificate:
                raise BrokerMetadataUnavailable()
            spki = (
                x509.load_der_x509_certificate(certificate)
                .public_key()
                .public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
            )
            if not hmac.compare_digest(hashlib.sha256(spki).hexdigest(), self._expected_pin):
                raise BrokerMetadataUnavailable()
            return secured
        except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
            secured.close()
            raise ConnectionError("Broker metadata unavailable") from None

    def send_command(self, *args: Any, **kwargs: Any) -> None:
        """Every command first checks the budget, @spec PROTECTED-HOOK-SOURCE-9."""
        if self._watchdog is not None:
            try:
                self._watchdog.remaining()
            except BrokerMetadataUnavailable:
                raise ConnectionError("Broker metadata unavailable") from None
        super().send_command(*args, **kwargs)  # type: ignore[no-untyped-call]

    def disconnect(self, *args: Any, **kwargs: Any) -> None:
        """A disconnected budgeted reader never reconnects, @spec PROTECTED-HOOK-SOURCE-9."""
        try:
            super().disconnect(*args, **kwargs)  # type: ignore[no-untyped-call]
        finally:
            if self._watchdog is not None:
                self._watchdog.release()

    def on_connect_check_health(self, check_health: bool = True) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        try:
            super().on_connect_check_health(check_health=check_health)
            self._identity()
        except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
            self.disconnect()
            raise ConnectionError("Broker metadata unavailable") from None

    def _identity(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        self.send_command("INFO", "server", check_health=False)
        info = parse_info(self.read_response())  # type: ignore[no-untyped-call]
        run_id = info.get("run_id")
        if type(run_id) is not str or run_id != self._expected_run_id:
            self.disconnect()
            raise BrokerMetadataUnavailable() from None
        return run_id


class AuthenticatedMetadataReader:
    """Closed metadata reader, @spec PROTECTED-HOOK-LANE-2/3/SOURCE-6."""

    __slots__ = ("__connection", "__lock", "__closed")
    __connection: _PinnedConnection
    __lock: Lock
    __closed: bool

    def __new__(cls) -> AuthenticatedMetadataReader:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        raise BrokerMetadataUnavailable() from None

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        return "AuthenticatedMetadataReader()"

    @classmethod
    def connect(
        cls, manifest: Manifest, credential: MetadataReaderCredential, ca_pem: str
    ) -> AuthenticatedMetadataReader:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        connection: _PinnedConnection | None = None
        try:
            if type(manifest) is not Manifest or type(credential) is not MetadataReaderCredential:
                raise BrokerMetadataUnavailable()
            credential.__post_init__()
            identity = Manifest(manifest.canonical_bytes).as_dict()["broker_identity"]
            if identity["endpoint"]["host"] != identity["tls_server_name"]:
                raise BrokerMetadataUnavailable()
            trusted_ca_pem(ca_pem)
            connection = _PinnedConnection(identity, credential, ca_pem)
            connection.connect()  # type: ignore[no-untyped-call]
            reader = object.__new__(cls)
            reader.__connection = connection
            reader.__lock = Lock()
            reader.__closed = False
            return reader
        except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
            if connection is not None:
                connection.disconnect()
            raise BrokerMetadataUnavailable() from None

    def __identity(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        if self.__closed:
            raise BrokerMetadataUnavailable()
        return self.__connection._identity()

    def __get(self, key: str) -> bytes | None:
        """@spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
        self.__connection.send_command("GET", key, check_health=False)
        raw: Any = self.__connection.read_response()
        if raw is not None and type(raw) is not bytes:
            raise BrokerMetadataUnavailable()
        return raw

    def read_source(self, agent_id: str, hook: str) -> SourceState:
        """@spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
        try:
            key = _source_key(agent_id, hook)
        except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
            raise BrokerMetadataUnavailable() from None
        with self.__lock:
            try:
                self.__identity()
                raw = self.__get(key)
                if raw is None:
                    return {"floor": 0, "operation_id": None, "active": None}
                return _decode_source(raw)
            except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
                self.__connection.disconnect()
                raise BrokerMetadataUnavailable() from None

    def read_control(self, key: str) -> bytes | None:
        """@spec PROTECTED-HOOK-LANE-3."""
        if type(key) is not str or _CONTROL_KEY.fullmatch(key) is None:
            raise BrokerMetadataUnavailable() from None
        with self.__lock:
            try:
                self.__identity()
                return self.__get(key)
            except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
                self.__connection.disconnect()
                raise BrokerMetadataUnavailable() from None

    def observe(self) -> BrokerObservation:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        with self.__lock:
            try:
                run_id = self.__identity()
                self.__connection.send_command("TIME", check_health=False)
                clock: Any = self.__connection.read_response()
                if (
                    type(clock) is not list
                    or len(clock) != 2
                    or any(type(value) is not bytes for value in clock)
                    or any(re.fullmatch(rb"[0-9]+", value) is None for value in clock)
                ):
                    raise BrokerMetadataUnavailable()
                seconds, microseconds = int(clock[0]), int(clock[1])
                if not 0 <= microseconds < 1000000:
                    raise BrokerMetadataUnavailable()
                return BrokerObservation(run_id, seconds * 1000 + microseconds // 1000)
            except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
                self.__connection.disconnect()
                raise BrokerMetadataUnavailable() from None

    def close(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        with self.__lock:
            if self.__closed:
                return
            self.__closed = True
            try:
                self.__connection.disconnect()
            except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
                raise BrokerMetadataUnavailable() from None


class _PinnedWriterConnection(_PinnedConnection):
    """Writer socket: same pinning and budget, no INFO and never a second connect.

    The source writer role has no INFO authority, so the live run_id is left to
    the control reader that brackets every writer effect.
    @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3.
    """

    _connected_once = False

    def connect_check_health(self, *args: Any, **kwargs: Any) -> None:
        """Connect exactly once; a lost connection stays lost, @spec PROTECTED-HOOK-SOURCE-6."""
        if self._sock:
            return
        if self._connected_once:
            raise ConnectionError("Broker metadata unavailable")
        self._connected_once = True
        super().connect_check_health(*args, **kwargs)

    def on_connect_check_health(self, check_health: bool = True) -> None:
        """Handshake and authentication only, @spec PROTECTED-HOOK-SOURCE-6."""
        try:
            SSLConnection.on_connect_check_health(self, check_health=check_health)
        except Exception:
            self.disconnect()
            raise ConnectionError("Broker metadata unavailable") from None


class _FenceClient:
    """The one EVAL surface SourceFence needs, over the pinned writer socket.

    @spec PROTECTED-HOOK-SOURCE-6.
    """

    __slots__ = ("_connection",)

    def __init__(self, connection: _PinnedWriterConnection) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        self._connection = connection

    def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        self._connection.send_command("EVAL", script, numkeys, *args, check_health=False)
        return self._connection.read_response()


_FENCE_REFUSALS = (SourceFenceConflict, SourceFenceExhausted, SourceFenceInvalid)


class AuthenticatedSourceWriter:
    """Closed source writer: reserve, ordinary publication and close only.

    It validates input, pins TLS and authenticates exactly as the metadata
    reader does and honors ``metadata_reader_budget``, but sends no INFO and
    never reconnects: any connection loss leaves it permanently unusable. A
    fence refusal (floor or operation mismatch, exhaustion, malformed record)
    is raised as the fence raises it; every other failure is the single safe
    error. @spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-SOURCE-7
    @spec PROTECTED-HOOK-LANE-3.
    """

    __slots__ = ("__connection", "__fence", "__lock", "__closed")
    __connection: _PinnedWriterConnection
    __fence: SourceFence
    __lock: Lock
    __closed: bool

    def __new__(cls) -> AuthenticatedSourceWriter:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        raise BrokerMetadataUnavailable() from None

    def __repr__(self) -> str:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        return "AuthenticatedSourceWriter()"

    @classmethod
    def connect(
        cls, manifest: Manifest, credential: SourceWriterCredential, ca_pem: str
    ) -> AuthenticatedSourceWriter:
        """@spec PROTECTED-HOOK-SOURCE-6 @spec PROTECTED-HOOK-LANE-3."""
        connection: _PinnedWriterConnection | None = None
        try:
            if type(manifest) is not Manifest or type(credential) is not SourceWriterCredential:
                raise BrokerMetadataUnavailable()
            credential.__post_init__()
            identity = Manifest(manifest.canonical_bytes).as_dict()["broker_identity"]
            if identity["endpoint"]["host"] != identity["tls_server_name"]:
                raise BrokerMetadataUnavailable()
            trusted_ca_pem(ca_pem)
            connection = _PinnedWriterConnection(identity, credential, ca_pem)
            connection.connect()  # type: ignore[no-untyped-call]
            writer = object.__new__(cls)
            writer.__connection = connection
            writer.__fence = SourceFence(_FenceClient(connection))  # type: ignore[arg-type]
            writer.__lock = Lock()
            writer.__closed = False
            return writer
        except Exception:
            if connection is not None:
                connection.disconnect()
            raise BrokerMetadataUnavailable() from None

    def reserve_and_revoke(
        self,
        agent_id: str,
        hook: str,
        expected_floor: int,
        operation_id: str,
        min_generation: int,
    ) -> int:
        """@spec PROTECTED-HOOK-SOURCE-6/7."""
        with self.__lock:
            if self.__closed:
                raise BrokerMetadataUnavailable()
            try:
                return self.__fence.reserve_and_revoke(
                    agent_id, hook, expected_floor, operation_id, min_generation
                )
            except _FENCE_REFUSALS:
                raise
            except Exception:
                self.__connection.disconnect()
                raise BrokerMetadataUnavailable() from None

    def publish_ordinary(
        self,
        agent_id: str,
        hook: str,
        generation: int,
        operation_id: str,
        policy_fingerprint: str,
    ) -> bool:
        """@spec PROTECTED-HOOK-SOURCE-3/6/7."""
        with self.__lock:
            if self.__closed:
                raise BrokerMetadataUnavailable()
            try:
                return self.__fence.publish_ordinary(
                    agent_id, hook, generation, operation_id, policy_fingerprint
                )
            except _FENCE_REFUSALS:
                raise
            except Exception:
                self.__connection.disconnect()
                raise BrokerMetadataUnavailable() from None

    def close(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-6."""
        with self.__lock:
            if self.__closed:
                return
            self.__closed = True
            try:
                self.__connection.disconnect()
            except Exception:
                raise BrokerMetadataUnavailable() from None
