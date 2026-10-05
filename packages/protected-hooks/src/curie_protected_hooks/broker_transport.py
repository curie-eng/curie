"""Authenticated metadata transport, @spec PROTECTED-HOOK-LANE-2/3/SOURCE-6."""

from __future__ import annotations

import hashlib
import hmac
import re
import ssl
from dataclasses import dataclass, field
from socket import socket
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
from curie_protected_hooks.source_fence import SourceState, _decode_source, _source_key

# @spec PROTECTED-HOOK-LANE-2/3
_CERTIFICATES = re.compile(
    r"\s*(?:-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\r\n]+"
    r"-----END CERTIFICATE-----\s*)+",
    re.ASCII,
)


def trusted_ca_pem(ca_pem: str) -> str:
    """Validated broker CA certificates or the single safe refusal.

    @spec PROTECTED-HOOK-LANE-2/3 @spec PROTECTED-HOOK-SOURCE-9.
    """
    try:
        if not isinstance(ca_pem, str) or _CERTIFICATES.fullmatch(ca_pem) is None:
            raise BrokerMetadataUnavailable()
        if not x509.load_pem_x509_certificates(ca_pem.encode("ascii")):
            raise BrokerMetadataUnavailable()
    except Exception:
        raise BrokerMetadataUnavailable() from None
    return ca_pem


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


class _PinnedConnection(SSLConnection):
    """Socket identity enforcement, @spec PROTECTED-HOOK-LANE-2/3."""

    def __init__(
        self, identity: dict[str, Any], credential: MetadataReaderCredential, ca_pem: str
    ) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        self._expected_pin = identity["tls_spki_sha256"]
        self._expected_run_id = identity["run_id"]
        super().__init__(
            host=identity["endpoint"]["host"],
            port=identity["endpoint"]["port"],
            db=0,
            username=credential.username,
            password=credential.password,
            ssl_ca_data=ca_pem,
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            ssl_check_hostname=True,
            socket_timeout=2,
            socket_connect_timeout=2,
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
        """@spec PROTECTED-HOOK-LANE-2/3."""
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
        except Exception:
            secured.close()
            raise ConnectionError("Broker metadata unavailable") from None

    def on_connect_check_health(self, check_health: bool = True) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        try:
            super().on_connect_check_health(check_health=check_health)
            self._identity()
        except Exception:
            self.disconnect()  # type: ignore[no-untyped-call]
            raise ConnectionError("Broker metadata unavailable") from None

    def _identity(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        self.send_command("INFO", "server", check_health=False)  # type: ignore[no-untyped-call]
        info = parse_info(self.read_response())  # type: ignore[no-untyped-call]
        run_id = info.get("run_id")
        if type(run_id) is not str or run_id != self._expected_run_id:
            self.disconnect()  # type: ignore[no-untyped-call]
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
        except Exception:
            if connection is not None:
                connection.disconnect()  # type: ignore[no-untyped-call]
            raise BrokerMetadataUnavailable() from None

    def __identity(self) -> str:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        if self.__closed:
            raise BrokerMetadataUnavailable()
        return self.__connection._identity()

    def __get(self, key: str) -> bytes | None:
        """@spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
        self.__connection.send_command("GET", key, check_health=False)  # type: ignore[no-untyped-call]
        raw: Any = self.__connection.read_response()
        if raw is not None and type(raw) is not bytes:
            raise BrokerMetadataUnavailable()
        return raw

    def read_source(self, agent_id: str, hook: str) -> SourceState:
        """@spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
        try:
            key = _source_key(agent_id, hook)
        except Exception:
            raise BrokerMetadataUnavailable() from None
        with self.__lock:
            try:
                self.__identity()
                raw = self.__get(key)
                if raw is None:
                    return {"floor": 0, "operation_id": None, "active": None}
                return _decode_source(raw)
            except Exception:
                self.__connection.disconnect()  # type: ignore[no-untyped-call]
                raise BrokerMetadataUnavailable() from None

    def read_control(self, key: str) -> bytes | None:
        """@spec PROTECTED-HOOK-LANE-3."""
        if type(key) is not str or _CONTROL_KEY.fullmatch(key) is None:
            raise BrokerMetadataUnavailable() from None
        with self.__lock:
            try:
                self.__identity()
                return self.__get(key)
            except Exception:
                self.__connection.disconnect()  # type: ignore[no-untyped-call]
                raise BrokerMetadataUnavailable() from None

    def observe(self) -> BrokerObservation:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        with self.__lock:
            try:
                run_id = self.__identity()
                self.__connection.send_command("TIME", check_health=False)  # type: ignore[no-untyped-call]
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
            except Exception:
                self.__connection.disconnect()  # type: ignore[no-untyped-call]
                raise BrokerMetadataUnavailable() from None

    def close(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        with self.__lock:
            if self.__closed:
                return
            self.__closed = True
            try:
                self.__connection.disconnect()  # type: ignore[no-untyped-call]
            except Exception:
                raise BrokerMetadataUnavailable() from None
