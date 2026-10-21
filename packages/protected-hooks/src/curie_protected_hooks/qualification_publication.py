"""Readiness writer for the unchanged v1 grammar.

@spec PROTECTED-COMMISSIONING-6 PROTECTED-HOOK-LANE-2/3.

ADR 0236 lets the verifier construct this publisher only after the campaign is
accepted. This module performs no measurement and decides no acceptance; it
writes the record the existing production readers already require, holding the
manifest's ``credential_refs.verifier`` credential under its unchanged recipe
(``SET`` on ``protected:control:readiness:*`` only).

The session dials the broker the manifest names, pins its TLS name, SPKI and
``run_id``, and never reconnects. It has no loopback assumption: the same
pinned identity decides whether the broker is reached from inside or outside
the cluster.

Every write is a compare and set on the exact stored bytes: ``SET NX`` for the
first Readiness and ``SET IFEQ`` for a renewal, so a second first write or a
stale renewal refuses without a script or a read-modify-write window. Before
it writes, the publisher re-reads the stored manifest and Qualification,
checks the written bytes with the unchanged ``validate_authority`` reader at
live broker time, and runs the caller's design decision 7 reobservation.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any, NoReturn, Self

from redis import asyncio as aioredis

from .authority_records import (
    AuthorityRecordInvalid,
    Manifest,
    Qualification,
    Readiness,
    _scalar,
    parse_manifest,
    parse_qualification,
    validate_authority,
)
from .broker_transport import trusted_ca_pem
from .worker_transport import WorkerCredential, _AsyncConnection, _AsyncPool, _Lifetime

__all__ = ["PublicationRefused", "ReadinessPublisher"]

# Fixed refusal reasons. None names a member, a value or a credential.
_REASONS = (
    "lifetime",
    "manifest",
    "principal",
    "qualification",
    "reobservation",
    "run_id",
    "stale",
    "unavailable",
)


class PublicationRefused(Exception):
    """Redacted refusal carrying one fixed reason, @spec PROTECTED-COMMISSIONING-6."""

    def __init__(self, reason: str = "unavailable") -> None:
        """@spec PROTECTED-COMMISSIONING-6."""
        if reason not in _REASONS:
            raise ValueError("unknown publication refusal")
        super().__init__("Protected authority publication refused: " + reason)
        self.reason = reason


def _refuse(reason: str = "unavailable") -> NoReturn:
    """Raise a detached refusal, even inside a caller's active exception."""
    error = PublicationRefused(reason)
    try:
        raise error from None
    finally:
        error.__context__ = None


def _keys(manifest: Manifest, qualification_generation: str) -> tuple[str, str, str]:
    m = manifest.as_dict()
    return (
        "protected:control:manifest:" + manifest.digest,
        "protected:control:qualification:" + m["qualification_id"] + ":" + qualification_generation,
        "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"],
    )


class ReadinessPublisher:
    """One pinned, non-reconnecting session as the manifest's verifier.

    @spec PROTECTED-COMMISSIONING-6 PROTECTED-HOOK-LANE-3.
    """

    __slots__ = ("__lifetime", "__lock", "__manifest", "__maximum", "__pool", "__redis")
    __lifetime: _Lifetime
    __lock: asyncio.Lock
    __manifest: Manifest
    __maximum: int
    __pool: _AsyncPool
    __redis: aioredis.Redis

    def __new__(cls) -> Self:
        """Only the verified factory constructs a publisher, @spec PROTECTED-COMMISSIONING-6."""
        _refuse()

    def __repr__(self) -> str:
        """No credential or endpoint in representations, @spec PROTECTED-COMMISSIONING-6."""
        return "ReadinessPublisher()"

    @classmethod
    async def connect(
        cls,
        manifest: Manifest,
        credential: WorkerCredential,
        ca_pem: str,
        *,
        trusted_max_readiness_ms: int,
    ) -> Self:
        """Authenticate as ``credential_refs.verifier`` on the manifest's pinned broker.

        @spec PROTECTED-COMMISSIONING-6 PROTECTED-HOOK-LANE-3.
        """
        lifetime = _Lifetime()
        pool: _AsyncPool | None = None
        try:
            if type(manifest) is not Manifest or type(credential) is not WorkerCredential:
                _refuse()
            if (
                type(trusted_max_readiness_ms) is not int
                or not 0 < trusted_max_readiness_ms <= 9007199254740991
            ):
                _refuse("lifetime")
            trusted = Manifest(manifest.canonical_bytes)
            m = trusted.as_dict()
            if credential.username != m["credential_refs"]["verifier"]["id"]:
                _refuse("principal")
            credential.__post_init__()
            identity = m["broker_identity"]
            if identity["endpoint"]["host"] != identity["tls_server_name"]:
                _refuse()
            trusted_ca_pem(ca_pem)
            pool = _AsyncPool(
                connection_class=_AsyncConnection,
                max_connections=1,
                identity=identity,
                credential=credential,
                ca_pem=ca_pem,
                lifetime=lifetime,
                protocol=2,
                decode_responses=True,
            )
            redis = aioredis.Redis(connection_pool=pool)
            # The handshake authenticates, pins TLS and checks the live run_id.
            await redis.execute_command("PING")  # type: ignore[no-untyped-call]
            publisher = object.__new__(cls)
            publisher.__lifetime = lifetime
            publisher.__lock = asyncio.Lock()
            publisher.__manifest = trusted
            publisher.__maximum = trusted_max_readiness_ms
            publisher.__pool = pool
            publisher.__redis = redis
            return publisher
        except BaseException as error:  # noqa: BLE001  Close the socket before suppressing transport details.
            lifetime.fail()
            if pool is not None:
                await pool.disconnect()
            if not isinstance(error, Exception):
                raise
            if isinstance(error, PublicationRefused):
                _refuse(error.reason)
            _refuse()

    async def __execute(self, *args: Any, **options: Any) -> Any:
        try:
            self.__lifetime.check()
            return await self.__redis.execute_command(*args, **options)  # type: ignore[no-untyped-call]
        except Exception:  # noqa: BLE001  The publication surface has one redacted refusal.
            _refuse()

    async def __broker_time_ms(self) -> int:
        info = await self.__execute("INFO", "server")
        run_id = self.__manifest.as_dict()["broker_identity"]["run_id"]
        if type(info) is not dict or info.get("run_id") != run_id:
            self.__lifetime.fail()
            _refuse("run_id")
        clock = await self.__execute("TIME")
        try:
            seconds, micros = (int(value) for value in clock)
        except (TypeError, ValueError):
            _refuse()
        if seconds < 0 or not 0 <= micros < 1000000:
            _refuse()
        return seconds * 1000 + micros // 1000

    async def publish(
        self,
        qualification: Qualification,
        *,
        lifetime_ms: int,
        measurement_record_id: str,
        reobserve: Callable[[], Awaitable[None]],
        expected: Readiness | None,
    ) -> Readiness:
        """Write Readiness bound to the manifest and the stored Qualification.

        ``expected`` is ``None`` for the first Readiness of this runtime
        generation, otherwise the exact record a renewal replaces.
        ``reobserve`` is the design decision 7 ACL principal comparison; any
        exception from it refuses. @spec PROTECTED-COMMISSIONING-6.
        """
        async with self.__lock:
            if type(qualification) is not Qualification:
                _refuse("qualification")
            if expected is not None and type(expected) is not Readiness:
                _refuse("stale")
            if type(lifetime_ms) is not int or lifetime_ms <= 0 or lifetime_ms > self.__maximum:
                _refuse("lifetime")
            try:
                _scalar(measurement_record_id, "opaque_ref")
            except (AuthorityRecordInvalid, ValueError):
                _refuse()
            manifest = self.__manifest
            m = manifest.as_dict()
            q = qualification.as_dict()
            manifest_key, qualification_key, readiness_key = _keys(
                manifest, q["qualification_generation"]
            )
            stored_manifest = await self.__execute("GET", manifest_key, NEVER_DECODE=True)
            try:
                if (
                    type(stored_manifest) is not bytes
                    or parse_manifest(stored_manifest).canonical_bytes != manifest.canonical_bytes
                ):
                    _refuse("manifest")
            except AuthorityRecordInvalid:
                _refuse("manifest")
            stored = await self.__execute("GET", qualification_key, NEVER_DECODE=True)
            try:
                if (
                    type(stored) is not bytes
                    or parse_qualification(stored).canonical_bytes != qualification.canonical_bytes
                ):
                    _refuse("qualification")
            except AuthorityRecordInvalid:
                _refuse("qualification")
            now = await self.__broker_time_ms()
            value = {
                "schema_version": 1,
                "manifest_digest": manifest.digest,
                "runtime_id": m["runtime_id"],
                "runtime_generation": m["runtime_generation"],
                "qualification_id": m["qualification_id"],
                "qualification_generation": q["qualification_generation"],
                "broker_identity": m["broker_identity"],
                "guard_identity": m["guard_identity"],
                "verifier_identity": m["credential_refs"]["verifier"],
                "issued_at_ms": str(now),
                "expires_at_ms": str(now + lifetime_ms),
                "measurement_record_id": measurement_record_id,
            }
            try:
                readiness = Readiness(_encode(value))
                # The unchanged reader decides whether these bytes are authority.
                validate_authority(
                    manifest,
                    qualification,
                    readiness,
                    broker_identity=m["broker_identity"],
                    broker_now_ms=now,
                    trusted_max_readiness_ms=self.__maximum,
                )
            except AuthorityRecordInvalid:
                _refuse("qualification")
            try:
                await reobserve()
            except Exception:  # noqa: BLE001  Any reobservation failure or difference refuses.
                _refuse("reobservation")
            if expected is None:
                condition: tuple[Any, ...] = ("NX",)
            else:
                condition = ("IFEQ", expected.canonical_bytes)
            written = await self.__execute(
                "SET", readiness_key, readiness.canonical_bytes, *condition
            )
            if written is not True:
                _refuse("stale")
            return readiness

    async def close(self) -> None:
        """End the session without a reconnect path, @spec PROTECTED-COMMISSIONING-6."""
        self.__lifetime.fail()
        await self.__pool.disconnect()


def _encode(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")
