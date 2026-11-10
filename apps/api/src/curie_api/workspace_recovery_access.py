"""Administrator recovery metadata and lease-bounded, verified object reads."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import tempfile
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, BinaryIO, Literal
from urllib.parse import parse_qs, urlsplit

import anyio
import httpx
from curie_internal.workspace_objects import WorkspaceObjectSigner
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from . import workspace_recovery as ledger
from .config import Settings
from .forges.errors import ForgeError
from .forges.registry import ForgeRegistry
from .forges.trackers import code_host_for_repository, work_item_policy
from .models import ExecutionRequest, WorkItem
from .models import WorkspaceRecoveryCheckpoint as Checkpoint
from .models import WorkspaceRecoveryGroup as Group
from .models import WorkspaceRecoveryInstallation as Installation
from .models import WorkspaceRecoveryObject as RecoveryObject
from .models import WorkspaceRecoveryReader as ReaderLease
from .schemas import workspace_recovery as wire
from .workitems.lifecycle import database_now, lock_work_item
from .workspace_policy import repository_allowed

ObjectKind = Literal["base", "patch", "manifest"]
logger = logging.getLogger(__name__)
_CHUNK_BYTES = 65536


def _item_identity(item: WorkItem) -> tuple[object, ...]:
    return (
        item.code_host_kind,
        item.code_host_host,
        item.repository_project_id,
        item.repository_path,
        item.tracker_kind,
        item.tracker_host,
        item.tracker_scope_id,
        item.tracker_issue_id,
        item.code_host_installation_id,
    )


def _group_identity(group: Group | None) -> tuple[str, ...] | None:
    if group is None:
        return None
    return (
        group.repository_provider,
        group.repository_host,
        group.repository_project_id,
        group.repository_path,
        group.base_commit,
        group.target_branch,
    )


def _repository(item: WorkItem | Group) -> wire.RecoveryRepository:
    if isinstance(item, WorkItem):
        return wire.RecoveryRepository(
            provider=item.code_host_kind,
            host=item.code_host_host,
            project_id=item.repository_project_id,
            path=item.repository_path,
        )
    return wire.RecoveryRepository(
        provider=item.repository_provider,
        host=item.repository_host,
        project_id=item.repository_project_id,
        path=item.repository_path,
    )


async def _observe(
    session: AsyncSession,
    settings: Settings,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    work_item_id: uuid.UUID,
) -> tuple[tuple[object, ...], tuple[str, ...] | None]:
    if not settings.workspace_recovery_enabled:
        raise ledger.RecoveryRefusal("disabled", 503)
    item = await session.get(WorkItem, work_item_id)
    if item is None:
        raise ledger.RecoveryRefusal("work_item_missing", 404)
    repository, issue = item.repository, item.tracker_issue
    identity, installation_id = _item_identity(item), item.code_host_installation_id
    group = await session.get(Group, work_item_id)
    retained = _group_identity(group)
    await session.rollback()
    if not repository_allowed(settings, repository):
        raise ledger.RecoveryRefusal("repository_forbidden", 403)
    try:
        registry.code_host_for(repository.kind, repository.host)
        policy = work_item_policy(registry, issue, repository)
        if retained is not None:
            if retained[:3] != (repository.kind, repository.host, repository.project_id):
                raise ledger.RecoveryRefusal("retained_identity", 403)
            if policy is not None and policy.bases:
                bases = policy.bases
            else:
                installations = (
                    {repository.path: installation_id} if installation_id is not None else None
                )
                host = code_host_for_repository(
                    registry, client, repository, installations=installations
                )
                current = await host.resolve_repository(repository.project_id)
                if current != repository or current.path != repository.path:
                    raise ledger.RecoveryRefusal("repository_identity", 403)
                if current.default_branch is None:
                    raise ledger.RecoveryRefusal("default_unavailable", 503)
                bases = (current.default_branch,)
            if retained[5] not in bases:
                raise ledger.RecoveryRefusal("base_forbidden", 403)
    except ForgeError:
        raise ledger.RecoveryRefusal("provider_unavailable", 503) from None
    return identity, retained


@dataclass
class _Selection:
    item: WorkItem
    group: Group | None
    checkpoints: list[Checkpoint]
    objects: list[RecoveryObject]
    sources: dict[uuid.UUID, ExecutionRequest]
    now: datetime


async def _select(
    session: AsyncSession,
    settings: Settings,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    work_item_id: uuid.UUID,
    checkpoint_id: uuid.UUID | None = None,
) -> _Selection:
    identity, retained = await _observe(session, settings, registry, client, work_item_id)
    item = await lock_work_item(session, work_item_id)
    if item is None:
        raise ledger.RecoveryRefusal("work_item_missing", 404)
    if _item_identity(item) != identity:
        raise ledger.RecoveryRefusal("repository_identity", 403)
    if (
        await session.scalar(select(Installation.id).where(Installation.id == 1).with_for_update())
        is None
    ):
        raise ledger.RecoveryRefusal("installation_unavailable", 503)
    group = await session.scalar(
        select(Group)
        .where(Group.work_item_id == item.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if _group_identity(group) != retained:
        raise ledger.RecoveryRefusal("retained_identity", 403)
    checkpoints = list(
        await session.scalars(
            select(Checkpoint)
            .where(
                Checkpoint.work_item_id == item.id,
                *([Checkpoint.id == checkpoint_id] if checkpoint_id is not None else []),
            )
            .order_by(Checkpoint.id)
            .limit(4)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if checkpoint_id is not None and not checkpoints:
        raise ledger.RecoveryRefusal("checkpoint_missing", 404)
    objects = list(
        await session.scalars(
            select(RecoveryObject)
            .where(RecoveryObject.work_item_id == item.id)
            .order_by(RecoveryObject.kind, RecoveryObject.id)
            .limit(8)
        )
    )
    if len(checkpoints) > 3 or len(objects) > 7:
        raise ledger.RecoveryRefusal("retained_capacity")
    sources = {
        source.id: source
        for source in await session.scalars(
            select(ExecutionRequest).where(
                ExecutionRequest.id.in_([cp.source_request_id for cp in checkpoints])
            )
        )
    }
    if any(
        cp.source_request_id not in sources or sources[cp.source_request_id].work_item_id != item.id
        for cp in checkpoints
    ):
        raise ledger.RecoveryRefusal("checkpoint_source_identity", 403)
    return _Selection(item, group, checkpoints, objects, sources, await database_now(session))


def _metadata(selection: _Selection, cp: Checkpoint) -> wire.RecoveryPublicCheckpoint:
    if selection.group is None:
        raise ledger.RecoveryRefusal("retained_identity", 403)
    objects = [obj for obj in selection.objects if obj.kind == "base" or obj.checkpoint_id == cp.id]
    source = selection.sources[cp.source_request_id]
    available = (
        cp.status == "complete"
        and cp.expires_at is not None
        and cp.expires_at > selection.now
        and {obj.kind for obj in objects} == {"base", "patch", "manifest"}
        and all(obj.status == "complete" for obj in objects)
    )
    captured_at = cp.completed_at or cp.capture_deadline - timedelta(seconds=60)
    try:
        return wire.RecoveryPublicCheckpoint.model_validate(
            {
                "checkpoint_id": cp.id,
                "work_item_id": cp.work_item_id,
                "source_request_id": cp.source_request_id,
                "repository": _repository(selection.group),
                "base_commit": selection.group.base_commit,
                "target_branch": selection.group.target_branch,
                "source_sequence": source.sequence,
                "source_status": source.status,
                "completed_at": cp.completed_at,
                "expires_at": cp.expires_at,
                "age_seconds": max(0, (selection.now - captured_at).total_seconds()),
                "status": cp.status,
                "available": available,
                "total_bytes": sum(obj.size_bytes for obj in objects),
                "objects": [
                    {"kind": obj.kind, "size_bytes": obj.size_bytes, "sha256": obj.sha256}
                    for obj in objects
                ],
            }
        )
    except ValidationError:
        raise ledger.RecoveryRefusal("metadata_invalid") from None


async def list_metadata(
    session: AsyncSession,
    settings: Settings,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    work_item_id: uuid.UUID,
) -> wire.RecoveryPublicList:
    selection = await _select(session, settings, registry, client, work_item_id)
    result = wire.RecoveryPublicList(
        work_item_id=selection.item.id,
        repository=_repository(selection.group or selection.item),
        capture_outcome=selection.item.workspace_recovery_outcome,
        capture_outcome_at=selection.item.workspace_recovery_outcome_at,
        checkpoints=[_metadata(selection, cp) for cp in selection.checkpoints],
    )
    await session.commit()
    return result


async def checkpoint_metadata(
    session: AsyncSession,
    settings: Settings,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    work_item_id: uuid.UUID,
    checkpoint_id: uuid.UUID,
) -> wire.RecoveryPublicCheckpoint:
    selection = await _select(session, settings, registry, client, work_item_id, checkpoint_id)
    result = _metadata(selection, selection.checkpoints[0])
    await session.commit()
    return result


class _RecoverySigner(WorkspaceObjectSigner):
    def __init__(self, *, client: Any, bucket: str, key: str) -> None:
        super().__init__(client=client, bucket=bucket, prefix="private")
        self._expected_key = key

    def _key(self, key: str) -> str:
        if key != self._expected_key:
            raise ValueError("recovery object identity")
        return super()._key(key)


@dataclass(frozen=True)
class _Object:
    key: str
    size_bytes: int
    sha256: str


class _SigningLogFilter(logging.Filter):
    def __init__(self) -> None:
        super().__init__()
        self._thread = threading.get_ident()

    def filter(self, record: logging.LogRecord) -> bool:
        return record.thread != self._thread


@contextmanager
def _private_signing_logs() -> Iterator[None]:
    log_filter = _SigningLogFilter()
    loggers = [logging.getLogger(name) for name in ("botocore.auth", "botocore.regions")]
    for signing_logger in loggers:
        signing_logger.addFilter(log_filter)
    try:
        yield
    finally:
        for signing_logger in loggers:
            signing_logger.removeFilter(log_filter)


def _signed_url(settings: Settings, key: str, deadline: float, expires_at: datetime) -> str:
    import boto3
    from botocore.client import Config as BotoConfig

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key or None,
        aws_secret_access_key=settings.s3_secret_key or None,
        region_name=settings.s3_region,
        config=BotoConfig(
            connect_timeout=10,
            read_timeout=10,
            retries={"total_max_attempts": 1, "mode": "standard"},
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )
    try:
        ttl = min(settings.workspace_reference_ttl_seconds, math.floor(deadline - time.monotonic()))
        if ttl < 1:
            raise TimeoutError
        signer = _RecoverySigner(client=client, bucket=settings.workspace_bucket, key=key)
        with _private_signing_logs():
            url = signer.presign_get(key, expires_seconds=ttl)
        query = parse_qs(urlsplit(url).query)
        signed_at = datetime.strptime(query["X-Amz-Date"][0], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        if signed_at + timedelta(seconds=int(query["X-Amz-Expires"][0])) > expires_at:
            raise TimeoutError
        return url
    finally:
        client.close()


async def _release(
    factory: async_sessionmaker[AsyncSession],
    lease: wire.RecoveryReader,
) -> None:
    with anyio.CancelScope(shield=True):
        try:
            async with asyncio.timeout(5):
                async with factory() as session:
                    await ledger.release_reader(
                        session,
                        wire.RecoveryReleaseReader(
                            lease_id=lease.lease_id,
                            holder_id=lease.holder_id,
                        ),
                    )
                    await session.commit()
        except Exception:  # noqa: BLE001
            logger.warning("workspace_recovery.reader_release_failed lease_id=%s", lease.lease_id)


def _check_deadline(deadline: float) -> None:
    if asyncio.get_running_loop().time() >= deadline:
        raise TimeoutError


async def _verify_spool(
    settings: Settings,
    obj: _Object,
    spool: BinaryIO,
    deadline: float,
    expires_at: datetime,
) -> None:
    async with asyncio.timeout_at(deadline):
        url = await asyncio.to_thread(_signed_url, settings, obj.key, deadline, expires_at)
        _check_deadline(deadline)
        async with httpx.AsyncHTTPTransport(retries=0) as transport:
            request = httpx.Request(
                "GET",
                url,
                extensions={
                    "timeout": {
                        "connect": 10.0,
                        "read": 10.0,
                        "write": 10.0,
                        "pool": 10.0,
                    }
                },
            )
            response = await transport.handle_async_request(request)
            try:
                if response.status_code != 200:
                    raise ledger.RecoveryRefusal("object_unavailable", 502)
                digest, size = hashlib.sha256(), 0
                async for chunk in response.aiter_raw(_CHUNK_BYTES):
                    _check_deadline(deadline)
                    size += len(chunk)
                    if size > obj.size_bytes:
                        raise ledger.RecoveryRefusal("object_corrupt", 502)
                    spool.write(chunk)
                    digest.update(chunk)
                _check_deadline(deadline)
                if size != obj.size_bytes or digest.hexdigest() != obj.sha256:
                    raise ledger.RecoveryRefusal("object_corrupt", 502)
                spool.seek(0)
                _check_deadline(deadline)
            finally:
                await response.aclose()


class VerifiedRecoveryResponse(StreamingResponse):
    def __init__(
        self,
        spool: BinaryIO,
        lease: wire.RecoveryReader,
        factory: async_sessionmaker[AsyncSession],
        deadline: float,
        size: int,
    ) -> None:
        self._spool, self._lease, self._factory, self._deadline = spool, lease, factory, deadline
        super().__init__(
            self._chunks(),
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(size),
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def _chunks(self) -> AsyncIterator[bytes]:
        while True:
            _check_deadline(self._deadline)
            chunk = self._spool.read(_CHUNK_BYTES)
            _check_deadline(self._deadline)
            if not chunk:
                return
            yield chunk

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            async with asyncio.timeout_at(self._deadline):
                await super().__call__(scope, receive, send)
        finally:
            try:
                self._spool.close()
            finally:
                await _release(self._factory, self._lease)


async def download(
    session: AsyncSession,
    settings: Settings,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    factory: async_sessionmaker[AsyncSession],
    work_item_id: uuid.UUID,
    checkpoint_id: uuid.UUID,
    kind: ObjectKind,
) -> VerifiedRecoveryResponse:
    selection = await _select(session, settings, registry, client, work_item_id, checkpoint_id)
    obj = next(
        (
            obj
            for obj in selection.objects
            if obj.kind == kind and (kind == "base" or obj.checkpoint_id == checkpoint_id)
        ),
        None,
    )
    if obj is None:
        raise ledger.RecoveryRefusal("checkpoint_incomplete")
    expected = (
        f"workspace-recovery/retained/{work_item_id}/{obj.id}/base"
        if kind == "base"
        else f"workspace-recovery/checkpoints/{checkpoint_id}/{kind}"
    )
    limits = {"base": settings.workspace_max_archive_bytes, "patch": 900000, "manifest": 1048576}
    if obj.key != expected or not 0 <= obj.size_bytes <= limits[kind]:
        raise ledger.RecoveryRefusal("object_identity")
    descriptor = _Object(obj.key, obj.size_bytes, obj.sha256)
    lease = await ledger.acquire_reader(
        session,
        settings,
        wire.RecoveryAcquireReader(
            work_item_id=work_item_id,
            checkpoint_id=checkpoint_id,
            holder_id=uuid.uuid4().hex,
        ),
    )
    spool: BinaryIO | None = None
    try:
        await session.commit()
        observed = (
            await session.execute(
                select(
                    ReaderLease.expires_at,
                    func.clock_timestamp(),
                ).where(ReaderLease.id == lease.lease_id, ReaderLease.holder_id == lease.holder_id)
            )
        ).one_or_none()
        if observed is None:
            raise ledger.RecoveryRefusal("reader_expired")
        remaining = (observed[0] - observed[1]).total_seconds()
        expires_at = observed[0]
        deadline = asyncio.get_running_loop().time() + remaining
        await session.rollback()
        _check_deadline(deadline)
        spool = tempfile.TemporaryFile(mode="w+b")
        os.fchmod(spool.fileno(), 0o600)
        await _verify_spool(settings, descriptor, spool, deadline, expires_at)
        return VerifiedRecoveryResponse(spool, lease, factory, deadline, descriptor.size_bytes)
    except BaseException as error:
        try:
            if spool is not None:
                spool.close()
        finally:
            try:
                with anyio.CancelScope(shield=True):
                    async with asyncio.timeout(5):
                        await session.rollback()
            finally:
                await _release(factory, lease)
        if isinstance(error, TimeoutError):
            raise ledger.RecoveryRefusal("reader_expired") from None
        if isinstance(error, Exception) and not isinstance(error, ledger.RecoveryRefusal):
            raise ledger.RecoveryRefusal("object_unavailable", 502) from None
        raise
