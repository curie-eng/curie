"""Immutable bundle storage: the object-store port and its S3/MinIO backing.

Bundles are written once per (agent, version) under a deterministic key and never
mutated (B2 constraint: immutability = write-once key, no dedup/GC/signing). boto3
is synchronous, so each call is offloaded to a worker thread to keep the event
loop free.

## The port

``ObjectStore`` (this module) is the storage port: the five operations the bundle
pipeline needs, plus the **write-once/no-mutation key discipline** promoted here
from convention into the contract (see the Protocol docstring). ``BundleStore``
is the one concrete backing today, the S3/MinIO client. Consumers
(``deps``/``gitflow``/``deploy``) type against ``ObjectStore``, so a future
non-S3 backend (GCS-native, Azure Blob) is a drop-in that satisfies the Protocol
rather than a rewrite of every call site. The GCS/Azure adapter itself is
deliberately NOT built (no non-S3 demand today; ADR-0007, ADR-0026) — this
extraction only makes it a drop-in when that demand arrives.
"""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from aci_protocol.s3 import build_s3_client as shared_build_s3_client
from botocore.exceptions import ClientError
from starlette.concurrency import run_in_threadpool

from .config import Settings

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@runtime_checkable
class ObjectStore(Protocol):
    """The storage port: immutable, key-addressed bundle bytes.

    Contract (part of the port, not backend-specific):

    - **Write-once / no-mutation.** A ``put`` to a key that already exists is a
      programming error, not a supported update: bundles are addressed by a
      deterministic ``(agent, version)`` key and never mutated. Callers derive
      keys so a rewrite never happens; a backend need not enforce it, but it must
      not *rely* on mutation semantics either. There is no delete/GC/signing in
      the port.
    - **Bytes in, bytes out.** ``get`` returns exactly the bytes ``put`` stored.
    - **Bucket/namespace bootstrap.** ``ensure_bucket`` is idempotent.

    A second implementation (GCS, Azure Blob) satisfies this Protocol; it is not
    required to be S3/boto3, only to honor these semantics.
    """

    async def ensure_bucket(self) -> None:
        """Create the bundle bucket/namespace if absent (idempotent)."""
        ...

    async def exists(self, key: str) -> bool:
        """True if an object is stored at ``key``."""
        ...

    async def put(self, key: str, data: bytes, content_type: str) -> None:
        """Store ``data`` at ``key`` (write-once; see the class contract)."""
        ...

    async def get(self, key: str) -> bytes:
        """Return the bytes stored at ``key``; raises if absent."""
        ...


def build_s3_client(settings: Settings) -> "S3Client":
    """Construct the path-style S3 client from the API's ``Settings``.

    A thin adapter over the one shared builder (`aci_protocol.s3.build_s3_client`,
    #501) so the API writer and the worker reader cannot drift on endpoint /
    credentials / path-style addressing. Kept as a named function because callers
    (and `test_storage_port`) import it from this module.
    """
    return shared_build_s3_client(
        endpoint_url=settings.s3_endpoint_url,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
        region=settings.s3_region,
    )


class BundleStore:
    """S3/MinIO backing for the ``ObjectStore`` port (the one impl today)."""

    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.bundle_bucket
        self._client: S3Client = build_s3_client(settings)

    async def ensure_bucket(self) -> None:
        await run_in_threadpool(self._ensure_bucket_sync)

    def _ensure_bucket_sync(self) -> None:
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError:
            self._client.create_bucket(Bucket=self._bucket)

    async def exists(self, key: str) -> bool:
        return await run_in_threadpool(self._exists_sync, key)

    def _exists_sync(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError:
            return False
        return True

    async def put(self, key: str, data: bytes, content_type: str) -> None:
        await run_in_threadpool(self._put_sync, key, data, content_type)

    def _put_sync(self, key: str, data: bytes, content_type: str) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)

    async def get(self, key: str) -> bytes:
        return await run_in_threadpool(self._get_sync, key)

    def _get_sync(self, key: str) -> bytes:
        obj = self._client.get_object(Bucket=self._bucket, Key=key)
        body: bytes = obj["Body"].read()
        return body
