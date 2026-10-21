"""Read-only access to plugin bundles in RustFS/S3 (mirrors the API's BundleStore).

The eval consumer fetches a version's immutable bundle by its bundle_ref key and
extracts it to read the bundle's own eval suite (evals/cases.json). Uses boto3
with path-style addressing (RustFS), the same construction the API's write path
uses, so the env names line up.

``extract_bundle`` is the Docker-substrate counterpart to the Kubernetes
bundle-fetch/extract init pair: with no init containers, the worker fetches and
unpacks the bundle itself, then copies it into the runner's plugin volume.
Its unwrap semantics mirror the API's ``bundles.bundle_root`` exactly (unwrap a
single top-level wrapper dir when that subdir carries the plugin manifest), so
the plugin root the runner sees matches the root the API validated on upload.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from aci_protocol.s3 import build_s3_client
from plugin_format import (
    DEFAULT_MAX_COMPRESSION_RATIO,
    DEFAULT_MAX_MEMBERS,
    DEFAULT_MAX_UNCOMPRESSED_BYTES,
    bundle_root,
    safe_extract,
)

from .config import WorkerConfig

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@runtime_checkable
class BundleReader(Protocol):
    """The read side of the storage port the worker needs (bytes by key).

    The API owns the full ``ObjectStore`` port (write + read); the worker only
    ever reads a bundle by key, so its slice of the port is this one method. A
    future non-S3 backend (GCS/Azure) supplies a reader satisfying this Protocol;
    the adapter itself is deferred until a non-S3 backend actually lands
    (ADR-0007, ADR-0026). Kept as a local Protocol because the worker
    deliberately does not import the API package (see ``binding.py``).
    """

    def get(self, key: str) -> bytes:
        """Fetch the object bytes for ``key``; raises on a missing key/error."""
        ...


class BundleStore:
    """S3/RustFS backing for the worker's ``BundleReader`` slice of the port.

    Mirrors the API's ``ObjectStore`` construction (path-style addressing) so the
    env names line up; a second backend is a drop-in ``BundleReader``.
    """

    def __init__(self, config: WorkerConfig) -> None:
        self._bucket = config.bundle_bucket
        # The one shared path-style construction (#501), also used by the API's
        # write path, so reader and writer cannot drift on addressing/creds.
        self._client: S3Client = build_s3_client(
            endpoint_url=config.s3_endpoint_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            region=config.s3_region,
        )

    def get(self, key: str) -> bytes:
        """Fetch the object bytes for ``key``. Raises on a missing key or S3 error
        (the caller treats any failure as an unresolvable suite)."""
        obj = self._client.get_object(Bucket=self._bucket, Key=key)
        body: bytes = obj["Body"].read()
        return body

    def presign_get(self, key: str, *, expires_seconds: int) -> str:
        """Mint one short-lived exact-object read URL for a claim init lane.

        The SandboxClaim receives this URL instead of a bucket credential. The
        worker remains the only identity that can list or sign arbitrary bundle
        keys; the URL expires shortly after the claim binds.
        """

        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": key},
                ExpiresIn=expires_seconds,
            )
        )


def extract_bundle(
    data: bytes,
    dest: Path,
    *,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
    max_compression_ratio: float = DEFAULT_MAX_COMPRESSION_RATIO,
    max_members: int = DEFAULT_MAX_MEMBERS,
) -> Path:
    """Extract ``data`` into ``dest`` and return the plugin root to mount.

    The returned path is ``dest`` when the archive is flat, or its single
    wrapper subdir when the manifest sits one level down -- the same root the
    API validated, so the runner reads the plugin from the expected layout.
    Extraction and unwrap route through ``plugin_format`` (the single audited
    home for the traversal/symlink/special-file and size/ratio guards); an
    unsafe, oversized, or unrecognized archive raises
    ``plugin_format.UnsupportedArchive``, which the Docker-substrate caller
    already treats as a fetch failure. The size/ratio caps default to
    ``plugin_format``'s generous fallbacks; the caller passes the operator-
    configured ``WorkerConfig`` values instead (ADR-0059 decision 3).
    """
    safe_extract(
        data,
        dest,
        max_uncompressed_bytes=max_uncompressed_bytes,
        max_compression_ratio=max_compression_ratio,
        max_members=max_members,
    )
    return bundle_root(dest)
