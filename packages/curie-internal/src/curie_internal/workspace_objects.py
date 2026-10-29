"""Private workspace object storage and exact-object signing ports."""

from collections.abc import Iterable, Iterator
from typing import Any, Protocol


class WorkspaceObjectPort(Protocol):
    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None: ...

    def get_stream(self, key: str) -> Iterator[bytes]: ...

    def presign_get(self, key: str, *, expires_seconds: int) -> str: ...

    def delete(self, key: str) -> None: ...

    def list_keys(self, prefix: str) -> Iterator[str]: ...


class WorkspaceObjectSigner:
    def __init__(self, *, client: Any, bucket: str, prefix: str = "private/workspaces") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, key: str) -> str:
        clean = key.lstrip("/")
        if clean.startswith("../") or "/../" in clean:
            raise ValueError("workspace object key escapes its private prefix")
        return f"{self._prefix}/{clean}"

    def presign_get(self, key: str, *, expires_seconds: int) -> str:
        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": self._key(key)},
                ExpiresIn=expires_seconds,
            )
        )
