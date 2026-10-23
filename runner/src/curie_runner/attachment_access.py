"""Turn-scoped platform attachment access and sandbox-local verified cache.

Authority remains opaque and memory-only. The API validates live scope and
expiry on every operation, including cache reuse; no provider URL enters here.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp
import anyio
from aci_protocol import AttachmentAccessCapability, Event

from .platform_slack.capability import Origin, url_origin

CHUNK_BYTES = 1024 * 1024
METADATA_BYTES = 256 * 1024
TRANSFER_SECONDS = 300
_ALL_OPENERS = object()
TOOL_NAMES = frozenset({"mcp__curie__list_attachments", "mcp__curie__fetch_attachment"})
LAZY_ATTACHMENT_NOTICE = (
    "Attachments are remote references, not files already on disk. Use list_attachments "
    "to discover current and earlier-message references, then fetch_attachment with a listed ID "
    "to obtain a complete verified local path. Only requested files are downloaded. "
    "A fetch may fail if access is revoked or sandbox storage is exhausted."
)


class AttachmentAccessError(RuntimeError):
    """A credential-free failure safe to expose to the model."""


@dataclass(eq=False, repr=False)
class _Authority:
    capability: AttachmentAccessCapability = field(repr=False)
    scopes: set[anyio.CancelScope] = field(default_factory=set, repr=False)


class AttachmentAccessTurn:
    """One logical opener; renewals preserve its in-flight transfers."""

    def __init__(self, *, trusted_origin: Origin | None) -> None:
        self._trusted_origin = trusted_origin
        self._active: _Authority | None = None
        self._opener: object | None = None
        self._admitted: bytes | None = None

    def __repr__(self) -> str:
        return f"AttachmentAccessTurn(active={self._active is not None})"

    def _trusted(self, capability: AttachmentAccessCapability) -> bool:
        parts = urlsplit(capability.url)
        return (
            self._trusted_origin is not None
            and url_origin(capability.url) == self._trusted_origin
            and parts.path == "/attachment-access"
            and not parts.query
            and not parts.fragment
            and parts.username is None
            and parts.password is None
            and capability.token.isascii()
            and all(32 < ord(c) < 127 for c in capability.token)
        )

    @staticmethod
    def _binding(capability: AttachmentAccessCapability) -> bytes:
        return hashlib.sha256((capability.url + "\0" + capability.token).encode()).digest()

    def begin(self, event: Event) -> object:
        self.end()
        capability = event.attachment_access
        if capability is not None and self._trusted(capability):
            self._admitted = self._binding(capability)
            self._active = _Authority(capability)
            self._opener = self._active
        else:
            # Even an opener with no credential has an identity. Its late
            # cleanup must never be interpreted as "clear whichever is current".
            self._opener = object()
        return self._opener

    def _revoke(self) -> None:
        active, self._active = self._active, None
        if active is not None:
            for scope in tuple(active.scopes):
                scope.cancel()

    def steer(self, event: Event | None) -> None:
        capability = event.attachment_access if event is not None else None
        if (
            capability is None
            or not self._trusted(capability)
            or self._binding(capability) != self._admitted
        ):
            self._revoke()
        elif self._active is None:
            # An omitted context revokes reads, not the admitted opener. A
            # later renewal can restore only that same opaque binding.
            self._active = _Authority(capability)

    def end(self, opener: object = _ALL_OPENERS) -> None:
        if opener is not _ALL_OPENERS and opener is not self._opener:
            return
        self._revoke()
        self._opener = self._admitted = None

    def current(self) -> _Authority | None:
        return self._active

    @contextmanager
    def operation(self) -> Iterator[_Authority]:
        active = self._active
        if active is None:
            raise AttachmentAccessError("Attachment access is unavailable for this turn.")
        with anyio.CancelScope() as scope:
            active.scopes.add(scope)
            try:
                yield active
            finally:
                active.scopes.discard(scope)
        if scope.cancel_called or self._active is not active:
            raise AttachmentAccessError("Attachment access ended before completion.")


def _text(value: Any, maximum: int) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= maximum
        and not any(
            ord(c) < 32 or 127 <= ord(c) <= 159 or 0xD800 <= ord(c) <= 0xDFFF for c in value
        )
    )


def _descriptor(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or not {"id", "name", "mime_type", "size_bytes", "sha256", "current"} <= value.keys()
        or not _text(value.get("id"), 256)
    ):
        raise AttachmentAccessError("Invalid attachment metadata.")
    name = value.get("name")
    if (
        not isinstance(name, str)
        or not _text(name, 255)
        or len(name.encode("utf-8")) > 255
        or name.startswith(".")
        or "/" in name
        or "\\" in name
    ):
        raise AttachmentAccessError("Unsafe attachment filename.")
    size, digest, mime = value.get("size_bytes"), value.get("sha256"), value.get("mime_type")
    if (
        (size is not None and (type(size) is not int or size < 0))
        or (
            digest is not None
            and (not isinstance(digest, str) or not re.fullmatch("[a-f0-9]{64}", digest))
        )
        or (mime is not None and not _text(mime, 255))
        or type(value.get("current")) is not bool
    ):
        raise AttachmentAccessError("Invalid attachment metadata.")
    return {
        key: value[key] for key in ("id", "name", "mime_type", "size_bytes", "sha256", "current")
    }


@dataclass(frozen=True)
class _Cached:
    filename: str
    digest: str
    size: int


class AttachmentAccess:
    """Bounded streaming, durable integrity completion, then exclusive publication."""

    def __init__(self, turn: AttachmentAccessTurn, root: Path) -> None:
        self.turn = turn
        self.root = root.absolute()
        self._cached: dict[str, _Cached] = {}
        self._locks = tuple(anyio.Lock() for _ in range(64))
        self._concurrency = anyio.Semaphore(2)

    async def _metadata(
        self,
        client: aiohttp.ClientSession,
        active: _Authority,
        operation: str,
        body: dict[str, Any],
    ) -> Any:
        async with client.post(
            active.capability.url + "/" + operation,
            json=body,
            headers={"X-Curie-Attachment-Access": active.capability.token},
            allow_redirects=False,
        ) as response:
            if response.status != 200:
                raise AttachmentAccessError("Attachment request was refused by the platform.")
            data = bytearray()
            async for chunk in response.content.iter_chunked(64 * 1024):
                if len(data) + len(chunk) > METADATA_BYTES:
                    raise AttachmentAccessError("Attachment metadata exceeds the response budget.")
                data.extend(chunk)
            try:
                return json.loads(data)
            except (ValueError, UnicodeError) as exc:
                raise AttachmentAccessError("Invalid attachment response.") from exc

    async def _completion(
        self, client: aiohttp.ClientSession, active: _Authority, transfer: str
    ) -> Any:
        # Only this status request is retried, once. Never repeat a body read;
        # the surrounding turn scope and total deadline still bound both attempts.
        try:
            return await self._metadata(client, active, "completion", {"transfer_id": transfer})
        except (aiohttp.ClientError, TimeoutError):
            return await self._metadata(client, active, "completion", {"transfer_id": transfer})

    def _directory(self) -> int:
        # Never follow a root symlink or use a path-based staging/open after this.
        return os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)

    def _same_directory(self, directory: int) -> bool:
        current = os.stat(self.root, follow_symlinks=False)
        opened = os.fstat(directory)
        return stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == (
            opened.st_dev,
            opened.st_ino,
        )

    def _verified(self, directory: int, cached: _Cached) -> bool:
        # All callers run in an AnyIO worker under the original operation's
        # authority and fail_after scope. Cooperative checks observe both even
        # though run_sync shields its await while the worker releases the fd.
        anyio.from_thread.check_cancelled()
        try:
            fd = os.open(
                cached.filename, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
            )
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_nlink != 1
                    or info.st_size != cached.size
                ):
                    return False
                digest = hashlib.sha256()
                while True:
                    anyio.from_thread.check_cancelled()
                    chunk = stream.read(CHUNK_BYTES)
                    anyio.from_thread.check_cancelled()
                    if not chunk:
                        break
                    digest.update(chunk)
                after = os.fstat(stream.fileno())
                entry = os.stat(cached.filename, dir_fd=directory, follow_symlinks=False)
                return (
                    digest.hexdigest() == cached.digest
                    and self._same_directory(directory)
                    and (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size)
                    == (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size)
                    and (entry.st_dev, entry.st_ino) == (info.st_dev, info.st_ino)
                )
        except OSError:
            return False

    async def list(self, args: dict[str, Any]) -> dict[str, Any]:
        if (
            set(args) - {"cursor", "limit"}
            or ("cursor" in args and not _text(args["cursor"], 2048))
            or (
                "limit" in args
                and (type(args["limit"]) is not int or not 1 <= args["limit"] <= 100)
            )
        ):
            raise AttachmentAccessError("Use an optional cursor and limit between 1 and 100.")
        try:
            with self.turn.operation() as active, anyio.fail_after(TRANSFER_SECONDS):
                async with aiohttp.ClientSession(
                    trust_env=False,
                    auto_decompress=False,
                    timeout=aiohttp.ClientTimeout(total=TRANSFER_SECONDS, connect=10, sock_read=30),
                ) as client:
                    page = await self._metadata(client, active, "list", args)
                if (
                    not isinstance(page, dict)
                    or not isinstance(page.get("files"), list)
                    or len(page["files"]) > 100
                    or (
                        page.get("next_cursor") is not None and not _text(page["next_cursor"], 2048)
                    )
                ):
                    raise AttachmentAccessError("Invalid attachment catalog.")
                files = [_descriptor(value) for value in page["files"]]
                for value in files:
                    cached = self._cached.get(value["id"])
                    value["cached"] = False
                    if cached is not None and cached.digest == value["sha256"]:
                        directory = self._directory()
                        try:
                            value["cached"] = await anyio.to_thread.run_sync(
                                self._verified, directory, cached
                            )
                        finally:
                            os.close(directory)
                return {"files": files, "next_cursor": page.get("next_cursor")}
        except (OSError, aiohttp.ClientError, TimeoutError) as exc:
            raise AttachmentAccessError("Attachment catalog could not be read.") from exc

    async def fetch(self, args: dict[str, Any]) -> dict[str, Any]:
        if set(args) != {"id"} or not _text(args.get("id"), 256):
            raise AttachmentAccessError("Provide only a catalog attachment ID.")
        identifier = args["id"]
        try:
            with self.turn.operation() as active, anyio.fail_after(TRANSFER_SECONDS):
                async with (
                    self._concurrency,
                    self._locks[
                        int.from_bytes(hashlib.sha256(identifier.encode()).digest()[:2]) % 64
                    ],
                ):
                    async with aiohttp.ClientSession(
                        trust_env=False,
                        auto_decompress=False,
                        timeout=aiohttp.ClientTimeout(
                            total=TRANSFER_SECONDS, connect=10, sock_read=30
                        ),
                    ) as client:
                        descriptor = _descriptor(
                            await self._metadata(client, active, "authorize", args)
                        )
                        if descriptor["id"] != identifier:
                            raise AttachmentAccessError("Attachment identity changed.")
                        directory = self._directory()
                        try:
                            cached = self._cached.get(identifier)
                            if (
                                cached is not None
                                and descriptor["sha256"] == cached.digest
                                and await anyio.to_thread.run_sync(
                                    self._verified, directory, cached
                                )
                            ):
                                if self.turn.current() is not active:
                                    raise AttachmentAccessError(
                                        "Attachment access ended before completion."
                                    )
                                return {
                                    "path": str(self.root / cached.filename),
                                    "size_bytes": cached.size,
                                }
                            return await self._download(client, active, directory, descriptor)
                        finally:
                            os.close(directory)
        except (OSError, aiohttp.ClientError, TimeoutError) as exc:
            raise AttachmentAccessError(
                "Attachment fetch failed; no complete path is available."
            ) from exc

    async def _download(
        self,
        client: aiohttp.ClientSession,
        active: _Authority,
        directory: int,
        descriptor: dict[str, Any],
    ) -> dict[str, Any]:
        staging = ".attachment-" + uuid.uuid4().hex
        fd = os.open(
            staging, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=directory
        )
        published: str | None = None
        try:
            digest, size = hashlib.sha256(), 0
            with os.fdopen(fd, "wb") as output:
                async with client.post(
                    active.capability.url + "/fetch",
                    json={"id": descriptor["id"]},
                    headers={"X-Curie-Attachment-Access": active.capability.token},
                    allow_redirects=False,
                ) as response:
                    transfer = response.headers.get("X-Curie-Transfer-ID")
                    if response.status != 200 or not _text(transfer, 256):
                        raise AttachmentAccessError("Attachment stream was refused.")
                    async for chunk in response.content.iter_chunked(CHUNK_BYTES):
                        output.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                output.flush()
                os.fsync(output.fileno())
                staged = os.fstat(output.fileno())
            assert isinstance(transfer, str)
            completion = await self._completion(client, active, transfer)
            actual = digest.hexdigest()
            if (
                not isinstance(completion, dict)
                or completion.get("status") != "complete"
                or completion.get("id") != descriptor["id"]
                or completion.get("sha256") != actual
                or type(completion.get("size_bytes")) is not int
                or completion["size_bytes"] != size
                or (descriptor["sha256"] is not None and descriptor["sha256"] != actual)
            ):
                raise AttachmentAccessError("Attachment integrity completion failed.")
            if self.turn.current() is not active or not self._same_directory(directory):
                raise AttachmentAccessError("Attachment authority or cache directory changed.")
            entry = os.stat(staging, dir_fd=directory, follow_symlinks=False)
            if (entry.st_dev, entry.st_ino, entry.st_size, entry.st_nlink) != (
                staged.st_dev,
                staged.st_ino,
                size,
                1,
            ):
                raise AttachmentAccessError("Attachment staging file changed.")
            # A random final name plus an exclusive link never overwrites a
            # model-created file or follows a symlink. Both names share the mount.
            extension = Path(descriptor["name"]).suffix
            if not re.fullmatch(r"\.[A-Za-z0-9_-]{1,24}", extension):
                extension = ""
            identifier = hashlib.sha256(descriptor["id"].encode()).hexdigest()
            candidate = f"{identifier}-{uuid.uuid4().hex}{extension}"
            os.link(
                staging,
                candidate,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
            # Ownership starts only after exclusive publication succeeds. A
            # collision must never make cleanup delete the pre-existing entry.
            published = candidate
            os.unlink(staging, dir_fd=directory)
            os.fsync(directory)
            cached = _Cached(published, actual, size)
            verified = await anyio.to_thread.run_sync(self._verified, directory, cached)
            if not verified or self.turn.current() is not active:
                raise AttachmentAccessError(
                    "Attachment authority or cache changed before publication."
                )
            self._cached[descriptor["id"]] = cached
            return {"path": str(self.root / published), "size_bytes": size}
        except BaseException:
            if published is not None:
                try:
                    os.unlink(published, dir_fd=directory)
                except FileNotFoundError:
                    pass
            raise
        finally:
            try:
                os.unlink(staging, dir_fd=directory)
            except FileNotFoundError:
                pass
