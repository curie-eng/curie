"""Trusted preparation of repository workspaces for sandbox claims.

The worker, not the sandbox, redeems the operator GitHub credential and clones
the configured deployment repository.  Authentication is carried in an
ephemeral git configuration header, never in argv.  The checkout's origin is
then set to the clean URL and verified before a normalized archive can be
uploaded.  A sandbox receives only an opaque, short-lived one-object read
capability and the archive digest.

This module deliberately defines worker-local ports.  Importing the API package
would couple two independently deployed applications, while giving the sandbox
an object-store credential would turn a repository workspace into a bucket-wide
read capability.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

WORKSPACE_REF_ENV = "CURIE_WORKSPACE_REF"
WORKSPACE_SHA256_ENV = "CURIE_WORKSPACE_SHA256"
WORKSPACE_MOUNT_PATH = "/workspace"
_GITHUB_URL = re.compile(r"https://github\.com/[^\s<>|]+", re.IGNORECASE)
_REPO_FULL_NAME = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})/"
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,98}[A-Za-z0-9_-])?$"
)
# Two path segments, not glued to a longer path on either side. A following
# slash is the nested-path case (`apps/worker/src/foo.py` is not a fact).
_BARE_REPO = re.compile(
    r"(?<![A-Za-z0-9._/-])"
    r"[A-Za-z0-9._-]+/[A-Za-z0-9._-]+"
    r"(?![A-Za-z0-9._/-])"
)
# Bare tokens that look like source-tree paths are not facts. URL-derived facts
# skip this check.
_BARE_REPO_SOURCE_ROOTS = frozenset(
    {
        "src",
        "lib",
        "docs",
        "doc",
        "bin",
        "app",
        "apps",
        "test",
        "tests",
        "pkg",
        "cmd",
        "cli",
        "scripts",
        "script",
        "examples",
        "example",
        "internal",
        "vendor",
        "dist",
        "build",
        "tmp",
        "temp",
        "include",
        "assets",
        "static",
        "public",
        "config",
        "configs",
        "tools",
        "packages",
        "package",
    }
)
_BARE_REPO_SOURCE_EXTENSIONS = frozenset(
    {
        "py",
        "md",
        "rs",
        "ts",
        "js",
        "go",
        "json",
        "toml",
        "yaml",
        "yml",
        "txt",
        "sh",
        "c",
        "h",
        "cc",
        "cpp",
        "java",
        "rb",
        "php",
        "css",
        "html",
        "xml",
        "sql",
        "lock",
        "proto",
        "kt",
        "swift",
    }
)
_ENGLISH_SLASH_PAIRS = frozenset({"and/or", "n/a", "w/o", "i/o", "y/n"})
_TRAILING_PUNCTUATION = ".,;:!?)]}"
# The terminal refusal for a message that names a repository while the worker-wide
# workspace switch is off (#2659). It names no repository, so it neither echoes
# untrusted message text nor reveals whether the allowlist permits one.
WORKSPACES_DISABLED_REFUSAL = (
    "Repository workspaces are turned off on this installation, so no repository "
    "was attached and no work started. An operator can turn them on with "
    "agentSandbox.runner.workspace.enabled in the chart values "
    "(CURIE_WORKSPACE_ENABLED on the worker)."
)
_SELECTION_REFUSAL_MESSAGES = {
    "workspace.deployment_disabled": (
        "This deployment does not enable repository workspaces."
    ),
    "workspace.repository_required": (
        "Start the thread by naming one allowed root GitHub repository URL."
    ),
    "workspace.selection_conflict": (
        "This thread is already bound to a different repository."
    ),
}


class WorkspacePreparationError(RuntimeError):
    """A workspace could not be made safe and ready for a sandbox."""

    def __init__(self, stage: str, detail: str) -> None:
        self.stage = stage
        super().__init__(f"workspace {stage} failed: {detail}")


class WorkspaceStageTimeout(WorkspacePreparationError):
    """A named preparation stage exhausted its own or aggregate budget."""

    def __init__(self, stage: str, limit_seconds: int | float) -> None:
        self.limit_seconds = limit_seconds
        super().__init__(stage, f"exceeded the {limit_seconds:g}s budget")


class WorkspaceArchiveError(WorkspacePreparationError):
    """An archive violates the normalized workspace format."""

    def __init__(self, detail: str) -> None:
        super().__init__("archive-validation", detail)


class WorkspaceSelectionRefused(WorkspacePreparationError):
    """A deliberate repository-selection refusal that must not be retried."""

    def __init__(self, detail: str) -> None:
        self.public_detail = detail
        super().__init__("repository-selection", detail)


class _OwnershipWriteUncertain(WorkspacePreparationError):
    """A ledger write may have committed but could not be read back safely."""

    def __init__(self) -> None:
        super().__init__(
            "ownership-ledger",
            "workspace ownership write outcome is uncertain; retaining its archive",
        )


@dataclass(frozen=True)
class WorkspaceLimits:
    """Resource and deadline envelope for one trusted preparation."""

    clone_timeout_seconds: int = 90
    archive_timeout_seconds: int = 30
    upload_timeout_seconds: int = 30
    total_timeout_seconds: int = 150
    max_checkout_bytes: int = 512 * 1024 * 1024
    max_archive_bytes: int = 256 * 1024 * 1024
    max_members: int = 4096
    max_compression_ratio: float = 20.0
    reference_ttl_seconds: int = 300
    max_concurrent_clones: int = 2

    def __post_init__(self) -> None:
        numeric = {
            "clone_timeout_seconds": self.clone_timeout_seconds,
            "archive_timeout_seconds": self.archive_timeout_seconds,
            "upload_timeout_seconds": self.upload_timeout_seconds,
            "total_timeout_seconds": self.total_timeout_seconds,
            "max_checkout_bytes": self.max_checkout_bytes,
            "max_archive_bytes": self.max_archive_bytes,
            "max_members": self.max_members,
            "max_compression_ratio": self.max_compression_ratio,
            "reference_ttl_seconds": self.reference_ttl_seconds,
            "max_concurrent_clones": self.max_concurrent_clones,
        }
        for name, value in numeric.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if (
            self.clone_timeout_seconds + self.archive_timeout_seconds + self.upload_timeout_seconds
            > self.total_timeout_seconds
        ):
            raise ValueError("workspace stage budgets exceed total_timeout_seconds")


@dataclass(frozen=True)
class WorkspaceCredential:
    """One deployment-derived clone credential returned by the API."""

    repo_full_name: str
    clone_url: str
    authorization_header: str
    revision: str | None = None

    def __post_init__(self) -> None:
        if not self.repo_full_name or not self.clone_url or not self.authorization_header:
            raise ValueError("workspace credential response is incomplete")
        if not self.clone_url.startswith("https://github.com/"):
            raise ValueError("workspace clone URL must be a clean GitHub HTTPS URL")
        if self.clone_url != f"https://github.com/{self.repo_full_name}.git":
            raise ValueError("workspace clone URL does not match the server-derived repository")
        authority = self.clone_url.removeprefix("https://").split("/", 1)[0]
        if "@" in authority:
            raise ValueError("workspace clone URL must not contain userinfo")
        if any(character in self.authorization_header for character in ("\r", "\n", "\0")):
            raise ValueError("workspace authorization header contains control characters")


def parse_github_repo_fact(message: str) -> str | None:
    """Extract one canonical GitHub repository from trusted turn text."""

    repositories: dict[str, str] = {}
    bare_repositories: dict[str, str] = {}
    for matched in _GITHUB_URL.finditer(message):
        raw = matched.group(0).rstrip(_TRAILING_PUNCTUATION)
        parsed = urlsplit(raw)
        try:
            invalid_authority = (
                parsed.hostname is None
                or parsed.hostname.casefold() != "github.com"
                or parsed.port is not None
                or parsed.username is not None
                or parsed.password is not None
            )
        except ValueError:
            invalid_authority = True
        if (
            parsed.scheme.casefold() != "https"
            or invalid_authority
            or parsed.query
            or parsed.fragment
        ):
            continue
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) != 2:
            continue
        owner, repository = parts
        if repository.endswith(".git"):
            repository = repository[:-4]
        candidate = f"{owner}/{repository}"
        if _REPO_FULL_NAME.fullmatch(candidate):
            repositories.setdefault(candidate.casefold(), candidate)
    # Mask every GitHub URL match, including non-root paths, so a pull/query
    # URL cannot also be read as a bare owner/repo.
    masked = _GITHUB_URL.sub(" ", message)
    for matched in _BARE_REPO.finditer(masked):
        raw = matched.group(0).rstrip(_TRAILING_PUNCTUATION)
        parts = [part for part in raw.split("/") if part]
        if len(parts) != 2:
            continue
        owner, repository = parts
        if owner.casefold() in _BARE_REPO_SOURCE_ROOTS:
            continue
        if repository.endswith(".git"):
            repository = repository[:-4]
        if "." in repository:
            suffix = repository.rsplit(".", 1)[-1].casefold()
            if suffix in _BARE_REPO_SOURCE_EXTENSIONS:
                continue
        candidate = f"{owner}/{repository}"
        if candidate.casefold() in _ENGLISH_SLASH_PAIRS:
            continue
        if _REPO_FULL_NAME.fullmatch(candidate):
            bare_repositories.setdefault(candidate.casefold(), candidate)
    if len(repositories) > 1:
        raise WorkspaceSelectionRefused(
            "This message names more than one GitHub repository, so no repository "
            "was attached and no work started. A thread works in only one repository."
        )
    if repositories:
        # A URL wins outright. A bare token beside one is noise next to a plain
        # statement, so it neither competes with it nor refuses.
        return next(iter(repositories.values()))
    # AMBIGUOUS MEANS ABSENT, NEVER REFUSED (#2767). A bare owner/repo is a
    # guess: `P/L`, `24/7`, `him/her` and `headlines/taglines` all match, and so
    # does every media type, `application/pdf` and `image/png` included. One
    # guess reaches the allowlist and is refused there. TWO guesses never got
    # that far -- the count above raised first -- so `the P/L statement and our
    # 24/7 support desk` and `the file is application/pdf and the deck is
    # image/png` ended the turn, and the refusal text was delivered to the
    # sender as the whole answer to their question. The second is any message
    # naming two attachments.
    #
    # `_ENGLISH_SLASH_PAIRS` cannot grow to cover it, and that is the difference
    # between it and `_BARE_REPO_SOURCE_EXTENSIONS` above: file extensions are a
    # finite set and slashed English is not. So two bare tokens are read as what
    # they much more often are, ordinary prose, and this message names no
    # repository. Two URLs still refuse -- a sender who writes two github.com
    # URLs has plainly named two repositories.
    if len(bare_repositories) > 1:
        return None
    return next(iter(bare_repositories.values()), None)


def trusted_repository_fact(message: str, *, ignore_message: bool) -> str | None:
    """Repository facts for workspace selection.

    Job payloads (webhook/cron), verified-review bodies, and approval resume
    text (#2828, which quotes the gated tool arguments) are untrusted for
    repository selection. A coding target comes from operator mapping or an
    already sticky thread row, never from a URL inside those documents.
    """

    if ignore_message:
        return None
    return parse_github_repo_fact(message)


def webhook_job_refuses_workspace(message: str) -> bool:
    """True when platform text, not the payload, stopped coding for this job."""

    trusted = message.split("<untrusted-hook-payload>", 1)[0]
    return "Coding is stopped" in trusted


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        return None


@dataclass(frozen=True)
class _TransportResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


def _url_transport(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None = None,
    allow_redirects: bool = False,
) -> _TransportResponse:
    if allow_redirects:
        raise ValueError("workspace credential redemption never follows redirects")
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=15) as response:
            return _TransportResponse(
                status=int(response.status),
                headers=dict(response.headers.items()),
                body=response.read(),
            )
    except urllib.error.HTTPError as exc:
        return _TransportResponse(
            status=int(exc.code),
            headers=dict(exc.headers.items()) if exc.headers is not None else {},
            body=exc.read(),
        )


class WorkspaceCredentialClient:
    """Redeem a deployment-derived credential through worker-only auth."""

    def __init__(
        self,
        *,
        api_url: str,
        worker_token: str,
        transport: Callable[..., Any] = _url_transport,
    ) -> None:
        if not worker_token:
            raise ValueError("workspace delivery requires CURIE_INTERNAL_WORKER_TOKEN")
        self._api_url = api_url.rstrip("/")
        self._worker_token = worker_token
        self._transport = transport

    def select(
        self,
        deployment_id: uuid.UUID,
        conversation_id: str,
        author: str,
        repo_full_name: str | None,
    ) -> str | None:
        body = json.dumps(
            {
                "conversation_id": conversation_id,
                "author": author,
                "repo_full_name": repo_full_name,
            }
        ).encode()
        try:
            response = self._transport(
                method="POST",
                url=f"{self._api_url}/v1/internal/workspaces/{deployment_id}/selection",
                headers={
                    "X-Curie-Worker-Token": self._worker_token,
                    "Content-Type": "application/json",
                },
                body=body,
                allow_redirects=False,
            )
        except Exception as exc:
            raise WorkspacePreparationError(
                "repository-selection", "worker could not reach the internal workspace API"
            ) from exc
        if response.status == 403:
            raise WorkspaceSelectionRefused(
                "That repository is not in api.githubRepoAllowlist for this installation; "
                "allow `owner/repo` or `owner/*` in the chart values."
            )
        if response.status == 409:
            try:
                payload = json.loads(response.body)
                api_detail = payload["detail"]
                if not isinstance(api_detail, Mapping):
                    raise TypeError("detail is not an object")
                code = api_detail["code"]
                if not isinstance(code, str):
                    raise TypeError("detail.code is not a string")
                public_detail = _SELECTION_REFUSAL_MESSAGES[code]
            except (
                KeyError,
                TypeError,
                ValueError,
                UnicodeDecodeError,
            ) as exc:
                raise WorkspacePreparationError(
                    "repository-selection",
                    "API returned an invalid selection refusal response",
                ) from exc
            raise WorkspaceSelectionRefused(public_detail)
        if response.status != 200:
            raise WorkspacePreparationError(
                "repository-selection", f"API returned HTTP {response.status}"
            )
        try:
            selected = json.loads(response.body)["repo_full_name"]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WorkspacePreparationError(
                "repository-selection", "API returned an invalid selection response"
            ) from exc
        if selected is None:
            return None
        if not isinstance(selected, str) or not _REPO_FULL_NAME.fullmatch(selected):
            raise WorkspacePreparationError(
                "repository-selection", "API returned an invalid repository selection"
            )
        return selected

    def redeem(
        self, deployment_id: uuid.UUID, conversation_id: str
    ) -> WorkspaceCredential:
        body = json.dumps({"conversation_id": conversation_id}).encode()
        try:
            response = self._transport(
                method="POST",
                url=f"{self._api_url}/v1/internal/workspaces/{deployment_id}/credential",
                headers={
                    "X-Curie-Worker-Token": self._worker_token,
                    "Content-Type": "application/json",
                },
                body=body,
                allow_redirects=False,
            )
        except Exception as exc:
            raise WorkspacePreparationError(
                "credential-redemption", "worker could not reach the internal credential API"
            ) from exc
        if response.status != 200:
            raise WorkspacePreparationError(
                "credential-redemption", f"API returned HTTP {response.status}"
            )
        cache_control = next(
            (
                str(value).lower()
                for key, value in response.headers.items()
                if str(key).lower() == "cache-control"
            ),
            "",
        )
        if "no-store" not in cache_control:
            raise WorkspacePreparationError(
                "credential-redemption", "API response omitted Cache-Control: no-store"
            )
        try:
            payload = json.loads(response.body)
            revision = payload.get("revision")
            if revision is not None and not isinstance(revision, str):
                raise TypeError("revision is not a string")
            return WorkspaceCredential(
                repo_full_name=str(payload["repo_full_name"]),
                clone_url=str(payload["clone_url"]),
                authorization_header=str(payload["authorization_header"]),
                revision=revision,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WorkspacePreparationError(
                "credential-redemption", "API returned an invalid credential response"
            ) from exc


@dataclass(frozen=True)
class CommandResult:
    stdout: str = ""


class CommandPort(Protocol):
    def require(self, executable: str) -> None: ...

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> CommandResult: ...


def scrubbed_git_environment(
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Drop ambient credentials/config before adding explicit Git settings."""

    process_env = os.environ.copy()
    for name in tuple(process_env):
        if name in {
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_ASKPASS",
            "SSH_ASKPASS",
            "GIT_SSH_COMMAND",
            "GIT_PROXY_COMMAND",
            "GIT_EXTERNAL_DIFF",
            "GIT_ALLOW_PROTOCOL",
            "GIT_CONFIG_PARAMETERS",
            "GIT_TERMINAL_PROMPT",
        } or name.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_")):
            process_env.pop(name, None)
    process_env.pop("GIT_CONFIG_COUNT", None)
    process_env.update(
        {
            "HOME": "/nonexistent",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ALLOW_PROTOCOL": "https",
        }
    )
    process_env.update(overrides or {})
    return process_env


class SubprocessCommands:
    """Subprocess adapter that never invokes a shell or echoes credentials."""

    def require(self, executable: str) -> None:
        if shutil.which(executable) is None:
            raise FileNotFoundError(executable)

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> CommandResult:
        # Ambient developer credentials and git process configuration are not
        # part of this trust boundary. The caller supplies the one redeemed
        # header explicitly after this scrub.
        process_env = scrubbed_git_environment(env)
        try:
            completed = subprocess.run(  # noqa: S603 -- fixed argv, no shell.
                list(argv),
                cwd=cwd,
                env=process_env,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("command exceeded its deadline") from exc
        if completed.returncode != 0:
            # stderr is intentionally omitted: git may include an authenticated
            # challenge in diagnostics, and no command detail is needed to act.
            raise WorkspacePreparationError(
                "command", f"git exited with status {completed.returncode}"
            )
        return CommandResult(stdout=completed.stdout)


class WorkspaceObjectPort(Protocol):
    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None: ...

    def get_stream(self, key: str) -> Iterator[bytes]: ...

    def presign_get(self, key: str, *, expires_seconds: int) -> str: ...

    def delete(self, key: str) -> None: ...

    def list_keys(self, prefix: str) -> Iterator[str]: ...


class WorkspaceObjectStore:
    """Private S3/RustFS storage with streaming exact-object operations."""

    def __init__(self, *, client: Any, bucket: str, prefix: str = "private/workspaces") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, key: str) -> str:
        clean = key.lstrip("/")
        if clean.startswith("../") or "/../" in clean:
            raise ValueError("workspace object key escapes its private prefix")
        return f"{self._prefix}/{clean}"

    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        # boto3 upload_fileobj streams and multipart-uploads as needed. Adapt the
        # iterator to read() without ever assembling the whole archive in memory
        # or in a second worker-side spool.
        self._client.upload_fileobj(_IteratorReader(chunks), self._bucket, self._key(key))

    def get_stream(self, key: str) -> Iterator[bytes]:
        body = self._client.get_object(Bucket=self._bucket, Key=self._key(key))["Body"]
        try:
            while True:
                chunk = body.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            close = getattr(body, "close", None)
            if close is not None:
                close()

    def presign_get(self, key: str, *, expires_seconds: int) -> str:
        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": self._key(key)},
                ExpiresIn=expires_seconds,
            )
        )

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))

    def list_keys(self, prefix: str) -> Iterator[str]:
        """Yield logical keys below the private workspace prefix."""

        logical_prefix = prefix.strip("/")
        object_prefix = self._key(logical_prefix)
        if logical_prefix:
            object_prefix += "/"
        private_prefix = f"{self._prefix}/"
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=object_prefix):
            for item in page.get("Contents") or ():
                key = str(item.get("Key") or "")
                if key.startswith(private_prefix):
                    yield key.removeprefix(private_prefix)


class _IteratorReader:
    """Minimal binary read port over chunks for boto3's upload_fileobj."""

    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._eof = False

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            for chunk in self._chunks:
                self._buffer.extend(chunk)
            self._eof = True
            data = bytes(self._buffer)
            self._buffer.clear()
            return data
        while len(self._buffer) < size and not self._eof:
            try:
                self._buffer.extend(next(self._chunks))
            except StopIteration:
                self._eof = True
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data


@dataclass(frozen=True)
class WorkspaceRef:
    """Opaque claim-scoped read capability carried in SandboxClaim state."""

    url: str
    sha256: str
    expires_at_epoch: int

    @property
    def expires_in_seconds(self) -> int:
        return max(0, self.expires_at_epoch - int(time.time()))

    def encode(self) -> str:
        raw = json.dumps(
            {"u": self.url, "s": self.sha256, "e": self.expires_at_epoch},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @classmethod
    def decode(cls, value: str) -> WorkspaceRef:
        try:
            raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
            payload = json.loads(raw)
            result = cls(
                url=str(payload["u"]),
                sha256=str(payload["s"]),
                expires_at_epoch=int(payload["e"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WorkspacePreparationError("reference", "invalid workspace reference") from exc
        if not result.url.startswith(("http://", "https://")):
            raise WorkspacePreparationError("reference", "workspace URL is not HTTP(S)")
        if len(result.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in result.sha256.lower()
        ):
            raise WorkspacePreparationError("reference", "workspace digest is invalid")
        return result


@dataclass(frozen=True)
class PreparedWorkspace:
    object_key: str
    sha256: str
    clean_clone_url: str
    repo_full_name: str
    base_sha: str
    materialized_head: str
    checkout_mode: int
    reference: WorkspaceRef

    def claim_env(self) -> dict[str, str]:
        return {
            WORKSPACE_REF_ENV: self.reference.encode(),
            WORKSPACE_SHA256_ENV: self.sha256,
        }


_OWNERSHIP_PREFIX = "_ownership"


@dataclass(frozen=True)
class _WorkspaceOwnership:
    """Private durable ownership record for one thread's workspace base."""

    thread_key: str
    prepared: PreparedWorkspace
    expires_at_epoch: int
    stale_object_keys: tuple[str, ...] = ()

    def encode(self) -> bytes:
        prepared = self.prepared
        return json.dumps(
            {
                "version": 1,
                "thread_key": self.thread_key,
                "expires_at_epoch": self.expires_at_epoch,
                "stale_object_keys": list(self.stale_object_keys),
                "prepared": {
                    "object_key": prepared.object_key,
                    "sha256": prepared.sha256,
                    "clean_clone_url": prepared.clean_clone_url,
                    "repo_full_name": prepared.repo_full_name,
                    "base_sha": prepared.base_sha,
                    "materialized_head": prepared.materialized_head,
                    "checkout_mode": prepared.checkout_mode,
                    "reference": prepared.reference.encode(),
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def decode(
        cls, payload: bytes, *, expected_thread_key: str | None = None
    ) -> _WorkspaceOwnership:
        try:
            raw = json.loads(payload)
            if raw.get("version") != 1:
                raise ValueError("unsupported ownership record version")
            thread_key = str(raw["thread_key"])
            if expected_thread_key is not None and thread_key != expected_thread_key:
                raise ValueError("ownership record names a different thread")
            prepared_raw = raw["prepared"]
            stale = tuple(str(key) for key in raw.get("stale_object_keys") or ())
            if any(not key or key.startswith("_") for key in stale):
                raise ValueError("ownership record contains an invalid stale object key")
            prepared = PreparedWorkspace(
                object_key=str(prepared_raw["object_key"]),
                sha256=str(prepared_raw["sha256"]),
                clean_clone_url=str(prepared_raw["clean_clone_url"]),
                repo_full_name=str(prepared_raw["repo_full_name"]),
                base_sha=str(prepared_raw["base_sha"]),
                materialized_head=str(
                    prepared_raw.get("materialized_head", prepared_raw["base_sha"])
                ),
                checkout_mode=int(prepared_raw["checkout_mode"]),
                reference=WorkspaceRef.decode(str(prepared_raw["reference"])),
            )
            expires_at_epoch = int(raw["expires_at_epoch"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise WorkspacePreparationError(
                "ownership-ledger", "private workspace ownership record is invalid"
            ) from exc
        if not thread_key or not prepared.object_key or prepared.object_key.startswith("_"):
            raise WorkspacePreparationError(
                "ownership-ledger", "private workspace ownership record is incomplete"
            )
        return cls(
            thread_key=thread_key,
            prepared=prepared,
            expires_at_epoch=expires_at_epoch,
            stale_object_keys=stale,
        )


@dataclass(frozen=True)
class WorkspaceReapCandidate:
    """Exact expired ledger observed before its potentially slow object cleanup."""

    ownership: _WorkspaceOwnership
    deleted_object_keys: tuple[str, ...]


class WorkspaceCloneLimiter:
    """Process-wide bounded clone semaphore with observable active count."""

    def __init__(self, *, max_concurrent: int) -> None:
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")
        self._semaphore = threading.BoundedSemaphore(max_concurrent)
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    def acquire(self, label: str, *, timeout_seconds: float | None = None) -> _CloneSlot:
        acquired = self._semaphore.acquire(timeout=timeout_seconds)
        if not acquired:
            raise WorkspaceStageTimeout("clone-slot", timeout_seconds or 0)
        with self._lock:
            self._active += 1
        return _CloneSlot(self, label)

    def _release(self) -> None:
        with self._lock:
            if self._active <= 0:
                raise RuntimeError("workspace clone slot released twice")
            self._active -= 1
        self._semaphore.release()


class _CloneSlot(AbstractContextManager[None]):
    def __init__(self, limiter: WorkspaceCloneLimiter, label: str) -> None:
        self._limiter = limiter
        self._label = label
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._limiter._release()

    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: object) -> None:
        self.release()


def _safe_member(member: tarfile.TarInfo) -> None:
    path = PurePosixPath(member.name)
    if path.is_absolute() or member.name.startswith("/"):
        raise WorkspaceArchiveError(f"absolute archive path refused: {member.name!r}")
    if ".." in path.parts:
        raise WorkspaceArchiveError(f"archive path traversal refused: {member.name!r}")
    if member.issym() or member.islnk():
        if _link_escapes(member.name, member.linkname):
            raise WorkspaceArchiveError(f"escaping archive link refused: {member.name!r}")
        return
    if not (member.isfile() or member.isdir()):
        raise WorkspaceArchiveError(f"archive special file refused: {member.name!r}")


def _link_escapes(member_name: str, link_name: str) -> bool:
    target = PurePosixPath(link_name)
    if target.is_absolute() or link_name.startswith("/"):
        return True
    parts = list(PurePosixPath(member_name).parent.parts)
    for part in target.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return True
            parts.pop()
        else:
            parts.append(part)
    return False


def _spool_chunks(
    chunks: Iterable[bytes], *, maximum: int, scratch_root: Path | None = None
) -> tuple[Any, int]:
    spool = tempfile.SpooledTemporaryFile(
        max_size=8 * 1024 * 1024,
        dir=str(scratch_root) if scratch_root is not None else None,
    )
    total = 0
    try:
        for chunk in chunks:
            total += len(chunk)
            if total > maximum:
                raise WorkspaceArchiveError("archive exceeds compressed-size limit")
            spool.write(chunk)
        spool.seek(0)
        return spool, total
    except Exception:
        spool.close()
        raise


def validate_workspace_archive(
    chunks: Iterable[bytes],
    *,
    limits: WorkspaceLimits,
    scratch_root: Path | None = None,
) -> None:
    """Validate normalized archive structure and decompression bounds."""

    spool, compressed = _spool_chunks(
        chunks, maximum=limits.max_archive_bytes, scratch_root=scratch_root
    )
    uncompressed = 0
    members = 0
    try:
        try:
            with tarfile.open(fileobj=spool, mode="r:*") as archive:
                for member in archive:
                    members += 1
                    if members > limits.max_members:
                        raise WorkspaceArchiveError("archive member limit exceeded")
                    _safe_member(member)
                    uncompressed += member.size
                    if uncompressed > limits.max_checkout_bytes:
                        raise WorkspaceArchiveError("archive uncompressed-size limit exceeded")
        except (tarfile.TarError, EOFError) as exc:
            raise WorkspaceArchiveError("invalid tar archive") from exc
        ratio = uncompressed / max(1, compressed)
        if ratio > limits.max_compression_ratio:
            raise WorkspaceArchiveError("archive compression ratio limit exceeded")
    finally:
        spool.close()


class WorkspacePreparer:
    """Clone, sanitize, normalize, upload, and sign one workspace."""

    def __init__(
        self,
        *,
        credentials: Any,
        commands: CommandPort,
        objects: WorkspaceObjectPort,
        scratch_root: Path,
        limits: WorkspaceLimits,
        clock: Callable[[], float] | None = None,
        limiter: WorkspaceCloneLimiter | None = None,
    ) -> None:
        self.credentials = credentials
        self.commands = commands
        self.objects = objects
        self.scratch_root = scratch_root
        self.limits = limits
        # Test and alternate wiring may pass None explicitly; treat it exactly
        # like omission so production always has a monotonic aggregate clock.
        # A supplied deterministic clock is preserved for deadline testing.
        self.clock = clock or time.monotonic
        self.limiter = limiter or WorkspaceCloneLimiter(max_concurrent=limits.max_concurrent_clones)

    def prepare(
        self,
        *,
        deployment_id: uuid.UUID,
        thread_key: str,
        generation: str,
        branch: str | None = None,
        expected_head: str | None = None,
        detached_head: str | None = None,
    ) -> PreparedWorkspace:
        started = self.clock()
        real_started = time.monotonic()
        object_key: str | None = None
        self.scratch_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        scratch = Path(tempfile.mkdtemp(prefix="claim-", dir=self.scratch_root))
        os.chmod(scratch, 0o700)
        checkout = scratch / "checkout"
        archive_path = scratch / "workspace.tar.gz"
        try:
            try:
                self.commands.require("git")
            except FileNotFoundError as exc:
                raise WorkspacePreparationError(
                    "git-preflight", "git is absent from the shipped worker image"
                ) from exc

            with self.limiter.acquire(
                f"{thread_key}:{generation}",
                timeout_seconds=self._real_remaining(real_started),
            ):
                credential = self.credentials.redeem(deployment_id, thread_key)
                clone_env = {
                    "GIT_TERMINAL_PROMPT": "0",
                    # Kept alongside the count/key/value form so older supported
                    # git releases that honor one process-config mechanism but
                    # not the other still refuse redirects. It carries no secret.
                    "GIT_CONFIG_PARAMETERS": "'http.followRedirects=false'",
                    "GIT_CONFIG_COUNT": "2",
                    "GIT_CONFIG_KEY_0": "http.followRedirects",
                    "GIT_CONFIG_VALUE_0": "false",
                    "GIT_CONFIG_KEY_1": "http.https://github.com/.extraHeader",
                    "GIT_CONFIG_VALUE_1": f"Authorization: {credential.authorization_header}",
                }
                try:
                    clone_argv = [
                        "git",
                        "clone",
                        "--depth=1",
                        "--single-branch",
                        "--no-tags",
                    ]
                    if branch is not None:
                        clone_argv.extend(["--branch", branch])
                    clone_argv.extend([credential.clone_url, str(checkout)])
                    self.commands.run(
                        clone_argv,
                        env=clone_env,
                        timeout_seconds=min(
                            self.limits.clone_timeout_seconds,
                            self._real_remaining(real_started),
                        ),
                    )
                    pin = detached_head
                    if pin is None and credential.revision and re.fullmatch(
                        r"[0-9a-f]{40}", credential.revision
                    ):
                        pin = credential.revision
                    if pin is not None:
                        self.commands.run(
                            [
                                "git",
                                "fetch",
                                "--depth=1",
                                "--no-tags",
                                "origin",
                                pin,
                            ],
                            cwd=checkout,
                            env=clone_env,
                            timeout_seconds=min(
                                self.limits.clone_timeout_seconds,
                                self._real_remaining(real_started),
                            ),
                        )
                        self.commands.run(
                            ["git", "checkout", "--detach", pin],
                            cwd=checkout,
                            timeout_seconds=min(
                                self.limits.clone_timeout_seconds,
                                self._real_remaining(real_started),
                            ),
                        )
                except TimeoutError as exc:
                    raise WorkspaceStageTimeout("clone", self.limits.clone_timeout_seconds) from exc
                except WorkspacePreparationError as exc:
                    raise WorkspacePreparationError(
                        "clone", "git refused the server-derived repository checkout"
                    ) from exc
                os.chmod(checkout, 0o700)
                self._check_total(started)

                # Defense in depth: header auth should leave a clean origin, but
                # observed git/credential variants have persisted an authenticated
                # URL.  The explicit rewrite and config scan are both mandatory.
                self.commands.run(
                    ["git", "remote", "set-url", "origin", credential.clone_url],
                    cwd=checkout,
                    timeout_seconds=self.limits.archive_timeout_seconds,
                )
                origin = self.commands.run(
                    ["git", "config", "--get", "remote.origin.url"],
                    cwd=checkout,
                    timeout_seconds=self.limits.archive_timeout_seconds,
                ).stdout.strip()
                self._verify_sanitized_origin(checkout, credential, origin)
                base_sha = self.commands.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=checkout,
                    timeout_seconds=self.limits.archive_timeout_seconds,
                ).stdout.strip()
                if expected_head is not None and base_sha != expected_head:
                    raise WorkspacePreparationError(
                        "lineage-checkout",
                        "checkout does not match the expected lineage head",
                    )

                checkout_bytes = self._checkout_size(checkout)
                if checkout_bytes > self.limits.max_checkout_bytes:
                    raise WorkspacePreparationError(
                        "checkout-size",
                        f"checkout exceeds {self.limits.max_checkout_bytes} bytes",
                    )

                try:
                    archive_started = time.monotonic()
                    chunks = self.archive_checkout(checkout, archive_path=archive_path)
                    # Consume here so archive failures are assigned to the archive
                    # stage before the storage adapter sees any object bytes.
                    with archive_path.open("wb") as output:
                        archive_bytes = 0
                        for chunk in chunks:
                            archive_bytes += len(chunk)
                            if archive_bytes > self.limits.max_archive_bytes:
                                raise WorkspacePreparationError(
                                    "archive-size",
                                    f"archive exceeds {self.limits.max_archive_bytes} bytes",
                                )
                            output.write(chunk)
                    if time.monotonic() - archive_started > self.limits.archive_timeout_seconds:
                        raise WorkspaceStageTimeout("archive", self.limits.archive_timeout_seconds)
                except TimeoutError as exc:
                    raise WorkspaceStageTimeout(
                        "archive", self.limits.archive_timeout_seconds
                    ) from exc
                self._check_total(started)

                validate_workspace_archive(
                    self._file_chunks(archive_path),
                    limits=self.limits,
                    scratch_root=self.scratch_root,
                )
                digest = self._hash_file(archive_path)
                object_key = (
                    f"{deployment_id}/{uuid.uuid4().hex}-"
                    f"{hashlib.sha256(generation.encode()).hexdigest()[:12]}.tar.gz"
                )
                try:
                    upload_started = time.monotonic()
                    self.objects.put_stream(object_key, self._file_chunks(archive_path))
                    if time.monotonic() - upload_started > self.limits.upload_timeout_seconds:
                        raise WorkspaceStageTimeout("upload", self.limits.upload_timeout_seconds)
                except TimeoutError as exc:
                    raise WorkspaceStageTimeout(
                        "upload", self.limits.upload_timeout_seconds
                    ) from exc
                self._check_total(started)

            assert object_key is not None
            signed_url = self.objects.presign_get(
                object_key, expires_seconds=self.limits.reference_ttl_seconds
            )
            reference = WorkspaceRef(
                url=signed_url,
                sha256=digest,
                expires_at_epoch=int(time.time()) + self.limits.reference_ttl_seconds,
            )
            return PreparedWorkspace(
                object_key=object_key,
                sha256=digest,
                clean_clone_url=credential.clone_url,
                repo_full_name=credential.repo_full_name,
                base_sha=base_sha,
                materialized_head=base_sha,
                checkout_mode=checkout.stat().st_mode,
                reference=reference,
            )
        except Exception:
            if object_key is not None:
                try:
                    self.objects.delete(object_key)
                except Exception:
                    pass
            raise
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    def prepare_lineage(
        self,
        *,
        deployment_id: uuid.UUID,
        thread_key: str,
        generation: str,
        branch: str,
        expected_head: str,
    ) -> PreparedWorkspace:
        """Materialize one stored lineage branch at its exact fenced head."""

        if not branch or re.fullmatch(r"[0-9a-f]{40}", expected_head) is None:
            raise WorkspacePreparationError(
                "lineage-checkout", "lineage branch or expected head is invalid"
            )
        return self.prepare(
            deployment_id=deployment_id,
            thread_key=thread_key,
            generation=generation,
            branch=branch,
            expected_head=expected_head,
        )

    def prepare_lineage_base(
        self,
        *,
        deployment_id: uuid.UUID,
        thread_key: str,
        generation: str,
        expected_base: str,
    ) -> PreparedWorkspace:
        """Materialize a headless lineage at its proposal base without a branch."""

        if re.fullmatch(r"[0-9a-f]{40}", expected_base) is None:
            raise WorkspacePreparationError(
                "lineage-checkout", "lineage base is invalid"
            )
        return self.prepare(
            deployment_id=deployment_id,
            thread_key=thread_key,
            generation=generation,
            expected_head=expected_base,
            detached_head=expected_base,
        )

    def _check_total(self, started: float) -> None:
        if self.clock() - started > self.limits.total_timeout_seconds:
            raise WorkspaceStageTimeout("total", self.limits.total_timeout_seconds)

    def _real_remaining(self, started: float) -> float:
        remaining = self.limits.total_timeout_seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise WorkspaceStageTimeout("total", self.limits.total_timeout_seconds)
        return remaining

    @staticmethod
    def _verify_sanitized_origin(
        checkout: Path, credential: WorkspaceCredential, observed_origin: str
    ) -> None:
        if observed_origin != credential.clone_url:
            raise WorkspacePreparationError(
                "origin-sanitization", "remote.origin.url is not the credential-free URL"
            )
        config_path = checkout / ".git" / "config"
        try:
            config = config_path.read_text(errors="replace")
        except OSError as exc:
            raise WorkspacePreparationError(
                "origin-sanitization", ".git/config could not be inspected"
            ) from exc
        forbidden = (credential.authorization_header, "@github.com")
        if any(value and value in config for value in forbidden):
            raise WorkspacePreparationError(
                "origin-sanitization", ".git/config still contains credential material"
            )
        if config.count(credential.clone_url) != 1:
            raise WorkspacePreparationError(
                "origin-sanitization", ".git/config does not contain exactly one clean origin"
            )

    def _checkout_size(self, checkout: Path) -> int:
        total = 0
        members = 0
        for path in checkout.rglob("*"):
            members += 1
            if members > self.limits.max_members:
                raise WorkspacePreparationError(
                    "checkout-members", f"checkout exceeds {self.limits.max_members} members"
                )
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                relative = path.relative_to(checkout).as_posix()
                if _link_escapes(relative, os.readlink(path)):
                    raise WorkspacePreparationError(
                        "checkout-normalization", f"escaping checkout link refused: {relative}"
                    )
                continue
            if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
                raise WorkspacePreparationError(
                    "checkout-normalization", f"checkout special file refused: {path.name}"
                )
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
                if total > self.limits.max_checkout_bytes:
                    return total
        return total

    def archive_checkout(self, checkout: Path, *, archive_path: Path) -> Iterator[bytes]:
        """Yield a deterministic gzip tar without links or special files."""

        del archive_path  # caller owns the final spool path
        with tempfile.NamedTemporaryFile(
            prefix="archive-", suffix=".tar.gz", dir=self.scratch_root
        ) as temp:
            with gzip.GzipFile(fileobj=temp, mode="wb", compresslevel=6, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w") as archive:
                    for path in sorted(checkout.rglob("*"), key=lambda item: item.as_posix()):
                        relative = path.relative_to(checkout).as_posix()
                        info = path.lstat()
                        member = tarfile.TarInfo(relative)
                        member.uid = 0
                        member.gid = 0
                        member.uname = ""
                        member.gname = ""
                        member.mtime = 0
                        member.mode = stat.S_IMODE(info.st_mode)
                        if stat.S_ISDIR(info.st_mode):
                            member.type = tarfile.DIRTYPE
                            archive.addfile(member)
                        elif stat.S_ISLNK(info.st_mode):
                            member.type = tarfile.SYMTYPE
                            member.linkname = os.readlink(path)
                            _safe_member(member)
                            archive.addfile(member)
                        elif stat.S_ISREG(info.st_mode):
                            member.size = info.st_size
                            with path.open("rb") as source:
                                archive.addfile(member, source)
                        else:
                            raise WorkspacePreparationError(
                                "checkout-normalization",
                                f"unsupported checkout member {relative!r}",
                            )
            temp.flush()
            temp.seek(0)
            while chunk := temp.read(1024 * 1024):
                yield chunk

    @staticmethod
    def _file_chunks(path: Path) -> Iterator[bytes]:
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                yield chunk

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        for chunk in WorkspacePreparer._file_chunks(path):
            digest.update(chunk)
        return digest.hexdigest()

    def verify(self, prepared: PreparedWorkspace) -> None:
        digest = hashlib.sha256()
        for chunk in self.objects.get_stream(prepared.object_key):
            digest.update(chunk)
        if digest.hexdigest() != prepared.sha256:
            raise WorkspacePreparationError(
                "digest-verification", "private workspace object digest mismatch"
            )

    def delete(self, prepared: PreparedWorkspace) -> None:
        self.objects.delete(prepared.object_key)


@dataclass(frozen=True)
class WorkspaceClaimResult:
    prepared: PreparedWorkspace
    handle: Any


class WorkspaceClaimCoordinator:
    """Prepare before claim/resume and durably own private workspace bases.

    Ownership lives beside the archives in the private workspace bucket rather
    than solely in this process. A replacement worker can therefore validate a
    publication against the existing base, release it, or reap it after the
    route lease expires. Superseded keys remain in the ledger until deletion is
    confirmed, making cleanup restart-safe as well.
    """

    def __init__(
        self,
        *,
        preparer: WorkspacePreparer,
        substrate: Any,
        suspended_error: type[Exception] | tuple[type[Exception], ...] = (),
        ownership_ttl_seconds: int = 24 * 60 * 60,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if ownership_ttl_seconds <= 0:
            raise ValueError("ownership_ttl_seconds must be positive")
        self.preparer = preparer
        self.substrate = substrate
        self._suspended_errors = (
            (suspended_error,) if isinstance(suspended_error, type) else suspended_error
        )
        self._ownership_ttl_seconds = ownership_ttl_seconds
        self._wall_clock = wall_clock
        self._current: dict[str, PreparedWorkspace] = {}
        self._lock = threading.Lock()

    def claim_or_resume_with_handle(
        self,
        *,
        thread_key: str,
        deployment_id: uuid.UUID,
        env: dict[str, str] | None = None,
        agent_name: str | None = None,
        runner_resources: dict[str, Any] | None = None,
        repo_full_name: str | None = None,
        replace_handle: Any | None = None,
        revalidate_before_handoff: Callable[[], None] | None = None,
        validate_candidate: Callable[[Any], None] | None = None,
        lineage_branch: str | None = None,
        lineage_head: str | None = None,
        lineage_base_sha: str | None = None,
        publication_visible_outcome_revision: int = 0,
        fresh_only: bool = False,
    ) -> WorkspaceClaimResult:
        """Prepare once, then cold-claim or resume a suspended route.

        ``fresh_only`` forwards to the substrate claim so an attachment turn
        never adopts a live runner that did not boot with its env (#2739).

        ``revalidate_before_handoff`` is the late-replacement linearization
        guard. It runs after the archive is verified and durably staged,
        immediately before the substrate begins replacing the old route. A
        refusal raises through the ordinary pre-exposure rollback path, so the
        old route stays authoritative and the newly staged archive is not
        orphaned. ``validate_candidate`` runs after the replacement runner is
        ready but before its route CAS; refusal follows the same rollback path.
        """

        generation = uuid.uuid4().hex
        if (lineage_branch is None) != (lineage_head is None):
            raise WorkspacePreparationError(
                "lineage-checkout", "lineage branch and head must be supplied together"
            )
        if lineage_base_sha is not None and lineage_head is not None:
            raise WorkspacePreparationError(
                "lineage-checkout", "lineage head and base are mutually exclusive"
            )
        if lineage_branch is not None and lineage_head is not None:
            prepared = self.preparer.prepare_lineage(
                deployment_id=deployment_id,
                thread_key=thread_key,
                generation=generation,
                branch=lineage_branch,
                expected_head=lineage_head,
            )
        elif lineage_base_sha is not None:
            prepared = self.preparer.prepare_lineage_base(
                deployment_id=deployment_id,
                thread_key=thread_key,
                generation=generation,
                expected_base=lineage_base_sha,
            )
        else:
            prepared = self.preparer.prepare(
                deployment_id=deployment_id,
                thread_key=thread_key,
                generation=generation,
            )
        previous_ownership: _WorkspaceOwnership | None = None
        ownership_staged = False
        sandbox_exposed = False
        try:
            self.preparer.verify(prepared)
            # Store the exact base facts before the substrate can expose a
            # sandbox using this object's claim capability. If the worker dies
            # after the claim starts, a replacement can still recover the base
            # used by that sandbox and all superseded objects remain accounted
            # for in the same durable record.
            previous_ownership = self._stage_ownership(thread_key, prepared)
            ownership_staged = True
            claim_env = {**(env or {}), **prepared.claim_env()}
            if replace_handle is not None:
                if repo_full_name is None:
                    raise WorkspacePreparationError(
                        "claim", "late workspace handoff requires a selected repository"
                    )
                if revalidate_before_handoff is not None:
                    revalidate_before_handoff()
                candidate_guard = (
                    {"validate_candidate": validate_candidate}
                    if validate_candidate is not None
                    else {}
                )
                handle = self.substrate.handoff(
                    thread_key,
                    expected=replace_handle,
                    env=claim_env,
                    workspace_repo=repo_full_name,
                    workspace_materialized_head=prepared.materialized_head,
                    publication_visible_outcome_revision=(
                        publication_visible_outcome_revision
                    ),
                    agent_name=agent_name,
                    runner_resources=runner_resources,
                    **candidate_guard,
                )
            else:
                try:
                    handle = self.substrate.claim(
                        thread_key,
                        env=claim_env,
                        agent_name=agent_name,
                        runner_resources=runner_resources,
                        workspace_repo=repo_full_name,
                        workspace_materialized_head=prepared.materialized_head,
                        publication_visible_outcome_revision=(
                            publication_visible_outcome_revision
                        ),
                        fresh_only=fresh_only,
                    )
                except Exception as exc:
                    # The substrate signal is injected to keep this worker-local
                    # port independent of the concrete Docker/Kubernetes package.
                    if not isinstance(exc, self._suspended_errors):
                        raise
                    handle = self.substrate.resume(
                        thread_key,
                        env=claim_env,
                        agent_name=agent_name,
                        runner_resources=runner_resources,
                        workspace_repo=repo_full_name,
                        workspace_materialized_head=prepared.materialized_head,
                        publication_visible_outcome_revision=(
                            publication_visible_outcome_revision
                        ),
                    )
            sandbox_exposed = True
            self._commit_ownership(thread_key, prepared)
            return WorkspaceClaimResult(prepared=prepared, handle=handle)
        except _OwnershipWriteUncertain:
            # A response-lost write whose read-back also failed may durably name
            # this archive. Retaining it is the only fail-safe choice.
            raise
        except Exception:
            if ownership_staged and not sandbox_exposed:
                self._rollback_ownership(thread_key, prepared, previous_ownership)
            elif not ownership_staged:
                self.preparer.delete(prepared)
            raise

    def select_repository(
        self,
        *,
        thread_key: str,
        deployment_id: uuid.UUID,
        author: str,
        repo_full_name: str | None,
    ) -> str | None:
        """Authorize or reuse the immutable server-side thread selection."""

        return cast(
            "str | None",
            self.preparer.credentials.select(
                deployment_id, thread_key, author, repo_full_name
            ),
        )

    def _stage_ownership(
        self, thread_key: str, prepared: PreparedWorkspace
    ) -> _WorkspaceOwnership | None:
        with self._lock:
            old = self._load_ownership(thread_key)
            stale = set(old.stale_object_keys if old is not None else ())
            if old is not None and old.prepared.object_key != prepared.object_key:
                stale.add(old.prepared.object_key)
            ownership = _WorkspaceOwnership(
                thread_key=thread_key,
                prepared=prepared,
                expires_at_epoch=self._lease_expiry(),
                stale_object_keys=tuple(sorted(stale)),
            )
            self._store_ownership(ownership)
            self._current[thread_key] = prepared
            return old

    def _commit_ownership(self, thread_key: str, prepared: PreparedWorkspace) -> None:
        with self._lock:
            ownership = self._load_ownership(thread_key)
            if ownership is None or ownership.prepared.object_key != prepared.object_key:
                raise WorkspacePreparationError(
                    "ownership-ledger", "workspace ownership changed during sandbox claim"
                )
            self._cleanup_stale(ownership)

    def _rollback_ownership(
        self,
        thread_key: str,
        prepared: PreparedWorkspace,
        previous: _WorkspaceOwnership | None,
    ) -> None:
        """Restore the exact prior ledger before discarding a failed claim's object."""

        with self._lock:
            ownership = self._load_ownership(thread_key)
            if ownership is None or ownership.prepared.object_key != prepared.object_key:
                raise WorkspacePreparationError(
                    "ownership-ledger", "workspace ownership changed during claim rollback"
                )
            if previous is None:
                self.preparer.objects.delete(self._ownership_key(thread_key))
                self._current.pop(thread_key, None)
            else:
                self._store_ownership(previous)
                self._current[thread_key] = previous.prepared
        # Delete only after durable ownership points away from this object. A
        # failed ledger restore deliberately retains the staged object rather
        # than creating an unowned archive or deleting a still-referenced base.
        self.preparer.delete(prepared)

    def release(self, thread_key: str) -> None:
        with self._lock:
            self._current.pop(thread_key, None)
            ownership = self._load_ownership(thread_key)
            if ownership is None:
                return
            self._delete_owned_objects(ownership)
            self.preparer.objects.delete(self._ownership_key(thread_key))

    def touch(self, thread_key: str, *, ttl_seconds: int | None = None) -> bool:
        """Refresh a durable workspace lease when sandbox affinity is refreshed."""

        ttl = self._ownership_ttl_seconds if ttl_seconds is None else ttl_seconds
        if ttl <= 0:
            raise ValueError("workspace ownership TTL must be positive")
        with self._lock:
            ownership = self._load_ownership(thread_key)
            if ownership is None:
                return False
            self._store_ownership(
                _WorkspaceOwnership(
                    thread_key=thread_key,
                    prepared=ownership.prepared,
                    expires_at_epoch=int(self._wall_clock()) + ttl,
                    stale_object_keys=ownership.stale_object_keys,
                )
            )
            return True

    def enumerate_expired(self) -> list[str]:
        """Snapshot expired thread ids without mutating their durable ledgers."""

        expired: list[str] = []
        now = int(self._wall_clock())
        with self._lock:
            for ledger_key in tuple(self.preparer.objects.list_keys(_OWNERSHIP_PREFIX)):
                ownership = self._load_ownership_key(ledger_key)
                if ownership.expires_at_epoch <= now:
                    expired.append(ownership.thread_key)
        return sorted(expired)

    def begin_expired_reap(self, thread_key: str) -> WorkspaceReapCandidate | None:
        """Re-read one expiry and delete only the objects named by that snapshot."""

        with self._lock:
            ownership = self._load_ownership(thread_key)
            if ownership is None or ownership.expires_at_epoch > int(self._wall_clock()):
                return None
            deleted = tuple(self._delete_owned_objects(ownership))
            return WorkspaceReapCandidate(
                ownership=ownership,
                deleted_object_keys=deleted,
            )

    def finish_expired_reap(self, candidate: WorkspaceReapCandidate) -> bool:
        """Delete only the unchanged ledger after the caller fences its lock token."""

        ownership = candidate.ownership
        with self._lock:
            current = self._load_ownership(ownership.thread_key)
            if current != ownership:
                return False
            self.preparer.objects.delete(self._ownership_key(ownership.thread_key))
            self._current.pop(ownership.thread_key, None)
            return True

    def current(self, thread_key: str) -> PreparedWorkspace | None:
        """The trusted base facts retained for this active/suspended session."""

        with self._lock:
            ownership = self._load_ownership(thread_key)
            if ownership is None:
                self._current.pop(thread_key, None)
                return None
            if ownership.expires_at_epoch <= int(self._wall_clock()):
                # Reaping is a separate enumerate -> distributed-lock -> exact
                # re-read operation. A read path must never race a fresh claim
                # by mutating an expiry observed before that claim committed.
                self._current.pop(thread_key, None)
                return None
            self._current[thread_key] = ownership.prepared
            return ownership.prepared

    def stream_current_base(self, thread_key: str) -> Iterator[bytes]:
        """Rehash, then stream the private base object for patch validation."""

        prepared = self.current(thread_key)
        if prepared is None:
            raise WorkspacePreparationError(
                "base-lookup", "thread has no prepared repository workspace"
            )
        self.preparer.verify(prepared)
        yield from self.preparer.objects.get_stream(prepared.object_key)

    def _lease_expiry(self) -> int:
        return int(self._wall_clock()) + self._ownership_ttl_seconds

    @staticmethod
    def _ownership_key(thread_key: str) -> str:
        digest = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()
        return f"{_OWNERSHIP_PREFIX}/{digest}.json"

    def _store_ownership(self, ownership: _WorkspaceOwnership) -> None:
        key = self._ownership_key(ownership.thread_key)
        try:
            self.preparer.objects.put_stream(key, (ownership.encode(),))
        except Exception as write_exc:
            # Object stores can commit PutObject and then lose the response. A
            # read-after-write match adopts that committed outcome. If read-back
            # itself is unavailable, preserve every possibly referenced archive
            # and fail loudly rather than guessing that the put did not land.
            try:
                observed = self._load_ownership_key(
                    key,
                    expected_thread_key=ownership.thread_key,
                )
            except Exception as read_exc:
                if self._is_missing_object(read_exc):
                    raise write_exc from None
                raise _OwnershipWriteUncertain() from write_exc
            if observed == ownership:
                return
            raise write_exc

    def _load_ownership(self, thread_key: str) -> _WorkspaceOwnership | None:
        key = self._ownership_key(thread_key)
        try:
            return self._load_ownership_key(key, expected_thread_key=thread_key)
        except Exception as exc:
            if self._is_missing_object(exc):
                return None
            raise

    def _load_ownership_key(
        self, key: str, *, expected_thread_key: str | None = None
    ) -> _WorkspaceOwnership:
        payload = b"".join(self.preparer.objects.get_stream(key))
        if len(payload) > 64 * 1024:
            raise WorkspacePreparationError(
                "ownership-ledger", "private workspace ownership record is oversized"
            )
        return _WorkspaceOwnership.decode(payload, expected_thread_key=expected_thread_key)

    def _cleanup_stale(self, ownership: _WorkspaceOwnership) -> None:
        remaining: list[str] = []
        for key in ownership.stale_object_keys:
            try:
                self.preparer.objects.delete(key)
            except Exception:
                remaining.append(key)
        if tuple(remaining) != ownership.stale_object_keys:
            self._store_ownership(
                _WorkspaceOwnership(
                    thread_key=ownership.thread_key,
                    prepared=ownership.prepared,
                    expires_at_epoch=ownership.expires_at_epoch,
                    stale_object_keys=tuple(remaining),
                )
            )

    def _delete_owned_objects(self, ownership: _WorkspaceOwnership) -> list[str]:
        keys = (ownership.prepared.object_key, *ownership.stale_object_keys)
        deleted: list[str] = []
        for key in dict.fromkeys(keys):
            self.preparer.objects.delete(key)
            deleted.append(key)
        return deleted

    @staticmethod
    def _is_missing_object(exc: Exception) -> bool:
        if isinstance(exc, (KeyError, FileNotFoundError)):
            return True
        response = getattr(exc, "response", None)
        if not isinstance(response, Mapping):
            return False
        error = response.get("Error")
        code = error.get("Code") if isinstance(error, Mapping) else None
        return str(code) in {"404", "NoSuchKey", "NotFound"}
