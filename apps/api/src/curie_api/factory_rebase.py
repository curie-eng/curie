"""Bounded, reproducible Git replay and exact leased publication."""

from __future__ import annotations

import asyncio
import errno
import math
import os
import re
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .forges.types import CredentialHeader, RepositoryRef
from .repository_access import IssuedCredential

_SHA = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})")


@dataclass(frozen=True)
class RebaseSnapshot:
    transition_id: uuid.UUID
    repository: RepositoryRef
    branch: str
    old_base_sha: str
    old_head_sha: str
    target_ref: str
    target_sha: str
    committer_name: str
    committer_email: str
    committer_time: datetime
    max_seconds: float = 60.0
    max_bytes: int = 512 * 1024 * 1024
    work_dir: Path | None = None


@dataclass(frozen=True)
class PreparedRebase:
    head_sha: str


@dataclass(frozen=True)
class ObservedPush:
    head_sha: str


class RebaseRefused(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _sha(value: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise RebaseRefused("invalid_snapshot")
    return value.lower()


def _head_ref(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 1024
        or value.startswith("-")
        or value == "HEAD"
    ):
        raise RebaseRefused("invalid_ref")
    return f"refs/heads/{value}"


def _validate(snapshot: RebaseSnapshot, credential: IssuedCredential) -> None:
    shas = [
        _sha(snapshot.old_base_sha),
        _sha(snapshot.old_head_sha),
        _sha(snapshot.target_sha),
    ]
    if (
        len({len(value) for value in shas}) != 1
        or not isinstance(snapshot.transition_id, uuid.UUID)
        or not math.isfinite(snapshot.max_seconds)
        or snapshot.max_seconds <= 0
        or not isinstance(snapshot.max_bytes, int)
        or snapshot.max_bytes <= 0
        or snapshot.committer_time.tzinfo is None
        or snapshot.committer_time.utcoffset() is None
    ):
        raise RebaseRefused("invalid_snapshot")
    for value in (snapshot.committer_name, snapshot.committer_email):
        if not value.strip() or any(char in value for char in "\r\n\0<>"):
            raise RebaseRefused("invalid_snapshot")
    _head_ref(snapshot.branch)
    _head_ref(snapshot.target_ref)
    _transport_header(credential)


def _transport_header(credential: IssuedCredential) -> str:
    value = credential.authorization_header
    if not value or any(char in value for char in "\r\n\0"):
        raise RebaseRefused("invalid_credential")
    if credential.header_form == CredentialHeader.AUTHORIZATION_BASIC:
        if not value.lower().startswith("basic ") or not value[6:].strip():
            raise RebaseRefused("invalid_credential")
        header = f"Authorization: {value}"
    elif credential.header_form == CredentialHeader.AUTHORIZATION_BEARER:
        if not value.lower().startswith("bearer ") or not value[7:].strip():
            raise RebaseRefused("invalid_credential")
        header = f"Authorization: {value}"
    elif credential.header_form == CredentialHeader.PRIVATE_TOKEN:
        header = f"PRIVATE-TOKEN: {value}"
    else:
        raise RebaseRefused("invalid_credential")
    try:
        clone = urlsplit(credential.clone_url)
        origin = urlsplit(credential.origin)
        _ = clone.port, origin.port
    except ValueError:
        raise RebaseRefused("invalid_credential") from None
    if clone.scheme in {"http", "https"}:
        if (
            origin.scheme != clone.scheme
            or not clone.hostname
            or not origin.hostname
            or clone.username is not None
            or clone.password is not None
            or origin.username is not None
            or origin.password is not None
            or clone.query
            or clone.fragment
            or origin.query
            or origin.fragment
            or credential.origin.endswith("/")
            or not credential.clone_url.startswith(f"{credential.origin}/")
            or any(ord(char) <= 32 or ord(char) == 127 for char in credential.clone_url)
            or any(ord(char) <= 32 or ord(char) == 127 for char in credential.origin)
        ):
            raise RebaseRefused("invalid_credential")
    elif (
        clone.scheme
        or not Path(credential.clone_url).is_absolute()
        or credential.clone_url != credential.origin
        or any(char in credential.clone_url for char in "\r\n\0")
    ):
        raise RebaseRefused("invalid_credential")
    return header


class _Git:
    def __init__(
        self,
        snapshot: RebaseSnapshot,
        credential: IssuedCredential,
        root: Path,
        deadline: float,
        cancelled: threading.Event,
    ) -> None:
        self.snapshot = snapshot
        self.clone_url = credential.clone_url
        self.root = root
        self.checkout = root / "repository"
        self.checkout.mkdir(mode=0o700)
        temporary = root / "temporary"
        temporary.mkdir(mode=0o700)
        self.deadline = deadline
        self.cancelled = cancelled
        self.env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("GIT_", "GCM_", "CURL_"))
        }
        date = snapshot.committer_time.astimezone(UTC).isoformat(timespec="seconds")
        self.env.update(
            {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_SYSTEM": os.devnull,
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_ASKPASS": "false",
                "GIT_EDITOR": "false",
                "GIT_SEQUENCE_EDITOR": "false",
                "GIT_NO_REPLACE_OBJECTS": "1",
                "GIT_ATTR_NOSYSTEM": "1",
                "GIT_COMMITTER_NAME": snapshot.committer_name,
                "GIT_COMMITTER_EMAIL": snapshot.committer_email,
                "GIT_COMMITTER_DATE": date,
                "LC_ALL": "C",
                "TMPDIR": str(temporary),
            }
        )
        configuration = [
            ("credential.helper", ""),
            ("core.hooksPath", os.devnull),
            ("core.fsmonitor", "false"),
            ("core.attributesFile", os.devnull),
            ("core.logAllRefUpdates", "false"),
            ("http.followRedirects", "false"),
            ("http.sslVerify", "true"),
            ("http.extraHeader", ""),
            (f"http.{credential.origin}/.extraHeader", _transport_header(credential)),
            ("commit.gpgSign", "false"),
            ("tag.gpgSign", "false"),
            ("rebase.backend", "merge"),
            ("rebase.autoStash", "false"),
            ("rebase.updateRefs", "false"),
            ("rerere.enabled", "false"),
            ("protocol.allow", "never"),
            ("protocol.file.allow", "always"),
            ("protocol.http.allow", "always"),
            ("protocol.https.allow", "always"),
            ("fetch.fsckObjects", "true"),
            ("transfer.fsckObjects", "true"),
            ("user.name", snapshot.committer_name),
            ("user.email", snapshot.committer_email),
            ("gc.auto", "0"),
            ("maintenance.auto", "false"),
            ("push.default", "nothing"),
        ]
        self.env["GIT_CONFIG_COUNT"] = str(len(configuration))
        for index, (key, value) in enumerate(configuration):
            self.env[f"GIT_CONFIG_KEY_{index}"] = key
            self.env[f"GIT_CONFIG_VALUE_{index}"] = value
        if credential.ca_bundle_ref is not None:
            self.env["GIT_SSL_CAINFO"] = credential.ca_bundle_ref
        self.marker_author = {
            "GIT_AUTHOR_NAME": snapshot.committer_name,
            "GIT_AUTHOR_EMAIL": snapshot.committer_email,
            "GIT_AUTHOR_DATE": date,
        }

    def check_limits(self) -> None:
        if self.cancelled.is_set() or time.monotonic() >= self.deadline:
            raise RebaseRefused("elapsed_limit")
        total = 0
        directories = [self.root]
        while directories:
            if self.cancelled.is_set() or time.monotonic() >= self.deadline:
                raise RebaseRefused("elapsed_limit")
            try:
                with os.scandir(directories.pop()) as entries:
                    for entry in entries:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                directories.append(Path(entry.path))
                            else:
                                total += entry.stat(follow_symlinks=False).st_size
                        except FileNotFoundError:
                            continue
                        if total > self.snapshot.max_bytes:
                            raise RebaseRefused("disk_limit")
            except FileNotFoundError:
                continue

    @staticmethod
    def _kill(process: subprocess.Popen[bytes]) -> None:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()

    def command(
        self,
        *arguments: str,
        check: bool = True,
        input_bytes: bytes | None = None,
        marker: bool = False,
    ) -> tuple[int, bytes]:
        self.check_limits()
        output_path = self.root / "command-output"
        try:
            with output_path.open("w+b") as output:
                process = subprocess.Popen(
                    ["git", *arguments],
                    cwd=self.checkout,
                    env=self.env | self.marker_author if marker else self.env,
                    stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                try:
                    if input_bytes is not None:
                        assert process.stdin is not None
                        process.stdin.write(input_bytes)
                        process.stdin.close()
                    while process.poll() is None:
                        self.check_limits()
                        self.cancelled.wait(min(0.02, max(0.0, self.deadline - time.monotonic())))
                    self.check_limits()
                except BaseException:
                    self._kill(process)
                    raise
                output.seek(0)
                result = output.read(self.snapshot.max_bytes + 1)
                if len(result) > self.snapshot.max_bytes:
                    raise RebaseRefused("disk_limit")
            if check and process.returncode != 0:
                raise RebaseRefused("git_failed")
            return process.returncode, result
        finally:
            output_path.unlink(missing_ok=True)

    def initialize(self) -> None:
        for value in (self.snapshot.branch, self.snapshot.target_ref):
            code, _ = self.command("check-ref-format", _head_ref(value), check=False)
            if code:
                raise RebaseRefused("invalid_ref")
        object_format = "sha1" if len(self.snapshot.old_head_sha) == 40 else "sha256"
        self.command(
            "init",
            "--quiet",
            "--template=",
            "--initial-branch=factory-rebase",
            f"--object-format={object_format}",
        )

    def fetch(self, *shas: str) -> None:
        commits = tuple(dict.fromkeys(_sha(value) for value in shas))
        self.command(
            "fetch",
            "--quiet",
            "--no-tags",
            "--no-recurse-submodules",
            "--no-write-fetch-head",
            self.clone_url,
            *commits,
        )
        for commit in commits:
            code, kind = self.command("cat-file", "-t", commit, check=False)
            if code or kind.strip() != b"commit":
                raise RebaseRefused("missing_commit")

    def require_ancestor(self, ancestor: str, descendant: str) -> None:
        code, _ = self.command("merge-base", "--is-ancestor", ancestor, descendant, check=False)
        if code:
            raise RebaseRefused("invalid_ancestry")

    def remote_head(self) -> str:
        reference = _head_ref(self.snapshot.branch)
        code, _ = self.command("check-ref-format", reference, check=False)
        if code:
            raise RebaseRefused("invalid_ref")
        code, output = self.command(
            "ls-remote", "--exit-code", "--refs", self.clone_url, reference, check=False
        )
        if code == 2:
            raise RebaseRefused("head_changed")
        if code:
            raise RebaseRefused("git_failed")
        for line in output.splitlines():
            sha, separator, name = line.partition(b"\t")
            if separator and name == reference.encode():
                try:
                    return _sha(sha.decode("ascii"))
                except (UnicodeError, RebaseRefused):
                    raise RebaseRefused("git_failed") from None
        raise RebaseRefused("head_changed")


def _in_checkout[Result](
    snapshot: RebaseSnapshot,
    credential: IssuedCredential,
    operation: Callable[[_Git], Result],
    cancelled: threading.Event,
) -> Result:
    deadline = time.monotonic() + snapshot.max_seconds
    _validate(snapshot, credential)
    try:
        with tempfile.TemporaryDirectory(
            prefix="curie-rebase-", dir=snapshot.work_dir
        ) as directory:
            git = _Git(snapshot, credential, Path(directory), deadline, cancelled)
            return operation(git)
    except OSError as exc:
        code = "disk_limit" if exc.errno in {errno.ENOSPC, errno.EDQUOT} else "git_failed"
        raise RebaseRefused(code) from None


async def _run[Result](
    snapshot: RebaseSnapshot,
    credential: IssuedCredential,
    operation: Callable[[_Git], Result],
) -> Result:
    cancelled = threading.Event()
    worker = asyncio.create_task(
        asyncio.to_thread(_in_checkout, snapshot, credential, operation, cancelled)
    )
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        cancelled.set()
        with suppress(RebaseRefused):
            await asyncio.shield(worker)
        raise


def _prepare_rebase(git: _Git) -> PreparedRebase:
    snapshot = git.snapshot
    old_base = _sha(snapshot.old_base_sha)
    old_head = _sha(snapshot.old_head_sha)
    target = _sha(snapshot.target_sha)
    git.initialize()
    git.fetch(old_base, old_head, target)
    git.require_ancestor(old_base, old_head)
    source_range = f"{old_base}..{old_head}"
    _, merges = git.command("rev-list", "--merges", source_range)
    if merges.strip():
        raise RebaseRefused("non_linear")
    _, expected_count = git.command("rev-list", "--count", source_range)
    git.command("checkout", "--quiet", "--detach", old_head)
    code, _ = git.command(
        "rebase",
        "--force-rebase",
        "--no-autostash",
        "--no-update-refs",
        "--reapply-cherry-picks",
        "--empty=keep",
        "--keep-empty",
        "--onto",
        target,
        old_base,
        check=False,
    )
    if code:
        if (git.checkout / ".git" / "rebase-merge").is_dir():
            raise RebaseRefused("conflict")
        raise RebaseRefused("git_failed")
    _, head = git.command("rev-parse", "--verify", "HEAD^{commit}")
    head_sha = _sha(head.decode("ascii").strip())
    git.require_ancestor(target, head_sha)
    _, actual_count = git.command("rev-list", "--count", f"{target}..{head_sha}")
    _, dirty = git.command("status", "--porcelain=v1", "-z", "--untracked-files=all")
    if (
        actual_count.strip() != expected_count.strip()
        or dirty
        or (git.checkout / ".git" / "rebase-merge").exists()
        or (git.checkout / ".git" / "rebase-apply").exists()
    ):
        raise RebaseRefused("replay_changed")
    return PreparedRebase(head_sha)


def _prepare_ci_marker(git: _Git, rebased_head_sha: str) -> PreparedRebase:
    rebased_head = _sha(rebased_head_sha)
    target = _sha(git.snapshot.target_sha)
    git.initialize()
    git.fetch(rebased_head, target)
    git.require_ancestor(target, rebased_head)
    _, tree = git.command("rev-parse", "--verify", f"{rebased_head}^{{tree}}")
    _, marker = git.command(
        "commit-tree",
        _sha(tree.decode("ascii").strip()),
        "-p",
        rebased_head,
        input_bytes=f"{git.snapshot.transition_id}\n".encode("ascii"),
        marker=True,
    )
    return PreparedRebase(_sha(marker.decode("ascii").strip()))


def _push(
    git: _Git,
    prepared: PreparedRebase,
    expected_head: str,
    reconstruct: Callable[[_Git], PreparedRebase],
) -> ObservedPush:
    candidate = _sha(prepared.head_sha)
    expected = _sha(expected_head)
    observed = git.remote_head()
    if observed == candidate:
        return ObservedPush(candidate)
    if observed != expected:
        raise RebaseRefused("head_changed")
    if reconstruct(git).head_sha != candidate:
        raise RebaseRefused("candidate_changed")
    reference = _head_ref(git.snapshot.branch)
    code, _ = git.command(
        "push",
        "--porcelain",
        "--no-follow-tags",
        f"--force-with-lease={reference}:{expected}",
        git.clone_url,
        f"{candidate}:{reference}",
        check=False,
    )
    observed = git.remote_head()
    if observed == candidate:
        return ObservedPush(candidate)
    if observed != expected:
        raise RebaseRefused("head_changed")
    raise RebaseRefused("git_failed" if code else "push_unconfirmed")


async def prepare_rebase(snapshot: RebaseSnapshot, credential: IssuedCredential) -> PreparedRebase:
    return await _run(snapshot, credential, _prepare_rebase)


async def push_rebase(
    snapshot: RebaseSnapshot, credential: IssuedCredential, prepared: PreparedRebase
) -> ObservedPush:
    return await _run(
        snapshot,
        credential,
        lambda git: _push(git, prepared, snapshot.old_head_sha, _prepare_rebase),
    )


async def prepare_ci_marker(
    snapshot: RebaseSnapshot, credential: IssuedCredential, rebased_head_sha: str
) -> PreparedRebase:
    return await _run(snapshot, credential, lambda git: _prepare_ci_marker(git, rebased_head_sha))


async def push_ci_marker(
    snapshot: RebaseSnapshot,
    credential: IssuedCredential,
    prepared: PreparedRebase,
    rebased_head_sha: str,
) -> ObservedPush:
    return await _run(
        snapshot,
        credential,
        lambda git: _push(
            git,
            prepared,
            rebased_head_sha,
            lambda checkout: _prepare_ci_marker(checkout, rebased_head_sha),
        ),
    )


async def describe_changed_paths(
    snapshot: RebaseSnapshot, credential: IssuedCredential, head_sha: str
) -> tuple[str, ...]:
    def describe(git: _Git) -> tuple[str, ...]:
        head = _sha(head_sha)
        target = _sha(snapshot.target_sha)
        git.initialize()
        git.fetch(target, head)
        git.require_ancestor(target, head)
        _, paths = git.command("diff", "--name-only", "--no-renames", "-z", target, head, "--")
        try:
            return tuple(path.decode("utf-8") for path in paths.split(b"\0") if path)
        except UnicodeError:
            raise RebaseRefused("invalid_paths") from None

    return await _run(snapshot, credential, describe)
