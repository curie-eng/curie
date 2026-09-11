"""Managed repository workspace preparation at the trusted worker boundary.

These tests deliberately model a clone implementation that records an
authenticated origin even though Curie asked git to use a clean URL plus an
ephemeral HTTP header.  That observed failure mode makes the explicit
``remote set-url`` defense non-vacuous: removing it puts the credential back in
the archive that the first sandbox turn receives.

The module under test is intentionally worker-local.  The API owns credential
minting and the sandbox owns only a one-object read capability; neither the
frozen ACI nor plugin-format contracts participate in this seam.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib
import io
import json
import os
import stat
import tarfile
import threading
import uuid
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

WORKER_AUTH = "worker-auth-value"
GIT_CREDENTIAL = "Basic redeemed-credential-value"
CLEAN_URL = "https://github.com/acme-corp/acme-bot.git"
AUTHENTICATED_URL = "https://redeemed-credential-value@github.com/acme-corp/acme-bot.git"
DEPLOYMENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")


@pytest.fixture
def workspace() -> Any:
    """Load the new worker-local seam per test so every red contract collects."""

    return importlib.import_module("curie_worker.workspace")


@dataclass
class _CommandResult:
    stdout: str = ""


class _FakeCommands:
    """A subprocess port whose clone writes the credential leak we must strip."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.events: list[str] = []
        self.available = True
        self.fail_stage: str | None = None

    def require(self, executable: str) -> None:
        self.calls.append({"require": executable})
        if not self.available:
            raise FileNotFoundError(executable)

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> _CommandResult:
        args = [str(part) for part in argv]
        self.calls.append(
            {
                "argv": args,
                "cwd": cwd,
                "env": dict(env or {}),
                "timeout_seconds": timeout_seconds,
            }
        )
        if "clone" in args:
            self.events.append("clone")
            if self.fail_stage == "clone":
                raise TimeoutError("clone exceeded its deadline")
            checkout = Path(args[-1])
            checkout.mkdir(parents=True, mode=0o700)
            os.chmod(checkout, 0o700)
            (checkout / ".git").mkdir()
            (checkout / ".git" / "config").write_text(
                '[remote "origin"]\n'
                f"\turl = {AUTHENTICATED_URL}\n"
                "\tfetch = +refs/heads/*:refs/remotes/origin/*\n"
            )
            (checkout / "README.md").write_text("workspace ready\n")
            return _CommandResult()

        if "remote" in args and "set-url" in args:
            self.events.append("set-url")
            assert cwd is not None
            config = cwd / ".git" / "config"
            config.write_text(config.read_text().replace(AUTHENTICATED_URL, CLEAN_URL))
            return _CommandResult()

        if "config" in args and "--get" in args and "remote.origin.url" in args:
            self.events.append("verify-origin")
            assert cwd is not None
            config = (cwd / ".git" / "config").read_text()
            value = CLEAN_URL if CLEAN_URL in config else AUTHENTICATED_URL
            return _CommandResult(stdout=f"{value}\n")

        if "rev-parse" in args:
            return _CommandResult(stdout=f"{'a' * 40}\n")

        raise AssertionError(f"unexpected command: {args}")


class _FakeCredentialClient:
    def __init__(self, workspace: Any) -> None:
        self._workspace = workspace
        self.requested: list[uuid.UUID] = []

    def redeem(self, deployment_id: uuid.UUID, conversation_id: str) -> Any:
        self.requested.append(deployment_id)
        assert conversation_id == "1700000000.000100"
        return self._workspace.WorkspaceCredential(
            repo_full_name="acme-corp/acme-bot",
            clone_url=CLEAN_URL,
            authorization_header=GIT_CREDENTIAL,
        )


class _StreamingObjectStore:
    """Private object-store port: streaming writes and exact-object signing."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_inputs: list[object] = []
        self.signed: list[tuple[str, int]] = []
        self.deleted: list[str] = []

    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        # Passing bytes here is a whole-object buffer masquerading as a stream.
        assert not isinstance(chunks, (bytes, bytearray, memoryview))
        self.put_inputs.append(chunks)
        self.objects[key] = b"".join(chunks)

    def get_stream(self, key: str) -> Iterator[bytes]:
        payload = self.objects[key]
        midpoint = max(1, len(payload) // 2)
        yield payload[:midpoint]
        yield payload[midpoint:]

    def presign_get(self, key: str, *, expires_seconds: int) -> str:
        assert key in self.objects
        self.signed.append((key, expires_seconds))
        return f"https://objects.example.com/workspaces/{key}?one-object=yes"

    def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.objects.pop(key, None)

    def list_keys(self, prefix: str) -> Iterator[str]:
        needle = f"{prefix.strip('/')}/"
        yield from sorted(key for key in self.objects if key.startswith(needle))


class _CommitThenRaiseObjectStore(_StreamingObjectStore):
    """Model an S3 PutObject that commits before its response is lost."""

    fail_next_ownership_put = True

    def put_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        super().put_stream(key, chunks)
        if key.startswith("_ownership/") and self.fail_next_ownership_put:
            self.fail_next_ownership_put = False
            raise TimeoutError("put response was lost after commit")


class _CommitThenRaiseUnreadableObjectStore(_CommitThenRaiseObjectStore):
    def get_stream(self, key: str) -> Iterator[bytes]:
        if key.startswith("_ownership/") and not self.fail_next_ownership_put:
            raise OSError("ownership read-back is unavailable")
        yield from super().get_stream(key)


def _limits(workspace: Any, **overrides: Any) -> Any:
    values = {
        "clone_timeout_seconds": 90,
        "archive_timeout_seconds": 30,
        "upload_timeout_seconds": 30,
        "total_timeout_seconds": 150,
        "max_checkout_bytes": 512 * 1024 * 1024,
        "max_archive_bytes": 256 * 1024 * 1024,
        "max_members": 4096,
        "max_compression_ratio": 20.0,
        "reference_ttl_seconds": 300,
        "max_concurrent_clones": 2,
    }
    values.update(overrides)
    return workspace.WorkspaceLimits(**values)


def _preparer(
    workspace: Any,
    tmp_path: Path,
    *,
    commands: _FakeCommands | None = None,
    objects: _StreamingObjectStore | None = None,
    limits: Any | None = None,
    clock: Any | None = None,
) -> tuple[Any, _FakeCommands, _StreamingObjectStore]:
    command_port = commands or _FakeCommands()
    object_port = objects or _StreamingObjectStore()
    preparer = workspace.WorkspacePreparer(
        credentials=_FakeCredentialClient(workspace),
        commands=command_port,
        objects=object_port,
        scratch_root=tmp_path / "clone-scratch",
        limits=limits or _limits(workspace),
        clock=clock,
    )
    return preparer, command_port, object_port


def _prepare(preparer: Any, *, generation: str = "claim-1") -> Any:
    return preparer.prepare(
        deployment_id=DEPLOYMENT_ID,
        thread_key="1700000000.000100",
        generation=generation,
    )


def _archive_members(payload: bytes) -> dict[str, bytes]:
    members: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as archive:
        for member in archive.getmembers():
            if member.isfile():
                extracted = archive.extractfile(member)
                assert extracted is not None
                members[member.name] = extracted.read()
    return members


def test_workspace_clone_strips_authenticated_remote_before_first_turn(
    tmp_path: Path,
) -> None:
    """Deleting the explicit set-url must put the fake clone's token in the archive."""

    # Keep this import inside the selected node.  The fix-pin gate reverses the
    # newly introduced worker module as part of the product diff; an import in
    # a shared fixture would turn that expected red result into an unattributed
    # setup error instead of a failure owned by this credential-isolation test.
    workspace = importlib.import_module("curie_worker.workspace")
    preparer, commands, objects = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    payload = objects.objects[prepared.object_key]
    members = _archive_members(payload)

    assert commands.events[:3] == ["clone", "set-url", "verify-origin"]
    assert members[".git/config"].decode().count(CLEAN_URL) == 1
    assert AUTHENTICATED_URL.encode() not in payload
    assert GIT_CREDENTIAL.encode() not in payload
    assert prepared.clean_clone_url == CLEAN_URL


def test_clone_credential_is_absent_from_argv_archive_config_and_claim_env(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, commands, objects = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    claim_env = prepared.claim_env()
    payload = objects.objects[prepared.object_key]

    argv_text = "\n".join(" ".join(call.get("argv", [])) for call in commands.calls)
    config = _archive_members(payload)[".git/config"]
    assert "redeemed-credential-value" not in argv_text
    assert b"redeemed-credential-value" not in payload
    assert b"redeemed-credential-value" not in config
    assert all("redeemed-credential-value" not in value for value in claim_env.values())
    assert set(claim_env) == {"CURIE_WORKSPACE_REF", "CURIE_WORKSPACE_SHA256"}


def test_clone_is_private_full_blob_shallow_and_refuses_redirects(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, commands, _ = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    clone_call = next(call for call in commands.calls if "clone" in call.get("argv", []))
    argv = clone_call["argv"]
    env = clone_call["env"]

    assert argv[0:2] == ["git", "clone"]
    assert "--depth=1" in argv
    assert "--single-branch" in argv
    assert "--no-tags" in argv
    assert "--filter" not in argv, "partial clones cannot hydrate without sandbox egress"
    assert CLEAN_URL in argv
    assert env["GIT_CONFIG_COUNT"]
    config_entries = "\n".join(f"{key}={value}" for key, value in env.items())
    assert "http.followRedirects=false" in config_entries
    assert GIT_CREDENTIAL in config_entries
    assert stat.S_IMODE(prepared.checkout_mode) == 0o700


def test_internal_workspace_redemption_uses_only_worker_auth_and_deployment_id(
    workspace: Any,
) -> None:
    calls: list[dict[str, Any]] = []

    def transport(**request: Any) -> Any:
        calls.append(request)
        return SimpleNamespace(
            status=200,
            headers={"Cache-Control": "no-store"},
            body=json.dumps(
                {
                    "repo_full_name": "acme-corp/acme-bot",
                    "clone_url": CLEAN_URL,
                    "authorization_header": GIT_CREDENTIAL,
                }
            ).encode(),
        )

    client = workspace.WorkspaceCredentialClient(
        api_url="https://api.example.com",
        worker_token=WORKER_AUTH,
        transport=transport,
    )
    redeemed = client.redeem(DEPLOYMENT_ID, "1700000000.000100")
    request = calls[0]

    assert request["method"] == "POST"
    assert request["url"].endswith(f"/v1/internal/workspaces/{DEPLOYMENT_ID}/credential")
    assert request["headers"] == {
        "X-Curie-Worker-Token": WORKER_AUTH,
        "Content-Type": "application/json",
    }
    assert json.loads(request["body"]) == {
        "conversation_id": "1700000000.000100",
    }
    assert request["allow_redirects"] is False
    assert "repo" not in request["url"]
    assert redeemed.repo_full_name == "acme-corp/acme-bot"
    assert redeemed.authorization_header == GIT_CREDENTIAL


def test_runtime_repo_parser_accepts_one_root_url_and_rejects_ambiguous(
    workspace: Any,
) -> None:
    assert workspace.parse_github_repo_fact(
        "Please update <https://github.com/acme-corp/acme-bot.git> and add a test."
    ) == "acme-corp/acme-bot"
    assert workspace.parse_github_repo_fact(
        "Keep working in this thread; the repository is already selected."
    ) is None

    with pytest.raises(workspace.WorkspaceSelectionRefused, match="only one"):
        workspace.parse_github_repo_fact(
            "Compare https://github.com/acme-corp/acme-bot with "
            "https://github.com/acme-corp/acme-api before changing anything."
        )


def test_runtime_repo_parser_deduplicates_repeated_repository_facts(
    workspace: Any,
) -> None:
    assert workspace.parse_github_repo_fact(
        "Update https://github.com/acme-corp/acme-bot and keep the notes at "
        "https://github.com/acme-corp/acme-bot.git current."
    ) == "acme-corp/acme-bot"


def test_runtime_repo_parser_strips_sentence_punctuation(workspace: Any) -> None:
    assert workspace.parse_github_repo_fact(
        "Please update https://github.com/acme-corp/acme-bot."
    ) == "acme-corp/acme-bot"


@pytest.mark.parametrize(
    "message",
    [
        "http://github.com/acme-corp/acme-bot",
        "https://token@github.com/acme-corp/acme-bot",
        "https://github.com:8443/acme-corp/acme-bot",
        "https://github.com.evil.example/acme-corp/acme-bot",
        "https://github.com/acme-corp/acme-bot/pull/1",
        "https://github.com/acme-corp/acme-bot?tab=readme",
        "https://github.com/acme-corp/acme-bot#readme",
    ],
)
def test_runtime_repo_parser_rejects_non_root_or_credentialed_urls(
    workspace: Any, message: str
) -> None:
    assert workspace.parse_github_repo_fact(message) is None


def test_internal_workspace_selection_sends_author_thread_and_optional_repo(
    workspace: Any,
) -> None:
    calls: list[dict[str, Any]] = []

    def transport(**request: Any) -> Any:
        calls.append(request)
        return SimpleNamespace(
            status=200,
            headers={},
            body=b'{"repo_full_name":"acme-corp/acme-bot"}',
        )

    client = workspace.WorkspaceCredentialClient(
        api_url="https://api.example.com",
        worker_token=WORKER_AUTH,
        transport=transport,
    )
    selected = client.select(
        DEPLOYMENT_ID,
        "1700000000.000100",
        "U0REQUEST1",
        "acme-corp/acme-bot",
    )

    assert selected == "acme-corp/acme-bot"
    assert calls[0]["url"].endswith(f"/{DEPLOYMENT_ID}/selection")
    assert json.loads(calls[0]["body"]) == {
        "conversation_id": "1700000000.000100",
        "author": "U0REQUEST1",
        "repo_full_name": "acme-corp/acme-bot",
    }


@pytest.mark.parametrize(
    ("code", "expected_detail"),
    [
        (
            "workspace.deployment_disabled",
            "This deployment does not enable repository workspaces.",
        ),
        (
            "workspace.repository_required",
            "Start the thread by naming one allowed root GitHub repository URL.",
        ),
        (
            "workspace.selection_conflict",
            "This thread is already bound to a different repository.",
        ),
    ],
)
def test_internal_workspace_selection_409_maps_machine_code_not_detail_prose(
    workspace: Any, code: str, expected_detail: str
) -> None:
    """The worker must not infer a control-plane decision from mutable prose."""

    def transport(**_request: Any) -> Any:
        return SimpleNamespace(
            status=409,
            headers={},
            body=json.dumps(
                {
                    "detail": {
                        "code": code,
                        "message": "wording intentionally shares no legacy match text",
                    }
                }
            ).encode(),
        )

    client = workspace.WorkspaceCredentialClient(
        api_url="https://api.example.com",
        worker_token=WORKER_AUTH,
        transport=transport,
    )

    with pytest.raises(workspace.WorkspaceSelectionRefused) as excinfo:
        client.select(DEPLOYMENT_ID, "1700000000.000100", "U0REQUEST1", None)

    assert excinfo.value.public_detail == expected_detail


def test_workspace_preparer_does_not_chmod_preexisting_mount_root(
    workspace: Any, tmp_path: Path
) -> None:
    scratch_root = tmp_path / "fs-group-owned-workspace"
    scratch_root.mkdir(mode=0o770)
    os.chmod(scratch_root, 0o770)
    preparer = workspace.WorkspacePreparer(
        credentials=_FakeCredentialClient(workspace),
        commands=_FakeCommands(),
        objects=_StreamingObjectStore(),
        scratch_root=scratch_root,
        limits=_limits(workspace),
    )

    _prepare(preparer)

    assert stat.S_IMODE(scratch_root.stat().st_mode) == 0o770


def test_workspace_upload_is_streamed_and_reference_is_one_object_and_short_lived(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, _, objects = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    decoded = workspace.WorkspaceRef.decode(prepared.claim_env()["CURIE_WORKSPACE_REF"])

    assert objects.put_inputs and not isinstance(objects.put_inputs[0], bytes)
    assert objects.signed == [(prepared.object_key, 300)]
    assert decoded.url == (
        f"https://objects.example.com/workspaces/{prepared.object_key}?one-object=yes"
    )
    assert decoded.sha256 == prepared.sha256
    assert decoded.expires_in_seconds <= 300
    assert "bucket" not in decoded.url
    assert "prefix" not in decoded.url
    assert "credential" not in decoded.url


def test_worker_rehashes_private_object_before_delivery(workspace: Any, tmp_path: Path) -> None:
    preparer, _, objects = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    objects.objects[prepared.object_key] += b"tampered"
    with pytest.raises(workspace.WorkspacePreparationError, match="digest"):
        preparer.verify(prepared)


def _tar_with_member(name: str, *, kind: str = "file", data: bytes = b"x") -> bytes:
    """Build one hostile archive, byte for byte the same on every call.

    `mode="w:gz"` writes a gzip header carrying the time it was written, and
    these bytes are used as pytest parameter values. Under xdist each worker
    collects at its own moment, so an unpinned header gives the same case a
    different node id per worker and pytest refuses to run at all: "Different
    tests were collected between gw0 and gw3". Pinning mtime makes the payload
    a constant, which is what a fixture of hostile bytes should have been.
    """
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            info = tarfile.TarInfo(name)
            if kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = "../../outside"
                archive.addfile(info)
            elif kind == "device":
                info.type = tarfile.CHRTYPE
                archive.addfile(info)
            else:
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
    return raw.getvalue()


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (_tar_with_member("../../outside"), "traversal"),
        (_tar_with_member("/absolute"), "absolute"),
        (_tar_with_member("repo/link", kind="symlink"), "link"),
        (_tar_with_member("repo/device", kind="device"), "special"),
    ],
    # Without these the node id is the raw gzip payload, which no `Fix pin:`
    # selector can be typed to name even once the bytes are deterministic.
    ids=["traversal", "absolute", "link", "special"],
)
def test_hostile_workspace_archive_is_rejected_before_init(
    workspace: Any, payload: bytes, reason: str
) -> None:
    with pytest.raises(workspace.WorkspaceArchiveError, match=reason):
        workspace.validate_workspace_archive(iter([payload]), limits=_limits(workspace))


def test_workspace_missing_git_fails_loudly_at_git_preflight(
    workspace: Any, tmp_path: Path
) -> None:
    commands = _FakeCommands()
    commands.available = False
    preparer, _, objects = _preparer(workspace, tmp_path, commands=commands)

    with pytest.raises(workspace.WorkspacePreparationError) as excinfo:
        _prepare(preparer)

    assert excinfo.value.stage == "git-preflight"
    assert "git" in str(excinfo.value).lower()
    assert objects.objects == {}


@pytest.mark.parametrize(
    ("stage", "expected_limit"),
    [("clone", 90), ("archive", 30), ("upload", 30)],
)
def test_workspace_stage_timeout_names_the_failed_stage_and_budget(
    workspace: Any, tmp_path: Path, stage: str, expected_limit: int
) -> None:
    commands = _FakeCommands()
    commands.fail_stage = stage if stage == "clone" else None
    objects = _StreamingObjectStore()
    if stage == "upload":
        objects.put_stream = lambda key, chunks: (_ for _ in ()).throw(TimeoutError("slow put"))
    preparer, _, _ = _preparer(workspace, tmp_path, commands=commands, objects=objects)
    if stage == "archive":
        preparer.archive_checkout = lambda *args, **kwargs: (_ for _ in ()).throw(
            TimeoutError("slow archive")
        )

    with pytest.raises(workspace.WorkspaceStageTimeout) as excinfo:
        _prepare(preparer)

    assert excinfo.value.stage == stage
    assert excinfo.value.limit_seconds == expected_limit
    assert stage in str(excinfo.value)


def test_workspace_aggregate_budget_is_one_deadline_not_three_fresh_budgets(
    workspace: Any, tmp_path: Path
) -> None:
    class AdvancingClock:
        def __init__(self) -> None:
            self.values = iter([0.0, 89.0, 119.0, 151.0])
            self.last = 151.0

        def __call__(self) -> float:
            self.last = next(self.values, self.last)
            return self.last

    preparer, _, objects = _preparer(
        workspace,
        tmp_path,
        clock=AdvancingClock(),
        limits=_limits(workspace, total_timeout_seconds=150),
    )

    with pytest.raises(workspace.WorkspaceStageTimeout) as excinfo:
        _prepare(preparer)

    assert excinfo.value.stage == "total"
    assert excinfo.value.limit_seconds == 150
    assert objects.objects == {}


@pytest.mark.parametrize(
    ("overrides", "extra_bytes", "stage"),
    [
        ({"max_checkout_bytes": 4}, b"five!", "checkout-size"),
        ({"max_archive_bytes": 32}, b"x" * 1000, "archive-size"),
    ],
)
def test_workspace_size_caps_fail_before_claim(
    workspace: Any,
    tmp_path: Path,
    overrides: dict[str, int],
    extra_bytes: bytes,
    stage: str,
) -> None:
    commands = _FakeCommands()
    original = commands.run

    def run(*args: Any, **kwargs: Any) -> _CommandResult:
        result = original(*args, **kwargs)
        argv = args[0]
        if "clone" in argv:
            (Path(argv[-1]) / "oversize.bin").write_bytes(extra_bytes)
        return result

    commands.run = run  # type: ignore[method-assign]
    preparer, _, objects = _preparer(
        workspace, tmp_path, commands=commands, limits=_limits(workspace, **overrides)
    )

    with pytest.raises(workspace.WorkspacePreparationError) as excinfo:
        _prepare(preparer)

    assert excinfo.value.stage == stage
    assert objects.objects == {}


def test_workspace_archive_member_and_compression_caps_are_enforced_before_init(
    workspace: Any,
) -> None:
    many = io.BytesIO()
    with tarfile.open(fileobj=many, mode="w:gz") as archive:
        for index in range(3):
            info = tarfile.TarInfo(f"file-{index}")
            info.size = 1
            archive.addfile(info, io.BytesIO(b"x"))
    bomb = _tar_with_member("repeat.bin", data=b"x" * 100_000)

    with pytest.raises(workspace.WorkspaceArchiveError, match="member"):
        workspace.validate_workspace_archive(
            iter([many.getvalue()]), limits=_limits(workspace, max_members=2)
        )
    with pytest.raises(workspace.WorkspaceArchiveError, match="compression"):
        workspace.validate_workspace_archive(
            iter([bomb]), limits=_limits(workspace, max_compression_ratio=2.0)
        )


class _RecordingSubstrate:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def claim(
        self, thread_key: str, *, env: dict[str, str] | None = None, **_: object
    ) -> object:
        self.calls.append(("claim", thread_key, dict(env or {})))
        return object()

    def resume(
        self, thread_key: str, *, env: dict[str, str] | None = None, **_: object
    ) -> object:
        self.calls.append(("resume", thread_key, dict(env or {})))
        return object()


def test_workspace_ownership_is_durable_before_the_sandbox_claim_is_exposed(
    workspace: Any, tmp_path: Path
) -> None:
    """A worker crash after claim starts must not orphan its delivered base."""

    preparer, _, objects = _preparer(workspace, tmp_path)
    thread_key = "1700000000.000100"

    class SimulatedWorkerCrash(BaseException):
        pass

    class CrashingSubstrate:
        observed: Any | None = None

        def claim(
            self, claimed_thread: str, *, env: dict[str, str] | None = None, **_: object
        ) -> object:
            restarted = workspace.WorkspaceClaimCoordinator(
                preparer=preparer,
                substrate=_RecordingSubstrate(),
            )
            self.observed = restarted.current(claimed_thread)
            assert self.observed is not None
            assert env is not None
            assert self.observed.claim_env() == {
                "CURIE_WORKSPACE_REF": env["CURIE_WORKSPACE_REF"],
                "CURIE_WORKSPACE_SHA256": env["CURIE_WORKSPACE_SHA256"],
            }
            raise SimulatedWorkerCrash

    substrate = CrashingSubstrate()
    coordinator = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=substrate,
    )

    with pytest.raises(SimulatedWorkerCrash):
        coordinator.claim_or_resume_with_handle(
            thread_key=thread_key,
            deployment_id=DEPLOYMENT_ID,
        )

    restarted = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
    )
    assert restarted.current(thread_key) == substrate.observed
    assert substrate.observed.object_key in objects.objects


@pytest.mark.parametrize("failure_mode", ["claim", "resume"])
def test_workspace_claim_or_resume_failure_restores_prior_durable_ownership(
    workspace: Any, tmp_path: Path, failure_mode: str
) -> None:
    preparer, _, objects = _preparer(workspace, tmp_path)
    thread_key = "1700000000.000100"

    class Suspended(Exception):
        pass

    class FailingSubstrate(_RecordingSubstrate):
        failure_mode: str | None = None

        def claim(
            self, claimed_thread: str, *, env: dict[str, str] | None = None, **_: object
        ) -> object:
            self.calls.append(("claim", claimed_thread, dict(env or {})))
            if self.failure_mode == "claim":
                raise RuntimeError("claim refused")
            if self.failure_mode == "resume":
                raise Suspended("route is suspended")
            return object()

        def resume(
            self, claimed_thread: str, *, env: dict[str, str] | None = None, **_: object
        ) -> object:
            self.calls.append(("resume", claimed_thread, dict(env or {})))
            raise RuntimeError("resume refused")

    substrate = FailingSubstrate()
    coordinator = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=substrate,
        suspended_error=Suspended,
        ownership_ttl_seconds=60,
        wall_clock=lambda: 1000.0,
    )
    previous = coordinator.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared
    substrate.failure_mode = failure_mode

    with pytest.raises(RuntimeError, match=rf"{failure_mode} refused"):
        coordinator.claim_or_resume_with_handle(
            thread_key=thread_key,
            deployment_id=DEPLOYMENT_ID,
        )

    restarted = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=60,
        wall_clock=lambda: 1001.0,
    )
    assert restarted.current(thread_key) == previous
    assert previous.object_key in objects.objects
    deleted_workspace_keys = [key for key in objects.deleted if not key.startswith("_ownership/")]
    assert len(deleted_workspace_keys) == 1
    assert previous.object_key not in objects.deleted


def test_fresh_claim_and_resume_each_prepare_a_new_workspace_and_reap_the_old_object(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, _, objects = _preparer(workspace, tmp_path)
    substrate = _RecordingSubstrate()
    class Suspended(Exception):
        pass

    original_claim = substrate.claim

    def claim(thread_key: str, *, env: dict[str, str] | None = None, **kwargs: object) -> object:
        if substrate.calls:
            raise Suspended(thread_key)
        return original_claim(thread_key, env=env, **kwargs)

    substrate.claim = claim  # type: ignore[method-assign]
    coordinator = workspace.WorkspaceClaimCoordinator(
        preparer=preparer, substrate=substrate, suspended_error=Suspended
    )

    first = coordinator.claim_or_resume_with_handle(
        thread_key="1700000000.000100",
        deployment_id=DEPLOYMENT_ID,
        env={"CURIE_BUNDLE_REF": "bundles/first"},
    ).prepared
    second = coordinator.claim_or_resume_with_handle(
        thread_key="1700000000.000100",
        deployment_id=DEPLOYMENT_ID,
        env={"CURIE_BUNDLE_REF": "bundles/first"},
    ).prepared

    first_env = substrate.calls[0][2]
    second_env = substrate.calls[1][2]
    assert [call[0] for call in substrate.calls] == ["claim", "resume"]
    assert first_env["CURIE_WORKSPACE_REF"] != second_env["CURIE_WORKSPACE_REF"]
    assert first.object_key != second.object_key
    assert first.object_key in objects.deleted
    assert second.object_key in objects.objects


def test_workspace_ownership_survives_worker_restart_and_release(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, _, objects = _preparer(workspace, tmp_path)
    thread_key = "1700000000.000100"
    first_process = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=60,
        wall_clock=lambda: 1000.0,
    )
    prepared = first_process.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared

    restarted = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=60,
        wall_clock=lambda: 1001.0,
    )

    assert restarted.current(thread_key) == prepared
    restarted.release(thread_key)
    assert prepared.object_key not in objects.objects
    assert restarted.current(thread_key) is None


def test_workspace_ownership_adopts_a_put_that_committed_before_response_loss(
    workspace: Any, tmp_path: Path
) -> None:
    objects = _CommitThenRaiseObjectStore()
    preparer, _, _ = _preparer(workspace, tmp_path, objects=objects)
    thread_key = "1700000000.000100"
    coordinator = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
    )

    prepared = coordinator.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared

    restarted = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
    )
    assert restarted.current(thread_key) == prepared
    assert prepared.object_key in objects.objects
    assert prepared.object_key not in objects.deleted


def test_uncertain_ownership_put_never_deletes_a_possibly_referenced_archive(
    workspace: Any, tmp_path: Path
) -> None:
    objects = _CommitThenRaiseUnreadableObjectStore()
    preparer, _, _ = _preparer(workspace, tmp_path, objects=objects)
    coordinator = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
    )

    with pytest.raises(workspace.WorkspacePreparationError, match="outcome is uncertain"):
        coordinator.claim_or_resume_with_handle(
            thread_key="1700000000.000100",
            deployment_id=DEPLOYMENT_ID,
        )

    archive_keys = [key for key in objects.objects if not key.startswith("_ownership/")]
    assert len(archive_keys) == 1
    assert archive_keys[0] not in objects.deleted


def test_expired_workspace_ownership_is_reaped_after_restart(
    workspace: Any, tmp_path: Path
) -> None:
    now = [1000.0]
    preparer, _, objects = _preparer(workspace, tmp_path)
    thread_key = "1700000000.000100"
    first_process = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )
    prepared = first_process.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared
    now[0] = 1011.0
    restarted = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )

    assert restarted.enumerate_expired() == [thread_key]
    candidate = restarted.begin_expired_reap(thread_key)
    assert candidate is not None
    assert candidate.deleted_object_keys == (prepared.object_key,)
    assert restarted.finish_expired_reap(candidate)
    assert prepared.object_key not in objects.objects
    assert restarted.current(thread_key) is None


def test_reaper_rereads_after_lock_and_preserves_a_newly_staged_ledger(
    workspace: Any, tmp_path: Path
) -> None:
    now = [1000.0]
    preparer, _, objects = _preparer(workspace, tmp_path)
    thread_key = "1700000000.000100"
    first = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )
    expired = first.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared
    now[0] = 1011.0
    reaper = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )

    # The reaper enumerates and begins slow object cleanup while holding its
    # lease. Model that lease being lost before the final ledger delete: another
    # replica acquires the lock and stages a fresh base first.
    assert reaper.enumerate_expired() == [thread_key]
    candidate = reaper.begin_expired_reap(thread_key)
    assert candidate is not None
    claimant = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )
    active = claimant.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared

    assert not reaper.finish_expired_reap(candidate)
    assert reaper.current(thread_key) == active
    assert active.object_key in objects.objects
    assert active.object_key not in objects.deleted
    assert expired.object_key in objects.deleted


def test_workspace_ownership_touch_extends_affinity_lease_across_restart(
    workspace: Any, tmp_path: Path
) -> None:
    now = [1000.0]
    preparer, _, objects = _preparer(workspace, tmp_path)
    thread_key = "1700000000.000100"
    first_process = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )
    prepared = first_process.claim_or_resume_with_handle(
        thread_key=thread_key,
        deployment_id=DEPLOYMENT_ID,
    ).prepared
    now[0] = 1008.0
    assert first_process.touch(thread_key, ttl_seconds=10)
    now[0] = 1011.0

    restarted = workspace.WorkspaceClaimCoordinator(
        preparer=preparer,
        substrate=_RecordingSubstrate(),
        ownership_ttl_seconds=10,
        wall_clock=lambda: now[0],
    )
    assert restarted.enumerate_expired() == []
    assert restarted.current(thread_key) == prepared
    assert prepared.object_key in objects.objects


def test_runner_claim_never_starts_until_workspace_preparation_and_verification_succeed(
    workspace: Any, tmp_path: Path
) -> None:
    commands = _FakeCommands()
    commands.fail_stage = "clone"
    preparer, _, _ = _preparer(workspace, tmp_path, commands=commands)
    substrate = _RecordingSubstrate()
    coordinator = workspace.WorkspaceClaimCoordinator(preparer=preparer, substrate=substrate)

    with pytest.raises(workspace.WorkspaceStageTimeout):
        coordinator.claim_or_resume_with_handle(
            thread_key="1700000000.000100",
            deployment_id=DEPLOYMENT_ID,
            env={"CURIE_BUNDLE_REF": "bundles/first"},
        )

    assert substrate.calls == []


def test_workspace_claim_env_never_carries_worker_auth_object_store_or_git_credentials(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, _, _ = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    env = {
        **prepared.claim_env(),
        "CURIE_BUNDLE_REF": "bundles/first",
    }
    forbidden = {
        "CURIE_INTERNAL_WORKER_TOKEN",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "GITHUB_TOKEN",
    }

    assert forbidden.isdisjoint(env)
    assert WORKER_AUTH not in env.values()
    assert GIT_CREDENTIAL not in env.values()


def test_two_clone_slots_bound_concurrency_and_a_third_waits(workspace: Any) -> None:
    limiter = workspace.WorkspaceCloneLimiter(max_concurrent=2)
    first = limiter.acquire("first")
    second = limiter.acquire("second")
    waiter_started = threading.Event()
    third_acquired = threading.Event()
    third_released = threading.Event()

    def take_third_slot() -> None:
        waiter_started.set()
        third = limiter.acquire("third", timeout_seconds=1)
        third_acquired.set()
        third_released.wait(1)
        third.release()

    waiter = threading.Thread(target=take_third_slot)
    waiter.start()

    assert limiter.active == 2
    assert waiter_started.wait(1)
    assert not third_acquired.wait(0.05), "a third clone exceeded the configured bound"
    first.release()

    assert third_acquired.wait(1), "the waiter did not receive the released clone slot"
    assert limiter.active == 2
    second.release()
    third_released.set()
    waiter.join(timeout=1)
    assert not waiter.is_alive()


def test_object_reference_digest_is_the_streamed_archive_digest(
    workspace: Any, tmp_path: Path
) -> None:
    preparer, _, objects = _preparer(workspace, tmp_path)
    prepared = _prepare(preparer)
    payload = objects.objects[prepared.object_key]
    decoded = workspace.WorkspaceRef.decode(prepared.claim_env()["CURIE_WORKSPACE_REF"])

    assert prepared.sha256 == hashlib.sha256(payload).hexdigest()
    assert decoded.sha256 == prepared.sha256
    assert prepared.claim_env()["CURIE_WORKSPACE_SHA256"] == prepared.sha256
