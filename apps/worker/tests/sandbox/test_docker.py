"""DockerSandboxClient: the local (no-Kubernetes) SandboxClient.

The ``docker`` CLI is the one external dependency, so it is captured/stubbed; the
argv construction, the RustFS bundle fetch + B2-aligned unwrap, and the inspect
parsing (port + operating mode) are exercised for real.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import tarfile
import time
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from curie_worker.attachments import (
    AttachmentRef,
    AttachmentResolutionError,
    encode_attachment_refs,
)
from curie_worker.bundle_store import extract_bundle
from curie_worker.sandbox import (
    AffinityStore,
    ClaimView,
    QuotaRejection,
    SandboxSubstrate,
    SandboxView,
    SubstrateConfig,
)
from curie_worker.sandbox.docker import (
    RUNNER_CONTAINER_PORT,
    DockerError,
    DockerSandboxClient,
    RunnerHardening,
)
from curie_worker.sandbox.types import RouteState, SandboxError, ToolDaemonMount
from curie_worker.workspace import WorkspacePreparationError, WorkspaceRef

from .conftest import (
    _FakeBundleStore,
    _flag_values,
    _RecordingDocker,
    _seed_source,
    _volume_mounts,
)


def _plugin_tar_gz(wrapper: str | None) -> bytes:
    """A tar.gz carrying a Claude Code plugin. When ``wrapper`` is set, the
    manifest sits one level down (the common ``tar czf b.tgz myplugin/`` shape)."""
    buf = io.BytesIO()
    prefix = f"{wrapper}/" if wrapper else ""
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        manifest = b'{"name": "deal-desk", "version": "0.1.0"}'
        info = tarfile.TarInfo(f"{prefix}.claude-plugin/plugin.json")
        info.size = len(manifest)
        tf.addfile(info, io.BytesIO(manifest))
    return buf.getvalue()


@pytest.fixture
def remote_claim_env(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as output:
        content = b"workspace seed\n"
        member = tarfile.TarInfo("README.md")
        member.size = len(content)
        output.addfile(member, io.BytesIO(content))
    workspace = archive.getvalue()
    attachment = b"attachment seed\n"
    objects = {
        "https://objects.example.com/workspace": workspace,
        "https://objects.example.com/attachment": attachment,
    }

    def open_object(request: Any, **kwargs: Any) -> io.BytesIO:
        return io.BytesIO(objects[str(request.full_url)])

    def open_attachment(opener: Any, request: Any, **kwargs: Any) -> io.BytesIO:
        return open_object(request, **kwargs)

    monkeypatch.setattr(urllib.request, "urlopen", open_object)
    monkeypatch.setattr(urllib.request.OpenerDirector, "open", open_attachment)
    expires = int(time.time()) + 300
    digest = hashlib.sha256(workspace).hexdigest()
    return {
        "CURIE_FAKE_MODEL": "1",
        "CURIE_BUNDLE_REF": "bundles/b.tar.gz",
        "CURIE_WORKSPACE_REF": WorkspaceRef(
            url="https://objects.example.com/workspace",
            sha256=digest,
            expires_at_epoch=expires,
        ).encode(),
        "CURIE_WORKSPACE_SHA256": digest,
        "CURIE_ATTACHMENTS_REF": encode_attachment_refs(
            [
                AttachmentRef(
                    name="report.txt",
                    url="https://objects.example.com/attachment",
                    sha256=hashlib.sha256(attachment).hexdigest(),
                    size_bytes=len(attachment),
                    expires_at_epoch=expires,
                )
            ]
        ),
    }


def test_create_claim_seeds_remote_volumes_before_starting_runner(
    remote_claim_env: dict[str, str],
) -> None:
    client = _RecordingDocker(
        image="curie-runner",
        network="seed-stack",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper="deal-desk")),
    )
    client.create_claim(
        "claim-remote", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
    )

    runner = client.runner_argv
    started = next(
        argv for argv in client.calls if argv == ["start", client.container_ids["claim-remote"]]
    )
    runner_labels = dict(label.split("=", 1) for label in _flag_values(runner, "--label"))
    attempt = runner_labels["curietech.ai/claim-attempt"]
    assert uuid.UUID(attempt).hex == attempt
    assert runner_labels["curietech.ai/claim"] == "claim-remote"
    assert runner_labels["curietech.ai/component"] == "claim-runner"
    assert runner_labels["curietech.ai/managed-by"] == "curie-sandbox-substrate"
    assert runner_labels["curietech.ai/stack"] == "seed-stack"
    mounts = _volume_mounts(runner)
    assert set(mounts) == {"/bundles/current", "/workspace", "/attachments"}
    assert mounts["/bundles/current"][1] == "ro"
    assert mounts["/workspace"][1] == "rw"
    assert mounts["/attachments"][1] == "ro"
    assert len({source for source, mode in mounts.values()}) == 3
    expected = {
        "/bundles/current": (
            ".claude-plugin/plugin.json",
            b'{"name": "deal-desk", "version": "0.1.0"}',
        ),
        "/workspace": ("README.md", b"workspace seed\n"),
        "/attachments": ("report.txt", b"attachment seed\n"),
    }
    for target, (volume, mode) in mounts.items():
        created = next(
            argv for argv in client.calls if argv[:2] == ["volume", "create"] and argv[-1] == volume
        )
        expected_labels = {
            "curietech.ai/managed-by": "curie-sandbox-substrate",
            "curietech.ai/claim": "claim-remote",
            "curietech.ai/component": {
                "/bundles/current": "claim-bundle",
                "/workspace": "claim-workspace",
                "/attachments": "claim-attachments",
            }[target],
            "curietech.ai/claim-attempt": attempt,
            "curietech.ai/stack": "seed-stack",
        }
        assert (
            dict(label.split("=", 1) for label in _flag_values(created, "--label"))
            == expected_labels
        )
        helper = next(
            argv
            for argv in client.calls
            if argv[0] == "create"
            and "curietech.ai/component=claim-runner" not in _flag_values(argv, "--label")
            and volume in {source for source, mode in _volume_mounts(argv).values()}
        )
        helper_name = client.container_ids[_flag_values(helper, "--name")[0]]
        assert (
            dict(label.split("=", 1) for label in _flag_values(helper, "--label"))
            == expected_labels
        )
        copied = next(
            argv
            for argv in client.calls
            if argv[0] == "cp" and argv[-1].startswith(f"{helper_name}:")
        )
        filename, content = expected[target]
        assert client.copied_files[copied[-1]][filename] == content
        assert client.calls.index(created) < client.calls.index(helper) < client.calls.index(copied)
        assert client.calls.index(runner) < client.calls.index(copied)
        assert client.calls.index(copied) < client.calls.index(started)
        assert client.volume_files[volume][filename] == content
    assert set(client.containers) == {"claim-remote"}
    assert client.container_status["claim-remote"] == "running"
    assert client.auto_created_volumes == []


def test_slow_input_seed_survives_orphan_sweep(
    remote_claim_env: dict[str, str],
) -> None:
    cutoff = datetime.now(UTC) - timedelta(minutes=1)
    old = (cutoff - timedelta(hours=1)).isoformat()

    class _SlowSeedDocker(_RecordingDocker):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.reaped: list[list[str]] = []
            self.copy_states: list[dict[str, str]] = []
            self.copy_helpers: list[str] = []
            self.staged_sources: list[Path] = []

        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            if args[0] == "cp":
                self.copy_helpers.append(self._container_name(args[-1].partition(":")[0]))
                self.staged_sources.append(Path(args[-2]))
                self.copy_states.append(dict(self.container_status))
                for volume in self.volumes:
                    self.volume_created_at[volume] = old
                observed = self.list_claims(
                    label_selector="curietech.ai/managed-by=curie-sandbox-substrate"
                )
                self.reaped.append(
                    self.reap_claim_templates(
                        keep={claim.name for claim in observed}, created_before=cutoff
                    )
                )
            return super()._docker(
                args,
                request_timeout_seconds=request_timeout_seconds,
                check=check,
            )

    client = _SlowSeedDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    error: DockerError | None = None
    try:
        try:
            client.create_claim(
                "claim-slow", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
            )
        except DockerError as exc:
            error = exc

        assert client.reaped == [[], [], []], "a live seed attempt was swept as an old orphan"
        assert error is None, f"the slow seed failed after reaping: {error}"
        for helper, states in zip(client.copy_helpers, client.copy_states, strict=True):
            assert states["claim-slow"] == "created"
            assert states[helper] == "running"
        assert set(client.containers) == {"claim-slow"}
        assert client.container_status["claim-slow"] == "running"
        assert client.started_containers.count("claim-slow") == 1
        assert client.auto_created_volumes == []
        mounts = client.container_mounts["claim-slow"]
        assert set(mounts) == {"/bundles/current", "/workspace", "/attachments"}
        expected = {
            "/bundles/current": {
                ".claude-plugin/plugin.json": b'{"name": "deal-desk", "version": "0.1.0"}'
            },
            "/workspace": {"README.md": b"workspace seed\n"},
            "/attachments": {"report.txt": b"attachment seed\n"},
        }
        for target, (volume, _mode) in mounts.items():
            assert client.volume_files[volume] == expected[target]
            assert (
                client.volumes[volume]["curietech.ai/claim-attempt"]
                == client.containers["claim-slow"]["curietech.ai/claim-attempt"]
            )
        assert all(not source.exists() for source in client.staged_sources)
    finally:
        client.delete_claim("claim-slow", request_timeout_seconds=1.0)


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "curietech.ai/managed-by",
        "curietech.ai/claim",
        "curietech.ai/claim-attempt",
        "curietech.ai/component",
        "curietech.ai/stack",
    ],
)
def test_damaged_input_volume_cannot_start_runner(
    remote_claim_env: dict[str, str],
    damage: str,
) -> None:
    class _DamagedInputDocker(_RecordingDocker):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.copies = 0
            self.damaged_volume: str | None = None

        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            result = super()._docker(
                args,
                request_timeout_seconds=request_timeout_seconds,
                check=check,
            )
            if args[0] == "cp":
                self.copies += 1
                if self.copies == 3:
                    helper, _, target = args[-1].partition(":")
                    helper = self._container_name(helper)
                    volume, mode = self.container_mounts[helper][target]
                    self.damaged_volume = volume
                    if damage == "missing":
                        self.volumes.pop(volume)
                        self.volume_created_at.pop(volume)
                        self.volume_files.pop(volume)
                    else:
                        self.volumes[volume] = {**self.volumes[volume], damage: "another-owner"}
            return result

    client = _DamagedInputDocker(
        image="curie-runner",
        network="seed-stack",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    try:
        with pytest.raises(DockerError):
            client.create_claim(
                "claim-damaged", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
            )

        assert client.damaged_volume is not None, "the damaged-volume path was not reached"
        assert "claim-damaged" not in client.started_containers
        assert "claim-damaged" not in client.containers
        assert client.auto_created_volumes == [], "a missing input was recreated without ownership"
    finally:
        client.delete_claim("claim-damaged", request_timeout_seconds=1.0)


def test_expired_seed_helper_cannot_start_runner(
    remote_claim_env: dict[str, str],
) -> None:
    class _ExpiredSeedDocker(_RecordingDocker):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.copies = 0
            self.expired_helper: str | None = None

        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            result = super()._docker(
                args, request_timeout_seconds=request_timeout_seconds, check=check
            )
            if args[0] == "cp":
                self.copies += 1
                if self.copies == 3:
                    helper = self._container_name(args[-1].partition(":")[0])
                    assert self.container_status[helper] == "running"
                    self.container_status[helper] = "exited"
                    self.expired_helper = helper
            return result

    client = _ExpiredSeedDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    with pytest.raises(DockerError, match="helper.*active"):
        client.create_claim(
            "claim-expired", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
        )

    assert client.copies == 3
    assert client.expired_helper is not None, "the helper expiry was not exercised"
    assert "claim-expired" not in client.started_containers
    assert client.containers == {}
    assert client.volumes == {}
    assert client.auto_created_volumes == []


@pytest.mark.parametrize("invalid_input", ["workspace", "attachments"])
def test_all_claim_inputs_are_validated_before_any_daemon_call(
    remote_claim_env: dict[str, str],
    invalid_input: str,
) -> None:
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    env = dict(remote_claim_env)
    error: type[Exception]
    if invalid_input == "workspace":
        env["CURIE_WORKSPACE_SHA256"] = "0" * 64
        error = WorkspacePreparationError
    else:
        env["CURIE_ATTACHMENTS_REF"] = "malformed"
        error = AttachmentResolutionError

    with pytest.raises(error):
        client.create_claim("claim-invalid", pool="pool", env=env, deadline=time.monotonic() + 300)

    assert client.calls == []
    assert client.volumes == {}
    assert client.containers == {}


@pytest.mark.parametrize("winner_exists", [False, True])
@pytest.mark.parametrize(
    "failure", ["runner-create", "helper-create", "cp", "workspace-ownership", "runner-start"]
)
def test_failed_claim_removes_its_remote_volumes(
    remote_claim_env: dict[str, str],
    failure: str,
    winner_exists: bool,
) -> None:
    class _FailedSeedDocker(_RecordingDocker):
        failure_enabled = False
        failure_triggered = False

        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            result = super()._docker(
                args,
                request_timeout_seconds=request_timeout_seconds,
                check=check,
            )
            component = dict(label.split("=", 1) for label in _flag_values(args, "--label")).get(
                "curietech.ai/component"
            )
            failed = (
                (failure == "runner-create" and args[0] == "create" and component == "claim-runner")
                or (
                    failure == "helper-create"
                    and args[0] == "create"
                    and component != "claim-runner"
                )
                or (failure == "cp" and args[0] == "cp")
                or (failure == "workspace-ownership" and args[0] == "exec")
                or (
                    failure == "runner-start"
                    and args[0] == "start"
                    and self._container_name(args[-1]) == "claim-remote"
                )
            )
            if self.failure_enabled and failed:
                self.failure_triggered = True
                raise DockerError(f"docker {failure} failed")
            return result

    client = _FailedSeedDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    client.volumes["unrelated-volume"] = {
        "curietech.ai/managed-by": "another-component",
    }
    if winner_exists:
        client.create_claim(
            "claim-remote", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
        )
    preserved_volumes = dict(client.volumes)
    preserved_containers = dict(client.containers)
    first_failure_call = len(client.calls)
    first_failure_start = len(client.started_containers)
    client.failure_enabled = True
    with pytest.raises(DockerError):
        client.create_claim(
            "claim-remote", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
        )

    if not winner_exists:
        assert client.failure_triggered, "the requested daemon failure was not exercised"
    failure_calls = client.calls[first_failure_call:]
    created = [argv[-1] for argv in failure_calls if argv[:2] == ["volume", "create"]]
    assert created, "failure cleanup must exercise allocated daemon volumes"
    assert client.volumes == preserved_volumes
    assert client.containers == preserved_containers
    assert not any(argv[0] == "run" for argv in failure_calls)
    if failure != "runner-start":
        assert "claim-remote" not in client.started_containers[first_failure_start:]


def test_fresh_client_deletes_only_volumes_owned_by_the_exact_claim(
    remote_claim_env: dict[str, str],
) -> None:
    owner = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    owner.create_claim(
        "claim-remote", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
    )
    owned = dict(owner.volumes)
    assert len(owned) == 3
    labels = next(iter(owned.values()))
    assert labels["curietech.ai/managed-by"] == "curie-sandbox-substrate"
    claim_key = next(key for key, value in labels.items() if value == "claim-remote")
    component_key = next(key for key in labels if key.rsplit("/", 1)[-1] == "component")
    foreign = {
        "other-claim-volume": {**labels, claim_key: "claim-other"},
        "unmanaged-volume": {**labels, "curietech.ai/managed-by": "another-component"},
        "other-component-volume": {**labels, component_key: "another-component"},
    }
    owner.volumes.update(foreign)
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.volumes = owner.volumes
    client.volume_created_at = owner.volume_created_at
    client.volume_files = owner.volume_files
    client.volume_modes = owner.volume_modes
    client.containers = owner.containers
    client.container_ids = owner.container_ids
    client.container_status = owner.container_status
    client.container_created_at = owner.container_created_at
    client.container_started_at = owner.container_started_at
    client.container_mounts = owner.container_mounts
    client.delete_claim("claim-remote", request_timeout_seconds=1.0)

    assert client.volumes == foreign
    removal_calls = [argv for argv in client.calls if argv[:2] == ["volume", "rm"]]
    assert removal_calls
    assert all(
        client.calls.index(["rm", "-f", owner.container_ids["claim-remote"]])
        < client.calls.index(argv)
        for argv in removal_calls
    )
    assert all(0 < timeout <= 1.0 for timeout in client.timeouts)


@pytest.mark.parametrize("winner_timing", ["already-running", "during-create"])
def test_lost_claim_creation_preserves_the_winning_attempt(
    remote_claim_env: dict[str, str],
    winner_timing: str,
) -> None:
    winner = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )

    class _RacingDocker(_RecordingDocker):
        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            if (
                args[0] == "create"
                and "curietech.ai/component=claim-runner" in _flag_values(args, "--label")
                and winner_timing == "during-create"
            ):
                winner.create_claim(
                    "claim-race", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
                )
            return super()._docker(
                args,
                request_timeout_seconds=request_timeout_seconds,
                check=check,
            )

    loser = _RacingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None)),
    )
    winner.volumes = loser.volumes
    winner.volume_created_at = loser.volume_created_at
    winner.volume_files = loser.volume_files
    winner.volume_modes = loser.volume_modes
    winner.containers = loser.containers
    winner.container_ids = loser.container_ids
    winner.container_status = loser.container_status
    winner.container_created_at = loser.container_created_at
    winner.container_started_at = loser.container_started_at
    winner.container_mounts = loser.container_mounts
    if winner_timing == "already-running":
        winner.create_claim(
            "claim-race", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
        )

    with pytest.raises(DockerError, match="already in use"):
        loser.create_claim(
            "claim-race", pool="pool", env=remote_claim_env, deadline=time.monotonic() + 300
        )

    winner_attempt = winner.containers["claim-race"]["curietech.ai/claim-attempt"]
    loser_attempt = dict(
        label.split("=", 1) for label in _flag_values(loser.runner_argv, "--label")
    )["curietech.ai/claim-attempt"]
    assert winner_attempt != loser_attempt
    assert set(loser.containers) == {"claim-race"}
    assert loser.container_status["claim-race"] == "running"
    assert len(loser.volumes) == 3
    assert all(
        labels["curietech.ai/claim-attempt"] == winner_attempt for labels in loser.volumes.values()
    )
    winner_volumes = {source for source, mode in _volume_mounts(winner.runner_argv).values()}
    assert set(loser.volumes) == winner_volumes
    loser_volumes = {source for source, mode in _volume_mounts(loser.runner_argv).values()}
    assert winner_volumes.isdisjoint(loser_volumes)
    assert not loser_volumes.intersection(loser.volumes)


def test_orphan_input_sweep_preserves_kept_recent_and_other_attempt_resources() -> None:
    client = _RecordingDocker(
        image="curie-runner", network="sweep-stack", bundle_store=_FakeBundleStore()
    )
    cutoff = datetime(2026, 10, 1, 12, tzinfo=UTC)
    old = (cutoff - timedelta(seconds=1)).isoformat()
    recent = (cutoff + timedelta(seconds=1)).isoformat()
    old_attempt = uuid.uuid4().hex

    def add_input(
        name: str,
        claim: str,
        created: str,
        *,
        attempt: str | None = None,
        component: str = "claim-bundle",
        managed_by: str = "curie-sandbox-substrate",
    ) -> None:
        labels = {
            "curietech.ai/managed-by": managed_by,
            "curietech.ai/claim": claim,
            "curietech.ai/component": component,
            "curietech.ai/stack": "sweep-stack",
        }
        if attempt is not None:
            labels["curietech.ai/claim-attempt"] = attempt
        label_args = [arg for key, value in labels.items() for arg in ("--label", f"{key}={value}")]
        client._docker(["volume", "create", *label_args, name], request_timeout_seconds=1.0)
        client.volume_created_at[name] = created
        client._docker(
            ["create", "--name", f"{name}-seed", *label_args, "curie-runner"],
            request_timeout_seconds=1.0,
        )

    add_input("orphan-bundle", "claim-orphan", old, attempt=old_attempt)
    add_input(
        "orphan-workspace",
        "claim-orphan",
        old,
        attempt=old_attempt,
        component="claim-workspace",
    )
    add_input("same-claim-recent", "claim-orphan", recent, attempt=uuid.uuid4().hex)
    add_input("kept", "claim-kept", old, attempt=uuid.uuid4().hex)
    add_input("recent", "claim-recent", recent, attempt=uuid.uuid4().hex)
    add_input("at-cutoff", "claim-cutoff", cutoff.isoformat(), attempt=uuid.uuid4().hex)
    add_input("undated", "claim-undated", "", attempt=uuid.uuid4().hex)
    add_input("malformed-date", "claim-malformed", "invalid", attempt=uuid.uuid4().hex)
    add_input("no-attempt", "claim-no-attempt", old)
    add_input(
        "unmanaged",
        "claim-unmanaged",
        old,
        attempt=uuid.uuid4().hex,
        managed_by="another-component",
    )
    add_input(
        "other-component",
        "claim-other-component",
        old,
        attempt=uuid.uuid4().hex,
        component="another-component",
    )
    client._docker(
        [
            "run",
            "--name",
            "claim-orphan",
            "--label",
            "curietech.ai/managed-by=curie-sandbox-substrate",
            "--label",
            "curietech.ai/claim=claim-orphan",
            "--label",
            "curietech.ai/component=claim-runner",
            "--label",
            "curietech.ai/stack=sweep-stack",
            "--label",
            f"curietech.ai/claim-attempt={uuid.uuid4().hex}",
            "curie-runner",
        ],
        request_timeout_seconds=1.0,
    )
    removed_names = {"orphan-bundle", "orphan-workspace"}
    preserved_volumes = {
        name: labels for name, labels in client.volumes.items() if name not in removed_names
    }
    removed_helpers = {f"{name}-seed" for name in removed_names}
    preserved_containers = {
        name: labels for name, labels in client.containers.items() if name not in removed_helpers
    }

    removed = client.reap_claim_templates(keep={"claim-kept"}, created_before=cutoff)

    assert removed == ["claim-orphan"]
    assert client.volumes == preserved_volumes
    assert client.containers == preserved_containers
    assert client.reap_claim_templates(keep={"claim-kept"}, created_before=cutoff) == []


def test_orphan_input_sweep_reaps_expired_seed_helper() -> None:
    client = _RecordingDocker(
        image="curie-runner", network="sweep-stack", bundle_store=_FakeBundleStore()
    )
    cutoff = datetime.now(UTC) - timedelta(minutes=1)
    old = (cutoff - timedelta(hours=1)).isoformat()
    labels = {
        "curietech.ai/managed-by": "curie-sandbox-substrate",
        "curietech.ai/claim": "claim-expired",
        "curietech.ai/component": "claim-bundle",
        "curietech.ai/claim-attempt": uuid.uuid4().hex,
        "curietech.ai/stack": "sweep-stack",
    }
    label_args = [arg for key, value in labels.items() for arg in ("--label", f"{key}={value}")]
    client._docker(["volume", "create", *label_args, "expired-input"], request_timeout_seconds=1.0)
    client.volume_created_at["expired-input"] = old
    client.volume_files["expired-input"] = {"marker.txt": b"seed input"}
    helper_id = client._docker(
        [
            "create",
            "--name",
            "expiring-seed",
            *label_args,
            "--mount",
            "type=volume,source=expired-input,target=/curie-input",
            "curie-runner",
        ],
        request_timeout_seconds=1.0,
    )
    client._docker(["start", helper_id], request_timeout_seconds=1.0)

    assert client.reap_claim_templates(keep=set(), created_before=cutoff) == []
    assert client.volume_files["expired-input"] == {"marker.txt": b"seed input"}
    assert client.container_status["expiring-seed"] == "running"

    client.container_status["expiring-seed"] = "exited"

    assert client.reap_claim_templates(keep=set(), created_before=cutoff) == ["claim-expired"]
    assert client.containers == {}
    assert client.volumes == {}
    assert client.volume_files == {}


def test_orphan_input_sweep_continues_after_volume_in_use_failure() -> None:
    class _InUseDocker(_RecordingDocker):
        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            if args[:2] == ["volume", "rm"] and args[-1] == "busy-input":
                self.calls.append(args)
                self.timeouts.append(request_timeout_seconds)
                raise DockerError("volume is in use by a concurrent container")
            return super()._docker(
                args, request_timeout_seconds=request_timeout_seconds, check=check
            )

    client = _InUseDocker(
        image="curie-runner", network="sweep-stack", bundle_store=_FakeBundleStore()
    )
    cutoff = datetime.now(UTC) - timedelta(minutes=1)
    inputs = (("busy-input", "claim-busy"), ("collectible-input", "claim-collectible"))
    for volume, claim in inputs:
        labels = {
            "curietech.ai/managed-by": "curie-sandbox-substrate",
            "curietech.ai/claim": claim,
            "curietech.ai/component": "claim-bundle",
            "curietech.ai/claim-attempt": uuid.uuid4().hex,
            "curietech.ai/stack": "sweep-stack",
        }
        label_args = [arg for key, value in labels.items() for arg in ("--label", f"{key}={value}")]
        client._docker(["volume", "create", *label_args, volume], request_timeout_seconds=1.0)
        client.volume_created_at[volume] = (cutoff - timedelta(hours=1)).isoformat()
        client.volume_files[volume] = {"marker.txt": volume.encode()}
    busy_labels = dict(client.volumes["busy-input"])

    removed = client.reap_claim_templates(keep=set(), created_before=cutoff)

    assert removed == ["claim-collectible"]
    assert client.volumes == {"busy-input": busy_labels}
    assert client.volume_files == {"busy-input": {"marker.txt": b"busy-input"}}
    assert ["volume", "rm", "busy-input"] in client.calls


@pytest.mark.parametrize("operation", ["list", "reap", "delete"])
def test_claim_operations_preserve_neighbor_stack_resources(operation: str) -> None:
    client = _RecordingDocker(
        image="curie-runner", network="stack-current", bundle_store=_FakeBundleStore()
    )
    cutoff = datetime.now(UTC) - timedelta(minutes=1)
    attempt = uuid.uuid4().hex
    for stack in ("stack-current", "stack-neighbor"):
        base_labels = {
            "curietech.ai/managed-by": "curie-sandbox-substrate",
            "curietech.ai/claim": "claim-shared",
            "curietech.ai/claim-attempt": attempt,
            "curietech.ai/stack": stack,
        }
        for component in ("claim-bundle", "claim-workspace", "claim-attachments"):
            volume = f"{stack}-{component}"
            labels = {**base_labels, "curietech.ai/component": component}
            label_args = [
                arg for key, value in labels.items() for arg in ("--label", f"{key}={value}")
            ]
            client._docker(["volume", "create", *label_args, volume], request_timeout_seconds=1.0)
            client.volume_created_at[volume] = (cutoff - timedelta(hours=1)).isoformat()
            client.volume_files[volume] = {"marker.txt": volume.encode()}
            client._docker(
                [
                    "create",
                    "--name",
                    f"{volume}-seed",
                    *label_args,
                    "--mount",
                    f"type=volume,source={volume},target=/curie-input",
                    "curie-runner",
                ],
                request_timeout_seconds=1.0,
            )
        if operation == "list":
            labels = {
                **base_labels,
                "curietech.ai/claim": f"claim-{stack}",
                "curietech.ai/component": "claim-runner",
            }
            label_args = [
                arg for key, value in labels.items() for arg in ("--label", f"{key}={value}")
            ]
            client._docker(
                ["create", "--name", f"claim-{stack}", *label_args, "curie-runner"],
                request_timeout_seconds=1.0,
            )
    expected_volumes = {
        name: dict(labels)
        for name, labels in client.volumes.items()
        if operation == "list" or labels["curietech.ai/stack"] == "stack-neighbor"
    }
    expected_files = {name: dict(client.volume_files[name]) for name in expected_volumes}
    expected_containers = {
        name: dict(labels)
        for name, labels in client.containers.items()
        if operation == "list" or labels["curietech.ai/stack"] == "stack-neighbor"
    }
    expected_status = {name: client.container_status[name] for name in expected_containers}

    if operation == "list":
        views = client.list_claims(label_selector="curietech.ai/managed-by=curie-sandbox-substrate")
        assert [view.name for view in views] == ["claim-stack-current"]
    elif operation == "reap":
        assert client.reap_claim_templates(keep=set(), created_before=cutoff) == ["claim-shared"]
    else:
        client.delete_claim("claim-shared", request_timeout_seconds=1.0)

    assert client.volumes == expected_volumes
    assert client.volume_files == expected_files
    assert client.containers == expected_containers
    assert client.container_status == expected_status


@pytest.mark.parametrize("component", ["claim-bundle", "claim-workspace", "claim-attachments"])
def test_input_copy_helpers_are_excluded_from_claim_views(component: str) -> None:
    client = _RecordingDocker(
        image="curie-runner", network="view-stack", bundle_store=_FakeBundleStore()
    )
    attempt = uuid.uuid4().hex
    for name, resource_component in (("claim-view", "claim-runner"), ("copy-helper", component)):
        client._docker(
            [
                "create",
                "--name",
                name,
                "--label",
                "curietech.ai/stack=view-stack",
                "--label",
                "curietech.ai/managed-by=curie-sandbox-substrate",
                "--label",
                "curietech.ai/claim=claim-view",
                "--label",
                f"curietech.ai/component={resource_component}",
                "--label",
                f"curietech.ai/claim-attempt={attempt}",
                "curie-runner",
            ],
            request_timeout_seconds=1.0,
        )

    assert client.get_claim("copy-helper", request_timeout_seconds=1.0) is None
    claims = client.list_claims(label_selector="curietech.ai/managed-by=curie-sandbox-substrate")
    assert [claim.name for claim in claims] == ["claim-view"]
    assert not claims[0].ready


def test_docker_cannot_prove_kubernetes_quota_headroom() -> None:
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
    )
    rejection = QuotaRejection(
        quota_name="curie-sandbox-quota",
        requested={"limits.cpu": "1"},
        used={"limits.cpu": "8"},
        hard={"limits.cpu": "8"},
    )

    assert not client.quota_has_headroom(
        rejection,
        request_timeout_seconds=1.0,
    )


def test_create_claim_argv_carries_boot_env() -> None:
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        network="curie_default",
        otel_endpoint="http://otel:4318",
    )
    client.create_claim(
        "thread-abc",
        pool="pool",
        env={
            "CURIE_BUDGET": '{"max_usd_per_day":5.0}',
            "CURIE_SESSION_ID": "sess-1",
            "CURIE_FAKE_MODEL": "1",
            "CURIE_PLUGIN_DIR": "/bundles/current",
        },
        labels={"curietech.ai/thread-hash": "abc"},
        deadline=time.monotonic() + 300,
    )
    argv = client.runner_argv
    assert argv[:3] == ["create", "--name", "thread-abc"]
    assert ["start", client.container_ids["thread-abc"]] in client.calls
    assert client.container_status["thread-abc"] == "running"
    assert "127.0.0.1::8080" in _flag_values(argv, "-p")
    assert "curie_default" in _flag_values(argv, "--network")
    labels = _flag_values(argv, "--label")
    assert "curietech.ai/managed-by=curie-sandbox-substrate" in labels
    assert "curietech.ai/thread-hash=abc" in labels
    envs = _flag_values(argv, "-e")
    assert "CURIE_PLUGIN_DIR=/bundles/current" in envs
    assert "CURIE_SANDBOX_ID=thread-abc" in envs
    assert "CURIE_RUNNER_PORT=8080" in envs
    assert 'CURIE_BUDGET={"max_usd_per_day":5.0}' in envs
    assert "CURIE_FAKE_MODEL=1" in envs
    assert "OTEL_EXPORTER_OTLP_ENDPOINT=http://otel:4318" in envs
    assert argv[-1] == "curie-runner"


def test_create_claim_forwards_the_claims_code_host_origin_and_no_github_url() -> None:
    """ADR 0197: the runner checks its checkout against the claim's origin and path.

    The worker sets them from the API's credential; the substrate injects no
    GitHub URL of its own.
    """

    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())

    client.create_claim(
        "thread-code-host",
        pool="pool",
        env={
            "CURIE_FAKE_MODEL": "1",
            "CURIE_REPO_ORIGIN": "https://gitlab.example.com",
            "CURIE_REPO_PATH": "group/sub/project",
        },
        deadline=time.monotonic() + 300,
    )

    envs = _flag_values(client.runner_argv, "-e")
    assert "CURIE_REPO_ORIGIN=https://gitlab.example.com" in envs
    assert "CURIE_REPO_PATH=group/sub/project" in envs
    assert not any(item.startswith("CURIE_GITHUB_API_URL=") for item in envs)


def test_create_claim_excludes_host_credentials_from_child_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    denied_names = {
        "POSTGRES_PASSWORD",
        "DATABASE_URL",
        "VALKEY_PASSWORD",
        "SLACK_BOT_TOKEN",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "CURIE_API_KEY",
        "LANGFUSE_SECRET_KEY",
        "CURIE_ADAPTER_CREDENTIALS",
        "CURIE_SEALING_PRIVATE_KEY",
        "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
        "CURIE_CONNECTOR_CALLER_SIGNING_KEY",
    }
    for name in denied_names:
        monkeypatch.setenv(name, "placeholder")
    monkeypatch.setenv("CURIE_BUDGET", "{}")
    monkeypatch.setenv("CURIE_CREDENTIALS", "placeholder")
    monkeypatch.delenv("CURIE_BUNDLE_REF", raising=False)
    monkeypatch.delenv("CURIE_FAKE_MODEL", raising=False)
    monkeypatch.delenv("ANTHROPIC_BASE_URL", raising=False)
    monkeypatch.delenv("CURIE_CONNECTOR_SECRET_KEYS", raising=False)

    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-credentials", pool="pool", env=os.environ, deadline=time.monotonic() + 300
    )

    child_env_names = {entry.partition("=")[0] for entry in _flag_values(client.runner_argv, "-e")}
    assert denied_names.isdisjoint(child_env_names)
    assert "CURIE_BUDGET" in child_env_names
    assert "CURIE_CREDENTIALS" in child_env_names


def test_create_claim_forwards_the_caller_token_and_never_its_signing_key() -> None:
    # ADR-0168 decision 7: the same split as the claim CR, on the substrate
    # that forwards env by value.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-caller",
        pool="pool",
        env={
            "CURIE_CONNECTOR_CALLER_TOKEN": "cct.payload.signature",
            "CURIE_CONNECTOR_CALLER_SIGNING_KEY": "placeholder",
        },
        deadline=time.monotonic() + 300,
    )
    forwarded = _flag_values(client.runner_argv, "-e")
    assert "CURIE_CONNECTOR_CALLER_TOKEN=cct.payload.signature" in forwarded
    assert not [e for e in forwarded if e.startswith("CURIE_CONNECTOR_CALLER_SIGNING_KEY=")]


def test_create_claim_preserves_declared_connector_secret() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-connector-secret",
        pool="pool",
        env={
            "CURIE_CONNECTOR_SECRET_KEYS": "SLACK_BOT_TOKEN",
            "SLACK_BOT_TOKEN": "placeholder",
        },
        deadline=time.monotonic() + 300,
    )

    child_env_names = {entry.partition("=")[0] for entry in _flag_values(client.runner_argv, "-e")}
    assert {"CURIE_CONNECTOR_SECRET_KEYS", "SLACK_BOT_TOKEN"} <= child_env_names


def test_local_substrate_claims_runner_with_connector_secret_without_a_warm_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    pools: list[str] = []
    original_create_claim = client.create_claim

    def record_claim(
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        pools.append(pool)
        original_create_claim(name, pool=pool, env=env, labels=labels, **kwargs)

    monkeypatch.setattr(client, "create_claim", record_claim)
    monkeypatch.setattr(
        client,
        "get_claim",
        lambda name, *, request_timeout_seconds: ClaimView(
            name, True, name, datetime.now(UTC), None, None, None
        ),
    )
    monkeypatch.setattr(
        client,
        "get_sandbox",
        lambda name, *, request_timeout_seconds: SandboxView(
            name, True, "127.0.0.1", "Running", 8080
        ),
    )
    substrate = SandboxSubstrate(
        client,
        cast(AffinityStore, object()),
        SubstrateConfig(namespace="default", warm_pool="curie-runner-pool"),
    )
    env = {
        "CURIE_CONNECTOR_SECRET_KEYS": "GRAFANA_SERVICE_ACCOUNT_TOKEN",
        "GRAFANA_SERVICE_ACCOUNT_TOKEN": "placeholder",
    }

    handle = substrate._claim_fresh(
        "local-connector",
        env=env,
        state=RouteState.LIVE,
        agent_name="sre-bot",
        publish=False,
    )

    assert handle.sandbox_name == handle.claim_name
    assert pools == ["curie-runner-pool"]
    child_env = _flag_values(client.runner_argv, "-e")
    assert "GRAFANA_SERVICE_ACCOUNT_TOKEN=placeholder" in child_env


def test_create_claim_connector_marker_cannot_readmit_reserved_curie_credential() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-reserved-connector-secret",
        pool="pool",
        env={
            "CURIE_CONNECTOR_SECRET_KEYS": "CURIE_SEALING_PRIVATE_KEY",
            "CURIE_SEALING_PRIVATE_KEY": "placeholder",
        },
        deadline=time.monotonic() + 300,
    )

    child_env_names = {entry.partition("=")[0] for entry in _flag_values(client.runner_argv, "-e")}
    assert "CURIE_CONNECTOR_SECRET_KEYS" in child_env_names
    assert "CURIE_SEALING_PRIVATE_KEY" not in child_env_names


def test_create_claim_excludes_every_slack_identity_token_from_child_env() -> None:
    """ADR-0168 decisions 1 and 5: each declared Slack identity's tokens reach
    the worker as indexed `CURIE_SLACK_*__<n>` variables. The filter drops them
    by prefix, at any index, and a connector marker cannot readmit one. The
    non-secret declaration itself carries no token and may pass."""

    tokens = {
        "CURIE_SLACK_BOT_TOKEN__0": "placeholder",
        "CURIE_SLACK_BOT_TOKEN__12": "placeholder",
        "CURIE_SLACK_APP_TOKEN__0": "placeholder",
        "CURIE_SLACK_SIGNING_SECRET__0": "placeholder",
    }
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-slack-identities",
        pool="pool",
        env={
            **tokens,
            "CURIE_SLACK_IDENTITIES": "[]",
            "CURIE_CONNECTOR_SECRET_KEYS": "CURIE_SLACK_BOT_TOKEN__0",
        },
        deadline=time.monotonic() + 300,
    )

    child_env_names = {entry.partition("=")[0] for entry in _flag_values(client.runner_argv, "-e")}
    assert tokens.keys().isdisjoint(child_env_names)
    assert "CURIE_SLACK_IDENTITIES" in child_env_names


def test_create_claim_fetches_and_unwraps_bundle() -> None:
    store = _FakeBundleStore(_plugin_tar_gz(wrapper="deal-desk"))
    client = _RecordingDocker(image="curie-runner", bundle_store=store)
    client.create_claim(
        "t1",
        pool="pool",
        env={
            "CURIE_BUNDLE_REF": "bundles/b.tar.gz",
            "CURIE_PLUGIN_DIR": "/bundles/current",
            "CURIE_BUNDLE_VERSION": "abc123def456",
        },
        deadline=time.monotonic() + 300,
    )
    assert store.requested == ["bundles/b.tar.gz"]  # the worker fetched the bundle
    mounts = _volume_mounts(client.runner_argv)
    assert set(mounts) == {"/bundles/current"}
    assert mounts["/bundles/current"][1] == "ro"
    host_dir = _seed_source(client, "/bundles/current")
    volume, mode = mounts["/bundles/current"]
    assert client.volume_files[volume] == {
        ".claude-plugin/plugin.json": b'{"name": "deal-desk", "version": "0.1.0"}'
    }
    assert not host_dir.exists(), "the local bundle copy survived successful seeding"
    child_env = _flag_values(client.runner_argv, "-e")
    # The RustFS object key itself is not forwarded into the container env.
    # Updated deliberately (#2174): keep stripping the object key, and forward
    # the platform-tracked version_label so a sandboxed agent can name itself.
    assert all("CURIE_BUNDLE_REF" not in e for e in child_env)
    assert "CURIE_BUNDLE_VERSION=abc123def456" in child_env


def test_create_claim_prefetches_workspace_without_forwarding_capability_env(
    tmp_path: Path,
) -> None:
    """Docker consumes workspace capabilities at the trusted worker boundary.

    Kubernetes gives these values only to its trusted init containers
    (``test_workspace_capability_targets_only_workspace_init_containers``).
    Docker has no init container, so its worker performs the equivalent fetch
    before mounting the checkout. Neither opaque capability may survive in the
    untrusted runner's environment.
    """

    checkout = tmp_path / "trusted-checkout"
    checkout.mkdir()
    workspace_ref = "opaque-presigned-workspace-reference"
    workspace_sha256 = "a" * 64

    class _WorkspacePreparingDocker(_RecordingDocker):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.prepared: list[tuple[str, str, str]] = []

        def _prepare_workspace(self, name: str, encoded_ref: str, expected_sha256: str) -> str:
            self.prepared.append((name, encoded_ref, expected_sha256))
            return str(checkout)

    client = _WorkspacePreparingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-workspace",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_WORKSPACE_REF": workspace_ref,
            "CURIE_WORKSPACE_SHA256": workspace_sha256,
        },
        deadline=time.monotonic() + 300,
    )

    argv = client.runner_argv
    attempt = dict(label.split("=", 1) for label in _flag_values(argv, "--label"))[
        "curietech.ai/claim-attempt"
    ]
    assert client.prepared == [(attempt, workspace_ref, workspace_sha256)]
    assert _volume_mounts(argv)["/workspace"][1] == "rw"
    assert _seed_source(client, "/workspace") == checkout
    child_env = _flag_values(argv, "-e")
    child_env_names = {entry.partition("=")[0] for entry in child_env}
    assert "CURIE_WORKSPACE_REF" not in child_env_names
    assert "CURIE_WORKSPACE_SHA256" not in child_env_names
    assert all(workspace_ref not in entry for entry in child_env)
    assert all(workspace_sha256 not in entry for entry in child_env)


def test_create_claim_without_bundle_ref_mounts_nothing() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"}, deadline=time.monotonic() + 300
    )
    assert _flag_values(client.runner_argv, "-v") == []


def test_sdk_credential_forwarded_by_name_only() -> None:
    # A real-model run: the SDK token is present in the worker env. It must be
    # forwarded by NAME (docker reads the value), never with its value in the argv.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={"CLAUDE_CODE_OAUTH_TOKEN": "sk-PLACEHOLDER-never-real"},
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_BUDGET": "{}"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    envs = _flag_values(argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" in envs  # forwarded by name
    assert all("PLACEHOLDER" not in a for a in argv)  # the value never leaks in


def test_sdk_credential_not_forwarded_when_absent() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore(), environ={})
    client.create_claim(
        "t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"}, deadline=time.monotonic() + 300
    )
    envs = _flag_values(client.runner_argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs
    assert "ANTHROPIC_API_KEY" not in envs


def test_curie_credentials_forwarded_by_name_never_as_value() -> None:
    # The ACI credential reference must reach the runner (which maps it) without
    # its value ever appearing in the docker argv.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={"CURIE_CREDENTIALS": "sk-ant-PLACEHOLDER-secret"},
    )
    # The worker also hands it in the boot env (from config.credentials); it must
    # still never be emitted as a -e KEY=value pair.
    client.create_claim(
        "t1",
        pool="pool",
        env={"CURIE_CREDENTIALS": "sk-ant-PLACEHOLDER-secret"},
        deadline=time.monotonic() + 300,
    )
    argv = client.runner_argv
    envs = _flag_values(argv, "-e")
    assert "CURIE_CREDENTIALS" in envs  # forwarded by name
    assert all("PLACEHOLDER-secret" not in a for a in argv)  # value never in argv
    assert "CURIE_CREDENTIALS=sk-ant-PLACEHOLDER-secret" not in envs


def test_ambient_sdk_creds_not_forwarded_when_curie_credentials_set() -> None:
    # BYO model: an explicit CURIE_CREDENTIALS is present, and the operator's
    # shell also carries ambient SDK tokens. Positive selection forwards exactly
    # CURIE_CREDENTIALS by name and does NOT forward the ambient SDK vars (which
    # would re-inject the operator's token and shadow the BYO credential in the
    # runner).
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={
            "CLAUDE_CODE_OAUTH_TOKEN": "sk-PLACEHOLDER-oauth",
            "ANTHROPIC_API_KEY": "sk-ant-PLACEHOLDER-key",
            "CURIE_CREDENTIALS": "sk-or-PLACEHOLDER-byo",
        },
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_BUDGET": "{}"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    envs = _flag_values(argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient OAuth token not forwarded by name
    assert "ANTHROPIC_API_KEY" not in envs  # ambient API key not forwarded by name
    assert "CURIE_CREDENTIALS" in envs  # explicit BYO reference still forwarded by name
    assert all("PLACEHOLDER" not in a for a in argv)  # no secret value leaks into argv


def test_ambient_sdk_creds_forwarded_when_curie_credentials_empty() -> None:
    # Legacy real-model path: an empty CURIE_CREDENTIALS placeholder (a blank
    # line in compose's .env) must NOT suppress a real ambient OAuth token.
    # Positive selection keys on VALUE-truthiness, not key-presence, so an empty
    # CURIE_CREDENTIALS is treated as absent and the ambient SDK var is forwarded.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={
            "CURIE_CREDENTIALS": "",
            "CLAUDE_CODE_OAUTH_TOKEN": "sk-PLACEHOLDER-oauth",
        },
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_BUDGET": "{}"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    envs = _flag_values(argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" in envs  # ambient OAuth token forwarded by name
    assert all("PLACEHOLDER" not in a for a in argv)  # the value never leaks into argv


# Placeholder ambient OAuth token (never real). Hoisted into a named constant so
# the secrets-scan pre-commit hook does not false-positive on an inline
# `"CLAUDE_CODE_OAUTH_TOKEN": "sk-..."` literal; the value is asserted absent from
# the forwarded argv below.
_AMBIENT_OAUTH = "sk-PLACEHOLDER-oauth"


def test_no_credential_forwarded_under_fake_model() -> None:
    # A fake-model run needs no model credential: neither the explicit BYO
    # reference nor the ambient SDK token must ride into the untrusted runner.
    # Gating keys on the boot env (CURIE_FAKE_MODEL present), not the worker
    # environ, so even a fully-credentialed worker leaks nothing.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={
            "CLAUDE_CODE_OAUTH_TOKEN": _AMBIENT_OAUTH,
            "CURIE_CREDENTIALS": "sk-ant-PLACEHOLDER",
        },
    )
    client.create_claim(
        "t1",
        pool="pool",
        env={"CURIE_FAKE_MODEL": "1", "CURIE_BUDGET": "{}"},
        deadline=time.monotonic() + 300,
    )
    envs = _flag_values(client.runner_argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient SDK token not forwarded
    assert "ANTHROPIC_API_KEY" not in envs
    assert "CURIE_CREDENTIALS" not in envs  # BYO reference not forwarded either
    assert all("PLACEHOLDER" not in a for a in client.runner_argv)  # no value leaks


def test_ambient_sdk_creds_not_forwarded_under_local_model() -> None:
    # A local/base-URL-override run (ANTHROPIC_BASE_URL in the boot env) points the
    # runner at a local endpoint that needs no Anthropic credential, so the ambient
    # SDK token must not ride into that container.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={"CLAUDE_CODE_OAUTH_TOKEN": _AMBIENT_OAUTH},
    )
    client.create_claim(
        "t1",
        pool="pool",
        env={"ANTHROPIC_BASE_URL": "http://ollama:11434", "CURIE_BUDGET": "{}"},
        deadline=time.monotonic() + 300,
    )
    envs = _flag_values(client.runner_argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient SDK token not forwarded
    assert "ANTHROPIC_API_KEY" not in envs
    assert all("PLACEHOLDER" not in a for a in client.runner_argv)  # no value leaks


def test_explicit_credential_forwarded_under_base_url_override() -> None:
    # BYO OpenRouter with a preset base URL: the runner routes an sk-or- key into
    # ANTHROPIC_API_KEY even when ANTHROPIC_BASE_URL is set (runner sdk_auth), so an
    # EXPLICIT CURIE_CREDENTIALS must still be forwarded by name -- while the
    # ambient SDK token still must not ride along.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={
            "CLAUDE_CODE_OAUTH_TOKEN": _AMBIENT_OAUTH,
            "CURIE_CREDENTIALS": "sk-or-PLACEHOLDER",
        },
    )
    client.create_claim(
        "t1",
        pool="pool",
        env={"ANTHROPIC_BASE_URL": "https://openrouter.ai/api", "CURIE_BUDGET": "{}"},
        deadline=time.monotonic() + 300,
    )
    envs = _flag_values(client.runner_argv, "-e")
    assert "CURIE_CREDENTIALS" in envs  # BYO OpenRouter key still forwarded by name
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient SDK token still suppressed
    assert "ANTHROPIC_API_KEY" not in envs
    assert all("PLACEHOLDER" not in a for a in client.runner_argv)  # value never in argv


def _runner_inspection(
    *,
    status: str = "running",
    stack: str = "default",
    created: str = "2026-08-16T12:00:00.123456789Z",
    started: str = "2026-08-16T12:00:00.123456789Z",
) -> str:
    labels = {
        "curietech.ai/managed-by": "curie-sandbox-substrate",
        "curietech.ai/stack": stack,
        "curietech.ai/claim": "t1",
        "curietech.ai/component": "claim-runner",
        "curietech.ai/claim-attempt": "a" * 32,
    }
    return "\t".join((status, json.dumps(labels), created, "b" * 64, started))


def test_get_sandbox_reports_published_port_and_mode() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {
        "inspect": _runner_inspection(),
        "port": "127.0.0.1:49173\n",
    }
    view = client.get_sandbox("t1", request_timeout_seconds=1.0)
    assert view is not None
    assert view.service_fqdn == "127.0.0.1"
    assert view.port == 49173
    assert view.operating_mode == "Running"
    assert client.timeouts
    assert all(0 < timeout <= 1.0 for timeout in client.timeouts)

    client.outputs["inspect"] = _runner_inspection(status="paused")
    paused = client.get_sandbox("t1", request_timeout_seconds=1.0)
    assert paused is not None
    assert paused.operating_mode == "Suspended"
    assert all(0 < timeout <= 1.0 for timeout in client.timeouts)


def test_get_claim_surfaces_the_container_created_at() -> None:
    # The local tier's reaper reads its bind-window grace off this value, so a
    # claim whose age never arrives is an orphan that is never reaped. The
    # nine-digit fraction in the fixture is not a synthetic edge case: it is
    # the RFC3339Nano form docker actually emits for .Created. On this repo's
    # Python, datetime.fromisoformat parses it and truncates to microseconds,
    # and production relies on exactly that, so this pins that the parser is
    # handed the real docker format and keeps microsecond precision. Do not
    # reintroduce a manual truncation step ahead of it.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {"inspect": _runner_inspection(), "port": ""}

    view = client.get_claim("t1", request_timeout_seconds=1.0)
    assert view is not None
    assert view.quota_rejection is None
    assert view.ready_reason is None
    assert view.ready_message is None
    created = view.created_at
    assert created is not None
    assert created.utcoffset() == timedelta(0)  # tz-aware UTC, never naive
    # Asserted to the microsecond, not to the second: dropping the fraction
    # instead of truncating it reads a container up to a second early, which is
    # a second of grace the reaper never gives back at the boundary.
    assert created == datetime(2026, 8, 16, 12, 0, 0, 123456, tzinfo=UTC)


def test_get_claim_age_begins_when_runner_starts_after_seeding() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {
        "inspect": _runner_inspection(
            created="2026-08-16T12:00:00.123456789Z",
            started="2026-08-16T14:00:00.987654321Z",
        ),
        "port": "",
    }

    view = client.get_claim("t1", request_timeout_seconds=1.0)

    assert view is not None
    assert view.created_at == datetime(2026, 8, 16, 14, 0, 0, 987654, tzinfo=UTC)


def test_get_sandbox_none_when_container_absent() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {"inspect": ""}  # docker inspect on a missing container
    assert client.get_sandbox("gone", request_timeout_seconds=1.0) is None


def test_get_sandbox_treats_dead_container_as_gone() -> None:
    # An exited/dead/created container is NOT a live sandbox: report it gone so
    # the substrate evicts the stale route instead of dialing a dead runner.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    for dead in ("exited", "dead", "created", "restarting"):
        client.outputs = {
            "inspect": _runner_inspection(status=dead),
            "port": "",
        }
        assert client.get_sandbox("t1", request_timeout_seconds=1.0) is None, dead


class _NetworkAwareDocker(DockerSandboxClient):
    """Fake that distinguishes the status inspect from the networks inspect (both
    are ``docker inspect``, so keying on the subcommand alone is not enough)."""

    def __init__(self, *, networks_json: str, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._networks_json = networks_json
        self._inspection = _runner_inspection(stack=str(kwargs.get("network") or "default"))

    def _docker(
        self,
        args: list[str],
        *,
        request_timeout_seconds: float,
        check: bool = True,
    ) -> str:
        assert request_timeout_seconds > 0
        if args and args[0] == "inspect":
            if any("NetworkSettings.Networks" in a for a in args):
                return self._networks_json
            return self._inspection
        if args and args[0] == "port":
            return "127.0.0.1:49173\n"
        return ""


def test_get_sandbox_dials_container_ip_on_shared_network() -> None:
    # A host-net worker container on a Docker Desktop VM cannot reach the runner's
    # host-loopback publish, but can reach its shared-network container IP. So the
    # dial target is that IP at the fixed container port, not the host port.
    client = _NetworkAwareDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        network="curie_default",
        networks_json='{"curie_default": {"IPAddress": "172.20.0.11"}}',
    )
    view = client.get_sandbox("t1", request_timeout_seconds=1.0)
    assert view is not None
    assert view.service_fqdn == "172.20.0.11"
    assert view.port == RUNNER_CONTAINER_PORT  # not the Docker-assigned host port
    assert view.operating_mode == "Running"


def test_get_sandbox_falls_back_to_published_port_without_network_ip() -> None:
    # No shared-network IP yet (or no network): dial the Docker-assigned host
    # loopback port, preserving the host-process worker path.
    client = _NetworkAwareDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        network="curie_default",
        networks_json="{}",
    )
    view = client.get_sandbox("t1", request_timeout_seconds=1.0)
    assert view is not None
    assert view.service_fqdn == "127.0.0.1"
    assert view.port == 49173


def test_prepare_bundle_is_readable_by_nonroot_runner() -> None:
    # The staged bundle is bind-mounted :ro into a runner running as uid 1000, so
    # the tree must be group/other traversable+readable. mkdtemp defaults to 0700,
    # which the non-root runner cannot enter -- _prepare_bundle must widen it.
    client = _RecordingDocker(
        image="curie-runner", bundle_store=_FakeBundleStore(_plugin_tar_gz(wrapper=None))
    )
    root = Path(client._prepare_bundle("t1", "bundles/b.tar.gz"))
    nested_dir = root / ".claude-plugin"
    nested_file = nested_dir / "plugin.json"

    assert root.stat().st_mode & 0o005 == 0o005  # root dir: o+rx
    assert nested_dir.stat().st_mode & 0o005 == 0o005  # nested dir: o+rx
    assert nested_file.stat().st_mode & 0o004 == 0o004  # nested file: o+r


def test_runner_token_forwarded_as_docker_env() -> None:
    # The per-sandbox runner token rides the generic claim-env loop (it is not a
    # worker-owned or credential var), so it must be emitted as -e KEY=value.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "t1",
        pool="pool",
        env={"CURIE_BUDGET": "{}", "CURIE_RUNNER_TOKEN": "tok-25"},
        deadline=time.monotonic() + 300,
    )
    envs = _flag_values(client.runner_argv, "-e")
    assert "CURIE_RUNNER_TOKEN=tok-25" in envs


def test_runner_token_forwarded_even_when_credentials_selected() -> None:
    # Credential selection (PR #109) forwards CURIE_CREDENTIALS by name and
    # suppresses ambient SDK vars, but must not disturb the token, which travels
    # in the claim env as an ordinary value.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={"CURIE_CREDENTIALS": "sk-PLACEHOLDER-byo"},
    )
    client.create_claim(
        "t1",
        pool="pool",
        env={
            "CURIE_CREDENTIALS": "sk-PLACEHOLDER-byo",
            "CURIE_RUNNER_TOKEN": "tok-25b",
        },
        deadline=time.monotonic() + 300,
    )
    envs = _flag_values(client.runner_argv, "-e")
    assert "CURIE_RUNNER_TOKEN=tok-25b" in envs
    assert "CURIE_CREDENTIALS" in envs  # still forwarded by name
    assert "CURIE_CREDENTIALS=sk-PLACEHOLDER-byo" not in envs  # never as a value


def test_extract_bundle_unwraps_single_wrapper_dir() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = extract_bundle(_plugin_tar_gz(wrapper="deal-desk"), Path(tmp))
        assert (root / ".claude-plugin" / "plugin.json").is_file()

    with tempfile.TemporaryDirectory() as tmp:
        # A flat bundle (manifest already at the archive root) is not descended.
        root = extract_bundle(_plugin_tar_gz(wrapper=None), Path(tmp))
        assert root == Path(tmp)
        assert (root / ".claude-plugin" / "plugin.json").is_file()


def _plugin_tar_gz_with_filler(size: int) -> bytes:
    """Like ``_plugin_tar_gz`` but with an extra file of ``size`` uncompressed
    bytes, so the archive's total declared uncompressed size is controllable."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        manifest = b'{"name": "deal-desk", "version": "0.1.0"}'
        info = tarfile.TarInfo(".claude-plugin/plugin.json")
        info.size = len(manifest)
        tf.addfile(info, io.BytesIO(manifest))
        filler = b"x" * size
        filler_info = tarfile.TarInfo("big.bin")
        filler_info.size = len(filler)
        tf.addfile(filler_info, io.BytesIO(filler))
    return buf.getvalue()


def test_extract_bundle_rejects_a_bundle_over_the_uncompressed_cap() -> None:
    # ADR-0059 decision 3: the Docker-substrate claim-time extraction is bound
    # the same as the API upload path, via the same plugin_format helper.
    import tempfile

    from plugin_format import UnsupportedArchive

    with tempfile.TemporaryDirectory() as tmp:
        try:
            extract_bundle(
                _plugin_tar_gz_with_filler(5000),
                Path(tmp),
                max_uncompressed_bytes=1000,
            )
            raise AssertionError("expected UnsupportedArchive")
        except UnsupportedArchive as exc:
            assert "1000 byte limit" in str(exc)
        # Nothing was written: the bound check runs before extraction.
        assert list(Path(tmp).iterdir()) == []


def test_prepare_bundle_rejects_oversized_bundle_and_writes_nothing() -> None:
    # The operator-configured cap (mirroring WorkerConfig's default) is
    # threaded from the client constructor through to extract_bundle at
    # claim time, so an oversized bundle fails the claim instead of silently
    # staging an unbounded tree for the runner to bind-mount.
    from plugin_format import UnsupportedArchive

    data = _plugin_tar_gz_with_filler(5000)
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(data),
        bundle_max_uncompressed_bytes=1000,
    )
    try:
        client._prepare_bundle("t1", "bundles/big.tar.gz")
        raise AssertionError("expected UnsupportedArchive")
    except UnsupportedArchive:
        pass
    # The claim-scoped bundle dir was never registered (cleaned up on failure).
    assert client._bundle_dirs == {}


# ---------------------------------------------------------------------------
# Container hardening (#631): every spawned runner is isolated at the container
# level (read-only rootfs, dropped caps, no privilege escalation, bounded
# resources) and never receives the Docker socket or a route to the data tier.
# ---------------------------------------------------------------------------


def test_create_claim_applies_container_hardening_by_default() -> None:
    # A default client hardens every runner: read-only rootfs + tmpfs for the
    # writable paths, all caps dropped, no-new-privileges, bounded pids/mem/cpu.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    assert "--read-only" in argv
    tmpfs = _flag_values(argv, "--tmpfs")
    assert "/tmp:rw,mode=1777" in tmpfs
    assert "/home/runner:rw,mode=1777" in tmpfs
    assert "ALL" in _flag_values(argv, "--cap-drop")
    assert "no-new-privileges" in _flag_values(argv, "--security-opt")
    assert "512" in _flag_values(argv, "--pids-limit")
    assert "768m" in _flag_values(argv, "--memory")
    assert "1" in _flag_values(argv, "--cpus")
    # The default seccomp profile stays active: never disabled.
    assert all("seccomp=unconfined" not in a for a in argv)


def test_runner_never_receives_the_docker_socket() -> None:
    # AC3: the runner must not be able to reach the Docker daemon. Only the worker
    # mounts the socket; a spawned runner never gets it as a bind mount.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={"CLAUDE_CODE_OAUTH_TOKEN": "sk-PLACEHOLDER"},
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_BUDGET": "{}"}, deadline=time.monotonic() + 300
    )
    assert all("docker.sock" not in m for m in _flag_values(client.runner_argv, "-v"))


def test_hardening_disabled_emits_no_hardening_flags() -> None:
    # An explicit opt-out (CURIE_RUNNER_HARDENING=0) degrades to the
    # pre-hardening argv exactly, for debugging a bundle the rails break.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        hardening=RunnerHardening.from_env({"CURIE_RUNNER_HARDENING": "0"}),
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    assert "--read-only" not in argv
    assert _flag_values(argv, "--cap-drop") == []
    assert _flag_values(argv, "--memory") == []


def test_hardening_env_overrides_limits_and_paths() -> None:
    # Each knob is overridable so an operator can loosen a limit a heavy bundle
    # needs without editing code.
    hardening = RunnerHardening.from_env(
        {
            "CURIE_RUNNER_MEMORY_LIMIT": "2g",
            "CURIE_RUNNER_CPU_LIMIT": "2",
            "CURIE_RUNNER_PIDS_LIMIT": "1024",
            "CURIE_RUNNER_WRITABLE_PATHS": "/tmp, /home/runner, /var/scratch",
        }
    )
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        hardening=hardening,
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    assert "2g" in _flag_values(argv, "--memory")
    assert "2" in _flag_values(argv, "--cpus")
    assert "1024" in _flag_values(argv, "--pids-limit")
    tmpfs = _flag_values(argv, "--tmpfs")
    assert "/var/scratch:rw,mode=1777" in tmpfs
    assert "/tmp:rw,mode=1777" in tmpfs


def test_hardening_read_only_opt_out_keeps_other_rails() -> None:
    # Turning off just the read-only rootfs (a bundle that legitimately writes
    # outside the tmpfs paths) still drops caps and blocks privilege escalation.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        hardening=RunnerHardening.from_env({"CURIE_RUNNER_READ_ONLY": "false"}),
    )
    client.create_claim(
        "t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"}, deadline=time.monotonic() + 300
    )
    argv = client.runner_argv
    assert "--read-only" not in argv
    assert _flag_values(argv, "--tmpfs") == []  # no read-only -> no tmpfs needed
    assert "ALL" in _flag_values(argv, "--cap-drop")
    assert "no-new-privileges" in _flag_values(argv, "--security-opt")


def test_ensure_image_pulls_when_absent() -> None:
    # docker image inspect returns "" (with check=False) when the image is not
    # cached locally, so ensure_image must pull it -- the IfNotPresent + prewarm.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {}  # "image" inspect -> "" == absent
    client.ensure_image()
    assert ["image", "inspect", "curie-runner"] in client.calls
    assert ["pull", "curie-runner"] in client.calls
    inspect_at = client.calls.index(["image", "inspect", "curie-runner"])
    pull_at = client.calls.index(["pull", "curie-runner"])
    assert inspect_at < pull_at  # inspect first, then pull


def test_ensure_image_skips_pull_when_present() -> None:
    # A non-empty inspect means the image is already cached: no pull, so an
    # offline run with the image present must not touch the network.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {"image": '[{"Id":"sha256:cafef00d"}]'}
    client.ensure_image()
    assert ["image", "inspect", "curie-runner"] in client.calls
    assert all(argv[0] != "pull" for argv in client.calls)


def test_ensure_image_is_best_effort_on_pull_failure(caplog) -> None:
    # A pull failure (offline, registry down) must NOT crash worker startup: a
    # truly-missing image still fails clearly later at claim time. The warning
    # names only the image -- never the argv or the docker stderr.
    class _PullFailsDocker(DockerSandboxClient):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]
            self.calls: list[list[str]] = []

        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            assert request_timeout_seconds > 0
            self.calls.append(args)
            if args[0] == "pull":
                raise DockerError("docker pull failed (1): SENTINEL_STDERR_LEAK")
            return ""  # inspect: absent

    client = _PullFailsDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
    )
    with caplog.at_level(logging.WARNING, logger="curie_worker.sandbox.docker"):
        client.ensure_image()  # must return normally, no exception propagates
    assert ["pull", "curie-runner"] in client.calls
    text = caplog.text
    assert "curie-runner" in text  # the warning names the image
    assert "SENTINEL_STDERR_LEAK" not in text  # but never dumps the stderr


def test_ensure_image_is_best_effort_when_docker_unavailable(caplog) -> None:
    # A missing docker binary (or unreachable daemon) makes subprocess.run raise
    # FileNotFoundError/OSError REGARDLESS of check=False, and that raise happens
    # at the inspect step -- before any pull. That OSError must NOT crash worker
    # startup: ensure_image is best-effort end to end, so the inspect call must be
    # guarded too. The warning still names only the image, never the argv.
    class _DockerUnavailable(DockerSandboxClient):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]
            self.calls: list[list[str]] = []

        def _docker(
            self,
            args: list[str],
            *,
            request_timeout_seconds: float,
            check: bool = True,
        ) -> str:
            assert request_timeout_seconds > 0
            self.calls.append(args)
            if args[:2] == ["image", "inspect"]:
                # subprocess.run(["docker", ...]) raises this when the docker
                # binary is absent, independent of check=False.
                raise FileNotFoundError("docker")
            return ""

    client = _DockerUnavailable(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
    )
    with caplog.at_level(logging.WARNING, logger="curie_worker.sandbox.docker"):
        client.ensure_image()  # must return normally, no exception propagates
    assert ["image", "inspect", "curie-runner"] in client.calls
    assert "curie-runner" in caplog.text  # the warning names the image


def test_missing_runner_network_error_carries_a_remediation_hint(monkeypatch) -> None:
    """#715: a `docker run` failing because our own configured network doesn't
    exist (compose topology drift -- exactly what escalated as an opaque
    runner-error while getting a real agent working locally) must raise a
    DockerError that tells the reader what to DO, not just what failed."""
    import subprocess

    from curie_worker.sandbox.docker import DockerError as _DockerError

    class _FakeCompletedProcess:
        returncode = 125
        stderr = (
            "docker: Error response from daemon: failed to set up container"
            " networking: network curie_runner not found."
        )
        stdout = ""

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _FakeCompletedProcess())

    client = DockerSandboxClient(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        network="curie_runner",
    )
    try:
        client._docker(
            ["run", "--rm", "curie-runner"],
            request_timeout_seconds=30.0,
        )
        raise AssertionError("expected DockerError")
    except _DockerError as exc:
        assert "curie_runner" in str(exc)
        assert "curie local up" in str(exc)


def test_docker_error_without_a_matching_network_name_carries_no_hint() -> None:
    """The remediation hint is specific to OUR configured network going
    missing -- an unrelated docker failure (e.g. no such image) must not gain
    a misleading "run curie local up" tacked onto it."""
    client = DockerSandboxClient(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        network="curie_runner",
    )
    hint = client._network_remediation_hint("Error: No such image: curie-runner:latest")
    assert hint == ""


# -- ADR 0217: the Docker substrate runs no per-claim tool daemon ---------------


def _tool_daemon_mount() -> ToolDaemonMount:
    return ToolDaemonMount(
        files={name: "placeholder" for name in ("ca.pem", "cert.pem", "key.pem")},
        env={
            "DOCKER_HOST": "tcp://claim.acme-docker.svc:2376",
            "DOCKER_PUBLISHED_HOST": "claim.acme-docker.svc",
            "DOCKER_TLS_VERIFY": "1",
            "DOCKER_CERT_PATH": "/var/run/curie/docker",
        },
        active_deadline_seconds=3600,
    )


def test_docker_claim_with_a_tool_daemon_is_refused_before_any_docker_call() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    with pytest.raises(SandboxError):
        client.create_claim(
            "claim-docker",
            pool="pool",
            env={"CURIE_BUDGET": "{}"},
            deadline=time.monotonic() + 300,
            tool_daemon=_tool_daemon_mount(),
        )
    assert client.calls == []
    assert client.containers == {}


def test_docker_claim_without_a_tool_daemon_still_starts() -> None:
    # Liveness of the refusal above: the keyword at its default changes nothing.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "claim-plain",
        pool="pool",
        env={"CURIE_BUDGET": "{}"},
        deadline=time.monotonic() + 300,
        tool_daemon=None,
    )
    assert client.runner_argv


@pytest.mark.parametrize(
    ("method", "args", "kwargs"),
    [
        (
            "create_daemon_object",
            ("acme-docker", {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "c"}}),
            {"request_timeout_seconds": 1.0},
        ),
        (
            "list_daemon_objects",
            ("acme-docker", "Pod"),
            {"label_selector": "curietech.ai/sandbox-claim=c", "request_timeout_seconds": 1.0},
        ),
        (
            "delete_daemon_object",
            ("acme-docker", "Pod", "c"),
            {"uid": "00000000-0000-0000-0000-000000000001", "request_timeout_seconds": 1.0},
        ),
        ("get_daemon_pod", ("acme-docker", "c"), {"request_timeout_seconds": 1.0}),
        ("open_daemon_stream", ("acme-docker", "c", 2376), {"timeout_seconds": 1.0}),
    ],
)
def test_docker_client_refuses_every_daemon_call(
    method: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    with pytest.raises(SandboxError):
        result = getattr(client, method)(*args, **kwargs)
        # A context-manager stream refuses on entry at the latest.
        with result:
            pass
    assert client.calls == []


def test_lazy_attachments_boot_with_empty_writable_claim_cache() -> None:
    client = _RecordingDocker(
        image="curie-runner", network="lazy-stack", bundle_store=_FakeBundleStore(b"")
    )
    client.create_claim(
        "claim-lazy",
        pool="pool",
        env={"CURIE_ATTACHMENT_ACCESS_ENABLED": "1"},
        deadline=time.monotonic() + 300,
    )
    mounts = _volume_mounts(client.runner_argv)
    volume, mode = mounts["/attachments"]
    assert mode == "rw"
    assert client.volume_files[volume] == {}
    allocation = next(
        argv for argv in client.calls if argv[:2] == ["volume", "create"] and argv[-1] == volume
    )
    assert _flag_values(allocation, "--opt") == [
        "type=tmpfs",
        "device=tmpfs",
        "o=size=536870912,uid=1000,gid=1000,mode=0700",
    ]
    labels = client.volumes[volume]
    assert labels["curietech.ai/claim"] == "claim-lazy"
    client.delete_claim("claim-lazy", request_timeout_seconds=10)
    assert volume not in client.volumes
