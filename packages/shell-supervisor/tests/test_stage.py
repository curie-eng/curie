"""Staging and the child environment, in process."""

from __future__ import annotations

import io
import json
import os
import stat
import tarfile
from pathlib import Path

import pytest
from curie_shell_supervisor import stage
from curie_shell_supervisor.config import ConfigError, SupervisorConfig, check_private
from curie_test_support.static_shell import CONTROL_TOKEN, claim_archive

BEARER = "bearer-" + "a" * 40


def _config(tmp_path: Path, **overrides: object) -> SupervisorConfig:
    env = {
        "CURIE_SUPERVISOR_SLOT": "slot-a",
        "CURIE_SUPERVISOR_UNISOLATED_FOR_TESTS": "1",
        "CURIE_SUPERVISOR_CONTROL_TOKEN": CONTROL_TOKEN,
        "CURIE_SUPERVISOR_TLS_CERT_FILE": "cert.pem",
        "CURIE_SUPERVISOR_TLS_KEY_FILE": "key.pem",
        "CURIE_SUPERVISOR_STATE_ROOT": str(tmp_path / "state"),
        "CURIE_SUPERVISOR_CHILD_ENV": "MODEL_KEY",
        "CURIE_MODEL_ENV_KEY": "MODEL_KEY",
        "CURIE_SUPERVISOR_CHILD_ENV_PUBLIC": "OTEL_EXPORTER_OTLP_ENDPOINT",
        **{key: str(value) for key, value in overrides.items()},
    }
    return SupervisorConfig.from_env(env)


def _stage(tmp_path: Path, **manifest: object) -> tuple[stage.StagedClaim, stage.Manifest]:
    config = _config(tmp_path)
    body = {
        "v": 2,
        "claim": "claim-a",
        "generation": 1,
        "env": {"CURIE_RUNNER_TOKEN": BEARER, "HOME": "/elsewhere", "CURIE_FLAG": "1"},
        "executor_secret_names": None,
        "withheld_env": [],
        "attachments": "none",
        "workspace": False,
        **manifest,
    }
    archive = tmp_path / "claim.tar"
    archive.write_bytes(
        claim_archive("claim-a", 1, {}, manifest=body, files={"bundle/plugin.json": b"{}"})
    )
    return stage.prepare(archive, claim="claim-a", generation=1, config=config)


def test_the_control_credential_leaves_the_supervisor_environment(tmp_path: Path) -> None:
    env = {
        "CURIE_SUPERVISOR_SLOT": "slot-a",
        "CURIE_SUPERVISOR_UNISOLATED_FOR_TESTS": "1",
        "CURIE_SUPERVISOR_CONTROL_TOKEN": CONTROL_TOKEN,
        "CURIE_SUPERVISOR_TLS_CERT_FILE": "c",
        "CURIE_SUPERVISOR_TLS_KEY_FILE": "k",
    }
    config = SupervisorConfig.from_env(env)
    assert config.control_token == CONTROL_TOKEN
    assert "CURIE_SUPERVISOR_CONTROL_TOKEN" not in env
    assert CONTROL_TOKEN not in repr(config)


@pytest.mark.parametrize(
    "override",
    [
        {"CURIE_SUPERVISOR_CONTROL_TOKEN": "short"},
        {"CURIE_SUPERVISOR_CHILD_ENV": "CURIE_SUPERVISOR_CONTROL_TOKEN"},
        {"CURIE_SUPERVISOR_CHILD_UID": "1000"},
        # Without the explicit test-only switch, an unprivileged supervisor
        # cannot isolate its child and refuses to start.
        {"CURIE_SUPERVISOR_UNISOLATED_FOR_TESTS": "0"},
    ],
)
def test_unsafe_supervisor_settings_are_refused(tmp_path: Path, override: dict[str, str]) -> None:
    with pytest.raises(ConfigError):
        _config(tmp_path, **override)


def test_the_child_environment_is_built_name_by_name(tmp_path: Path) -> None:
    staged, manifest = _stage(tmp_path)
    environ = {
        "PATH": "/usr/bin",
        "MODEL_KEY": "model-secret",
        "CONNECTOR_A": "a-secret",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector",
        "UNLISTED": "never",
    }
    from curie_shell_supervisor.launch import claim_environment

    env = claim_environment(
        stage.child_environment(manifest, staged, config=_config(tmp_path), environ=environ)
    )
    assert env["MODEL_KEY"] == "model-secret"
    assert "CONNECTOR_A" not in env
    assert env["OTEL_EXPORTER_OTLP_ENDPOINT"] == "http://collector"
    assert "UNLISTED" not in env
    # The claim env reaches the child, except names the supervisor owns.
    assert env["CURIE_FLAG"] == "1" and env["CURIE_RUNNER_TOKEN"] == BEARER
    assert env["HOME"] == str(staged.home) and env["TMPDIR"] == str(staged.tmp)
    assert env["CURIE_PLUGIN_DIR"] == str(staged.bundle)
    assert env["CURIE_SANDBOX_ID"] == "claim-a"
    # The manifest carried credentials and is gone before the child runs.
    assert not (staged.root / "stage" / "claim.json").exists()


def test_an_executor_claim_gets_only_the_secrets_it_names(tmp_path: Path) -> None:
    staged, manifest = _stage(tmp_path, executor_secret_names=["MODEL_KEY"], withheld_env=[])
    environ = {"MODEL_KEY": "m", "CONNECTOR_A": "a", "CONNECTOR_B": "b"}
    from curie_shell_supervisor.launch import claim_environment

    env = claim_environment(
        stage.child_environment(manifest, staged, config=_config(tmp_path), environ=environ)
    )
    assert env.get("MODEL_KEY") == "m"
    assert "CONNECTOR_B" not in env and "CONNECTOR_A" not in env


def test_the_bundle_is_read_only_and_home_is_private(tmp_path: Path) -> None:
    staged, _ = _stage(tmp_path)
    assert stat.S_IMODE(staged.bundle.stat().st_mode) == 0o555
    assert stat.S_IMODE((staged.bundle / "plugin.json").stat().st_mode) == 0o444
    assert stat.S_IMODE(staged.home.stat().st_mode) == 0o700
    assert stage.wipe(staged.root)
    assert not staged.root.exists()


def test_every_claim_stages_at_the_same_path_and_never_into_a_leftover(
    tmp_path: Path,
) -> None:
    """#5571: a resumed claim works where the approved arguments point."""

    first, _ = _stage(tmp_path)
    (first.home / "approval-check.txt").write_text("from the first claim")
    with pytest.raises(FileExistsError):
        _stage(tmp_path)
    assert (first.home / "approval-check.txt").read_text() == "from the first claim"
    assert stage.wipe(first.root)
    second, _ = _stage(tmp_path)
    assert second.root == first.root and second.home == first.home
    assert not (second.home / "approval-check.txt").exists()
    assert stat.S_IMODE(second.root.stat().st_mode) == 0o711


def test_upload_tokens_are_private_claim_files_and_wiped(tmp_path: Path) -> None:
    tokens = {"reviewer": "ccu.example.signature"}
    staged, manifest = _stage(tmp_path, connector_upload_tokens=tokens)
    env = stage.child_environment(manifest, staged, config=_config(tmp_path), environ={})
    path = Path(env["CURIE_CONNECTOR_UPLOAD_TOKENS_FILE"])
    assert path.is_relative_to(staged.root)
    assert json.loads(path.read_text()) == tokens
    assert stat.S_IMODE(path.stat().st_mode) == 0o400
    assert path.stat().st_uid == os.getuid()
    assert all(token not in value for token in tokens.values() for value in env.values())
    assert stage.wipe(staged.root)
    assert not path.exists()


def test_no_upload_tokens_means_no_file_or_environment_override(tmp_path: Path) -> None:
    staged, manifest = _stage(
        tmp_path,
        env={"CURIE_RUNNER_TOKEN": BEARER, "CURIE_CONNECTOR_UPLOAD_TOKENS_FILE": "/untrusted"},
    )
    env = stage.child_environment(manifest, staged, config=_config(tmp_path), environ={})
    assert "CURIE_CONNECTOR_UPLOAD_TOKENS_FILE" not in env
    assert not (staged.root / "connector-upload-tokens.json").exists()


@pytest.mark.parametrize("tokens", [[], {"reviewer": 1}, {"reviewer": ""}])
def test_invalid_upload_token_payload_is_refused(tmp_path: Path, tokens: object) -> None:
    with pytest.raises(stage.StageError, match="upload tokens"):
        _stage(tmp_path, connector_upload_tokens=tokens)


def test_a_lazy_attachment_cache_starts_empty_and_writable(tmp_path: Path) -> None:
    staged, _ = _stage(tmp_path, attachments="lazy")
    assert staged.attachments is not None
    assert list(staged.attachments.iterdir()) == []
    assert os.access(staged.attachments, os.W_OK)


def test_an_oversized_or_crowded_archive_is_refused(tmp_path: Path) -> None:
    config = _config(tmp_path, CURIE_SUPERVISOR_MAX_STAGE_MEMBERS="3")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for index in range(5):
            info = tarfile.TarInfo(f"bundle/f{index}")
            tar.addfile(info, io.BytesIO(b""))
    archive = tmp_path / "many.tar"
    archive.write_bytes(buffer.getvalue())
    dest = tmp_path / "dest"
    dest.mkdir()
    with pytest.raises(stage.StageError, match="too many members"):
        stage.unpack(archive, dest, claim="claim-a", generation=1, config=config)


def test_a_link_never_replaces_a_real_directory(tmp_path: Path) -> None:
    occupied = tmp_path / "workspace"
    occupied.mkdir()
    (occupied / "file").write_text("x")
    with pytest.raises(stage.StageError):
        stage.link(occupied, tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    stage.link(link, target)
    assert link.resolve() == target.resolve()
    stage.link(link, None)
    assert not link.exists() and not link.is_symlink()


def test_a_credential_file_the_child_could_read_is_refused(tmp_path: Path) -> None:
    key = tmp_path / "server.key"
    key.write_text("key")
    other = os.getuid() + 4242
    key.chmod(0o600)
    check_private(key, child_uid=other, child_gid=other)
    check_private(key, child_uid=None, child_gid=None)
    for mode, uid, gid in (
        (0o644, other, other),
        (0o640, other, key.stat().st_gid),
        (0o600, os.getuid(), other),
    ):
        key.chmod(mode)
        with pytest.raises(ConfigError, match="readable by the runner child"):
            check_private(key, child_uid=uid, child_gid=gid)


def test_claim_environment_is_a_private_file_and_not_shell_configuration(tmp_path: Path) -> None:
    secret = "claim-value-" + "a" * 32
    staged, manifest = _stage(
        tmp_path,
        env={
            "CURIE_RUNNER_TOKEN": BEARER,
            "CURIE_CONNECTOR_AGENT": "acme-a",
            "CURIE_CONNECTOR_SECRET_KEYS": "EXTERNAL_API_TOKEN",
            "EXTERNAL_API_TOKEN": secret,
        },
    )
    env = stage.child_environment(manifest, staged, config=_config(tmp_path), environ={})
    path = Path(env["CURIE_SHELL_CLAIM_ENV_FILE"])
    assert path.is_relative_to(staged.root)
    assert stat.S_IMODE(path.stat().st_mode) == 0o400
    assert path.stat().st_uid == os.getuid()
    assert json.loads(path.read_text())["EXTERNAL_API_TOKEN"] == secret
    assert "EXTERNAL_API_TOKEN" not in env
    assert "CURIE_CONNECTOR_AGENT" not in env
    assert stage.wipe(staged.root)
    assert not path.exists()


@pytest.mark.parametrize(
    "override",
    [
        {"CURIE_SUPERVISOR_CHILD_ENV": "EXTERNAL_API_TOKEN"},
        {"CURIE_SUPERVISOR_CHILD_ENV_PUBLIC": "CURIE_CONNECTOR_AGENT"},
        {"CURIE_SUPERVISOR_CHILD_ENV_PUBLIC": "CURIE_CONNECTOR_CALLER_TOKEN"},
        {"CURIE_SUPERVISOR_CHILD_ENV_PUBLIC": "CURIE_HOSTED_CONNECTOR_ENDPOINTS"},
    ],
)
def test_agent_material_in_shell_configuration_is_refused(
    tmp_path: Path, override: dict[str, str]
) -> None:
    with pytest.raises(ConfigError, match="per claim"):
        _config(tmp_path, **override)


@pytest.mark.parametrize("value", [[], {"NAME": 1}, {"BAD=NAME": "value"}, {"NAME": "bad\0value"}])
def test_claim_environment_loader_refuses_malformed_files(tmp_path: Path, value: object) -> None:
    from curie_shell_supervisor.launch import claim_environment

    source = tmp_path / "environment.json"
    source.write_text(json.dumps(value))
    source.chmod(0o400)
    with pytest.raises(ValueError):
        claim_environment({"CURIE_SHELL_CLAIM_ENV_FILE": str(source)})


def test_claim_environment_loader_refuses_missing_or_public_files(tmp_path: Path) -> None:
    from curie_shell_supervisor.launch import claim_environment

    source = tmp_path / "environment.json"
    with pytest.raises(OSError):
        claim_environment({"CURIE_SHELL_CLAIM_ENV_FILE": str(source)})
    source.write_text("{}")
    source.chmod(0o444)
    with pytest.raises(ValueError, match="0400"):
        claim_environment({"CURIE_SHELL_CLAIM_ENV_FILE": str(source)})


def test_the_launcher_consumes_platform_credentials_before_executing_bundle_code(
    tmp_path: Path,
) -> None:
    from curie_shell_supervisor.launch import claim_environment

    caller = "caller-" + "a" * 32
    staged, manifest = _stage(
        tmp_path,
        env={
            "CURIE_RUNNER_TOKEN": BEARER,
            "CURIE_CONNECTOR_CALLER_TOKEN": caller,
        },
    )
    env = stage.child_environment(manifest, staged, config=_config(tmp_path), environ={})
    source = Path(env["CURIE_SHELL_CLAIM_ENV_FILE"])
    restored = claim_environment(env)
    assert restored["CURIE_CONNECTOR_CALLER_TOKEN"] == caller
    assert restored["CURIE_RUNNER_TOKEN"] == BEARER
    assert not source.exists(), "bundle processes must not recover platform credentials from a file"


def test_claim_environment_refuses_a_symlink_and_nonregular_file(tmp_path: Path) -> None:
    from curie_shell_supervisor.launch import claim_environment

    target = tmp_path / "target"
    target.write_text("{}")
    target.chmod(0o400)
    source = tmp_path / "bootstrap"
    source.symlink_to(target)
    with pytest.raises((OSError, ValueError)):
        claim_environment({"CURIE_SHELL_CLAIM_ENV_FILE": str(source)})
    assert target.read_text() == "{}"
    source.unlink()
    os.mkfifo(source, 0o400)
    with pytest.raises((OSError, ValueError)):
        claim_environment({"CURIE_SHELL_CLAIM_ENV_FILE": str(source)})
