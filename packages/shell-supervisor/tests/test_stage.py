"""Staging and the child environment, in process."""

from __future__ import annotations

import io
import os
import stat
import tarfile
from pathlib import Path

import pytest
from curie_shell_supervisor import stage
from curie_shell_supervisor.config import ConfigError, SupervisorConfig
from curie_test_support.static_shell import CONTROL_TOKEN, claim_archive

BEARER = "bearer-" + "a" * 40


def _config(tmp_path: Path, **overrides: object) -> SupervisorConfig:
    env = {
        "CURIE_SUPERVISOR_SLOT": "slot-a",
        "CURIE_SUPERVISOR_CONTROL_TOKEN": CONTROL_TOKEN,
        "CURIE_SUPERVISOR_TLS_CERT_FILE": "cert.pem",
        "CURIE_SUPERVISOR_TLS_KEY_FILE": "key.pem",
        "CURIE_SUPERVISOR_STATE_ROOT": str(tmp_path / "state"),
        "CURIE_SUPERVISOR_CHILD_ENV": "MODEL_KEY,CONNECTOR_A,CONNECTOR_B",
        "CURIE_SUPERVISOR_CHILD_ENV_PUBLIC": "OTEL_EXPORTER_OTLP_ENDPOINT",
        **{key: str(value) for key, value in overrides.items()},
    }
    return SupervisorConfig.from_env(env)


def _stage(tmp_path: Path, **manifest: object) -> tuple[stage.StagedClaim, stage.Manifest]:
    config = _config(tmp_path)
    body = {
        "v": 1,
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
        {"CURIE_SUPERVISOR_CHILD_UID": "0"},
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
    env = stage.child_environment(manifest, staged, config=_config(tmp_path), environ=environ)
    assert env["MODEL_KEY"] == "model-secret" and env["CONNECTOR_A"] == "a-secret"
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
    staged, manifest = _stage(
        tmp_path, executor_secret_names=["CONNECTOR_A"], withheld_env=["MODEL_KEY"]
    )
    environ = {"MODEL_KEY": "m", "CONNECTOR_A": "a", "CONNECTOR_B": "b"}
    env = stage.child_environment(manifest, staged, config=_config(tmp_path), environ=environ)
    assert env.get("CONNECTOR_A") == "a"
    assert "CONNECTOR_B" not in env and "MODEL_KEY" not in env


def test_the_bundle_is_read_only_and_home_is_private(tmp_path: Path) -> None:
    staged, _ = _stage(tmp_path)
    assert stat.S_IMODE(staged.bundle.stat().st_mode) == 0o555
    assert stat.S_IMODE((staged.bundle / "plugin.json").stat().st_mode) == 0o444
    assert stat.S_IMODE(staged.home.stat().st_mode) == 0o700
    assert stage.wipe(staged.root)
    assert not staged.root.exists()


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
