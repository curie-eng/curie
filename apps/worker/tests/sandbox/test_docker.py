"""DockerSandboxClient: the local (no-Kubernetes) SandboxClient.

The ``docker`` CLI is the one external dependency, so it is captured/stubbed; the
argv construction, the RustFS bundle fetch + B2-aligned unwrap, and the inspect
parsing (port + operating mode) are exercised for real.
"""

from __future__ import annotations

import io
import logging
import os
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from curie_worker.bundle_store import extract_bundle
from curie_worker.sandbox.docker import (
    RUNNER_CONTAINER_PORT,
    DockerError,
    DockerSandboxClient,
    RunnerHardening,
)

from .conftest import _FakeBundleStore, _flag_values, _RecordingDocker


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
    )
    argv = client.calls[0]
    joined = " ".join(argv)
    assert joined.startswith("run -d --name thread-abc")
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
    client.create_claim("thread-credentials", pool="pool", env=os.environ)

    child_env_names = {
        entry.partition("=")[0] for entry in _flag_values(client.calls[0], "-e")
    }
    assert denied_names.isdisjoint(child_env_names)
    assert "CURIE_BUDGET" in child_env_names
    assert "CURIE_CREDENTIALS" in child_env_names


def test_create_claim_preserves_declared_connector_secret() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-connector-secret",
        pool="pool",
        env={
            "CURIE_CONNECTOR_SECRET_KEYS": "SLACK_BOT_TOKEN",
            "SLACK_BOT_TOKEN": "placeholder",
        },
    )

    child_env_names = {
        entry.partition("=")[0] for entry in _flag_values(client.calls[0], "-e")
    }
    assert {"CURIE_CONNECTOR_SECRET_KEYS", "SLACK_BOT_TOKEN"} <= child_env_names


def test_create_claim_connector_marker_cannot_readmit_reserved_curie_credential() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim(
        "thread-reserved-connector-secret",
        pool="pool",
        env={
            "CURIE_CONNECTOR_SECRET_KEYS": "CURIE_SEALING_PRIVATE_KEY",
            "CURIE_SEALING_PRIVATE_KEY": "placeholder",
        },
    )

    child_env_names = {
        entry.partition("=")[0] for entry in _flag_values(client.calls[0], "-e")
    }
    assert "CURIE_CONNECTOR_SECRET_KEYS" in child_env_names
    assert "CURIE_SEALING_PRIVATE_KEY" not in child_env_names


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
    )
    assert store.requested == ["bundles/b.tar.gz"]  # the worker fetched the bundle
    mounts = _flag_values(client.calls[0], "-v")
    assert len(mounts) == 1
    host_dir, container_dir, mode = mounts[0].split(":")
    assert container_dir == "/bundles/current"
    assert mode == "ro"
    # Unwrapped: the manifest sits at the mount root, not under the wrapper dir.
    assert (Path(host_dir) / ".claude-plugin" / "plugin.json").is_file()
    child_env = _flag_values(client.calls[0], "-e")
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

        def _prepare_workspace(
            self, name: str, encoded_ref: str, expected_sha256: str
        ) -> str:
            self.prepared.append((name, encoded_ref, expected_sha256))
            return str(checkout)

    client = _WorkspacePreparingDocker(
        image="curie-runner", bundle_store=_FakeBundleStore()
    )
    client.create_claim(
        "thread-workspace",
        pool="pool",
        env={
            "CURIE_BUDGET": "{}",
            "CURIE_WORKSPACE_REF": workspace_ref,
            "CURIE_WORKSPACE_SHA256": workspace_sha256,
        },
    )

    assert client.prepared == [
        ("thread-workspace", workspace_ref, workspace_sha256)
    ]
    argv = client.calls[0]
    assert f"{checkout}:/workspace:rw" in _flag_values(argv, "-v")
    child_env = _flag_values(argv, "-e")
    child_env_names = {entry.partition("=")[0] for entry in child_env}
    assert "CURIE_WORKSPACE_REF" not in child_env_names
    assert "CURIE_WORKSPACE_SHA256" not in child_env_names
    assert all(workspace_ref not in entry for entry in child_env)
    assert all(workspace_sha256 not in entry for entry in child_env)


def test_create_claim_without_bundle_ref_mounts_nothing() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    assert _flag_values(client.calls[0], "-v") == []


def test_sdk_credential_forwarded_by_name_only() -> None:
    # A real-model run: the SDK token is present in the worker env. It must be
    # forwarded by NAME (docker reads the value), never with its value in the argv.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        environ={"CLAUDE_CODE_OAUTH_TOKEN": "sk-PLACEHOLDER-never-real"},
    )
    client.create_claim("t1", pool="pool", env={"CURIE_BUDGET": "{}"})
    argv = client.calls[0]
    envs = _flag_values(argv, "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" in envs  # forwarded by name
    assert all("PLACEHOLDER" not in a for a in argv)  # the value never leaks in


def test_sdk_credential_not_forwarded_when_absent() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore(), environ={})
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    envs = _flag_values(client.calls[0], "-e")
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
    client.create_claim("t1", pool="pool", env={"CURIE_CREDENTIALS": "sk-ant-PLACEHOLDER-secret"})
    argv = client.calls[0]
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
    client.create_claim("t1", pool="pool", env={"CURIE_BUDGET": "{}"})
    argv = client.calls[0]
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
    client.create_claim("t1", pool="pool", env={"CURIE_BUDGET": "{}"})
    argv = client.calls[0]
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
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1", "CURIE_BUDGET": "{}"})
    envs = _flag_values(client.calls[0], "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient SDK token not forwarded
    assert "ANTHROPIC_API_KEY" not in envs
    assert "CURIE_CREDENTIALS" not in envs  # BYO reference not forwarded either
    assert all("PLACEHOLDER" not in a for a in client.calls[0])  # no value leaks


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
    )
    envs = _flag_values(client.calls[0], "-e")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient SDK token not forwarded
    assert "ANTHROPIC_API_KEY" not in envs
    assert all("PLACEHOLDER" not in a for a in client.calls[0])  # no value leaks


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
    )
    envs = _flag_values(client.calls[0], "-e")
    assert "CURIE_CREDENTIALS" in envs  # BYO OpenRouter key still forwarded by name
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in envs  # ambient SDK token still suppressed
    assert "ANTHROPIC_API_KEY" not in envs
    assert all("PLACEHOLDER" not in a for a in client.calls[0])  # value never in argv


def test_get_sandbox_reports_published_port_and_mode() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {
        "inspect": "running\t{}\t2026-08-16T12:00:00.123456789Z",
        "port": "127.0.0.1:49173\n",
    }
    view = client.get_sandbox("t1")
    assert view is not None
    assert view.service_fqdn == "127.0.0.1"
    assert view.port == 49173
    assert view.operating_mode == "Running"

    client.outputs["inspect"] = "paused\t{}\t2026-08-16T12:00:00.123456789Z"
    paused = client.get_sandbox("t1")
    assert paused is not None
    assert paused.operating_mode == "Suspended"


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
    client.outputs = {"inspect": "running\t{}\t2026-08-16T12:00:00.123456789Z", "port": ""}

    view = client.get_claim("t1")
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


def test_get_sandbox_none_when_container_absent() -> None:
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    client.outputs = {"inspect": ""}  # docker inspect on a missing container
    assert client.get_sandbox("gone") is None


def test_get_sandbox_treats_dead_container_as_gone() -> None:
    # An exited/dead/created container is NOT a live sandbox: report it gone so
    # the substrate evicts the stale route instead of dialing a dead runner.
    client = _RecordingDocker(image="curie-runner", bundle_store=_FakeBundleStore())
    for dead in ("exited", "dead", "created", "restarting"):
        client.outputs = {
            "inspect": f"{dead}\t{{}}\t2026-08-16T12:00:00.123456789Z",
            "port": "",
        }
        assert client.get_sandbox("t1") is None, dead


class _NetworkAwareDocker(DockerSandboxClient):
    """Fake that distinguishes the status inspect from the networks inspect (both
    are ``docker inspect``, so keying on the subcommand alone is not enough)."""

    def __init__(self, *, networks_json: str, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._networks_json = networks_json

    def _docker(self, args: list[str], *, check: bool = True) -> str:
        if args and args[0] == "inspect":
            if any("NetworkSettings.Networks" in a for a in args):
                return self._networks_json
            return "running\t{}\t2026-08-16T12:00:00.123456789Z"
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
    view = client.get_sandbox("t1")
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
    view = client.get_sandbox("t1")
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
    )
    envs = _flag_values(client.calls[0], "-e")
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
    )
    envs = _flag_values(client.calls[0], "-e")
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
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    argv = client.calls[0]
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
    client.create_claim("t1", pool="pool", env={"CURIE_BUDGET": "{}"})
    assert all("docker.sock" not in m for m in _flag_values(client.calls[0], "-v"))


def test_hardening_disabled_emits_no_hardening_flags() -> None:
    # An explicit opt-out (CURIE_RUNNER_HARDENING=0) degrades to the
    # pre-hardening argv exactly, for debugging a bundle the rails break.
    client = _RecordingDocker(
        image="curie-runner",
        bundle_store=_FakeBundleStore(),
        hardening=RunnerHardening.from_env({"CURIE_RUNNER_HARDENING": "0"}),
    )
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    argv = client.calls[0]
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
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    argv = client.calls[0]
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
    client.create_claim("t1", pool="pool", env={"CURIE_FAKE_MODEL": "1"})
    argv = client.calls[0]
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

        def _docker(self, args: list[str], *, check: bool = True) -> str:
            self.calls.append(args)
            if args[0] == "pull":
                raise DockerError("docker pull failed (1): SENTINEL_STDERR_LEAK")
            return ""  # inspect: absent

    client = _PullFailsDocker(image="curie-runner", bundle_store=_FakeBundleStore())
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

        def _docker(self, args: list[str], *, check: bool = True) -> str:
            self.calls.append(args)
            if args[:2] == ["image", "inspect"]:
                # subprocess.run(["docker", ...]) raises this when the docker
                # binary is absent, independent of check=False.
                raise FileNotFoundError("docker")
            return ""

    client = _DockerUnavailable(image="curie-runner", bundle_store=_FakeBundleStore())
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
        client._docker(["run", "--rm", "curie-runner"])
        raise AssertionError("expected DockerError")
    except _DockerError as exc:
        assert "curie_runner" in str(exc)
        assert "curie local up" in str(exc)


def test_docker_error_without_a_matching_network_name_carries_no_hint() -> None:
    """The remediation hint is specific to OUR configured network going
    missing -- an unrelated docker failure (e.g. no such image) must not gain
    a misleading "run curie local up" tacked onto it."""
    client = DockerSandboxClient(
        image="curie-runner", bundle_store=_FakeBundleStore(), network="curie_runner"
    )
    hint = client._network_remediation_hint("Error: No such image: curie-runner:latest")
    assert hint == ""
