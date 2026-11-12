"""Hermetic contract for the single-profile acceptance driver (#5375).

The live path needs a throwaway cluster, a dedicated personal grant and an
owned forge fixture, so it is proved by running ``personal_profile.py run``.
These tests pin what must hold before and after anything live: every unpaid
refusal through the real ``preflight`` entry, the judge's refusal of
incomplete evidence, the SQLite budget ledger, native transcript parsing, the
in-pod harvester program against a recorded-shape tree, secret scanning, and
the finalizer's behavior on failure, refusal and interruption.

The two-owner scenario (#5379) command-path refusals and their authorized
controls are in the section at the end of this file.

Nothing here touches a cluster, GitHub, Docker or a network beyond a loopback
registry fixture, and no value is a real credential or identifier.
"""

from __future__ import annotations

import base64
import contextlib
import dataclasses
import datetime as dt
import hashlib
import http.server
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import personal_profile as pp
import personal_profile_support as pps
import personal_two_owner as pt
import personal_two_owner_support as pto
import pytest
import two_owner_shared as tos

SCRIPT = Path(pp.__file__).resolve()
REPO_ROOT = pp.REPO_ROOT
CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
GRANT = "synthetic-personal-grant-not-a-token"
REPO = "acme-corp/acme-fixture"
SESSION = "5d0c7f3e-0000-4000-8000-00000000a001"
TAG = pps.image_tag(CANDIDATE, "acme-run-1")
# Kubernetes Secret object names, not values.
APP_SECRET_NAME = "ppa-acme-github-app"
GRANT_SECRET_NAME = "ppa-acme-personal-grant"
RUNNER_KEY_SECRET_NAME = "acme-key"
T0 = 1_800_000_000.0


# --------------------------------------------------------------------------
# Fixtures built in the test body (no provisioning fixtures)
# --------------------------------------------------------------------------


def _private(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


SLACK_BOT = "xoxb-synthetic-gateway-bot-not-a-token"
RESOLUTION_CHANNEL = "C0EXAMPLE1"


def _slack_file(directory: Path, content: bytes = SLACK_BOT.encode() + b"\n") -> Path:
    path = directory / "slack-bot"
    path.write_bytes(content)
    os.chmod(path, 0o600)
    return path


def _grant_file(
    directory: Path, content: bytes = GRANT.encode() + b"\n", mode: int = 0o600
) -> Path:
    path = directory / "grant"
    path.write_bytes(content)
    os.chmod(path, mode)
    return path


def _app_dir(directory: Path) -> Path:
    app = _private(directory / "app")
    (app / "app.json").write_text(
        json.dumps({"id": 42, "installation_id": 7, "repo": REPO, "slug": "acme-factory"})
    )
    (app / "app.pem").write_text("placeholder, never used by preflight\n")
    return app


def _iso(seconds_from_now: float) -> str:
    moment = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=seconds_from_now)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _image(revision: str) -> tuple[bytes, bytes]:
    """A recorded-shape image: Docker manifest v2 schema 2 plus its image config blob.

    Shapes: https://distribution.github.io/distribution/spec/manifest-v2-2/ and
    the registry API at https://distribution.github.io/distribution/spec/api/ .
    """

    config = json.dumps(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {"Labels": {pps.REVISION_LABEL: revision}},
            "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "1" * 64]},
        }
    ).encode()
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "size": len(config),
                "digest": "sha256:" + hashlib.sha256(config).hexdigest(),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "size": 32,
                    "digest": "sha256:" + "2" * 64,
                }
            ],
        }
    ).encode()
    return manifest, config


def _index_image(revision: str, *, platforms: int = 1) -> tuple[bytes, bytes, dict[str, bytes]]:
    """A recorded buildkit push: an OCI index naming the image and its attestation.

    Recorded from registry:2.8.3 after ``docker build --platform linux/amd64``
    and ``docker push``: one ``linux/amd64`` image manifest plus an
    ``unknown/unknown`` manifest annotated ``attestation-manifest``. Returns the
    index, the image config blob and the child manifests by digest.
    """

    _manifest, config = _image(revision)
    config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
    image = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": len(config),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:" + "2" * 64,
                    "size": 32,
                }
            ],
        }
    ).encode()
    attestation = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:" + "3" * 64,
                "size": 167,
            },
            "layers": [],
        }
    ).encode()
    image_digest = "sha256:" + hashlib.sha256(image).hexdigest()
    attestation_digest = "sha256:" + hashlib.sha256(attestation).hexdigest()
    entries: list[dict[str, Any]] = [
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": image_digest,
            "size": len(image),
            "platform": {"architecture": arch, "os": "linux"},
        }
        for arch in ("amd64", "arm64")[:platforms]
    ]
    entries.append(
        {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": attestation_digest,
            "size": len(attestation),
            "annotations": {
                "vnd.docker.reference.digest": image_digest,
                "vnd.docker.reference.type": "attestation-manifest",
            },
            "platform": {"architecture": "unknown", "os": "unknown"},
        }
    )
    index = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": entries,
        }
    ).encode()
    return index, config, {image_digest: image, attestation_digest: attestation}


@contextlib.contextmanager
def _registry(
    images: Mapping[tuple[str, str], tuple[bytes, bytes]],
    children: Mapping[tuple[str, str], bytes] | None = None,
) -> Iterator[int]:
    """A loopback Registry HTTP API v2 fixture serving manifests and config blobs.

    ``images`` maps (repository, tag or digest) to a manifest or index plus its
    config blob; ``children`` maps (repository, digest) to manifests an index
    names. Content negotiation follows registry:2.8.3: a manifest is served only
    when Accept names its media type, and an index otherwise answers 404
    MANIFEST_UNKNOWN.
    """

    manifests = {key: value[0] for key, value in images.items()}
    manifests.update(children or {})
    blobs = {
        "sha256:" + hashlib.sha256(config).hexdigest(): config
        for _manifest, config in images.values()
    }

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802 - http.server naming
            parts = self.path.split("/")
            body: bytes | None = None
            headers: dict[str, str] = {}
            if len(parts) == 5 and parts[1] == "v2" and parts[3] == "manifests":
                accepted = [item.strip() for item in self.headers.get("Accept", "").split(",")]
                found = manifests.get((parts[2], parts[4]))
                media_type = json.loads(found).get("mediaType") if found else None
                if found and media_type in accepted:
                    body = found
                    headers = {
                        "Content-Type": str(media_type),
                        "Docker-Content-Digest": "sha256:" + hashlib.sha256(body).hexdigest(),
                    }
            elif len(parts) == 5 and parts[1] == "v2" and parts[3] == "blobs":
                body = blobs.get(parts[4])
            if body is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def test_registry_image_reads_the_revision_through_a_buildkit_index() -> None:
    index, config, children = _index_image(CANDIDATE)
    tag = CANDIDATE[:12]
    get_children = {("curie-api", digest): body for digest, body in children.items()}
    with _registry({("curie-api", tag): (index, config)}, get_children) as port:
        image = pps.registry_image(pps.loopback_get("127.0.0.1", port), "curie-api", tag)
    assert image is not None
    # The pinned and pod-reported digest is the index the tag names, not the child.
    assert image.digest == "sha256:" + hashlib.sha256(index).hexdigest()
    assert image.revision == CANDIDATE

    index, config, children = _index_image(CANDIDATE, platforms=2)
    get_children = {("curie-api", digest): body for digest, body in children.items()}
    with _registry({("curie-api", tag): (index, config)}, get_children) as port:
        assert pps.registry_image(pps.loopback_get("127.0.0.1", port), "curie-api", tag) is None


def test_docker_listed_tags_keep_tagged_rows_only() -> None:
    # Recorded `docker image ls --format '{{.Repository}}:{{.Tag}}' <repo>` output
    # after `curie build` pushed a runner layer, plus an untagged leftover row.
    listing = (
        b"localhost:5999/acme-bundle-runner:77d29ee6c43d4e87\n"
        b"localhost:5999/acme-bundle-runner:<none>\n"
    )
    assert pps.docker_listed_tags(listing) == ["localhost:5999/acme-bundle-runner:77d29ee6c43d4e87"]
    assert pps.docker_listed_tags(b"") == []


def test_registry_cleanup_finds_a_push_the_driver_never_read_back() -> None:
    index, config, children = _index_image(OTHER_SHA)
    tag = CANDIDATE[:12]
    served = {("curie-api", digest): body for digest, body in children.items()}
    with _registry({("curie-api", tag): (index, config)}, served) as port:
        targets = pp.registry_targets(port, [], [f"localhost:{port}/curie-api:{tag}"])
        absent = pp.registry_targets(port, [], [f"localhost:{port}/curie-worker:{tag}"])
    assert targets == [("curie-api", "sha256:" + hashlib.sha256(index).hexdigest())]
    assert absent == []


def _candidate_images(revision: str = CANDIDATE) -> dict[tuple[str, str], tuple[bytes, bytes]]:
    return {(name, TAG): _image(revision) for name in pps.REQUIRED_IMAGES}


def _command_argv(command: str, base: Path, port: int, **overrides: str | None) -> list[str]:
    kubeconfig = base / "kubeconfig"
    kubeconfig.write_text("apiVersion: v1\nkind: Config\n")
    values: dict[str, str | None] = {
        "--candidate": CANDIDATE,
        "--context": "kind-acme-scratch",
        "--namespace": "ppa-acme-test",
        "--release": "ppa-acme",
        "--registry": f"127.0.0.1:{port}",
        "--docker-context": "default",
        "--kubeconfig": str(kubeconfig),
        "--app-dir": str(_app_dir(base)),
        "--grant-file": str(_grant_file(base)),
        "--grant-expires-at": _iso(3 * 3600),
        "--state-dir": str(_private(base / "state")),
        "--run-id": "acme-run-1",
        "--approve-live": pps.approved_ceiling(),
    }
    if command in {"run", "rehearse"}:
        values["--slack-bot-token-file"] = str(_slack_file(base))
        values["--resolution-channel"] = RESOLUTION_CHANNEL
    values.update(overrides)
    argv = [command]
    for flag, value in values.items():
        if value is not None:
            argv += [flag, value]
    return argv


def _run_script(argv: list[str], home: Path, **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *argv],
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(home), **env},
        capture_output=True,
        text=True,
        timeout=60,
    )


def _refusal(result: subprocess.CompletedProcess[str]) -> str:
    assert result.returncode == 2, result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    payload = json.loads(result.stdout)
    assert set(payload) == {"error", "fix"}
    return str(payload["error"])


# --------------------------------------------------------------------------
# AC 1: unpaid refusals through the real preflight entry
# --------------------------------------------------------------------------


def test_preflight_refuses_service_credentials(tmp_path: Path) -> None:
    secret_value = "sk-ant-api03-synthetic-value-never-printed"
    with _registry(_candidate_images()) as port:
        cases: list[tuple[str, dict[str, str | None], dict[str, str], str]] = [
            ("api key", {}, {"ANTHROPIC_API_KEY": secret_value}, "service_credentials"),
            ("ambient grant", {}, {"CLAUDE_CODE_OAUTH_TOKEN": secret_value}, "service_credentials"),
            ("pattern", {}, {"CLAUDE_CODE_SESSION_TOKEN": secret_value}, "service_credentials"),
            (
                "gateway",
                {},
                {"CURIE_MODEL_BASE_URL": "https://gateway.example.com"},
                "service_credentials",
            ),
            ("fake mode", {}, {"CURIE_FAKE_MODEL": "1"}, "fake_mode"),
            ("no grant flag", {"--grant-file": None}, {}, "grant_missing"),
            ("absent grant", {"--grant-file": str(tmp_path / "absent")}, {}, "grant_missing"),
            ("unknown expiry", {"--grant-expires-at": "tomorrow"}, {}, "expiry_unknown"),
            ("empty expiry", {"--grant-expires-at": ""}, {}, "expiry_unknown"),
            ("naive expiry", {"--grant-expires-at": "2099-11-11T09:30:00"}, {}, "expiry_unknown"),
            ("short lifetime", {"--grant-expires-at": _iso(600)}, {}, "lifetime_insufficient"),
            ("no approval", {"--approve-live": None}, {}, "live_unapproved"),
            (
                "other approval",
                {"--approve-live": pps.approved_ceiling().replace("=8,", "=9,")},
                {},
                "live_unapproved",
            ),
            ("short candidate", {"--candidate": CANDIDATE[:12]}, {}, "candidate_invalid"),
            (
                "remote registry",
                {"--registry": "registry.example.com:5000"},
                {},
                "registry_invalid",
            ),
        ]
        for index, (label, overrides, env, expected) in enumerate(cases):
            base = _private(tmp_path / f"case-{index}")
            result = _run_script(_command_argv("preflight", base, port, **overrides), base, **env)
            assert _refusal(result) == expected, label
            assert secret_value not in result.stdout + result.stderr, label
        named = _run_script(
            _command_argv("preflight", _private(tmp_path / "named"), port),
            tmp_path,
            ANTHROPIC_API_KEY=secret_value,
        )
        assert "ANTHROPIC_API_KEY" in json.loads(named.stdout)["fix"]

        empty = _private(tmp_path / "empty")
        empty_grant = _grant_file(_private(empty / "g"), b"")
        result = _run_script(
            _command_argv("preflight", empty, port, **{"--grant-file": str(empty_grant)}), empty
        )
        assert _refusal(result) == "grant_missing"
        loose = _private(tmp_path / "loose")
        loose_grant = _grant_file(_private(loose / "g"), mode=0o644)
        result = _run_script(
            _command_argv("preflight", loose, port, **{"--grant-file": str(loose_grant)}), loose
        )
        assert _refusal(result) == "grant_unsafe"

        clean = _private(tmp_path / "clean")
        result = _run_script(_command_argv("preflight", clean, port), clean)
        assert result.returncode == 0, result.stdout + result.stderr
        payload = json.loads(result.stdout)
        assert payload["verdict"] == "pass"
        assert payload["counts"] == {"images_verified": len(pps.REQUIRED_IMAGES)}
        assert GRANT not in result.stdout + result.stderr
        ledger = clean / "state" / pps.LEDGER_FILE
        assert ledger.stat().st_mode & 0o777 == 0o600
        epoch = _private(tmp_path / "epoch")
        expires = str(int(dt.datetime.now(dt.UTC).timestamp()) + 4 * 3600)
        result = _run_script(
            _command_argv("preflight", epoch, port, **{"--grant-expires-at": expires}), epoch
        )
        assert result.returncode == 0, result.stdout

    mismatched = _candidate_images()
    mismatched[("curie-worker", TAG)] = _image(OTHER_SHA)
    with _registry(mismatched) as port:
        base = _private(tmp_path / "mismatch")
        assert _refusal(_run_script(_command_argv("preflight", base, port), base)) == (
            "candidate_image_mismatch"
        )
    missing = _candidate_images()
    del missing[(pps.LAYER_IMAGE, TAG)]
    with _registry(missing) as port:
        base = _private(tmp_path / "missing")
        assert _refusal(_run_script(_command_argv("preflight", base, port), base)) == (
            "candidate_image_mismatch"
        )


def test_preflight_reads_the_recorded_layer_ref(tmp_path: Path) -> None:
    images = _candidate_images()
    del images[(pps.LAYER_IMAGE, TAG)]
    layer_digest = "sha256:" + "a" * 64
    images[("acme-bundle-runner", layer_digest)] = _image(CANDIDATE)
    with _registry(images) as port:
        base = _private(tmp_path / "layer")
        argv = _command_argv("preflight", base, port)
        assert _refusal(_run_script(argv, base)) == "candidate_image_mismatch"
        ledger = pps.Ledger.open(base / "state", "acme-run-1")
        ledger.ensure_run(CANDIDATE, phase="images")
        ledger.record("images", "layer_ref", f"localhost:{port}/acme-bundle-runner@{layer_digest}")
        ledger.close()
        result = _run_script(argv, base)
        assert result.returncode == 0, result.stdout


def test_state_dir_must_be_private(tmp_path: Path) -> None:
    with _registry(_candidate_images()) as port:
        base = _private(tmp_path / "open")
        argv = _command_argv("preflight", base, port)
        os.chmod(base / "state", 0o755)
        assert _refusal(_run_script(argv, base)) == "state_dir_unsafe"
        os.chmod(base / "state", 0o700)
        assert _run_script(argv, base).returncode == 0


def test_approved_ceiling_is_the_maintainer_string() -> None:
    assert pps.approved_ceiling() == (
        "sessions=8,turns=96,session-turns=32,output-tokens=32768,"
        "session-output-tokens=8192,session-seconds=300,live-seconds=2400,"
        "cleanup-seconds=300,paid-retries=0"
    )
    assert pps.GRANT_MIN_LIFETIME == 2700
    assert len(pps.ATTEMPT_ROLES) * 2 == pps.MAX_SESSIONS


# --------------------------------------------------------------------------
# Native transcripts in the recorded shapes
# --------------------------------------------------------------------------


def _stamp(offset: float) -> str:
    return dt.datetime.fromtimestamp(T0 + offset, dt.UTC).isoformat().replace("+00:00", "Z")


def _assistant(
    message_id: str, model: str, tokens: int | None, at: float, content: list[Any], **extra: Any
) -> dict[str, Any]:
    usage = {} if tokens is None else {"input_tokens": 10, "output_tokens": tokens}
    return {
        "type": "assistant",
        "uuid": f"u-{message_id}-{at}",
        "timestamp": _stamp(at),
        "sessionId": SESSION,
        "message": {
            "id": message_id,
            "model": model,
            "role": "assistant",
            "content": content,
            "usage": usage,
        },
        **extra,
    }


def _jsonl(records: list[dict[str, Any]]) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def _transcripts(
    *,
    implementer_model: str = "claude-sonnet-5-5-20261001",
    reviewer_model: str = "claude-opus-5-5[1m]",
    reviewers: tuple[str, ...] = (pps.PLAN_REVIEWER, pps.DIFF_REVIEWER),
    synthetic: bool = False,
    config: str = "curie-claude-config-a1b2",
) -> dict[str, bytes]:
    """Native CLI transcripts as observed in the #5531/#5422 live proofs.

    Main session: ``<config>/projects/<slug>/<session>.jsonl``. Subagents:
    ``<config>/projects/<slug>/<session>/subagents/agent-<id>.jsonl`` with an
    ``agent-<id>.meta.json`` sidecar (claude_agent_sdk/_internal/sessions.py).
    """

    project = f"{config}/projects/-workspace"
    main: list[dict[str, Any]] = [
        {
            "type": "user",
            "timestamp": _stamp(0),
            "sessionId": SESSION,
            "isSidechain": False,
            "message": {
                "role": "user",
                "content": "https://github.com/acme-corp/acme-fixture/issues/7",
            },
        },
        _assistant(
            "msg_01",
            implementer_model,
            120,
            2,
            [{"type": "text", "text": "Reading."}],
            isSidechain=False,
        ),
        _assistant(
            "msg_01",
            implementer_model,
            180,
            3,
            [
                {
                    "type": "tool_use",
                    "id": "toolu_issue",
                    "name": "mcp__curie__get_issue",
                    "input": {},
                }
            ],
            isSidechain=False,
        ),
    ]
    files: dict[str, bytes] = {}
    offset = 20.0
    for role in reviewers:
        tool = f"toolu_{role}"
        agent = f"a{role[:4]}01"
        main.append(
            _assistant(
                f"msg_{role}",
                implementer_model,
                60,
                offset - 2,
                [
                    {
                        "type": "tool_use",
                        "id": tool,
                        "name": "Agent",
                        "input": {"subagent_type": f"dark-factory:{role}", "prompt": "review"},
                    }
                ],
                isSidechain=False,
            )
        )
        records = [
            _assistant(
                f"msg_{role}_1",
                reviewer_model,
                400,
                offset,
                [{"type": "text", "text": "VERDICT: APPROVE"}],
                isSidechain=True,
                agentId=agent,
            ),
            _assistant(
                f"msg_{role}_2",
                reviewer_model,
                300,
                offset + 60,
                [{"type": "text", "text": "done"}],
                isSidechain=True,
                agentId=agent,
            ),
        ]
        files[f"{project}/{SESSION}/subagents/agent-{agent}.jsonl"] = _jsonl(records)
        if role == pps.PLAN_REVIEWER:
            files[f"{project}/{SESSION}/subagents/agent-{agent}.meta.json"] = json.dumps(
                {"toolUseId": tool}
            ).encode()
        main.append(
            {
                "type": "user",
                "timestamp": _stamp(offset + 62),
                "sessionId": SESSION,
                "isSidechain": False,
                "toolUseResult": {"agentId": agent, "status": "completed"},
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": tool, "content": "VERDICT: APPROVE"}
                    ],
                },
            }
        )
        offset += 80
    if synthetic:
        main.append(
            _assistant(
                "msg_synth",
                "<synthetic>",
                None,
                offset,
                [{"type": "text", "text": "Failed to authenticate"}],
                isSidechain=False,
            )
        )
    main.append(
        _assistant(
            "msg_99",
            implementer_model,
            90,
            offset + 5,
            [{"type": "text", "text": "Published."}],
            isSidechain=False,
        )
    )
    files[f"{project}/{SESSION}.jsonl"] = _jsonl(main)
    return files


def test_native_transcripts_attribute_roles_models_and_usage() -> None:
    facts = pps.parse_native_transcripts(_transcripts())
    by_role = {session.role: session for session in facts.sessions}
    assert set(by_role) == {pps.IMPLEMENTER, pps.PLAN_REVIEWER, pps.DIFF_REVIEWER}
    implementer = by_role[pps.IMPLEMENTER]
    assert implementer.assistant_turns == 4  # msg_01 is one turn across two records
    assert implementer.output_tokens == 180 + 60 + 60 + 90
    assert implementer.models == ("claude-sonnet-5-5-20261001",)
    assert implementer.aci_tool_calls == 1
    total = implementer.last - implementer.first if implementer.last and implementer.first else 0
    assert implementer.elapsed == pytest.approx(total - 2 * 60)
    for role in pps.REVIEWER_ROLES:
        assert by_role[role].assistant_turns == 2
        assert by_role[role].output_tokens == 700
        assert all(pps.same_model(model, pps.REVIEWER_MODEL) for model in by_role[role].models)
    assert facts.synthetic_records == 0
    assert all(session.usage_known for session in facts.sessions)

    unattributed = _transcripts()
    meta = next(name for name in unattributed if name.endswith(".meta.json"))
    del unattributed[meta]
    main = next(name for name in unattributed if name.endswith(f"{SESSION}.jsonl"))
    unattributed[main] = unattributed[main].replace(
        b'"dark-factory:plan-reviewer"', b'"general-purpose"'
    )
    roles = sorted(s.role for s in pps.parse_native_transcripts(unattributed).sessions)
    assert roles == sorted([pps.IMPLEMENTER, pps.DIFF_REVIEWER, pps.UNATTRIBUTED])

    synthetic = pps.parse_native_transcripts(_transcripts(synthetic=True))
    assert synthetic.synthetic_records == 1

    replay = pps.parse_native_transcripts(
        _transcripts(), exclude_ids=frozenset({"msg_01"}), not_before=T0 + 10
    )
    replayed = {session.role: session for session in replay.sessions}
    assert replayed[pps.IMPLEMENTER].assistant_turns == 3
    assert replayed[pps.IMPLEMENTER].aci_tool_calls == 0
    assert replay.replayed_records == 3
    assert facts.message_ids() >= {"msg_01", "msg_99"}


# --------------------------------------------------------------------------
# Ledger construction through the support API, and the judge
# --------------------------------------------------------------------------


def _owner(**changes: Any) -> dict[str, Any]:
    owner = {
        "tenant_id": pps.DEFAULT_TENANT,
        "mode": pps.PERSONAL_MODE,
        "principal_id": "8a1d2c3b-0000-4000-8000-0000000000aa",
        "generation": 1,
    }
    owner.update(changes)
    return owner


def _build_ledger(
    state: Path,
    run_id: str,
    *,
    transcripts: dict[str, bytes] | None = None,
    owner_api: dict[str, Any] | None = None,
    owner_cli: dict[str, Any] | None = None,
    approval_overrides: Mapping[str, Any] | None = None,
    inventory_status: str = "absent",
    failed_step: bool = False,
    published_paths: Sequence[str] = ("NONCE.md",),
    budget_overrides: Mapping[str, Any] | None = None,
) -> None:
    ledger = pps.Ledger.open(state, run_id)
    ledger.ensure_run(CANDIDATE, phase="run", now=T0)
    ledger.begin_live(now=T0, grant_expires_at=T0 + 4000)
    first = pps.parse_native_transcripts(transcripts or _transcripts())
    second = pps.parse_native_transcripts(_transcripts(config="curie-claude-config-c3d4"))
    for number, facts in ((1, first), (2, second)):
        reservations = ledger.reserve(pps.ATTEMPT_ROLES, attempt=number, now=T0 + number * 600)
        pps.settle_attempt(ledger, reservations, facts, complete=True)
    ledger.end_live(now=T0 + 1900)
    roles = {
        "sessions_initial": [pps.session_summary(s) for s in first.sessions],
        "sessions_resumed": [pps.session_summary(s) for s in second.sessions],
        "synthetic_records": first.synthetic_records + second.synthetic_records,
        "owner_seeded": _owner(),
        "owner_api": owner_api or _owner(),
        "owner_cli": owner_cli or _owner(),
        "request_owners": [_owner(), _owner()],
        "bundle_hook_observed": True,
        "aci_tool_calls": first.aci_tool_calls,
        "service_credentials_absent": True,
        "candidate_images_running": True,
        "runner_image_attempt_1": {
            "from_run_registry": True,
            "digest_matches": True,
            "revision_matches": True,
        },
        "runner_image_attempt_2": {
            "from_run_registry": True,
            "digest_matches": True,
            "revision_matches": True,
        },
        "active_profile_processes_max": 1,
    }
    approval: dict[str, Any] = {
        "approval_parked": True,
        "unlisted_status": 403,
        "pending_after_unlisted": True,
        "unlisted_audit_denied": True,
        "work_item_unchanged_after_unlisted": True,
        "execution_requests_unchanged_after_unlisted": True,
        "publication_jobs_after_unlisted": 0,
        "remote_branches_after_unlisted": 0,
        "owner_unchanged_after_unlisted": True,
        "subject_digests": [pps.ref_digest(s) for s in ("owner-x", "U0ACMEALLOW", "U0ACMEOTHER")],
        "reject_resolved": True,
        "reject_audit_operator": True,
        "approve_resolved": True,
        "approve_audit_operator": True,
        "approval_digests": [pps.ref_digest("approval-1"), pps.ref_digest("approval-2")],
        "owner_unchanged_after_resume": True,
        "work_item_unchanged_after_resume": True,
        "same_grant": True,
    }
    for key, value in (approval_overrides or {}).items():
        if value is None:
            approval.pop(key)
        else:
            approval[key] = value
    nonce_hash = hashlib.sha256(b"acceptance nonce: 0f0f\n").hexdigest()
    restart = {
        "old_pod_absent": True,
        "old_process_stream_ended": True,
        "new_pod_uid_differs": True,
        "new_process_after_deletion": True,
        "owner_generation_unchanged": True,
        "approval_pending_after_deletion": True,
        "no_publication_during_restart": True,
        "reviewed_pod_absent_at_park": True,
        "reviewed_workspace": {
            "head": "1" * 40,
            "status": [["??", "NONCE.md"]],
            "hashes": {"NONCE.md": nonce_hash},
            "nonce_paths": ["NONCE.md"],
        },
        "published_workspace": {
            "hashes": {"NONCE.md": nonce_hash},
            "nonce_paths": ["NONCE.md"],
            "changed_paths": list(published_paths),
            "changed_paths_complete": True,
        },
    }
    publication = {
        "rejected_remote_branches": 0,
        "rejected_prs": 0,
        "rejection_stays_rejected": True,
        "prs_for_run": 1,
        "pr_draft": False,
        "pr_head_is_publication_branch": True,
        "publication_status": "succeeded",
        "work_item_state": "published",
        "diff_review_approved": True,
        "head_sha": "2" * 40,
        "tree_sha": "3" * 40,
        "head_checks": pps.judge_head_checks(
            {"total_count": 1, "check_runs": [{"status": "completed", "conclusion": "success"}]},
            {"state": "success", "statuses": []},
        ),
        "owner_unchanged": True,
    }
    for criterion, values in (
        ("roles", roles),
        ("approval", approval),
        ("pod-restart", restart),
        ("publication", publication),
    ):
        for key, value in values.items():
            ledger.record(criterion, key, value)
    ledger.record("budget", "cleanup_elapsed", 41.5)
    budget = {"paid_starts_fenced": True, "allowance_breach": [], **(budget_overrides or {})}
    for key, value in budget.items():
        ledger.record("budget", key, value)
    for name in ("delete-namespace", "helm-uninstall", "delete-grant-secret"):
        ledger.record_cleanup(f"step:{name}", name, "failed" if failed_step else "ok")
    for kind in pps.INVENTORY_KINDS:
        status = inventory_status if kind == "namespace" else "absent"
        ledger.record_cleanup(f"inventory:{kind}", kind, status)
    ledger.record(
        "cleanup",
        "token_scan",
        {
            channel: {"scanned_bytes": 10, "token_bytes_found": False}
            for channel in pps.SCAN_CHANNELS
        },
    )
    ledger.record("cleanup", "foreign_preserved", True)
    ledger.close()


def _judge(
    state: Path,
    run_id: str,
    criterion: str,
    capsys: pytest.CaptureFixture[str],
    candidate: str = CANDIDATE,
) -> tuple[int, dict[str, Any]]:
    capsys.readouterr()
    code = pp.main(
        [
            "judge",
            "--criterion",
            criterion,
            "--state-dir",
            str(state),
            "--run-id",
            run_id,
            "--candidate",
            candidate,
        ],
        environ={"PATH": "/usr/bin:/bin"},
    )
    return code, json.loads(capsys.readouterr().out)


VIOLATIONS: dict[str, tuple[dict[str, Any], str, str]] = {
    "absent_reviewer_evidence": (
        {"transcripts": _transcripts(reviewers=(pps.PLAN_REVIEWER,))},
        "roles",
        "diff_reviewer_session",
    ),
    "synthetic_assistant_record": (
        {"transcripts": _transcripts(synthetic=True)},
        "roles",
        "no_synthetic_records",
    ),
    "wrong_implementer_model": (
        {"transcripts": _transcripts(implementer_model="claude-haiku-5-0")},
        "roles",
        "implementer_model",
    ),
    "wrong_reviewer_model": (
        {"transcripts": _transcripts(reviewer_model="claude-sonnet-5-5")},
        "roles",
        "reviewer_model",
    ),
    "wrong_owner": (
        {"owner_api": _owner(principal_id="8a1d2c3b-0000-4000-8000-0000000000bb")},
        "roles",
        "owner_api_matches",
    ),
    "wrong_generation": ({"owner_cli": _owner(generation=2)}, "roles", "owner_cli_matches"),
    "failed_control_unlisted_403_missing": (
        {"approval_overrides": {"unlisted_status": None}},
        "approval",
        "unlisted_refused_403",
    ),
    "failed_control_unlisted_succeeded": (
        {"approval_overrides": {"unlisted_status": 200}},
        "approval",
        "unlisted_refused_403",
    ),
    "extra_published_path": (
        {"published_paths": ("NONCE.md", "unitconv/core.py")},
        "pod-restart",
        "workspace_paths_equal",
    ),
    "unfenced_paid_starts": (
        {"budget_overrides": {"paid_starts_fenced": False}},
        "budget",
        "paid_starts_fenced",
    ),
    "allowance_breach_recorded": (
        {"budget_overrides": {"allowance_breach": ["output_tokens:implementer"]}},
        "budget",
        "no_allowance_breach",
    ),
    "incomplete_teardown_residual": (
        {"inventory_status": "present"},
        "cleanup",
        "inventory_absent",
    ),
    "incomplete_teardown_uncertain": (
        {"inventory_status": "uncertain"},
        "cleanup",
        "inventory_absent",
    ),
    "incomplete_teardown_failed_step": ({"failed_step": True}, "cleanup", "steps_ok"),
}


@pytest.mark.parametrize("violation", [*VIOLATIONS, "none"])
def test_judge_refuses_incomplete_evidence(
    violation: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    run_id = f"judge-{violation}".replace("_", "-")[:60]
    if violation == "none":
        _build_ledger(state, run_id)
        for criterion in (*pps.CRITERIA, "all"):
            code, result = _judge(state, run_id, criterion, capsys)
            assert code == 0, (criterion, [k for k, v in result["checks"].items() if not v])
            assert result["verdict"] == "pass"
        code, _ = _judge(state, run_id, "all", capsys, candidate=OTHER_SHA)
        assert code == 2
        return
    options, criterion, check = VIOLATIONS[violation]
    _build_ledger(state, run_id, **options)
    code, result = _judge(state, run_id, criterion, capsys)
    assert code == 1
    assert result["verdict"] == "fail"
    assert result["checks"][check] is False
    assert all(isinstance(value, bool) for value in result["checks"].values())
    code, everything = _judge(state, run_id, "all", capsys)
    assert code == 1 and everything["checks"][f"{criterion}.{check}"] is False
    text = json.dumps(everything)
    assert pps.DEFAULT_TENANT not in text and "8a1d2c3b" not in text


def test_judge_fails_every_criterion_on_an_empty_ledger(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = _private(tmp_path / "state")
    ledger = pps.Ledger.open(state, "empty-run")
    ledger.ensure_run(CANDIDATE, phase="run")
    ledger.close()
    for criterion in (*pps.CRITERIA, "all"):
        code, result = _judge(state, "empty-run", criterion, capsys)
        assert code == 1 and result["verdict"] == "fail", criterion
    capsys.readouterr()
    code = pp.main(
        [
            "judge",
            "--criterion",
            "all",
            "--state-dir",
            str(state),
            "--run-id",
            "never-ran",
            "--candidate",
            CANDIDATE,
        ],
        environ={},
    )
    assert code == 2
    assert json.loads(capsys.readouterr().out)["error"] == "ledger_missing"


# --------------------------------------------------------------------------
# AC 6: the budget ledger
# --------------------------------------------------------------------------


def _live_ledger(
    tmp_path: Path, run_id: str = "budget-run", expires: float = T0 + 4000
) -> pps.Ledger:
    ledger = pps.Ledger.open(_private(tmp_path / "state"), run_id)
    ledger.ensure_run(CANDIDATE, phase="run", now=T0)
    ledger.begin_live(now=T0, grant_expires_at=expires)
    return ledger


def _refused(call: Callable[[], Any]) -> str:
    with pytest.raises(pps.Refusal) as caught:
        call()
    return caught.value.code


def test_budget_unknown_usage_blocks_the_next_reservation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ledger = _live_ledger(tmp_path)
    first = ledger.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0 + 10)
    assert [item.output_tokens for item in first] == [8192] * 4
    assert [item.turns for item in first] == [24] * 4
    harvest = pps.HarvestState(pps.TokenScan(pps.Scrubber()))
    harvest.feed(
        json.dumps(
            {
                "kind": "file",
                "path": "x/projects/a.jsonl",
                "offset": 64,
                "data": base64.b64encode(b"{}\n").decode(),
            }
        ).encode()
    )
    assert not harvest.complete
    counts = pps.settle_attempt(
        ledger, first, pps.parse_native_transcripts(harvest.transcript_files()), complete=False
    )
    assert counts["unknown"] == 4
    assert _refused(lambda: ledger.reserve([pps.IMPLEMENTER], attempt=2, now=T0 + 20)) == (
        "usage_unknown"
    )
    ledger.close()
    code, result = _judge(tmp_path / "state", "budget-run", "budget", capsys)
    assert code == 1 and result["checks"]["no_unknown_usage"] is False

    known = _live_ledger(tmp_path / "known", run_id="known-run")
    reservations = known.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0 + 10)
    pps.settle_attempt(
        known, reservations, pps.parse_native_transcripts(_transcripts()), complete=True
    )
    second = known.reserve(pps.ATTEMPT_ROLES, attempt=2, now=T0 + 600)
    assert len(second) == 4
    sessions = known.sessions()
    assert all(item["output_tokens"] is not None for item in sessions[:4])
    assert all(item["output_tokens"] is None for item in sessions[4:])
    known.close()


def test_budget_caps_refuse_past_any_allowance(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    assert _refused(lambda: ledger.reserve(["implementer"] * 9, attempt=1, now=T0)) == (
        "allowance_exhausted"
    )
    batch = ledger.reserve(["implementer"] * 8, attempt=1, now=T0 + 1)
    for item in batch:
        ledger.settle(
            item.seq, pps.SessionUsage(10, 1, 5.0, pps.IMPLEMENTER_MODEL), source="native"
        )
    assert _refused(lambda: ledger.reserve([pps.IMPLEMENTER], attempt=2, now=T0 + 2)) == (
        "allowance_exhausted"
    )
    ledger.close()

    tokens = _live_ledger(tmp_path / "tokens", run_id="tokens-run")
    first = tokens.reserve([pps.IMPLEMENTER] * 4, attempt=1, now=T0 + 1)
    for item in first:
        tokens.settle(
            item.seq, pps.SessionUsage(8192, 24, 200.0, pps.IMPLEMENTER_MODEL), source="native"
        )
    assert tokens.budget_state() == "ok"
    assert _refused(lambda: tokens.reserve([pps.IMPLEMENTER], attempt=2, now=T0 + 2)) == (
        "allowance_exhausted"
    )
    tokens.close()

    session = _live_ledger(tmp_path / "session", run_id="session-run")
    over = session.reserve([pps.IMPLEMENTER], attempt=1, now=T0 + 1)
    session.settle(
        over[0].seq, pps.SessionUsage(8193, 3, 20.0, pps.IMPLEMENTER_MODEL), source="native"
    )
    assert session.budget_state() == "exceeded"
    assert _refused(lambda: session.reserve([pps.IMPLEMENTER], attempt=2, now=T0 + 2)) == (
        "budget_exceeded"
    )
    session.close()

    slow = _live_ledger(tmp_path / "slow", run_id="slow-run")
    item = slow.reserve([pps.DIFF_REVIEWER], attempt=1, now=T0 + 1)[0]
    slow.settle(item.seq, pps.SessionUsage(100, 3, 301.0, pps.REVIEWER_MODEL), source="native")
    assert slow.budget_state() == "exceeded"
    slow.close()

    unreserved = _live_ledger(tmp_path / "extra", run_id="extra-run")
    slots = unreserved.reserve([pps.IMPLEMENTER], attempt=1, now=T0 + 1)
    counts = pps.settle_attempt(
        unreserved, slots, pps.parse_native_transcripts(_transcripts()), complete=True
    )
    assert counts["unreserved"] == 2
    assert unreserved.budget_state() == "exceeded"
    unreserved.close()


def test_a_stricter_output_token_ceiling_holds_both_attempts(tmp_path: Path) -> None:
    assert pps.require_output_token_ceiling(None) is None
    assert pps.require_output_token_ceiling("") is None
    assert pps.require_output_token_ceiling("28302") == 28302
    for raw in ("0", "-1", "abc", str(pps.MAX_OUTPUT_TOKENS + 1)):
        with pytest.raises(pps.Refusal):
            pps.require_output_token_ceiling(raw)
    ledger = _live_ledger(tmp_path)
    assert ledger.output_token_ceiling() == pps.MAX_OUTPUT_TOKENS
    ledger.set_output_token_ceiling(28302)
    ledger.set_output_token_ceiling(28302)
    assert _refused(lambda: ledger.set_output_token_ceiling(30000)) == "arguments_invalid"
    first = ledger.reserve(list(pps.ATTEMPT_ROLES), attempt=1, now=T0 + 1)
    assert [item.output_tokens for item in first] == [28302 // 4] * 4
    # The first live run's shape: three sessions used, one diff slot unused.
    for item, used in zip(first, (2584, 1282, 600, 0), strict=True):
        ledger.settle(
            item.seq, pps.SessionUsage(used, 3, 20.0, pps.REVIEWER_MODEL), source="native"
        )
    second = ledger.reserve(list(pps.ATTEMPT_ROLES), attempt=2, now=T0 + 2)
    assert [item.output_tokens for item in second] == [(28302 - 4466) // 4] * 4
    for item in second:
        ledger.settle(
            item.seq, pps.SessionUsage(5959, 3, 20.0, pps.REVIEWER_MODEL), source="native"
        )
    # 4466 + 4 * 5959 = 28302: at the ceiling, not over it.
    assert ledger.budget_state() == "ok"
    ledger.close()
    over = _live_ledger(tmp_path / "over", run_id="over-run")
    over.set_output_token_ceiling(1000)
    item = over.reserve([pps.IMPLEMENTER], attempt=1, now=T0 + 1)[0]
    assert item.output_tokens == 1000
    over.settle(item.seq, pps.SessionUsage(1001, 3, 20.0, pps.IMPLEMENTER_MODEL), source="native")
    # Over the run's ceiling though far under the approved maximum.
    assert over.budget_state() == "exceeded"
    checks, _ = pps._judge_budget(over)
    assert checks["total_output_tokens_within"] is False
    over.close()
    late = _live_ledger(tmp_path / "late", run_id="late-run")
    late.reserve([pps.IMPLEMENTER], attempt=1, now=T0 + 1)
    assert _refused(lambda: late.set_output_token_ceiling(28302)) == "arguments_invalid"
    late.close()


def test_budget_live_window_and_grant_lifetime_refuse(tmp_path: Path) -> None:
    ledger = _live_ledger(tmp_path)
    late = T0 + pps.LIVE_SECONDS - pps.SESSION_SECONDS + 1
    assert _refused(lambda: ledger.reserve([pps.IMPLEMENTER], attempt=1, now=late)) == (
        "allowance_exhausted"
    )
    assert len(ledger.reserve([pps.IMPLEMENTER], attempt=1, now=late - 2)) == 1
    ledger.close()
    short = _live_ledger(tmp_path / "short", run_id="short-run", expires=T0 + 1000)
    assert _refused(lambda: short.reserve([pps.IMPLEMENTER], attempt=1, now=T0 + 10)) == (
        "lifetime_insufficient"
    )
    assert short.sessions() == []
    short.close()


def test_ledger_refuses_paid_retry_through_the_run_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base = _private(tmp_path / "retry")
    argv = _command_argv("run", base, 5999)
    ledger = _live_ledger(base, run_id="acme-run-1")
    ledger.reserve([pps.IMPLEMENTER], attempt=1, now=T0 + 5)
    ledger.close()
    empty_path = _private(tmp_path / "no-tools")
    capsys.readouterr()
    code = pp.main(argv, environ={"PATH": str(empty_path), "HOME": str(tmp_path)})
    assert code == 2
    assert json.loads(capsys.readouterr().out)["error"] == "paid_retry_refused"
    check = pps.Ledger.open(base / "state", "acme-run-1")
    assert len(check.sessions()) == 1
    assert check.observations("budget")["retry_refusals"] == 1
    check.close()

    fresh = _private(tmp_path / "fresh")
    capsys.readouterr()
    code = pp.main(
        _command_argv("run", fresh, 5999), environ={"PATH": str(empty_path), "HOME": str(tmp_path)}
    )
    result = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 1
    assert result["phase"] == "refusals" and result["error"] == "step_failed"
    after = pps.Ledger.open(fresh / "state", "acme-run-1")
    assert after.sessions() == [] and after.run_row()["live_started_at"] is None
    after.close()


# --------------------------------------------------------------------------
# The in-pod harvester against a recorded-shape tree, and secret scanning
# --------------------------------------------------------------------------


def _fake_pod(root: Path, nonce: str) -> dict[str, Any]:
    tmp = root / "tmp"
    transcripts = _transcripts()
    for name, data in transcripts.items():
        path = tmp / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    (tmp / "curie-claude-config-a1b2" / ".credentials.json").write_text(
        json.dumps({"claudeAiOauth": {"accessToken": GRANT}})
    )
    review = tmp / "dark-factory-review"
    review.mkdir(parents=True)
    # examples/dark-factory/hooks/review_gate.py::_fresh state shape after both reviews.
    (review / f"{SESSION}.json").write_text(
        json.dumps(
            {
                "plan": {"round": 1, "verdict": "APPROVE"},
                "diff": {"round": 1, "verdict": "APPROVE"},
                "stopped": None,
                "seq": 9,
            }
        )
    )
    (review / f"{SESSION}.tmp").write_text("partial")
    workspace = root / "workspace"
    workspace.mkdir()
    (workspace / "README.md").write_text("acme fixture\n")
    (workspace / "NONCE.md").write_text(f"acceptance nonce: {nonce}\n")
    (workspace / "new.txt").write_text("renamed\n")
    git = root / "gitbin" / "git"
    git.parent.mkdir()
    # Output shapes from git-status(1) "Porcelain Format Version 1" with -z:
    # NUL-terminated "XY path" entries; a rename carries the origin as the next field.
    git.write_text(
        "#!/bin/sh\n"
        'case "$*" in\n'
        "  *'rev-parse HEAD'*) printf '%s\\n' 1111111111111111111111111111111111111111 ;;\n"
        "  *'status --porcelain=v1 -z'*) printf ' M README.md\\000?? NONCE.md\\000"
        "R  new.txt\\000old.txt\\000 D gone.txt\\000' ;;\n"
        "  *) exit 1 ;;\n"
        "esac\n"
    )
    git.chmod(0o755)
    proc = root / "proc"
    (proc / "101").mkdir(parents=True)
    (proc / "102").mkdir()
    (proc / "self").mkdir()
    (proc / "stat").write_text("cpu  1 2 3 4\nbtime 1700000000\nprocesses 99\n")
    (proc / "101" / "cmdline").write_bytes(
        b"node\0/usr/lib/node_modules/@anthropic-ai/claude-code/cli.js\0--print\0"
    )
    (proc / "101" / "stat").write_text("101 (node) S " + " ".join(["0"] * 18) + " 12345 0 0\n")
    (proc / "101" / "environ").write_bytes(f"CLAUDE_CODE_OAUTH_TOKEN={GRANT}\0".encode())
    (proc / "102" / "cmdline").write_bytes(b"python3\0-m\0curie_runner\0")
    (proc / "102" / "stat").write_text("102 (python3) S " + " ".join(["0"] * 18) + " 5 0 0\n")
    return transcripts


def test_harvester_program_streams_recorded_shapes(tmp_path: Path) -> None:
    nonce = "0f0f1e1e2d2d3c3c"
    transcripts = _fake_pod(tmp_path, nonce)
    config = pps.harvest_config(
        nonce=nonce,
        tmp_root=str(tmp_path / "tmp"),
        review_dir=str(tmp_path / "tmp" / "dark-factory-review"),
        workspace=str(tmp_path / "workspace"),
        proc_root=str(tmp_path / "proc"),
        git=str(tmp_path / "gitbin" / "git"),
        once=True,
    )
    result = subprocess.run(
        [sys.executable, "-c", pps.HARVEST_PROGRAM, config],
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert GRANT.encode() not in result.stdout
    scrubber = pps.Scrubber()
    held = scrubber.hold(pps.HeldSecret(GRANT))
    held.clear()
    scan = pps.TokenScan(scrubber)
    harvest = pps.HarvestState(scan)
    for line in result.stdout.splitlines():
        harvest.feed(line)
    assert harvest.ended and harvest.complete
    assert scan.as_json()["harvest"]["token_bytes_found"] is False
    assert harvest.transcript_files() == transcripts
    verdicts = harvest.review_verdicts()
    assert verdicts["hook_observed"] and verdicts["diff_approved"] and verdicts["state_files"] == 1
    record = pps.workspace_record(harvest.workspace)
    assert record is not None
    digest = hashlib.sha256(f"acceptance nonce: {nonce}\n".encode()).hexdigest()
    assert record["head"] == "1" * 40
    assert record["hashes"] == {
        "README.md": hashlib.sha256(b"acme fixture\n").hexdigest(),
        "NONCE.md": digest,
        "new.txt": hashlib.sha256(b"renamed\n").hexdigest(),
        "gone.txt": None,
    }
    assert record["nonce_paths"] == ["NONCE.md"]
    assert harvest.process_max == 1
    ticks = os.sysconf("SC_CLK_TCK")
    assert harvest.process_start() == pytest.approx(1_700_000_000 + 12345 / ticks, abs=0.01)
    facts = pps.parse_native_transcripts(harvest.transcript_files())
    assert {s.role for s in facts.sessions} == {pps.IMPLEMENTER, *pps.REVIEWER_ROLES}


def _file_record(path: str, offset: int, data: bytes) -> bytes:
    return json.dumps(
        {"kind": "file", "path": path, "offset": offset, "data": base64.b64encode(data).decode()}
    ).encode()


def _sync_record(sizes: Mapping[str, int], at: float = T0) -> bytes:
    return json.dumps({"kind": "sync", "sizes": dict(sizes), "at": at}).encode()


def test_a_secret_split_across_harvest_chunks_is_found() -> None:
    scrubber = pps.Scrubber()
    scrubber.hold(pps.HeldSecret(GRANT)).clear()
    scan = pps.TokenScan(scrubber)
    harvest = pps.HarvestState(scan)
    first, second = b"prefix " + GRANT.encode()[:9], GRANT.encode()[9:] + b" suffix\n"
    other = pps.TokenScan(scrubber)
    assert not other.scan("harvest", first) and not other.scan("harvest", second)
    harvest.feed(_file_record("c/projects/s.jsonl", 0, first))
    assert scan.as_json()["harvest"]["token_bytes_found"] is False
    harvest.feed(_file_record("c/projects/s.jsonl", len(first), second))
    assert scan.as_json()["harvest"]["token_bytes_found"] is True


def test_harvest_is_complete_only_after_a_matching_size_report(tmp_path: Path) -> None:
    path = "c/projects/s.jsonl"
    data = _transcripts()
    empty = pps.HarvestState(pps.TokenScan(pps.Scrubber()))
    assert not empty.complete  # a failed exec or an empty EOF reports nothing
    harvest = pps.HarvestState(pps.TokenScan(pps.Scrubber()))
    harvest.feed(_file_record(path, 0, b"{}\n"))
    assert not harvest.complete
    harvest.feed(_sync_record({path: 3}))
    assert harvest.complete
    harvest.feed(_file_record(path, 3, b"{}\n"))  # bytes after the last report
    assert not harvest.complete
    harvest.feed(_sync_record({path: 6}, at=T0 + 5))
    assert harvest.complete
    # Terminal only when the matching report was taken after native execution ended
    # (the approval's creation): a synchronized prefix taken earlier proves nothing.
    assert harvest.terminal_after(T0 + 5) and not harvest.terminal_after(T0 + 6)
    assert pps.iso_epoch("2026-11-11T04:00:00.250000+00:00") == pytest.approx(
        dt.datetime(2026, 11, 11, 4, 0, 0, 250000, tzinfo=dt.UTC).timestamp()
    )
    assert pps.iso_epoch("2026-11-11T04:00:00") == pps.iso_epoch("2026-11-11T04:00:00Z")
    assert pps.iso_epoch(None) == pps.iso_epoch("not a time") == 0.0
    user_only = {
        "c/projects/p/s.jsonl": json.dumps(
            {"type": "user", "timestamp": "2026-11-11T04:00:00Z", "message": {"role": "user"}}
        ).encode()
        + b"\n"
    }
    sessions = pps.parse_native_transcripts(user_only).sessions
    assert all(not session.usage_known for session in sessions)

    ledger = _live_ledger(tmp_path)
    reservations = ledger.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0 + 10)
    reviewers_only = {k: v for k, v in data.items() if "subagents" in k}
    counts = pps.settle_attempt(
        ledger, reservations, pps.parse_native_transcripts(reviewers_only), complete=True
    )
    assert counts["unknown"] == len(reservations)  # no implementer: never settle to zero
    assert all(item["source"] == "unknown" for item in ledger.sessions())
    ledger.close()
    malformed = _live_ledger(tmp_path / "malformed", run_id="malformed-run")
    reservations = malformed.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0 + 10)
    broken = {**data, next(iter(data)): data[next(iter(data))] + b"{not json\n"}
    facts = pps.parse_native_transcripts(broken)
    assert facts.malformed_lines
    counts = pps.settle_attempt(malformed, reservations, facts, complete=True)
    assert counts["unknown"] == len(reservations)
    malformed.close()


def _gate(tmp_path: Path, event: Mapping[str, Any]) -> subprocess.CompletedProcess[str]:
    script = tmp_path / pps.RESERVATION_GATE_FILE
    script.write_text(pps.RESERVATION_GATE_PROGRAM)
    # The gate resolves reviewers with the bundle's own review_gate.py beside it.
    shutil.copy(REPO_ROOT / "examples/dark-factory/hooks/review_gate.py", tmp_path)
    limits = json.dumps(pps.RESERVATION_LIMITS)
    return subprocess.run(
        [sys.executable, "-I", str(script), limits],
        input=json.dumps(event),
        capture_output=True,
        text=True,
        env={"PATH": os.environ.get("PATH", ""), "PPA_RESERVATION_DIR": str(tmp_path / "r")},
        check=False,
        timeout=30,
    )


def test_reservation_gate_refuses_reviewer_starts_past_the_reservation(tmp_path: Path) -> None:
    def start(kind: str) -> int:
        event = {"tool_name": "Agent", "tool_input": {"subagent_type": kind, "prompt": "review"}}
        return _gate(tmp_path, event).returncode

    assert pps.RESERVATION_LIMITS == {"plan": 1, "diff": 2}
    assert start("dark-factory:plan-reviewer") == 0
    assert start("plan-reviewer") == 2
    assert start("dark-factory:diff-reviewer") == 0
    assert start("diff-reviewer") == 0
    refused = _gate(
        tmp_path,
        {"tool_name": "Task", "tool_input": {"subagent_type": "dark-factory:diff-reviewer"}},
    )
    assert refused.returncode == 2 and "allowance is spent" in refused.stderr
    assert start("general-purpose") == 2
    assert start("") == 2  # no type and no reviewer description

    inferred = tmp_path / "inferred"
    inferred.mkdir()

    def described(text: str) -> int:
        event = {"tool_name": "Agent", "tool_input": {"description": text, "prompt": "review"}}
        return _gate(inferred, event).returncode

    assert described("Plan review round 1") == 0  # the bundle infers the plan reviewer
    assert described("Plan review round 2") == 2
    assert described("Diff review round 1") == 0
    assert _gate(tmp_path, {"tool_name": "Bash", "tool_input": {"command": "ls"}}).returncode == 0
    (tmp_path / "r" / "starts.json").write_text("[]")
    assert start("dark-factory:plan-reviewer") == 2  # unreadable state refuses

    hooks = json.loads((REPO_ROOT / "examples/dark-factory/hooks/hooks.json").read_text())
    patched = pps.private_bundle_hooks(hooks)
    added = patched["hooks"]["PreToolUse"][-1]
    assert added["matcher"] == "Agent|Task"
    assert pps.RESERVATION_GATE_FILE in added["hooks"][0]["command"]
    assert patched["hooks"]["PreToolUse"][:-1] == hooks["hooks"]["PreToolUse"]


def test_allowance_breaches_name_every_reached_bound() -> None:
    facts = pps.parse_native_transcripts(_transcripts())
    implementer = next(s for s in facts.sessions if s.role == pps.IMPLEMENTER)
    roomy = [
        pps.Reservation(n, 1, role, 8192, 32, 300.0) for n, role in enumerate(pps.ATTEMPT_ROLES)
    ]
    now = max(s.last for s in facts.sessions if s.last is not None)
    assert pps.allowance_breaches(roomy, facts, now=now) == []
    tight = [
        pps.Reservation(0, 1, pps.IMPLEMENTER, implementer.output_tokens, 32, 300.0),
        *roomy[1:],
    ]
    assert pps.allowance_breaches(tight, facts, now=now) == ["output_tokens:implementer"]
    assert "seconds:" in " ".join(pps.allowance_breaches(roomy, facts, now=now + 400))
    assert pps.allowance_breaches(roomy[:1], facts, now=now)[0].startswith("unreserved:")
    assert pps.allowance_breaches(roomy, None, now=now) == []


def test_image_tags_and_layer_repository_are_run_owned() -> None:
    assert pps.image_tag(CANDIDATE, "acme-run-1") != pps.image_tag(CANDIDATE, "acme-run-2")
    assert pps.image_tag(CANDIDATE, "acme-run-1").startswith(CANDIDATE[:12] + "-")
    assert pps.layer_repository("localhost:5999/", "dark-factory") == (
        "localhost:5999/dark-factory-runner"
    )


def test_held_secrets_are_found_in_every_encoding_after_clear() -> None:
    scrubber = pps.Scrubber()
    grant = scrubber.hold(pps.HeldSecret(GRANT))
    assert repr(grant) == str(grant) == "<held secret>"
    with pytest.raises(TypeError):
        json.dumps({"grant": grant})
    assert scrubber.scrub(f"token={GRANT};") == "token=<redacted>;"
    grant.clear()
    assert grant.reveal() == ""
    scan = pps.TokenScan(scrubber)
    harvest = pps.HarvestState(scan)
    harvest.feed(b'{"kind": "process", "count": 1, "processes": []}')
    assert scan.as_json()["harvest"]["token_bytes_found"] is False
    leaked = f"prefix {GRANT} suffix\n".encode()
    harvest.feed(
        json.dumps(
            {
                "kind": "file",
                "path": "c/projects/s.jsonl",
                "offset": 0,
                "data": base64.b64encode(leaked).decode(),
            }
        ).encode()
    )
    assert scan.as_json()["harvest"]["token_bytes_found"] is True
    secret_dump = json.dumps({"data": {"token": base64.b64encode(GRANT.encode()).decode()}})
    assert scan.scan("children", secret_dump.encode()) is True
    assert scrubber.scrub(f"x {GRANT} y") == "x <redacted> y"


def test_shell_keeps_held_secrets_out_of_argv_env_and_stdin(tmp_path: Path) -> None:
    scrubber = pps.Scrubber()
    scrubber.hold(pps.HeldSecret(GRANT))
    scan = pps.TokenScan(scrubber)
    env = pps.child_environment(
        {
            "PATH": "/usr/bin:/bin",
            "HOME": str(tmp_path),
            "AWS_SECRET_ACCESS_KEY": "x",
            "GITHUB_TOKEN": "y",
            "LANG": "C.UTF-8",
        },
        kubeconfig=str(tmp_path / "kubeconfig"),
        docker_context="ssh://builder.example.com",
    )
    assert set(env) == {
        "PATH",
        "HOME",
        "LANG",
        "KUBECONFIG",
        "DOCKER_HOST",
        "PYTHONDONTWRITEBYTECODE",
    }
    shell = pp.Shell(env=env, scrubber=scrubber, scan=scan, cwd=tmp_path, log=tmp_path / "log")
    marker = tmp_path / "ran"
    program = (
        f"import pathlib, sys; pathlib.Path({str(marker)!r}).write_text('x'); "
        "sys.stdout.write(str(len(sys.stdin.read())))"
    )
    with pytest.raises(pp.StepFailed):
        shell.run([sys.executable, "-c", program, GRANT])
    with pytest.raises(pp.StepFailed):
        shell.run([sys.executable, "-c", program], extra_env={"LEAK": GRANT})
    with pytest.raises(pp.StepFailed):
        shell.run([sys.executable, "-c", program], stdin=GRANT.encode())
    assert not marker.exists()
    result = shell.run([sys.executable, "-c", program], stdin=GRANT.encode(), secret_stdin=True)
    assert result.stdout == str(len(GRANT)).encode()
    allowed = shell.run(
        [
            sys.executable,
            "-c",
            "import os; print(len(os.environ['CURIE_APPROVAL_PRINCIPAL_TOKEN']))",
        ],
        extra_env={"CURIE_APPROVAL_PRINCIPAL_TOKEN": GRANT},
        secret_env=("CURIE_APPROVAL_PRINCIPAL_TOKEN",),
    )
    assert allowed.returncode == 0
    assert GRANT not in (tmp_path / "log").read_text()
    assert (tmp_path / "log").stat().st_mode & 0o777 == 0o600


# A recorded refusal: a tracker-surface publication approval answered by its
# listed operator (apps/api approvers.NoVerifiableApprovers), as cli/src/exit.rs
# prints it on stdout under --json.
TRACKER_REFUSAL = "could not verify approvers: github has no approver list that can be verified"
TRACKER_REFUSAL_STDOUT = json.dumps(
    {
        "error": "resolving approval failed with 403 Forbidden: "
        + json.dumps({"detail": TRACKER_REFUSAL}),
        "fix": None,
    }
).encode()

FAKE_CURIE = """
import json, sys
args = sys.argv[1:]
sys.stderr.write("Kubernetes context: kind-acme-scratch\\n")
if "--mint-operator-principal" in args:
    subject = args[args.index("--mint-operator-principal") + 1]
    token = "" if subject == "U0ACMEEMPTY" else "synthetic-operator-token-" + subject
    print(json.dumps({"operator_principal": {"token": token, "subject": subject}}))
elif "--resolve" in args:
    sys.stdout.write(REFUSAL)
    sys.exit(1)
else:
    print(json.dumps({"pending": [], "count": 0}))
"""


def _fake_curie(tmp_path: Path) -> pp.CurieCli:
    program = tmp_path / "fake_curie.py"
    program.write_text(FAKE_CURIE.replace("REFUSAL", repr(TRACKER_REFUSAL_STDOUT.decode())))
    program.chmod(0o700)
    launcher = tmp_path / "curie"
    launcher.write_text(f'#!/bin/sh\nexec {sys.executable} {program} "$@"\n')
    launcher.chmod(0o700)
    scrubber = pps.Scrubber()
    scrubber.hold(pps.HeldSecret(GRANT))
    shell = pp.Shell(
        env={"PATH": "/usr/bin:/bin"},
        scrubber=scrubber,
        scan=pps.TokenScan(scrubber, pps.SCAN_CHANNELS),
        cwd=tmp_path,
        log=tmp_path / "commands.jsonl",
    )
    inputs: Any = type(
        "Inputs", (), {"context": "kind-acme-scratch", "namespace": "ppa-acme", "release": "ppa"}
    )()
    return pp.CurieCli(shell, str(launcher), inputs)


def _log(tmp_path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (tmp_path / "commands.jsonl").read_text().splitlines()]


def test_approval_cli_calls_keep_full_output_in_the_private_log(tmp_path: Path) -> None:
    curie = _fake_curie(tmp_path)
    token = curie.mint_operator_principal("acme-pp", "U0ACMEALLOW")
    assert token.reveal() == "synthetic-operator-token-U0ACMEALLOW"
    refused = curie.run(
        "approvals",
        "acme-pp",
        "--resolve",
        "a-1",
        "--reject",
        check=False,
        extra_env={"CURIE_APPROVAL_PRINCIPAL_TOKEN": token.reveal()},
        secret_env=("CURIE_APPROVAL_PRINCIPAL_TOKEN",),
    )
    assert refused.returncode == 1
    curie.json("work-items", "--agent", "acme-pp")
    with pytest.raises(pp.StepFailed, match="returned no token"):
        curie.mint_operator_principal("acme-pp", "U0ACMEEMPTY")
    mint, resolve, other, empty = _log(tmp_path)
    # The mint keeps its output with the token redacted, never the token itself.
    assert mint["exit"] == 0
    assert "<redacted>" in mint["stdout"] and "synthetic-operator-token" not in mint["stdout"]
    assert mint["stderr"].startswith("Kubernetes context")
    # The refusal's API detail is on stdout, where the stderr tail never looked.
    assert resolve["exit"] == 1
    assert TRACKER_REFUSAL in resolve["stdout"]
    assert resolve["stderr"].startswith("Kubernetes context")
    # Other verbs keep the stderr tail only.
    assert set(other) == {"argv", "exit", "stderr_tail"}
    # A zero exit without a parseable token withholds stdout and keeps its size.
    assert empty["stdout"].startswith("<withheld: ")
    log = (tmp_path / "commands.jsonl").read_text()
    assert "synthetic-operator-token" not in log and GRANT not in log


def test_resolution_outcome_names_the_api_refusal_and_its_authorizer() -> None:
    outcome = pps.cli_outcome(1, TRACKER_REFUSAL_STDOUT, b"Kubernetes context: kind\n")
    assert outcome == {"exit": 1, "http_status": 403, "reason": TRACKER_REFUSAL}
    assert pps.cli_outcome(0, b'{"id": "a-1"}', b"") == {
        "exit": 0,
        "http_status": 200,
        "reason": None,
    }
    plain = pps.cli_outcome(1, b"", b"Error: resolving approval failed with 410 Gone: gone\n")
    assert plain == {"exit": 1, "http_status": 410, "reason": "gone"}
    # apps/api/src/curie_api/schemas/approvals.py ApprovalAuditOut, as the tracker
    # refusal records it for the listed operator.
    tracker = {
        "action": "denied",
        "actor": "U0ACMEALLOW",
        "principal_kind": "operator",
        "principal_subject": None,
        "decision": "rejected",
        "authorizer": "NoVerifiableApprovers",
        "authorized": False,
        "reason": TRACKER_REFUSAL,
        "created_at": "2026-11-11T05:52:37.350823",
    }
    listed = {
        **tracker,
        "actor": "U0ACMEOTHER",
        "authorizer": pps.EXPLICIT_USERS_AUTHORIZER,
        "reason": "not on the explicit list of approvers",
    }
    assert pps.audit_denial([tracker], "U0ACMEALLOW") == {
        "audit_authorizer": "NoVerifiableApprovers",
        "audit_reason": TRACKER_REFUSAL,
    }
    assert pps.audit_denial([], "U0ACMEALLOW") == {"audit_authorizer": None, "audit_reason": None}
    # A set that admits nobody refuses an unlisted operator too; it is no control.
    unlisted = {**tracker, "actor": "U0ACMEOTHER"}
    kwargs: dict[str, Any] = {
        "action": "denied",
        "subject": "U0ACMEOTHER",
        "authorized": False,
        "authorizer": pps.EXPLICIT_USERS_AUTHORIZER,
    }
    assert not pps.audit_row([unlisted], **kwargs)
    assert pps.audit_row([listed], **kwargs)


# --------------------------------------------------------------------------
# Deployed settings, forge checks and helpers over recorded shapes
# --------------------------------------------------------------------------


def _values() -> dict[str, Any]:
    return pps.install_values(
        release="ppa-acme",
        registry="localhost:5999",
        digests={name: "sha256:" + "4" * 64 for name in pps.REQUIRED_IMAGES},
        agent="acme-pp-0a0b0c0d",
        repo=REPO,
        app_id="42",
        app_secret=APP_SECRET_NAME,
        trigger_label="ppa-unused-0a0b0c0d",
        priority_classes=None,
        controller_deploy=False,
        model_addresses=["192.0.2.10", "192.0.2.10", "192.0.2.11"],
        slack_bot_secret="ppa-acme-slack-bot",
    )


def test_factory_values_pass_the_api_forge_model() -> None:
    """The factory lists, keyed as the chart's curie.factory.forgesJson renders them."""

    from curie_api.forges.config import ForgesConfig

    def snake(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower(): snake(v) for key, v in value.items()
            }
        if isinstance(value, list):
            return [snake(item) for item in value]
        return value

    factory = _values()["factory"]
    document: dict[str, Any] = {}
    for key, section in (("codeHosts", "code_hosts"), ("trackers", "trackers")):
        entries = snake(factory[key])
        for entry in entries:
            credential = entry["credential"]
            for name in ("existing_secret", "existing_secret_key"):
                credential.pop(name)
            # The chart mounts the Secret and the API reads it as private_key.
            credential["private_key"] = "placeholder-key-material"
        document[section] = entries
    parsed = ForgesConfig.model_validate(document)
    [tracker] = parsed.trackers
    assert tracker.label == tracker.mention == "ppa-unused-0a0b0c0d"


def test_deployed_settings_scan_names_service_credentials() -> None:
    values = _values()
    assert values["agentSandbox"]["runner"]["fakeModel"] is False
    assert len(values["security"]["networkPolicy"]["allowedEgress"]) == 2
    # Deployment env shape from charts/curie/templates/worker.yaml (CURIE_FAKE_MODEL "0").
    worker = {
        "kind": "Deployment",
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "worker",
                            "env": [
                                {"name": "CURIE_FAKE_MODEL", "value": "0"},
                                {"name": "CURIE_MODEL", "value": "claude-sonnet-5-5"},
                            ],
                        }
                    ]
                }
            }
        },
    }
    assert pps.deployed_credential_findings([values, worker], grant_secret=GRANT_SECRET_NAME) == []
    leaks: list[tuple[Any, str]] = [
        (
            {
                "env": [
                    {
                        "name": "CURIE_CREDENTIALS",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": "ppa-acme-curie-secrets",
                                "key": "agentCredentials",
                            }
                        },
                    }
                ]
            },
            "CURIE_CREDENTIALS",
        ),
        (
            {"env": [{"name": "ANTHROPIC_BASE_URL", "value": "https://gateway.example.com"}]},
            "ANTHROPIC_BASE_URL",
        ),
        ({"settings": {"apiKeyHelper": "/bin/print-key"}}, "apiKeyHelper"),
        ({"agentSandbox": {"runner": {"fakeModel": True}}}, "fakeModel"),
        (
            {"agentSandbox": {"runner": {"credentialsExistingSecret": RUNNER_KEY_SECRET_NAME}}},
            "credentialsExistingSecret",
        ),
        (
            {"env": [{"name": "CLAUDE_CODE_OAUTH_TOKEN", "value": "inline-value"}]},
            "CLAUDE_CODE_OAUTH_TOKEN",
        ),
        ({"env": [{"name": "CLAUDE_CODE_USE_BEDROCK", "value": "1"}]}, "CLAUDE_CODE_USE_BEDROCK"),
    ]
    for document, name in leaks:
        found = pps.deployed_credential_findings([values, document], grant_secret=GRANT_SECRET_NAME)
        assert found == [name]
        assert "inline-value" not in json.dumps(found)


def test_pod_diagnostics_report_reasons_never_env() -> None:
    # Pod and Event shapes from the Kubernetes core/v1 API (PodStatus,
    # ContainerStatus.state and lastState, Event type/reason/involvedObject).
    pods = {
        "items": [
            {
                "metadata": {"name": "ppa-acme-curie-api-7c9d"},
                "spec": {
                    "containers": [
                        {"name": "api", "env": [{"name": "CURIE_API_KEY", "value": "inline-value"}]}
                    ]
                },
                "status": {
                    "phase": "Running",
                    "initContainerStatuses": [
                        {
                            "name": "migrate",
                            "ready": True,
                            "restartCount": 0,
                            "state": {"terminated": {"reason": "Completed", "exitCode": 0}},
                        }
                    ],
                    "containerStatuses": [
                        {
                            "name": "api",
                            "ready": False,
                            "restartCount": 4,
                            "state": {
                                "waiting": {
                                    "reason": "CrashLoopBackOff",
                                    "message": "back-off 1m20s restarting failed container",
                                }
                            },
                            "lastState": {"terminated": {"reason": "Error", "exitCode": 3}},
                        }
                    ],
                },
            }
        ]
    }
    events = {
        "items": [
            {
                "type": "Normal",
                "reason": "Pulled",
                "involvedObject": {"kind": "Pod", "name": "ppa-acme-curie-api-7c9d"},
            },
            {
                "type": "Warning",
                "reason": "BackOff",
                "message": "Back-off restarting failed container api",
                "count": 6,
                "involvedObject": {"kind": "Pod", "name": "ppa-acme-curie-api-7c9d"},
            },
        ]
    }
    found = pps.pod_diagnostics(pods, events)
    [pod] = found["pods"]
    assert pod["phase"] == "Running"
    migrate, api = pod["containers"]
    assert migrate["terminated"]["reason"] == "Completed"
    assert api["waiting"]["reason"] == "CrashLoopBackOff"
    assert api["last_terminated"] == {"reason": "Error", "exit_code": 3}
    assert [item["reason"] for item in found["warnings"]] == ["BackOff"]
    assert "inline-value" not in json.dumps(found)
    assert "CURIE_API_KEY" not in json.dumps(found)


def test_head_checks_require_completed_success() -> None:
    # Shapes: GitHub REST check runs (filter=latest) and the combined status endpoint.
    success = {"status": "completed", "conclusion": "success"}
    assert pps.judge_head_checks(
        {"total_count": 1, "check_runs": [success]}, {"state": "success", "statuses": []}
    )["all_completed_successfully"]
    assert pps.judge_head_checks(
        {"total_count": 0, "check_runs": []},
        {"state": "pending", "statuses": [{"context": "ci", "state": "success"}]},
    )["all_completed_successfully"]
    failing = [
        ({"check_runs": []}, {"statuses": []}),
        ({"check_runs": [{"status": "in_progress", "conclusion": None}]}, {"statuses": []}),
        (
            {"check_runs": [success, {"status": "completed", "conclusion": "failure"}]},
            {"statuses": []},
        ),
        ({"check_runs": [success]}, {"statuses": [{"context": "ci", "state": "pending"}]}),
    ]
    for runs, statuses in failing:
        assert not pps.judge_head_checks(runs, statuses)["all_completed_successfully"]


def test_recorded_platform_shapes_parse() -> None:
    lock = (
        "version: 1\nconnectors: {}\nrunner:\n"
        "  image: localhost:5999/curie-runner-layer-dark-factory@sha256:" + "5" * 64 + "\n"
        "  base: localhost:5999/curie-runner@sha256:" + "6" * 64 + "\n"
        "  delivery: registry\n  platforms:\n  - linux/amd64\n  source_digest: abc\n"
    )
    image = pps.lock_runner_image(lock)
    assert image == "localhost:5999/curie-runner-layer-dark-factory@sha256:" + "5" * 64
    assert pps.split_image_ref(image) == ("curie-runner-layer-dark-factory", "sha256:" + "5" * 64)
    assert pps.lock_runner_image("version: 1\nconnectors: {}\n") is None
    # apps/api/src/curie_api/schemas/workitems.py: owner keys on the item and each request.
    item = {
        "tenant_id": pps.DEFAULT_TENANT,
        "model_auth_mode": "personal",
        "owner_principal_id": "p-1",
        "enrollment_generation": 1,
        "requests": [
            {
                "sequence": 1,
                "terminal_cause": "publication_denied",
                "tenant_id": pps.DEFAULT_TENANT,
                "model_auth_mode": "personal",
                "owner_principal_id": "p-1",
                "enrollment_generation": 1,
            }
        ],
    }
    seeded = _owner(principal_id="p-1")
    assert pps.owner_matches(pps.owner_of(item), seeded)
    assert all(pps.owner_matches(owner, seeded) for owner in pps.request_owners(item))
    assert not pps.owner_matches(pps.owner_of({**item, "enrollment_generation": 2}), seeded)
    # apps/api/src/curie_api/schemas/approvals.py ApprovalAuditOut rows.
    rows = [
        {
            "action": "denied",
            "authorized": False,
            "principal_kind": "operator",
            "principal_subject": "U0ACMEOTHER",
            "actor": "U0ACMEOTHER",
            "decision": "approved",
        },
        {
            "action": "resolved",
            "authorized": True,
            "principal_kind": "operator",
            "principal_subject": "U0ACMEALLOW",
            "actor": "U0ACMEALLOW",
            "decision": "rejected",
        },
    ]
    assert pps.audit_row(rows, action="denied", subject="U0ACMEOTHER", authorized=False)
    assert pps.audit_row(
        rows, action="resolved", subject="U0ACMEALLOW", authorized=True, decision="rejected"
    )
    assert not pps.audit_row(
        rows, action="resolved", subject="U0ACMEALLOW", authorized=True, decision="approved"
    )
    # cli/src/main.rs error text for a refused operator principal.
    refused = b'Error: resolving approval failed with 403 Forbidden: {"detail":"not listed"}'
    assert pps.resolve_status(1, refused) == 403
    assert pps.resolve_status(0, b"") == 200
    assert pps.chart_fullname("ppa-acme") == "ppa-acme-curie"
    assert pps.chart_fullname("curie-acme") == "curie-acme"
    subject = pps.slack_shaped_subject(bytes(range(8)))
    assert subject.startswith("U0") and len(subject) == 10 and subject.isupper()
    pod = {
        "spec": {
            "containers": [
                {
                    "name": "runner",
                    "env": [
                        {
                            "name": "CLAUDE_CODE_OAUTH_TOKEN",
                            "valueFrom": {
                                "secretKeyRef": {"name": "ppa-acme-personal-grant", "key": "token"}
                            },
                        }
                    ],
                }
            ]
        }
    }
    assert pps.pod_grant_ref(pod) == ("ppa-acme-personal-grant", "token")
    pods = {
        "items": [
            {
                "spec": {
                    "containers": [
                        {"name": "api", "image": "localhost:5999/curie-api@sha256:" + "4" * 64}
                    ]
                },
                "status": {
                    "containerStatuses": [
                        {"name": "api", "imageID": "localhost:5999/curie-api@sha256:" + "4" * 64}
                    ]
                },
            }
        ]
    }
    expected = {"curie-api": "sha256:" + "4" * 64}
    assert (
        pps.running_image_failures(
            pods, expected, registry="localhost:5999", required=["curie-api"]
        )
        == []
    )
    assert pps.running_image_failures(
        pods, {"curie-api": "sha256:" + "7" * 64}, registry="localhost:5999", required=["curie-api"]
    ) == ["curie-api_digest_mismatch", "curie-api_not_running"]


# --------------------------------------------------------------------------
# The run's finalizer and the order of paid starts
# --------------------------------------------------------------------------


class _FakeClock(pp.Clock):
    def __init__(self, start: float) -> None:
        self.current = start

    def now(self) -> float:
        return self.current

    def monotonic(self) -> float:
        return self.current

    def wait(self, seconds: float) -> None:
        self.current += max(0.0, seconds)


class _ScriptedRun(pp.LiveRun):
    """Replaces the cluster-facing unpaid phases; the decision logic under test is real."""

    def __init__(self, events: list[str], fail_steps: tuple[str, ...] = (), **kwargs: Any):
        super().__init__(**kwargs)
        self.events = events
        self.fail_steps = fail_steps

    def _owned(self, name: str) -> None:
        def undo() -> None:
            self.events.append(f"undo:{name}")
            if name in self.fail_steps:
                raise pp.StepFailed(f"{name} cleanup failed")

        self.finalizer.register(name, undo)
        self.events.append(f"create:{name}")

    def phase_refusals(self) -> None:
        self.foreign_before = self.foreign_snapshot()

    def foreign_snapshot(self) -> dict[str, str]:
        return {"namespace/kube-system": "uid-kube-system"}

    def phase_namespace(self) -> None:
        self.mutated = True
        self._owned("namespace")

    def phase_images(self) -> None:
        self._owned("registry-images")

    def phase_install(self) -> None:
        self._owned("release")

    def phase_setup(self) -> None:
        self._owned("port-forward")
        self.agent_id = "agent-1"
        self.owner = _owner()

    def phase_forge(self) -> None:
        self._owned("fixture-issue")
        self.issue_number = 7
        self.repo_id = "1001"

    def phase_drill(self) -> None:
        if self.rehearsal:
            self.events.append("drill")
            return
        # A paid run skips the drill: the real phase returns before any call.
        super().phase_drill()

    def query_inventory(self) -> dict[str, str]:
        self.events.append("inventory")
        return {kind: "absent" for kind in pps.INVENTORY_KINDS}

    def set_platform_budget(self, output_tokens: int) -> None:
        self.events.append(f"budget:{output_tokens}")

    def fence_paid_starts(self) -> None:
        self.events.append("fence")
        self.ledger.record("budget", "paid_starts_fenced", True)


def _scripted(
    tmp_path: Path,
    cls: type[_ScriptedRun],
    events: list[str],
    *,
    expires_in: float = 4000,
    run_id: str = "scripted-run",
    **kwargs: Any,
) -> _ScriptedRun:
    clock = _FakeClock(T0)
    state = _private(tmp_path / "state")
    scrubber = pps.Scrubber()
    scan = pps.TokenScan(scrubber, pps.SCAN_CHANNELS)
    runtime = pp.Runtime({"PATH": "/nonexistent"}, pp.Output(scrubber, scan), scrubber, scan, clock)
    inputs = pp.Inputs(
        candidate=CANDIDATE,
        run_id=run_id,
        state_dir=state,
        context="kind-acme-scratch",
        namespace="ppa-acme-test",
        release="ppa-acme",
        registry="localhost:5999",
        registry_port=5999,
        docker_context="default",
        kubeconfig=str(tmp_path / "kubeconfig"),
        app_dir=tmp_path,
        app={"id": 42, "installation_id": 7, "repo": REPO},
        grant_file=_grant_file(_private(tmp_path / "scripted-grant")),
        expires_at=T0 + expires_in,
    )
    ledger = pps.Ledger.open(state, inputs.run_id, scrubber=scrubber)
    ledger.ensure_run(CANDIDATE, phase="run")
    return cls(events, inputs=inputs, runtime=runtime, ledger=ledger, **kwargs)


class _AdmissionProbe(_ScriptedRun):
    def phase_grant(self) -> None:
        self._owned("grant-secret")

    def admit(self, attempt: pp.Attempt) -> tuple[int, Any]:
        db = sqlite3.connect(self.inputs.state_dir / pps.LEDGER_FILE)
        rows = db.execute(
            "SELECT role, reserved_output FROM sessions WHERE run_id=?", (self.inputs.run_id,)
        ).fetchall()
        live = db.execute(
            "SELECT live_started_at FROM runs WHERE run_id=?", (self.inputs.run_id,)
        ).fetchone()[0]
        db.close()
        self.events.append(f"admit:reserved={len(rows)}:live={live is not None}")
        # apps/api/src/curie_api/lifecycle.py refuses a second active request with 409.
        return 409, {"detail": {"code": "active_request"}}


def test_budget_reservation_is_persisted_before_admission(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    run = _scripted(tmp_path, _AdmissionProbe, events)
    capsys.readouterr()
    code = run.execute()
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 1
    assert summary["phase"] == "attempt-1" and summary["detail"] == "admission returned 409"
    budget = events.index("budget:32768")
    assert events[budget + 1] == "admit:reserved=4:live=True"
    assert events[events.index("inventory") - 6 : events.index("inventory")] == [
        "undo:grant-secret",
        "undo:fixture-issue",
        "undo:port-forward",
        "undo:release",
        "undo:registry-images",
        "undo:namespace",
    ]
    ledger = pps.Ledger.open(tmp_path / "state", "scripted-run")
    assert [row["source"] for row in ledger.sessions()] == ["not_started"] * 4
    assert ledger.observations("cleanup")["foreign_preserved"] is True
    assert ledger.observations("cleanup")["token_scan"]["harvest"]["token_bytes_found"] is False
    ledger.close()


def test_refusal_before_admission_makes_no_paid_start_and_cleans_up(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    run = _scripted(
        tmp_path,
        _ScriptedRun,
        events,
        expires_in=pps.GRANT_MIN_LIFETIME - 1,
        fail_steps=("release",),
    )
    capsys.readouterr()
    code = run.execute()
    output = capsys.readouterr()
    summary = json.loads(output.out.strip().splitlines()[-1])
    assert code == 2
    assert summary["error"] == "lifetime_insufficient" and summary["phase"] == "grant"
    assert summary["cleanup_ok"] is False
    assert [event for event in events if event.startswith("undo:")] == [
        "undo:fixture-issue",
        "undo:port-forward",
        "undo:release",
        "undo:registry-images",
        "undo:namespace",
    ]
    assert not any(event.startswith(("budget:", "admit")) for event in events)
    assert GRANT not in output.out + output.err
    ledger = pps.Ledger.open(tmp_path / "state", "scripted-run")
    assert ledger.sessions() == []
    assert ledger.run_row()["live_started_at"] is None
    statuses = {row["kind"]: row["status"] for row in ledger.cleanup_rows()}
    assert statuses["step:release"] == "failed" and statuses["step:namespace"] == "ok"
    ledger.close()
    code, result = _judge(tmp_path / "state", "scripted-run", "cleanup", capsys)
    assert code == 1 and result["checks"]["steps_ok"] is False


class _Interrupted(_ScriptedRun):
    def phase_install(self) -> None:
        self._owned("release")
        os.kill(os.getpid(), signal.SIGTERM)


def test_interruption_runs_every_registered_cleanup(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    previous = signal.getsignal(signal.SIGTERM)
    run = _scripted(tmp_path, _Interrupted, events)
    capsys.readouterr()
    code = run.execute()
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 1 and summary["error"] == "interrupted" and summary["phase"] == "install"
    assert [event for event in events if event.startswith("undo:")] == [
        "undo:release",
        "undo:registry-images",
        "undo:namespace",
    ]
    assert signal.getsignal(signal.SIGTERM) == previous


# --------------------------------------------------------------------------
# rehearse: every unpaid live phase, then the finalizer, with no grant
# --------------------------------------------------------------------------

NO_GRANT: dict[str, str | None] = {
    "--grant-file": None,
    "--grant-expires-at": None,
    "--approve-live": None,
}


def test_rehearse_refuses_grant_inputs_through_the_command(tmp_path: Path) -> None:
    secret_value = "sk-ant-api03-synthetic-value-never-printed"
    supplied: list[tuple[str, dict[str, str | None], dict[str, str], str]] = [
        (
            "grant file",
            {**NO_GRANT, "--grant-file": str(tmp_path / "grant")},
            {},
            "arguments_invalid",
        ),
        ("empty grant file", {**NO_GRANT, "--grant-file": ""}, {}, "arguments_invalid"),
        ("expiry", {**NO_GRANT, "--grant-expires-at": _iso(3 * 3600)}, {}, "arguments_invalid"),
        (
            "approval",
            {**NO_GRANT, "--approve-live": pps.approved_ceiling()},
            {},
            "arguments_invalid",
        ),
        (
            "credential first",
            {**NO_GRANT, "--grant-file": str(tmp_path / "grant")},
            {"ANTHROPIC_API_KEY": secret_value},
            "service_credentials",
        ),
        ("candidate", {**NO_GRANT, "--candidate": "main"}, {}, "candidate_invalid"),
        ("release", {**NO_GRANT, "--release": "curie"}, {}, "release_reserved"),
    ]
    for index, (label, overrides, env, expected) in enumerate(supplied):
        base = _private(tmp_path / f"case-{index}")
        result = _run_script(_command_argv("rehearse", base, 5999, **overrides), base, **env)
        assert _refusal(result) == expected, label
        assert secret_value not in result.stdout + result.stderr
        assert not (base / "state" / pps.LEDGER_FILE).exists(), label

    base = _private(tmp_path / "clean")
    no_tools = _private(tmp_path / "no-tools")
    result = _run_script(
        _command_argv("rehearse", base, 5999, **NO_GRANT), base, PATH=str(no_tools)
    )
    summary = json.loads(result.stdout.strip().splitlines()[-1])
    assert result.returncode == 1
    assert summary["command"] == "rehearse" and summary["phase"] == "refusals"
    ledger = pps.Ledger.open(base / "state", "acme-run-1")
    assert ledger.is_rehearsal()
    assert ledger.sessions() == [] and ledger.run_row()["live_started_at"] is None
    ledger.close()


class _FakeCurie(pp.CurieCli):
    """Answers the setup read paths with the CLI's recorded --json shapes.

    cli/src/commands/approvals.rs (``--list`` and ``--list-routes``) and
    cli/src/commands/work.rs (``work-items`` list).
    """

    def __init__(self, base: pp.CurieCli, route_users: list[str]) -> None:
        super().__init__(base.shell, base.binary, base.inputs)
        self.route_users = route_users
        self.calls: list[tuple[str, ...]] = []

    def json(self, verb: str, *args: str, **kwargs: Any) -> Any:
        self.calls.append((verb, *args))
        if verb == "approvals" and "--list" in args:
            return {"agent": args[0], "pending": [], "count": 0, "truncated": False}
        if verb == "approvals" and "--list-routes" in args:
            return {
                "agent": args[0],
                "routes": {
                    pps.PUBLICATION_ROUTE: {
                        "resolution": {"kind": "slack", "address": RESOLUTION_CHANNEL},
                        "approvers": {"users": self.route_users},
                    }
                },
                "count": 1,
            }
        if verb == "work-items":
            return {"items": [], "limit": 50, "truncated": False}
        raise AssertionError(f"unexpected curie call {verb}")


class _FakeApi(pp.CurieApi):
    def __init__(self) -> None:
        scan = pps.TokenScan(pps.Scrubber())
        super().__init__(
            pp.JsonHttp("127.0.0.1", 9, tls=False, scan=scan),
            pps.HeldSecret("api-key-placeholder"),
            pps.HeldSecret("worker-token-placeholder"),
        )
        self.paths: list[str] = []

    def get(self, path: str) -> Any:
        self.paths.append(path)
        return []


class _Rehearsal(_ScriptedRun):
    route_users: list[str] | None = None

    def phase_setup(self) -> None:
        super().phase_setup()
        self.agent_id = "4f1e0000-0000-4000-8000-0000000000a1"
        self.policy_set = True
        users = self.route_users if self.route_users is not None else [self.allowed_subject]
        self.curie = _FakeCurie(self.curie, users)
        self.api = _FakeApi()
        self.check_read_paths()

    def phase_grant(self) -> None:
        self.events.append("grant")

    def admit(self, attempt: pp.Attempt) -> tuple[int, Any]:
        self.events.append("admit")
        return 409, None


class _WrongRoute(_Rehearsal):
    route_users = ["U0ACMEOTHER"]


class _FenceKube:
    """Records kubectl verbs; answers secret identity and runner pod listings."""

    def __init__(self, *, running: bool, secret: bool = True) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.running = running
        self.secret = secret

    def ctl(self, *args: str, **kwargs: Any) -> pp.Completed:
        self.calls.append(args)
        if args[:2] == ("get", "secret"):
            out = b"uid-grant 7" if self.secret else b""
            return pp.Completed(0, out, b"")
        if args[0] == "delete" and args[1] == "secret":
            self.secret = False
        # A worker that drains past the wait timeout answers nonzero; it must not stop the fence.
        return pp.Completed(1 if args[0] == "wait" else 0, b"", b"")

    def json(self, *args: str, **kwargs: Any) -> Any:
        self.calls.append(args)
        state = (
            {"running": {"startedAt": "2026-11-11T04:00:00Z"}}
            if self.running
            else {"waiting": {"reason": "CreateContainerConfigError"}}
        )
        return {
            "items": [{"status": {"phase": "Pending", "containerStatuses": [{"state": state}]}}]
        }


def test_fence_deletes_before_waiting_and_keeps_the_grant_identity(
    tmp_path: Path,
) -> None:
    run = _scripted(tmp_path, _ScriptedRun, [])
    run.grant_identity = ("uid-grant", "7")
    kube = _FenceKube(running=False)
    run.kube = kube  # type: ignore[assignment]
    pp.LiveRun.fence_paid_starts(run)
    verbs = [call[0] for call in kube.calls]
    assert verbs.index("scale") < verbs.index("wait")
    deletes = [i for i, call in enumerate(kube.calls) if call[0] == "delete"]
    assert [kube.calls[i][1] for i in deletes] == ["secret", "pod"]
    assert max(deletes) < verbs.index("wait")
    assert verbs.index("get") < deletes[0]  # identity read before the Secret is deleted
    assert run.final_grant_identity == ("uid-grant", "7")
    assert run.ledger.observations("budget")["paid_starts_fenced"] is True

    stuck = _scripted(_private(tmp_path / "stuck"), _ScriptedRun, [])
    stuck.grant_identity = ("uid-grant", "7")
    stuck.kube = _FenceKube(running=True)  # type: ignore[assignment]
    with pytest.raises(pp.StepFailed):
        pp.LiveRun.fence_paid_starts(stuck)
    assert stuck.ledger.observations("budget")["paid_starts_fenced"] is False


def test_rehearse_phase_cut_stops_after_forge_and_finalizes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    run = _scripted(tmp_path, _Rehearsal, events, stop_after=pp.REHEARSAL_CUT)
    capsys.readouterr()
    code = run.execute()
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 0, summary
    assert summary["command"] == "rehearse" and summary["phase"] == "drill"
    assert not any(event in {"grant", "admit"} or event.startswith("budget:") for event in events)
    assert [event for event in events if event.startswith("undo:")] == [
        "undo:fixture-issue",
        "undo:port-forward",
        "undo:release",
        "undo:registry-images",
        "undo:namespace",
    ]
    assert events[-1] == "inventory"
    assert isinstance(run.curie, _FakeCurie) and isinstance(run.api, _FakeApi)
    assert [call[0] for call in run.curie.calls] == ["approvals", "approvals", "work-items"]
    assert run.api.paths == [f"/approvals?status_filter=pending&agent_id={run.agent_id}"]
    ledger = pps.Ledger.open(tmp_path / "state", "scripted-run")
    assert ledger.sessions() == [] and ledger.run_row()["live_started_at"] is None
    assert ledger.observations("rehearsal") == {
        "marker": True,
        "approvals_list_empty": True,
        "publication_route_bound": True,
        "work_items_list_empty": True,
        "api_pending_empty": True,
        "fence_exercised": True,
    }
    assert events.index("drill") < events.index("fence") < events.index("undo:fixture-issue")
    ledger.close()
    code, result = _judge(tmp_path / "state", "scripted-run", "cleanup", capsys)
    assert code == 0, [key for key, value in result["checks"].items() if not value]

    wrong: list[str] = []
    failed = _scripted(_private(tmp_path / "wrong"), _WrongRoute, wrong, stop_after="forge")
    capsys.readouterr()
    assert failed.execute() == 1
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["phase"] == "setup"
    assert "publication_route_bound" in summary["detail"]
    assert [event for event in wrong if event.startswith("undo:")] == [
        "undo:port-forward",
        "undo:release",
        "undo:registry-images",
        "undo:namespace",
    ]
    with pytest.raises(ValueError):
        _scripted(_private(tmp_path / "late"), _Rehearsal, [], stop_after="attempt-1")


class _Drill(_Rehearsal):
    """The drill's verdicts over recorded CLI and audit shapes; no cluster."""

    authorizer = pps.EXPLICIT_USERS_AUTHORIZER

    def phase_setup(self) -> None:
        super().phase_setup()
        self.allowed_token = pps.HeldSecret("synthetic-allowed")
        self.unlisted_token = pps.HeldSecret("synthetic-unlisted")
        self.status = {"a-reject": "pending", "a-approve": "pending"}
        self.rows: dict[str, list[dict[str, Any]]] = {"a-reject": [], "a-approve": []}

    def drill_publication(self, label: str) -> str:
        self.events.append(f"drill:{label}")
        return f"a-{label}"

    def approval(self, approval_id: str) -> dict[str, Any]:
        return {"status": self.status[approval_id]}

    def audit(self, approval_id: str) -> Any:
        return self.rows[approval_id]

    def phase_drill(self) -> None:
        drill = self

        class Resolver:
            def run(self, verb: str, *args: str, **kwargs: Any) -> pp.Completed:
                approval_id = args[2]
                token = kwargs["extra_env"]["CURIE_APPROVAL_PRINCIPAL_TOKEN"]
                listed = token == "synthetic-allowed"
                subject = drill.allowed_subject if listed else drill.unlisted_subject
                if not listed or drill.authorizer != pps.EXPLICIT_USERS_AUTHORIZER:
                    drill.rows[approval_id].append(
                        {
                            "action": "denied",
                            "actor": subject,
                            "authorized": False,
                            "authorizer": drill.authorizer,
                            "reason": "refused",
                            "created_at": "2026-11-11T10:00:00",
                        }
                    )
                    return pp.Completed(1, TRACKER_REFUSAL_STDOUT, b"")
                decision = "rejected" if "--reject" in args else "approved"
                drill.status[approval_id] = decision
                drill.rows[approval_id].append(
                    {
                        "action": "resolved",
                        "actor": subject,
                        "authorized": True,
                        "principal_kind": "operator",
                        "decision": decision,
                    }
                )
                return pp.Completed(0, b"{}", b"")

        self.curie = Resolver()  # type: ignore[assignment]
        pp.LiveRun.phase_drill(self)


def test_drill_proves_the_list_refusal_reject_and_approve(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    run = _scripted(tmp_path, _Drill, events, stop_after=pp.REHEARSAL_CUT)
    capsys.readouterr()
    assert run.execute() == 0
    assert events.index("drill:reject") < events.index("drill:approve") < events.index("fence")
    ledger = pps.Ledger.open(tmp_path / "state", "scripted-run")
    observed = ledger.observations("rehearsal")
    assert observed["drill_unlisted_refused_by_list"] is True
    assert observed["drill_reject_resolved"] is True
    assert observed["drill_approve_resolved"] is True
    assert ledger.observations("approval")["cli_drill_unlisted"]["audit_authorizer"] == (
        pps.EXPLICIT_USERS_AUTHORIZER
    )
    ledger.close()
    # The tracker-surface refusal denies everyone; the drill must not pass on it.
    nobody: list[str] = []
    failed = _scripted(_private(tmp_path / "nobody"), _Drill, nobody, stop_after="drill")
    assert isinstance(failed, _Drill)
    failed.authorizer = "NoVerifiableApprovers"
    capsys.readouterr()
    assert failed.execute() == 1
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["phase"] == "drill" and "drill_unlisted_refused_by_list" in summary["detail"]


class _DrillApi(_FakeApi):
    """Records the drill's internal writes; answers in the API's recorded shapes."""

    def __init__(self) -> None:
        super().__init__()
        self.posts: list[tuple[str, Any, bool]] = []

    def get(self, path: str) -> Any:
        self.paths.append(path)
        # apps/api/src/curie_api/schemas/deployments.py DeploymentOut rows.
        return [{"id": "d-dev", "environment": "dev"}, {"id": "d-prod", "environment": "prod"}]

    def post(self, path: str, body: Any, *, internal: bool = False) -> tuple[int, Any]:
        self.posts.append((path, body, internal))
        if path.endswith("/selection"):
            return 200, {"conversation_id": body["conversation_id"]}
        return 201, {"id": "4f1e0000-0000-4000-8000-0000000000b1", "approval_id": "a-1"}


class _CardRun(_Rehearsal):
    card_channel = RESOLUTION_CHANNEL
    states: list[str] = ["f|0|f", "f|1|t", "t|2|f"]

    def approval(self, approval_id: str) -> dict[str, Any]:
        return {"card_channel": self.card_channel, "status": "pending"}

    def card_state(self, publication_id: str) -> dict[str, Any]:
        self.events.append(f"card:{publication_id}")
        return pps.card_delivery(self.states.pop(0) if len(self.states) > 1 else self.states[0])


def _card_run(tmp_path: Path, **attrs: Any) -> _CardRun:
    run = _scripted(tmp_path, _CardRun, [], stop_after=pp.REHEARSAL_CUT)
    assert isinstance(run, _CardRun)
    run.inputs = dataclasses.replace(run.inputs, resolution_channel=RESOLUTION_CHANNEL)
    run.agent_id = "4f1e0000-0000-4000-8000-0000000000a1"
    run.api = _DrillApi()
    for key, value in attrs.items():
        setattr(run, key, value)
    return run


def test_drill_publication_posts_a_routed_tracker_publication_and_waits_for_its_card(
    tmp_path: Path,
) -> None:
    run = _card_run(_private(tmp_path / "ok"), states=["f|0|f", "f|1|t", "t|2|f"])
    assert run.drill_publication("reject") == "a-1"
    assert isinstance(run.api, _DrillApi)
    (selection, _, selection_internal), (create, body, create_internal) = run.api.posts
    assert selection == "/v1/internal/workspaces/d-prod/selection" and selection_internal
    assert create == "/v1/internal/publications" and create_internal
    # apps/api/src/curie_api/schemas/publications.py PublicationCreate, as a factory
    # publication arrives: tracker reply, the publication route, its fixed card channel.
    assert body["reply_kind"] == "github" and body["reply_channel"] == REPO
    assert body["route"] == pps.PUBLICATION_ROUTE
    assert body["card_channel"] == RESOLUTION_CHANNEL
    assert body["base_sha"] == "0" * 40 and body["deployment_id"] == "d-prod"
    assert body["conversation_id"] != body["reply_conversation_id"]
    # A retry that clears the error is waited through; delivery ends the wait.
    assert [e for e in run.events if e.startswith("card:")] == [
        "card:4f1e0000-0000-4000-8000-0000000000b1"
    ] * 3
    state = run.ledger.observations("approval")["drill_reject_card"]
    assert state == {"delivered": True, "attempts": 2, "error": False}

    wrong = _card_run(_private(tmp_path / "wrong"), card_channel="C0EXAMPLE2")
    with pytest.raises(pp.StepFailed, match="not on the route's channel"):
        wrong.drill_publication("reject")
    assert not any(event.startswith("card:") for event in wrong.events)


def test_await_card_fails_on_exhausted_or_missing_delivery(tmp_path: Path) -> None:
    exhausted = _card_run(_private(tmp_path / "exhausted"), states=["f|3|t", "f|5|t"])
    with pytest.raises(pp.StepFailed, match="card was not delivered"):
        exhausted.await_card("4f1e0000-0000-4000-8000-0000000000b1", "attempt_1")
    assert exhausted.ledger.observations("approval")["attempt_1_card"]["attempts"] == 5
    silent = _card_run(_private(tmp_path / "silent"), states=["f|0|f"])
    start = silent.clock.now()
    with pytest.raises(pp.StepFailed, match="did not happen in time"):
        silent.await_card("4f1e0000-0000-4000-8000-0000000000b1", "attempt_2")
    assert silent.clock.now() - start >= pp.CARD_WAIT_SECONDS
    with pytest.raises(ValueError):
        pp.LiveRun.card_state(silent, "not-a-uuid'; DROP TABLE x; --")


def test_slack_kit_refuses_before_any_mutation(tmp_path: Path) -> None:
    good = _slack_file(_private(tmp_path / "good"))
    assert pps.require_slack_kit(str(good), RESOLUTION_CHANNEL) == (good, RESOLUTION_CHANNEL)
    for token_file, channel in (
        ("", RESOLUTION_CHANNEL),
        (str(good), ""),
        (str(good), "#testing"),
        (str(_slack_file(_private(tmp_path / "user"), b"xoxp-user-token\n")), RESOLUTION_CHANNEL),
    ):
        with pytest.raises(pps.Refusal) as refused:
            pps.require_slack_kit(token_file, channel)
        assert refused.value.code == "slack_kit_invalid"
    os.chmod(good, 0o644)
    with pytest.raises(pps.Refusal):
        pps.require_slack_kit(str(good), RESOLUTION_CHANNEL)
    # Through the real command, before any cluster call.
    base = _private(tmp_path / "command")
    result = _run_script(
        _command_argv(
            "rehearse",
            base,
            5999,
            **{
                "--grant-file": None,
                "--grant-expires-at": None,
                "--approve-live": None,
                "--resolution-channel": "general",
            },
        ),
        tmp_path,
    )
    assert _refusal(result) == "slack_kit_invalid"


def test_install_values_deploy_the_gateway_but_not_the_dispatcher() -> None:
    values = _values()
    assert values["channelGateway"] == {"deploy": True}
    dispatcher = values["dispatcher"]
    assert dispatcher["deploy"] is False
    assert dispatcher["image"]["repository"] == "localhost:5999/curie-dispatcher"
    assert dispatcher["slack"] == {
        "botTokenExistingSecret": "ppa-acme-slack-bot",
        "botTokenExistingSecretKey": "slackBotToken",
    }
    assert "appToken" not in json.dumps(dispatcher)
    assert pps.card_delivery("t|1|f\n") == {"delivered": True, "attempts": 1, "error": False}
    assert pps.card_delivery("f|5|t") == {"delivered": False, "attempts": 5, "error": True}
    assert pps.card_delivery("") == {"delivered": False, "attempts": None, "error": None}


def test_rehearse_ledger_is_refused_by_run_and_judge(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base = _private(tmp_path / "rehearsed")
    rehearsal = _scripted(base, _Rehearsal, [], run_id="acme-run-1", stop_after="forge")
    assert rehearsal.execute() == 0
    capsys.readouterr()
    code = pp.main(_command_argv("run", base, 5999), environ={"PATH": "/nonexistent"})
    assert code == 2
    assert json.loads(capsys.readouterr().out)["error"] == "rehearsal_run_id"
    for criterion in (*[c for c in pps.CRITERIA if c != "cleanup"], "all"):
        capsys.readouterr()
        code = pp.main(
            [
                "judge",
                "--criterion",
                criterion,
                "--state-dir",
                str(base / "state"),
                "--run-id",
                "acme-run-1",
                "--candidate",
                CANDIDATE,
            ],
            environ={},
        )
        assert code == 2, criterion
        assert json.loads(capsys.readouterr().out)["error"] == "rehearsal_ledger"
    code, result = _judge(base / "state", "acme-run-1", "cleanup", capsys)
    assert code == 0 and result["criterion"] == "cleanup"
    ledger = pps.Ledger.open(base / "state", "acme-run-1")
    assert _refused(lambda: ledger.begin_live(now=T0, grant_expires_at=T0 + 4000)) == (
        "rehearsal_run_id"
    )
    assert ledger.sessions() == [] and ledger.run_row()["live_started_at"] is None
    ledger.close()

    fresh = _private(tmp_path / "fresh")
    capsys.readouterr()
    code = pp.main(
        _command_argv("run", fresh, 5999), environ={"PATH": str(_private(tmp_path / "e"))}
    )
    assert code == 1
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["command"] == "run"


# --------------------------------------------------------------------------
# Error classes, runner credential evidence and the pre-start grant probe
# --------------------------------------------------------------------------

LOOKALIKE = "sk-ant-oat01-" + "Q" * 40
# The CLI's synthetic record for a refused request (Claude Code transcript
# fields `error`, `apiErrorStatus`, `isApiErrorMessage`; text "API Error: ...").
REVOKED_TEXT = (
    "Failed to authenticate. API Error: 401 "
    '{"type":"error","error":{"type":"authentication_error","message":"OAuth token revoked"}} '
    f"{GRANT} {LOOKALIKE}"
)


def _refused_transcript(config: str = "curie-claude-config-a1b2") -> dict[str, bytes]:
    records = [
        {
            "type": "user",
            "timestamp": _stamp(0),
            "sessionId": SESSION,
            "isSidechain": False,
            "message": {"role": "user", "content": "work the fixture issue"},
        },
        _assistant(
            "msg_synth",
            "<synthetic>",
            0,
            3,
            [{"type": "text", "text": REVOKED_TEXT}],
            isSidechain=False,
            error="authentication_failed",
            apiErrorStatus=401,
            isApiErrorMessage=True,
        ),
    ]
    return {f"{config}/projects/-workspace/{SESSION}.jsonl": _jsonl(records)}


def test_synthetic_records_keep_a_sanitized_error_class(tmp_path: Path) -> None:
    facts = pps.parse_native_transcripts(_refused_transcript())
    (session,) = facts.sessions
    summary = pps.session_summary(session)
    assert summary["synthetic"] == 1 and summary["usage_known"] is False
    (error,) = summary["error_classes"]
    assert error["cli_error"] == "authentication_failed"
    assert error["http_status"] == 401
    assert error["provider_error_type"] == "authentication_error"
    assert error["first_line"].startswith("Failed to authenticate. API Error: 401")
    assert "OAuth token revoked" in error["first_line"]
    assert GRANT not in error["first_line"] and "sk-ant-" not in error["first_line"]

    # Held secrets are scrubbed again on the way into the ledger, and its leak scan reads it.
    scrubber = pps.Scrubber()
    scrubber.hold(pps.HeldSecret(GRANT))
    ledger = pps.Ledger.open(_private(tmp_path / "state"), "error-class", scrubber=scrubber)
    ledger.ensure_run(CANDIDATE, phase="run")
    ledger.record("roles", "sessions_initial", [summary])
    assert "diagnostics" not in ledger.recorded_criteria()
    stored = json.dumps(ledger.observations("roles"))
    assert GRANT not in stored and LOOKALIKE not in stored
    ledger.close()


def test_a_failed_attempt_names_its_error_class_and_records_credential_evidence(
    tmp_path: Path,
) -> None:
    run = _scripted(tmp_path, _ScriptedRun, [])
    run.ledger.begin_live(now=T0, grant_expires_at=T0 + 4000)
    attempt = pp.Attempt(1)
    run.attempts[1] = attempt
    attempt.admitted_at = T0
    attempt.reservations = run.ledger.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0)
    harvest = pps.HarvestState(run.scan)
    for name, data in _refused_transcript().items():
        harvest.feed(_file_record(name, 0, data))
    harvest.feed(
        json.dumps(
            {"kind": "container_credential", "credential": "present", "credential_length": 36}
        ).encode()
    )
    harvest.feed(
        json.dumps(
            {
                "kind": "process",
                "count": 1,
                "processes": [
                    {"pid": 41, "start": T0, "credential": "empty", "credential_length": 0}
                ],
            }
        ).encode()
    )
    attempt.harvest = harvest
    assert run.error_suffix(attempt) == (
        " (session error: authentication_failed 401 authentication_error)"
    )
    run.settle(attempt)
    assert run.ledger.observations("diagnostics")["runner_credential_1"] == {
        "container": {"credential": "present", "credential_length": 36},
        "cli_processes": [{"credential": "empty", "credential_length": 0}],
    }
    (initial,) = run.ledger.observations("roles")["sessions_initial"]
    assert initial["error_classes"][0]["http_status"] == 401
    # Unknown usage settles every reserved session as unknown, so no further start.
    assert all(row["source"] == "unknown" for row in run.ledger.sessions())
    # The finalizer's ledger leak scan reads every recorded section, new ones included.
    assert '"runner_credential_1"' in run.ledger_dump()
    assert "authentication_failed" in run.ledger_dump()


def test_harvester_reports_credential_presence_and_length_never_the_value(
    tmp_path: Path,
) -> None:
    nonce = "0f0f1e1e2d2d3c3c"
    _fake_pod(tmp_path, nonce)
    proc = tmp_path / "proc"
    for pid, environ in (("103", b"CLAUDE_CODE_OAUTH_TOKEN=\0HOME=/home/runner\0"), ("104", None)):
        (proc / pid).mkdir()
        (proc / pid / "cmdline").write_bytes(b"/usr/local/bin/claude\0--print\0")
        (proc / pid / "stat").write_text(f"{pid} (claude) S " + " ".join(["0"] * 18) + " 9 0 0\n")
        if environ is not None:
            (proc / pid / "environ").write_bytes(environ)
    config = pps.harvest_config(
        nonce=nonce,
        tmp_root=str(tmp_path / "tmp"),
        review_dir=str(tmp_path / "tmp" / "dark-factory-review"),
        workspace=str(tmp_path / "workspace"),
        proc_root=str(proc),
        git=str(tmp_path / "gitbin" / "git"),
        once=True,
    )

    def harvest(env: dict[str, str]) -> pps.HarvestState:
        result = subprocess.run(
            [sys.executable, "-c", pps.HARVEST_PROGRAM, config],
            env={"PATH": "/usr/bin:/bin", **env},
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert GRANT.encode() not in result.stdout + result.stderr
        state = pps.HarvestState(pps.TokenScan(pps.Scrubber()))
        for line in result.stdout.splitlines():
            state.feed(line)
        return state

    seen = harvest({pps.PERSONAL_GRANT_ENV: GRANT}).credential_evidence()
    assert seen == {
        "container": {"credential": "present", "credential_length": len(GRANT)},
        "cli_processes": [
            {"credential": "present", "credential_length": len(GRANT)},
            {"credential": "empty", "credential_length": 0},
            {"credential": "unreadable", "credential_length": 0},
        ],
    }
    missing = harvest({}).credential_evidence()["container"]
    assert missing == {"credential": "absent", "credential_length": 0}


PROBE_OK = {
    "type": "result",
    "subtype": "success",
    "is_error": False,
    "result": "ok",
    "total_cost_usd": 0.0006,
    "usage": {"output_tokens": 4},
    "modelUsage": {"claude-haiku-5-5": {"outputTokens": 4}},
}
# The CLI's --output-format json result for a refused grant: subtype stays
# "success"; terminal_reason and api_error_status name the failure.
PROBE_REVOKED = {
    "type": "result",
    "subtype": "success",
    "is_error": True,
    "terminal_reason": "api_error",
    "api_error_status": 401,
    "result": REVOKED_TEXT,
    "total_cost_usd": 0,
    "usage": {"output_tokens": 0},
    "modelUsage": {},
}


def _probe_cli(directory: Path, answer: Mapping[str, Any], code: int) -> tuple[Path, Path]:
    """A stand-in CLI that reports what it was given, then answers like the real one."""

    report = directory / "probe-report.json"
    cli = directory / "claude"
    cli.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        f"GRANT = {GRANT!r}\n"
        "home = os.environ.get('HOME', '')\n"
        "seen = {\n"
        "    'env': sorted(os.environ),\n"
        "    'home': home,\n"
        "    'home_entries': sorted(os.listdir(home)) if home else None,\n"
        "    'cwd_is_home': os.path.realpath(os.getcwd()) == os.path.realpath(home),\n"
        "    'grant_matches': os.environ.get('CLAUDE_CODE_OAUTH_TOKEN') == GRANT,\n"
        "    'grant_in_argv': any(GRANT in arg for arg in sys.argv),\n"
        "    'argv': sys.argv[1:],\n"
        "    'stdin': sys.stdin.read(),\n"
        "}\n"
        f"open({str(report)!r}, 'w').write(json.dumps(seen))\n"
        f"print({json.dumps(json.dumps(answer))})\n"
        f"sys.exit({code})\n"
    )
    cli.chmod(0o755)
    return cli, report


def test_grant_probe_runs_with_an_empty_home_and_the_grant_alone(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    caller_home = _private(tmp_path / "caller-home")
    (caller_home / ".claude").mkdir()
    (caller_home / ".claude" / ".credentials.json").write_text('{"claudeAiOauth": {}}')
    cli, report = _probe_cli(_private(tmp_path / "bin"), PROBE_OK, 0)
    grant = _grant_file(_private(tmp_path / "grant"))
    code = pp.main(
        [
            "grant-probe",
            "--grant-file",
            str(grant),
            "--grant-expires-at",
            str(int(T0 + 4000)),
            "--grant-probe-cli",
            str(cli),
        ],
        environ={"PATH": "/usr/bin:/bin", "HOME": str(caller_home), "LANG": "C.UTF-8"},
        clock=_FakeClock(T0),
    )
    output = capsys.readouterr()
    summary = json.loads(output.out.strip().splitlines()[-1])
    assert code == 0 and summary["verdict"] == "pass"
    assert summary["probe"]["cost_usd"] == 0.0006 and summary["probe"]["output_tokens"] == 4
    assert summary["probe"]["models"] == ["claude-haiku-5-5"]
    seen = json.loads(report.read_text())
    assert seen["home"] != str(caller_home) and seen["home_entries"] == []
    assert seen["cwd_is_home"] and seen["grant_matches"] and not seen["grant_in_argv"]
    assert set(seen["env"]) == set(pps.grant_probe_env(home="h", cli_dir="d", grant="g"))
    assert not [name for name in seen["env"] if name.startswith("ANTHROPIC")]
    assert seen["argv"][seen["argv"].index("--model") + 1] == pps.GRANT_PROBE_MODEL
    assert seen["stdin"] == pps.GRANT_PROBE_PROMPT.decode()
    assert not Path(seen["home"]).exists()
    assert GRANT not in output.out + output.err


def test_a_failed_grant_probe_stops_before_any_reservation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    run = _scripted(tmp_path, _ScriptedRun, events)
    cli, _report = _probe_cli(_private(tmp_path / "bin"), PROBE_REVOKED, 1)
    run.inputs = dataclasses.replace(run.inputs, grant_probe_cli=str(cli))
    capsys.readouterr()
    code = run.execute()
    output = capsys.readouterr()
    summary = json.loads(output.out.strip().splitlines()[-1])
    assert code == 1 and summary["phase"] == "grant"
    assert summary["detail"] == "grant probe failed: api_error 401 authentication_error"
    assert not any(event.startswith(("budget:", "admit")) for event in events)
    ledger = pps.Ledger.open(tmp_path / "state", "scripted-run")
    assert ledger.sessions() == []
    assert ledger.run_row()["live_started_at"] is None
    probe = ledger.observations("grant_probe")["result"]
    assert probe["ok"] is False and probe["cost_usd"] == 0.0
    assert probe["error"]["http_status"] == 401
    assert GRANT not in json.dumps(probe) and LOOKALIKE not in json.dumps(probe)
    ledger.close()
    assert GRANT not in output.out + output.err


# --------------------------------------------------------------------------
# The run owns each publication's own branch, not only its prefix
# --------------------------------------------------------------------------

OWN_BRANCH = "curie/publication-" + "c" * 32
FOREIGN_BRANCH = "curie/publication-" + "f" * 32
OWN_SHA = "c" * 40
OTHER_SHA = "e" * 40


class _BranchForge(pp.Forge):
    """Answers GitHub's pulls, matching-refs and ref reads; records writes."""

    def __init__(self, pulls: list[tuple[str, str]], refs: dict[str, str]) -> None:
        self.repo = REPO
        self.pulls = pulls
        self.refs = refs
        self.writes: list[tuple[str, str]] = []

    def call(self, method: str, path: str, body: Any = None, *, ok: Sequence[int] = (200,)) -> Any:
        if method != "GET":
            self.writes.append((method, path))
            return {}
        if "/pulls?" in path:
            return [
                {"number": n, "state": "open", "head": {"ref": ref, "sha": sha}}
                for n, (ref, sha) in enumerate(self.pulls, 1)
            ]
        if "/git/matching-refs/heads/" in path:
            prefix = "refs/heads/" + path.split("/git/matching-refs/heads/", 1)[1]
            return [{"ref": ref} for ref in self.refs if ref.startswith(prefix)]
        ref = path.split("/git/", 1)[1]
        return {"ref": ref, "object": {"sha": self.refs[ref]}}


class _BranchApi(_FakeApi):
    branch = OWN_BRANCH

    def get(self, path: str) -> Any:
        return {"id": path.rsplit("/", 1)[-1], "branch": self.branch, "status": "pending"}


def _branch_run(tmp_path: Path, forge: _BranchForge) -> _ScriptedRun:
    run = _scripted(tmp_path, _ScriptedRun, [])
    run.api = _BranchApi()
    run.forge = forge
    return run


def test_the_run_owns_its_publication_branch_outside_its_prefix(tmp_path: Path) -> None:
    probe = _BranchForge(pulls=[], refs={})
    run = _branch_run(tmp_path, probe)
    probe.pulls = [
        (OWN_BRANCH, OWN_SHA),
        (FOREIGN_BRANCH, OTHER_SHA),
        (f"{run.prefix}x", OTHER_SHA),
    ]
    probe.refs = {f"refs/heads/{OWN_BRANCH}": OWN_SHA, f"refs/heads/{FOREIGN_BRANCH}": OTHER_SHA}
    attempt = pp.Attempt(2)
    attempt.publication_id = "4f1e0000-0000-4000-8000-0000000000b1"
    # Before the branch is known, only the prefix is the run's.
    assert [pull["head"]["ref"] for pull in run.run_pulls()] == [f"{run.prefix}x"]
    run.record_publication_branch(attempt)
    assert attempt.branch == OWN_BRANCH
    assert run.ledger.observations("publication")["attempt_2_branch_known"] is True
    assert sorted(pull["head"]["ref"] for pull in run.run_pulls()) == sorted(
        [OWN_BRANCH, f"{run.prefix}x"]
    )
    assert run.run_branches() == [f"refs/heads/{OWN_BRANCH}"]

    bad = _branch_run(_private(tmp_path / "bad"), _BranchForge(pulls=[], refs={}))
    bad.api = _BranchApi()
    bad.api.branch = "main"
    with pytest.raises(pp.StepFailed, match="no valid branch"):
        bad.record_publication_branch(attempt)
    assert bad.publication_branches == []


def test_cleanup_closes_and_deletes_only_at_the_runs_own_commit(tmp_path: Path) -> None:
    forge = _BranchForge(pulls=[], refs={})
    run = _branch_run(tmp_path, forge)
    forge.pulls = [
        (OWN_BRANCH, OWN_SHA),
        (FOREIGN_BRANCH, OTHER_SHA),
        (f"{run.prefix}x", OTHER_SHA),
    ]
    forge.refs = {f"refs/heads/{OWN_BRANCH}": OWN_SHA, f"refs/heads/{FOREIGN_BRANCH}": OTHER_SHA}
    run.publication_branches = [OWN_BRANCH]
    run.published_heads = {OWN_BRANCH: OWN_SHA}
    # The prefix pull request points at no commit this run made: it is left open and reported.
    with pytest.raises(pp.StepFailed, match="left open"):
        run.close_run_pulls()
    run.delete_run_branches()
    assert forge.writes == [
        ("PATCH", f"/repos/{REPO}/pulls/1"),
        ("DELETE", f"/repos/{REPO}/git/refs/heads/{OWN_BRANCH}"),
    ]

    # A run branch that now points elsewhere is not the run's to delete.
    moved = _BranchForge(pulls=[], refs={f"refs/heads/{OWN_BRANCH}": OTHER_SHA})
    later = _branch_run(_private(tmp_path / "moved"), moved)
    later.publication_branches = [OWN_BRANCH]
    later.published_heads = {OWN_BRANCH: OWN_SHA}
    with pytest.raises(pp.StepFailed, match="left in place"):
        later.delete_run_branches()
    assert moved.writes == []


class _ProbeForge(pp.Forge):
    """The fixture repository for forge-probe; the probe branch name already exists."""

    writes: list[tuple[str, str]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.repo = REPO
        _ProbeForge.writes = []

    def mint(self) -> None:
        return None

    def revoke(self) -> None:
        return None

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if method == "GET" and path == f"/repos/{REPO}":
            return 200, {"default_branch": "main"}
        if method == "GET" and path.endswith("/git/ref/heads/main"):
            return 200, {"object": {"sha": "b" * 40}}
        if method == "GET" and "/git/commits/" in path:
            return 200, {"tree": {"sha": "d" * 40}}
        if method == "GET" and "/git/ref/heads/ppa-probe-" in path:
            return 200, {"object": {"sha": OTHER_SHA}}
        _ProbeForge.writes.append((method, path))
        if path.endswith("/git/commits"):
            return 201, {"sha": OWN_SHA}
        if path.endswith("/git/refs"):
            # GitHub: "Reference already exists".
            return 422, {"message": "Reference already exists"}
        return 200, {}


def test_forge_probe_never_deletes_a_branch_it_did_not_create(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pp, "Forge", _ProbeForge)
    state = _private(tmp_path / "state")
    app = _app_dir(_private(tmp_path / "apps"))
    code = pp.main(
        [
            "forge-probe",
            "--candidate",
            CANDIDATE,
            "--state-dir",
            str(state),
            "--run-id",
            "probe-collision",
            "--app-dir",
            str(app),
        ],
        environ={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        clock=_FakeClock(T0),
    )
    capsys.readouterr()
    assert code == 1
    assert not any(method == "DELETE" for method, _path in _ProbeForge.writes)
    assert not any(method == "PATCH" for method, _path in _ProbeForge.writes)


class _OwnedProbeForge(_ProbeForge):
    """The probe creates its own branch; the ref listing lags the delete by ``lag`` reads."""

    lag = 0
    reads_after_delete = 0
    deleted = False

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        if "/git/ref/heads/ppa-probe-" in path and method == "GET":
            if not type(self).deleted:
                return 200, {"object": {"sha": OWN_SHA}}
            type(self).reads_after_delete += 1
            if type(self).reads_after_delete > type(self).lag:
                return 404, {}
            return 200, {"object": {"sha": OWN_SHA}}
        if method == "DELETE":
            type(self).deleted = True
            _ProbeForge.writes.append((method, path))
            return 204, {}
        if method == "POST" and path.endswith("/git/refs"):
            return 201, {}
        if method == "POST" and path.endswith("/pulls"):
            return 201, {"number": 7}
        if method == "GET" and path.endswith("/pulls/7"):
            closed = any(m == "PATCH" for m, _p in _ProbeForge.writes)
            return 200, {"head": {"sha": OWN_SHA}, "state": "closed" if closed else "open"}
        return super().request(method, path, body)


def _run_owned_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, forge: type[_OwnedProbeForge], lag: int
) -> int:
    forge.lag = lag
    forge.reads_after_delete = 0
    forge.deleted = False
    monkeypatch.setattr(pp, "Forge", forge)
    return pp.main(
        [
            "forge-probe",
            "--candidate",
            CANDIDATE,
            "--state-dir",
            str(_private(tmp_path / "state")),
            "--run-id",
            "probe-lag",
            "--app-dir",
            str(_app_dir(_private(tmp_path / "apps"))),
        ],
        environ={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        clock=_FakeClock(T0),
    )


def test_forge_probe_waits_for_the_ref_listing_to_catch_up_with_the_delete(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    code = _run_owned_probe(tmp_path, monkeypatch, _OwnedProbeForge, lag=2)
    assert code == 0, capsys.readouterr()
    assert _OwnedProbeForge.reads_after_delete == 3


def test_forge_probe_fails_when_the_branch_never_disappears(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    code = _run_owned_probe(tmp_path, monkeypatch, _OwnedProbeForge, lag=10_000)
    capsys.readouterr()
    assert code == 1
    # Bounded: about 30 s at one read per 2 s, not unbounded.
    assert _OwnedProbeForge.reads_after_delete <= 17


# --------------------------------------------------------------------------
# A cut final record, the runner image, and retained volumes
# --------------------------------------------------------------------------


def test_an_unterminated_final_record_makes_usage_unknown(tmp_path: Path) -> None:
    project = f"curie-claude-config-a1b2/projects/-workspace/{SESSION}.jsonl"
    complete = [
        {
            "type": "user",
            "timestamp": _stamp(0),
            "sessionId": SESSION,
            "isSidechain": False,
            "message": {"role": "user", "content": "work the fixture issue"},
        },
        _assistant("msg_01", "claude-sonnet-5-5", 7900, 5, [{"type": "text", "text": "a"}]),
    ]
    cut = _assistant("msg_02", "claude-sonnet-5-5", 4000, 9, [{"type": "text", "text": "b"}])
    # 11,900 output tokens were written; the capture ends inside the last record.
    data = _jsonl(complete) + json.dumps(cut).encode()
    facts = pps.parse_native_transcripts({project: data})
    (session,) = facts.sessions
    # The cut record's 4,000 tokens cannot be counted, so the session is not 7,900.
    assert session.usage() is None
    assert facts.truncated_files == 1 and session.truncated

    state = _private(tmp_path / "state")
    ledger = pps.Ledger.open(state, "cut-record")
    ledger.ensure_run(CANDIDATE, phase="run")
    ledger.begin_live(now=T0, grant_expires_at=T0 + 4000)
    reservations = ledger.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0)
    pps.settle_attempt(ledger, reservations, facts, complete=True)
    assert all(row["source"] == "unknown" for row in ledger.sessions())
    checks, _counts = pps._judge_budget(ledger)
    assert checks["no_unknown_usage"] is False
    ledger.close()


def _runner_pod(image: str, image_id: str) -> dict[str, Any]:
    return {
        "metadata": {"name": "sandbox-1", "uid": "uid-1"},
        "spec": {
            "initContainers": [{"name": "attachments-init", "image": "busybox:1"}],
            "containers": [{"name": "runner", "image": image}],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{"name": "runner", "imageID": image_id}],
        },
    }


def test_each_runner_pod_must_run_the_candidate_layer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layer = "sha256:" + "1" * 64
    stale = "sha256:" + "2" * 64
    run = _scripted(tmp_path, _ScriptedRun, [])
    run.digests = {pps.LAYER_IMAGE: layer}
    seen: list[tuple[str, str]] = []

    def registry_image(get: Any, repository: str, reference: str) -> pps.RegistryImage:
        seen.append((repository, reference))
        return pps.RegistryImage(repository, reference, reference, CANDIDATE)

    monkeypatch.setattr(pps, "registry_image", registry_image)
    image = f"{run.inputs.registry}/dark-factory-runner@{layer}"
    good = pp.Attempt(1)
    good.pod_record = _runner_pod(image, f"{image}")
    run.check_runner_image(good)
    assert run.ledger.observations("roles")["runner_image_attempt_1"] == {
        "from_run_registry": True,
        "digest_matches": True,
        "revision_matches": True,
    }
    assert seen == [("dark-factory-runner", layer)]

    # The same tag can resolve to a stale image on the node; only the digest proves it.
    old = pp.Attempt(2)
    old.pod_record = _runner_pod(
        f"{run.inputs.registry}/dark-factory-runner:candidate",
        f"{run.inputs.registry}/dark-factory-runner@{stale}",
    )
    with pytest.raises(pp.StepFailed, match="not running the candidate runner image"):
        run.check_runner_image(old)
    recorded = run.ledger.observations("roles")["runner_image_attempt_2"]
    assert recorded["digest_matches"] is False
    checks, _counts = pps._judge_roles(run.ledger)
    assert checks["runner_images_are_candidate"] is False


class _VolumeKube:
    """kubectl for one namespace's PVCs and the cluster's PVs; records deletes."""

    def __init__(self, namespace: str, pvs: dict[str, dict[str, Any]]) -> None:
        self.namespace = namespace
        self.pvs = pvs
        self.namespace_present = True
        self.calls: list[tuple[str, ...]] = []

    def ctl(self, *args: str, **kwargs: Any) -> pp.Completed:
        self.calls.append(args)
        if args[:2] == ("get", "namespace"):
            out = b"volume-run" if self.namespace_present else b""
            return pp.Completed(0, out, b"")
        if args[:2] == ("delete", "namespace"):
            self.namespace_present = False
        if args[:2] == ("get", "persistentvolume"):
            pv = self.pvs.get(args[2])
            return pp.Completed(0, json.dumps(pv).encode() if pv else b"", b"")
        if args[:2] == ("delete", "persistentvolume"):
            self.pvs.pop(args[2], None)
        return pp.Completed(0, b"", b"")

    def json(self, *args: str, **kwargs: Any) -> Any:
        self.calls.append(args)
        return {
            "items": [
                {
                    "metadata": {"name": "data-postgres-0", "uid": "pvc-uid-1"},
                    "spec": {"volumeName": "pvc-0f0e0d0c"},
                },
                {"metadata": {"name": "pending", "uid": "pvc-uid-2"}, "spec": {}},
            ]
        }


def _retained(uid: str, namespace: str) -> dict[str, Any]:
    return {
        "metadata": {"name": "pvc-0f0e0d0c"},
        "spec": {
            "persistentVolumeReclaimPolicy": "Retain",
            "claimRef": {"namespace": namespace, "name": "data-postgres-0", "uid": uid},
        },
        "status": {"phase": "Released"},
    }


def test_a_retained_volume_of_the_run_is_recorded_deleted_and_checked(tmp_path: Path) -> None:
    run = _scripted(tmp_path, _ScriptedRun, [], run_id="volume-run")
    kube = _VolumeKube(run.inputs.namespace, {})
    kube.pvs["pvc-0f0e0d0c"] = _retained("pvc-uid-1", run.inputs.namespace)
    run.kube = kube  # type: ignore[assignment]
    pp.LiveRun.delete_namespace(run)
    assert run.volumes == {"pvc-0f0e0d0c": "pvc-uid-1"}
    # The namespace is gone but the Retain volume is not: the inventory must say so.
    assert run.volumes_absent() is False
    run.delete_owned_volumes()
    assert ("delete", "persistentvolume", "pvc-0f0e0d0c") == kube.calls[-2][:3]
    assert run.volumes_absent() is True
    assert run.ledger.observations("install")["owned_volumes"] == 1

    # A volume now bound to another claim is not the run's to delete.
    other = _scripted(_private(tmp_path / "other"), _ScriptedRun, [], run_id="volume-run")
    rebound = _VolumeKube(other.inputs.namespace, {})
    rebound.pvs["pvc-0f0e0d0c"] = _retained("someone-else", "elsewhere")
    other.kube = rebound  # type: ignore[assignment]
    other.volumes = {"pvc-0f0e0d0c": "pvc-uid-1"}
    with pytest.raises(pp.StepFailed, match="bound elsewhere"):
        other.delete_owned_volumes()
    assert not any(call[:2] == ("delete", "persistentvolume") for call in rebound.calls)


def test_the_item_total_stops_paid_work_at_its_approved_limit(tmp_path: Path) -> None:
    assert pps.require_item_spent("21831") == 21831 and pps.require_item_spent("") is None
    with pytest.raises(pps.Refusal):
        pps.require_item_spent(str(pps.MAX_OUTPUT_TOKENS))
    run = _scripted(tmp_path, _ScriptedRun, [])
    run.ledger.record("budget", "item_output_tokens_spent", 25000)
    run.ledger.begin_live(now=T0, grant_expires_at=T0 + 4000)
    attempt = pp.Attempt(1)
    run.attempts[1] = attempt
    attempt.admitted_at = T0
    attempt.reservations = run.ledger.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0)
    project = f"curie-claude-config-a1b2/projects/-workspace/{SESSION}.jsonl"
    records = [
        {
            "type": "user",
            "timestamp": _stamp(0),
            "sessionId": SESSION,
            "isSidechain": False,
            "message": {"role": "user", "content": "work the fixture issue"},
        },
        # Inside its 8,192 session reservation, but 25,000 + 8,000 passes the item's 32,768.
        _assistant("msg_01", "claude-sonnet-5-5", 8000, 5, [{"type": "text", "text": "a"}]),
    ]
    harvest = pps.HarvestState(run.scan)
    harvest.feed(_file_record(project, 0, _jsonl(records)))
    attempt.harvest = harvest
    run.grant_identity = ("uid-grant", "7")
    run.kube = _FenceKube(running=False)  # type: ignore[assignment]
    with pytest.raises(pp.StepFailed, match="paid work stopped"):
        run.check_allowance(attempt)
    budget = run.ledger.observations("budget")
    assert budget["allowance_breach"] == ["item_output_tokens"]
    assert budget["paid_starts_fenced"] is True

    under = _scripted(_private(tmp_path / "under"), _ScriptedRun, [])
    under.ledger.record("budget", "item_output_tokens_spent", 21831)
    under.ledger.begin_live(now=T0, grant_expires_at=T0 + 4000)
    second = pp.Attempt(1)
    under.attempts[1] = second
    second.admitted_at = T0
    second.reservations = under.ledger.reserve(pps.ATTEMPT_ROLES, attempt=1, now=T0)
    second.harvest = harvest
    under.check_allowance(second)  # 21,831 + 8,000 stays inside the item total


# --------------------------------------------------------------------------
# Two-owner scenario (#5379): command-path refusals, each with its control
#
# ``personal_profile.py --scenario two-owner`` refuses before paid admission.
# Every case drives the real ``main`` and ends with an authorized positive
# control; the driver-level cases live in test_personal_two_owner.py.
# --------------------------------------------------------------------------


def test_preflight_passes_with_candidate_images_and_the_mirrored_dex(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    images = tos._candidate_registry()
    dex_digest = next(ref for (repo, ref) in images if repo == pt.DEX_REPOSITORY)
    with tos._registry(images) as port:
        state = tos._private(tmp_path / "state")
        tos._record_dex(state, digest=dex_digest)
        inputs = tos._inputs_file(tmp_path, tos._kit(tmp_path, registry=f"localhost:{port}"))
        code, result = tos._main(tos._argv("preflight", inputs, state, *tos.APPROVE), capsys)
    assert code == 0, result
    assert result["verdict"] == "pass" and result["scenario"] == "two-owner"
    assert all(result["checks"].values())
    assert {row["owner"] for row in result["schedule"]["groups"]} == {"a", "b"}
    ledger = pto.TwoOwnerLedger.open_existing(state, tos.RUN_ID)
    try:
        dex_ref = f"localhost:{port}/{pt.DEX_REPOSITORY}@{dex_digest}"
        assert ledger.observations("images")["preflight_dex"] == dex_ref
    finally:
        ledger.close()


@pytest.mark.parametrize(
    ("served", "dex", "detail"),
    [("other", "served", "curie-"), ("candidate", "missing", "dex")],
)
def test_preflight_refuses_a_candidate_or_dex_image_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], served: str, dex: str, detail: str
) -> None:
    images = tos._candidate_registry(tos.OTHER_SHA if served == "other" else tos.CANDIDATE)
    dex_digest = next(ref for (repo, ref) in images if repo == pt.DEX_REPOSITORY)
    with tos._registry(images) as port:
        state = tos._private(tmp_path / "state")
        tos._record_dex(state, digest=dex_digest if dex == "served" else "sha256:" + "e" * 64)
        inputs = tos._inputs_file(tmp_path, tos._kit(tmp_path, registry=f"localhost:{port}"))
        code, result = tos._main(tos._argv("preflight", inputs, state, *tos.APPROVE), capsys)
    assert code == 2 and result["error"] == "candidate_image_mismatch", result
    assert detail in result["fix"]


@pytest.mark.parametrize("command", ["preflight", "run"])
def test_live_commands_refuse_a_setup_descriptor(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    state = tos._private(tmp_path / "state")
    setup = tos._inputs_file(tmp_path / "setup", tos._setup_document(tmp_path / "setup"))
    code, result = tos._main(tos._argv(command, setup, state, *tos.APPROVE), capsys)
    assert code == 2 and result["error"] == "independence_unasserted"
    # Control: the live descriptor gets past every input refusal.
    with tos._registry({}) as port:
        live = tos._inputs_file(
            tmp_path / "live", tos._kit(tmp_path / "live", registry=f"localhost:{port}")
        )
        code, result = tos._main(tos._argv(command, live, state, *tos.APPROVE), capsys)
    assert result.get("error") != "independence_unasserted"
    if command == "run":
        assert code == 1 and result["phase"] == "refusals" and result["error"] == "step_failed"
        ledger = pto.TwoOwnerLedger.open_existing(state, tos.RUN_ID)
        try:
            assert ledger.observations("budget")["schedule_check_passed"] is True
            assert not ledger.is_rehearsal()
        finally:
            ledger.close()


@pytest.mark.parametrize("command", ["images", "forge-probe", "rehearse"])
def test_setup_commands_refuse_a_live_descriptor(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    state = tos._private(tmp_path / "state")
    live = tos._inputs_file(tmp_path / "live", tos._kit(tmp_path / "live"))
    code, result = tos._main(tos._argv(command, live, state), capsys)
    assert code == 2 and result["error"] == "setup_inputs_carry_grants"
    # Control: a setup descriptor is accepted and the command reaches its first
    # child, which this PATH lacks (git, openssl or kubectl): a step failure.
    setup = tos._inputs_file(tmp_path / "setup", tos._setup_document(tmp_path / "setup"))
    code, result = tos._main(tos._argv(command, setup, state), capsys)
    assert code == 1 and result.get("error", "step_failed") == "step_failed", result
    if command == "rehearse":
        ledger = pto.TwoOwnerLedger.open_existing(state, tos.RUN_ID)
        try:
            assert ledger.is_rehearsal()
            assert ledger.observations("rehearsal")["schedule_check_passed"] is True
        finally:
            ledger.close()


@pytest.mark.parametrize(
    ("command", "flag", "value"),
    [
        ("images", "--context", tos.CONTEXT),
        ("forge-probe", "--app-dir", "app"),
        ("rehearse", "--resolution-channel", tos.CHANNEL),
        ("rehearse", "--grant-file", "grant"),
        ("preflight", "--output-token-ceiling", "32768"),
        ("run", "--item-output-tokens-spent", "0"),
        # Equal to its default, still a single-profile flag.
        ("run", "--grant-probe-cli", pp.DEFAULT_PROBE_CLI),
    ],
)
def test_single_profile_flags_are_refused_with_the_scenario(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, flag: str, value: str
) -> None:
    state = tos._private(tmp_path / "state")
    live = command in {"preflight", "run"}
    approve = tos.APPROVE if live else ()
    with tos._registry({}) as port:
        build = tos._kit if live else tos._setup_document
        inputs = tos._inputs_file(tmp_path, build(tmp_path, registry=f"localhost:{port}"))
        code, result = tos._main(tos._argv(command, inputs, state, *approve, flag, value), capsys)
        assert code == 2 and result["error"] == "arguments_invalid" and flag in result["fix"]
        code, result = tos._main(tos._argv(command, inputs, state, *approve), capsys)
    assert result.get("error") != "arguments_invalid", result


def test_inputs_file_is_a_two_owner_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tos._private(tmp_path / "state")
    inputs = tos._inputs_file(tmp_path, tos._setup_document(tmp_path))
    argv = ["judge", "--inputs-file", str(inputs), "--candidate", tos.CANDIDATE]
    argv += ["--state-dir", str(state), "--run-id", tos.RUN_ID, "--criterion", "roles"]
    code, result = tos._main(argv, capsys)
    assert code == 2 and result["error"] == "arguments_invalid"
    # Control: without it the single-profile judge runs as before (no ledger yet).
    code, result = tos._main([argv[0], *argv[3:]], capsys)
    assert result["error"] != "arguments_invalid"


@pytest.mark.parametrize("command", ["preflight", "run"])
@pytest.mark.parametrize("approval", [None, "approve everything"])
def test_live_commands_need_the_exact_live_approval(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, approval: str | None
) -> None:
    state = tos._private(tmp_path / "state")
    extra = () if approval is None else ("--approve-live", approval)
    with tos._registry({}) as port:
        inputs = tos._inputs_file(tmp_path, tos._kit(tmp_path, registry=f"localhost:{port}"))
        code, result = tos._main(tos._argv(command, inputs, state, *extra), capsys)
        assert code == 2 and result["error"] == "live_not_approved"
        code, result = tos._main(tos._argv(command, inputs, state, *tos.APPROVE), capsys)
    assert result.get("error") != "live_not_approved"


def test_rehearse_refuses_a_live_approval(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tos._private(tmp_path / "state")
    inputs = tos._inputs_file(tmp_path, tos._setup_document(tmp_path))
    code, result = tos._main(tos._argv("rehearse", inputs, state, *tos.APPROVE), capsys)
    assert code == 2 and result["error"] == "arguments_invalid"


@pytest.mark.parametrize("command", ["preflight", "run", "rehearse"])
def test_an_impossible_schedule_is_refused_before_any_mutation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    state = tos._private(tmp_path / "state")
    build = tos._setup_document if command == "rehearse" else tos._kit
    approve = () if command == "rehearse" else tos.APPROVE
    inputs = tos._inputs_file(tmp_path / "high", build(tmp_path / "high", prior=12_000))
    code, result = tos._main(tos._argv(command, inputs, state, *approve), capsys)
    assert code == 2 and result["error"] == "session_plan_impossible"
    assert not (state / f"{tos.RUN_ID}.work").exists()
    with tos._registry({}) as port:
        low = build(tmp_path / "low", prior=4_000, registry=f"localhost:{port}")
        code, result = tos._main(
            tos._argv(command, tos._inputs_file(tmp_path / "low", low), state, *approve), capsys
        )
    assert result.get("error") != "session_plan_impossible"


@pytest.mark.parametrize("command", ["images", "preflight", "rehearse", "run", "judge"])
@pytest.mark.parametrize(
    ("variable", "code"),
    [("ANTHROPIC_API_KEY", "service_credentials"), (pps.FAKE_MODEL_NAME, "fake_mode")],
)
def test_ambient_credentials_and_fake_mode_are_refused_first(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: str,
    variable: str,
    code: str,
) -> None:
    state = tos._private(tmp_path / "state")
    live = command in {"preflight", "run"}
    with tos._registry({}) as port:
        build = tos._kit if live else tos._setup_document
        inputs = tos._inputs_file(tmp_path, build(tmp_path, registry=f"localhost:{port}"))
        argv = tos._argv(command, None if command == "judge" else inputs, state)
        argv += [*tos.APPROVE] if live else []
        if command == "judge":
            argv += ["--criterion", "rehearsal"]
        exit_code, result = tos._main(argv, capsys, {**tos.PATH_ONLY, variable: "marker-value-1"})
        assert exit_code == 2 and result["error"] == code
        assert "marker-value-1" not in json.dumps(result)
        _exit, control = tos._main(argv, capsys)
    assert control.get("error") not in {"service_credentials", "fake_mode"}


def test_grant_probe_is_refused_for_the_scenario(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tos._private(tmp_path / "state")
    code, result = tos._main(tos._argv("grant-probe", None, state), capsys)
    assert code == 2 and result["error"] == "arguments_invalid"


def test_judge_reads_a_complete_rehearsal_ledger_and_nothing_else(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tos._private(tmp_path / "state")
    tos._rehearsal_ledger(state)
    # No inputs file, no tools on PATH, no credential: receipts only.
    code, result = tos._main(tos._judge_argv(state, "rehearsal"), capsys)
    assert code == 0 and result["verdict"] == "pass" and result["scenario"] == "two-owner"
    assert result["checks"]["no_native_sessions"] is True
    code, result = tos._main(tos._judge_argv(state, "owners"), capsys)
    assert code == 1 and result["checks"] == {"live_ledger": False}
    code, result = tos._main(tos._judge_argv(state, "rehearsal", tos.OTHER_SHA), capsys)
    assert code == 2 and result["error"] == "candidate_mismatch"


def test_judge_fails_a_rehearsal_ledger_missing_one_key(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tos._private(tmp_path / "state")
    tos._rehearsal_ledger(state, drop="admission_controls_refused")
    code, result = tos._main(tos._judge_argv(state, "rehearsal"), capsys)
    assert code == 1 and result["checks"]["admission_controls_refused"] is False


def test_judge_refuses_a_single_profile_ledger_and_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tos._private(tmp_path / "state")
    ledger = pps.Ledger.open(state, tos.RUN_ID)
    ledger.ensure_run(tos.CANDIDATE, phase="run")
    ledger.close()
    code, result = tos._main(tos._judge_argv(state, "rehearsal"), capsys)
    assert code == 2 and result["error"] == "ledger_missing"
    inputs = tos._inputs_file(tmp_path, tos._setup_document(tmp_path))
    argv = [*tos._judge_argv(state, "rehearsal"), "--inputs-file", str(inputs)]
    code, result = tos._main(argv, capsys)
    assert code == 2 and result["error"] == "arguments_invalid"
    # A two-owner criterion is not a single-profile one.
    single = [
        arg for arg in tos._judge_argv(state, "owners") if arg not in {"--scenario", "two-owner"}
    ]
    code, result = tos._main(single, capsys)
    assert code == 2 and result["error"] == "arguments_invalid"


def test_parser_criteria_cover_every_two_owner_criterion() -> None:
    choices = {*pps.CRITERIA, *pp.TWO_OWNER_CRITERIA}
    assert {*pto.CRITERIA, "rehearsal"} <= choices


@pytest.mark.parametrize("command", ["preflight", "run"])
@pytest.mark.parametrize("duplicate", ["same_path", "same_content", "hard_link"])
def test_two_owner_duplicate_grants_are_refused_by_the_real_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, duplicate: str
) -> None:
    state = tos._private(tmp_path / "state")
    document = tos._kit(tmp_path)
    grant_a = Path(document["owners"]["a"]["grant_file"])
    grant_b = Path(document["owners"]["b"]["grant_file"])
    if duplicate == "same_path":
        document["owners"]["b"]["grant_file"] = str(grant_a)
    elif duplicate == "same_content":
        grant_b.write_bytes(grant_a.read_bytes())
    else:
        grant_b.unlink()
        os.link(grant_a, grant_b)
    inputs = tos._inputs_file(tmp_path, document)
    code, result = tos._main(tos._argv(command, inputs, state, *tos.APPROVE), capsys)
    assert code == 2 and result["error"] == "duplicate_owner_grant", result
    # Control: two distinct grant files pass the duplicate check.
    with tos._registry({}) as port:
        control = tos._inputs_file(
            tmp_path / "ok", tos._kit(tmp_path / "ok", registry=f"localhost:{port}")
        )
        code, result = tos._main(tos._argv(command, control, state, *tos.APPROVE), capsys)
    assert result.get("error") != "duplicate_owner_grant", result


@pytest.mark.parametrize("command", ["images", "forge-probe", "rehearse", "preflight", "run"])
@pytest.mark.parametrize("unsafe", ["mode", "symlink", "inline_grant"])
def test_two_owner_unsafe_inputs_files_are_refused_by_the_real_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, unsafe: str
) -> None:
    state = tos._private(tmp_path / "state")
    live = command in {"preflight", "run"}
    approve = tos.APPROVE if live else ()
    build = tos._kit if live else tos._setup_document
    document = build(tmp_path)
    if unsafe == "inline_grant":
        document["target"]["grant"] = "synthetic-inline-marker-not-a-token"
    inputs = tos._inputs_file(tmp_path, document)
    if unsafe == "mode":
        os.chmod(inputs, 0o644)
    elif unsafe == "symlink":
        link = inputs.with_name("link.json")
        link.symlink_to(inputs)
        inputs = link
    code, result = tos._main(tos._argv(command, inputs, state, *approve), capsys)
    expected = {
        "mode": "inputs_unsafe",
        "symlink": "inputs_unsafe",
        "inline_grant": "inline_grant_refused",
    }[unsafe]
    assert code == 2 and result["error"] == expected, result
    assert "synthetic-inline-marker" not in json.dumps(result)
    # Control: the same document in a safe file is not refused for its file.
    with tos._registry({}) as port:
        good = build(tmp_path / "ok", registry=f"localhost:{port}")
        safe = tos._inputs_file(tmp_path / "ok", good)
        code, result = tos._main(tos._argv(command, safe, state, *approve), capsys)
    assert result.get("error") not in {"inputs_unsafe", "inline_grant_refused"}, result


@pytest.mark.parametrize("command", ["preflight", "run"])
@pytest.mark.parametrize("owner", ["a", "b"])
def test_two_owner_insufficient_lifetime_is_refused_by_the_real_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str, owner: str
) -> None:
    state = tos._private(tmp_path / "state")
    document = tos._kit(tmp_path)
    document["owners"][owner]["usable_until"] = int(tos.T0 + 60)
    inputs = tos._inputs_file(tmp_path, document)
    code, result = tos._main(tos._argv(command, inputs, state, *tos.APPROVE), capsys)
    assert code == 2 and result["error"] == "lifetime_insufficient", result
    assert f"owners.{owner}.usable_until" in json.dumps(result)
    # Control: a window covering the live and cleanup seconds is not refused.
    with tos._registry({}) as port:
        ok = tos._kit(tmp_path / "ok", registry=f"localhost:{port}")
        code, result = tos._main(
            tos._argv(command, tos._inputs_file(tmp_path / "ok", ok), state, *tos.APPROVE), capsys
        )
    assert result.get("error") != "lifetime_insufficient", result
