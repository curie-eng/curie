"""Shared fixtures for the two-owner command-path tests (#5379).

``test_personal_profile.py`` holds the two-owner command-path section and
``test_personal_two_owner.py`` holds the driver-level cases; both build their
inputs files, loopback registry and ``main`` calls from here. Nothing in this
module touches a cluster, GitHub, Docker or a network beyond a loopback
registry fixture, and no value is a real credential or identifier.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.server
import json
import os
import threading
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import personal_profile as pp
import personal_profile_support as pps
import personal_two_owner as pt
import personal_two_owner_support as pto
import pytest

REPO_ROOT = pp.REPO_ROOT
T0 = 1_800_000_000.0
CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
RUN_ID = "acme-two-owner-1"
TAG = pps.image_tag(CANDIDATE, RUN_ID)
MARKER_A = "synthetic-owner-a-marker-not-a-token"
MARKER_B = "synthetic-owner-b-marker-not-a-token"
SLACK_MARKER = "xoxb-synthetic-gateway-marker-not-a-token"
SESSION_MARKER = "session-marker-0001"
STATE_MARKER = "state-marker-0001"
CHANNEL = "C0EXAMPLE1"
REPO = "acme-corp/acme-fixture"
CONTEXT = "kind-acme-test"
NAMESPACE = "pto-acme-1"
RELEASE = "pto-acme"
FULLNAME = pps.chart_fullname(RELEASE)
REGISTRY = "localhost:5999"
WINDOW = T0 + pto.GRANT_MIN_LIFETIME + 3600
TENANT = pps.DEFAULT_TENANT
PRINCIPAL_A = "5d0c7f3e-0000-4000-8000-00000000a001"
PRINCIPAL_B = "5d0c7f3e-0000-4000-8000-00000000b002"
AGENT_ID = "5d0c7f3e-0000-4000-8000-0000000a9e01"
WORK_ITEM = "5d0c7f3e-0000-4000-8000-0000000c0001"
DIGESTS = {
    name: "sha256:" + hashlib.sha256(name.encode()).hexdigest() for name in pps.REQUIRED_IMAGES
}
DEX_DIGEST = "sha256:" + "d" * 64
API_PORT = 41001
DEX_PORT = 41002
PATH_ONLY = {"PATH": "/nonexistent"}
#: The native CLI's synthetic API-error text, as test_personal_profile.py
#: _refused_transcript replays it (a refused OAuth grant), without secret bytes.
REFUSED_TEXT = (
    "Failed to authenticate. API Error: 401 "
    '{"type":"error","error":{"type":"authentication_error","message":"OAuth token revoked"}}'
)
#: Its sanitized class (pps.error_summary of pps.error_class).
REFUSED_CLASS = "authentication_failed 401 authentication_error"


def _private(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _file(path: Path, content: bytes, mode: int = 0o600) -> Path:
    path.write_bytes(content)
    os.chmod(path, mode)
    return path


def _refused_record(timestamp: str, message_id: str = "msg_synth") -> dict[str, Any]:
    """The native CLI's synthetic assistant record after a refused request.

    Model ``<synthetic>`` with the CLI's ``error``, ``apiErrorStatus`` and
    ``isApiErrorMessage`` fields, the shape test_personal_profile.py replays.
    """

    return {
        "type": "assistant",
        "timestamp": timestamp,
        "isSidechain": False,
        "message": {
            "id": message_id,
            "model": pps.SYNTHETIC_MODEL,
            "role": "assistant",
            "content": [{"type": "text", "text": REFUSED_TEXT}],
            "usage": {"input_tokens": 10, "output_tokens": 0},
        },
        "error": "authentication_failed",
        "apiErrorStatus": 401,
        "isApiErrorMessage": True,
    }


def _kit(tmp_path: Path, *, registry: str = REGISTRY, prior: int = 0) -> dict[str, Any]:
    """A valid live inputs document (personal_two_owner_support.load_inputs_file)."""

    kit = _private(tmp_path / "kit")
    app = _private(kit / "app")
    (app / "app.json").write_text(json.dumps({"id": 42, "installation_id": 7, "repo": REPO}))
    (app / "app.pem").write_text("placeholder, never read here\n")
    kubeconfig = _file(kit / "kubeconfig", b"apiVersion: v1\nkind: Config\n")
    slack = _file(kit / "slack-bot", SLACK_MARKER.encode() + b"\n")
    grant_a = _file(kit / "grant-a", MARKER_A.encode() + b"\n")
    grant_b = _file(kit / "grant-b", MARKER_B.encode() + b"\n")
    return {
        "target": {
            "context": CONTEXT,
            "namespace": NAMESPACE,
            "release": RELEASE,
            "registry": registry,
            "docker_context": "acme-docker",
            "kubeconfig": str(kubeconfig),
        },
        "forge": {"app_dir": str(app)},
        "gateway": {"slack_bot_token_file": str(slack), "resolution_channel": CHANNEL},
        "owners": {
            "a": {"grant_file": str(grant_a), "usable_until": int(WINDOW)},
            "b": {"grant_file": str(grant_b), "usable_until": int(WINDOW)},
        },
        "independent_accounts": True,
        "prior_item_output_tokens": prior,
        "probe_cli": "claude",
    }


def _setup_document(tmp_path: Path, **kwargs: Any) -> dict[str, Any]:
    document = _kit(tmp_path, **kwargs)
    for key in ("owners", "independent_accounts"):
        document.pop(key)
    return document


def _inputs_file(tmp_path: Path, document: Mapping[str, Any]) -> Path:
    folder = _private(tmp_path / "inputs")
    return _file(folder / "two-owner.json", json.dumps(document).encode())


class _FakeClock(pp.Clock):
    """Waiting advances time; nothing sleeps."""

    def __init__(self, now: float = T0) -> None:
        self.current = now
        self.ticks = 0.0

    def now(self) -> float:
        return self.current

    def monotonic(self) -> float:
        return self.ticks

    def wait(self, seconds: float) -> None:
        self.current += max(0.0, seconds)
        self.ticks += max(0.0, seconds)


def _refusal(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    payload = json.loads(lines[-1])
    assert isinstance(payload, dict)
    return payload


def _image(revision: str) -> tuple[bytes, bytes]:
    """Docker manifest v2 schema 2 plus its config blob (distribution spec manifest-v2-2)."""

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


@contextlib.contextmanager
def _registry(images: Mapping[tuple[str, str], tuple[bytes, bytes]]) -> Iterator[int]:
    """Registry HTTP API v2 GETs for manifests (by tag or digest) and config blobs."""

    manifests = {key: value[0] for key, value in images.items()}
    blobs = {
        "sha256:" + hashlib.sha256(config).hexdigest(): config for _m, config in images.values()
    }

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def do_GET(self) -> None:  # noqa: N802 - http.server naming
            parts = self.path.split("/")
            body: bytes | None = None
            headers: dict[str, str] = {}
            if len(parts) == 5 and parts[1] == "v2" and parts[3] == "manifests":
                found = manifests.get((parts[2], parts[4]))
                accepted = [item.strip() for item in self.headers.get("Accept", "").split(",")]
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


def _candidate_registry(revision: str = CANDIDATE) -> dict[tuple[str, str], tuple[bytes, bytes]]:
    images = {(name, TAG): _image(revision) for name in pps.REQUIRED_IMAGES}
    dex = _image("dex-upstream")
    images[(pt.DEX_REPOSITORY, "sha256:" + hashlib.sha256(dex[0]).hexdigest())] = dex
    return images


def _record_dex(state: Path, *, digest: str) -> None:
    ledger = pto.TwoOwnerLedger.open(state, RUN_ID)
    ledger.ensure_run(CANDIDATE, phase="images")
    ledger.record(
        "images",
        "dex",
        {"source": pt.DEX_SOURCE, "repository": pt.DEX_REPOSITORY, "digest": digest, "tag": TAG},
    )
    ledger.close()


def _argv(command: str, inputs: Path | None, state: Path, *extra: str) -> list[str]:
    argv = [command, "--scenario", "two-owner"]
    if inputs is not None:
        argv += ["--inputs-file", str(inputs)]
    return [*argv, "--candidate", CANDIDATE, "--state-dir", str(state), "--run-id", RUN_ID, *extra]


def _main(
    argv: list[str], capsys: pytest.CaptureFixture[str], environ: Mapping[str, str] = PATH_ONLY
) -> tuple[int, dict[str, Any]]:
    code = pp.main(argv, environ=dict(environ), clock=_FakeClock())
    return code, _refusal(capsys)


APPROVE = ("--approve-live", pto.approved_ceiling())


def _rehearsal_ledger(state: Path, *, drop: str | None = None) -> None:
    ledger = pto.TwoOwnerLedger.open(state, RUN_ID)
    ledger.ensure_run(CANDIDATE, phase="rehearse")
    ledger.mark_rehearsal()
    for key in pto.REHEARSAL_EXPECTED:
        if key != drop:
            ledger.record("rehearsal", key, 403 if key == "card_unlisted_status" else True)
    ledger.close()


def _judge_argv(state: Path, criterion: str, candidate: str = CANDIDATE) -> list[str]:
    return [
        "judge",
        "--scenario",
        "two-owner",
        "--candidate",
        candidate,
        "--state-dir",
        str(state),
        "--run-id",
        RUN_ID,
        "--criterion",
        criterion,
    ]
