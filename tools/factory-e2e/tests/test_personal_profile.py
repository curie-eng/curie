"""Hermetic contract for the single-profile acceptance driver (#5375).

The live path needs a throwaway cluster, a dedicated personal grant and an
owned forge fixture, so it is proved by running ``personal_profile.py run``.
These tests pin what must hold before and after anything live: every unpaid
refusal through the real ``preflight`` entry, the judge's refusal of
incomplete evidence, the SQLite budget ledger, native transcript parsing, the
in-pod harvester program against a recorded-shape tree, secret scanning, and
the finalizer's behavior on failure, refusal and interruption.

Nothing here touches a cluster, GitHub, Docker or a network beyond a loopback
registry fixture, and no value is a real credential or identifier.
"""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import hashlib
import http.server
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import personal_profile as pp
import personal_profile_support as pps
import pytest

SCRIPT = Path(pp.__file__).resolve()
CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
GRANT = "synthetic-personal-grant-not-a-token"
REPO = "acme-corp/acme-fixture"
SESSION = "5d0c7f3e-0000-4000-8000-00000000a001"
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


@contextlib.contextmanager
def _registry(images: Mapping[tuple[str, str], tuple[bytes, bytes]]) -> Iterator[int]:
    """A loopback Registry HTTP API v2 fixture serving manifests by tag and config blobs."""

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
                accepted = self.headers.get("Accept", "")
                found = images.get((parts[2], parts[4]))
                if found and "manifest.v2+json" in accepted:
                    body = found[0]
                    headers = {
                        "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
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


def _candidate_images(revision: str = CANDIDATE) -> dict[tuple[str, str], tuple[bytes, bytes]]:
    return {(name, CANDIDATE[:12]): _image(revision) for name in pps.REQUIRED_IMAGES}


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
    mismatched[("curie-worker", CANDIDATE[:12])] = _image(OTHER_SHA)
    with _registry(mismatched) as port:
        base = _private(tmp_path / "mismatch")
        assert _refusal(_run_script(_command_argv("preflight", base, port), base)) == (
            "candidate_image_mismatch"
        )
    missing = _candidate_images()
    del missing[(pps.LAYER_IMAGE, CANDIDATE[:12])]
    with _registry(missing) as port:
        base = _private(tmp_path / "missing")
        assert _refusal(_run_script(_command_argv("preflight", base, port), base)) == (
            "candidate_image_mismatch"
        )


def test_preflight_reads_the_recorded_layer_ref(tmp_path: Path) -> None:
    images = _candidate_images()
    del images[(pps.LAYER_IMAGE, CANDIDATE[:12])]
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
        "published_workspace": {"hashes": {"NONCE.md": nonce_hash}, "nonce_paths": ["NONCE.md"]},
    }
    publication = {
        "rejected_remote_branches": 0,
        "rejected_prs": 0,
        "rejection_stays_rejected": True,
        "prs_for_prefix": 1,
        "pr_draft": False,
        "pr_head_prefix": True,
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
    )


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

    def query_inventory(self) -> dict[str, str]:
        self.events.append("inventory")
        return {kind: "absent" for kind in pps.INVENTORY_KINDS}

    def set_platform_budget(self, output_tokens: int) -> None:
        self.events.append(f"budget:{output_tokens}")


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
                        "resolution": {"kind": "slack", "address": pps.RESOLUTION_CHANNEL},
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


def test_rehearse_phase_cut_stops_after_forge_and_finalizes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    events: list[str] = []
    run = _scripted(tmp_path, _Rehearsal, events, stop_after=pp.REHEARSAL_CUT)
    capsys.readouterr()
    code = run.execute()
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert code == 0, summary
    assert summary["command"] == "rehearse" and summary["phase"] == "forge"
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
    }
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
