"""Command controls for startup retry and readiness, separate from real proof."""

from __future__ import annotations

import base64
import json
import os
import sqlite3
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
READINESS = REPO_ROOT / "scripts" / "wait-for-langfuse.py"

FAKE_DOCKER = '''#!/usr/bin/env python3
import json, os, sqlite3, sys

db = sqlite3.connect(os.environ["FAKE_DOCKER_STATE"])
db.execute("CREATE TABLE IF NOT EXISTS calls (arguments TEXT)")
db.execute("INSERT INTO calls VALUES (?)", (json.dumps(sys.argv[1:]),))
db.commit()
calls = [json.loads(row[0]) for row in db.execute("SELECT arguments FROM calls")]
args = sys.argv[1:]
scenario = os.environ["FAKE_DOCKER_SCENARIO"]
retries = sum("--force-recreate" in call for call in calls)
if "ps" in args:
    service = args[-1]
    failed = service == "langfuse-web" and (
        scenario in {"repeated-deadlock", "migration-failure"}
        or (scenario == "deadlock-once" and retries == 0)
    )
    print(json.dumps([{"Service": service,
                      "State": "exited" if failed else "running",
                      "ExitCode": 1 if failed else 0}]))
elif "logs" in args:
    if scenario == "migration-failure":
        print("Prisma migrate deploy failed: migration checksum mismatch")
    else:
        # PostgreSQL reports migration deadlock as SQLSTATE 40P01:
        # https://www.postgresql.org/docs/current/errcodes-appendix.html
        print("Prisma migrate deploy failed: ERROR deadlock detected SQLSTATE 40P01")
elif "exec" in args:
    checks = sum("exec" in call for call in calls)
    raise SystemExit(0 if checks >= int(os.environ["FAKE_WORKER_READY_AFTER"]) else 1)
'''


@pytest.fixture
def web() -> Iterator[tuple[str, list[tuple[str, str | None]]]]:
    reads: list[tuple[str, str | None]] = []
    expected_auth = "Basic " + base64.b64encode(b"example-public:example-secret").decode()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            reads.append((self.path, self.headers.get("Authorization")))
            # Pinned 3.225.5 web entrypoint observed by the driver executes
            # Prisma and ClickHouse migrations before starting this server.
            # The public read API uses Basic Auth and a data array:
            # https://api.reference.langfuse.com/api-reference/trace/list
            if self.path == "/api/public/health":
                status, body = 200, {"status": "OK"}
            elif self.path.startswith("/api/public/traces?"):
                if self.headers.get("Authorization") == expected_auth:
                    status, body = 200, {"data": [], "meta": {"page": 1, "limit": 1}}
                else:
                    status, body = 401, {"message": "Unauthorized"}
            else:
                status, body = 404, {"message": "Not found"}
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", reads
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _run(
    tmp_path: Path, web_url: str, *, scenario: str, worker_ready_after: int = 1,
    start: bool = True, timeout: float = 3,
) -> tuple[subprocess.CompletedProcess[str], list[list[str]], list[str]]:
    fake_docker = tmp_path / "docker"
    fake_docker.write_text(FAKE_DOCKER)
    fake_docker.chmod(0o700)
    state = tmp_path / "command-state.sqlite"
    override = tmp_path / "compose.yaml"
    override.write_text("services: {}\n")
    files = [str(REPO_ROOT / "compose.dev.yaml"), str(override)]
    environment = {
        **os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "COMPOSE_PROJECT_NAME": "curie-check-3864-example", "COMPOSE_FILE": ":".join(files),
        "LANGFUSE_PUBLIC_KEY": "example-public", "LANGFUSE_SECRET_KEY": "example-secret",
        "FAKE_DOCKER_STATE": str(state), "FAKE_DOCKER_SCENARIO": scenario,
        "FAKE_WORKER_READY_AFTER": str(worker_ready_after),
    }
    command = [sys.executable, str(READINESS), "--web-url", web_url,
               "--timeout-seconds", str(timeout), "--poll-interval-seconds", "0.01"]
    if start:
        command.append("--start")
    result = subprocess.run(command, env=environment, text=True, capture_output=True,
                            check=False, timeout=10)
    calls: list[list[str]] = []
    if state.exists():
        with sqlite3.connect(state) as db:
            calls = [json.loads(row[0]) for row in db.execute("SELECT arguments FROM calls")]
    assert "example-secret" not in result.stdout + result.stderr
    return result, calls, files


def test_readiness_waits_for_worker_and_keeps_exact_compose_contract(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, files = _run(tmp_path, web[0], scenario="healthy", worker_ready_after=3)

    assert result.returncode == 0, result.stderr
    assert len([call for call in calls if "exec" in call]) == 3
    assert any(path.startswith("/api/public/traces?") and auth for path, auth in web[1])
    assert all(call[:7] == ["compose", "-p", "curie-check-3864-example",
                           "-f", files[0], "-f", files[1]] for call in calls)
    worker_checks = [call for call in calls if "exec" in call]
    # The driver observed pinned 3.225.5 bind to env.HOSTNAME, whose container
    # address serves readiness while loopback refuses connections. Probe the
    # Compose service DNS name on the observed port and path.
    assert all("http://langfuse-worker:3030/api/ready" in call[-1] for call in worker_checks)
    assert not any("--force-recreate" in call for call in calls)


def test_readiness_retries_migration_deadlock_exactly_once(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario="deadlock-once")

    assert result.returncode == 0, result.stderr
    retries = [call for call in calls if "--force-recreate" in call]
    assert len(retries) == 1
    assert retries[0][-1] == "langfuse-web"
    assert "--no-deps" in retries[0]


@pytest.mark.parametrize("scenario,retries", [("repeated-deadlock", 1),
                                             ("migration-failure", 0)])
def test_readiness_terminal_migration_failure_does_not_start_worker(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
    scenario: str, retries: int,
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario=scenario)

    assert result.returncode == 1
    assert len([call for call in calls if "--force-recreate" in call]) == retries
    assert not any("up" in call and "langfuse-worker" in call for call in calls)
    assert "migration" in result.stderr.lower() or "exited" in result.stderr.lower()


def test_readiness_check_only_never_restarts_exited_web(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario="deadlock-once", start=False)

    assert result.returncode == 1
    assert not any("up" in call or "restart" in call for call in calls)


def test_readiness_pending_worker_fails_within_bound(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario="healthy", worker_ready_after=10000,
                            start=False, timeout=0.5)

    assert result.returncode == 1
    assert len([call for call in calls if "exec" in call]) >= 1
    assert "deadline" in result.stderr.lower() or "ready" in result.stderr.lower()
