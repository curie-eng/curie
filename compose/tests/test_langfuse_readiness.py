"""Command controls for startup retry and readiness, separate from real proof."""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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

def command(call):
    if not call or call[0] != "compose":
        raise SystemExit(97)
    index = 1
    while index < len(call) and call[index] in {"-p", "-f"}:
        index += 2
    return call[index:]

current = command(args)
prior = [command(call) for call in calls[:-1]]
backing = ["postgres", "valkey", "clickhouse", "rustfs"]
if current == ["wait", "rustfs-init"]:
    # Compose 2.24.4 includes wait; it propagates the container exit status:
    # https://github.com/docker/compose/blob/v2.24.4/cmd/compose/wait.go
    raise SystemExit(23 if scenario == "bucket-failure" else 0)
elif current and current[0] == "up":
    if current == ["up", "-d", *backing, "rustfs-init"]:
        pass
    elif current[:4] == ["up", "-d", "--wait", "--wait-timeout"]:
        if len(current) < 6 or not current[4].isdigit() or int(current[4]) <= 0:
            raise SystemExit(97)
        if current[5:] not in [backing, ["langfuse-worker", "otel-collector"]]:
            raise SystemExit(97)
    elif current not in [
        ["up", "-d", "--no-deps", "langfuse-web"],
        ["up", "-d", "--no-deps", "--force-recreate", "langfuse-web"],
    ]:
        raise SystemExit(97)
    if any(service in current for service in ["langfuse-web", "langfuse-worker"]):
        if ["wait", "rustfs-init"] not in prior:
            raise SystemExit(96)
elif current[:4] == ["ps", "--all", "--format", "json"]:
    service = current[-1]
    if service not in {"langfuse-web", "langfuse-worker"} or len(current) != 5:
        raise SystemExit(97)
    failed = service == "langfuse-web" and (
        scenario in {"repeated-deadlock", "migration-failure"}
        or (scenario == "deadlock-once" and retries == 0)
    )
    running = service != "langfuse-worker" or any(
        call[0] == "up" and "langfuse-worker" in call for call in prior
    )
    print(json.dumps([{"Service": service,
                      "State": "exited" if failed else "running" if running else "created",
                      "ExitCode": 1 if failed else 0}]))
elif current == ["logs", "--no-color", "--tail=200", "langfuse-web"]:
    if scenario == "migration-failure":
        print("Prisma migrate deploy failed: migration checksum mismatch")
    else:
        # PostgreSQL reports migration deadlock as SQLSTATE 40P01:
        # https://www.postgresql.org/docs/current/errcodes-appendix.html
        print("Prisma migrate deploy failed: ERROR deadlock detected SQLSTATE 40P01")
elif current[:5] == ["exec", "-T", "langfuse-worker", "node", "-e"]:
    if len(current) != 6 or "http://langfuse-worker:3030/api/ready" not in current[-1]:
        raise SystemExit(97)
    checks = sum("exec" in call for call in calls)
    raise SystemExit(0 if checks >= int(os.environ["FAKE_WORKER_READY_AFTER"]) else 1)
else:
    raise SystemExit(97)
'''


@pytest.fixture
def web(request: pytest.FixtureRequest) -> Iterator[tuple[str, list[tuple[str, str | None]]]]:
    reads: list[tuple[str, str | None]] = []
    expected_auth = "Basic " + base64.b64encode(b"example-public:example-secret").decode()
    refused_status = getattr(request, "param", None)
    assert refused_status in (None, 401, 403)

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
                if refused_status is not None:
                    status, body = refused_status, {"message": "Read refused"}
                elif self.headers.get("Authorization") == expected_auth:
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
    start: bool = True, timeout: float = 10,
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
                            check=False, timeout=30)
    calls: list[list[str]] = []
    if state.exists():
        with sqlite3.connect(state) as db:
            calls = [json.loads(row[0]) for row in db.execute("SELECT arguments FROM calls")]
    assert "example-secret" not in result.stdout + result.stderr
    return result, calls, files


@dataclass
class _Clock:
    elapsed: float = 0
    sleeps: list[float] = field(default_factory=list)

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        assert seconds > 0
        self.sleeps.append(seconds)
        self.elapsed += seconds


@dataclass
class _VirtualControl:
    helper: Any
    clock: _Clock
    calls: list[list[str]]
    budgets: list[float]
    reads: list[tuple[str, str | None]]
    worker_results: list[int]


def _virtual_control(
    monkeypatch: pytest.MonkeyPatch, *, worker_ready_after: int,
) -> _VirtualControl:
    specification = importlib.util.spec_from_file_location("readiness_control", READINESS)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    clock = _Clock()
    calls: list[list[str]] = []
    budgets: list[float] = []
    reads: list[tuple[str, str | None]] = []
    worker_results: list[int] = []
    project = "curie-check-3864-example"
    files = [str(REPO_ROOT / "compose.dev.yaml")]
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)
    monkeypatch.setenv("COMPOSE_FILE", os.pathsep.join(files))
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "example-public")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "example-secret")
    expected_auth = "Basic " + base64.b64encode(b"example-public:example-secret").decode()

    def docker(
        arguments: list[str], *, capture_output: bool, text: bool, check: bool, timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output and text and not check
        assert 0 < timeout <= 0.5
        prefix = ["docker", "compose", "-p", project, "-f", files[0]]
        assert arguments[:len(prefix)] == prefix
        command = arguments[len(prefix):]
        calls.append(command)
        budgets.append(timeout)
        if command[0] == "ps":
            assert command[:4] == ["ps", "--all", "--format", "json"]
            assert command[-1] in {"langfuse-web", "langfuse-worker"}
            body = json.dumps([{"Service": command[-1], "State": "running", "ExitCode": 0}])
            return subprocess.CompletedProcess(arguments, 0, stdout=body, stderr="")
        assert command[:5] == ["exec", "-T", "langfuse-worker", "node", "-e"]
        assert "http://langfuse-worker:3030/api/ready" in command[-1]
        exit_code = 0 if len(worker_results) + 1 >= worker_ready_after else 1
        worker_results.append(exit_code)
        return subprocess.CompletedProcess(arguments, exit_code, stdout="", stderr="")

    def urlopen(request: urllib.request.Request, *, timeout: float) -> io.BytesIO:
        assert 0 < timeout <= 0.5
        assert request.full_url.startswith("http://example.com/api/public/")
        authorization = request.get_header("Authorization")
        reads.append((request.selector, authorization))
        if request.selector == "/api/public/health":
            assert authorization is None
            body: dict[str, object] = {"status": "OK"}
        else:
            assert request.selector == "/api/public/traces?limit=1"
            assert authorization == expected_auth
            # Use the authenticated trace list API's actual response shape:
            # https://api.reference.langfuse.com/api-reference/trace/list
            body = {"data": [], "meta": {"page": 1, "limit": 1}}
        return io.BytesIO(json.dumps(body).encode())

    # Replace only the external command, HTTP and clock boundaries. The real
    # helper still parses state, authenticates the read, probes the worker,
    # derives remaining budgets and refuses at its deadline.
    monkeypatch.setattr(module, "time", clock)
    monkeypatch.setattr(module, "subprocess", SimpleNamespace(
        run=docker, TimeoutExpired=subprocess.TimeoutExpired,
    ))
    monkeypatch.setattr(module, "urllib", SimpleNamespace(
        error=urllib.error,
        request=SimpleNamespace(Request=urllib.request.Request, urlopen=urlopen),
    ))
    helper = module.Readiness(0.5, 0.2, "http://example.com")
    return _VirtualControl(helper, clock, calls, budgets, reads, worker_results)


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
    bucket_waits = [index for index, call in enumerate(calls)
                    if call[-2:] == ["wait", "rustfs-init"]]
    web_starts = [index for index, call in enumerate(calls)
                  if "up" in call and call[-1] == "langfuse-web"]
    worker_starts = [index for index, call in enumerate(calls)
                     if "up" in call and "langfuse-worker" in call]
    worker_probes = [index for index, call in enumerate(calls) if "exec" in call]
    assert len(bucket_waits) == len(web_starts) == len(worker_starts) == 1
    assert bucket_waits[0] < web_starts[0] < worker_starts[0] < worker_probes[0]


def test_readiness_failed_bucket_initialization_stops_web_and_worker_startup(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario="bucket-failure")

    assert result.returncode == 1
    assert len([call for call in calls if call[-2:] == ["wait", "rustfs-init"]]) == 1
    assert not any("up" in call and "langfuse-web" in call for call in calls)
    assert not any("up" in call and "langfuse-worker" in call for call in calls)
    assert not any("--force-recreate" in call or "exec" in call for call in calls)
    assert web[1] == []
    assert result.stderr.strip() == (
        "Langfuse readiness Compose command failed (phase=bucket initialization, exit code=23)"
    )


@pytest.mark.parametrize("web", [401, 403], indirect=True)
def test_readiness_authenticated_read_refusal_stops_worker_startup_without_retry(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario="healthy")

    assert result.returncode == 1
    assert result.stderr.strip() == "Langfuse readiness authenticated read was refused"
    expected_auth = "Basic " + base64.b64encode(b"example-public:example-secret").decode()
    assert web[1] == [
        ("/api/public/health", None), ("/api/public/traces?limit=1", expected_auth),
    ]
    assert len([call for call in calls if "ps" in call and call[-1] == "langfuse-web"]) == 1
    assert not any("--force-recreate" in call or "logs" in call for call in calls)
    assert not any("up" in call and "langfuse-worker" in call for call in calls)
    assert not any("exec" in call for call in calls)


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
    assert any("logs" in call and call[-1] == "langfuse-web" for call in calls)
    assert result.stderr.strip() == "Langfuse web exited before migration readiness"


def test_readiness_check_only_never_restarts_exited_web(
    tmp_path: Path, web: tuple[str, list[tuple[str, str | None]]],
) -> None:
    result, calls, _ = _run(tmp_path, web[0], scenario="deadlock-once", start=False)

    assert result.returncode == 1
    assert not any("up" in call or "restart" in call for call in calls)
    assert any("logs" in call and call[-1] == "langfuse-web" for call in calls)
    assert result.stderr.strip() == "Langfuse web exited before migration readiness"


def test_readiness_pending_worker_fails_within_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = _virtual_control(monkeypatch, worker_ready_after=10000)
    control.helper.wait_web(retry_deadlock=False)

    with pytest.raises(TimeoutError, match="^Langfuse readiness deadline expired$"):
        control.helper.wait_worker()

    assert [path for path, _ in control.reads] == [
        "/api/public/health", "/api/public/traces?limit=1",
    ]
    assert control.worker_results == [1, 1, 1]
    assert [call[-1] for call in control.calls if call[0] == "ps"] == [
        "langfuse-web", "langfuse-worker", "langfuse-worker", "langfuse-worker",
    ]
    probe_budgets = [
        budget for call, budget in zip(control.calls, control.budgets, strict=True)
        if call[0] == "exec"
    ]
    assert probe_budgets == pytest.approx([0.5, 0.3, 0.1])
    assert control.clock.sleeps == pytest.approx([0.2, 0.2, 0.1])
    assert control.clock.elapsed == pytest.approx(0.5)


def test_readiness_pending_worker_recovers_before_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = _virtual_control(monkeypatch, worker_ready_after=3)
    control.helper.wait_web(retry_deadlock=False)

    control.helper.wait_worker()

    assert control.worker_results == [1, 1, 0]
    probe_budgets = [
        budget for call, budget in zip(control.calls, control.budgets, strict=True)
        if call[0] == "exec"
    ]
    assert probe_budgets == pytest.approx([0.5, 0.3, 0.1])
    assert control.clock.sleeps == pytest.approx([0.2, 0.2])
    assert control.clock.elapsed == pytest.approx(0.4)
    assert control.helper.remaining() == pytest.approx(0.1)
