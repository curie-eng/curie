"""Real API requests leave the shipped entrypoint as redacted access logs (#2536)."""

from __future__ import annotations

import gzip
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from curie_api.config import get_settings
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

from .identity_fence_support import fence_child_env

CANARY = "synthetic-access-log-canary-2536"
REDACTION_MARKER = "[REDACTED:url_secret_param]"


@dataclass
class ApiProcess:
    process: subprocess.Popen[str]
    url: str
    output: tuple[str, None] | None = None
    missing_entrypoint: bool = False
    forced_kill: bool = False

    def stop(self) -> tuple[str, None]:
        if self.output is None:
            if self.process.poll() is None:
                self.process.terminate()
            try:
                self.output = self.process.communicate(timeout=20)
            except subprocess.TimeoutExpired:
                self.forced_kill = True
                self.process.kill()
                self.output = self.process.communicate(timeout=5)
        return self.output


def _start_api(
    request: pytest.FixtureRequest,
    cwd: Path,
    extra_env: dict[str, str],
    *,
    factory: str | None = None,
) -> ApiProcess:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    settings = get_settings()
    # Only the driver's actual backing-service settings cross into the child.
    # The empty working directory also prevents Settings from reading a .env.
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONUNBUFFERED": "1",
        "DATABASE_URL": settings.database_url,
        "DB_SCHEMA": settings.db_schema,
        "VALKEY_URL": settings.valkey_dsn(),
        "S3_ENDPOINT_URL": settings.s3_endpoint_url,
        "S3_ACCESS_KEY": settings.s3_access_key,
        "S3_SECRET_KEY": settings.s3_secret_key,
        "S3_REGION": settings.s3_region,
        "BUNDLE_BUCKET": settings.bundle_bucket,
        "ENVIRONMENT": "dev",
        "API_KEY": "access-log-test-key",
        "LOG_LEVEL": "INFO",
        "FORWARDED_ALLOW_IPS": "",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
    }
    env.update(extra_env)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = int(reservation.getsockname()[1])
    command = [sys.executable, "-m", "curie_api"]
    if factory is not None:
        command = [sys.executable, "-m", "uvicorn", factory, "--factory"]
    process = subprocess.Popen(
        [*command, "--host", "127.0.0.1", "--port", str(port)],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    api = ApiProcess(process, f"http://127.0.0.1:{port}")

    def cleanup() -> None:
        api.stop()
        if api.forced_kill:
            raise RuntimeError("the API process required SIGKILL during cleanup")

    request.addfinalizer(cleanup)
    deadline = time.monotonic() + 60
    with httpx.Client(base_url=api.url, timeout=1, trust_env=False) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                stdout, _ = api.stop()
                if "No module named curie_api.__main__" in stdout:
                    # This product regression must be a test assertion failure,
                    # rather than an unavailable-service error on the base ref.
                    api.missing_entrypoint = True
                    return api
                raise RuntimeError(f"the API exited before readiness: {process.returncode}")
            try:
                if client.get("/health").status_code == 200:
                    return api
            except httpx.TransportError:
                pass
            time.sleep(0.05)
    raise RuntimeError("the API process did not become healthy within 60 seconds")


@pytest.fixture
def plain_api(migrated: None, request: pytest.FixtureRequest, tmp_path: Path) -> ApiProcess:
    return _start_api(request, tmp_path, {})


@pytest.fixture
def namespaced_identity_api(
    migrated: None,
    identity_fence_key: str,
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> ApiProcess:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
    return _start_api(
        request,
        tmp_path,
        fence_child_env({}, identity_fence_key),
        factory="identity_fence_support:create_namespaced_app",
    )


class OtlpReceiver:
    def __init__(self) -> None:
        self.requests: list[tuple[str, bytes, bytes]] = []
        self.lock = threading.Lock()

    def record(self, path: str, raw: bytes, decoded: bytes) -> None:
        with self.lock:
            self.requests.append((path, raw, decoded))

    def snapshot(self) -> list[tuple[str, bytes, bytes]]:
        with self.lock:
            return list(self.requests)


class _OtlpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        body = (
            gzip.decompress(raw)
            if (self.headers.get("Content-Encoding") or "").lower() == "gzip"
            else raw
        )
        self.server.receiver.record(self.path, raw, body)  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args: object) -> None:
        """Keep the receiver's own access lines out of the process evidence."""


@pytest.fixture
def otlp() -> Iterator[tuple[OtlpReceiver, str]]:
    receiver = OtlpReceiver()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OtlpHandler)
    server.receiver = receiver  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    try:
        thread.start()
        yield receiver, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def exported_api(
    migrated: None,
    request: pytest.FixtureRequest,
    tmp_path: Path,
    otlp: tuple[OtlpReceiver, str],
) -> ApiProcess:
    return _start_api(
        request,
        tmp_path,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": otlp[1],
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        },
    )


def _assert_access_output(api: ApiProcess) -> None:
    stdout, _ = api.stop()
    assert CANARY not in stdout, "the synthetic query secret reached process output"
    records = []
    for line in stdout.splitlines():
        if line.strip():
            record = json.loads(line)
            assert isinstance(record, dict), "a process log was not a JSON object"
            records.append(record)
    access = [record for record in records if record.get("logger") == "uvicorn.access"]
    assert access, "the real requests emitted no uvicorn.access JSON records"
    assert all(record.get("service.name") == "curie-api" for record in access)
    messages = [str(record.get("message")) for record in access]
    for path in ("/health", "/access-redaction-missing"):
        assert sum(path + REDACTION_MARKER in message for message in messages) == 1
    assert any("/health?probe=ordinary" in message for message in messages)


def _request_access_probes(api: ApiProcess) -> None:
    assert not api.missing_entrypoint, "python -m curie_api has no executable entrypoint"
    with httpx.Client(base_url=api.url, timeout=5, trust_env=False) as client:
        health = client.get("/health", params={"token": CANARY})
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}
        assert client.get("/access-redaction-missing", params={"token": CANARY}).status_code == 404
        assert client.get("/health", params={"probe": "ordinary"}).status_code == 200


def test_access_log_leaves_the_real_entrypoint_as_redacted_json(plain_api: ApiProcess) -> None:
    _request_access_probes(plain_api)
    _assert_access_output(plain_api)


def test_access_log_is_exported_redacted(
    exported_api: ApiProcess, otlp: tuple[OtlpReceiver, str]
) -> None:
    _request_access_probes(exported_api)
    _assert_access_output(exported_api)
    requests = otlp[0].snapshot()
    assert requests, "the real API process sent nothing to the OTLP receiver"
    assert all(CANARY.encode() not in raw + body for _, raw, body in requests)
    exported_access = []
    for path, _, body in requests:
        if path != "/v1/logs":
            continue
        export = ExportLogsServiceRequest()
        export.ParseFromString(body)
        for resource in export.resource_logs:
            services = [
                attr.value.string_value
                for attr in resource.resource.attributes
                if attr.key == "service.name"
            ]
            for scope in resource.scope_logs:
                if scope.scope.name == "uvicorn.access":
                    for record in scope.log_records:
                        assert services == ["curie-api"]
                        exported_access.append(record.body.string_value)
    assert exported_access, "no uvicorn.access log record was exported"
    # Safe dynamic arguments may be omitted from OTLP bodies by shared policy.
    assert sum(REDACTION_MARKER in body for body in exported_access) == 2


def test_sender_connect_url_is_redacted_at_real_entrypoint(plain_api: ApiProcess) -> None:
    token = "a" * 43
    with httpx.Client(base_url=plain_api.url, timeout=5, trust_env=False) as client:
        response = client.get(f"/identity/connect/{token}")
        assert response.status_code in (400, 404)
    stdout, _ = plain_api.stop()
    assert token not in stdout
    assert "/identity/connect/[REDACTED:identity_connect_token]" in stdout


def test_oidc_callback_query_is_redacted_at_real_entrypoint(plain_api: ApiProcess) -> None:
    with httpx.Client(base_url=plain_api.url, timeout=5, trust_env=False) as client:
        response = client.get("/console/oidc/callback", params={"code": CANARY, "state": CANARY})
        assert response.status_code == 404
    stdout, _ = plain_api.stop()
    assert CANARY not in stdout
    assert "/console/oidc/callback?[REDACTED:oidc_callback_query]" in stdout
