"""Third party library warnings leave a bootstrapped process as redacted JSON.

The api lifespan is not spawned because it needs Valkey and Postgres before
bootstrap, and #2536 owns uvicorn.access. The four entrypoints below call the
same ``bootstrap_service_telemetry`` the api lifespan calls. No api test here
starts uvicorn.

Only a synthetic channel token appears. The driver is a real process with a
closed environment, so an outer ``OTEL_*`` variable cannot choose the path.
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

DRIVER_PATH = Path(__file__).with_name("_library_log_process.py")

# Duplicated from the driver rather than imported. This suite has no conftest
# putting its directory on sys.path, and the root run uses
# --import-mode=importlib, so a sibling module is not importable by bare name.
PLANTED_CHANNEL_TOKEN = "chn.not-a-real-payload-2535.not-a-real-signature-2535"
REDACTION_MARKER = "[REDACTED:channel_token]"
WARNING_CARRIER = "library warning probe 2535"
INFO_CARRIER = "library info probe 2535"
SERVICE_CARRIER = "service logger probe 2535"

_ENTRYPOINTS = ("mail", "dispatcher", "worker", "runner")


def wait_until(predicate: object, timeout: float, message: str) -> None:
    """Poll instead of sleeping a fixed interval, and fail on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out after {timeout}s waiting for: {message}")


class OtlpReceiver:
    """A local HTTP OTLP endpoint that keeps every request body verbatim."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, bytes, dict[str, str]]] = []
        self._lock = threading.Lock()

    def record(self, path: str, body: bytes, headers: dict[str, str]) -> None:
        with self._lock:
            self.requests.append((path, body, headers))

    def paths(self) -> list[str]:
        with self._lock:
            return [path for path, _, _ in self.requests]

    def bodies(self, path: str) -> list[bytes]:
        with self._lock:
            return [body for request_path, body, _ in self.requests if request_path == path]

    def all_bytes(self) -> bytes:
        with self._lock:
            return b"".join(body for _, body, _ in self.requests)

    def log_requests(self) -> list[ExportLogsServiceRequest]:
        """Decode each ``/v1/logs`` body as ``ExportLogsServiceRequest``."""
        decoded = []
        for body in self.bodies("/v1/logs"):
            request = ExportLogsServiceRequest()
            request.ParseFromString(body)
            decoded.append(request)
        return decoded


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        if (self.headers.get("Content-Encoding") or "").lower() == "gzip":
            body = gzip.decompress(body)
        self.server.receiver.record(  # type: ignore[attr-defined]
            self.path, body, {key.lower(): value for key, value in self.headers.items()}
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args: object) -> None:
        """Drop the default access line so it does not mix into pytest output."""


@pytest.fixture
def otlp() -> Iterator[tuple[OtlpReceiver, str]]:
    receiver = OtlpReceiver()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.receiver = receiver  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield receiver, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def driver_env(scenario: str, **extra: str) -> dict[str, str]:
    """Closed child environment: PATH plus the scenario and planted secret.

    Built explicitly rather than copied from ``os.environ``, so a parent
    ``OTEL_*`` variable cannot select the export path.
    """
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "CURIE_LIBRARY_LOG_SCENARIO": scenario,
        "CURIE_LIBRARY_LOG_PLANTED_SECRET": PLANTED_CHANNEL_TOKEN,
    }
    env.update(extra)
    return env


def run_driver(env: dict[str, str], *, timeout: float = 120.0) -> tuple[int, str]:
    """Run the driver to completion and return its exit code and merged output."""
    process = subprocess.run(
        [sys.executable, str(DRIVER_PATH)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )
    return process.returncode, process.stdout


def json_records(output: str) -> list[dict[str, object]]:
    """Every non-blank merged line, parsed as one JSON object."""
    records: list[dict[str, object]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            raise AssertionError(
                f"a line of process output was not JSON:\n{line!r}\n\nfull output:\n{output}"
            ) from None
        assert isinstance(record, dict), f"a log line was not a JSON object: {line!r}"
        records.append(record)
    return records


def assert_redacted_process(output: str) -> None:
    """JSON lines, then carrier, absent token, marker, absent INFO, one service line."""
    json_records(output)
    assert WARNING_CARRIER in output, (
        f"the library warning was never emitted, so later redaction checks would "
        f"pass vacuously:\n{output}"
    )
    assert PLANTED_CHANNEL_TOKEN not in output, (
        f"the planted channel token survived into process output:\n{output}"
    )
    assert REDACTION_MARKER in output, (
        f"process output has no channel_token placeholder:\n{output}"
    )
    assert INFO_CARRIER not in output, (
        f"the library INFO record was emitted despite the WARNING backstop:\n{output}"
    )
    service_lines = [line for line in output.splitlines() if SERVICE_CARRIER in line]
    assert len(service_lines) == 1, (
        f"the service INFO record should appear on exactly one line, saw "
        f"{len(service_lines)}:\n{output}"
    )


def assert_carrier_then_secret_then_marker(text: str, *, where: str) -> None:
    """Carrier present, raw token absent, redaction marker present, in that order."""
    assert WARNING_CARRIER in text, (
        f"the planted warning never reached {where}, so the secret checks would pass vacuously"
    )
    assert PLANTED_CHANNEL_TOKEN not in text, f"the planted channel token survived into {where}"
    assert REDACTION_MARKER in text, f"{where} carries no channel_token placeholder"


def wait_for_log_request(receiver: OtlpReceiver, *, where: str) -> None:
    wait_until(lambda: "/v1/logs" in receiver.paths(), 10.0, where)


@pytest.mark.parametrize("scenario", _ENTRYPOINTS)
def test_entrypoint_redacts_a_library_warning_and_emits_service_info_once(scenario: str) -> None:
    """A new library warning is redacted JSON, and the service INFO line is not doubled."""
    code, output = run_driver(driver_env(scenario))
    assert code == 0, f"{scenario} did not exit 0; output:\n{output}"
    assert_redacted_process(output)


def test_mail_adapter_exports_the_library_warning_redacted(
    otlp: tuple[OtlpReceiver, str],
) -> None:
    """An HTTP protobuf collector receives the warning redacted, and stderr stays JSON."""
    receiver, base_url = otlp
    env = driver_env(
        "mail",
        OTEL_EXPORTER_OTLP_ENDPOINT=base_url,
        OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf",
    )

    code, output = run_driver(env)
    assert code == 0, f"the mail adapter did not exit 0; output:\n{output}"
    assert_redacted_process(output)

    wait_for_log_request(
        receiver,
        where="at least one /v1/logs request from the mail adapter. "
        f"Process output:\n{output}",
    )
    assert receiver.log_requests(), "a /v1/logs body was not an ExportLogsServiceRequest"
    exported = receiver.all_bytes().decode("utf-8", errors="replace")
    assert_carrier_then_secret_then_marker(exported, where="the collector's raw log bytes")


def test_mail_adapter_without_otlp_exports_nothing() -> None:
    """No OTEL endpoint means no export, including to the SDK HTTP default."""
    receiver = OtlpReceiver()
    # 4318 is the SDK HTTP default, so a fallback export would hit this listener.
    # The child is not told the URL.
    server = ThreadingHTTPServer(("127.0.0.1", 4318), _Handler)
    server.receiver = receiver  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = driver_env("mail")
        assert not [name for name in env if name.startswith("OTEL_")], (
            "the closed environment contained an OTEL_* key, so this is not the unset path"
        )

        code, output = run_driver(env)
        assert code == 0, f"the mail adapter failed with no OTLP endpoint:\n{output}"
        assert_redacted_process(output)
        assert receiver.requests == [], (
            f"the process contacted the SDK default collector on 4318: {receiver.paths()}"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_unconfigured_library_warning_keeps_the_raw_token_and_is_not_json() -> None:
    """Without bootstrap the same warning is plain text and still contains the token."""
    code, output = run_driver(driver_env("unconfigured"))
    assert code == 0, f"the unconfigured driver did not exit 0; output:\n{output}"
    assert WARNING_CARRIER in output, f"the library warning was never emitted:\n{output}"
    assert PLANTED_CHANNEL_TOKEN in output, (
        f"the raw token was absent without bootstrap, so the leak is not observable:\n{output}"
    )
    offending = [line for line in output.splitlines() if PLANTED_CHANNEL_TOKEN in line]
    assert offending, f"no line carried the raw token:\n{output}"
    for line in offending:
        try:
            json.loads(line)
        except ValueError:
            continue
        raise AssertionError(
            f"the line carrying the raw token was valid JSON, so the negative missed:\n{line!r}"
        )
