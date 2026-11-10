"""Exercise the ladder's OTel file transfer through the real Docker daemon."""

from __future__ import annotations

import json
import os
import re
import selectors
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
LADDER = REPO / "cli/scripts/e2e-ladder.sh"
OWNER_LABEL = "curietech.ai/e2e-owner"


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=45, check=False
    )
    if check and result.returncode:
        raise RuntimeError(f"Docker {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")
    return result


def _otel_functions(source: str) -> str:
    start = re.search(r"^start_local_otel_sink\(\) \{", source, re.MULTILINE)
    end = re.search(r"^assert_local_otel_no_turn_pipeline_live\(\) \{", source, re.MULTILINE)
    if start is None or end is None or start.start() >= end.start():
        raise RuntimeError("Could not extract the ladder's OTel lifecycle and query functions")
    return source[start.start() : end.start()]


class LadderOtelSink:
    def __init__(self, workdir: Path) -> None:
        self.workdir = workdir
        self.name = f"curie-check-remote-files-{uuid.uuid4().hex[:12]}"
        self.network = f"{self.name}-runner"
        self.marker = f"OTEL_SLICE_{uuid.uuid4().hex}"
        self.process: subprocess.Popen[bytes] | None = None
        self.buffer = b""
        self.log: list[str] = []
        self.volumes: set[str] = set()
        self.existing_volumes: set[str] = set()
        self.docker_ready = False

    def start(self) -> None:
        _docker("info", "--format", "{{.ServerVersion}}")
        self.docker_ready = True
        self.existing_volumes = set(_docker("volume", "ls", "--format", "{{.Name}}").stdout.split())
        source = LADDER.read_text(encoding="utf-8")
        initializers = "\n".join(re.findall(r"^LOCAL_OTEL_[A-Z_]+=.*$", source, re.MULTILINE))
        env = os.environ.copy()
        env.pop("STUB_STATE", None)
        env.update(
            {
                "WORKDIR": str(self.workdir),
                "REPO_ROOT": str(REPO),
                "COMPOSE_PROJECT": self.name,
                "CURIE_DOCKER_NETWORK": self.network,
                "OTEL_SLICE_NAME": self.name,
                "OTEL_SLICE_MARKER": self.marker,
                "PROMPT": "acme-export-probe",
                "OTEL_E2E_SECRET_SENTINEL": "acme-test-only-sentinel",
                "LIVE": "0",
            }
        )
        script = (
            "set -euo pipefail\n"
            + initializers
            + '\nLOCAL_OTEL_SINK_NAME="$OTEL_SLICE_NAME"\n'
            + _otel_functions(source)
            + r'''
trap stop_local_otel_sink EXIT
start_local_otel_sink
printf '%s 0\n' "$OTEL_SLICE_MARKER"
while IFS= read -r operation; do
    status=0
    case "$operation" in
        snapshot) local_otel_query snapshot || status=$? ;;
        zero) assert_local_otel_zero_export_control || status=$? ;;
        stop) exit 0 ;;
        *) echo "unknown OTel slice operation" >&2; status=2 ;;
    esac
    printf '%s %s\n' "$OTEL_SLICE_MARKER" "$status"
done
'''
        )
        self.process = subprocess.Popen(
            ["bash", "-c", script],
            cwd=self.workdir,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self._receive(timeout=180)
        inspected = json.loads(_docker("inspect", self.name).stdout)[0]
        self.volumes = {
            mount["Name"]
            for mount in inspected["Mounts"]
            if mount["Type"] == "volume" and mount["Name"] not in self.existing_volumes
        }

    def _receive(self, *, timeout: float = 30) -> tuple[int, str]:
        if self.process is None or self.process.stdout is None:
            raise RuntimeError("OTel slice shell has not started")
        lines: list[str] = []
        deadline = time.monotonic() + timeout
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            while True:
                while b"\n" in self.buffer:
                    raw, self.buffer = self.buffer.split(b"\n", 1)
                    line = raw.decode(errors="replace")
                    self.log.append(line)
                    if line.startswith(f"{self.marker} "):
                        return int(line.split()[-1]), "\n".join(lines)
                    lines.append(line)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(f"OTel slice shell timed out:\n{'\n'.join(self.log)}")
                if not selector.select(remaining):
                    continue
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError(
                        f"OTel slice shell exited before its response:\n{'\n'.join(self.log)}"
                    )
                self.buffer += chunk

    def command(self, operation: str) -> tuple[int, str]:
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("OTel slice shell has not started")
        self.process.stdin.write(f"{operation}\n".encode())
        self.process.stdin.flush()
        return self._receive()

    def snapshot(self) -> dict:
        status, output = self.command("snapshot")
        if status:
            raise RuntimeError(f"The ladder's local_otel_query failed:\n{output}")
        return json.loads(output.splitlines()[-1])

    def endpoint(self, port: int) -> str:
        mappings = _docker("port", self.name, f"{port}/tcp").stdout.splitlines()
        if len(mappings) != 1:
            raise RuntimeError(f"Expected one published OTel port: {mappings}")
        published = int(mappings[0].rsplit(":", 1)[1])
        host = os.environ.get("DOCKER_PUBLISHED_HOST", "127.0.0.1")
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{published}"

    def close(self) -> None:
        if not self.docker_ready:
            return
        errors: list[str] = []
        try:
            if self.process is not None:
                try:
                    output, _ = self.process.communicate(b"stop\n", timeout=45)
                    if output:
                        self.log.extend(output.decode(errors="replace").splitlines())
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.communicate(timeout=10)
                    errors.append("The ladder's stop_local_otel_sink did not finish")
        finally:
            containers = _docker(
                "ps", "-aq", "--filter", f"label={OWNER_LABEL}={self.name}"
            ).stdout.split()
            networks = _docker(
                "network", "ls", "-q", "--filter", f"label=com.docker.compose.project={self.name}"
            ).stdout.split()
            labeled_volumes = _docker(
                "volume", "ls", "-q", "--filter", f"label={OWNER_LABEL}={self.name}"
            ).stdout.split()
            volumes = sorted(
                volume
                for volume in self.volumes | set(labeled_volumes)
                if _docker("volume", "inspect", volume, check=False).returncode == 0
            )
            for kind, resources in (
                ("container", containers),
                ("network", networks),
                ("volume", volumes),
            ):
                if resources:
                    errors.append(f"The ladder left owned {kind} resources: {resources}")
                    args = ("rm", "-f") if kind == "container" else (kind, "rm")
                    _docker(*args, *resources)
            remaining_containers = _docker(
                "ps", "-aq", "--filter", f"label={OWNER_LABEL}={self.name}"
            ).stdout.strip()
            remaining_networks = _docker(
                "network", "ls", "-q", "--filter", f"label=com.docker.compose.project={self.name}"
            ).stdout.strip()
            if remaining_containers or remaining_networks:
                errors.append("Owned OTel containers or networks remain after cleanup")
            for volume in self.volumes | set(labeled_volumes):
                if _docker("volume", "inspect", volume, check=False).returncode == 0:
                    errors.append(f"Owned OTel volume remains after cleanup: {volume}")
        if errors:
            raise RuntimeError("\n".join(errors))


@pytest.fixture
def ladder_otel_sink(tmp_path: Path, request: pytest.FixtureRequest) -> LadderOtelSink:
    sink = LadderOtelSink(tmp_path)
    request.addfinalizer(sink.close)
    sink.start()
    return sink


def test_ladder_otel_sink_transfers_config_and_exports_without_shared_filesystem(
    ladder_otel_sink: LadderOtelSink,
    tmp_path: Path,
) -> None:
    sink = ladder_otel_sink
    assert sink.snapshot() == {"trace_ids": [], "metrics": []}
    status, output = sink.command("zero")
    assert status == 0, output

    # OTLP/HTTP JSON uses lowerCamelCase fields and decimal-string nanoseconds:
    # https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
    payload = {
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "acme-export-probe"}},
                        {"key": "service.instance.id", "value": {"stringValue": sink.name}},
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {"name": "acme-remote-files"},
                        "metrics": [
                            {
                                "name": "curie.remote.files.probe",
                                "gauge": {
                                    "dataPoints": [
                                        {"asInt": "7", "timeUnixNano": str(time.time_ns())}
                                    ]
                                },
                            }
                        ],
                    }
                ],
            }
        ]
    }
    request = urllib.request.Request(
        f"{sink.endpoint(4318)}/v1/metrics",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
        assert response.status == 200
        assert not json.loads(response.read()).get("partialSuccess"), "OTLP data was rejected"

    expected = {
        "name": "curie.remote.files.probe",
        "service": "acme-export-probe",
        "instance": sink.name,
        "attributes": {},
        "value": 7.0,
    }
    deadline = time.monotonic() + 30
    snapshot = sink.snapshot()
    while expected not in snapshot["metrics"] and time.monotonic() < deadline:
        time.sleep(0.2)
        snapshot = sink.snapshot()
    assert snapshot == {"trace_ids": [], "metrics": [expected]}

    exported = tmp_path / "copied-metrics.json"
    _docker("cp", f"{sink.name}:/var/lib/otel-e2e/metrics.json", str(exported))
    documents = [json.loads(line) for line in exported.read_text().splitlines() if line.strip()]
    resources = [resource for doc in documents for resource in doc["resourceMetrics"]]
    assert len(resources) == 1
    assert resources[0]["resource"]["attributes"] == payload["resourceMetrics"][0]["resource"][
        "attributes"
    ]
    metrics = [metric for scope in resources[0]["scopeMetrics"] for metric in scope["metrics"]]
    assert len(metrics) == 1
    assert metrics[0]["name"] == expected["name"]
    assert int(metrics[0]["gauge"]["dataPoints"][0]["asInt"]) == 7
    status, _ = sink.command("zero")
    assert status != 0, "The zero-export control accepted a sink that exported a metric"
