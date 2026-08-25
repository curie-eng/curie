"""Docker backed behavioral proof for the SRE bot observability connectors.

This is deliberately not a Kubernetes substitute. Chart rendering owns the
Kubernetes object contract. Here, the exact configured backends and real MCP
connector artifacts are driven over their network protocols so a green test
means a LogQL result and a Tempo span actually crossed the connector boundary.
"""

from __future__ import annotations

import base64
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import textwrap
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml
from aci_protocol import OtelConfig
from curie_runner import RunTracer, build_tracer_provider
from plugin_format.connector_render import host_aliases, object_name, service_dns

REPO_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY = REPO_ROOT / "examples" / "sre-bot" / "observability"
CONNECTORS = REPO_ROOT / "examples" / "sre-bot" / "connectors.yaml"
TEMPO_CONNECTOR = REPO_ROOT / "examples" / "sre-bot" / "connectors" / "tempo"

RELEASE = "curie"
AGENT = "sre-bot"
NAMESPACE = "curie"
CONNECTOR_PORT = 8000


MCP_PROBE = r"""
import json
import sys
import urllib.request

url, tool, raw_arguments = sys.argv[1:]
state = {"session": None, "version": "2024-11-05"}


def post(body, notification=False):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": state["version"],
    }
    if state["session"]:
        headers["mcp-session-id"] = state["session"]
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method="POST"
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        session = response.headers.get("mcp-session-id")
        if session:
            state["session"] = session
        raw = response.read().decode("utf-8", "replace")
    if notification:
        return None
    for line in raw.splitlines():
        if line.startswith("data:"):
            return json.loads(line[5:].strip())
    return json.loads(raw)


initialized = post(
    {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": state["version"],
            "capabilities": {},
            "clientInfo": {"name": "curie-observability-runtime", "version": "0"},
        },
    }
)
state["version"] = initialized["result"].get("protocolVersion", state["version"])
post({"jsonrpc": "2.0", "method": "notifications/initialized"}, notification=True)
response = post(
    {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": tool, "arguments": json.loads(raw_arguments)},
    }
)
print(json.dumps(response))
"""


def _run(
    command: list[str],
    *,
    input_text: str | None = None,
    environment: dict[str, str] | None = None,
    timeout: int = 180,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        input=input_text,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=None if environment is None else {**os.environ, **environment},
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def _docker(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return _run(["docker", *args], **kwargs)


def _require_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is unavailable: docker CLI is not installed")
    result = _docker("info", "--format", "{{.ServerVersion}}", check=False, timeout=15)
    if result.returncode != 0:
        reason = result.stderr.strip() or result.stdout.strip()
        pytest.skip(f"Docker is unavailable: {reason}")


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    assert isinstance(value, dict), f"{path} must contain one YAML mapping"
    return value


def _resolve(mapping: dict[str, Any], dotted: str) -> Any:
    value: Any = mapping
    for part in dotted.split("."):
        value = value[part]
    return value


def _render_loki_config(values: dict[str, Any]) -> str:
    """Resolve the small Helm expression subset in the committed Loki config."""

    loki = values["loki"]
    rendered = str(loki["config"])
    rendered = rendered.replace(
        "{{ .Values.loki.auth_enabled }}",
        str(bool(loki["auth_enabled"])).lower(),
    )
    rendered = rendered.replace(
        "{{ .Values.loki.commonConfig.replication_factor }}",
        str(loki["commonConfig"]["replication_factor"]),
    )
    block = re.compile(
        r"(?m)^[ ]*{{- toYaml \.Values\.loki\.([A-Za-z0-9_]+) \| nindent ([0-9]+) }}$"
    )

    def replace(match: re.Match[str]) -> str:
        source = _resolve(loki, match.group(1))
        dumped = yaml.safe_dump(source, sort_keys=False).rstrip()
        return textwrap.indent(dumped, " " * int(match.group(2)))

    rendered = block.sub(replace, rendered)
    assert "{{" not in rendered, f"unresolved Loki Helm expression:\n{rendered}"
    parsed = yaml.safe_load(rendered)
    assert parsed["ingester"]["wal"]["disk_full_threshold"] == 0.9
    return rendered


def _tempo_config_and_image() -> tuple[str, str]:
    documents = [
        document
        for document in yaml.safe_load_all((OBSERVABILITY / "tempo.yaml").read_text())
        if document
    ]
    config = next(document for document in documents if document["kind"] == "ConfigMap")
    stateful_set = next(document for document in documents if document["kind"] == "StatefulSet")
    image = stateful_set["spec"]["template"]["spec"]["containers"][0]["image"]
    return config["data"]["tempo.yaml"], image


def _collector_config_and_image() -> tuple[dict[str, Any], str]:
    config = _load_yaml(REPO_ROOT / "otel" / "collector-config.yaml")
    integration = _load_yaml(OBSERVABILITY / "curie-values.yaml")["otelCollector"]
    config["exporters"].update(integration["extraExporters"])
    config["service"]["pipelines"]["traces"]["exporters"].extend(
        integration["extraPipelineExporters"]
    )
    assert config["service"]["pipelines"]["traces"]["exporters"] == [
        "otlphttp/langfuse",
        "debug",
        "otlphttp/tempo",
    ]
    chart_values = _load_yaml(REPO_ROOT / "charts" / "curie" / "values.yaml")
    return config, chart_values["otelCollector"]["image"]


def _write_runtime_configs(root: Path) -> dict[str, str]:
    root.mkdir(parents=True, exist_ok=True)

    loki_values = _load_yaml(OBSERVABILITY / "loki-values.yaml")
    (root / "loki.yaml").write_text(_render_loki_config(loki_values))
    (root / "loki-runtime.yaml").write_text("{}\n")

    tempo_config, tempo_image = _tempo_config_and_image()
    (root / "tempo.yaml").write_text(tempo_config)

    grafana_values = _load_yaml(OBSERVABILITY / "grafana-values.yaml")
    provisioning = {
        "apiVersion": 1,
        "datasources": grafana_values["datasources"]["datasources.yaml"]["datasources"],
    }
    (root / "grafana-datasources.yaml").write_text(yaml.safe_dump(provisioning, sort_keys=False))

    collector_config, collector_image = _collector_config_and_image()
    (root / "collector.yaml").write_text(yaml.safe_dump(collector_config, sort_keys=False))
    return {
        "loki": f"docker.io/grafana/loki:{loki_values['loki']['image']['tag']}",
        "tempo": tempo_image,
        "grafana": f"docker.io/grafana/grafana:{grafana_values['image']['tag']}",
        "collector": collector_image,
    }


def _request(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    expected: tuple[int, ...] = (200,),
    timeout: float = 5,
) -> tuple[int, bytes]:
    data = None if body is None else json.dumps(body).encode()
    request_headers = dict(headers or {})
    if data is not None:
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url,
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            response_body = response.read()
    except urllib.error.HTTPError as error:
        status = error.code
        response_body = error.read()
    if status not in expected:
        raise AssertionError(
            f"{method} {url} returned HTTP {status}, expected {expected}: "
            f"{response_body.decode(errors='replace')}"
        )
    return status, response_body


def _wait_http(url: str, container: str, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            status, _ = _request(url, expected=(200, 204), timeout=2)
            if status in (200, 204):
                return
        except Exception as error:  # noqa: BLE001
            last_error = str(error)
        time.sleep(1)
    result = _docker("logs", container, check=False)
    logs = result.stdout + result.stderr
    raise AssertionError(f"{url} never became ready: {last_error}\n{logs}")


def _wait_port(host: str, port: int, container: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    logs = _docker("logs", container, check=False).stdout
    raise AssertionError(f"{host}:{port} never became reachable\n{logs}")


def _host_port(container: str, port: int) -> int:
    output = _docker("port", container, f"{port}/tcp").stdout.strip().splitlines()
    assert output, f"container {container} publishes no host port for {port}"
    return int(output[0].rsplit(":", 1)[1])


def _container_name(prefix: str, suffix: str) -> str:
    return f"curie-obs-{prefix}-{suffix}"


@dataclass
class RuntimeStack:
    suffix: str
    front_network: str
    back_network: str
    tempo_probe_container: str
    grafana_url: str
    loki_url: str
    collector_url: str
    token: str
    containers: list[str]
    volumes: list[str]
    tempo_connector_image: str

    def connector_url(self, connector: str) -> str:
        return f"http://{service_dns(RELEASE, AGENT, connector, NAMESPACE)}:{CONNECTOR_PORT}/mcp"

    def call(self, connector: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = _docker(
            "exec",
            "-i",
            self.tempo_probe_container,
            "python",
            "-",
            self.connector_url(connector),
            tool,
            json.dumps(arguments),
            input_text=MCP_PROBE,
            timeout=90,
        )
        return json.loads(result.stdout.strip().splitlines()[-1])

    def call_text(self, connector: str, tool: str, arguments: dict[str, Any]) -> str:
        response = self.call(connector, tool, arguments)
        assert "error" not in response, response
        result = response["result"]
        assert result.get("isError") is not True, response
        return "\n".join(
            item["text"] for item in result.get("content", []) if item.get("type") == "text"
        )

    def eventually_call_text(
        self,
        connector: str,
        tool: str,
        arguments: dict[str, Any],
        marker: str,
        timeout: float = 45,
    ) -> str:
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            last = self.call_text(connector, tool, arguments)
            if marker in last:
                return last
            time.sleep(1)
        raise AssertionError(
            f"{connector}.{tool} never returned marker {marker!r}; last response: {last}"
        )


def _run_container(
    stack: RuntimeStack,
    name: str,
    image: str,
    network: str,
    *,
    aliases: tuple[str, ...] = (),
    env: dict[str, str] | None = None,
    volumes: tuple[tuple[Path, str], ...] = (),
    named_volumes: tuple[tuple[str, str], ...] = (),
    publish: tuple[int, ...] = (),
    args: tuple[str, ...] = (),
) -> None:
    command = ["run", "-d", "--name", name, "--network", network]
    for alias in aliases:
        command.extend(["--network-alias", alias])
    for key in env or {}:
        command.extend(["-e", key])
    for source, target in volumes:
        command.extend(["-v", f"{source.resolve()}:{target}:ro"])
    for source, target in named_volumes:
        command.extend(["-v", f"{source}:{target}"])
    for port in publish:
        command.extend(["-p", f"127.0.0.1::{port}"])
    command.append(image)
    command.extend(args)
    stack.containers.append(name)
    _docker(*command, environment=env, timeout=300)


def _mint_grafana_token(grafana_url: str, password: str) -> str:
    authorization = base64.b64encode(f"admin:{password}".encode()).decode()
    headers = {"Authorization": f"Basic {authorization}"}
    _, body = _request(
        f"{grafana_url}/api/serviceaccounts",
        method="POST",
        body={"name": "curie-observability-runtime", "role": "Viewer"},
        headers=headers,
        expected=(200, 201),
    )
    account_id = json.loads(body)["id"]
    _, body = _request(
        f"{grafana_url}/api/serviceaccounts/{account_id}/tokens",
        method="POST",
        body={"name": "runtime"},
        headers=headers,
        expected=(200, 201),
    )
    token = json.loads(body)["key"]
    assert token
    return token


def _start_runtime_stack(root: Path) -> RuntimeStack:
    suffix = uuid.uuid4().hex[:10]
    front = f"curie-obs-front-{suffix}"
    back = f"curie-obs-back-{suffix}"
    images = _write_runtime_configs(root)
    tempo_connector_image = f"curie-sre-bot-tempo-runtime:{suffix}"
    stack = RuntimeStack(
        suffix=suffix,
        front_network=front,
        back_network=back,
        tempo_probe_container="",
        grafana_url="",
        loki_url="",
        collector_url="",
        token="",
        containers=[],
        volumes=[],
        tempo_connector_image=tempo_connector_image,
    )
    try:
        _docker("network", "create", front)
        _docker("network", "create", back)
        _populate_runtime_stack(stack, root, images)
        return stack
    except Exception:
        _stop_runtime_stack(stack)
        raise


def _create_volume(stack: RuntimeStack, purpose: str) -> str:
    name = f"curie-obs-{purpose}-{stack.suffix}"
    _docker("volume", "create", name)
    stack.volumes.append(name)
    _docker(
        "run",
        "--rm",
        "--user",
        "0",
        "-v",
        f"{name}:/data",
        "docker.io/library/alpine:3.22",
        "chown",
        "10001:10001",
        "/data",
    )
    return name


def _populate_runtime_stack(stack: RuntimeStack, root: Path, images: dict[str, str]) -> None:
    suffix = stack.suffix
    back = stack.back_network
    front = stack.front_network
    loki_data = _create_volume(stack, "loki-data")
    tempo_data = _create_volume(stack, "tempo-data")
    collector_data = _create_volume(stack, "collector-data")

    loki = _container_name("loki", suffix)
    _run_container(
        stack,
        loki,
        images["loki"],
        back,
        aliases=("loki.observability.svc.cluster.local",),
        volumes=(
            (root / "loki.yaml", "/etc/loki/loki.yaml"),
            (
                root / "loki-runtime.yaml",
                "/etc/loki/runtime-config/runtime-config.yaml",
            ),
        ),
        named_volumes=((loki_data, "/var/loki"),),
        publish=(3100,),
        args=("-config.file=/etc/loki/loki.yaml",),
    )
    stack.loki_url = f"http://127.0.0.1:{_host_port(loki, 3100)}"
    _wait_http(f"{stack.loki_url}/ready", loki)

    tempo = _container_name("tempo", suffix)
    _run_container(
        stack,
        tempo,
        images["tempo"],
        back,
        aliases=("tempo.observability.svc.cluster.local",),
        volumes=((root / "tempo.yaml", "/conf/tempo.yaml"),),
        named_volumes=((tempo_data, "/var/tempo"),),
        publish=(3200,),
        args=("-config.file=/conf/tempo.yaml",),
    )
    tempo_url = f"http://127.0.0.1:{_host_port(tempo, 3200)}"
    _wait_http(f"{tempo_url}/ready", tempo)

    grafana = _container_name("grafana", suffix)
    admin_password = uuid.uuid4().hex + uuid.uuid4().hex
    _run_container(
        stack,
        grafana,
        images["grafana"],
        front,
        aliases=("grafana.observability.svc.cluster.local",),
        env={
            "GF_SECURITY_ADMIN_USER": "admin",
            "GF_SECURITY_ADMIN_PASSWORD": admin_password,
            "GF_AUTH_ANONYMOUS_ENABLED": "false",
            "GF_SERVER_ROUTER_LOGGING": "true",
            "GF_SERVER_HTTP_PORT": "80",
        },
        volumes=(
            (
                root / "grafana-datasources.yaml",
                "/etc/grafana/provisioning/datasources/runtime.yaml",
            ),
        ),
        publish=(80,),
    )
    _docker("network", "connect", back, grafana)
    stack.grafana_url = f"http://127.0.0.1:{_host_port(grafana, 80)}"
    _wait_http(f"{stack.grafana_url}/api/health", grafana)
    stack.token = _mint_grafana_token(stack.grafana_url, admin_password)

    collector = _container_name("collector", suffix)
    _run_container(
        stack,
        collector,
        images["collector"],
        back,
        env={"LANGFUSE_OTLP_AUTH_HEADER": "Basic runtime-test"},
        volumes=((root / "collector.yaml", "/etc/otel/collector.yaml"),),
        named_volumes=((collector_data, "/var/lib/otelcol"),),
        publish=(4318,),
        args=("--config=/etc/otel/collector.yaml",),
    )
    collector_port = _host_port(collector, 4318)
    stack.collector_url = f"http://127.0.0.1:{collector_port}"
    _wait_port("127.0.0.1", collector_port, collector)

    _docker(
        "build",
        "-t",
        stack.tempo_connector_image,
        str(TEMPO_CONNECTOR),
        timeout=300,
    )

    declaration = _load_yaml(CONNECTORS)["connectors"]
    grafana_spec = declaration["grafana"]
    grafana_args = [
        ",".join(host_aliases(RELEASE, AGENT, "grafana", NAMESPACE, CONNECTOR_PORT))
        if argument == "${CURIE_ALLOWED_HOSTS}"
        else argument
        for argument in grafana_spec["args"]
    ]
    grafana_connector = _container_name("mcp-grafana", suffix)
    grafana_alias = service_dns(RELEASE, AGENT, "grafana", NAMESPACE)
    _run_container(
        stack,
        grafana_connector,
        grafana_spec["image"],
        front,
        aliases=(grafana_alias, object_name(RELEASE, AGENT, "grafana")),
        env={
            "GRAFANA_URL": grafana_spec["env"]["GRAFANA_URL"],
            "GRAFANA_SERVICE_ACCOUNT_TOKEN": stack.token,
        },
        args=tuple(grafana_args),
    )

    tempo_connector = _container_name("mcp-tempo", suffix)
    tempo_alias = service_dns(RELEASE, AGENT, "tempo", NAMESPACE)
    _run_container(
        stack,
        tempo_connector,
        stack.tempo_connector_image,
        front,
        aliases=(tempo_alias, object_name(RELEASE, AGENT, "tempo")),
        env={
            "GRAFANA_URL": declaration["tempo"]["env"]["GRAFANA_URL"],
            "GRAFANA_SERVICE_ACCOUNT_TOKEN": stack.token,
        },
    )
    stack.tempo_probe_container = tempo_connector

    deadline = time.monotonic() + 60
    last_error = "connectors were not probed"
    while time.monotonic() < deadline:
        try:
            stack.call("grafana", "list_datasources", {})
            stack.call("tempo", "list_trace_tags", {})
            return
        except Exception as error:  # noqa: BLE001
            last_error = str(error)
            time.sleep(1)
    logs = "\n".join(
        _docker("logs", container, check=False).stdout
        for container in (grafana_connector, tempo_connector)
    )
    raise AssertionError(f"MCP connectors never became ready: {last_error}\n{logs}")


def _stop_runtime_stack(stack: RuntimeStack) -> None:
    for container in reversed(stack.containers):
        _docker("rm", "-f", container, check=False, timeout=30)
    _docker("network", "rm", stack.front_network, check=False, timeout=30)
    _docker("network", "rm", stack.back_network, check=False, timeout=30)
    _docker("image", "rm", "-f", stack.tempo_connector_image, check=False, timeout=30)
    for volume in reversed(stack.volumes):
        _docker("volume", "rm", "-f", volume, check=False, timeout=30)


@pytest.fixture(scope="module")
def observability_runtime() -> Iterator[RuntimeStack]:
    _require_docker()
    with tempfile.TemporaryDirectory(prefix="curie-sre-bot-observability-runtime-") as root:
        stack: RuntimeStack | None = None
        try:
            stack = _start_runtime_stack(Path(root))
            yield stack
        finally:
            if stack is not None:
                _stop_runtime_stack(stack)


def _response_text(response: dict[str, Any]) -> str:
    result = response["result"]
    return "\n".join(
        item["text"] for item in result.get("content", []) if item.get("type") == "text"
    )


def test_logql_through_the_bots_real_grafana_connector_returns_a_curie_log(
    observability_runtime: RuntimeStack,
) -> None:
    marker = f"curie-api observability-runtime-{uuid.uuid4().hex}"
    _request(
        f"{observability_runtime.loki_url}/loki/api/v1/push",
        method="POST",
        body={
            "streams": [
                {
                    "stream": {
                        "namespace": "curie",
                        "container": "api",
                        "job": "curie/api",
                    },
                    "values": [[str(time.time_ns()), marker]],
                }
            ]
        },
        expected=(204,),
    )

    query = {
        "datasourceUid": "loki",
        "logql": f'{{namespace="curie", container="api"}} |= "{marker}"',
        "limit": 10,
        "direction": "backward",
    }
    result = observability_runtime.eventually_call_text("grafana", "query_loki_logs", query, marker)
    assert marker in result

    missing = observability_runtime.call_text(
        "grafana",
        "query_loki_logs",
        {
            **query,
            "logql": '{namespace="curie", container="api"} |= "absent-runtime-marker"',
        },
    )
    assert marker not in missing
    assert '"data":[]' in missing.replace(" ", "")


@contextmanager
def _otel_environment(endpoint: str) -> Iterator[None]:
    keys = (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_HEADERS",
    )
    old = {key: os.environ.get(key) for key in keys}
    os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint
    os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
    os.environ.pop("OTEL_EXPORTER_OTLP_HEADERS", None)
    try:
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_tempo_connector_returns_a_real_curie_span_through_grafanas_uid_proxy(
    observability_runtime: RuntimeStack,
) -> None:
    marker = f"curie-observability-runtime-{uuid.uuid4().hex}"
    with _otel_environment(observability_runtime.collector_url):
        provider = build_tracer_provider(
            OtelConfig(endpoint=observability_runtime.collector_url),
            marker,
            "runtime-sandbox",
        )
        assert provider is not None
        tracer = RunTracer(provider)
        with tracer.run_span(marker, "fake-model") as generation:
            generation.record_usage({"input_tokens": 1, "output_tokens": 1})
        tracer.shutdown()

    search = {
        "query": '{ resource.service.name = "curie-runner" }',
        "limit": 20,
    }
    result = observability_runtime.eventually_call_text(
        "tempo", "search_traces", search, "curie-runner", timeout=60
    )
    payload = json.loads(result)
    traces = payload.get("traces", [])
    assert traces, payload
    trace_id = traces[0]["traceID"]

    trace = observability_runtime.eventually_call_text(
        "tempo", "get_trace", {"trace_id": trace_id}, marker
    )
    assert marker in trace
    assert "curie-runner" in trace

    missing = observability_runtime.call_text(
        "tempo",
        "search_traces",
        {"query": '{ resource.service.name = "curie-runtime-absent" }', "limit": 20},
    )
    assert json.loads(missing).get("traces") == []
