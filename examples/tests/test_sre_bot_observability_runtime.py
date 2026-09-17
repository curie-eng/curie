"""Docker backed behavioral proof for the SRE bot observability connectors.

This is deliberately not a Kubernetes substitute. Chart rendering owns the
Kubernetes object contract. Here, the exact configured backends and real MCP
connector artifacts are driven over their network protocols so a green test
means a LogQL result and a Tempo span actually crossed the connector boundary.
"""

from __future__ import annotations

import base64
import configparser
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


@dataclass(frozen=True)
class TempoEnvelope:
    """The memory envelope the shipped Tempo manifest declares (#2059).

    Both fields are read from `examples/sre-bot/observability/tempo.yaml` and are
    deliberately optional: the manifest is the single source of truth, so a
    number restated here would keep the runtime regression green against a
    reverted manifest. `None` means the manifest does not declare it, which the
    regression asserts on by name rather than silently substituting a default.
    """

    memory_limit: str | None
    gomemlimit: str | None


_QUANTITY_SUFFIXES: tuple[tuple[str, int], ...] = (
    # Longest suffix first: "Mi" must win over "M", "MiB" over "M".
    ("KiB", 1024),
    ("MiB", 1024**2),
    ("GiB", 1024**3),
    ("TiB", 1024**4),
    ("Ki", 1024),
    ("Mi", 1024**2),
    ("Gi", 1024**3),
    ("Ti", 1024**4),
    ("B", 1),
    ("k", 1000),
    ("K", 1000),
    ("M", 1000**2),
    ("G", 1000**3),
    ("T", 1000**4),
)


def _memory_bytes(value: str) -> int:
    """Parse a Kubernetes quantity (`512Mi`) or a Go `GOMEMLIMIT` (`600MiB`).

    One parser for both because the regression compares them against each other
    and against `docker inspect`, which speaks only bytes.
    """

    text = value.strip()
    assert text, "memory quantity must not be empty"
    for suffix, multiplier in _QUANTITY_SUFFIXES:
        if text.endswith(suffix):
            digits = text[: -len(suffix)].strip()
            assert digits.isdigit(), f"unparsable memory quantity {value!r}"
            return int(digits) * multiplier
    assert text.isdigit(), f"unparsable memory quantity {value!r}"
    return int(text)


def _tempo_config_and_image() -> tuple[str, str, TempoEnvelope]:
    documents = [
        document
        for document in yaml.safe_load_all((OBSERVABILITY / "tempo.yaml").read_text())
        if document
    ]
    config = next(document for document in documents if document["kind"] == "ConfigMap")
    stateful_set = next(document for document in documents if document["kind"] == "StatefulSet")
    container = stateful_set["spec"]["template"]["spec"]["containers"][0]
    image = container["image"]
    limit = container.get("resources", {}).get("limits", {}).get("memory")
    gomemlimit = next(
        (
            entry.get("value")
            for entry in container.get("env") or []
            if entry.get("name") == "GOMEMLIMIT"
        ),
        None,
    )
    envelope = TempoEnvelope(
        memory_limit=None if limit is None else str(limit),
        gomemlimit=None if gomemlimit is None else str(gomemlimit),
    )
    return config["data"]["tempo.yaml"], image, envelope


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


def _write_runtime_configs(root: Path) -> tuple[dict[str, str], TempoEnvelope]:
    root.mkdir(parents=True, exist_ok=True)

    loki_values = _load_yaml(OBSERVABILITY / "loki-values.yaml")
    (root / "loki.yaml").write_text(_render_loki_config(loki_values))
    (root / "loki-runtime.yaml").write_text("{}\n")

    tempo_config, tempo_image, tempo_envelope = _tempo_config_and_image()
    (root / "tempo.yaml").write_text(tempo_config)

    grafana_values = _load_yaml(OBSERVABILITY / "grafana-values.yaml")
    grafana_ini = configparser.ConfigParser(interpolation=None)
    grafana_ini.read_dict(
        {
            section: {
                key: str(value).lower() if isinstance(value, bool) else str(value)
                for key, value in settings.items()
            }
            for section, settings in grafana_values["grafana.ini"].items()
        }
    )
    with (root / "grafana.ini").open("w") as grafana_ini_file:
        grafana_ini.write(grafana_ini_file)
    provisioning = {
        "apiVersion": 1,
        "datasources": grafana_values["datasources"]["datasources.yaml"]["datasources"],
    }
    (root / "grafana-datasources.yaml").write_text(yaml.safe_dump(provisioning, sort_keys=False))

    collector_config, collector_image = _collector_config_and_image()
    (root / "collector.yaml").write_text(yaml.safe_dump(collector_config, sort_keys=False))
    images = {
        "loki": f"docker.io/grafana/loki:{loki_values['loki']['image']['tag']}",
        "tempo": tempo_image,
        "grafana": f"docker.io/grafana/grafana:{grafana_values['image']['tag']}",
        "collector": collector_image,
    }
    return images, tempo_envelope


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
    tempo_container: str
    tempo_url: str
    tempo_envelope: TempoEnvelope
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
    memory: str | None = None,
) -> None:
    command = ["run", "-d", "--name", name, "--network", network]
    if memory is not None:
        # `--memory-swap` must be given AND equal to `--memory`: left unset the
        # container gets swap equal to twice the limit, so it can swap instead
        # of being OOM killed and any assertion about running "under the
        # configured limit" becomes vacuous (#2059 edge case E8).
        command.extend(["--memory", memory, "--memory-swap", memory])
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
    images, tempo_envelope = _write_runtime_configs(root)
    tempo_connector_image = f"curie-sre-bot-tempo-runtime:{suffix}"
    stack = RuntimeStack(
        suffix=suffix,
        front_network=front,
        back_network=back,
        tempo_probe_container="",
        tempo_container="",
        tempo_url="",
        tempo_envelope=tempo_envelope,
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
    # Run Tempo AT the envelope the shipped manifest declares (#2059). Started
    # unbounded, this stack proves only that Tempo works, never that it works
    # inside the cgroup it actually ships with. The values come from the
    # manifest, never from a literal here, so a reverted manifest cannot leave
    # this test green.
    envelope = stack.tempo_envelope
    tempo_env = None if envelope.gomemlimit is None else {"GOMEMLIMIT": envelope.gomemlimit}
    tempo_memory = (
        None if envelope.memory_limit is None else str(_memory_bytes(envelope.memory_limit))
    )
    _run_container(
        stack,
        tempo,
        images["tempo"],
        back,
        aliases=("tempo.observability.svc.cluster.local",),
        env=tempo_env,
        volumes=((root / "tempo.yaml", "/conf/tempo.yaml"),),
        named_volumes=((tempo_data, "/var/tempo"),),
        publish=(3200,),
        args=("-config.file=/conf/tempo.yaml",),
        memory=tempo_memory,
    )
    stack.tempo_container = tempo
    tempo_url = f"http://127.0.0.1:{_host_port(tempo, 3200)}"
    stack.tempo_url = tempo_url
    # Load bearing twice over: a config the pinned Tempo rejects never becomes
    # ready, so this is also the version skew gate for every bound #2059 adds.
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
            (root / "grafana.ini", "/etc/grafana/grafana.ini"),
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


# #2059: the shipped Tempo ran every memory knob at Tempo's DISTRIBUTED
# deployment defaults inside a single 512Mi pod, and ordinary ingest plus
# bounded operator reads got the container kernel OOM killed (exit 137,
# `Memory cgroup out of memory: Killed process (tempo)`,
# `constraint=CONSTRAINT_MEMCG`), taking the evidence path down with it.
#
# Peak memory is driven by BLOCK COUNT, near independently of how much trace
# data each block holds: search fans out over every block the ingester still
# holds. Measured on this exact pinned image and config
# (`.projects/plans/task-2059-tempo-query-oom.evidence.md`), two tiny traces per
# wave with the ingester flushed between waves:
#
#   shipped config @512Mi, no GOMEMLIMIT, 150 blocks -> OOMKilled exit 137, 205/216 reads failed
#   shipped config @512Mi, no GOMEMLIMIT, 290 blocks -> OOMKilled exit 137, 216/216 reads failed
#   bounded config @1Gi + GOMEMLIMIT=600MiB, 290 blocks -> peak 549 MiB, 216/216 reads OK
#   bounded config @1Gi + GOMEMLIMIT=600MiB, 450 blocks -> peak 685 MiB, 216/216 reads OK
#
# 150 is the SMALLEST measured block count that killed the unbounded shipped
# envelope, and a wave of two ten span traces is enough to cut a block, so this
# target stands on a load that demonstrably killed the pre-fix configuration
# while keeping the run to a couple of minutes.
#
# The assertion direction is deliberate and is NOT reversible. The shipped
# configuration is non monotonic near its ceiling -- 150 blocks died, 220 blocks
# survived at a peak of exactly 512 MiB -- so whether it crosses is GC timing
# dependent. A test asserting the UNBOUNDED config OOMs would be flaky; this one
# asserts the CONFIGURED envelope SURVIVES.
TEMPO_BLOCK_TARGET = 150
# Each wave is two traces of ten spans. Cutting a block needs data in the head
# block at flush time, and the collector batches, so a wave occasionally cuts
# nothing; the cap bounds the retry rather than the target.
TEMPO_INGEST_WAVE_CAP = TEMPO_BLOCK_TARGET * 3
TEMPO_INGEST_DEADLINE_SECONDS = 420
TEMPO_TRACES_PER_WAVE = 2
TEMPO_TOOL_SPANS_PER_TRACE = 8
# A block is only cut if the head block holds data when `/flush` arrives, and
# the wave crosses the shipped collector, whose `batch` processor holds spans for
# its 200ms default before exporting to Tempo. Flushing faster than that cuts
# empty blocks: measured, 450 unpaced waves produced 16 blocks, 150 paced waves
# produce one block each. This is the pacing, not a sleep-and-hope.
TEMPO_COLLECTOR_SETTLE_SECONDS = 0.3
# The read sequence an operator ran during the soak, repeated. Each round is one
# search at the connector ceiling, three whole trace fetches, one tag listing and
# one tag VALUES lookup -- the last is the only call that reaches
# `overrides.defaults.read.max_bytes_per_tag_values_query`.
TEMPO_READ_ROUNDS = 8
TEMPO_TRACE_FETCHES_PER_ROUND = 3
# Mirrors `examples/sre-bot/connectors/tempo/server.py:69` MAX_LIMIT. The
# connector clamps to it, so this is the widest search an operator can drive
# through the real MCP path.
TEMPO_CONNECTOR_MAX_LIMIT = 50


def _inspect(container: str) -> dict[str, Any]:
    result = _docker("inspect", container, timeout=60)
    payload = json.loads(result.stdout)
    assert payload, f"docker inspect returned nothing for {container}"
    return payload[0]


def _tempo_block_count(container: str) -> int:
    """Count the blocks Tempo has cut, which is what search fan out scales with.

    A plain `ls | wc -l` on the tenant directory overcounts: Tempo's compactor
    also writes tenant bookkeeping into that same directory, sibling to the
    UUID block directories -- verified against tempo:2.9.1 as `index.json.gz`
    plus its companion `index.pb.zst`, written by the blocklist poller on
    every poll (including immediately at startup, not just on its 5m ticker).
    A naive listing counts those as blocks too, so it can claim
    TEMPO_BLOCK_TARGET blocks while one or more entries are not blocks at all.
    That silently undershoots the real block count the regression depends on
    -- 150 is the smallest measured count that OOM-killed the unbounded
    config, so a false 150 no longer provably exercises that threshold. Count
    real blocks instead: each one is a UUID-named directory holding a
    `meta.json`, so `find -name meta.json` counts exactly the blocks with
    metadata and can't be fooled by the tenant index or a stray file. (A
    compacted block's metadata is `meta.compacted.json` in some Tempo versions
    and is deliberately excluded -- it is no longer a searchable block.)
    """

    result = _docker(
        "exec",
        container,
        "sh",
        "-c",
        "find /var/tempo/blocks/single-tenant -name meta.json 2>/dev/null | wc -l",
        check=False,
        timeout=60,
    )
    if result.returncode != 0:
        return 0
    digits = result.stdout.strip()
    return int(digits) if digits.isdigit() else 0


def _emit_trace_wave(tracer: RunTracer, marker: str, wave: int) -> None:
    for index in range(TEMPO_TRACES_PER_WAVE):
        with tracer.run_span(f"{marker}-{wave}-{index}", "fake-model") as generation:
            generation.record_usage({"input_tokens": 1, "output_tokens": 1})
            for tool in range(TEMPO_TOOL_SPANS_PER_TRACE):
                generation.tool_span(f"probe-{tool}")


def _read_json(stack: RuntimeStack, tool: str, arguments: dict[str, Any]) -> Any:
    """One operator read through the real MCP connector, JSON or a failure.

    The connector returns backend errors as prose rather than raising, so a dead
    or stalled Tempo comes back as an unparsable body. Turning that into an
    exception here is what lets the caller count it as a failed read instead of
    silently treating a sentence as a successful answer.
    """

    text = stack.call_text("tempo", tool, arguments)
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise AssertionError(f"tempo.{tool} did not return JSON: {text[:400]}") from error


def _assert_tempo_survived(stack: RuntimeStack, phase: str) -> None:
    state = _inspect(stack.tempo_container)["State"]
    assert state.get("OOMKilled") is False, f"Tempo was OOM killed during {phase}: {state}"
    assert state.get("ExitCode") != 137, (
        f"Tempo exited 137 (SIGKILL, the cgroup OOM signature) during {phase}: {state}"
    )
    assert state.get("Status") == "running", f"Tempo is not running after {phase}: {state}"
    assert state.get("Restarting") is False, f"Tempo is restarting after {phase}: {state}"
    assert _inspect(stack.tempo_container).get("RestartCount") == 0, (
        f"Tempo restarted during {phase}, so the evidence path went away: "
        f"{_inspect(stack.tempo_container).get('RestartCount')}"
    )


def test_tempo_survives_ingest_and_bounded_reads_under_its_configured_memory_limit(
    observability_runtime: RuntimeStack,
) -> None:
    """#2059: ingest plus representative operator reads, under the shipped limit.

    This is the regression the issue asks for: the real pinned Tempo image, the
    shipped config, inside the cgroup the manifest declares, driven by real
    spans through the real collector and read back through the real MCP
    connector.

    What it does NOT prove: a 24h soak. Retention is 48h and blocks are cut every
    5m, so a real deployment reaches roughly six times this block count and also
    runs the compactor, which none of the read path bounds cover. The soak shaped
    assurance comes from the static read buffer product assertion in
    `charts/curie/ci/observability-stack-assertions.sh`, not from this test.
    """

    envelope = observability_runtime.tempo_envelope
    # Read from the manifest, never restated here: a literal copy would keep this
    # test green against a reverted `tempo.yaml`.
    assert envelope.memory_limit is not None, (
        "examples/sre-bot/observability/tempo.yaml must declare "
        "spec.template.spec.containers[0].resources.limits.memory for the tempo "
        "container: without it there is no configured limit to run under and this "
        "regression cannot detect #2059"
    )
    assert envelope.gomemlimit is not None, (
        "examples/sre-bot/observability/tempo.yaml must set a GOMEMLIMIT env var on "
        "the tempo container (#2059): the Go GC otherwise never learns the cgroup "
        "ceiling and grows the heap until the kernel kills the process"
    )
    limit_bytes = _memory_bytes(envelope.memory_limit)
    gomemlimit_bytes = _memory_bytes(envelope.gomemlimit)
    assert 0 < gomemlimit_bytes < limit_bytes, (
        f"GOMEMLIMIT {envelope.gomemlimit} must be a soft ceiling strictly below the "
        f"hard cgroup limit {envelope.memory_limit}"
    )

    # The manifest and the container the assertions below judge must agree, or a
    # green run proves nothing about the shipped envelope.
    inspected = _inspect(observability_runtime.tempo_container)
    assert inspected["HostConfig"]["Memory"] == limit_bytes, (
        f"Tempo is not running under the manifest's {envelope.memory_limit} limit: "
        f"docker reports HostConfig.Memory={inspected['HostConfig']['Memory']}, "
        f"expected {limit_bytes}. A daemon that silently ignored --memory would "
        f"otherwise make this whole test vacuous"
    )
    assert inspected["HostConfig"]["MemorySwap"] == limit_bytes, (
        "Tempo's --memory-swap must equal --memory or the container can swap "
        f"instead of being OOM killed: {inspected['HostConfig']['MemorySwap']}"
    )
    assert f"GOMEMLIMIT={envelope.gomemlimit}" in inspected["Config"]["Env"], (
        f"GOMEMLIMIT={envelope.gomemlimit} never reached the Tempo container; its "
        f"env is {inspected['Config']['Env']}. A dropped value would let a low "
        f"pressure workload pass without ever exercising the new GC ceiling"
    )

    marker = f"curie-observability-oom-{uuid.uuid4().hex}"
    flush_url = f"{observability_runtime.tempo_url}/flush"
    blocks = 0
    waves = 0
    deadline = time.monotonic() + TEMPO_INGEST_DEADLINE_SECONDS
    with _otel_environment(observability_runtime.collector_url):
        provider = build_tracer_provider(
            OtelConfig(endpoint=observability_runtime.collector_url),
            marker,
            "runtime-sandbox",
        )
        assert provider is not None
        tracer = RunTracer(provider)
        try:
            while (
                blocks < TEMPO_BLOCK_TARGET
                and waves < TEMPO_INGEST_WAVE_CAP
                and time.monotonic() < deadline
            ):
                _emit_trace_wave(tracer, marker, waves)
                tracer.force_flush()
                time.sleep(TEMPO_COLLECTOR_SETTLE_SECONDS)
                # Cut a block on demand rather than waiting out
                # `ingester.max_block_duration: 5m`. This is how a two minute
                # test reaches the block count a multi hour deployment reaches.
                try:
                    _request(flush_url, method="POST", expected=(200, 204), timeout=10)
                except Exception:  # noqa: BLE001
                    # A refused flush means Tempo is already gone; stop ingesting
                    # so the container assertions below name the real cause.
                    break
                waves += 1
                if waves % 10 == 0:
                    blocks = _tempo_block_count(observability_runtime.tempo_container)
        finally:
            tracer.shutdown()
    blocks = _tempo_block_count(observability_runtime.tempo_container)

    _assert_tempo_survived(observability_runtime, f"ingest of {waves} waves ({blocks} blocks)")
    assert blocks >= TEMPO_BLOCK_TARGET, (
        f"only {blocks} blocks accumulated over {waves} flushed waves, below the "
        f"{TEMPO_BLOCK_TARGET} the measured envelope is judged against; block count "
        f"is what drives Tempo's search fan out, so a lighter run would pass "
        f"without exercising #2059 at all"
    )

    read_failures: list[str] = []
    reads = 0
    for round_index in range(TEMPO_READ_ROUNDS):
        try:
            found = _read_json(
                observability_runtime,
                "search_traces",
                {
                    "query": '{ resource.service.name = "curie-runner" }',
                    "limit": TEMPO_CONNECTOR_MAX_LIMIT,
                },
            )
            reads += 1
            traces = found.get("traces") or []
            for trace in traces[:TEMPO_TRACE_FETCHES_PER_ROUND]:
                _read_json(observability_runtime, "get_trace", {"trace_id": trace["traceID"]})
                reads += 1
            _read_json(observability_runtime, "list_trace_tags", {})
            reads += 1
            _read_json(
                observability_runtime,
                "list_trace_tag_values",
                {"tag": "resource.service.name"},
            )
            reads += 1
        except Exception as error:  # noqa: BLE001
            read_failures.append(f"round {round_index}: {error}")

    _assert_tempo_survived(observability_runtime, f"{reads} bounded reads over {blocks} blocks")

    # A GOMEMLIMIT GC death spiral does NOT set OOMKilled: the process stays
    # alive and stops answering. Liveness alone would pass that, so the envelope
    # is only safe to ship if a read still RETURNS.
    still_serving = _read_json(
        observability_runtime,
        "search_traces",
        {
            "query": '{ resource.service.name = "curie-runner" }',
            "limit": TEMPO_CONNECTOR_MAX_LIMIT,
        },
    )
    assert still_serving.get("traces"), (
        "Tempo is still running but no longer answering operator searches over "
        f"{blocks} blocks: {str(still_serving)[:400]}"
    )

    assert not read_failures, (
        f"{len(read_failures)} of the operator read sequence failed under the "
        f"configured {envelope.memory_limit} limit over {blocks} blocks: "
        f"{read_failures}"
    )
