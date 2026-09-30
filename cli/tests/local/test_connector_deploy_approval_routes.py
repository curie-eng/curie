"""Real local fix pin for connector approval route provisioning."""

from __future__ import annotations

import json
import os
import pathlib
import re
import shlex
import shutil
import socket
import subprocess
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass

import pytest

REPO = pathlib.Path(__file__).parents[3]
LADDER = REPO / "cli/scripts/e2e-ladder.sh"
API_KEY = "curie-dev-key"
EXPECTED_ROUTES = {"incident-approvals", "sre-approvals"}
EXISTING_ROUTE = "existing-approvals"


def _run(
    argv: list[str],
    *,
    cwd: pathlib.Path = REPO,
    env: dict[str, str] | None = None,
    timeout: int = 300,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode:
        raise RuntimeError(
            f"{purpose} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "CURIE_API_KEY": API_KEY,
            "GITHUB_APP_ID": "",
            "GITHUB_APP_PRIVATE_KEY": "",
            "GITHUB_REVIEW_INGRESS_ENABLED": "false",
            "GITHUB_WEBHOOK_SECRET": "dev-webhook-secret",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "",
            "SLACK_APP_TOKEN": "",
            "SLACK_BOT_TOKEN": "",
            "SLACK_SIGNING_SECRET": "",
        }
    )
    return env


def _function(source: str, name: str, *, optional: bool = False) -> str:
    marker = f"{name}() {{\n"
    start = source.find(marker)
    if start < 0:
        if optional:
            return ""
        raise RuntimeError(f"{LADDER} does not define {name}")
    next_function = re.search(r"(?m)^[a-zA-Z_][a-zA-Z0-9_]*\(\) \{$", source[start + len(marker) :])
    limit = len(source) if next_function is None else start + len(marker) + next_function.start()
    end = source.rfind("\n}\n", start, limit)
    if end < 0:
        raise RuntimeError(f"could not find the end of {name} in {LADDER}")
    return source[start : end + 3]


def _deploy_block(source: str, tier: str) -> str:
    rung = "rung_local_release" if tier == "local-release" else "rung_local"
    body = _function(source, rung)
    start = body.find("    local deploy_json digest agent_id agent_name deployment_id\n")
    label = "local-release" if tier == "local-release" else "local"
    last = f'    deployment_id="$(deploy_field "{label}" "$deploy_json" deployment.id)"\n'
    end = body.find(last, start)
    if start < 0 or end < 0:
        raise RuntimeError(f"could not extract the {tier} deployment block")
    return body[start : end + len(last)]


def _api(
    base: str,
    method: str,
    path: str,
    payload: dict[str, object] | None = None,
) -> object:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "X-API-Key": API_KEY},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read()
    return None if not raw else json.loads(raw)


@pytest.fixture(scope="session")
def source_artifacts(tmp_path_factory: pytest.TempPathFactory):
    owned = tmp_path_factory.mktemp("connector-local-artifacts")
    binary_value = os.environ.get("CURIE_BIN")
    if binary_value:
        binary = pathlib.Path(binary_value).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not an executable file: {binary}")
    else:
        result = _run(
            ["cargo", "build", "--release", "--locked"],
            cwd=REPO / "cli",
            timeout=1200,
        )
        _require(result, "building the source CLI")
        metadata = _run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=REPO / "cli",
        )
        target = pathlib.Path(
            json.loads(_require(metadata, "reading the Cargo target"))["target_directory"]
        )
        built = target / "release/curie"
        binary = owned / "curie"
        shutil.copy2(built, binary)
        binary.chmod(0o755)

    supplied_image = os.environ.get("CURIE_LOCAL_DEPLOY_API_IMAGE")
    api_image = supplied_image or f"curie-2762-api:{uuid.uuid4().hex[:12]}"
    if supplied_image:
        _require(_run(["docker", "image", "inspect", api_image]), "finding the owned API image")
    else:
        _require(
            _run(
                ["docker", "build", "-f", "apps/api/Dockerfile", "-t", api_image, "."],
                timeout=1200,
            ),
            "building the source API image",
        )
    if supplied_image:
        yield binary, api_image
        return
    try:
        yield binary, api_image
    finally:
        removed = _run(["docker", "image", "rm", "-f", api_image])
        if removed.returncode:
            raise RuntimeError(
                f"removing owned API image {api_image} failed\n{removed.stdout}\n{removed.stderr}"
            )


@dataclass
class LocalCase:
    tier: str
    root: pathlib.Path
    bundle: pathlib.Path
    binary: pathlib.Path
    api_url: str
    env: dict[str, str]


def _kernel_picked_ports(count: int) -> list[int]:
    # Every probe stays bound until all are picked, so the ports are distinct.
    probes = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(count)]
    try:
        for probe in probes:
            probe.bind(("127.0.0.1", 0))
        return [probe.getsockname()[1] for probe in probes]
    finally:
        for probe in probes:
            probe.close()


def _write_override(path: pathlib.Path, api_image: str, host_ports: dict[str, int]) -> None:
    worker_database = (
        "postgresql+asyncpg://postgres:postgres@127.0.0.1:"
        "${CURIE_LOCAL_POSTGRES_PORT}/postgres"
    )
    path.write_text(
        f"""services:
  postgres:
    ports: !override [\"127.0.0.1:{host_ports['postgres']}:5432\"]
  valkey:
    ports: !override [\"127.0.0.1:{host_ports['valkey']}:6379\"]
  rustfs:
    ports: !override [\"127.0.0.1:{host_ports['rustfs']}:9000\", \"127.0.0.1::9001\"]
  curie-migrate:
    image: {api_image}
    pull_policy: never
  curie-api:
    image: {api_image}
    pull_policy: never
    environment:
      API_KEY: {API_KEY}
      GITHUB_REVIEW_INGRESS_ENABLED: \"false\"
      GITHUB_APP_ID: \"\"
      GITHUB_APP_PRIVATE_KEY: \"\"
      GITHUB_WEBHOOK_SECRET: dev-webhook-secret
      SLACK_BOT_TOKEN: \"\"
      OTEL_EXPORTER_OTLP_ENDPOINT: \"\"
      OTEL_EXPORTER_OTLP_PROTOCOL: \"\"
    ports: !override [\"127.0.0.1:{host_ports['curie-api']}:8000\"]
  curie-worker:
    environment:
      DATABASE_URL: {worker_database}
      VALKEY_HOST: ${{VALKEY_HOST}}
      VALKEY_PORT: ${{VALKEY_PORT}}
      S3_ENDPOINT_URL: ${{S3_ENDPOINT_URL}}
      CURIE_API_URL: ${{CURIE_API_URL}}
      SLACK_API_BASE_URL: http://127.0.0.1:${{CURIE_LOCAL_STUB_PORT}}/api/
      TMPDIR: ${{CURIE_LOCAL_STAGING_DIR}}
      CURIE_DOCKER_NETWORK: ${{CURIE_DOCKER_NETWORK}}
    volumes: !override
      - /var/run/docker.sock:/var/run/docker.sock
      - ${{CURIE_LOCAL_STAGING_DIR}}:${{CURIE_LOCAL_STAGING_DIR}}
networks:
  curie_runner:
    name: \"${{COMPOSE_PROJECT_NAME}}_runner\"
"""
    )


def _prepare_bundle(
    source: str,
    bundle: pathlib.Path,
    plugin_name: str,
    env: dict[str, str],
) -> None:
    shutil.copytree(REPO / "examples/sre-bot", bundle)
    script = (
        "set -euo pipefail\n"
        f'CONNECTOR_FIXTURE="{REPO / "cli/scripts/fixtures/sre-bot-connectors-enabled.yaml"}"\n'
        + _function(source, "prepare_connector_bundle")
        + f'prepare_connector_bundle "{bundle}"\n'
    )
    _require(_run(["bash", "-c", script], env=env), "preparing the real connector bundle")
    manifest_path = bundle / ".claude-plugin/plugin.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["name"] = plugin_name
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def _provision_connector_credentials(
    source: str,
    root: pathlib.Path,
    env: dict[str, str],
) -> dict[str, str]:
    names = (
        "K8S_READONLY_KUBECONFIG",
        "SELF_UPGRADE_KUBECONFIG",
        "GRAFANA_SERVICE_ACCOUNT_TOKEN",
    )
    script = f"""set -euo pipefail
WORKDIR={shlex.quote(str(root))}
CONNECTOR_OMIT_SECRET=''
{_function(source, "provision_connector_credentials")}
provision_connector_credentials >/dev/null
python3 - <<'PY'
import json
import os
names = {names!r}
print(json.dumps({{name: os.environ[name] for name in names}}))
PY
"""
    result = _run(["bash", "-c", script], env=env)
    return json.loads(_require(result, "provisioning connector credentials"))


@pytest.fixture
def local_case(request: pytest.FixtureRequest, tmp_path: pathlib.Path, source_artifacts):
    tier = str(request.param)
    binary, api_image = source_artifacts
    source = LADDER.read_text()
    suffix = uuid.uuid4().hex[:10]
    project = f"curie-2762-{tier.replace('-', '')}-{suffix}"
    plugin_name = f"acme-2762-{suffix}"
    root = tmp_path
    bundle = root / ("bundle-release" if tier == "local-release" else "bundle")
    override = root / "compose.override.yaml"
    release = root / "compose.release.yaml"
    # Named rather than Docker-allocated so the host-network worker can dial
    # them on Docker Desktop too; the render check below says why.
    host_ports = dict(
        zip(
            ("postgres", "valkey", "rustfs", "curie-api"),
            _kernel_picked_ports(4),
            strict=True,
        )
    )
    _write_override(override, api_image, host_ports)
    if tier == "local-release":
        generated = _run(["python3", "compose/generate_release_compose.py"])
        release.write_text(_require(generated, "generating release Compose"))
        base = release
    else:
        base = REPO / "compose.dev.yaml"

    env = _clean_env()
    staging = root / "staging"
    staging.mkdir()
    env.update(
        {
            "COMPOSE_FILE": f"{base}:{override}",
            "COMPOSE_PROJECT_NAME": project,
            "CURIE_API_KEY": API_KEY,
            "CURIE_API_URL": f"http://127.0.0.1:{host_ports['curie-api']}",
            "CURIE_DOCKER_NETWORK": f"{project}_runner",
            "CURIE_LOCAL_IMAGE_TAG": f"test-{suffix}",
            "CURIE_LOCAL_POSTGRES_HOST": "127.0.0.1",
            "CURIE_LOCAL_POSTGRES_PORT": str(host_ports["postgres"]),
            "CURIE_LOCAL_STAGING_DIR": str(staging),
            "CURIE_LOCAL_STUB_PORT": "9",
            "S3_ENDPOINT_URL": f"http://127.0.0.1:{host_ports['rustfs']}",
            "VALKEY_HOST": "127.0.0.1",
            "VALKEY_PORT": str(host_ports["valkey"]),
        }
    )
    env.update(_provision_connector_credentials(source, root, env))
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(base),
        "-f",
        str(override),
        "--profile",
        "core",
    ]
    tags = [
        f"curie-connector-{project}-{plugin_name}-self-upgrade:build",
        f"curie-connector-{project}-{plugin_name}-tempo:build",
    ]
    teardown_errors: list[str] = []
    try:
        rendered_result = _run(compose + ["config", "--format", "json"], env=env)
        rendered = json.loads(_require(rendered_result, "rendering private Compose"))
        # The host-network worker dials each of these at 127.0.0.1:<host port>.
        # From inside Docker Desktop's VM a Docker-allocated host port refuses
        # that connection while a host port the caller names answers, so each
        # dialed port must be published at exactly the port the worker is given.
        # Any other port stays Docker-allocated, and every port is loopback only.
        dialed = {
            "postgres": (5432, env["CURIE_LOCAL_POSTGRES_PORT"]),
            "valkey": (6379, env["VALKEY_PORT"]),
            "rustfs": (9000, str(urllib.parse.urlsplit(env["S3_ENDPOINT_URL"]).port)),
            "curie-api": (8000, str(urllib.parse.urlsplit(env["CURIE_API_URL"]).port)),
        }
        for service, (target, host_port) in dialed.items():
            ports = rendered["services"][service].get("ports", [])
            published = {port["target"]: port.get("published") for port in ports}
            if (
                str(published.get(target)) != host_port
                or any(
                    value not in {None, "", 0, "0"}
                    for key, value in published.items()
                    if key != target
                )
                or any(port.get("host_ip") != "127.0.0.1" for port in ports)
            ):
                raise RuntimeError(
                    f"{service} does not publish {target} at the worker's host port "
                    f"{host_port} with every other port Docker-allocated on loopback: {ports}"
                )
        if rendered["networks"]["curie_runner"]["name"] != f"{project}_runner":
            raise RuntimeError("private runner network name was not applied")
        for tag in tags:
            if _run(["docker", "image", "inspect", tag]).returncode == 0:
                raise RuntimeError(f"owned connector tag already exists: {tag}")

        _prepare_bundle(source, bundle, plugin_name, env)
        _require(
            _run(
                compose
                + [
                    "up",
                    "-d",
                    "--wait",
                    "--wait-timeout",
                    "240",
                    "postgres",
                    "valkey",
                    "rustfs-perms",
                    "rustfs",
                    "rustfs-init",
                    "curie-migrate",
                    "curie-api",
                ],
                env=env,
                timeout=300,
            ),
            "starting the private API stack",
        )
        api_url = env["CURIE_API_URL"]
        build = _run(
            [str(binary), "--json", "build", "--plugin-dir", str(bundle)],
            env=env,
            timeout=1200,
        )
        receipt = json.loads(_require(build, "building both source connectors"))
        records = {row["name"]: row for row in receipt["connectors"]}
        if set(records) != {"self-upgrade", "tempo"}:
            raise RuntimeError(f"connector build receipt was incomplete: {receipt}")
        for tag, connector in zip(tags, ("self-upgrade", "tempo"), strict=True):
            inspected = _run(["docker", "image", "inspect", "--format", "{{.Id}}", tag])
            image_id = _require(inspected, f"finding connector image {tag}").strip()
            if image_id != records[connector]["image"]:
                raise RuntimeError(
                    f"connector tag {tag} points to {image_id}, receipt has "
                    f"{records[connector]['image']}"
                )
        _api(api_url, "GET", "/agents")
        yield LocalCase(tier, root, bundle, binary, api_url, env)
    finally:
        connector_ids = _run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                "label=curietech.ai/component=connector",
                "--filter",
                f"label=curietech.ai/project={project}",
            ]
        ).stdout.split()
        if connector_ids:
            removed = _run(["docker", "rm", "-f", *connector_ids])
            if removed.returncode:
                teardown_errors.append(f"connector removal failed: {removed.stderr}")
        runner_ids = _run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                "label=curietech.ai/managed-by=curie-sandbox-substrate",
                "--filter",
                f"network={project}_runner",
            ]
        ).stdout.split()
        if runner_ids:
            removed = _run(["docker", "rm", "-f", *runner_ids])
            if removed.returncode:
                teardown_errors.append(f"runner removal failed: {removed.stderr}")
        down = _run(compose + ["down", "-v", "--remove-orphans"], env=env, timeout=180)
        if down.returncode:
            teardown_errors.append(f"Compose teardown failed: {down.stdout}\n{down.stderr}")
        local_source_tags = [
            f"ghcr.io/curie-eng/curie-{component}:{env['CURIE_LOCAL_IMAGE_TAG']}"
            for component in ("api", "worker", "dispatcher", "runner")
        ]
        local_source_tags.append(f"{project}-curie-worker:latest")
        owned_source_tags = [
            tag
            for tag in local_source_tags
            if _run(["docker", "image", "inspect", tag]).returncode == 0
        ]
        if owned_source_tags:
            image_rm = _run(["docker", "image", "rm", "-f", *owned_source_tags])
            if image_rm.returncode:
                teardown_errors.append(
                    f"source image cleanup failed: {image_rm.stderr}"
                )
        existing_tags = [
            tag
            for tag in tags
            if _run(["docker", "image", "inspect", tag]).returncode == 0
        ]
        if existing_tags:
            image_rm = _run(["docker", "image", "rm", "-f", *existing_tags])
            if image_rm.returncode:
                teardown_errors.append(f"connector image cleanup failed: {image_rm.stderr}")
        survivors = _run(
            ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"]
        ).stdout.split()
        connector_survivors = _run(
            ["docker", "ps", "-aq", "--filter", f"label=curietech.ai/project={project}"]
        ).stdout.split()
        networks = _run(
            [
                "docker",
                "network",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ]
        ).stdout.split()
        volumes = _run(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ]
        ).stdout.split()
        if survivors or connector_survivors or networks or volumes:
            teardown_errors.append(
                "owned resources survived: "
                f"containers={survivors} connectors={connector_survivors} "
                f"networks={networks} volumes={volumes}"
            )
        if teardown_errors:
            raise RuntimeError("; ".join(teardown_errors))


def _drive_deploy(case: LocalCase, *, connector_mode: bool) -> subprocess.CompletedProcess[str]:
    source = LADDER.read_text()
    functions = "\n".join(
        part
        for part in (
            _function(source, "connector_mode"),
            _function(source, "deploy_field"),
            _function(source, "bind_local_connector_approval_routes", optional=True),
            _function(source, "capture_local_deploy", optional=True),
        )
        if part
    )
    block = _deploy_block(source, case.tier)
    connector_bundle = str(case.bundle) if connector_mode else ""
    script = f"""set -euo pipefail
BIN={shlex.quote(str(case.binary))}
WORKDIR={shlex.quote(str(case.root))}
CONNECTOR_BUNDLE={shlex.quote(connector_bundle)}
{functions}
run_deploy() {{
{block}
printf 'SUCCESS_RECEIPT agent=%s deployment=%s digest=%s\\n' "$agent_id" "$deployment_id" "$digest"
}}
trap 'code=$?; printf "EXIT_TRAP status=%s\\n" "$code"; exit "$code"' EXIT
run_deploy
"""
    return _run(["bash", "-c", script], env=case.env, timeout=240)


def _configure_approval_seed_route(
    case: LocalCase,
    agent_id: str,
    channel: str = "C0EXAMPLE1",
) -> subprocess.CompletedProcess[str]:
    source = LADDER.read_text()
    script = f"""set -euo pipefail
BIN={shlex.quote(str(case.binary))}
WORKDIR={shlex.quote(str(case.root))}
{_function(source, "configure_deterministic_approval_seed_route")}
configure_deterministic_approval_seed_route local {shlex.quote(agent_id)} {shlex.quote(channel)}
"""
    return _run(["bash", "-c", script], env=case.env, timeout=120)


def _dedicated_approval_seed_source(source: str) -> str:
    return "\n".join(
        _function(source, name)
        for name in (
            "stop_approval_seed_message",
            "assert_finalized_reply",
            "approval_resume_failure_summary",
            "configure_deterministic_approval_seed_route",
            "prepare_approval_seed_fixture",
            "cleanup_approval_seed_fixture",
            "seed_approval_resume_turn",
        )
    )


def _start_isolated_approval_seed_worker(case: LocalCase) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        case.env["CURIE_LOCAL_STUB_PORT"] = str(probe.getsockname()[1])
    # `local up --build` builds twice, `docker build` for the source images and
    # then compose for the worker overlay, and both must use the daemon's own
    # builder rather than an ambient one. In Docker Desktop's `desktop-linux`
    # context the first refuses the `default` builder and the second refuses
    # `desktop-linux`. Naming the current context's daemon in DOCKER_HOST puts
    # both in the `default` context on that daemon, where `default` is its own
    # builder. On Linux the endpoint is the default socket, so nothing changes.
    endpoint = _require(
        _run(
            ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
            env=case.env,
        ),
        "reading the current Docker context's daemon endpoint",
    ).strip()
    if not endpoint:
        raise RuntimeError("the current Docker context names no daemon endpoint")
    env = dict(case.env)
    env["DOCKER_HOST"] = endpoint
    env["BUILDX_BUILDER"] = "default"
    env["CURIE_FAKE_MODEL"] = "1"
    result = _run(
        [str(case.binary), "--json", "local", "up", "--minimal", "--build"],
        env=env,
        timeout=1800,
    )
    _require(result, "starting the isolated local worker for the approval seed")
    # The seed's `curie local message` enqueues through a one-shot container of
    # the dispatcher image, which it takes from the running API's tag. This
    # fixture runs a private API image, so no dispatcher exists at that tag and
    # compose's own default applies: it must be the dispatcher `local up
    # --build` just built, not whatever `curie-dispatcher:latest` is local.
    candidate = f"ghcr.io/curie-eng/curie-dispatcher:{case.env['CURIE_LOCAL_IMAGE_TAG']}"
    case.env["CURIE_DISPATCHER_IMAGE"] = candidate
    rendered = json.loads(
        _require(
            _run(
                [
                    "docker",
                    "compose",
                    "--profile",
                    "core",
                    "--profile",
                    "slack",
                    "config",
                    "--format",
                    "json",
                ],
                env=case.env,
            ),
            "rendering the one-shot dispatcher's Compose",
        )
    )
    image = rendered["services"]["curie-dispatcher"].get("image")
    if image != candidate:
        raise RuntimeError(
            f"the approval seed would enqueue through {image}, not the candidate {candidate}"
        )
    _require(_run(["docker", "image", "inspect", candidate]), "finding the candidate dispatcher")


def _drive_dedicated_approval_seed(
    case: LocalCase,
    functions: str,
    parity_agent_id: str,
) -> subprocess.CompletedProcess[str]:
    script = f"""set -euo pipefail
BIN={shlex.quote(str(case.binary))}
WORKDIR={shlex.quote(str(case.root))}
LIVE=0
APPROVAL_SEED_MESSAGE_PID=''
APPROVAL_SEED_AGENT_ID=''
APPROVAL_SEED_CHANNEL=''
{functions}
capture_stream_cursor() {{ printf 'stream-cursor'; }}
discover_trace_id_for_seed() {{ printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'; }}
query_exact_seed_trace() {{ LAST_QUERY_MEMBERSHIP=true; return 0; }}
cleanup_seed() {{
    local code=$?
    cleanup_approval_seed_fixture || code=$?
    exit "$code"
}}
trap cleanup_seed EXIT
seed_approval_resume_turn local {shlex.quote(parity_agent_id)} stub
python3 - "$CURIE_API_URL" "$APPROVAL_SEED_AGENT_ID" "$APPROVAL_SEED_CHANNEL" <<'PY'
import json
import os
import sys
import urllib.request

api_base, agent_id, channel = sys.argv[1:]
if not agent_id:
    raise SystemExit("approval seed did not expose APPROVAL_SEED_AGENT_ID")
if channel != "C0E2EAPPROVAL":
    raise SystemExit("approval seed did not expose its dedicated Slack channel")
request = urllib.request.Request(
    api_base.rstrip("/") + "/agents/" + agent_id,
    headers={{"Accept": "application/json", "X-API-Key": os.environ["CURIE_API_KEY"]}},
)
with urllib.request.urlopen(request, timeout=30) as response:
    agent = json.load(response)
route = agent.get("approval_routes", {{}}).get("e2e")
expected = {{
    "resolution": {{"kind": "slack", "address": channel}},
    "notification": None,
    "approvers": {{"group": None, "users": ["U0EXAMPLE1"]}},
}}
if agent.get("name", "").startswith("approval-seed-") is False:
    raise SystemExit("approval seed did not create an owned agent name")
if route != expected:
    raise SystemExit("approval seed did not bind the dedicated explicit-user e2e route")
for path in ("/agents/" + agent_id + "/versions", "/deployments?agent_id=" + agent_id):
    request = urllib.request.Request(
        api_base.rstrip("/") + path,
        headers={{"Accept": "application/json", "X-API-Key": os.environ["CURIE_API_KEY"]}},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if not json.load(response):
            raise SystemExit("approval seed fixture did not deploy its owned agent")
print("DEDICATED_APPROVAL_SEED_FINALIZED agent=" + agent_id + " channel=" + channel)
PY
"""
    return _run(["bash", "-c", script], env=case.env, timeout=300)


def _approval_seed_failure_category(result: subprocess.CompletedProcess[str]) -> str:
    output = result.stdout + result.stderr
    if "awaiting-approval record did not become pending" in output:
        return "pending_approval_missing"
    if "deterministic approval resolution command failed" in output:
        return "approval_resolution_failed"
    if "approval resolution did not resume to a final reply" in output:
        return "finalized_reply_missing"
    if "could not prepare dedicated approval fixture" in output:
        return "fixture_preparation_failed"
    return "unclassified"


def _json_objects(text: str) -> list[object]:
    decoder = json.JSONDecoder()
    values: list[object] = []
    offset = 0
    while True:
        start = text.find("{", offset)
        if start < 0:
            return values
        try:
            value, length = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            offset = start + 1
            continue
        values.append(value)
        offset = start + length


def _add_second_route(bundle: pathlib.Path) -> None:
    manifest_path = bundle / ".claude-plugin/plugin.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["version"] = "0.1.1"
    manifest["approvalPolicy"]["gates"].append(
        {"gate": "mcp__tempo__search_traces", "route": "incident-approvals"}
    )
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


@pytest.mark.parametrize("local_case", ["local", "local-release"], indirect=True)
def test_connector_local_deploy_binds_retained_routes_and_preserves_refusal_json(
    local_case: LocalCase,
) -> None:
    refused = _drive_deploy(local_case, connector_mode=False)
    assert refused.returncode == 2, refused.stdout + refused.stderr
    assert refused.stdout.rstrip().endswith("EXIT_TRAP status=2"), refused.stdout
    receipt_text = refused.stdout.rsplit("EXIT_TRAP status=2", 1)[0]
    refusal_values = _json_objects(receipt_text)
    assert refusal_values, refused.stdout
    refusal = refusal_values[0]
    refusal_rendered = json.dumps(refusal, sort_keys=True)
    assert "sre-approvals" in refusal_rendered
    assert "curie local approvals" in refusal_rendered

    agents = _api(local_case.api_url, "GET", "/agents")
    assert isinstance(agents, list) and len(agents) == 1
    agent = agents[0]
    agent_id = agent["id"]
    assert _api(local_case.api_url, "GET", f"/agents/{agent_id}/versions") == []
    assert _api(local_case.api_url, "GET", f"/deployments?agent_id={agent_id}") == []
    _api(local_case.api_url, "DELETE", f"/agents/{agent_id}")
    assert _api(local_case.api_url, "GET", "/agents") == []

    stock = _drive_deploy(local_case, connector_mode=True)
    assert stock.returncode == 0, stock.stdout + stock.stderr
    assert stock.stdout.rstrip().endswith("EXIT_TRAP status=0"), stock.stdout
    stock_receipts = [
        value
        for value in _json_objects(stock.stdout)
        if isinstance(value, dict) and "deployment" in value and "bundle" in value
    ]
    assert len(stock_receipts) == 1, stock.stdout
    stock_receipt = stock_receipts[0]
    agent_id = stock_receipt["agent"]["id"]
    stock_agent = _api(local_case.api_url, "GET", f"/agents/{agent_id}")
    assert set(stock_agent["approval_routes"]) == {"sre-approvals"}

    retained_sre = {
        "resolution": {"kind": "slack", "address": "C0SREBOT"},
        "approvers": {"users": ["U0EXAMPLE1"]},
    }
    retained_other = {
        "resolution": {"kind": "slack", "address": "C0EXAMPLE2"},
        "approvers": {"group": "S0EXAMPLE1"},
    }
    before_approval_seed = _api(
        local_case.api_url,
        "PATCH",
        f"/agents/{agent_id}",
        {
            "approval_routes": {
                "sre-approvals": retained_sre,
                EXISTING_ROUTE: retained_other,
            }
        },
    )
    approval_seed = _configure_approval_seed_route(local_case, agent_id)
    assert approval_seed.returncode == 0, approval_seed.stdout + approval_seed.stderr
    seeded_agent = _api(local_case.api_url, "GET", f"/agents/{agent_id}")
    assert set(seeded_agent["approval_routes"]) == {
        "e2e",
        "sre-approvals",
        EXISTING_ROUTE,
    }
    assert seeded_agent["approval_routes"]["sre-approvals"] == before_approval_seed[
        "approval_routes"
    ]["sre-approvals"]
    assert seeded_agent["approval_routes"][EXISTING_ROUTE] == before_approval_seed[
        "approval_routes"
    ][EXISTING_ROUTE]
    assert seeded_agent["approval_routes"]["e2e"] == {
        "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
        "notification": None,
        "approvers": {"group": None, "users": ["U0EXAMPLE1"]},
    }

    existing = {"resolution": {"kind": "slack", "address": "C0EXAMPLE2"}}
    retained_with_notification = {
        "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
        "notification": {"kind": "slack", "address": "C0EXAMPLE4"},
        "approvers": {"users": ["U0EXAMPLE1"]},
    }
    seeded = _api(
        local_case.api_url,
        "PATCH",
        f"/agents/{agent_id}",
        {
            "approval_routes": {
                EXISTING_ROUTE: existing,
                "sre-approvals": retained_with_notification,
            }
        },
    )
    seeded_routes = seeded["approval_routes"]
    notification_refused = _configure_approval_seed_route(local_case, agent_id)
    assert notification_refused.returncode == 1, (
        notification_refused.stdout + notification_refused.stderr
    )
    assert "carry notification targets" in notification_refused.stderr, (
        notification_refused.stdout + notification_refused.stderr
    )
    assert (
        _api(local_case.api_url, "GET", f"/agents/{agent_id}")["approval_routes"]
        == seeded_routes
    )

    _add_second_route(local_case.bundle)
    guarded = _drive_deploy(local_case, connector_mode=True)
    assert guarded.returncode == 1, guarded.stdout + guarded.stderr
    assert "carry notification targets" in guarded.stderr, guarded.stdout + guarded.stderr
    assert "incident-approvals" in guarded.stderr, guarded.stdout + guarded.stderr
    guarded_agent = _api(local_case.api_url, "GET", f"/agents/{agent_id}")
    assert guarded_agent["approval_routes"] == seeded_routes
    guarded_versions = _api(local_case.api_url, "GET", f"/agents/{agent_id}/versions")
    guarded_deployments = _api(
        local_case.api_url,
        "GET",
        f"/deployments?agent_id={agent_id}",
    )
    assert len(guarded_versions) == 1
    assert {row["id"] for row in guarded_deployments} == {
        stock_receipt["deployment"]["id"]
    }

    safe_retained = {
        "resolution": retained_with_notification["resolution"],
        "approvers": retained_with_notification["approvers"],
    }
    safe_seeded = _api(
        local_case.api_url,
        "PATCH",
        f"/agents/{agent_id}",
        {
            "approval_routes": {
                EXISTING_ROUTE: existing,
                "sre-approvals": safe_retained,
            }
        },
    )
    deployed = _drive_deploy(local_case, connector_mode=True)
    assert deployed.returncode == 0, deployed.stdout + deployed.stderr
    assert deployed.stdout.rstrip().endswith("EXIT_TRAP status=0"), deployed.stdout
    payloads = [
        value
        for value in _json_objects(deployed.stdout)
        if isinstance(value, dict) and "deployment" in value and "bundle" in value
    ]
    assert len(payloads) == 1, deployed.stdout
    receipt = payloads[0]
    assert receipt["agent"]["id"] == agent_id
    assert receipt["agent"]["name"] == stock_receipt["agent"]["name"]
    assert receipt["deployment"]["id"]
    assert receipt["bundle"]["sha256"]

    stored = _api(local_case.api_url, "GET", f"/agents/{agent_id}")
    routes = stored["approval_routes"]
    assert set(routes) == EXPECTED_ROUTES | {EXISTING_ROUTE}
    assert routes[EXISTING_ROUTE] == safe_seeded["approval_routes"][EXISTING_ROUTE]
    assert routes["sre-approvals"] == safe_seeded["approval_routes"]["sre-approvals"]
    assert routes["incident-approvals"]["resolution"]["address"] == "C0LOCALDEV"
    versions = _api(local_case.api_url, "GET", f"/agents/{agent_id}/versions")
    deployments = _api(local_case.api_url, "GET", f"/deployments?agent_id={agent_id}")
    assert len(versions) == 2
    assert {row["id"] for row in deployments} == {
        stock_receipt["deployment"]["id"],
        receipt["deployment"]["id"],
    }


@pytest.mark.parametrize("local_case", ["local"], indirect=True)
def test_dedicated_approval_seed_prepares_and_resumes_an_owned_local_agent(
    local_case: LocalCase,
) -> None:
    functions = _dedicated_approval_seed_source(LADDER.read_text())
    suffix = uuid.uuid4().hex[:10]
    sentinel = _api(
        local_case.api_url,
        "POST",
        "/agents",
        {
            "name": f"approval-seed-parity-{suffix}",
            "channel": {"kind": "slack", "address": f"C0{suffix.upper()}"},
            "approval_routes": {
                "restricted": {
                    "resolution": {"kind": "slack", "address": "C0PARITYROUTE"},
                    "approvers": {"users": ["U0PARITYUSER"]},
                }
            },
        },
    )
    parity_agent_id = sentinel["id"]
    before_routes = sentinel["approval_routes"]
    assert _api(local_case.api_url, "GET", f"/agents/{parity_agent_id}/versions") == []
    assert _api(local_case.api_url, "GET", f"/deployments?agent_id={parity_agent_id}") == []
    _start_isolated_approval_seed_worker(local_case)

    try:
        completed = _drive_dedicated_approval_seed(local_case, functions, parity_agent_id)
        assert completed.returncode == 0, (
            "dedicated approval lifecycle failed with category="
            f"{_approval_seed_failure_category(completed)}"
        )
        match = re.search(
            r"DEDICATED_APPROVAL_SEED_FINALIZED agent=([^\s]+) channel=(C0E2EAPPROVAL)",
            completed.stdout,
        )
        assert match, completed.stdout
        agent_id, channel = match.groups()
        assert channel == "C0E2EAPPROVAL"
        agents = _api(local_case.api_url, "GET", "/agents")
        assert all(agent["id"] != agent_id for agent in agents)
        parity_agent = _api(local_case.api_url, "GET", f"/agents/{parity_agent_id}")
        assert parity_agent["approval_routes"] == before_routes
        assert _api(local_case.api_url, "GET", f"/agents/{parity_agent_id}/versions") == []
        assert _api(
            local_case.api_url,
            "GET",
            f"/deployments?agent_id={parity_agent_id}",
        ) == []
    finally:
        _api(local_case.api_url, "DELETE", f"/agents/{parity_agent_id}")
