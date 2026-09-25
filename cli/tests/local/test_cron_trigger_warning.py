"""Real service proof for cron trigger reporting on skill check and local deploy."""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import time
import urllib.request
import uuid
from dataclasses import dataclass

import pytest

REPO = pathlib.Path(__file__).parents[3]
API_KEY = "curie-dev-key"
AGENT_NAME = "acme-cron-warning"


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


@pytest.fixture(scope="session")
def curie_binary(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    supplied = os.environ.get("CURIE_BIN")
    if supplied:
        binary = pathlib.Path(supplied).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not executable: {binary}")
        return binary

    build = _run(
        ["cargo", "build", "--release", "--locked"],
        cwd=REPO / "cli",
        timeout=1200,
    )
    _require(build, "building the source CLI")
    metadata = _run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=REPO / "cli",
    )
    target = pathlib.Path(
        json.loads(_require(metadata, "reading the Cargo target"))["target_directory"]
    )
    built = target / "release/curie"
    if not built.is_file():
        raise RuntimeError(f"built CLI is missing: {built}")
    owned = tmp_path_factory.mktemp("cron-warning-cli") / "curie"
    shutil.copy2(built, owned)
    owned.chmod(0o755)
    return owned


@dataclass(frozen=True)
class Case:
    name: str
    bundle: pathlib.Path
    valid: bool
    expects_warning: bool


@dataclass(frozen=True)
class Runtime:
    binary: pathlib.Path
    api_url: str
    runner_image: str
    env: dict[str, str]
    cases: tuple[Case, ...]


def _write_bundle(path: pathlib.Path, triggers: object) -> None:
    manifest_dir = path / ".claude-plugin"
    skill_dir = path / "skills/hello"
    manifest_dir.mkdir(parents=True)
    skill_dir.mkdir(parents=True)
    manifest: dict[str, object] = {
        "name": AGENT_NAME,
        "version": "1.0.0",
        "description": "Cron warning verification",
    }
    if triggers is not None:
        manifest["triggers"] = triggers
    (manifest_dir / "plugin.json").write_text(json.dumps(manifest) + "\n")
    (skill_dir / "SKILL.md").write_text(
        "---\nname: hello\ndescription: Say hello\n---\nSay hello.\n"
    )


def _compose_endpoint(compose: list[str], service: str, port: int) -> str:
    result = _run([*compose, "port", service, str(port)])
    return _require(result, f"finding the private {service} endpoint").strip()


@pytest.fixture
def runtime(tmp_path: pathlib.Path, curie_binary: pathlib.Path):
    suffix = uuid.uuid4().hex[:10]
    project = f"curie-check-2876-{suffix}"
    override = tmp_path / "compose.override.yaml"
    override.write_text(
        f"""services:
  postgres:
    ports: !override ["127.0.0.1::5432"]
  valkey:
    ports: !override ["127.0.0.1::6379"]
  rustfs:
    ports: !override ["127.0.0.1::9000", "127.0.0.1::9001"]
networks:
  curie_runner:
    name: {project}_runner
"""
    )
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(REPO / "compose.dev.yaml"),
        "-f",
        str(override),
    ]
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    env = os.environ.copy()
    env.update(
        {
            "API_KEY": API_KEY,
            "CURIE_CONFIG_DIR": str(config_dir),
            "DB_SCHEMA": "curie",
            "ENVIRONMENT": "dev",
            "GITHUB_FACTORY_INGRESS_ENABLED": "false",
            "GITHUB_APP_ID": "",
            "GITHUB_APP_PRIVATE_KEY": "",
            "GITHUB_REVIEW_INGRESS_ENABLED": "false",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "",
            "S3_ACCESS_KEY": "rustfs",
            "S3_SECRET_KEY": "rustfssecret",
            "SLACK_BOT_TOKEN": "",
            "VALKEY_PASSWORD": "valkeypass",
        }
    )
    runner_ref = os.environ.get("CURIE_CRON_RUNNER_IMAGE", "curie-runner:latest")
    runner_image = ""
    api: subprocess.Popen[str] | None = None
    api_log = None
    api_log_path = tmp_path / "api.log"
    listener: socket.socket | None = None
    owned_container_ids: list[str] = []
    teardown_errors: list[str] = []

    cases = (
        Case("cron", tmp_path / "cron", True, True),
        Case("none", tmp_path / "none", True, False),
        Case("malformed", tmp_path / "malformed", False, False),
    )
    _write_bundle(
        cases[0].bundle,
        [
            {
                "name": "acme-nightly",
                "type": "cron",
                "schedule": "0 2 * * *",
                "prompt": "Summarize the nightly build.",
            }
        ],
    )
    _write_bundle(cases[1].bundle, None)
    _write_bundle(
        cases[2].bundle,
        [
            {
                "name": "acme-nightly",
                "type": "cron",
                "prompt": "Summarize the nightly build.",
            }
        ],
    )

    try:
        image = _run(["docker", "image", "inspect", runner_ref, "--format", "{{.Id}}"])
        runner_image = _require(image, f"finding runner image {runner_ref}").strip()
        rendered = json.loads(
            _require(
                _run([*compose, "--profile", "full", "config", "--format", "json"]),
                "rendering private Compose",
            )
        )
        for service in ("postgres", "valkey", "rustfs"):
            ports = rendered["services"][service].get("ports", [])
            if not ports or any(
                port.get("host_ip") != "127.0.0.1" or port.get("published") for port in ports
            ):
                raise RuntimeError(f"{service} does not use random loopback ports: {ports}")
        if rendered["networks"]["curie_runner"]["name"] != f"{project}_runner":
            raise RuntimeError("private runner network name was not applied")

        _require(
            _run(
                [*compose, "up", "-d", "--wait", "postgres", "valkey", "rustfs"],
                env=env,
                timeout=300,
            ),
            "starting private backing services",
        )
        owned_container_ids = _require(
            _run(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            ),
            "recording owned containers",
        ).split()
        if not owned_container_ids:
            raise RuntimeError(f"Compose project {project} started no containers")

        postgres = _compose_endpoint(compose, "postgres", 5432)
        valkey = _compose_endpoint(compose, "valkey", 6379)
        rustfs = _compose_endpoint(compose, "rustfs", 9000)
        env.update(
            {
                "DATABASE_URL": f"postgresql+asyncpg://postgres:postgres@{postgres}/postgres",
                "S3_ENDPOINT_URL": f"http://{rustfs}",
                "VALKEY_HOST": "127.0.0.1",
                "VALKEY_PORT": valkey.rsplit(":", 1)[1],
            }
        )
        _require(
            _run(
                ["uv", "run", "alembic", "upgrade", "head"],
                cwd=REPO / "apps/api",
                env=env,
            ),
            "migrating the private database",
        )

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        api_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        api_log = api_log_path.open("w")
        api = subprocess.Popen(
            ["uv", "run", "uvicorn", "curie_api.main:app", "--fd", str(listener.fileno())],
            cwd=REPO / "apps/api",
            env=env,
            pass_fds=(listener.fileno(),),
            stdout=api_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for _ in range(60):
            if api.poll() is not None:
                api_log.flush()
                raise RuntimeError(f"source API exited during startup:\n{api_log_path.read_text()}")
            try:
                with urllib.request.urlopen(f"{api_url}/health", timeout=1) as response:
                    if response.status == 200:
                        break
            except Exception:
                time.sleep(1)
        else:
            api_log.flush()
            raise RuntimeError(f"source API never became healthy:\n{api_log_path.read_text()}")

        yield Runtime(curie_binary, api_url, runner_image, env, cases)
    finally:
        if api is not None:
            api.terminate()
            try:
                api.wait(timeout=15)
            except subprocess.TimeoutExpired:
                api.kill()
                api.wait(timeout=15)
            if api.returncode not in (
                0,
                -signal.SIGTERM,
                128 + signal.SIGTERM,
            ):
                teardown_errors.append(f"source API exited with {api.returncode}")
        if api_log is not None:
            api_log.close()
        if listener is not None:
            listener.close()

        down = _run(
            [*compose, "--profile", "full", "down", "-v", "--remove-orphans"],
            env=env,
            timeout=180,
        )
        if down.returncode:
            teardown_errors.append(
                f"Compose teardown for {project} failed:\n{down.stdout}\n{down.stderr}"
            )
        exact_survivors = [
            container_id
            for container_id in owned_container_ids
            if _run(["docker", "inspect", container_id]).returncode == 0
        ]
        project_survivors = _require(
            _run(
                [
                    "docker",
                    "ps",
                    "-aq",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            ),
            f"checking container cleanup for {project}",
        ).split()
        networks = _require(
            _run(
                [
                    "docker",
                    "network",
                    "ls",
                    "-q",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            ),
            f"checking network cleanup for {project}",
        ).split()
        named_network = _run(["docker", "network", "inspect", f"{project}_runner"])
        volumes = _require(
            _run(
                [
                    "docker",
                    "volume",
                    "ls",
                    "-q",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                ]
            ),
            f"checking volume cleanup for {project}",
        ).split()
        if (
            exact_survivors
            or project_survivors
            or networks
            or named_network.returncode == 0
            or volumes
        ):
            teardown_errors.append(
                f"owned resources survived for {project}: recorded={owned_container_ids} "
                f"exact={exact_survivors} project={project_survivors} "
                f"networks={networks} named_network={named_network.returncode == 0} "
                f"volumes={volumes}"
            )
        if teardown_errors:
            raise RuntimeError("; ".join(teardown_errors))


def _json_lines(text: str) -> list[object]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def test_cron_trigger_warning_across_skill_and_local_deploy(runtime: Runtime) -> None:
    for case in runtime.cases:
        for surface in ("skill", "local"):
            args = [
                str(runtime.binary),
                "--color",
                "never",
                "--json",
                surface,
                "check" if surface == "skill" else "deploy",
                "--plugin-dir",
                str(case.bundle),
            ]
            if surface == "skill":
                args.extend(["--image", runtime.runner_image, "--timeout", "30"])
            else:
                args.extend(
                    [
                        "--api-url",
                        runtime.api_url,
                        "--api-key",
                        API_KEY,
                        "--agent",
                        AGENT_NAME,
                        "--label",
                        f"{case.name}-proof",
                    ]
                )

            result = _run(args, env=runtime.env, timeout=90)
            warnings = [line for line in result.stderr.splitlines() if "cron trigger" in line]
            # The worker scheduler fires cron triggers on local installs, so only
            # the skill tier, which has no scheduler, reports a declared cron.
            expects_warning = case.expects_warning and surface == "skill"

            assert len(warnings) == (1 if expects_warning else 0), (
                case.name,
                surface,
                result.stdout,
                result.stderr,
            )
            if expects_warning:
                assert "acme-nightly" in warnings[0]
                assert "the skill tier has no scheduler" in warnings[0]
            if case.valid:
                assert result.returncode == 0, (
                    case.name,
                    surface,
                    result.stdout,
                    result.stderr,
                )
                assert len(_json_lines(result.stdout)) == 1
            else:
                assert result.returncode != 0, (case.name, surface, result.stdout)
                assert "schedule" in result.stdout + result.stderr
                assert _json_lines(result.stdout)
