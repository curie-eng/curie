"""Source CLI console login against a private API and its real backing stores.

Each test owns a fresh Compose project and credential store. Login codes issued
through either CLI tier are exchanged through the real console session route.
Only the external kubectl process is replaced for cluster credential discovery.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from http.client import HTTPMessage

import pytest

REPO = pathlib.Path(__file__).parents[3]
SUBJECT = "operator@example.com"
INSTALL_KEY = "fixture_console_login_install_key"
POISONED_KEY = "fixture_ambient_key_that_the_api_refuses"
NAMESPACE = "acme-system"
RELEASE = "acme-release"


def _run(argv, *, env=None, cwd=REPO, timeout=300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout, check=False
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode:
        raise RuntimeError(
            f"{purpose} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


@pytest.fixture(scope="session")
def source_artifacts(tmp_path_factory: pytest.TempPathFactory):
    owned = tmp_path_factory.mktemp("console-login-artifacts")
    binary_value = os.environ.get("CURIE_BIN")
    if binary_value:
        binary = pathlib.Path(binary_value).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not an executable file: {binary}")
    else:
        _require(
            _run(["cargo", "build", "--locked"], cwd=REPO / "cli", timeout=1200),
            "building the source CLI",
        )
        metadata = _run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=REPO / "cli"
        )
        target = pathlib.Path(
            json.loads(_require(metadata, "reading the Cargo target"))["target_directory"]
        )
        binary = owned / "curie"
        shutil.copy2(target / "debug/curie", binary)
        binary.chmod(0o755)

    _require(_run(["docker", "info", "--format", "{{.ServerVersion}}"]), "reaching Docker")
    supplied_image = os.environ.get("CURIE_LOCAL_DEPLOY_API_IMAGE")
    api_image = supplied_image or f"curie-console-login-api:{uuid.uuid4().hex[:12]}"
    if supplied_image:
        _require(_run(["docker", "image", "inspect", api_image]), "finding the supplied API image")
        yield binary, api_image
        return
    try:
        _require(
            _run(
                ["docker", "build", "-f", "apps/api/Dockerfile", "-t", api_image, "."],
                timeout=1200,
            ),
            "building the source API image",
        )
        yield binary, api_image
    finally:
        if _run(["docker", "image", "inspect", api_image]).returncode == 0:
            _require(
                _run(["docker", "image", "rm", "-f", api_image]),
                "removing the owned API image",
            )


@dataclass
class Stack:
    binary: pathlib.Path
    api_url: str
    env: dict[str, str]
    kubectl_log: pathlib.Path


def _write_kubectl(directory: pathlib.Path) -> pathlib.Path:
    directory.mkdir()
    kubectl = directory / "kubectl"
    kubectl.write_text(
        """#!/usr/bin/env python3
import os
import sys

args = sys.argv[1:]
with open(os.environ["CURIE_TEST_KUBECTL_LOG"], "a", encoding="utf-8") as log:
    log.write(" ".join(args) + "\\n")
if "get" in args and "secret" in args:
    if "-l" in args:
        print("acme-release-curie-secrets", end="")
        sys.exit(0)
    if any("apiKey" in arg for arg in args):
        print(os.environ["CURIE_TEST_DISCOVERED_KEY"], end="")
        sys.exit(0)
print("unexpected fixture kubectl invocation", file=sys.stderr)
sys.exit(64)
"""
    )
    kubectl.chmod(0o755)
    return directory


@pytest.fixture
def stack(request: pytest.FixtureRequest, tmp_path: pathlib.Path, source_artifacts):
    binary, api_image = source_artifacts
    api_key = getattr(request, "param", INSTALL_KEY)
    project = f"curie-console-login-{uuid.uuid4().hex[:10]}"
    config_dir = tmp_path / "config"
    credential_dir = config_dir / "local"
    credential_dir.mkdir(parents=True, mode=0o700)
    config_dir.chmod(0o700)
    credential_file = credential_dir / f"{project}.json"
    credential_file.write_text(
        json.dumps({"api_key": INSTALL_KEY, "postgres_password": "postgres"})
    )
    credential_file.chmod(0o600)
    tools = _write_kubectl(tmp_path / "tools")
    kubectl_log = tmp_path / "kubectl.log"
    override = tmp_path / "compose.override.yaml"
    override.write_text(
        f"""services:
  postgres:
    ports: !override ["127.0.0.1::5432"]
  valkey:
    ports: !override ["127.0.0.1::6379"]
  rustfs:
    ports: !override ["127.0.0.1::9000", "127.0.0.1::9001"]
  curie-migrate:
    image: {api_image}
    pull_policy: never
  curie-api:
    image: {api_image}
    pull_policy: never
    environment:
      API_KEY: {api_key}
      GITHUB_REVIEW_INGRESS_ENABLED: "false"
      GITHUB_FACTORY_INGRESS_ENABLED: "false"
      GITHUB_APP_ID: ""
      GITHUB_APP_PRIVATE_KEY: ""
      SLACK_BOT_TOKEN: ""
      OTEL_EXPORTER_OTLP_ENDPOINT: ""
      OTEL_EXPORTER_OTLP_PROTOCOL: ""
    ports: !override ["127.0.0.1::8000"]
  curie-worker:
    environment:
      DATABASE_URL: "postgresql+asyncpg://postgres:postgres@${{CURIE_LOCAL_POSTGRES_HOST}}:${{CURIE_LOCAL_POSTGRES_PORT}}/postgres"
      VALKEY_HOST: "${{VALKEY_HOST}}"
      VALKEY_PORT: "${{VALKEY_PORT}}"
      S3_ENDPOINT_URL: "${{S3_ENDPOINT_URL}}"
      CURIE_API_URL: "${{CURIE_API_URL}}"
      CURIE_DOCKER_NETWORK: "${{CURIE_DOCKER_NETWORK}}"
      TMPDIR: "${{CURIE_LOCAL_STAGING_DIR}}"
      SLACK_API_BASE_URL: "http://127.0.0.1:${{CURIE_LOCAL_STUB_PORT}}/api/"
      OTEL_EXPORTER_OTLP_ENDPOINT: ""
    volumes: !override
      - /var/run/docker.sock:/var/run/docker.sock
      - "${{CURIE_LOCAL_STAGING_DIR}}:${{CURIE_LOCAL_STAGING_DIR}}"
networks:
  curie_runner:
    name: "{project}_runner"
"""
    )
    env = os.environ.copy()
    for name in (
        "CURIE_API_KEY",
        "CURIE_API_URL",
        "CURIE_LOCAL_API_KEY",
        "CURIE_LOCAL_POSTGRES_PASSWORD",
        "COMPOSE_FILE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        env.pop(name, None)
    env.update(
        {
            "COMPOSE_PROJECT_NAME": project,
            "COMPOSE_FILE": f"{REPO / 'compose.dev.yaml'}:{override}",
            "CURIE_CONFIG_DIR": str(config_dir),
            "CURIE_LOCAL_IMAGE_TAG": f"test-{project}",
            "CURIE_API_URL": "http://127.0.0.1:1",
            "VALKEY_HOST": "127.0.0.1",
            "VALKEY_PORT": "1",
            "S3_ENDPOINT_URL": "http://127.0.0.1:1",
            "CURIE_DOCKER_NETWORK": f"{project}_runner",
            "CURIE_LOCAL_POSTGRES_HOST": "127.0.0.1",
            "CURIE_LOCAL_POSTGRES_PORT": "1",
            "CURIE_LOCAL_STUB_PORT": "1",
            "CURIE_LOCAL_STAGING_DIR": str(tmp_path / "staging"),
            "CURIE_API_KEY": POISONED_KEY,
            "CURIE_TEST_DISCOVERED_KEY": api_key,
            "CURIE_TEST_KUBECTL_LOG": str(kubectl_log),
            "PATH": str(tools) + os.pathsep + env.get("PATH", ""),
            "NO_COLOR": "1",
        }
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
        "--profile",
        "core",
    ]

    def teardown() -> None:
        _require(
            _run(compose + ["down", "-v", "--remove-orphans"], env=env),
            "stopping the private console stack",
        )
        owned_resources = (
            ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"],
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ],
            [
                "docker",
                "network",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ],
        )
        for argv in owned_resources:
            if _require(_run(argv), "checking private stack cleanup").strip():
                raise RuntimeError(f"private project {project} left resources behind")
        print(f"Verified cleanup of private Compose project {project}")

    # This finally is installed before any service can start, including a
    # partially failed startup. Environmental failures stay fixture errors.
    try:
        _require(
            _run(["docker", "ps", "--format", "{{.ID}} {{.Names}} {{.Ports}}"]),
            "inventorying running containers and ports",
        )
        _require(
            _run(["docker", "network", "ls", "--format", "{{.ID}} {{.Name}}"]),
            "inventorying existing networks",
        )
        rendered = json.loads(
            _require(_run(compose + ["config", "--format", "json"], env=env), "rendering Compose")
        )
        if rendered["networks"]["curie_runner"]["name"] != f"{project}_runner":
            raise RuntimeError("private runner network name was not applied")
        for service in ("postgres", "valkey", "rustfs", "curie-api"):
            for port in rendered["services"][service].get("ports", []):
                if port.get("host_ip") != "127.0.0.1" or port.get("published") not in (
                    None, "", "0", 0
                ):
                    raise RuntimeError(f"{service} retained a shared published port")
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
            "starting the private console API stack",
        )
        port = _require(
            _run(compose + ["port", "curie-api", "8000"], env=env), "finding the API port"
        )
        api_url = f"http://127.0.0.1:{port.strip().rsplit(':', 1)[1]}"
        env["CURIE_API_URL"] = api_url
        for service, internal, name in (
            ("postgres", "5432", "CURIE_LOCAL_POSTGRES_PORT"),
            ("valkey", "6379", "VALKEY_PORT"),
            ("rustfs", "9000", "S3_ENDPOINT_URL"),
        ):
            mapping = _require(
                _run(compose + ["port", service, internal], env=env),
                f"finding the private {service} port",
            ).strip()
            published = mapping.rsplit(":", 1)[1]
            env[name] = f"http://127.0.0.1:{published}" if name == "S3_ENDPOINT_URL" else published
        yield Stack(binary, api_url, env, kubectl_log)
    finally:
        teardown()


def _exchange(api_url: str, code: str) -> tuple[int, dict, HTTPMessage]:
    request = urllib.request.Request(
        f"{api_url}/console/session",
        data=json.dumps({"code": code}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # The exchange has no platform key or cookie. The minted code is the sole
    # credential and reaches the same API process and Postgres row as the CLI.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=10) as response:
            return response.status, json.load(response), response.headers
    except urllib.error.HTTPError as error:
        return error.code, json.load(error), error.headers


@pytest.mark.parametrize("tier", ["local", "cluster"])
def test_console_login_mints_human_and_json_codes_that_exchange_once(
    stack: Stack, tier: str
) -> None:
    codes = []
    for json_output in (False, True):
        argv = [str(stack.binary)]
        if json_output:
            argv.append("--json")
        argv.extend([tier, "console", "login", "--subject", SUBJECT])
        if tier == "cluster":
            argv.extend(
                ["--namespace", NAMESPACE, "--release", RELEASE, "--api-url", stack.api_url]
            )
        result = _run(argv, env=stack.env)
        rendered = result.stdout + result.stderr
        assert result.returncode == 0, rendered
        assert INSTALL_KEY not in rendered and POISONED_KEY not in rendered
        if json_output:
            wrapper = json.loads(result.stdout)
            assert set(wrapper) == {"console_login_code"}, wrapper
            delivery = wrapper["console_login_code"]
            assert set(delivery) == {"code", "subject", "expires_at"}, delivery
            assert delivery["subject"] == SUBJECT
            expiry = datetime.fromisoformat(delivery["expires_at"].replace("Z", "+00:00"))
            # Real POST /console/login-codes responses observed on 2026-10-04
            # encode the API's UTC expiry without a timezone offset.
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=UTC)
            assert expiry > datetime.now(UTC)
            code = delivery["code"]
        else:
            code = result.stdout.strip()
            assert result.stdout == f"{code}\n", "human stdout must contain only one code"
            assert code and not any(character.isspace() for character in code)
            assert SUBJECT not in result.stdout
        assert code and code not in codes
        codes.append(code)

        status, session, headers = _exchange(stack.api_url, code)
        assert status == 200, session
        assert set(session) == {"subject", "expires_at"}, session
        assert session["subject"] == SUBJECT
        cookie = headers.get("Set-Cookie", "")
        assert cookie.startswith("__Host-curie_console_session=")
        assert "HttpOnly" in cookie and "Secure" in cookie and "SameSite=strict" in cookie
        consumed_status, consumed, _ = _exchange(stack.api_url, code)
        assert consumed_status == 401, consumed
        assert consumed == {"detail": "invalid or expired login code"}

    # The credential selected by this tier must authorize the mint. A valid
    # store or discovered release key is restored before the liveness control.
    invalid_key = "fixture_invalid_console_login_key"
    invalid_env = dict(stack.env)
    credential_file = (
        pathlib.Path(stack.env["CURIE_CONFIG_DIR"])
        / "local"
        / f"{stack.env['COMPOSE_PROJECT_NAME']}.json"
    )
    stored = credential_file.read_text()
    if tier == "local":
        invalid_credentials = json.loads(stored)
        invalid_credentials["api_key"] = invalid_key
        credential_file.write_text(json.dumps(invalid_credentials))
    else:
        invalid_env["CURIE_TEST_DISCOVERED_KEY"] = invalid_key
    try:
        rejected = _run(argv, env=invalid_env)
        rejection = rejected.stdout + rejected.stderr
        assert rejected.returncode != 0, "an invalid tier credential minted a code"
        assert "401" in rejection, rejection
        for key in (invalid_key, INSTALL_KEY, POISONED_KEY):
            assert key not in rejection, "the credential rejection disclosed a key"
    finally:
        if tier == "local":
            credential_file.write_text(stored)

    restored = _run(argv, env=stack.env)
    restored_text = restored.stdout + restored.stderr
    assert restored.returncode == 0, restored_text
    for key in (invalid_key, INSTALL_KEY, POISONED_KEY):
        assert key not in restored_text, "the restored mint disclosed a key"
    restored_wrapper = json.loads(restored.stdout)
    assert set(restored_wrapper) == {"console_login_code"}, restored_wrapper
    restored_delivery = restored_wrapper["console_login_code"]
    assert set(restored_delivery) == {"code", "subject", "expires_at"}, restored_delivery
    assert restored_delivery["subject"] == SUBJECT
    assert restored_delivery["code"] not in codes
    restored_status, restored_session, _ = _exchange(stack.api_url, restored_delivery["code"])
    assert restored_status == 200, restored_session
    assert restored_session["subject"] == SUBJECT

    if tier == "local":
        assert not stack.kubectl_log.exists(), "local login performed cluster discovery"
    else:
        discovery = stack.kubectl_log.read_text()
        assert f"-n {NAMESPACE}" in discovery, discovery
        assert f"app.kubernetes.io/instance={RELEASE}" in discovery, discovery
        assert "apiKey" in discovery, discovery
        assert "port-forward" not in discovery, "explicit API URL created a tunnel"


@pytest.mark.parametrize("stack", ["curie-dev-key"], indirect=True)
def test_cluster_console_login_keeps_the_discovered_default_key_over_the_local_store(
    stack: Stack,
) -> None:
    # The private API accepts the discovered release key, while this machine's
    # private local project stores a different key. A 201 mint proves which
    # credential reached the real API without exposing request headers.
    discovered_key = stack.env["CURIE_TEST_DISCOVERED_KEY"]
    assert discovered_key == "curie-dev-key"
    result = _run(
        [
            str(stack.binary),
            "--json",
            "cluster",
            "console",
            "login",
            "--subject",
            SUBJECT,
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
            "--api-url",
            stack.api_url,
        ],
        env=stack.env,
    )
    rendered = result.stdout + result.stderr
    assert result.returncode == 0, rendered
    for key in (discovered_key, INSTALL_KEY, POISONED_KEY):
        assert key not in rendered, "cluster login disclosed a platform key"
    wrapper = json.loads(result.stdout)
    assert set(wrapper) == {"console_login_code"}, wrapper
    delivery = wrapper["console_login_code"]
    assert set(delivery) == {"code", "subject", "expires_at"}, delivery
    assert delivery["subject"] == SUBJECT
    status, session, _ = _exchange(stack.api_url, delivery["code"])
    assert status == 200, session
    assert session["subject"] == SUBJECT
    consumed_status, consumed, _ = _exchange(stack.api_url, delivery["code"])
    assert consumed_status == 401, consumed
