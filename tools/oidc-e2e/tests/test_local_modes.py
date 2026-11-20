"""The local OIDC fixture of #5801 decision 9, offline.

`local-env` is run for real and its file is rendered with
`docker compose ... config`, which reads no daemon. Every condition
`validate_isolated_override` in cli/src/local.rs checks is asserted one by one,
and the isolation variable list is read from that file, so a variable added
there fails these tests until the env file and the overlay carry it.
`local-session` runs against a fake opener: no network.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import oidc_e2e
import pytest

REPO_ROOT = Path(oidc_e2e.__file__).resolve().parents[2]
DRIVER = REPO_ROOT / "tools" / "oidc-e2e" / "oidc_e2e.py"
LOCAL_RS = REPO_ROOT / "cli" / "src" / "local.rs"
COMPOSE_DEV = REPO_ROOT / "compose.dev.yaml"
OVERLAY = REPO_ROOT / "tools" / "oidc-e2e" / "compose.local-oidc.yaml"
PTO = oidc_e2e._two_owner_support()
#: `!override` (2.24.4) and inline `configs.content` (2.23.1).
MIN_COMPOSE = (2, 24, 4)
#: The services `validate_isolated_override` checks for a registry image.
VALIDATED_IMAGES = (
    "curie-api",
    "curie-worker",
    "curie-dispatcher",
    "curie-channel-gateway",
    "curie-runner",
    "curie-ui",
)


def isolation_env() -> list[str]:
    """`ISOLATION_ENV` as cli/src/local.rs declares it."""

    match = re.search(r"const ISOLATION_ENV: &\[&str\] = &\[(.*?)\];", LOCAL_RS.read_text(), re.S)
    assert match, "ISOLATION_ENV not found in cli/src/local.rs"
    names = re.findall(r'"([A-Z0-9_]+)"', match.group(1))
    assert len(names) >= 10
    return names


ISOLATION_ENV = isolation_env()


def compose_version() -> tuple[int, int, int] | None:
    if shutil.which("docker") is None:
        return None
    out = subprocess.run(
        ["docker", "compose", "version", "--short"], capture_output=True, text=True, check=False
    )
    match = re.match(r"v?(\d+)\.(\d+)\.(\d+)", out.stdout.strip())
    if out.returncode != 0 or not match:
        return None
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


_VERSION = compose_version()
needs_compose = pytest.mark.skipif(
    _VERSION is None or _VERSION < MIN_COMPOSE,
    reason=(
        "the docker CLI with Compose 2.24.4 or later is absent; the overlay's !override "
        "tag and inline configs content need it"
    ),
)


def run_local_env(out: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(DRIVER), "local-env", "--out", str(out), *extra],
        capture_output=True,
        text=True,
        check=False,
        cwd=REPO_ROOT,
    )


@pytest.fixture(scope="module")
def env_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    path = tmp_path_factory.mktemp("local-oidc") / "local-oidc.env"
    proc = run_local_env(path)
    assert proc.returncode == 0, proc.stderr
    return path, proc.stdout


@pytest.fixture(scope="module")
def env_file(env_run: tuple[Path, str]) -> Path:
    return env_run[0]


@pytest.fixture(scope="module")
def values(env_file: Path) -> dict[str, str]:
    return oidc_e2e.read_env_file(env_file)


def port_of(url: str) -> int | None:
    return urllib.parse.urlsplit(url).port


def origin_of(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def dex_config(values: Mapping[str, str]) -> dict[str, Any]:
    config: dict[str, Any] = json.loads(values["CURIE_LOCAL_OIDC_DEX_CONFIG"])
    return config


# --- local-env ------------------------------------------------------------------


def test_local_env_writes_a_private_file_with_every_isolation_variable(
    env_run: tuple[Path, str], values: dict[str, str]
) -> None:
    path, stdout = env_run
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    project = values["COMPOSE_PROJECT_NAME"]
    assert project == "curie-local-oidc"
    missing = [name for name in ISOLATION_ENV if not values.get(name)]
    assert not missing, f"local-env does not write {missing}"
    assert values["CURIE_DOCKER_NETWORK"] == f"{project}_runner"
    assert values["CURIE_LOCAL_IMAGE_TAG"] == f"dev-{project}"
    for host in ("VALKEY_HOST", "CURIE_LOCAL_POSTGRES_HOST"):
        assert values[host] == "127.0.0.1"
    for url in ("CURIE_API_URL", "S3_ENDPOINT_URL"):
        assert urllib.parse.urlsplit(values[url]).hostname == "127.0.0.1"
    assert port_of(values["CURIE_API_URL"]) == int(values["CURIE_LOCAL_OIDC_API_PORT"])
    assert port_of(values["S3_ENDPOINT_URL"]) == int(values["CURIE_LOCAL_OIDC_S3_PORT"])
    ports = [
        int(values[name])
        for name in (
            *oidc_e2e.LOCAL_PORT_VARIABLES,
            "CURIE_LOCAL_STUB_PORT",
            "CURIE_LOCAL_GATEWAY_PORT",
        )
    ]
    assert len(set(ports)) == len(ports)
    assert all(0 < port < 49152 for port in ports)
    assert values["COMPOSE_FILE"].split(":") == [str(COMPOSE_DEV), str(OVERLAY)]
    assert values["CURIE_LOCAL_OIDC_CLIENT_SECRET"] not in stdout
    assert json.loads(stdout)["project"] == project


def test_local_env_dex_config_has_one_user_and_the_api_callback(values: dict[str, str]) -> None:
    config = dex_config(values)
    api = values["CURIE_API_URL"]
    dex_port = values["CURIE_LOCAL_OIDC_DEX_PORT"]
    assert (
        config["issuer"] == values["CURIE_LOCAL_OIDC_ISSUER"] == f"http://localhost:{dex_port}/dex"
    )
    assert config["web"] == {"http": f"0.0.0.0:{dex_port}"}
    users = config["staticPasswords"]
    assert [user["email"] for user in users] == ["approver@example.com"]
    assert base64.b64decode(users[0]["hash"]).decode() == PTO.DEX_PASSWORD_HASH
    (client,) = config["staticClients"]
    assert client["id"] == values["CURIE_LOCAL_OIDC_AUDIENCE"] == PTO.OIDC_CLIENT_ID
    assert client["secret"] == values["CURIE_LOCAL_OIDC_CLIENT_SECRET"]
    (redirect,) = client["redirectURIs"]
    assert redirect == values["CURIE_LOCAL_OIDC_REDIRECT_URI"]
    assert origin_of(redirect) == origin_of(api)
    assert urllib.parse.urlsplit(redirect).path == "/console/oidc/callback"
    assert values["CURIE_LOCAL_OIDC_JWKS_URL"] == config["issuer"] + "/keys"
    assert "$" not in values["CURIE_LOCAL_OIDC_DEX_CONFIG"]


def test_local_env_secret_is_per_run(tmp_path: Path, values: dict[str, str]) -> None:
    proc = run_local_env(tmp_path / "again.env", "--project", "curie-local-oidc-b")
    assert proc.returncode == 0, proc.stderr
    again = oidc_e2e.read_env_file(tmp_path / "again.env")
    assert again["CURIE_LOCAL_OIDC_CLIENT_SECRET"] != values["CURIE_LOCAL_OIDC_CLIENT_SECRET"]
    assert again["COMPOSE_PROJECT_NAME"] == "curie-local-oidc-b"
    assert again["CURIE_DOCKER_NETWORK"] == "curie-local-oidc-b_runner"
    assert again["CURIE_LOCAL_IMAGE_TAG"] == "dev-curie-local-oidc-b"


DEV_TOKENS = ("curie-dev-gateway-api", "curie-dev-gateway-worker")
TOKEN_VARIABLES = ("CURIE_CHANNEL_GATEWAY_API_TOKEN", "CURIE_CHANNEL_GATEWAY_WORKER_TOKEN")


def test_local_env_gateway_tokens_are_fresh_and_distinct(
    tmp_path: Path, values: dict[str, str]
) -> None:
    api, worker = (values[name] for name in TOKEN_VARIABLES)
    assert api != worker
    assert not {api, worker} & set(DEV_TOKENS)
    assert len(api) >= 32 and len(worker) >= 32
    proc = run_local_env(tmp_path / "again.env", "--project", "curie-local-oidc-c")
    assert proc.returncode == 0, proc.stderr
    again = oidc_e2e.read_env_file(tmp_path / "again.env")
    assert again[TOKEN_VARIABLES[0]] != api
    assert again[TOKEN_VARIABLES[1]] != worker


def _held_port(host: str) -> tuple[socket.socket, int]:
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.bind((host, 0))
    holder.listen()
    return holder, holder.getsockname()[1]


@pytest.mark.parametrize("host", ["127.0.0.1", "0.0.0.0"], ids=["loopback", "wildcard"])
def test_a_port_held_by_a_listener_reads_as_used(host: str) -> None:
    """A Docker published port listens on every interface, so the loopback probe must see it."""

    holder, port = _held_port(host)
    try:
        assert not oidc_e2e._port_unused(port)
    finally:
        holder.close()
    assert oidc_e2e._port_unused(port)


def test_local_env_refuses_a_bad_project(tmp_path: Path) -> None:
    proc = run_local_env(tmp_path / "bad.env", "--project", "Bad Name")
    assert proc.returncode == 2
    assert not (tmp_path / "bad.env").exists()


def test_sourced_env_file_matches_every_value(env_file: Path, values: dict[str, str]) -> None:
    dump = "import json, os; print(json.dumps(dict(os.environ)))"
    proc = subprocess.run(
        ["bash", "-c", 'set -a; . "$1"; set +a; exec "$2" -c "$3"', "_", str(env_file)]
        + [sys.executable, dump],
        capture_output=True,
        text=True,
        check=True,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
    )
    sourced = json.loads(proc.stdout)
    assert {key: sourced.get(key) for key in values} == values
    assert "\n" in sourced["CURIE_LOCAL_OIDC_DEX_CONFIG"]


# --- the merged compose config ------------------------------------------------------


# The render needs the docker CLI and nothing else from the caller. compose.dev.yaml forwards
# provider credentials from the caller's environment into the worker, so none may reach it.
RENDER_ENV = ("PATH", "HOME", "DOCKER_CONFIG", "XDG_CONFIG_HOME")


def require(condition: bool, message: str) -> None:
    """Fail with `message` alone: a bare `assert` would also print the merged config, and the
    service environments in it carry whatever the caller's shell forwards."""

    if not condition:
        raise AssertionError(message)


def render(env_path: Path, profiles: Sequence[str]) -> dict[str, Any]:
    """`docker compose -f compose.dev.yaml -f overlay config --format json`, env file sourced."""

    env = {key: os.environ[key] for key in RENDER_ENV if key in os.environ}
    argv = ["docker", "compose", "-f", str(COMPOSE_DEV), "-f", str(OVERLAY)]
    for profile in profiles:
        argv += ["--profile", profile]
    argv += ["config", "--format", "json"]
    proc = subprocess.run(
        ["bash", "-c", 'set -a; . "$1"; set +a; shift; exec "$@"', "_", str(env_path), *argv],
        capture_output=True,
        text=True,
        check=False,
        cwd=REPO_ROOT,
        env=env,
    )
    if proc.returncode != 0:
        raise AssertionError(f"compose config failed: {proc.stderr.strip()}")
    config: dict[str, Any] = json.loads(proc.stdout)
    return config


def environment(config: Mapping[str, Any], service: str) -> dict[str, str]:
    raw = config["services"][service].get("environment") or {}
    if isinstance(raw, list):
        return dict(item.split("=", 1) for item in raw)
    return {key: "" if value is None else str(value) for key, value in raw.items()}


def assert_isolated(config: Mapping[str, Any], values: Mapping[str, str]) -> None:
    """Each condition of `validate_isolated_override`, plus AC13's port and Dex checks.

    Every check goes through `require`, so a failure names the condition and never prints a
    service environment."""

    services = config["services"]
    require(config["name"] == values["COMPOSE_PROJECT_NAME"], "the project name is not rebound")
    require("curie-worker" in services, "the worker is turned off")
    gateway_port = int(values["CURIE_LOCAL_GATEWAY_PORT"])

    worker = environment(config, "curie-worker")
    postgres = f"@{values['CURIE_LOCAL_POSTGRES_HOST']}:{values['CURIE_LOCAL_POSTGRES_PORT']}/"
    require(postgres in worker.get("DATABASE_URL", ""), "worker DATABASE_URL is not rebound")
    for key in (
        "VALKEY_HOST",
        "VALKEY_PORT",
        "S3_ENDPOINT_URL",
        "CURIE_API_URL",
        "CURIE_DOCKER_NETWORK",
    ):
        require(worker.get(key) == values[key], f"worker {key} is not rebound")
    require(
        port_of(worker.get("CURIE_CHANNEL_GATEWAY_URL", "")) == gateway_port,
        "worker CURIE_CHANNEL_GATEWAY_URL is not this project's gateway",
    )

    api = environment(config, "curie-api")
    require(
        port_of(api.get("CURIE_CHANNEL_GATEWAY_URL", "")) == gateway_port,
        "curie-api CURIE_CHANNEL_GATEWAY_URL is not this project's gateway",
    )

    gateway = environment(config, "curie-channel-gateway")
    require(
        gateway.get("CURIE_CHANNEL_GATEWAY_PORT") == str(gateway_port),
        "the gateway does not listen on CURIE_LOCAL_GATEWAY_PORT",
    )
    stub = int(values["CURIE_LOCAL_STUB_PORT"])
    slack = gateway.get("SLACK_API_BASE_URL", "")
    require(
        port_of(slack) == stub and f"localhost:{stub}" in slack,
        "the gateway's SLACK_API_BASE_URL is not CURIE_LOCAL_STUB_PORT",
    )

    require(
        config["networks"]["curie_runner"]["name"] == values["CURIE_DOCKER_NETWORK"],
        "networks.curie_runner.name is not this project's runner network",
    )

    tag = values["CURIE_LOCAL_IMAGE_TAG"]
    for name, service in services.items():
        image = service.get("image") or ""
        if "ghcr.io/curie-eng/" in image:
            require(image.endswith(f":{tag}"), f"{name} pins an image without {tag}")
    require(
        worker.get("CURIE_RUNNER_IMAGE", "").endswith(f":{tag}"),
        "worker CURIE_RUNNER_IMAGE does not carry CURIE_LOCAL_IMAGE_TAG",
    )
    build_args = (services["curie-worker"].get("build") or {}).get("args", {})
    require(build_args.get("BASE_TAG") == tag, "the worker build BASE_TAG is not the image tag")

    allowed = {int(values[name]) for name in oidc_e2e.LOCAL_PORT_VARIABLES}
    published: set[int] = set()
    for name, service in services.items():
        for port in service.get("ports") or []:
            require(port.get("host_ip") == "127.0.0.1", f"{name} publishes beyond loopback")
            require(int(port["published"]) in allowed, f"{name} publishes a port not in the file")
            published.add(int(port["published"]))
    require(published == allowed, "a host port of the env file is not published")
    api_ports = {
        (int(port["published"]), int(port["target"])) for port in services["curie-api"]["ports"]
    }
    dex_port = int(values["CURIE_LOCAL_OIDC_DEX_PORT"])
    require(
        api_ports == {(int(values["CURIE_LOCAL_OIDC_API_PORT"]), 8000), (dex_port, dex_port)},
        "curie-api does not publish exactly its API and Dex ports",
    )

    require(
        services["dex"].get("network_mode") == "service:curie-api",
        "dex does not share the API's network",
    )
    require(services["dex"].get("image") == PTO.DEX_IMAGE, "dex runs another image")
    issuer = dex_config(values)["issuer"]
    redirect = values["CURIE_API_URL"] + "/console/oidc/callback"
    for key, expected in (
        ("CURIE_OIDC_ISSUER", issuer),
        ("CURIE_OIDC_REDIRECT_URI", redirect),
        ("CURIE_OIDC_CLIENT_SECRET", values["CURIE_LOCAL_OIDC_CLIENT_SECRET"]),
        ("CURIE_OIDC_JWKS_URL", issuer + "/keys"),
        ("CURIE_OIDC_AUDIENCE", PTO.OIDC_CLIENT_ID),
        ("CURIE_OIDC_SCOPES", PTO.OIDC_SCOPES),
        ("CURIE_OIDC_ADMIT_ALL_AUTHENTICATED", "true"),
    ):
        require(api.get(key) == expected, f"curie-api {key} is not the local Dex's")
    content = config["configs"]["local_oidc_dex"]["content"]
    require(json.loads(content) == dex_config(values), "Dex reads a different config")


@needs_compose
@pytest.mark.parametrize("profiles", [("core",), ("full", "slack")], ids=["core", "full-slack"])
def test_merged_config_is_isolated(
    env_file: Path, values: dict[str, str], profiles: Sequence[str]
) -> None:
    assert_isolated(render(env_file, profiles), values)


@needs_compose
@pytest.mark.parametrize("name", ISOLATION_ENV)
def test_a_missing_isolation_variable_fails_the_render(
    tmp_path: Path, values: dict[str, str], name: str
) -> None:
    broken = tmp_path / "broken.env"
    broken.write_text(oidc_e2e.render_env_file({k: v for k, v in values.items() if k != name}))
    with pytest.raises(AssertionError) as caught:
        assert_isolated(render(broken, ("core",)), values)
    assert name in str(caught.value)


@needs_compose
def test_gateway_tokens_reach_the_three_services(env_file: Path, values: dict[str, str]) -> None:
    config = render(env_file, ("core",))
    api_token, worker_token = (values[name] for name in TOKEN_VARIABLES)
    gateway = environment(config, "curie-channel-gateway")
    assert gateway["CURIE_CHANNEL_GATEWAY_API_TOKEN"] == api_token
    assert gateway["CURIE_CHANNEL_GATEWAY_WORKER_TOKEN"] == worker_token
    assert environment(config, "curie-api")["CURIE_CHANNEL_GATEWAY_TOKEN"] == api_token
    assert environment(config, "curie-worker")["CURIE_CHANNEL_GATEWAY_TOKEN"] == worker_token
    for service in ("curie-channel-gateway", "curie-api", "curie-worker"):
        leaked = set(DEV_TOKENS) & set(environment(config, service).values())
        require(not leaked, f"{service} carries a public development token")


@needs_compose
@pytest.mark.parametrize("name", TOKEN_VARIABLES)
def test_a_missing_gateway_token_fails_the_render(
    tmp_path: Path, values: dict[str, str], name: str
) -> None:
    broken = tmp_path / "broken.env"
    broken.write_text(oidc_e2e.render_env_file({k: v for k, v in values.items() if k != name}))
    with pytest.raises(AssertionError) as caught:
        render(broken, ("core",))
    assert name in str(caught.value)


PROVIDER_CREDENTIAL = "sk-provider-credential-marker-7d3e91"
FORWARDED_CREDENTIALS = ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "CURIE_CREDENTIALS")


@needs_compose
def test_a_caller_credential_never_reaches_the_render_or_a_failure(
    env_file: Path, values: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """compose.dev.yaml forwards these names from the caller's shell into the worker."""

    for name in FORWARDED_CREDENTIALS:
        monkeypatch.setenv(name, PROVIDER_CREDENTIAL)
    config = render(env_file, ("core",))
    assert PROVIDER_CREDENTIAL not in json.dumps(config)
    assert_isolated(config, values)
    # Every way assert_isolated can fail: the message names the condition, not the config.
    for label, mutate in MUTATIONS.items():
        broken = copy.deepcopy(config)
        mutate(broken)
        with pytest.raises(AssertionError) as caught:
            assert_isolated(broken, values)
        assert PROVIDER_CREDENTIAL not in str(caught.value), label
    # A failure on a config that carries a credential still does not print it. The config is
    # small so that even an introspected `in` check would show the whole services mapping.
    leaky = {
        "name": values["COMPOSE_PROJECT_NAME"],
        "services": {"curie-api": {"environment": {"ANTHROPIC_API_KEY": PROVIDER_CREDENTIAL}}},
    }
    with pytest.raises(AssertionError) as caught:
        assert_isolated(leaky, values)
    assert "worker is turned off" in str(caught.value)
    assert PROVIDER_CREDENTIAL not in str(caught.value)


def _set_env(service: str, key: str, value: str) -> Callable[[dict[str, Any]], None]:
    def mutate(config: dict[str, Any]) -> None:
        env = config["services"][service]["environment"]
        env[key] = value

    return mutate


def _set(path: Sequence[str], value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(config: dict[str, Any]) -> None:
        node = config
        for part in path[:-1]:
            node = node[part]
        node[path[-1]] = value

    return mutate


def _shared_port(config: dict[str, Any]) -> None:
    config["services"]["valkey"]["ports"][0]["published"] = "26379"


MUTATIONS: dict[str, Callable[[dict[str, Any]], None]] = {
    "worker-database": _set_env(
        "curie-worker", "DATABASE_URL", "postgresql+asyncpg://p:p@localhost:25432/postgres"
    ),
    "worker-valkey-host": _set_env("curie-worker", "VALKEY_HOST", "localhost"),
    "worker-valkey-port": _set_env("curie-worker", "VALKEY_PORT", "26379"),
    "worker-s3": _set_env("curie-worker", "S3_ENDPOINT_URL", "http://localhost:29000"),
    "worker-api": _set_env("curie-worker", "CURIE_API_URL", "http://localhost:28000"),
    "worker-gateway": _set_env(
        "curie-worker", "CURIE_CHANNEL_GATEWAY_URL", "http://localhost:8081"
    ),
    "worker-network": _set_env("curie-worker", "CURIE_DOCKER_NETWORK", "curie_runner"),
    "api-gateway": _set_env(
        "curie-api", "CURIE_CHANNEL_GATEWAY_URL", "http://host.docker.internal:8081"
    ),
    "gateway-listener": _set_env("curie-channel-gateway", "CURIE_CHANNEL_GATEWAY_PORT", "8081"),
    "gateway-stub": _set_env(
        "curie-channel-gateway", "SLACK_API_BASE_URL", "http://localhost:8155/api/"
    ),
    "runner-network": _set(("networks", "curie_runner", "name"), "curie_runner"),
    "registry-image": _set(
        ("services", "curie-api", "image"), "ghcr.io/curie-eng/curie-api:latest"
    ),
    "worker-off": lambda config: config["services"].pop("curie-worker"),
    "shared-port": _shared_port,
    "dex-network": _set(("services", "dex", "network_mode"), "host"),
    "issuer": _set_env("curie-api", "CURIE_OIDC_ISSUER", "http://localhost:5556/dex"),
}


@needs_compose
def test_each_condition_is_checked(env_file: Path, values: dict[str, str]) -> None:
    config = render(env_file, ("core",))
    assert_isolated(config, values)
    for label, mutate in MUTATIONS.items():
        broken = copy.deepcopy(config)
        mutate(broken)
        with pytest.raises(AssertionError):
            assert_isolated(broken, values)
            pytest.fail(f"mutation {label} passed the isolation check")


# --- local-session ------------------------------------------------------------------

COOKIE = "session-cookie-marker-4f1c2b9e7a"
STATE = "state-cookie-marker-90d1"
PRINCIPAL_ID = "6f9c2f0e-1d7b-4c55-9b1e-0b4ad4c1f001"
TENANT_ID = "00000000-0000-0000-0000-000000000001"
LOGIN_FORM = (
    '<form method="post" action="/dex/auth/local/login?back=&amp;state=r1">'
    '<input name="login"><input type="password" name="password"></form>'
)
Reply = tuple[int, Sequence[tuple[str, str]], bytes]


class FakeStack:
    """The API's login, callback and session routes and Dex's password form, scripted."""

    def __init__(
        self, values: Mapping[str, str], *, kind: str = "principal", login_ok: bool = True
    ) -> None:
        self.api = values["CURIE_API_URL"]
        self.issuer = values["CURIE_LOCAL_OIDC_ISSUER"]
        self.dex = f"http://127.0.0.1:{values['CURIE_LOCAL_OIDC_DEX_PORT']}"
        self.redirect = values["CURIE_LOCAL_OIDC_REDIRECT_URI"]
        self.kind = kind
        self.login_ok = login_ok
        self.requests: list[tuple[str, str]] = []

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> Reply:
        self.requests.append((method, url))
        origin, path = origin_of(url), urllib.parse.urlsplit(url).path
        if origin == self.api and path == "/console/oidc/login":
            query = urllib.parse.urlencode({"redirect_uri": self.redirect, "state": "s1"})
            return (
                302,
                [
                    ("Location", f"{self.issuer}/auth?{query}"),
                    ("Set-Cookie", f"{PTO.STATE_COOKIE}={STATE}; HttpOnly; Path=/; Secure"),
                ],
                b"",
            )
        if origin == self.dex and path == "/dex/auth" and method == "GET":
            return 200, [], LOGIN_FORM.encode()
        if origin == self.dex and path == "/dex/auth/local/login" and method == "POST":
            form = urllib.parse.parse_qs((body or b"").decode())
            assert form == {"login": ["approver@example.com"], "password": [PTO.DEX_PASSWORD]}
            return 303, [("Location", f"{self.redirect}?code=c1&state=s1")], b""
        if origin == self.api and path == "/console/oidc/callback":
            assert headers == {"Cookie": f"{PTO.STATE_COOKIE}={STATE}"}
            if not self.login_ok:
                return 400, [], b'{"detail": "OIDC login failed"}'
            cookie = f"{PTO.SESSION_COOKIE}={COOKIE}; HttpOnly; Path=/; Secure"
            return 303, [("Location", "/"), ("Set-Cookie", cookie)], b""
        if origin == self.api and path in ("/console/session", "/console/principal"):
            assert headers == {"Cookie": f"{PTO.SESSION_COOKIE}={COOKIE}"}
            principal = self.kind == "principal"
            if path == "/console/session":
                answer = {
                    "subject": f"principal:{PRINCIPAL_ID}" if principal else "U0OPERATOR",
                    "expires_at": "2030-01-01T00:00:00Z",
                    "kind": self.kind,
                    "principal": {"id": PRINCIPAL_ID, "display_name": None, "email": None}
                    if principal
                    else None,
                }
                return 200, [], json.dumps(answer).encode()
            if not principal:
                return 401, [], b'{"detail": "a principal session is required"}'
            answer = {
                "id": PRINCIPAL_ID,
                "tenant_id": TENANT_ID,
                "idp_subject": "subject-marker",
                "display_name": None,
                "email": "approver@example.com",
                "status": "active",
            }
            return 200, [], json.dumps(answer).encode()
        raise AssertionError(f"unexpected {method} {url}")


def assert_no_secret(text: str, values: Mapping[str, str]) -> None:
    for secret in (COOKIE, values["CURIE_LOCAL_OIDC_CLIENT_SECRET"], PTO.DEX_PASSWORD):
        assert secret not in text


def test_local_session_writes_only_the_cookie_and_prints_three_fields(
    tmp_path: Path,
    env_file: Path,
    values: dict[str, str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    out = tmp_path / "session"
    fake = FakeStack(values)
    code = oidc_e2e.local_session(env_file, "approver@example.com", out, opener=fake)
    captured = capsys.readouterr()
    assert code == 0, captured.err
    assert out.read_text() == COOKIE
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    printed = json.loads(captured.out)
    assert printed == {"kind": "principal", "principal_id": PRINCIPAL_ID, "tenant_id": TENANT_ID}
    assert_no_secret(captured.out + captured.err, values)
    assert ("GET", values["CURIE_API_URL"] + "/console/principal") in fake.requests


@pytest.mark.parametrize(
    "fake_args", [{"kind": "operator"}, {"login_ok": False}], ids=["operator", "failed-login"]
)
def test_local_session_refuses_without_a_principal_session(
    tmp_path: Path,
    env_file: Path,
    values: dict[str, str],
    capsys: pytest.CaptureFixture[str],
    fake_args: dict[str, Any],
) -> None:
    out = tmp_path / "session"
    fake = FakeStack(values, **fake_args)
    code = oidc_e2e.local_session(env_file, "approver@example.com", out, opener=fake)
    captured = capsys.readouterr()
    assert code != 0
    assert not out.exists()
    assert captured.out == ""
    assert captured.err.startswith("local-session: ")
    assert_no_secret(captured.out + captured.err, values)


def test_local_session_refuses_a_foreign_env_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = tmp_path / "other.env"
    env.write_text("CURIE_API_URL=http://127.0.0.1:1\n")

    def no_network(*_args: Any) -> Reply:
        raise AssertionError("no request expected")

    code = oidc_e2e.local_session(env, "approver@example.com", tmp_path / "s", opener=no_network)
    assert code == 2 and not (tmp_path / "s").exists()
    assert capsys.readouterr().out == ""


def test_loopback_opener_refuses_other_hosts() -> None:
    opener = oidc_e2e.LoopbackOpener()
    with pytest.raises(PTO.LoginFailed):
        opener("GET", "http://example.com/console/session", {}, None)


def test_no_mode_keeps_the_existing_driver(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as done:
        oidc_e2e.main(["--help"])
    assert done.value.code == 0
    usage = capsys.readouterr().out
    assert "--keep" in usage and "--evidence" in usage and "local-env" not in usage
