"""Exercise CLI ownership, concurrent stores, and cleanup using actual Docker."""

from __future__ import annotations

import ipaddress
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
RESULT_KEYS = {"profile", "project", "ports", "networks", "child_exit_code", "cleanup_verified"}
CHILD_STDOUT = '{"child_stdout":"isolated-test-env"}'
RESOURCE_COMMANDS = {
    "containers": ["ps", "-aq"],
    "networks": ["network", "ls", "-q"],
    "volumes": ["volume", "ls", "-q"],
}

CHILD = r"""
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import urllib.request
from urllib.parse import urlsplit

import asyncpg
import redis

token, mode = sys.argv[1:3]
assert sys.argv[3:] == ["--profile", "full", "--json", "argument with spaces"]
database = os.environ["TEST_ENV_CONTROL_DATABASE"]

def update(**values):
    with sqlite3.connect(database, timeout=30) as connection:
        columns = ", ".join(f"{name} = ?" for name in values)
        connection.execute(f"UPDATE runs SET {columns} WHERE token = ?", [*values.values(), token])

update(child_pid=os.getpid())

async def main():
    assert os.environ["DATABASE_URL"] == os.environ["TEST_DATABASE_URL"]
    assert os.environ["VALKEY_HOST"] == os.environ["TEST_VALKEY_HOST"]
    assert os.environ["VALKEY_PORT"] == os.environ["TEST_VALKEY_PORT"]
    dsn = os.environ["TEST_DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://", 1)
    endpoint = urlsplit(dsn)
    cache = redis.Redis(
        host=os.environ["TEST_VALKEY_HOST"], port=int(os.environ["TEST_VALKEY_PORT"]),
        password=os.environ["TEST_VALKEY_PW"], decode_responses=True,
        socket_connect_timeout=10, socket_timeout=10,
    )
    connection = await asyncpg.connect(dsn, timeout=15)
    descendant = 0
    try:
        await connection.execute("CREATE TABLE isolated_test_env_probe (value text NOT NULL)")
        await connection.execute("INSERT INTO isolated_test_env_probe VALUES ($1)", token)
        assert cache.set("isolated-test-env-probe", token)
        if mode == "full":
            url = os.environ["TEST_LANGFUSE_HOST"].rstrip("/") + "/api/public/health"
            with urllib.request.urlopen(url, timeout=20) as response:
                assert response.status == 200
            assert os.environ["LANGFUSE_HOST"] == os.environ["TEST_LANGFUSE_HOST"]
        if mode == "signal":
            descendant = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(600)"]
            ).pid
        selected = {
            "project": os.environ["COMPOSE_PROJECT_NAME"],
            "compose_file": os.environ["COMPOSE_FILE"],
            "postgres_host": endpoint.hostname,
            "postgres_port": endpoint.port,
            "valkey_host": os.environ["TEST_VALKEY_HOST"],
            "valkey_port": int(os.environ["TEST_VALKEY_PORT"]),
            "runner_network": os.environ["CURIE_DOCKER_NETWORK"],
        }
        print('{"child_stdout":"isolated-test-env"}', flush=True)
        print("child-stderr:" + token, file=sys.stderr, flush=True)
        while True:
            values = [row["value"] for row in await connection.fetch(
                "SELECT value FROM isolated_test_env_probe"
            )]
            cached = cache.get("isolated-test-env-probe")
            assert values == [token], values
            assert cached == token, cached
            with sqlite3.connect(database, timeout=30) as control:
                release, ticks = control.execute(
                    "SELECT release, ticks FROM runs WHERE token = ?", (token,)
                ).fetchone()
            update(ready=1, environment=json.dumps(selected), postgres_value=values[0],
                   valkey_value=cached, descendant=descendant, ticks=ticks + 1)
            if release:
                return 42 if mode == "exit42" else 0
            await asyncio.sleep(0.1)
    finally:
        cache.close()
        await connection.close()

raise SystemExit(asyncio.run(main()))
"""

DOCKER_WRAPPER = r"""
import os
import sqlite3
import subprocess
import sys

args = sys.argv[1:]
database = os.environ["TEST_ENV_CONTROL_DATABASE"]
real = os.environ["TEST_ENV_REAL_DOCKER"]
if "compose" not in args:
    os.execv(real, [real, *args])
compose_index = args.index("compose")
project = os.environ.get("COMPOSE_PROJECT_NAME")
for index, value in enumerate(args):
    if value in ("-p", "--project-name"):
        project = args[index + 1]
    elif value.startswith("--project-name="):
        project = value.split("=", 1)[1]
if not project:
    raise SystemExit("Compose invocation omitted its project identity")
with sqlite3.connect(database, timeout=30) as connection:
    connection.execute("INSERT OR IGNORE INTO projects VALUES (?)", (project,))
if os.environ.get("TEST_ENV_FAIL_STARTUP") != "1" or "up" not in args[compose_index + 1:]:
    os.execv(real, [real, *args])
prefix = args[:args.index("up", compose_index + 1)]
result = subprocess.run(
    [real, *prefix, "up", "-d", "--no-deps", "postgres"],
    stdout=sys.stderr, stderr=sys.stderr, check=False,
)
counts = []
for command in (["ps", "-aq"], ["network", "ls", "-q"], ["volume", "ls", "-q"]):
    inventory = subprocess.run(
        [real, *command, "--filter", "label=com.docker.compose.project=" + project],
        text=True, capture_output=True, check=False,
    )
    if inventory.returncode:
        raise SystemExit("Cannot inventory the actual partial startup")
    counts.append(len(inventory.stdout.split()))
with sqlite3.connect(database, timeout=30) as connection:
    connection.execute("INSERT INTO partial VALUES (?, ?, ?, ?, ?)",
                       (project, result.returncode, *counts))
print("caller-injected-compose-startup-failure", file=sys.stderr, flush=True)
raise SystemExit(71)
"""


@dataclass
class Invocation:
    token: str
    process: subprocess.Popen[str]
    stdout: Path
    stderr: Path


@dataclass
class Trial:
    binary: Path
    docker: str
    directory: Path
    database: Path
    environment: dict[str, str]
    sentinel: str
    initial: dict[str, set[str]]
    invocations: list[Invocation] = field(default_factory=list)

    def docker_run(self, *args: str, timeout: int = 60) -> str:
        result = subprocess.run(
            [self.docker, *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(f"Docker {args[0]} failed with exit {result.returncode}")
        return result.stdout.strip()

    def inventory(self, project: str | None = None) -> dict[str, set[str]]:
        label = ["--filter", f"label=com.docker.compose.project={project}"] if project else []
        return {
            kind: set(self.docker_run(*arguments, *label).split())
            for kind, arguments in RESOURCE_COMMANDS.items()
        }

    def row(self, invocation: Invocation) -> sqlite3.Row:
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(
                "SELECT * FROM runs WHERE token = ?", (invocation.token,)
            ).fetchone()

    def launch(self, *, profile: str | None = None, mode: str = "normal") -> Invocation:
        token = uuid.uuid4().hex
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute("INSERT INTO runs (token) VALUES (?)", (token,))
        stdout = self.directory / f"{token}.stdout"
        stderr = self.directory / f"{token}.stderr"
        arguments = [str(self.binary), "--json", "dev", "test-env", "run"]
        if profile is not None:
            arguments.extend(["--profile", profile])
        arguments.extend(
            [
                "--",
                sys.executable,
                "-c",
                CHILD,
                token,
                mode,
                "--profile",
                "full",
                "--json",
                "argument with spaces",
            ]
        )
        environment = dict(self.environment)
        if mode == "partial":
            environment["TEST_ENV_FAIL_STARTUP"] = "1"
        with stdout.open("w") as output, stderr.open("w") as errors:
            process = subprocess.Popen(
                arguments,
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=output,
                stderr=errors,
                start_new_session=True,
            )
        invocation = Invocation(token, process, stdout, stderr)
        self.invocations.append(invocation)
        return invocation

    def wait_ready(self, *invocations: Invocation) -> None:
        deadline = time.monotonic() + 660
        while time.monotonic() < deadline:
            if all(self.row(invocation)["ready"] for invocation in invocations):
                for invocation in invocations:
                    assert invocation.stdout.read_text() == "", "A result preceded child completion"
                return
            for invocation in invocations:
                assert invocation.process.poll() is None, (
                    f"CLI exited before its real-store child became ready: "
                    f"{invocation.stdout.read_text()}\n{invocation.stderr.read_text()}"
                )
            time.sleep(0.1)
        pytest.fail("CLI did not deliver usable isolated stores before the deadline")

    def release(self, *invocations: Invocation) -> None:
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.executemany(
                "UPDATE runs SET release = 1 WHERE token = ?",
                [(invocation.token,) for invocation in invocations],
            )

    def finish(
        self, invocation: Invocation, *, code: int, child_code: int | None
    ) -> dict[str, Any]:
        invocation.process.wait(timeout=180)
        assert invocation.process.returncode == code, invocation.stderr.read_text()
        result = json.loads(invocation.stdout.read_text())
        assert isinstance(result, dict) and RESULT_KEYS <= result.keys()
        assert result["child_exit_code"] == child_code
        assert result["cleanup_verified"] is True
        assert isinstance(result["project"], str) and result["project"] != self.sentinel
        assert isinstance(result["ports"], dict)
        assert isinstance(result["networks"], list)
        assert not any(self.inventory(result["project"]).values())
        self.sentinel_intact()
        return result

    def sentinel_intact(self) -> None:
        running = self.docker_run("inspect", "--format", "{{.State.Running}}", self.sentinel)
        if running != "true":
            raise RuntimeError("The unrelated sentinel container stopped")
        value = self.docker_run("exec", self.sentinel, "valkey-cli", "GET", "sentinel")
        if value != self.sentinel:
            raise RuntimeError("The unrelated sentinel's stored value changed")
        self.docker_run("network", "inspect", self.sentinel, "--format", "{{.Id}}")
        self.docker_run("volume", "inspect", self.sentinel, "--format", "{{.Name}}")


def _alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    status = Path(f"/proc/{pid}/stat")
    try:
        return not status.exists() or status.read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def _teardown(trial: Trial) -> None:
    errors = []

    def attempt(action: Callable[[], Any], default: Any = None) -> Any:
        try:
            return action()
        except (
            OSError,
            RuntimeError,
            ValueError,
            sqlite3.Error,
            subprocess.SubprocessError,
        ) as error:
            errors.append(f"Cleanup could not complete: {type(error).__name__}")
            return default

    def stop(invocation: Invocation) -> None:
        if invocation.process.poll() is None:
            with suppress(ProcessLookupError):
                invocation.process.send_signal(signal.SIGTERM)
            try:
                invocation.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                with suppress(ProcessLookupError):
                    os.killpg(invocation.process.pid, signal.SIGKILL)
                invocation.process.wait(timeout=15)
                errors.append("A CLI process required forced termination")
        row = trial.row(invocation)
        for field_name in ("child_pid", "descendant"):
            if _alive(row[field_name]):
                with suppress(ProcessLookupError):
                    os.kill(row[field_name], signal.SIGKILL)
                errors.append("A child process survived CLI cleanup")

    for invocation in trial.invocations:
        attempt(partial(stop, invocation))
    with sqlite3.connect(trial.database, timeout=30) as connection:
        projects = [row[0] for row in connection.execute("SELECT project FROM projects")]
        partial_rows = list(connection.execute("SELECT startup_code FROM partial"))
    if any(code for (code,) in partial_rows):
        errors.append("The real partial-startup prerequisite failed")

    def remove(kind: str, identity: str) -> None:
        arguments = {
            "containers": ["rm", "-f", "-v"],
            "networks": ["network", "rm"],
            "volumes": ["volume", "rm"],
        }[kind]
        trial.docker_run(*arguments, identity)

    for project in projects:
        inventory = attempt(partial(trial.inventory, project), {})
        for kind, identities in inventory.items():
            owned = identities - trial.initial[kind]
            if owned:
                errors.append(f"The CLI left {len(owned)} owned {kind} after cleanup")
            for identity in owned:
                attempt(partial(remove, kind, identity))
        after = attempt(partial(trial.inventory, project), {})
        if any(identities - trial.initial[kind] for kind, identities in after.items()):
            errors.append("Rescue cleanup left owned Docker resources")
    sentinel = attempt(lambda: trial.inventory(trial.sentinel), {})
    if sentinel and all(sentinel.values()):
        attempt(trial.sentinel_intact)
    elif trial.invocations:
        errors.append("The unrelated sentinel lost Docker resources")
    for kind, identities in sentinel.items():
        for identity in identities:
            attempt(partial(remove, kind, identity))
    if any(attempt(lambda: trial.inventory(trial.sentinel), {}).values()):
        errors.append("Sentinel teardown left Docker resources")
    if errors:
        raise RuntimeError("; ".join(errors))


@pytest.fixture
def trial(tmp_path: Path, request: pytest.FixtureRequest) -> Trial:
    if os.name != "posix":
        raise RuntimeError("The lifecycle tests require POSIX process signals")
    binary = Path(os.environ.get("CURIE_BIN", ROOT / "cli/target/debug/curie")).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(
            "A built candidate CLI is required through CURIE_BIN or cli/target/debug/curie"
        )
    docker = shutil.which("docker")
    if docker is None:
        raise RuntimeError("The actual Docker CLI is required")
    packages = subprocess.run(
        [sys.executable, "-c", "import asyncpg; import redis"],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if packages.returncode:
        raise RuntimeError("The child Python requires the asyncpg and redis packages")
    database = tmp_path / "control.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript("""
            PRAGMA journal_mode = WAL;
            CREATE TABLE runs (
                token TEXT PRIMARY KEY, ready INTEGER DEFAULT 0, release INTEGER DEFAULT 0,
                ticks INTEGER DEFAULT 0, environment TEXT, postgres_value TEXT, valkey_value TEXT,
                child_pid INTEGER DEFAULT 0, descendant INTEGER DEFAULT 0
            );
            CREATE TABLE projects (project TEXT PRIMARY KEY);
            CREATE TABLE partial (
                project TEXT, startup_code INTEGER, containers INTEGER, networks INTEGER,
                volumes INTEGER
            );
        """)
    sentinel = "curie-test-env-sentinel-" + uuid.uuid4().hex[:12]
    result = Trial(binary, docker, tmp_path, database, {}, sentinel, {})
    result.docker_run("info", "--format", "{{.ServerVersion}}")
    version = result.docker_run("compose", "version", "--short").lstrip("v").split("-")[0]
    if tuple(int(value) for value in version.split(".")[:3]) < (2, 24, 4):
        raise RuntimeError("Docker Compose 2.24.4 or newer is required for port replacement")
    result.initial = result.inventory()
    request.addfinalizer(lambda: _teardown(result))
    wrapper_directory = tmp_path / "bin"
    wrapper_directory.mkdir()
    wrapper = wrapper_directory / "docker"
    wrapper.write_text(f"#!{sys.executable}\n" + DOCKER_WRAPPER)
    wrapper.chmod(0o755)
    result.environment = {
        **os.environ,
        "PATH": str(wrapper_directory) + os.pathsep + os.environ.get("PATH", ""),
        "TEST_ENV_REAL_DOCKER": docker,
        "TEST_ENV_CONTROL_DATABASE": str(database),
    }
    for name in ("COMPOSE_FILE", "COMPOSE_PROJECT_NAME", "TEST_ENV_FAIL_STARTUP"):
        result.environment.pop(name, None)
    image = yaml.safe_load((ROOT / "compose.dev.yaml").read_text())["services"]["valkey"]["image"]
    result.docker_run("pull", image, timeout=300)
    network_ids = result.docker_run("network", "ls", "-q").split()
    networks = json.loads(result.docker_run("network", "inspect", *network_ids))
    occupied = [
        ipaddress.ip_network(config["Subnet"], strict=False)
        for network in networks
        for config in (network.get("IPAM", {}).get("Config") or [])
        if config.get("Subnet")
    ]
    subnet = next(
        candidate
        for candidate in ipaddress.ip_network("198.18.0.0/15").subnets(new_prefix=28)
        if not any(candidate.overlaps(existing) for existing in occupied)
    )
    label = f"com.docker.compose.project={sentinel}"
    result.docker_run(
        "network", "create", "--label", label, "--internal", "--subnet", str(subnet), sentinel
    )
    result.docker_run("volume", "create", "--label", label, sentinel)
    result.docker_run(
        "run",
        "-d",
        "--name",
        sentinel,
        "--label",
        label,
        "--network",
        sentinel,
        "--volume",
        f"{sentinel}:/data",
        image,
        "valkey-server",
        "--save",
        "",
        "--appendonly",
        "no",
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            result.docker_run("exec", sentinel, "valkey-cli", "SET", "sentinel", sentinel)
            result.sentinel_intact()
            return result
        except RuntimeError:
            time.sleep(0.1)
    raise RuntimeError("The real unrelated sentinel did not become ready")


def _environment(trial: Trial, invocation: Invocation) -> dict[str, Any]:
    return json.loads(trial.row(invocation)["environment"])


def _ports(trial: Trial, environment: dict[str, Any]) -> set[int]:
    containers = trial.inventory(environment["project"])["containers"]
    assert containers
    actual = json.loads(trial.docker_run("inspect", *sorted(containers)))
    ports = set()
    for container in actual:
        for bindings in (container["NetworkSettings"].get("Ports") or {}).values():
            for binding in bindings or []:
                assert binding["HostIp"] == "127.0.0.1"
                ports.add(int(binding["HostPort"]))
    assert {environment["postgres_port"], environment["valkey_port"]} <= ports
    return ports


def _networks(trial: Trial, environment: dict[str, Any]) -> set[str]:
    identities = trial.inventory(environment["project"])["networks"]
    assert identities
    actual = json.loads(trial.docker_run("network", "inspect", *sorted(identities)))
    for network in actual:
        assert network["Labels"]["com.docker.compose.project"] == environment["project"]
    names = {network["Name"] for network in actual}
    assert environment["runner_network"] in names
    return names


def test_concurrent_default_core_environments_have_disjoint_ports_networks_and_data(trial: Trial):
    first, second = trial.launch(), trial.launch()
    trial.wait_ready(first, second)
    environments = [_environment(trial, invocation) for invocation in (first, second)]
    left, right = environments
    assert left["project"] != right["project"]
    assert left["runner_network"] != right["runner_network"]
    actual_ports = [_ports(trial, environment) for environment in environments]
    actual_networks = [_networks(trial, environment) for environment in environments]
    assert actual_ports[0].isdisjoint(actual_ports[1])
    assert actual_networks[0].isdisjoint(actual_networks[1])
    owned = [trial.inventory(environment["project"]) for environment in environments]
    assert owned[0]["networks"] and owned[1]["networks"]
    assert owned[0]["networks"].isdisjoint(owned[1]["networks"])
    assert owned[0]["volumes"] and owned[1]["volumes"]
    assert owned[0]["volumes"].isdisjoint(owned[1]["volumes"])
    for invocation, environment in zip((first, second), environments, strict=True):
        assert environment["postgres_host"] == environment["valkey_host"] == "127.0.0.1"
        files = environment["compose_file"].split(os.pathsep)
        assert Path(files[0]).resolve() == ROOT / "compose.dev.yaml"
        assert len(files) >= 2 and all(Path(value).is_file() for value in files)
        row = trial.row(invocation)
        assert row["postgres_value"] == row["valkey_value"] == invocation.token
    trial.release(first)
    first_report = trial.finish(first, code=0, child_code=0)
    ticks = trial.row(second)["ticks"]
    deadline = time.monotonic() + 15
    while trial.row(second)["ticks"] <= ticks and time.monotonic() < deadline:
        time.sleep(0.1)
    assert trial.row(second)["ticks"] > ticks
    assert second.process.poll() is None
    trial.release(second)
    second_report = trial.finish(second, code=0, child_code=0)
    for invocation, report, environment, ports, networks in zip(
        (first, second),
        (first_report, second_report),
        environments,
        actual_ports,
        actual_networks,
        strict=True,
    ):
        assert report["profile"] == "core"
        assert report["project"] == environment["project"]
        assert report["ports"]["postgres"] == environment["postgres_port"]
        assert report["ports"]["valkey"] == environment["valkey_port"]
        assert all(type(port) is int and 0 < port < 65536 for port in report["ports"].values())
        assert set(report["ports"].values()) == ports
        assert set(report["networks"]) == networks
        assert CHILD_STDOUT in invocation.stderr.read_text()
        assert "child-stderr:" + invocation.token in invocation.stderr.read_text()


def test_explicit_full_profile_supplies_a_live_observability_endpoint(trial: Trial):
    invocation = trial.launch(profile="full", mode="full")
    trial.wait_ready(invocation)
    trial.release(invocation)
    report = trial.finish(invocation, code=0, child_code=0)
    assert report["profile"] == "full"


def test_child_exit_42_is_preserved_in_metadata_and_maps_to_cli_failure(trial: Trial):
    invocation = trial.launch(profile="core", mode="exit42")
    trial.wait_ready(invocation)
    trial.release(invocation)
    report = trial.finish(invocation, code=1, child_code=42)
    assert report["profile"] == "core"
    assert CHILD_STDOUT in invocation.stderr.read_text()
    assert "child-stderr:" + invocation.token in invocation.stderr.read_text()


def test_sigterm_reaches_the_child_tree_and_cleans_the_owned_environment(trial: Trial):
    invocation = trial.launch(mode="signal")
    trial.wait_ready(invocation)
    descendant = trial.row(invocation)["descendant"]
    assert _alive(descendant)
    invocation.process.send_signal(signal.SIGTERM)
    invocation.process.wait(timeout=180)
    result = json.loads(invocation.stdout.read_text())
    assert result["child_exit_code"] in (-signal.SIGTERM, 128 + signal.SIGTERM)
    trial.finish(invocation, code=1, child_code=result["child_exit_code"])
    deadline = time.monotonic() + 10
    while _alive(descendant) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not _alive(descendant)


def test_actual_partial_startup_failure_is_transient_and_preserves_the_sentinel(trial: Trial):
    invocation = trial.launch(mode="partial")
    invocation.process.wait(timeout=660)
    with sqlite3.connect(trial.database, timeout=30) as connection:
        partial = list(
            connection.execute(
                "SELECT project, startup_code, containers, networks, volumes FROM partial"
            )
        )
    assert partial, "The caller wrapper never reached a real Compose startup"
    assert all(
        code == 0 and containers and networks and volumes
        for _, code, containers, networks, volumes in partial
    )
    report = trial.finish(invocation, code=3, child_code=None)
    assert {row[0] for row in partial} == {report["project"]}
    assert trial.row(invocation)["ready"] == 0
    assert "caller-injected-compose-startup-failure" in invocation.stderr.read_text()
