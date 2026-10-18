#!/usr/bin/env python3
"""Own isolated backing services for a foreground command or preflight."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import ipaddress
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Never
from urllib.parse import quote


class TransientError(ValueError):
    """An unavailable operational dependency may recover with the same input."""


class GateFailure(ValueError):
    """A well-formed invocation reached a failing product verification gate."""


class Interrupted(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        self.returncode: int | None = None


def interrupt(signum: int, _frame: Any) -> Never:
    raise Interrupted(signum)


@contextmanager
def signal_handlers(handler: Any) -> Iterator[None]:
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        for number in previous:
            signal.signal(number, handler)
        yield
    finally:
        for number, old in previous.items():
            signal.signal(number, old)


def kill_group(process: subprocess.Popen[str], signum: int) -> None:
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        pass


def run_process(
    command: list[str],
    root: Path,
    environment: dict[str, str] | None = None,
    *,
    capture: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    pending: list[int] = []
    process: subprocess.Popen[str] | None = None
    try:
        # Defer parent interrupts across Popen so the child always has an owner.
        with signal_handlers(lambda number, _frame: pending.append(number)):
            process = subprocess.Popen(
                command,
                cwd=root,
                env=environment,
                text=True,
                stdout=subprocess.PIPE if capture else sys.stderr,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        if pending:
            raise Interrupted(pending[0])
        output, _ = process.communicate(timeout=timeout)
    except BaseException as error:
        if process is None:
            if isinstance(error, OSError):
                raise TransientError("Cannot start the required toolchain command") from error
            raise
        with signal_handlers(signal.SIG_IGN):
            kill_group(process, signal.SIGTERM)
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                kill_group(process, signal.SIGKILL)
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    raise TransientError("The owned process group could not be reaped") from None
            finally:
                kill_group(process, signal.SIGKILL)
        if isinstance(error, Interrupted):
            error.returncode = process.returncode
        if isinstance(error, subprocess.TimeoutExpired):
            raise TransientError("The required toolchain command timed out") from None
        raise
    finally:
        if process is not None:
            with signal_handlers(signal.SIG_IGN):
                kill_group(process, signal.SIGKILL)
    return subprocess.CompletedProcess(command, process.returncode, output or "")


def released_upgrade_environment(
    project: str, compose_files: str, postgres_port: int
) -> dict[str, str]:
    return {
        "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT": project,
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES": compose_files,
        "CURIE_RELEASED_UPGRADE_POSTGRES_HOST": "127.0.0.1",
        "CURIE_RELEASED_UPGRADE_POSTGRES_PORT": str(postgres_port),
        "CURIE_RELEASED_UPGRADE_INTEGRATION": "1",
    }


def private_service_environment(
    environment: dict[str, str], *, postgres: int, valkey: int, s3: int, otel: int | None
) -> dict[str, str]:
    host = environment.get("CURIE_TEST_ENV_HOST", "127.0.0.1")
    address = f"[{host}]" if ":" in host else host
    database = f"postgresql+asyncpg://postgres:postgres@{address}:{postgres}/postgres"
    valkey_url = (
        f"redis://:{quote(environment.get('TEST_VALKEY_PW', 'valkeypass'), safe='')}"
        f"@{address}:{valkey}/0"
    )
    result = {
        **environment,
        **released_upgrade_environment(
            environment["COMPOSE_PROJECT_NAME"], environment["COMPOSE_FILE"], postgres
        ),
        "CURIE_RELEASED_UPGRADE_POSTGRES_HOST": host,
        "CURIE_LOCAL_POSTGRES_HOST": host,
        "CURIE_LOCAL_POSTGRES_PORT": str(postgres),
        "DATABASE_URL": database,
        "TEST_DATABASE_URL": database,
        "TEST_VALKEY_HOST": host,
        "TEST_VALKEY_PORT": str(valkey),
        "TEST_VALKEY_URL": valkey_url,
        "CURIE_PRIMARY_SOURCE_RESOURCES": "1",
        "CURIE_SETTLEMENT_TEST_REDIS_URL": valkey_url,
        "VALKEY_HOST": host,
        "VALKEY_PORT": str(valkey),
        "S3_ENDPOINT_URL": f"http://{address}:{s3}",
        "TEST_S3_ENDPOINT_URL": f"http://{address}:{s3}",
    }
    for name in environment:
        if name.startswith("OTEL_EXPORTER_OTLP_") and name.endswith("_HEADERS"):
            result[name] = ""
    base = f"http://{address}:{otel}" if otel is not None else ""
    result.update(
        TEST_OTEL_COLLECTOR_ENDPOINT=f"{base}/v1/traces" if base else "",
        OTEL_EXPORTER_OTLP_ENDPOINT=base,
        OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf",
        CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT=base,
        CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT=base,
    )
    for lane in ("TRACES", "LOGS", "METRICS"):
        result[f"OTEL_EXPORTER_OTLP_{lane}_ENDPOINT"] = f"{base}/v1/{lane.lower()}" if base else ""
        result[f"OTEL_EXPORTER_OTLP_{lane}_PROTOCOL"] = "http/protobuf"
    if otel is None:
        for name in ("LANGFUSE_HOST", "TEST_LANGFUSE_HOST", "CURIE_LOCAL_OTEL_METRICS_URL"):
            result[name] = ""
    return result


def docker_output(
    root: Path, environment: dict[str, str], *arguments: str, timeout: float = 30
) -> str:
    if timeout <= 0:
        raise TransientError("The Docker operation deadline expired")
    result = run_process(["docker", *arguments], root, environment, timeout=timeout)
    if result.returncode:
        error_class = GateFailure if "config" in arguments else TransientError
        raise error_class(f"Docker {arguments[0]} operation failed (exit {result.returncode})")
    return result.stdout.strip()


def resource_inventory(
    root: Path, environment: dict[str, str], project: str, *, deadline: float | None = None
) -> dict[str, list[str]]:
    label = f"label=com.docker.compose.project={project}"
    return {
        kind: docker_output(
            root,
            environment,
            *arguments,
            "--filter",
            label,
            timeout=30 if deadline is None else min(30, deadline - time.monotonic()),
        ).splitlines()
        for kind, arguments in (
            ("containers", ("ps", "-aq")),
            ("networks", ("network", "ls", "-q")),
            ("volumes", ("volume", "ls", "-q")),
        )
    }


def owned_resources(root: Path, environment: dict[str, str], project: str) -> list[str]:
    return [
        identity
        for values in resource_inventory(root, environment, project).values()
        for identity in values
    ]


def validate_private_config(
    configuration: dict[str, Any],
    project: str,
    published: dict[str, list[int]],
    network_subnets: tuple[str, str],
) -> None:
    for service in published:
        ports = configuration["services"][service]["ports"]
        if any(port.get("host_ip") != "127.0.0.1" for port in ports):
            raise GateFailure("The private Compose configuration retained a shared binding")
    for name, expected_name, subnet in (
        ("default", f"{project}_default", network_subnets[0]),
        ("curie_runner", f"{project}_runner", network_subnets[1]),
    ):
        network = configuration["networks"][name]
        if network["name"] != expected_name:
            raise GateFailure(f"The private {name} network has an incorrect owner")
        if subnet not in [item.get("subnet") for item in network.get("ipam", {}).get("config", [])]:
            raise GateFailure(f"The private {name} network has no owned subnet")


def private_network_subnets(project: str, occupied: Sequence[str]) -> tuple[str, str]:
    """Choose two unused /24s outside Docker's exhausted automatic pools."""
    existing = [ipaddress.ip_network(value, strict=False) for value in occupied]
    pools = (
        ipaddress.IPv4Network("10.240.0.0/20"),
        ipaddress.IPv4Network("172.31.240.0/20"),
        ipaddress.IPv4Network("192.168.240.0/20"),
    )
    seed = int(hashlib.sha256(project.encode()).hexdigest(), 16)
    selected: list[str] = []
    for pool_index, pool in enumerate(pools):
        candidates = list(pool.subnets(new_prefix=24))
        start = (seed + pool_index) % len(candidates)
        for offset in range(len(candidates)):
            candidate = candidates[(start + offset) % len(candidates)]
            if any(candidate.overlaps(network) for network in existing):
                continue
            selected.append(str(candidate))
            existing.append(candidate)
            if len(selected) == 2:
                return selected[0], selected[1]
    raise TransientError("No free private preflight subnets remain in the reserved range")


def docker_network_subnets(root: Path, environment: dict[str, str]) -> list[str]:
    network_ids = docker_output(root, environment, "network", "ls", "-q").splitlines()
    if not network_ids:
        return []
    rendered = docker_output(
        root,
        environment,
        "network",
        "inspect",
        *network_ids,
        "--format",
        "{{json .IPAM.Config}}",
    )
    result: list[str] = []
    for line in rendered.splitlines():
        configurations = json.loads(line)
        if not isinstance(configurations, list):
            continue
        for configuration in configurations:
            subnet = configuration.get("Subnet")
            if isinstance(subnet, str):
                result.append(subnet)
    return result


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def port_is_occupied(port: int) -> bool:
    with socket.socket() as listener:
        try:
            listener.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False


@contextmanager
def worktree_lock(worktree: Path, project: str) -> Iterator[None]:
    directory = worktree / ".projects" / "preflight"
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"{project}.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TransientError(
                "Another full preflight owns this worktree's Compose project"
            ) from error
        yield


@contextmanager
def allocation_lock(root: Path, environment: dict[str, str]) -> Iterator[None]:
    daemon = docker_output(root, environment, "info", "--format", "{{.ID}}")
    if not daemon:
        raise TransientError("Docker returned no daemon identity")
    identity = hashlib.sha256(daemon.encode()).hexdigest()[:24]
    path = Path("/tmp") / f"curie-test-env-allocation-{identity}.lock"
    with path.open("a") as lock:
        deadline = time.monotonic() + 120
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TransientError("Private Docker network allocation is busy") from None
                time.sleep(0.1)
        yield


def cleanup_resources(root: Path, environment: dict[str, str], compose: list[str]) -> None:
    project = environment["COMPOSE_PROJECT_NAME"]
    deadline = time.monotonic() + 90
    with signal_handlers(signal.SIG_IGN):
        try:
            docker_output(
                root,
                environment,
                *compose,
                "--profile",
                "full",
                "down",
                "--timeout",
                "10",
                "--remove-orphans",
                "-v",
                timeout=min(30, deadline - time.monotonic()),
            )
        except (TransientError, GateFailure):
            pass
        inventory = resource_inventory(root, environment, project, deadline=deadline)
        for kind, command in (
            ("containers", ("rm", "-f", "-v")),
            ("networks", ("network", "rm")),
            ("volumes", ("volume", "rm")),
        ):
            if inventory[kind]:
                try:
                    docker_output(
                        root,
                        environment,
                        *command,
                        *inventory[kind],
                        timeout=min(30, deadline - time.monotonic()),
                    )
                except (TransientError, GateFailure):
                    pass
        if any(resource_inventory(root, environment, project, deadline=deadline).values()):
            raise TransientError("The owned Compose resources were not removed")


def seed_collector_config(root: Path, environment: dict[str, str], image: str) -> None:
    project = environment["COMPOSE_PROJECT_NAME"]
    volume = f"{project}_otel_config"
    seed = f"{project}-collector-seed"
    label = f"com.docker.compose.project={project}"
    docker_output(root, environment, "volume", "create", "--label", label, volume)
    result = run_process(
        [
            "docker",
            "create",
            "--pull=missing",
            "--name",
            seed,
            "--label",
            label,
            "--mount",
            f"type=volume,src={volume},dst=/etc/otel",
            image,
        ],
        root,
        environment,
        timeout=300,
    )
    if result.returncode:
        raise TransientError("Docker could not create the collector config seed container")
    docker_output(
        root,
        environment,
        "cp",
        str(root / "otel/collector-config.yaml"),
        f"{seed}:/etc/otel/collector-config.yaml",
    )
    docker_output(root, environment, "rm", "-v", seed)


@contextmanager
def private_services(
    root: Path,
    worktree: Path,
    *,
    profile: str = "full",
    project: str | None = None,
    migrate: bool = True,
    report: dict[str, Any] | None = None,
) -> Iterator[dict[str, str]]:
    stable = project is None
    project = project or (
        "curie-preflight-" + hashlib.sha256(str(worktree.resolve()).encode()).hexdigest()[:10]
    )
    if profile not in ("core", "full"):
        raise ValueError("Profile must be core or full")
    report = {} if report is None else report
    report.update(project=project, networks=[f"{project}_default", f"{project}_runner"])
    scratch = root / ".projects" / ("preflight" if stable else "test-env")
    scratch.mkdir(parents=True, exist_ok=True)
    lock = worktree_lock(worktree, project) if stable else nullcontext()
    with lock, tempfile.TemporaryDirectory(prefix="compose-", dir=scratch) as directory:
        override = Path(directory) / "compose.yaml"
        environment = {
            **os.environ,
            "COMPOSE_PROJECT_NAME": project,
            "COMPOSE_FILE": os.pathsep.join((str(root / "compose.dev.yaml"), str(override))),
            "COMPOSE_PROFILES": profile,
            "COMPOSE_PATH_SEPARATOR": os.pathsep,
            "CURIE_LOCAL_POSTGRES_PASSWORD": "postgres",
            "VALKEY_PASSWORD": "valkeypass",
            "TEST_VALKEY_PW": "valkeypass",
            "S3_ACCESS_KEY": "rustfs",
            "TEST_S3_ACCESS_KEY": "rustfs",
            "S3_SECRET_KEY": "rustfssecret",
            "TEST_S3_SECRET_KEY": "rustfssecret",
            "LANGFUSE_PUBLIC_KEY": "pk-lf-curie-dev",
            "TEST_LANGFUSE_PUBLIC_KEY": "pk-lf-curie-dev",
            "LANGFUSE_SECRET_KEY": "sk-lf-curie-dev",
            "TEST_LANGFUSE_SECRET_KEY": "sk-lf-curie-dev",
            "CI_REQUIRE_POSTGRES_TESTS": "1",
            "CI_REQUIRE_VALKEY_TESTS": "1",
            "CURIE_DOCKER_NETWORK": f"{project}_runner",
        }
        host = environment.get("CURIE_TEST_ENV_HOST", "127.0.0.1")
        if not host or any(
            char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.:-_"
            for char in host
        ):
            raise ValueError("CURIE_TEST_ENV_HOST must be a host name or IP address")
        address = f"[{host}]" if ":" in host else host
        compose = [
            "compose",
            "-p",
            project,
            "-f",
            str(root / "compose.dev.yaml"),
            "-f",
            str(override),
        ]
        with signal_handlers(interrupt):
            if owned_resources(root, environment, project):
                raise TransientError(
                    "The private preflight project already has resources; preserve its owner"
                )
            touched = False
            try:
                for attempt in range(3):
                    web_port = free_port() if profile == "full" else 0
                    published = {"postgres": [5432], "valkey": [6379], "rustfs": [9000, 9001]}
                    if profile == "full":
                        published.update(
                            {
                                "clickhouse": [8123, 9000],
                                "langfuse-web": [3000],
                                "otel-collector": [4317, 4318, 8888],
                            }
                        )
                    with allocation_lock(root, environment):
                        subnets = private_network_subnets(
                            project, docker_network_subnets(root, environment)
                        )
                        lines = ["services:"]
                        for service, targets in published.items():
                            bindings = [
                                f"127.0.0.1:{web_port if service == 'langfuse-web' else ''}:{port}"
                                for port in targets
                            ]
                            lines.extend(
                                (f"  {service}:", "    ports: !override " + json.dumps(bindings))
                            )
                            if service == "otel-collector":
                                lines.append(
                                    "    volumes: !override "
                                    + json.dumps(
                                        [
                                            "test_env_collector_config:/etc/otel:ro",
                                            "otel_collector_storage:/var/lib/otelcol/storage",
                                        ]
                                    )
                                )
                        lines.append("networks:")
                        for name, network, subnet in zip(
                            ("default", "curie_runner"), report["networks"], subnets, strict=True
                        ):
                            lines.extend(
                                (
                                    f"  {name}:",
                                    f"    name: {network}",
                                    "    ipam:",
                                    "      config:",
                                    f"        - subnet: {subnet}",
                                )
                            )
                        if profile == "full":
                            lines.extend(
                                (
                                    "volumes:",
                                    "  test_env_collector_config:",
                                    f"    name: {project}_otel_config",
                                    "    external: true",
                                )
                            )
                        override.write_text("\n".join(lines) + "\n")
                        configuration = json.loads(
                            docker_output(
                                root,
                                environment,
                                *compose,
                                "--profile",
                                profile,
                                "config",
                                "--format",
                                "json",
                            )
                        )
                        validate_private_config(configuration, project, published, subnets)
                        touched = True
                        report["cleanup_verified"] = False
                        for name, network, subnet in zip(
                            ("default", "curie_runner"), report["networks"], subnets, strict=True
                        ):
                            docker_output(
                                root,
                                environment,
                                "network",
                                "create",
                                "--driver",
                                "bridge",
                                "--subnet",
                                subnet,
                                "--label",
                                f"com.docker.compose.project={project}",
                                "--label",
                                f"com.docker.compose.network={name}",
                                network,
                            )
                    if profile == "full":
                        seed_collector_config(
                            root, environment, configuration["services"]["otel-collector"]["image"]
                        )
                        environment["TEST_LANGFUSE_HOST"] = f"http://{address}:{web_port}"
                        environment["LANGFUSE_HOST"] = environment["TEST_LANGFUSE_HOST"]
                        startup = run_process(
                            [
                                sys.executable,
                                "scripts/wait-for-langfuse.py",
                                "--start",
                                "--timeout-seconds",
                                "480",
                            ],
                            root,
                            environment,
                            capture=False,
                            timeout=510,
                        )
                    else:
                        startup = run_process(
                            [
                                "docker",
                                *compose,
                                "--profile",
                                "core",
                                "up",
                                "-d",
                                "--wait",
                                "--wait-timeout",
                                "480",
                                "postgres",
                                "valkey",
                                "rustfs",
                            ],
                            root,
                            environment,
                            capture=False,
                            timeout=510,
                        )
                        if startup.returncode == 0:
                            startup = run_process(
                                [
                                    "docker",
                                    *compose,
                                    "--profile",
                                    "core",
                                    "up",
                                    "-d",
                                    "rustfs-init",
                                ],
                                root,
                                environment,
                                capture=False,
                                timeout=90,
                            )
                        if startup.returncode == 0:
                            startup = run_process(
                                ["docker", *compose, "wait", "rustfs-init"],
                                root,
                                environment,
                                capture=False,
                                timeout=90,
                            )
                    if startup.returncode == 0:
                        break
                    web = (
                        docker_output(
                            root,
                            environment,
                            *compose,
                            "ps",
                            "--status",
                            "running",
                            "-q",
                            "langfuse-web",
                        )
                        if profile == "full"
                        else ""
                    )
                    if profile != "full" or web or not port_is_occupied(web_port) or attempt == 2:
                        raise TransientError(
                            f"Backing service startup failed (exit {startup.returncode})"
                        )
                    cleanup_resources(root, environment, compose)
                ports = {}
                port_names = {
                    ("postgres", 5432): "postgres",
                    ("valkey", 6379): "valkey",
                    ("rustfs", 9000): "rustfs",
                    ("rustfs", 9001): "rustfs-console",
                    ("clickhouse", 8123): "clickhouse",
                    ("clickhouse", 9000): "clickhouse-native",
                    ("langfuse-web", 3000): "langfuse-web",
                    ("otel-collector", 4317): "otel-grpc",
                    ("otel-collector", 4318): "otel-http",
                    ("otel-collector", 8888): "otel-metrics",
                }
                for service, targets in published.items():
                    for target in targets:
                        binding = docker_output(
                            root, environment, *compose, "port", service, str(target)
                        )
                        binding_host, value = binding.rsplit(":", 1)
                        if binding_host != "127.0.0.1":
                            raise GateFailure("The private Compose port is not bound to loopback")
                        ports[port_names[service, target]] = int(value)
                report["ports"] = ports
                environment = private_service_environment(
                    environment,
                    postgres=ports["postgres"],
                    valkey=ports["valkey"],
                    s3=ports["rustfs"],
                    otel=ports.get("otel-http"),
                )
                if profile == "full":
                    environment["TEST_LANGFUSE_HOST"] = f"http://{address}:{ports['langfuse-web']}"
                    environment["LANGFUSE_HOST"] = environment["TEST_LANGFUSE_HOST"]
                    environment["CURIE_LOCAL_OTEL_METRICS_URL"] = (
                        f"http://{address}:{ports['otel-metrics']}/metrics"
                    )
                if migrate:
                    migration = run_process(
                        ["uv", "run", "alembic", "upgrade", "head"],
                        root / "apps/api",
                        environment,
                        capture=False,
                        timeout=180,
                    )
                    if migration.returncode:
                        raise GateFailure("Python test database migrations failed")
                yield environment
            finally:
                if touched:
                    cleanup_resources(root, environment, compose)
                    report["cleanup_verified"] = True


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ValueError("Use --profile core|full -- COMMAND")


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    report: dict[str, Any] = {
        "profile": "core",
        "project": "",
        "ports": {},
        "networks": [],
        "child_exit_code": None,
        "cleanup_verified": True,
    }
    code = 0
    try:
        if "--" not in arguments:
            raise ValueError("A command after -- is required")
        boundary = arguments.index("--")
        parser = Parser(description=__doc__)
        parser.add_argument("--profile", default="core", choices=("core", "full"))
        options = parser.parse_args(arguments[:boundary])
        report["profile"] = options.profile
        command = arguments[boundary + 1 :]
        if not command or not command[0] or command[0].startswith("-"):
            raise ValueError("A command after -- is required")
        root = Path(__file__).resolve().parents[2]
        project = "curie-test-env-" + uuid.uuid4().hex[:16]
        with (
            signal_handlers(interrupt),
            private_services(
                root, root, profile=options.profile, project=project, migrate=False, report=report
            ) as environment,
        ):
            try:
                result = run_process(command, Path.cwd(), environment, capture=False)
                report["child_exit_code"] = result.returncode
            except Interrupted as error:
                report["child_exit_code"] = error.returncode
                raise
        if report["child_exit_code"] != 0:
            report["error"] = "The child command failed"
            code = 1
    except Interrupted as error:
        report.update(signal=error.signum, error="The test environment run was interrupted")
        code = 1 if report["child_exit_code"] is not None else 3
    except TransientError as error:
        report["error"] = str(error)
        code = 3
    except GateFailure as error:
        report["error"] = str(error)
        code = 1
    except ValueError as error:
        report["error"] = str(error)
        code = 2
    except OSError:
        report["error"] = "The test environment operation could not be completed"
        code = 3
    if not report["cleanup_verified"]:
        code = 3
    with signal_handlers(signal.SIG_IGN):
        print(json.dumps(report))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
