"""Full-profile local wipe must drop every volume the project mounted (#5362).

``curie local up`` starts the full profile. A plain ``curie local down`` then
removes the exited otel-collector-perms one-shot without its volumes, and
``curie local down --wipe --yes`` is the wipe. The RustFS image declares
VOLUME /data. A named project volume covers that path, so the wipe still
removes it after the container is gone.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO / "compose.dev.yaml"
PUBLISHED = (
    ("postgres", 5432),
    ("valkey", 6379),
    ("clickhouse", 8123),
    ("clickhouse", 9000),
    ("rustfs", 9000),
    ("rustfs", 9001),
    ("langfuse-web", 3000),
    ("otel-collector", 4317),
    ("otel-collector", 4318),
    ("otel-collector", 8888),
    ("curie-api", 8000),
    ("curie-ui", 8080),
)


def _run(
    argv: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"{purpose} failed with {result.returncode}: {detail}")
    return result.stdout


def _volume_ids(env: dict[str, str]) -> set[str]:
    listed = _require(_run(["docker", "volume", "ls", "-q"], env=env), "listing volumes")
    return {line.strip() for line in listed.splitlines() if line.strip()}


def _project_volume_ids(project: str, env: dict[str, str]) -> set[str]:
    listed = _require(
        _run(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ],
            env=env,
        ),
        "listing project volumes",
    )
    return {line.strip() for line in listed.splitlines() if line.strip()}


def _free_ports(count: int) -> list[int]:
    holders: list[socket.socket] = []
    try:
        for _ in range(count):
            holder = socket.socket()
            holder.bind(("127.0.0.1", 0))
            holders.append(holder)
        return [holder.getsockname()[1] for holder in holders]
    finally:
        for holder in holders:
            holder.close()


def _curie_binary() -> Path:
    supplied = os.environ.get("CURIE_BIN")
    if supplied:
        binary = Path(supplied).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not an executable file: {binary}")
        return binary
    candidates: list[Path] = []
    target = os.environ.get("CARGO_TARGET_DIR")
    if target:
        candidates.extend([Path(target) / "release" / "curie", Path(target) / "debug" / "curie"])
    candidates.extend(
        [
            REPO / "cli" / "target" / "release" / "curie",
            REPO / "cli" / "target" / "debug" / "curie",
        ]
    )
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate.resolve()
    built = _run(
        ["cargo", "build", "--release", "--locked", "--manifest-path", "cli/Cargo.toml"],
        timeout=1200,
    )
    _require(built, "building the source CLI")
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate.resolve()
    fallback = REPO / "cli" / "target" / "release" / "curie"
    if not fallback.is_file():
        raise RuntimeError(f"built CLI is missing: {fallback}")
    return fallback


@pytest.fixture(scope="session")
def curie_binary() -> Path:
    return _curie_binary()


@dataclass
class Mount:
    container: str
    destination: str
    name: str


@dataclass
class Cycle:
    binary: Path
    project: str
    foreign: str
    override: Path
    env: dict[str, str]
    mounts: list[Mount] = field(default_factory=list)

    def local(self, verb: str, *extra: str, timeout: int) -> subprocess.CompletedProcess[str]:
        return _run(
            [
                str(self.binary),
                "local",
                verb,
                "--project",
                self.project,
                "-f",
                str(COMPOSE_FILE),
                "-f",
                str(self.override),
                *extra,
            ],
            env=self.env,
            timeout=timeout,
        )

    def volume_labels(self, name: str) -> dict[str, str]:
        raw = _require(
            _run(
                ["docker", "volume", "inspect", name, "--format", "{{json .Labels}}"],
                env=self.env,
            ),
            "inspecting a volume",
        ).strip()
        parsed = json.loads(raw)
        return parsed or {}


def _override_text(project: str, ports: dict[tuple[str, int], int], stub: int) -> str:
    grouped: dict[str, list[str]] = {}
    for service, container in PUBLISHED:
        host = ports[(service, container)]
        grouped.setdefault(service, []).append(f"127.0.0.1:{host}:{container}")
    lines = ["services:"]
    for service, mappings in grouped.items():
        rendered = ", ".join(f'"{mapping}"' for mapping in mappings)
        lines.append(f"  {service}:")
        lines.append(f"    ports: !override [{rendered}]")
    postgres = ports[("postgres", 5432)]
    valkey = ports[("valkey", 6379)]
    s3 = ports[("rustfs", 9000)]
    api = ports[("curie-api", 8000)]
    lines.extend(
        [
            "  curie-worker:",
            "    environment:",
            "      DATABASE_URL: "
            f'"postgresql+asyncpg://postgres:${{CURIE_LOCAL_POSTGRES_PASSWORD:-postgres}}'
            f'@127.0.0.1:{postgres}/postgres"',
            '      VALKEY_HOST: "127.0.0.1"',
            f'      VALKEY_PORT: "{valkey}"',
            f'      S3_ENDPOINT_URL: "http://127.0.0.1:{s3}"',
            f'      CURIE_API_URL: "http://127.0.0.1:{api}"',
            f'      CURIE_DOCKER_NETWORK: "{project}_runner"',
            "  curie-channel-gateway:",
            "    environment:",
            f'      SLACK_API_BASE_URL: "http://127.0.0.1:{stub}/api/"',
            "networks:",
            "  curie_runner:",
            f'    name: "{project}_runner"',
            "",
        ]
    )
    return "\n".join(lines)


def _isolated_env(
    project: str,
    ports: dict[tuple[str, int], int],
    stub: int,
    gateway: int,
    config: Path,
) -> dict[str, str]:
    env = os.environ.copy()
    for name in (
        "COMPOSE_FILE",
        "COMPOSE_PROJECT_NAME",
        "CURIE_API_KEY",
        "CURIE_LOCAL_API_KEY",
        "CURIE_LOCAL_POSTGRES_PASSWORD",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CURIE_CREDENTIALS",
    ):
        env.pop(name, None)
    env.update(
        {
            "COMPOSE_PROJECT_NAME": project,
            "CURIE_CONFIG_DIR": str(config),
            "CURIE_FAKE_MODEL": "1",
            "CURIE_LOCAL_IMAGE_TAG": "dev",
            "CURIE_API_URL": f"http://127.0.0.1:{ports[('curie-api', 8000)]}",
            "VALKEY_HOST": "127.0.0.1",
            "VALKEY_PORT": str(ports[("valkey", 6379)]),
            "S3_ENDPOINT_URL": f"http://127.0.0.1:{ports[('rustfs', 9000)]}",
            "CURIE_DOCKER_NETWORK": f"{project}_runner",
            "CURIE_LOCAL_STUB_PORT": str(stub),
            "CURIE_LOCAL_GATEWAY_PORT": str(gateway),
            "CURIE_LOCAL_POSTGRES_HOST": "127.0.0.1",
            "CURIE_LOCAL_POSTGRES_PORT": str(ports[("postgres", 5432)]),
            "NO_COLOR": "1",
        }
    )
    return env


def _compose(cycle: Cycle, *args: str) -> subprocess.CompletedProcess[str]:
    return _run(
        [
            "docker",
            "compose",
            "--profile",
            "core",
            "--profile",
            "full",
            "--profile",
            "local-model",
            "--profile",
            "slack",
            "-p",
            cycle.project,
            "-f",
            str(COMPOSE_FILE),
            "-f",
            str(cycle.override),
            *args,
        ],
        env=cycle.env,
        timeout=180,
    )


def _cleanup(cycle: Cycle) -> None:
    errors: list[str] = []
    down = cycle.local("down", "--wipe", "--yes", timeout=180)
    if down.returncode != 0:
        fallback = _compose(cycle, "down", "-v", "--remove-orphans")
        if fallback.returncode != 0:
            errors.append((fallback.stderr or fallback.stdout or "down failed").strip())
    containers = _run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label=com.docker.compose.project={cycle.project}",
        ],
        env=cycle.env,
    )
    if containers.returncode == 0 and containers.stdout.strip():
        removed = _run(
            ["docker", "rm", "-f", "-v", *containers.stdout.split()],
            env=cycle.env,
            timeout=120,
        )
        if removed.returncode != 0:
            errors.append((removed.stderr or "container cleanup failed").strip())
    mounted_ids = {mount.name for mount in cycle.mounts}
    owned = mounted_ids | _project_volume_ids(cycle.project, cycle.env) | {cycle.foreign}
    for name in sorted(owned):
        if name not in _volume_ids(cycle.env):
            continue
        removed = _run(["docker", "volume", "rm", name], env=cycle.env)
        if removed.returncode != 0 and "No such volume" not in removed.stderr:
            errors.append((removed.stderr or f"could not remove {name}").strip())
    networks = _run(
        [
            "docker",
            "network",
            "ls",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={cycle.project}",
        ],
        env=cycle.env,
    )
    if networks.returncode == 0 and networks.stdout.strip():
        removed = _run(["docker", "network", "rm", *networks.stdout.split()], env=cycle.env)
        if removed.returncode != 0:
            errors.append((removed.stderr or "network cleanup failed").strip())
    if _project_volume_ids(cycle.project, cycle.env):
        errors.append("project volumes remained after cleanup")
    left = mounted_ids & _volume_ids(cycle.env)
    if left:
        errors.append(f"mounted volumes remained after cleanup: {sorted(left)}")
    still = _run(
        [
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"label=com.docker.compose.project={cycle.project}",
        ],
        env=cycle.env,
    )
    if still.returncode != 0 or still.stdout.strip():
        errors.append("project containers remained after cleanup")
    if cycle.foreign in _volume_ids(cycle.env):
        errors.append("the unrelated volume remained after cleanup")
    if errors:
        raise RuntimeError("; ".join(errors))


def _mounted(cycle: Cycle) -> list[Mount]:
    listed = _require(
        _run(
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                f"label=com.docker.compose.project={cycle.project}",
            ],
            env=cycle.env,
        ),
        "listing project containers",
    )
    mounts: list[Mount] = []
    for container_id in listed.split():
        raw = _require(
            _run(
                ["docker", "inspect", container_id, "--format", "{{json .}}"],
                env=cycle.env,
            ),
            "inspecting container mounts",
        )
        payload = json.loads(raw)
        container = str(payload.get("Name") or container_id)
        for mount in payload.get("Mounts") or []:
            if mount.get("Type") != "volume":
                continue
            mounts.append(
                Mount(
                    container=container,
                    destination=str(mount.get("Destination") or ""),
                    name=str(mount.get("Name") or ""),
                )
            )
    return mounts


@pytest.fixture
def cycle(tmp_path: Path, curie_binary: Path) -> Iterator[Cycle]:
    _require(
        _run(["docker", "info", "--format", "{{.ServerVersion}}"]),
        "reaching Docker",
    )
    project = f"c5362{uuid.uuid4().hex[:10]}"
    foreign = f"c5362foreign{uuid.uuid4().hex[:10]}"
    hosts = _free_ports(len(PUBLISHED) + 2)
    ports = {spec: host for spec, host in zip(PUBLISHED, hosts[:-2], strict=True)}
    stub, gateway = hosts[-2:]
    override = tmp_path / "compose.override.yaml"
    override.write_text(_override_text(project, ports, stub))
    config = tmp_path / "config"
    config.mkdir()
    started = Cycle(
        binary=curie_binary,
        project=project,
        foreign=foreign,
        override=override,
        env=_isolated_env(project, ports, stub, gateway, config),
    )
    try:
        _require(
            _run(["docker", "volume", "create", foreign], env=started.env),
            "creating an unrelated volume",
        )
        _require(started.local("up", timeout=240), "starting the full profile")
        started.mounts = _mounted(started)
        yield started
    finally:
        _cleanup(started)


def _perms_data(cycle: Cycle) -> Mount:
    data = [
        mount
        for mount in cycle.mounts
        if mount.container.endswith("-otel-collector-perms-1") and mount.destination == "/data"
    ]
    assert len(data) == 1
    return data[0]


def test_full_profile_perms_wipe_leaves_no_anonymous_volume(cycle: Cycle) -> None:
    """A full-profile up, plain down, and wipe leaves no mounted volume behind.

    The plain down removes the exited one-shot without its volumes. Compose
    5.6.1 still deletes an anonymous image volume when the wipe runs while
    that container exists, so this records the volume that remains once the
    container is already gone. An unrelated volume must survive the wipe.
    """

    assert cycle.mounts
    data = _perms_data(cycle)
    data_labels = cycle.volume_labels(data.name)
    mounted = {mount.name for mount in cycle.mounts}
    _require(cycle.local("down", timeout=180), "stopping the project")
    _require(cycle.local("down", "--wipe", "--yes", timeout=180), "wiping the project")
    remaining = _volume_ids(cycle.env)
    survived = sorted(name for name in mounted if name in remaining)
    # The surviving id is first. A volume that is still present fails here,
    # before the project label is checked. The release generator copies this
    # named mount, and its test locks that copy.
    assert not survived, f"volumes still present after wipe: {survived}"
    assert cycle.foreign in remaining
    assert data_labels.get("com.docker.compose.project") == cycle.project, (
        f"/data volume {data.name} labels {data_labels}"
    )
    assert data_labels.get("com.docker.compose.volume") == "otel_collector_perms_data"
    assert "com.docker.volume.anonymous" not in data_labels


def test_plain_down_keeps_named_project_volumes(cycle: Cycle) -> None:
    """A plain local down stops the stack and leaves named project volumes."""

    named = []
    for mount in cycle.mounts:
        labels = cycle.volume_labels(mount.name)
        if labels.get("com.docker.compose.project") == cycle.project:
            named.append(mount.name)
    assert named
    _require(cycle.local("down", timeout=180), "stopping the project")
    remaining = _volume_ids(cycle.env)
    missing = sorted(name for name in named if name not in remaining)
    assert not missing, f"plain down removed named volumes: {missing}"
    assert cycle.foreign in remaining
