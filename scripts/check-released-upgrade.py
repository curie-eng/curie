"""Verify that the candidate migrations upgrade the latest released database.

The gate walks six phases per pair:

    released upgrade -> released head check -> SEED
        -> candidate upgrade -> candidate head check -> READ-BACK

The four alembic phases are #1706's original contract: they prove the candidate
migrations RUN against a database stamped by the released line. The two phases
added by #2098 prove the RESULT IS READABLE, which is a different claim and the
one #1914 needed: migration 0021 backfilled `curie.agent_channels` verbatim out
of `agents.slack_channel`, the copied rows then failed `ChannelBinding`'s
address rule, and `GET /agents` returned 500 for every agent while every
migration in the chain had reported success.

SEED writes rows in the shapes the RELEASED code actually permitted, straight
into the scratch database over SQL. It deliberately does not go through a
current-HEAD model: constructing rows that satisfy today's validators would
prove only that today's validators accept what they just produced. READ-BACK
then serializes the migrated rows through the CANDIDATE tree's `AgentOut` and
requires that they load.
"""

import argparse
import asyncio
import json
import os
import re
import secrets
import signal
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any

import asyncpg

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "compose.dev.yaml"
STABLE_TAG_PATTERN = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
COMMIT_PATTERN = re.compile(r"^[0-9a-f]+$")
COMPOSE_PROJECT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
# Always HEAD's copy of the runner, never the tree being judged. See that file's
# docstring: it is one fixed assertion harness executed inside the candidate's
# environment, so the thing that varies across directions is the package it
# imports, not the assertions themselves.
READBACK_SCRIPT = REPO_ROOT / "scripts" / "released_upgrade_readback.py"

# The six phase names, in the order `_upgrade_pair` walks them. Each one is a
# THREE-way coupling -- it is printed in a banner the marker checks read, it is
# what a `PairResult` reports, and `SELF_TEST_DIRECTIONS.expect_failure_phase`
# names it -- so they are constants rather than literals hand-synced across the
# module. The strings themselves are load-bearing and must not be reworded.
RELEASED_UPGRADE_PHASE = "released upgrade"
RELEASED_HEAD_CHECK_PHASE = "released head check"
SEED_PHASE = "seed"
CANDIDATE_UPGRADE_PHASE = "candidate upgrade"
CANDIDATE_HEAD_CHECK_PHASE = "candidate head check"
READBACK_PHASE = "read-back"

# One alembic phase, as `_upgrade_pair` walks it:
# (phase name, tree, ref, commit, alembic arguments).
_AlembicPhase = tuple[str, Path, str, str, tuple[str, ...]]


@dataclass(frozen=True)
class SelfTestDirection:
    """One pinned `--self-test` pair and the outcome it must produce.

    `expect_failure_phase` is the phase the pair MUST fail in, or `None` for a
    direction that must pass cleanly. `markers` are substrings the failure output
    must contain -- they are the defense against the right phase failing for the
    wrong reason, which is not hypothetical: a `MissingGreenlet` in the read-back
    also lands in the `read-back` phase, and only the markers separate "the
    failure we pinned" from "some failure".
    """

    released_ref: str
    candidate_ref: str
    expect_failure_phase: str | None
    markers: tuple[str, ...]


# The pinned directions. #1706's negative control is the FIRST row and is
# retained verbatim, markers included -- a self test that only knows how to fail
# is inert, and one that has lost its must-fail direction is worse. The other two
# rows are #2098's addition, and they are what keep both seed branches alive:
# every direction seeds through the legacy `agents.slack_channel` column (v0.6.2
# is pre-0021), while the bare gate is `v0.8.0 -> HEAD` and seeds through
# `agent_channels`.
SELF_TEST_DIRECTIONS: tuple[SelfTestDirection, ...] = (
    SelfTestDirection(
        released_ref="v0.6.2",
        candidate_ref="v0.7.0-rc.1",
        expect_failure_phase=CANDIDATE_UPGRADE_PHASE,
        markers=(
            "asyncpg.exceptions.UndefinedTableError",
            'relation "curie.agent_channels" does not exist',
            "0022_approvals_reply_kind.py",
        ),
    ),
    SelfTestDirection(
        # #1914's reproduction. v0.6.2 upgrades to v0.7.3 CLEANLY today, so the
        # only thing that can redden this direction is the read-back phase --
        # which makes it a clean pin on the read path rather than on migrations.
        released_ref="v0.6.2",
        candidate_ref="v0.7.3",
        expect_failure_phase=READBACK_PHASE,
        markers=("C-0a1b2c3d", "is not a Slack channel ID"),
    ),
    SelfTestDirection(
        # The must-PASS direction: v0.8.0 is the first tag containing the
        # tolerant `ChannelBindingOut`, so the same legacy rows that break
        # v0.7.3 must serialize here.
        released_ref="v0.6.2",
        candidate_ref="v0.8.0",
        expect_failure_phase=None,
        markers=(),
    ),
)


@dataclass(frozen=True)
class SeedAgent:
    """One row the seed writes before the candidate upgrade.

    `legacy` means the address is a shape the RELEASED code stored but the
    CURRENT write validator rejects. That flag is asserted against the live
    `curie_api.schemas._validate_channel_binding` in the gate's tests, so an
    address softened to make the gate green stops being legacy and reddens the
    suite instead.

    `approval_route_address` seeds `agents.approval_routes` with a route
    resolving to the same rejected address. Without it the sibling read
    projections (`ApprovalTargetOut`, `ApprovalApproversOut`,
    `ApprovalRouteBindingOut`) -- made tolerant by the SAME fix commit as
    `ChannelBindingOut` -- could be reverted with the gate staying green.
    """

    name: str
    address: str
    kind: str
    legacy: bool
    approval_route_address: str | None


@dataclass(frozen=True)
class LegacyStateSeed:
    """The released-shaped general-state row carried through the upgrade."""

    owner_name: str
    namespace: str
    key: str
    value: dict[str, Any]


@dataclass(frozen=True)
class SeedMetadata:
    """Exact optional rows represented by a rendered seed plan."""

    legacy_state: LegacyStateSeed | None


# Three agents, one binding each. The names are prefixed `gate-` so they are
# unmistakably synthetic, and all three names and all three addresses are
# distinct so neither the seed nor 0021's backfill can trip
# `agents_slack_channel_key`, `agent_channels_kind_address_key` or 0061's
# `agent_channels_route_key`.
#
# `C0EXAMPLE1` is the repo's sanctioned placeholder Slack id (`.gitleaks.toml`
# stopword); `#general` and `C-0a1b2c3d` cannot match the scanner's
# conversation-id rule at all.
SEED_FIXTURE: tuple[SeedAgent, ...] = (
    SeedAgent(
        # #143's shape: a literal channel NAME, stored before the validator
        # existed and reported as a success at the time.
        name="gate-legacy-hash",
        address="#general",
        kind="slack",
        legacy=True,
        approval_route_address=None,
    ),
    SeedAgent(
        # #1914's shape, exactly: a `C-<hex>` address.
        name="gate-legacy-prefix",
        address="C-0a1b2c3d",
        kind="slack",
        legacy=True,
        approval_route_address="C-0a1b2c3d",
    ),
    SeedAgent(
        # The valid neighbour. It is what proves the "one bad row takes the
        # whole endpoint down" property: a tolerant read path must return
        # EVERY agent, not merely the ones that happen to be well-formed.
        name="gate-valid",
        address="C0EXAMPLE1",
        kind="slack",
        legacy=False,
        approval_route_address=None,
    ),
)

# One pre-0031 general-state row. Its non-reserved namespace is load-bearing:
# `memory` and `transcript` are permanently shared reserved state and therefore
# do not prove that migration 0037 repaired a legacy general-state owner.
LEGACY_STATE_SEED = LegacyStateSeed(
    owner_name="gate-valid",
    namespace="upgrade-gate",
    key="legacy-shared-state",
    value={"survived": "released-upgrade"},
)


# The two `agents.approval_routes` storage eras, split by migration 0034
# (#1460). They are named constants because the era is a REQUIRED argument to
# the seed planner: a mistyped string must raise rather than quietly select a
# shape, which is the failure #2098 is closing here.
APPROVAL_ROUTE_ERA_LEGACY = "legacy"
APPROVAL_ROUTE_ERA_SPLIT = "split"


@dataclass(frozen=True)
class ColumnInfo:
    """One `information_schema.columns` row from the scratch database.

    `is_nullable` keeps Postgres' verbatim `"YES"` / `"NO"` rather than being
    coerced to a bool, so a mis-parsed introspection row cannot quietly read as
    "nullable" and disarm the mandatory-column check below.
    """

    table_name: str
    column_name: str
    is_nullable: str
    column_default: str | None


class GateError(RuntimeError):
    """A released upgrade gate setup or cleanup failure."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    output: str


@dataclass(frozen=True)
class PairResult:
    phase: str
    returncode: int
    output: str


@dataclass(frozen=True)
class ReleaseUpgradeResources:
    """One immutable Compose and Postgres scope for a gate invocation."""

    compose_project: str
    compose_files: tuple[Path, ...]
    postgres_host: str
    postgres_port: int


_RELEASED_UPGRADE_RESOURCE_ENV = (
    "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT",
    "CURIE_RELEASED_UPGRADE_COMPOSE_FILES",
    "CURIE_RELEASED_UPGRADE_POSTGRES_HOST",
    "CURIE_RELEASED_UPGRADE_POSTGRES_PORT",
)


def _released_upgrade_resources_from_env() -> ReleaseUpgradeResources:
    """Read the all or nothing isolated-resource contract.

    Without the contract, retain the shared baseline endpoint while respecting
    an ambient Compose project. A caller that supplies any isolation value must
    supply all four, so a partially redirected gate cannot mutate a database
    selected by a different Compose invocation.
    """

    values = {name: os.environ.get(name) for name in _RELEASED_UPGRADE_RESOURCE_ENV}
    present = [name for name, value in values.items() if value is not None]
    if not present:
        return ReleaseUpgradeResources(
            compose_project=os.environ.get("COMPOSE_PROJECT_NAME") or "curie",
            compose_files=(COMPOSE_FILE,),
            postgres_host="localhost",
            postgres_port=25432,
        )
    if len(present) != len(_RELEASED_UPGRADE_RESOURCE_ENV):
        missing = [
            name for name in _RELEASED_UPGRADE_RESOURCE_ENV if values[name] is None
        ]
        raise GateError(
            "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT, "
            "CURIE_RELEASED_UPGRADE_COMPOSE_FILES, "
            "CURIE_RELEASED_UPGRADE_POSTGRES_HOST, and "
            "CURIE_RELEASED_UPGRADE_POSTGRES_PORT must be set all together; "
            f"missing {', '.join(missing)}"
        )

    compose_project = values["CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT"]
    compose_files_value = values["CURIE_RELEASED_UPGRADE_COMPOSE_FILES"]
    postgres_host = values["CURIE_RELEASED_UPGRADE_POSTGRES_HOST"]
    postgres_port_value = values["CURIE_RELEASED_UPGRADE_POSTGRES_PORT"]
    assert compose_project is not None
    assert compose_files_value is not None
    assert postgres_host is not None
    assert postgres_port_value is not None

    if not COMPOSE_PROJECT_PATTERN.fullmatch(compose_project):
        raise GateError(
            "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT must be a nonempty "
            "lowercase Compose project name"
        )

    compose_file_values = compose_files_value.split(os.pathsep)
    if not compose_file_values or any(not value for value in compose_file_values):
        raise GateError(
            "CURIE_RELEASED_UPGRADE_COMPOSE_FILES must contain one or more "
            "ordered compose files"
        )
    compose_file_paths = tuple(Path(value) for value in compose_file_values)
    if any(not path.is_absolute() for path in compose_file_paths):
        raise GateError(
            "CURIE_RELEASED_UPGRADE_COMPOSE_FILES must contain existing absolute "
            "compose files"
        )
    compose_files = tuple(path.resolve() for path in compose_file_paths)
    missing_files = [str(path) for path in compose_files if not path.is_file()]
    if missing_files:
        raise GateError(
            "CURIE_RELEASED_UPGRADE_COMPOSE_FILES names missing files: "
            f"{', '.join(missing_files)}"
        )
    if len(compose_files) < 2 or compose_files[0] != COMPOSE_FILE:
        raise GateError(
            "CURIE_RELEASED_UPGRADE_COMPOSE_FILES must contain the base "
            "compose.dev.yaml followed by an isolated override"
        )

    if postgres_host not in {"localhost", "127.0.0.1", "::1"}:
        raise GateError(
            "CURIE_RELEASED_UPGRADE_POSTGRES_HOST must be localhost, 127.0.0.1, "
            "or ::1"
        )
    if not postgres_port_value.isascii() or not postgres_port_value.isdecimal():
        raise GateError(
            "CURIE_RELEASED_UPGRADE_POSTGRES_PORT must be a decimal TCP port"
        )
    postgres_port = int(postgres_port_value)
    if not 1 <= postgres_port <= 65535:
        raise GateError(
            "CURIE_RELEASED_UPGRADE_POSTGRES_PORT must be between 1 and 65535"
        )

    return ReleaseUpgradeResources(
        compose_project=compose_project,
        compose_files=compose_files,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
    )


def _run(command: list[str], *, cwd: Path = REPO_ROOT) -> CommandResult:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as exc:
        raise GateError(f"could not run {command[0]}: {exc}") from exc
    return CommandResult(completed.returncode, completed.stdout or "")


def _checked_output(
    command: list[str],
    *,
    description: str,
    cwd: Path = REPO_ROOT,
) -> str:
    result = _run(command, cwd=cwd)
    if result.returncode != 0:
        if result.output:
            print(result.output, end="" if result.output.endswith("\n") else "\n")
        raise GateError(f"{description} failed with exit code {result.returncode}")
    return result.output


def _resolve_ref(ref: str) -> str:
    commit = _checked_output(
        ["git", "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"],
        description=f"resolving Git ref {ref}",
    ).strip()
    if not COMMIT_PATTERN.fullmatch(commit):
        raise GateError(f"Git ref {ref} resolved to an invalid commit: {commit!r}")
    return commit


def _latest_released_tag() -> tuple[str, str]:
    main_commit = _resolve_ref("origin/main")
    tag_output = _checked_output(
        ["git", "tag", "--merged", main_commit, "--list"],
        description="listing stable tags reachable from origin/main",
    )
    stable_tags: list[tuple[tuple[int, int, int], str]] = []
    for tag in tag_output.splitlines():
        match = STABLE_TAG_PATTERN.fullmatch(tag)
        if match is None:
            continue
        version = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        stable_tags.append((version, tag))
    if not stable_tags:
        raise GateError("no stable vX.Y.Z release tag is reachable from origin/main")
    _, latest_tag = max(stable_tags)
    return latest_tag, _resolve_ref(latest_tag)


def _compose_command(
    *arguments: str, resources: ReleaseUpgradeResources
) -> list[str]:
    command = ["docker", "compose", "-p", resources.compose_project]
    for compose_file in resources.compose_files:
        command.extend(("-f", str(compose_file)))
    return [*command, *arguments]


def _database_url(database_name: str, *, resources: ReleaseUpgradeResources) -> str:
    host = resources.postgres_host
    if ":" in host:
        host = f"[{host}]"
    return (
        "postgresql+asyncpg://postgres:postgres@"
        f"{host}:{resources.postgres_port}/{database_name}"
    )


def _postgres_container_id(*, resources: ReleaseUpgradeResources) -> str:
    output = _checked_output(
        _compose_command("ps", "--all", "-q", "postgres", resources=resources),
        description="locating the selected compose Postgres service",
    )
    container_ids = [line.strip() for line in output.splitlines() if line.strip()]
    if len(container_ids) != 1:
        raise GateError(
            "the selected compose scope must contain exactly one Postgres "
            f"container, found {len(container_ids)}"
        )
    container_id = container_ids[0]
    labels_output = _checked_output(
        ["docker", "inspect", "--format", "{{json .Config.Labels}}", container_id],
        description="checking selected Postgres container ownership",
    )
    try:
        labels = json.loads(labels_output)
    except json.JSONDecodeError as exc:
        raise GateError(
            "could not parse selected Postgres container ownership labels"
        ) from exc
    if not isinstance(labels, dict):
        raise GateError("selected Postgres container has no ownership labels")
    if (
        labels.get("com.docker.compose.project") != resources.compose_project
        or labels.get("com.docker.compose.service") != "postgres"
    ):
        raise GateError(
            "selected Postgres container ownership labels do not match the "
            "requested compose project and service"
        )
    running = _checked_output(
        ["docker", "inspect", "--format", "{{.State.Running}}", container_id],
        description="checking selected Postgres container state",
    ).strip()
    if running != "true":
        raise GateError("selected Postgres container is not running")
    return container_id


def _published_postgres_endpoints(
    *, resources: ReleaseUpgradeResources
) -> tuple[tuple[str, int], ...]:
    output = _checked_output(
        _compose_command("port", "postgres", "5432", resources=resources),
        description="reading the selected compose Postgres published port",
    )
    endpoints: list[tuple[str, int]] = []
    for line in output.splitlines():
        value = line.strip()
        if not value:
            continue
        if value.startswith("["):
            host, separator, port_value = value[1:].partition("]:")
            if not separator:
                raise GateError(
                    f"could not parse published Postgres endpoint {value!r}"
                )
        else:
            host, separator, port_value = value.rpartition(":")
            if not separator or not host:
                raise GateError(
                    f"could not parse published Postgres endpoint {value!r}"
                )
        if not port_value.isascii() or not port_value.isdecimal():
            raise GateError(f"could not parse published Postgres endpoint {value!r}")
        port = int(port_value)
        if not 1 <= port <= 65535:
            raise GateError(f"could not parse published Postgres endpoint {value!r}")
        endpoints.append((host, port))
    if not endpoints:
        raise GateError("the selected compose Postgres service has no published port")
    return tuple(endpoints)


def _published_host_serves_requested_host(
    published_host: str, requested_host: str
) -> bool:
    if requested_host == "localhost":
        return published_host in {"127.0.0.1", "::1", "0.0.0.0", "::"}
    if requested_host == "127.0.0.1":
        return published_host in {"127.0.0.1", "0.0.0.0"}
    return published_host in {"::1", "::"}


async def _host_postgres_system_identifier(
    *, resources: ReleaseUpgradeResources
) -> str:
    try:
        connection = await asyncpg.connect(
            host=resources.postgres_host,
            port=resources.postgres_port,
            user="postgres",
            password="postgres",
            database="postgres",
            timeout=5,
            command_timeout=5,
        )
    except Exception as exc:
        raise GateError(
            "connecting to the configured host Postgres endpoint failed"
        ) from exc
    try:
        identifier = await connection.fetchval(
            "SELECT system_identifier FROM pg_control_system()"
        )
    except Exception as exc:
        raise GateError(
            "reading the configured host Postgres system identifier failed"
        ) from exc
    finally:
        await connection.close()
    if not isinstance(identifier, int | str) or not str(identifier):
        raise GateError("configured host Postgres returned no system identifier")
    return str(identifier)


def _check_postgres(*, resources: ReleaseUpgradeResources) -> None:
    _checked_output(
        ["docker", "compose", "version"],
        description="checking Docker Compose availability",
    )
    _postgres_container_id(resources=resources)
    _checked_output(
        _compose_command(
            "exec",
            "-T",
            "postgres",
            "pg_isready",
            "-U",
            "postgres",
            "-d",
            "postgres",
            resources=resources,
        ),
        description="checking the compose Postgres service",
    )
    published_endpoints = _published_postgres_endpoints(resources=resources)
    if not any(
        port == resources.postgres_port
        and _published_host_serves_requested_host(host, resources.postgres_host)
        for host, port in published_endpoints
    ):
        rendered = ", ".join(f"{host}:{port}" for host, port in published_endpoints)
        raise GateError(
            "the configured host Postgres endpoint does not match the selected "
            f"compose published port: expected {resources.postgres_host}:"
            f"{resources.postgres_port}, found {rendered}"
        )
    container_identifier = _checked_output(
        _database_command(
            "SELECT system_identifier FROM pg_control_system()",
            resources=resources,
            flags=("-t", "-A"),
        ),
        description="reading selected compose Postgres system identifier",
    ).strip()
    if not container_identifier:
        raise GateError("selected compose Postgres returned no system identifier")
    host_identifier = asyncio.run(_host_postgres_system_identifier(resources=resources))
    if host_identifier != container_identifier:
        raise GateError(
            "configured host Postgres endpoint is not the selected compose "
            "Postgres service"
        )


def _database_command(
    sql: str,
    *,
    resources: ReleaseUpgradeResources,
    database_name: str = "postgres",
    flags: tuple[str, ...] = (),
) -> list[str]:
    """psql into `database_name`, defaulting to the maintenance database.

    The default keeps every pre-#2098 caller (`_reset_database`, `_cleanup`)
    byte-for-byte identical: DROP/CREATE DATABASE cannot be issued from inside
    the database being dropped. The seed is the only caller that needs the
    SCRATCH database, and it is the reason this parameter exists.

    `flags` is spliced in ahead of `-v`, so the default empty tuple leaves those
    callers' argv unchanged; `_scratch_query` is the only caller that passes it.
    """

    return _compose_command(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        database_name,
        *flags,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
        resources=resources,
    )


def _scratch_query(
    database_name: str, sql: str, *, resources: ReleaseUpgradeResources
) -> str:
    """Run a read-only query against the scratch database and return stdout.

    `-t -A` strips the header, the row-count footer and the column padding, so
    the caller parses `|`-separated fields instead of psql's table art.
    """

    return _checked_output(
        _database_command(
            sql,
            resources=resources,
            database_name=database_name,
            flags=("-t", "-A"),
        ),
        description=f"querying scratch database {database_name}",
    )


def _reset_database(database_name: str, *, resources: ReleaseUpgradeResources) -> None:
    _checked_output(
        _database_command(
            f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)',
            resources=resources,
        ),
        description=f"dropping stale scratch database {database_name}",
    )
    _checked_output(
        _database_command(f'CREATE DATABASE "{database_name}"', resources=resources),
        description=f"creating scratch database {database_name}",
    )


def _add_worktree(path: Path, commit: str, *, label: str) -> None:
    _checked_output(
        ["git", "worktree", "add", "--detach", str(path), commit],
        description=f"creating detached {label} worktree",
    )


def _tree_command(tree: Path, *arguments: str) -> list[str]:
    """Resolve an executable's dependencies from the tree under judgement.

    Every phase that runs code runs it this way, so `uv` builds the environment
    from `<tree>`'s own lockfile rather than from HEAD's.
    """

    return [
        "uv",
        "run",
        "--frozen",
        "--project",
        str(tree),
        "--directory",
        str(tree / "apps" / "api"),
        *arguments,
    ]


def _alembic_command(tree: Path, *arguments: str) -> list[str]:
    return _tree_command(tree, "alembic", *arguments)


def _readback_command(tree: Path, *arguments: str) -> list[str]:
    """Run HEAD's read-back runner inside `tree`'s environment.

    It shares `_alembic_command`'s `_tree_command` prefix because it is the same
    trick: resolve the executable's dependencies from the tree under judgement.
    The inversion here is that the SCRIPT is HEAD's (`READBACK_SCRIPT`, never
    `tree`'s copy) while the `curie_api` it imports is the tree's -- one fixed
    assertion harness pointed at whichever read models we are judging.
    """

    return _tree_command(tree, "python", str(READBACK_SCRIPT), *arguments)


def _run_in_tree(
    command: list[str],
    *,
    database_url: str,
    phase: str,
    ref: str,
    commit: str,
) -> CommandResult:
    """Announce a phase, run it against the scratch database, echo its output.

    Both code-running phase families go through here so neither can drift off
    the other's contract: banner, copied environment plus `DATABASE_URL`, and
    stderr folded into stdout.

    The output has to come back combined AND echoed -- `_run_self_test` pins
    markers against it, and a marker that landed on a stderr stream the caller
    never captured is a marker that silently stops defending anything.

    Only `DATABASE_URL` is added to the environment. `curie_api` was verified to
    import with nothing else set at v0.7.3, v0.8.0 and HEAD; a candidate that
    needs more would fail here loudly rather than skip the phase.
    """

    print(f"=== {phase}: {ref} ({commit}) ===", flush=True)
    environment = os.environ.copy()
    environment["DATABASE_URL"] = database_url
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=environment,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as exc:
        raise GateError(f"could not run {phase}: {exc}") from exc
    output = completed.stdout or ""
    if output:
        print(output, end="" if output.endswith("\n") else "\n")
    return CommandResult(completed.returncode, output)


def _run_alembic(
    tree: Path,
    *,
    database_url: str,
    phase: str,
    ref: str,
    commit: str,
    arguments: tuple[str, ...],
) -> CommandResult:
    return _run_in_tree(
        _alembic_command(tree, *arguments),
        database_url=database_url,
        phase=phase,
        ref=ref,
        commit=commit,
    )


def _run_readback(
    tree: Path,
    *,
    database_url: str,
    phase: str,
    ref: str,
    commit: str,
    seed_metadata: SeedMetadata,
) -> CommandResult:
    arguments: list[str] = []
    for agent in SEED_FIXTURE:
        # Both options are `action="append"` on the runner side, so pairing them
        # per agent builds the same two lists a pass per option would.
        arguments.extend(("--expect-agent", agent.name))
        arguments.extend(("--expect-address", agent.address))

    legacy_state = seed_metadata.legacy_state
    if _candidate_supports_legacy_state(tree):
        if legacy_state is None:
            print(
                "Skipping released-state sentinel: released schema predates "
                "workflow_state_entries."
            )
        else:
            arguments.extend(("--expect-state-owner", legacy_state.owner_name))
            arguments.extend(("--expect-state-namespace", legacy_state.namespace))
            arguments.extend(("--expect-state-key", legacy_state.key))
            arguments.extend(
                ("--expect-state-value", json.dumps(legacy_state.value, sort_keys=True))
            )
    if _candidate_supports_route_identity(tree):
        # Every seeded binding is Slack, stored NULL by the released app.
        arguments.extend(("--expect-slack-identity", "default"))

    return _run_in_tree(
        _readback_command(tree, *arguments),
        database_url=database_url,
        phase=phase,
        ref=ref,
        commit=commit,
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_literal(payload: dict[str, Any] | None) -> str:
    if payload is None:
        return "NULL"
    return f"{_sql_literal(json.dumps(payload))}::jsonb"


def _introspect_columns(
    database_name: str, *, resources: ReleaseUpgradeResources
) -> tuple[ColumnInfo, ...]:
    """Read the scratch database's `curie` schema after the released upgrade.

    The seed cannot be written against one hard-coded schema: the released ref is
    `v0.6.2` for the `--self-test` directions (pre-0021, `agents.slack_channel`)
    and the latest stable tag for the bare gate (post-0021, `agent_channels`).
    Introspecting is what lets one fixture serve both.
    """

    rows = _scratch_query(
        database_name,
        "SELECT table_name, column_name, is_nullable, "
        "coalesce(column_default, '') FROM information_schema.columns "
        "WHERE table_schema = 'curie' "
        # The tables the fixture can write. Narrowing here rather than in the
        # consumers keeps `_verify_mandatory_columns` honest: it only reports a
        # mandatory column for a table the branch actually fills, so every other
        # table's rows were dead weight the caller had to filter back out.
        "AND table_name IN "
        "('agents', 'agent_channels', 'workflow_state_entries') "
        "ORDER BY table_name, ordinal_position",
        resources=resources,
    )
    columns: list[ColumnInfo] = []
    for line in rows.splitlines():
        if not line.strip():
            continue
        # maxsplit=3: a column DEFAULT expression may itself contain a `|`, and
        # it is the last field, so everything after the third separator is it.
        fields = line.split("|", 3)
        if len(fields) != 4:
            raise GateError(f"could not parse an information_schema row: {line!r}")
        table_name, column_name, is_nullable, column_default = fields
        columns.append(
            ColumnInfo(
                table_name=table_name,
                column_name=column_name,
                is_nullable=is_nullable,
                # psql renders SQL NULL as an empty field under `-t -A`, and a
                # real default is never the empty string, so the round-trip is
                # unambiguous.
                column_default=column_default or None,
            )
        )
    return tuple(columns)


def _verify_mandatory_columns(
    columns: tuple[ColumnInfo, ...],
    *,
    filled: dict[str, set[str]],
) -> None:
    """Refuse to seed if a mandatory column the fixture does not fill exists.

    Only the tables this branch actually writes are checked; a table the branch
    ignores has no rows to be incomplete.

    The value here is the message, not the rejection: without it, the next person
    to add a NOT NULL column without a server default gets a bare Postgres
    not-null error from inside a CI gate they did not write. With it, they are
    told which column to teach the fixture about.
    """

    unfilled = [
        f"{column.table_name}.{column.column_name}"
        for column in columns
        if column.table_name in filled
        and column.is_nullable == "NO"
        and column.column_default is None
        and column.column_name not in filled[column.table_name]
    ]
    if unfilled:
        raise GateError(
            "the released-upgrade seed fixture does not fill these mandatory "
            f"columns: {', '.join(unfilled)}. Add a value for each to "
            "SEED_FIXTURE's rendered statements in scripts/"
            "check-released-upgrade.py."
        )


def _detect_approval_route_era(released_tree: Path) -> str:
    """Decide which `approval_routes` shape the RELEASED code actually stored.

    This is a SECOND era, independent of the 0021 channel-storage era that
    `_plan_seed_statements` branches on (#2098). 0021 moved the channel binding
    into `curie.agent_channels`; 0034 (#1460) rewrote the approval route from
    `{"deploy": {"channel": X}}` into the split
    `{"deploy": {"resolution": {"kind": ..., "address": X}}}`. A released ref can
    sit BETWEEN them -- v0.7.0..v0.7.3 is post-0021 and pre-0034 -- so reading
    the route shape off the channel discriminator seeds a FUTURE-shaped row the
    released code could never have produced. The candidate's 0034 then rejects
    `resolution` as an unknown legacy key and the gate blames the migration for
    its own fixture, which is the worst failure mode a truth-telling gate has.

    The DATABASE cannot answer this, so do not "fix" this into an
    `information_schema` query. 0034 rewrites JSONB *content* only: it adds no
    column, drops none, and adds no constraint, and the freshly-upgraded scratch
    database is still empty, so there is no row whose shape could be observed.

    The released WORKTREE is the authority instead. `_upgrade_pair` runs
    `alembic upgrade head` against this tree, so every revision file present in
    it HAS been applied by the time the seed runs.
    """

    versions = released_tree / "apps" / "api" / "alembic" / "versions"
    if not versions.is_dir():
        # No versions directory means the tree is not what we think it is.
        # Guessing an era there is exactly how the bug above happens, so refuse.
        raise GateError(
            f"the released worktree has no alembic versions directory at "
            f"{versions}, so the approval-route era cannot be determined; "
            "refusing to guess an era and seed a route shape the released "
            "code may never have stored"
        )
    if any(versions.glob("0034_*.py")):
        return APPROVAL_ROUTE_ERA_SPLIT
    return APPROVAL_ROUTE_ERA_LEGACY


ROUTE_IDENTITY_REVISION_FILE = "0061_agent_channels_route_identity.py"


def _candidate_supports_route_identity(candidate_tree: Path) -> bool:
    """Whether the candidate carries migration 0061 exactly (ADR-0168 decision 3).

    The same exact-file gating as the 0037 state sentinel: only a candidate
    that names every Slack binding's identity is asked to prove it did.
    """

    versions = candidate_tree / "apps" / "api" / "alembic" / "versions"
    return (versions / ROUTE_IDENTITY_REVISION_FILE).is_file()


def _candidate_supports_legacy_state(candidate_tree: Path) -> bool:
    """Whether the candidate contains the exact 0037 repair capability.

    Older candidates used by the pinned self-test must never be asked to import
    or inspect the newer state model. The revision filename is authoritative:
    an arbitrary filename beginning with ``0037`` is not the repair.
    """

    versions = candidate_tree / "apps" / "api" / "alembic" / "versions"
    return any(path.is_file() for path in versions.glob("0037_*.py"))


def _route_literal(agent: SeedAgent, era: str) -> str:
    """Render one `agents.approval_routes` value in `era`'s storage shape.

    Both eras are reachable from `_plan_seed_statements`' `agent_channels`
    branch; only the legacy one is reachable from its pre-0021 `slack_channel`
    branch, which passes the era explicitly rather than inferring it.
    """

    if agent.approval_route_address is None:
        return _json_literal(None)
    if era == APPROVAL_ROUTE_ERA_SPLIT:
        # Post-0034 storage: already the split shape.
        return _json_literal(
            {
                "deploy": {
                    "resolution": {
                        "kind": agent.kind,
                        "address": agent.approval_route_address,
                    }
                }
            }
        )
    # Pre-0034 storage; migration 0034 (#1460) rewrites it into the split
    # `resolution` shape on the way up with NO validation, which is what carries
    # the legacy address into the read models.
    return _json_literal({"deploy": {"channel": agent.approval_route_address}})


def _plan_seed_statements(
    columns: tuple[ColumnInfo, ...],
    *,
    approval_route_era: str,
    released_state_repaired: bool = False,
) -> tuple[tuple[str, ...], SeedMetadata]:
    """Render released-shaped SQL and exact metadata for optional seeded rows.

    Branches on `agents.slack_channel` -- the pre/post-0021 discriminator -- for
    the channel binding, and on `approval_route_era` for the route shape. The two
    are INDEPENDENT migration eras and must not be read off one discriminator
    (#2098): a released ref between 0021 and 0034 gets the channel binding from
    the schema and the route shape from the released tree.

    `approval_route_era` is a required keyword with NO default on purpose. A
    default is a silent wrong-era guess, and a silent wrong-era guess is the bug.

    `released_state_repaired` distinguishes the first upgrade that applies 0037
    from a later stable-to-candidate direction where the released line already
    contains it. In the latter direction the sentinel owner is seeded with
    memory enabled up front: a post-0037 released database could legitimately
    contain that shared identity, but not under a memory-disabled owner.

    Raises `GateError` rather than returning an empty plan when neither binding
    location exists. A seed that no-ops turns the read-back into a vacuous pass,
    which is the exact class of failure #2098 exists to close.
    """

    if approval_route_era not in (
        APPROVAL_ROUTE_ERA_LEGACY,
        APPROVAL_ROUTE_ERA_SPLIT,
    ):
        raise GateError(
            f"unrecognized approval route era {approval_route_era!r}; expected "
            f"{APPROVAL_ROUTE_ERA_LEGACY!r} (pre-0034) or "
            f"{APPROVAL_ROUTE_ERA_SPLIT!r} (post-0034)"
        )

    present = {(column.table_name, column.column_name) for column in columns}
    statements: list[str] = []

    if released_state_repaired and ("agents", "memory") not in present:
        raise GateError(
            "the released tree contains the 0037 state repair but the released "
            "schema has no agents.memory column; refusing to fabricate an "
            "impossible repaired-state seed"
        )

    if ("agents", "slack_channel") in present:
        _verify_mandatory_columns(
            columns,
            filled={"agents": {"id", "name", "slack_channel", "approval_routes"}},
        )
        for agent in SEED_FIXTURE:
            # `slack_channel` present means pre-0021, and 0021 precedes 0034, so
            # this branch is pre-0034 by construction: the `slack_channel` +
            # `split` quadrant cannot exist and nothing here is conditioned on
            # the era. The legacy era is therefore passed as a constant, NOT
            # forwarded from `approval_route_era`.
            route = _route_literal(agent, APPROVAL_ROUTE_ERA_LEGACY)
            insert_columns = "id, name, slack_channel, approval_routes"
            insert_values = (
                f"gen_random_uuid(), {_sql_literal(agent.name)}, "
                f"{_sql_literal(agent.address)}, {route}"
            )
            if (
                released_state_repaired
                and agent.name == LEGACY_STATE_SEED.owner_name
            ):
                insert_columns += ", memory"
                insert_values += ", TRUE"
            statements.append(
                f"INSERT INTO curie.agents ({insert_columns})\n"
                f"VALUES ({insert_values})"
            )
    elif ("agent_channels", "address") in present:
        _verify_mandatory_columns(
            columns,
            filled={
                "agents": {"id", "name", "approval_routes"},
                "agent_channels": {"id", "agent_id", "kind", "address"},
            },
        )
        for agent in SEED_FIXTURE:
            # This is the branch that can reach BOTH eras, so the era is
            # forwarded rather than assumed. Post-0021 but pre-0034
            # (v0.7.0..v0.7.3) is the quadrant that only exists here: the channel
            # binding already lives in `agent_channels`, yet the route is still
            # the legacy `channel` shape -- seeding `resolution` there writes a
            # fixture the released code could never have produced, and the
            # candidate's 0034 rejects it as an unknown legacy key (#2098).
            route = _route_literal(agent, approval_route_era)
            insert_columns = "id, name, approval_routes"
            insert_values = f"gen_random_uuid(), {_sql_literal(agent.name)}, {route}"
            if (
                released_state_repaired
                and agent.name == LEGACY_STATE_SEED.owner_name
            ):
                insert_columns += ", memory"
                insert_values += ", TRUE"
            # One statement per agent, so the binding is written in the same
            # transaction as the row it belongs to and the new id never has to be
            # round-tripped back through psql. `endpoint` / `adapter` are left
            # NULL, the released shape of a Slack row (0024's
            # `agent_channels_route_pair_ck` permits it), which 0061 names
            # `default` under `agent_channels_route_ck`.
            statements.append(
                "WITH seeded AS (\n"
                f"    INSERT INTO curie.agents ({insert_columns})\n"
                f"    VALUES ({insert_values})\n"
                "    RETURNING id\n"
                ")\n"
                "INSERT INTO curie.agent_channels (id, agent_id, kind, address)\n"
                f"SELECT gen_random_uuid(), seeded.id, "
                f"{_sql_literal(agent.kind)}, {_sql_literal(agent.address)}\n"
                "FROM seeded"
            )
    else:
        raise GateError(
            "the released schema has neither agents.slack_channel nor an "
            "agent_channels.address column, so the seed has nowhere to write a "
            "channel binding; refusing to run a read-back that would pass "
            "vacuously"
        )

    legacy_state: LegacyStateSeed | None = None
    if any(table == "workflow_state_entries" for table, _column in present):
        state_filled = {"id", "agent_id", "namespace", "key", "value"}
        has_binding_scope = (
            "workflow_state_entries",
            "binding_scope",
        ) in present
        if has_binding_scope:
            state_filled.add("binding_scope")
        _verify_mandatory_columns(
            columns,
            filled={"workflow_state_entries": state_filled},
        )

        legacy_state = LEGACY_STATE_SEED
        insert_columns = "id, agent_id, namespace, key, value"
        select_values = (
            "gen_random_uuid(), id, "
            f"{_sql_literal(legacy_state.namespace)}, "
            f"{_sql_literal(legacy_state.key)}, "
            f"{_json_literal(legacy_state.value)}"
        )
        if has_binding_scope:
            insert_columns = "id, agent_id, binding_scope, namespace, key, value"
            select_values = (
                "gen_random_uuid(), id, NULL, "
                f"{_sql_literal(legacy_state.namespace)}, "
                f"{_sql_literal(legacy_state.key)}, "
                f"{_json_literal(legacy_state.value)}"
            )
        statements.append(
            "INSERT INTO curie.workflow_state_entries "
            f"({insert_columns})\n"
            f"SELECT {select_values}\n"
            "FROM curie.agents\n"
            f"WHERE name = {_sql_literal(legacy_state.owner_name)}"
        )

    return tuple(statements), SeedMetadata(legacy_state=legacy_state)


def _seed_released_database(
    database_name: str,
    *,
    resources: ReleaseUpgradeResources,
    phase: str,
    approval_route_era: str,
    released_state_repaired: bool = False,
) -> SeedMetadata:
    """Write the fixture into the released database, between the two upgrades.

    Every failure in here is a SETUP fault, not a gate verdict: it raises
    `GateError`, which `main` reports as `Released upgrade gate failed:` and
    exit 1, distinct from a `PairResult` verdict. A broken seed must never be
    reported as "the migration is fine" or as "the read path is broken".
    """

    print(f"=== {phase}: {database_name} ===", flush=True)
    columns = _introspect_columns(database_name, resources=resources)
    statements, metadata = _plan_seed_statements(
        columns,
        approval_route_era=approval_route_era,
        released_state_repaired=released_state_repaired,
    )
    state_capable = any(
        column.table_name == "workflow_state_entries" for column in columns
    )
    state_statement_planned = any(
        "curie.workflow_state_entries" in statement for statement in statements
    )
    if state_capable and (
        metadata.legacy_state is None or not state_statement_planned
    ):
        raise GateError(
            "the released schema contains workflow_state_entries but the seed "
            "planner produced no legacy-state sentinel; refusing a vacuous "
            "released-state upgrade check"
        )
    # One psql invocation for the whole plan, not one per row. psql runs a
    # multi-statement simple query inside a single implicit transaction, so the
    # fixture now lands whole or not at all -- and a PARTIAL seed is worse than
    # none, because the read-back would then pass on whichever rows survived.
    _checked_output(
        _database_command(
            ";\n".join(statements),
            resources=resources,
            database_name=database_name,
        ),
        description=f"seeding the released database {database_name}",
    )
    print(
        f"Seeded {len(statements)} released-shaped rows into "
        f"{database_name}."
    )
    return metadata


def _upgrade_pair(
    released_tree: Path,
    candidate_tree: Path,
    *,
    resources: ReleaseUpgradeResources,
    database_url: str,
    database_name: str,
    released_ref: str,
    released_commit: str,
    candidate_ref: str,
    candidate_commit: str,
) -> PairResult:
    released_phases: tuple[_AlembicPhase, ...] = (
        (
            RELEASED_UPGRADE_PHASE,
            released_tree,
            released_ref,
            released_commit,
            ("upgrade", "head"),
        ),
        (
            RELEASED_HEAD_CHECK_PHASE,
            released_tree,
            released_ref,
            released_commit,
            ("current", "--check-heads"),
        ),
    )
    candidate_phases: tuple[_AlembicPhase, ...] = (
        (
            CANDIDATE_UPGRADE_PHASE,
            candidate_tree,
            candidate_ref,
            candidate_commit,
            ("upgrade", "head"),
        ),
        (
            CANDIDATE_HEAD_CHECK_PHASE,
            candidate_tree,
            candidate_ref,
            candidate_commit,
            ("current", "--check-heads"),
        ),
    )

    def _walk(phases: tuple[_AlembicPhase, ...]) -> PairResult | None:
        """Run phases in order; return the FIRST failure, or None if all passed.

        Returning `None` for success rather than a `PairResult` is what keeps the
        caller's control flow honest: only a failing phase short-circuits the
        pair, and the pair's real verdict is the read-back at the end.
        """

        for phase, tree, ref, commit, arguments in phases:
            result = _run_alembic(
                tree,
                database_url=database_url,
                phase=phase,
                ref=ref,
                commit=commit,
                arguments=arguments,
            )
            if result.returncode != 0:
                return PairResult(phase, result.returncode, result.output)
        return None

    failure = _walk(released_phases)
    if failure is not None:
        return failure

    # The seed sits HERE, between the two lines' phases, because that is the only
    # window in which the released schema is what an operator's database actually
    # looks like. Seeding earlier has no schema to write into; seeding later
    # writes rows the candidate migrations never had to carry.
    #
    # The route era is detected HERE, from the released tree, and not from the
    # schema the seed introspects: the 0021 and 0034 eras are independent and a
    # released ref can sit between them (#2098).
    seed_metadata = _seed_released_database(
        database_name,
        resources=resources,
        phase=SEED_PHASE,
        approval_route_era=_detect_approval_route_era(released_tree),
        released_state_repaired=_candidate_supports_legacy_state(released_tree),
    )

    failure = _walk(candidate_phases)
    if failure is not None:
        return failure

    readback = _run_readback(
        candidate_tree,
        database_url=database_url,
        phase=READBACK_PHASE,
        ref=candidate_ref,
        commit=candidate_commit,
        seed_metadata=seed_metadata,
    )
    return PairResult(READBACK_PHASE, readback.returncode, readback.output)


def _cleanup(
    worktrees: list[Path],
    *,
    resources: ReleaseUpgradeResources,
    database_name: str,
    database_touched: bool,
) -> None:
    errors: list[str] = []
    for worktree in reversed(worktrees):
        try:
            result = _run(
                ["git", "worktree", "remove", "--force", str(worktree)]
            )
        except GateError as exc:
            errors.append(str(exc))
        else:
            if result.returncode != 0:
                errors.append(
                    f"removing scratch worktree {worktree} failed: "
                    f"{result.output.strip()}"
                )
    if database_touched:
        try:
            result = _run(
                _database_command(
                    f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)',
                    resources=resources,
                )
            )
        except GateError as exc:
            errors.append(str(exc))
        else:
            if result.returncode != 0:
                errors.append(
                    f"dropping scratch database {database_name} failed: "
                    f"{result.output.strip()}"
                )
    if errors:
        rendered = "\n".join(f"  {error}" for error in errors)
        raise GateError(f"scratch cleanup failed:\n{rendered}")


def _run_pair(
    *,
    resources: ReleaseUpgradeResources,
    released_ref: str,
    released_commit: str,
    candidate_ref: str,
    candidate_commit: str,
    current_candidate: bool,
) -> PairResult:
    _check_postgres(resources=resources)
    database_name = f"curie_upgrade_{os.getpid()}_{secrets.token_hex(4)}"
    database_url = _database_url(database_name, resources=resources)
    worktrees: list[Path] = []
    database_touched = False
    with tempfile.TemporaryDirectory(prefix="curie-released-upgrade-") as temp_dir:
        temp_root = Path(temp_dir)
        try:
            database_touched = True
            _reset_database(database_name, resources=resources)

            released_tree = temp_root / "released"
            _add_worktree(released_tree, released_commit, label="released")
            worktrees.append(released_tree)

            if current_candidate:
                candidate_tree = REPO_ROOT
            else:
                candidate_tree = temp_root / "candidate"
                _add_worktree(candidate_tree, candidate_commit, label="candidate")
                worktrees.append(candidate_tree)

            return _upgrade_pair(
                released_tree,
                candidate_tree,
                resources=resources,
                database_url=database_url,
                database_name=database_name,
                released_ref=released_ref,
                released_commit=released_commit,
                candidate_ref=candidate_ref,
                candidate_commit=candidate_commit,
            )
        finally:
            _cleanup(
                worktrees,
                resources=resources,
                database_name=database_name,
                database_touched=database_touched,
            )


def _describe(direction: SelfTestDirection) -> str:
    return f"{direction.released_ref} to {direction.candidate_ref}"


def _check_direction(
    direction: SelfTestDirection, *, resources: ReleaseUpgradeResources
) -> str | None:
    """Run one pinned direction. Returns a mismatch description, or None on match.

    It RETURNS the mismatch instead of raising it so `_run_self_test` can walk
    every direction and report all of them. A self test that stops at the first
    mismatch hides the second one behind a fix for the first, and the whole point
    of the table is that the directions defend different properties.
    """

    result = _run_pair(
        resources=resources,
        released_ref=direction.released_ref,
        released_commit=_resolve_ref(direction.released_ref),
        candidate_ref=direction.candidate_ref,
        candidate_commit=_resolve_ref(direction.candidate_ref),
        current_candidate=False,
    )

    if direction.expect_failure_phase is None:
        if result.returncode != 0:
            return (
                f"self test direction {_describe(direction)} was expected to "
                f"upgrade and read back cleanly, but failed during "
                f"{result.phase} with exit code {result.returncode}"
            )
        return None

    if result.returncode == 0:
        return (
            f"self test direction {_describe(direction)} unexpectedly "
            f"succeeded; it must fail during {direction.expect_failure_phase}"
        )
    if result.phase != direction.expect_failure_phase:
        return (
            f"self test direction {_describe(direction)} failed during "
            f"{result.phase}, before the expected "
            f"{direction.expect_failure_phase} failure"
        )
    missing_markers = [
        marker for marker in direction.markers if marker not in result.output
    ]
    if missing_markers:
        # The markers are what separate "the failure we pinned" from "some
        # failure in the right phase". Dropping this check would let a
        # MissingGreenlet, or any incidental error, stand in for the defect the
        # direction exists to reproduce.
        return (
            f"self test direction {_describe(direction)} failed during "
            f"{result.phase} but did not report the known markers: "
            f"{', '.join(missing_markers)}"
        )
    return None


def _run_self_test(*, resources: ReleaseUpgradeResources) -> int:
    mismatches: list[str] = []
    for direction in SELF_TEST_DIRECTIONS:
        expectation = (
            "must pass"
            if direction.expect_failure_phase is None
            else f"must fail during {direction.expect_failure_phase}"
        )
        print(
            f"=== self test direction {_describe(direction)} ({expectation}) ===",
            flush=True,
        )
        mismatch = _check_direction(direction, resources=resources)
        if mismatch is not None:
            mismatches.append(mismatch)

    if mismatches:
        rendered = "\n".join(f"  {mismatch}" for mismatch in mismatches)
        raise GateError(f"released upgrade self test mismatched:\n{rendered}")

    print(
        f"Released upgrade self test passed across {len(SELF_TEST_DIRECTIONS)} "
        "pinned directions."
    )
    return 0


def _run_requested_pair(
    released_ref: str,
    candidate_ref: str,
    *,
    resources: ReleaseUpgradeResources,
) -> int:
    released_commit = _resolve_ref(released_ref)
    candidate_commit = _resolve_ref(candidate_ref)
    result = _run_pair(
        resources=resources,
        released_ref=released_ref,
        released_commit=released_commit,
        candidate_ref=candidate_ref,
        candidate_commit=candidate_commit,
        current_candidate=False,
    )
    if result.returncode != 0:
        print(
            f"Released upgrade gate failed during {result.phase} with exit "
            f"code {result.returncode}.",
            file=sys.stderr,
        )
    return result.returncode


def _run_normal(*, resources: ReleaseUpgradeResources) -> int:
    released_ref, released_commit = _latest_released_tag()
    candidate_commit = _resolve_ref("HEAD")
    print(
        f"Selected latest stable release {released_ref} ({released_commit}) "
        "reachable from origin/main."
    )
    result = _run_pair(
        resources=resources,
        released_ref=released_ref,
        released_commit=released_commit,
        candidate_ref="HEAD",
        candidate_commit=candidate_commit,
        current_candidate=True,
    )
    if result.returncode != 0:
        print(
            f"Released upgrade gate failed during {result.phase} with exit "
            f"code {result.returncode}.",
            file=sys.stderr,
        )
        return result.returncode
    print(
        f"Released upgrade gate passed from {released_ref} to candidate "
        f"{candidate_commit}."
    )
    return 0


def _raise_interrupted(_signum: int, _frame: FrameType | None) -> None:
    raise KeyboardInterrupt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upgrade a released Curie database with candidate migrations."
    )
    parser.add_argument(
        "--released-ref",
        help="Released Git ref for an explicit upgrade pair.",
    )
    parser.add_argument(
        "--candidate-ref",
        help="Candidate Git ref for an explicit upgrade pair.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Require every pinned direction: the known revision collision, the "
            "known read-back failure, and the direction that must pass."
        ),
    )
    args = parser.parse_args()

    if args.self_test and (args.released_ref or args.candidate_ref):
        parser.error("--self-test cannot be combined with explicit refs")
    if bool(args.released_ref) != bool(args.candidate_ref):
        parser.error("--released-ref and --candidate-ref must be provided together")

    signal.signal(signal.SIGTERM, _raise_interrupted)
    try:
        resources = _released_upgrade_resources_from_env()
        if args.self_test:
            return _run_self_test(resources=resources)
        if args.released_ref and args.candidate_ref:
            return _run_requested_pair(
                args.released_ref, args.candidate_ref, resources=resources
            )
        return _run_normal(resources=resources)
    except KeyboardInterrupt:
        print("Released upgrade gate interrupted after cleanup.", file=sys.stderr)
        return 130
    except GateError as exc:
        print(f"Released upgrade gate failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
