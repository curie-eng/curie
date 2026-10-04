"""Declared factory verification preflight (#3375, #3521).

A factory execution reports real sandbox verification before the model starts.
The runner does not assume a toolchain: the checks come from a declaration.
The bundle may ship ``verification/checks.json`` (operator owned, next to
``progress/phases.json``) and the repository may ship
``.curie/verification.json``. Bundle checks win when the bundle declares any;
otherwise the repository's are used; otherwise nothing runs and a single
``not_declared`` record is reported.

Each check is an argv list run without a shell in the mounted checkout, with
package managers forced offline. A lockfile-pinned install may run first, but
only when the bundle sets ``lockfile_installs``. Every record must be accepted
by the api or boot fails before model start.

After the probes each declared check takes one route (#3873). A passed or
failed check is executable. An unavailable check that declares
``delegated_to`` is delegated to that required pull request CI check. An
unavailable check without it is blocked, and any blocked check stops the run
before the model starts. A ``not_declared`` record takes no route.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import anyio

from .progress import ProgressClient
from .subprocess_env import shell_and_hook_env

logger = logging.getLogger(__name__)

BUNDLE_VERIFICATION_FILE = Path("verification") / "checks.json"
REPOSITORY_VERIFICATION_FILE = Path(".curie") / "verification.json"

_CHECK_ID = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
# Declared values reach the model prompt only inside a fenced JSON data block.
# Path globs and command arguments may be any printable text except control
# characters and backticks; install option values use the strict argument
# alphabet.
_ARGUMENT = re.compile(r"^[A-Za-z0-9_./:=@%+,*?{}\[\]-]+$")
_UNPRINTABLE = re.compile(r"[\x00-\x1f\x7f`]")
_MAX_CHECKS = 4
_MAX_PATHS = 8
_MAX_PATH_CHARS = 64
_MAX_ARGV = 16
_MAX_ARG_CHARS = 128
_MAX_COMMAND_CHARS = 120
_MAX_DELEGATED_CHARS = 64
# The api stores each observation as canonical compact JSON and rejects a note
# over this many UTF-8 bytes.
MAX_STORED_OBSERVATION_BYTES = 280
_MAX_FILE_BYTES = 64 * 1024
_MAX_REPORTED_NAMES = 8
_MAX_NAME_CHARS = 64
# The only accepted install forms: (program, subcommand) -> (the flags of which
# at least one must be present, empty meaning none is required; the allowed
# bare options; the allowed ``--name=value`` option names). Anything else, such
# as a registry, index, upgrade, or no-lockfile option, is rejected.
_INSTALL_FORMS: dict[tuple[str, str], tuple[frozenset[str], frozenset[str], frozenset[str]]] = {
    ("uv", "sync"): (
        frozenset({"--frozen", "--locked"}),
        frozenset(
            {
                "--frozen",
                "--locked",
                "--all-packages",
                "--all-extras",
                "--all-groups",
                "--no-dev",
                "--no-install-project",
            }
        ),
        frozenset({"--extra", "--group", "--package"}),
    ),
    ("cargo", "fetch"): (
        frozenset({"--locked"}),
        frozenset({"--locked", "--frozen"}),
        frozenset({"--target", "--manifest-path"}),
    ),
    ("pnpm", "install"): (
        frozenset({"--frozen-lockfile"}),
        frozenset({"--frozen-lockfile", "--prefer-offline", "--ignore-scripts"}),
        frozenset({"--filter"}),
    ),
    ("npm", "ci"): (
        frozenset(),
        frozenset({"--ignore-scripts", "--no-audit", "--no-fund"}),
        frozenset(),
    ),
}
_TIMEOUT_SECONDS = 600.0
# Forces the common package managers offline for the check itself; only a
# declared, bundle-allowed install may reach a registry.
_OFFLINE_ENV = {
    "UV_OFFLINE": "1",
    "UV_NO_SYNC": "1",
    "UV_LOCKED": "1",
    "CARGO_NET_OFFLINE": "true",
    "npm_config_offline": "true",
}

_SERVICE_FAILURE = re.compile(
    r"(?:connection refused|could not connect|cannot connect|connection reset|"
    r"connection error|no route to host|temporary failure in name resolution|"
    r"service unavailable|failed to connect)",
    re.IGNORECASE,
)
_KNOWN_SERVICE_MARKERS: dict[str, tuple[str, ...]] = {
    "docker": ("docker daemon", "docker.sock", "docker service"),
    "postgres": ("postgres", "postgresql", ":5432"),
    "valkey": ("valkey", "redis", ":6379"),
    "clickhouse": ("clickhouse", ":8123"),
    "rustfs": ("rustfs", "s3 endpoint"),
    "langfuse": ("langfuse", ":3000", ":23000"),
}
# Offline or registry-fetch failures, named by the package manager's own text.
_PACKAGE_REGISTRY_FAILURE = re.compile(
    r"(?:network connectivity is disabled|--offline was specified|"
    r"failed to download|ENOTCACHED|ERR_PNPM_NO_OFFLINE_(?:TARBALL|META))",
    re.IGNORECASE,
)
_PACKAGE_REGISTRY = "package_registry"
_KNOWN_BINARIES = (
    "uv",
    "pytest",
    "python3",
    "python",
    "docker",
    "cargo",
    "rustc",
    "pnpm",
    "npm",
    "node",
)
# Names the runner itself knows; any other reported name came from a declaration.
KNOWN_BLOCKER_NAMES = frozenset((*_KNOWN_BINARIES, *_KNOWN_SERVICE_MARKERS, _PACKAGE_REGISTRY))


@dataclass(frozen=True)
class VerificationCheck:
    """One declared check: the area it covers and its argv lists."""

    id: str
    paths: tuple[str, ...]
    command: tuple[str, ...]
    install: tuple[str, ...] | None = None
    delegated_to: str | None = None


@dataclass(frozen=True)
class VerificationDeclaration:
    """The resolved declaration. ``unreadable`` names a malformed repository file."""

    source: str | None
    lockfile_installs: bool
    unreadable: str | None
    checks: tuple[VerificationCheck, ...]


def _argv(raw: object, field: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not 1 <= len(raw) <= _MAX_ARGV:
        raise ValueError(f"{field} must be an argv list of 1 to {_MAX_ARGV} strings")
    for arg in raw:
        if (
            not isinstance(arg, str)
            or not 1 <= len(arg) <= _MAX_ARG_CHARS
            or _UNPRINTABLE.search(arg)
        ):
            raise ValueError(
                f"{field} entries must be 1 to {_MAX_ARG_CHARS} printable characters "
                "without backticks"
            )
    return tuple(raw)


def _install(raw: object) -> tuple[str, ...]:
    """Accept only a lockfile-pinned package manager install; ``ValueError`` otherwise."""

    install = _argv(raw, "install")
    form = _INSTALL_FORMS.get((install[0], install[1])) if len(install) >= 2 else None
    if form is None:
        raise ValueError("install must be uv sync, cargo fetch, pnpm install, or npm ci")
    required, flags, valued = form
    options = install[2:]
    for option in options:
        name, has_value, value = option.partition("=")
        allowed = (
            name in valued and bool(value) and _ARGUMENT.fullmatch(value) is not None
            if has_value
            else option in flags
        )
        if not allowed:
            raise ValueError(f"install option is not allowed for {install[0]} {install[1]}")
    if required and not required.intersection(options):
        raise ValueError(
            "install must be lockfile pinned: uv sync --frozen or --locked, cargo fetch "
            "--locked, pnpm install --frozen-lockfile, or npm ci"
        )
    return install


def stored_observation_bytes(record: dict[str, Any]) -> int:
    """UTF-8 size of the api's canonical stored note for one observation.

    The api's canonical note omits ``delegated_to`` when it is None.
    """

    stored = {
        key: record.get(key)
        for key in (
            "check",
            "command",
            "outcome",
            "exit_status",
            "missing_binaries",
            "blocked_services",
        )
    }
    if record.get("delegated_to") is not None:
        stored["delegated_to"] = record["delegated_to"]
    return len(json.dumps(stored, sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _worst_case_fits(
    check_id: str,
    command: tuple[str, ...],
    install: tuple[str, ...] | None,
    delegated_to: str | None,
) -> bool:
    """Whether an unavailable record with the longest single blocker fits the api."""

    blockers = list(KNOWN_BLOCKER_NAMES)
    blockers.extend(_binary_name(argv[0]) for argv in (command, install) if argv is not None)
    record: dict[str, Any] = {
        "check": check_id,
        "command": shlex.join(command),
        "outcome": "unavailable",
        "exit_status": None,
        "missing_binaries": [max(blockers, key=len)],
        "blocked_services": [],
    }
    if delegated_to is not None:
        record["delegated_to"] = delegated_to
    return stored_observation_bytes(record) <= MAX_STORED_OBSERVATION_BYTES


def _delegated_to(raw: object) -> str:
    """The required pull request CI check a check delegates to; never echoed."""

    if (
        not isinstance(raw, str)
        or not 1 <= len(raw) <= _MAX_DELEGATED_CHARS
        or raw != raw.strip()
        or _UNPRINTABLE.search(raw)
    ):
        raise ValueError(
            f"delegated_to must be 1 to {_MAX_DELEGATED_CHARS} printable characters "
            "without backticks or surrounding spaces"
        )
    return raw


def _check(raw: object) -> VerificationCheck:
    if (
        not isinstance(raw, dict)
        or not {"id", "paths", "command"} <= set(raw)
        or set(raw) - {"id", "paths", "command", "install", "delegated_to"}
    ):
        raise ValueError(
            "each check needs id, paths and command, and optional install and delegated_to"
        )
    check_id = raw["id"]
    if not isinstance(check_id, str) or not _CHECK_ID.fullmatch(check_id):
        raise ValueError("check id must match ^[a-z][a-z0-9_]{0,31}$")
    paths = raw["paths"]
    if not isinstance(paths, list) or not 1 <= len(paths) <= _MAX_PATHS:
        raise ValueError(f"paths must list 1 to {_MAX_PATHS} globs")
    for glob in paths:
        if (
            not isinstance(glob, str)
            or not 1 <= len(glob) <= _MAX_PATH_CHARS
            or _UNPRINTABLE.search(glob)
        ):
            raise ValueError(
                f"paths entries must be 1 to {_MAX_PATH_CHARS} printable characters "
                "without backticks"
            )
    command = _argv(raw["command"], "command")
    if len(shlex.join(command)) > _MAX_COMMAND_CHARS:
        raise ValueError(f"command must join to at most {_MAX_COMMAND_CHARS} characters")
    install: tuple[str, ...] | None = None
    if "install" in raw:
        install = _install(raw["install"])
    delegated_to: str | None = None
    if "delegated_to" in raw:
        delegated_to = _delegated_to(raw["delegated_to"])
    if not _worst_case_fits(check_id, command, install, delegated_to):
        raise ValueError(
            "check id and command are too long for an unavailable report of "
            f"{MAX_STORED_OBSERVATION_BYTES} bytes"
        )
    return VerificationCheck(
        id=check_id,
        paths=tuple(paths),
        command=command,
        install=install,
        delegated_to=delegated_to,
    )


def _read(path: Path) -> object:
    """Parse a declaration file; ``ValueError`` for anything but a small JSON file."""

    if not path.is_file():
        raise ValueError("is not a regular file")
    try:
        with path.open("rb") as handle:
            data = handle.read(_MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise ValueError(f"could not be read ({type(exc).__name__})") from exc
    if len(data) > _MAX_FILE_BYTES:
        raise ValueError(f"exceeds {_MAX_FILE_BYTES} bytes")
    try:
        return json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"is not valid JSON: {exc}") from exc


def _checks(raw: object, *, minimum: int) -> tuple[VerificationCheck, ...]:
    if not isinstance(raw, list) or not minimum <= len(raw) <= _MAX_CHECKS:
        raise ValueError(f"checks must list {minimum} to {_MAX_CHECKS} entries")
    checks = tuple(_check(entry) for entry in raw)
    ids = [check.id for check in checks]
    if len(set(ids)) != len(ids):
        raise ValueError("check ids must be unique")
    return checks


def _bundle(plugin_dir: Path) -> tuple[bool, tuple[VerificationCheck, ...]]:
    path = plugin_dir / BUNDLE_VERIFICATION_FILE
    if not path.exists():
        return False, ()
    try:
        raw = _read(path)
    except ValueError as exc:
        raise ValueError(f"{BUNDLE_VERIFICATION_FILE} {exc}") from exc
    if not isinstance(raw, dict) or set(raw) - {"lockfile_installs", "checks"}:
        raise ValueError(
            f"{BUNDLE_VERIFICATION_FILE} must be an object with checks and optional "
            "lockfile_installs"
        )
    lockfile_installs = raw.get("lockfile_installs", False)
    if not isinstance(lockfile_installs, bool):
        raise ValueError(f"{BUNDLE_VERIFICATION_FILE} lockfile_installs must be a boolean")
    try:
        checks = _checks(raw.get("checks", []), minimum=0)
    except ValueError as exc:
        raise ValueError(f"{BUNDLE_VERIFICATION_FILE}: {exc}") from exc
    return lockfile_installs, checks


def _repository(workspace: Path) -> tuple[VerificationCheck, ...]:
    raw = _read(workspace / REPOSITORY_VERIFICATION_FILE)
    if not isinstance(raw, dict) or set(raw) != {"checks"}:
        raise ValueError("must be an object with exactly checks")
    return _checks(raw["checks"], minimum=1)


def load_verification_declaration(plugin_dir: Path, workspace: Path) -> VerificationDeclaration:
    """Resolve the declared checks: bundle, then repository, then none.

    Raises ``ValueError`` for a malformed bundle file (operator error, as for
    ``phases.json``). A malformed repository file is repository content and must
    not crash boot: it resolves as not declared and is named in ``unreadable``.
    Validation messages never echo declaration values, since ``unreadable``
    reaches the system prompt.
    """

    lockfile_installs, bundle_checks = _bundle(plugin_dir)
    if bundle_checks:
        return VerificationDeclaration("bundle", lockfile_installs, None, bundle_checks)
    if not (workspace / REPOSITORY_VERIFICATION_FILE).exists():
        return VerificationDeclaration(None, lockfile_installs, None, ())
    try:
        repository_checks = _repository(workspace)
    except ValueError as exc:
        logger.warning("repository verification declaration is unreadable")
        return VerificationDeclaration(
            None, lockfile_installs, f"{REPOSITORY_VERIFICATION_FILE} {exc}", ()
        )
    return VerificationDeclaration("repository", lockfile_installs, None, repository_checks)


def _subprocess_text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def _binary_name(argv0: str) -> str:
    return Path(argv0).name[:_MAX_NAME_CHARS] or argv0[:_MAX_NAME_CHARS]


def _missing_binary_patterns(names: tuple[str, ...]) -> tuple[re.Pattern[str], ...]:
    alternation = "|".join(re.escape(name) for name in sorted(set(names), key=len, reverse=True))
    group = rf"({alternation})(?![\w.-])"
    return (
        re.compile(
            r"^(?:[^\n]{0,40}: )?(?:command not found|failed to spawn):?\s*"
            rf"['`\"]?{group}",
            re.IGNORECASE | re.MULTILINE,
        ),
        re.compile(
            rf"^(?:[^\n]{{0,40}}: )?{group}:?\s*(?:command not found|not found)\b",
            re.IGNORECASE | re.MULTILINE,
        ),
    )


def _failures(output: str, names: tuple[str, ...]) -> tuple[list[str], list[str]]:
    """Extract only known missing tools and named service failures from output."""

    missing = sorted(
        {
            match.group(1).lower()
            for pattern in _missing_binary_patterns(names)
            for match in pattern.finditer(output)
        }
    )
    blocked: set[str] = set()
    for line in output.splitlines():
        if not _SERVICE_FAILURE.search(line):
            continue
        lowered = line.casefold()
        for service, markers in _KNOWN_SERVICE_MARKERS.items():
            if any(marker in lowered for marker in markers):
                blocked.add(service)
    if _PACKAGE_REGISTRY_FAILURE.search(output):
        blocked.add(_PACKAGE_REGISTRY)
    return missing[:_MAX_REPORTED_NAMES], sorted(blocked)[:_MAX_REPORTED_NAMES]


@dataclass
class _Result:
    outcome: str
    exit_status: int | None = None
    missing: tuple[str, ...] = ()
    blocked: tuple[str, ...] = ()
    failure_reason: str | None = None


def _resolvable(argv0: str, workspace: Path) -> bool:
    # A relative path with a separator runs relative to the checkout.
    if os.sep in argv0 and not os.path.isabs(argv0):
        return shutil.which(str(workspace / argv0)) is not None
    return shutil.which(argv0) is not None


async def _run(
    argv: tuple[str, ...],
    workspace: Path,
    env: dict[str, str],
    names: tuple[str, ...],
    label: str,
) -> _Result:
    """Run one argv bounded in the checkout and classify what was observed."""

    if not _resolvable(argv[0], workspace):
        return _Result("unavailable", missing=(_binary_name(argv[0]),))
    try:
        result = await anyio.to_thread.run_sync(
            lambda: subprocess.run(
                list(argv),
                cwd=workspace,
                env=env,
                capture_output=True,
                text=True,
                timeout=_TIMEOUT_SECONDS,
                check=False,
            )
        )
    except subprocess.TimeoutExpired as exc:
        output = "\n".join((_subprocess_text(exc.stdout), _subprocess_text(exc.stderr)))
        missing, blocked = _failures(output, names)
        if missing or blocked:
            return _Result("unavailable", missing=tuple(missing), blocked=tuple(blocked))
        return _Result(
            "failed",
            exit_status=124,
            failure_reason=f"{label} timed out after {_TIMEOUT_SECONDS:g} seconds",
        )
    except OSError as exc:
        # ``which`` and process creation can race if the image is changing.
        # Do not expose arbitrary exception text in the progress record.
        logger.warning("verification %s could not start error_class=%s", label, type(exc).__name__)
        if isinstance(exc, FileNotFoundError):
            return _Result("unavailable", missing=(_binary_name(argv[0]),))
        return _Result(
            "failed",
            exit_status=126 if isinstance(exc, PermissionError) else 125,
            failure_reason=f"{label} could not start ({type(exc).__name__})",
        )
    if result.returncode == 0:
        return _Result("passed", exit_status=0)
    missing, blocked = _failures(f"{result.stdout}\n{result.stderr}", names)
    if missing or blocked:
        return _Result("unavailable", missing=tuple(missing), blocked=tuple(blocked))
    return _Result("failed", exit_status=result.returncode)


def _fit_stored_size(record: dict[str, Any]) -> dict[str, Any]:
    """Drop trailing blocker names until the api can store the record.

    ``blocked_services`` entries go first, then ``missing_binaries``, always
    keeping at least one blocker. Declaration validation guarantees a record
    with any single blocker fits.
    """

    fitted = {
        **record,
        "missing_binaries": list(record["missing_binaries"]),
        "blocked_services": list(record["blocked_services"]),
    }
    for key in ("blocked_services", "missing_binaries"):
        while (
            stored_observation_bytes(fitted) > MAX_STORED_OBSERVATION_BYTES
            and len(fitted["missing_binaries"]) + len(fitted["blocked_services"]) > 1
            and fitted[key]
        ):
            fitted[key].pop()
    return fitted


async def _post(client: ProgressClient, record: dict[str, Any]) -> int | None:
    status = await client.post(record.copy())
    if status != 201:
        logger.warning("verification preflight report was not accepted status=%s", status)
        raise RuntimeError("verification preflight report was not accepted")
    return status


async def _verify(
    check: VerificationCheck, workspace: Path, lockfile_installs: bool
) -> tuple[_Result, bool]:
    names = _KNOWN_BINARIES + tuple(
        _binary_name(argv[0]) for argv in (check.command, check.install) if argv is not None
    )
    installed = False
    if check.install is not None and lockfile_installs:
        # Registry egress for the install is the bundle's business; inherit env.
        install = await _run(
            check.install, workspace, shell_and_hook_env(os.environ), names, "install"
        )
        if install.outcome == "failed" and install.failure_reason is None:
            install.failure_reason = f"install exited {install.exit_status}"
        if install.outcome != "passed":
            return install, installed
        installed = True
    env = shell_and_hook_env(os.environ)
    env.update(_OFFLINE_ENV)
    return await _run(check.command, workspace, env, names, "command"), installed


async def preflight_workspace_verification(
    workspace: Path, plugin_dir: Path, url: str, token: str
) -> dict[str, Any]:
    """Run and report each declared check before the model starts.

    Checks run sequentially and each result is POSTed as it completes; any
    report not answered 201 raises ``RuntimeError`` (fail-closed boot). A
    result describes this execution only and does not certify later edits.
    Raises ``ValueError`` for a malformed bundle declaration.
    """

    declaration = load_verification_declaration(plugin_dir, workspace)
    client = ProgressClient(f"{url.rstrip('/')}/verification", token)
    summary: dict[str, Any] = {
        "source": declaration.source,
        "lockfile_installs": declaration.lockfile_installs,
        "unreadable": declaration.unreadable,
        "checks": [],
    }
    if not declaration.checks:
        summary["report_status"] = await _post(
            client,
            {
                "check": None,
                "command": None,
                "outcome": "not_declared",
                "exit_status": None,
                "missing_binaries": [],
                "blocked_services": [],
            },
        )
        return summary
    for check in declaration.checks:
        result, installed = await _verify(check, workspace, declaration.lockfile_installs)
        passed = result.outcome == "passed"
        observed: dict[str, Any] = {
            "check": check.id,
            "command": shlex.join(check.command),
            "outcome": result.outcome,
            "exit_status": result.exit_status,
            "missing_binaries": [] if passed else list(result.missing),
            "blocked_services": [] if passed else list(result.blocked),
        }
        # Sent only when declared, so an undelegated body is unchanged.
        if check.delegated_to is not None:
            observed["delegated_to"] = check.delegated_to
        record = _fit_stored_size(observed)
        status = await _post(client, record)
        entry: dict[str, Any] = {
            "id": check.id,
            "paths": list(check.paths),
            "command": record["command"],
            "install": shlex.join(check.install) if check.install is not None else None,
            "installed": installed,
            "outcome": record["outcome"],
            "exit_status": record["exit_status"],
            "missing_binaries": record["missing_binaries"],
            "blocked_services": record["blocked_services"],
            "report_status": status,
            "delegated_to": check.delegated_to,
        }
        if result.failure_reason is not None:
            entry["failure_reason"] = result.failure_reason
        summary["checks"].append(entry)
    return summary


def preflight_route(
    entry: dict[str, Any],
) -> Literal["executable", "delegated", "blocked"] | None:
    """The route of one summary entry: run here, delegated to CI, or blocked."""

    outcome = entry.get("outcome")
    if outcome in ("passed", "failed"):
        return "executable"
    if outcome == "unavailable":
        return "delegated" if entry.get("delegated_to") is not None else "blocked"
    return None


def blocked_checks(summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The blocked entries of a preflight summary, in declaration order."""

    if summary is None:
        return []
    return [entry for entry in summary["checks"] if preflight_route(entry) == "blocked"]
