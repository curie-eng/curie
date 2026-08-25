from __future__ import annotations

import re
import shlex
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "runner" / "Dockerfile"
_EXPORTER = _REPO_ROOT / "runner" / "export_dependency_pins.py"
_UV_LOCK = _REPO_ROOT / "uv.lock"
_REQUIREMENTS_PATH = "/tmp/runner-dependency-pins.txt"
_EXACT_NPM_VERSION = re.compile(
    r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?"
)
_NPM_PACKAGE = re.compile(r"(?:@[0-9A-Za-z._-]+/)?[0-9A-Za-z._-]+")
_PIP_EXECUTABLE = re.compile(r"pip(?:\d+(?:\.\d+)?)?$")
_NPM_INSTALL_ALIASES = {"install", "i", "add"}
_SYNTHETIC_RECURSIVE_LOCK = """\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "registry-second-level"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "registry-root" },
]

[[package]]
name = "curie-runner"
version = "0.0.0"
source = { editable = "runner" }
dependencies = [
    { name = "local-bridge" },
    { name = "registry-root" },
]

[[package]]
name = "registry-middle"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "registry-second-level" },
]

[[package]]
name = "local-bridge"
version = "0.0.0"
source = { editable = "packages/local-bridge" }
dependencies = [
    { name = "registry-middle" },
]

[[package]]
name = "registry-root"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "registry-middle" },
]
"""


@dataclass(frozen=True)
class Violation:
    package: str
    message: str


def _normalize_python_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _logical_instructions(dockerfile_text: str) -> list[str]:
    instructions: list[str] = []
    pending: list[str] = []
    for raw_line in dockerfile_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        continues = line.endswith("\\")
        pending.append(line[:-1].rstrip() if continues else line)
        if not continues:
            instructions.append(" ".join(pending))
            pending = []
    assert not pending, "Dockerfile ends with an unfinished continuation"
    return instructions


def _shell_tokens(instruction: str) -> list[str]:
    lexer = shlex.shlex(instruction, posix=True, punctuation_chars=";&|")
    lexer.commenters = ""
    lexer.whitespace_split = True
    return list(lexer)


def _locked_runner_dependencies(lock_text: str) -> dict[str, str]:
    lock = tomllib.loads(lock_text)
    packages = lock.get("package")
    assert isinstance(packages, list), "uv.lock must contain package records"
    assert all(isinstance(package, dict) for package in packages), (
        "uv.lock package records must be tables"
    )

    runner_records = [
        package
        for package in packages
        if _normalize_python_name(str(package.get("name", ""))) == "curie-runner"
    ]
    assert len(runner_records) == 1, "uv.lock must contain exactly one curie-runner record"

    dependencies = runner_records[0].get("dependencies")
    assert isinstance(dependencies, list), "curie-runner must declare dependencies in uv.lock"
    expected: dict[str, str] = {}
    for dependency in dependencies:
        assert isinstance(dependency, dict) and isinstance(dependency.get("name"), str), (
            "each curie-runner dependency must have a name"
        )
        name = _normalize_python_name(dependency["name"])
        resolved = [
            package
            for package in packages
            if _normalize_python_name(str(package.get("name", ""))) == name
        ]
        assert len(resolved) == 1, f"uv.lock must resolve {name} exactly once"
        source = resolved[0].get("source")
        assert isinstance(source, dict), f"uv.lock package {name} must declare its source"
        if "registry" not in source:
            continue
        version = resolved[0].get("version")
        assert isinstance(version, str) and version, (
            f"uv.lock registry package {name} must declare a version"
        )
        assert name not in expected, f"curie-runner dependency {name} is duplicated"
        expected[name] = version
    return expected


def _dockerfile_python_pins(instructions: list[str]) -> dict[str, list[str]]:
    pins: dict[str, list[str]] = {}
    controls = {"&&", "||", ";"}
    for instruction in instructions:
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if not _PIP_EXECUTABLE.fullmatch(token.rsplit("/", 1)[-1]) or (
                tokens[index + 1] != "install"
            ):
                continue
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                if operand.startswith("-") or "==" not in operand:
                    continue
                name, version = operand.split("==", 1)
                if name:
                    pins.setdefault(_normalize_python_name(name), []).append(version)
    return pins


def _dockerfile_pip_violations(instructions: list[str]) -> list[Violation]:
    violations: list[Violation] = []
    controls = {"&&", "||", ";"}
    for instruction in instructions:
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if not _PIP_EXECUTABLE.fullmatch(token.rsplit("/", 1)[-1]) or (
                tokens[index + 1] != "install"
            ):
                continue
            command: list[str] = []
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                command.append(operand)
            skip_next = False
            for operand in command:
                if skip_next:
                    skip_next = False
                    continue
                if operand in {"-r", "--requirement"}:
                    skip_next = True
                    continue
                if operand.startswith("-") or operand == "pip" or operand.startswith(
                    ("./", "../", "/")
                ):
                    continue
                if "==" not in operand:
                    violations.append(
                        Violation(operand, "pip install is missing an exact version")
                    )
    return violations


def _installs_generated_runner_requirements(instructions: list[str]) -> bool:
    controls = {"&&", "||", ";"}
    for instruction in instructions:
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if not _PIP_EXECUTABLE.fullmatch(token.rsplit("/", 1)[-1]) or (
                tokens[index + 1] != "install"
            ):
                continue
            command: list[str] = []
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                command.append(operand)
            if any(
                option in {"-r", "--requirement"} and requirement == _REQUIREMENTS_PATH
                for option, requirement in zip(command, command[1:], strict=False)
            ):
                return True
    return False


def _generated_runner_requirements_install_command(
    instructions: list[str],
) -> list[str]:
    controls = {"&&", "||", ";"}
    matches: list[list[str]] = []
    for instruction in instructions:
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if not _PIP_EXECUTABLE.fullmatch(token.rsplit("/", 1)[-1]) or (
                tokens[index + 1] != "install"
            ):
                continue
            command: list[str] = []
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                command.append(operand)
            if any(
                option in {"-r", "--requirement"}
                and requirement == _REQUIREMENTS_PATH
                for option, requirement in zip(command, command[1:], strict=False)
            ):
                matches.append(command)
    assert len(matches) == 1, "Dockerfile must install runner requirements once"
    return matches[0]


def _split_npm_operand(operand: str) -> tuple[str, str | None]:
    version_at = operand.rfind("@")
    if version_at <= 0:
        return operand, None
    return operand[:version_at], operand[version_at + 1 :]


def _npm_violations(instructions: list[str]) -> list[Violation]:
    violations: list[Violation] = []
    controls = {"&&", "||", ";"}
    for instruction in instructions:
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if token.rsplit("/", 1)[-1] != "npm" or (
                tokens[index + 1] not in _NPM_INSTALL_ALIASES
            ):
                continue
            command: list[str] = []
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                command.append(operand)
            if not ({"-g", "--global"} & set(command)):
                continue
            for operand in command:
                if operand.startswith("-"):
                    continue
                package, version = _split_npm_operand(operand)
                if not _NPM_PACKAGE.fullmatch(package) or not (
                    version and _EXACT_NPM_VERSION.fullmatch(version)
                ):
                    violations.append(
                        Violation(package, "global npm install is missing an exact version")
                    )
    return violations


def _dockerfile_global_npm_operands(dockerfile_text: str) -> list[str]:
    operands: list[str] = []
    controls = {"&&", "||", ";"}
    for instruction in _logical_instructions(dockerfile_text):
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if token.rsplit("/", 1)[-1] != "npm" or (
                tokens[index + 1] not in _NPM_INSTALL_ALIASES
            ):
                continue
            command: list[str] = []
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                command.append(operand)
            if {"-g", "--global"} & set(command):
                operands.extend(
                    operand for operand in command if not operand.startswith("-")
                )
    return operands


def _find_violations(lock_text: str, dockerfile_text: str) -> list[Violation]:
    expected = _locked_runner_dependencies(lock_text)
    instructions = _logical_instructions(dockerfile_text)
    pins = _dockerfile_python_pins(instructions)
    violations = _dockerfile_pip_violations(instructions) + _npm_violations(instructions)

    if _installs_generated_runner_requirements(instructions):
        pins = {package: [version] for package, version in expected.items()} | pins

    for package, versions in pins.items():
        if len(versions) > 1:
            violations.append(Violation(package, "duplicate exact Dockerfile pin"))

    for package in expected.keys() - pins.keys():
        violations.append(
            Violation(
                package,
                f"missing exact Dockerfile pin for lock version {expected[package]}",
            )
        )
    for package in pins.keys() - expected.keys():
        for version in pins[package]:
            violations.append(
                Violation(
                    package,
                    f"exact Dockerfile pin {version} is not a direct registry dependency",
                )
            )
    for package in expected.keys() & pins.keys():
        for version in set(pins[package]):
            if version != expected[package]:
                violations.append(
                    Violation(
                        package,
                        f"expected lock version {expected[package]}, "
                        f"found Dockerfile version {version}",
                    )
                )

    return sorted(violations, key=lambda violation: (violation.package, violation.message))


def _run_dependency_exporter(lock_text: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_EXPORTER)],
        input=lock_text,
        capture_output=True,
        check=False,
        text=True,
    )


def _export_runner_dependencies(lock_text: str) -> list[str]:
    result = _run_dependency_exporter(lock_text)
    assert result.returncode == 0, result.stderr
    return result.stdout.splitlines()


def _replace_locked_package_version(
    lock_text: str, package: str, replacement: str
) -> str:
    pattern = re.compile(
        rf'(?m)(^\[\[package\]\]\nname = "{re.escape(package)}"\nversion = ")[^"]+("$)'
    )
    updated, count = pattern.subn(
        lambda match: f"{match.group(1)}{replacement}{match.group(2)}",
        lock_text,
    )
    assert count == 1, f"uv.lock must contain exactly one {package} record"
    return updated


def _dockerfile_with_python_pins(pins: dict[str, str]) -> str:
    operands = " \\\n    ".join(f'"{package}=={version}"' for package, version in pins.items())
    return f"RUN pip install --no-cache-dir \\\n    {operands}\n"


def test_dependency_exporter_emits_sorted_complete_registry_closure() -> None:
    assert _export_runner_dependencies(_SYNTHETIC_RECURSIVE_LOCK) == [
        "registry-middle==2.0.0",
        "registry-root==1.0.0",
        "registry-second-level==3.0.0",
    ]


def test_dependency_exporter_propagates_markers_through_transitives() -> None:
    lock_text = """\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "curie-runner"
version = "0.0.0"
source = { editable = "runner" }
dependencies = [
    { name = "always" },
    { name = "windows-only", marker = "sys_platform == 'win32'" },
]

[[package]]
name = "always"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "windows-only"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "windows-child" },
]

[[package]]
name = "windows-child"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
"""

    assert _export_runner_dependencies(lock_text) == [
        "always==1.0.0",
        "windows-child==3.0.0 ; sys_platform == 'win32'",
        "windows-only==2.0.0 ; sys_platform == 'win32'",
    ]


def test_dependency_exporter_pins_a_shared_transitive_reached_both_ways() -> None:
    """A package reached under a marker and again unconditionally pins unconditionally.

    The unconditional path has to widen the recorded reachability of a package
    already recorded as conditional. The exporter runs inside the runner image
    build, so failing that widening takes the whole image build down rather than
    emitting a wrong pin.
    """
    lock_text = """\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "curie-runner"
version = "0.0.0"
source = { editable = "runner" }
dependencies = [
    { name = "windows-only", marker = "sys_platform == 'win32'" },
    { name = "everywhere" },
]

[[package]]
name = "windows-only"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "shared" },
]

[[package]]
name = "everywhere"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "shared" },
]

[[package]]
name = "shared"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
"""

    assert _export_runner_dependencies(lock_text) == [
        "everywhere==1.0.0",
        "shared==2.0.0",
        "windows-only==1.0.0 ; sys_platform == 'win32'",
    ]


@pytest.mark.parametrize(
    "marker_literal",
    [
        '""',
        r'"\r"',
        r'"\n"',
        '"sys_platform == \'win32\'; python_version > \'3.13\'"',
    ],
)
def test_dependency_exporter_rejects_malformed_markers(
    marker_literal: str,
) -> None:
    lock_text = f"""\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "curie-runner"
version = "0.0.0"
source = {{ editable = "runner" }}
dependencies = [
    {{ name = "conditional", marker = {marker_literal} }},
]

[[package]]
name = "conditional"
version = "1.0.0"
source = {{ registry = "https://pypi.org/simple" }}
"""

    result = _run_dependency_exporter(lock_text)

    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


def test_dependency_exporter_follows_selected_extra_dependency_closure() -> None:
    lock_text = """\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "curie-runner"
version = "0.0.0"
source = { editable = "runner" }
dependencies = [
    { name = "provider", extra = ["crypto", "crypto"] },
]

[[package]]
name = "provider"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }

[package.optional-dependencies]
crypto = [
    { name = "extra-child" },
]

[[package]]
name = "extra-child"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "extra-leaf" },
]

[[package]]
name = "extra-leaf"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
optional-dependencies = "unused malformed table"
"""

    assert _export_runner_dependencies(lock_text) == [
        "extra-child==2.0.0",
        "extra-leaf==3.0.0",
        "provider==1.0.0",
    ]


def test_dependency_exporter_rejects_a_missing_referenced_transitive() -> None:
    second_level_record = """\
[[package]]
name = "registry-second-level"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "registry-root" },
]

"""
    assert second_level_record in _SYNTHETIC_RECURSIVE_LOCK
    incomplete_lock = _SYNTHETIC_RECURSIVE_LOCK.replace(second_level_record, "")

    result = _run_dependency_exporter(incomplete_lock)

    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


def test_dependency_exporter_reflects_a_lock_version_bump() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    replacement = f"{expected['claude-agent-sdk']}.post1"
    bumped_lock = _replace_locked_package_version(
        lock_text, "claude-agent-sdk", replacement
    )

    pins = _export_runner_dependencies(bumped_lock)

    assert "claude-agent-sdk==" + replacement in pins
    assert f"claude-agent-sdk=={expected['claude-agent-sdk']}" not in pins


def test_dependency_exporter_rejects_malformed_lock_input() -> None:
    result = _run_dependency_exporter("[[package]\nname = ")

    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


def test_dockerfile_generates_python_requirements_from_the_lock_exporter() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    instructions = _logical_instructions(dockerfile)

    assert "COPY uv.lock ./uv.lock" in instructions
    assert any(
        "python3 runner/export_dependency_pins.py < uv.lock "
        f"> {_REQUIREMENTS_PATH}" in instruction
        for instruction in instructions
    )
    assert "--no-deps" in _generated_runner_requirements_install_command(instructions)
    assert _dockerfile_python_pins(instructions) == {}


def test_runner_image_installs_the_shared_telemetry_workspace_dependency() -> None:
    instructions = _logical_instructions(_DOCKERFILE.read_text(encoding="utf-8"))

    assert "COPY packages/telemetry ./packages/telemetry" in instructions
    assert any(
        instruction.startswith("RUN python3 -m venv ")
        and "pip install --no-cache-dir --no-deps" in instruction
        and "./packages/telemetry" in instruction
        for instruction in instructions
    )


def test_actual_runner_dockerfile_matches_locked_dependencies() -> None:
    assert _find_violations(
        _UV_LOCK.read_text(encoding="utf-8"),
        _DOCKERFILE.read_text(encoding="utf-8"),
    ) == []


def test_actual_runner_dockerfile_rejects_unpinned_pip_requirements() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    mutated = (
        dockerfile
        + "\nRUN /app/.venv/bin/pip install --no-cache-dir --upgrade "
        "claude-agent-sdk aiohttp\n"
    )

    assert Violation(
        "claude-agent-sdk", "pip install is missing an exact version"
    ) in _find_violations(_UV_LOCK.read_text(encoding="utf-8"), mutated)


def test_prechange_drift_reports_all_five_violations() -> None:
    dockerfile = """\
RUN npm install -g @anthropic-ai/claude-code
RUN npm install -g @modelcontextprotocol/server-github
RUN /app/.venv/bin/pip install \\
    "claude-agent-sdk==0.2.115" \\
    "aiohttp==3.14.1" \\
    "opentelemetry-sdk==1.44.0" \\
    "opentelemetry-exporter-otlp-proto-http==1.44.0" \\
    "anyio==4.14.1"
"""
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    violations = _find_violations(lock_text, dockerfile)
    assert violations == [
        Violation(
            "@anthropic-ai/claude-code",
            "global npm install is missing an exact version",
        ),
        Violation(
            "@modelcontextprotocol/server-github",
            "global npm install is missing an exact version",
        ),
        Violation(
            "aiohttp",
            "expected lock version "
            f"{expected['aiohttp']}, found Dockerfile version 3.14.1",
        ),
        Violation(
            "claude-agent-sdk",
            "expected lock version "
            f"{expected['claude-agent-sdk']}, found Dockerfile version 0.2.115",
        ),
        Violation(
            "opentelemetry-exporter-otlp-proto-http",
            "exact Dockerfile pin 1.44.0 is not a direct registry dependency",
        ),
    ]


def test_python_pin_revert_is_rejected() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    stale_version = f"{expected['claude-agent-sdk']}.stale"
    pins = expected | {"claude-agent-sdk": stale_version}
    mutated = _dockerfile_with_python_pins(pins)

    violations = _find_violations(lock_text, mutated)
    assert Violation(
        "claude-agent-sdk",
        "expected lock version "
        f"{expected['claude-agent-sdk']}, found Dockerfile version {stale_version}",
    ) in violations


def test_runner_image_uses_bundled_claude_cli_and_keeps_pinned_github_mcp() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    npm_operands = _dockerfile_global_npm_operands(dockerfile)
    assert all(
        _split_npm_operand(operand)[0] != "@anthropic-ai/claude-code"
        for operand in npm_operands
    )
    github_operands = [
        operand
        for operand in npm_operands
        if _split_npm_operand(operand)[0]
        == "@modelcontextprotocol/server-github"
    ]
    assert len(github_operands) == 1
    current = github_operands[0]
    package, version = _split_npm_operand(current)
    assert package == "@modelcontextprotocol/server-github"
    assert version and _EXACT_NPM_VERSION.fullmatch(version)
    assert dockerfile.count(current) == 1
    mutated = dockerfile.replace(current, package)

    violations = _find_violations(_UV_LOCK.read_text(encoding="utf-8"), mutated)
    assert Violation(
        package, "global npm install is missing an exact version"
    ) in violations


@pytest.mark.parametrize("command", sorted(_NPM_INSTALL_ALIASES))
def test_npm_global_install_aliases_require_exact_versions(command: str) -> None:
    violations = _find_violations(
        _UV_LOCK.read_text(encoding="utf-8"),
        f"RUN npm {command} -g @example/tool\n",
    )
    assert Violation(
        "@example/tool", "global npm install is missing an exact version"
    ) in violations


@pytest.mark.parametrize("executable", ["pip", "pip3"])
def test_pip_executable_forms_are_compared_with_the_lock(executable: str) -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    operands = {
        package: f"{package}=={version}"
        for package, version in _locked_runner_dependencies(lock_text).items()
    }
    synthetic = "RUN " + executable + " install " + " ".join(
        f'"{operand}"' for operand in operands.values()
    )

    assert _find_violations(lock_text, synthetic) == []


def test_python_pin_set_must_match_all_direct_registry_dependencies() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    operands = {
        package: f"{package}=={version}" for package, version in expected.items()
    }
    baseline = "RUN pip install " + " ".join(
        f'"{operand}"' for operand in operands.values()
    )
    missing = baseline.replace(f' "{operands["anyio"]}"', "")
    extra = baseline + ' "httpx==1.0.0"'
    duplicate = baseline + f' "{operands["aiohttp"]}"'

    assert Violation(
        "anyio", f"missing exact Dockerfile pin for lock version {expected['anyio']}"
    ) in _find_violations(lock_text, missing)
    assert Violation(
        "httpx", "exact Dockerfile pin 1.0.0 is not a direct registry dependency"
    ) in _find_violations(lock_text, extra)
    assert Violation("aiohttp", "duplicate exact Dockerfile pin") in _find_violations(
        lock_text, duplicate
    )
