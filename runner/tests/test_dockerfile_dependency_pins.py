from __future__ import annotations

import re
import shlex
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

import pytest
from runner_dockerfile_support import logical_instructions

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "runner" / "Dockerfile"
_EXPORTER = _REPO_ROOT / "runner" / "export_dependency_pins.py"
_UV_LOCK = _REPO_ROOT / "uv.lock"
_REQUIREMENTS_PATH = "/tmp/runner-dependency-pins.txt"
_EXACT_NPM_VERSION = re.compile(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?")
_NPM_PACKAGE = re.compile(r"(?:@[0-9A-Za-z._-]+/)?[0-9A-Za-z._-]+")
_PIP_EXECUTABLE = re.compile(r"pip(?:\d+(?:\.\d+)?)?$")
_PYTHON_EXECUTABLE = re.compile(r"python(?:\d+(?:\.\d+)?)?$")
_BOOTSTRAP_REQUIREMENTS_PATH = "runner/pip-requirements.txt"
_AUDITED_REQUIREMENT_PATHS = frozenset({_BOOTSTRAP_REQUIREMENTS_PATH, _REQUIREMENTS_PATH})
_PIP_REQUIREMENT_OPTION = re.compile(r"--requirement(?:=(?P<long>.*))?|-r(?P<short>.*)")
_PIP_SHORT_VERBOSITY = re.compile(r"-[qv]+$")
_PIP_NON_INSTALL_FLAGS = frozenset({"-h", "--help", "-V", "--version"})
_PIP_GLOBAL_FLAG_OPTIONS = frozenset(
    {
        "--debug",
        "--disable-pip-version-check",
        "--isolated",
        "--no-cache-dir",
        "--no-color",
        "--no-input",
        "--no-progress",
        "--quiet",
        "--require-hashes",
        "--require-virtualenv",
        "--verbose",
    }
)
_PIP_GLOBAL_VALUE_OPTIONS = frozenset(
    {
        "-i",
        "--cache-dir",
        "--cert",
        "--client-cert",
        "--default-timeout",
        "--exists-action",
        "--extra-index-url",
        "--index-url",
        "--keyring-provider",
        "--log",
        "--progress-bar",
        "--proxy",
        "--python",
        "--resume-retries",
        "--retries",
        "--root",
        "--root-user-action",
        "--timeout",
        "--trusted-host",
        "--use-deprecated",
        "--use-feature",
        "--upgrade-strategy",
    }
)
_UNSUPPORTED_PIP_GLOBAL_OPTION = "unsupported pip global option"
_MISSING_PIP_OPTION_VALUE = "pip global option is missing a value"
_UNAUDITED_REQUIREMENTS_FILE = "pip requirements file is not audited"
_MISSING_REQUIREMENTS_ARGUMENT = "pip requirement option is missing an argument"
_NPM_INSTALL_ALIASES = {"install", "i", "add"}
_HASH = " --hash=sha256:" + "1" * 64
_SYNTHETIC_RECURSIVE_LOCK = """\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "registry-second-level"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
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
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
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
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
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


@dataclass(frozen=True)
class _PipInvocation:
    """One normalized `pip install` command found in a Dockerfile RUN instruction."""

    operands: tuple[str, ...]
    requirements: tuple[str, ...]
    violations: tuple[Violation, ...]


def _pip_invocation_start(tokens: list[str], index: int) -> int | None:
    """Return the index after a recognized pip invocation, or None."""
    basename = tokens[index].rsplit("/", 1)[-1]
    if _PIP_EXECUTABLE.fullmatch(basename):
        return index + 1
    if (
        _PYTHON_EXECUTABLE.fullmatch(basename)
        and tokens[index + 1 : index + 2] == ["-m"]
        and tokens[index + 2 : index + 3] == ["pip"]
    ):
        return index + 3
    if basename == "uv" and tokens[index + 1 : index + 2] == ["pip"]:
        return index + 2
    return None


def _shell_segments(instruction: str) -> list[list[str]]:
    """Split an instruction into shell command segments at unquoted operators.

    A quoted or escaped `&&`, `||` or `;` is an option value, not a boundary.
    """
    raw_segments: list[str] = []
    buffer: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(instruction):
        char = instruction[index]
        if quote is not None:
            buffer.append(char)
            if char == "\\" and quote == '"' and index + 1 < len(instruction):
                buffer.append(instruction[index + 1])
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            buffer.append(char)
            index += 1
            continue
        if char == "\\" and index + 1 < len(instruction):
            buffer.append(char)
            buffer.append(instruction[index + 1])
            index += 2
            continue
        if instruction.startswith("&&", index) or instruction.startswith("||", index):
            raw_segments.append("".join(buffer))
            buffer = []
            index += 2
            continue
        if char == ";":
            raw_segments.append("".join(buffer))
            buffer = []
            index += 1
            continue
        buffer.append(char)
        index += 1
    raw_segments.append("".join(buffer))
    return [_shell_tokens(segment) for segment in raw_segments if segment.strip()]


def _pip_install_at(
    tokens: list[str], start: int
) -> tuple[int | None, list[Violation]]:
    """Find the `install` subcommand after consuming global options by arity."""
    violations: list[Violation] = []
    unsupported_value = False
    cursor = start
    while cursor < len(tokens):
        token = tokens[cursor]
        if token in _PIP_NON_INSTALL_FLAGS:
            return None, []
        if token in _PIP_GLOBAL_FLAG_OPTIONS or _PIP_SHORT_VERBOSITY.fullmatch(token):
            cursor += 1
            continue
        if token in _PIP_GLOBAL_VALUE_OPTIONS:
            if cursor + 1 >= len(tokens):
                return None, []
            cursor += 2
            continue
        name, separator, value = token.partition("=")
        if separator and name in _PIP_GLOBAL_VALUE_OPTIONS:
            if not value:
                violations.append(Violation(token, _MISSING_PIP_OPTION_VALUE))
            cursor += 1
            continue
        if token == "install":
            return cursor, violations
        if token.startswith("-"):
            violations.append(Violation(token, _UNSUPPORTED_PIP_GLOBAL_OPTION))
            unsupported_value = True
            cursor += 1
            continue
        if unsupported_value:
            # An unsupported option may take a separate value; skip it so the
            # subcommand is still found and the install is still validated.
            unsupported_value = False
            cursor += 1
            continue
        return None, []
    return None, []


def _pip_requirement_operands(
    operands: list[str],
) -> tuple[list[str], list[Violation]]:
    """Collect requirement files in every spelling and audit their literal paths."""
    requirements: list[str] = []
    violations: list[Violation] = []
    cursor = 0
    while cursor < len(operands):
        option = operands[cursor]
        match = _PIP_REQUIREMENT_OPTION.fullmatch(option)
        if match is None:
            cursor += 1
            continue
        path = match.group("long")
        if path is None:
            path = match.group("short") or None
        if (
            path is None
            and cursor + 1 < len(operands)
            and not operands[cursor + 1].startswith("-")
        ):
            path = operands[cursor + 1]
            cursor += 1
        cursor += 1
        if not path:
            violations.append(Violation(option, _MISSING_REQUIREMENTS_ARGUMENT))
            continue
        requirements.append(path)
        if path not in _AUDITED_REQUIREMENT_PATHS:
            violations.append(Violation(path, _UNAUDITED_REQUIREMENTS_FILE))
    return requirements, violations


def _pip_invocations(tokens: list[str]) -> list[_PipInvocation]:
    """Normalize the `pip install` in one shell segment's tokens.

    Invocations without an `install` subcommand (`pip check`, `pip --version`,
    `pip help install`, `uv pip list`, ...) are not installs and stay outside
    the gate.
    """
    invocations: list[_PipInvocation] = []
    for index in range(len(tokens)):
        start = _pip_invocation_start(tokens, index)
        if start is None:
            continue
        install_at, option_violations = _pip_install_at(tokens, start)
        if install_at is None:
            continue
        operands = tokens[install_at + 1 :]
        requirements, operand_violations = _pip_requirement_operands(operands)
        invocations.append(
            _PipInvocation(
                operands=tuple(operands),
                requirements=tuple(requirements),
                violations=tuple(option_violations + operand_violations),
            )
        )
        break
    return invocations


def _pip_install_operands(invocation: _PipInvocation) -> list[str]:
    """Package operands only: options and requirement-file arguments are removed."""
    requirements = set(invocation.requirements)
    return [
        operand
        for operand in invocation.operands
        if not operand.startswith("-") and operand not in requirements
    ]


def _dockerfile_pip_installs(instructions: list[str]) -> list[_PipInvocation]:
    invocations: list[_PipInvocation] = []
    for instruction in instructions:
        for position, segment in enumerate(_shell_segments(instruction)):
            if position == 0:
                if not segment or segment[0].upper() != "RUN":
                    break
                segment = segment[1:]
            invocations.extend(_pip_invocations(segment))
    return invocations


def _dockerfile_python_pins(instructions: list[str]) -> dict[str, list[str]]:
    pins: dict[str, list[str]] = {}
    for invocation in _dockerfile_pip_installs(instructions):
        for operand in _pip_install_operands(invocation):
            if "==" not in operand:
                continue
            name, version = operand.split("==", 1)
            if name:
                pins.setdefault(_normalize_python_name(name), []).append(version)
    return pins


def _dockerfile_pip_violations(instructions: list[str]) -> list[Violation]:
    violations: list[Violation] = []
    for invocation in _dockerfile_pip_installs(instructions):
        violations.extend(invocation.violations)
        for operand in _pip_install_operands(invocation):
            if operand == "pip" or operand.startswith(("./", "../", "/")):
                continue
            if "==" not in operand:
                violations.append(Violation(operand, "pip install is missing an exact version"))
    return violations


def _installs_generated_runner_requirements(instructions: list[str]) -> bool:
    return any(
        _REQUIREMENTS_PATH in invocation.requirements
        for invocation in _dockerfile_pip_installs(instructions)
    )


def _generated_runner_requirements_install_command(
    instructions: list[str],
) -> list[str]:
    matches = [
        list(invocation.operands)
        for invocation in _dockerfile_pip_installs(instructions)
        if _REQUIREMENTS_PATH in invocation.requirements
    ]
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
            if token.rsplit("/", 1)[-1] != "npm" or (tokens[index + 1] not in _NPM_INSTALL_ALIASES):
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
    for instruction in logical_instructions(dockerfile_text):
        tokens = _shell_tokens(instruction)
        if not tokens or tokens[0].upper() != "RUN":
            continue
        for index, token in enumerate(tokens[:-1]):
            if token.rsplit("/", 1)[-1] != "npm" or (tokens[index + 1] not in _NPM_INSTALL_ALIASES):
                continue
            command: list[str] = []
            for operand in tokens[index + 2 :]:
                if operand in controls:
                    break
                command.append(operand)
            if {"-g", "--global"} & set(command):
                operands.extend(operand for operand in command if not operand.startswith("-"))
    return operands


def _find_violations(lock_text: str, dockerfile_text: str) -> list[Violation]:
    expected = _locked_runner_dependencies(lock_text)
    instructions = logical_instructions(dockerfile_text)
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


def _replace_locked_package_version(lock_text: str, package: str, replacement: str) -> str:
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
        f"registry-middle==2.0.0{_HASH}",
        f"registry-root==1.0.0{_HASH}",
        f"registry-second-level==3.0.0{_HASH}",
    ]


_EXPORTER_LOCK_HEADER = """\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "curie-runner"
version = "0.0.0"
source = { editable = "runner" }
dependencies = [
"""


@pytest.mark.parametrize(
    ("root_and_packages", "expected"),
    [
        pytest.param(
            """\
    { name = "always" },
    { name = "windows-only", marker = "sys_platform == 'win32'" },
]

[[package]]
name = "always"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }

[[package]]
name = "windows-only"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
dependencies = [
    { name = "windows-child" },
]

[[package]]
name = "windows-child"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
""",
            [
                f"always==1.0.0{_HASH}",
                f"windows-child==3.0.0 ; sys_platform == 'win32'{_HASH}",
                f"windows-only==2.0.0 ; sys_platform == 'win32'{_HASH}",
            ],
            id="markers-propagate-through-transitives",
        ),
        # A package reached under a marker and again unconditionally pins
        # unconditionally. The unconditional path has to widen the recorded
        # reachability of a package already recorded as conditional; the exporter
        # runs inside the runner image build, so failing that widening takes the
        # whole image build down rather than emitting a wrong pin.
        pytest.param(
            """\
    { name = "windows-only", marker = "sys_platform == 'win32'" },
    { name = "everywhere" },
]

[[package]]
name = "windows-only"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
dependencies = [
    { name = "shared" },
]

[[package]]
name = "everywhere"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
dependencies = [
    { name = "shared" },
]

[[package]]
name = "shared"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
""",
            [
                f"everywhere==1.0.0{_HASH}",
                f"shared==2.0.0{_HASH}",
                f"windows-only==1.0.0 ; sys_platform == 'win32'{_HASH}",
            ],
            id="shared-transitive-pins-unconditionally",
        ),
        pytest.param(
            """\
    { name = "provider", extra = ["crypto", "crypto"] },
]

[[package]]
name = "provider"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }

[package.optional-dependencies]
crypto = [
    { name = "extra-child" },
]

[[package]]
name = "extra-child"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
dependencies = [
    { name = "extra-leaf" },
]

[[package]]
name = "extra-leaf"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
optional-dependencies = "unused malformed table"
""",
            [
                f"extra-child==2.0.0{_HASH}",
                f"extra-leaf==3.0.0{_HASH}",
                f"provider==1.0.0{_HASH}",
            ],
            id="selected-extra-closure",
        ),
    ],
)
def test_dependency_exporter_pins_the_reachable_closure(
    root_and_packages: str, expected: list[str]
) -> None:
    assert _export_runner_dependencies(_EXPORTER_LOCK_HEADER + root_and_packages) == expected


def test_dependency_exporter_rejects_a_missing_referenced_transitive() -> None:
    second_level_record = """\
[[package]]
name = "registry-second-level"
version = "3.0.0"
source = { registry = "https://pypi.org/simple" }
sdist = { url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }
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
    bumped_lock = _replace_locked_package_version(lock_text, "claude-agent-sdk", replacement)

    pins = _export_runner_dependencies(bumped_lock)

    assert any(pin.startswith(f"claude-agent-sdk=={replacement} ") for pin in pins)
    assert not any(
        pin.startswith(f"claude-agent-sdk=={expected['claude-agent-sdk']} ") for pin in pins
    )


@pytest.mark.parametrize(
    "marker_literal",
    [
        '""',
        r'"\r"',
        r'"\n"',
        "\"sys_platform == 'win32'; python_version > '3.13'\"",
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
sdist = {{ url = "https://example.invalid/pkg.tar.gz", hash = "sha256:\
1111111111111111111111111111111111111111111111111111111111111111" }}
"""

    result = _run_dependency_exporter(lock_text)

    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


def test_dependency_exporter_rejects_malformed_lock_input() -> None:
    result = _run_dependency_exporter("[[package]\nname = ")

    assert result.returncode != 0
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


def test_dockerfile_generates_python_requirements_from_the_lock_exporter() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    instructions = logical_instructions(dockerfile)

    assert "COPY uv.lock ./uv.lock" in instructions
    assert any(
        f"python3 runner/export_dependency_pins.py < uv.lock > {_REQUIREMENTS_PATH}" in instruction
        for instruction in instructions
    )
    assert "--no-deps" in _generated_runner_requirements_install_command(instructions)
    assert _dockerfile_python_pins(instructions) == {}


def test_runner_image_installs_the_shared_telemetry_workspace_dependency() -> None:
    instructions = logical_instructions(_DOCKERFILE.read_text(encoding="utf-8"))

    assert "COPY packages/telemetry ./packages/telemetry" in instructions
    assert any(
        "pip install --no-cache-dir --no-deps" in instruction
        and "./packages/telemetry" in instruction
        for instruction in instructions
    )


def test_actual_runner_dockerfile_matches_locked_dependencies() -> None:
    assert (
        _find_violations(
            _UV_LOCK.read_text(encoding="utf-8"),
            _DOCKERFILE.read_text(encoding="utf-8"),
        )
        == []
    )


def test_actual_runner_dockerfile_rejects_unpinned_pip_requirements() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    mutated = (
        dockerfile + "\nRUN /app/.venv/bin/pip install --no-cache-dir --upgrade "
        "claude-agent-sdk aiohttp\n"
    )

    assert Violation(
        "claude-agent-sdk", "pip install is missing an exact version"
    ) in _find_violations(_UV_LOCK.read_text(encoding="utf-8"), mutated)


def test_prechange_drift_reports_all_six_violations() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    # The opentelemetry-sdk pin tracks the lock so it stays the one matching pin.
    dockerfile = f"""\
RUN npm install -g @anthropic-ai/claude-code
RUN npm install -g @modelcontextprotocol/server-github
RUN /app/.venv/bin/pip install \\
    "claude-agent-sdk==0.2.115" \\
    "aiohttp==3.14.1" \\
    "opentelemetry-sdk=={expected['opentelemetry-sdk']}" \\
    "opentelemetry-exporter-otlp-proto-http==1.44.0" \\
    "anyio==4.14.1"
"""
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
            f"expected lock version {expected['aiohttp']}, found Dockerfile version 3.14.1",
        ),
        Violation(
            "anyio",
            f"expected lock version {expected['anyio']}, found Dockerfile version 4.14.1",
        ),
        Violation(
            "claude-agent-sdk",
            "expected lock version "
            f"{expected['claude-agent-sdk']}, found Dockerfile version 0.2.115",
        ),
        Violation(
            "mcp",
            f"missing exact Dockerfile pin for lock version {expected['mcp']}",
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
    assert (
        Violation(
            "claude-agent-sdk",
            "expected lock version "
            f"{expected['claude-agent-sdk']}, found Dockerfile version {stale_version}",
        )
        in violations
    )


def test_runner_image_uses_bundled_claude_cli_and_does_not_bless_bundle_mcp() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    assert _dockerfile_global_npm_operands(dockerfile) == []
    assert "@modelcontextprotocol/server-github" not in dockerfile
    assert "@zencoderai/slack-mcp-server" not in dockerfile
    assert "bless another authed third-party MCP server" not in dockerfile
    assert "COPY --from=node /usr/local/bin/node" in dockerfile


def test_bundle_layers_pin_the_mcp_servers_they_moved() -> None:
    github_issues = (
        _REPO_ROOT / "examples" / "github-issues" / "runner.Dockerfile"
    ).read_text(encoding="utf-8")
    dark_factory = (
        _REPO_ROOT / "examples" / "dark-factory" / "runner.Dockerfile"
    ).read_text(encoding="utf-8")
    mean_tester = (
        _REPO_ROOT / "examples" / "mean-tester" / "runner.Dockerfile"
    ).read_text(encoding="utf-8")
    for text in (github_issues, dark_factory, mean_tester):
        assert _npm_violations(logical_instructions(text)) == []
    assert _dockerfile_global_npm_operands(github_issues) == [
        "@modelcontextprotocol/server-github@2025.4.8"
    ]
    # The factory reads its issue through the platform (ADR 0187): no MCP server.
    assert _dockerfile_global_npm_operands(dark_factory) == ["pnpm@9.15.9"]
    assert _dockerfile_global_npm_operands(mean_tester) == [
        "@zencoderai/slack-mcp-server@0.0.1",
        "@modelcontextprotocol/server-github@2025.4.8",
    ]
    unpinned = github_issues.replace(
        "@modelcontextprotocol/server-github@2025.4.8",
        "@modelcontextprotocol/server-github",
    )
    assert (
        Violation(
            "@modelcontextprotocol/server-github",
            "global npm install is missing an exact version",
        )
        in _npm_violations(logical_instructions(unpinned))
    )


@pytest.mark.parametrize("command", sorted(_NPM_INSTALL_ALIASES))
def test_npm_global_install_aliases_require_exact_versions(command: str) -> None:
    violations = _find_violations(
        _UV_LOCK.read_text(encoding="utf-8"),
        f"RUN npm {command} -g @example/tool\n",
    )
    assert (
        Violation("@example/tool", "global npm install is missing an exact version") in violations
    )


@pytest.mark.parametrize("executable", ["pip", "pip3"])
def test_pip_executable_forms_are_compared_with_the_lock(executable: str) -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    operands = {
        package: f"{package}=={version}"
        for package, version in _locked_runner_dependencies(lock_text).items()
    }
    synthetic = (
        "RUN "
        + executable
        + " install "
        + " ".join(f'"{operand}"' for operand in operands.values())
    )

    assert _find_violations(lock_text, synthetic) == []


def test_python_pin_set_must_match_all_direct_registry_dependencies() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    operands = {package: f"{package}=={version}" for package, version in expected.items()}
    baseline = "RUN pip install " + " ".join(f'"{operand}"' for operand in operands.values())
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


_PIP_INVOCATION_FAMILIES = [
    pytest.param("pip", ["--disable-pip-version-check"], id="pip"),
    pytest.param("pip3", ["--quiet"], id="pip3"),
    pytest.param("python3", ["-m", "pip", "--isolated"], id="python-m-pip"),
    pytest.param("uv", ["pip", "--no-progress"], id="uv-pip"),
]


@pytest.mark.parametrize(("command", "global_options"), _PIP_INVOCATION_FAMILIES)
def test_pip_global_options_preserve_pin_validation(
    command: str, global_options: list[str]
) -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    expected = _locked_runner_dependencies(lock_text)
    prefix = " ".join([command, *global_options])
    pinned = " ".join(f'"{package}=={version}"' for package, version in expected.items())

    # The unchanged Dockerfile stays the positive control.
    assert _find_violations(lock_text, _DOCKERFILE.read_text(encoding="utf-8")) == []

    # Valid pinned operands are still accepted behind documented global options.
    assert _find_violations(lock_text, f"RUN {prefix} install {pinned}\n") == []
    for verbosity in ("-q", "-qq", "-v", "-vvv"):
        assert (
            _find_violations(lock_text, f"RUN {prefix} {verbosity} install {pinned}\n")
            == []
        )

    # Unpinned registry operands are still rejected behind those same options.
    unpinned = _find_violations(lock_text, f"RUN {prefix} install claude-agent-sdk aiohttp\n")
    assert Violation(
        "claude-agent-sdk", "pip install is missing an exact version"
    ) in unpinned
    assert Violation("aiohttp", "pip install is missing an exact version") in unpinned

    # A wrong pin is still compared with the lock.
    stale = f"{expected['aiohttp']}.stale"
    drift = _find_violations(lock_text, f'RUN {prefix} install "aiohttp=={stale}"\n')
    assert (
        Violation(
            "aiohttp",
            f"expected lock version {expected['aiohttp']}, found Dockerfile version {stale}",
        )
        in drift
    )

    # Unknown or malformed global options fail closed instead of hiding the install.
    unknown = _find_violations(
        lock_text, f"RUN {prefix} --frobnicate install claude-agent-sdk\n"
    )
    assert Violation("--frobnicate", "unsupported pip global option") in unknown
    assert Violation(
        "claude-agent-sdk", "pip install is missing an exact version"
    ) in unknown
    missing_value = _find_violations(
        lock_text, f"RUN {prefix} --timeout= install claude-agent-sdk\n"
    )
    assert (
        Violation("--timeout=", "pip global option is missing a value") in missing_value
    )

    if command == "pip":
        # A global option value is consumed by arity, even when it spells `install`.
        arity = (
            _DOCKERFILE.read_text(encoding="utf-8")
            + f"\nRUN {prefix} --log install install -r {_BOOTSTRAP_REQUIREMENTS_PATH}\n"
            + f"RUN {prefix} --log=install install -r {_BOOTSTRAP_REQUIREMENTS_PATH}\n"
        )
        assert _find_violations(lock_text, arity) == []

        # Supported value-option aliases are consumed by arity.
        for options in (
            "--resume-retries 5",
            "--default-timeout 15",
            "--retries 3",
            "--timeout 30",
        ):
            assert (
                _find_violations(lock_text, f"RUN pip {options} install {pinned}\n")
                == []
            )
        resume = _find_violations(lock_text, "RUN pip --resume-retries 5 install aiohttp\n")
        assert Violation("aiohttp", "pip install is missing an exact version") in resume

        # An unsupported option's separate value cannot hide the install.
        unknown_value = _find_violations(
            lock_text, "RUN pip --frobnicate 5 install claude-agent-sdk\n"
        )
        assert (
            Violation("--frobnicate", "unsupported pip global option") in unknown_value
        )
        assert Violation(
            "claude-agent-sdk", "pip install is missing an exact version"
        ) in unknown_value

        # A quoted operator is an option value, not a command boundary.
        quoted = _find_violations(
            lock_text, f"RUN {prefix} --log ';' install claude-agent-sdk\n"
        )
        assert Violation(
            "claude-agent-sdk", "pip install is missing an exact version"
        ) in quoted

    # Benign non-install invocations are not installs and stay outside the gate.
    benign = (
        _DOCKERFILE.read_text(encoding="utf-8")
        + "\nRUN pip check\nRUN pip --version\nRUN uv pip list\n"
        + "RUN python3 -m pip freeze\nRUN pip help install\n"
    )
    assert _find_violations(lock_text, benign) == []


def test_pip_requirement_files_fail_closed() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")

    # The unchanged Dockerfile stays the positive control.
    assert _find_violations(lock_text, dockerfile) == []

    # Both audited paths are accepted in every supported spelling.
    for path in (_BOOTSTRAP_REQUIREMENTS_PATH, _REQUIREMENTS_PATH):
        for spelling in (
            f"-r {path}",
            f"--requirement {path}",
            f"--requirement={path}",
            f"-r{path}",
        ):
            assert (
                _find_violations(lock_text, f"{dockerfile}\nRUN pip install {spelling}\n")
                == []
            )

    # Every unaudited path is rejected in every supported spelling.
    for spelling in (
        "-r /tmp/extra.txt",
        "--requirement /tmp/extra.txt",
        "--requirement=/tmp/extra.txt",
        "-r/tmp/extra.txt",
    ):
        violations = _find_violations(
            lock_text, f"{dockerfile}\nRUN pip install {spelling}\n"
        )
        assert Violation("/tmp/extra.txt", "pip requirements file is not audited") in violations

    # A missing requirement argument is a diagnostic, not a silent pass.
    for option in ("-r", "--requirement"):
        violations = _find_violations(lock_text, f"{dockerfile}\nRUN pip install {option}\n")
        assert (
            Violation(option, "pip requirement option is missing an argument") in violations
        )

    # A supported timeout alias does not hide an unaudited requirement file.
    default_timeout = _find_violations(
        lock_text,
        f"{dockerfile}\nRUN pip --default-timeout 15 install -r /tmp/extra.txt\n",
    )
    assert Violation("/tmp/extra.txt", "pip requirements file is not audited") in default_timeout

    # A quoted operator is an option value, not a boundary; the file is still audited.
    quoted = _find_violations(
        lock_text, f"{dockerfile}\nRUN pip --log '&&' install -r /tmp/extra.txt\n"
    )
    assert Violation("/tmp/extra.txt", "pip requirements file is not audited") in quoted
    quoted_audited = _find_violations(
        lock_text,
        f"{dockerfile}\nRUN pip --log '&&' install -r {_BOOTSTRAP_REQUIREMENTS_PATH}\n",
    )
    assert quoted_audited == []

    # A separate invalid install is still checked beside the generated install.
    violations = _find_violations(
        lock_text, f"{dockerfile}\nRUN pip install claude-agent-sdk aiohttp\n"
    )
    assert Violation(
        "claude-agent-sdk", "pip install is missing an exact version"
    ) in violations
    assert Violation("aiohttp", "pip install is missing an exact version") in violations


@pytest.mark.parametrize("path", (_BOOTSTRAP_REQUIREMENTS_PATH, _REQUIREMENTS_PATH))
@pytest.mark.parametrize(
    ("argument", "option"),
    [
        ("--requirement=", "--requirement="),
        ("--requirement=''", "--requirement="),
        ('--requirement=""', "--requirement="),
        ("--requirement ''", "--requirement"),
        ("-r ''", "-r"),
    ],
)
def test_pip_requirement_files_fail_closed_for_empty_values(
    path: str, argument: str, option: str
) -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    violations = _find_violations(
        lock_text, f"{dockerfile}\nRUN pip install {argument} {path}\n"
    )
    assert Violation(option, "pip requirement option is missing an argument") in violations
