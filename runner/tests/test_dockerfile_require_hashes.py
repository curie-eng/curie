"""The runner image installs every Python package from hashed requirements.

The pin exporter lists every sha256 in a package's uv.lock record, pip itself
comes from a hashed requirements file, and both installs run under
--require-hashes so a tampered or substituted artifact fails the build.
"""

from __future__ import annotations

import re
import shlex
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from runner_dockerfile_support import logical_instructions

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "runner" / "Dockerfile"
_EXPORTER = _REPO_ROOT / "runner" / "export_dependency_pins.py"
_PIP_REQUIREMENTS = _REPO_ROOT / "runner" / "pip-requirements.txt"
_UV_LOCK = _REPO_ROOT / "uv.lock"
_GENERATED_REQUIREMENTS = "/tmp/runner-dependency-pins.txt"
_PIP_REQUIREMENTS_COPY = "COPY runner/pip-requirements.txt ./runner/pip-requirements.txt"
_PIP_VERSION = "26.2.1"
_PIP_WHEEL_HASH = "sha256:71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e"
_HASH_OPTION = re.compile(r"--hash=(sha256:[0-9a-f]{64})")
_HASH_VALUE = re.compile(r"sha256:[0-9a-f]{64}")
_REGISTRY = '{ registry = "https://pypi.org/simple" }'


def _lock(root_deps: str, packages: str) -> str:
    return f"""\
version = 1
revision = 3
requires-python = ">=3.13"

[[package]]
name = "curie-runner"
version = "0.0.0"
source = {{ editable = "runner" }}
dependencies = [
{root_deps}
]

{packages}
"""


def _run_exporter(lock_text: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_EXPORTER)],
        input=lock_text,
        capture_output=True,
        check=False,
        text=True,
    )


def _split_line(line: str) -> tuple[str, list[str]]:
    """Split an exporter line into its requirement and its --hash values."""
    requirement, *options = line.split(" --hash=")
    return requirement, [f"{option}" for option in options]


def _record_hashes(package: dict) -> set[str]:
    hashes = {wheel["hash"] for wheel in package.get("wheels", [])}
    if "sdist" in package:
        hashes.add(package["sdist"]["hash"])
    return hashes


def _dockerfile_violations(dockerfile_text: str) -> list[str]:
    """Return the hash-pinning violations in a runner Dockerfile text."""
    violations: list[str] = []
    instructions = logical_instructions(dockerfile_text)
    runs = [i for i in instructions if i.upper().startswith("RUN ")]

    for instruction in runs:
        tokens = shlex.split(instruction)
        for index, token in enumerate(tokens):
            if token in {"--upgrade", "-U"} and "pip" in tokens[index + 1 :][:2]:
                violations.append("pip is upgraded unhashed")
        if re.search(r"(--upgrade|-U)\s+pip\b", instruction):
            violations.append("pip is upgraded unhashed")

    pip_installs = [
        i for i in runs if "pip-requirements.txt" in i and "pip install" in i
    ]
    if len(pip_installs) != 1:
        violations.append("pip must be installed once from runner/pip-requirements.txt")
    elif not re.search(
        r"pip install\b[^&|;]*--require-hashes[^&|;]*-r runner/pip-requirements\.txt",
        pip_installs[0],
    ) and not re.search(
        r"pip install\b[^&|;]*-r runner/pip-requirements\.txt[^&|;]*--require-hashes",
        pip_installs[0],
    ):
        violations.append("pip install of pip-requirements.txt lacks --require-hashes")

    dependency_installs = [
        i for i in runs if f"-r {_GENERATED_REQUIREMENTS}" in i and "pip install" in i
    ]
    if len(dependency_installs) != 1:
        violations.append("generated requirements must be installed once")
    else:
        segment = re.search(
            rf"pip install\b[^&|;]*-r {re.escape(_GENERATED_REQUIREMENTS)}[^&|;]*",
            dependency_installs[0],
        )
        text = segment.group(0) if segment else ""
        before = re.search(r"pip install\b[^&|;]*", dependency_installs[0]).group(0)
        flags = text + " " + before
        if "--require-hashes" not in flags:
            violations.append("dependency install lacks --require-hashes")
        if "--no-deps" not in flags:
            violations.append("dependency install lacks --no-deps")

    copy_index = next(
        (n for n, i in enumerate(instructions) if i == _PIP_REQUIREMENTS_COPY), None
    )
    venv_index = next(
        (n for n, i in enumerate(instructions) if i.startswith("RUN ") and "-m venv" in i),
        None,
    )
    if copy_index is None:
        violations.append("pip-requirements.txt is never copied")
    elif venv_index is None or copy_index > venv_index:
        violations.append("pip-requirements.txt copy must precede the venv RUN")
    return violations


def test_real_lock_exports_hashes_matching_each_lock_record() -> None:
    lock_text = _UV_LOCK.read_text(encoding="utf-8")
    packages = {p["name"]: p for p in tomllib.loads(lock_text)["package"]}

    result = _run_exporter(lock_text)

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines
    for line in lines:
        requirement, hashes = _split_line(line)
        name = Requirement(requirement.split(" ;")[0]).name
        assert hashes, f"{line[:80]} carries no --hash"
        assert all(_HASH_VALUE.fullmatch(h) for h in hashes), line[:120]
        assert hashes == sorted(set(hashes)), f"{name} hashes not sorted and unique"
        assert set(hashes) == _record_hashes(packages[name]), (
            f"{name} hashes differ from its uv.lock record"
        )


def test_exporter_lists_every_wheel_and_sdist_hash_sorted_and_deduplicated() -> None:
    a, b, c = ("sha256:" + ch * 64 for ch in "abc")
    lock_text = _lock(
        '    { name = "pkg" },',
        f"""\
[[package]]
name = "pkg"
version = "1.0.0"
source = {_REGISTRY}
sdist = {{ url = "https://example.invalid/p.tar.gz", hash = "{c}" }}
wheels = [
    {{ url = "https://example.invalid/p-1-py3-none-any.whl", hash = "{b}" }},
    {{ url = "https://example.invalid/p-1-cp313.whl", hash = "{a}" }},
    {{ url = "https://example.invalid/p-1-dup.whl", hash = "{b}" }},
]
""",
    )

    result = _run_exporter(lock_text)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == (
        f"pkg==1.0.0 --hash={a} --hash={b} --hash={c}"
    )


def test_exporter_puts_hashes_after_the_marker() -> None:
    h = "sha256:" + "2" * 64
    lock_text = _lock(
        "    { name = \"win\", marker = \"sys_platform == 'win32'\" },",
        f"""\
[[package]]
name = "win"
version = "1.0.0"
source = {_REGISTRY}
wheels = [
    {{ url = "https://example.invalid/w.whl", hash = "{h}" }},
]
""",
    )

    result = _run_exporter(lock_text)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"win==1.0.0 ; sys_platform == 'win32' --hash={h}"


def test_exporter_output_is_pip_requirements_syntax() -> None:
    h1, h2 = "sha256:" + "3" * 64, "sha256:" + "4" * 64
    lock_text = _lock(
        '    { name = "plain" },\n    { name = "win", marker = "sys_platform == \'win32\'" },',
        f"""\
[[package]]
name = "plain"
version = "1.2.3"
source = {_REGISTRY}
sdist = {{ url = "https://example.invalid/p.tar.gz", hash = "{h1}" }}

[[package]]
name = "win"
version = "4.5.6"
source = {_REGISTRY}
wheels = [
    {{ url = "https://example.invalid/w1.whl", hash = "{h1}" }},
    {{ url = "https://example.invalid/w2.whl", hash = "{h2}" }},
]
""",
    )

    result = _run_exporter(lock_text)

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) == 2
    for line in lines:
        requirement, *options = line.split(" --")
        parsed = Requirement(requirement)
        assert len(parsed.specifier) == 1 and str(parsed.specifier).startswith("==")
        assert options, "requirement carries no hash options"
        for option in options:
            assert re.fullmatch(r"hash=sha256:[0-9a-f]{64}", option.strip()), option
    assert Requirement(lines[1].split(" --")[0]).marker is not None


@pytest.mark.parametrize(
    "record_extra",
    [
        pytest.param("", id="no-wheels-no-sdist"),
        pytest.param("wheels = []", id="empty-wheels"),
    ],
)
def test_exporter_refuses_a_registry_package_without_hashes(record_extra: str) -> None:
    lock_text = _lock(
        '    { name = "bare" },',
        f"""\
[[package]]
name = "bare"
version = "1.0.0"
source = {_REGISTRY}
{record_extra}
""",
    )

    result = _run_exporter(lock_text)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


@pytest.mark.parametrize(
    "bad_hash",
    ["md5:abc", "sha256:" + "A" * 64, "sha256:" + "1" * 63, "1" * 64],
)
def test_exporter_refuses_malformed_hash_values(bad_hash: str) -> None:
    lock_text = _lock(
        '    { name = "pkg" },',
        f"""\
[[package]]
name = "pkg"
version = "1.0.0"
source = {_REGISTRY}
sdist = {{ url = "https://example.invalid/p.tar.gz", hash = "{bad_hash}" }}
""",
    )

    result = _run_exporter(lock_text)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "invalid uv.lock" in result.stderr


def test_pip_requirements_pin_pip_exactly_with_hashes() -> None:
    lines = [
        line.strip()
        for line in _PIP_REQUIREMENTS.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    joined = " ".join(line.rstrip("\\").strip() for line in lines)
    requirement, *options = joined.split(" --")
    parsed = Requirement(requirement)

    assert parsed.name == "pip"
    assert str(parsed.specifier) == f"=={_PIP_VERSION}"
    assert parsed.marker is None
    hashes = [o.strip().removeprefix("hash=") for o in options]
    assert hashes and all(_HASH_VALUE.fullmatch(h) for h in hashes)
    assert _PIP_WHEEL_HASH in hashes
    assert not any(line.startswith("-") and "--hash" not in line for line in lines)


def test_actual_dockerfile_installs_everything_with_require_hashes() -> None:
    assert _dockerfile_violations(_DOCKERFILE.read_text(encoding="utf-8")) == []


def test_actual_dockerfile_does_not_upgrade_pip() -> None:
    for instruction in logical_instructions(_DOCKERFILE.read_text(encoding="utf-8")):
        assert not re.search(r"(--upgrade|-U)\s+pip\b", instruction), instruction


def test_guard_rejects_dependency_install_without_require_hashes() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    dependency_install = (
        f"/app/.venv/bin/pip install --no-cache-dir --require-hashes --no-deps "
        f"-r {_GENERATED_REQUIREMENTS}"
    )
    # Mutate whatever the implementer wrote: strip the flag from the install
    # that consumes the generated requirements.
    mutated = re.sub(
        rf"(pip install[^\n&|;]*?)\s*--require-hashes"
        rf"([^\n&|;]*-r {re.escape(_GENERATED_REQUIREMENTS)})",
        r"\1\2",
        dockerfile,
    )
    if mutated == dockerfile:
        mutated = re.sub(
            rf"(pip install[^\n&|;]*-r {re.escape(_GENERATED_REQUIREMENTS)})"
            r"[^\n&|;]*--require-hashes",
            r"\1",
            dockerfile,
        )
    assert mutated != dockerfile, f"real Dockerfile has no {dependency_install!r}-shaped install"
    assert "dependency install lacks --require-hashes" in _dockerfile_violations(mutated)


def test_guard_rejects_the_prechange_unhashed_pip_upgrade() -> None:
    prechange = """\
FROM python:3.13.15-slim
COPY uv.lock ./uv.lock
RUN python3 -m venv /app/.venv \\
    && /app/.venv/bin/pip install --no-cache-dir --upgrade pip \\
    && python3 runner/export_dependency_pins.py < uv.lock > /tmp/runner-dependency-pins.txt \\
    && /app/.venv/bin/pip install --no-cache-dir --no-deps -r /tmp/runner-dependency-pins.txt
"""
    violations = _dockerfile_violations(prechange)
    assert "pip is upgraded unhashed" in violations
    assert "dependency install lacks --require-hashes" in violations
    assert "pip-requirements.txt is never copied" in violations
