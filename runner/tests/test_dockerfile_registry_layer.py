"""The runner image installs the registry dependency closure before copying
runner source, so an edit under runner/src does not rebuild claude_agent_sdk.

The pin exporter (export_dependency_pins.py) and uv.lock are the only runner
inputs that may precede that install; they are the pin source, not product
source. The per-Dockerfile ignore must exist and must not drop a COPY source.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "runner" / "Dockerfile"
_DOCKERIGNORE = _REPO_ROOT / "runner" / "Dockerfile.dockerignore"
_REQUIREMENTS_PATH = "/tmp/runner-dependency-pins.txt"
_RUNNER_SOURCE_COPY = "COPY runner ./runner"
_PIN_EXPORTER_COPY = (
    "COPY runner/export_dependency_pins.py ./runner/export_dependency_pins.py"
)
_LOCKFILE_COPY = "COPY uv.lock ./uv.lock"

# Mirror apps/api/Dockerfile.dockerignore plus the trees a repo-root build
# otherwise walks on a developer checkout.
_REQUIRED_IGNORE_PATTERNS = (
    ".git",
    "**/.venv",
    "**/__pycache__",
    "**/*.pyc",
    "**/.mypy_cache",
    "**/.pytest_cache",
    "**/.ruff_cache",
    "**/node_modules",
    "apps/ui/dist",
    "apps/ui/test-results",
    "apps/ui/playwright-report",
    "cli/target",
    "docs",
    "prototypes",
    ".github",
    "charts",
    "*.md",
    "**/*.md",
    "**/.env",
    ".worktrees",
    ".projects",
)

_COPY_SOURCES = (
    "uv.lock",
    "packages/aci-protocol",
    "packages/plugin-format",
    "packages/telemetry",
    "packages/telemetry-schema",
    "runner",
    "runner/export_dependency_pins.py",
    "runner/pip.conf",
)

# Patterns that would drop a COPY source wholesale. dockerignore last-match
# wins; none of these may appear as a (non-negated) pattern.
_FORBIDDEN_IGNORE_PATTERNS = (
    "*",
    "**",
    "uv.lock",
    "packages",
    "packages/",
    "packages/**",
    "packages/aci-protocol",
    "packages/plugin-format",
    "packages/telemetry",
    "packages/telemetry-schema",
    "runner",
    "runner/",
    "runner/**",
    "runner/export_dependency_pins.py",
)


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


def _index_of(instructions: list[str], exact: str) -> int:
    try:
        return instructions.index(exact)
    except ValueError as exc:
        raise AssertionError(f"missing instruction {exact!r}") from exc


def _registry_install_indexes(instructions: list[str]) -> list[int]:
    return [
        index
        for index, instruction in enumerate(instructions)
        if instruction.startswith("RUN ")
        and f"-r {_REQUIREMENTS_PATH}" in instruction
        and "pip install" in instruction
    ]


def _dockerignore_patterns(text: str) -> list[str]:
    patterns: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def test_registry_install_precedes_runner_source_copy() -> None:
    instructions = _logical_instructions(_DOCKERFILE.read_text(encoding="utf-8"))
    runner_copy = _index_of(instructions, _RUNNER_SOURCE_COPY)
    registry_indexes = _registry_install_indexes(instructions)
    assert len(registry_indexes) == 1, (
        "Dockerfile must pip-install the generated runner requirements once"
    )
    assert registry_indexes[0] < runner_copy, (
        "registry dependency install must precede COPY runner ./runner so a "
        "runner source edit does not rebuild the registry closure"
    )


def test_registry_layer_copies_only_lock_and_pin_exporter() -> None:
    instructions = _logical_instructions(_DOCKERFILE.read_text(encoding="utf-8"))
    registry_indexes = _registry_install_indexes(instructions)
    assert len(registry_indexes) == 1
    registry_index = registry_indexes[0]
    allowed_copies = {_LOCKFILE_COPY, _PIN_EXPORTER_COPY}

    for instruction in instructions[:registry_index]:
        first_word = instruction.split(maxsplit=1)[0].upper()
        if first_word not in ("COPY", "ADD"):
            continue
        if instruction.startswith("COPY --from="):
            continue
        assert instruction in allowed_copies, (
            "unexpected source transfer before registry layer: "
            f"{instruction!r}"
        )

    assert _index_of(instructions, _LOCKFILE_COPY) < registry_index
    assert _index_of(instructions, _PIN_EXPORTER_COPY) < registry_index


def test_local_package_install_follows_source_copy() -> None:
    instructions = _logical_instructions(_DOCKERFILE.read_text(encoding="utf-8"))
    runner_copy = _index_of(instructions, _RUNNER_SOURCE_COPY)
    local_install_indexes = [
        index
        for index, instruction in enumerate(instructions)
        if instruction.startswith("RUN ")
        and "pip install --no-cache-dir --no-deps ./runner" in instruction
    ]
    assert local_install_indexes, (
        "Dockerfile must pip-install ./runner with --no-deps after copying it"
    )
    assert all(index > runner_copy for index in local_install_indexes)


def test_combined_venv_and_registry_install_is_rejected() -> None:
    """A synthetic pre-change Dockerfile fails the precedes-source check."""
    prechange = """\
FROM python:3.13.15-slim
WORKDIR /app
COPY uv.lock ./uv.lock
COPY packages/aci-protocol ./packages/aci-protocol
COPY runner ./runner
RUN python3 -m venv /app/.venv \\
    && /app/.venv/bin/pip install --no-cache-dir --no-deps ./runner \\
    && python3 runner/export_dependency_pins.py < uv.lock > /tmp/runner-dependency-pins.txt \\
    && /app/.venv/bin/pip install --no-cache-dir --no-deps -r /tmp/runner-dependency-pins.txt
"""
    instructions = _logical_instructions(prechange)
    runner_copy = _index_of(instructions, _RUNNER_SOURCE_COPY)
    registry_indexes = _registry_install_indexes(instructions)
    assert len(registry_indexes) == 1
    assert registry_indexes[0] > runner_copy


def test_dockerignore_exists_with_required_patterns() -> None:
    assert _DOCKERIGNORE.is_file(), (
        "runner/Dockerfile.dockerignore must exist so a repo-root build does "
        "not walk .worktrees, .projects, and cli/target"
    )
    patterns = set(_dockerignore_patterns(_DOCKERIGNORE.read_text(encoding="utf-8")))
    missing = [
        pattern for pattern in _REQUIRED_IGNORE_PATTERNS if pattern not in patterns
    ]
    assert missing == []


def test_dockerignore_does_not_exclude_dockerfile_copy_sources() -> None:
    assert _DOCKERIGNORE.is_file()
    patterns = _dockerignore_patterns(_DOCKERIGNORE.read_text(encoding="utf-8"))
    forbidden = [
        pattern for pattern in patterns if pattern in _FORBIDDEN_IGNORE_PATTERNS
    ]
    assert forbidden == []
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    for source in _COPY_SOURCES:
        assert source in dockerfile, (
            f"COPY source {source!r} is missing from runner/Dockerfile"
        )
