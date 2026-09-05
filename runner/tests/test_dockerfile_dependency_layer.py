"""The app Dockerfiles' builder stages install locked third-party dependencies
in a layer that precedes the source copy, so an edit to application source
never invalidates that ~443 MB install. The dependency `uv sync` runs
`--no-install-workspace` before `COPY . .`; a final `uv sync` without that flag
runs after, to build the workspace member itself against the already-warm venv.
The allowlist of instructions permitted before that sync is exact, so nothing
but manifest copies can land in the dependency layer.
"""

from __future__ import annotations

import functools
import tomllib
from collections.abc import Callable
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

APP_DOCKERFILES = (
    "apps/api/Dockerfile",
    "apps/worker/Dockerfile",
    "apps/dispatcher/Dockerfile",
    "apps/mail-adapter/Dockerfile",
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


@functools.cache
def _builder_stage_instructions(relative: str) -> list[str]:
    text = (_REPO_ROOT / relative).read_text(encoding="utf-8")
    instructions = _logical_instructions(text)
    from_indexes = [i for i, ins in enumerate(instructions) if ins.startswith("FROM ")]
    assert len(from_indexes) >= 2, f"{relative} does not have a runtime stage"
    return instructions[from_indexes[0] : from_indexes[1]]


def _index_of(instructions: list[str], predicate: Callable[[str], bool]) -> int:
    return next(i for i, ins in enumerate(instructions) if predicate(ins))


def _is_dep_sync(ins: str) -> bool:
    return ins.startswith("RUN uv sync") and "--frozen" in ins and "--no-install-workspace" in ins


def _is_final_sync(ins: str) -> bool:
    return (
        ins.startswith("RUN uv sync") and "--frozen" in ins and "--no-install-workspace" not in ins
    )


@functools.cache
def _workspace_members() -> list[str]:
    data = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    members = data["tool"]["uv"]["workspace"]["members"]
    assert isinstance(members, list)
    return [str(m) for m in members]


@pytest.mark.parametrize("relative", APP_DOCKERFILES)
def test_dependency_sync_precedes_source_copy(relative: str) -> None:
    instructions = _builder_stage_instructions(relative)
    copy_all_index = _index_of(instructions, lambda ins: ins == "COPY . .")
    dep_sync_index = _index_of(instructions, _is_dep_sync)
    final_sync_index = _index_of(instructions, _is_final_sync)

    assert dep_sync_index < copy_all_index, (
        f"{relative}: the --no-install-workspace dependency sync must precede COPY . ."
    )
    assert final_sync_index > copy_all_index, (
        f"{relative}: the final workspace sync must follow COPY . ."
    )


@pytest.mark.parametrize("relative", APP_DOCKERFILES)
def test_every_workspace_member_manifest_precedes_dependency_sync(relative: str) -> None:
    instructions = _builder_stage_instructions(relative)
    dep_sync_index = _index_of(instructions, _is_dep_sync)

    lockfile_index = _index_of(instructions, lambda ins: ins == "COPY pyproject.toml uv.lock ./")
    assert lockfile_index < dep_sync_index

    for member in _workspace_members():
        expected = f"COPY {member}/pyproject.toml {member}/"
        member_index = next((i for i, ins in enumerate(instructions) if ins == expected), None)
        assert member_index is not None, (
            f"{relative}: missing '{expected}' before the dependency sync"
        )
        assert member_index < dep_sync_index


def _allowed_manifest_copies() -> set[str]:
    allowed = {"COPY pyproject.toml uv.lock ./"}
    allowed.update(f"COPY {member}/pyproject.toml {member}/" for member in _workspace_members())
    return allowed


@pytest.mark.parametrize("relative", APP_DOCKERFILES)
def test_no_source_copied_before_dependency_sync(relative: str) -> None:
    instructions = _builder_stage_instructions(relative)
    dep_sync_index = _index_of(instructions, _is_dep_sync)
    allowed = _allowed_manifest_copies()

    for instruction in instructions[:dep_sync_index]:
        first_word = instruction.split(maxsplit=1)[0].upper()
        if first_word not in ("COPY", "ADD"):
            continue
        assert instruction in allowed, (
            f"{relative}: unexpected source transfer before dependency layer: {instruction!r}"
        )
