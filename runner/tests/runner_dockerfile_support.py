"""Shared Dockerfile parsing for the runner image tests."""

from __future__ import annotations

from pathlib import Path

RUNNER_DOCKERFILE = Path(__file__).resolve().parents[1] / "Dockerfile"


def logical_instructions(dockerfile_text: str) -> list[str]:
    """Join backslash continuations and drop blank and comment lines."""
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
