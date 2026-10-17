"""Run each image's dependency-layer sync using only its declared metadata."""

import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("application", ["api", "worker", "dispatcher", "mail-adapter"])
def test_production_dependency_layer_resolves_complete_workspace(
    tmp_path: Path, application: str
) -> None:
    dockerfile = ROOT / "apps" / application / "Dockerfile"
    instructions = dockerfile.read_text(encoding="utf-8").replace("\\\n", " ").splitlines()
    command: list[str] | None = None
    for instruction in instructions:
        tokens = shlex.split(instruction, comments=True)
        if tokens[:3] == ["RUN", "uv", "sync"]:
            command = tokens[1:]
            break
        if tokens[:1] != ["COPY"]:
            continue
        sources, destination = tokens[1:-1], tokens[-1]
        for source in sources:
            if source not in {"pyproject.toml", "uv.lock"} and not source.endswith(
                "/pyproject.toml"
            ):
                continue
            target = tmp_path / destination / Path(source).name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / source, target)
    assert command is not None, f"No dependency-layer uv sync found in {dockerfile}"
    assert "--frozen" in command
    executable = shutil.which(command[0])
    assert executable is not None, "The workspace dependency-layer check requires uv"
    environment = dict(os.environ)
    environment["UV_PYTHON"] = sys.executable
    process = subprocess.run(
        [executable, *command[1:], "--dry-run"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    output = process.stdout + process.stderr
    assert process.returncode == 0, output
    assert (
        re.search(r"^\s*\+\s+curie[-_]factory[-_]preparation(?:\s|[=@])", output, re.MULTILINE)
        is None
    ), output
