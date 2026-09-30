"""The shipped publication Job image must contain git."""

import re

from runner_dockerfile_support import RUNNER_DOCKERFILE, logical_instructions


def test_runner_image_installs_git_for_snapshot_and_publication_jobs() -> None:
    instructions = logical_instructions(RUNNER_DOCKERFILE.read_text(encoding="utf-8"))
    git_install = next(
        index
        for index, instruction in enumerate(instructions)
        if instruction.startswith("RUN ")
        and "apt-get" in instruction
        and "install" in instruction
        and re.search(r"\bgit\b", instruction)
    )
    drop_root = instructions.index("USER 1000:1000")

    assert git_install < drop_root
