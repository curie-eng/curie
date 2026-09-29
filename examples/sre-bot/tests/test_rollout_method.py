"""Static contract for the SRE example's rollout investigation method."""

import re
from pathlib import Path

SKILL = Path(__file__).parents[1] / "skills" / "sre-bot" / "SKILL.md"


def test_failed_new_replica_set_requires_template_diff_before_hypothesis() -> None:
    method = SKILL.read_text().split("## How to answer", 1)[1].split(
        "## How to write the reply", 1
    )[0]
    steps = re.findall(r"(?ms)^\d+\.\s.*?(?=^\d+\.|\Z)", method)
    rollout = next((step for step in steps if "ReplicaSet" in step), "")

    assert "resources_get" in rollout
    assert "old" in rollout and "new" in rollout
    assert "pod templates" in rollout
    assert "before" in rollout and "hypothes" in rollout
    assert "changed fields" in rollout
