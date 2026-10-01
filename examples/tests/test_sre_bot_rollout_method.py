"""Static contract for the SRE example's rollout investigation method."""

import json
import re
from pathlib import Path

EXAMPLE = Path(__file__).parents[1] / "sre-bot"
SKILL = EXAMPLE / "skills" / "sre-bot" / "SKILL.md"
OLD_RS = "acme-dispatcher-5c72a"
NEW_RS = "acme-dispatcher-6f89d"
GET = "mcp__kubernetes__resources_get"


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


def test_rollout_evals_pin_evidence_acquisition_and_command_contrast() -> None:
    acquisition = json.loads((EXAMPLE / "evals" / "rollout" / "cases.json").read_text())
    case = acquisition["cases"][0]
    question = case["input"]
    assert question.count(OLD_RS) == 1
    assert question.count(NEW_RS) == 1
    assert "old ReplicaSet " + OLD_RS in question
    assert "new ReplicaSet " + NEW_RS in question
    assert "resources_get replies" not in question
    assert "spec.template" not in question
    assert "exit 1" not in question
    assert case["grader"] == {"kind": "tool_called", "expected": GET}

    trajectory = json.loads((EXAMPLE / "evals" / "rollout" / "trajectory.json").read_text())
    assert trajectory["specs"][0]["case_id"] == case["id"]
    assert trajectory["specs"][0]["expected"] == [GET, GET]

    interpretation = json.loads((EXAMPLE / "evals" / "cases.json").read_text())
    explanation = next(
        item for item in interpretation["cases"]
        if item["id"] == "failed-rollout-names-the-template-command-change"
    )
    assert '"command":["python","-m","dispatcher"]' in explanation["input"]
    assert '"command":["sh","-c","exit 1"]' in explanation["input"]
