"""The scripted factory transcript publishes a committed change (#5230).

The ``scenario`` leg replays ``transcripts/unitconv-issue.json`` against the
real review gate. Its implementing lane commits the fixture change before it
calls the diff reviewer, so the factory control publishes a committed delta.
The gate appends a CANDIDATE block naming the run's base commit to the diff
reviewer's prompt, after the verification contract. The replay server keys a
lane by the scrubbed first user message, so the recorded diff reviewer lane
must end with that block, or replay stops matching the reviewer conversation.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parents[1]
TRANSCRIPT = ROOT / "transcripts" / "unitconv-issue.json"
CONTRACT = REPO_ROOT / "examples" / "dark-factory" / "verification" / "contract.md"
DIFF_LANE = "You are the diff reviewer"
PLAN_LANE = "You are the plan reviewer"
# The implementing lane's Bash call that runs the checks right before the diff review.
COMMIT_ORDINAL = 14


def _module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "curie_model_script_candidate_lane", ROOT / "model_script.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ms = _module()


def candidate_block(base: str) -> str:
    """The block the review gate appends to the diff reviewer's prompt, after the contract."""
    return (
        "\n\nCANDIDATE:\n"
        f"base commit {base}\n"
        "The publication candidate is the net change from that base commit to the final "
        "working tree files, with committed, staged, unstaged and untracked changes together. "
        f"Review git diff {base} and every untracked file git status lists. "
        "Local commits are flattened into one publication."
    )


def _exchanges() -> list[dict[str, Any]]:
    exchanges: list[dict[str, Any]] = json.loads(TRANSCRIPT.read_text())["exchanges"]
    return exchanges


def _main_lane() -> dict[int, dict[str, Any]]:
    """The implementing lane, the one lane that is not a reviewer's, by ordinal."""
    main = [e for e in _exchanges() if "You are the" not in e["lane_key"]]
    assert len({e["lane_key"] for e in main}) == 1
    return {e["ordinal"]: e for e in main}


def _tool_uses(exchange: dict[str, Any], name: str) -> list[dict[str, Any]]:
    return [
        block["input"]
        for block in exchange["message"]["content"]
        if block.get("type") == "tool_use" and block.get("name") == name
    ]


def test_the_implementing_lane_commits_the_change_before_the_diff_review() -> None:
    main = _main_lane()

    (bash,) = _tool_uses(main[COMMIT_ORDINAL], "Bash")
    command = bash["command"]
    # The checks still run first, then everything is staged and committed.
    add = command.find("git -C /workspace add -A")
    assert add > command.find("unittest") >= 0, command
    assert re.search(r"git -C /workspace(?: -c \S+)* commit\b", command[add:]), command
    # The very next turn calls the diff reviewer, so the review sees the commit.
    (agent,) = _tool_uses(main[COMMIT_ORDINAL + 1], "Agent")
    assert agent["prompt"].startswith(DIFF_LANE)


def test_every_diff_reviewer_exchange_ends_with_the_scrubbed_candidate_block() -> None:
    contract = CONTRACT.read_text(encoding="utf-8")
    block = ms.scrub(candidate_block("0" * 40))
    # Any 40 hex base scrubs to the same text, so the recorded lane matches every run.
    assert "0" * 40 not in block and block.count("<v>") == 2
    assert block == ms.scrub(candidate_block("1a2b3c4d" * 5))
    diff = [e for e in _exchanges() if DIFF_LANE in e["lane_key"]]
    assert diff, "the transcript has no diff reviewer lane"
    assert sorted(e["ordinal"] for e in diff) == list(range(len(diff)))

    expected = ms.scrub(contract + candidate_block("0" * 40))
    for exchange in diff:
        assert exchange["lane_key"].endswith(expected), (
            exchange["ordinal"],
            exchange["lane_key"][-400:],
        )


def test_plan_reviewer_exchanges_carry_no_candidate_block() -> None:
    """Control: the gate appends the block to the diff reviewer only."""
    contract = ms.scrub(CONTRACT.read_text(encoding="utf-8"))
    plan = [e for e in _exchanges() if PLAN_LANE in e["lane_key"]]
    assert plan, "the transcript has no plan reviewer lane"
    for exchange in plan:
        assert exchange["lane_key"].endswith(contract), exchange["ordinal"]
        assert "CANDIDATE:" not in exchange["lane_key"], exchange["ordinal"]
