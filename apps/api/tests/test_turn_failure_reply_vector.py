"""Factory status comments expose the same failure class as the channel reply (#3401)."""

from __future__ import annotations

import json
from pathlib import Path

from curie_api.factory_notices import needs_human, result_section

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "turn-failure-reply.json"


def test_factory_result_names_the_vector_failure_class() -> None:
    vector = json.loads(_VECTOR.read_text())
    for example in vector["examples"]:
        body = result_section(example["factory_cause"], pr_url=None)
        assert body.startswith("Could not complete:")
        assert example["factory_class_line"] in body
        assert f"Cause: {example['factory_cause']}" in body


def test_split_escalation_causes_still_need_a_person() -> None:
    assert needs_human("failed", "runner_escalated")
    assert needs_human("failed", "unclassified")
    assert needs_human("failed", "max_turns")
    assert needs_human("failed", "ci_failed")
    assert not needs_human("failed", "early_stop")
    assert not needs_human("completed", "max_turns")


def test_a_completed_result_has_no_failure_class() -> None:
    body = result_section(
        "completed",
        pr_url="https://github.com/acme-corp/acme-bot/pull/1",
    )
    assert "Failure class:" not in body
    assert "curie-turn-failure:" not in body
