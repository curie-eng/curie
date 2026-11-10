"""Cross-surface wording contract for model feedback."""

import json
from pathlib import Path

from curie_runner.caller_feedback import action_label


def test_runner_action_labels_match_presentation_vector() -> None:
    vector = Path(__file__).resolve().parents[2] / "tests/vectors/user-action-wording.json"
    for case in json.loads(vector.read_text())["vectors"]:
        assert action_label(case["tool"]) == case["label"]
