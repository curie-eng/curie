"""The worker reason Literal matches tests/vectors/hook-run-reasons.json."""

from __future__ import annotations

import json
from pathlib import Path
from typing import get_args

from curie_worker.hook_runs import HookRunReason

_VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "hook-run-reasons.json"


def test_hook_run_reasons_vector() -> None:
    parsed = json.loads(_VECTOR.read_text(encoding="utf-8"))
    assert list(get_args(HookRunReason)) == parsed["reasons"]
