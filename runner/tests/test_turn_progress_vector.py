"""Runner half of the frozen turn progress capability vector (ADR 0130).

The runner reads the capability from the runner control headers the worker
sends and presents the token to the API in its request header. The worker and
the API ship in other images, so each side compares its own constants against
``tests/vectors/turn-progress-capability.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

from curie_runner import turn_progress

_VECTOR = (
    Path(__file__).resolve().parents[2] / "tests" / "vectors" / "turn-progress-capability.json"
)
_EXPECTED_KEYS = {
    "comment",
    "url_header",
    "token_header",
    "token_request_header",
    "token_scope",
    "route",
    "inbox_key",
    "inbox_entry_example",
}


def test_the_runner_reads_and_presents_the_frozen_headers() -> None:
    vector = json.loads(_VECTOR.read_text())
    unknown = set(vector) - _EXPECTED_KEYS
    assert not unknown, (
        f"unknown keys in {_VECTOR.name}: {sorted(unknown)}. Teach them to this test, "
        "apps/worker/tests/test_turn_progress_vector.py and "
        "apps/api/tests/test_turn_progress_vector.py."
    )
    assert set(vector) == _EXPECTED_KEYS
    assert turn_progress.PROGRESS_URL_HEADER == vector["url_header"]
    assert turn_progress.PROGRESS_TOKEN_HEADER == vector["token_header"]
    assert turn_progress.PROGRESS_TOKEN_REQUEST_HEADER == vector["token_request_header"]
