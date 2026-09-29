"""Worker half of the frozen turn progress capability vector (ADR 0130).

The worker mints the capability and sends it in the runner control headers, and
its pump reads the inbox the API writes. The runner and the API ship in other
images, so each side compares its own constants against
``tests/vectors/turn-progress-capability.json``.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from channel_protocol.progress import ProgressCommand
from curie_worker import turn_progress
from curie_worker.config import WorkerConfig

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "turn-progress-capability.json"
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


def _vector() -> dict:
    parsed = json.loads(_VECTOR.read_text())
    unknown = set(parsed) - _EXPECTED_KEYS
    assert not unknown, (
        f"unknown keys in {_VECTOR.name}: {sorted(unknown)}. Teach them to this test, "
        "runner/tests/test_turn_progress_vector.py and apps/api/tests/test_turn_progress_vector.py."
    )
    assert set(parsed) == _EXPECTED_KEYS
    return parsed


def test_the_worker_sends_the_frozen_headers_scope_and_route() -> None:
    vector = _vector()
    assert turn_progress.URL_HEADER == vector["url_header"]
    assert turn_progress.TOKEN_HEADER == vector["token_header"]
    assert turn_progress.TOKEN_SCOPE == vector["token_scope"]
    assert turn_progress.ROUTE == vector["route"]


def test_the_worker_reads_the_frozen_inbox_key() -> None:
    vector = _vector()
    progress_id = str(uuid.uuid4())
    config = WorkerConfig(key_prefix="acme:worker")
    assert config.progress_inbox_key(progress_id) == vector["inbox_key"].format(
        key_prefix="acme:worker", progress_id=progress_id
    )


def test_the_pump_parses_the_frozen_inbox_entry() -> None:
    example = _vector()["inbox_entry_example"]
    parsed = turn_progress.parse_inbox_entry(example)
    assert parsed is not None
    command, epoch, seq = parsed
    assert command == ProgressCommand.model_validate_json(example["command"])
    assert (epoch, seq) == (int(example["epoch"]), int(example["seq"]))
    # Any other field set is not the API's entry and is refused, not guessed at.
    assert turn_progress.parse_inbox_entry({**example, "channel": "C0EXAMPLE1"}) is None
    assert turn_progress.parse_inbox_entry({"command": example["command"]}) is None
