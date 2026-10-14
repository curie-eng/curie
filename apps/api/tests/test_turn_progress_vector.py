"""API half of the frozen turn progress capability vector (ADR 0130).

The API verifies the token the worker minted and appends to the inbox the
worker's pump reads. The worker and the runner ship in other images, so each
side compares its own constants against
``tests/vectors/turn-progress-capability.json``.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from channel_protocol.progress import ProgressCommand
from curie_api import turn_progress
from curie_api.routers import turn_progress as turn_progress_router
from curie_internal.keyspace import inbox_key, inbox_pending_key

_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "turn-progress-capability.json"
)
_EXPECTED_KEYS = {
    "comment",
    "url_header",
    "token_header",
    "generation_header",
    "eligibility_env",
    "token_request_header",
    "token_scope",
    "route",
    "inbox_key",
    "inbox_pending_key",
    "inbox_entry_example",
}


def _vector() -> dict:
    parsed = json.loads(_VECTOR.read_text())
    unknown = set(parsed) - _EXPECTED_KEYS
    assert not unknown, (
        f"unknown keys in {_VECTOR.name}: {sorted(unknown)}. Teach them to this test, "
        "apps/worker/tests/test_turn_progress_vector.py and "
        "runner/tests/test_turn_progress_vector.py."
    )
    assert set(parsed) == _EXPECTED_KEYS
    return parsed


def test_the_api_verifies_the_frozen_scope_on_the_frozen_route() -> None:
    vector = _vector()
    assert turn_progress.TURN_PROGRESS_SCOPE == vector["token_scope"]
    assert turn_progress.TURN_PROGRESS_PATH == vector["route"]
    assert turn_progress.TOKEN_REQUEST_HEADER == vector["token_request_header"]
    paths = {getattr(route, "path", None) for route in turn_progress_router.router.routes}
    assert vector["route"] in paths


def test_the_api_writes_the_frozen_inbox_key_and_entry() -> None:
    vector = _vector()
    progress_id = str(uuid.uuid4())
    assert inbox_key("acme:worker", progress_id) == vector["inbox_key"].format(
        key_prefix="acme:worker", progress_id=progress_id
    )
    assert inbox_pending_key("acme:worker") == vector["inbox_pending_key"].format(
        key_prefix="acme:worker"
    )
    example = vector["inbox_entry_example"]
    command = ProgressCommand.model_validate_json(example["command"])
    body = turn_progress.TurnProgressBody(
        **command.model_dump(exclude_none=True),
        generation=int(example["generation"]),
        seq=int(example["seq"]),
    )
    fields = turn_progress.inbox_fields(body)
    assert set(fields) == set(example)
    assert ProgressCommand.model_validate_json(fields["command"]) == command
    assert (fields["generation"], fields["seq"]) == (
        example["generation"],
        example["seq"],
    )
