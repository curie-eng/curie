"""Worker half of the frozen sealed reply vector (ACTION-EXECUTOR-9).

@spec ACTION-EXECUTOR-9. The worker records ``prior_state`` exactly when the
frame is not ``redacted``, ``prior`` validates as a sealed envelope, and
neither ``target`` nor ``version`` carries the shared redaction placeholder
prefix; it records ``version`` as the action's ``post_version``. The runner's
redactor (``runner/tests/test_sealed_snapshot_reply_vector.py``) decides in
another image what frame the worker receives, so each vector's worker input is
the frame the runner verdict describes: ``withheld`` replay inputs arrive as
null and ``redacted`` is the runner's flag. Read through the production
recording path, ``ActionClient.complete``, which posts what ``_snapshot``
parsed to the ledger.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from aci_protocol import SideEffectFlag
from curie_worker.actions import ActionClient

pytestmark = pytest.mark.anyio

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "sealed-snapshot-reply.json"
    ).read_text("utf-8")
)
_CASE_KEYS = {
    "name",
    "reply",
    "held_secrets",
    "expand_ciphertext",
    "envelope_valid",
    "runner",
    "worker",
}
_REPLAY_INPUTS = ("prior", "version", "target")


def _reply(case: dict[str, Any]) -> dict[str, Any]:
    reply = copy.deepcopy(case["reply"])
    expand = case.get("expand_ciphertext")
    if expand is not None:
        reply["prior"]["ciphertext"] = expand["fill"] * expand["length"] + expand["suffix"]
    return reply


def _emitted(case: dict[str, Any]) -> SideEffectFlag:
    """The closing frame as the runner verdict says it leaves the sandbox."""

    result = _reply(case)
    runner = case["runner"]
    if runner["replay_inputs"] == "withheld":
        for key in _REPLAY_INPUTS:
            result[key] = None
    for field in runner.get("scrubbed", []):
        result[field] = "[REDACTED:secret_assignment]"
    return SideEffectFlag(
        tool="mcp__example-scale__scale",
        call_id="toolu_example",
        arguments={"replicas": 5},
        result=result,
        redacted=runner["redacted"],
    )


def test_the_vector_has_only_known_keys() -> None:
    assert set(_VECTOR) == {"comment", "envelope", "vectors"}
    for case in _VECTOR["vectors"]:
        unknown = set(case) - _CASE_KEYS
        assert not unknown, (
            f"{case['name']}: unknown keys {sorted(unknown)}. Teach them to this test, "
            "apps/api/tests/test_sealed_snapshot_reply_vector.py and "
            "runner/tests/test_sealed_snapshot_reply_vector.py."
        )
        assert set(case["worker"]) == {"records_snapshot"}


@pytest.mark.parametrize("case", _VECTOR["vectors"], ids=lambda case: case["name"])
async def test_the_worker_records_a_snapshot_exactly_as_frozen(case: dict[str, Any]) -> None:
    """@spec ACTION-EXECUTOR-9: the completion body the ledger receives."""

    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "a1", "status": "succeeded"})

    frame = _emitted(case)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1", frame
        )

    (body,) = seen
    reply = _reply(case)
    if case["worker"]["records_snapshot"]:
        assert body["prior_state"] == reply["prior"]
        assert body["post_version"] == reply["version"]
        assert body["target"] == reply["target"]
    else:
        assert body["prior_state"] is None
