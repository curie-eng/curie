"""Harness self-checks. Product acceptance lives in test_c4_dynamic_base.py."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402
from c4_dynamic_base import assert_ci_still_owns_request, dynamic_model_type  # noqa: E402


def test_pending_final_head_rejects_old_target_success() -> None:
    transition = {"request_id": "current", "ci_settled_at": None}
    assert_ci_still_owns_request({"id": "current", "status": "running"}, transition)
    with pytest.raises(AssertionError, match="old-target or pending CI"):
        assert_ci_still_owns_request({"id": "current", "status": "completed"}, transition)
    with pytest.raises(AssertionError, match="another execution request"):
        assert_ci_still_owns_request({"id": "old", "status": "running"}, transition)
    with pytest.raises(AssertionError, match="received a CI settlement"):
        assert_ci_still_owns_request(
            {"id": "current", "status": "running"},
            {**transition, "ci_settled_at": "2026-01-01T00:00:00Z"},
        )


@pytest.fixture
def model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    source = Path(__file__).resolve().parents[2] / "model-script/model_script.py"
    spec = importlib.util.spec_from_file_location("dynamic_model_selftest", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    transcript = tmp_path / "transcript.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": []}))
    instance = dynamic_model_type(module)(transcript)
    try:
        yield instance
    finally:
        instance.close()


def reply(model: Any, messages: list[dict[str, Any]]) -> dict[str, Any]:
    return model._match("POST", "/v1/messages", json.dumps({"messages": messages}).encode())[
        "message"
    ]


def test_scripted_nochange_requires_successful_actual_tool_output(model: Any) -> None:
    model.mode("nochange")
    first = [{"role": "user", "content": "Verify the example repository."}]
    response = reply(model, first)
    assert response["stop_reason"] == "tool_use"
    tool = response["content"][0]
    assert tool["name"] == "Bash"
    assert model.observations() == []
    messages = [
        *first,
        {"role": "assistant", "content": response["content"]},
        {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": tool["id"],
                    "is_error": False,
                    "content": "C4_HEAD="
                    + "a" * 40
                    + '\nC4_TARGET=next\nC4_VALUE=VALUE = "factory"\n',
                }
            ],
        },
    ]
    verified = reply(model, messages)
    assert verified["stop_reason"] == "end_turn"
    assert model.observations() == [
        {"tool": tool["id"], "head": "a" * 40, "target": "next", "value": 'VALUE = "factory"'}
    ]
    messages[-1]["content"][0]["is_error"] = True
    failed = reply(model, messages)
    assert failed["content"][0]["text"].startswith("Could not complete:")


def test_unissued_output_does_not_claim_a_checkout_observation(model: Any) -> None:
    model.mode("nochange")
    response = reply(
        model,
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "unissued",
                        "is_error": False,
                        "content": "C4_HEAD=" + "b" * 40 + "\nC4_TARGET=next\nC4_VALUE=synthetic\n",
                    },
                ],
            }
        ],
    )
    assert response["stop_reason"] == "tool_use"
    assert model.observations() == []
