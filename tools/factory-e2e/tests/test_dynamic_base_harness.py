"""Harness self-checks. Product acceptance lives in test_c4_dynamic_base.py."""

from __future__ import annotations

import importlib.util
import json
import ssl
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx  # noqa: E402
import pytest  # noqa: E402
from c4_dynamic_base import (  # noqa: E402
    CHANGE,
    assert_ci_still_owns_request,
    dynamic_model_type,
    dynamic_stub_type,
)


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


@pytest.mark.parametrize("prior_failed", [False, True], ids=["verified-history", "failed-history"])
def test_new_objective_requires_fresh_verification_after_a_completed_turn(
    model: Any, prior_failed: bool
) -> None:
    # Anthropic's assistant tool_use and matching user tool_result shapes:
    # https://platform.claude.com/docs/en/agents-and-tools/tool-use/implement-tool-use
    model.mode("nochange")
    earlier = [{"role": "user", "content": "Verify the example repository on its original base."}]
    prior_response = reply(model, earlier)
    assert prior_response["stop_reason"] == "tool_use"
    prior_tool = prior_response["content"][0]
    assert prior_tool["name"] == "Bash"
    earlier.extend(
        [
            {"role": "assistant", "content": prior_response["content"]},
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": prior_tool["id"],
                        "is_error": prior_failed,
                        "content": "Earlier repository verification failed."
                        if prior_failed
                        else "C4_HEAD="
                        + "a" * 40
                        + '\nC4_TARGET=main\nC4_VALUE=VALUE = "factory"\n',
                    }
                ],
            },
        ]
    )
    prior_final = reply(model, earlier)
    assert prior_final["stop_reason"] == "end_turn"
    assert prior_final["content"][0]["text"].startswith(
        "Could not complete:" if prior_failed else "No changes needed:"
    )
    prior_observations = (
        []
        if prior_failed
        else [
            {
                "tool": prior_tool["id"],
                "head": "a" * 40,
                "target": "main",
                "value": 'VALUE = "factory"',
            }
        ]
    )
    assert model.observations() == prior_observations
    messages = [
        *earlier,
        {"role": "assistant", "content": prior_final["content"]},
        {
            "role": "user",
            "content": "The base moved. Verify the current repository on its selected base.",
        },
    ]

    current_response = reply(model, messages)

    assert current_response["stop_reason"] == "tool_use"
    current_tool = current_response["content"][0]
    assert current_tool["name"] == "Bash"
    assert current_tool["id"] != prior_tool["id"]
    assert model.observations() == prior_observations
    messages.extend(
        [
            {"role": "assistant", "content": current_response["content"]},
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": current_tool["id"],
                        "is_error": False,
                        "content": "C4_HEAD="
                        + "b" * 40
                        + '\nC4_TARGET=next\nC4_VALUE=VALUE = "factory"\n',
                    }
                ],
            },
        ]
    )

    current_final = reply(model, messages)

    assert current_final["stop_reason"] == "end_turn"
    assert current_final["content"][0]["text"].startswith("No changes needed:")
    assert model.observations() == [
        *prior_observations,
        {
            "tool": current_tool["id"],
            "head": "b" * 40,
            "target": "next",
            "value": 'VALUE = "factory"',
        },
    ]


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


@pytest.fixture
def dynamic_ci_response(tmp_path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    from curie_telemetry.redact import redact_text

    tools = Path(__file__).resolve().parents[2]
    source = tools / "github-stub/github_stub.py"
    spec = importlib.util.spec_from_file_location("dynamic_github_selftest", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    recording = json.loads((tools / "github-stub/recordings/curie-pr-3400.json").read_text())
    stub = dynamic_stub_type(module, redact=redact_text)(
        tmp_path / "github",
        recording,
        seed_tree=tools / "factory-e2e/fixtures/unitconv",
        issue_file=tools / "factory-e2e/fixtures/unitconv-issue.md",
    )
    try:
        stub.start()
        head = stub.heads()["next"]
        stub.check(head, "success", target="next")
        with httpx.Client(
            base_url=stub.base_url,
            verify=ssl.create_default_context(cafile=str(stub.ca_file)),
            trust_env=False,
        ) as client:
            response = client.get(f"/api/v3/repos/acme-corp/acme-bot/commits/{head}/check-runs")
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["total_count"] == len(payload["check_runs"]) == 1
        yield head, payload
    finally:
        stub.close()


@pytest.mark.parametrize("unrelated_app", [False, True], ids=["generated-native", "unrelated-app"])
def test_dynamic_python_check_passes_the_factory_ci_gate_only_for_native_ci(
    dynamic_ci_response: tuple[str, dict[str, Any]], unrelated_app: bool
) -> None:
    from curie_api import factory_ci
    from curie_api.forges.github.ci import CiDetail
    from curie_api.forges.github.code_host import normalize_checks
    from curie_api.forges.types import CheckState

    # Check-run app identities follow the GitHub REST contract and the captured
    # lifecycle in tools/github-stub/recordings/curie-pr-3400.json.
    # https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
    head, payload = dynamic_ci_response
    runs = payload["check_runs"]
    if unrelated_app:
        runs = [{**run, "app": {**run["app"], "slug": "example-ci"}} for run in runs]
    detail = CiDetail(state="observed", reason=None, head_sha=head, check_runs=runs)
    checks = normalize_checks(detail, head)
    assert len(checks) == 1
    assert checks[0].head_sha == head
    assert checks[0].state is CheckState.SUCCESS
    now = datetime.now(UTC)

    verdict = factory_ci.decide(
        factory_ci.CiView(head, checks),
        now=now,
        published_at=now - timedelta(minutes=3),
        execution_deadline=now + timedelta(hours=1),
        ci_wait_seconds=1200,
        changed_paths=[CHANGE],
        python_ci=factory_ci.PythonCiPolicy(check="dynamic-ci", paths=(CHANGE,)),
        metadata_ci=None,
        host_required_checks=("dynamic-ci",),
    )

    assert verdict.kind == ("unverified" if unrelated_app else "green"), verdict
    assert checks[0].native is (not unrelated_app)
    assert verdict.reason == ("required_python_ci_unrelated" if unrelated_app else None)
