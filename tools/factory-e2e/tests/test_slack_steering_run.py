from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest
import scripted_kind as sk
from slack_steering_model import ANCHOR, INSTRUCTION, SteeringRouter
from slack_steering_run import SlackSteeringRun, verify_consumers, verify_revision

NONCE = "a1" * 16


def _frame(reply: dict[str, str]) -> dict[str, Any]:
    return {
        "role": "user",
        "content": (
            "[platform-sender curie-sender-boundary]\nevent: job\nchannel: github\nperson: none\n"
            "role: scheduled run\n[user-message curie-sender-boundary]\n"
            "https://example.com/acme-corp/acme-bot/pull/1\n\nCurie pull request follow-up.\n"
            + json.dumps(reply)
            + "\n[end-user-message curie-sender-boundary]"
        ),
    }


def test_model_edits_only_for_this_turns_received_steering() -> None:
    module = sk._module("steer_model_server", sk.REPO_ROOT / "tools/model-script/model_script.py")
    model = module.ModelScript(sk.TRANSCRIPT)
    router = SteeringRouter()
    router.install(model, module)
    model.start()
    reply = {"kind": "slack_mention", "body": "<@U0EXAMPLE9> " + INSTRUCTION.format(NONCE)}
    messages = [_frame(reply)]
    try:
        for expected in (
            "Skill",
            "mcp__curie__get_issue",
            "Agent",
            "mcp__curie__report_progress",
            "Read",
            "Edit",
        ):
            status, answer = fe.http_json(
                "POST", model.base_url + "/v1/messages", body={"messages": messages}
            )
            assert status == 200
            (tool,) = answer["content"]
            assert tool["name"] == expected
            if expected == "Agent":
                assert tool["input"]["run_in_background"] is False
            messages += [
                {"role": "assistant", "content": [tool]},
                {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": tool["id"], "content": ANCHOR}
                    ],
                },
                {"role": "system", "content": [{"type": "text", "text": "Tool use completed."}]},
            ]
        assert tool["input"]["new_string"] == f"Run note {NONCE}\n{ANCHOR}"
        assert router.received == [NONCE]
        for missing in (
            {"kind": "slack_mention", "body": "Update the README."},
            {"kind": "pull_request_body", "body": reply["body"]},
        ):
            status, _ = fe.http_json(
                "POST",
                model.base_url + "/v1/messages",
                body={"messages": [*messages, _frame(missing)]},
            )
            assert status == 422
        assert router.received == [NONCE]
    finally:
        with pytest.raises(module.UnexpectedRequest, match="2 request"):
            model.close()


def test_scripted_cli_selects_steering_and_keeps_publication_driver(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    selected: list[type] = []

    def execute(run: sk.ScriptedRun) -> None:
        selected.append(type(run))

    monkeypatch.setattr(sk, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sk.ScriptedRun, "execute", execute)
    monkeypatch.setattr(sk.ScriptedRun, "cleanup", lambda run: [])
    monkeypatch.setattr(sk.ScriptedRun, "one_ticket_verdict_error", lambda run: None)
    for name in ("publication", "slack-steering"):
        args = fe.parse_args(
            ["scripted", "--scenario", name, "--build-images", "--transcript", str(sk.TRANSCRIPT)]
        )
        assert sk.main(args) == 0
    assert selected == [sk.ScriptedRun, SlackSteeringRun]


def test_publication_requires_exact_edit_on_the_same_pull_request_and_branch() -> None:
    before = (sk.FIXTURE / "README.md").read_bytes()
    expected = before.replace(ANCHOR.encode(), f"Run note {NONCE}\n{ANCHOR}".encode())
    pr = (41, "curie/example")
    verify_revision(before, expected, b"README.md\n", NONCE, pr, pr)
    for after, paths, current in (
        (before, b"README.md", pr),
        (expected + b"another change\n", b"README.md", pr),
        (expected.replace(b"standard library only", b"changed"), b"README.md", pr),
        (expected, b"README.md\nside-effect.txt", pr),
        (expected, b"README.md", (42, "curie/example")),
        (expected, b"README.md", (41, "curie/other")),
    ):
        with pytest.raises(sk.ScriptedScenarioError):
            verify_revision(before, after, paths, NONCE, pr, current)


def test_each_accepted_reply_has_one_distinct_consuming_publication() -> None:
    url = "https://example.com/acme-corp/acme-bot/pull/41"
    pubs = [
        {"request": request, "url": url, "lineage": "one", "prior_head": head}
        for request, head in (("initial", "base"), ("early", "initial"), ("late", "early"))
    ]
    stamps = ["1800000500.000001", "1800000500.000002"]
    rows = [
        {"ts": stamp, "state": "applied", "request": request}
        for stamp, request in zip(stamps, ("early", "late"), strict=True)
    ]
    verify_consumers(pubs, rows, stamps, url, "early")
    for observed, accepted in (
        ([*pubs, pubs[-1]], rows),
        ([*pubs[:-1], pubs[-2]], rows),
        ([*pubs[:-1], pubs[-1] | {"lineage": "other"}], rows),
        ([*pubs[:-1], pubs[-1] | {"prior_head": "initial"}], rows),
        (pubs, [rows[0], rows[1] | {"request": "early"}]),
        (pubs, [rows[0], rows[1] | {"state": "queued"}]),
    ):
        with pytest.raises(sk.ScriptedScenarioError):
            verify_consumers(observed, accepted, stamps, url, "early")
