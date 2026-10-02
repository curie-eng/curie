"""Presentation keeps exact approval authority and caller supplied values."""

import json
import uuid
from datetime import UTC, datetime

from curie_api.models import Approval
from curie_api.schemas import ApprovalOut


def approval(summary: str, tool: str | None = None, arguments: dict | None = None) -> Approval:
    return Approval(
        id=uuid.uuid4(),
        agent_id=None,
        conversation_id="thread-example",
        author="U0EXAMPLE1",
        summary=summary,
        reply_kind="slack",
        reply_channel="C0EXAMPLE1",
        reply_placeholder=None,
        dedupe_key="display-example",
        gate_kind="permission" if tool else None,
        granted_tool=tool,
        granted_arguments=arguments,
        status="pending",
        created_at=datetime.now(UTC),
    )


def test_api_approval_exposes_plain_display_without_changing_exact_record() -> None:
    tool = "mcp__plugin_acme_files__file_attachment"
    arguments = {"file_name": "example.pdf", "destination": "Approved"}
    summary = "Tool call awaiting approval: " + tool + " " + json.dumps(arguments)
    row = approval(summary, tool, arguments)
    out = ApprovalOut.model_validate(row).model_dump()
    assert (
        out["display_summary"]
        == "Approve file attachment. Destination: Approved; File name: example.pdf"
    )
    assert out["summary"] == summary
    assert out["granted_tool"] == tool
    assert "granted_arguments" not in out
    assert row.granted_arguments == arguments


def test_api_legacy_approval_has_plain_display_without_inventing_authority() -> None:
    row = approval('Tool call awaiting approval: mcp__acme__write_file {"path": "example.txt"}')
    out = ApprovalOut.model_validate(row).model_dump()
    assert out["display_summary"] == "Approve write file. Path: example.txt"
    assert out["gate_kind"] is None and out["granted_tool"] is None


def test_api_policy_approval_retains_the_meaningful_request() -> None:
    row = approval("Approve the revised $4.4M budget")
    assert ApprovalOut.model_validate(row).model_dump()["display_summary"] == row.summary


def test_approval_label_siblings_follow_the_shared_presentation_vector() -> None:
    from pathlib import Path

    from curie_api.approval_wording import action_label as api_label
    from curie_runner.approval_wording import action_label as runner_label
    from curie_worker.approval_wording import action_label as worker_label

    vectors = json.loads(
        (Path(__file__).resolve().parents[3] / "tests/vectors/user-action-wording.json").read_text()
    )["vectors"]
    for case in vectors:
        assert api_label(case["tool"]) == case["label"]
        assert runner_label(case["tool"]) == case["label"]
        assert worker_label(case["tool"]) == case["label"]


def test_approval_nested_document_keys_are_literal_data_and_grant_stays_exact() -> None:
    tool = "mcp__acme__file_attachment"
    nested = {
        "mcp__acme__file_attachment": "draft",
        "example.pdf": "literal",
        "customer_id": "keep",
        "record_values": ["1", 1, True, None, {"customer_id": "keep"}, "문서"],
    }
    arguments = {"file_contents": nested}
    summary = "Tool call awaiting approval: " + tool + " " + json.dumps(arguments)
    row = approval(summary, tool, arguments)
    out = ApprovalOut.model_validate(row).model_dump()
    display = out["display_summary"]
    prefix = "Approve file attachment. File contents: "
    assert display == prefix + json.dumps(nested, ensure_ascii=False)
    assert json.loads(display.removeprefix(prefix)) == nested
    assert out["summary"] == summary
    assert out["granted_tool"] == tool
    assert row.granted_arguments == arguments
