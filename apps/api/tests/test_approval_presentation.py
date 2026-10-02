"""Presentation keeps exact approval authority and caller supplied values."""

import json
import uuid
from datetime import UTC, datetime

from curie_api.models import Approval
from curie_api.schemas import ApprovalOut


def approval(summary: str, tool: str | None = None, arguments: dict | None = None) -> Approval:
    return Approval(
        id=uuid.uuid4(), agent_id=None, conversation_id="thread-example",
        author="U0EXAMPLE1", summary=summary, reply_kind="slack",
        reply_channel="C0EXAMPLE1", reply_placeholder=None, dedupe_key="display-example",
        gate_kind="permission" if tool else None, granted_tool=tool,
        granted_arguments=arguments, status="pending", created_at=datetime.now(UTC),
    )


def test_api_approval_exposes_plain_display_without_changing_exact_record() -> None:
    tool = "mcp__plugin_acme_files__file_attachment"
    arguments = {"file_name": "example.pdf", "destination": "Approved"}
    summary = "Tool call awaiting approval: " + tool + " " + json.dumps(arguments)
    row = approval(summary, tool, arguments)
    out = ApprovalOut.model_validate(row).model_dump()
    assert out["display_summary"] == "Approve file attachment. Destination: Approved; File name: example.pdf"
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
