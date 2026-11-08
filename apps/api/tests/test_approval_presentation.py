"""Presentation keeps exact approval authority and caller supplied values."""

import json
import uuid
from datetime import UTC, datetime

from curie_api.models import Approval
from curie_api.schemas.approvals import ApprovalOut


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
    from curie_runner.approval_wording import describe_approval as runner_describe
    from curie_worker.approval_wording import describe_approval as worker_describe

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
    assert runner_describe(tool, arguments) == display
    assert worker_describe(tool, arguments) == display
    assert out["summary"] == summary
    assert out["granted_tool"] == tool
    assert row.granted_arguments == arguments


def test_api_approval_metadata_sentence_boundaries_preserve_exact_grant() -> None:
    """@spec plain-approval-wording: the serialized consumer preserves authority."""
    from pathlib import Path

    cases = json.loads(
        (Path(__file__).resolve().parents[3] / "tests/vectors/user-action-wording.json").read_text()
    )["metadata_references"]
    for case in cases:
        arguments = {"file_name": case["tool"] + ".json"}
        row = approval(case["summary"], case["tool"], arguments)
        out = ApprovalOut.model_validate(row).model_dump()
        assert out["display_summary"] == case["display"], case
        assert out["summary"] == case["summary"]
        assert out["granted_tool"] == case["tool"]
        assert row.granted_arguments == arguments


# --- ADR 0271: the top-level approval_ticket member is metadata, not preview ---

_TICKET = "cat1." + "T" * 3000 + ".sig"
_WORDING_FILES = (
    "apps/api/src/curie_api/approval_wording.py",
    "apps/worker/src/curie_worker/approval_wording.py",
    "runner/src/curie_runner/approval_wording.py",
)


def _describers() -> list:
    from curie_api.approval_wording import describe_approval as api_describe
    from curie_runner.approval_wording import describe_approval as runner_describe
    from curie_worker.approval_wording import describe_approval as worker_describe

    return [api_describe, worker_describe, runner_describe]


def test_describe_approval_leaves_out_only_the_top_level_ticket() -> None:
    tool = "mcp__acme__execute_approved"
    arguments = {"note": "synthetic", "approval_ticket": _TICKET}
    for describe in _describers():
        text = describe(tool, arguments)
        assert text == "Approve execute approved. Note: synthetic"
        assert "icket" not in text
        assert _TICKET not in text


def test_a_nested_member_named_approval_ticket_still_renders() -> None:
    tool = "mcp__acme__execute_approved"
    nested = {"approval_ticket": "nested-value"}
    for describe in _describers():
        text = describe(tool, {"payload": nested, "approval_ticket": _TICKET})
        assert text == "Approve execute approved. Payload: " + json.dumps(nested)


def test_a_ticket_only_argument_object_reads_as_no_details() -> None:
    for describe in _describers():
        assert "approval_ticket" not in describe("mcp__acme__go", {"approval_ticket": _TICKET})
        assert _TICKET not in describe("mcp__acme__go", {"approval_ticket": _TICKET})


def test_describe_approval_does_not_mutate_its_input() -> None:
    for describe in _describers():
        arguments = {"note": "synthetic", "approval_ticket": _TICKET}
        describe("mcp__acme__execute_approved", arguments)
        assert arguments == {"note": "synthetic", "approval_ticket": _TICKET}


def test_approval_display_prefers_stored_arguments_and_omits_the_ticket() -> None:
    from curie_api.approval_wording import approval_display as api_display
    from curie_runner.approval_wording import approval_display as runner_display
    from curie_worker.approval_wording import approval_display as worker_display

    tool = "mcp__acme__execute_approved"
    stored = {"note": "stored", "approval_ticket": _TICKET}
    summary = "Tool call awaiting approval: " + tool + " " + json.dumps({"note": "machine"})
    for display in (api_display, worker_display, runner_display):
        assert display(summary, tool, stored) == "Approve execute approved. Note: stored"
        assert stored["approval_ticket"] == _TICKET
        # A machine summary carrying a ticket renders without it too.
        with_ticket = "Tool call awaiting approval: " + tool + " " + json.dumps(stored)
        assert display(with_ticket, tool, None) == "Approve execute approved. Note: stored"


def test_the_preview_bound_counts_rendered_values_and_never_the_ticket() -> None:
    tool = "mcp__acme__execute_approved"
    notice = "Details exceed this preview; review the original request before approving."
    for describe in _describers():
        long_note = {"note": "n" * 3000, "approval_ticket": _TICKET}
        text = describe(tool, long_note)
        assert text.endswith(notice)
        assert _TICKET not in text
        assert len(text) == 2400 + len("… " + notice)
        # A long ticket beside a short body stays under the limit: no truncation.
        short = describe(tool, {"note": "ok", "approval_ticket": "x" * 5000})
        assert short == "Approve execute approved. Note: ok"
        assert notice not in short


def test_the_three_wording_siblings_are_byte_identical() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    contents = {name: (root / name).read_bytes() for name in _WORDING_FILES}
    assert len(set(contents.values())) == 1, sorted(contents)


def test_api_display_summary_omits_the_ticket_while_the_grant_keeps_it() -> None:
    tool = "mcp__acme__execute_approved"
    arguments = {"note": "synthetic", "approval_ticket": _TICKET}
    summary = "Tool call awaiting approval: " + tool + " " + json.dumps(arguments)
    row = approval(summary, tool, arguments)
    out = ApprovalOut.model_validate(row).model_dump(mode="json")
    assert out["display_summary"] == "Approve execute approved. Note: synthetic"
    # The exact `summary` record legitimately keeps the raw call text; the
    # presentation fields and the grant arguments must not carry the ticket.
    assert "granted_arguments" not in out
    assert _TICKET not in json.dumps({k: v for k, v in out.items() if k != "summary"})
    assert row.granted_arguments == {"note": "synthetic", "approval_ticket": _TICKET}


# --- ADR 0271 decision 8: a dynamic row describes its stored values ---------------

_DYNAMIC_TOOL = "mcp__access__execute"
_TEAM_SUMMARY = "Approve the synthetic access change for the whole team"


def _selected(summary: str, arguments: dict, *, mode: str) -> Approval:
    row = approval(summary, _DYNAMIC_TOOL, arguments)
    row.gate_kind = "policy"
    row.selection_mode = mode
    return row


def test_dynamic_display_summary_describes_stored_values_and_ordinary_keeps_summary() -> None:
    """@spec REQUEST-APPROVAL-8: the stored grant, not the summary text, is what is shown."""
    from curie_api.approval_wording import describe_approval

    stored = {"account": "acct-example-1", "approval_ticket": _TICKET, "note": "stored"}
    expected = "Approve execute. Account: acct-example-1; Note: stored"
    assert describe_approval(_DYNAMIC_TOOL, stored) == expected
    row = _selected(_TEAM_SUMMARY, stored, mode="dynamic")
    assert row.display_summary == expected
    out = ApprovalOut.model_validate(row).model_dump(mode="json")
    assert out["display_summary"] == expected
    assert out["summary"] == _TEAM_SUMMARY
    assert _TICKET not in json.dumps(out)
    assert row.granted_arguments == stored
    # Neighbour: the same stored values on an ordinary row keep today's display.
    ordinary = _selected(_TEAM_SUMMARY, stored, mode="ordinary")
    assert ordinary.display_summary == _TEAM_SUMMARY
    assert ApprovalOut.model_validate(ordinary).model_dump()["display_summary"] == _TEAM_SUMMARY


def test_dynamic_display_ignores_a_summary_naming_other_arguments() -> None:
    """@spec REQUEST-APPROVAL-8: neither model prose nor a machine summary replaces storage."""
    stored = {"approval_ticket": _TICKET, "note": "stored"}
    machine = (
        "Tool call awaiting approval: " + _DYNAMIC_TOOL + " " + json.dumps({"note": "model"})
    )
    for summary in (machine, "Approve note model via mcp__access__execute", "Approve it"):
        row = _selected(summary, stored, mode="dynamic")
        assert row.display_summary == "Approve execute. Note: stored", summary
    # Neighbour: an ordinary row with prose keeps that prose, normalized as today.
    ordinary = _selected("Approve note model via mcp__access__execute", stored, mode="ordinary")
    assert ordinary.display_summary == "Approve note model via execute"


def test_dynamic_display_keeps_the_bounded_preview_notice() -> None:
    """@spec REQUEST-APPROVAL-8: no claim to display arbitrarily large arguments in full."""
    notice = "Details exceed this preview; review the original request before approving."
    stored = {"approval_ticket": _TICKET, "note": "n" * 3000}
    display = _selected(_TEAM_SUMMARY, stored, mode="dynamic").display_summary
    assert display.startswith("Approve execute. Note: nnn")
    assert display.endswith("… " + notice)
    assert len(display) == 2400 + len("… " + notice)
    assert _TICKET not in display
    assert _selected(_TEAM_SUMMARY, stored, mode="ordinary").display_summary == _TEAM_SUMMARY
