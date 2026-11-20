"""The stored-arguments-card step checks, judged on recorded shapes.

Covers the pure checks the ``stored-arguments-card`` scenario (ADR 0271,
decision 8, S0 to S3) judges its steps with: the card, notice and preview text
requirement, the dynamic API row requirement, the bounded preview requirement,
and the single dynamic row selection. Synthetic ids and a clearly fake ticket.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import identity_scenarios_stored_card as card
import pytest
from identity_scenarios import StepFailed

TOOL = "search_dashboards"
CONVERSATION = "ie2e-conv-0001"
OTHER_CONVERSATION = "ie2e-conv-0002"
STORED = "stored-marker-7f3a91"
TEMPLATE = "template-marker-c4d2e8"
# Clearly fake; the real thing is `cat1.` plus base64url of a signed payload.
TICKET = "cat1.FAKETICKETFAKETICKET0123456789abcdefghijklmnopqrstuvwxyz-_ABCDEFGHIJKLMNOP"
LIMIT = 2400
NOTICE = "… Details exceed this preview; review the original request before approving."


def row(
    *,
    ident: str = "apr_0001",
    mode: str = "dynamic",
    status: str = "pending",
    tool: str = TOOL,
    arguments: Any = None,
    conversation: str = CONVERSATION,
) -> dict[str, Any]:
    if arguments is None:
        arguments = {"query": STORED, "approval_ticket": TICKET}
    return {
        "id": ident,
        "agent_id": "ie2e-on",
        "conversation_id": conversation,
        "selection_mode": mode,
        "status": status,
        "granted_tool": tool,
        "granted_arguments": arguments,
        "display_summary": f"Run {TOOL} on netpol-probe. Query: {STORED}",
    }


CARD_TEXT = (
    "Approval needed. Requested by alice@example.test. "
    f"Search dashboards on netpol-probe. Query: {STORED}\n"
    "Approve or reject below."
)


# --- require_stored_text ------------------------------------------------------------


def test_stored_text_passes_and_reports_only_length() -> None:
    result = card.require_stored_text(
        CARD_TEXT, stored_marker=STORED, template_marker=TEMPLATE, ticket=TICKET
    )
    assert result == {"length": len(CARD_TEXT), "stored_marker": True}
    assert TICKET not in json.dumps(result)


def test_stored_text_fails_without_stored_marker() -> None:
    with pytest.raises(StepFailed) as err:
        card.require_stored_text(
            "Approval needed. Query: something else",
            stored_marker=STORED,
            template_marker=TEMPLATE,
            ticket=TICKET,
        )
    assert "stored" in str(err.value).lower()


def test_stored_text_fails_when_template_marker_present() -> None:
    with pytest.raises(StepFailed) as err:
        card.require_stored_text(
            f"{CARD_TEXT} Summary: {TEMPLATE}",
            stored_marker=STORED,
            template_marker=TEMPLATE,
            ticket=TICKET,
        )
    assert "template" in str(err.value).lower()
    assert TICKET not in str(err.value)


def test_stored_text_fails_on_ticket_prefix() -> None:
    with pytest.raises(StepFailed) as err:
        card.require_stored_text(
            f"{CARD_TEXT} cat1.something",
            stored_marker=STORED,
            template_marker=TEMPLATE,
            ticket=TICKET,
        )
    assert "cat1." in str(err.value)
    assert TICKET not in str(err.value)


@pytest.mark.parametrize("offset", [0, 7, 19, len(TICKET) - 24])
def test_stored_text_fails_on_any_24_char_ticket_slice(offset: int) -> None:
    # Offset 0 starts with `cat1.`; the others have no prefix, so only the
    # slice check can catch them.
    leaked = TICKET[offset : offset + 24]
    with pytest.raises(StepFailed) as err:
        card.require_stored_text(
            f"{CARD_TEXT} {leaked}",
            stored_marker=STORED,
            template_marker=TEMPLATE,
            ticket=TICKET,
        )
    assert leaked not in str(err.value)
    assert TICKET not in str(err.value)


def test_stored_text_tolerates_a_short_ticket_overlap() -> None:
    # 23 characters of the ticket is below the slice width and not a leak.
    text = f"{CARD_TEXT} {TICKET[10:33]}"
    result = card.require_stored_text(
        text, stored_marker=STORED, template_marker=TEMPLATE, ticket=TICKET
    )
    assert result["stored_marker"] is True


@pytest.mark.parametrize("label", ["approval_ticket", "Approval ticket", "APPROVAL TICKET"])
def test_stored_text_fails_on_ticket_field_label(label: str) -> None:
    with pytest.raises(StepFailed) as err:
        card.require_stored_text(
            f"{CARD_TEXT} {label}: [redacted]",
            stored_marker=STORED,
            template_marker=TEMPLATE,
            ticket=TICKET,
        )
    assert "label" in str(err.value).lower()


# --- require_dynamic_row ------------------------------------------------------------


def test_dynamic_row_passes_with_fingerprint_only() -> None:
    result = card.require_dynamic_row(row(), tool=TOOL, stored_marker=STORED)
    fingerprint = hashlib.sha256(TICKET.encode()).hexdigest()[:16]
    assert result == {
        "id": "apr_0001",
        "selection_mode": "dynamic",
        "ticket_fingerprint": fingerprint,
    }
    assert TICKET not in json.dumps(result)
    assert STORED not in json.dumps(result)


@pytest.mark.parametrize(
    ("changes", "needle"),
    [
        ({"mode": "static"}, "selection_mode"),
        ({"status": "approved"}, "status"),
        ({"tool": "other_tool"}, "granted_tool"),
        ({"arguments": ["query", STORED]}, "granted_arguments"),
        ({"arguments": {"query": TEMPLATE, "approval_ticket": TICKET}}, "query"),
        ({"arguments": {"approval_ticket": TICKET}}, "query"),
        ({"arguments": {"query": STORED}}, "approval_ticket"),
        ({"arguments": {"query": STORED, "approval_ticket": 7}}, "approval_ticket"),
        ({"arguments": {"query": STORED, "approval_ticket": "v9.nope"}}, "cat1."),
    ],
)
def test_dynamic_row_fails_each_condition(changes: dict[str, Any], needle: str) -> None:
    with pytest.raises(StepFailed) as err:
        card.require_dynamic_row(row(**changes), tool=TOOL, stored_marker=STORED)
    assert needle in str(err.value)
    assert TICKET not in str(err.value)


# --- require_bounded_preview --------------------------------------------------------


def test_bounded_preview_passes_at_the_exact_bound() -> None:
    text = "x" * (LIMIT + len(NOTICE))
    assert card.require_bounded_preview(text, LIMIT) == LIMIT + len(NOTICE)


def test_bounded_preview_passes_for_short_text() -> None:
    assert card.require_bounded_preview("short", LIMIT) == 5


def test_bounded_preview_fails_one_over_the_bound() -> None:
    with pytest.raises(StepFailed):
        card.require_bounded_preview("x" * (LIMIT + len(NOTICE) + 1), LIMIT)


def test_bounded_preview_bound_follows_the_limit() -> None:
    assert card.require_bounded_preview("x" * (10 + len(NOTICE)), 10) == 10 + len(NOTICE)
    with pytest.raises(StepFailed):
        card.require_bounded_preview("x" * (11 + len(NOTICE)), 10)


# --- single_dynamic -----------------------------------------------------------------


def test_single_dynamic_returns_the_one_matching_row() -> None:
    mine = row(ident="apr_0001")
    rows = [
        mine,
        row(ident="apr_0002", conversation=OTHER_CONVERSATION),
        row(ident="apr_0003", tool="other_tool"),
    ]
    assert card.single_dynamic(rows, conversation_id=CONVERSATION, tool=TOOL) == mine


def test_single_dynamic_fails_on_zero() -> None:
    rows = [row(conversation=OTHER_CONVERSATION), row(tool="other_tool")]
    with pytest.raises(StepFailed):
        card.single_dynamic(rows, conversation_id=CONVERSATION, tool=TOOL)
    with pytest.raises(StepFailed):
        card.single_dynamic([], conversation_id=CONVERSATION, tool=TOOL)


def test_single_dynamic_fails_on_two() -> None:
    rows = [row(ident="apr_0001"), row(ident="apr_0002")]
    with pytest.raises(StepFailed) as err:
        card.single_dynamic(rows, conversation_id=CONVERSATION, tool=TOOL)
    assert TICKET not in str(err.value)
