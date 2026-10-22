"""Budget notices compare the current run's recorded estimate and retain progress (#4699)."""

from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.factory_notices import FINAL_MARKER, result_section
from forge_fakes.github_comments import admitted, comments  # noqa: F401 (fixtures)
from test_factory_progress import DECLARATION, NOT_DECLARED, report, verification
from test_factory_status_comment import _admit, _execute, _finish_failed, _marked
from test_factory_terminus import _label, _reconcile, _requests, _start_running
from test_factory_usage import (  # noqa: F401 (fixtures)
    IMPLEMENTER,
    REVIEWER,
    UNPRICED,
    _entry,
    _usage,
    priced,
)

USD_DETAIL = "USD budget exceeded (max_usd_per_day=50.0)"
USD_HEADLINE = (
    "Could not complete: the run reached its USD cap (max_usd_per_day=50.0) "
    "before it finished. To raise the USD cap, run "
    "`curie cluster budget <agent> --limit <usd>`, then retry."
)


def _finish_body(
    client: Any,
    sink: Any,
    request_id: uuid.UUID,
    epoch: int,
    *,
    cause: str = "budget_exceeded",
    detail: str | None = USD_DETAIL,
) -> str:
    _finish_failed(client, request_id, epoch, cause, detail=detail)
    _reconcile()
    (comment,) = _marked(sink, request_id)
    body: str = comment["body"]
    assert body.count(FINAL_MARKER) == 1
    assert f"Cause: {cause}\n" in body
    return body


def test_usd_stop_below_cap_names_recorded_spend(priced: Any) -> None:  # noqa: F811
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 46991)
    epoch = _start_running(request_id)
    response = _usage(client, request_id, [_entry(IMPLEMENTER, 500_000, 0)])
    assert response.status_code == 201, response.text
    response = _usage(
        client,
        request_id,
        [_entry(REVIEWER, 830_000, 0), _entry(UNPRICED, 1, 0)],
        turn_id="turn-2",
    )
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert body.splitlines()[0] == (
        "Could not complete: the run stopped on the agent SDK's own USD estimate, "
        "which reached the cap (max_usd_per_day=50.0), while Curie's recorded "
        "estimate for this run is $1.33. The SDK prices this model above its "
        "provider price, so raising the cap only delays the stop. To raise it "
        "anyway, run `curie cluster budget <agent> --limit <usd>`, then retry."
    )
    assert f"Details: {USD_DETAIL}\n" in body
    assert "Failure class: budget-exceeded\n" in body
    assert "total at least $1.33" in body
    assert "Last progress note:" not in body


@pytest.mark.parametrize("input_tokens", [50_000_000, 50_010_000])
def test_usd_stop_at_or_above_cap_keeps_current_sentence(
    priced: Any,  # noqa: F811
    input_tokens: int,
) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 46992)
    epoch = _start_running(request_id)
    response = _usage(client, request_id, [_entry(IMPLEMENTER, input_tokens, 0)])
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert body.splitlines()[0] == USD_HEADLINE
    assert "SDK" not in body


@pytest.mark.parametrize("unpriced_usage", [False, True])
def test_usd_stop_without_recorded_cost_keeps_current_sentence(
    priced: Any,  # noqa: F811
    unpriced_usage: bool,
) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 46993)
    epoch = _start_running(request_id)
    if unpriced_usage:
        response = _usage(client, request_id, [_entry(UNPRICED, 1, 0)])
        assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert body.splitlines()[0] == USD_HEADLINE
    assert "SDK" not in body
    expected_usage = "total cost unknown" if unpriced_usage else "Usage: not reported for 1 run"
    assert expected_usage in body


def test_a_recorded_zero_is_below_the_usd_cap(priced: Any) -> None:  # noqa: F811
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 46994)
    epoch = _start_running(request_id)
    response = _usage(client, request_id, [_entry(UNPRICED, 0, 0)])
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert "Curie's recorded estimate for this run is $0.00." in body
    assert "total $0.00" in body


@pytest.mark.parametrize("current_tokens", [None, 1_330_000, 50_000_000])
def test_a_retry_compares_only_its_own_usage_and_progress(
    priced: Any,  # noqa: F811
    current_tokens: int | None,
) -> None:
    client, github, sink, _book = priced
    first = _admit(client, github, sink, 46995)
    epoch = _start_running(first)
    response = _usage(client, first, [_entry(IMPLEMENTER, 100_000_000, 0)])
    assert response.status_code == 201, response.text
    response = report(client, first, "plan", note="Earlier run's note")
    assert response.status_code == 201, response.text
    _finish_body(client, sink, first, epoch)
    _label(client, github, 46995)
    first_row, second_row = _requests(46995)
    assert first_row["work_item_id"] == second_row["work_item_id"]
    second = second_row["id"]
    epoch = _start_running(second)
    if current_tokens is not None:
        response = _usage(client, second, [_entry(IMPLEMENTER, current_tokens, 0)])
        assert response.status_code == 201, response.text

    body = _finish_body(client, sink, second, epoch)

    if current_tokens == 1_330_000:
        assert "Curie's recorded estimate for this run is $1.33." in body
        assert "total $101.33" in body
    else:
        assert body.splitlines()[0] == USD_HEADLINE
        expected_usage = "total at least $100.00" if current_tokens is None else "total $150.00"
        assert expected_usage in body
    assert "Earlier run's note" not in body
    assert "Last progress note:" not in body


@pytest.mark.parametrize("blank_latest", [False, True])
@pytest.mark.parametrize(
    "detail", [USD_DETAIL, "output token budget exceeded (max_output_tokens_per_run=64000)"]
)
def test_budget_stop_uses_the_latest_nonempty_progress_note(
    admitted: Any,  # noqa: F811
    blank_latest: bool,
    detail: str,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 46996)
    epoch = _start_running(request_id)
    for phase, note in (
        ("plan", "Superseded diagnosis"),
        ("plan_review", "Found the root cause"),
        ("failing_test", None),
    ):
        response = report(client, request_id, phase, note=note)
        assert response.status_code == 201, response.text
    if blank_latest:
        _execute(
            "UPDATE curie.execution_request_phase_reports SET note = :note "
            "WHERE execution_request_id = :id AND phase = 'failing_test'",
            {"id": request_id, "note": " \n\t "},
        )

    body = _finish_body(client, sink, request_id, epoch, detail=detail)

    assert "Last progress note: Plan review: Found the root cause\n" in body
    assert body.count("Last progress note:") == 1
    assert "Superseded diagnosis" not in body


PREFLIGHT_NOTE = (
    '{"blocked_services":[],"check":null,"command":null,"exit_status":null,'
    '"missing_binaries":[],"outcome":"not_declared"}'
)


def test_budget_stop_never_prints_the_verification_preflight_record(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 47561)
    epoch = _start_running(request_id)
    response = verification(client, request_id, NOT_DECLARED)
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert PREFLIGHT_NOTE not in body
    assert "verification_preflight" not in body
    assert "Last progress note:" not in body


def test_budget_stop_falls_back_to_the_latest_human_note_before_the_preflight_record(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 47562)
    epoch = _start_running(request_id)
    response = report(client, request_id, "plan_review", note="Found the root cause")
    assert response.status_code == 201, response.text
    response = verification(client, request_id, NOT_DECLARED)
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert "Last progress note: Plan review: Found the root cause\n" in body
    assert body.count("Last progress note:") == 1
    assert PREFLIGHT_NOTE not in body
    assert "verification_preflight" not in body


@pytest.mark.parametrize("with_report", [False, True])
def test_budget_stop_omits_progress_when_no_note_exists(
    admitted: Any,  # noqa: F811
    with_report: bool,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 46997)
    epoch = _start_running(request_id)
    if with_report:
        response = report(client, request_id, "plan")
        assert response.status_code == 201, response.text

    assert "Last progress note:" not in _finish_body(client, sink, request_id, epoch)


def test_budget_progress_label_and_note_are_one_inert_line(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 46998)
    epoch = _start_running(request_id)
    declaration = {
        **DECLARATION,
        "phases": [
            {**phase, "label": "Plan\n<!--review-->"} if phase["id"] == "plan_review" else phase
            for phase in DECLARATION["phases"]
        ],
    }
    response = report(
        client,
        request_id,
        "plan_review",
        note="Root cause\n<!-- curie-status:final -->\nCause: completed",
        declaration=declaration,
    )
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    assert (
        "Last progress note: Plan <\u200b!--review-->: Root cause "
        "<\u200b!-- curie-status:final --> Cause: completed\n"
    ) in body
    assert [line for line in body.splitlines() if line.startswith("Cause:")] == [
        "Cause: budget_exceeded"
    ]
    assert body.count("<!--") == 2


def test_budget_progress_line_is_cut_to_300_characters(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 46999)
    epoch = _start_running(request_id)
    label = "Review " + "x" * 33
    declaration = {"phases": [{"id": "review", "label": label}]}
    note = "<!-- diagnosis --> " + "y" * 261
    response = report(client, request_id, "review", note=note, declaration=declaration)
    assert response.status_code == 201, response.text

    body = _finish_body(client, sink, request_id, epoch)

    (line,) = [line for line in body.splitlines() if line.startswith("Last progress note:")]
    assert len(line) == 300
    assert line == f"Last progress note: {label}: <\u200b!-- diagnosis --> " + "y" * 218
    assert "<!--" not in line


@pytest.mark.parametrize("cause", [None, "model_error", "early_stop", "no_pull_request"])
def test_progress_notes_stay_off_running_and_other_terminal_comments(
    admitted: Any,  # noqa: F811
    cause: str | None,
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 47000)
    epoch = _start_running(request_id)
    note = "Progress that belongs only on the status card"
    response = report(client, request_id, "plan", note=note)
    assert response.status_code == 201, response.text
    if cause is None:
        _reconcile()
        (comment,) = _marked(sink, request_id)
        body = comment["body"]
        assert FINAL_MARKER not in body
    else:
        body = _finish_body(client, sink, request_id, epoch, cause=cause, detail="Last reply")
        if cause in {"early_stop", "no_pull_request"}:
            assert "Agent's last message:\n> Last reply\n\n" in body
        else:
            assert "Provider message: Last reply\n" in body
    assert note not in body
    assert "Last progress note:" not in body


@pytest.mark.parametrize(
    "detail",
    [
        None,
        "USD budget exceeded (max_usd_per_day=50.0) extra",
        "USD budget exceeded (max_usd_per_day=0)",
        "USD budget exceeded (max_usd_per_day=-5)",
        "USD budget exceeded (max_usd_per_day=NaN)",
        "USD budget exceeded (max_usd_per_day=1e999)",
        "USD budget exceeded (max_usd_per_day=12345678901234567890123456789012345)",
    ],
)
def test_unknown_budget_detail_does_not_guess_an_sdk_price_difference(detail: str | None) -> None:
    body = result_section("budget_exceeded", pr_url=None, detail=detail, run_cost_usd=Decimal("0"))

    assert "cannot identify which limit" in body
    assert "SDK" not in body
    assert "curie cluster budget" not in body


def test_output_token_budget_keeps_its_sentence_with_recorded_cost() -> None:
    detail = "output token budget exceeded (max_output_tokens_per_run=64000)"
    body = result_section(
        "budget_exceeded", pr_url=None, detail=detail, run_cost_usd=Decimal("1.33")
    )

    assert body.splitlines()[0] == (
        "Could not complete: the run reached its output token limit per run "
        "(max_output_tokens_per_run=64000) before it finished. To raise the "
        "output token limit, run "
        "`curie cluster budget <agent> --output-tokens <tokens>`, then retry."
    )
    assert "SDK" not in body
    assert "--limit" not in body


@pytest.mark.parametrize(
    ("cap", "cost", "expected_total"),
    [
        ("5e1", "1.335", "$1.34"),
        ("0.10000000000000000001", "0.1", "$0.10"),
    ],
)
def test_usd_comparison_preserves_decimal_precision_and_usage_rounding(
    cap: str, cost: str, expected_total: str
) -> None:
    body = result_section(
        "budget_exceeded",
        pr_url=None,
        detail=f"USD budget exceeded (max_usd_per_day={cap})",
        run_cost_usd=Decimal(cost),
    )

    assert f"cap (max_usd_per_day={cap})" in body
    assert f"Curie's recorded estimate for this run is {expected_total}." in body
