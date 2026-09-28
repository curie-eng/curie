"""The live SVG status card (#3077).

GitHub's image proxy (camo) fetches ``/v1/factory/cards/{token}.svg`` with no
credential, so the token is the capability and the response must never be
cached. The HTTP cases drive a signed label admission and real progress reports
through create_app(); the renderer cases call the pure ``render_card``.

Card markup contract the tests pin (the renderer is otherwise free):

- each stage is a ``<g>`` with ``data-stage="<id>"`` and a class naming its
  state (``done``, ``current``, ``redo``, ``pending``, ``blocked``);
- each loop arc is a ``<path>`` with class ``loop-arc`` and ``data-loop``, and
  its numbered badge is a ``<text>`` with class ``loop-badge``;
- a legacy declaration without stages renders one group per phase with
  ``data-phase="<id>"`` and no arcs;
- the pill dot carries class ``live`` exactly when the pill is live.
"""

from __future__ import annotations

import asyncio
import re
import sys
import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.factory_card import CardInput, render_card
from curie_api.factory_progress import phase_view, record_wait_ci
from curie_api.models import ExecutionRequestPhaseReport
from test_factory_progress import (
    ACTIVITY,
    DECLARATION,
    PILLS,
    STAGED_DECLARATION,
    WORKER,
    _constraint_statuses,
    report,
)
from test_factory_terminus import (  # noqa: F401  (fixtures)
    REPO,
    _label,
    _reconcile,
    _request,
    _rows,
    _start_running,
    admitted,
    comments,
)

pytestmark = pytest.mark.usefixtures("clean_db")

SVG = "{http://www.w3.org/2000/svg}"
HOSTILE_TITLE = '<script>alert(1)</script> & "x"'
NOW = datetime(2026, 9, 24, 12, 30, 5, tzinfo=UTC)
LIVE = {"waiting": False, "running": True, "cancellation_requested": True}


def _token(request_id: uuid.UUID) -> str:
    return _rows(
        "SELECT card_token FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )[0]["card_token"]


def _parse(body: str) -> ET.Element:
    root = ET.fromstring(body)
    assert root.tag in {"svg", f"{SVG}svg"}, root.tag
    return root


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _classes(element: ET.Element) -> set[str]:
    return set((element.get("class") or "").split())


def _with_class(root: ET.Element, name: str) -> list[ET.Element]:
    return [element for element in root.iter() if name in _classes(element)]


def _slot(root: ET.Element, phase: str) -> ET.Element:
    (slot,) = [e for e in root.iter() if e.get("data-phase") == phase]
    return slot


def _stage(root: ET.Element, stage: str) -> ET.Element:
    (slot,) = [e for e in root.iter() if e.get("data-stage") == stage]
    return slot


def _arc(root: ET.Element, loop: str) -> ET.Element:
    (arc,) = [e for e in _with_class(root, "loop-arc") if e.get("data-loop") == loop]
    return arc


def _badge(root: ET.Element, loop: str) -> str:
    (badge,) = [e for e in _with_class(root, "loop-badge") if e.get("data-loop") == loop]
    return "".join(badge.itertext()).strip()


def _path_points(path: ET.Element) -> list[tuple[float, float]]:
    return [
        (float(x), float(y))
        for x, y in re.findall(r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)", path.get("d") or "")
    ]


def _all_text(root: ET.Element) -> str:
    return " ".join("".join(element.itertext()) for element in [root])


def _style(root: ET.Element) -> str:
    return " ".join(
        "".join(element.itertext()) for element in root.iter() if _local(element.tag) == "style"
    )


def _is_italic(element: ET.Element, style: str) -> bool:
    if (element.get("font-style") or "") == "italic":
        return True
    if "italic" in (element.get("style") or ""):
        return True
    for name in _classes(element):
        if re.search(rf"\.{re.escape(name)}\s*\{{[^}}]*font-style:\s*italic", style):
            return True
    return False


# --- HTTP ---------------------------------------------------------------------


def test_a_running_card_is_an_uncached_svg_behind_no_auth(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9801
    _label(client, github, number)
    request_id = _request(number)["id"]
    _start_running(request_id)
    token = _token(request_id)
    assert re.fullmatch(r"[0-9a-f]{64}", token)

    response = client.get(f"/v1/factory/cards/{token}.svg")

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("image/svg+xml")
    assert "no-cache" in response.headers["cache-control"]
    assert response.headers["x-content-type-options"] == "nosniff"
    assert "default-src 'none'" in response.headers["content-security-policy"]
    root = _parse(response.text)
    assert "RUNNING" in _all_text(root)


@pytest.mark.parametrize(
    "token",
    [
        "0123456789abcdef" * 4,
        "abc",
        "a" * 63,
        "0123456789ABCDEF" * 4,
        "g" * 64,
    ],
)
def test_an_unknown_or_malformed_token_is_404(admitted: Any, token: str) -> None:  # noqa: F811
    client, github, _sink = admitted
    _label(client, github, 9802)
    response = client.get(f"/v1/factory/cards/{token}.svg")
    assert response.status_code == 404, response.text


def test_each_request_gets_its_own_unguessable_token(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    tokens = set()
    for number in (9803, 9804):
        _label(client, github, number)
        tokens.add(_token(_request(number)["id"]))
    assert len(tokens) == 2
    assert all(re.fullmatch(r"[0-9a-f]{64}", token) for token in tokens)


def test_the_card_escapes_the_issue_title_and_shows_the_note_in_italics(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9805
    sink.titles[number] = HOSTILE_TITLE
    _label(client, github, number)
    request_id = _request(number)["id"]
    _start_running(request_id)
    note = "Reading <the> issue & its comments"
    assert report(client, request_id, "read_issue", note=note).status_code == 201
    _reconcile()  # fetches and stores the title once

    response = client.get(f"/v1/factory/cards/{_token(request_id)}.svg")

    assert response.status_code == 200, response.text
    body = response.text
    assert "<script" not in body
    assert "&lt;script&gt;" in body
    root = _parse(body)
    text = _all_text(root)
    assert REPO in text
    assert f"#{number}" in text
    assert "<script>alert(1)</script>" in text  # decoded text content, never markup
    assert "RUNNING" in text
    style = _style(root)
    (holder,) = [e for e in root.iter() if (e.text or "").strip() == note.strip()]
    assert _is_italic(holder, style)


def test_a_card_with_a_kickback_badges_the_plan_arc(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9806
    _label(client, github, number)
    request_id = _request(number)["id"]
    _start_running(request_id)
    for phase, loop_round in (("read_issue", None), ("plan", 1), ("plan_review", 1)):
        assert (
            report(client, request_id, phase, round=loop_round, declaration=STAGED_DECLARATION)
            .status_code
            == 201
        )
    before = _parse(client.get(f"/v1/factory/cards/{_token(request_id)}.svg").text)
    assert _with_class(before, "loop-arc") == []
    assert _with_class(before, "loop-badge") == []

    assert (
        report(client, request_id, "plan", round=2, declaration=STAGED_DECLARATION).status_code
        == 201
    )
    after = _parse(client.get(f"/v1/factory/cards/{_token(request_id)}.svg").text)

    arcs = _with_class(after, "loop-arc")
    assert len(arcs) == 1
    assert all(_local(arc.tag) == "path" for arc in arcs)
    assert _badge(after, "plan") == "1"
    assert "redo" in _classes(_stage(after, "plan_review"))
    assert "current" in _classes(_stage(after, "plan"))
    assert "round 2 of 3" in _all_text(after)


def test_platform_wait_ci_keeps_the_latest_agent_note_in_the_card(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 9808
    _label(client, github, number)
    request_id = _request(number)["id"]
    _start_running(request_id)
    note = "Review passed and publication requested"
    for phase, loop_round, progress_note in (
        ("implement", 1, None),
        ("review_diff", 1, note),
        ("publish", None, None),
    ):
        response = report(
            client,
            request_id,
            phase,
            round=loop_round,
            note=progress_note,
            declaration=STAGED_DECLARATION,
        )
        assert response.status_code == 201, response.text

    async def mark_waiting() -> bool:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await record_wait_ci(session, request_id)
        finally:
            await engine.dispose()

    assert asyncio.run(mark_waiting())
    assert _rows(
        "SELECT phase, note FROM curie.execution_request_phase_reports "
        "WHERE execution_request_id = :id ORDER BY id DESC LIMIT 1",
        {"id": request_id},
    ) == [{"phase": "wait_ci", "note": None}]

    response = client.get(f"/v1/factory/cards/{_token(request_id)}.svg")
    assert response.status_code == 200, response.text
    root = _parse(response.text)
    assert "current" in _classes(_stage(root, "wait_ci"))
    (holder,) = [element for element in root.iter() if (element.text or "") == note]
    assert _is_italic(holder, _style(root))


@pytest.mark.parametrize(
    ("cause", "pill"),
    [("ci_failed", "NEEDS HUMAN"), ("runner_failed", "FAILED")],
)
def test_terminal_cause_selects_the_card_pill_at_the_route(
    admitted: Any, cause: str, pill: str  # noqa: F811
) -> None:
    client, github, _sink = admitted
    number = 9807
    _label(client, github, number)
    request_id = _request(number)["id"]
    epoch = _start_running(request_id)
    assert (
        report(client, request_id, "implement", round=1, declaration=STAGED_DECLARATION)
        .status_code
        == 201
    )
    finished = client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers=WORKER,
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": cause},
    )
    assert finished.status_code == 200, finished.text

    response = client.get(f"/v1/factory/cards/{_token(request_id)}.svg")
    assert response.status_code == 200, response.text
    root = _parse(response.text)
    assert pill in _all_text(root)
    assert "blocked" in _classes(_stage(root, "implement"))


# --- the pure renderer ------------------------------------------------------------


def _reports(*entries: tuple[str, int | None]) -> list[ExecutionRequestPhaseReport]:
    request_id = uuid.uuid4()
    return [
        ExecutionRequestPhaseReport(
            id=index + 1, execution_request_id=request_id, phase=phase, loop_round=loop_round
        )
        for index, (phase, loop_round) in enumerate(entries)
    ]


def _card(
    *,
    status: str = "running",
    publishing: bool = False,
    reports: list[ExecutionRequestPhaseReport] | None = None,
    declaration: dict[str, Any] | None = None,
    repo: str = REPO,
    title: str | None = "Fix the flaky test",
    activity: dict[str, Any] | None = None,
    note: str | None = None,
    cause_text: str | None = None,
    started_at: datetime | None = NOW - timedelta(minutes=12, seconds=4),
    terminal_at: datetime | None = None,
    revision_pr: int | None = None,
    needs_human: bool = False,
) -> str:
    terminal_cause = None if status in LIVE else ("completed" if status == "completed" else "x")
    view = phase_view(declaration or DECLARATION, reports or [], status, terminal_cause)
    return render_card(
        CardInput(
            repo=repo,
            issue_number=42,
            title=title,
            revision_pr=revision_pr,
            status=status,
            publishing=publishing,
            started_at=started_at,
            terminal_at=terminal_at,
            now=NOW,
            activity=ACTIVITY if activity is None else activity,
            note=note,
            phase_view=view,
            cause_text=cause_text,
            needs_human=needs_human,
        )
    )


@pytest.mark.parametrize("status", _constraint_statuses())
def test_every_status_renders_its_pill_and_only_live_ones_pulse(status: str) -> None:
    root = _parse(_card(status=status))
    label, _color, live = PILLS[status]
    assert label in _all_text(root)
    assert bool(_with_class(root, "live")) is live


def test_a_publishing_run_shows_the_publishing_pill_live() -> None:
    root = _parse(_card(status="running", publishing=True))
    assert "PUBLISHING" in _all_text(root)
    assert _with_class(root, "live")


def test_the_card_is_as_wide_as_a_desktop_issue_comment() -> None:
    root = _parse(_card())
    assert (root.get("width"), root.get("height")) == ("878", "300")
    assert root.get("viewBox") == "0 0 878 300"


def test_both_themes_ride_one_style_block() -> None:
    style = _style(_parse(_card()))
    assert re.search(r"@media\s*\(\s*prefers-color-scheme\s*:\s*dark\s*\)", style)


def test_no_review_kickback_means_no_arc() -> None:
    root = _parse(
        _card(reports=_reports(("plan", 1), ("plan_review", 1), ("failing_test", None)))
    )
    assert _with_class(root, "loop-arc") == []
    assert _with_class(root, "loop-badge") == []
    assert "approved, 1 round" in _all_text(root)


def test_a_diff_review_approved_after_three_rounds_badges_two() -> None:
    root = _parse(
        _card(
            declaration=STAGED_DECLARATION,
            reports=_reports(
                ("plan", 1),
                ("plan_review", 1),
                ("implement", 1),
                ("review_diff", 1),
                ("implement", 2),
                ("review_diff", 2),
                ("implement", 3),
                ("review_diff", 3),
                ("publish", None),
            )
        )
    )
    assert len(_with_class(root, "loop-arc")) == 1
    assert _badge(root, "implement") == "2"
    assert "approved · 3 rounds" in _all_text(root)
    assert "current" in _classes(_stage(root, "review_diff"))


def test_both_loops_kicked_back_draw_two_arcs() -> None:
    root = _parse(
        _card(
            declaration=STAGED_DECLARATION,
            reports=_reports(
                ("plan", 1),
                ("plan_review", 1),
                ("plan", 2),
                ("plan_review", 2),
                ("implement", 1),
                ("review_diff", 1),
                ("implement", 2),
            )
        )
    )
    assert len(_with_class(root, "loop-arc")) == 2
    assert [_badge(root, loop) for loop in ("plan", "implement")] == ["1", "1"]


def test_every_declared_phase_has_a_slot_in_declared_order() -> None:
    root = _parse(_card(reports=_reports(("read_issue", None), ("pin_criteria", None))))
    slots = [e.get("data-phase") for e in root.iter() if e.get("data-phase")]
    assert slots == [phase["id"] for phase in DECLARATION["phases"]]
    assert _with_class(root, "loop-arc") == []
    assert "done" in _classes(_slot(root, "read_issue"))
    assert "current" in _classes(_slot(root, "pin_criteria"))
    assert "pending" in _classes(_slot(root, "wait_ci"))


CI_RETRY_ENTRIES = (
    ("plan", 1),
    ("plan_review", 1),
    ("plan", 2),
    ("plan_review", 2),
    ("implement", 1),
    ("review_diff", 1),
    ("implement", 2),
    ("review_diff", 2),
    ("publish", None),
    ("wait_ci", None),
    ("implement", 3),
    ("review_diff", 3),
    ("publish", None),
    ("wait_ci", None),
)


@pytest.mark.parametrize(
    ("entries", "status", "states", "arcs"),
    [
        pytest.param(
            (("plan", 1), ("plan_review", 1), ("plan", 2)),
            "running",
            ("current", "redo", "pending", "pending", "pending"),
            (("plan", "1", "live"),),
            id="second_plan_round",
        ),
        pytest.param(
            (
                ("plan", 1),
                ("plan_review", 1),
                ("plan", 2),
                ("plan_review", 2),
                ("implement", 1),
                ("review_diff", 1),
                ("implement", 2),
                ("review_diff", 2),
                ("implement", 3),
            ),
            "running",
            ("done", "done", "current", "redo", "pending"),
            (("plan", "1", "approved"), ("implement", "2", "live")),
            id="third_diff_round",
        ),
        pytest.param(
            CI_RETRY_ENTRIES,
            "running",
            ("done", "done", "done", "done", "current"),
            (
                ("plan", "1", "approved"),
                ("implement", "1", "approved"),
                ("wait_ci", "1", "live"),
            ),
            id="ci_retry_waiting",
        ),
        pytest.param(
            CI_RETRY_ENTRIES,
            "completed",
            ("done", "done", "done", "done", "done"),
            (
                ("plan", "1", "approved"),
                ("implement", "1", "approved"),
                ("wait_ci", "1", "approved"),
            ),
            id="succeeded",
        ),
    ],
)
def test_five_stages_and_kickback_arcs_show_the_run_state(
    entries: tuple[tuple[str, int | None], ...],
    status: str,
    states: tuple[str, ...],
    arcs: tuple[tuple[str, str, str], ...],
) -> None:
    root = _parse(
        _card(status=status, declaration=STAGED_DECLARATION, reports=_reports(*entries))
    )
    assert (root.get("width"), root.get("height")) == ("878", "300")
    stage_ids = ("plan", "plan_review", "implement", "review_diff", "wait_ci")
    stages = [_stage(root, stage_id) for stage_id in stage_ids]
    assert [
        "".join(e.itertext()).strip()
        for stage in stages
        for e in stage.iter()
        if "label" in _classes(e)
    ] == ["Plan", "Plan review", "Implement", "Review diff", "Wait for CI"]
    assert all(state in _classes(stage) for state, stage in zip(states, stages, strict=True))
    labels = [next(e for e in stage.iter() if "label" in _classes(e)) for stage in stages]
    assert len({float(label.get("y") or "0") for label in labels}) == 1
    assert [float(label.get("x") or "0") for label in labels] == sorted(
        float(label.get("x") or "0") for label in labels
    )
    assert len(_with_class(root, "loop-arc")) == len(arcs)
    assert len(_with_class(root, "loop-badge")) == len(arcs)
    assert {arc.get("data-loop") for arc in _with_class(root, "loop-arc")} == {
        loop for loop, _badge_text, _state in arcs
    }
    for loop, badge, arc_state in arcs:
        assert _badge(root, loop) == badge
        assert arc_state in _classes(_arc(root, loop))


def test_ci_arc_wraps_the_diff_arc_and_returns_to_implement() -> None:
    root = _parse(_card(declaration=STAGED_DECLARATION, reports=_reports(*CI_RETRY_ENTRIES)))
    ci = _path_points(_arc(root, "wait_ci"))
    diff = _path_points(_arc(root, "implement"))
    assert len(ci) >= 4 and len(diff) >= 4
    assert min(y for _x, y in ci) < min(y for _x, y in diff)
    implement = _stage(root, "implement")
    (icon,) = [e for e in implement.iter() if _local(e.tag) == "circle"]
    assert ci[-1][0] == float(icon.get("cx") or "nan")
    assert ci[0][0] > diff[0][0]


def test_staged_header_uses_recorded_activity_and_the_declared_reviewer() -> None:
    note = "Checking <the> fix"
    root = _parse(_card(declaration=STAGED_DECLARATION, note=note))
    text = _all_text(root)
    assert "12m 04s" in text
    assert ACTIVITY["model"] in text
    assert "turns 14" in text
    assert "tool calls 37" in text
    assert "last tool Bash" in text
    assert "anthropic/claude-opus-5.5" in text
    assert "loop cap 3" in text
    (holder,) = [element for element in root.iter() if (element.text or "") == note]
    assert _is_italic(holder, _style(root))


@pytest.mark.parametrize(
    ("status", "needs_human", "pill"),
    [("failed", False, "FAILED"), ("failed", True, "NEEDS HUMAN")],
)
def test_terminal_failure_marks_its_stage_with_an_amber_cross(
    status: str, needs_human: bool, pill: str
) -> None:
    root = _parse(
        _card(
            status=status,
            needs_human=needs_human,
            declaration=STAGED_DECLARATION,
            reports=_reports(("implement", 1)),
            cause_text="Review did not pass",
        )
    )
    assert pill in _all_text(root)
    assert "blocked" in _classes(_stage(root, "implement"))
    assert _with_class(_stage(root, "implement"), "icon-blocked")
    assert not _with_class(root, "live")


def test_elapsed_is_minutes_and_padded_seconds_and_a_hyphen_before_start() -> None:
    assert "12m 04s" in _all_text(_parse(_card()))
    waiting = _all_text(_parse(_card(status="waiting", started_at=None)))
    assert "12m 04s" not in waiting
    assert re.search(r"(?<![\w-])-(?![\w-])", waiting)
    finished = _all_text(
        _parse(
            _card(
                status="failed",
                terminal_at=NOW - timedelta(minutes=2),
                cause_text="the run ended without a result.",
            )
        )
    )
    assert "10m 04s" in finished


def test_the_footer_stamps_the_render_time() -> None:
    text = _all_text(_parse(_card()))
    assert "phases reported by the agent" in text
    assert "12:30:05 UTC" in text


@pytest.mark.parametrize(
    "field",
    ["title", "note", "repo", "model", "last_tool", "cause_text", "phase_label"],
)
def test_every_untrusted_string_is_escaped_text(field: str) -> None:
    hostile = '<svg onload="x"><a href="https://evil.example">&amp;'
    kwargs: dict[str, Any] = {}
    if field in {"model", "last_tool"}:
        kwargs["activity"] = {**ACTIVITY, field: hostile}
    elif field == "phase_label":
        phases = [dict(p) for p in DECLARATION["phases"]]
        phases[0]["label"] = "<b>&x</b>"[:40]
        kwargs["declaration"] = {**DECLARATION, "phases": phases}
        hostile = "<b>&x</b>"
    elif field == "cause_text":
        kwargs["status"] = "failed"
        kwargs["cause_text"] = hostile
    else:
        kwargs[field] = hostile
    body = _card(**kwargs)
    root = _parse(body)
    assert "onload" not in "".join(
        f"{k}={v}" for element in root.iter() for k, v in element.attrib.items()
    )
    assert "evil.example" not in "".join(
        v for element in root.iter() for v in element.attrib.values()
    )
    # Parsing succeeded, so nothing hostile became markup: no injected element.
    assert not any(_local(element.tag) in {"a", "b"} for element in root.iter())
    assert [_local(e.tag) for e in root.iter()].count("svg") == 1
    assert "<b>" not in body
    assert "&lt;" in body


@pytest.mark.parametrize("status", _constraint_statuses())
def test_no_card_carries_script_foreign_objects_or_external_links(status: str) -> None:
    body = _card(
        status=status,
        reports=_reports(("plan", 1), ("plan_review", 1), ("plan", 2)),
        note="Replanning after review",
    )
    lowered = body.lower()
    assert "<script" not in lowered
    assert "foreignobject" not in lowered
    assert not re.search(r"href\s*=\s*[\"']?\s*(https?:|//|javascript:)", lowered)
