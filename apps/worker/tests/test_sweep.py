"""The pure pieces of a long scheduled sweep (ADR-0160, #2878).

The checkpoint parser, the continuation event ids and the coverage notice text
in ``curie_worker.sweep``. No Valkey, no Postgres: these are the rules every
kernel path leans on, pinned where a regression is cheapest to read.

``curie_worker.sweep`` is imported inside each test so one missing name fails
that test rather than the whole module's collection.
"""

from __future__ import annotations

import importlib
import inspect
import uuid
from datetime import UTC, date, datetime, timedelta
from types import ModuleType

import pytest
from curie_worker import runner_client
from curie_worker.hook_runs import retry_event_id, retry_expiry


def _sweep() -> ModuleType:
    return importlib.import_module("curie_worker.sweep")


_AGENT = "0b7e4c2a-9f43-4d55-9a51-3f1d2c8e6b10"
_BASE = f"cron:{_AGENT}:weekday-plan:2026-10-05T11:00:00+00:00"

_DOCUMENTED = """sweep-checkpoint
sweep: weekday-plan
date: 2026-10-05
hook: weekday-plan
covered: slack
uncovered: github, notes"""


def _expected(sweep: ModuleType, **overrides: object) -> object:
    fields: dict[str, object] = {
        "sweep": "weekday-plan",
        "date": date(2026, 10, 5),
        "hook": "weekday-plan",
        "covered": ("slack",),
        "uncovered": ("github", "notes"),
    }
    fields.update(overrides)
    return sweep.SweepCheckpoint(**fields)


# --- the checkpoint parser -------------------------------------------------------


def test_parse_checkpoint_accepts_the_documented_shape() -> None:
    sweep = _sweep()

    parsed = sweep.parse_checkpoint(_DOCUMENTED)

    assert parsed == _expected(sweep)
    assert sweep.CHECKPOINT_HEADER == "sweep-checkpoint"


def test_parse_checkpoint_accepts_extra_whitespace_blank_lines_and_any_key_order() -> None:
    """Liveness: a model rarely writes the block byte for byte."""
    sweep = _sweep()
    statement = (
        "\n\n   sweep-checkpoint   \n"
        "\n"
        "  uncovered :   github ,notes  \n"
        "hook:weekday-plan\n"
        "\t\n"
        "   date:  2026-10-05 \n"
        "covered:slack\n"
        "sweep:   weekday-plan\n\n"
    )

    assert sweep.parse_checkpoint(statement) == _expected(sweep)


def test_parse_checkpoint_keys_are_case_insensitive_and_unknown_keys_are_ignored() -> None:
    """Liveness: header and key case, an unknown key, and a colon in a value."""
    sweep = _sweep()
    statement = (
        "Sweep-Checkpoint\n"
        "SWEEP: weekday-plan\n"
        "Date: 2026-10-05\n"
        "Hook: weekday-plan\n"
        "progress: 1/3 sources, next: github\n"
        "Covered: slack\n"
        "UNCOVERED: github, notes\n"
        "note: retried github twice: rate limited"
    )

    assert sweep.parse_checkpoint(statement) == _expected(sweep)


@pytest.mark.parametrize(
    ("covered", "uncovered"),
    [("none", "none"), ("NONE", ""), ("", " None "), (" , ,", "none")],
)
def test_parse_checkpoint_none_and_empty_lists_are_empty(covered: str, uncovered: str) -> None:
    sweep = _sweep()
    statement = (
        "sweep-checkpoint\nsweep: weekday-plan\ndate: 2026-10-05\nhook: weekday-plan\n"
        f"covered: {covered}\nuncovered: {uncovered}"
    )

    assert sweep.parse_checkpoint(statement) == _expected(sweep, covered=(), uncovered=())


def _without(key: str) -> str:
    return "\n".join(line for line in _DOCUMENTED.splitlines() if not line.startswith(f"{key}:"))


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param(_DOCUMENTED.replace("sweep-checkpoint", "checkpoint", 1), id="wrong-header"),
        pytest.param(
            _DOCUMENTED.replace("sweep-checkpoint", "sweep checkpoint", 1), id="near-header"
        ),
        pytest.param("sweep: weekday-plan\n" + _DOCUMENTED, id="header-not-first"),
        pytest.param(_without("sweep"), id="missing-sweep"),
        pytest.param(_without("date"), id="missing-date"),
        pytest.param(_without("hook"), id="missing-hook"),
        pytest.param(_without("covered"), id="missing-covered"),
        pytest.param(_without("uncovered"), id="missing-uncovered"),
        pytest.param(_DOCUMENTED.replace("2026-10-05", "2026-13-05"), id="bad-month"),
        pytest.param(_DOCUMENTED.replace("2026-10-05", "2026-10-5"), id="short-date"),
        pytest.param(_DOCUMENTED.replace("2026-10-05", "10/05/2026"), id="us-date"),
        pytest.param(_DOCUMENTED.replace("2026-10-05", "2026-10-05T00:00"), id="datetime"),
        pytest.param(_DOCUMENTED.replace("hook: weekday-plan", "hook:   "), id="empty-hook"),
        pytest.param(_DOCUMENTED.replace("sweep: weekday-plan", "sweep:"), id="empty-sweep"),
        pytest.param(_DOCUMENTED + "\ncovered: github", id="duplicate-covered"),
        pytest.param(_DOCUMENTED + "\nHOOK: weekday-plan", id="duplicate-hook-any-case"),
        pytest.param(
            _DOCUMENTED.replace("uncovered: github, notes", "uncovered: slack, notes"),
            id="source-in-both-lists",
        ),
        pytest.param(
            "sweep-checkpoint sweep: weekday-plan date: 2026-10-05 hook: weekday-plan "
            "covered: slack uncovered: github, notes",
            id="single-line-flattened",
        ),
        pytest.param("sweep-checkpoint", id="header-only"),
        pytest.param("", id="empty"),
        pytest.param("   \n\t\n", id="blank"),
        pytest.param(None, id="none"),
        pytest.param(42, id="int"),
        pytest.param(_DOCUMENTED.encode(), id="bytes"),
        pytest.param(_DOCUMENTED.splitlines(), id="list"),
        pytest.param({"statement": _DOCUMENTED}, id="dict"),
    ],
)
def test_parse_checkpoint_refuses_malformed(statement: object) -> None:
    sweep = _sweep()

    assert sweep.parse_checkpoint(statement) is None


def test_parse_checkpoint_accepts_disjoint_lists() -> None:
    """Liveness for the overlap refusal: near-identical names are distinct."""
    sweep = _sweep()
    statement = (
        "sweep-checkpoint\nsweep: weekday-plan\ndate: 2026-10-05\nhook: weekday-plan\n"
        "covered: slack, github-enterprise\nuncovered: github, notes, slack-archive"
    )

    assert sweep.parse_checkpoint(statement) == _expected(
        sweep,
        covered=("slack", "github-enterprise"),
        uncovered=("github", "notes", "slack-archive"),
    )


# --- continuation ids -------------------------------------------------------------


def test_continuation_event_id_from_a_first_fire() -> None:
    sweep = _sweep()

    first = sweep.continuation_event_id(_BASE, 1, 0)

    assert first == f"{_BASE}:sweep:1:1:0"
    assert sweep.parse_continuation(first) == (_BASE, 1, 1, 0)
    assert sweep.parse_continuation(_BASE) is None


def test_continuation_event_id_replaces_a_previous_continuation_suffix() -> None:
    sweep = _sweep()

    second = sweep.continuation_event_id(f"{_BASE}:sweep:1:1:0", 3, 0)
    stalled = sweep.continuation_event_id(second, 3, 1)
    recovered = sweep.continuation_event_id(stalled, 4, 0)

    assert second == f"{_BASE}:sweep:2:3:0"
    assert stalled == f"{_BASE}:sweep:3:3:1"
    assert recovered == f"{_BASE}:sweep:4:4:0"
    assert sweep.parse_continuation(stalled) == (_BASE, 3, 3, 1)
    assert sweep.parse_continuation(recovered) == (_BASE, 4, 4, 0)


def test_continuation_event_id_keeps_a_deferred_retry_base() -> None:
    sweep = _sweep()
    retry = retry_event_id(f"cron:{_AGENT}:weekday-plan", datetime(2026, 10, 5, 12, tzinfo=UTC))

    first = sweep.continuation_event_id(retry, 2, 0)
    second = sweep.continuation_event_id(first, 2, 1)

    assert first == f"{retry}:sweep:1:2:0"
    assert second == f"{retry}:sweep:2:2:1"
    assert sweep.parse_continuation(first) == (retry, 1, 2, 0)
    assert sweep.parse_continuation(second) == (retry, 2, 2, 1)
    assert sweep.parse_continuation(retry) is None


def test_retry_expiry_is_none_for_a_continuation_id() -> None:
    """A continuation of an expired retry is not refused by the catch-up bound."""
    sweep = _sweep()
    expired = retry_event_id(f"cron:{_AGENT}:weekday-plan", datetime.now(UTC) - timedelta(days=1))

    assert retry_expiry(sweep.continuation_event_id(expired, 1, 0)) is None
    assert retry_expiry(sweep.continuation_event_id(_BASE, 1, 0)) is None
    assert retry_expiry(sweep.continuation_event_id(f"{_BASE}:sweep:4:7:2", 7, 3)) is None


def test_retry_expiry_still_reads_a_plain_retry_id() -> None:
    """Liveness: the retry id the catch-up bound reads is unchanged."""
    sweep = _sweep()
    expires = datetime(2026, 10, 5, 12, 30, tzinfo=UTC)
    retry = retry_event_id(f"cron:{_AGENT}:weekday-plan", expires)

    assert retry_expiry(retry) == expires
    # The continuation helper keeps the retry base readable by the same parser.
    parsed = sweep.parse_continuation(sweep.continuation_event_id(retry, 1, 0))
    assert parsed is not None
    assert retry_expiry(parsed[0]) == expires


def test_stall_and_slice_bounds_are_pinned() -> None:
    sweep = _sweep()

    assert sweep.MAX_STALLED_SLICES == 3
    assert sweep.MAX_SWEEP_SLICES == 48


@pytest.mark.parametrize(
    "event_id",
    [
        pytest.param(_BASE, id="first-fire-with-slot-colons"),
        pytest.param(f"cron:{_AGENT}:sweep:2026-10-05T11:00:00+00:00", id="hook-named-sweep"),
        pytest.param(
            retry_event_id(f"cron:{_AGENT}:weekday-plan", datetime(2026, 10, 5, tzinfo=UTC)),
            id="retry",
        ),
        pytest.param(f"work-item-{uuid.uuid4()}-execute-1", id="work-item"),
        pytest.param("approval-appr_123-resolved", id="approval-resume"),
        pytest.param(f"github-feedback-{uuid.uuid4()}", id="github-feedback"),
        pytest.param("1727000000.123456", id="slack-ts"),
        pytest.param("cron:a:b:sweep:1:1", id="four-trailing-parts"),
        pytest.param("sweep:1:1:0", id="no-base"),
        pytest.param("", id="empty"),
    ],
)
def test_parse_continuation_ignores_foreign_ids(event_id: str) -> None:
    sweep = _sweep()

    assert sweep.parse_continuation(event_id) is None


@pytest.mark.parametrize(
    "suffix",
    [
        ":sweep:0:1:0",
        ":sweep:x:1:0",
        ":sweep:1:x:0",
        ":sweep:1:1:x",
        ":sweep:-1:1:0",
        ":sweep:1:-1:0",
        ":sweep:1:1:-1",
        ":sweep:+1:1:0",
        ":sweep: 1:1:0",
        ":sweep:\uff11:1:0",  # a fullwidth digit is decimal but not ASCII
        ":sweep:1:1:\u0661",  # an Arabic-Indic digit
        ":sweep::1:0",
        ":sweep:1::0",
        ":sweep:1:1:",
        ":SWEEP:1:1:0",
    ],
)
def test_parse_continuation_refuses_non_digit_or_zero_slice(suffix: str) -> None:
    sweep = _sweep()

    assert sweep.parse_continuation(f"{_BASE}{suffix}") is None
    # Liveness: the same base with a valid suffix parses.
    assert sweep.parse_continuation(f"{_BASE}:sweep:1:0:0") == (_BASE, 1, 0, 0)


# --- the coverage notice ----------------------------------------------------------


def _read(sweep: ModuleType, *, uncovered: tuple[str, ...], hook: str = "weekday-plan") -> object:
    return sweep.SweepRead(
        date=date(2026, 10, 5),
        checkpoint=_expected(sweep, hook=hook, uncovered=uncovered),
    )


def test_notice_text_names_hook_date_outcome_and_uncovered() -> None:
    sweep = _sweep()

    failed = sweep.coverage_notice_text(
        hook="weekday-plan", outcome="failed", read=_read(sweep, uncovered=("github", "notes"))
    )
    blocked_empty = sweep.coverage_notice_text(
        hook="weekday-plan", outcome="blocked", read=_read(sweep, uncovered=())
    )

    assert failed == (
        'Scheduled sweep "weekday-plan" for 2026-10-05 stopped before it finished '
        "(run outcome: failed). Not covered: github, notes."
    )
    assert blocked_empty == (
        'Scheduled sweep "weekday-plan" for 2026-10-05 stopped before it finished '
        "(run outcome: blocked). Not covered: none recorded; the run stopped before "
        "it posted its result."
    )


def test_notice_text_without_checkpoint_says_nothing_was_recorded_as_covered() -> None:
    sweep = _sweep()

    notice = sweep.coverage_notice_text(
        hook="weekday-plan",
        outcome="skipped",
        read=sweep.SweepRead(date=date(2026, 10, 5), checkpoint=None),
    )

    assert notice == (
        'Scheduled sweep "weekday-plan" for 2026-10-05 stopped before it finished '
        "(run outcome: skipped). Nothing was recorded as covered."
    )


def test_notice_text_drops_unsafe_source_labels() -> None:
    """Model-authored labels cannot ping a channel or inject markup."""
    sweep = _sweep()
    unsafe = ("<!channel>", "@here", "x" * 200, "a" * 65, "-leading-dash", "line\nbreak")

    notice = sweep.coverage_notice_text(
        hook="weekday-plan",
        outcome="failed",
        read=_read(sweep, uncovered=("github", *unsafe)),
    )

    assert notice == (
        'Scheduled sweep "weekday-plan" for 2026-10-05 stopped before it finished '
        f"(run outcome: failed). Not covered: github. {len(unsafe)} source name(s) "
        "could not be shown."
    )
    for label in unsafe:
        assert label not in notice
    refused_hook = sweep.coverage_notice_text(
        hook="<!here> deploy",
        outcome="failed",
        read=_read(sweep, uncovered=("github",), hook="<!here> deploy"),
    )
    assert "this hook" in refused_hook
    assert "<!here>" not in refused_hook


def test_notice_text_keeps_ordinary_source_labels() -> None:
    """Liveness for the label rule: everyday source names survive untouched."""
    sweep = _sweep()
    # ``#`` is in the label set, but not as the first character (the plan's
    # rule is ``^[A-Za-z0-9][A-Za-z0-9 ._/#-]{0,63}``), so the channel-style
    # name is written ``ops#2`` here rather than ``#ops``.
    ordinary = (
        "github",
        "meeting-notes",
        "eng/infra",
        "ops#2",
        "Q3 roadmap",
        "notes.v2",
        "a" * 64,
    )

    notice = sweep.coverage_notice_text(
        hook="weekday_plan.v2", outcome="failed", read=_read(sweep, uncovered=ordinary)
    )

    assert notice.startswith('Scheduled sweep "weekday_plan.v2" for 2026-10-05 ')
    assert notice.endswith(f"Not covered: {', '.join(ordinary)}.")
    assert "could not be shown" not in notice


def test_notice_text_omits_an_unresolved_date() -> None:
    sweep = _sweep()

    without = sweep.coverage_notice_text(
        hook="weekday-plan",
        outcome="failed",
        read=sweep.SweepRead(date=None, checkpoint=None),
    )
    with_checkpoint = sweep.coverage_notice_text(
        hook="weekday-plan",
        outcome="failed",
        read=sweep.SweepRead(date=None, checkpoint=_expected(sweep)),
    )

    assert without == (
        'Scheduled sweep "weekday-plan" stopped before it finished '
        "(run outcome: failed). Nothing was recorded as covered."
    )
    assert with_checkpoint == (
        'Scheduled sweep "weekday-plan" stopped before it finished '
        "(run outcome: failed). Not covered: github, notes."
    )


# --- constants ----------------------------------------------------------------------


def test_margin_constant_pins_its_components() -> None:
    """Revision 2 (F1): the delivery-start hook lease margin covers the interrupt
    RPC the timed-out attempt awaits, the coverage read, and 30 s of slack."""
    sweep = _sweep()

    assert sweep.READ_TIMEOUT_S == 3.0
    assert sweep.HOOK_LEASE_START_MARGIN_S == (
        runner_client._DEFAULT_INTERRUPT_TIMEOUT_S + sweep.READ_TIMEOUT_S + 30.0
    )
    default = inspect.signature(sweep.SweepCoverage).parameters["read_timeout_s"].default
    assert default == sweep.READ_TIMEOUT_S
    assert sweep.BUDGET_CUT_CLASSIFICATIONS == frozenset(
        {"runner-timeout", "runner-timeout-unconfirmed"}
    )
    assert sweep.MAX_BUSY_PROBE_INTERVAL_S == 2.0
