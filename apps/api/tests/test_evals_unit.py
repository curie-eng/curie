"""Unit tests for the eval matrix pivot (no I/O)."""

from typing import Any

from curie_api.evals import build_matrix


def _trace(tid: str, version: str, case: str, ts: str, passed: bool) -> dict[str, Any]:
    return {
        "id": tid,
        "timestamp": ts,
        "tags": ["eval", f"version:{version}", "suite:s"],
        "metadata": {"version": version, "case_id": case, "passed": passed},
    }


def test_build_matrix_pivots_cases_by_version() -> None:
    traces = [
        _trace("t1", "shaA", "c1", "2026-07-01T00:00:00Z", passed=True),
        _trace("t2", "shaA", "c2", "2026-07-01T00:00:00Z", passed=False),
        _trace("t3", "shaB", "c1", "2026-07-02T00:00:00Z", passed=True),
        # c2 was never run on shaB -> that cell is missing.
    ]
    matrix = build_matrix(traces, "s", 5)

    assert matrix.versions == ["shaB", "shaA"]  # most recent first
    assert matrix.cases == ["c1", "c2"]

    cells = {(row.case_id, cell.version): cell.status for row in matrix.rows for cell in row.cells}
    assert cells[("c1", "shaA")] == "pass"
    assert cells[("c1", "shaB")] == "pass"
    assert cells[("c2", "shaA")] == "fail"
    assert cells[("c2", "shaB")] == "missing"


def test_latest_trace_per_cell_wins() -> None:
    traces = [
        _trace("old", "shaA", "c1", "2026-07-01T00:00:00Z", passed=False),
        _trace("new", "shaA", "c1", "2026-07-02T00:00:00Z", passed=True),
    ]
    matrix = build_matrix(traces, "s", 5)
    assert matrix.rows[0].cells[0].status == "pass"  # the newer run


def test_version_limit_keeps_the_most_recent_columns() -> None:
    traces = [
        _trace("t1", "shaA", "c1", "2026-07-01T00:00:00Z", passed=True),
        _trace("t2", "shaB", "c1", "2026-07-02T00:00:00Z", passed=True),
        _trace("t3", "shaC", "c1", "2026-07-03T00:00:00Z", passed=True),
    ]
    matrix = build_matrix(traces, "s", 2)
    assert matrix.versions == ["shaC", "shaB"]


def _mtrace(
    tid: str,
    version: str,
    case: str,
    ts: str,
    passed: bool,
    model: str | None,
    cost: float | None = None,
) -> dict[str, Any]:
    tags = ["eval", f"version:{version}", "suite:s"]
    if model:
        tags.append(f"model:{model}")
    meta: dict[str, Any] = {
        "version": version,
        "case_id": case,
        "passed": passed,
        "model": model,
    }
    if cost is not None:
        meta["cost_usd"] = cost
    return {"id": tid, "timestamp": ts, "tags": tags, "metadata": meta}


def test_model_dimension_rolls_up_pass_rate_and_cost_per_model() -> None:
    # Same suite run across two models: the matrix slices pass-rate and summed
    # cost per model (issue #255). opus: 2/2 passed, $0.03; sonnet: 1/2, $0.01.
    traces = [
        _mtrace("o1", "shaA", "c1", "2026-07-01T00:00:00Z", True, "opus", 0.02),
        _mtrace("o2", "shaA", "c2", "2026-07-01T00:00:00Z", True, "opus", 0.01),
        _mtrace("s1", "shaB", "c1", "2026-07-02T00:00:00Z", True, "sonnet", 0.006),
        _mtrace("s2", "shaB", "c2", "2026-07-02T00:00:00Z", False, "sonnet", 0.004),
    ]
    matrix = build_matrix(traces, "s", 5)

    assert matrix.models == ["opus", "sonnet"]
    summaries = {m.model: m for m in matrix.model_summaries}
    assert summaries["opus"].passed == 2
    assert summaries["opus"].total == 2
    assert summaries["opus"].pass_rate == 1.0
    assert abs(summaries["opus"].cost_usd - 0.03) < 1e-9
    assert summaries["sonnet"].passed == 1
    assert summaries["sonnet"].total == 2
    assert summaries["sonnet"].pass_rate == 0.5
    assert abs(summaries["sonnet"].cost_usd - 0.01) < 1e-9


def test_same_version_sweep_keeps_every_model() -> None:
    """A sweep (#526) runs the SAME suite + version across N models, so those
    traces collide on (version, case) and would collapse to one in the grid dedup.
    The per-model rollup keys on the model too, so all N models survive with their
    own pass-rate -- the read surface a `local/cluster eval --model` sweep polls."""
    traces = [
        _mtrace("o1", "shaA", "c1", "2026-07-01T00:00:00Z", True, "opus", 0.02),
        _mtrace("o2", "shaA", "c2", "2026-07-01T00:00:01Z", True, "opus", 0.01),
        # sonnet ran the same version+cases slightly later (higher ts).
        _mtrace("s1", "shaA", "c1", "2026-07-01T00:01:00Z", True, "sonnet", 0.006),
        _mtrace("s2", "shaA", "c2", "2026-07-01T00:01:01Z", False, "sonnet", 0.004),
    ]
    matrix = build_matrix(traces, "s", 5)

    # One version column, but BOTH models present in the rollup (not collapsed).
    assert matrix.versions == ["shaA"]
    summaries = {m.model: m for m in matrix.model_summaries}
    assert set(summaries) == {"opus", "sonnet"}
    assert summaries["opus"].passed == 2 and summaries["opus"].total == 2
    assert summaries["sonnet"].passed == 1 and summaries["sonnet"].total == 2
    # The displayed grid still shows one cell per (version, case): newest wins.
    cell_models = {cell.model for row in matrix.rows for cell in row.cells}
    assert cell_models == {"sonnet"}  # sonnet recorded last


def test_cells_carry_model_and_unlabelled_runs_sort_last() -> None:
    traces = [
        _mtrace("m1", "shaA", "c1", "2026-07-01T00:00:00Z", True, "opus"),
        _mtrace("u1", "shaB", "c1", "2026-07-02T00:00:00Z", True, None),
    ]
    matrix = build_matrix(traces, "s", 5)

    cell_models = {cell.version: cell.model for row in matrix.rows for cell in row.cells}
    assert cell_models["shaA"] == "opus"
    assert cell_models["shaB"] is None
    # Unlabelled (None) model rolls up too and sorts after named models.
    assert matrix.models == ["opus", None]
    # A model with no reported cost anywhere stays None, not a misleading 0.
    assert {m.model: m.cost_usd for m in matrix.model_summaries} == {
        "opus": None,
        None: None,
    }


def _otrace(
    tid: str,
    version: str,
    case: str,
    ts: str,
    outcome: str,
    model: str | None = None,
    passed: Any = ...,
    error: str | None = None,
) -> dict[str, Any]:
    """A trace as the recorder writes it post-#606: an ``outcome`` alongside the
    derived ``passed``. ``passed`` can be forced to prove which one the grid reads.
    ``error`` mirrors ``EvalCaseResult.error`` (#622): set on a FAIL trace whose
    turn never completed (a classified failure, wrong terminal status, or a
    transport/runner exception), left ``None`` for a FAIL the grader judged."""
    tags = ["eval", f"version:{version}", "suite:s"]
    if model:
        tags.append(f"model:{model}")
    if outcome == "plumbing_ok":
        tags.append("plumbing")
    if passed is ...:
        passed = None if outcome == "plumbing_ok" else outcome == "pass"
    meta: dict[str, Any] = {
        "version": version,
        "case_id": case,
        "outcome": outcome,
        "passed": passed,
        "model": model,
        "error": error,
    }
    return {"id": tid, "timestamp": ts, "tags": tags, "metadata": meta}


def test_a_plumbing_cell_is_never_a_pass() -> None:
    """The fake tier asserts only that the turn completed, so its cell must be a
    distinct non-graded status: readable as "ran, not judged", impossible to mistake
    for a green promotion gate."""
    traces = [
        _otrace("p1", "shaA", "c1", "2026-07-01T00:00:00Z", "plumbing_ok", "fake-model"),
        _otrace("g1", "shaA", "c2", "2026-07-01T00:00:00Z", "fail", "opus"),
    ]
    matrix = build_matrix(traces, "s", 5)

    cells = {(row.case_id, cell.version): cell.status for row in matrix.rows for cell in row.cells}
    assert cells[("c1", "shaA")] == "plumbing_ok"
    assert cells[("c2", "shaA")] == "fail"


def test_the_outcome_wins_over_a_stale_passed_and_legacy_traces_still_render() -> None:
    """``outcome`` is authoritative: a trace whose ``passed`` says True but whose
    outcome is non-graded must NOT render green (that is exactly the false green).
    A pre-#606 trace with no outcome key at all keeps rendering off ``passed``, so
    historical matrix data needs no migration."""
    traces = [
        _otrace(
            "new",
            "shaA",
            "c1",
            "2026-07-01T00:00:00Z",
            "plumbing_ok",
            "fake-model",
            passed=True,
        ),
        _trace("legacy", "shaA", "c2", "2026-07-01T00:00:00Z", passed=True),
    ]
    matrix = build_matrix(traces, "s", 5)

    cells = {(row.case_id, cell.version): cell.status for row in matrix.rows for cell in row.cells}
    assert cells[("c1", "shaA")] == "plumbing_ok"  # not "pass"
    assert cells[("c2", "shaA")] == "pass"


def test_a_later_plumbing_run_does_not_erase_an_earlier_graded_result() -> None:
    """The grid is a promotion/comparison surface and a plumbing row carries zero
    comparative information, so it must not overwrite real signal just by being
    newer (someone re-running the sealed fake loop after a real eval). A graded cell
    beats a plumbing cell regardless of timestamp; newest-wins still applies within
    the same kind."""
    traces = [
        _otrace("graded", "shaA", "c1", "2026-07-01T00:00:00Z", "pass", "opus"),
        _otrace("plumb", "shaA", "c1", "2026-07-09T00:00:00Z", "plumbing_ok", "fake-model"),
        # c2 has only a plumbing run, so there is no graded signal to protect.
        _otrace("only", "shaA", "c2", "2026-07-09T00:00:00Z", "plumbing_ok", "fake-model"),
    ]
    matrix = build_matrix(traces, "s", 5)

    cells = {(row.case_id, cell.version): cell for row in matrix.rows for cell in row.cells}
    assert cells[("c1", "shaA")].status == "pass"
    assert cells[("c1", "shaA")].model == "opus"  # the graded run's label, not the fake's
    assert cells[("c2", "shaA")].status == "plumbing_ok"


def test_plumbing_rows_are_surfaced_without_diluting_any_pass_rate() -> None:
    """A sealed sweep's rows must never enter a pass-rate: with 2 plumbing rows
    counted as passes the fake model reads 100%, counted as fails it reads 0% -- both
    fabricated. They are excluded from passed/total and surfaced as their own count,
    so the rows stay visible rather than silently dropped."""
    traces = [
        _otrace("o1", "shaA", "c1", "2026-07-01T00:00:00Z", "pass", "opus"),
        _otrace("o2", "shaA", "c2", "2026-07-01T00:00:00Z", "fail", "opus"),
        _otrace("f1", "shaB", "c1", "2026-07-02T00:00:00Z", "plumbing_ok", "fake-model"),
        _otrace("f2", "shaB", "c2", "2026-07-02T00:00:00Z", "plumbing_ok", "fake-model"),
    ]
    matrix = build_matrix(traces, "s", 5)

    summaries = {m.model: m for m in matrix.model_summaries}
    assert summaries["opus"].passed == 1
    assert summaries["opus"].total == 2
    assert summaries["opus"].pass_rate == 0.5
    assert summaries["opus"].plumbing == 0

    fake = summaries["fake-model"]
    assert fake.total == 0  # nothing was graded...
    assert fake.passed == 0
    assert fake.plumbing == 2  # ...but the rows are visible, not dropped


def test_a_plumbing_row_does_not_dilute_its_own_model_pass_rate() -> None:
    """The exclusion is per-row, not per-model: a model with real graded rows keeps
    its true pass-rate even when a plumbing row shares the label."""
    traces = [
        _otrace("g1", "shaA", "c1", "2026-07-01T00:00:00Z", "pass", "opus"),
        _otrace("p1", "shaA", "c2", "2026-07-01T00:00:00Z", "plumbing_ok", "opus"),
    ]
    matrix = build_matrix(traces, "s", 5)

    opus = next(m for m in matrix.model_summaries if m.model == "opus")
    assert opus.passed == 1
    assert opus.total == 1  # the plumbing row is not a denominator
    assert opus.pass_rate == 1.0
    assert opus.plumbing == 1


def test_a_real_zero_percent_model_is_completed_but_not_passed() -> None:
    """A model that answered every case wrong is still `total > 0, completed ==
    total`: the negative control for #622. `passed == 0` alone must not be read
    as "never completed"."""
    traces = [
        _otrace("o1", "shaA", "c1", "2026-07-01T00:00:00Z", "fail", "opus"),
        _otrace("o2", "shaA", "c2", "2026-07-01T00:00:00Z", "fail", "opus"),
    ]
    matrix = build_matrix(traces, "s", 5)

    opus = next(m for m in matrix.model_summaries if m.model == "opus")
    assert opus.passed == 0
    assert opus.total == 2
    assert opus.completed == 2  # every case reached a verdict; it just lost


def test_a_model_that_never_completed_a_turn_is_distinct_from_a_real_zero() -> None:
    """A FAIL whose turn never completed (classified failure, wrong terminal
    status, a transport error) carries `error` in its metadata (issue #622).
    `total > 0` and `completed == 0` is the "never answered" outcome the CLI
    reports distinctly and fails on, not a real 0%."""
    traces = [
        _otrace(
            "b1",
            "shaA",
            "c1",
            "2026-07-01T00:00:00Z",
            "fail",
            "bogus-model-xyz",
            error="runner reported a classified failure",
        ),
        _otrace(
            "b2",
            "shaA",
            "c2",
            "2026-07-01T00:00:00Z",
            "fail",
            "bogus-model-xyz",
            error="runner reported a classified failure",
        ),
    ]
    matrix = build_matrix(traces, "s", 5)

    bogus = next(m for m in matrix.model_summaries if m.model == "bogus-model-xyz")
    assert bogus.passed == 0
    assert bogus.total == 2
    assert bogus.completed == 0


def test_a_partially_completed_model_counts_only_the_completed_rows() -> None:
    """Mixed within one model: some cases complete (graded fail), some never do
    (error set). `completed` counts only the former, so a model that is flaky
    rather than wholly unresolvable is not miscategorized as never-completed."""
    traces = [
        _otrace("g1", "shaA", "c1", "2026-07-01T00:00:00Z", "fail", "flaky"),
        _otrace(
            "g2",
            "shaA",
            "c2",
            "2026-07-01T00:00:00Z",
            "fail",
            "flaky",
            error="turn ended classified-failure, expected status 'done'",
        ),
    ]
    matrix = build_matrix(traces, "s", 5)

    flaky = next(m for m in matrix.model_summaries if m.model == "flaky")
    assert flaky.total == 2
    assert flaky.completed == 1


def test_legacy_traces_with_no_error_key_read_as_completed() -> None:
    """A pre-#622 trace never wrote `error` at all; there is nothing in it to say
    the turn did not complete, so it must not retroactively read as `completed ==
    0` and trip the new outcome on historical data."""
    traces = [_trace("t1", "shaA", "c1", "2026-07-01T00:00:00Z", passed=False)]
    matrix = build_matrix(traces, "s", 5)

    unlabelled = next(m for m in matrix.model_summaries if m.model is None)
    assert unlabelled.total == 1
    assert unlabelled.completed == 1


def test_per_version_summaries_scope_completed_to_each_sha() -> None:
    """#814: the blended `model_summaries` sums `completed` across every in-window
    sha, so a model that completed on an OLDER in-window sha but never completes on
    the TRIGGERED sha reads `completed > 0` in the blend -- masking the "never
    completed" outcome the sweep must fail on. `model_version_summaries` slices the
    same rollup per (version, model) so a caller can scope completion to one sha.

    Here opus completed and passed both cases on the older `shaOld`, but on the
    triggered `shaNew` every case landed as a graded FAIL whose turn never
    completed (error set). The blend hides it; the per-version rows must not.
    """
    traces = [
        # Older in-window sha: opus completed and passed both cases.
        _otrace("o1", "shaOld", "c1", "2026-07-01T00:00:00Z", "pass", "opus"),
        _otrace("o2", "shaOld", "c2", "2026-07-01T00:00:01Z", "pass", "opus"),
        # Triggered sha: every case is a graded FAIL that never completed.
        _otrace(
            "n1",
            "shaNew",
            "c1",
            "2026-07-02T00:00:00Z",
            "fail",
            "opus",
            error="runner reported a classified failure",
        ),
        _otrace(
            "n2",
            "shaNew",
            "c2",
            "2026-07-02T00:00:01Z",
            "fail",
            "opus",
            error="runner reported a classified failure",
        ),
    ]
    matrix = build_matrix(traces, "s", 5)

    # The blended rollup masks the outcome (completed picked up the old sha).
    blended = next(m for m in matrix.model_summaries if m.model == "opus")
    assert blended.total == 4
    assert blended.completed == 2  # the two old-sha completions -- the mask

    per_version = {(s.version, s.model): s for s in matrix.model_version_summaries}
    # The triggered sha's own row: never completed (total > 0, completed == 0).
    new = per_version[("shaNew", "opus")]
    assert new.total == 2
    assert new.completed == 0
    assert new.passed == 0
    # The older sha's row is unaffected: it did complete both cases.
    old = per_version[("shaOld", "opus")]
    assert old.total == 2
    assert old.completed == 2
    assert old.passed == 2


def test_per_version_summaries_surface_a_plumbing_only_row() -> None:
    """A (version, model) whose every row is plumbing has no graded total but must
    still surface with `total == 0, plumbing > 0`, mirroring `_model_summaries`
    (#700) -- the sweep reads this to tell a landed plumbing fixture apart from a
    model that has not landed at all."""
    traces = [
        _otrace("f1", "shaA", "c1", "2026-07-01T00:00:00Z", "plumbing_ok", "fake"),
        _otrace("f2", "shaA", "c2", "2026-07-01T00:00:01Z", "plumbing_ok", "fake"),
    ]
    matrix = build_matrix(traces, "s", 5)

    per_version = {(s.version, s.model): s for s in matrix.model_version_summaries}
    fake = per_version[("shaA", "fake")]
    assert fake.total == 0
    assert fake.plumbing == 2
    assert fake.completed == 0
