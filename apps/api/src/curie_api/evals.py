"""Eval matrix (K1): assemble the case-by-version grid from Langfuse.

K1's recorder writes, per case, a trace tagged version:<sha> + suite:<name> whose
metadata carries case_id and the outcome (mirrored, for a graded case, by an
eval_pass score). The matrix reads the outcome straight off each suite trace's
metadata -- scoping to the traces just fetched, rather than joining a
globally-paginated scores query -- and pivots them into rows = cases, columns =
the last N versions, cells = pass/fail/plumbing_ok/missing. Pure builder so the
pivot is unit-testable off canned data.
"""

from typing import Any, Literal

from .schemas import (
    EvalCell,
    EvalMatrix,
    EvalMatrixRow,
    EvalModelSummary,
    EvalModelVersionSummary,
)

Status = Literal["pass", "fail", "plumbing_ok", "missing"]

_VERSION_TAG = "version:"
_MODEL_TAG = "model:"
_PLUMBING_OK = "plumbing_ok"


def _outcome_of(trace: dict[str, Any]) -> Status:
    """This trace's cell status, from ``outcome`` when present.

    ``outcome`` is authoritative over ``passed``: a non-graded row derives
    ``passed=None``, but reading ``passed`` first would render any stale or
    fabricated True as a green cell -- exactly the false green the fake tier's
    non-graded outcome exists to prevent. A pre-#606 trace carries no ``outcome``
    key at all, so it falls back to ``passed`` and historical matrix data keeps
    rendering with no migration.
    """
    metadata = trace.get("metadata") or {}
    outcome = metadata.get("outcome")
    if outcome == _PLUMBING_OK:
        return "plumbing_ok"
    if outcome == "pass":
        return "pass"
    if outcome == "fail":
        return "fail"
    passed = metadata.get("passed")
    if passed is None:
        return "missing"
    return "pass" if passed else "fail"


def _is_graded(trace: dict[str, Any]) -> bool:
    return _outcome_of(trace) in ("pass", "fail")


def _completed(trace: dict[str, Any]) -> bool:
    """Did this case's turn actually reach a graded verdict?

    A graded ``pass`` is always a completion. A graded ``fail`` is ambiguous on
    its own: the worker (``EvalCaseResult``) sets ``error`` on a FAIL precisely
    when the turn never completed -- a classified failure, a terminal status
    that did not match what the case expected, or a transport/runner exception
    -- and leaves it unset when the turn completed cleanly and the grader simply
    said no. Reading ``error`` is what turns "0/5 (0%)" into two distinguishable
    outcomes: a model that answered wrong five times, and a model that never
    answered at all (issue #622, #526 AC4). Not called for a non-graded
    (plumbing/missing) trace, so a pre-#606 trace with no ``error`` key at all
    (or none written) reads as completed by omission -- there is nothing in a
    legacy trace to say otherwise.
    """
    outcome = _outcome_of(trace)
    if outcome == "pass":
        return True
    if outcome == "fail":
        return not (trace.get("metadata") or {}).get("error")
    return False


def _version_of(trace: dict[str, Any]) -> str | None:
    metadata = trace.get("metadata") or {}
    version = metadata.get("version")
    if isinstance(version, str) and version:
        return version
    for tag in trace.get("tags") or []:
        if isinstance(tag, str) and tag.startswith(_VERSION_TAG):
            return tag[len(_VERSION_TAG) :]
    return None


def _model_of(trace: dict[str, Any] | None) -> str | None:
    """The model tag/metadata on an eval trace, or None when unlabelled/absent.

    Mirrors ``_version_of``: metadata field first (what the recorder writes
    straight onto the trace), falling back to the ``model:`` tag. A run with no
    resolved model records neither, so the case is model-unlabelled (``None``);
    a missing trace for the (version, case) cell is likewise ``None``.
    """
    if trace is None:
        return None
    metadata = trace.get("metadata") or {}
    model = metadata.get("model")
    if isinstance(model, str) and model:
        return model
    for tag in trace.get("tags") or []:
        if isinstance(tag, str) and tag.startswith(_MODEL_TAG):
            return tag[len(_MODEL_TAG) :]
    return None


def _cost_of(trace: dict[str, Any]) -> float | None:
    cost = (trace.get("metadata") or {}).get("cost_usd")
    if isinstance(cost, (int, float)) and not isinstance(cost, bool):
        return float(cost)
    return None


def _supersedes(trace: dict[str, Any], current: dict[str, Any] | None, ts: str) -> bool:
    """Should ``trace`` replace ``current`` in its cell?

    Newest wins, with one exception: a graded result always beats a non-graded one
    regardless of timestamp. The grid is a promotion/comparison surface and a
    plumbing row carries zero comparative information, so re-running the sealed
    fake loop after a real eval must not erase the real signal just by being newer.
    A plumbing row only occupies a cell no graded run has claimed; it stays visible
    either way via the per-model plumbing count.
    """
    if current is None:
        return True
    trace_graded, current_graded = _is_graded(trace), _is_graded(current)
    if trace_graded != current_graded:
        return trace_graded
    return ts >= str(current.get("timestamp") or "")


def build_matrix(
    traces: list[dict[str, Any]],
    suite: str,
    limit_versions: int,
) -> EvalMatrix:
    # Latest trace per (version, case) so a re-run supersedes an older result.
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    # The model dimension needs a model-aware key: a sweep runs the SAME suite +
    # version under several models, so those traces share (version, case) and would
    # collapse to one in `latest` -- the per-model rollup must keep the latest per
    # (version, case, MODEL) instead, or a same-version sweep shows only one model
    # (#526). The displayed grid stays (version, case): one cell, newest wins.
    latest_by_model: dict[tuple[str, str, str | None], dict[str, Any]] = {}
    version_last_seen: dict[str, str] = {}
    for trace in traces:
        version = _version_of(trace)
        case_id = (trace.get("metadata") or {}).get("case_id")
        if not version or not isinstance(case_id, str):
            continue
        ts = str(trace.get("timestamp") or "")
        key = (version, case_id)
        if _supersedes(trace, latest.get(key), ts):
            latest[key] = trace
        model_key = (version, case_id, _model_of(trace))
        if _supersedes(trace, latest_by_model.get(model_key), ts):
            latest_by_model[model_key] = trace
        if ts >= version_last_seen.get(version, ""):
            version_last_seen[version] = ts

    # Columns: the most recently exercised versions, newest first, capped at N.
    versions = sorted(version_last_seen, key=lambda v: version_last_seen[v], reverse=True)[
        :limit_versions
    ]
    version_set = set(versions)
    cases = sorted({case for (version, case) in latest if version in version_set})

    def _status(version: str, case: str) -> Status:
        trace = latest.get((version, case))
        if trace is None:
            return "missing"
        return _outcome_of(trace)

    rows = [
        EvalMatrixRow(
            case_id=case,
            cells=[
                EvalCell(
                    version=version,
                    status=_status(version, case),
                    model=_model_of(latest.get((version, case))),
                )
                for version in versions
            ],
        )
        for case in cases
    ]
    summaries = _model_summaries(latest_by_model, version_set)
    return EvalMatrix(
        suite=suite,
        versions=versions,
        cases=cases,
        rows=rows,
        models=[s.model for s in summaries],
        model_summaries=summaries,
        model_version_summaries=_model_version_summaries(latest_by_model, version_set),
    )


def _model_summaries(
    latest_by_model: dict[tuple[str, str, str | None], dict[str, Any]],
    version_set: set[str],
) -> list[EvalModelSummary]:
    """Roll the shown versions up per model: pass-rate + summed cost.

    Aggregates over the latest trace per (version, case, MODEL), scoped to the
    shown version columns. Keying on the model (not just (version, case) like the
    displayed grid) is what lets a same-version sweep across N models keep all N
    rows instead of collapsing to whichever recorded last (#526). Cost sums only
    the cases that reported one; a model with no cost anywhere stays ``None``.

    A non-graded row (ADR-0055) never enters ``passed``/``total`` -- there is no
    verdict to average -- and is counted under ``plumbing`` instead, so the rows
    are surfaced rather than dropped. The exclusion is per row, not per model: a
    model with real graded rows keeps its true pass-rate even when a plumbing row
    shares its label.

    ``completed`` is a subset of ``total``: the graded rows whose turn actually
    reached a verdict rather than tripping the completion gate (see
    ``_completed``). A model with ``total > 0`` and ``completed == 0`` never
    produced a single completed turn across the whole suite -- a categorically
    different outcome from a real 0%, which the CLI reports distinctly and fails
    on (issue #622).
    """
    passed: dict[str | None, int] = {}
    total: dict[str | None, int] = {}
    completed: dict[str | None, int] = {}
    plumbing: dict[str | None, int] = {}
    cost: dict[str | None, float | None] = {}
    for (version, _case, model), trace in latest_by_model.items():
        if version not in version_set:
            continue
        status = _outcome_of(trace)
        if status == "plumbing_ok":
            plumbing[model] = plumbing.get(model, 0) + 1
            continue
        if status == "missing":
            continue
        total[model] = total.get(model, 0) + 1
        passed[model] = passed.get(model, 0) + (1 if status == "pass" else 0)
        if _completed(trace):
            completed[model] = completed.get(model, 0) + 1
        case_cost = _cost_of(trace)
        if case_cost is not None:
            cost[model] = (cost.get(model) or 0.0) + case_cost

    # Deterministic order: named models sorted, unlabelled (None) last. A model
    # whose every row was plumbing has no `total` entry, so the keys are the union
    # -- otherwise a fully sealed sweep would vanish from the rollup entirely.
    keys = sorted(set(total) | set(plumbing), key=lambda m: (m is None, m or ""))
    return [
        EvalModelSummary(
            model=model,
            passed=passed.get(model, 0),
            total=total.get(model, 0),
            cost_usd=cost.get(model),
            plumbing=plumbing.get(model, 0),
            completed=completed.get(model, 0),
        )
        for model in keys
    ]


def _model_version_summaries(
    latest_by_model: dict[tuple[str, str, str | None], dict[str, Any]],
    version_set: set[str],
) -> list[EvalModelVersionSummary]:
    """The per-``(version, model)`` slice of the same rollup ``_model_summaries``
    blends across the window.

    ``_model_summaries`` accumulates ``completed`` over every ``(version, case,
    model)`` in ``version_set`` for a model, so a model that completed on an
    older in-window sha carries ``completed > 0`` even when its turns never
    complete on the triggered sha -- masking the "never completed" outcome the
    sweep must fail on (ADR-0068, #814). Keying the accumulation by
    ``(version, model)`` instead keeps each sha's counts separate, so the CLI can
    read the triggered sha's row alone. The per-row inclusion rules are identical
    to ``_model_summaries`` (plumbing counted separately, missing skipped, graded
    rows into ``total``/``passed`` with the completion gate feeding ``completed``);
    only the aggregation key differs.
    """
    passed: dict[tuple[str, str | None], int] = {}
    total: dict[tuple[str, str | None], int] = {}
    completed: dict[tuple[str, str | None], int] = {}
    plumbing: dict[tuple[str, str | None], int] = {}
    for (version, _case, model), trace in latest_by_model.items():
        if version not in version_set:
            continue
        key = (version, model)
        status = _outcome_of(trace)
        if status == "plumbing_ok":
            plumbing[key] = plumbing.get(key, 0) + 1
            continue
        if status == "missing":
            continue
        total[key] = total.get(key, 0) + 1
        passed[key] = passed.get(key, 0) + (1 if status == "pass" else 0)
        if _completed(trace):
            completed[key] = completed.get(key, 0) + 1

    # Deterministic order: by version, then named models sorted, unlabelled last.
    # The key union mirrors `_model_summaries`: a (version, model) whose every row
    # was plumbing has no `total` entry but must still surface.
    keys = sorted(
        set(total) | set(plumbing),
        key=lambda k: (k[0], k[1] is None, k[1] or ""),
    )
    return [
        EvalModelVersionSummary(
            version=version,
            model=model,
            passed=passed.get((version, model), 0),
            total=total.get((version, model), 0),
            completed=completed.get((version, model), 0),
            plumbing=plumbing.get((version, model), 0),
        )
        for (version, model) in keys
    ]
