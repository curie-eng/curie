import uuid
from typing import Literal

from pydantic import BaseModel, field_validator

from .common import validate_model_override


class GraderOut(BaseModel):
    """A deterministic grader, mirroring the frozen eval-case Grader shape
    (`apps/worker/schema/eval-cases.schema.json`). Do not let this drift from the
    worker's `Grader` model."""

    kind: Literal["exact", "contains", "regex", "tool_called"]
    expected: str
    case_sensitive: bool = False


class EvalCaseOut(BaseModel):
    """An eval case conforming to the frozen eval-case format (#8, ADR-0019):
    an input prompt plus the grader that judges the answer. Emitted by the
    promote-a-trace-to-an-eval-case endpoint (#259).

    ``shared_history`` mirrors the worker's ``EvalCase`` field (#550, ADR-0051):
    a promoted trace is a standalone case, so it emits the ``False`` default
    (fresh conversation). Kept here to satisfy the schema field-parity gate; the
    promote endpoint has no reason to mint a history-chained case.

    ``expect_status`` mirrors the frozen ``ExpectedStatus`` (#262, ADR-0053): the
    terminal session status the case asserts, default ``done``. A promoted trace
    is a completed conversation, so the emitted case keeps the default; a human
    edits it to ``awaiting-approval`` when the case should assert an approval gate
    held. Do not let this literal drift from the schema's ``ExpectedStatus`` enum."""

    id: str
    input: str
    grader: GraderOut
    shared_history: bool = False
    expect_status: Literal["done", "awaiting-approval"] = "done"


class EvalCell(BaseModel):
    """One cell of the eval matrix: a case's result on a version.

    ``model`` is the model the result was produced under (the matrix's model
    dimension), or ``None`` when the recording run carried no model tag.

    ``detail`` is the scorer's optional explanation for the verdict. It is
    distinct from a trace's ``error``, which identifies a turn that did not
    complete.

    ``plumbing_ok`` means the case ran to completion but no grader judged it (the
    fake-model tier, ADR-0055). It is a distinct status rather than a pass or a
    fail because it is neither: the fake answers from a canned script, so its cell
    carries no comparative information and must never read as a green promotion
    gate.
    """

    version: str
    status: Literal["pass", "fail", "plumbing_ok", "missing"]
    model: str | None = None
    detail: str | None = None
    stream_id: str | None = None
    scorer: Literal["grader", "trajectory"] | None = None
    case_count: int | None = None


class EvalMatrixRow(BaseModel):
    """One row of the eval matrix: a case across every version column."""

    case_id: str
    cells: list[EvalCell]


class EvalModelSummary(BaseModel):
    """A per-model rollup across the suite: pass-rate and total cost.

    The model dimension of the matrix (issue #255): the same suite run across
    models is sliceable here into ``passed/total`` pass-rate and summed
    ``cost_usd`` per model, so BYO-model work can compare which models a use case
    tolerates and at what cost. ``cost_usd`` is ``None`` when no case under this
    model reported a cost (e.g. the fake-model path), rather than a misleading 0.

    ``plumbing`` counts the rows that ran but were never graded (ADR-0055). They
    are excluded from ``passed``/``total``: counted as passes the fake model reads
    100% and counted as fails it reads 0%, both fabricated. The count keeps those
    rows visible instead of silently dropping them, so a model whose only rows are
    plumbing still appears with ``total == 0``.

    ``completed`` counts the graded rows (within ``total``) whose turn actually
    reached a verdict, as opposed to a graded FAIL that never completed at all
    (a classified failure, a turn that ended in the wrong terminal status, or a
    transport/runner exception -- see ``EvalCaseResult.error`` in the worker).
    ``total`` alone cannot tell a real 0% (every case completed and the grader
    said no) apart from a model that never produced one completed turn (issue
    #622, #526 AC4): a model whose id does not resolve, or whose runner boots but
    never answers, drives every case through the SAME classified-failure path a
    real model's bad answer never touches. A sweep row with ``total > 0`` and
    ``completed == 0`` is that distinct outcome, not a real (if unlucky) 0%.
    """

    model: str | None = None
    passed: int
    total: int
    cost_usd: float | None = None
    plumbing: int = 0
    completed: int = 0

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 0.0


class EvalModelVersionSummary(BaseModel):
    """A per-(version, model) rollup: the graded aggregates scoped to a single
    version column, not rolled across the whole shown window.

    ``EvalModelSummary`` sums ``completed`` over EVERY in-window version for a
    model. That blend can mask a triggered sha that lands all-incomplete (the
    model boots but never completes a turn on the new code) when a prior in-window
    sha completed cases for that same model: the blended ``completed`` stays ``> 0``
    from the old sha, so the "never completed" outcome the sweep must fail on
    (ADR-0068, #622) is hidden and a blended pass-rate is reported as a real
    comparison (issue #814). This per-version breakdown exposes the
    ``(version, model)`` dimension so a caller -- the CLI ``--model`` sweep, which
    knows the sha it just triggered -- can scope ``completed``/never-completed to
    that one sha instead of the window.

    Fields mirror the graded subset of ``EvalModelSummary`` (``cost_usd`` is not
    sliced per version, since the sweep does not compare cost per sha). It is
    additive and defaulted the way ``completed``/``plumbing`` already are: a caller
    that predates the field reads an empty list and degrades to the blended
    reading rather than misreporting.
    """

    version: str
    model: str | None = None
    passed: int
    total: int
    completed: int = 0
    plumbing: int = 0

    @property
    def pass_rate(self) -> float:
        return self.passed / self.total if self.total else 0.0


class EvalMatrix(BaseModel):
    """The eval matrix grid: rows = cases, columns = versions (most recent first).

    ``models`` and ``model_summaries`` add the model dimension: the distinct
    models observed across the fetched traces, and a pass-rate + cost rollup per
    model for BYO-model comparison. ``model_version_summaries`` slices that same
    rollup per ``(version, model)`` so a caller can scope completion to a single
    triggered sha rather than the blended window (#814). They are additive; the
    version grid is unchanged.
    """

    suite: str
    versions: list[str]
    cases: list[str]
    rows: list[EvalMatrixRow]
    models: list[str | None] = []
    model_summaries: list[EvalModelSummary] = []
    model_version_summaries: list[EvalModelVersionSummary] = []


class EvalTriggerRequest(BaseModel):
    """Ask for an on-demand platform eval run for an agent (issue #10).

    Enqueues the same EvalJob the git-push fan-out uses, minus the
    push-only gate. With no version_id the agent's active dev deployment is
    evaluated; suite falls back to Settings.eval_default_suite when omitted.
    """

    agent_id: uuid.UUID
    version_id: uuid.UUID | None = None
    suite: str | None = None
    target_url: str | None = None
    # The model to evaluate under (#526): booted into the eval sandbox and used as
    # the run's matrix model dimension. None uses the worker default. A sweep posts
    # one trigger per model, then reads GET /evals/matrix sliced by model back.
    # Blank and whitespace-only are refused (#1389): "" is not None, so it won the
    # binding's override ternary but was then falsy, CURIE_MODEL was never emitted
    # and the run booted the BYO endpoint's own default while the matrix labelled
    # the row ''. Whitespace was worse -- it passed the falsy check and rode
    # through as a garbage model id. Send null to get the worker default.
    model: str | None = None

    _check_model = field_validator("model")(validate_model_override)


class EvalTriggerResult(BaseModel):
    """The enqueued eval job's stream id plus the resolved job identity."""

    stream_id: str
    agent_id: uuid.UUID
    version_id: uuid.UUID
    sha: str
    suite: str
    bundle_ref: str | None
    # Echoes the requested model (#526) so a sweep caller can key each enqueued
    # job to the model it will land under in the matrix; None = worker default.
    model: str | None = None


class EvalReportResult(BaseModel):
    """The committed GitHub commit-status state for a reported eval run."""

    state: str
    sha: str
