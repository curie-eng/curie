"""Behavioral diff of one bundle against another, across the consistency ladder.

The eval plane already answers "did this suite pass". It does not answer the two
questions a reviewer actually has in front of a bundle change:

1. What BEHAVIOR changed, relative to the bundle that is currently deployed?
2. Does that behavior hold at every rung of the ladder, or only at some?

Those are orthogonal axes over the same rows, not one ranking. A case can be
unchanged and still disagree between tiers (a latent environment problem the
change merely revealed), and a case can change identically on every tier (a real
behavior change with no environment component). Collapsing them into a single
verdict destroys exactly the distinction that makes the report actionable, so
:class:`CaseDelta` carries both and never folds one into the other.

Tier names here are the rung names the ladder already uses (``skill``, ``local``,
``cluster``; see ``cli/scripts/e2e-ladder.sh`` and ``CURIE_E2E_TIERS``). This
module does not invent a second vocabulary for them.

Honesty rules this module enforces, because a diff that overclaims is worse than
no diff at all:

* A row no grader judged is ``UNCOMPARABLE``, never "unchanged". The eval models
  already make ``EvalCaseResult.passed`` tri-state for this reason; a plumbing
  row carries no verdict, so no verdict of ours can have moved.
* A delta that does not reproduce across repeats is ``flaky``, and is reported as
  a flake rate rather than as a change.
* Tier attribution is signature matching over runner error text. It is a
  heuristic, so it is allowed to return nothing. ``None`` renders as
  ``unclassified``, and a confidently wrong cause is treated as the worse
  outcome than an admitted unknown.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import EvalCaseResult, EvalOutcome, EvalRunResult


class Tier(StrEnum):
    """A rung of the consistency ladder, named as the ladder already names it.

    Ordered weakest to heaviest, which is also the order a report reads in. If
    this module is ever promoted out of the spike, this enum belongs in a shared
    location rather than here, because the ladder script names the same three
    rungs in shell and the two would then be free to drift.
    """

    SKILL = "skill"
    LOCAL = "local"
    CLUSTER = "cluster"


TIER_ORDER: tuple[Tier, ...] = (Tier.SKILL, Tier.LOCAL, Tier.CLUSTER)


class ChangeKind(StrEnum):
    """Axis 1: how a case's behavior moved relative to the baseline bundle.

    ``VERDICT`` and ``TRAJECTORY`` are separate values because they carry very
    different review weight. A verdict move is a pass/fail flip and is loud. A
    trajectory move is the agent taking a different route to the same graded
    answer, which no pass-rate diff can see at all and which is the failure mode
    a scores-only report is blind to.

    ``CONFOUNDED`` is the value that keeps the two axes from contaminating each
    other. The change is measured PER TIER; when the tiers do not report the same
    change, the environment is varying at the same time as the bundle, and no
    honest single answer to "what did my change do" exists yet. Naming that is
    strictly more useful than picking one tier and reporting its answer as though
    the environment had been held constant, which is how an environment failure
    gets mislabeled as a behavior regression.
    """

    UNCHANGED = "unchanged"
    VERDICT = "verdict"
    TRAJECTORY = "trajectory"
    CONFOUNDED = "confounded"
    NEW = "new"
    REMOVED = "removed"
    UNCOMPARABLE = "uncomparable"


class TierAgreement(StrEnum):
    """Axis 2: whether the candidate's own tiers agree with each other.

    ``INSUFFICIENT`` is not a soft ``AGREE``. Fewer than two comparable tiers
    means the question was never asked, and saying "agree" there would sell an
    unproven consistency claim as a proven one.
    """

    AGREE = "agree"
    DISAGREE = "disagree"
    INSUFFICIENT = "insufficient"


class TierRun(BaseModel):
    """One suite run, of one bundle version, on one rung of the ladder.

    ``provenance`` records how the artifact came to exist (the command that
    produced it, or an explicit note that it was captured earlier). It is
    required and free-form on purpose: a diff report that cannot say where its
    inputs came from is not evidence, and leaving the field optional would make
    the unlabeled case the easy one.
    """

    model_config = ConfigDict(frozen=True)

    tier: Tier
    provenance: str = Field(min_length=1)
    run: EvalRunResult

    def rows(self) -> dict[str, EvalCaseResult]:
        """Return this run's case results keyed by case id.

        Returns:
            A mapping of case id to its result. Later rows win on a duplicate id,
            which cannot occur through the runner but is not worth crashing over
            in a reader.
        """
        return {result.case_id: result for result in self.run.results}


class Observation(BaseModel):
    """Every tier's run of a single bundle version: one column of the report.

    A baseline observation is the deployed bundle. A candidate observation is one
    repeat of the branch under review; several repeats are what make the flake
    rate measurable.
    """

    model_config = ConfigDict(frozen=True)

    version: str = Field(min_length=1)
    tiers: tuple[TierRun, ...]

    @model_validator(mode="after")
    def _tiers_are_unique(self) -> Observation:
        """Reject two runs claiming the same tier, which would silently shadow.

        Returns:
            The validated observation.

        Raises:
            ValueError: If the same tier appears more than once.
        """
        names = [tier_run.tier for tier_run in self.tiers]
        if len(names) != len(set(names)):
            raise ValueError("an observation cannot carry two runs of the same tier")
        return self

    def by_tier(self) -> dict[Tier, TierRun]:
        """Return this observation's runs keyed by tier."""
        return {tier_run.tier: tier_run for tier_run in self.tiers}

    def case_ids(self) -> set[str]:
        """Return every case id this observation saw on any tier."""
        return {case_id for tier_run in self.tiers for case_id in tier_run.rows()}


class TierOutcome(BaseModel):
    """What one tier reported for one case: the cell of the report's grid."""

    model_config = ConfigDict(frozen=True)

    tier: Tier
    outcome: EvalOutcome
    trajectory: tuple[str, ...] = ()
    error: str | None = None
    detail: str | None = None


class Attribution(BaseModel):
    """A named cause for a tier disagreement, plus the remedy to try.

    Produced only by an explicit signature match. The absence of an attribution
    is a first-class result, rendered as ``unclassified``, so a reader is never
    handed a guess dressed as a diagnosis.
    """

    model_config = ConfigDict(frozen=True)

    cause: str = Field(min_length=1)
    remedy: str | None = None


class CaseDelta(BaseModel):
    """One row of the report: both axes for one case, plus the evidence.

    ``flaky`` outranks both axes in rendering. A delta that did not reproduce is
    not a finding about the change, it is a finding about the suite, and
    presenting it as the former is how a diff loses a reviewer's trust.
    """

    model_config = ConfigDict(frozen=True)

    case_id: str
    change: ChangeKind
    tier_agreement: TierAgreement
    flaky: bool = False
    flake_rate: str | None = None
    baseline: tuple[TierOutcome, ...] = ()
    candidate: tuple[TierOutcome, ...] = ()
    trajectory_gained: tuple[str, ...] = ()
    trajectory_lost: tuple[str, ...] = ()
    attribution: Attribution | None = None

    @property
    def is_noteworthy(self) -> bool:
        """True if this row deserves a reviewer's attention.

        An unchanged, tier-agreeing, non-flaky row is the quiet case and is
        summarized rather than listed, so that a report with nothing to say stays
        short enough to be read when it does have something to say.
        """
        return (
            self.flaky
            or self.change is not ChangeKind.UNCHANGED
            or self.tier_agreement is TierAgreement.DISAGREE
        )


class DiffReport(BaseModel):
    """The whole comparison: every row, plus the rollups a reviewer reads first.

    One rule governs every rollup below: **a flaky row is counted only as flaky.**
    A classification that did not reproduce across repeats has not been shown to be
    anything else, so letting it also land in the changed, not-measurable,
    disagreement or unclassified counts would report suite instability as a finding
    about the bundle. That is the failure mode that gets a report ignored.
    """

    model_config = ConfigDict(frozen=True)

    baseline_version: str
    candidate_version: str
    tiers_compared: tuple[Tier, ...]
    repeats: int
    deltas: tuple[CaseDelta, ...]

    @property
    def noteworthy(self) -> tuple[CaseDelta, ...]:
        """Return only the rows worth listing, in report order."""
        return tuple(delta for delta in self.deltas if delta.is_noteworthy)

    @property
    def changed_count(self) -> int:
        """Count rows whose behavior moved identically on every compared tier.

        Deliberately excludes ``CONFOUNDED`` rows. A row where the tiers disagree
        has not been shown to be a behavior change, and counting it as one is the
        specific error that would let an allowlist gap be reported to a reviewer
        as a regression in their prompt.
        """
        moved = {ChangeKind.VERDICT, ChangeKind.TRAJECTORY}
        return sum(1 for delta in self.deltas if not delta.flaky and delta.change in moved)

    @property
    def confounded_count(self) -> int:
        """Count rows where bundle and environment both moved, so neither is provable."""
        return sum(
            1 for delta in self.deltas if not delta.flaky and delta.change is ChangeKind.CONFOUNDED
        )

    @property
    def disagreement_count(self) -> int:
        """Count rows where the candidate's own tiers disagree."""
        return sum(
            1
            for delta in self.deltas
            if not delta.flaky and delta.tier_agreement is TierAgreement.DISAGREE
        )

    @property
    def flaky_count(self) -> int:
        """Count rows whose delta failed to reproduce across repeats."""
        return sum(1 for delta in self.deltas if delta.flaky)

    @property
    def unclassified_count(self) -> int:
        """Count tier disagreements this module declined to explain.

        Surfaced as a headline number on purpose. It is the report's own admission
        of how much it could not diagnose, and hiding it would let the attribution
        table look better than it is.
        """
        return sum(
            1
            for delta in self.deltas
            if not delta.flaky
            and delta.tier_agreement is TierAgreement.DISAGREE
            and delta.attribution is None
        )


class _Signature(BaseModel):
    """One entry of the attribution table: a pattern, its cause, and its remedy."""

    model_config = ConfigDict(frozen=True)

    pattern: str
    cause: str
    remedy_template: str | None = None
    capture_group: int | None = None


# The attribution table. Deliberately short and deliberately specific: every
# entry exists because a real rung produces that text, and a generic catch-all
# entry would defeat the purpose by turning every unknown failure into a
# confident guess. Adding an entry is cheap; the cost of a wrong entry is a
# reviewer chasing a cause that is not there, so entries stay narrow.
_SIGNATURES: tuple[_Signature, ...] = (
    _Signature(
        pattern=r"egress (?:denied|blocked)(?:[^\w]+)(?:to\s+)?([A-Za-z0-9._-]+)",
        cause="egress to {0} is not in the sandbox allowlist",
        remedy_template="curie cluster apply --allow-egress-host {0}",
        capture_group=1,
    ),
    _Signature(
        pattern=r"(?:secret|credential) (?:not found|missing|unset)[:\s]+([A-Za-z0-9_]+)",
        cause="secret {0} is not present in this tier's secret store",
        remedy_template="curie secrets set {0} --agent <agent>",
        capture_group=1,
    ),
    _Signature(
        pattern=r"(?:ImagePullBackOff|ErrImagePull|failed to pull image)",
        cause="the runner image is not resolvable from inside the cluster",
        remedy_template="curie build --push",
    ),
    _Signature(
        pattern=r"(?:OOMKilled|out of memory|memory limit exceeded)",
        cause="the sandbox hit its memory limit on this tier",
        remedy_template=None,
    ),
    _Signature(
        pattern=r"(?:DeadlineExceeded|timed out after|deadline exceeded)",
        cause="the turn exceeded this tier's deadline",
        remedy_template=None,
    ),
)


def attribute(outcomes: Sequence[TierOutcome]) -> Attribution | None:
    """Explain a tier disagreement by matching runner text against known signatures.

    Only failing tiers are inspected, and only their ``error`` and ``detail``
    text. Tiers are consulted heaviest first, because a cluster-only failure is
    the case this exists for and its message is the one that names the cause.

    Args:
        outcomes: The candidate's per-tier outcomes for one case.

    Returns:
        The matched attribution, or ``None`` when no signature matched. ``None``
        is a legitimate answer and callers must render it as unclassified rather
        than substituting a guess.
    """
    ranked = sorted(
        (out for out in outcomes if out.outcome is EvalOutcome.FAIL),
        key=lambda out: TIER_ORDER.index(out.tier),
        reverse=True,
    )
    for outcome in ranked:
        haystack = " ".join(part for part in (outcome.error, outcome.detail) if part)
        if not haystack:
            continue
        for signature in _SIGNATURES:
            match = re.search(signature.pattern, haystack, re.IGNORECASE)
            if match is None:
                continue
            captured = ""
            if signature.capture_group is not None:
                captured = match.group(signature.capture_group)
            cause = signature.cause.format(captured)
            remedy = signature.remedy_template.format(captured) if signature.remedy_template else None
            return Attribution(cause=cause, remedy=remedy)
    return None


def _to_outcome(tier: Tier, result: EvalCaseResult) -> TierOutcome:
    """Project an eval case result onto the report's per-tier cell."""
    return TierOutcome(
        tier=tier,
        outcome=result.outcome,
        trajectory=tuple(result.trajectory),
        error=result.error,
        detail=result.detail,
    )


def _graded(outcomes: Iterable[TierOutcome]) -> list[TierOutcome]:
    """Return only the outcomes a grader actually judged.

    A ``plumbing_ok`` row completed but was never graded, so it can neither
    agree nor disagree with a graded row about a verdict. Filtering here is what
    keeps the fake tier from manufacturing either a false agreement or a false
    disagreement.
    """
    return [out for out in outcomes if out.outcome is not EvalOutcome.PLUMBING_OK]


def _agreement(outcomes: Sequence[TierOutcome]) -> TierAgreement:
    """Classify whether the candidate's own tiers agree about this case.

    Args:
        outcomes: The candidate's per-tier outcomes for one case.

    Returns:
        ``DISAGREE`` if two graded tiers reported different outcomes,
        ``AGREE`` if two or more graded tiers reported the same one, and
        ``INSUFFICIENT`` when fewer than two tiers were comparable.
    """
    graded = _graded(outcomes)
    if len(graded) < 2:
        return TierAgreement.INSUFFICIENT
    if len({out.outcome for out in graded}) > 1:
        return TierAgreement.DISAGREE
    return TierAgreement.AGREE


def _representative(outcomes: Sequence[TierOutcome]) -> TierOutcome | None:
    """Pick the tier whose behavior stands for the bundle in a cross-bundle diff.

    The heaviest graded tier wins, because it is the one closest to production.
    A bundle whose only rows are ungraded has no representative and returns
    ``None``, which the caller turns into ``UNCOMPARABLE``.
    """
    graded = _graded(outcomes)
    if not graded:
        return None
    return max(graded, key=lambda out: TIER_ORDER.index(out.tier))


def _classify_change(
    before: Sequence[TierOutcome],
    after: Sequence[TierOutcome],
) -> tuple[ChangeKind, tuple[str, ...], tuple[str, ...]]:
    """Compare a case across two bundles, tier by tier, and combine the answers.

    Comparing per tier is what holds the environment constant. Each tier is
    diffed against the SAME tier of the baseline, so a difference found there is
    attributable to the bundle. Only when every comparable tier reports the same
    difference is that difference reported as the change; when they disagree the
    result is ``CONFOUNDED``, because the bundle and the environment both moved
    and the report cannot separate them.

    Verdict is checked before trajectory within a tier because a pass/fail flip
    subsumes a route change in review weight: a reviewer told only "the trajectory
    moved" about a case that also started failing has been told the less important
    half.

    Args:
        before: The baseline bundle's per-tier outcomes for this case.
        after: The candidate bundle's per-tier outcomes for this case.

    Returns:
        A tuple of the combined change kind, the tool names gained, and the tool
        names lost. Gained and lost are empty for ``CONFOUNDED``: with the tiers
        disagreeing, any one tier's route delta would be presented as the
        bundle's when it may just be where that tier died.
    """
    before_by = {out.tier: out for out in before}
    after_by = {out.tier: out for out in after}
    per_tier: dict[Tier, ChangeKind] = {}
    for tier in TIER_ORDER:
        base, cand = before_by.get(tier), after_by.get(tier)
        if base is None or cand is None:
            continue
        if EvalOutcome.PLUMBING_OK in (base.outcome, cand.outcome):
            # Ungraded on at least one side of this tier's pair, so this tier has
            # no verdict to compare. Skipped rather than counted as agreement.
            continue
        if base.outcome is not cand.outcome:
            per_tier[tier] = ChangeKind.VERDICT
        elif tuple(base.trajectory) != tuple(cand.trajectory):
            per_tier[tier] = ChangeKind.TRAJECTORY
        else:
            per_tier[tier] = ChangeKind.UNCHANGED
    if not per_tier:
        return ChangeKind.UNCOMPARABLE, (), ()
    kinds = set(per_tier.values())
    if len(kinds) > 1:
        return ChangeKind.CONFOUNDED, (), ()
    kind = kinds.pop()
    if kind is ChangeKind.UNCHANGED:
        return kind, (), ()
    heaviest = max(per_tier, key=lambda tier: TIER_ORDER.index(tier))
    base_route = tuple(before_by[heaviest].trajectory)
    cand_route = tuple(after_by[heaviest].trajectory)
    gained = tuple(name for name in cand_route if name not in base_route)
    lost = tuple(name for name in base_route if name not in cand_route)
    return kind, gained, lost


def _outcomes_for(observation: Observation, case_id: str) -> tuple[TierOutcome, ...]:
    """Collect one case's per-tier outcomes from an observation, in ladder order."""
    by_tier = observation.by_tier()
    collected: list[TierOutcome] = []
    for tier in TIER_ORDER:
        tier_run = by_tier.get(tier)
        if tier_run is None:
            continue
        result = tier_run.rows().get(case_id)
        if result is None:
            continue
        collected.append(_to_outcome(tier, result))
    return tuple(collected)


def _repeat_delta(
    case_id: str,
    baseline: Observation,
    candidate: Observation,
) -> CaseDelta:
    """Compute one case's delta for a single candidate repeat.

    Args:
        case_id: The case being compared.
        baseline: The deployed bundle's observation.
        candidate: One repeat of the candidate bundle's observation.

    Returns:
        The delta for this case in this repeat, with flake fields left unset;
        folding across repeats is :func:`diff_observations`'s job.
    """
    before = _outcomes_for(baseline, case_id)
    after = _outcomes_for(candidate, case_id)
    agreement = _agreement(after)
    if not before:
        change: ChangeKind = ChangeKind.NEW
        gained: tuple[str, ...] = ()
        lost: tuple[str, ...] = ()
    elif not after:
        change, gained, lost = ChangeKind.REMOVED, (), ()
    else:
        change, gained, lost = _classify_change(before, after)
    return CaseDelta(
        case_id=case_id,
        change=change,
        tier_agreement=agreement,
        baseline=before,
        candidate=after,
        trajectory_gained=gained,
        trajectory_lost=lost,
        # Attributed only where there is a disagreement to explain. Attributing an
        # agreeing row would attach a cause to something that is not a symptom.
        attribution=attribute(after) if agreement is TierAgreement.DISAGREE else None,
    )


def _majority(
    counts: Mapping[tuple[ChangeKind, TierAgreement], int],
) -> tuple[ChangeKind, TierAgreement]:
    """Pick the winning classification, breaking a tie by a rule rather than by luck.

    Extracted and named because the tie is the interesting case and it needs to be
    testable on its own. Choosing from an unordered collection breaks a tie by hash
    order, which for StrEnum keys is string hashing and therefore varies between
    processes: the same three artifacts could then produce two different reports on
    two machines. Sorting first makes the tie-break a property of the data.

    Args:
        counts: How many repeats produced each classification.

    Returns:
        The classification with the most repeats. A tie goes to the one that sorts
        first, which is arbitrary but fixed, and fixed is the whole requirement.
    """
    return max(sorted(counts), key=lambda signature: counts[signature])


def diff_observations(
    baseline: Observation,
    candidates: Sequence[Observation],
) -> DiffReport:
    """Diff a candidate bundle against the deployed one across the ladder.

    Several candidate observations are repeats of the same bundle version. A row
    whose classification is identical in every repeat is reported as a finding. A
    row that classifies differently between repeats is reported as ``flaky`` with
    its disagreement rate and is NOT reported as a change: the suite moved, not
    the bundle, and conflating the two is how a behavioral diff earns a reputation
    for crying wolf.

    Args:
        baseline: The deployed bundle's observation, one run per tier.
        candidates: One or more repeats of the candidate bundle's observation.

    Returns:
        The assembled report, rows sorted with noteworthy ones first and then by
        case id so that output is stable across invocations.

    Raises:
        ValueError: If no candidate repeat is supplied, or if the repeats do not
            all describe the same candidate version. Diffing two different
            candidate versions as though they were repeats would silently
            attribute a real change to flake.
    """
    if not candidates:
        raise ValueError("a diff needs at least one candidate observation")
    versions = {observation.version for observation in candidates}
    if len(versions) != 1:
        raise ValueError(f"candidate repeats must all be one version, got {sorted(versions)}")

    case_ids = baseline.case_ids()
    for observation in candidates:
        case_ids |= observation.case_ids()

    repeats = len(candidates)
    deltas: list[CaseDelta] = []
    for case_id in sorted(case_ids):
        per_repeat = [_repeat_delta(case_id, baseline, cand) for cand in candidates]
        # The classification pair is the identity of a finding. Evidence text can
        # legitimately vary between repeats (a different host in a message, say)
        # without the finding itself being unstable, so folding compares the pair.
        counts: dict[tuple[ChangeKind, TierAgreement], int] = {}
        for delta in per_repeat:
            key = (delta.change, delta.tier_agreement)
            counts[key] = counts.get(key, 0) + 1
        if len(counts) == 1:
            # Every repeat classified this case identically, so the row's verdict is
            # settled. Its EVIDENCE is not: two repeats can both report a tier
            # disagreement while naming different hosts, and taking the first would
            # make the reported host depend on argument order. The canonical choice
            # is arbitrary on purpose; what matters is that it is the same for any
            # ordering of the same repeats.
            deltas.append(min(per_repeat, key=lambda delta: delta.model_dump_json()))
            continue
        majority = _majority(counts)
        agreeing = counts[majority]
        change, agreement = majority
        deltas.append(
            CaseDelta(
                case_id=case_id,
                # The majority classification, not one repeat's. It is carried for
                # information only: every rollup below skips a flaky row, because a
                # classification that did not reproduce is a fact about the suite.
                change=change,
                tier_agreement=agreement,
                flaky=True,
                flake_rate=f"{repeats - agreeing}/{repeats} repeats disagreed",
            )
        )

    tiers_compared = tuple(
        tier
        for tier in TIER_ORDER
        if tier in baseline.by_tier() and all(tier in cand.by_tier() for cand in candidates)
    )
    ordered = sorted(deltas, key=lambda delta: (not delta.is_noteworthy, delta.case_id))
    return DiffReport(
        baseline_version=baseline.version,
        candidate_version=next(iter(versions)),
        tiers_compared=tiers_compared,
        repeats=repeats,
        deltas=tuple(ordered),
    )


def observation_from_runs(
    version: str,
    runs: Mapping[Tier, EvalRunResult],
    provenance: str,
) -> Observation:
    """Build an observation from per-tier run results.

    A convenience for callers holding run results rather than parsed artifacts.

    Args:
        version: The bundle version every run describes.
        runs: One run result per tier.
        provenance: How these runs came to exist, recorded on every tier.

    Returns:
        The assembled observation.
    """
    return Observation(
        version=version,
        tiers=tuple(TierRun(tier=tier, provenance=provenance, run=run) for tier, run in runs.items()),
    )
