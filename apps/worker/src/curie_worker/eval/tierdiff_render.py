"""Render a :class:`DiffReport` for the two places a reviewer meets it.

Two surfaces, one report: a terminal view for the author mid-change, and a
Markdown view for the pull request where the decision is actually made. Both are
pure functions of the report so neither can show something the other cannot.

Layout rules, both surfaces:

* Quiet rows are counted, not listed. A report that prints every unchanged case
  is a report nobody reads by the third time they see it, and then the one row
  that matters scrolls past.
* Every claim carries its evidence on the line under it. A reviewer should never
  have to open a dashboard to learn what "changed" meant.
* Prose stays inside 100 columns, so a terminal recording is legible without
  reflow and so the Markdown never grows a horizontal scrollbar. A line carrying
  a shell command is exempt: a remedy folded across two lines cannot be pasted,
  and that costs the reader more than a long line does.
"""

from __future__ import annotations

import textwrap

from .models import EvalOutcome
from .tierdiff import CaseDelta, ChangeKind, DiffReport, TierAgreement, TierOutcome

_WIDTH = 100

# Evidence sits under its row rather than in a right-hand column, which gives a
# long remedy command the full line width instead of a narrow one.
_EVIDENCE_INDENT = " " * 6

# The reviewer-facing label for each change kind. UNCOMPARABLE reads as an
# admission rather than a verdict, because that is what it is.
_CHANGE_LABEL: dict[ChangeKind, str] = {
    ChangeKind.UNCHANGED: "unchanged",
    ChangeKind.VERDICT: "CHANGED",
    ChangeKind.TRAJECTORY: "ROUTE CHANGED",
    ChangeKind.CONFOUNDED: "ENVIRONMENT DIFFERS",
    ChangeKind.NEW: "new case",
    ChangeKind.REMOVED: "case removed",
    ChangeKind.UNCOMPARABLE: "not comparable",
}

_OUTCOME_LABEL: dict[EvalOutcome, str] = {
    EvalOutcome.PASS: "pass",
    EvalOutcome.FAIL: "FAIL",
    EvalOutcome.PLUMBING_OK: "ungraded",
}


def _headline(delta: CaseDelta) -> str:
    """Return the single label that names why this row is listed.

    A flake outranks both axes, and a tier disagreement outranks a change,
    because that ordering matches what a reviewer must decide first: whether the
    finding is real, then whether it is an environment problem, then what moved.
    """
    if delta.flaky:
        return "FLAKY"
    if delta.tier_agreement is TierAgreement.DISAGREE:
        return "TIER DISAGREEMENT"
    return _CHANGE_LABEL[delta.change]


def _tier_cells(outcomes: tuple[TierOutcome, ...]) -> str:
    """Render the per-tier grid for one case as ``skill pass  local pass  cluster FAIL``."""
    return "  ".join(f"{out.tier.value} {_OUTCOME_LABEL[out.outcome]}" for out in outcomes)


def _evidence(delta: CaseDelta) -> list[tuple[str, bool]]:
    """Build the evidence lines shown under a row's headline.

    Args:
        delta: The row being rendered.

    Returns:
        Zero or more ``(text, wrappable)`` pairs. ``wrappable`` is False for a
        line that is a shell command: a command is unpasteable if it is broken
        anywhere at all, so it is exempt from the layout width rather than being
        folded to respect it. Labeling the line is what keeps that decision in one
        place instead of leaving the renderer to guess from the text.
    """
    lines: list[tuple[str, bool]] = []
    if delta.flaky:
        # A flaky row renders its rate and stops. Everything else on the row came from
        # one repeat, and which repeat that is depends on the order the caller listed
        # them in; printing a per-tier grid or a cause here would present that as the
        # candidate's behavior when the repeats did not agree it was.
        rate = delta.flake_rate or "repeats disagreed"
        return [(f"{rate}; reported as suite instability, not as a change", True)]
    if delta.change is ChangeKind.CONFOUNDED:
        lines.append(("behavior not measurable here: the environment moved too. Fix the tier gap first.", True))
    if delta.change is ChangeKind.VERDICT:
        before = _verdict_of(delta.baseline)
        after = _verdict_of(delta.candidate)
        lines.append((f"verdict {before} -> {after}", True))
    if delta.trajectory_lost:
        lines.append((f"tools no longer called: {', '.join(delta.trajectory_lost)}", True))
    if delta.trajectory_gained:
        lines.append((f"tools newly called: {', '.join(delta.trajectory_gained)}", True))
    if delta.change is ChangeKind.TRAJECTORY and not (delta.trajectory_gained or delta.trajectory_lost):
        # Same tools, different order. Without this line the row would announce
        # ROUTE CHANGED and then offer no evidence at all, which reads as a bug in
        # the report rather than as the finding it is. Order is behavior: refunding
        # before notifying is not the same act as notifying before refunding.
        lines.append(("same tools, called in a different order", True))
    if delta.tier_agreement is TierAgreement.DISAGREE:
        lines.append((_tier_cells(delta.candidate), True))
        if delta.attribution is None:
            # The report saying it does not know is the designed outcome here. A
            # plausible-sounding guess would cost a reviewer more than this line.
            lines.append(("cause: unclassified (no signature matched the runner output)", True))
        else:
            lines.append((f"cause: {delta.attribution.cause}", True))
            if delta.attribution.remedy:
                lines.append((f"fix:   {delta.attribution.remedy}", False))
    return lines


def _verdict_of(outcomes: tuple[TierOutcome, ...]) -> str:
    """Name the verdict of the heaviest graded tier in a set of outcomes."""
    graded = [out for out in outcomes if out.outcome is not EvalOutcome.PLUMBING_OK]
    if not graded:
        return "ungraded"
    return _OUTCOME_LABEL[graded[-1].outcome]


def _summary_counts(report: DiffReport) -> str:
    """Return the one-line rollup, including what the report could not explain."""
    return (
        f"{len(report.deltas)} cases   "
        f"{report.changed_count} changed   "
        f"{report.confounded_count} not measurable   "
        f"{report.disagreement_count} tier disagreement(s)   "
        f"{report.unclassified_count} unclassified   "
        f"{report.flaky_count} flaky"
    )


def _wrapped(line: str, wrappable: bool) -> list[str]:
    """Wrap one evidence line to the layout width, indenting continuations.

    Truncation is not an option here: the lines this renders include the remedy
    command a reader is meant to copy, and half a command is worse than no
    command because it looks complete.

    Neither is folding a command. A remedy split over two lines cannot be
    selected and pasted in one go, and wrapping cannot be made safe for it at any
    width, so a non-wrappable line is emitted whole and is allowed to overflow the
    layout. Prose wraps; commands overflow. Hyphen breaking is off for the prose
    too, because a hostname inside a sentence is just as unusable when split.

    Args:
        line: The evidence text for one bullet.
        wrappable: False for a line that must stay intact, such as a command.

    Returns:
        One or more terminal lines, already indented. A line exceeds
        :data:`_WIDTH` only when it is non-wrappable or holds one long token.
    """
    if not wrappable:
        return [f"{_EVIDENCE_INDENT}{line}"]
    body = textwrap.wrap(
        line,
        width=_WIDTH - len(_EVIDENCE_INDENT) - 2,
        break_long_words=False,
        break_on_hyphens=False,
    ) or [""]
    first, *rest = body
    return [f"{_EVIDENCE_INDENT}{first}"] + [f"{_EVIDENCE_INDENT}  {part}" for part in rest]


def render_terminal(report: DiffReport) -> str:
    """Render the report as plain text for a terminal.

    Args:
        report: The assembled diff report.

    Returns:
        The full report text, without a trailing newline.
    """
    tiers = ", ".join(tier.value for tier in report.tiers_compared) or "none in common"
    out: list[str] = [
        f"Behavior diff: candidate {report.candidate_version} vs deployed {report.baseline_version}",
        f"Tiers compared: {tiers}   Repeats: {report.repeats}",
        "",
    ]
    noteworthy = report.noteworthy
    if not noteworthy:
        out.append("  No behavioral difference, on any compared tier.")
    for delta in noteworthy:
        # The case id column is padded to a fixed width so headlines line up; a
        # long id overflows its column rather than being truncated, because a
        # truncated case id is not a case id.
        out.append(f"  {delta.case_id:<28} {_headline(delta)}")
        for line, wrappable in _evidence(delta):
            out.extend(_wrapped(line, wrappable))
    quiet = len(report.deltas) - len(noteworthy)
    out.extend(["", f"  {quiet} case(s) unchanged and tier-consistent, not listed."])
    out.extend(["", f"  {_summary_counts(report)}"])
    return "\n".join(out)


def render_markdown(report: DiffReport) -> str:
    """Render the report as the Markdown body of a pull request comment.

    Args:
        report: The assembled diff report.

    Returns:
        The comment body, without a trailing newline.
    """
    tiers = ", ".join(f"`{tier.value}`" for tier in report.tiers_compared) or "none in common"
    out: list[str] = [
        f"### Behavior diff vs deployed `{report.baseline_version}`",
        "",
        f"Candidate `{report.candidate_version}` | tiers {tiers} | {report.repeats} repeat(s)",
        "",
        f"`{_summary_counts(report)}`",
        "",
    ]
    noteworthy = report.noteworthy
    if not noteworthy:
        out.append("No behavioral difference, on any compared tier.")
        return "\n".join(out)
    for delta in noteworthy:
        out.append(f"**`{delta.case_id}`** {_headline(delta)}")
        out.append("")
        for line, _wrappable in _evidence(delta):
            # Markdown reflows on its own, so the wrappable label is irrelevant
            # here; the backticks are what keep a remedy selectable.
            out.append(f"- {line}")
        out.append("")
    quiet = len(report.deltas) - len(noteworthy)
    out.append(f"<sub>{quiet} case(s) unchanged and tier-consistent, not listed.</sub>")
    return "\n".join(out)
