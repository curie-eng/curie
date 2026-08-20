"""Tests for the cross-tier behavioral diff.

The tests are organized around the claims the report makes, because a report is
only as good as its refusal to overclaim. Every honesty rule in ``tierdiff`` has
a test whose failure would mean the report lies: ungraded rows are not compared,
unstable rows are not called changes, environment-confounded rows are not called
behavior changes, and an unknown cause stays unknown.
"""

from __future__ import annotations

import pytest
from curie_worker.eval.models import EvalCaseResult, EvalOutcome, EvalRunResult
from curie_worker.eval.tierdiff import (
    ChangeKind,
    Observation,
    Tier,
    TierAgreement,
    TierOutcome,
    TierRun,
    attribute,
    diff_observations,
)
from curie_worker.eval.tierdiff_render import render_markdown, render_terminal


def _result(
    case_id: str,
    outcome: EvalOutcome = EvalOutcome.PASS,
    trajectory: tuple[str, ...] = (),
    error: str | None = None,
) -> EvalCaseResult:
    """Build one case result with only the fields this module reads."""
    return EvalCaseResult(
        case_id=case_id,
        outcome=outcome,
        output="",
        latency_ms=1.0,
        trajectory=trajectory,
        error=error,
    )


def _observation(version: str, per_tier: dict[Tier, list[EvalCaseResult]]) -> Observation:
    """Build an observation from per-tier case-result lists."""
    return Observation(
        version=version,
        tiers=tuple(
            TierRun(
                tier=tier,
                provenance="unit test",
                run=EvalRunResult(version=version, suite="s", results=results),
            )
            for tier, results in per_tier.items()
        ),
    )


def _uniform(version: str, results: list[EvalCaseResult]) -> Observation:
    """Build an observation where all three tiers behaved identically."""
    return _observation(version, {tier: list(results) for tier in Tier})


def _only(report, case_id: str):
    """Return the single delta for ``case_id``."""
    matches = [delta for delta in report.deltas if delta.case_id == case_id]
    assert len(matches) == 1, f"expected exactly one {case_id} row"
    return matches[0]


class TestChangeAxis:
    """Axis 1: what moved relative to the deployed bundle."""

    def test_identical_bundles_report_no_behavioral_difference(self) -> None:
        rows = [_result("a", trajectory=("t1",))]
        report = diff_observations(_uniform("v1", rows), [_uniform("v1", rows)])
        assert _only(report, "a").change is ChangeKind.UNCHANGED
        assert report.noteworthy == ()

    def test_a_route_change_is_found_even_though_the_verdict_did_not_move(self) -> None:
        """The whole argument for diffing trajectories rather than pass-rates.

        Both bundles pass this case, so every score-based comparison reports
        nothing. The agent nonetheless stopped calling a tool.
        """
        base = _uniform("v1", [_result("a", trajectory=("lookup", "refund", "escalate"))])
        cand = _uniform("v2", [_result("a", trajectory=("lookup", "escalate"))])
        delta = _only(diff_observations(base, [cand]), "a")
        assert delta.change is ChangeKind.TRAJECTORY
        assert delta.trajectory_lost == ("refund",)
        assert delta.trajectory_gained == ()

    def test_reordering_the_same_tools_is_a_route_change(self) -> None:
        """A route is a sequence, so comparing it as a set would miss this entirely.

        Refunding before notifying and notifying before refunding are different
        acts with the same tool set. Nothing else in this file would fail if the
        comparison were set-based, which is why this test exists on its own.
        """
        base = _uniform("v1", [_result("a", trajectory=("refund", "notify"))])
        cand = _uniform("v2", [_result("a", trajectory=("notify", "refund"))])
        delta = _only(diff_observations(base, [cand]), "a")
        assert delta.change is ChangeKind.TRAJECTORY
        assert delta.trajectory_gained == ()
        assert delta.trajectory_lost == ()

    def test_a_reorder_row_still_says_what_it_found(self) -> None:
        """A headline with no evidence under it reads as a broken report."""
        base = _uniform("v1", [_result("a", trajectory=("refund", "notify"))])
        cand = _uniform("v2", [_result("a", trajectory=("notify", "refund"))])
        text = render_terminal(diff_observations(base, [cand]))
        assert "different order" in text

    def test_a_verdict_flip_outranks_the_route_change_it_came_with(self) -> None:
        base = _uniform("v1", [_result("a", trajectory=("x",))])
        cand = _uniform("v2", [_result("a", EvalOutcome.FAIL, trajectory=("y",))])
        assert _only(diff_observations(base, [cand]), "a").change is ChangeKind.VERDICT

    def test_a_case_only_the_candidate_has_is_new_not_changed(self) -> None:
        base = _uniform("v1", [_result("a")])
        cand = _uniform("v2", [_result("a"), _result("b")])
        assert _only(diff_observations(base, [cand]), "b").change is ChangeKind.NEW

    def test_a_case_only_the_baseline_has_is_removed(self) -> None:
        base = _uniform("v1", [_result("a"), _result("b")])
        cand = _uniform("v2", [_result("a")])
        assert _only(diff_observations(base, [cand]), "b").change is ChangeKind.REMOVED

    def test_an_ungraded_row_is_uncomparable_and_never_unchanged(self) -> None:
        """A plumbing row carries no verdict, so no verdict of ours can have moved.

        Reporting ``unchanged`` here would be the false green: it would tell a
        reviewer the behavior held when nothing was ever graded.
        """
        base = _uniform("v1", [_result("a", EvalOutcome.PLUMBING_OK)])
        cand = _uniform("v2", [_result("a", EvalOutcome.PLUMBING_OK)])
        delta = _only(diff_observations(base, [cand]), "a")
        assert delta.change is ChangeKind.UNCOMPARABLE
        assert delta.change is not ChangeKind.UNCHANGED


class TestTierAxis:
    """Axis 2: whether the candidate's own tiers agree with each other."""

    def test_tiers_that_disagree_are_reported_as_a_disagreement(self) -> None:
        base = _uniform("v1", [_result("a")])
        cand = _observation(
            "v2",
            {
                Tier.SKILL: [_result("a")],
                Tier.LOCAL: [_result("a")],
                Tier.CLUSTER: [_result("a", EvalOutcome.FAIL)],
            },
        )
        assert _only(diff_observations(base, [cand]), "a").tier_agreement is TierAgreement.DISAGREE

    def test_one_comparable_tier_is_insufficient_not_agreement(self) -> None:
        """Fewer than two graded tiers means the consistency question was never asked."""
        base = _observation("v1", {Tier.SKILL: [_result("a")]})
        cand = _observation("v2", {Tier.SKILL: [_result("a")]})
        delta = _only(diff_observations(base, [cand]), "a")
        assert delta.tier_agreement is TierAgreement.INSUFFICIENT
        assert delta.tier_agreement is not TierAgreement.AGREE

    def test_an_ungraded_tier_cannot_manufacture_a_disagreement(self) -> None:
        """The fake tier completes without a grade, so it cannot dissent about one."""
        base = _uniform("v1", [_result("a")])
        cand = _observation(
            "v2",
            {
                Tier.SKILL: [_result("a", EvalOutcome.PLUMBING_OK)],
                Tier.LOCAL: [_result("a")],
                Tier.CLUSTER: [_result("a")],
            },
        )
        assert _only(diff_observations(base, [cand]), "a").tier_agreement is TierAgreement.AGREE


class TestAxesDoNotContaminateEachOther:
    """The reason the two candidates belong in one report rather than two."""

    def test_an_environment_only_failure_is_not_counted_as_a_behavior_change(self) -> None:
        """The defect this design exists to prevent.

        The bundle passes this case on the two lighter rungs and fails only on the
        cluster. Picking the heaviest tier as the single point of comparison would
        report a verdict regression, sending a reviewer to audit a prompt when the
        actual problem is an allowlist.
        """
        base = _uniform("v1", [_result("a")])
        cand = _observation(
            "v2",
            {
                Tier.SKILL: [_result("a")],
                Tier.LOCAL: [_result("a")],
                Tier.CLUSTER: [_result("a", EvalOutcome.FAIL, error="egress denied to api.example.com")],
            },
        )
        report = diff_observations(base, [cand])
        delta = _only(report, "a")
        assert delta.change is ChangeKind.CONFOUNDED
        assert report.changed_count == 0
        assert report.confounded_count == 1

    def test_a_uniform_change_stays_a_change_when_every_tier_agrees(self) -> None:
        base = _uniform("v1", [_result("a", trajectory=("x",))])
        cand = _uniform("v2", [_result("a", trajectory=("x", "y"))])
        report = diff_observations(base, [cand])
        assert _only(report, "a").change is ChangeKind.TRAJECTORY
        assert report.changed_count == 1
        assert report.confounded_count == 0

    def test_a_confounded_row_reports_no_route_delta(self) -> None:
        """A failing tier's truncated route is not the bundle's route change."""
        base = _uniform("v1", [_result("a", trajectory=("x", "y"))])
        cand = _observation(
            "v2",
            {
                Tier.SKILL: [_result("a", trajectory=("x", "y"))],
                Tier.LOCAL: [_result("a", trajectory=("x", "y"))],
                Tier.CLUSTER: [_result("a", EvalOutcome.FAIL, trajectory=("x",))],
            },
        )
        delta = _only(diff_observations(base, [cand]), "a")
        assert delta.trajectory_lost == ()
        assert delta.trajectory_gained == ()


class TestFlake:
    """A delta that does not reproduce is a fact about the suite, not the bundle."""

    def test_a_delta_present_in_only_some_repeats_is_folded_into_a_flake_rate(self) -> None:
        base = _uniform("v1", [_result("a")])
        steady = _uniform("v2", [_result("a")])
        odd = _uniform("v2", [_result("a", EvalOutcome.FAIL)])
        report = diff_observations(base, [steady, steady, odd])
        delta = _only(report, "a")
        assert delta.flaky is True
        assert delta.flake_rate == "1/3 repeats disagreed"
        assert report.flaky_count == 1

    @pytest.mark.parametrize("reversed_order", [False, True])
    def test_a_flaky_row_is_not_also_counted_as_a_change(self, reversed_order) -> None:
        """Crying wolf is the failure mode that gets a report ignored.

        Both orders are asserted deliberately. An earlier version of this test
        listed the steady observation first and passed for that reason alone:
        reversing the two made the flaky row land in ``changed_count``, which the
        ADR forbids and which this test claimed to prevent.
        """
        base = _uniform("v1", [_result("a")])
        steady = _uniform("v2", [_result("a")])
        odd = _uniform("v2", [_result("a", EvalOutcome.FAIL)])
        candidates = [odd, steady] if reversed_order else [steady, odd]
        report = diff_observations(base, candidates)
        assert report.flaky_count == 1
        assert report.changed_count == 0

    def test_a_flaky_row_is_counted_only_as_flaky(self) -> None:
        """Every rollup skips a flaky row, not just the changed count.

        The majority here is deliberately a REAL finding, two repeats out of three,
        so the skip is the only thing keeping these counts at zero. An earlier
        version of this test used a two-way tie, where the tie-break happened to
        land on ``unchanged`` and the counts were zero whether the rollups skipped
        flaky rows or not.
        """
        base = _uniform("v1", [_result("a")])
        steady = _uniform("v2", [_result("a")])
        disagreeing = _observation(
            "v2",
            {
                Tier.SKILL: [_result("a")],
                Tier.LOCAL: [_result("a")],
                Tier.CLUSTER: [_result("a", EvalOutcome.FAIL, error="nothing recognizable")],
            },
        )
        report = diff_observations(base, [disagreeing, disagreeing, steady])
        delta = _only(report, "a")
        assert delta.flaky is True
        # The majority classification is carried, and is a finding on both axes.
        assert delta.change is ChangeKind.CONFOUNDED
        assert delta.tier_agreement is TierAgreement.DISAGREE
        # And none of it is counted, because it did not reproduce.
        assert report.flaky_count == 1
        assert report.changed_count == 0
        assert report.confounded_count == 0
        assert report.disagreement_count == 0
        assert report.unclassified_count == 0

    def test_a_flaky_majority_that_is_a_real_change_is_still_not_counted(self) -> None:
        """The case where the skip is the only thing holding the count at zero.

        Two repeats out of three show a uniform verdict flip on every tier, which is
        a genuine `changed` classification, and the third does not. Without the skip
        this lands in ``changed_count`` while the row is flaky, which is exactly what
        the ADR forbids.
        """
        base = _uniform("v1", [_result("a")])
        flipped = _uniform("v2", [_result("a", EvalOutcome.FAIL)])
        held = _uniform("v2", [_result("a")])
        report = diff_observations(base, [flipped, flipped, held])
        delta = _only(report, "a")
        assert delta.flaky is True
        assert delta.change is ChangeKind.VERDICT
        assert report.changed_count == 0

    def test_a_stable_row_reports_the_same_evidence_whatever_the_order(self) -> None:
        """Identical classification does not mean identical evidence.

        Every repeat here reports a tier disagreement, so the row is not flaky, but
        each names a different host. Taking the first repeat's evidence would make
        the host in the report, and the remedy command printed under it, depend on
        the order the artifacts were listed in.
        """
        import itertools

        base = _uniform("v1", [_result("a")])
        candidates = [
            _observation(
                "v2",
                {
                    Tier.SKILL: [_result("a")],
                    Tier.LOCAL: [_result("a")],
                    Tier.CLUSTER: [_result("a", EvalOutcome.FAIL, error=f"egress denied to {host}")],
                },
            )
            for host in ("alpha.example.com", "bravo.example.com", "charlie.example.com")
        ]
        rendered = {
            render_terminal(diff_observations(base, list(order)))
            for order in itertools.permutations(candidates)
        }
        assert len(rendered) == 1, "the reported host depended on argument order"

    @pytest.mark.parametrize(
        "first,second",
        [
            (ChangeKind.UNCHANGED, ChangeKind.VERDICT),
            (ChangeKind.VERDICT, ChangeKind.TRAJECTORY),
            (ChangeKind.CONFOUNDED, ChangeKind.UNCOMPARABLE),
            (ChangeKind.NEW, ChangeKind.REMOVED),
            (ChangeKind.TRAJECTORY, ChangeKind.UNCHANGED),
        ],
    )
    def test_a_tie_goes_to_the_signature_that_sorts_first(self, first, second) -> None:
        """The rule, asserted directly, over several pairs.

        One pair is a weak guard: choosing from an unordered set can coincide with
        the sorted answer for that particular pair, and measurement showed it doing
        exactly that for the pair this fold actually hits. Several pairs make the
        coincidence unlikely rather than assumed.
        """
        from curie_worker.eval.tierdiff import _majority

        counts = {
            (first, TierAgreement.AGREE): 1,
            (second, TierAgreement.AGREE): 1,
        }
        expected = min([(first, TierAgreement.AGREE), (second, TierAgreement.AGREE)])
        assert _majority(counts) == expected

    def test_a_tie_is_broken_the_same_way_in_a_fresh_process(self, tmp_path) -> None:
        """A tie resolves identically under different hash seeds.

        Scoped honestly: this checks cross-process determinism end to end, and it is
        NOT a sufficient guard against choosing from an unordered set. Measurement
        across six seeds showed the unsorted choice agreeing with the sorted one for
        the pair this fold hits, so the test above is what pins the rule and this one
        checks that nothing else in the path has become seed-dependent.
        """
        import os
        import subprocess
        import sys

        script = tmp_path / "tie.py"
        script.write_text(
            "from curie_worker.eval.models import EvalCaseResult, EvalOutcome, EvalRunResult\n"
            "from curie_worker.eval.tierdiff import Observation, Tier, TierRun, diff_observations\n"
            "def r(o):\n"
            "    return EvalCaseResult(case_id='a', outcome=o, output='', latency_ms=1.0)\n"
            "def obs(v, o):\n"
            "    return Observation(version=v, tiers=tuple(\n"
            "        TierRun(tier=t, provenance='x',\n"
            "                run=EvalRunResult(version=v, suite='s', results=[r(o)])) for t in Tier))\n"
            "base = obs('v1', EvalOutcome.PASS)\n"
            "steady = obs('v2', EvalOutcome.PASS)\n"
            "odd = obs('v2', EvalOutcome.FAIL)\n"
            "print(diff_observations(base, [steady, odd]).model_dump_json())\n",
            encoding="utf-8",
        )
        outputs = set()
        for seed in ("0", "1", "7", "12345"):
            env = dict(os.environ, PYTHONHASHSEED=seed)
            done = subprocess.run(
                [sys.executable, str(script)],
                capture_output=True, text=True, env=env, check=True,
            )
            outputs.add(done.stdout)
        assert len(outputs) == 1, "the tie broke differently under a different hash seed"

    def test_a_tied_flake_resolves_the_same_way_whatever_the_order(self) -> None:
        """A two-way tie is where an unsorted max stops being deterministic."""
        base = _uniform("v1", [_result("a")])
        steady = _uniform("v2", [_result("a")])
        odd = _uniform("v2", [_result("a", EvalOutcome.FAIL)])
        forward = diff_observations(base, [steady, odd])
        backward = diff_observations(base, [odd, steady])
        assert forward.model_dump_json() == backward.model_dump_json()

    def test_the_report_is_a_function_of_the_candidate_set_not_its_order(self) -> None:
        """The guard for the whole class of defect the fold used to have.

        Three repeats, every permutation, one rendered report. Anything that reads
        ``per_repeat[0]``, or takes ``max`` over an unsorted set of ties, makes the
        headline depend on the order the artifacts were listed in, which tells the
        reader about their argv rather than about their bundle.
        """
        import itertools

        base = _uniform("v1", [_result("steady"), _result("wobbly")])
        good = _uniform("v2", [_result("steady"), _result("wobbly")])
        bad = _observation(
            "v2",
            {
                Tier.SKILL: [_result("steady"), _result("wobbly")],
                Tier.LOCAL: [_result("steady"), _result("wobbly")],
                Tier.CLUSTER: [
                    _result("steady"),
                    _result("wobbly", EvalOutcome.FAIL, error="egress denied to api.example.com"),
                ],
            },
        )
        reports = [diff_observations(base, list(order)) for order in itertools.permutations([good, good, bad])]
        # The MODEL, not just the rendering. A flaky row deliberately renders only its
        # rate, so a rendering-only assertion cannot see the fields the fold picks and
        # would pass while the JSON output still depended on argument order.
        assert len({report.model_dump_json() for report in reports}) == 1
        assert len({render_terminal(report) for report in reports}) == 1

    def test_a_reproducible_delta_is_not_called_flaky(self) -> None:
        base = _uniform("v1", [_result("a")])
        cand = _uniform("v2", [_result("a", EvalOutcome.FAIL)])
        report = diff_observations(base, [cand, cand, cand])
        assert _only(report, "a").flaky is False
        assert report.changed_count == 1

    def test_repeats_of_two_different_versions_are_refused(self) -> None:
        """Diffing two versions as repeats would blame a real change on flake."""
        base = _uniform("v1", [_result("a")])
        with pytest.raises(ValueError, match="one version"):
            diff_observations(base, [_uniform("v2", [_result("a")]), _uniform("v3", [_result("a")])])

    def test_a_diff_with_no_candidate_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least one candidate"):
            diff_observations(_uniform("v1", [_result("a")]), [])


class TestAttribution:
    """Naming a cause is optional; inventing one is not permitted."""

    def test_an_egress_denial_names_the_host_and_prints_the_remedy(self) -> None:
        found = attribute(
            [
                TierOutcome(tier=Tier.CLUSTER, outcome=EvalOutcome.FAIL,
                            error="egress denied to quickbooks.api.intuit.com by policy"),
            ]
        )
        assert found is not None
        assert "quickbooks.api.intuit.com" in found.cause
        assert found.remedy is not None
        assert "quickbooks.api.intuit.com" in found.remedy

    def test_an_unrecognized_failure_stays_unclassified(self) -> None:
        """The honesty valve. A confident wrong cause costs more than an admitted unknown."""
        assert (
            attribute(
                [
                    TierOutcome(tier=Tier.CLUSTER, outcome=EvalOutcome.FAIL,
                                error="unexpected end of stream from the sandbox supervisor"),
                ]
            )
            is None
        )

    def test_a_passing_tier_is_never_mined_for_a_cause(self) -> None:
        """Only a failure has a cause; matching on a passing tier's text would invent one."""
        assert (
            attribute(
                [
                    TierOutcome(tier=Tier.CLUSTER, outcome=EvalOutcome.PASS,
                                detail="egress denied to api.example.com"),
                ]
            )
            is None
        )

    def test_the_heaviest_failing_tier_is_consulted_first(self) -> None:
        """A cluster-only failure is the case this exists for, so its text wins."""
        found = attribute(
            [
                TierOutcome(tier=Tier.LOCAL, outcome=EvalOutcome.FAIL, error="OOMKilled"),
                TierOutcome(tier=Tier.CLUSTER, outcome=EvalOutcome.FAIL,
                            error="egress denied to api.example.com"),
            ]
        )
        assert found is not None
        assert "allowlist" in found.cause

    def test_an_unclassified_disagreement_is_counted_in_the_headline(self) -> None:
        """The report advertises how much it could not explain, rather than hiding it."""
        base = _uniform("v1", [_result("a")])
        cand = _observation(
            "v2",
            {
                Tier.SKILL: [_result("a")],
                Tier.LOCAL: [_result("a")],
                Tier.CLUSTER: [_result("a", EvalOutcome.FAIL, error="something nobody has seen")],
            },
        )
        report = diff_observations(base, [cand])
        assert report.unclassified_count == 1


class TestRendering:
    """Both surfaces are pure functions of the report, so neither can hide a row."""

    @pytest.fixture
    def report(self):
        base = _uniform("v0.7.0", [_result("quiet"), _result("noisy", trajectory=("x", "refund"))])
        cand = _observation(
            "v0.7.1",
            {
                Tier.SKILL: [_result("quiet"), _result("noisy", trajectory=("x",))],
                Tier.LOCAL: [_result("quiet"), _result("noisy", trajectory=("x",))],
                Tier.CLUSTER: [_result("quiet"), _result("noisy", trajectory=("x",))],
            },
        )
        return diff_observations(base, [cand])

    def test_the_terminal_report_lists_the_noisy_row_and_only_counts_the_quiet_one(self, report) -> None:
        text = render_terminal(report)
        assert "noisy" in text
        assert "quiet" not in text
        assert "1 case(s) unchanged" in text

    def test_the_terminal_report_stays_inside_the_layout_width(self, report) -> None:
        """A recorded terminal that reflows is unreadable.

        The guarantee is deliberately conditional: a line may exceed the width
        only when a single unbreakable token does. Splitting a hostname or a
        command to respect a margin buys a tidy report and an unusable one.
        """
        for line in render_terminal(report).split("\n"):
            if len(line) > 100:
                assert max(len(token) for token in line.split()) > 92, line

    def test_a_remedy_is_never_truncated_mid_command(self) -> None:
        """The remedy exists to be pasted; half of one looks complete and is not."""
        host = "a-very-long-hostname-that-would-overflow-any-narrow-column.example.com"
        base = _uniform("v0.7.0", [_result("a")])
        cand = _observation(
            "v0.7.1",
            {
                Tier.SKILL: [_result("a")],
                Tier.LOCAL: [_result("a")],
                Tier.CLUSTER: [_result("a", EvalOutcome.FAIL, error=f"egress denied to {host}")],
            },
        )
        text = render_terminal(diff_observations(base, [cand]))
        # Assert on the FIX line specifically. An earlier version of this test
        # asserted only that the host appeared somewhere in the report, which the
        # cause line satisfied by itself, so the test passed green while the remedy
        # command was in fact being split across two lines by the wrapper.
        fix_lines = [line for line in text.split("\n") if "curie cluster apply" in line]
        assert len(fix_lines) == 1, "the remedy must render as one selectable line"
        assert fix_lines[0].strip().endswith(host)

    def test_the_markdown_report_carries_the_same_rows_as_the_terminal_one(self, report) -> None:
        assert "noisy" in render_markdown(report)
        assert "quiet" not in render_markdown(report)

    def test_a_clean_diff_says_so_on_both_surfaces(self) -> None:
        rows = [_result("a", trajectory=("t",))]
        report = diff_observations(_uniform("v1", rows), [_uniform("v1", rows)])
        assert "No behavioral difference" in render_terminal(report)
        assert "No behavioral difference" in render_markdown(report)


class TestTheRunnerActuallyRecordsTheRoute:
    """The one claim the diff engine cannot prove about itself.

    Everything above tests the differ against results built in the test. These
    tests drive the real eval runner over real loopback HTTP against a scripted
    runner, so
    a regression that stopped recording the trajectory would turn them red rather
    than quietly reducing every route diff to "unchanged".
    """

    def test_the_observed_tool_calls_survive_onto_the_result(self, make_eval_harness) -> None:
        import asyncio

        from curie_worker.eval import EvalCase, EvalSuite, Grader
        from curie_worker.eval.models import GraderKind
        from curie_worker.eval.runner import EvalRunner

        async def go() -> None:
            async with make_eval_harness() as (base_url, fake, client):
                fake.responses = {"refund it": "done, refunded"}
                fake.tool_calls = {"refund it": ["lookup_order", "refund", "notify"]}
                suite = EvalSuite(
                    name="s",
                    cases=[
                        EvalCase(
                            id="c",
                            input="refund it",
                            grader=Grader(kind=GraderKind.CONTAINS, expected="refunded"),
                        )
                    ],
                )
                result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")
                # Order matters: a route is a sequence, and a set would lose the
                # difference between calling refund before and after notifying.
                assert result.results[0].trajectory == ("lookup_order", "refund", "notify")

        asyncio.run(go())

    def test_a_turn_that_called_nothing_records_an_empty_route(self, make_eval_harness) -> None:
        import asyncio

        from curie_worker.eval import EvalCase, EvalSuite, Grader
        from curie_worker.eval.models import GraderKind
        from curie_worker.eval.runner import EvalRunner

        async def go() -> None:
            async with make_eval_harness() as (base_url, fake, client):
                fake.responses = {"hello": "hi there"}
                suite = EvalSuite(
                    name="s",
                    cases=[
                        EvalCase(
                            id="c",
                            input="hello",
                            grader=Grader(kind=GraderKind.CONTAINS, expected="hi"),
                        )
                    ],
                )
                result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")
                assert result.results[0].trajectory == ()

        asyncio.run(go())

    def test_a_failing_turn_still_records_how_far_it_got(self, make_eval_harness) -> None:
        """A partial route is the evidence a tier-disagreement report needs.

        Knowing the cluster died after calling one tool, and which one, is most of
        the diagnosis. Discarding the route on failure would throw away exactly
        the case the report exists for.
        """
        import asyncio

        from curie_worker.eval import EvalCase, EvalSuite, Grader
        from curie_worker.eval.models import GraderKind
        from curie_worker.eval.runner import EvalRunner

        async def go() -> None:
            async with make_eval_harness() as (base_url, fake, client):
                fake.responses = {"report": ""}
                fake.tool_calls = {"report": ["query_ledger"]}
                fake.classified_failure_inputs = {"report"}
                suite = EvalSuite(
                    name="s",
                    cases=[
                        EvalCase(
                            id="c",
                            input="report",
                            grader=Grader(kind=GraderKind.CONTAINS, expected="ready"),
                        )
                    ],
                )
                result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")
                assert result.results[0].outcome is EvalOutcome.FAIL
                assert result.results[0].trajectory == ("query_ledger",)

        asyncio.run(go())
