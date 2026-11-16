"""The factory retry live driver's inputs, values and judgements, without a cluster.

The live driver (test_factory_retry_live.py) runs only on a caller's throwaway
cluster. These checks pin what it refuses before touching anything, the values
it installs, the directive it writes, that the timing constants match the
product's, and that each judgement fails on the outcome it exists to catch and
passes on the shape a correct install leaves.
"""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import candidate_install as ci
import factory_retry_live as frl
import pytest

ROOT = Path(__file__).resolve().parents[3]
DRIVER = "tools/factory-e2e/tests/test_factory_retry_live.py"
ITEM = "00000000-0000-0000-0000-0000000000aa"
ORIGIN = "11111111-1111-1111-1111-111111111111"
SECOND = "22222222-2222-2222-2222-222222222222"
THIRD = "33333333-3333-3333-3333-333333333333"
RESET = 1_800_000_000


def _complete_env(tmp_path: Path) -> dict[str, str]:
    cli = tmp_path / "curie"
    cli.write_text("#!/bin/sh\nexit 0\n")
    cli.chmod(0o755)
    return {
        frl.FLAG: "1",
        "CURIE_FACTORY_KUBE_CONTEXT": "throwaway",
        frl.NAMES.namespace: "retry-1",
        frl.NAMES.release: "curie-retry",
        frl.NAMES.pull_policy: "IfNotPresent",
        "CURIE_C4_CANDIDATE_CLI": str(cli),
        "CURIE_C4_CANDIDATE_IMAGES": "registry.test/curie-{component}:cand",
    }


# --------------------------------------------------------------------------
# Inputs, opt-in and the CI selection
# --------------------------------------------------------------------------


def test_inputs_read_back_from_a_complete_environment(tmp_path: Path) -> None:
    inputs = ci.read_inputs(_complete_env(tmp_path), frl.NAMES)
    assert (inputs.namespace, inputs.release) == ("retry-1", "curie-retry")


@pytest.mark.parametrize("name", [frl.NAMES.namespace, frl.NAMES.release, "CURIE_C4_CANDIDATE_CLI"])
def test_a_missing_input_is_a_refusal_naming_it(tmp_path: Path, name: str) -> None:
    env = _complete_env(tmp_path)
    del env[name]
    with pytest.raises(ci.Refused, match=name):
        ci.read_inputs(env, frl.NAMES)


def test_the_driver_skips_without_its_flag() -> None:
    env = {key: value for key, value in os.environ.items() if key != frl.FLAG}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", DRIVER, "-rs"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    # 5: a module-level skip collects no test.
    assert result.returncode in (0, 5), result.stdout[-2000:] + result.stderr[-2000:]
    assert f"set {frl.FLAG}=1" in result.stdout


def test_the_default_pr_selection_ignores_the_live_driver() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yaml").read_text()
    assert f"--ignore {DRIVER}" in workflow
    assert f'"$path" != "{DRIVER}"' in workflow


# --------------------------------------------------------------------------
# Constants match the product and the fake tier
# --------------------------------------------------------------------------


def test_timing_constants_match_the_product() -> None:
    from curie_api.factory_retry_policy import RESET_GRACE
    from curie_api.workitems.lifecycle import (
        AUTOMATIC_RETRY_DELAYS_S,
        AUTOMATIC_RETRY_JITTER,
        AUTOMATIC_RETRY_LIMIT,
    )

    assert frl.AUTOMATIC_RETRY_LIMIT == AUTOMATIC_RETRY_LIMIT
    assert frl.BACKOFF_S == tuple(float(s) for s in AUTOMATIC_RETRY_DELAYS_S)
    assert frl.JITTER == AUTOMATIC_RETRY_JITTER
    assert frl.RESET_GRACE_S == RESET_GRACE.total_seconds()


@pytest.mark.parametrize(
    "directive",
    [
        frl.rate_limit(RESET),
        frl.daily_budget(RESET),
        frl.RATE_LIMIT_UNTIMED,
        frl.GENERIC_403,
        frl.UNREACHABLE,
    ],
)
def test_every_fault_directive_parses_in_the_fake_tier(directive: str) -> None:
    from curie_runner import fake

    match = fake._MODEL_FAULT_RE.search(directive)
    assert match is not None and match.group(0) == directive


def test_the_hold_directive_parses_in_the_fake_tier() -> None:
    from curie_runner import fake

    assert fake._resource_mib(fake._FILL_SCRATCH_RE, frl.HOLD) == 8


# --------------------------------------------------------------------------
# Install values and the template directive
# --------------------------------------------------------------------------


def _base() -> dict[str, Any]:
    return {"agentSandbox": {"runner": {}}, "factory": {"codeHosts": []}, "api": {}}


def test_install_values_give_every_leg_agent_a_terminal_standing_directive() -> None:
    base = _base()
    pristine = copy.deepcopy(base)
    values = frl.install_values(base)
    agents = {leg.agent for leg in frl.LEGS}
    assert base == pristine
    assert set(values["agentSandbox"]["runnerEnv"]) == agents
    assert {env[frl.DIRECTIVE_ENV] for env in values["agentSandbox"]["runnerEnv"].values()} == {
        frl.GENERIC_403
    }
    assert set(values["factory"]["sandboxEgress"]) == agents
    assert len({leg.label for leg in frl.LEGS}) == len(frl.LEGS)
    assert all(len(frl.template_name("curie-retry-1234567890", a)) <= 63 for a in agents)


def _template(directive: str) -> dict[str, Any]:
    return {
        "spec": {
            "podTemplate": {
                "spec": {
                    "containers": [
                        {"name": "bundle-fetch", "env": [{"name": "X", "value": "y"}]},
                        {
                            "name": "runner",
                            "env": [
                                {"name": "CURIE_FAKE_MODEL", "value": "1"},
                                {"name": frl.DIRECTIVE_ENV, "value": directive},
                            ],
                        },
                    ]
                }
            }
        }
    }


def test_the_directive_patch_replaces_only_the_runner_directive_it_read() -> None:
    patch = frl.directive_patch(_template(frl.GENERIC_403), frl.UNREACHABLE)
    path = "/spec/podTemplate/spec/containers/1/env/1/value"
    assert patch == [
        {"op": "test", "path": path, "value": frl.GENERIC_403},
        {"op": "replace", "path": path, "value": frl.UNREACHABLE},
    ]
    assert frl.standing_directive(_template(frl.HOLD)) == frl.HOLD


def test_the_directive_patch_refuses_a_template_without_the_directive() -> None:
    template = _template(frl.GENERIC_403)
    template["spec"]["podTemplate"]["spec"]["containers"][1]["env"].pop()
    with pytest.raises(ValueError, match=frl.DIRECTIVE_ENV):
        frl.directive_patch(template, frl.UNREACHABLE)
    template["spec"]["podTemplate"]["spec"]["containers"].pop()
    with pytest.raises(ValueError, match="runner"):
        frl.directive_patch(template, frl.UNREACHABLE)


# --------------------------------------------------------------------------
# Row judgements, against recorded row shapes
# --------------------------------------------------------------------------


def _at(seconds: float) -> str:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(RESET + seconds, tz=UTC).isoformat()


def _row(request: str, sequence: int, **overrides: Any) -> dict[str, Any]:
    base = sequence * 1000
    row = {
        "id": request,
        "sequence": sequence,
        "status": "failed",
        "terminal_cause": frl.CAUSE_UNREACHABLE,
        "retry_safety": "no_effects",
        "retry_condition_reason": None,
        "retry_condition_retry_at": None,
        "retry_condition_scope_key": None,
        "retry_of_request_id": None,
        "retry_origin_request_id": None,
        "owner_lost_retry": False,
        "blocked_on": None,
        "created_at": _at(base),
        "wait_deadline": _at(base + 86400),
        "dispatch_not_before": _at(base),
        "dispatch_generation": 1,
        "started_at": _at(base + 40),
        "execution_deadline": _at(base + 40 + 1800),
        "terminal_at": _at(base + 60),
        "execution_attempts": 1,
        "runtime_claim_name": f"claim-{sequence}",
    }
    row.update(overrides)
    return row


def _chain() -> list[dict[str, Any]]:
    origin = _row(ORIGIN, 1)
    second = _row(
        SECOND,
        2,
        created_at=origin["terminal_at"],
        wait_deadline=_at(1060 + 86400),
        dispatch_not_before=_at(1060 + 31),
        started_at=_at(1060 + 40),
        execution_deadline=_at(1060 + 40 + 1800),
        terminal_at=_at(1060 + 70),
        retry_of_request_id=ORIGIN,
        retry_origin_request_id=ORIGIN,
    )
    third = _row(
        THIRD,
        3,
        created_at=second["terminal_at"],
        wait_deadline=_at(1130 + 86400),
        dispatch_not_before=_at(1130 + 110),
        started_at=_at(1130 + 115),
        execution_deadline=_at(1130 + 115 + 1800),
        terminal_at=_at(1130 + 130),
        retry_of_request_id=SECOND,
        retry_origin_request_id=ORIGIN,
    )
    return [origin, second, third]


def test_a_correct_chain_passes_every_judgement() -> None:
    origin, second, third = _chain()
    assert frl.successor_problems(origin, second, origin=origin) == []
    assert frl.successor_problems(second, third, origin=origin) == []
    assert frl.scheduled_problems(origin, second, due=None, work_slot=1) == []
    assert frl.scheduled_problems(second, third, due=None, work_slot=2) == []
    assert frl.started_once_problems(second) == []
    assert frl.fresh_attempt_problems([origin, second, third]) == []


@pytest.mark.parametrize(
    ("change", "fragment"),
    [
        ({"id": ORIGIN}, "reuses"),
        ({"sequence": 1}, "sequence"),
        ({"retry_of_request_id": THIRD}, "retry_of_request_id"),
        ({"retry_origin_request_id": SECOND}, "retry_origin_request_id"),
        ({"owner_lost_retry": True}, "owner_lost_retry"),
        ({"wait_deadline": _at(1000 + 86400)}, "waiting deadline"),
        ({"created_at": _at(1000)}, "admitted"),
    ],
)
def test_successor_lineage_fails_on_each_defect(change: dict[str, Any], fragment: str) -> None:
    origin, second, _ = _chain()
    second.update(change)
    assert any(
        fragment in problem for problem in frl.successor_problems(origin, second, origin=origin)
    )


@pytest.mark.parametrize(
    ("not_before", "slot", "ok"),
    [
        (1060 + 24, 1, True),
        (1060 + 36, 1, True),
        (1060 + 5, 1, False),
        (1060 + 60, 1, False),
        (1060 + 96, 2, True),
        (1060 + 30, 2, False),
    ],
)
def test_the_backoff_window_follows_the_slot(not_before: float, slot: int, ok: bool) -> None:
    origin, second, _ = _chain()
    second["dispatch_not_before"] = _at(not_before)
    assert (frl.scheduled_problems(origin, second, due=None, work_slot=slot) == []) is ok


def test_a_timed_successor_must_be_due_at_reset_plus_grace_and_unblocked() -> None:
    origin, second, _ = _chain()
    due = RESET + 2000 + frl.RESET_GRACE_S
    second["dispatch_not_before"] = _at(2000 + frl.RESET_GRACE_S)
    assert frl.scheduled_problems(origin, second, due=due, work_slot=1) == []
    second["dispatch_not_before"] = _at(2000)
    assert frl.scheduled_problems(origin, second, due=due, work_slot=1)
    second["dispatch_not_before"] = _at(2000 + frl.RESET_GRACE_S)
    second["blocked_on"] = "provider_rate_limit"
    assert any("blocked" in p for p in frl.scheduled_problems(origin, second, due=due, work_slot=1))


@pytest.mark.parametrize(
    ("change", "fragment"),
    [
        ({"started_at": None, "execution_attempts": 0}, "never started"),
        ({"started_at": _at(1060 + 10)}, "before its due time"),
        ({"execution_deadline": None}, "execution deadline"),
        ({"runtime_claim_name": None}, "claim"),
    ],
)
def test_a_start_fails_when_early_absent_or_without_its_own_deadline(
    change: dict[str, Any], fragment: str
) -> None:
    _, second, _ = _chain()
    second.update(change)
    assert any(fragment in problem for problem in frl.started_once_problems(second))


def test_fresh_attempts_fail_on_a_shared_claim_or_deadline() -> None:
    rows = _chain()
    rows[2]["runtime_claim_name"] = rows[1]["runtime_claim_name"]
    assert any("claim" in p for p in frl.fresh_attempt_problems(rows))
    rows = _chain()
    rows[2]["wait_deadline"] = rows[1]["wait_deadline"]
    assert any("waiting" in p for p in frl.fresh_attempt_problems(rows))


def test_a_waiting_successor_that_started_or_claimed_is_caught() -> None:
    row = _row(
        SECOND, 2, status="waiting", started_at=None, execution_attempts=0, runtime_claim_name=None
    )
    assert frl.unstarted_problems(row) == []
    assert frl.unstarted_problems({**row, "runtime_claim_name": "c"})
    assert frl.unstarted_problems({**row, "started_at": _at(5), "execution_attempts": 1})


def test_the_carrier_judgement_checks_cause_safety_reason_reset_and_scope() -> None:
    row = _row(
        ORIGIN,
        1,
        terminal_cause=frl.CAUSE_RATE_LIMITED,
        retry_condition_reason="rate_limited",
        retry_condition_retry_at=_at(0),
    )
    good = {"cause": frl.CAUSE_RATE_LIMITED, "reason": "rate_limited", "retry_at": float(RESET)}
    assert frl.carrier_problems(row, **good) == []  # type: ignore[arg-type]
    for change in (
        {"terminal_cause": frl.CAUSE_UNREACHABLE},
        {"retry_safety": "flagged"},
        {"retry_safety": None},
        {"retry_condition_reason": None},
        {"retry_condition_retry_at": None},
        {"retry_condition_retry_at": _at(30)},
        {"retry_condition_scope_key": "scope"},
        {"status": "completed"},
    ):
        assert frl.carrier_problems({**row, **change}, **good), change  # type: ignore[arg-type]
    untimed = {**row, "retry_condition_retry_at": None}
    assert (
        frl.carrier_problems(
            untimed, cause=frl.CAUSE_RATE_LIMITED, reason="rate_limited", retry_at=None
        )
        == []
    )
    assert frl.carrier_problems(
        row, cause=frl.CAUSE_RATE_LIMITED, reason="rate_limited", retry_at=None
    )


# --------------------------------------------------------------------------
# Notices
# --------------------------------------------------------------------------

_RETRYING = (
    "<!-- curie-factory-status -->\n**Status**\n"
    "Retrying: this run failed with no side effects, so Curie admitted automatic retry 1 of 2.\n"
    "Scheduled: the automatic retry starts at or after 2026-11-16 12:00:30 UTC.\n"
    "Cause: model_unreachable\n"
)
_NEEDS_HUMAN = (
    "<!-- curie-factory-status -->\n**Status**\n"
    "Could not complete: the model provider could not be reached.\n"
    "2 of 2 automatic retries used.\nCause: model_unreachable\n"
)


def test_notices_count_retrying_and_needs_human_status_comments() -> None:
    found = frl.notices(
        [{"body": _RETRYING}, {"body": _RETRYING}, {"body": _NEEDS_HUMAN}, {"body": "a person"}]
    )
    assert (found.retrying, found.needs_human, found.other) == (2, 1, 1)
    assert frl.notice_problems(found, retrying=2, needs_human=1, used=2) == []
    assert frl.notice_problems(found, retrying=2, needs_human=1, used=1)
    assert frl.notice_problems(found, retrying=1, needs_human=1)
    twice = frl.notices([{"body": _NEEDS_HUMAN}, {"body": _NEEDS_HUMAN}])
    assert frl.notice_problems(twice, retrying=0, needs_human=1)


# --------------------------------------------------------------------------
# The compiled CLI's answers
# --------------------------------------------------------------------------

_REFUSED = json.dumps(
    {
        "error": (
            "work item retry refused (allowance_exhausted): All 2 of 2 automatic retries of "
            "this work item are used; the newest request failed with model_unreachable. "
            "This command grants no more."
        ),
        "fix": "no retry authority remains; readmit through the tracker if policy allows",
    }
)


def test_a_refusal_needs_exit_one_and_the_named_code() -> None:
    result = frl.CliResult(1, _REFUSED + "\n", "")
    assert frl.refusal_problems(result, "allowance_exhausted") == []
    assert frl.refusal_problems(result, "not_retryable")
    assert frl.refusal_problems(frl.CliResult(0, _REFUSED, ""), "allowance_exhausted")
    assert frl.refusal_problems(frl.CliResult(1, "", "boom"), "allowance_exhausted")


def _plan(**overrides: Any) -> dict[str, Any]:
    plan = {
        "work_item_id": ITEM,
        "request_id": "44444444-4444-4444-4444-444444444444",
        "dry_run": True,
        "replayed": False,
        "disposition": "implementation",
        "predecessor_request_id": ORIGIN,
        "predecessor_cause": frl.CAUSE_RATE_LIMITED,
        "successor_request_id": None,
        "successor_dispatch_not_before": _at(frl.RESET_GRACE_S),
        "successor_blocked_on": None,
        "work_item_version": 3,
        "retries_used": 1,
        "retry_limit": 2,
        "actor_kind": "platform_key",
        "recorded_at": None,
    }
    plan.update(overrides)
    return plan


@pytest.mark.parametrize(
    ("change", "ok"),
    [
        ({}, True),
        ({"dry_run": False}, False),
        ({"successor_request_id": THIRD}, False),
        ({"successor_blocked_on": "provider_rate_limit"}, False),
        ({"successor_dispatch_not_before": _at(0)}, False),
        ({"retries_used": 2}, False),
        ({"disposition": "publication_recovery"}, False),
    ],
)
def test_the_dry_run_plan_must_schedule_without_writing(change: dict[str, Any], ok: bool) -> None:
    result = frl.CliResult(0, json.dumps(_plan(**change)), "")
    problems = frl.dry_run_problems(
        result,
        work_item_id=ITEM,
        predecessor_id=ORIGIN,
        cause=frl.CAUSE_RATE_LIMITED,
        due=RESET + frl.RESET_GRACE_S,
    )
    assert (problems == []) is ok, problems


def test_a_refusal_or_dry_run_that_wrote_is_caught() -> None:
    before = {
        "version": 3,
        "automatic_retries_used": 0,
        "cancelled_at": None,
        "readmit_request_id": None,
    }
    assert frl.unchanged_problems(before, dict(before), 1, 1) == []
    assert frl.unchanged_problems(before, {**before, "version": 4}, 1, 1)
    assert frl.unchanged_problems(before, {**before, "automatic_retries_used": 1}, 1, 1)
    assert frl.unchanged_problems(before, dict(before), 1, 2)
