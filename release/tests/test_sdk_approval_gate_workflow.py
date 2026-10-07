"""Contract tests for the SDK-bump live approval-gate PR workflow.

This workflow runs on a rare trigger -- a pull request that touches `uv.lock` --
so a maintainer will not notice in the ordinary course of review if a future edit
quietly narrows its `paths` filter, decouples its `if:` gate from `detect`, or
strips the `CURIE_E2E_LIVE` flag that makes the ladder step actually dispatch a
real model instead of running sealed. Any of those regressions makes the gate
silently stop proving anything while still reporting green on every PR that
bumps the Claude Agent SDK. This file pins the contract so a revert of any of
those properties fails CI here, not in production three months from now.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "sdk-approval-gate.yaml"


def load_workflow() -> dict:
    return yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def all_steps(workflow: dict) -> list[dict]:
    return [step for job in workflow["jobs"].values() for step in job["steps"]]


class TestSdkApprovalGateWorkflowContract:
    def test_trigger_is_pull_request_on_main_and_next_scoped_to_the_lock_file(self) -> None:
        workflow = load_workflow()
        trigger = workflow["on"]

        assert set(trigger) == {"pull_request"}
        assert trigger["pull_request"]["branches"] == ["main", "next"]
        assert set(trigger["pull_request"]["paths"]) == {
            "uv.lock",
            ".github/workflows/sdk-approval-gate.yaml",
            "tools/sdk-lock-gate/**",
        }
        assert len(trigger["pull_request"]["paths"]) == 3

    def test_detect_job_runs_detect_py_and_exposes_its_output(self) -> None:
        workflow = load_workflow()
        detect_job = workflow["jobs"]["detect"]

        detect_steps = [
            step
            for step in detect_job["steps"]
            if "tools/sdk-lock-gate/detect.py" in step.get("run", "")
        ]
        assert detect_steps
        detect_step = detect_steps[0]
        step_id = detect_step.get("id")
        assert step_id

        assert detect_job["outputs"]["changed"] == f"${{{{ steps.{step_id}.outputs.changed }}}}"

    def test_live_job_is_gated_on_both_needs_and_the_changed_output(self) -> None:
        workflow = load_workflow()
        jobs = workflow["jobs"]
        live_job = next(job for name, job in jobs.items() if name != "detect")

        needs = live_job["needs"]
        needs_list = [needs] if isinstance(needs, str) else list(needs)
        assert "detect" in needs_list

        condition = " ".join(live_job["if"].split())
        assert "needs.detect.outputs.changed == 'true'" in condition

    def test_ladder_step_runs_the_skill_tier_live(self) -> None:
        workflow = load_workflow()
        jobs = workflow["jobs"]
        live_job = next(job for name, job in jobs.items() if name != "detect")

        ladder_steps = [
            step
            for step in live_job["steps"]
            if step.get("run") == "bash cli/scripts/e2e-ladder.sh"
        ]
        assert len(ladder_steps) == 1
        env = ladder_steps[0]["env"]

        assert env["CURIE_E2E_TIERS"] == "skill"
        # This is the single most important assertion in this file: without
        # CURIE_E2E_LIVE: "1" the ladder runs sealed against a fake model and
        # this job is green forever while proving nothing about #1852/#2068.
        assert env["CURIE_E2E_LIVE"] == "1"
        assert "CURIE_BIN" in env
        assert "CURIE_BASE_TAG" in env
        assert "secrets.OPENROUTER_API_KEY" in env["CURIE_CREDENTIALS"]
        assert "CURIE_MODEL" in env

    def test_missing_credential_summarizes_and_then_fails_the_job(self) -> None:
        """A run with no model credential must go RED, not green-with-a-notice.

        The posture this pins is the whole point of the workflow: when
        `OPENROUTER_API_KEY` is absent -- a fork PR, or the Dependabot PR that
        is this gate's own motivating trigger -- the live approval-gate proof
        cannot run, and a check that reports success anyway is exactly the
        vacuous green that let #1852 ship.

        A saboteur reverting to the report-don't-block posture would make the
        model-credit step `exit 0`, drop the captured command status, or
        neutralize the failure with `continue-on-error: true` or a step `if:`.
        So this asserts the structure each of those edits breaks: the step is
        unconditional, writes the command class to the job summary, exits with
        the command status, and neither the step nor the job swallows that
        failure. Asserting on the summary prose alone would survive every one
        of those edits.
        """
        workflow = load_workflow()
        jobs = workflow["jobs"]
        live_job = next(job for name, job in jobs.items() if name != "detect")

        guard_steps = [
            step
            for step in live_job["steps"]
            if "curie dev model-credit" in step.get("run", "")
            and "GITHUB_STEP_SUMMARY" in step.get("run", "")
        ]
        assert len(guard_steps) == 1
        guard = guard_steps[0]
        run = guard["run"]

        assert "GITHUB_STEP_SUMMARY" in run
        assert "curie dev model-credit" in run
        assert "secrets.OPENROUTER_API_KEY" not in run
        assert "exit 0" not in run
        assert 'exit "$status"' in run or 'exit "${status}"' in run, run

        env = guard.get("env")
        assert isinstance(env, dict)
        assert "secrets.OPENROUTER_API_KEY" in env.get("CURIE_CREDENTIALS", "")

        # An exit that the runner is told to ignore is the same vacuous green
        # by another name, at either the step or the job level.
        assert guard.get("continue-on-error", "false") == "false"
        assert live_job.get("continue-on-error", "false") == "false"
        # And the guard itself must be unconditional: an `if:` on this step
        # would let the missing-credential case route around the failure.
        assert "if" not in guard

    def test_every_checkout_step_disables_persisted_credentials(self) -> None:
        workflow = load_workflow()
        checkouts = [
            step
            for step in all_steps(workflow)
            if step.get("uses", "").startswith("actions/checkout@")
        ]

        assert checkouts
        assert all(step.get("with", {}).get("persist-credentials") == "false" for step in checkouts)
