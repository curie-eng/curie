"""Contract tests for the SDK-bump live approval-gate PR workflow.

This workflow runs when the SDK lock or a named approval enforcement path changes
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
    def test_trigger_is_pull_request_on_main_and_next_scoped_to_sdk_and_approval_paths(
        self,
    ) -> None:
        workflow = load_workflow()
        trigger = workflow["on"]

        assert set(trigger) == {"pull_request"}
        assert trigger["pull_request"]["branches"] == ["main", "next"]
        assert set(trigger["pull_request"]["paths"]) == {
            "uv.lock",
            ".github/workflows/sdk-approval-gate.yaml",
            "tools/sdk-lock-gate/detect.py",
            "tools/sdk-lock-gate/run-proof.py",
            "tools/sdk-lock-gate/tests/test_approval_paths.py",
            "tools/sdk-lock-gate/tests/test_live_proof_consumer.py",
            "tools/sdk-lock-gate/tests/test_sdk_lock_gate.py",
            "runner/src/curie_runner/__main__.py",
            "runner/src/curie_runner/adapter.py",
            "runner/src/curie_runner/approval.py",
            "runner/src/curie_runner/config.py",
            "runner/src/curie_runner/connectors.py",
            "runner/src/curie_runner/hooks.py",
            "runner/src/curie_runner/session.py",
            "runner/tests/fixtures/mcp_tool_capability_server.py",
            "runner/tests/test_approval.py",
            "runner/tests/test_approval_gate_enforcement.py",
            "runner/tests/test_connectors.py",
            "runner/tests/test_gate_e2e.py",
            "runner/tests/test_gate_shadowing.py",
            "runner/tests/test_hooks.py",
            "runner/tests/test_hosted_mcp_approval_catalog.py",
            "runner/tests/test_ladder_approval_gate_case.py",
            "runner/tests/test_live.py",
            "runner/tests/test_mcp_tool_capability.py",
        }
        assert len(trigger["pull_request"]["paths"]) == 25

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
            # #2308's consumer executes the ladder and refuses missing case,
            # park/no-effect proof, or a later ladder failure.
            if step.get("run") == "python3 tools/sdk-lock-gate/run-proof.py"
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

        A saboteur reverting to the report-don't-block posture would flip the
        guard's `exit 1` to `exit 0`, delete the exit so the branch falls
        through, or leave the exit in place and neutralize it with
        `continue-on-error: true`. So this asserts the structure each of those
        edits breaks: inside the empty-credential branch itself there is a
        nonzero `exit` and no zero `exit`, and neither the step nor the job
        swallows that failure. Asserting on the summary prose alone would
        survive every one of those three edits.
        """
        workflow = load_workflow()
        jobs = workflow["jobs"]
        live_job = next(job for name, job in jobs.items() if name != "detect")

        guard_steps = [
            step
            for step in live_job["steps"]
            if "GITHUB_STEP_SUMMARY" in step.get("run", "")
            and "OPENROUTER_API_KEY" in step.get("run", "")
        ]
        assert len(guard_steps) == 1
        guard = guard_steps[0]

        lines = guard["run"].splitlines()
        branch_starts = [
            index
            for index, line in enumerate(lines)
            if line.strip().startswith("if ") and "OPENROUTER_API_KEY" in line and "-z" in line
        ]
        assert len(branch_starts) == 1, "expected one empty-credential branch in the guard"
        start = branch_starts[0]
        ends = [index for index, line in enumerate(lines) if index > start and line.strip() == "fi"]
        assert ends, "the empty-credential branch is unterminated"
        branch = [line.strip() for line in lines[start + 1 : ends[0]]]

        assert any(line.startswith("echo") or line == "{" for line in branch), branch
        assert any("GITHUB_STEP_SUMMARY" in line for line in branch), branch

        exits = [line for line in branch if line.split("#")[0].strip().startswith("exit")]
        assert exits, "the empty-credential branch must terminate the job explicitly"
        codes = [int(line.split()[1]) for line in exits]
        assert all(code != 0 for code in codes), (
            f"a missing credential must fail the job, not exit 0 (found {codes})"
        )

        # An `exit 1` that the runner is told to ignore is the same vacuous
        # green by another name, at either the step or the job level.
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
