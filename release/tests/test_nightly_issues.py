"""Tests for nightly-ladder issue filing and signature dedup (#2245)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
NIGHTLY = REPO_ROOT / "release" / "nightly.py"
NIGHTLY_YAML = REPO_ROOT / ".github" / "workflows" / "nightly-graded-ladder.yaml"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


nightly = load_module(NIGHTLY, "release_nightly")


DISPATCHER_LOG = (
    "\n########## rung: local-release (compose, generated release artifact) "
    "##########\n"
    "error: image 'ghcr.io/curie-eng/curie-dispatcher:latest' is required by "
    "compose.release.yaml's full profile and is not present locally.\n"
    "fix: build and tag the missing image(s) locally\n"
)

CLUSTER_LOG = (
    "\n=== curie cluster message after repeated eval ===\n"
    "cluster: message after repeated eval timed out at 45s without a "
    "finalized reply; eval-owned sandboxes likely still hold the quota "
    "(#1534).\n"
)

SKILL_UP_LOG = "\n=== curie skill up (fake model, offline) ===\n"

OTEL_LOG = (
    "\n=== case: local runner failure is observable and recovers ===\n"
    "AssertionError: no new worker-to-runner trace carried ERROR on "
    "turn.process + agent.run and classified_failure\n"
)


class TestExtractSignatures:
    def test_dispatcher_image_signature_is_stable(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        assert len(signatures) == 1
        assert "curie-dispatcher:latest" in signatures[0].text
        assert signatures[0].job == "local-release"

    def test_cluster_timeout_signature_is_stable(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "cluster", "conclusion": "failure", "log": CLUSTER_LOG}]
        )
        assert len(signatures) == 1
        assert "timed out at 45s" in signatures[0].text

    def test_skill_up_banner_without_diagnostic_is_a_signature(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "skill+local connector", "conclusion": "failure", "log": SKILL_UP_LOG}]
        )
        assert len(signatures) == 1
        assert "skill up" in signatures[0].text.lower()

    def test_skill_up_banner_with_a_later_error_uses_the_error(self) -> None:
        log = SKILL_UP_LOG + "error: connector image missing\n"
        signatures = nightly.extract_signatures(
            [{"name": "skill+local connector", "conclusion": "failure", "log": log}]
        )
        assert len(signatures) == 1
        assert "connector image missing" in signatures[0].text
        assert "skill up" not in signatures[0].text.lower()

    def test_otel_assertion_signature_is_stable(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "skill+local default", "conclusion": "failure", "log": OTEL_LOG}]
        )
        assert len(signatures) == 1
        assert "classified_failure" in signatures[0].text

    def test_successful_jobs_emit_no_signatures(self) -> None:
        assert nightly.extract_signatures(
            [{"name": "build-cli", "conclusion": "success", "log": "ok"}]
        ) == []

    def test_same_failure_twice_dedups_to_one_signature_id(self) -> None:
        first = nightly.extract_signatures(
            [{"name": "a", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )[0]
        second = nightly.extract_signatures(
            [{"name": "b", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )[0]
        assert first.signature_id == second.signature_id


class TestPlanIssueActions:
    def test_unknown_signature_is_created(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        actions = nightly.plan_issue_actions(signatures, existing_issues=[])
        assert len(actions) == 1
        assert actions[0].kind == "create"
        assert nightly.NIGHTLY_LABEL in actions[0].labels
        assert nightly.signature_marker(signatures[0].signature_id) in actions[0].body

    def test_known_signature_is_commented_not_recreated(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        marker = nightly.signature_marker(signatures[0].signature_id)
        existing = [
            {
                "number": 99,
                "title": "nightly-ladder: dispatcher image",
                "body": f"prior run\n{marker}\n",
            }
        ]
        actions = nightly.plan_issue_actions(signatures, existing_issues=existing)
        assert len(actions) == 1
        assert actions[0].kind == "comment"
        assert actions[0].number == 99

    def test_unrelated_open_issue_does_not_absorb_a_new_signature(self) -> None:
        signatures = nightly.extract_signatures(
            [{"name": "cluster", "conclusion": "failure", "log": CLUSTER_LOG}]
        )
        existing = [
            {
                "number": 1,
                "title": "other",
                "body": nightly.signature_marker("deadbeefdeadbeef"),
            }
        ]
        actions = nightly.plan_issue_actions(signatures, existing_issues=existing)
        assert actions[0].kind == "create"


class TestNightlyWorkflowFilesIssues:
    def test_file_failures_job_is_gated_on_a_failed_ladder_job(self) -> None:
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        job = workflow["jobs"]["file-failures"]
        needs = job["needs"]
        needs_list = [needs] if isinstance(needs, str) else list(needs)
        for name in (
            "ladder-skill-local",
            "ladder-local-release",
            "ladder-cluster",
        ):
            assert name in needs_list
        condition = " ".join(job["if"].split())
        assert "always()" in condition
        assert "failure" in condition
        assert "success" not in condition or "needs." in condition

    def test_file_failures_job_has_issues_write_and_does_not_raise_workflow_contents(self) -> None:
        source = NIGHTLY_YAML.read_text()
        workflow = yaml.load(source, Loader=yaml.BaseLoader)
        assert workflow["permissions"]["contents"] == "read"
        job = workflow["jobs"]["file-failures"]
        assert job["permissions"]["issues"] == "write"
        assert (job["permissions"].get("contents") or "read") == "read"

    def test_file_failures_invokes_the_filer_with_the_run_id(self) -> None:
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        runs = [
            step.get("run", "")
            for step in workflow["jobs"]["file-failures"]["steps"]
        ]
        joined = "\n".join(runs)
        assert "release/nightly.py" in joined
        assert "GITHUB_RUN_ID" in joined
        assert "file-issues" in joined or "--run-id" in joined


ANSI_DISPATCHER_LOG = (
    "\x1b[0m\n\x1b[36m########## rung: local-release (compose, generated "
    "release artifact) ##########\x1b[0m\n"
    "\x1b[0;31merror: image 'ghcr.io/curie-eng/curie-dispatcher:latest' is "
    "required by compose.release.yaml's full profile and is not present "
    "locally.\x1b[0m\n"
    "\x1b]0;a title sequence\x07"
    "fix: build and tag the missing image(s) locally\n"
)


class FakeCompleted:
    def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


class TestEscapeSequenceLogs:
    """#2819: gh refuses logs carrying terminal escape sequences."""

    def test_escaped_log_yields_the_same_signature_as_a_clean_log(self) -> None:
        escaped = nightly.extract_signatures(
            [
                {
                    "name": "local-release",
                    "conclusion": "failure",
                    "log": nightly.strip_ansi(ANSI_DISPATCHER_LOG),
                }
            ]
        )
        clean = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        assert len(escaped) == 1
        assert escaped[0].signature_id == clean[0].signature_id

    def test_raw_escaped_log_without_stripping_does_not_match(self) -> None:
        """Negative control: the stripping is what makes the signature stable."""
        raw = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": ANSI_DISPATCHER_LOG}]
        )
        clean = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )
        assert raw and raw[0].signature_id != clean[0].signature_id

    def test_colon_separated_sgr_is_stripped(self) -> None:
        """Truecolor SGR uses colon parameter bytes, not just digits."""
        log = (
            "\x1b[38:2::255:0:0merror: connector image missing\x1b[0m\n"
        )
        signatures = nightly.extract_signatures(
            [{"name": "skill", "conclusion": "failure", "log": nightly.strip_ansi(log)}]
        )
        assert "\x1b" not in nightly.strip_ansi(log)
        assert signatures[0].text == "error: connector image missing"

    def test_job_log_fetch_asks_gh_to_allow_escape_sequences(self, monkeypatch) -> None:
        seen: list[list[str]] = []

        def fake_run(args, **kwargs):
            seen.append(list(args))
            return FakeCompleted(stdout=ANSI_DISPATCHER_LOG)

        monkeypatch.setattr(nightly.subprocess, "run", fake_run)
        log = nightly.job_log("curie-eng/curie", 1234)
        assert "--allow-escape-sequences" in seen[0]
        assert "\x1b" not in log
        assert "curie-dispatcher:latest" in log

    def test_job_log_fetch_retries_without_the_flag_when_gh_rejects_it(
        self, monkeypatch
    ) -> None:
        seen: list[list[str]] = []

        def fake_run(args, **kwargs):
            seen.append(list(args))
            if "--allow-escape-sequences" in args:
                raise nightly.subprocess.CalledProcessError(
                    1, args, output="", stderr="unknown flag: --allow-escape-sequences"
                )
            return FakeCompleted(stdout=ANSI_DISPATCHER_LOG)

        monkeypatch.setattr(nightly.subprocess, "run", fake_run)
        log = nightly.job_log("curie-eng/curie", 1234)
        assert len(seen) == 2
        assert "--allow-escape-sequences" not in seen[1]
        assert "curie-dispatcher:latest" in log

    def test_a_real_gh_refusal_is_not_swallowed(self, monkeypatch) -> None:
        def fake_run(args, **kwargs):
            raise nightly.subprocess.CalledProcessError(
                1,
                args,
                output="",
                stderr=(
                    "the response contains terminal escape sequences; pass "
                    "--allow-escape-sequences to output it anyway"
                ),
            )

        monkeypatch.setattr(nightly.subprocess, "run", fake_run)
        try:
            nightly.job_log("curie-eng/curie", 1234)
        except nightly.subprocess.CalledProcessError:
            return
        raise AssertionError("a non-flag gh failure must propagate")


class TestFilingFailsLoudly:
    """#2819: the filing job must go red when it cannot file."""

    def test_no_signatures_from_a_failed_run_is_an_error_exit(
        self, monkeypatch, capsys
    ) -> None:
        monkeypatch.setattr(nightly, "_ensure_label", lambda repo: None)
        monkeypatch.setattr(nightly, "_failed_job_logs", lambda repo, run_id: [])
        rc = nightly.file_issues("curie-eng/curie", "1", "http://run")
        out = capsys.readouterr().out
        assert rc == 1
        assert "::error" in out

    def test_a_gh_failure_while_filing_is_an_error_exit(self, monkeypatch, capsys) -> None:
        monkeypatch.setattr(nightly, "_ensure_label", lambda repo: None)

        def boom(repo, run_id):
            raise nightly.subprocess.CalledProcessError(
                1, ["gh"], output="", stderr="gh: server error"
            )

        monkeypatch.setattr(nightly, "_failed_job_logs", boom)
        rc = nightly.file_issues("curie-eng/curie", "1", "http://run")
        out = capsys.readouterr().out
        assert rc == 1
        assert "::error" in out
        assert "gh: server error" in out

    def test_a_successful_filing_still_exits_zero(self, monkeypatch) -> None:
        monkeypatch.setattr(nightly, "_ensure_label", lambda repo: None)
        monkeypatch.setattr(
            nightly,
            "_failed_job_logs",
            lambda repo, run_id: [
                {"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}
            ],
        )
        monkeypatch.setattr(nightly, "_open_nightly_issues", lambda repo: [])
        monkeypatch.setattr(nightly, "_gh", lambda args: "")
        assert nightly.file_issues("curie-eng/curie", "1", "http://run") == 0

    def test_workflow_surfaces_a_filing_failure_in_the_run_summary(self) -> None:
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        job = workflow["jobs"]["file-failures"]
        steps = job["steps"]
        assert any(
            "failure()" in (step.get("if") or "")
            and "GITHUB_STEP_SUMMARY" in (step.get("run") or "")
            for step in steps
        ), "no failure-surfacing step in file-failures"
        assert all(
            (step.get("continue-on-error") or "false") == "false" for step in steps
        )


class TestFilingPathWithEscapedLogs:
    """#2819 AC2: an escaped log drives file_issues end to end."""

    def test_file_issues_files_a_clean_signature_from_an_escaped_log(
        self, monkeypatch
    ) -> None:
        calls: list[list[str]] = []

        def fake_run(args, **kwargs):
            calls.append(list(args))
            joined = " ".join(args)
            if "actions/runs/" in joined and joined.endswith("/jobs"):
                return FakeCompleted(
                    stdout=json.dumps(
                        {"jobs": [{"id": 77, "name": "local-release", "conclusion": "failure"}]}
                    )
                )
            if "/logs" in joined:
                if "--allow-escape-sequences" not in args:
                    raise nightly.subprocess.CalledProcessError(
                        1,
                        args,
                        output="",
                        stderr=(
                            "the response contains terminal escape sequences; "
                            "pass --allow-escape-sequences to output it anyway"
                        ),
                    )
                return FakeCompleted(stdout=ANSI_DISPATCHER_LOG)
            if "label" in args and "list" in args:
                return FakeCompleted(stdout=json.dumps([{"name": nightly.NIGHTLY_LABEL}]))
            if "issue" in args and "list" in args:
                return FakeCompleted(stdout="[]")
            return FakeCompleted(stdout="")

        monkeypatch.setattr(nightly.subprocess, "run", fake_run)
        assert nightly.file_issues("curie-eng/curie", "42", "http://run") == 0

        created = [c for c in calls if "issue" in c and "create" in c]
        assert len(created) == 1, calls
        body = created[0][created[0].index("--body") + 1]
        title = created[0][created[0].index("--title") + 1]
        assert "\x1b" not in body and "\x1b" not in title
        assert "curie-dispatcher:latest" in title + body
        expected = nightly.extract_signatures(
            [{"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG}]
        )[0]
        assert nightly.signature_marker(expected.signature_id) in body


class TestEveryRedRungIsFiled:
    """#2868: a red rung with no issue created or updated fails the run."""

    def _jobs_payload(self, *jobs: tuple[int, str]) -> str:
        return json.dumps(
            {
                "jobs": [
                    {"id": job_id, "name": name, "conclusion": "failure"}
                    for job_id, name in jobs
                ]
            }
        )

    def test_a_failed_log_fetch_still_files_with_the_log_omitted(
        self, monkeypatch, capsys
    ) -> None:
        calls: list[list[str]] = []

        def fake_run(args, **kwargs):
            calls.append(list(args))
            joined = " ".join(args)
            if joined.endswith("/jobs"):
                return FakeCompleted(stdout=self._jobs_payload((77, "cluster")))
            if "/logs" in joined:
                raise nightly.subprocess.CalledProcessError(
                    1, args, output="", stderr="HTTP 410: log expired"
                )
            if "label" in args and "list" in args:
                return FakeCompleted(stdout=json.dumps([{"name": nightly.NIGHTLY_LABEL}]))
            if "issue" in args and "list" in args:
                return FakeCompleted(stdout="[]")
            return FakeCompleted(stdout="")

        monkeypatch.setattr(nightly.subprocess, "run", fake_run)
        assert nightly.file_issues("curie-eng/curie", "42", "http://run") == 0

        created = [c for c in calls if "issue" in c and "create" in c]
        assert len(created) == 1, calls
        body = created[0][created[0].index("--body") + 1]
        title = created[0][created[0].index("--title") + 1]
        assert "cluster" in title
        assert "Log omitted" in body
        assert "HTTP 410: log expired" in body

    def test_a_red_rung_whose_issue_create_fails_fails_the_run_and_names_it(
        self, monkeypatch, capsys
    ) -> None:
        monkeypatch.setattr(nightly, "_ensure_label", lambda repo: None)
        monkeypatch.setattr(
            nightly,
            "_failed_job_logs",
            lambda repo, run_id: [
                {"name": "local-release", "conclusion": "failure", "log": DISPATCHER_LOG},
                {"name": "cluster", "conclusion": "failure", "log": CLUSTER_LOG},
            ],
        )
        monkeypatch.setattr(nightly, "_open_nightly_issues", lambda repo: [])

        def fake_gh(args):
            if "create" in args and any("timed out" in a for a in args):
                raise nightly.subprocess.CalledProcessError(
                    1, ["gh"], output="", stderr="gh: validation failed"
                )
            return ""

        monkeypatch.setattr(nightly, "_gh", fake_gh)
        rc = nightly.file_issues("curie-eng/curie", "1", "http://run")
        out = capsys.readouterr().out
        assert rc == 1
        assert "::error" in out
        assert "cluster" in out
        assert "local-release" not in out.split("::", 2)[-1].split("gh errors")[0]
        assert "created issue" in out

    def test_duplicate_signatures_across_rungs_count_as_filed(self, monkeypatch) -> None:
        monkeypatch.setattr(nightly, "_ensure_label", lambda repo: None)
        monkeypatch.setattr(
            nightly,
            "_failed_job_logs",
            lambda repo, run_id: [
                {"name": "a", "conclusion": "failure", "log": DISPATCHER_LOG},
                {"name": "b", "conclusion": "failure", "log": DISPATCHER_LOG},
            ],
        )
        monkeypatch.setattr(nightly, "_open_nightly_issues", lambda repo: [])
        monkeypatch.setattr(nightly, "_gh", lambda args: "")
        assert nightly.file_issues("curie-eng/curie", "1", "http://run") == 0

    def test_the_filing_job_can_fail_the_workflow_run(self) -> None:
        workflow = yaml.load(NIGHTLY_YAML.read_text(), Loader=yaml.BaseLoader)
        job = workflow["jobs"]["file-failures"]
        assert (job.get("continue-on-error") or "false") == "false"
