"""Contract tests for the release authorization gate (release/authorize.py).

The gate is the deterministic check behind issue #628: a `v*` tag must not be
able to start the publish pipeline unless its commit is reachable from
`origin/main` or `origin/next` and that commit's required checks are all green. These tests
drive both functions directly -- `commit_is_on_reviewed_branch` against a real,
disposable git repo (no network needed for ancestry) and
`missing_required_checks` against constructed
check-run lists -- plus `authorize()`, which combines them and is what
`authorize-release` actually calls.
"""

import importlib.util
import inspect
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "release" / "authorize.py"


def load_module():
    """Import the standalone script by path (release/ is not on sys.path)."""
    spec = importlib.util.spec_from_file_location("release_authorize", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["release_authorize"] = module
    spec.loader.exec_module(module)
    return module


authorize_module = load_module()


def run_git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def commit(repo: Path, name: str) -> str:
    (repo / name).write_text(name)
    run_git(repo, "add", name)
    run_git(repo, "commit", "-m", f"add {name}")
    return run_git(repo, "rev-parse", "HEAD")


@pytest.fixture
def git_repo(tmp_path) -> Path:
    """A repo with a `main` branch, plus commits diverging on an unmerged branch."""
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(repo, "init", "-q", "-b", "main")
    run_git(repo, "config", "user.email", "test@example.com")
    run_git(repo, "config", "user.name", "Test")
    commit(repo, "on-main.txt")
    return repo


@pytest.fixture
def reviewed_refs_repo(git_repo) -> tuple[Path, dict[str, str]]:
    """A history with commits unique to main, next, and an unmerged feature."""
    base = run_git(git_repo, "rev-parse", "HEAD")

    run_git(git_repo, "checkout", "-q", "-b", "next")
    next_commit = commit(git_repo, "next-only.txt")
    run_git(git_repo, "update-ref", "refs/remotes/origin/next", next_commit)

    run_git(git_repo, "checkout", "-q", "main")
    main_commit = commit(git_repo, "main-only.txt")
    run_git(git_repo, "update-ref", "refs/remotes/origin/main", main_commit)

    run_git(git_repo, "checkout", "-q", "-b", "feature", base)
    feature_commit = commit(git_repo, "feature-only.txt")

    return git_repo, {"main": main_commit, "next": next_commit, "feature": feature_commit}


# A required-name set distinct from the real, larger production
# REQUIRED_CHECK_NAMES (issue #733). Most tests in this file exercise the
# *logic* of required-check matching and should not need updating every time
# a ci.yaml job is renamed or added; the production constant itself gets its
# own coverage in TestRequiredCheckAllowlist below.
TEST_REQUIRED_NAMES = frozenset({"CI", "CodeQL"})
REVIEWED_REFS = ("origin/main", "origin/next")

CHECK_RUNS_ALL_GREEN = [
    {"name": "CI", "conclusion": "success"},
    {"name": "CodeQL", "conclusion": "neutral"},
    {"name": "Secret Scan", "conclusion": "skipped"},
]
CHECK_RUNS_ONE_FAILED = [
    {"name": "CI", "conclusion": "success"},
    {"name": "CodeQL", "conclusion": "failure"},
]

CURRENT_RUN_ID = "29811627398"
OTHER_RUN_ID = "29811600001"


def check_run(name: str, conclusion: str | None, run_id: str, job_id: str) -> dict:
    """A check-run shaped like the live API response (issue #732).

    `details_url` observed live on this repo:
    https://github.com/curie-eng/curie/actions/runs/29811627398/job/88573652086
    """
    return {
        "name": name,
        "status": "completed" if conclusion is not None else "in_progress",
        "conclusion": conclusion,
        "details_url": (
            f"https://github.com/curie-eng/curie/actions/runs/{run_id}/job/{job_id}"
        ),
    }


GATE_OWN_IN_PROGRESS = check_run(
    "authorize-release", None, CURRENT_RUN_ID, "88573652086"
)


class TestCommitIsOnReviewedBranch:
    def test_reviewed_branch_ref_has_no_default(self):
        parameters = inspect.signature(
            authorize_module.commit_is_on_reviewed_branch
        ).parameters

        assert parameters["reviewed_ref"].default is inspect.Parameter.empty

    def test_head_of_main_is_reachable(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")

        assert authorize_module.commit_is_on_reviewed_branch(sha, "main", cwd=git_repo)

    def test_ancestor_of_main_is_reachable(self, git_repo):
        first = run_git(git_repo, "rev-parse", "HEAD")
        commit(git_repo, "later-on-main.txt")

        assert authorize_module.commit_is_on_reviewed_branch(first, "main", cwd=git_repo)

    def test_commit_only_on_an_unmerged_branch_is_refused(self, git_repo):
        run_git(git_repo, "checkout", "-q", "-b", "feature")
        unmerged = commit(git_repo, "feature-only.txt")

        assert not authorize_module.commit_is_on_reviewed_branch(
            unmerged, "main", cwd=git_repo
        )

    def test_unknown_sha_refuses_as_indeterminate_reachability(self, git_repo):
        sha = "0" * 40

        with pytest.raises(authorize_module.AuthorizationError) as exc_info:
            authorize_module.commit_is_on_reviewed_branch(sha, "main", cwd=git_repo)

        assert sha in str(exc_info.value)
        assert "main" in str(exc_info.value)


class TestRequiredCheckSatisfaction:
    def test_all_required_names_present_and_green_passes(self):
        assert (
            authorize_module.missing_required_checks(
                CHECK_RUNS_ALL_GREEN, TEST_REQUIRED_NAMES
            )
            == set()
        )

    def test_a_required_name_that_concluded_failure_fails(self):
        assert authorize_module.missing_required_checks(
            CHECK_RUNS_ONE_FAILED, TEST_REQUIRED_NAMES
        ) == {"CodeQL"}

    def test_no_check_runs_fails(self):
        # Absence of checks is not evidence they passed.
        assert (
            authorize_module.missing_required_checks([], TEST_REQUIRED_NAMES)
            == TEST_REQUIRED_NAMES
        )

    def test_a_missing_required_name_fails_even_if_everything_present_is_green(self):
        # issue #733's core scenario: a non-empty, fully-passing list that
        # simply never contains the name that matters.
        only_unrelated = [{"name": "Secret Scan", "conclusion": "success"}]

        assert (
            authorize_module.missing_required_checks(
                only_unrelated, TEST_REQUIRED_NAMES
            )
            == TEST_REQUIRED_NAMES
        )

    def test_an_unrelated_failing_check_does_not_affect_the_required_set(self):
        # Only required names are asserted; a failing check outside
        # `required_names` has no bearing (this is not "everything present
        # must pass" -- that was the old, weaker behavior issue #733 replaces).
        runs = [
            {"name": "CI", "conclusion": "success"},
            {"name": "CodeQL", "conclusion": "neutral"},
            {"name": "Some Unrelated Job", "conclusion": "failure"},
        ]

        assert (
            authorize_module.missing_required_checks(runs, TEST_REQUIRED_NAMES)
            == set()
        )


class TestMissingRequiredChecks:
    def test_empty_check_runs_reports_every_required_name_missing(self):
        assert (
            authorize_module.missing_required_checks([], TEST_REQUIRED_NAMES)
            == TEST_REQUIRED_NAMES
        )

    def test_all_present_and_green_reports_nothing_missing(self):
        assert (
            authorize_module.missing_required_checks(
                CHECK_RUNS_ALL_GREEN, TEST_REQUIRED_NAMES
            )
            == set()
        )

    def test_a_required_name_present_but_not_concluded_is_reported_missing(self):
        runs = [
            {"name": "CI", "conclusion": "success"},
            {"name": "CodeQL", "conclusion": None},  # still in_progress
        ]

        assert authorize_module.missing_required_checks(
            runs, TEST_REQUIRED_NAMES
        ) == {"CodeQL"}


class TestAuthorize:
    def test_reviewed_ref_collection_has_no_default(self):
        parameters = inspect.signature(authorize_module.authorize).parameters

        assert "main_ref" not in parameters
        assert parameters["reviewed_refs"].default is inspect.Parameter.empty

    def test_commit_reachable_only_from_next_is_authorized(
        self, reviewed_refs_repo
    ):
        git_repo, commits = reviewed_refs_repo
        authorize_module.authorize(
            commits["next"],
            CHECK_RUNS_ALL_GREEN,
            REVIEWED_REFS,
            cwd=git_repo,
            required_names=TEST_REQUIRED_NAMES,
        )

    def test_commit_reachable_only_from_main_is_authorized(
        self, reviewed_refs_repo
    ):
        git_repo, commits = reviewed_refs_repo

        authorize_module.authorize(
            commits["main"],
            CHECK_RUNS_ALL_GREEN,
            REVIEWED_REFS,
            cwd=git_repo,
            required_names=TEST_REQUIRED_NAMES,
        )

    def test_commit_reachable_from_neither_reviewed_ref_is_refused(
        self, reviewed_refs_repo
    ):
        git_repo, commits = reviewed_refs_repo

        with pytest.raises(authorize_module.AuthorizationError) as exc_info:
            authorize_module.authorize(
                commits["feature"],
                CHECK_RUNS_ALL_GREEN,
                REVIEWED_REFS,
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
            )

        assert "origin/main" in str(exc_info.value)
        assert "origin/next" in str(exc_info.value)

    def test_matching_ref_does_not_hide_an_unresolvable_ref(self, reviewed_refs_repo):
        git_repo, commits = reviewed_refs_repo
        unresolvable_ref = "origin/not-a-ref"

        with pytest.raises(authorize_module.AuthorizationError) as exc_info:
            authorize_module.authorize(
                commits["main"],
                CHECK_RUNS_ALL_GREEN,
                ("origin/main", unresolvable_ref),
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
            )

        assert commits["main"] in str(exc_info.value)
        assert unresolvable_ref in str(exc_info.value)

    def test_empty_reviewed_ref_collection_is_refused(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")

        with pytest.raises(authorize_module.AuthorizationError, match="reviewed ref"):
            authorize_module.authorize(
                sha,
                CHECK_RUNS_ALL_GREEN,
                (),
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
            )

    def test_reviewed_commit_with_a_failed_required_check_is_refused(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            authorize_module.authorize(
                sha,
                CHECK_RUNS_ONE_FAILED,
                ("main",),
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
            )


class TestRequiredCheckAllowlist:
    """Issue #733: a non-empty, fully-green check-run list is not enough on
    its own -- the checks that matter must actually be among them. These use
    the real production `REQUIRED_CHECK_NAMES` (no override), covering the
    exact failure scenario from the issue: main's real CI never started for a
    commit, but one unrelated check-run (e.g. a security scanner) passed on
    that SHA.
    """

    UNRELATED_BUT_GREEN = [
        {"name": "gitleaks (full history)", "conclusion": "success"},
        {"name": "Analyze (python)", "conclusion": "success"},
    ]

    def test_unrelated_green_checks_alone_leave_required_names_missing(self):
        assert authorize_module.missing_required_checks(self.UNRELATED_BUT_GREEN)

    def test_missing_required_checks_lists_every_ci_yaml_job(self):
        missing = authorize_module.missing_required_checks(self.UNRELATED_BUT_GREEN)

        assert missing == authorize_module.REQUIRED_CHECK_NAMES

    def test_authorize_refuses_a_commit_whose_only_checks_are_unrelated_but_green(
        self, git_repo
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")

        with pytest.raises(
            authorize_module.AuthorizationError, match="required check-run"
        ):
            authorize_module.authorize(
                sha, self.UNRELATED_BUT_GREEN, ("main",), cwd=git_repo
            )

    def test_every_required_check_present_and_green_authorizes(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            {"name": name, "conclusion": "success"}
            for name in authorize_module.REQUIRED_CHECK_NAMES
        ]

        authorize_module.authorize(sha, runs, ("main",), cwd=git_repo)

    def test_a_single_missing_required_check_among_an_otherwise_full_set_is_refused(
        self, git_repo
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        names = sorted(authorize_module.REQUIRED_CHECK_NAMES)
        dropped, remaining = names[0], names[1:]
        runs = [{"name": name, "conclusion": "success"} for name in remaining]

        with pytest.raises(
            authorize_module.AuthorizationError, match=re.escape(dropped)
        ):
            authorize_module.authorize(sha, runs, ("main",), cwd=git_repo)

    def test_authorize_refuses_a_failed_chart_check_when_every_other_check_passes(
        self, git_repo
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        chart_check = "Chart (lint + template + kubeconform)"
        runs = [
            {
                "name": name,
                "conclusion": "failure" if name == chart_check else "success",
            }
            for name in authorize_module.REQUIRED_CHECK_NAMES
        ]

        with pytest.raises(
            authorize_module.AuthorizationError, match=re.escape(chart_check)
        ):
            authorize_module.authorize(sha, runs, ("main",), cwd=git_repo)


class TestFetchCheckRuns:
    """`gh api` must be pinned to GET (issue #732, defect 1).

    `gh api` defaults to GET but switches to POST as soon as any `-f`/`-F`
    flag is present, and `POST /repos/{owner}/{repo}/commits/{sha}/check-runs`
    does not exist. Verified live against curie-eng/curie on 2026-07-21 at
    commit 276774ff: the `-f`-only form returned
    `{"message": "Not Found", "status": "404"}`, while adding `-X GET`
    returned `{"total_count": 42, ...}`. Mocking `gh` here is correct: GitHub
    is an external service.
    """

    @staticmethod
    def _capture(monkeypatch, payload: dict) -> list:
        captured: list = []

        def fake_run(argv, **kwargs):
            captured.append(argv)
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload), stderr="")

        monkeypatch.setattr(authorize_module.subprocess, "run", fake_run)
        return captured

    def test_check_runs_are_fetched_with_an_explicit_get(self, monkeypatch):
        payload = {"total_count": 1, "check_runs": [CHECK_RUNS_ALL_GREEN[0]]}
        captured = self._capture(monkeypatch, payload)

        runs = authorize_module.fetch_check_runs("deadbeef", "curie-eng/curie")

        argv = captured[0]
        endpoint = "repos/curie-eng/curie/commits/deadbeef/check-runs"
        assert "-X" in argv
        assert argv[argv.index("-X") + 1] == "GET"
        assert argv.index("-X") < argv.index(endpoint)
        assert "-f" in argv
        assert argv[argv.index("-f") + 1] == "per_page=100"
        assert runs == [CHECK_RUNS_ALL_GREEN[0]]


class TestFetchCheckRunsPagination:
    """The check-runs endpoint's default page size is 30, and a real commit
    on this repo has been measured with several dozen check-runs across its
    workflows (issue #733) -- comfortably past that default and past what a
    single `per_page=100` page happened to cover historically. These tests
    drive `fetch_check_runs`'s own pagination loop (not a stubbed
    single-response mock) to prove it walks every page, and that a required
    check which only fails or is only missing on a later page still causes a
    refusal rather than being silently dropped.
    """

    @staticmethod
    def _paged_fake_run(pages: dict, total_count: int):
        def fake_run(argv, **kwargs):
            page_arg = next(
                arg for arg in argv if isinstance(arg, str) and arg.startswith("page=")
            )
            page = int(page_arg.split("=", 1)[1])
            payload = {"total_count": total_count, "check_runs": pages.get(page, [])}
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload), stderr="")

        return fake_run

    def test_collects_every_page_in_order(self, monkeypatch):
        pages = {
            1: [
                {"name": "CI", "conclusion": "success"},
                {"name": "Unrelated", "conclusion": "success"},
            ],
            2: [{"name": "CodeQL", "conclusion": "neutral"}],
        }
        monkeypatch.setattr(
            authorize_module.subprocess, "run", self._paged_fake_run(pages, total_count=3)
        )

        runs = authorize_module.fetch_check_runs("deadbeef", "curie-eng/curie", per_page=2)

        assert [run["name"] for run in runs] == ["CI", "Unrelated", "CodeQL"]

    def test_a_required_check_failing_only_on_a_later_page_still_refuses(self, monkeypatch):
        pages = {
            1: [
                {"name": "CI", "conclusion": "success"},
                {"name": "Unrelated", "conclusion": "success"},
            ],
            2: [{"name": "CodeQL", "conclusion": "failure"}],
        }
        monkeypatch.setattr(
            authorize_module.subprocess, "run", self._paged_fake_run(pages, total_count=3)
        )

        runs = authorize_module.fetch_check_runs("deadbeef", "curie-eng/curie", per_page=2)

        assert len(runs) == 3
        assert authorize_module.missing_required_checks(runs, TEST_REQUIRED_NAMES) == {
            "CodeQL"
        }

    def test_a_required_check_only_present_on_a_later_page_still_authorizes(
        self, git_repo, monkeypatch
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        pages = {
            1: [{"name": "CI", "conclusion": "success"}],
            2: [{"name": "CodeQL", "conclusion": "success"}],
        }
        # Scope the `gh api` stub to the fetch call only -- `authorize()` below
        # also shells out to real `git merge-base` via the same
        # `subprocess.run`, which must not be intercepted by this fake.
        with monkeypatch.context() as page_fetch:
            page_fetch.setattr(
                authorize_module.subprocess, "run", self._paged_fake_run(pages, total_count=2)
            )
            runs = authorize_module.fetch_check_runs(
                "deadbeef", "curie-eng/curie", per_page=1
            )

        authorize_module.authorize(
            sha, runs, ("main",), cwd=git_repo, required_names=TEST_REQUIRED_NAMES
        )

    def test_stops_when_a_page_reports_nothing_even_if_total_count_implied_more(
        self, monkeypatch
    ):
        # A stale/wrong total_count must not spin the loop forever.
        pages = {1: [{"name": "CI", "conclusion": "success"}], 2: []}
        monkeypatch.setattr(
            authorize_module.subprocess, "run", self._paged_fake_run(pages, total_count=5)
        )

        runs = authorize_module.fetch_check_runs("deadbeef", "curie-eng/curie", per_page=1)

        assert runs == [{"name": "CI", "conclusion": "success"}]


class TestExcludeCurrentWorkflowRun:
    """The gate is itself a check-run on the tagged SHA (issue #732, defect 2)."""

    def test_current_run_entries_are_dropped_and_others_survive(self):
        other = check_run("CI", "success", OTHER_RUN_ID, "88573600001")

        remaining = authorize_module.exclude_current_workflow_run(
            [GATE_OWN_IN_PROGRESS, other], CURRENT_RUN_ID
        )

        assert remaining == [other]

    def test_falsy_run_id_leaves_the_list_untouched(self):
        runs = [GATE_OWN_IN_PROGRESS, check_run("CI", "success", OTHER_RUN_ID, "1")]

        assert authorize_module.exclude_current_workflow_run(runs, None) == runs
        assert authorize_module.exclude_current_workflow_run(runs, "") == runs

    def test_entries_without_details_url_are_never_dropped(self):
        external = {"name": "External Check", "status": "completed", "conclusion": "success"}

        remaining = authorize_module.exclude_current_workflow_run(
            [GATE_OWN_IN_PROGRESS, external], CURRENT_RUN_ID
        )

        assert remaining == [external]


class TestAuthorizeExcludesCurrentRun:
    def test_gate_own_in_progress_entry_does_not_block_authorization(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", "success", OTHER_RUN_ID, "88573600001"),
            check_run("CodeQL", "neutral", OTHER_RUN_ID, "88573600002"),
        ]

        authorize_module.authorize(
            sha,
            runs,
            ("main",),
            cwd=git_repo,
            exclude_run_id=CURRENT_RUN_ID,
            required_names=TEST_REQUIRED_NAMES,
        )

    def test_unrelated_check_present_does_not_block_when_required_checks_are_green(
        self, git_repo
    ):
        # New semantics (issue #733): only the required names are asserted --
        # an unrelated check-run, however incomplete, has no bearing.
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", "success", OTHER_RUN_ID, "88573600001"),
            check_run("CodeQL", "neutral", OTHER_RUN_ID, "88573600002"),
            check_run("Integration Tests", None, OTHER_RUN_ID, "88573600003"),
        ]

        authorize_module.authorize(
            sha,
            runs,
            ("main",),
            cwd=git_repo,
            exclude_run_id=CURRENT_RUN_ID,
            required_names=TEST_REQUIRED_NAMES,
        )

    def test_required_check_still_in_progress_is_refused(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", "success", OTHER_RUN_ID, "88573600001"),
            check_run("CodeQL", None, OTHER_RUN_ID, "88573600002"),
        ]

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            authorize_module.authorize(
                sha,
                runs,
                ("main",),
                cwd=git_repo,
                exclude_run_id=CURRENT_RUN_ID,
                required_names=TEST_REQUIRED_NAMES,
            )

    def test_required_check_that_concluded_failure_is_refused(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", "success", OTHER_RUN_ID, "88573600001"),
            check_run("CodeQL", "failure", OTHER_RUN_ID, "88573600004"),
        ]

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            authorize_module.authorize(
                sha,
                runs,
                ("main",),
                cwd=git_repo,
                exclude_run_id=CURRENT_RUN_ID,
                required_names=TEST_REQUIRED_NAMES,
            )

    def test_only_current_run_checks_is_refused(self, git_repo):
        # Nothing survives filtering, and absence of checks is not evidence
        # they passed.
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("authorize-release setup", "success", CURRENT_RUN_ID, "88573652087"),
        ]

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            authorize_module.authorize(
                sha,
                runs,
                ("main",),
                cwd=git_repo,
                exclude_run_id=CURRENT_RUN_ID,
                required_names=TEST_REQUIRED_NAMES,
            )


class TestMain:
    """`main()` must thread `GITHUB_RUN_ID` into `authorize()` as
    `exclude_run_id` (issue #732, defect 2).

    `main()` never passes `required_names` through explicitly, so it always
    resolves the module-level `REQUIRED_CHECK_NAMES` at call time; these tests
    monkeypatch that constant to the small `TEST_REQUIRED_NAMES` set so the
    fixtures stay independent of the production ci.yaml job list.

    Note on the required-check allowlist (issue #733): the gate's own
    check-run is always named after its job (e.g. `authorize-release`), never
    after a ci.yaml job, so it can never itself satisfy or block a required
    name -- unlike the old "every present check-run must pass" rule, an
    unfiltered self-entry sitting in the list with `conclusion: null` no
    longer affects the outcome at all. What still matters, and what these
    tests cover, is that a *wrong* run id can incorrectly filter out a
    legitimate required check-run (mistaking another run's job for this one),
    which must still refuse.

    `fetch_check_runs` is stubbed so no network call happens; `authorize()`
    runs for real against `git_repo`, so `main()` is run with that repo as
    the working directory (`main()` calls `authorize()` without a `cwd`).
    """

    @staticmethod
    def _stub_fetch_check_runs(monkeypatch, runs: list[dict]) -> None:
        monkeypatch.setattr(
            authorize_module, "fetch_check_runs", lambda sha, repo: runs
        )

    @staticmethod
    def _use_test_required_names(monkeypatch) -> None:
        monkeypatch.setattr(authorize_module, "REQUIRED_CHECK_NAMES", TEST_REQUIRED_NAMES)

    @staticmethod
    def _stub_green_nightly(monkeypatch) -> None:
        monkeypatch.setattr(
            authorize_module._nightly,
            "fetch_latest_nightly_conclusion",
            lambda repo, branch: "success",
        )
        monkeypatch.setattr(
            authorize_module._nightly,
            "fetch_associated_pr_bodies",
            lambda sha, repo: [],
        )

    @staticmethod
    def _runs_with_gate_own_in_progress() -> list[dict]:
        return [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", "success", OTHER_RUN_ID, "88573600001"),
            check_run("CodeQL", "neutral", OTHER_RUN_ID, "88573600002"),
        ]

    def test_main_authorizes_when_github_run_id_excludes_its_own_check(
        self, git_repo, monkeypatch
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
        self._stub_green_nightly(monkeypatch)
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)

        exit_code = authorize_module.main(
            [sha, "--repo", "curie-eng/curie", "--reviewed-ref", "main"]
        )

        assert exit_code == 0

    def test_main_still_authorizes_when_github_run_id_is_absent_and_required_checks_are_green(
        self, git_repo, monkeypatch
    ):
        # The gate's own check-run is never itself a required name, so
        # leaving it unfiltered (no run id to exclude by) has no bearing on
        # whether the real required checks (CI, CodeQL here) are satisfied.
        sha = run_git(git_repo, "rev-parse", "HEAD")
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
        self._stub_green_nightly(monkeypatch)
        monkeypatch.chdir(git_repo)
        monkeypatch.delenv("GITHUB_RUN_ID", raising=False)

        exit_code = authorize_module.main(
            [sha, "--repo", "curie-eng/curie", "--reviewed-ref", "main"]
        )

        assert exit_code == 0

    def test_main_refuses_a_red_nightly_without_a_pr_body_override(
        self, git_repo, monkeypatch, capsys
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
        monkeypatch.setattr(
            authorize_module._nightly,
            "fetch_latest_nightly_conclusion",
            lambda repo, branch: "failure",
        )
        monkeypatch.setattr(
            authorize_module._nightly,
            "fetch_associated_pr_bodies",
            lambda sha, repo: ["cut the release"],
        )
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)

        exit_code = authorize_module.main(
            [sha, "--repo", "curie-eng/curie", "--reviewed-ref", "main"]
        )

        assert exit_code == 1
        assert "nightly" in capsys.readouterr().err.lower()

    def test_main_authorizes_a_red_nightly_when_the_pr_body_records_the_override(
        self, git_repo, monkeypatch
    ):
        sha = run_git(git_repo, "rev-parse", "HEAD")
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
        monkeypatch.setattr(
            authorize_module._nightly,
            "fetch_latest_nightly_conclusion",
            lambda repo, branch: "failure",
        )
        monkeypatch.setattr(
            authorize_module._nightly,
            "fetch_associated_pr_bodies",
            lambda sha, repo: ["--allow-red-nightly"],
        )
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)

        exit_code = authorize_module.main(
            [sha, "--repo", "curie-eng/curie", "--reviewed-ref", "main"]
        )

        assert exit_code == 0

    def test_main_refuses_when_github_run_id_is_a_different_run(
        self, git_repo, monkeypatch
    ):
        # The fixture's real "CI"/"CodeQL" entries are marked as belonging to
        # OTHER_RUN_ID; passing that value as GITHUB_RUN_ID makes
        # `exclude_current_workflow_run` mistake them for this run's own and
        # strip them out, leaving only the gate's unrelated in-progress entry.
        # A wrong run id over-excluding legitimate required checks must still
        # refuse, not slip through.
        sha = run_git(git_repo, "rev-parse", "HEAD")
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", OTHER_RUN_ID)

        exit_code = authorize_module.main(
            [sha, "--repo", "curie-eng/curie", "--reviewed-ref", "main"]
        )

        assert exit_code == 1


class TestMainLookupFailures:
    """A failed check-run lookup must refuse legibly, not traceback (#732).

    Before this, `main()` caught only `AuthorizationError`, so every failure
    inside `fetch_check_runs` escaped as an unhandled traceback. The exit code
    was already 1, so the gate did fail closed; what was missing was any way
    for an operator to tell an unauthorized tag from a lookup that never
    completed -- which is exactly why the `gh api` POST/GET defect read as an
    opaque crash. Each path below must still return 1.
    """

    @staticmethod
    def _stub_fetch_raising(monkeypatch, exc: BaseException) -> None:
        def raising(sha, repo):
            raise exc

        monkeypatch.setattr(authorize_module, "fetch_check_runs", raising)

    @staticmethod
    def _run_main(git_repo, monkeypatch) -> int:
        sha = run_git(git_repo, "rev-parse", "HEAD")
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)
        return authorize_module.main(
            [sha, "--repo", "curie-eng/curie", "--reviewed-ref", "main"]
        )

    def test_gh_api_failure_refuses_with_a_message_naming_the_lookup(
        self, git_repo, monkeypatch, capsys
    ):
        self._stub_fetch_raising(
            monkeypatch,
            subprocess.CalledProcessError(
                1,
                ["gh", "api", "-X", "GET", "repos/curie-eng/curie/commits/x/check-runs"],
                stderr="gh: Not Found (HTTP 404)",
            ),
        )

        exit_code = self._run_main(git_repo, monkeypatch)

        assert exit_code == 1
        stderr = capsys.readouterr().err
        assert "ERROR: could not retrieve check-runs" in stderr
        assert "gh: Not Found (HTTP 404)" in stderr

    def test_unparseable_response_refuses_with_a_message_naming_the_lookup(
        self, git_repo, monkeypatch, capsys
    ):
        self._stub_fetch_raising(
            monkeypatch, json.JSONDecodeError("Expecting value", "not json", 0)
        )

        exit_code = self._run_main(git_repo, monkeypatch)

        assert exit_code == 1
        assert "ERROR: could not retrieve check-runs" in capsys.readouterr().err

    def test_payload_without_check_runs_key_refuses_with_a_message(
        self, git_repo, monkeypatch, capsys
    ):
        self._stub_fetch_raising(monkeypatch, KeyError("check_runs"))

        exit_code = self._run_main(git_repo, monkeypatch)

        assert exit_code == 1
        assert "ERROR: could not retrieve check-runs" in capsys.readouterr().err

    def test_lookup_failure_is_distinguishable_from_an_unauthorized_tag(
        self, git_repo, monkeypatch, capsys
    ):
        # The unauthorized-tag wording must not appear on the lookup path, or
        # an operator cannot tell the two refusals apart.
        monkeypatch.setattr(authorize_module, "REQUIRED_CHECK_NAMES", TEST_REQUIRED_NAMES)
        self._stub_fetch_raising(monkeypatch, KeyError("check_runs"))

        assert self._run_main(git_repo, monkeypatch) == 1
        lookup_stderr = capsys.readouterr().err

        monkeypatch.setattr(
            authorize_module, "fetch_check_runs", lambda sha, repo: CHECK_RUNS_ONE_FAILED
        )
        assert self._run_main(git_repo, monkeypatch) == 1
        refusal_stderr = capsys.readouterr().err

        assert "could not retrieve check-runs" in lookup_stderr
        assert "could not retrieve check-runs" not in refusal_stderr
        assert "required check-run" in refusal_stderr


CI_YAML = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
HELM_CI_YAML = REPO_ROOT / ".github" / "workflows" / "helm-ci.yaml"
RELEASE_YAML = REPO_ROOT / ".github" / "workflows" / "release.yaml"

_MATRIX_REF = re.compile(r"\$\{\{\s*matrix\.([A-Za-z0-9_]+)\s*\}\}")


def workflow_job_check_run_names(path: Path) -> set[str]:
    """The concrete check-run names a workflow's jobs produce (issue #811).

    Parses the real workflow rather than any list derived from
    `REQUIRED_CHECK_NAMES` -- deriving the expected set from the constant
    would recreate the very drift-blindness issue #811 is about. Each job's
    check-run name is its `name:` field; a matrixed job whose name interpolates
    `${{ matrix.<key> }}` and that declares `strategy.matrix.include` expands
    to one concrete name per include row, substituting that row's `<key>`
    value. The substitution is general over the matrix key (regex, not a
    literal `matrix.name`), so a future job that matrixes on a different key
    is handled without editing this helper. A job with no `name:` is skipped.
    """
    doc = yaml.safe_load(path.read_text())
    names: set[str] = set()
    for job in doc["jobs"].values():
        name = job.get("name")
        if not name:
            continue
        ref = _MATRIX_REF.search(name)
        include = ((job.get("strategy") or {}).get("matrix") or {}).get("include")
        if ref and include:
            for entry in include:
                names.add(_MATRIX_REF.sub(lambda m, entry=entry: str(entry[m.group(1)]), name))
        else:
            names.add(name)
    return names


def ci_job_check_run_names() -> set[str]:
    return workflow_job_check_run_names(CI_YAML)


def helm_ci_job_check_run_names() -> set[str]:
    return workflow_job_check_run_names(HELM_CI_YAML)


def ci_connector_image_check_run_names() -> set[str]:
    """The `(no push)` check-run names of ci.yaml's *connector* image rows (#1951).

    Derived from `ci.yaml`, never from `REQUIRED_CHECK_NAMES`, for the same
    reason `workflow_job_check_run_names` is: a set built from the constant can
    only ever agree with itself, and the drift this guards is precisely a row
    that exists in the workflow and nowhere in the constant.

    A connector row is identified by its `dockerfile` path resolving under
    `examples/`, not by its `context`. `dockerfile` is the discriminator
    because it is not optional the way `context` is: the `images` job's build
    step (`file: ${{ matrix.dockerfile }}`) has nothing to build without it, so
    every row -- including `mail-adapter`, which has no `context` -- carries
    one. A helper keyed on `context` would silently skip a connector row that
    used an equivalent context-less form, exactly the drift this guard exists
    to catch; keying on `dockerfile` closes that hole because there is no
    context-less-but-valid way to omit it. The path is normalized
    (`posixpath.normpath`, since workflow paths are always POSIX regardless of
    the runner OS) before the prefix check so a form like `./examples/...`
    still matches.

    `mail-adapter` stays excluded under this rule too: its dockerfile is
    `apps/mail-adapter/Dockerfile`, not under `examples/`, so the same row that
    used to be excluded by missing `context` is now excluded on the merits --
    it is a platform image, not a bundle connector.

    A row with no `dockerfile` at all is not silently skipped: `dockerfile` is
    the one field every row must have for the build step to do anything, so
    its absence is a workflow defect in its own right, not an unrelated bug to
    tolerate. Raising here surfaces that defect immediately instead of letting
    this helper go quiet on a row that builds nothing.

    The concrete name is produced by substituting the row into the job's own
    `name:` template, so renaming the template moves both this set and the
    constant's expected values together instead of silently emptying the guard.
    """

    doc = yaml.safe_load(CI_YAML.read_text())
    job = doc["jobs"]["images"]
    template = job["name"]
    rows = ((job.get("strategy") or {}).get("matrix") or {}).get("include") or []
    names: set[str] = set()
    for row in rows:
        dockerfile = row.get("dockerfile")
        if not isinstance(dockerfile, str) or not dockerfile:
            raise AssertionError(
                f"ci.yaml images row {row!r} has no dockerfile -- the build "
                "step has nothing to build for this row"
            )
        if not posixpath.normpath(dockerfile).startswith("examples/"):
            continue
        names.add(_MATRIX_REF.sub(lambda m, row=row: str(row[m.group(1)]), template))
    return names


class TestRustValkeyWorkflowContract:
    def test_rust_job_requires_and_connects_to_valkey_guarded_tests(self):
        workflow = yaml.safe_load(CI_YAML.read_text())
        rust = workflow["jobs"]["rust"]

        assert rust["env"]["CI_REQUIRE_VALKEY_TESTS"] == "1"
        assert rust["env"]["TEST_VALKEY_URL"] == "redis://localhost:26379"

        valkey = rust["services"]["valkey"]
        assert str(valkey["image"]).startswith("valkey/")
        assert "26379:6379" in valkey["ports"]


class TestPythonValkeyWorkflowContract:
    def test_python_job_requires_valkey_guarded_tests(self):
        workflow = yaml.safe_load(CI_YAML.read_text())
        python = workflow["jobs"]["python"]

        assert python["env"]["CI_REQUIRE_VALKEY_TESTS"] == "1"


class TestHelmCiCheckRunNames:
    def test_expands_matrix_include_rows(self, tmp_path, monkeypatch):
        workflow = tmp_path / "helm-ci.yaml"
        workflow.write_text(
            """\
jobs:
  chart:
    name: Chart (${{ matrix.helm }})
    strategy:
      matrix:
        include:
          - helm: 3.16.4
          - helm: 3.17.0
"""
        )
        monkeypatch.setitem(globals(), "HELM_CI_YAML", workflow)

        assert helm_ci_job_check_run_names() == {
            "Chart (3.16.4)",
            "Chart (3.17.0)",
        }


class TestReleaseWorkflowContract:
    def test_rust_clippy_checks_all_targets_for_await_holding_lock(self):
        """Clippy must compile test targets, where this lint is reachable (#1704)."""
        workflow = yaml.load(CI_YAML.read_text(), Loader=yaml.BaseLoader)
        clippy_step = next(
            step
            for step in workflow["jobs"]["rust"]["steps"]
            if step.get("name") == "Clippy"
        )
        command = clippy_step["run"]

        for required_fragment in (
            "cargo clippy",
            "--locked",
            "--all-targets",
            "-D warnings",
        ):
            assert required_fragment in command, (
                "Rust CI Clippy must retain "
                f"{required_fragment!r} to catch await-holding-lock regressions: {command!r}"
            )

    def test_sre_bot_tempo_connector_builds_in_ordinary_ci(self):
        workflow = yaml.load(CI_YAML.read_text(), Loader=yaml.BaseLoader)
        image_job = workflow["jobs"]["images"]
        tempo_rows = [
            row
            for row in image_job["strategy"]["matrix"]["include"]
            if row.get("name") == "sre-bot-tempo"
        ]
        assert tempo_rows == [
            {
                "name": "sre-bot-tempo",
                "context": "examples/sre-bot/connectors/tempo",
                "dockerfile": "examples/sre-bot/connectors/tempo/Dockerfile",
                # release.yaml publishes every image for both architectures, so
                # CI validates the example connectors on both. Without this the
                # first place an arm64-only Dockerfile failure could appear was a
                # release, where it fails the release rather than the pull
                # request that introduced it.
                "platforms": "linux/amd64,linux/arm64",
            }
        ]
        build_step = next(
            step for step in image_job["steps"] if step.get("name") == "Build (no push)"
        )
        assert build_step["with"]["context"] == "${{ matrix.context }}"
        assert build_step["with"]["platforms"] == "${{ matrix.platforms }}", (
            "the build step must read the per-entry platforms, so the platform "
            "images keep building natively while the connectors cross-build"
        )
        assert "Build sre-bot-tempo image (no push)" in authorize_module.REQUIRED_CHECK_NAMES

    def test_image_matrix_scopes_its_layer_cache_per_image(self):
        """Every leg of the images matrix must name its own gha cache scope.

        All ten legs run concurrently and build different Dockerfiles, so an
        unscoped ``type=gha`` gives them one shared namespace with nothing to
        share: they only contend for the same reservation. Unscoped, the cache
        export outgrew the build it was meant to save -- sre-bot-tempo built in
        80.5s and spent 1057.7s writing one layer, turning a 2 minute image into
        a 19 minute job on the critical path.
        """
        workflow = yaml.load(CI_YAML.read_text(), Loader=yaml.BaseLoader)
        image_job = workflow["jobs"]["images"]
        build_step = next(
            step for step in image_job["steps"] if step.get("name") == "Build (no push)"
        )

        assert build_step["with"]["cache-from"] == "type=gha,scope=${{ matrix.name }}"
        assert build_step["with"]["cache-to"] == "type=gha,mode=max,scope=${{ matrix.name }}"

        # The scope is only per image if the key it reads is unique per leg, so
        # assert the matrix names are distinct rather than trusting the template.
        names = [row["name"] for row in image_job["strategy"]["matrix"]["include"]]
        assert len(names) == len(set(names)), f"image matrix names must be unique: {names}"

    def test_observability_stack_assertions_run_in_helm_ci(self):
        workflow = yaml.load(HELM_CI_YAML.read_text(), Loader=yaml.BaseLoader)
        chart_steps = workflow["jobs"]["helm"]["steps"]
        matching = [
            step
            for step in chart_steps
            if "observability-stack-assertions.sh" in step.get("run", "")
        ]
        assert len(matching) == 1

    def test_sre_bot_tempo_connector_is_a_first_party_release_image(self):
        workflow = yaml.load(RELEASE_YAML.read_text(), Loader=yaml.BaseLoader)
        build_matrix = workflow["jobs"]["build"]["strategy"]["matrix"]
        merge_matrix = workflow["jobs"]["merge"]["strategy"]["matrix"]

        assert "sre-bot-tempo" in build_matrix["name"]
        assert "sre-bot-tempo" in merge_matrix["name"]
        tempo_rows = [
            row
            for row in build_matrix["include"]
            if row.get("name") == "sre-bot-tempo"
        ]
        assert tempo_rows == [
            {
                "name": "sre-bot-tempo",
                "context": "examples/sre-bot/connectors/tempo",
                "dockerfile": "examples/sre-bot/connectors/tempo/Dockerfile",
            }
        ]
        build_step = next(
            step
            for step in workflow["jobs"]["build"]["steps"]
            if step.get("name") == "Build and push by digest"
        )
        assert build_step["with"]["context"] == "${{ matrix.context }}"
        tempo_context = REPO_ROOT / tempo_rows[0]["context"]
        assert (tempo_context / "requirements.txt").is_file()
        assert (tempo_context / "server.py").is_file()

    def test_release_branch_sources_are_anchored_and_wired_to_authorization(self):
        source = RELEASE_YAML.read_text()
        workflow = yaml.load(source, Loader=yaml.BaseLoader)
        trigger = workflow["on"]["push"]

        assert trigger["branches"] == ["main", "next"]
        assert trigger["tags"] == ["v*"]

        trigger_source = re.search(
            r"(?ms)^on:\n  push:\n(?P<body>.*?)(?=^permissions:)", source
        )
        assert trigger_source is not None
        anchors = {}
        for branch in ("main", "next"):
            anchor = re.search(
                rf"&(?P<name>[A-Za-z_][A-Za-z0-9_-]*)\s+['\"]?{branch}['\"]?",
                trigger_source.group("body"),
            )
            assert anchor is not None
            anchors[branch] = anchor.group("name")
        assert len(set(anchors.values())) == 2

        authorization_source = re.search(
            r"(?ms)^  authorize-release:\n(?P<body>.*?)(?=^  build:)", source
        )
        assert authorization_source is not None
        env_source = re.search(
            r"(?ms)^    env:\n(?P<body>(?:^      [^\n]+\n)+)",
            authorization_source.group("body"),
        )
        assert env_source is not None
        aliases = dict(
            re.findall(
                r"^      (?P<name>[A-Za-z_][A-Za-z0-9_]*): \*(?P<anchor>[^\s]+)\s*$",
                env_source.group("body"),
                re.MULTILINE,
            )
        )
        assert set(aliases.values()) == set(anchors.values())

        authorization = workflow["jobs"]["authorize-release"]
        authorization_env = authorization["env"]
        assert set(authorization_env) == set(aliases)
        assert {authorization_env[name] for name in aliases} == {"main", "next"}

        command = next(
            step["run"]
            for step in authorization["steps"]
            if "release/authorize.py" in step.get("run", "")
        )
        reviewed_ref_envs = re.findall(
            r"--reviewed-ref\s+origin/\$\{\{\s*env\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}",
            command,
        )
        assert "--main-ref" not in command
        assert set(reviewed_ref_envs) == set(authorization_env)
        assert len(reviewed_ref_envs) == len(authorization_env) == 2
        assert {authorization_env[name] for name in reviewed_ref_envs} == {"main", "next"}

        overlay = re.search(
            r"git checkout origin/\$\{\{\s*env\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}\s+-- release/",
            authorization_source.group("body"),
        )
        assert overlay is not None
        assert authorization_env[overlay.group(1)] == "main"

    def test_continuous_metadata_and_worker_local_base_use_the_push_sha(self):
        workflow = yaml.load(RELEASE_YAML.read_text(), Loader=yaml.BaseLoader)

        for job_name in ("merge", "worker-local-merge"):
            metadata = next(
                step
                for step in workflow["jobs"][job_name]["steps"]
                if step.get("name") == "Image metadata"
            )
            tags = metadata["with"]["tags"]
            assert "type=sha,format=long" in tags
            assert "type=raw,value=latest,enable={{is_default_branch}}" in tags

        base_tag_script = next(
            step["run"]
            for step in workflow["jobs"]["worker-local-build"]["steps"]
            if step.get("name") == "Compute base tag"
        )
        assert 'echo "v=${GITHUB_REF_NAME#v}" >> "$GITHUB_OUTPUT"' in base_tag_script
        assert 'echo "v=sha-${GITHUB_SHA}" >> "$GITHUB_OUTPUT"' in base_tag_script
        assert 'echo "v=latest" >> "$GITHUB_OUTPUT"' not in base_tag_script


class TestRequiredNamesMatchCiWorkflows:
    """`REQUIRED_CHECK_NAMES` must not drift from CI workflow job names (#811).

    The gate is fail-closed: a required name that no CI workflow job produces can
    never appear among a commit's real check-runs, so it is reported missing on
    every otherwise-legitimate commit and blocks the release. Conversely a
    stale name masks the loss of the check it was meant to assert. This pins the
    constant as a subset of the concrete check-run names the current CI workflows
    actually emit, parsed live (never derived from the constant itself).

    So renaming or splitting a required job in a CI workflow without updating
    `REQUIRED_CHECK_NAMES` fails here (with the drifted names listed), rather
    than silently dropping a release gate.

    That subset relation is one-directional, and the second test closes the
    other direction for the connector images (#1951). Subset means ADDING a job
    to `ci.yaml` and never requiring it here keeps this class green forever --
    which is how three of the four `examples/` connector image builds came to
    be unrequired while `sre-bot-tempo` alone was named. An unrequired connector
    build that goes red on `next` still authorizes the tag, then fails its
    `release.yaml` `build` leg, and `merge` (`needs.build.result == 'success'`)
    skips the multi-arch manifest for EVERY image while the CLI binaries and
    the GitHub Release publish anyway.
    """

    def test_every_required_name_is_a_real_ci_workflow_check_run_name(self):
        ci_names = ci_job_check_run_names() | helm_ci_job_check_run_names()
        stale = authorize_module.REQUIRED_CHECK_NAMES - ci_names

        assert authorize_module.REQUIRED_CHECK_NAMES <= ci_names, (
            "REQUIRED_CHECK_NAMES has drifted from the CI workflows -- these required "
            f"names match no current job check-run: {sorted(stale)}"
        )

    def test_every_connector_image_build_is_a_required_check(self):
        connector_names = ci_connector_image_check_run_names()

        # A guard that finds nothing passes vacuously. If a matrix restructure,
        # a `dockerfile` convention change, or a rename of the `images` job
        # empties this set, that must read as a broken guard rather than as
        # compliance.
        assert connector_names, (
            "no connector image rows found in ci.yaml's `images` job -- the matrix "
            "shape or the `dockerfile: examples/...` convention changed, and "
            "ci_connector_image_check_run_names() now guards nothing"
        )

        unrequired = connector_names - authorize_module.REQUIRED_CHECK_NAMES

        assert unrequired == set(), (
            "ci.yaml builds connector images that no required check names, so a red "
            f"connector build would still authorize a release: {sorted(unrequired)}. "
            "Add each of those names to REQUIRED_CHECK_NAMES in release/authorize.py. "
            "A connector build failing on next without this authorizes the tag, fails "
            "release.yaml's `build` leg, and skips the manifest `merge` for every "
            "image while the binaries and the GitHub Release publish anyway."
        )


class TestHelmCiWorkflowTriggers:
    """Pin the deliberate push and pull request trigger asymmetry."""

    def test_push_trigger_is_unfiltered_for_releasable_branches(self):
        doc = yaml.safe_load(HELM_CI_YAML.read_text())
        triggers = doc[True]

        assert triggers["push"]["branches"] == ["main", "next"]
        assert "paths" not in triggers["push"]
        assert "paths-ignore" not in triggers["push"]
        assert triggers["pull_request"]["branches"] == ["main", "next"]
        # The Python paths are not strays to tidy up: the object-store
        # web-identity gate executes those repository files against each
        # rendered workload, so a PR touching only them (a revert of the
        # credential fix) must still match this filter or the gate never runs.
        # compose.dev.yaml is here for the same reason: two chart gates (the
        # unpinned-image gate, #2319, and the Langfuse image-pin gate, #2190)
        # read it, so a compose-only unpin must still trigger this workflow.
        assert triggers["pull_request"]["paths"] == [
            "charts/curie/**",
            "examples/sre-bot/observability/**",
            ".github/workflows/helm-ci.yaml",
            "packages/aci-protocol/src/aci_protocol/s3.py",
            "apps/api/src/curie_api/config.py",
            "apps/api/src/curie_api/storage.py",
            "apps/worker/src/curie_worker/config.py",
            "apps/worker/src/curie_worker/bundle_store.py",
            "uv.lock",
            "pyproject.toml",
            "compose.dev.yaml",
        ]


class TestMixedPassFailRequiredCheck:
    """A required name with any non-passing run is not satisfied (issue #811).

    The set logic only tracks names that have at least one *passing* run, so a
    required check that ran twice -- once green, once red (a re-run that failed)
    -- has its name in the passing set and is silently treated as satisfied.
    For a fail-closed release gate that masks a genuinely failing required
    check. These use the `required_names` override so they exercise the logic
    independent of the production constant.
    """

    MIXED_CI = [
        {"name": "CI", "conclusion": "success"},
        {"name": "CI", "conclusion": "failure"},  # a re-run that failed
        {"name": "CodeQL", "conclusion": "success"},
    ]

    def test_a_required_name_with_a_failing_run_is_reported_missing(self):
        assert "CI" in authorize_module.missing_required_checks(
            self.MIXED_CI, TEST_REQUIRED_NAMES
        )

    def test_a_required_name_with_a_failing_run_is_not_satisfied(self):
        assert authorize_module.missing_required_checks(
            self.MIXED_CI, TEST_REQUIRED_NAMES
        )

    def test_a_single_passing_run_with_no_failing_run_is_satisfied(self):
        # Positive boundary: the fix must not over-reject a clean pass.
        runs = [
            {"name": "CI", "conclusion": "success"},
            {"name": "CodeQL", "conclusion": "success"},
        ]

        assert authorize_module.missing_required_checks(runs, TEST_REQUIRED_NAMES) == set()

    def test_multiple_all_passing_runs_stay_satisfied(self):
        # Positive boundary: several entries for a required name, all passing.
        runs = [
            {"name": "CI", "conclusion": "success"},
            {"name": "CI", "conclusion": "neutral"},
            {"name": "CodeQL", "conclusion": "success"},
        ]

        assert authorize_module.missing_required_checks(runs, TEST_REQUIRED_NAMES) == set()

    def test_authorize_refuses_a_mixed_pass_fail_required_check(self, git_repo):
        sha = run_git(git_repo, "rev-parse", "HEAD")

        with pytest.raises(
            authorize_module.AuthorizationError, match="required check-run"
        ):
            authorize_module.authorize(
                sha,
                self.MIXED_CI,
                ("main",),
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
            )


class TestSkippedRequiredCheck:
    """A required check-run that concluded `skipped` is not a pass (#1470).

    `skipped` used to sit in `PASSING_CONCLUSIONS` alongside `success` and
    `neutral`, so a required check that never actually ran authorized a
    release. Two routes produce it, and both are live risks here:

      * a job-level `if:` added to the job. Adding one to helm-ci.yaml's
        `helm` job would make GitHub record
        `Chart (lint + template + kubeconform)` as `skipped` on every
        releasable push, and the gate would then authorize a release whose
        chart was never rendered, linted, or kubeconform-validated.
      * a failed job in the job's `needs:`. ci.yaml's `rust-build` and
        `changes` are not themselves required names, so a FAILED `rust-build`
        skips the three ladder jobs -- which ARE required names -- into
        `conclusion: skipped`, and that authorized a release too.

    Measured live against the real `authorize()` before this change, with the
    chart check varied and all 15 other required checks `success`:
    `success` authorized, `failure` raised, `skipped` AUTHORIZED (the defect),
    `cancelled` raised, `neutral` authorized, `None` raised, and an absent
    entry raised. Only the `skipped` row changes here; a workflow whose
    triggers never match produces NO check-run at all and is still correctly
    refused as absent.
    """

    def test_a_required_name_whose_only_run_was_skipped_is_reported_missing(self):
        runs = [
            {"name": "CI", "conclusion": "success"},
            {"name": "CodeQL", "conclusion": "skipped"},
        ]

        assert authorize_module.missing_required_checks(
            runs, TEST_REQUIRED_NAMES
        ) == {"CodeQL"}

    def test_authorize_refuses_a_skipped_chart_check_when_every_other_check_passes(
        self, git_repo
    ):
        # The exact scenario a job-level `if:` on helm-ci.yaml's `helm` job
        # would produce, driven against the real production
        # REQUIRED_CHECK_NAMES.
        sha = run_git(git_repo, "rev-parse", "HEAD")
        chart_check = "Chart (lint + template + kubeconform)"
        runs = [
            {
                "name": name,
                "conclusion": "skipped" if name == chart_check else "success",
            }
            for name in authorize_module.REQUIRED_CHECK_NAMES
        ]

        with pytest.raises(
            authorize_module.AuthorizationError, match="required check-run"
        ):
            authorize_module.authorize(sha, runs, ("main",), cwd=git_repo)

    def test_a_required_name_with_both_a_success_and_a_skipped_run_is_refused(
        self, git_repo
    ):
        # Fail-closed, consistent with the mixed pass/fail behavior above: one
        # green run does not excuse a same-named run that never executed.
        sha = run_git(git_repo, "rev-parse", "HEAD")
        runs = [
            {"name": "CI", "conclusion": "success"},
            {"name": "CI", "conclusion": "skipped"},
            {"name": "CodeQL", "conclusion": "success"},
        ]

        assert "CI" in authorize_module.missing_required_checks(
            runs, TEST_REQUIRED_NAMES
        )
        with pytest.raises(
            authorize_module.AuthorizationError, match="required check-run"
        ):
            authorize_module.authorize(
                sha, runs, ("main",), cwd=git_repo, required_names=TEST_REQUIRED_NAMES
            )


class TestLegitimateSkips:
    """Why refusing skipped required checks does not over-reject (#1470).

    There is exactly ONE legitimate `skipped` conclusion, enumerated below: a
    check-run whose name is not in the required set, which the gate ignores
    entirely. It needs no blanket allowance and does not put `skipped` back in
    `PASSING_CONCLUSIONS`.

    The two ci.yaml tests here are not a second legitimate skip. They pin the
    only place a required job carries a job-level `if:` -- the conditional
    ladder jobs -- so that it cannot conclude `skipped` on a releasable push,
    which is what keeps the stricter gate from blocking every release.
    """

    def test_a_skipped_check_run_outside_the_required_set_does_not_block(
        self, git_repo
    ):
        # CHECK_RUNS_ALL_GREEN carries `Secret Scan` at `skipped`, which is not
        # a required name. Non-required entries are ignored entirely, so this
        # still authorizes -- the only legitimate skip.
        sha = run_git(git_repo, "rev-parse", "HEAD")

        authorize_module.authorize(
            sha,
            CHECK_RUNS_ALL_GREEN,
            ("main",),
            cwd=git_repo,
            required_names=TEST_REQUIRED_NAMES,
        )

    def test_required_ci_job_conditions_match_tier_outputs(self):
        """Required conditional jobs must select exactly their owned tiers."""
        doc = yaml.safe_load(CI_YAML.read_text())
        actual = {
            job_id: str(job["if"]).strip()
            for job_id, job in doc["jobs"].items()
            if job.get("name") in authorize_module.REQUIRED_CHECK_NAMES
            and "if" in job
        }
        expected = {
            "e2e-ladder": (
                "${{ needs.changes.outputs.skill == 'true' || "
                "needs.changes.outputs.local == 'true' }}"
            ),
            "e2e-ladder-release": (
                "${{ needs.changes.outputs.local_release == 'true' }}"
            ),
        }

        assert actual == expected, (
            "required ci.yaml jobs no longer gate on their exact tier outputs: "
            f"expected {expected!r}, got {actual!r}"
        )

    def test_the_changes_filter_step_emits_every_tier_when_executed_as_a_push(
        self, tmp_path
    ):
        """A push selects every tier through the real selector runtime."""
        doc = yaml.safe_load(CI_YAML.read_text())
        filter_step = next(
            step
            for step in doc["jobs"]["changes"]["steps"]
            if step.get("id") == "filter"
        )
        script = re.sub(
            r"\$\{\{\s*(.*?)\s*\}\}",
            lambda m: "push" if m.group(1) == "github.event_name" else "",
            filter_step["run"],
        )
        script_path = tmp_path / "filter.sh"
        script_path.write_text(script)
        selector_path = tmp_path / "tools" / "e2e-ci-selection" / "select_tiers.py"
        selector_path.parent.mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / "tools" / "e2e-ci-selection" / "select_tiers.py", selector_path
        )
        registry_path = tmp_path / ".github" / "e2e-selection.yaml"
        registry_path.parent.mkdir()
        shutil.copy2(REPO_ROOT / ".github" / "e2e-selection.yaml", registry_path)
        github_output = tmp_path / "github_output"
        github_output.touch()

        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=tmp_path,
            env={**os.environ, "GITHUB_OUTPUT": str(github_output)},
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, (
            "ci.yaml's `changes` filter step no longer selects tiers on push: "
            f"{result.stderr}"
        )
        assert github_output.read_text().splitlines() == [
            "skill=true",
            "local=true",
            "local_release=true",
            "cluster=true",
            "released_upgrade=true",
            "skill_local_tiers=skill,local",
            "pytest=true",
        ], (
            "ci.yaml's push selection no longer emits the complete tier contract: "
            f"{github_output.read_text()!r}"
        )


class TestHelmCiJobsCarryNoJobLevelIf:
    """No helm-ci.yaml job may carry a job-level `if:` (#1470).

    A job-level `if:` on the `helm` job makes GitHub record
    `Chart (lint + template + kubeconform)` -- a required check name -- with
    `conclusion: skipped` on every releasable push. The gate now refuses a
    skipped required check, so every release would be blocked with the chart
    never rendered; before that change it silently AUTHORIZED instead, which is
    strictly worse. Either way the job must simply run on push.

    `TestHelmCiWorkflowTriggers` pins the trigger-level route into the same
    hole (a filtered `push:` trigger produces no check-run at all); this pins
    the job-level route. Asserted over every job in the workflow, not just
    `helm`, so a future job added there is covered without editing this test.
    """

    def test_no_helm_ci_job_declares_a_job_level_if(self):
        doc = yaml.safe_load(HELM_CI_YAML.read_text())
        conditional = sorted(
            job_id for job_id, job in doc["jobs"].items() if "if" in job
        )

        assert not conditional, (
            "helm-ci.yaml job(s) carry a job-level `if:`, which makes their "
            "check-run `skipped` on releasable pushes and blocks every release "
            f"with the chart never rendered: {conditional}"
        )
