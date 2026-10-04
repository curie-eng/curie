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
import posixpath
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

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
            tag="v0.7.0-rc.5",
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


class TestTagClassAuthorization:
    """Issue #1560: final tags require the primary reviewed ref; prereleases may use any.

    The four cases are the issue's acceptance criteria. Reverting the
    classification in `authorize()` makes `test_final_tag_on_next_only_commit_is_refused`
    fail: that commit is reachable from `origin/next` and would authorize again.
    """

    def test_final_tag_on_next_only_commit_is_refused(self, reviewed_refs_repo):
        git_repo, commits = reviewed_refs_repo

        with pytest.raises(authorize_module.AuthorizationError) as exc_info:
            authorize_module.authorize(
                commits["next"],
                CHECK_RUNS_ALL_GREEN,
                REVIEWED_REFS,
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
                tag="v0.7.0",
            )

        message = str(exc_info.value)
        assert "origin/main" in message
        assert "merge" in message.lower()
        assert "tag" in message.lower()

    def test_prerelease_tag_on_next_only_commit_is_authorized(self, reviewed_refs_repo):
        git_repo, commits = reviewed_refs_repo

        authorize_module.authorize(
            commits["next"],
            CHECK_RUNS_ALL_GREEN,
            REVIEWED_REFS,
            cwd=git_repo,
            required_names=TEST_REQUIRED_NAMES,
            tag="v0.7.0-rc.5",
        )

    def test_final_tag_on_main_commit_is_authorized(self, reviewed_refs_repo):
        git_repo, commits = reviewed_refs_repo

        authorize_module.authorize(
            commits["main"],
            CHECK_RUNS_ALL_GREEN,
            REVIEWED_REFS,
            cwd=git_repo,
            required_names=TEST_REQUIRED_NAMES,
            tag="v0.7.0",
        )

    def test_commit_reachable_from_neither_ref_is_still_refused(
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
                tag="v0.7.0",
            )

        message = str(exc_info.value)
        assert "not reachable from any reviewed ref" in message
        assert "origin/main" in message
        assert "origin/next" in message

    def test_omitted_tag_on_next_only_commit_is_treated_as_final(
        self, reviewed_refs_repo
    ):
        git_repo, commits = reviewed_refs_repo

        with pytest.raises(authorize_module.AuthorizationError) as exc_info:
            authorize_module.authorize(
                commits["next"],
                CHECK_RUNS_ALL_GREEN,
                REVIEWED_REFS,
                cwd=git_repo,
                required_names=TEST_REQUIRED_NAMES,
            )

        assert "origin/main" in str(exc_info.value)


class TestClassifyReleaseTag:
    def test_final_semver_is_final(self):
        assert authorize_module.classify_release_tag("v0.7.0") == "final"

    def test_rc_prerelease_is_prerelease(self):
        assert authorize_module.classify_release_tag("v0.7.0-rc.5") == "prerelease"

    def test_build_metadata_without_prerelease_is_final(self):
        assert authorize_module.classify_release_tag("v0.7.0+build.1") == "final"

    def test_invalid_tag_is_refused(self):
        with pytest.raises(authorize_module.AuthorizationError, match="semver"):
            authorize_module.classify_release_tag("vnext")


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
            [
                sha,
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "main",
                "--tag",
                "v0.7.0",
            ]
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
            [
                sha,
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "main",
                "--tag",
                "v0.7.0",
            ]
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
            [
                sha,
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "main",
                "--tag",
                "v0.7.0",
            ]
        )

        assert exit_code == 1

    def test_main_refuses_a_final_tag_on_a_next_only_commit(
        self, reviewed_refs_repo, monkeypatch, capsys
    ):
        git_repo, commits = reviewed_refs_repo
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)

        exit_code = authorize_module.main(
            [
                commits["next"],
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "origin/main",
                "--reviewed-ref",
                "origin/next",
                "--tag",
                "v0.7.0",
            ]
        )

        assert exit_code == 1
        err = capsys.readouterr().err
        assert "origin/main" in err
        assert "merge" in err.lower()

    def test_main_authorizes_a_prerelease_tag_on_a_next_only_commit(
        self, reviewed_refs_repo, monkeypatch
    ):
        git_repo, commits = reviewed_refs_repo
        self._use_test_required_names(monkeypatch)
        self._stub_fetch_check_runs(monkeypatch, self._runs_with_gate_own_in_progress())
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
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)

        exit_code = authorize_module.main(
            [
                commits["next"],
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "origin/main",
                "--reviewed-ref",
                "origin/next",
                "--tag",
                "v0.7.0-rc.5",
            ]
        )

        assert exit_code == 0


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
            [
                sha,
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "main",
                "--tag",
                "v0.7.0",
            ]
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


# The exact five files of the real v0.11.2 preparation PR #3845 (#3858): a
# release bump changes only version identity, and the atlas snapshot it adds
# is named for that release.
V0112_PREP_PATHS = (
    "charts/curie/Chart.yaml",
    "cli/Cargo.toml",
    "cli/Cargo.lock",
    "docs/architecture-atlas/versions.json",
    "docs/architecture-atlas/snapshots/v0.11.2.json",
)
V0112_TAG = "v0.11.2"


def commit_paths(repo: Path, paths: Sequence[str], label: str) -> str:
    """Write every path with label-specific content and commit them together."""
    for path in paths:
        target = repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"{path} {label}\n")
    run_git(repo, "add", "--", *paths)
    run_git(repo, "commit", "-q", "-m", label)
    return run_git(repo, "rev-parse", "HEAD")


def green_runs(job_prefix: str = "9") -> list[dict]:
    return [
        check_run("CI", "success", OTHER_RUN_ID, f"{job_prefix}1"),
        check_run("CodeQL", "neutral", OTHER_RUN_ID, f"{job_prefix}2"),
    ]


def red_runs(job_prefix: str = "8") -> list[dict]:
    return [
        check_run("CI", "success", OTHER_RUN_ID, f"{job_prefix}1"),
        check_run("CodeQL", "failure", OTHER_RUN_ID, f"{job_prefix}2"),
    ]


class FakeProofFetcher:
    """Check-runs keyed by sha, shaped like the live API, recording each lookup."""

    def __init__(self, table: dict[str, list[dict]]) -> None:
        self.table = table
        self.calls: list[str] = []

    def __call__(self, sha: str) -> list[dict]:
        self.calls.append(sha)
        return list(self.table.get(sha, []))


class TestVersionOnlyProof:
    """#3858: a tag on a version-only commit may borrow its parent's green CI.

    The tagged commit S changes exactly the version-only set for its own tag
    (`release/atlas.py`'s `version_only_paths`). When S's own required checks
    are absent or still running, the gate walks S's first-parent ancestors and
    authorizes against the nearest one whose required checks are green, as long
    as everything between that ancestor and S is version-only. Contrary
    evidence on S itself (a required check that concluded non-passing) still
    refuses, and any other path in the delta still refuses.
    """

    @staticmethod
    def _prep(git_repo: Path, paths: Sequence[str] = V0112_PREP_PATHS) -> tuple[str, str]:
        parent = run_git(git_repo, "rev-parse", "HEAD")
        tagged = commit_paths(git_repo, paths, "prepare release")
        return parent, tagged

    @staticmethod
    def _authorize(
        git_repo: Path,
        sha: str,
        own_runs: list[dict],
        fetcher: FakeProofFetcher | None,
        *,
        tag: str | None = V0112_TAG,
        reviewed_refs: Sequence[str] = ("main",),
        exclude_run_id: str | None = CURRENT_RUN_ID,
    ):
        return authorize_module.authorize(
            sha,
            own_runs,
            reviewed_refs,
            cwd=git_repo,
            exclude_run_id=exclude_run_id,
            required_names=TEST_REQUIRED_NAMES,
            tag=tag,
            fetch_proof_check_runs=fetcher,
        )

    def test_proof_depth_is_ten(self):
        assert authorize_module.VERSION_ONLY_PROOF_DEPTH == 10

    def test_version_only_tag_with_no_own_runs_authorizes_from_its_green_parent(
        self, git_repo
    ):
        parent, tagged = self._prep(git_repo)
        fetcher = FakeProofFetcher({parent: green_runs()})

        proof = self._authorize(git_repo, tagged, [GATE_OWN_IN_PROGRESS], fetcher)

        assert proof == parent
        assert fetcher.calls == [parent]

    def test_liveness_realistic_in_progress_tag_authorizes(self, git_repo):
        """The liveness case: the tag push's own CI is still running.

        This is the shape the gate actually sees at tag time: the merge commit's
        push CI is in progress (conclusion null) and the gate's own job is on
        the same sha. Before #3858 the gate refused until that CI finished.
        """
        parent, tagged = self._prep(git_repo)
        own_runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", None, OTHER_RUN_ID, "71"),
            check_run("CodeQL", None, OTHER_RUN_ID, "72"),
        ]
        fetcher = FakeProofFetcher({parent: green_runs()})

        assert self._authorize(git_repo, tagged, own_runs, fetcher) == parent

    def test_refs_tags_prefix_names_the_same_snapshot(self, git_repo):
        parent, tagged = self._prep(git_repo)
        fetcher = FakeProofFetcher({parent: green_runs()})

        proof = self._authorize(
            git_repo, tagged, [], fetcher, tag=f"refs/tags/{V0112_TAG}"
        )

        assert proof == parent

    def test_own_green_checks_return_none_and_never_fetch_a_proof(self, git_repo):
        parent, tagged = self._prep(git_repo)
        fetcher = FakeProofFetcher({parent: green_runs()})

        proof = self._authorize(
            git_repo, tagged, [GATE_OWN_IN_PROGRESS, *green_runs("5")], fetcher
        )

        assert proof is None
        assert fetcher.calls == []

    def test_a_source_path_in_the_delta_is_refused(self, git_repo):
        parent, tagged = self._prep(git_repo, (*V0112_PREP_PATHS, "cli/src/main.rs"))
        fetcher = FakeProofFetcher({parent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher)

    def test_another_versions_snapshot_in_the_delta_is_refused(self, git_repo):
        paths = (*V0112_PREP_PATHS[:-1], "docs/architecture-atlas/snapshots/v9.9.9.json")
        parent, tagged = self._prep(git_repo, paths)
        fetcher = FakeProofFetcher({parent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher)

    def test_red_parent_is_not_a_proof(self, git_repo):
        parent, tagged = self._prep(git_repo)
        fetcher = FakeProofFetcher({parent: red_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher)

    @pytest.mark.parametrize("conclusion", ["failure", "skipped", "cancelled", "timed_out"])
    def test_own_required_check_that_concluded_non_passing_refuses_despite_green_parent(
        self, git_repo, conclusion
    ):
        # Contrary evidence on the tagged commit itself wins over any proof.
        # `skipped` included: #1470 refuses a skipped required check.
        parent, tagged = self._prep(git_repo)
        own_runs = [
            GATE_OWN_IN_PROGRESS,
            check_run("CI", conclusion, OTHER_RUN_ID, "61"),
            check_run("CodeQL", None, OTHER_RUN_ID, "62"),
        ]
        fetcher = FakeProofFetcher({parent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, own_runs, fetcher)

    def test_no_tag_means_no_proof_fallback(self, git_repo):
        parent, tagged = self._prep(git_repo)
        fetcher = FakeProofFetcher({parent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher, tag=None)

    def test_no_fetcher_means_no_proof_fallback(self, git_repo):
        _parent, tagged = self._prep(git_repo)

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], None)

    def test_parent_runs_from_the_current_workflow_run_are_not_a_proof(self, git_repo):
        parent, tagged = self._prep(git_repo)
        fetcher = FakeProofFetcher(
            {
                parent: [
                    check_run("CI", "success", CURRENT_RUN_ID, "51"),
                    check_run("CodeQL", "success", CURRENT_RUN_ID, "52"),
                ]
            }
        )

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher)

    def test_proof_two_commits_back_through_a_red_version_only_commit(self, git_repo):
        grandparent = run_git(git_repo, "rev-parse", "HEAD")
        middle = commit_paths(git_repo, ("cli/Cargo.toml", "cli/Cargo.lock"), "bump once")
        tagged = commit_paths(git_repo, V0112_PREP_PATHS, "prepare release")
        fetcher = FakeProofFetcher({grandparent: green_runs(), middle: red_runs()})

        assert self._authorize(git_repo, tagged, [], fetcher) == grandparent
        assert fetcher.calls == [middle, grandparent]

    def test_no_proof_past_a_non_version_only_ancestor_delta(self, git_repo):
        grandparent = run_git(git_repo, "rev-parse", "HEAD")
        middle = commit_paths(git_repo, ("cli/src/main.rs",), "runtime change")
        tagged = commit_paths(git_repo, V0112_PREP_PATHS, "prepare release")
        fetcher = FakeProofFetcher({grandparent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher)
        # The middle commit is a version-only distance from S, so it is
        # consulted (and has no runs); the walk stops before the green
        # grandparent because that delta includes cli/src/main.rs.
        assert fetcher.calls == [middle]

    def test_proof_walk_stops_at_the_depth_limit(self, git_repo):
        depth = authorize_module.VERSION_ONLY_PROOF_DEPTH
        green = run_git(git_repo, "rev-parse", "HEAD")
        for index in range(depth - 1):
            commit_paths(git_repo, ("cli/Cargo.toml",), f"bump {index}")
        at_limit = commit_paths(git_repo, V0112_PREP_PATHS, "prepare at limit")
        fetcher = FakeProofFetcher({green: green_runs()})

        assert self._authorize(git_repo, at_limit, [], fetcher) == green

        past_limit = commit_paths(git_repo, ("cli/Cargo.lock",), "one more bump")
        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, past_limit, [], FakeProofFetcher({green: green_runs()}))

    def test_prerelease_tag_uses_its_own_snapshot_name(self, git_repo):
        tag = "v0.12.0-rc.1"
        paths = (*V0112_PREP_PATHS[:-1], f"docs/architecture-atlas/snapshots/{tag}.json")
        parent, tagged = self._prep(git_repo, paths)
        fetcher = FakeProofFetcher({parent: green_runs()})

        assert self._authorize(git_repo, tagged, [], fetcher, tag=tag) == parent

    def test_prerelease_tag_refuses_the_final_versions_snapshot(self, git_repo):
        paths = (*V0112_PREP_PATHS[:-1], "docs/architecture-atlas/snapshots/v0.12.0.json")
        parent, tagged = self._prep(git_repo, paths)
        fetcher = FakeProofFetcher({parent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, tagged, [], fetcher, tag="v0.12.0-rc.1")

    def test_ancestry_refusal_still_wins_over_a_green_version_only_parent(
        self, git_repo
    ):
        parent = run_git(git_repo, "rev-parse", "HEAD")
        run_git(git_repo, "checkout", "-q", "-b", "unreviewed")
        tagged = commit_paths(git_repo, V0112_PREP_PATHS, "prepare off main")
        run_git(git_repo, "checkout", "-q", "main")
        fetcher = FakeProofFetcher({parent: green_runs()})

        with pytest.raises(authorize_module.AuthorizationError, match="not reachable"):
            self._authorize(git_repo, tagged, [], fetcher)

    @staticmethod
    def _merge_prep_branch(git_repo: Path) -> tuple[str, str, str]:
        """Merge a version-only prep branch into main with a real merge commit."""
        first_parent = run_git(git_repo, "rev-parse", "HEAD")
        run_git(git_repo, "checkout", "-q", "-b", "prep")
        pr_head = commit_paths(git_repo, V0112_PREP_PATHS, "prepare release")
        run_git(git_repo, "checkout", "-q", "main")
        run_git(git_repo, "merge", "-q", "--no-ff", "-m", "Merge prep", "prep")
        merge = run_git(git_repo, "rev-parse", "HEAD")
        assert run_git(git_repo, "rev-parse", f"{merge}^1") == first_parent
        assert run_git(git_repo, "rev-parse", f"{merge}^2") == pr_head
        return first_parent, pr_head, merge

    def test_merge_commit_borrows_its_first_parent(self, git_repo):
        first_parent, pr_head, merge = self._merge_prep_branch(git_repo)
        fetcher = FakeProofFetcher({first_parent: green_runs(), pr_head: green_runs("4")})

        assert self._authorize(git_repo, merge, [], fetcher) == first_parent
        assert pr_head not in fetcher.calls

    def test_merge_commit_green_second_parent_alone_is_not_a_proof(self, git_repo):
        first_parent, pr_head, merge = self._merge_prep_branch(git_repo)
        fetcher = FakeProofFetcher({first_parent: red_runs(), pr_head: green_runs("4")})

        with pytest.raises(authorize_module.AuthorizationError, match="required check-run"):
            self._authorize(git_repo, merge, [], fetcher)
        assert pr_head not in fetcher.calls


class TestMainVersionOnlyProof:
    """#3858: `main()` wires the live fetcher in as the proof source."""

    @staticmethod
    def _run(git_repo: Path, monkeypatch, tagged: str, table: dict[str, list[dict]]) -> int:
        TestMain._use_test_required_names(monkeypatch)
        TestMain._stub_green_nightly(monkeypatch)
        monkeypatch.setattr(
            authorize_module, "fetch_check_runs", lambda sha, repo: table.get(sha, [])
        )
        monkeypatch.chdir(git_repo)
        monkeypatch.setenv("GITHUB_RUN_ID", CURRENT_RUN_ID)
        return authorize_module.main(
            [
                tagged,
                "--repo",
                "curie-eng/curie",
                "--reviewed-ref",
                "main",
                "--tag",
                V0112_TAG,
            ]
        )

    def test_main_authorizes_a_version_only_tag_and_names_the_proof_commit(
        self, git_repo, monkeypatch, capsys
    ):
        parent = run_git(git_repo, "rev-parse", "HEAD")
        tagged = commit_paths(git_repo, V0112_PREP_PATHS, "prepare release")
        table = {
            parent: green_runs(),
            tagged: [GATE_OWN_IN_PROGRESS, check_run("CI", None, OTHER_RUN_ID, "31")],
        }

        exit_code = self._run(git_repo, monkeypatch, tagged, table)

        captured = capsys.readouterr()
        assert exit_code == 0, captured.err
        assert captured.out.startswith("OK:")
        assert parent in captured.out

    def test_main_refuses_a_tag_whose_delta_is_not_version_only(
        self, git_repo, monkeypatch, capsys
    ):
        parent = run_git(git_repo, "rev-parse", "HEAD")
        tagged = commit_paths(
            git_repo, (*V0112_PREP_PATHS, "cli/src/main.rs"), "prepare with code"
        )
        table = {parent: green_runs(), tagged: [GATE_OWN_IN_PROGRESS]}

        exit_code = self._run(git_repo, monkeypatch, tagged, table)

        assert exit_code == 1
        assert "required check-run" in capsys.readouterr().err


CI_YAML = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
HELM_CI_YAML = REPO_ROOT / ".github" / "workflows" / "helm-ci.yaml"

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
        assert "paths" not in triggers["pull_request"]
        assert "paths-ignore" not in triggers["pull_request"]
        # Those trees are executed by the chart scripts. The list now lives
        # in CHART_PATHS, and a pull request that touches one must still
        # select the chart run.
        assert _load_helm_decide().CHART_PATHS == (
            "charts/curie/**",
            "examples/sre-bot/**",
            ".github/workflows/helm-ci.yaml",
            ".github/workflows/ci.yaml",
            ".github/workflows/release.yaml",
            "cli/**",
            "apps/api/**",
            "apps/worker/**",
            "apps/dispatcher/**",
            "packages/**",
            "scripts/**",
            "uv.lock",
            "pyproject.toml",
            "compose.yaml",
            "compose.dev.yaml",
            "cli/src/ops/upgrade.rs",
            "cli/tests/data/upgrade-driver.py",
            "packages/aci-protocol/src/aci_protocol/slack_identities.py",
            "packages/aci-protocol/src/aci_protocol/turn.py",
            "apps/worker/src/curie_worker/sandbox/types.py",
            "compose/**",
        )

    def test_a_cli_only_change_runs_the_chart_scripts(self):
        # The Rust job no longer runs the chart scripts, so a CLI-only PR
        # that breaks upgrade-retained-scalar or observability-stack is
        # caught only if helm-ci's filter matches it.
        paths = _load_helm_decide().CHART_PATHS
        for changed in (
            "cli/src/ops/upgrade.rs",
            "cli/src/examples.rs",
            "cli/scripts/e2e-ladder.sh",
            "cli/Cargo.lock",
        ):
            assert any(
                PurePosixPath(changed).full_match(pattern) for pattern in paths
            ), f"{changed} matches no helm-ci pull_request path"


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
        images_if = "${{ needs.changes.outputs.images == 'true' }}"
        expected = {
            "ci-images": (
                "${{ needs.changes.outputs.images == 'true' || "
                "needs.changes.outputs.skill == 'true' || "
                "needs.changes.outputs.local == 'true' || "
                "needs.changes.outputs.local_release == 'true' || "
                "needs.changes.outputs.cluster == 'true' || "
                "needs.changes.outputs.released_upgrade == 'true' }}"
            ),
            "dispatcher-image-smoke": images_if,
            "repo-toolchain-proof": images_if,
            "eval-falsifiability": "${{ needs.changes.outputs.skill == 'true' }}",
            "e2e-ladder": (
                "${{ needs.changes.outputs.skill == 'true' || "
                "needs.changes.outputs.local == 'true' }}"
            ),
            "e2e-ladder-release": (
                "${{ needs.changes.outputs.local_release == 'true' }}"
            ),
            # Not a tier gate: the required Python job waits on its pytest
            # shards and must still run and report whatever they concluded.
            "python": "always()",
            # Same for Rust: it waits on its test partitions.
            "rust": "${{ always() }}",
        }

        assert actual == expected, (
            "required ci.yaml jobs no longer gate on their exact tier outputs: "
            f"expected {expected!r}, got {actual!r}"
        )


class TestHelmCiJobsCarryNoJobLevelIf:
    """A conditional if on helm skips the required check name on a releasable push.

    After #1470 the gate refuses a skipped required check, so that skip blocks
    every release. helm's if must be exactly `${{ !cancelled() }}` so a
    completed run cannot record the required check as skipped. chart-changes
    has no if. The four gated jobs use exactly
    `${{ needs.chart-changes.outputs.chart == 'true' }}`. Any other job-level
    if fails the test.
    """

    GATED = (
        "chart-lint-and-assertions-1",
        "chart-assertions-2",
        "chart-assertions-retained-values",
        "reserved-env-upgrade",
    )
    GATED_IF = "${{ needs.chart-changes.outputs.chart == 'true' }}"

    def test_only_the_chart_gate_ifs_are_allowed(self):
        jobs = yaml.safe_load(HELM_CI_YAML.read_text())["jobs"]

        assert "if" not in jobs["chart-changes"]
        assert jobs["helm"]["if"] == "${{ !cancelled() }}"
        for job_id in self.GATED:
            assert jobs[job_id]["if"] == self.GATED_IF
        extra = sorted(
            job_id
            for job_id, job in jobs.items()
            if "if" in job and job_id not in {"helm", *self.GATED}
        )
        assert not extra, (
            "helm-ci.yaml job(s) carry an unexpected job-level if, which can "
            f"skip the required chart check and block every release: {extra}"
        )


def _load_helm_decide():
    """Import the chart gate by path. Do not name the module `select`."""
    path = REPO_ROOT / "tools" / "helm-ci-gate" / "decide.py"
    spec = importlib.util.spec_from_file_location("helm_ci_gate_decide", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
