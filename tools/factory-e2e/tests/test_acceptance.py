"""Unit contract for the acceptance runner (tools/factory-e2e/acceptance.py).

One entry per tracker and code host pairing. Each entry fixes its steps: the
inputs, the `factory_e2e.py` command, a deadline, what the forge must show,
which evidence fields must hold, and the teardown checks. These pin that a dry
run lists every step and refuses an entry with an unset field, that a pairing
whose forge is not installed launches nothing, and that a real github+github
run passes the shared flags to each step, judges each step's evidence and
teardown, and writes one summary.

The real-run contract the fakes below rely on:

1. ``acceptance.run(args, *, runner, kubectl)`` where ``args`` is
   ``factory_e2e.parse_args(["acceptance", ...])``.
2. ``runner(argv, timeout) -> int`` runs one step's full subprocess argv (which
   contains the step's command and a per-step ``--evidence <path>``) under
   ``timeout == step.deadline_seconds``; it may raise
   ``subprocess.TimeoutExpired``.
3. Each persisted assertion is a ``(dotted evidence path, expected value)``
   pair.
4. ``kubectl(argv) -> subprocess.CompletedProcess[str]`` gets a
   ``kubectl --context <ctx> get namespace <ns> --ignore-not-found -o name``
   argv; exit 0 with empty stdout means the namespace is gone, non-empty stdout
   means it remains, and any nonzero exit is a failed check, never gone.
"""

from __future__ import annotations

import dataclasses
import io
import json
import os
import subprocess
import sys
import textwrap
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import acceptance
import factory_e2e as fe
import pytest

EXPECTED_PAIRINGS = {
    "github+github",
    "jira_cloud+github",
    "gitlab+gitlab@fixture",
    "gitlab+gitlab@saas",
    "jira_cloud+gitlab",
    "jira_cloud+bitbucket_cloud",
    "jira_cloud+bitbucket_dc",
}
INSTALLED = {
    "github+github",
    "jira_cloud+github",
    "jira_cloud+bitbucket_cloud",
    "jira_cloud+bitbucket_dc",
    "gitlab+gitlab@fixture",
    "gitlab+gitlab@saas",
}
NOT_INSTALLED = sorted(EXPECTED_PAIRINGS - INSTALLED)
NAMESPACE = "test-factory-acceptance"
# The steps every forge-loop entry tears down, whatever its driver adds.
GENERIC_TEARDOWN = {
    "reset fixture repository",
    "restore fixture default branch",
    "delete namespaces",
}


def _forges(key: str) -> tuple[str, str]:
    """(tracker, code host) of an acceptance key; a GitLab key ends in @<target>."""

    tracker, code_host = key.split("@", 1)[0].split("+")
    return tracker, code_host


# --- the pairing table ---------------------------------------------------------------


def test_every_pairing_is_an_entry() -> None:
    assert set(acceptance.PAIRINGS) == EXPECTED_PAIRINGS


@pytest.mark.parametrize("key", sorted(EXPECTED_PAIRINGS))
def test_each_entry_names_its_tracker_code_host_and_install_state(key: str) -> None:
    pairing = acceptance.PAIRINGS[key]
    tracker, code_host = _forges(key)
    assert (pairing.tracker, pairing.code_host) == (tracker, code_host)
    assert pairing.installed is (key in INSTALLED)


@pytest.mark.parametrize("key", sorted(EXPECTED_PAIRINGS))
def test_each_entry_has_steps_with_every_field_set(key: str) -> None:
    pairing = acceptance.PAIRINGS[key]
    assert len(pairing.steps) >= 1
    assert acceptance.unset_fields(pairing) == []
    for step in pairing.steps:
        assert isinstance(step.inputs, Mapping)
        assert isinstance(step.command, list) and step.command
        assert step.deadline_seconds > 0
        for field, _expected in step.persisted_assertions:
            assert isinstance(field, str) and field


def test_github_github_runs_the_github_loop_and_every_command_parses() -> None:
    steps = acceptance.PAIRINGS["github+github"].steps
    assert ["run", "--scenario", "github-loop"] in [step.command[:3] for step in steps]
    for step in steps:
        fe.parse_args(list(step.command))


def _with_step(key: str, **change: Any) -> Any:
    pairing = acceptance.PAIRINGS[key]
    first = dataclasses.replace(pairing.steps[0], **change)
    return dataclasses.replace(pairing, steps=(first, *pairing.steps[1:]))


@pytest.mark.parametrize(
    ("field", "empty"),
    [
        ("name", ""),
        ("inputs", {}),
        ("command", []),
        ("deadline_seconds", None),
        ("remote_assertions", ()),
        ("persisted_assertions", ()),
        ("teardown_checks", ()),
    ],
)
def test_unset_fields_names_each_empty_step_field(field: str, empty: Any) -> None:
    unset = acceptance.unset_fields(_with_step("github+github", **{field: empty}))
    assert len(unset) == 1, unset
    assert field in unset[0]


# --- dry run -------------------------------------------------------------------------


def test_dry_run_of_github_github_lists_every_step_and_passes() -> None:
    out = io.StringIO()
    assert acceptance.dry_run("github+github", out) == 0
    text = out.getvalue()
    assert "not installed" not in text
    assert "installed" in text
    for step in acceptance.PAIRINGS["github+github"].steps:
        assert step.name in text
        assert " ".join(step.command) in text
        assert str(step.deadline_seconds) in text
        for key in step.inputs:
            assert str(key) in text
        for item in step.remote_assertions:
            assert str(item) in text
        for field, _expected in step.persisted_assertions:
            assert field in text
        for item in step.teardown_checks:
            assert str(item) in text


@pytest.mark.parametrize("key", NOT_INSTALLED)
def test_dry_run_of_a_not_installed_pairing_lists_its_steps_and_says_so(key: str) -> None:
    out = io.StringIO()
    assert acceptance.dry_run(key, out) == fe.EXIT_SCENARIO
    text = out.getvalue()
    assert "not installed" in text
    for step in acceptance.PAIRINGS[key].steps:
        assert step.name in text
        assert " ".join(step.command) in text


@pytest.mark.parametrize("key", sorted(EXPECTED_PAIRINGS))
def test_dry_run_lists_the_pre_teardown_assertions_of_every_pairing(key: str) -> None:
    """#4866: every entry's listing carries the usage cost assertion and the
    credential expiry assertion, installed or not, each with its own exit
    status (the not-installed entry still lists its steps)."""

    out = io.StringIO()
    expected = fe.EXIT_SCENARIO if key in NOT_INSTALLED else 0
    assert acceptance.dry_run(key, out) == expected
    text = out.getvalue()
    assert "scenario.usage.cost_complete == true" in text
    assert (
        "scenario.cluster_status.credentials == "
        f"{json.dumps(acceptance.CREDENTIALS_EXPIRY_VALID)}"
    ) in text


def test_dry_run_fails_on_an_unset_step_field(monkeypatch: pytest.MonkeyPatch) -> None:
    broken = _with_step("github+github", deadline_seconds=None)
    monkeypatch.setitem(acceptance.PAIRINGS, "github+github", broken)
    out = io.StringIO()
    code = acceptance.dry_run("github+github", out)
    assert code != 0
    assert code == fe.EXIT_CONFIG
    assert "deadline_seconds" in out.getvalue()


# --- the CLI surface -----------------------------------------------------------------


def test_acceptance_mode_takes_a_known_pairing() -> None:
    args = fe.parse_args(["acceptance", "--pairing", "github+github", "--dry-run"])
    assert (args.mode, args.pairing, args.dry_run) == ("acceptance", "github+github", True)
    args = fe.parse_args(["acceptance", "--pairing", "gitlab+gitlab@saas"])
    assert args.dry_run is False


def test_acceptance_refuses_an_unknown_pairing() -> None:
    with pytest.raises(SystemExit):
        fe.parse_args(["acceptance", "--pairing", "github+gitlab"])
    with pytest.raises(SystemExit):
        fe.parse_args(["acceptance", "--pairing", "gitlab+gitlab"])
    with pytest.raises(SystemExit):
        fe.parse_args(["acceptance"])


def test_main_dry_run_lists_github_github(capsys: pytest.CaptureFixture[str]) -> None:
    assert fe.main(["acceptance", "--pairing", "github+github", "--dry-run"]) == 0
    out = capsys.readouterr().out
    for step in acceptance.PAIRINGS["github+github"].steps:
        assert step.name in out


def _forbid_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a not-installed pairing must launch nothing")

    monkeypatch.setattr(subprocess, "run", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)
    monkeypatch.setattr(fe, "Preflight", boom)
    monkeypatch.setattr(fe, "load_config", boom)


@pytest.mark.parametrize("key", NOT_INSTALLED)
def test_main_real_run_of_a_not_installed_pairing_launches_nothing(
    key: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _forbid_launch(monkeypatch)
    assert fe.main(["acceptance", "--pairing", key]) == fe.EXIT_SCENARIO
    assert "not installed" in capsys.readouterr().err


@pytest.mark.parametrize("key", NOT_INSTALLED)
def test_main_dry_run_of_a_not_installed_pairing_reports_it(
    key: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _forbid_launch(monkeypatch)
    assert fe.main(["acceptance", "--pairing", key, "--dry-run"]) == fe.EXIT_SCENARIO
    assert "not installed" in capsys.readouterr().out


# --- real run of github+github -------------------------------------------------------


def _set_path(evidence: dict[str, Any], dotted: str, value: Any) -> None:
    node = evidence
    *parents, leaf = dotted.split(".")
    for part in parents:
        node = node.setdefault(part, {})
    node[leaf] = value


def _passing_evidence() -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for step in acceptance.PAIRINGS["github+github"].steps:
        for field, expected in step.persisted_assertions:
            _set_path(evidence, field, expected)
    # The credentials assertion (#4866) describes its predicate, so the
    # derived value above is its text; the passing record carries real rows.
    _set_path(
        evidence,
        "scenario.cluster_status.credentials",
        [
            {
                "section": "code_host",
                "name": "actor",
                "type": "pat",
                "authentication": "accepted",
                "expiry": "known",
                "expires_at": None,
            }
        ],
    )
    evidence.update({"result": "passed", "teardown_clean": True, "namespace": NAMESPACE})
    return evidence


def _contains(argv: list[str], command: list[str]) -> bool:
    n = len(command)
    return any(argv[i : i + n] == command for i in range(len(argv) - n + 1))


class _Runner:
    def __init__(self, evidence: dict[str, Any] | None, *, code: int = 0) -> None:
        self.evidence = evidence
        self.code = code
        self.calls: list[tuple[list[str], float]] = []

    def __call__(self, argv: list[str], timeout: float) -> int:
        self.calls.append((list(argv), timeout))
        if self.evidence is not None:
            path = Path(argv[argv.index("--evidence") + 1])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(self.evidence))
        return self.code


class _Kubectl:
    def __init__(self, *, gone: bool = True) -> None:
        self.gone = gone
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        assert argv[-3:] == ["--ignore-not-found", "-o", "name"], argv
        if self.gone:
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 0, f"namespace/{NAMESPACE}\n", "")


class _KubectlError:
    """A kubectl that fails for a reason unrelated to the namespace."""

    def __init__(self, stderr: str) -> None:
        self.stderr = stderr
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 1, "", self.stderr)


def _args(tmp_path: Path, pairing: str = "github+github") -> Any:
    issue = tmp_path / "issue.md"
    issue.write_text("Title\n\nBody\n")
    revision = tmp_path / "revision.md"
    revision.write_text("Review text\n")
    return fe.parse_args(
        [
            "acceptance",
            "--pairing",
            pairing,
            "--context",
            "k8",
            "--candidate",
            "refs/pull/1/head",
            "--candidate-images",
            "build",
            "--kind-cluster",
            "dev",
            "--namespace",
            NAMESPACE,
            "--issue-file",
            str(issue),
            "--revision-file",
            str(revision),
            "--admission-timeout",
            "45",
            "--evidence",
            str(tmp_path / "summary.json"),
        ]
    )


def test_real_run_passes_shared_flags_and_judges_each_step(tmp_path: Path) -> None:
    args = _args(tmp_path)
    runner = _Runner(_passing_evidence())
    kubectl = _Kubectl()
    assert acceptance.run(args, runner=runner, kubectl=kubectl) == 0

    steps = acceptance.PAIRINGS["github+github"].steps
    assert len(runner.calls) == len(steps)
    step_evidence: list[str] = []
    for step, (argv, timeout) in zip(steps, runner.calls, strict=True):
        assert _contains(argv, list(step.command)), argv
        assert timeout == step.deadline_seconds
        for flag, value in (
            ("--context", "k8"),
            ("--candidate", "refs/pull/1/head"),
            ("--candidate-images", "build"),
            ("--kind-cluster", "dev"),
            ("--namespace", NAMESPACE),
            ("--issue-file", str(tmp_path / "issue.md")),
            ("--revision-file", str(tmp_path / "revision.md")),
        ):
            assert argv[argv.index(flag) + 1] == value, flag
        assert float(argv[argv.index("--admission-timeout") + 1]) == 45
        step_evidence.append(argv[argv.index("--evidence") + 1])
    assert str(tmp_path / "summary.json") not in step_evidence
    assert len(set(step_evidence)) == len(step_evidence)

    assert kubectl.calls, "the namespace teardown check never ran"
    for argv in kubectl.calls:
        assert argv[argv.index("--context") + 1] == "k8"
        assert NAMESPACE in argv

    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["schema"] == "curie.factory-e2e.acceptance/v1"
    assert summary["result"] == "passed"
    assert summary["pairing"] == "github+github"


def _fail_one_persisted(evidence: dict[str, Any]) -> dict[str, Any]:
    step = next(s for s in acceptance.PAIRINGS["github+github"].steps if s.persisted_assertions)
    field, expected = step.persisted_assertions[0]
    _set_path(evidence, field, {"not": expected})
    return evidence


@pytest.mark.parametrize(
    ("label", "runner", "kubectl"),
    [
        ("failed result", lambda: _Runner({**_passing_evidence(), "result": "failed"}), _Kubectl),
        (
            "teardown not clean",
            lambda: _Runner({**_passing_evidence(), "teardown_clean": False}),
            _Kubectl,
        ),
        (
            "persisted assertion",
            lambda: _Runner(_fail_one_persisted(_passing_evidence())),
            _Kubectl,
        ),
        ("namespace remains", lambda: _Runner(_passing_evidence()), lambda: _Kubectl(gone=False)),
        ("no evidence", lambda: _Runner(None, code=fe.EXIT_FAILED), _Kubectl),
    ],
)
def test_real_run_fails_on_each_shortfall(
    label: str, runner: Any, kubectl: Any, tmp_path: Path
) -> None:
    args = _args(tmp_path)
    assert acceptance.run(args, runner=runner(), kubectl=kubectl()) != 0, label
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["schema"] == "curie.factory-e2e.acceptance/v1"
    assert summary["result"] == "failed", label


# --- the credential expiry predicate (#4866) ------------------------------------------


@pytest.mark.parametrize("expiry", acceptance.CREDENTIALS_EXPIRY_LABELS)
def test_the_credentials_assertion_accepts_each_classified_expiry(expiry: str) -> None:
    rows = [{"section": "code_host", "name": "actor", "type": "pat", "expiry": expiry}]
    assert (
        acceptance._credential_expiry_failures("scenario.cluster_status.credentials", rows) == []
    )


def test_the_credentials_assertion_accepts_a_release_with_no_credential() -> None:
    assert acceptance._credential_expiry_failures("scenario.cluster_status.credentials", []) == []


@pytest.mark.parametrize(
    ("rows", "match"),
    [
        ([{"expiry": "soon"}], "expiry one of"),
        (["actor token"], "expiry one of"),
        ({}, "credential rows"),
    ],
    ids=["unclassified-expiry", "row-not-an-object", "not-a-list"],
)
def test_the_credentials_assertion_rejects_each_shortfall(rows: Any, match: str) -> None:
    failures = acceptance._credential_expiry_failures("scenario.cluster_status.credentials", rows)
    assert len(failures) == 1
    assert "scenario.cluster_status.credentials" in failures[0]
    assert match in failures[0]


def test_the_credentials_assertion_rejects_a_missing_path() -> None:
    failures = acceptance._credential_expiry_failures(
        "scenario.cluster_status.credentials", acceptance._MISSING
    )
    assert len(failures) == 1
    assert "is missing" in failures[0]


def test_a_real_run_fails_on_an_unclassified_credential_expiry(tmp_path: Path) -> None:
    evidence = _passing_evidence()
    _set_path(evidence, "scenario.cluster_status.credentials", [{"expiry": "soon"}])
    args = _args(tmp_path)
    assert acceptance.run(args, runner=_Runner(evidence), kubectl=_Kubectl()) != 0
    summary = json.loads((tmp_path / "summary.json").read_text())
    failures = summary["steps"][0]["failures"]
    assert any("scenario.cluster_status.credentials" in f for f in failures), failures


@pytest.mark.parametrize(
    "stderr",
    [
        'error: context "k8" does not exist',
        "Unable to connect to the server: dial tcp: lookup k8: no such host",
        # An unrelated NotFound must not read as the namespace being gone.
        "Error from server (NotFound): the server could not find the requested resource",
    ],
    ids=["unknown-context", "unreachable", "unrelated-notfound"],
)
def test_a_kubectl_error_is_a_failed_namespace_check_not_gone(stderr: str, tmp_path: Path) -> None:
    args = _args(tmp_path)
    kubectl = _KubectlError(stderr)
    assert acceptance.run(args, runner=_Runner(_passing_evidence()), kubectl=kubectl) != 0
    assert kubectl.calls
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["result"] == "failed"
    failures = summary["steps"][0]["failures"]
    assert any("could not read namespace" in f for f in failures), failures


def test_real_run_fails_a_step_that_overruns_its_deadline(tmp_path: Path) -> None:
    def runner(argv: list[str], timeout: float) -> int:
        raise subprocess.TimeoutExpired(argv, timeout)

    args = _args(tmp_path)
    assert acceptance.run(args, runner=runner, kubectl=_Kubectl()) != 0
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["result"] == "failed"


@pytest.mark.parametrize("key", NOT_INSTALLED)
def test_real_run_of_a_not_installed_pairing_runs_no_step(
    key: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    args = fe.parse_args(["acceptance", "--pairing", key, "--context", "k8"])
    runner = _Runner(_passing_evidence())
    kubectl = _Kubectl()
    assert acceptance.run(args, runner=runner, kubectl=kubectl) == fe.EXIT_SCENARIO
    assert runner.calls == [] and kubectl.calls == []
    assert "not installed" in capsys.readouterr().err


# --- independently built evidence ------------------------------------------------------


def _github_loop_evidence() -> dict[str, Any]:
    """Evidence as main(), run_loop and _finish write it for a passing github-loop.

    Written out literally, not derived from the persisted assertions, so an
    assertion whose dotted path names a field the harness never writes fails.
    """

    return {
        "schema": "curie.factory-e2e.evidence/v1",
        "mode": "run:github-loop",
        "candidate_commit": "c" * 40,
        "image_tag": "sha-" + "c" * 40,
        "candidate_image_mode": "published",
        "kube_context": "k8",
        "namespace": NAMESPACE,
        "release": "curie",
        "run_id": "0123456789ab",
        "started_at": "2026-10-11T00:00:00+00:00",
        "steps": [],
        "forge": "github",
        "execution_request_id": "6f1c3a52-0000-4000-8000-000000000002",
        "work_item_id": "6f1c3a52-0000-4000-8000-000000000001",
        "scenario": {
            "work_item_id": "6f1c3a52-0000-4000-8000-000000000001",
            "request_id": "6f1c3a52-0000-4000-8000-000000000002",
            "delivery_id": None,
            "issue_number": 41,
            "cli": [],
            "cli_failures": [],
            "forge": "github",
            "pairing": "github+github",
            "tracker": {},
            "change_number": 42,
            "change_url": "https://github.com/acme/fixture/pull/42",
            "head_initial": "a" * 40,
            "commits_initial": 1,
            "red_ci": {"sha": "a" * 40, "context": "factory-e2e/loop-gate", "state": "failure"},
            "first_run_terminal": True,
            "first_run_statuses": ["completed"],
            "first_run_cause": None,
            "head_after_ci": "b" * 40,
            "commits_after_ci": 2,
            "ci_on_initial_head": {"sha": "a" * 40, "state": "failing"},
            "ci_after_fix": {
                "sha": "b" * 40,
                "state": "passing",
                "check_runs": [{"name": "test", "status": "completed", "conclusion": "success"}],
                "statuses": [],
            },
            "review_id": 9,
            "review_url": "https://github.com/acme/fixture/pull/42#pullrequestreview-9",
            "review_delivery_id": "delivery-guid",
            "review_delivery_status_code": 200,
            "review_delivery_api_status": "factory_admitted",
            "review_terminal": True,
            "request_statuses": ["completed", "completed"],
            "head_after_review": "d" * 40,
            "commits_after_review": 3,
            "work_item_change_number": 42,
            "default_branch_moved_before_merge": False,
            "merge": {"status_code": 200, "merged": True, "method": "merge", "sha": "e" * 40},
            "forge_merged": True,
            "work_item_pr_status": "merged",
            "cli_pr_status": "merged",
            "change_numbers": [42],
            # The reads run_loop records before teardown (#4866).
            "usage": {
                "work_item_id": "6f1c3a52-0000-4000-8000-000000000001",
                "total_tokens": 1000,
                "estimated_cost_usd": 0.42,
                "cost_complete": True,
                "requests_without_usage": 0,
            },
            "cluster_status": {
                "credentials": [
                    {
                        "section": "code_host",
                        "name": "actor",
                        "type": "pat",
                        "authentication": "accepted",
                        "expiry": "known",
                        "expires_at": "2026-10-23T00:00:00Z",
                    }
                ]
            },
            "work_items": [
                {
                    "tracker": {
                        "kind": "github",
                        "host": "https://github.com",
                        "scope_id": "acme/fixture",
                        "issue_id": "41",
                        "display_key": "acme/fixture#41",
                        "url": "https://github.com/acme/fixture/issues/41",
                    }
                }
            ],
            "verdict": "passed",
            "failures": [],
        },
        "result": "passed",
        "teardown": [
            {"step": name, "ok": True, "detail": {}}
            for name in (
                "reset fixture repository",
                "restore App webhook",
                "stop tunnel",
                "stop api port-forward",
                "restore fixture default branch",
                "delete namespaces",
            )
        ],
        "teardown_clean": True,
        "teardown_ok": {
            "reset fixture repository": True,
            "restore App webhook": True,
            "stop tunnel": True,
            "stop api port-forward": True,
            "restore fixture default branch": True,
            "delete namespaces": True,
        },
    }


def test_independently_built_github_loop_evidence_satisfies_the_persisted_assertions() -> None:
    evidence = _github_loop_evidence()
    for step in acceptance.PAIRINGS["github+github"].steps:
        for field, expected in step.persisted_assertions:
            got = acceptance._get_path(evidence, field)
            assert got is not acceptance._MISSING, f"{field} is not in the harness evidence"
            if expected == acceptance.CREDENTIALS_EXPIRY_VALID:
                assert acceptance._credential_expiry_failures(field, got) == [], (field, got)
            else:
                assert got == expected, (field, got, expected)


def test_real_run_passes_on_independently_built_evidence(tmp_path: Path) -> None:
    args = _args(tmp_path)
    assert acceptance.run(args, runner=_Runner(_github_loop_evidence()), kubectl=_Kubectl()) == 0


def test_the_github_loop_judge_passes_the_independent_scenario() -> None:
    # The literal scenario block is a passing loop by the harness's own judge.
    assert fe.judge_github_loop(_github_loop_evidence()["scenario"]) == []


# --- the default runner against a real child process -----------------------------------

_CHILD = textwrap.dedent(
    """
    import os, signal, sys, time
    mode, marker, sleep = sys.argv[1], sys.argv[2], float(sys.argv[3])
    with open(marker + ".pid", "w") as handle:
        handle.write(str(os.getpid()))
    def on_term(_signum, _frame):
        with open(marker + ".term", "w") as handle:
            handle.write("term")
        if mode == "graceful":
            sys.exit(7)
    if mode in ("graceful", "stubborn"):
        # graceful exits from the handler; stubborn records it and sleeps on.
        signal.signal(signal.SIGTERM, on_term)
    time.sleep(sleep)
    sys.exit(3)
    """
)


def _child(tmp_path: Path, mode: str, sleep: float) -> tuple[list[str], Path]:
    script = tmp_path / "child.py"
    script.write_text(_CHILD)
    marker = tmp_path / mode
    return [sys.executable, "-I", str(script), mode, str(marker), str(sleep)], marker


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_default_runner_returns_the_exit_code_of_a_step_that_finishes(tmp_path: Path) -> None:
    argv, marker = _child(tmp_path, "plain", 0)
    assert acceptance.default_runner(argv, 10, grace=5) == 3
    assert not Path(f"{marker}.term").exists()


def test_default_runner_terminates_an_overrun_step_and_lets_it_tear_down(
    tmp_path: Path,
) -> None:
    argv, marker = _child(tmp_path, "graceful", 30)
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        acceptance.default_runner(argv, 0.5, grace=10)
    elapsed = time.monotonic() - started
    assert Path(f"{marker}.term").read_text() == "term"
    # It exited on SIGTERM within the grace, so no wait for the whole grace.
    assert elapsed < 8, elapsed
    assert not _alive(int(Path(f"{marker}.pid").read_text()))


def test_default_runner_kills_a_step_that_ignores_sigterm_after_the_grace(
    tmp_path: Path,
) -> None:
    argv, marker = _child(tmp_path, "stubborn", 30)
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        acceptance.default_runner(argv, 0.5, grace=1)
    elapsed = time.monotonic() - started
    assert Path(f"{marker}.term").read_text() == "term"
    assert 1.4 <= elapsed < 10, elapsed
    assert not _alive(int(Path(f"{marker}.pid").read_text()))


def test_an_overrun_step_under_the_default_runner_fails_with_its_deadline(
    tmp_path: Path,
) -> None:
    child, marker = _child(tmp_path, "graceful", 30)

    def runner(_argv: list[str], _timeout: float) -> int:
        return acceptance.default_runner(child, 0.5, grace=10)

    args = _args(tmp_path)
    assert acceptance.run(args, runner=runner, kubectl=_Kubectl()) != 0
    summary = json.loads((tmp_path / "summary.json").read_text())
    step = summary["steps"][0]
    assert step["exit_code"] is None
    assert any("overran its" in f and "deadline" in f for f in step["failures"]), step
    assert Path(f"{marker}.term").exists()


# --- the jira_cloud+github entry (#4358) -------------------------------------------------

JIRA = "jira_cloud+github"
CLOSE_JIRA = "close Jira test issues"
TRACKER_ASSERTIONS = {
    "scenario.tracker.status_comments": 1,
    "scenario.tracker.remote_links": 1,
    "scenario.tracker.pr_body_has_key": True,
    "scenario.tracker.bot_label_admitted": False,
}


def _persisted_map(key: str) -> dict[str, Any]:
    (step,) = acceptance.PAIRINGS[key].steps
    return dict(step.persisted_assertions)


def _teardown_names(key: str) -> set[str]:
    return {
        field.removeprefix("teardown_ok.")
        for field in _persisted_map(key)
        if field.startswith("teardown_ok.")
    }


def test_jira_entry_persisted_forge_matches_driver_key() -> None:
    (step,) = acceptance.PAIRINGS[JIRA].steps
    assert step.command == ["run", "--scenario", "forge-loop", "--forge", "jira_cloud"]
    fe.parse_args(list(step.command))
    persisted = _persisted_map(JIRA)
    assert persisted["mode"] == "run:forge-loop"
    assert persisted["forge"] == "jira_cloud"
    assert persisted["scenario.forge"] == "jira_cloud"
    assert persisted["scenario.pairing"] == JIRA
    for field, expected in TRACKER_ASSERTIONS.items():
        assert persisted[field] == expected, field
    # Red then green CI and the review revision are still asserted.
    assert persisted["scenario.red_ci.state"] == "failure"
    assert persisted["scenario.ci_after_fix.state"] == "passing"
    assert persisted["scenario.review_delivery_api_status"] == "factory_admitted"

    github = _persisted_map("github+github")
    assert github["forge"] == "github"
    assert github["scenario.forge"] == "github"
    assert github["scenario.pairing"] == "github+github"
    assert not [f for f in github if f.startswith("scenario.tracker.")]


@pytest.mark.parametrize("key", sorted(EXPECTED_PAIRINGS))
def test_every_entry_asserts_its_pairing_and_a_jira_tracker_its_tracker_evidence(
    key: str,
) -> None:
    persisted = _persisted_map(key)
    tracker, code_host = _forges(key)
    assert persisted["scenario.pairing"] == key
    if tracker == "jira_cloud" and code_host.startswith("bitbucket"):
        # The Bitbucket driver delegates the ticket and its teardown to the
        # Jira driver and asserts the loop only.
        assert persisted["forge"] == persisted["scenario.forge"] == code_host
        assert not [f for f in persisted if f.startswith("scenario.tracker.")]
        assert CLOSE_JIRA in _teardown_names(key)
    elif tracker == "jira_cloud":
        assert persisted["forge"] == persisted["scenario.forge"] == "jira_cloud"
        for field, expected in TRACKER_ASSERTIONS.items():
            assert persisted[field] == expected, field
        assert CLOSE_JIRA in _teardown_names(key)
    elif tracker == "gitlab":
        # GitLab is its own tracker; its evidence is pinned in test_gitlab_acceptance.py.
        assert persisted["forge"] == persisted["scenario.forge"] == "gitlab"
        assert persisted["scenario.tracker.closes_issue"] is True
        assert CLOSE_JIRA not in _teardown_names(key)
    else:
        assert persisted["forge"] == persisted["scenario.forge"] == code_host
        assert not [f for f in persisted if f.startswith("scenario.tracker.")]
        assert CLOSE_JIRA not in _teardown_names(key)


def test_jira_entry_teardown_names_match_the_driver_teardowns() -> None:
    # The Jira entry is the generic forge-loop teardown plus exactly the step
    # the Jira driver's teardowns() pushes (pinned on the driver side by
    # test_jira_driver.py::test_teardowns_is_exactly_the_close_step and
    # test_close_jira_test_issues_is_a_separate_teardown_step).
    names = _teardown_names(JIRA)
    assert names - GENERIC_TEARDOWN == {CLOSE_JIRA}
    assert GENERIC_TEARDOWN <= names
    assert "restore App webhook" not in names


def test_jira_entry_inputs_name_the_jira_env_file() -> None:
    (step,) = acceptance.PAIRINGS[JIRA].steps
    assert "CURIE_FACTORY_JIRA_ENV_FILE" in step.inputs["credentials"]
    (github,) = acceptance.PAIRINGS["github+github"].steps
    assert "CURIE_FACTORY_JIRA_ENV_FILE" not in github.inputs["credentials"]


def test_dry_run_of_jira_cloud_github_lists_every_step_and_passes() -> None:
    out = io.StringIO()
    assert acceptance.dry_run(JIRA, out) == 0
    text = out.getvalue()
    assert "not installed" not in text
    assert "unset" not in text
    for step in acceptance.PAIRINGS[JIRA].steps:
        assert step.name in text
        assert " ".join(step.command) in text
        for field, _expected in step.persisted_assertions:
            assert field in text


def _jira_loop_evidence() -> dict[str, Any]:
    """Evidence as main(), run_loop and _finish write it for a passing Jira loop.

    Written out from the GitHub literal with the Jira differences spelled out,
    not derived from the persisted assertions.
    """

    evidence = json.loads(json.dumps(_github_loop_evidence()))
    evidence["mode"] = "run:forge-loop"
    evidence["forge"] = "jira_cloud"
    evidence["issue_number"] = 0
    scenario = evidence["scenario"]
    scenario.update(
        forge="jira_cloud",
        pairing="jira_cloud+github",
        issue_number=0,
        tracker={
            "key": "FAC-42",
            "status_comments": 1,
            "remote_links": 1,
            "pr_body_has_key": True,
            "bot_label_admitted": False,
        },
    )
    names = (
        "close Jira test issues",
        "reset fixture repository",
        "stop tunnel",
        "stop api port-forward",
        "restore fixture default branch",
        "delete namespaces",
    )
    evidence["teardown"] = [{"step": name, "ok": True, "detail": {}} for name in names]
    evidence["teardown_ok"] = dict.fromkeys(names, True)
    evidence["teardown_clean"] = True
    return evidence


def test_jira_entry_passes_on_a_complete_evidence_record(tmp_path: Path) -> None:
    evidence = _jira_loop_evidence()
    for field, expected in _persisted_map(JIRA).items():
        got = acceptance._get_path(evidence, field)
        assert got is not acceptance._MISSING, f"{field} is not in the harness evidence"
        if expected == acceptance.CREDENTIALS_EXPIRY_VALID:
            assert acceptance._credential_expiry_failures(field, got) == [], (field, got)
        else:
            assert got == expected, (field, got, expected)
    args = _args(tmp_path, JIRA)
    assert acceptance.run(args, runner=_Runner(evidence), kubectl=_Kubectl()) == 0
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["pairing"] == JIRA and summary["result"] == "passed"


def _drop(evidence: dict[str, Any], dotted: str) -> None:
    node = evidence
    *parents, leaf = dotted.split(".")
    for part in parents:
        node = node[part]
    del node[leaf]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("scenario.tracker.status_comments", 0),
        ("scenario.tracker.status_comments", 2),
        ("scenario.tracker.remote_links", 0),
        ("scenario.tracker.pr_body_has_key", False),
        ("scenario.tracker.bot_label_admitted", True),
        ("scenario.pairing", "github+github"),
        ("scenario.forge", "github"),
        ("forge", "github"),
        ("scenario.red_ci.state", "success"),
        ("scenario.ci_after_fix.state", "failing"),
        ("scenario.review_delivery_api_status", "factory_ignored"),
        ("teardown_ok.close Jira test issues", False),
        ("scenario.tracker.status_comments", None),
        ("scenario.tracker.remote_links", None),
        ("scenario.tracker.pr_body_has_key", None),
        ("scenario.tracker.bot_label_admitted", None),
        ("scenario.pairing", None),
        ("teardown_ok.close Jira test issues", None),
    ],
)
def test_jira_entry_fails_when_an_observation_is_missing_or_wrong(
    field: str, value: Any, tmp_path: Path
) -> None:
    evidence = _jira_loop_evidence()
    if value is None:
        _drop(evidence, field)
    else:
        _set_path(evidence, field, value)
    args = _args(tmp_path, JIRA)
    assert acceptance.run(args, runner=_Runner(evidence), kubectl=_Kubectl()) != 0
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["result"] == "failed"
    failures = summary["steps"][0]["failures"]
    assert any(field in failure for failure in failures), failures
