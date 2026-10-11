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
   ``kubectl --context <ctx> get namespace <ns>`` style argv; a nonzero exit
   whose stderr says ``NotFound`` means the namespace is gone.
"""

from __future__ import annotations

import dataclasses
import io
import json
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import acceptance
import factory_e2e as fe
import pytest

EXPECTED_PAIRINGS = {
    "github+github",
    "jira_cloud+github",
    "gitlab+gitlab",
    "jira_cloud+gitlab",
    "bitbucket_cloud+bitbucket_cloud",
    "jira_cloud+bitbucket_cloud",
    "bitbucket_dc+bitbucket_dc",
}
INSTALLED = {"github+github"}
NOT_INSTALLED = sorted(EXPECTED_PAIRINGS - INSTALLED)
STEP_FIELDS = (
    "name",
    "inputs",
    "command",
    "deadline_seconds",
    "remote_assertions",
    "persisted_assertions",
    "teardown_checks",
)
NAMESPACE = "test-factory-acceptance"


# --- the pairing table ---------------------------------------------------------------


def test_every_pairing_is_an_entry() -> None:
    assert set(acceptance.PAIRINGS) == EXPECTED_PAIRINGS


@pytest.mark.parametrize("key", sorted(EXPECTED_PAIRINGS))
def test_each_entry_names_its_tracker_code_host_and_install_state(key: str) -> None:
    pairing = acceptance.PAIRINGS[key]
    tracker, code_host = key.split("+")
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
    args = fe.parse_args(["acceptance", "--pairing", "gitlab+gitlab"])
    assert args.dry_run is False


def test_acceptance_refuses_an_unknown_pairing() -> None:
    with pytest.raises(SystemExit):
        fe.parse_args(["acceptance", "--pairing", "github+gitlab"])
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
        if self.gone:
            return subprocess.CompletedProcess(
                argv, 1, "", f'Error from server (NotFound): namespaces "{NAMESPACE}" not found'
            )
        return subprocess.CompletedProcess(argv, 0, f"namespace/{NAMESPACE}\n", "")


def _args(tmp_path: Path) -> Any:
    issue = tmp_path / "issue.md"
    issue.write_text("Title\n\nBody\n")
    revision = tmp_path / "revision.md"
    revision.write_text("Review text\n")
    return fe.parse_args(
        [
            "acceptance",
            "--pairing",
            "github+github",
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
        ("persisted assertion", lambda: _Runner(_fail_one_persisted(_passing_evidence())), _Kubectl),
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
