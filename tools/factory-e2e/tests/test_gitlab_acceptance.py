"""Unit contract for the GitLab acceptance entries (tools/factory-e2e/acceptance.py, #4399).

Decision 27 and ACs 11 and 12: the table registers exactly
``gitlab+gitlab@fixture`` and ``gitlab+gitlab@saas`` in place of the
not-installed ``gitlab+gitlab`` entry; every other key is unchanged. The live
fixture run needs a GitLab CE fixture, a cluster and a real model, so it is
proved by running it; these pin the table, the dry run, the saas fail-fast and
how the fixture teardown is judged.

The production contract the fakes below rely on:

1. ``Pairing`` gains ``target: str | None`` (None for every other entry) and
   its ``key`` is ``<tracker>+<code host>@<target>`` when a target is set.
2. Each GitLab entry has one step whose command is ``["run", "--scenario",
   "forge-loop", "--forge", "gitlab", "--forge-target", <target>]``, under the
   shared deadline. Its persisted assertions are the forge-loop ones with
   ``forge == scenario.forge == "gitlab"`` and ``scenario.pairing == <key>``,
   plus ``scenario.tracker.closes_issue == True``,
   ``scenario.tracker.thread_replies == 1`` and
   ``scenario.tracker.status_note_links_mr == True`` (#4867); ``@fixture``
   adds ``scenario.tracker.reporter_label_admitted == False`` and
   ``scenario.tracker.reporter_state_labels == []``.
3. ``@fixture`` teardown checks are ``TEARDOWN_CLEAN``, ``NAMESPACE_GONE`` and
   ``acceptance.FIXTURE_GONE``; ``FIXTURE_GONE`` passes only when the evidence
   names ``gitlab_fixture.state_dir`` and that directory no longer exists
   (`forge-fixture gitlab down` deletes it only once nothing labelled with the
   compose project remains). ``@saas`` has no fixture check.
4. ``@fixture`` inputs name ``--advertise-host``; its remote assertions name the
   ``reporter`` label that admits nothing, the ``Closes #<iid>`` merge request,
   the one thread reply and the merge as ``root``. Both entries also name the
   ``curie-factory:running`` in-progress label within one poll interval of the
   labeler's label (``scenario.tracker.in_progress_label_seconds``) and the one
   factory status note that links the merge request (#4867). ``@saas`` inputs
   name each of ``CURIE_FACTORY_GITLAB_PROJECT``,
   ``CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE`` and
   ``CURIE_FACTORY_GITLAB_HUMAN_TOKEN_FILE``, and nothing names a reporter.
5. ``acceptance.run`` on ``@saas`` with any of those variables unset returns
   ``fe.EXIT_CONFIG`` with the first unset name on stderr, before any step
   runs or any request is sent. ``--dry-run`` needs none of them.
"""

from __future__ import annotations

import io
import json
import subprocess
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import acceptance
import factory_e2e as fe
import pytest

FIXTURE = "gitlab+gitlab@fixture"
SAAS = "gitlab+gitlab@saas"
GITLAB_KEYS = (FIXTURE, SAAS)
TARGETS = {FIXTURE: "fixture", SAAS: "saas"}
EXPECTED_PAIRINGS = {
    "github+github",
    "jira_cloud+github",
    FIXTURE,
    SAAS,
    "jira_cloud+gitlab",
    "jira_cloud+bitbucket_cloud",
    "jira_cloud+bitbucket_dc",
}
SAAS_VARS = (
    "CURIE_FACTORY_GITLAB_PROJECT",
    "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE",
    "CURIE_FACTORY_GITLAB_HUMAN_TOKEN_FILE",
)
GENERIC_TEARDOWN = {
    "reset fixture repository",
    "restore fixture default branch",
    "delete namespaces",
}
NAMESPACE = "test-factory-acceptance"


def _step(key: str) -> acceptance.Step:
    (step,) = acceptance.PAIRINGS[key].steps
    return step


def _persisted(key: str) -> dict[str, Any]:
    return dict(_step(key).persisted_assertions)


def _teardown_names(key: str) -> set[str]:
    return {
        field.removeprefix("teardown_ok.")
        for field in _persisted(key)
        if field.startswith("teardown_ok.")
    }


@pytest.fixture
def _no_saas_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in SAAS_VARS:
        monkeypatch.delenv(name, raising=False)


def _forbid_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("the saas run must launch nothing and send no request")

    monkeypatch.setattr(subprocess, "run", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)
    monkeypatch.setattr(fe, "Preflight", boom)
    monkeypatch.setattr(fe, "load_config", boom)
    monkeypatch.setattr(urllib.request, "urlopen", boom)
    monkeypatch.setattr(urllib.request.OpenerDirector, "open", boom)


# --- the table ---------------------------------------------------------------------------


def test_the_table_replaces_gitlab_gitlab_with_the_two_targets() -> None:
    assert set(acceptance.PAIRINGS) == EXPECTED_PAIRINGS
    assert "gitlab+gitlab" not in acceptance.PAIRINGS


@pytest.mark.parametrize("key", GITLAB_KEYS)
def test_each_gitlab_entry_is_installed_for_its_target(key: str) -> None:
    pairing = acceptance.PAIRINGS[key]
    assert (pairing.tracker, pairing.code_host) == ("gitlab", "gitlab")
    assert pairing.target == TARGETS[key]
    assert pairing.key == key
    assert pairing.installed is True
    assert acceptance.unset_fields(pairing) == []


@pytest.mark.parametrize("key", sorted(EXPECTED_PAIRINGS - set(GITLAB_KEYS)))
def test_every_other_entry_has_no_target_and_keeps_its_key(key: str) -> None:
    pairing = acceptance.PAIRINGS[key]
    assert pairing.target is None
    assert pairing.key == f"{pairing.tracker}+{pairing.code_host}" == key


@pytest.mark.parametrize("key", GITLAB_KEYS)
def test_each_gitlab_entry_runs_forge_loop_on_the_gitlab_forge_for_its_target(key: str) -> None:
    step = _step(key)
    target = TARGETS[key]
    assert step.command == [
        "run",
        "--scenario",
        "forge-loop",
        "--forge",
        "gitlab",
        "--forge-target",
        target,
    ]
    fe.load_forge_entries()
    args = fe.parse_args(list(step.command))
    assert (args.forge, args.forge_target) == ("gitlab", target)
    assert step.deadline_seconds == acceptance.LOOP_DEADLINE_SECONDS


@pytest.mark.parametrize("key", GITLAB_KEYS)
def test_each_gitlab_entry_asserts_the_loop_on_the_gitlab_forge(key: str) -> None:
    persisted = _persisted(key)
    for field, expected in {
        "result": "passed",
        "mode": "run:forge-loop",
        "forge": "gitlab",
        "scenario.forge": "gitlab",
        "scenario.pairing": key,
        "scenario.verdict": "passed",
        "scenario.red_ci.state": "failure",
        "scenario.ci_after_fix.state": "passing",
        "scenario.review_delivery_api_status": "factory_admitted",
        "scenario.merge.merged": True,
        "scenario.forge_merged": True,
        "scenario.work_item_pr_status": "merged",
        "scenario.cli_pr_status": "merged",
        "scenario.tracker.closes_issue": True,
        "scenario.tracker.thread_replies": 1,
        "teardown_clean": True,
    }.items():
        assert field in persisted, field
        assert persisted[field] == expected, field
    names = _teardown_names(key)
    assert GENERIC_TEARDOWN <= names
    assert acceptance.CLOSE_JIRA_STEP not in names
    assert "restore App webhook" not in names


def test_the_fixture_entry_asserts_the_reporter_label_admits_nothing() -> None:
    persisted = _persisted(FIXTURE)
    assert persisted["scenario.tracker.reporter_label_admitted"] is False
    assert persisted["scenario.tracker.reporter_state_labels"] == []


def test_the_saas_entry_has_no_reporter_step() -> None:
    persisted = _persisted(SAAS)
    assert not [field for field in persisted if "reporter" in field]
    step = _step(SAAS)
    text = " ".join([*step.remote_assertions, *step.inputs.values()]).lower()
    assert "reporter" not in text


@pytest.mark.parametrize("key", GITLAB_KEYS)
def test_each_gitlab_entry_asserts_the_in_progress_label_and_the_status_note(key: str) -> None:
    """#4867: both GitLab entries assert the in-progress state label within one
    poll interval of the labeler's label (``scenario.tracker.
    in_progress_label_seconds``, a remote assertion the driver bounds) and the
    status note linking the merge request (``scenario.tracker.
    status_note_links_mr``, persisted true)."""

    persisted = _persisted(key)
    assert persisted["scenario.tracker.status_note_links_mr"] is True
    assert "scenario.tracker.in_progress_label_seconds" not in persisted
    remote = [item.lower() for item in _step(key).remote_assertions]
    assert any(
        "in-progress label" in item and "curie-factory:running" in item for item in remote
    ), remote
    assert any("status note" in item and "merge request" in item for item in remote), remote


def test_the_fixture_entry_checks_the_namespace_and_the_fixture_are_gone() -> None:
    assert _step(FIXTURE).teardown_checks == (
        acceptance.TEARDOWN_CLEAN,
        acceptance.NAMESPACE_GONE,
        acceptance.FIXTURE_GONE,
    )
    assert _step(SAAS).teardown_checks == (acceptance.TEARDOWN_CLEAN, acceptance.NAMESPACE_GONE)


def test_the_fixture_entry_names_every_step_of_decision_27() -> None:
    step = _step(FIXTURE)
    inputs = " ".join(step.inputs.values())
    assert "--advertise-host" in inputs
    remote = [item.lower() for item in step.remote_assertions]
    assert any("reporter" in item and "admit" in item for item in remote), remote
    assert any("closes #<iid>" in item for item in remote), remote
    assert any("thread reply" in item for item in remote), remote
    assert any("root" in item and "merge" in item for item in remote), remote
    assert any("merge request" in item for item in remote), remote


def test_the_saas_entry_names_its_environment_variables() -> None:
    step = _step(SAAS)
    inputs = " ".join(step.inputs.values())
    for name in SAAS_VARS:
        assert name in inputs, name
    remote = [item.lower() for item in step.remote_assertions]
    assert any("closes #<iid>" in item for item in remote), remote


def test_the_old_gitlab_key_is_not_a_pairing() -> None:
    with pytest.raises(SystemExit):
        fe.parse_args(["acceptance", "--pairing", "gitlab+gitlab"])
    for key in GITLAB_KEYS:
        assert fe.parse_args(["acceptance", "--pairing", key]).pairing == key


# --- dry run -----------------------------------------------------------------------------


@pytest.mark.parametrize("key", GITLAB_KEYS)
def test_dry_run_lists_every_step_with_none_unset(key: str) -> None:
    out = io.StringIO()
    assert acceptance.dry_run(key, out) == 0
    text = out.getvalue()
    assert "not installed" not in text
    assert "unset" not in text
    assert f"acceptance {key}" in text
    step = _step(key)
    assert step.name in text
    assert " ".join(step.command) in text
    assert str(step.deadline_seconds) in text
    for name in step.inputs:
        assert name in text
    for item in step.remote_assertions:
        assert item in text
    for field, _expected in step.persisted_assertions:
        assert field in text
    for item in step.teardown_checks:
        assert item in text


@pytest.mark.usefixtures("_no_saas_env")
@pytest.mark.parametrize("key", GITLAB_KEYS)
def test_main_dry_run_needs_no_gitlab_environment(
    key: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _forbid_launch(monkeypatch)
    assert fe.main(["acceptance", "--pairing", key, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert _step(key).name in out
    assert "unset" not in out


# --- the saas live run fails fast --------------------------------------------------------


class _Runner:
    def __init__(self, evidence: dict[str, Any] | None) -> None:
        self.evidence = evidence
        self.calls: list[tuple[list[str], float]] = []

    def __call__(self, argv: list[str], timeout: float) -> int:
        self.calls.append((list(argv), timeout))
        if self.evidence is not None:
            path = Path(argv[argv.index("--evidence") + 1])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(self.evidence))
        return 0


class _Kubectl:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")


def _args(tmp_path: Path, key: str) -> Any:
    return fe.parse_args(
        [
            "acceptance",
            "--pairing",
            key,
            "--context",
            "k8",
            "--candidate-images",
            "build",
            "--namespace",
            NAMESPACE,
            "--evidence",
            str(tmp_path / "summary.json"),
        ]
    )


@pytest.mark.usefixtures("_no_saas_env")
def test_saas_live_run_without_the_environment_names_the_project_variable_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _forbid_launch(monkeypatch)
    runner, kubectl = _Runner(None), _Kubectl()
    code = acceptance.run(_args(tmp_path, SAAS), runner=runner, kubectl=kubectl)
    assert code == fe.EXIT_CONFIG
    err = capsys.readouterr().err
    assert "CURIE_FACTORY_GITLAB_PROJECT" in err
    assert "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE" not in err
    assert runner.calls == [] and kubectl.calls == []


@pytest.mark.usefixtures("_no_saas_env")
@pytest.mark.parametrize("index", range(len(SAAS_VARS)))
def test_saas_live_run_names_the_first_unset_variable(
    index: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name in SAAS_VARS[:index]:
        monkeypatch.setenv(name, str(tmp_path / name.lower()))
    _forbid_launch(monkeypatch)
    runner = _Runner(None)
    code = acceptance.run(_args(tmp_path, SAAS), runner=runner, kubectl=_Kubectl())
    assert code == fe.EXIT_CONFIG
    err = capsys.readouterr().err
    assert SAAS_VARS[index] in err
    for later in SAAS_VARS[index + 1 :]:
        assert later not in err
    assert runner.calls == []


@pytest.mark.usefixtures("_no_saas_env")
def test_main_saas_live_run_without_the_environment_exits_nonzero_naming_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _forbid_launch(monkeypatch)
    code = fe.main(["acceptance", "--pairing", SAAS])
    assert code != 0
    assert code == fe.EXIT_CONFIG
    assert "CURIE_FACTORY_GITLAB_PROJECT" in capsys.readouterr().err


def test_saas_live_run_with_the_environment_set_runs_its_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in SAAS_VARS:
        monkeypatch.setenv(name, str(tmp_path / name.lower()))
    runner = _Runner(None)
    acceptance.run(_args(tmp_path, SAAS), runner=runner, kubectl=_Kubectl())
    assert len(runner.calls) == 1
    argv, timeout = runner.calls[0]
    assert argv[argv.index("--forge-target") + 1] == "saas"
    assert timeout == _step(SAAS).deadline_seconds


# --- the fixture run and its fixture-gone teardown check ---------------------------------


def _fixture_evidence(state_dir: Path) -> dict[str, Any]:
    """Evidence as main(), run_loop and _finish write it for a passing @fixture run.

    Written out literally, not derived from the persisted assertions, so an
    assertion whose dotted path names a field the harness never writes fails.
    """

    names = (
        "reset fixture repository",
        "stop api port-forward",
        "restore fixture default branch",
        "delete namespaces",
    )
    return {
        "schema": "curie.factory-e2e.evidence/v1",
        "mode": "run:forge-loop",
        "kube_context": "k8",
        "namespace": NAMESPACE,
        "forge": "gitlab",
        "forge_target": "fixture",
        "gitlab_fixture": {"state_dir": str(state_dir)},
        "work_item_id": "6f1c3a52-0000-4000-8000-000000000001",
        "scenario": {
            "forge": "gitlab",
            "pairing": FIXTURE,
            "tracker": {
                "iid": 3,
                "closes_issue": True,
                "thread_replies": 1,
                "in_progress_label_seconds": 21.0,
                "status_note_links_mr": True,
                "reporter_label_admitted": False,
                "reporter_state_labels": [],
            },
            "change_number": 7,
            "red_ci": {"sha": "a" * 40, "context": "factory-e2e/loop-gate", "state": "failure"},
            "ci_after_fix": {"sha": "b" * 40, "state": "passing"},
            "review_delivery_status_code": 200,
            "review_delivery_api_status": "factory_admitted",
            "merge": {"status_code": 200, "merged": True},
            "forge_merged": True,
            "work_item_pr_status": "merged",
            "cli_pr_status": "merged",
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
                        "kind": "gitlab",
                        "host": "https://gitlab.example",
                        "scope_id": "acme/fixture",
                        "issue_id": "3",
                        # Fixture label, same shape as other factory probes.
                        # scope_id and issue_id already name the fake project
                        # and issue.
                        "display_key": "ACME-3",
                        "url": "https://gitlab.example/acme/fixture/issues/3",
                    }
                }
            ],
            "verdict": "passed",
            "failures": [],
        },
        "result": "passed",
        "teardown": [{"step": name, "ok": True, "detail": {}} for name in names],
        "teardown_clean": True,
        "teardown_ok": dict.fromkeys(names, True),
    }


def _with_driver_teardowns(evidence: dict[str, Any]) -> dict[str, Any]:
    """Add whatever extra teardown_ok steps the entry asserts beyond the generic ones."""

    for name in _teardown_names(FIXTURE) - set(evidence["teardown_ok"]):
        evidence["teardown_ok"][name] = True
        evidence["teardown"].append({"step": name, "ok": True, "detail": {}})
    return evidence


def test_independently_built_fixture_evidence_satisfies_the_persisted_assertions(
    tmp_path: Path,
) -> None:
    evidence = _with_driver_teardowns(_fixture_evidence(tmp_path / "gone"))
    for field, expected in _persisted(FIXTURE).items():
        got = acceptance._get_path(evidence, field)
        assert got is not acceptance._MISSING, f"{field} is not in the harness evidence"
        if expected == acceptance.CREDENTIALS_EXPIRY_VALID:
            assert acceptance._credential_expiry_failures(field, got) == [], (field, got)
        else:
            assert got == expected, (field, got, expected)


def test_fixture_run_passes_when_the_namespace_and_the_fixture_are_gone(tmp_path: Path) -> None:
    evidence = _with_driver_teardowns(_fixture_evidence(tmp_path / "gitlab-state-gone"))
    runner, kubectl = _Runner(evidence), _Kubectl()
    assert acceptance.run(_args(tmp_path, FIXTURE), runner=runner, kubectl=kubectl) == 0
    (argv, _timeout) = runner.calls[0]
    assert argv[argv.index("--forge-target") + 1] == "fixture"
    assert kubectl.calls, "the namespace check never ran"
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert (summary["pairing"], summary["result"]) == (FIXTURE, "passed")


def test_fixture_run_fails_while_the_fixture_state_dir_remains(tmp_path: Path) -> None:
    state_dir = tmp_path / "gitlab-state"
    state_dir.mkdir()
    (state_dir / "state.json").write_text("{}")
    evidence = _with_driver_teardowns(_fixture_evidence(state_dir))
    assert acceptance.run(_args(tmp_path, FIXTURE), runner=_Runner(evidence), kubectl=_Kubectl())
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["result"] == "failed"
    failures = summary["steps"][0]["failures"]
    assert any("fixture" in failure.lower() for failure in failures), failures
    assert not any("no teardown check is defined" in failure for failure in failures), failures


def test_fixture_run_fails_when_the_evidence_names_no_fixture(tmp_path: Path) -> None:
    evidence = _with_driver_teardowns(_fixture_evidence(tmp_path / "gone"))
    del evidence["gitlab_fixture"]
    assert acceptance.run(_args(tmp_path, FIXTURE), runner=_Runner(evidence), kubectl=_Kubectl())
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["result"] == "failed"
    failures = summary["steps"][0]["failures"]
    assert any("fixture" in failure.lower() for failure in failures), failures
    assert not any("no teardown check is defined" in failure for failure in failures), failures


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("scenario.tracker.reporter_label_admitted", True),
        ("scenario.tracker.reporter_state_labels", ["curie-factory:in-progress"]),
        ("scenario.tracker.status_note_links_mr", False),
        ("scenario.tracker.closes_issue", False),
        ("scenario.tracker.thread_replies", 2),
        ("scenario.pairing", SAAS),
        ("forge", "github"),
    ],
)
def test_fixture_run_fails_on_each_wrong_observation(
    field: str, value: Any, tmp_path: Path
) -> None:
    evidence = _with_driver_teardowns(_fixture_evidence(tmp_path / "gone"))
    node: Mapping[str, Any] | dict[str, Any] = evidence
    *parents, leaf = field.split(".")
    for part in parents:
        node = node[part]
    assert isinstance(node, dict)
    node[leaf] = value
    assert acceptance.run(_args(tmp_path, FIXTURE), runner=_Runner(evidence), kubectl=_Kubectl())
    summary = json.loads((tmp_path / "summary.json").read_text())
    failures = summary["steps"][0]["failures"]
    assert any(field in failure for failure in failures), failures
