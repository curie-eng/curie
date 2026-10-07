"""Negative controls for the scripted factory scenario (#3814).

The legacy helpers are the #3521 and #3617 behaviors. The test shows that
putting either one back makes the scenario assertion fail. The product does
not call them.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "curie_scripted_scenario", ROOT / "scripted_scenario.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


scenario = _load()


def _load_kind():
    spec = importlib.util.spec_from_file_location("curie_scripted_kind", ROOT / "scripted_kind.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_empty_transcript_cannot_qualify_a_healthy_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A ready Helm release is not an executed #3814 factory scenario."""
    kind = _load_kind()
    transcript = tmp_path / "empty.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": []}))
    monkeypatch.delenv("CURIE_FACTORY_SCRIPTED_PLAN", raising=False)
    monkeypatch.setenv("CURIE_FACTORY_MODEL_BASE_URL", "http://10.1.2.3:8080")
    # Harness unit control only: the integration lane must execute real Helm.
    monkeypatch.setattr(
        kind.fe.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    args = argparse.Namespace(
        context="kind-example",
        namespace="test-factory-empty",
        transcript=str(transcript),
        model_base_url=None,
        listen_host="10.1.2.3",
    )
    assert kind.main(args) == kind.fe.EXIT_CONFIG
    assert "contains no recorded exchanges" in capsys.readouterr().err


def test_unconsumed_model_recording_fails_cleanup_after_run_success(tmp_path: Path) -> None:
    kind = _load_kind()
    transcript = tmp_path / "unconsumed.json"
    transcript.write_text(json.dumps({"version": 1, "exchanges": [{"match": {}}]}))
    model_module = kind.fixture_module(
        "cleanup_model_fixture", kind.REPO_ROOT / "tools/model-script/model_script.py"
    )
    model = model_module.ModelScript(transcript, require_consumed=True)
    model.start()
    fixtures = kind.fe.Teardown()
    fixtures.push("close model fixture", model.close)
    code, cleanup = kind.finish_run(None, fixtures, model, code=0, record=False)
    assert code == kind.fe.EXIT_FAILED
    assert cleanup[0]["ok"] is False
    assert "unconsumed" in cleanup[0]["detail"]


def test_declared_unittest_preflight_is_accepted() -> None:
    scenario.assert_preflight_outcome("passed")
    scenario.assert_preflight_outcome("failed")


@pytest.mark.parametrize(
    "fault", [None, "timeout", "early_stop", "missing_pending", "wrong_head", "failing"]
)
def test_ci_oracle_requires_published_head_pending_then_green(fault: str | None) -> None:
    head = "a" * 40
    result = {
        "terminal": True,
        "request_status": "completed",
        "terminal_cause": "completed",
        "ending_cause": "completed",
        "work_item_state": "published",
        "ci": {"state": "passing", "head_sha": head},
    }
    observations = [{"state": "pending", "head_sha": head}, {"state": "passing", "head_sha": head}]
    if fault in {"timeout", "early_stop"}:
        result.update(request_status="failed", terminal_cause=fault, ending_cause=fault)
    elif fault == "missing_pending":
        observations = observations[1:]
    elif fault == "wrong_head":
        observations[0]["head_sha"] = "b" * 40
    elif fault == "failing":
        result["ci"]["state"] = "failing"
    if fault is None:
        scenario.assert_ci_completion(result, observations, head)
    else:
        with pytest.raises(scenario.ScenarioAssertionError):
            scenario.assert_ci_completion(result, observations, head)


def test_fixture_bundle_preserves_factory_hooks_and_agents(tmp_path: Path) -> None:
    kind = _load_kind()
    source = kind.REPO_ROOT / "examples" / "dark-factory"
    target = kind.fixture_bundle(tmp_path)
    assert (target / "connectors.yaml").read_text() == "connectors: {}\n"
    for area in ("agents", "skills", "hooks"):
        for path in (source / area).rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                assert (target / path.relative_to(source)).read_bytes() == path.read_bytes()


def test_unavailable_preflight_is_rejected() -> None:
    with pytest.raises(scenario.ScenarioAssertionError, match="unavailable"):
        scenario.assert_preflight_outcome("unavailable")


def test_reintroducing_hardcoded_preflight_fails_the_scenario() -> None:
    outcome = scenario.legacy_3521_preflight(scenario.FIXTURE_CHECK)
    with pytest.raises(scenario.ScenarioAssertionError):
        scenario.assert_preflight_outcome(outcome)


def test_fixture_paths_are_on_the_publication_request() -> None:
    scenario.assert_publication_paths(list(scenario.FIXTURE_CHANGED_PATHS))


def test_scripted_values_put_the_proxy_on_the_worker() -> None:
    kind = _load_kind()
    kind.refuse_context("kind-curie-e2e")
    with pytest.raises(kind.ScriptedScenarioError):
        kind.refuse_context("k8")
    values = kind.scripted_values(
        model_base_url="http://10.1.2.3:8080",
        github_api="https://10.9.9.9:8443/api/v3",
        clone_base="https://10.9.9.9:8443",
        ca_configmap="github-stub-ca",
    )
    assert {"name": "CURIE_MODEL_BASE_URL", "value": "http://10.1.2.3:8080"} in values["worker"][
        "extraEnv"
    ]
    assert values["api"]["githubStubCaConfigMap"] == "github-stub-ca"
    assert {"cidr": "10.1.2.3/32", "ports": [{"protocol": "TCP", "port": 8080}]} in values[
        "security"
    ]["networkPolicy"]["allowedEgress"]


def test_reintroducing_curie_path_prefixes_fails_the_scenario() -> None:
    filtered = scenario.legacy_3617_paths(scenario.FIXTURE_CHANGED_PATHS)
    with pytest.raises(scenario.ScenarioAssertionError, match="unitconv"):
        scenario.assert_publication_paths(filtered)
