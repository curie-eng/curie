"""Negative controls for the scripted factory scenario (#3814).

The legacy helpers are the #3521 and #3617 behaviors. The test shows that
putting either one back makes the scenario assertion fail. The product does
not call them.
"""

from __future__ import annotations

import importlib.util
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
    spec = importlib.util.spec_from_file_location(
        "curie_scripted_kind", ROOT / "scripted_kind.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_declared_unittest_preflight_is_accepted() -> None:
    scenario.assert_preflight_outcome("passed")
    scenario.assert_preflight_outcome("failed")


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
