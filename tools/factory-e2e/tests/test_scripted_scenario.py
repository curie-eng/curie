"""Negative controls for the scripted factory scenario (#3814).

The legacy helpers are the #3521 and #3617 behaviors. The test shows that
putting either one back makes the scenario assertion fail. The product does
not call them.
"""

from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path
from typing import Any

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
    values = kind.scripted_values(
        model_base_url="http://10.1.2.3:8080",
        github_api="https://10.9.9.9:8443/api/v3",
        clone_base="https://10.9.9.9:8443",
        ca_configmap="github-stub-ca",
        runner_image="localhost:24111/curie-runner",
    )
    assert values["agentSandbox"]["runner"]["image"] == "localhost:24111/curie-runner"
    assert {"name": "CURIE_MODEL_BASE_URL", "value": "http://10.1.2.3:8080"} in values["worker"][
        "extraEnv"
    ]
    assert "githubStubCaConfigMap" not in values["api"]
    factory = values["factory"]
    assert {entry["apiUrl"] for entry in factory["codeHosts"] + factory["trackers"]} == {
        "https://10.9.9.9:8443/api/v3"
    }
    assert factory["trackers"][0]["pollIntervalSeconds"] == 5
    assert values["codeHostTrust"]["caBundle"]["configMapRef"] == {
        "name": "github-stub-ca", "key": "ca.pem"
    }
    assert {"cidr": "10.1.2.3/32", "ports": [{"protocol": "TCP", "port": 8080}]} in values[
        "security"
    ]["networkPolicy"]["allowedEgress"]


# Recorded from `kubectl get networkpolicy -A -l app.kubernetes.io/component=publication
# -o json` on the scripted kind install (chart 0.12.4). Trimmed to two GitHub CIDRs,
# with metadata reduced to name, namespace and labels.
RECORDED_PUBLICATION_POLICY: dict[str, Any] = {
    "apiVersion": "networking.k8s.io/v1",
    "kind": "NetworkPolicy",
    "metadata": {
        "name": "curie-publication-egress",
        "namespace": "curie-curie-publication",
        "labels": {
            "app.kubernetes.io/component": "publication",
            "app.kubernetes.io/instance": "curie",
            "app.kubernetes.io/managed-by": "Helm",
            "app.kubernetes.io/name": "curie",
            "app.kubernetes.io/version": "0.12.4",
            "helm.sh/chart": "curie-0.12.4",
        },
    },
    "spec": {
        "egress": [
            {
                "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}],
                "to": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                        }
                    }
                ],
            },
            {
                "ports": [{"port": 443, "protocol": "TCP"}],
                "to": [
                    {"ipBlock": {"cidr": "192.30.252.0/22"}},
                    {"ipBlock": {"cidr": "185.199.108.0/22"}},
                ],
            },
        ],
        "podSelector": {"matchLabels": {"curietech.ai/component": "publication"}},
        "policyTypes": ["Ingress", "Egress"],
    },
}


def _policy_list(*items: dict[str, Any]) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "List", "items": list(items)}


def test_publication_stub_egress_allows_only_the_stub_for_the_chart_publication_pods() -> None:
    kind = _load_kind()
    policy = kind.publication_stub_egress_policy(
        _policy_list(RECORDED_PUBLICATION_POLICY), "https://172.18.0.1:40095"
    )
    assert policy["apiVersion"] == "networking.k8s.io/v1"
    assert policy["kind"] == "NetworkPolicy"
    assert policy["metadata"] == {
        "name": "scripted-github-stub-publication-egress",
        "namespace": "curie-curie-publication",
    }
    assert policy["spec"]["podSelector"] == {
        "matchLabels": {"curietech.ai/component": "publication"}
    }
    assert policy["spec"]["policyTypes"] == ["Egress"]
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "172.18.0.1/32"}}],
            "ports": [{"protocol": "TCP", "port": 40095}],
        }
    ]


def test_publication_stub_egress_copies_namespace_and_selector_from_the_chart_policy() -> None:
    kind = _load_kind()
    chart_policy = copy.deepcopy(RECORDED_PUBLICATION_POLICY)
    chart_policy["metadata"]["namespace"] = "team-a-curie-publication"
    other_selector = {
        "matchLabels": {"curietech.ai/component": "publication", "example": "other-release"}
    }
    chart_policy["spec"]["podSelector"] = other_selector
    policy = kind.publication_stub_egress_policy(
        _policy_list(chart_policy), "https://10.9.9.9:8443"
    )
    assert policy["metadata"]["namespace"] == "team-a-curie-publication"
    assert policy["spec"]["podSelector"] == other_selector
    assert policy["spec"]["policyTypes"] == ["Egress"]
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "10.9.9.9/32"}}],
            "ports": [{"protocol": "TCP", "port": 8443}],
        }
    ]


@pytest.mark.parametrize("count", [0, 2], ids=["no-chart-policy", "two-chart-policies"])
def test_publication_stub_egress_refuses_an_ambiguous_chart_policy_set(count: int) -> None:
    kind = _load_kind()
    with pytest.raises(kind.ScriptedScenarioError, match="exactly one"):
        kind.publication_stub_egress_policy(
            _policy_list(*[RECORDED_PUBLICATION_POLICY] * count), "https://172.18.0.1:40095"
        )


def test_reintroducing_curie_path_prefixes_fails_the_scenario() -> None:
    filtered = scenario.legacy_3617_paths(scenario.FIXTURE_CHANGED_PATHS)
    with pytest.raises(scenario.ScenarioAssertionError, match="unitconv"):
        scenario.assert_publication_paths(filtered)


@pytest.mark.parametrize("outcome", ["passed", "failed"])
def test_driver_verdict_accepts_an_observed_preflight_and_both_fixture_paths(
    outcome: str,
) -> None:
    kind = _load_kind()
    assert kind.verdict(outcome, list(scenario.FIXTURE_CHANGED_PATHS)) == 0


def test_driver_verdict_rejects_an_unavailable_preflight() -> None:
    kind = _load_kind()
    assert kind.verdict("unavailable", list(scenario.FIXTURE_CHANGED_PATHS)) == 1


@pytest.mark.parametrize("missing", scenario.FIXTURE_CHANGED_PATHS)
def test_driver_verdict_rejects_each_missing_fixture_path(missing: str) -> None:
    kind = _load_kind()
    paths = [path for path in scenario.FIXTURE_CHANGED_PATHS if path != missing]
    assert kind.verdict("passed", paths) == 1


@pytest.mark.parametrize("flag", ["--context", "--model-base-url"])
def test_scripted_parser_refuses_removed_external_cluster_flags(flag: str) -> None:
    import factory_e2e as fe

    with pytest.raises(SystemExit):
        fe.parse_args(["scripted", flag, "example"])


def test_scripted_parser_owns_its_cluster_and_supports_recording_and_image_builds() -> None:
    import factory_e2e as fe

    args = fe.parse_args(["scripted", "--build-images", "--record"])
    assert args.build_images is True
    assert args.record is True
    assert not hasattr(args, "context")
    assert not hasattr(args, "model_base_url")
