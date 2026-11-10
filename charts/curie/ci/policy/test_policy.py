from pathlib import Path

import pytest

from .engine import PROFILES, audit_chart
from .exemptions import EXEMPTIONS, Exemption

REPO = Path(__file__).resolve().parents[4]
CHART = REPO / "charts" / "curie"
DISPATCHER = (
    "--set",
    "dispatcher.deploy=true",
    "--set-string",
    "dispatcher.slack.appToken=xapp-policy-example",
    "--set-string",
    "dispatcher.slack.botToken=xoxb-policy-example",
)
MAIL = (
    "--set",
    "mailAdapter.deploy=true",
    "--set",
    "mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24",
    "--set-string",
    "mailAdapter.channelToken=example-channel-token",
    "--set-string",
    "mailAdapter.egressSecret=example-egress-secret",
    "--set-string",
    "mailAdapter.agentmail.apiKey=example-api-key",
)
EXTERNAL_COLLECTOR = (
    "--set",
    "otelCollector.deploy=false",
    "--set-string",
    "otelCollector.endpoint=https://otel.example.com:4318",
    "--set",
    "otelCollector.egress[0].cidr=192.0.2.40/32",
    "--set",
    "otelCollector.egress[0].ports[0].protocol=TCP",
    "--set",
    "otelCollector.egress[0].ports[0].port=4318",
    "--set",
    "mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32",
)


@pytest.fixture(scope="session")
def baseline_errors() -> list[str]:
    return audit_chart(CHART, REPO, profiles=PROFILES)


def test_all_four_chart_profiles_satisfy_policy(baseline_errors: list[str]) -> None:
    assert baseline_errors == []


@pytest.mark.parametrize(
    "helm_args",
    [DISPATCHER, DISPATCHER + MAIL, DISPATCHER + MAIL + EXTERNAL_COLLECTOR],
    ids=["dispatcher", "dispatcher-mail", "dispatcher-mail-byo-collector"],
)
def test_optional_instrumented_workloads(helm_args: tuple[str, ...]) -> None:
    assert audit_chart(CHART, REPO, profiles=(None,), helm_args=helm_args) == []


def test_valid_operator_probe_cadence_override() -> None:
    errors = audit_chart(
        CHART,
        REPO,
        profiles=PROFILES,
        helm_args=(
            "--set",
            "langfuse.worker.readinessProbe.initialDelaySeconds=25",
            "--set",
            "langfuse.worker.livenessProbe.initialDelaySeconds=100",
        ),
    )
    assert errors == []


def test_unmatched_exemption_is_an_error() -> None:
    unused = Exemption(
        rule_id="P2",
        workload="*",
        container="missing-policy-container",
        reason="Negative control for exemption retirement",
        reference="#5297",
    )
    errors = audit_chart(
        CHART,
        REPO,
        profiles=PROFILES,
        exemptions=(*EXEMPTIONS, unused),
    )
    assert any(
        "exemption" in error.lower() and "missing-policy-container" in error for error in errors
    ), errors
