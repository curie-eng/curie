from dataclasses import dataclass


@dataclass(frozen=True)
class Exemption:
    rule_id: str
    workload: str
    container: str
    reason: str
    reference: str
    classification: str | None = None
    profile: str | None = None


EXEMPTIONS = (
    Exemption(
        "P2",
        "Deployment/agent-sandbox-controller",
        "agent-sandbox-controller",
        "The vendored controller has no probes; adding them is separate work.",
        "#5297",
    ),
    Exemption(
        "P4",
        "Deployment/*-ui",
        "ui",
        "The SPA uses the numeric user inherited from its upstream nginx base image.",
        "#5297",
    ),
    Exemption(
        "P1",
        "Deployment/*-api",
        "api",
        "The offline development profile uses an imported local application image.",
        "#5297",
        classification="offline-local-api",
        profile="values-dev.yaml",
    ),
    Exemption(
        "P1",
        "Deployment/*-worker",
        "worker",
        "The offline development profile uses an imported local application image.",
        "#5297",
        classification="offline-local-worker",
        profile="values-dev.yaml",
    ),
    Exemption(
        "P1",
        "Deployment/*-ui",
        "ui",
        "The offline development profile uses an imported local application image.",
        "#5297",
        classification="offline-local-ui",
        profile="values-dev.yaml",
    ),
    Exemption(
        "P1",
        "Deployment/*-api",
        "schema-wait",
        "The offline schema gate uses the imported local API image.",
        "#5297",
        classification="offline-local-api",
        profile="values-dev.yaml",
    ),
    Exemption(
        "P1",
        "Job/*-schema-migrate",
        "schema-migrate",
        "The offline migration hook uses the imported local API image.",
        "#5297",
        classification="offline-local-api",
        profile="values-dev.yaml",
    ),
    Exemption(
        "P1",
        "Job/*-upgrade-drain*",
        "upgrade-drain*",
        "The offline upgrade hooks use the imported local worker image.",
        "#5297",
        classification="offline-local-worker",
        profile="values-dev.yaml",
    ),
    Exemption(
        "P5",
        "*",
        "*",
        "Init containers perform finite bootstrap work.",
        "#2360",
        classification="init-bootstrap",
    ),
    Exemption(
        "P5",
        "*",
        "*",
        "Helm hook containers perform finite lifecycle work.",
        "#2360",
        classification="helm-hook",
    ),
    Exemption(
        "P5",
        "*",
        "*",
        "Sleeping prewarm containers only keep images cached.",
        "#2360",
        classification="prewarm-sleep",
    ),
    Exemption(
        "P5",
        "*",
        "*",
        "The nginx SPA has no application telemetry SDK.",
        "#2360",
        classification="spa",
    ),
    Exemption(
        "P5",
        "SandboxTemplate/*-runner",
        "runner",
        "The runtime harness exercises bundle fetching with no collector or app services.",
        "#5297",
        classification="harness-no-collector",
        profile="values-e2e-harness.yaml",
    ),
)
