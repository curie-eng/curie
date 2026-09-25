//! Issue #3156: the observability provision plan installs the upstream stack
//! and the connector secret. It does not deploy the bundle or clear a
//! recorded model credential.

#[test]
fn provision_plan_installs_grafana_and_the_connector_secret_without_deploying() {
    let lines = curie::examples::observability_provision_plan(
        "charts/curie",
        "curie",
        "curie",
        "observability",
    );
    let joined = lines.join("\n");
    for needle in [
        "create namespace observability when it is absent",
        "preserve or create Secret grafana-admin",
        "grafana-community/grafana",
        "--reuse-values",
        "curie-values.yaml",
        "curie-grafana-connector",
    ] {
        assert!(
            lines.iter().any(|line| line.contains(needle)),
            "provision plan must include {needle}; lines:\n{joined}"
        );
    }
    for needle in [
        "cluster deploy",
        "fakeModel",
        "agentSandbox.runner.credentials",
    ] {
        assert!(
            !joined.contains(needle),
            "provision plan must not include {needle}; lines:\n{joined}"
        );
    }
}
