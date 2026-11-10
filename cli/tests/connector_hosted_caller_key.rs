//! A hosted connector started locally without a caller public key is refused.
//!
//! `ConnectorStartSpec::from_declaration` takes `caller_public_key: Option<&str>`
//! as its last argument. An empty key is the same named failure the chart
//! render uses.

use curie::connector_build::{
    ConnectorScope, ConnectorSecretVolumes, ConnectorSpecDecl, ConnectorsFileDecl,
};
use curie::docker::ConnectorStartSpec;
use std::collections::BTreeMap;

fn scope() -> ConnectorScope {
    ConnectorScope {
        release: "curie".to_string(),
        agent: "acme-bot".to_string(),
        namespace: "default".to_string(),
    }
}

fn hosted() -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        image: Some("ghcr.io/acme-corp/acme-bot-grafana:v1".to_string()),
        ..Default::default()
    }
}

fn start(
    spec: &ConnectorSpecDecl,
    caller_public_key: Option<&str>,
) -> anyhow::Result<ConnectorStartSpec> {
    ConnectorStartSpec::from_declaration(
        "grafana",
        spec,
        "ghcr.io/acme-corp/acme-bot-grafana:v1",
        &scope(),
        "curie-skill-net",
        "curie-skill-abc123",
        &ConnectorSecretVolumes::new(&ConnectorsFileDecl::default()),
        &BTreeMap::new(),
        caller_public_key,
    )
}

#[test]
fn a_hosted_connector_with_an_empty_caller_public_key_is_refused() {
    for key in [None, Some("")] {
        let error =
            start(&hosted(), key).expect_err("a hosted connector with an empty caller public key");
        let text = format!("{error:#}");
        assert!(
            text.contains("hosted_connector_requires_caller_key"),
            "{text}"
        );
    }
}

#[test]
fn a_hosted_connector_with_a_caller_public_key_still_starts() {
    start(
        &hosted(),
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
    .expect("a caller public key lets the hosted connector start");
}

#[test]
fn a_remote_connector_does_not_need_a_caller_public_key() {
    let spec = ConnectorSpecDecl {
        url: Some("https://mcp.example.com/mcp".to_string()),
        ..Default::default()
    };
    start(&spec, None).expect("a remote connector is not hosted");
}
