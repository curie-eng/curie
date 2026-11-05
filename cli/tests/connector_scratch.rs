//! `scratch:` on a hosted connector (#5415): a bounded writable volume.
//!
//! The CLI carries the declaration through to the API and, on the local
//! docker tier, starts the connector container itself. That container must get
//! a writable path where the rendered pod has its `emptyDir`, or a server that
//! spools to its declared path fails locally on a read-only root filesystem.
//! `plugin_format.validate_connectors` owns the path rules; the size grammar is
//! shared through `tests/vectors/connector-scratch-size.json`, which both
//! readers parse with their own code.

use curie::connector_build::{
    self, ConnectorScope, ConnectorScratchDecl, ConnectorSecretVolumes, ConnectorSpecDecl,
    ConnectorsFileDecl,
};
use curie::docker::ConnectorStartSpec;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

fn vector_file(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/vectors")
        .join(name);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

fn scope() -> ConnectorScope {
    ConnectorScope {
        release: "curie".to_string(),
        agent: "acme-bot".to_string(),
        namespace: "default".to_string(),
    }
}

fn start(spec: &ConnectorSpecDecl) -> anyhow::Result<ConnectorStartSpec> {
    ConnectorStartSpec::from_declaration(
        "files",
        spec,
        "ghcr.io/acme-corp/acme-bot-files:v1",
        &scope(),
        "curie-skill-net",
        "curie-skill-abc123",
        &ConnectorSecretVolumes::new(&ConnectorsFileDecl::default()),
        &BTreeMap::new(),
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
}

fn hosted(scratch: Option<ConnectorScratchDecl>) -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        image: Some("ghcr.io/acme-corp/acme-bot-files:v1".to_string()),
        scratch,
        ..Default::default()
    }
}

fn mounts(args: &[String]) -> Vec<String> {
    args.windows(2)
        .filter(|pair| pair[0] == "--mount")
        .map(|pair| pair[1].clone())
        .collect()
}

#[test]
fn the_mirror_carries_scratch_as_written() {
    let file = connector_build::parse_connectors(
        "connectors:\n  files:\n    image: ghcr.io/example/files-mcp:1\n    \
         scratch:\n      path: /scratch\n      size_limit: 2Gi\n",
    )
    .expect("scratch is accepted");
    let spec = serde_json::to_value(&file.connectors["files"]).expect("serialize the mirror");
    assert_eq!(spec["scratch"]["path"], "/scratch");
    assert_eq!(spec["scratch"]["size_limit"], "2Gi");

    let fields = vector_file("connector-fields.json");
    let expected: BTreeSet<String> = fields["models"]["ConnectorScratch"]
        .as_array()
        .expect("ConnectorScratch is listed in the vector")
        .iter()
        .map(|v| v.as_str().expect("a field name").to_string())
        .collect();
    let keys: BTreeSet<String> = spec["scratch"]
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, expected);

    for document in [
        "connectors:\n  files:\n    image: x:1\n    scratch:\n      path: /scratch\n      \
         size_limit: 2Gi\n      medium: Memory\n",
        "connectors:\n  files:\n    image: x:1\n    scratch:\n      path: /scratch\n",
    ] {
        assert!(
            connector_build::parse_connectors(document).is_err(),
            "an unknown or missing scratch key must be refused: {document}"
        );
    }
}

#[test]
fn the_scratch_size_grammar_reads_the_shared_vector() {
    let cases = vector_file("connector-scratch-size.json")["cases"]
        .as_array()
        .expect("cases")
        .clone();
    assert!(cases.len() >= 20);
    for case in cases {
        let text = case["size_limit"].as_str().expect("a size_limit string");
        let expected = case["bytes"].as_u64();
        assert_eq!(
            connector_build::scratch_size_bytes(text),
            expected,
            "size_limit {text:?}"
        );
    }
}

#[test]
fn a_local_connector_with_scratch_gets_a_bounded_world_writable_tmpfs() {
    let spec = hosted(Some(ConnectorScratchDecl {
        path: "/scratch".to_string(),
        size_limit: "2Gi".to_string(),
    }));
    let args = start(&spec).expect("a hosted connector with scratch starts").run_args();
    assert_eq!(
        mounts(&args),
        vec!["type=tmpfs,destination=/scratch,tmpfs-size=2147483648,tmpfs-mode=1777".to_string()]
    );
    // The root filesystem stays read-only: the tmpfs is the only writable path.
    assert!(args.iter().any(|arg| arg == "--read-only"), "{args:?}");
}

#[test]
fn a_local_connector_without_scratch_mounts_nothing() {
    let args = start(&hosted(None)).expect("a hosted connector starts").run_args();
    assert!(mounts(&args).is_empty(), "{args:?}");
}

#[test]
fn a_local_connector_with_an_unreadable_scratch_size_is_refused() {
    let spec = hosted(Some(ConnectorScratchDecl {
        path: "/scratch".to_string(),
        size_limit: "65Gi".to_string(),
    }));
    let error = start(&spec).expect_err("a scratch size above the ceiling");
    let text = format!("{error:#}");
    assert!(text.contains("connectors.bad_scratch_size"), "{text}");
}
