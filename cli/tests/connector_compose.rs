//! Connector argv, Compose overlays, and project-scoped credential volume teardown.
//! Daemon-backed transfer behavior is covered by the local lifecycle tests.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use curie::connector_build::{
    compose_overlay, compose_overlay_path, compose_up_command, connector_scope_env,
    connector_substitutions, object_name, service_dns, substitute, ConnectorBuildDecl,
    ConnectorLockEntryDecl, ConnectorLockFileDecl, ConnectorScope, ConnectorSecretVolumes,
    ConnectorSpecDecl, ConnectorsFileDecl, Delivery, SecretDecl, LOCK_VERSION,
};
use curie::docker::{
    connector_project_label, connector_teardown_plan, ConnectorStartSpec, ConnectorTeardownStep,
    CLI_MANAGED_LABEL, CONNECTOR_COMPONENT_LABEL,
};
use curie::local::connector_teardown_plan_for_down;
use curie_aci_protocol::env_keys;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const TEMPO_IMAGE: &str = "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
                           2222222222222222222222222222222222222222222222222222222222222222";
const WRITE_IMAGE: &str = "ghcr.io/acme-corp/sre-bot-k8s-write@sha256:\
                           5555555555555555555555555555555555555555555555555555555555555555";

// ─── Fixtures ────────────────────────────────────────────────────────────────

fn scope(agent: &str) -> ConnectorScope {
    ConnectorScope {
        release: "curie".to_string(),
        agent: agent.to_string(),
        namespace: "default".to_string(),
    }
}

/// The read-only `kubernetes` connector exactly as `examples/sre-bot`
/// declares it: the flag list is load-bearing security, not configuration.
fn kubernetes_spec() -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        image: Some("ghcr.io/containers/kubernetes-mcp-server:v0.0.66".to_string()),
        args: [
            "--port",
            "8000",
            "--bind-address",
            "0.0.0.0",
            "--read-only",
            "--toolsets",
            "core",
            "--disable-destructive",
            "--disable-multi-cluster",
            "--stateless",
            "--kubeconfig",
            "/secrets/kubeconfig",
        ]
        .iter()
        .map(|a| (*a).to_string())
        .collect(),
        secret_files: BTreeMap::from([(
            "K8S_READONLY_KUBECONFIG".to_string(),
            "/secrets/kubeconfig".to_string(),
        )]),
        ..Default::default()
    }
}

/// The gated write connector from the README's enable ceremony: a declared
/// env var, a declared secret env var, and a credential file.
fn k8s_write_spec() -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        env: BTreeMap::from([
            (
                "K8S_WRITE_ALLOWLIST".to_string(),
                "acme-ns/checkout-api".to_string(),
            ),
            (
                "K8S_WRITE_SELF_URL".to_string(),
                "${CURIE_CONNECTOR_URL}".to_string(),
            ),
        ]),
        args: vec![
            "--allowed-hosts".to_string(),
            "${CURIE_ALLOWED_HOSTS}".to_string(),
        ],
        secrets: vec![SecretDecl::Name("K8S_WRITE_TOKEN".to_string())],
        secret_files: BTreeMap::from([(
            "K8S_WRITE_KUBECONFIG".to_string(),
            "/secrets/kubeconfig".to_string(),
        )]),
        ..Default::default()
    }
}

fn start_spec(
    connector: &str,
    spec: &ConnectorSpecDecl,
    image: &str,
    identity: &ConnectorScope,
    plugin_dir: &Path,
    secret_values: &BTreeMap<String, String>,
) -> ConnectorStartSpec {
    let decl = ConnectorsFileDecl {
        connectors: BTreeMap::from([(connector.to_string(), spec.clone())]),
        ..Default::default()
    };
    let volumes = ConnectorSecretVolumes::new(&decl);
    let rendered = ConnectorStartSpec::from_declaration(
        connector,
        spec,
        image,
        identity,
        "curie-skill-net",
        "curie-skill-abc123",
        &volumes,
        secret_values,
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
    .unwrap_or_else(|error| panic!("start spec for {connector}: {error:#}"));
    let args = rendered.run_args();
    crate::docker_no_host_binds::assert_no_client_host_binds(&args);
    assert!(!args
        .iter()
        .any(|arg| arg.contains(&plugin_dir.display().to_string())));
    rendered
}

fn pairs_after(argv: &[String], flag: &str) -> Vec<String> {
    argv.windows(2)
        .filter(|w| w[0] == flag)
        .map(|w| w[1].clone())
        .collect()
}

fn vector_file() -> Value {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/vectors/connector-service-dns.json");
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&body).expect("parse connector-service-dns.json")
}

// ─── The start spec mirrors the rendered container ───────────────────────────

/// The alias is the Service DNS name, and it is frozen against the same corpus
/// the Python renderer is.
///
/// The runner never learns this name from the CLI: it derives it independently
/// from `connector_render.service_dns` using the boot-env scope. A hand port
/// that drifts leaves the sandbox dialing a name no container answers to, which
/// surfaces as a bare connection timeout with nothing logged on either side.
#[test]
fn the_network_alias_is_the_frozen_service_dns_for_every_vector() {
    let dir = TempDir::new().expect("a scratch bundle");
    for vector in vector_file()["vectors"].as_array().expect("vectors") {
        let identity = ConnectorScope {
            release: vector["release"].as_str().expect("release").to_string(),
            agent: vector["agent"].as_str().expect("agent").to_string(),
            namespace: vector["namespace"].as_str().expect("namespace").to_string(),
        };
        let connector = vector["connector"].as_str().expect("connector");
        let expected = vector["service_dns"].as_str().expect("service_dns");

        let argv = start_spec(
            connector,
            &kubernetes_spec(),
            TEMPO_IMAGE,
            &identity,
            dir.path(),
            &BTreeMap::new(),
        )
        .run_args();

        assert!(
            pairs_after(&argv, "--network-alias").contains(&expected.to_string()),
            "{}: the alias must be {expected}, got {argv:?}",
            vector["name"]
        );
    }
}

/// No published port, ever.
///
/// ADR 0113's "no host port dependency": the runner reaches the connector over
/// the private network by alias. A `-p` would collide the moment two bundles
/// ran at once, and would expose a credential-holding server on the host.
#[test]
fn the_connector_container_publishes_no_host_port() {
    let dir = TempDir::new().expect("a scratch bundle");
    let argv = start_spec(
        "tempo",
        &kubernetes_spec(),
        TEMPO_IMAGE,
        &scope("sre-bot"),
        dir.path(),
        &BTreeMap::new(),
    )
    .run_args();

    assert!(
        !argv.iter().any(|t| t == "-p" || t == "--publish"),
        "a connector must not be reachable from the host: {argv:?}"
    );
    assert!(
        !argv.iter().any(|t| t == "-P" || t == "--publish-all"),
        "{argv:?}"
    );
}

/// Every declared `arg` survives, in order.
///
/// This is the guard finding 5 asked for. `--read-only`, `--toolsets core` and
/// `--disable-destructive` are what keep this connector from exposing general
/// write and `configuration_view` (which returns the kubeconfig, bearer token
/// included, and is annotated readOnlyHint=true). Dropping the args mirroring
/// starts the same image with its DEFAULT tool surface and nothing anywhere
/// reports a problem.
#[test]
fn every_declared_arg_reaches_the_container_in_declaration_order() {
    let dir = TempDir::new().expect("a scratch bundle");
    let spec = kubernetes_spec();
    let argv = start_spec(
        "kubernetes",
        &spec,
        TEMPO_IMAGE,
        &scope("sre-bot"),
        dir.path(),
        &BTreeMap::new(),
    )
    .run_args();

    let image_at = argv
        .iter()
        .position(|t| t == TEMPO_IMAGE)
        .unwrap_or_else(|| panic!("the image is in argv: {argv:?}"));
    let tail = &argv[image_at + 1..];
    assert_eq!(
        tail, spec.args,
        "the container's argv tail is exactly the declared args, in order: {argv:?}"
    );

    for guard in [
        "--read-only",
        "--toolsets",
        "core",
        "--disable-destructive",
        "--disable-multi-cluster",
    ] {
        assert!(
            tail.iter().any(|t| t == guard),
            "{guard} is security, not configuration: {tail:?}"
        );
    }
}

/// The hardening set is applied by construction, exactly as `render_deployment`
/// applies it. The author never writes it, so the author cannot omit it.
#[test]
fn the_connector_container_is_hardened_the_same_way_the_pod_is() {
    let dir = TempDir::new().expect("a scratch bundle");
    let argv = start_spec(
        "tempo",
        &kubernetes_spec(),
        TEMPO_IMAGE,
        &scope("sre-bot"),
        dir.path(),
        &BTreeMap::new(),
    )
    .run_args();
    let joined = argv.join(" ");

    for flag in [
        "--read-only",
        "--cap-drop ALL",
        "--security-opt no-new-privileges",
        "--user 65532:65532",
    ] {
        assert!(
            joined.contains(flag),
            "the pod runs with {flag}'s equivalent; the container must too: {joined}"
        );
    }
}

/// Both labels, on every connector container, because teardown resolves them.
///
/// B1-8's whole point: the overlay file is regenerated state and must never be
/// load-bearing for teardown. If a container is not labeled at creation, no
/// `down` from any directory can find it.
#[test]
fn every_connector_container_carries_the_component_and_project_labels() {
    let dir = TempDir::new().expect("a scratch bundle");
    let argv = start_spec(
        "tempo",
        &kubernetes_spec(),
        TEMPO_IMAGE,
        &scope("sre-bot"),
        dir.path(),
        &BTreeMap::new(),
    )
    .run_args();
    let labels = pairs_after(&argv, "--label");

    assert!(
        labels.iter().any(|l| l == CONNECTOR_COMPONENT_LABEL),
        "{labels:?}"
    );
    assert!(
        labels
            .iter()
            .any(|l| *l == connector_project_label("curie-skill-abc123")),
        "{labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == CLI_MANAGED_LABEL),
        "a leftover connector must be identifiable as the CLI's (#747): {labels:?}"
    );
}

// ─── Substitution: the placeholders only Curie can fill ──────────────────────

/// `${CURIE_*}` placeholders expand from the identity, in both `args` and `env`.
///
/// Ported from `connector_render.substitutions` (`:143`) and `substitute`
/// (`:167`). `CURIE_ALLOWED_HOSTS` in particular carries all three host forms:
/// a server that validates the Host header rejects an in-cluster caller with
/// `forbidden: host not allowed` when the list is short one form (ADR-0086),
/// and which form the sandbox dials depends on how the URL was written.
#[test]
fn the_curie_placeholders_expand_from_the_identity() {
    let identity = scope("sre-bot");
    let subs = connector_substitutions(&identity, "tempo", 8000);
    let short = object_name(&identity.release, &identity.agent, "tempo");
    let dns = service_dns(
        &identity.release,
        &identity.agent,
        "tempo",
        &identity.namespace,
    );

    assert_eq!(subs.get("CURIE_CONNECTOR_HOST"), Some(&short));
    assert_eq!(subs.get("CURIE_CONNECTOR_PORT"), Some(&"8000".to_string()));
    assert_eq!(
        subs.get("CURIE_CONNECTOR_URL"),
        Some(&format!("http://{dns}:8000"))
    );

    let allowed = subs
        .get("CURIE_ALLOWED_HOSTS")
        .expect("the host allowlist is derived, never authored");
    assert_eq!(
        allowed,
        &format!("{short}:8000,{short}.default:8000,{dns}:8000"),
        "all three forms, in this order, matching connector_render.host_aliases"
    );

    assert_eq!(
        substitute("--hosts=${CURIE_ALLOWED_HOSTS}", &subs),
        format!("--hosts={allowed}")
    );
    assert_eq!(
        substitute("nothing to expand", &subs),
        "nothing to expand",
        "a value with no placeholder passes through untouched"
    );
}

/// The substituted values reach the container, in args and in env alike.
#[test]
fn substituted_args_and_env_reach_the_container() {
    let dir = TempDir::new().expect("a scratch bundle");
    let identity = scope("sre-bot");
    let subs = connector_substitutions(&identity, "k8s-write", 8000);
    let argv = start_spec(
        "k8s-write",
        &k8s_write_spec(),
        WRITE_IMAGE,
        &identity,
        dir.path(),
        &BTreeMap::from([("K8S_WRITE_TOKEN".to_string(), "s3cr3t-value".to_string())]),
    )
    .run_args();

    let expanded_arg = subs.get("CURIE_ALLOWED_HOSTS").expect("the allowlist");
    assert!(
        argv.iter().any(|t| t == expanded_arg),
        "an unexpanded ${{CURIE_ALLOWED_HOSTS}} reaches the server as a literal: {argv:?}"
    );
    assert!(
        !argv.iter().any(|t| t.contains("${CURIE_")),
        "no placeholder may survive into argv: {argv:?}"
    );

    let env = pairs_after(&argv, "-e");
    assert!(
        env.iter()
            .any(|e| e == "K8S_WRITE_ALLOWLIST=acme-ns/checkout-api"),
        "{env:?}"
    );
    assert!(
        env.iter().any(|e| e
            == &format!(
                "K8S_WRITE_SELF_URL={}",
                subs.get("CURIE_CONNECTOR_URL").expect("the url")
            )),
        "{env:?}"
    );
}

// Credential environment and daemon volume mounts.

/// A declared secret is forwarded by NAME; its value never enters argv.
///
/// Same rule `StartSpec` already follows for model credentials: `-e NAME` with
/// the value supplied to the Docker CLI process only, so it never lands in
/// `ps -ef` or `/proc/<pid>/cmdline`.
#[test]
fn a_declared_secrets_value_never_appears_in_argv() {
    let dir = TempDir::new().expect("a scratch bundle");
    let spec = start_spec(
        "k8s-write",
        &k8s_write_spec(),
        WRITE_IMAGE,
        &scope("sre-bot"),
        dir.path(),
        &BTreeMap::from([("K8S_WRITE_TOKEN".to_string(), "s3cr3t-value".to_string())]),
    );
    let argv = spec.run_args();

    assert!(
        pairs_after(&argv, "-e")
            .iter()
            .any(|e| e == "K8S_WRITE_TOKEN"),
        "the bare name is what is forwarded: {argv:?}"
    );
    assert!(
        !argv.iter().any(|t| t.contains("s3cr3t-value")),
        "the credential value leaked into the process table: {argv:?}"
    );
    assert!(
        spec.docker_env
            .iter()
            .any(|(k, v)| k == "K8S_WRITE_TOKEN" && v == "s3cr3t-value"),
        "the value travels to the Docker CLI child's environment instead"
    );
}

/// A `from_secret` reference has no local equivalent, so bring-up fails closed.
///
/// The Kubernetes form points at a Secret someone else provisioned (#1163);
/// there is nothing on a laptop that corresponds to it. Starting the container
/// without the credential would give a server that 401s on every call, which
/// reads as a broken connector rather than a missing prerequisite. The refusal
/// has to name all three ways forward (7.3, review round 2 finding r2-8).
#[test]
fn an_out_of_band_secret_reference_fails_closed_with_all_three_alternatives() {
    let spec = ConnectorSpecDecl {
        secrets: vec![SecretDecl::Ref {
            name: "GRAFANA_TOKEN".to_string(),
            from_secret: "grafana-sa".to_string(),
            key: None,
        }],
        ..Default::default()
    };

    let error = ConnectorStartSpec::from_declaration(
        "grafana",
        &spec,
        TEMPO_IMAGE,
        &scope("sre-bot"),
        "curie-skill-net",
        "curie-skill-abc123",
        &ConnectorSecretVolumes::new(&ConnectorsFileDecl::default()),
        &BTreeMap::new(),
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
    .expect_err("a SecretRef has no local-daemon equivalent");

    let text = format!("{error:#}");
    for needle in ["GRAFANA_TOKEN", "curie secrets set", "unhosted_url"] {
        assert!(
            text.contains(needle),
            "the refusal must name {needle}: {text}"
        );
    }
}

#[test]
fn secret_files_mount_one_daemon_volume_at_the_exact_declared_targets() {
    let mut spec = kubernetes_spec();
    spec.secret_files = BTreeMap::from([
        ("FIRST_TOKEN".to_string(), "/a/token".to_string()),
        ("SECOND_TOKEN".to_string(), "/b/token".to_string()),
    ]);
    let decl = ConnectorsFileDecl {
        connectors: BTreeMap::from([("kubernetes".to_string(), spec.clone())]),
        ..Default::default()
    };
    let volumes = ConnectorSecretVolumes::new(&decl);
    let mounts = volumes
        .mounts("kubernetes", &spec)
        .expect("credential mounts");
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts[0].volume, mounts[1].volume);
    assert_ne!(mounts[0].subpath, mounts[1].subpath);
    for (mount, target) in mounts.iter().zip(["/a/token", "/b/token"]) {
        assert_eq!(mount.target, target);
        assert_eq!(
            mount.subpath,
            Sha256::digest(target.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        assert_eq!(mount.volume, volumes.volumes["kubernetes"]);
    }

    let start = ConnectorStartSpec::from_declaration(
        "kubernetes",
        &spec,
        TEMPO_IMAGE,
        &scope("acme-bot"),
        "curie-skill-net",
        "curie-skill-abc123",
        &volumes,
        &BTreeMap::new(),
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
    .expect("render the connector");
    let argv = start.run_args();
    crate::docker_no_host_binds::assert_no_client_host_binds(&argv);
    assert_eq!(
        pairs_after(&argv, "--mount"),
        mounts
            .iter()
            .map(|mount| mount.docker_mount())
            .collect::<Vec<_>>()
    );
    for mount in &mounts {
        assert!(mount.docker_mount().contains("readonly"));
        assert!(mount.docker_mount().contains("volume-nocopy"));
    }

    let overlay = compose_overlay(
        &ConnectorLockFileDecl::default(),
        &decl,
        &scope("acme-bot"),
        "acme-project",
        &volumes,
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
    .expect("render the same credential volumes for Compose");
    let service = &overlay["services"][object_name("curie", "acme-bot", "kubernetes")];
    let compose_mounts = service["volumes"].as_array().expect("credential mounts");
    assert_eq!(compose_mounts.len(), mounts.len());
    for (actual, expected) in compose_mounts.iter().zip(&mounts) {
        assert_eq!(actual["type"], "volume");
        assert_eq!(actual["source"], expected.volume);
        assert_eq!(actual["target"], expected.target);
        assert_eq!(actual["read_only"], true);
        assert_eq!(actual["volume"]["subpath"], expected.subpath);
        assert_eq!(actual["volume"]["nocopy"], true);
        assert_eq!(overlay["volumes"][&expected.volume]["external"], true);
        assert_eq!(
            overlay["volumes"][&expected.volume]["name"],
            expected.volume
        );
    }

    let absent = ConnectorSecretVolumes::new(&ConnectorsFileDecl::default());
    assert!(absent.mounts("kubernetes", &spec).is_err());
}

#[test]
fn connector_volumes_are_private_to_each_connector_and_bring_up() {
    let decl = ConnectorsFileDecl {
        connectors: BTreeMap::from([
            ("first".to_string(), kubernetes_spec()),
            ("second".to_string(), kubernetes_spec()),
        ]),
        ..Default::default()
    };
    let first = ConnectorSecretVolumes::new(&decl);
    let second = ConnectorSecretVolumes::new(&decl);
    assert_eq!(first.volumes.len(), 2);
    assert_eq!(second.volumes.len(), 2);
    assert_ne!(first.run_id, second.run_id);
    let unique: std::collections::BTreeSet<_> = first
        .volumes
        .values()
        .chain(second.volumes.values())
        .collect();
    assert_eq!(unique.len(), 4);
}

#[test]
fn secret_subpaths_hash_the_full_target_without_filesystem_traversal() {
    for target in [
        "/a/token",
        "/b/token",
        "/secrets/../../token",
        "/secrets/a,b$c",
    ] {
        let mut spec = kubernetes_spec();
        spec.secret_files = BTreeMap::from([("TOKEN".to_string(), target.to_string())]);
        let decl = ConnectorsFileDecl {
            connectors: BTreeMap::from([("kubernetes".to_string(), spec.clone())]),
            ..Default::default()
        };
        let volumes = ConnectorSecretVolumes::new(&decl);
        let mounts = volumes
            .mounts("kubernetes", &spec)
            .expect("credential mounts");
        assert_eq!(mounts[0].target, target);
        assert_eq!(
            mounts[0].subpath,
            Sha256::digest(target.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        assert_eq!(mounts[0].subpath.len(), 64);
        assert!(mounts[0]
            .subpath
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit()));
    }
}

#[test]
fn teardown_reaps_containers_then_secret_volumes_then_the_network() {
    let steps = connector_teardown_plan("acme-project", Some("acme-skill-net"));
    assert_eq!(steps.len(), 3, "{steps:?}");
    match &steps[0] {
        ConnectorTeardownStep::ReapLabeled(command) => {
            assert_eq!(command.program, "docker");
            let argv = command.argv();
            let filters = pairs_after(&argv, "--filter");
            for label in [
                CLI_MANAGED_LABEL.to_string(),
                CONNECTOR_COMPONENT_LABEL.to_string(),
                connector_project_label("acme-project"),
            ] {
                assert!(filters.contains(&format!("label={label}")), "{argv:?}");
            }
            assert!(argv.iter().any(|arg| arg == "-a" || arg == "--all"));
        }
        other => panic!("containers must be removed first: {other:?}"),
    }
    assert!(matches!(
        &steps[1],
        ConnectorTeardownStep::RemoveSecretVolumes(project) if project == "acme-project"
    ));
    match &steps[2] {
        ConnectorTeardownStep::RemoveNetwork(command) => {
            assert_eq!(command.argv(), vec!["network", "rm", "acme-skill-net"]);
        }
        other => panic!("the network must be removed last: {other:?}"),
    }
}

#[test]
fn teardown_without_a_network_still_collects_project_secret_volumes() {
    let steps = connector_teardown_plan("acme-project", None);
    assert_eq!(steps.len(), 2, "{steps:?}");
    assert!(matches!(&steps[0], ConnectorTeardownStep::ReapLabeled(_)));
    assert!(matches!(
        &steps[1],
        ConnectorTeardownStep::RemoveSecretVolumes(project) if project == "acme-project"
    ));
}

#[test]
fn local_down_collects_secret_volumes_without_a_bundle_directory() {
    let steps = connector_teardown_plan_for_down("acme-project");
    assert_eq!(steps.len(), 2, "{steps:?}");
    assert!(matches!(&steps[0], ConnectorTeardownStep::ReapLabeled(_)));
    assert!(matches!(
        &steps[1],
        ConnectorTeardownStep::RemoveSecretVolumes(project) if project == "acme-project"
    ));
}

// ─── The compose overlay (block B1-9) ────────────────────────────────────────

/// The `build:` form of a declaration, which is the only form a lock entry
/// stands for (ADR 0113): the digest in the lock is what building THIS source
/// resolved to, so a fixture that expects the overlay to be digest-pinned
/// declares the build rather than an `image:` the author would have to keep in
/// sync with it. `image` is cleared because exactly one of `image`/`build`/`url`
/// may be set.
fn built(mut spec: ConnectorSpecDecl, context: &str) -> ConnectorSpecDecl {
    spec.image = None;
    spec.build = Some(ConnectorBuildDecl {
        context: context.to_string(),
        dockerfile: "Dockerfile".to_string(),
        platforms: vec!["linux/amd64".to_string(), "linux/arm64".to_string()],
    });
    spec
}

fn overlay_fixture(agent: &str, plugin_dir: &Path) -> Value {
    let mut decl = ConnectorsFileDecl::default();
    decl.connectors.insert(
        "kubernetes".to_string(),
        built(kubernetes_spec(), "connectors/kubernetes"),
    );
    decl.connectors.insert(
        "k8s-write".to_string(),
        built(k8s_write_spec(), "connectors/k8s-write"),
    );

    let mut lock = ConnectorLockFileDecl {
        version: LOCK_VERSION,
        connectors: BTreeMap::new(),
        ..Default::default()
    };
    for (name, image) in [("kubernetes", TEMPO_IMAGE), ("k8s-write", WRITE_IMAGE)] {
        lock.connectors.insert(
            name.to_string(),
            ConnectorLockEntryDecl {
                image: image.to_string(),
                delivery: Delivery::Registry,
                platforms: vec!["linux/amd64".into(), "linux/arm64".into()],
                source_digest: "sha256:\
                                3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
            },
        );
    }

    let volumes = ConnectorSecretVolumes::new(&decl);
    let overlay = compose_overlay(
        &lock,
        &decl,
        &scope(agent),
        "curie",
        &volumes,
        Some("A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="),
    )
    .expect("generate the overlay");
    assert!(!overlay
        .to_string()
        .contains(&plugin_dir.display().to_string()));
    overlay
}

/// One service per hosted connector, pinned to the locked digest, joined to the
/// running stack's network under the Service-DNS alias, with no published port.
#[test]
fn the_overlay_declares_one_digest_pinned_service_per_hosted_connector() {
    let dir = TempDir::new().expect("a scratch bundle");
    let overlay = overlay_fixture("sre-bot", dir.path());
    let services = overlay["services"].as_object().expect("services");
    assert_eq!(services.len(), 2, "{overlay}");

    for connector in ["kubernetes", "k8s-write"] {
        let key = object_name("curie", "sre-bot", connector);
        let service = services
            .get(&key)
            .unwrap_or_else(|| panic!("a service named {key}: {overlay}"));

        let image = service["image"].as_str().expect("an image");
        assert!(
            image.contains("@sha256:"),
            "compose must start the SAME digest the cluster pulls: {image}"
        );
        assert!(
            service.get("ports").is_none(),
            "a connector is reachable by alias, never by host port: {service}"
        );

        let networks = service["networks"].as_object().expect("a networks mapping");
        assert_eq!(networks.len(), 1, "{service}");
        let (network_key, attachment) = networks.iter().next().expect("one network");
        let aliases: Vec<&str> = attachment["aliases"]
            .as_array()
            .expect("aliases")
            .iter()
            .map(|a| a.as_str().expect("a string"))
            .collect();
        assert!(
            aliases.contains(&service_dns("curie", "sre-bot", connector, "default").as_str()),
            "{service}"
        );

        let declared = &overlay["networks"][network_key];
        assert_eq!(
            declared["external"], true,
            "the overlay JOINS the stack's existing curie_runner network; creating its own \
             leaves the worker's sandbox unable to resolve the alias: {overlay}"
        );
        assert!(
            network_key.contains("curie_runner")
                || declared["name"]
                    .as_str()
                    .is_some_and(|n| n.contains("curie_runner")),
            "the network is the one the worker puts the runner on: {overlay}"
        );
    }
}

/// The overlay carries both teardown labels, since it is not itself
/// load-bearing for teardown.
#[test]
fn every_overlay_service_carries_the_teardown_labels() {
    let dir = TempDir::new().expect("a scratch bundle");
    let overlay = overlay_fixture("sre-bot", dir.path());

    for (_name, service) in overlay["services"].as_object().expect("services") {
        let labels = &service["labels"];
        assert_eq!(labels["curietech.ai/component"], "connector", "{service}");
        assert_eq!(labels["curietech.ai/project"], "curie", "{service}");
    }
}

/// The overlay's command, environment and mounts are the same rendered
/// contract the start spec emits: one set of semantics, two emitters.
#[test]
fn the_overlay_reproduces_the_same_args_env_and_secret_files() {
    let dir = TempDir::new().expect("a scratch bundle");
    let overlay = overlay_fixture("sre-bot", dir.path());
    let service = &overlay["services"][object_name("curie", "sre-bot", "kubernetes")];

    let command: Vec<&str> = service["command"]
        .as_array()
        .expect("a command list")
        .iter()
        .map(|a| a.as_str().expect("a string"))
        .collect();
    assert_eq!(
        command,
        kubernetes_spec().args,
        "dropping the args here restores the default tool surface exactly as it would in the \
         start spec: {service}"
    );

    let write = &overlay["services"][object_name("curie", "sre-bot", "k8s-write")];
    let env = &write["environment"];
    assert_eq!(
        env["K8S_WRITE_ALLOWLIST"], "acme-ns/checkout-api",
        "{write}"
    );
    assert_eq!(
        env["K8S_WRITE_SELF_URL"],
        format!(
            "http://{}:8000",
            service_dns("curie", "sre-bot", "k8s-write", "default")
        ),
        "{write}"
    );
    assert_eq!(
        env["K8S_WRITE_TOKEN"], "${K8S_WRITE_TOKEN}",
        "compose has no secretKeyRef, so a declared secret is resolved locally -- but the \
         overlay carries only the reference compose expands from its own environment, never \
         the resolved value: {write}"
    );

    let mounts = write["volumes"].as_array().expect("volumes");
    assert!(
        mounts.iter().any(|mount| {
            mount["type"] == "volume"
                && mount["target"] == "/secrets/kubeconfig"
                && mount["read_only"] == true
                && mount["volume"]["subpath"]
                    == Sha256::digest(b"/secrets/kubeconfig")
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                && mount["volume"]["nocopy"] == true
        }),
        "the credential file mounts read-only at its declared path: {mounts:?}"
    );
}

/// The overlay is ephemeral state under `.curie/`, so it never enters the
/// uploaded bundle and never perturbs the digest the ladder asserts.
#[test]
fn the_overlay_is_written_under_the_bundles_state_directory() {
    let dir = TempDir::new().expect("a scratch bundle");
    let path = compose_overlay_path(dir.path());
    assert_eq!(path, dir.path().join(".curie/compose.connectors.yaml"));

    let command = compose_up_command(&path, "curie", &BTreeMap::new(), Duration::from_secs(60));
    assert_eq!(command.program, "docker");
    let argv = command.argv();
    assert_eq!(
        argv.first().map(String::as_str),
        Some("compose"),
        "{argv:?}"
    );
    assert!(
        pairs_after(&argv, "-p").contains(&"curie".to_string()),
        "the services must join the RUNNING stack's project: {argv:?}"
    );
    assert!(
        pairs_after(&argv, "-f").contains(&path.display().to_string()),
        "{argv:?}"
    );
    assert!(argv.iter().any(|t| t == "--wait"), "{argv:?}");
    let wait_timeout = argv
        .iter()
        .position(|token| token == "--wait-timeout")
        .and_then(|index| argv.get(index + 1));
    assert_eq!(
        wait_timeout.map(String::as_str),
        Some("60"),
        "Compose --wait only has a finite connector-start deadline when its timeout is explicit: \
         {argv:?}"
    );
}

#[test]
fn the_up_command_uses_the_configured_connector_start_timeout() {
    let dir = TempDir::new().expect("a scratch bundle");
    let path = compose_overlay_path(dir.path());
    let command = compose_up_command(&path, "curie", &BTreeMap::new(), Duration::from_secs(80));
    let argv = command.argv();

    let wait_timeouts = pairs_after(&argv, "--wait-timeout");
    assert_eq!(
        wait_timeouts,
        vec!["80".to_string()],
        "Compose must receive the configured connector readiness window exactly once: {argv:?}"
    );
}

// ─── The overlay file holds references, the child env holds the values ───────

/// The bytes that actually land on disk carry the `${NAME}` reference and no
/// resolved value.
///
/// The overlay is written to a 0644 file under `.curie/` that outlives the
/// stack and every `local down`, so a value embedded here is a credential left
/// on disk indefinitely. Asserted on the SERIALIZED form, the way
/// `bring_up_local` writes it, rather than on the JSON tree: a value could
/// otherwise hide in a field the tree assertions do not name. The resolved value
/// is exported into this process first, so an emitter that reached for the
/// ambient value instead of emitting the reference fails here.
#[test]
fn the_serialized_overlay_carries_the_reference_and_never_the_resolved_value() {
    let dir = TempDir::new().expect("a scratch bundle");
    let _env = crate::support::ENV_LOCK.blocking_lock();
    let previous = std::env::var_os("K8S_WRITE_TOKEN");
    std::env::set_var("K8S_WRITE_TOKEN", "s3cr3t-value");
    let overlay = overlay_fixture("sre-bot", dir.path());
    let written = serde_norway::to_string(&overlay).expect("serialize the overlay");
    match previous {
        Some(value) => std::env::set_var("K8S_WRITE_TOKEN", value),
        None => std::env::remove_var("K8S_WRITE_TOKEN"),
    }

    assert!(
        written.contains("${K8S_WRITE_TOKEN}"),
        "compose expands the reference from its own environment: {written}"
    );
    assert!(
        !written.contains("s3cr3t-value"),
        "a resolved credential must never reach the overlay file: {written}"
    );
}

/// The value travels on the compose child's environment, masked, and never in
/// argv -- the one channel `${NAME}` in the overlay expands from.
#[test]
fn the_up_command_carries_the_secret_in_the_child_env_never_in_argv() {
    let dir = TempDir::new().expect("a scratch bundle");
    let path = compose_overlay_path(dir.path());
    let command = compose_up_command(
        &path,
        "curie",
        &BTreeMap::from([("K8S_WRITE_TOKEN".to_string(), "s3cr3t-value".to_string())]),
        Duration::from_secs(60),
    );

    assert!(
        command
            .secret_env
            .contains(&("K8S_WRITE_TOKEN".to_string(), "s3cr3t-value".to_string())),
        "compose reads `${{K8S_WRITE_TOKEN}}` from its own environment: {:?}",
        command.secret_env
    );
    let argv = command.argv();
    assert!(
        !argv.iter().any(|t| t.contains("s3cr3t-value")),
        "a credential in argv is readable from `ps -ef`: {argv:?}"
    );
    let printed = command.display();
    assert!(
        !printed.contains("s3cr3t-value"),
        "the printed command line masks the credential: {printed}"
    );
    assert!(
        printed.contains("K8S_WRITE_TOKEN=s3cr3t-v***"),
        "the operator still sees WHICH credential is applied: {printed}"
    );
}

/// The local tier refuses the whole bring-up when a declared secret has no value
/// here, instead of writing a `${NAME}` nothing will populate.
///
/// It used to skip an unresolved secret silently -- the check `skill up` has
/// always performed. With the overlay carrying references, skipping is worse
/// than before: compose would warn, expand the reference to empty, and start the
/// connector authenticating with nothing. The overlay file is asserted absent
/// because it is written before `docker compose` is invoked, so its absence is
/// the proof that the refusal came first and no container was ever started.
#[tokio::test]
async fn bring_up_local_refuses_a_declared_secret_with_no_value() {
    let dir = TempDir::new().expect("a scratch bundle");
    // Point the credential vault at an empty directory: the refusal must come
    // from the declaration having no value HERE, not from whatever this box's
    // operator happens to have stored (and the redirect keeps the test off the
    // OS credential store entirely).
    let _env = crate::support::ENV_LOCK.lock().await;
    let previous_config_dir = std::env::var_os("CURIE_CONFIG_DIR");
    std::env::set_var("CURIE_CONFIG_DIR", dir.path().join("config"));
    std::env::remove_var("CURIE_TEST_UNSET_CONNECTOR_TOKEN");
    std::fs::write(
        dir.path().join("connectors.yaml"),
        "connectors:\n  \
           gated:\n    \
             image: ghcr.io/acme-corp/gated:v1\n    \
             secrets:\n      \
               - CURIE_TEST_UNSET_CONNECTOR_TOKEN\n",
    )
    .expect("write connectors.yaml");

    let lock = ConnectorLockFileDecl {
        version: LOCK_VERSION,
        connectors: BTreeMap::new(),
        ..Default::default()
    };
    let error = curie::commands::bring_up_local(
        dir.path(),
        &curie::connector_build::load(dir.path()).unwrap(),
        &lock,
        &scope("sre-bot"),
        "curie",
    )
    .await
    .expect_err("a declared secret with no value must refuse the bring-up");
    match previous_config_dir {
        Some(value) => std::env::set_var("CURIE_CONFIG_DIR", value),
        None => std::env::remove_var("CURIE_CONFIG_DIR"),
    }

    let message = format!("{error:#}");
    assert!(
        message.contains("CURIE_TEST_UNSET_CONNECTOR_TOKEN"),
        "the refusal names the missing credential: {message}"
    );
    assert!(
        !compose_overlay_path(dir.path()).exists(),
        "the refusal must land before anything is written or started"
    );
}

// ─── Identity: one struct drives the alias and the runner scope (B1-11) ──────

/// `--agent <override>` moves the alias AND the runner scope together.
///
/// `DeployOpts.agent` and `DeployOpts.target` can both override the manifest's
/// name, and `object_name` is agent-scoped since #1116. A helper that
/// re-derived the name from `plugin.json` would alias the connectors under the
/// manifest identity while the runner dialed the override -- reachable at a
/// name nothing answers to, with no error anywhere. Passing the RESOLVED
/// identity in makes the divergence unrepresentable.
#[test]
fn an_agent_override_moves_the_alias_and_the_runner_scope_together() {
    let dir = TempDir::new().expect("a scratch bundle");
    let manifest_name = "sre-bot";
    let override_name = "sre-bot-prod";

    let overlay = overlay_fixture(override_name, dir.path());
    let services = overlay["services"].as_object().expect("services");
    assert!(
        services.contains_key(&object_name("curie", override_name, "kubernetes")),
        "the service is named for the OVERRIDE: {overlay}"
    );
    assert!(
        !services.contains_key(&object_name("curie", manifest_name, "kubernetes")),
        "the manifest name must not survive the override: {overlay}"
    );

    let alias = service_dns("curie", override_name, "kubernetes", "default");
    let attachment = &services[&object_name("curie", override_name, "kubernetes")]["networks"];
    assert!(
        attachment.to_string().contains(&alias),
        "{attachment} must alias {alias}"
    );

    let env: BTreeMap<String, String> = connector_scope_env(&scope(override_name))
        .into_iter()
        .collect();
    assert_eq!(
        env.get(env_keys::CURIE_CONNECTOR_AGENT),
        Some(&override_name.to_string()),
        "the runner scope must carry the same identity the alias was built from: {env:?}"
    );
    assert_eq!(
        env.get(env_keys::CURIE_CONNECTOR_RELEASE),
        Some(&"curie".to_string()),
        "{env:?}"
    );
    assert_eq!(
        env.get(env_keys::CURIE_CONNECTOR_NAMESPACE),
        Some(&"default".to_string()),
        "{env:?}"
    );
    assert_eq!(
        env.len(),
        3,
        "the scope is all-or-nothing: a partial set is dropped entirely by the boot contract, \
         so emitting two of three silently switches connectors off: {env:?}"
    );
}

// ─── RunnerState records what boot created (block B1-6) ──────────────────────

fn runner_state(dir: &Path) -> curie::state::RunnerState {
    curie::state::RunnerState {
        container_id: "abc123".to_string(),
        container_name: "curie-runner-local".to_string(),
        image: "curie-runner".to_string(),
        port: 7245,
        base_url: "http://127.0.0.1:7245".to_string(),
        session_id: "curie-skill-abc123".to_string(),
        plugin_dir: dir.display().to_string(),
        fake_model: false,
        ollama_container: None,
        network: Some("curie-skill-net".to_string()),
        model_base_url: None,
        bundle_digest: None,
        bundle_snapshot_dir: None,
        connector_containers: vec!["conn-kubernetes".to_string(), "conn-k8s-write".to_string()],
        connector_network: Some("curie-skill-net".to_string()),
    }
}

/// `skill up` records the connector containers and the network so `skill down`
/// reaps exactly what boot created.
///
/// Without the record, a run that dies mid-turn leaves labeled containers
/// holding a resolved credential -- the failure class #747 already fixed once
/// for the runner itself.
#[test]
fn runner_state_round_trips_the_connector_containers_and_network() {
    let dir = TempDir::new().expect("a scratch bundle");
    curie::state::save(dir.path(), &runner_state(dir.path())).expect("save the state");

    let loaded = curie::state::load(dir.path())
        .expect("load the state")
        .expect("the state exists");
    assert_eq!(
        loaded.connector_containers,
        vec!["conn-kubernetes".to_string(), "conn-k8s-write".to_string()]
    );
    assert_eq!(
        loaded.connector_network,
        Some("curie-skill-net".to_string())
    );
}

/// A state file written before this change still loads.
///
/// `skill down` on a runner booted by the previous CLI must not fail to parse
/// its own state file and orphan the runner it was asked to reap.
#[test]
fn a_state_file_predating_connectors_still_loads() {
    let dir = TempDir::new().expect("a scratch bundle");
    std::fs::create_dir_all(dir.path().join(".curie")).expect("mkdir .curie");
    std::fs::write(
        dir.path().join(".curie/runner.json"),
        r#"{"container_id":"abc123","container_name":"curie-runner-local",
            "image":"curie-runner","port":7245,"base_url":"http://127.0.0.1:7245",
            "session_id":"s","plugin_dir":"/tmp/b","fake_model":false}"#,
    )
    .expect("write a legacy state file");

    let loaded = curie::state::load(dir.path())
        .expect("a legacy state file still parses")
        .expect("the state exists");
    assert!(loaded.connector_containers.is_empty());
    assert_eq!(loaded.connector_network, None);
}

// ─── The packer ships the lock and never the overlay ─────────────────────────

/// `connectors.lock.yaml` reaches the platform; `.curie/` does not.
///
/// Both halves matter. Without the lock the API cannot resolve the `build:`
/// declaration and intake refuses the bundle. The overlay is runtime state
/// tied to private daemon volumes and must stay outside the upload.
///
/// Listed through `tar` rather than a decoder because `flate2`/`tar` are
/// library dependencies, not dev-dependencies, and this target must not add
/// any.
#[test]
fn the_packed_bundle_carries_the_lock_and_no_runtime_state() {
    let dir = TempDir::new().expect("a scratch bundle");
    curie::scaffold::scaffold(dir.path(), "sre-bot").expect("scaffold a bundle");
    std::fs::write(
        dir.path().join("connectors.lock.yaml"),
        "version: 1\nconnectors: {}\n",
    )
    .expect("write the lock");
    std::fs::create_dir_all(dir.path().join(".curie")).expect("mkdir .curie");
    std::fs::write(compose_overlay_path(dir.path()), "services: {}\n").expect("write the overlay");

    let archive = dir.path().join("bundle.tar.gz");
    std::fs::write(
        &archive,
        curie::bundle::pack_tar_gz(dir.path()).expect("pack the bundle"),
    )
    .expect("write the archive");

    let listing = std::process::Command::new("tar")
        .arg("-tzf")
        .arg(&archive)
        .output()
        .expect("list the archive with tar");
    assert!(listing.status.success(), "{listing:?}");
    let names = String::from_utf8_lossy(&listing.stdout);

    assert!(
        names
            .lines()
            .any(|n| n.trim_start_matches("./") == "connectors.lock.yaml"),
        "the lock must reach the platform or intake refuses the bundle: {names}"
    );
    assert!(
        !names.contains(".curie"),
        "runtime state must never enter the upload: {names}"
    );
}
