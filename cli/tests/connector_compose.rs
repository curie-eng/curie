//! RED contract for PR B / Stream B1: running a locked connector locally.
//!
//! Two emitters, one set of semantics. The skill tier starts each hosted
//! connector with `docker run` (`docker::ConnectorStartSpec`, block B1-4); the
//! local tier generates a compose overlay and brings it up (block B1-9). Both
//! must reproduce `connector_render.render_deployment` field for field --
//! substituted `args`, substituted `env`, declared secrets, `secret_files`,
//! the hardening set, and the port -- because the runner derives the URL it
//! dials from the PYTHON copy. An approximation here does not degrade: it
//! either times out against a name no container owns, or (the review's finding
//! 5) silently drops `args` and restores general write plus the
//! credential-exposing `configuration_view` tool.
//!
//! This file also carries the three smaller B1 pins that have nowhere better
//! to live: `RunnerState` recording what boot created, the packer's treatment
//! of the lock and the overlay, and the teardown ordering.
//!
//! Everything is pure. Nothing here starts a container, and the credential
//! staging is asserted at the filesystem level (write it, read the exact bytes
//! back, wipe it, assert it is gone) rather than by inspecting flags -- an
//! argv assertion passes against an empty mount, which is the failure this
//! replaces. The daemon-backed half of that proof is the ladder's job (B3).
//!
//! One test calls a function that WOULD reach the daemon --
//! `bring_up_local_refuses_a_declared_secret_with_no_value` -- precisely because
//! the property under test is that it refuses before getting there. Its
//! assertion that no overlay file was written is what pins the refusal's
//! position; a regression there turns it into a slow test, which is the signal.
//!
//! Surface the implementer must add:
//!
//! ```ignore
//! // curie::connector_build
//! pub struct ConnectorScope { pub release: String, pub agent: String, pub namespace: String }
//! pub fn connector_substitutions(
//!     scope: &ConnectorScope, connector: &str, port: u32,
//! ) -> BTreeMap<String, String>;
//! pub fn substitute(value: &str, subs: &BTreeMap<String, String>) -> String;
//! pub fn connector_scope_env(scope: &ConnectorScope) -> Vec<(String, String)>;
//! pub fn connector_secrets_root(plugin_dir: &Path) -> PathBuf;
//! pub fn stage_secret_file(
//!     plugin_dir: &Path, connector: &str, declared_path: &str, value: &str,
//! ) -> anyhow::Result<PathBuf>;
//! pub fn wipe_connector_secrets(plugin_dir: &Path) -> anyhow::Result<()>;
//! pub fn compose_overlay_path(plugin_dir: &Path) -> PathBuf;
//! pub fn compose_overlay(
//!     lock: &ConnectorLockFileDecl,
//!     decl: &ConnectorsFileDecl,
//!     identity: &ConnectorScope,
//!     project: &str,
//!     plugin_dir: &Path,
//! ) -> anyhow::Result<serde_json::Value>;
//! /// Resolved secret values handed to the compose child's environment only,
//! /// where the overlay's `${NAME}` references expand from; the file on disk
//! /// never holds a value.
//! pub fn compose_up_command(
//!     overlay: &Path, project: &str, secret_values: &BTreeMap<String, String>,
//! ) -> curie::ops::OpsCommand;
//!
//! // curie::docker
//! pub const CONNECTOR_COMPONENT_LABEL: &str = "curietech.ai/component=connector";
//! pub fn connector_project_label(project: &str) -> String;
//! pub struct ConnectorStartSpec {
//!     // .. image / container_name / network / alias / args / env ..
//!     /// Resolved secret values handed to the Docker CLI child process only,
//!     /// forwarded by bare `-e NAME` so they never enter argv. Same rule
//!     /// `StartSpec::docker_env` already follows.
//!     pub docker_env: Vec<(String, String)>,
//! }
//! impl ConnectorStartSpec {
//!     pub fn from_declaration(
//!         connector: &str,
//!         spec: &ConnectorSpecDecl,
//!         image: &str,
//!         identity: &ConnectorScope,
//!         network: &str,
//!         project: &str,
//!         plugin_dir: &Path,
//!         secret_values: &BTreeMap<String, String>,
//!     ) -> anyhow::Result<Self>;
//!     pub fn run_args(&self) -> Vec<String>;
//! }
//! pub enum ConnectorTeardownStep {
//!     ReapLabeled(OpsCommand), RemoveNetwork(OpsCommand), WipeSecrets(PathBuf),
//! }
//! pub fn connector_teardown_plan(
//!     project: &str, network: Option<&str>, plugin_dir: Option<&Path>,
//! ) -> Vec<ConnectorTeardownStep>;
//!
//! // curie::local
//! /// The plan `local down` runs: the label-scoped reap, plus the staged tree
//! /// when the directory it ran from holds one.
//! pub fn connector_teardown_plan_for_down(cwd: &Path) -> Vec<ConnectorTeardownStep>;
//!
//! // curie::state::RunnerState
//! pub connector_containers: Vec<String>,
//! pub connector_network: Option<String>,
//! ```

use std::collections::BTreeMap;
use std::path::Path;

use curie::connector_build::{
    compose_overlay, compose_overlay_path, compose_up_command, connector_scope_env,
    connector_secrets_root, connector_substitutions, object_name, service_dns, stage_secret_file,
    staged_secret_path, substitute, ConnectorBuildDecl, ConnectorLockEntryDecl,
    ConnectorLockFileDecl, ConnectorScope, ConnectorSpecDecl, ConnectorsFileDecl, Delivery,
    SecretDecl, LOCK_VERSION,
};
use curie::docker::{
    connector_project_label, connector_teardown_plan, run_connector_teardown, ConnectorStartSpec,
    ConnectorTeardownStep, CLI_MANAGED_LABEL, CONNECTOR_COMPONENT_LABEL,
};
use curie::local::connector_teardown_plan_for_down;
use curie_aci_protocol::env_keys;
use serde_json::Value;
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
    ConnectorStartSpec::from_declaration(
        connector,
        spec,
        image,
        identity,
        "curie-skill-net",
        "curie-skill-abc123",
        plugin_dir,
        secret_values,
    )
    .unwrap_or_else(|error| panic!("start spec for {connector}: {error:#}"))
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

// ─── Secrets: never in argv, readable on disk, gone after teardown ───────────

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
    let dir = TempDir::new().expect("a scratch bundle");
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
        dir.path(),
        &BTreeMap::new(),
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

/// The credential file is staged at a DETERMINISTIC path, holds exactly the
/// resolved value, and is bind-mounted read-only where the declaration says.
///
/// Deterministic, not a per-run tempdir: teardown is label-driven and a reap
/// that never inspected a container cannot discover a random path, so the
/// credential would survive every `down` (review round 3, finding r3-9). The
/// content assertion is what an argv check cannot make -- an empty mount
/// satisfies every flag.
#[test]
fn a_secret_file_is_staged_readable_and_mounted_where_declared() {
    let dir = TempDir::new().expect("a scratch bundle");
    let staged = stage_secret_file(
        dir.path(),
        "kubernetes",
        "/secrets/kubeconfig",
        "apiVersion: v1\nkind: Config\n",
    )
    .expect("stage the credential");

    let connector_dir = connector_secrets_root(dir.path()).join("kubernetes");
    assert_eq!(
        staged.parent(),
        Some(connector_dir.as_path()),
        "the path is recomputable by `skill down` from the plugin dir alone"
    );
    assert!(
        staged
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("kubeconfig-")),
        "the declared name still leads, so an operator can read the tree: {staged:?}"
    );
    assert!(
        connector_secrets_root(dir.path()).starts_with(dir.path().join(".curie")),
        "the tree lives under .curie/, which the packer excludes and the scaffold gitignores"
    );
    assert_eq!(
        std::fs::read_to_string(&staged).expect("read the staged credential"),
        "apiVersion: v1\nkind: Config\n",
        "an empty file mounts just as successfully as a correct one"
    );

    let argv = start_spec(
        "kubernetes",
        &kubernetes_spec(),
        TEMPO_IMAGE,
        &scope("sre-bot"),
        dir.path(),
        &BTreeMap::new(),
    )
    .run_args();
    assert!(
        pairs_after(&argv, "-v")
            .iter()
            .any(|m| m == &format!("{}:/secrets/kubeconfig:ro", staged.display())),
        "the declared mount path is the container's, not the host's: {argv:?}"
    );
}

/// Two `secret_files` entries sharing a basename must not stage over each other.
///
/// `/a/token` and `/b/token` are two different credentials in the container, and
/// a name derived from the basename alone silently leaves the connector holding
/// the second value at both mount points.
#[test]
fn two_secret_files_sharing_a_basename_stage_to_distinct_paths() {
    let dir = TempDir::new().expect("a scratch bundle");
    let first =
        stage_secret_file(dir.path(), "kubernetes", "/a/token", "alpha").expect("stage /a/token");
    let second =
        stage_secret_file(dir.path(), "kubernetes", "/b/token", "bravo").expect("stage /b/token");

    assert_ne!(first, second, "one basename, two declarations, two files");
    assert_eq!(
        std::fs::read_to_string(&first).expect("read the first credential"),
        "alpha",
        "staging the second must not overwrite the first"
    );
    assert_eq!(
        std::fs::read_to_string(&second).expect("read the second credential"),
        "bravo"
    );
}

/// Restaging replaces the hardened file. The first staging leaves 0400/0444,
/// so a plain write onto it is EACCES and a redeploy or rotation would wedge;
/// deleting the replace-before-write from `stage_secret_file` fails this.
#[test]
fn restaging_replaces_the_hardened_credential() {
    let dir = TempDir::new().expect("a scratch bundle");
    let first = stage_secret_file(dir.path(), "kubernetes", "/secrets/kubeconfig", "old-creds")
        .expect("first staging");
    let again = stage_secret_file(dir.path(), "kubernetes", "/secrets/kubeconfig", "new-creds")
        .expect("restaging over the hardened file must succeed");
    assert_eq!(first, again, "one declaration, one staged path");
    assert_eq!(
        std::fs::read_to_string(&again).expect("read the rotated credential"),
        "new-creds",
        "rotation must land the new value"
    );
}

/// A declared path is the CONTAINER's, so nothing constrains its shape. Every
/// one of these must still land inside the connector's own directory: the
/// staged path is a host filesystem write, and the bundle author picks its input.
#[test]
fn a_hostile_declared_path_cannot_escape_the_connector_directory() {
    let dir = TempDir::new().expect("a scratch bundle");
    let expected_parent = connector_secrets_root(dir.path()).join("kubernetes");

    for declared in ["..", "../../evil", "/secrets/../../evil", "c:\\evil", "/"] {
        let staged = staged_secret_path(dir.path(), "kubernetes", declared);
        assert_eq!(
            staged.parent(),
            Some(expected_parent.as_path()),
            "{declared:?} escaped to {staged:?}"
        );
    }
}

/// The parent directory is 0700, so the fallback file mode is still private.
///
/// Rootless Docker and Docker Desktop map uids differently, so the `chown` to
/// 65532 is not always permitted; where it is not, the file falls back to 0444
/// and the private parent is the only thing keeping the credential unreadable.
/// That is why the parent mode is load-bearing rather than tidiness.
#[cfg(unix)]
#[test]
fn the_staging_tree_is_private_even_when_the_file_must_be_world_readable() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().expect("a scratch bundle");
    let staged = stage_secret_file(dir.path(), "kubernetes", "/secrets/kubeconfig", "creds")
        .expect("stage the credential");

    let root_mode = std::fs::metadata(connector_secrets_root(dir.path()))
        .expect("stat the root")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(root_mode, 0o700, "connector-secrets/ must be private");

    let connector_mode = std::fs::metadata(staged.parent().expect("a parent"))
        .expect("stat the connector dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(connector_mode, 0o700, "each connector's subdirectory too");

    let file_mode = std::fs::metadata(&staged)
        .expect("stat the file")
        .permissions()
        .mode()
        & 0o777;
    assert!(
        file_mode == 0o400 || file_mode == 0o444,
        "0400 when chown succeeded, 0444 inside the 0700 parent when it could not; got {file_mode:o}"
    );
}

/// Teardown removes the tree. Deleting the `WipeSecrets` arm must fail this.
///
/// Driven through `run_connector_teardown`, the executor production runs, not
/// through the helper it calls -- a test of the helper alone stays green while
/// the arm that reaches it is gone. The step is built on its own because its
/// two siblings shell to docker.
#[tokio::test]
async fn teardown_wipes_the_staged_credentials() {
    let dir = TempDir::new().expect("a scratch bundle");
    stage_secret_file(dir.path(), "kubernetes", "/secrets/kubeconfig", "creds")
        .expect("stage the credential");
    let root = connector_secrets_root(dir.path());
    assert!(root.exists());

    let steps = vec![ConnectorTeardownStep::WipeSecrets(root.clone())];
    assert!(
        run_connector_teardown(&steps).await.is_empty(),
        "the wipe must report no problem"
    );
    assert!(
        !root.exists(),
        "a resolved credential must not outlive the containers that used it"
    );

    assert!(
        run_connector_teardown(&steps).await.is_empty(),
        "a second wipe is a no-op; `down` after `down` must not error"
    );
}

// ─── Teardown ordering (block B1-8) ──────────────────────────────────────────

/// Containers first, then the network, then the credential tree.
///
/// The order is the property: a network still attached to a running container
/// cannot be removed, and a tree still bind-mounted into one cannot be wiped
/// cleanly. Reaping is label-scoped rather than file-scoped because
/// `LocalDownOpts` carries no plugin directory and a `down` run from another
/// directory must still reap (review round 2, finding r2-4).
#[test]
fn teardown_reaps_by_label_then_removes_the_network_then_wipes_the_tree() {
    let dir = TempDir::new().expect("a scratch bundle");
    let steps = connector_teardown_plan("curie", Some("curie-skill-net"), Some(dir.path()));

    assert_eq!(steps.len(), 3, "{steps:?}");

    match &steps[0] {
        ConnectorTeardownStep::ReapLabeled(command) => {
            assert_eq!(command.program, "docker");
            let argv = command.argv();
            let filters = pairs_after(&argv, "--filter");
            assert!(
                filters
                    .iter()
                    .any(|f| f == &format!("label={CONNECTOR_COMPONENT_LABEL}")),
                "{argv:?}"
            );
            assert!(
                filters
                    .iter()
                    .any(|f| f == &format!("label={}", connector_project_label("curie"))),
                "the project label is what keeps two concurrent stacks apart: {argv:?}"
            );
            assert!(
                argv.iter().any(|t| t == "-a" || t == "--all"),
                "a stopped connector still holds the mount: {argv:?}"
            );
        }
        other => panic!("the first step must reap containers, got {other:?}"),
    }

    match &steps[1] {
        ConnectorTeardownStep::RemoveNetwork(command) => {
            let argv = command.argv();
            assert!(argv.iter().any(|t| t == "curie-skill-net"), "{argv:?}");
        }
        other => panic!("the network comes after the containers, got {other:?}"),
    }

    match &steps[2] {
        ConnectorTeardownStep::WipeSecrets(path) => {
            assert_eq!(path, &connector_secrets_root(dir.path()));
        }
        other => panic!("the credential tree is wiped last, got {other:?}"),
    }
}

/// `curie local down` from any directory still reaps, with no plugin dir and no
/// network of its own to remove.
#[test]
fn teardown_without_a_plugin_dir_still_reaps_the_containers() {
    let steps = connector_teardown_plan("curie", None, None);
    assert_eq!(steps.len(), 1, "{steps:?}");
    assert!(
        matches!(steps[0], ConnectorTeardownStep::ReapLabeled(_)),
        "{steps:?}"
    );
}

/// `curie local down` run from the bundle's own directory also removes the tree
/// a `local deploy` staged there.
///
/// The local tier records the deployed bundle's directory nowhere, so the
/// resolution is the directory `down` ran from -- the one `local deploy`
/// defaults `--plugin-dir` to. Built through the same resolver `down` calls, so
/// what stays unpinned here is only its `current_dir()` hand-off. The wipe is
/// run on its own because its two siblings shell to docker.
#[tokio::test]
async fn local_down_wipes_the_staged_tree_of_the_bundle_it_was_run_from() {
    let dir = TempDir::new().expect("a scratch bundle");
    stage_secret_file(dir.path(), "kubernetes", "/secrets/kubeconfig", "creds")
        .expect("stage the credential");
    let root = connector_secrets_root(dir.path());

    let steps = connector_teardown_plan_for_down(dir.path());
    assert_eq!(
        steps.len(),
        2,
        "the reap is unchanged; the wipe is added: {steps:?}"
    );
    assert!(
        matches!(steps[0], ConnectorTeardownStep::ReapLabeled(_)),
        "containers first, or the tree is still bind-mounted: {steps:?}"
    );
    match &steps[1] {
        ConnectorTeardownStep::WipeSecrets(path) => assert_eq!(path, &root),
        other => panic!("the staged tree is wiped last, got {other:?}"),
    }

    assert!(
        run_connector_teardown(&[ConnectorTeardownStep::WipeSecrets(root.clone())])
            .await
            .is_empty(),
        "the wipe must report no problem"
    );
    assert!(
        !root.exists(),
        "a resolved credential must not outlive `curie local down`"
    );
}

/// A `down` from a directory holding no staged tree plans no wipe: the
/// resolution never guesses at another bundle's credentials.
#[test]
fn local_down_plans_no_wipe_without_a_staged_tree() {
    let dir = TempDir::new().expect("a directory that is not a staged bundle");
    let steps = connector_teardown_plan_for_down(dir.path());
    assert_eq!(steps.len(), 1, "{steps:?}");
    assert!(
        matches!(steps[0], ConnectorTeardownStep::ReapLabeled(_)),
        "{steps:?}"
    );
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

    compose_overlay(&lock, &decl, &scope(agent), "curie", plugin_dir).expect("generate the overlay")
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

    let mounts: Vec<&str> = write["volumes"]
        .as_array()
        .expect("volumes")
        .iter()
        .map(|v| v.as_str().expect("a string"))
        .collect();
    assert!(
        mounts
            .iter()
            .any(|m| m.ends_with(":/secrets/kubeconfig:ro")),
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

    let command = compose_up_command(&path, "curie", &BTreeMap::new());
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
    std::env::set_var("K8S_WRITE_TOKEN", "s3cr3t-value");
    let overlay = overlay_fixture("sre-bot", dir.path());
    let written = serde_norway::to_string(&overlay).expect("serialize the overlay");

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
    };
    let error = curie::commands::bring_up_local(dir.path(), &lock, &scope("sre-bot"), "curie")
        .await
        .expect_err("a declared secret with no value must refuse the bring-up");

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
/// declaration and intake refuses the bundle. With the overlay, a resolved
/// credential (staged under the same `.curie/`) would ride the upload into
/// stored bundle bytes.
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
    stage_secret_file(dir.path(), "kubernetes", "/secrets/kubeconfig", "creds")
        .expect("stage a credential");

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
        "runtime state, including a staged credential, must never enter the upload: {names}"
    );
}
