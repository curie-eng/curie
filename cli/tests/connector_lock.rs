//! RED contract for PR B / Stream B1: the deploy-time lock preflight.
//!
//! `curie local deploy` and `curie cluster deploy` share one `commands::deploy`,
//! so both reach the same preflight and it discriminates on an explicit tier
//! (block B1-10's `DeployOpts::tier`). The rules it enforces, per B1-7:
//!
//!   both tiers   -- a `build:` connector with no lock entry, or with a
//!                   `source_digest` that no longer matches the tree, is
//!                   refused naming `curie build --plugin-dir <dir>`;
//!   cluster only -- a `delivery: local-daemon` lock is refused naming
//!                   `--registry <ref>`. Intake deliberately accepts either
//!                   delivery, because a local deploy legitimately uploads a
//!                   local-daemon lock (review round 2, finding r2-3);
//!   cluster only -- the REGISTRY is queried for the manifest the lock names,
//!                   and the deploy is refused when the digest does not resolve
//!                   (AC B6) or the resolved index does not cover every
//!                   architecture the nodes report (AC B7).
//!
//! Purity. The decision functions take canned inputs -- a recomputed digest
//! map, the raw `imagetools inspect --raw` body, the node architecture set --
//! so no registry and no cluster is contacted here. What is asserted about the
//! impure half is only the argv it would issue. That split is the same one
//! `cli/CLAUDE.md:133-142` already requires of every operator verb.
//!
//! Exit class. All five refusals are `ExitClass::Usage` (exit 2). The operative
//! clause is ADR-0021's own: "retrying the same argv will fail identically, so
//! fix the input". A missing lock, a stale lock, a local-daemon lock at
//! cluster, a deleted package and a single-arch index are all conditions no
//! retry clears -- each needs the operator to do something first, and each
//! carries the `fix` line that says what. Nothing here is Transient: none of
//! them is an endpoint that might come back up.
//!
//! Surface the implementer must add to `curie::commands`:
//!
//! ```ignore
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub enum DeployTier { Local, Cluster }        // no Default impl, by design
//!
//! pub fn lock_preflight(
//!     plugin_dir: &Path,
//!     decl: &ConnectorsFileDecl,
//!     lock: Option<&ConnectorLockFileDecl>,
//!     recomputed: &BTreeMap<String, String>,   // connector -> source_digest
//!     tier: DeployTier,
//! ) -> anyhow::Result<()>;
//!
//! pub fn registry_manifest_argv(image: &str) -> curie::ops::OpsCommand;
//! pub fn node_architectures_argv() -> curie::ops::OpsCommand;
//! pub fn node_architectures(raw: &str) -> BTreeSet<String>;
//! pub fn manifest_platforms(raw: &str) -> anyhow::Result<BTreeSet<String>>;
//! pub fn registry_preflight(
//!     image: &str,
//!     inspect: Result<&str, String>,           // Ok(raw manifest) | Err(stderr)
//!     node_architectures: &BTreeSet<String>,
//!     declared_platforms: &[String],
//! ) -> anyhow::Result<()>;
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use curie::commands::{
    lock_preflight, manifest_platforms, node_architectures, node_architectures_argv,
    registry_manifest_argv, registry_preflight, registry_preflight_targets, DeployOpts, DeployTier,
    WorkspaceIntent,
};
use curie::connector_build::{
    ConnectorBuildDecl, ConnectorLockEntryDecl, ConnectorLockFileDecl, ConnectorSpecDecl,
    ConnectorsFileDecl, Delivery, LOCK_VERSION,
};
use curie::exit::{classify, error_json, ExitClass};

const FRESH: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const STALE: &str = "sha256:9999999999999999999999999999999999999999999999999999999999999999";
const PUSHED: &str = "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
                      2222222222222222222222222222222222222222222222222222222222222222";
const LOCAL_ID: &str = "sha256:4444444444444444444444444444444444444444444444444444444444444444";

// ─── Fixtures ────────────────────────────────────────────────────────────────

fn declaring_a_build(connector: &str) -> ConnectorsFileDecl {
    let mut connectors = BTreeMap::new();
    connectors.insert(
        connector.to_string(),
        ConnectorSpecDecl {
            build: Some(ConnectorBuildDecl {
                context: format!("connectors/{connector}"),
                dockerfile: "Dockerfile".to_string(),
                platforms: vec!["linux/amd64".into(), "linux/arm64".into()],
            }),
            ..Default::default()
        },
    );
    ConnectorsFileDecl { connectors }
}

fn declaring_an_image(connector: &str) -> ConnectorsFileDecl {
    let mut connectors = BTreeMap::new();
    connectors.insert(
        connector.to_string(),
        ConnectorSpecDecl {
            image: Some("ghcr.io/containers/kubernetes-mcp-server:v0.0.66".to_string()),
            ..Default::default()
        },
    );
    ConnectorsFileDecl { connectors }
}

fn lock_for(
    connector: &str,
    image: &str,
    delivery: Delivery,
    digest: &str,
) -> ConnectorLockFileDecl {
    let mut connectors = BTreeMap::new();
    connectors.insert(
        connector.to_string(),
        ConnectorLockEntryDecl {
            image: image.to_string(),
            delivery,
            platforms: vec!["linux/amd64".into(), "linux/arm64".into()],
            source_digest: digest.to_string(),
        },
    );
    ConnectorLockFileDecl {
        version: LOCK_VERSION,
        connectors,
    }
}

fn recomputed(connector: &str, digest: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(connector.to_string(), digest.to_string())])
}

fn plugin_dir() -> PathBuf {
    PathBuf::from("/home/operator/agents/sre-bot")
}

/// Every refusal is a Usage error carrying a non-empty `fix`, rendered into the
/// `{"error","fix"}` body ADR-0021 freezes. Asserted through the real
/// `exit::classify` / `exit::error_json`, not a hand-rolled reimplementation of
/// them, so a refusal that forgot to tag itself as a `CliError` fails here.
fn assert_actionable_usage_refusal(error: &anyhow::Error, must_name: &[&str]) {
    let (class, fix) = classify(error);
    assert_eq!(
        class,
        ExitClass::Usage,
        "a lock refusal is a deterministic input error (exit 2): {error:#}"
    );
    let fix = fix.expect("every lock refusal names the command that clears it");

    let json = error_json(error);
    let body = json.as_object().expect("the error payload is an object");
    assert!(body.contains_key("error"), "{json}");
    assert!(body.contains_key("fix"), "{json}");
    assert!(
        !json["fix"].is_null(),
        "a null fix leaves the operator with nowhere to go: {json}"
    );

    let haystack = format!("{}\n{}", json["error"], fix);
    for needle in must_name {
        assert!(
            haystack.contains(needle),
            "the refusal must name {needle:?}; got {haystack}"
        );
    }
}

// ─── Both tiers ──────────────────────────────────────────────────────────────

/// A bundle with no `build:` connector never reaches any of this.
///
/// The overwhelmingly common bundle declares `image:` or nothing at all, and a
/// preflight that fired for it would break every existing deploy.
#[test]
fn a_bundle_with_no_build_connectors_is_a_no_op_at_both_tiers() {
    for tier in [DeployTier::Local, DeployTier::Cluster] {
        lock_preflight(
            &plugin_dir(),
            &declaring_an_image("kubernetes"),
            None,
            &BTreeMap::new(),
            tier,
        )
        .unwrap_or_else(|error| {
            panic!("an image-only bundle must not be gated ({tier:?}): {error:#}")
        });
    }
}

/// A `build:` connector with no lock at all is refused, at BOTH tiers.
///
/// This is the CLI mirror of the platform's `connectors.lock_missing` intake
/// rule (block A17). Without it the operator learns only after the upload, from
/// a rejection whose message is about a file they were never told to make.
#[test]
fn a_build_connector_with_no_lock_is_refused_at_both_tiers() {
    for tier in [DeployTier::Local, DeployTier::Cluster] {
        let error = lock_preflight(
            &plugin_dir(),
            &declaring_a_build("tempo"),
            None,
            &recomputed("tempo", FRESH),
            tier,
        )
        .expect_err("a lockless build bundle must be refused");
        assert_actionable_usage_refusal(&error, &["tempo", "curie build --plugin-dir", "sre-bot"]);
    }
}

/// A lock that exists but omits this connector is the same failure.
///
/// A whole-file existence check would pass here while the connector it needs
/// has no resolved image at all -- the exact shape a partially-successful build
/// leaves behind.
#[test]
fn a_lock_missing_this_connectors_entry_is_refused() {
    let mut decl = declaring_a_build("tempo");
    decl.connectors
        .extend(declaring_a_build("k8s-write").connectors);

    let error = lock_preflight(
        &plugin_dir(),
        &decl,
        Some(&lock_for("tempo", PUSHED, Delivery::Registry, FRESH)),
        &BTreeMap::from([
            ("tempo".to_string(), FRESH.to_string()),
            ("k8s-write".to_string(), FRESH.to_string()),
        ]),
        DeployTier::Cluster,
    )
    .expect_err("a lock covering only one of two build connectors must be refused");
    assert_actionable_usage_refusal(&error, &["k8s-write", "curie build --plugin-dir"]);
}

/// A source edited since the lock was written is refused, at BOTH tiers.
///
/// The stale case is the one that would otherwise deploy: the image resolves,
/// the pull succeeds, and the operator watches a container running code they
/// changed an hour ago. The lock's recorded digest and the recomputed one are
/// the only things that disagree.
#[test]
fn a_stale_source_digest_is_refused_at_both_tiers() {
    for tier in [DeployTier::Local, DeployTier::Cluster] {
        let error = lock_preflight(
            &plugin_dir(),
            &declaring_a_build("tempo"),
            Some(&lock_for("tempo", PUSHED, Delivery::Registry, STALE)),
            &recomputed("tempo", FRESH),
            tier,
        )
        .expect_err("a stale lock must be refused");
        assert_actionable_usage_refusal(&error, &["tempo", "curie build --plugin-dir"]);
    }
}

/// A fresh registry lock passes at both tiers. The gate has an open state.
#[test]
fn a_fresh_registry_lock_passes_at_both_tiers() {
    for tier in [DeployTier::Local, DeployTier::Cluster] {
        lock_preflight(
            &plugin_dir(),
            &declaring_a_build("tempo"),
            Some(&lock_for("tempo", PUSHED, Delivery::Registry, FRESH)),
            &recomputed("tempo", FRESH),
            tier,
        )
        .unwrap_or_else(|error| panic!("a fresh registry lock must deploy ({tier:?}): {error:#}"));
    }
}

// ─── Cluster only: the delivery rule ─────────────────────────────────────────

/// A `local-daemon` lock deploys locally and is refused at cluster.
///
/// One test, both halves, because the pair IS the property (AC B11): a rule
/// stated only as "cluster refuses" would pass just as well if local refused
/// too, and that variant breaks the fast path this whole delivery mode exists
/// for. Cluster nodes cannot see the operator's Docker daemon; a local run
/// legitimately can.
#[test]
fn a_local_daemon_lock_deploys_locally_and_is_refused_at_cluster() {
    let decl = declaring_a_build("tempo");
    let lock = lock_for("tempo", LOCAL_ID, Delivery::LocalDaemon, FRESH);

    lock_preflight(
        &plugin_dir(),
        &decl,
        Some(&lock),
        &recomputed("tempo", FRESH),
        DeployTier::Local,
    )
    .expect("a local-daemon lock is exactly what `curie local deploy` is for");

    let error = lock_preflight(
        &plugin_dir(),
        &decl,
        Some(&lock),
        &recomputed("tempo", FRESH),
        DeployTier::Cluster,
    )
    .expect_err("a cluster node cannot pull from the operator's local daemon");
    assert_actionable_usage_refusal(&error, &["tempo", "--registry"]);
}

/// The tier is a required field with no default, so a new deploy caller cannot
/// silently inherit `Local` and skip every cluster-only check (block B1-10).
///
/// The compiler is the real enforcement -- adding the field without a `Default`
/// impl makes it enumerate all three call sites. This test pins that the field
/// exists, is public, and that the two variants are distinguishable, so the
/// discrimination above has something to discriminate on.
#[test]
fn deploy_opts_carries_an_explicit_tier() {
    let opts = DeployOpts {
        delivery: None,
        agent: None,
        target: None,
        plugin_dir: plugin_dir(),
        api_url: "http://127.0.0.1:8000".to_string(),
        api_key: "curie-dev-key".to_string(),
        slack_channel: None,
        repo: None,
        workspace: WorkspaceIntent::Preserve,
        env: None,
        label: None,
        secret: Vec::new(),
        secret_binding_supported: false,
        connect_hint: "run `curie cluster up` first".to_string(),
        tier: DeployTier::Cluster,
    };
    assert_eq!(opts.tier, DeployTier::Cluster);
    assert_ne!(
        DeployTier::Cluster,
        DeployTier::Local,
        "the two tiers must be distinguishable, or the cluster-only checks select on nothing"
    );
}

// ─── Cluster only: the registry query (AC B6, AC B7) ─────────────────────────

/// An OCI image index, the shape a multi-platform `buildx --push` produces.
/// Spec: <https://github.com/opencontainers/image-spec/blob/main/image-index.md>
fn index_covering(platforms: &[(&str, &str)]) -> String {
    let manifests: Vec<String> = platforms
        .iter()
        .map(|(os, arch)| {
            format!(
                r#"{{"mediaType":"application/vnd.oci.image.manifest.v1+json",
                     "digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000",
                     "size":1234,
                     "platform":{{"architecture":"{arch}","os":"{os}"}}}}"#
            )
        })
        .collect();
    format!(
        r#"{{"schemaVersion":2,
            "mediaType":"application/vnd.oci.image.index.v1+json",
            "manifests":[{}]}}"#,
        manifests.join(",")
    )
}

/// A single-platform image manifest: what a `docker build && docker push`
/// produces, with no `manifests` array at all.
/// Spec: <https://github.com/opencontainers/image-spec/blob/main/manifest.md>
fn single_platform_manifest() -> String {
    r#"{"schemaVersion":2,
        "mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json",
                  "digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000",
                  "size":42},
        "layers":[]}"#
        .to_string()
}

/// The preflight asks the REGISTRY, by digest, for the raw manifest.
///
/// `--raw` matters: without it `imagetools inspect` pretty-prints and a parser
/// reading that output is reading a presentation format that has changed before.
#[test]
fn the_registry_is_queried_by_digest_for_the_raw_manifest() {
    let command = registry_manifest_argv(PUSHED);
    assert_eq!(command.program, "docker");
    let argv = command.argv();
    assert_eq!(
        &argv[..3],
        &[
            "buildx".to_string(),
            "imagetools".to_string(),
            "inspect".to_string()
        ],
        "{argv:?}"
    );
    assert!(argv.iter().any(|t| t == PUSHED), "{argv:?}");
    assert!(argv.iter().any(|t| t == "--raw"), "{argv:?}");
    assert!(
        argv.iter().any(|t| t.contains("@sha256:")),
        "the query is by DIGEST, not by tag; a tag can move between the check and the pull: \
         {argv:?}"
    );
}

/// The node architecture set comes from the cluster, not from the lock.
#[test]
fn the_node_architectures_are_read_from_the_cluster() {
    let command = node_architectures_argv();
    assert_eq!(command.program, "kubectl");
    let argv = command.argv();
    assert!(argv.iter().any(|t| t == "nodes"), "{argv:?}");
    assert!(
        argv.iter()
            .any(|t| t.contains("status.nodeInfo.architecture")),
        "{argv:?}"
    );

    assert_eq!(
        node_architectures("amd64 arm64 amd64"),
        BTreeSet::from(["amd64".to_string(), "arm64".to_string()]),
        "jsonpath emits one space-separated word per node; duplicates are the common case"
    );
    assert!(
        node_architectures("   ").is_empty(),
        "whitespace must not become an architecture named \"\""
    );
}

/// The resolved index's platform set is read from the registry's own answer.
#[test]
fn the_platform_set_is_parsed_out_of_the_resolved_index() {
    let platforms = manifest_platforms(&index_covering(&[("linux", "amd64"), ("linux", "arm64")]))
        .expect("an index parses");
    assert_eq!(
        platforms,
        BTreeSet::from(["linux/amd64".to_string(), "linux/arm64".to_string()])
    );

    let single = manifest_platforms(&single_platform_manifest()).expect("a single manifest parses");
    assert!(
        single.len() <= 1,
        "a plain image manifest is at most one platform, never the declared set: {single:?}"
    );
}

/// buildx's attestation manifests are `unknown/unknown` and must not be read as
/// a platform the image covers, nor treated as a defect.
///
/// A real multi-arch `buildx --push` emits them alongside the real entries by
/// default. A parser that choked on them, or that reported `unknown/unknown` as
/// coverage, would fail (or pass) every genuine multi-arch push.
/// See <https://docs.docker.com/build/metadata/attestations/>.
#[test]
fn attestation_manifests_are_not_platforms() {
    let raw = index_covering(&[
        ("linux", "amd64"),
        ("linux", "arm64"),
        ("unknown", "unknown"),
        ("unknown", "unknown"),
    ]);
    let platforms = manifest_platforms(&raw).expect("an attested index parses");
    assert_eq!(
        platforms,
        BTreeSet::from(["linux/amd64".to_string(), "linux/arm64".to_string()]),
        "unknown/unknown is an attestation, not an architecture"
    );

    registry_preflight(
        PUSHED,
        Ok(&raw),
        &BTreeSet::from(["amd64".to_string(), "arm64".to_string()]),
        &["linux/amd64".to_string(), "linux/arm64".to_string()],
    )
    .expect("an attested multi-arch push must deploy");
}

/// AC B6: a digest that does not resolve stops the deploy.
///
/// The falsifiable negative for this is the test above it: the same command
/// with the artifact present succeeds. Implemented against the registry, not
/// against the lock's declaration -- a declaration-only check would pass this
/// with the package deleted, which is the failure it exists to catch.
#[test]
fn an_unresolvable_digest_is_refused_before_anything_is_applied() {
    let error = registry_preflight(
        PUSHED,
        Err("ERROR: failed to resolve reference: manifest unknown".to_string()),
        &BTreeSet::from(["amd64".to_string()]),
        &["linux/amd64".to_string(), "linux/arm64".to_string()],
    )
    .expect_err("a deleted package must stop the deploy");

    assert_actionable_usage_refusal(
        &error,
        &[
            "2222222222222222222222222222222222222222222222222222222222222222",
            "curie build --plugin-dir",
        ],
    );
}

/// AC B7: the resolved index must cover every architecture the nodes report.
///
/// The refusal names BOTH sets. "Architecture mismatch" alone leaves the
/// operator guessing which side to change; naming the registry's set and the
/// node set makes the next action obvious.
#[test]
fn an_index_that_does_not_cover_every_node_architecture_is_refused() {
    let error = registry_preflight(
        PUSHED,
        Ok(&index_covering(&[("linux", "amd64")])),
        &BTreeSet::from(["amd64".to_string(), "arm64".to_string()]),
        &["linux/amd64".to_string(), "linux/arm64".to_string()],
    )
    .expect_err("an amd64-only index cannot run on an arm64 node");

    assert_actionable_usage_refusal(&error, &["linux/amd64", "arm64"]);
}

/// The comparison reads the REGISTRY, never the lock's declaration.
///
/// The second independent path AC B7 names: the lock still declares both
/// platforms while the push went out single-arch. A preflight comparing node
/// architectures against `lock.platforms` passes this and the failure resurfaces
/// after apply as `no matching manifest for linux/arm64`.
#[test]
fn a_lock_declaring_two_platforms_does_not_excuse_a_single_arch_push() {
    let declared = ["linux/amd64".to_string(), "linux/arm64".to_string()];

    registry_preflight(
        PUSHED,
        Ok(&index_covering(&[("linux", "amd64"), ("linux", "arm64")])),
        &BTreeSet::from(["amd64".to_string(), "arm64".to_string()]),
        &declared,
    )
    .expect("a genuinely multi-arch push deploys");

    assert!(
        registry_preflight(
            PUSHED,
            Ok(&single_platform_manifest()),
            &BTreeSet::from(["amd64".to_string(), "arm64".to_string()]),
            &declared,
        )
        .is_err(),
        "the lock's declared platforms must not be able to vouch for what was actually pushed"
    );
}

/// A single-node amd64 cluster is covered by an amd64-only image.
///
/// The gate is coverage of the node set, not equality with the declared set:
/// otherwise every homogeneous cluster would be refused for an image that runs
/// on it perfectly well.
#[test]
fn coverage_is_against_the_node_set_not_equality_with_the_declaration() {
    registry_preflight(
        PUSHED,
        Ok(&index_covering(&[("linux", "amd64")])),
        &BTreeSet::from(["amd64".to_string()]),
        &["linux/amd64".to_string(), "linux/arm64".to_string()],
    )
    .expect("an amd64 image on an amd64-only cluster is fine");
}

/// Which connectors the cluster deploy actually asks the registry about.
///
/// The selection decides whether a deploy contacts a registry at all, so it is
/// the seam that keeps every existing bundle untouched: an `image:` connector
/// builds nothing, and a `local-daemon` build is not in any registry to be
/// found (`lock_preflight` has already refused that one at this tier).
#[test]
fn only_registry_delivered_build_connectors_are_queried() {
    assert!(
        registry_preflight_targets(&declaring_a_build("tempo"), None).is_empty(),
        "with no lock there is nothing to ask about; lock_preflight already refused this"
    );
    assert!(
        registry_preflight_targets(
            &declaring_an_image("kubernetes"),
            Some(&lock_for("kubernetes", PUSHED, Delivery::Registry, FRESH)),
        )
        .is_empty(),
        "an image: connector builds nothing, so no lock entry of its own is checked"
    );
    assert!(
        registry_preflight_targets(
            &declaring_a_build("tempo"),
            Some(&lock_for("tempo", LOCAL_ID, Delivery::LocalDaemon, FRESH)),
        )
        .is_empty(),
        "a local-daemon build is never in a registry to be found"
    );

    let pushed_lock = lock_for("tempo", PUSHED, Delivery::Registry, FRESH);
    let targets = registry_preflight_targets(&declaring_a_build("tempo"), Some(&pushed_lock));
    assert_eq!(targets.len(), 1, "{targets:?}");
    assert_eq!(
        targets[0].image, PUSHED,
        "the digest-pinned reference is what gets queried"
    );
    assert_eq!(
        targets[0].platforms,
        vec!["linux/amd64".to_string(), "linux/arm64".to_string()],
        "the declared platforms ride along for the refusal to name"
    );
}

// ─── The preflight runs before the upload ────────────────────────────────────

/// The refusal names the bundle directory it wants rebuilt, not a temp path.
///
/// `curie build --plugin-dir /tmp/.tmpXYZ` is a command the operator cannot
/// run; the fix line has to point at the directory they actually have. Guards
/// the case where the preflight is moved after the pack and starts describing
/// the extracted copy.
#[test]
fn the_fix_line_names_the_operators_own_bundle_directory() {
    let dir = Path::new("/home/operator/agents/sre-bot");
    let error = lock_preflight(
        dir,
        &declaring_a_build("tempo"),
        None,
        &recomputed("tempo", FRESH),
        DeployTier::Cluster,
    )
    .expect_err("a lockless bundle is refused");

    let (_class, fix) = classify(&error);
    let fix = fix.expect("a fix line");
    assert!(
        fix.contains("/home/operator/agents/sre-bot"),
        "the fix must be runnable as written: {fix}"
    );
    assert!(
        !fix.contains("/tmp/"),
        "a temp path means the preflight ran after the pack: {fix}"
    );
}
