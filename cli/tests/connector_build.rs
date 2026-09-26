//! RED contract for PR B / Stream B1: `curie build --plugin-dir` execution.
//!
//! PR A landed the *declaration* mirrors (`connectors.yaml`,
//! `connectors.lock.yaml`, `source_digest_of`, `object_name`/`service_dns`) in
//! `cli/src/connector_build.rs`. This file pins what B1 adds on top: the argv
//! `curie build` actually issues, how a registry digest is recovered from
//! buildx's metadata file, and the shape and overwrite rule of the lock it
//! writes.
//!
//! Everything here is a PURE builder assertion, per the `OpsCommand`
//! convention in `cli/CLAUDE.md:133-142`: no Docker daemon is contacted, no
//! registry is dialed, and no `--dry-run` printer is involved. A builder that
//! only produced the right argv when a daemon answered would be untestable in
//! CI, which is exactly why the split exists.
//!
//! The builders this imports do not exist yet. That compile failure IS the
//! intended RED, and it is isolated to this test target because the file
//! imports from the `curie` lib rather than adding inline `#[cfg(test)]`
//! blocks to production modules.
//!
//! Surface the implementer must add to `curie::connector_build`:
//!
//! ```ignore
//! pub struct ConnectorBuildPlan {
//!     pub connector: String,
//!     pub context: PathBuf,          // canonical, inside the bundle
//!     pub dockerfile: PathBuf,       // canonical, inside the context
//!     pub platforms: Vec<String>,    // DECLARED order
//!     pub delivery: Delivery,
//!     pub image_ref: String,         // the `-t` tag this build produces
//!     pub source_digest: String,
//!     pub metadata_file: Option<PathBuf>, // registry delivery only
//! }
//!
//! pub fn host_platform() -> String;
//! pub fn build_plan(
//!     bundle_root: &Path,
//!     bundle_name: &str,
//!     connector: &str,
//!     spec: &ConnectorSpecDecl,
//!     registry: Option<&str>,
//!     host_platform: &str,
//!     metadata_dir: &Path,
//! ) -> anyhow::Result<ConnectorBuildPlan>;
//! pub fn build_argv(plan: &ConnectorBuildPlan) -> curie::ops::OpsCommand;
//! pub fn image_inspect_argv(tag: &str) -> curie::ops::OpsCommand;
//! pub fn registry_image_ref(
//!     registry: &str, bundle: &str, connector: &str, source_digest: &str,
//! ) -> String;
//! pub fn digest_from_metadata(raw: &str) -> anyhow::Result<String>;
//! pub fn digest_pinned_ref(image_ref: &str, digest: &str) -> String;
//! pub fn lock_overwrite_refusal(
//!     existing: Option<&ConnectorLockFileDecl>,
//!     next: &ConnectorLockFileDecl,
//!     force: bool,
//! ) -> Option<String>;
//! pub fn write_lock(
//!     bundle_root: &Path, lock: &ConnectorLockFileDecl, force: bool,
//! ) -> anyhow::Result<()>;
//! ```
//!
//! and to `curie::commands`: `ConnectorBuildOutput` / `ConnectorBuildRecord`
//! implementing `curie::ui::CliOutput` (block B1-2).
//!
//! Registry placeholders throughout are `ghcr.io/acme-corp/...`; nothing here
//! names a real registry, org, or namespace.

use std::collections::BTreeMap;
use std::path::Path;

use curie::commands::{connectors_needing_rebuild, ConnectorBuildOutput, ConnectorBuildRecord};
use curie::connector_build::{
    self, build_argv, build_plan, digest_from_metadata, digest_pinned_ref, host_platform,
    hosted_secret_names, image_inspect_argv, lock_overwrite_refusal, missing_secrets_error,
    refuse_reserved_secret_names, registry_image_ref, write_lock, ConnectorBuildDecl,
    ConnectorLockEntryDecl, ConnectorLockFileDecl, ConnectorSpecDecl, Delivery, SecretDecl,
};
use curie::ui::CliOutput;
use tempfile::TempDir;

// ─── Fixtures ────────────────────────────────────────────────────────────────

/// A bundle carrying one buildable connector under `connectors/<name>/`.
///
/// Deliberately hand-built rather than read from `examples/sre-bot`: the
/// example's `k8s-write` block ships commented out (it needs an image the
/// reader builds), and Stream B3 owns the enabled fixture. A B1 unit test that
/// depended on B3's file would go red for a reason that is not B1's.
fn bundle_with(connector: &str) -> TempDir {
    let dir = TempDir::new().expect("a scratch bundle");
    let context = dir.path().join("connectors").join(connector);
    std::fs::create_dir_all(&context).expect("mkdir -p the build context");
    std::fs::write(
        context.join("Dockerfile"),
        "FROM python:3.13-slim\nCOPY server.py /server.py\n",
    )
    .expect("write Dockerfile");
    std::fs::write(context.join("server.py"), "# the connector\n").expect("write server.py");
    dir
}

fn spec_for(connector: &str, platforms: &[&str]) -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        build: Some(ConnectorBuildDecl {
            context: format!("connectors/{connector}"),
            dockerfile: "Dockerfile".to_string(),
            platforms: platforms.iter().map(|p| (*p).to_string()).collect(),
        }),
        ..Default::default()
    }
}

fn plan_for(
    root: &Path,
    connector: &str,
    platforms: &[&str],
    registry: Option<&str>,
    host: &str,
    metadata_dir: &Path,
) -> connector_build::ConnectorBuildPlan {
    build_plan(
        root,
        "sre-bot",
        connector,
        &spec_for(connector, platforms),
        registry,
        host,
        metadata_dir,
    )
    .unwrap_or_else(|error| panic!("build_plan for {connector}: {error}"))
}

/// The index of `needle` in `argv`, so adjacency of a flag and its value can be
/// asserted rather than mere presence: `--platform` somewhere and
/// `linux/amd64` somewhere else is not the same command.
fn index_of(argv: &[String], needle: &str) -> usize {
    argv.iter()
        .position(|token| token == needle)
        .unwrap_or_else(|| panic!("{needle:?} is missing from argv: {argv:?}"))
}

fn value_after(argv: &[String], flag: &str) -> String {
    let at = index_of(argv, flag);
    argv.get(at + 1)
        .unwrap_or_else(|| panic!("{flag:?} has no value in argv: {argv:?}"))
        .clone()
}

// ─── The two delivery paths produce two different commands ───────────────────

/// (1) No `--registry`: a plain `docker build` for the HOST platform only,
/// into the local daemon.
///
/// The host platform, not the declared set, is the whole point of this path:
/// `docker build` cannot emit a multi-arch index, so building the declared
/// `linux/amd64,linux/arm64` here would either fail or silently produce one
/// arch under a name that claims two. The declared set stays in the lock (and
/// in the source digest) so `cluster deploy` can still refuse it.
#[test]
fn the_local_daemon_path_builds_the_host_platform_with_plain_docker_build() {
    let bundle = bundle_with("tempo");
    let meta = TempDir::new().expect("a metadata dir");
    let plan = plan_for(
        bundle.path(),
        "tempo",
        &["linux/amd64", "linux/arm64"],
        None,
        "linux/arm64",
        meta.path(),
    );

    assert_eq!(plan.delivery, Delivery::LocalDaemon);
    let command = build_argv(&plan);
    assert_eq!(command.program, "docker");
    let argv = command.argv();

    assert_eq!(argv.first().map(String::as_str), Some("build"), "{argv:?}");
    assert_eq!(
        value_after(&argv, "--platform"),
        "linux/arm64",
        "the local-daemon path builds exactly the host platform: {argv:?}"
    );
    assert_eq!(
        value_after(&argv, "-f"),
        plan.dockerfile.display().to_string(),
        "the resolved (symlink-refused) Dockerfile is what -f names: {argv:?}"
    );
    assert_eq!(value_after(&argv, "-t"), plan.image_ref, "{argv:?}");
    assert_eq!(
        argv.last().map(String::as_str),
        Some(plan.context.display().to_string().as_str()),
        "the canonical context is the final positional: {argv:?}"
    );

    // Not buildx and not pushed. A local build that quietly pushed would put an
    // artifact in a registry the operator never named.
    assert!(!argv.iter().any(|t| t == "buildx"), "{argv:?}");
    assert!(!argv.iter().any(|t| t == "--push"), "{argv:?}");
    assert!(!argv.iter().any(|t| t == "--metadata-file"), "{argv:?}");
    assert!(plan.metadata_file.is_none());
}

/// (2) `--registry <ref>`: `docker buildx build` over the DECLARED platform
/// set, pushed, with a metadata file to read the index digest back out of.
///
/// The joined order is the declared order, matching `source_digest_of`'s
/// canonical `build` block (`platform_order_is_significant` in
/// `tests/vectors/connector-source-digest.json`). A builder that sorted them
/// would make the argv and the digest disagree about what was declared.
#[test]
fn the_registry_path_builds_every_declared_platform_with_buildx_and_pushes() {
    let bundle = bundle_with("tempo");
    let meta = TempDir::new().expect("a metadata dir");
    let plan = plan_for(
        bundle.path(),
        "tempo",
        &["linux/arm64", "linux/amd64"],
        Some("ghcr.io/acme-corp"),
        "linux/amd64",
        meta.path(),
    );

    assert_eq!(plan.delivery, Delivery::Registry);
    let command = build_argv(&plan);
    assert_eq!(command.program, "docker");
    let argv = command.argv();

    assert_eq!(
        &argv[..2],
        &["buildx".to_string(), "build".to_string()],
        "{argv:?}"
    );
    assert_eq!(
        value_after(&argv, "--platform"),
        "linux/arm64,linux/amd64",
        "declared order, comma-joined, not sorted: {argv:?}"
    );
    assert!(
        argv.iter().any(|t| t == "--push"),
        "an unpushed index cannot be pulled by a cluster node: {argv:?}"
    );

    let metadata_file = plan
        .metadata_file
        .as_ref()
        .expect("the registry path records where buildx writes its metadata");
    assert_eq!(
        value_after(&argv, "--metadata-file"),
        metadata_file.display().to_string(),
        "{argv:?}"
    );
    assert!(
        metadata_file.starts_with(meta.path()),
        "the metadata file lands in the caller's directory, not next to the bundle: {}",
        metadata_file.display()
    );
    assert_eq!(value_after(&argv, "-t"), plan.image_ref, "{argv:?}");
}

/// The host platform is a real OCI platform and is a Linux one.
///
/// `docker build --platform darwin/arm64` on a Mac would ask the daemon for an
/// image no Linux container runtime can run; the host here means "the arch of
/// the machine", never "the OS of the machine".
#[test]
fn the_host_platform_is_a_linux_oci_platform() {
    let host = host_platform();
    assert!(
        host.starts_with("linux/"),
        "the host build platform must be a linux one, got {host:?}"
    );
    let arch = host.trim_start_matches("linux/");
    assert!(
        !arch.is_empty()
            && arch
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
        "{host:?} is not an os/arch platform"
    );
}

// ─── The image reference and the digest that replaces it ─────────────────────

/// The pushed tag is derived from the source digest, so two different sources
/// never collide on one tag, and it is never a mutable name.
///
/// `apply_lock` on the Python side refuses a lock whose image ends in a tag, so
/// the tag here is only ever the push target; what the lock records is the
/// `@sha256:` form below. But a mutable push target (`:latest`, `:v1`) would
/// still let a second build silently replace the bytes a first deploy resolved.
#[test]
fn the_pushed_tag_is_derived_from_the_source_digest_and_is_not_mutable() {
    let first = registry_image_ref(
        "ghcr.io/acme-corp",
        "sre-bot",
        "k8s-write",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let second = registry_image_ref(
        "ghcr.io/acme-corp",
        "sre-bot",
        "k8s-write",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );

    let (repo, tag) = first.rsplit_once(':').expect("a tagged reference");
    assert_eq!(repo, "ghcr.io/acme-corp/sre-bot-k8s-write");
    assert!(!tag.is_empty(), "an empty tag is `latest` by another name");
    assert_ne!(tag, "latest");
    assert!(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".starts_with(tag),
        "the tag is a prefix of the source digest hex, got {tag:?}"
    );
    assert_ne!(
        first, second,
        "two sources must not push to one tag, or the second build overwrites the first"
    );
}

/// buildx reports the index digest through `--metadata-file`, keyed
/// `containerimage.digest`.
///
/// External shape, not ours: the file is a JSON object of build-result keys,
/// documented at
/// <https://docs.docker.com/reference/cli/docker/buildx/build/#metadata-file>.
/// Parsing it by hand (grepping stderr for `sha256:`) is how a digest for the
/// wrong artifact gets recorded, so the key is named explicitly.
#[test]
fn the_index_digest_is_read_from_the_buildx_metadata_file() {
    let raw = r#"{
      "buildx.build.ref": "default/default/abcdef",
      "containerimage.config.digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
      "containerimage.digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
      "image.name": "ghcr.io/acme-corp/sre-bot-tempo:abc123"
    }"#;

    let digest = digest_from_metadata(raw).expect("the metadata file names the index digest");
    assert_eq!(
        digest, "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        "containerimage.digest is the INDEX digest; containerimage.config.digest is the \
         single-platform config blob and pulling by it fails on a multi-arch node"
    );
}

/// A metadata file with no digest is a failed push wearing a success costume.
#[test]
fn a_metadata_file_without_a_digest_is_an_error_not_an_empty_string() {
    let raw = r#"{"buildx.build.ref": "default/default/abcdef"}"#;
    assert!(
        digest_from_metadata(raw).is_err(),
        "a missing containerimage.digest must fail loudly; recording an empty image would \
         surface much later as ImagePullBackOff"
    );
}

/// What the lock records is the repository at a digest, with the push tag gone.
#[test]
fn the_locked_reference_pins_the_repository_at_the_digest_and_drops_the_tag() {
    let pinned = digest_pinned_ref(
        "ghcr.io/acme-corp/sre-bot-tempo:abc123",
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    );
    assert_eq!(
        pinned,
        "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
         2222222222222222222222222222222222222222222222222222222222222222"
    );
    assert!(
        !pinned.contains(":abc123"),
        "a tag surviving next to the digest is a reference two things can resolve: {pinned}"
    );
}

/// The local-daemon path records the daemon's immutable image id, not the
/// ephemeral tag it built under.
#[test]
fn the_local_daemon_path_reads_back_an_immutable_image_id() {
    let command = image_inspect_argv("curie-connector-sre-bot-tempo:build");
    assert_eq!(command.program, "docker");
    let argv = command.argv();
    assert_eq!(
        argv,
        vec![
            "image".to_string(),
            "inspect".to_string(),
            "--format".to_string(),
            "{{.Id}}".to_string(),
            "curie-connector-sre-bot-tempo:build".to_string(),
        ],
        "the id is what survives a later rebuild reusing the same tag"
    );
}

// ─── The lock write ──────────────────────────────────────────────────────────

fn lock_with(image: &str, delivery: Delivery, digest: &str) -> ConnectorLockFileDecl {
    let mut connectors = BTreeMap::new();
    connectors.insert(
        "tempo".to_string(),
        ConnectorLockEntryDecl {
            image: image.to_string(),
            delivery,
            platforms: vec!["linux/amd64".into(), "linux/arm64".into()],
            source_digest: digest.to_string(),
        },
    );
    ConnectorLockFileDecl {
        version: connector_build::LOCK_VERSION,
        connectors,
        ..Default::default()
    }
}

/// The lock lands at the bundle root, at version 1, and reads back through the
/// PR A parser with every field intact.
#[test]
fn the_written_lock_round_trips_through_the_reader() {
    let bundle = TempDir::new().expect("a scratch bundle");
    let lock = lock_with(
        "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
         2222222222222222222222222222222222222222222222222222222222222222",
        Delivery::Registry,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    write_lock(bundle.path(), &lock, false).expect("write the lock");

    let path = bundle.path().join(connector_build::CONNECTOR_LOCK_FILE);
    assert!(
        path.is_file(),
        "the lock lands at the BUNDLE ROOT so the packer ships it"
    );

    let read = connector_build::load_lock(bundle.path())
        .expect("read the lock")
        .expect("the lock exists");
    assert_eq!(read.version, connector_build::LOCK_VERSION);
    let entry = read.connectors.get("tempo").expect("tempo is locked");
    assert_eq!(entry.delivery, Delivery::Registry);
    assert!(entry.image.contains("@sha256:"), "{}", entry.image);
    assert_eq!(entry.platforms, vec!["linux/amd64", "linux/arm64"]);
    assert_eq!(
        entry.source_digest,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333"
    );
}

/// Rewriting the same resolution produces byte-identical bytes.
///
/// This is what makes "nothing changed" observable to a human reading
/// `git status` after a rebuild (AC B2's negative). A writer that stamped a
/// build timestamp, or serialized a HashMap, would show a diff on every run and
/// train the operator to ignore the file.
#[test]
fn rewriting_an_unchanged_resolution_is_byte_identical() {
    let bundle = TempDir::new().expect("a scratch bundle");
    let lock = lock_with(
        "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
         2222222222222222222222222222222222222222222222222222222222222222",
        Delivery::Registry,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    let path = bundle.path().join(connector_build::CONNECTOR_LOCK_FILE);

    write_lock(bundle.path(), &lock, false).expect("first write");
    let first = std::fs::read(&path).expect("read the first lock");
    write_lock(bundle.path(), &lock, false).expect("second write");
    let second = std::fs::read(&path).expect("read the second lock");

    assert_eq!(
        first, second,
        "an unchanged rebuild must not move a single byte of the lock"
    );
}

/// An unchanged source keeps its digest across two independent resolutions,
/// with different file mtimes.
///
/// `tests/vectors/connector-source-digest.json` freezes the algorithm's
/// content rules; what it cannot cover is the property this build path depends
/// on -- that `bundle.rs`'s ARCHIVE digest (which embeds mtime, uid and gid,
/// `cli/src/bundle.rs:181-184`) was NOT reused here. Copy the same content into
/// a second tree with a different mtime and the digest must not move.
#[test]
fn the_source_digest_is_content_derived_not_timestamp_derived() {
    let first = bundle_with("tempo");
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let second = bundle_with("tempo");

    let meta = TempDir::new().expect("a metadata dir");
    let a = plan_for(
        first.path(),
        "tempo",
        &["linux/amd64"],
        None,
        "linux/amd64",
        meta.path(),
    );
    let b = plan_for(
        second.path(),
        "tempo",
        &["linux/amd64"],
        None,
        "linux/amd64",
        meta.path(),
    );

    assert_eq!(
        a.source_digest, b.source_digest,
        "identical content in two trees with different mtimes must digest identically, or every \
         fresh clone reports its lock as stale"
    );
    assert!(
        a.source_digest.starts_with("sha256:"),
        "{}",
        a.source_digest
    );
}

/// A changed source moves the digest, so the two halves of AC B2 are a real
/// pair rather than one assertion stated twice.
#[test]
fn a_changed_source_moves_the_digest() {
    let bundle = bundle_with("tempo");
    let meta = TempDir::new().expect("a metadata dir");
    let before = plan_for(
        bundle.path(),
        "tempo",
        &["linux/amd64"],
        None,
        "linux/amd64",
        meta.path(),
    );

    std::fs::write(
        bundle.path().join("connectors/tempo/server.py"),
        "# the connector, edited\n",
    )
    .expect("edit the source");

    let after = plan_for(
        bundle.path(),
        "tempo",
        &["linux/amd64"],
        None,
        "linux/amd64",
        meta.path(),
    );
    assert_ne!(before.source_digest, after.source_digest);
}

// ─── The `--force` rule (Decision 1, review finding 2) ───────────────────────

/// A local-daemon build must not silently replace a registry lock.
///
/// The registry lock is the one a cluster can deploy; the local-daemon one is
/// not. Overwriting without asking turns "I rebuilt quickly to test something"
/// into a cluster deploy that refuses hours later, with nothing in the working
/// tree explaining what removed the pushed digest.
#[test]
fn a_local_daemon_build_refuses_to_overwrite_a_registry_lock_without_force() {
    let existing = lock_with(
        "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
         2222222222222222222222222222222222222222222222222222222222222222",
        Delivery::Registry,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    let next = lock_with(
        "sha256:4444444444444444444444444444444444444444444444444444444444444444",
        Delivery::LocalDaemon,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );

    let refusal = lock_overwrite_refusal(Some(&existing), &next, false)
        .expect("downgrading registry -> local-daemon must be refused");
    assert!(
        refusal.contains("--force"),
        "the refusal must name the flag that proceeds: {refusal}"
    );

    assert!(
        lock_overwrite_refusal(Some(&existing), &next, true).is_none(),
        "--force is the deliberate override and must proceed"
    );
}

/// The rule is one-directional, and absent on a first build.
#[test]
fn upgrading_to_a_registry_lock_and_first_writes_are_never_refused() {
    let local = lock_with(
        "sha256:4444444444444444444444444444444444444444444444444444444444444444",
        Delivery::LocalDaemon,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    let registry = lock_with(
        "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
         2222222222222222222222222222222222222222222222222222222222222222",
        Delivery::Registry,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );

    assert!(
        lock_overwrite_refusal(Some(&local), &registry, false).is_none(),
        "local-daemon -> registry is a promotion and needs no ceremony"
    );
    assert!(
        lock_overwrite_refusal(None, &local, false).is_none(),
        "a bundle with no lock has nothing to protect"
    );
}

/// The refusal must leave the file untouched, not half-written.
///
/// Asserting the refusal message alone would pass against a writer that
/// truncated the lock and then errored, which is the worst of both outcomes:
/// the registry resolution is gone AND the command failed.
#[test]
fn a_refused_overwrite_leaves_the_existing_lock_byte_identical() {
    let bundle = TempDir::new().expect("a scratch bundle");
    let existing = lock_with(
        "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
         2222222222222222222222222222222222222222222222222222222222222222",
        Delivery::Registry,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    write_lock(bundle.path(), &existing, false).expect("seed the registry lock");
    let path = bundle.path().join(connector_build::CONNECTOR_LOCK_FILE);
    let before = std::fs::read(&path).expect("read the seeded lock");

    let next = lock_with(
        "sha256:4444444444444444444444444444444444444444444444444444444444444444",
        Delivery::LocalDaemon,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );
    let error = write_lock(bundle.path(), &next, false)
        .expect_err("the downgrade must be refused at the write, not only by the predicate");
    assert!(format!("{error:#}").contains("--force"), "{error:#}");

    assert_eq!(
        before,
        std::fs::read(&path).expect("read the lock after the refusal"),
        "a refused write must not have touched the file"
    );

    write_lock(bundle.path(), &next, true).expect("--force proceeds");
    assert_ne!(
        before,
        std::fs::read(&path).expect("read the forced lock"),
        "--force must actually write, or the escape hatch is decorative"
    );
}

// ─── A build that cannot run writes no lock (AC B1's negative) ───────────────

/// A context with no Dockerfile fails during planning, before any argv exists.
///
/// The negative for AC B1: the failure names the connector, and nothing is
/// written. A planner that returned a plan here would hand `docker` a `-f` for
/// a file that is not there and fail with Docker's error instead of ours.
#[test]
fn a_context_without_a_dockerfile_fails_planning_and_writes_no_lock() {
    let bundle = TempDir::new().expect("a scratch bundle");
    std::fs::create_dir_all(bundle.path().join("connectors/tempo")).expect("mkdir the context");
    let meta = TempDir::new().expect("a metadata dir");

    let error = build_plan(
        bundle.path(),
        "sre-bot",
        "tempo",
        &spec_for("tempo", &["linux/amd64"]),
        None,
        "linux/amd64",
        meta.path(),
    )
    .expect_err("a context with no Dockerfile cannot be planned");
    assert!(
        format!("{error:#}").contains("tempo"),
        "the failure must name the connector: {error:#}"
    );
    assert!(
        !bundle
            .path()
            .join(connector_build::CONNECTOR_LOCK_FILE)
            .exists(),
        "a failed build must leave no lock behind"
    );
}

/// A symlinked Dockerfile is refused before `docker` is ever handed a path
/// (review finding 11): `curie build` reads it BEFORE the bundle is packed, so
/// the packer's own symlink refusal never runs.
#[cfg(unix)]
#[test]
fn a_symlinked_dockerfile_is_refused_during_planning() {
    let bundle = bundle_with("tempo");
    let host = TempDir::new().expect("a host directory outside the bundle");
    std::fs::write(host.path().join("Dockerfile"), "FROM scratch\n").expect("write a host file");
    let inside = bundle.path().join("connectors/tempo/Dockerfile");
    std::fs::remove_file(&inside).expect("remove the real Dockerfile");
    std::os::unix::fs::symlink(host.path().join("Dockerfile"), &inside).expect("symlink it");

    let meta = TempDir::new().expect("a metadata dir");
    assert!(
        build_plan(
            bundle.path(),
            "sre-bot",
            "tempo",
            &spec_for("tempo", &["linux/amd64"]),
            None,
            "linux/amd64",
            meta.path(),
        )
        .is_err(),
        "a symlink out of the bundle must be refused, not dereferenced"
    );
}

// ─── The `--json` receipt (block B1-2, AC B1) ────────────────────────────────

/// `curie build --plugin-dir --json` emits one object naming every connector's
/// resolved identity. Never empty stdout under `--json` (ADR-0021,
/// `cli/CLAUDE.md:16-43`).
#[test]
fn the_json_receipt_names_each_connectors_resolved_identity() {
    let output = ConnectorBuildOutput {
        connectors: vec![
            ConnectorBuildRecord {
                name: "kubernetes".to_string(),
                image: "ghcr.io/acme-corp/sre-bot-kubernetes@sha256:\
                        1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                delivery: Delivery::Registry,
                platforms: vec!["linux/amd64".into(), "linux/arm64".into()],
                source_digest: "sha256:\
                                3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
            },
            ConnectorBuildRecord {
                name: "tempo".to_string(),
                image: "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
                        2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                delivery: Delivery::Registry,
                platforms: vec!["linux/amd64".into(), "linux/arm64".into()],
                source_digest: "sha256:\
                                4444444444444444444444444444444444444444444444444444444444444444"
                    .to_string(),
            },
        ],
    };

    let json = output.to_json();
    let connectors = json["connectors"]
        .as_array()
        .unwrap_or_else(|| panic!("the receipt carries a `connectors` array: {json}"));
    assert_eq!(connectors.len(), 2, "{json}");

    for entry in connectors {
        for key in ["name", "image", "delivery", "platforms", "source_digest"] {
            assert!(
                entry.get(key).is_some(),
                "every record names {key}: {entry}"
            );
        }
        assert_eq!(
            entry["delivery"], "registry",
            "delivery serializes kebab-case, matching the lock the platform reads: {entry}"
        );
        assert!(
            entry["image"]
                .as_str()
                .expect("a string")
                .contains("@sha256:"),
            "a registry record reports the digest-pinned ref: {entry}"
        );
    }
    assert_eq!(connectors[0]["name"], "kubernetes", "{json}");
    assert_eq!(connectors[1]["name"], "tempo", "{json}");
}

/// A bundle declaring no buildable connectors still emits a JSON object.
///
/// Empty stdout under `--json` is the #485 failure: an agent driving the CLI
/// cannot distinguish "nothing to build" from "the command produced nothing".
#[test]
fn a_bundle_with_nothing_to_build_still_emits_an_object() {
    let json = ConnectorBuildOutput {
        connectors: Vec::new(),
    }
    .to_json();
    assert!(json.is_object(), "{json}");
    assert_eq!(
        json["connectors"].as_array().map(Vec::len),
        Some(0),
        "an empty list, not a missing key: {json}"
    );
}

// ─── The skill tier's rebuild decision (ADR 0113, Decision 3) ────────────────

/// A hosted connector built from source, with no `url`/`unhosted_url`.
fn built_spec() -> ConnectorSpecDecl {
    spec_for("tempo", &["linux/amd64"])
}

fn declaration(entries: &[(&str, ConnectorSpecDecl)]) -> connector_build::ConnectorsFileDecl {
    connector_build::ConnectorsFileDecl {
        connectors: entries
            .iter()
            .map(|(name, spec)| ((*name).to_string(), spec.clone()))
            .collect(),
        ..Default::default()
    }
}

fn digests(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(name, digest)| ((*name).to_string(), (*digest).to_string()))
        .collect()
}

const FRESH: &str = "sha256:5555555555555555555555555555555555555555555555555555555555555555";
const CHANGED: &str = "sha256:6666666666666666666666666666666666666666666666666666666666666666";

/// No lock at all: `skill up` has to build before it can start anything.
#[test]
fn an_absent_lock_names_every_built_connector() {
    let decl = declaration(&[("tempo", built_spec())]);
    assert_eq!(
        connectors_needing_rebuild(&decl, None, &digests(&[("tempo", FRESH)])),
        vec!["tempo".to_string()],
        "a bundle with no lock has no image to start"
    );
}

/// The driver-verified defect: the source moved on, the lock did not, and the
/// old digest's container used to start with no warning.
#[test]
fn a_stale_digest_names_the_connector_that_moved() {
    let decl = declaration(&[("tempo", built_spec())]);
    let lock = lock_with(
        "curie-connector-sre-bot-tempo:build",
        Delivery::LocalDaemon,
        FRESH,
    );
    assert_eq!(
        connectors_needing_rebuild(&decl, Some(&lock), &digests(&[("tempo", CHANGED)])),
        vec!["tempo".to_string()],
        "the locked image no longer stands for this source"
    );
}

/// The offline guarantee: a lock that still matches the tree triggers no build,
/// so `skill up` stays runnable with no daemon build and no network.
#[test]
fn a_fresh_lock_asks_for_no_build() {
    let decl = declaration(&[("tempo", built_spec())]);
    let lock = lock_with(
        "curie-connector-sre-bot-tempo:build",
        Delivery::LocalDaemon,
        FRESH,
    );
    assert!(
        connectors_needing_rebuild(&decl, Some(&lock), &digests(&[("tempo", FRESH)])).is_empty(),
        "an unchanged bundle must not invoke docker build"
    );
}

/// Nothing to build: an `image:`-hosted connector carries no source, and a
/// `build:` connector overridden to an already-running process is not started
/// here. Neither may drag the bundle into a build.
#[test]
fn connectors_with_no_source_to_build_ask_for_no_build() {
    let mut overridden = built_spec();
    overridden.unhosted_url = Some("http://localhost:9000/mcp".to_string());
    let decl = declaration(&[
        (
            "pinned",
            ConnectorSpecDecl {
                image: Some("ghcr.io/acme-corp/pinned:v1".to_string()),
                ..Default::default()
            },
        ),
        ("dev-override", overridden),
    ]);
    assert!(
        connectors_needing_rebuild(&decl, None, &BTreeMap::new()).is_empty(),
        "a bundle with no buildable hosted connector never reaches the build machinery"
    );
}

// ─── The bring-up secret preflight (fail closed) ─────────────────────────────
//
// The driver-verified defect: `skill up` on a bundle whose hosted connector
// declared a secret with no value here reported SUCCESS and started the
// container anyway; the container exited 1 on its own missing-credential check
// and the runner was left dialing an MCP URL that connection-refused mid-turn.
// `hosted_secret_names` is the seam the refusal is computed from -- which NAMES
// a bring-up must be able to resolve before it creates anything.

/// A connector started locally, declaring one plain `secrets:` entry.
fn hosted_with_secret(name: &str) -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        image: Some("ghcr.io/acme-corp/grafana-mcp:v1".to_string()),
        secrets: vec![SecretDecl::Name(name.to_string())],
        ..Default::default()
    }
}

/// THE RED. Without the preflight there is nothing to compute the refusal from,
/// and bring-up proceeds to start a container that cannot serve a single call.
#[test]
fn a_hosted_connectors_declared_secret_is_demanded_at_bring_up() {
    let decl = declaration(&[(
        "grafana",
        hosted_with_secret("GRAFANA_SERVICE_ACCOUNT_TOKEN"),
    )]);
    assert_eq!(
        hosted_secret_names(&decl),
        vec!["GRAFANA_SERVICE_ACCOUNT_TOKEN".to_string()],
        "the name the container's own startup check would have died on"
    );
}

/// The over-refusal guard. A connector pointed at something already running is
/// not started here at all: its secrets belong to the remote's client config and
/// are expanded by the MCP client, so demanding them would refuse a bundle that
/// works perfectly.
#[test]
fn a_connector_that_is_not_hosted_here_has_its_secrets_left_alone() {
    let mut remote = hosted_with_secret("REMOTE_API_KEY");
    remote.image = None;
    remote.url = Some("https://mcp.acme-corp.example/mcp".to_string());
    let mut overridden = hosted_with_secret("DEV_OVERRIDE_TOKEN");
    overridden.unhosted_url = Some("http://localhost:9000/mcp".to_string());
    let decl = declaration(&[("remote", remote), ("dev-override", overridden)]);
    assert!(
        hosted_secret_names(&decl).is_empty(),
        "neither connector is started by this bring-up, so neither credential is its prerequisite"
    );
}

/// The second channel a hosted connector resolves: a credential written to a
/// file the container mounts. An unstaged `secret_files` mount is the same
/// broken container, so its KEY is a prerequisite too.
#[test]
fn a_credential_file_key_is_a_prerequisite_as_well() {
    let mut spec = hosted_with_secret("GRAFANA_SERVICE_ACCOUNT_TOKEN");
    spec.secret_files = BTreeMap::from([(
        "K8S_READONLY_KUBECONFIG".to_string(),
        "/secrets/kubeconfig".to_string(),
    )]);
    let decl = declaration(&[("grafana", spec)]);
    assert_eq!(
        hosted_secret_names(&decl),
        vec![
            "GRAFANA_SERVICE_ACCOUNT_TOKEN".to_string(),
            "K8S_READONLY_KUBECONFIG".to_string(),
        ],
        "both resolution channels feed the one refusal"
    );
}

/// Every hosted connector in the bundle is weighed before anything starts, so
/// one run names every gap instead of one refusal per attempt.
#[test]
fn every_hosted_connector_in_the_bundle_is_weighed_at_once() {
    let decl = declaration(&[
        (
            "grafana",
            hosted_with_secret("GRAFANA_SERVICE_ACCOUNT_TOKEN"),
        ),
        ("kubernetes", hosted_with_secret("K8S_READONLY_KUBECONFIG")),
    ]);
    assert_eq!(
        hosted_secret_names(&decl),
        vec![
            "GRAFANA_SERVICE_ACCOUNT_TOKEN".to_string(),
            "K8S_READONLY_KUBECONFIG".to_string(),
        ],
        "a bring-up that refused one at a time would cost an operator a run per gap"
    );
}

/// The refusal itself: one shared message for the skill tier and the cluster
/// deploy path, classified as a usage error (exit 2), carrying the fix line and
/// nothing but NAMES.
#[test]
fn the_refusal_is_a_usage_error_naming_every_gap_and_the_fix() {
    let err = missing_secrets_error(&[
        "GRAFANA_SERVICE_ACCOUNT_TOKEN".to_string(),
        "K8S_READONLY_KUBECONFIG".to_string(),
    ]);
    let (class, _fix) = curie::exit::classify(&err);
    assert_eq!(
        class,
        curie::exit::ExitClass::Usage,
        "an operator prerequisite is a usage error, not a runtime failure"
    );
    let message = err.to_string();
    for needle in [
        "declares secret(s) with no value available",
        "GRAFANA_SERVICE_ACCOUNT_TOKEN",
        "K8S_READONLY_KUBECONFIG",
        "curie secrets set <NAME>",
    ] {
        assert!(
            message.contains(needle),
            "the refusal must carry `{needle}`: {message}"
        );
    }
}

// ─── The names a connector may not claim ─────────────────────────────────────

/// THE RED for the exfiltration: a connector that declares a model-credential
/// key gets that credential resolved out of the operator's own environment or
/// vault and injected into a container the bundle chose the image for. The
/// refusal is a pure decision over NAMES, so it lands before the resolve sweep
/// rather than reporting the declaration as satisfied.
#[test]
fn a_connector_may_not_declare_a_reserved_model_credential_key() {
    let decl = declaration(&[("grafana", hosted_with_secret("ANTHROPIC_API_KEY"))]);
    let err = refuse_reserved_secret_names(&decl).expect_err("a reserved name must be refused");
    let message = err.to_string();
    for needle in ["grafana", "ANTHROPIC_API_KEY", "reserved"] {
        assert!(
            message.contains(needle),
            "the refusal must carry `{needle}`: {message}"
        );
    }
    assert_eq!(
        curie::exit::classify(&err).0,
        curie::exit::ExitClass::Usage,
        "a bundle declaring a name it may not own is a usage error, not a runtime failure"
    );
}

/// The forward-safe half: a boot key nobody remembered to enumerate is still
/// refused because it carries the `CURIE_` prefix.
#[test]
fn a_connector_may_not_claim_the_curie_boot_namespace() {
    let decl = declaration(&[("grafana", hosted_with_secret("CURIE_RUNNER_TOKEN"))]);
    let message = refuse_reserved_secret_names(&decl)
        .expect_err("the prefix rule must refuse it")
        .to_string();
    assert!(message.contains("CURIE_RUNNER_TOKEN"), "{message}");
}

/// The second channel: same name, same resolution, same operator credential --
/// only the delivery differs, so the fence covers a `secret_files` key too.
#[test]
fn a_credential_file_key_is_held_to_the_same_fence() {
    let mut spec = hosted_with_secret("GRAFANA_SERVICE_ACCOUNT_TOKEN");
    spec.secret_files = BTreeMap::from([(
        "NODE_EXTRA_CA_CERTS".to_string(),
        "/secrets/ca.pem".to_string(),
    )]);
    let decl = declaration(&[("grafana", spec)]);
    let message = refuse_reserved_secret_names(&decl)
        .expect_err("a reserved secret_files key must be refused")
        .to_string();
    assert!(message.contains("NODE_EXTRA_CA_CERTS"), "{message}");
}

/// A name outside the env-var shape can never bind, and the check reuses the
/// one validator `curie secrets set` already holds an operator to.
#[test]
fn a_malformed_secret_name_is_refused_by_the_same_gate() {
    let decl = declaration(&[("grafana", hosted_with_secret("grafana-token"))]);
    let message = refuse_reserved_secret_names(&decl)
        .expect_err("a malformed name must be refused")
        .to_string();
    assert!(message.contains("grafana-token"), "{message}");
    assert!(message.contains("environment variable"), "{message}");
}

/// The over-refusal control: an ordinary credential name passes this gate
/// untouched and reaches the missing-value refusal exactly as before.
#[test]
fn an_ordinary_credential_name_passes_the_fence() {
    let mut spec = hosted_with_secret("GRAFANA_SERVICE_ACCOUNT_TOKEN");
    spec.secret_files = BTreeMap::from([(
        "K8S_READONLY_KUBECONFIG".to_string(),
        "/secrets/kubeconfig".to_string(),
    )]);
    let decl = declaration(&[("grafana", spec)]);
    assert!(
        refuse_reserved_secret_names(&decl).is_ok(),
        "the names the shipped examples declare must keep working"
    );
    assert_eq!(
        hosted_secret_names(&decl),
        vec![
            "GRAFANA_SERVICE_ACCOUNT_TOKEN".to_string(),
            "K8S_READONLY_KUBECONFIG".to_string(),
        ],
        "and they still reach the missing-value refusal"
    );
}

// ─── The buildx metadata directory ───────────────────────────────────────────

/// buildx's build result lands in a per-run directory under the SHARED system
/// temp dir, so its mode is the only thing keeping the other accounts on the
/// box out of it. Created private in one step rather than widened afterwards:
/// buildx starts writing as soon as it is handed the path.
///
/// The helper is asserted rather than `build_connectors` itself, which needs a
/// Docker daemon and a real build; `build_connectors` has exactly one call site
/// for it, at the create of `curie-build-<uuid>`.
#[cfg(unix)]
#[test]
fn the_buildx_metadata_directory_is_readable_only_by_its_owner() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = TempDir::new().expect("a scratch temp dir");
    let dir = tmp.path().join("curie-build-0000");
    curie::commands::create_private_dir(&dir).expect("create the metadata dir");

    let mode = std::fs::metadata(&dir)
        .expect("stat the metadata dir")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o700,
        "a world-readable directory in /tmp hands every account on the box the build's metadata"
    );
}
