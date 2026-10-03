//! Connector image builds, registry preflight, and connector bring-up for `connector build` and the tiers.

use super::*;

/// One connector's resolved identity, as `curie build --plugin-dir` reports it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectorBuildRecord {
    pub name: String,
    pub image: String,
    pub delivery: crate::connector_build::Delivery,
    pub platforms: Vec<String>,
    pub source_digest: String,
}

/// The `curie build --plugin-dir` receipt: one object, always, even when the
/// bundle declares nothing to build. Empty stdout under `--json` is the #485
/// failure -- an agent consumer cannot tell "nothing to build" from "the
/// command produced nothing".
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectorBuildOutput {
    pub connectors: Vec<ConnectorBuildRecord>,
}

impl crate::ui::CliOutput for ConnectorBuildOutput {
    fn to_json(&self) -> serde_json::Value {
        // Delegated wholesale to the `Serialize` value rather than hand-picked
        // field by field, so a record can never lose a field on the emit hop.
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({ "connectors": [] }))
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if self.connectors.is_empty() {
            ui.note("this bundle declares no connectors to build");
            return;
        }
        for record in &self.connectors {
            ui.payload_plain(&format!("{} -> {}", record.name, record.image));
        }
    }
}

/// The flags `curie build --plugin-dir` carries.
pub struct ConnectorBuildOpts {
    pub plugin_dir: PathBuf,
    /// `Some(ref)` pushes every declared (or `platforms`) platform there; `None` builds the host
    /// platform into the local Docker daemon.
    pub registry: Option<String>,
    /// The platform runner a declared runner layer builds on; `None` is the
    /// same default `curie skill up` runs.
    pub runner_image: Option<String>,
    /// Replace a registry lock with a local-daemon one deliberately.
    pub force: bool,
    /// `--platform` values narrowing each declared set; empty builds every
    /// declared platform.
    pub platforms: Vec<String>,
}

/// `curie build --plugin-dir <dir>`: build every connector the bundle declares
/// from source and write `connectors.lock.yaml`.
///
/// Deliberately does NOT look for a repo checkout the way `build` does: a
/// released binary building a bundle's connectors has none, and requiring one
/// would make the whole feature source-only.
pub async fn build_connectors(opts: ConnectorBuildOpts) -> Result<ConnectorBuildOutput> {
    use crate::connector_build as cb;

    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    let decl = cb::load(&plugin_dir)?;
    let buildable: Vec<(&String, &cb::ConnectorSpecDecl)> = decl
        .connectors
        .iter()
        .filter(|(_, spec)| spec.build.is_some())
        .collect();
    if buildable.is_empty() && decl.runner.is_none() {
        return Ok(ConnectorBuildOutput {
            connectors: Vec::new(),
        });
    }
    // The runner Dockerfile's base rule is checked before anything is resolved
    // or built: a literal base builds on something the lock would not record.
    if let Some(runner) = &decl.runner {
        cb::check_runner_source(&plugin_dir, runner)?;
    }
    // Narrow every declared set before anything is built, so an undeclared
    // `--platform` fails before minutes of builds.
    let mut narrowed = std::collections::BTreeMap::new();
    for (connector, spec) in &buildable {
        if let Some(build) = &spec.build {
            let set = cb::narrow_platforms(&build.platforms, &opts.platforms)
                .with_context(|| format!("connectors.{connector}"))?;
            narrowed.insert((*connector).clone(), set);
        }
    }
    let runner_platforms = match &decl.runner {
        Some(runner) => {
            cb::narrow_platforms(&runner.build.platforms, &opts.platforms).context("runner")?
        }
        None => Vec::new(),
    };
    if !crate::ops::on_path("docker") {
        bail!(
            "Docker is not installed or not on PATH. Install Docker \
             (https://docs.docker.com/get-docker/) and retry."
        );
    }
    let (bundle_name, _version) = read_manifest(&plugin_dir)?;

    // buildx writes its build result here; a private per-run directory rather
    // than the bundle, so a metadata file never rides the upload. The system
    // temp dir is shared with every other user on the box, so the directory is
    // the owner's alone rather than the 0755 a default umask would give it.
    let metadata_dir = std::env::temp_dir().join(format!("curie-build-{}", uuid::Uuid::new_v4()));
    create_private_dir(&metadata_dir)
        .with_context(|| format!("create {}", metadata_dir.display()))?;

    let ui = crate::ui::ui();
    let host = cb::host_platform();
    let mut records = Vec::new();
    let mut entries = std::collections::BTreeMap::new();
    let mut failure = None;
    let mut runner_entry = None;
    // Resolve the platform runner to an immutable identity before any build, so
    // a base that cannot be resolved fails before minutes of connector builds.
    let runner_base = match &decl.runner {
        Some(_) => {
            let image = crate::artifacts::resolve_image(
                opts.runner_image.as_deref(),
                crate::artifacts::Channel::current(),
                crate::artifacts::version(),
            );
            match resolve_runner_base(&image, opts.registry.is_some()).await {
                Ok(base) => Some(base),
                Err(err) => {
                    let _ = std::fs::remove_dir_all(&metadata_dir);
                    return Err(err);
                }
            }
        }
        None => None,
    };
    for (connector, spec) in buildable {
        let plan = match cb::build_plan(
            &plugin_dir,
            &bundle_name,
            connector,
            spec,
            opts.registry.as_deref(),
            &host,
            &metadata_dir,
            narrowed
                .get(connector)
                .map(Vec::as_slice)
                .unwrap_or_default(),
        ) {
            Ok(plan) => plan,
            Err(err) => {
                failure = Some(err);
                break;
            }
        };
        match run_one_connector_build(&plan, ui).await {
            Ok(image) => {
                entries.insert(
                    connector.clone(),
                    cb::ConnectorLockEntryDecl {
                        image: image.clone(),
                        delivery: plan.delivery,
                        platforms: plan.platforms.clone(),
                        source_digest: plan.source_digest.clone(),
                    },
                );
                records.push(ConnectorBuildRecord {
                    name: connector.clone(),
                    image,
                    delivery: plan.delivery,
                    platforms: plan.platforms.clone(),
                    source_digest: plan.source_digest,
                });
            }
            Err(err) => {
                failure = Some(err);
                break;
            }
        }
    }
    if let (None, Some(runner), Some((base_arg, base))) = (&failure, &decl.runner, &runner_base) {
        let built = match cb::runner_build_plan(
            &plugin_dir,
            &bundle_name,
            runner,
            opts.registry.as_deref(),
            base_arg,
            &host,
            &metadata_dir,
            &runner_platforms,
        ) {
            Ok(plan) => run_one_connector_build(&plan, ui)
                .await
                .map(|image| (plan, image)),
            Err(err) => Err(err),
        };
        match built {
            Ok((plan, image)) => {
                // Local-daemon delivery builds on a mutable tag, not a digest
                // pin: if `base_arg` was retagged between the pre-build
                // inspect above and this build finishing, the layer was built
                // on a base other than the one `base` records. Re-inspect and
                // refuse to write a lock that would misrecord it.
                let mut mismatch = None;
                if plan.delivery == cb::Delivery::LocalDaemon {
                    match crate::docker::docker(&cb::image_inspect_argv(base_arg).argv()).await {
                        Ok(current_id) => {
                            let current_id = current_id.trim();
                            if current_id != base {
                                mismatch = Some(anyhow::anyhow!(
                                    "runner: the platform runner {base_arg} changed during the \
                                     build (inspected {base}, now {current_id}); refusing to \
                                     record a lock for a base the layer was not built on"
                                ));
                            }
                        }
                        Err(err) => {
                            mismatch = Some(err.context(format!(
                                "runner: re-inspect the platform runner {base_arg} after build"
                            )))
                        }
                    }
                }
                match mismatch {
                    Some(err) => failure = Some(err),
                    None => {
                        runner_entry = Some(cb::RunnerLockEntryDecl {
                            image: image.clone(),
                            base: base.clone(),
                            delivery: plan.delivery,
                            platforms: plan.platforms.clone(),
                            source_digest: plan.source_digest.clone(),
                        });
                        records.push(ConnectorBuildRecord {
                            name: "runner".to_string(),
                            image,
                            delivery: plan.delivery,
                            platforms: plan.platforms,
                            source_digest: plan.source_digest,
                        });
                    }
                }
            }
            Err(err) => failure = Some(err),
        }
    }
    let _ = std::fs::remove_dir_all(&metadata_dir);
    if let Some(err) = failure {
        // A build that could not run writes no lock: a partial lock would claim
        // a resolved image for a connector that has none.
        return Err(err);
    }

    cb::write_lock(
        &plugin_dir,
        &cb::ConnectorLockFileDecl {
            version: cb::LOCK_VERSION,
            connectors: entries,
            runner: runner_entry,
        },
        opts.force,
    )?;
    Ok(ConnectorBuildOutput {
        connectors: records,
    })
}

/// Pin the platform runner a runner layer builds on (ADR 0173).
///
/// Returns `(build_arg, recorded_base)`. Registry delivery records and builds
/// on `<repo>@sha256:<manifest digest>`, asked of the registry unless the
/// reference already carries one. Local-daemon delivery builds on the
/// reference it inspected and records the daemon's image id for it.
pub(super) async fn resolve_runner_base(image: &str, registry: bool) -> Result<(String, String)> {
    use crate::connector_build as cb;

    if !registry {
        let id = crate::docker::docker(&cb::image_inspect_argv(image).argv())
            .await
            .with_context(|| format!("runner: inspect the platform runner {image}"))?;
        return Ok((image.to_string(), id.trim().to_string()));
    }
    if image.contains("@sha256:") {
        return Ok((image.to_string(), image.to_string()));
    }
    let inspect = cb::plain_command(
        "docker",
        vec![
            "buildx".into(),
            "imagetools".into(),
            "inspect".into(),
            image.to_string(),
            "--format".into(),
            "{{json .Manifest}}".into(),
        ],
    );
    let (ok, stdout, stderr) = crate::ops::run_capture(&inspect).await?;
    if !ok {
        bail!(
            "runner: could not resolve the platform runner {image} in its registry: {}",
            stderr.trim()
        );
    }
    let manifest: serde_json::Value = serde_json::from_str(stdout.trim())
        .with_context(|| format!("runner: parse the manifest of {image}"))?;
    let digest = manifest
        .get("digest")
        .and_then(|d| d.as_str())
        .ok_or_else(|| anyhow::anyhow!("runner: the manifest of {image} names no digest"))?;
    let base = cb::digest_pinned_ref(image, digest);
    Ok((base.clone(), base))
}

/// Create a directory only its owner can enter, in one step.
///
/// The mode rides the create rather than a `set_permissions` after it: in a
/// shared `/tmp` the window between the two is a window in which anyone can
/// read what lands there, and buildx starts writing as soon as it is handed the
/// path. Non-unix keeps the platform default, as the other cfg splits here do.
#[cfg(unix)]
pub fn create_private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}

#[cfg(not(unix))]
pub fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

/// Run one connector's build and read back the immutable reference it produced.
/// The fix for Docker's default driver refusing a multi-platform push: build
/// the one platform the cluster runs, or switch to a docker-container builder.
pub fn multi_platform_driver_fix(stderr: &str, host_platform: &str) -> Option<String> {
    if !stderr.contains("Multi-platform build is not supported for the docker driver") {
        return None;
    }
    Some(format!(
        "the default Docker driver cannot push a multi-platform image. Rerun with \
         `--platform {host_platform}` (or the architecture your cluster nodes run) to push \
         one platform, or create a docker-container builder \
         (`docker buildx create --driver docker-container --use`) to push them all"
    ))
}

pub(super) async fn run_one_connector_build(
    plan: &crate::connector_build::ConnectorBuildPlan,
    ui: &crate::ui::Ui,
) -> Result<String> {
    use crate::connector_build as cb;

    let command = cb::build_argv(plan);
    ui.note(&format!("=== {} ===", command.display()));
    // Inherit stdout so the build log streams like a hand-run build. A registry
    // build tees stderr so a driver refusal can be answered with a fix.
    let (status, stderr) = if plan.delivery == cb::Delivery::Registry {
        use tokio::io::AsyncBufReadExt;
        let mut child = tokio::process::Command::new(&command.program)
            .args(command.argv())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("failed to invoke docker")?;
        let mut captured = String::new();
        if let Some(pipe) = child.stderr.take() {
            let mut lines = tokio::io::BufReader::new(pipe).lines();
            while let Some(line) = lines.next_line().await.unwrap_or(None) {
                eprintln!("{line}");
                captured.push_str(&line);
                captured.push('\n');
            }
        }
        let status = child.wait().await.context("failed to wait for docker")?;
        (status, captured)
    } else {
        let status = tokio::process::Command::new(&command.program)
            .args(command.argv())
            .status()
            .await
            .context("failed to invoke docker")?;
        (status, String::new())
    };
    if !status.success() {
        let message = format!("building connector '{}' failed ({status})", plan.connector);
        if let Some(fix) = multi_platform_driver_fix(&stderr, &plan.host_platform) {
            return Err(anyhow::Error::from(
                crate::exit::CliError::failure(format!("{message}: {fix}")).with_fix(fix),
            ));
        }
        bail!("{message}");
    }
    match plan.delivery {
        cb::Delivery::Registry => {
            let metadata_file = plan
                .metadata_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("a registry build records no metadata file"))?;
            let raw = std::fs::read_to_string(metadata_file)
                .with_context(|| format!("read {}", metadata_file.display()))?;
            Ok(cb::digest_pinned_ref(
                &plan.image_ref,
                &cb::digest_from_metadata(&raw)?,
            ))
        }
        cb::Delivery::LocalDaemon => {
            let inspect = cb::image_inspect_argv(&plan.image_ref);
            crate::docker::docker(&inspect.argv()).await
        }
    }
}

// ---------------------------------------------------------------------------
// The deploy-time lock preflight (ADR 0113)
// ---------------------------------------------------------------------------

/// The command that clears a missing or stale lock, as the operator would type
/// it against their own bundle directory.
pub(super) fn rebuild_hint(plugin_dir: &Path, registry: bool) -> String {
    if registry {
        format!(
            "run `curie build --plugin-dir {} --registry <ref>` and redeploy",
            plugin_dir.display()
        )
    } else {
        format!(
            "run `curie build --plugin-dir {}` and redeploy",
            plugin_dir.display()
        )
    }
}

/// Refuse a deploy whose `build:` connectors have no usable lock.
///
/// The CLI mirror of the platform's intake rule, run before the bundle is even
/// packed so the operator gets a local failure naming their own directory
/// instead of an upload rejection about a file nobody told them to make. A
/// bundle with no `build:` connector is untouched.
pub fn lock_preflight(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&crate::connector_build::ConnectorLockFileDecl>,
    recomputed: &std::collections::BTreeMap<String, String>,
    tier: DeployTier,
) -> Result<()> {
    use crate::connector_build::Delivery;

    for (connector, spec) in &decl.connectors {
        if spec.build.is_none() {
            continue;
        }
        let Some(entry) = lock.and_then(|lock| lock.connectors.get(connector)) else {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "connectors.{connector} builds from source, but {} records no image for it. \
                     Build it before deploying.",
                    crate::connector_build::CONNECTOR_LOCK_FILE
                ))
                .with_fix(rebuild_hint(plugin_dir, false)),
            ));
        };
        if let Some(fresh) = recomputed.get(connector) {
            if &entry.source_digest != fresh {
                return Err(anyhow::Error::from(
                    crate::exit::CliError::usage(format!(
                        "connectors.{connector} has changed since {} was written, so the locked \
                         image no longer matches this source.",
                        crate::connector_build::CONNECTOR_LOCK_FILE
                    ))
                    .with_fix(rebuild_hint(plugin_dir, false)),
                ));
            }
        }
        if tier == DeployTier::Cluster && entry.delivery == Delivery::LocalDaemon {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "connectors.{connector} is locked to an image in your local Docker daemon, \
                     which no cluster node can pull. Push it to a registry first."
                ))
                .with_fix(rebuild_hint(plugin_dir, true)),
            ));
        }
    }
    if decl.runner.is_some() {
        let Some(entry) = lock.and_then(|lock| lock.runner.as_ref()) else {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "{} declares a runner layer, but {} records no runner image for it. Build \
                     it before deploying.",
                    crate::connector_build::CONNECTORS_FILE,
                    crate::connector_build::CONNECTOR_LOCK_FILE
                ))
                .with_fix(rebuild_hint(plugin_dir, false)),
            ));
        };
        if let Some(fresh) = recomputed.get(crate::connector_build::RUNNER_DIGEST_KEY) {
            if &entry.source_digest != fresh {
                return Err(anyhow::Error::from(
                    crate::exit::CliError::usage(format!(
                        "the runner layer has changed since {} was written, so the locked \
                         runner image no longer matches this source.",
                        crate::connector_build::CONNECTOR_LOCK_FILE
                    ))
                    .with_fix(rebuild_hint(plugin_dir, false)),
                ));
            }
        }
        if tier == DeployTier::Cluster && entry.delivery == Delivery::LocalDaemon {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(
                    "the runner layer is locked to an image in your local Docker daemon, which \
                     no cluster node can pull. Push it to a registry first."
                        .to_string(),
                )
                .with_fix(rebuild_hint(plugin_dir, true)),
            ));
        }
    }
    Ok(())
}

/// Ask the REGISTRY, by digest, for the raw manifest.
///
/// `--raw` matters: without it `imagetools inspect` pretty-prints, and a parser
/// reading that output is reading a presentation format that has changed before.
pub fn registry_manifest_argv(image: &str) -> crate::ops::OpsCommand {
    crate::connector_build::plain_command(
        "docker",
        vec![
            "buildx".into(),
            "imagetools".into(),
            "inspect".into(),
            image.to_string(),
            "--raw".into(),
        ],
    )
}

/// Ask docker for a single-image manifest's config blob, so a registry login
/// is honored when the native blob fetch is refused.
pub fn registry_config_argv(image: &str) -> crate::ops::OpsCommand {
    crate::connector_build::plain_command(
        "docker",
        vec![
            "buildx".into(),
            "imagetools".into(),
            "inspect".into(),
            image.to_string(),
            "--format".into(),
            "{{json .Image}}".into(),
        ],
    )
}

/// The architectures the cluster's own nodes report.
pub fn node_architectures_argv() -> crate::ops::OpsCommand {
    crate::connector_build::plain_command(
        "kubectl",
        vec![
            "get".into(),
            "nodes".into(),
            "-o".into(),
            "jsonpath={.items[*].status.nodeInfo.architecture}".into(),
        ],
    )
}

/// Parse that jsonpath's output: one space-separated word per node, duplicates
/// being the common case.
pub fn node_architectures(raw: &str) -> std::collections::BTreeSet<String> {
    raw.split_whitespace().map(str::to_string).collect()
}

/// The platform set an OCI image index actually covers.
///
/// buildx's attestation manifests are `unknown/unknown` and are neither a
/// platform the image covers nor a defect: a real multi-arch `--push` emits
/// them alongside the real entries by default. A plain single-platform manifest
/// has no `manifests` array at all and covers nothing the declaration promised.
pub fn manifest_platforms(raw: &str) -> Result<std::collections::BTreeSet<String>> {
    let parsed: serde_json::Value =
        serde_json::from_str(raw).context("parse the registry manifest")?;
    let Some(manifests) = parsed.get("manifests").and_then(|m| m.as_array()) else {
        return Ok(std::collections::BTreeSet::new());
    };
    Ok(manifests
        .iter()
        .filter_map(|entry| {
            let platform = entry.get("platform")?;
            let os = platform.get("os")?.as_str()?;
            let arch = platform.get("architecture")?.as_str()?;
            (os != "unknown" && arch != "unknown").then(|| format!("{os}/{arch}"))
        })
        .collect())
}

/// The `config.digest` of a plain image manifest (one with no `manifests`
/// array); `None` for an index or anything unparseable.
pub fn single_manifest_config_digest(raw: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    if parsed.get("manifests").is_some() {
        return None;
    }
    parsed
        .get("config")?
        .get("digest")?
        .as_str()
        .map(str::to_string)
}

/// The `os/architecture` an image config JSON names.
pub fn config_platform(raw: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(raw).ok()?;
    let os = parsed.get("os")?.as_str()?;
    let arch = parsed.get("architecture")?.as_str()?;
    Some(format!("{os}/{arch}"))
}

/// Refuse a cluster deploy whose locked image is gone from the registry, or
/// whose resolved index cannot run on every node.
///
/// The comparison is against the REGISTRY's answer, never the lock's declared
/// platforms: a lock declaring two platforms while the push went out single-arch
/// passes a declaration check and fails after apply as `no matching manifest`.
pub fn registry_preflight(
    image: &str,
    inspect: std::result::Result<&str, String>,
    node_architectures: &std::collections::BTreeSet<String>,
    declared_platforms: &[String],
    single_platform: Option<&str>,
) -> Result<()> {
    let raw = match inspect {
        Ok(raw) => raw,
        Err(stderr) => {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "the registry could not resolve {image}: {}. The image the lock names is not \
                     there, so no node could pull it.",
                    stderr.trim()
                ))
                .with_fix(
                    "rebuild and push it with `curie build --plugin-dir <dir> --registry <ref>`, \
                     then redeploy",
                ),
            ));
        }
    };
    let covered = manifest_platforms(raw).map_err(|err| {
        anyhow::Error::from(
            crate::exit::CliError::usage(format!(
                "the registry's answer for {image} is not a manifest this build understands: {err}"
            ))
            .with_fix(
                "rebuild and push it with `curie build --plugin-dir <dir> --registry <ref>`, \
                 then redeploy",
            ),
        )
    })?;
    // A plain single-image manifest covers the one platform its config names.
    let covered = match single_platform {
        Some(platform) if covered.is_empty() && single_manifest_config_digest(raw).is_some() => {
            std::collections::BTreeSet::from([platform.to_string()])
        }
        _ => covered,
    };
    let missing: Vec<String> = node_architectures
        .iter()
        .filter(|arch| !covered.contains(&format!("linux/{arch}")))
        .cloned()
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(anyhow::Error::from(
        crate::exit::CliError::usage(format!(
            "{image} covers [{}] in the registry, but this cluster's nodes report [{}], so [{}] \
             has no image to run. The lock declares [{}].",
            covered.iter().cloned().collect::<Vec<_>>().join(", "),
            node_architectures
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            missing.join(", "),
            declared_platforms.join(", "),
        ))
        .with_fix(
            "rebuild every architecture the nodes run with `curie build --plugin-dir <dir> \
             --registry <ref>`, then redeploy",
        ),
    ))
}

/// The lock entries a cluster deploy has to ask the registry about: the entries
/// of the `build:` connectors whose lock delivers through a registry.
///
/// Everything else is out of scope by construction -- an `image:` connector
/// pulls a reference nobody here built, and a `local-daemon` build has already
/// been refused by [`lock_preflight`] at this tier. An empty result is the
/// common bundle, and it queries nothing.
pub fn registry_preflight_targets<'a>(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&'a crate::connector_build::ConnectorLockFileDecl>,
) -> Vec<&'a crate::connector_build::ConnectorLockEntryDecl> {
    decl.connectors
        .iter()
        .filter(|(_, spec)| spec.build.is_some())
        .filter_map(|(connector, _)| lock.and_then(|lock| lock.connectors.get(connector)))
        .filter(|entry| entry.delivery == crate::connector_build::Delivery::Registry)
        .collect()
}

/// The impure half of [`registry_preflight`]: query the registry and the
/// cluster, then hand both answers to the decision.
///
/// The node architectures are read ONCE and shared across every connector --
/// they are a property of the cluster, not of the image, so one `kubectl get
/// nodes` covers a bundle building any number of connectors.
pub(super) async fn run_registry_preflight(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&crate::connector_build::ConnectorLockFileDecl>,
) -> Result<()> {
    let targets = registry_preflight_targets(decl, lock);
    if targets.is_empty() {
        return Ok(());
    }

    let (ok, out, err) = crate::ops::run_capture(&node_architectures_argv()).await?;
    if !ok {
        return Err(anyhow::anyhow!("{}", err.trim()))
            .context("read the architectures this cluster's nodes report");
    }
    let node_archs = node_architectures(&out);

    // The registry is read natively first, so a host without docker can still
    // deploy (#3503); docker, when present, is the fallback that honors a
    // registry login.
    let docker = crate::ops::on_path("docker");
    for entry in targets {
        let inspect = match crate::oci_registry::fetch_manifest(&entry.image).await {
            Ok(manifest) => Ok(String::from_utf8_lossy(&manifest.raw).into_owned()),
            Err(native) if docker => {
                let (ok, raw, err) =
                    crate::ops::run_capture(&registry_manifest_argv(&entry.image)).await?;
                if ok {
                    Ok(raw)
                } else {
                    Err(format!("{native:#}; docker also failed: {}", err.trim()))
                }
            }
            Err(native) => Err(format!(
                "{native:#}, and `docker` is not on PATH to ask with a registry login"
            )),
        };
        // A plain manifest names its platform only in its config blob; read
        // it natively, and on failure leave the refusal standing.
        let single_platform = match inspect
            .as_deref()
            .ok()
            .and_then(single_manifest_config_digest)
        {
            Some(digest) => {
                let native = crate::oci_registry::fetch_blob(&entry.image, &digest)
                    .await
                    .ok()
                    .and_then(|blob| config_platform(&String::from_utf8_lossy(&blob)));
                match native {
                    Some(platform) => Some(platform),
                    None if docker => {
                        let (ok, raw, _) =
                            crate::ops::run_capture(&registry_config_argv(&entry.image)).await?;
                        if ok {
                            config_platform(&raw)
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            }
            None => None,
        };
        registry_preflight(
            &entry.image,
            inspect.as_deref().map_err(Clone::clone),
            &node_archs,
            &entry.platforms,
            single_platform.as_deref(),
        )?;
    }
    Ok(())
}

/// The recomputed `source_digest` of every `build:` connector in a bundle, for
/// the preflight to compare the lock against.
pub fn recompute_source_digests(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut digests = std::collections::BTreeMap::new();
    for (connector, spec) in &decl.connectors {
        let Some(build) = &spec.build else { continue };
        let context = crate::connector_build::resolve_context(plugin_dir, &build.context)
            .with_context(|| format!("connectors.{connector}"))?;
        digests.insert(
            connector.clone(),
            crate::connector_build::source_digest_of(&context, build)
                .with_context(|| format!("connectors.{connector}"))?,
        );
    }
    if let Some(runner) = &decl.runner {
        let context = crate::connector_build::resolve_context(plugin_dir, &runner.build.context)
            .context("runner")?;
        digests.insert(
            crate::connector_build::RUNNER_DIGEST_KEY.to_string(),
            crate::connector_build::source_digest_of(&context, &runner.build).context("runner")?,
        );
    }
    Ok(digests)
}

/// The hosted `build:` connectors whose locked image no longer stands for this
/// source: either the lock records nothing for them, or it records a different
/// `source_digest` than the tree hashes to now.
///
/// The staleness test is `lock_preflight`'s, verbatim -- a missing lock entry
/// and a digest mismatch are the two failures it refuses a deploy on, and a
/// connector the recompute could not weigh in on is left alone by both. What
/// differs is only what the caller does with the answer: the skill tier rebuilds
/// (ADR 0113's Decision 3), the cluster tier refuses.
///
/// An `image:`-hosted connector has no source to be stale against, and one
/// pointed at an already-running process with `unhosted_url` is not started
/// here, so neither can put this bundle into a build.
pub fn connectors_needing_rebuild(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&crate::connector_build::ConnectorLockFileDecl>,
    recomputed: &std::collections::BTreeMap<String, String>,
) -> Vec<String> {
    let mut stale = Vec::new();
    for (connector, spec) in &decl.connectors {
        if spec.build.is_none() || spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        let Some(entry) = lock.and_then(|lock| lock.connectors.get(connector)) else {
            stale.push(connector.clone());
            continue;
        };
        if let Some(fresh) = recomputed.get(connector) {
            if &entry.source_digest != fresh {
                stale.push(connector.clone());
            }
        }
    }
    stale
}

// ---------------------------------------------------------------------------
// Shared connector startup configuration
// ---------------------------------------------------------------------------

pub(super) const CONNECTOR_START_TIMEOUT_ENV: &str = "CURIE_CONNECTOR_START_TIMEOUT_SECONDS";

/// Default readiness observation window for both skill and local connectors.
pub(super) const DEFAULT_CONNECTOR_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Extra time for the Compose client to start and collect its own result. This
/// does not extend the readiness value passed to Compose itself.
pub(super) const CONNECTOR_COMPOSE_CLIENT_ALLOWANCE: Duration = Duration::from_secs(5);

pub(super) fn invalid_connector_start_timeout() -> anyhow::Error {
    anyhow::Error::from(
        crate::exit::CliError::usage(format!(
            "{CONNECTOR_START_TIMEOUT_ENV} must be a positive whole number of seconds small enough for the connector startup clock"
        ))
        .with_fix(format!(
            "unset {CONNECTOR_START_TIMEOUT_ENV} to use the {} second default, or set it to a positive whole number of seconds that fits this system",
            DEFAULT_CONNECTOR_START_TIMEOUT.as_secs()
        )),
    )
}

/// Read the connector startup window once for either local startup path.
///
/// Checking the Compose allowance here keeps every later deadline addition
/// representable. Callers invoke this only when a bundle has a hosted
/// connector, so an unrelated invalid setting cannot affect connectorless
/// bundles.
pub(super) fn connector_start_timeout_from_env() -> Result<Duration> {
    let Some(raw) = std::env::var_os(CONNECTOR_START_TIMEOUT_ENV) else {
        return Ok(DEFAULT_CONNECTOR_START_TIMEOUT);
    };
    let raw = raw
        .into_string()
        .map_err(|_| invalid_connector_start_timeout())?;
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_connector_start_timeout());
    }
    let seconds = raw
        .parse::<u64>()
        .map_err(|_| invalid_connector_start_timeout())?;
    if seconds == 0 {
        return Err(invalid_connector_start_timeout());
    }
    let timeout = Duration::from_secs(seconds);
    let complete_compose_window = timeout
        .checked_add(CONNECTOR_COMPOSE_CLIENT_ALLOWANCE)
        .ok_or_else(invalid_connector_start_timeout)?;
    if Instant::now()
        .checked_add(complete_compose_window)
        .is_none()
    {
        return Err(invalid_connector_start_timeout());
    }
    Ok(timeout)
}

// ---------------------------------------------------------------------------
// The local tier's connector bring-up
// ---------------------------------------------------------------------------

/// Resolve each generated Compose service to its actual Docker container IDs.
///
/// The overlay's service names are not Docker container names: Compose adds its
/// project and replica components. An empty result remains a named readiness
/// target so the shared waiter can report which connector Compose did not
/// create, rather than leaking Compose output or assuming a container name.
pub(super) async fn compose_connector_readiness_targets(
    overlay: &Path,
    project: &str,
    connectors: &[(String, String)],
    deadline: Instant,
) -> Vec<(String, String)> {
    let mut targets = Vec::new();
    for (connector, service) in connectors {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining == Duration::ZERO {
            targets.push((connector.clone(), String::new()));
            continue;
        }
        let command =
            crate::connector_build::compose_service_ids_command(overlay, project, service);
        let ids = match tokio::time::timeout(remaining, crate::ops::run_capture(&command)).await {
            Ok(Ok((true, stdout, _))) => stdout
                .lines()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .collect(),
            _ => Vec::new(),
        };
        if ids.is_empty() {
            targets.push((connector.clone(), String::new()));
        } else {
            targets.extend(ids.into_iter().map(|id| (connector.clone(), id)));
        }
    }
    targets
}

/// Reconcile this agent's connector containers against the deployed bundle, then
/// generate the connector compose overlay and bring the declared set up.
///
/// One helper, both local deploy callers (`local deploy` and `deploy-local`), so
/// the shorthand cannot upload a source-built bundle and start no connector.
/// The RESOLVED identity is passed in rather than re-derived from `plugin.json`:
/// `--agent`/`--target` can override the manifest's name, and a helper that read
/// it again would alias the connectors under one identity while the runner
/// dialed another.
pub async fn bring_up_local(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: &crate::connector_build::ConnectorLockFileDecl,
    identity: &crate::connector_build::ConnectorScope,
    project: &str,
) -> Result<()> {
    use crate::connector_build as cb;

    let hosted: Vec<(&String, &cb::ConnectorSpecDecl)> = decl
        .connectors
        .iter()
        .filter(|(_, spec)| spec.url.is_none() && spec.unhosted_url.is_none())
        .collect();
    let connector_start_timeout = if hosted.is_empty() {
        DEFAULT_CONNECTOR_START_TIMEOUT
    } else {
        connector_start_timeout_from_env()?
    };

    // Fail closed BEFORE anything is reaped, staged, written, or started -- the
    // same refusal `skill up` performs, which this path used to skip. A declared
    // secret with no value here would otherwise be written into the overlay as a
    // `${NAME}` nothing populates: compose warns, expands it to empty, and the
    // connector comes up authenticating with nothing. It runs above the reap so
    // a bundle that cannot come up does not first tear down the connectors that
    // are serving.
    refuse_missing_connector_secrets(decl)?;

    // Reconcile before starting: compose only ADDS the services the overlay
    // names, so a connector this bundle version dropped or renamed would keep
    // running -- serving the runner an MCP endpoint the bundle no longer
    // declares. It is deliberately NOT `--remove-orphans` and not a
    // project-label sweep: this compose project holds the api/worker stack and
    // every other locally deployed agent's connectors, so the reap is scoped to
    // this agent's own containers and, within those, to the names the new
    // desired set does not contain. A zero-connector bundle reaches this with an
    // empty desired set, which is how the last one gets removed.
    let desired: std::collections::BTreeSet<String> = hosted
        .iter()
        .map(|(connector, _)| {
            cb::object_name(&identity.release, &identity.agent, connector.as_str())
        })
        .collect();
    for problem in docker::reap_undesired_connectors(project, &identity.agent, &desired).await {
        crate::ui::ui().warn(&problem);
    }

    if hosted.is_empty() {
        return Ok(());
    }

    let connector_services: Vec<(String, String)> = hosted
        .iter()
        .map(|(connector, _)| {
            (
                connector.to_string(),
                cb::object_name(&identity.release, &identity.agent, connector.as_str()),
            )
        })
        .collect();

    let mut secret_values = std::collections::BTreeMap::new();
    for (connector, spec) in &hosted {
        for name in cb::declared_secret_names(spec) {
            // The refusal above already proved every one of these resolves, so a
            // gap here is reported as the missing credential it is rather than
            // silently skipped.
            let value = resolve_connector_secret(&name)?
                .ok_or_else(|| cb::missing_secrets_error(std::slice::from_ref(&name)))?;
            secret_values.insert(name, value);
        }
        // Credential files are staged where both emitters expect them, so the
        // container finds bytes rather than an empty mount.
        for (key, declared_path) in &spec.secret_files {
            let value = resolve_connector_secret(key)?
                .ok_or_else(|| cb::missing_secrets_error(std::slice::from_ref(key)))?;
            cb::stage_secret_file(plugin_dir, connector, declared_path, &value)?;
        }
    }

    let caller_key = crate::connector_caller::generate_keypair()?;
    let overlay = cb::compose_overlay(
        lock,
        decl,
        identity,
        project,
        plugin_dir,
        Some(&caller_key.verify_key),
    )?;
    let path = cb::compose_overlay_path(plugin_dir);
    std::fs::create_dir_all(path.parent().expect("the overlay path has a parent"))?;
    std::fs::write(
        &path,
        serde_norway::to_string(&overlay).context("serialize the connector compose overlay")?,
    )
    .with_context(|| format!("write {}", path.display()))?;

    // The values reach the containers through the compose child's environment,
    // where `${NAME}` in the file above expands from -- never through the file,
    // never through argv, and masked in anything printed.
    let command = cb::compose_up_command(&path, project, &secret_values, connector_start_timeout);
    // Compose receives the configured readiness limit exactly. The client gets
    // a separate five second allowance for process creation and polling
    // overhead. ID resolution and final diagnostics retain their own clocks.
    let compose_client_timeout = connector_start_timeout
        .checked_add(CONNECTOR_COMPOSE_CLIENT_ALLOWANCE)
        .expect("the connector timeout parser validated the client allowance");
    let activation_context = "the API deployment was activated and was not rolled back, but starting the bundle's connectors failed";
    let compose_succeeded =
        match tokio::time::timeout(compose_client_timeout, crate::ops::run_capture(&command)).await
        {
            // `compose up --wait` can identify an exited or unhealthy service, but
            // its text is not a stable connector diagnostic. The shared Docker
            // waiter below reads the owned service IDs and reports the named reason
            // without exposing Compose, container, or health check logs.
            Ok(Ok((ok, _out, _err))) => ok,
            Err(_) => false,
            Ok(Err(err)) => return Err(err).context(activation_context),
        };
    // Compose can return ready at the end of its configured readiness limit.
    // Resolve the declared keys to actual Docker IDs in a separately
    // bounded window so that work cannot consume the five-second readiness
    // observation a no-health connector needs to prove uninterrupted running.
    let resolution_deadline = Instant::now() + docker::CONNECTOR_ID_RESOLUTION_TIMEOUT;
    let readiness_targets = compose_connector_readiness_targets(
        &path,
        project,
        &connector_services,
        resolution_deadline,
    )
    .await;
    // The waiter starts this fresh clock only after every ID lookup has
    // completed. This never turns a failed Compose invocation into success.
    let mut readiness =
        docker::wait_for_connectors_ready(&readiness_targets, docker::CONNECTOR_DIAGNOSTIC_TIMEOUT)
            .await;
    if !compose_succeeded && readiness.is_ok() {
        let connector_keys = connector_services
            .iter()
            .map(|(connector, _)| connector.clone())
            .collect::<Vec<_>>();
        readiness = Err(docker::connector_compose_start_failure(&connector_keys));
    }
    if let Err(err) = readiness {
        let (_, remedy) = crate::exit::classify(&err);
        let message = format!("{activation_context}: {err}");
        return Err(crate::exit::operator_context(err, message, remedy));
    }
    Ok(())
}

/// Refuse the WHOLE bundle when a hosted connector declares a secret this box
/// must not hand it, or has no value for -- before a network, a build, or a
/// container exists. Both tiers' single pre-resolution gate, so a name the
/// bundle must not own is refused here rather than at each caller.
///
/// Bring-up used to skip an unresolved secret silently: `skill up` reported
/// success, the connector container exited 1 on its own missing-credential
/// check, and the runner was left holding an MCP URL that connection-refused
/// mid-turn. Between a silent connection-refused mid-turn and an actionable
/// refusal at bring-up, the refusal is the correct behavior. Every gap across
/// every hosted connector is collected first so one run names them all, and the
/// check is resolve-only -- nothing is staged and no value is retained.
pub(super) fn refuse_missing_connector_secrets(
    decl: &crate::connector_build::ConnectorsFileDecl,
) -> Result<()> {
    // Ahead of every resolve below: a name the bundle must not own is refused
    // before a single value is read, because reading it IS the exfiltration --
    // the sweep below would resolve the operator's own model credential and
    // report the declaration as satisfied.
    crate::connector_build::refuse_reserved_secret_names(decl)?;
    // Then, so a `from_secret` reference keeps its own, more specific refusal
    // (`refuse_out_of_band_secrets` names all three ways forward) instead of
    // being reported as an ordinary unset name by the sweep below.
    for (connector, spec) in &decl.connectors {
        if spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        crate::connector_build::refuse_out_of_band_secrets(connector, spec)?;
    }
    let mut missing = Vec::new();
    for name in crate::connector_build::hosted_secret_names(decl) {
        if resolve_connector_secret(&name)?.is_none() {
            missing.push(name);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    Err(crate::connector_build::missing_secrets_error(&missing))
}

/// One connector credential, from the environment first and the host vault
/// second. Never printed.
pub(super) fn resolve_connector_secret(name: &str) -> Result<Option<String>> {
    if let Some(value) = std::env::var(name).ok().filter(|v| !v.is_empty()) {
        return Ok(Some(value));
    }
    crate::secrets::get_value(name)
}

/// Which hosted connectors a skill-tier boot starts, each paired with the image
/// it runs, in declaration order.
///
/// The selection is `connector_build::resolved_image`'s, which is also what the
/// local tier's overlay emits -- one rule, so the two tiers cannot resolve the
/// same declaration to two different images. Extracted as a pure seam for two
/// reasons: it is decidable with no Docker daemon, and it leaves the starter
/// below with no image of its own to compute. The inline lock-first copy it
/// replaces is what let a stale lock entry hijack a connector whose declaration
/// had since switched from `build:` to `image:`.
///
/// Every image is resolved before the first container starts, so a bundle
/// missing one is refused whole rather than half-started.
pub fn skill_connector_plan(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: &crate::connector_build::ConnectorLockFileDecl,
) -> Result<Vec<(String, String)>> {
    let mut plan = Vec::new();
    for (connector, spec) in &decl.connectors {
        if spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        plan.push((
            connector.clone(),
            crate::connector_build::resolved_image(connector, spec, lock)?,
        ));
    }
    Ok(plan)
}

/// Start one container per hosted connector on the runner's private network.
///
/// Returns the container names so `skill down` reaps exactly what this boot
/// created. Credentials are staged first, so each container finds bytes at its
/// declared mount path rather than an empty file.
pub(super) async fn start_skill_connectors(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
    identity: &crate::connector_build::ConnectorScope,
    network: &str,
    project: &str,
) -> Result<Vec<String>> {
    use crate::connector_build as cb;

    let readiness_timeout = connector_start_timeout_from_env()?;
    let lock = cb::load_lock(plugin_dir)?.unwrap_or_else(|| cb::ConnectorLockFileDecl {
        version: cb::LOCK_VERSION,
        connectors: std::collections::BTreeMap::new(),
        runner: None,
    });
    let mut started = Vec::new();
    let mut readiness_targets = Vec::new();
    for (connector, image) in skill_connector_plan(decl, &lock)? {
        let connector = connector.as_str();
        let spec = decl
            .connectors
            .get(connector)
            .expect("the plan names only this declaration's own connectors");
        cb::refuse_out_of_band_secrets(connector, spec)?;
        let mut secret_values = std::collections::BTreeMap::new();
        for name in cb::declared_secret_names(spec) {
            if let Some(value) = resolve_connector_secret(&name)? {
                secret_values.insert(name, value);
            }
        }
        for (key, declared_path) in &spec.secret_files {
            if let Some(value) = resolve_connector_secret(key)? {
                cb::stage_secret_file(plugin_dir, connector, declared_path, &value)?;
            }
        }
        let caller_key = crate::connector_caller::generate_keypair()?;
        let start = docker::ConnectorStartSpec::from_declaration(
            connector,
            spec,
            &image,
            identity,
            network,
            project,
            plugin_dir,
            &secret_values,
            Some(&caller_key.verify_key),
        )?;
        docker::docker_with_env(&start.run_args(), &start.docker_env)
            .await
            .with_context(|| format!("starting connector '{connector}'"))?;
        readiness_targets.push((connector.to_string(), start.container_name.clone()));
        started.push(start.container_name);
    }
    docker::wait_for_connectors_ready(&readiness_targets, readiness_timeout).await?;
    Ok(started)
}

#[cfg(test)]
mod single_platform_preflight_tests {
    use super::*;
    use std::collections::BTreeSet;

    const DOCKER_V2_MANIFEST: &str = r#"{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":601,"digest":"sha256:0d64a902c265bc2c08f0b5978ac2641b0ed5bf60dead4de516ace9263308097e"},"layers":[]}"#;

    const OCI_INDEX: &str = r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","size":500,"digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","platform":{"os":"linux","architecture":"amd64"}}]}"#;

    const IMAGE: &str = "localhost:5001/factory/tempo@sha256:2222222222222222222222222222222222222222222222222222222222222222";

    // B3
    #[test]
    fn a_plain_manifest_names_its_config_digest() {
        assert_eq!(
            single_manifest_config_digest(DOCKER_V2_MANIFEST).as_deref(),
            Some("sha256:0d64a902c265bc2c08f0b5978ac2641b0ed5bf60dead4de516ace9263308097e")
        );
    }

    #[test]
    fn an_index_has_no_single_config_digest() {
        assert_eq!(single_manifest_config_digest(OCI_INDEX), None);
    }

    // B4
    #[test]
    fn registry_config_argv_asks_docker_for_the_image_config() {
        let cmd = registry_config_argv("localhost:35719/probe:1");
        let rendered = format!("{cmd:?}");
        for part in [
            "docker",
            "buildx",
            "imagetools",
            "inspect",
            "localhost:35719/probe:1",
            "--format",
            "{{json .Image}}",
        ] {
            assert!(rendered.contains(part), "{part} missing from {rendered}");
        }
        assert_eq!(
            config_platform(
                "{\n  \"architecture\": \"amd64\",\n  \"os\": \"linux\",\n  \"config\": {}\n}"
            )
            .as_deref(),
            Some("linux/amd64")
        );
    }

    #[test]
    fn config_platform_reads_os_and_architecture() {
        assert_eq!(
            config_platform(r#"{"os":"linux","architecture":"amd64","config":{}}"#).as_deref(),
            Some("linux/amd64")
        );
    }

    // B5
    #[test]
    fn a_single_platform_push_deploys_to_matching_nodes() {
        registry_preflight(
            IMAGE,
            Ok(DOCKER_V2_MANIFEST),
            &BTreeSet::from(["amd64".to_string()]),
            &["linux/amd64".to_string(), "linux/arm64".to_string()],
            Some("linux/amd64"),
        )
        .expect("a single-platform amd64 push covers amd64 nodes");
    }

    #[test]
    fn a_single_platform_push_is_refused_on_other_architecture_nodes() {
        let error = registry_preflight(
            IMAGE,
            Ok(DOCKER_V2_MANIFEST),
            &BTreeSet::from(["arm64".to_string()]),
            &["linux/amd64".to_string(), "linux/arm64".to_string()],
            Some("linux/amd64"),
        )
        .expect_err("an amd64-only push cannot run on arm64 nodes");
        let text = format!("{error:#}");
        assert!(
            text.contains("arm64"),
            "the refusal should name arm64: {text}"
        );
        assert!(
            text.contains(IMAGE),
            "the refusal should name the image: {text}"
        );
    }
}
