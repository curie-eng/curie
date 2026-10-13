//! `curie build --repo-runner`: build, probe and push a repository runner image
//! (ADR 0216 decisions 2-5). The pure pieces live in `crate::repo_runner`.

use super::*;
use crate::repo_runner as rr;

/// The flags `curie build --repo-runner` carries.
pub struct RepoRunnerBuildOpts {
    /// The build input file (YAML or JSON).
    pub spec: PathBuf,
    /// The registry the image is pushed to, e.g. `ghcr.io/acme-corp`.
    pub registry: String,
    /// The platform runner to build on; `None` is the runner `curie skill up` runs.
    pub runner_image: Option<String>,
}

impl crate::ui::CliOutput for rr::RepoRunnerBuildOutput {
    fn to_json(&self) -> serde_json::Value {
        // Delegated wholesale to the `Serialize` value so no field is dropped
        // on the emit hop.
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match serde_norway::to_string(&self.runner) {
            Ok(yaml) => ui.payload_plain(yaml.trim_end()),
            Err(err) => ui.note(&format!("could not render the runner block: {err}")),
        }
        ui.note(&format!(
            "paste the block above under factory.bindings[].repos[].runner in your Helm values; \
             the image carries its tool manifest at {}",
            self.manifest_path
        ));
    }
}

/// Build the repository runner image the input file describes for both
/// platforms in one `docker buildx bake`, probe every tool inside it, push it,
/// and report its digests.
pub async fn build_repo_runner(opts: RepoRunnerBuildOpts) -> Result<rr::RepoRunnerBuildOutput> {
    use crate::connector_build as cb;

    let loaded = rr::load_spec(&opts.spec)?;
    if !crate::ops::on_path("docker") {
        bail!(
            "Docker is not installed or not on PATH. Install Docker \
             (https://docs.docker.com/get-docker/) and retry."
        );
    }
    let image = crate::artifacts::resolve_image(
        opts.runner_image.as_deref(),
        crate::artifacts::Channel::current(),
        crate::artifacts::version(),
    );
    let (base_ref, _) = resolve_runner_base(&image, true).await?;
    let base_digest = rr::bare_digest(&base_ref)?;
    let dockerfile_digest = match &loaded.spec.dockerfile {
        Some(dockerfile) => Some(cb::source_digest_of(
            &loaded.dir,
            &cb::ConnectorBuildDecl {
                context: ".".to_string(),
                dockerfile: dockerfile.clone(),
                platforms: rr::PLATFORMS.iter().map(|p| p.to_string()).collect(),
            },
        )?),
        None => None,
    };
    let tag = rr::input_tag(&loaded.spec, dockerfile_digest.as_deref(), &base_digest);
    let registry = opts.registry.trim().trim_end_matches('/');
    let image_ref = format!("{registry}/{}:{tag}", rr::IMAGE_NAME);

    // The bake file, the metadata buildx writes and the generated targets'
    // empty context, in a directory only the owner can enter.
    let work = std::env::temp_dir().join(format!("curie-repo-runner-{}", uuid::Uuid::new_v4()));
    create_private_dir(&work).with_context(|| format!("create {}", work.display()))?;
    let result = bake_and_inspect(&work, &loaded, &image_ref, &base_ref).await;
    let _ = std::fs::remove_dir_all(&work);
    let (index_digest, platform_digests) = result?;

    Ok(rr::RepoRunnerBuildOutput {
        runner: rr::RunnerRecord {
            tools: rr::selected_entries(&loaded.spec)
                .iter()
                .map(|e| e.name.clone())
                .collect(),
            image: format!("{image_ref}@{index_digest}"),
            base: base_digest,
            platform_digests,
            dockerfile: loaded.spec.dockerfile.clone(),
            probes: loaded.spec.probes.clone(),
        },
        platform_runner: base_ref,
        catalog_version: rr::CATALOG_VERSION,
        manifest_path: rr::MANIFEST_PATH.to_string(),
    })
}

async fn bake_and_inspect(
    work: &Path,
    loaded: &rr::LoadedSpec,
    image_ref: &str,
    base_ref: &str,
) -> Result<(String, BTreeMap<String, String>)> {
    use crate::connector_build as cb;

    let empty_context = work.join("context");
    create_private_dir(&empty_context)
        .with_context(|| format!("create {}", empty_context.display()))?;
    let plan = rr::BakePlan {
        image_ref: image_ref.to_string(),
        base_ref: base_ref.to_string(),
        spec: loaded.spec.clone(),
        dockerfile: loaded
            .spec
            .dockerfile
            .as_ref()
            .map(|rel| (loaded.dir.clone(), rel.clone())),
        empty_context,
    };
    let bake_file = work.join("bake.json");
    let definition = serde_json::to_vec_pretty(&rr::bake_definition(&plan))?;
    std::fs::write(&bake_file, definition)
        .with_context(|| format!("write {}", bake_file.display()))?;
    let metadata_file = work.join("metadata.json");

    // Bake refuses a local context outside its working directory unless the
    // read is granted. Grant exactly the contexts the bake file names: the
    // private empty context, and the input file's directory when a Dockerfile
    // is declared. Never the blanket `BUILDX_BAKE_ENTITLEMENTS_FS=0`.
    let mut argv: Vec<String> = vec!["buildx".into(), "bake".into()];
    let mut granted = vec![plan.empty_context.clone()];
    if let Some((context, _)) = &plan.dockerfile {
        granted.push(context.clone());
    }
    for path in &granted {
        argv.push(format!("--allow=fs.read={}", path.display()));
    }
    argv.extend([
        "-f".into(),
        bake_file.display().to_string(),
        "--metadata-file".into(),
        metadata_file.display().to_string(),
        rr::BAKE_TARGET.into(),
    ]);
    let command = cb::plain_command("docker", argv);
    let ui = crate::ui::ui();
    ui.note(&format!("=== {} ===", command.display()));
    // Inherit stdout so the build log streams; tee stderr so a driver refusal
    // can be answered with a fix.
    let (status, stderr) = run_teeing_stderr(&command).await?;
    if !status.success() {
        let message = format!(
            "repository runner build failed ({status}); a failing probe or catalog layer fails \
             the build"
        );
        // `--platform` is not offered here: a repository runner is always
        // both platforms, so the only fix is a builder that can push them.
        if multi_platform_driver_fix(&stderr, &cb::host_platform()).is_some() {
            let fix = "the default Docker driver cannot push a multi-platform image; create a \
                       docker-container builder (`docker buildx create --driver docker-container \
                       --use`) or point BUILDX_BUILDER at one, then rerun"
                .to_string();
            return Err(anyhow::Error::from(
                crate::exit::CliError::failure(format!("{message}: {fix}")).with_fix(fix),
            ));
        }
        bail!("{message}");
    }

    let raw = std::fs::read_to_string(&metadata_file)
        .with_context(|| format!("read {}", metadata_file.display()))?;
    let metadata: serde_json::Value =
        serde_json::from_str(&raw).context("parse the buildx bake metadata file")?;
    let target = metadata.get(rr::BAKE_TARGET).ok_or_else(|| {
        anyhow::anyhow!(
            "the buildx bake metadata names no `{}` target result",
            rr::BAKE_TARGET
        )
    })?;
    let index_digest = cb::digest_from_metadata(&target.to_string())?;

    let pinned = cb::digest_pinned_ref(image_ref, &index_digest);
    let inspect = registry_manifest_argv(&pinned);
    let (ok, stdout, stderr) = crate::ops::run_capture(&inspect).await?;
    if !ok {
        bail!(
            "could not read back the pushed index {pinned}: {}",
            stderr.trim()
        );
    }
    let platform_digests = rr::platform_digests_from_index(&stdout)
        .with_context(|| format!("read the platform digests of {pinned}"))?;
    check_layers(base_ref, &pinned).await?;
    Ok((index_digest, platform_digests))
}

/// `docker buildx imagetools inspect <ref> --format '{{json .Image}}'`, parsed
/// to each built platform's layer diff IDs.
async fn layer_ids(reference: &str) -> Result<BTreeMap<String, Vec<String>>> {
    let inspect = registry_config_argv(reference);
    let (ok, stdout, stderr) = crate::ops::run_capture(&inspect).await?;
    if !ok {
        bail!(
            "could not read the image config of {reference}: {}",
            stderr.trim()
        );
    }
    rr::platform_diff_ids(&stdout).with_context(|| format!("read the layers of {reference}"))
}

/// The pushed image must extend the platform runner the receipt records as its
/// base; a Dockerfile whose final stage starts elsewhere fails here.
async fn check_layers(base_ref: &str, pinned: &str) -> Result<()> {
    let base = layer_ids(base_ref).await?;
    let image = layer_ids(pinned).await?;
    rr::check_builds_on_base(&base, &image).with_context(|| {
        format!("{pinned} was pushed but must not be used; the pushed tag is not a valid repository runner")
    })
}
