//! `skill up`, `down`, `status`, and `message`: the local runner lifecycle.

use super::*;

/// Options for `curie skill up`, mirroring its clap flags.
pub struct StartOpts {
    pub plugin_dir: PathBuf,
    pub image: String,
    pub port: u16,
    pub name: String,
    pub fake_model: bool,
    pub network: Option<String>,
    pub otel_endpoint: Option<String>,
    pub budget: String,
    pub model: Option<String>,
    pub local_model: Option<String>,
    /// Opt in to downloading the `--local-model` assets this run (ADR 0093).
    /// Without it, `up` refuses when the pinned Ollama image or the requested
    /// model is not already on the machine. From `skill up --pull-model`.
    pub pull_model: bool,
    /// Extra env var NAMES to forward by name into the runner sandbox, for a
    /// bundle's authed MCP server to read a secret. Forwarded exactly like the
    /// model credentials (docker reads the value from the caller's env; the
    /// value never appears in argv). From `skill up --secret <NAME>`.
    pub secret: Vec<String>,
    /// Opt-in path to a bundle-local `.env` read as the LOWEST-priority model-
    /// credential source (#749, ADR-0070): shell env > vault > this file. Only
    /// the recognized credential names are read; every other key is ignored.
    /// From `skill up --env-file <PATH>`.
    pub env_file: Option<PathBuf>,
    /// Remove a pre-existing container of the same name before booting, instead
    /// of failing on the conflict. From `skill up --replace` (#747).
    pub replace: bool,
}

/// What a recorded runner means for a fresh `skill up` (#747, #1905).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedStatePlan {
    /// Nothing recorded; boot.
    Proceed,
    /// Tear the recorded runner down and boot. Either `--replace` names that
    /// exact container, or a verified same-bundle identity is serving a stale
    /// snapshot of this source.
    ClearAndProceed,
    /// The recorded runner is this bundle and already serves the current
    /// source; do not restart.
    AlreadyRunning,
    /// A runner is recorded that this `up` must not remove: a different name,
    /// a foreign or unverifiable identity, or a snapshot that cannot be
    /// compared. Refuse so a second bundle's live runner cannot be silently
    /// forgotten.
    Refuse,
}

/// Inputs to [`plan_recorded_state`]. Identity fields are optional so the
/// `--replace` name gate stays testable without Docker; auto-reload requires
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedStateQuery<'a> {
    pub recorded_name: Option<&'a str>,
    pub target_name: &'a str,
    pub replace: bool,
    pub recorded_id: Option<&'a str>,
    pub live_id: Option<&'a str>,
    pub same_bundle_dir: bool,
    pub recorded_digest: Option<&'a str>,
    pub current_digest: Option<&'a str>,
}

/// Whether a recorded container id and a `docker ps` id name the same
/// container. `docker ps` reports a 12-char short id while `docker run`
/// returns the full 64-char one, so identity is a prefix match, not equality.
/// An empty recorded id cannot be compared and is not a match: auto-reload
/// must not treat an unverified identity as owned (#1905 / #747).
pub fn recorded_ids_match(recorded_id: &str, live_id: &str) -> bool {
    !recorded_id.is_empty()
        && (recorded_id.starts_with(live_id) || live_id.starts_with(recorded_id))
}

/// Resolve the recorded-state gate. Pure so every branch is testable without a
/// bundle on disk or a Docker daemon.
///
/// `--replace` still clears only the record for the exact target name: a record
/// naming a DIFFERENT runner still blocks, since removing one container is no
/// reason to forget another (#747). Without `--replace`, a verified
/// same-directory runner whose snapshot no longer matches this source is
/// replaced automatically (#1905); an unverified identity never is.
pub fn plan_recorded_state(q: RecordedStateQuery<'_>) -> RecordedStatePlan {
    let Some(recorded) = q.recorded_name else {
        return RecordedStatePlan::Proceed;
    };
    if q.replace && recorded == q.target_name {
        return RecordedStatePlan::ClearAndProceed;
    }
    if recorded != q.target_name {
        return RecordedStatePlan::Refuse;
    }
    let identity_verified = q.same_bundle_dir
        && match (q.recorded_id, q.live_id) {
            (Some(recorded_id), Some(live_id)) => recorded_ids_match(recorded_id, live_id),
            _ => false,
        };
    if !identity_verified {
        return RecordedStatePlan::Refuse;
    }
    match (q.recorded_digest, q.current_digest) {
        (Some(recorded_digest), Some(current_digest)) if recorded_digest == current_digest => {
            RecordedStatePlan::AlreadyRunning
        }
        (Some(_), Some(_)) => RecordedStatePlan::ClearAndProceed,
        _ => RecordedStatePlan::Refuse,
    }
}

/// The leading characters of a bundle digest, for a one-line summary (#1087).
/// Taken by character rather than by byte slice so an unexpectedly short digest
/// truncates instead of panicking.
pub(super) fn short_digest(digest: &str) -> String {
    digest.chars().take(12).collect()
}

/// Release everything a boot that has already packed its snapshot leaves behind
/// when it aborts: the ollama sidecar, the hosted connectors, the network
/// `start` owns, the credentials it staged, and the snapshot itself.
///
/// One definition for all three abort arms between the pack and a recorded
/// state, because nothing else will ever collect these: no state was saved, so
/// no `skill down` can find them (#1087). A fourth abort path added later gets
/// the whole sequence by calling this rather than by remembering three lines.
///
/// Order is load bearing: the connectors are attached to the owned network, and
/// `docker network rm` fails while any container is still on it, so every
/// connector goes before the network does (ADR 0113).
///
/// Every release is best effort and deliberately swallows its error: these run
/// while a command is already failing for its own reason, and must not change
/// the error it reports or the code it exits with.
pub(super) async fn release_boot_scaffolding(
    ollama_container: Option<&String>,
    connector_containers: &[String],
    owned_network: Option<&String>,
    snapshot_dir: &Path,
    plugin_dir: &Path,
) {
    if let Some(ollama) = ollama_container {
        let _ = docker::remove_container(ollama).await;
    }
    for connector in connector_containers {
        let _ = docker::remove_container(connector).await;
    }
    if let Some(net) = owned_network {
        let _ = docker::remove_network(net).await;
    }
    // After the connectors, never before: a tree still bind-mounted into a
    // running container cannot be wiped cleanly, the same ordering
    // `connector_teardown_plan` holds. Resolved credentials must not outlive a
    // boot that no `skill down` can find.
    let _ = crate::connector_build::wipe_connector_secrets(plugin_dir);
    let _ = crate::bundle::remove_snapshot(snapshot_dir, plugin_dir);
}

pub async fn start(opts: StartOpts) -> Result<()> {
    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    // Fail fast on a directory that is not a bundle; the runner would reject
    // it at boot anyway (real-model mode), with a worse error surface.
    let (plugin_name, manifest_version) = read_manifest(&plugin_dir)?;

    if opts.local_model.is_some() && opts.fake_model {
        return Err(crate::exit::usage(
            "--local-model cannot be combined with --fake-model",
        ));
    }
    if opts.local_model.is_some() && opts.model.is_some() {
        return Err(crate::exit::usage(
            "--local-model cannot be combined with --model",
        ));
    }

    // Decided here, ACTED ON below: refusing is free, but replacing tears down a
    // live runner and must not happen until nothing cheap can still abort (#747).
    let recorded_runner = state::load(&plugin_dir)?;
    // `--replace` decides from the name alone and must stay a cheap abort
    // until budget / model preflights pass (#747). Auto-reload needs the live
    // container id, so only probe Docker when that path could fire.
    let live_for_plan = match recorded_runner.as_ref() {
        Some(saved) if !opts.replace => docker::container_facts(&saved.container_name).await?,
        _ => None,
    };
    let same_bundle_dir = recorded_runner.as_ref().is_some_and(|saved| {
        Path::new(&saved.plugin_dir)
            .canonicalize()
            .ok()
            .is_some_and(|recorded| recorded == plugin_dir)
    });
    let identity_verified = recorded_runner.as_ref().is_some_and(|saved| {
        same_bundle_dir
            && live_for_plan
                .as_ref()
                .is_some_and(|facts| recorded_ids_match(&saved.container_id, &facts.id))
    });
    // Packing is cheap relative to teardown and is how we know the snapshot is
    // stale. Skip it unless auto-reload could actually fire: `--replace` does
    // not need a digest, and an unverified identity must still refuse.
    let current_digest = if identity_verified && !opts.replace {
        crate::bundle::digest_source(&plugin_dir).ok()
    } else {
        None
    };
    let recorded_plan = plan_recorded_state(RecordedStateQuery {
        recorded_name: recorded_runner.as_ref().map(|s| s.container_name.as_str()),
        target_name: &opts.name,
        replace: opts.replace,
        recorded_id: recorded_runner.as_ref().map(|s| s.container_id.as_str()),
        live_id: live_for_plan.as_ref().map(|f| f.id.as_str()),
        same_bundle_dir,
        recorded_digest: recorded_runner
            .as_ref()
            .and_then(|s| s.bundle_digest.as_deref()),
        current_digest: current_digest.as_deref(),
    });
    if recorded_plan == RecordedStatePlan::Refuse {
        let recorded_name = &recorded_runner
            .as_ref()
            .expect("a refusal requires a recorded runner")
            .container_name;
        return Err(crate::exit::usage(format!(
            "a local runner is already recorded in {}/.curie/runner.json; run 'curie skill down' there first, or rerun 'curie skill up --replace --name {recorded_name}' to replace it",
            plugin_dir.display(),
        )));
    }
    if recorded_plan == RecordedStatePlan::AlreadyRunning {
        let saved = recorded_runner.expect("already-running requires a recorded runner");
        let ui = crate::ui::ui();
        ui.success(&format!(
            "runner '{}' is already running this bundle snapshot",
            saved.container_name
        ));
        if let Some(digest) = saved.bundle_digest.as_deref() {
            ui.note(&format!("bundle {}", short_digest(digest)));
        }
        return Ok(());
    }

    // Parse (not just forward) the budget so a typo fails here, not in-container.
    let _: Budget = serde_json::from_str(&opts.budget).map_err(|e| {
        crate::exit::usage(format!(
            "--budget is not a valid ACI budget: {}: {e}",
            opts.budget
        ))
    })?;

    // The sidecar's container name, derived once here so the preflight probe
    // below and the sidecar setup after the teardown derive their volume from
    // one name and cannot drift.
    let ollama = format!("{}-ollama", opts.name);

    // ADR 0093: preflight --local-model before ANY teardown below, since
    // refusing is free but replacing tears down a live runner (the #747
    // invariant above). This is the same refusal `local up` gives, so both
    // tiers answer `--local-model` identically (ADR 0041). The skill tier's
    // model cache is the sidecar's own volume, not compose's -- and
    // stop_recorded explicitly keeps that volume, so this preflight returns
    // the identical answer whether it runs here or after the teardown;
    // moving it earlier is purely about ordering, not about its verdict.
    if let Some(local_model) = &opts.local_model {
        if !opts.pull_model {
            docker::preflight_local_model(
                DEFAULT_OLLAMA_IMAGE,
                &docker::ollama_volume(&ollama),
                local_model,
                &format!(
                    "curie skill up --local-model {local_model} --pull-model --name {}",
                    opts.name
                ),
            )
            .await?;
        }
    }

    // The replacement itself, once every cheap validation has passed. Tearing
    // down the record means EVERYTHING it describes -- container, ollama sidecar,
    // network, then the file -- because clearing the record while its runner is
    // still live strands exactly the untracked orphan this ticket removes (#747).
    if let (RecordedStatePlan::ClearAndProceed, Some(saved)) = (recorded_plan, recorded_runner) {
        let reason = if opts.replace {
            "--replace: tearing down the recorded runner"
        } else {
            "bundle changed: replacing the recorded runner"
        };
        crate::ui::ui().note(&format!("{reason} '{}' first", saved.container_name));
        let live = docker::container_facts(&saved.container_name).await?;
        stop_recorded(&plugin_dir, crate::ui::ui(), saved, live.as_ref()).await?;
    }

    // Catch a leftover container of the same name here, before anything is
    // booted, so the operator gets the remedies instead of docker's raw
    // exit-125 conflict at the very end of the boot (#747).
    docker::ensure_container_name_free(
        &opts.name,
        Some(opts.port),
        opts.replace,
        docker::ConflictContext::SkillUp,
    )
    .await?;

    let session_id = format!("local-{}", unix_now());
    let mut network = opts.network.clone();
    let mut owned_network: Option<String> = None;
    let mut ollama_container: Option<String> = None;
    let mut model_base_url: Option<String> = None;
    let mut model = opts.model.clone();

    if let Some(local_model) = &opts.local_model {
        // The sidecar is derived from the same --name, so a leftover
        // `<name>-ollama` is the same wedge one step over (#747). Catch it
        // before creating anything, and let --replace cover it too. (The
        // --local-model preflight itself, when --pull-model is absent, already
        // ran above the teardown against this same binding.)
        // No host port on the sidecar, so the remedy never offers --port.
        docker::ensure_container_name_free(
            &ollama,
            None,
            opts.replace,
            docker::ConflictContext::SkillUp,
        )
        .await?;
        let (net, owned) = match &opts.network {
            Some(net) => (net.clone(), false),
            None => (format!("{}-net", opts.name), true),
        };
        if owned {
            // Only claim ownership (and teardown responsibility) when this call
            // actually created the network; a pre-existing one is not ours to rm.
            let created = docker::create_network(&net).await?;
            if created {
                owned_network = Some(net.clone());
            }
        }
        if let Err(err) = docker::run_ollama(&ollama, &net, DEFAULT_OLLAMA_IMAGE).await {
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(docker::map_name_conflict(
                err.context("starting local model container"),
                &ollama,
                None,
                docker::ConflictContext::SkillUp,
            ));
        }
        if let Err(err) = docker::wait_ollama_ready(&ollama, Duration::from_secs(120)).await {
            let _ = docker::remove_container(&ollama).await;
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err.context("waiting for local model container"));
        }
        if let Err(err) = docker::pull_model(&ollama, local_model).await {
            let _ = docker::remove_container(&ollama).await;
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err.context("pulling local model"));
        }
        let url = format!("http://{ollama}:{OLLAMA_PORT}");
        network = Some(net);
        ollama_container = Some(ollama);
        model_base_url = Some(url);
        model = Some(local_model.clone());
    }

    // Forward exactly one model credential (or none under fake/local) -- never
    // the ambient SDK token alongside a chosen BYO credential. See
    // select_passthrough_env.
    // `--local-model` is a base-URL override, not a fake-model run: it keeps an
    // explicit BYO credential (the runner routes it at the local endpoint) and
    // drops only the ambient SDK fallback. Derive both states the way the
    // container actually gets them, so the seam cannot drift from the argv.
    let fake_model = opts.local_model.is_none() && opts.fake_model;
    let base_url_override = model_base_url.is_some();
    let mut docker_env = Vec::new();
    if !fake_model {
        docker_env.extend(load_model_credentials_from_secret_store()?);
        // #749/ADR-0070: an opt-in bundle `.env` is the lowest-priority source
        // -- appended after the vault, filling only names the shell env and the
        // vault did not (shell env > vault > file). `select_passthrough_env`
        // below stays the frozen authority on what is actually forwarded.
        let from_env_file =
            load_model_credentials_from_env_file(opts.env_file.as_deref(), &docker_env)?;
        docker_env.extend(from_env_file);
    }
    let byo_credential = std::env::var("CURIE_CREDENTIALS")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            stored_env_contains(&docker_env, "CURIE_CREDENTIALS").then_some("stored".to_string())
        });
    // Hydrate `--secret NAME` from Curie private storage when it is not
    // already present in the process env. The docker argv still forwards only
    // the NAME (`-e NAME`); the value is supplied only to the Docker CLI child
    // process so Docker can copy it into the runner container.
    for name in &opts.secret {
        if !env_credential_present(name) && !stored_env_contains(&docker_env, name) {
            match secret_store_env(name)? {
                Some(pair) => docker_env.push(pair),
                None => {
                    crate::ui::ui().note(&format!(
                        "--secret {name}: not set in the environment or Curie secret store; nothing will be forwarded for it"
                    ));
                }
            }
        }
    }
    // Scoped so the borrow of `docker_env` ends before it is moved into the spec.
    // `model_cred_names` is kept separately from the merged list so the summary
    // below reports the MODEL credential alone -- a `--secret` name riding in
    // `passthrough_env` is not one, and counting it would let the panel claim a
    // credential for a runner that has none.
    let (model_cred_names, mut passthrough_env) = {
        let ambient_present = ambient_present_for(&docker_env);
        let model_cred_names = select_passthrough_env(
            fake_model,
            base_url_override,
            byo_credential.as_deref(),
            &ambient_present,
        );
        let passthrough = merge_secret_env(model_cred_names.clone(), &opts.secret);
        (model_cred_names, passthrough)
    };

    // Hosted-connector preflights (ADR 0113) run BEFORE the snapshot is packed:
    // the runner validates the packed snapshot, so a lock rewritten after
    // packing would leave the runner refusing the stale copy it was handed
    // while the source directory holds the fresh one.
    let connector_decl = crate::connector_build::load(&plugin_dir)?;
    // Stage each hosted connector's Bearer secret into the runner env (#2518),
    // the skill-tier twin of the cluster sandbox binding (#2503). The mounted
    // entry (including the `unhosted_url` fallback) carries
    // `Authorization: Bearer ${NAME}`, and the MCP client expands it from THIS
    // container's env; a name that never reaches it ships the literal
    // placeholder and the server answers 401. Resolved like `--secret`: the
    // shell env first, then Curie storage; only the NAME rides the argv.
    for name in crate::connector_build::hosted_env_secret_names(&connector_decl) {
        crate::connector_build::refuse_reserved_env_secret_name(&name)?;
        if passthrough_env.contains(&name) {
            continue;
        }
        if !env_credential_present(&name) && !stored_env_contains(&docker_env, &name) {
            match secret_store_env(&name)? {
                Some(pair) => docker_env.push(pair),
                None => crate::ui::ui().note(&format!(
                    "connector secret {name}: not set in the environment or Curie secret store; \
                     nothing will be forwarded for it"
                )),
            }
        }
        passthrough_env.push(name);
    }
    let declares_hosted_connectors = connector_decl
        .connectors
        .values()
        .any(|spec| spec.url.is_none() && spec.unhosted_url.is_none());
    if declares_hosted_connectors {
        // Fail closed on a missing credential BEFORE anything is created. It
        // runs ahead of the rebuild below because there is no point spending
        // minutes building images for a bring-up that will refuse, and ahead of
        // the network and every container because a partial bring-up that then
        // aborts leaks work this checks for up front instead.
        if let Err(err) = refuse_missing_connector_secrets(&connector_decl) {
            if let Some(ollama) = &ollama_container {
                let _ = docker::remove_container(ollama).await;
            }
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err);
        }
        // ADR 0113's Decision 3, and its deliberate asymmetry: at the skill tier
        // a missing or stale `connectors.lock.yaml` is REBUILT here, where the
        // source and a Docker daemon are both within reach; at the cluster tier
        // `lock_preflight` refuses instead. Without this, an edit to a connector's
        // source silently brings up the previously locked image. A fresh lock
        // computes no rebuild and issues no `docker build`, so the offline
        // guarantee `cli/CLAUDE.md` makes for `skill up` still holds. It runs
        // before the snapshot pack so the runner's copy carries the lock this
        // writes, and before `start_skill_connectors`, which re-reads the lock
        // from disk and so picks up whatever this wrote.
        let stale = connectors_needing_rebuild(
            &connector_decl,
            crate::connector_build::load_lock(&plugin_dir)?.as_ref(),
            &recompute_source_digests(&plugin_dir, &connector_decl)?,
        );
        if !stale.is_empty() {
            // Not forced: a lock resolved to a pushed registry image is the
            // operator's, and `write_lock` refuses to quietly downgrade it to a
            // local-daemon one behind a `skill up`.
            if let Err(err) = build_connectors(ConnectorBuildOpts {
                plugin_dir: plugin_dir.clone(),
                registry: None,
                runner_image: None,
                force: false,
                platforms: Vec::new(),
            })
            .await
            {
                // A build is minutes long and fails for ordinary reasons (a bad
                // Dockerfile, no daemon), so it releases what boot has created so
                // far rather than leaving an ollama sidecar to collide with the
                // operator's next attempt. No snapshot or connector container
                // exists yet.
                if let Some(ollama) = &ollama_container {
                    let _ = docker::remove_container(ollama).await;
                }
                if let Some(net) = &owned_network {
                    let _ = docker::remove_network(net).await;
                }
                return Err(err.context("rebuilding the bundle's connectors"));
            }
        }
    }

    // #1087: the skill tier executes an immutable, content-addressed snapshot,
    // not the editable source -- matching what local and cluster already do. The
    // packer is the deploy path's packer, so the digest is the same one the API
    // records for this source. Placed AFTER the --replace teardown above so a
    // re-up of unchanged source (same digest, same directory) cannot have the
    // snapshot it just created torn down again, and after the connector
    // rebuild above so the packed bundle carries the lock the runner validates.
    let snapshot = match crate::bundle::snapshot(&plugin_dir) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            // Nothing of the runner exists yet, but the ollama sidecar above may:
            // release it the same way every other abort on this path does.
            if let Some(ollama) = &ollama_container {
                let _ = docker::remove_container(ollama).await;
            }
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err.context("packaging the bundle snapshot for the runner"));
        }
    };

    // Hosted connectors (ADR 0113). A bundle that declares none takes the
    // existing hermetic path untouched; one that declares some gets a private
    // network the runner and the connectors share, and the connector scope in
    // the runner's boot env so `derive_mcp_servers` takes its normal path.
    let mut connector_containers: Vec<String> = Vec::new();
    let mut connector_network: Option<String> = None;
    if declares_hosted_connectors {
        let net = match &network {
            Some(net) => net.clone(),
            None => {
                let net = format!("{}-net", opts.name);
                if docker::create_network(&net).await? {
                    owned_network = Some(net.clone());
                }
                net
            }
        };
        let identity = crate::connector_build::ConnectorScope {
            release: "curie".to_string(),
            agent: plugin_name.clone(),
            namespace: "default".to_string(),
        };
        match start_skill_connectors(&plugin_dir, &connector_decl, &identity, &net, &session_id)
            .await
        {
            Ok(started) => connector_containers = started,
            Err(err) => {
                // Nothing is recorded yet, so this is the only chance to release
                // whatever the partial bring-up created (#747).
                let steps = docker::connector_teardown_plan(
                    &session_id,
                    owned_network.as_deref(),
                    Some(&plugin_dir),
                );
                let _ = docker::run_connector_teardown(&steps).await;
                if let Some(ollama) = &ollama_container {
                    let _ = docker::remove_container(ollama).await;
                }
                // The label teardown has removed every started connector, so
                // this unrecorded boot can now release the snapshot that no
                // later `skill down` can discover.
                let _ = crate::bundle::remove_snapshot(&snapshot.dir, &plugin_dir);
                return Err(err.context("starting the bundle's connectors"));
            }
        }
        for (key, value) in crate::connector_build::connector_scope_env(&identity) {
            passthrough_env.push(key.clone());
            docker_env.push((key, value));
        }
        network = Some(net.clone());
        connector_network = Some(net);
    }

    let spec = StartSpec {
        image: opts.image.clone(),
        container_name: opts.name.clone(),
        host_port: opts.port,
        plugin_dir: snapshot.dir.clone(),
        session_id: session_id.clone(),
        sandbox_id: "local".into(),
        budget_json: opts.budget,
        fake_model,
        network,
        otel_endpoint: opts.otel_endpoint,
        model_base_url: model_base_url.clone(),
        model,
        passthrough_env,
        docker_env,
    };

    let ui = crate::ui::ui();
    ui.note(&format!(
        "starting runner container '{}' from '{}'",
        opts.name, opts.image
    ));
    let container_id = match docker::docker_with_env(&spec.run_args(), &spec.docker_env).await {
        Ok(id) => id,
        Err(err) => {
            // Nothing recorded the snapshot, so no teardown will ever find it
            // (#1087); release it here alongside the sidecar, the connectors,
            // and the network.
            release_boot_scaffolding(
                ollama_container.as_ref(),
                &connector_containers,
                owned_network.as_ref(),
                &snapshot.dir,
                &plugin_dir,
            )
            .await;
            // The preflight above can lose the race to a container created
            // between the probe and here; map that onto the same actionable
            // error rather than docker's raw conflict (#747).
            return Err(docker::map_name_conflict(
                err.context("starting runner container"),
                &opts.name,
                Some(opts.port),
                docker::ConflictContext::SkillUp,
            ));
        }
    };

    let base_url = format!("http://localhost:{}", opts.port);
    let client = RunnerClient::new(&base_url)?;
    let cl = ui.checklist();
    let step = cl.step("waiting for runner");
    if let Err(err) = client.wait_healthy(Duration::from_secs(60)).await {
        step.fail("unhealthy");
        let logs = docker::container_logs(&opts.name, 40).await;
        ui.note(&logs);
        let _ = docker::remove_container(&opts.name).await;
        // Same as the start-failure arm above: unrecorded, so this is the only
        // chance to release it (#1087).
        release_boot_scaffolding(
            ollama_container.as_ref(),
            &connector_containers,
            owned_network.as_ref(),
            &snapshot.dir,
            &plugin_dir,
        )
        .await;
        ui.failure(&format!("runner failed to become healthy: {err}"));
        bail!("runner failed to become healthy: {err}");
    }
    step.done("healthy");

    // State lives with the bundle: init gitignores .curie/ there, and the
    // follow-up commands are documented to run from the bundle directory. If
    // the save fails (e.g. a read-only bundle), tear the container down again:
    // a live runner with no recorded state would be invisible to stop/status.
    if let Err(err) = state::save(
        &plugin_dir,
        &RunnerState {
            container_id,
            container_name: opts.name.clone(),
            image: opts.image,
            port: opts.port,
            base_url: base_url.clone(),
            session_id,
            plugin_dir: plugin_dir.display().to_string(),
            fake_model: opts.fake_model,
            ollama_container: ollama_container.clone(),
            network: owned_network.clone(),
            model_base_url: model_base_url.clone(),
            bundle_digest: Some(snapshot.digest.clone()),
            bundle_snapshot_dir: Some(snapshot.dir.display().to_string()),
            connector_containers: connector_containers.clone(),
            connector_network: connector_network.clone(),
        },
    ) {
        let _ = docker::remove_container(&opts.name).await;
        // The last of the three abort paths between the pack and a recorded
        // state; each releases the snapshot itself, so a failed boot leaves
        // nothing behind (#1087).
        release_boot_scaffolding(
            ollama_container.as_ref(),
            &connector_containers,
            owned_network.as_ref(),
            &snapshot.dir,
            &plugin_dir,
        )
        .await;
        return Err(err.context("recording runner state (container removed again)"));
    }

    let version = git_short_sha(&plugin_dir)
        .await
        .map(|sha| format!("dev @ {sha}"))
        .unwrap_or_else(|| format!("{plugin_name} @ {manifest_version}"));
    // `fake_model`, not `opts.fake_model`: a `--local-model` overrides a
    // `--fake-model` (line ~1432), and that resolved value is the one that
    // actually drove the credential selection being reported.
    let (model_credential, model_warning) =
        model_credential_summary(fake_model, opts.local_model.as_deref(), &model_cred_names);
    let rows = [
        ("Local bot", base_url),
        (
            "Skill message",
            "curie skill message \"<message>\"".to_string(),
        ),
        ("Skill eval", "curie skill eval".to_string()),
        ("Version", version),
        // What makes AC1/AC3 confirmable by eye: re-up after a source edit and
        // this visibly changes, while a source edit under a live runner does not.
        (
            "Bundle",
            format!("{} (snapshot)", short_digest(&snapshot.digest)),
        ),
        ("Model", model_credential),
    ];
    ui.payload_plain(&boxed_summary("curie dev environment", &rows));
    if let Some(warning) = model_warning {
        ui.note(&warning);
    }
    if let Some(local_model) = &opts.local_model {
        ui.note(&format!(
            "local model running in container '{}' from '{}' with model '{}'",
            ollama_container.as_deref().unwrap_or("unknown"),
            DEFAULT_OLLAMA_IMAGE,
            local_model
        ));
    }
    let cwd = Path::new(".").canonicalize()?;
    if cwd != plugin_dir {
        ui.note(&format!(
            "State recorded in {}/.curie/runner.json; run skill down from that directory. skill message, skill eval, and skill status also work there. skill message and skill eval also accept --url.",
            plugin_dir.display()
        ));
    }
    Ok(())
}

/// What `curie skill down` should tear down, resolved from the recorded state
/// and an explicit `--name` (#747).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownPlan {
    /// A runner is recorded and `container` IS it: remove it along with its
    /// ollama container and network, then clear the state file.
    Recorded { container: String },
    /// An explicit `--name` that is not the recorded runner, and that container
    /// is present: remove only it. The state file and the recorded runner's
    /// ollama container and network are left alone, since they describe a
    /// different, still-running runner (#747).
    Targeted { container: String },
    /// The same, except the named container is not there. A no-op teardown is
    /// not an error, but it must not claim a removal that never happened:
    /// `docker rm -f` exits 0 on a missing name, so absence is established by
    /// the probe rather than inferred from the removal (#747).
    TargetedAbsent { container: String },
    /// No state file, but a container of that name exists: remove it. Nothing to
    /// clear, so a stray runner is no longer un-stoppable from the CLI.
    Orphan { container: String },
    /// Nothing to remove; the message names what was looked for and the remedy.
    Nothing { message: String },
}

/// Resolve the teardown target. Pure so the no-state fallback (#747) is testable
/// without a Docker daemon or a bundle on disk. An explicit `--name` that
/// disagrees with the recorded runner is a TARGETED removal, never a reason to
/// clear state that describes a different container.
/// What the recorded teardown does once the container actually holding the
/// recorded NAME has been identified (#747).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedTeardown {
    /// The container holding the name IS the recorded one: full teardown. The
    /// verified `id` is what gets removed, never the name: another bundle could
    /// take the name between this check and the removal, and the whole point of
    /// the check is that the removal hits the container it approved (#747).
    Remove { id: String },
    /// Nothing holds the name any more: clear the record without claiming a
    /// removal that did not happen.
    AlreadyGone,
    /// A DIFFERENT container now holds the recorded name. Removing it would
    /// destroy someone else's live runner, so it is left alone and the stale
    /// record is cleared instead.
    Hijacked { message: String },
}

/// Resolve the recorded teardown by container IDENTITY rather than by name.
///
/// The name alone is not identity: another bundle's `skill up --replace` can
/// create a new container under the same name, and a later plain `skill down`
/// here would then destroy that live runner (#747). Pure so both branches are
/// testable without a Docker daemon.
pub fn plan_recorded_teardown(
    recorded_id: &str,
    container: &str,
    live_id: Option<&str>,
) -> RecordedTeardown {
    let Some(live) = live_id else {
        return RecordedTeardown::AlreadyGone;
    };
    // `docker ps` reports a 12-char short id while `docker run` returns the full
    // 64-char one, so identity is a prefix match, not equality. A record with no
    // id at all cannot be compared, so teardown keeps the old name-based
    // behavior rather than refusing to tear down. Auto-reload does NOT use this
    // empty-id fallback: [`recorded_ids_match`] rejects it (#1905).
    if recorded_id.is_empty() || recorded_ids_match(recorded_id, live) {
        return RecordedTeardown::Remove {
            id: live.to_string(),
        };
    }
    RecordedTeardown::Hijacked {
        message: format!(
            "the runner recorded in .curie/runner.json is gone, and container '{container}' is now a different container ({live}); \
nothing was removed and the stale record has been cleared. \
To remove the container currently holding that name, run 'curie skill down --name {container}'"
        ),
    }
}

pub fn plan_skill_down(recorded: Option<&str>, requested: Option<&str>, exists: bool) -> DownPlan {
    match (recorded, requested) {
        (Some(recorded), Some(requested)) if requested != recorded => {
            let container = requested.to_string();
            if exists {
                DownPlan::Targeted { container }
            } else {
                DownPlan::TargetedAbsent { container }
            }
        }
        (Some(recorded), _) => DownPlan::Recorded {
            container: recorded.to_string(),
        },
        (None, requested) => {
            let container = requested.unwrap_or(docker::RUNNER_CONTAINER_LOCAL);
            if exists {
                DownPlan::Orphan {
                    container: container.to_string(),
                }
            } else {
                DownPlan::Nothing {
                    message: format!(
                        "no local runner recorded in .curie/runner.json and no container named '{container}' is running; \
run 'curie skill down' from the bundle directory, \
or name the container with 'curie skill down --name <container>'"
                    ),
                }
            }
        }
    }
}

/// Warn before removing a container by NAME that is not identifiably ours.
///
/// `skill down --name postgres` would otherwise destroy an unrelated container
/// with no signal at all. A warning, never a refusal: a runner left by a release
/// that predates the CLI label carries no label and must stay removable, which
/// is the whole point of #747. The label is read from the same probe that
/// established the container exists, so a Docker error has already aborted the
/// teardown before this is reached; here, absent means genuinely unlabeled.
pub(super) fn warn_if_not_cli_managed(
    container: &str,
    facts: Option<&docker::ContainerFacts>,
    ui: &crate::ui::Ui,
) {
    if docker::is_cli_managed(facts) {
        return;
    }
    ui.warn(&format!(
        "container '{container}' does not carry the {} label, so it may not be a Curie runner; removing it anyway",
        docker::CLI_MANAGED_LABEL
    ));
}

/// Said on every `--name` teardown that deliberately leaves the recorded runner
/// running, so the two arms cannot drift apart.
pub(super) const RECORDED_RUNNER_LEFT_ALONE: &str =
    "left the recorded runner in .curie/runner.json alone; run 'curie skill down' with no --name to stop it";

/// The note for a container that turned out to be gone before it was removed.
///
/// `state_cleared` is the recorded path, which ALSO clears
/// `.curie/runner.json`; that half of the sentence is the user's only signal
/// that it did, so it is stated here once rather than left to each caller to
/// remember (#747). Pure so the wording is testable.
pub(super) fn absent_container_note(container: &str, state_cleared: bool) -> String {
    format!(
        "container '{container}' was already gone{}",
        if state_cleared {
            "; cleared stale state"
        } else {
            ""
        }
    )
}

/// Remove a container, reporting success, and treat "already gone" as success
/// too (the same tolerance the recorded-runner path has always had).
pub(super) async fn remove_container_tolerating_absence(
    target: &str,
    display: &str,
    state_cleared: bool,
    ui: &crate::ui::Ui,
) -> Result<()> {
    match docker::remove_container(target).await {
        Ok(()) => ui.success(&format!("stopped and removed container '{display}'")),
        Err(err) if err.to_string().contains("No such container") => {
            ui.note(&absent_container_note(display, state_cleared));
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

pub async fn stop(name: Option<String>, dir: &Path) -> Result<()> {
    let ui = crate::ui::ui();
    let saved = state::load(dir)?;
    let recorded = saved.as_ref().map(|s| s.container_name.clone());
    // Ask Docker what actually holds the target name. Every path needs it:
    // `docker rm -f` exits 0 on a missing name, so only the probe can tell a real
    // removal from a no-op, and the recorded path compares the live container's
    // ID against the recorded one before removing anything (#747). Exactly one
    // branch below runs, so one probe of one name is enough, and taking the id
    // and the managed-by label from that same probe leaves no window for the two
    // to disagree.
    let target = name
        .as_deref()
        .or(recorded.as_deref())
        .unwrap_or(docker::RUNNER_CONTAINER_LOCAL);
    // Propagated, never swallowed: an unreachable daemon reported as "no such
    // container" would hand the user a remedy that cannot work and hide the real
    // fault.
    let live = docker::container_facts(target)
        .await
        .with_context(|| format!("checking whether container '{target}' exists"))?;

    let plan = plan_skill_down(recorded.as_deref(), name.as_deref(), live.is_some());
    // `Recorded` is returned only when a runner is recorded, so pairing the plan
    // with the record here is what makes the teardown total.
    if let (DownPlan::Recorded { .. }, Some(saved)) = (&plan, saved) {
        return stop_recorded(dir, ui, saved, live.as_ref()).await;
    }
    match plan {
        DownPlan::Targeted { container } => {
            // A different container than the one on record: remove exactly it and
            // leave the recorded runner (and its state, ollama, network) intact.
            warn_if_not_cli_managed(&container, live.as_ref(), ui);
            remove_container_tolerating_absence(&container, &container, false, ui).await?;
            ui.note(RECORDED_RUNNER_LEFT_ALONE);
            Ok(())
        }
        DownPlan::TargetedAbsent { container } => {
            ui.note(&format!(
                "no container named '{container}' is present; nothing was removed"
            ));
            ui.note(RECORDED_RUNNER_LEFT_ALONE);
            Ok(())
        }
        DownPlan::Orphan { container } => {
            // No state file to clear, so the container IS the identity (#747).
            warn_if_not_cli_managed(&container, live.as_ref(), ui);
            remove_container_tolerating_absence(&container, &container, false, ui).await?;
            ui.note("no .curie/runner.json was present, so nothing to clear");
            Ok(())
        }
        DownPlan::Nothing { message } => bail!(message),
        // Unreachable: `plan_skill_down` returns `Recorded` only when a runner
        // is recorded, and the `if let` above takes that pairing.
        DownPlan::Recorded { container } => {
            bail!("internal: a recorded teardown of '{container}' reached the unrecorded path")
        }
    }
}

/// Tear down the runner recorded in `.curie/runner.json`, plus the ollama
/// sidecar, network and state file it owns.
pub(super) async fn stop_recorded(
    dir: &Path,
    ui: &crate::ui::Ui,
    saved: RunnerState,
    live: Option<&docker::ContainerFacts>,
) -> Result<()> {
    match plan_recorded_teardown(
        &saved.container_id,
        &saved.container_name,
        live.map(|f| f.id.as_str()),
    ) {
        // By ID, not by name: the identity check above approved exactly this
        // container, and a name can change hands before the removal lands.
        RecordedTeardown::Remove { id } => {
            remove_container_tolerating_absence(&id, &saved.container_name, true, ui).await?
        }
        // Nothing holds the name, so there is no removal to claim; the stale
        // record still gets cleared below.
        RecordedTeardown::AlreadyGone => {
            ui.note(&absent_container_note(&saved.container_name, true))
        }
        // Another bundle's runner now holds this name. Removing it would destroy
        // a live container this bundle never booted.
        RecordedTeardown::Hijacked { message } => {
            ui.warn(&message);
            state::remove(dir)?;
            return Ok(());
        }
    }
    if let Some(ollama) = &saved.ollama_container {
        // A sidecar that will not die is a warning, not a failed teardown.
        if let Err(err) = remove_container_tolerating_absence(ollama, ollama, false, ui).await {
            ui.warn(&format!("could not remove container '{ollama}': {err}"));
        }
        // Keep the model-cache volume so the next `skill up` reuses the pulled
        // model instead of re-downloading it (mirrors compose `down` keeping
        // `ollama_data`). Removal is left to the user.
        let volume = docker::ollama_volume(ollama);
        ui.note(&format!(
            "kept model-cache volume '{volume}' for fast re-up; remove it with 'docker volume rm {volume}'"
        ));
    }
    // Connector containers, then the staged credential tree (ADR 0113,
    // block B1-8). Label-scoped, so a connector this bundle started is reaped
    // even if the record is incomplete; the network is left to the owned-network
    // branch below, which alone knows whether this boot created it.
    let connector_problems = docker::run_connector_teardown(&docker::connector_teardown_plan(
        &saved.session_id,
        None,
        Some(dir),
    ))
    .await;
    for problem in connector_problems {
        ui.warn(&problem);
    }
    if let Some(net) = &saved.network {
        match docker::remove_network(net).await {
            Ok(()) => ui.success(&format!("removed network '{net}'")),
            Err(err) if err.to_string().contains("No such network") => {
                ui.note(&format!("network '{net}' was already gone"));
            }
            Err(err) => ui.warn(&format!("could not remove network '{net}': {err}")),
        }
    }
    // Remove the snapshot this record owns (#1087), the same way the ollama
    // sidecar and the network above are released. Guarded and tolerant: a
    // snapshot that will not delete is a warning, not a failed teardown (#323 --
    // the agent consumer needs the teardown to succeed), and a recorded path
    // outside <bundle>/.curie/snapshots/ is refused rather than deleted.
    if let Some(snapshot_dir) = &saved.bundle_snapshot_dir {
        if let Err(err) = crate::bundle::remove_snapshot(Path::new(snapshot_dir), dir) {
            ui.warn(&format!(
                "could not remove bundle snapshot '{snapshot_dir}': {err}"
            ));
        }
    }
    state::remove(dir)?;
    Ok(())
}

/// The `curie skill status --json` payload: the runner base URL plus the
/// serialized session status. Generic over the status shape so it serves both
/// the frozen `SessionStatus` (contract test) and the runner's raw `/status`
/// body (the live call site), which are both left unconstrained by
/// `cli/schema/status.schema.json`. Pure so it stays contract-testable.
///
/// `bundle_digest` (#1087) is the sha256 of the snapshot this runner mounted.
/// The key is always emitted -- `null` when no runner is recorded or the record
/// predates #1087 -- so an agent consumer can read it unconditionally.
pub fn status_json<T: serde::Serialize>(
    url: &str,
    status: &T,
    bundle_digest: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({ "url": url, "session": status, "bundle_digest": bundle_digest })
}

/// The recorded bundle digest that honestly applies to the runner at `url`
/// (#1087) -- the one home of that rule, shared by `skill status` and
/// `skill eval`.
///
/// A digest is only knowable for the runner this checkout recorded:
/// `resolve_url` is explicit-wins, so an explicit `--url` at some other runner
/// would otherwise marry a foreign session to the local bundle's digest.
/// Matching the resolved url against the recorded `base_url` is the honest
/// test -- with no `--url` the resolved value IS the recorded one, so the digest
/// still reports. `None` when nothing is recorded here, when the record predates
/// #1087, or when it points elsewhere: a null digest, never an error.
pub(super) fn recorded_bundle_digest(
    saved: Option<&state::RunnerState>,
    url: &str,
) -> Option<String> {
    saved
        .filter(|s| s.base_url == url)
        .and_then(|s| s.bundle_digest.clone())
}

pub async fn status(url: Option<String>) -> Result<()> {
    let url = resolve_url(url)?;
    // The bundle the runner BEING SHOWN is executing (#1087); see
    // `recorded_bundle_digest` for why a foreign `--url` reports none. The load
    // is tolerant because an unreadable `.curie/runner.json` must not break an
    // explicit `--url`, a path that never read local state before #1087 (the
    // no-`--url` path still hard-errors on it, inside `resolve_url`).
    let saved = state::load(Path::new(".")).unwrap_or(None);
    let bundle_digest = recorded_bundle_digest(saved.as_ref(), &url);
    let client = RunnerClient::new(&url)?;
    let status = client.status().await?;
    crate::ui::ui().emit(&StatusOutput {
        url,
        status: serde_json::to_value(&status)?,
        bundle_digest,
    });
    Ok(())
}

/// Output of `skill status` (#474). `to_json` delegates to the schema-gated
/// `status_json` builder (byte-identical, so `cli/schema/status.schema.json` and
/// `json_contract.rs` stay green); `render` reproduces the note + pretty payload.
pub(super) struct StatusOutput {
    url: String,
    status: serde_json::Value,
    /// The digest recorded in `.curie/runner.json` (#1087), and only when that
    /// record is the runner at `url`; `None` otherwise, so the key never claims
    /// a digest for a runner it was not recorded against.
    bundle_digest: Option<String>,
}

impl crate::ui::CliOutput for StatusOutput {
    fn to_json(&self) -> serde_json::Value {
        status_json(&self.url, &self.status, self.bundle_digest.as_deref())
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.note(&format!("runner {}", self.url));
        // Diagnostics on stderr (#11): the digest is a note, so the machine
        // payload on stdout is unchanged for a human-path consumer.
        ui.note(&format!(
            "bundle {}",
            self.bundle_digest.as_deref().unwrap_or("<none recorded>")
        ));
        ui.payload_plain(
            &serde_json::to_string_pretty(&self.status).unwrap_or_else(|_| self.status.to_string()),
        );
    }
}

pub async fn send(
    text: &str,
    user: &str,
    event_type: EventType,
    url: Option<String>,
    r#continue: bool,
) -> Result<SkillMessageOutput> {
    let url = resolve_url(url)?;
    let saved = state::load(Path::new(".")).unwrap_or(None);
    let bundle_warning = editable_bundle_warning(saved.as_ref(), &url);
    let client = RunnerClient::new(&url)?;
    let ui = crate::ui::ui();
    if let Some(warning) = bundle_warning {
        ui.warn(&warning);
    }
    let mut printer = TurnPrinter::default();

    if !r#continue {
        client
            .reset()
            .await
            .context("resetting the runner conversation before message")?;
    }

    // Buffer the reply for the caller's typed result while the human path
    // streams live. Callers emit success only after their cleanup has finished.
    let json = ui.json();
    let mut reply = String::new();

    // A "thinking" spinner marks the wait for the first token; it is cleared the
    // instant streaming begins (committing no line) so the agent answer streams
    // clean. `streamed` tracks whether any answer token reached stdout;
    // `at_line_start` tracks whether stdout is at a fresh line (no un-terminated
    // streamed text) so a stderr diagnostic never glues onto a token line.
    let cl = ui.checklist();
    let mut step = Some(cl.step("thinking"));
    let mut streamed = false;
    let mut at_line_start = true;

    let events = client
        .send_event(event_type, text, user, |event| {
            let part = printer.part_for(event);
            // Clear the "thinking" spinner on the FIRST rendered event of any
            // kind (token, note, or failure). A Note/Fail is written to stderr
            // immediately, so if one arrives before the first token it would
            // garble the still-live spinner line unless we drop it first.
            if matches!(
                part,
                Some(TurnPart::Token(_) | TurnPart::Note(_) | TurnPart::Fail(_))
            ) {
                if let Some(step) = step.take() {
                    step.clear();
                }
            }
            match part {
                // Answer tokens are raw payload -> stdout, concatenated at network
                // pace with no per-delta newline. Track mid-line state so a later
                // note closes an un-terminated line first.
                Some(TurnPart::Token(token)) => {
                    reply.push_str(&token);
                    if !json {
                        ui.answer(&token);
                    }
                    streamed = true;
                    at_line_start = token.ends_with('\n');
                }
                // Tool notes and errors are diagnostics -> stderr. If stdout is
                // mid-line, close that streamed line with a single newline first
                // so the note does not glue onto the token text. Under `-q` the
                // note itself is a no-op; the lone separating newline lands in
                // the middle of the streamed answer, which is just whitespace and
                // harmless to `| jq` (a median newline in the payload is fine).
                Some(TurnPart::Note(msg)) => {
                    if !at_line_start {
                        ui.print_tokens("\n");
                        at_line_start = true;
                    }
                    ui.note(&msg);
                }
                Some(TurnPart::Fail(msg)) => {
                    if !at_line_start {
                        ui.print_tokens("\n");
                        at_line_start = true;
                    }
                    ui.failure(&msg);
                }
                // The status trailer is emitted once at the end from events.last().
                Some(TurnPart::Status(_)) | None => {}
            }
        })
        .await?;

    // Nothing ever streamed (e.g. an empty final): drop the spinner silently.
    if let Some(step) = step.take() {
        step.clear();
    }

    if let Some(final_event @ OutboundEvent::Final { status, text, .. }) = events.last() {
        // Close the streamed answer on stdout only if the last thing written was
        // un-terminated token text; if a note already added its own newline (or
        // the last token ended in one) skip it to avoid a blank line. The status
        // trailer is rendered by the caller after success.
        if streamed && !at_line_start {
            ui.print_tokens("\n");
        }
        if *status == SessionStatus::ClassifiedFailure {
            let detail = text.trim();
            let message = if detail.is_empty() {
                "runner turn ended with classified-failure".to_string()
            } else {
                format!("runner turn ended with classified-failure: {detail}")
            };
            return Err(crate::exit::CliError::failure(message).into());
        }
        return SkillMessageOutput::from_final(reply, final_event)
            .context("runner stream ended without a final frame");
    }
    bail!("runner stream ended without a final frame")
}

/// Successful streamed turn result with the full reply, final session status,
/// and approval metadata. Human answer tokens already streamed in send, so
/// rendering this result adds only the final status.
#[derive(Debug)]
pub struct SkillMessageOutput {
    pub reply: String,
    pub status: String,
    pub finalized: bool,
    pub approval_summary: Option<String>,
    pub approval_route: Option<String>,
    pub approval_gate_kind: Option<String>,
    pub approval_granted_tool: Option<String>,
}

impl SkillMessageOutput {
    /// Project a runner final frame into the agent-facing result without
    /// inventing terminal state. An approval park is resumable, so it is the
    /// only final-frame status for which `finalized` is false.
    pub fn from_final(reply: String, event: &OutboundEvent) -> Option<Self> {
        let OutboundEvent::Final {
            status,
            approval_summary,
            approval_route,
            approval_gate_kind,
            approval_granted_tool,
            ..
        } = event
        else {
            return None;
        };

        Some(Self {
            reply,
            status: status_str(status).to_string(),
            finalized: !matches!(status, SessionStatus::AwaitingApproval),
            approval_summary: approval_summary.clone(),
            approval_route: approval_route.clone(),
            approval_gate_kind: approval_gate_kind.clone(),
            approval_granted_tool: approval_granted_tool.clone(),
        })
    }
}

pub(super) fn editable_bundle_warning(
    saved: Option<&state::RunnerState>,
    url: &str,
) -> Option<String> {
    let saved = saved.filter(|saved| saved.base_url == url)?;
    let recorded_digest = saved.bundle_digest.as_deref()?;
    let editable_digest = match crate::bundle::digest_source(Path::new(&saved.plugin_dir)) {
        Ok(digest) => digest,
        Err(_) => {
            return Some(
                "could not compare the editable bundle with the running snapshot. Run `curie skill up` after fixing the bundle."
                    .to_string(),
            );
        }
    };
    if editable_digest == recorded_digest {
        return None;
    }
    Some(
        "editable bundle differs from the running snapshot. Run `curie skill up` to load the changes."
            .to_string(),
    )
}

impl crate::ui::CliOutput for SkillMessageOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "reply": self.reply,
            "status": self.status,
            "finalized": self.finalized,
            "approval_summary": self.approval_summary,
            "approval_route": self.approval_route,
            "approval_gate_kind": self.approval_gate_kind,
            "approval_granted_tool": self.approval_granted_tool,
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.note(&format!("-- final ({})", self.status));
    }
}
