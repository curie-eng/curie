//! The install half shared by the quickstarts that register no GitHub App:
//! GitLab as code host and tracker, and a Bitbucket code host with the Jira
//! Cloud tracker. Each reads its forge first, then plans the install here,
//! runs it here, and writes its own forge entries and Secrets.

use std::path::Path;

use anyhow::Result;

use super::{
    credential_decision, credit_check_key, ensure_credential, install_steps, kind_clusters,
    quickstart_model, read_release, release_model_recorded, run_install_action, Action,
    ClusterUpStep, CredentialDecision, InstallSteps, InstallTarget, QuickstartOpts,
};
use crate::exit::CliError;
use crate::ops::CommonOpts;

/// The planned install and the model credential decision for it.
pub(super) struct NativeInstall {
    pub install: InstallSteps,
    pub credential: CredentialDecision,
    pub release_has_real_model: bool,
}

/// Plans the install: the kind cluster or targeted context, and whether the
/// release already runs the chart and model the quickstart would install.
/// `current` is the ambient kube context when `--context` is absent. Under
/// `--dry-run` nothing is read from the cluster.
pub(super) async fn plan_install(
    opts: &QuickstartOpts,
    current: Option<&str>,
    real_interactive: bool,
) -> Result<NativeInstall> {
    let existing = if opts.context.is_none() && current.is_none() {
        match kind_clusters().await {
            Ok(clusters) => clusters,
            Err(err) if opts.dry_run => {
                crate::ui::ui().note(&format!("kind was not queried: {err:#}"));
                Vec::new()
            }
            Err(err) => return Err(err),
        }
    } else {
        Vec::new()
    };
    let credential_in_env = crate::ops::model_credential_env()?.is_some();
    let targeted = opts.context.as_deref().or(current);
    if let (Some(context), false) = (targeted, opts.dry_run) {
        // Helm value reads have no context flag. Pin first so a rerun against
        // --context sees that release, not the ambient one.
        crate::kube_context::pin_for_cluster_command(Some(context))?;
    }
    let release_has_real_model = if opts.dry_run || targeted.is_none() {
        false
    } else {
        release_model_recorded(opts).await?
    };
    // Read once: a second read can print the deprecation warning twice.
    let explicit_credential = crate::ops::explicit_model_credential_env();
    let explicit_credential_absent = explicit_credential.is_none();
    let effective_credential = credit_check_key(
        explicit_credential,
        crate::ops::saved_model_credential(),
        release_has_real_model,
    );
    let model = quickstart_model(opts.model.as_deref(), effective_credential.as_deref());
    let (release_read, model) = read_release(
        opts,
        targeted,
        false,
        model,
        opts.model.is_none() && effective_credential.is_none() && release_has_real_model,
        explicit_credential_absent,
    )
    .await?;
    let install = install_steps(&InstallTarget {
        explicit_context: opts.context.as_deref(),
        current_context: current,
        kind_name: &opts.kind_name,
        existing_kind_clusters: &existing,
        namespace: &opts.namespace,
        release: &opts.release,
        model: &model,
        chart: &opts.chart,
        release_read,
        add_agent: false,
    });
    Ok(NativeInstall {
        install,
        credential: credential_decision(
            credential_in_env,
            release_has_real_model,
            real_interactive,
        ),
        release_has_real_model,
    })
}

/// Refuses a run that needs a model credential it cannot ask for.
pub(super) fn refuse_without_credential(credential: &CredentialDecision) -> Result<()> {
    if *credential == CredentialDecision::RefuseNonInteractive {
        return Err(
            CliError::usage("model credential needed and stdin is not a terminal")
                .with_fix(
                    "set CURIE_CREDENTIALS to an Anthropic API key starting with sk-ant- or an OpenRouter key starting with sk-or- and rerun",
                )
                .into(),
        );
    }
    Ok(())
}

/// Runs the planned install actions, asking for the model credential once
/// before `cluster up` when the plan says so. Returns whether the release
/// runs a real model afterwards.
pub(super) async fn run_install(
    install: &InstallSteps,
    credential: &CredentialDecision,
    mut release_has_real_model: bool,
    opts: &QuickstartOpts,
) -> Result<bool> {
    let ui = crate::ui::ui();
    ui.note(&format!("Kubernetes context: {}", install.context));
    if install.cluster_up == ClusterUpStep::Skipped {
        ui.note("release already at the target chart and values; skipping cluster up");
    }
    let mut prompted_model = None;
    for action in &install.actions {
        if matches!(action, Action::Cluster { .. }) {
            if install.kind_cluster.is_some() {
                // The plan could not read the release: the context did not
                // exist yet. Read it now, before `cluster up` records a key.
                release_has_real_model =
                    crate::kube_context::pin_for_cluster_command(Some(&install.context)).is_err()
                        || release_model_recorded(opts).await.unwrap_or(true);
            }
            ensure_credential(credential)?;
            if *credential == CredentialDecision::PromptOnce {
                let credential = crate::ops::model_credential_env()?;
                prompted_model = Some(quickstart_model(
                    opts.model.as_deref(),
                    credential.as_deref(),
                ));
            }
        }
        run_install_action(action, prompted_model.as_ref()).await?;
    }
    Ok(release_has_real_model)
}

/// Checks the bundle's runner against the installation before the first
/// forge write, so a mismatch leaves the release as it was. Returns the
/// runner image the agent will run.
pub(super) async fn check_bundle(
    common: &CommonOpts,
    agent: &str,
    bundle: Option<&Path>,
    runner_image: Option<&str>,
) -> Result<Option<String>> {
    Ok(match bundle {
        Some(dir) => {
            crate::cluster_secrets::check_layered_runner_base(common, dir).await?;
            runner_image.map(str::to_string)
        }
        None => Some(super::github::render_and_check(common, agent, None).await?),
    })
}
