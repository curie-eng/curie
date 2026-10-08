//! Resolve which cluster a `curie cluster` command targets before it runs.

use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ClusterTargetSources {
    pub(crate) namespace_supplied: bool,
    pub(crate) namespace_from_environment: bool,
    pub(crate) release_supplied: bool,
}

impl ClusterTargetSources {
    pub(crate) fn from_matches(matches: &clap::ArgMatches) -> Self {
        let Some(("cluster", cluster_matches)) = matches.subcommand() else {
            return Self::default();
        };
        let Some((action_name, action_matches)) = cluster_matches.subcommand() else {
            return Self::default();
        };
        let action_matches = if matches!(
            action_name,
            "hooks"
                | "console"
                | "actions"
                | "remediation-policy"
                | "remediation"
                | "remediation-qualification"
        ) {
            let Some((_, leaf_matches)) = action_matches.subcommand() else {
                return Self::default();
            };
            leaf_matches
        } else {
            // `cluster hook fire` carries namespace on the leaf, not on `hook`.
            match action_matches.subcommand() {
                Some(("fire", fire_matches))
                    if action_matches
                        .try_get_one::<String>("namespace")
                        .ok()
                        .flatten()
                        .is_none()
                        && fire_matches.try_get_one::<String>("namespace").is_ok() =>
                {
                    fire_matches
                }
                _ => action_matches,
            }
        };
        Self {
            namespace_supplied: matches!(
                action_matches.value_source("namespace"),
                Some(ValueSource::CommandLine | ValueSource::EnvVariable)
            ),
            namespace_from_environment: matches!(
                action_matches.value_source("namespace"),
                Some(ValueSource::EnvVariable)
            ),
            release_supplied: matches!(
                action_matches.value_source("release"),
                Some(ValueSource::CommandLine | ValueSource::EnvVariable)
            ),
        }
    }
}

/// The chart value `cluster up --e2e-connector-identity` sets (ADR 0176
/// decision 4, #3243). The chart template owns the grant itself.
pub(crate) const E2E_CONNECTOR_IDENTITY_SET: &str = "e2eConnectorIdentity.enabled=true";

pub(crate) fn cluster_action_target(action: &ClusterAction) -> (Option<&str>, Option<&str>) {
    match action {
        ClusterAction::Console {
            action:
                ClusterConsoleAction::Login {
                    namespace, release, ..
                },
        }
        | ClusterAction::LintValues {
            namespace, release, ..
        }
        | ClusterAction::Up {
            namespace, release, ..
        }
        | ClusterAction::Down {
            namespace, release, ..
        }
        | ClusterAction::Rollback {
            namespace, release, ..
        }
        | ClusterAction::Upgrade {
            namespace, release, ..
        }
        | ClusterAction::MigrateStore {
            namespace, release, ..
        }
        | ClusterAction::Status {
            namespace, release, ..
        }
        | ClusterAction::Observability {
            namespace, release, ..
        }
        | ClusterAction::Comms {
            namespace, release, ..
        }
        | ClusterAction::GithubApp {
            namespace, release, ..
        }
        | ClusterAction::Factory {
            namespace, release, ..
        }
        | ClusterAction::Eval {
            namespace, release, ..
        }
        | ClusterAction::Deploy {
            namespace, release, ..
        } => (Some(namespace.as_str()), Some(release.as_str())),
        ClusterAction::Message {
            namespace, release, ..
        } => (namespace.as_deref(), release.as_deref()),
        ClusterAction::Kill { conn, .. }
        | ClusterAction::Resume { conn, .. }
        | ClusterAction::Overrides { conn, .. }
        | ClusterAction::PublicationPolicy { conn, .. }
        | ClusterAction::Surfaces { conn, .. }
        | ClusterAction::Callers { conn, .. }
        | ClusterAction::ChannelToken { conn, .. }
        | ClusterAction::Budget { conn, .. }
        | ClusterAction::ResetThread { conn, .. }
        | ClusterAction::WorkItems { conn, .. }
        | ClusterAction::Schedules { conn, .. }
        | ClusterAction::Delete { conn, .. } => {
            (Some(conn.namespace.as_str()), Some(conn.release.as_str()))
        }
        ClusterAction::Hook { conn, .. } => {
            (Some(conn.namespace.as_str()), Some(conn.release.as_str()))
        }
        ClusterAction::Versions { target }
        | ClusterAction::Memory { target, .. }
        | ClusterAction::Approvals { target, .. } => (
            Some(target.conn.namespace.as_str()),
            Some(target.conn.release.as_str()),
        ),
        ClusterAction::Actions { verb } => {
            let conn = verb.conn();
            (Some(conn.namespace.as_str()), Some(conn.release.as_str()))
        }
        ClusterAction::RemediationPolicy { verb } => {
            let conn = verb.conn();
            (Some(conn.namespace.as_str()), Some(conn.release.as_str()))
        }
        ClusterAction::Remediation { verb } => {
            let conn = verb.conn();
            (Some(conn.namespace.as_str()), Some(conn.release.as_str()))
        }
        ClusterAction::RemediationQualification { verb } => {
            let conn = verb.conn();
            (Some(conn.namespace.as_str()), Some(conn.release.as_str()))
        }
        ClusterAction::Hooks { action } => {
            let target = match action {
                ClusterHooksAction::Show { target }
                | ClusterHooksAction::Configure { target, .. }
                | ClusterHooksAction::Secret { target } => target,
            };
            (
                Some(target.conn.namespace.as_str()),
                Some(target.conn.release.as_str()),
            )
        }
    }
}

pub(crate) fn retarget_cluster_action(
    action: &mut ClusterAction,
    namespace: Option<String>,
    release: Option<String>,
) {
    let replace = |current: &mut String, value: &Option<String>| {
        if let Some(value) = value {
            *current = value.clone();
        }
    };
    match action {
        ClusterAction::Console {
            action:
                ClusterConsoleAction::Login {
                    namespace: current_namespace,
                    release: current_release,
                    ..
                },
        }
        | ClusterAction::LintValues {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Up {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Down {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Rollback {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Upgrade {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::MigrateStore {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Status {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Observability {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Comms {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::GithubApp {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Factory {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Eval {
            namespace: current_namespace,
            release: current_release,
            ..
        }
        | ClusterAction::Deploy {
            namespace: current_namespace,
            release: current_release,
            ..
        } => {
            replace(current_namespace, &namespace);
            replace(current_release, &release);
        }
        ClusterAction::Message {
            namespace: current_namespace,
            release: current_release,
            ..
        } => {
            *current_namespace = namespace;
            *current_release = release;
        }
        ClusterAction::Kill { conn, .. }
        | ClusterAction::Resume { conn, .. }
        | ClusterAction::Overrides { conn, .. }
        | ClusterAction::PublicationPolicy { conn, .. }
        | ClusterAction::Surfaces { conn, .. }
        | ClusterAction::Callers { conn, .. }
        | ClusterAction::ChannelToken { conn, .. }
        | ClusterAction::Budget { conn, .. }
        | ClusterAction::ResetThread { conn, .. }
        | ClusterAction::WorkItems { conn, .. }
        | ClusterAction::Schedules { conn, .. }
        | ClusterAction::Delete { conn, .. } => {
            replace(&mut conn.namespace, &namespace);
            replace(&mut conn.release, &release);
        }
        ClusterAction::Hook { conn, .. } => {
            replace(&mut conn.namespace, &namespace);
            replace(&mut conn.release, &release);
        }
        ClusterAction::Versions { target }
        | ClusterAction::Memory { target, .. }
        | ClusterAction::Approvals { target, .. } => {
            replace(&mut target.conn.namespace, &namespace);
            replace(&mut target.conn.release, &release);
        }
        ClusterAction::Actions { verb } => {
            let conn = verb.conn_mut();
            replace(&mut conn.namespace, &namespace);
            replace(&mut conn.release, &release);
        }
        ClusterAction::RemediationPolicy { verb } => {
            let conn = verb.conn_mut();
            replace(&mut conn.namespace, &namespace);
            replace(&mut conn.release, &release);
        }
        ClusterAction::Remediation { verb } => {
            let conn = verb.conn_mut();
            replace(&mut conn.namespace, &namespace);
            replace(&mut conn.release, &release);
        }
        ClusterAction::RemediationQualification { verb } => {
            let conn = verb.conn_mut();
            replace(&mut conn.namespace, &namespace);
            replace(&mut conn.release, &release);
        }
        ClusterAction::Hooks { action } => {
            let target = match action {
                ClusterHooksAction::Show { target }
                | ClusterHooksAction::Configure { target, .. }
                | ClusterHooksAction::Secret { target } => target,
            };
            replace(&mut target.conn.namespace, &namespace);
            replace(&mut target.conn.release, &release);
        }
    }
}

pub(crate) fn load_declared_cluster_target() -> Result<Option<curie::installation::Installation>> {
    let path = std::path::Path::new("curie.yaml");
    match std::fs::symlink_metadata(path) {
        Ok(_) => curie::installation::Installation::load(path)
            .map(Some)
            .map_err(|source| {
                anyhow::Error::from(
                    curie::exit::CliError::usage(format!(
                        "curie.yaml could not supply the cluster namespace and release: {source:#}"
                    ))
                    .with_fix(
                        "Fix curie.yaml, or pass both --namespace and --release to select the target explicitly.",
                    ),
                )
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(anyhow::Error::from(
            curie::exit::CliError::usage(format!(
                "curie.yaml could not be inspected for the cluster namespace and release: {source}"
            ))
            .with_fix(
                "Make curie.yaml readable, or pass both --namespace and --release to select the target explicitly.",
            ),
        )),
    }
}

pub(crate) fn resolve_cluster_command_target(
    command: Option<&mut Command>,
    sources: ClusterTargetSources,
) -> Result<()> {
    let Some(Command::Cluster { action, .. }) = command else {
        return Ok(());
    };

    let (current_namespace, current_release) = cluster_action_target(action);
    let supplied_namespace = sources
        .namespace_supplied
        .then(|| current_namespace.map(str::to_owned))
        .flatten();
    let supplied_release = sources
        .release_supplied
        .then(|| current_release.map(str::to_owned))
        .flatten();

    if matches!(
        action,
        ClusterAction::Message {
            r#continue: true,
            ..
        }
    ) {
        retarget_cluster_action(action, supplied_namespace, supplied_release);
        return Ok(());
    }

    let declared = if supplied_namespace.is_none() || supplied_release.is_none() {
        load_declared_cluster_target()?
    } else {
        None
    };
    let target = curie::doctor::resolve_target(
        supplied_namespace.as_deref(),
        supplied_release.as_deref(),
        declared.as_ref().map(|config| {
            (
                config.install.namespace.as_str(),
                config.install.release.as_str(),
            )
        }),
    );
    let curie::doctor::Target {
        namespace,
        release,
        announcement,
    } = target;
    let announcement = announcement.map(|_| {
        let namespace_source = if sources.namespace_supplied {
            if sources.namespace_from_environment {
                "CURIE_NAMESPACE"
            } else {
                "flag"
            }
        } else if declared.is_some() {
            "curie.yaml"
        } else {
            "default"
        };
        let release_source = if sources.release_supplied {
            "flag"
        } else if declared.is_some() {
            "curie.yaml"
        } else {
            "default"
        };
        format!(
            "target: namespace {namespace} ({namespace_source}), release {release} ({release_source})"
        )
    });
    retarget_cluster_action(action, Some(namespace), Some(release));
    if let Some(announcement) = announcement {
        ui::ui().note(&announcement);
    }
    Ok(())
}
