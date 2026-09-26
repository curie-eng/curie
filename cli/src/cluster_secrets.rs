//! Cluster-tier per-agent connector secret binding (#1488, ADR-0009).
//!
//! Values go to the per-agent Helm Secret through a private values file (never
//! argv). The agent record stores a names-only placeholder so the worker can
//! route claims to the per-agent pool without keeping the secret material in
//! Postgres. Rotation deletes SandboxClaims labeled for that agent; sandbox
//! pods are not Deployments, so there is no `rollout restart` of claimed
//! sandboxes.
//!
//! The same bind carries the agent's layered runner image (#3260): the digest
//! `connectors.lock.yaml` records lands in `agentSandbox.runnerImages.<agent>`,
//! and a bundle with no runner entry clears an earlier value.

use std::collections::BTreeMap;

use anyhow::{bail, Result};

use crate::docker::CONNECTOR_AGENT_LABEL_KEY;
use crate::ops::{plain, require_on_path, run_step, CmdArg, CommonOpts, OpsCommand};

/// Placeholder stored on the agent record at the cluster tier. Non-empty so the
/// API validator accepts it; not the secret material. The k8s substrate strips
/// these keys off the claim; the template secretKeyRef delivers the real value.
pub const CLUSTER_SECRET_PLACEHOLDER: &str = "secretKeyRef";

/// Chart agent names match templates/agent-sandbox.yaml.
fn validate_agent_resource_name(agent: &str) -> Result<()> {
    // `self` is reserved for `admits` (ADR-0168 decision 7), where it means
    // the agent this bundle is deployed as. A real agent named `self` would
    // be indistinguishable from that sentinel wherever `admits` is resolved.
    if agent == "self" {
        bail!(
            "agent name \"self\" is not valid -- it is reserved for `admits`, where it means \
             the agent this bundle is deployed as"
        );
    }
    let valid = agent.len() <= 40
        && agent
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && agent.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && agent.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit());
    if !valid {
        bail!(
            "agent name {agent:?} is not a valid per-agent Secret key (lowercase DNS label, max 40 characters)"
        );
    }
    Ok(())
}

/// Resolve `--secret` NAMES the same way `deploy` does: env, then the host vault.
pub fn resolve_named_secrets(names: &[String]) -> Result<BTreeMap<String, String>> {
    let mut secrets = BTreeMap::new();
    for name in names {
        let value = std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .or(crate::secrets::get_value(name)?);
        match value {
            Some(v) => {
                secrets.insert(name.clone(), v);
            }
            None => {
                return Err(crate::exit::usage(format!(
                    "--secret {name}: not set in the environment and not saved in Curie \
                     storage; export it or run `curie secrets set {name}` first"
                )));
            }
        }
    }
    Ok(secrets)
}

/// Names-only placeholder map written to the agent record at the cluster tier,
/// built from NAMES that have no resolved value on this box.
///
/// The cluster tier's agent record is placeholders either way -- the value
/// lives in the per-agent Helm Secret, never in the record -- so a connector
/// secret whose value is resolved later, cluster-scoped (#1913), still belongs
/// in it. It has to: the worker keys `inject_connector_secrets` off this map,
/// and `sandbox.types` routes the claim to the per-agent pool only when the
/// marker is present. A record built from `--secret` alone routes the pod to
/// the generic pool with no connector secret env at all (#2503).
pub fn agent_record_secret_names<'a, I>(names: I) -> BTreeMap<String, String>
where
    I: IntoIterator<Item = &'a String>,
{
    names
        .into_iter()
        .map(|name| (name.clone(), CLUSTER_SECRET_PLACEHOLDER.to_string()))
        .collect()
}

/// Dotted helm keys for `agentSandbox.connectorSecrets.<agent>.<NAME>`.
pub fn helm_secret_pairs(
    agent: &str,
    secrets: &BTreeMap<String, String>,
) -> Result<Vec<(String, String)>> {
    validate_agent_resource_name(agent)?;
    Ok(secrets
        .iter()
        .map(|(name, value)| {
            (
                format!("agentSandbox.connectorSecrets.{agent}.{name}"),
                value.clone(),
            )
        })
        .collect())
}

pub struct BindOpts {
    pub common: CommonOpts,
    pub chart: String,
    pub agent: String,
    pub secrets: BTreeMap<String, String>,
    pub runner_image: RunnerImageUpdate,
}

/// What a bind does to `agentSandbox.runnerImages.<agent>` (#3260).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerImageUpdate {
    /// Leave the release's value as it is.
    Keep,
    /// Bind this locked digest.
    Set(String),
    /// Clear an earlier value: the bundle no longer declares a runner.
    Clear,
}

/// helm upgrade with a private values file for the secrets and a `--set` for
/// the runner image, then replace the agent's claimed sandboxes so
/// secretKeyRef env and the runner image are re-resolved at pod start.
pub fn bind_commands(opts: &BindOpts) -> Result<Vec<OpsCommand>> {
    let pairs = helm_secret_pairs(&opts.agent, &opts.secrets)?;
    let runner_set = match &opts.runner_image {
        RunnerImageUpdate::Keep => None,
        RunnerImageUpdate::Set(digest) => Some(digest.as_str()),
        RunnerImageUpdate::Clear => Some("null"),
    };
    if pairs.is_empty() && runner_set.is_none() {
        return Ok(Vec::new());
    }
    // --reset-then-reuse-values, not --reuse-values: on helm v3.20 a `null`
    // override under --reuse-values prunes the stored value but the manifest
    // keeps rendering the old key, so a cleared runner image never leaves.
    let mut args = vec![
        plain("upgrade"),
        plain(&opts.common.release),
        plain(&opts.chart),
        plain("-n"),
        plain(&opts.common.namespace),
        plain("--reset-then-reuse-values"),
    ];
    if !pairs.is_empty() {
        args.push(CmdArg::SecretValuesFile(pairs));
    }
    if let Some(value) = runner_set {
        args.push(plain("--set"));
        args.push(plain(format!(
            "agentSandbox.runnerImages.{}={value}",
            opts.agent
        )));
    }
    Ok(vec![
        OpsCommand::new("helm", args),
        retire_claims_command(&opts.common.namespace, &opts.agent),
    ])
}

/// Replace the agent's claimed sandboxes so the next turn starts a fresh pod
/// with the newly deployed bundle and re-resolved secretKeyRef env.
fn retire_claims_command(namespace: &str, agent: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("delete"),
            plain("sandboxclaim"),
            plain("-l"),
            plain(format!("{CONNECTOR_AGENT_LABEL_KEY}={agent}")),
            plain("--wait=true"),
            plain("--ignore-not-found=true"),
        ],
    )
}

/// Whether a bind would change the release at all (#3082).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindNeed {
    /// Every requested name already holds the requested value in the
    /// release's supplied values, so a `helm upgrade` would only re-render the
    /// platform and restart its pods for nothing.
    Current,
    /// Something differs. `secrets` names the connector secrets that are
    /// missing or hold a different value (names only, never values);
    /// `runner_image` is true when the agent's runner image must be set to a
    /// new digest or cleared.
    Changed {
        secrets: Vec<String>,
        runner_image: bool,
    },
}

/// Pure over the JSON `helm get values -o json` returns for the release.
///
/// `--reuse-values` merges onto exactly these supplied values, so a name whose
/// value already matches here is a no-op for the bind. The runner image
/// changes when a locked digest differs from the release's value, or when no
/// digest is locked and the release still holds one for this agent.
pub fn bind_need(
    release_values: &serde_json::Value,
    agent: &str,
    secrets: &BTreeMap<String, String>,
    runner_image: Option<&str>,
) -> BindNeed {
    let bound = release_values
        .pointer("/agentSandbox/connectorSecrets")
        .and_then(|all| all.get(agent));
    let changed: Vec<String> = secrets
        .iter()
        .filter(|(name, value)| {
            bound
                .and_then(|b| b.get(name.as_str()))
                .and_then(|v| v.as_str())
                != Some(value.as_str())
        })
        .map(|(name, _)| name.clone())
        .collect();
    let bound_runner = release_values
        .pointer("/agentSandbox/runnerImages")
        .and_then(|all| all.get(agent))
        .filter(|v| !v.is_null());
    let runner_changed = match runner_image {
        Some(digest) => bound_runner.and_then(|v| v.as_str()) != Some(digest),
        None => bound_runner.is_some(),
    };
    if changed.is_empty() && !runner_changed {
        BindNeed::Current
    } else {
        BindNeed::Changed {
            secrets: changed,
            runner_image: runner_changed,
        }
    }
}

fn helm_values_command(common: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("values"),
            plain(&common.release),
            plain("-n"),
            plain(&common.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// Read the release's supplied values and judge whether binding `secrets`
/// for `agent` changes anything. A values read that fails or does not parse
/// cannot prove the bind is a no-op, so every name counts as changed and the
/// caller upgrades exactly as it did before this check existed. The runner
/// image counts as changed too: with none locked, an unread release could
/// still hold an earlier image, and a `null` override of an absent key is
/// harmless, so the clear is sent rather than silently skipped.
pub async fn read_bind_need(
    common: &CommonOpts,
    agent: &str,
    secrets: &BTreeMap<String, String>,
    runner_image: Option<&str>,
) -> Result<BindNeed> {
    validate_agent_resource_name(agent)?;
    // Even a deploy that wants nothing reads the release: an earlier runner
    // image may still need clearing, and without helm that cannot be known.
    require_on_path("helm")?;
    let (ok, stdout, _stderr) = crate::ops::run_capture(&helm_values_command(common)).await?;
    let parsed = if ok {
        serde_json::from_str::<serde_json::Value>(&stdout).ok()
    } else {
        None
    };
    Ok(match parsed {
        Some(values) => bind_need(&values, agent, secrets, runner_image),
        None => BindNeed::Changed {
            secrets: secrets.keys().cloned().collect(),
            runner_image: true,
        },
    })
}

/// Bind only when the release does not already hold these values (#3082).
///
/// A bundle deploy re-binds its connector secrets every time, and each bind
/// used to be a full `helm upgrade` of the platform release: it re-rendered
/// every template, restarted platform pods, and reverted any out-of-band
/// change. When nothing differs the release is left alone. When something
/// does, the operator is told the platform release is being upgraded and why.
/// `chart` is only awaited on that path, so an unchanged bind never resolves
/// or downloads a chart.
pub async fn bind_if_changed<F>(
    common: CommonOpts,
    agent: String,
    secrets: BTreeMap<String, String>,
    runner_image: Option<String>,
    chart: F,
) -> Result<BindNeed>
where
    F: std::future::Future<Output = Result<String>>,
{
    let need = read_bind_need(&common, &agent, &secrets, runner_image.as_deref()).await?;
    let ui = crate::ui::ui();
    match &need {
        BindNeed::Current => {
            if !secrets.is_empty() || runner_image.is_some() {
                ui.note(&format!(
                    "connector secrets and runner image for agent {agent} are already current \
                     on release {}; the platform release was not upgraded",
                    common.release
                ));
                // Sandboxes are not platform pods: the agent's claims are
                // still retired so a running thread picks up the new bundle.
                require_on_path("kubectl")?;
                run_step(
                    &ui.checklist(),
                    &format!("replacing sandboxes for agent {agent}"),
                    "replaced",
                    &retire_claims_command(&common.namespace, &agent),
                )
                .await?;
            }
        }
        BindNeed::Changed {
            secrets: names,
            runner_image: runner_changed,
        } => {
            let mut what = Vec::new();
            if !names.is_empty() {
                what.push(format!("connector secret(s) {}", names.join(", ")));
            }
            let update = match (&runner_image, runner_changed) {
                (Some(digest), true) => {
                    what.push(format!("runner image digest {digest}"));
                    RunnerImageUpdate::Set(digest.clone())
                }
                (None, true) => {
                    what.push("removal of the runner image".to_string());
                    RunnerImageUpdate::Clear
                }
                (_, false) => RunnerImageUpdate::Keep,
            };
            ui.note(&format!(
                "platform change required: {} for agent {agent} changed, so release {} \
                 is being helm-upgraded to bind it",
                what.join(" and "),
                common.release
            ));
            let chart = chart.await?;
            bind(BindOpts {
                common,
                chart,
                agent,
                secrets,
                runner_image: update,
            })
            .await?;
        }
    }
    Ok(need)
}

pub async fn bind(opts: BindOpts) -> Result<()> {
    let cmds = bind_commands(&opts)?;
    if cmds.is_empty() {
        return Ok(());
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    let label = format!(
        "binding sandbox values for agent {} on release {}",
        opts.agent, opts.common.release
    );
    for cmd in &cmds {
        run_step(&cl, &label, "bound", cmd).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secrets() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("GITHUB_PERSONAL_ACCESS_TOKEN".into(), "ghp_agent_a".into()),
            ("JIRA_TOKEN".into(), "jira-a".into()),
        ])
    }

    #[test]
    fn agent_record_keeps_names_and_drops_values() {
        let stored = agent_record_secret_names(secrets().keys());
        assert_eq!(
            stored["GITHUB_PERSONAL_ACCESS_TOKEN"],
            CLUSTER_SECRET_PLACEHOLDER
        );
        assert_eq!(stored["JIRA_TOKEN"], CLUSTER_SECRET_PLACEHOLDER);
        assert!(!stored
            .values()
            .any(|v| v.contains("ghp_") || v.contains("jira-a")));
    }

    #[test]
    fn record_from_names_alone_is_the_same_placeholder_map() {
        // #2503: a connector secret whose value is resolved cluster-scoped
        // later has no value on this box, but the record still has to carry
        // its NAME -- the worker keys `inject_connector_secrets` (and the
        // per-agent sandbox pool routing) off this map. Names-only and
        // value-bearing inputs must produce byte-identical records for the
        // same key set, so the two deploy paths cannot diverge.
        let names: Vec<String> = secrets().keys().cloned().collect();
        let from_names = agent_record_secret_names(names.iter());
        assert_eq!(from_names, agent_record_secret_names(secrets().keys()));
        assert_eq!(
            from_names["GITHUB_PERSONAL_ACCESS_TOKEN"],
            CLUSTER_SECRET_PLACEHOLDER
        );
        assert_eq!(from_names["JIRA_TOKEN"], CLUSTER_SECRET_PLACEHOLDER);
    }

    #[test]
    fn record_from_names_carries_a_name_that_has_no_local_value() {
        // The #2503 case proper: the name reaches the record even though
        // nothing on this box ever resolved a value for it, and no value-like
        // material is invented for it.
        let stored = agent_record_secret_names(["CONNECTOR_ONLY".to_string()].iter());
        assert_eq!(stored.len(), 1);
        assert_eq!(stored["CONNECTOR_ONLY"], CLUSTER_SECRET_PLACEHOLDER);
    }

    #[test]
    fn helm_pairs_are_per_agent_and_keep_values_off_the_other_agent() {
        let a = helm_secret_pairs("acme-a", &secrets()).unwrap();
        let b = helm_secret_pairs(
            "acme-b",
            &BTreeMap::from([("GITHUB_PERSONAL_ACCESS_TOKEN".into(), "ghp_agent_b".into())]),
        )
        .unwrap();
        assert!(a.iter().any(|(k, v)| {
            k == "agentSandbox.connectorSecrets.acme-a.GITHUB_PERSONAL_ACCESS_TOKEN"
                && v == "ghp_agent_a"
        }));
        assert!(b.iter().any(|(k, v)| {
            k == "agentSandbox.connectorSecrets.acme-b.GITHUB_PERSONAL_ACCESS_TOKEN"
                && v == "ghp_agent_b"
        }));
        assert!(!a.iter().any(|(k, _)| k.contains("acme-b")));
        assert!(!b.iter().any(|(_, v)| v == "ghp_agent_a"));
    }

    #[test]
    fn bind_commands_use_a_values_file_and_never_argv_set() {
        let cmds = bind_commands(&BindOpts {
            common: CommonOpts {
                namespace: "curie".into(),
                release: "curie".into(),
                dry_run: false,
            },
            chart: "charts/curie".into(),
            agent: "acme-a".into(),
            secrets: secrets(),
            runner_image: RunnerImageUpdate::Keep,
        })
        .unwrap();
        let helm = cmds[0].display();
        assert!(helm.contains("helm upgrade"), "{helm}");
        // #3260: a cleared runner image under plain --reuse-values keeps
        // rendering the old key, so every bind resets then reuses.
        assert!(helm.contains("--reset-then-reuse-values"), "{helm}");
        assert!(helm.contains("-f"), "{helm}");
        assert!(
            !helm.contains("ghp_agent_a"),
            "secret leaked into argv: {helm}"
        );
        assert!(!helm.contains("--set"), "{helm}");
        let delete = cmds[1].display();
        assert!(delete.contains("delete sandboxclaim"), "{delete}");
        assert!(
            delete.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}=acme-a")),
            "{delete}"
        );
        assert!(delete.contains("--ignore-not-found=true"), "{delete}");
    }

    #[test]
    fn invalid_agent_name_is_rejected() {
        let err = helm_secret_pairs("Not_A_DNS", &secrets())
            .unwrap_err()
            .to_string();
        assert!(err.contains("Not_A_DNS"), "{err}");
    }

    #[test]
    fn the_agent_name_self_is_rejected_as_reserved() {
        // `self` is well-formed RFC 1123, so only a dedicated check catches
        // it. `admits:` reads `self` as the sentinel for "the deploying
        // agent"; a real agent named `self` would be indistinguishable from
        // it wherever `admits` is resolved.
        let err = helm_secret_pairs("self", &secrets())
            .unwrap_err()
            .to_string();
        assert!(err.contains("self"), "{err}");
        assert!(err.contains("admits"), "{err}");
    }

    #[test]
    fn bind_need_is_current_when_every_value_already_matches() {
        let values = serde_json::json!({"agentSandbox": {"connectorSecrets": {"acme-a": {
            "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_agent_a",
            "JIRA_TOKEN": "jira-a",
            "OTHER": "kept"
        }}}});
        assert_eq!(
            bind_need(&values, "acme-a", &secrets(), None),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_need_names_only_the_changed_or_missing_secrets() {
        let values = serde_json::json!({"agentSandbox": {"connectorSecrets": {
            "acme-a": {"GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_rotated"},
            "acme-b": {"JIRA_TOKEN": "jira-a"}
        }}});
        assert_eq!(
            bind_need(&values, "acme-a", &secrets(), None),
            BindNeed::Changed {
                secrets: vec!["GITHUB_PERSONAL_ACCESS_TOKEN".into(), "JIRA_TOKEN".into()],
                runner_image: false,
            }
        );
        assert_eq!(
            bind_need(&serde_json::json!({}), "acme-a", &secrets(), None),
            BindNeed::Changed {
                secrets: secrets().keys().cloned().collect(),
                runner_image: false,
            }
        );
    }

    /// Fake `helm`: `get values` answers from a file, `upgrade` logs its argv
    /// and bumps a revision counter the way a real upgrade bumps the release
    /// revision.
    const HELM_STUB: &str = r#"#!/bin/sh
case "$1 $2" in
  "get values") cat "$CURIE_TEST_BIND_DIR/values.json" ;;
  upgrade*) echo "$*" >> "$CURIE_TEST_BIND_DIR/helm.log"; r=$(cat "$CURIE_TEST_BIND_DIR/revision"); echo $((r + 1)) > "$CURIE_TEST_BIND_DIR/revision" ;;
  *) echo "unexpected helm invocation: $*" >&2; exit 64 ;;
esac
"#;

    struct StubbedHelm {
        restore: Vec<(&'static str, Option<std::ffi::OsString>)>,
        dir: tempfile::TempDir,
    }

    impl StubbedHelm {
        fn install(values: &serde_json::Value) -> Self {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            for (name, body) in [
                ("helm", HELM_STUB),
                (
                    "kubectl",
                    "#!/bin/sh\necho \"$*\" >> \"$CURIE_TEST_BIND_DIR/kubectl.log\"\n",
                ),
            ] {
                let path = dir.path().join(name);
                std::fs::write(&path, body).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            std::fs::write(dir.path().join("values.json"), values.to_string()).unwrap();
            std::fs::write(dir.path().join("revision"), "7\n").unwrap();
            let mut entries = vec![dir.path().to_path_buf()];
            entries.extend(std::env::split_paths(
                &std::env::var_os("PATH").unwrap_or_default(),
            ));
            let path = std::env::join_paths(entries).unwrap();
            let restore = vec![
                ("PATH", std::env::var_os("PATH")),
                (
                    "CURIE_TEST_BIND_DIR",
                    std::env::var_os("CURIE_TEST_BIND_DIR"),
                ),
            ];
            std::env::set_var("PATH", path);
            std::env::set_var("CURIE_TEST_BIND_DIR", dir.path());
            Self { restore, dir }
        }

        fn revision(&self) -> u32 {
            std::fs::read_to_string(self.dir.path().join("revision"))
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        }
    }

    impl StubbedHelm {
        /// Every `helm upgrade` argv, one per line; empty when none ran.
        fn helm_log(&self) -> String {
            std::fs::read_to_string(self.dir.path().join("helm.log")).unwrap_or_default()
        }

        fn kubectl_log(&self) -> Option<String> {
            std::fs::read_to_string(self.dir.path().join("kubectl.log")).ok()
        }
    }

    impl Drop for StubbedHelm {
        fn drop(&mut self) {
            for (name, value) in &self.restore {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    fn common() -> CommonOpts {
        CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        }
    }

    #[tokio::test]
    async fn bundle_only_deploy_leaves_the_release_revision_unchanged() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "connectorSecrets": {"acme-a": {
                "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_agent_a",
                "JIRA_TOKEN": "jira-a"
            }}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), secrets(), None, async {
            panic!("an unchanged bind must not resolve a chart")
        })
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7, "release was upgraded for a no-op bind");
        let kubectl = std::fs::read_to_string(helm.dir.path().join("kubectl.log")).unwrap();
        assert!(
            kubectl.contains("delete sandboxclaim"),
            "claims must still be retired so the new bundle loads: {kubectl}"
        );
    }

    #[tokio::test]
    async fn changed_secret_upgrades_the_release() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "connectorSecrets": {"acme-a": {
                "GITHUB_PERSONAL_ACCESS_TOKEN": "ghp_old",
                "JIRA_TOKEN": "jira-a"
            }}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), secrets(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: vec!["GITHUB_PERSONAL_ACCESS_TOKEN".into()],
                runner_image: false,
            }
        );
        assert_eq!(helm.revision(), 8);
    }

    const DIGEST: &str = "ghcr.io/acme-corp/acme-bot-runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const EARLIER: &str = "ghcr.io/acme-corp/acme-bot-runner@sha256:9999999999999999999999999999999999999999999999999999999999999999";

    fn opts(secrets: BTreeMap<String, String>, runner_image: RunnerImageUpdate) -> BindOpts {
        BindOpts {
            common: common(),
            chart: "charts/curie".into(),
            agent: "acme-a".into(),
            secrets,
            runner_image,
        }
    }

    #[test]
    fn bind_need_sees_a_new_or_differing_runner_image() {
        let none = serde_json::json!({});
        assert_eq!(
            bind_need(&none, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        let earlier = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": EARLIER}}});
        assert_eq!(
            bind_need(&earlier, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        let same = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": DIGEST}}});
        assert_eq!(
            bind_need(&same, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_need_clears_an_earlier_runner_image_when_none_is_locked() {
        // #3260 AC2: a bundle with no runner entry clears the agent's value.
        let earlier = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": EARLIER}}});
        assert_eq!(
            bind_need(&earlier, "acme-a", &BTreeMap::new(), None),
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        let nulled = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-a": null}}});
        assert_eq!(
            bind_need(&nulled, "acme-a", &BTreeMap::new(), None),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_need_ignores_another_agents_runner_image() {
        let values = serde_json::json!({"agentSandbox": {"runnerImages": {"acme-b": EARLIER}}});
        assert_eq!(
            bind_need(&values, "acme-a", &BTreeMap::new(), None),
            BindNeed::Current
        );
        let values = serde_json::json!({"agentSandbox": {"runnerImages": {
            "acme-a": DIGEST,
            "acme-b": EARLIER
        }}});
        assert_eq!(
            bind_need(&values, "acme-a", &BTreeMap::new(), Some(DIGEST)),
            BindNeed::Current
        );
    }

    #[test]
    fn bind_commands_set_the_runner_digest_without_a_secret_file() {
        let cmds = bind_commands(&opts(
            BTreeMap::new(),
            RunnerImageUpdate::Set(DIGEST.into()),
        ))
        .unwrap();
        let helm = cmds[0].display();
        assert!(
            helm.contains("helm upgrade curie charts/curie -n curie"),
            "{helm}"
        );
        assert!(helm.contains("--reset-then-reuse-values"), "{helm}");
        assert!(!helm.contains(" --reuse-values"), "{helm}");
        assert!(
            helm.contains(&format!("--set agentSandbox.runnerImages.acme-a={DIGEST}")),
            "{helm}"
        );
        assert!(
            !helm.contains("-f "),
            "no secrets means no values file: {helm}"
        );
        let delete = cmds.last().unwrap().display();
        assert!(delete.contains("delete sandboxclaim"), "{delete}");
        assert!(
            delete.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}=acme-a")),
            "{delete}"
        );
    }

    #[test]
    fn bind_commands_clear_the_runner_image_with_null() {
        let cmds = bind_commands(&opts(secrets(), RunnerImageUpdate::Clear)).unwrap();
        let helm = cmds[0].display();
        assert!(helm.contains("--reset-then-reuse-values"), "{helm}");
        assert!(helm.contains("-f"), "{helm}");
        assert!(
            helm.contains("--set agentSandbox.runnerImages.acme-a=null"),
            "{helm}"
        );
        assert!(
            !helm.contains("ghp_agent_a"),
            "secret leaked into argv: {helm}"
        );
        assert!(cmds
            .last()
            .unwrap()
            .display()
            .contains("delete sandboxclaim"));
    }

    #[test]
    fn bind_commands_are_empty_with_no_secrets_and_the_runner_kept() {
        assert!(
            bind_commands(&opts(BTreeMap::new(), RunnerImageUpdate::Keep))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn locked_runner_digest_upgrades_the_release_and_retires_claims() {
        // #3260 AC1: the recorded digest reaches the release and the agent's
        // claimed sandboxes are rolled onto it.
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-a": EARLIER}
        }}));
        let need = bind_if_changed(
            common(),
            "acme-a".into(),
            BTreeMap::new(),
            Some(DIGEST.to_string()),
            async { Ok("charts/curie".to_string()) },
        )
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        assert_eq!(helm.revision(), 8);
        let log = helm.helm_log();
        assert!(
            log.contains(&format!("agentSandbox.runnerImages.acme-a={DIGEST}")),
            "{log}"
        );
        assert!(log.contains("@sha256:"), "{log}");
        assert!(log.contains("--reset-then-reuse-values"), "{log}");
        let kubectl = helm.kubectl_log().expect("claims must be retired");
        assert!(kubectl.contains("delete sandboxclaim"), "{kubectl}");
        assert!(
            kubectl.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}=acme-a")),
            "{kubectl}"
        );
    }

    #[tokio::test]
    async fn no_runner_entry_clears_an_earlier_runner_image() {
        // #3260 AC2.
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-a": EARLIER}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: vec![],
                runner_image: true
            }
        );
        assert_eq!(helm.revision(), 8);
        let log = helm.helm_log();
        assert!(
            log.contains("agentSandbox.runnerImages.acme-a=null"),
            "{log}"
        );
        assert!(log.contains("--reset-then-reuse-values"), "{log}");
        let kubectl = helm.kubectl_log().expect("claims must be retired");
        assert!(kubectl.contains("delete sandboxclaim"), "{kubectl}");
    }

    #[tokio::test]
    async fn unchanged_runner_digest_leaves_the_release_but_retires_claims() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-a": DIGEST}
        }}));
        let need = bind_if_changed(
            common(),
            "acme-a".into(),
            BTreeMap::new(),
            Some(DIGEST.to_string()),
            async { panic!("an unchanged bind must not resolve a chart") },
        )
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7, "release was upgraded for a no-op bind");
        assert_eq!(helm.helm_log(), "");
        let kubectl = helm.kubectl_log().expect("claims must be retired");
        assert!(
            kubectl.contains("delete sandboxclaim"),
            "claims must still be retired so the runner layer loads: {kubectl}"
        );
    }

    #[tokio::test]
    async fn no_runner_no_secrets_and_no_earlier_value_does_nothing() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({"agentSandbox": {
            "runnerImages": {"acme-b": EARLIER}
        }}));
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            panic!("a no-op bind must not resolve a chart")
        })
        .await
        .unwrap();
        assert_eq!(need, BindNeed::Current);
        assert_eq!(helm.revision(), 7);
        assert_eq!(helm.helm_log(), "");
        assert_eq!(helm.kubectl_log(), None, "no sandbox should be touched");
    }

    #[tokio::test]
    async fn failed_values_read_with_no_runner_still_clears_the_runner_image() {
        let _env = crate::PROCESS_ENV_LOCK.lock().await;
        let helm = StubbedHelm::install(&serde_json::json!({}));
        // `cat` of a missing file fails, so `helm get values` exits non-zero.
        // An unread release could still hold an earlier image (AC2), so the
        // clear is sent rather than assumed unnecessary.
        std::fs::remove_file(helm.dir.path().join("values.json")).unwrap();
        let need = bind_if_changed(common(), "acme-a".into(), BTreeMap::new(), None, async {
            Ok("charts/curie".to_string())
        })
        .await
        .unwrap();
        assert_eq!(
            need,
            BindNeed::Changed {
                secrets: Vec::new(),
                runner_image: true
            }
        );
        assert!(
            helm.helm_log()
                .contains("agentSandbox.runnerImages.acme-a=null"),
            "{}",
            helm.helm_log()
        );
    }
}
