//! `curie cluster comms`: wire or clear the cluster's real Slack surface
//! with one `helm upgrade --reuse-values`, keeping the chart as the source of
//! truth.

use anyhow::{bail, Result};

use crate::local::{fake_model_env_override, otel_endpoint_env_override, ModelMode};
use crate::ops::{plain, require_on_path, run_step, secret_set, CommonOpts, OpsCommand};

/// The worker's Slack stub sink URL (compose default); restored on disconnect.
const LOCAL_SLACK_STUB_URL: &str = "http://localhost:8155/api/";
/// The worker's stub bot token (compose default); restored on disconnect.
const LOCAL_SLACK_STUB_BOT_TOKEN: &str = "xoxb-dev";

#[derive(Debug, Clone)]
pub struct CommsOpts {
    pub common: CommonOpts,
    pub chart: String,
    pub app_token: String,
    pub bot_token: String,
    pub disconnect: bool,
}

pub struct LocalCommsOpts {
    pub file: String,
    pub dry_run: bool,
    pub app_token: String,
    pub bot_token: String,
    pub disconnect: bool,
    /// Model mode resolved from the shell (skill/`local up` parity, issue
    /// #450): the worker-restarting commands below apply the same
    /// `fake_model_env_override` as `up_command` so `local comms` never
    /// silently downgrades a live stack back to the fake model.
    pub model_mode: ModelMode,
    /// Model credentials resolved with the same precedence as `local up`.
    pub model_credentials: Vec<(String, String)>,
    /// Explicit provider model id to preserve from `local up`.
    pub model: Option<String>,
    /// Whether the stack was brought up with `local up --minimal` (the `core`
    /// profile, no otel-collector). Same parity obligation as `model_mode`: the
    /// worker-restarting commands below apply the same
    /// `otel_endpoint_env_override` as `up_command` so `local comms` never
    /// re-points a collector-less stack at a collector that is not running.
    pub minimal: bool,
}

pub fn connect_commands(opts: &CommsOpts) -> Vec<OpsCommand> {
    vec![OpsCommand::new(
        "helm",
        vec![
            plain("upgrade"),
            plain(&opts.common.release),
            plain(&opts.chart),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("--reuse-values"),
            plain("--set"),
            secret_set("dispatcher.slack.appToken", &opts.app_token),
            plain("--set"),
            secret_set("dispatcher.slack.botToken", &opts.bot_token),
            plain("--set"),
            plain("worker.slackApiBaseUrl="),
        ],
    )]
}

pub fn disconnect_commands(opts: &CommsOpts) -> Vec<OpsCommand> {
    vec![OpsCommand::new(
        "helm",
        vec![
            plain("upgrade"),
            plain(&opts.common.release),
            plain(&opts.chart),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("--reuse-values"),
            plain("--set"),
            plain("dispatcher.slack.appToken="),
            plain("--set"),
            plain("dispatcher.slack.botToken="),
        ],
    )]
}

pub fn local_connect_commands(o: &LocalCommsOpts) -> Vec<OpsCommand> {
    let mut env = vec![("SLACK_API_BASE_URL".into(), String::new())];
    env.extend(fake_model_env_override(o.model_mode));
    if let Some(model) = &o.model {
        env.push(("CURIE_MODEL".into(), model.clone()));
    }
    env.extend(otel_endpoint_env_override(o.minimal));
    vec![OpsCommand::new(
        "docker",
        vec![
            plain("compose"),
            plain("--profile"),
            plain("core"),
            plain("--profile"),
            plain("slack"),
            plain("-f"),
            plain(&o.file),
            plain("up"),
            plain("-d"),
            plain("--wait"),
            plain("curie-worker"),
            plain("curie-dispatcher"),
        ],
    )
    .with_env(env)
    .with_secret_env({
        let mut secret_env = vec![
            ("SLACK_APP_TOKEN".into(), o.app_token.clone()),
            ("SLACK_BOT_TOKEN".into(), o.bot_token.clone()),
        ];
        secret_env.extend(o.model_credentials.clone());
        secret_env
    })]
}

pub fn local_disconnect_commands(o: &LocalCommsOpts) -> Vec<OpsCommand> {
    let mut worker_env = vec![
        ("SLACK_API_BASE_URL".into(), LOCAL_SLACK_STUB_URL.into()),
        ("SLACK_BOT_TOKEN".into(), LOCAL_SLACK_STUB_BOT_TOKEN.into()),
    ];
    worker_env.extend(fake_model_env_override(o.model_mode));
    if let Some(model) = &o.model {
        worker_env.push(("CURIE_MODEL".into(), model.clone()));
    }
    worker_env.extend(otel_endpoint_env_override(o.minimal));
    vec![
        OpsCommand::new(
            "docker",
            vec![
                plain("compose"),
                plain("--profile"),
                plain("core"),
                plain("--profile"),
                plain("slack"),
                plain("-f"),
                plain(&o.file),
                plain("stop"),
                plain("curie-dispatcher"),
            ],
        ),
        OpsCommand::new(
            "docker",
            vec![
                plain("compose"),
                plain("--profile"),
                plain("core"),
                plain("-f"),
                plain(&o.file),
                plain("up"),
                plain("-d"),
                plain("--wait"),
                plain("curie-worker"),
            ],
        )
        .with_env(worker_env)
        .with_secret_env(o.model_credentials.clone()),
    ]
}

/// `kubectl -n <ns> rollout restart deployment/<release>-<component>`: force the
/// pods to pick up the new Secret-backed Slack tokens (secretKeyRef env vars are
/// resolved once at pod start, so a Secret change alone does not roll them).
fn rollout_restart_command(namespace: &str, release: &str, component: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("rollout"),
            plain("restart"),
            plain(format!("deployment/{release}-{component}")),
        ],
    )
}

/// `kubectl -n <ns> rollout status deployment/<release>-<component> --timeout=120s`:
/// wait for the restarted pods to become ready before reporting success.
fn rollout_status_command(namespace: &str, release: &str, component: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("rollout"),
            plain("status"),
            plain(format!("deployment/{release}-{component}")),
            plain("--timeout=120s"),
        ],
    )
}

/// The kubectl rollout commands that follow the helm upgrade so the running pods
/// pick up the token change. Connect must roll the worker AND the dispatcher (an
/// existing dispatcher would otherwise keep stale tokens; a freshly rendered one
/// is rolled harmlessly). Disconnect rolls only the worker -- helm deletes the
/// dispatcher (its gate `curie.dispatcher.enabled` goes false), so there is no
/// dispatcher to wait on.
pub fn rollout_commands(disconnect: bool, namespace: &str, release: &str) -> Vec<OpsCommand> {
    let components: &[&str] = if disconnect {
        &["worker"]
    } else {
        &["worker", "dispatcher"]
    };
    let mut cmds: Vec<OpsCommand> = components
        .iter()
        .map(|c| rollout_restart_command(namespace, release, c))
        .collect();
    cmds.extend(
        components
            .iter()
            .map(|c| rollout_status_command(namespace, release, c)),
    );
    cmds
}

/// Exactly one chat surface must be selected. `--slack` is the only surface
/// today; a future surface adds another flag and this check widens.
pub fn require_provider(slack: bool) -> Result<()> {
    if !slack {
        bail!("specify a chat surface, e.g. --slack");
    }
    Ok(())
}

/// On connect, both Slack tokens must be present (from env or the explicit
/// flags). Disconnect needs no tokens, so it always passes.
pub fn require_connect_tokens(disconnect: bool, app_token: &str, bot_token: &str) -> Result<()> {
    if !disconnect && (app_token.is_empty() || bot_token.is_empty()) {
        bail!(
            "Slack tokens missing; set SLACK_APP_TOKEN and SLACK_BOT_TOKEN (or pass --app-token/--bot-token)"
        );
    }
    Ok(())
}

/// Decide a Slack token from the two candidate sources, highest wins: the
/// clap-merged `--flag`/env value, then a value persisted via `curie secrets
/// set` (#749). `cli_value` is empty when neither the flag nor the env var was
/// supplied; `saved` is `None` when the vault has no entry. Returns the empty
/// string when nothing is available, which `require_connect_tokens` then rejects
/// on connect. Pure so the precedence (flag/env > saved vault) is unit-testable
/// without touching the vault or the process env.
pub fn resolve_slack_token(cli_value: &str, saved: Option<String>) -> String {
    if !cli_value.is_empty() {
        return cli_value.to_string();
    }
    saved.unwrap_or_default()
}

/// Resolve a `local comms` Slack token from the live vault, so a token persisted
/// once with `curie secrets set SLACK_APP_TOKEN`/`SLACK_BOT_TOKEN` survives
/// across sessions and `--slack` needs no per-session re-export (#749).
/// Precedence: the clap-merged `--flag`/env `cli_value` wins; only when it is
/// empty is the vault consulted. The vault is never read on `--disconnect`
/// (which needs no tokens) or when a value was already supplied, so an unrelated
/// run never opens the credential store. Emits a masked "loaded from storage"
/// note, never the value; the resolved token still travels only through the
/// masked `secret_env` channel downstream.
pub fn resolve_local_slack_token(name: &str, cli_value: &str, disconnect: bool) -> Result<String> {
    if !cli_value.is_empty() || disconnect {
        return Ok(cli_value.to_string());
    }
    let saved = if crate::secrets::is_saved(name)? {
        match crate::secrets::get_value(name)? {
            Some(value) => {
                crate::ui::ui().note(&format!(
                    "{name}: loaded from Curie private storage for this run"
                ));
                Some(value)
            }
            None => None,
        }
    } else {
        None
    };
    Ok(resolve_slack_token(cli_value, saved))
}

/// Output of `<tier> comms`: the dry-run plan, or the resulting Slack wiring
/// state. `--json` emits a JSON object; the real path formerly emitted only
/// stderr notes, so `comms --json` on success exited 0 with empty stdout (#485).
#[derive(Debug)]
pub enum CommsOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { connected: bool },
}

impl crate::ui::CliOutput for CommsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            CommsOutput::DryRun(plan) => plan.to_json(),
            CommsOutput::Done { connected } => serde_json::json!({"connected": connected}),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if let CommsOutput::DryRun(plan) = self {
            plan.render(ui);
        }
        // The `Done` note is emitted live during execution (below), so nothing
        // to render on the human path here.
    }
}

pub async fn comms(opts: CommsOpts) -> Result<CommsOutput> {
    let ui = crate::ui::ui();
    require_connect_tokens(opts.disconnect, &opts.app_token, &opts.bot_token)?;

    let cmds = if opts.disconnect {
        disconnect_commands(&opts)
    } else {
        connect_commands(&opts)
    };
    let rollout = rollout_commands(
        opts.disconnect,
        &opts.common.namespace,
        &opts.common.release,
    );

    if opts.common.dry_run {
        return Ok(CommsOutput::DryRun(crate::ui::DryRunPlan {
            lines: cmds
                .iter()
                .chain(rollout.iter())
                .map(|cmd| cmd.display())
                .collect(),
        }));
    }

    require_on_path("helm")?;
    require_on_path("kubectl")?;
    let cl = ui.checklist();
    let label = if opts.disconnect {
        format!("disconnecting Slack from release {}", opts.common.release)
    } else {
        format!("connecting Slack to release {}", opts.common.release)
    };
    let ok_detail = if opts.disconnect {
        "disconnected"
    } else {
        "connected"
    };
    for cmd in &cmds {
        run_step(&cl, &label, ok_detail, cmd).await?;
    }
    // The Secret change alone does not roll the pods (secretKeyRef env vars are
    // resolved once at pod start), so restart them and wait for the new/cleared
    // token to be live before reporting success.
    let roll_label = format!("rolling {} to pick up tokens", opts.common.release);
    for cmd in &rollout {
        run_step(&cl, &roll_label, "rolled", cmd).await?;
    }
    if opts.disconnect {
        ui.note("Slack disconnected; dispatcher tokens cleared");
    } else {
        ui.note("Slack connected");
    }
    Ok(CommsOutput::Done {
        connected: !opts.disconnect,
    })
}

pub async fn local_comms(opts: LocalCommsOpts) -> Result<CommsOutput> {
    let ui = crate::ui::ui();
    require_connect_tokens(opts.disconnect, &opts.app_token, &opts.bot_token)?;
    let cmds = if opts.disconnect {
        local_disconnect_commands(&opts)
    } else {
        local_connect_commands(&opts)
    };

    if opts.dry_run {
        return Ok(CommsOutput::DryRun(crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        }));
    }

    if opts.model_mode == ModelMode::FakePinnedDespiteCredential {
        ui.warn(
            "Running the FAKE model despite a credential in your shell: CURIE_FAKE_MODEL is pinned on. Unset it or set CURIE_FAKE_MODEL=0 to go live.",
        );
    }
    require_on_path("docker")?;
    let cl = ui.checklist();
    let label = if opts.disconnect {
        "disconnecting Slack from the local stack"
    } else {
        "connecting Slack to the local stack"
    };
    let ok_detail = if opts.disconnect {
        "disconnected"
    } else {
        "connected"
    };
    for cmd in &cmds {
        run_step(&cl, label, ok_detail, cmd).await?;
    }
    if opts.disconnect {
        ui.note("Slack disconnected; worker back on the local stub");
    } else {
        ui.note("Slack connected to the local stack (dispatcher running, worker on real Slack)");
    }
    Ok(CommsOutput::Done {
        connected: !opts.disconnect,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn common() -> CommonOpts {
        CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        }
    }

    #[test]
    fn require_provider_ok_only_with_a_surface() {
        assert!(require_provider(true).is_ok());
        let err = require_provider(false).unwrap_err().to_string();
        assert!(err.contains("specify a chat surface"), "{err}");
    }

    /// #749 token resolution precedence: the clap-merged `--flag`/env value wins;
    /// only when it is empty does the saved vault value apply; empty + no saved
    /// value yields the empty string (which `require_connect_tokens` rejects on
    /// connect).
    #[test]
    fn resolve_slack_token_prefers_flag_env_then_saved_vault() {
        // Flag/env supplied: wins even when a vault value also exists.
        assert_eq!(
            resolve_slack_token("xapp-flag", Some("xapp-saved".to_string())),
            "xapp-flag"
        );
        // Flag/env empty: fall back to the saved vault value.
        assert_eq!(
            resolve_slack_token("", Some("xapp-saved".to_string())),
            "xapp-saved"
        );
        // Nothing anywhere: empty (rejected downstream on connect).
        assert_eq!(resolve_slack_token("", None), "");
    }

    #[test]
    fn require_connect_tokens_guards_connect_but_not_disconnect() {
        assert!(require_connect_tokens(true, "", "").is_ok());
        assert!(require_connect_tokens(false, "xapp-1", "xoxb-1").is_ok());
        let missing_app = require_connect_tokens(false, "", "xoxb-1")
            .unwrap_err()
            .to_string();
        assert!(
            missing_app.contains("Slack tokens missing"),
            "{missing_app}"
        );
        let missing_bot = require_connect_tokens(false, "xapp-1", "")
            .unwrap_err()
            .to_string();
        assert!(
            missing_bot.contains("Slack tokens missing"),
            "{missing_bot}"
        );
    }

    #[test]
    fn connect_command_sets_tokens_and_clears_stub_wiring() {
        let cmds = connect_commands(&CommsOpts {
            common: common(),
            chart: "charts/curie".into(),
            app_token: "xapp-123456789".into(),
            bot_token: "xoxb-123456789".into(),
            disconnect: false,
        });
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0].display(),
            "helm upgrade curie charts/curie -n curie --reuse-values \
             --set 'dispatcher.slack.appToken=xapp-123***' \
             --set 'dispatcher.slack.botToken=xoxb-123***' \
             --set worker.slackApiBaseUrl="
        );
    }

    #[test]
    fn connect_display_masks_both_tokens_but_argv_keeps_raw_values() {
        let cmds = connect_commands(&CommsOpts {
            common: common(),
            chart: "charts/curie".into(),
            app_token: "xapp-1-secretsecret".into(),
            bot_token: "xoxb-1-secretsecret".into(),
            disconnect: false,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("dispatcher.slack.appToken=xapp-1-s***"),
            "{line}"
        );
        assert!(
            line.contains("dispatcher.slack.botToken=xoxb-1-s***"),
            "{line}"
        );
        assert!(!line.contains("secretsecret"), "secret leaked: {line}");

        let argv = cmds[0].argv().join(" ");
        assert!(
            argv.contains("dispatcher.slack.appToken=xapp-1-secretsecret"),
            "{argv}"
        );
        assert!(
            argv.contains("dispatcher.slack.botToken=xoxb-1-secretsecret"),
            "{argv}"
        );
    }

    #[test]
    fn rollout_commands_connect_rolls_worker_and_dispatcher() {
        let cmds = rollout_commands(false, "curie", "curie");
        let lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        assert_eq!(
            lines,
            vec![
                "kubectl -n curie rollout restart deployment/curie-worker".to_string(),
                "kubectl -n curie rollout restart deployment/curie-dispatcher".to_string(),
                "kubectl -n curie rollout status deployment/curie-worker --timeout=120s"
                    .to_string(),
                "kubectl -n curie rollout status deployment/curie-dispatcher --timeout=120s"
                    .to_string(),
            ]
        );
    }

    #[test]
    fn rollout_commands_disconnect_rolls_worker_only() {
        let cmds = rollout_commands(true, "curie", "curie");
        let lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        assert_eq!(
            lines,
            vec![
                "kubectl -n curie rollout restart deployment/curie-worker".to_string(),
                "kubectl -n curie rollout status deployment/curie-worker --timeout=120s"
                    .to_string(),
            ]
        );
        assert!(
            !lines.iter().any(|l| l.contains("dispatcher")),
            "disconnect must not touch the dispatcher: {lines:?}"
        );
    }

    #[test]
    fn disconnect_command_clears_tokens_without_stub_url_or_secret_bytes() {
        let cmds = disconnect_commands(&CommsOpts {
            common: common(),
            chart: "charts/curie".into(),
            app_token: "xapp-1-secretsecret".into(),
            bot_token: "xoxb-1-secretsecret".into(),
            disconnect: true,
        });
        let line = cmds[0].display();
        assert_eq!(
            line,
            "helm upgrade curie charts/curie -n curie --reuse-values \
             --set dispatcher.slack.appToken= --set dispatcher.slack.botToken="
        );
        assert!(!line.contains("worker.slackApiBaseUrl"), "{line}");
        assert!(!line.contains("xapp-"), "{line}");
        assert!(!line.contains("xoxb-"), "{line}");
    }

    #[test]
    fn comms_upgrade_does_not_drop_the_github_token() {
        // #1124 AC3, armed through the SIBLING verb only. `cluster comms` is one
        // of only three `helm upgrade` sites on this release, and #1067's whole
        // lesson is that a sibling upgrade verb can silently destroy another
        // verb's recorded values. Both comms arms pass `--reuse-values`, which
        // helm documents as "existing values will be merged with any values set
        // via --values/-f or --set flags. Priority is given to new values"
        // (`helm upgrade --help`, helm v3) -- so a value neither arm re-passes,
        // `api.githubToken` among them, survives the upgrade untouched. `up`, by
        // contrast, does a FULL upgrade, which is why it needs its own preserve.
        let opts = |disconnect| CommsOpts {
            common: common(),
            chart: "charts/curie".into(),
            app_token: "xapp-123456789".into(),
            bot_token: "xoxb-123456789".into(), // gitleaks:allow -- dummy placeholder Slack token, not real (moved from line 347; see .gitleaksignore)
            disconnect,
        };
        for (label, cmds) in [
            ("connect", connect_commands(&opts(false))),
            ("disconnect", disconnect_commands(&opts(true))),
        ] {
            let argv = cmds[0].argv().join(" ");
            assert!(
                argv.contains("--reuse-values"),
                "{label} must keep --reuse-values or it would drop api.githubToken: {argv}"
            );
            assert!(
                !argv.contains("api.githubToken"),
                "{label} must not re-pass or clear the GitHub credential: {argv}"
            );
        }
    }

    fn local_comms_opts(disconnect: bool, mode: ModelMode) -> LocalCommsOpts {
        local_comms_opts_with(disconnect, mode, false)
    }

    fn local_comms_opts_with(disconnect: bool, mode: ModelMode, minimal: bool) -> LocalCommsOpts {
        LocalCommsOpts {
            file: "compose.dev.yaml".into(),
            dry_run: false,
            app_token: if disconnect {
                String::new()
            } else {
                "xapp-1".into()
            },
            bot_token: if disconnect {
                String::new()
            } else {
                "xoxb-1".into()
            },
            disconnect,
            model_mode: mode,
            model_credentials: vec![],
            model: None,
            minimal,
        }
    }

    #[test]
    fn local_connect_command_wires_dispatcher_and_unwires_stub() {
        let cmds = local_connect_commands(&LocalCommsOpts {
            file: "compose.dev.yaml".into(),
            dry_run: false,
            app_token: "xapp-1-secretsecret".into(),
            bot_token: "xoxb-1-secretsecret".into(),
            disconnect: false,
            model_mode: ModelMode::DefaultFake,
            model_credentials: vec![],
            model: None,
            minimal: false,
        });
        assert_eq!(cmds.len(), 1);
        let line = cmds[0].display();
        assert_eq!(
            line,
            "SLACK_API_BASE_URL= 'SLACK_APP_TOKEN=xapp-1-s***' \
             'SLACK_BOT_TOKEN=xoxb-1-s***' \
             docker compose --profile core --profile slack -f compose.dev.yaml up -d --wait \
             curie-worker curie-dispatcher"
        );
        assert!(!line.contains("secretsecret"), "secret leaked: {line}");
    }

    #[test]
    fn local_disconnect_commands_stop_dispatcher_and_restub_worker() {
        let cmds = local_disconnect_commands(&local_comms_opts(true, ModelMode::DefaultFake));
        assert_eq!(cmds.len(), 2);
        assert_eq!(
            cmds[0].display(),
            "docker compose --profile core --profile slack -f compose.dev.yaml stop curie-dispatcher"
        );
        let second = cmds[1].display();
        assert_eq!(
            second,
            "SLACK_API_BASE_URL=http://localhost:8155/api/ \
             SLACK_BOT_TOKEN=xoxb-dev \
             docker compose --profile core -f compose.dev.yaml up -d --wait curie-worker"
        );
        assert!(
            !second.contains("curie-dispatcher"),
            "disconnect worker restart must not mention dispatcher: {second}"
        );
    }

    /// Issue #450: `local comms --slack` restarted the worker on compose's fake
    /// default even when a real model credential was present in the shell. With
    /// a credential (`LiveFromCredential`), connect must inject
    /// `CURIE_FAKE_MODEL=0` exactly once, and the pre-existing
    /// `SLACK_API_BASE_URL` clear must survive alongside it (env is REPLACED,
    /// not appended, by `with_env`, so building the full vec once is load-bearing).
    #[test]
    fn local_connect_live_from_credential_injects_fake_zero_once() {
        let cmds = local_connect_commands(&local_comms_opts(false, ModelMode::LiveFromCredential));
        let cmd = &cmds[0];
        assert_eq!(
            cmd.env
                .iter()
                .filter(|(k, _)| k == "CURIE_FAKE_MODEL")
                .count(),
            1,
            "exactly one CURIE_FAKE_MODEL; env={:?}",
            cmd.env
        );
        assert!(cmd
            .env
            .contains(&("CURIE_FAKE_MODEL".to_string(), "0".to_string())));
        assert!(
            cmd.env
                .contains(&("SLACK_API_BASE_URL".to_string(), String::new())),
            "SLACK_API_BASE_URL clear must survive the env rebuild; env={:?}",
            cmd.env
        );
    }

    /// `DefaultFake` and `FakePinnedDespiteCredential` must inject no
    /// `CURIE_FAKE_MODEL` at all, leaving compose's `${CURIE_FAKE_MODEL:-1}`
    /// default (or the operator's pin) alone.
    #[test]
    fn local_connect_non_live_modes_inject_nothing() {
        for mode in [
            ModelMode::DefaultFake,
            ModelMode::FakePinnedDespiteCredential,
        ] {
            let cmds = local_connect_commands(&local_comms_opts(false, mode));
            assert!(
                !cmds[0].env.iter().any(|(k, _)| k == "CURIE_FAKE_MODEL"),
                "{mode:?} must not inject CURIE_FAKE_MODEL; env={:?}",
                cmds[0].env
            );
        }
    }

    /// Same bug, same fix, on the worker-restarting leg of disconnect: it also
    /// silently reverted to the fake model. The stub `SLACK_API_BASE_URL` /
    /// `SLACK_BOT_TOKEN` entries must survive the env rebuild alongside the
    /// injected override.
    #[test]
    fn local_disconnect_live_from_credential_injects_fake_zero_and_keeps_stub_env() {
        let cmds =
            local_disconnect_commands(&local_comms_opts(true, ModelMode::LiveFromCredential));
        let worker_cmd = &cmds[1];
        assert_eq!(
            worker_cmd
                .env
                .iter()
                .filter(|(k, _)| k == "CURIE_FAKE_MODEL")
                .count(),
            1,
            "exactly one CURIE_FAKE_MODEL; env={:?}",
            worker_cmd.env
        );
        assert!(worker_cmd
            .env
            .contains(&("CURIE_FAKE_MODEL".to_string(), "0".to_string())));
        assert!(worker_cmd.env.contains(&(
            "SLACK_API_BASE_URL".to_string(),
            LOCAL_SLACK_STUB_URL.to_string(),
        )));
        assert!(worker_cmd.env.contains(&(
            "SLACK_BOT_TOKEN".to_string(),
            LOCAL_SLACK_STUB_BOT_TOKEN.to_string(),
        )));
    }

    #[test]
    fn local_disconnect_non_live_modes_inject_nothing() {
        for mode in [
            ModelMode::DefaultFake,
            ModelMode::FakePinnedDespiteCredential,
        ] {
            let cmds = local_disconnect_commands(&local_comms_opts(true, mode));
            let worker_cmd = &cmds[1];
            assert!(
                !worker_cmd.env.iter().any(|(k, _)| k == "CURIE_FAKE_MODEL"),
                "{mode:?} must not inject CURIE_FAKE_MODEL; env={:?}",
                worker_cmd.env
            );
        }
    }

    /// Anti-drift guard (issue #450): `local up` and `local comms` connect must
    /// agree on whether/how they inject `CURIE_FAKE_MODEL`, for every
    /// `ModelMode`. Compares `up_command` (with `local_model: None`, since that
    /// path is its own independent live route) against `local_connect_commands`.
    #[test]
    fn up_and_local_connect_agree_on_fake_model_override_for_every_mode() {
        for mode in [
            ModelMode::LiveFromCredential,
            ModelMode::FakePinnedDespiteCredential,
            ModelMode::DefaultFake,
        ] {
            let up_env = crate::local::up_command(&crate::local::LocalOpts {
                file: "compose.dev.yaml".into(),
                dry_run: false,
                minimal: false,
                local_model: None,
                pull_model: false,
                slack: false,
                model_mode: mode,
                env_file: None,
                build: false,
            })
            .env;
            let connect_env = local_connect_commands(&local_comms_opts(false, mode))[0]
                .env
                .clone();
            let up_override: Option<&(String, String)> =
                up_env.iter().find(|(k, _)| k == "CURIE_FAKE_MODEL");
            let connect_override: Option<&(String, String)> =
                connect_env.iter().find(|(k, _)| k == "CURIE_FAKE_MODEL");
            assert_eq!(
                up_override, connect_override,
                "{mode:?}: up_command and local_connect_commands disagree on \
                 CURIE_FAKE_MODEL; up={up_env:?} connect={connect_env:?}"
            );
        }
    }

    /// Same anti-drift guard (issue #450) for the DISCONNECT leg: `local up`
    /// and `local comms --disconnect`'s worker-restarting command must agree
    /// on whether/how they inject `CURIE_FAKE_MODEL`, for every `ModelMode`.
    #[test]
    fn up_and_local_disconnect_agree_on_fake_model_override_for_every_mode() {
        for mode in [
            ModelMode::LiveFromCredential,
            ModelMode::FakePinnedDespiteCredential,
            ModelMode::DefaultFake,
        ] {
            let up_env = crate::local::up_command(&crate::local::LocalOpts {
                file: "compose.dev.yaml".into(),
                dry_run: false,
                minimal: false,
                local_model: None,
                pull_model: false,
                slack: false,
                model_mode: mode,
                env_file: None,
                build: false,
            })
            .env;
            let disconnect_env = local_disconnect_commands(&local_comms_opts(true, mode))[1]
                .env
                .clone();
            let up_override: Option<&(String, String)> =
                up_env.iter().find(|(k, _)| k == "CURIE_FAKE_MODEL");
            let disconnect_override: Option<&(String, String)> =
                disconnect_env.iter().find(|(k, _)| k == "CURIE_FAKE_MODEL");
            assert_eq!(
                up_override, disconnect_override,
                "{mode:?}: up_command and local_disconnect_commands disagree on \
                 CURIE_FAKE_MODEL; up={up_env:?} disconnect={disconnect_env:?}"
            );
        }
    }

    /// Anti-drift guard (issue #545): `local up` and `local comms` connect must
    /// agree on whether/how they suppress `OTEL_EXPORTER_OTLP_ENDPOINT`, for
    /// both `minimal` values. `local comms` recreates the worker, so a dropped
    /// override here re-resolves compose's collector default onto a `core`-only
    /// stack that never started one, and every span pays a synchronous DNS retry.
    #[test]
    fn up_and_local_connect_agree_on_otel_endpoint_override_for_every_minimal() {
        for minimal in [false, true] {
            let up_env = crate::local::up_command(&crate::local::LocalOpts {
                file: "compose.dev.yaml".into(),
                dry_run: false,
                minimal,
                local_model: None,
                pull_model: false,
                slack: false,
                model_mode: ModelMode::DefaultFake,
                env_file: None,
                build: false,
            })
            .env;
            let connect_env = local_connect_commands(&local_comms_opts_with(
                false,
                ModelMode::DefaultFake,
                minimal,
            ))[0]
                .env
                .clone();
            let up_override: Option<&(String, String)> = up_env
                .iter()
                .find(|(k, _)| k == "OTEL_EXPORTER_OTLP_ENDPOINT");
            let connect_override: Option<&(String, String)> = connect_env
                .iter()
                .find(|(k, _)| k == "OTEL_EXPORTER_OTLP_ENDPOINT");
            assert_eq!(
                up_override, connect_override,
                "minimal={minimal}: up_command and local_connect_commands disagree on \
                 OTEL_EXPORTER_OTLP_ENDPOINT; up={up_env:?} connect={connect_env:?}"
            );
        }
    }

    /// Same anti-drift guard (issue #545) for the DISCONNECT leg: its
    /// worker-restarting command must agree with `local up` on
    /// `OTEL_EXPORTER_OTLP_ENDPOINT`, for both `minimal` values.
    #[test]
    fn up_and_local_disconnect_agree_on_otel_endpoint_override_for_every_minimal() {
        for minimal in [false, true] {
            let up_env = crate::local::up_command(&crate::local::LocalOpts {
                file: "compose.dev.yaml".into(),
                dry_run: false,
                minimal,
                local_model: None,
                pull_model: false,
                slack: false,
                model_mode: ModelMode::DefaultFake,
                env_file: None,
                build: false,
            })
            .env;
            let disconnect_env = local_disconnect_commands(&local_comms_opts_with(
                true,
                ModelMode::DefaultFake,
                minimal,
            ))[1]
                .env
                .clone();
            let up_override: Option<&(String, String)> = up_env
                .iter()
                .find(|(k, _)| k == "OTEL_EXPORTER_OTLP_ENDPOINT");
            let disconnect_override: Option<&(String, String)> = disconnect_env
                .iter()
                .find(|(k, _)| k == "OTEL_EXPORTER_OTLP_ENDPOINT");
            assert_eq!(
                up_override, disconnect_override,
                "minimal={minimal}: up_command and local_disconnect_commands disagree on \
                 OTEL_EXPORTER_OTLP_ENDPOINT; up={up_env:?} disconnect={disconnect_env:?}"
            );
        }
    }
}
