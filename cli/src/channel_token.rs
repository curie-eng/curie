//! `curie cluster channel-token`: mint or inspect the adapter's channel token.
//!
//! Through 0.8.4 recovery was six undiscoverable steps (port-forward, read the
//! platform key, POST /channels/token, decode exp, kubectl patch, rollout).
//! Writing the token through helm values also left it in `helm get values`, so a
//! later values-file upgrade silently undid a kubectl-patched rotation (#2378).
//!
//! This verb mints through the platform API, writes the Secret the adapter
//! actually reads (chart Secret or `channelTokenExistingSecret`) from a 0600
//! temp file so the token never enters argv, rolls the adapter, and prints
//! `exp`. It never prints the token. `--show-exp` is the read-only sibling of
//! doctor's mail-channel check: same `/statusz` observation, no Secret read.

use anyhow::Result;
use base64::Engine;

use crate::api::ApiClient;
use crate::mail_channel::{self, TokenState};
use crate::ops::{
    chart_fullname, fetch_release_computed_values, plain, release_fullname, require_on_path,
    resolve_existing_secret_ref, run_step, secret_patch_file, CommonOpts, OpsCommand,
};

/// API ceiling on `ChannelTokenRequest.ttl_s` (one week).
pub const MAX_TTL_S: i64 = 604_800;

/// Default lifetime for this verb: the API ceiling. Recovery from expiry is
/// the use case; a one-hour default would make the operator pass `--ttl` on
/// every recovery.
pub const DEFAULT_TTL: &str = "7d";

const DEFAULT_DATA_KEY: &str = "mailChannelToken";
const EXISTING_SECRET: &str = "mailAdapter.channelTokenExistingSecret";
const EXISTING_SECRET_KEY: &str = "mailAdapter.channelTokenExistingSecretKey";

#[derive(Debug, Clone)]
pub struct ChannelTokenOpts {
    pub common: CommonOpts,
    pub api_url: String,
    pub api_key: String,
    pub agent: String,
    pub kind: Option<String>,
    pub address: Option<String>,
    pub ttl: String,
    pub show_exp: bool,
}

pub enum ChannelTokenOutput {
    DryRun(crate::ui::DryRunPlan),
    Minted {
        agent: String,
        kind: String,
        address: String,
        exp: i64,
        expires_at: String,
        secret_name: String,
        secret_key: String,
    },
    ShowExp {
        exp: Option<i64>,
        expires_at: Option<String>,
        accepted: bool,
        state: String,
        detail: String,
    },
}

impl crate::ui::CliOutput for ChannelTokenOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ChannelTokenOutput::DryRun(plan) => plan.to_json(),
            ChannelTokenOutput::Minted {
                agent,
                kind,
                address,
                exp,
                expires_at,
                secret_name,
                secret_key,
            } => serde_json::json!({
                "agent": agent,
                "kind": kind,
                "address": address,
                "exp": exp,
                "expires_at": expires_at,
                "secret": {"name": secret_name, "key": secret_key},
            }),
            ChannelTokenOutput::ShowExp {
                exp,
                expires_at,
                accepted,
                state,
                detail,
            } => serde_json::json!({
                "exp": exp,
                "expires_at": expires_at,
                "accepted": accepted,
                "state": state,
                "detail": detail,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ChannelTokenOutput::DryRun(plan) => plan.render(ui),
            ChannelTokenOutput::Minted {
                agent,
                kind,
                address,
                expires_at,
                secret_name,
                secret_key,
                ..
            } => ui.payload(&format!(
                "channel token for {agent} {kind}:{address} expires at {expires_at} \
                 (secret {secret_name}/{secret_key})"
            )),
            ChannelTokenOutput::ShowExp {
                expires_at,
                accepted,
                state,
                detail,
                ..
            } => {
                let accepted = if *accepted {
                    "accepted"
                } else {
                    "not accepted"
                };
                let exp = expires_at.as_deref().unwrap_or("unknown");
                ui.payload(&format!(
                    "channel token exp {exp} ({accepted}, state {state}); {detail}"
                ));
            }
        }
    }
}

pub async fn channel_token(opts: ChannelTokenOpts) -> Result<ChannelTokenOutput> {
    if opts.show_exp {
        return show_exp(opts).await;
    }
    let kind = opts.kind.clone().filter(|v| !v.is_empty()).ok_or_else(|| {
        crate::exit::usage(
            "channel-token requires --kind and --address (or pass --show-exp to inspect)",
        )
    })?;
    let address = opts
        .address
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            crate::exit::usage(
                "channel-token requires --kind and --address (or pass --show-exp to inspect)",
            )
        })?;
    let ttl_s = parse_ttl(&opts.ttl)?;
    if opts.common.dry_run {
        let (secret_name, secret_key) = resolve_token_secret(None, &opts.common.release);
        return Ok(ChannelTokenOutput::DryRun(crate::ui::DryRunPlan {
            lines: mint_plan(&opts, &kind, &address, ttl_s, &secret_name, &secret_key),
        }));
    }
    require_on_path("kubectl")?;
    require_on_path("helm")?;
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    if !agent
        .channels
        .iter()
        .any(|binding| binding.kind == kind && binding.address == address)
    {
        return Err(crate::exit::usage(format!(
            "agent {} has no {kind}:{address} surface; add it with \
             `curie cluster surfaces {} --add {kind}={address}` before minting a token",
            agent.name, agent.name
        )));
    }
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    // Resolve the Secret BEFORE minting. The mint is a rotation write: the API
    // bumps the binding's generation and signs the new value, so the token the
    // adapter is currently holding is revoked the moment that request returns
    // (#2379). There is no un-mint. Every step that can fail for a reason
    // unrelated to the token -- an expired kubeconfig, a misnamed release, a
    // `--namespace` typo, an API-server blip -- therefore has to run while
    // failing is still free, or a failed recovery attempt turns a scheduled
    // expiry into an outage (#2553).
    let values = fetch_release_computed_values(&opts.common).await?;
    let (secret_name, secret_key) = live_token_secret(&opts.common, values.as_ref()).await;
    let mint_step = cl.step(&format!("minting channel token for {kind}:{address}"));
    let token = match client.mint_channel_token(&kind, &address, ttl_s).await {
        Ok(token) => {
            mint_step.done("minted");
            token
        }
        Err(err) => {
            mint_step.fail("failed");
            return Err(err);
        }
    };
    let exp = token_exp(&token)?;
    let expires_at = format_exp(exp);
    let patch = serde_json::json!({ "stringData": { &secret_key: token } });
    // The one step that structurally cannot precede the mint: it writes the
    // token. If it fails the install IS worse off than before the command ran,
    // and the operator has to be told that rather than left reading a bare
    // kubectl error about a Secret.
    run_step(
        &cl,
        &format!("writing {secret_name}/{secret_key}"),
        "written",
        &patch_command(&opts.common, &secret_name, &secret_key, patch),
    )
    .await
    .map_err(|err| revoked_without_install(err, &opts, &kind, &address, &secret_name))?;
    // Past the patch the new token is installed and the old one's revocation is
    // no longer a surprise -- the adapter simply has not reloaded yet. A rollout
    // failure is a restart away from resolved, so it must NOT be reported as a
    // revocation.
    for (label, ok_detail, cmd) in [
        (
            "rolling mail adapter",
            "restarted",
            rollout_restart_command(&opts.common),
        ),
        (
            "waiting for mail adapter",
            "ready",
            rollout_status_command(&opts.common),
        ),
    ] {
        run_step(&cl, label, ok_detail, &cmd)
            .await
            .map_err(|err| installed_but_not_loaded(err, &opts))?;
    }
    Ok(ChannelTokenOutput::Minted {
        agent: agent.name,
        kind,
        address,
        exp,
        expires_at,
        secret_name,
        secret_key,
    })
}

/// The mint succeeded and the Secret write did not: the adapter is still
/// holding a token this very command revoked. Name that state, because the
/// underlying `kubectl` error describes a Secret and says nothing about a
/// channel that has just stopped accepting mail.
///
fn revoked_without_install(
    err: anyhow::Error,
    opts: &ChannelTokenOpts,
    kind: &str,
    address: &str,
    secret_name: &str,
) -> anyhow::Error {
    let message = format!(
        "the channel token for {kind}:{address} was minted but could not be written to secret \
         {secret_name} in namespace {}: that mint already revoked the token the mail adapter is \
         running, so this binding refuses inbound mail until a token is installed",
        opts.common.namespace
    );
    let fix = format!(
        "grant access to secret {secret_name} (or point --release/--namespace at the install that \
         owns it) and re-run `curie cluster channel-token {}`; every re-run mints a fresh token, \
         so retrying costs nothing",
        opts.agent
    );
    wrap(err, message, fix)
}

/// The Secret holds the new token; only the restart did not complete. Milder
/// than [`revoked_without_install`] and deliberately worded so the two cannot
/// be confused: here the recovery is a restart, not another mint.
fn installed_but_not_loaded(err: anyhow::Error, opts: &ChannelTokenOpts) -> anyhow::Error {
    let message = format!(
        "the new channel token was written, but the mail adapter in namespace {} did not restart \
         onto it and is still running the token this mint revoked",
        opts.common.namespace
    );
    let fix = format!(
        "restart it with `kubectl -n {} rollout restart deployment -l {}`; the new token is \
         already installed, so do not mint another one",
        opts.common.namespace,
        adapter_selector(&opts.common.release)
    );
    wrap(err, message, fix)
}

/// Attach one `{message, fix}` pair to both operator surfaces at once, the way
/// [`Ui::failed_report`](crate::ui::Ui::failed_report) does: the explicit JSON
/// payload is what `--json` emits, and the operator context is what the
/// terminal's `Error:`/`Fix:` pair reads. Wrapping with a bare
/// [`CliError`](crate::exit::CliError) instead would reach only the JSON half,
/// and a bare operator context only the human half.
fn wrap(err: anyhow::Error, message: String, fix: String) -> anyhow::Error {
    let payload = serde_json::json!({ "error": message, "fix": fix });
    crate::exit::operator_context(
        crate::exit::with_json_payload(err, payload),
        message,
        Some(fix),
    )
}

async fn show_exp(opts: ChannelTokenOpts) -> Result<ChannelTokenOutput> {
    if opts.common.dry_run {
        return Ok(ChannelTokenOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "kubectl -n {} get --raw /api/v1/namespaces/{}/pods/<mail-adapter>:8080/proxy/statusz  \
                 (read-only: would print the installed token exp; no mint, no write)",
                opts.common.namespace, opts.common.namespace
            )],
        }));
    }
    require_on_path("kubectl")?;
    let reports = mail_channel::observe(&opts.common.namespace, &opts.common.release).await;
    let report = reports.into_iter().next().ok_or_else(|| {
        anyhow::Error::from(
            crate::exit::CliError::failure(format!(
                "mail adapter is not running in namespace {} release {}",
                opts.common.namespace, opts.common.release
            ))
            .with_fix(
                "deploy the mail adapter, or pass --namespace/--release for the install that has it",
            ),
        )
    })?;
    let token = report.channel_token.as_ref();
    let exp = token.and_then(|t| t.exp);
    let state = token
        .map(|t| token_state_name(t.state))
        .unwrap_or("unknown")
        .to_string();
    let accepted = token.is_some_and(|t| {
        matches!(
            t.state,
            TokenState::Ok | TokenState::Expiring | TokenState::Disabled
        )
    });
    Ok(ChannelTokenOutput::ShowExp {
        expires_at: exp.map(format_exp),
        exp,
        accepted,
        state,
        detail: report.detail,
    })
}

fn mint_plan(
    opts: &ChannelTokenOpts,
    kind: &str,
    address: &str,
    ttl_s: i64,
    secret_name: &str,
    secret_key: &str,
) -> Vec<String> {
    vec![
        format!(
            "POST {}/channels/token {{\"kind\":\"{kind}\",\"address\":\"{address}\",\"ttl_s\":{ttl_s}}}  \
             (would resolve agent {:?} first)",
            opts.api_url, opts.agent
        ),
        format!(
            "kubectl -n {} patch secret {secret_name} --type merge --patch-file <secret patch: {secret_key}>  \
             (or the Secret named by mailAdapter.channelTokenExistingSecret when that is set)",
            opts.common.namespace
        ),
        format!(
            "kubectl -n {} rollout restart deployment -l {}",
            opts.common.namespace,
            adapter_selector(&opts.common.release)
        ),
        format!(
            "kubectl -n {} rollout status deployment -l {} --timeout=120s",
            opts.common.namespace,
            adapter_selector(&opts.common.release)
        ),
    ]
}

fn patch_command(
    common: &CommonOpts,
    secret_name: &str,
    secret_key: &str,
    document: serde_json::Value,
) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(&common.namespace),
            plain("patch"),
            plain("secret"),
            plain(secret_name),
            plain("--type"),
            plain("merge"),
            secret_patch_file(vec![secret_key.to_string()], document),
        ],
    )
}

fn rollout_restart_command(common: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(&common.namespace),
            plain("rollout"),
            plain("restart"),
            plain("deployment"),
            plain("-l"),
            plain(adapter_selector(&common.release)),
        ],
    )
}

fn rollout_status_command(common: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(&common.namespace),
            plain("rollout"),
            plain("status"),
            plain("deployment"),
            plain("-l"),
            plain(adapter_selector(&common.release)),
            plain("--timeout=120s"),
        ],
    )
}

fn adapter_selector(release: &str) -> String {
    format!("app.kubernetes.io/instance={release},app.kubernetes.io/component=mail-adapter")
}

/// Chart-default Secret targeting. Live path uses [`live_token_secret`] so a
/// `fullnameOverride` still names the Secret the chart rendered.
pub(crate) fn resolve_token_secret(
    values: Option<&serde_json::Value>,
    release: &str,
) -> (String, String) {
    if let Some(pair) = resolve_existing_secret_ref(
        values,
        EXISTING_SECRET,
        EXISTING_SECRET_KEY,
        DEFAULT_DATA_KEY,
    ) {
        return pair;
    }
    (
        chart_fullname(release).resource("secrets"),
        DEFAULT_DATA_KEY.to_string(),
    )
}

async fn live_token_secret(
    common: &CommonOpts,
    values: Option<&serde_json::Value>,
) -> (String, String) {
    if let Some(pair) = resolve_existing_secret_ref(
        values,
        EXISTING_SECRET,
        EXISTING_SECRET_KEY,
        DEFAULT_DATA_KEY,
    ) {
        return pair;
    }
    let fullname = release_fullname(&common.namespace, &common.release).await;
    (fullname.resource("secrets"), DEFAULT_DATA_KEY.to_string())
}

/// Parse `--ttl`: a positive integer number of seconds, optionally with a
/// `s`/`m`/`h`/`d` suffix. Ceiling is [`MAX_TTL_S`].
pub(crate) fn parse_ttl(raw: &str) -> Result<i64> {
    let trimmed = raw.trim();
    let usage = || {
        crate::exit::usage(format!(
            "--ttl takes a duration such as 7d, 24h, 60m, or 3600 (seconds), at most {MAX_TTL_S}s"
        ))
    };
    if trimmed.is_empty() {
        return Err(usage());
    }
    let (digits, multiplier) = match trimmed.as_bytes().last().copied() {
        Some(b's' | b'S') => (&trimmed[..trimmed.len() - 1], 1_i64),
        Some(b'm' | b'M') => (&trimmed[..trimmed.len() - 1], 60),
        Some(b'h' | b'H') => (&trimmed[..trimmed.len() - 1], 3600),
        Some(b'd' | b'D') => (&trimmed[..trimmed.len() - 1], 86_400),
        Some(b'0'..=b'9') => (trimmed, 1),
        _ => return Err(usage()),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(usage());
    }
    let amount: i64 = digits.parse().map_err(|_| usage())?;
    let ttl = amount.checked_mul(multiplier).ok_or_else(usage)?;
    if ttl <= 0 || ttl > MAX_TTL_S {
        return Err(usage());
    }
    Ok(ttl)
}

/// Decode `exp` from a `chn.<payload>.<sig>` token without verifying the
/// signature. The payload is compact JSON with sorted keys; this is not a JWT.
pub(crate) fn token_exp(token: &str) -> Result<i64> {
    let mut parts = token.split('.');
    let prefix = parts.next().unwrap_or("");
    let payload = parts.next().unwrap_or("");
    let sig = parts.next().unwrap_or("");
    if prefix != "chn" || payload.is_empty() || sig.is_empty() || parts.next().is_some() {
        return Err(decode_err());
    }
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .map_err(|_| decode_err())?;
    let value: serde_json::Value = serde_json::from_slice(&raw).map_err(|_| decode_err())?;
    value
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(decode_err)
}

fn decode_err() -> anyhow::Error {
    crate::exit::CliError::failure(
        "platform returned a channel token whose payload could not be decoded",
    )
    .with_fix("retry; if it persists, the API and CLI disagree on the chn token shape")
    .into()
}

fn format_exp(exp: i64) -> String {
    time::OffsetDateTime::from_unix_timestamp(exp)
        .ok()
        .and_then(|value| {
            value
                .format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(|| exp.to_string())
}

fn token_state_name(state: TokenState) -> &'static str {
    match state {
        TokenState::Ok => "ok",
        TokenState::Expiring => "expiring",
        TokenState::Expired => "expired",
        TokenState::Rejected => "rejected",
        TokenState::Missing => "missing",
        TokenState::Invalid => "invalid",
        TokenState::Disabled => "disabled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ttl_accepts_suffixes_and_raw_seconds() {
        assert_eq!(parse_ttl("7d").unwrap(), MAX_TTL_S);
        assert_eq!(parse_ttl("24h").unwrap(), 86_400);
        assert_eq!(parse_ttl("60m").unwrap(), 3_600);
        assert_eq!(parse_ttl("3600").unwrap(), 3_600);
        assert_eq!(parse_ttl("3600s").unwrap(), 3_600);
        assert_eq!(parse_ttl("1H").unwrap(), 3_600);
    }

    #[test]
    fn parse_ttl_rejects_zero_over_ceiling_and_garbage() {
        assert!(parse_ttl("0").is_err());
        assert!(parse_ttl("8d").is_err());
        assert!(parse_ttl("604801").is_err());
        assert!(parse_ttl("").is_err());
        assert!(parse_ttl("1w").is_err());
        assert!(parse_ttl("-1").is_err());
        assert!(parse_ttl("1.5h").is_err());
    }

    fn sample_token(exp: i64) -> String {
        let payload = format!(
            r#"{{"channel_id":"11111111-1111-1111-1111-111111111111","exp":{exp},"generation":0,"scope":"channel.enqueue"}}"#
        );
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes());
        format!("chn.{b64}.TESTSIG")
    }

    #[test]
    fn token_exp_reads_the_payload_claim() {
        assert_eq!(
            token_exp(&sample_token(1_800_000_000)).unwrap(),
            1_800_000_000
        );
    }

    #[test]
    fn token_exp_rejects_malformed() {
        assert!(token_exp("not-a-token").is_err());
        assert!(token_exp("sbx.abc.def").is_err());
        assert!(token_exp("chn.not-base64.sig").is_err());
    }

    #[test]
    fn chart_secret_is_the_default_target() {
        let (name, key) = resolve_token_secret(None, "curie");
        assert_eq!(name, "curie-secrets");
        assert_eq!(key, "mailChannelToken");
        let (name, key) = resolve_token_secret(None, "acme");
        assert_eq!(name, "acme-curie-secrets");
        assert_eq!(key, "mailChannelToken");
    }

    #[test]
    fn existing_secret_wins_over_the_chart_secret() {
        let values = serde_json::json!({
            "mailAdapter": {
                "channelTokenExistingSecret": "curie-mail-credentials",
                "channelTokenExistingSecretKey": "channel-token"
            }
        });
        let (name, key) = resolve_token_secret(Some(&values), "curie");
        assert_eq!(name, "curie-mail-credentials");
        assert_eq!(key, "channel-token");
    }

    #[test]
    fn existing_secret_default_key_is_mail_channel_token() {
        let values = serde_json::json!({
            "mailAdapter": {"channelTokenExistingSecret": "ops-mail"}
        });
        let (name, key) = resolve_token_secret(Some(&values), "curie");
        assert_eq!(name, "ops-mail");
        assert_eq!(key, "mailChannelToken");
    }

    #[test]
    fn empty_existing_secret_falls_back_to_the_chart_secret() {
        let values = serde_json::json!({
            "mailAdapter": {"channelTokenExistingSecret": ""}
        });
        let (name, key) = resolve_token_secret(Some(&values), "curie");
        assert_eq!(name, "curie-secrets");
        assert_eq!(key, "mailChannelToken");
    }

    #[test]
    fn dry_run_plan_never_contains_a_token() {
        let token = sample_token(1_800_000_000);
        let opts = ChannelTokenOpts {
            common: CommonOpts {
                namespace: "mail-test".into(),
                release: "acme".into(),
                dry_run: true,
            },
            api_url: "http://127.0.0.1:9".into(),
            api_key: "k".into(),
            agent: "acme-bot".into(),
            kind: Some("email".into()),
            address: Some("ops@example.com".into()),
            ttl: "7d".into(),
            show_exp: false,
        };
        let lines = mint_plan(
            &opts,
            "email",
            "ops@example.com",
            MAX_TTL_S,
            "acme-curie-secrets",
            "mailChannelToken",
        );
        let joined = lines.join("\n");
        assert!(joined.contains("POST http://127.0.0.1:9/channels/token"));
        assert!(joined.contains("ttl_s\":604800"));
        assert!(joined.contains("--patch-file <secret patch: mailChannelToken>"));
        assert!(joined.contains("rollout restart"));
        assert!(!joined.contains(&token));
        assert!(!joined.contains("chn."));
    }

    #[test]
    fn minted_json_has_exp_and_no_token_key() {
        let output = ChannelTokenOutput::Minted {
            agent: "acme-bot".into(),
            kind: "email".into(),
            address: "ops@example.com".into(),
            exp: 1_800_000_000,
            expires_at: "2027-01-15T08:00:00Z".into(),
            secret_name: "acme-curie-secrets".into(),
            secret_key: "mailChannelToken".into(),
        };
        let value = crate::ui::CliOutput::to_json(&output);
        assert_eq!(value["exp"], 1_800_000_000);
        assert!(value.get("token").is_none());
        let dumped = value.to_string();
        assert!(!dumped.contains("chn."));
    }

    #[test]
    fn patch_display_masks_the_token() {
        let token = sample_token(1_800_000_000);
        let cmd = patch_command(
            &CommonOpts {
                namespace: "mail-test".into(),
                release: "acme".into(),
                dry_run: false,
            },
            "acme-curie-secrets",
            "mailChannelToken",
            serde_json::json!({"stringData": {"mailChannelToken": token}}),
        );
        let display = cmd.display();
        assert!(display.contains("<secret patch: mailChannelToken>"));
        assert!(!display.contains(&sample_token(1_800_000_000)));
        assert!(!display.contains("chn."));
    }
}
