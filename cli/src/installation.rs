//! `curie.yaml`: one file declares an installation (ADR-0097).
//!
//! This module is the ONLY parser for that file, mirroring the rule ADR-0089
//! set for `deploy.yaml`. The difference is which side owns it: `deploy.yaml`
//! and `connectors.yaml` describe a *bundle* and are read by the API and the
//! worker, while `curie.yaml` describes an *installation* -- something only the
//! CLI performs. It must not become a second thing the API also reads.
//!
//! Two properties are load-bearing rather than stylistic:
//!
//! - **Secret NAMES only, never values.** The file is committed. A `curie.yaml`
//!   that could carry a token would be strictly worse than the flags it
//!   replaces, so every credential field here names an environment variable or
//!   a `curie secrets` entry, and resolution happens at apply time.
//! - **Unknown keys are an error.** A config file that silently ignores a typo
//!   is a config file that lies about what it applied. `deny_unknown_fields`
//!   everywhere is the whole reason to prefer a schema over a `--set` bag.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// The parsed file. `version` is required and checked, so a future incompatible
/// schema can be rejected by an old binary rather than half-applied by it.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Installation {
    pub version: u32,
    pub install: Install,
    #[serde(default)]
    pub platform: Platform,
    #[serde(default)]
    pub credentials: Credentials,
    #[serde(default)]
    pub comms: Comms,
    /// Verbatim `helm --set key=value` escape hatch, for anything this schema
    /// does not model yet. Present deliberately: without it, adopting the file
    /// would mean giving up settings that flags can express, and nobody would
    /// adopt it.
    #[serde(default)]
    pub set: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Install {
    pub namespace: String,
    pub release: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Platform {
    /// `None` leaves the chart default alone; `Some(false)` turns the component
    /// off. Tri-state on purpose -- a plain `bool` would make "unmentioned"
    /// indistinguishable from "explicitly false" and quietly rewrite defaults.
    #[serde(default)]
    pub ui: Option<bool>,
    #[serde(default)]
    pub inference: Option<bool>,
    /// Named providers, resolved to narrow host CIDRs at install time. Named
    /// hosts rather than hand-written CIDRs because the allowlist is a security
    /// control, and a hand-copied range is how it silently goes wrong.
    #[serde(default)]
    pub egress: Vec<Egress>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Egress {
    pub host: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// NAME of the env var / secret holding the model credential.
    #[serde(default)]
    pub model: Option<String>,
    /// NAME of the env var / secret holding a GitHub token.
    #[serde(default)]
    pub github_token: Option<String>,
    /// Declared but not yet applied by `curie apply` -- see [`Installation::validate`].
    #[serde(default)]
    pub github_app: Option<GithubApp>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GithubApp {
    pub id: String,
    pub private_key: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Comms {
    #[serde(default)]
    pub slack: Option<Slack>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Slack {
    /// NAME of the env var / secret holding the `xapp-` app token.
    pub app_token: String,
    /// NAME of the env var / secret holding the `xoxb-` bot token.
    pub bot_token: String,
}

/// The only schema version this binary understands.
pub const SUPPORTED_VERSION: u32 = 1;

impl Installation {
    /// Parse and validate, naming the file in every error so a schema mistake
    /// reads like a compiler message rather than a serde dump.
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&raw).with_context(|| format!("in {}", path.display()))
    }

    pub fn parse(raw: &str) -> Result<Self> {
        let parsed: Self = serde_norway::from_str(raw)?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<()> {
        if self.version != SUPPORTED_VERSION {
            bail!(
                "unsupported version {}: this curie understands version {}. \
                 Upgrade the CLI, or pin the file to the version it was written for.",
                self.version,
                SUPPORTED_VERSION
            );
        }
        if self.install.namespace.trim().is_empty() {
            bail!("install.namespace must not be empty");
        }
        if self.install.release.trim().is_empty() {
            bail!("install.release must not be empty");
        }
        for e in &self.platform.egress {
            if e.host.trim().is_empty() {
                bail!("platform.egress[].host must not be empty");
            }
        }
        // Refuse rather than ignore. A declared-but-unapplied credential is the
        // failure this whole ADR exists to prevent: the file would say the App
        // is configured while the cluster disagreed, which is exactly the
        // half-configured state #1262 had to add a warning for.
        if self.credentials.github_app.is_some() {
            bail!(
                "credentials.github_app is not applied by `curie apply` yet. \
                 Use `curie cluster github-app` for now, and remove the key -- \
                 leaving it here would claim an identity the cluster does not have."
            );
        }
        Self::reject_secret_shaped(&self.credentials.model, "credentials.model")?;
        Self::reject_secret_shaped(&self.credentials.github_token, "credentials.github_token")?;
        if let Some(slack) = &self.comms.slack {
            Self::reject_secret_shaped(&Some(slack.app_token.clone()), "comms.slack.app_token")?;
            Self::reject_secret_shaped(&Some(slack.bot_token.clone()), "comms.slack.bot_token")?;
        }
        Ok(())
    }

    /// Catch the single most likely misuse: pasting the token instead of naming
    /// the variable that holds it. This file gets committed, so a value here is
    /// a leak, and the shapes are recognisable enough to refuse by prefix.
    ///
    /// Deliberately a prefix check, not a heuristic on entropy or length: a
    /// false positive would reject a legitimate variable name, and this is a
    /// guard rail, not the security boundary. `gitleaks` in CI remains that.
    fn reject_secret_shaped(value: &Option<String>, field: &str) -> Result<()> {
        const SECRET_PREFIXES: &[&str] = &["sk-", "xoxb-", "xapp-", "ghp_", "github_pat_"];
        let Some(v) = value else { return Ok(()) };
        let trimmed = v.trim();
        if trimmed.is_empty() {
            bail!("{field} must name a variable, not be empty");
        }
        for prefix in SECRET_PREFIXES {
            if trimmed.starts_with(prefix) {
                bail!(
                    "{field} looks like a secret VALUE (starts with `{prefix}`), not the \
                     NAME of a variable holding one. This file is committed -- put the \
                     value in the environment or `curie secrets set`, and name it here."
                );
            }
        }
        Ok(())
    }

    /// The `--set key=value` tokens this file implies, in a stable order so a
    /// plan diff is readable and a test can pin it.
    ///
    /// Platform toggles render before the `set:` escape hatch so an explicit
    /// `set:` entry wins on a later-key-wins reading, matching how a trailing
    /// `--set` beats an earlier one on the helm command line.
    pub fn helm_sets(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(ui) = self.platform.ui {
            out.push(format!("ui.deploy={ui}"));
        }
        if let Some(inference) = self.platform.inference {
            out.push(format!("inference.deploy={inference}"));
        }
        for (key, value) in &self.set {
            out.push(format!("{key}={value}"));
        }
        out
    }

    /// Provider names for `--allow-egress-host`, validated downstream by
    /// `ops::parse_egress_provider` so an unknown host is one error message,
    /// not two divergent ones.
    pub fn egress_hosts(&self) -> Vec<String> {
        self.platform
            .egress
            .iter()
            .map(|e| e.host.clone())
            .collect()
    }

    /// Every credential NAME this file references, for a single up-front
    /// resolution pass. Ordered and de-duplicated so the "missing:" list in an
    /// error reads the same way twice.
    pub fn credential_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        let mut push = |n: Option<&String>| {
            if let Some(n) = n {
                if !names.contains(n) {
                    names.push(n.clone());
                }
            }
        };
        push(self.credentials.model.as_ref());
        push(self.credentials.github_token.as_ref());
        if let Some(slack) = &self.comms.slack {
            push(Some(&slack.app_token));
            push(Some(&slack.bot_token));
        }
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> &'static str {
        "version: 1\ninstall:\n  namespace: acme\n  release: acme\n"
    }

    #[test]
    fn parses_a_minimal_file() {
        let cfg = Installation::parse(minimal()).expect("minimal file should parse");
        assert_eq!(cfg.install.namespace, "acme");
        assert_eq!(cfg.install.release, "acme");
        assert!(cfg.helm_sets().is_empty(), "no platform toggles declared");
        assert!(cfg.credential_names().is_empty());
    }

    /// The reason to have a schema at all: a typo must not be silently dropped.
    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        let cases = [
            ("version: 1\ninstall:\n  namespace: a\n  release: a\nplatfrom: {}\n", "top level"),
            ("version: 1\ninstall:\n  namespace: a\n  release: a\n  nmespace: b\n", "install"),
            (
                "version: 1\ninstall:\n  namespace: a\n  release: a\nplatform:\n  ui_deploy: false\n",
                "platform",
            ),
            (
                "version: 1\ninstall:\n  namespace: a\n  release: a\ncomms:\n  slak: {}\n",
                "comms",
            ),
        ];
        for (raw, where_) in cases {
            let err = Installation::parse(raw).expect_err(&format!("{where_}: typo must fail"));
            assert!(
                format!("{err:#}").contains("unknown field"),
                "{where_}: error should name the unknown field: {err:#}"
            );
        }
    }

    /// The file is committed. A pasted token must never survive parsing.
    #[test]
    fn a_pasted_secret_value_is_refused() {
        let cases = [
            (
                "credentials:\n  model: sk-ant-api03-realtoken\n",
                "credentials.model",
            ),
            (
                "comms:\n  slack:\n    app_token: xapp-1-A-B-c\n    bot_token: BOT\n",
                "comms.slack.app_token",
            ),
            (
                "comms:\n  slack:\n    app_token: APP\n    bot_token: xoxb-11-22-zz\n",
                "comms.slack.bot_token",
            ),
            (
                "credentials:\n  github_token: ghp_abcdefghijklmnop\n",
                "credentials.github_token",
            ),
        ];
        for (tail, field) in cases {
            let raw = format!("{}{tail}", minimal());
            let err = Installation::parse(&raw).expect_err(&format!("{field}: must refuse"));
            let msg = format!("{err:#}");
            assert!(msg.contains(field), "error should name {field}: {msg}");
            assert!(
                msg.contains("NAME of a variable"),
                "error should say what to do instead: {msg}"
            );
        }
    }

    /// A NAME that merely looks ordinary must still be accepted -- the guard
    /// must not be so eager that it blocks legitimate files.
    #[test]
    fn ordinary_variable_names_are_accepted() {
        let raw = format!(
            "{}credentials:\n  model: ANTHROPIC_API_KEY\ncomms:\n  slack:\n    \
             app_token: SLACK_APP_TOKEN\n    bot_token: SLACK_BOT_TOKEN\n",
            minimal()
        );
        let cfg = Installation::parse(&raw).expect("plain names must parse");
        assert_eq!(
            cfg.credential_names(),
            vec!["ANTHROPIC_API_KEY", "SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"]
        );
    }

    /// Declared-but-unapplied is the exact half-configured state #1262 had to
    /// add a runtime warning for. Refuse it at parse time instead.
    #[test]
    fn github_app_is_refused_rather_than_ignored() {
        let raw = format!(
            "{}credentials:\n  github_app:\n    id: GITHUB_APP_ID\n    \
             private_key: GITHUB_APP_PRIVATE_KEY\n",
            minimal()
        );
        let err = Installation::parse(&raw).expect_err("must not silently ignore");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cluster github-app"),
            "must point at the verb that does work today: {msg}"
        );
    }

    #[test]
    fn version_must_match() {
        let err = Installation::parse("version: 2\ninstall:\n  namespace: a\n  release: a\n")
            .expect_err("a future version must not be half-applied");
        assert!(format!("{err:#}").contains("unsupported version 2"));
    }

    /// Tri-state: unmentioned must not silently rewrite a chart default.
    #[test]
    fn an_unmentioned_toggle_emits_no_set() {
        let raw = format!("{}platform:\n  ui: false\n", minimal());
        let cfg = Installation::parse(&raw).unwrap();
        assert_eq!(
            cfg.helm_sets(),
            vec!["ui.deploy=false"],
            "inference was never mentioned, so it must not appear"
        );
    }

    #[test]
    fn explicit_set_entries_render_after_platform_toggles() {
        let raw = format!(
            "{}platform:\n  ui: false\nset:\n  security.gvisor.mode: \"off\"\n",
            minimal()
        );
        let cfg = Installation::parse(&raw).unwrap();
        assert_eq!(
            cfg.helm_sets(),
            vec!["ui.deploy=false", "security.gvisor.mode=off"],
            "a later --set wins on the helm command line, so escape-hatch keys go last"
        );
    }

    #[test]
    fn egress_hosts_are_named_not_cidrs() {
        let raw = format!(
            "{}platform:\n  egress:\n    - host: anthropic\n    - host: slack\n",
            minimal()
        );
        let cfg = Installation::parse(&raw).unwrap();
        assert_eq!(cfg.egress_hosts(), vec!["anthropic", "slack"]);
    }

    #[test]
    fn empty_required_scalars_are_rejected() {
        for (raw, field) in [
            ("version: 1\ninstall:\n  namespace: \"\"\n  release: a\n", "namespace"),
            ("version: 1\ninstall:\n  namespace: a\n  release: \"\"\n", "release"),
            (
                "version: 1\ninstall:\n  namespace: a\n  release: a\nplatform:\n  egress:\n    - host: \"\"\n",
                "host",
            ),
        ] {
            let err = Installation::parse(raw).expect_err(&format!("empty {field} must fail"));
            assert!(format!("{err:#}").contains(field), "should name {field}");
        }
    }

    /// The ADR's own example must parse, minus the one key it documents as
    /// deferred -- otherwise the decision and the code disagree.
    #[test]
    fn the_adr_example_parses() {
        let raw = "\
version: 1

install:
  namespace: acme-bot
  release: acme-bot

platform:
  ui: false
  inference: false
  egress:
    - host: anthropic
    - host: slack

credentials:
  model: ANTHROPIC_API_KEY

comms:
  slack:
    app_token: SLACK_APP_TOKEN
    bot_token: SLACK_BOT_TOKEN
";
        let cfg = Installation::parse(raw).expect("the ADR example must parse");
        assert_eq!(cfg.install.namespace, "acme-bot");
        assert_eq!(
            cfg.helm_sets(),
            vec!["ui.deploy=false", "inference.deploy=false"]
        );
        assert_eq!(cfg.egress_hosts(), vec!["anthropic", "slack"]);
        assert_eq!(
            cfg.credential_names(),
            vec!["ANTHROPIC_API_KEY", "SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"]
        );
    }
}

// ---------------------------------------------------------------------------
// apply
// ---------------------------------------------------------------------------

/// Resolve one credential NAME to its value: the process environment first,
/// then Curie private storage. Mirrors `commands::secret_store_env`'s order --
/// shell env beats the vault -- so `curie apply` and `curie skill up` disagree
/// about nothing.
fn resolve_credential(name: &str) -> Result<Option<String>> {
    if let Ok(value) = std::env::var(name) {
        if !value.is_empty() {
            return Ok(Some(value));
        }
    }
    if crate::secrets::is_saved(name)? {
        return crate::secrets::get_value(name);
    }
    Ok(None)
}

/// Every name this file references, resolved in one pass.
///
/// Reports **all** missing names at once rather than failing on the first. An
/// operator setting up a new install is typically missing several, and a
/// one-at-a-time gauntlet is the difference between one round trip and four.
pub fn resolve_credentials(
    cfg: &Installation,
    resolver: &dyn Fn(&str) -> Result<Option<String>>,
) -> Result<BTreeMap<String, String>> {
    let mut resolved = BTreeMap::new();
    let mut missing = Vec::new();
    for name in cfg.credential_names() {
        match resolver(&name)? {
            Some(value) => {
                resolved.insert(name, value);
            }
            None => missing.push(name),
        }
    }
    if !missing.is_empty() {
        bail!(
            "curie.yaml names credential(s) with no value available: {}. \
             Export each in the environment, or save it with `curie secrets set <NAME>`. \
             The file names them; it never carries their values.",
            missing.join(", ")
        );
    }
    Ok(resolved)
}

/// What `curie apply` did, as one object (the `--json` contract allows exactly
/// one). `apply` drives two underlying verbs, so their outputs are composed
/// here rather than each emitting its own.
#[derive(Debug)]
pub enum ApplyOutput {
    DryRun(crate::ui::DryRunPlan),
    Applied {
        namespace: String,
        release: String,
        comms: bool,
    },
}

impl crate::ui::CliOutput for ApplyOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ApplyOutput::DryRun(plan) => {
                <crate::ui::DryRunPlan as crate::ui::CliOutput>::to_json(plan)
            }
            ApplyOutput::Applied {
                namespace,
                release,
                comms,
            } => serde_json::json!({
                "applied": true,
                "namespace": namespace,
                "release": release,
                "comms": comms,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ApplyOutput::DryRun(plan) => {
                <crate::ui::DryRunPlan as crate::ui::CliOutput>::render(plan, ui)
            }
            ApplyOutput::Applied {
                namespace,
                release,
                comms,
            } => {
                ui.payload_plain(&format!("applied {release} in {namespace}"));
                if *comms {
                    ui.payload_plain("slack comms configured");
                }
            }
        }
    }
}

pub struct ApplyOpts {
    pub local: LocalInstallationPlan,
    pub chart: String,
    /// Proceed even when the upgrade would delete a stateful component. The
    /// operator asserting the data is migrated, or expendable.
    pub allow_stateful_removal: bool,
}

pub struct LocalInstallationPlan {
    cfg: Installation,
    resolved: BTreeMap<String, String>,
    up: crate::ops::UpOpts,
    github_token: Option<String>,
}

struct EffectiveInstallationPlan {
    cfg: Installation,
    up: crate::ops::UpOpts,
    up_values: crate::ops::UpValuePlan,
    github_token: Option<String>,
    comms: Option<crate::comms::CommsOpts>,
    live: Option<serde_json::Value>,
    desired: BTreeMap<String, String>,
    preserves_undeclared_github_token: bool,
}

pub fn plan_installation(cfg: Installation, dry_run: bool) -> Result<LocalInstallationPlan> {
    let resolved = resolve_credentials(&cfg, &resolve_credential)?;
    let github_token = cfg
        .credentials
        .github_token
        .as_ref()
        .and_then(|name| resolved.get(name).cloned());
    let up = crate::ops::UpOpts {
        common: crate::ops::CommonOpts {
            namespace: cfg.install.namespace.clone(),
            release: cfg.install.release.clone(),
            dry_run,
        },
        chart: String::new(),
        no_expose: false,
        set: cfg.helm_sets(),
        allow_egress_host: cfg.egress_hosts(),
        resolved_egress_cidrs: vec![],
        allow_web_egress: vec![],
        fake_model: cfg.credentials.model.is_none(),
        credentials: cfg
            .credentials
            .model
            .as_ref()
            .and_then(|name| resolved.get(name).cloned()),
        local_model: None,
        model: std::env::var("CURIE_MODEL").ok().filter(|s| !s.is_empty()),
        secrets: vec![],
        github_token: crate::ops::GithubTokenPlan::Untouched,
        dev: false,
    };
    crate::ops::validate_up_inputs(&up, github_token.as_deref(), false)?;
    Ok(LocalInstallationPlan {
        cfg,
        resolved,
        up,
        github_token,
    })
}

async fn complete_installation_plan(
    local: LocalInstallationPlan,
) -> Result<EffectiveInstallationPlan> {
    let LocalInstallationPlan {
        cfg,
        resolved,
        up,
        github_token,
    } = local;
    // Apply in dry run mode remains a local preview: no Helm read and no provider
    // DNS resolution. `diff` builds a live plan, so it still completes the
    // desired values against the live release.
    let complete_live_state = !up.common.dry_run;
    let live = if complete_live_state {
        crate::ops::fetch_release_values(&up.common).await?
    } else {
        None
    };
    let preserves_undeclared_github_token = cfg.credentials.github_token.is_none()
        && !cfg.set.contains_key(crate::ops::GITHUB_TOKEN_KEY)
        && live
            .as_ref()
            .and_then(|values| values.get("api"))
            .and_then(|api| api.get("githubToken"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|token| !token.is_empty());
    let up = crate::ops::complete_up_opts(
        up,
        live.as_ref(),
        github_token.as_deref(),
        false,
        complete_live_state,
    )?;
    let up_values = crate::ops::up_value_plan(&up);
    let mut desired = up_values.effective_values();
    let comms = cfg
        .comms
        .slack
        .as_ref()
        .map(|slack| crate::comms::CommsOpts {
            common: up.common.clone(),
            chart: up.chart.clone(),
            app_token: resolved.get(&slack.app_token).cloned().unwrap_or_default(),
            bot_token: resolved.get(&slack.bot_token).cloned().unwrap_or_default(),
            disconnect: false,
        });
    if let Some(comms) = &comms {
        desired.insert(
            "dispatcher.slack.appToken".to_string(),
            comms.app_token.clone(),
        );
        desired.insert(
            "dispatcher.slack.botToken".to_string(),
            comms.bot_token.clone(),
        );
        desired.insert("worker.slackApiBaseUrl".to_string(), String::new());
    }
    Ok(EffectiveInstallationPlan {
        cfg,
        up,
        up_values,
        github_token,
        comms,
        live,
        desired,
        preserves_undeclared_github_token,
    })
}

/// Refuse an apply that would delete a stateful component the release runs.
///
/// Runs even under `--dry-run`: the plan a dry run prints is exactly the plan
/// that would destroy the store, so an operator reading it deserves the same
/// warning the real run would give.
async fn guard_stateful_removal(up: &crate::ops::UpOpts) -> Result<()> {
    let live = crate::ops::live_stateful_components(&up.common).await?;
    if live.is_empty() {
        // Fresh install, or no cluster to read. Nothing to lose either way.
        return Ok(());
    }
    // The same effective values the upgrade would send, so the render reflects
    // what this apply would actually create rather than the chart's defaults.
    let sets: Vec<String> = crate::ops::up_value_plan(up)
        .effective_values()
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    let rendered = crate::ops::chart_stateful_components(&up.chart, &up.common, &sets).await?;
    let removed = crate::ops::removed_stateful_components(&live, &rendered);
    if removed.is_empty() {
        return Ok(());
    }
    bail!(
        "refusing to apply: this would DELETE {} stateful component(s) the release is \
         running, and the persistent data with them:\n  {}\n\n\
         The target chart does not render them, which usually means a chart version \
         renamed or removed the component. Migrate the data first -- for the bundle \
         store, every sandbox reads from it at start, so losing it breaks the next turn \
         and not merely a rollback.\n\n\
         Re-run with --allow-stateful-removal once the data is migrated or you accept \
         losing it.",
        removed.len(),
        removed.join("\n  "),
    )
}

/// Converge the cluster to the file.
///
/// The ordering -- platform install, THEN comms -- is handled here rather than
/// asked of the operator. That ordering is load-bearing (a `cluster up` has
/// historically dropped what `comms` configured, #1256) and until now lived
/// only as a sentence in a runbook, which is exactly what ADR-0097 set out to
/// fix: the interface could not express it, so prose had to.
pub async fn apply(opts: ApplyOpts) -> Result<ApplyOutput> {
    let ApplyOpts {
        mut local,
        chart,
        allow_stateful_removal,
    } = opts;
    local.up.chart = chart;
    let dry_run = local.up.common.dry_run;
    let plan = complete_installation_plan(local).await?;
    let EffectiveInstallationPlan {
        cfg,
        up,
        up_values,
        github_token,
        comms,
        live,
        ..
    } = plan;

    // Refuse before the first mutation, not after.
    //
    // `up` does a FULL upgrade, so a component the target chart no longer
    // renders is DELETED -- and for a StatefulSet that is the data with it.
    // This is not hypothetical: chart 0.6.0 renamed the object store from
    // `minio` to `rustfs`, and applying it to a 0.5.1 release would remove the
    // store every sandbox's bundle-fetch init container reads from. The next
    // Slack message would fail, not merely a rollback.
    //
    // `curie diff` learned to warn about the chart mismatch; `apply` had no
    // guard at all and would have gone ahead silently.
    if !allow_stateful_removal {
        guard_stateful_removal(&up).await?;
    }
    let up_out = crate::ops::up_prepared(up, up_values, live, github_token).await?;

    let mut lines = match up_out {
        crate::ops::ClusterUpOutput::DryRun(plan) => plan.lines,
        crate::ops::ClusterUpOutput::Up { .. } => vec![],
    };

    let mut configured_comms = false;
    if let Some(comms) = comms {
        let comms_out = crate::comms::comms(comms).await?;
        configured_comms = true;
        if let crate::comms::CommsOutput::DryRun(plan) = comms_out {
            lines.extend(plan.lines);
        }
    }

    if dry_run {
        return Ok(ApplyOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    Ok(ApplyOutput::Applied {
        namespace: cfg.install.namespace,
        release: cfg.install.release,
        comms: configured_comms,
    })
}

// -- curie diff ---------------------------------------------------------------

/// How one chart value relates the file to the live release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffKind {
    /// The file declares it; the release has no record of it.
    Add,
    /// Both have it, with different values.
    Change,
    /// Both agree. Reported so `diff` can show the whole intent, not just deltas.
    Same,
    /// Only the release has it, and a plain `up` carries it forward untouched.
    /// NOT a removal -- see [`crate::ops::is_preserved_by_up`].
    Preserved,
    /// Only the release has it, and `apply` would reset it to the chart default.
    Reset,
}

impl DiffKind {
    /// The leading glyph. `~`/`+` are diff conventions; `!` marks the one kind
    /// that loses configuration, so it does not read as ordinary noise.
    pub fn marker(self) -> char {
        match self {
            DiffKind::Add => '+',
            DiffKind::Change => '~',
            DiffKind::Same => '=',
            DiffKind::Preserved => '=',
            DiffKind::Reset => '!',
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            DiffKind::Add => "add",
            DiffKind::Change => "change",
            DiffKind::Same => "unchanged",
            DiffKind::Preserved => "preserved",
            DiffKind::Reset => "reset to chart default",
        }
    }

    /// Would applying this file change the cluster?
    pub fn is_change(self) -> bool {
        matches!(self, DiffKind::Add | DiffKind::Change | DiffKind::Reset)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub key: String,
    pub kind: DiffKind,
    /// The live value, already masked when the key carries a secret.
    pub from: Option<String>,
    /// The declared value, already masked when the key carries a secret.
    pub to: Option<String>,
}

/// Flatten `helm get values -o json` into the dotted keys `--set` speaks.
///
/// Helm returns nested objects; the file and `up` both express values as dotted
/// paths. Comparing the two shapes directly would report every key as missing.
///
/// Arrays are rendered with helm's own `key[i]` indexing rather than descended
/// into as objects, so a declared `security.networkPolicy.allowedEgress[0].cidr`
/// lines up with what a prior `--set` recorded.
pub fn flatten_values(value: &serde_json::Value, prefix: &str, out: &mut BTreeMap<String, String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_values(v, &key, out);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                flatten_values(v, &format!("{prefix}[{i}]"), out);
            }
        }
        serde_json::Value::Null => {}
        other => {
            let rendered = match other {
                serde_json::Value::String(s) => s.clone(),
                v => v.to_string(),
            };
            out.insert(prefix.to_string(), rendered);
        }
    }
}

/// What a value may be shown as. A secret is never rendered, not even partially:
/// this output goes to a terminal, a log, and a `--json` consumer.
fn display_value(key: &str, value: &str) -> String {
    if crate::ops::is_secret_value_key(key) {
        "<secret>".to_string()
    } else {
        value.to_string()
    }
}

/// Compare what the file declares against what the release records.
///
/// Pure: no cluster, no helm, no clock. The caller supplies both sides.
///
/// The `Preserved` classification is the point of the whole function. A cluster
/// stood up by flags carries Slack tokens, a GitHub App, and generated store
/// passwords that `curie.yaml` does not mention -- and `up` re-supplies every
/// one of them. Calling those removals would make `diff` lie in the one
/// situation it exists for: the operator deciding whether it is safe to adopt
/// the file at all.
pub fn diff_plan(
    declared: &BTreeMap<String, String>,
    live: Option<&serde_json::Value>,
) -> Vec<DiffEntry> {
    let mut current = BTreeMap::new();
    if let Some(values) = live {
        flatten_values(values, "", &mut current);
    }

    let mut entries: Vec<DiffEntry> = Vec::new();

    for (key, want) in declared {
        let kind = match current.get(key) {
            None => DiffKind::Add,
            Some(have) if have == want => DiffKind::Same,
            Some(_) => DiffKind::Change,
        };
        entries.push(DiffEntry {
            key: key.clone(),
            kind,
            from: current.get(key).map(|v| display_value(key, v)),
            to: Some(display_value(key, want)),
        });
    }

    for (key, have) in &current {
        if declared.contains_key(key) {
            continue;
        }
        let kind = if crate::ops::is_preserved_by_up(key) {
            DiffKind::Preserved
        } else {
            DiffKind::Reset
        };
        entries.push(DiffEntry {
            key: key.clone(),
            kind,
            from: Some(display_value(key, have)),
            to: None,
        });
    }

    entries.sort_by(|a, b| a.key.cmp(&b.key));
    entries
}

/// What `curie diff` found.
#[derive(Debug)]
pub struct DiffOutput {
    pub namespace: String,
    pub release: String,
    /// `false` when helm has no record of the release: everything is a create.
    pub release_exists: bool,
    /// The chart the release was installed with (`curie-0.5.1`), if readable.
    pub chart_deployed: Option<String>,
    /// The chart version this CLI would apply.
    pub chart_target: String,
    pub entries: Vec<DiffEntry>,
}

impl DiffOutput {
    pub fn changes(&self) -> usize {
        self.entries.iter().filter(|e| e.kind.is_change()).count()
    }

    /// The deployed chart's version, stripped of the `curie-` name prefix.
    fn deployed_version(&self) -> Option<&str> {
        self.chart_deployed
            .as_deref()
            .map(|c| c.rsplit_once('-').map(|(_, v)| v).unwrap_or(c))
    }

    /// Would `apply` change the chart under these values, not just the values?
    ///
    /// A value-level diff cannot see a component being added, removed, or
    /// renamed between chart versions -- and when that happens its output is
    /// not merely incomplete but misleading, since a renamed component's old
    /// keys render as ordinary resets.
    pub fn chart_version_differs(&self) -> bool {
        match self.deployed_version() {
            Some(deployed) => deployed != self.chart_target,
            None => false,
        }
    }
}

impl crate::ui::CliOutput for DiffOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "namespace": self.namespace,
            "release": self.release,
            "release_exists": self.release_exists,
            "chart_deployed": self.chart_deployed,
            "chart_target": self.chart_target,
            "chart_version_differs": self.chart_version_differs(),
            "changes": self.changes(),
            "entries": self.entries.iter().map(|e| serde_json::json!({
                "key": e.key,
                "kind": e.kind.label(),
                "from": e.from,
                "to": e.to,
            })).collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if !self.release_exists {
            ui.payload_plain(&format!(
                "release '{}' does not exist in namespace '{}' -- every value below would be created",
                self.release, self.namespace
            ));
        }
        for e in &self.entries {
            let line = match (&e.from, &e.to) {
                (Some(from), Some(to)) if e.kind == DiffKind::Change => {
                    format!("{} {}: {} -> {}", e.kind.marker(), e.key, from, to)
                }
                (_, Some(to)) if e.kind == DiffKind::Add => {
                    format!("{} {}: {}", e.kind.marker(), e.key, to)
                }
                (Some(from), None) => {
                    format!(
                        "{} {}: {} ({})",
                        e.kind.marker(),
                        e.key,
                        from,
                        e.kind.label()
                    )
                }
                (_, Some(to)) => format!("{} {}: {}", e.kind.marker(), e.key, to),
                _ => format!("{} {}", e.kind.marker(), e.key),
            };
            ui.payload_plain(&line);
        }
        let changes = self.changes();
        if changes == 0 {
            ui.payload_plain("no changes: the cluster already matches this file");
        } else {
            ui.payload_plain(&format!("{changes} change(s) would be applied"));
        }
        if self.entries.iter().any(|e| e.kind == DiffKind::Reset) {
            ui.note(
                "`!` marks a value the release carries that this file does not declare. \
                 `curie apply` does a full upgrade, so it would go back to the chart \
                 default. Declare it in curie.yaml to keep it.",
            );
        }
        // Last, so it is the line left on screen. This diff is value-level and
        // says nothing about components a chart bump adds, removes, or renames
        // -- and a renamed component's old keys appear above as ordinary
        // resets, which reads far milder than the swap it would actually be.
        if self.chart_version_differs() {
            ui.note(&format!(
                "CHART VERSION MISMATCH: the release runs {} but this curie applies {}. \
                 The comparison above is values-only -- it cannot see a component added, \
                 removed, or renamed between those versions, and a renamed one shows up \
                 as an ordinary reset. Do not read this as a safe apply. Reconcile the \
                 chart version first, or apply with the matching chart.",
                self.chart_deployed.as_deref().unwrap_or("unknown"),
                self.chart_target,
            ));
        }
    }
}

pub struct DiffOpts {
    pub local: LocalInstallationPlan,
}

/// Compare the file against the live release.
///
/// Resolves the same local inputs as apply, then performs one values read and
/// provider resolution to complete the desired plan without mutating it.
pub async fn diff(opts: DiffOpts) -> Result<DiffOutput> {
    let plan = complete_installation_plan(opts.local).await?;
    // A second, independent read: the values plan says nothing about WHICH
    // chart consumes them, and a component renamed between chart versions
    // shows up in the entries below as an ordinary reset.
    let chart_deployed = crate::ops::fetch_release_chart(&plan.up.common).await?;
    let mut entries = diff_plan(&plan.desired, plan.live.as_ref());
    if plan.preserves_undeclared_github_token {
        if let Some(entry) = entries
            .iter_mut()
            .find(|entry| entry.key == crate::ops::GITHUB_TOKEN_KEY)
        {
            entry.kind = DiffKind::Preserved;
            entry.to = None;
        }
    }
    Ok(DiffOutput {
        namespace: plan.cfg.install.namespace,
        release: plan.cfg.install.release,
        release_exists: plan.live.is_some(),
        chart_deployed,
        chart_target: crate::artifacts::version().to_string(),
        entries,
    })
}

#[cfg(test)]
mod diff_tests {
    use super::*;

    fn live(json: serde_json::Value) -> serde_json::Value {
        json
    }

    /// The classification tests below run against the REAL key set of the
    /// adopting agent's release (`helm get values`, key names only -- no values were
    /// read). A fixture I invented would have agreed with whatever I wrote;
    /// this one disagreed, and that is how the `Managed` bug was found.
    const LIVE_KEYS: &[&str] = &[
        "agentSandbox.runner.credentials",
        "agentSandbox.runner.fakeModel",
        "agentSandbox.runner.tag",
        "api.apiKey",
        "api.githubAppExistingSecret",
        "api.githubAppExistingSecretKey",
        "api.githubAppId",
        "api.githubAppPrivateKey",
        "api.githubCloneBase",
        "api.githubWebhookSecret",
        "api.image.tag",
        "clickhouse.auth.password",
        "dispatcher.slack.appToken",
        "dispatcher.slack.botToken",
        "inference.deploy",
        "langfuse.encryptionKey",
        "langfuse.nextauthSecret",
        "langfuse.salt",
        "nameOverride",
        "postgres.auth.password",
        "security.gvisor.mode",
        "security.networkPolicy.allowedEgress[0].cidr",
        "security.networkPolicy.allowedEgress[0].ports[0].port",
        "security.networkPolicy.allowedEgress[0].ports[0].protocol",
        "ui.deploy",
        "valkey.password",
        "worker.slackApiBaseUrl",
    ];

    /// Rebuild the nested shape `helm get values -o json` returns from those
    /// flat keys, so the test exercises `flatten_values` too.
    fn live_release() -> serde_json::Value {
        // Recursive rather than a loop with a cursor: re-seating a `&mut` into a
        // child it was just borrowed from is what the borrow checker refuses.
        fn insert(node: &mut serde_json::Value, parts: &[&str]) {
            let (head, rest) = parts.split_first().expect("non-empty path");
            let indexed = head
                .split_once('[')
                .map(|(name, r)| (name, r.trim_end_matches(']').parse::<usize>().unwrap()));
            match indexed {
                Some((name, idx)) => {
                    let arr = node
                        .as_object_mut()
                        .unwrap()
                        .entry(name.to_string())
                        .or_insert_with(|| serde_json::json!([]));
                    let items = arr.as_array_mut().unwrap();
                    while items.len() <= idx {
                        items.push(serde_json::json!({}));
                    }
                    if rest.is_empty() {
                        items[idx] = serde_json::json!("LIVE");
                    } else {
                        insert(&mut items[idx], rest);
                    }
                }
                None if rest.is_empty() => {
                    node.as_object_mut()
                        .unwrap()
                        .insert((*head).to_string(), serde_json::json!("LIVE"));
                }
                None => {
                    let child = node
                        .as_object_mut()
                        .unwrap()
                        .entry((*head).to_string())
                        .or_insert_with(|| serde_json::json!({}));
                    insert(child, rest);
                }
            }
        }

        let mut root = serde_json::json!({});
        for key in LIVE_KEYS {
            let parts: Vec<&str> = key.split('.').collect();
            insert(&mut root, &parts);
        }
        root
    }

    fn plan_against_live(desired: &BTreeMap<String, String>) -> Vec<DiffEntry> {
        diff_plan(desired, Some(&live_release()))
    }

    fn kind_of<'a>(entries: &'a [DiffEntry], key: &str) -> &'a DiffKind {
        &entries
            .iter()
            .find(|e| e.key == key)
            .unwrap_or_else(|| panic!("{key} missing from the plan"))
            .kind
    }

    /// Values from the shared effective plan carry their literal desired value
    /// into the diff rather than taking a separate classification path.
    #[test]
    fn shared_effective_values_are_reported_as_literal_changes() {
        let desired = BTreeMap::from([
            (
                "agentSandbox.runner.credentials".to_string(),
                "resolved-model-credential".to_string(),
            ),
            (
                "agentSandbox.runner.fakeModel".to_string(),
                "false".to_string(),
            ),
            (
                "security.networkPolicy.allowedEgress[0].cidr".to_string(),
                "203.0.113.10/32".to_string(),
            ),
            (
                "security.networkPolicy.allowedEgress[0].ports[0].port".to_string(),
                "443".to_string(),
            ),
            (
                "security.networkPolicy.allowedEgress[0].ports[0].protocol".to_string(),
                "TCP".to_string(),
            ),
        ]);
        let entries = plan_against_live(&desired);
        for (key, value) in desired {
            let entry = entries.iter().find(|entry| entry.key == key).unwrap();
            assert_eq!(entry.kind, DiffKind::Change, "{key}");
            assert_eq!(
                entry.to.as_deref(),
                Some(display_value(&key, &value).as_str())
            );
        }
    }

    /// A file naming NO model credential really does drop those two, and
    /// claiming otherwise would be the same lie inverted.
    #[test]
    fn without_a_declared_model_credential_those_keys_are_resets() {
        let entries = plan_against_live(&BTreeMap::new());
        for key in [
            "agentSandbox.runner.credentials",
            "agentSandbox.runner.fakeModel",
        ] {
            assert_eq!(kind_of(&entries, key), &DiffKind::Reset, "{key}");
        }
    }

    /// Every credential-bearing key on the real release must be preserved, and
    /// none may print. This is the whole "is it safe to adopt the file" answer.
    #[test]
    fn every_live_secret_is_preserved_and_masked() {
        let entries = plan_against_live(&BTreeMap::new());
        for key in [
            "api.apiKey",
            "api.githubAppId",
            "api.githubAppPrivateKey",
            "api.githubWebhookSecret",
            "clickhouse.auth.password",
            "dispatcher.slack.appToken",
            "dispatcher.slack.botToken",
            "langfuse.encryptionKey",
            "langfuse.nextauthSecret",
            "langfuse.salt",
            "postgres.auth.password",
            "valkey.password",
        ] {
            let entry = entries.iter().find(|e| e.key == key).expect(key);
            assert_eq!(entry.kind, DiffKind::Preserved, "{key} must survive apply");
            assert_eq!(entry.from.as_deref(), Some("<secret>"), "{key} must mask");
        }
    }

    #[test]
    fn nested_values_flatten_to_the_dotted_keys_set_speaks() {
        let mut out = BTreeMap::new();
        flatten_values(
            &serde_json::json!({"ui": {"deploy": false}, "api": {"apiKey": "x"}}),
            "",
            &mut out,
        );
        assert_eq!(out.get("ui.deploy").map(String::as_str), Some("false"));
        assert_eq!(out.get("api.apiKey").map(String::as_str), Some("x"));
    }

    /// Helm indexes arrays; descending into them as objects would misalign every
    /// declared `allowedEgress[0].cidr` against what a prior --set recorded.
    #[test]
    fn arrays_flatten_with_helm_index_syntax() {
        let mut out = BTreeMap::new();
        flatten_values(
            &serde_json::json!({"security": {"networkPolicy": {"allowedEgress": [{"cidr": "10.0.0.0/8"}]}}}),
            "",
            &mut out,
        );
        assert_eq!(
            out.get("security.networkPolicy.allowedEgress[0].cidr")
                .map(String::as_str),
            Some("10.0.0.0/8")
        );
    }

    #[test]
    fn a_declared_key_the_release_lacks_is_an_add() {
        let declared = BTreeMap::from([("ui.deploy".to_string(), "false".to_string())]);
        let entries = diff_plan(&declared, Some(&live(serde_json::json!({}))));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind, DiffKind::Add);
    }

    #[test]
    fn matching_values_are_unchanged_and_count_as_no_change() {
        let declared = BTreeMap::from([("ui.deploy".to_string(), "false".to_string())]);
        let entries = diff_plan(
            &declared,
            Some(&live(serde_json::json!({"ui": {"deploy": false}}))),
        );
        assert_eq!(entries[0].kind, DiffKind::Same);
        assert!(!entries[0].kind.is_change());
    }

    #[test]
    fn a_differing_value_is_a_change_and_shows_both_sides() {
        let declared = BTreeMap::from([("ui.deploy".to_string(), "false".to_string())]);
        let entries = diff_plan(
            &declared,
            Some(&live(serde_json::json!({"ui": {"deploy": true}}))),
        );
        assert_eq!(entries[0].kind, DiffKind::Change);
        assert_eq!(entries[0].from.as_deref(), Some("true"));
        assert_eq!(entries[0].to.as_deref(), Some("false"));
    }

    /// The honesty requirement ADR-0097 named: a cluster stood up by flags
    /// carries tokens and generated passwords the file never mentions, and `up`
    /// re-supplies every one. Calling them removals would be a lie in exactly
    /// the situation diff exists for.
    #[test]
    fn values_up_carries_forward_are_preserved_never_removals() {
        let declared = BTreeMap::new();
        let entries = diff_plan(
            &declared,
            Some(&live(serde_json::json!({
                "dispatcher": {"slack": {"appToken": "xapp-x", "botToken": "xoxb-y"}},
                "api": {"githubAppId": "000000", "apiKey": "generated"},
                "postgres": {"auth": {"password": "generated"}},
            }))),
        );
        assert!(
            !entries.is_empty(),
            "the fixture declares several preserved keys"
        );
        for e in &entries {
            assert_eq!(
                e.kind,
                DiffKind::Preserved,
                "{} must not be reported as lost",
                e.key
            );
            assert!(!e.kind.is_change(), "{} must not count as a change", e.key);
        }
    }

    /// The other half: an undeclared key that `up` does NOT carry forward really
    /// would be reset, and staying quiet about it would be the same lie inverted.
    #[test]
    fn an_undeclared_unpreserved_value_is_reported_as_a_reset() {
        let declared = BTreeMap::new();
        let entries = diff_plan(
            &declared,
            Some(&live(serde_json::json!({"ui": {"deploy": false}}))),
        );
        assert_eq!(entries[0].kind, DiffKind::Reset);
        assert!(entries[0].kind.is_change(), "a reset is a real change");
    }

    /// `helm get values` returns real passwords. None may reach the output.
    #[test]
    fn secret_values_are_never_rendered() {
        let declared = BTreeMap::from([(
            "api.githubToken".to_string(),
            "ghp_declared_secret".to_string(),
        )]);
        let entries = diff_plan(
            &declared,
            Some(&live(serde_json::json!({
                "api": {"githubToken": "ghp_live_secret", "apiKey": "live_api_key"},
                "dispatcher": {"slack": {"botToken": "xoxb-live"}},
                "postgres": {"auth": {"password": "live_pg_password"}},
                "agentSandbox": {"runner": {"credentials": "sk-ant-live"}},
            }))),
        );
        let rendered = format!("{entries:?}");
        for leaked in [
            "ghp_declared_secret",
            "ghp_live_secret",
            "live_api_key",
            "xoxb-live",
            "live_pg_password",
            "sk-ant-live",
        ] {
            assert!(
                !rendered.contains(leaked),
                "{leaked} must never appear in diff output: {rendered}"
            );
        }
        assert!(rendered.contains("<secret>"), "must mask, not omit");
    }

    /// The leak, as it actually happened. `curie diff` against a live release
    /// printed `minio.auth.rootPassword` in full: the chart had renamed that
    /// store to `rustfs`, so the live key matched no managed list. The store
    /// was still running.
    ///
    /// The value is a PLACEHOLDER of the same shape as a generated secret. It
    /// must never be a real one: this repository is public, and a fixture is
    /// exactly where a live credential gets committed by accident (it did --
    /// see AGENTS.md on placeholder values).
    #[test]
    fn a_credential_key_no_managed_list_knows_is_still_masked() {
        let leaked = "000000000000000000000000000000000000000000000000";
        let entries = diff_plan(
            &BTreeMap::new(),
            Some(&live(serde_json::json!({
                "minio": {"auth": {"rootPassword": leaked}},
            }))),
        );
        let rendered = format!("{entries:?}");
        assert!(
            !rendered.contains(leaked),
            "a renamed chart's credential key must still mask: {rendered}"
        );
        assert!(rendered.contains("<secret>"), "{rendered}");
    }

    /// The class, not just the one instance: any key naming itself a credential
    /// masks, whether or not this chart version manages it.
    #[test]
    fn credential_shaped_key_names_mask_by_name_alone() {
        for key in [
            "minio.auth.rootPassword",
            "somevendor.apiToken",
            "legacy.encryptionKey",
            "custom.thing.secret",
            "old.store.passwd",
            "whatever.salt",
        ] {
            assert!(
                crate::ops::is_secret_value_key(key),
                "{key} names a credential and must mask"
            );
        }
    }

    /// Over-masking is safe but not free: if everything masks, diff is useless.
    #[test]
    fn ordinary_keys_still_show_their_values() {
        for key in [
            "ui.deploy",
            "api.image.tag",
            "security.gvisor.mode",
            "priorityClasses.platform.name",
            "worker.connectorReconciler.intervalSeconds",
        ] {
            assert!(
                !crate::ops::is_secret_value_key(key),
                "{key} is not a credential and must stay readable"
            );
        }
    }

    /// A value-level diff cannot see a component renamed between chart
    /// versions, and on the real cluster it rendered exactly that as a set of
    /// ordinary resets. It has to say so.
    #[test]
    fn a_chart_version_mismatch_is_reported() {
        let out = DiffOutput {
            namespace: "acme-bot".into(),
            release: "acme-bot".into(),
            release_exists: true,
            chart_deployed: Some("curie-0.5.1".into()),
            chart_target: "0.6.0".into(),
            entries: vec![],
        };
        assert!(out.chart_version_differs());
        let json = <DiffOutput as crate::ui::CliOutput>::to_json(&out);
        assert_eq!(json["chart_version_differs"], serde_json::json!(true));
        assert_eq!(json["chart_deployed"], serde_json::json!("curie-0.5.1"));
    }

    /// The matching case must stay quiet, or the warning becomes background
    /// noise that gets ignored on the run that matters.
    #[test]
    fn a_matching_chart_version_does_not_warn() {
        let out = DiffOutput {
            namespace: "acme-bot".into(),
            release: "acme-bot".into(),
            release_exists: true,
            chart_deployed: Some("curie-0.6.0".into()),
            chart_target: "0.6.0".into(),
            entries: vec![],
        };
        assert!(!out.chart_version_differs());
    }

    /// An unreadable chart version must not fabricate a mismatch.
    #[test]
    fn an_unknown_deployed_chart_does_not_claim_a_mismatch() {
        let out = DiffOutput {
            namespace: "acme-bot".into(),
            release: "acme-bot".into(),
            release_exists: false,
            chart_deployed: None,
            chart_target: "0.6.0".into(),
            entries: vec![],
        };
        assert!(!out.chart_version_differs());
    }

    /// A non-secret value must still be shown, or the mask is useless noise.
    #[test]
    fn ordinary_values_are_shown_in_full() {
        let declared = BTreeMap::from([("ui.deploy".to_string(), "false".to_string())]);
        let entries = diff_plan(&declared, Some(&live(serde_json::json!({}))));
        assert_eq!(entries[0].to.as_deref(), Some("false"));
    }

    #[test]
    fn a_missing_release_makes_every_declared_value_an_add() {
        let declared = BTreeMap::from([
            ("ui.deploy".to_string(), "false".to_string()),
            ("inference.deploy".to_string(), "false".to_string()),
        ]);
        let entries = diff_plan(&declared, None);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.kind == DiffKind::Add));
    }

    #[test]
    fn output_is_one_json_object_with_a_change_count() {
        use crate::ui::CliOutput;
        let out = DiffOutput {
            namespace: "acme".into(),
            release: "acme".into(),
            release_exists: true,
            chart_deployed: Some("curie-0.6.0".into()),
            chart_target: "0.6.0".into(),
            entries: diff_plan(
                &BTreeMap::from([("ui.deploy".to_string(), "false".to_string())]),
                Some(&live(serde_json::json!({"ui": {"deploy": true}}))),
            ),
        };
        let json = out.to_json();
        assert_eq!(json["changes"], serde_json::json!(1));
        assert_eq!(json["release_exists"], serde_json::json!(true));
        assert_eq!(json["entries"][0]["kind"], serde_json::json!("change"));
    }
}

#[cfg(test)]
mod apply_tests {
    use super::*;

    fn cfg_with_all_names() -> Installation {
        Installation::parse(
            "version: 1\ninstall:\n  namespace: a\n  release: a\n\
             credentials:\n  model: MODEL_KEY\n\
             comms:\n  slack:\n    app_token: APP_TOK\n    bot_token: BOT_TOK\n",
        )
        .unwrap()
    }

    /// One round trip, not four. An operator standing up a new install is
    /// usually missing several at once.
    #[test]
    fn every_missing_credential_is_reported_together() {
        let cfg = cfg_with_all_names();
        let err = resolve_credentials(&cfg, &|_| Ok(None)).expect_err("must refuse");
        let msg = format!("{err:#}");
        for name in ["MODEL_KEY", "APP_TOK", "BOT_TOK"] {
            assert!(msg.contains(name), "{name} must be listed: {msg}");
        }
    }

    /// A partially-resolved set must still fail, and name only what is absent.
    #[test]
    fn a_partial_resolution_names_only_what_is_missing() {
        let cfg = cfg_with_all_names();
        let err = resolve_credentials(&cfg, &|n| {
            Ok((n == "MODEL_KEY").then(|| "value".to_string()))
        })
        .expect_err("still incomplete");
        let msg = format!("{err:#}");
        assert!(
            !msg.contains("MODEL_KEY"),
            "resolved name must not be listed: {msg}"
        );
        assert!(msg.contains("APP_TOK") && msg.contains("BOT_TOK"), "{msg}");
    }

    #[test]
    fn a_fully_resolved_file_yields_every_value() {
        let cfg = cfg_with_all_names();
        let resolved =
            resolve_credentials(&cfg, &|n| Ok(Some(format!("{n}-value")))).expect("all present");
        assert_eq!(
            resolved.get("MODEL_KEY").map(String::as_str),
            Some("MODEL_KEY-value")
        );
        assert_eq!(
            resolved.get("APP_TOK").map(String::as_str),
            Some("APP_TOK-value")
        );
        assert_eq!(
            resolved.get("BOT_TOK").map(String::as_str),
            Some("BOT_TOK-value")
        );
    }

    /// A file naming nothing must resolve to nothing rather than erroring --
    /// the sealed/fake-model install is a legitimate shape.
    #[test]
    fn a_file_naming_no_credentials_resolves_empty() {
        let cfg =
            Installation::parse("version: 1\ninstall:\n  namespace: a\n  release: a\n").unwrap();
        let resolved = resolve_credentials(&cfg, &|_| {
            panic!("resolver must not be called when nothing is named")
        })
        .expect("no names, no error");
        assert!(resolved.is_empty());
    }

    /// The JSON contract is one object per invocation (#456).
    #[test]
    fn applied_output_is_one_json_object() {
        use crate::ui::CliOutput;
        let out = ApplyOutput::Applied {
            namespace: "acme".into(),
            release: "acme".into(),
            comms: true,
        };
        let json = out.to_json();
        assert_eq!(json["applied"], serde_json::json!(true));
        assert_eq!(json["namespace"], serde_json::json!("acme"));
        assert_eq!(json["comms"], serde_json::json!(true));
    }
}
