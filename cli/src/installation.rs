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
    /// Verbatim values emitted as `helm --set-string key=value` for chart settings
    /// this schema does not model yet. Every accepted value remains a string.
    /// Values shaped like booleans or null after trimmed ASCII case normalization
    /// are refused because Helm treats every nonempty string as true in template
    /// conditions. Use a modeled `curie.yaml` field when typed behavior is required.
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
    #[serde(default)]
    pub inference_persistence: Option<bool>,
    #[serde(default)]
    pub inference_pull_model: Option<bool>,
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
        if self.platform.inference == Some(true)
            && !crate::ops::inference_asset_policy_is_safe(
                self.platform.inference_persistence,
                self.platform.inference_pull_model,
            )
        {
            return Err(crate::exit::CliError::usage(
                "`platform.inference: true` requires an explicit model-weight policy: set \
                 `inference_persistence: true` under `platform` to pull weights into persistent \
                 storage, or set `inference_pull_model: false` there when the model is already \
                 present",
            )
            .with_fix(
                "add either `platform.inference_persistence: true` or \
                 `platform.inference_pull_model: false` to curie.yaml",
            )
            .into());
        }
        for (key, value) in &self.set {
            let trimmed = value.trim();
            if ["true", "false", "null"]
                .iter()
                .any(|reserved| trimmed.eq_ignore_ascii_case(reserved))
            {
                bail!(
                    "set.{key} cannot use `{value}` because set values are always strings and \
                     Helm treats every nonempty string as true in template conditions. \
                     Use a modeled curie.yaml field for typed boolean or null behavior."
                );
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
        Self::validate_credential_name(&self.credentials.model, "credentials.model")?;
        Self::validate_credential_name(&self.credentials.github_token, "credentials.github_token")?;
        if let Some(slack) = &self.comms.slack {
            Self::reject_secret_shaped(&Some(slack.app_token.clone()), "comms.slack.app_token")?;
            Self::reject_secret_shaped(&Some(slack.bot_token.clone()), "comms.slack.bot_token")?;
            Self::validate_credential_name(
                &Some(slack.app_token.clone()),
                "comms.slack.app_token",
            )?;
            Self::validate_credential_name(
                &Some(slack.bot_token.clone()),
                "comms.slack.bot_token",
            )?;
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

    /// Keep the name safe to show in a shell assignment guide.
    fn validate_credential_name(value: &Option<String>, field: &str) -> Result<()> {
        let Some(name) = value else { return Ok(()) };
        let valid = name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid {
            bail!(
                "{field} must name an environment variable using letters, digits, and \
                 underscores, and must not start with a digit"
            );
        }
        Ok(())
    }

    /// The modeled `--set key=value` tokens this file implies, in a stable
    /// order so a plan diff is readable and a test can pin it.
    pub fn helm_sets(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(ui) = self.platform.ui {
            out.push(format!("ui.deploy={ui}"));
        }
        if let Some(inference) = self.platform.inference {
            out.push(format!("inference.deploy={inference}"));
        }
        if let Some(persistence) = self.platform.inference_persistence {
            out.push(format!(
                "{}={persistence}",
                crate::ops::INFERENCE_PERSISTENCE_ENABLED_KEY
            ));
        }
        if let Some(pull_model) = self.platform.inference_pull_model {
            out.push(format!(
                "{}={pull_model}",
                crate::ops::INFERENCE_PULL_MODEL_KEY
            ));
        }
        out
    }

    /// The declared string tokens this file implies, in stable key order.
    pub fn helm_set_strings(&self) -> Vec<String> {
        self.set
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect()
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
        assert!(
            cfg.helm_set_strings().is_empty(),
            "no declared string values"
        );
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
    fn declared_set_entries_remain_verbatim_after_platform_toggles() {
        let raw = format!(
            "{}platform:\n  ui: false\nset:\n  api.githubAppId: \"4475970\"\n  \
             example.label: plain\n  example.leadingZero: \"00123\"\n  worker.replicas: \"3\"\n",
            minimal()
        );
        let cfg = Installation::parse(&raw).unwrap();
        assert_eq!(
            cfg.helm_sets(),
            vec!["ui.deploy=false"],
            "modeled settings must remain in the Helm typed lane"
        );
        assert_eq!(
            cfg.helm_set_strings(),
            vec![
                "api.githubAppId=4475970",
                "example.label=plain",
                "example.leadingZero=00123",
                "worker.replicas=3",
            ],
            "declared strings must retain their exact value in the Helm string lane"
        );
    }

    #[test]
    fn boolean_and_null_shaped_declared_set_values_are_rejected() {
        for value in ["\"true\"", "\" FALSE \"", "\"Null\""] {
            let raw = format!("{}set:\n  example.value: {value}\n", minimal());
            let err =
                Installation::parse(&raw).expect_err(&format!("quoted {value} must be rejected"));
            let message = format!("{err:#}");
            assert!(
                message.contains("set.example.value"),
                "error must name the rejected key: {message}"
            );
            assert!(
                message.contains("set values are always strings"),
                "error must explain the string contract: {message}"
            );
            assert!(
                message.contains("Use a modeled curie.yaml field"),
                "error must give the operator a safe next action: {message}"
            );
        }
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
    let (resolved, missing) = resolve_credentials_lenient(cfg, resolver)?;
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

/// Resolve what is available and REPORT what is not, rather than refusing.
///
/// `curie diff` needs the credential NAMES to know which chart values the file
/// governs. When it cannot resolve a value, the affected comparison is marked
/// unknown rather than inferred from the incomplete plan. Refusing there
/// withheld the answer exactly when it is most wanted: on an install that is
/// not finished yet, which is the state an operator is in when they ask "what
/// would this change?".
///
/// That is what the verb was for, and a shared-plan refactor (#1319) quietly
/// took it away: `diff` started going through `plan_installation`, which bails.
/// Nothing caught it, because the tests cover the pure planner and not this
/// seam. Found by running `curie diff` against a real cluster.
pub fn resolve_credentials_lenient(
    cfg: &Installation,
    resolver: &dyn Fn(&str) -> Result<Option<String>>,
) -> Result<(BTreeMap<String, String>, Vec<String>)> {
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
    Ok((resolved, missing))
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
    /// Carry the object store's contents across a rename, instead of refusing.
    /// `apply` then stages every object, upgrades, and loads them back.
    pub migrate_store: bool,
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
    /// What the stateful probe learned about THIS plan, or `None` when the plan
    /// was completed with [`StatefulProbe::Skip`] and no cluster read happened.
    ///
    /// `None` means NOT PROBED -- never "clear". Reading it as clear is the
    /// silent store destruction the guard exists to prevent, so every consumer
    /// fails closed on it. The verdict lives HERE, in the plan both `apply` and
    /// `diff` consume, rather than beside one of them: #1338 bolted the guard
    /// into `apply` after the shared plan was already built, and the two
    /// surfaces promptly disagreed about the same file on the same cluster
    /// (#1352).
    stateful: Option<StatefulDelta>,
}

pub fn plan_installation(cfg: Installation, dry_run: bool) -> Result<LocalInstallationPlan> {
    plan_installation_inner(cfg, dry_run, false)
}

/// The same plan, but tolerating credentials that are not available yet.
///
/// Only `curie diff` uses this. `apply` must keep refusing: resolving BEFORE
/// mutating is what stops a missing Slack token being discovered after the
/// platform install has already run, leaving a half-applied cluster.
pub fn plan_installation_lenient(
    cfg: Installation,
) -> Result<(LocalInstallationPlan, Vec<String>)> {
    let (_, missing) = resolve_credentials_lenient(&cfg, &resolve_credential)?;
    let plan = plan_installation_inner(cfg, false, true)?;
    Ok((plan, missing))
}

fn plan_installation_inner(
    cfg: Installation,
    dry_run: bool,
    lenient: bool,
) -> Result<LocalInstallationPlan> {
    let resolved = if lenient {
        resolve_credentials_lenient(&cfg, &resolve_credential)?.0
    } else {
        resolve_credentials(&cfg, &resolve_credential)?
    };
    let github_token = cfg
        .credentials
        .github_token
        .as_ref()
        .and_then(|name| resolved.get(name).cloned());
    let up = crate::ops::UpOpts {
        retained_mail_values: None,
        common: crate::ops::CommonOpts {
            namespace: cfg.install.namespace.clone(),
            release: cfg.install.release.clone(),
            dry_run,
        },
        chart: String::new(),
        no_expose: false,
        set: cfg.helm_sets(),
        set_string: cfg.helm_set_strings(),
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

/// Whether completing the plan reads the cluster's StatefulSets.
///
/// `Skip` exists SOLELY for `apply --allow-stateful-removal`, which has already
/// declared the verdict irrelevant and today performs no cluster read at all.
/// Preserving that is what keeps the override working against an unreadable
/// cluster: an operator who has decided to discard a store must not be blocked
/// by a probe whose answer they already overrode (#1352).
enum StatefulProbe {
    /// Read the live StatefulSets and reach a verdict. Every surface that has
    /// not opted out passes this, so `apply` and `diff` read ONE answer.
    Run,
    /// Do not read the cluster at all; the plan's `stateful` stays `None`.
    Skip,
}

async fn complete_installation_plan(
    local: LocalInstallationPlan,
    probe: StatefulProbe,
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
    // Deliberately NOT gated on `complete_live_state`. `apply --dry-run` sets
    // that false, but the guard is documented to run under `--dry-run` anyway:
    // the plan a dry run prints is exactly the plan that would destroy the
    // store. Gating the probe on it would silently stop `apply --dry-run`
    // warning about store deletion, and no existing test would catch it, since
    // the dry-run fixtures only cover values/conflict parity (#1352).
    let stateful = match probe {
        StatefulProbe::Run => Some(guard_stateful_removal(&up, &up_values).await?),
        StatefulProbe::Skip => None,
    };
    let mut desired = up_values.effective_values();
    // Declaring a model credential selects the real model independently of the
    // credential value. The lenient diff may not know that value, but it still
    // knows apply will set fakeModel=false. Keep an explicit set override if
    // the file supplied one.
    if cfg.credentials.model.is_some() {
        desired
            .entry(crate::ops::FAKE_MODEL_KEY.to_string())
            .or_insert_with(|| "false".to_string());
    }
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
        stateful,
    })
}

/// What the stateful-removal guard learned about this apply.
///
/// The verdict is a VALUE, not the `Err` half of a `Result`. While the refusal
/// was the only error the guard could produce, "Err means a removal was found"
/// held by construction, and `--migrate-store` read it that way. Once the guard
/// could also fail because the cluster was unreadable (#1351), that reading
/// promoted "I could not find out" to "definitely at risk, start moving data",
/// which is the same ambiguous-signal-turned-into-an-action shape #1351 exists
/// to close, pointed the other way. So `Err` now means only that the guard could
/// not reach a verdict, and every caller must propagate it.
enum GuardVerdict {
    /// Nothing the release runs would be deleted by this apply.
    Clear,
    /// This apply would delete stateful component(s), carrying the operator
    /// facing causes.
    WouldRemove(Vec<crate::ops::StatefulRemoval>),
}

/// Everything one stateful probe learned: the verdict `apply` turns on, plus
/// the object-store rename `--migrate-store` is the remedy for.
///
/// The two travel together because they are read from the SAME live and
/// rendered component lists, and computing the migration anywhere else would
/// mean a second cluster read that could disagree with the verdict -- the very
/// two-surfaces-disagree defect #1352 exists to close.
///
/// Only `diff` reads `migration`; `apply` reads `verdict` alone and behaves
/// exactly as it did before this field existed.
struct StatefulDelta {
    verdict: GuardVerdict,
    /// The object-store component rename this upgrade performs, as
    /// `(from, to)` COMPONENT names (`minio` -> `rustfs`) -- never resource
    /// names, which embed the chart fullname and change with `nameOverride`.
    ///
    /// `Some` is exactly the case `curie apply --migrate-store` carries the
    /// data across. `None` means there is no such rename, which is NOT a
    /// statement that anything is safe: a store disabled through the chart's
    /// own BYO gate (`postgres.deploy: false`, the #1352 reproduction) is a
    /// removal with no automatic carry, and `verdict` is what says so.
    migration: Option<(String, String)>,
}

/// Decide whether an apply would delete a stateful component the release runs.
///
/// Runs even under `--dry-run`: the plan a dry run prints is exactly the plan
/// that would destroy the store, so an operator reading it deserves the same
/// warning the real run would give.
async fn guard_stateful_removal(
    up: &crate::ops::UpOpts,
    value_plan: &crate::ops::UpValuePlan,
) -> Result<StatefulDelta> {
    let live = crate::ops::live_stateful_components(&up.common)
        .await
        // Verb-neutral: this verdict is now carried by the shared plan and read
        // by BOTH `curie apply` and `curie diff` (#1352), so naming "this apply"
        // here would have `curie diff` report a verb the operator did not run.
        // `live_stateful_components` already keeps its own wording neutral for
        // the same reason and leaves the framing to its caller (#1351); this
        // caller now has two callers of its own.
        .context("could not check whether this change would remove stateful component(s)")?;
    if live.is_empty() {
        // Genuinely nothing to lose: a namespaced LIST returns an empty items
        // array with exit 0 for a fresh install AND for a namespace that does
        // not exist. A read that FAILED never reaches here; it is an error now
        // rather than a silent empty answer (#1351).
        //
        // This short-circuits BEFORE the chart is rendered, so `migration` is
        // `None` here. Correct rather than incomplete: nothing is running, so
        // there is nothing to carry across.
        return Ok(StatefulDelta {
            verdict: GuardVerdict::Clear,
            migration: None,
        });
    }
    let rendered = crate::ops::chart_stateful_components(&up.chart, &up.common, value_plan).await?;
    // The one place both the live and the rendered component lists exist, so
    // the object-store rename is decided from the SAME read as the verdict
    // (#1352). Reuses `migrate_store`'s pure decision rather than re-detecting
    // the store here -- a second copy of "which component is the store" is how
    // the two surfaces would drift apart again.
    //
    // Swallowing the `Err` is correct for this field, and only for this field.
    // `plan` errors when neither side runs a store it can name, and "there is
    // no store to migrate" is a legitimate answer to the question this field
    // asks. It must NEVER be read as "safe": `verdict` below is the sole
    // statement about whether anything is at risk, and it is computed
    // independently of this.
    let migration = match crate::migrate_store::plan(
        &live.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>(),
        &rendered.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>(),
    )
    .ok()
    {
        Some(crate::migrate_store::MigrationPlan::Migrate { from, to }) => {
            Some((from.suffix().to_string(), to.suffix().to_string()))
        }
        Some(crate::migrate_store::MigrationPlan::NotNeeded { .. }) | None => None,
    };
    let removed = crate::ops::removed_stateful_components(&live, &rendered);
    if removed.is_empty() {
        return Ok(StatefulDelta {
            verdict: GuardVerdict::Clear,
            migration,
        });
    }
    Ok(StatefulDelta {
        verdict: GuardVerdict::WouldRemove(removed),
        migration,
    })
}

/// The one rendering of a component the target chart does not render at all.
/// Both refusals show this line, and an operator matching what one message
/// said against what the other says needs them identical, so the phrasing
/// lives once (#1501).
fn component_gone_line(removal: &crate::ops::StatefulRemoval) -> String {
    format!(
        "{} ({}, not rendered at all)",
        removal.name, removal.component
    )
}

/// The refusal text, factored out so its ordering is testable with no cluster.
///
/// Branches on the CAUSE, because the two causes need opposite advice and a
/// refusal an operator cannot act on is a wall, not a guard. A rename in
/// particular has a one-line fix in their own file -- if the message withheld
/// that and offered `--migrate-store` instead, the only way past would be
/// `--allow-stateful-removal`, i.e. the guard would talk them into the
/// destruction it exists to prevent.
fn stateful_removal_message(removed: &[crate::ops::StatefulRemoval]) -> String {
    use crate::ops::RemovalCause;

    let listed = removed
        .iter()
        .map(|r| match &r.cause {
            RemovalCause::ComponentGone => component_gone_line(r),
            RemovalCause::RenamedTo(to) => format!("{} -> {}", r.name, to),
        })
        .collect::<Vec<_>>()
        .join("\n  ");

    let mut msg = format!(
        "refusing to apply: this would DELETE {} stateful component(s) the release is \
         running, and the persistent data with them:\n  {listed}\n\n\
         For the bundle store, every sandbox reads from it at start, so losing it \
         breaks the next turn and not merely a rollback.\n\n",
        removed.len(),
    );

    msg.push_str(&stateful_removal_remedies(removed));

    msg.push_str("Use --allow-stateful-removal only to proceed WITHOUT the data.");
    msg
}

/// The per-cause remedies, factored out of [`stateful_removal_message`] so
/// `curie diff` can name the SAME two fixes without restating apply's refusal
/// header (#1352).
///
/// Factored rather than restated: the two causes have OPPOSITE remedies, and a
/// hand-written second copy in `diff` would drift from the refusal an operator
/// then hits from `apply` on the identical file -- the two surfaces disagreeing
/// is the whole defect. `stateful_removal_message` composes header + these
/// remedies + trailer, so its text is unchanged byte for byte.
fn stateful_removal_remedies(removed: &[crate::ops::StatefulRemoval]) -> String {
    use crate::ops::RemovalCause;

    let mut msg = String::new();
    let renamed: Vec<&crate::ops::StatefulRemoval> = removed
        .iter()
        .filter(|r| matches!(r.cause, RemovalCause::RenamedTo(_)))
        .collect();
    if !renamed.is_empty() {
        // Deliberately concrete. The operator is looking at a resource named
        // `<something>-postgres` and a plan that creates `<something>-curie-postgres`;
        // "your values disagree" is true and useless. The name the chart uses
        // is derived from `nameOverride`, so hand them that line.
        msg.push_str(
            "These components still exist in the chart -- they would just be recreated \
             under NEW names, empty, beside the orphaned volumes. That is a values \
             difference, not a chart change: the release was installed with a \
             `nameOverride` your curie.yaml does not declare.\n\n\
             Fix it in the file rather than overriding the guard -- add the release's \
             own name:\n\n\
             \x20 set:\n\
             \x20   nameOverride: <the name the live resources start with>\n\n\
             then re-run `curie diff` and confirm the rename entries are gone.\n\n",
        );
    }

    // #1501 settled what `--migrate-store` can actually carry -- the OBJECT
    // STORE and nothing else -- and made `apply` REFUSE a migration batch
    // holding anything else. So the ComponentGone removals split by that same
    // predicate, rather than all receiving one remedy: telling the operator of
    // a dropped `postgres` to "re-run with --migrate-store" would name a flag
    // apply then refuses, which is the diff-vs-apply disagreement #1352 exists
    // to close, reintroduced from the other side.
    let (carriable, uncarriable): (
        Vec<&crate::ops::StatefulRemoval>,
        Vec<&crate::ops::StatefulRemoval>,
    ) = removed
        .iter()
        .filter(|r| r.cause == RemovalCause::ComponentGone)
        .partition(|r| crate::migrate_store::is_object_store_component(&r.component));

    if !uncarriable.is_empty() {
        // Deliberately the substance and the voice of `uncarriable_removal_message`
        // (the refusal `apply --migrate-store` raises just below `apply`), down to
        // the shared `component_gone_line`: an operator matching what `diff` said
        // against what `apply` says on the identical file has to read one story,
        // not two (#1352, #1501). It does NOT offer `--migrate-store`, because for
        // these components that flag is a dead end.
        let listed = uncarriable
            .iter()
            .map(|r| component_gone_line(r))
            .collect::<Vec<_>>()
            .join("\n  ");
        msg.push_str(&format!(
            "--migrate-store carries the OBJECT STORE and nothing else, so it cannot \
             carry {} of these stateful component(s):\n  {listed}\n\n\
             A component the target chart does not render at all is usually a values \
             difference -- a `<component>.deploy=false` in the file, or a chart version \
             that dropped it. Fix the values so these components are still rendered, then \
             re-run `curie diff` and confirm these entries are gone.\n\n",
            uncarriable.len(),
        ));
    }

    if !carriable.is_empty() {
        // Conditional on whatever still blocks the migration, in the order the
        // operator has to clear it: renames live in their own file, and an
        // uncarriable component has to be rendered again before `--migrate-store`
        // will be accepted at all (#1501).
        let migration_instruction = match (renamed.is_empty(), uncarriable.is_empty()) {
            (true, true) => "Re-run with --migrate-store",
            (false, true) => "After fixing the renames, re-run with --migrate-store",
            (true, false) => "After restoring the component(s) above, re-run with --migrate-store",
            (false, false) => {
                "After fixing the renames and restoring the component(s) above, re-run with \
                 --migrate-store"
            }
        };
        msg.push_str(&format!(
            "Component(s) the chart does not render at all usually mean a chart version \
                 renamed or removed them. {migration_instruction} and apply will carry \
                 the data across itself: it stages every object, upgrades, loads them back, \
                 and verifies per object.\n\n"
        ));
    }
    msg
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
        migrate_store,
    } = opts;
    // The parser rejects this pair (`conflicts_with`), but `ApplyOpts` has pub
    // fields on a library crate, so the invariant is asserted where the
    // destruction is owned rather than only at the CLI edge. Contradictory
    // intent is refused, never resolved by picking one: silently dropping
    // --migrate-store took the data destroying path with exit 0 (#1351).
    if allow_stateful_removal && migrate_store {
        bail!(
            "--migrate-store and --allow-stateful-removal are contradictory: one \
             carries the object store's data across the upgrade, the other proceeds \
             WITHOUT it. Pass exactly one."
        );
    }
    // Before the plan is completed, because the plan's stateful probe renders
    // THIS chart: an empty `up.chart` would render nothing and report every
    // live component as gone.
    local.up.chart = chart;
    let dry_run = local.up.common.dry_run;
    // `Skip` is the only path that reads no cluster state, and it is reserved
    // for the operator who already overrode the verdict. Everything else --
    // including `--dry-run` and `--migrate-store` -- probes (#1352).
    let plan = complete_installation_plan(
        local,
        if allow_stateful_removal {
            StatefulProbe::Skip
        } else {
            StatefulProbe::Run
        },
    )
    .await?;
    let EffectiveInstallationPlan {
        cfg,
        up,
        up_values,
        github_token,
        comms,
        live,
        stateful,
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
    // `apply` handles the migration itself rather than sending the operator to a
    // separate verb. ADR-0097's premise is that the file states whole intent and
    // apply computes the delta; "go run this other command first, then come back
    // and pass an override" is the opposite of that -- and it made the
    // documented happy path include `--allow-stateful-removal`, training an
    // operator to bypass the one guard that protects the case that is real.
    //
    // Still opt-in, because a store migration has a window where the store is
    // empty and the bot cannot answer. An `apply` that changes a log level must
    // never silently start moving data. So the refusal NAMES the flag, and
    // passing it makes apply do the whole thing.
    //
    // An `Err` here is the guard failing to reach a verdict (an unreadable
    // cluster, a failed `helm template`), never a removal. It propagates on
    // EVERY path including `--migrate-store`: "I could not find out" must not
    // become "start moving data" (#1351).
    //
    // The verdict is read from the shared plan rather than computed here, so
    // `curie diff` cannot reach a different answer on the same file (#1352).
    // `None` is NOT PROBED and must NEVER be read as `Clear` -- that would
    // restore exactly the silent destruction this guard exists to prevent -- so
    // the read is fail-closed through `.context(..)?`.
    let migrating = if allow_stateful_removal {
        false
    } else {
        // Guard the exact upgrade plan, including preserved values carried through
        // the private values file.
        // `.verdict` only: the delta's `migration` is a `diff` reporting field.
        // What apply does is decided by the verdict alone, exactly as before
        // that field existed (#1352).
        match stateful
            .context("the stateful-removal guard did not run")?
            .verdict
        {
            GuardVerdict::Clear => false,
            // All-ComponentGone was the whole bypass condition, and it is only
            // HALF the question. `--migrate-store` carries the OBJECT STORE and
            // nothing else -- `migrate_store::detect_store` knows `minio` and
            // `rustfs` -- so a batch of {minio gone, postgres gone} (postgres
            // goes ComponentGone the moment target values set
            // `postgres.deploy=false`) passed the check, and apply migrated the
            // store while silently DELETING the database beside it, exit 0
            // (#1501). The cause says the data must be carried; only the
            // COMPONENT says whether this flag can carry it.
            GuardVerdict::WouldRemove(removed)
                if migrate_store
                    && removed.iter().all(|removal| {
                        matches!(&removal.cause, crate::ops::RemovalCause::ComponentGone)
                    }) =>
            {
                let uncarriable: Vec<&crate::ops::StatefulRemoval> = removed
                    .iter()
                    .filter(|removal| {
                        !crate::migrate_store::is_object_store_component(&removal.component)
                    })
                    .collect();
                if uncarriable.is_empty() {
                    true
                } else {
                    bail!("{}", uncarriable_removal_message(&uncarriable))
                }
            }
            GuardVerdict::WouldRemove(removed) => {
                bail!("{}", stateful_removal_message(&removed))
            }
        }
    };

    // Stage BEFORE the upgrade deletes the old store. A failure here leaves the
    // cluster untouched.
    if migrating {
        // Same values the guard rendered with, and the same values the upgrade
        // below will apply. Rendering the export's copy of the chart with NO
        // values made the two halves plan different upgrades: the guard saw the
        // store removed and waved the migration through, the export saw a store
        // to migrate INTO, and the disagreement only surfaced after the
        // irreversible upgrade had run (#1501). Borrowed here, before
        // `up_prepared` takes ownership.
        crate::migrate_store::run_export_with_values(
            &up.common,
            &up.chart,
            BUNDLE_BUCKET,
            &up_values,
        )
        .await?;
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

    // Load the staged objects into the planned target. Import returns only once
    // the persisted source inventory is present there at the same sizes.
    if migrating {
        let common = crate::ops::CommonOpts {
            namespace: cfg.install.namespace.clone(),
            release: cfg.install.release.clone(),
            dry_run: false,
        };
        let recovery_command = format!(
            "`curie cluster migrate-store --phase import --namespace {} --release {}`",
            common.namespace, common.release
        );
        let target = crate::migrate_store::read_planned_target(&common)
            .await
            .with_context(|| {
                format!(
                    "the upgrade applied, but the planned migration target could not be verified; the staging pod remains available. Retry with {recovery_command}"
                )
            })?;
        crate::migrate_store::run_import_to_planned_target(
            &common,
            target,
            BUNDLE_BUCKET,
            false,
        )
            .await
            .with_context(|| {
                format!(
                    "the upgrade applied, but store import verification did not complete; the staging pod remains available. Retry with {recovery_command}"
                )
            })?;
    }

    Ok(ApplyOutput::Applied {
        namespace: cfg.install.namespace,
        release: cfg.install.release,
        comms: configured_comms,
    })
}

/// The refusal for removals `--migrate-store` cannot carry.
///
/// Separate from [`stateful_removal_message`] rather than a branch inside it,
/// because the operator is in a different position: they already reached for
/// the flag that keeps data, and the answer is that this flag's reach stops at
/// the object store. So it names the COMPONENTS -- "some component" would be a
/// wall, and their next move is finding which of their values drops this one --
/// and it does not re-offer `--migrate-store`, which is what sent them here.
fn uncarriable_removal_message(uncarriable: &[&crate::ops::StatefulRemoval]) -> String {
    let listed = uncarriable
        .iter()
        .map(|r| component_gone_line(r))
        .collect::<Vec<_>>()
        .join("\n  ");

    format!(
        "refusing to apply: --migrate-store carries the OBJECT STORE and nothing else, \
         and this apply would also DELETE {} stateful component(s) it cannot carry, \
         with the persistent data in them:\n  {listed}\n\n\
         A component the target chart does not render at all is usually a values \
         difference -- a `<component>.deploy=false` in the file, or a chart version \
         that dropped it. Fix the values so these components are still rendered, then \
         re-run with --migrate-store to carry the store across.\n\n\
         Use --allow-stateful-removal only to proceed WITHOUT their data.",
        uncarriable.len(),
    )
}

/// The bundle bucket the platform reads, mirroring the chart's `BUNDLE_BUCKET`.
const BUNDLE_BUCKET: &str = "curie-bundles";

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
    /// The file declares the value through a credential that is unavailable.
    /// Apply refuses this state, so no concrete outcome may be inferred.
    Unknown,
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
            DiffKind::Unknown => '?',
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            DiffKind::Add => "add",
            DiffKind::Change => "change",
            DiffKind::Same => "unchanged",
            DiffKind::Preserved => "preserved",
            DiffKind::Reset => "reset to chart default",
            DiffKind::Unknown => "unknown",
        }
    }

    /// Must this entry keep the change count above zero?
    pub fn is_change(self) -> bool {
        matches!(
            self,
            DiffKind::Add | DiffKind::Change | DiffKind::Reset | DiffKind::Unknown
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub key: String,
    pub kind: DiffKind,
    /// The live value, already masked when the key carries a secret.
    pub from: Option<String>,
    /// The resolved declared value, already masked when the key carries a secret.
    /// Absent when the file does not declare the key or its credential is unavailable.
    pub to: Option<String>,
    /// The unavailable credential NAME that prevents a concrete comparison.
    pub unresolved_credential: Option<String>,
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
            unresolved_credential: None,
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
            unresolved_credential: None,
        });
    }

    entries.sort_by(|a, b| a.key.cmp(&b.key));
    entries
}

/// Replace every comparison that depends on an unavailable credential with one
/// explicit unknown entry. Missing desired and live values still get an entry,
/// because their absence cannot prove what strict apply would do after the
/// credential resolves.
fn disclose_unresolved_credentials(
    entries: &mut Vec<DiffEntry>,
    cfg: &Installation,
    unresolved: &[String],
) {
    let is_unresolved = |name: &str| unresolved.iter().any(|missing| missing == name);
    let mut affected = Vec::new();

    if let Some(name) = cfg
        .credentials
        .model
        .as_deref()
        .filter(|n| is_unresolved(n))
    {
        affected.push((crate::ops::MODEL_CREDENTIAL_KEY, name));
    }
    if let Some(name) = cfg
        .credentials
        .github_token
        .as_deref()
        .filter(|n| is_unresolved(n))
    {
        affected.push((crate::ops::GITHUB_TOKEN_KEY, name));
    }
    if let Some(slack) = &cfg.comms.slack {
        if is_unresolved(&slack.app_token) {
            affected.push(("dispatcher.slack.appToken", slack.app_token.as_str()));
        }
        if is_unresolved(&slack.bot_token) {
            affected.push(("dispatcher.slack.botToken", slack.bot_token.as_str()));
        }
    }

    for (key, credential) in affected {
        if let Some(entry) = entries.iter_mut().find(|entry| entry.key == key) {
            entry.kind = DiffKind::Unknown;
            entry.to = None;
            entry.unresolved_credential = Some(credential.to_string());
        } else {
            entries.push(DiffEntry {
                key: key.to_string(),
                kind: DiffKind::Unknown,
                from: None,
                to: None,
                unresolved_credential: Some(credential.to_string()),
            });
        }
    }

    entries.sort_by(|left, right| left.key.cmp(&right.key));
}

/// What `curie diff` found.
#[derive(Debug)]
pub struct DiffOutput {
    /// Declared credential names with no value available. A non-empty list
    /// means `apply` would refuse until they resolve. Entries that depend on a
    /// missing value are marked unknown.
    pub unresolved_credentials: Vec<String>,
    pub namespace: String,
    pub release: String,
    /// `false` when helm has no record of the release. Known values are creates,
    /// while unavailable credential values remain unknown.
    pub release_exists: bool,
    /// The chart the release was installed with (`curie-0.5.1`), if readable.
    pub chart_deployed: Option<String>,
    /// The version of the chart this run would apply: this CLI's own, or the
    /// `--chart` override's when one was given.
    pub chart_target: String,
    pub entries: Vec<DiffEntry>,
    /// Live stateful components the target chart would not recreate in place.
    ///
    /// Non-empty means `curie apply` on this exact file REFUSES. Carried as the
    /// full `StatefulRemoval` rather than a name list because the two causes
    /// have opposite remedies, and a diff that hid the cause would send half the
    /// operators to a flag that cannot help them (#1352).
    pub stateful_removals: Vec<crate::ops::StatefulRemoval>,
    /// The object-store rename this upgrade performs, as `(from, to)` COMPONENT
    /// names (`minio` -> `rustfs`) -- never resource names, which embed the
    /// chart fullname and all change together under a `nameOverride`.
    ///
    /// The discriminator the removals list alone cannot supply: `Some` is
    /// exactly the case `curie apply --migrate-store` carries the data across.
    /// `None` beside a non-empty `stateful_removals` means there is no
    /// automatic carry -- a store disabled through the chart's own BYO gate
    /// (`postgres.deploy: false`, #1352's own reproduction) removes a component
    /// `--migrate-store` has nothing to move, so apply keeps refusing until the
    /// file is fixed.
    pub migration: Option<(String, String)>,
}

impl DiffOutput {
    /// Every change `apply` would make, values AND components.
    ///
    /// The removals are counted, not merely listed: automation reads this
    /// integer, and the defect this closes was `changes: 3` with exit 0 on the
    /// file `apply` refuses. A store deletion that did not move the number is
    /// still invisible to every consumer that only reads it (#1352).
    pub fn changes(&self) -> usize {
        self.entries.iter().filter(|e| e.kind.is_change()).count() + self.stateful_removals.len()
    }

    /// The deployed chart's version, stripped of the `curie-` name prefix.
    fn deployed_version(&self) -> Option<&str> {
        self.chart_deployed
            .as_deref()
            .map(|c| c.rsplit_once('-').map(|(_, v)| v).unwrap_or(c))
    }

    /// Would `apply` change the chart under these values, not just the values?
    ///
    /// The ENTRY comparison is value-level, so it cannot see a non-stateful
    /// component being added, removed, or renamed between chart versions --
    /// when that happens its output is not merely incomplete but misleading,
    /// since a renamed component's old keys render as ordinary resets.
    /// `stateful_removals` closes the half of that blind spot where the cost is
    /// irreversible (#1352); the rest of it remains.
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
            "unresolved_credentials": self.unresolved_credentials,
            "chart_deployed": self.chart_deployed,
            "chart_target": self.chart_target,
            "chart_version_differs": self.chart_version_differs(),
            "changes": self.changes(),
            "entries": self.entries.iter().map(|e| {
                let mut value = serde_json::json!({
                    "key": e.key,
                    "kind": e.kind.label(),
                    "from": e.from,
                    "to": e.to,
                });
                if let Some(credential) = &e.unresolved_credential {
                    value
                        .as_object_mut()
                        .expect("diff entry is an object")
                        .insert(
                            "unresolved_credential".to_string(),
                            serde_json::Value::String(credential.clone()),
                        );
                }
                value
            }).collect::<Vec<_>>(),
            // ALWAYS emitted, including as `[]`. The key is optional in the
            // schema (an additive MINOR bump), but omitting it when empty would
            // re-hide the field from a consumer that looks for it and cannot
            // tell "no removals" from "a CLI that does not report them" (#1352).
            "stateful_removals": self.stateful_removals.iter().map(|r| {
                // `RemovalCause` carries no serde derive, so the wire encoding
                // is written here by hand and is the contract: `renamed_to` is
                // present ONLY on `renamed`, so a consumer branching on the
                // cause cannot read a missing target as an empty one.
                let mut value = serde_json::json!({
                    "name": r.name,
                    "component": r.component,
                    "cause": match &r.cause {
                        crate::ops::RemovalCause::ComponentGone => "component_gone",
                        crate::ops::RemovalCause::RenamedTo(_) => "renamed",
                    },
                });
                if let crate::ops::RemovalCause::RenamedTo(to) = &r.cause {
                    value
                        .as_object_mut()
                        .expect("stateful removal is an object")
                        .insert(
                            "renamed_to".to_string(),
                            serde_json::Value::String(to.clone()),
                        );
                }
                value
            }).collect::<Vec<_>>(),
            // ALWAYS emitted too, as an object or as `null`, for the same
            // reason: a consumer must be able to tell "no store rename" from "a
            // CLI that does not report one". Component names, never resource
            // names -- a resource name embeds the chart fullname, so a
            // `nameOverride` release would report a pair no consumer could
            // match against `minio`/`rustfs` (#1352).
            "migration": self.migration.as_ref().map(|(from, to)| serde_json::json!({
                "from": from,
                "to": to,
            })),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if !self.release_exists {
            ui.payload_plain(&format!(
                "release '{}' does not exist in namespace '{}'; every known value below would be created, while entries marked `?` remain unknown",
                self.release, self.namespace
            ));
        }
        for e in &self.entries {
            let line = if e.kind == DiffKind::Unknown {
                match (&e.from, &e.unresolved_credential) {
                    (Some(from), Some(credential)) => format!(
                        "{} {}: {} to unknown (`export {}='<value>'` to resolve)",
                        e.kind.marker(),
                        e.key,
                        from,
                        credential
                    ),
                    (_, Some(credential)) => format!(
                        "{} {}: unknown (`export {}='<value>'` to resolve)",
                        e.kind.marker(),
                        e.key,
                        credential
                    ),
                    (Some(from), None) => {
                        format!("{} {}: {} to unknown", e.kind.marker(), e.key, from)
                    }
                    (None, None) => format!("{} {}: unknown", e.kind.marker(), e.key),
                }
            } else {
                match (&e.from, &e.to) {
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
                }
            };
            ui.payload_plain(&line);
        }
        // Beside the entries, not folded into them: a removal is a COMPONENT
        // going away with its data, and rendering it as another `key: value`
        // row is what let store destruction read as `+ postgres.deploy: false`
        // (#1352). The live resource name leads, because that is the string the
        // operator recognises in their own cluster.
        for removal in &self.stateful_removals {
            let cause = match &removal.cause {
                crate::ops::RemovalCause::ComponentGone => {
                    "the target chart does not render it".to_string()
                }
                crate::ops::RemovalCause::RenamedTo(to) => {
                    format!("the target renders it as {to}")
                }
            };
            ui.payload_plain(&format!(
                "STATEFUL: {} ({}) would be DELETED with its data -- {}",
                removal.name, removal.component, cause,
            ));
        }
        let changes = self.changes();
        if changes == 0 {
            ui.payload_plain("no changes: the cluster already matches this file");
        } else if self
            .entries
            .iter()
            .any(|entry| entry.kind == DiffKind::Unknown)
        {
            ui.payload_plain(&format!("{changes} change(s) or unresolved comparison(s)"));
        } else if !self.stateful_removals.is_empty() {
            // "would be applied" is the ORIGINAL defect's wording, and folding
            // the removals into `changes()` without touching this line merely
            // made it a bigger number telling the same lie: `curie apply` on
            // this file REFUSES, it does not apply anything. The count is still
            // reported, because automation and humans both read it -- it is the
            // OUTCOME that must not be misstated (#1352).
            ui.payload_plain(&format!(
                "{changes} change(s), including {} stateful removal(s): `curie apply` will \
                 REFUSE this file rather than apply it",
                self.stateful_removals.len(),
            ));
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
        if !self.unresolved_credentials.is_empty() {
            let exports = self
                .unresolved_credentials
                .iter()
                .map(|name| format!("`export {name}='<value>'`"))
                .collect::<Vec<_>>()
                .join(", ");
            ui.note(&format!(
                "{} declared credential(s) have no value here. Entries marked `?` are \
                 unknown until they resolve. Set them with {}; `curie apply` will refuse \
                 until then.",
                self.unresolved_credentials.len(),
                exports,
            ));
        }
        if self.chart_version_differs() {
            ui.note(&format!(
                "CHART VERSION MISMATCH: the release runs {} but this curie applies {}. \
                 The comparison above is values-only, so it cannot see a NON-STATEFUL \
                 component added, removed, or renamed between those versions, and a \
                 renamed one shows up as an ordinary reset. Stateful components are the \
                 exception -- those are reported directly above from a live read (#1352). \
                 Do not read this as a safe apply. Reconcile the chart version first, or \
                 apply with the matching chart.",
                self.chart_deployed.as_deref().unwrap_or("unknown"),
                self.chart_target,
            ));
        }
        // Truly last, so it is the line left on screen: it is the only note
        // that says the answer above is not merely incomplete but will be
        // REFUSED. The remedies come from the same helper apply's refusal uses,
        // so the two surfaces cannot drift into naming different fixes (#1352).
        if !self.stateful_removals.is_empty() {
            // The remedies now say WHICH `component_gone` removals
            // `--migrate-store` can carry, but not what it would carry them
            // FROM and TO. Naming the concrete store rename here is what turns
            // "this flag applies to you" into a fact the operator can check
            // against their own release -- and its absence, beside a non-empty
            // removal list, is how a store merely disabled through the chart's
            // BYO gate reads as the dead end it is (#1352, #1501).
            let migration = match &self.migration {
                Some((from, to)) => format!(
                    "\n\nThis upgrade renames the object store {from} -> {to}, which is \
                     exactly what `curie apply --migrate-store` carries the data across."
                ),
                None => String::new(),
            };
            ui.note(&format!(
                "STATEFUL REMOVAL: `curie apply` will REFUSE this file -- it would DELETE \
                 {} stateful component(s) the release is running, and the persistent data \
                 with them.\n\n{}{}",
                self.stateful_removals.len(),
                stateful_removal_remedies(&self.stateful_removals).trim_end(),
                migration,
            ));
        }
    }
}

pub struct DiffOpts {
    /// Credential NAMES the file declares that have no value available here.
    /// Reported, never fatal: diff mutates nothing.
    pub unresolved_credentials: Vec<String>,
    pub local: LocalInstallationPlan,
    /// The chart `apply` would use, already materialized.
    ///
    /// Required, not optional: the stateful probe RENDERS this chart to decide
    /// what survives, and diff answering that question against a different
    /// chart than apply would use is the same two-surfaces-disagree defect in a
    /// new place (#1352).
    pub chart: String,
    /// The version of the chart named by `chart`.
    ///
    /// Supplied by the caller rather than read from `artifacts::version()`
    /// here: under `--chart` the rendered chart is not this CLI's own, and a
    /// diff that renders one chart while reporting another as the target is the
    /// same two-sources-of-truth defect in a new place (#1352).
    pub chart_target: String,
}

/// Compare the file against the live release.
///
/// Resolves the same local inputs as apply, then performs one values read and
/// provider resolution to complete the desired plan without mutating it.
///
/// **A credential it cannot resolve is reported, never fatal.** The question
/// "what would this change?" is most urgent on an install that is not finished,
/// and refusing to answer it there was the behaviour a shared-plan refactor
/// introduced and a real run against a cluster exposed.
pub async fn diff(opts: DiffOpts) -> Result<DiffOutput> {
    let DiffOpts {
        unresolved_credentials,
        mut local,
        chart,
        chart_target,
    } = opts;
    // Before completing the plan, exactly as `apply` does: the probe renders
    // THIS chart, and an empty ref would render nothing and call every live
    // component gone.
    local.up.chart = chart;
    // `Run` unconditionally. `diff` has no `--allow-stateful-removal` to opt
    // out with, and skipping the probe on, say, an absent release would
    // recreate the disagreement: `apply` does not skip it either (#1352).
    let plan = complete_installation_plan(local, StatefulProbe::Run).await?;
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
    disclose_unresolved_credentials(&mut entries, &plan.cfg, &unresolved_credentials);
    // Fail closed, exactly as `apply` does. `None` is NOT PROBED, and the
    // schema defines `[]` as "the live StatefulSets were read and none are at
    // risk" -- so mapping the unprobed case to `[]` would have `diff` state a
    // fact it never established. That vacuous pass is the shape #1351 exists to
    // close, and a `diff` that cannot say whether a store would be deleted must
    // fail rather than answer `[]` (#1352). A future `Skip` caller therefore
    // gets an error here, not a quiet diff.
    let stateful = plan
        .stateful
        .context("the stateful-removal check did not run")?;
    let stateful_removals = match stateful.verdict {
        GuardVerdict::WouldRemove(removed) => removed,
        GuardVerdict::Clear => Vec::new(),
    };
    Ok(DiffOutput {
        migration: stateful.migration,
        unresolved_credentials,
        namespace: plan.cfg.install.namespace,
        release: plan.cfg.install.release,
        release_exists: plan.live.is_some(),
        chart_deployed,
        chart_target,
        entries,
        stateful_removals,
    })
}

#[cfg(test)]
mod stateful_guard_message_tests {
    /// The refusal is the only place an operator learns what to do next, so it
    /// has to lead with the flag that KEEPS the data. Leading with
    /// `--allow-stateful-removal` is what made the documented happy path a
    /// safety-override, which trains the habit the guard exists to prevent.
    #[test]
    fn the_refusal_offers_migration_before_discarding() {
        let msg = super::stateful_removal_message(&[crate::ops::StatefulRemoval {
            name: "acme-minio".to_string(),
            component: "minio".to_string(),
            cause: crate::ops::RemovalCause::ComponentGone,
        }]);
        let migrate = msg
            .find("--migrate-store")
            .expect("must offer --migrate-store");
        let discard = msg
            .find("--allow-stateful-removal")
            .expect("must still mention the discard flag");
        assert!(
            migrate < discard,
            "the data-preserving flag must come first:\n{msg}"
        );
        assert!(
            msg.contains("WITHOUT the data"),
            "the discard flag must say what it costs:\n{msg}"
        );
        assert!(msg.contains("acme-minio"), "must name the component: {msg}");
    }

    /// A rename has a one-line fix in the operator's OWN file. If the refusal
    /// does not say so, the only way past it is `--allow-stateful-removal` --
    /// the guard would be arguing for the destruction it exists to prevent.
    #[test]
    fn a_rename_is_pointed_at_the_file_not_at_a_flag() {
        let msg = super::stateful_removal_message(&[crate::ops::StatefulRemoval {
            name: "acme-bot-postgres".to_string(),
            component: "postgres".to_string(),
            cause: crate::ops::RemovalCause::RenamedTo("acme-bot-curie-postgres".to_string()),
        }]);
        assert!(
            msg.contains("nameOverride"),
            "must name the value that causes it:\n{msg}"
        );
        assert!(
            msg.contains("acme-bot-postgres") && msg.contains("acme-bot-curie-postgres"),
            "must show both names, so the operator can see it IS their release:\n{msg}"
        );
        assert!(
            !msg.contains("--migrate-store"),
            "--migrate-store cannot fix a rename -- both sides run the same store, so \
             there is nothing to migrate between. Offering it sends the operator down a \
             path that dead-ends at --allow-stateful-removal:\n{msg}"
        );
    }

    /// Both causes at once must not lose either remedy.
    #[test]
    fn a_mixed_batch_carries_both_remedies() {
        let msg = super::stateful_removal_message(&[
            crate::ops::StatefulRemoval {
                name: "acme-bot-minio".to_string(),
                component: "minio".to_string(),
                cause: crate::ops::RemovalCause::ComponentGone,
            },
            crate::ops::StatefulRemoval {
                name: "acme-bot-postgres".to_string(),
                component: "postgres".to_string(),
                cause: crate::ops::RemovalCause::RenamedTo("acme-bot-curie-postgres".to_string()),
            },
        ]);
        let rename = msg.find("nameOverride").expect("must name the rename fix");
        let migrate = msg
            .find("After fixing the renames, re-run with --migrate-store")
            .expect("migration must be conditional on fixing the rename first");
        let discard = msg.find("--allow-stateful-removal").unwrap();
        assert!(
            rename < migrate && migrate < discard,
            "fix the rename first, then migrate, and offer discard only last:\n{msg}"
        );
        assert!(
            !msg.contains("Re-run with --migrate-store"),
            "must not offer migration unconditionally while a rename still blocks it:\n{msg}"
        );
    }
}

#[cfg(test)]
mod diff_tests {
    use super::*;
    // Env mutation is process-global: there must be exactly one lock domain
    // guarding it, so this reuses the crate-wide lock (also taken by
    // `doctor.rs`'s model-env test) instead of a lock private to this file.
    // Named at every use site rather than aliased, so the single domain stays
    // visible to whoever next reaches for a second one.

    struct CredentialEnvRestore(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl CredentialEnvRestore {
        fn clear(names: &[&'static str]) -> Self {
            let saved = names
                .iter()
                .map(|name| (*name, std::env::var_os(*name)))
                .collect();
            for name in names {
                std::env::remove_var(*name);
            }
            Self(saved)
        }

        fn set(&self, name: &str, value: &str) {
            std::env::set_var(name, value);
        }
    }

    impl Drop for CredentialEnvRestore {
        fn drop(&mut self) {
            for (name, value) in &self.0 {
                match value {
                    Some(value) => std::env::set_var(*name, value),
                    None => std::env::remove_var(*name),
                }
            }
        }
    }

    #[test]
    fn shipped_curie_yaml_example_plans_for_apply() {
        let _lock = crate::PROCESS_ENV_LOCK.blocking_lock();
        let names = ["ANTHROPIC_API_KEY", "SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"];
        let env = CredentialEnvRestore::clear(&names);
        for name in names {
            env.set(name, "example credential");
        }

        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/curie.yaml");
        let cfg = Installation::load(&path).expect("the shipped curie.yaml example must parse");

        assert_eq!(cfg.install.namespace, "acme-bot");
        assert_eq!(cfg.install.release, "acme-bot");
        assert_eq!(
            cfg.helm_sets(),
            vec!["ui.deploy=false", "inference.deploy=false"]
        );
        assert_eq!(
            cfg.egress_hosts().first().map(String::as_str),
            Some("anthropic")
        );
        assert_eq!(cfg.credential_names(), names.map(str::to_string));

        plan_installation(cfg, true)
            .expect("the shipped curie.yaml example must plan through curie apply");
    }

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

    fn desired_from_local(
        local: LocalInstallationPlan,
        live: &serde_json::Value,
    ) -> BTreeMap<String, String> {
        let LocalInstallationPlan {
            cfg,
            resolved,
            up,
            github_token,
        } = local;
        let up =
            crate::ops::complete_up_opts(up, Some(live), github_token.as_deref(), false, false)
                .expect("complete desired values");
        let mut desired = crate::ops::up_value_plan(&up).effective_values();
        if cfg.credentials.model.is_some() {
            desired
                .entry(crate::ops::FAKE_MODEL_KEY.to_string())
                .or_insert_with(|| "false".to_string());
        }
        if let Some(slack) = &cfg.comms.slack {
            desired.insert(
                "dispatcher.slack.appToken".to_string(),
                resolved.get(&slack.app_token).cloned().unwrap_or_default(),
            );
            desired.insert(
                "dispatcher.slack.botToken".to_string(),
                resolved.get(&slack.bot_token).cloned().unwrap_or_default(),
            );
            desired.insert("worker.slackApiBaseUrl".to_string(), String::new());
        }
        desired
    }

    #[test]
    fn every_lenient_desired_map_difference_is_disclosed_as_unknown() {
        let _lock = crate::PROCESS_ENV_LOCK.blocking_lock();
        let names = [
            "CURIE_1426_MODEL_CREDENTIAL",
            "CURIE_1426_GITHUB_CREDENTIAL",
            "CURIE_1426_SLACK_APP_CREDENTIAL",
            "CURIE_1426_SLACK_BOT_CREDENTIAL",
        ];
        let env = CredentialEnvRestore::clear(&names);
        let cfg = Installation::parse(concat!(
            "version: 1\n",
            "install:\n",
            "  namespace: acme\n",
            "  release: acme\n",
            "credentials:\n",
            "  model: CURIE_1426_MODEL_CREDENTIAL\n",
            "  github_token: CURIE_1426_GITHUB_CREDENTIAL\n",
            "comms:\n",
            "  slack:\n",
            "    app_token: CURIE_1426_SLACK_APP_CREDENTIAL\n",
            "    bot_token: CURIE_1426_SLACK_BOT_CREDENTIAL\n",
        ))
        .expect("configuration parses");
        let live = serde_json::json!({
            "agentSandbox": {"runner": {"credentials": "model live", "fakeModel": false}},
            "api": {"githubToken": "github live"},
            "dispatcher": {"slack": {"appToken": "app live", "botToken": "bot live"}}
        });

        let (lenient_local, missing) =
            plan_installation_lenient(cfg.clone()).expect("lenient planning must answer");
        assert_eq!(missing, names.map(str::to_string));
        let lenient = desired_from_local(lenient_local, &live);

        env.set("CURIE_1426_MODEL_CREDENTIAL", "model replacement");
        env.set("CURIE_1426_GITHUB_CREDENTIAL", "github replacement");
        env.set("CURIE_1426_SLACK_APP_CREDENTIAL", "app replacement");
        env.set("CURIE_1426_SLACK_BOT_CREDENTIAL", "bot replacement");
        let strict = desired_from_local(
            plan_installation(cfg.clone(), false).expect("strict planning must resolve"),
            &live,
        );

        assert_eq!(
            lenient.get("agentSandbox.runner.fakeModel"),
            strict.get("agentSandbox.runner.fakeModel")
        );

        let all_keys = lenient
            .keys()
            .chain(strict.keys())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let differing = all_keys
            .into_iter()
            .filter(|key| lenient.get(key) != strict.get(key))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            differing,
            [
                "agentSandbox.runner.credentials",
                "api.githubToken",
                "dispatcher.slack.appToken",
                "dispatcher.slack.botToken",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        );

        let mut entries = diff_plan(&lenient, Some(&live));
        disclose_unresolved_credentials(&mut entries, &cfg, &missing);
        let unknown = entries
            .iter()
            .filter(|entry| entry.kind == DiffKind::Unknown)
            .map(|entry| entry.key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unknown, differing);
        for entry in entries
            .iter()
            .filter(|entry| entry.kind == DiffKind::Unknown)
        {
            let expected = match entry.key.as_str() {
                "agentSandbox.runner.credentials" => "CURIE_1426_MODEL_CREDENTIAL",
                "api.githubToken" => "CURIE_1426_GITHUB_CREDENTIAL",
                "dispatcher.slack.appToken" => "CURIE_1426_SLACK_APP_CREDENTIAL",
                "dispatcher.slack.botToken" => "CURIE_1426_SLACK_BOT_CREDENTIAL",
                key => panic!("unexpected unknown key {key}"),
            };
            assert_eq!(entry.unresolved_credential.as_deref(), Some(expected));
        }
    }

    #[tokio::test]
    async fn explicit_model_credential_set_survives_the_environment() {
        let local = {
            let _lock = crate::PROCESS_ENV_LOCK.lock().await;
            let env = CredentialEnvRestore::clear(&["CURIE_MODEL_CREDENTIALS"]);
            env.set(
                "CURIE_MODEL_CREDENTIALS",
                "model credential from environment",
            );
            let cfg = Installation::parse(concat!(
                "version: 1\n",
                "install:\n",
                "  namespace: acme\n",
                "  release: acme\n",
                "credentials:\n",
                "  model: CURIE_MODEL_CREDENTIALS\n",
                "set:\n",
                "  agentSandbox.runner.credentials: model credential from set\n",
            ))
            .expect("configuration parses");
            plan_installation(cfg, true).expect("installation plans")
        };

        // `Skip`: this fixture's `up.chart` is empty, so a probe would shell out
        // to a real `kubectl` and `helm template ""`.
        let plan = complete_installation_plan(local, StatefulProbe::Skip)
            .await
            .expect("completed plan");

        assert_eq!(
            plan.desired
                .get(crate::ops::MODEL_CREDENTIAL_KEY)
                .map(String::as_str),
            Some("model credential from set"),
            "the explicit set value must remain in the desired map"
        );
    }

    #[test]
    fn an_unresolved_declared_github_change_never_reports_zero_changes() {
        let _lock = crate::PROCESS_ENV_LOCK.blocking_lock();
        let env = CredentialEnvRestore::clear(&["CURIE_1426_GITHUB_CREDENTIAL"]);
        let cfg = Installation::parse(concat!(
            "version: 1\n",
            "install:\n",
            "  namespace: acme\n",
            "  release: acme\n",
            "credentials:\n",
            "  github_token: CURIE_1426_GITHUB_CREDENTIAL\n",
        ))
        .expect("configuration parses");
        let live = serde_json::json!({
            "api": {"githubToken": "github live"},
            "ui": {"service": {"type": "NodePort"}},
            "langfuse": {"web": {"service": {"type": "NodePort"}}}
        });
        let (lenient_local, missing) =
            plan_installation_lenient(cfg.clone()).expect("lenient planning must answer");
        assert_eq!(missing, vec!["CURIE_1426_GITHUB_CREDENTIAL".to_string()]);
        let lenient = desired_from_local(lenient_local, &live);

        env.set("CURIE_1426_GITHUB_CREDENTIAL", "github replacement");
        let strict = desired_from_local(
            plan_installation(cfg.clone(), false).expect("strict planning must resolve"),
            &live,
        );
        let differing = lenient
            .keys()
            .chain(strict.keys())
            .filter(|key| lenient.get(*key) != strict.get(*key))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            differing,
            ["api.githubToken".to_string()].into_iter().collect()
        );

        let mut entries = diff_plan(&lenient, Some(&live));
        disclose_unresolved_credentials(&mut entries, &cfg, &missing);
        let out = DiffOutput {
            unresolved_credentials: vec!["CURIE_1426_GITHUB_CREDENTIAL".to_string()],
            namespace: "acme".to_string(),
            release: "acme".to_string(),
            release_exists: true,
            chart_deployed: Some("curie-0.6.0".to_string()),
            chart_target: "0.6.0".to_string(),
            entries,
            stateful_removals: Vec::new(),
            migration: None,
        };

        let github_entry = out
            .entries
            .iter()
            .find(|entry| entry.key == "api.githubToken")
            .expect("the declared GitHub token must remain in the diff");
        assert_eq!(github_entry.kind, DiffKind::Unknown);
        assert_eq!(
            github_entry.unresolved_credential.as_deref(),
            Some("CURIE_1426_GITHUB_CREDENTIAL")
        );
        let extra: Vec<&str> = out
            .entries
            .iter()
            .filter(|entry| entry.kind != DiffKind::Same && entry.kind != DiffKind::Preserved)
            .map(|entry| entry.key.as_str())
            .collect();
        assert!(
            extra.contains(&"api.githubToken"),
            "unresolved github must remain a change: {extra:?}"
        );
        assert!(
            extra
                .iter()
                .all(|key| *key == "api.githubToken" || key.starts_with("config.")),
            "unresolved github must not grow unrelated non-config changes: {extra:?}"
        );
        assert!(
            out.changes() >= 1,
            "an unresolved declared GitHub change must never report zero changes"
        );
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

    #[test]
    fn an_unresolved_declared_model_credential_without_a_release_is_unknown() {
        let cfg = Installation::parse(concat!(
            "version: 1\n",
            "install:\n",
            "  namespace: acme\n",
            "  release: acme\n",
            "credentials:\n",
            "  model: CURIE_1426_MODEL_CREDENTIAL\n",
        ))
        .expect("configuration parses");
        let desired =
            BTreeMap::from([("agentSandbox.runner.credentials".to_string(), String::new())]);
        let mut entries = diff_plan(&desired, None);
        disclose_unresolved_credentials(
            &mut entries,
            &cfg,
            &["CURIE_1426_MODEL_CREDENTIAL".to_string()],
        );
        let entry = entries
            .iter()
            .find(|entry| entry.key == "agentSandbox.runner.credentials")
            .expect("the declared model credential must remain in the diff");
        assert_eq!(entry.kind, DiffKind::Unknown);
        assert_eq!(entry.to, None);
        assert_eq!(
            entry.unresolved_credential.as_deref(),
            Some("CURIE_1426_MODEL_CREDENTIAL")
        );
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
            unresolved_credentials: Vec::new(),
            namespace: "acme-bot".into(),
            release: "acme-bot".into(),
            release_exists: true,
            chart_deployed: Some("curie-0.5.1".into()),
            chart_target: "0.6.0".into(),
            entries: vec![],
            stateful_removals: Vec::new(),
            migration: None,
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
            unresolved_credentials: Vec::new(),
            namespace: "acme-bot".into(),
            release: "acme-bot".into(),
            release_exists: true,
            chart_deployed: Some("curie-0.6.0".into()),
            chart_target: "0.6.0".into(),
            entries: vec![],
            stateful_removals: Vec::new(),
            migration: None,
        };
        assert!(!out.chart_version_differs());
    }

    /// An unreadable chart version must not fabricate a mismatch.
    #[test]
    fn an_unknown_deployed_chart_does_not_claim_a_mismatch() {
        let out = DiffOutput {
            unresolved_credentials: Vec::new(),
            namespace: "acme-bot".into(),
            release: "acme-bot".into(),
            release_exists: false,
            chart_deployed: None,
            chart_target: "0.6.0".into(),
            entries: vec![],
            stateful_removals: Vec::new(),
            migration: None,
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
            unresolved_credentials: Vec::new(),
            namespace: "acme".into(),
            release: "acme".into(),
            release_exists: true,
            chart_deployed: Some("curie-0.6.0".into()),
            chart_target: "0.6.0".into(),
            entries: diff_plan(
                &BTreeMap::from([("ui.deploy".to_string(), "false".to_string())]),
                Some(&live(serde_json::json!({"ui": {"deploy": true}}))),
            ),
            stateful_removals: Vec::new(),
            migration: None,
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

    /// The library-level half of #1351's AC1, and the one invariant here that
    /// cannot be pinned through the binary: clap's `conflicts_with` rejects the
    /// pair at parse time, so a test that shells `curie` can only ever prove the
    /// PARSER refuses it. `ApplyOpts` has pub fields on a crate that also ships
    /// as a lib, so a consumer can hand `apply` the contradictory pair with clap
    /// never involved, and that is exactly the caller the refusal has to survive
    /// for. Asserted with `dry_run` so it needs no cluster; the `bail!` precedes
    /// `complete_installation_plan` either way.
    #[tokio::test]
    async fn apply_refuses_the_contradictory_flag_pair_without_clap() {
        let cfg = Installation::parse("version: 1\ninstall:\n  namespace: a\n  release: a\n")
            .expect("minimal installation parses");
        let opts = ApplyOpts {
            local: plan_installation(cfg, true).expect("plan the installation"),
            chart: "curie".to_string(),
            allow_stateful_removal: true,
            migrate_store: true,
        };

        let Err(err) = apply(opts).await else {
            panic!("contradictory intent must be refused, never resolved by picking one");
        };

        let msg = format!("{err:#}");
        assert!(
            msg.contains("--migrate-store") && msg.contains("--allow-stateful-removal"),
            "the refusal must name both colliding flags: {msg}"
        );
    }

    fn cfg_with_all_names() -> Installation {
        Installation::parse(
            "version: 1\ninstall:\n  namespace: a\n  release: a\n\
             credentials:\n  model: MODEL_KEY\n\
             comms:\n  slack:\n    app_token: APP_TOK\n    bot_token: BOT_TOK\n",
        )
        .unwrap()
    }

    /// The property a real run against a cluster proved was gone: `diff` must
    /// answer on an install whose credentials are not in place yet. That is
    /// precisely when "what would this change?" is worth asking.
    #[test]
    fn the_lenient_resolver_reports_gaps_instead_of_refusing() {
        let cfg = cfg_with_all_names();
        let (resolved, missing) =
            resolve_credentials_lenient(&cfg, &|_| Ok(None)).expect("must not refuse");
        assert!(resolved.is_empty());
        assert_eq!(missing, vec!["MODEL_KEY", "APP_TOK", "BOT_TOK"]);
    }

    /// Apply keeps refusing. Resolving BEFORE mutating is what stops a missing
    /// Slack token being discovered after the platform install already ran,
    /// leaving a half-applied cluster.
    #[test]
    fn the_strict_resolver_still_refuses_so_apply_cannot_half_apply() {
        let cfg = cfg_with_all_names();
        assert!(resolve_credentials(&cfg, &|_| Ok(None)).is_err());
    }

    /// A partial resolution reports only what is absent, in both modes.
    #[test]
    fn the_lenient_resolver_keeps_what_it_found() {
        let cfg = cfg_with_all_names();
        let (resolved, missing) =
            resolve_credentials_lenient(&cfg, &|n| Ok((n == "MODEL_KEY").then(|| "v".to_string())))
                .expect("must not refuse");
        assert_eq!(resolved.get("MODEL_KEY").map(String::as_str), Some("v"));
        assert_eq!(missing, vec!["APP_TOK", "BOT_TOK"]);
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
