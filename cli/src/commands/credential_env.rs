//! Model-credential discovery: passthrough env, the secret store, and `.env` files.

use super::*;

/// Walk up from the current directory to the repo root: the nearest ancestor
/// that contains `runner/Dockerfile`.
///
/// `pub(crate)` for `build_image` and `local::build_source_images` (#1915),
/// which build the dev stack's images from the same root and want the same
/// "are we in a checkout" answer rather than a second walk with its own
/// anchor file.
pub(crate) fn find_repo_root() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join("runner/Dockerfile").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Pick the model-credential env vars to forward into the runner container, BY
/// NAME (docker reads their values from the caller's env; no secret is put in
/// argv). Mirrors the worker docker substrate's positive single-credential
/// selection (apps/worker/src/curie_worker/sandbox/docker.py:199-207), which is
/// the authority this function mirrors. Three states:
/// - `fake_model`: forward NONE, and fake dominates every other state -- a fake
///   runner resolves no Anthropic credential, and a real token must not sit in an
///   untrusted, egress-rail-less container readable via /proc/1/environ.
/// - an explicit non-empty CURIE_CREDENTIALS (`byo_credential`): the operator's
///   chosen BYO credential, forwarded ALONE so an ambient SDK token can neither
///   shadow it nor ride into the sandbox. Kept under a `base_url_override` when it
///   is a provider key -- the runner routes an sk-or- OpenRouter key into
///   ANTHROPIC_API_KEY with a preset base URL, so dropping it would break BYO
///   OpenRouter -- but DROPPED under an override when it is OAuth-shaped
///   (`sk-ant-oat`): the runner blanks such a token behind an override
///   (runner sdk_auth.resolve_sdk_env), so forwarding it authenticates nothing and
///   only lands a real token in the container's /proc/1/environ (issue #603).
/// - otherwise: the ambient SDK creds for the legacy real-Anthropic path, each
///   only when `ambient_present` reports it, and only when there is no
///   `base_url_override` -- a local endpoint needs no real Anthropic token.
///
/// The rule is frozen as data in tests/vectors/model-credential-forwarding.json,
/// which both this lane and the worker lane assert against: changing the rule
/// here without changing the worker (or the vectors) fails that gate (issue #495).
pub(super) fn select_passthrough_env(
    fake_model: bool,
    base_url_override: bool,
    byo_credential: Option<&str>,
    ambient_present: &dyn Fn(&str) -> bool,
) -> Vec<String> {
    if fake_model {
        return Vec::new();
    }
    if let Some(cred) = byo_credential.filter(|c| !c.is_empty()) {
        // An OAuth-shaped token under a base-URL override authenticates nothing
        // (the runner blanks it), so drop it rather than leave a real token inert
        // in /proc/1/environ; a provider key is still routed and kept (issue #603).
        if base_url_override && cred.starts_with(OAUTH_TOKEN_PREFIX) {
            return Vec::new();
        }
        return vec!["CURIE_CREDENTIALS".into()];
    }
    if base_url_override {
        return Vec::new();
    }
    ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]
        .into_iter()
        .filter(|name| ambient_present(name))
        .map(String::from)
        .collect()
}

/// What `skill up` will run the model as, for one panel row and one warning.
///
/// The gap this closes: `select_passthrough_env` above resolves the model
/// credential AT BOOT, and `skill up` then said nothing about it. A first-run
/// user who had not exported anything got a clean boot panel, ran the very
/// command that panel recommends, and only then hit
/// `model-credential-rejected` from the provider -- one command after the CLI
/// already knew. The README does say to export `CURIE_CREDENTIALS` first, but
/// nothing in `init`'s `Next:` hint or this panel repeats it, so following the
/// CLI rather than the README walks straight into it.
///
/// Names only, never values: the masking rule in cli/CLAUDE.md applies here as
/// everywhere, and a name is all that is diagnostic anyway.
pub(super) fn model_credential_summary(
    fake_model: bool,
    local_model: Option<&str>,
    names: &[String],
) -> (String, Option<String>) {
    // Local model first: `--local-model` overrides `--fake-model` at the call
    // site, so checking fake first would mislabel a run that set both.
    if let Some(model) = local_model {
        return (format!("local ollama ({model})"), None);
    }
    if fake_model {
        return ("fake (offline, scripted replies)".to_string(), None);
    }
    if !names.is_empty() {
        return (names.join(" + "), None);
    }
    (
        "none".to_string(),
        Some(
            "no model credential resolved, so `curie skill message` will fail with \
             model-credential-rejected. Either export one (CURIE_CREDENTIALS, \
             ANTHROPIC_API_KEY, or CLAUDE_CODE_OAUTH_TOKEN) and re-run `curie skill up \
             --replace`, or re-run with `--fake-model` to drive the loop offline."
                .to_string(),
        ),
    )
}

/// A Claude Code OAuth token shares the sk-ant- prefix with an API key; this more
/// specific prefix marks it (issue #603). A literal mirror of
/// runner/src/curie_runner/sdk_auth.py::OAUTH_TOKEN_PREFIX, the authority for the
/// prefix semantics, and of the worker lane's `_OAUTH_TOKEN_PREFIX`.
pub(super) const OAUTH_TOKEN_PREFIX: &str = "sk-ant-oat";

/// Append `--secret` env var NAMES to the model-credential passthrough list,
/// de-duplicating. Unlike the model credential these are NOT suppressed under a
/// fake/local model run: a bundle's authed MCP server needs its token
/// regardless of which model drives the session. Names already present (a user
/// passing a model-credential var as a secret) are not duplicated.
///
/// Also the union used for the connector-owned secret NAMES resolved from a
/// bundle's `connectors.yaml` (#2503): same order-preserving dedupe, so the
/// explicit `--secret` order wins and an owned name already named by a flag is
/// bound once.
pub fn merge_secret_env(mut passthrough: Vec<String>, secrets: &[String]) -> Vec<String> {
    for name in secrets {
        if !passthrough.contains(name) {
            passthrough.push(name.clone());
        }
    }
    passthrough
}

/// Is `name` exported with a usable value?
///
/// An empty-string credential is absent, not supplied (issue #540): `var_os`
/// alone reports `NAME=""` as present, which would suppress the vault fallback
/// and forward nothing usable. Mirrors `ops.rs::resolve_up_credentials` and
/// `interactive.rs::env_credential_present`.
pub(crate) fn env_credential_present(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.is_empty())
}

pub(crate) fn secret_store_env(name: &str) -> Result<Option<(String, String)>> {
    if env_credential_present(name) {
        return Ok(None);
    }
    if !crate::secrets::is_saved(name)? {
        return Ok(None);
    }
    if let Some(value) = crate::secrets::get_value(name)? {
        crate::ui::ui().note(&format!(
            "{name}: loaded from Curie private storage for this run"
        ));
        return Ok(Some((name.to_string(), value)));
    }
    Ok(None)
}

pub(super) fn stored_env_contains(env: &[(String, String)], name: &str) -> bool {
    env.iter().any(|(stored_name, _)| stored_name == name)
}

/// The ambient-presence rule `select_passthrough_env` selects on.
///
/// Presence must match what `StartSpec::run_args` later filters the NAMES on
/// (docker.rs:117), or selection and emission disagree.
pub(super) fn ambient_present_for(docker_env: &[(String, String)]) -> impl Fn(&str) -> bool + '_ {
    move |name| std::env::var_os(name).is_some() || stored_env_contains(docker_env, name)
}

pub(crate) fn load_model_credentials_from_secret_store() -> Result<Vec<(String, String)>> {
    // Prefer an explicitly BYO Curie credential when saved, otherwise hydrate
    // the SDK credential names in the same order `select_passthrough_env` uses.
    if env_credential_present("CURIE_CREDENTIALS") {
        return Ok(Vec::new());
    }
    if let Some(pair) = secret_store_env("CURIE_CREDENTIALS")? {
        return Ok(vec![pair]);
    }
    let mut env = Vec::new();
    if let Some(pair) = secret_store_env("CLAUDE_CODE_OAUTH_TOKEN")? {
        env.push(pair);
    }
    if let Some(pair) = secret_store_env("ANTHROPIC_API_KEY")? {
        env.push(pair);
    }
    Ok(env)
}

/// The model-credential names, in the precedence order the vault loader uses
/// (`CURIE_CREDENTIALS` dominates the SDK pair). These are the ONLY keys read
/// from an opt-in `--env-file` (#749, ADR-0070); every other key in the dotfile
/// is ignored, never absorbed into any process env.
pub const MODEL_CREDENTIAL_ENV_NAMES: [&str; 3] = [
    "CURIE_CREDENTIALS",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
];

/// Parse just the recognized model-credential names out of a dotenv file,
/// dropping every other key. An empty value is absent, not supplied (issue
/// #540), so it is dropped too. A missing/unreadable file is a hard error --
/// `--env-file` is an explicit opt-in, so pointing it at nothing is a mistake.
pub(crate) fn parse_credential_env_file(path: &Path) -> Result<Vec<(String, String)>> {
    let mut found = Vec::new();
    for item in dotenvy::from_path_iter(path)
        .with_context(|| format!("reading --env-file {}", path.display()))?
    {
        let (key, value) =
            item.with_context(|| format!("parsing --env-file {}", path.display()))?;
        if MODEL_CREDENTIAL_ENV_NAMES.contains(&key.as_str()) && !value.is_empty() {
            found.push((key, value));
        }
    }
    Ok(found)
}

/// Which parsed `.env` credentials to add, given what a higher-priority source
/// already supplied (`is_present`: shell env OR vault). Pure, so the precedence
/// (#749: shell env > vault > file) is unit-testable without touching the
/// process env. Mirrors `load_model_credentials_from_secret_store`'s shape:
/// `CURIE_CREDENTIALS` dominates and suppresses the SDK pair, matching
/// `select_passthrough_env`'s byo branch.
pub(crate) fn resolve_env_file_credentials(
    parsed: &[(String, String)],
    is_present: &dyn Fn(&str) -> bool,
) -> Vec<(String, String)> {
    let take = |name: &str| -> Option<(String, String)> {
        if is_present(name) {
            return None;
        }
        parsed
            .iter()
            .find(|(key, _)| key == name)
            .map(|(key, value)| (key.clone(), value.clone()))
    };
    if let Some(pair) = take("CURIE_CREDENTIALS") {
        return vec![pair];
    }
    // A BYO credential from a higher source dominates: the SDK pair is never
    // forwarded alongside it, so do not pull it from the file either.
    if is_present("CURIE_CREDENTIALS") {
        return Vec::new();
    }
    ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]
        .into_iter()
        .filter_map(take)
        .collect()
}

/// Load model credentials from an opt-in bundle `.env` as the LOWEST-priority
/// source (#749, ADR-0070). `already` is the vault-hydrated `docker_env`, so a
/// name supplied by the shell env or the vault always wins; only a name missing
/// from both is taken from the file.
pub(super) fn load_model_credentials_from_env_file(
    env_file: Option<&Path>,
    already: &[(String, String)],
) -> Result<Vec<(String, String)>> {
    let Some(path) = env_file else {
        return Ok(Vec::new());
    };
    let parsed = parse_credential_env_file(path)?;
    let is_present =
        |name: &str| env_credential_present(name) || stored_env_contains(already, name);
    let resolved = resolve_env_file_credentials(&parsed, &is_present);
    for (name, _) in &resolved {
        crate::ui::ui().note(&format!(
            "{name}: loaded from --env-file {} for this run",
            path.display()
        ));
    }
    Ok(resolved)
}
