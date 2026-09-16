//! Model-credential and provider egress resolution: which provider a
//! credential names, which hosts it needs, and the CIDRs the NetworkPolicy
//! is opened to.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;

#[allow(unused_imports)]
use super::{command::*, up::*, verbs::*};

// ---------------------------------------------------------------------------
// Command builders (pure; unit-tested below)
// ---------------------------------------------------------------------------

/// Egress port shared by every runner allowlist entry (provider + web): TLS only.
pub(super) const EGRESS_TCP_PORT: u16 = 443;

/// Resolve the model credential `up` installs with. `--fake-model` forces the
/// sealed install regardless of the environment; otherwise a non-empty
/// credential value enables the real model.
pub fn resolve_up_credentials(fake_model: bool, env_value: Option<String>) -> Option<String> {
    if fake_model {
        return None;
    }
    env_value.filter(|v| !v.is_empty())
}

/// The operator's model credential from the shell for `cluster up`, canonically
/// `CURIE_CREDENTIALS` -- the same name the runtime plane (runner/worker/chart)
/// uses everywhere. The CLI's historical `CURIE_MODEL_CREDENTIALS` is accepted
/// as a deprecated alias for one release, with a warning naming the replacement,
/// so an operator who set the one name for `skill up` isn't met with a silent
/// no-op at `cluster up` (#496). Private storage is the final fallback. Returns
/// None when no source has a nonempty value.
pub fn model_credential_env() -> Result<Option<String>> {
    if let Some(value) = std::env::var("CURIE_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        return Ok(Some(value));
    }
    if let Some(value) = std::env::var("CURIE_MODEL_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        eprintln!(
            "warning: CURIE_MODEL_CREDENTIALS is deprecated and will be removed in a future \
             release; set CURIE_CREDENTIALS instead."
        );
        return Ok(Some(value));
    }
    match crate::commands::secret_store_env("CURIE_CREDENTIALS") {
        Ok(stored) => Ok(stored.map(|(_, value)| value)),
        Err(error) => {
            crate::ui::ui().warn(&format!(
                "Saved model credentials could not be read; continuing without them: {error}"
            ));
            Ok(None)
        }
    }
}

/// The helm value key that pins the sandbox runner model in the chart.
pub(super) const RUNNER_MODEL_KEY: &str = "agentSandbox.runner.model";

pub(crate) const INFERENCE_PERSISTENCE_ENABLED_KEY: &str = "inference.persistence.enabled";
pub(crate) const INFERENCE_PULL_MODEL_KEY: &str = "inference.pullModel";
pub(crate) const INFERENCE_DEPLOY_KEY: &str = "inference.deploy";
pub(crate) const INFERENCE_MODEL_KEY: &str = "inference.model";

pub(crate) fn inference_asset_policy_is_safe(
    persistence_enabled: Option<bool>,
    pull_model: Option<bool>,
) -> bool {
    persistence_enabled == Some(true) || pull_model == Some(false)
}

/// The value of the last explicit `--set agentSandbox.runner.model=VAL` in
/// `set`, if the operator passed one (last wins, matching helm precedence).
/// Helm accepts comma-joined `--set a=1,b=2`, so each element is split on `,`
/// (mirroring `operator_set_keys`) before the prefix match — a runner model
/// pinned alongside other keys is detected, and a trailing key after the model
/// assignment is not swallowed into the value.
pub(super) fn explicit_runner_model(set: &[String]) -> Option<&str> {
    let prefix = format!("{RUNNER_MODEL_KEY}=");
    // `strip_prefix` returns a slice of `part` (borrowing `set`), not of
    // `prefix`, so the returned borrow outlives the temporary `prefix`.
    set.iter()
        .flat_map(|s| s.split(','))
        .filter_map(|part| part.strip_prefix(&prefix))
        .next_back()
}

/// Fail loud when the shell `CURIE_MODEL` and an explicit
/// `--set agentSandbox.runner.model=` disagree, so the runner model is never
/// silently ambiguous (#361).
pub fn check_runner_model_conflict(model: Option<&str>, set: &[String]) -> Result<()> {
    if let (Some(y), Some(x)) = (model, explicit_runner_model(set)) {
        if x != y {
            bail!(
                "conflicting sandbox runner model: CURIE_MODEL=`{y}` but \
                 `--set {RUNNER_MODEL_KEY}={x}` was also passed. Remove one so the \
                 runner model is unambiguous."
            );
        }
    }
    Ok(())
}

/// Reject supplying the GitHub credential through BOTH the private input and the
/// argv `--set` pass-through. Silently letting `--set` win would discard the
/// operator's protected input AND leak the `--set` value into the process table,
/// which is the exact defect #1124 exists to close, so this is a usage error
/// rather than a precedence rule.
pub fn check_github_token_conflict(flag: Option<&str>, clear: bool, set: &[String]) -> Result<()> {
    let explicit = clear || flag.is_some_and(|v| !v.is_empty());
    if explicit && operator_set_keys(set).contains(GITHUB_TOKEN_KEY) {
        bail!(
            "conflicting GitHub credential: `--set {GITHUB_TOKEN_KEY}=` was passed \
             alongside `--github-token` / `--clear-github-token`. Remove the \
             `--set`: it puts the complete token in the process table and shell \
             history, which the dedicated input exists to avoid."
        );
    }
    Ok(())
}

/// Validate every input that must fail before the installer reads cluster
/// state. `curie apply` and `curie diff` call this through their shared local
/// planner, while `cluster up` keeps the same validation before its own read.
pub(crate) fn validate_up_inputs(
    opts: &UpOpts,
    github_token: Option<&str>,
    clear_github_token: bool,
) -> Result<()> {
    validate_local_model_asset_policy(opts)?;
    validate_web_egress_cidrs(&opts.allow_web_egress)
        .context("invalid --allow-web-egress value")?;
    let operator_sets = opts.operator_sets();
    check_runner_model_conflict(opts.model.as_deref(), &operator_sets)?;
    check_github_token_conflict(github_token, clear_github_token, &operator_sets)?;
    for host in &opts.allow_egress_host {
        parse_egress_provider(host)?;
    }
    Ok(())
}

fn last_typed_bool(sets: &[String], key: &str) -> Option<bool> {
    operator_set_entries(sets)
        .into_iter()
        .filter_map(|(candidate, value)| (candidate.trim() == key).then_some(value.trim()))
        .next_back()
        .and_then(|value| match value {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
}

fn validate_local_model_asset_policy(opts: &UpOpts) -> Result<()> {
    if opts.local_model.is_none() {
        return Ok(());
    }
    let persistence_enabled = last_typed_bool(&opts.set, INFERENCE_PERSISTENCE_ENABLED_KEY);
    let pull_model = last_typed_bool(&opts.set, INFERENCE_PULL_MODEL_KEY);
    if inference_asset_policy_is_safe(persistence_enabled, pull_model) {
        return Ok(());
    }

    Err(crate::exit::CliError::usage(
        "`--local-model` requires an explicit model-weight policy: pass \
         `--set inference.persistence.enabled=true` to pull weights into persistent storage, \
         or pass `--set inference.pullModel=false` when the model is already present; \
         `--set-string` cannot express the required typed boolean policy",
    )
    .with_fix(
        "re-run with `--set inference.persistence.enabled=true` or \
         `--set inference.pullModel=false`",
    )
    .into())
}

/// Validate every operator-supplied `--allow-web-egress` value is a real CIDR
/// (`addr/prefix`) before it is interpolated into a `helm --set` argument. A
/// value containing a comma or `=` would otherwise be split by helm into
/// multiple `--set` assignments and could overwrite the model rule at index
/// `[0]`; requiring a parseable `IpAddr` plus an in-range prefix naturally
/// rejects those (and whitespace) because they fail to parse.
pub fn validate_web_egress_cidrs(cidrs: &[String]) -> Result<()> {
    for cidr in cidrs {
        let (addr, prefix) = cidr.split_once('/').ok_or_else(|| {
            anyhow::anyhow!("`--allow-web-egress` value `{cidr}` is not a CIDR (expected addr/prefix, e.g. 10.0.0.0/8)")
        })?;
        let ip: std::net::IpAddr = addr.parse().map_err(|_| {
            anyhow::anyhow!(
                "`--allow-web-egress` value `{cidr}` has an unparseable address `{addr}`"
            )
        })?;
        let bits: u8 = prefix.parse().map_err(|_| {
            anyhow::anyhow!(
                "`--allow-web-egress` value `{cidr}` has an unparseable prefix `{prefix}`"
            )
        })?;
        let max = if ip.is_ipv4() { 32 } else { 128 };
        if bits > max {
            bail!("`--allow-web-egress` value `{cidr}` has an out-of-range prefix `/{bits}` (max /{max})");
        }
    }
    Ok(())
}

/// A CIDR is a *default route* when its prefix length is `/0` (`0.0.0.0/0`,
/// `::/0`, or any `addr/0`) -- a `/0` prefix ignores the address bits entirely
/// and matches the whole address space. Opening runner egress to such a route
/// removes the chart's default-deny internet rail. Assumes the value already
/// passed `validate_web_egress_cidrs`.
pub fn is_default_route(cidr: &str) -> bool {
    cidr.rsplit_once('/')
        .and_then(|(_, prefix)| prefix.trim().parse::<u8>().ok())
        .is_some_and(|bits| bits == 0)
}

/// The distinct rail-removal warning to emit when the web-egress allowlist
/// contains one or more default routes, or `None` when it does not. Returned as
/// a pure value (not printed here) so the warning text stays unit-testable
/// independently of the `up` handler's UI side effects.
pub fn default_route_egress_warning(cidrs: &[String]) -> Option<String> {
    let routes: Vec<&str> = cidrs
        .iter()
        .map(String::as_str)
        .filter(|c| is_default_route(c))
        .collect();
    if routes.is_empty() {
        return None;
    }
    Some(format!(
        "`--allow-web-egress` includes a default route ({}); this removes the egress rail -- the sandbox can reach the entire internet",
        routes.join(", ")
    ))
}

/// Credential prefixes whose runtime routing selects one unambiguous provider.
/// Keep this aligned with `runner/src/curie_runner/sdk_auth.py`: credentials
/// outside these exact prefixes do not carry enough information to infer an
/// egress destination.
const CREDENTIAL_PREFIX_PROVIDERS: &[(&str, &str)] =
    &[("sk-ant-", "anthropic"), ("sk-or-", "openrouter")];

/// Return the provider unambiguously selected by a credential prefix.
///
/// Callers that inspect a credential must discard it after deriving this
/// non-secret provider name; it is safe to render the returned value but never
/// the credential itself.
pub fn provider_from_credential_prefix(credential: &str) -> Option<&'static str> {
    CREDENTIAL_PREFIX_PROVIDERS
        .iter()
        .find(|(prefix, _)| credential.starts_with(prefix))
        .map(|(_, provider)| *provider)
}

/// The canonical model providers `--allow-egress-host` accepts, each paired with
/// the API hostname(s) its runner must reach, in the order shown in help and
/// error text. The single source of truth for both the accepted-provider set and
/// their egress hosts.
///
/// This set is deliberately limited to the providers the runner can drive
/// end to end today. Opening egress to a host the runner cannot actually talk to
/// gives false confidence, so a provider is only listed once the runner has
/// runtime support for it. Native OpenAI and Gemini remain unsupported here.
///
/// HOSTNAMES, never CIDRs: provider IPs rotate, so they are resolved to narrow
/// host routes at install time (see [`resolve_provider_egress_cidrs`]) instead of
/// baked into this binary where a stale literal would silently break a real model
/// call.
const EGRESS_PROVIDERS: &[(&str, &[&str])] = &[
    ("anthropic", &["api.anthropic.com"]),
    ("openrouter", &["openrouter.ai"]),
    ("zhipu", &["api.z.ai"]),
    ("moonshot", &["api.moonshot.ai"]),
    ("deepseek", &["api.deepseek.com"]),
];

/// The API hostname(s) a named model provider's runner must reach, or `None`
/// when the value is not one of the known providers. Lowercase-exact only, so an
/// uppercased spelling is rejected rather than silently normalized.
pub fn provider_egress_hosts(provider: &str) -> Option<&'static [&'static str]> {
    EGRESS_PROVIDERS
        .iter()
        .find(|(n, _)| *n == provider)
        .map(|(_, hosts)| *hosts)
}

/// Validate one `--allow-egress-host` value against the known providers,
/// returning its canonical `'static` name. An unknown value is a deterministic
/// input error (exit 2 / Usage) that enumerates the accepted providers and
/// points at the `--allow-web-egress` escape hatch for arbitrary destinations.
pub fn parse_egress_provider(value: &str) -> Result<&'static str, crate::exit::CliError> {
    EGRESS_PROVIDERS
        .iter()
        .find(|(n, _)| *n == value)
        .map(|(n, _)| *n)
        .ok_or_else(|| {
            let known = EGRESS_PROVIDERS
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join(", ");
            crate::exit::CliError::usage(format!(
                "`--allow-egress-host` value `{value}` is not a known provider (expected one of: {known})"
            ))
            .with_fix(
                "pick a named provider, or open a raw range with `--allow-web-egress <CIDR>`",
            )
        })
}

fn credential_egress_provider(credential: &str) -> Option<&'static str> {
    if credential.starts_with("sk-ant-") {
        Some("anthropic")
    } else if credential.starts_with("sk-or-") {
        Some("openrouter")
    } else {
        None
    }
}

pub(super) fn validate_credential_egress_consistency(
    opts: &UpOpts,
) -> std::result::Result<(), crate::exit::CliError> {
    let operator_sets = opts.operator_sets();
    let explicit_credential = operator_set_entries(&operator_sets)
        .into_iter()
        .filter(|(key, _)| key.trim() == MODEL_CREDENTIAL_KEY)
        .map(|(_, value)| value.trim())
        .next_back();
    let Some(detected) = explicit_credential
        .or(opts.credentials.as_deref())
        .and_then(credential_egress_provider)
    else {
        return Ok(());
    };

    if opts.allow_egress_host.is_empty()
        || opts
            .allow_egress_host
            .iter()
            .any(|provider| provider == detected)
    {
        return Ok(());
    }

    let explicit = opts
        .allow_egress_host
        .iter()
        .map(|provider| format!("--allow-egress-host {provider}"))
        .collect::<Vec<_>>()
        .join(" ");
    Err(crate::exit::CliError::usage(format!(
        "the configured model credential identifies `{detected}`, but `--allow-egress-host` permits only: {explicit}"
    ))
    .with_fix(format!(
        "include `--allow-egress-host {detected}`, or remove the contradictory provider selection"
    )))
}

/// A resolved host address as a single-host CIDR: `/32` for IPv4, `/128` for
/// IPv6. The egress rule opens exactly that address, nothing wider.
pub fn ip_to_egress_cidr(ip: std::net::IpAddr) -> String {
    let prefix = if ip.is_ipv4() { 32 } else { 128 };
    format!("{ip}/{prefix}")
}

/// Whether a resolved provider address is safe to open a runner egress route to:
/// a globally-routable unicast address. A poisoned or split-horizon DNS answer
/// that maps a provider host to the node metadata endpoint or any internal /
/// overlay host must never mint an egress /32 -- the chart emits no
/// metadataExcept for an exact-host allow, so this predicate is the only guard.
///
/// This is a COMPREHENSIVE denylist that mirrors, by hand, the special-use
/// ranges excluded by `std`'s `Ipv4Addr::is_global`/`Ipv6Addr::is_global` --
/// those APIs are still unstable, so we cannot call them and a partial denylist
/// would give false assurance. Every non-global-unicast range is rejected,
/// including ones reachable on internal/overlay networks (CGNAT, benchmarking,
/// reserved/future) that the earlier selective list let slip through.
fn is_globally_routable_egress(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            // Reject if the address falls in ANY special-use / non-global range.
            let non_global = o[0] == 0                        // 0.0.0.0/8 "this host on this network"
                || v4.is_private()                            // 10/8, 172.16/12, 192.168/16
                || (o[0] == 100 && (o[1] & 0xc0) == 0x40)     // CGNAT 100.64.0.0/10 (RFC6598)
                || v4.is_loopback()                           // 127.0.0.0/8
                || v4.is_link_local()                         // 169.254.0.0/16 (incl. IMDS 169.254.169.254)
                || (o[0] == 192 && o[1] == 0 && o[2] == 0)    // IETF protocol assignments 192.0.0.0/24
                || v4.is_documentation()                      // 192.0.2/24, 198.51.100/24, 203.0.113/24
                || (o[0] == 192 && o[1] == 88 && o[2] == 99)  // 6to4 relay anycast 192.88.99.0/24
                || (o[0] == 198 && (o[1] & 0xfe) == 18)       // benchmarking 198.18.0.0/15 (RFC2544)
                || o[0] >= 240                                // reserved/future 240.0.0.0/4 (incl. 255.255.255.255 broadcast)
                || v4.is_multicast()                          // 224.0.0.0/4
                || v4.is_unspecified()                        // 0.0.0.0 (belt-and-suspenders; covered by o[0]==0)
                || v4.is_broadcast(); // 255.255.255.255 (belt-and-suspenders; covered by o[0]>=240)
            !non_global
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() {
                return false;
            }
            // Map an IPv4-mapped v6 back to v4 and re-check.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_globally_routable_egress(IpAddr::V4(v4));
            }
            let seg = v6.segments();
            let is_ula = (seg[0] & 0xfe00) == 0xfc00; // fc00::/7
            let is_link_local = (seg[0] & 0xffc0) == 0xfe80; // fe80::/10
            let is_documentation = seg[0] == 0x2001 && seg[1] == 0x0db8; // 2001:db8::/32
            !(is_ula || is_link_local || is_documentation)
        }
    }
}

/// Resolve each named provider's API host(s) to single-host egress CIDRs. The
/// resolver is injected so the pure logic (dedup, sort, empty/error handling) is
/// unit-testable without touching real DNS. An unknown provider, a resolver
/// failure, or a host that resolves to no addresses is a hard error naming the
/// host -- never a silent skip, which would leave a real model call failing
/// closed with no clue why. The result is deduplicated and sorted so the install
/// argv is stable across runs.
pub fn resolve_provider_egress_cidrs(
    providers: &[String],
    resolve: impl Fn(&str) -> std::io::Result<Vec<std::net::IpAddr>>,
) -> Result<Vec<String>> {
    let mut cidrs = Vec::new();
    for p in providers {
        let hosts = provider_egress_hosts(p)
            .ok_or_else(|| anyhow::anyhow!("unknown egress provider `{p}`"))?;
        for host in hosts {
            let ips = resolve(host)
                .with_context(|| format!("resolving egress host {host} for provider {p}"))?;
            if ips.is_empty() {
                bail!("egress host {host} (provider {p}) resolved to no addresses");
            }
            for ip in ips {
                if !is_globally_routable_egress(ip) {
                    bail!("egress host {host} (provider {p}) resolved to non-routable address {ip}; refusing to open an egress route (possible DNS poisoning or split-horizon)");
                }
                cidrs.push(ip_to_egress_cidr(ip));
            }
        }
    }
    cidrs.sort();
    cidrs.dedup();
    Ok(cidrs)
}

type ProviderAddressResolver = Box<dyn Fn(&str) -> std::io::Result<Vec<std::net::IpAddr>>>;

fn system_provider_address_resolver() -> ProviderAddressResolver {
    Box::new(|host| {
        use std::net::ToSocketAddrs;
        (host, 443u16)
            .to_socket_addrs()
            .map(|addresses| addresses.map(|address| address.ip()).collect())
    })
}

#[cfg(not(debug_assertions))]
fn provider_address_resolver() -> Result<ProviderAddressResolver> {
    Ok(system_provider_address_resolver())
}

#[cfg(debug_assertions)]
fn provider_address_resolver() -> Result<ProviderAddressResolver> {
    let raw = match std::env::var("CURIE_TEST_PROVIDER_EGRESS_JSON") {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(system_provider_address_resolver()),
        Err(error) => {
            return Err(anyhow::anyhow!(
                "reading CURIE_TEST_PROVIDER_EGRESS_JSON: {error}"
            ));
        }
    };

    let values: serde_json::Value = serde_json::from_str(&raw).context(
        "CURIE_TEST_PROVIDER_EGRESS_JSON must be a JSON object mapping hosts to IP lists",
    )?;
    let values = values.as_object().ok_or_else(|| {
        anyhow::anyhow!(
            "CURIE_TEST_PROVIDER_EGRESS_JSON must be a JSON object mapping hosts to IP lists"
        )
    })?;
    let mut resolved = BTreeMap::new();
    for (host, addresses) in values {
        let addresses = addresses.as_array().ok_or_else(|| {
            anyhow::anyhow!(
                "CURIE_TEST_PROVIDER_EGRESS_JSON entry for {host} must be an array of IP strings"
            )
        })?;
        let mut parsed = Vec::with_capacity(addresses.len());
        for address in addresses {
            let address = address.as_str().ok_or_else(|| {
                anyhow::anyhow!(
                    "CURIE_TEST_PROVIDER_EGRESS_JSON entry for {host} must contain only IP strings"
                )
            })?;
            parsed.push(address.parse().with_context(|| {
                format!(
                    "CURIE_TEST_PROVIDER_EGRESS_JSON entry for {host} has invalid IP address {address}"
                )
            })?);
        }
        resolved.insert(host.clone(), parsed);
    }
    Ok(Box::new(move |host| {
        resolved.get(host).cloned().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "CURIE_TEST_PROVIDER_EGRESS_JSON has no address list for declared egress host {host}"
                ),
            )
        })
    }))
}

/// Resolve provider egress through the system resolver. Debug builds may inject
/// deterministic addresses, which still pass through the routability checks in
/// [`resolve_provider_egress_cidrs`].
pub(crate) fn resolve_provider_egress_cidrs_for_current_environment(
    providers: &[String],
) -> Result<Vec<String>> {
    let resolver = provider_address_resolver()?;
    resolve_provider_egress_cidrs(providers, |host| resolver(host))
}

/// A note naming the model provider(s) whose egress `cluster up` opened, or
/// `None` when no provider was requested.
pub fn provider_egress_note(providers: &[String]) -> Option<String> {
    if providers.is_empty() {
        return None;
    }
    Some(format!(
        "real model egress opened to provider(s): {}",
        providers.join(", ")
    ))
}

/// The warning to emit when a real model credential is installed but no egress
/// was opened: the runner sandbox is fail-closed, so the model is unreachable.
/// `Some` only in that one combination (a credential present with nothing opened);
/// every other case stays silent. Names both the provider flag and the raw
/// escape hatch so the operator can fix it without reading source.
pub fn sealed_credential_warning(
    credentials_present: bool,
    any_egress_opened: bool,
) -> Option<String> {
    if credentials_present && !any_egress_opened {
        Some(
            "a real model credential is set but the sandbox is sealed -- no egress opened, so the \
             model is unreachable. Pass --allow-egress-host \
             <anthropic|openrouter|zhipu|moonshot|deepseek> \
             (or --allow-web-egress <CIDR>) and re-run."
                .to_string(),
        )
    } else {
        None
    }
}

/// Shared tail of the no-credential guidance: both the live fake-model note
/// and the dry-run fresh-install note end with exactly this text, so the two
/// paths cannot drift apart (#1898).
const NO_CREDENTIAL_GUIDANCE: &str = "Set CURIE_CREDENTIALS to an Anthropic, OpenRouter, Zhipu, \
     Moonshot, or DeepSeek credential and configure matching egress before re-running \
     `curie cluster up` to enable the real model. Provider native Zhipu, Moonshot, and \
     DeepSeek also need their matching worker runtime base URL.";

/// The ordered model+egress status lines `up` prints, as (is_warning, message)
/// pairs, derived purely so every credential/egress combination is unit-tested.
/// The web-egress *count* note and the default-route warning stay in the handler
/// (they keep their own tested helpers). `any_egress_opened` folds resolved
/// provider routes, declared web egress, and (under dry-run) the intent to open.
/// Under `--dry-run`, the no-credential arm reports that whether the model is
/// preserved is unknown offline, instead of asserting the fake-model outcome
/// (#1898).
pub fn model_egress_status_lines(
    credentials_present: bool,
    local_model: bool,
    fake_model: bool,
    providers: &[String],
    any_egress_opened: bool,
    dry_run: bool,
) -> Vec<(bool, String)> {
    let mut lines: Vec<(bool, String)> = Vec::new();
    // Past-tense provider note only on a live run; under dry-run the handler
    // prints its own "a live run resolves..." note instead.
    if !providers.is_empty() && !dry_run {
        lines.push((
            false,
            provider_egress_note(providers).expect("providers non-empty"),
        ));
        lines.push((
            false,
            "resolved provider IPs can rotate; re-run `curie cluster up` if model calls start failing".into(),
        ));
    }
    if credentials_present {
        if let Some(w) = sealed_credential_warning(true, any_egress_opened) {
            lines.push((true, w));
        }
    } else if local_model {
        lines.push((
            false,
            "local model enabled; installing the chart inference deployment".into(),
        ));
    } else if !fake_model && !dry_run {
        lines.push((
            true,
            format!(
                "no CURIE_CREDENTIALS set; installing with the fake model{}",
                if any_egress_opened {
                    ""
                } else {
                    " (model egress stays sealed)"
                }
            ),
        ));
        lines.push((
            false,
            format!("Replies will be canned. {NO_CREDENTIAL_GUIDANCE}"),
        ));
    } else if !fake_model {
        // `--dry-run` stays offline (#1898): it cannot read the release's
        // recorded model configuration the way `resolve_preserved_runner_identity_values`
        // does on the live path, so it cannot know whether a rerun would
        // preserve a real credential or land on the fake model. Asserting the
        // fake-model outcome here contradicted the live run and `cluster up
        // --help`, which is corrosive for the one preflight signal an operator
        // has before an upgrade -- so state what is unknown offline instead.
        lines.push((
            true,
            format!(
                "no CURIE_CREDENTIALS set; a live run preserves the release's recorded model \
                 configuration when there is one -- not read under --dry-run{}",
                if any_egress_opened {
                    ""
                } else {
                    "; no model egress is opened by this run"
                }
            ),
        ));
        lines.push((
            false,
            format!(
                "With nothing recorded -- a fresh install -- the release comes up on the fake \
                 model and replies will be canned. {NO_CREDENTIAL_GUIDANCE}"
            ),
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_prefix_inference_matches_the_shared_provider_registry() {
        #[derive(serde::Deserialize)]
        struct Registry {
            providers: Vec<Provider>,
        }

        #[derive(serde::Deserialize)]
        struct Provider {
            name: String,
            inferred_provider: Option<String>,
            credential_examples: Vec<String>,
        }

        let registry: Registry = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/vectors/model-provider-registry.json"
        )))
        .expect("parse provider registry");

        for provider in registry.providers {
            for credential in provider.credential_examples {
                assert_eq!(
                    provider_from_credential_prefix(&credential),
                    provider.inferred_provider.as_deref(),
                    "credential example for {}",
                    provider.name
                );
            }
        }
    }

    #[test]
    fn resolve_up_credentials_reflects_env_and_fake_model() {
        // Env set, not fake: real model.
        assert_eq!(
            resolve_up_credentials(false, Some("sk-ant-x".into())).as_deref(),
            Some("sk-ant-x")
        );
        // --fake-model wins even with a credential in the environment.
        assert_eq!(resolve_up_credentials(true, Some("sk-ant-x".into())), None);
        // Empty and absent both mean sealed.
        assert_eq!(resolve_up_credentials(false, Some(String::new())), None);
        assert_eq!(resolve_up_credentials(false, None), None);
    }

    #[test]
    fn check_runner_model_conflict_mismatch_is_err() {
        let set = vec!["agentSandbox.runner.model=sonnet".into()];
        let err = check_runner_model_conflict(Some("glm"), &set).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("glm"), "{msg}");
        assert!(msg.contains("sonnet"), "{msg}");
    }

    #[test]
    fn check_runner_model_conflict_matching_is_ok() {
        let set = vec!["agentSandbox.runner.model=glm".into()];
        assert!(check_runner_model_conflict(Some("glm"), &set).is_ok());
    }

    #[test]
    fn check_runner_model_conflict_no_env_is_ok() {
        // No CURIE_MODEL: an explicit operator set stands, no conflict.
        let set = vec!["agentSandbox.runner.model=sonnet".into()];
        assert!(check_runner_model_conflict(None, &set).is_ok());
    }

    #[test]
    fn check_runner_model_conflict_no_explicit_set_is_ok() {
        // CURIE_MODEL set, no explicit set: nothing to conflict with.
        assert!(check_runner_model_conflict(Some("glm"), &[]).is_ok());
    }

    #[test]
    fn check_runner_model_conflict_comma_joined_detects_mismatch() {
        // Helm accepts `--set a=1,b=2`; the runner model pinned alongside another
        // key must still be detected so the conflict fails loud (#361).
        let set = vec!["worker.replicas=2,agentSandbox.runner.model=glm".into()];
        let err = check_runner_model_conflict(Some("sonnet"), &set).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("sonnet"), "{msg}");
        assert!(msg.contains("glm"), "{msg}");
    }

    #[test]
    fn check_runner_model_conflict_comma_joined_model_first_matches() {
        // The model assignment leading a comma-joined element must not swallow
        // the trailing key into its value (which would falsely report a
        // conflict); a matching model is a legitimate, non-conflicting install.
        let set = vec!["agentSandbox.runner.model=glm,worker.replicas=2".into()];
        assert!(check_runner_model_conflict(Some("glm"), &set).is_ok());
    }

    #[test]
    fn validate_web_egress_cidrs_accepts_valid_and_rejects_bad() {
        // Valid IPv4 CIDR and both catch-all forms pass.
        assert!(validate_web_egress_cidrs(&["203.0.113.0/24".into()]).is_ok());
        assert!(validate_web_egress_cidrs(&["0.0.0.0/0".into()]).is_ok());
        assert!(validate_web_egress_cidrs(&["::/0".into()]).is_ok());

        // A value with a comma is rejected (would split into multiple --set).
        let err = validate_web_egress_cidrs(&[
            "10.0.0.0/8,security.networkPolicy.allowedEgress[0].cidr=0.0.0.0/0".into(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("10.0.0.0/8,"), "{err}");

        // A value with an `=` is rejected.
        assert!(validate_web_egress_cidrs(&["10.0.0.0/8=x".into()]).is_err());

        // A bare address with no /prefix is rejected.
        assert!(validate_web_egress_cidrs(&["10.0.0.0".into()]).is_err());

        // An out-of-range prefix is rejected.
        assert!(validate_web_egress_cidrs(&["10.0.0.0/33".into()]).is_err());
    }

    #[test]
    fn default_route_egress_warning_fires_on_default_routes() {
        // The distinct rail-removal warning names the offending route and says
        // the sandbox can reach the entire internet -- for both catch-all forms
        // and for any `/0` prefix, which ignores the address bits.
        for route in ["0.0.0.0/0", "::/0", "10.0.0.0/0"] {
            let warning = default_route_egress_warning(&[route.into()])
                .unwrap_or_else(|| panic!("expected a warning for {route}"));
            assert!(warning.contains("removes the egress rail"), "{warning}");
            assert!(warning.contains("entire internet"), "{warning}");
            assert!(warning.contains(route), "{warning}");
        }

        // The offending route is called out even when mixed with scoped CIDRs.
        let warning = default_route_egress_warning(&["203.0.113.0/24".into(), "0.0.0.0/0".into()])
            .expect("expected a warning when a default route is present");
        assert!(warning.contains("0.0.0.0/0"), "{warning}");

        // No default route -> no warning (and it is distinct from the generic
        // "N declared destination(s)" note, which still fires separately).
        assert!(default_route_egress_warning(&[]).is_none());
        assert!(default_route_egress_warning(&["203.0.113.0/24".into()]).is_none());
        assert!(default_route_egress_warning(&["10.0.0.0/8".into()]).is_none());
        // A `/0`-suffixed *host* octet is not a default route (prefix is 24).
        assert!(default_route_egress_warning(&["10.0.0.10/24".into()]).is_none());
    }

    // -----------------------------------------------------------------------
    // Explicit provider egress (issue #362): the model-provider carve-out is no
    // longer a hardcoded Anthropic CIDR pushed whenever a credential is present;
    // egress is opened only for operator-named providers, resolved to their API
    // host IPs, so a real model call fails closed unless the provider is asked
    // for by name.
    // -----------------------------------------------------------------------

    #[test]
    fn provider_egress_hosts_maps_known_providers_and_rejects_unknown() {
        for (provider, hosts) in [
            ("anthropic", vec!["api.anthropic.com"]),
            ("openrouter", vec!["openrouter.ai"]),
            ("zhipu", vec!["api.z.ai"]),
            ("moonshot", vec!["api.moonshot.ai"]),
            ("deepseek", vec!["api.deepseek.com"]),
        ] {
            assert_eq!(
                provider_egress_hosts(provider).unwrap(),
                hosts,
                "{provider}"
            );
        }

        // `openai` and `gemini` are not runner-drivable today, so they are NOT
        // known providers: they fall through to `None` rather than minting an
        // egress route to a host the harness cannot talk to (#362).
        assert!(provider_egress_hosts("openai").is_none());
        assert!(provider_egress_hosts("gemini").is_none());

        // Anything that is not a canonical provider name is unknown: a bare
        // domain, a host, the empty string.
        assert!(provider_egress_hosts("acme.com").is_none());
        assert!(provider_egress_hosts("api.anthropic.com").is_none());
        assert!(provider_egress_hosts("").is_none());

        // Case-sensitive: only the lowercase canonical names resolve, so an
        // uppercased spelling is rejected rather than silently normalized.
        assert!(provider_egress_hosts("Anthropic").is_none());
        assert!(provider_egress_hosts("ANTHROPIC").is_none());
    }

    #[test]
    fn parse_egress_provider_accepts_known_and_errs_usage_on_unknown() {
        // Each runner-drivable provider parses to its own canonical name.
        for p in ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"] {
            assert_eq!(parse_egress_provider(p).unwrap(), p);
        }

        // `openai` and `gemini` are no longer accepted -- the runner cannot
        // drive them, so they are usage errors like any other unknown value.
        for p in ["openai", "gemini"] {
            assert_eq!(
                parse_egress_provider(p).unwrap_err().class,
                crate::exit::ExitClass::Usage
            );
        }

        // An unknown value is a deterministic input error (exit 2 / Usage).
        let err = parse_egress_provider("acme.com").unwrap_err();
        assert_eq!(err.class, crate::exit::ExitClass::Usage);
        assert!(err.message.contains("acme.com"), "{}", err.message);
        assert!(
            err.message.contains("not a known provider"),
            "{}",
            err.message
        );
        // The message enumerates the accepted providers so the operator can fix
        // the flag without reading source.
        for p in ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"] {
            assert!(
                err.message.contains(p),
                "message should list `{p}`: {}",
                err.message
            );
        }
        // ...and does NOT advertise the providers the runner cannot drive.
        assert!(
            !err.message.contains("openai") && !err.message.contains("gemini"),
            "message should not list undrivable providers: {}",
            err.message
        );
        // The fix hint points at the escape hatch for arbitrary destinations.
        let fix = err.fix.expect("a usage error should carry a fix hint");
        assert!(fix.contains("--allow-web-egress"), "{fix}");

        // Case-sensitivity is enforced here too: `Anthropic` is not `anthropic`.
        assert_eq!(
            parse_egress_provider("Anthropic").unwrap_err().class,
            crate::exit::ExitClass::Usage
        );
    }

    #[test]
    fn ip_to_egress_cidr_appends_full_host_prefix() {
        use std::net::IpAddr;
        // An IPv4 host is a /32; an IPv6 host is a /128 -- a single-host CIDR so
        // the egress rule opens exactly that resolved address, nothing wider.
        let v4: IpAddr = "1.2.3.4".parse().unwrap();
        assert_eq!(ip_to_egress_cidr(v4), "1.2.3.4/32");
        let v6: IpAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(ip_to_egress_cidr(v6), "2001:db8::1/128");
    }

    #[test]
    fn resolve_provider_egress_cidrs_dedups_sorts_and_covers_all_hosts() {
        use std::net::IpAddr;
        // Injected resolver so the test never touches real DNS. Anthropic and
        // OpenRouter share 1.1.1.1 to prove deduplication. Anthropic also
        // yields an IPv6 address to prove the v4/v6 mix. All addresses are
        // globally routable so they survive the split horizon guard.
        let resolve = |host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(match host {
                "api.anthropic.com" => {
                    vec![
                        "1.1.1.1".parse().unwrap(),
                        "2606:4700::1111".parse().unwrap(),
                    ]
                }
                "openrouter.ai" => {
                    vec!["1.1.1.1".parse().unwrap(), "1.0.0.1".parse().unwrap()]
                }
                "api.z.ai" => vec!["8.8.8.8".parse().unwrap()],
                "api.moonshot.ai" => vec!["8.8.4.4".parse().unwrap()],
                "api.deepseek.com" => vec!["9.9.9.9".parse().unwrap()],
                other => panic!("unexpected host {other}"),
            })
        };
        let providers = ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"]
            .map(str::to_string)
            .to_vec();
        let cidrs = resolve_provider_egress_cidrs(&providers, resolve).unwrap();
        // Deduplicated to one 1.1.1.1/32 and sorted for a stable install argv.
        assert_eq!(
            cidrs,
            vec![
                "1.0.0.1/32",
                "1.1.1.1/32",
                "2606:4700::1111/128",
                "8.8.4.4/32",
                "8.8.8.8/32",
                "9.9.9.9/32"
            ]
        );
    }

    #[test]
    fn resolve_provider_egress_cidrs_errs_when_host_resolves_empty() {
        use std::net::IpAddr;
        // A host that resolves to nothing is a hard error naming the host, not a
        // silent skip -- a real model call would otherwise fail closed with no
        // clue why.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec![]) };
        let err = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("api.anthropic.com"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_propagates_resolver_error_naming_host() {
        use std::net::IpAddr;
        // A resolver failure propagates as an error that names the host that
        // failed to resolve.
        let resolve = |host: &str| -> std::io::Result<Vec<IpAddr>> {
            Err(std::io::Error::other(format!("dns down for {host}")))
        };
        let err = resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("openrouter.ai"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_errs_on_unknown_provider() {
        use std::net::IpAddr;
        // An unknown provider in the slice fails loudly (should be pre-validated,
        // but never silently skipped).
        let resolve =
            |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec!["10.0.0.1".parse().unwrap()]) };
        let err = resolve_provider_egress_cidrs(&["acme.com".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("acme.com"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_imds_address() {
        use std::net::IpAddr;
        // A poisoned DNS answer mapping a provider host to the node metadata
        // endpoint must fail loud, naming both the host and the address.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec!["169.254.169.254".parse().unwrap()])
        };
        let err = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("api.anthropic.com"), "{msg}");
        assert!(msg.contains("169.254.169.254"), "{msg}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_private_v4() {
        use std::net::IpAddr;
        let resolve =
            |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec!["10.0.0.5".parse().unwrap()]) };
        let err = resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("10.0.0.5"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_non_routable_v6() {
        use std::net::IpAddr;
        // Loopback, link-local, and ULA v6 answers all fail closed.
        for addr in ["::1", "fe80::1", "fc00::1"] {
            let resolve = move |_host: &str| -> std::io::Result<Vec<IpAddr>> {
                Ok(vec![addr.parse().unwrap()])
            };
            let err =
                resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
            assert!(format!("{err:#}").contains(addr), "{addr}: {err:#}");
        }
    }

    #[test]
    fn resolve_provider_egress_cidrs_accepts_public_addresses() {
        use std::net::IpAddr;
        // A normal public v4 + v6 pair mints the expected single-host CIDRs.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec![
                "1.1.1.1".parse().unwrap(),
                "2606:4700::1111".parse().unwrap(),
            ])
        };
        let cidrs = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap();
        assert_eq!(cidrs, vec!["1.1.1.1/32", "2606:4700::1111/128"]);
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_mix_with_one_private() {
        use std::net::IpAddr;
        // A host that resolves to a public AND a private address fails loud --
        // the private one must never be silently dropped.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec![
                "1.1.1.1".parse().unwrap(),
                "10.0.0.5".parse().unwrap(),
            ])
        };
        let err = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("10.0.0.5"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_ipv4_mapped_private_v6() {
        use std::net::IpAddr;
        // An IPv4-mapped v6 of a private v4 is unmapped and re-checked, so it
        // is rejected just like the bare private v4.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec!["::ffff:10.0.0.5".parse().unwrap()])
        };
        let err = resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("10.0.0.5"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_routability_table() {
        use std::net::IpAddr;
        // Every non-globally-routable range must fail closed (Err), and every
        // public address must succeed (Ok). Injecting a single resolved answer
        // per case exercises `is_globally_routable_egress` end to end through
        // the resolver seam.
        let cases: &[(&str, bool)] = &[
            // Non-routable v4 -- each must be rejected.
            ("0.0.0.0", false),         // 0.0.0.0/8 / unspecified
            ("10.0.0.5", false),        // private 10/8
            ("100.64.0.1", false),      // CGNAT 100.64.0.0/10
            ("169.254.169.254", false), // link-local / IMDS
            ("192.0.0.1", false),       // IETF protocol assignments 192.0.0.0/24
            ("192.88.99.1", false),     // 6to4 relay anycast 192.88.99.0/24
            ("198.18.0.1", false),      // benchmarking 198.18.0.0/15
            ("240.0.0.1", false),       // reserved/future 240.0.0.0/4
            ("255.255.255.255", false), // broadcast (240/4)
            // Non-routable v6 -- each must be rejected.
            ("::1", false),             // loopback
            ("fe80::1", false),         // link-local
            ("fc00::1", false),         // ULA
            ("2001:db8::1", false),     // documentation
            ("::ffff:10.0.0.5", false), // IPv4-mapped private
            // Public addresses -- each must succeed.
            ("1.1.1.1", true),
            ("8.8.8.8", true),
            ("2606:4700::1111", true),
            ("2001:4860:4860::8888", true),
        ];
        for (addr, expect_ok) in cases {
            let a = *addr;
            let resolve =
                move |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec![a.parse().unwrap()]) };
            let res = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve);
            if *expect_ok {
                let cidrs = res.unwrap_or_else(|e| panic!("{a} should be routable: {e:#}"));
                assert_eq!(cidrs.len(), 1, "{a} should mint one CIDR");
            } else {
                let err = res
                    .err()
                    .unwrap_or_else(|| panic!("{a} should be rejected as non-routable"));
                assert!(format!("{err:#}").contains(a), "{a}: {err:#}");
            }
        }
    }

    #[test]
    fn provider_egress_note_none_on_empty_and_lists_providers() {
        // No providers -> no note.
        assert!(provider_egress_note(&[]).is_none());
        // Non-empty -> a note that says egress was opened and names each provider.
        let note = provider_egress_note(&["anthropic".to_string(), "openrouter".to_string()])
            .expect("a note for a non-empty provider list");
        assert!(note.contains("egress opened"), "{note}");
        assert!(note.contains("anthropic"), "{note}");
        assert!(note.contains("openrouter"), "{note}");
    }

    #[test]
    fn sealed_credential_warning_only_when_cred_present_and_no_egress() {
        // The one combination that warns: a credential is present but nothing
        // opened egress, so the model is unreachable behind the sealed sandbox.
        let warn =
            sealed_credential_warning(true, false).expect("cred present + no egress must warn");
        assert!(warn.contains("sealed"), "{warn}");
        assert!(warn.contains("unreachable"), "{warn}");
        assert!(warn.contains("--allow-egress-host"), "{warn}");
        assert!(warn.contains("--allow-web-egress"), "{warn}");
        for provider in ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"] {
            assert!(
                warn.contains(provider),
                "warning should name {provider}: {warn}"
            );
        }

        // Every other combination stays silent.
        assert!(sealed_credential_warning(true, true).is_none());
        assert!(sealed_credential_warning(false, false).is_none());
        assert!(sealed_credential_warning(false, true).is_none());
    }

    #[test]
    fn model_egress_status_lines_no_cred_open_egress_never_says_sealed() {
        // The exact contradiction bug: no credential but egress opened via a
        // provider. The provider note must report the open, and the fake-model
        // warning must NOT claim the egress is sealed.
        let lines =
            model_egress_status_lines(false, false, false, &["anthropic".to_string()], true, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(msgs.iter().any(|m| m.contains("egress opened")), "{msgs:?}");
        for m in &msgs {
            assert!(!m.contains("sealed"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_cred_no_egress_warns_sealed() {
        // A credential present with nothing opened surfaces the sealed warning
        // naming both flags.
        let lines = model_egress_status_lines(true, false, false, &[], false, false);
        let warn = lines
            .iter()
            .find(|(w, _)| *w)
            .map(|(_, m)| m.as_str())
            .expect("a warn line");
        assert!(warn.contains("sealed"), "{warn}");
        assert!(warn.contains("--allow-egress-host"), "{warn}");
        assert!(warn.contains("--allow-web-egress"), "{warn}");
    }

    #[test]
    fn model_egress_status_lines_cred_open_egress_no_sealed() {
        // A credential with a provider egress opened: provider note + rotation
        // present, and no message claims the sandbox is sealed.
        let lines =
            model_egress_status_lines(true, false, false, &["openrouter".to_string()], true, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(msgs.iter().any(|m| m.contains("egress opened")), "{msgs:?}");
        assert!(msgs.iter().any(|m| m.contains("can rotate")), "{msgs:?}");
        for m in &msgs {
            assert!(!m.contains("sealed"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_fake_model_sealed_and_canned() {
        // No credential, no egress, real (not --fake-model) install: the
        // fake-model warning keeps the "(model egress stays sealed)" clause and
        // a canned-replies note follows.
        let lines = model_egress_status_lines(false, false, false, &[], false, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("(model egress stays sealed)")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m.contains("Replies will be canned")),
            "{msgs:?}"
        );
    }

    #[test]
    fn model_egress_status_lines_canned_guidance_requires_native_base_urls() {
        let lines = model_egress_status_lines(false, false, false, &[], false, false);
        let canned = lines
            .iter()
            .map(|(_, message)| message.as_str())
            .find(|message| message.contains("Replies will be canned"))
            .expect("canned reply guidance");

        for provider in ["Zhipu", "Moonshot", "DeepSeek"] {
            assert!(canned.contains(provider), "{canned}");
        }
        assert!(canned.contains("worker runtime base URL"), "{canned}");
        assert!(canned.contains("matching egress"), "{canned}");
    }

    #[test]
    fn model_egress_status_lines_dry_run_skips_past_tense_note() {
        // Under dry-run the handler prints its own "a live run resolves..."
        // note, so this fn emits no past-tense "egress opened" line.
        let lines =
            model_egress_status_lines(true, false, false, &["anthropic".to_string()], true, true);
        for (_, m) in &lines {
            assert!(!m.contains("egress opened"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_dry_run_does_not_assert_fake_model() {
        // #1898: under --dry-run there is no `existing` release to read, so the
        // no-credential arm must not assert the fake-model outcome -- it must
        // say preservation is unknown offline instead, as a warning. It also
        // must not claim the sandbox "stays sealed": a live rerun could
        // re-supply the release's recorded egress, so that assertion would be
        // just as false offline as the fake-model one.
        let lines = model_egress_status_lines(false, false, false, &[], false, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        for m in &msgs {
            assert!(!m.contains("installing with the fake model"), "{m}");
            assert!(!m.contains("sealed"), "{m}");
        }
        let (is_warning, preservation_msg) = lines
            .iter()
            .find(|(_, m)| m.contains("preserves the release's recorded model configuration"))
            .expect("a preservation-unknown message");
        assert!(
            preservation_msg.contains("not read under --dry-run"),
            "{preservation_msg}"
        );
        assert!(*is_warning, "{msgs:?}");
    }

    #[test]
    fn model_egress_status_lines_live_run_still_asserts_fake_model_install() {
        // Sibling of model_egress_status_lines_dry_run_does_not_assert_fake_model:
        // same inputs but a live run, which must keep asserting the fake-model
        // outcome. Pins that only the dry-run path changed under #1898.
        let lines = model_egress_status_lines(false, false, false, &[], false, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("installing with the fake model")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("(model egress stays sealed)")),
            "{msgs:?}"
        );
        for m in &msgs {
            assert!(!m.contains("not read under --dry-run"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_dry_run_keeps_credential_guidance() {
        // An operator on a fresh install must still be told how to enable the
        // real model, so softening the assertion under --dry-run must not drop
        // the guidance.
        let lines = model_egress_status_lines(false, false, false, &[], false, true);
        let note = lines
            .iter()
            .find(|(is_warning, _)| !*is_warning)
            .map(|(_, m)| m.as_str())
            .expect("a non-warn note");
        assert!(note.contains("CURIE_CREDENTIALS"), "{note}");
        assert!(note.contains("fresh install"), "{note}");
        assert!(note.contains("replies will be canned"), "{note}");
        assert!(note.contains("worker runtime base URL"), "{note}");
    }

    #[test]
    fn model_egress_status_lines_dry_run_open_egress_never_says_sealed() {
        // Dry-run sibling of model_egress_status_lines_no_cred_open_egress_never_says_sealed:
        // no credential, dry-run, but a provider egress is opened. The
        // preservation-unknown line must not carry the live-only "sealed" or
        // "installing with the fake model" language, and its "no model egress
        // is opened by this run" suffix must drop when egress is in fact open.
        let lines =
            model_egress_status_lines(false, false, false, &["anthropic".to_string()], true, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        for m in &msgs {
            assert!(!m.contains("sealed"), "{m}");
            assert!(!m.contains("installing with the fake model"), "{m}");
            assert!(!m.contains("no model egress is opened by this run"), "{m}");
        }
        assert!(
            msgs.iter().any(|m| m.contains("not read under --dry-run")),
            "{msgs:?}"
        );
    }

    #[test]
    fn model_egress_status_lines_explicit_fake_model_stays_silent_under_dry_run() {
        // No test above ever passes fake_model = true. An explicit --fake-model
        // run has already declared the outcome, so this helper must emit
        // nothing for it even under --dry-run.
        let lines = model_egress_status_lines(false, false, true, &[], false, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(lines.is_empty(), "{msgs:?}");
    }

    #[test]
    fn model_egress_status_lines_local_model_wins_over_dry_run_arm() {
        // No test above ever passes local_model = true. --dry-run --local-model
        // must keep reporting the local-model install, not the new
        // preservation-unknown warning.
        let lines = model_egress_status_lines(false, true, false, &[], false, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(
            msgs.iter().any(|m| m.contains("local model enabled")),
            "{msgs:?}"
        );
        for m in &msgs {
            assert!(!m.contains("not read under --dry-run"), "{m}");
            assert!(!m.contains("installing with the fake model"), "{m}");
        }
    }
}
