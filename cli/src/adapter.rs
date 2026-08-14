//! `curie adapter`: author, validate and wire up a channel adapter binding
//! profile (issue #1516).
//!
//! A binding profile is one install's `adapter.yaml`: the channel kind, the
//! address shape, the reply route, and the credential IDENTITIES the adapter
//! expects. It is authored by a third party, so this module treats it as data
//! and never as an instruction:
//!
//! - `address.example` is authoring documentation. Every verb that resolves
//!   against a concrete pair takes an explicit `--address`, checked against the
//!   profile's own `address.pattern` before any request leaves.
//! - `credentials.egress` is a non binding SUGGESTION. The worker indexes its
//!   GLOBAL credential map by the slug the route carries, so a profile chosen
//!   slug reaching the write path would let one adapter's file select another
//!   adapter's secret. `bind` therefore requires `--adapter-slug` and writes
//!   that value and only that value.
//! - `credentials.egress_secret_env` documents what the ADAPTER reads. Nothing
//!   here resolves it: `smoke-test` takes its egress secret from a file or from
//!   stdin, supplied at the invocation boundary, or it refuses.
//!
//! The profile shape itself is validated against the committed
//! `packages/channel-protocol` schema, so the CLI and the Python conformance kit
//! agree on one contract instead of two mirrors. One rule cannot export: JSON
//! Schema has no way to say "the Rust regex crate can compile this string", so
//! `address.pattern` is an admitted PAIRED validator, enforced here in code and
//! in `channel_protocol.manifest` in Python, with
//! `packages/channel-protocol/schema/adapter-profile.corpus.json` as the drift
//! gate between them.

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::api::{ApiClient, ChannelBindingWrite};
use crate::exit::usage;
use crate::ui::{CliOutput, Ui};

/// The committed adapter binding profile schema, embedded at compile time.
/// `packages/channel-protocol` exports and drift checks it against its own
/// Pydantic models, so this constant tracks the frozen contract with no manual
/// upkeep on the Rust side.
const ADAPTER_PROFILE_SCHEMA: &str =
    include_str!("../../packages/channel-protocol/schema/adapter-profile.schema.json");

/// The profile format version this build understands. Independent of the reply
/// wire version a profile declares in `conformance.wire_version`: conflating the
/// two would let a wire bump silently invalidate every profile.
pub const PROFILE_VERSION: &str = "1.0";

/// The header the worker authenticates its egress with.
const ADAPTER_SECRET_HEADER: &str = "X-Curie-Adapter-Secret";

/// The worker refuses an acknowledgement body OVER this many bytes, so exactly
/// this many still passes.
const MAX_ACK_BODY_BYTES: usize = 65536;

/// `ChannelTokenRequest.ttl_s` is `gt=0, le=604800`; a value outside that is a
/// usage error here rather than a 422 from the API.
const MAX_TTL_S: u64 = 604_800;

/// What no wire probe can observe, reported on every smoke-test so a passing
/// verdict is never read as whole floor conformance.
const MANUAL_REVIEW_SECRET_ORDER: &str =
    "floor clause 3c: that the adapter verifies the egress secret BEFORE any side effect. \
     A probe sees the answer, never the order, so a human reads the request handler.";

// ─── The profile ─────────────────────────────────────────────────────────────

/// One install's binding profile for one adapter.
///
/// `deny_unknown_fields` is belt and braces behind the schema (which is closed
/// too): a field this struct forgot becomes a loud parse failure rather than a
/// silent drop.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterProfile {
    pub version: String,
    pub kind: String,
    /// OPTIONAL here, required by the verbs that build a request from it. An
    /// author publishes the address shape and the credential identities long
    /// before any one install has a route. Absent reads back as JSON null, never
    /// a missing key, so a consumer can branch on it unconditionally.
    #[serde(default)]
    pub endpoint: Option<String>,
    pub address: AddressShape,
    pub credentials: AdapterCredentials,
    pub conformance: AdapterConformance,
}

/// The address form this adapter owns, as a regex plus a legible example.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AddressShape {
    pub description: String,
    pub pattern: String,
    pub example: String,
}

/// Which credential identities this adapter expects, by NAME only.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterCredentials {
    /// A SUGGESTION. `bind` requires an operator supplied slug and never takes
    /// this value alone.
    pub egress: String,
    /// Documentation of what the ADAPTER reads. Never resolved here.
    pub egress_secret_env: String,
    /// Documentation of what the ADAPTER reads. Never resolved here.
    pub ingress_token_env: String,
}

/// What this adapter claims about the reply wire it speaks.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConformance {
    pub wire_version: String,
    pub mints_reply_ref: bool,
}

/// The committed profile schema compiled with its root pointed at
/// `AdapterProfile`. Same document, same `$defs`, different entry point.
fn profile_validator() -> &'static jsonschema::Validator {
    static VALIDATOR: std::sync::OnceLock<jsonschema::Validator> = std::sync::OnceLock::new();
    VALIDATOR.get_or_init(|| {
        let mut doc: serde_json::Value = serde_json::from_str(ADAPTER_PROFILE_SCHEMA).expect(
            "packages/channel-protocol/schema/adapter-profile.schema.json is committed and valid JSON",
        );
        doc["$ref"] = serde_json::Value::String("#/$defs/AdapterProfile".to_string());
        jsonschema::validator_for(&doc)
            .expect("adapter-profile.schema.json's AdapterProfile def compiles to a validator")
    })
}

/// Every way the committed schema rejects this document, joined.
fn schema_errors(raw: &serde_json::Value) -> Option<String> {
    let errors: Vec<String> = profile_validator()
        .iter_errors(raw)
        .map(|e| format!("{e} (at instance path {})", e.instance_path()))
        .collect();
    (!errors.is_empty()).then(|| errors.join("; "))
}

/// Read and accept one binding profile, in the order the acceptance rules run.
///
/// 1. The VERSION check, first and on its own. A schema for a version this build
///    does not speak has no authority over the file, so reporting a field level
///    error from it would send the author to fix a shape that was never theirs.
/// 2. The committed schema, which is the sole authority for everything it can
///    express.
/// 3. The paired tier: `address.pattern` has to compile with the Rust regex
///    crate, which JSON Schema cannot say.
pub fn load_profile(file: &Path) -> Result<AdapterProfile> {
    let raw = std::fs::read_to_string(file).map_err(|err| {
        usage(format!(
            "cannot read the adapter profile {}: {err}",
            file.display()
        ))
    })?;
    let value: serde_json::Value = serde_norway::from_str(&raw)
        .map_err(|err| usage(format!("{} is not valid YAML: {err}", file.display())))?;

    if let Some(declared) = value.get("version").and_then(|v| v.as_str()) {
        if declared != PROFILE_VERSION {
            return Err(usage(format!(
                "{} declares adapter profile version {declared}; this build understands \
                 version {PROFILE_VERSION}. Rewrite the profile against {PROFILE_VERSION}, \
                 or use a Curie build that speaks {declared}.",
                file.display()
            )));
        }
    }

    if let Some(errors) = schema_errors(&value) {
        return Err(usage(format!(
            "{} is not a valid adapter profile: {errors}",
            file.display()
        )));
    }

    let profile: AdapterProfile = serde_json::from_value(value).map_err(|err| {
        usage(format!(
            "{} is not a valid adapter profile: {err}",
            file.display()
        ))
    })?;

    compile_pattern(&profile.address.pattern, file)?;
    Ok(profile)
}

/// The paired half of the address rule: the Rust regex dialect is the floor, so
/// a pattern Python `re` compiles and this crate refuses (lookaround, a
/// backreference) is refused here too rather than validating in one language and
/// failing in the other.
fn compile_pattern(pattern: &str, file: &Path) -> Result<regex::Regex> {
    regex::Regex::new(pattern).map_err(|err| {
        usage(format!(
            "{}: address.pattern does not compile with the Rust regex crate, which is the \
             floor both validators hold to (no lookaround, no backreferences): {err}",
            file.display()
        ))
    })
}

/// Check an operator supplied address against the profile's declared shape,
/// before any request. Without this the operator meets the mismatch as a 404
/// from a live ingress instead of a usage error naming both halves.
fn check_address(profile: &AdapterProfile, file: &Path, address: &str) -> Result<()> {
    let pattern = compile_pattern(&profile.address.pattern, file)?;
    if pattern.is_match(address) {
        return Ok(());
    }
    Err(usage(format!(
        "the address {address:?} does not match the shape {} declares: {}",
        file.display(),
        profile.address.pattern
    )))
}

/// The route to POST reply events to: the explicit override, else the profile's
/// own, else a usage error naming the missing field and the flag that supplies
/// it. Never an empty string turned into a confusing HTTP error.
fn resolve_endpoint(
    profile: &AdapterProfile,
    file: &Path,
    override_endpoint: Option<&str>,
) -> Result<String> {
    if let Some(endpoint) = override_endpoint {
        return Ok(endpoint.to_string());
    }
    match profile.endpoint.as_deref() {
        Some(endpoint) => Ok(endpoint.to_string()),
        None => Err(usage(format!(
            "{} declares no endpoint, and this verb needs a concrete route: set endpoint \
             in the profile, or pass --endpoint <url>",
            file.display()
        ))),
    }
}

/// An endpoint reduced to scheme, host and port, the same redaction the worker's
/// reply sink uses. An endpoint can carry a token in its path or query, and
/// these strings reach terminals and logs.
fn redacted(endpoint: &str) -> String {
    match reqwest::Url::parse(endpoint) {
        Ok(url) => match url.host_str() {
            Some(host) => match url.port() {
                Some(port) => format!("{}://{host}:{port}", url.scheme()),
                None => format!("{}://{host}", url.scheme()),
            },
            None => "an unparseable endpoint".to_string(),
        },
        Err(_) => "an unparseable endpoint".to_string(),
    }
}

// ─── scaffold ────────────────────────────────────────────────────────────────

/// Options for `curie adapter scaffold`.
#[derive(Debug, Clone)]
pub struct ScaffoldOpts {
    pub name: String,
    pub kind: String,
    pub address: String,
    pub endpoint: String,
    pub adapter: String,
    pub dir: Option<PathBuf>,
}

/// Write one binding profile and print the next steps. Deliberately minimal: no
/// project tree and no README, because the first thing an author would otherwise
/// do after scaffolding is fix the scaffold's own output.
///
/// The generated `address.pattern` matches the address it was scaffolded for
/// exactly (the address, regex escaped), so the profile it writes is one the
/// validator accepts for that address on the very next command. Widen it by hand
/// to the real shape of the channel's addresses.
pub fn scaffold(opts: ScaffoldOpts) -> Result<AdapterScaffoldOutput> {
    let root = opts.dir.unwrap_or_else(|| PathBuf::from("."));
    let dir = root.join(&opts.name);
    let file = dir.join("adapter.yaml");

    let profile = AdapterProfile {
        version: PROFILE_VERSION.to_string(),
        kind: opts.kind.clone(),
        endpoint: Some(opts.endpoint.clone()),
        address: AddressShape {
            description: format!("The {} address this adapter owns.", opts.kind),
            pattern: format!("^{}$", regex::escape(&opts.address)),
            example: opts.address.clone(),
        },
        credentials: AdapterCredentials {
            egress: opts.adapter.clone(),
            egress_secret_env: "CURIE_EGRESS_SECRET".to_string(),
            ingress_token_env: "CURIE_INGRESS_TOKEN".to_string(),
        },
        conformance: AdapterConformance {
            wire_version: "1.0".to_string(),
            mints_reply_ref: false,
        },
    };

    let body = serde_json::to_value(&profile).context("serializing the scaffolded profile")?;
    if let Some(errors) = schema_errors(&body) {
        return Err(usage(format!(
            "those values do not make a valid adapter profile: {errors}"
        )));
    }
    compile_pattern(&profile.address.pattern, &file)?;

    if file.exists() {
        return Err(usage(format!(
            "{} already exists; scaffold never overwrites a profile",
            file.display()
        )));
    }
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let yaml = serde_norway::to_string(&profile).context("rendering the profile as YAML")?;
    let contents = format!("{}{yaml}", scaffold_header(&opts.address));
    std::fs::write(&file, contents).with_context(|| format!("writing {}", file.display()))?;

    let display = file.display().to_string();
    Ok(AdapterScaffoldOutput {
        next_steps: vec![
            format!(
                "curie adapter validate -f {display} --address {}",
                opts.address
            ),
            format!(
                "curie adapter bind -f {display} <agent> --address {} --adapter-slug {} --yes",
                opts.address, opts.adapter
            ),
            format!(
                "curie adapter smoke-test -f {display} --address {} --secret-file <path> --yes",
                opts.address
            ),
        ],
        file: display,
        kind: opts.kind,
        address: opts.address,
        endpoint: opts.endpoint,
        adapter: opts.adapter,
    })
}

/// The comment block the scaffolded profile opens with: what the file is, and
/// the one field an author almost always has to widen.
fn scaffold_header(address: &str) -> String {
    format!(
        "# One install's binding profile for one channel adapter.\n\
         #\n\
         # address.pattern below matches exactly {address}, the address this was\n\
         # scaffolded for. Widen it to the real shape of your channel's addresses; it\n\
         # must compile in both Python re and the Rust regex crate, so no lookaround\n\
         # and no backreferences.\n\
         #\n\
         # credentials names IDENTITIES only. egress is a suggestion that `curie\n\
         # adapter bind` asks an operator to confirm, and the two env entries\n\
         # document what your adapter reads. Curie resolves neither.\n"
    )
}

/// Output of `adapter scaffold`.
#[derive(Debug)]
pub struct AdapterScaffoldOutput {
    pub file: String,
    pub kind: String,
    pub address: String,
    pub endpoint: String,
    pub adapter: String,
    pub next_steps: Vec<String>,
}

impl CliOutput for AdapterScaffoldOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "file": self.file,
            "kind": self.kind,
            "address": self.address,
            "endpoint": self.endpoint,
            "adapter": self.adapter,
            "next_steps": self.next_steps,
        })
    }

    fn render(&self, ui: &Ui) {
        ui.payload(&format!("wrote {}", self.file));
        for step in &self.next_steps {
            ui.payload_plain(step);
        }
    }
}

// ─── validate ────────────────────────────────────────────────────────────────

/// Accept or refuse one profile, and report the values it parsed.
///
/// The payload carries the parsed profile rather than a bare verdict, because
/// the parsed values are what the cross language corpus compares against.
pub fn validate(file: &Path, address: Option<&str>) -> Result<AdapterValidateOutput> {
    let profile = load_profile(file)?;
    if let Some(address) = address {
        check_address(&profile, file, address)?;
    }
    Ok(AdapterValidateOutput {
        file: file.display().to_string(),
        profile,
    })
}

/// Output of `adapter validate`.
#[derive(Debug)]
pub struct AdapterValidateOutput {
    pub file: String,
    pub profile: AdapterProfile,
}

impl CliOutput for AdapterValidateOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "file": self.file,
            "profile": serde_json::to_value(&self.profile)
                .expect("a parsed adapter profile serializes"),
        })
    }

    fn render(&self, ui: &Ui) {
        ui.payload(&format!("{} is a valid adapter profile", self.file));
        ui.kv("kind", &self.profile.kind);
        ui.kv("address", &self.profile.address.pattern);
        ui.kv(
            "endpoint",
            self.profile.endpoint.as_deref().unwrap_or("unset"),
        );
        ui.kv("egress", &self.profile.credentials.egress);
    }
}

// ─── bind ────────────────────────────────────────────────────────────────────

/// Options for `curie adapter bind`.
#[derive(Debug, Clone)]
pub struct BindOpts {
    pub file: PathBuf,
    pub agent: String,
    pub address: String,
    pub adapter_slug: String,
    pub endpoint: Option<String>,
    pub api_url: String,
    pub api_key: String,
    pub yes: bool,
}

/// Write the four field route for one agent.
///
/// The slug written is ALWAYS the operator's `--adapter-slug`. The profile's
/// `credentials.egress` is read for one purpose only: to show the operator that
/// the file suggested something else, which is a confirmation and never a
/// selection. The worker indexes its global credential map by this slug, so a
/// profile chosen value here would let one adapter's file redirect another
/// adapter's secret to the endpoint that same file names.
pub async fn bind(opts: BindOpts) -> Result<AdapterBindOutput> {
    let profile = load_profile(&opts.file)?;
    check_address(&profile, &opts.file, &opts.address)?;
    let endpoint = resolve_endpoint(&profile, &opts.file, opts.endpoint.as_deref())?;

    let suggested = profile.credentials.egress.as_str();
    if !opts.yes {
        let mut refusal = format!(
            "binding {} would point the platform's authenticated egress at {}, under the \
             egress credential {:?}. Re-run with --yes to confirm.",
            opts.agent,
            redacted(&endpoint),
            opts.adapter_slug
        );
        if suggested != opts.adapter_slug {
            refusal.push_str(&format!(
                " The profile suggests a DIFFERENT slug: it names {suggested:?} and you passed \
                 {:?}. The slug selects which stored secret the worker sends, so confirm the \
                 one you meant.",
                opts.adapter_slug
            ));
        }
        return Err(usage(refusal));
    }

    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let binding = ChannelBindingWrite {
        kind: profile.kind.clone(),
        address: opts.address.clone(),
        endpoint: endpoint.clone(),
        adapter: opts.adapter_slug.clone(),
    };
    client.set_agent_channel(&opts.agent, &binding).await?;

    Ok(AdapterBindOutput {
        agent: opts.agent,
        kind: binding.kind,
        address: binding.address,
        endpoint: binding.endpoint,
        adapter: binding.adapter,
    })
}

/// Output of `adapter bind`: the whole route that was written, so the operator
/// reads back the slug that will select the egress secret.
#[derive(Debug)]
pub struct AdapterBindOutput {
    pub agent: String,
    pub kind: String,
    pub address: String,
    pub endpoint: String,
    pub adapter: String,
}

impl CliOutput for AdapterBindOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "agent": self.agent,
            "kind": self.kind,
            "address": self.address,
            "endpoint": self.endpoint,
            "adapter": self.adapter,
        })
    }

    fn render(&self, ui: &Ui) {
        ui.payload(&format!("bound {} to {}", self.agent, self.kind));
        ui.kv("address", &self.address);
        ui.kv("endpoint", &redacted(&self.endpoint));
        ui.kv("adapter", &self.adapter);
    }
}

// ─── token ───────────────────────────────────────────────────────────────────

/// Options for `curie adapter token`.
#[derive(Debug, Clone)]
pub struct TokenOpts {
    pub file: PathBuf,
    pub address: String,
    pub api_url: String,
    pub api_key: String,
    pub ttl_s: u64,
}

/// Mint a `chn` token for the concrete `(kind, address)` pair.
///
/// This is the one verb whose payload carries a secret VALUE, and it says so:
/// the token rides under `token` alongside `"secret": true`, goes to stdout and
/// to no diagnostic stream, so a caller can pipe stdout while keeping stderr in
/// a log.
pub async fn token(opts: TokenOpts) -> Result<AdapterTokenOutput> {
    let profile = load_profile(&opts.file)?;
    check_address(&profile, &opts.file, &opts.address)?;
    if opts.ttl_s == 0 || opts.ttl_s > MAX_TTL_S {
        return Err(usage(format!(
            "--ttl-s {} is outside what the API accepts (1 to {MAX_TTL_S} seconds)",
            opts.ttl_s
        )));
    }

    let client = http_client()?;
    let (status, body) = post_channel_token(
        &client,
        &opts.api_url,
        &opts.api_key,
        &profile.kind,
        &opts.address,
        opts.ttl_s,
    )
    .await?;
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "minting a channel token failed with {status}: {}",
            body.trim()
        ));
    }

    let minted: MintedToken = serde_json::from_str(&body).context("decoding the minted token")?;
    Ok(AdapterTokenOutput {
        token: minted.token,
        kind: profile.kind,
        address: opts.address,
        ttl_s: opts.ttl_s,
    })
}

/// `ChannelTokenOut`, the mint response.
#[derive(Debug, Deserialize)]
struct MintedToken {
    token: String,
}

/// Output of `adapter token`.
#[derive(Debug)]
pub struct AdapterTokenOutput {
    pub token: String,
    pub kind: String,
    pub address: String,
    pub ttl_s: u64,
}

impl CliOutput for AdapterTokenOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "token": self.token,
            "secret": true,
            "kind": self.kind,
            "address": self.address,
            "ttl_s": self.ttl_s,
        })
    }

    fn render(&self, ui: &Ui) {
        // The token alone, with no surrounding line: the human path is also the
        // one a shell pipes into a variable.
        ui.payload_plain(&self.token);
    }
}

// ─── smoke-test ──────────────────────────────────────────────────────────────

/// Options for `curie adapter smoke-test`.
#[derive(Debug, Clone)]
pub struct SmokeTestOpts {
    pub file: PathBuf,
    pub address: String,
    pub endpoint: Option<String>,
    pub api_url: String,
    pub api_key: String,
    pub secret_file: Option<PathBuf>,
    pub secret_stdin: bool,
    pub enqueue: bool,
    pub allow_insecure: bool,
    pub yes: bool,
}

/// Probe a deployed adapter from the outside.
///
/// What the checks are allowed to claim is narrow on purpose. The egress checks
/// prove that THIS endpoint accepts the secret the operator supplied and refuses
/// a wrong one; the binding check proves the route FIELDS are present. Neither
/// proves the running worker holds that slug or that value, because the worker's
/// lookup happens on its own side and the secret only travels during a real
/// emit.
pub async fn smoke_test(opts: SmokeTestOpts) -> Result<()> {
    let profile = load_profile(&opts.file)?;
    check_address(&profile, &opts.file, &opts.address)?;
    let endpoint = resolve_endpoint(&profile, &opts.file, opts.endpoint.as_deref())?;

    let (secret, source) = read_egress_secret(opts.secret_file.as_deref(), opts.secret_stdin)?;

    if !endpoint.starts_with("https://") && !opts.allow_insecure {
        return Err(usage(format!(
            "{} is not an https route, so the egress secret would travel in cleartext. \
             Pass --allow-insecure to probe it anyway.",
            redacted(&endpoint)
        )));
    }

    if !opts.yes {
        return Err(usage(format!(
            "this would send the secret from {source} to {}, as the egress identity the \
             profile names ({:?}). Re-run with --yes to confirm both halves.",
            redacted(&endpoint),
            profile.credentials.egress
        )));
    }

    let client = http_client()?;
    let conversation = format!("curie-smoke-{}", uuid::Uuid::new_v4());
    let event = status_event(&profile.kind, &opts.address, &conversation);

    let positive = probe_egress(&client, &endpoint, &secret, &event, "egress_positive").await;
    let wrong = format!("{secret}-not-the-secret");
    let negative = refusal_of_a_wrong_secret(
        probe_egress(&client, &endpoint, &wrong, &event, "egress_negative").await,
    );

    let (binding, minted) = probe_binding(&client, &opts, &profile).await;
    let round_trip = if opts.enqueue {
        Some(probe_round_trip(&client, &opts, &profile, minted.as_deref()).await)
    } else {
        None
    };

    let out = AdapterSmokeTestOutput {
        endpoint: redacted(&endpoint),
        kind: profile.kind,
        address: opts.address,
        egress_positive: positive,
        egress_negative: negative,
        binding,
        round_trip,
    };
    let failed = out.counts().1;
    crate::ui::ui().emit(&out);
    if failed > 0 {
        std::process::exit(crate::exit::ExitClass::Failure.code());
    }
    Ok(())
}

/// The egress secret, from a file or from stdin, and never from anywhere else.
///
/// `credentials.egress_secret_env` names a variable the ADAPTER reads; resolving
/// it here would let a third party file choose which of an operator's secrets
/// gets read and, in the same breath, where it is sent. There is deliberately no
/// flag that names a variable either.
fn read_egress_secret(file: Option<&Path>, stdin: bool) -> Result<(String, String)> {
    match (file, stdin) {
        (Some(path), false) => {
            let raw = std::fs::read_to_string(path).map_err(|err| {
                usage(format!(
                    "cannot read the egress secret {}: {err}",
                    path.display()
                ))
            })?;
            let value = raw.trim().to_string();
            if value.is_empty() {
                return Err(usage(format!("{} is empty", path.display())));
            }
            Ok((value, path.display().to_string()))
        }
        (None, true) => {
            let mut raw = String::new();
            std::io::stdin()
                .read_to_string(&mut raw)
                .context("reading the egress secret from stdin")?;
            let value = raw.trim().to_string();
            if value.is_empty() {
                return Err(usage("stdin carried no egress secret"));
            }
            Ok((value, "stdin".to_string()))
        }
        (Some(_), true) => Err(usage(
            "pass the egress secret through --secret-file OR --secret-stdin, not both",
        )),
        (None, false) => Err(usage(
            "an explicit egress secret source is required: pass --secret-file <path> or \
             --secret-stdin. A profile names the variable its own adapter reads and that name \
             is never resolved here, so nothing is read from your environment.",
        )),
    }
}

/// The best effort, side effect free probe event. `turn.status` is explicitly
/// best effort at the adapter, so steps that send it cause no agent run and no
/// outbound message.
fn status_event(kind: &str, address: &str, conversation: &str) -> serde_json::Value {
    serde_json::json!({
        "version": "1.0",
        "event": "turn.status",
        "target": {
            "kind": kind,
            "address": address,
            "conversation_id": conversation,
            "reply_ref": serde_json::Value::Null,
        },
        "status": "curie adapter smoke test",
    })
}

/// One check's verdict.
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub name: String,
    pub ok: bool,
    pub status: Option<u16>,
    pub detail: String,
}

impl CheckResult {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ok": self.ok,
            "status": self.status,
            "detail": self.detail,
        })
    }
}

/// POST the probe event under one secret and judge the answer against the wire
/// floor: 2xx, a JSON body, at or under the ack cap, and never a redirect.
async fn probe_egress(
    client: &reqwest::Client,
    endpoint: &str,
    secret: &str,
    event: &serde_json::Value,
    name: &str,
) -> CheckResult {
    let sent = client
        .post(endpoint)
        .header("Content-Type", "application/json")
        .header(ADAPTER_SECRET_HEADER, secret)
        .json(event)
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(err) => {
            return CheckResult {
                name: name.to_string(),
                ok: false,
                status: None,
                detail: format!("the endpoint did not answer: {err}"),
            }
        }
    };
    let status = response.status().as_u16();
    let body = response.bytes().await.unwrap_or_default();
    let mut detail = format!("the endpoint answered {status}");
    let mut ok = (200..300).contains(&status);
    if (300..400).contains(&status) {
        ok = false;
        detail = format!(
            "the endpoint answered {status} (a redirect), which the worker refuses rather \
             than replaying the egress credential at the redirect target"
        );
    } else if ok && body.len() > MAX_ACK_BODY_BYTES {
        ok = false;
        detail = format!(
            "the acknowledgement body is {} bytes, over the {MAX_ACK_BODY_BYTES} byte cap",
            body.len()
        );
    } else if ok && serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        ok = false;
        detail = format!("the endpoint answered {status} with a body that is not JSON");
    }
    CheckResult {
        name: name.to_string(),
        ok,
        status: Some(status),
        detail,
    }
}

/// The negative probe's verdict is INVERTED: the adapter must refuse a
/// deliberately wrong secret, so a 2xx here is a hard failure, meaning it is not
/// authenticating the platform at all.
fn refusal_of_a_wrong_secret(outcome: CheckResult) -> CheckResult {
    let accepted = |status: u16| (200..300).contains(&status);
    CheckResult {
        name: outcome.name,
        ok: outcome.status.is_some_and(|status| !accepted(status)),
        status: outcome.status,
        detail: match outcome.status {
            Some(status) if accepted(status) => {
                "the endpoint accepted a WRONG secret; it is not authenticating the platform"
                    .to_string()
            }
            Some(status) => format!("the endpoint refused a wrong secret with {status}"),
            None => outcome.detail,
        },
    }
}

/// Mint a token for the pair, which is what proves the route FIELDS are present:
/// a 404 means the pair is unbound and a 409 means the reply route is unset.
async fn probe_binding(
    client: &reqwest::Client,
    opts: &SmokeTestOpts,
    profile: &AdapterProfile,
) -> (CheckResult, Option<String>) {
    let sent = post_channel_token(
        client,
        &opts.api_url,
        &opts.api_key,
        &profile.kind,
        &opts.address,
        3600,
    )
    .await;
    let (status, body) = match sent {
        Ok(pair) => pair,
        Err(err) => {
            return (
                CheckResult {
                    name: "binding".to_string(),
                    ok: false,
                    status: None,
                    detail: format!("the platform API did not answer: {err:#}"),
                },
                None,
            )
        }
    };
    let code = status.as_u16();
    let detail = match code {
        404 => "no agent is bound to this pair; bind one first".to_string(),
        409 => "the binding carries no reply route; bind its endpoint and adapter slug".to_string(),
        _ if status.is_success() => {
            "the route fields are present for this pair. This says nothing about whether the \
             worker holds a credential under that slug."
                .to_string()
        }
        _ => format!("the platform API answered {code}: {}", body.trim()),
    };
    let minted = status
        .is_success()
        .then(|| {
            serde_json::from_str::<MintedToken>(&body)
                .ok()
                .map(|m| m.token)
        })
        .flatten();
    (
        CheckResult {
            name: "binding".to_string(),
            ok: status.is_success(),
            status: Some(code),
            detail,
        },
        minted,
    )
}

/// Post the same delivery twice and assert the second answer is the platform's
/// duplicate. Opt in behind `--enqueue` because it enqueues a REAL turn.
async fn probe_round_trip(
    client: &reqwest::Client,
    opts: &SmokeTestOpts,
    profile: &AdapterProfile,
    token: Option<&str>,
) -> RoundTrip {
    let Some(token) = token else {
        return RoundTrip {
            check: CheckResult {
                name: "round_trip".to_string(),
                ok: false,
                status: None,
                detail: "no channel token was minted, so no turn could be posted".to_string(),
            },
            duplicate: None,
        };
    };
    let delivery = format!("curie-smoke-{}", uuid::Uuid::new_v4());
    let body = serde_json::json!({
        "kind": profile.kind,
        "address": opts.address,
        "delivery_id": delivery,
        "conversation_id": delivery,
        "author": "curie-adapter-smoke-test",
        "text": "curie adapter smoke test",
    });
    let url = format!("{}/channels/turns", opts.api_url.trim_end_matches('/'));

    let mut last: Option<(u16, serde_json::Value)> = None;
    for _ in 0..2 {
        let sent = client
            .post(&url)
            .header("X-API-Key", token)
            .json(&body)
            .send()
            .await;
        let response = match sent {
            Ok(response) => response,
            Err(err) => {
                return RoundTrip {
                    check: CheckResult {
                        name: "round_trip".to_string(),
                        ok: false,
                        status: None,
                        detail: format!("the platform API did not answer: {err}"),
                    },
                    duplicate: None,
                }
            }
        };
        let status = response.status().as_u16();
        let payload = response
            .json::<serde_json::Value>()
            .await
            .unwrap_or(serde_json::Value::Null);
        last = Some((status, payload));
    }

    let (status, payload) = last.expect("two attempts were made");
    let duplicate = payload
        .get("duplicate")
        .and_then(serde_json::Value::as_bool);
    RoundTrip {
        check: CheckResult {
            name: "round_trip".to_string(),
            ok: duplicate == Some(true),
            status: Some(status),
            detail: match duplicate {
                Some(true) => "the re-posted delivery converged on the first answer".to_string(),
                _ => "the re-posted delivery was not reported as a duplicate".to_string(),
            },
        },
        duplicate,
    }
}

/// The round trip check plus the platform's duplicate verdict.
#[derive(Debug, Clone)]
pub struct RoundTrip {
    pub check: CheckResult,
    pub duplicate: Option<bool>,
}

/// Output of `adapter smoke-test`.
#[derive(Debug)]
pub struct AdapterSmokeTestOutput {
    pub endpoint: String,
    pub kind: String,
    pub address: String,
    pub egress_positive: CheckResult,
    pub egress_negative: CheckResult,
    pub binding: CheckResult,
    pub round_trip: Option<RoundTrip>,
}

impl AdapterSmokeTestOutput {
    fn checks(&self) -> Vec<&CheckResult> {
        let mut checks = vec![&self.egress_positive, &self.egress_negative, &self.binding];
        if let Some(round_trip) = &self.round_trip {
            checks.push(&round_trip.check);
        }
        checks
    }

    /// `(pass, fail, not_run)`. A check that did not run is counted, never
    /// dropped, so no evidence never reads as evidence.
    fn counts(&self) -> (u64, u64, u64) {
        let mut pass = 0;
        let mut fail = 0;
        for check in self.checks() {
            if check.ok {
                pass += 1;
            } else {
                fail += 1;
            }
        }
        let not_run = u64::from(self.round_trip.is_none());
        (pass, fail, not_run)
    }
}

impl CliOutput for AdapterSmokeTestOutput {
    fn to_json(&self) -> serde_json::Value {
        let (pass, fail, not_run) = self.counts();
        serde_json::json!({
            "endpoint": self.endpoint,
            "kind": self.kind,
            "address": self.address,
            "verdict": if fail == 0 { "pass" } else { "fail" },
            "counts": {"pass": pass, "fail": fail, "not_run": not_run},
            "manual_review_required": [MANUAL_REVIEW_SECRET_ORDER],
            "egress_positive": self.egress_positive.to_json(),
            "egress_negative": self.egress_negative.to_json(),
            "binding": self.binding.to_json(),
            "round_trip": match &self.round_trip {
                Some(round_trip) => {
                    let mut value = round_trip.check.to_json();
                    value["duplicate"] = serde_json::json!(round_trip.duplicate);
                    value
                }
                None => serde_json::Value::Null,
            },
        })
    }

    fn render(&self, ui: &Ui) {
        let (pass, fail, not_run) = self.counts();
        ui.payload(&format!("{} {}", self.kind, self.endpoint));
        for check in self.checks() {
            ui.payload_plain(&format!(
                "{}  {}  {}",
                if check.ok { "pass" } else { "fail" },
                check.name,
                check.detail
            ));
        }
        ui.payload_plain(&format!("{pass} passed, {fail} failed, {not_run} not run"));
        ui.payload_plain(MANUAL_REVIEW_SECRET_ORDER);
    }
}

// ─── HTTP ────────────────────────────────────────────────────────────────────

/// The one client these verbs use. Redirects are disabled: following one would
/// replay the egress secret at whatever origin the redirect named, which is the
/// same reason the worker refuses them.
fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("building HTTP client")
}

/// `POST /channels/token` for one concrete pair, returning the raw answer so
/// each caller judges the status itself (a mint failure is fatal to `token` and
/// a reported check to `smoke-test`).
async fn post_channel_token(
    client: &reqwest::Client,
    api_url: &str,
    api_key: &str,
    kind: &str,
    address: &str,
    ttl_s: u64,
) -> Result<(reqwest::StatusCode, String)> {
    let url = format!("{}/channels/token", api_url.trim_end_matches('/'));
    let response = client
        .post(&url)
        .header("X-API-Key", api_key)
        .json(&serde_json::json!({
            "kind": kind,
            "address": address,
            "ttl_s": ttl_s,
        }))
        .send()
        .await
        .context("POST /channels/token")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Ok((status, body))
}
