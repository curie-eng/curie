//! `skill approvals` and the declared approval-route checks shared with deploy.

use super::*;

/// The env var the runner reads for the operator's approval-gate override.
pub(super) const APPROVAL_TOOLS_ENV: &str = "CURIE_APPROVAL_REQUIRED_TOOLS";

/// The manifest locations the runner's `load_approval_policy` probes, in order.
pub(super) const MANIFEST_LOCATIONS: [&str; 2] = [".claude-plugin/plugin.json", "plugin.json"];

/// One `approvalPolicy.gates[]` entry: the tool the runner intercepts plus the
/// approval route the platform binds per deployment. A local mirror of
/// `plugin_format.models.ApprovalGate`, kept private here so `scaffold`'s
/// `read_manifest` (used by `skill up`/`skill check`) keeps its narrow shape.
///
/// Both fields are `Option` so a MISSING key stays distinguishable from one
/// present but empty. Since #520 both refuse the manifest (the runner arms a
/// declared policy exactly or not at all), so the distinction no longer changes
/// the verdict -- but it still names the actual defect in the error, which is
/// the difference between a fixable message and a puzzle. `#[serde(default)]`
/// on a `String` would collapse them and report "empty" for a key that is
/// simply absent.
#[derive(Deserialize)]
pub(super) struct ApprovalGateDecl {
    pub(super) gate: Option<String>,
    pub(super) route: Option<String>,
    /// Operator opt-in (#558). Unlike `gate`/`route`, collapsing absent/false is
    /// intentional here: a bool with a safe default carries no absent-vs-empty
    /// distinction worth preserving. Unread -- this struct only mirrors the
    /// manifest shape for round-trip parsing on the display/parse path.
    #[allow(dead_code)]
    #[serde(default, rename = "grantableViaPolicy")]
    pub(super) grantable_via_policy: bool,
    /// Bundle-authored human sentence for the approval card (#2565).
    #[allow(dead_code)]
    #[serde(default)]
    pub(super) summary: Option<String>,
}

/// The manifest `approvalPolicy` object; mirrors `plugin_format.models.ApprovalPolicy`.
#[derive(Deserialize, Default)]
pub(super) struct ApprovalPolicyDecl {
    #[serde(default)]
    gates: Vec<ApprovalGateDecl>,
}

/// Just the slice of the plugin manifest this verb reads.
///
/// `name` is carried only to mirror `plugin_format.models.PluginManifest`, whose
/// sole required field it is: without it `model_validate` raises and the runner
/// arms zero gates, so a narrower struct that parsed happily would report gates
/// the runner never arms.
///
/// FORMERLY A KNOWN LIMITATION (ADR-0041), closed by #701: this struct still
/// validates only the approval-relevant subset of the manifest -- `name` +
/// `approvalPolicy` -- so on its own a manifest invalid in some OTHER modeled
/// field (say `commands: 123`) would parse into it happily and report gates as
/// armed for a manifest the runner's `PluginManifest.model_validate` rejects
/// outright. `parse_manifest_gates` closes that gap by additionally validating
/// the RAW manifest against the frozen `packages/plugin-format` JSON Schema
/// (`validate_against_plugin_format_schema`) whenever `approvalPolicy` is
/// declared -- the same condition under which the runner's
/// `resolve_approval_policy` promotes to full-manifest validation (ADR-0041
/// decision 1). That is schema-driven, not a hand-mirror of every
/// `PluginManifest` field, so it tracks the frozen contract with no manual
/// upkeep here. `cli/plugin-format-mirrors.json` + `curie dev field-parity`
/// (which now also runs `cli/tests/plugin_format_field_parity.rs`) separately
/// gate that THIS struct's own fields (and its sibling mirrors in
/// `cli/src/spec.rs`) stay honest about which `plugin_format` fields they
/// cover.
#[derive(Deserialize)]
pub(super) struct ManifestApprovals {
    name: Option<String>,
    #[serde(rename = "approvalPolicy")]
    approval_policy: Option<ApprovalPolicyDecl>,
}

/// The frozen `packages/plugin-format` JSON Schema (issue #701), embedded at
/// compile time. Committed and drift-checked by `plugin-format`'s own
/// `test_schema_compat.py` (the export is regenerated and diffed against this
/// exact file at CI), so this constant tracks the frozen contract with zero
/// manual upkeep on the Rust side: a schema change picks up automatically the
/// next time the CLI is built against this checkout.
pub(super) const PLUGIN_FORMAT_SCHEMA: &str =
    include_str!("../../../packages/plugin-format/schema/plugin-format.schema.json");

/// Validate a RAW parsed `.claude-plugin/plugin.json` body against the frozen
/// `PluginManifest` schema (issue #701, sibling of #691 on the `plugin_format`
/// seam).
///
/// `ManifestApprovals` deliberately reads only `name` + `approvalPolicy` (see
/// its doc comment): hand-mirroring every `PluginManifest` field in Rust would
/// itself be a second ungated mirror of a Python model, which is the drift
/// class this repo already tracks elsewhere (ADR-0041). Validating the raw
/// JSON against the committed schema instead means an invalid OTHER field
/// (e.g. `commands: 123`) is caught here, matching the runner's
/// `PluginManifest.model_validate` failing on the exact same input, without
/// this Rust code needing to know that field exists at all.
///
/// Returns the joined validator error messages on failure so the CLI's error
/// names the actual offending field/type rather than an approximation of one.
pub(super) fn validate_against_plugin_format_schema(
    raw: &serde_json::Value,
) -> std::result::Result<(), String> {
    static VALIDATOR: std::sync::OnceLock<jsonschema::Validator> = std::sync::OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| {
        let mut doc: serde_json::Value = serde_json::from_str(PLUGIN_FORMAT_SCHEMA).expect(
            "packages/plugin-format/schema/plugin-format.schema.json is committed and valid JSON",
        );
        // The committed document's root is the bare `$defs` container (no
        // `type`/`required` of its own); point the root at `PluginManifest`
        // instead. Same document, same `$defs`, different entry point.
        doc["$ref"] = serde_json::Value::String("#/$defs/PluginManifest".to_string());
        jsonschema::validator_for(&doc)
            .expect("plugin-format.schema.json's PluginManifest def compiles to a validator")
    });
    let errors: Vec<String> = validator
        .iter_errors(raw)
        .map(|e| format!("{e} (at instance path {})", e.instance_path()))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Output of `skill approvals`: the bundle's declared gates, or the env
/// assignment a set/clear WOULD need (this tier boots the runner from env, so
/// there is nothing to mutate; see ADR-0041).
#[derive(Debug)]
pub enum SkillApprovalsOutput {
    Gates {
        gates: Vec<(String, String)>,
    },
    Env {
        env: String,
        restart: String,
        bundle_note: String,
    },
}

impl crate::ui::CliOutput for SkillApprovalsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            SkillApprovalsOutput::Gates { gates } => {
                let gates: Vec<serde_json::Value> = gates
                    .iter()
                    .map(|(gate, route)| serde_json::json!({"gate": gate, "route": route}))
                    .collect();
                serde_json::json!({ "gates": gates })
            }
            SkillApprovalsOutput::Env {
                env,
                restart,
                bundle_note,
            } => serde_json::json!({
                "env": env,
                "restart": restart,
                "bundle_note": bundle_note,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            SkillApprovalsOutput::Gates { gates } => {
                ui.payload(&gates_summary_line(gates));
                for (gate, route) in gates {
                    ui.kv(gate, route);
                }
            }
            SkillApprovalsOutput::Env {
                env,
                restart,
                bundle_note,
            } => {
                ui.payload(&human_env_line(env));
                ui.kv("restart", restart);
                ui.kv("bundle", bundle_note);
            }
        }
    }
}

/// The human-rendered form of the `NAME=value` assignment `skill approvals`
/// hands back.
///
/// The guidance tells the caller to export this, so the human line is read as
/// shell text and the value must survive being pasted into one. A gate name is
/// only rejected here for a comma or for being whitespace-only, so `--gate 'Foo
/// Bar'` or `--gate '$(cmd)'` are accepted and would otherwise be word-split or
/// command-substituted by the shell -- the runner would receive a different gate
/// than the one printed, or the paste would execute shell text outright.
/// Quoting the right-hand side is what keeps the printed line and the applied
/// value the same string.
///
/// The `--json` `env` field deliberately does NOT get this treatment: a machine
/// consumer wants the raw assignment, not a shell literal it would have to
/// unquote.
pub(super) fn human_env_line(env: &str) -> String {
    match env.split_once('=') {
        Some((name, value)) => format!("{name}={}", shell_quote(value)),
        // Not reachable from `skill_approvals` (it always formats `NAME=`), but
        // echoing the input beats inventing an assignment that was never made.
        None => env.to_string(),
    }
}

/// The human summary line for `skill approvals`' gate view.
///
/// Scoped to what this command actually knows: it reads the bundle on disk and
/// nothing else. The runner also unions in `CURIE_APPROVAL_REQUIRED_TOOLS`,
/// resolved once at container boot and invisible from here, so neither branch may
/// present the bundle's gates as the complete effective set. Saying "no gates
/// declared, so calls run without approval" would be flatly false against a runner
/// booted with that override set.
pub(super) fn gates_summary_line(gates: &[(String, String)]) -> String {
    let unseen = "a CURIE_APPROVAL_REQUIRED_TOOLS override applied at container boot may gate more, and is not visible from the bundle";
    if gates.is_empty() {
        format!("the bundle declares no approval gates ({unseen})")
    } else {
        format!("{} bundle-declared gate(s) ({unseen}):", gates.len())
    }
}

/// Render the advisory for cron triggers that passed the authoritative bundle
/// validator. This formatter deliberately does not parse cron expressions: the
/// declaration validator remains the single authority for accepted syntax.
pub(super) fn cron_trigger_warning_from_manifest(body: &str) -> Result<Option<String>> {
    let manifest: serde_json::Value =
        serde_json::from_str(body).context("plugin manifest is not valid JSON")?;
    let Some(triggers) = manifest.get("triggers").and_then(|value| value.as_array()) else {
        return Ok(None);
    };

    let identities: Vec<String> = triggers
        .iter()
        .enumerate()
        .filter_map(|(index, trigger)| {
            if trigger.get("type").and_then(|value| value.as_str()) != Some("cron") {
                return None;
            }
            let schedule = trigger
                .get("schedule")
                .and_then(|value| value.as_str())?
                .trim();
            if schedule.is_empty() {
                return None;
            }
            let name = trigger
                .get("name")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty());
            Some(match name {
                Some(name) => {
                    serde_json::to_string(name).expect("serializing a manifest string cannot fail")
                }
                None => format!(
                    "{} with schedule {}",
                    index + 1,
                    serde_json::to_string(schedule)
                        .expect("serializing a manifest string cannot fail")
                ),
            })
        })
        .collect();

    let warning = match identities.as_slice() {
        [] => return Ok(None),
        [identity] => format!(
            "cron trigger {identity} is declared, but the skill tier has no scheduler and does not fire it; cron triggers fire only on local and cluster installs"
        ),
        [first, second] => format!(
            "cron triggers {first} and {second} are declared, but the skill tier has no scheduler and does not fire them; cron triggers fire only on local and cluster installs"
        ),
        many => {
            let (last, rest) = many.split_last().expect("cron identities are not empty");
            format!(
                "cron triggers {}, and {last} are declared, but the skill tier has no scheduler and does not fire them; cron triggers fire only on local and cluster installs",
                rest.join(", ")
            )
        }
    };
    Ok(Some(warning))
}

pub(super) fn read_bundle_manifest(plugin_dir: &Path) -> Result<(String, String)> {
    let manifest_path = MANIFEST_LOCATIONS
        .iter()
        .map(|loc| plugin_dir.join(loc))
        .find(|path| path.is_file())
        .ok_or_else(|| crate::exit::usage(crate::scaffold::no_manifest_message(plugin_dir)))?;
    let body = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    Ok((manifest_path.display().to_string(), body))
}

pub(super) fn cron_trigger_warning_from_bundle(plugin_dir: &Path) -> Result<Option<String>> {
    let (_, body) = read_bundle_manifest(plugin_dir)?;
    cron_trigger_warning_from_manifest(&body)
}

/// Read the bundle's declared approval gates as `(gate, route)` pairs.
///
/// The manifest is probed at `.claude-plugin/plugin.json` then `plugin.json`,
/// mirroring the runner's `load_approval_policy`. Since #520 that function is
/// single-tier and fail-closed: ANY gate it cannot arm exactly as declared --
/// a required key missing (the manifest's `name`, or a gate's `gate`/`route`),
/// or a key present but empty/whitespace so it keys nothing -- raises rather
/// than degrading to "nothing gated". So both shapes are reported here as one
/// usage error naming the problem. The manifest is invalid input, deterministic
/// and fixable by hand; reporting an empty list instead would read as "no gates
/// configured", a different lie (#607).
///
/// A manifest with no `approvalPolicy` at all, or an explicitly empty `gates`
/// list, declares no gate: no gates and no error. A bundle with no manifest is a
/// usage error (the plugin dir is simply wrong).
pub(super) fn read_bundle_gates(plugin_dir: &Path) -> Result<Vec<(String, String)>> {
    let (location, body) = read_bundle_manifest(plugin_dir)?;
    parse_manifest_gates(&body, &location)
}

/// Read the manifest from a PACKED tar.gz archive, not the source tree. These
/// are the same bytes `pack_tar_gz` produces and `local`/`cluster deploy`
/// upload. A source-tree read can name a manifest a
/// root `.curieignore` (or one of the packer's built-in exclusions) keeps out
/// of the archive entirely, or miss a manifest the archive packs from a
/// location the source read never looked at; reading the archive itself is
/// the only way the pre-check judges the exact manifest the platform will.
///
/// Mirrors the platform's `plugin_format.archive.bundle_root`: probes
/// `MANIFEST_LOCATIONS` at the archive root first, and only when the root
/// carries no manifest and the archive has exactly one top-level directory
/// (root-level files do not count) does it probe that one directory for a
/// manifest, matching `load_approval_policy`. Entry paths are normalized by
/// stripping a leading `./` before matching, since a tar entry may carry one
/// even though this crate's own `pack_tar_gz` does not emit it. Errors (an
/// unreadable archive, or no manifest found by that resolution) are reported
/// the same way `read_bundle_gates` reports a missing manifest, and the
/// caller treats them identically: fail-open, warn, skip the pre-check.
pub(super) fn read_packed_bundle_manifest(archive: &[u8]) -> Result<(String, String)> {
    let mut tar_archive = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    let entries = tar_archive
        .entries()
        .context("reading the packed bundle archive")?;
    let mut manifests: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut top_level_dirs: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for entry in entries {
        let mut entry = entry.context("reading a packed bundle archive entry")?;
        let raw_path = entry
            .path()
            .context("reading a packed bundle archive entry path")?
            .to_string_lossy()
            .into_owned();
        let normalized = raw_path
            .strip_prefix("./")
            .unwrap_or(raw_path.as_str())
            .trim_end_matches('/')
            .to_string();
        if normalized.is_empty() {
            continue;
        }
        if let Some((top, _)) = normalized.split_once('/') {
            top_level_dirs.insert(top.to_string());
        } else if entry.header().entry_type().is_dir() {
            top_level_dirs.insert(normalized.clone());
        }
        // Buffer any candidate manifest content, whether at the root or one
        // level under a top-level directory: which one is actually consulted
        // is decided below, once every entry has been walked.
        let is_root_candidate = MANIFEST_LOCATIONS.contains(&normalized.as_str());
        let nested_candidate = MANIFEST_LOCATIONS.iter().find_map(|loc| {
            normalized
                .strip_suffix(&format!("/{loc}"))
                .filter(|prefix| !prefix.contains('/'))
                .map(|prefix| format!("{prefix}/{loc}"))
        });
        if is_root_candidate || nested_candidate.is_some() {
            let mut content = String::new();
            std::io::Read::read_to_string(&mut entry, &mut content)
                .with_context(|| format!("reading {normalized} from the packed bundle archive"))?;
            manifests.insert(normalized, content);
        }
    }
    // Mirror the platform's `plugin_format.archive.bundle_root`: prefer a
    // manifest at the archive root; otherwise, when the archive has exactly
    // one top-level directory (root-level files do not count) and that
    // directory carries a manifest, descend into it; otherwise there is no
    // manifest to read.
    let root_manifest = MANIFEST_LOCATIONS.iter().find_map(|loc| {
        manifests
            .remove(*loc)
            .map(|content| (loc.to_string(), content))
    });
    let resolved = match root_manifest {
        Some(found) => Some(found),
        None => match top_level_dirs.len() {
            1 => {
                let dir = top_level_dirs
                    .iter()
                    .next()
                    .expect("top_level_dirs has exactly one entry")
                    .clone();
                MANIFEST_LOCATIONS.iter().find_map(|loc| {
                    let key = format!("{dir}/{loc}");
                    manifests.remove(&key).map(|content| (key, content))
                })
            }
            _ => None,
        },
    };
    resolved.ok_or_else(|| {
        crate::exit::usage(
            "the packed bundle archive contains no plugin manifest \
             (.claude-plugin/plugin.json or plugin.json)"
                .to_string(),
        )
    })
}

/// Parse the `approvalPolicy` gates out of a plugin-manifest JSON body, mirroring
/// the runner's `load_approval_policy` fail-closed semantics (#520): any declared
/// gate the runner cannot arm exactly as declared -- a missing REQUIRED key, or a
/// key present but empty -- refuses the whole manifest rather than arming a
/// subset, so this reports a usage error for both. `source` labels the manifest in
/// errors. Shared by the skill tier (manifest on local disk) and the local/cluster
/// tiers (manifest pulled from the deployed bundle over the API, #546) so both
/// read gates identically. Also validates the raw manifest against the frozen
/// `plugin_format` schema once a policy is declared (#701) -- see
/// `validate_against_plugin_format_schema`.
pub(super) fn parse_manifest_gates(body: &str, source: &str) -> Result<Vec<(String, String)>> {
    let invalid = |problem: &str| {
        crate::exit::usage(format!(
            "invalid plugin manifest {source}: {problem}. The runner rejects this manifest and arms ZERO approval gates, including any well-formed ones"
        ))
    };
    let raw: serde_json::Value =
        serde_json::from_str(body).map_err(|e| invalid(&format!("not valid JSON ({e})")))?;
    // #701: the runner only promotes to full-`PluginManifest` validation once
    // `approvalPolicy` is actually declared (an absent or explicit-null policy
    // returns the honest empty policy without reading the rest of the
    // manifest, matching `resolve_approval_policy`'s early return). Mirror that
    // gate exactly: only above this line would a manifest invalid in some
    // OTHER field silently slip past, since `ManifestApprovals` below never
    // looks at it.
    if raw.get("approvalPolicy").is_some_and(|v| !v.is_null()) {
        if let Err(detail) = validate_against_plugin_format_schema(&raw) {
            return Err(invalid(&format!(
                "manifest fails plugin_format schema validation ({detail})"
            )));
        }
    }
    let manifest: ManifestApprovals = serde_json::from_value(raw)
        .map_err(|e| invalid(&format!("does not match the expected manifest shape ({e})")))?;
    if manifest.name.is_none() {
        return Err(invalid("missing the required `name` field"));
    }
    let mut gates = Vec::new();
    for g in manifest.approval_policy.unwrap_or_default().gates {
        let (gate, route) = match (&g.gate, &g.route) {
            (None, _) => return Err(invalid("a gate in `approvalPolicy.gates` omits `gate`")),
            (_, None) => return Err(invalid("a gate in `approvalPolicy.gates` omits `route`")),
            (Some(gate), Some(route)) => (gate.trim(), route.trim()),
        };
        // Present but empty: parses, but keys nothing once trimmed, so the
        // runner refuses to boot rather than arm a partial policy (#520).
        // Reporting it as armed here would name a gate that stops the runner.
        if gate.is_empty() || route.is_empty() {
            return Err(invalid(
                "a gate in `approvalPolicy.gates` has an empty `gate` or `route`",
            ));
        }
        // The runner keys a dict by the trimmed gate name, so a repeated gate
        // collapses to one entry: the first declaration fixes the position, the
        // last one wins the route. Mirror both halves -- keeping the duplicate
        // would report a gate the runner never arms plus a stale route.
        match gates
            .iter_mut()
            .find(|(name, _): &&mut (String, String)| name == gate)
        {
            Some((_, existing_route)) => *existing_route = route.to_string(),
            None => gates.push((gate.to_string(), route.to_string())),
        }
    }
    Ok(gates)
}

/// The active deployment whose bundle is in force for an agent: prod outranks dev
/// (mirroring `binding.py`), then most recent. `list_deployments` returns rows
/// oldest-first, so "most recent" is the last match. `None` when the agent has no
/// active deployment (nothing is running its bundle yet).
pub(super) fn select_in_force_deployment(
    deployments: &[crate::api::Deployment],
) -> Option<&crate::api::Deployment> {
    let active: Vec<&crate::api::Deployment> = deployments
        .iter()
        .filter(|d| d.status == "active")
        .collect();
    active
        .iter()
        .rev()
        .find(|d| d.environment == "prod")
        .or_else(|| active.iter().rev().find(|d| d.environment == "dev"))
        .or_else(|| active.last())
        .copied()
}

/// The approval-gate tool names armed by the agent's in-force DEPLOYED bundle
/// manifest (#546): resolve the active deployment → its version → the version's
/// stored manifest → `approvalPolicy.gates[].gate`. This is the source the runner
/// consults that the platform's mutable `approval_required_tools` field does NOT
/// carry, so `local`/`cluster approvals` must union it in or it reports an empty
/// gate set while the manifest gate is armed and blocking. Best-effort on the
/// fetch (no deployment / no bundle / API hiccup → no manifest gates), but a
/// deployed manifest that is actually invalid is surfaced (it disarms every gate).
///
/// The empty-vec outcomes are NOT interchangeable, which is why this returns
/// `ManifestGates` rather than a bare list (#607): "the manifest declares nothing"
/// is an answer, "the API call failed" is the absence of one, and the caller's
/// report reads very differently for each.
pub(super) enum ManifestGates {
    /// The lookup completed. The vec is the manifest's armed gates, empty when
    /// there is no deployed bundle, no manifest in it, or no `approvalPolicy`.
    Readable(Vec<String>),
    /// The lookup did not complete, so the manifest's gates are unknown. Carries
    /// the reason, which is reported rather than swallowed.
    Unreadable(String),
}

pub(super) async fn deployed_manifest_gate_names(
    client: &ApiClient,
    agent_id: &str,
) -> Result<ManifestGates> {
    let deployments = match client.list_deployments(agent_id).await {
        Ok(d) => d,
        Err(err) => {
            return Ok(ManifestGates::Unreadable(format!(
                "listing the agent's deployments failed: {err}"
            )))
        }
    };
    // No active deployment is a real answer: nothing is running this agent's
    // bundle, so no manifest gate can be armed from one.
    let Some(deployment) = select_in_force_deployment(&deployments) else {
        return Ok(ManifestGates::Readable(Vec::new()));
    };
    // A deployment IS in force but names no version. `version_id` is
    // `#[serde(default)]`, so this is response drift rather than a stated absence
    // -- the bundle exists and we simply cannot address it.
    let Some(version_id) = deployment.version_id.clone() else {
        return Ok(ManifestGates::Unreadable(format!(
            "the in-force deployment {} reports no version id",
            deployment.id
        )));
    };
    let files = match client.bundle_files(agent_id, &version_id).await {
        Ok(f) => f,
        Err(err) => {
            return Ok(ManifestGates::Unreadable(format!(
                "fetching the deployed bundle's files failed: {err}"
            )))
        }
    };
    let Some(manifest) = files
        .iter()
        .find(|f| MANIFEST_LOCATIONS.contains(&f.path.as_str()))
    else {
        return Ok(ManifestGates::Readable(Vec::new()));
    };
    let gates = parse_manifest_gates(
        &manifest.content,
        &format!("deployed bundle manifest ({})", manifest.path),
    )?;
    Ok(ManifestGates::Readable(
        gates.into_iter().map(|(gate, _route)| gate).collect(),
    ))
}

/// The distinct approval routes a bundle declares, from `parse_manifest_gates`
/// output (#2448). That output is already trimmed and last-wins per gate, so this
/// set equals the API's `set(route_by_tool.values())` (#2436). Pinned to
/// `tests/vectors/approval-route-normalization.json` by an executed test.
pub(super) fn declared_approval_routes(gates: &[(String, String)]) -> BTreeSet<String> {
    gates.iter().map(|(_gate, route)| route.clone()).collect()
}

/// The declared routes with no entry in `bound`, sorted. Comparison is verbatim
/// and case-sensitive with no trimming of bound keys, like the API's lookup: a
/// stored `" ops "` or `"Ops"` does not bind `ops`.
pub(super) fn unbound_approval_routes<'a>(
    declared: &BTreeSet<String>,
    bound: impl IntoIterator<Item = &'a String>,
) -> Vec<String> {
    let bound: BTreeSet<&String> = bound.into_iter().collect();
    declared
        .iter()
        .filter(|route| !bound.contains(route))
        .cloned()
        .collect()
}

/// Route names joined as `"a", "b"`, Debug-quoted so padding stays visible.
pub(super) fn quoted_routes<'a>(names: impl IntoIterator<Item = &'a String>) -> String {
    names
        .into_iter()
        .map(|name| format!("{name:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Refuse a deploy whose bundle declares an approval route the resolved agent
/// does not bind (#2448), before any secrets, version, or bundle request.
///
/// Advisory and fail-open: `None` (the local policy could not be read) or an
/// empty declared set never refuses. The platform API remains the gate; it
/// refuses the same gap on `POST /deployments` whether or not this ran. The
/// refusal is a usage error whose fix is ONE full-replacement route write that
/// lists every unbound and already-bound route.
pub(super) fn check_deploy_routes_bound(
    declared: Option<&BTreeSet<String>>,
    plugin_name: &str,
    agent: &crate::api::Agent,
    channel: &ChannelOutcome,
    tier: DeployTier,
) -> Result<()> {
    let Some(declared) = declared.filter(|d| !d.is_empty()) else {
        return Ok(());
    };
    let bound: Vec<&String> = agent
        .approval_routes
        .as_ref()
        .map(|routes| routes.keys().collect())
        .unwrap_or_default();
    let unbound = unbound_approval_routes(declared, bound.iter().copied());
    if unbound.is_empty() {
        return Ok(());
    }
    let name = &agent.name;
    let bound_clause = if bound.is_empty() {
        "this agent binds no approval routes".to_string()
    } else {
        format!("bound routes are {}", quoted_routes(bound.iter().copied()))
    };
    let state_clause = match channel {
        ChannelOutcome::Created(_) => format!(
            "The agent {name} was created by this deploy so its routes can be bound; \
             no version, bundle, or deployment was created."
        ),
        _ => "No version, bundle, or deployment was created.".to_string(),
    };
    let message = format!(
        "refusing to deploy {plugin_name} as agent {name}: the bundle declares approval \
         route(s) {} with no entry in this agent's approval_routes; {bound_clause}. \
         {state_clause}",
        quoted_routes(&unbound)
    );
    let tier_word = match tier {
        DeployTier::Local => "local",
        DeployTier::Cluster => "cluster",
    };
    let mut fix = format!(
        "bind every declared route in ONE write, then re-run this deploy: curie {tier_word} \
         approvals {name}"
    );
    for route in unbound.iter().chain(bound.iter().copied()) {
        fix.push_str(&format!(
            " --route-resolution {route}=<channel> --route-approvers {route}=users:<user-id>"
        ));
    }
    // #2902: an operator principal resolves only a route bound to an explicit
    // user list, so name it here rather than leave the channel-members default.
    fix.push_str(
        " (the explicit users list is what lets an operator principal resolve these \
         approvals from the CLI; drop --route-approvers to leave approval to channel members)",
    );
    if !bound.is_empty() {
        fix.push_str(
            " (a route write replaces the whole map, so this repeats the routes already bound; \
             use --routes-from <file> instead to keep an existing notification or approver set)",
        );
    }
    if tier == DeployTier::Cluster {
        fix.push_str(" with the same --namespace/--release/--api-url you passed to this deploy");
    }
    Err(crate::exit::CliError::usage(message).with_fix(fix).into())
}

/// One deployed version with at least one active deployment, and the routes its
/// stored manifest declares (#2448).
pub(super) struct DeclaringVersion {
    pub(super) version_id: String,
    /// `(deployment id, environment)` for every active deployment on it.
    pub(super) deployments: Vec<(String, String)>,
    pub(super) routes: BTreeSet<String>,
}

/// The declared routes of every ACTIVE deployment in both environments, grouped
/// by distinct version so each version's files are read once (#2448).
///
/// Best-effort input to an advisory check: a failed deployment list, an active
/// row with no version id, a failed files read, or an unparseable manifest each
/// becomes an unreadable reason in the second return value, never an error. The
/// platform API remains the gate for a route write either way. The manifest is
/// chosen by `MANIFEST_LOCATIONS` precedence; a bundle with no manifest declares
/// nothing.
pub(super) async fn active_declared_routes(
    client: &ApiClient,
    agent_id: &str,
) -> (Vec<DeclaringVersion>, Vec<String>) {
    let mut unreadable = Vec::new();
    let deployments = match client.list_deployments(agent_id).await {
        Ok(d) => d,
        Err(err) => {
            return (
                Vec::new(),
                vec![format!("listing the agent's deployments failed: {err:#}")],
            )
        }
    };
    let mut groups: Vec<(String, Vec<(String, String)>)> = Vec::new();
    for deployment in deployments.iter().filter(|d| d.status == "active") {
        let Some(version_id) = deployment.version_id.clone() else {
            unreadable.push(format!(
                "the active deployment {} reports no version id",
                deployment.id
            ));
            continue;
        };
        let row = (deployment.id.clone(), deployment.environment.clone());
        match groups.iter_mut().find(|(v, _)| *v == version_id) {
            Some((_, rows)) => rows.push(row),
            None => groups.push((version_id, vec![row])),
        }
    }
    let mut declaring = Vec::new();
    for (version_id, deployments) in groups {
        let files = match client.bundle_files(agent_id, &version_id).await {
            Ok(f) => f,
            Err(err) => {
                unreadable.push(format!(
                    "fetching the files of version {version_id} failed: {err:#}"
                ));
                continue;
            }
        };
        let manifest = MANIFEST_LOCATIONS
            .iter()
            .find_map(|loc| files.iter().find(|f| f.path == *loc));
        let routes = match manifest {
            None => BTreeSet::new(),
            Some(manifest) => match parse_manifest_gates(
                &manifest.content,
                &format!("deployed bundle manifest (version {version_id})"),
            ) {
                Ok(gates) => declared_approval_routes(&gates),
                Err(err) => {
                    unreadable.push(format!("{err:#}"));
                    continue;
                }
            },
        };
        declaring.push(DeclaringVersion {
            version_id,
            deployments,
            routes,
        });
    }
    (declaring, unreadable)
}

/// The local refusal for a route write that drops a route an active deployment
/// still declares (#2448), or `None` when the proposed keys keep every one.
///
/// Pure and advisory: it judges only the versions the caller could read, and the
/// platform API remains the gate for the PATCH itself.
pub(super) fn route_write_refusal(
    agent_name: &str,
    proposed: &BTreeMap<String, crate::api::ApprovalRouteBindingWrite>,
    declaring: &[DeclaringVersion],
) -> Option<anyhow::Error> {
    let mut removed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for version in declaring {
        for route in unbound_approval_routes(&version.routes, proposed.keys()) {
            let by = removed.entry(route).or_default();
            for (id, env) in &version.deployments {
                by.push(format!(
                    "deployment {id} ({env}, version {})",
                    version.version_id
                ));
            }
        }
    }
    if removed.is_empty() {
        return None;
    }
    let routes: Vec<String> = removed.keys().cloned().collect();
    let details = removed
        .iter()
        .map(|(route, by)| format!("{route:?} by {}", by.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    let message = format!(
        "refusing to write approval routes for {agent_name}: this write removes approval \
         route(s) {} that active deployment(s) still declare: {details}. The platform API \
         refuses this write, so nothing was sent.",
        quoted_routes(&routes)
    );
    let example = routes.first().map(String::as_str).unwrap_or("<route>");
    let fix = format!(
        "keep every declared route in the write (for example add --route-resolution \
         {example}=<channel>, or keep it in the --routes-from file), or first end every active \
         deployment that declares it (DELETE /deployments/<id> on the platform API); deploying \
         a version that declares no gates is not enough, because older active deployments keep \
         declaring the route"
    );
    Some(crate::exit::CliError::usage(message).with_fix(fix).into())
}

/// POSIX-shell-quote a value for safe interpolation into emitted shell text.
///
/// Two callers, both emitting text a caller reads as shell: the bundle path named
/// by the `restart` guidance, and the right-hand side of the human-rendered
/// `NAME=value` assignment the guidance says to export. A value holding
/// whitespace or shell metacharacters (`/tmp/my bundle`, `$(cmd)`) would
/// otherwise be word-split or substituted, so what the shell sees differs from
/// what we printed. Single-quoting is the one POSIX form that quotes every
/// character literally; the only byte it cannot contain is
/// `'` itself, which is escaped by closing the quote, emitting an escaped quote,
/// and reopening (`'\''`). Done by hand rather than by pulling in a crate: the
/// rule is four lines and a dependency here is not worth the supply-chain surface.
///
/// Not shared with `ops::shell_quote`, which is deliberately different: that one
/// leaves shell-safe tokens bare because it renders helm `--set` argv for humans
/// to read, where quoting every token is noise. This one always quotes, because
/// these values are copied into a shell and an unquoted one is a silent
/// mis-target rather than a visible mistake.
pub(super) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// `skill approvals [--plugin-dir DIR] [--gate TOOL]... [--clear]`: view the
/// bundle's declared approval gates, or print the env assignment that sets or
/// clears the runner's override.
///
/// Unlike the `local`/`cluster` tiers there is no platform record to PATCH: this
/// tier's runner resolves `CURIE_APPROVAL_REQUIRED_TOOLS` once at container
/// boot. So set/clear mutate nothing and instead hand back the assignment plus
/// the two caveats that make it honest (issue #459).
pub async fn skill_approvals(
    plugin_dir: PathBuf,
    gate: Vec<String>,
    clear: bool,
) -> Result<SkillApprovalsOutput> {
    if clear && !gate.is_empty() {
        return Err(crate::exit::usage(
            "--clear cannot be combined with --gate (clear removes the env override)",
        ));
    }
    for g in &gate {
        if g.trim().is_empty() {
            return Err(crate::exit::usage("--gate cannot be empty"));
        }
        if g.contains(',') {
            return Err(crate::exit::usage(format!(
                "--gate {g:?} cannot contain a comma: {APPROVAL_TOOLS_ENV} is comma-separated"
            )));
        }
    }
    if !clear && gate.is_empty() {
        return Ok(SkillApprovalsOutput::Gates {
            gates: read_bundle_gates(&plugin_dir)?,
        });
    }
    // The set/clear path emits guidance that names this bundle and tells the
    // caller to re-boot a runner for it, so it must be at least as sure the
    // bundle exists as the view path is -- otherwise `--plugin-dir /does/not/exist`
    // exits 0 with instructions that fail at `skill up`, which is the tier-parity
    // lie this command exists to avoid (issue #459). Same resolution and same
    // validation as the view path, deliberately reusing the one function so the
    // two paths cannot diverge on what counts as a usable bundle. A manifest
    // present and valid but declaring no `approvalPolicy` yields an empty list
    // and no error: setting an override for a bundle that declares no gates is
    // exactly the legitimate case, so only a missing, unreadable, or invalid
    // manifest is rejected. The gates themselves are irrelevant here; the call is
    // for its validation.
    read_bundle_gates(&plugin_dir)?;
    let tools: Vec<&str> = gate.iter().map(|g| g.trim()).collect();
    Ok(SkillApprovalsOutput::Env {
        env: format!("{APPROVAL_TOOLS_ENV}={}", tools.join(",")),
        // This states the MECHANISM and the DELTA; it deliberately does not
        // synthesize a command line to paste. `skill up` carries the runner's
        // whole configuration in its flags (`StartOpts`: image, port, name,
        // network, otel_endpoint, budget, model, local_model, fake_model, and
        // repeatable secret), and `skill approvals` reads only the bundle on
        // disk -- it has no idea which of those the caller passed. A synthesized
        // `skill up --secret ...` would therefore re-boot the runner on DEFAULTS
        // plus the approval var: a different model provider, a different image
        // and port, and every other `--secret` connector credential silently
        // dropped. Naming the caller's own invocation as the thing to re-run is
        // the only form that stays true without knowing it.
        //
        // The clauses that remain are each verifiable:
        // 1. `skill up` forwards an env var into the runner only when its NAME is
        //    on the passthrough list, and the model-credential names are all that
        //    list holds by default (`select_passthrough_env`). `--secret NAME`
        //    appends to it (`merge_secret_env`), so a re-run without it arms
        //    nothing.
        // 2. `start` hard-errors when a runner is already recorded for the dir, so
        //    an existing runner must be stopped first.
        // 3. `stop` takes no args and hardcodes `Path::new(".")`, so `skill down`
        //    can only act on the bundle in the CWD -- there is no `--plugin-dir`
        //    for it. Naming the bundle dir (shell-quoted, since it is read as a
        //    path in shell text) tells a caller working elsewhere which bundle
        //    this output is about.
        restart: format!(
            "env resolves once at container boot, so nothing changes until the runner re-boots. This output is about the bundle at {}. To apply it: export the assignment above, then re-run your own original `curie skill up` invocation for that bundle with `--secret {APPROVAL_TOOLS_ENV}` added -- a plain `curie skill up` does not forward it. This command cannot see how that runner was started, so re-run your invocation rather than a fresh one, which would boot on defaults and drop your other flags. Stop an already-recorded runner first with `curie skill down`, run from that bundle directory (it takes no --plugin-dir and acts on the bundle in the current directory).",
            shell_quote(&plugin_dir.display().to_string())
        ),
        // The runner UNIONS the bundle's declared gates with this env override,
        // so saying only "set/cleared" would lie by omission about what is armed.
        bundle_note: if clear {
            "clears only the env override; gates declared in the bundle manifest stay armed"
                .to_string()
        } else {
            "adds to the gates declared in the bundle manifest; it cannot remove one".to_string()
        },
    })
}

// The reason/alternative for each tier-unavailable skill verb has TWO consumers:
// the runtime `{error, fix}` payload built by `exit::unsupported` below, and the
// clap `about` text in `main.rs` that flows into the committed
// `command-manifest.json` (the discovery surface the UI parity mirror reads).
// Nothing gates prose against prose, so they are single-sourced here: a stale
// help string is the same class of lie as a stale runtime answer, just on the
// discovery surface (issue #459, ADR-0041).
