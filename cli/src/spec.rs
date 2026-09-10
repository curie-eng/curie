//! `curie init --from-spec`: the agent-authored spec model + strict parse.
//!
//! A coding agent interviews the human and writes a JSON spec; the CLI scaffolds
//! a runnable bundle from it with zero interactive prompts (ADR-0021 decision 5).
//! Both structs use `deny_unknown_fields` so an authoring typo (e.g. `skils`)
//! fails loud rather than silently dropping the intended field -- the verify-first
//! ethos applied to the spec itself. The `evals` field reuses the frozen
//! `crate::evals::EvalCase` type directly, so a spec's eval shape is locked to the
//! same contract `curie skill eval` loads and cannot drift.

use anyhow::{anyhow, bail, Result};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::evals::{validate_suite, EvalCase};
use crate::scaffold::valid_name;

/// One skill an agent describes: it becomes a `skills/<name>/SKILL.md`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillSpec {
    pub name: String,
    pub description: String,
    /// Verbatim Claude Code `allowed-tools` values; empty omits the key entirely.
    /// The scaffold renders them as ONE canonical space-separated scalar
    /// (ADR-0135), so `validate` refuses any entry that cannot survive that join.
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    pub instructions: String,
}

/// One approval gate a spec declares: the fully-namespaced LIVE tool name and the
/// route it escalates to. Mirrors `plugin_format.models.ApprovalGate`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalGateSpec {
    pub gate: String,
    pub route: String,
    /// Operator opt-in: lets a policy-gate approval mint a one-shot tool grant
    /// for this gate (#558). Unlike `gate`/`route`, where absent-vs-empty is
    /// load-bearing, a bool with a safe default may collapse absent and false.
    #[serde(default, rename = "grantableViaPolicy")]
    pub grantable_via_policy: bool,
    /// Bundle-authored human sentence for the approval card (#2565).
    #[serde(default)]
    pub summary: Option<String>,
}

/// The `approvalPolicy` a spec declares. Mirrors `plugin_format.models.ApprovalPolicy`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalPolicySpec {
    #[serde(default)]
    pub gates: Vec<ApprovalGateSpec>,
}

/// The whole bundle an agent describes. `connectors` is the raw `.mcp.json`
/// `mcpServers` map (server name -> server object) and is written through
/// verbatim after validation. `secrets` (names only, ADR-0009) and
/// `approval_policy` let a spec express a gated, authed agent without hand-editing
/// the manifest afterwards (#549); both default to empty so existing specs parse.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSpec {
    pub name: String,
    pub description: String,
    pub skills: Vec<SkillSpec>,
    #[serde(default)]
    pub connectors: Map<String, Value>,
    /// Connector-secret NAMES the bundle needs at launch (`--secret <NAME>`); no
    /// values, per ADR-0009. Written to the manifest's `secrets` array.
    #[serde(default)]
    pub secrets: Vec<String>,
    /// Approval gates to arm at runtime. Written to the manifest's `approvalPolicy`.
    #[serde(default, rename = "approvalPolicy")]
    pub approval_policy: Option<ApprovalPolicySpec>,
    pub evals: Vec<EvalCase>,
}

/// Whether an `.mcp.json` connector object is usable: it must define `command`
/// (stdio) or `url` (remote), and any `command`/`url` present must be a string --
/// mirroring `plugin_format` `_validate_mcp_object` + the `McpServer` string typing.
/// The single accept decision the shared corpus (#491) tests against, so the CLI
/// cannot silently drift from what the server-side `validate_bundle` accepts.
pub(crate) fn mcp_object_valid(value: &Value) -> bool {
    let obj = value.as_object();
    let string_typed = ["command", "url"].iter().all(|key| {
        obj.and_then(|o| o.get(*key))
            .is_none_or(|field| field.is_null() || field.is_string())
    });
    let defines = |key: &str| obj.and_then(|o| o.get(key)).is_some_and(|v| v.is_string());
    string_typed && (defines("command") || defines("url"))
}

/// Reject an `mcp__`-prefixed approval gate that is not a fully-namespaced live
/// tool name for one of this bundle's declared connectors. Non-`mcp__` gates
/// (built-ins like `Bash`/`Write`) pass untouched. Mirrors the namespacing check
/// in `plugin_format` `_validate_approval_policy`.
fn validate_gate_namespacing(
    bundle: &str,
    connectors: &Map<String, Value>,
    gate: &str,
) -> Result<()> {
    if !gate.starts_with("mcp__") {
        return Ok(());
    }
    // Try each declared connector's live prefix; the gate must carry a non-empty
    // tool suffix after it.
    for server in connectors.keys() {
        let prefix = format!("mcp__plugin_{bundle}_{server}__");
        if let Some(tool) = gate.strip_prefix(&prefix) {
            if tool.is_empty() {
                bail!(
                    "spec approvalPolicy gate {gate:?} names connector {server:?} but has no tool after the prefix; expected {prefix}<tool>"
                );
            }
            return Ok(());
        }
    }
    let expected: Vec<String> = connectors
        .keys()
        .map(|s| format!("mcp__plugin_{bundle}_{s}__<tool>"))
        .collect();
    let hint = if expected.is_empty() {
        "the spec declares no connectors, so it can gate only built-in tools (e.g. Bash)"
            .to_string()
    } else {
        format!("expected one of: {}", expected.join(", "))
    };
    bail!("spec approvalPolicy gate {gate:?} is not a fully-namespaced live tool name; {hint}")
}

/// Whether `entry` survives the canonical `allowed-tools` round-trip. The
/// scaffold joins the entries with a space into one scalar, and every consumer
/// splits that scalar back apart on whitespace or a comma at paren DEPTH 0
/// (`plugin_format.parse_allowed_tools`). So an entry carrying a depth-0
/// separator comes back as two, and an entry whose parens never balance swallows
/// the entry after it -- either way the list a consumer reads is not the list the
/// author wrote. Depth-awareness is what makes `Bash(git commit:*)`, a real
/// Claude Code permission rule, ACCEPTABLE: its space sits at depth 1.
///
/// Why refuse rather than absorb: the runner's #1852 gate-shadow check reads
/// these entries, and a corrupted one drops out of shadow detection entirely
/// (`_entry_tool("Bash(git")` is `None`, `_entry_tool("commit:*)")` is a garbage
/// tool name), so the bundle would boot reporting a gate as armed while the tool
/// runs unapproved. Refused before any write, like the skill-name rule above.
fn round_trips_in_allowed_tools(entry: &str) -> bool {
    if entry.is_empty() {
        return false;
    }
    let mut depth = 0usize;
    for ch in entry.chars() {
        match ch {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            // A close paren at depth 0 is unbalanced; a comma or whitespace at
            // depth 0 is a separator. Both are fine at depth >= 1 (`Bash(a,b)`).
            ')' => return false,
            ',' if depth == 0 => return false,
            c if depth == 0 && c.is_whitespace() => return false,
            _ => {}
        }
    }
    depth == 0
}

/// Deserialize the spec JSON (strict) then validate it. Returns actionable
/// errors that name the offending value so an agent can self-correct.
pub fn parse(json: &str) -> Result<AgentSpec> {
    // Inline serde's message (not just wrap it) so `deny_unknown_fields` errors
    // that name the offending field -- e.g. "unknown field `skils`" -- survive an
    // anyhow `.to_string()`, which only renders the outermost context otherwise.
    let spec: AgentSpec = serde_json::from_str(json)
        .map_err(|err| anyhow!("spec is not a valid agent spec (JSON): {err}"))?;
    validate(&spec)?;
    Ok(spec)
}

fn validate(spec: &AgentSpec) -> Result<()> {
    if !valid_name(&spec.name) {
        bail!(
            "spec name {:?} must be kebab-case (lowercase letters, digits, hyphens)",
            spec.name
        );
    }
    if spec.skills.is_empty() {
        bail!("spec must define at least one skill");
    }

    // Skill names must be valid kebab and unique: they collide on the same
    // `skills/<name>/SKILL.md` path otherwise, which we refuse before any write.
    let mut seen = std::collections::HashSet::new();
    for skill in &spec.skills {
        if !valid_name(&skill.name) {
            bail!(
                "spec skill name {:?} must be kebab-case (lowercase letters, digits, hyphens)",
                skill.name
            );
        }
        if !seen.insert(skill.name.as_str()) {
            bail!(
                "spec has two skills named {:?}; skill names must be unique",
                skill.name
            );
        }
        // Same ethos, one field over: the emitted `allowed-tools` scalar is
        // separator-delimited at paren depth 0, so an entry that cannot survive
        // the join would be silently rewritten into a different list. Refuse it
        // here, before any write, naming the entry the author has to fix.
        for entry in &skill.allowed_tools {
            if !round_trips_in_allowed_tools(entry) {
                bail!(
                    "spec skill {:?} allowed_tools entry {:?} cannot round-trip the canonical \
                     `allowed-tools` scalar: entries are joined with a space and split back apart \
                     on whitespace or a comma at paren depth 0, so each entry must be non-empty, \
                     carry no whitespace or comma outside parentheses, and balance its \
                     parentheses. A separator INSIDE a specifier is fine -- split the entry, or \
                     close the parenthesis",
                    skill.name,
                    entry
                );
            }
        }
    }

    if spec.evals.is_empty() {
        bail!("spec must define at least one eval case");
    }

    // Mirror plugin_format `_validate_mcp_object`: a connector object is only
    // usable if it defines `command` (stdio) or `url` (remote); reject anything
    // else rather than scaffold a broken `.mcp.json`. When a field is present it
    // must be a STRING: the frozen `McpServer` types `command`/`url` as `str`, so
    // a non-string here (e.g. `{"command": 42}`) would pass this gate but make
    // `validate_bundle` reject the emitted `.mcp.json` -- catch it now with a
    // message that names the offending field.
    for (name, value) in &spec.connectors {
        if mcp_object_valid(value) {
            continue;
        }
        // Rejected -- re-derive the specific reason for an actionable message.
        let obj = value.as_object();
        for key in ["command", "url"] {
            if let Some(field) = obj.and_then(|o| o.get(key)) {
                if !field.is_null() && !field.is_string() {
                    bail!("connector {name:?} field '{key}' must be a string");
                }
            }
        }
        bail!("connector {name:?} must define either 'command' (stdio) or 'url' (remote)");
    }

    // Secret NAMES must look like env vars (#549); the same syntax gate `secrets
    // set`/`--secret` apply. Reserved-name rejection is deliberately left to the
    // deploy-time `validate_bundle` backstop (the Rust CLI does not mirror the
    // reserved set -- drift risk -- exactly as `unbound_declared_secrets` does).
    for name in &spec.secrets {
        crate::secrets::validate_name(name)
            .map_err(|e| anyhow!("spec secret name {name:?} is invalid: {e}"))?;
    }

    // Approval gates: mirror plugin_format `_validate_approval_policy`. Each gate
    // needs a non-empty `gate` and `route`, and an `mcp__`-prefixed gate must be a
    // fully-namespaced LIVE tool name `mcp__plugin_<bundle>_<server>__<tool>`
    // (bundle = spec name, server = a declared connector). A bare `mcp__<server>__`
    // or a prefix with no tool suffix silently fails to gate at runtime, so reject
    // it now with a message that shows the expected shape.
    if let Some(policy) = &spec.approval_policy {
        for g in &policy.gates {
            if g.gate.trim().is_empty() {
                bail!("spec approvalPolicy gate has an empty 'gate'");
            }
            if g.route.trim().is_empty() {
                bail!("spec approvalPolicy gate {:?} has an empty 'route'", g.gate);
            }
            validate_gate_namespacing(&spec.name, &spec.connectors, g.gate.trim())?;
        }
    }

    // Reuse the frozen suite-level discipline (empty-cases + regex compile) so a
    // spec's evals are held to the exact contract `curie skill eval` enforces.
    // Borrow the spec's own name/evals directly rather than cloning them into a
    // throwaway suite purely to validate.
    validate_suite(&spec.name, &spec.evals)?;

    Ok(())
}

#[cfg(test)]
mod corpus_tests {
    use super::mcp_object_valid;
    use crate::scaffold::valid_name;
    use serde_json::Value;

    // The shared cross-language corpus (#491): the SAME file the Python
    // plugin-format test asserts against, so a name/mcp rule change on one side
    // without the other fails a corpus test here or there.
    const CORPUS: &str = include_str!("../../packages/plugin-format/schema/name-mcp.fixture.json");

    fn corpus() -> Value {
        serde_json::from_str(CORPUS).expect("corpus is valid JSON")
    }

    #[test]
    fn valid_name_matches_the_shared_corpus() {
        let c = corpus();
        for n in c["valid_names"].as_array().unwrap() {
            let n = n.as_str().unwrap();
            assert!(valid_name(n), "corpus valid name rejected: {n:?}");
        }
        for n in c["invalid_names"].as_array().unwrap() {
            let n = n.as_str().unwrap();
            assert!(!valid_name(n), "corpus invalid name accepted: {n:?}");
        }
    }

    #[test]
    fn mcp_object_valid_matches_the_shared_corpus() {
        let c = corpus();
        for obj in c["valid_mcp"].as_array().unwrap() {
            assert!(mcp_object_valid(obj), "corpus valid mcp rejected: {obj}");
        }
        for obj in c["invalid_mcp"].as_array().unwrap() {
            assert!(!mcp_object_valid(obj), "corpus invalid mcp accepted: {obj}");
        }
    }
}
