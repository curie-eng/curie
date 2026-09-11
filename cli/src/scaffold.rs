//! `curie init`: scaffold a Claude Code plugin bundle.
//!
//! Seven files. The deployed bundle shape matches the frozen `plugin-format`
//! package: a manifest at `.claude-plugin/plugin.json`, a genericized starter
//! `skills/<name>/SKILL.md` with YAML frontmatter, a root `.mcp.json`, an
//! empty-but-documented `connectors.yaml` and `deploy.yaml`, plus a
//! CLI-local `evals/cases.json` seed (a suite object
//! `{name, cases: [{id, input, grader}]}`, hand-mirroring the frozen eval-case
//! schema) for `curie skill eval` -- a single smoke case whose grader is
//! falsifiable: it requires the agent to name itself, so an empty, broken, or
//! off-topic turn fails it (#527). Under `--fake-model` that grader never runs
//! at all -- the fake tier is a plumbing fixture and reports the non-graded
//! `plumbing_ok` (ADR-0055). Two more files teach the developer's coding agent how to
//! drive the harness: a root `AGENTS.md` (the cross-agent auto-scanned standard)
//! and an installable primer skill at `.claude/skills/using-curie/SKILL.md`
//! whose body is rendered from `guide::primer_markdown()` so it can never drift
//! from `curie guide`. Names are kebab-case per the validator.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::Value;

use crate::evals::EvalSuite;
use crate::spec::{AgentSpec, SkillSpec};

/// Kebab-case check mirroring plugin_format's `^[a-z0-9]+(-[a-z0-9]+)*$`.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Derive a kebab-case plugin name from a directory's own name, for `init
/// --adopt` (#745). Lowercases, turns each run of non-alphanumerics into a
/// single hyphen, and trims leading/trailing hyphens, so the result satisfies
/// `valid_name`. Returns None when nothing usable remains (the caller then asks
/// for an explicit NAME).
pub(crate) fn derive_plugin_name(dir: &Path) -> Option<String> {
    let base = dir
        .canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|s| s.to_string_lossy().into_owned()))?;
    let mut out = String::new();
    let mut prev_hyphen = true; // start true to suppress any leading hyphen
    for ch in base.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_hyphen = false;
        } else if !prev_hyphen {
            out.push('-');
            prev_hyphen = true;
        }
    }
    let name = out.trim_end_matches('-').to_string();
    (!name.is_empty() && valid_name(&name)).then_some(name)
}

fn manifest(name: &str) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "name": name,
        "description": format!("The {name} agent plugin."),
        "systemPrompt": format!("You are the {name} agent. Help users with {name} requests."),
        "version": "0.1.0",
    }))
    .expect("static manifest serializes")
}

/// The genericized starter skill for `<name>`: a believable, editable
/// placeholder scoped to the bundle name (no weather demo). Keeps the
/// correct-by-example `allowed-tools` key -- now the single space-separated
/// scalar the Agent Skills specification calls canonical (ADR-0135), since this
/// template is a teaching surface and whatever it shows is what authors copy --
/// and the when/how/rules section shape a real skill uses. The two tool names
/// are literals we control, so a bare scalar is safe here exactly as it already
/// is for `name:`; `render_skill_md` quotes its joined value because those
/// entries are arbitrary agent-authored text.
fn skill_md(name: &str) -> String {
    format!(
        "---\nname: {name}\ndescription: Starter skill for the {name} agent. Replace this description with when the agent should invoke this skill -- it is the routing signal.\nallowed-tools: WebSearch WebFetch\n---\n\n# {name}\n\nThis is the starter skill scaffolded by `curie init`. Replace each section\nbelow with your agent's real behavior; keep the section shape.\n\n## When to run\nDescribe the requests this skill should handle.\n\n## How to answer\n1. Numbered, concrete steps the agent follows.\n2. Prefer verifiable sources and tools over recall.\n\n## Hard rules\n- Never invent an answer. If you cannot find one, say so and name what you tried.\n- Keep replies short enough to read in Slack without expanding.\n"
    )
}

/// The `evals/cases.json` seed: a single smoke case named for `<name>`, graded
/// by a falsifiable `contains: <name>` (#527) -- the agent must name itself, so
/// a broken, empty, or off-topic turn is red.
///
/// The two tiers reach that grader differently, and neither is "green out of the
/// box". Under a real credential the case is graded and a correct introduction
/// passes it. Under `--fake-model` the case is NOT graded at all: the fake
/// answers every input with the same canned text, so `skill eval` reports the
/// non-graded `plumbing_ok` -- exit 0, but a claim about the plumbing, not about
/// the agent (ADR-0055). Replacing this with a real domain grader is the
/// author's first step either way.
fn eval_cases(name: &str) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "name": name,
        "cases": [
            {
                "id": format!("{name}-smoke"),
                "input": format!("In one short sentence, introduce yourself as the {name} agent."),
                // Assert the agent NAMED itself, not `contains: ""` (which passed
                // on any output, even an empty or errored turn). A broken agent
                // that produces no reply -- or an off-topic one -- fails this,
                // while a correct introduction contains the name the input asked
                // for. Replace with a real domain grader as the first authoring
                // step (see the scaffolded AGENTS.md).
                "grader": {
                    "kind": "contains",
                    "expected": name,
                    "case_sensitive": false,
                },
            }
        ],
    }))
    .expect("static eval cases serialize")
}

/// The root `AGENTS.md`: the cross-agent auto-scanned standard carrying the
/// non-discoverable operating rules (the authoring loop, eval-as-promotion-gate,
/// verify-first) and a pointer to `curie guide` for the full primer and the
/// current landmine list.
fn agents_md(name: &str) -> String {
    format!(
        "# Agent instructions: {name}\n\nThis is a Curie bundle (a Claude Code plugin shape). The full harness\nprimer is one command away and is the source of truth:\n\n    curie guide\n\n## The loop\n\n1. `curie skill up --fake-model` -- boot the runner offline, no credential.\n   The fake model is plumbing, not a subject under test: it answers every input\n   with the same canned reply, so nothing it says is evidence about behavior.\n2. Edit `skills/{name}/SKILL.md` (behavior) and `evals/cases.json` (the contract).\n3. `curie skill up --fake-model` -- the runner executes an immutable\n   snapshot of the bundle taken at `skill up`, so a `SKILL.md` edit reaches it\n   ONLY after a restart. A second `up` from this directory replaces the recorded\n   runner automatically when the source changed; `--replace` still forces a\n   restart of an unchanged snapshot. Skip this and step 4 grades the pre-edit bundle with\n   no sign anything is stale. (`evals/cases.json` is read live from source, so\n   the contract does not need the restart -- only the behavior does.) Confirm\n   what is loaded with `curie skill status --json` and its `bundle_digest`.\n4. `curie skill eval` under `--fake-model` reports `plumbing_ok` -- it proves\n   the turn completed, and grades nothing. Re-run it with a real credential to\n   grade the cases; that green is the promotion gate. Merging to main promotes.\n5. `curie skill down` when finished.\n\n## What this bundle declares\n\n- `skills/{name}/SKILL.md` -- behavior. The main thing to edit.\n- `evals/cases.json` -- the promotion gate. Make it FALSIFIABLE.\n- `connectors.yaml` -- what this agent needs RUNNING (an MCP server Curie\n  hosts for it). Scaffolded empty and commented; Curie derives the Deployment,\n  Service, hardening, host allowlist, NetworkPolicy, and the URL the agent\n  dials, so none of that belongs in this repo. Never hand-write that URL: it\n  embeds the release, the agent, and the namespace, so it differs per agent.\n- `deploy.yaml` -- WHERE this bundle goes. `curie cluster deploy --target prod`\n  reads it, so routing is a reviewable diff instead of flags in CI. The bundle\n  is identical across targets; only the binding differs.\n\n## Rules\n\n- Verify before running: `curie schema` lists every real command; never\n  invoke one you have not confirmed.\n- The eval file is the promotion gate and never changes across tiers\n  (skill/local/cluster). Grading, and therefore green and red, is a\n  real-credential concept; never deploy on red.\n- Landmines: run `curie guide` (or read\n  `.claude/skills/using-curie/SKILL.md`) for the full, current list.\n- If a step genuinely needs human sign-off, do NOT collect it in the skill. Call\n  the built-in `request_approval` tool only when it is available, then end the\n  turn: the platform posts the card, authorizes the resolver server-side, and\n  resumes you with a turn beginning `[approval resolved]`, which this skill must\n  handle. If the tool is absent, explain that the bundle cannot perform the\n  action; do not fabricate a remediation request. `curie guide` has the section;\n  `docs/approvals.md` in the Curie repo has the full walkthrough.\n- The scaffolded eval is a starter smoke test: it only checks the agent named\n  itself, so it fails on an empty/errored turn but proves nothing about the\n  real work. Replace it with a FALSIFIABLE grader -- one a plausibly-broken\n  agent would fail -- as the first authoring step (ADR-0022).\n- A bare greeting (\"hey\", \"hi\") is answered by the real model by default --\n  a full sandbox claim and model turn for something a canned reply could\n  handle for free. If this agent gets greeted often, consider a `greeting`\n  behavior pack: `GET`/`PUT /agents/{{id}}/behavior-packs` (no CLI verb yet)\n  short-circuits a bare greeting/help request before the model ever runs.\n  See `docs/behavior-packs.md`.\n"
    )
}

/// The installable harness primer skill at `.claude/skills/using-curie/`,
/// auto-discovered by Claude Code as a PROJECT skill. Frontmatter (guidance-only,
/// so no `allowed-tools`) followed by the guide body VERBATIM from
/// `crate::guide::primer_markdown()` -- one source of truth, drift-gated.
fn using_curie_skill() -> String {
    format!(
        "---\nname: using-curie\ndescription: How to drive the Curie harness -- the parity ladder, tier decision logic, landmines, and recovery steps. Invoke when running curie commands, authoring or evaluating a bundle, or debugging a divergence between skill, local, and cluster tiers.\n---\n\n{}",
        crate::guide::primer_markdown()
    )
}

const MCP_JSON: &str = "{\n  \"mcpServers\": {}\n}\n";

/// Declared connectors (ADR-0086). Scaffolded COMMENTED OUT so the bundle
/// validates as-is: an empty `connectors:` map is valid, and a half-filled one
/// is not. The point of shipping it at all is discoverability -- before this,
/// nothing in a fresh bundle mentioned the file, so an author had no way to
/// learn that Curie will run a connector for them.
const CONNECTORS_YAML: &str = r#"# What this agent needs running. Curie derives the Deployment, Service,
# container hardening, host allowlist, sandbox-to-connector NetworkPolicy,
# secret wiring, and the URL the agent dials -- so none of that lives here.
# The container must listen on port 8000.
# HTTP MCP is served at /mcp where applicable.
# Connector args are passed through verbatim.
#
# Uncomment and edit to declare one:
#
# connectors:
#   grafana:
#     image: grafana/mcp-grafana:0.17.2
#     args:
#       - -t
#       - streamable-http
#       # Curie names the Service, so Curie supplies every name the sandbox may
#       # dial. Hardcoding these breaks the moment a second agent runs this
#       # bundle -- the names are per-agent.
#       - -allowed-hosts
#       - ${CURIE_ALLOWED_HOSTS}
#     env:
#       GRAFANA_URL: https://grafana.example.com
#     # NAMES only. Values resolve at deploy time from your environment or
#     # `curie secrets set <NAME>`; they never belong in this file.
#     secrets:
#       - GRAFANA_SERVICE_ACCOUNT_TOKEN
#     secret_files:
#       GRAFANA_CLIENT_CERT: /etc/grafana/client.pem
#
#     # Where to reach it on a tier that cannot host it -- `skill` hosts
#     # nothing, so this is how local development reaches your own copy.
#     unhosted_url: ${GRAFANA_MCP_URL}
connectors: {}
"#;

/// Declared deploy targets (ADR-0089). Also commented out, and for a sharper
/// reason than connectors: a target names a Slack channel and an agent, and a
/// scaffolded placeholder that looked real would deploy somewhere nobody
/// intended. An empty `targets:` map declares no routing, so a git flow push
/// falls back to the single agent this repository binds (#1210); with
/// several agents bound the push is refused rather than guessed.
const DEPLOY_YAML: &str = r#"# Where this bundle gets deployed. `curie cluster deploy --target prod` reads
# this, so the routing is a reviewable diff in the repository instead of flags
# scattered across whatever invokes the command.
#
# The BUNDLE is identical for every target -- only the binding differs, which
# is what lets prod promote the exact artifact dev validated.
#
# Left empty, a push deploys to the single agent this repository binds.
# Declare targets once the repository binds more than one.
#
# Uncomment and edit. Use Slack channel IDs (starting with C), not #names:
#
# targets:
#   dev:
#     agent: my-agent-dev
#     env: dev
#     slack_channel: C0EXAMPLE1
#   prod:
#     agent: my-agent
#     env: prod
#     slack_channel: C0EXAMPLE2
targets: {}
"#;
const GITIGNORE: &str = ".curie/\n";

/// Create the bundle skeleton under `dir`; returns the created files.
pub fn scaffold(dir: &Path, name: &str) -> Result<Vec<PathBuf>> {
    if !valid_name(name) {
        bail!("plugin name {name:?} must be kebab-case (lowercase letters, digits, hyphens)");
    }

    let files: Vec<(PathBuf, String)> = vec![
        (dir.join(".claude-plugin/plugin.json"), manifest(name)),
        (dir.join(format!("skills/{name}/SKILL.md")), skill_md(name)),
        (dir.join(".mcp.json"), MCP_JSON.to_string()),
        (dir.join("connectors.yaml"), CONNECTORS_YAML.to_string()),
        (dir.join("deploy.yaml"), DEPLOY_YAML.to_string()),
        (dir.join("evals/cases.json"), eval_cases(name)),
        (dir.join(".gitignore"), GITIGNORE.to_string()),
        (dir.join("AGENTS.md"), agents_md(name)),
        (
            dir.join(".claude/skills/using-curie/SKILL.md"),
            using_curie_skill(),
        ),
    ];

    write_bundle(dir, files)
}

/// Refuse to create a generated demo directory over an existing nonempty tree.
///
/// `write_bundle` still refuses named scaffold targets so `init --adopt` can
/// write alongside surviving files. `curie try --keep` is not adopt: a second
/// stage or rerun must not recreate `./curie-demo` over a previous generated
/// bundle or a user's files (#2423). Missing and empty directories are owned
/// by this stage and may be filled.
pub(crate) fn refuse_unowned_nonempty_dir(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    if !dir.is_dir() {
        bail!("refusing to recreate {}: not a directory", dir.display());
    }
    let empty = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .next()
        .is_none();
    if !empty {
        bail!(
            "refusing to recreate {}: directory exists and is not empty",
            dir.display()
        );
    }
    Ok(())
}

/// Write a bundle's files with the shared collision-refusal discipline: refuse
/// if ANY target (or a stray root `plugin.json`) already exists, leaving every
/// existing file untouched and creating nothing on collision. Both the weather
/// scaffold and the spec scaffold funnel through here so the "never truncate a
/// file the user already has" guarantee is identical for both paths.
fn write_bundle(dir: &Path, files: Vec<(PathBuf, String)>) -> Result<Vec<PathBuf>> {
    let mut collisions: Vec<PathBuf> = files
        .iter()
        .map(|(path, _)| path.clone())
        .filter(|path| path.exists())
        .collect();
    if dir.join("plugin.json").exists() {
        collisions.push(dir.join("plugin.json"));
    }
    if !collisions.is_empty() {
        let listed: Vec<String> = collisions.iter().map(|p| p.display().to_string()).collect();
        bail!(
            "refusing to overwrite existing files: {}",
            listed.join(", ")
        );
    }

    let mut created = Vec::new();
    for (path, body) in files {
        let parent = path.parent().expect("scaffold paths have parents");
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
        std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
        created.push(path);
    }
    Ok(created)
}

/// Render one skill's `SKILL.md`: YAML frontmatter (name, description, and
/// `allowed-tools` only when non-empty) then the instructions body.
///
/// The frontmatter is rendered without a YAML crate. `name` is kebab-case
/// (validated in `spec.rs`), so a bare scalar is always safe. `description` and
/// the `allowed-tools` value are ARBITRARY agent-authored text, so they are
/// emitted as JSON-encoded strings: a serde_json string is a valid YAML
/// double-quoted scalar (YAML double-quoted supports the same `\n \t \" \\
/// \uXXXX` escapes serde_json emits), so the round-trip is exact. A bare scalar
/// here would corrupt or invalidate the frontmatter `plugin_format.validate_bundle`
/// parses with `yaml.safe_load` -- a colon-space breaks parsing, a leading ` #`
/// silently truncates as a comment, leading `"[{&*@` etc. error the scanner.
///
/// `allowed-tools` is emitted as ONE canonical space-separated scalar (ADR-0135)
/// rather than a block list, and the JSON encoding now wraps the WHOLE joined
/// value: quoting each entry individually made a space or a `#` inside an entry
/// safe by construction, and joining gives that up, so `spec::validate` refuses
/// any entry that carries a depth-0 separator or unbalanced parens before a byte
/// is written. That refusal is what keeps this join lossless.
fn render_skill_md(skill: &SkillSpec) -> String {
    let mut s = String::new();
    s.push_str("---\n");
    s.push_str(&format!("name: {}\n", skill.name));
    s.push_str(&format!(
        "description: {}\n",
        serde_json::to_string(&skill.description).expect("string serializes")
    ));
    // Omit the key entirely when empty: the wrong shape (an empty list) reads as
    // "no tools" but is noise; a real Claude Code bundle just leaves it out.
    if !skill.allowed_tools.is_empty() {
        s.push_str(&format!(
            "allowed-tools: {}\n",
            serde_json::to_string(&skill.allowed_tools.join(" ")).expect("string serializes")
        ));
    }
    s.push_str("---\n\n");
    // Normalize to a single trailing newline regardless of how the author ended
    // the body, so the file always ends with exactly one.
    s.push_str(skill.instructions.trim_end_matches('\n'));
    s.push('\n');
    s
}

/// Scaffold a bundle deterministically from an agent-authored spec. The spec is
/// already validated by `spec::parse`; this only lays down files, funneling
/// through `write_bundle` for the identical collision refusal as `scaffold`.
pub fn scaffold_from_spec(dir: &Path, spec: &AgentSpec) -> Result<Vec<PathBuf>> {
    // Build the manifest incrementally so `secrets`/`approvalPolicy` are OMITTED
    // when the spec declares none -- keeping the default scaffold byte-identical to
    // the trivial case while letting a spec produce a gated, authed bundle without
    // hand-editing plugin.json (#549).
    let mut manifest_obj = serde_json::Map::new();
    manifest_obj.insert("name".into(), serde_json::json!(spec.name));
    manifest_obj.insert("description".into(), serde_json::json!(spec.description));
    manifest_obj.insert(
        "systemPrompt".into(),
        serde_json::json!(format!(
            "You are the {} agent. Help users with {} requests.",
            spec.name, spec.name
        )),
    );
    manifest_obj.insert("version".into(), serde_json::json!("0.1.0"));
    if !spec.secrets.is_empty() {
        manifest_obj.insert("secrets".into(), serde_json::json!(spec.secrets));
    }
    if let Some(policy) = &spec.approval_policy {
        let gates: Vec<Value> = policy
            .gates
            .iter()
            .map(|g| {
                // Emit `grantableViaPolicy` only when true so specs that don't
                // use the opt-in keep a byte-identical default scaffold (#558).
                let mut gate = serde_json::Map::new();
                gate.insert("gate".into(), serde_json::json!(g.gate));
                gate.insert("route".into(), serde_json::json!(g.route));
                if g.grantable_via_policy {
                    gate.insert("grantableViaPolicy".into(), serde_json::json!(true));
                }
                if let Some(summary) = &g.summary {
                    gate.insert("summary".into(), serde_json::json!(summary));
                }
                Value::Object(gate)
            })
            .collect();
        manifest_obj.insert(
            "approvalPolicy".into(),
            serde_json::json!({ "gates": gates }),
        );
    }
    let manifest = serde_json::to_string_pretty(&Value::Object(manifest_obj))
        .expect("spec manifest serializes");

    // `.mcp.json` carries the connectors verbatim under `mcpServers`; an empty
    // map yields `{"mcpServers": {}}`, matching the weather scaffold's seed.
    let mut mcp_root = serde_json::Map::new();
    mcp_root.insert(
        "mcpServers".to_string(),
        Value::Object(spec.connectors.clone()),
    );
    let mcp = serde_json::to_string_pretty(&Value::Object(mcp_root)).expect("mcp serializes");
    let mcp = format!("{mcp}\n");

    // Assemble the suite and emit it, reusing the frozen `EvalSuite` Serialize so
    // the written `evals/cases.json` loads unchanged through `load_suite`.
    let suite = EvalSuite {
        name: spec.name.clone(),
        cases: spec.evals.clone(),
    };
    let cases = serde_json::to_string_pretty(&suite).expect("suite serializes");

    let mut files: Vec<(PathBuf, String)> =
        vec![(dir.join(".claude-plugin/plugin.json"), manifest)];
    for skill in &spec.skills {
        files.push((
            dir.join(format!("skills/{}/SKILL.md", skill.name)),
            render_skill_md(skill),
        ));
    }
    files.push((dir.join(".mcp.json"), mcp));
    files.push((dir.join("evals/cases.json"), cases));
    files.push((dir.join(".gitignore"), GITIGNORE.to_string()));

    write_bundle(dir, files)
}

/// Locate and parse a bundle's manifest JSON, trying
/// `.claude-plugin/plugin.json` then `plugin.json`. Shared by `read_manifest`
/// and `read_declared_secrets` so the path-resolution/read/parse steps (and
/// their error messages) live in one place. Returns the resolved manifest path
/// alongside the parsed value so callers can keep referencing it (e.g. `name`)
/// in their own error messages.
pub(crate) fn load_manifest_json(dir: &Path) -> Result<(std::path::PathBuf, serde_json::Value)> {
    let path = [".claude-plugin/plugin.json", "plugin.json"]
        .iter()
        .map(|rel| dir.join(rel))
        .find(|p| p.is_file())
        .ok_or_else(|| anyhow::anyhow!("{}", no_manifest_message(dir)))?;
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("{} is not valid JSON", path.display()))?;
    Ok((path, value))
}

/// A directory that looks like a pre-plugin `agent-ss-template` bundle: a
/// Python-package agent (`src/` plus a build/run file) rather than a plugin
/// (#745). Used only to make the "no plugin manifest" error point somewhere.
fn looks_like_pre_plugin_bundle(dir: &Path) -> bool {
    dir.join("src").is_dir()
        && ["Makefile", "Dockerfile", "compose.yaml", "server.py"]
            .iter()
            .any(|f| dir.join(f).exists())
}

/// The "no plugin manifest" error, shape-aware (#745): a pre-plugin bundle is
/// pointed at `init --adopt` and the porting guide; anything else gets the
/// generic scaffold hint. Shared by every manifest-read call site so the dead
/// end always names the path forward.
pub(crate) fn no_manifest_message(dir: &Path) -> String {
    let d = dir.display();
    if looks_like_pre_plugin_bundle(dir) {
        format!(
            "no plugin manifest under {d} -- this looks like a pre-plugin \
             (agent-ss-template) bundle. Run `curie init --adopt {d}` to scaffold \
             the plugin skeleton alongside your code, then port its logic into \
             skills/ and .mcp.json (see docs/adopting-a-bundle.md)."
        )
    } else {
        format!(
            "no plugin manifest under {d}: expected .claude-plugin/plugin.json or \
             plugin.json. Run `curie init <name>` to scaffold a new bundle, or \
             `curie init --adopt {d}` to adopt an existing directory."
        )
    }
}

/// Read the plugin name and version from a bundle's manifest.
pub fn read_manifest(dir: &Path) -> Result<(String, String)> {
    let (path, value) = load_manifest_json(dir)?;
    let name = value
        .get("name")
        .and_then(|v| v.as_str())
        .with_context(|| format!("{} has no string 'name'", path.display()))?
        .to_string();
    let version = value
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("0.0.0")
        .to_string();
    Ok((name, version))
}

/// Read the bundle's declared connector-secret NAMES (#464 / ADR-0009).
/// The manifest `secrets` policy lists NAMES only (never values). Returns an
/// empty vec when the field is absent or null. Uses the same manifest-path
/// resolution as `read_manifest`.
pub fn read_declared_secrets(dir: &Path) -> Result<Vec<String>> {
    let (_path, value) = load_manifest_json(dir)?;
    // Missing/null -> no declared secrets. Non-string entries are ignored
    // defensively; the plugin-format validator already rejects malformed names.
    let names = value
        .get("secrets")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_plugin_name_kebabizes_a_directory_basename() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("Revenue_Leak Agent!");
        std::fs::create_dir(&sub).unwrap();
        assert_eq!(
            derive_plugin_name(&sub).as_deref(),
            Some("revenue-leak-agent")
        );
    }

    #[test]
    fn derive_plugin_name_is_none_when_nothing_usable_remains() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("!!!");
        std::fs::create_dir(&sub).unwrap();
        assert_eq!(derive_plugin_name(&sub), None);
    }

    #[test]
    fn no_manifest_message_points_a_pre_plugin_bundle_at_adopt() {
        // src/ + a build/run file => looks like an agent-ss-template bundle.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("server.py"), "# flask\n").unwrap();
        let msg = no_manifest_message(dir.path());
        assert!(msg.contains("agent-ss-template"), "{msg}");
        assert!(msg.contains("init --adopt"), "{msg}");
        assert!(msg.contains("docs/adopting-a-bundle.md"), "{msg}");
    }

    #[test]
    fn no_manifest_message_is_generic_for_a_plain_directory() {
        let dir = tempfile::tempdir().unwrap();
        let msg = no_manifest_message(dir.path());
        assert!(msg.contains(".claude-plugin/plugin.json"), "{msg}");
        assert!(!msg.contains("agent-ss-template"), "{msg}");
        // still offers adopt as an option, just not as the lead.
        assert!(msg.contains("init --adopt"), "{msg}");
    }

    #[test]
    fn accepts_kebab_case_and_rejects_everything_else() {
        for good in ["deal-desk", "a", "x9", "a-b-c1"] {
            assert!(valid_name(good), "{good} should be valid");
        }
        for bad in ["", "Deal-Desk", "deal_desk", "-x", "x-", "a--b", "a b"] {
            assert!(!valid_name(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn scaffolds_the_frozen_bundle_shape() {
        let dir = tempfile::tempdir().unwrap();
        let created = scaffold(dir.path(), "deal-desk").unwrap();
        assert_eq!(created.len(), 9);

        let manifest: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join(".claude-plugin/plugin.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["name"], "deal-desk");
        assert_eq!(
            manifest["systemPrompt"],
            "You are the deal-desk agent. Help users with deal-desk requests."
        );

        // Genericized starter skill for <name>: no weather anywhere.
        let skill = std::fs::read_to_string(dir.path().join("skills/deal-desk/SKILL.md")).unwrap();
        assert!(skill.starts_with("---\nname: deal-desk\n"));
        let description_line = skill
            .lines()
            .find(|l| l.starts_with("description:"))
            .expect("skill has a description line");
        assert!(
            description_line.contains("deal-desk"),
            "description mentions the name: {description_line}"
        );
        // The canonical single-line scalar (ADR-0135), asserted exactly: a bare
        // `contains("allowed-tools:")` passes on the block form too, so the old
        // shape has to be asserted ABSENT or a regression to it goes unnoticed.
        assert!(
            skill.contains("allowed-tools: WebSearch WebFetch\n"),
            "starter skill must teach the canonical scalar: {skill}"
        );
        assert!(
            !skill.contains("  - "),
            "the block list form must be gone entirely: {skill}"
        );
        assert!(
            !skill.contains("weather") && !skill.contains("Weather"),
            "starter skill must be genericized, not weather"
        );

        // connectors.yaml and deploy.yaml ship COMMENTED: a fresh bundle must
        // validate as-is, and a half-filled connector or a placeholder Slack
        // channel would not. They exist for discoverability -- before this,
        // nothing in a scaffolded bundle mentioned either file.
        for (name, key) in [
            ("connectors.yaml", "connectors:"),
            ("deploy.yaml", "targets:"),
        ] {
            let body = std::fs::read_to_string(dir.path().join(name)).unwrap();
            assert!(
                body.contains(&format!("{key} {{}}")),
                "{name} must scaffold empty so the bundle validates: {body}"
            );
            assert!(
                body.lines().filter(|l| l.starts_with('#')).count() > 5,
                "{name} must explain itself; it is the only place an author learns it exists"
            );
        }

        let connectors = std::fs::read_to_string(dir.path().join("connectors.yaml")).unwrap();
        assert!(
            connectors.contains("The container must listen on port 8000."),
            "connectors.yaml must name Curie's fixed connector listen port: {connectors}"
        );
        assert!(
            connectors.contains("HTTP MCP is served at /mcp where applicable."),
            "connectors.yaml must name the HTTP MCP path when the connector serves it: {connectors}"
        );
        assert!(
            connectors.contains("Connector args are passed through verbatim."),
            "connectors.yaml must warn authors that args control the connector itself: {connectors}"
        );
        assert!(
            connectors.ends_with("connectors: {}\n"),
            "connectors.yaml must remain an empty valid map until an author opts in: {connectors}"
        );
        assert!(
            connectors.lines().all(|line| {
                let trimmed = line.trim_start();
                trimmed.starts_with('#') || !trimmed.starts_with("port:")
            }),
            "connectors.yaml must not advertise an active port: key: {connectors}"
        );

        let mcp: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join(".mcp.json")).unwrap())
                .unwrap();
        assert!(mcp["mcpServers"].is_object());

        // Single smoke case named for <name>, graded by a falsifiable
        // `contains: <name>` (#527) -- never the vacuous `contains ""`.
        let cases: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("evals/cases.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(cases["name"], "deal-desk");
        let case_list = cases["cases"].as_array().unwrap();
        assert_eq!(case_list.len(), 1);
        assert_eq!(case_list[0]["id"], "deal-desk-smoke");
        assert_eq!(case_list[0]["grader"]["kind"], "contains");
        // The seed asserts the agent named itself, not `contains: ""` (#527):
        // an empty/errored turn fails, so the starter case is not vacuously green.
        assert_eq!(case_list[0]["grader"]["expected"], "deal-desk");
        assert_eq!(case_list[0]["grader"]["case_sensitive"], false);

        // AGENTS.md teaches the developer's coding agent the harness.
        let agents = std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap();
        assert!(agents.contains("curie guide"));
        assert!(agents.contains("curie skill eval"));
        // AGENTS.md defers the landmine list to `curie guide` rather than
        // naming any specific landmine: no drift gate covers this hand-written
        // prose, so a copy here goes stale silently. The `allowed-tools`
        // teaching lives in the correct-by-example scaffolded SKILL.md above.
        assert!(!agents.contains("allowed-tools"));
        // The approval pointer is pinned, unlike the landmine prose above, and
        // the difference is which way each can rot. The landmine list is
        // deliberately NOT copied here, so there is nothing to go stale. The
        // approval pointer IS content, and it is the one #1051 propagation path
        // with no gate: SKILL.md is byte-pinned against `guide::primer_markdown`
        // and the primer has its own drift test, while this file is hand-written
        // prose that satisfied a done-when item and could quietly lose it (#1086).
        //
        // Three facts, all non-derivable from the command tree: the conditional
        // availability of the tool that raises a request, the required honest
        // fallback when it is absent, and the prefix the resume turn arrives
        // with. Missing the final one silently drops the verdict.
        assert!(
            agents.contains("request_approval"),
            "AGENTS.md must name the conditionally available approval tool"
        );
        assert!(
            agents.contains("bundle cannot perform the\n  action"),
            "AGENTS.md must tell an agent not to invent an unavailable remediation"
        );
        assert!(
            agents.contains("[approval resolved]"),
            "AGENTS.md must name the resume-turn prefix a skill has to handle"
        );

        // The installable harness primer skill, discovered by Claude Code.
        let harness_skill =
            std::fs::read_to_string(dir.path().join(".claude/skills/using-curie/SKILL.md"))
                .unwrap();
        assert!(harness_skill.starts_with("---\nname: using-curie\n"));
        let harness_description = harness_skill
            .lines()
            .find(|l| l.starts_with("description:"))
            .expect("harness skill has a description line");
        assert!(
            !harness_description
                .trim_start_matches("description:")
                .trim()
                .is_empty(),
            "harness skill description is non-empty"
        );
        assert!(harness_skill.contains("# Curie harness primer"));
        assert!(harness_skill.contains("curie schema"));

        assert_eq!(
            read_manifest(dir.path()).unwrap(),
            ("deal-desk".to_string(), "0.1.0".to_string())
        );
    }

    #[test]
    fn harness_skill_body_equals_the_guide() {
        // Anti-drift (D2): the scaffolded harness skill body is rendered from
        // `guide::primer_markdown()`, never re-authored, so the guide and the
        // scaffolded skill can never diverge. The body after the frontmatter's
        // closing `---\n` (plus its blank line) must equal the guide verbatim.
        let dir = tempfile::tempdir().unwrap();
        scaffold(dir.path(), "deal-desk").unwrap();
        let content =
            std::fs::read_to_string(dir.path().join(".claude/skills/using-curie/SKILL.md"))
                .unwrap();
        let body = content
            .splitn(3, "---\n")
            .nth(2)
            .expect("frontmatter-delimited body")
            .strip_prefix('\n')
            .expect("blank line after the closing frontmatter fence");
        assert_eq!(body, crate::guide::primer_markdown());
    }

    #[test]
    fn eval_cases_byte_match_the_committed_fixture() {
        // The frozen cross-language fixture: `eval_cases("example")` must be
        // byte-for-byte the worker's committed example, so a scaffolded bundle
        // is loadable by the platform eval path.
        assert_eq!(
            eval_cases("example"),
            include_str!("../../apps/worker/schema/eval-cases.example.json")
        );
    }

    #[test]
    fn refuses_to_overwrite_an_existing_bundle() {
        let dir = tempfile::tempdir().unwrap();
        scaffold(dir.path(), "deal-desk").unwrap();
        assert!(scaffold(dir.path(), "deal-desk").is_err());
    }

    #[test]
    fn refuse_unowned_nonempty_dir_allows_missing_and_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("curie-demo");
        refuse_unowned_nonempty_dir(&missing).unwrap();
        std::fs::create_dir(&missing).unwrap();
        refuse_unowned_nonempty_dir(&missing).unwrap();
    }

    #[test]
    fn refuse_unowned_nonempty_dir_rejects_user_files_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let demo = dir.path().join("curie-demo");
        std::fs::create_dir(&demo).unwrap();
        let notes = demo.join("notes.txt");
        std::fs::write(&notes, "user notes\n").unwrap();
        let err = refuse_unowned_nonempty_dir(&demo).unwrap_err().to_string();
        assert!(err.contains("not empty"), "{err}");
        assert_eq!(std::fs::read_to_string(&notes).unwrap(), "user notes\n");
        assert!(!demo.join(".claude-plugin/plugin.json").exists());
    }

    #[test]
    fn refuses_to_truncate_any_existing_target_even_without_a_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let existing = r#"{"mcpServers":{"important":{"command":"x"}}}"#;
        std::fs::write(dir.path().join(".mcp.json"), existing).unwrap();

        let err = scaffold(dir.path(), "deal-desk").unwrap_err();
        assert!(err.to_string().contains(".mcp.json"), "{err}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".mcp.json")).unwrap(),
            existing,
            "existing file must be untouched"
        );
        assert!(!dir.path().join(".claude-plugin/plugin.json").exists());
    }

    #[test]
    fn refuses_to_overwrite_an_existing_agents_md() {
        let dir = tempfile::tempdir().unwrap();
        let existing = "# My own agent notes\nDo not clobber me.\n";
        std::fs::write(dir.path().join("AGENTS.md"), existing).unwrap();

        let err = scaffold(dir.path(), "deal-desk").unwrap_err();
        assert!(err.to_string().contains("AGENTS.md"), "{err}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("AGENTS.md")).unwrap(),
            existing,
            "existing file must be untouched"
        );
        assert!(!dir.path().join(".claude-plugin/plugin.json").exists());
    }

    #[test]
    fn rejects_a_bad_name_before_touching_disk() {
        let dir = tempfile::tempdir().unwrap();
        assert!(scaffold(dir.path(), "Bad_Name").is_err());
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn read_declared_secrets_absent_field_is_empty_and_present_array_is_the_names() {
        // Absent `secrets` field -> empty (the scaffold seeds no secrets policy).
        let dir = tempfile::tempdir().unwrap();
        scaffold(dir.path(), "deal-desk").unwrap();
        assert!(read_declared_secrets(dir.path()).unwrap().is_empty());

        // Present array -> the declared NAMES, in order.
        let manifest_path = dir.path().join(".claude-plugin/plugin.json");
        let mut manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest["secrets"] =
            serde_json::json!(["GITHUB_PERSONAL_ACCESS_TOKEN", "SLACK_APP_TOKEN"]);
        std::fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();
        assert_eq!(
            read_declared_secrets(dir.path()).unwrap(),
            vec![
                "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
                "SLACK_APP_TOKEN".to_string(),
            ]
        );
    }
}
