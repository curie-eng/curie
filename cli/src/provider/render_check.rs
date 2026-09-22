//! Render coverage check for the credential inventory (ADR 0163 decision 4).
//!
//! Renders the chart with `helm template` over a fixed list of values sets,
//! extracts every Secret reference the manifests make, and reports each one no
//! inventory entry lists. It only ever spawns `helm`: no cluster, no provider
//! CLI, no External Secrets Operator, so it runs in CI and on a laptop alike.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use super::catalog::{bundle_entries, merge, parse_inventory, platform_inventory};
use super::inventory::{InventoryEntry, Store};

/// The release name every set renders with, so `{release}` is `inv` and the
/// chart's fullname is `inv-curie` unless a set overrides it.
pub const RELEASE: &str = "inv";
const NAMESPACE: &str = "curie";
const CHART_NAME: &str = "curie";

/// Overlays under the chart that exist only to widen what this check renders.
pub const OVERLAY_DIR: &str = "ci/inventory-values";

/// One Secret reference a rendered object makes. `key` is `None` when the
/// reference takes the whole Secret (envFrom, an unfiltered volume, TLS).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct RenderedRef {
    pub name: String,
    pub key: Option<String>,
    /// `<Kind>/<metadata.name>` of the object that makes the reference.
    pub object: String,
}

/// What `{release}` and `{fullname}` expand to for one values set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameContext {
    pub release: String,
    pub fullname: String,
}

/// Inputs to [`run_check`].
#[derive(Debug, Clone)]
pub struct CheckOptions {
    pub chart: PathBuf,
    /// An inventory file to check instead of the embedded one.
    pub inventory: Option<PathBuf>,
    /// `(agent, bundle dir)` pairs whose connectors extend the inventory.
    pub bundles: Vec<(String, PathBuf)>,
    /// Relative bundle paths resolve from here.
    pub repo_root: PathBuf,
}

/// One values set's outcome.
#[derive(Debug, Clone, Serialize)]
pub struct SetReport {
    pub name: String,
    pub refs_checked: usize,
    pub uncovered: Vec<RenderedRef>,
    /// Set when the chart failed to render; a set that cannot render fails.
    pub error: Option<String>,
}

impl SetReport {
    pub fn passed(&self) -> bool {
        self.error.is_none() && self.uncovered.is_empty()
    }
}

/// The whole run.
#[derive(Debug, Clone, Serialize)]
pub struct CheckReport {
    pub entries: usize,
    pub sets: Vec<SetReport>,
    /// Entries no set rendered a reference for. A note, not a failure: rows
    /// the chart never renders (hosted connector Secrets, runtime Secrets) are
    /// listed for completeness.
    pub unmatched_entries: Vec<String>,
}

impl CheckReport {
    pub fn passed(&self) -> bool {
        self.sets.iter().all(SetReport::passed)
    }
}

// --------------------------------------------------------------------------
// Extraction
// --------------------------------------------------------------------------

/// Every Secret reference in a multi-document manifest stream.
///
/// The walk is generic below the object so pod specs nested in a Job,
/// CronJob, StatefulSet or a SandboxTemplate are seen without naming each kind.
pub fn extract_refs(manifest: &str) -> Result<Vec<RenderedRef>> {
    let mut refs = Vec::new();
    for document in serde_norway::Deserializer::from_str(manifest) {
        let doc = Value::deserialize(document).context("parse rendered manifest")?;
        if !doc.is_object() {
            continue;
        }
        let kind = doc["kind"].as_str().unwrap_or("Unknown");
        let name = doc["metadata"]["name"].as_str().unwrap_or("");
        let object = format!("{kind}/{name}");
        if kind == "Secret" {
            for field in ["data", "stringData"] {
                if let Some(data) = doc[field].as_object() {
                    for key in data.keys() {
                        refs.push(RenderedRef {
                            name: name.to_string(),
                            key: Some(key.clone()),
                            object: object.clone(),
                        });
                    }
                }
            }
            continue;
        }
        if kind == "ServiceAccount" {
            for item in doc["secrets"].as_array().into_iter().flatten() {
                if let Some(secret) = item["name"].as_str() {
                    refs.push(whole(secret, &object));
                }
            }
        }
        if kind == "Ingress" {
            if let Some(tls) = doc["spec"]["tls"].as_array() {
                for item in tls {
                    if let Some(secret) = item["secretName"].as_str() {
                        refs.push(whole(secret, &object));
                    }
                }
            }
        }
        walk(&doc, &object, &mut refs);
    }
    Ok(refs)
}

fn whole(name: &str, object: &str) -> RenderedRef {
    RenderedRef {
        name: name.to_string(),
        key: None,
        object: object.to_string(),
    }
}

fn keyed(name: &str, key: &str, object: &str) -> RenderedRef {
    RenderedRef {
        name: name.to_string(),
        key: Some(key.to_string()),
        object: object.to_string(),
    }
}

/// A `secret` volume source or projected source: named by `secretName` or
/// `name`, narrowed to `items[].key` when items are listed.
fn secret_source(source: &Value, object: &str, refs: &mut Vec<RenderedRef>) {
    let Some(name) = source["secretName"]
        .as_str()
        .or_else(|| source["name"].as_str())
    else {
        return;
    };
    match source["items"].as_array() {
        Some(items) if !items.is_empty() => {
            for item in items {
                if let Some(key) = item["key"].as_str() {
                    refs.push(keyed(name, key, object));
                }
            }
        }
        _ => refs.push(whole(name, object)),
    }
}

fn walk(value: &Value, object: &str, refs: &mut Vec<RenderedRef>) {
    match value {
        Value::Object(map) => {
            for (field, child) in map {
                match field.as_str() {
                    "secretKeyRef" => {
                        if let (Some(name), Some(key)) =
                            (child["name"].as_str(), child["key"].as_str())
                        {
                            refs.push(keyed(name, key, object));
                        }
                    }
                    "envFrom" => {
                        for item in child.as_array().into_iter().flatten() {
                            if let Some(name) = item["secretRef"]["name"].as_str() {
                                refs.push(whole(name, object));
                            }
                        }
                    }
                    "imagePullSecrets" => {
                        for item in child.as_array().into_iter().flatten() {
                            if let Some(name) = item["name"].as_str() {
                                refs.push(keyed(name, ".dockerconfigjson", object));
                            }
                        }
                    }
                    // Every CSI Secret reference shape, on a Pod volume or a
                    // PersistentVolume, names a whole Secret.
                    "nodePublishSecretRef"
                    | "nodeStageSecretRef"
                    | "nodeExpandSecretRef"
                    | "controllerPublishSecretRef"
                    | "controllerExpandSecretRef" => {
                        if let Some(name) = child["name"].as_str() {
                            refs.push(whole(name, object));
                        }
                    }
                    "secret" if child.is_object() => secret_source(child, object, refs),
                    _ => walk(child, object, refs),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(item, object, refs);
            }
        }
        _ => {}
    }
}

// --------------------------------------------------------------------------
// Coverage
// --------------------------------------------------------------------------

/// Expand a name pattern into an anchored regex.
fn pattern_regex(pattern: &str, ctx: &NameContext) -> Regex {
    let mut out = String::from("^");
    let mut rest = pattern;
    while let Some(open) = rest.find('{') {
        out.push_str(&regex::escape(&rest[..open]));
        let after = &rest[open + 1..];
        let close = after.find('}').unwrap_or(after.len());
        out.push_str(&match &after[..close] {
            "release" => regex::escape(&ctx.release),
            "fullname" => regex::escape(&ctx.fullname),
            "agent" => "[a-z0-9]([a-z0-9-]*[a-z0-9])?".to_string(),
            "id" => "[a-f0-9]+".to_string(),
            // Validation refuses any other placeholder, so this never widens
            // a pattern; an unknown one matches nothing.
            _ => "[^\\s\\S]".to_string(),
        });
        rest = after.get(close + 1..).unwrap_or("");
    }
    out.push_str(&regex::escape(rest));
    out.push('$');
    Regex::new(&out).expect("an escaped pattern is a valid regex")
}

/// Expand a pattern with no `{agent}` or `{id}` into one name.
fn expand(pattern: &str, ctx: &NameContext) -> Option<String> {
    if pattern.contains("{agent}") || pattern.contains("{id}") {
        return None;
    }
    Some(
        pattern
            .replace("{release}", &ctx.release)
            .replace("{fullname}", &ctx.fullname),
    )
}

/// The value at a dotted values path.
fn node<'a>(values: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(values, |node, segment| node.get(segment))
}

/// The non-empty string at a dotted values path.
fn lookup<'a>(values: &'a Value, path: &str) -> Option<&'a str> {
    node(values, path)
        .and_then(Value::as_str)
        .filter(|found| !found.is_empty())
}

/// Every Secret name a knob's values path sets: one string, or each entry of
/// a list of `{name: X}` maps or strings.
fn knob_names<'a>(values: &'a Value, path: &str) -> Vec<&'a str> {
    match node(values, path) {
        Some(Value::String(name)) if !name.is_empty() => vec![name.as_str()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().or_else(|| item["name"].as_str()))
            .filter(|name| !name.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether `entry` lists `reference`.
///
/// A name matched through the entry's own patterns needs its key among the
/// entry's keys. A name matched through a knob with a key path needs the key
/// that path sets, and only that key: the Secret an operator supplies need not
/// carry the inventory's own key name. A whole-Secret reference is covered by
/// whichever match names it exactly.
fn covers(
    entry: &InventoryEntry,
    reference: &RenderedRef,
    values: &Value,
    ctx: &NameContext,
) -> bool {
    let listed = |key: &String| entry.keys.contains(key);
    let by_pattern = pattern_regex(&entry.target, ctx).is_match(&reference.name)
        || entry
            .chart
            .as_ref()
            .and_then(|chart| chart.default_secret.as_deref())
            .is_some_and(|pattern| pattern_regex(pattern, ctx).is_match(&reference.name));
    let knobs: Vec<_> = entry
        .chart
        .iter()
        .flat_map(|chart| chart.knobs.iter())
        .filter(|knob| knob_names(values, &knob.secret).contains(&reference.name.as_str()))
        .collect();
    // A knob that names this Secret decides alone. Otherwise a BYO name that
    // happens to equal the target pattern would accept the inventory key even
    // though the key knob says the supplied Secret carries another one.
    if knobs.is_empty() {
        return by_pattern && reference.key.as_ref().is_none_or(listed);
    }
    knobs.into_iter().any(|knob| {
        let Some(key) = &reference.key else {
            return true;
        };
        match knob.key.as_deref().and_then(|path| lookup(values, path)) {
            Some(effective) => key == effective,
            // No key knob, or it is unset: the chart reads its own key.
            None => listed(key),
        }
    })
}

/// References no entry lists, given the effective values of the set that
/// rendered them.
pub fn uncovered(
    refs: &[RenderedRef],
    entries: &[InventoryEntry],
    values: &Value,
    ctx: &NameContext,
) -> Vec<RenderedRef> {
    refs.iter()
        .filter(|reference| !entries.iter().any(|e| covers(e, reference, values, ctx)))
        .cloned()
        .collect()
}

// --------------------------------------------------------------------------
// Values sets
// --------------------------------------------------------------------------

struct ValuesSet {
    name: String,
    files: Vec<PathBuf>,
}

fn values_sets(chart: &Path) -> Result<Vec<ValuesSet>> {
    let set = |name: &str, files: &[&str]| ValuesSet {
        name: name.to_string(),
        files: files.iter().map(|file| chart.join(file)).collect(),
    };
    let mut sets = vec![
        set("defaults", &[]),
        set("dev", &["values-dev.yaml"]),
        set(
            "dev+nogvisor",
            &["values-dev.yaml", "values-e2e-nogvisor.yaml"],
        ),
        set("external", &["values-external.yaml"]),
        set(
            "nogvisor+e2e-harness",
            &["values-e2e-nogvisor.yaml", "values-e2e-harness.yaml"],
        ),
        // The consumer overlay composes with nogvisor, per its own header.
        set(
            "two-release-consumer",
            &[
                "values-e2e-nogvisor.yaml",
                "values-e2e-two-release-consumer.yaml",
            ],
        ),
    ];
    let dir = chart.join(OVERLAY_DIR);
    let mut overlays: Vec<PathBuf> = std::fs::read_dir(&dir)
        .with_context(|| format!("read {}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "yaml"))
        .collect();
    overlays.sort();
    for path in overlays {
        let stem = path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        sets.push(ValuesSet {
            name: format!("overlay:{stem}"),
            files: vec![path],
        });
    }
    Ok(sets)
}

/// The values an ESO-backed install would set: every `store: sm` entry's
/// knobs pointed at its expanded target, and a knob's key path at the key
/// when the entry has exactly one.
///
/// A knob whose path holds a list in `base` (an `imagePullSecrets` list) is
/// set to a one-item list naming the target.
pub fn provider_values(entries: &[InventoryEntry], ctx: &NameContext, base: &Value) -> Value {
    let mut values = json!({});
    for entry in entries.iter().filter(|e| e.store == Store::Sm) {
        let Some(chart) = &entry.chart else { continue };
        let Some(target) = expand(&entry.target, ctx) else {
            continue;
        };
        for knob in &chart.knobs {
            let name = if node(base, &knob.secret).is_some_and(Value::is_array) {
                json!([{ "name": target }])
            } else {
                Value::String(target.clone())
            };
            set_path(&mut values, &knob.secret, name);
            if let (Some(path), [key]) = (&knob.key, entry.keys.as_slice()) {
                set_path(&mut values, path, Value::String(key.clone()));
            }
        }
    }
    values
}

fn set_path(values: &mut Value, path: &str, leaf: Value) {
    let mut node = values;
    let segments: Vec<&str> = path.split('.').collect();
    for segment in &segments[..segments.len() - 1] {
        if !node[*segment].is_object() {
            node[*segment] = Value::Object(Map::new());
        }
        node = &mut node[*segment];
    }
    node[segments[segments.len() - 1]] = leaf;
}

fn deep_merge(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        deep_merge(existing, value)
                    }
                    // Helm drops a key an overlay sets to null.
                    _ if value.is_null() => {
                        base.remove(key);
                    }
                    _ => {
                        base.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base, overlay) => *base = overlay.clone(),
    }
}

fn read_yaml(path: &Path) -> Result<Value> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let value: Value =
        serde_norway::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    Ok(if value.is_null() { json!({}) } else { value })
}

/// Chart defaults deep-merged with each file in order, as Helm does.
fn effective_values(chart: &Path, files: &[PathBuf]) -> Result<Value> {
    let mut values = read_yaml(&chart.join("values.yaml"))?;
    for file in files {
        deep_merge(&mut values, &read_yaml(file)?);
    }
    Ok(values)
}

/// `curie.fullname` from `_helpers.tpl`, evaluated over effective values.
fn name_context(values: &Value) -> NameContext {
    let trim = |name: String| {
        name.chars()
            .take(63)
            .collect::<String>()
            .trim_end_matches('-')
            .to_string()
    };
    let fullname = if let Some(full) = lookup(values, "fullnameOverride") {
        trim(full.to_string())
    } else {
        let name = lookup(values, "nameOverride").unwrap_or(CHART_NAME);
        if RELEASE.contains(name) {
            trim(RELEASE.to_string())
        } else {
            trim(format!("{RELEASE}-{name}"))
        }
    };
    NameContext {
        release: RELEASE.to_string(),
        fullname,
    }
}

fn helm_template(chart: &Path, files: &[PathBuf]) -> Result<String> {
    let mut command = Command::new("helm");
    command
        .args(["template", RELEASE])
        .arg(chart)
        .args(["--namespace", NAMESPACE]);
    for file in files {
        command.arg("-f").arg(file);
    }
    let output = command
        .output()
        .context("run helm template (is helm on PATH?)")?;
    if !output.status.success() {
        bail!(
            "helm template failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).context("helm template output is not UTF-8")
}

// --------------------------------------------------------------------------
// The check
// --------------------------------------------------------------------------

/// Load the inventory, merge bundle entries, render every values set and
/// report what no entry lists. An invalid inventory is an error before any
/// render, which is where a rotation-owned key marked ESO-managed fails.
pub fn run_check(options: &CheckOptions) -> Result<CheckReport> {
    let platform = match &options.inventory {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("read {}", path.display()))?;
            parse_inventory(&text)
                .map_err(|error| anyhow!("inventory {}: {error:#}", path.display()))?
        }
        None => platform_inventory().map_err(|error| anyhow!("{error:#}"))?,
    };
    let mut bundles = Vec::new();
    for (agent, dir) in &options.bundles {
        let dir = if dir.is_absolute() {
            dir.clone()
        } else {
            options.repo_root.join(dir)
        };
        let decl = crate::connector_build::load(&dir)
            .map_err(|error| anyhow!("bundle {agent} at {}: {error:#}", dir.display()))?;
        bundles.extend(bundle_entries(&decl, agent)?);
    }
    // The provider set redirects platform knobs only. A per-agent BYO knob
    // also needs its key list and excludes the agent's connectorSecrets, which
    // is deploy's routing to make; byo.yaml renders that knob instead.
    let platform_for_provider = platform.clone();
    let entries = merge(platform, bundles).map_err(|error| anyhow!("{error:#}"))?;

    let mut sets = values_sets(&options.chart)?;
    let scratch = tempfile::tempdir().context("create a scratch directory")?;
    // The provider set layers on the widest overlay, so every knob it points
    // at an ESO target has a consumer that renders the reference.
    let mut provider_files: Vec<PathBuf> = sets
        .iter()
        .find(|set| set.name == "overlay:full")
        .map(|set| set.files.clone())
        .unwrap_or_default();
    let base_values = effective_values(&options.chart, &provider_files)?;
    let base = name_context(&base_values);
    let provider_path = scratch.path().join("provider-values.yaml");
    std::fs::write(
        &provider_path,
        serde_norway::to_string(&provider_values(
            &platform_for_provider,
            &base,
            &base_values,
        ))?,
    )
    .context("write provider values")?;
    provider_files.push(provider_path);
    sets.push(ValuesSet {
        name: "provider".into(),
        files: provider_files,
    });

    let mut matched = BTreeSet::new();
    let mut reports = Vec::new();
    for set in &sets {
        let values = effective_values(&options.chart, &set.files)?;
        let ctx = name_context(&values);
        let rendered = match helm_template(&options.chart, &set.files) {
            Ok(rendered) => rendered,
            Err(error) => {
                reports.push(SetReport {
                    name: set.name.clone(),
                    refs_checked: 0,
                    uncovered: Vec::new(),
                    error: Some(format!("{error:#}")),
                });
                continue;
            }
        };
        let refs: Vec<RenderedRef> = extract_refs(&rendered)
            .with_context(|| format!("values set {}", set.name))?
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for entry in &entries {
            if refs.iter().any(|r| covers(entry, r, &values, &ctx)) {
                matched.insert(entry.logical_name.clone());
            }
        }
        reports.push(SetReport {
            name: set.name.clone(),
            refs_checked: refs.len(),
            uncovered: uncovered(&refs, &entries, &values, &ctx),
            error: None,
        });
    }
    let unmatched_entries = entries
        .iter()
        .filter(|entry| !matched.contains(&entry.logical_name))
        .map(|entry| entry.logical_name.clone())
        .collect();
    Ok(CheckReport {
        entries: entries.len(),
        sets: reports,
        unmatched_entries,
    })
}
