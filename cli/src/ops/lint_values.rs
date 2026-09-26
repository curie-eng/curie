//! Compare Helm's recorded user values with an ordered set of pending files.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;

use super::{fetch_release_values, plain, run_capture, CommonOpts, OpsCommand};
use crate::ui::{CliOutput, Ui};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PathSegment {
    Key(String),
    Index(usize),
}

/// A successful read only check. Changed paths are advisory, so a nonempty
/// result still has a successful exit status.
pub struct LintValuesOutput {
    pub changed_paths: Vec<String>,
    pub release_exists: bool,
}

impl CliOutput for LintValuesOutput {
    fn to_json(&self) -> Value {
        serde_json::json!({
            "changed_paths": self.changed_paths,
            "changed_count": self.changed_paths.len(),
            "release_exists": self.release_exists,
        })
    }

    fn render(&self, ui: &Ui) {
        if !self.release_exists {
            ui.payload_plain("Release does not exist.");
        }
        if self.changed_paths.is_empty() {
            ui.payload_plain("No user value paths would change.");
        } else {
            ui.payload_plain(&format!(
                "{} user value path(s) would change:",
                self.changed_paths.len()
            ));
            for path in &self.changed_paths {
                ui.payload_plain(path);
            }
        }
    }
}

/// Ask Helm to parse each pending file. Reading the copies through `fromYaml`
/// keeps explicit nulls that Helm omits from its computed `.Values` template.
/// The original files also go through `-f` so Helm validates the upgrade input.
async fn pending_values(files: &[PathBuf]) -> Result<Value> {
    let chart = tempfile::tempdir()
        .map_err(|_| crate::exit::CliError::failure("could not prepare values lint chart"))?;
    let templates = chart.path().join("templates");
    std::fs::create_dir(&templates)
        .map_err(|_| crate::exit::CliError::failure("could not prepare values lint chart"))?;
    std::fs::write(
        chart.path().join("Chart.yaml"),
        "apiVersion: v2\nname: curie-values-lint\nversion: 0.0.1\n",
    )
    .map_err(|_| crate::exit::CliError::failure("could not prepare values lint chart"))?;
    let chart_files = chart.path().join("files");
    std::fs::create_dir(&chart_files)
        .map_err(|_| crate::exit::CliError::failure("could not prepare values lint chart"))?;
    let mut template = String::from(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: curie-values-lint\ndata:\n",
    );
    if files.is_empty() {
        template.push_str("  empty: \"{}\"\n");
    }
    for (index, file) in files.iter().enumerate() {
        let contents = std::fs::read(file)
            .map_err(|_| crate::exit::CliError::failure("could not read values file"))?;
        std::fs::write(chart_files.join(format!("{index}.yaml")), contents)
            .map_err(|_| crate::exit::CliError::failure("could not prepare values lint chart"))?;
        template.push_str(&format!(
            "  file{index}: {{{{ (.Files.Get \"files/{index}.yaml\" | fromYaml | toJson) | quote }}}}\n"
        ));
    }
    std::fs::write(templates.join("values.yaml"), template)
        .map_err(|_| crate::exit::CliError::failure("could not prepare values lint chart"))?;

    let mut args = vec![
        plain("template"),
        plain("curie-values-lint"),
        plain(chart.path().to_string_lossy().into_owned()),
    ];
    for file in files {
        let path = file
            .to_str()
            .ok_or_else(|| crate::exit::CliError::failure("values file path is not UTF-8"))?;
        args.push(plain("-f"));
        args.push(plain(path));
    }
    let (ok, output, _) = run_capture(&OpsCommand::new("helm", args))
        .await
        .map_err(|_| {
            crate::exit::CliError::failure("could not invoke Helm to parse values files")
        })?;
    if !ok {
        return Err(
            crate::exit::CliError::failure("could not parse values files with Helm").into(),
        );
    }
    let rendered: Value = serde_norway::from_str(&output)
        .map_err(|_| crate::exit::CliError::failure("could not read Helm parsed values"))?;
    let mut pending = Value::Object(serde_json::Map::new());
    for index in 0..files.len() {
        let key = format!("file{index}");
        let json = rendered
            .get("data")
            .and_then(|data| data.get(key.as_str()))
            .and_then(Value::as_str)
            .ok_or_else(|| crate::exit::CliError::failure("could not read Helm parsed values"))?;
        let values: Value = serde_json::from_str(json)
            .map_err(|_| crate::exit::CliError::failure("could not read Helm parsed values"))?;
        if !values.is_object() {
            return Err(
                crate::exit::CliError::failure("values files must contain YAML maps").into(),
            );
        }
        merge_values(&mut pending, values);
    }
    Ok(pending)
}

/// Parse pending input before reading the release so invalid input cannot
/// produce a reassuring clean report. Stored values exclude chart defaults.
pub async fn lint_values(common: CommonOpts, files: Vec<PathBuf>) -> Result<LintValuesOutput> {
    let pending = pending_values(&files).await?;

    // The shared reader can include raw Helm stderr in an error. Do not allow
    // a provider response to quote stored values in this path only command.
    let stored = fetch_release_values(&common).await.map_err(|error| {
        let message = format!(
            "could not read stored user values for release {} in namespace {}",
            common.release, common.namespace
        );
        if crate::exit::classify(&error).0 == crate::exit::ExitClass::Transient {
            crate::exit::CliError::transient(message)
        } else {
            crate::exit::CliError::failure(message)
        }
    })?;
    let release_exists = stored.is_some();
    let empty = Value::Object(serde_json::Map::new());
    let changed_paths = changed_paths(stored.as_ref().unwrap_or(&empty), &pending);
    Ok(LintValuesOutput {
        changed_paths,
        release_exists,
    })
}

/// Helm merges maps recursively and replaces lists, scalars, and nulls.
fn merge_values(previous: &mut Value, later: Value) {
    match (previous, later) {
        (Value::Object(previous), Value::Object(later)) => {
            for (key, value) in later {
                if let Some(existing) = previous.get_mut(&key) {
                    merge_values(existing, value);
                } else {
                    previous.insert(key, value);
                }
            }
        }
        (previous, later) => *previous = later,
    }
}

/// Only terminal nodes count. Internal paths stay structured so a key that
/// contains punctuation cannot collide with a nested path or list index.
fn leaf_values(value: &Value) -> BTreeMap<Vec<PathSegment>, &Value> {
    fn walk<'a>(
        value: &'a Value,
        path: &mut Vec<PathSegment>,
        values: &mut BTreeMap<Vec<PathSegment>, &'a Value>,
    ) {
        match value {
            Value::Object(map) if !map.is_empty() => {
                for (key, child) in map {
                    path.push(PathSegment::Key(key.clone()));
                    walk(child, path, values);
                    path.pop();
                }
            }
            Value::Array(items) if !items.is_empty() => {
                for (index, child) in items.iter().enumerate() {
                    path.push(PathSegment::Index(index));
                    walk(child, path, values);
                    path.pop();
                }
            }
            _ if !path.is_empty() => {
                values.insert(path.clone(), value);
            }
            _ => {}
        }
    }

    let mut values = BTreeMap::new();
    walk(value, &mut Vec::new(), &mut values);
    values
}

fn render_path(path: &[PathSegment]) -> String {
    let mut rendered = String::new();
    for segment in path {
        match segment {
            PathSegment::Key(key)
                if !key.is_empty()
                    && key
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_') =>
            {
                if !rendered.is_empty() {
                    rendered.push('.');
                }
                rendered.push_str(key);
            }
            PathSegment::Key(key) => {
                rendered.push('[');
                // Serializing a key cannot expose its value. The key is already
                // a path component, and JSON quoting makes it unambiguous.
                rendered.push_str(&serde_json::to_string(key).expect("string serialization"));
                rendered.push(']');
            }
            PathSegment::Index(index) => {
                rendered.push('[');
                rendered.push_str(&index.to_string());
                rendered.push(']');
            }
        }
    }
    rendered
}

fn changed_paths(stored: &Value, pending: &Value) -> Vec<String> {
    let stored = leaf_values(stored);
    let pending = leaf_values(pending);
    let paths: BTreeSet<_> = stored.keys().chain(pending.keys()).cloned().collect();
    let mut result: Vec<_> = paths
        .iter()
        .filter(|path| stored.get(*path) != pending.get(*path))
        .map(|path| render_path(path))
        .collect();
    result.sort();
    result.dedup();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_maps_merge_but_replacements_remove_old_children() {
        let mut pending = serde_json::json!({"shared": {"keep": false}, "replace": {"leaf": 1}});
        merge_values(
            &mut pending,
            serde_json::json!({"shared": {}, "replace": null}),
        );
        assert_eq!(
            pending,
            serde_json::json!({"shared": {"keep": false}, "replace": null})
        );
        assert_eq!(
            changed_paths(
                &serde_json::json!({"shared": {"keep": false}, "replace": {"leaf": 1}}),
                &pending,
            ),
            vec!["replace", "replace.leaf"]
        );
    }

    #[test]
    fn changed_leaves_include_additions_false_null_and_empty_collections() {
        let stored = serde_json::json!({
            "disabled": false,
            "unset": null,
            "emptyMap": {},
            "emptyList": [],
            "emptyString": "",
            "items": [{"name": "first"}, {"name": "second"}],
            "a.b": {"[one]": true},
        });
        let pending = serde_json::json!({
            "disabled": true,
            "unset": false,
            "emptyMap": [],
            "emptyList": {},
            "emptyString": "changed",
            "items": [{"name": "first"}, {"name": "third"}, {"name": "fourth"}],
            "a.b": {"[one]": true},
            "added": {"leaf": 0},
        });
        assert_eq!(
            changed_paths(&stored, &pending),
            vec![
                "added.leaf",
                "disabled",
                "emptyList",
                "emptyMap",
                "emptyString",
                "items[1].name",
                "items[2].name",
                "unset",
            ]
        );
        assert!(changed_paths(&stored, &stored).is_empty());
    }
}
