//! #3568: every `kubectl` and `helm` child goes through `OpsCommand`.
//!
//! A literal `Command::new("kubectl")` or `Command::new("helm")` bypasses the one
//! place that builds argv, masks secrets for display, and delivers secret values
//! off the process table. `cli/src/ops/command.rs` owns the only spawn, so no
//! other source file may name either program in a `Command::new` call.

use std::fs;
use std::path::{Path, PathBuf};

const OWNER: &str = "src/ops/command.rs";

/// Offending `Command::new("kubectl"|"helm")` sites in one source text, found after
/// removing whitespace so a call split across lines is still caught.
fn literal_spawns(source: &str) -> Vec<&'static str> {
    let compact: String = source.chars().filter(|c| !c.is_whitespace()).collect();
    ["Command::new(\"kubectl\"", "Command::new(\"helm\""]
        .into_iter()
        .filter(|needle| {
            // `OpsCommand::new("kubectl", ..)` is the sanctioned constructor, so a
            // match must not be the tail of a longer identifier.
            compact.match_indices(needle).any(|(at, _)| {
                !compact[..at]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_')
            })
        })
        .collect()
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn detector_flags_split_and_std_spawns() {
    assert_eq!(
        literal_spawns("tokio::process::Command::new(\n    \"kubectl\")"),
        vec!["Command::new(\"kubectl\""]
    );
    assert_eq!(
        literal_spawns("std::process::Command::new(\"helm\").arg(\"status\")"),
        vec!["Command::new(\"helm\""]
    );
    assert!(literal_spawns("OpsCommand::new(\"kubectl\", args)").is_empty());
}

#[test]
fn no_literal_kubectl_or_helm_spawn_outside_ops_command() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    assert!(files.len() > 10, "found too few sources under cli/src");
    let owner = root.join(OWNER);
    let offenders: Vec<String> = files
        .iter()
        .filter(|path| **path != owner)
        .flat_map(|path| {
            let source = fs::read_to_string(path).expect("read source");
            literal_spawns(&source)
                .into_iter()
                .map(|needle| format!("{}: {needle})", path.display()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "route these through OpsCommand (crate::ops::run_capture and friends):\n{}",
        offenders.join("\n")
    );
}
