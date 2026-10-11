//! The one integration test crate root for the curie CLI. Every file
//! cli/tests/<name>.rs is a module declared below. With `autotests = false`
//! an undeclared file never compiles, which the guard test below catches.

// Declare `mod support;` and the sorted `mod <name>;` lines for each test file here.

use std::fs;
use std::path::Path;

/// File names (sorted) ending in `.rs`, other than `integration.rs`, whose stem
/// has no line in `root_source` that equals `mod <stem>;` after trimming.
fn undeclared_test_files(file_names: &[String], root_source: &str) -> Vec<String> {
    let declared: Vec<&str> = root_source.lines().map(str::trim).collect();
    let mut missing: Vec<String> = file_names
        .iter()
        .filter(|name| name.as_str() != "integration.rs")
        .filter_map(|name| name.strip_suffix(".rs").map(|stem| (name, stem)))
        .filter(|(_, stem)| !declared.contains(&format!("mod {stem};").as_str()))
        .map(|(name, _)| name.clone())
        .collect();
    missing.sort();
    missing
}

#[test]
fn every_test_file_is_declared_in_the_integration_crate() {
    let tests_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut names = Vec::new();
    for entry in fs::read_dir(&tests_dir).expect("read cli/tests") {
        let entry = entry.expect("read cli/tests entry");
        if !entry.file_type().expect("file type").is_file() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if name.ends_with(".rs") {
                names.push(name.to_string());
            }
        }
    }
    let root_source =
        fs::read_to_string(tests_dir.join("integration.rs")).expect("read tests/integration.rs");
    let missing = undeclared_test_files(&names, &root_source);
    assert!(
        missing.is_empty(),
        "these test files are not declared, so they never compile: {}. \
         Add `mod <name>;` for each to cli/tests/integration.rs",
        missing.join(", ")
    );
}

#[test]
fn every_test_file_is_declared_guard_names_an_undeclared_file() {
    let names: Vec<String> = ["alpha.rs", "zz_probe.rs", "integration.rs", "notes.txt"]
        .iter()
        .map(|name| name.to_string())
        .collect();
    let missing = undeclared_test_files(&names, "mod alpha;\n// mod zz_probe;\n");
    assert_eq!(missing, vec!["zz_probe.rs"]);
}

#[test]
fn every_test_file_is_declared_guard_accepts_a_fully_declared_tree() {
    let names: Vec<String> = ["alpha.rs", "beta.rs", "integration.rs"]
        .iter()
        .map(|name| name.to_string())
        .collect();
    let missing = undeclared_test_files(&names, "mod support;\nmod alpha;\n    mod beta;\n");
    assert!(missing.is_empty(), "{missing:?}");
}
