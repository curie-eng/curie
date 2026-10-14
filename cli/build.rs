use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=CURIE_BUILD_CHANNEL");
    let channel = std::env::var("CURIE_BUILD_CHANNEL").unwrap_or_else(|_| "dev".to_string());
    match channel.as_str() {
        "release" | "dev" => {}
        other => panic!("CURIE_BUILD_CHANNEL must be `release` or `dev`, got `{other}`"),
    }
    println!("cargo:rustc-env=CURIE_BUILD_CHANNEL={channel}");

    // Only release builds can run the installer that records the commit
    // (#2128): `cargo install` uses the release profile and the release
    // workflow sets the channel. Debug and test builds skip the stamp so a
    // moved HEAD does not rerun this script and rebuild every unit (#4381).
    if std::env::var("PROFILE").as_deref() == Ok("release") || channel == "release" {
        stamp_repo_commit();
    }
    embed_result_schemas();
}

/// Stamp the repository HEAD into binaries built from a Git checkout. Source
/// archives and other no-Git builds deliberately carry no commit provenance.
fn stamp_repo_commit() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let manifest_dir = Path::new(&manifest_dir);

    // Cargo otherwise has no reason to rerun this build script when only HEAD
    // moves. Watch both the worktree HEAD and its loose branch ref; packed-refs
    // covers checkouts whose branch has not been materialized as a loose ref.
    if let Some(git_dir) = git_output(manifest_dir, &["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
    }
    if let (Some(common_dir), Some(head_ref)) = (
        git_output(manifest_dir, &["rev-parse", "--git-common-dir"]),
        git_output(manifest_dir, &["symbolic-ref", "-q", "HEAD"]),
    ) {
        let common_dir = Path::new(&common_dir);
        let common_dir = if common_dir.is_absolute() {
            common_dir.to_path_buf()
        } else {
            manifest_dir.join(common_dir)
        };
        println!(
            "cargo:rerun-if-changed={}",
            common_dir.join(head_ref).display()
        );
        println!(
            "cargo:rerun-if-changed={}",
            common_dir.join("packed-refs").display()
        );
    }

    // The embedded installer is built from the checkout's working files, so
    // only claim HEAD provenance when those files match it. This keeps the
    // build-time override aligned with ordinary deploy provenance discovery.
    if !git_worktree_is_clean(manifest_dir) {
        return;
    }

    let Some(commit) = git_output(manifest_dir, &["rev-parse", "--verify", "HEAD"]) else {
        return;
    };
    if commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        println!("cargo:rustc-env=CURIE_BUILD_COMMIT={commit}");
    }
}

fn git_worktree_is_clean(current_dir: &Path) -> bool {
    Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(current_dir)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .is_ok_and(|output| output.status.success() && output.stdout.is_empty())
}

fn git_output(current_dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(current_dir)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Embed every committed CLI result JSON Schema (issue #634) into the binary so a
/// released `curie` ships its schemas and can print them on demand
/// (`curie schema-index [NAME]`) with no source checkout. The generated table
/// is `(short_name, filename, contents)` rows plus the index; it is derived from
/// whatever `.schema.json` files sit in `cli/schema/`, so adding a schema needs
/// no edit here. The inventory contract test keeps that directory in lockstep
/// with the source's result families.
fn embed_result_schemas() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let schema_dir = Path::new(&manifest_dir).join("schema");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let out_path = Path::new(&out_dir).join("schemas_embedded.rs");

    // Re-run if any schema file (or the directory listing) changes.
    println!("cargo:rerun-if-changed={}", schema_dir.display());

    let mut files: Vec<String> = std::fs::read_dir(&schema_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", schema_dir.display()))
        .filter_map(|entry| {
            entry
                .ok()
                .map(|e| e.file_name().to_string_lossy().into_owned())
        })
        .filter(|name| name.ends_with(".schema.json"))
        .collect();
    files.sort();

    let mut generated = String::new();
    generated.push_str(
        "// @generated by build.rs: the committed CLI result schemas, embedded (#634).\n\
         /// The schema inventory index (`cli/schema/index.json`), embedded.\n",
    );
    let index_path = schema_dir.join("index.json");
    println!("cargo:rerun-if-changed={}", index_path.display());
    writeln!(
        generated,
        "pub static SCHEMA_INDEX: &str = include_str!({:?});",
        index_path.display().to_string()
    )
    .unwrap();

    generated.push_str(
        "/// Every embedded result schema as `(short_name, file_name, contents)`,\n\
         /// sorted by file name. `short_name` drops the `.schema.json` suffix.\n\
         pub static SCHEMAS: &[(&str, &str, &str)] = &[\n",
    );
    for file in &files {
        let short = file.trim_end_matches(".schema.json");
        let path = schema_dir.join(file);
        println!("cargo:rerun-if-changed={}", path.display());
        writeln!(
            generated,
            "    ({short:?}, {file:?}, include_str!({:?})),",
            path.display().to_string()
        )
        .unwrap();
    }
    generated.push_str("];\n");

    std::fs::write(&out_path, generated)
        .unwrap_or_else(|e| panic!("write {}: {e}", out_path.display()));
}
