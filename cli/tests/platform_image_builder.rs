//! #1931: exactly one CLI function invokes `docker build` for a platform image,
//! and the two runner identities (`curie-runner` and the `--build` ghcr `:dev`
//! ref) are produced together.
//!
//! Connector image builds (`connector_build.rs`) are a different product
//! surface and are excluded. This gate is about Curie platform images.

use std::path::{Path, PathBuf};

fn cli_src_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn production_src_files() -> Vec<(String, String)> {
    let dir = cli_src_dir();
    let mut paths = Vec::new();
    collect_rs_paths(&dir, &mut paths);
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let name = path
            .file_name()
            .and_then(|n| n.to_string_lossy().into_owned().into())
            .unwrap_or_default();
        if name == "connector_build.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        out.push((name, strip_cfg_test_modules(&text)));
    }
    assert!(
        !out.is_empty(),
        "expected cli/src/*.rs files; glob is misconfigured"
    );
    out
}

/// Recursively collect every `.rs` file under `dir`, subdirectories included
/// (`cli/src/ops/*.rs` since the ops module split). `connector_build.rs` is
/// still excluded by file name, wherever it lives.
fn collect_rs_paths(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
    {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir entry: {e}"));
        let path = entry.path();
        if path.is_dir() {
            collect_rs_paths(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Drop `#[cfg(test)]` modules so fixtures cannot satisfy or violate the gate.
fn strip_cfg_test_modules(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        if starts_with_at(&chars, i, "#[cfg(test)]") {
            i += "#[cfg(test)]".len();
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            // Skip an annotated `mod ... { ... }` or a single `fn` item.
            if starts_with_at(&chars, i, "mod ") || starts_with_at(&chars, i, "pub mod ") {
                while i < chars.len() && chars[i] != '{' {
                    i += 1;
                }
                if i < chars.len() && chars[i] == '{' {
                    i = skip_brace_block(&chars, i);
                }
                continue;
            }
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

fn starts_with_at(chars: &[char], i: usize, needle: &str) -> bool {
    let n: Vec<char> = needle.chars().collect();
    chars.get(i..i + n.len()) == Some(n.as_slice())
}

fn skip_brace_block(chars: &[char], mut i: usize) -> usize {
    let mut depth = 0;
    let mut in_str = false;
    let mut in_char = false;
    let mut escaped = false;
    while i < chars.len() {
        let c = chars[i];
        if in_str {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if in_char {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '\'' {
                in_char = false;
            }
            i += 1;
            continue;
        }
        match c {
            '"' => in_str = true,
            '\'' => in_char = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                i += 1;
                if depth == 0 {
                    return i;
                }
                continue;
            }
            '/' if chars.get(i + 1) == Some(&'/') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    i
}

fn function_items(src: &str) -> Vec<(String, String)> {
    let chars: Vec<char> = src.chars().collect();
    let mut items = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if let Some((name, start)) = match_fn_at(&chars, i) {
            let mut j = start;
            while j < chars.len() && chars[j] != '{' && chars[j] != ';' {
                j += 1;
            }
            if j < chars.len() && chars[j] == '{' {
                let end = skip_brace_block(&chars, j);
                items.push((name, chars[j..end].iter().collect()));
                i = end;
                continue;
            }
        }
        i += 1;
    }
    items
}

fn match_fn_at(chars: &[char], i: usize) -> Option<(String, usize)> {
    // Accept `fn name`, `pub fn`, `pub(crate) fn`, `pub(crate) async fn`, `async fn`.
    let rest: String = chars[i..].iter().take(80).collect();
    let trimmed = rest.trim_start();
    let skipped = rest.len() - trimmed.len();
    let mut s = trimmed;
    if let Some(x) = s.strip_prefix("pub(crate) ") {
        s = x;
    } else if let Some(x) = s.strip_prefix("pub ") {
        s = x;
    }
    if let Some(x) = s.strip_prefix("async ") {
        s = x;
    }
    let after = s.strip_prefix("fn ")?;
    // Must be a real item, not `fn` inside a comment we failed to skip. The
    // start index `i` should sit at a token boundary.
    if i > 0 {
        let prev = chars[i - 1];
        if prev.is_ascii_alphanumeric() || prev == '_' {
            return None;
        }
    }
    let name: String = after
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return None;
    }
    let start = i + skipped + (trimmed.len() - after.len());
    Some((name, start))
}

fn is_platform_docker_build(body: &str) -> bool {
    if !body.contains(r#"Command::new("docker")"#) && !body.contains("Command::new(\"docker\")") {
        return false;
    }
    // The docker *subcommand* `build` plus a Dockerfile `-f`, which is how
    // both current platform-image sites invoke it. `docker image inspect`
    // and `docker run` do not match.
    let has_build_arg = body.contains(r#""build""#) || body.contains("\"build\",");
    let has_file_flag = body.contains(r#""-f""#) || body.contains("\"-f\",");
    has_build_arg && has_file_flag
}

fn platform_docker_build_functions() -> Vec<(String, String)> {
    let mut found = Vec::new();
    for (file, src) in production_src_files() {
        for (name, body) in function_items(&src) {
            if is_platform_docker_build(&body) {
                found.push((file.clone(), name));
            }
        }
    }
    found.sort();
    found.dedup();
    found
}

fn file_src(name: &str) -> String {
    production_src_files()
        .into_iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("missing {name}"))
        .1
}

fn function_body(file: &str, fn_name: &str) -> String {
    let src = file_src(file);
    function_items(&src)
        .into_iter()
        .find(|(n, _)| n == fn_name)
        .unwrap_or_else(|| panic!("{file} has no fn {fn_name}"))
        .1
}

/// Adding a second `docker build` for a platform image must go red.
#[test]
fn exactly_one_function_invokes_docker_build_for_a_platform_image() {
    let found = platform_docker_build_functions();
    assert_eq!(
        found,
        vec![("commands.rs".to_string(), "build_image".to_string())],
        "exactly one function may shell `docker build` for a platform image; \
         found {found:?}. Extract new sites into `commands::build_image`."
    );
}

/// Both product paths must call that function, not shell docker themselves.
#[test]
fn curie_build_and_local_up_build_route_through_build_image() {
    let build = function_body("commands.rs", "build");
    assert!(
        build.contains("build_image("),
        "`curie build` must call build_image; body was: {build}"
    );
    assert!(
        !is_platform_docker_build(&build),
        "`curie build` must not invoke docker build itself"
    );

    let source = function_body("local.rs", "build_source_images");
    assert!(
        source.contains("build_image("),
        "`local up --build` must call build_image; body was: {source}"
    );
    assert!(
        !is_platform_docker_build(&source),
        "`build_source_images` must not invoke docker build itself"
    );
}

/// Changing the tag on one path without the other must go red.
///
/// The ghcr `:dev` ref is defined once (`source_image_ref`) and both the
/// compose env `--build` exports and the images `--build` builds must call it.
/// `build_image` must also tag the short `RUNNER_IMAGE` name when it builds
/// the runner, so `curie build` refreshes what a `--build` stack runs.
#[test]
fn runner_tags_are_one_identity_across_both_paths() {
    let local = file_src("local.rs");
    assert!(
        local.contains("fn source_image_ref"),
        "the ghcr source-image ref must be a named function so both paths share it"
    );

    let compose_env = function_body("local.rs", "compose_model_env");
    assert!(
        compose_env.contains("source_image_ref("),
        "compose_model_env must use source_image_ref so the stack runs what --build built; body: {compose_env}"
    );
    assert!(
        !compose_env.contains("ghcr.io/curie-eng/"),
        "compose_model_env must not inline a second copy of the ghcr ref; body: {compose_env}"
    );

    let source_builds = function_body("local.rs", "build_source_images");
    assert!(
        source_builds.contains("source_image_ref("),
        "build_source_images must use source_image_ref so the tag it builds matches compose; body: {source_builds}"
    );
    assert!(
        !source_builds.contains("ghcr.io/curie-eng/"),
        "build_source_images must not inline a second copy of the ghcr ref; body: {source_builds}"
    );

    let builder = function_body("commands.rs", "build_image");
    assert!(
        builder.contains("RUNNER_IMAGE") || builder.contains("platform_image_tags("),
        "build_image must reconcile the short runner name; body: {builder}"
    );
    assert!(
        builder.contains("source_image_ref(")
            || builder.contains("SOURCE_IMAGE_TAG")
            || builder.contains("platform_image_tags("),
        "build_image must reconcile the --build ghcr runner ref; body: {builder}"
    );

    // The relationship itself: building the runner Dockerfile under either
    // identity applies both tags. platform_image_tags is the named decision.
    let tags_fn = function_body("commands.rs", "platform_image_tags");
    assert!(
        tags_fn.contains("RUNNER_IMAGE"),
        "platform_image_tags must name the short runner identity"
    );
    assert!(
        tags_fn.contains("source_image_ref(") || tags_fn.contains("SOURCE_IMAGE_TAG"),
        "platform_image_tags must name the --build runner identity"
    );
}

/// The one-shot producer must use the same named running-stack resolver as the
/// other recreating local verbs, not rebuild half of that chain in message.rs.
#[test]
fn one_shot_dispatcher_image_uses_the_shared_running_stack_resolver() {
    let body = function_body("message.rs", "one_shot_dispatcher_image");
    assert!(
        body.contains("crate::local::running_stack_image("),
        "one_shot_dispatcher_image must delegate to the shared named-image resolver; body: {body}"
    );
    assert!(
        !body.contains("running_stack_tag(")
            && !body.contains("image_ref(")
            && !body.contains("image_present("),
        "one_shot_dispatcher_image must not independently compose the tag and presence probes; body: {body}"
    );
}

/// Sanity: the scanner still sees the files it claims to police.
#[test]
fn scanner_sees_the_two_callers() {
    let names: Vec<String> = function_items(&file_src("commands.rs"))
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert!(
        names.contains(&"build".to_string()),
        "commands.rs must still define build; scanner broken if not"
    );
    let local_names: Vec<String> = function_items(&file_src("local.rs"))
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert!(
        local_names.contains(&"build_source_images".to_string()),
        "local.rs must still define build_source_images; scanner broken if not"
    );
}

/// CLAUDE.md must keep `curie build` as the runner-image entry point (the
/// line the issue cites) and must not describe a second builder.
#[test]
fn claude_md_names_curie_build_as_the_runner_image_loop() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../CLAUDE.md");
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let runner_line = text
        .lines()
        .find(|l| l.contains("Building the runner image"))
        .unwrap_or("");
    assert!(
        runner_line.contains("`curie build`"),
        "CLAUDE.md must name curie build as the runner-image command; got: {runner_line}"
    );
    assert!(
        !text.contains("two independent") && !text.contains("its own `docker build`"),
        "CLAUDE.md must not document a second builder; that alternative was rejected"
    );
}
