use std::process::Command;

use curie::retired_hint;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn run_help(args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .args(args)
        .arg("--help")
        .output()
        .expect("run curie --help")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn help_lists_subcommand(text: &str, name: &str) -> bool {
    text.lines().any(|line| {
        let line = line.trim_start();
        line == name
            || line.starts_with(&format!("{name} "))
            || line.starts_with(&format!("{name}\t"))
    })
}

#[test]
fn process_help_routes_positive_forms() {
    let cases: &[&[&str]] = &[
        &["skill", "up"],
        &["skill", "down"],
        &["skill", "status"],
        &["skill", "message"],
        &["skill", "eval"],
        &["skill", "check"],
        &["local", "up"],
        &["local", "down"],
        &["local", "status"],
        &["local", "message"],
        &["local", "eval"],
        &["local", "deploy"],
        &["cluster", "up"],
        &["cluster", "down"],
        &["cluster", "status"],
        &["cluster", "message"],
        &["cluster", "eval"],
        &["cluster", "deploy"],
        &["adapter", "scaffold"],
        &["adapter", "validate"],
        &["adapter", "bind"],
        &["adapter", "token"],
        &["adapter", "smoke-test"],
        &["init"],
        &["interactive"],
        &["secrets", "set"],
        &["secrets", "list"],
        &["secrets", "unset"],
    ];

    for args in cases.iter().copied() {
        let output = run_help(args);
        assert!(
            output.status.success(),
            "expected success for {:?}\n{}",
            args,
            output_text(&output)
        );
    }
}

#[test]
fn process_help_rejects_retired_top_level_tokens() {
    let cases = [
        "start",
        "stop",
        "send",
        "eval",
        "runner-status",
        "chat",
        "steer",
        "interrupt",
        "up",
        "down",
        "status",
        "message",
        "deploy",
    ];

    for token in cases {
        let output = run_help(&[token]);
        assert!(
            !output.status.success(),
            "expected failure for {token}\n{}",
            output_text(&output)
        );
    }
}

#[test]
fn process_help_rejects_retired_local_flag_on_a_leaf_command() {
    let output = run_help(&["skill", "message", "hello", "--local"]);
    assert!(
        !output.status.success(),
        "expected failure for retired --local on a leaf command\n{}",
        output_text(&output)
    );
}

#[test]
fn process_help_top_level_lists_new_surface_and_hides_retired_verbs() {
    let output = run_help(&[]);
    assert!(
        output.status.success(),
        "expected success for top level help\n{}",
        output_text(&output)
    );
    let text = output_text(&output);

    for needle in [
        "adapter",
        "skill",
        "local",
        "cluster",
        "init",
        "interactive",
        "secrets",
    ] {
        assert!(
            help_lists_subcommand(&text, needle),
            "missing {needle}\n{text}"
        );
    }

    for needle in [
        "start",
        "stop",
        "send",
        "eval",
        "runner-status",
        "chat",
        "steer",
        "interrupt",
        "up",
        "down",
        "status",
        "message",
        "deploy",
    ] {
        assert!(
            !help_lists_subcommand(&text, needle),
            "unexpected retired verb {needle}\n{text}"
        );
    }
}

/// `curie dev plugin-compat` is the operator-facing name of the outbound
/// Claude-Code-compatibility gate (see the bundle-format seam doc). If the verb
/// stops being reachable, the gate is still in CI but nobody can run it locally
/// before pushing.
#[test]
fn process_dev_help_lists_the_plugin_compat_gate() {
    let output = run_help(&["dev"]);
    assert!(
        output.status.success(),
        "expected success for dev help\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        help_lists_subcommand(&text, "plugin-compat"),
        "missing plugin-compat\n{text}"
    );
}

/// The checked-in manifest must equal what `curie schema` emits from the live
/// clap grammar. This is the generated-artifact + CI drift gate (mirroring the
/// schema-export discipline for `packages/aci-protocol` / `packages/plugin-format`):
/// any grammar change (new command, flag, default, env var, help text) must be
/// accompanied by a regenerated `cli/command-manifest.json` in the same PR.
#[test]
fn command_manifest_matches_committed_artifact() {
    let output = Command::new(bin())
        .arg("schema")
        .output()
        .expect("run curie schema");
    assert!(
        output.status.success(),
        "curie schema failed\n{}",
        output_text(&output)
    );
    let generated = String::from_utf8(output.stdout).expect("manifest is utf-8");

    let manifest_path = concat!(env!("CARGO_MANIFEST_DIR"), "/command-manifest.json");
    let committed =
        std::fs::read_to_string(manifest_path).expect("cli/command-manifest.json is committed");

    assert_eq!(
        generated, committed,
        "cli/command-manifest.json is stale; regenerate with \
         `cargo run -- schema > cli/command-manifest.json`"
    );
}

/// `dump-commands` is the documented alias for the hidden `schema` verb.
#[test]
fn dump_commands_alias_emits_same_manifest() {
    let schema = Command::new(bin())
        .arg("schema")
        .output()
        .expect("run curie schema");
    let alias = Command::new(bin())
        .arg("dump-commands")
        .output()
        .expect("run curie dump-commands");
    assert!(schema.status.success() && alias.status.success());
    assert_eq!(schema.stdout, alias.stdout);
}

/// Collect every long flag (`--foo`) the command's help exposes.
fn help_flags(args: &[&str]) -> std::collections::BTreeSet<String> {
    let output = run_help(args);
    assert!(
        output.status.success(),
        "expected success for {:?}\n{}",
        args,
        output_text(&output)
    );
    output_text(&output)
        .split_whitespace()
        .filter(|token| token.starts_with("--") && token.len() > 2)
        .map(|token| token.trim_end_matches(',').to_string())
        .collect()
}

/// The agent-target verbs share one `AgentTarget<T>` whose only per-tier
/// difference is where the platform API listens. Lock both defaults so the
/// shared struct cannot silently collapse them onto one port (issue #466).
///
/// Assert the full bracketed clap default string, not a bare port number:
/// `8000` is a substring of `28000`, so a bare-port assertion for `cluster`
/// would still pass even if `cluster` silently inherited `local`'s default.
#[test]
fn agent_target_verbs_keep_their_per_tier_api_url_default() {
    for verb in ["versions", "memory", "approvals"] {
        let local = output_text(&run_help(&["local", verb]));
        assert!(
            local.contains("[default: http://localhost:28000]"),
            "local {verb} lost its api-url default\n{local}"
        );

        // The cluster tier deliberately has NO localhost:8000 default (#524):
        // --api-url is optional and discovered from the release when omitted, so
        // the dev localhost default (which silently fails against a real release)
        // is gone. Instead the cluster verb exposes --namespace/--release.
        let cluster = output_text(&run_help(&["cluster", verb]));
        assert!(
            !cluster.contains("[default: http://localhost:8000]"),
            "cluster {verb} must not carry the dev localhost:8000 api-url default\n{cluster}"
        );
        assert!(
            cluster.contains("--namespace") && cluster.contains("--release"),
            "cluster {verb} must expose --namespace/--release for release discovery\n{cluster}"
        );
    }
}

/// Tier parity gate: the shared `AgentTarget<T>` means a flag added to `local
/// versions` is structurally impossible to forget on `cluster versions`. This
/// test fails if the two tiers ever drift apart again (issue #466).
#[test]
fn agent_target_verbs_expose_the_same_flags_on_both_tiers() {
    // The two tiers share the agent-facing flags (--agent/--api-url/--api-key/
    // --dry-run) but DIVERGE intentionally on the cluster side (#524): cluster
    // adds --namespace/--release to discover the release's connection. So the
    // cluster flag set must be a strict superset of the local one, differing only
    // by those two discovery flags -- a flag added to local can still never be
    // silently dropped from cluster.
    for verb in ["versions", "memory", "approvals"] {
        let local = help_flags(&["local", verb]);
        let cluster = help_flags(&["cluster", verb]);
        for flag in &local {
            assert!(
                cluster.contains(flag),
                "cluster {verb} is missing the local flag {flag:?}\nlocal: {local:?}\ncluster: {cluster:?}"
            );
        }
        let extra: Vec<_> = cluster.iter().filter(|f| !local.contains(*f)).collect();
        assert_eq!(
            extra.len(),
            2,
            "cluster {verb} should add exactly --namespace/--release; got extras {extra:?}"
        );
    }
}

fn to_argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_string()).collect()
}

fn assert_hint_contains(argv: &[&str], needle: &str) {
    let argv = to_argv(argv);
    let hint = retired_hint(&argv).unwrap_or_else(|| panic!("expected hint for {:?}", argv));
    assert!(
        hint.contains(needle),
        "expected {needle:?} in hint {hint:?} for {:?}",
        argv
    );
}

fn assert_hint_contains_any(argv: &[&str], needles: &[&str]) {
    let argv = to_argv(argv);
    let hint = retired_hint(&argv).unwrap_or_else(|| panic!("expected hint for {:?}", argv));
    assert!(
        needles.iter().any(|needle| hint.contains(needle)),
        "expected one of {needles:?} in hint {hint:?} for {:?}",
        argv
    );
}

fn assert_hint_none(argv: &[&str]) {
    let argv = to_argv(argv);
    assert_eq!(retired_hint(&argv), None, "unexpected hint for {:?}", argv);
}

#[test]
fn retired_hint_maps_retired_tokens_and_ignores_global_flags() {
    let cases: &[(&[&str], &str)] = &[
        (&["start"], "skill up"),
        (&["stop"], "skill down"),
        (&["send"], "skill message"),
        (&["runner-status"], "skill status"),
        (&["eval"], "skill eval"),
        (&["chat"], "local message"),
        (&["up"], "cluster up"),
        (&["down"], "cluster down"),
        (&["status"], "cluster status"),
        (&["message"], "cluster message"),
        (&["deploy"], "cluster deploy"),
        (&["steer"], "removed"),
        (&["interrupt"], "removed"),
        (&["--local", "start"], "local"),
        (&["start", "--local"], "local"),
        (&["skill", "up", "--local"], "local"),
        (&["--debug", "start"], "skill up"),
        (&["-q", "stop"], "skill down"),
        (&["--quiet", "send"], "skill message"),
        (&["--color", "always", "status"], "cluster status"),
        (&["--color=always", "deploy"], "cluster deploy"),
    ];

    for (argv, needle) in cases.iter().copied() {
        assert_hint_contains(argv, needle);
    }

    assert_hint_contains_any(&["interrupt"], &["Ctrl-C", "skill message"]);
}

#[test]
fn retired_hint_returns_none_for_valid_starts_help_and_message_bodies() {
    let cases: &[&[&str]] = &[
        &["skill", "up"],
        &["skill", "message", "hello"],
        &["skill", "message", "please deploy the thing"],
        &["local", "up"],
        &["local", "message", "please deploy the thing"],
        &["local", "message", "please", "deploy", "the", "thing"],
        &["local", "deploy"],
        &["cluster", "up"],
        &["cluster", "message", "status of the world"],
        &["cluster", "message", "status", "of", "the", "world"],
        &["cluster", "deploy"],
        &["init", "my-plugin"],
        &["--help"],
        &["--debug"],
        &["--debug", "skill", "up"],
        &["--color", "always"],
        &["--color=always"],
        &["--color", "always", "cluster", "status"],
        &["--color", "always", "skill", "up"],
        &["-h"],
        &["-V"],
        &["--version"],
        &[],
    ];

    for argv in cases.iter().copied() {
        assert_hint_none(argv);
    }
}
