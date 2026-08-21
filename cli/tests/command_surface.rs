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

fn live_command_manifest() -> serde_json::Value {
    let output = Command::new(bin())
        .arg("schema")
        .output()
        .expect("run curie schema");
    assert!(
        output.status.success(),
        "curie schema failed\n{}",
        output_text(&output)
    );
    serde_json::from_slice(&output.stdout).expect("curie schema emits JSON")
}

fn cluster_status_plan(env_namespace: &str, flag_namespace: Option<&str>) -> Vec<String> {
    let mut command = Command::new(bin());
    command
        .arg("--json")
        .args(["cluster", "status", "--dry-run"])
        .env("CURIE_NAMESPACE", env_namespace);
    if let Some(namespace) = flag_namespace {
        command.args(["--namespace", namespace]);
    }
    let output = command.output().expect("run curie cluster status dry run");
    assert!(
        output.status.success(),
        "curie cluster status dry run failed\n{}",
        output_text(&output)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("status dry run emits JSON");
    value["plan"]
        .as_array()
        .expect("status dry run includes a plan")
        .iter()
        .map(|line| {
            line.as_str()
                .expect("status plan entries are strings")
                .to_string()
        })
        .collect()
}

fn assert_status_plan_namespace(plan: &[String], namespace: &str) {
    let namespaced: Vec<_> = plan.iter().filter(|line| line.contains(" -n ")).collect();
    assert!(
        !namespaced.is_empty(),
        "status plan must include namespace aware commands: {plan:?}"
    );
    let expected = format!(" -n {namespace}");
    assert!(
        namespaced.iter().all(|line| line.contains(&expected)),
        "every namespace aware command must use {namespace}: {plan:?}"
    );
    assert!(
        namespaced.iter().any(|line| line.starts_with("helm ")),
        "the Helm status command must use {namespace}: {plan:?}"
    );
    assert!(
        namespaced.iter().any(|line| line.starts_with("kubectl ")),
        "the kubectl status commands must use {namespace}: {plan:?}"
    );
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
        &["try"],
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
        "skill",
        "local",
        "cluster",
        "try",
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

#[test]
fn process_skill_help_distinguishes_tier_from_bundle_artifact() {
    let output = run_help(&["skill"]);
    assert!(
        output.status.success(),
        "expected success for skill help\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    let distinction =
        "`skill` names that tier, not a bundle skill artifact at `skills/<name>/SKILL.md`.";
    assert!(
        text.contains(distinction),
        "skill help must clearly distinguish its runner tier from the bundle artifact\n{text}"
    );
    assert!(
        text.contains("Subcommands: `skill <up|down|status|message|eval|approvals>`"),
        "skill help must introduce its subcommand list separately\n{text}"
    );
    assert!(
        !text.contains("skills/<name>/SKILL.md`: `skill <up|down|status|message|eval|approvals>"),
        "skill help must not attach the subcommand list to the artifact distinction\n{text}"
    );
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

#[test]
fn process_dev_help_lists_verify_fix_pin() {
    let output = run_help(&["dev"]);
    assert!(
        output.status.success(),
        "expected success for dev help\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        help_lists_subcommand(&text, "verify-fix-pin"),
        "missing verify-fix-pin\n{text}"
    );

    let leaf = run_help(&["dev", "verify-fix-pin"]);
    assert!(
        leaf.status.success(),
        "expected success for verify-fix-pin help\n{}",
        output_text(&leaf)
    );
    let leaf_text = output_text(&leaf);
    assert!(
        leaf_text.contains("<CHANGE>") && leaf_text.contains("<SELECTOR>"),
        "verify-fix-pin help must require a change and selector\n{leaf_text}"
    );
}

/// `curie dev remote-dev-spike` is the EXPERIMENTAL THROWAWAY remote-development
/// prototype. It is only useful if it is reachable from the one entry point the
/// repo documents, so assert the verb is listed and that it takes pass-through
/// arguments (the mode selector: `run`, `follow-up`, `down`).
#[test]
fn process_dev_help_lists_the_remote_dev_spike() {
    let output = run_help(&["dev"]);
    assert!(
        output.status.success(),
        "expected success for dev help\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        help_lists_subcommand(&text, "remote-dev-spike"),
        "missing remote-dev-spike\n{text}"
    );

    let leaf = run_help(&["dev", "remote-dev-spike"]);
    assert!(
        leaf.status.success(),
        "expected success for remote-dev-spike help\n{}",
        output_text(&leaf)
    );
    let leaf_text = output_text(&leaf);
    assert!(
        leaf_text.contains("[ARGS]"),
        "remote-dev-spike help must accept pass-through args\n{leaf_text}"
    );
}

/// `curie dev remote-dev-session` is the EXPERIMENTAL developer-facing remote
/// development session loop. Same reachability contract as the spike above: it
/// is only useful if it is listed under the one entry point the repo documents,
/// and it must take pass-through arguments (the verb selector: `start`, `turn`,
/// `status`, `finish`, `down`).
#[test]
fn process_dev_help_lists_the_remote_dev_session() {
    let output = run_help(&["dev"]);
    assert!(
        output.status.success(),
        "expected success for dev help\n{}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        help_lists_subcommand(&text, "remote-dev-session"),
        "missing remote-dev-session\n{text}"
    );

    let leaf = run_help(&["dev", "remote-dev-session"]);
    assert!(
        leaf.status.success(),
        "expected success for remote-dev-session help\n{}",
        output_text(&leaf)
    );
    let leaf_text = output_text(&leaf);
    assert!(
        leaf_text.contains("[ARGS]"),
        "remote-dev-session help must accept pass-through args\n{leaf_text}"
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

#[test]
fn message_tiers_share_the_conversation_flag_and_default() {
    let manifest = live_command_manifest();
    let mut contracts = Vec::new();

    for tier in ["skill", "local", "cluster"] {
        let tier_command = manifest["subcommands"]
            .as_array()
            .expect("manifest has top level subcommands")
            .iter()
            .find(|command| command["name"] == tier)
            .unwrap_or_else(|| panic!("manifest has the {tier} tier"));
        let message = tier_command["subcommands"]
            .as_array()
            .expect("tier has subcommands")
            .iter()
            .find(|command| command["name"] == "message")
            .unwrap_or_else(|| panic!("{tier} has the message verb"));
        let conversation = message["args"]
            .as_array()
            .expect("message has arguments")
            .iter()
            .find(|arg| arg["id"] == "continue")
            .unwrap_or_else(|| panic!("{tier} message exposes --continue"));

        contracts.push(serde_json::json!({
            "id": conversation["id"],
            "long": conversation["long"],
            "positional": conversation["positional"],
            "required": conversation["required"],
            "possible_values": conversation["possible_values"],
            "default_values": conversation["default_values"],
        }));
    }

    assert!(
        contracts.windows(2).all(|pair| pair[0] == pair[1]),
        "skill, local, and cluster message must share conversation flag semantics: {contracts:?}"
    );
    assert!(
        contracts[0]["default_values"].is_null(),
        "message must start a fresh conversation unless --continue is present"
    );
}

#[test]
fn cluster_namespace_env_reaches_every_cluster_verb() {
    let manifest = live_command_manifest();
    let cluster = manifest["subcommands"]
        .as_array()
        .expect("manifest has top level subcommands")
        .iter()
        .find(|command| command["name"] == "cluster")
        .expect("manifest has the cluster command");
    let subcommands = cluster["subcommands"]
        .as_array()
        .expect("cluster has subcommands");
    assert!(
        !subcommands.is_empty(),
        "cluster namespace coverage must not be vacuous"
    );

    for subcommand in subcommands {
        let name = subcommand["name"]
            .as_str()
            .expect("cluster subcommand has a name");
        let namespace_args: Vec<_> = subcommand["args"]
            .as_array()
            .expect("cluster subcommand has arguments")
            .iter()
            .filter(|arg| arg["id"] == "namespace")
            .collect();
        assert_eq!(
            namespace_args.len(),
            1,
            "cluster {name} must expose exactly one namespace argument"
        );
        assert_eq!(
            namespace_args[0]["env"], "CURIE_NAMESPACE",
            "cluster {name} must read CURIE_NAMESPACE"
        );
    }
}

#[test]
fn cluster_namespace_flag_wins_over_environment() {
    let env_plan = cluster_status_plan("env-namespace", None);
    assert_status_plan_namespace(&env_plan, "env-namespace");

    let flag_plan = cluster_status_plan("env-namespace", Some("flag-namespace"));
    assert_status_plan_namespace(&flag_plan, "flag-namespace");
    assert!(
        flag_plan.iter().all(|line| !line.contains("env-namespace")),
        "the environment namespace must not survive an explicit flag: {flag_plan:?}"
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

#[test]
fn process_try_help_lists_only_keep_beyond_global_flags() {
    let top_level = output_text(&run_help(&[]));
    assert!(
        help_lists_subcommand(&top_level, "try"),
        "top level help must list try\n{top_level}"
    );

    let global_flags = help_flags(&[]);
    let try_flags = help_flags(&["try"]);
    let command_flags: Vec<_> = try_flags.difference(&global_flags).cloned().collect();
    assert_eq!(
        command_flags,
        vec!["--keep".to_string()],
        "try must expose --keep as its only command specific flag"
    );
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
