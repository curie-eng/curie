//! Stream C tests for issue #488: the boot env is declared once, in the frozen
//! contract, and every producer references that declaration instead of
//! retyping it.
//!
//! The CLI is a boot-env producer: `curie skill up` and `curie skill check`
//! hand a runner container its env on the `docker run` command line. Today
//! `cli/src/docker.rs` spells those names out as bare string literals, so a
//! rename in the contract leaves the CLI silently emitting a key the runner
//! never reads. These tests pin the two halves of that fix:
//!
//! 1. The spelling: no bare boot-env literal survives in `docker.rs`.
//! 2. The behavior: the emitted argv is byte-identical across the swap, so
//!    moving to the constants is provably a no-op on the wire.
//!
//! Test 1 is written to FAIL until the Implementer converts the literals; that
//! RED state is the contract handoff.

use std::path::PathBuf;

use curie::docker::StartSpec;
use curie_aci_protocol::env_keys;

/// Env namespaces the frozen `BootEnv` contract governs. A string literal in
/// this namespace inside `docker.rs` is a hand-typed copy of a declared key.
const CONTRACT_PREFIXES: [&str; 3] = ["CURIE_", "OTEL_EXPORTER_OTLP_", "ANTHROPIC_"];

/// The one contract-namespaced literal `docker.rs` may keep.
///
/// `CURIE_CHECK_TIMEOUT_S` is NOT a boot-env key and deliberately is not one.
/// It configures `curie skill check`, a one-shot `python -m
/// curie_runner.check` container that runs `--network none` with no session,
/// no budget, and no sandbox identity (`CheckSpec`), and it is read by
/// `runner/src/curie_runner/check.py:418` alone. `BootEnv` is the
/// multi-producer worker-to-runner SESSION boot contract; this var has exactly
/// one producer (this file) and one consumer (check.py), and neither the chart
/// nor the worker ever emits it. Forcing it into the frozen contract would add
/// a non-session var to a wire-locked model and make `from_env` parse it on
/// every sandbox boot. It is allowlisted rather than declared.
///
/// `CURIE_RUNNER_ALLOW_TOKENLESS` (#3821) is the same shape: a runner-local dev
/// flag, not a boot-env key. It lets a local skill-tier or eval-sweep runner,
/// which the CLI dials without a bearer, serve its control routes tokenless.
/// One producer (`StartSpec::run_args` in this file), one consumer
/// (`runner/src/curie_runner/config.py`), and the chart reserves the name so a
/// cluster runner can never carry it. It stays deliberately outside the frozen
/// contract, so the worker's `BootEnv` render can never emit it.
const ALLOWED_LITERALS: [&str; 2] = ["CURIE_CHECK_TIMEOUT_S", "CURIE_RUNNER_ALLOW_TOKENLESS"];

/// Runner-local knobs `StartSpec` emits that are not `BootEnv` keys. Spelled as
/// literals: a test that referenced the constant under test would pass through
/// any rename of it.
const RUNNER_LOCAL_KNOBS: [&str; 1] = ["CURIE_RUNNER_ALLOW_TOKENLESS"];

fn docker_rs() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/docker.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The contents of every double-quoted string literal in `source`, ignoring
/// comments (a name in a comment is prose, not a wire value).
///
/// A small hand lexer rather than a regex: the point is to see string literals
/// specifically, since after the fix the same names appear as IDENTIFIERS
/// (`env_keys::CURIE_MODEL`), which must not be flagged.
fn string_literals(source: &str) -> Vec<String> {
    let bytes: Vec<char> = source.chars().collect();
    let mut literals = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    // Code, LineComment, BlockComment, Str
    let mut state = "code";
    while i < bytes.len() {
        let c = bytes[i];
        let next = bytes.get(i + 1).copied().unwrap_or('\0');
        match state {
            "code" => {
                if c == '/' && next == '/' {
                    state = "line";
                    i += 2;
                    continue;
                }
                if c == '/' && next == '*' {
                    state = "block";
                    i += 2;
                    continue;
                }
                if c == '"' {
                    state = "str";
                    current.clear();
                }
            }
            "line" => {
                if c == '\n' {
                    state = "code";
                }
            }
            "block" => {
                if c == '*' && next == '/' {
                    state = "code";
                    i += 2;
                    continue;
                }
            }
            "str" => {
                if c == '\\' {
                    // Skip the escaped char; no escape sequence can spell a key.
                    i += 2;
                    continue;
                }
                if c == '"' {
                    literals.push(std::mem::take(&mut current));
                    state = "code";
                } else {
                    current.push(c);
                }
            }
            _ => unreachable!(),
        }
        i += 1;
    }
    literals
}

/// The production half of `docker.rs`. The `#[cfg(test)] mod tests` block
/// legitimately spells the env names out: those inline tests are behavioral
/// pins on the emitted argv, and a pin that referenced the same constant as the
/// code under test would assert nothing (it would pass through any rename).
fn production_source(source: &str) -> &str {
    match source.find("#[cfg(test)]") {
        Some(idx) => &source[..idx],
        None => source,
    }
}

#[test]
fn docker_rs_carries_no_bare_boot_env_literal() {
    let source = docker_rs();
    let offenders: Vec<String> = string_literals(production_source(&source))
        .into_iter()
        .filter(|lit| {
            CONTRACT_PREFIXES.iter().any(|p| lit.starts_with(p))
                && !ALLOWED_LITERALS.iter().any(|a| lit.starts_with(a))
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "cli/src/docker.rs retypes boot-env keys as bare string literals: {offenders:?}\n\
         The boot env is declared once, in aci_protocol.session.BootEnv (issue #488). \
         Import the generated constants (`use curie_aci_protocol::env_keys::*;`) and \
         build these args from them, so a contract rename breaks the build here instead \
         of silently emitting a key the runner never reads."
    );
}

fn spec() -> StartSpec {
    StartSpec {
        image: "curie-runner".into(),
        container_name: "curie-runner-local".into(),
        host_port: 7245,
        plugin_dir: PathBuf::from("/tmp/deal-desk"),
        session_id: "local-1".into(),
        sandbox_id: "local".into(),
        budget_json: r#"{"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#.into(),
        fake_model: true,
        network: Some("curie_default".into()),
        otel_endpoint: Some("http://otel-collector:4318".into()),
        model_base_url: Some("http://x-ollama:11434".into()),
        model: Some("claude-opus-4-8".into()),
        passthrough_env: vec![],
        docker_env: vec![],
    }
}

/// The behavioral half: the emitted command line must not move.
///
/// Byte-exact rather than `contains`, and over the every-branch-on spec, so the
/// literal-to-constant swap is provably a no-op on what Docker actually
/// receives: a changed name, a changed value, a dropped `-e`, or a reordering
/// all fail here. This is the pin that makes the spelling test above safe to
/// satisfy. Mirrors the frozen-argv pin `CheckSpec` already has in
/// `cli/tests/skill_check.rs`.
#[test]
fn start_spec_run_args_are_the_frozen_argv() {
    let expected: Vec<String> = [
        "run",
        "-d",
        "--name",
        "curie-runner-local",
        "-p",
        "7245:8080",
        "-v",
        "/tmp/deal-desk:/plugin:ro",
        "-e",
        "CURIE_PLUGIN_DIR=/plugin",
        "-e",
        "CURIE_SESSION_ID=local-1",
        "-e",
        "CURIE_SANDBOX_ID=local",
        "-e",
        r#"CURIE_BUDGET={"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#,
        "-e",
        "CURIE_RUNNER_ALLOW_TOKENLESS=1",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,mode=1777",
        "--tmpfs",
        "/home/runner:rw,mode=1777",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--label",
        "curietech.ai/managed-by=curie-cli",
        "--label",
        "curietech.ai/component=runner",
        "-e",
        "CURIE_FAKE_MODEL=1",
        "-e",
        "CURIE_MODEL=claude-opus-4-8",
        "-e",
        "ANTHROPIC_BASE_URL=http://x-ollama:11434",
        "--network",
        "curie_default",
        "-e",
        "OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318",
        "curie-runner",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(
        spec().run_args(),
        expected,
        "StartSpec::run_args drifted from the frozen boot argv"
    );
}

/// Every env NAME the CLI emits is a declared boot-env key, or one of the named
/// runner-local knobs.
///
/// The complement of the argv pin: that one freezes today's spelling, this one
/// holds it to the contract, so a future `-e` addition cannot introduce a key
/// `BootEnv` does not declare (the CLI equivalent of the chart render-assert).
/// The only carve-out is `RUNNER_LOCAL_KNOBS`, each with its reason above.
#[test]
fn every_emitted_env_name_is_a_declared_boot_env_key() {
    let declared = [
        env_keys::CURIE_PLUGIN_DIR,
        env_keys::CURIE_SESSION_ID,
        env_keys::CURIE_SANDBOX_ID,
        env_keys::CURIE_BUDGET,
        env_keys::CURIE_FAKE_MODEL,
        env_keys::CURIE_MODEL,
        env_keys::ANTHROPIC_BASE_URL,
        env_keys::OTEL_EXPORTER_OTLP_ENDPOINT,
    ];

    let args = spec().run_args();
    let names: Vec<&str> = args
        .iter()
        .zip(args.iter().skip(1))
        .filter(|(flag, _)| flag.as_str() == "-e")
        .map(|(_, value)| value.split('=').next().unwrap_or(""))
        .collect();

    // Non-vacuity floor: if run_args stopped emitting env entirely, the
    // membership loop below would pass over an empty set.
    assert!(
        names.len() >= 9,
        "expected the every-branch-on spec to emit 9+ env vars, got {names:?}"
    );
    for name in names {
        assert!(
            declared.contains(&name) || RUNNER_LOCAL_KNOBS.contains(&name),
            "`docker run -e {name}` is not a declared boot-env key in \
             aci_protocol.session.BootEnv (issue #488)"
        );
    }
}

/// Every `StartSpec` runner serves its control routes to a CLI that sends no
/// bearer (`cli/src/runner.rs`), so the tokenless dev flag is unconditional
/// (#3821). Checked on the eval-sweep shape, a real-model spec with every
/// optional branch off, so gating the flag on `fake_model` (or on any other
/// branch) fails here: without it a real-model local runner refuses to boot.
#[test]
fn start_spec_always_carries_the_tokenless_dev_flag() {
    let sweep = StartSpec {
        fake_model: false,
        network: None,
        otel_endpoint: None,
        model_base_url: None,
        model: None,
        ..spec()
    };

    for args in [spec().run_args(), sweep.run_args()] {
        let pairs = args
            .iter()
            .zip(args.iter().skip(1))
            .filter(|(flag, value)| {
                flag.as_str() == "-e" && value.as_str() == "CURIE_RUNNER_ALLOW_TOKENLESS=1"
            })
            .count();
        assert_eq!(
            pairs, 1,
            "StartSpec::run_args must emit `-e CURIE_RUNNER_ALLOW_TOKENLESS=1` exactly once, \
             got {pairs} in {args:?}"
        );
    }
}
