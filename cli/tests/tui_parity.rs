//! Parity gate between the TUI recipe catalog and the live CLI grammar.
//!
//! What it does: execs each Command recipe's argv (with placeholder field
//! values) plus `--help` against the built `curie` binary and asserts it
//! resolves, mirroring the manifest gate in `command_surface.rs`.
//!
//! What it catches: a recipe pointing at a renamed or removed verb, or
//! passing a renamed or removed flag, now fails CI. Clap exits non-zero on
//! an unknown subcommand or flag even when `--help` is present.
//!
//! What it does NOT catch: `--help` short-circuits clap's required-argument
//! validation, so this gate will not detect a verb that gains a new required
//! argument if the recipe is not updated to supply it. Catching that would
//! require parsing the argv in-process through the clap `Command` (without
//! `--help`), which is not possible from an integration test today because
//! the `Cli` grammar is defined in the binary crate (`main.rs`), not the
//! library. Relocating the grammar into the library to enable in-process
//! parsing is a separate, larger change.
//!
//! The guided prompt check also drives cancellation through a real terminal
//! and requires the recovery message before the action list accepts input.

#[cfg(target_os = "linux")]
use std::io::Write;
use std::process::Command;
#[cfg(target_os = "linux")]
use std::process::Stdio;
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

use curie::recipes::command_recipe_argvs;

#[cfg(target_os = "linux")]
const CAPTURE_ROWS: usize = 40;
#[cfg(target_os = "linux")]
const CAPTURE_COLUMNS: usize = 240;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn run_help(argv: &[String]) -> std::process::Output {
    Command::new(bin())
        .args(argv)
        .arg("--help")
        .output()
        .expect("run curie --help")
}

fn output_text(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

#[cfg(target_os = "linux")]
fn replay_terminal(text: &str) -> String {
    let mut screen = vec![vec![' '; CAPTURE_COLUMNS]; CAPTURE_ROWS];
    let mut row = 0usize;
    let mut column = 0usize;
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            let mut parameters = String::new();
            let mut final_byte = None;
            for next in chars.by_ref() {
                if next.is_ascii() && ('@'..='~').contains(&next) {
                    final_byte = Some(next);
                    break;
                }
                parameters.push(next);
            }
            if matches!(final_byte, Some('H' | 'f')) {
                let mut position = parameters.split(';');
                row = position
                    .next()
                    .filter(|value| !value.is_empty())
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .saturating_sub(1)
                    .min(CAPTURE_ROWS - 1);
                column = position
                    .next()
                    .filter(|value| !value.is_empty())
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1)
                    .saturating_sub(1)
                    .min(CAPTURE_COLUMNS - 1);
            }
            continue;
        }

        match ch {
            '\r' => column = 0,
            '\n' => {
                row = (row + 1).min(CAPTURE_ROWS - 1);
            }
            printable if !printable.is_control() => {
                if column >= CAPTURE_COLUMNS {
                    column = 0;
                    row = (row + 1).min(CAPTURE_ROWS - 1);
                }
                screen[row][column] = printable;
                column += 1;
            }
            _ => {}
        }
    }

    screen
        .into_iter()
        .map(|line| line.into_iter().collect::<String>().trim_end().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every Command recipe in the TUI catalog must resolve to a real verb with
/// real flags in the live CLI grammar. `<argv> --help` exits 0 only when
/// every token in argv is still a valid subcommand/flag path -- clap errors
/// on an unknown verb or flag even with --help present, so success here is
/// equivalent to "this recipe still matches the compiled grammar."
#[test]
fn every_command_recipe_resolves_to_a_real_verb() {
    let recipes = command_recipe_argvs();
    assert!(
        !recipes.is_empty(),
        "expected at least one Command recipe; the RecipeKind::Command filter matched none"
    );

    for (title, argv) in &recipes {
        let output = run_help(argv);
        assert!(
            output.status.success(),
            "recipe {title:?} no longer resolves: argv {argv:?}\n{}",
            output_text(&output)
        );
    }

    eprintln!("tui_parity: exercised {} command recipes", recipes.len());
}

/// The cluster tier must be REACHABLE from the TUI catalog, not just claimed
/// by the Platform tab's tier explainer (#463). The catalog has to expand its
/// tier-bearing recipes into cluster argv, and every one of those argvs has to
/// resolve against the real grammar -- the same `<argv> --help` exec the
/// catalog-wide gate uses, so a cluster verb that does not exist fails here.
#[test]
fn cluster_tier_recipes_are_expanded_and_resolve() {
    let recipes = command_recipe_argvs();
    let cluster: Vec<&(&str, Vec<String>)> = recipes
        .iter()
        .filter(|(_, argv)| argv.first().map(String::as_str) == Some("cluster"))
        .collect();

    assert!(
        !cluster.is_empty(),
        "no cluster argv in the TUI catalog: the cluster tier is unreachable"
    );

    // Governance, not just the pre-existing cluster status/message recipes:
    // a tier-bearing platform recipe must actually reach the cluster tier.
    assert!(
        cluster
            .iter()
            .any(|(_, argv)| argv.get(1).map(String::as_str) == Some("versions")),
        "no `cluster versions` argv: the platform governance recipes are still local-only"
    );

    for (title, argv) in &cluster {
        let output = run_help(argv);
        assert!(
            output.status.success(),
            "cluster recipe {title:?} does not resolve: argv {argv:?}\n{}",
            output_text(&output)
        );
    }

    eprintln!("tui_parity: exercised {} cluster recipes", cluster.len());
}

/// Negative control: proves the gate mechanism actually rejects drift rather
/// than always passing regardless of argv.
#[test]
fn a_bogus_verb_fails_the_gate() {
    let argv = vec!["definitely-not-a-real-verb".to_string()];
    let output = run_help(&argv);
    assert!(
        !output.status.success(),
        "expected failure for a bogus verb\n{}",
        output_text(&output)
    );
}

#[cfg(target_os = "linux")]
fn cancel_next_local_deploy_prompt(
    environment: &[(&str, &str)],
    after_intro: &[&[u8]],
    cancellation: &str,
) -> String {
    let config = tempfile::tempdir().expect("create isolated config directory");
    let shell_command = format!(
        "stty rows {CAPTURE_ROWS} cols {CAPTURE_COLUMNS}; exec {} interactive",
        bin()
    );
    let mut process = Command::new("script");
    process
        .args([
            "--quiet",
            "--return",
            "--command",
            &shell_command,
            "/dev/null",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("TERM", "xterm-256color")
        .env("NO_COLOR", "1")
        .env("CURIE_CONFIG_DIR", config.path());
    for name in [
        "CURIE_CREDENTIALS",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CURIE_MODEL_BASE_URL",
        "SLACK_APP_TOKEN",
        "SLACK_BOT_TOKEN",
    ] {
        process.env_remove(name);
    }
    process.envs(environment.iter().copied());
    let mut child = process
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start interactive CLI in a terminal");

    let mut input = child.stdin.take().expect("terminal input");
    thread::sleep(Duration::from_millis(200));
    for keys in [b"\t\t\t\t".as_slice(), b"\r", b"\r", b"\r"] {
        input.write_all(keys).expect("send terminal input");
        input.flush().expect("flush terminal input");
        thread::sleep(Duration::from_millis(150));
    }
    for keys in after_intro {
        input.write_all(keys).expect("send terminal input");
        input.flush().expect("flush terminal input");
        thread::sleep(Duration::from_millis(150));
    }
    thread::sleep(Duration::from_millis(350));
    input.write_all(b"q").expect("leave interactive CLI");
    input.flush().expect("flush exit input");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().expect("poll interactive CLI").is_some() {
            break;
        }
        if Instant::now() >= deadline {
            drop(input);
            child.kill().expect("stop stuck interactive CLI");
            let output = child.wait_with_output().expect("collect terminal output");
            panic!(
                "{cancellation} cancellation did not return to the action list:\n{}",
                output_text(&output)
            );
        }
        thread::sleep(Duration::from_millis(25));
    }
    drop(input);

    let output = child.wait_with_output().expect("collect terminal output");
    let text = replay_terminal(&output_text(&output));
    assert!(output.status.success(), "interactive CLI failed:\n{text}");
    text
}

#[cfg(target_os = "linux")]
#[test]
fn guided_prompt_cancellation_names_visible_recovery() {
    let text = cancel_next_local_deploy_prompt(
        &[
            ("CURIE_CREDENTIALS", "sk-EXAMPLE-model-credential"),
            ("SLACK_APP_TOKEN", "xapp-EXAMPLE-app-token"),
            ("SLACK_BOT_TOKEN", "xoxb-EXAMPLE-bot-token"),
        ],
        &[b"\x1b"],
        "channel",
    );
    assert!(
        text.contains("Action failed: A Slack channel ID is required to deploy to Slack"),
        "channel cancellation must show the recovery requirement:\n{text}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn slack_app_token_cancellation_names_every_recovery_path() {
    let text = cancel_next_local_deploy_prompt(
        &[
            ("CURIE_CREDENTIALS", "sk-EXAMPLE-model-credential"),
            ("SLACK_BOT_TOKEN", "xoxb-EXAMPLE-bot-token"),
        ],
        &[b"\x1b"],
        "Slack app token",
    );
    for recovery in [
        "curie secrets set SLACK_APP_TOKEN",
        "export SLACK_APP_TOKEN=",
    ] {
        assert!(
            text.contains(recovery),
            "Slack app token cancellation must show {recovery:?}:\n{text}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn slack_app_token_cancel_choice_names_every_recovery_path() {
    let text = cancel_next_local_deploy_prompt(
        &[
            ("CURIE_CREDENTIALS", "sk-EXAMPLE-model-credential"),
            ("SLACK_BOT_TOKEN", "xoxb-EXAMPLE-bot-token"),
        ],
        &[b"\x1b[B", b"\r"],
        "Slack app token cancel choice",
    );
    for recovery in [
        "curie secrets set SLACK_APP_TOKEN",
        "export SLACK_APP_TOKEN=",
    ] {
        assert!(
            text.contains(recovery),
            "Slack app token cancel choice must show {recovery:?}:\n{text}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn slack_app_token_value_cancellation_names_every_recovery_path() {
    let text = cancel_next_local_deploy_prompt(
        &[
            ("CURIE_CREDENTIALS", "sk-EXAMPLE-model-credential"),
            ("SLACK_BOT_TOKEN", "xoxb-EXAMPLE-bot-token"),
        ],
        &[b"\r", b"\x1b"],
        "Slack app token value",
    );
    for recovery in [
        "curie secrets set SLACK_APP_TOKEN",
        "export SLACK_APP_TOKEN=",
    ] {
        assert!(
            text.contains(recovery),
            "Slack app token value cancellation must show {recovery:?}:\n{text}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn model_credential_value_cancellation_names_every_recovery_path() {
    let text = cancel_next_local_deploy_prompt(
        &[
            ("SLACK_APP_TOKEN", "xapp-EXAMPLE-app-token"),
            ("SLACK_BOT_TOKEN", "xoxb-EXAMPLE-bot-token"),
        ],
        &[b"\r", b"\x1b"],
        "model credential value",
    );
    for recovery in [
        "CURIE_MODEL_BASE_URL",
        "curie secrets set <NAME>",
        "export <NAME>=",
    ] {
        assert!(
            text.contains(recovery),
            "model credential value cancellation must show {recovery:?}:\n{text}"
        );
    }
}
