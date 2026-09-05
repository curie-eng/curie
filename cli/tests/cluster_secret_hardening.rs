//! Binary and public surface regressions for cluster secret lifecycle and
//! rendered Helm passthrough values.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use curie::ops::{up_commands, CmdArg, CommonOpts, GithubTokenPlan, OpsCommand, UpOpts};
use curie::ui::{CliOutput, DryRunPlan};

const TARGET_RELEASE: &str = "acme-release";
const TARGET_NAMESPACE: &str = "acme-namespace";

const SET_SECRET: &str = "ghp-SET-SENTINEL-1138";
const SET_MASKED: &str = "ghp-SET-***";
const SET_EXPRESSION: &str =
    "worker.replicas=2,connector.accessToken=ghp-SET-SENTINEL-1138,api.githubToken=,bare";
const MASKED_SET_EXPRESSION: &str =
    "worker.replicas=2,connector.accessToken=ghp-SET-***,api.githubToken=,bare";

const SET_STRING_SECRET: &str = "xoxb-STRING-SENTINEL-1138";
const SET_STRING_MASKED: &str = "xoxb-STR***";
const SET_STRING_EXPRESSION: &str =
    "worker.label=blue,connector.clientSecret=xoxb-STRING-SENTINEL-1138";
const MASKED_SET_STRING_EXPRESSION: &str = "worker.label=blue,connector.clientSecret=xoxb-STR***";

const ESCAPED_COMMA_EXPRESSION: &str = r"connector.accessToken=ghp-ESC\,CLEAR-COMMA-TAIL-1138";
const ESCAPED_COMMA_MASKED: &str = r"connector.accessToken=ghp-ESC\***";
const BRACE_LIST_EXPRESSION: &str =
    "connector.accessTokens={ghp-BRACE-FIRST-1138,ghp-BRACE-SECOND-1138}";
const BRACE_LIST_MASKED: &str = "connector.accessTokens={ghp-BRA***";
const SHORT_SECRET_EXPRESSION: &str = "connector.auth=abc";
const UNMATCHED_BRACE_GITHUB_SECRET: &str = "ghp-Y";
const UNMATCHED_BRACE_EXPRESSION: &str = "worker.note=literal{brace,api.githubToken=ghp-Y";
const UNCLOSED_LIST_GITHUB_SECRET: &str = "ghp-X";
const UNCLOSED_LIST_EXPRESSION: &str = "worker.note={literal,api.githubToken=ghp-X";

const MODEL_CREDENTIAL: &str = "sk-ant-PLACEHOLDER-SIGNAL-1137";
const GITHUB_CREDENTIAL: &str = "ghp-PLACEHOLDER-SIGNAL-1137";

const FIRST_FILE_WRITTEN_ENV: &str = "CURIE_TEST_SECRET_FIRST_FILE_WRITTEN";
const RESUME_WRITER_ENV: &str = "CURIE_TEST_SECRET_RESUME_WRITER";
const SIGNAL_CLEANED_ENV: &str = "CURIE_TEST_SECRET_SIGNAL_CLEANED";
const RESUME_SIGNAL_ENV: &str = "CURIE_TEST_SECRET_RESUME_SIGNAL";
const WRITER_PARKED_ENV: &str = "CURIE_TEST_SECRET_WRITER_PARKED";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn chart() -> &'static str {
    concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has a repository parent")
        .to_path_buf()
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let body = if matches!(name, "helm" | "kubectl") {
        format!(
            "#!/bin/sh\n{}\n{}",
            include_str!("data/converged-installation-read.sh"),
            body.strip_prefix("#!/bin/sh\n").unwrap_or(body)
        )
    } else {
        body.to_string()
    };
    let path = dir.join(name);
    fs::write(&path, body).unwrap_or_else(|error| panic!("write {name}: {error}"));
    let mut permissions = fs::metadata(&path)
        .unwrap_or_else(|error| panic!("read {name} metadata: {error}"))
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions)
        .unwrap_or_else(|error| panic!("make {name} executable: {error}"));
}

struct Fixture {
    temp: tempfile::TempDir,
    bin_dir: PathBuf,
    helm_log: PathBuf,
    installation: PathBuf,
    secret_tmp: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary directory");
        let bin_dir = temp.path().join("bin");
        let secret_tmp = temp.path().join("tmp");
        fs::create_dir(&bin_dir).expect("create fake binary directory");
        fs::create_dir(&secret_tmp).expect("create secret temporary directory");
        let helm_log = temp.path().join("helm.log");
        let installation = temp.path().join("curie.yaml");
        fs::write(
            &installation,
            format!(
                "version: 1\ninstall:\n  namespace: {TARGET_NAMESPACE}\n  release: {TARGET_RELEASE}\nset:\n  worker.label: \"blue,connector.clientSecret={SET_STRING_SECRET}\"\n"
            ),
        )
        .expect("write installation fixture");

        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "values" ]; then
    printf '%s\n' 'Error: release: not found' >&2
    exit 1
fi

if [ "$1" = "template" ]; then
    case " $* " in
        *" --show-only templates/priorityclass.yaml "*)
            printf '%s\n' 'Error: could not find template templates/priorityclass.yaml in chart' >&2
            exit 1
            ;;
    esac
    exit 0
fi

if [ "$1" = "upgrade" ] && [ "$2" = "--install" ]; then
    printf '%s\n' "$*" >> "$CURIE_TEST_HELM_LOG"
    exit 0
fi

printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        write_exec(
            &bin_dir,
            "kubectl",
            r#"#!/bin/sh
if [ "$1" = "get" ] && [ "$2" = "namespace" ]; then
    exit 0
fi

if [ "$1" = "get" ] && [ "$2" = "deployment" ] &&
   [ "$3" = "agent-sandbox-controller" ] && [ "$4" = "-n" ] &&
   [ "$5" = "agent-sandbox-system" ] && [ "$6" = "--ignore-not-found" ] &&
   [ "$7" = "-o" ] && [ "$8" = "json" ] && [ -z "$9" ]; then
    exit 0
fi

if [ "$1" = "get" ] && [ "$2" = "statefulset" ]; then
    printf '%s\n' '{"apiVersion":"v1","items":[],"kind":"List","metadata":{}}'
    exit 0
fi

printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        Self {
            temp,
            bin_dir,
            helm_log,
            installation,
            secret_tmp,
        }
    }

    fn path(&self) -> std::ffi::OsString {
        let mut paths = vec![self.bin_dir.clone()];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        std::env::join_paths(paths).expect("join PATH")
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new(bin());
        command
            .current_dir(repo_root())
            .env("PATH", self.path())
            .env("TMPDIR", &self.secret_tmp)
            .env("CURIE_TEST_HELM_LOG", &self.helm_log)
            .env("CI", "1")
            .env("TERM", "dumb")
            .env("NO_COLOR", "1")
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL");
        command
    }

    fn debug_cluster_up(&self) -> Output {
        self.base_command()
            .args([
                "--color",
                "never",
                "--debug",
                "cluster",
                "up",
                "--chart",
                chart(),
                "--namespace",
                TARGET_NAMESPACE,
                "--release",
                TARGET_RELEASE,
                "--dev",
                "--no-expose",
                "--fake-model",
                "--set",
                SET_EXPRESSION,
                "--set",
                ESCAPED_COMMA_EXPRESSION,
                "--set",
                BRACE_LIST_EXPRESSION,
                "--set",
                SHORT_SECRET_EXPRESSION,
            ])
            .output()
            .expect("run debug cluster up")
    }

    fn apply(&self, dry_run: bool, json: bool, debug: bool) -> Output {
        let mut command = self.base_command();
        command.args(["--color", "never"]);
        if json {
            command.arg("--json");
        }
        if debug {
            command.arg("--debug");
        }
        command.args([
            "apply",
            "--file",
            self.installation.to_str().expect("UTF 8 installation path"),
            "--chart",
            chart(),
        ]);
        if dry_run {
            command.arg("--dry-run");
        }
        command.output().expect("run installation apply")
    }

    fn signal_cluster_up(&self, signal_name: &str) -> SignalRun {
        let case_dir = self.temp.path().join(signal_name);
        fs::create_dir(&case_dir).expect("create signal coordination directory");
        let first_file_written = case_dir.join("first-file-written");
        let resume_writer = case_dir.join("resume-writer");
        let signal_cleaned = case_dir.join("signal-cleaned");
        let resume_signal = case_dir.join("resume-signal");
        let writer_parked = case_dir.join("writer-parked");

        let child = self
            .base_command()
            .args([
                "--color",
                "never",
                "cluster",
                "up",
                "--chart",
                chart(),
                "--namespace",
                TARGET_NAMESPACE,
                "--release",
                TARGET_RELEASE,
                "--dev",
                "--no-expose",
            ])
            .env("CURIE_CREDENTIALS", MODEL_CREDENTIAL)
            .env("CURIE_GITHUB_TOKEN", GITHUB_CREDENTIAL)
            .env(FIRST_FILE_WRITTEN_ENV, &first_file_written)
            .env(RESUME_WRITER_ENV, &resume_writer)
            .env(SIGNAL_CLEANED_ENV, &signal_cleaned)
            .env(RESUME_SIGNAL_ENV, &resume_signal)
            .env(WRITER_PARKED_ENV, &writer_parked)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start cluster up for signal test");

        SignalRun {
            child: ChildGuard { child },
            first_file_written,
            resume_writer,
            signal_cleaned,
            resume_signal,
            writer_parked,
        }
    }

    fn helm_log(&self) -> String {
        fs::read_to_string(&self.helm_log).unwrap_or_default()
    }

    fn secret_files(&self) -> Vec<PathBuf> {
        fs::read_dir(&self.secret_tmp)
            .expect("read secret temporary directory")
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("curie-helm-values-") && name.ends_with(".yaml")
                    })
            })
            .collect()
    }
}

struct ChildGuard {
    child: Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct SignalRun {
    child: ChildGuard,
    first_file_written: PathBuf,
    resume_writer: PathBuf,
    signal_cleaned: PathBuf,
    resume_signal: PathBuf,
    writer_parked: PathBuf,
}

fn wait_for_path(child: &mut Child, path: &Path, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if path.exists() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll cluster up") {
            panic!("cluster up exited as {status} before {label}");
        }
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_exit(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("poll cluster up exit") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "cluster up did not terminate after signal cleanup was released"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn send_signal(pid: u32, signal: i32) {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }

    let result = unsafe { kill(pid as i32, signal) };
    assert_eq!(result, 0, "send signal {signal} to process {pid}");
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_parser_edge_cases_are_masked(rendered: &str, surface: &str) {
    assert!(
        rendered.contains("connector.accessToken=") && rendered.contains("ghp-ESC"),
        "escaped comma credential disappeared from {surface}: {rendered}"
    );
    assert!(
        !rendered.contains("CLEAR-COMMA-TAIL-1138"),
        "escaped comma credential tail leaked through {surface}: {rendered}"
    );
    assert!(
        rendered.contains("connector.accessTokens=") && rendered.contains("ghp-BRA"),
        "brace list credential disappeared from {surface}: {rendered}"
    );
    assert!(
        !rendered.contains("ghp-BRACE-FIRST-1138") && !rendered.contains("ghp-BRACE-SECOND-1138"),
        "brace list credential member leaked through {surface}: {rendered}"
    );
    assert!(
        !rendered.contains(SHORT_SECRET_EXPRESSION),
        "short credential was reproduced in full through {surface}: {rendered}"
    );
    let short_value = rendered
        .split_once("connector.auth=")
        .map(|(_, value)| value)
        .unwrap_or_else(|| panic!("short credential disappeared from {surface}: {rendered}"));
    let mask_offset = short_value
        .find("***")
        .unwrap_or_else(|| panic!("short credential has no mask in {surface}: {rendered}"));
    assert!(
        mask_offset < "abc".len(),
        "short credential mask retained the complete value through {surface}: {rendered}"
    );
}

fn pure_up() -> UpOpts {
    UpOpts {
        retained_mail_values: None,
        common: CommonOpts {
            namespace: TARGET_NAMESPACE.to_string(),
            release: TARGET_RELEASE.to_string(),
            dry_run: true,
        },
        chart: chart().to_string(),
        no_expose: true,
        set: vec![
            SET_EXPRESSION.to_string(),
            ESCAPED_COMMA_EXPRESSION.to_string(),
            BRACE_LIST_EXPRESSION.to_string(),
            SHORT_SECRET_EXPRESSION.to_string(),
            UNMATCHED_BRACE_EXPRESSION.to_string(),
            UNCLOSED_LIST_EXPRESSION.to_string(),
        ],
        set_string: vec![SET_STRING_EXPRESSION.to_string()],
        allow_egress_host: vec![],
        resolved_egress_cidrs: vec![],
        allow_web_egress: vec![],
        fake_model: true,
        credentials: None,
        local_model: None,
        model: None,
        secrets: vec![],
        github_token: GithubTokenPlan::Untouched,
        dev: true,
    }
}

#[test]
fn set_passthrough_is_masked_in_every_rendered_form() {
    let commands = up_commands(&pure_up());
    let helm = &commands[0];

    let display = helm.display();
    assert!(
        display.contains(MASKED_SET_EXPRESSION),
        "direct display lost or exposed the set expression: {display}"
    );
    assert!(
        display.contains(MASKED_SET_STRING_EXPRESSION),
        "direct display lost or exposed the set string expression: {display}"
    );
    assert!(!display.contains(SET_SECRET), "set leak: {display}");
    assert!(
        !display.contains(SET_STRING_SECRET),
        "set string leak: {display}"
    );
    assert!(display.contains(SET_MASKED), "missing set mask: {display}");
    assert!(
        display.contains(SET_STRING_MASKED),
        "missing set string mask: {display}"
    );
    assert!(display.contains(ESCAPED_COMMA_MASKED), "{display}");
    assert!(display.contains(BRACE_LIST_MASKED), "{display}");
    assert_parser_edge_cases_are_masked(&display, "direct display");
    assert!(display.contains("worker.replicas=2"), "{display}");
    assert!(display.contains("api.githubToken="), "{display}");
    assert!(display.contains("bare"), "{display}");
    assert!(display.contains("worker.note="), "{display}");
    assert!(
        !display.contains(UNMATCHED_BRACE_GITHUB_SECRET),
        "an unmatched brace in an ordinary value hid a later credential from direct display masking: {display}"
    );
    assert!(
        !display.contains(UNCLOSED_LIST_GITHUB_SECRET),
        "an unclosed ordinary list exposed a later GitHub credential in direct display: {display}"
    );

    let argv = helm.argv();
    let set_index = argv
        .windows(2)
        .position(|pair| pair[0] == "--set" && pair[1] == SET_EXPRESSION)
        .expect("exact set flag and expression pair in argv");
    let set_string_index = argv
        .windows(2)
        .position(|pair| pair[0] == "--set-string" && pair[1] == SET_STRING_EXPRESSION)
        .expect("exact set string flag and expression pair in argv");
    assert_eq!(argv[set_index + 1], SET_EXPRESSION);
    assert_eq!(argv[set_string_index + 1], SET_STRING_EXPRESSION);
    for expression in [
        ESCAPED_COMMA_EXPRESSION,
        BRACE_LIST_EXPRESSION,
        SHORT_SECRET_EXPRESSION,
        UNMATCHED_BRACE_EXPRESSION,
        UNCLOSED_LIST_EXPRESSION,
    ] {
        assert!(
            argv.windows(2)
                .any(|pair| pair[0] == "--set" && pair[1] == expression),
            "executed set expression changed bytes: {expression:?} in {argv:?}"
        );
    }
    assert!(
        set_index < set_string_index,
        "set string precedence must remain after set: {argv:?}"
    );

    let plan = DryRunPlan {
        lines: commands.iter().map(|command| command.display()).collect(),
    };
    let human_plan = plan.lines.join("\n");
    assert!(human_plan.contains(MASKED_SET_EXPRESSION), "{human_plan}");
    assert!(
        human_plan.contains(MASKED_SET_STRING_EXPRESSION),
        "{human_plan}"
    );
    assert!(!human_plan.contains(SET_SECRET), "{human_plan}");
    assert!(!human_plan.contains(SET_STRING_SECRET), "{human_plan}");
    assert!(human_plan.contains(ESCAPED_COMMA_MASKED), "{human_plan}");
    assert!(human_plan.contains(BRACE_LIST_MASKED), "{human_plan}");
    assert_parser_edge_cases_are_masked(&human_plan, "human dry run plan");
    assert!(
        !human_plan.contains(UNMATCHED_BRACE_GITHUB_SECRET),
        "an unmatched brace exposed the later GitHub credential in the human plan: {human_plan}"
    );
    assert!(
        !human_plan.contains(UNCLOSED_LIST_GITHUB_SECRET),
        "an unclosed ordinary list exposed the later GitHub credential in the human plan: {human_plan}"
    );

    let json = plan.to_json().to_string();
    assert!(json.contains(MASKED_SET_EXPRESSION), "{json}");
    assert!(json.contains(MASKED_SET_STRING_EXPRESSION), "{json}");
    assert!(!json.contains(SET_SECRET), "{json}");
    assert!(!json.contains(SET_STRING_SECRET), "{json}");
    assert_parser_edge_cases_are_masked(&json, "dry run plan JSON");
    assert!(
        !json.contains(UNMATCHED_BRACE_GITHUB_SECRET),
        "an unmatched brace exposed the later GitHub credential in plan JSON: {json}"
    );
    assert!(
        !json.contains(UNCLOSED_LIST_GITHUB_SECRET),
        "an unclosed ordinary list exposed the later GitHub credential in plan JSON: {json}"
    );

    let human_output = Command::new(bin())
        .current_dir(repo_root())
        .args([
            "--color",
            "never",
            "cluster",
            "up",
            "--chart",
            chart(),
            "--dev",
            "--no-expose",
            "--fake-model",
            "--dry-run",
            "--set",
            SET_EXPRESSION,
            "--set",
            ESCAPED_COMMA_EXPRESSION,
            "--set",
            BRACE_LIST_EXPRESSION,
            "--set",
            SHORT_SECRET_EXPRESSION,
        ])
        .env("CI", "1")
        .env("TERM", "dumb")
        .env("NO_COLOR", "1")
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("CURIE_GITHUB_TOKEN")
        .env_remove("CURIE_MODEL")
        .output()
        .expect("run human dry run");
    assert!(
        human_output.status.success(),
        "human dry run failed: {}",
        output_text(&human_output)
    );
    let human_output = output_text(&human_output);
    assert!(
        human_output.contains(MASKED_SET_EXPRESSION),
        "{human_output}"
    );
    assert!(!human_output.contains(SET_SECRET), "{human_output}");
    assert_parser_edge_cases_are_masked(&human_output, "binary human dry run");

    let fixture = Fixture::new();
    let set_string_human_output = fixture.apply(true, false, false);
    assert!(
        set_string_human_output.status.success(),
        "set string human dry run failed: {}",
        output_text(&set_string_human_output)
    );
    let set_string_human_output = output_text(&set_string_human_output);
    assert!(
        set_string_human_output.contains("--set-string")
            && set_string_human_output.contains(MASKED_SET_STRING_EXPRESSION),
        "{set_string_human_output}"
    );
    assert!(
        !set_string_human_output.contains(SET_STRING_SECRET),
        "{set_string_human_output}"
    );

    let json_output = Command::new(bin())
        .current_dir(repo_root())
        .args([
            "--color",
            "never",
            "--json",
            "cluster",
            "up",
            "--chart",
            chart(),
            "--dev",
            "--no-expose",
            "--fake-model",
            "--dry-run",
            "--set",
            SET_EXPRESSION,
            "--set",
            ESCAPED_COMMA_EXPRESSION,
            "--set",
            BRACE_LIST_EXPRESSION,
            "--set",
            SHORT_SECRET_EXPRESSION,
        ])
        .env("CI", "1")
        .env("TERM", "dumb")
        .env("NO_COLOR", "1")
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env_remove("CURIE_GITHUB_TOKEN")
        .env_remove("CURIE_MODEL")
        .output()
        .expect("run JSON dry run");
    assert!(
        json_output.status.success(),
        "JSON dry run failed: {}",
        output_text(&json_output)
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&json_output.stdout).expect("dry run emits one JSON object");
    let json_output = parsed.to_string();
    assert!(json_output.contains(MASKED_SET_EXPRESSION), "{json_output}");
    assert!(!json_output.contains(SET_SECRET), "{json_output}");
    assert_parser_edge_cases_are_masked(&json_output, "binary dry run JSON");

    let set_string_json_output = fixture.apply(true, true, false);
    assert!(
        set_string_json_output.status.success(),
        "set string JSON dry run failed: {}",
        output_text(&set_string_json_output)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&set_string_json_output.stdout)
        .expect("set string dry run emits one JSON object");
    let set_string_json_output = parsed.to_string();
    assert!(
        set_string_json_output.contains("--set-string")
            && set_string_json_output.contains(MASKED_SET_STRING_EXPRESSION),
        "{set_string_json_output}"
    );
    assert!(
        !set_string_json_output.contains(SET_STRING_SECRET),
        "{set_string_json_output}"
    );

    let debug_output = fixture.debug_cluster_up();
    assert!(
        debug_output.status.success(),
        "debug cluster up failed: {}",
        output_text(&debug_output)
    );
    let debug_echo = String::from_utf8_lossy(&debug_output.stderr);
    assert!(debug_echo.contains(MASKED_SET_EXPRESSION), "{debug_echo}");
    assert!(!debug_echo.contains(SET_SECRET), "{debug_echo}");
    assert_parser_edge_cases_are_masked(&debug_echo, "binary debug echo");

    let set_string_debug_output = fixture.apply(false, false, true);
    assert!(
        set_string_debug_output.status.success(),
        "set string debug apply failed: {}",
        output_text(&set_string_debug_output)
    );
    let set_string_debug_echo = String::from_utf8_lossy(&set_string_debug_output.stderr);
    assert!(
        set_string_debug_echo.contains("--set-string")
            && set_string_debug_echo.contains(MASKED_SET_STRING_EXPRESSION),
        "{set_string_debug_echo}"
    );
    assert!(
        !set_string_debug_echo.contains(SET_STRING_SECRET),
        "{set_string_debug_echo}"
    );
    let helm_log = fixture.helm_log();
    assert!(
        helm_log.contains(&format!("--set {SET_EXPRESSION}")),
        "the executed set argv must retain the original bytes: {helm_log}"
    );
    assert!(
        helm_log.contains(&format!("--set-string {SET_STRING_EXPRESSION}")),
        "the executed set string argv must retain the original bytes: {helm_log}"
    );
}

#[test]
fn secret_values_file_short_value_does_not_reproduce_complete_secret() {
    let command = OpsCommand {
        program: "helm".to_string(),
        args: vec![CmdArg::SecretValuesFile(vec![(
            "api.githubToken".to_string(),
            "abc".to_string(),
        )])],
        env: vec![],
        secret_env: vec![],
    };

    let display = command.display();
    assert!(
        display.contains("<secret values file: api.githubToken="),
        "secret values file disappeared from display: {display}"
    );
    assert!(
        !display.contains("api.githubToken=abc"),
        "shared secret masking reproduced the complete three character value: {display}"
    );
    let shown = display
        .split_once("api.githubToken=")
        .map(|(_, value)| value)
        .expect("displayed secret values file key");
    let mask_offset = shown.find("***").expect("displayed secret mask");
    assert!(
        mask_offset < "abc".len(),
        "shared secret mask retained all three characters: {display}"
    );
}

#[test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "requires debug only signal coordination hooks"
)]
fn signal_interrupt_removes_every_secret_values_file() {
    for (signal_name, signal) in [("sigint", 2), ("sigterm", 15)] {
        let fixture = Fixture::new();
        let mut run = fixture.signal_cluster_up(signal_name);

        wait_for_path(
            &mut run.child.child,
            &run.first_file_written,
            "the first secret values file",
        );
        let first_files = fixture.secret_files();
        assert_eq!(
            first_files.len(),
            1,
            "the coordination point must pause after exactly one materialization: {first_files:?}"
        );
        let first_body = fs::read_to_string(&first_files[0]).expect("read first secret file");
        assert!(
            first_body.contains(MODEL_CREDENTIAL),
            "the first materialization must be the model credential: {first_body}"
        );

        send_signal(run.child.child.id(), signal);
        wait_for_path(
            &mut run.child.child,
            &run.signal_cleaned,
            "signal cleanup barrier",
        );
        assert!(
            fixture.secret_files().is_empty(),
            "{signal_name} left a cleartext values file after cleanup: {:?}",
            fixture.secret_files()
        );

        fs::write(&run.resume_writer, b"resume").expect("release the materializer");
        wait_for_path(
            &mut run.child.child,
            &run.writer_parked,
            "the racing secret writer to park",
        );
        assert!(
            run.child
                .child
                .try_wait()
                .expect("poll blocked materializer")
                .is_none(),
            "a writer racing {signal_name} returned normally instead of parking for signal death"
        );
        assert!(
            fixture.secret_files().is_empty(),
            "a writer racing {signal_name} created a second cleartext values file: {:?}",
            fixture.secret_files()
        );

        fs::write(&run.resume_signal, b"resume").expect("release signal termination");
        let status = wait_for_exit(&mut run.child.child);
        assert_eq!(
            status.signal(),
            Some(signal),
            "cluster up must retain the original {signal_name} termination: {status}"
        );
        assert!(
            fixture.secret_files().is_empty(),
            "{signal_name} left a cleartext values file after process death: {:?}",
            fixture.secret_files()
        );
    }
}
