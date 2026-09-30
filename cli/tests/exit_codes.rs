//! Unit-level coverage for the semantic exit-code + error-classification
//! contract (ADR-0021 decision 1, AC 2 and AC 4). These reference the
//! yet-to-exist `curie::exit` module, so the file will not compile until the
//! implementer adds it; that red state is the contract handoff.

use curie::exit::{self, CliError, ExitClass};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

const SEAL_VALUE: &str = "placeholder-seal-value";
const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";
const INVALID_API_URL: &str = "localhost:8000";
const INVALID_RUNNER_URL: &str = "localhost:8787";
const DOCKER_FAILURE_SENTINEL: &str = "curie-1655-docker-command-failure";

fn run_seal(connector: &str, json: bool) -> Output {
    let keypair = curie::sealing::generate_keypair();
    let connector_arg = format!("--connector={connector}");
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if json {
        command.arg("--json");
    }
    command
        .args([
            "seal",
            &connector_arg,
            "GRAFANA_TOKEN",
            "--public-key",
            &keypair.public_key,
            "--from-env",
            "CURIE_TEST_SEAL_VALUE",
        ])
        .env("CURIE_TEST_SEAL_VALUE", SEAL_VALUE)
        .output()
        .expect("run curie seal")
}

fn run_unreachable_local_deploy(debug: bool) -> Output {
    let dir = tempfile::tempdir().expect("create plugin directory");
    curie::scaffold::scaffold(dir.path(), "test-agent").expect("scaffold plugin");
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if debug {
        command.arg("--debug");
    }
    command
        .args(["local", "deploy", "--api-url", UNREACHABLE_API_URL])
        .current_dir(dir.path())
        .output()
        .expect("run unreachable local deploy")
}

fn run_unreachable_local_versions(debug: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if debug {
        command.arg("--debug");
    }
    command
        .args([
            "local",
            "versions",
            "acme-agent",
            "--api-url",
            UNREACHABLE_API_URL,
        ])
        .output()
        .expect("run unreachable local versions")
}

fn run_invalid_local_versions(debug: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if debug {
        command.arg("--debug");
    }
    command
        .args([
            "local",
            "versions",
            "acme-agent",
            "--api-url",
            INVALID_API_URL,
        ])
        .output()
        .expect("run local versions with an invalid API URL")
}

fn run_invalid_runner_message(debug: bool) -> Output {
    let dir = tempfile::tempdir().expect("create runner state directory");
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if debug {
        command.arg("--debug");
    }
    command
        .args(["skill", "message", "hello", "--url", INVALID_RUNNER_URL])
        .current_dir(dir.path())
        .env("CURIE_CONFIG_DIR", dir.path().join("config"))
        .output()
        .expect("run skill message with an invalid runner URL")
}

fn run_skill_up_with_path(debug: bool, path: &Path, name: &str) -> Output {
    let dir = tempfile::tempdir().expect("create plugin directory");
    curie::scaffold::scaffold(dir.path(), "acme-agent").expect("scaffold plugin");
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if debug {
        command.arg("--debug");
    }
    command
        .args(["skill", "up", "--fake-model", "--name", name])
        .current_dir(dir.path())
        .env("CURIE_CONFIG_DIR", dir.path().join("config"))
        .env("PATH", path)
        .output()
        .expect("run skill up")
}

fn run_installation_command(
    cwd: &Path,
    verb: &str,
    file: &Path,
    json: bool,
    debug: bool,
) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    if json {
        command.arg("--json");
    }
    if debug {
        command.arg("--debug");
    }
    command.arg(verb);
    if verb == "apply" {
        command.arg("--dry-run");
    }
    command
        .arg("--file")
        .arg(file)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|err| panic!("run curie {verb}: {err}"))
}

fn json_error(output: &Output, label: &str) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        panic!("{label} must emit one JSON error: {err}\nstdout: {stdout}\nstderr: {stderr}")
    })
}

fn executable_docker_stub() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create executable directory");
    let docker = dir.path().join("docker");
    fs::write(
        &docker,
        format!("#!/bin/sh\nprintf '%s\\n' '{DOCKER_FAILURE_SENTINEL}' >&2\nexit 37\n"),
    )
    .expect("write docker stub");
    let mut permissions = fs::metadata(&docker)
        .expect("read docker stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&docker, permissions).expect("make docker stub executable");
    dir
}

fn executable_docker_lost_race_stub(container_name: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create executable directory");
    let docker = dir.path().join("docker");
    fs::write(
        &docker,
        format!(
            "#!/bin/sh\n\
             case \"$1\" in\n\
               ps) exit 0 ;;\n\
               run) printf '%s\\n' 'docker: Error response from daemon: Conflict. The container name \"/{container_name}\" is already in use by container \"9f2c\".' >&2; exit 125 ;;\n\
               *) printf '%s\\n' 'unexpected docker command' >&2; exit 93 ;;\n\
             esac\n"
        ),
    )
    .expect("write Docker race stub");
    let mut permissions = fs::metadata(&docker)
        .expect("read Docker race stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&docker, permissions).expect("make Docker race stub executable");
    dir
}

fn single_line_with_prefix<'a>(output: &'a str, prefix: &str) -> &'a str {
    let mut matching = output.lines().filter(|line| line.starts_with(prefix));
    let line = matching
        .next()
        .unwrap_or_else(|| panic!("output must contain one {prefix:?} line: {output}"));
    assert!(
        matching.next().is_none(),
        "output must not repeat its {prefix:?} line: {output}"
    );
    line
}

#[test]
fn exit_class_codes_are_stable() {
    assert_eq!(ExitClass::Success.code(), 0);
    assert_eq!(ExitClass::Failure.code(), 1);
    assert_eq!(ExitClass::Usage.code(), 2);
    assert_eq!(ExitClass::Transient.code(), 3);
}

#[test]
fn classify_usage_error_is_usage_class() {
    let err = exit::usage("bad");
    let (class, _fix) = exit::classify(&err);
    assert_eq!(class, ExitClass::Usage);
}

#[test]
fn classify_transient_error_is_transient_with_fix() {
    let err = exit::transient("net");
    let (class, fix) = exit::classify(&err);
    assert_eq!(class, ExitClass::Transient);
    assert!(fix.is_some(), "transient errors carry a retry hint");
}

#[test]
fn classify_plain_anyhow_is_failure_with_no_fix() {
    let err = anyhow::anyhow!("boom");
    let (class, fix) = exit::classify(&err);
    assert_eq!(class, ExitClass::Failure);
    assert_eq!(fix, None);
}

#[test]
fn untyped_layered_error_presents_outer_context_and_keeps_debug_chain() {
    let err = anyhow::anyhow!("raw transport detail").context("loading operator configuration");

    let (message, fix) = exit::present_error(&err);
    assert_eq!(message, "loading operator configuration");
    assert_eq!(fix, None);
    assert!(
        !message.contains("raw transport detail"),
        "normal presentation must hide the inner cause: {message}"
    );

    let debug = format!("{err:#}");
    assert!(
        debug.contains("loading operator configuration") && debug.contains("raw transport detail"),
        "debug formatting must retain the complete cause chain: {debug}"
    );
}

#[test]
fn classify_finds_clierror_through_context_wrapping() {
    // A tagged CliError buried under an anyhow context layer must still be
    // discovered by walking the error chain: class + fix survive wrapping.
    let base: anyhow::Error = CliError::usage("nope").with_fix("do X").into();
    let wrapped = base.context("outer");
    let (class, fix) = exit::classify(&wrapped);
    assert_eq!(class, ExitClass::Usage);
    assert_eq!(fix.as_deref(), Some("do X"));
}

#[test]
fn error_json_carries_message_and_fix() {
    let err: anyhow::Error = CliError::usage("nope").with_fix("do X").into();
    let value = exit::error_json(&err);
    assert_eq!(value["error"], "nope");
    assert_eq!(value["fix"], "do X");
}

#[test]
fn error_json_fix_is_null_for_plain_error() {
    let err = anyhow::anyhow!("kaboom");
    let value = exit::error_json(&err);
    assert_eq!(value["error"], "kaboom");
    assert!(value["fix"].is_null());
}

#[test]
fn apply_and_diff_file_errors_name_the_problem_and_give_a_runnable_fix() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let missing = temp.path().join("missing install.yaml");
    let unknown = temp.path().join("unknown install.yaml");
    fs::write(
        &unknown,
        "version: 1\ninstall:\n  namespace: x\n  release: x\n  cluster: y\n",
    )
    .expect("write installation with an unknown key");

    for (case, file) in [("missing", &missing), ("unknown", &unknown)] {
        let quoted_file = format!("'{}'", file.display());
        for verb in ["apply", "diff"] {
            for json in [false, true] {
                let label = format!(
                    "{verb} {case} file in {} mode",
                    if json { "JSON" } else { "human" }
                );
                let output = run_installation_command(temp.path(), verb, file, json, false);
                assert_eq!(
                    output.status.code(),
                    Some(ExitClass::Usage.code()),
                    "{label} must be a deterministic input error"
                );

                let (message, fix) = if json {
                    let payload = json_error(&output, &label);
                    (
                        payload["error"]
                            .as_str()
                            .unwrap_or_else(|| panic!("{label} must carry an error: {payload}"))
                            .to_string(),
                        payload["fix"]
                            .as_str()
                            .unwrap_or_else(|| panic!("{label} must carry a fix: {payload}"))
                            .to_string(),
                    )
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    (
                        single_line_with_prefix(&stderr, "Error: ")
                            .trim_start_matches("Error: ")
                            .to_string(),
                        single_line_with_prefix(&stderr, "Fix: ")
                            .trim_start_matches("Fix: ")
                            .to_string(),
                    )
                };

                assert!(
                    message.contains(&file.display().to_string()),
                    "{label} must name the selected file: {message}"
                );
                assert!(
                    fix.contains(&quoted_file),
                    "{label} must shell quote the selected file in its runnable fix: {fix}"
                );
                assert!(!fix.trim().is_empty(), "{label} must carry a usable fix");

                match case {
                    "missing" => {
                        assert!(
                            message.contains("not found"),
                            "{label} must distinguish a missing file: {message}"
                        );
                        assert!(
                            !message.contains("No such file or directory")
                                && !message.contains("os error"),
                            "{label} must hide the raw operating system error: {message}"
                        );
                    }
                    "unknown" => {
                        assert!(
                            message.contains("cluster")
                                && message.contains("line 5")
                                && message.contains("column 3"),
                            "{label} must name the key and exact YAML location: {message}"
                        );
                        assert_eq!(
                            message.matches("cluster").count(),
                            1,
                            "{label} must not duplicate the parser cause: {message}"
                        );
                        assert!(
                            message.matches(": ").count() <= 1,
                            "{label} must render one operator sentence: {message}"
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[test]
fn installation_file_debug_output_retains_read_and_parse_causes() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let missing = temp.path().join("missing.yaml");
    let unknown = temp.path().join("unknown.yaml");
    fs::write(
        &unknown,
        "version: 1\ninstall:\n  namespace: x\n  release: x\n  cluster: y\n",
    )
    .expect("write installation with an unknown key");

    let missing_output = run_installation_command(temp.path(), "apply", &missing, false, true);
    let missing_stderr = String::from_utf8_lossy(&missing_output.stderr);
    assert!(
        missing_stderr.contains("No such file or directory"),
        "debug output must retain the read cause: {missing_stderr}"
    );

    let unknown_output = run_installation_command(temp.path(), "diff", &unknown, false, true);
    let unknown_stderr = String::from_utf8_lossy(&unknown_output.stderr);
    assert!(
        unknown_stderr.contains("unknown field `cluster`")
            && unknown_stderr.contains("line 5 column 3"),
        "debug output must retain the serde cause: {unknown_stderr}"
    );
}

#[test]
fn malformed_installation_yaml_names_its_file_and_source_location() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let file = temp.path().join("malformed install.yaml");
    fs::write(
        &file,
        "version: 1\ninstall:\n  namespace: \"unterminated\n  release: x\n",
    )
    .expect("write malformed installation");

    let output = run_installation_command(temp.path(), "apply", &file, true, false);
    assert_eq!(output.status.code(), Some(ExitClass::Usage.code()));
    let payload = json_error(&output, "malformed apply");
    let message = payload["error"].as_str().expect("malformed error message");
    let fix = payload["fix"].as_str().expect("malformed error fix");
    assert!(
        message.contains(&file.display().to_string())
            && message.contains("line ")
            && message.contains("column "),
        "malformed YAML must name its file and parser location: {message}"
    );
    assert!(
        fix.contains(&format!("'{}'", file.display())),
        "malformed YAML must give a shell safe edit command: {fix}"
    );
}

#[test]
fn wrong_typed_installation_value_preserves_the_parser_key() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let file = temp.path().join("wrong type.yaml");
    fs::write(
        &file,
        "version: 1\ninstall:\n  namespace: [x]\n  release: x\n",
    )
    .expect("write installation with wrong value type");

    let output = run_installation_command(temp.path(), "diff", &file, false, false);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(ExitClass::Usage.code()));
    let message = single_line_with_prefix(&stderr, "Error: ");
    assert!(
        message.contains(&file.display().to_string())
            && message.contains("install.namespace")
            && message.contains("line ")
            && message.contains("column "),
        "a typed parser error must retain its field path and location: {stderr}"
    );
    single_line_with_prefix(&stderr, "Fix: ");
}

#[test]
fn a_directory_is_reported_as_an_unreadable_installation_file() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let file = temp.path().join("directory instead of file");
    fs::create_dir(&file).expect("create directory at installation path");

    let output = run_installation_command(temp.path(), "diff", &file, false, false);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(ExitClass::Failure.code()));
    let message = single_line_with_prefix(&stderr, "Error: ");
    let fix = single_line_with_prefix(&stderr, "Fix: ");
    assert!(
        message.contains("could not be read") && message.contains(&file.display().to_string()),
        "a nonfile path must receive an accurate read diagnosis: {stderr}"
    );
    assert!(
        !message.contains("Is a directory") && !message.contains("os error"),
        "normal output must hide the raw read cause: {stderr}"
    );
    assert!(
        fix.contains(&format!("'{}'", file.display())),
        "the read remedy must shell quote the selected path: {stderr}"
    );

    let json_output = run_installation_command(temp.path(), "diff", &file, true, false);
    assert_eq!(json_output.status.code(), Some(ExitClass::Failure.code()));
    let payload = json_error(&json_output, "unreadable directory");
    let json_fix = payload["fix"]
        .as_str()
        .expect("an unreadable path must carry a JSON fix");
    assert!(
        json_fix.contains(&format!("'{}'", file.display())),
        "the JSON read remedy must shell quote the selected path: {payload}"
    );
}

#[test]
fn installation_load_still_accepts_a_valid_file() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let file = temp.path().join("valid.yaml");
    fs::write(
        &file,
        "version: 1\ninstall:\n  namespace: x\n  release: x\n",
    )
    .expect("write valid installation");

    let loaded = curie::installation::Installation::load(&file)
        .expect("a valid installation file must still load");
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.install.namespace, "x");
    assert_eq!(loaded.install.release, "x");
}

#[test]
fn installation_load_preserves_typed_validation_class_and_fix() {
    let temp = tempfile::tempdir().expect("create installation directory");
    let file = temp.path().join("typed validation.yaml");
    fs::write(
        &file,
        "version: 1\ninstall:\n  namespace: x\n  release: x\nplatform:\n  inference: true\n",
    )
    .expect("write semantically invalid installation");

    let err = curie::installation::Installation::load(&file)
        .expect_err("an incomplete inference policy must fail validation");
    let (class, fix) = exit::classify(&err);
    let (message, presented_fix) = exit::present_error(&err);
    assert_eq!(class, ExitClass::Usage);
    assert_eq!(fix, presented_fix);
    let fix = fix.expect("typed validation must retain its actionable fix");
    for choice in ["inference_persistence: true", "inference_pull_model: false"] {
        assert!(
            message.contains(choice) || fix.contains(choice),
            "typed validation must retain recovery choice {choice}: {message}; {fix}"
        );
    }
    assert!(
        !message.contains("line ") && !message.contains("column "),
        "semantic validation must not invent a parser location: {message}"
    );
}

#[test]
fn local_deploy_keeps_connection_causes_out_of_the_human_error() {
    let human = run_unreachable_local_deploy(false);
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Transient.code()),
        "unreachable local deploy must fail: {human_stderr}"
    );
    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("Check that --api-url points to a reachable API"),
        "the human error must retain the custom API remedy: {human_stderr}"
    );
    assert!(
        !error_line.contains("curie local up"),
        "a custom API failure must not tell the user to start the local stack: {human_stderr}"
    );
    assert_eq!(
        error_line.matches(UNREACHABLE_API_URL).count(),
        1,
        "the specific local deploy error must name the API URL once: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("GET /agents")
            && !human_stderr.contains("error sending request for url")
            && !human_stderr.contains("os error"),
        "the human error must not expose the raw connection cause: {human_stderr}"
    );

    let debug = run_unreachable_local_deploy(true);
    let debug_stderr = String::from_utf8_lossy(&debug.stderr);
    assert_eq!(
        debug.status.code(),
        Some(ExitClass::Transient.code()),
        "debug must preserve the semantic exit code: {debug_stderr}"
    );
    assert!(
        debug_stderr.contains("error sending request for url"),
        "debug plumbing must retain the raw connection cause: {debug_stderr}"
    );
}

#[test]
fn local_versions_unreachable_has_operator_message_and_runnable_remedy() {
    let human = run_unreachable_local_versions(false);
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Transient.code()),
        "an unreachable API must be transient: {human_stderr}"
    );

    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("the platform API") && error_line.contains("is unreachable"),
        "the error must name the failed operator surface: {human_stderr}"
    );
    assert_eq!(
        error_line.matches(UNREACHABLE_API_URL).count(),
        1,
        "the error line must name the configured API once: {human_stderr}"
    );

    let remedy = format!("curl -fsS {UNREACHABLE_API_URL}/health");
    let fix_line = single_line_with_prefix(&human_stderr, "Fix: ");
    assert!(
        fix_line.contains(&remedy),
        "the remedy must contain a runnable health probe: {human_stderr}"
    );
    assert_eq!(
        fix_line.matches(UNREACHABLE_API_URL).count(),
        1,
        "the remedy line must name the configured API once: {human_stderr}"
    );
    assert_eq!(
        human_stderr.matches(UNREACHABLE_API_URL).count(),
        2,
        "normal output may name the API once in each relevant line: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("GET /agents")
            && !human_stderr.contains("error sending request for url")
            && !human_stderr.contains("os error"),
        "normal output must hide the transport chain: {human_stderr}"
    );

    let debug = run_unreachable_local_versions(true);
    let debug_stderr = String::from_utf8_lossy(&debug.stderr);
    assert_eq!(
        debug.status.code(),
        Some(ExitClass::Transient.code()),
        "debug must preserve the semantic exit code: {debug_stderr}"
    );
    assert!(
        debug_stderr.contains("GET /agents")
            || debug_stderr.contains("error sending request for url"),
        "debug plumbing must disclose the transport source: {debug_stderr}"
    );
}

#[test]
fn local_versions_invalid_url_has_operator_message_and_hides_builder_detail() {
    let human = run_invalid_local_versions(false);
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Failure.code()),
        "an invalid API URL must be a failure: {human_stderr}"
    );

    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("platform API") && error_line.contains(INVALID_API_URL),
        "the error must name the API surface and configured URL: {human_stderr}"
    );
    let fix_line = single_line_with_prefix(&human_stderr, "Fix: ");
    assert!(
        fix_line.len() > "Fix: ".len(),
        "the invalid URL must carry an operator remedy: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("GET /agents")
            && !human_stderr.contains("builder error")
            && !human_stderr.contains("relative URL without a base"),
        "normal output must hide request builder detail: {human_stderr}"
    );

    let debug = run_invalid_local_versions(true);
    let debug_stderr = String::from_utf8_lossy(&debug.stderr);
    assert_eq!(
        debug.status.code(),
        Some(ExitClass::Failure.code()),
        "debug must preserve the semantic exit code: {debug_stderr}"
    );
    assert!(
        debug_stderr.contains("GET /agents")
            && debug_stderr.contains(
                "builder error for url (localhost:8000/agents): URL scheme is not allowed"
            ),
        "debug plumbing must retain the request builder cause: {debug_stderr}"
    );
}

#[test]
fn skill_message_invalid_url_has_runner_message_and_absolute_url_remedy() {
    let human = run_invalid_runner_message(false);
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Failure.code()),
        "an invalid runner URL must be a failure: {human_stderr}"
    );

    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("runner") && error_line.contains(INVALID_RUNNER_URL),
        "the error must name the runner surface and configured URL: {human_stderr}"
    );
    let fix_line = single_line_with_prefix(&human_stderr, "Fix: ");
    assert!(
        fix_line.contains("http://localhost:8787") || fix_line.contains("https://localhost:8787"),
        "the remedy must provide an absolute runner URL: {human_stderr}"
    );
    assert!(
        !fix_line.contains("curie skill status"),
        "an invalid URL must not suggest probing that same URL: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("POST localhost:8787/v1/event")
            && !human_stderr.contains("builder error")
            && !human_stderr.contains("relative URL without a base"),
        "normal output must hide runner request detail: {human_stderr}"
    );

    let debug = run_invalid_runner_message(true);
    let debug_stderr = String::from_utf8_lossy(&debug.stderr);
    assert_eq!(
        debug.status.code(),
        Some(ExitClass::Failure.code()),
        "debug must preserve the semantic exit code: {debug_stderr}"
    );
    assert!(
        debug_stderr.contains(
            "POST localhost:8787/v1/reset: builder error for url \
             (localhost:8787/v1/reset): URL scheme is not allowed"
        ),
        "debug plumbing must retain the runner request cause: {debug_stderr}"
    );
}

#[test]
fn skill_up_without_docker_has_operator_message_and_runnable_remedy() {
    let empty_path = tempfile::tempdir().expect("create empty executable directory");
    let human = run_skill_up_with_path(
        false,
        empty_path.path(),
        "acme-1655-docker-unavailable-human",
    );
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Failure.code()),
        "an unavailable Docker binary must be a failure: {human_stderr}"
    );
    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("Docker"),
        "the error must name Docker in operator language: {human_stderr}"
    );
    let fix_line = single_line_with_prefix(&human_stderr, "Fix: ");
    assert!(
        fix_line.contains("docker info"),
        "the remedy must contain a runnable Docker probe: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("failed to invoke docker")
            && !human_stderr.contains("No such file or directory")
            && !human_stderr.contains("os error"),
        "normal output must hide the process spawn source: {human_stderr}"
    );

    let debug = run_skill_up_with_path(
        true,
        empty_path.path(),
        "acme-1655-docker-unavailable-debug",
    );
    let debug_stderr = String::from_utf8_lossy(&debug.stderr);
    assert_eq!(
        debug.status.code(),
        Some(ExitClass::Failure.code()),
        "debug must preserve the semantic exit code: {debug_stderr}"
    );
    assert!(
        debug_stderr.contains("failed to invoke docker"),
        "debug plumbing must disclose the process spawn source: {debug_stderr}"
    );
}

#[test]
fn skill_up_docker_command_failure_keeps_the_command_diagnosis() {
    let stub = executable_docker_stub();
    let human = run_skill_up_with_path(false, stub.path(), "acme-1655-docker-command-human");
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Failure.code()),
        "a nonzero Docker command must be a failure: {human_stderr}"
    );
    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("docker ps failed") && error_line.contains(DOCKER_FAILURE_SENTINEL),
        "the error must retain Docker's command diagnosis: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("docker info")
            && !human_stderr.lines().any(|line| line.starts_with("Fix: ")),
        "a nonzero Docker command must not suggest a daemon probe: {human_stderr}"
    );

    let debug = run_skill_up_with_path(true, stub.path(), "acme-1655-docker-command-debug");
    let debug_stderr = String::from_utf8_lossy(&debug.stderr);
    assert_eq!(
        debug.status.code(),
        Some(ExitClass::Failure.code()),
        "debug must preserve the semantic exit code: {debug_stderr}"
    );
    assert!(
        debug_stderr.contains(DOCKER_FAILURE_SENTINEL),
        "debug plumbing must disclose Docker stderr: {debug_stderr}"
    );
}

#[test]
fn skill_up_lost_name_race_keeps_the_specialized_replacement_remedy() {
    let container_name = "acme-1655-docker-name-race";
    let stub = executable_docker_lost_race_stub(container_name);
    let human = run_skill_up_with_path(false, stub.path(), container_name);
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Usage.code()),
        "a lost container name race must be a usage error: {human_stderr}"
    );

    let error_line = single_line_with_prefix(&human_stderr, "Error: ");
    assert!(
        error_line.contains("container name conflict")
            && error_line.contains("--replace")
            && error_line.contains(&format!("curie skill down --name {container_name}")),
        "the race must retain its specific replacement remedies: {human_stderr}"
    );
    assert!(
        !human_stderr.contains("already in use by container")
            && !human_stderr.contains("exit status: 125")
            && !human_stderr.contains("docker info"),
        "the race remedy must replace the raw Docker diagnosis: {human_stderr}"
    );
}

#[test]
fn seal_rejects_invalid_connector_names_as_usage_with_a_fix() {
    let invalid = [
        "Grafana".to_string(),
        "graf ana".to_string(),
        "graf_ana".to_string(),
        "-grafana".to_string(),
        "grafana-".to_string(),
        "a".repeat(41),
    ];

    for connector in invalid {
        let output = run_seal(&connector, true);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(ExitClass::Usage.code()),
            "{connector:?} must be rejected as Usage\nstdout: {stdout}\nstderr: {stderr}"
        );
        let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|err| panic!("{connector:?} must emit JSON: {err}; {stdout}"));
        assert!(
            payload["fix"].as_str().is_some_and(|fix| !fix.is_empty()),
            "{connector:?} must include actionable paste guidance: {payload}"
        );
        assert!(
            !stdout.contains(SEAL_VALUE) && !stderr.contains(SEAL_VALUE),
            "a rejected connector must not expose the value"
        );
    }
}

#[test]
fn seal_accepts_the_connector_name_length_boundary_and_interior_dash() {
    for connector in ["a".repeat(40), "grafana-cloud".to_string()] {
        let output = run_seal(&connector, true);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(ExitClass::Success.code()),
            "{connector:?} must be accepted\nstdout: {stdout}\nstderr: {stderr}"
        );
        let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|err| panic!("{connector:?} must emit JSON: {err}; {stdout}"));
        assert_eq!(payload["connector"].as_str(), Some(connector.as_str()));
        assert!(
            !stdout.contains(SEAL_VALUE) && !stderr.contains(SEAL_VALUE),
            "a sealed value must not expose its plaintext"
        );
    }
}

#[test]
fn seal_human_output_warns_before_pasting_rejected_sealed_secrets() {
    let human = run_seal("grafana", false);
    let human_stdout = String::from_utf8_lossy(&human.stdout);
    let human_stderr = String::from_utf8_lossy(&human.stderr);
    let human_output = format!("{human_stdout}{human_stderr}");
    assert_eq!(
        human.status.code(),
        Some(ExitClass::Success.code()),
        "human seal must succeed\nstdout: {human_stdout}\nstderr: {human_stderr}"
    );
    assert!(
        human_stdout.contains("sealed_secrets"),
        "human seal must retain the paste block:\n{human_stdout}"
    );
    assert!(
        human_output.contains("connector validation")
            && human_output.contains("sealed_secrets")
            && human_output.contains("#1434")
            && human_output.contains("until"),
        "human seal must warn that connector validation rejects sealed_secrets until #1434 lands:\n{human_output}"
    );
    let human_output_lower = human_output.to_ascii_lowercase();
    assert!(
        !human_output_lower.contains("merge") && !human_output_lower.contains("paste"),
        "human seal must locate the block without telling the user to merge or paste it:\n{human_output}"
    );

    let json = run_seal("grafana", true);
    let json_stdout = String::from_utf8_lossy(&json.stdout);
    let json_stderr = String::from_utf8_lossy(&json.stderr);
    assert_eq!(
        json.status.code(),
        Some(ExitClass::Success.code()),
        "JSON seal must succeed\nstdout: {json_stdout}\nstderr: {json_stderr}"
    );
    let payload: serde_json::Value = serde_json::from_slice(&json.stdout)
        .unwrap_or_else(|err| panic!("JSON seal must emit only JSON: {err}; {json_stdout}"));
    let fields = payload.as_object().expect("JSON seal must emit an object");
    assert_eq!(
        fields
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        ["connector", "env_name", "sealed", "public_key", "yaml"]
            .into_iter()
            .collect(),
        "JSON seal must retain its exact machine readable shape"
    );
    assert!(
        !json_stdout.contains("#1434") && !json_stderr.contains("#1434"),
        "the human warning must not appear in JSON mode:\nstdout: {json_stdout}\nstderr: {json_stderr}"
    );
}
