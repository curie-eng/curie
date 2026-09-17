use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn fix_pin_ci_check() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CLI directory has a repository parent")
        .join("tools/fix-pin-ci/check.py")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn write_file(root: &Path, rel: &str, body: &str) {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().expect("file has a parent")).expect("create parent");
    fs::write(path, body).expect("write fixture file");
}

fn write_exec(root: &Path, name: &str, body: &str) {
    let path = root.join(name);
    fs::write(&path, body).expect("write executable");
    let mut permissions = fs::metadata(&path)
        .expect("executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make executable");
}

fn git(root: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .arg("-c")
        .arg("commit.gpgsign=false")
        .arg("-c")
        .arg("core.hooksPath=/dev/null")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git command failed\n{}",
        output_text(&output)
    );
    output
}

fn stdout_has_result(output: &Output, result: &str) -> bool {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.trim() == result)
}

struct Fixture {
    temp: tempfile::TempDir,
    root: PathBuf,
    tools: PathBuf,
}

impl Fixture {
    fn new(test_path: &str, test_body: &str) -> Self {
        let temp = tempfile::tempdir().expect("create temporary directory");
        let root = temp.path().join("repo");
        let tools = temp.path().join("tools");
        fs::create_dir_all(&root).expect("create repository");
        fs::create_dir_all(&tools).expect("create tools directory");

        write_file(&root, "runner/Dockerfile", "value=old\n");
        write_file(&root, test_path, test_body);
        let source_script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/verify-fix-pin.sh");
        let script = fs::read_to_string(source_script).expect("read checked in verify script");
        write_file(&root, "cli/scripts/verify-fix-pin.sh", &script);

        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.name", "Curie Test"]);
        git(&root, &["config", "user.email", "curie@example.com"]);
        git(&root, &["add", "."]);
        git(&root, &["commit", "-q", "-m", "Initial fixture"]);

        Self { temp, root, tools }
    }

    fn write(&self, rel: &str, body: &str) {
        write_file(&self.root, rel, body);
    }

    fn commit(&self, changes: &[(&str, &str)], message: &str) -> String {
        for (path, body) in changes {
            self.write(path, body);
        }
        git(&self.root, &["add", "."]);
        git(&self.root, &["commit", "-q", "-m", message]);
        self.head()
    }

    fn head(&self) -> String {
        String::from_utf8(git(&self.root, &["rev-parse", "HEAD"]).stdout)
            .expect("head is UTF 8")
            .trim()
            .to_owned()
    }

    fn patch(&self, base: &str, head: &str) -> Vec<u8> {
        git(&self.root, &["diff", base, head]).stdout
    }

    fn command(&self, change: &str, selector: &str) -> Command {
        let mut paths = vec![self.tools.clone()];
        if let Some(path) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&path));
        }
        let path = std::env::join_paths(paths).expect("join PATH");

        let mut command = Command::new(bin());
        command
            .current_dir(&self.root)
            .args(["dev", "verify-fix-pin", change, selector])
            .env("PATH", path);
        command
    }

    fn ci_gate_command(&self, event_path: &Path) -> Command {
        let mut command = Command::new("python3");
        command
            .current_dir(&self.root)
            .arg(fix_pin_ci_check())
            .args(["--event"])
            .arg(event_path)
            .args(["--curie", bin(), "--ref", "HEAD"]);
        command
    }

    fn assert_clean_and_single_worktree(&self) {
        let status = git(&self.root, &["status", "--porcelain"]);
        assert!(
            status.stdout.is_empty(),
            "the source repository changed\n{}",
            String::from_utf8_lossy(&status.stdout)
        );
        let worktrees = git(&self.root, &["worktree", "list", "--porcelain"]);
        let count = String::from_utf8_lossy(&worktrees.stdout)
            .lines()
            .filter(|line| line.starts_with("worktree "))
            .count();
        assert_eq!(count, 1, "the scratch worktree was not removed");
    }

    fn external_path(&self, name: &str) -> PathBuf {
        self.temp.path().join(name)
    }

    fn fix_pin_event(&self, selector: &str) -> PathBuf {
        let event = self.external_path("pull-request-event.json");
        fs::write(
            &event,
            format!(r#"{{"pull_request":{{"body":"Fix pin: {selector}\n"}}}}"#),
        )
        .expect("write pull request event");
        event
    }
}

const CHART_OLD: &str = r#"#!/bin/sh
set -eu
grep -qx 'value=old' runner/Dockerfile
"#;

const CHART_FIXED: &str = r#"#!/bin/sh
set -eu
grep -qx 'value=fixed' runner/Dockerfile
"#;

#[test]
fn commit_is_pinned_only_when_reversing_product_code_breaks_the_changed_chart_assertion() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/assert-pin.sh", CHART_FIXED),
        ],
        "Fix chart behavior",
    );

    let output = fixture
        .command(&change, "charts/curie/ci/assert-pin.sh")
        .output()
        .expect("run verify fix pin");
    assert!(
        output.status.success(),
        "a pinned change must succeed\n{}",
        output_text(&output)
    );
    assert!(
        stdout_has_result(&output, "PINNED"),
        "stdout must report PINNED\n{}",
        output_text(&output)
    );
    assert_eq!(
        fs::read_to_string(fixture.root.join("runner/Dockerfile")).expect("read source behavior"),
        "value=fixed\n"
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn commit_is_unpinned_when_the_changed_assertion_stays_green_after_product_reversal() {
    let fixture = Fixture::new(
        "charts/curie/ci/assert-pin.sh",
        "#!/bin/sh\nset -eu\ntest -s runner/Dockerfile\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "charts/curie/ci/assert-pin.sh",
                "#!/bin/sh\nset -eu\ntest -f runner/Dockerfile\n",
            ),
        ],
        "Add weak chart assertion",
    );

    let output = fixture
        .command(&change, "charts/curie/ci/assert-pin.sh")
        .output()
        .expect("run verify fix pin");
    assert!(
        !output.status.success(),
        "an unpinned change must fail\n{}",
        output_text(&output)
    );
    assert!(
        stdout_has_result(&output, "UNPINNED"),
        "stdout must report UNPINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn ci_gate_reports_pinned_when_the_declared_chart_assertion_breaks_after_reversal() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/assert-pin.sh", CHART_FIXED),
        ],
        "Fix chart behavior",
    );
    let event = fixture.fix_pin_event("charts/curie/ci/assert-pin.sh");

    let output = fixture
        .ci_gate_command(&event)
        .output()
        .expect("run fix pin CI gate");
    assert!(
        output.status.success(),
        "the declared pinned change must succeed through the CI gate\n{}",
        output_text(&output)
    );
    assert!(
        stdout_has_result(&output, "PINNED"),
        "the CI gate must preserve the verifier's PINNED result\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn ci_gate_reports_unpinned_when_the_declared_chart_assertion_stays_green_after_reversal() {
    let fixture = Fixture::new(
        "charts/curie/ci/assert-pin.sh",
        "#!/bin/sh\nset -eu\ntest -s runner/Dockerfile\n",
    );
    fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "charts/curie/ci/assert-pin.sh",
                "#!/bin/sh\nset -eu\ntest -f runner/Dockerfile\n",
            ),
        ],
        "Add weak chart assertion",
    );
    let event = fixture.fix_pin_event("charts/curie/ci/assert-pin.sh");

    let output = fixture
        .ci_gate_command(&event)
        .output()
        .expect("run fix pin CI gate");
    assert!(
        !output.status.success(),
        "the declared weak change must fail through the CI gate\n{}",
        output_text(&output)
    );
    assert!(
        stdout_has_result(&output, "UNPINNED"),
        "the CI gate must preserve the verifier's UNPINNED result\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn ci_gate_rejects_rust_test_flag_injection_before_it_can_report_pinned() {
    let fixture = Fixture::new(
        "cli/tests/verify_pin.rs",
        "use pin_fixture::old;\n\n#[test]\nfn pin_breaks() {\n    assert_eq!(old(), 1);\n}\n",
    );
    fixture.commit(
        &[
            (
                "cli/Cargo.toml",
                "[package]\nname = \"pin_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            ("cli/src/lib.rs", "pub fn old() -> i32 {\n    1\n}\n"),
        ],
        "Add Rust fixture",
    );
    fixture.commit(
        &[
            (
                "cli/src/lib.rs",
                "pub fn old() -> i32 {\n    1\n}\n\npub fn fixed() -> i32 {\n    2\n}\n",
            ),
            (
                "cli/tests/verify_pin.rs",
                "use pin_fixture::fixed;\n\n#[test]\nfn pin_breaks() {\n    assert_eq!(fixed(), 2);\n}\n",
            ),
        ],
        "Fix Rust behavior",
    );
    let event = fixture.fix_pin_event("cli/tests/verify_pin.rs::--no-run");

    let output = fixture
        .ci_gate_command(&event)
        .output()
        .expect("run fix pin CI gate");
    assert!(
        !output.status.success(),
        "a Rust test flag must be rejected instead of proving a pin by compilation alone\n{}",
        output_text(&output)
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "a Rust test flag must not let the CI gate report PINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn direct_verifier_rejects_rust_test_flag_before_cargo_can_report_pinned() {
    let fixture = Fixture::new("cli/tests/verify_pin.rs", "old assertion\n");
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("cli/tests/verify_pin.rs", "new assertion\n"),
        ],
        "Fix Rust behavior",
    );
    let cargo_log = fixture.external_path("cargo.log");
    write_exec(
        &fixture.tools,
        "cargo",
        r#"#!/bin/sh
printf 'called\n' >> "$VERIFY_CARGO_LOG"
grep -qx 'value=fixed' runner/Dockerfile
"#,
    );

    let output = fixture
        .command(&change, "cli/tests/verify_pin.rs::--no-run")
        .env("VERIFY_CARGO_LOG", &cargo_log)
        .output()
        .expect("run verify fix pin");
    assert!(
        !output.status.success(),
        "a Rust test flag must be rejected by the direct verifier\n{}",
        output_text(&output)
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "a Rust test flag must never report PINNED\n{}",
        output_text(&output)
    );
    assert!(
        !cargo_log.exists(),
        "a flag like Rust test name must be rejected before Cargo runs"
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn change_without_a_test_hunk_refuses_before_running_the_selector() {
    let fixture = Fixture::new(
        "charts/curie/ci/assert-pin.sh",
        "#!/bin/sh\nset -eu\nprintf 'run\\n' >> \"$VERIFY_RUN_LOG\"\n",
    );
    let change = fixture.commit(
        &[("runner/Dockerfile", "value=fixed\n")],
        "Change behavior only",
    );
    let run_log = fixture.external_path("run.log");

    let output = fixture
        .command(&change, "charts/curie/ci/assert-pin.sh")
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    let lower = shown.to_lowercase();
    assert!(
        !output.status.success(),
        "missing test hunks must fail\n{shown}"
    );
    assert!(
        lower.contains("no test") && lower.contains("file"),
        "the refusal must explain that the change has no test files\n{shown}"
    );
    assert!(!run_log.exists(), "the selector must not run");
    assert!(!stdout_has_result(&output, "PINNED"));
    assert!(!stdout_has_result(&output, "UNPINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn overlapping_later_edit_refuses_before_the_post_reversal_selector_run() {
    let fixture = Fixture::new(
        "charts/curie/ci/assert-pin.sh",
        "#!/bin/sh\nset -eu\nprintf 'run\\n' >> \"$VERIFY_RUN_LOG\"\ntest -s runner/Dockerfile\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "charts/curie/ci/assert-pin.sh",
                "#!/bin/sh\nset -eu\nprintf 'run\\n' >> \"$VERIFY_RUN_LOG\"\ngrep -q '^value=' runner/Dockerfile\n",
            ),
        ],
        "Fix behavior with assertion",
    );
    fixture.commit(
        &[("runner/Dockerfile", "value=later\n")],
        "Change the same behavior later",
    );
    let run_log = fixture.external_path("run.log");

    let output = fixture
        .command(&change, "charts/curie/ci/assert-pin.sh")
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "a reverse conflict must fail\n{shown}"
    );
    assert!(
        shown.contains("runner/Dockerfile") && shown.to_lowercase().contains("reverse"),
        "the refusal must name the path that cannot be reversed\n{shown}"
    );
    assert_eq!(
        fs::read_to_string(&run_log).expect("read selector log"),
        "run\n",
        "only the baseline selector may run"
    );
    assert!(!stdout_has_result(&output, "PINNED"));
    assert!(!stdout_has_result(&output, "UNPINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn red_baseline_refuses_before_reversing_product_code() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "charts/curie/ci/assert-pin.sh",
                "#!/bin/sh\nset -eu\nprintf 'run\\n' >> \"$VERIFY_RUN_LOG\"\ngrep -qx 'value=missing' runner/Dockerfile\n",
            ),
        ],
        "Add a red assertion",
    );
    let run_log = fixture.external_path("run.log");

    let output = fixture
        .command(&change, "charts/curie/ci/assert-pin.sh")
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "a red baseline must fail\n{shown}"
    );
    assert!(
        shown.to_lowercase().contains("baseline"),
        "the refusal must identify the red baseline\n{shown}"
    );
    assert_eq!(
        fs::read_to_string(run_log).expect("read selector log"),
        "run\n",
        "the red selector must run only once"
    );
    assert!(!stdout_has_result(&output, "PINNED"));
    assert!(!stdout_has_result(&output, "UNPINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn selector_not_changed_by_the_reference_refuses_before_running_it() {
    let fixture = Fixture::new(
        "charts/curie/ci/selected.sh",
        "#!/bin/sh\nset -eu\nprintf 'run\\n' >> \"$VERIFY_RUN_LOG\"\ngrep -qx 'value=fixed' runner/Dockerfile\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/other.sh", "#!/bin/sh\nset -eu\nexit 0\n"),
        ],
        "Change behavior with another assertion",
    );
    let run_log = fixture.external_path("run.log");

    let output = fixture
        .command(&change, "charts/curie/ci/selected.sh")
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "an unchanged selector must fail\n{shown}"
    );
    assert!(
        shown.to_lowercase().contains("selector") && shown.to_lowercase().contains("changed"),
        "the refusal must identify the unchanged selector\n{shown}"
    );
    assert!(!run_log.exists(), "the unchanged selector must not run");
    assert!(!stdout_has_result(&output, "PINNED"));
    assert!(!stdout_has_result(&output, "UNPINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn change_with_only_test_files_refuses_before_running_the_selector() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let change = fixture.commit(
        &[(
            "charts/curie/ci/assert-pin.sh",
            "#!/bin/sh\nset -eu\nprintf 'run\\n' >> \"$VERIFY_RUN_LOG\"\ngrep -qx 'value=old' runner/Dockerfile\n",
        )],
        "Change assertion only",
    );
    let run_log = fixture.external_path("run.log");

    let output = fixture
        .command(&change, "charts/curie/ci/assert-pin.sh")
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "a change without product files must fail\n{shown}"
    );
    assert!(
        shown.to_lowercase().contains("no product files"),
        "the refusal must identify the missing product files\n{shown}"
    );
    assert!(!run_log.exists(), "the selector must not run");
    assert!(!stdout_has_result(&output, "PINNED"));
    assert!(!stdout_has_result(&output, "UNPINNED"));
    fixture.assert_clean_and_single_worktree();
}

fn assert_tool_selector_route(
    test_path: &str,
    selector: &str,
    old_test_body: &str,
    new_test_body: &str,
    tool: &str,
    argv: &[&str],
    reversed_failure: &str,
) {
    let mut selector_parts = selector.split("::");
    let selector_file = selector_parts.next().expect("selector file");
    let selector_nodes: Vec<&str> = selector_parts.collect();
    let junit_name = selector_nodes.last().copied().unwrap_or("");
    let mut junit_classname = selector_file
        .strip_suffix(".py")
        .unwrap_or(selector_file)
        .replace('/', ".");
    if selector_nodes.len() > 1 {
        junit_classname.push('.');
        junit_classname.push_str(&selector_nodes[..selector_nodes.len() - 1].join("."));
    }
    let fixture = Fixture::new(test_path, old_test_body);
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (test_path, new_test_body),
        ],
        "Fix behavior with routed assertion",
    );
    let route_log = fixture.external_path("route.log");
    write_exec(
        &fixture.tools,
        tool,
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
{
    printf 'call\n'
    printf 'cwd=%s\n' "$PWD"
    printf 'argc=%s\n' "$#"
    for arg in "$@"; do
        printf 'arg=%s\n' "$arg"
    done
} >> "$VERIFY_ROUTE_LOG"
if [ "$6" = --junitxml ]; then
    printf '<testsuites><testsuite tests="1" failures="1" errors="0"><testcase classname="%s" name="%s"><failure message="assertion failed" type="AssertionError">assert 1 == 2</failure></testcase></testsuite></testsuites>\n' \
        "$VERIFY_JUNIT_CLASSNAME" "$VERIFY_JUNIT_NAME" > "$7"
fi
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
printf '%s\n' "$VERIFY_REVERSED_FAILURE"
exit 1
"#,
    );

    let output = fixture
        .command(&change, selector)
        .env("VERIFY_ROUTE_LOG", &route_log)
        .env("VERIFY_REVERSED_FAILURE", reversed_failure)
        .env("VERIFY_JUNIT_CLASSNAME", junit_classname)
        .env("VERIFY_JUNIT_NAME", junit_name)
        .output()
        .expect("run verify fix pin");
    assert!(
        output.status.success(),
        "the routed selector must prove the pin\n{}",
        output_text(&output)
    );
    assert!(stdout_has_result(&output, "PINNED"));

    let log = fs::read_to_string(route_log).expect("read route log");
    let calls: Vec<Vec<&str>> = log
        .split("call\n")
        .filter(|call| !call.is_empty())
        .map(|call| call.lines().collect())
        .collect();
    assert_eq!(
        calls.len(),
        2,
        "the selector must run before and after reversal"
    );
    let mut scratch_cwd: Option<&str> = None;
    for (call_index, call) in calls.into_iter().enumerate() {
        let cwd = call[0].strip_prefix("cwd=").expect("cwd record");
        assert_ne!(Path::new(cwd), fixture.root.as_path());
        if let Some(expected) = scratch_cwd {
            assert_eq!(cwd, expected, "both runs must use one scratch repository");
        } else {
            scratch_cwd = Some(cwd);
        }
        let actual: Vec<&str> = call[2..]
            .iter()
            .map(|line| line.strip_prefix("arg=").expect("argument record"))
            .collect();
        if tool == "uv" && call_index == 1 {
            assert_eq!(call[1], format!("argc={}", argv.len() + 2));
            assert_eq!(&actual[..argv.len()], argv);
            assert_eq!(actual[argv.len()], "--junitxml");
            let junit_path = Path::new(actual[argv.len() + 1]);
            assert!(
                junit_path.is_absolute(),
                "JUnit output path must be absolute"
            );
            assert_eq!(
                junit_path.file_name().and_then(|name| name.to_str()),
                Some("reversed.junit.xml")
            );
        } else {
            assert_eq!(call[1], format!("argc={}", argv.len()));
            assert_eq!(actual, argv);
        }
    }
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn python_app_selector_uses_uv_with_the_exact_root_command() {
    let selector = "apps/api/tests/test_pin.py::test_pin";
    assert_tool_selector_route(
        "apps/api/tests/test_pin.py",
        selector,
        "def test_pin():\n    assert 1 == 1\n",
        "def test_pin():\n    assert 2 == 2\n",
        "uv",
        &["run", "--python", "3.13", "pytest", selector],
        &format!("FAILED {selector} - AssertionError"),
    );
}

#[test]
fn python_package_selector_uses_uv_with_the_exact_root_command() {
    let selector = "packages/example/tests/test_pin.py::test_pin";
    assert_tool_selector_route(
        "packages/example/tests/test_pin.py",
        selector,
        "def test_pin():\n    assert 1 == 1\n",
        "def test_pin():\n    assert 2 == 2\n",
        "uv",
        &["run", "--python", "3.13", "pytest", selector],
        &format!("FAILED {selector} - AssertionError"),
    );
}

#[test]
fn python_runner_selector_uses_uv_with_the_exact_root_command() {
    let selector = "runner/tests/test_pin.py::test_pin";
    assert_tool_selector_route(
        "runner/tests/test_pin.py",
        selector,
        "def test_pin():\n    assert 1 == 1\n",
        "def test_pin():\n    assert 2 == 2\n",
        "uv",
        &["run", "--python", "3.13", "pytest", selector],
        &format!("FAILED {selector} - AssertionError"),
    );
}

#[test]
fn local_python_selector_uses_the_existing_python_verifier_command() {
    let selector = "cli/tests/local/test_pin.py::test_pin";
    assert_tool_selector_route(
        "cli/tests/local/test_pin.py",
        selector,
        "def test_pin():\n    assert 1 == 1\n",
        "def test_pin():\n    assert 2 == 2\n",
        "uv",
        &["run", "--python", "3.13", "pytest", selector],
        &format!("FAILED {selector} - AssertionError"),
    );
}

#[test]
fn local_python_selector_is_pinned_by_a_real_selected_assertion_failure() {
    let selector = "cli/tests/local/test_pin.py::test_selected";
    let fixture = Fixture::new(
        "cli/tests/local/test_pin.py",
        "from pin_fixture import value\n\n\ndef test_selected():\n    assert value() == 1\n",
    );
    fixture.commit(
        &[
            (
                "pyproject.toml",
                r#"[project]
name = "pin-fixture"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = []

[dependency-groups]
dev = ["pytest>=8.3"]

[tool.uv]
package = false

[tool.pytest.ini_options]
pythonpath = ["."]
"#,
            ),
            ("pin_fixture.py", "def value():\n    return 1\n"),
        ],
        "Add local Python fixture",
    );
    let change = fixture.commit(
        &[
            ("pin_fixture.py", "def value():\n    return 2\n"),
            (
                "cli/tests/local/test_pin.py",
                "from pin_fixture import value\n\n\ndef test_selected():\n    assert value() == 2\n",
            ),
        ],
        "Fix local Python behavior",
    );

    let output = fixture
        .command(&change, selector)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        output.status.success(),
        "the local selector must prove the pin\n{shown}"
    );
    assert!(stdout_has_result(&output, "PINNED"), "{shown}");
    assert!(
        shown.contains("1 failed"),
        "pytest must report the selected failure\n{shown}"
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn local_python_selector_reports_unpinned_when_reversal_stays_green() {
    let selector = "cli/tests/local/test_pin.py::test_selected";
    let fixture = Fixture::new(
        "cli/tests/local/test_pin.py",
        "def test_selected():\n    assert True\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "cli/tests/local/test_pin.py",
                "def test_selected():\n    assert 1 == 1\n",
            ),
        ],
        "Add weak local assertion",
    );
    let run_log = fixture.external_path("uv-runs.log");
    write_exec(
        &fixture.tools,
        "uv",
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
printf 'run\n' >> "$VERIFY_RUN_LOG"
exit 0
"#,
    );

    let output = fixture
        .command(&change, selector)
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "an unpinned local selector must fail\n{shown}"
    );
    assert!(stdout_has_result(&output, "UNPINNED"), "{shown}");
    assert_eq!(
        fs::read_to_string(run_log).expect("read uv run log"),
        "run\nrun\n",
        "both selector phases must run",
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn rust_selector_uses_cargo_with_an_exact_safe_test_name() {
    assert_tool_selector_route(
        "cli/tests/verify_pin.rs",
        "cli/tests/verify_pin.rs::pin_breaks",
        "#[test]\nfn pin_breaks() {\n    assert_eq!(1, 1);\n}\n",
        "#[test]\nfn pin_breaks() {\n    assert_eq!(2, 2);\n}\n",
        "cargo",
        &[
            "test",
            "--manifest-path",
            "cli/Cargo.toml",
            "--test",
            "verify_pin",
            "--",
            "--exact",
            "pin_breaks",
        ],
        "test pin_breaks ... FAILED",
    );
}

#[test]
fn one_line_rust_test_function_can_prove_a_pin() {
    assert_tool_selector_route(
        "cli/tests/verify_pin.rs",
        "cli/tests/verify_pin.rs::pin_breaks",
        "#[test]\nfn pin_breaks() { assert_eq!(1, 1); }\n",
        "#[test]\nfn pin_breaks() { assert_eq!(2, 2); }\n",
        "cargo",
        &[
            "test",
            "--manifest-path",
            "cli/Cargo.toml",
            "--test",
            "verify_pin",
            "--",
            "--exact",
            "pin_breaks",
        ],
        "test pin_breaks ... FAILED",
    );
}

fn assert_unchanged_selected_python_node_refuses(selector: &str) {
    let test_path = selector.split("::").next().expect("selector path");
    let fixture = Fixture::new(
        test_path,
        r#"def test_selected():
    assert True


def test_other():
    assert True
"#,
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                test_path,
                r#"def test_selected():
    assert True


def test_other():
    assert 1 == 1
"#,
            ),
        ],
        "Change only another Python test",
    );
    let run_log = fixture.external_path("uv-runs.log");
    write_exec(
        &fixture.tools,
        "uv",
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
printf 'run\n' >> "$VERIFY_RUN_LOG"
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
printf 'FAILED %s - AssertionError\n' "$5"
exit 1
"#,
    );

    let output = fixture
        .command(&change, selector)
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "an unchanged selected Python node must refuse a claimed selected failure\n{shown}"
    );
    assert!(
        shown.contains("selected Python test node was not changed"),
        "the refusal must identify the unchanged selected Python node\n{shown}"
    );
    assert_eq!(
        fs::read_to_string(run_log).expect("read uv run log"),
        "run\n",
        "only the baseline selector may run"
    );
    assert!(!stdout_has_result(&output, "PINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn unchanged_selected_python_node_refuses_a_claimed_selected_failure() {
    assert_unchanged_selected_python_node_refuses("apps/api/tests/test_pin.py::test_selected");
}

#[test]
fn unchanged_selected_local_python_node_refuses_a_claimed_failure() {
    assert_unchanged_selected_python_node_refuses("cli/tests/local/test_pin.py::test_selected");
}

#[test]
fn unchanged_selected_rust_node_refuses_a_claimed_selected_runtime_failure() {
    let fixture = Fixture::new(
        "cli/tests/verify_pin.rs",
        r#"#[test]
fn selected_pin() {
    assert_eq!(1, 1);
}

#[test]
fn other_pin() {
    assert_eq!(1, 1);
}
"#,
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "cli/tests/verify_pin.rs",
                r#"#[test]
fn selected_pin() {
    assert_eq!(1, 1);
}

#[test]
fn other_pin() {
    assert_eq!(2, 2);
}
"#,
            ),
        ],
        "Change only another Rust test",
    );
    let run_log = fixture.external_path("cargo-runs.log");
    write_exec(
        &fixture.tools,
        "cargo",
        r#"#!/bin/sh
printf 'run\n' >> "$VERIFY_RUN_LOG"
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
printf 'test selected_pin ... FAILED\n'
exit 1
"#,
    );

    let output = fixture
        .command(&change, "cli/tests/verify_pin.rs::selected_pin")
        .env("VERIFY_RUN_LOG", &run_log)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "an unchanged selected Rust node must refuse a claimed selected failure\n{shown}"
    );
    assert!(
        shown.contains("selected Rust test node was not changed"),
        "the refusal must identify the unchanged selected Rust node\n{shown}"
    );
    assert_eq!(
        fs::read_to_string(run_log).expect("read cargo run log"),
        "run\n",
        "only the baseline selector may run"
    );
    assert!(!stdout_has_result(&output, "PINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn changed_selected_python_node_refuses_an_unrelated_collection_failure() {
    let selector = "apps/api/tests/test_pin.py::test_selected";
    let fixture = Fixture::new(
        "apps/api/tests/test_pin.py",
        "def test_selected():\n    assert 1 == 1\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "apps/api/tests/test_pin.py",
                "def test_selected():\n    assert 2 == 2\n",
            ),
        ],
        "Change selected Python test",
    );
    write_exec(
        &fixture.tools,
        "uv",
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
printf 'ERROR collecting apps/api/tests/test_other.py\n' >&2
printf 'ImportError: unrelated collection failure\n' >&2
exit 2
"#,
    );

    let output = fixture
        .command(&change, selector)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "an unrelated collection failure must not pin the changed selected node\n{shown}"
    );
    assert!(
        shown.contains("reversed failure was not attributed"),
        "the refusal must distinguish collection from selected test failure\n{shown}"
    );
    assert!(!stdout_has_result(&output, "PINNED"));
    fixture.assert_clean_and_single_worktree();
}

fn assert_local_python_reversed_failure_refused(junit: &str, reversed_exit: &str) {
    let selector = "cli/tests/local/test_pin.py::test_selected";
    let fixture = Fixture::new(
        "cli/tests/local/test_pin.py",
        "def test_selected():\n    assert 1 == 1\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "cli/tests/local/test_pin.py",
                "def test_selected():\n    assert 2 == 2\n",
            ),
        ],
        "Change selected local Python test",
    );
    let run_log = fixture.external_path("uv-runs.log");
    write_exec(
        &fixture.tools,
        "uv",
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
printf 'run\n' >> "$VERIFY_RUN_LOG"
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
if [ -n "$VERIFY_JUNIT" ]; then
    printf '%s\n' "$VERIFY_JUNIT" > "$7"
fi
exit "$VERIFY_REVERSED_EXIT"
"#,
    );

    let output = fixture
        .command(&change, selector)
        .env("VERIFY_RUN_LOG", &run_log)
        .env("VERIFY_JUNIT", junit)
        .env("VERIFY_REVERSED_EXIT", reversed_exit)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "a nonassertion local failure must be refused\n{shown}",
    );
    assert!(
        shown.contains("reversed failure was not attributed"),
        "the refusal must identify failed attribution\n{shown}",
    );
    assert!(!stdout_has_result(&output, "PINNED"), "{shown}");
    assert_eq!(
        fs::read_to_string(run_log).expect("read uv run log"),
        "run\nrun\n",
        "both selector phases must run",
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn local_python_selector_refuses_a_setup_error() {
    assert_local_python_reversed_failure_refused(
        r#"<testsuites><testsuite tests="1" failures="0" errors="1"><testcase classname="cli.tests.local.test_pin" name="test_selected"><error message="setup failed" type="RuntimeError">setup failed</error></testcase></testsuite></testsuites>"#,
        "1",
    );
}

#[test]
fn local_python_selector_refuses_pytest_exit_five() {
    assert_local_python_reversed_failure_refused("", "5");
}

#[test]
fn forged_selected_python_failure_followed_by_unrelated_teardown_error_refuses() {
    let selector = "apps/api/tests/test_pin.py::test_selected";
    let fixture = Fixture::new(
        "apps/api/tests/test_pin.py",
        "def test_selected():\n    assert 1 == 1\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "apps/api/tests/test_pin.py",
                "def test_selected():\n    assert 2 == 2\n",
            ),
        ],
        "Change selected Python test",
    );
    let junit_copy = fixture.external_path("reversed.junit.xml");
    write_exec(
        &fixture.tools,
        "uv",
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
printf '%s\n' '<testsuites><testsuite tests="2" failures="1" errors="1"><testcase classname="apps.api.tests.test_pin" name="test_selected"><failure message="assertion failed" type="AssertionError">assert 1 == 2</failure></testcase><testcase classname="apps.api.tests.test_other" name="test_other"><error message="teardown failed" type="RuntimeError">unrelated teardown failure</error></testcase></testsuite></testsuites>' > "$7"
cp "$7" "$VERIFY_JUNIT_COPY"
printf 'FAILED %s - forged by selected test output\n' "$5"
printf 'ERROR at teardown of apps/api/tests/test_other.py::test_other\n' >&2
printf 'RuntimeError: unrelated teardown failure\n' >&2
exit 1
"#,
    );

    let output = fixture
        .command(&change, selector)
        .env("VERIFY_JUNIT_COPY", &junit_copy)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "forged selected output plus unrelated teardown must not prove a pin\n{shown}"
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "forged selected output must not report PINNED\n{shown}"
    );
    let junit = fs::read_to_string(junit_copy).expect("read copied JUnit report");
    assert!(
        junit.contains(r#"classname="apps.api.tests.test_pin" name="test_selected"><failure"#),
        "the JUnit report must contain the selected assertion failure"
    );
    assert!(
        junit.contains(r#"classname="apps.api.tests.test_other" name="test_other"><error"#),
        "the JUnit report must contain the unrelated teardown error"
    );
    fixture.assert_clean_and_single_worktree();
}

fn assert_python_junit_cardinality_refused(junit: &str) {
    let selector = "apps/api/tests/test_pin.py::test_selected";
    let fixture = Fixture::new(
        "apps/api/tests/test_pin.py",
        "def test_selected():\n    assert 1 == 1\n",
    );
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            (
                "apps/api/tests/test_pin.py",
                "def test_selected():\n    assert 2 == 2\n",
            ),
        ],
        "Change selected Python test",
    );
    write_exec(
        &fixture.tools,
        "uv",
        r#"#!/bin/sh
if [ "$1" = run ] && [ "$2" = --python ] && [ "$4" = python ]; then
    shift 4
    exec python3 "$@"
fi
if grep -qx 'value=fixed' runner/Dockerfile; then
    exit 0
fi
printf '%s\n' "$VERIFY_JUNIT" > "$7"
exit 1
"#,
    );

    let output = fixture
        .command(&change, selector)
        .env("VERIFY_JUNIT", junit)
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "invalid JUnit failure cardinality must be refused\n{shown}"
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "invalid JUnit failure cardinality must not report PINNED\n{shown}"
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn selected_python_test_cannot_use_another_testcase_failure_as_a_pin() {
    assert_python_junit_cardinality_refused(
        r#"<testsuites><testsuite tests="2" failures="1" errors="0"><testcase classname="apps.api.tests.test_pin" name="test_selected"/><testcase classname="apps.api.tests.test_pin" name="test_other"><failure message="assertion failed" type="AssertionError">assert 1 == 2</failure></testcase></testsuite></testsuites>"#,
    );
}

#[test]
fn selected_python_test_cannot_use_two_failure_elements_as_a_pin() {
    assert_python_junit_cardinality_refused(
        r#"<testsuites><testsuite tests="2" failures="2" errors="0"><testcase classname="apps.api.tests.test_pin" name="test_selected"><failure message="assertion failed" type="AssertionError">assert 1 == 2</failure></testcase><testcase classname="apps.api.tests.test_pin" name="test_other"><failure message="assertion failed" type="AssertionError">assert 2 == 3</failure></testcase></testsuite></testsuites>"#,
    );
}

#[test]
fn duplicate_selected_python_testcases_cannot_prove_a_pin() {
    assert_python_junit_cardinality_refused(
        r#"<testsuites><testsuite tests="2" failures="1" errors="0"><testcase classname="apps.api.tests.test_pin" name="test_selected"><failure message="assertion failed" type="AssertionError">assert 1 == 2</failure></testcase><testcase classname="apps.api.tests.test_pin" name="test_selected"/></testsuite></testsuites>"#,
    );
}

#[test]
fn unchanged_selected_python_test_cannot_use_collection_failure_as_a_pin() {
    let fixture = Fixture::new(
        "apps/api/tests/test_pin.py",
        r#"from apps.api.pin_fixture import old_value


def test_selected():
    assert old_value() == 1


def test_other():
    assert old_value() == 1
"#,
    );
    fixture.commit(
        &[
            (
                "pyproject.toml",
                r#"[project]
name = "pin-fixture"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = []

[dependency-groups]
dev = ["pytest>=8.3"]

[tool.uv]
package = false

[tool.pytest.ini_options]
pythonpath = ["."]
"#,
            ),
            ("apps/__init__.py", ""),
            ("apps/api/__init__.py", ""),
            (
                "apps/api/pin_fixture.py",
                "def old_value():\n    return 1\n",
            ),
        ],
        "Add Python fixture",
    );
    let change = fixture.commit(
        &[
            (
                "apps/api/pin_fixture.py",
                "def old_value():\n    return 1\n\n\ndef fixed_value():\n    return 2\n",
            ),
            (
                "apps/api/tests/test_pin.py",
                r#"from apps.api.pin_fixture import fixed_value, old_value


def test_selected():
    assert old_value() == 1


def test_other():
    assert fixed_value() == 2
"#,
            ),
        ],
        "Fix Python behavior",
    );

    let output = fixture
        .command(&change, "apps/api/tests/test_pin.py::test_selected")
        .output()
        .expect("run verify fix pin");
    assert!(
        !output.status.success(),
        "an unchanged selected test must not be pinned by collection failure\n{}",
        output_text(&output)
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "collection failure outside the unchanged selected test must not report PINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn changed_selected_rust_test_cannot_use_another_test_compile_error_as_a_pin() {
    let fixture = Fixture::new(
        "cli/tests/verify_pin.rs",
        r#"#[test]
fn selected_pin() {
    assert_eq!(pin_fixture::value(), 1);
}

#[test]
fn other_pin() {
    assert_eq!(pin_fixture::value(), 1);
}
"#,
    );
    fixture.commit(
        &[
            (
                "cli/Cargo.toml",
                "[package]\nname = \"pin_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            ("cli/src/lib.rs", "pub fn value() -> i32 {\n    1\n}\n"),
        ],
        "Add Rust fixture",
    );
    let change = fixture.commit(
        &[
            (
                "cli/src/lib.rs",
                "pub fn value() -> i32 {\n    1\n}\n\npub fn fixed() -> i32 {\n    2\n}\n",
            ),
            (
                "cli/tests/verify_pin.rs",
                r#"#[test]
fn selected_pin() {
    assert_eq!(pin_fixture::value() + 1, 2);
}

#[test]
fn other_pin() {
    assert_eq!(pin_fixture::fixed(), 2);
}
"#,
            ),
        ],
        "Fix Rust behavior",
    );

    let output = fixture
        .command(&change, "cli/tests/verify_pin.rs::selected_pin")
        .env(
            "CARGO_TARGET_DIR",
            fixture.external_path("other-test-cargo-target"),
        )
        .output()
        .expect("run verify fix pin");
    assert!(
        !output.status.success(),
        "another test compile error must not pin the selected test\n{}",
        output_text(&output)
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "another test compile error must not report PINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn changed_selected_rust_test_may_pin_through_its_own_new_api_compile_error() {
    let fixture = Fixture::new(
        "cli/tests/verify_pin.rs",
        r#"#[test]
fn selected_pin() {
    assert_eq!(pin_fixture::value(), 1);
}
"#,
    );
    fixture.commit(
        &[
            (
                "cli/Cargo.toml",
                "[package]\nname = \"pin_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            ("cli/src/lib.rs", "pub fn value() -> i32 {\n    1\n}\n"),
        ],
        "Add Rust fixture",
    );
    let change = fixture.commit(
        &[
            (
                "cli/src/lib.rs",
                "pub fn value() -> i32 {\n    1\n}\n\npub fn fixed() -> i32 {\n    2\n}\n",
            ),
            (
                "cli/tests/verify_pin.rs",
                r#"#[test]
fn selected_pin() {
    assert_eq!(pin_fixture::fixed(), 2);
}
"#,
            ),
        ],
        "Fix Rust behavior",
    );

    let output = fixture
        .command(&change, "cli/tests/verify_pin.rs::selected_pin")
        .env(
            "CARGO_TARGET_DIR",
            fixture.external_path("selected-test-cargo-target"),
        )
        .output()
        .expect("run verify fix pin");
    assert!(
        output.status.success(),
        "the selected test own new API compile error must prove the pin\n{}",
        output_text(&output)
    );
    assert!(
        stdout_has_result(&output, "PINNED"),
        "the selected test own new API compile error must report PINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn rust_open_brace_in_string_does_not_credit_a_later_unrelated_compile_error() {
    let fixture = Fixture::new(
        "cli/tests/verify_pin.rs",
        r#"#[test]
fn selected_pin() {
    let _brace = "{";
    assert_eq!(1, 1);
}

#[test]
fn other_pin() {
    assert_eq!(pin_fixture::value(), 1);
}
// }
"#,
    );
    fixture.commit(
        &[
            (
                "cli/Cargo.toml",
                "[package]\nname = \"pin_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            ("cli/src/lib.rs", "pub fn value() -> i32 {\n    1\n}\n"),
        ],
        "Add Rust fixture",
    );
    let change = fixture.commit(
        &[
            (
                "cli/src/lib.rs",
                "pub fn value() -> i32 {\n    1\n}\n\npub fn fixed() -> i32 {\n    2\n}\n",
            ),
            (
                "cli/tests/verify_pin.rs",
                r#"#[test]
fn selected_pin() {
    let _brace = "{";
    assert_eq!(2, 2);
}

#[test]
fn other_pin() {
    assert_eq!(pin_fixture::fixed(), 2);
}
// }
"#,
            ),
        ],
        "Change selected assertion and unrelated Rust behavior",
    );

    let output = fixture
        .command(&change, "cli/tests/verify_pin.rs::selected_pin")
        .env(
            "CARGO_TARGET_DIR",
            fixture.external_path("open-brace-cargo-target"),
        )
        .output()
        .expect("run verify fix pin");
    assert!(
        !output.status.success(),
        "an opening brace in a string must not extend the selected node to an unrelated error\n{}",
        output_text(&output)
    );
    assert!(
        !stdout_has_result(&output, "PINNED"),
        "the unrelated compile error must not report PINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn rust_closing_brace_char_does_not_truncate_the_changed_selected_node() {
    let fixture = Fixture::new(
        "cli/tests/verify_pin.rs",
        r#"#[test]
fn selected_pin() {
    let _brace = '}';
    assert_eq!(pin_fixture::value(), 1);
}
"#,
    );
    fixture.commit(
        &[
            (
                "cli/Cargo.toml",
                "[package]\nname = \"pin_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
            ),
            ("cli/src/lib.rs", "pub fn value() -> i32 {\n    1\n}\n"),
        ],
        "Add Rust fixture",
    );
    let change = fixture.commit(
        &[
            (
                "cli/src/lib.rs",
                "pub fn value() -> i32 {\n    1\n}\n\npub fn fixed() -> i32 {\n    2\n}\n",
            ),
            (
                "cli/tests/verify_pin.rs",
                r#"#[test]
fn selected_pin() {
    let _brace = '}';
    assert_eq!(pin_fixture::fixed(), 2);
}
"#,
            ),
        ],
        "Fix Rust behavior",
    );

    let output = fixture
        .command(&change, "cli/tests/verify_pin.rs::selected_pin")
        .env(
            "CARGO_TARGET_DIR",
            fixture.external_path("closing-brace-cargo-target"),
        )
        .output()
        .expect("run verify fix pin");
    assert!(
        output.status.success(),
        "a closing brace character must not truncate the changed selected node\n{}",
        output_text(&output)
    );
    assert!(
        stdout_has_result(&output, "PINNED"),
        "the selected compile failure after the brace character must report PINNED\n{}",
        output_text(&output)
    );
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn unsupported_selector_path_is_refused() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/assert-pin.sh", CHART_FIXED),
        ],
        "Fix chart behavior",
    );

    let output = fixture
        .command(&change, "tests/assert-pin.sh")
        .output()
        .expect("run verify fix pin");
    let shown = output_text(&output);
    assert!(
        !output.status.success(),
        "an unsupported selector must fail\n{shown}"
    );
    assert!(
        shown.to_lowercase().contains("selector"),
        "the refusal must identify the selector\n{shown}"
    );
    assert!(!stdout_has_result(&output, "PINNED"));
    assert!(!stdout_has_result(&output, "UNPINNED"));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn local_python_selector_grammar_rejects_other_cli_python_paths() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let change = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/assert-pin.sh", CHART_FIXED),
        ],
        "Fix chart behavior",
    );

    for selector in [
        "cli/tests/test_pin.py::test_pin",
        "cli/tests/local/nested/test_pin.py::test_pin",
        "cli/tests/local/../test_pin.py::test_pin",
        "cli/tests/local/pin.py::test_pin",
        "cli/tests/local/test_pin.py::helper",
    ] {
        let output = fixture
            .command(&change, selector)
            .output()
            .expect("run verify fix pin");
        let shown = output_text(&output);
        assert!(
            !output.status.success(),
            "an out of grammar local selector must fail: {selector}\n{shown}",
        );
        assert!(
            shown.contains("unsupported selector"),
            "the refusal must identify the unsupported selector: {selector}\n{shown}",
        );
        assert!(!stdout_has_result(&output, "PINNED"));
    }
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn pr_url_uses_the_resolved_patch_and_reports_the_same_pinned_result() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let base = fixture.head();
    let head = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/assert-pin.sh", CHART_FIXED),
        ],
        "Fix chart behavior",
    );
    let patch_path = fixture.external_path("pr.patch");
    fs::write(&patch_path, fixture.patch(&base, &head)).expect("write PR patch");
    let gh_log = fixture.external_path("gh.log");
    write_exec(
        &fixture.tools,
        "gh",
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$VERIFY_GH_LOG"
if [ "$1" = pr ] && [ "$2" = view ]; then
    exit 0
fi
if [ "$1" = pr ] && [ "$2" = diff ]; then
    # The GitHub CLI manual says --patch requests patch format. This verifier needs the default combined diff so one reverse apply covers later edits: https://cli.github.com/manual/gh_pr_diff
    case "$*" in
        *--patch*) exit 65 ;;
    esac
    case "$*" in
        *--name-only*)
            printf '%s\n' 'charts/curie/ci/assert-pin.sh' 'runner/Dockerfile'
            ;;
        *) cat "$VERIFY_GH_PATCH" ;;
    esac
    exit 0
fi
exit 64
"#,
    );

    let output = fixture
        .command(
            "https://github.com/curie-eng/curie/pull/1479",
            "charts/curie/ci/assert-pin.sh",
        )
        .env("VERIFY_GH_PATCH", &patch_path)
        .env("VERIFY_GH_LOG", &gh_log)
        .output()
        .expect("run verify fix pin for PR");
    assert!(
        output.status.success(),
        "the PR patch must prove the pin\n{}",
        output_text(&output)
    );
    assert!(stdout_has_result(&output, "PINNED"));
    let calls = fs::read_to_string(gh_log).expect("read gh calls");
    assert!(calls.lines().any(|line| line.starts_with("pr view ")));
    assert!(calls.lines().any(|line| line.starts_with("pr diff ")));
    fixture.assert_clean_and_single_worktree();
}

#[test]
fn numeric_pr_uses_gh_even_when_git_would_resolve_the_number_as_a_commit() {
    let fixture = Fixture::new("charts/curie/ci/assert-pin.sh", CHART_OLD);
    let base = fixture.head();
    let head = fixture.commit(
        &[
            ("runner/Dockerfile", "value=fixed\n"),
            ("charts/curie/ci/assert-pin.sh", CHART_FIXED),
        ],
        "Fix chart behavior",
    );
    let patch_path = fixture.external_path("pr.patch");
    fs::write(&patch_path, fixture.patch(&base, &head)).expect("write PR patch");
    let gh_log = fixture.external_path("gh.log");
    let git_log = fixture.external_path("git.log");
    write_exec(
        &fixture.tools,
        "gh",
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$VERIFY_GH_LOG"
if [ "$1" = pr ] && [ "$2" = view ]; then
    exit 0
fi
if [ "$1" = pr ] && [ "$2" = diff ]; then
    # The GitHub CLI manual says --patch requests patch format. This verifier needs the default combined diff so one reverse apply covers later edits: https://cli.github.com/manual/gh_pr_diff
    case "$*" in
        *--patch*) exit 65 ;;
    esac
    case "$*" in
        *--name-only*)
            printf '%s\n' 'charts/curie/ci/assert-pin.sh' 'runner/Dockerfile'
            ;;
        *) cat "$VERIFY_GH_PATCH" ;;
    esac
    exit 0
fi
exit 64
"#,
    );
    write_exec(
        &fixture.tools,
        "git",
        r#"#!/bin/sh
if [ "$1" = -C ] && [ "$3" = rev-parse ] && [ "$4" = --verify ] && [ "$5" = '1479^{commit}' ]; then
    printf 'numeric probe\n' >> "$VERIFY_GIT_LOG"
    printf '%s\n' "$VERIFY_COLLIDING_COMMIT"
    exit 0
fi
command -p git "$@"
"#,
    );

    let output = fixture
        .command("1479", "charts/curie/ci/assert-pin.sh")
        .env("VERIFY_COLLIDING_COMMIT", &head)
        .env("VERIFY_GH_PATCH", &patch_path)
        .env("VERIFY_GH_LOG", &gh_log)
        .env("VERIFY_GIT_LOG", &git_log)
        .output()
        .expect("run verify fix pin for numeric PR");
    assert!(
        output.status.success(),
        "the numeric PR patch must prove the pin\n{}",
        output_text(&output)
    );
    assert!(stdout_has_result(&output, "PINNED"));
    let calls = fs::read_to_string(gh_log).expect("read gh calls");
    assert!(calls.lines().any(|line| line.starts_with("pr view 1479")));
    assert!(calls.lines().any(|line| line.starts_with("pr diff 1479")));
    assert!(
        !git_log.exists(),
        "a numeric PR must not be resolved as a Git commit"
    );
    fixture.assert_clean_and_single_worktree();
}
