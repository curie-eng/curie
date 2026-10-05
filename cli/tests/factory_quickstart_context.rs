//! Factory quickstart must not install into an arbitrary current kube context.
//!
//! Kind contexts use the `kind-` prefix this command already treats as a kind
//! target. A current context outside that prefix needs a terminal confirmation.
//! Without a terminal the refusal names `--context <name>`. The command tests
//! below spawn the real `curie` binary. Stand-in `helm` and `kind` programs
//! record that they ran and exit, so a missing confirmation cannot hide behind
//! a planner test and cannot reach a real cluster.

use std::fs;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use curie::exit::{classify, ExitClass};
use curie::factory_quickstart::{
    describe, quickstart_plan, PlanInput, DEFAULT_BUDGET_USD, DEFAULT_DEADLINE_SECONDS,
    DEFAULT_KIND_NAME, DEFAULT_MODEL,
};

fn plan_input() -> PlanInput {
    PlanInput {
        repo: "acme/widgets".into(),
        app_id: None,
        private_key_file: None,
        explicit_context: None,
        current_context: None,
        kind_name: DEFAULT_KIND_NAME.into(),
        existing_kind_clusters: Vec::new(),
        namespace: "curie".into(),
        release: "curie".into(),
        model: DEFAULT_MODEL.into(),
        org: None,
        execution_deadline_seconds: DEFAULT_DEADLINE_SECONDS,
        budget_usd: DEFAULT_BUDGET_USD,
        chart: "charts/curie".into(),
        credential_in_env: false,
        release_has_real_model: false,
        interactive: false,
        release_at_target: false,
    }
}

fn refusal_names_context(name: &str) {
    let mut input = plan_input();
    input.current_context = Some(name.into());
    input.interactive = false;
    let err = quickstart_plan(&input).expect_err("a non-kind current context needs a terminal");
    let shown = format!("{err:#}");
    assert!(
        shown.contains(&format!("--context {name}")),
        "the refusal must name --context {name}: {shown}"
    );
    let (class, fix) = classify(&err);
    assert_eq!(class, ExitClass::Usage);
    let fix = fix.expect("fix");
    assert!(
        fix.contains(&format!("--context {name}")),
        "the fix must name --context {name}: {fix}"
    );
}

#[test]
fn a_non_kind_current_context_is_refused_without_a_terminal() {
    refusal_names_context("work-cluster");
}

#[test]
fn a_kind_current_context_is_not_refused_without_a_terminal() {
    let mut input = plan_input();
    input.current_context = Some("kind-curie-factory".into());
    let planned = quickstart_plan(&input).expect("kind context proceeds");
    let text = describe(&planned).join("\n");
    assert!(text.contains("kind context"), "{text}");
    assert!(!text.contains("must confirm"), "{text}");
}

#[test]
fn no_current_context_is_not_refused_without_a_terminal() {
    let planned = quickstart_plan(&plan_input()).expect("no context proceeds");
    let text = describe(&planned).join("\n");
    assert!(
        text.contains("kind create cluster --name curie-factory"),
        "{text}"
    );
    assert!(text.contains("no current context"), "{text}");
    assert!(!text.contains("must confirm"), "{text}");
}

#[test]
fn an_explicit_context_is_not_refused_without_a_terminal() {
    let mut input = plan_input();
    input.explicit_context = Some("work-cluster".into());
    input.current_context = Some("other-cluster".into());
    let planned = quickstart_plan(&input).expect("explicit context never prompts");
    let text = describe(&planned).join("\n");
    assert!(text.contains("--context work-cluster"), "{text}");
    assert!(text.contains("because --context was passed"), "{text}");
    assert!(!text.contains("must confirm"), "{text}");
    assert!(!text.contains("kind "), "{text}");
}

#[test]
fn a_non_kind_current_context_is_not_refused_when_interactive() {
    let mut input = plan_input();
    input.current_context = Some("work-cluster".into());
    input.interactive = true;
    let planned = quickstart_plan(&input).expect("a terminal may confirm");
    let text = describe(&planned).join("\n");
    assert!(text.contains("--context work-cluster"), "{text}");
    assert!(text.contains("must confirm"), "{text}");
    assert!(!text.contains("kind create"), "{text}");
}

#[test]
fn dry_run_plan_states_why_each_context_is_used() {
    let mut remote = plan_input();
    remote.current_context = Some("work-cluster".into());
    remote.interactive = true;
    let remote_text = describe(&quickstart_plan(&remote).unwrap()).join("\n");
    assert!(
        remote_text.contains(
            "Kubernetes context: work-cluster because it is the current context and it is not a kind context"
        ),
        "{remote_text}"
    );

    let mut kind = plan_input();
    kind.current_context = Some("kind-acme".into());
    let kind_text = describe(&quickstart_plan(&kind).unwrap()).join("\n");
    assert!(
        kind_text.contains(
            "Kubernetes context: kind-acme because the current context is a kind context"
        ),
        "{kind_text}"
    );

    let created = describe(&quickstart_plan(&plan_input()).unwrap()).join("\n");
    assert!(
        created.contains(
            "Kubernetes context: kind-curie-factory because no current context is set, so a kind cluster is created"
        ),
        "{created}"
    );

    let mut explicit = plan_input();
    explicit.explicit_context = Some("chosen".into());
    let explicit_text = describe(&quickstart_plan(&explicit).unwrap()).join("\n");
    assert!(
        explicit_text.contains("Kubernetes context: chosen because --context was passed"),
        "{explicit_text}"
    );

    let mut reused = plan_input();
    reused.existing_kind_clusters = vec!["curie-factory".into()];
    let reused_text = describe(&quickstart_plan(&reused).unwrap()).join("\n");
    assert!(
        reused_text.contains("kind cluster curie-factory already exists"),
        "{reused_text}"
    );
    assert!(!reused_text.contains("is created"), "{reused_text}");
}

struct ToolDir {
    root: PathBuf,
    log: PathBuf,
}

impl ToolDir {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "curie-quickstart-context-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let log = root.join("tool.log");
        fs::write(&log, "").unwrap();
        for name in ["helm", "kubectl", "kind", "docker"] {
            let path = bin.join(name);
            fs::write(
                &path,
                format!("#!/bin/sh\nprintf '%s\\n' \"{name}\" >> \"$CURIE_TOOL_LOG\"\nexit 1\n"),
            )
            .unwrap();
            let mut perms = fs::metadata(&path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&path, perms).unwrap();
        }
        Self { root, log }
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for ToolDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct KubeDir(PathBuf);

impl Drop for KubeDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn kubeconfig(current: &str) -> (KubeDir, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "curie-quickstart-kube-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config");
    let current_line = if current.is_empty() {
        "current-context: \"\"".to_string()
    } else {
        format!("current-context: {current}")
    };
    fs::write(
        &path,
        format!(
            "apiVersion: v1\nkind: Config\n{current_line}\ncontexts:\n- name: work-cluster\n  context:\n    cluster: work\n    user: work\n- name: kind-curie-factory\n  context:\n    cluster: kind\n    user: kind\n- name: chosen\n  context:\n    cluster: chosen\n    user: chosen\nclusters:\n- name: work\n  cluster:\n    server: https://127.0.0.1:9\n- name: kind\n  cluster:\n    server: https://127.0.0.1:9\n- name: chosen\n  cluster:\n    server: https://127.0.0.1:9\nusers:\n- name: work\n  user: {{}}\n- name: kind\n  user: {{}}\n- name: chosen\n  user: {{}}\n"
        ),
    )
    .unwrap();
    (KubeDir(dir), path)
}

fn chart() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../charts/curie")
}

fn command(tools: &ToolDir, kubeconfig: &PathBuf, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .args(["factory", "quickstart", "--repo", "acme/widgets", "--chart"])
        .arg(chart())
        .args(args)
        .env("KUBECONFIG", kubeconfig)
        .env("CURIE_TOOL_LOG", &tools.log)
        .env(
            "PATH",
            format!(
                "{}:{}",
                tools.root.join("bin").display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .env_remove("CURIE_CREDENTIALS")
        .env("HOME", &tools.root);
    command
}

fn wait_bounded(child: &mut std::process::Child) -> std::process::ExitStatus {
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if started.elapsed() > Duration::from_secs(20) {
            let _ = child.kill();
            panic!("quickstart did not exit within 20s");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn run_piped(tools: &ToolDir, kubeconfig: &PathBuf, args: &[&str]) -> (i32, String, String) {
    let mut child = command(tools, kubeconfig, args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let status = wait_bounded(&mut child);
    let mut stdout = String::new();
    let mut stderr = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    (status.code().unwrap_or(1), stdout, stderr)
}

fn set_nonblocking(file: &std::fs::File) {
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0, "fcntl get");
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    assert_eq!(rc, 0, "fcntl set");
}

fn run_on_terminal(tools: &ToolDir, kubeconfig: &PathBuf, answer: &str) -> (i32, String) {
    let mut primary: libc::c_int = -1;
    let mut replica: libc::c_int = -1;
    let rc = unsafe {
        libc::openpty(
            &mut primary,
            &mut replica,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, 0, "openpty");
    let primary = unsafe { OwnedFd::from_raw_fd(primary) };
    let replica = unsafe { OwnedFd::from_raw_fd(replica) };
    let stdout_fd = replica.try_clone().unwrap();
    let stderr_fd = replica.try_clone().unwrap();
    let mut child = command(tools, kubeconfig, &[])
        .stdin(Stdio::from(replica))
        .stdout(Stdio::from(stdout_fd))
        .stderr(Stdio::from(stderr_fd))
        .spawn()
        .unwrap();
    let mut primary = std::fs::File::from(primary);
    set_nonblocking(&primary);
    let mut collected = Vec::new();
    let mut answered = false;
    let started = std::time::Instant::now();
    loop {
        let mut buf = [0u8; 1024];
        match primary.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                collected.extend_from_slice(&buf[..n]);
                if !answered && collected.windows(5).any(|window| window == b"[y/N]") {
                    primary.write_all(answer.as_bytes()).unwrap();
                    let _ = primary.flush();
                    answered = true;
                }
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.raw_os_error() == Some(libc::EIO) =>
            {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                if started.elapsed() > Duration::from_secs(20) {
                    let _ = child.kill();
                    panic!(
                        "terminal quickstart did not exit within 20s: {}",
                        String::from_utf8_lossy(&collected)
                    );
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(err) => panic!("reading the terminal: {err}"),
        }
    }
    let status = wait_bounded(&mut child);
    (
        status.code().unwrap_or(1),
        String::from_utf8_lossy(&collected).into_owned(),
    )
}

#[test]
fn the_cli_refuses_a_non_kind_context_without_a_terminal_before_helm() {
    let tools = ToolDir::new();
    let (dir, config) = kubeconfig("work-cluster");
    let (code, stdout, stderr) = run_piped(&tools, &config, &[]);
    let shown = format!("{stdout}\n{stderr}");
    assert_eq!(code, 2, "{shown}");
    assert!(
        shown.contains("--context work-cluster"),
        "the CLI refusal must name the flag: {shown}"
    );
    assert!(
        shown.contains("not a terminal"),
        "the CLI refusal must name the missing terminal: {shown}"
    );
    assert!(
        !tools.log().contains("helm"),
        "helm ran before the refusal: {}",
        tools.log()
    );
    drop(dir);
}

#[test]
fn the_cli_dry_run_states_the_context_without_a_terminal_or_helm() {
    let tools = ToolDir::new();
    let (dir, config) = kubeconfig("work-cluster");
    let (code, stdout, stderr) = run_piped(&tools, &config, &["--dry-run"]);
    let shown = format!("{stdout}\n{stderr}");
    assert_eq!(code, 0, "{shown}");
    assert!(
        stdout.contains("Kubernetes context: work-cluster because it is the current context and it is not a kind context"),
        "{shown}"
    );
    assert!(!tools.log().contains("helm"), "{}", tools.log());
    drop(dir);
}

#[test]
fn the_cli_does_not_prompt_for_kind_absent_or_explicit_contexts() {
    for (current, expected) in [
        (
            "kind-curie-factory",
            "because the current context is a kind context",
        ),
        (
            "",
            "because no current context is set, so a kind cluster is created",
        ),
    ] {
        let tools = ToolDir::new();
        let (dir, config) = kubeconfig(current);
        let (code, stdout, stderr) = run_piped(&tools, &config, &["--dry-run"]);
        let shown = format!("{stdout}\n{stderr}");
        assert_eq!(code, 0, "{current:?} {shown}");
        assert!(stdout.contains(expected), "{current:?} {shown}");
        assert!(!shown.contains("[y/N]"), "{current:?} {shown}");
        drop(dir);
    }

    let tools = ToolDir::new();
    let (dir, config) = kubeconfig("work-cluster");
    let (code, stdout, stderr) = run_piped(&tools, &config, &["--context", "chosen", "--dry-run"]);
    let shown = format!("{stdout}\n{stderr}");
    assert_eq!(code, 0, "{shown}");
    assert!(
        stdout.contains("Kubernetes context: chosen because --context was passed"),
        "{shown}"
    );
    assert!(!shown.contains("must confirm"), "{shown}");
    assert!(!shown.contains("[y/N]"), "{shown}");
    drop(dir);
}

#[test]
fn the_cli_terminal_accepts_yes_and_reaches_helm_and_no_stops_first() {
    let tools = ToolDir::new();
    let (dir, config) = kubeconfig("work-cluster");
    let (code, shown) = run_on_terminal(&tools, &config, "n\n");
    assert_eq!(code, 2, "{shown}");
    assert!(shown.contains("[y/N]"), "{shown}");
    assert!(shown.contains("--context work-cluster"), "{shown}");
    assert!(!tools.log().contains("helm"), "{}", tools.log());
    drop(dir);

    let tools = ToolDir::new();
    let (dir, config) = kubeconfig("work-cluster");
    let (code, shown) = run_on_terminal(&tools, &config, "y\n");
    assert_ne!(code, 0, "{shown}");
    assert!(shown.contains("[y/N]"), "{shown}");
    assert!(
        !shown.contains("without confirmation"),
        "yes must not be treated as a decline: {shown}"
    );
    assert!(
        tools.log().contains("helm"),
        "yes must reach helm and stop on the stand-in: {} {shown}",
        tools.log()
    );
    drop(dir);
}
