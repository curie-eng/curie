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
    describe, quickstart_plan, PlanInput, ReleaseRead, DEFAULT_AGENT, DEFAULT_BUDGET_USD,
    DEFAULT_DEADLINE_SECONDS, DEFAULT_KIND_NAME, DEFAULT_MODEL, DEFAULT_OUTPUT_TOKENS_PER_RUN,
};

fn plan_input() -> PlanInput {
    PlanInput {
        repos: vec!["acme/widgets".into()],
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
        output_tokens: DEFAULT_OUTPUT_TOKENS_PER_RUN,
        chart: "charts/curie".into(),
        credential_in_env: false,
        release_has_real_model: false,
        chart_explicit: false,
        interactive: false,
        release_read: ReleaseRead::NotAtTarget,
        agent: DEFAULT_AGENT.into(),
        label: None,
        plugin_dir: None,
        model_explicit: false,
        recorded: None,
        bindings: Vec::new(),
        runner_image: None,
        jira: None,
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

/// `cluster status` prints the console at 127.0.0.1:30080 and Langfuse at
/// 127.0.0.1:30000. A kind node publishes no NodePort to the host unless the
/// cluster is created with `extraPortMappings`, so the plan creates it with them.
#[test]
fn the_kind_cluster_is_created_with_the_console_ports_published() {
    let planned = quickstart_plan(&plan_input()).expect("no context proceeds");
    let text = describe(&planned).join("\n");
    assert!(
        text.contains("kind create cluster --name curie-factory --config"),
        "{text}"
    );
    assert!(text.contains("127.0.0.1:30080"), "{text}");
    assert!(text.contains("127.0.0.1:30000"), "{text}");
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

/// #4626: a dry run with a targeted context does not read the release, so it
/// must not claim an unconditional `cluster up` or that the release has no real
/// model. The plan states the live decision as conditional lines, identically
/// in human and JSON output.
#[test]
fn the_cli_targeted_dry_run_plans_cluster_up_conditionally() {
    let run = |json: bool| {
        let tools = ToolDir::new();
        let (dir, config) = kubeconfig("work-cluster");
        let config_dir = tools.root.join("curie-config");
        fs::create_dir_all(&config_dir).unwrap();
        let mut args = vec!["--context", "chosen", "--dry-run"];
        if json {
            args.push("--json");
        }
        let output = command(&tools, &config, &args)
            .env("CURIE_CONFIG_DIR", &config_dir)
            .env("XDG_CONFIG_HOME", tools.root.join("config"))
            .env_remove("CURIE_MODEL")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let shown = format!("{stdout}\n{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(output.status.code(), Some(0), "json={json}: {shown}");
        let log = tools.log();
        assert!(
            log.lines().all(|line| line != "kind"),
            "json={json}: a targeted dry run never runs kind: {log}"
        );
        drop(dir);
        (stdout, shown)
    };

    let (human, human_shown) = run(false);
    let (json, json_shown) = run(true);
    let human_lines: Vec<String> = human
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect();
    let body: serde_json::Value = serde_json::from_str(&json)
        .unwrap_or_else(|error| panic!("one plan object ({error}): {json_shown}"));
    assert_eq!(body["dry_run"], true, "{body}");
    let plan: Vec<String> = body["plan"]
        .as_array()
        .unwrap_or_else(|| panic!("plan lines: {body}"))
        .iter()
        .map(|line| line.as_str().expect("plan line is a string").to_owned())
        .collect();
    assert_eq!(
        human_lines, plan,
        "human and JSON dry runs must carry the same lines:\n{human_shown}\n{json_shown}"
    );

    let expected = [
        "release curie in namespace curie was not read under --dry-run; a live run reads it and decides whether cluster up runs".to_owned(),
        "if the release already runs the target chart and values: skip cluster up and keep the model the release records".to_owned(),
        format!("if the release records a real model, cluster up keeps that model and its credential in place of --model {DEFAULT_MODEL}"),
    ];
    let mut positions = Vec::new();
    for line in &expected {
        let found: Vec<usize> = plan
            .iter()
            .enumerate()
            .filter(|(_, candidate)| *candidate == line)
            .map(|(index, _)| index)
            .collect();
        assert_eq!(found.len(), 1, "exactly one {line:?}: {plan:#?}");
        positions.push(found[0]);
    }
    let otherwise: Vec<usize> = plan
        .iter()
        .enumerate()
        .filter(|(_, line)| line.starts_with("otherwise: curie cluster up "))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(otherwise.len(), 1, "one conditional cluster up: {plan:#?}");
    positions.push(otherwise[0]);
    assert!(
        positions.windows(2).all(|pair| pair[0] + 1 == pair[1]),
        "the conditional lines must be consecutive and in order: {plan:#?}"
    );
    let up = &plan[otherwise[0]];
    assert!(
        up.starts_with(
            "otherwise: curie cluster up --context chosen --namespace curie --release curie "
        ),
        "{up}"
    );
    assert!(up.contains(&format!("--model {DEFAULT_MODEL}")), "{up}");
    assert!(
        plan.iter()
            .filter(|line| line.contains("curie cluster up"))
            .all(|line| line.starts_with("otherwise: curie cluster up ")),
        "no unconditional cluster up may be planned: {plan:#?}"
    );
    assert!(
        !plan
            .iter()
            .any(|line| line
                == "skip cluster up: the release is already at the target chart and values"),
        "an unread release must not be claimed at target: {plan:#?}"
    );
    let credential: Vec<&String> = plan
        .iter()
        .filter(|line| line.starts_with("model credential: "))
        .collect();
    assert_eq!(
        credential,
        vec!["model credential: if the release records a real model, keep it (no prompt); otherwise refuse because CURIE_CREDENTIALS is unset, the release has no real model, and stdin is not a terminal"],
        "{plan:#?}"
    );
}
