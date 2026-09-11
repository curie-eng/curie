//! Daemon-backed acceptance contract for #2530's hosted-connector start wait.
//!
//! This target deliberately uses the built CLI, Docker, and the task-owned local
//! API. It has no HTTP or Docker mock. Set `CURIE_E2E_DOCKER=1` to opt into the
//! real-process tests; the driver supplies the private daemon and, for `local`
//! tests, may supply `CURIE_E2E_API_URL` / `CURIE_E2E_API_KEY`.
//!
//! Docker Compose documents `up --wait` as waiting for services to be running
//! or healthy and documents `--wait-timeout` in seconds:
//! <https://docs.docker.com/reference/cli/docker/compose/up/>.
//! The health-state fixtures use Dockerfile `HEALTHCHECK`, whose state model is
//! documented at <https://docs.docker.com/reference/dockerfile/#healthcheck>.

use std::env;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use curie::scaffold::scaffold;
use serde_json::Value;
use tempfile::TempDir;
use uuid::Uuid;

const E2E_GATE: &str = "CURIE_E2E_DOCKER";
const E2E_API_URL: &str = "CURIE_E2E_API_URL";
const E2E_API_KEY: &str = "CURIE_E2E_API_KEY";
const DEFAULT_API_URL: &str = "http://localhost:28000";
const DEFAULT_API_KEY: &str = "curie-dev-key";
const CONNECTOR_COMPONENT_LABEL: &str = "curietech.ai/component=connector";
const CONNECTOR_AGENT_LABEL: &str = "curietech.ai/agent";

#[derive(Clone, Copy)]
enum ConnectorCase {
    Healthy,
    LateHealthy,
    SteadyWithoutHealthcheck,
    Exits17,
    StartingPastDeadline,
    Unhealthy,
}

impl ConnectorCase {
    fn slug(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::LateHealthy => "late-healthy",
            Self::SteadyWithoutHealthcheck => "steady",
            Self::Exits17 => "exit17",
            Self::StartingPastDeadline => "starting",
            Self::Unhealthy => "unhealthy",
        }
    }

    fn dockerfile(self) -> &'static str {
        match self {
            Self::Healthy => {
                "FROM busybox:1.36.1\n\
                 HEALTHCHECK --interval=1s --timeout=1s --retries=1 CMD /bin/true\n\
                 CMD [\"sh\", \"-c\", \"sleep 300\"]\n"
            }
            // The first probe starts after its one-second interval, then takes
            // 58 seconds. Compose reaches ready near its 60-second bound while
            // the no-health sibling below has already been running.
            Self::LateHealthy => {
                "FROM busybox:1.36.1\n\
                 HEALTHCHECK --interval=1s --timeout=60s --retries=1 CMD [\"sh\", \"-c\", \"sleep 58; exit 0\"]\n\
                 CMD [\"sh\", \"-c\", \"sleep 300\"]\n"
            }
            Self::SteadyWithoutHealthcheck => "FROM busybox:1.36.1\nCMD [\"sh\", \"-c\", \"sleep 300\"]\n",
            Self::Exits17 => "FROM busybox:1.36.1\nCMD [\"sh\", \"-c\", \"exit 17\"]\n",
            // False probes remain `starting` for the complete 90-second start
            // period. The 90-second interval also prevents an early retry from
            // becoming `unhealthy` before Curie's 60-second deadline.
            Self::StartingPastDeadline => {
                "FROM busybox:1.36.1\n\
                 HEALTHCHECK --interval=90s --timeout=1s --start-period=90s --retries=3 CMD /bin/false\n\
                 CMD [\"sh\", \"-c\", \"sleep 300\"]\n"
            }
            Self::Unhealthy => {
                "FROM busybox:1.36.1\n\
                 HEALTHCHECK --interval=1s --timeout=1s --retries=1 CMD /bin/false\n\
                 CMD [\"sh\", \"-c\", \"sleep 300\"]\n"
            }
        }
    }
}

#[derive(Clone)]
struct ApiTarget {
    url: String,
    key: String,
}

/// Every daemon resource has a generated name. Drop removes only exact runner
/// names, images, networks, and connectors bearing the generated agent label;
/// it never sweeps a shared Compose project.
struct TestScope {
    scratch: TempDir,
    id: String,
    images: Vec<String>,
    runners: Vec<String>,
    networks: Vec<String>,
    agents: Vec<(String, ApiTarget)>,
    connector_agents: Vec<String>,
}

impl TestScope {
    fn new() -> Self {
        Self {
            scratch: TempDir::new().expect("create #2530 test scratch directory"),
            id: Uuid::new_v4().simple().to_string(),
            images: Vec::new(),
            runners: Vec::new(),
            networks: Vec::new(),
            agents: Vec::new(),
            connector_agents: Vec::new(),
        }
    }

    fn image(&mut self, case: ConnectorCase) -> String {
        let image = format!("curie-2530-{}-{}:test", case.slug(), self.id);
        let context = self.scratch.path().join(format!("image-{}", case.slug()));
        fs::create_dir_all(&context).expect("create disposable connector-image context");
        fs::write(context.join("Dockerfile"), case.dockerfile())
            .expect("write disposable connector Dockerfile");

        let status = Command::new("docker")
            .args(["build", "--pull=false", "-t", &image])
            .arg(&context)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("invoke Docker to build the disposable connector image");
        assert!(
            status.success(),
            "the gated test requires the locally loaded busybox:1.36.1 base image"
        );
        self.images.push(image.clone());
        image
    }

    fn bundle(&mut self, agent: &str, connector: &str, image: Option<&str>) -> PathBuf {
        let path = self.scratch.path().join(format!("bundle-{agent}"));
        scaffold(&path, agent).expect("scaffold a valid disposable plugin bundle");
        if let Some(image) = image {
            self.connector_agents.push(agent.to_string());
            fs::write(
                path.join("connectors.yaml"),
                format!("connectors:\n  {connector}:\n    image: {image}\n    port: 8000\n"),
            )
            .expect("write the hosted connector declaration");
        }
        path
    }

    fn runner(&mut self, case: ConnectorCase) -> String {
        let name = format!("curie-2530-runner-{}-{}", case.slug(), self.id);
        self.runners.push(name.clone());
        self.networks.push(format!("{name}-net"));
        name
    }

    fn register_agent(&mut self, agent: String, api: ApiTarget) {
        self.agents.push((agent, api));
    }
}

impl Drop for TestScope {
    fn drop(&mut self) {
        // The platform owns deployments, so end/delete the generated test agent
        // through its real CLI surface before deleting the connector container.
        for (agent, api) in &self.agents {
            let _ = Command::new(curie_bin())
                .args([
                    "local",
                    "delete",
                    agent,
                    "--yes",
                    "--api-url",
                    &api.url,
                    "--api-key",
                    &api.key,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        for agent in &self.connector_agents {
            let output = Command::new("docker")
                .args([
                    "ps",
                    "-aq",
                    "--filter",
                    &format!("label={CONNECTOR_COMPONENT_LABEL}"),
                    "--filter",
                    &format!("label={CONNECTOR_AGENT_LABEL}={agent}"),
                ])
                .output();
            if let Ok(output) = output {
                let listing = String::from_utf8_lossy(&output.stdout);
                let ids: Vec<&str> = listing
                    .lines()
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .collect();
                if !ids.is_empty() {
                    let _ = Command::new("docker")
                        .arg("rm")
                        .arg("-f")
                        .args(ids)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }
        for runner in &self.runners {
            let _ = Command::new("docker")
                .args(["rm", "-f", runner])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        for network in &self.networks {
            let _ = Command::new("docker")
                .args(["network", "rm", network])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        for image in &self.images {
            let _ = Command::new("docker")
                .args(["image", "rm", "-f", image])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn docker_e2e_enabled() -> bool {
    match env::var(E2E_GATE).as_deref() {
        Ok("1") => true,
        _ => {
            eprintln!("skipping #2530 daemon test: set {E2E_GATE}=1 to opt in");
            false
        }
    }
}

fn curie_bin() -> PathBuf {
    env::var_os("CURIE_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_curie")))
}

fn runner_image() -> String {
    env::var("CURIE_E2E_IMAGE").unwrap_or_else(|_| "curie-runner".to_string())
}

fn local_api() -> ApiTarget {
    ApiTarget {
        url: env::var(E2E_API_URL)
            .or_else(|_| env::var("CURIE_API_URL"))
            .unwrap_or_else(|_| DEFAULT_API_URL.to_string()),
        key: env::var(E2E_API_KEY)
            .or_else(|_| env::var("CURIE_API_KEY"))
            .unwrap_or_else(|_| DEFAULT_API_KEY.to_string()),
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a free loopback port");
    let port = listener
        .local_addr()
        .expect("read the loopback port")
        .port();
    drop(listener);
    port
}

fn unique_agent(scope: &TestScope, case: ConnectorCase) -> String {
    format!("wait-{}-{}", case.slug(), &scope.id[..8])
}

fn unique_connector(scope: &TestScope, case: ConnectorCase) -> String {
    format!("probe-{}-{}", case.slug(), &scope.id[..8])
}

fn skill_up(plugin_dir: &Path, runner: &str) -> Output {
    Command::new(curie_bin())
        .args(["--json", "skill", "up", "--plugin-dir"])
        .arg(plugin_dir)
        .args([
            "--image",
            &runner_image(),
            "--name",
            runner,
            "--port",
            &free_port().to_string(),
            "--fake-model",
        ])
        .output()
        .expect("run the built curie skill up command")
}

fn skill_up_human(plugin_dir: &Path, runner: &str) -> Output {
    Command::new(curie_bin())
        .args(["skill", "up", "--plugin-dir"])
        .arg(plugin_dir)
        .args([
            "--image",
            &runner_image(),
            "--name",
            runner,
            "--port",
            &free_port().to_string(),
            "--fake-model",
        ])
        .output()
        .expect("run the built curie skill up command in default human mode")
}

fn skill_down(plugin_dir: &Path, runner: &str) -> Output {
    Command::new(curie_bin())
        .current_dir(plugin_dir)
        .args(["skill", "down", "--name", runner])
        .output()
        .expect("run the built curie skill down command")
}

fn local_deploy(plugin_dir: &Path, agent: &str, api: &ApiTarget, label: &str) -> Output {
    Command::new(curie_bin())
        .args(["--json", "local", "deploy", "--plugin-dir"])
        .arg(plugin_dir)
        .args([
            "--agent",
            agent,
            "--api-url",
            &api.url,
            "--api-key",
            &api.key,
            "--label",
            label,
        ])
        .output()
        .expect("run the built curie local deploy command")
}

fn local_deploy_human(plugin_dir: &Path, agent: &str, api: &ApiTarget, label: &str) -> Output {
    Command::new(curie_bin())
        .args(["local", "deploy", "--plugin-dir"])
        .arg(plugin_dir)
        .args([
            "--agent",
            agent,
            "--api-url",
            &api.url,
            "--api-key",
            &api.key,
            "--label",
            label,
        ])
        .output()
        .expect("run the built curie local deploy command in default human mode")
}

fn local_versions(agent: &str, api: &ApiTarget) -> Output {
    Command::new(curie_bin())
        .args(["--json", "local", "versions", agent, "--api-url", &api.url])
        .args(["--api-key", &api.key])
        .output()
        .expect("read the deployed version through the built CLI")
}

fn assert_succeeded(output: &Output, action: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{action} must succeed for a ready connector (exit status {:?}); stdout: {stdout}; stderr: {stderr}",
        output.status.code(),
    );
}

fn assert_failed_naming_connector(output: &Output, connector: &str, action: &str) -> Value {
    assert!(
        !output.status.success(),
        "{action} must never report success for connector {connector}"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "{action} must report a runtime startup failure"
    );
    let payload: Value = serde_json::from_slice(&output.stdout)
        .expect("--json connector-start failure must emit one recovery object");
    let error = payload["error"]
        .as_str()
        .expect("the recovery object has an error string");
    assert!(
        error.contains(connector),
        "{action} failure must name its connector (reason: {error})"
    );
    assert!(
        payload["fix"]
            .as_str()
            .is_some_and(|fix| !fix.trim().is_empty()),
        "the --json recovery object must retain a nonempty fix string"
    );
    payload
}

fn assert_human_failure_naming_connector_and_fix(output: &Output, connector: &str, action: &str) {
    assert_eq!(
        output.status.code(),
        Some(1),
        "{action} must report a runtime startup failure"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(connector),
        "{action}'s default human error must retain the full declared connector key"
    );
    assert!(
        stderr.contains("Fix:"),
        "{action}'s default human error must include a recovery fix"
    );
}

fn assert_skill_state(plugin_dir: &Path, connector: &str) {
    let state: Value = serde_json::from_slice(
        &fs::read(plugin_dir.join(".curie/runner.json"))
            .expect("successful skill up must persist runner state"),
    )
    .expect("runner state is JSON");
    assert!(
        state["connector_containers"]
            .as_array()
            .is_some_and(|containers| {
                containers.iter().any(|container| {
                    container
                        .as_str()
                        .is_some_and(|name| name.contains(connector))
                })
            }),
        "successful skill up must record the hosted connector it started"
    );
}

fn assert_no_skill_success(plugin_dir: &Path) {
    assert!(
        !plugin_dir.join(".curie/runner.json").exists(),
        "a failed connector start must not leave a successful runner record"
    );
    let snapshots = plugin_dir.join(".curie/snapshots");
    assert!(
        !snapshots.exists()
            || (snapshots.is_dir()
                && fs::read_dir(&snapshots)
                    .expect("read the skill snapshot root")
                    .next()
                    .is_none()),
        "a failed connector start must release the materialized #1087 snapshot"
    );
}

fn add_healthy_sibling_before_starting_connector(
    scope: &mut TestScope,
    plugin_dir: &Path,
    starting_connector: &str,
    starting_image: &str,
) -> String {
    let healthy_connector = format!("a-healthy-{}", &scope.id[..8]);
    let healthy_image = scope.image(ConnectorCase::Healthy);
    fs::write(
        plugin_dir.join("connectors.yaml"),
        format!(
            "connectors:\n  {healthy_connector}:\n    image: {healthy_image}\n    port: 8000\n  \
             {starting_connector}:\n    image: {starting_image}\n    port: 8000\n"
        ),
    )
    .expect("write healthy-then-starting connector declarations");
    healthy_connector
}

fn run_skill_success_case(case: ConnectorCase) {
    let mut scope = TestScope::new();
    let agent = unique_agent(&scope, case);
    let connector = unique_connector(&scope, case);
    let image = scope.image(case);
    let plugin_dir = scope.bundle(&agent, &connector, Some(&image));
    let runner = scope.runner(case);

    let output = skill_up(&plugin_dir, &runner);
    assert_succeeded(&output, "curie skill up");
    assert_skill_state(&plugin_dir, &connector);
    assert_succeeded(&skill_down(&plugin_dir, &runner), "curie skill down");
}

fn run_skill_failure_case(case: ConnectorCase) {
    let mut scope = TestScope::new();
    let agent = unique_agent(&scope, case);
    let connector = unique_connector(&scope, case);
    let image = scope.image(case);
    let plugin_dir = scope.bundle(&agent, &connector, Some(&image));
    let healthy_sibling = matches!(case, ConnectorCase::StartingPastDeadline).then(|| {
        add_healthy_sibling_before_starting_connector(&mut scope, &plugin_dir, &connector, &image)
    });
    let runner = scope.runner(case);

    let started = Instant::now();
    let output = skill_up(&plugin_dir, &runner);
    let failure = assert_failed_naming_connector(&output, &connector, "curie skill up");
    if matches!(case, ConnectorCase::StartingPastDeadline) {
        assert!(
            started.elapsed() >= Duration::from_secs(55),
            "a connector that remains starting must consume Curie's bounded readiness window"
        );
        assert!(
            started.elapsed() <= Duration::from_secs(80),
            "the bounded connector-start deadline must not fall through to Docker's longer wait"
        );
        assert!(
            failure["error"]
                .as_str()
                .is_some_and(|error| error.contains("timeout") || error.contains("timed out")),
            "the starting-state failure must explain that the readiness deadline timed out"
        );
        assert!(
            !failure["error"].as_str().is_some_and(|error| {
                healthy_sibling
                    .as_deref()
                    .is_some_and(|healthy| error.contains(healthy))
            }),
            "the starting-state failure must not blame its earlier healthy sibling"
        );
    }
    assert_no_skill_success(&plugin_dir);
}

fn run_local_success_case(case: ConnectorCase) {
    let mut scope = TestScope::new();
    let api = local_api();
    let agent = unique_agent(&scope, case);
    let connector = unique_connector(&scope, case);
    let image = scope.image(case);
    let plugin_dir = scope.bundle(&agent, &connector, Some(&image));
    let label = format!("wait-{}-{}", case.slug(), &scope.id[..8]);
    scope.register_agent(agent.clone(), api.clone());

    assert_succeeded(
        &local_deploy(&plugin_dir, &agent, &api, &label),
        "curie local deploy",
    );
    let versions = local_versions(&agent, &api);
    assert_succeeded(&versions, "curie local versions");
    assert!(
        String::from_utf8_lossy(&versions.stdout).contains(&label),
        "successful local deploy must persist the deployed version record"
    );
}

fn run_local_failure_case(case: ConnectorCase) {
    let mut scope = TestScope::new();
    let api = local_api();
    let agent = unique_agent(&scope, case);
    let connector = unique_connector(&scope, case);
    let image = scope.image(case);
    let plugin_dir = scope.bundle(&agent, &connector, Some(&image));
    let healthy_sibling = matches!(case, ConnectorCase::StartingPastDeadline).then(|| {
        add_healthy_sibling_before_starting_connector(&mut scope, &plugin_dir, &connector, &image)
    });
    let label = format!("wait-{}-{}", case.slug(), &scope.id[..8]);
    scope.register_agent(agent.clone(), api.clone());

    let started = Instant::now();
    let output = local_deploy(&plugin_dir, &agent, &api, &label);
    let failure = assert_failed_naming_connector(&output, &connector, "curie local deploy");
    if matches!(case, ConnectorCase::StartingPastDeadline) {
        assert!(
            started.elapsed() >= Duration::from_secs(55),
            "a Compose connector that remains starting must consume the 60-second wait"
        );
        assert!(
            started.elapsed() <= Duration::from_secs(80),
            "the Compose wait must stop at Curie's explicit 60-second deadline"
        );
        assert!(
            failure["error"]
                .as_str()
                .is_some_and(|error| error.contains("timeout") || error.contains("timed out")),
            "the starting-state failure must explain that the readiness deadline timed out"
        );
        assert!(
            !failure["error"].as_str().is_some_and(|error| {
                healthy_sibling
                    .as_deref()
                    .is_some_and(|healthy| error.contains(healthy))
            }),
            "the starting-state failure must not blame its earlier healthy sibling"
        );
    }
}

#[test]
fn skill_up_allows_healthy_and_steady_connectors() {
    if !docker_e2e_enabled() {
        return;
    }
    run_skill_success_case(ConnectorCase::Healthy);
    run_skill_success_case(ConnectorCase::SteadyWithoutHealthcheck);
}

#[test]
fn skill_up_refuses_an_exited_connector() {
    if !docker_e2e_enabled() {
        return;
    }
    run_skill_failure_case(ConnectorCase::Exits17);
}

#[test]
fn skill_up_human_exit_names_the_declared_connector_and_fix() {
    if !docker_e2e_enabled() {
        return;
    }
    let mut scope = TestScope::new();
    let agent = unique_agent(&scope, ConnectorCase::Exits17);
    let connector = format!("declared-exit-key-{}", &scope.id[..8]);
    let image = scope.image(ConnectorCase::Exits17);
    let plugin_dir = scope.bundle(&agent, &connector, Some(&image));
    let runner = scope.runner(ConnectorCase::Exits17);

    let output = skill_up_human(&plugin_dir, &runner);
    assert_human_failure_naming_connector_and_fix(&output, &connector, "curie skill up");
    assert_no_skill_success(&plugin_dir);
}

#[test]
fn skill_up_cleans_earlier_connectors_when_later_connector_exits() {
    if !docker_e2e_enabled() {
        return;
    }
    let mut scope = TestScope::new();
    let agent = unique_agent(&scope, ConnectorCase::Exits17);
    let healthy_connector = format!("a-healthy-{}", &scope.id[..8]);
    let exit_connector = format!("z-exit17-{}", &scope.id[..8]);
    let healthy_image = scope.image(ConnectorCase::Healthy);
    let exit_image = scope.image(ConnectorCase::Exits17);
    let plugin_dir = scope.bundle(&agent, &healthy_connector, Some(&healthy_image));
    fs::write(
        plugin_dir.join("connectors.yaml"),
        format!(
            "connectors:\n  {healthy_connector}:\n    image: {healthy_image}\n    port: 8000\n  \
             {exit_connector}:\n    image: {exit_image}\n    port: 8000\n"
        ),
    )
    .expect("write sorted healthy-then-exit connector declarations");
    let runner = scope.runner(ConnectorCase::Exits17);

    let output = skill_up(&plugin_dir, &runner);
    assert_failed_naming_connector(&output, &exit_connector, "curie skill up");
    assert_no_skill_success(&plugin_dir);

    let listing = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label={CONNECTOR_COMPONENT_LABEL}"),
            "--filter",
            &format!("label={CONNECTOR_AGENT_LABEL}={agent}"),
        ])
        .output()
        .expect("list only this test agent's hosted connectors");
    assert!(
        listing.status.success(),
        "Docker must list the generated agent's connector labels"
    );
    assert!(
        String::from_utf8_lossy(&listing.stdout).trim().is_empty(),
        "a later connector failure must clean the earlier connector before test cleanup"
    );
}

#[test]
fn skill_up_refuses_an_unhealthy_connector() {
    if !docker_e2e_enabled() {
        return;
    }
    run_skill_failure_case(ConnectorCase::Unhealthy);
}

#[test]
fn skill_up_times_out_when_connector_stays_starting() {
    if !docker_e2e_enabled() {
        return;
    }
    run_skill_failure_case(ConnectorCase::StartingPastDeadline);
}

#[test]
fn skill_up_without_a_hosted_connector_is_unaffected() {
    if !docker_e2e_enabled() {
        return;
    }
    let mut scope = TestScope::new();
    let agent = format!("wait-none-{}", &scope.id[..8]);
    let plugin_dir = scope.bundle(&agent, "unused", None);
    let runner = scope.runner(ConnectorCase::SteadyWithoutHealthcheck);

    assert_succeeded(
        &skill_up(&plugin_dir, &runner),
        "curie skill up without a hosted connector",
    );
    let state: Value = serde_json::from_slice(
        &fs::read(plugin_dir.join(".curie/runner.json"))
            .expect("successful no-connector skill up persists runner state"),
    )
    .expect("runner state is JSON");
    assert_eq!(
        state["connector_containers"].as_array().map(Vec::len),
        Some(0),
        "a bundle with no hosted connector must not gain one"
    );
    assert_succeeded(&skill_down(&plugin_dir, &runner), "curie skill down");
}

#[test]
fn local_deploy_allows_healthy_and_steady_connectors() {
    if !docker_e2e_enabled() {
        return;
    }
    run_local_success_case(ConnectorCase::Healthy);
    run_local_success_case(ConnectorCase::SteadyWithoutHealthcheck);
}

#[test]
fn local_deploy_allows_forty_steady_connectors_after_compose_wait() {
    if !docker_e2e_enabled() {
        return;
    }
    let mut scope = TestScope::new();
    let api = local_api();
    let agent = unique_agent(&scope, ConnectorCase::SteadyWithoutHealthcheck);
    let image = scope.image(ConnectorCase::SteadyWithoutHealthcheck);
    let connectors: Vec<String> = (0..40)
        .map(|index| format!("n{index:02}-{}", &scope.id[..8]))
        .collect();
    let plugin_dir = scope.bundle(&agent, &connectors[0], Some(&image));
    let mut declarations = String::from("connectors:\n");
    for connector in &connectors {
        declarations.push_str(&format!(
            "  {connector}:\n    image: {image}\n    port: 8000\n"
        ));
    }
    fs::write(plugin_dir.join("connectors.yaml"), declarations)
        .expect("write forty steady connector declarations");
    let label = format!("forty-steady-{}", &scope.id[..8]);
    scope.register_agent(agent.clone(), api.clone());

    assert_succeeded(
        &local_deploy(&plugin_dir, &agent, &api, &label),
        "curie local deploy with forty steady connectors",
    );
    let versions = local_versions(&agent, &api);
    assert_succeeded(&versions, "curie local versions");
    assert!(
        String::from_utf8_lossy(&versions.stdout).contains(&label),
        "the forty-connector deploy must persist its deployed version"
    );
}

#[test]
fn local_deploy_refuses_an_exited_connector() {
    if !docker_e2e_enabled() {
        return;
    }
    run_local_failure_case(ConnectorCase::Exits17);
}

#[test]
fn local_deploy_human_exit_names_the_full_declared_connector_and_fix() {
    if !docker_e2e_enabled() {
        return;
    }
    let mut scope = TestScope::new();
    let api = local_api();
    let agent = format!("local-{}-{}", "a".repeat(25), &scope.id[..8]);
    let connector = format!("declared-exit-key-{}", &scope.id[..8]);
    let rendered_name = curie::connector_build::object_name("curie", &agent, &connector);
    assert!(
        !rendered_name.contains(&connector),
        "the local fixture must force object_name truncation"
    );
    let image = scope.image(ConnectorCase::Exits17);
    let plugin_dir = scope.bundle(&agent, &connector, Some(&image));
    let label = format!("human-exit-{}", &scope.id[..8]);
    scope.register_agent(agent.clone(), api.clone());

    let output = local_deploy_human(&plugin_dir, &agent, &api, &label);
    assert_human_failure_naming_connector_and_fix(&output, &connector, "curie local deploy");
}

#[test]
fn local_deploy_refuses_an_unhealthy_connector() {
    if !docker_e2e_enabled() {
        return;
    }
    run_local_failure_case(ConnectorCase::Unhealthy);
}

#[test]
fn local_deploy_succeeds_when_late_health_leaves_nohealth_sibling_to_verify() {
    if !docker_e2e_enabled() {
        return;
    }
    let mut scope = TestScope::new();
    let api = local_api();
    let agent = unique_agent(&scope, ConnectorCase::LateHealthy);
    let nohealth_connector = format!("a-nohealth-{}", &scope.id[..8]);
    let late_connector = format!("z-latehealthy-{}", &scope.id[..8]);
    let nohealth_image = scope.image(ConnectorCase::SteadyWithoutHealthcheck);
    let late_image = scope.image(ConnectorCase::LateHealthy);
    let plugin_dir = scope.bundle(&agent, &nohealth_connector, Some(&nohealth_image));
    fs::write(
        plugin_dir.join("connectors.yaml"),
        format!(
            "connectors:\n  {nohealth_connector}:\n    image: {nohealth_image}\n    port: 8000\n  \
             {late_connector}:\n    image: {late_image}\n    port: 8000\n"
        ),
    )
    .expect("write no-health then late-healthy connector declarations");
    let label = format!("late-health-{}", &scope.id[..8]);
    scope.register_agent(agent.clone(), api.clone());

    let started = Instant::now();
    assert_succeeded(
        &local_deploy(&plugin_dir, &agent, &api, &label),
        "curie local deploy with a late healthy connector",
    );
    assert!(
        started.elapsed() >= Duration::from_secs(55),
        "the health fixture must reach Compose near its 60-second boundary"
    );
    assert!(
        started.elapsed() <= Duration::from_secs(80),
        "the local verification must finish shortly after Compose reports ready"
    );
    let versions = local_versions(&agent, &api);
    assert_succeeded(&versions, "curie local versions");
    assert!(
        String::from_utf8_lossy(&versions.stdout).contains(&label),
        "the late-ready local deploy must persist its deployed version"
    );
}

#[test]
fn local_deploy_times_out_when_connector_stays_starting() {
    if !docker_e2e_enabled() {
        return;
    }
    run_local_failure_case(ConnectorCase::StartingPastDeadline);
}
