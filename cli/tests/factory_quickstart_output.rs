//! Capture the candidate quickstart binary at its process boundary, including
//! a real terminal. Only its external Helm, Kubernetes, GitHub, registry and
//! platform API peers are fixtures. The Kubernetes wire shapes follow the
//! Deployment and Pod references cited in `cluster_convergence.rs`; GitHub
//! responses follow the REST App endpoints cited in `factory_github_app.rs`.

#![cfg(unix)]

mod support;

use std::fs;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{serve, MockServer, Request, Response};

const INDEX: &str =
    r#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#;

// The driver answers explicit external commands and rejects unknown ones.
// State lives inside the owned temporary fixture, never in a real cluster.
const TOOL_DRIVER: &str = r#"#!/usr/bin/python3
import json, os, sys, time
from pathlib import Path
root = Path(os.environ['QUICKSTART_FIXTURE'])
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / 'calls').open('a') as log:
    log.write(json.dumps([tool, *args]) + '\n')
ns = os.environ.get('QUICKSTART_NAMESPACE', 'curie')
release = os.environ.get('QUICKSTART_RELEASE', 'curie')
def emit(value):
    print(json.dumps(value)); sys.exit(0)
def absent(kind, name):
    print('Error from server (NotFound): ' + kind + ' "' + name + '" not found', file=sys.stderr)
    sys.exit(1)
def owned(kind, name):
    return {'kind':kind, 'metadata':{'name':name, 'namespace':ns, 'labels':{'app.kubernetes.io/managed-by':'Helm'}, 'annotations':{'meta.helm.sh/release-name':'acme-platform','meta.helm.sh/release-namespace':'acme-system'}}}
deployment = {'apiVersion':'apps/v1', 'kind':'Deployment', 'metadata':{'name':release+'-api','namespace':ns,'generation':1}, 'spec':{'replicas':1,'selector':{'matchLabels':{'app':'api'}},'template':{'metadata':{'labels':{'app':'api'}},'spec':{'containers':[{'name':'api','image':'example.com/api:fixture'}]}}}}
live = json.loads(json.dumps(deployment))
live['status'] = {'observedGeneration':1,'replicas':1,'updatedReplicas':1,'readyReplicas':1,'availableReplicas':1,'unavailableReplicas':0,'conditions':[{'type':'Available','status':'True'}]}
pod = {'apiVersion':'v1','kind':'Pod','metadata':{'name':release+'-api-fixture','namespace':ns,'labels':{'app':'api'}},'spec':{'containers':deployment['spec']['template']['spec']['containers']},'status':{'phase':'Running','containerStatuses':[{'name':'api','image':'example.com/api:fixture','imageID':'containerd://sha256:fixture','ready':True,'state':{'running':{}}}]}}
if tool == 'kind':
    if args == ['get','clusters']:
        if (root / 'kind-created').exists(): print('curie-factory')
        sys.exit(0)
    if args[:2] == ['create','cluster']:
        (root / 'kind-created').touch()
        config = root / 'kubeconfig'
        config.write_text(config.read_text().replace('current-context: ""','current-context: kind-curie-factory'))
        print('Created fixture kind cluster'); sys.exit(0)
if tool == 'helm':
    if args[:2] == ['get','values']: print((root / 'values').read_text()); sys.exit(0)
    if args[:2] == ['get','hooks']: sys.exit(0)
    if args[:2] == ['get','metadata']: emit({'appVersion':os.environ['QUICKSTART_VERSION']})
    if args[:2] == ['get','manifest']: emit(deployment)
    if args[0] == 'history': emit([{'revision':1,'status':'deployed'}])
    if args[0] == 'status': emit({'version':1,'info':{'status':'deployed'},'hooks':[]})
    if args[:2] == ['show','chart']: print('name: curie\nversion: '+os.environ['QUICKSTART_VERSION']+'\nappVersion: '+os.environ['QUICKSTART_VERSION']); sys.exit(0)
    if args[0] == 'template':
        template = args[args.index('--show-only')+1]
        if 'priorityclass' in template:
            role = 'platform' if args[-1] == 'priorityClasses.sandbox.create=false' else 'sandbox'
            emit({'kind':'PriorityClass','metadata':{'name':'curie-'+role}})
        if 'preflight-gvisor' in template and os.environ.get('QUICKSTART_GVISOR_RETRY') == '1' and 'security.gvisor.mode=off' not in args:
            emit({'kind':'Job','metadata':{'name':release+'-preflight-gvisor'}})
        print('Error: could not find template '+template+' in chart', file=sys.stderr); sys.exit(1)
    if args[0] == 'upgrade':
        for index, argument in enumerate(args[:-1]):
            if argument in ['-f','--values']:
                with (root / 'helm-values').open('a') as values_log:
                    values_log.write(json.dumps(Path(args[index+1]).read_text()) + '\n')
        if os.environ.get('QUICKSTART_FAIL') == 'up':
            print('Error: fixture install refused at '+str(root / 'cached-chart'), file=sys.stderr); sys.exit(1)
        if os.environ.get('QUICKSTART_GVISOR_RETRY') == '1' and not (root / 'retried').exists():
            (root / 'retried').touch()
            time.sleep(10)
            print('Error: pods "fixture" is forbidden: RuntimeClass "gvisor" not found', file=sys.stderr); sys.exit(1)
        print('Release accepted'); sys.exit(0)
    if args[0] == 'uninstall': sys.exit(0)
if tool == 'kubectl':
    if args[:2] == ['config','view']:
        emit({'clusters':[{'cluster':{'server':'https://127.0.0.1:9'}}],'users':[{'user':{'token':'fixture'}}]})
    if 'scale' in args: print('deployment.apps/coredns scaled'); sys.exit(0)
    if 'apply' in args: sys.stdin.read(); print('secret configured'); sys.exit(0)
    if 'delete' in args and 'sandboxclaim' in args: print('No resources found'); sys.exit(0)
    if 'rollout' in args or 'label' in args or 'patch' in args: print('fixture command completed'); sys.exit(0)
    if 'get' in args:
        ix = args.index('get'); kind = args[ix+1]; name = args[ix+2] if len(args) > ix+2 else ''
        if kind == 'events' and '--watch' in args:
            deadline = time.monotonic() + 5
            while not (root / 'retried').exists():
                if time.monotonic() >= deadline:
                    print('fixture event watch did not observe the install starting', file=sys.stderr)
                    sys.exit(64)
                time.sleep(0.01)
            print('\x1f'.join(['ADDED','fixture-admission','Job',ns,release+'-preflight-gvisor','FailedCreate','Error creating: pods "fixture" is forbidden: pod rejected: RuntimeClass "gvisor" not found']),flush=True)
            time.sleep(10); sys.exit(0)
        if kind == 'events' and any('jsonpath=' in arg for arg in args): sys.exit(0)
        if kind in ['namespace','namespaces'] and name not in ['-o','--output']:
            emit({'apiVersion':'v1','kind':'Namespace','metadata':{'name':name,'uid':'fixture-'+name,'resourceVersion':'1','labels':{'curietech.ai/created-by':release,'curietech.ai/created-in':ns}}})
        if kind in ['secret','secrets']: absent(kind, name)
        if kind in ['priorityclass','priorityclasses'] and name not in ['-o','--output']: emit(owned('PriorityClass',name))
        if kind in ['deployment','deployments'] and name == 'agent-sandbox-controller': emit(owned('Deployment',name))
        if kind == 'deployments,statefulsets,daemonsets,pods,jobs': emit({'items':[live,pod]})
        if kind in ['deployment','deployments'] and ('jsonpath' in ' '.join(args)): print(release+'-api'); sys.exit(0)
        if kind in ['deployment','deployments']: emit({'items':[live]})
        if kind in ['pod','pods']: emit({'items':[pod]})
        if kind in ['events','event','replicasets','jobs','nodes','services','svc','service','statefulsets','daemonsets']: emit({'items':[]})
print('unexpected fixture command: '+tool+' '+str(args), file=sys.stderr)
sys.exit(64)
"#;

fn agent() -> Value {
    json!({"id":"agent-fixture","name":"dark-factory","channels":[{"kind":"slack","address":"C0EXAMPLE1"}],"created_at":"2026-10-04T00:00:00Z","memory":false,"memory_writes":true,"publication_policy":"auto","execution_deadline_seconds":3600})
}

fn platform(req: &Request) -> Response {
    let path = req.path.split('?').next().unwrap();
    let result = match (req.method.as_str(), path) {
        ("GET", "/agents") => json!([agent()]),
        ("POST", "/agents") | ("GET", "/agents/agent-fixture") => agent(),
        ("PATCH", "/agents/agent-fixture") => {
            let mut saved = agent();
            let patch: Value = serde_json::from_slice(&req.body).unwrap();
            saved
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            saved
        }
        ("POST", "/agents/agent-fixture/channels") => {
            let mut saved = agent();
            let binding: Value = serde_json::from_slice(&req.body).unwrap();
            saved["channels"].as_array_mut().unwrap().push(binding);
            saved
        }
        ("POST", "/agents/agent-fixture/versions") => {
            json!({"id":"version-fixture","agent_id":"agent-fixture","version_label":"fixture","created_by":"fixture","created_at":"2026-10-04T00:00:00Z"})
        }
        ("PUT", "/agents/agent-fixture/versions/version-fixture/bundle") => {
            json!({"version_id":"version-fixture","bundle_ref":"bundles/fixture.tar.gz","bundle_sha256":"fixture","size_bytes":100})
        }
        ("GET", "/agents/agent-fixture/versions/version-fixture/connectors") => {
            json!({"manifests":[],"owned_secret_name":"","owned_secret_keys":[],"mcp_entries":{},"version_id":"version-fixture","triggers":[]})
        }
        ("GET", "/deployments") => json!([]),
        ("POST", "/deployments") => {
            json!({"id":"deployment-fixture","agent_id":"agent-fixture","version_id":"version-fixture","environment":"prod","workspace_enabled":false,"status":"active","deployed_at":"2026-10-04T00:00:00Z"})
        }
        ("GET", "/agents/agent-fixture/budget") | ("PUT", "/agents/agent-fixture/budget") => {
            json!({"max_output_tokens_per_run":null,"max_usd_per_day":5.0})
        }
        _ => return Response::json(500, &format!("unexpected {} {}", req.method, req.path)),
    };
    Response::json(200, &result.to_string())
}

struct Fixture {
    dir: tempfile::TempDir,
    github: MockServer,
    platform: MockServer,
    registry: MockServer,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("bin")).unwrap();
        for tool in ["helm", "kubectl", "kind", "docker"] {
            let path = dir.path().join("bin").join(tool);
            fs::write(&path, TOOL_DRIVER).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(dir.path().join("kubeconfig"), "apiVersion: v1\nkind: Config\ncurrent-context: \"\"\ncontexts:\n  - name: kind-curie-factory\n    context:\n      cluster: fixture\n  - name: acme-cluster\n    context:\n      cluster: fixture\n").unwrap();
        let output = Command::new("openssl")
            .args(["genrsa", "2048"])
            .output()
            .expect("runtime PEM generator");
        assert!(output.status.success(), "runtime PEM generator failed");
        fs::write(dir.path().join("app.pem"), output.stdout).unwrap();
        // The deploy's runner binding resolves its chart through the release
        // cache. Package the candidate chart as that external artifact so a
        // source version needs no matching public GitHub Release to run here.
        let artifact = curie::artifacts::resolve_chart(
            None,
            curie::artifacts::Channel::Release,
            env!("CARGO_PKG_VERSION"),
            || Ok(dir.path().join(".cache/curie")),
            false,
        )
        .unwrap();
        let path = artifact.planned_target();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let archive = flate2::write::GzEncoder::new(
            fs::File::create(path).unwrap(),
            flate2::Compression::default(),
        );
        let mut archive = tar::Builder::new(archive);
        archive
            .append_dir_all(
                "curie",
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"),
            )
            .unwrap();
        archive.into_inner().unwrap().finish().unwrap();
        let digest = format!(
            "sha256:{}",
            Sha256::digest(INDEX.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        fs::write(dir.path().join("values"), json!({"security":{"allowDevDefaults":false},"agentSandbox":{"runner":{"fakeModel":false,"image":"ghcr.io/curie-eng/curie-runner","tag":env!("CARGO_PKG_VERSION"),"digest":digest}},"api":{"environment":"dev"}}).to_string()).unwrap();
        let github = serve(|req| {
            let path = req.path.split('?').next().unwrap();
            match (req.method.as_str(), path) {
                ("GET", "/app") => Response::json(
                    200,
                    r#"{"id":1234567,"slug":"acme-factory","name":"acme-factory"}"#,
                ),
                ("GET", "/app/installations") => {
                    Response::json(200, r#"[{"id":42,"account":{"login":"acme"}}]"#)
                }
                ("POST", "/app/installations/42/access_tokens") => Response::json(
                    201,
                    r#"{"token":"ghs_fixture","expires_at":"2099-01-01T00:00:00Z"}"#,
                ),
                ("GET", "/installation/repositories") => Response::json(
                    200,
                    r#"{"total_count":1,"repositories":[{"full_name":"acme/widgets"}]}"#,
                ),
                ("GET", "/repos/acme/widgets/labels/curie-factory") => {
                    Response::json(200, r#"{"name":"curie-factory"}"#)
                }
                ("GET", "/meta") => Response::json(200, r#"{"api":["192.0.2.0/24"]}"#),
                _ => Response::json(404, r#"{"message":"Not Found"}"#),
            }
        });
        let registry = serve(|req| {
            if req.path.starts_with("/token?") {
                return Response::json(200, r#"{"token":"fixture"}"#);
            }
            if req.path.contains("/manifests/") {
                return Response {
                    status: 200,
                    content_type: "application/vnd.oci.image.index.v1+json".into(),
                    body: INDEX.as_bytes().to_vec(),
                };
            }
            Response::json(404, "{}")
        });
        Self {
            dir,
            github,
            platform: serve(platform),
            registry,
        }
    }

    fn command(&self, second: bool, extra: &[&str]) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_curie"));
        cmd.args(["factory", "quickstart", "--repo", "acme/widgets", "--chart"])
            .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie"))
            .args(extra)
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.dir.path().join("bin").display()),
            )
            .env("QUICKSTART_FIXTURE", self.dir.path())
            .env("QUICKSTART_VERSION", env!("CARGO_PKG_VERSION"))
            .env("KUBECONFIG", self.dir.path().join("kubeconfig"))
            .env("HOME", self.dir.path())
            .env_remove("XDG_CACHE_HOME")
            .env("TMPDIR", self.dir.path())
            .env("CURIE_CREDENTIALS", "sk-or-fixture")
            .env("CURIE_GITHUB_API_URL", &self.github.base_url)
            .env("CURIE_API_URL", &self.platform.base_url)
            .env("CURIE_API_KEY", "fixture-key")
            .env_remove(curie::factory_intake::WEBHOOK_SECRET_ENV)
            .env("CURIE_TEST_ARTIFACT_CHANNEL", "release")
            .env(
                "CURIE_TEST_SRE_BOT_REGISTRY_ENDPOINT",
                &self.registry.base_url,
            )
            .env_remove("CI")
            .env_remove("NO_COLOR")
            .env_remove("CLICOLOR_FORCE")
            .env("TERM", "xterm")
            .env("LANG", "C.UTF-8");
        if second {
            cmd.args(["--app-id", "1234567", "--private-key-file"])
                .arg(self.dir.path().join("app.pem"));
        }
        cmd
    }

    fn run(&self, second: bool, extra: &[&str]) -> (i32, String) {
        let output = self
            .command(second, extra)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        (
            output.status.code().unwrap_or(1),
            format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    }

    fn terminal(&self, second: bool) -> (i32, String) {
        let mut primary = -1;
        let mut replica = -1;
        assert_eq!(
            unsafe {
                libc::openpty(
                    &mut primary,
                    &mut replica,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        let primary = unsafe { OwnedFd::from_raw_fd(primary) };
        let replica = unsafe { OwnedFd::from_raw_fd(replica) };
        let mut child = self
            .command(second, &["--color", "never"])
            .stdin(Stdio::from(replica.try_clone().unwrap()))
            .stdout(Stdio::from(replica.try_clone().unwrap()))
            .stderr(Stdio::from(replica))
            .spawn()
            .unwrap();
        let mut primary = fs::File::from(primary);
        let flags = unsafe { libc::fcntl(primary.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(primary.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let start = Instant::now();
        let mut bytes = Vec::new();
        loop {
            let mut buf = [0u8; 4096];
            match primary.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => bytes.extend_from_slice(&buf[..n]),
                Err(err)
                    if err.kind() == std::io::ErrorKind::WouldBlock
                        || err.raw_os_error() == Some(libc::EIO) =>
                {
                    if child.try_wait().unwrap().is_some() {
                        break;
                    }
                    if start.elapsed() > Duration::from_secs(30) {
                        let _ = child.kill();
                        panic!(
                            "terminal quickstart timed out: {}",
                            String::from_utf8_lossy(&bytes)
                        );
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                Err(err) => panic!("reading terminal: {err}"),
            }
        }
        let code = child.wait().unwrap().code().unwrap_or(1);
        (code, String::from_utf8_lossy(&bytes).replace("\r\n", "\n"))
    }
}

fn assert_short_pass(code: i32, shown: &str, second: bool, surface: &str) {
    assert_eq!(code, 0, "{shown}");
    assert_eq!(shown.matches("Kubernetes context:").count(), 1, "{shown}");
    assert_eq!(
        shown
            .lines()
            .filter(|line| *line == "Creating kind cluster")
            .count(),
        usize::from(!second),
        "{shown}"
    );
    assert!(
        shown.lines().count() <= if second { 18 } else { 14 },
        "line budget exceeded: {shown}"
    );
    for noise in [
        "push delivery",
        "C0EXAMPLE1",
        "channel ",
        "rolling curie",
        "binding sandbox",
        "Release accepted",
        "installed 0.0s",
        "\u{1b}[",
    ] {
        assert!(
            !shown.contains(noise),
            "unexpected child output {noise}: {shown}"
        );
    }
    assert!(shown.contains("inferred model provider"), "{shown}");
    assert!(shown.contains("inferred reuse of PriorityClass"), "{shown}");
    assert!(
        shown.contains("inferred reuse of `agent-sandbox-controller`"),
        "{shown}"
    );
    for equivalent_override in [
        "--allow-egress-host openrouter",
        "--set priorityClasses.platform.create=false",
        "--set priorityClasses.sandbox.create=false",
        "--set agentSandbox.controller.deploy=false",
    ] {
        assert!(
            shown.contains(equivalent_override),
            "missing {equivalent_override}: {shown}"
        );
    }
    for step in ["Scaling CoreDNS", "Installing Curie"].into_iter().chain(
        second
            .then_some([
                "Configuring factory intake",
                "Rendering factory bundle",
                "Deploying dark factory",
                "Binding GitHub repository",
                "Setting execution deadline",
                "Setting publication policy",
                "Setting factory budget",
            ])
            .into_iter()
            .flatten(),
    ) {
        assert_eq!(
            shown.lines().filter(|line| *line == step).count(),
            1,
            "{shown}"
        );
    }
    if second {
        assert!(shown.contains("Factory quickstart ready"), "{shown}");
        assert!(shown.contains("Intake poll. App acme-factory."), "{shown}");
        assert!(shown.contains("Deployed dark-factory"), "{shown}");
    } else {
        assert!(
            shown.contains("https://github.com/settings/apps/new"),
            "{shown}"
        );
        let rerun = shown.lines().find(|line| line.contains("Rerun:")).unwrap();
        for default in ["--namespace", "--release", "--model"] {
            assert!(!rerun.contains(default), "{rerun}");
        }
    }
    if std::env::var("CURIE_QUICKSTART_PROOF").as_deref() == Ok("1") {
        eprintln!(
            "Factory proof {surface}, {} pass, {} lines:\n{shown}",
            if second { "ready" } else { "registration" },
            shown.lines().count()
        );
    }
}

#[test]
fn both_default_passes_stay_within_the_line_budget_against_a_fake_cluster() {
    let fixture = Fixture::new();
    for second in [false, true] {
        let (code, shown) = fixture.run(second, &["--color", "never"]);
        assert_short_pass(code, &shown, second, "pipes");
    }
    assert!(
        fixture
            .platform
            .recorded()
            .iter()
            .any(|req| req.method == "POST" && req.path == "/deployments"),
        "the second pass must traverse the real deploy path"
    );
    let requests = fixture.platform.recorded();
    for (method, path, field, value) in [
        (
            "POST",
            "/agents/agent-fixture/channels",
            "address",
            json!("acme/widgets"),
        ),
        (
            "PATCH",
            "/agents/agent-fixture",
            "execution_deadline_seconds",
            json!(3600),
        ),
        (
            "PATCH",
            "/agents/agent-fixture",
            "publication_policy",
            json!("auto"),
        ),
        (
            "PUT",
            "/agents/agent-fixture/budget",
            "max_usd_per_day",
            json!(5.0),
        ),
    ] {
        assert!(
            requests.iter().any(|request| {
                request.method == method
                    && request.path == path
                    && serde_json::from_slice::<Value>(&request.body).unwrap()[field] == value
            }),
            "the ready path did not apply {field}"
        );
    }
}

#[test]
fn both_passes_use_separate_complete_lines_in_a_real_terminal() {
    let fixture = Fixture::new();
    for second in [false, true] {
        let (code, shown) = fixture.terminal(second);
        assert_short_pass(code, &shown, second, "terminal");
        assert!(!shown.contains('\r'), "spinner redraw in terminal: {shown}");
    }
}

#[test]
fn debug_reveals_chained_detail_and_default_failure_reports_one_pathless_error() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(false, &["--debug", "--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    assert!(shown.contains("helm upgrade"), "{shown}");
    assert!(shown.contains("Release accepted"), "{shown}");
    let mut cmd = fixture.command(false, &["--color", "never"]);
    let output = cmd.env("QUICKSTART_FAIL", "up").output().unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{shown}");
    assert_eq!(shown.matches("Error:").count(), 1, "{shown}");
    assert!(shown.contains("Installing Curie failed"), "{shown}");
    assert!(shown.contains("fixture install refused"), "{shown}");
    assert!(shown.contains("Fix:"), "{shown}");
    assert!(
        !shown.contains(fixture.dir.path().to_str().unwrap()),
        "{shown}"
    );
    assert!(!shown.contains(env!("CARGO_BIN_EXE_curie")), "{shown}");
    assert!(!shown.contains("cached-chart"), "{shown}");
    let output = fixture
        .command(false, &["--json"])
        .env("QUICKSTART_FAIL", "up")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).expect("one JSON error");
    let error = body["error"].as_str().expect("JSON error text");
    assert!(error.contains("Installing Curie failed"), "{body}");
    assert!(error.contains("fixture install refused"), "{body}");
    assert!(body["fix"].is_string(), "{body}");
    assert!(
        !body
            .to_string()
            .contains(fixture.dir.path().to_str().unwrap()),
        "{body}"
    );
    assert!(
        !body.to_string().contains(env!("CARGO_BIN_EXE_curie")),
        "{body}"
    );
    assert!(!body.to_string().contains("cached-chart"), "{body}");
    let output = fixture
        .command(false, &["--debug", "--color", "never"])
        .env("QUICKSTART_FAIL", "up")
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{shown}");
    assert!(shown.contains("fixture install refused"), "{shown}");
    assert!(
        shown.contains(fixture.dir.path().to_str().unwrap()),
        "{shown}"
    );
    assert!(shown.contains(env!("CARGO_BIN_EXE_curie")), "{shown}");
}

#[test]
fn a_chained_usage_failure_keeps_its_exit_class_and_one_error() {
    let fixture = Fixture::new();
    fs::remove_file(fixture.dir.path().join("app.pem")).unwrap();
    let (code, shown) = fixture.run(true, &["--color", "never"]);
    assert_eq!(code, 2, "{shown}");
    assert_eq!(shown.matches("Error:").count(), 1, "{shown}");
    assert!(
        shown.contains("Configuring factory intake failed"),
        "{shown}"
    );
    assert!(shown.contains("Fix:"), "{shown}");
}

#[test]
fn poll_quickstart_ignores_an_exported_webhook_secret() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(false, &["--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    let output = fixture
        .command(true, &["--color", "never"])
        .env(
            curie::factory_intake::WEBHOOK_SECRET_ENV,
            "dev-webhook-secret",
        )
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_short_pass(
        output.status.code().unwrap_or(1),
        &shown,
        true,
        "pipes with exported webhook secret",
    );
    let documents = fs::read_to_string(fixture.dir.path().join("helm-values")).unwrap();
    let intake = documents
        .lines()
        .find_map(|line| {
            let document: String = serde_json::from_str(line).unwrap();
            let values: Value = serde_norway::from_str(&document).unwrap();
            (values.pointer("/api/githubFactoryIntake") == Some(&json!("poll"))).then_some(values)
        })
        .expect("the actual poll intake values document was passed to Helm");
    assert!(
        intake["api"].get("githubWebhookSecret").is_none(),
        "poll intake must not write a webhook secret"
    );
}

#[test]
fn gvisor_admission_inference_remains_visible_in_default_output() {
    let fixture = Fixture::new();
    let output = fixture
        .command(false, &["--context", "acme-cluster", "--color", "never"])
        .env("QUICKSTART_GVISOR_RETRY", "1")
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{shown}");
    assert!(shown.contains("inferred that the cluster has no `gvisor` RuntimeClass from admission; applying `--set security.gvisor.mode=off`"),"{shown}");
}

#[test]
fn a_json_second_pass_is_one_ready_object_and_custom_rerun_flags_are_retained() {
    let fixture = Fixture::new();
    let output = fixture.command(true, &["--json"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).expect("one JSON object");
    assert_eq!(body["phase"], "ready");
    let mut cmd = fixture.command(
        false,
        &[
            "--namespace",
            "acme-dev",
            "--release",
            "acme-platform",
            "--model",
            "acme/model",
            "--org",
            "acme",
            "--color",
            "never",
        ],
    );
    let output = cmd
        .env("QUICKSTART_NAMESPACE", "acme-dev")
        .env("QUICKSTART_RELEASE", "acme-platform")
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{shown}");
    let rerun = shown.lines().find(|line| line.contains("Rerun:")).unwrap();
    for flag in [
        "--namespace acme-dev",
        "--release acme-platform",
        "--model acme/model",
        "--org acme",
    ] {
        assert!(rerun.contains(flag), "{rerun}");
    }
}
