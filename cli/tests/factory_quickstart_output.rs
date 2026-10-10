//! Capture the candidate quickstart binary at its process boundary, including
//! a real terminal. Only its external Helm, Kubernetes, GitHub, registry and
//! platform API peers are fixtures. The Kubernetes wire shapes follow the
//! Deployment and Pod references cited in `cluster_convergence.rs`; GitHub
//! responses follow the REST App endpoints cited in `factory_github_app.rs`.
//! Bot user ids follow https://docs.github.com/en/rest/users/users#get-a-user.
//! OpenRouter responses follow the `/key` and `/credits` shapes recorded from
//! the real API on 2026-10-04 and cited in `openrouter_credit.rs` (#3935).
//! Namespace labels follow the Namespace metadata object, including the
//! `kubernetes.io/metadata.name` label Kubernetes writes on every namespace:
//! https://kubernetes.io/docs/reference/labels-annotations-taints/#kubernetesiometadataname

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
def deep_merge(dst, src):
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            deep_merge(dst[key], value)
        else:
            dst[key] = value
def apply_set(doc, expression):
    for item in expression.split(','):
        if '=' not in item or '[' in item or '{' in item:
            continue
        path, raw = item.split('=', 1)
        cursor = doc
        parts = path.split('.')
        for part in parts[:-1]:
            nxt = cursor.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cursor[part] = nxt
            cursor = nxt
        cursor[parts[-1]] = {'true': True, 'false': False, 'null': None}.get(raw, raw)
def record_upgrade(args):
    try:
        doc = json.loads((root / 'values').read_text())
    except Exception:
        doc = {}
    if not isinstance(doc, dict):
        doc = {}
    index = 0
    while index < len(args):
        if args[index] in ['-f', '--values'] and index + 1 < len(args):
            try:
                overlay = json.loads(Path(args[index + 1]).read_text())
            except Exception:
                overlay = None
            if isinstance(overlay, dict):
                deep_merge(doc, overlay)
            index += 2
            continue
        if args[index] in ['--set', '--set-string'] and index + 1 < len(args):
            apply_set(doc, args[index + 1])
            index += 2
            continue
        index += 1
    (root / 'values').write_text(json.dumps(doc))
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
    if args[:2] == ['get','values']:
        if os.environ.get('QUICKSTART_REMOVE_KEY_AFTER_PREFLIGHT') == '1':
            (root / 'app.pem').unlink(missing_ok=True)
        print((root / 'values').read_text()); sys.exit(0)
    if args[:2] == ['get','hooks']: sys.exit(0)
    if args[:2] == ['get','metadata']: emit({'appVersion':os.environ['QUICKSTART_VERSION'],'version':os.environ['QUICKSTART_VERSION'],'chart':'curie-'+os.environ['QUICKSTART_VERSION']})
    if args[:2] == ['get','manifest']: emit(deployment)
    if args[0] == 'history': emit([{'revision':1,'status':os.environ.get('QUICKSTART_REVISION_STATUS','deployed')}])
    if args[0] == 'status': emit({'version':1,'info':{'status':'deployed'},'hooks':[]})
    if args[:2] == ['show','chart']: print('name: curie\nversion: '+os.environ['QUICKSTART_VERSION']+'\nappVersion: '+os.environ['QUICKSTART_VERSION']); sys.exit(0)
    if args[0] == 'template':
        template = args[args.index('--show-only')+1]
        if 'priorityclass' in template:
            role = 'platform' if args[-1] == 'priorityClasses.sandbox.create=false' else 'sandbox'
            emit({'kind':'PriorityClass','metadata':{'name':'curie-'+role}})
        if 'preflight-gvisor' in template and os.environ.get('QUICKSTART_GVISOR_RETRY') == '1' and 'security.gvisor.mode=off' not in args:
            emit({'kind':'Job','metadata':{'name':release+'-preflight-gvisor'},'spec':{'template':{'spec':{'runtimeClassName':'gvisor'}}}})
        print('Error: could not find template '+template+' in chart', file=sys.stderr); sys.exit(1)
    if args[0] == 'upgrade':
        for index, argument in enumerate(args[:-1]):
            if argument in ['-f','--values']:
                with (root / 'helm-values').open('a') as values_log:
                    values_log.write(json.dumps(Path(args[index+1]).read_text()) + '\n')
        if os.environ.get('QUICKSTART_FAIL') == 'up':
            print('Error: fixture install refused at '+str(root / 'cached-chart'), file=sys.stderr); sys.exit(1)
        if os.environ.get('QUICKSTART_FAIL') == 'merged' and '--install' not in args and not (root / 'merged-failed').exists():
            (root / 'merged-failed').touch()
            print('Error: fixture merged upgrade refused', file=sys.stderr); sys.exit(1)
        if os.environ.get('QUICKSTART_GVISOR_RETRY') == '1' and not (root / 'retried').exists():
            (root / 'retried').touch()
            time.sleep(10)
            print('Error: pods "fixture" is forbidden: RuntimeClass "gvisor" not found', file=sys.stderr); sys.exit(1)
        record_upgrade(args)
        print('Release accepted'); sys.exit(0)
    if args[0] == 'uninstall': sys.exit(0)
if tool == 'kubectl':
    if args[:2] == ['config','view']:
        emit({'clusters':[{'cluster':{'server':'https://127.0.0.1:9'}}],'users':[{'user':{'token':'fixture'}}]})
    if 'scale' in args: print('deployment.apps/coredns scaled'); sys.exit(0)
    if 'apply' in args: sys.stdin.read(); print('secret configured'); sys.exit(0)
    if 'delete' in args and 'sandboxclaim' in args: print('No resources found'); sys.exit(0)
    if args and args[0] == 'api-resources': print('configmaps\nserviceaccounts'); sys.exit(0)
    if 'rollout' in args or 'label' in args or 'patch' in args: print('fixture command completed'); sys.exit(0)
    if 'get' in args:
        ix = args.index('get'); kind = args[ix+1]; name = args[ix+2] if len(args) > ix+2 else ''
        if kind == 'runtimeclass': print('Error from server (Forbidden): runtimeclasses.node.k8s.io "'+name+'" is forbidden: User "fixture" cannot get resource "runtimeclasses" in API group "node.k8s.io" at the cluster scope', file=sys.stderr); sys.exit(1)
        if kind == 'apiservices.apiregistration.k8s.io': emit({'apiVersion':'v1','kind':'List','items':[]})
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
            labels = {'curietech.ai/created-by':release,'curietech.ai/created-in':ns}
            if name == ns and os.environ.get('QUICKSTART_NAMESPACE_LABELS'):
                labels = json.loads(os.environ['QUICKSTART_NAMESPACE_LABELS'])
            emit({'apiVersion':'v1','kind':'Namespace','metadata':{'name':name,'uid':'fixture-'+name,'resourceVersion':'1','labels':labels}})
        if kind == 'configmaps':
            items = [{'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'acme-settings','namespace':ns},'data':{'setting':'value'}}] if os.environ.get('QUICKSTART_NON_DEFAULT_OBJECTS') == '1' else []
            emit({'apiVersion':'v1','kind':'List','items':items})
        if kind == 'serviceaccounts': emit({'apiVersion':'v1','kind':'List','items':[]})
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

/// The fake account balance a default fixture reports: ample, so the key's
/// 73.49 USD limit is the remaining credit and no warning prints.
const AMPLE_ACCOUNT_LEFT: f64 = 200.0;
const KEY_LIMIT_LEFT: f64 = 73.49;

/// A fake OpenRouter with the recorded 2026-10-04 shapes. The key has 73.49
/// USD of its limit left; the account has `account_left` USD left.
fn openrouter(account_left: f64) -> MockServer {
    serve(move |req| {
        let path = req.path.split('?').next().unwrap();
        if req.method == "GET" && path.ends_with("/key") {
            Response::json(
                200,
                &json!({"data":{"label":"sk-or-v1-PLACEHOLDER","is_management_key":false,"is_provisioning_key":false,"limit":300,"limit_reset":null,"limit_remaining":KEY_LIMIT_LEFT,"include_byok_in_limit":false,"usage":226.51,"usage_daily":0.11,"usage_weekly":19.44,"usage_monthly":5.74,"is_free_tier":false,"expires_at":null}}).to_string(),
            )
        } else if req.method == "GET" && path.ends_with("/credits") {
            Response::json(
                200,
                &json!({"data":{"total_credits":800.0,"total_usage":800.0 - account_left}})
                    .to_string(),
            )
        } else {
            Response::json(404, r#"{"error":{"message":"Not Found","code":404}}"#)
        }
    })
}

struct Fixture {
    dir: tempfile::TempDir,
    github: MockServer,
    platform: MockServer,
    registry: MockServer,
    openrouter: MockServer,
}

impl Fixture {
    fn new() -> Self {
        Self::with_credit(AMPLE_ACCOUNT_LEFT)
    }

    fn with_credit(account_left: f64) -> Self {
        Self::with_bot_lookup(account_left, 200, r#"{"id":123}"#)
    }

    fn with_bot_lookup(account_left: f64, bot_status: u16, bot_body: &'static str) -> Self {
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
        let github = serve(move |req| {
            let path = req.path.split('?').next().unwrap();
            match (req.method.as_str(), path) {
                ("GET", "/app") => Response::json(
                    200,
                    r#"{"id":1234567,"slug":"acme-factory","name":"acme-factory"}"#,
                ),
                ("GET", "/users/acme-factory[bot]") => {
                    assert!(req.header("authorization").is_none());
                    Response::json(bot_status, bot_body)
                }
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
                ("GET", "/repos/acme/widgets") => {
                    Response::json(200, r#"{"default_branch":"main"}"#)
                }
                ("GET", "/repos/acme/widgets/commits/main") => {
                    Response::json(200, r#"{"sha":"1111111111111111111111111111111111111111"}"#)
                }
                ("GET", "/repos/acme/widgets/contents") => Response::json(200, "[]"),
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
            openrouter: openrouter(account_left),
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
            .env("CURIE_OPENROUTER_API_URL", &self.openrouter.base_url)
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
    // The ready pass grew by exactly one line, the reviewer model and run
    // credit line (#3935).
    assert!(
        shown.lines().count() <= if second { 19 } else { 14 },
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
    assert_eq!(
        shown
            .lines()
            .filter(|line| *line == "Installing Curie")
            .count(),
        usize::from(!second),
        "{shown}"
    );
    if second {
        assert!(
            shown.contains("skipping cluster up"),
            "second pass still installed: {shown}"
        );
    } else {
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
    }
    for step in ["Scaling CoreDNS"]
        .into_iter()
        .chain((!second).then_some("Installing Curie"))
        .chain(
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
        )
    {
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
        assert_eq!(
            shown
                .lines()
                .filter(|line| line.contains("Reviewers run openai/gpt-6.1-sol")
                    && line.contains("5 USD"))
                .count(),
            1,
            "{shown}"
        );
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

fn helm_upgrade_calls(fixture: &Fixture) -> Vec<Vec<String>> {
    let text = fs::read_to_string(fixture.dir.path().join("calls")).unwrap_or_default();
    text.lines()
        .filter_map(|line| {
            let call: Vec<String> = serde_json::from_str(line).ok()?;
            (call.first().map(String::as_str) == Some("helm")
                && call.get(1).map(String::as_str) == Some("upgrade"))
            .then_some(call)
        })
        .collect()
}

fn namespace_mutations(fixture: &Fixture) -> Vec<Vec<String>> {
    let text = fs::read_to_string(fixture.dir.path().join("calls")).unwrap_or_default();
    text.lines()
        .filter_map(|line| {
            let call: Vec<String> = serde_json::from_str(line).ok()?;
            (call.first().map(String::as_str) == Some("kubectl")
                && matches!(
                    call.get(1).map(String::as_str),
                    Some(
                        "patch" | "label" | "annotate" | "create" | "delete" | "apply" | "replace"
                    )
                ))
            .then_some(call)
        })
        .collect()
}

fn intake_documents(fixture: &Fixture) -> Vec<Value> {
    let text = fs::read_to_string(fixture.dir.path().join("helm-values")).unwrap_or_default();
    text.lines()
        .filter_map(|line| {
            let document: String = serde_json::from_str(line).ok()?;
            let values: Value = serde_json::from_str(&document).ok()?;
            (values.pointer("/api/githubFactoryIntake") == Some(&json!("poll"))).then_some(values)
        })
        .collect()
}

#[test]
fn quickstart_second_pass_applies_the_app_bot_publication_identity() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(true, &["--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    let documents = intake_documents(&fixture);
    assert!(!documents.is_empty(), "factory intake document missing");
    for values in documents {
        assert_eq!(
            values.pointer("/worker/publication/gitUserName"),
            Some(&json!("acme-factory[bot]"))
        );
        assert_eq!(
            values.pointer("/worker/publication/gitUserEmail"),
            Some(&json!("123+acme-factory[bot]@users.noreply.github.com"))
        );
    }
}

#[test]
fn quickstart_second_pass_keeps_each_recorded_operator_publication_identity() {
    for publication in [
        json!({"gitUserName":"Operator"}),
        json!({"gitUserEmail":"operator@example.com"}),
        json!({"gitUserName":"Operator","gitUserEmail":"operator@example.com"}),
    ] {
        let fixture = Fixture::new();
        let path = fixture.dir.path().join("values");
        let mut values: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        values["worker"] = json!({"publication":publication});
        fs::write(&path, values.to_string()).unwrap();
        let (code, shown) = fixture.run(true, &["--color", "never"]);
        assert_eq!(code, 0, "{shown}");
        let applied: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(applied.pointer("/worker/publication"), Some(&publication));
        assert!(
            intake_documents(&fixture)
                .iter()
                .all(|values| values.pointer("/worker/publication").is_none()),
            "factory intake must preserve both fields when either operator field is set"
        );
    }
}

#[test]
fn quickstart_keeps_numeric_and_empty_operator_publication_values() {
    for publication in [
        json!({"gitUserName":123}),
        json!({"gitUserName":""}),
        json!({"gitUserEmail":""}),
    ] {
        let fixture = Fixture::new();
        let (code, shown) = fixture.run(false, &["--color", "never"]);
        assert_eq!(code, 0, "{shown}");
        let path = fixture.dir.path().join("values");
        let mut values: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        values["worker"] = json!({"publication":publication});
        fs::write(&path, values.to_string()).unwrap();
        let (code, shown) = fixture.run(true, &["--color", "never"]);
        assert_eq!(code, 0, "{shown}");
        let applied: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(applied.pointer("/worker/publication"), Some(&publication));
        assert!(
            intake_documents(&fixture)
                .iter()
                .all(|values| values.pointer("/worker/publication").is_none()),
            "factory intake must preserve every explicit operator value"
        );
    }
}

#[test]
fn quickstart_bot_lookup_failure_exits_three_before_any_helm_call() {
    for (status, body) in [
        // An invalid HTTP status line exercises the transport error path.
        (0, "{}"),
        (404, r#"{"message":"Not Found"}"#),
        (503, r#"{"message":"Unavailable"}"#),
        (200, "not JSON"),
        (200, r#"{"login":"acme-factory[bot]"}"#),
        (200, r#"{"id":"123"}"#),
    ] {
        let fixture = Fixture::with_bot_lookup(AMPLE_ACCOUNT_LEFT, status, body);
        let (code, shown) = fixture.run(true, &["--color", "never"]);
        assert_eq!(code, 3, "{shown}");
        assert!(shown.contains("/users/acme-factory[bot]"), "{shown}");
        assert!(
            shown.contains("--set worker.publication.gitUserEmail="),
            "{shown}"
        );
        let calls = fs::read_to_string(fixture.dir.path().join("calls")).unwrap_or_default();
        assert!(
            calls.lines().all(|line| {
                let call: Vec<String> = serde_json::from_str(line).unwrap();
                call.first().map(String::as_str) != Some("helm")
            }),
            "bot lookup failure must precede all Helm calls: {calls}"
        );
    }
}

#[test]
fn quickstart_foreign_labels_refusal_names_only_foreign_keys_and_their_removal() {
    let fixture = Fixture::new();
    let output = fixture
        .command(
            false,
            &[
                "--context",
                "acme-cluster",
                "--namespace",
                "acme-dev",
                "--color",
                "never",
            ],
        )
        .env("QUICKSTART_NAMESPACE", "acme-dev")
        .env(
            "QUICKSTART_NAMESPACE_LABELS",
            json!({"foo":"bar","kubernetes.io/metadata.name":"acme-dev"}).to_string(),
        )
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{shown}");
    assert!(shown.contains("foreign labels (foo)"), "{shown}");
    assert!(
        shown.contains("kubectl --context acme-cluster label namespace acme-dev foo-"),
        "{shown}"
    );
    assert!(shown.contains("--namespace"), "{shown}");
    assert!(!shown.contains("--adopt"), "{shown}");
    assert!(!shown.contains("kubernetes.io/metadata.name"), "{shown}");
    assert!(helm_upgrade_calls(&fixture).is_empty(), "{shown}");
    assert!(
        namespace_mutations(&fixture).is_empty(),
        "foreign-label refusal mutated the namespace: {shown}"
    );
}

#[test]
fn quickstart_ownership_and_contents_refusals_suggest_another_namespace() {
    for (labels, non_default_objects, expected) in [
        (
            json!({"curietech.ai/created-by":"acme-other","kubernetes.io/metadata.name":"curie"}),
            false,
            "incomplete or foreign ownership labels",
        ),
        (
            json!({"kubernetes.io/metadata.name":"curie"}),
            true,
            "contains non-default objects",
        ),
    ] {
        let fixture = Fixture::new();
        let output = fixture
            .command(false, &["--context", "acme-cluster", "--color", "never"])
            .env("QUICKSTART_NAMESPACE_LABELS", labels.to_string())
            .env(
                "QUICKSTART_NON_DEFAULT_OBJECTS",
                if non_default_objects { "1" } else { "0" },
            )
            .output()
            .unwrap();
        let shown = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.status.success(), "{shown}");
        assert!(shown.contains(expected), "{shown}");
        assert!(shown.contains("--namespace"), "{shown}");
        assert!(!shown.contains("--adopt"), "{shown}");
        assert!(!shown.contains("label namespace"), "{shown}");
        assert!(helm_upgrade_calls(&fixture).is_empty(), "{shown}");
        assert!(
            namespace_mutations(&fixture).is_empty(),
            "{expected} refusal mutated the namespace: {shown}"
        );
    }
}

#[test]
fn quickstart_adopts_an_empty_namespace_with_only_its_metadata_name_label() {
    let fixture = Fixture::new();
    let output = fixture
        .command(false, &["--context", "acme-cluster", "--color", "never"])
        .env(
            "QUICKSTART_NAMESPACE_LABELS",
            json!({"kubernetes.io/metadata.name":"curie"}).to_string(),
        )
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{shown}");
    assert!(shown.contains("Installing Curie"), "{shown}");
    let calls = fs::read_to_string(fixture.dir.path().join("calls")).unwrap();
    assert!(
        calls.lines().any(|line| {
            let call: Vec<String> = serde_json::from_str(line).unwrap();
            call.first().map(String::as_str) == Some("kubectl")
                && call.iter().any(|arg| arg == "patch")
                && call.iter().any(|arg| arg == "namespace")
                && call.iter().any(|arg| arg == "curie")
        }),
        "metadata-only namespace did not reach guarded adoption: {calls}"
    );
    assert_eq!(helm_upgrade_calls(&fixture).len(), 1, "{shown}");
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
    let upgrades = helm_upgrade_calls(&fixture);
    assert_eq!(
        upgrades.len(),
        2,
        "first pass installs once and the second pass upgrades once: {upgrades:?}"
    );
    assert!(
        upgrades[0].iter().any(|arg| arg == "--install"),
        "the first pass is cluster up: {upgrades:?}"
    );
    assert!(
        !upgrades[1]
            .iter()
            .any(|arg| arg == "--install" || arg == "--reset-then-reuse-values"),
        "the second pass is one reuse-values upgrade: {:?}",
        upgrades[1]
    );
    assert!(
        upgrades[1].iter().any(|arg| arg == "--reuse-values"),
        "{:?}",
        upgrades[1]
    );
    let documents = intake_documents(&fixture);
    assert_eq!(documents.len(), 1, "{documents:?}");
    let image = documents[0]
        .pointer("/agentSandbox/runnerImages/dark-factory")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    assert!(
        image.contains("@sha256:"),
        "the one upgrade must bind the runner image: {documents:?}"
    );
}

#[test]
fn a_failed_merged_upgrade_rerun_applies_the_same_values() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(false, &["--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    let failed = fixture
        .command(true, &["--color", "never"])
        .env("QUICKSTART_FAIL", "merged")
        .output()
        .unwrap();
    let failed_shown = format!(
        "{}{}",
        String::from_utf8_lossy(&failed.stdout),
        String::from_utf8_lossy(&failed.stderr)
    );
    assert!(!failed.status.success(), "{failed_shown}");
    assert!(
        failed_shown.contains("Configuring factory intake failed"),
        "{failed_shown}"
    );
    let attempted = intake_documents(&fixture);
    assert_eq!(attempted.len(), 1, "{attempted:?}");
    let again = fixture
        .command(true, &["--color", "never"])
        .env("QUICKSTART_FAIL", "merged")
        .output()
        .unwrap();
    let again_shown = format!(
        "{}{}",
        String::from_utf8_lossy(&again.stdout),
        String::from_utf8_lossy(&again.stderr)
    );
    assert!(again.status.success(), "{again_shown}");
    let applied = intake_documents(&fixture);
    assert_eq!(applied.len(), 2, "{applied:?}");
    assert_eq!(
        attempted[0], applied[1],
        "rerun drifted from the failed upgrade"
    );
    let upgrades = helm_upgrade_calls(&fixture);
    let finish = upgrades
        .iter()
        .filter(|argv| !argv.iter().any(|arg| arg == "--install"))
        .count();
    assert_eq!(
        finish, 2,
        "failed attempt plus the converging rerun: {upgrades:?}"
    );
}

#[test]
fn a_failed_revision_still_runs_cluster_up() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(false, &["--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    let before = helm_upgrade_calls(&fixture).len();
    let output = fixture
        .command(false, &["--color", "never"])
        .env("QUICKSTART_REVISION_STATUS", "failed")
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{shown}");
    assert!(shown.contains("Installing Curie"), "{shown}");
    assert!(!shown.contains("skipping cluster up"), "{shown}");
    let upgrades = helm_upgrade_calls(&fixture);
    assert!(
        upgrades.len() > before
            && upgrades
                .last()
                .unwrap()
                .iter()
                .any(|arg| arg == "--install"),
        "a failed revision must be repaired by cluster up: {upgrades:?}"
    );
}

#[test]
fn a_different_runner_base_is_refused_before_the_binding_upgrade() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(false, &["--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    let mut values: Value =
        serde_json::from_str(&fs::read_to_string(fixture.dir.path().join("values")).unwrap())
            .unwrap();
    values["agentSandbox"]["runner"]["digest"] =
        json!("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    fixture.record_values(values);
    let before = helm_upgrade_calls(&fixture).len();
    let output = fixture
        .command(true, &["--color", "never"])
        .output()
        .unwrap();
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{shown}");
    assert!(shown.contains("Checking the runner base failed"), "{shown}");
    assert!(
        shown.contains("another base") || shown.contains("was built on"),
        "{shown}"
    );
    assert_eq!(
        helm_upgrade_calls(&fixture).len(),
        before,
        "the binding upgrade ran before the refusal: {shown}"
    );
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
    // A fresh fixture is not yet at the target release, so this failure still
    // enters cluster up. The debug run above already is, and would skip it.
    let fixture = Fixture::new();
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
    // The parent now validates the key before any Helm read. Simulate a key
    // removed after that valid preflight so the child still exercises the
    // chained usage-error boundary, rather than bypassing it with an earlier
    // parent validation error.
    let output = fixture
        .command(true, &["--color", "never"])
        .env("QUICKSTART_REMOVE_KEY_AFTER_PREFLIGHT", "1")
        .output()
        .unwrap();
    let code = output.status.code().unwrap_or(1);
    let shown = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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

#[test]
fn low_openrouter_credit_warns_before_the_factory_deploys() {
    let fixture = Fixture::with_credit(1.0);
    let (code, shown) = fixture.run(true, &["--color", "never"]);
    assert_eq!(code, 0, "a low balance warns, it never fails: {shown}");
    let lines: Vec<&str> = shown.lines().collect();
    let warning = lines
        .iter()
        .position(|line| {
            line.contains("OpenRouter credit left") && line.contains("openai/gpt-6.1-sol")
        })
        .unwrap_or_else(|| panic!("no credit warning: {shown}"));
    let deploy = lines
        .iter()
        .position(|line| *line == "Deploying dark factory")
        .unwrap_or_else(|| panic!("no deploy step: {shown}"));
    assert!(
        warning < deploy,
        "the warning must precede the deploy: {shown}"
    );
    assert!(!shown.contains("OpenRouter credit not checked"), "{shown}");
    let requests = fixture.openrouter.recorded();
    assert!(
        !requests.is_empty(),
        "the credit check never reached OpenRouter"
    );
    for request in &requests {
        assert_eq!(
            request.header("authorization"),
            Some("Bearer sk-or-fixture"),
            "the check must use the bound model credential: {request:?}"
        );
    }
}

#[test]
fn ample_openrouter_credit_prints_no_credit_line() {
    let fixture = Fixture::new();
    let (code, shown) = fixture.run(true, &["--color", "never"]);
    assert_eq!(code, 0, "{shown}");
    assert!(!shown.contains("OpenRouter credit left"), "{shown}");
    assert!(!shown.contains("OpenRouter credit not checked"), "{shown}");
    assert!(
        !fixture.openrouter.recorded().is_empty(),
        "the credit check never reached OpenRouter"
    );
}

#[test]
fn factory_quickstart_direct_anthropic_credentials_select_native_sonnet_and_skip_credit() {
    // The runner's sdk_auth.py accepts both synthetic Anthropic shapes.
    // This process path exercises quickstart validation, model resolution,
    // child cluster-up argv, and the real OpenRouter client boundary.
    for key in ["sk-ant-api03-PLACEHOLDER", "sk-ant-oat01-PLACEHOLDER"] {
        let fixture = Fixture::new();
        let output = fixture
            .command(true, &["--json"])
            .env("CURIE_CREDENTIALS", key)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let ready: Value = serde_json::from_slice(&output.stdout).expect("one ready JSON object");
        assert_eq!(ready["phase"], "ready");
        assert_eq!(ready["credit_remaining_usd"], Value::Null);
        // The settled display value remains the OpenRouter reviewer id.
        assert_eq!(ready["reviewer_model"], "openai/gpt-6.1-sol");
        let values: Value =
            serde_json::from_slice(&fs::read(fixture.dir.path().join("values")).unwrap()).unwrap();
        assert_eq!(
            values.pointer("/agentSandbox/runner/model"),
            Some(&json!("claude-sonnet-5-5")),
            "the model selected by quickstart must reach the install"
        );
        assert!(
            fixture.openrouter.recorded().is_empty(),
            "a direct Anthropic credential must never reach an OpenRouter credit endpoint"
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains(key));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(key));
    }
}

#[test]
fn factory_quickstart_direct_anthropic_keeps_every_explicit_model_including_the_old_default() {
    for model in ["acme/direct-model", "z-ai/glm-5.3-flash"] {
        let fixture = Fixture::new();
        let output = fixture
            .command(true, &["--json", "--model", model])
            .env("CURIE_CREDENTIALS", "sk-ant-api03-PLACEHOLDER")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let ready: Value = serde_json::from_slice(&output.stdout).expect("one ready JSON object");
        assert_eq!(ready["phase"], "ready");
        let values: Value =
            serde_json::from_slice(&fs::read(fixture.dir.path().join("values")).unwrap()).unwrap();
        assert_eq!(
            values.pointer("/agentSandbox/runner/model"),
            Some(&json!(model))
        );
        assert!(fixture.openrouter.recorded().is_empty());
    }
}

#[test]
fn factory_quickstart_direct_anthropic_dry_run_plans_the_credential_default() {
    let fixture = Fixture::new();
    let output = fixture
        .command(false, &["--dry-run", "--json"])
        .env("CURIE_CREDENTIALS", "sk-ant-api03-PLACEHOLDER")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).expect("one plan JSON object");
    let lines = body["plan"].as_array().expect("plan lines");
    assert!(lines.iter().any(|line| {
        let line = line.as_str().unwrap();
        line.contains("curie cluster up") && line.contains("--model claude-sonnet-5-5")
    }));
    assert!(fixture.openrouter.recorded().is_empty());
    assert!(helm_upgrade_calls(&fixture).is_empty());
}

#[test]
fn factory_quickstart_invalid_model_credential_never_installs_or_checks_credit() {
    for key in ["invalid-credential", "sk-or-", "sk-ant-"] {
        let fixture = Fixture::new();
        let output = fixture
            .command(false, &["--context", "acme-cluster", "--json"])
            .env("CURIE_CREDENTIALS", key)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "invalid credential is a usage error"
        );
        assert!(helm_upgrade_calls(&fixture).is_empty());
        assert!(namespace_mutations(&fixture).is_empty());
        assert!(fixture.openrouter.recorded().is_empty());
    }
}

#[test]
fn a_json_ready_object_names_the_reviewer_model_and_run_credit() {
    let fixture = Fixture::new();
    let output = fixture.command(true, &["--json"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let body: Value = serde_json::from_slice(&output.stdout).expect("one JSON object");
    assert_eq!(body["phase"], "ready", "{body}");
    assert_eq!(body["reviewer_model"], "openai/gpt-6.1-sol", "{body}");
    assert_eq!(body["run_credit_usd"], json!(5.0), "{body}");
    let remaining = body["credit_remaining_usd"]
        .as_f64()
        .unwrap_or_else(|| panic!("credit_remaining_usd is not a number: {body}"));
    assert!(
        (remaining - KEY_LIMIT_LEFT.min(AMPLE_ACCOUNT_LEFT)).abs() < 1e-9,
        "{body}"
    );
}

const SAVED_KEY: &str = "sk-or-saved-PLACEHOLDER";

impl Fixture {
    /// Saves `SAVED_KEY` as CURIE_CREDENTIALS through the real `secrets set`
    /// verb, into a config dir private to this fixture.
    fn save_credential(&self) {
        let seed = Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(["secrets", "set", "CURIE_CREDENTIALS", "--from-env", "SEED"])
            .env("CURIE_CONFIG_DIR", self.dir.path().join("cfg"))
            .env("HOME", self.dir.path())
            .env("SEED", SAVED_KEY)
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .output()
            .unwrap();
        assert!(
            seed.status.success(),
            "seed saved credential: {}",
            String::from_utf8_lossy(&seed.stderr)
        );
    }

    /// The release's recorded helm values, as `helm get values` answers them.
    fn record_values(&self, values: Value) {
        fs::write(self.dir.path().join("values"), values.to_string()).unwrap();
    }

    /// A second pass whose shell carries `explicit` as CURIE_CREDENTIALS, or no
    /// model credential at all, with the fixture's saved credential store.
    fn run_with_saved(&self, explicit: Option<&str>) -> (i32, String) {
        let mut cmd = self.command(true, &["--color", "never"]);
        cmd.env("CURIE_CONFIG_DIR", self.dir.path().join("cfg"))
            .env_remove("CURIE_MODEL_CREDENTIALS");
        match explicit {
            Some(key) => cmd.env("CURIE_CREDENTIALS", key),
            None => cmd.env_remove("CURIE_CREDENTIALS"),
        };
        let output = cmd.stdin(Stdio::null()).output().unwrap();
        (
            output.status.code().unwrap_or(1),
            format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    }

    fn openrouter_bearers(&self) -> Vec<String> {
        self.openrouter
            .recorded()
            .iter()
            .filter_map(|request| request.header("authorization").map(str::to_string))
            .collect()
    }
}

#[test]
fn a_saved_key_is_not_checked_when_the_release_keeps_its_recorded_credential() {
    // #3935 review: with no shell credential and a release that already records
    // a real model, `cluster up` keeps the release's recorded credential, so the
    // saved key is not the one the reviewers spend. Checking it would report
    // another key's balance as the factory's.
    let fixture = Fixture::new();
    fixture.save_credential();
    // The default fixture release records fakeModel false; stated here so the
    // test does not lean on that default.
    let mut values: Value =
        serde_json::from_str(&fs::read_to_string(fixture.dir.path().join("values")).unwrap())
            .unwrap();
    values["agentSandbox"]["runner"]["fakeModel"] = json!(false);
    fixture.record_values(values);

    let (code, shown) = fixture.run_with_saved(None);

    assert_eq!(code, 0, "{shown}");
    assert_eq!(
        shown.matches("OpenRouter credit not checked").count(),
        1,
        "{shown}"
    );
    let bearers = fixture.openrouter_bearers();
    assert!(
        !bearers
            .iter()
            .any(|bearer| bearer == &format!("Bearer {SAVED_KEY}")),
        "the saved key was checked although the release keeps its own: {bearers:?}"
    );
}

#[test]
fn factory_quickstart_rerun_preserves_the_recorded_native_model_without_a_local_credential() {
    let fixture = Fixture::new();
    let mut values: Value =
        serde_json::from_slice(&fs::read(fixture.dir.path().join("values")).unwrap()).unwrap();
    values["agentSandbox"]["runner"]["fakeModel"] = json!(false);
    values["agentSandbox"]["runner"]["model"] = json!("claude-sonnet-5-5");
    fixture.record_values(values);

    let output = fixture
        .command(true, &["--context", "acme-cluster", "--json"])
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("CURIE_MODEL_CREDENTIALS")
        .env("CURIE_CONFIG_DIR", fixture.dir.path().join("cfg"))
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let ready: Value = serde_json::from_slice(&output.stdout).expect("one ready JSON object");
    assert_eq!(ready["phase"], "ready");
    let upgrades = helm_upgrade_calls(&fixture);
    assert_eq!(
        upgrades.len(),
        1,
        "only the merged intake upgrade is needed: {upgrades:?}"
    );
    assert!(
        upgrades[0].iter().any(|arg| arg == "--reuse-values"),
        "the existing release must be reused: {upgrades:?}"
    );
    assert!(
        !upgrades[0].iter().any(|arg| arg == "--install"),
        "a credential-free rerun must not replace the native model: {upgrades:?}"
    );
    let recorded: Value =
        serde_json::from_slice(&fs::read(fixture.dir.path().join("values")).unwrap()).unwrap();
    assert_eq!(
        recorded.pointer("/agentSandbox/runner/model"),
        Some(&json!("claude-sonnet-5-5"))
    );
    assert!(fixture.openrouter.recorded().is_empty());
}

#[test]
fn an_explicit_key_is_checked_even_when_a_saved_key_and_a_recorded_model_exist() {
    // Liveness: the shell credential is the one `cluster up` deploys, so it is
    // the one checked, whatever is saved or recorded.
    let fixture = Fixture::new();
    fixture.save_credential();

    let (code, shown) = fixture.run_with_saved(Some("sk-or-explicit-PLACEHOLDER"));

    assert_eq!(code, 0, "{shown}");
    assert!(!shown.contains("OpenRouter credit not checked"), "{shown}");
    let bearers = fixture.openrouter_bearers();
    assert!(
        !bearers.is_empty(),
        "the credit check never reached OpenRouter"
    );
    assert!(
        bearers
            .iter()
            .all(|bearer| bearer == "Bearer sk-or-explicit-PLACEHOLDER"),
        "{bearers:?}"
    );
}

#[test]
fn a_saved_key_is_checked_when_the_release_records_no_model() {
    // Liveness: on a release with no real model recorded, the saved key is the
    // one `cluster up` deploys, so it is the one checked.
    let fixture = Fixture::new();
    fixture.save_credential();
    let mut values: Value =
        serde_json::from_str(&fs::read_to_string(fixture.dir.path().join("values")).unwrap())
            .unwrap();
    values["agentSandbox"]["runner"]
        .as_object_mut()
        .unwrap()
        .remove("fakeModel");
    fixture.record_values(values);

    let (code, shown) = fixture.run_with_saved(None);

    assert_eq!(code, 0, "{shown}");
    assert!(!shown.contains("OpenRouter credit not checked"), "{shown}");
    let bearers = fixture.openrouter_bearers();
    assert!(
        !bearers.is_empty(),
        "the credit check never reached OpenRouter"
    );
    assert!(
        bearers
            .iter()
            .all(|bearer| bearer == &format!("Bearer {SAVED_KEY}")),
        "{bearers:?}"
    );
}
