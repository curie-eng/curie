use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{json, Value};

const IPV4: &str = "1.1.1.1";
const IPV6: &str = "2606:4700:4700::1111";
const MODEL_VALUE: &str = "model-value-for-plan";
const GITHUB_VALUE: &str = "github-value-for-plan";
const RERUN_CREDENTIAL_SENTINEL: &str = "placeholder credential sentinel";
const RERUN_MODEL_SENTINEL: &str = "placeholder/model";
const RERUN_EGRESS_CIDR: &str = "192.0.2.10/32";
const ANTHROPIC_CREDENTIAL: &str = "sk-ant-PLACEHOLDER";
const OPENROUTER_CREDENTIAL: &str = "sk-or-PLACEHOLDER";
const AMBIGUOUS_MODEL_CREDENTIAL: &str = "sk-PLACEHOLDER";
const OVERRIDE_MODEL_SET: &str = "agentSandbox.runner.model=operator/model";
const OVERRIDE_EGRESS_SET: &str = "security.networkPolicy.allowedEgress[0].cidr=198.51.100.20/32";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has a repository parent")
        .to_path_buf()
}

/// The answer a real `kubectl get statefulset -n <ns> -o json` gives when the
/// namespace holds none, and (identically) when the namespace does not exist at
/// all: an empty List, exit 0. Observed behaviour, not an assumption about the
/// implementation: recorded against a real apiserver with kubectl v1.36.2 and
/// written up in the plan for #1351. The whole guard rests on it, so the stub
/// reproduces the shape verbatim.
const KUBECTL_EMPTY_LIST: &str =
    r#"{"apiVersion":"v1","items":[],"kind":"List","metadata":{"resourceVersion":""}}"#;

/// The stderr a real kubectl writes when it cannot reach an apiserver, observed
/// in the same recording (`KUBECONFIG=/dev/null kubectl get statefulset ...`,
/// exit 1). This is the shape a CI runner or a laptop with no cluster produces.
const KUBECTL_UNREACHABLE: &str =
    "The connection to the server localhost:8080 was refused - did you specify the right host or port?";

/// The stderr a real kubectl writes when the caller's identity is denied by
/// RBAC rather than the cluster being unreachable, for a namespaced `get
/// statefulset` list. Verified against a publicly reported operator RBAC
/// denial of this exact shape (a service account listing statefulsets
/// without the role), which read: `statefulsets.apps is forbidden: User
/// "system:serviceaccount:extension-system:keda-operator" cannot list
/// resource "statefulsets" in API group "apps" at the cluster scope`. This
/// string swaps in the namespaced tail (`in the namespace "<ns>"`) that a
/// `-n` scoped list produces instead of "at the cluster scope". Deliberately
/// carries none of `is_connectivity_failure`'s markers (no "connection",
/// "refused", "timeout", "unreachable", "dial tcp", etc.): this is the
/// non-connectivity failure shape the guard must still fail closed on.
const KUBECTL_FORBIDDEN: &str = "Error from server (Forbidden): statefulsets.apps is forbidden: User \"system:serviceaccount:curie:deployer\" cannot list resource \"statefulsets\" in API group \"apps\" in the namespace \"curie\"";

const HELM_UNREACHABLE: &str =
    "Error: Kubernetes cluster unreachable: dial tcp 127.0.0.1:6443: connect: connection refused";
const HELM_FORBIDDEN: &str = "Error: query: secrets is forbidden: User \"system:serviceaccount:curie:deployer\" cannot list resource \"secrets\" in API group \"\" in the namespace \"parity\"";
const HELM_EXECUTABLE_NOT_FOUND: &str = "Error: exec: executable kubelogin file not found in PATH";
const SENTINEL_SEALING_KEY: &str = "SENTINEL_SEALING_PRIVATE_KEY";
const PRESERVED_SEALING_KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
const WARNING_PREFIXED_VALUES: &str = "WARNING: cached discovery information is stale\n{\"sealing\":{\"privateKey\":\"SENTINEL_SEALING_PRIVATE_KEY\"}}";
const ARRAY_VALUES: &str = "[{\"sealing\":{\"privateKey\":\"SENTINEL_SEALING_PRIVATE_KEY\"}}]";

/// One staged object, as `<size> <key>`, the shape both halves of the
/// migration's verify compare: the staging pod's `find -printf` listing and the
/// new store's `aws s3 ls` listing. Identical on both sides means nothing was
/// lost, which is the successful migration the AC2 test asserts.
const STAGED_OBJECT: &str = "100 bundle.tar";

/// Writes `body` to `dir/name` and makes it executable (mode 0o755), the
/// dance both the helm and kubectl stubs need identically. Returns the path
/// written.
fn write_exec(dir: &Path, name: &str, body: &str) -> PathBuf {
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
    fs::write(&path, body).unwrap_or_else(|error| panic!("write {name} stub: {error}"));
    let mut permissions = fs::metadata(&path)
        .unwrap_or_else(|error| panic!("{name} metadata: {error}"))
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions)
        .unwrap_or_else(|error| panic!("make {name} stub executable: {error}"));
    path
}

#[derive(Clone)]
enum HelmValuesResponse {
    Object(Value),
    Null,
    Absent,
    Failure(&'static str),
    RawSuccess(&'static str),
}

struct HelmFixture {
    temp: tempfile::TempDir,
    file: PathBuf,
    values_response: HelmValuesResponse,
    log: PathBuf,
    migration_state: PathBuf,
    last_exec_script: PathBuf,
}

impl HelmFixture {
    fn new(config: &str, values_response: HelmValuesResponse) -> Self {
        let temp = tempfile::tempdir().expect("tempdir");
        let file = temp.path().join("curie.yaml");
        fs::write(&file, config).expect("write curie.yaml");

        let bin_dir = temp.path().join("bin");
        fs::create_dir(&bin_dir).expect("create stub bin directory");
        write_exec(
            &bin_dir,
            "helm",
            r#"#!/bin/sh
if [ -n "${CURIE_TEST_CALL_LOG:-}" ]; then
    printf 'HELM_CALL: %s\n' "$*" >> "$CURIE_TEST_CALL_LOG"
fi
if [ "$1" = get ] && [ "$2" = values ]; then
    case "${CURIE_TEST_HELM_VALUES_MODE:-}" in
        absent)
            printf '%s\n' 'Error: release: not found' >&2
            exit 1
            ;;
        failure)
            printf '%s\n' "$CURIE_TEST_HELM_VALUES" >&2
            exit 1
            ;;
        success)
            printf '%s\n' "$CURIE_TEST_HELM_VALUES"
            exit 0
            ;;
        *)
            printf '%s\n' 'missing values response mode' >&2
            exit 64
            ;;
    esac
fi
if [ "$1" = show ] && [ "$2" = chart ]; then
    # Only `curie diff --chart` reaches this: the default path reports
    # `artifacts::version()` and makes no such call at all. Answered from the
    # real Chart.yaml the flag points at rather than an invented version --
    # the version REPORTED has to be the version RENDERED, and a stub that made
    # one up would hide a regression that swapped them (#1352).
    cat "$3/Chart.yaml"
    exit 0
fi
if [ "$1" = template ]; then
    if [ -n "${CURIE_TEST_REAL_HELM:-}" ]; then
        exec "$CURIE_TEST_REAL_HELM" "$@"
    fi
    case " $* " in
        *" --show-only templates/preflight-gvisor.yaml "*)
            printf '%s\n' 'Error: could not find template templates/preflight-gvisor.yaml in chart' >&2
            exit 1
            ;;
        *" --show-only templates/priorityclass.yaml "*)
            case " $* " in
                *" --set priorityClasses.sandbox.create=false "*)
                    cat <<'YAML'
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: curie-platform
value: 1000000
globalDefault: false
YAML
                    ;;
                *" --set priorityClasses.platform.create=false "*)
                    cat <<'YAML'
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: curie-sandbox
value: 100000
globalDefault: false
YAML
                    ;;
                *)
                    printf 'unexpected PriorityClass render: %s\n' "$*" >&2
                    exit 64
                    ;;
            esac
            exit 0
            ;;
        *" --set-string rustfs.host=s3.example.com "*)
            # #1501: the file points the object store at an external instance
            # (the BYO block every store carries in values.yaml), so the render
            # has NO in-cluster store. Keyed on the VALUE rather than on a mode
            # env var deliberately: the stub can then only diverge when the two
            # callers really do pass different values, which is the whole defect
            # -- the guard rendered with the effective values and saw no store,
            # the export rendered with none at all and saw rustfs.
            cat <<'YAML'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: parity-curie-postgres
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: postgres
YAML
            exit 0
            ;;
    esac
    if [ "${CURIE_TEST_HELM_MIXED_STATEFULSETS:-}" = 1 ]; then
        cat <<'YAML'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: parity-rustfs
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: rustfs
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: parity-curie-postgres
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: postgres
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: parity-curie-valkey
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: valkey
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: parity-curie-clickhouse
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: clickhouse
YAML
        exit 0
    fi
    cat <<'YAML'
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: parity-rustfs
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: rustfs
YAML
    exit 0
fi
if [ "$1" = upgrade ]; then
    previous=''
    sealing_key_present='no'
    runner_credential_present='no'
    runner_credential_preserved='no'
    runner_model_present='no'
    runner_model_preserved='no'
    runner_real_mode_preserved='no'
    runner_egress_preserved='no'
    for argument in "$@"; do
        if [ "$previous" = '-f' ] && [ -n "${CURIE_TEST_CAPTURE_MAIL_VALUES:-}" ] && grep -Fq '"mailAdapter"' "$argument"; then
            cp "$argument" "$CURIE_TEST_CAPTURE_MAIL_VALUES"
        fi
        if [ "$previous" = '-f' ] && grep -q '"sealing"' "$argument" && grep -q '"privateKey"' "$argument"; then
            sealing_key_present='yes'
        fi
        if [ "$previous" = '-f' ] && grep -Fq '"credentials"' "$argument"; then
            runner_credential_present='yes'
        fi
        if [ "$previous" = '-f' ] && [ -n "${CURIE_TEST_EXPECT_RUNNER_CREDENTIAL:-}" ] && grep -Fq "$CURIE_TEST_EXPECT_RUNNER_CREDENTIAL" "$argument"; then
            runner_credential_preserved='yes'
        fi
        case "$argument" in
            agentSandbox.runner.model=*) runner_model_present='yes' ;;
        esac
        if [ -n "${CURIE_TEST_EXPECT_RUNNER_MODEL:-}" ] && [ "$argument" = "agentSandbox.runner.model=$CURIE_TEST_EXPECT_RUNNER_MODEL" ]; then
            runner_model_preserved='yes'
        fi
        if [ "$argument" = 'agentSandbox.runner.fakeModel=false' ]; then
            runner_real_mode_preserved='yes'
        fi
        if [ "$previous" = '-f' ] && [ -n "${CURIE_TEST_EXPECT_RUNNER_EGRESS:-}" ] && grep -Fq "$CURIE_TEST_EXPECT_RUNNER_EGRESS" "$argument"; then
            runner_egress_preserved='yes'
        fi
        case "$argument" in
            *"security.networkPolicy.allowedEgress"*"${CURIE_TEST_EXPECT_RUNNER_EGRESS:-}"*) runner_egress_preserved='yes' ;;
        esac
        previous="$argument"
    done
    if [ -n "${CURIE_TEST_CALL_LOG:-}" ]; then
        printf 'SEALING_KEY_PRESENT: %s\n' "$sealing_key_present" >> "$CURIE_TEST_CALL_LOG"
        if [ -n "${CURIE_TEST_EXPECT_RUNNER_CREDENTIAL:-}" ]; then
            printf 'RUNNER_CREDENTIAL_PRESERVED: %s\n' "$runner_credential_preserved" >> "$CURIE_TEST_CALL_LOG"
            printf 'RUNNER_MODEL_PRESERVED: %s\n' "$runner_model_preserved" >> "$CURIE_TEST_CALL_LOG"
            printf 'RUNNER_REAL_MODE_PRESERVED: %s\n' "$runner_real_mode_preserved" >> "$CURIE_TEST_CALL_LOG"
        fi
        if [ -n "${CURIE_TEST_EXPECT_RUNNER_EGRESS:-}" ]; then
            printf 'RUNNER_EGRESS_PRESERVED: %s\n' "$runner_egress_preserved" >> "$CURIE_TEST_CALL_LOG"
        fi
        if [ "${CURIE_TEST_EXPECT_FRESH_FAKE_MODEL:-}" = 1 ]; then
            if [ "$runner_credential_present" = 'no' ] && [ "$runner_model_present" = 'no' ] && [ "$runner_real_mode_preserved" = 'no' ]; then
                printf 'RUNNER_FRESH_FAKE_MODE: yes\n' >> "$CURIE_TEST_CALL_LOG"
            else
                printf 'RUNNER_FRESH_FAKE_MODE: no\n' >> "$CURIE_TEST_CALL_LOG"
            fi
        fi
        if [ "${CURIE_TEST_EXPECT_RECORDED_PROVIDER_SUPPRESSED:-}" = 1 ]; then
            if [ "$runner_credential_present" = 'no' ] && [ "$runner_model_present" = 'no' ] && [ "$runner_real_mode_preserved" = 'no' ]; then
                printf 'RUNNER_RECORDED_PROVIDER_SUPPRESSED: yes\n' >> "$CURIE_TEST_CALL_LOG"
            else
                printf 'RUNNER_RECORDED_PROVIDER_SUPPRESSED: no\n' >> "$CURIE_TEST_CALL_LOG"
            fi
        fi
    fi
    exit 0
fi
printf 'unexpected helm invocation: %s\n' "$*" >&2
exit 64
"#,
        );

        write_exec(
            &bin_dir,
            "kubectl",
            &format!(
                r#"#!/bin/sh
# Answers only the calls the paths under test actually make, and exits 64 on
# anything else, the same tripwire convention the helm stub uses. A stub that
# hands every question the same plausible blob cannot tell "the code asked the
# right question" from "the code asked a nonsense question and got a nonsense
# answer": the migration's Service lookup was being answered with the whole
# StatefulSet List, which then landed inside an endpoint URL (#1351).
if [ -n "${{CURIE_TEST_CALL_LOG:-}}" ]; then
    printf 'KUBECTL_CALL: %s\n' "$*" >> "$CURIE_TEST_CALL_LOG"
fi
all="$*"
script=""
previous=""
for argument in "$@"; do
    if [ "$previous" = '-c' ]; then
        script="$argument"
    fi
    previous="$argument"
done
verb=""
object=""
while [ $# -gt 0 ]; do
    case "$1" in
        --) break ;;
        -n|--namespace|-o|--output|-l|--selector|--image|--overrides) shift 2 ;;
        -*) shift ;;
        *)
            if [ -z "$verb" ]; then
                verb="$1"
            elif [ -z "$object" ]; then
                object="$1"
            fi
            shift
            ;;
    esac
done
unexpected() {{
    printf 'unexpected kubectl invocation: %s\n' "$all" >&2
    exit 64
}}
migration_target="$CURIE_TEST_MIGRATION_STATE/target"
migration_source="$CURIE_TEST_MIGRATION_STATE/source.list"
persist_target() {{
    target=$(printf '%s\n' "$script" | sed -n "s/.*printf '%s\\\\n' '\\(minio\\|rustfs\\)' > .*/\\1/p")
    case "$target" in
        minio|rustfs) printf '%s\n' "$target" > "$migration_target" ;;
        *) unexpected ;;
    esac
}}
persist_source() {{
    if [ "${{CURIE_TEST_SOURCE_LIST_FAIL:-}}" = 1 ]; then
        printf '%s\n' 'source listing failed' >&2
        exit 1
    fi
    if [ -n "${{CURIE_TEST_SOURCE_LIST+x}}" ]; then
        printf '%s\n' "$CURIE_TEST_SOURCE_LIST" > "$migration_source"
    else
        printf '%s\n' '{STAGED_OBJECT}' > "$migration_source"
    fi
    cat "$migration_source"
}}
case "$verb $object" in
'get deployment')
    case "$all" in
        'get deployment agent-sandbox-controller -n agent-sandbox-system --ignore-not-found -o json') : ;;
        *) unexpected ;;
    esac
    ;;
'get priorityclass')
    # Empty stdout with exit 0 is kubectl --ignore-not-found for an absent class.
    :
    ;;
'get namespace')
    exit 1
    ;;
'get statefulset')
    if [ "${{CURIE_TEST_KUBECTL_FAIL:-}}" = 1 ]; then
        printf '%s\n' '{KUBECTL_UNREACHABLE}' >&2
        exit 1
    fi
    if [ "${{CURIE_TEST_KUBECTL_FORBIDDEN:-}}" = 1 ]; then
        printf '%s\n' '{KUBECTL_FORBIDDEN}' >&2
        exit 1
    fi
    if [ -n "${{CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE:-}}" ] &&
       grep -q '^HELM_CALL: upgrade ' "$CURIE_TEST_CALL_LOG"; then
        printf '%s\n' "$CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE"
    elif [ -n "${{CURIE_TEST_KUBECTL_STS:-}}" ]; then
        printf '%s\n' "$CURIE_TEST_KUBECTL_STS"
    else
        printf '%s\n' '{KUBECTL_EMPTY_LIST}'
    fi
    ;;
'get svc')
    # The jsonpath asks for a Service NAME, so answer with one, for whichever
    # store component the caller named and nothing else.
    case "$all" in
        *'=="minio"'*) printf '%s\n' 'parity-minio' ;;
        *'=="rustfs"'*) printf '%s\n' 'parity-rustfs' ;;
        *) unexpected ;;
    esac
    ;;
'get secret')
    printf '%s\n' "${{CURIE_TEST_RELEASE_SECRET:-parity-secrets}}"
    ;;
'run '*)
    mkdir -p "$CURIE_TEST_MIGRATION_STATE"
    ;;
'wait '*)
    :
    ;;
'delete pod')
    case "$all" in
        *store-migration*) rm -f "$migration_target" "$migration_source" ;;
    esac
    ;;
'exec '*)
    printf '%s' "$script" > "$CURIE_TEST_LAST_EXEC_SCRIPT"
    # One answer per in-pod script the migration runs, keyed on the script
    # itself: a single canned answer here would let the export and the verify
    # read each other's output.
    case "$script" in
        *'aws s3 ls'*'/migration/source.list'*|*'/migration/source.list'*'aws s3 ls'*)
            persist_source
            case "$script" in
                *'/migration/target'*) persist_target ;;
            esac
            ;;
        *'printf'*'/migration/target'*|*'echo '*'/migration/target'*)
            persist_target
            ;;
        *'cat /migration/target'*)
            if [ ! -s "$migration_target" ]; then
                printf '%s\n' 'planned migration target is missing' >&2
                exit 1
            fi
            cat "$migration_target"
            ;;
        *'cat /migration/source.list'*)
            if [ ! -s "$migration_source" ]; then
                printf '%s\n' 'persisted source inventory is missing' >&2
                exit 1
            fi
            cat "$migration_source"
            ;;
        *'wc -l'*) printf '%s\n' '1' ;;
        *'-printf'*)
            if [ -n "${{CURIE_TEST_STAGED_LIST+x}}" ]; then
                printf '%s\n' "$CURIE_TEST_STAGED_LIST"
            else
                printf '%s\n' '{STAGED_OBJECT}'
            fi
            ;;
        *'aws s3 ls'*)
            case "$script" in
                *'parity-minio.parity.svc.cluster.local'*)
                    if [ "${{CURIE_TEST_SOURCE_LIST_FAIL:-}}" = 1 ]; then
                        printf '%s\n' 'source listing failed' >&2
                        exit 1
                    fi
                    if [ -n "${{CURIE_TEST_SOURCE_LIST+x}}" ]; then
                        printf '%s\n' "$CURIE_TEST_SOURCE_LIST"
                    else
                        printf '%s\n' '{STAGED_OBJECT}'
                    fi
                    ;;
                *'parity-rustfs.parity.svc.cluster.local'*)
                    if [ "${{CURIE_TEST_TARGET_LIST_FAIL:-}}" = 1 ]; then
                        printf '%s\n' 'target listing failed' >&2
                        exit 1
                    fi
                    if [ -n "${{CURIE_TEST_TARGET_LIST+x}}" ]; then
                        printf '%s\n' "$CURIE_TEST_TARGET_LIST"
                    else
                        printf '%s\n' '{STAGED_OBJECT}'
                    fi
                    ;;
                *) unexpected ;;
            esac
            ;;
        *'aws s3 sync'*) printf '%s\n' 'synced' ;;
        *'/migration/'*) unexpected ;;
        *) unexpected ;;
    esac
    ;;
*)
    unexpected
    ;;
esac
exit 0
"#
            ),
        );

        let log = temp.path().join("calls.log");
        let migration_state = temp.path().join("migration");
        fs::create_dir(&migration_state).expect("create migration state directory");
        let last_exec_script = temp.path().join("last-exec-script");
        Self {
            temp,
            file,
            values_response,
            log,
            migration_state,
            last_exec_script,
        }
    }

    /// Every stubbed helm and kubectl invocation this fixture has seen, in
    /// order. Empty when nothing ran, which is the assertion a run that must
    /// touch no cluster turns on, so a missing file reads as "no calls" rather
    /// than panicking.
    fn calls(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn seed_migration_evidence(&self, target: &str, source: &str) {
        fs::write(self.migration_state.join("target"), format!("{target}\n"))
            .expect("write planned migration target");
        fs::write(
            self.migration_state.join("source.list"),
            format!("{source}\n"),
        )
        .expect("write persisted source inventory");
    }

    fn migration_target(&self) -> Option<String> {
        fs::read_to_string(self.migration_state.join("target")).ok()
    }

    fn migration_source(&self) -> Option<String> {
        fs::read_to_string(self.migration_state.join("source.list")).ok()
    }

    fn last_exec_script(&self) -> String {
        fs::read_to_string(&self.last_exec_script).expect("captured in pod shell")
    }

    fn run(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        self.run_with_globals(&["--json"], args, env)
    }

    /// The same stubbed run with NO global flags, so a test can assert on the
    /// HUMAN render rather than the `--json` payload. `render` and `to_json`
    /// are two independent projections of the same output object, and a
    /// removal reported in one but not the other is exactly the surface
    /// disagreement #1352 is about.
    fn run_human(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        self.run_with_globals(&[], args, env)
    }

    fn run_with_globals(&self, globals: &[&str], args: &[&str], env: &[(&str, &str)]) -> Output {
        let mut paths = vec![self.temp.path().join("bin")];
        if let Some(current) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&current));
        }
        let path = std::env::join_paths(paths).expect("join PATH");

        let mut command = Command::new(bin());
        command
            .current_dir(repo_root())
            .args(globals)
            .args(args)
            .env("PATH", path)
            .env("CURIE_TEST_CALL_LOG", &self.log)
            .env("CURIE_TEST_MIGRATION_STATE", &self.migration_state)
            .env("CURIE_TEST_LAST_EXEC_SCRIPT", &self.last_exec_script)
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env_remove("CURIE_TEST_KUBECTL_STS")
            .env_remove("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE")
            .env_remove("CURIE_TEST_KUBECTL_FAIL")
            .env_remove("CURIE_TEST_KUBECTL_FORBIDDEN")
            .env_remove("CURIE_TEST_HELM_MIXED_STATEFULSETS")
            .env_remove("CURIE_TEST_RELEASE_SECRET")
            .env_remove("CURIE_TEST_SOURCE_LIST_FAIL")
            .env_remove("CURIE_TEST_SOURCE_LIST")
            .env_remove("CURIE_TEST_STAGED_LIST")
            .env_remove("CURIE_TEST_TARGET_LIST_FAIL")
            .env_remove("CURIE_TEST_TARGET_LIST")
            .env_remove("CURIE_CREDENTIALS")
            .env_remove("CURIE_MODEL_CREDENTIALS")
            .env_remove("CURIE_GITHUB_TOKEN")
            .env_remove("CURIE_MODEL")
            .env_remove("CURIE_TEST_PROVIDER_EGRESS_JSON")
            .env_remove("CURIE_APPLY_TEST_MODEL_KEY")
            .env_remove("CURIE_APPLY_TEST_GITHUB_TOKEN");
        match &self.values_response {
            HelmValuesResponse::Object(values) => {
                command
                    .env("CURIE_TEST_HELM_VALUES_MODE", "success")
                    .env("CURIE_TEST_HELM_VALUES", values.to_string());
            }
            HelmValuesResponse::RawSuccess(values) => {
                command
                    .env("CURIE_TEST_HELM_VALUES_MODE", "success")
                    .env("CURIE_TEST_HELM_VALUES", values);
            }
            HelmValuesResponse::Null => {
                command
                    .env("CURIE_TEST_HELM_VALUES_MODE", "success")
                    .env("CURIE_TEST_HELM_VALUES", "null");
            }
            HelmValuesResponse::Absent => {
                command
                    .env("CURIE_TEST_HELM_VALUES_MODE", "absent")
                    .env_remove("CURIE_TEST_HELM_VALUES");
            }
            HelmValuesResponse::Failure(reason) => {
                command
                    .env("CURIE_TEST_HELM_VALUES_MODE", "failure")
                    .env("CURIE_TEST_HELM_VALUES", reason);
            }
        }
        for (key, value) in env {
            command.env(key, value);
        }
        command.output().expect("run curie")
    }

    fn apply_dry_run(&self, env: &[(&str, &str)]) -> Output {
        self.run(
            &[
                "apply",
                "--dry-run",
                "--file",
                self.file.to_str().expect("UTF 8 path"),
            ],
            env,
        )
    }

    /// The REAL apply path, no `--dry-run`: the guard, the migration branch and
    /// the upgrade all run against the stubs.
    fn apply(&self, extra: &[&str], env: &[(&str, &str)]) -> Output {
        let mut args = vec!["apply", "--file", self.file.to_str().expect("UTF 8 path")];
        args.extend_from_slice(extra);
        self.run(&args, env)
    }

    fn diff(&self, env: &[(&str, &str)]) -> Output {
        self.diff_with(&[], env)
    }

    /// The same `--json` diff with extra argv, so a test can exercise a flag
    /// rather than only the default resolution. `diff` delegates here, so no
    /// existing call site changes.
    fn diff_with(&self, extra: &[&str], env: &[(&str, &str)]) -> Output {
        let mut args = vec!["diff", "--file", self.file.to_str().expect("UTF 8 path")];
        args.extend_from_slice(extra);
        self.run(&args, env)
    }

    /// `curie diff` without `--json`: the operator-facing render.
    fn diff_human(&self, env: &[(&str, &str)]) -> Output {
        self.run_human(
            &["diff", "--file", self.file.to_str().expect("UTF 8 path")],
            env,
        )
    }

    fn cluster_up(&self) -> Output {
        self.cluster_up_with(&["--fake-model"], &[])
    }

    fn cluster_up_with(&self, extra: &[&str], env: &[(&str, &str)]) -> Output {
        let chart = repo_root().join("charts/curie");
        let mut args = vec![
            "cluster",
            "up",
            "--namespace",
            "parity",
            "--release",
            "parity",
            "--chart",
            chart.to_str().expect("UTF 8 chart path"),
        ];
        args.extend_from_slice(extra);
        args.extend_from_slice(&["--set", "agentSandbox.controller.deploy=false"]);
        self.run(&args, env)
    }

    fn cluster_up_without_credentials(&self, env: &[(&str, &str)]) -> Output {
        self.cluster_up_with(&[], env)
    }

    fn cluster_up_dry_run(&self) -> Output {
        let chart = repo_root().join("charts/curie");
        self.run(
            &[
                "cluster",
                "up",
                "--namespace",
                "parity",
                "--release",
                "parity",
                "--chart",
                chart.to_str().expect("UTF 8 chart path"),
                "--fake-model",
                "--set",
                "agentSandbox.controller.deploy=false",
                "--dry-run",
            ],
            &[],
        )
    }
}

fn provider_egress_fixture() -> String {
    json!({"api.anthropic.com": [IPV4, IPV6]}).to_string()
}

fn recorded_runner_values() -> HelmValuesResponse {
    recorded_runner_values_with_credential(RERUN_CREDENTIAL_SENTINEL)
}

fn recorded_runner_values_with_credential(credential: &str) -> HelmValuesResponse {
    HelmValuesResponse::Object(json!({
        "agentSandbox": {"runner": {
            "credentials": credential,
            "fakeModel": false,
            "model": RERUN_MODEL_SENTINEL
        }},
        "security": {"networkPolicy": {"allowedEgress": [{
            "cidr": RERUN_EGRESS_CIDR,
            "ports": [{"port": 443, "protocol": "TCP"}]
        }]}}
    }))
}

fn json_output(output: Output, verb: &str) -> Value {
    assert!(
        output.status.success(),
        "{verb} failed with stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{verb} did not emit JSON: {error}; stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn json_error(output: Output, verb: &str) -> Value {
    assert!(
        !output.status.success(),
        "{verb} unexpectedly succeeded; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "{verb} wrote an error to stderr under --json:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{verb} did not emit structured JSON error: {error}; stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(json["error"].is_string(), "{verb} error payload: {json}");
    assert!(json.get("fix").is_some(), "{verb} error payload: {json}");
    json
}

fn assert_provider_contradiction(
    output: Output,
    credential: &str,
    detected: &str,
    allowed: &str,
    verb: &str,
) {
    assert_eq!(
        output.status.code(),
        Some(2),
        "a contradictory provider is a usage error; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let error = json_error(output, verb);
    let diagnostic = format!("{} {}", error["error"], error["fix"]).to_ascii_lowercase();
    for expected in [detected, allowed, "--allow-egress-host"] {
        assert!(
            diagnostic.contains(expected),
            "the contradiction must identify both providers and the correction: {error}"
        );
    }
    assert!(
        !visible.contains(credential),
        "the rejected credential leaked into command output: {visible}"
    );
}

fn plan(output: Output) -> String {
    let json = json_output(output, "apply --dry-run");
    json["plan"]
        .as_array()
        .expect("dry run plan array")
        .iter()
        .map(|line| line.as_str().expect("dry run line"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn shell_tokens(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quoted = false;
    for character in line.chars() {
        match character {
            '\'' => quoted = !quoted,
            character if character.is_whitespace() && !quoted => {
                if !token.is_empty() {
                    tokens.push(std::mem::take(&mut token));
                }
            }
            character => token.push(character),
        }
    }
    assert!(!quoted, "unterminated quote in dry run command: {line}");
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
}

fn helm_values(plan: &str) -> BTreeMap<String, String> {
    let helm = plan
        .lines()
        .find(|line| line.starts_with("helm upgrade "))
        .unwrap_or_else(|| panic!("missing helm upgrade command: {plan}"));
    let tokens = shell_tokens(helm);
    let mut values = BTreeMap::new();
    let mut index = 0;
    while index < tokens.len() {
        if tokens[index] == "--set" || tokens[index] == "--set-string" {
            let setting = tokens.get(index + 1).unwrap_or_else(|| {
                panic!("{} has no value in dry run command: {helm}", tokens[index])
            });
            let (key, value) = setting
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid Helm setting {setting}: {helm}"));
            values.insert(key.to_string(), value.to_string());
            index += 2;
            continue;
        }
        if tokens[index] == "-f" {
            let file = tokens
                .get(index + 1)
                .unwrap_or_else(|| panic!("-f has no value in dry run command: {helm}"));
            if let Some(secret_values) = file
                .strip_prefix("<secret values file: ")
                .and_then(|value| value.strip_suffix('>'))
            {
                for setting in secret_values.split(", ") {
                    let (key, value) = setting
                        .split_once('=')
                        .unwrap_or_else(|| panic!("invalid secret Helm setting {setting}: {helm}"));
                    if matches!(key, "agentSandbox.runner.credentials" | "api.githubToken") {
                        values.insert(key.to_string(), value.to_string());
                    }
                }
            }
            index += 2;
            continue;
        }
        index += 1;
    }
    values
}

fn entry<'a>(diff: &'a Value, key: &str) -> &'a Value {
    diff["entries"]
        .as_array()
        .expect("diff entries array")
        .iter()
        .find(|entry| entry["key"] == key)
        .unwrap_or_else(|| panic!("missing diff entry for {key}: {diff}"))
}

fn assert_added(diff: &Value, key: &str, value: &str) {
    let entry = entry(diff, key);
    assert_eq!(entry["kind"], "add", "{key}: {entry}");
    assert_eq!(entry["to"].as_str(), Some(value), "{key}: {entry}");
}

fn assert_diff_keys(diff: &Value, expected: &[&str]) {
    let actual = diff["entries"]
        .as_array()
        .expect("diff entries array")
        .iter()
        .map(|entry| entry["key"].as_str().expect("diff entry key"))
        .collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected, "diff effective values: {diff}");
}

/// The smallest installation the stateful-removal guard runs against: it names
/// only the namespace and release, so the run makes exactly the cluster calls
/// the guard and the upgrade need and nothing else.
fn installation_for_the_stateful_guard() -> &'static str {
    "version: 1\ninstall:\n  namespace: parity\n  release: parity\n"
}

/// A live release running a `minio` object store, in the shape
/// `kubectl get statefulset -o json` returns it. The component label is the
/// identity the guard reads; the resource name is what an operator sees.
fn live_minio_statefulset() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "parity-minio"},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
        }]
    })
    .to_string()
}

/// A live release running the bundled `postgres`, in the shape
/// `kubectl get statefulset -o json` returns it. The default helm stub renders
/// `rustfs` and nothing else, so against that render this component is
/// `ComponentGone` -- the issue's reproduction, where a curie.yaml that stops
/// deploying postgres reads as an ordinary values add.
fn live_postgres_statefulset() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "parity-postgres"},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "postgres"}}}
        }]
    })
    .to_string()
}

fn live_mixed_store_statefulsets() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "metadata": {"name": "parity-minio"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            },
            {
                "metadata": {"name": "parity-postgres"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "postgres"}}}
            },
            {
                "metadata": {"name": "parity-valkey"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "valkey"}}}
            },
            {
                "metadata": {"name": "parity-clickhouse"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "clickhouse"}}}
            }
        ]
    })
    .to_string()
}

/// A live release running a `minio` object store BESIDE a `postgres` one: the
/// two-component batch #1501 is about. Against a chart render that has neither,
/// BOTH are `ComponentGone`, which used to be the entire `--migrate-store`
/// bypass condition -- and only one of the two is something the migration can
/// actually carry.
fn live_minio_and_postgres_statefulsets() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "metadata": {"name": "parity-minio"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            },
            {
                "metadata": {"name": "parity-postgres"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "postgres"}}}
            }
        ]
    })
    .to_string()
}

/// The stateful-guard installation with the object store pointed at an EXTERNAL
/// instance in the file's own values, so the target chart renders no in-cluster
/// store. `rustfs.deploy: "false"` is the other half of that BYO block, but a
/// `set:` map refuses boolean-shaped values by design (every `--set` is a
/// string, and Helm reads every nonempty string as true), so the host key alone
/// carries the intent here. The guard renders WITH these values; before #1501
/// the export rendered with NONE, so the two halves of the same apply planned
/// different upgrades and the disagreement only surfaced once the irreversible
/// half had run.
fn installation_that_turns_the_store_off() -> &'static str {
    "version: 1\ninstall:\n  namespace: parity\n  release: parity\nset:\n  rustfs.host: s3.example.com\n"
}

fn live_rustfs_statefulset() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "parity-rustfs"},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "rustfs"}}}
        }]
    })
    .to_string()
}

/// A live release running the store the default render ALSO produces, beside a
/// bundled `postgres` that render drops. The store half is what makes this
/// distinct from `live_postgres_statefulset`: both sides run `rustfs`, so the
/// upgrade renames no store and `migration` is `null` while the removal list is
/// not empty -- the values-gated drop `--migrate-store` cannot carry (#1352).
fn live_rustfs_and_postgres_statefulsets() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "metadata": {"name": "parity-rustfs"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "rustfs"}}}
            },
            {
                "metadata": {"name": "parity-postgres"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "postgres"}}}
            }
        ]
    })
    .to_string()
}

fn live_both_store_statefulsets() -> String {
    json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {
                "metadata": {"name": "parity-minio"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            },
            {
                "metadata": {"name": "parity-rustfs"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "rustfs"}}}
            }
        ]
    })
    .to_string()
}

fn installation_with_effective_values() -> &'static str {
    "version: 1\ninstall:\n  namespace: parity\n  release: parity\ncredentials:\n  model: CURIE_APPLY_TEST_MODEL_KEY\n  github_token: CURIE_APPLY_TEST_GITHUB_TOKEN\nplatform:\n  ui: false\n  inference: true\n  inference_persistence: true\nset:\n  example.mode: disabled\n  worker.replicas: \"3\"\n"
}

fn installation_with_provider_contradiction() -> &'static str {
    "version: 1\ninstall:\n  namespace: parity\n  release: parity\ncredentials:\n  model: CURIE_APPLY_TEST_MODEL_KEY\nplatform:\n  egress:\n    - host: anthropic\n"
}

#[derive(Clone, Copy)]
enum ExistingValuesConsumer {
    ClusterUp,
    Apply,
    Diff,
}

impl ExistingValuesConsumer {
    const ALL: [Self; 3] = [Self::ClusterUp, Self::Apply, Self::Diff];
    const MUTATING: [Self; 2] = [Self::ClusterUp, Self::Apply];

    fn name(self) -> &'static str {
        match self {
            Self::ClusterUp => "cluster up",
            Self::Apply => "apply",
            Self::Diff => "diff",
        }
    }

    fn run(self, fixture: &HelmFixture) -> Output {
        match self {
            Self::ClusterUp => fixture.cluster_up(),
            Self::Apply => fixture.apply(&[], &[]),
            Self::Diff => fixture.diff(&[]),
        }
    }
}

fn assert_only_existing_values_read(fixture: &HelmFixture, consumer: ExistingValuesConsumer) {
    let calls = fixture.calls();
    assert_eq!(
        calls.trim(),
        "HELM_CALL: get values parity -n parity -o json",
        "{} must stop after the existing values read:\n{calls}",
        consumer.name()
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade") && !calls.contains("KUBECTL_CALL:"),
        "{} reached a mutating or secondary cluster command:\n{calls}",
        consumer.name()
    );
    assert!(
        !calls.contains("SEALING_KEY_PRESENT: yes"),
        "{} regenerated a sealing key after an unknown read:\n{calls}",
        consumer.name()
    );
}

#[test]
fn existing_values_absent_is_fresh_for_all_consumers() {
    for consumer in ExistingValuesConsumer::MUTATING {
        let fixture = HelmFixture::new(
            installation_for_the_stateful_guard(),
            HelmValuesResponse::Absent,
        );
        let output = consumer.run(&fixture);
        json_output(output, consumer.name());

        let calls = fixture.calls();
        assert!(
            calls.contains("HELM_CALL: upgrade"),
            "{} must reach Helm upgrade after exact release absence:\n{calls}",
            consumer.name()
        );
        assert!(
            calls.contains("SEALING_KEY_PRESENT: yes"),
            "{} must generate the initial sealing key after exact release absence:\n{calls}",
            consumer.name()
        );
    }

    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let diff = json_output(fixture.diff(&[]), "diff");
    assert_eq!(diff["release_exists"], false, "{diff}");
    // #1352 grew this list by exactly ONE call. `diff` now runs the same
    // stateful-removal probe `apply` does, from the shared plan, so it reads the
    // live StatefulSets between the values read and the deployed-chart read.
    // The fixture's default kubectl answer is the empty List, and the guard
    // short-circuits on an empty live list, so an absent release never reaches
    // `helm template` -- asserting a render here would fail a correct
    // implementation and pressure removal of the short circuit that saves a
    // render on every fresh apply.
    assert_eq!(
        fixture.calls().trim(),
        "HELM_CALL: get values parity -n parity -o json\n\
         KUBECTL_CALL: get statefulset -n parity -o json\n\
         HELM_CALL: list -n parity -o json"
    );
}

#[test]
fn existing_values_nonzero_is_unknown_for_all_consumers() {
    let failures = [
        (HELM_UNREACHABLE, 3, "connection refused"),
        (HELM_FORBIDDEN, 1, "forbidden"),
        (HELM_EXECUTABLE_NOT_FOUND, 1, "executable"),
    ];

    for (reason, expected_exit, expected_message) in failures {
        for consumer in ExistingValuesConsumer::ALL {
            let fixture = HelmFixture::new(
                installation_for_the_stateful_guard(),
                HelmValuesResponse::Failure(reason),
            );
            let output = consumer.run(&fixture);
            assert_eq!(
                output.status.code(),
                Some(expected_exit),
                "{} classified an unknown values read incorrectly; stdout:\n{}\nstderr:\n{}",
                consumer.name(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let error = json_error(output, consumer.name());
            assert!(
                error["error"]
                    .as_str()
                    .is_some_and(|message| message.contains(expected_message)),
                "{} did not preserve the Helm diagnosis: {error}",
                consumer.name()
            );
            assert!(
                error["fix"]
                    .as_str()
                    .is_some_and(|fix| fix.contains("helm status")),
                "{} did not provide the safe release access check: {error}",
                consumer.name()
            );
            assert!(
                error.get("release_exists").is_none(),
                "{} reported an unknown release as known: {error}",
                consumer.name()
            );
            assert_only_existing_values_read(&fixture, consumer);
        }
    }
}

#[test]
fn existing_values_malformed_is_unknown_for_all_consumers() {
    for malformed in [WARNING_PREFIXED_VALUES, ARRAY_VALUES] {
        for consumer in ExistingValuesConsumer::ALL {
            let fixture = HelmFixture::new(
                installation_for_the_stateful_guard(),
                HelmValuesResponse::RawSuccess(malformed),
            );
            let output = consumer.run(&fixture);
            let visible = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                !visible.contains(SENTINEL_SEALING_KEY),
                "{} exposed Helm values output:\n{visible}",
                consumer.name()
            );
            assert!(
                !visible.contains("generated strong per-release secrets"),
                "{} reported sealing key generation after malformed values:\n{visible}",
                consumer.name()
            );
            let error = json_error(output, consumer.name());
            assert!(
                error["error"]
                    .as_str()
                    .is_some_and(|message| message.contains("malformed")),
                "{} did not identify malformed Helm values: {error}",
                consumer.name()
            );
            assert!(
                error.get("release_exists").is_none(),
                "{} reported a malformed release read as known: {error}",
                consumer.name()
            );
            assert_only_existing_values_read(&fixture, consumer);
            assert!(
                !fixture.calls().contains(SENTINEL_SEALING_KEY),
                "{} exposed the sentinel in the command log:\n{}",
                consumer.name(),
                fixture.calls()
            );
        }
    }
}

#[test]
fn existing_values_null_is_an_existing_release() {
    for consumer in ExistingValuesConsumer::MUTATING {
        let fixture = HelmFixture::new(
            installation_for_the_stateful_guard(),
            HelmValuesResponse::Null,
        );
        let output = consumer.run(&fixture);
        let visible = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !visible.contains("generated strong per-release secrets"),
            "{} mislabeled a valid null release as fresh:\n{visible}",
            consumer.name()
        );
        json_output(output, consumer.name());

        let calls = fixture.calls();
        assert!(
            calls.contains("HELM_CALL: upgrade"),
            "{} must accept valid null values:\n{calls}",
            consumer.name()
        );
        assert!(
            calls.contains("SEALING_KEY_PRESENT: yes"),
            "{} may add sealing to an existing release with no values:\n{calls}",
            consumer.name()
        );
    }

    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Null,
    );
    let diff = json_output(fixture.diff(&[]), "diff");
    assert_eq!(diff["release_exists"], true, "{diff}");
    assert!(
        diff["entries"].is_array(),
        "valid null values must produce a completed diff for the existing release: {diff}"
    );
}

#[test]
fn store_migration_preview_mounts_the_discovered_release_secret() {
    let fixture = HelmFixture::new("", HelmValuesResponse::Absent);
    let chart = repo_root().join("charts/curie");
    let live = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "acme-minio"},
            "spec": {"selector": {"matchLabels": {
                "app.kubernetes.io/component": "minio"
            }}}
        }]
    })
    .to_string();
    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--namespace",
            "acme",
            "--release",
            "acme",
            "--chart",
            chart.to_str().expect("UTF 8 chart path"),
            "--dry-run",
        ],
        &[
            ("CURIE_TEST_KUBECTL_STS", live.as_str()),
            ("CURIE_TEST_RELEASE_SECRET", "acme-curie-secrets"),
        ],
    );
    let preview = json_output(output, "cluster migrate-store --dry-run")["plan"]
        .as_array()
        .expect("migration preview plan")
        .iter()
        .map(|line| line.as_str().expect("preview command"))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        fixture.calls().contains("KUBECTL_CALL: -n acme get secret"),
        "the preview must discover the release Secret before planning its consumer:\n{}",
        fixture.calls()
    );
    assert!(
        preview.contains(r#""secretName":"acme-curie-secrets""#),
        "the staging pod must mount the discovered Secret: {preview}"
    );
    assert!(
        !preview.contains(r#""secretName":"acme-secrets""#),
        "the preview must not guess a Secret name the chart does not render: {preview}"
    );
}

#[test]
fn store_migration_export_preview_mounts_the_discovered_release_secret() {
    let fixture = HelmFixture::new("", HelmValuesResponse::Absent);
    let chart = repo_root().join("charts/curie");
    let live = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "acme-minio"},
            "spec": {"selector": {"matchLabels": {
                "app.kubernetes.io/component": "minio"
            }}}
        }]
    })
    .to_string();
    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "export",
            "--namespace",
            "acme",
            "--release",
            "acme",
            "--chart",
            chart.to_str().expect("UTF 8 chart path"),
            "--dry-run",
        ],
        &[
            ("CURIE_TEST_KUBECTL_STS", live.as_str()),
            ("CURIE_TEST_RELEASE_SECRET", "acme-curie-secrets"),
        ],
    );
    let preview = json_output(output, "cluster migrate-store --phase export --dry-run")["plan"]
        .as_array()
        .expect("migration export preview plan")
        .iter()
        .map(|line| line.as_str().expect("preview command"))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        fixture.calls().contains("KUBECTL_CALL: -n acme get secret"),
        "the export preview must discover the release Secret before planning its consumer:\n{}",
        fixture.calls()
    );
    assert!(
        preview.contains(r#""secretName":"acme-curie-secrets""#),
        "the export staging pod must mount the discovered Secret: {preview}"
    );
    assert!(
        !preview.contains(r#""secretName":"acme-secrets""#),
        "the export staging pod must not guess a Secret name the chart does not render: {preview}"
    );
}

#[test]
fn cluster_up_reports_a_preserved_sealing_key_as_preserved() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Object(json!({
            "sealing": {"privateKey": PRESERVED_SEALING_KEY}
        })),
    );
    let output = fixture.cluster_up();
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    json_output(output, "cluster up with a recorded sealing key");

    assert!(
        visible.contains("preserving 1") && visible.contains("sealing"),
        "the existing sealing key must be counted and named as preserved:\n{visible}"
    );
    assert!(
        !visible.contains("generated") || !visible.contains("sealing"),
        "a recorded sealing key must not be reported as generated:\n{visible}"
    );
    assert!(
        fixture.calls().contains("SEALING_KEY_PRESENT: yes"),
        "the preserved key must reach the Helm consumer:\n{}",
        fixture.calls()
    );
    assert!(
        !visible.contains(PRESERVED_SEALING_KEY)
            && !fixture.calls().contains(PRESERVED_SEALING_KEY),
        "the preserved private key must remain redacted from output and command logs"
    );
}

#[test]
fn cluster_up_reports_a_new_sealing_key_as_generated_not_preserved() {
    for (release_state, values_response) in [
        ("absent release", HelmValuesResponse::Absent),
        (
            "existing release without a sealing key",
            HelmValuesResponse::Object(json!({"ui": {"deploy": false}})),
        ),
    ] {
        let fixture = HelmFixture::new(installation_for_the_stateful_guard(), values_response);
        let output = fixture.cluster_up();
        let visible = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        json_output(output, &format!("cluster up for {release_state}"));
        let calls = fixture.calls();

        assert!(
            visible.contains(
                "generated a sealing private key for this release; later cluster up runs preserve it"
            ),
            "{release_state} must report the generated sealing key exactly:\n{visible}"
        );
        assert!(
            !visible.contains("preserving"),
            "{release_state} must not report preservation for a generated key:\n{visible}"
        );
        assert!(
            !visible.contains("cluster comms"),
            "{release_state} must not attribute generation to cluster comms:\n{visible}"
        );
        assert!(
            !visible.contains("cluster github-app"),
            "{release_state} must not attribute generation to cluster github-app:\n{visible}"
        );
        assert!(
            calls.contains("HELM_CALL: get values "),
            "{release_state} must inspect the release values:\n{calls}"
        );
        assert!(
            calls.contains("SEALING_KEY_PRESENT: yes"),
            "the generated key for {release_state} must reach the Helm consumer:\n{calls}"
        );
    }
}

#[test]
fn cluster_up_dry_run_defers_sealing_resolution_without_inventing_a_key() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let output = fixture.cluster_up_dry_run();
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    json_output(output, "cluster up --dry-run");

    assert!(
        fixture.calls().is_empty(),
        "the offline preview must not inspect the release:\n{}",
        fixture.calls()
    );
    assert!(
        !visible.contains("sealing.privateKey"),
        "an offline preview cannot honestly choose or invent the private key:\n{visible}"
    );
    assert!(
        visible.contains("live run")
            && visible.contains("sealing")
            && visible.contains("preserv")
            && visible.contains("generat"),
        "the preview must explain that live release discovery decides preservation or generation:\n{visible}"
    );
}

#[test]
fn clean_external_mail_diff_has_no_phantom_inline_additions() {
    let existing = external_mail_sources();
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Object(existing),
    );
    let diff = json_output(fixture.diff(&[]), "clean external mail diff");
    for entry in diff["entries"].as_array().unwrap() {
        assert!(
            ![
                "mailAdapter.agentmail.apiKey",
                "mailAdapter.channelToken",
                "mailAdapter.egressSecret",
                "worker.adapterCredentials"
            ]
            .contains(&entry["key"].as_str().unwrap()),
            "an absent inline credential must stay absent from a clean diff: {entry}"
        );
    }
}

fn external_mail_sources() -> Value {
    json!({
        "mailAdapter": {
            "deploy": true,
            "agentmail": {"apiKeyExistingSecret": "acme-provider", "apiKeyExistingSecretKey": "provider-key"},
            "channelTokenExistingSecret": "acme-channel", "channelTokenExistingSecretKey": "channel-key",
            "egressSecretExistingSecret": "acme-egress", "egressSecretExistingSecretKey": "egress-key"
        },
        "worker": {"adapterCredentialsExistingSecret": "acme-worker", "adapterCredentialsExistingSecretKey": "worker-map"}
    })
}

#[test]
fn typed_worker_mail_credential_map_removal_has_no_phantom_parent_diff() {
    for inline_map in [json!({}), json!({"mail-adapter": "obsolete-inline"})] {
        let mut existing = external_mail_sources();
        existing["worker"]["adapterCredentials"] = inline_map.clone();
        let fixture = HelmFixture::new(
            installation_for_the_stateful_guard(),
            HelmValuesResponse::Object(existing),
        );
        let diff = json_output(fixture.diff(&[]), "typed worker map diff");
        let entries = diff["entries"].as_array().unwrap();
        assert!(
            !entries
                .iter()
                .any(|entry| entry["key"] == "worker.adapterCredentials"),
            "an object is not a new scalar Helm value"
        );
        if !inline_map.as_object().unwrap().is_empty() {
            let leaf = entries
                .iter()
                .find(|entry| entry["key"] == "worker.adapterCredentials.mail-adapter")
                .unwrap();
            assert_eq!(
                leaf["kind"], "change",
                "the stale inline map must not claim preservation"
            );
        }
        let captured = fixture.temp.path().join("mail-values.json");
        json_output(
            fixture.cluster_up_with(
                &["--fake-model"],
                &[("CURIE_TEST_CAPTURE_MAIL_VALUES", captured.to_str().unwrap())],
            ),
            "typed worker map up",
        );
        let actual: Value = serde_json::from_slice(&fs::read(captured).unwrap()).unwrap();
        assert!(actual["worker"].get("adapterCredentials").is_none());
        assert_eq!(
            actual["worker"]["adapterCredentialsExistingSecret"],
            "acme-worker"
        );
    }
}

#[test]
fn empty_inline_mail_clears_preserve_external_sources_through_up_and_apply() {
    for (inline, value) in [
        ("mailAdapter.agentmail.apiKey", ""),
        ("mailAdapter.channelToken", ""),
        ("mailAdapter.egressSecret", ""),
        ("worker.adapterCredentials", ""),
        ("worker.adapterCredentials", "{}"),
    ] {
        for surface in ["up", "apply"] {
            let config = format!(
                "{}set:\n  {inline}: {value:?}\n",
                installation_for_the_stateful_guard()
            );
            let fixture =
                HelmFixture::new(&config, HelmValuesResponse::Object(external_mail_sources()));
            let captured = fixture.temp.path().join("mail-values.json");
            let env = [("CURIE_TEST_CAPTURE_MAIL_VALUES", captured.to_str().unwrap())];
            let set = format!("{inline}={value}");
            let output = if surface == "up" {
                fixture.cluster_up_with(&["--fake-model", "--set", &set], &env)
            } else {
                fixture.apply(&[], &env)
            };
            json_output(output, surface);
            let actual: Value = serde_json::from_slice(&fs::read(captured).unwrap()).unwrap();
            for (pointer, expected) in [
                (
                    "/mailAdapter/agentmail/apiKeyExistingSecret",
                    "acme-provider",
                ),
                ("/mailAdapter/channelTokenExistingSecret", "acme-channel"),
                ("/mailAdapter/egressSecretExistingSecret", "acme-egress"),
                ("/worker/adapterCredentialsExistingSecret", "acme-worker"),
            ] {
                assert_eq!(
                    actual.pointer(pointer).and_then(Value::as_str),
                    Some(expected),
                    "{surface}: empty {inline}={value:?} must not switch credential sources"
                );
            }
        }
    }
}

#[test]
fn empty_worker_map_clear_reaches_real_helm_with_its_map_type_intact() {
    let helm = Command::new("sh")
        .args(["-c", "command -v helm"])
        .output()
        .unwrap();
    assert!(
        helm.status.success(),
        "real Helm is required for the CLI consumer render"
    );
    let helm = String::from_utf8(helm.stdout).unwrap();
    for value in ["", "{}"] {
        for surface in ["up", "apply"] {
            let config = format!(
                "{}set:\n  security.gvisor.mode: off\n  worker.adapterCredentials: {value:?}\n",
                installation_for_the_stateful_guard()
            );
            let mut existing = external_mail_sources();
            existing["mailAdapter"]["ingressEnabled"] = json!(false);
            existing["mailAdapter"]["agentmail"]["httpsCidrs"] = json!(["203.0.113.10/32"]);
            let fixture = HelmFixture::new(&config, HelmValuesResponse::Object(existing));
            let env = [("CURIE_TEST_REAL_HELM", helm.trim())];
            let output = if surface == "up" {
                fixture.cluster_up_with(
                    &[
                        "--fake-model",
                        "--set",
                        "security.gvisor.mode=off",
                        "--set",
                        &format!("worker.adapterCredentials={value}"),
                    ],
                    &env,
                )
            } else {
                fixture.apply(
                    &[
                        "--chart",
                        concat!(env!("CARGO_MANIFEST_DIR"), "/../charts/curie"),
                    ],
                    &env,
                )
            };
            json_output(
                output,
                &format!("{surface}: real Helm accepts empty map {value:?}"),
            );
        }
    }
}

#[test]
fn changing_mail_egress_source_requires_an_explicit_worker_pair_decision() {
    for set in [
        "mailAdapter.egressSecret=new-inline",
        "mailAdapter.egressSecretExistingSecret=",
        "mailAdapter.egressSecretExistingSecretKey=new-egress-key",
    ] {
        let (key, value) = set.split_once('=').unwrap();
        let config = format!(
            "{}set:\n  {key}: {value:?}\n",
            installation_for_the_stateful_guard()
        );
        for surface in ["up", "apply", "diff"] {
            let fixture =
                HelmFixture::new(&config, HelmValuesResponse::Object(external_mail_sources()));
            let output = match surface {
                "up" => fixture.cluster_up_with(&["--fake-model", "--set", set], &[]),
                "apply" => fixture.apply(&[], &[]),
                _ => fixture.diff(&[]),
            };
            assert!(
                !output.status.success(),
                "{surface}: unpaired egress source change must be refused before Helm"
            );
            assert!(
                !fixture.calls().contains("upgrade --install"),
                "refusal must precede mutation"
            );
            let visible = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                visible.contains("paired worker"),
                "actionable paired-source refusal: {visible}"
            );
        }
    }
    // A paired switch can use the chart's derived worker credential map.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Object(external_mail_sources()),
    );
    json_output(
        fixture.cluster_up_with(
            &[
                "--fake-model",
                "--set",
                "mailAdapter.egressSecret=new-inline",
                "--set",
                "worker.adapterCredentialsExistingSecret=",
            ],
            &[],
        ),
        "paired inline switch",
    );
    // Repeating an existing source is not a source change.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Object(external_mail_sources()),
    );
    json_output(
        fixture.cluster_up_with(
            &[
                "--fake-model",
                "--set",
                "mailAdapter.egressSecretExistingSecret=acme-egress",
            ],
            &[],
        ),
        "reassert unchanged source",
    );
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Object(external_mail_sources()),
    );
    json_output(
        fixture.cluster_up_with(
            &[
                "--fake-model",
                "--set",
                "mailAdapter.egressSecretExistingSecretKey=egress-key",
            ],
            &[],
        ),
        "reassert unchanged source key",
    );
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Object(external_mail_sources()),
    );
    json_output(
        fixture.cluster_up_with(
            &[
                "--fake-model",
                "--set",
                "mailAdapter.egressSecretExistingSecretKey=new-egress-key",
                "--set",
                "worker.adapterCredentialsExistingSecretKey=new-worker-key",
            ],
            &[],
        ),
        "paired source key rotation",
    );
}

#[test]
fn apply_and_plain_up_preserve_omitted_mail_but_honor_explicit_source_clear() {
    let existing = json!({
        "mailAdapter": {
            "deploy": true,
            "inbox": "mail@example.com",
            "channelToken": "obsolete-inline-token",
            "channelTokenExistingSecret": "acme-mail-credentials",
            "channelTokenExistingSecretKey": "channel-key",
            "allowedSenders": ["operator@example.com"],
            "pollIntervalSeconds": 37
        },
        "worker": {
            "adapterCredentialsExistingSecret": "acme-mail-credentials",
            "adapterCredentialsExistingSecretKey": "worker-map"
        }
    });
    for surface in ["up", "apply", "apply-clear"] {
        let config = if surface == "apply-clear" {
            format!(
                "{}set:\n  mailAdapter.channelTokenExistingSecret: \"\"\n",
                installation_for_the_stateful_guard()
            )
        } else {
            installation_for_the_stateful_guard().to_string()
        };
        let fixture = HelmFixture::new(&config, HelmValuesResponse::Object(existing.clone()));
        let captured = fixture.temp.path().join("mail-values.json");
        let env = [("CURIE_TEST_CAPTURE_MAIL_VALUES", captured.to_str().unwrap())];
        let output = if surface == "up" {
            fixture.cluster_up_with(&["--fake-model"], &env)
        } else {
            fixture.apply(&[], &env)
        };
        json_output(output, surface);
        let actual: Value =
            serde_json::from_slice(&fs::read(captured).expect("Helm received mail values"))
                .unwrap();
        assert_eq!(actual["mailAdapter"]["deploy"], true);
        assert_eq!(actual["mailAdapter"]["pollIntervalSeconds"], 37);
        assert_eq!(
            actual["mailAdapter"]["allowedSenders"],
            json!(["operator@example.com"])
        );
        assert!(actual["mailAdapter"].get("channelToken").is_none());
        if surface == "apply-clear" {
            assert!(actual["mailAdapter"]
                .get("channelTokenExistingSecret")
                .is_none());
            assert!(fixture
                .calls()
                .contains("--set-string mailAdapter.channelTokenExistingSecret="));
        } else {
            assert_eq!(
                actual["mailAdapter"]["channelTokenExistingSecret"],
                "acme-mail-credentials"
            );
        }
        let diff = json_output(fixture.diff(&[]), "diff retained mail");
        let entries = diff["entries"].as_array().unwrap();
        let deploy = entries
            .iter()
            .find(|entry| entry["key"] == "mailAdapter.deploy")
            .unwrap();
        assert_eq!(deploy["kind"], "preserved");
        let stale = entries
            .iter()
            .find(|entry| entry["key"] == "mailAdapter.channelToken")
            .unwrap();
        assert_eq!(
            stale["kind"], "change",
            "diff must disclose stripping the stale inline copy"
        );
        assert!(!fixture.calls().contains("obsolete-inline-token"));
    }
}

#[test]
fn cluster_up_rerun_preserves_existing_runner_model_and_egress_configuration() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        recorded_runner_values(),
    );

    let output = fixture.cluster_up_without_credentials(&[
        (
            "CURIE_TEST_EXPECT_RUNNER_CREDENTIAL",
            RERUN_CREDENTIAL_SENTINEL,
        ),
        ("CURIE_TEST_EXPECT_RUNNER_MODEL", RERUN_MODEL_SENTINEL),
        ("CURIE_TEST_EXPECT_RUNNER_EGRESS", RERUN_EGRESS_CIDR),
    ]);
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !visible.contains(RERUN_CREDENTIAL_SENTINEL),
        "the credential sentinel leaked into command output: {visible}"
    );
    assert!(
        !visible.contains("the sandbox is sealed -- no egress opened")
            && !visible.contains("Pass --allow-egress-host"),
        "a rerun preserving egress must not claim it is sealed or ask the operator to reopen it: {visible}"
    );
    json_output(output, "cluster up rerun");

    let calls = fixture.calls();
    assert!(
        calls.contains("HELM_CALL: get values parity -n parity -o json"),
        "a rerun must read the recorded release values: {calls}"
    );
    assert!(
        calls.contains("RUNNER_CREDENTIAL_PRESERVED: yes"),
        "a rerun must preserve the recorded runner credential through Helm's private values file: {calls}"
    );
    assert!(
        calls.contains("RUNNER_MODEL_PRESERVED: yes"),
        "a rerun must preserve the recorded runner model: {calls}"
    );
    assert!(
        calls.contains("RUNNER_REAL_MODE_PRESERVED: yes"),
        "a rerun with a recorded credential must keep the real model enabled: {calls}"
    );
    assert!(
        calls.contains("RUNNER_EGRESS_PRESERVED: yes"),
        "a rerun must preserve the recorded runner egress route: {calls}"
    );
    assert!(
        !calls.contains(RERUN_CREDENTIAL_SENTINEL),
        "the credential sentinel leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_detected_credential_without_provider_infers_bounded_egress() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.cluster_up_with(&[], &[("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL)]);
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    json_output(output, "cluster up with an inferred OpenRouter provider");
    assert!(
        visible.contains("applying `--allow-egress-host openrouter`"),
        "the recognized credential must infer its provider route: {visible}"
    );
    assert!(
        !visible.contains(OPENROUTER_CREDENTIAL),
        "the credential leaked into command output: {visible}"
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("HELM_CALL: upgrade "),
        "the inferred provider must not block installation: {calls}"
    );
    assert!(
        calls.contains("security.networkPolicy.allowedEgress"),
        "the inferred provider must open only its resolved egress route: {calls}"
    );
    assert!(
        !calls.contains(OPENROUTER_CREDENTIAL),
        "the sealed credential leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_explicit_openrouter_egress_accepts_detected_credential() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let provider_egress = json!({"openrouter.ai": [IPV4, IPV6]}).to_string();

    let output = fixture.cluster_up_with(
        &["--allow-egress-host", "openrouter"],
        &[
            ("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL),
            ("CURIE_TEST_PROVIDER_EGRESS_JSON", provider_egress.as_str()),
        ],
    );
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    json_output(output, "cluster up with explicit OpenRouter egress");
    assert!(
        !visible.contains(OPENROUTER_CREDENTIAL),
        "the OpenRouter credential leaked into command output: {visible}"
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("HELM_CALL: upgrade "),
        "the explicit provider must reach the Helm install: {calls}"
    );
    assert!(
        calls.contains(&format!(
            "security.networkPolicy.allowedEgress[0].cidr={IPV4}/32"
        )),
        "the explicit OpenRouter IPv4 route must reach Helm: {calls}"
    );
    assert!(
        calls.contains(&format!(
            "security.networkPolicy.allowedEgress[1].cidr={IPV6}/128"
        )),
        "the explicit OpenRouter IPv6 route must reach Helm: {calls}"
    );
    assert!(
        !calls.contains(OPENROUTER_CREDENTIAL),
        "the OpenRouter credential leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_multi_provider_egress_accepts_matching_detected_credential() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let provider_egress = json!({
        "api.anthropic.com": [IPV4],
        "openrouter.ai": [IPV6]
    })
    .to_string();

    let output = fixture.cluster_up_with(
        &[
            "--allow-egress-host",
            "anthropic",
            "--allow-egress-host",
            "openrouter",
        ],
        &[
            ("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL),
            ("CURIE_TEST_PROVIDER_EGRESS_JSON", provider_egress.as_str()),
        ],
    );
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    json_output(output, "cluster up with two explicit providers");
    assert!(
        !visible.contains(OPENROUTER_CREDENTIAL),
        "the OpenRouter credential leaked into command output: {visible}"
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("HELM_CALL: upgrade "),
        "one matching provider in a larger list must allow installation: {calls}"
    );
    for expected in [
        format!("security.networkPolicy.allowedEgress[0].cidr={IPV4}/32"),
        format!("security.networkPolicy.allowedEgress[1].cidr={IPV6}/128"),
    ] {
        assert!(
            calls.contains(&expected),
            "both explicit provider routes must reach Helm: {calls}"
        );
    }
    assert!(
        !calls.contains(OPENROUTER_CREDENTIAL),
        "the OpenRouter credential leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_rejects_a_contradictory_provider_before_dns_and_helm() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.cluster_up_with(
        &["--allow-egress-host", "anthropic"],
        &[
            ("CURIE_CREDENTIALS", OPENROUTER_CREDENTIAL),
            (
                "CURIE_TEST_PROVIDER_EGRESS_JSON",
                "PLACEHOLDER invalid provider DNS injection",
            ),
        ],
    );
    assert_provider_contradiction(
        output,
        OPENROUTER_CREDENTIAL,
        "openrouter",
        "anthropic",
        "cluster up with a contradictory provider",
    );
    assert!(
        fixture.calls().is_empty(),
        "the contradiction must stop before Helm or kubectl: {}",
        fixture.calls()
    );
}

#[test]
fn cluster_up_rejects_operator_set_credential_before_dns_and_helm() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let credential_set = format!("agentSandbox.runner.credentials={OPENROUTER_CREDENTIAL}");

    let output = fixture.cluster_up_with(
        &[
            "--allow-egress-host",
            "anthropic",
            "--set",
            credential_set.as_str(),
        ],
        &[(
            "CURIE_TEST_PROVIDER_EGRESS_JSON",
            "PLACEHOLDER invalid provider DNS injection",
        )],
    );
    assert_provider_contradiction(
        output,
        OPENROUTER_CREDENTIAL,
        "openrouter",
        "anthropic",
        "cluster up with an operator set OpenRouter credential and Anthropic egress",
    );
    assert!(
        fixture.calls().is_empty(),
        "the operator set contradiction must stop before Helm or kubectl: {}",
        fixture.calls()
    );
}

#[test]
fn cluster_up_rejects_anthropic_credential_with_openrouter_before_dns_and_helm() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.cluster_up_with(
        &["--allow-egress-host", "openrouter"],
        &[
            ("CURIE_CREDENTIALS", ANTHROPIC_CREDENTIAL),
            (
                "CURIE_TEST_PROVIDER_EGRESS_JSON",
                "PLACEHOLDER invalid provider DNS injection",
            ),
        ],
    );
    assert_provider_contradiction(
        output,
        ANTHROPIC_CREDENTIAL,
        "anthropic",
        "openrouter",
        "cluster up with an Anthropic credential and OpenRouter egress",
    );
    assert!(
        fixture.calls().is_empty(),
        "the Anthropic contradiction must stop before Helm or kubectl: {}",
        fixture.calls()
    );
}

#[test]
fn cluster_up_explicit_openrouter_accepts_ambiguous_bare_sk_credential() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let provider_egress = json!({"openrouter.ai": [IPV4, IPV6]}).to_string();

    let output = fixture.cluster_up_with(
        &["--allow-egress-host", "openrouter"],
        &[
            ("CURIE_CREDENTIALS", AMBIGUOUS_MODEL_CREDENTIAL),
            ("CURIE_TEST_PROVIDER_EGRESS_JSON", provider_egress.as_str()),
        ],
    );
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    json_output(
        output,
        "cluster up with an explicit provider and an ambiguous bare sk credential",
    );
    assert!(
        !visible.contains(AMBIGUOUS_MODEL_CREDENTIAL),
        "the ambiguous credential leaked into command output: {visible}"
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("HELM_CALL: upgrade "),
        "an explicit provider must allow an ambiguous credential through to Helm: {calls}"
    );
    for expected in [
        format!("security.networkPolicy.allowedEgress[0].cidr={IPV4}/32"),
        format!("security.networkPolicy.allowedEgress[1].cidr={IPV6}/128"),
    ] {
        assert!(
            calls.contains(&expected),
            "the explicit OpenRouter route must reach Helm: {calls}"
        );
    }
    assert!(
        !calls.contains(AMBIGUOUS_MODEL_CREDENTIAL),
        "the ambiguous credential leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_rejects_preserved_openrouter_credential_before_dns_or_helm_mutation() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        recorded_runner_values_with_credential(OPENROUTER_CREDENTIAL),
    );

    let output = fixture.cluster_up_with(
        &["--allow-egress-host", "anthropic"],
        &[(
            "CURIE_TEST_PROVIDER_EGRESS_JSON",
            "PLACEHOLDER invalid provider DNS injection",
        )],
    );
    assert_provider_contradiction(
        output,
        OPENROUTER_CREDENTIAL,
        "openrouter",
        "anthropic",
        "cluster up with a preserved OpenRouter credential and Anthropic egress",
    );
    assert_only_existing_values_read(&fixture, ExistingValuesConsumer::ClusterUp);
    assert!(
        !fixture.calls().contains(OPENROUTER_CREDENTIAL),
        "the preserved credential leaked into the command log: {}",
        fixture.calls()
    );
}

#[test]
fn cluster_up_explicit_model_modes_suppress_recorded_provider_configuration() {
    for (name, args, expected_value) in [
        (
            "local model",
            &[
                "--local-model",
                "qwen3:4b",
                "--set",
                "inference.pullModel=false",
            ] as &[&str],
            Some("inference.model=qwen3:4b"),
        ),
        ("fake model", &["--fake-model"], None),
    ] {
        let fixture = HelmFixture::new(
            installation_for_the_stateful_guard(),
            recorded_runner_values(),
        );
        let output = fixture.cluster_up_with(
            args,
            &[("CURIE_TEST_EXPECT_RECORDED_PROVIDER_SUPPRESSED", "1")],
        );
        let visible = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !visible.contains(RERUN_CREDENTIAL_SENTINEL),
            "the recorded credential leaked during {name}: {visible}"
        );
        json_output(output, name);

        let calls = fixture.calls();
        assert!(
            calls.contains("RUNNER_RECORDED_PROVIDER_SUPPRESSED: yes"),
            "{name} must suppress the recorded provider credential and model: {calls}"
        );
        if let Some(expected_value) = expected_value {
            assert!(
                calls.contains(expected_value),
                "{name} must select the requested local model: {calls}"
            );
        }
        assert!(
            !calls.contains(RERUN_CREDENTIAL_SENTINEL),
            "the recorded credential leaked into the command log during {name}: {calls}"
        );
    }
}

#[test]
fn cluster_up_explicit_model_replaces_recorded_model() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        recorded_runner_values(),
    );
    let output = fixture.cluster_up_with(&["--set", OVERRIDE_MODEL_SET], &[]);
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !visible.contains(RERUN_CREDENTIAL_SENTINEL),
        "the recorded credential leaked while replacing the model: {visible}"
    );
    json_output(output, "cluster up with explicit model");

    let calls = fixture.calls();
    assert!(
        calls.contains(OVERRIDE_MODEL_SET),
        "the explicit model must reach Helm: {calls}"
    );
    assert!(
        !calls.contains(RERUN_MODEL_SENTINEL),
        "the recorded model must not accompany the explicit model: {calls}"
    );
    assert!(
        !calls.contains(RERUN_CREDENTIAL_SENTINEL),
        "the recorded credential leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_explicit_egress_replaces_recorded_egress() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        recorded_runner_values(),
    );
    let output = fixture.cluster_up_with(&["--set", OVERRIDE_EGRESS_SET], &[]);
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !visible.contains(RERUN_CREDENTIAL_SENTINEL),
        "the recorded credential leaked while replacing egress: {visible}"
    );
    json_output(output, "cluster up with explicit egress");

    let calls = fixture.calls();
    assert!(
        calls.contains(OVERRIDE_EGRESS_SET),
        "the explicit egress route must reach Helm: {calls}"
    );
    assert!(
        !calls.contains(RERUN_EGRESS_CIDR),
        "the recorded egress route must not accompany the explicit route: {calls}"
    );
    assert!(
        !calls.contains(RERUN_CREDENTIAL_SENTINEL),
        "the recorded credential leaked into the command log: {calls}"
    );
}

#[test]
fn cluster_up_fresh_release_without_credentials_stays_fake() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    json_output(
        fixture.cluster_up_without_credentials(&[("CURIE_TEST_EXPECT_FRESH_FAKE_MODEL", "1")]),
        "fresh cluster up",
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("RUNNER_FRESH_FAKE_MODE: yes"),
        "a fresh release without credentials must leave the runner in fake mode: {calls}"
    );
}

#[test]
fn apply_and_diff_refuse_inference_without_an_explicit_asset_policy_before_helm() {
    let config = "version: 1\ninstall:\n  namespace: parity\n  release: parity\nplatform:\n  inference: true\n";

    for verb in ["apply", "diff"] {
        let fixture = HelmFixture::new(config, HelmValuesResponse::Absent);
        let output = match verb {
            "apply" => fixture.apply_dry_run(&[]),
            "diff" => fixture.diff(&[]),
            _ => unreachable!(),
        };

        assert_eq!(
            output.status.code(),
            Some(2),
            "an ambiguous inference asset policy is a usage error for {verb}"
        );
        let error = json_error(output, verb);
        let guidance = format!("{} {}", error["error"], error["fix"]);
        for recovery in ["inference_persistence: true", "inference_pull_model: false"] {
            assert!(
                guidance.contains(recovery),
                "{verb} must give both explicit curie.yaml recovery choices ({recovery}): {error}"
            );
        }
        assert!(
            fixture.calls().is_empty(),
            "{verb} must refuse before reading or mutating Helm state: {}",
            fixture.calls()
        );
    }
}

#[test]
fn absent_release_diff_matches_apply_dry_run_for_effective_installation_values() {
    let fixture = HelmFixture::new(
        installation_with_effective_values(),
        HelmValuesResponse::Absent,
    );
    let env = [
        ("CURIE_APPLY_TEST_MODEL_KEY", MODEL_VALUE),
        ("CURIE_APPLY_TEST_GITHUB_TOKEN", GITHUB_VALUE),
        ("CURIE_MODEL", "runner-model-for-plan"),
    ];

    let apply = plan(fixture.apply_dry_run(&env));
    let diff = json_output(fixture.diff(&env), "diff");

    assert_eq!(diff["release_exists"], false, "{diff}");
    assert!(
        diff["changes"].as_u64().is_some_and(|changes| changes > 0),
        "an absent release must have create changes: {diff}"
    );
    assert!(
        apply.contains("agentSandbox.runner.credentials=model-va***"),
        "apply must use and mask the resolved model credential: {apply}"
    );
    assert!(
        apply.contains("api.githubToken=github-v***"),
        "apply must use and mask the resolved GitHub credential: {apply}"
    );
    assert!(
        !apply.contains(MODEL_VALUE) && !apply.contains(GITHUB_VALUE),
        "apply must not leak credential values: {apply}"
    );
    assert_added(&diff, "agentSandbox.runner.credentials", "<secret>");
    assert_added(&diff, "api.githubToken", "<secret>");
    for key in ["inference.deploy", "inference.persistence.enabled"] {
        let setting = format!("--set {key}=true");
        assert!(
            apply.contains(&setting),
            "apply must plan the modeled inference boolean through Helm's typed lane: {apply}"
        );
        assert_added(&diff, key, "true");
    }

    let apply_values = helm_values(&apply);
    for (key, apply_value) in &apply_values {
        let diff_entry = entry(&diff, key);
        assert_eq!(diff_entry["kind"], "add", "{key}: {diff_entry}");
        let diff_value = diff_entry["to"]
            .as_str()
            .unwrap_or_else(|| panic!("diff entry has no target value for {key}: {diff_entry}"));
        if diff_value == "<secret>" {
            assert!(
                apply_value.ends_with("***"),
                "apply must expose only a masked value for {key}: {apply}"
            );
        } else {
            assert_eq!(apply_value, diff_value, "{key}: {diff_entry}");
        }
    }

    let diff_keys = diff["entries"]
        .as_array()
        .expect("diff entries array")
        .iter()
        .filter(|entry| entry["kind"] != "preserved")
        .map(|entry| entry["key"].as_str().expect("diff entry key"))
        .collect::<BTreeSet<_>>();
    let apply_keys = apply_values
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(diff_keys, apply_keys, "apply plan: {apply}; diff: {diff}");
}

#[test]
fn explicit_no_pull_inference_policy_matches_apply_and_diff_typed_plans() {
    let fixture = HelmFixture::new(
        "version: 1\ninstall:\n  namespace: parity\n  release: parity\nplatform:\n  inference: true\n  inference_pull_model: false\n",
        HelmValuesResponse::Absent,
    );

    let apply = plan(fixture.apply_dry_run(&[]));
    let diff = json_output(fixture.diff(&[]), "diff");
    for (key, value) in [
        ("inference.deploy", "true"),
        ("inference.pullModel", "false"),
    ] {
        let typed = format!("--set {key}={value}");
        assert!(
            apply.contains(&typed),
            "apply must plan the modeled inference value through Helm's typed lane: {apply}"
        );
        assert_added(&diff, key, value);
    }
    assert!(
        !apply.contains("--set-string inference.pullModel=false"),
        "a string false is truthy to Helm and must not represent no-pull: {apply}"
    );
}

#[test]
fn inference_absent_or_false_needs_no_asset_policy_and_keeps_prior_plans() {
    for (label, config, expected_deploy) in [
        (
            "absent",
            "version: 1\ninstall:\n  namespace: parity\n  release: parity\nplatform:\n  ui: false\n",
            None,
        ),
        (
            "false",
            "version: 1\ninstall:\n  namespace: parity\n  release: parity\nplatform:\n  inference: false\n",
            Some("false"),
        ),
    ] {
        let fixture = HelmFixture::new(config, HelmValuesResponse::Absent);
        let apply = plan(fixture.apply_dry_run(&[]));
        let diff = json_output(fixture.diff(&[]), "diff");

        match expected_deploy {
            Some(value) => {
                assert!(
                    apply.contains(&format!("--set inference.deploy={value}")),
                    "explicit inference=false must retain its typed apply value: {apply}"
                );
                assert_added(&diff, "inference.deploy", value);
            }
            None => {
                assert!(
                    !apply.contains("inference.deploy"),
                    "absent inference must continue to leave the chart default alone: {apply}"
                );
                assert!(
                    diff["entries"]
                        .as_array()
                        .expect("diff entries array")
                        .iter()
                        .all(|entry| entry["key"] != "inference.deploy"),
                    "absent inference must not appear in diff: {diff}"
                );
            }
        }
        for policy_key in ["inference.persistence.enabled", "inference.pullModel"] {
            assert!(
                !apply.contains(policy_key),
                "inference {label} must not invent {policy_key}: {apply}"
            );
            assert!(
                diff["entries"]
                    .as_array()
                    .expect("diff entries array")
                    .iter()
                    .all(|entry| entry["key"] != policy_key),
                "inference {label} must not invent {policy_key}: {diff}"
            );
        }
    }
}

#[test]
fn apply_and_diff_do_not_enforce_cluster_up_credential_provider_guard() {
    let provider_egress = provider_egress_fixture();
    let env = [
        ("CURIE_APPLY_TEST_MODEL_KEY", OPENROUTER_CREDENTIAL),
        ("CURIE_TEST_PROVIDER_EGRESS_JSON", provider_egress.as_str()),
    ];

    let apply_fixture = HelmFixture::new(
        installation_with_provider_contradiction(),
        HelmValuesResponse::Absent,
    );
    let apply_output = apply_fixture.apply(&[], &env);
    let apply_visible = format!(
        "{}{}",
        String::from_utf8_lossy(&apply_output.stdout),
        String::from_utf8_lossy(&apply_output.stderr)
    );
    json_output(
        apply_output,
        "apply with a cluster up provider contradiction",
    );
    assert!(
        apply_fixture.calls().contains("HELM_CALL: upgrade "),
        "apply must remain outside the cluster up guard: {}",
        apply_fixture.calls()
    );
    assert!(
        !apply_visible.contains(OPENROUTER_CREDENTIAL)
            && !apply_fixture.calls().contains(OPENROUTER_CREDENTIAL),
        "apply leaked the model credential"
    );

    let diff_fixture = HelmFixture::new(
        installation_with_provider_contradiction(),
        HelmValuesResponse::Absent,
    );
    let diff_output = diff_fixture.diff(&env);
    let diff_visible = format!(
        "{}{}",
        String::from_utf8_lossy(&diff_output.stdout),
        String::from_utf8_lossy(&diff_output.stderr)
    );
    let diff = json_output(diff_output, "diff with a cluster up provider contradiction");
    assert_eq!(diff["release_exists"], false, "{diff}");
    assert!(
        diff["changes"].as_u64().is_some_and(|changes| changes > 0),
        "diff must still produce an effective plan: {diff}"
    );
    assert!(
        !diff_visible.contains(OPENROUTER_CREDENTIAL)
            && !diff_fixture.calls().contains(OPENROUTER_CREDENTIAL),
        "diff leaked the model credential"
    );
}

#[test]
fn apply_string_egress_does_not_warn_that_model_credential_is_sealed() {
    let fixture = HelmFixture::new(
        "version: 1\ninstall:\n  namespace: parity\n  release: parity\ncredentials:\n  model: CURIE_APPLY_TEST_MODEL_KEY\nset:\n  security.networkPolicy.allowedEgress[0].cidr: \"198.51.100.20/32\"\n",
        HelmValuesResponse::Absent,
    );
    let live_statefulset = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "parity-rustfs"},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "rustfs"}}}
        }]
    })
    .to_string();

    let output = fixture.apply(
        &[],
        &[
            ("CURIE_APPLY_TEST_MODEL_KEY", MODEL_VALUE),
            ("CURIE_TEST_KUBECTL_STS", live_statefulset.as_str()),
        ],
    );
    let visible = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !visible.contains("the sandbox is sealed"),
        "an allowed egress string override must suppress the sealed credential warning: {visible}"
    );
    assert!(
        !visible.contains(MODEL_VALUE),
        "the model credential leaked during apply: {visible}"
    );
    json_output(output, "apply with string egress override");

    let calls = fixture.calls();
    assert!(
        calls.contains(&format!("--set-string {OVERRIDE_EGRESS_SET}")),
        "the string egress override must reach Helm through its original value lane: {calls}"
    );
    assert!(
        !calls.contains(MODEL_VALUE),
        "the model credential leaked into the command log: {calls}"
    );
}

#[test]
fn numeric_looking_declared_set_values_use_helm_string_semantics() {
    let fixture = HelmFixture::new(
        "version: 1\ninstall:\n  namespace: parity\n  release: parity\nplatform:\n  ui: false\nset:\n  api.githubAppId: \"4475970\"\n  example.label: plain\n  example.leadingZero: \"00123\"\n  ui.deploy: disabled\n  worker.replicas: \"3\"\n",
        HelmValuesResponse::Absent,
    );
    let live_statefulset = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "metadata": {"name": "parity-rustfs"},
            "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "rustfs"}}}
        }]
    })
    .to_string();

    let output = fixture.apply(
        &[],
        &[("CURIE_TEST_KUBECTL_STS", live_statefulset.as_str())],
    );
    let calls = fixture.calls();
    assert!(
        output.status.success(),
        "apply failed with stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let template = calls
        .lines()
        .find(|line| line.starts_with("HELM_CALL: template "))
        .unwrap_or_else(|| panic!("stateful guard did not render the chart:\n{calls}"));
    let upgrade = calls
        .lines()
        .find(|line| line.starts_with("HELM_CALL: upgrade "))
        .unwrap_or_else(|| panic!("apply did not upgrade the release:\n{calls}"));

    for command in [template, upgrade] {
        let tokens = command.split_whitespace().collect::<Vec<_>>();
        assert!(
            tokens
                .windows(2)
                .any(|pair| pair == ["--set", "ui.deploy=false"]),
            "modeled values must retain Helm typed semantics: {command}"
        );
        let modeled_index = tokens
            .windows(2)
            .position(|pair| pair == ["--set", "ui.deploy=false"])
            .expect("modeled ui value");
        let declared_index = tokens
            .windows(2)
            .position(|pair| pair == ["--set-string", "ui.deploy=disabled"])
            .unwrap_or_else(|| panic!("declared ui override missing: {command}"));
        assert!(
            modeled_index < declared_index,
            "declared string override must follow the modeled value: {command}"
        );
        for setting in [
            "api.githubAppId=4475970",
            "example.label=plain",
            "example.leadingZero=00123",
            "ui.deploy=disabled",
            "worker.replicas=3",
        ] {
            assert!(
                tokens
                    .windows(2)
                    .any(|pair| pair == ["--set-string", setting]),
                "declared value must use Helm string semantics: {command}"
            );
            assert!(
                !tokens.windows(2).any(|pair| pair == ["--set", setting]),
                "declared value reached Helm through the typed lane: {command}"
            );
        }
    }

    let diff = json_output(fixture.diff(&[]), "diff");
    for (key, value) in [
        ("ui.deploy", "disabled"),
        ("api.githubAppId", "<secret>"),
        ("example.label", "plain"),
        ("example.leadingZero", "00123"),
        ("worker.replicas", "3"),
    ] {
        assert_added(&diff, key, value);
    }
}

#[test]
fn empty_github_token_clear_is_shared_by_apply_and_diff() {
    let fixture = HelmFixture::new(
        "version: 1\ninstall:\n  namespace: parity\n  release: parity\nset:\n  api.githubToken: \"\"\n",
        HelmValuesResponse::Absent,
    );

    let apply = plan(fixture.apply_dry_run(&[]));
    let diff = json_output(fixture.diff(&[]), "diff");

    let apply_values = helm_values(&apply);
    assert_eq!(
        apply_values.get("api.githubToken").map(String::as_str),
        Some(""),
        "apply must carry the exact empty GitHub token clear: {apply}"
    );
    assert_added(&diff, "api.githubToken", "<secret>");
    assert_diff_keys(
        &diff,
        &[
            "api.githubToken",
            "config.schemaVersion",
            "langfuse.web.service.type",
            "ui.service.type",
        ],
    );
    assert_added(&diff, "config.schemaVersion", "0.9.0");
}

#[test]
fn diff_marks_only_extra_live_egress_for_reset_and_preserves_live_github_token() {
    let fixture = HelmFixture::new(
        "version: 1\ninstall:\n  namespace: parity\n  release: parity\nplatform:\n  egress:\n    - host: anthropic\n",
        HelmValuesResponse::Object(json!({
            "api": {"githubToken": "ghp-live-token"},
            "security": {"networkPolicy": {"allowedEgress": [
                {"cidr": "1.1.1.1/32", "ports": [{"port": 443, "protocol": "TCP"}]},
                {"cidr": "2606:4700:4700::1111/128", "ports": [{"port": 443, "protocol": "TCP"}]},
                {"cidr": "9.9.9.9/32", "ports": [{"port": 443, "protocol": "TCP"}]}
            ]}}
        })),
    );
    let egress = provider_egress_fixture();
    let diff = json_output(
        fixture.diff(&[("CURIE_TEST_PROVIDER_EGRESS_JSON", egress.as_str())]),
        "diff",
    );

    for key in [
        "security.networkPolicy.allowedEgress[0].cidr",
        "security.networkPolicy.allowedEgress[1].cidr",
        "security.networkPolicy.allowedEgress[0].ports[0].port",
        "security.networkPolicy.allowedEgress[1].ports[0].port",
    ] {
        assert_eq!(entry(&diff, key)["kind"], "unchanged", "{key}: {diff}");
    }
    for key in [
        "security.networkPolicy.allowedEgress[2].cidr",
        "security.networkPolicy.allowedEgress[2].ports[0].port",
    ] {
        assert_eq!(
            entry(&diff, key)["kind"],
            "reset to chart default",
            "{key}: {diff}"
        );
    }
    let github = entry(&diff, "api.githubToken");
    assert_eq!(github["kind"], "preserved", "{github}");
    assert_eq!(github["from"], "<secret>", "{github}");
    assert!(
        !diff.to_string().contains("ghp-live-token"),
        "diff must not leak the preserved GitHub token: {diff}"
    );
}

#[test]
fn apply_and_diff_report_the_same_curie_model_conflict() {
    let fixture = HelmFixture::new(
        "version: 1\ninstall:\n  namespace: parity\n  release: parity\nset:\n  agentSandbox.runner.model: file-model\n",
        HelmValuesResponse::Absent,
    );
    let env = [("CURIE_MODEL", "shell-model")];
    let apply = fixture.apply_dry_run(&env);
    let diff = fixture.diff(&env);

    assert_eq!(apply.status.code(), diff.status.code());
    let apply_error = json_error(apply, "apply --dry-run");
    let diff_error = json_error(diff, "diff");
    let apply_message = apply_error["error"]
        .as_str()
        .expect("apply error message string");
    assert!(
        apply_message.contains("CURIE_MODEL")
            && apply_message.contains("agentSandbox.runner.model"),
        "apply returned the wrong conflict error: {apply_error}"
    );
    assert_eq!(
        apply_error, diff_error,
        "apply and diff must use the same effective-plan conflict validation"
    );
}

#[test]
fn apply_with_both_flags_makes_no_cluster_call() {
    // #1351: --migrate-store and --allow-stateful-removal state contradictory
    // intent, and apply resolved the contradiction by silently dropping the
    // migration and taking the data destroying path. The primary assertion is
    // the ABSENCE of the mutation, not the wording of the refusal.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.apply(&["--migrate-store", "--allow-stateful-removal"], &[]);

    let calls = fixture.calls();
    assert!(
        calls.is_empty(),
        "a contradictory flag pair must touch no cluster at all; recorded calls:\n{calls}"
    );
    assert!(
        !calls.contains("upgrade"),
        "no upgrade may run for a rejected apply; recorded calls:\n{calls}"
    );
    assert!(
        !output.status.success(),
        "a contradictory flag pair must not exit successfully; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // The two flag names only, never clap's full sentence, and no pinned exit
    // code: an operator needs to be told which two flags collided, and a clap
    // patch bump must not fail this.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--migrate-store") && stderr.contains("--allow-stateful-removal"),
        "the refusal must name both colliding flags; stderr:\n{stderr}"
    );
}

#[test]
fn migrate_store_alone_still_migrates() {
    // AC2: the control run. A live minio store plus a chart that renders rustfs
    // is the rename the guard exists for, and --migrate-store alone must still
    // take the migration branch.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();
    let live_after = live_rustfs_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE", live_after.as_str()),
        ],
    );

    let calls = fixture.calls();
    assert!(
        output.status.success(),
        "--migrate-store alone must carry the migration through; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let live_read = calls
        .find("KUBECTL_CALL: get statefulset")
        .unwrap_or_else(|| panic!("the guard must read the live StatefulSets:\n{calls}"));
    let render = calls
        .find("HELM_CALL: template")
        .unwrap_or_else(|| panic!("the guard must render the target chart:\n{calls}"));
    let store_lookup = calls
        .find("KUBECTL_CALL: get svc")
        .unwrap_or_else(|| panic!("the migration must look up the store Service:\n{calls}"));
    let upgrade = calls
        .find("HELM_CALL: upgrade")
        .unwrap_or_else(|| panic!("the migration must upgrade the release:\n{calls}"));
    assert!(
        live_read < render && render < store_lookup && store_lookup < upgrade,
        "the migration branch runs the live read, then the render, then the store lookup, then the upgrade:\n{calls}"
    );
    // The export must be COMPLETE before the upgrade deletes the old store, and
    // the import must run after it. Staging alone, then upgrading, is the
    // failure mode that empties the store.
    let exported = calls
        .find("aws s3 sync s3://curie-bundles /stage")
        .unwrap_or_else(|| panic!("the export must copy the source into staging:\n{calls}"));
    let staged = calls
        .find("find . -type f -printf")
        .unwrap_or_else(|| panic!("the export must inventory what it staged:\n{calls}"));
    let source_capture = calls
        .find("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-minio.parity.svc.cluster.local:9000")
        .unwrap_or_else(|| panic!("the export must capture the final source inventory:\n{calls}"));
    let imported = calls
        .find("aws s3 sync /stage")
        .unwrap_or_else(|| panic!("the import must load the staged objects back:\n{calls}"));
    assert!(
        exported < staged && staged < upgrade && upgrade < imported,
        "the export completes before the upgrade and the import runs after it:\n{calls}"
    );
    let source_read = upgrade
        + calls[upgrade..]
            .find("/migration/source.list")
            .unwrap_or_else(|| {
                panic!("the import must read the persisted source inventory:\n{calls}")
            });
    let target_listing = calls
        .find("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-rustfs.parity.svc.cluster.local:9000")
        .unwrap_or_else(|| panic!("the migration must verify the planned target:\n{calls}"));
    let released = calls
        .rfind("KUBECTL_CALL: delete pod")
        .unwrap_or_else(|| panic!("a verified migration must release staging:\n{calls}"));
    assert!(
        target_listing < released,
        "a verified migration releases the staging pod:\n{calls}"
    );
    assert!(
        exported < source_capture
            && source_capture < upgrade
            && upgrade < source_read
            && source_read < target_listing
            && target_listing < released,
        "the safe path must export, capture source evidence, upgrade, read that evidence, verify the target, then release staging:\n{calls}"
    );
}

#[test]
fn migrate_store_refuses_a_live_store_that_disagrees_with_the_planned_target() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();
    let live_after = live_minio_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE", live_after.as_str()),
        ],
    );

    let calls = fixture.calls();
    let upgrade = calls
        .find("HELM_CALL: upgrade")
        .unwrap_or_else(|| panic!("the migration must reach the planned upgrade:\n{calls}"));
    assert!(
        !output.status.success(),
        "a live minio store cannot satisfy the planned rustfs target; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !calls[upgrade..].contains("aws s3 sync /stage"),
        "the migration must not import into the detected old store:\n{calls}"
    );
    assert!(
        !calls[upgrade..].contains("KUBECTL_CALL: delete pod"),
        "a target disagreement must retain the staged copy:\n{calls}"
    );
}

#[test]
fn migrate_store_refuses_a_component_it_cannot_carry() {
    // AC1 (#1501): the bypass may only wave through removals the migration can
    // actually carry, and it carries the OBJECT STORE alone -- `detect_store`
    // knows `minio` and `rustfs` and nothing else. A batch of {minio gone,
    // postgres gone} satisfied the old all-ComponentGone condition, so apply
    // migrated the store, DELETED the database beside it, and exited 0. The
    // refusal has to land before the export as well as before the upgrade: a
    // staged copy is worthless once the postgres volume is orphaned.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_and_postgres_statefulsets();

    let output = fixture.apply(
        &["--migrate-store"],
        &[("CURIE_TEST_KUBECTL_STS", live_before.as_str())],
    );

    let calls = fixture.calls();
    assert!(
        !output.status.success(),
        "--migrate-store must refuse a batch holding a component it cannot carry; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "the refusal must precede the irreversible upgrade:\n{calls}"
    );
    assert!(
        !calls.contains("aws s3 sync s3://curie-bundles /stage"),
        "nothing may be staged for a migration that will not run:\n{calls}"
    );
    // The fixture runs with `--json`, so the refusal is the error payload on
    // stdout. Assert the COMPONENT name: "some component" would be a wall, and
    // the operator's next move is to find which of their values drops it.
    let reported = String::from_utf8_lossy(&output.stdout);
    assert!(
        reported.contains("postgres"),
        "the refusal must name the component it cannot carry; stdout:\n{reported}"
    );
    // The bare "postgres" substring is not enough on its own: it also matches
    // the live resource name `parity-postgres`, so it stays green even if the
    // bypass falls through to the PRE-EXISTING `stateful_removal_message`.
    // That message ends by telling the operator to "re-run with --migrate-store
    // and apply will carry the data across itself" -- sending them straight
    // back to the flag that just refused them, which is exactly the
    // operator-recruitment loop #1501 is about. So pin both halves: that the
    // refusal is the uncarriable one, and that it does NOT re-offer the flag.
    assert!(
        reported.contains("cannot carry"),
        "the refusal must say --migrate-store cannot carry these component(s); stdout:\n{reported}"
    );
    assert!(
        !reported.contains("apply will carry the data across itself"),
        "the refusal must not re-offer the flag that just refused them (#1501); stdout:\n{reported}"
    );
}

#[test]
fn migrate_store_refuses_a_values_file_that_turns_the_store_off() {
    // AC2 (#1501): the guard and the export must render the SAME chart. The
    // guard renders with the effective values, so a file that points the store
    // at an external instance over a live minio release reads as "the store is
    // gone" and the bypass accepts it. The export used to render with `UpValuePlan::default()`, saw the
    // rustfs the values had switched off, and planned minio -> rustfs -- so the
    // staging pod went up, the upgrade DELETED minio, and only then did the
    // run fail, telling the operator to re-run the upgrade that had already
    // happened. The refusal has to come first, while it is still reversible.
    let fixture = HelmFixture::new(
        installation_that_turns_the_store_off(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[("CURIE_TEST_KUBECTL_STS", live_before.as_str())],
    );

    let calls = fixture.calls();
    assert!(
        !output.status.success(),
        "a values file that leaves no target store cannot be migrated into; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "the refusal must precede the irreversible upgrade:\n{calls}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: run"),
        "no staging pod may be created for a migration that cannot complete:\n{calls}"
    );
    assert!(
        !calls.contains("aws s3 sync s3://curie-bundles /stage"),
        "nothing may be staged for a migration that will not run:\n{calls}"
    );
    let reported = String::from_utf8_lossy(&output.stdout);
    assert!(
        reported.contains("renders no known object store"),
        "the refusal must say the target chart has no store to migrate into; stdout:\n{reported}"
    );
    // The direct AC2 assertion (#1501): the guard and the export must render
    // the chart with the SAME values. Pinning one `--set-string` would survive
    // a mutation that threads only that value and drops the rest of the plan,
    // and a real chart could then again disagree about which StatefulSets
    // exist. Both halves build the command identically -- `helm template
    // <release> <chart> -n <namespace> <value-plan args>` -- so the two
    // full-chart renders must be byte-identical. `--show-only` renders are the
    // priorityclass/preflight probes, not stateful-component detection. The one
    // provably incidental difference is the per-call temp values file, whose
    // name carries a fresh uuid, so that token alone is normalised.
    let full_chart_renders: Vec<String> = calls
        .lines()
        .filter(|line| line.starts_with("HELM_CALL: template "))
        .filter(|line| !line.contains("--show-only"))
        .map(|line| {
            line.split_whitespace()
                .map(|token| {
                    if token.starts_with("/tmp/curie-helm-values-") {
                        "<values-file>"
                    } else {
                        token
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    assert!(
        full_chart_renders.len() >= 2,
        "the guard and the export must each render the full chart:\n{calls}"
    );
    assert_eq!(
        full_chart_renders[0], full_chart_renders[1],
        "the guard and the export must render the chart with the SAME values:\n{calls}"
    );
}

#[test]
fn migrate_store_standalone_import_dry_run_reads_no_live_state() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
            "--dry-run",
        ],
        &[],
    );

    let calls = fixture.calls();
    assert!(
        calls.is_empty(),
        "standalone import dry run must not read the cluster or persisted state:\n{calls}"
    );
    let json = json_output(output, "cluster migrate-store --phase import --dry-run");
    let plan = json["plan"]
        .as_array()
        .expect("standalone import dry run plan array");
    assert!(
        plan.iter()
            .filter_map(Value::as_str)
            .any(|line| line.contains("cat /migration/target")),
        "the plan must show the persisted target read: {json}"
    );
    assert!(
        plan.iter()
            .filter_map(Value::as_str)
            .any(|line| line.contains("cat /migration/source.list")),
        "the plan must show the persisted source proof read: {json}"
    );
}

#[test]
fn migrate_store_split_export_then_import_uses_persisted_evidence() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let chart = repo_root().join("charts/curie");
    let live_before = live_minio_statefulset();
    let inventory = "100 bundle.tar\n200 second_bundle.tar";

    let exported = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "export",
            "--namespace",
            "parity",
            "--release",
            "parity",
            "--chart",
            chart.to_str().expect("UTF 8 chart path"),
        ],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_SOURCE_LIST", inventory),
            ("CURIE_TEST_STAGED_LIST", inventory),
        ],
    );

    let export_json = json_output(exported, "cluster migrate-store --phase export");
    assert_eq!(export_json["phase"], "export", "{export_json}");
    assert_eq!(export_json["to"], "rustfs", "{export_json}");
    assert_eq!(export_json["objects"], 2, "{export_json}");
    assert_eq!(
        fixture.migration_target().as_deref(),
        Some("rustfs\n"),
        "export must persist its planned target for a later process"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n200 second_bundle.tar\n"),
        "export must persist its final source inventory for a later process"
    );

    let before_import = fixture.calls().len();
    let live_after = live_rustfs_statefulset();
    let imported = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
        ],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_after.as_str()),
            ("CURIE_TEST_STAGED_LIST", inventory),
            ("CURIE_TEST_TARGET_LIST", inventory),
        ],
    );

    let import_json = json_output(imported, "cluster migrate-store --phase import");
    assert_eq!(import_json["phase"], "import", "{import_json}");
    assert_eq!(import_json["store"], "rustfs", "{import_json}");
    assert_eq!(import_json["objects"], 2, "{import_json}");
    assert_eq!(import_json["verified"], true, "{import_json}");
    assert_eq!(import_json["staging_kept"], false, "{import_json}");
    let calls = fixture.calls();
    let import_calls = &calls[before_import..];
    assert!(
        import_calls.contains("/migration/target"),
        "the second process must read the target exported by the first:\n{import_calls}"
    );
    assert!(
        import_calls.contains("/migration/source.list"),
        "the second process must read the source inventory exported by the first:\n{import_calls}"
    );
    assert!(
        import_calls.contains("http://parity-rustfs.parity.svc.cluster.local:9000"),
        "the second process must import into the persisted rustfs target:\n{import_calls}"
    );
    assert!(
        fixture.migration_target().is_none() && fixture.migration_source().is_none(),
        "verified import may release the staging pod and its evidence"
    );
}

#[test]
fn migrate_store_standalone_import_refuses_a_detected_store_that_disagrees_with_the_plan() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    fixture.seed_migration_evidence("rustfs", STAGED_OBJECT);
    let live = live_minio_statefulset();

    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
        ],
        &[("CURIE_TEST_KUBECTL_STS", live.as_str())],
    );

    let calls = fixture.calls();
    let error = json_error(output, "cluster migrate-store --phase import");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("rustfs") && message.contains("minio"),
        "the refusal must name the planned and detected stores: {error}"
    );
    assert!(
        calls.contains("/migration/target"),
        "standalone import must read the persisted plan:\n{calls}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: get svc")
            && !calls.contains("aws s3 sync /stage")
            && !calls.contains("KUBECTL_CALL: delete pod"),
        "a plan disagreement must stop before target lookup, import, or staging deletion:\n{calls}"
    );
    assert_eq!(
        fixture.migration_target().as_deref(),
        Some("rustfs\n"),
        "a target disagreement must retain the persisted plan"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n"),
        "a target disagreement must retain the persisted source proof"
    );
}

#[test]
fn migrate_store_standalone_import_refuses_an_unknown_persisted_target() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    fixture.seed_migration_evidence("seaweedfs", STAGED_OBJECT);
    let live = live_rustfs_statefulset();

    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
        ],
        &[("CURIE_TEST_KUBECTL_STS", live.as_str())],
    );

    let calls = fixture.calls();
    let error = json_error(output, "cluster migrate-store --phase import");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("unknown") && message.contains("seaweedfs"),
        "the refusal must name the malformed persisted target: {error}"
    );
    assert!(
        !calls.contains("aws s3 sync /stage") && !calls.contains("KUBECTL_CALL: delete pod"),
        "an unknown target must stop before import or staging deletion:\n{calls}"
    );
    assert_eq!(
        fixture.migration_target().as_deref(),
        Some("seaweedfs\n"),
        "an unknown target must retain the persisted evidence"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n"),
        "an unknown target must retain the source proof"
    );
}

#[test]
fn migrate_store_standalone_import_refuses_both_live_stores() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    fixture.seed_migration_evidence("rustfs", STAGED_OBJECT);
    let live = live_both_store_statefulsets();

    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
        ],
        &[("CURIE_TEST_KUBECTL_STS", live.as_str())],
    );

    let calls = fixture.calls();
    let error = json_error(output, "cluster migrate-store --phase import");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("both minio and rustfs"),
        "the refusal must name the ambiguous live stores: {error}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: get svc")
            && !calls.contains("aws s3 sync /stage")
            && !calls.contains("KUBECTL_CALL: delete pod"),
        "ambiguous live stores must stop before target lookup, import, or staging deletion:\n{calls}"
    );
    assert_eq!(
        fixture.migration_target().as_deref(),
        Some("rustfs\n"),
        "ambiguous live stores must retain the persisted plan"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n"),
        "ambiguous live stores must retain the source proof"
    );
}

#[test]
fn migrate_store_requires_a_successful_source_listing_before_deleting_staging() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();
    let live_after = live_rustfs_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE", live_after.as_str()),
            ("CURIE_TEST_SOURCE_LIST_FAIL", "1"),
        ],
    );

    let calls = fixture.calls();
    let source_listing = calls
        .find("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-minio.parity.svc.cluster.local:9000")
        .unwrap_or_else(|| panic!("verification must list the planned source store:\n{calls}"));
    assert!(
        !output.status.success(),
        "a failed source listing leaves the migration unverified; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let reported = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        reported.contains("source listing failed"),
        "the source listing failure must remain diagnosable:\n{reported}"
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "a failed final source inventory must stop before the destructive upgrade:\n{calls}"
    );
    assert!(
        !calls[source_listing..].contains("KUBECTL_CALL: delete pod"),
        "a failed source listing must retain the staged copy:\n{calls}"
    );
}

#[test]
fn migrate_store_refuses_a_late_source_object_before_the_upgrade() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();
    let live_after = live_rustfs_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE", live_after.as_str()),
            (
                "CURIE_TEST_SOURCE_LIST",
                "100 bundle.tar\n200 late_bundle.tar",
            ),
            ("CURIE_TEST_STAGED_LIST", STAGED_OBJECT),
        ],
    );

    let calls = fixture.calls();
    let source_listing = calls
        .find("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-minio.parity.svc.cluster.local:9000")
        .unwrap_or_else(|| panic!("the export must capture the final source inventory:\n{calls}"));
    assert!(
        calls[..source_listing].contains("aws s3 sync s3://curie-bundles /stage"),
        "the late write check must follow the export copy:\n{calls}"
    );
    assert!(
        !output.status.success(),
        "a source object absent from staging must refuse the migration; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let reported = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        reported.contains("late_bundle.tar"),
        "the late source object must be named in the refusal:\n{reported}"
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade") && !calls.contains("aws s3 sync /stage"),
        "a source versus staging mismatch must stop before upgrade and import:\n{calls}"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n200 late_bundle.tar\n"),
        "the failed preupgrade check must retain its final source evidence"
    );
}

#[test]
fn migrate_store_keeps_staging_when_the_target_omits_a_source_object() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();
    let live_after = live_rustfs_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE", live_after.as_str()),
            (
                "CURIE_TEST_SOURCE_LIST",
                "100 bundle.tar\n200 late_bundle.tar",
            ),
            (
                "CURIE_TEST_STAGED_LIST",
                "100 bundle.tar\n200 late_bundle.tar",
            ),
            ("CURIE_TEST_TARGET_LIST", STAGED_OBJECT),
        ],
    );

    let calls = fixture.calls();
    let source_listing = calls
        .find("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-minio.parity.svc.cluster.local:9000")
        .unwrap_or_else(|| panic!("verification must list the planned source store:\n{calls}"));
    let target_listing = calls
        .find("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-rustfs.parity.svc.cluster.local:9000")
        .unwrap_or_else(|| panic!("verification must list the planned target store:\n{calls}"));
    assert!(
        source_listing < target_listing,
        "verification must compare the source snapshot with the target result:\n{calls}"
    );
    let upgrade = calls.find("HELM_CALL: upgrade").unwrap_or_else(|| {
        panic!("equal source and staging inventories must reach the upgrade:\n{calls}")
    });
    assert!(
        upgrade < target_listing,
        "the target mismatch must be detected after the safe preupgrade check:\n{calls}"
    );
    assert!(
        !output.status.success(),
        "a target missing a source object must be unverified; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let reported = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        reported.contains("late_bundle.tar"),
        "the unsafe target object must be named in the failure:\n{reported}"
    );
    assert!(
        !calls[target_listing..].contains("KUBECTL_CALL: delete pod"),
        "an incomplete target must retain the staged copy:\n{calls}"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n200 late_bundle.tar\n"),
        "failed target verification must retain the persisted source proof"
    );
}

#[test]
fn migrate_store_import_exits_nonzero_when_the_target_listing_fails() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    fixture.seed_migration_evidence("rustfs", STAGED_OBJECT);
    let live = live_rustfs_statefulset();

    let output = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
        ],
        &[
            ("CURIE_TEST_KUBECTL_STS", live.as_str()),
            ("CURIE_TEST_TARGET_LIST_FAIL", "1"),
        ],
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("/migration/target") && calls.contains("/migration/source.list"),
        "standalone import must use the persisted plan and source proof:\n{calls}"
    );
    assert!(
        calls.contains("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-rustfs.parity.svc.cluster.local:9000"),
        "the import must attempt target verification:\n{calls}"
    );
    assert!(
        !output.status.success(),
        "a failed target listing must not report a verified migration; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let reported = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        reported.contains("target listing failed"),
        "the target listing failure must remain diagnosable:\n{reported}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: delete pod"),
        "failed target verification must retain the staged copy:\n{calls}"
    );
    assert_eq!(
        fixture.migration_target().as_deref(),
        Some("rustfs\n"),
        "failed target verification must retain the planned target"
    );
    assert_eq!(
        fixture.migration_source().as_deref(),
        Some("100 bundle.tar\n"),
        "failed target verification must retain the source proof"
    );
}

#[test]
fn apply_import_failure_names_the_standalone_recovery_command() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_before = live_minio_statefulset();
    let live_after = live_rustfs_statefulset();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live_before.as_str()),
            ("CURIE_TEST_KUBECTL_STS_AFTER_UPGRADE", live_after.as_str()),
            ("CURIE_TEST_TARGET_LIST_FAIL", "1"),
        ],
    );

    let calls = fixture.calls();
    assert!(
        calls.contains("HELM_CALL: upgrade")
            && calls.contains("aws s3 ls s3://curie-bundles --recursive --endpoint-url http://parity-rustfs.parity.svc.cluster.local:9000"),
        "the fixture must fail during import verification after the upgrade:\n{calls}"
    );
    assert!(
        !output.status.success(),
        "apply import verification failure must exit nonzero; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let error: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|parse_error| {
        panic!(
            "apply import verification failure did not emit one JSON error object: {parse_error}; stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert!(error.get("fix").is_some(), "apply error payload: {error}");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("curie cluster migrate-store --phase import"),
        "an applied upgrade with failed import must name the standalone recovery command: {error}"
    );
    let upgrade = calls
        .find("HELM_CALL: upgrade")
        .expect("the fixture reached the applied upgrade");
    assert!(
        !calls[upgrade..].contains("KUBECTL_CALL: delete pod"),
        "failed apply import verification must retain staging after the upgrade:\n{calls}"
    );
}

#[test]
fn migrate_store_source_listing_shell_preserves_a_failed_aws_status() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_minio_statefulset();

    let _ = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_KUBECTL_STS", live.as_str()),
            ("CURIE_TEST_SOURCE_LIST_FAIL", "1"),
        ],
    );

    let script = fixture.last_exec_script();
    assert!(
        script.contains("aws s3 ls")
            && script.contains("parity-minio.parity.svc.cluster.local")
            && script.contains("/migration/source.list"),
        "the public CLI must expose the source evidence shell to the kubectl surface: {script}"
    );

    let shell_bin = fixture.temp.path().join("source-listing-shell-bin");
    fs::create_dir(&shell_bin).expect("create source listing shell bin directory");
    let aws_log = fixture.temp.path().join("source-aws-calls.log");
    write_exec(
        &shell_bin,
        "aws",
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_AWS_LOG"
if [ "$1" = configure ]; then
    exit 0
fi
if [ "$1" = s3 ] && [ "$2" = ls ]; then
    printf '%s\n' 'forced aws source listing failure' >&2
    exit 42
fi
printf 'unexpected aws invocation: %s\n' "$*" >&2
exit 64
"#,
    );
    write_exec(
        &shell_bin,
        "cat",
        "#!/bin/sh\nprintf '%s\\n' 'fixture-secret'\n",
    );
    write_exec(&shell_bin, "[", "#!/bin/sh\nexit 0\n");
    let source_raw = fixture.temp.path().join("source.raw");
    let source_tmp = fixture.temp.path().join("source.list.tmp");
    let target_tmp = fixture.temp.path().join("target.tmp");
    let bash_env = fixture.temp.path().join("source-listing-bash-env");
    fs::write(
        &bash_env,
        "enable -n [\ntrap 'source_raw=\"$CURIE_TEST_SOURCE_RAW\"; source_tmp=\"$CURIE_TEST_SOURCE_TMP\"; target_tmp=\"$CURIE_TEST_TARGET_TMP\"' DEBUG\n",
    )
    .expect("write source listing bash environment");
    let mut paths = vec![shell_bin];
    if let Some(current) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&current));
    }
    let path = std::env::join_paths(paths).expect("join source listing shell PATH");

    let shell_output = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .env("BASH_ENV", &bash_env)
        .env("PATH", path)
        .env("CURIE_TEST_AWS_LOG", &aws_log)
        .env("CURIE_TEST_SOURCE_RAW", &source_raw)
        .env("CURIE_TEST_SOURCE_TMP", &source_tmp)
        .env("CURIE_TEST_TARGET_TMP", &target_tmp)
        .output()
        .expect("execute the generated source listing shell");
    let aws_calls = fs::read_to_string(&aws_log).expect("read fake source aws calls");
    assert!(
        aws_calls.lines().any(|line| line.starts_with("s3 ls ")),
        "the generated source shell must reach the failing aws listing:\n{aws_calls}"
    );
    assert_eq!(
        shell_output.status.code(),
        Some(42),
        "the exact generated source shell must preserve aws exit 42; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&shell_output.stdout),
        String::from_utf8_lossy(&shell_output.stderr)
    );
}

#[test]
fn migrate_store_listing_shell_preserves_a_failed_aws_status() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    fixture.seed_migration_evidence("rustfs", STAGED_OBJECT);
    let live = live_rustfs_statefulset();

    let _ = fixture.run(
        &[
            "cluster",
            "migrate-store",
            "--phase",
            "import",
            "--namespace",
            "parity",
            "--release",
            "parity",
        ],
        &[
            ("CURIE_TEST_KUBECTL_STS", live.as_str()),
            ("CURIE_TEST_TARGET_LIST_FAIL", "1"),
        ],
    );

    let script = fixture.last_exec_script();
    assert!(
        script.contains("aws s3 ls") && script.contains("parity-rustfs.parity.svc.cluster.local"),
        "the public CLI must expose the target listing shell to the kubectl surface: {script}"
    );

    let shell_bin = fixture.temp.path().join("listing-shell-bin");
    fs::create_dir(&shell_bin).expect("create listing shell bin directory");
    let aws_log = fixture.temp.path().join("aws-calls.log");
    write_exec(
        &shell_bin,
        "aws",
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_AWS_LOG"
if [ "$1" = configure ]; then
    exit 0
fi
if [ "$1" = s3 ] && [ "$2" = ls ]; then
    printf '%s\n' 'forced aws listing failure' >&2
    exit 1
fi
printf 'unexpected aws invocation: %s\n' "$*" >&2
exit 64
"#,
    );
    write_exec(
        &shell_bin,
        "cat",
        "#!/bin/sh\nprintf '%s\\n' 'fixture-secret'\n",
    );
    write_exec(&shell_bin, "[", "#!/bin/sh\nexit 0\n");
    let bash_env = fixture.temp.path().join("listing-bash-env");
    fs::write(&bash_env, "enable -n [\n").expect("write bash environment");
    let mut paths = vec![shell_bin];
    if let Some(current) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&current));
    }
    let path = std::env::join_paths(paths).expect("join listing shell PATH");

    let shell_output = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .env("BASH_ENV", &bash_env)
        .env("PATH", path)
        .env("CURIE_TEST_AWS_LOG", &aws_log)
        .output()
        .expect("execute the generated listing shell");
    let aws_calls = fs::read_to_string(&aws_log).expect("read fake aws calls");
    assert!(
        aws_calls.lines().any(|line| line.starts_with("s3 ls ")),
        "the generated shell must reach the failing aws listing:\n{aws_calls}"
    );
    assert!(
        !shell_output.status.success(),
        "the exact generated listing shell must preserve aws exit one; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&shell_output.stdout),
        String::from_utf8_lossy(&shell_output.stderr)
    );
}

#[test]
fn migrate_store_refuses_a_mixed_removed_and_renamed_batch() {
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live_statefulsets = live_mixed_store_statefulsets();

    let output = fixture.apply(
        &["--migrate-store"],
        &[
            ("CURIE_TEST_HELM_MIXED_STATEFULSETS", "1"),
            ("CURIE_TEST_KUBECTL_STS", &live_statefulsets),
        ],
    );

    let calls = fixture.calls();
    let error = json_error(output, "apply --migrate-store");
    let message = error["error"].as_str().expect("apply error message string");
    assert!(
        message.contains("nameOverride"),
        "a renamed StatefulSet must direct the operator to nameOverride:\n{message}"
    );
    assert!(
        message.contains("parity-minio")
            && message.contains("parity-postgres")
            && message.contains("parity-curie-postgres"),
        "the refusal must include the removed store and renamed StatefulSet:\n{message}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: run "),
        "a mixed batch must stop before the migration export:\n{calls}"
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "a mixed batch must stop before the upgrade:\n{calls}"
    );
}

#[test]
fn migrate_store_does_not_read_a_failed_cluster_read_as_a_removal() {
    // #1351, the other face of the same defect. While the refusal was the only
    // error the guard could raise, `--migrate-store` read any Err as "a removal
    // was found" and started moving data. Once the guard could also fail
    // because the cluster was unreadable, that reading promoted "I could not
    // find out" to "definitely at risk, start staging" -- and the run then died
    // inside the export with "nothing to migrate", a message about a decision
    // the operator never made, blaming the wrong thing.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.apply(&["--migrate-store"], &[("CURIE_TEST_KUBECTL_FAIL", "1")]);

    let calls = fixture.calls();
    assert!(
        calls.contains("KUBECTL_CALL: get statefulset"),
        "the guard must have tried to read the live StatefulSets:\n{calls}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: run "),
        "an unreadable cluster must not start an export:\n{calls}"
    );
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "an unreadable cluster must stop the apply before the upgrade:\n{calls}"
    );
    // ADR-0021's exit-code contract: an automation loop branches on the code
    // rather than parsing prose, so an apiserver rolling restart reports exit 3
    // Transient (retry the same argv) and not exit 1 Failure, which reads as
    // "stop".
    assert_eq!(
        output.status.code(),
        Some(3),
        "an unreachable apiserver is transient; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let error = json_error(output, "apply --migrate-store");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("The connection to the server localhost:8080 was refused"),
        "the failure must carry kubectl's own words: {error}"
    );
    assert!(
        !message.contains("nothing to migrate"),
        "a failed read must not be reported as a migration decision: {error}"
    );
}

#[test]
fn allow_stateful_removal_alone_still_proceeds() {
    // AC3: the override short circuits the guard entirely, so the same live
    // minio store that stops a plain apply proceeds straight to the upgrade.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.apply(
        &["--allow-stateful-removal"],
        &[("CURIE_TEST_KUBECTL_STS", &live_minio_statefulset())],
    );

    let calls = fixture.calls();
    assert!(
        output.status.success(),
        "--allow-stateful-removal alone must still apply; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        calls.contains("HELM_CALL: upgrade"),
        "the upgrade must run:\n{calls}"
    );
    assert!(
        !calls.contains("KUBECTL_CALL: get statefulset"),
        "the override skips the guard, so nothing reads the live StatefulSets:\n{calls}"
    );
}

#[test]
fn a_failed_kubectl_read_fails_the_apply() {
    // AC4, the core anti regression for #1351: a kubectl read that FAILED was
    // returned as an empty list, which told the guard "fresh install, nothing
    // to lose" while the upgrade went on to prune the StatefulSet.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.apply(&[], &[("CURIE_TEST_KUBECTL_FAIL", "1")]);

    let calls = fixture.calls();
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "an unreadable cluster must stop the apply before the upgrade:\n{calls}"
    );
    assert!(
        !output.status.success(),
        "an unreadable cluster must not exit successfully; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let reported = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        reported.contains("The connection to the server localhost:8080 was refused"),
        "the failure must carry kubectl's own words, so a guard failure reads differently from a refusal:\n{reported}"
    );
}

#[test]
fn a_forbidden_kubectl_read_fails_the_apply_as_a_permanent_failure() {
    // Closes a gap the spec reviewer named for #1351: the only kubectl-failure
    // stderr both `a_failed_kubectl_read_fails_the_apply` and
    // `migrate_store_does_not_read_a_failed_cluster_read_as_a_removal` drive is
    // a CONNECTIVITY failure. Nothing pinned the sibling branch,
    // `is_connectivity_failure(stderr) == false`, which is what an RBAC
    // Forbidden denial takes. A regression shaped as "swallow the failure only
    // when it is NOT a connectivity failure" would leave both of those tests
    // green while restoring the exact vacuous-pass data-loss bug for a
    // Forbidden read: exit 0, no error, the guard reading "fresh install,
    // nothing to lose", and the upgrade pruning a live StatefulSet.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.apply(&[], &[("CURIE_TEST_KUBECTL_FORBIDDEN", "1")]);

    let calls = fixture.calls();
    assert!(
        !calls.contains("HELM_CALL: upgrade"),
        "an RBAC denied cluster read must stop the apply before the upgrade:\n{calls}"
    );
    // ADR-0021's exit-code contract: exit 3 Transient means "retry the same
    // argv", which is wrong advice for a permission denial that will not
    // clear on its own. This is the assertion that actually closes the gap:
    // it can only pass if the non-connectivity branch runs at all, which the
    // vacuous-pass shape of the bug would never reach.
    assert_eq!(
        output.status.code(),
        Some(1),
        "a Forbidden cluster read is a permanent failure, not transient or a silent success; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let error = json_error(output, "apply");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("cannot list resource \"statefulsets\""),
        "the failure must carry kubectl's own Forbidden wording, so an operator can tell an RBAC problem from an unreachable cluster: {error}"
    );
}

#[test]
fn a_namespace_with_no_statefulsets_still_passes_the_guard() {
    // AC5, the trap guard. An empty items array with exit 0 is what a real
    // namespaced LIST returns for a fresh install AND for a namespace that does
    // not exist, so it is a genuine "nothing to lose" and must still apply.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );

    let output = fixture.apply(&[], &[]);

    let calls = fixture.calls();
    assert!(
        output.status.success(),
        "an empty namespace must still apply; stdout:\n{}\nstderr:\n{}\ncalls:\n{calls}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        calls.contains("HELM_CALL: upgrade"),
        "the upgrade must run:\n{calls}"
    );
}

/// The `stateful_removals` array `diff --json` must ALWAYS carry, even empty.
///
/// `expect` rather than `unwrap_or(&[])` on purpose: an omitted key is the
/// pre-#1352 shape, and defaulting it to an empty slice would let every
/// assertion below pass against a payload that never grew the field.
fn stateful_removals(diff: &Value) -> &Vec<Value> {
    diff["stateful_removals"]
        .as_array()
        .unwrap_or_else(|| panic!("diff must always emit a stateful_removals array: {diff}"))
}

/// The one removal naming `name`, or a panic naming what was reported instead.
fn stateful_removal<'a>(diff: &'a Value, name: &str) -> &'a Value {
    stateful_removals(diff)
        .iter()
        .find(|removal| removal["name"] == name)
        .unwrap_or_else(|| panic!("no stateful removal reported for {name}: {diff}"))
}

/// How many entries `changes` counts on its own, derived from the payload the
/// run actually emitted rather than a hardcoded total, so the arithmetic
/// relationship is what is pinned and an unrelated chart-default gaining or
/// losing a value cannot make this test lie in either direction.
fn change_kind_entries(diff: &Value) -> usize {
    diff["entries"]
        .as_array()
        .expect("diff entries array")
        .iter()
        .filter(|entry| {
            matches!(
                entry["kind"].as_str().expect("diff entry kind"),
                "add" | "change" | "reset to chart default" | "unknown"
            )
        })
        .count()
}

#[test]
fn diff_reports_the_stateful_removal_apply_refuses_on() {
    // AC1, the issue's reproduction and the core of #1352. `diff()` called
    // `complete_installation_plan` + `fetch_release_chart` and nothing else, so
    // it never read a StatefulSet: a file that would DELETE a live stateful
    // component rendered as an ordinary values change, exit 0, while `apply` on
    // the SAME file and the SAME cluster exited 1 refusing. Two surfaces, one
    // input, opposite answers.
    //
    // Red on revert: without the fix `stateful_removals` is absent entirely, so
    // the `expect` in `stateful_removals` panics. Note that a bare
    // `changes > 0` assertion would pass TODAY -- the values delta already
    // counts -- which is why the count is pinned as arithmetic over the
    // payload's own entries plus the removals, not as a threshold.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_postgres_statefulset();
    let env = [("CURIE_TEST_KUBECTL_STS", live.as_str())];

    let diff = json_output(fixture.diff(&env), "diff");

    let removals = stateful_removals(&diff);
    assert_eq!(
        removals.len(),
        1,
        "the live postgres StatefulSet is the only removal in this render: {diff}"
    );
    let removal = stateful_removal(&diff, "parity-postgres");
    assert_eq!(
        removal["component"], "postgres",
        "the removal must carry the component identity the guard matched on: {removal}"
    );
    assert_eq!(
        removal["cause"], "component_gone",
        "the target chart does not render postgres at all: {removal}"
    );
    assert!(
        removal.get("renamed_to").is_none(),
        "a component that is gone has no rename target: {removal}"
    );
    assert_eq!(
        diff["changes"].as_u64().expect("changes is an integer") as usize,
        change_kind_entries(&diff) + removals.len(),
        "a removal that is not counted is a removal an agent consumer gating on `changes` never sees: {diff}"
    );

    // The human render is an independent projection of the same output object;
    // a removal reported only in the JSON leaves the operator reading `diff`
    // in a terminal with the exact pre-fix experience.
    let human = fixture.diff_human(&env);
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&human.stdout),
        String::from_utf8_lossy(&human.stderr)
    );
    assert!(
        rendered.contains("parity-postgres"),
        "the render must name the live resource an operator recognises:\n{rendered}"
    );
    assert!(
        rendered.contains("DELETED"),
        "the render must say the component would be DELETED, not merely changed:\n{rendered}"
    );
    assert!(
        rendered.contains("curie apply") && rendered.contains("REFUSE"),
        "the render must say `curie apply` will refuse, so the operator is not surprised by exit 1:\n{rendered}"
    );

    // The other half of the parity claim: the same file and the same cluster,
    // through `apply`. A second fixture because the first one's call log has
    // already recorded the diff run.
    let apply_fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let apply = apply_fixture.apply(&[], &env);
    assert_eq!(
        apply.status.code(),
        Some(1),
        "apply must still refuse the same input; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let error = json_error(apply, "apply");
    let message = error["error"].as_str().expect("apply error message string");
    assert!(
        message.contains("refusing to apply") && message.contains("parity-postgres"),
        "apply's refusal must name the same resource diff reported: {error}"
    );
    assert!(
        !apply_fixture.calls().contains("HELM_CALL: upgrade"),
        "the refusal must stop before the upgrade:\n{}",
        apply_fixture.calls()
    );
}

#[test]
fn diff_reports_a_renamed_stateful_component_with_its_rename_target() {
    // AC2, the #1323 shape. Here the chart DOES render postgres -- under
    // `parity-curie-postgres` rather than the live `parity-postgres`, which is
    // what a curie.yaml that does not reproduce the release's `nameOverride`
    // produces. Helm deletes the old object and creates the new one empty
    // beside the orphaned volumes, so it is exactly as destructive as a drop,
    // and the two causes have OPPOSITE remedies (`--migrate-store` vs.
    // declaring `nameOverride`).
    //
    // Red on revert twice over: `stateful_removals` does not exist pre-fix, and
    // a fix that collapsed the removals to bare names would lose the cause the
    // remedy is chosen from. The mixed render stub is load-bearing -- against
    // the default rustfs-only render a live postgres is `ComponentGone`, so an
    // AC2 written without it is a mislabelled AC1 and a rename regression still
    // ships.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_mixed_store_statefulsets();

    let diff = json_output(
        fixture.diff(&[
            ("CURIE_TEST_HELM_MIXED_STATEFULSETS", "1"),
            ("CURIE_TEST_KUBECTL_STS", live.as_str()),
        ]),
        "diff",
    );

    let renamed = stateful_removal(&diff, "parity-postgres");
    assert_eq!(
        renamed["component"], "postgres",
        "the rename is same-component, different-name: {renamed}"
    );
    assert_eq!(
        renamed["cause"], "renamed",
        "a component the render keeps under another name is a rename, not a drop: {renamed}"
    );
    assert_eq!(
        renamed["renamed_to"], "parity-curie-postgres",
        "without the rename target the operator cannot tell which name to declare: {renamed}"
    );

    // The same payload must still distinguish the other cause, or the two
    // remedies collapse into one.
    let gone = stateful_removal(&diff, "parity-minio");
    assert_eq!(gone["cause"], "component_gone", "{gone}");
    assert!(
        gone.get("renamed_to").is_none(),
        "`renamed_to` belongs only to the rename cause: {gone}"
    );

    assert_eq!(
        diff["changes"].as_u64().expect("changes is an integer") as usize,
        change_kind_entries(&diff) + stateful_removals(&diff).len(),
        "every removal must be counted, renames included: {diff}"
    );
}

#[test]
fn diff_reports_no_stateful_removal_when_live_and_rendered_agree() {
    // AC3, the no-false-alarm control. A guard that cries wolf teaches
    // operators to pass the override flag by reflex, which is worse than no
    // guard at all for the one case that is real. The live release runs
    // `rustfs` under exactly the name the default render produces, so there is
    // nothing to report.
    //
    // Red on revert through the `expect` in `stateful_removals`: the key must
    // be PRESENT as `[]`. An omitted optional key would re-hide the field from
    // every consumer that looks for it, and would make this test's emptiness
    // assertion vacuously true.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_rustfs_statefulset();

    let diff = json_output(
        fixture.diff(&[("CURIE_TEST_KUBECTL_STS", live.as_str())]),
        "diff",
    );

    assert!(
        stateful_removals(&diff).is_empty(),
        "a release whose live components match the render loses nothing: {diff}"
    );
    assert_eq!(
        diff["changes"].as_u64().expect("changes is an integer") as usize,
        change_kind_entries(&diff),
        "with no removals `changes` must stay exactly the entry count it always was: {diff}"
    );
}

#[test]
fn an_unreadable_cluster_fails_the_diff_rather_than_reporting_no_removals() {
    // AC5, the strongest red-on-revert available: pre-fix `diff` never calls
    // kubectl at all, so both of these runs currently produce a SUCCESSFUL
    // diff reporting no removals -- the #1351 vacuous-pass shape, pointed at
    // the read-only surface. "I could not find out" must never render as
    // "nothing would be deleted", because that is the answer an operator
    // approves an apply on.
    //
    // The values response is an EXISTING release: an unreadable cluster is
    // interesting precisely when there is a live release with data to lose.
    let live_values = || {
        HelmValuesResponse::Object(json!({
            "api": {"githubToken": "ghp-live-token"}
        }))
    };

    // ADR-0021's exit-code contract: an unreachable apiserver is exit 3
    // Transient (retry the same argv), because an apiserver rolling restart
    // clears on its own.
    let fixture = HelmFixture::new(installation_for_the_stateful_guard(), live_values());
    let output = fixture.diff(&[("CURIE_TEST_KUBECTL_FAIL", "1")]);
    assert_eq!(
        output.status.code(),
        Some(3),
        "an unreachable apiserver is transient, not a clean diff; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let error = json_error(output, "diff");
    let message = error["error"].as_str().expect("error message string");
    assert!(
        message.contains("parity"),
        "the failure must name the namespace it could not read: {error}"
    );
    assert!(
        message.contains("The connection to the server localhost:8080 was refused"),
        "the failure must carry kubectl's own words: {error}"
    );
    // R4: the guard's framing is now SHARED by apply and diff, so wording that
    // names one verb reports a check the other caller was not performing. A
    // `curie diff` that says "this apply" is the seam leaking.
    assert!(
        !message.to_ascii_lowercase().contains("apply"),
        "the shared guard's context must stay verb neutral: {error}"
    );

    // Exit 1 Failure, not 3: a permission denial will not clear on its own, so
    // telling an automation loop to retry the same argv is wrong advice.
    let forbidden_fixture = HelmFixture::new(installation_for_the_stateful_guard(), live_values());
    let forbidden = forbidden_fixture.diff(&[("CURIE_TEST_KUBECTL_FORBIDDEN", "1")]);
    assert_eq!(
        forbidden.status.code(),
        Some(1),
        "a Forbidden cluster read is permanent, not transient or a silent success; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&forbidden.stdout),
        String::from_utf8_lossy(&forbidden.stderr)
    );
    let forbidden_error = json_error(forbidden, "diff");
    let forbidden_message = forbidden_error["error"]
        .as_str()
        .expect("error message string");
    assert!(
        forbidden_message.contains("cannot list resource \"statefulsets\""),
        "the failure must carry kubectl's own Forbidden wording, so an RBAC problem reads differently from an unreachable cluster: {forbidden_error}"
    );
    assert!(
        !forbidden_message.to_ascii_lowercase().contains("apply"),
        "the shared guard's context must stay verb neutral: {forbidden_error}"
    );
}

/// The `migration` field `diff --json` must ALWAYS carry, as an object or as
/// `null`.
///
/// A missing key panics rather than reading as `null`: "this CLI does not
/// report store renames" and "this upgrade renames no store" are the two
/// answers the field exists to separate, and letting an absent key stand in for
/// the second would make every assertion below pass against a payload that
/// never grew the field.
fn migration(diff: &Value) -> &Value {
    diff.get("migration")
        .unwrap_or_else(|| panic!("diff must always emit a migration key: {diff}"))
}

#[test]
fn diff_reports_the_store_rename_migrate_store_carries_the_data_across() {
    // The discriminator the removals list alone cannot supply, and the reason
    // the `component_gone` remedy is not universal. Here the live release runs
    // `minio` while the target render produces `rustfs`: a store SWAP, which is
    // exactly the case `curie apply --migrate-store` carries the objects
    // across. It reports the same `component_gone` cause as the values-gated
    // drop below, and only `migration` tells the two apart.
    //
    // Red on revert through the `expect` in `migration`: without the field the
    // payload carries the removal and nothing else, so a consumer -- human or
    // agent -- reading `component_gone` has to guess whether the flag the
    // remedy names can help, and half of them guess wrong (#1352).
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_minio_statefulset();

    let diff = json_output(
        fixture.diff(&[("CURIE_TEST_KUBECTL_STS", live.as_str())]),
        "diff",
    );

    // COMPONENT names on both sides, never resource names: the live object is
    // `parity-minio`, and a `migration` reporting that could not be matched
    // against `minio` by any consumer, since the fullname moves under a
    // `nameOverride` while the component does not.
    assert_eq!(
        migration(&diff),
        &json!({"from": "minio", "to": "rustfs"}),
        "the store swap must be reported as the component pair `--migrate-store` moves between: {diff}"
    );

    let removal = stateful_removal(&diff, "parity-minio");
    assert_eq!(
        removal["component"], "minio",
        "the removal must carry the component identity the migration is keyed on: {removal}"
    );
    assert_eq!(
        removal["cause"], "component_gone",
        "the target chart renders rustfs, so the live minio component is gone entirely: {removal}"
    );
    assert!(
        removal.get("renamed_to").is_none(),
        "a component that is gone has no rename target: {removal}"
    );
}

#[test]
fn diff_reports_no_migration_for_a_values_gated_stateful_drop() {
    // The issue's own reproduction, and the half `--migrate-store` cannot
    // reach. The live release runs `rustfs` -- the SAME store the target render
    // produces, so no store is renamed -- beside a bundled `postgres` the
    // render drops, which is what a curie.yaml that stops deploying postgres
    // (the chart's BYO gate) does to a running database.
    //
    // `migration: null` beside a NON EMPTY `stateful_removals` is the payload
    // that says there is no automatic carry: `curie apply --migrate-store`
    // copies OBJECT STORE buckets only, so it has nothing to move here and
    // apply keeps refusing until the file is fixed. Pre-fix a consumer could
    // not tell this from the store swap above at all -- both reported the same
    // `component_gone`, and only one of them has a flag that helps (#1352).
    //
    // Red on revert through the `expect` in `migration`, and red on a fix that
    // reported a migration unconditionally: `null` here is a fact about this
    // upgrade, not a missing value.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_rustfs_and_postgres_statefulsets();

    let diff = json_output(
        fixture.diff(&[("CURIE_TEST_KUBECTL_STS", live.as_str())]),
        "diff",
    );

    assert_eq!(
        migration(&diff),
        &Value::Null,
        "both sides run rustfs, so this upgrade renames no store: {diff}"
    );
    let removals = stateful_removals(&diff);
    assert_eq!(
        removals.len(),
        1,
        "the live postgres is dropped while the store survives in place: {diff}"
    );
    let removal = stateful_removal(&diff, "parity-postgres");
    assert_eq!(
        removal["component"], "postgres",
        "the dropped component is the database, not the store: {removal}"
    );
    assert_eq!(
        removal["cause"], "component_gone",
        "the target chart does not render postgres at all: {removal}"
    );
    // The control that keeps `migration: null` meaningful: a run that reported
    // the store as removed too would make the null merely wrong rather than
    // informative.
    assert!(
        !removals
            .iter()
            .any(|reported| reported["name"] == "parity-rustfs"),
        "the store the render also produces is not at risk: {diff}"
    );
}

#[test]
fn diff_does_not_offer_a_flag_apply_would_refuse_for_an_uncarriable_removal() {
    // The #1352 / #1501 interaction. #1352 made `diff` reuse
    // `stateful_removal_remedies` -- the helper apply's refusal composes -- so
    // the two surfaces cannot name different fixes. #1501 then established that
    // `--migrate-store` carries the OBJECT STORE and nothing else, and made
    // `apply --migrate-store` REFUSE any batch holding anything else. Between
    // them, a remedy that offered `--migrate-store` for EVERY `component_gone`
    // told the operator of a dropped `postgres` to re-run with a flag apply
    // then refuses: a brand new diff-vs-apply disagreement, which is the exact
    // defect class #1352 exists to close.
    //
    // The fixture is the issue's own reproduction: live `rustfs` (the store the
    // target render also produces, so nothing is being migrated between) beside
    // a bundled `postgres` the render drops. Red on revert on the first
    // assertion -- the pre-fix remedy names `--migrate-store` unconditionally.
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_rustfs_and_postgres_statefulsets();
    let env = [("CURIE_TEST_KUBECTL_STS", live.as_str())];

    let human = fixture.diff_human(&env);
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&human.stdout),
        String::from_utf8_lossy(&human.stderr)
    );

    assert!(
        !rendered.contains("re-run with --migrate-store"),
        "diff must not send the operator to the flag apply refuses for this batch:\n{rendered}"
    );
    assert!(
        !rendered.contains("apply will carry the data across itself"),
        "there is nothing --migrate-store can carry here, so it must not be promised:\n{rendered}"
    );
    // The remedy that replaces it has to be actionable, not merely absent: the
    // component's identity, and where the drop actually comes from.
    assert!(
        rendered.contains("parity-postgres") && rendered.contains("postgres"),
        "the render must name the component whose data is at risk:\n{rendered}"
    );
    assert!(
        rendered.contains("carries the OBJECT STORE and nothing else"),
        "the render must say why --migrate-store is not the fix here:\n{rendered}"
    );
    assert!(
        rendered.contains("`<component>.deploy=false`"),
        "the render must point at the values that drop the component:\n{rendered}"
    );

    // The other half of the parity claim: the SAME file and the SAME cluster,
    // through the flag diff no longer recommends. A second fixture because the
    // first one's call log has already recorded the diff run.
    let apply_fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let apply = apply_fixture.apply(&["--migrate-store"], &env);
    assert!(
        !apply.status.success(),
        "apply --migrate-store must refuse the batch diff declined to recommend it for; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let error = json_error(apply, "apply");
    let message = error["error"].as_str().expect("apply error message string");
    assert!(
        message.contains("parity-postgres"),
        "apply's refusal must name the same component diff reported: {error}"
    );
    assert!(
        message.contains("cannot carry"),
        "apply's refusal must be the uncarriable one, not the generic guard: {error}"
    );
    assert!(
        !apply_fixture.calls().contains("HELM_CALL: upgrade"),
        "the refusal must precede the irreversible upgrade:\n{}",
        apply_fixture.calls()
    );
}

#[test]
fn the_diff_summary_does_not_claim_a_refused_file_would_be_applied() {
    // `N change(s) would be applied` on a file `curie apply` REFUSES is the
    // original defect's wording, and folding the removals into `changes()`
    // without touching this line merely made it a bigger number telling the
    // same lie. The operator reads the last summary line and approves; the
    // apply then exits 1 having applied nothing.
    //
    // Red on revert on the `would be applied` half specifically: the REFUSE
    // note below the summary already existed in the first pass, so a run that
    // kept the old summary line would still satisfy an assertion that only
    // looked for `REFUSE` anywhere in the output. The count must SURVIVE --
    // automation and humans both read it -- so this pins the outcome, not the
    // number (#1352).
    let fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_postgres_statefulset();

    let human = fixture.diff_human(&[("CURIE_TEST_KUBECTL_STS", live.as_str())]);
    let rendered = format!(
        "{}{}",
        String::from_utf8_lossy(&human.stdout),
        String::from_utf8_lossy(&human.stderr)
    );

    assert!(
        !rendered.contains("would be applied"),
        "nothing about a file apply refuses would be applied:\n{rendered}"
    );
    let summary = rendered
        .lines()
        .find(|line| line.contains("change(s)"))
        .unwrap_or_else(|| panic!("the render must still summarise the change count:\n{rendered}"));
    // The expected count comes from the payload the SAME input produces rather
    // than a literal, so an unrelated chart default gaining or losing a value
    // cannot make this test lie in either direction -- and so the human and
    // JSON projections are pinned to the same number, which is the parity claim
    // #1352 rests on. A second fixture because the first one's call log has
    // already recorded the human run.
    let json_fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let diff = json_output(
        json_fixture.diff(&[("CURIE_TEST_KUBECTL_STS", live.as_str())]),
        "diff",
    );
    let changes = diff["changes"].as_u64().expect("changes is an integer");
    assert!(
        summary.starts_with(&format!("{changes} change(s)")),
        "the count must still be reported, and must be the count the payload carries: {summary}"
    );
    assert!(
        summary.contains(&format!(
            "including {} stateful removal(s)",
            stateful_removals(&diff).len()
        )),
        "the summary must say how much of that count is destruction: {summary}"
    );
    assert!(
        summary.contains("REFUSE"),
        "the summary line itself must state the outcome, not leave it to a note further down: {summary}"
    );
}

#[test]
fn diff_answers_the_same_against_an_explicitly_passed_chart() {
    // `--chart` exists on this verb because diff RENDERS the chart to decide
    // what survives, and `resolve_chart`'s own remedy text on a dev build with
    // no `charts/curie` in cwd literally says "pass --chart" (#1352). The
    // fixture runs from the repository root, so the DEFAULT path already
    // resolves `charts/curie` implicitly -- which is exactly why the flag needs
    // its own coverage: every other diff test would stay green with the
    // argument unwired.
    //
    // Red on revert two ways: a `--chart` clap surface that does not exist
    // fails argument parsing, and one that parses but is dropped before the
    // render would answer from the implicitly resolved chart, which this test
    // cannot distinguish -- so the assertion is that the OVERRIDDEN path
    // produces the same removal, i.e. the flag reaches a chart that renders.
    let default_fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let live = live_postgres_statefulset();
    let env = [("CURIE_TEST_KUBECTL_STS", live.as_str())];
    let expected = json_output(default_fixture.diff(&env), "diff");

    let chart = repo_root().join("charts/curie");
    let overridden_fixture = HelmFixture::new(
        installation_for_the_stateful_guard(),
        HelmValuesResponse::Absent,
    );
    let overridden = json_output(
        overridden_fixture.diff_with(
            &["--chart", chart.to_str().expect("UTF 8 chart path")],
            &env,
        ),
        "diff",
    );

    assert_eq!(
        stateful_removals(&overridden),
        stateful_removals(&expected),
        "an explicit chart must answer the stateful question exactly as the resolved default does: {overridden}"
    );
    assert_eq!(
        migration(&overridden),
        migration(&expected),
        "the store rename is read from the same render, so it cannot differ either: {overridden}"
    );
    // The flag has to reach the RESOLUTION, not merely parse. `helm show chart`
    // fires on the overridden path ONLY -- the default reports
    // `artifacts::version()` and never asks -- so this is the one call that
    // distinguishes "the argument was honoured" from "the argument was accepted
    // and dropped", which the identical answers above cannot.
    let calls = overridden_fixture.calls();
    assert!(
        calls.contains(&format!(
            "HELM_CALL: show chart {}",
            chart.to_str().expect("UTF 8 chart path")
        )),
        "the version REPORTED must be read from the chart the flag named:\n{calls}"
    );
    assert!(
        !default_fixture.calls().contains("HELM_CALL: show chart"),
        "the default path must stay exactly as it was, with no extra helm call:\n{}",
        default_fixture.calls()
    );
    // And it has to reach the RENDER too: the chart the probe renders is the
    // chart apply would use, and a diff rendering one chart while reporting
    // another is the two-sources-of-truth defect in a new place.
    assert!(
        calls.contains(&format!(
            "HELM_CALL: template parity {}",
            chart.to_str().expect("UTF 8 chart path")
        )),
        "the overridden chart must actually be the one rendered:\n{calls}"
    );
}
