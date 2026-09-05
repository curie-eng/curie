//! Probe fan-out for `curie doctor` and `curie cluster status`.
//!
//! Both verbs are read-only reports built from a handful of independent
//! kubectl/helm/docker reads. Nothing in the *report* says whether those reads
//! were awaited one after another or joined, so a purely sequential
//! implementation and a fully concurrent one produce byte-identical output and
//! differ only in wall time -- which is exactly the property a timing assertion
//! cannot pin on a shared CI box. These tests measure the fan-out itself
//! instead of the clock.
//!
//! # How the stage count works
//!
//! Every stub (`docker`, `kubectl`, `helm`) records `start_ns`, sleeps
//! `CURIE_TEST_PROBE_LATENCY_MS` (default 250), records `end_ns`, and appends
//! one `<start_ns> <end_ns> <tool> <argv>` line to `$CURIE_TEST_PROBE_LOG`
//! under `flock(LOCK_EX)`. [`probe_stages`] sorts those intervals by start and
//! greedily chains them: a call whose start is at or after the running maximum
//! end of every call so far opens a new *stage*. Calls that overlap in time
//! land in the same stage.
//!
//! So the stage count is the length of the longest chain of strictly
//! non-overlapping probes -- the number of round trips the operator actually
//! waits through, independent of how fast the box is. A sequential
//! implementation yields one stage per call; joining N independent probes
//! collapses them into one. The sleep exists only to make concurrency
//! observable at all: without it every call is a point in time and everything
//! looks sequential.
//!
//! Duplicates are a second, orthogonal signal: the same `tool + argv` line
//! appearing twice is a probe whose answer the process already had. `doctor`
//! used to resolve the release's rendered fullname once in `resolve_api` and
//! again in `api_nodeport`, which showed up here as a duplicated
//! label-selector read; `ops::release_fullname` memoizes per process, so that
//! line must now appear exactly once.
//!
//! # Where doctor's floor is
//!
//! Five stages, and each boundary is a real data dependency rather than a
//! missed fan-out:
//!
//! 1. the fullname discovery, the kubeconfig host read and the chart-Secret
//!    name listing -- nothing any of them needs is known only after another;
//! 2. the `curie-ui` Service read (needs the fullname) alongside `helm list -n
//!    curie --all` (issued only because the Secret listing came back empty);
//! 3. the four `gather` probes -- docker, the kube context, the helm version
//!    and `helm list -n curie -o json`;
//! 4. the two `helm get values` reads, computed and operator-supplied;
//! 5. the api Service NodePort read, which needs the fullname (a cache hit, so
//!    no second discovery) and only happens because the values carry no
//!    ingress.
//!
//! Step 2's helm hop is the second rung of `ops::discover_api_key`'s ladder:
//! the Secret name listing answers empty, so the key is looked for in Helm's
//! record instead, which reports the release deployed and ends the ladder with
//! "its chart Secret API key could not be read". That is what makes the API
//! leg observable here without an HTTP round trip. Baseline was 13 calls in 12
//! stages, including the duplicated label-selector read; the joined
//! implementation is 12 calls in 5.
//!
//! # Why the scenario makes no HTTP call
//!
//! The stubs answer a default `curie`/`curie` install, and the chart-Secret
//! name listing deliberately comes back EMPTY. `ops::discover_api_key` then
//! falls through to its helm ladder, finds no deployed release to read a key
//! from, and errors; `doctor::resolve_api` discards that (`.ok()?`) and hands
//! `gather` `api: None`. So `ApiClient` is never constructed and no `/agents`
//! request is ever issued. That is deliberate: an HTTP round trip to a mock
//! server would add a probe this harness cannot see (it never reaches a stub)
//! and would make the stage count depend on socket timing rather than on the
//! CLI's own await structure. The API leg is covered by `doctor_api.rs`.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Instant;

/// The one stub body, installed under three names. It dispatches on
/// `basename(argv[0])` plus the EXACT argv the CLI builds, so a changed probe
/// shows up as a loud `exit 64` rather than as a silently different answer.
/// An unmatched argv also appends a log line carrying the literal token
/// `UNEXPECTED` between the tool name and the argv; [`probe_stages`] fails
/// the fan-out tests on any such line, so an unrecognized probe cannot pass
/// silently just because the caller tolerates the stub's failure exit.
const STUB: &str = r###"#!/usr/bin/env python3
import fcntl
import os
import shlex
import sys
import time

args = sys.argv[1:]
tool = os.path.basename(sys.argv[0])

start_ns = time.time_ns()
time.sleep(int(os.environ.get("CURIE_TEST_PROBE_LATENCY_MS", "250")) / 1000.0)
end_ns = time.time_ns()

with open(os.environ["CURIE_TEST_PROBE_LOG"], "a", encoding="utf-8") as log:
    fcntl.flock(log, fcntl.LOCK_EX)
    log.write("%d %d %s %s\n" % (start_ns, end_ns, tool, shlex.join(args)))
    log.flush()

# `kubectl ... -o jsonpath=...` name listing, as ops.rs builds it (the `\n` is
# two characters in the argv, not a newline).
NAMES = r'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}'
API_SELECTOR = "app.kubernetes.io/instance=curie,app.kubernetes.io/component=api"
WORKER_SELECTOR = "app.kubernetes.io/instance=curie,app.kubernetes.io/component=worker"

RULES = {
    ("docker", ("info",)): "",

    # -- doctor's context probe ------------------------------------------
    ("kubectl", ("config", "current-context")): "doctor-stub-context\n",
    # -- resolve_node_host ------------------------------------------------
    ("kubectl", ("config", "view", "--minify", "-o",
                 "jsonpath={.clusters[0].cluster.server}")): "https://127.0.0.1:6443",
    # -- release_fullname discovery (api Service wins; the worker Deployment
    #    probe is defensive and must not fire) ------------------------------
    ("kubectl", ("-n", "curie", "get", "svc", "-l", API_SELECTOR, "-o", NAMES)): "curie-api\n",
    ("kubectl", ("-n", "curie", "get", "deployment", "-l", WORKER_SELECTOR, "-o", NAMES)):
        "curie-worker\n",
    # -- service reads -----------------------------------------------------
    ("kubectl", ("get", "svc", "curie-ui", "-n", "curie", "-o", "json")):
        '{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":30080}]}}\n',
    ("kubectl", ("get", "svc", "curie-langfuse-web", "-n", "curie", "-o", "json")):
        '{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}\n',
    ("kubectl", ("get", "svc", "curie-api", "-n", "curie", "-o",
                 "jsonpath={.spec.ports[?(@.nodePort)].nodePort}")): "30800",
    # -- chart Secret name listing: EMPTY on purpose (see the module doc) ---
    ("kubectl", ("-n", "curie", "get", "secret", "-l",
                 "app.kubernetes.io/instance=curie", "-o", NAMES)): "",
    # -- cluster status pod health ----------------------------------------
    ("kubectl", ("get", "pods", "-n", "curie", "-o", "json")): '{"items":[]}\n',
    # -- resolve_node_host fallback (defensive; must not fire) -------------
    ("kubectl", ("get", "nodes", "-o", "json")): '{"items":[]}\n',

    ("helm", ("version", "--short")): "v3.14.0+gstub\n",
    # Ordered by exact argv, so `--all` and `-o json` variants cannot collide.
    ("helm", ("list", "-n", "curie", "--all", "-o", "json")):
        '[{"name":"curie","namespace":"curie","status":"deployed","chart":"curie-0.8.2"}]\n',
    ("helm", ("list", "-n", "curie", "-o", "json")):
        '[{"name":"curie","chart":"curie-0.8.2"}]\n',
    # convergence::observe reads this one; no `version` field means it bails
    # after exactly one call with "Helm release has no verifiable revision".
    ("helm", ("status", "curie", "-n", "curie", "-o", "json")): "{}\n",
    ("helm", ("status", "curie", "-n", "curie")):
        "NAME: curie\nSTATUS: deployed\nREVISION: 3\n",
    ("helm", ("get", "values", "curie", "-n", "curie", "--all", "-o", "json")): "{}\n",
    ("helm", ("get", "values", "curie", "-n", "curie", "-o", "json")): "{}\n",
}

key = (tool, tuple(args))
if key in RULES:
    sys.stdout.write(RULES[key])
    sys.exit(0)

sys.stderr.write("unexpected %s invocation: %s\n" % (tool, shlex.join(args)))
with open(os.environ["CURIE_TEST_PROBE_LOG"], "a", encoding="utf-8") as log:
    fcntl.flock(log, fcntl.LOCK_EX)
    log.write("%d %d %s UNEXPECTED %s\n" % (start_ns, end_ns, tool, shlex.join(args)))
    log.flush()
sys.exit(64)
"###;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/probe_fanout")
}

/// Write `body` executable at `path`, atomically: with
/// `CURIE_TEST_PROBE_FANOUT_KEEP_STUBS` several tests install into the SAME
/// directory concurrently, and a partially written or busy script would fail
/// as a mysterious `exit 26`/short read rather than as a test failure.
fn write_executable(path: &Path, body: &str) {
    let staging = path.with_extension(format!("tmp{}", std::process::id()));
    fs::write(&staging, body).expect("write stub executable");
    let mut permissions = fs::metadata(&staging)
        .expect("read stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&staging, permissions).expect("make stub executable");
    fs::rename(&staging, path).expect("install stub executable");
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

/// One isolated run environment: stubbed tools on PATH, a private HOME (so a
/// developer box's saved `curie secrets` cannot reach `secrets::is_saved`,
/// which reads `$CURIE_CONFIG_DIR` or `$HOME/.config/curie/secrets.json`), an
/// empty cwd (no `curie.yaml`, no `.claude-plugin/plugin.json`), and a fresh
/// probe log.
struct Fixture {
    _temp: tempfile::TempDir,
    tools: PathBuf,
    home: PathBuf,
    cwd: PathBuf,
    log: PathBuf,
}

fn fixture() -> Fixture {
    let temp = tempfile::tempdir().expect("tempdir");
    let tools = match std::env::var_os("CURIE_TEST_PROBE_FANOUT_KEEP_STUBS") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => temp.path().join("tools"),
    };
    let home = temp.path().join("home");
    let cwd = temp.path().join("cwd");
    for dir in [&tools, &home, &cwd] {
        fs::create_dir_all(dir).expect("create fixture dir");
    }
    for tool in ["docker", "kubectl", "helm"] {
        write_executable(tools.join(tool).as_path(), STUB);
    }
    let log = temp.path().join("probes.log");
    fs::write(&log, "").expect("create probe log");
    Fixture {
        _temp: temp,
        tools,
        home,
        cwd,
        log,
    }
}

impl Fixture {
    /// Run the real binary against the stubs. `env_clear` is deliberate: the
    /// model-credential sweep in `doctor::gather` reads
    /// `commands::MODEL_CREDENTIAL_ENV_NAMES` and `CURIE_MODEL` straight from
    /// the process environment, so a developer's exported
    /// `ANTHROPIC_API_KEY` would change the golden output. python3 needs
    /// nothing from the environment beyond finding itself on PATH, which the
    /// `/usr/bin` entry provides.
    fn run(&self, args: &[&str]) -> (Output, u128) {
        let mut cmd = Command::new(bin());
        cmd.current_dir(&self.cwd)
            .args(args)
            .env_clear()
            .env("PATH", stub_path(&self.tools))
            .env("HOME", &self.home)
            .env("CURIE_CONFIG_DIR", self.home.join(".config/curie"))
            .env("LC_ALL", "C")
            .env("CURIE_TEST_PROBE_LOG", &self.log);
        if let Some(latency) = std::env::var_os("CURIE_TEST_PROBE_LATENCY_MS") {
            cmd.env("CURIE_TEST_PROBE_LATENCY_MS", latency);
        }
        let started = Instant::now();
        let output = cmd.output().expect("run curie");
        (output, started.elapsed().as_millis())
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

/// `(calls, stages, duplicate command lines, unexpected-argv log lines)` for
/// one probe log.
///
/// See the module doc for what a stage is. Duplicates and unexpected lines
/// are reported sorted so a panic message is stable. A line carrying the
/// stub's `UNEXPECTED` token (see [`STUB`]) still contributes an interval to
/// the stage count -- it happened and took real wall time -- but is
/// collected separately so [`assert_fanout`] can fail loudly on it.
fn probe_stages(log: &str) -> (usize, usize, Vec<String>, Vec<String>) {
    let mut intervals: Vec<(u128, u128)> = Vec::new();
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut unexpected: Vec<String> = Vec::new();
    for line in log.lines().filter(|l| !l.trim().is_empty()) {
        let mut parts = line.splitn(4, ' ');
        let (start, end, tool, argv) = match (
            parts.next().and_then(|f| f.parse::<u128>().ok()),
            parts.next().and_then(|f| f.parse::<u128>().ok()),
            parts.next(),
            parts.next().unwrap_or(""),
        ) {
            (Some(start), Some(end), Some(tool), argv) => (start, end, tool, argv),
            _ => panic!("malformed probe log line: {line:?}"),
        };
        intervals.push((start, end));
        if argv.starts_with("UNEXPECTED ") || argv == "UNEXPECTED" {
            unexpected.push(line.to_string());
            continue;
        }
        let command = format!("{tool} {argv}").trim_end().to_string();
        *seen.entry(command).or_default() += 1;
    }
    intervals.sort_unstable();
    let mut stages = 0usize;
    let mut stage_end = 0u128;
    for (start, end) in &intervals {
        if *start >= stage_end {
            stages += 1;
        }
        stage_end = stage_end.max(*end);
    }
    let duplicates = seen
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(line, count)| format!("{line} (x{count})"))
        .collect();
    unexpected.sort();
    (intervals.len(), stages, duplicates, unexpected)
}

/// `expected_calls` pins the documented probe set from the module doc (12 for
/// `doctor`, 7 for `cluster status`) exactly, so a probe silently added or
/// dropped fails here rather than only showing up as a stage-count wobble.
fn assert_fanout(
    label: &str,
    fixture: &Fixture,
    max_stages: usize,
    expected_calls: usize,
    wall_ms: u128,
) {
    let log = fixture.log();
    let (calls, stages, duplicates, unexpected) = probe_stages(&log);
    eprintln!("{label}: {calls} calls, {stages} stages, wall {wall_ms} ms");
    assert!(
        unexpected.is_empty(),
        "{label}: {} unexpected-argv probe(s) hit the stub's fallback rule; \
         the documented probe set changed: {unexpected:?}\n--- probe log ---\n{log}",
        unexpected.len()
    );
    assert_eq!(
        calls, expected_calls,
        "{label}: {calls} calls, expected exactly {expected_calls}; the documented probe set \
         changed\n--- probe log ---\n{log}"
    );
    assert!(
        duplicates.is_empty() && stages <= max_stages,
        "{label}: {calls} calls in {stages} sequential stages (max {max_stages}); \
         duplicate command lines: {duplicates:?}\n--- probe log ---\n{log}"
    );
}

/// Compare stdout or stderr to a committed fixture as raw bytes, or rewrite
/// it under `CURIE_TEST_PROBE_FANOUT_UPDATE_GOLDEN=1` so the baseline binary
/// can seed what the changed binary must still produce byte-for-byte. Raw
/// bytes, not `String::from_utf8_lossy`, because a lossy comparison would
/// paper over a mismatched byte inside otherwise-valid-looking text. stderr
/// is pinned the same way as stdout: the once-per-process fallback warning
/// (`ui.warn`) and the failed-report error text (`failed_report`) both live
/// on stderr, so a plumbing or warning-count regression there would
/// otherwise go unnoticed by a stdout-only pin.
fn check_golden(name: &str, actual: &[u8]) {
    let path = golden_dir().join(name);
    if std::env::var("CURIE_TEST_PROBE_FANOUT_UPDATE_GOLDEN").as_deref() == Ok("1") {
        fs::create_dir_all(golden_dir()).expect("create golden dir");
        fs::write(&path, actual).expect("write golden fixture");
        return;
    }
    let expected =
        fs::read(&path).unwrap_or_else(|e| panic!("read golden fixture {}: {e}", path.display()));
    assert!(
        actual == expected.as_slice(),
        "{} drifted; re-record with CURIE_TEST_PROBE_FANOUT_UPDATE_GOLDEN=1 only after \
         confirming the change is intended\n--- actual ---\n{}\n--- expected ---\n{}",
        path.display(),
        String::from_utf8_lossy(actual),
        String::from_utf8_lossy(&expected)
    );
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `curie doctor` must join its independent docker/kubectl/helm reads and
/// resolve the release fullname once. 5 stages is the floor for this scenario
/// (see the module doc); the sequential implementation took 12.
#[test]
fn doctor_fans_out_independent_probes() {
    let fixture = fixture();
    let (output, wall_ms) = fixture.run(&["--color=never", "--json", "doctor"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "doctor reports rather than fails: stdout {}\nstderr {}",
        stdout_of(&output),
        stderr_of(&output)
    );
    assert_fanout("doctor", &fixture, 5, 12, wall_ms);
}

/// `curie cluster status` must issue helm status, the pod list, convergence,
/// the fullname and the host as one stage, then the two Service reads (which
/// consume the fullname) as a second. The sequential implementation took 6.
/// Exit 1 is part of the contract (see `cluster_status_output_is_golden`).
#[test]
fn cluster_status_fans_out_independent_probes() {
    let fixture = fixture();
    let (output, wall_ms) = fixture.run(&["--color=never", "--json", "cluster", "status"]);
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout {}\nstderr {}",
        stdout_of(&output),
        stderr_of(&output)
    );
    assert_fanout("status", &fixture, 2, 7, wall_ms);
}

/// Joining probes must not move a byte of what either surface prints, so both
/// the machine payload and the human render are pinned, along with the exit
/// code.
#[test]
fn doctor_output_is_golden() {
    let json_fixture = fixture();
    let (json_output, _) = json_fixture.run(&["--color=never", "--json", "doctor"]);
    assert_eq!(
        json_output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&json_output)
    );
    check_golden("doctor.json", &json_output.stdout);
    check_golden("doctor.json.stderr", &json_output.stderr);

    let human_fixture = fixture();
    let (human_output, _) = human_fixture.run(&["--color=never", "doctor"]);
    assert_eq!(
        human_output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&human_output)
    );
    check_golden("doctor.txt", &human_output.stdout);
    check_golden("doctor.txt.stderr", &human_output.stderr);
}

/// Same pin for `cluster status`. Exit 1 is part of the contract here, not an
/// accident: `convergence::observe` cannot verify a revision off the stubbed
/// `helm status ... -o json` (`{}`), so the release reads unhealthy and
/// `status` returns a `CliError::failure`.
#[test]
fn cluster_status_output_is_golden() {
    let json_fixture = fixture();
    let (json_output, _) = json_fixture.run(&["--color=never", "--json", "cluster", "status"]);
    assert_eq!(
        json_output.status.code(),
        Some(1),
        "stderr: {}",
        stderr_of(&json_output)
    );
    check_golden("cluster_status.json", &json_output.stdout);
    check_golden("cluster_status.json.stderr", &json_output.stderr);

    let human_fixture = fixture();
    let (human_output, _) = human_fixture.run(&["--color=never", "cluster", "status"]);
    assert_eq!(
        human_output.status.code(),
        Some(1),
        "stderr: {}",
        stderr_of(&human_output)
    );
    check_golden("cluster_status.txt", &human_output.stdout);
    check_golden("cluster_status.txt.stderr", &human_output.stderr);
}
