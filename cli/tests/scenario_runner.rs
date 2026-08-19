//! `curie scenario`: the five-stage skill-tier pipeline, driven end to end
//! through the REAL binary behind two doubled EXTERNAL peers.
//!
//! 1. **The Docker daemon**, doubled with a fake `docker` executable placed on
//!    the CHILD's PATH -- the precedent `cli/tests/cluster_down_failforward.rs`
//!    sets, which prepends a directory of fakes so they win resolution and
//!    thereby proves wiring the unit tests cannot reach. That file mutates
//!    process-global PATH because it calls into the lib in-process; these tests
//!    invoke the binary as a subprocess, so PATH is set on the child and nothing
//!    process-global is touched.
//! 2. **The runner's ACI HTTP/NDJSON surface**, the seam `cli/tests/support/mod.rs`
//!    names explicitly.
//!
//! With `docker` doubled the command runs its REAL orchestration: the real
//! `bundle::snapshot`, the real `docker run` argv, the real `.curie/runner.json`
//! write, the real `docker inspect` identity read, real graded turns, the real
//! teardown path and the real postcondition checks. No runner state is ever
//! seeded, and there is no test-only branch anywhere.
//!
//! **Why this file carries its own HTTP double instead of `support::serve`.**
//! `commands::start` computes the runner URL as `http://localhost:{opts.port}`
//! (`cli/src/commands.rs:1693`) and polls `/healthz` there, so the double has to
//! answer on the port the command CHOSE, not on an ephemeral one it was told
//! about. `support::serve` binds `127.0.0.1:0` and offers no way to pin a port.
//! The tests below therefore pin the contract that `curie scenario` boots the
//! runner on `commands::DEFAULT_PORT`, the same default `skill up` uses -- the
//! only port a caller can predict, since the clap surface has no `--port` and
//! the manifest has no port field. One double is bound there for the whole test
//! binary and leased by one test at a time; the tests stay individually named
//! and simply serialize on that lease.
//!
//! These assertions are RED until the implementer lands `curie scenario`: today
//! the binary rejects the subcommand outright. That is a BEHAVIORAL red on
//! purpose -- this file references no `curie::scenario::*` symbol, so it keeps
//! compiling against today's crate.

use std::collections::VecDeque;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard, Once, OnceLock};
use std::thread;

use curie::commands::DEFAULT_PORT;
use curie::state::{self, RunnerState};
use curie_aci_protocol::PROTOCOL_VERSION;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

// ---------------------------------------------------------------------------
// The runner double
// ---------------------------------------------------------------------------

/// The scripted runner peer: an ordered queue of NDJSON turns, one per
/// `/v1/event`, and the ordered log of every request it answered.
struct RunnerDouble {
    turns: Mutex<VecDeque<Vec<String>>>,
    log: Mutex<Vec<(String, String)>>,
}

static LEASE: Mutex<()> = Mutex::new(());

fn runner_double() -> &'static RunnerDouble {
    static DOUBLE: OnceLock<RunnerDouble> = OnceLock::new();
    static STARTED: Once = Once::new();
    let double = DOUBLE.get_or_init(|| RunnerDouble {
        turns: Mutex::new(VecDeque::new()),
        log: Mutex::new(Vec::new()),
    });
    STARTED.call_once(|| {
        let listener = TcpListener::bind(("127.0.0.1", DEFAULT_PORT)).unwrap_or_else(|e| {
            panic!(
                "the runner double must bind 127.0.0.1:{DEFAULT_PORT}, the port `curie scenario` \
                 boots on: {e}. Something else is already listening there (a live `curie skill \
                 up`, or `cli/scripts/e2e.sh`); stop it and re-run."
            )
        });
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                thread::spawn(move || answer(double, stream));
            }
        });
    });
    double
}

fn answer(state: &'static RunnerDouble, stream: TcpStream) {
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let mut parts = line.split_whitespace();
        let (Some(method), Some(path)) = (parts.next(), parts.next()) else {
            return;
        };
        let (method, path) = (method.to_string(), path.to_string());

        let mut length = 0usize;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).is_err() {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
        }
        let mut body = vec![0u8; length];
        if length > 0 && reader.read_exact(&mut body).is_err() {
            return;
        }
        lock(&state.log).push((method.clone(), path.clone()));

        let (content_type, payload) = match (method.as_str(), path.as_str()) {
            ("POST", "/v1/event") => {
                let frames = lock(&state.turns).pop_front().unwrap_or_default();
                (
                    "application/x-ndjson",
                    format!("{}\n", frames.join("\n")).into_bytes(),
                )
            }
            ("GET", "/status") => (
                "application/json",
                br#"{"status":"done","ready":true,"turn_active":false}"#.to_vec(),
            ),
            _ => ("application/json", b"{}".to_vec()),
        };
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        );
        let stream = reader.get_mut();
        if stream.write_all(head.as_bytes()).is_err() || stream.write_all(&payload).is_err() {
            return;
        }
        let _ = stream.flush();
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Exclusive use of the double for one scenario run, with its turns scripted
/// and its request log cleared.
struct Lease {
    _guard: MutexGuard<'static, ()>,
}

fn lease(turns: Vec<Vec<String>>) -> Lease {
    let guard = LEASE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let double = runner_double();
    *lock(&double.turns) = turns.into();
    lock(&double.log).clear();
    Lease { _guard: guard }
}

impl Lease {
    /// Every request the runner answered, in order, as `(method, path)`.
    fn recorded(&self) -> Vec<(String, String)> {
        lock(&runner_double().log).clone()
    }

    /// Just the turn-lifecycle paths, in order: what a probe loop looks like on
    /// the wire.
    fn turn_paths(&self) -> Vec<String> {
        self.recorded()
            .into_iter()
            .filter(|(_, path)| path == "/v1/reset" || path == "/v1/event")
            .map(|(_, path)| path)
            .collect()
    }
}

/// One completed turn whose final answer is `text`.
fn turn(text: &str) -> Vec<String> {
    vec![serde_json::to_string(&serde_json::json!({
        "type": "final",
        "version": PROTOCOL_VERSION,
        "text": text,
        "status": "done",
    }))
    .expect("frame serializes")]
}

// ---------------------------------------------------------------------------
// The docker double
// ---------------------------------------------------------------------------

/// A `docker` that records every invocation and answers the reads `skill
/// up`/`skill down` and the new identity readers actually perform:
/// `ps -a --filter name=^N$ --format "{{.Names}}\t{{.ID}}\t{{.Label ...}}"`,
/// `run -d ...` (echoing a container id), `inspect --format` over `.Mounts` and
/// `.Config.Env`, and `rm -f`.
///
/// The `/plugin` mount source it reports is PARSED OUT OF THE REAL `docker run`
/// argv the command issued, so the happy path proves the container was booted
/// on the very snapshot this run packed rather than on a value the test handed
/// back. The env knobs below are the only way that agreement is broken.
const FAKE_DOCKER: &str = r#"#!/bin/sh
state='__STATE__'
all="$*"
printf '%s\n' "$all" >> "$state/docker.log"

case "$all" in
  *.Mounts*)
    [ -n "$CURIE_FAKE_DOCKER_NO_PLUGIN_MOUNT" ] && exit 0
    if [ -n "$CURIE_FAKE_DOCKER_PLUGIN_SOURCE" ]; then
      printf '/plugin\t%s\n' "$CURIE_FAKE_DOCKER_PLUGIN_SOURCE"
    elif [ -f "$state/mount_source" ]; then
      printf '/plugin\t%s\n' "$(cat "$state/mount_source")"
    fi
    exit 0
    ;;
  *.Config.Env*)
    printf '%s\n' 'CURIE_PLUGIN_DIR=/plugin'
    if [ -n "$CURIE_FAKE_DOCKER_FORCE_FAKE_MODEL" ] || [ -f "$state/fake_model" ]; then
      printf '%s\n' 'CURIE_FAKE_MODEL=1'
    fi
    exit 0
    ;;
esac

case "$1" in
  ps)
    if [ -f "$state/container" ]; then
      printf '%s\t%s\t%s\n' "$(cat "$state/name")" 'c0ffeec0ffee' 'curie-cli'
    fi
    exit 0
    ;;
  run)
    prev=''
    for arg in "$@"; do
      case "$arg" in
        */plugin:ro) printf '%s' "${arg%:/plugin:ro}" > "$state/mount_source" ;;
        CURIE_FAKE_MODEL=*) : > "$state/fake_model" ;;
      esac
      case "$prev" in
        --name) printf '%s' "$arg" > "$state/name" ;;
      esac
      prev="$arg"
    done
    : > "$state/container"
    printf '%s\n' 'c0ffeec0ffee'
    exit 0
    ;;
  rm)
    if [ -n "$CURIE_FAKE_DOCKER_SEAL_SNAPSHOTS" ]; then
      chmod 0555 "$CURIE_FAKE_DOCKER_SNAPSHOT_ROOT" 2>/dev/null
    fi
    if [ -z "$CURIE_FAKE_DOCKER_CONTAINER_SURVIVES_RM" ]; then
      rm -f "$state/container"
    fi
    printf '%s\n' 'c0ffeec0ffee'
    exit 0
    ;;
esac
exit 0
"#;

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake executable");
    let mut perms = fs::metadata(&path).expect("stat fake").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod fake executable");
}

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

struct Rig {
    dir: tempfile::TempDir,
    bundle: PathBuf,
    manifest: PathBuf,
    fake_bin: PathBuf,
    docker_state: PathBuf,
}

impl Rig {
    fn new(fixture: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let bundle = dir.path().join("bundle");
        let init = Command::new(bin())
            .args(["init", "weather-bot", "--dir"])
            .arg(&bundle)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("run curie init");
        assert!(
            init.status.success(),
            "init must scaffold a bundle\n{}",
            output_text(&init)
        );

        let raw = fs::read_to_string(
            Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/scenario")).join(fixture),
        )
        .unwrap_or_else(|e| panic!("committed fixture {fixture} must exist: {e}"));
        let manifest = dir.path().join("scenario.json");
        fs::write(
            &manifest,
            raw.replace("BUNDLE_PATH", &bundle.display().to_string()),
        )
        .expect("write the resolved manifest");

        let fake_bin = dir.path().join("fakebin");
        let docker_state = dir.path().join("dockerstate");
        fs::create_dir_all(&fake_bin).expect("create the fake PATH directory");
        fs::create_dir_all(&docker_state).expect("create the docker double state directory");
        write_exec(
            &fake_bin,
            "docker",
            &FAKE_DOCKER.replace("__STATE__", &docker_state.display().to_string()),
        );

        Self {
            dir,
            bundle,
            manifest,
            fake_bin,
            docker_state,
        }
    }

    /// Run `curie scenario <manifest> --json` from `cwd`, with the doubled
    /// `docker` first on the CHILD's PATH.
    fn run_from(&self, cwd: &Path, env: &[(&str, &str)]) -> Output {
        let inherited = std::env::var("PATH").unwrap_or_default();
        let mut cmd = Command::new(bin());
        cmd.arg("scenario")
            .arg(&self.manifest)
            .arg("--json")
            .current_dir(cwd)
            .env("PATH", format!("{}:{inherited}", self.fake_bin.display()))
            .stdin(std::process::Stdio::null());
        for (key, value) in env {
            cmd.env(key, value);
        }
        cmd.output().expect("run curie scenario")
    }

    fn run(&self, env: &[(&str, &str)]) -> Output {
        self.run_from(self.dir.path(), env)
    }

    /// Every `docker` argv line the run issued, in order.
    fn docker_log(&self) -> Vec<String> {
        fs::read_to_string(self.docker_state.join("docker.log"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// The `/plugin` mount source the DAEMON saw, taken straight out of the
    /// `docker run` argv the command issued.
    fn mounted_source(&self) -> String {
        fs::read_to_string(self.docker_state.join("mount_source"))
            .expect("the run must have booted a container with a /plugin mount")
    }

    fn snapshot_root(&self) -> PathBuf {
        self.bundle.join(".curie").join("snapshots")
    }

    fn state_file(&self) -> PathBuf {
        self.bundle.join(".curie").join("runner.json")
    }
}

/// The single JSON object on stdout. Also proves it is a single object: a
/// second one on the same stream is unreadable to an agent consumer.
fn payload(output: &Output) -> serde_json::Value {
    let mut objects =
        serde_json::Deserializer::from_slice(&output.stdout).into_iter::<serde_json::Value>();
    let first = objects
        .next()
        .unwrap_or_else(|| {
            panic!(
                "--json must emit one JSON object, stdout was empty\n{}",
                output_text(output)
            )
        })
        .unwrap_or_else(|e| {
            panic!(
                "--json must emit parseable JSON: {e}\n{}",
                output_text(output)
            )
        });
    assert!(
        objects.next().is_none(),
        "--json must emit EXACTLY one JSON object\n{}",
        output_text(output)
    );
    assert!(first.is_object(), "{first}");
    first
}

fn index_of(log: &[String], prefix: &str) -> usize {
    log.iter()
        .position(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("expected a `docker {prefix}...` invocation, log was: {log:?}"))
}

fn assert_validates_against_result_schema(value: &serde_json::Value) {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/schema/scenario.schema.json");
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("committed schema {path} must exist: {e}"));
    let schema: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("schema {path} must be JSON: {e}"));
    let validator = jsonschema::validator_for(&schema).expect("the schema compiles");
    assert!(
        validator.is_valid(value),
        "the emitted payload must validate against scenario.schema.json: {value}"
    );
}

/// The two-turn script that keeps the committed fixture green: the positive
/// probe's grader (`contains: "Oslo"`) matches, and the negative control's
/// (`contains: "I cannot answer that"`) does not.
fn green_turns() -> Vec<Vec<String>> {
    vec![
        turn("It is 4C in Oslo right now."),
        turn("Bergen is rainy today."),
    ]
}

// ---------------------------------------------------------------------------
// DW1 -- the checked fixture drives the whole pipeline
// ---------------------------------------------------------------------------

/// The headline: a committed fixture drives package -> boot -> identity ->
/// probes -> teardown through the real binary, and every stage leaves an
/// observable behind. The identity assertion compares the report against what
/// the DAEMON saw, not against our own record.
///
/// *Deletion checks*: delete it and nothing proves the command orchestrates
/// anything. Delete the probe loop and `probes` is empty, so it fails. Rename
/// internals and it survives -- it drives the binary by argv against a
/// committed JSON contract.
#[test]
fn the_checked_fixture_drives_the_full_skill_pipeline_through_the_real_binary() {
    let rig = Rig::new("skill-two-probes.json");
    let run = lease(green_turns());
    let output = rig.run(&[]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "a fixture whose probes all resolve must exit 0\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_validates_against_result_schema(&value);
    assert_eq!(value["verdict"], serde_json::json!("passed"), "{value}");
    assert!(value["error"].is_null(), "{value}");

    let tiers = value["tiers"].as_array().expect("tiers is an array");
    assert_eq!(tiers.len(), 1, "the fixture selects one tier: {value}");
    let tier = &tiers[0];
    assert_eq!(tier["tier"], serde_json::json!("skill"), "{value}");
    assert_eq!(tier["verdict"], serde_json::json!("passed"), "{value}");
    assert_eq!(
        tier["identity_verified"],
        serde_json::json!(true),
        "{value}"
    );
    assert_eq!(
        tier["mounted_snapshot_dir"],
        serde_json::json!(rig.mounted_source()),
        "the reported mount must be the one the daemon actually saw: {value}"
    );
    assert!(
        rig.mounted_source()
            .starts_with(&rig.snapshot_root().display().to_string()),
        "the container must have been booted on this run's content-addressed \
         snapshot, not on the editable source: {}",
        rig.mounted_source()
    );

    let probes = tier["probes"].as_array().expect("probes is an array");
    assert_eq!(probes.len(), 2, "both probes must be resolved: {value}");
    assert_eq!(
        probes[0]["id"],
        serde_json::json!("ac1-positive"),
        "{value}"
    );
    assert_eq!(
        probes[1]["id"],
        serde_json::json!("ac1-negative"),
        "{value}"
    );
    assert_eq!(probes[0]["outcome"], serde_json::json!("pass"), "{value}");
    assert_eq!(probes[1]["outcome"], serde_json::json!("pass"), "{value}");
    assert_eq!(
        probes[0]["acceptance_criteria"],
        serde_json::json!(["AC1"]),
        "every probe row carries the criteria it answers: {value}"
    );

    // The wire, in probe order: reset for isolation (#550), then the turn.
    assert_eq!(
        run.turn_paths(),
        vec![
            "/v1/reset".to_string(),
            "/v1/event".to_string(),
            "/v1/reset".to_string(),
            "/v1/event".to_string(),
        ],
        "one reset and one event per probe, in probe order"
    );

    // The daemon side, in pipeline order: boot, identity read, teardown.
    let log = rig.docker_log();
    let booted = index_of(&log, "run ");
    let inspected = log
        .iter()
        .position(|line| line.contains(".Mounts"))
        .unwrap_or_else(|| panic!("expected a `docker inspect` identity read: {log:?}"));
    let removed = index_of(&log, "rm ");
    assert!(
        booted < inspected && inspected < removed,
        "the run must boot, then verify identity, then tear down: {log:?}"
    );

    assert!(
        !rig.state_file().exists(),
        "teardown clears the runner record"
    );
    assert_eq!(
        tier["teardown"]["container_removed"],
        serde_json::json!(true),
        "{value}"
    );
    assert_eq!(
        tier["teardown"]["state_cleared"],
        serde_json::json!(true),
        "{value}"
    );
    assert_eq!(
        tier["teardown"]["snapshot_released"],
        serde_json::json!(true),
        "{value}"
    );
}

// ---------------------------------------------------------------------------
// DW2 -- a negative probe can make the run red
// ---------------------------------------------------------------------------

/// The falsifiability claim, stated as behavior: when the control's grader IS
/// satisfied, the positive probe proves nothing and the whole run goes red.
///
/// *Named mutation*: score a `negative` like a positive in `probe_verdict` and
/// this test must fail.
#[test]
fn a_negative_probe_whose_grader_matches_turns_the_whole_run_red() {
    let rig = Rig::new("skill-two-probes.json");
    let _run = lease(vec![
        turn("It is 4C in Oslo right now."),
        // The control's own grader text, so the control is SATISFIED.
        turn("I cannot answer that."),
    ]);
    let output = rig.run(&[]);

    assert!(
        !output.status.success(),
        "a satisfied negative control must turn the run red\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_validates_against_result_schema(&value);
    assert_eq!(value["verdict"], serde_json::json!("failed"), "{value}");

    let control = &value["tiers"][0]["probes"][1];
    assert_eq!(control["id"], serde_json::json!("ac1-negative"), "{value}");
    assert_eq!(
        control["grader_matched"],
        serde_json::json!(true),
        "`grader_matched` is the RAW grader result, before the kind inversion: {value}"
    );
    assert_eq!(control["outcome"], serde_json::json!("fail"), "{value}");
    assert_eq!(control["passed"], serde_json::json!(false), "{value}");

    let error = value["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("ac1-negative"),
        "the error must name the control that failed: {value}"
    );
    assert!(
        !value["fix"].as_str().unwrap_or_default().trim().is_empty(),
        "a red run carries an actionable fix: {value}"
    );
}

/// The paired positive control. Without it the test above could pass vacuously
/// against an implementation that reds every negative probe regardless of what
/// the grader said.
#[test]
fn a_negative_probe_whose_grader_does_not_match_keeps_the_run_green() {
    let rig = Rig::new("skill-two-probes.json");
    let _run = lease(green_turns());
    let output = rig.run(&[]);

    assert!(
        output.status.success(),
        "an unsatisfied negative control keeps the run green\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    let control = &value["tiers"][0]["probes"][1];
    assert_eq!(
        control["grader_matched"],
        serde_json::json!(false),
        "the control's grader must NOT have matched here: {value}"
    );
    assert_eq!(control["outcome"], serde_json::json!("pass"), "{value}");
    assert_eq!(value["verdict"], serde_json::json!("passed"), "{value}");
}

// ---------------------------------------------------------------------------
// DW3 -- identity mismatch fails closed
// ---------------------------------------------------------------------------

/// The daemon reports a `/plugin` mount source that is NOT the snapshot this
/// run packed -- a boot that mounted a different directory, or a stale
/// container holding the name. The tier aborts before any probe runs.
///
/// *Mutation*: make the identity check return Ok on mismatch and this fails.
#[test]
fn a_container_mounting_a_different_directory_aborts_before_any_probe() {
    let rig = Rig::new("skill-two-probes.json");
    let run = lease(green_turns());
    let output = rig.run(&[(
        "CURIE_FAKE_DOCKER_PLUGIN_SOURCE",
        "/tmp/some-other-bundle-snapshot",
    )]);

    assert!(
        !output.status.success(),
        "an identity mismatch must fail closed\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_validates_against_result_schema(&value);
    let tier = &value["tiers"][0];
    assert_eq!(
        tier["identity_verified"],
        serde_json::json!(false),
        "{value}"
    );
    assert_eq!(
        tier["mounted_snapshot_dir"],
        serde_json::json!("/tmp/some-other-bundle-snapshot"),
        "the daemon's reading must be reported so the mismatch is diagnosable: {value}"
    );
    assert_eq!(
        tier["probes"],
        serde_json::json!([]),
        "no probe may run against an unverified container: {value}"
    );
    assert!(
        !run.turn_paths().contains(&"/v1/event".to_string()),
        "no turn may be sent against an unverified container: {:?}",
        run.recorded()
    );
}

/// Absent must not read as "matches": a container reporting no `/plugin` mount
/// at all is a mismatch, not a pass. This is the fail-open direction, and it is
/// the one a naive `Option` comparison gets wrong.
#[test]
fn a_container_reporting_no_plugin_mount_is_a_mismatch_not_a_pass() {
    let rig = Rig::new("skill-two-probes.json");
    let run = lease(green_turns());
    let output = rig.run(&[("CURIE_FAKE_DOCKER_NO_PLUGIN_MOUNT", "1")]);

    assert!(
        !output.status.success(),
        "an absent /plugin mount must fail closed\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    let tier = &value["tiers"][0];
    assert_eq!(
        tier["identity_verified"],
        serde_json::json!(false),
        "{value}"
    );
    assert!(
        tier["mounted_snapshot_dir"].is_null(),
        "nothing was mounted, so the key is null -- never a fabricated path: {value}"
    );
    assert_eq!(tier["probes"], serde_json::json!([]), "{value}");
    assert!(
        !run.turn_paths().contains(&"/v1/event".to_string()),
        "no turn may be sent: {:?}",
        run.recorded()
    );
}

/// The manifest declares `live`, but the live container carries
/// `CURIE_FAKE_MODEL=1`. Grading a fake turn as a real one is the false green
/// ADR-0055 exists to stop, so the tier is refused before any probe.
#[test]
fn a_container_booted_in_the_wrong_model_mode_is_refused_before_any_probe() {
    let rig = Rig::new("skill-two-probes.json");
    let run = lease(green_turns());
    let output = rig.run(&[("CURIE_FAKE_DOCKER_FORCE_FAKE_MODEL", "1")]);

    assert!(
        !output.status.success(),
        "a model-mode mismatch must fail closed\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_validates_against_result_schema(&value);
    let tier = &value["tiers"][0];
    assert_eq!(
        tier["container_fake_model"],
        serde_json::json!(true),
        "the daemon's reading of the container env must be reported: {value}"
    );
    assert_eq!(
        tier["probes"],
        serde_json::json!([]),
        "no probe may run in the wrong model mode: {value}"
    );
    assert!(
        !run.turn_paths().contains(&"/v1/event".to_string()),
        "no turn may be sent: {:?}",
        run.recorded()
    );
    let error = format!(
        "{} {}",
        value["error"].as_str().unwrap_or_default(),
        tier["error"].as_str().unwrap_or_default()
    );
    assert!(
        error.to_lowercase().contains("model"),
        "the diagnosis must name the model mode, not read like an identity \
         mismatch: {value}"
    );
}

// ---------------------------------------------------------------------------
// DW4 -- teardown runs, and its postconditions are verified
// ---------------------------------------------------------------------------

/// Teardown is unconditional: a probe going red mid-run must not skip it, and
/// the three postconditions must be asserted rather than self-reported.
///
/// *Mutation*: move teardown inside the success branch and this fails.
#[test]
fn teardown_runs_after_a_mid_run_probe_failure_and_verifies_its_postconditions() {
    let rig = Rig::new("skill-two-probes.json");
    let _run = lease(vec![
        // The positive probe's grader (`contains: "Oslo"`) is NOT satisfied.
        turn("I have no idea."),
        turn("Bergen is rainy today."),
    ]);
    let output = rig.run(&[]);

    assert!(
        !output.status.success(),
        "a failed positive probe reds the run\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_validates_against_result_schema(&value);
    let tier = &value["tiers"][0];
    assert_eq!(tier["probes"][0]["outcome"], serde_json::json!("fail"));
    let teardown = &tier["teardown"];
    assert_eq!(teardown["attempted"], serde_json::json!(true), "{value}");
    assert_eq!(
        teardown["container_removed"],
        serde_json::json!(true),
        "{value}"
    );
    assert_eq!(
        teardown["state_cleared"],
        serde_json::json!(true),
        "{value}"
    );
    assert_eq!(
        teardown["snapshot_released"],
        serde_json::json!(true),
        "{value}"
    );
    assert!(teardown["error"].is_null(), "{value}");
    assert!(
        value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("ac1-positive"),
        "the original probe diagnosis must survive teardown: {value}"
    );
    assert!(
        !rig.state_file().exists() && !rig.snapshot_root().join("..").join("runner.json").exists(),
        "the record is gone from disk, not merely reported gone"
    );
}

/// The abort path is the one most likely to leak: a tier that never ran a probe
/// still booted a container, so it still owns a teardown.
#[test]
fn teardown_runs_even_when_identity_verification_aborts_the_tier() {
    let rig = Rig::new("skill-two-probes.json");
    let _run = lease(green_turns());
    let output = rig.run(&[(
        "CURIE_FAKE_DOCKER_PLUGIN_SOURCE",
        "/tmp/some-other-bundle-snapshot",
    )]);

    assert!(!output.status.success(), "{}", output_text(&output));
    let value = payload(&output);
    let teardown = &value["tiers"][0]["teardown"];
    assert_eq!(teardown["attempted"], serde_json::json!(true), "{value}");
    assert_eq!(
        teardown["container_removed"],
        serde_json::json!(true),
        "an aborted tier still owns the container it booted: {value}"
    );
    let log = rig.docker_log();
    assert!(
        index_of(&log, "rm ") > index_of(&log, "run "),
        "the container booted by an aborted tier must still be removed: {log:?}"
    );
    assert!(
        !rig.state_file().exists(),
        "an aborted tier still clears its record"
    );
}

/// The postconditions have teeth: a container that outlives `docker rm` is a
/// real leak and must red the run. This is the case that would catch the
/// deliberately tolerant snapshot/teardown warnings on the `skill down` path.
#[test]
fn a_container_that_survives_teardown_turns_the_run_red() {
    let rig = Rig::new("skill-two-probes.json");
    let _run = lease(green_turns());
    let output = rig.run(&[("CURIE_FAKE_DOCKER_CONTAINER_SURVIVES_RM", "1")]);

    assert!(
        !output.status.success(),
        "a stranded container must never be reported as a clean run\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_validates_against_result_schema(&value);
    assert_eq!(value["verdict"], serde_json::json!("failed"), "{value}");
    let teardown = &value["tiers"][0]["teardown"];
    assert_eq!(
        teardown["container_removed"],
        serde_json::json!(false),
        "the postcondition is asserted against the daemon, not assumed from a \
         successful `docker rm`: {value}"
    );
    assert!(
        !teardown["error"].is_null(),
        "an unmet postcondition carries its own diagnosis: {value}"
    );
}

/// The same teeth for the snapshot: a snapshot directory still on disk after
/// teardown means the run leaked the artifact it packed.
#[test]
fn a_snapshot_left_on_disk_turns_the_run_red() {
    let rig = Rig::new("skill-two-probes.json");
    let root = rig.snapshot_root();
    let _run = lease(green_turns());
    let output = rig.run(&[
        ("CURIE_FAKE_DOCKER_SEAL_SNAPSHOTS", "1"),
        (
            "CURIE_FAKE_DOCKER_SNAPSHOT_ROOT",
            &root.display().to_string(),
        ),
    ]);
    // Restore write permission immediately so the tempdir can be cleaned up
    // regardless of how the assertions below go.
    if let Ok(meta) = fs::metadata(&root) {
        let mut perms = meta.permissions();
        perms.set_mode(0o755);
        let _ = fs::set_permissions(&root, perms);
    }

    assert!(
        !output.status.success(),
        "a leaked snapshot must never be reported as a clean run\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    let teardown = &value["tiers"][0]["teardown"];
    assert_eq!(
        teardown["snapshot_released"],
        serde_json::json!(false),
        "the snapshot postcondition is asserted on the filesystem: {value}"
    );
    assert_eq!(value["verdict"], serde_json::json!("failed"), "{value}");
}

/// Two independent failures must stay independent: a tier already red keeps its
/// probe diagnosis, and the teardown failure is carried separately. Collapsing
/// them loses the reason the run was red in the first place.
#[test]
fn a_teardown_failure_never_overwrites_the_original_diagnosis() {
    let rig = Rig::new("skill-two-probes.json");
    let _run = lease(vec![
        turn("I have no idea."),
        turn("Bergen is rainy today."),
    ]);
    let output = rig.run(&[("CURIE_FAKE_DOCKER_CONTAINER_SURVIVES_RM", "1")]);

    assert!(!output.status.success(), "{}", output_text(&output));
    let value = payload(&output);
    let tier = &value["tiers"][0];
    let tier_error = tier["error"].as_str().unwrap_or_default();
    assert!(
        tier_error.contains("ac1-positive"),
        "the tier error must still name the probe failure: {value}"
    );
    let teardown_error = tier["teardown"]["error"].as_str().unwrap_or_default();
    assert!(
        !teardown_error.is_empty(),
        "the teardown failure must be carried too: {value}"
    );
    assert_ne!(
        tier_error, teardown_error,
        "two failures, two diagnoses: {value}"
    );
}

/// Teardown targets the MANIFEST's bundle directory. A CWD-scoped teardown
/// would clear whatever runner record happens to sit in the working directory
/// -- someone else's live runner.
///
/// *Mutation*: point the teardown's directory back at the process CWD and this
/// fails.
#[test]
fn teardown_targets_the_manifests_bundle_directory_not_the_cwd() {
    let rig = Rig::new("skill-two-probes.json");
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let bystander = RunnerState {
        container_id: "deadbeef0000".into(),
        container_name: format!("curie-bystander-{}", std::process::id()),
        image: "curie-runner".into(),
        port: 7999,
        base_url: "http://localhost:7999".into(),
        session_id: "bystander".into(),
        plugin_dir: elsewhere.path().display().to_string(),
        fake_model: true,
        ollama_container: None,
        network: None,
        model_base_url: None,
        bundle_digest: None,
        bundle_snapshot_dir: None,
    };
    state::save(elsewhere.path(), &bystander).expect("record the bystander runner");

    let _run = lease(green_turns());
    let output = rig.run_from(elsewhere.path(), &[]);

    assert!(
        output.status.success(),
        "the run itself is green\n{}",
        output_text(&output)
    );
    assert!(
        !rig.state_file().exists(),
        "the MANIFEST's bundle record must be cleared"
    );
    let survivor = state::load(elsewhere.path())
        .expect("load the bystander record")
        .expect("a runner recorded in the CWD must be untouched by a scenario run");
    assert_eq!(
        survivor.container_name, bystander.container_name,
        "the scenario tore down the wrong bundle's runner"
    );
}
