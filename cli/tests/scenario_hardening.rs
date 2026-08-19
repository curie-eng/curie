//! `curie scenario`: the hardening pins from the cross-engine review of the
//! frozen candidate.
//!
//! Every case here is a BEHAVIORAL red against the current implementation: it
//! drives the real binary as a subprocess with a doubled `docker` on the CHILD's
//! PATH and a doubled runner HTTP peer bound on `commands::DEFAULT_PORT`, the
//! exact harness `cli/tests/scenario_runner.rs` and `cli/tests/scenario_manifest.rs`
//! establish. Nothing here reaches a real daemon, a real network, or a
//! credential.
//!
//! The runner double binds one fixed port for the whole test binary and is
//! leased by one test at a time, for the same reason the runner-pipeline file
//! does it: `commands::start` polls `http://localhost:{port}` on the port the
//! COMMAND chose, and there is no flag or manifest field that can redirect it.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, MutexGuard, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use curie::commands::DEFAULT_PORT;
use curie::docker::RUNNER_CONTAINER_LOCAL;
use curie_aci_protocol::PROTOCOL_VERSION;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

// ---------------------------------------------------------------------------
// The runner double
// ---------------------------------------------------------------------------

/// Answer every request normally.
const MODE_NORMAL: u8 = 0;
/// Accept the connection for a turn-lifecycle request and then never answer it:
/// the stalled-peer shape a deadline-free client cannot survive.
const MODE_STALL: u8 = 1;

static MODE: AtomicU8 = AtomicU8::new(MODE_NORMAL);
static LEASE: Mutex<()> = Mutex::new(());

struct RunnerDouble {
    turns: Mutex<std::collections::VecDeque<Vec<String>>>,
    log: Mutex<Vec<(String, String)>>,
}

fn runner_double() -> &'static RunnerDouble {
    static DOUBLE: OnceLock<RunnerDouble> = OnceLock::new();
    static STARTED: Once = Once::new();
    let double = DOUBLE.get_or_init(|| RunnerDouble {
        turns: Mutex::new(std::collections::VecDeque::new()),
        log: Mutex::new(Vec::new()),
    });
    STARTED.call_once(|| {
        let listener = TcpListener::bind(("127.0.0.1", DEFAULT_PORT)).unwrap_or_else(|e| {
            panic!(
                "the runner double must bind 127.0.0.1:{DEFAULT_PORT}, the port `curie scenario` \
                 boots on: {e}. Something else is already listening there; stop it and re-run."
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

        let turn_path = path == "/v1/reset" || path == "/v1/event";
        if turn_path && MODE.load(Ordering::SeqCst) == MODE_STALL {
            // Connected, request consumed, no response and no close. A client
            // with no read deadline waits here forever.
            thread::sleep(Duration::from_secs(600));
            return;
        }

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

struct Lease {
    _guard: MutexGuard<'static, ()>,
}

fn lease(turns: Vec<Vec<String>>, mode: u8) -> Lease {
    let guard = LEASE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let double = runner_double();
    *lock(&double.turns) = turns.into();
    lock(&double.log).clear();
    MODE.store(mode, Ordering::SeqCst);
    Lease { _guard: guard }
}

impl Drop for Lease {
    fn drop(&mut self) {
        MODE.store(MODE_NORMAL, Ordering::SeqCst);
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

/// The two-turn script that satisfies the committed fixture: the positive
/// probe's grader (`contains: "Oslo"`) matches, the control's does not.
fn green_turns() -> Vec<Vec<String>> {
    vec![
        turn("It is 4C in Oslo right now."),
        turn("Bergen is rainy today."),
    ]
}

// ---------------------------------------------------------------------------
// The docker double
// ---------------------------------------------------------------------------

/// A `docker` that records every invocation, answers the reads the boot,
/// identity and teardown stages perform, and can be pushed into each hazard the
/// review named through env knobs read at invocation time:
///
/// - `CURIE_HARDEN_NAME_TAKEN`: the runner name is already held, so the boot's
///   name-conflict preflight refuses AFTER the snapshot has been packed.
/// - `CURIE_HARDEN_PS_FAILS_AFTER_RM`: the daemon errors on the teardown's
///   container probe, so "the container is gone" is UNVERIFIED rather than
///   observed.
/// - `CURIE_HARDEN_FAKE_MODEL_VALUE`: the literal `CURIE_FAKE_MODEL` value the
///   container reports.
/// - `CURIE_HARDEN_SWAP_ON_NAME`: the container NAME now resolves to a different
///   container than the id `docker run` returned, so a read keyed by name and a
///   read keyed by id disagree.
/// - `CURIE_HARDEN_UNREADABLE_SNAPSHOT_ROOT`: teardown drops the execute bit on
///   the snapshot root, so a metadata read of the snapshot directory ERRORS
///   rather than reporting absence.
/// - `CURIE_HARDEN_RENAME_ON_RM`: the removal frees the container NAME while the
///   container the boot recorded keeps running under its own id, the shape a
///   name-only postcondition reports as removed.
///
/// The `/plugin` mount source it reports is parsed out of the REAL `docker run`
/// argv the command issued, so agreement is never handed back by the test.
const FAKE_DOCKER: &str = r#"#!/bin/sh
state='__STATE__'
runner_name='__RUNNER_NAME__'
all="$*"
printf '%s\n' "$all" >> "$state/docker.log"

target=''
for a in "$@"; do target="$a"; done

case "$all" in
  *.Mounts*)
    if [ -f "$state/mount_source" ]; then
      printf '/plugin\t%s\n' "$(cat "$state/mount_source")"
    fi
    exit 0
    ;;
  *.Config.Env*)
    printf '%s\n' 'CURIE_PLUGIN_DIR=/plugin'
    if [ -n "$CURIE_HARDEN_SWAP_ON_NAME" ]; then
      if [ "$target" = "$runner_name" ]; then
        printf '%s\n' 'CURIE_FAKE_MODEL=1'
      fi
      exit 0
    fi
    if [ -n "$CURIE_HARDEN_FAKE_MODEL_VALUE" ]; then
      printf 'CURIE_FAKE_MODEL=%s\n' "$CURIE_HARDEN_FAKE_MODEL_VALUE"
    elif [ -f "$state/fake_model" ]; then
      printf '%s\n' 'CURIE_FAKE_MODEL=1'
    fi
    exit 0
    ;;
esac

case "$1" in
  ps)
    if [ -n "$CURIE_HARDEN_PS_FAILS_AFTER_RM" ] && [ -f "$state/removed" ]; then
      printf '%s\n' 'error during connect: Cannot connect to the Docker daemon' >&2
      exit 1
    fi
    case "$all" in
      *"--filter id="*)
        if [ -f "$state/id_present" ]; then
          printf '%s\n' 'c0ffeec0ffee'
        fi
        exit 0
        ;;
    esac
    if [ -n "$CURIE_HARDEN_NAME_TAKEN" ]; then
      printf '%s\t%s\t%s\n' "$runner_name" 'deadbeefdead' 'curie-cli'
      exit 0
    fi
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
    : > "$state/id_present"
    printf '%s\n' 'c0ffeec0ffee'
    exit 0
    ;;
  rm)
    if [ -n "$CURIE_HARDEN_UNREADABLE_SNAPSHOT_ROOT" ]; then
      chmod 0666 "$CURIE_HARDEN_SNAPSHOT_ROOT" 2>/dev/null
    fi
    rm -f "$state/container"
    if [ -z "$CURIE_HARDEN_RENAME_ON_RM" ]; then
      rm -f "$state/id_present"
    fi
    : > "$state/removed"
    printf '%s\n' 'c0ffeec0ffee'
    exit 0
    ;;
esac
exit 0
"#;

/// A `git` that answers "not a git tree" (the honest answer for a bundle in a
/// tempdir, which is what the REAL git already says here) and, under
/// `CURIE_HARDEN_CORRUPT_STATE`, overwrites an already-written runner record
/// with something that will not parse.
///
/// `commands::start` calls `git rev-parse` once, between `state::save` and the
/// scenario's read-back of that record, so this is the seam that makes the boot
/// leave a container behind AND a record that cannot be read. The
/// already-exists guard is what keeps the earlier `source_commit` call (which
/// runs before anything is recorded) from writing the file itself.
const FAKE_GIT: &str = r#"#!/bin/sh
if [ -n "$CURIE_HARDEN_CORRUPT_STATE" ] && [ -f "$CURIE_HARDEN_CORRUPT_STATE" ]; then
  printf '%s' 'not a runner record' > "$CURIE_HARDEN_CORRUPT_STATE"
fi
exit 1
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
    /// Scaffold a real bundle, then materialize the committed fixture with its
    /// `BUNDLE_PATH` resolved and `edit` applied to the parsed JSON. The checked
    /// file stays the contract; `edit` expresses only what this case varies.
    fn new(fixture: &str, edit: impl FnOnce(&mut serde_json::Value, &Path)) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let bundle = dir.path().join("bundle");
        let init = Command::new(bin())
            .args(["init", "weather-bot", "--dir"])
            .arg(&bundle)
            .stdin(Stdio::null())
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
        let mut value: serde_json::Value =
            serde_json::from_str(&raw.replace("BUNDLE_PATH", &bundle.display().to_string()))
                .expect("the committed fixture is JSON");
        edit(&mut value, dir.path());
        let manifest = dir.path().join("scenario.json");
        fs::write(
            &manifest,
            serde_json::to_string_pretty(&value).expect("manifest serializes"),
        )
        .expect("write the resolved manifest");

        let fake_bin = dir.path().join("fakebin");
        let docker_state = dir.path().join("dockerstate");
        fs::create_dir_all(&fake_bin).expect("create the fake PATH directory");
        fs::create_dir_all(&docker_state).expect("create the docker double state directory");
        write_exec(
            &fake_bin,
            "docker",
            &FAKE_DOCKER
                .replace("__STATE__", &docker_state.display().to_string())
                .replace("__RUNNER_NAME__", RUNNER_CONTAINER_LOCAL),
        );
        write_exec(&fake_bin, "git", FAKE_GIT);

        Self {
            dir,
            bundle,
            manifest,
            fake_bin,
            docker_state,
        }
    }

    fn unchanged(fixture: &str) -> Self {
        Self::new(fixture, |_, _| {})
    }

    fn command(&self, env: &[(&str, &str)]) -> Command {
        let inherited = std::env::var("PATH").unwrap_or_default();
        let mut cmd = Command::new(bin());
        cmd.arg("scenario")
            .arg(&self.manifest)
            .arg("--json")
            .current_dir(self.dir.path())
            .env("PATH", format!("{}:{inherited}", self.fake_bin.display()))
            .stdin(Stdio::null());
        for (key, value) in env {
            cmd.env(key, value);
        }
        cmd
    }

    fn run(&self, env: &[(&str, &str)]) -> Output {
        self.command(env).output().expect("run curie scenario")
    }

    fn docker_log_path(&self) -> PathBuf {
        self.docker_state.join("docker.log")
    }

    fn docker_log(&self) -> Vec<String> {
        fs::read_to_string(self.docker_log_path())
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn snapshot_root(&self) -> PathBuf {
        self.bundle.join(".curie").join("snapshots")
    }

    fn state_file(&self) -> PathBuf {
        self.bundle.join(".curie").join("runner.json")
    }

    /// Every entry directly under the snapshot root, or an empty list when the
    /// root itself was never created.
    fn snapshot_entries(&self) -> Vec<String> {
        let Ok(entries) = fs::read_dir(self.snapshot_root()) else {
            return Vec::new();
        };
        entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect()
    }

    /// Restore traversal on the snapshot root so the tempdir can always be
    /// cleaned up, whatever the assertions did.
    fn unseal_snapshot_root(&self) {
        if let Ok(meta) = fs::metadata(self.snapshot_root()) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            let _ = fs::set_permissions(self.snapshot_root(), perms);
        }
    }
}

/// The single JSON object on stdout.
fn payload(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "--json must emit one JSON object: {e}\n{}",
            output_text(output)
        )
    })
}

// ---------------------------------------------------------------------------
// Finding 1 -- the model-mode read (cli/src/scenario.rs:845, :865)
// ---------------------------------------------------------------------------

/// A tier whose identity check FAILED must never report `identity_verified:
/// true`. The mounted-identity arm already reports false; the model-mode arm
/// returns true beside a refusal, so the one field a consumer reads to ask
/// "was this container proved to be the artifact under test" says yes on a run
/// that proved nothing.
///
/// *Deletion checks*: delete it and a red identity run keeps advertising a
/// verified identity. Delete the model-mode gate and the tier goes green, so the
/// refusal assertion fails. Rename internals and it survives -- it drives the
/// binary by argv and reads a committed JSON field name.
#[test]
fn a_model_mode_mismatch_must_not_report_a_verified_identity() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_FAKE_MODEL_VALUE", "1")]);

    assert!(
        !output.status.success(),
        "a model-mode mismatch must fail closed\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    let tier = &value["tiers"][0];
    assert_eq!(
        tier["probes"],
        serde_json::json!([]),
        "no probe may run in the wrong model mode: {value}"
    );
    assert_eq!(
        tier["identity_verified"],
        serde_json::json!(false),
        "the model mode is half of runtime identity, so a mismatch leaves identity \
         UNVERIFIED: {value}"
    );
}

/// The container reports `CURIE_FAKE_MODEL=false`. The runtime's own parse
/// (`runner/src/curie_runner/__main__.py`, and its CLI twin
/// `local::fake_model_is_truthy`) accepts only `1`/`true`/`yes`, and that boot
/// comment names this exact value as the one a non-`0` truthiness test gets
/// wrong. So this container is LIVE, and a `live` manifest must run against it.
///
/// *Deletion checks*: delete it and the scenario runner keeps a private
/// truthiness rule that disagrees with the runtime it is verifying, red-flagging
/// healthy live runs. Delete the model-mode read and the manifest's claim goes
/// unchecked, which the mismatch test above catches. Rename internals and it
/// survives -- the env NAME and its accepted values are the contract.
#[test]
fn an_unrecognized_fake_model_value_is_read_the_way_the_runtime_reads_it() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_FAKE_MODEL_VALUE", "false")]);

    assert!(
        output.status.success(),
        "CURIE_FAKE_MODEL=false boots the runner LIVE, so a live manifest must run\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_eq!(
        value["tiers"][0]["container_fake_model"],
        serde_json::json!(false),
        "the daemon's reading must be interpreted by the runtime's own rule: {value}"
    );
}

// ---------------------------------------------------------------------------
// Finding 2 -- a boot that aborts after the pack (cli/src/commands.rs:1529,
// cli/src/scenario.rs:733)
// ---------------------------------------------------------------------------

/// The snapshot is materialized (`scenario.rs:733`) before the boot's
/// name-conflict preflight (`commands.rs:1529`) can refuse. Nothing has recorded
/// it at that point, so no later `skill down` can ever find it: an abort here
/// leaks the artifact permanently.
///
/// *Deletion checks*: delete it and every abort arm between the pack and a
/// recorded state can leak silently. Delete the release and the snapshot root
/// keeps a directory, so it fails. Rename internals and it survives -- it
/// asserts on the on-disk artifact, not on a call.
#[test]
fn a_boot_that_aborts_after_the_pack_leaves_no_snapshot_behind() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let output = rig.run(&[("CURIE_HARDEN_NAME_TAKEN", "1")]);

    assert!(
        !output.status.success(),
        "a taken runner name must refuse the boot\n{}",
        output_text(&output)
    );
    assert_eq!(
        rig.snapshot_entries(),
        Vec::<String>::new(),
        "the snapshot this run packed is unrecorded, so an abort must release it: {:?}",
        rig.snapshot_entries()
    );
}

/// The same abort, on the reporting side. `scenario::run` propagates the boot
/// error with `?`, so the run emits the generic `{error, fix}` object instead of
/// the scenario-shaped payload every other red path emits. An agent consumer
/// branching on `tiers`/`verdict` reads nothing at all.
///
/// *Deletion checks*: delete it and the red boot path silently emits a second
/// payload shape. Delete the shaped emit and `tiers` is missing, so it fails.
/// Rename internals and it survives -- the emitted keys are the contract.
#[test]
fn a_boot_failure_still_emits_the_scenario_shaped_payload() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let output = rig.run(&[("CURIE_HARDEN_NAME_TAKEN", "1")]);

    let value = payload(&output);
    assert!(
        value.get("tiers").is_some() && value.get("verdict").is_some(),
        "a failed boot is a failed SCENARIO and must emit the scenario payload, not the \
         generic error object: {value}"
    );
    assert_eq!(
        value["verdict"],
        serde_json::json!("failed"),
        "the run did not succeed: {value}"
    );
}

// ---------------------------------------------------------------------------
// Finding 3 -- unverified teardown reported as verified (cli/src/scenario.rs:982)
// ---------------------------------------------------------------------------

/// The teardown probe's ERROR is collapsed into "no container", so a daemon that
/// cannot answer produces `container_removed: true`. The postcondition claims an
/// observation that was never made.
///
/// *Deletion checks*: delete it and an unreachable daemon greens the very
/// assertion that exists to catch a stranded container. Delete the error
/// handling and the probe error surfaces, so it fails. Rename internals and it
/// survives -- it asserts the emitted postcondition against a daemon that
/// refused to answer.
#[test]
fn a_teardown_probe_the_daemon_could_not_answer_is_not_a_satisfied_postcondition() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_PS_FAILS_AFTER_RM", "1")]);

    let value = payload(&output);
    let teardown = &value["tiers"][0]["teardown"];
    assert_ne!(
        teardown["container_removed"],
        serde_json::json!(true),
        "the daemon errored, so nothing observed the container gone: {value}"
    );
    assert!(
        !teardown["error"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .is_empty(),
        "the daemon's failure must surface in the teardown diagnosis: {value}"
    );
    assert!(
        !output.status.success(),
        "an unverified teardown must never be reported as a clean run\n{}",
        output_text(&output)
    );
}

/// The filesystem half of the same collapse: `Path::exists` answers false for a
/// metadata ERROR as well as for a genuine absence, so a snapshot directory
/// under a root that cannot be traversed is reported RELEASED while it is still
/// on disk.
///
/// *Deletion checks*: delete it and a leaked snapshot behind an unreadable
/// parent reports clean. Delete the error handling and the read errors, so it
/// fails. Rename internals and it survives -- it compares the report against the
/// directory that is still there.
#[test]
fn a_snapshot_whose_presence_could_not_be_read_is_not_a_released_snapshot() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let root = rig.snapshot_root().display().to_string();
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[
        ("CURIE_HARDEN_UNREADABLE_SNAPSHOT_ROOT", "1"),
        ("CURIE_HARDEN_SNAPSHOT_ROOT", &root),
    ]);
    rig.unseal_snapshot_root();

    assert!(
        !rig.snapshot_entries().is_empty(),
        "the rig must actually have left a snapshot behind for this case to mean anything"
    );
    let value = payload(&output);
    let teardown = &value["tiers"][0]["teardown"];
    assert_ne!(
        teardown["snapshot_released"],
        serde_json::json!(true),
        "the snapshot is still on disk and its presence was never readable: {value}"
    );
    assert!(
        !output.status.success(),
        "a leaked snapshot must never be reported as a clean run\n{}",
        output_text(&output)
    );
}

// ---------------------------------------------------------------------------
// Finding 4 -- the tier refusal is reachable only after the bundle resolves
// (cli/src/scenario.rs:672)
// ---------------------------------------------------------------------------

/// A tier this command cannot run is refused Unsupported (exit 4, ADR-0041), and
/// that refusal must not depend on anything about the bundle: the tier alone
/// decides it. Today the bundle is canonicalized first, so a `local` manifest
/// whose `bundle_path` does not exist exits 1 with a path error, and the ADR-0041
/// contract an agent branches on is lost to an unrelated failure.
///
/// *Deletion checks*: delete it and the refusal's exit class silently depends on
/// the filesystem. Delete the ordering fix and the exit code is 1, so it fails.
/// Rename internals and it survives -- it asserts an exit code and a tier name.
#[test]
fn an_unsupported_tier_is_refused_before_the_bundle_is_resolved() {
    let rig = Rig::new("claims-local-tier.json", |value, dir| {
        value["bundle_path"] = serde_json::json!(dir.join("no-such-bundle").display().to_string());
    });
    let output = rig.run(&[]);

    assert_eq!(
        output.status.code(),
        Some(4),
        "the tier alone decides this refusal, so it must not wait on the bundle path\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert!(
        value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("local"),
        "the refusal must name the tier it refused: {value}"
    );
}

// ---------------------------------------------------------------------------
// Finding 5 -- runtime validation weaker than the committed schema
// (cli/src/scenario.rs:163)
// ---------------------------------------------------------------------------

/// `expect.value` carries `minLength: 1` in `scenario-manifest.schema.json`, and
/// `parse_manifest` does not enforce it. An empty `contains` value matches every
/// string, so the positive probe passes against any completed reply whatsoever:
/// the vacuous green the whole falsifiability contract exists to prevent.
///
/// *Deletion checks*: delete it and a hand-written manifest (which never passes
/// through the schema) buys a permanently green probe. Delete the check and the
/// run reaches Docker instead of refusing, so it fails. Rename internals and it
/// survives -- it drives the binary against a manifest.
#[test]
fn an_empty_positive_grader_value_is_refused_before_anything_is_packed() {
    let rig = Rig::new("skill-two-probes.json", |value, _| {
        value["probes"][0]["expect"]["value"] = serde_json::json!("");
    });
    // Leased even though a refused manifest never reaches the runner: today it
    // is NOT refused, so the run does boot and probe, and an unleased run would
    // consume another test's scripted turns.
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a grader that matches every reply is a Usage error, never a probe\n{}",
        output_text(&output)
    );
    assert!(
        !rig.docker_log_path().exists(),
        "a manifest refused at parse must never reach Docker: {:?}",
        rig.docker_log()
    );
    assert_eq!(
        rig.snapshot_entries(),
        Vec::<String>::new(),
        "nothing may be packed before the manifest is accepted"
    );
}

/// `evals::load_suite` already refuses an eval case whose regex will not
/// compile, because `Grader::grade` answers false for an uncompilable pattern.
/// On a NEGATIVE probe that false is then INVERTED into a pass, so a typo'd
/// control is a control that can never fail: the scenario becomes unfalsifiable
/// while reporting green.
///
/// *Deletion checks*: delete it and a broken control silently greens forever.
/// Delete the check and the run reaches Docker instead of refusing, so it fails.
/// Rename internals and it survives -- the manifest and the exit class are the
/// contract.
#[test]
fn an_uncompilable_negative_regex_is_refused_rather_than_inverted_into_a_pass() {
    let rig = Rig::new("skill-two-probes.json", |value, _| {
        value["probes"][1]["expect"]["grader"] = serde_json::json!("regex");
        value["probes"][1]["expect"]["value"] = serde_json::json!("cannot [answer");
    });
    // Leased for the same reason as the case above: an unrefused manifest boots
    // and probes, and would otherwise steal another test's scripted turns.
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[]);

    assert_eq!(
        output.status.code(),
        Some(2),
        "a control whose grader cannot compile can never fail, so it is a Usage error\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert!(
        value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("ac1-negative"),
        "the refusal must name the probe an author has to fix: {value}"
    );
    assert!(
        !rig.docker_log_path().exists(),
        "a manifest refused at parse must never reach Docker: {:?}",
        rig.docker_log()
    );
}

// ---------------------------------------------------------------------------
// Finding 6 -- no deadline on the turn lifecycle (cli/src/scenario.rs:900)
// ---------------------------------------------------------------------------

/// `RunnerClient` sets a connect timeout and no read timeout, so a peer that
/// accepts the connection and never answers hangs the reset (and the streamed
/// turn) forever. A scenario runner that can hang has no teardown at all: the
/// container, the record and the snapshot all outlive it.
///
/// The test bounds the wait itself so a hang cannot take the suite with it. The
/// pin is that the COMMAND terminates on its own, with a diagnosis, and still
/// tears down; a process this test had to kill is the red.
///
/// *Deletion checks*: delete it and a stalled runner wedges the command
/// indefinitely with no gate noticing. Delete the deadline and the process must
/// be killed, so it fails. Rename internals and it survives -- it observes the
/// process and the daemon log.
#[test]
fn a_runner_that_stalls_mid_probe_terminates_the_run_instead_of_hanging() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_STALL);

    let out_path = rig.dir.path().join("stall.out");
    let err_path = rig.dir.path().join("stall.err");
    let mut child = rig
        .command(&[])
        .stdout(Stdio::from(
            fs::File::create(&out_path).expect("create stdout capture"),
        ))
        .stderr(Stdio::from(
            fs::File::create(&err_path).expect("create stderr capture"),
        ))
        .spawn()
        .expect("spawn curie scenario");

    let deadline = Instant::now() + Duration::from_secs(45);
    let status = loop {
        match child.try_wait().expect("poll the scenario process") {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            None => thread::sleep(Duration::from_millis(250)),
        }
    };

    let captured = fs::read_to_string(&out_path).unwrap_or_default()
        + &fs::read_to_string(&err_path).unwrap_or_default();
    let status = status.unwrap_or_else(|| {
        panic!(
            "the command must impose its own deadline on a stalled runner; this one had to be \
             killed after 45s\n{captured}"
        )
    });
    assert!(
        !status.success(),
        "a runner that never answered cannot have produced a green run\n{captured}"
    );
    assert!(
        rig.docker_log().iter().any(|line| line.starts_with("rm ")),
        "the container booted before the stall must still be torn down: {:?}",
        rig.docker_log()
    );
}

// ---------------------------------------------------------------------------
// Finding 7 -- the canonical snapshot is wiped before the name-conflict check
// (cli/src/scenario.rs:705)
// ---------------------------------------------------------------------------

/// The canonical snapshot path is content-addressed, so a second run of the same
/// unchanged bundle resolves onto the very directory the FIRST run's container
/// has mounted read-only at `/plugin` -- and `bundle::snapshot` removes and
/// recreates an existing destination. The scenario packs before
/// `commands::start` checks the runner name, so the second run destroys the live
/// artifact and only then discovers it was never allowed to run.
///
/// *Deletion checks*: delete it and a concurrent scenario silently corrupts a
/// live runner's mounted bundle. Delete the ordering fix and the sentinel is
/// gone, so it fails. Rename internals and it survives -- it asserts on a file
/// inside the first run's snapshot.
#[test]
fn a_run_refused_on_a_taken_runner_name_leaves_the_holders_snapshot_intact() {
    let rig = Rig::unchanged("skill-two-probes.json");
    // Stand in for the run that already booted: its canonical snapshot is
    // materialized by the real packer and its container holds the runner name.
    let held = curie::bundle::snapshot(&rig.bundle).expect("pack the holder's snapshot");
    let sentinel = held.dir.join("held-by-the-live-runner");
    fs::write(&sentinel, b"mounted at /plugin").expect("mark the holder's snapshot");

    let output = rig.run(&[("CURIE_HARDEN_NAME_TAKEN", "1")]);

    assert!(
        !output.status.success(),
        "the second run must be refused\n{}",
        output_text(&output)
    );
    assert!(
        sentinel.exists(),
        "a run that is not even allowed to boot must not touch the snapshot another \
         runner has mounted at /plugin: {} was destroyed",
        sentinel.display()
    );
}

// ---------------------------------------------------------------------------
// Finding 8 -- two daemon reads keyed by a mutable name (cli/src/scenario.rs:829)
// ---------------------------------------------------------------------------

/// The mount read and the model read are two separate `docker inspect` calls
/// keyed by the container NAME, which a container can take over between them
/// (the same hazard `commands::stop_recorded` already answers by removing BY
/// ID). Keyed by name, the two observations can come from two different
/// containers and be reported as one runtime identity.
///
/// Here the name has been taken over by an impostor booted on the fake model
/// while the container this run actually started, `c0ffeec0ffee`, is live. Read
/// by id, both observations describe the run's own container and the live
/// manifest holds; read by name, the run mixes them.
///
/// *Deletion checks*: delete it and identity can be assembled from two
/// containers. Delete the identity read entirely and there is no observation at
/// all, so it fails. Rename internals and it survives -- what it pins is which
/// container the daemon was asked about.
#[test]
fn runtime_identity_is_read_from_one_immutable_container_id() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_SWAP_ON_NAME", "1")]);

    assert!(
        output.status.success(),
        "both observations belong to the container this run booted, which matches the \
         manifest\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    assert_eq!(
        value["tiers"][0]["identity_verified"],
        serde_json::json!(true),
        "{value}"
    );
    let inspects: Vec<String> = rig
        .docker_log()
        .into_iter()
        .filter(|line| line.starts_with("inspect "))
        .collect();
    assert!(
        inspects.len() >= 2 && inspects.iter().all(|line| line.ends_with("c0ffeec0ffee")),
        "every identity read must name the immutable container id, never the name a \
         second container can take over: {inspects:?}"
    );
}

// ---------------------------------------------------------------------------
// Re-review, blocking B -- teardown proves only that the NAME is free
// (cli/src/scenario.rs:1253)
// ---------------------------------------------------------------------------

/// The teardown postcondition is read with `docker ps --filter name=...`, which
/// answers "nothing holds this name". A container renamed out of the way keeps
/// running under the id the boot recorded while that probe reports it removed,
/// so the one assertion that exists to catch a stranded runner greens on one.
///
/// *Deletion checks*: delete it and a surviving runner is reported torn down.
/// Delete the id probe and the run goes green, so it fails. Rename internals and
/// it survives -- it asserts the emitted postcondition against a container the
/// daemon still lists by id.
#[test]
fn teardown_verifies_the_container_it_booted_by_id_not_only_by_name() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_RENAME_ON_RM", "1")]);

    let value = payload(&output);
    let teardown = &value["tiers"][0]["teardown"];
    assert_ne!(
        teardown["container_removed"],
        serde_json::json!(true),
        "the container this run booted is still running under its own id: {value}"
    );
    assert!(
        teardown["error"]
            .as_str()
            .unwrap_or_default()
            .contains("c0ffeec0ffee"),
        "the diagnosis must name the container that survived: {value}"
    );
    assert!(
        !output.status.success(),
        "a stranded runner must never be reported as a clean run\n{}",
        output_text(&output)
    );
}

// ---------------------------------------------------------------------------
// Re-review, major C -- a failed record read discards the real teardown report
// (cli/src/scenario.rs:946)
// ---------------------------------------------------------------------------

/// When the record the boot wrote cannot be read back, the run DOES tear down --
/// and then throws that report away, emitting the never-attempted placeholder.
/// A consumer reading `attempted: false` believes nothing was cleaned up and
/// nothing was leaked, on the one path where both are actually in question.
///
/// *Deletion checks*: delete it and the report on that path is fiction. Discard
/// the teardown report again and `attempted` is false, so it fails. Rename
/// internals and it survives -- the emitted teardown block is the contract.
#[test]
fn a_boot_whose_record_cannot_be_read_still_reports_the_teardown_it_ran() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let corrupt = rig.state_file().display().to_string();
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_CORRUPT_STATE", &corrupt)]);

    assert!(
        !output.status.success(),
        "a record that cannot be read is a failed run\n{}",
        output_text(&output)
    );
    let value = payload(&output);
    let teardown = &value["tiers"][0]["teardown"];
    assert_eq!(
        teardown["attempted"],
        serde_json::json!(true),
        "teardown ran, so the report must be the one it produced: {value}"
    );
    assert!(
        !teardown["error"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .is_empty(),
        "the leftovers that teardown could not clear must surface: {value}"
    );
}

/// Teardown releases the snapshot only through the runner record, so the one
/// path where that record cannot be read is the path where the snapshot this
/// run packed is never removed. The snapshot is RUN-UNIQUE, so no later run
/// reuses that directory and every such failure strands one more of them under
/// `.curie/snapshots/` forever.
///
/// *Deletion checks*: delete it and the leak is invisible -- the run already
/// reports a teardown error for the container. Delete the record-independent
/// release and a `sweep-*` directory survives, so it fails. Rename internals and
/// it survives -- it asserts on the on-disk artifact, not on a call.
#[test]
fn a_boot_whose_record_cannot_be_read_still_releases_the_snapshot_it_packed() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let corrupt = rig.state_file().display().to_string();
    let _run = lease(green_turns(), MODE_NORMAL);
    let output = rig.run(&[("CURIE_HARDEN_CORRUPT_STATE", &corrupt)]);

    assert!(
        !output.status.success(),
        "a record that cannot be read is a failed run\n{}",
        output_text(&output)
    );
    assert_eq!(
        rig.snapshot_entries(),
        Vec::<String>::new(),
        "the run knows the directory it packed, so an unreadable record must not \
         strand it: {:?}\n{}",
        rig.snapshot_entries(),
        output_text(&output)
    );
}

// ---------------------------------------------------------------------------
// Re-review, major D -- a deadline expiry is reported as exit 1
// (cli/src/scenario.rs:893)
// ---------------------------------------------------------------------------

/// A runner that accepted the connection and never answered is retryable: the
/// same argv may well succeed once it does. ADR-0021 reserves exit 3 for exactly
/// that, and reporting it as exit 1 tells an agent the run failed for a reason
/// re-running cannot change.
///
/// *Deletion checks*: delete it and a timeout is indistinguishable from a red
/// probe. Delete the transient class and the exit code is 1, so it fails. Rename
/// internals and it survives -- it asserts a process exit code.
#[test]
fn a_deadline_expiry_exits_transient_rather_than_failure() {
    let rig = Rig::unchanged("skill-two-probes.json");
    let _run = lease(green_turns(), MODE_STALL);

    let out_path = rig.dir.path().join("deadline.out");
    let err_path = rig.dir.path().join("deadline.err");
    let mut child = rig
        .command(&[])
        .stdout(Stdio::from(
            fs::File::create(&out_path).expect("create stdout capture"),
        ))
        .stderr(Stdio::from(
            fs::File::create(&err_path).expect("create stderr capture"),
        ))
        .spawn()
        .expect("spawn curie scenario");

    let deadline = Instant::now() + Duration::from_secs(45);
    let status = loop {
        match child.try_wait().expect("poll the scenario process") {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            None => thread::sleep(Duration::from_millis(250)),
        }
    };

    let captured = fs::read_to_string(&out_path).unwrap_or_default()
        + &fs::read_to_string(&err_path).unwrap_or_default();
    let status =
        status.unwrap_or_else(|| panic!("the command must impose its own deadline\n{captured}"));
    assert_eq!(
        status.code(),
        Some(3),
        "a deadline expiry is Transient (ADR-0021 exit 3), not a Failure\n{captured}"
    );
}
