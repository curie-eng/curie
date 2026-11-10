//! Argument handling of the built `curie-docker-authz` binary.
//!
//! `curie-docker-authz cgroup-isolate -- CMD [ARGS...]` unshares the cgroup
//! and mount namespaces, remounts cgroup2 on /sys/fs/cgroup, verifies
//! /proc/self/cgroup is `0::/`, then execs CMD. That path needs root, so it is
//! covered by tests/e2e.sh; these cases pin only the refusals that happen
//! before any privileged step. Each one passes a command that would leave a
//! marker file behind if it ran, and asserts the marker is absent.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_curie-docker-authz");

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("curie-authz-cli-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    fn marker(&self) -> PathBuf {
        self.0.join("ran")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .output()
        .expect("spawn curie-docker-authz")
}

fn text(o: &Output) -> String {
    format!(
        "status {:?}\nstdout: {}\nstderr: {}",
        o.status,
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

#[track_caller]
fn assert_usage_refusal(o: &Output, marker: &Path) {
    assert!(!o.status.success(), "expected nonzero exit; {}", text(o));
    let err = String::from_utf8_lossy(&o.stderr);
    // The usage text documents the subcommand form, not merely echoes the
    // rejected word back (`unknown argument "cgroup-isolate"` is not usage).
    assert!(
        err.contains("usage") && err.contains("cgroup-isolate --"),
        "expected a usage message showing `cgroup-isolate -- CMD` on stderr; {}",
        text(o)
    );
    assert!(
        !err.contains("unknown argument \"cgroup-isolate\""),
        "cgroup-isolate is not recognized as a subcommand; {}",
        text(o)
    );
    assert!(
        !marker.exists(),
        "the command ran despite the refusal; {}",
        text(o)
    );
}

#[test]
fn cgroup_isolate_without_separator_is_refused() {
    let s = Scratch::new("nosep");
    let m = s.marker();
    let m = m.to_str().unwrap();
    // The command is there but not behind `--`.
    let o = run(&["cgroup-isolate", "touch", m]);
    assert_usage_refusal(&o, s.marker().as_path());
    let o = run(&["cgroup-isolate", "sh", "-c", &format!("touch {m}")]);
    assert_usage_refusal(&o, s.marker().as_path());
}

#[test]
fn cgroup_isolate_without_command_is_refused() {
    let s = Scratch::new("nocmd");
    let o = run(&["cgroup-isolate"]);
    assert_usage_refusal(&o, s.marker().as_path());
    let o = run(&["cgroup-isolate", "--"]);
    assert_usage_refusal(&o, s.marker().as_path());
}

#[test]
fn unknown_subcommand_is_refused() {
    let s = Scratch::new("unknown");
    let m = s.marker();
    let m = m.to_str().unwrap();
    for args in [
        vec!["cgroup-isolat", "--", "touch", m],
        vec!["isolate", "--", "touch", m],
        vec!["bogus"],
    ] {
        let o = run(&args);
        assert!(
            !o.status.success(),
            "{args:?}: expected nonzero exit; {}",
            text(&o)
        );
        assert!(
            !o.stderr.is_empty(),
            "{args:?}: expected a message on stderr; {}",
            text(&o)
        );
        assert!(
            !s.marker().exists(),
            "{args:?}: the command ran; {}",
            text(&o)
        );
    }
}

#[test]
fn serve_mode_bad_argument_still_fails_with_usage() {
    // The existing serve CLI keeps refusing unknown flags.
    let o = run(&["--no-such-flag"]);
    assert!(!o.status.success(), "{}", text(&o));
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("usage"),
        "{}",
        text(&o)
    );
}
