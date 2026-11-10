//! Isolation of Valkey stream names minted by CLI integration tests (#2086).
//!
//! `unique_stream` must stay collision-resistant across parallel test threads
//! and across separate processes. Timestamp-only names failed that on hosted
//! runners and on a separately owned Valkey.

use crate::support;
use support::unique_stream;

use std::collections::HashSet;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;

const PREFIX: &str = "curie:test:ns:";
const CHILD_ENV: &str = "CURIE_UNIQUE_STREAM_CHILD_COUNT";

fn suffix(name: &str) -> &str {
    name.strip_prefix(PREFIX)
        .expect("unique_stream must keep the caller prefix")
}

#[test]
fn unique_stream_keeps_the_prefix() {
    let name = unique_stream(PREFIX);
    assert!(name.starts_with(PREFIX), "{name}");
    assert!(!suffix(&name).is_empty(), "{name}");
}

#[test]
fn unique_stream_suffix_is_not_timestamp_only() {
    let name = unique_stream(PREFIX);
    let suffix = suffix(&name);
    assert!(
        suffix.chars().any(|c| !c.is_ascii_digit()),
        "unique_stream must not be prefix plus SystemTime nanos; got {name}"
    );
}

#[test]
fn unique_stream_names_are_unique_across_parallel_threads() {
    const THREADS: usize = 64;
    const PER_THREAD: usize = 32;
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut joins = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let barrier = Arc::clone(&barrier);
        joins.push(thread::spawn(move || {
            barrier.wait();
            (0..PER_THREAD)
                .map(|_| unique_stream(PREFIX))
                .collect::<Vec<_>>()
        }));
    }
    let mut all = Vec::with_capacity(THREADS * PER_THREAD);
    for join in joins {
        all.extend(join.join().expect("unique_stream thread"));
    }
    let unique: HashSet<&String> = all.iter().collect();
    assert_eq!(
        unique.len(),
        all.len(),
        "parallel unique_stream calls collided"
    );
}

#[test]
fn unique_stream_names_are_unique_across_processes() {
    if let Ok(count) = std::env::var(CHILD_ENV) {
        let n: usize = count.parse().expect("child count");
        for _ in 0..n {
            println!("{}", unique_stream(PREFIX));
        }
        return;
    }

    const PROCESSES: usize = 4;
    const PER_PROCESS: usize = 32;
    let exe = std::env::current_exe().expect("current test binary");
    let mut children = Vec::with_capacity(PROCESSES);
    for _ in 0..PROCESSES {
        children.push(
            Command::new(&exe)
                .env(CHILD_ENV, PER_PROCESS.to_string())
                .args([
                    "unique_stream::unique_stream_names_are_unique_across_processes",
                    "--exact",
                    "--nocapture",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn unique_stream child"),
        );
    }

    let mut all = Vec::with_capacity(PROCESSES * PER_PROCESS);
    for child in children {
        let output = child
            .wait_with_output()
            .expect("wait for unique_stream child");
        assert!(
            output.status.success(),
            "unique_stream child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if let Some(rest) = line.strip_prefix(PREFIX) {
                if !rest.is_empty() {
                    all.push(format!("{PREFIX}{rest}"));
                }
            }
        }
    }
    assert_eq!(
        all.len(),
        PROCESSES * PER_PROCESS,
        "missing child unique_stream names: {all:?}"
    );
    let unique: HashSet<&String> = all.iter().collect();
    assert_eq!(
        unique.len(),
        all.len(),
        "cross-process unique_stream calls collided"
    );
}
