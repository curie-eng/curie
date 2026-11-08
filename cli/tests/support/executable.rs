//! Install test executables without exposing a writable executable file to forked children.
#![allow(dead_code)]

use std::ffi::OsString;
use std::io::Write;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Replace `path` atomically, including when a test rewrites a shim in place.
pub fn install(path: &Path, body: &str) {
    let parent = path
        .parent()
        .expect("test executable has a parent directory");
    let mut staging =
        tempfile::NamedTempFile::new_in(parent).expect("create executable staging file");
    staging
        .write_all(body.as_bytes())
        .expect("write executable staging file");
    staging
        .as_file()
        .sync_all()
        .expect("sync executable staging file");

    // Closing the write descriptor before setting the execute bit is essential:
    // another test thread may fork at any point after this line. Sync first so
    // the bytes are durable when the descriptor goes away.
    let staging = staging.into_temp_path();
    std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o755))
        .expect("make staged executable runnable");
    staging.persist(path).expect("install test executable");
}

/// Install `name` in `dir`.
pub fn install_in(dir: &Path, name: &str, body: &str) {
    install(&dir.join(name), body);
}

/// Python interpreter on this process's PATH.
pub fn caller_python() -> PathBuf {
    let output = Command::new("python3")
        .args(["-c", "import sys; print(sys.executable, end='')"])
        .output()
        .expect("resolve python3 on PATH");
    assert!(
        output.status.success(),
        "resolve python3 on PATH: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    PathBuf::from(String::from_utf8(output.stdout).expect("python path is utf-8"))
}

/// Directory owned by `owner` whose `python3` entry is [`caller_python`].
///
/// Fixture programs use `#!/usr/bin/env python3`. A PATH that hides the caller
/// PATH still resolves `python3` from this directory.
pub fn caller_python_dir(owner: &Path) -> PathBuf {
    let dir = owner.join("caller-python");
    std::fs::create_dir_all(&dir).expect("create caller python directory");
    let link = dir.join("python3");
    if link.symlink_metadata().is_err() {
        symlink(caller_python(), &link).expect("link caller python");
    }
    dir
}

/// `front`, then the caller Python directory.
pub fn fixture_python_path(front: &Path) -> OsString {
    let mut path = OsString::from(front.as_os_str());
    path.push(":");
    path.push(caller_python_dir(front));
    path
}

/// `front`, the caller Python directory, then `/usr/bin` and `/bin`.
pub fn isolated_fixture_path(front: &Path) -> OsString {
    let mut path = fixture_python_path(front);
    path.push(":/usr/bin:/bin");
    path
}
