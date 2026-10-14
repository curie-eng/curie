//! Install test executables without exposing a writable executable file to forked children.
#![allow(dead_code)]

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

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
