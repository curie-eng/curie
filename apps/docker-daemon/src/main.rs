//! `curie-docker-authz [--socket PATH]`: serve the Curie authorization plugin.
//! `curie-docker-authz cgroup-isolate -- CMD [ARGS...]`: run CMD (dockerd) in a
//! private cgroup namespace; see `isolate.rs`.

mod isolate;

use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const DEFAULT_SOCKET: &str = "/run/docker/plugins/curie-authz.sock";
const USAGE: &str = "usage: curie-docker-authz [--socket PATH]\n       \
                     curie-docker-authz cgroup-isolate -- CMD [ARGS...]";

fn parse_args(args: Vec<String>) -> Result<PathBuf, String> {
    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = args.next().ok_or("--socket needs a PATH")?.into();
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            _ => match arg.strip_prefix("--socket=") {
                Some(p) => socket = p.into(),
                None => return Err(format!("unknown argument {arg:?}")),
            },
        }
    }
    if socket.as_os_str().is_empty() {
        return Err("--socket PATH is empty".into());
    }
    Ok(socket)
}

fn bind(path: &Path) -> Result<UnixListener, String> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() {
            return Err(format!("{} exists and is not a socket", path.display()));
        }
        std::fs::remove_file(path).map_err(|e| format!("remove stale {}: {e}", path.display()))?;
    }
    let listener = UnixListener::bind(path).map_err(|e| format!("bind {}: {e}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("chmod {}: {e}", path.display()))?;
    Ok(listener)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("cgroup-isolate") {
        return isolate::run(&args[1..]);
    }
    let socket = match parse_args(args) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("curie-authz: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let listener = match bind(&socket) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("curie-authz: {e}");
            return ExitCode::FAILURE;
        }
    };
    eprintln!("curie-authz listening on {}", socket.display());
    match curie_docker_authz::server::serve(listener) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("curie-authz: serve: {e}");
            ExitCode::FAILURE
        }
    }
}
