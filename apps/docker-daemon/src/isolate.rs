//! `curie-docker-authz cgroup-isolate -- CMD [ARGS...]`: run CMD in a private
//! cgroup namespace with cgroup2 remounted on /sys/fs/cgroup.
//!
//! containerd gives a privileged container no private cgroup namespace (a
//! Kubernetes privileged pod, or `docker run --cgroupns=host`), so dockerd
//! would see the node's whole cgroup tree. A cgroup parent chosen through
//! BuildKit's `/grpc`, which the authorization plugin cannot read, would then
//! resolve against the node's hierarchy and escape the pod's limits. Here the
//! process unshares its cgroup and mount namespaces, remounts cgroup2 so the
//! namespace root (its current cgroup) is the top of /sys/fs/cgroup, checks
//! /proc/self/cgroup is `0::/`, and only then execs CMD.

use std::ffi::{c_char, c_int, c_ulong, c_void, CStr};
use std::io;
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitCode};

const USAGE: &str = "usage: curie-docker-authz cgroup-isolate -- CMD [ARGS...]";

const CGROUP_DIR: &CStr = c"/sys/fs/cgroup";

// linux/sched.h
const CLONE_NEWNS: c_int = 0x0002_0000;
const CLONE_NEWCGROUP: c_int = 0x0200_0000;
// linux/mount.h
const MS_NOSUID: c_ulong = 2;
const MS_NODEV: c_ulong = 4;
const MS_NOEXEC: c_ulong = 8;
const MS_REC: c_ulong = 0x4000;
const MS_PRIVATE: c_ulong = 1 << 18;
const MS_RELATIME: c_ulong = 1 << 21;
const MNT_DETACH: c_int = 2;

// std already links libc on both gnu and musl targets.
extern "C" {
    fn unshare(flags: c_int) -> c_int;
    fn mount(
        source: *const c_char,
        target: *const c_char,
        fstype: *const c_char,
        flags: c_ulong,
        data: *const c_void,
    ) -> c_int;
    fn umount2(target: *const c_char, flags: c_int) -> c_int;
}

/// `args` are the arguments after `cgroup-isolate`.
pub fn run(args: &[String]) -> ExitCode {
    let cmd = match args.split_first() {
        Some((sep, cmd)) if sep == "--" && !cmd.is_empty() => cmd,
        _ => {
            eprintln!("curie-authz: cgroup-isolate needs `--` and a command\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    if let Err(e) = isolate() {
        eprintln!("curie-authz: cgroup-isolate: {e}");
        return ExitCode::FAILURE;
    }
    let err = Command::new(&cmd[0]).args(&cmd[1..]).exec();
    eprintln!("curie-authz: cgroup-isolate: exec {:?}: {err}", cmd[0]);
    ExitCode::FAILURE
}

fn isolate() -> Result<(), String> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")
        .map_err(|e| format!("read /proc/self/mountinfo: {e}"))?;
    if cgroup_fstype(&mountinfo) != Some("cgroup2") {
        return Err("refusing to start: /sys/fs/cgroup is not a cgroup2 mount".into());
    }
    check("unshare(CLONE_NEWCGROUP|CLONE_NEWNS)", unsafe {
        unshare(CLONE_NEWCGROUP | CLONE_NEWNS)
    })?;
    // Keep the remount below from propagating back to the parent namespace.
    check("make / rprivate", unsafe {
        mount(
            std::ptr::null(),
            c"/".as_ptr(),
            std::ptr::null(),
            MS_REC | MS_PRIVATE,
            std::ptr::null(),
        )
    })?;
    check("umount /sys/fs/cgroup", unsafe {
        umount2(CGROUP_DIR.as_ptr(), MNT_DETACH)
    })?;
    check("mount cgroup2 on /sys/fs/cgroup", unsafe {
        mount(
            c"cgroup2".as_ptr(),
            CGROUP_DIR.as_ptr(),
            c"cgroup2".as_ptr(),
            MS_NOSUID | MS_NODEV | MS_NOEXEC | MS_RELATIME,
            std::ptr::null(),
        )
    })?;
    let own = std::fs::read_to_string("/proc/self/cgroup")
        .map_err(|e| format!("read /proc/self/cgroup: {e}"))?;
    if own.trim() != "0::/" {
        return Err(format!(
            "refusing to start: /proc/self/cgroup is {:?}, not \"0::/\" after unsharing",
            own.trim()
        ));
    }
    Ok(())
}

fn check(what: &str, rc: c_int) -> Result<(), String> {
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("{what}: {}", io::Error::last_os_error()))
    }
}

/// Filesystem type of the visible (last listed) mount at /sys/fs/cgroup.
/// mountinfo lines: `id parent maj:min root mountpoint opts [optional...] -
/// fstype source superopts`.
fn cgroup_fstype(mountinfo: &str) -> Option<&str> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let mut fields = line.split(' ');
            let mount_point = fields.nth(4)?;
            let fstype = fields.skip_while(|f| *f != "-").nth(1)?;
            (mount_point == "/sys/fs/cgroup").then_some(fstype)
        })
        .next_back()
}

#[cfg(test)]
mod tests {
    use super::cgroup_fstype;

    #[test]
    fn finds_the_visible_cgroup_mount() {
        let v2 = "1 0 0:1 / / rw - overlay overlay rw\n\
                  30 1 0:26 / /sys/fs/cgroup rw,nosuid shared:4 - cgroup2 cgroup2 rw\n";
        assert_eq!(cgroup_fstype(v2), Some("cgroup2"));
        let v1 = "30 1 0:26 / /sys/fs/cgroup ro - tmpfs tmpfs ro\n\
                  31 30 0:27 / /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu\n";
        assert_eq!(cgroup_fstype(v1), Some("tmpfs"));
        let over = format!("{v2}40 30 0:30 / /sys/fs/cgroup rw - tmpfs tmpfs rw\n");
        assert_eq!(cgroup_fstype(&over), Some("tmpfs"));
        assert_eq!(cgroup_fstype("1 0 0:1 / / rw - overlay overlay rw\n"), None);
    }
}
