#!/usr/bin/python3
"""Drive two actual CLI processes across a single recorded upgrade target."""

import json
import os
import pathlib
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time

root = pathlib.Path(os.environ["UPGRADE_DRIVER_ROOT"])
argv = sys.argv[1:]
assert pathlib.Path(os.environ["TMPDIR"]) == root


def exited(fd):
    poller = select.poll()
    poller.register(fd, select.POLLIN)
    return bool(poller.poll(0))


def stop(fd):
    if fd is not None:
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        except ProcessLookupError:
            pass


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)
cli_fd = helm_fd = None
proof = {}
with (root / "first-stdout").open("w") as out, (root / "first-stderr").open("w") as err:
    cli = subprocess.Popen(argv, stdout=out, stderr=err)
    try:
        cli_fd = os.pidfd_open(cli.pid)
        deadline = time.monotonic() + 10
        while not (root / "helm-owner.json").exists():
            assert cli.poll() is None, "first upgrade ended before Helm"
            assert time.monotonic() < deadline, "first upgrade did not reach Helm"
            time.sleep(0.01)
        owner = json.loads((root / "helm-owner.json").read_text())
        assert owner["parent"] == cli.pid
        helm_fd = os.pidfd_open(owner["pid"])
        # flock(2) binds the inherited open description across fork/exec:
        # https://man7.org/linux/man-pages/man2/flock.2.html
        lock = next((pathlib.Path(os.environ["XDG_STATE_HOME"]) / "curie/upgrades").glob("*.lock"))
        expected_inode = (lock.stat().st_dev, lock.stat().st_ino)
        child_files = list(pathlib.Path(f"/proc/{owner['pid']}/fd").iterdir())
        proof["direct_child_holds_same_lock_inode"] = any(
            (item.stat().st_dev, item.stat().st_ino) == expected_inode for item in child_files
        )
        assert proof["direct_child_holds_same_lock_inode"], (
            "direct Helm did not inherit upgrade ownership"
        )
        before = (root / "record.json").read_bytes()
        env = dict(os.environ, UPGRADE_DRIVER_SCENARIO="healthy")
        overlap = subprocess.run(argv, env=env, capture_output=True, timeout=20)
        proof["overlap_exit"] = overlap.returncode
        assert overlap.returncode == 3, f"concurrent upgrade was not refused: {overlap.stdout!r}"
        payload = json.loads(overlap.stdout)
        assert payload["fix"], "refusal needs actionable recovery"
        alias = subprocess.run(
            argv,
            env=dict(env, UPGRADE_DRIVER_SERVER="https://alias.example.com"),
            capture_output=True,
            timeout=20,
        )
        proof["alias_overlap_exit"] = alias.returncode
        assert alias.returncode == 3, (
            "another server spelling bypassed same namespace UID ownership"
        )
        with tempfile.TemporaryDirectory(prefix="independent-target-", dir=root) as target:
            independent = pathlib.Path(target)
            for name in ["helm", "kubectl", "candidate-chart", "values.json"]:
                shutil.copy2(root / name, independent / name)
            independent_env = dict(
                env,
                UPGRADE_DRIVER_ROOT=str(independent),
                TMPDIR=str(independent),
                UPGRADE_DRIVER_NAMESPACE_UID="acme-other-namespace-uid",
                PATH=f"{independent}:/usr/bin:/bin",
            )
            independent_argv = [value.replace(str(root), str(independent)) for value in argv]
            other = subprocess.run(
                independent_argv, env=independent_env, capture_output=True, timeout=20
            )
            proof["independent_target_exit"] = other.returncode
            assert other.returncode == 0, f"independent target blocked: {other.stdout!r}"
        proof["checkpoint_unchanged"] = (root / "record.json").read_bytes() == before
        assert proof["checkpoint_unchanged"], "contender changed first owner's checkpoint"
        stop(cli_fd)
        cli.wait(timeout=5)
        deadline = time.monotonic() + 5
        while not exited(helm_fd) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert exited(helm_fd), "first owner's Helm survived"
        retry = subprocess.run(argv, env=env, capture_output=True, timeout=20)
        proof["retry_exit"] = retry.returncode
        assert retry.returncode == 0, f"same command could not reacquire: {retry.stdout!r}"
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        if cli_fd is None:
            cli.kill()
        stop(cli_fd)
        cli.wait(timeout=5)
        stop(helm_fd)
        if helm_fd is not None:
            deadline = time.monotonic() + 5
            while not exited(helm_fd) and time.monotonic() < deadline:
                time.sleep(0.01)
            assert exited(helm_fd), "owned Helm cleanup failed"
        for fd in (cli_fd, helm_fd):
            if fd is not None:
                os.close(fd)
        proof["cleanup_complete"] = True
print(json.dumps(proof))
