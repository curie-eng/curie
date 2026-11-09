"""Run the pinned Git-history scanner on any caller-selected Docker daemon.

Only objects reachable from the two resolved commits and committed scanner
configuration cross the Docker API. Host paths and credential configuration
are never mounted or copied. Shallow boundaries retain the caller's graph.
"""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import BinaryIO

LABEL = "io.curie.preflight.scan"


class ScanFailure(RuntimeError):
    pass


def checked(
    root: Path,
    arguments: list[str],
    *,
    data: bytes | None = None,
    output: BinaryIO | None = None,
) -> bytes:
    result = subprocess.run(
        arguments,
        cwd=root,
        input=data,
        stdout=output if output is not None else subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise ScanFailure(f"scanner transport: {arguments[0]} {arguments[1]} failed")
    return result.stdout if output is None else b""


def git(root: Path, *arguments: str) -> str:
    return checked(root, ["git", *arguments]).decode().strip()


def config_file(root: Path, head: str, name: str, destination: Path, *, required: bool) -> None:
    entry = git(root, "ls-tree", head, "--", name)
    if not entry and not required:
        return
    if not entry.startswith(("100644 blob ", "100755 blob ")):
        raise ScanFailure(f"scanner transport: {name} must be a committed regular file")
    destination.write_bytes(checked(root, ["git", "show", f"{head}:{name}"]))


def snapshot(root: Path, destination: Path, *, base: str, head: str) -> tuple[str, str]:
    base_sha = git(root, "rev-parse", "--verify", "--end-of-options", f"origin/{base}^{{commit}}")
    head_sha = git(root, "rev-parse", "--verify", "--end-of-options", f"{head}^{{commit}}")
    object_format = git(root, "rev-parse", "--show-object-format")
    git(
        destination,
        "init",
        "-q",
        "--template=",
        "-b",
        "task/scanner",
        f"--object-format={object_format}",
    )
    objects = destination / ".git/objects/pack"
    objects.mkdir(parents=True, exist_ok=True)
    pack = objects / "snapshot.pack"
    with pack.open("wb") as output:
        checked(
            root,
            ["git", "pack-objects", "--stdout", "--revs", "--threads=1", "--window-memory=16m"],
            data=f"{head_sha}\n{base_sha}\n".encode(),
            output=output,
        )
    shallow = Path(git(root, "rev-parse", "--path-format=absolute", "--git-path", "shallow"))
    if shallow.exists():
        shutil.copyfile(shallow, destination / ".git/shallow")
    git(destination, "index-pack", str(pack))
    git(destination, "update-ref", "HEAD", head_sha)
    git(destination, "fsck", "--connectivity-only", "--no-dangling", head_sha, base_sha)
    config_file(root, head_sha, ".gitleaks.toml", destination / ".gitleaks.toml", required=True)
    config_file(root, head_sha, ".gitleaksignore", destination / ".gitleaksignore", required=False)
    return base_sha, head_sha


def owned(root: Path, name: str, owner: str) -> list[str]:
    return (
        checked(
            root,
            [
                "docker",
                "ps",
                "-aq",
                "--filter",
                f"name=^/{name}$",
                "--filter",
                f"label={LABEL}={owner}",
            ],
        )
        .decode()
        .split()
    )


def detect_arguments(base_sha: str, head_sha: str) -> list[str]:
    return [
        "detect",
        "--source",
        "/repo",
        "--config",
        "/repo/.gitleaks.toml",
        "--redact",
        "--verbose",
        "--exit-code",
        "1",
        "--report-path",
        "/repo/gitleaks-report.json",
        f"--log-opts={base_sha}..{head_sha}",
    ]


def scan(root: Path, *, base: str, head: str, image: str, owner: str) -> int:
    directory = root / ".projects/preflight"
    directory.mkdir(parents=True, exist_ok=True)
    name = "curie-history-scan-" + uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix="scan-", dir=directory) as temporary:
        candidate = Path(temporary) / "candidate"
        candidate.mkdir()
        base_sha, head_sha = snapshot(root, candidate, base=base, head=head)
        try:
            checked(
                root,
                [
                    "docker",
                    "create",
                    "--name",
                    name,
                    "--network",
                    "none",
                    "--label",
                    f"{LABEL}={owner}",
                    image,
                    *detect_arguments(base_sha, head_sha),
                ],
            )
            checked(root, ["docker", "cp", str(candidate) + "/.", name + ":/repo"])
            attached = subprocess.run(
                ["docker", "start", "--attach", name],
                cwd=root,
                capture_output=True,
                check=False,
            )
            state = json.loads(
                checked(root, ["docker", "inspect", "--format", "{{json .State}}", name])
            )
            if state["Status"] != "exited" or state["OOMKilled"]:
                raise ScanFailure("scanner transport: scanner did not finish normally")
            code = int(state["ExitCode"])
            if attached.returncode not in (0, code):
                raise ScanFailure("scanner transport: attached output was interrupted")
            sys.stdout.buffer.write(attached.stdout)
            sys.stderr.buffer.write(attached.stderr)
            report = Path(temporary) / "gitleaks-report.json"
            checked(root, ["docker", "cp", name + ":/repo/gitleaks-report.json", str(report)])
            findings = json.loads(report.read_text())
            if not isinstance(findings, list) or (code == 0 and findings):
                raise ScanFailure("scanner transport: inconsistent scanner report")
            report.chmod(0o600)
            report.replace(root / "gitleaks-report.json")
            return code
        finally:
            containers = owned(root, name, owner)
            if containers:
                checked(root, ["docker", "rm", "--force", "--volumes", *containers])
            if owned(root, name, owner):
                raise ScanFailure("scanner transport: owned container remains")


def interrupted(_signum: int, _frame: object) -> None:
    raise KeyboardInterrupt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--image", required=True)
    parser.add_argument("--owner", default=uuid.uuid4().hex)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, interrupted)
    try:
        return scan(Path.cwd(), base=args.base, head=args.head, image=args.image, owner=args.owner)
    except (ScanFailure, OSError, ValueError) as error:
        print(f"pre-push scanner failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("pre-push scanner interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
