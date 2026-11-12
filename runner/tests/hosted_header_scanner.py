"""One containment scanner for hosted connector header values (#5378).

The channel tests copy this file into a bundle and run it from native Bash, a
bundle command hook, a declared stdio MCP server and, as the negative control,
a plain process over a deliberately exposed fixture. It is stdlib only and runs
under ``python -I``.

The spec names the protected values reversed, so the spec file itself never
carries one. A report names where a value was seen (a category and a path or
pid), never the value, and counts what each category inspected so a scan that
looked at nothing cannot pass for a clean one.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import stat
import sys
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

CATEGORIES = ("file", "symlink", "hardlink", "argv", "fd", "env", "proc")
_READ_LIMIT = 1024 * 1024
_TREE_BUDGET = 3000
_TEMP_BUDGET = 500
# Temp directories the runner, the CLI and this scan create.
_TEMP_PREFIXES = ("curie", "claude")


def _forms(values: Iterable[str]) -> tuple[bytes, ...]:
    """Each value and its base64 forms, as the runner's redaction matches them."""

    forms: set[str] = set()
    for value in values:
        forms.add(value)
        for encode in (base64.standard_b64encode, base64.urlsafe_b64encode):
            encoded = encode(value.encode()).decode()
            forms.update(form for form in (encoded, encoded.rstrip("=")) if len(form) >= 8)
    return tuple(form.encode() for form in forms if form)


class Scan:
    def __init__(self, spec: dict[str, Any]) -> None:
        self.spec = spec
        self.needles = _forms(value[::-1] for value in spec["reversed"])
        self.found: list[str] = []
        self.counts = dict.fromkeys(CATEGORIES, 0)
        self.denied = dict.fromkeys(CATEGORIES, 0)

    def check(self, category: str, where: str, data: bytes) -> None:
        if any(needle in data for needle in self.needles):
            self.found.append(f"{category}:{where}")

    def refused(self, category: str) -> None:
        self.counts[category] += 1
        self.denied[category] += 1

    def read(self, category: str, where: str, path: str | Path, *, follow: bool = True) -> bytes:
        """Read a regular file, never a FIFO, socket or device another process owns."""

        flags = os.O_RDONLY | os.O_NONBLOCK | (0 if follow else os.O_NOFOLLOW)
        try:
            fd = os.open(path, flags)
        except OSError:
            self.refused(category)
            return b""
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                return b""
            chunks: list[bytes] = []
            total = 0
            while total < _READ_LIMIT:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
        except OSError:
            self.refused(category)
            return b""
        finally:
            os.close(fd)
        data = b"".join(chunks)
        self.counts[category] += 1
        self.check(category, where, data)
        return data

    # --- files ------------------------------------------------------------------

    def files(self) -> None:
        tempdir = Path(self.spec["tempdir"])
        named_trees: list[tuple[float, Path]] = []
        named: list[Path] = []
        other: list[Path] = []
        for entry in _entries(tempdir):
            try:
                status = entry.lstat()
            except OSError:
                continue
            if stat.S_ISDIR(status.st_mode) and entry.name.startswith(_TEMP_PREFIXES):
                named_trees.append((status.st_mtime, entry))
            elif stat.S_ISREG(status.st_mode):
                (named if entry.name.startswith(_TEMP_PREFIXES) else other).append(entry)
        for path in named:
            self.read("file", str(path), path, follow=False)
        # The spec's roots first, then this run's temp trees: newest before stale.
        trees = [Path(root) for root in self.spec["roots"]]
        trees += [path for _mtime, path in sorted(named_trees, reverse=True)]
        budget = _TREE_BUDGET
        for path in _regular_files(trees):
            if budget <= 0:
                break
            budget -= 1
            self.read("file", str(path), path, follow=False)
        for path in other[:_TEMP_BUDGET]:
            self.read("file", str(path), path, follow=False)

    # --- aliases ------------------------------------------------------------------

    def aliases(self, public: bytes) -> tuple[bytes, bytes]:
        tempdir = Path(self.spec["tempdir"])
        alias_dir = Path(tempfile.mkdtemp(prefix="curie-relay-scan-", dir=tempdir))
        try:
            targets = {"public": str(self.spec["public"]), "self-environ": "/proc/self/environ"}
            configs = sorted(str(path) for path in tempdir.glob("curie-mcp-config-*"))
            if self.spec.get("live_config"):
                configs.append(str(self.spec["live_config"]))
            for index, config in enumerate(configs):
                targets[f"config-{index}"] = config
            cli = self.spec.get("cli_pid")
            if cli:
                targets["cli-environ"] = f"/proc/{cli}/environ"
                targets["cli-cmdline"] = f"/proc/{cli}/cmdline"
            public_symlink = b""
            for name, target in targets.items():
                link = alias_dir / f"{name}.symlink"
                os.symlink(target, link)
                data = self.read("symlink", name, link)
                if name == "public":
                    public_symlink = data
            source = alias_dir / "public.source"
            source.write_bytes(public)
            linkable = {"public": str(source)}
            linkable.update(
                (name, target)
                for name, target in targets.items()
                if name.startswith(("config-", "cli-"))
            )
            public_hardlink = b""
            for name, target in linkable.items():
                link = alias_dir / f"{name}.hardlink"
                try:
                    os.link(target, link)
                except OSError:
                    # /proc is another filesystem (EXDEV); a refusal still counts.
                    self.refused("hardlink")
                    continue
                data = self.read("hardlink", name, link)
                if name == "public":
                    public_hardlink = data
            return public_symlink, public_hardlink
        finally:
            shutil.rmtree(alias_dir, ignore_errors=True)

    # --- processes ------------------------------------------------------------------

    def processes(self) -> None:
        for entry in _entries(Path("/proc")):
            if entry.name.isdigit():
                self.read("argv", entry.name, entry / "cmdline")
                self.read("env", entry.name, entry / "environ")
        own = b"\0".join(key + b"=" + value for key, value in os.environb.items())
        self.counts["env"] += 1
        self.check("env", "os.environ", own)

    def descriptors(self) -> None:
        """Every inherited descriptor: regular files by offset, sockets by peek.

        Pipes are counted but never read: a read would take bytes from the
        process that owns the other end (stdin and stdout are JSON-RPC here).
        """

        for name in _names("/proc/self/fd"):
            fd = int(name)
            try:
                mode = os.fstat(fd).st_mode
            except OSError:
                continue
            self.counts["fd"] += 1
            data = b""
            if stat.S_ISREG(mode):
                try:
                    data = os.pread(fd, _READ_LIMIT, 0)
                except OSError:
                    self.denied["fd"] += 1
            elif stat.S_ISSOCK(mode):
                try:
                    peer = socket.socket(fileno=os.dup(fd))
                except (OSError, ValueError):
                    self.denied["fd"] += 1
                    continue
                try:
                    data = peer.recv(65536, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                except OSError:
                    data = b""
                finally:
                    peer.close()
            self.check("fd", str(fd), data)

    def proc(self) -> None:
        paths = ["/proc/self/environ", "/proc/self/cmdline"]
        cli = self.spec.get("cli_pid")
        if cli:
            paths += [
                f"/proc/{cli}/environ",
                f"/proc/{cli}/cmdline",
                f"/proc/{cli}/root/proc/{cli}/environ",
            ]
            try:
                descriptors = os.listdir(f"/proc/{cli}/fd")
            except OSError:
                descriptors = []
                self.refused("proc")
            for name in descriptors:
                path = f"/proc/{cli}/fd/{name}"
                try:
                    regular = stat.S_ISREG(os.stat(path).st_mode)
                except OSError:
                    self.refused("proc")
                    continue
                if regular:
                    paths.append(path)
        for path in paths:
            self.read("proc", path, path)

    def live_config(self) -> dict[str, Any] | None:
        """The shape of the CLI's MCP entry for the connector, from its config file."""

        path = self.spec.get("live_config")
        if not path:
            return None
        try:
            entry = json.loads(Path(path).read_text(encoding="utf-8"))["mcpServers"][
                self.spec["server"]
            ]
        except (OSError, ValueError, KeyError, TypeError):
            return None
        if not isinstance(entry, dict):
            return None
        return {"type": entry.get("type"), "keys": sorted(entry)}


def _entries(directory: Path) -> list[Path]:
    try:
        return list(directory.iterdir())
    except OSError:
        return []


def _names(directory: str) -> list[str]:
    try:
        return os.listdir(directory)
    except OSError:
        return []


def _regular_files(trees: Iterable[Path]) -> Iterator[Path]:
    for tree in trees:
        for directory, _dirnames, filenames in os.walk(tree):
            for name in filenames:
                path = Path(directory, name)
                try:
                    regular = stat.S_ISREG(path.lstat().st_mode)
                except OSError:
                    continue
                if regular:
                    yield path


def run(spec_path: str | Path, label: str) -> dict[str, Any]:
    spec = json.loads(Path(spec_path).read_text(encoding="utf-8"))
    scan = Scan(spec)
    public = Path(spec["public"]).read_bytes()
    scan.files()
    public_symlink, public_hardlink = scan.aliases(public)
    scan.processes()
    scan.descriptors()
    scan.proc()
    report = {
        "label": label,
        "public": public.decode("utf-8", "replace"),
        "public_symlink": public_symlink.decode("utf-8", "replace"),
        "public_hardlink": public_hardlink.decode("utf-8", "replace"),
        "found": scan.found,
        "counts": scan.counts,
        "denied": scan.denied,
        "live_config": scan.live_config(),
    }
    reports = Path(spec["reports"])
    partial = reports / f"{label}-{os.getpid()}.partial"
    partial.write_text(json.dumps(report), encoding="utf-8")
    partial.replace(reports / f"{label}-{os.getpid()}.json")
    return report


if __name__ == "__main__":
    finished = run(sys.argv[1], sys.argv[2])
    sys.stdout.write(f"scan complete {finished['public']}\n")
