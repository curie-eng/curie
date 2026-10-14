"""Probe a repository runner image's tools and write its tool manifest (ADR 0216).

Run by `curie build --repo-runner` inside the composed image, as that image's own
user. Usage: curie-probe.py PROBES_JSON OUTPUT_PATH. Inputs come only from those
two arguments and the environment is ignored, so an operator Dockerfile `ENV`
cannot change what is probed. PROBES_JSON is {"docker_cli": bool, "probes":
[{name, command, source}]}. Every probe runs; any failure is printed naming the
probe, the script exits 1 and writes nothing, which fails the image build.
"""

import json
import os
import re
import shutil
import subprocess
import sys

VERSION = re.compile(r"\d+(?:\.\d+)+")
TIMEOUT_SECONDS = 120


def resolve(argv0):
    if "/" in argv0:
        if os.path.isfile(argv0) and os.access(argv0, os.X_OK):
            return os.path.abspath(argv0)
        return None
    found = shutil.which(argv0)
    return os.path.abspath(found) if found else None


def version_of(stdout, stderr):
    for text in (stdout, stderr):
        match = VERSION.search(text)
        if match:
            return match.group(0)
    for line in (stdout + "\n" + stderr).splitlines():
        if line.strip():
            return line.strip()
    return None


def last_line(text):
    for line in reversed(text.splitlines()):
        if line.strip():
            return line.strip()
    return None


def probe(run):
    argv = run["command"]
    path = resolve(argv[0])
    if path is None:
        return None, f"{argv[0]} is not on PATH"
    try:
        done = subprocess.run(
            [path, *argv[1:]],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return None, f"timed out after {TIMEOUT_SECONDS} s"
    except OSError as err:
        return None, f"could not run {path}: {err}"
    stdout = done.stdout.decode("utf-8", errors="replace")
    stderr = done.stderr.decode("utf-8", errors="replace")
    if done.returncode != 0:
        reason = f"exit status {done.returncode}"
        last = last_line(stderr) or last_line(stdout)
        if last:
            reason += f": {last[:200]}"
        return None, reason
    version = version_of(stdout, stderr)
    if version is None:
        return None, "printed no output"
    return {"name": run["name"], "version": version, "path": path, "source": run["source"]}, None


def main():
    if len(sys.argv) != 3:
        print("usage: curie-probe.py PROBES_JSON OUTPUT_PATH", file=sys.stderr)
        return 2
    try:
        with open(sys.argv[1], encoding="utf-8") as handle:
            spec = json.load(handle)
        runs = spec["probes"]
        docker_cli = spec["docker_cli"] is True
    except (OSError, ValueError, KeyError, TypeError) as err:
        print(f"repo runner: cannot read probes file {sys.argv[1]}: {err}", file=sys.stderr)
        return 1
    out = sys.argv[2]
    tools = []
    failed = False
    for run in runs:
        tool, reason = probe(run)
        if tool is None:
            failed = True
            joined = " ".join(run["command"])
            print(f"repo runner: probe {run['name']} ({joined}) failed: {reason}", file=sys.stderr)
            continue
        tools.append(tool)
    if failed:
        return 1
    manifest = {"schema_version": 1, "tools": tools, "docker_cli": docker_cli}
    with open(out, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
