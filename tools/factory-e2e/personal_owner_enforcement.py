#!/usr/bin/env python3
"""Live proof driver for personal owner binding across claims, adoption and recovery (#5378).

Four subcommands share one private SQLite ledger under ``--state-dir``:

1. ``preflight`` (unpaid, read-only) probes every prerequisite and refuses with
   a nonzero exit when one is missing: readyz, namespace absence, RBAC,
   ``docker info``, the loopback registry, the input file, the grant file (by
   stat only), the grant expiry, a clean candidate checkout at the SHA, the
   Python environment, and an unused run ID.
2. ``rehearse`` (unpaid) runs every phase that starts no native model session
   against the real cluster, with the runner's offline fake model and a
   synthetic grant, then the full finalizer. Any native CLI process fences
   every start and fails the rehearsal.
3. ``run`` makes exactly one bounded live attempt under the approved ceiling.
4. ``judge --criterion X`` reads the ledger only and prints a sanitized verdict.

The candidate worker kernel authorizes the owner, reads custody and launches.
Every WorkItem runs with a real repository workspace cloned from the recorded
TLS GitHub stub, which runs in the owned namespace as tls_code_host runs it.
The driver admits through the API's trusted internal admission route, seeds a
marker record into the durable transcript, interrupts, and observes. ``rehearse``
and ``run`` re-execute this file as an ``env -i`` child whose environment is an
allowlist of names and paths; only that child opens the grant file. See the
README section "Personal owner enforcement proof".
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import http.client
import importlib.util
import json
import os
import secrets
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# factory_e2e registers the tls-code-host scenario at import time by loading
# tls_code_host itself, so it is imported before tls_code_host; the reverse
# order re-enters factory_e2e while tls_code_host is half initialized.
import factory_e2e  # noqa: E402, F401
import personal_owner_enforcement_support as poe  # noqa: E402
import personal_profile_support as pps  # noqa: E402
import tls_code_host as tch  # noqa: E402

REPO_ROOT = HERE.parents[1]
ENROLLMENT_SUPPORT = (
    REPO_ROOT / "apps" / "worker" / "tests" / "sandbox" / "personal_enrollment_cluster_support.py"
)
CHART_SANDBOX = REPO_ROOT / "charts" / "curie" / "templates" / "agent-sandbox.yaml"
SUBCOMMANDS = ("preflight", "rehearse", "run", "judge")
CHILD = "_child"
POLL_SECONDS = 1.0
HOLD_SECONDS = 160
WORK_DIR = "/tmp/proof-work"
REPOSITORY = "curie-runner-candidate"
SENTINEL = "sentinel"
SENTINEL_PORT = 8000
SENTINEL_SECRET_NAME = "acme-sentinel"
SENTINEL_ENV = "SENTINEL_TOKEN"
SENTINEL_SERVER_MAP = "acme-sentinel-server"
REQUIRED_MODULES = (
    "curie_api",
    "curie_worker",
    "curie_runner",
    "kubernetes",
    "sqlalchemy",
    "cryptography",
)
# Interruption pairs: kind and reply conversation. Each pair is its own
# WorkItem on its own stub issue, so its recovery is judged on its own thread.
PAIRS = (
    ("process", "1700000000.000311"),
    ("pod", "1700000000.000312"),
    ("worker", "1700000000.000313"),
)
OTHER_CONVERSATION = "1700000000.000310"
# One stub issue per WorkItem: the refused other principal, then each pair.
ISSUE_ROLES = ("other", "process", "pod", "worker")
CUSTODY_LABEL = "curietech.ai/personal-custody=true"
# The recorded TLS GitHub stub (tools/github-stub), as tls_code_host runs it.
STUB_PORT = tch.STUB_PORT_DEFAULT
STUB_OBJECTS = (
    f"pod/{tch.STUB_NAME}",
    f"service/{tch.STUB_NAME}",
    f"configmap/{tch.STUB_SOURCE_CONFIG_MAP}",
    f"configmap/{tch.CA_CONFIG_MAP}",
)
SEED_FILES = {
    "README.md": "# acme-bot\n\nExample repository for the owner enforcement proof.\n",
    "example.py": 'def example() -> str:\n    return "example"\n',
}
SEED_ISSUE = "Owner enforcement proof fixture. Read the brief and reply; change nothing.\n"
NONCE_FILE = ".proof-nonce"
# The signed workspace reference outlives one interruption pair, so a Pod the
# controller recreates can still stage the checkout (the chart default is 300).
REFERENCE_TTL_SECONDS = 900
# The finalizer runs these in this order. Each is registered before setup
# creates anything, in reverse, because the finalizer is LIFO.
TEARDOWN_ORDER = ("fence", "inventory", "proof-cleanup", "registry", "absence")

PAIR_OBJECTIVE = (
    "This is an automated platform check. Use the Read tool exactly once to read "
    "/bundle/custody/brief.txt, then reply with exactly the single line it contains. "
    "Use no other tool."
)
HOSTED_OBJECTIVE = (
    "This is an automated platform check. In one response, call exactly these three tools "
    "and no others: the Bash tool with the command `python3 /bundle/custody/scan.py`, the "
    "`challenge` tool of the sentinel connector, and the `public_file` tool. After their "
    "results arrive, reply with exactly the text the challenge tool returned."
)
REHEARSE_PAIR_OBJECTIVE = "[fake:progress-demo]"
REHEARSE_HOSTED_OBJECTIVE = "[fake:connector-call]"

# --- programs the bundle carries ---------------------------------------------

HOLD_SCRIPT = f"""\
import json, os, pathlib, sys, threading
mode = sys.argv[1] if len(sys.argv) > 1 else "read"
root = pathlib.Path({WORK_DIR!r})
root.mkdir(parents=True, exist_ok=True)
def stat(pid):
    return pathlib.Path(f"/proc/{{pid}}/stat").read_text().rsplit(")", 1)[1].split()
def cli():
    pid = os.getppid()
    for _ in range(8):
        try:
            argv = pathlib.Path(f"/proc/{{pid}}/cmdline").read_bytes().split(b"\\0")
            fields = stat(pid)
        except OSError:
            return None, None
        if any(b"claude" in part for part in argv[:2]):
            return pid, int(fields[19])
        pid = int(fields[1])
        if pid <= 1:
            return None, None
    return None, None
try:
    hook = json.load(sys.stdin)
except ValueError:
    hook = {{}}
pid, start = cli()
holding = root / f"holding-{{mode}}"
release = root / f"release-{{mode}}"
holding.write_text(json.dumps({{
    "cli_pid": pid, "cli_start": start, "session_id": hook.get("session_id"),
    "transcript_path": hook.get("transcript_path"), "hook_pid": os.getpid(),
}}))
pause = threading.Event()
waited = 0.0
while not release.exists() and waited < {HOLD_SECONDS}:
    pause.wait(0.5)
    waited += 0.5
holding.unlink(missing_ok=True)
release.unlink(missing_ok=True)
print("{{}}")
"""
SCAN_SCRIPT = f"""\
import os, pathlib, sys
out = bytearray()
sources = 0
def add(data):
    global sources
    out.extend(data)
    out.extend(b"\\n")
    sources += 1
for key, value in os.environ.items():
    add(f"{{key}}={{value}}".encode())
add("\\0".join(sys.argv).encode())
for proc in pathlib.Path("/proc").iterdir():
    if proc.name.isdigit():
        for leaf in ("cmdline", "environ"):
            try:
                add((proc / leaf).read_bytes())
            except OSError:
                pass
target = pathlib.Path({WORK_DIR!r}) / "scan.bin"
for top in ("/tmp", "/home/runner", "/bundle"):
    for base, _dirs, names in os.walk(top, onerror=lambda error: None):
        for name in names:
            path = pathlib.Path(base) / name
            try:
                if path == target or path.is_symlink() or path.stat().st_size > 4194304:
                    continue
                add(path.read_bytes())
            except OSError:
                pass
target.parent.mkdir(parents=True, exist_ok=True)
target.write_bytes(bytes(out))
print(f"SCANNED {{len(out)}} bytes from {{sources}} sources")
"""
SENTINEL_SERVER = """\
import hmac, http.server, json, os, pathlib, threading
TOKEN = os.environ["SENTINEL_TOKEN"]
CHALLENGE = os.environ["SENTINEL_CHALLENGE"]
COUNTS = pathlib.Path("/tmp/sentinel/counts.json")
COUNTS.parent.mkdir(parents=True, exist_ok=True)
counts = {"authorized": 0, "unauthorized": 0, "tool_calls": 0}
lock = threading.Lock()
TOOL = {"name": "challenge", "description": "Return the sentinel challenge text.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": {"readOnlyHint": True}}
def bump(key):
    with lock:
        counts[key] += 1
        COUNTS.write_text(json.dumps(counts))
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def reply(self, status, body=None, extra=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        self.reply(405)
    def do_DELETE(self):
        self.reply(200)
    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0") or 0))
        presented = self.headers.get("Authorization", "")
        if not hmac.compare_digest(presented.encode(), ("Bearer " + TOKEN).encode()):
            bump("unauthorized")
            return self.reply(401, {"error": "unauthorized"})
        bump("authorized")
        try:
            request = json.loads(raw or b"{}")
        except ValueError:
            return self.reply(400)
        if "id" not in request:
            return self.reply(202)
        method = request.get("method")
        extra = {}
        if method == "initialize":
            params = request.get("params") or {}
            result = {"protocolVersion": params.get("protocolVersion", "2025-03-26"),
                      "capabilities": {"tools": {}},
                      "serverInfo": {"name": "sentinel", "version": "1"}}
            extra["Mcp-Session-Id"] = "sentinel-session"
        elif method == "tools/list":
            result = {"tools": [TOOL]}
        elif method == "tools/call":
            bump("tool_calls")
            result = {"content": [{"type": "text", "text": CHALLENGE}], "isError": False}
        elif method == "ping":
            result = {}
        else:
            return self.reply(200, {"jsonrpc": "2.0", "id": request["id"],
                                    "error": {"code": -32601, "message": "unknown method"}})
        self.reply(200, {"jsonrpc": "2.0", "id": request["id"], "result": result}, extra)
http.server.ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
"""

# --- programs the driver runs in a runner container ----------------------------

SWEEP_PROGRAM = f"""\
import json, os, pathlib
def fields(pid):
    return pathlib.Path(f"/proc/{{pid}}/stat").read_text().rsplit(")", 1)[1].split()
clis = []
for proc in pathlib.Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        argv = (proc / "cmdline").read_bytes().split(b"\\0")
        stat = fields(proc.name)
    except OSError:
        continue
    if int(stat[1]) == 1 and any(b"claude" in a for a in argv[:2]):
        clis.append({{"pid": int(proc.name), "start": int(stat[19])}})
files = {{}}
for top in ("/home/runner", "/tmp"):
    for base, _dirs, names in os.walk(top, onerror=lambda error: None):
        for name in names:
            path = pathlib.Path(base) / name
            if name.endswith(".jsonl") and "projects" in path.parts:
                try:
                    files[str(path)] = path.stat().st_size
                except OSError:
                    pass
holds = {{}}
for mode in ("read", "stop"):
    try:
        holds[mode] = json.loads((pathlib.Path({WORK_DIR!r}) / f"holding-{{mode}}").read_text())
    except (OSError, ValueError):
        holds[mode] = None
print(json.dumps({{"clis": clis, "files": files, "holds": holds}}))
"""
READ_PROGRAM = """\
import base64, pathlib, sys
try:
    data = pathlib.Path(sys.argv[1]).read_bytes()
except OSError:
    print("-")
else:
    print(base64.b64encode(data).decode())
"""
RELEASE_PROGRAM = f"""\
import pathlib, sys
root = pathlib.Path({WORK_DIR!r})
root.mkdir(parents=True, exist_ok=True)
(root / f"release-{{sys.argv[1]}}").write_text("go")
print("released")
"""
# The real repository workspace: HEAD, the SHA-256 of every tracked file, the
# porcelain status, and the uncommitted nonce (by hash). argv: write <nonce> | read.
WORKSPACE_PROGRAM = f"""\
import hashlib, json, os, pathlib, subprocess, sys
root = pathlib.Path({poe.WORKSPACE_MOUNT!r})
nonce = root / {NONCE_FILE!r}
if len(sys.argv) > 2 and sys.argv[1] == "write":
    nonce.write_text(sys.argv[2])
env = {{**os.environ, "GIT_OPTIONAL_LOCKS": "0"}}
git = ["git", "-c", "safe.directory=" + str(root), "-C", str(root)]
def run(*args):
    try:
        return subprocess.run([*git, *args], capture_output=True, env=env, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
state = {{"head": None, "files": {{}}, "status": None, "nonce": "absent"}}
head = run("rev-parse", "HEAD")
if head is not None and head.returncode == 0:
    state["head"] = head.stdout.decode().strip()
    listed = run("ls-files", "-z")
    for raw in listed.stdout.split(b"\\0") if listed is not None else []:
        if raw:
            try:
                digest = hashlib.sha256((root / raw.decode()).read_bytes()).hexdigest()
            except OSError:
                digest = "unreadable"
            state["files"][raw.decode()] = digest
    status = run("status", "--porcelain=v1", "--untracked-files=all")
    if status is not None and status.returncode == 0:
        state["status"] = sorted(status.stdout.decode().splitlines())
if nonce.is_file():
    state["nonce"] = hashlib.sha256(nonce.read_bytes()).hexdigest()
print(json.dumps(state))
"""
# In the stub Pod: main's commit and the SHA-256 of each committed blob of the
# stub's bare repository, the durable source a workspace is restored from.
STUB_TREE_PROGRAM = f"""\
import hashlib, json, subprocess
git = ["git", "--git-dir", {tch.STUB_BARE_REPO!r}]
head = subprocess.check_output([*git, "rev-parse", "refs/heads/main"], timeout=30).decode().strip()
names = subprocess.check_output([*git, "ls-tree", "-r", "-z", "--name-only", head], timeout=30)
files = {{}}
for raw in names.split(b"\\0"):
    if raw:
        path = raw.decode()
        blob = subprocess.check_output([*git, "cat-file", "blob", head + ":" + path], timeout=30)
        files[path] = hashlib.sha256(blob).hexdigest()
print(json.dumps({{"head": head, "files": files}}))
"""
# The stub Pod's entrypoint: copy the seed tree and issue out of the ConfigMap
# into a directory the stub's user owns, then become the stub with its flags.
STUB_BOOT = f"""\
import os, shutil, sys
seed, work = {poe.STUB_SEED_DIR!r}, {poe.STUB_WORK_DIR!r}
os.makedirs(os.path.join(work, "tree"))
for name in sorted(os.listdir(os.path.join(seed, "tree"))):
    if not name.startswith("."):
        shutil.copyfile(os.path.join(seed, "tree", name), os.path.join(work, "tree", name))
shutil.copyfile(os.path.join(seed, "issue.md"), os.path.join(work, "issue.md"))
stub = [sys.executable, "/stub/github_stub.py", "serve", *sys.argv[1:]]
os.execv(sys.executable, stub)
"""
INTERRUPT_PROGRAM = """\
import os, signal
me = os.getpid()
for name in os.listdir("/proc"):
    if name.isdigit() and int(name) not in (1, me, os.getppid()):
        try:
            os.kill(int(name), signal.SIGKILL)
        except OSError:
            pass
os.kill(1, signal.SIGTERM)
print("interrupted")
"""
KILL_PROGRAM = """\
import os, signal, sys
try:
    os.kill(int(sys.argv[1]), signal.SIGKILL)
except OSError:
    pass
print("killed")
"""
HISTORY_IDS_PROGRAM = """\
import asyncio, json, os
from curie_runner.history import StateApiTranscriptStore
env = os.environ
store = StateApiTranscriptStore(env["CURIE_HISTORY_REF"], env.get("CURIE_HISTORY_TOKEN"))
records = asyncio.run(store.load())
print(json.dumps([str(getattr(record, "record_id", "") or "") for record in records]))
"""


# --------------------------------------------------------------------------
# Output and clock
# --------------------------------------------------------------------------


class Output:
    """JSON on stdout, progress on stderr; both scanned raw and written scrubbed."""

    def __init__(self, scrubber: pps.Scrubber, scan: pps.TokenScan) -> None:
        self.scrubber = scrubber
        self.scan = scan

    def __repr__(self) -> str:
        return "<output>"

    def emit(self, payload: Mapping[str, Any]) -> None:
        raw = json.dumps(payload, sort_keys=True, default=str)
        self.scan.scan("stdout", raw.encode())
        print(self.scrubber.scrub(raw), file=sys.stdout, flush=True)

    def progress(self, message: str) -> None:
        self.scan.scan("stderr", message.encode())
        print(f"[owner-enforcement] {self.scrubber.scrub(message)}", file=sys.stderr, flush=True)


@dataclasses.dataclass
class Runtime:
    environ: dict[str, str]
    out: Output
    scrubber: pps.Scrubber
    scan: pps.TokenScan


@contextlib.contextmanager
def signals_to_exit() -> Iterator[None]:
    """SIGINT and SIGTERM become SystemExit so the finalizer runs."""

    def handler(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    previous = {sig: signal.signal(sig, handler) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)


@contextlib.contextmanager
def signals_deferred() -> Iterator[list[int]]:
    """While cleanup runs, a further signal is recorded instead of interrupting it."""

    received: list[int] = []

    def handler(signum: int, _frame: object) -> None:
        received.append(signum)

    previous = {sig: signal.signal(sig, handler) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield received
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)


def pause(seconds: float) -> None:
    threading.Event().wait(max(0.0, seconds))


# --------------------------------------------------------------------------
# Arguments
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="personal_owner_enforcement.py",
        description="Live proof of personal owner binding (#5378). See README.md.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def common(sub: argparse.ArgumentParser, *, grant: bool) -> None:
        sub.add_argument("--candidate", required=True)
        sub.add_argument("--context", required=True)
        sub.add_argument("--namespace", required=True)
        sub.add_argument("--docker-context", required=True)
        sub.add_argument("--registry", required=True)
        sub.add_argument("--input-file", required=True)
        sub.add_argument("--state-dir", required=True)
        sub.add_argument("--run-id", required=True)
        sub.add_argument("--grant-file", required=grant, default="")
        sub.add_argument("--grant-expires-at", required=grant, default="")

    common(commands.add_parser("preflight"), grant=True)
    common(commands.add_parser("rehearse"), grant=False)
    common(commands.add_parser("run"), grant=True)
    judge = commands.add_parser("judge")
    judge.add_argument("--state-dir", required=True)
    judge.add_argument("--run-id", required=True)
    judge.add_argument("--criterion", required=True, choices=(*poe.CRITERIA, "all"))
    judge.add_argument("--candidate", default="")
    child = commands.add_parser(CHILD)
    child.add_argument("mode", choices=("rehearse", "run"))
    common(child, grant=False)
    return parser


def flags_of(args: argparse.Namespace) -> poe.Flags:
    return poe.require_flags(
        candidate=args.candidate,
        context=args.context,
        namespace=args.namespace,
        docker_context=args.docker_context,
        registry=args.registry,
        state_dir=args.state_dir,
        run_id=args.run_id,
    )


def forwarded(args: argparse.Namespace) -> list[str]:
    argv = []
    for flag in (
        "candidate",
        "context",
        "namespace",
        "docker_context",
        "registry",
        "input_file",
        "state_dir",
        "run_id",
        "grant_file",
        "grant_expires_at",
    ):
        value = getattr(args, flag)
        if value:
            argv += ["--" + flag.replace("_", "-"), str(value)]
    return argv


# --------------------------------------------------------------------------
# Preflight (read-only)
# --------------------------------------------------------------------------


def probe(
    argv: Sequence[str], env: Mapping[str, str], rt: Runtime
) -> subprocess.CompletedProcess[bytes]:
    try:
        result = subprocess.run(
            list(argv),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=60,
            cwd=REPO_ROOT,
            env=dict(env),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return subprocess.CompletedProcess(list(argv), 127, b"", b"")
    rt.scan.scan("children", result.stdout + result.stderr)
    return result


def rbac_checks(namespace: str, grant_namespace: str) -> list[tuple[str, str, str | None]]:
    claims = "sandboxclaims.extensions.agents.x-k8s.io"
    found: list[tuple[str, str, str | None]] = [
        ("create", "namespaces", None),
        ("delete", "namespaces", None),
        ("get", "persistentvolumes", None),
    ]
    for verb in ("create", "get", "list", "delete"):
        found += [(verb, "secrets", namespace), (verb, "secrets", grant_namespace)]
        found += [(verb, "pods", namespace), (verb, claims, namespace)]
    for resource in (
        "configmaps",
        "services",
        "pods/exec",
        # The driver's forward to the stub code host.
        "pods/portforward",
        "persistentvolumeclaims",
        "sandboxtemplates.extensions.agents.x-k8s.io",
        "sandboxwarmpools.extensions.agents.x-k8s.io",
    ):
        found.append(
            ("create" if resource != "persistentvolumeclaims" else "list", resource, namespace)
        )
    found.append(("get", "pods/log", namespace))
    return found


def registry_status(port: int, path: str = "/v2/", method: str = "GET") -> int:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        connection.request(method, path, headers={"Accept": pps.MANIFEST_ACCEPT})
        response = connection.getresponse()
        response.read()
        return response.status
    except (OSError, http.client.HTTPException):
        return 0
    finally:
        connection.close()


def preflight(args: argparse.Namespace, rt: Runtime, *, grant: bool) -> dict[str, Any]:
    """Every prerequisite, in order; the first missing one is a setup error."""

    pps.refuse_ambient_credentials(rt.environ)
    flags = flags_of(args)
    inputs = poe.load_input_file(Path(args.input_file))
    checks: dict[str, bool] = {"flags": True, "input_file": True}
    if grant:
        poe.stat_grant_file(Path(args.grant_file) if args.grant_file else None)
        poe.require_expiry(args.grant_expires_at, time.time())
        checks.update({"grant_file": True, "grant_lifetime": True})
    missing = [name for name in REQUIRED_MODULES if importlib.util.find_spec(name) is None]
    if missing:
        raise poe.Refusal("python_env_incomplete", ", ".join(missing))
    checks["python_env"] = True
    env = {name: rt.environ[name] for name in poe.CHILD_ENV_NAMES if rt.environ.get(name)}
    env["KUBECONFIG"] = inputs.kubeconfig
    head = probe(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], env, rt)
    status = probe(
        ["git", "-C", str(REPO_ROOT), "status", "--porcelain", "--untracked-files=all"], env, rt
    )
    if (
        head.returncode != 0
        or head.stdout.decode().strip() != flags.candidate
        or status.returncode != 0
        or status.stdout.strip()
    ):
        raise poe.Refusal("candidate_dirty", "--candidate")
    checks["candidate_clean"] = True
    if (
        probe(
            poe.docker_argv(flags.docker_context, "info", "--format", "{{.ServerVersion}}"), env, rt
        ).returncode
        != 0
    ):
        raise poe.Refusal("docker_unavailable", "--docker-context")
    checks["docker_info"] = True
    if registry_status(flags.registry_port) != 200:
        raise poe.Refusal("registry_unreachable", "--registry")
    checks["registry"] = True
    ready = probe(poe.kubectl_argv(flags.context, None, "get", "--raw=/readyz"), env, rt)
    if ready.returncode != 0 or ready.stdout.strip() != b"ok":
        raise poe.Refusal("cluster_unready", "--context")
    checks["readyz"] = True
    for name in (flags.namespace, flags.grant_namespace):
        found = probe(
            poe.kubectl_argv(flags.context, None, "get", "namespace", name, "-o", "name"), env, rt
        )
        if found.returncode == 0:
            raise poe.Refusal("namespace_exists", "--namespace")
        if b"NotFound" not in found.stderr:
            raise poe.Refusal("cluster_unready", "--context")
    checks["namespaces_absent"] = True
    for verb, resource, namespace in rbac_checks(flags.namespace, flags.grant_namespace):
        answer = probe(
            poe.kubectl_argv(flags.context, namespace, "auth", "can-i", verb, resource), env, rt
        )
        if answer.stdout.strip() != b"yes":
            raise poe.Refusal("rbac_denied", f"{verb} {resource}")
    checks["rbac"] = True
    try:
        used = poe.OwnerLedger.open(flags.state_dir, flags.run_id, create=False)
    except poe.Refusal as refusal:
        if refusal.code != "ledger_missing":
            raise
    else:
        mode = (used.run() or {}).get("mode")
        used.close()
        raise poe.Refusal(
            "rehearsal_run_id" if mode == "rehearse" and grant else "paid_retry_refused",
            "--run-id",
        )
    checks["run_id_unused"] = True
    return {"flags": flags, "inputs": inputs, "checks": checks}


def cmd_preflight(args: argparse.Namespace, rt: Runtime) -> int:
    found = preflight(args, rt, grant=True)
    rt.out.emit({"preflight": "ok", "checks": found["checks"], "ceiling": poe.ceiling()})
    return poe.EXIT_PASS


def spawn_child(mode: str, args: argparse.Namespace, rt: Runtime, found: Mapping[str, Any]) -> int:
    """Re-execute as an ``env -i`` child: allowlisted names and paths, stdin from /dev/null."""

    flags: poe.Flags = found["flags"]
    env = poe.child_env(
        rt.environ,
        flags=flags,
        inputs=found["inputs"],
        evidence_dir=flags.state_dir / f"evidence-{flags.run_id}",
    )
    argv = [sys.executable, str(Path(__file__).resolve()), CHILD, mode, *forwarded(args)]
    process = subprocess.Popen(argv, env=env, stdin=subprocess.DEVNULL, cwd=REPO_ROOT)

    def relay(signum: int, _frame: object) -> None:
        with contextlib.suppress(OSError):
            process.send_signal(signum)

    previous = {sig: signal.signal(sig, relay) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        return process.wait()
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)


def cmd_rehearse(args: argparse.Namespace, rt: Runtime) -> int:
    found = preflight(args, rt, grant=False)
    rt.out.progress("preflight passed; starting the rehearsal child")
    return spawn_child("rehearse", args, rt, found)


def cmd_run(args: argparse.Namespace, rt: Runtime) -> int:
    found = preflight(args, rt, grant=True)
    rt.out.progress("preflight passed; starting the live child")
    return spawn_child("run", args, rt, found)


def cmd_judge(args: argparse.Namespace, rt: Runtime) -> int:
    state = Path(args.state_dir)
    pps.require_private_dir(state)
    pps.require_run_id(args.run_id)
    ledger = poe.OwnerLedger.open(state, args.run_id, create=False)
    try:
        run = ledger.run()
        if args.candidate and run is not None and run["candidate"] != args.candidate:
            raise poe.Refusal("candidate_mismatch", "--candidate")
        verdict = poe.judge(ledger, args.criterion)
    finally:
        ledger.close()
    rt.out.emit(verdict)
    return poe.exit_code(verdict["verdict"])


# --------------------------------------------------------------------------
# The child: the proof itself, on top of #5377's EnrollmentProof
# --------------------------------------------------------------------------


@dataclasses.dataclass
class ChildContext:
    mode: str
    flags: poe.Flags
    inputs: poe.InputFile
    grant_file: str
    expires_text: str
    ledger: poe.OwnerLedger
    rt: Runtime


@dataclasses.dataclass
class Track:
    """One native session the driver observes: its pod, process and transcript."""

    slot: str
    seq: int | None
    pod: str = ""
    pod_uid: str = ""
    claim: str = ""
    restart_count: int = 0
    cli: str = ""
    session_id: str = ""
    transcript: str = ""
    started_at: float = 0.0
    deadline: float = 0.0
    closed: bool = False
    seeds: list[dict[str, Any]] = dataclasses.field(default_factory=list)


def load_enrollment() -> Any:
    """#5377's cluster support module, by path (it is not an importable package)."""

    spec = importlib.util.spec_from_file_location(
        "personal_enrollment_cluster_support", ENROLLMENT_SUPPORT
    )
    if spec is None or spec.loader is None:
        raise poe.StepFailed("the enrollment proof support module is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def owner_proof_class(e: Any) -> type[Any]:  # noqa: C901 - one scenario, one class
    """``OwnerProof`` over the loaded ``EnrollmentProof`` (whose module loads at runtime)."""

    # Loaded by the enrollment module from runner/tests on its own path.
    cluster: Any = sys.modules["personal_token_cluster_support"]

    class OwnerProof(e.EnrollmentProof):  # type: ignore[misc]
        def __repr__(self) -> str:
            return "<private owner enforcement proof>"

        def __init__(self, ctx: ChildContext) -> None:
            super().__init__("synthetic")
            self.ctx = ctx
            self.paid = ctx.mode == "run"
            self.release = ctx.flags.release
            self.sentinel_token = e.HeldSecret(secrets.token_urlsafe(32))
            self.sentinel_challenge = "CHALLENGE-" + secrets.token_hex(8).upper()
            self.pushed: list[tuple[str, str]] = []
            self.native: dict[str, dict[str, Any]] = {}
            self.pending: list[poe.Reservation] = []
            self.reserved_at: dict[int, float] = {}
            self.tracks: list[Track] = []
            self.pod_states: dict[str, dict[str, Any]] = {}
            self.pvs: set[str] = set()
            self.foreign_before: dict[str, str] = {}
            self.live_deadline: float | None = None
            self.held_scrubbed: set[str] = set()
            self.work_items: dict[str, dict[str, Any]] = {}
            self.fenced = False
            # The stub code host: its forward, trust bundle, App key, issues
            # and committed source; the object store relay the sandbox reads.
            self.app_key = e.HeldSecret("")
            self.ca_bundle_path: Path | None = None
            self.forward_port = 0
            self.forward_process: subprocess.Popen[bytes] | None = None
            self.forward_restarts = 0
            self.s3_relay: Any = None
            self.repository_id = ""
            self.issues: dict[str, str] = {}
            self.stub_source: dict[str, Any] = {}
            if self.paid:
                grant = pps.load_grant(Path(ctx.grant_file))
                self.grant = e.HeldSecret(grant.reveal())
                grant.clear()
                self.grant_file = ctx.grant_file
                # Well formed and never issued, so the provider refuses it.
                self.invalid = e.HeldSecret("sk-ant-oat01-" + secrets.token_urlsafe(72))
            self.sync_scrubber()

        # --- private values ---------------------------------------------------

        def held(self) -> list[Any]:
            return [*super().held(), self.sentinel_token, self.app_key]

        def sync_scrubber(self) -> None:
            for value in self.held_values():
                if value not in self.held_scrubbed:
                    self.ctx.rt.scrubber.hold(pps.HeldSecret(value))
                    self.held_scrubbed.add(value)

        def scrub(self, text: str) -> str:
            self.sync_scrubber()
            return str(self.ctx.rt.scrubber.scrub(text))

        def observe(self, criterion: str, key: str, observed: Any) -> None:
            self.ctx.ledger.record(criterion, key, observed, time.time())

        def say(self, message: str) -> None:
            self.ctx.rt.out.progress(message)

        def targets(self) -> dict[str, str]:
            found = dict(super().targets())
            found[f"127.0.0.1:{self.ctx.flags.registry_port}"] = "<registry>"
            found[f"localhost:{self.ctx.flags.registry_port}"] = "<registry>"
            found[str(self.ctx.flags.state_dir)] = "<state>"
            return found

        # --- environment of the candidate API and worker ---------------------

        @property
        def code_host_origin(self) -> str:
            """The stub through the driver's forward; its certificate names 127.0.0.1."""

            e.require(self.forward_port > 0, "the code host stub is not running")
            return f"https://127.0.0.1:{self.forward_port}"

        def api_env(self) -> dict[str, str]:
            """The source API on the stub: its GitHub API and clone bases, App and trust.

            The API is a host process, so it trusts the stub's CA (plus the public
            roots) through ``SSL_CERT_FILE``; ``CURIE_CODE_HOST_CA_BUNDLE`` stays
            unset so no host path reaches a claim. The sandbox gets the chart's
            code host trust mount instead (``poe.workspace_template``).
            """

            e.require(self.ca_bundle_path is not None, "the code host trust bundle is missing")
            origin = self.code_host_origin
            return {
                **super().api_env(),
                "FACTORY_MODEL_AUTH_MODE": "personal",
                "GITHUB_API_URL": f"{origin}/api/v3",
                "GITHUB_CLONE_BASE": origin,
                "GITHUB_APP_ID": tch.STUB_APP_ID,
                "GITHUB_APP_PRIVATE_KEY": self.app_key.reveal(),
                "GITHUB_REPO_ALLOWLIST": json.dumps([tch.STUB_REPO]),
                "SSL_CERT_FILE": str(self.ca_bundle_path),
            }

        def worker_env(self) -> dict[str, str]:
            env = {
                **super().worker_env(),
                "FACTORY_MODEL_AUTH_MODE": "personal",
                "CURIE_WORKSPACE_ENABLED": "true",
                "CURIE_WORKSPACE_SCRATCH_ROOT": str(Path(self.private_dir) / "workspaces"),
                "CURIE_WORKSPACE_REFERENCE_TTL_SECONDS": str(REFERENCE_TTL_SECONDS),
                "CURIE_PUBLICATION_ENABLED": "false",
                "CURIE_WORK_ITEM_MAX_TURNS": str(poe.SESSION_TURNS),
                "DEFAULT_MAX_OUTPUT_TOKENS_PER_RUN": str(poe.SESSION_OUTPUT_TOKENS),
                "CURIE_MAX_ATTEMPTS": "2",
                "CURIE_RELEASE": self.release,
                "CURIE_RUNNER_TOTAL_TIMEOUT_S": str(poe.SESSION_SECONDS),
            }
            if self.ca_bundle_path is not None:
                # The worker clones the stub itself; git reads this, the scrubbed
                # clone environment keeps it.
                env["GIT_SSL_CAINFO"] = str(self.ca_bundle_path)
            if self.s3_relay is not None:
                # The worker signs workspace reads against this endpoint, so the
                # sandbox's workspace-init can redeem them through the relay.
                env["S3_ENDPOINT_URL"] = f"http://{self.runner_host}:{self.s3_relay.port}"
            if not self.paid:
                env["CURIE_FAKE_MODEL"] = "1"
            return env

        # --- image: the caller's loopback registry, digest pinned -------------

        def build_image(self) -> None:
            port = self.ctx.flags.registry_port
            root = self.docker("info", "--format", "{{.DockerRootDir}}").stdout.decode().strip()
            disk = self.command([*self.ssh_argv(), "df", "-P", root]).stdout.decode().splitlines()
            e.require(
                len(disk) >= 2 and int(disk[-1].split()[-2].rstrip("%")) <= 82,
                "scratch Docker storage exceeds the build threshold",
            )
            tag = f"{self.namespace}-runner:{self.candidate[:12]}"
            published = (
                f"127.0.0.1:{port}/{REPOSITORY}:"
                f"{pps.image_tag(self.candidate, self.ctx.flags.run_id)}"
            )
            for name in (tag, published):
                e.require(
                    self.docker("image", "inspect", name, check=False).returncode != 0,
                    "candidate image tag is already in use",
                )
                self.tags.append(name)
            self.command([self.cli, "build", "--tag", tag], timeout=1200)
            dockerfile = (
                f"FROM {tag}\nLABEL {pps.REVISION_LABEL}={self.candidate}\n"
                f"LABEL curie-proof-owner={self.ownership}\n"
            )
            self.docker(
                "build",
                "--file",
                "-",
                "--tag",
                published,
                ".",
                data=dockerfile.encode(),
                timeout=300,
            )
            revision = (
                self.docker(
                    "image",
                    "inspect",
                    published,
                    "--format",
                    '{{index .Config.Labels "' + pps.REVISION_LABEL + '"}}',
                )
                .stdout.decode()
                .strip()
            )
            e.require(revision == self.candidate, "candidate OCI revision does not match source")
            e.require(registry_status(port) == 200, "caller registry is unreachable")
            self.docker("push", published, timeout=300)
            digests = json.loads(
                self.docker(
                    "image", "inspect", published, "--format", "{{json .RepoDigests}}"
                ).stdout
            )
            matches = [
                value.rsplit("@", 1)[1]
                for value in digests
                if value.startswith(f"127.0.0.1:{port}/{REPOSITORY}@")
            ]
            e.require(
                len(matches) == 1 and matches[0].startswith("sha256:"),
                "pushed candidate manifest digest is unavailable",
            )
            self.digest = matches[0]
            self.pushed.append((REPOSITORY, self.digest))
            self.image = f"localhost:{port}/{REPOSITORY}@{self.digest}"
            e.require(
                self.command(["git", "status", "--porcelain", "--untracked-files=all"]).stdout
                == b"",
                "candidate source changed during the build",
            )
            self.evidence["manifest_digest"] = self.digest
            self.evidence["oci_revision_matches_source"] = True

        # --- inputs: template, bundle, sentinel --------------------------------

        def pod_spec(self) -> dict[str, Any]:
            """#5374's template, restarting in place and carrying the connector bearer."""

            spec: dict[str, Any] = super().pod_spec()
            # The chart sets none, so production pods restart in place.
            spec["restartPolicy"] = "Always"
            runner = spec["containers"][0]
            runner["env"] = [
                *runner["env"],
                cluster.secret_env(SENTINEL_ENV, SENTINEL_SECRET_NAME, "token"),
            ]
            return spec

        def create_inputs(self) -> None:
            super().create_inputs()
            self.secret(SENTINEL_SECRET_NAME, {"token": self.sentinel_token.reveal()})
            self.core.patch_namespaced_config_map(
                "custody-fixture",
                self.namespace,
                {"data": {"files.json": json.dumps(self.bundle())}},
                _request_timeout=30,
            )
            # Before the API starts: its GitHub bases name the stub's forward.
            self.start_code_host()

        def extension(self, plural: str, kind: str, name: str, spec: Mapping[str, Any]) -> None:
            """The source template gains the chart's workspace and code host trust."""

            if plural == "sandboxtemplates" and name == "custody-source":
                try:
                    program = poe.chart_workspace_init(
                        CHART_SANDBOX.read_text(), poe.CHART_WORKSPACE_VALUES
                    )
                    spec = poe.workspace_template(
                        spec, program=program, ca_config_map=tch.CA_CONFIG_MAP, ca_key=tch.CA_KEY
                    )
                except (OSError, ValueError) as failure:
                    raise poe.StepFailed(f"workspace template: {failure}") from None
            super().extension(plural, kind, name, spec)

        def start_backing_services(self) -> None:
            super().start_backing_services()
            # The object store binds loopback; the sandbox redeems its signed
            # workspace read through this relay on the caller's runner API host.
            self.s3_relay = e.TcpRelay(self.runner_host, self.ports["s3"])

        # --- the code host: the recorded TLS GitHub stub ------------------------

        def stub_exec(self, *argv: str, timeout: float = 60) -> tuple[int, bytes]:
            result = self.command(
                poe.kubectl_argv(
                    self.context,
                    self.namespace,
                    "exec",
                    tch.STUB_NAME,
                    "--container",
                    tch.STUB_NAME,
                    "--",
                    *argv,
                ),
                timeout=timeout,
                check=False,
            )
            return result.returncode, result.stdout

        def stub_api(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
            """One REST call inside the stub Pod, verified against its CA (tls_code_host's)."""

            code, raw = self.stub_exec(
                "python3",
                "-c",
                tch._STUB_CLIENT,
                method,
                path,
                json.dumps(body) if body is not None else "",
                str(STUB_PORT),
            )
            try:
                reply = json.loads(raw.decode().strip().splitlines()[-1]) if code == 0 else {}
            except (ValueError, IndexError):
                reply = {}
            return int(reply.get("status") or 0), reply.get("body")

        def stub_tree(self) -> dict[str, Any]:
            code, raw = self.stub_exec("python3", "-c", STUB_TREE_PROGRAM)
            try:
                found = json.loads(raw) if code == 0 else {}
            except ValueError:
                found = {}
            return found if isinstance(found, dict) else {}

        def start_code_host(self) -> None:
            """The stub Pod, its seeded repository and issues, its CA, the App key, the forward.

            One Pod in the owned namespace on the digest-pinned candidate image,
            as tls_code_host runs it. Only the public CA leaves the Pod, as the
            ``code-host-ca`` ConfigMap the template mounts; the App key is made
            here and reaches only the API process environment.
            """

            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import rsa

            manifests = poe.code_host_manifests(
                tch.stub_manifests(self.namespace, self.image, STUB_PORT),
                image=self.image,
                port=STUB_PORT,
                seed_files=SEED_FILES,
                issue_body=SEED_ISSUE,
                boot=STUB_BOOT,
                labels={"curie-proof-owner": self.ownership},
            )
            create = {
                "ConfigMap": self.core.create_namespaced_config_map,
                "Pod": self.core.create_namespaced_pod,
                "Service": self.core.create_namespaced_service,
            }
            for manifest in manifests:
                create[manifest["kind"]](self.namespace, manifest, _request_timeout=30)
            try:
                # Ready once it listens; its image is asserted to be the candidate digest.
                self.wait_pod(f"{tch.STUB_COMPONENT_LABEL}={tch.STUB_NAME}")
            except RuntimeError:
                raise poe.StepFailed("the code host stub Pod never became ready") from None

            def started() -> dict[str, str] | None:
                logs = self.command(
                    poe.kubectl_argv(
                        self.context, self.namespace, "logs", tch.STUB_NAME, "-c", tch.STUB_NAME
                    ),
                    check=False,
                ).stdout.decode(errors="replace")
                with contextlib.suppress(Exception):
                    return tch.parse_stub_startup(logs)
                return None

            startup = e.wait_until(
                started, timeout=120, interval=1, failure="the code host stub never started"
            )
            e.require(
                startup["base_url"] == f"https://{tch.stub_host(self.namespace)}:{STUB_PORT}",
                "the code host stub advertises an unexpected origin",
            )
            status, repository = self.stub_api("GET", f"/repos/{tch.STUB_REPO}")
            e.require(
                status == 200 and isinstance(repository, dict),
                "the code host stub did not serve its repository",
            )
            self.repository_id = str(repository["id"])
            for role in ISSUE_ROLES:
                status, issue = self.stub_api(
                    "POST",
                    f"/repos/{tch.STUB_REPO}/issues",
                    {"title": f"Owner enforcement proof: {role}", "body": SEED_ISSUE},
                )
                e.require(
                    status == 201 and isinstance(issue, dict), "the code host stub refused an issue"
                )
                self.issues[role] = str(issue["number"])
            self.stub_source = self.stub_tree()
            e.require(
                bool(self.stub_source.get("head")) and bool(self.stub_source.get("files")),
                "the code host stub's repository has no committed tree",
            )
            self.observe(
                "restart",
                "code_host.source",
                {"head": self.stub_source["head"], "files": sorted(self.stub_source["files"])},
            )
            code, ca = self.stub_exec("cat", tch.STUB_CA_FILE)
            e.require(
                code == 0 and b"CERTIFICATE" in ca and b"PRIVATE" not in ca,
                "the stub CA is not one public certificate",
            )
            code, roots = self.stub_exec("cat", tch.SYSTEM_ROOTS)
            e.require(code == 0 and b"CERTIFICATE" in roots, "the image's public roots are missing")
            # Stub CA first, then the public roots: this becomes the sandbox's
            # whole SSL_CERT_FILE store, as the chart's codeHostTrust bundle does.
            bundle = ca.rstrip(b"\n") + b"\n" + roots
            path = Path(self.private_dir) / "code-host-ca.crt"
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(bundle)
            self.ca_bundle_path = path
            self.core.create_namespaced_config_map(
                self.namespace,
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": tch.CA_CONFIG_MAP,
                        "labels": {"curie-proof-owner": self.ownership},
                    },
                    "data": {tch.CA_KEY: bundle.decode()},
                },
                _request_timeout=30,
            )
            # The stub checks no App JWT; any RSA key signs one. Per run, held.
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            self.app_key = e.HeldSecret(
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ).decode()
            )
            del key
            self.sync_scrubber()
            (Path(self.private_dir) / "workspaces").mkdir(mode=0o700)
            self.forward_port = e.free_local_port()
            self.ensure_forward()
            self.evidence["code_host"] = {
                "stub": "tools/github-stub (recorded, TLS)",
                "image": "candidate digest",
                "trust": "codeHostTrust ConfigMap mount; host processes via SSL_CERT_FILE",
            }

        def forward_ready(self) -> bool | None:
            """A TLS read of the stub's repository through the forward, verified."""

            if self.ca_bundle_path is None:
                return None
            context = ssl.create_default_context(cafile=str(self.ca_bundle_path))
            connection = http.client.HTTPSConnection(
                "127.0.0.1", self.forward_port, context=context, timeout=10
            )
            try:
                connection.request(
                    "GET",
                    f"/api/v3/repos/{tch.STUB_REPO}",
                    headers={"Accept": "application/vnd.github+json"},
                )
                status = connection.getresponse().status
            except (OSError, http.client.HTTPException):
                return None
            finally:
                connection.close()
            return True if status == 200 else None

        def ensure_forward(self) -> None:
            """The driver-owned forward to the stub Pod, restarted on the same port if it died."""

            process = self.forward_process
            if process is not None and process.poll() is None and self.forward_ready():
                return
            if process is not None:
                self.stop_process(process)
                self.forward_restarts += 1
                self.observe("restart", "code_host.forward_restarts", self.forward_restarts)
            self.forward_process = self.spawn(
                "code-host-forward",
                poe.kubectl_argv(
                    self.context,
                    self.namespace,
                    "port-forward",
                    "--address",
                    "127.0.0.1",
                    f"pod/{tch.STUB_NAME}",
                    f"{self.forward_port}:{STUB_PORT}",
                ),
                self.env,
            )
            e.wait_until(
                self.forward_ready,
                timeout=60,
                interval=0.5,
                failure="the code host forward never served the stub over TLS",
            )

        def check_stub(self) -> None:
            """The stub answered every call; an unsupported one stops it, so name it."""

            code, raw = self.stub_exec("python3", "-c", tch._STUB_UNSUPPORTED)
            try:
                refused = json.loads(raw) if code == 0 else None
            except ValueError:
                refused = None
            if not isinstance(refused, list):
                logs = self.command(
                    poe.kubectl_argv(
                        self.context, self.namespace, "logs", tch.STUB_NAME, "-c", tch.STUB_NAME
                    ),
                    check=False,
                ).stdout.decode(errors="replace")
                refused = tch._UNSUPPORTED_LINE.findall(logs) or ["no answer from the stub"]
            self.observe("restart", "code_host.unsupported_requests", refused)
            if refused:
                raise poe.StepFailed(f"the code host stub refused {refused[0]}")

        def workspace_state(self, pod: str, *, nonce: str | None = None) -> dict[str, Any]:
            """HEAD, tracked file hashes, status and nonce hash of the real workspace."""

            code, raw = self.pod_exec(
                pod, WORKSPACE_PROGRAM, *(("write", nonce) if nonce else ("read",)), timeout=60
            )
            try:
                state = json.loads(raw) if code == 0 else None
            except ValueError:
                state = None
            if not isinstance(state, dict):
                return {"head": None, "files": {}, "status": None, "nonce": "unreadable"}
            return state

        def bundle(self) -> dict[str, str]:
            files = dict(cluster.bundle_files())
            hold = "python3 ${CLAUDE_PLUGIN_ROOT}/hold.py"
            files["hooks/hooks.json"] = json.dumps(
                {
                    "hooks": {
                        "PreToolUse": [
                            {
                                "matcher": "Read",
                                "hooks": [
                                    {
                                        "type": "command",
                                        "command": f"{hold} read",
                                        "timeout": HOLD_SECONDS + 10,
                                    }
                                ],
                            }
                        ],
                        "Stop": [
                            {
                                "hooks": [
                                    {
                                        "type": "command",
                                        "command": f"{hold} stop",
                                        "timeout": HOLD_SECONDS + 10,
                                    }
                                ]
                            }
                        ],
                    }
                }
            )
            files["hold.py"] = HOLD_SCRIPT
            files["scan.py"] = SCAN_SCRIPT
            files["brief.txt"] = "PROOF-READ-OK\n"
            files["connectors.yaml"] = json.dumps(
                {
                    "connectors": {
                        SENTINEL: {
                            "image": self.image,
                            "port": SENTINEL_PORT,
                            "secrets": [SENTINEL_ENV],
                            "bearer_secret": SENTINEL_ENV,
                        }
                    }
                }
            )
            return files

        def deploy_sentinel(self) -> None:
            from plugin_format.connector_render import object_name

            selector = {"curie-proof-sentinel": self.ownership}
            self.core.create_namespaced_config_map(
                self.namespace,
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": SENTINEL_SERVER_MAP},
                    "data": {"server.py": SENTINEL_SERVER},
                },
                _request_timeout=30,
            )
            self.core.create_namespaced_pod(
                self.namespace,
                {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": "acme-sentinel", "labels": selector},
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": False,
                        "enableServiceLinks": False,
                        "securityContext": cluster.POD_SECURITY,
                        "volumes": [
                            {"name": "server", "configMap": {"name": SENTINEL_SERVER_MAP}},
                            {"name": "tmp", "emptyDir": {"sizeLimit": "16Mi"}},
                        ],
                        "containers": [
                            {
                                "name": "sentinel",
                                "image": self.image,
                                "imagePullPolicy": "Always",
                                "securityContext": cluster.CONTAINER_SECURITY,
                                "command": ["python3", "/srv/server.py"],
                                "env": [
                                    cluster.secret_env(SENTINEL_ENV, SENTINEL_SECRET_NAME, "token"),
                                    cluster.value_env(
                                        "SENTINEL_CHALLENGE", self.sentinel_challenge
                                    ),
                                ],
                                "ports": [{"containerPort": SENTINEL_PORT}],
                                "volumeMounts": [
                                    {"name": "server", "mountPath": "/srv", "readOnly": True},
                                    {"name": "tmp", "mountPath": "/tmp"},
                                ],
                                "readinessProbe": {
                                    "tcpSocket": {"port": SENTINEL_PORT},
                                    "periodSeconds": 1,
                                },
                                "resources": {
                                    "requests": {"cpu": "50m", "memory": "64Mi"},
                                    "limits": {"cpu": "500m", "memory": "256Mi"},
                                },
                            }
                        ],
                    },
                },
                _request_timeout=30,
            )
            self.core.create_namespaced_service(
                self.namespace,
                {
                    "apiVersion": "v1",
                    "kind": "Service",
                    "metadata": {"name": object_name(self.release, e.AGENT_NAME, SENTINEL)},
                    "spec": {
                        "selector": selector,
                        "ports": [{"port": SENTINEL_PORT, "targetPort": SENTINEL_PORT}],
                    },
                },
                _request_timeout=30,
            )
            self.wait_pod(f"curie-proof-sentinel={self.ownership}")

        def sentinel_counts(self) -> dict[str, int]:
            result = self.command(
                poe.kubectl_argv(
                    self.context,
                    self.namespace,
                    "exec",
                    "acme-sentinel",
                    "--container",
                    "sentinel",
                    "--",
                    "cat",
                    "/tmp/sentinel/counts.json",
                ),
                check=False,
            )
            try:
                body = json.loads(result.stdout) if result.returncode == 0 else {}
            except ValueError:
                body = {}
            return {
                key: int(body.get(key, 0)) for key in ("authorized", "unauthorized", "tool_calls")
            }

        # --- agent, principals, enrollment -------------------------------------

        def prepare_owner(self) -> None:
            oidc = e.oidc_harness()
            bundle = Path(self.private_dir) / "bundle"
            (bundle / ".claude-plugin").mkdir(parents=True)
            (bundle / ".claude-plugin" / "plugin.json").write_text(
                json.dumps({"name": e.AGENT_NAME, "version": "0.1.0"})
            )
            deployed = self.command(
                [
                    self.cli,
                    "--json",
                    "cluster",
                    "deploy",
                    "--plugin-dir",
                    str(bundle),
                    "--api-url",
                    self.api_url,
                    "--agent",
                    e.AGENT_NAME,
                    "--slack-channel",
                    e.ADDRESS,
                    "--release",
                    self.release,
                    "--namespace",
                    self.namespace,
                ],
                private_env={"CURIE_API_KEY": self.platform_key.reveal()},
                timeout=180,
                check=False,
            )
            # The API deploy completes first; the connector step after it reads a
            # Helm release this proof does not install, and only that refusal passes.
            e.require(
                deployed.returncode == 0
                or (
                    b"could not read the app name for release" in deployed.stdout
                    and b"as " + e.AGENT_NAME.encode() + b": ok" in deployed.stderr
                ),
                "cluster deploy failed before its API deploy completed",
            )
            agents = self.sql_rows(
                "SELECT id, tenant_id FROM curie.agents WHERE name = :name",
                {"name": e.AGENT_NAME},
            )
            e.require(len(agents) == 1, "the deployed agent is missing")
            self.agent_id = str(agents[0]["id"])
            self.tenant_id = str(agents[0]["tenant_id"])
            for who, email in (("owner", e.OWNER_EMAIL), ("other", e.OTHER_EMAIL)):
                session = e.oidc_login(self.api_url, self.dex_origin, email)
                self.sessions[who] = session
                self.private_file(f"session-{who}", session)
                principal = oidc.request(
                    "GET",
                    f"{self.api_url}/console/principal",
                    headers={"Cookie": f"{e.SESSION_COOKIE}={session.reveal()}"},
                )
                e.require(principal.status == 200, "a minted session is not principal bound")
                self.principals[who] = {
                    "id": str(principal.json()["id"]),
                    "tenant_id": str(principal.json()["tenant_id"]),
                }
            if not self.paid:
                self.private_file("token-valid", self.grant)
            self.private_file("token-invalid", self.invalid)
            self.launcher = self.run_async(self._make_launcher)
            self.sync_scrubber()

        # --- the API -----------------------------------------------------------

        def api(
            self, method: str, path: str, body: Any = None, params: Mapping[str, str] | None = None
        ) -> tuple[int, dict[str, Any]]:
            query = ("?" + urllib.parse.urlencode(params)) if params else ""
            connection = http.client.HTTPConnection("127.0.0.1", self.port_api(), timeout=60)
            try:
                connection.request(
                    method,
                    path + query,
                    body=None if body is None else json.dumps(body).encode(),
                    headers={
                        "X-Curie-Worker-Token": self.worker_token.reveal(),
                        "Content-Type": "application/json",
                    },
                )
                response = connection.getresponse()
                raw = response.read()
            except (OSError, http.client.HTTPException):
                raise poe.StepFailed("the candidate API did not answer") from None
            finally:
                connection.close()
            try:
                parsed = json.loads(raw) if raw else {}
            except ValueError:
                parsed = {}
            return response.status, parsed if isinstance(parsed, dict) else {}

        def admit_item(
            self,
            issue: str,
            conversation: str,
            objective: str,
            generation: int,
            *,
            principal: str = "owner",
        ) -> tuple[int, dict[str, Any]]:
            request_id = uuid.uuid4()
            # The stub's own identities: the API's GitHub host is the forward's
            # authority, the repository id scopes the issue, and 5501 is the
            # only installation the stub mints tokens for.
            host = self.code_host_origin.removeprefix("https://")
            status, body = self.api(
                "POST",
                "/v1/internal/work-items/admissions",
                {
                    "agent_id": self.agent_id,
                    "kind": e.KIND,
                    "address": e.ADDRESS,
                    "reply_conversation_id": conversation,
                    "tracker": {
                        "kind": "github",
                        "host": host,
                        "scope_id": self.repository_id,
                        "issue_id": issue,
                    },
                    "repository": {
                        "code_host_kind": "github",
                        "host": host,
                        "project_id": self.repository_id,
                        "path": tch.STUB_REPO,
                    },
                    "code_host_installation_id": tch.STUB_INSTALLATION_ID,
                    "objective": objective,
                    "requester": e.REQUESTER,
                    "request_id": str(request_id),
                    "model_auth_mode": "personal",
                    "owner": {
                        "tenant_id": self.tenant_id,
                        "principal_id": self.principals[principal]["id"],
                        "enrollment_generation": generation,
                    },
                },
            )
            return status, {**body, "_request_id": str(request_id)}

        @staticmethod
        def code_of(body: Mapping[str, Any]) -> Any:
            detail = body.get("detail")
            return detail.get("code") if isinstance(detail, dict) else body.get("code")

        def revalidate_item(self, work_item_id: str) -> tuple[int, Any, Any]:
            self.work_item_id = work_item_id
            status, body = self.run_async(self.launch.revalidate)
            return status, body.get("enrollment_generation"), body.get("code")

        def durable_ids(self, thread: str) -> list[str]:
            value = self.sql_scalar(
                "SELECT value::text FROM curie.thread_transcripts WHERE agent_id = :agent "
                "AND thread_key = :thread AND binding_scope IS NULL",
                {"agent": uuid.UUID(self.agent_id), "thread": thread},
            )
            try:
                items = json.loads(value) if isinstance(value, str) else []
            except ValueError:
                items = []
            return [str(item.get("record_id") or "") for item in items if isinstance(item, dict)]

        def seed_history(self, thread: str) -> str:
            """One marker record in the thread's durable transcript, through the state API."""

            record_id = uuid.uuid4().hex
            url = (
                f"{self.api_url}/agents/{self.agent_id}/state/transcript/"
                f"{urllib.parse.quote(thread, safe='')}"
            )

            async def append() -> None:
                from curie_runner.history import StateApiTranscriptStore, TurnRecord

                store = StateApiTranscriptStore(url, self.platform_key.reveal())
                await store.append(
                    TurnRecord(
                        user=f"Remember the proof marker {self.marker}.",
                        assistant="NOTED",
                        record_id=record_id,
                    )
                )

            self.run_async(append)
            e.require(record_id in self.durable_ids(thread), "the marker record was not stored")
            return record_id

        def requests_of(self, work_item_id: str) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = self.sql_rows(
                "SELECT id, status, terminal_cause, terminal_at, termination_observation, "
                "teardown_unconfirmed_at, runtime_claim_name, runtime_owner, started_at, "
                "enrollment_generation FROM curie.execution_requests "
                "WHERE work_item_id = :w ORDER BY created_at",
                {"w": uuid.UUID(work_item_id)},
            )
            return rows

        # --- pods, sessions, holds -------------------------------------------

        def runner_pods(self) -> list[dict[str, Any]]:
            listing = self.core.list_namespaced_pod(
                self.namespace, label_selector=e.ADMISSION_LABEL, _request_timeout=30
            )
            return [self.api_client.sanitize_for_serialization(pod) for pod in listing.items]

        def pod_exec(
            self, pod: str, program: str, *args: str, timeout: float = 60, retain: bool = True
        ) -> tuple[int, bytes]:
            result = self.command(
                poe.kubectl_argv(
                    self.context,
                    self.namespace,
                    "exec",
                    pod,
                    "--container",
                    "runner",
                    "--",
                    "python3",
                    "-c",
                    program,
                    *args,
                ),
                timeout=timeout,
                check=False,
                retain_output=retain,
            )
            return result.returncode, result.stdout

        def image_ok(self, pod: Mapping[str, Any]) -> bool:
            status = pod.get("status") or {}
            observed = [
                *(status.get("containerStatuses") or []),
                *(status.get("initContainerStatuses") or []),
            ]
            return bool(observed) and all(
                str(item.get("imageID", "")).removeprefix("docker-pullable://") == self.image
                for item in observed
                if item.get("imageID")
            )

        def fence(self) -> None:
            """Stop every start: the worker, then every claim and runner pod in the namespace."""

            self.fenced = True
            self.stop_worker()
            with contextlib.suppress(Exception):
                for claim in self.custom.list_namespaced_custom_object(
                    cluster.GROUP,
                    cluster.VERSION,
                    self.namespace,
                    "sandboxclaims",
                    _request_timeout=30,
                ).get("items", []):
                    self.custom.delete_namespaced_custom_object(
                        cluster.GROUP,
                        cluster.VERSION,
                        self.namespace,
                        "sandboxclaims",
                        claim["metadata"]["name"],
                        _request_timeout=30,
                    )
            with contextlib.suppress(Exception):
                for pod in self.runner_pods():
                    self.core.delete_namespaced_pod(
                        pod["metadata"]["name"],
                        self.namespace,
                        grace_period_seconds=0,
                        _request_timeout=30,
                    )

        def sweep(self) -> None:
            """Observe every runner pod: images, native CLI processes and holds.

            A native process nothing reserved fences every start. An open
            session past its absolute deadline is interrupted and its pod deleted.
            """

            now = time.time()
            if self.paid and self.live_deadline is not None and now > self.live_deadline:
                self.fence()
                raise poe.StepFailed("the live window was exceeded; starts are fenced")
            states: dict[str, dict[str, Any]] = {}
            live: set[str] = set()
            listed: set[str] = set()
            for pod in self.runner_pods():
                meta, status = pod["metadata"], pod.get("status") or {}
                listed.add(meta["uid"])
                if status.get("phase") != "Running":
                    continue
                e.require(self.image_ok(pod), "a runner pod is not the pushed candidate digest")
                code, raw = self.pod_exec(meta["name"], SWEEP_PROGRAM, timeout=30)
                if code != 0:
                    continue
                try:
                    state = json.loads(raw)
                except ValueError:
                    continue
                runner: dict[str, Any] = next(
                    (
                        item
                        for item in status.get("containerStatuses") or []
                        if item.get("name") == "runner"
                    ),
                    {},
                )
                state.update(
                    {
                        "pod": meta["name"],
                        "uid": meta["uid"],
                        "claim": (meta.get("labels") or {}).get(e.ADMISSION_LABEL, ""),
                        "restarts": int(runner.get("restartCount") or 0),
                        "ready": bool(runner.get("ready")),
                    }
                )
                states[meta["uid"]] = state
                for cli in state.get("clis", []):
                    ref = f"{meta['uid']}:{cli['pid']}:{cli['start']}"
                    live.add(ref)
                    if ref not in self.native:
                        self.native[ref] = {
                            "pod": meta["name"],
                            "uid": meta["uid"],
                            "seen_at": now,
                            "seq": None,
                        }
                        self.on_native_start(ref, now)
            self.pod_states = states
            for track in self.tracks:
                if track.closed or not track.cli or not self.paid:
                    continue
                # Gone: its pod was read and the process is absent, or the pod is gone.
                # An unreadable pod proves nothing, so its deadline still holds.
                if (track.pod_uid in states and track.cli not in live) or (
                    track.pod_uid not in listed
                ):
                    track.closed = True
                elif track.deadline and now > track.deadline:
                    self.deadline_action(track)
            for reservation in list(self.pending):
                if now - self.reserved_at.get(reservation.seq, now) > poe.START_WINDOW:
                    self.close_unstarted(reservation, now)

        def on_native_start(self, ref: str, now: float) -> None:
            if not self.paid or not self.pending:
                self.ctx.ledger.record_unreserved(ref, now)
                self.fence()
                raise poe.StepFailed(
                    "a native session started during the rehearsal"
                    if not self.paid
                    else "a native session started without a reservation"
                )
            reservation = self.pending.pop(0)
            self.native[ref]["seq"] = reservation.seq
            self.ctx.ledger.mark_started(reservation.seq, ref, now)
            deadline = now + poe.SESSION_SECONDS
            if self.live_deadline is not None:
                deadline = min(deadline, self.live_deadline)
            for track in self.tracks:
                if track.seq == reservation.seq:
                    track.started_at, track.deadline = now, deadline
                    track.cli = ref
                    track.pod = str(self.native[ref]["pod"])
                    track.pod_uid = str(self.native[ref]["uid"])

        def close_unstarted(self, reservation: poe.Reservation, now: float) -> None:
            self.pending.remove(reservation)
            usage = poe.SessionUsage(
                0,
                0,
                True,
                error_classes=(
                    {
                        "cli_error": "not_started",
                        "http_status": None,
                        "provider_error_type": None,
                        "first_line": "",
                    },
                ),
            )
            self.ctx.ledger.settle(reservation.seq, usage, 0.0, now)

        def deadline_action(self, track: Track) -> None:
            """The absolute deadline: harvest, interrupt the native process, delete its pod.

            The session settles from what it wrote by then; usage that cannot be
            read is unknown, which exhausts the allowance and stops the run.
            """

            track.closed = True
            _data, usage = self.harvest(track)
            if track.cli:
                self.pod_exec(track.pod, KILL_PROGRAM, track.cli.split(":")[1], timeout=20)
            with contextlib.suppress(Exception):
                self.core.delete_namespaced_pod(
                    track.pod, self.namespace, grace_period_seconds=0, _request_timeout=30
                )
            self.settle(track, usage)
            self.observe("budget", f"deadline.{track.slot}", "enforced")
            if not usage.known:
                self.fence()
                raise poe.StepFailed("a session passed its deadline with unknown usage")

        def wait(self, predicate: Callable[[], Any], *, timeout: float, failure: str) -> Any:
            deadline = time.monotonic() + timeout
            while True:
                self.sweep()
                value = predicate()
                if value is not None:
                    return value
                if time.monotonic() >= deadline:
                    raise poe.StepFailed(failure)
                pause(POLL_SECONDS)

        def wait_soft(self, predicate: Callable[[], Any], *, timeout: float) -> Any:
            try:
                return self.wait(predicate, timeout=timeout, failure="")
            except poe.StepFailed as failure:
                if str(failure):
                    raise
                return None

        def reserve(self, slot: str) -> poe.Reservation | None:
            if not self.paid:
                return None
            reservation = self.ctx.ledger.reserve(slot, time.time())
            self.reserved_at[reservation.seq] = time.time()
            self.pending.append(reservation)
            self.tracks.append(Track(slot=slot, seq=reservation.seq))
            return reservation

        def track_of(self, slot: str) -> Track:
            found = next((t for t in self.tracks if t.slot == slot), None)
            if found is None:
                found = Track(slot=slot, seq=None)
                self.tracks.append(found)
            return found

        def boundary(
            self,
            slot: str,
            mode: str,
            *,
            exclude: Sequence[str] = (),
            timeout: float = poe.START_WINDOW + 30,
        ) -> Track | None:
            """The pod and process of ``slot`` once it holds at ``mode``.

            Paid: the bundle's hook holds the native process and names it. Rehearsal:
            a Ready runner pod with no native process (the fake model holds nothing).
            """

            track = self.track_of(slot)

            def held() -> Track | None:
                for state in self.pod_states.values():
                    if self.paid:
                        hold = (state.get("holds") or {}).get(mode)
                        if not hold or not hold.get("cli_pid"):
                            continue
                        ref = f"{state['uid']}:{hold['cli_pid']}:{hold['cli_start']}"
                        native = self.native.get(ref)
                        if ref in exclude or native is None or native["seq"] != track.seq:
                            continue
                        track.cli, track.session_id = ref, str(hold.get("session_id") or "")
                        track.transcript = str(hold.get("transcript_path") or "")
                    else:
                        ref = f"{state['uid']}:{state['restarts']}"
                        if ref in exclude or not state.get("ready"):
                            continue
                        track.cli = ref
                    track.pod, track.pod_uid = state["pod"], state["uid"]
                    track.claim, track.restart_count = state["claim"], state["restarts"]
                    return track
                return None

            found: Track | None = self.wait_soft(held, timeout=timeout)
            return found

        def release_hold(self, track: Track, mode: str) -> None:
            if self.paid and track.pod:
                self.pod_exec(track.pod, RELEASE_PROGRAM, mode, timeout=20)

        def read_pod_file(self, pod: str, path: str) -> bytes | None:
            code, raw = self.pod_exec(pod, READ_PROGRAM, path, retain=False)
            text = raw.strip()
            if code != 0 or text == b"-":
                return None
            try:
                return base64.b64decode(text, validate=True)
            except ValueError:
                return None

        def harvest(self, track: Track) -> tuple[bytes | None, poe.SessionUsage]:
            if not self.paid:
                return None, poe.SessionUsage.unknown()
            if not track.transcript:
                # A session that never held at Read may still hold at Stop.
                state = self.pod_states.get(track.pod_uid) or {}
                for hold in (state.get("holds") or {}).values():
                    if (
                        hold
                        and f"{track.pod_uid}:{hold.get('cli_pid')}:{hold.get('cli_start')}"
                        == track.cli
                    ):
                        track.transcript = str(hold.get("transcript_path") or "")
            if not track.transcript:
                return None, poe.SessionUsage.unknown()
            data = self.read_pod_file(track.pod, track.transcript)
            if data is None:
                return None, poe.SessionUsage.unknown()
            if not track.seeds:
                code, raw = self.pod_exec(track.pod, e._REPLAY_SEED_READ, retain=False)
                with contextlib.suppress(ValueError):
                    track.seeds = json.loads(raw) if code == 0 else []
            return data, poe.transcript_usage(data, track.seeds, scrub=self.scrub)

        def settle(self, track: Track, usage: poe.SessionUsage) -> None:
            if track.seq is None:
                return
            now = time.time()
            elapsed = now - track.started_at if track.started_at else None
            self.ctx.ledger.settle(track.seq, usage, elapsed, now)

        def claim_labels(self, claim: str) -> dict[str, str]:
            try:
                found = self.custom.get_namespaced_custom_object(
                    cluster.GROUP,
                    cluster.VERSION,
                    self.namespace,
                    "sandboxclaims",
                    claim,
                    _request_timeout=30,
                )
            except Exception:  # noqa: BLE001 - an absent claim has no labels
                return {}
            return dict((found.get("metadata") or {}).get("labels") or {})

        def owner_labels_match(self, claim: str, generation: int) -> bool:
            from curie_worker.sandbox import types

            labels = self.claim_labels(claim)
            return (
                labels.get(types.MODEL_AUTH_MODE_LABEL) == "personal"
                and labels.get(types.OWNER_TENANT_LABEL) == self.tenant_id
                and labels.get(types.OWNER_PRINCIPAL_LABEL) == self.principals["owner"]["id"]
                and labels.get(types.ENROLLMENT_GENERATION_LABEL) == str(generation)
            )

        def history_in_pod(self, pod: str) -> list[str] | None:
            code, raw = self.pod_exec(pod, HISTORY_IDS_PROGRAM, retain=False)
            try:
                found = json.loads(raw) if code == 0 else None
            except ValueError:
                return None
            return [str(item) for item in found] if isinstance(found, list) else None

        def live_clis(self) -> set[str]:
            return {
                f"{state['uid']}:{cli['pid']}:{cli['start']}"
                for state in self.pod_states.values()
                for cli in state.get("clis", [])
            }

        def old_runtime_gone(
            self, kind: str, a: Track, b: Track | None, facts: Mapping[str, Any]
        ) -> bool:
            """The interrupted runtime no longer exists: process, Pod or worker."""

            if kind == "process":
                # A container restart ends every process in it, the native CLI included.
                return (
                    b is not None
                    and b.restart_count > a.restart_count
                    and (not self.paid or a.cli not in self.live_clis())
                )
            if kind == "pod":
                return a.pod_uid not in {pod["metadata"]["uid"] for pod in self.runner_pods()}
            return bool(facts.get("old_worker_exited"))

        # --- the live window and the grant probe -------------------------------

        def start_live(self) -> None:
            """The one-turn grant probe with an empty HOME, then the live window opens."""

            expires_at = poe.require_expiry(self.ctx.expires_text, time.time())
            resolved = shutil.which(
                self.ctx.inputs.grant_probe_cli, path=os.environ.get("PATH", "")
            )
            if resolved is None:
                raise poe.Refusal("grant_probe_failed", "grant_probe_cli not found")
            with tempfile.TemporaryDirectory(prefix="owner-grant-probe-") as home:
                e.require(not any(Path(home).iterdir()), "the grant probe home was not empty")
                try:
                    result = subprocess.run(
                        pps.grant_probe_argv(resolved),
                        input=pps.GRANT_PROBE_PROMPT,
                        capture_output=True,
                        timeout=pps.GRANT_PROBE_SECONDS,
                        cwd=home,
                        env=pps.grant_probe_env(
                            home=home, cli_dir=str(Path(resolved).parent), grant=self.grant.reveal()
                        ),
                        check=False,
                    )
                except (OSError, subprocess.TimeoutExpired):
                    result = subprocess.CompletedProcess([], 124, b"", b"")
            self.ctx.rt.scan.scan("children", result.stdout + result.stderr)
            outcome = pps.grant_probe_result(result.returncode, result.stdout, result.stderr)
            if outcome.get("error"):
                outcome["error"]["first_line"] = self.scrub(outcome["error"]["first_line"])
            self.observe("budget", "grant_probe.ok", bool(outcome["ok"]))
            self.observe("budget", "grant_probe.outcome", outcome)
            if not outcome["ok"]:
                raise poe.Refusal("grant_probe_failed", pps.probe_summary(outcome))
            self.live_deadline = self.ctx.ledger.start_live(time.time(), expires_at)

        # --- the scenario -------------------------------------------------------

        def execute(self) -> None:
            self.snapshot_foreign()
            for name, action in (
                ("setup", self.setup),
                ("prepare", self.prepare_owner),
                ("sentinel", self.deploy_sentinel),
                ("enroll", self.enroll),
                ("process", lambda: self.pair(*PAIRS[0])),
                ("pod", lambda: self.pair(*PAIRS[1])),
                ("worker", lambda: self.pair(*PAIRS[2])),
                ("rejected", self.rejected),
                ("revalidated", self.revalidated),
                ("disconnect", self.disconnect),
                ("audit", self.final_audit),
            ):
                self.step(name)
                self.say(f"phase {name}")
                action()
            self.step("completed")

        def objective(self, hosted: bool = False) -> str:
            if self.paid:
                return HOSTED_OBJECTIVE if hosted else PAIR_OBJECTIVE
            return REHEARSE_HOSTED_OBJECTIVE if hosted else REHEARSE_PAIR_OBJECTIVE

        def enroll(self) -> None:
            self.headers_seen["enrolled"] = self.write(
                "enroll", token=self.valid_token, previous=None
            )
            status, body = self.admit_item(
                self.issues["other"], OTHER_CONVERSATION, self.objective(), 1, principal="other"
            )
            self.observe(
                "selection",
                "other_principal.admission_code",
                self.code_of(body) if status != 200 else "admitted",
            )
            if status == 200:
                self.abandon(str(body["work_item"]["id"]))
            self.observe(
                "selection",
                "worker.model_credential_names",
                sorted(name for name in self.worker_env() if name in pps.SERVICE_CREDENTIAL_NAMES),
            )
            if self.paid:
                self.start_live()

        def admit_owner(
            self, issue: str, conversation: str, objective: str, generation: int
        ) -> dict[str, Any]:
            self.stop_worker()
            # The worker clones through the forward when this WorkItem starts.
            self.ensure_forward()
            status, body = self.admit_item(issue, conversation, objective, generation)
            e.require(status == 200, f"owner admission was refused: {self.code_of(body)}")
            item = dict(body["work_item"])
            item["_request_id"] = body["_request_id"]
            self.work_items[issue] = item
            return item

        def observe_selection(self, track: Track, item: Mapping[str, Any]) -> None:
            from curie_worker.sandbox import types
            from curie_worker.sandbox.claim_tokens import claim_object_names

            self.observe("selection", "admission.model_auth_mode", item.get("model_auth_mode"))
            self.observe(
                "selection", "admission.enrollment_generation", item.get("enrollment_generation")
            )
            labels = self.claim_labels(track.claim)
            self.observe(
                "selection", "claim.label.model_auth_mode", labels.get(types.MODEL_AUTH_MODE_LABEL)
            )
            self.observe(
                "selection",
                "claim.label.owner_tenant_matches",
                labels.get(types.OWNER_TENANT_LABEL) == self.tenant_id,
            )
            self.observe(
                "selection",
                "claim.label.owner_principal_matches",
                labels.get(types.OWNER_PRINCIPAL_LABEL) == self.principals["owner"]["id"],
            )
            self.observe(
                "selection",
                "claim.label.enrollment_generation",
                labels.get(types.ENROLLMENT_GENERATION_LABEL),
            )
            secret_name = claim_object_names(track.claim).secret
            try:
                stored = self.core.read_namespaced_secret(
                    secret_name, self.namespace, _request_timeout=30
                )
                data = base64.b64decode((stored.data or {}).get("CLAUDE_CODE_OAUTH_TOKEN", ""))
            except Exception:  # noqa: BLE001 - recorded as not carrying the grant
                data = b""
            self.observe(
                "selection",
                "claim.tokens_secret_carries_grant",
                bool(data) and data == self.grant.reveal().encode(),
            )
            del data
            pod = self.api_client.sanitize_for_serialization(
                self.core.read_namespaced_pod(track.pod, self.namespace, _request_timeout=30)
            )
            runner = next(c for c in pod["spec"]["containers"] if c["name"] == "runner")
            env = {item["name"]: item for item in runner.get("env") or []}
            ref = ((env.get("CLAUDE_CODE_OAUTH_TOKEN") or {}).get("valueFrom") or {}).get(
                "secretKeyRef"
            ) or {}
            self.observe(
                "selection", "pod.grant_env_from_tokens_secret", ref.get("name") == secret_name
            )
            self.observe(
                "selection",
                "pod.service_credential_names",
                sorted(
                    name
                    for name in env
                    if name
                    in {
                        "ANTHROPIC_API_KEY",
                        "ANTHROPIC_BASE_URL",
                        "CURIE_CREDENTIALS",
                        "ANTHROPIC_AUTH_TOKEN",
                    }
                ),
            )
            self.observe("selection", "pod.image_digest_matches", self.image_ok(pod))
            self.observe(
                "selection",
                "pod.revision_label_matches",
                self.image_ok(pod) and bool(self.evidence.get("oci_revision_matches_source")),
            )
            rows = self.requests_of(str(item["id"]))
            owner = rows[-1]["runtime_owner"] if rows else None
            self.observe(
                "selection",
                "worker.launched_without_driver",
                owner is not None and owner != e.DRIVER_OWNER,
            )

        def interrupt(self, kind: str, track: Track) -> dict[str, Any]:
            if kind == "process":
                self.pod_exec(track.pod, INTERRUPT_PROGRAM, timeout=30)
                return {"kind": kind}
            if kind == "pod":
                self.core.delete_namespaced_pod(
                    track.pod, self.namespace, grace_period_seconds=0, _request_timeout=30
                )
                return {"kind": kind}
            before = self.kill_worker()
            e.require(before is not None, "the candidate worker is not running")
            # SIGKILL was reaped above; the old worker's PID is gone before the new one starts.
            exited = before is not None and not Path(f"/proc/{before}").exists()
            self.start_worker()
            restarted: Any = vars(self).get("worker_process")
            return {
                "kind": kind,
                "worker_restarted": restarted is not None and restarted.pid != before,
                "old_worker_exited": exited,
            }

        def kill_worker(self) -> int | None:
            """SIGKILL the candidate worker: no graceful drain, its claims stay retained."""

            state: dict[str, Any] = self.__dict__
            process: Any = state.get("worker_process")
            if process is None:
                return None
            pid = int(process.pid)
            process.kill()
            process.wait(timeout=30)
            state["worker_process"] = None
            return pid

        def pair(self, kind: str, conversation: str) -> None:
            """One interruption and its automatic recovery on its own WorkItem and stub issue.

            The workspace is the real repository checkout the worker cloned from
            the stub. Before the interruption the driver writes an uncommitted
            nonce into it and reads HEAD, every tracked file's hash and the
            status; after recovery it reads them again. Committed state must match
            the stub repository on both sides for every kind.
            """

            a_slot, b_slot = f"{kind}-interrupted", f"{kind}-recovered"
            item = self.admit_owner(self.issues[kind], conversation, self.objective(), 1)
            thread = str(item["conversation_id"])
            self.seed_history(thread)
            self.reserve(a_slot)
            self.start_worker()
            a = self.boundary(a_slot, "read")
            if a is None:
                self.check_stub()
                raise poe.StepFailed(f"{kind}: the first session never reached its boundary")
            if kind == "process":
                self.observe_selection(a, item)
            ids_before = self.durable_ids(thread)
            before = self.workspace_state(a.pod, nonce=secrets.token_hex(16))
            self.capture_pod(a.pod)
            _data, usage = self.harvest(a)
            self.settle(a, usage)
            self.reserve(b_slot)
            facts = self.interrupt(kind, a)
            b = self.boundary(b_slot, "read", exclude=[a.cli])
            restart: dict[str, Any] = {
                "automatic_recovery": b is not None,
                "old_runtime_gone": self.old_runtime_gone(kind, a, b, facts),
                "git_status_before": before.get("status"),
            }
            if b is not None:
                b_data, _ = self.harvest(b)
                loaded = self.history_in_pod(b.pod)
                after = self.workspace_state(b.pod)
                restored = before.get("nonce") not in {"absent", "unreadable", None} and (
                    after.get("nonce") == before.get("nonce")
                )
                restart.update(
                    {
                        "owner_labels_match": self.owner_labels_match(b.claim, 1),
                        "history_identity": loaded is not None and loaded == ids_before,
                        "replay_marker_seen": (
                            b_data is not None and self.marker.encode() in b_data
                        )
                        if self.paid
                        else loaded is not None and loaded == ids_before,
                        "fresh_native_process": b.cli != a.cli,
                        **poe.committed_restoration(self.stub_source, before, after),
                        "git_status_after": after.get("status"),
                    }
                )
                if kind == "process":
                    restart.update(
                        {
                            "same_pod": b.pod_uid == a.pod_uid,
                            "container_restarted": b.restart_count == a.restart_count + 1,
                            "workspace_nonce_restored": restored,
                        }
                    )
                elif kind == "pod":
                    restart.update(
                        {
                            "new_pod": b.pod_uid != a.pod_uid,
                            "workspace_nonce_restored": restored,
                            "workspace_nonce_after_recreation": (
                                "present" if restored else after.get("nonce")
                            ),
                        }
                    )
                else:
                    restart["worker_restarted"] = bool(facts.get("worker_restarted"))
                    # Whether recovery adopted the retained Pod; informational only.
                    restart["workspace_nonce_retained_informational"] = restored
            for key, value in restart.items():
                self.observe("restart", f"{kind}.{key}", value)
            if kind == "worker":
                # The generation advance runs even without a recovered session, so
                # the later phases keep their generations; the retained-claim
                # checks are then missing and the fencing judge fails on them.
                self.fence_retained(b, item)
            elif b is None:
                self.abandon(str(item["id"]))
            else:
                self.finish_session(b)
                self.settled_runtime(str(item["id"]))
            self.check_stub()

        def finish_session(self, track: Track) -> bytes | None:
            """Release the read hold, harvest at the Stop hold, settle, release it."""

            self.release_hold(track, "read")
            if not self.paid:
                self.wait_soft(lambda: None, timeout=10)
                track.closed = True
                return None
            stopped = self.boundary(track.slot, "stop", timeout=poe.SESSION_SECONDS)
            data, usage = (
                self.harvest(track) if stopped is not None else (None, poe.SessionUsage.unknown())
            )
            self.settle(track, usage)
            self.capture_pod(track.pod)
            self.release_hold(track, "stop")
            track.closed = True
            return data

        def settled_runtime(self, work_item_id: str) -> None:
            """Every request of the WorkItem terminal and its runtime gone (bounded)."""

            def done() -> bool | None:
                rows = self.requests_of(work_item_id)
                claims = {row["runtime_claim_name"] for row in rows if row["runtime_claim_name"]}
                live = [
                    pod
                    for pod in self.runner_pods()
                    if (pod["metadata"].get("labels") or {}).get(e.ADMISSION_LABEL) in claims
                ]
                return True if all(row["terminal_at"] for row in rows) and not live else None

            if self.wait_soft(done, timeout=150) is None:
                self.abandon(work_item_id)

        def abandon(self, work_item_id: str) -> None:
            """Cancel a WorkItem whose recovery never came, through the internal route."""

            version = self.sql_scalar(
                "SELECT version FROM curie.work_items WHERE id = :w", {"w": uuid.UUID(work_item_id)}
            )
            self.api(
                "POST",
                f"/v1/internal/work-items/{work_item_id}/cancel",
                {"expected_version": version},
            )
            self.wait_soft(
                lambda: (
                    True
                    if all(row["terminal_at"] for row in self.requests_of(work_item_id))
                    else None
                ),
                timeout=120,
            )

        def fence_retained(self, track: Track | None, item: Mapping[str, Any]) -> None:
            """Advance the generation while the worker is down and the g1 claim is retained."""

            work_item_id = str(item["id"])
            if track is not None:
                _data, usage = self.harvest(track)
                self.settle(track, usage)
                self.capture_pod(track.pod)
            self.kill_worker()
            self.headers_seen["replaced"] = self.write(
                "replace",
                token=self.files["token-invalid"],
                previous=self.headers_seen["enrolled"],
            )
            self.observe(
                "fencing", "advance.generation", int(self.headers_seen["replaced"]["generation"])
            )
            fenced = self.requests_of(work_item_id)[-1]
            self.observe("fencing", "advance.request_cause", fenced["terminal_cause"])
            status, _generation, code = self.revalidate_item(work_item_id)
            self.observe(
                "fencing",
                "advance.revalidation_while_retained",
                code if status != 200 else "revalidated",
            )
            refusal = self.run_async(functools_partial(self.launch.read_grant, 1))
            self.observe(
                "fencing", "advance.old_generation_custody", getattr(refusal, "code", "granted")
            )
            known = set(self.native)
            self.start_worker()

            def confirmed() -> dict[str, Any] | None:
                row = self.requests_of(work_item_id)[-1]
                return (
                    row
                    if (
                        row["terminal_at"] is not None
                        and (row["termination_observation"] or "").strip()
                        and row["teardown_unconfirmed_at"] is None
                    )
                    else None
                )

            row = self.wait_soft(confirmed, timeout=150)
            from curie_worker.sandbox.claim_tokens import claim_object_names

            self.observe("fencing", "retained.teardown_confirmed", row is not None)
            if track is not None:
                track.closed = True
                self.observe(
                    "fencing", "retained.claim_absent", self.absent("sandboxclaims", track.claim)
                )
                self.observe(
                    "fencing",
                    "retained.tokens_secret_absent",
                    self.secret_absent(self.namespace, claim_object_names(track.claim).secret),
                )
            self.observe(
                "fencing", "retained.native_sessions_after_restart", len(set(self.native) - known)
            )
            status, generation, _code = self.revalidate_item(work_item_id)
            self.observe("fencing", "revalidated.generation", generation if status == 200 else None)

        def rejected(self) -> None:
            """Generation 2 holds a deliberately invalid grant: the provider refuses it."""

            issue, conversation = self.issues["worker"], PAIRS[2][1]
            work_item_id = str(self.work_items[issue]["id"])
            thread = str(self.work_items[issue]["conversation_id"])
            ids_before = self.durable_ids(thread)
            self.admit_owner(issue, conversation, self.objective(), 2)
            self.reserve("rejected")
            self.start_worker()
            track = self.track_of("rejected")
            files: dict[str, bytes] = {}

            def terminal() -> bool | None:
                # No hook holds a refused session, so every transcript file in its
                # pod is read on every poll, until the request is terminal.
                rows = self.requests_of(work_item_id)
                for state in self.pod_states.values():
                    mine = track.seq is not None and any(
                        n["seq"] == track.seq and n["uid"] == state["uid"]
                        for n in self.native.values()
                    )
                    if not self.paid or not mine:
                        continue
                    track.pod, track.pod_uid = state["pod"], state["uid"]
                    if not track.seeds:
                        code, raw = self.pod_exec(state["pod"], e._REPLAY_SEED_READ, retain=False)
                        with contextlib.suppress(ValueError):
                            track.seeds = json.loads(raw) if code == 0 else []
                    for path in state.get("files", {}):
                        data = self.read_pod_file(state["pod"], path)
                        if data:
                            files[path] = data
                return True if rows and rows[-1]["terminal_at"] is not None else None

            self.wait_soft(terminal, timeout=poe.SESSION_SECONDS)
            track.closed = True
            usage = combined_usage(
                [
                    poe.transcript_usage(data, track.seeds, scrub=self.scrub)
                    for data in files.values()
                ]
            )
            if self.paid:
                self.settle(track, usage)
            row = self.requests_of(work_item_id)[-1]
            error = usage.error_classes[0] if usage.error_classes else {}
            self.observe(
                "authentication-required", "rejected.http_status", error.get("http_status")
            )
            self.observe(
                "authentication-required",
                "rejected.provider_error_type",
                error.get("provider_error_type"),
            )
            self.observe(
                "authentication-required", "rejected.terminal_cause", row["terminal_cause"]
            )
            self.observe("authentication-required", "rejected.output_tokens", usage.output_tokens)
            self.observe(
                "authentication-required",
                "rejected.transcript_preserved",
                self.durable_ids(thread) == ids_before and bool(ids_before),
            )
            owner = self.sql_scalar(
                "SELECT owner_principal_id::text FROM curie.work_items WHERE id = :w",
                {"w": uuid.UUID(work_item_id)},
            )
            self.observe(
                "authentication-required",
                "rejected.owner_preserved",
                owner == self.principals["owner"]["id"],
            )
            from curie_worker.sandbox import types

            service = self.custom.list_namespaced_custom_object(
                cluster.GROUP,
                cluster.VERSION,
                self.namespace,
                "sandboxclaims",
                label_selector=f"{types.MODEL_AUTH_MODE_LABEL}=service",
                _request_timeout=30,
            ).get("items", [])
            self.observe(
                "authentication-required", "rejected.service_fallback_claims", len(service)
            )
            self.rejected_track = track

        def revalidated(self) -> None:
            """Replace with the valid grant, confirm teardown, revalidate, fresh process."""

            issue, conversation = self.issues["worker"], PAIRS[2][1]
            work_item_id = str(self.work_items[issue]["id"])
            self.headers_seen["restored"] = self.write(
                "replace", token=self.valid_token, previous=self.headers_seen["replaced"]
            )
            self.wait_soft(
                lambda: (
                    True
                    if (
                        self.requests_of(work_item_id)[-1]["teardown_unconfirmed_at"] is None
                        and self.requests_of(work_item_id)[-1]["terminal_at"] is not None
                    )
                    else None
                ),
                timeout=150,
            )
            status, generation, _code = self.revalidate_item(work_item_id)
            self.observe(
                "authentication-required",
                "revalidated.generation",
                generation if status == 200 else None,
            )
            self.admit_owner(issue, conversation, self.objective(hosted=True), 3)
            self.reserve("revalidated")
            self.start_worker()
            track = self.boundary("revalidated", "stop" if self.paid else "read")
            if track is None:
                self.observe("authentication-required", "revalidated.fresh_native_process", False)
                return
            before = getattr(self, "rejected_track", None)
            self.observe(
                "authentication-required",
                "revalidated.fresh_native_process",
                before is None or track.cli != before.cli,
            )
            data, usage = self.harvest(track)
            scan = self.read_pod_file(track.pod, f"{WORK_DIR}/scan.bin") if self.paid else None
            if not self.paid:
                # The fake model dials the hosted connector after the pod is ready.
                self.wait_soft(
                    lambda: True if self.sentinel_counts()["authorized"] else None, timeout=60
                )
            counts = self.sentinel_counts()
            if self.paid:
                self.settle(track, usage)
            self.capture_pod(track.pod)
            self.release_hold(track, "stop")
            track.closed = True
            results = poe.tool_results(data or b"")
            self.observe(
                "authentication-required",
                "revalidated.replay_marker_seen",
                data is not None and self.marker.encode() in data,
            )
            self.observe(
                "authentication-required",
                "revalidated.assistant_turns_positive",
                bool(usage.assistant_turns),
            )
            header = self.sentinel_token.reveal().encode()
            grant = self.grant.reveal().encode()
            self.observe(
                "hosted-header",
                "sentinel.challenge_returned",
                any(self.sentinel_challenge in text for text in results),
            )
            self.observe(
                "hosted-header", "sentinel.authorized_calls_positive", counts["authorized"] > 0
            )
            self.observe("hosted-header", "sentinel.unauthorized_calls", counts["unauthorized"])
            self.observe(
                "hosted-header",
                "public_control.returned",
                any(cluster.PUBLIC in text for text in results),
            )
            self.observe("hosted-header", "bash.scan_ran", scan is not None)
            self.observe(
                "hosted-header",
                "bash.negative_control_found",
                scan is not None and track.pod.encode() in scan,
            )
            self.observe(
                "hosted-header",
                "bash.header_found",
                scan is not None and e.leaks(scan, [header.decode()]),
            )
            self.observe(
                "hosted-header",
                "bash.grant_found_informational",
                scan is not None and e.leaks(scan, [grant.decode()]),
            )
            del scan, header, grant
            self.settled_runtime(work_item_id)

        def disconnect(self) -> None:
            """Disconnect with a waiting request; two worker restarts start nothing."""

            issue, conversation = self.issues["worker"], PAIRS[2][1]
            work_item_id = str(self.work_items[issue]["id"])
            self.thread_key = str(self.work_items[issue]["conversation_id"])
            waiting = self.admit_owner(issue, conversation, self.objective(), 3)
            disconnected = self.write(
                "disconnect", token=None, previous=self.headers_seen["restored"]
            )
            self.observe("disconnect", "disconnect.generation", disconnected.get("generation"))
            self.observe("disconnect", "disconnect.state", disconnected.get("state"))

            def custody_count() -> int:
                return len(
                    self.core.list_namespaced_secret(
                        self.grant_namespace, label_selector=CUSTODY_LABEL, _request_timeout=30
                    ).items
                )

            self.wait_soft(lambda: True if custody_count() == 0 else None, timeout=120)
            self.observe("disconnect", "disconnect.custody_secrets", custody_count())
            known = set(self.native)
            claims_before = {
                item["metadata"]["name"]
                for item in self.custom.list_namespaced_custom_object(
                    cluster.GROUP,
                    cluster.VERSION,
                    self.namespace,
                    "sandboxclaims",
                    _request_timeout=30,
                ).get("items", [])
            }
            for _ in range(2):
                self.start_worker()
                self.wait_soft(lambda: None, timeout=20)
                self.stop_worker()
            claims = [
                item
                for item in self.custom.list_namespaced_custom_object(
                    cluster.GROUP,
                    cluster.VERSION,
                    self.namespace,
                    "sandboxclaims",
                    _request_timeout=30,
                ).get("items", [])
                if item["metadata"]["name"] not in claims_before
            ]
            names = {item["metadata"]["name"] for item in claims}
            pods = [
                pod
                for pod in self.runner_pods()
                if (pod["metadata"].get("labels") or {}).get(e.ADMISSION_LABEL) in names
            ]
            from curie_worker.sandbox.claim_tokens import claim_object_names

            tokens = [
                name
                for name in names
                if not self.secret_absent(self.namespace, claim_object_names(name).secret)
            ]
            self.observe("disconnect", "disconnect.claims_after_restarts", len(claims))
            self.observe("disconnect", "disconnect.pods_after_restarts", len(pods))
            self.observe("disconnect", "disconnect.tokens_secrets_after_restarts", len(tokens))
            self.observe(
                "disconnect",
                "disconnect.native_sessions_after_restarts",
                len(set(self.native) - known),
            )
            row = next(
                (
                    r
                    for r in self.requests_of(work_item_id)
                    if str(r["id"]) == waiting["_request_id"]
                ),
                None,
            )
            self.observe(
                "disconnect",
                "disconnect.waiting_request_started",
                row is not None and row["started_at"] is not None,
            )
            status, body = self.admit_item(issue, conversation, self.objective(), 4)
            self.observe(
                "disconnect",
                "disconnect.admission_code",
                self.code_of(body) if status != 200 else "admitted",
            )
            status, _generation, code = self.revalidate_item(work_item_id)
            self.observe(
                "disconnect",
                "disconnect.revalidation_code",
                code if status != 200 else "revalidated",
            )
            refusal = self.run_async(functools_partial(self.launch.read_grant, 4))
            self.observe(
                "disconnect", "disconnect.custody_reader_code", getattr(refusal, "code", "granted")
            )
            route = self.run_async(self.launch.lookup)
            self.observe("disconnect", "disconnect.route", "absent" if route is None else "live")

        def final_audit(self) -> None:
            """No header or grant bytes in manifests, logs, events or the private ledger."""

            for plural in ("sandboxclaims", "sandboxtemplates", "sandboxwarmpools"):
                self.captures.append(
                    json.dumps(
                        self.custom.list_namespaced_custom_object(
                            cluster.GROUP,
                            cluster.VERSION,
                            self.namespace,
                            plural,
                            _request_timeout=30,
                        )
                    ).encode()
                )
            for namespace in (self.namespace, self.grant_namespace):
                self.captures.append(
                    json.dumps(
                        self.api_client.sanitize_for_serialization(
                            self.core.list_namespaced_event(namespace, _request_timeout=30)
                        )
                    ).encode()
                )
            with contextlib.suppress(Exception):
                self.capture_pod("acme-sentinel")
            header = [self.sentinel_token.reveal()]
            self.observe(
                "hosted-header",
                "audit.header_in_captures",
                any(e.leaks(capture, header) for capture in self.captures),
            )
            self.check_stub()

        # --- foreign resources, inventory, teardown ---------------------------

        def foreign_snapshot(self) -> dict[str, str]:
            found: dict[str, str] = {}
            env = {
                "PATH": os.environ.get("PATH", ""),
                "HOME": os.environ.get("HOME", ""),
                "KUBECONFIG": os.environ.get("KUBECONFIG", ""),
            }
            for kind, static in (
                ("priorityclasses", True),
                ("sandboxtemplates", False),
                ("deployments", False),
                ("validatingadmissionpolicies", True),
            ):
                argv = poe.kubectl_argv(self.context, None, "get", kind, "-A", "-o", "json")
                try:
                    result = subprocess.run(
                        argv,
                        env=env,
                        stdin=subprocess.DEVNULL,
                        capture_output=True,
                        timeout=60,
                        check=False,
                    )
                except (OSError, subprocess.TimeoutExpired):
                    continue
                if result.returncode != 0:
                    continue
                for item in json.loads(result.stdout).get("items", []):
                    meta = item.get("metadata") or {}
                    if meta.get("namespace") in {self.namespace, self.grant_namespace} or (
                        kind == "namespaces"
                        and meta.get("name") in {self.namespace, self.grant_namespace}
                    ):
                        continue
                    if kind == "deployments" and "sandbox" not in str(meta.get("name")):
                        continue
                    key = f"{kind}/{meta.get('namespace', '')}/{meta.get('name')}"
                    version = meta.get("resourceVersion") if static else meta.get("generation")
                    found[key] = f"{meta.get('uid')}:{version}"
            return found

        def snapshot_foreign(self) -> None:
            self.foreign_before = self.foreign_snapshot()

        def fence_step(self) -> None:
            self.fence()

        def inventory_step(self) -> None:
            """Before deletion: PVs bound to owned claims, and grant copies in Secrets."""

            if self.core is None:
                return
            now = time.time()
            for namespace in (self.namespace, self.grant_namespace):
                with contextlib.suppress(Exception):
                    for pvc in self.core.list_namespaced_persistent_volume_claim(
                        namespace, _request_timeout=30
                    ).items:
                        if pvc.spec.volume_name:
                            self.pvs.add(pvc.spec.volume_name)
            copies = 0
            grants = [value for value in (self.grant.reveal(), self.invalid.reveal()) if value]
            for namespace in (self.namespace, self.grant_namespace):
                with contextlib.suppress(Exception):
                    for secret in self.core.list_namespaced_secret(
                        namespace, _request_timeout=30
                    ).items:
                        for encoded in (secret.data or {}).values():
                            if e.leaks(base64.b64decode(encoded), grants):
                                copies += 1
            self.ctx.ledger.record_cleanup(
                "grant_copies_before_delete", "secrets", "ok", f"copies={copies}", now
            )

        def proof_cleanup_step(self) -> None:
            self.cleanup()

        def cleanup(self) -> None:
            """Stop the code host forward and close the object store relay, then #5377's cleanup.

            The stub Pod, Service and ConfigMaps go with the owned namespace; the
            trust bundle and the worker's clone scratch go with the private
            directory; the App key is cleared with every other held value.
            """

            if self.forward_process is not None:
                with contextlib.suppress(Exception):
                    self.stop_process(self.forward_process)
            if self.s3_relay is not None:
                self.s3_relay.close()
            super().cleanup()

        def registry_step(self) -> None:
            port = self.ctx.flags.registry_port
            for repository, digest in self.pushed:
                path = f"/v2/{repository}/manifests/{digest}"
                registry_status(port, path, "DELETE")
                gone = registry_status(port, path) == 404
                self.ctx.ledger.record_cleanup(
                    "registry",
                    f"{repository}@{digest}",
                    "absent" if gone else "present",
                    "",
                    time.time(),
                )
            if not self.pushed:
                self.ctx.ledger.record_cleanup("registry", "none", "absent", "", time.time())

        def absence_step(self) -> None:
            """Prove every owned kind absent and every foreign resource untouched."""

            now = time.time()
            ledger = self.ctx.ledger
            names = {"namespace": self.namespace, "grant_namespace": self.grant_namespace}
            gone = {
                kind: kube_absent(self.context, "namespace", name) for kind, name in names.items()
            }
            for kind, name in names.items():
                ledger.record_cleanup(kind, name, "absent" if gone[kind] else "present", "", now)
            both = all(gone.values())
            # Namespaced objects cannot outlive their namespaces.
            for owned in ("claims", "pods", "grant_copies", "pvcs"):
                ledger.record_cleanup(owned, "namespaced", "absent" if both else "present", "", now)
            # The stub's Pod, Service and ConfigMaps (source, seed, CA) live in
            # the owned namespace; the App key was never a cluster object.
            for name in STUB_OBJECTS:
                ledger.record_cleanup(
                    "code_host", name, "absent" if gone["namespace"] else "present", "", now
                )
            for name in sorted(self.pvs):
                ok = kube_absent(self.context, "persistentvolume", name)
                ledger.record_cleanup("pvs", name, "absent" if ok else "present", "", now)
            if not self.pvs:
                ledger.record_cleanup("pvs", "none", "absent", "", now)
            running = [
                p
                for p in [vars(self).get("worker_process"), self.api_process, *self.processes]
                if p is not None and p.poll() is None
            ]
            ledger.record_cleanup(
                "processes", "source", "absent" if not running else "present", "", now
            )
            forward_closed = not self.forward_port or not e.tcp_open("127.0.0.1", self.forward_port)
            forwards = {
                "api-relay": self.relay is None or not self.relay.alive,
                "object-store-relay": self.s3_relay is None or not self.s3_relay.alive,
                "code-host-forward": (
                    self.forward_process is None or self.forward_process.poll() is not None
                )
                and forward_closed,
            }
            for name, closed in forwards.items():
                ledger.record_cleanup(
                    "port_forwards",
                    name,
                    "absent" if closed and not running else "present",
                    "",
                    now,
                )
            problems = (self.evidence.get("cleanup") or {}).get("problems") or []
            ledger.record_cleanup(
                "containers",
                "backing",
                "absent" if not any("container" in p for p in problems) else "present",
                "",
                now,
            )
            ledger.record_cleanup(
                "private_dir",
                "run",
                "absent"
                if not self.private_dir or not Path(self.private_dir).exists()
                else "present",
                "",
                now,
            )
            after = self.foreign_snapshot() if self.foreign_before else {}
            changed = [key for key, value in self.foreign_before.items() if after.get(key) != value]
            ledger.record_cleanup(
                "foreign_untouched",
                "snapshot",
                "ok" if self.foreign_before and not changed else "present",
                f"changed={len(changed)}",
                now,
            )

        def register_teardown(self, finalizer: poe.Finalizer) -> None:
            """Registered before anything is created; runs last-registered first."""

            steps = {
                "fence": self.fence_step,
                "inventory": self.inventory_step,
                "proof-cleanup": self.proof_cleanup_step,
                "registry": self.registry_step,
                "absence": self.absence_step,
            }
            for name in reversed(TEARDOWN_ORDER):
                finalizer.register(name, steps[name])

    return OwnerProof


def kube_absent(context: str, kind: str, name: str) -> bool:
    """A cluster-scoped object is NotFound through the caller's kubeconfig and context."""

    env = {key: os.environ[key] for key in ("PATH", "HOME", "KUBECONFIG") if key in os.environ}
    try:
        result = subprocess.run(
            poe.kubectl_argv(context, None, "get", kind, name, "-o", "name"),
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode != 0 and b"NotFound" in result.stderr


def combined_usage(parts: Sequence[poe.SessionUsage]) -> poe.SessionUsage:
    """One session written across several files; any unknown part makes it unknown."""

    if not parts or not all(part.known for part in parts):
        return poe.SessionUsage.unknown()
    return poe.SessionUsage(
        output_tokens=sum(part.output_tokens or 0 for part in parts),
        assistant_turns=sum(part.assistant_turns or 0 for part in parts),
        known=True,
        models=tuple(sorted({model for part in parts for model in part.models})),
        error_classes=tuple(item for part in parts for item in part.error_classes),
        texts=tuple(text for part in parts for text in part.texts),
    )


def functools_partial(function: Callable[..., Any], *args: Any) -> Callable[[], Any]:
    def call() -> Any:
        return function(*args)

    return call


def child_main(args: argparse.Namespace, rt: Runtime) -> int:
    flags = flags_of(args)
    inputs = poe.load_input_file(Path(args.input_file))
    paid = args.mode == "run"
    if paid:
        poe.stat_grant_file(Path(args.grant_file) if args.grant_file else None)
        poe.require_expiry(args.grant_expires_at, time.time())
    ledger = poe.OwnerLedger.open(
        flags.state_dir, flags.run_id, create=True, scrub=rt.scrubber.scrub
    )
    ledger.begin(flags.candidate, "live" if paid else "rehearse", time.time())
    ctx = ChildContext(args.mode, flags, inputs, args.grant_file, args.grant_expires_at, ledger, rt)

    def record(name: str, ok: bool, detail: str) -> None:
        ledger.record_cleanup("step", name, "ok" if ok else "failed", detail, time.time())

    finalizer = poe.Finalizer(record)
    outcome, detail = "failed", ""
    proof: Any = None
    try:
        with signals_to_exit():
            enrollment = load_enrollment()
            proof = owner_proof_class(enrollment)(ctx)
            proof.register_teardown(finalizer)
            proof.execute()
        outcome = "completed"
    except (poe.StepFailed, pps.Refusal) as failure:
        detail = str(failure.payload() if isinstance(failure, pps.Refusal) else failure)
    except SystemExit as stop:
        detail = f"interrupted ({stop.code})"
    except AssertionError as failure:
        detail = str(failure)
    except BaseException as failure:  # noqa: BLE001 - a fixed name, never the message
        detail = type(failure).__name__
        if proof is not None:
            with contextlib.suppress(Exception):
                proof.retain_failure(failure)
    finally:
        with signals_deferred():
            ledger.mark_cleanup(started=time.time())
            results = finalizer.run()
            ledger.mark_cleanup(finished=time.time())
            ledger.finish(outcome, time.time())
    sessions = ledger.sessions()
    summary = {
        "run_id": flags.run_id,
        "mode": args.mode,
        "outcome": outcome,
        "detail": rt.scrubber.scrub(detail),
        "sessions": [
            {
                key: item[key]
                for key in (
                    "seq",
                    "slot",
                    "known",
                    "output_tokens",
                    "assistant_turns",
                    "unreserved",
                )
            }
            for item in sessions
        ],
        "cleanup": [{"step": name, "ok": ok} for name, ok in results],
        "scan": rt.scan.as_json(),
    }
    leaked = any(entry.token_bytes_found for entry in rt.scan.channels.values())
    stored = [flags.state_dir / poe.LEDGER_FILE]
    evidence = flags.state_dir / f"evidence-{flags.run_id}"
    if evidence.is_dir():
        stored += [path for path in evidence.iterdir() if path.is_file()]
    for path in stored:
        with contextlib.suppress(OSError):
            leaked |= rt.scrubber.found(path.read_bytes())
    ledger.record("hosted-header", "audit.header_in_ledger_or_output", leaked, time.time())
    ledger.close()
    rt.out.emit(summary)
    for held in rt.scrubber.held():
        held.clear()  # fingerprints stay for any later scan; the values do not
    return (
        poe.EXIT_PASS if outcome == "completed" and all(ok for _, ok in results) else poe.EXIT_FAIL
    )


COMMANDS: dict[str, Callable[[argparse.Namespace, Runtime], int]] = {
    "preflight": cmd_preflight,
    "rehearse": cmd_rehearse,
    "run": cmd_run,
    "judge": cmd_judge,
    CHILD: child_main,
}


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    environ = dict(os.environ if environ is None else environ)
    scrubber = pps.Scrubber()
    scan = pps.TokenScan(scrubber, ("stdout", "stderr", "children"))
    rt = Runtime(environ, Output(scrubber, scan), scrubber, scan)
    try:
        args = build_parser().parse_args(argv)
        if args.command != "judge":
            pps.refuse_ambient_credentials(environ)
        return COMMANDS[args.command](args, rt)
    except pps.Refusal as refusal:
        payload = (
            refusal.payload()
            if isinstance(refusal, poe.Refusal)
            else poe.Refusal(refusal.code, refusal.detail).payload()
        )
        rt.out.emit(payload)
        return poe.EXIT_REFUSED
    except poe.StepFailed as failure:
        rt.out.emit({"error": "step_failed", "detail": str(failure)})
        return poe.EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
