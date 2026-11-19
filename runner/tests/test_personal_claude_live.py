"""Live proof of explicit personal Claude authentication on a throwaway cluster (#5373, AC 5).

What this proves. The production runner image (the candidate built at the PR
head and imported into the cluster's containerd, asserted by image ID before
any prompt) runs the real runner, Claude Agent SDK and bundled native CLI in a
hardened pod and is driven only through the runner HTTP API. In order:

1. Service control: mode absent, the owner grant reaches the runner as
   ``CURIE_CREDENTIALS``, one short turn. This is also the redacted one-turn
   liveness check. If it fails the test stops with ``missing proof: liveness``
   and no paid session runs.
2. Personal session: ``CURIE_MODEL_AUTH_MODE=personal`` with the owner grant in
   ``CLAUDE_CODE_OAUTH_TOKEN`` and every AC 1 competing input present at once
   (cloud selectors and settings, API and auth tokens, custom headers, both base
   URL paths, a declared credential key, profile and federation state, saved
   login, a restorable gateway session in the inherited config dir and in a
   separate ``CLAUDE_SECURESTORAGE_CONFIG_DIR``, a user ``apiKeyHelper``, and
   subagent model forcing onto another Claude model). One implementer turn
   runs one Bash command (a bundle ``PreToolUse`` command hook marks it), then
   the real factory ``plan-reviewer`` and ``diff-reviewer`` roles as subagents.
   Every outbound connection goes through a CONNECT-logging proxy sidecar and
   every alternate endpoint points at a counting recorder sidecar, so the test
   observes that model traffic reached only the first-party endpoint, that no
   alternate saw a request, and that per-role usage carried the configured
   implementer and reviewer models, not the forced subagent model. The run
   succeeding at all is the evidence
   that no placeholder service credential authenticated it.
3. Negative control: the same conflicts without the grant refuse with
   ``authentication required`` before any connection. No inference.
4. Forced gateway managed policy at the real Linux managed settings path refuses
   before any connection. No inference.

Decision 7 ceilings are enforced from the observed stream, usage report and
native transcripts, and the budget env caps spend. There are no retries of a
paid turn anywhere.

What this does not prove. Full trusted factory admission, publication and
lifecycle are #5375. Worker emission of the personal boot env is #5378 (this
test writes that env into its own pod). Containment of the grant inside the
sandbox is #5374. The proxy does not intercept TLS, so request headers are not
observed here; the offline suite (``test_personal_claude_auth.py``) asserts the
bearer and absent ``x-api-key`` against a Messages fixture.

How it runs. Skipped unless ``CURIE_PERSONAL_LIVE=1``. When gated on, every
missing or failing prerequisite is a setup error, never a skip. The caller
supplies ``KUBECONFIG``, ``THROWAWAY_CONTEXT`` (every kubectl call passes it as
``--context``), ``SUBSCRIPTION_TEST_GRANT_FILE`` (checked by stat only, never
read here), ``CURIE_CANDIDATE_RUNNER_IMAGE_ID`` (``sha256:<hex>``) and the grant
in ``CLAUDE_CODE_OAUTH_TOKEN``. ``CURIE_CANDIDATE_RUNNER_IMAGE`` optionally names
the image by reference when the node cannot run it by ID; the ID is still
asserted. ``CURIE_PERSONAL_LIVE_EVIDENCE`` optionally names where to write the
sanitized evidence JSON. The grant is taken out of the process environment, is
never printed or put in argv, and reaches the cluster only inside one Secret
applied on stdin. The test owns one namespace and no cluster-scoped object;
teardown is registered before the namespace exists and audits its absence.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import os
import queue
import re
import secrets
import shutil
import socket
import stat
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENTS_DIR = REPO_ROOT / "examples" / "dark-factory" / "agents"
REVIEWER_ROLES = ("plan-reviewer", "diff-reviewer")

GATE_ENV = "CURIE_PERSONAL_LIVE"
EVIDENCE_ENV = "CURIE_PERSONAL_LIVE_EVIDENCE"
CONTEXT_ENV = "THROWAWAY_CONTEXT"
GRANT_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
GRANT_FILE_ENV = "SUBSCRIPTION_TEST_GRANT_FILE"
IMAGE_ID_ENV = "CURIE_CANDIDATE_RUNNER_IMAGE_ID"
IMAGE_REF_ENV = "CURIE_CANDIDATE_RUNNER_IMAGE"
REQUIRED_ENV = ("KUBECONFIG", CONTEXT_ENV, GRANT_FILE_ENV, IMAGE_ID_ENV, GRANT_ENV)

NAMESPACE = "test-5373-personal-claude-live"
FIELD_MANAGER = NAMESPACE
GRANT_SECRET = "live-grant"
AUTH_SECRET = "live-runner-auth"
BUNDLE_CONFIGMAP = "live-probe-bundle"
SCRIPTS_CONFIGMAP = "live-probe-scripts"
MANAGED_CONFIGMAP = "live-probe-managed-policy"
SYSTEM_OBJECTS = frozenset({"configmap/kube-root-ca.crt"})

IMPLEMENTER_MODEL = "claude-sonnet-5-5"
REVIEWER_MODEL = "claude-opus-5-5"
# The model the inherited subagent forcing names. Observed on CLI 2.1.301:
# CLAUDE_CODE_SUBAGENT_MODEL_FORCE=1 runs every subagent, the factory reviewer
# roles included, on CLAUDE_CODE_SUBAGENT_MODEL instead of their ``model:``.
FORCED_SUBAGENT_MODEL = "claude-haiku-4-5"
# The only first-party Messages host Decision 3 allows.
# https://code.claude.com/docs/en/network-config lists api.anthropic.com as the
# endpoint the CLI must reach.
PROVIDER_HOST = "api.anthropic.com"
PROVIDER_TARGET = f"{PROVIDER_HOST}:443"

# The chart default (agentSandbox.runner.hardening.runAsUser/runAsGroup/fsGroup)
# and the runner image's own USER.
RUNNER_UID = 1000
RUNNER_PORT = 8080
PROXY_PORT = 18080
RECORDER_PORT = 18081
SINK_PORT = 18082
PLUGIN_NAME = "live-probe"
PLUGIN_DIR = f"/bundle/{PLUGIN_NAME}"
MARKER_DIR = "/tmp/live-probe"
HOOK_MARKER = f"{MARKER_DIR}/hook-marker.jsonl"
HELPER_MARKER = f"{MARKER_DIR}/api-key-helper.log"
PROBE_LOGS = "/probe-logs"
INHERITED_CLAUDE_CONFIG_DIR = "/home/runner/.inherited-claude-config"
INHERITED_ANTHROPIC_CONFIG_DIR = "/home/runner/.inherited-anthropic-config"
# Holds only the saved credentials. Observed in CLI 2.1.301: the credential
# store is read from CLAUDE_SECURESTORAGE_CONFIG_DIR when set, whatever
# CLAUDE_CONFIG_DIR says.
INHERITED_SECURE_STORAGE_DIR = "/home/runner/.inherited-secure-storage"
TOOL_MARKER = "live-tool-ok"
RECORDER_URL = f"http://127.0.0.1:{RECORDER_PORT}"

pytestmark = pytest.mark.skipif(
    os.environ.get(GATE_ENV) != "1",
    reason=f"set {GATE_ENV}=1 with the private inputs to run the live personal Claude proof",
)


@dataclass(frozen=True)
class Ceiling:
    """Decision 7 bounds a step must stay within, and the env that caps spend."""

    turns: int
    output_tokens: int
    wall_seconds: float
    max_turns_env: int
    max_usd_per_day: float


CONTROL_CEILING = Ceiling(
    turns=6, output_tokens=2048, wall_seconds=300, max_turns_env=3, max_usd_per_day=5.0
)
PERSONAL_CEILING = Ceiling(
    turns=18, output_tokens=6144, wall_seconds=900, max_turns_env=8, max_usd_per_day=20.0
)
# A refused launch spends nothing. The small caps only bound a regression.
REFUSAL_CEILING = Ceiling(
    turns=0, output_tokens=512, wall_seconds=180, max_turns_env=1, max_usd_per_day=1.0
)

CONTROL_PROMPT = (
    "Live connectivity check. Do not use any tools. Reply with exactly this one line "
    "and nothing else: CONTROL-OK"
)
REVIEWER_PROMPT = (
    "Connectivity probe only, not a real review. There is no plan, no candidate and no "
    "repository to read. Do not use any tools. Reply with exactly one line: VERDICT: approve"
)
PERSONAL_PROMPT = f"""Live probe of this session's tool, hook and reviewer wiring. Do exactly \
these four steps, in order, and nothing else.
1. Run this Bash command once, in the foreground, exactly as written: \
echo '{{"marker":"{TOOL_MARKER}"}}'
2. Call the Agent tool once with subagent_type "{PLUGIN_NAME}:plan-reviewer", \
run_in_background false, a short description, and this prompt: "{REVIEWER_PROMPT}"
3. Then call the Agent tool once with subagent_type "{PLUGIN_NAME}:diff-reviewer", \
run_in_background false, a short description, and the same prompt.
4. Then reply with exactly one line: DONE"""
REFUSAL_PROMPT = "Live refusal check. Reply with exactly one line: SHOULD-NOT-RUN"


# --- Sidecar and bundle programs -------------------------------------------------
#
# Each runs with python3 from the candidate image. They are written for stdlib
# only and record no header values, so nothing they log can hold a credential.

PROXY_PY = r'''
"""Forward proxy that logs every CONNECT target and plain-HTTP host (no TLS interception)."""
import json
import os
import socket
import threading
import time
from urllib.parse import urlsplit

LOG_DIR = os.environ.get("PROBE_LOG_DIR", "/probe-logs")
PORT = int(os.environ["PROBE_PROXY_PORT"])
RECORDER = ("127.0.0.1", int(os.environ["PROBE_RECORDER_PORT"]))
LOG = os.path.join(LOG_DIR, "proxy.jsonl")
LOCK = threading.Lock()


def log(entry):
    entry["t"] = round(time.time(), 3)
    with LOCK, open(LOG, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def alternate(host):
    host = host.lower().rstrip(".")
    return (
        host == "example.com"
        or host.endswith(".example.com")
        or host in ("127.0.0.1", "localhost", "::1")
    )


def split(target, default_port):
    if target.startswith("["):
        host, _, rest = target[1:].partition("]")
        port = rest[1:] if rest.startswith(":") else ""
    else:
        host, sep, port = target.rpartition(":")
        if not sep:
            host, port = target, ""
    return host.lower(), int(port) if port.isdigit() else default_port


def pipe(source, sink):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            sink.sendall(data)
    except OSError:
        pass
    try:
        sink.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def read_head(conn):
    data = b""
    while b"\r\n\r\n" not in data and len(data) < 65536:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


def handle(conn):
    upstream = None
    try:
        conn.settimeout(30)
        data = read_head(conn)
        head, _, rest = data.partition(b"\r\n\r\n")
        words = head.split(b"\r\n", 1)[0].decode("latin-1").split()
        if len(words) < 3:
            log({"kind": "malformed"})
            return
        method, target = words[0].upper(), words[1]
        if method == "CONNECT":
            host, port = split(target, 443)
            label = f"{host}:{port}"
            route = "recorder" if alternate(host) else "upstream"
            log({"kind": "connect", "target": label, "route": route})
            payload = rest
        else:
            netloc = urlsplit(target).netloc
            host, port = split(netloc, 80) if netloc else ("", 80)
            label = f"{host}:{port}"
            route = "recorder" if alternate(host) else "upstream"
            log({"kind": "http", "method": method[:16], "host": label, "route": route})
            if not host:
                conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                return
            payload = data
        address = RECORDER if route == "recorder" else (host, port)
        try:
            upstream = socket.create_connection(address, timeout=30)
        except OSError as exc:
            log({"kind": "upstream_error", "target": label, "error": type(exc).__name__})
            conn.sendall(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            return
        if method == "CONNECT":
            conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        if payload:
            upstream.sendall(payload)
        conn.settimeout(None)
        upstream.settimeout(None)
        back = threading.Thread(target=pipe, args=(upstream, conn), daemon=True)
        back.start()
        pipe(conn, upstream)
        back.join()
    except Exception as exc:
        log({"kind": "error", "error": type(exc).__name__})
    finally:
        for sock in (upstream, conn):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    open(LOG, "a", encoding="utf-8").close()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", PORT))
    server.listen(128)
    open(os.path.join(LOG_DIR, "proxy.ready"), "w", encoding="utf-8").close()
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


main()
'''

RECORDER_PY = r'''
"""Alternate endpoint stand-in: answers 200 to HTTP and counts every connection."""
import json
import os
import socket
import threading
import time

LOG_DIR = os.environ.get("PROBE_LOG_DIR", "/probe-logs")
PORT = int(os.environ["PROBE_RECORDER_PORT"])
STATE = os.path.join(LOG_DIR, "recorder.json")
LOCK = threading.Lock()
RECORDS = []


def save():
    partial = STATE + ".partial"
    with open(partial, "w", encoding="utf-8") as handle:
        json.dump({"count": len(RECORDS), "requests": RECORDS}, handle, sort_keys=True)
    os.replace(partial, STATE)


def handle(conn):
    data = b""
    try:
        conn.settimeout(5)
        try:
            while b"\r\n\r\n" not in data and len(data) < 65536:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
        except OSError:
            pass
        lines = data.split(b"\r\n")
        words = lines[0].decode("latin-1", "replace").split()
        if len(words) >= 3 and words[2].startswith("HTTP/"):
            host = ""
            for raw in lines[1:]:
                name, _, value = raw.partition(b":")
                if name.strip().lower() == b"host":
                    host = value.strip().decode("latin-1", "replace")[:200]
            record = {"kind": "http", "method": words[0][:16], "host": host}
            try:
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Content-Length: 2\r\nConnection: close\r\n\r\n{}"
                )
            except OSError:
                pass
        else:
            record = {"kind": "opaque", "bytes": len(data)}
        record["t"] = round(time.time(), 3)
        with LOCK:
            RECORDS.append(record)
            save()
    finally:
        conn.close()


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    with LOCK:
        save()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", PORT))
    server.listen(128)
    open(os.path.join(LOG_DIR, "recorder.ready"), "w", encoding="utf-8").close()
    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


main()
'''

SINK_PY = r'''
"""The runner's progress port stand-in: records the per-model usage report bodies."""
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG_DIR = os.environ.get("PROBE_LOG_DIR", "/probe-logs")
PORT = int(os.environ["PROBE_SINK_PORT"])
TOKEN = os.environ.get("PROBE_SINK_TOKEN", "").encode()
LOG = os.path.join(LOG_DIR, "sink.jsonl")
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(min(length, 1048576)) if length > 0 else b""
        presented = (self.headers.get("X-API-Key") or "").encode()
        token_ok = bool(TOKEN) and hmac.compare_digest(presented, TOKEN)
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = None
        usage = self.path.rstrip("/").endswith("/usage")
        entry = {
            "kind": "usage" if usage else "other",
            "path": self.path[:200],
            "token_ok": token_ok,
            "usage": body if usage and isinstance(body, dict) else None,
            "t": round(time.time(), 3),
        }
        with LOCK, open(LOG, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")
        payload = b'{"ok":true}'
        self.send_response(200 if usage else 202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass


def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    open(LOG, "a", encoding="utf-8").close()
    server = Server(("127.0.0.1", PORT), Handler)
    open(os.path.join(LOG_DIR, "sink.ready"), "w", encoding="utf-8").close()
    server.serve_forever()


main()
'''

# The bundle's PreToolUse command hook. It records the tool and whether the
# command carried the marker, never the command text.
MARK_TOOL_PY = r"""
import json
import os
import sys
import time

directory = os.environ.get("LIVE_PROBE_MARKER_DIR", "/tmp/live-probe")
try:
    payload = json.load(sys.stdin)
except ValueError:
    payload = {}
if not isinstance(payload, dict):
    payload = {}
tool_input = payload.get("tool_input")
command = tool_input.get("command", "") if isinstance(tool_input, dict) else ""
os.makedirs(directory, exist_ok=True)
line = {
    "marker": "live-probe-hook",
    "event": payload.get("hook_event_name"),
    "tool": payload.get("tool_name"),
    "command_has_marker": "live-tool-ok" in str(command),
    "t": round(time.time(), 3),
}
with open(os.path.join(directory, "hook-marker.jsonl"), "a", encoding="utf-8") as handle:
    handle.write(json.dumps(line, sort_keys=True) + "\n")
sys.exit(0)
"""

# Stages the bundle into a writable emptyDir, as the chart's bundle init pair
# does, and seeds the inherited config dirs the conflict pods name.
STAGE_PY = r"""
import os
import shutil
from pathlib import Path

seed = Path(os.environ.get("STAGE_SEED_DIR", "/seed"))
shutil.copytree(seed / "live-probe", Path(os.environ["STAGE_BUNDLE_DIR"]))
if os.environ.get("STAGE_CONFLICTS") == "1":
    claude = Path(os.environ["STAGE_CLAUDE_CONFIG_DIR"])
    claude.mkdir(mode=0o700, parents=True)
    shutil.copyfile(seed / "conflict" / "settings.json", claude / "settings.json")
    saved = claude / ".credentials.json"
    shutil.copyfile(seed / "conflict" / "credentials.json", saved)
    saved.chmod(0o600)
    store = Path(os.environ["STAGE_SECURE_STORAGE_DIR"])
    store.mkdir(mode=0o700, parents=True)
    stored = store / ".credentials.json"
    shutil.copyfile(seed / "conflict" / "credentials.json", stored)
    stored.chmod(0o600)
    profiles = Path(os.environ["STAGE_ANTHROPIC_CONFIG_DIR"]) / "configs"
    profiles.mkdir(mode=0o700, parents=True)
    shutil.copyfile(seed / "conflict" / "alt-profile.json", profiles / "alt-profile.json")
"""

# Read in the runner container after the turn. Prints counts, model ids and
# booleans only; no transcript text leaves the pod. The native CLI writes one
# JSONL line per content block under <config dir>/projects, repeating the API
# message id, and subagent turns either under .../subagents/ or flagged
# isSidechain, so unique ids per role count assistant turns.
TRANSCRIPT_SUMMARY_PY = r"""
import json
import os
import sys

MARK = "live-tool-ok"
roles = {"implementer": {}, "reviewer": {}}
roots = set()
files = synthetic = unparsed = 0
bash_calls = set()
results = []
for base in sys.argv[1:]:
    for dirpath, _dirs, names in os.walk(base):
        for name in names:
            if not name.endswith(".jsonl"):
                continue
            path = os.path.join(dirpath, name)
            parts = path.split(os.sep)
            if "projects" not in parts:
                continue
            index = parts.index("projects")
            try:
                handle = open(path, encoding="utf-8", errors="replace")
            except OSError:
                continue
            files += 1
            roots.add(os.sep.join(parts[:index]) or os.sep)
            nested = "subagents" in parts[index:]
            with handle:
                for raw in handle:
                    try:
                        entry = json.loads(raw)
                    except ValueError:
                        unparsed += 1
                        continue
                    if not isinstance(entry, dict) or not isinstance(entry.get("message"), dict):
                        continue
                    message = entry["message"]
                    side = nested or entry.get("isSidechain") is True
                    role = "reviewer" if side else "implementer"
                    content = message.get("content")
                    blocks = content if isinstance(content, list) else []
                    if entry.get("type") == "assistant":
                        model = str(message.get("model") or "")
                        if model == "<synthetic>":
                            synthetic += 1
                            continue
                        key = str(message.get("id") or entry.get("uuid") or len(roles[role]))
                        usage = message.get("usage")
                        usage = usage if isinstance(usage, dict) else {}
                        out = usage.get("output_tokens")
                        out = out if isinstance(out, int) and not isinstance(out, bool) else 0
                        prior = roles[role].get(key)
                        roles[role][key] = (model, max(out, prior[1] if prior else 0))
                        for block in blocks:
                            if (
                                isinstance(block, dict)
                                and block.get("type") == "tool_use"
                                and block.get("name") == "Bash"
                            ):
                                bash_calls.add(str(block.get("id")))
                    elif entry.get("type") == "user":
                        for block in blocks:
                            if isinstance(block, dict) and block.get("type") == "tool_result":
                                marked = MARK in json.dumps(block.get("content"))
                                results.append((str(block.get("tool_use_id")), marked))
summary = {
    "files": files,
    "roots": sorted(roots),
    "synthetic": synthetic,
    "unparsed_lines": unparsed,
    "bash_results_with_marker": sum(1 for call, marked in results if marked and call in bash_calls),
}
for role, seen in roles.items():
    models = {}
    for model, _out in seen.values():
        models[model] = models.get(model, 0) + 1
    summary[role] = {
        "messages": len(seen),
        "models": models,
        "output_tokens": sum(out for _model, out in seen.values()),
    }
print(json.dumps(summary, sort_keys=True))
"""


# --- Conflict inputs (placeholders only) -----------------------------------------
#
# https://code.claude.com/docs/en/authentication orders the sources the CLI
# would otherwise prefer over CLAUDE_CODE_OAUTH_TOKEN: a cloud provider selector,
# ANTHROPIC_AUTH_TOKEN, ANTHROPIC_API_KEY, an apiKeyHelper, and the saved login in
# the config dir's .credentials.json on Linux. Every alternate URL below is a
# recorder: http://127.0.0.1:<recorder> directly, or an example.com host that the
# proxy logs and routes to the recorder.

ALT_BASE = "https://alt-base.example.com"
ALT_BEDROCK = "https://alt-bedrock.example.com"
ALT_FOUNDRY = "https://alt-foundry.example.com"
ALT_GATEWAY = "https://alt-gateway.example.com"
ALT_PROFILE = "https://alt-profile.example.com"
PLACEHOLDER_ORG = "00000000-0000-0000-0000-000000000000"

CONFLICT_ENV: tuple[tuple[str, str], ...] = (
    ("CLAUDE_CODE_USE_BEDROCK", "1"),
    ("CLAUDE_CODE_USE_VERTEX", "1"),
    ("CLAUDE_CODE_USE_FOUNDRY", "1"),
    ("AWS_ACCESS_KEY_ID", "placeholder-access-key-id"),
    ("AWS_SECRET_ACCESS_KEY", "placeholder-secret-access-key"),
    ("AWS_SESSION_TOKEN", "placeholder-session-token"),
    ("AWS_REGION", "us-east-1"),
    ("AWS_BEARER_TOKEN_BEDROCK", "placeholder-bedrock-bearer"),
    ("CLAUDE_CODE_SKIP_BEDROCK_AUTH", "1"),
    ("ANTHROPIC_BEDROCK_BASE_URL", ALT_BEDROCK),
    ("ANTHROPIC_VERTEX_PROJECT_ID", "placeholder-project"),
    ("CLOUD_ML_REGION", "us-east5"),
    ("ANTHROPIC_VERTEX_BASE_URL", f"{RECORDER_URL}/vertex"),
    ("ANTHROPIC_FOUNDRY_API_KEY", "placeholder-foundry-key"),
    ("ANTHROPIC_FOUNDRY_BASE_URL", ALT_FOUNDRY),
    ("ANTHROPIC_API_KEY", "sk-ant-api03-placeholder-not-a-key"),
    ("ANTHROPIC_AUTH_TOKEN", "placeholder-auth-token"),
    ("ANTHROPIC_CUSTOM_HEADERS", "X-Live-Probe-Placeholder: placeholder"),
    ("ANTHROPIC_BASE_URL", ALT_BASE),
    ("CURIE_MODEL_BASE_URL", f"{RECORDER_URL}/curie-model"),
    ("CURIE_CREDENTIALS", "sk-ant-api03-placeholder-service-key"),
    ("CURIE_MODEL_ENV_KEY", "ACME_MODEL_KEY"),
    ("ACME_MODEL_KEY", "sk-ant-api03-placeholder-declared-key"),
    ("ANTHROPIC_PROFILE", "alt-profile"),
    ("ANTHROPIC_CONFIG_DIR", INHERITED_ANTHROPIC_CONFIG_DIR),
    ("ANTHROPIC_IDENTITY_TOKEN", "placeholder-identity-token"),
    ("ANTHROPIC_FEDERATION_RULE_ID", "placeholder-federation-rule"),
    ("ANTHROPIC_ORGANIZATION_ID", PLACEHOLDER_ORG),
    ("ANTHROPIC_WORKSPACE_ID", "placeholder-workspace"),
    ("CLAUDE_CODE_USE_GATEWAY", "1"),
    ("CLAUDE_CODE_GATEWAY_TOKEN", "placeholder-gateway-token"),
    ("CLAUDE_CONFIG_DIR", INHERITED_CLAUDE_CONFIG_DIR),
    ("CLAUDE_SECURESTORAGE_CONFIG_DIR", INHERITED_SECURE_STORAGE_DIR),
    ("CLAUDE_CODE_SUBAGENT_MODEL_FORCE", "1"),
    ("CLAUDE_CODE_SUBAGENT_MODEL", FORCED_SUBAGENT_MODEL),
)

# User settings in the inherited config dir. The helper leaves a marker if the
# CLI ever runs it.
CONFLICT_SETTINGS: dict[str, Any] = {
    "apiKeyHelper": (
        f"mkdir -p {MARKER_DIR} && echo ran >> {HELPER_MARKER} && echo placeholder-helper-key"
    ),
    "forceLoginGatewayUrl": ALT_GATEWAY,
    "env": {"ANTHROPIC_BASE_URL": ALT_BASE},
}
# Saved login and signed-in gateway session, where the authentication docs say
# the CLI stores credentials on Linux. The gateway record is the shape the CLI
# 2.1.301 startup restore reads: ``enterpriseGateway`` with ``url``, ``jwt`` and
# ``expiresAt`` (ms), and ``gatewayTrust`` keyed by the URL's hostname holding a
# TLS certificate SHA-256 as 64 lowercase hex characters. Without the trust
# entry, or once expired, the restore skips the session. The gateway host here
# reaches only the recorder, which does not speak TLS, so the restore's pin probe
# fails and the CLI proceeds without re-verifying (only a completed probe with a
# different fingerprint skips). Observed offline on CLI 2.1.301 with an
# unreachable gateway and this placeholder pin: the session is installed, then
# the CLI exits with "Couldn't load settings from Cloud gateway". A leak shows as
# a CONNECT to the gateway and a failed turn.
CONFLICT_CREDENTIALS: dict[str, Any] = {
    "claudeAiOauth": {
        "accessToken": "placeholder-saved-access-token",
        "refreshToken": "placeholder-saved-refresh-token",
        "expiresAt": 4102444800000,
        "scopes": ["user:inference", "user:profile"],
        "subscriptionType": "max",
    },
    "enterpriseGateway": {
        "url": ALT_GATEWAY,
        "jwt": "placeholder-gateway-session-jwt",
        "expiresAt": 4102444800000,
    },
    "gatewayTrust": {"alt-gateway.example.com": "0123456789abcdef" * 4},
}
# A named profile with federation, at <ANTHROPIC_CONFIG_DIR>/configs/<name>.json,
# the layout the pinned CLI resolves.
CONFLICT_PROFILE: dict[str, Any] = {
    "base_url": ALT_PROFILE,
    "organization_id": PLACEHOLDER_ORG,
    "authentication": {
        "type": "oidc_federation",
        "federation_rule_id": "placeholder-federation-rule",
        "identity_token": "placeholder-identity-token",
    },
}
# https://code.claude.com/docs/en/managed-settings: on Linux the CLI reads
# mandatory policy from /etc/claude-code/managed-settings.json. Forcing the
# gateway login is policy the runner must refuse, never bypass.
MANAGED_GATEWAY_POLICY: dict[str, Any] = {
    "forceLoginMethod": "gateway",
    "forceLoginGatewayUrl": ALT_GATEWAY,
}


# --- Small helpers -----------------------------------------------------------------


class PrerequisiteError(RuntimeError):
    """A live prerequisite is missing or failing; raised from the fixture as a setup error."""


class StepSetupError(RuntimeError):
    """A step's pod could not be prepared; no prompt was sent."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


class _Secret:
    """Holds a value that must never reach a repr, a log or an assertion message."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "<redacted>"

    __str__ = __repr__


def _value_env(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": value}


def _secret_env(name: str, secret: str, key: str) -> dict[str, Any]:
    return {
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": secret, "key": key, "optional": False}},
    }


def _as_int(raw: object) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        return 0
    return max(raw, 0)


def _same_model(observed: str, expected: str) -> bool:
    """An observed model id names the expected one, allowing a context or date suffix."""

    base = re.sub(r"\[[^\[\]]*\]$", "", observed)
    return base == expected or re.fullmatch(re.escape(expected) + r"-\d{8}", base) is not None


def _is_alternate_host(host: str) -> bool:
    host = host.lower().rstrip(".")
    return (
        host == "example.com"
        or host.endswith(".example.com")
        or host in ("127.0.0.1", "localhost", "::1")
    )


def _host_of(target: str) -> str:
    return target.rsplit(":", 1)[0].strip("[]").lower()


def _is_first_party(host: str) -> bool:
    return host == "anthropic.com" or host.endswith(".anthropic.com")


def _jsonl(text: str | None) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for raw in (text or "").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except ValueError:
            continue
        if isinstance(parsed, dict):
            entries.append(parsed)
    return entries


# --- Turn driving over port-forward ------------------------------------------------


@dataclass
class TurnResult:
    events: list[dict[str, Any]] = field(default_factory=list)
    wall_seconds: float = 0.0
    timed_out: bool = False
    stopped_reason: str | None = None
    transport_error: str | None = None
    http_status: int | None = None

    @property
    def final(self) -> dict[str, Any] | None:
        finals = [event for event in self.events if event.get("type") == "final"]
        return finals[-1] if finals else None

    def of_type(self, kind: str) -> list[dict[str, Any]]:
        return [event for event in self.events if event.get("type") == kind]


def _http(
    port: int, method: str, path: str, *, token: str | None, body: object | None = None
) -> tuple[int, bytes]:
    # http.client never consults proxy env, so the port-forward is dialled directly.
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        headers: dict[str, str] = {}
        payload = None
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def _stream_turn(port: int, token: str, text: str, ceiling: Ceiling) -> TurnResult:
    """POST one ACI event and read its NDJSON stream to the final, within the ceiling.

    Sent exactly once: a failure is reported, never retried. Past the wall
    ceiling, or once more tool calls start than the turn ceiling allows, the
    turn is interrupted through the runner API.
    """

    result = TurnResult()
    frame = {
        "kind": "event",
        "type": "message",
        "text": text,
        "user": "live-probe",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    started = time.monotonic()
    deadline = started + ceiling.wall_seconds
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    sock: socket.socket | None = None
    try:
        conn.connect()
        sock = conn.sock
        conn.request(
            "POST",
            "/v1/event",
            body=json.dumps(frame).encode("utf-8"),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        response = conn.getresponse()
        result.http_status = response.status
        if response.status != 200:
            result.transport_error = f"POST /v1/event answered {response.status}"
            response.read()
            return result
        if sock is not None:
            sock.settimeout(None)
        lines: queue.Queue[tuple[str, bytes]] = queue.Queue()

        def read() -> None:
            try:
                while True:
                    line = response.readline()
                    if not line:
                        lines.put(("eof", b""))
                        return
                    lines.put(("line", line))
            except Exception as exc:  # noqa: BLE001 - any read failure ends the stream
                lines.put(("error", type(exc).__name__.encode()))

        threading.Thread(target=read, daemon=True).start()
        tool_calls = 0
        stop_deadline: float | None = None
        while True:
            now = time.monotonic()
            if stop_deadline is None and now >= deadline:
                result.timed_out = True
                stop_deadline = _stop(port, token, result, "wall ceiling reached", now)
            limit = stop_deadline if stop_deadline is not None else deadline
            if stop_deadline is not None and now >= stop_deadline:
                break
            try:
                kind, raw = lines.get(timeout=max(min(limit - now, 5.0), 0.05))
            except queue.Empty:
                continue
            if kind != "line":
                if result.final is None:
                    result.transport_error = f"event stream ended ({kind}: {raw.decode()})"
                break
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            result.events.append(event)
            if event.get("type") == "tool_note":
                tool_calls += 1
                if ceiling.turns and tool_calls > ceiling.turns and stop_deadline is None:
                    stop_deadline = _stop(
                        port, token, result, "more tool calls than the turn ceiling", now
                    )
            if event.get("type") == "final":
                break
    except (OSError, http.client.HTTPException) as exc:
        result.transport_error = f"runner API transport failure: {type(exc).__name__}"
    finally:
        result.wall_seconds = round(time.monotonic() - started, 3)
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
        conn.close()
    return result


def _stop(port: int, token: str, result: TurnResult, reason: str, now: float) -> float:
    """Interrupt the open turn and give its final a short grace to arrive."""

    result.stopped_reason = reason
    with contextlib.suppress(OSError, http.client.HTTPException):
        _http(
            port,
            "POST",
            "/v1/interrupt",
            token=token,
            body={"kind": "interrupt", "reason": f"live test: {reason}"},
        )
    return now + 60.0


class _PortForward:
    """A bounded ``kubectl port-forward`` to one pod's runner port."""

    def __init__(self, live: LiveCluster, pod: str) -> None:
        self._live = live
        self._pod = pod
        self._process: subprocess.Popen[str] | None = None
        self.port = 0

    def __enter__(self) -> _PortForward:
        process = subprocess.Popen(
            self._live.kubectl_argv(
                "port-forward",
                "--address",
                "127.0.0.1",
                f"pod/{self._pod}",
                f":{RUNNER_PORT}",
                namespaced=True,
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=self._live.child_env,
        )
        self._process = process
        self._live.track(process)
        lines: queue.Queue[str | None] = queue.Queue()

        def drain() -> None:
            # Keep draining for the forward's whole life: a full pipe blocks kubectl.
            assert process.stdout is not None
            for line in process.stdout:
                lines.put(line)
            lines.put(None)

        threading.Thread(target=drain, daemon=True).start()
        deadline = time.monotonic() + 45
        seen: list[str] = []
        while not self.port:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.__exit__(None, None, None)
                raise StepSetupError("port-forward did not start", "\n".join(seen))
            try:
                line = lines.get(timeout=remaining)
            except queue.Empty:
                continue
            if line is None:
                self.__exit__(None, None, None)
                raise StepSetupError("port-forward exited", "\n".join(seen))
            seen.append(line.rstrip())
            match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+) ->", line)
            if match:
                self.port = int(match.group(1))
        return self

    def __exit__(self, *_exc: object) -> None:
        process = self._process
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


# --- Steps ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StepPlan:
    key: str
    pod: str
    prompt: str
    ceiling: Ceiling
    personal: bool
    grant: bool
    conflicts: bool
    managed_policy: bool
    expected: Mapping[str, Any]


CONTROL_STEP = StepPlan(
    key="service_control",
    pod="live-control",
    prompt=CONTROL_PROMPT,
    ceiling=CONTROL_CEILING,
    personal=False,
    grant=True,
    conflicts=False,
    managed_policy=False,
    expected={
        "final_status": "done",
        "implementer_model": IMPLEMENTER_MODEL,
        "max_assistant_turns": CONTROL_CEILING.turns,
        "max_output_tokens": CONTROL_CEILING.output_tokens,
        "max_wall_seconds": CONTROL_CEILING.wall_seconds,
        "connect_targets": [PROVIDER_TARGET],
        "recorder_count": 0,
    },
)
PERSONAL_STEP = StepPlan(
    key="personal_session",
    pod="live-personal",
    prompt=PERSONAL_PROMPT,
    ceiling=PERSONAL_CEILING,
    personal=True,
    grant=True,
    conflicts=True,
    managed_policy=False,
    expected={
        "final_status": "done",
        "implementer_model": IMPLEMENTER_MODEL,
        "reviewer_model": REVIEWER_MODEL,
        "forced_subagent_model_unused": FORCED_SUBAGENT_MODEL,
        "reviewer_roles": list(REVIEWER_ROLES),
        "bash_result_marker": TOOL_MARKER,
        "hook_marker": True,
        "api_key_helper_ran": False,
        "max_assistant_turns": PERSONAL_CEILING.turns,
        "max_output_tokens": PERSONAL_CEILING.output_tokens,
        "max_wall_seconds": PERSONAL_CEILING.wall_seconds,
        "connect_targets": [PROVIDER_TARGET],
        "alternate_requests": 0,
        "recorder_count": 0,
    },
)
NO_GRANT_STEP = StepPlan(
    key="negative_control_no_grant",
    pod="live-no-grant",
    prompt=REFUSAL_PROMPT,
    ceiling=REFUSAL_CEILING,
    personal=True,
    grant=False,
    conflicts=True,
    managed_policy=False,
    expected={
        "error_contains": "authentication required",
        "final_status": "classified-failure",
        "proxy_entries": 0,
        "recorder_count": 0,
        "usage_reports": 0,
        "native_transcripts": 0,
    },
)
MANAGED_STEP = StepPlan(
    key="forced_gateway_managed_policy",
    pod="live-managed-gateway",
    prompt=REFUSAL_PROMPT,
    ceiling=REFUSAL_CEILING,
    personal=True,
    grant=True,
    conflicts=False,
    managed_policy=True,
    expected={
        "error": "refused before launch, not authentication required",
        "final_status": "classified-failure",
        "proxy_entries": 0,
        "recorder_count": 0,
        "usage_reports": 0,
        "native_transcripts": 0,
    },
)


@dataclass
class StepRun:
    plan: StepPlan
    setup_reason: str | None = None
    setup_detail: str = ""
    image_check: list[dict[str, Any]] = field(default_factory=list)
    turn: TurnResult | None = None
    proxy: list[dict[str, Any]] = field(default_factory=list)
    recorder: dict[str, Any] = field(default_factory=dict)
    sink: list[dict[str, Any]] = field(default_factory=list)
    transcripts: dict[str, Any] = field(default_factory=dict)
    hook_lines: list[dict[str, Any]] = field(default_factory=list)
    helper_ran: bool | None = None
    # Short, test-authored reasons (evidence) and their tool output (terminal only).
    observe_errors: list[str] = field(default_factory=list)
    observe_details: list[str] = field(default_factory=list)
    pod_deleted: bool | None = None

    # -- derived views --

    def usage(self) -> list[dict[str, Any]]:
        keys = ("input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens")
        totals: dict[tuple[str, str], dict[str, int]] = {}
        for entry in self.sink:
            body = entry.get("usage")
            if entry.get("kind") != "usage" or not isinstance(body, dict):
                continue
            for item in body.get("models") or ():
                if not isinstance(item, dict):
                    continue
                bucket = totals.setdefault(
                    (str(item.get("role") or "unknown"), str(item.get("model") or "")),
                    dict.fromkeys(keys, 0),
                )
                for key in keys:
                    bucket[key] += _as_int(item.get(key))
        return [
            {"role": role, "model": model, **counts}
            for (role, model), counts in sorted(totals.items())
        ]

    def usage_posts(self) -> list[dict[str, Any]]:
        return [entry for entry in self.sink if entry.get("kind") == "usage"]

    def connect_targets(self) -> list[str]:
        return sorted(
            {str(entry.get("target")) for entry in self.proxy if entry.get("kind") == "connect"}
        )

    def http_hosts(self) -> list[str]:
        return sorted(
            {str(entry.get("host")) for entry in self.proxy if entry.get("kind") == "http"}
        )

    def recorder_count(self) -> int:
        return _as_int(self.recorder.get("count"))

    def assistant_turns(self) -> int | None:
        if not self.transcripts.get("files"):
            return None
        return sum(
            _as_int((self.transcripts.get(role) or {}).get("messages"))
            for role in ("implementer", "reviewer")
        )

    def output_tokens(self) -> int | None:
        usage = self.usage()
        final = self.turn.final if self.turn is not None else None
        final_output = _as_int(final.get("output_tokens")) if final else 0
        if not usage and final_output == 0:
            return None
        return max(sum(_as_int(item["output_tokens"]) for item in usage), final_output)

    def side_effects(self) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        opened: list[dict[str, Any]] = []
        closed: dict[str, dict[str, Any]] = {}
        for event in self.turn.of_type("side_effect_flag") if self.turn is not None else ():
            if isinstance(event.get("arguments"), dict):
                opened.append(event)
            elif event.get("call_id") and "failed" in event:
                closed[str(event["call_id"])] = event
        return opened, closed

    def bash_probe(self) -> dict[str, Any]:
        opened, closed = self.side_effects()
        calls = [
            event
            for event in opened
            if event.get("tool") == "Bash"
            and TOOL_MARKER in str((event.get("arguments") or {}).get("command", ""))
        ]
        closings = [closed.get(str(event.get("call_id"))) for event in calls]
        return {
            "calls": len(calls),
            "closed_without_failure": any(
                closing is not None and closing.get("failed") is False for closing in closings
            ),
            "wire_result_has_marker": any(
                closing is not None and TOOL_MARKER in json.dumps(closing.get("result"))
                for closing in closings
            ),
            "transcript_results_with_marker": _as_int(
                self.transcripts.get("bash_results_with_marker")
            ),
        }

    def reviewer_calls(self) -> dict[str, dict[str, Any]]:
        opened, closed = self.side_effects()
        found: dict[str, dict[str, Any]] = {}
        for role in REVIEWER_ROLES:
            calls = [
                event
                for event in opened
                if event.get("tool") in ("Agent", "Task")
                and str((event.get("arguments") or {}).get("subagent_type", "")).split(":")[-1]
                == role
            ]
            closings = [closed.get(str(event.get("call_id"))) for event in calls]
            found[role] = {
                "calls": len(calls),
                "closed_without_failure": any(
                    closing is not None and closing.get("failed") is False for closing in closings
                ),
                "subagent_types": sorted(
                    {str((event.get("arguments") or {}).get("subagent_type")) for event in calls}
                ),
            }
        return found

    def observed(self, redact: Callable[[str], str]) -> dict[str, Any]:
        """The sanitized evidence view: counts, ids, statuses and short messages only."""

        turn = self.turn
        final = turn.final if turn is not None else None
        errors = [
            redact(str(event.get("message", "")))[:300]
            for event in (turn.of_type("error") if turn is not None else ())
        ]
        targets = self.connect_targets()
        tool_notes: dict[str, int] = {}
        for event in turn.of_type("tool_note") if turn is not None else ():
            name = str(event.get("tool"))
            tool_notes[name] = tool_notes.get(name, 0) + 1
        observed: dict[str, Any] = {
            "pod": self.plan.pod,
            "setup_failed": self.setup_reason is not None,
            "setup_reason": self.setup_reason,
            "image_ids_match": bool(self.image_check)
            and all(item["matches"] for item in self.image_check),
            "containers_checked": [item["container"] for item in self.image_check],
            "final_status": final.get("status") if final else None,
            "final_input_tokens": final.get("input_tokens") if final else None,
            "final_output_tokens": final.get("output_tokens") if final else None,
            "error_messages": errors,
            "tool_calls": tool_notes,
            "wall_seconds": turn.wall_seconds if turn is not None else None,
            "timed_out": turn.timed_out if turn is not None else None,
            "stopped_reason": turn.stopped_reason if turn is not None else None,
            "transport_error": turn.transport_error if turn is not None else None,
            "connect_targets": targets,
            "auxiliary_first_party_targets": [
                target
                for target in targets
                if target != PROVIDER_TARGET and _is_first_party(_host_of(target))
            ],
            "plain_http_hosts": self.http_hosts(),
            "proxy_entries": len(self.proxy),
            "upstream_errors": sum(
                1 for entry in self.proxy if entry.get("kind") == "upstream_error"
            ),
            "recorder_count": self.recorder_count(),
            "recorder_hosts": sorted(
                {
                    str(item.get("host") or item.get("kind"))
                    for item in self.recorder.get("requests") or ()
                    if isinstance(item, dict)
                }
            ),
            "usage_reports": len(self.usage_posts()),
            "usage_reports_authenticated": all(
                entry.get("token_ok") is True for entry in self.usage_posts()
            ),
            "usage_by_role_and_model": self.usage(),
            "output_tokens_observed": self.output_tokens(),
            "assistant_turns_observed": self.assistant_turns(),
            "native_transcripts": {
                key: self.transcripts.get(key)
                for key in (
                    "files",
                    "roots",
                    "implementer",
                    "reviewer",
                    "synthetic",
                    "bash_results_with_marker",
                )
            },
            "hook_marker_lines": len(self.hook_lines),
            "hook_marked_bash": any(
                line.get("tool") == "Bash" and line.get("command_has_marker") is True
                for line in self.hook_lines
            ),
            "api_key_helper_ran": self.helper_ran,
            "bash_probe": self.bash_probe(),
            "reviewer_calls": self.reviewer_calls(),
            "observation_errors": [redact(error)[:200] for error in self.observe_errors],
            "pod_deleted": self.pod_deleted,
        }
        if observed["auxiliary_first_party_targets"]:
            observed["auxiliary_note"] = (
                "First-party Anthropic hosts other than the Messages host were observed. They "
                "are Anthropic-operated CLI service traffic "
                "(https://code.claude.com/docs/en/network-config), allowed only because they "
                "were observed; no alternate endpoint received a request."
            )
        return observed


# --- Judges ------------------------------------------------------------------------------


def _setup_failures(run: StepRun) -> list[str]:
    if run.setup_reason is not None:
        return [f"setup failed before any prompt: {run.setup_reason}"]
    turn = run.turn
    if turn is None:
        return ["no turn was sent"]
    failures: list[str] = []
    if turn.transport_error:
        failures.append(turn.transport_error)
    if turn.timed_out:
        failures.append(f"wall ceiling {run.plan.ceiling.wall_seconds:.0f} s exceeded")
    if turn.stopped_reason and not turn.timed_out:
        failures.append(f"turn stopped: {turn.stopped_reason}")
    if turn.final is None:
        failures.append("no final event")
    failures.extend(run.observe_errors)
    return failures


def _ceiling_failures(run: StepRun) -> list[str]:
    ceiling = run.plan.ceiling
    failures: list[str] = []
    turns = run.assistant_turns()
    if turns is None:
        failures.append("missing proof: turn count (no native transcript found)")
    elif turns > ceiling.turns:
        failures.append(f"assistant turns {turns} exceed the ceiling {ceiling.turns}")
    elif turns < 1:
        failures.append("no assistant turn was recorded")
    output = run.output_tokens()
    if output is None:
        failures.append("missing proof: output token usage")
    elif output > ceiling.output_tokens:
        failures.append(f"output tokens {output} exceed the ceiling {ceiling.output_tokens}")
    if run.turn is not None and run.turn.wall_seconds > ceiling.wall_seconds:
        failures.append(
            f"wall seconds {run.turn.wall_seconds} exceed the ceiling {ceiling.wall_seconds}"
        )
    return failures


def _endpoint_failures(run: StepRun) -> list[str]:
    """Model traffic tunnelled only to the first-party host; nothing reached an alternate."""

    failures: list[str] = []
    targets = run.connect_targets()
    if PROVIDER_TARGET not in targets:
        failures.append(f"no CONNECT to {PROVIDER_TARGET} was observed")
    alternates = [target for target in targets if _is_alternate_host(_host_of(target))]
    if alternates:
        failures.append(f"CONNECT reached alternate endpoints {alternates}")
    foreign = [
        target
        for target in targets
        if not _is_alternate_host(_host_of(target)) and not _is_first_party(_host_of(target))
    ]
    if foreign:
        failures.append(f"CONNECT reached hosts outside anthropic.com {foreign}")
    off_port = [
        target
        for target in targets
        if _is_first_party(_host_of(target)) and not target.endswith(":443")
    ]
    if off_port:
        failures.append(f"first-party CONNECT on a port other than 443 {off_port}")
    if run.http_hosts():
        failures.append(f"plain HTTP requests were sent to {run.http_hosts()}")
    if run.recorder_count() != 0:
        failures.append(f"the alternate endpoint recorder saw {run.recorder_count()} connections")
    return failures


def _no_traffic_failures(run: StepRun) -> list[str]:
    """A refused launch made no connection and no model call at all."""

    failures: list[str] = []
    if run.proxy:
        failures.append(
            f"the proxy saw {len(run.proxy)} entries (targets {run.connect_targets()}, "
            f"plain HTTP {run.http_hosts()})"
        )
    if run.recorder_count() != 0:
        failures.append(f"the alternate endpoint recorder saw {run.recorder_count()} connections")
    spent = [item for item in run.usage() if item["output_tokens"] or item["input_tokens"]]
    if spent:
        failures.append(f"model usage was reported {spent}")
    if run.transcripts.get("files"):
        failures.append("the native CLI wrote a session transcript")
    if run.helper_ran:
        failures.append("the inherited apiKeyHelper ran")
    return failures


def _usage_model_failures(run: StepRun, *, reviewers: bool) -> list[str]:
    failures: list[str] = []
    usage = run.usage()
    posts = run.usage_posts()
    if not posts:
        return ["missing proof: per-model usage report"]
    if not all(entry.get("token_ok") is True for entry in posts):
        failures.append("a usage report did not carry the progress token")
    implementer = [
        item
        for item in usage
        if item["role"] == "implementer" and _same_model(item["model"], IMPLEMENTER_MODEL)
    ]
    if not any(item["output_tokens"] > 0 for item in implementer):
        failures.append(
            f"usage shows no implementer output on {IMPLEMENTER_MODEL}: "
            f"{[(item['role'], item['model']) for item in usage]}"
        )
    unexpected = [item["model"] for item in usage if not item["model"].startswith("claude-")]
    if unexpected:
        failures.append(f"usage names non-Claude models {unexpected}")
    reviewer = [item for item in usage if item["role"] == "reviewer"]
    if reviewers:
        if not reviewer or not any(item["output_tokens"] > 0 for item in reviewer):
            failures.append("usage shows no reviewer output")
        wrong = [
            item["model"] for item in reviewer if not _same_model(item["model"], REVIEWER_MODEL)
        ]
        if wrong:
            failures.append(f"reviewer usage names {wrong}, not {REVIEWER_MODEL}")
    return failures


def _transcript_model_failures(run: StepRun, *, reviewers: bool) -> list[str]:
    """The native transcript's per-message model for each role."""

    failures: list[str] = []
    implementer = run.transcripts.get("implementer") or {}
    models = implementer.get("models") or {}
    if not models:
        failures.append("the native transcript shows no implementer message")
    wrong = [model for model in models if not _same_model(model, IMPLEMENTER_MODEL)]
    if wrong:
        failures.append(f"implementer messages used {wrong}, not {IMPLEMENTER_MODEL}")
    if reviewers:
        reviewer_models = (run.transcripts.get("reviewer") or {}).get("models") or {}
        if sum(_as_int(count) for count in reviewer_models.values()) < len(REVIEWER_ROLES):
            failures.append("the native transcript shows fewer reviewer messages than roles")
        wrong = [model for model in reviewer_models if not _same_model(model, REVIEWER_MODEL)]
        if wrong:
            failures.append(f"reviewer messages used {wrong}, not {REVIEWER_MODEL}")
    return failures


def _judge_control(run: StepRun) -> list[str]:
    failures = _setup_failures(run)
    if run.setup_reason is not None or run.turn is None:
        return failures
    final = run.turn.final or {}
    if final.get("status") != "done":
        failures.append(f"final status {final.get('status')!r}, expected 'done'")
    if run.turn.of_type("error"):
        failures.append(f"{len(run.turn.of_type('error'))} error events")
    failures += _usage_model_failures(run, reviewers=False)
    failures += _transcript_model_failures(run, reviewers=False)
    failures += _ceiling_failures(run)
    failures += _endpoint_failures(run)
    return failures


def _judge_personal(run: StepRun) -> list[str]:
    failures = _setup_failures(run)
    if run.setup_reason is not None or run.turn is None:
        return failures
    final = run.turn.final or {}
    if final.get("status") != "done":
        failures.append(f"final status {final.get('status')!r}, expected 'done'")
    if run.turn.of_type("error"):
        failures.append(f"{len(run.turn.of_type('error'))} error events")
    bash = run.bash_probe()
    if not bash["calls"]:
        failures.append("no Bash call carrying the marker command was observed")
    if not bash["closed_without_failure"]:
        failures.append("the marker Bash call did not complete without failure")
    if not (bash["wire_result_has_marker"] or bash["transcript_results_with_marker"]):
        failures.append(f"the Bash tool result did not contain {TOOL_MARKER!r}")
    if not any(
        line.get("tool") == "Bash" and line.get("command_has_marker") is True
        for line in run.hook_lines
    ):
        failures.append("the bundle PreToolUse hook left no marker for the Bash call")
    for role, call in run.reviewer_calls().items():
        if not call["calls"]:
            failures.append(f"the {role} subagent was not invoked")
        elif not call["closed_without_failure"]:
            failures.append(f"the {role} subagent did not complete without failure")
    roots = run.transcripts.get("roots") or []
    if INHERITED_CLAUDE_CONFIG_DIR in roots:
        failures.append("the native CLI wrote its transcript under the inherited config dir")
    gateway_host = ALT_GATEWAY.removeprefix("https://")
    if any(_host_of(target) == gateway_host for target in run.connect_targets()):
        failures.append(f"the CLI restored the saved gateway session (CONNECT to {gateway_host})")
    forced = [
        item["model"]
        for item in run.usage()
        if item["role"] == "reviewer" and _same_model(item["model"], FORCED_SUBAGENT_MODEL)
    ] + [
        model
        for model in (run.transcripts.get("reviewer") or {}).get("models") or {}
        if _same_model(model, FORCED_SUBAGENT_MODEL)
    ]
    if forced:
        failures.append(
            f"reviewer requests ran on the forced subagent model {FORCED_SUBAGENT_MODEL}, "
            f"not {REVIEWER_MODEL}"
        )
    if run.helper_ran is not False:
        failures.append("the inherited apiKeyHelper ran or its marker could not be read")
    failures += _usage_model_failures(run, reviewers=True)
    failures += _transcript_model_failures(run, reviewers=True)
    failures += _ceiling_failures(run)
    failures += _endpoint_failures(run)
    return failures


def _refusal_errors(run: StepRun) -> tuple[list[str], list[str]]:
    failures = _setup_failures(run)
    if run.setup_reason is not None or run.turn is None:
        return failures, []
    final = run.turn.final or {}
    if final.get("status") != "classified-failure":
        failures.append(f"final status {final.get('status')!r}, expected 'classified-failure'")
    messages = [str(event.get("message", "")) for event in run.turn.of_type("error")]
    if not messages:
        failures.append("no error event")
    failures += _no_traffic_failures(run)
    return failures, messages


def _judge_no_grant(run: StepRun) -> list[str]:
    failures, messages = _refusal_errors(run)
    if messages and not any("authentication required" in m.lower() for m in messages):
        failures.append("no error event says authentication required")
    return failures


def _judge_managed(run: StepRun) -> list[str]:
    failures, messages = _refusal_errors(run)
    if any("authentication required" in m.lower() for m in messages):
        failures.append("refused as a missing grant instead of for the managed policy")
    return failures


# --- The live cluster -------------------------------------------------------------------


class LiveCluster:
    """The private inputs, the owned namespace and its audited teardown."""

    def __init__(
        self,
        config: pytest.Config,
        *,
        context: str,
        image_id: str,
        image_ref: str | None,
        grant: _Secret,
        private: Sequence[str],
    ) -> None:
        self._config = config
        self.context = context
        self.image_id = image_id
        self.image = image_ref or image_id
        self._grant = grant
        self._runner_token = _Secret(secrets.token_urlsafe(32))
        self._progress_token = _Secret(secrets.token_urlsafe(32))
        self._private = [value for value in private if value and len(value) >= 3]
        self.run_id = secrets.token_hex(6)
        self._cache_dir = tempfile.mkdtemp(prefix="curie-5373-kube-cache-")
        self.child_env = {key: value for key, value in os.environ.items() if key != GRANT_ENV}
        self.child_env["KUBECACHEDIR"] = self._cache_dir
        self._processes: list[subprocess.Popen[str]] = []
        self._created: set[str] = set()
        self._namespace_requested = False
        self._namespace_created = False
        self.evidence: dict[str, Any] = {
            "test": "runner/tests/test_personal_claude_live.py",
            "issue": 5373,
            "candidate_image_id": image_id,
            "candidate_image_reference_used": image_ref is not None,
            "runner_tree": _runner_tree(self.child_env),
            "namespace": NAMESPACE,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "prerequisites": {"passed": False},
            "steps": {},
        }

    # -- custody --

    def redact(self, text: str) -> str:
        for secret in (self._grant, self._runner_token, self._progress_token):
            value = secret.reveal()
            if value:
                text = text.replace(value, "[redacted]")
        for value in self._private:
            text = text.replace(value, "[private]")
        return text

    def track(self, process: subprocess.Popen[str]) -> None:
        self._processes.append(process)

    # -- kubectl --

    def kubectl_argv(self, *args: str, namespaced: bool) -> list[str]:
        argv = ["kubectl", "--context", self.context]
        if namespaced:
            argv += ["-n", NAMESPACE]
        return argv + list(args)

    def kubectl(
        self,
        *args: str,
        namespaced: bool,
        input_text: str | None = None,
        timeout: float = 120,
    ) -> subprocess.CompletedProcess[str]:
        argv = self.kubectl_argv(*args, namespaced=namespaced)
        try:
            if input_text is None:
                return subprocess.run(
                    argv,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    env=self.child_env,
                    check=False,
                )
            return subprocess.run(
                argv,
                input=input_text,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=self.child_env,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"kubectl {args[0]} timed out after {timeout:.0f} s") from None

    def ok(self, result: subprocess.CompletedProcess[str], what: str) -> str:
        if result.returncode != 0:
            raise RuntimeError(
                f"{what} failed (exit {result.returncode}): "
                f"{self.redact((result.stderr or result.stdout or '').strip())[-2000:]}"
            )
        return result.stdout

    def create(self, manifest: Mapping[str, Any], what: str) -> None:
        # Recorded before the call: a create that times out may still have landed.
        self._created.add(f"{str(manifest['kind']).lower()}/{manifest['metadata']['name']}")
        created = self.kubectl(
            "create", "-f", "-", namespaced=True, input_text=json.dumps(manifest)
        )
        self.ok(created, what)

    # -- prerequisites and owned objects --

    def check_cluster(self) -> None:
        ready = self.kubectl("get", "--raw=/readyz", namespaced=False, timeout=30)
        if ready.returncode != 0 or ready.stdout.strip() != "ok":
            raise PrerequisiteError(
                f"the API server /readyz is not ok: "
                f"{self.redact((ready.stderr or ready.stdout).strip())[-500:]}"
            )
        for verb in ("create", "delete", "get"):
            self._require_access(verb, "namespaces", namespaced=False)
        existing = self.kubectl("get", "namespace", NAMESPACE, "-o", "name", namespaced=False)
        if existing.returncode == 0:
            raise PrerequisiteError(
                f"namespace {NAMESPACE} already exists; this test refuses to adopt it. "
                "Remove it after confirming it is not in use."
            )
        if "notfound" not in (existing.stderr or "").lower().replace(" ", ""):
            raise PrerequisiteError(
                f"could not confirm namespace {NAMESPACE} is absent: "
                f"{self.redact(existing.stderr.strip())[-500:]}"
            )

    def _require_access(
        self, verb: str, resource: str, *, namespaced: bool, subresource: str | None = None
    ) -> None:
        # can-i reads "pods/x" as the pod named x, so a subresource needs its own flag.
        extra = [f"--subresource={subresource}"] if subresource else []
        result = self.kubectl(
            "auth", "can-i", verb, resource, *extra, namespaced=namespaced, timeout=30
        )
        if result.stdout.strip() != "yes":
            scope = f"in namespace {NAMESPACE}" if namespaced else "cluster-wide"
            target = f"{resource}/{subresource}" if subresource else resource
            raise PrerequisiteError(f"not authorized to {verb} {target} {scope}")

    def create_namespace(self) -> None:
        manifest = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": NAMESPACE, "labels": self._labels()},
        }
        self._namespace_requested = True
        self.ok(
            self.kubectl("create", "-f", "-", namespaced=False, input_text=json.dumps(manifest)),
            f"create namespace {NAMESPACE}",
        )
        self._namespace_created = True

    def check_namespaced_access(self) -> None:
        for verb, resource, subresource in (
            ("create", "pods", None),
            ("get", "pods", None),
            ("delete", "pods", None),
            ("create", "secrets", None),
            # Server-side apply is a PATCH, so it needs patch as well as create.
            ("patch", "secrets", None),
            ("create", "configmaps", None),
            ("create", "pods", "portforward"),
            ("create", "pods", "exec"),
            ("get", "pods", "log"),
        ):
            self._require_access(verb, resource, namespaced=True, subresource=subresource)

    def create_inputs(self) -> None:
        labels = self._labels()
        # Server-side apply on stdin: the grant is in no argv, and unlike a
        # client-side apply no last-applied annotation keeps a second copy.
        grant: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": GRANT_SECRET, "namespace": NAMESPACE, "labels": labels},
            "type": "Opaque",
            "stringData": {"grant": self._grant.reveal()},
        }
        auth: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": AUTH_SECRET, "namespace": NAMESPACE, "labels": labels},
            "type": "Opaque",
            "stringData": {
                "runner-token": self._runner_token.reveal(),
                "progress-token": self._progress_token.reveal(),
            },
        }
        for secret in (grant, auth):
            name = secret["metadata"]["name"]
            self._created.add(f"secret/{name}")
            self.ok(
                self.kubectl(
                    "apply",
                    "--server-side",
                    f"--field-manager={FIELD_MANAGER}",
                    "-f",
                    "-",
                    namespaced=True,
                    input_text=json.dumps(secret),
                ),
                f"apply secret {name}",
            )
        self.create(_bundle_configmap(labels), "create the bundle configmap")
        self.create(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": SCRIPTS_CONFIGMAP, "namespace": NAMESPACE, "labels": labels},
                "data": {"proxy.py": PROXY_PY, "recorder.py": RECORDER_PY, "sink.py": SINK_PY},
            },
            "create the probe scripts configmap",
        )
        self.create(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": MANAGED_CONFIGMAP, "namespace": NAMESPACE, "labels": labels},
                "data": {"managed-settings.json": json.dumps(MANAGED_GATEWAY_POLICY, indent=2)},
            },
            "create the managed policy configmap",
        )

    def _labels(self) -> dict[str, str]:
        return {"curie-test-issue": "5373", "curie-test-run-id": self.run_id}

    # -- steps --

    def run_step(self, plan: StepPlan) -> StepRun:
        run = StepRun(plan=plan)
        try:
            self.create(_pod_manifest(plan, self.image, self._labels()), f"create pod {plan.pod}")
            self._wait_ready(plan.pod)
            run.image_check = self._image_check(plan.pod)
            if not all(item["matches"] for item in run.image_check):
                raise StepSetupError(
                    "a container is not running the candidate image",
                    json.dumps(run.image_check),
                )
            with _PortForward(self, plan.pod) as forward:
                try:
                    status, _body = _http(forward.port, "GET", "/healthz", token=None)
                except (OSError, http.client.HTTPException) as exc:
                    raise StepSetupError(f"runner /healthz failed: {type(exc).__name__}") from None
                if status != 200:
                    raise StepSetupError(f"runner /healthz answered {status}")
                # The only model-bearing call of the step, sent exactly once.
                run.turn = _stream_turn(
                    forward.port, self._runner_token.reveal(), plan.prompt, plan.ceiling
                )
            self._observe(run)
        except StepSetupError as exc:
            run.setup_reason = exc.reason
            run.setup_detail = self.redact(exc.detail)[-3000:]
        except Exception as exc:  # noqa: BLE001 - reported as this step's failure
            if run.turn is None:
                run.setup_reason = "pod preparation failed"
                run.setup_detail = self.redact(str(exc))[-3000:]
            else:
                run.observe_errors.append("observation failed")
                run.observe_details.append(self.redact(str(exc))[-500:])
        finally:
            deleted = self.kubectl(
                "delete",
                "pod",
                plan.pod,
                "--ignore-not-found",
                "--grace-period=5",
                "--wait=true",
                "--timeout=120s",
                namespaced=True,
                timeout=150,
            )
            run.pod_deleted = deleted.returncode == 0
        return run

    def _wait_ready(self, pod: str) -> None:
        waited = self.kubectl(
            "wait",
            "--for=condition=Ready",
            f"pod/{pod}",
            "--timeout=240s",
            namespaced=True,
            timeout=270,
        )
        if waited.returncode == 0:
            return
        detail = [self.redact(waited.stderr.strip())]
        described = self.kubectl("get", "pod", pod, "-o", "json", namespaced=True)
        if described.returncode == 0:
            status = json.loads(described.stdout).get("status") or {}
            for item in (status.get("initContainerStatuses") or []) + (
                status.get("containerStatuses") or []
            ):
                state = item.get("state") or {}
                reasons = {key: (value or {}).get("reason") for key, value in state.items()}
                detail.append(f"{item.get('name')}: ready={item.get('ready')} state={reasons}")
        logs = self.kubectl("logs", pod, "-c", "runner", "--tail=40", namespaced=True)
        if logs.returncode == 0:
            detail.append(self.redact(logs.stdout))
        raise StepSetupError(f"pod {pod} did not become Ready", "\n".join(detail))

    def _image_check(self, pod: str) -> list[dict[str, Any]]:
        described = json.loads(
            self.ok(self.kubectl("get", "pod", pod, "-o", "json", namespaced=True), "get pod")
        )
        status = described.get("status") or {}
        expected = self.image_id.split(":", 1)[1]
        checked: list[dict[str, Any]] = []
        for item in (status.get("initContainerStatuses") or []) + (
            status.get("containerStatuses") or []
        ):
            image_id = str(item.get("imageID") or "")
            checked.append(
                {
                    "container": str(item.get("name")),
                    "matches": image_id.endswith(expected),
                    "image_id": image_id.rsplit("@", 1)[-1],
                }
            )
        spec = described.get("spec") or {}
        declared = len(spec.get("initContainers") or []) + len(spec.get("containers") or [])
        if len(checked) != declared:
            checked.append({"container": "<missing status>", "matches": False, "image_id": ""})
        return checked

    def read_file(self, pod: str, container: str, path: str) -> str | None:
        result = self.kubectl("exec", pod, "-c", container, "--", "cat", path, namespaced=True)
        if result.returncode == 0:
            return self.redact(result.stdout)
        if "no such file" in (result.stderr or "").lower():
            return None
        raise RuntimeError(
            f"read {path} from {container} failed: {self.redact(result.stderr.strip())[-500:]}"
        )

    def _observe(self, run: StepRun) -> None:
        """Read every observation; a failed read is a step failure, never a pass."""

        pod = run.plan.pod

        def attempt(label: str, read: Callable[[], None]) -> None:
            try:
                read()
            except Exception as exc:  # noqa: BLE001 - recorded as a missing observation
                run.observe_errors.append(f"missing proof: {label}")
                run.observe_details.append(f"{label}: {self.redact(str(exc))[-500:]}")

        def proxy() -> None:
            text = self.read_file(pod, "proxy", f"{PROBE_LOGS}/proxy.jsonl")
            if text is None:
                raise RuntimeError("the proxy log is absent")
            run.proxy = _jsonl(text)

        def recorder() -> None:
            text = self.read_file(pod, "recorder", f"{PROBE_LOGS}/recorder.json")
            parsed = json.loads(text or "")
            if not isinstance(parsed, dict) or not isinstance(parsed.get("count"), int):
                raise RuntimeError("the recorder state is not an object with a count")
            run.recorder = parsed

        def sink() -> None:
            text = self.read_file(pod, "sink", f"{PROBE_LOGS}/sink.jsonl")
            if text is None:
                raise RuntimeError("the usage sink log is absent")
            run.sink = _jsonl(text)

        def hook() -> None:
            run.hook_lines = _jsonl(self.read_file(pod, "runner", HOOK_MARKER))

        def helper() -> None:
            run.helper_ran = self.read_file(pod, "runner", HELPER_MARKER) is not None

        attempt("proxy log", proxy)
        attempt("alternate endpoint recorder", recorder)
        attempt("usage sink", sink)
        attempt("native transcripts", lambda: self._summarize_transcripts(run))
        attempt("hook marker", hook)
        attempt("apiKeyHelper marker", helper)

    def _summarize_transcripts(self, run: StepRun) -> None:
        pod = run.plan.pod
        summary = self.kubectl(
            "exec",
            pod,
            "-c",
            "runner",
            "--",
            "python3",
            "-I",
            "-c",
            TRANSCRIPT_SUMMARY_PY,
            "/tmp",
            "/home/runner",
            namespaced=True,
        )
        if summary.returncode != 0:
            raise RuntimeError(f"the summary failed: {self.redact(summary.stderr.strip())[-300:]}")
        lines = summary.stdout.strip().splitlines()
        parsed = json.loads(lines[-1]) if lines else None
        if not isinstance(parsed, dict):
            raise RuntimeError("the summary printed no object")
        run.transcripts = parsed

    def record(self, run: StepRun, failures: Sequence[str]) -> None:
        self.evidence["steps"][run.plan.key] = {
            "expected": dict(run.plan.expected),
            "observed": run.observed(self.redact),
            "passed": not failures,
            "failures": [self.redact(failure)[:500] for failure in failures],
        }

    def failure_text(self, run: StepRun, failures: Sequence[str]) -> str:
        text = f"{run.plan.key}: " + "; ".join(failures)
        if run.setup_detail:
            text += f"\n{run.plan.key} setup detail:\n{run.setup_detail}"
        if run.observe_details:
            text += f"\n{run.plan.key} observation detail:\n" + "\n".join(run.observe_details)
        return self.redact(text)

    # -- teardown --

    def _namespace_is_ours(self) -> bool:
        if self._namespace_created:
            return True
        if not self._namespace_requested:
            return False
        found = self.kubectl("get", "namespace", NAMESPACE, "-o", "json", namespaced=False)
        if found.returncode != 0:
            return False
        labels = (json.loads(found.stdout).get("metadata") or {}).get("labels") or {}
        return labels.get("curie-test-run-id") == self.run_id

    def teardown(self) -> None:
        problems: list[str] = []
        cleanup: dict[str, Any] = {"namespace_owned": False}
        started = time.monotonic()
        try:
            for process in self._processes:
                if process.poll() is None:
                    process.kill()
                    with contextlib.suppress(subprocess.TimeoutExpired):
                        process.wait(timeout=10)
            cleanup["port_forwards_running"] = sum(
                1 for process in self._processes if process.poll() is None
            )
            if cleanup["port_forwards_running"]:
                problems.append("a port-forward process is still running")
            if self._namespace_is_ours():
                cleanup["namespace_owned"] = True
                listed = self.kubectl(
                    "get", "pods,secrets,configmaps", "-o", "name", namespaced=True
                )
                if listed.returncode == 0:
                    inventory = sorted(
                        line.strip() for line in listed.stdout.splitlines() if line.strip()
                    )
                    unexpected = sorted(set(inventory) - self._created - SYSTEM_OBJECTS)
                    cleanup["inventory_before_delete"] = inventory
                    cleanup["unexpected_objects"] = unexpected
                    if unexpected:
                        problems.append(f"objects this test did not create: {unexpected}")
                else:
                    problems.append("could not list the namespace before deletion")
                deleted = self.kubectl(
                    "delete", "namespace", NAMESPACE, "--wait=false", namespaced=False
                )
                if deleted.returncode != 0 and "notfound" not in deleted.stderr.lower().replace(
                    " ", ""
                ):
                    problems.append(
                        f"delete namespace failed: {self.redact(deleted.stderr.strip())[-300:]}"
                    )
                self.kubectl(
                    "wait",
                    "--for=delete",
                    f"namespace/{NAMESPACE}",
                    "--timeout=180s",
                    namespaced=False,
                    timeout=200,
                )
                remaining = self.kubectl(
                    "get", "namespace", NAMESPACE, "-o", "name", namespaced=False
                )
                absent = remaining.returncode != 0 and "notfound" in (
                    remaining.stderr or ""
                ).lower().replace(" ", "")
                cleanup["namespace_absent"] = absent
                # The grant's only cluster copy was the Secret (and the env of the
                # step pods) inside this namespace.
                cleanup["grant_copies_remaining"] = 0 if absent else None
                if not absent:
                    problems.append(f"namespace {NAMESPACE} is still present after 180 s")
            else:
                cleanup["namespace_absent"] = None
        except Exception as exc:  # noqa: BLE001 - every teardown failure is reported below
            problems.append(f"teardown error: {self.redact(str(exc))[-500:]}")
        finally:
            shutil.rmtree(self._cache_dir, ignore_errors=True)
            cleanup["seconds"] = round(time.monotonic() - started, 3)
            cleanup["passed"] = not problems
            # Evidence keeps test-authored text; kubectl output stays in the terminal.
            cleanup["problems"] = [
                self.redact(problem)[:500]
                if problem.startswith("objects this test did not create")
                else self.redact(problem).split(": ", 1)[0]
                for problem in problems
            ]
            self.evidence["cleanup"] = cleanup
            self.evidence["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            written = self._write_evidence(problems)
            self._summary(written)
        if problems:
            raise AssertionError(self.redact("cleanup audit failed: " + "; ".join(problems)))

    def _write_evidence(self, problems: list[str]) -> str:
        steps = self.evidence["steps"]
        passed = (
            self.evidence["prerequisites"].get("passed") is True
            and len(steps) == 4
            and all(step["passed"] for step in steps.values())
            and self.evidence["cleanup"]["passed"]
        )
        self.evidence["result"] = "pass" if passed else "fail"
        target = os.environ.get(EVIDENCE_ENV, "").strip()
        if not target:
            return "not requested"
        text = self.redact(json.dumps(self.evidence, indent=2, sort_keys=True))
        try:
            Path(target).write_text(text + "\n", encoding="utf-8")
        except OSError as exc:
            problems.append(f"could not write the evidence file: {type(exc).__name__}")
            return "failed"
        return "written"

    def _summary(self, written: str) -> None:
        steps = ",".join(
            f"{key}={'pass' if step['passed'] else 'fail'}"
            for key, step in self.evidence["steps"].items()
        )
        line = (
            f"personal-claude-live: result={self.evidence.get('result')} "
            f"steps=[{steps or 'none'}] "
            f"cleanup={'pass' if self.evidence['cleanup']['passed'] else 'fail'} "
            f"evidence={written}"
        )
        capture = self._config.pluginmanager.getplugin("capturemanager")
        with capture.global_and_fixture_disabled() if capture else contextlib.nullcontext():
            print(self.redact(line), flush=True)


# --- Manifests ---------------------------------------------------------------------------


def _bundle_configmap(labels: Mapping[str, str]) -> dict[str, Any]:
    plugin = {
        "name": PLUGIN_NAME,
        "version": "0.1.0",
        "description": "Live probe bundle: the factory reviewer roles plus a marking hook.",
    }
    # A bundle command hook, declared the way the factory bundle declares its own.
    # https://code.claude.com/docs/en/hooks: a PreToolUse command hook receives the
    # tool input on stdin and exit 0 allows the call.
    hooks = {
        "hooks": {
            "PreToolUse": [
                {
                    "matcher": "Bash",
                    "hooks": [
                        {
                            "type": "command",
                            "command": "python3 ${CLAUDE_PLUGIN_ROOT}/hooks/mark_tool.py",
                            "timeout": 10,
                        }
                    ],
                }
            ]
        }
    }
    data = {
        "plugin.json": json.dumps(plugin, indent=2),
        "hooks.json": json.dumps(hooks, indent=2),
        "mark_tool.py": MARK_TOOL_PY,
        "stage.py": STAGE_PY,
        "conflict-settings.json": json.dumps(CONFLICT_SETTINGS, indent=2),
        "conflict-credentials.json": json.dumps(CONFLICT_CREDENTIALS, indent=2),
        "conflict-profile.json": json.dumps(CONFLICT_PROFILE, indent=2),
    }
    for role in REVIEWER_ROLES:
        # The real role declarations, unmodified (their model alias is opus).
        data[f"{role}.md"] = (AGENTS_DIR / f"{role}.md").read_text(encoding="utf-8")
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": BUNDLE_CONFIGMAP, "namespace": NAMESPACE, "labels": dict(labels)},
        "data": data,
    }


BUNDLE_ITEMS = (
    ("plugin.json", f"{PLUGIN_NAME}/.claude-plugin/plugin.json"),
    ("plan-reviewer.md", f"{PLUGIN_NAME}/agents/plan-reviewer.md"),
    ("diff-reviewer.md", f"{PLUGIN_NAME}/agents/diff-reviewer.md"),
    ("hooks.json", f"{PLUGIN_NAME}/hooks/hooks.json"),
    ("mark_tool.py", f"{PLUGIN_NAME}/hooks/mark_tool.py"),
    ("stage.py", "stage.py"),
    ("conflict-settings.json", "conflict/settings.json"),
    ("conflict-credentials.json", "conflict/credentials.json"),
    ("conflict-profile.json", "conflict/alt-profile.json"),
)

CONTAINER_SECURITY: dict[str, Any] = {
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "runAsNonRoot": True,
    "capabilities": {"drop": ["ALL"]},
}


def _runner_env(plan: StepPlan) -> list[dict[str, Any]]:
    proxy = f"http://127.0.0.1:{PROXY_PORT}"
    env = [
        _value_env("HOME", "/home/runner"),
        _value_env("CURIE_PLUGIN_DIR", PLUGIN_DIR),
        _value_env("CURIE_SESSION_ID", f"live-5373-{plan.key.replace('_', '-')}"),
        {"name": "CURIE_SANDBOX_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        _value_env(
            "CURIE_BUDGET",
            json.dumps(
                {
                    "max_output_tokens_per_run": plan.ceiling.output_tokens,
                    "max_usd_per_day": plan.ceiling.max_usd_per_day,
                }
            ),
        ),
        _value_env("CURIE_MAX_TURNS", str(plan.ceiling.max_turns_env)),
        _value_env("CURIE_RUNNER_PORT", str(RUNNER_PORT)),
        _value_env("CURIE_MODEL", IMPLEMENTER_MODEL),
        _secret_env("CURIE_RUNNER_TOKEN", AUTH_SECRET, "runner-token"),
        # The progress port the kernel gives a factory execution. The runner posts
        # each turn's per-role, per-model usage to <url>/usage (usage_report.py).
        _value_env("CURIE_PROGRESS_URL", f"http://127.0.0.1:{SINK_PORT}/progress"),
        _secret_env("CURIE_PROGRESS_TOKEN", AUTH_SECRET, "progress-token"),
        # Installation transport, kept by the personal policy.
        # https://code.claude.com/docs/en/network-config: the CLI honours
        # HTTPS_PROXY and HTTP_PROXY.
        _value_env("HTTPS_PROXY", proxy),
        _value_env("HTTP_PROXY", proxy),
        _value_env("https_proxy", proxy),
        _value_env("http_proxy", proxy),
        # https://code.claude.com/docs/en/settings: turns off telemetry, error
        # reporting and auto-update traffic, so the CONNECT log is model traffic.
        _value_env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1"),
    ]
    if plan.personal:
        env += [
            _value_env("CURIE_MODEL_AUTH_MODE", "personal"),
            _value_env("CURIE_REVIEWER_MODEL", REVIEWER_MODEL),
        ]
    if plan.grant:
        name = "CLAUDE_CODE_OAUTH_TOKEN" if plan.personal else "CURIE_CREDENTIALS"
        env.append(_secret_env(name, GRANT_SECRET, "grant"))
    if plan.conflicts:
        env += [_value_env(name, value) for name, value in CONFLICT_ENV]
    return env


def _sidecar(name: str, image: str, script: str) -> dict[str, Any]:
    ready = f"{PROBE_LOGS}/{name}.ready"
    return {
        "name": name,
        "image": image,
        "imagePullPolicy": "Never",
        # A native sidecar: started, and its startup probe passed, before the runner.
        "restartPolicy": "Always",
        "command": ["python3", "-I", "-u", f"/probe-scripts/{script}"],
        "env": [
            _value_env("HOME", "/tmp"),
            _value_env("PYTHONDONTWRITEBYTECODE", "1"),
            _value_env("PROBE_LOG_DIR", PROBE_LOGS),
            _value_env("PROBE_PROXY_PORT", str(PROXY_PORT)),
            _value_env("PROBE_RECORDER_PORT", str(RECORDER_PORT)),
            _value_env("PROBE_SINK_PORT", str(SINK_PORT)),
            *(
                [_secret_env("PROBE_SINK_TOKEN", AUTH_SECRET, "progress-token")]
                if name == "sink"
                else []
            ),
        ],
        "securityContext": CONTAINER_SECURITY,
        "startupProbe": {
            "exec": {
                "command": [
                    "python3",
                    "-I",
                    "-c",
                    f"import os, sys; sys.exit(0 if os.path.exists({ready!r}) else 1)",
                ]
            },
            "periodSeconds": 1,
            "failureThreshold": 60,
        },
        "volumeMounts": [
            {"name": "probe-scripts", "mountPath": "/probe-scripts", "readOnly": True},
            {"name": "probe-logs", "mountPath": PROBE_LOGS},
            {"name": "sidecar-tmp", "mountPath": "/tmp"},
        ],
    }


def _pod_manifest(plan: StepPlan, image: str, labels: Mapping[str, str]) -> dict[str, Any]:
    stage = {
        "name": "stage",
        "image": image,
        "imagePullPolicy": "Never",
        "command": ["python3", "-I", "/seed/stage.py"],
        "env": [
            _value_env("STAGE_BUNDLE_DIR", PLUGIN_DIR),
            _value_env("STAGE_CONFLICTS", "1" if plan.conflicts else "0"),
            _value_env("STAGE_CLAUDE_CONFIG_DIR", INHERITED_CLAUDE_CONFIG_DIR),
            _value_env("STAGE_ANTHROPIC_CONFIG_DIR", INHERITED_ANTHROPIC_CONFIG_DIR),
            _value_env("STAGE_SECURE_STORAGE_DIR", INHERITED_SECURE_STORAGE_DIR),
            _value_env("PYTHONDONTWRITEBYTECODE", "1"),
        ],
        "securityContext": CONTAINER_SECURITY,
        "volumeMounts": [
            {"name": "seed", "mountPath": "/seed", "readOnly": True},
            {"name": "bundle", "mountPath": "/bundle"},
            {"name": "home", "mountPath": "/home/runner"},
        ],
    }
    runner_mounts: list[dict[str, Any]] = [
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "home", "mountPath": "/home/runner"},
        {"name": "bundle", "mountPath": "/bundle", "readOnly": True},
    ]
    volumes: list[dict[str, Any]] = [
        {"name": "tmp", "emptyDir": {"sizeLimit": "2Gi"}},
        {"name": "home", "emptyDir": {"sizeLimit": "2Gi"}},
        {"name": "bundle", "emptyDir": {"sizeLimit": "64Mi"}},
        {"name": "probe-logs", "emptyDir": {"sizeLimit": "64Mi"}},
        {"name": "sidecar-tmp", "emptyDir": {"sizeLimit": "64Mi"}},
        {
            "name": "seed",
            "configMap": {
                "name": BUNDLE_CONFIGMAP,
                "items": [{"key": key, "path": path} for key, path in BUNDLE_ITEMS],
            },
        },
        {"name": "probe-scripts", "configMap": {"name": SCRIPTS_CONFIGMAP}},
    ]
    if plan.managed_policy:
        runner_mounts.append(
            {"name": "managed-policy", "mountPath": "/etc/claude-code", "readOnly": True}
        )
        volumes.append(
            {
                "name": "managed-policy",
                "configMap": {
                    "name": MANAGED_CONFIGMAP,
                    "items": [{"key": "managed-settings.json", "path": "managed-settings.json"}],
                },
            }
        )
    runner = {
        "name": "runner",
        "image": image,
        "imagePullPolicy": "Never",
        "securityContext": CONTAINER_SECURITY,
        "ports": [{"name": "aci", "containerPort": RUNNER_PORT}],
        "env": _runner_env(plan),
        "readinessProbe": {
            "httpGet": {"path": "/healthz", "port": RUNNER_PORT},
            "periodSeconds": 2,
            "failureThreshold": 3,
        },
        "volumeMounts": runner_mounts,
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": plan.pod,
            "namespace": NAMESPACE,
            "labels": {**labels, "curie-test-step": plan.key.replace("_", "-")},
        },
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "terminationGracePeriodSeconds": 10,
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": RUNNER_UID,
                "runAsGroup": RUNNER_UID,
                "fsGroup": RUNNER_UID,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "initContainers": [
                stage,
                _sidecar("proxy", image, "proxy.py"),
                _sidecar("recorder", image, "recorder.py"),
                _sidecar("sink", image, "sink.py"),
            ],
            "containers": [runner],
            "volumes": volumes,
        },
    }


def _runner_tree(env: Mapping[str, str]) -> str | None:
    if shutil.which("git") is None:
        return None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD:runner"],
            cwd=REPO_ROOT,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=30,
            env=dict(env),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    tree = result.stdout.strip()
    return tree if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40,64}", tree) else None


# --- Fixture and test -------------------------------------------------------------------------


def _require_inputs() -> tuple[str, str, str | None, _Secret, list[str]]:
    """Validate the private inputs without echoing any value."""

    grant_value = os.environ.pop(GRANT_ENV, "")
    missing = [name for name in REQUIRED_ENV if name != GRANT_ENV and not os.environ.get(name)]
    if not grant_value:
        missing.append(GRANT_ENV)
    if missing:
        raise PrerequisiteError(f"missing required live inputs: {', '.join(sorted(missing))}")
    if grant_value != grant_value.strip() or any(ch.isspace() for ch in grant_value):
        raise PrerequisiteError(f"{GRANT_ENV} must be one token with no whitespace")
    grant_file = Path(os.environ[GRANT_FILE_ENV])
    try:
        info = grant_file.stat()
    except OSError:
        raise PrerequisiteError(f"{GRANT_FILE_ENV} does not name a readable file") from None
    if not stat.S_ISREG(info.st_mode) or info.st_size == 0:
        raise PrerequisiteError(f"{GRANT_FILE_ENV} must name a nonempty regular file")
    if info.st_mode & 0o077:
        raise PrerequisiteError(f"{GRANT_FILE_ENV} must not be group or world accessible")
    image_id = os.environ[IMAGE_ID_ENV].strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise PrerequisiteError(f"{IMAGE_ID_ENV} must be sha256:<64 hex>")
    image_ref = os.environ.get(IMAGE_REF_ENV, "").strip() or None
    if image_ref is not None and any(ch.isspace() for ch in image_ref):
        raise PrerequisiteError(f"{IMAGE_REF_ENV} must be one image reference")
    if shutil.which("kubectl") is None:
        raise PrerequisiteError("kubectl is not on PATH")
    for role in REVIEWER_ROLES:
        role_file = AGENTS_DIR / f"{role}.md"
        try:
            text = role_file.read_text(encoding="utf-8")
        except OSError:
            raise PrerequisiteError(f"cannot read the factory role {role_file.name}") from None
        if not re.search(r"^model:\s*opus\s*$", text, re.MULTILINE):
            raise PrerequisiteError(f"the factory role {role_file.name} no longer declares opus")
    context = os.environ[CONTEXT_ENV].strip()
    private = [context, str(grant_file), *os.environ["KUBECONFIG"].split(os.pathsep)]
    return context, image_id, image_ref, _Secret(grant_value), private


@pytest.fixture(scope="session")
def live(request: pytest.FixtureRequest) -> Iterator[LiveCluster]:
    if os.environ.get(GATE_ENV) != "1":
        pytest.skip(f"set {GATE_ENV}=1 to run the live personal Claude proof")
    context, image_id, image_ref, grant, private = _require_inputs()
    cluster = LiveCluster(
        request.config,
        context=context,
        image_id=image_id,
        image_ref=image_ref,
        grant=grant,
        private=private,
    )
    # Registered before anything exists, so a failure at any later point still
    # deletes and audits what was created. It never deletes a namespace it did
    # not create.
    request.addfinalizer(cluster.teardown)
    try:
        cluster.check_cluster()
        cluster.create_namespace()
        cluster.check_namespaced_access()
        cluster.create_inputs()
    except PrerequisiteError as exc:
        # Evidence keeps the test-authored reason only; tool output stays in the terminal.
        reason = cluster.redact(str(exc)).split(": ", 1)[0]
        cluster.evidence["prerequisites"] = {"passed": False, "reason": reason}
        raise
    except Exception as exc:  # noqa: BLE001 - any setup failure is a setup error
        reason = cluster.redact(str(exc))[-1000:]
        cluster.evidence["prerequisites"] = {"passed": False, "reason": "setup failed"}
        raise PrerequisiteError(reason) from None
    cluster.evidence["prerequisites"] = {"passed": True}
    yield cluster


def test_personal_claude_seam_on_a_throwaway_cluster(live: LiveCluster) -> None:
    control = live.run_step(CONTROL_STEP)
    failures = _judge_control(control)
    live.record(control, failures)
    if failures:
        # The redacted one-turn liveness check failed: the paid session never runs.
        pytest.fail(
            "missing proof: liveness\n" + live.failure_text(control, failures), pytrace=False
        )

    reported: list[str] = []
    for plan, judge in (
        (PERSONAL_STEP, _judge_personal),
        (NO_GRANT_STEP, _judge_no_grant),
        (MANAGED_STEP, _judge_managed),
    ):
        # Steps 3 and 4 cost no inference, so they run even when step 2 failed.
        run = live.run_step(plan)
        step_failures = judge(run)
        live.record(run, step_failures)
        if step_failures:
            reported.append(live.failure_text(run, step_failures))
    if reported:
        pytest.fail("\n".join(reported), pytrace=False)
