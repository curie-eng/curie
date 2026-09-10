#!/usr/bin/env bash
#
# Behavioural test of the rendered `attachments-init` program (#2567, S4).
#
# attachment-init-assertions.sh reads the manifest and proves the digest check
# and the size cap are THERE. This one proves they WORK: it extracts the program
# helm renders into the init container, points its mount at a temp directory and
# a fixture HTTP server at 127.0.0.1, and runs it. A structural gate can be
# satisfied by a script that computes a digest and then ignores the comparison,
# or caps a counter it never checks; only executing it can tell the difference.
#
# It also covers the refusals nobody would notice in production, because every
# one of them looks like an agent that simply did not mention the file:
#
#   * a digest that does not match the minted one       -> refuse, write nothing
#   * a body larger than the operator's cap             -> refuse, write nothing
#   * a reference whose window has closed               -> refuse
#   * a redirect away from the presigned host           -> refuse (no-redirect
#     opener, same reason the worker's Slack client refuses one: a redirect
#     carries the request to a host the store named rather than one we chose)
#   * a non-HTTP(S) url                                 -> refuse
#   * a filename carrying ../ path traversal            -> never escape the mount
#   * NO reference at all                               -> exit 0, touch nothing
#
# The reference wire shape (base64url of a JSON list keyed n/u/s/b/e/m) is built
# by hand below rather than by importing the worker, because a chart CI script
# must not depend on the python packages. The same shape is pinned from the
# other side by
# apps/worker/tests/test_attachment_wiring.py::test_the_encoded_reference_field_names_are_the_chart_contract,
# so a rename cannot pass both.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="${CHART:-$(cd "$SCRIPT_DIR/.." && pwd)}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

MOUNT="$TMP/attachments"
mkdir -p "$MOUNT"

# A cap small enough to cross with a fixture body, set through the same value an
# operator would use -- so this also proves the cap is templated rather than a
# literal in the template.
CAP=64

echo "=== Rendering the sandbox pod (mountPath=$MOUNT, maxFileBytes=$CAP) ==="
helm template curie "$CHART" --namespace dev \
  --set "agentSandbox.runner.attachments.mountPath=$MOUNT" \
  --set "worker.attachments.maxFileBytes=$CAP" \
  > "$TMP/rendered.yaml"

python3 - "$TMP/rendered.yaml" "$MOUNT" "$CAP" <<'PY'
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

RENDERED, MOUNT, CAP = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])

INIT_NAME = "attachments-init"
REF_ENV = "CURIE_ATTACHMENTS_REF"


def fail(message):
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


# --- the program helm rendered ---------------------------------------------


def init_program():
    for doc in yaml.safe_load_all(Path(RENDERED).read_text()):
        if not doc or doc.get("kind") != "SandboxTemplate":
            continue
        pod = doc["spec"]["podTemplate"]
        component = doc["metadata"].get("labels", {}).get(
            "app.kubernetes.io/component"
        ) or pod.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
        if component != "agent-sandbox":
            continue
        for container in pod["spec"].get("initContainers") or []:
            if container.get("name") == INIT_NAME:
                return container["command"]
    fail(
        f"no {INIT_NAME} init container in the rendered generic SandboxTemplate. "
        "attachment-init-assertions.sh explains why that is the whole feature."
    )


COMMAND = init_program()
# The program body is the one argument that is not a flag or an interpreter
# name: `python -c <body>` and `/bin/sh -c <body>` are both this shape.
BODY = max((str(part) for part in COMMAND), key=len)
INTERPRETER = str(COMMAND[0])
if "python" in INTERPRETER:
    ARGV = [sys.executable, "-c", BODY]
else:
    ARGV = ["/bin/sh", "-c", BODY]


# --- the fixture store ------------------------------------------------------


class _Store(BaseHTTPRequestHandler):
    objects: dict[str, bytes] = {}

    def do_GET(self):  # noqa: N802 -- BaseHTTPRequestHandler's own name
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/object-0")
            self.end_headers()
            return
        payload = self.objects.get(self.path)
        if payload is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        return None


server = ThreadingHTTPServer(("127.0.0.1", 0), _Store)
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{server.server_address[1]}"


def publish(index, payload):
    path = f"/object-{index}"
    _Store.objects[path] = payload
    return f"{BASE}{path}"


# --- the reference wire shape -----------------------------------------------
#
# Mirrors curie_worker.attachments.encode_attachment_refs. See the header for
# why it is duplicated and where the other side is pinned.


def encode(entries):
    raw = json.dumps(entries, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def entry(name, url, payload, *, sha256=None, expires=2_000_000_000, mime="text/plain"):
    return {
        "n": name,
        "u": url,
        "s": sha256 or hashlib.sha256(payload).hexdigest(),
        "b": len(payload),
        "e": expires,
        "m": mime,
    }


# --- the run harness --------------------------------------------------------


def visible():
    """What the runner would announce to the model: non-hidden entries only."""

    if not MOUNT.is_dir():
        return {}
    return {
        child.name: child.read_bytes()
        for child in sorted(MOUNT.iterdir())
        if not child.name.startswith(".") and child.is_file()
    }


def reset():
    for child in MOUNT.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def run(ref):
    reset()
    env = dict(os.environ)
    if ref is None:
        env.pop(REF_ENV, None)
    else:
        env[REF_ENV] = ref
    completed = subprocess.run(ARGV, env=env, capture_output=True, text=True, timeout=120)
    return completed


def expect_refusal(label, ref, *, escaped=()):
    completed = run(ref)
    if completed.returncode == 0:
        fail(
            f"{label}: the init container exited 0. In a pod that boots the runner "
            f"and the agent silently reads whatever landed.\n"
            f"    stdout={completed.stdout!r}\n    stderr={completed.stderr!r}"
        )
    left = visible()
    if left:
        fail(
            f"{label}: refused but left {sorted(left)} in the mount. The runner "
            "announces every visible file, so a refused fetch must leave none."
        )
    for path in escaped:
        if Path(path).exists():
            fail(f"{label}: wrote outside the mount at {path}")
    print(f"  ok: {label}: refused (exit {completed.returncode}), mount left empty")


# --- (a) the happy path -----------------------------------------------------

first, second = b"a,b\n1,2\n", b"hello attachment"
completed = run(
    encode(
        [
            entry("report.csv", publish(0, first), first, mime="text/csv"),
            entry("notes.txt", publish(1, second), second),
        ]
    )
)
if completed.returncode != 0:
    fail(
        "the happy path did not materialize two attachments "
        f"(exit {completed.returncode})\n    stdout={completed.stdout!r}\n"
        f"    stderr={completed.stderr!r}"
    )
landed = visible()
if sorted(landed.values()) != sorted((first, second)):
    fail(f"the materialized bytes are not the served bytes: {landed}")
if len(landed) != 2:
    fail(f"expected two visible files in the mount, got {sorted(landed)}")
print(f"  ok: two attachments materialized byte-exactly as {sorted(landed)}")

# The names a person recognises must survive: the whole point of the lane is
# that the agent can say which file it read.
if not any("report" in name for name in landed):
    fail(f"the uploader's filename did not survive into the mount: {sorted(landed)}")
print("  ok: the uploader's filename is recoverable from the mount")

# --- (b) a digest that does not match ---------------------------------------

expect_refusal(
    "digest mismatch",
    encode([entry("report.csv", publish(2, first), first, sha256="b" * 64)]),
)

# --- (c) a body over the operator's cap -------------------------------------

oversize = b"x" * (CAP + 1)
expect_refusal(
    f"over the {CAP} byte cap",
    encode([entry("big.bin", publish(3, oversize), oversize)]),
)

# One byte under the cap must still be accepted, or "the cap works" would also
# be satisfied by a container that refuses everything.
under = b"y" * CAP
completed = run(encode([entry("small.bin", publish(4, under), under)]))
if completed.returncode != 0:
    fail(
        f"a body of exactly the {CAP} byte cap was refused "
        f"(exit {completed.returncode}); the cap is off by one\n"
        f"    stderr={completed.stderr!r}"
    )
if list(visible().values()) != [under]:
    fail(f"a body at exactly the cap did not materialize: {sorted(visible())}")
print(f"  ok: a body of exactly {CAP} bytes still materializes")

# --- (d) a reference whose window has closed --------------------------------

expect_refusal(
    "expired reference",
    encode([entry("report.csv", publish(5, first), first, expires=1_000_000_000)]),
)

# --- (e) a redirect away from the presigned host ----------------------------

expect_refusal(
    "redirected fetch",
    encode([entry("report.csv", f"{BASE}/redirect", first)]),
)

# --- (f) a url that is not HTTP(S) ------------------------------------------

expect_refusal(
    "non-HTTP(S) url",
    encode([entry("report.csv", "file:///etc/passwd", first)]),
)

# --- (g) a filename carrying path traversal ---------------------------------
#
# `name` is text supplied by whoever uploaded the file. The worker already keeps
# it out of the object key (test_a_hostile_filename_never_reaches_the_object_key);
# the init container is the second place it could escape, and here it would
# escape onto the pod's filesystem.

escape_target = MOUNT.parent / "escaped.bin"
completed = run(
    encode([entry("../escaped.bin", publish(6, first), first)])
)
if escape_target.exists():
    fail(
        f"a ../ filename wrote outside the mount to {escape_target}. The name is "
        "uploader-supplied text; it must be reduced to a basename or refused."
    )
if completed.returncode == 0 and not visible():
    fail("a ../ filename was accepted but materialized nothing visible")
print("  ok: a ../ filename never escapes the mount")

# --- (h) no reference at all -------------------------------------------------
#
# The overwhelming majority of turns. It must be a clean no-op: a non-zero exit
# here would crash-loop the init container on every ordinary message.

completed = run(None)
if completed.returncode != 0:
    fail(
        f"no {REF_ENV} exited {completed.returncode}; every ordinary turn would "
        f"crash-loop the pod\n    stderr={completed.stderr!r}"
    )
if visible():
    fail(f"no {REF_ENV} still wrote {sorted(visible())} into the mount")
print(f"  ok: no {REF_ENV} is a clean no-op (exit 0, nothing written)")

# An empty-valued key is what a claim that declares the env but resolved no
# files looks like, and the pod template bakes exactly that. Same no-op.
completed = run("")
if completed.returncode != 0:
    fail(f"an empty {REF_ENV} exited {completed.returncode}; the baked empty ref crash-loops")
if visible():
    fail(f"an empty {REF_ENV} still wrote {sorted(visible())}")
print(f"  ok: an empty {REF_ENV} (the baked template value) is the same no-op")

server.shutdown()

print()
print(
    "PASS: the rendered attachments-init program materializes verified bytes, "
    "and refuses -- writing nothing the runner could announce -- on a digest "
    "mismatch, an over-cap body, an expired reference, a redirect, a non-HTTP "
    "url and a traversal filename, while an absent or empty reference is a "
    "clean no-op."
)
PY
