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
# ADR 0205 (decisions 4, 6, 8) adds the thread's earlier files to the same
# payload, and the outcome for each case is a SHARED VECTOR,
# tests/vectors/attachment-init-outcomes.json, which
# apps/worker/tests/sandbox/test_docker_attachment_claim.py also runs through the
# docker driver -- so the two substrates cannot drift into treating the same
# payload differently. In short: an entry's "n" is written exactly (never
# renamed), and an unclean, reserved (.curie-) or duplicate name is fatal; the
# current message's entries ("c": 1, or no "c" from an older worker) are fetched
# first and any failure of theirs is fatal; an earlier entry ("c": 0) that is
# expired, answered with an HTTP error, times out, or is reached after the
# overall fetch deadline is skipped and recorded unavailable with a reason; a
# digest mismatch is fatal either way; and the outcome of every entry is written
# to the hidden /attachments/.curie-attachments-status.json.
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
# The init container's overall fetch deadline, small enough that the vector's
# `stall` entries (a server that goes silent for STALL seconds) cross it in a CI
# run. The per-fetch socket timeout is bounded by what is left of it.
FETCH_TIMEOUT=2
STALL=5
VECTOR="${VECTOR:-$(cd "$CHART/../.." && pwd)/tests/vectors/attachment-init-outcomes.json}"

echo "=== Rendering the sandbox pod (mountPath=$MOUNT, maxFileBytes=$CAP, fetchTimeoutSeconds=$FETCH_TIMEOUT) ==="
# The lane ships OFF (worker.attachments.enabled: false), so every assertion
# below is about what an operator gets after switching it on. The off state has
# its own gate in attachment-init-assertions.sh: with the flag false the
# rendered pod carries no attachments-init container at all.
helm template curie "$CHART" --namespace dev \
  --set "worker.attachments.enabled=true" \
  --set "agentSandbox.runner.attachments.mountPath=$MOUNT" \
  --set "worker.attachments.maxFileBytes=$CAP" \
  --set "agentSandbox.runner.attachments.fetchTimeoutSeconds=$FETCH_TIMEOUT" \
  > "$TMP/rendered.yaml"

python3 - "$TMP/rendered.yaml" "$MOUNT" "$CAP" "$VECTOR" "$FETCH_TIMEOUT" "$STALL" <<'PY'
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import yaml

RENDERED, MOUNT, CAP = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
VECTOR = json.loads(Path(sys.argv[4]).read_text())
FETCH_TIMEOUT, STALL = int(sys.argv[5]), int(sys.argv[6])
STATUS_FILE = VECTOR["status_file"]

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
    # path -> HTTP status to answer with instead of the object.
    errors: dict[str, int] = {}
    # paths that send the headers and one byte, then go silent for STALL seconds.
    stalls: set[str] = set()
    requested: list[str] = []

    def do_GET(self):  # noqa: N802 -- BaseHTTPRequestHandler's own name
        self.requested.append(self.path)
        if self.path in self.errors:
            self.send_response(self.errors[self.path])
            self.end_headers()
            return
        if self.path in self.stalls:
            payload = self.objects[self.path]
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload[:1])
                self.wfile.flush()
                time.sleep(STALL)
                self.wfile.write(payload[1:])
            except OSError:
                pass
            return
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

# The names a person recognises must survive EXACTLY: the name on the reference
# is the one the worker recorded in the thread's ledger and told the agent
# about, so the path a notice gave is the path on disk (ADR 0205 decision 4).
if landed != {"report.csv": first, "notes.txt": second}:
    fail(f"the recorded names were not written exactly: {sorted(landed)}")
print("  ok: each file is written under exactly the recorded name")

# --- (a2) names are never renamed -------------------------------------------
#
# Two uploads sharing a filename used to be disambiguated HERE (report.pdf,
# report-2.pdf). ADR 0205 decision 4 moves that to the worker, which fixes the
# on-disk name once against the whole thread's ledger; this program writes "n"
# exactly and refuses a duplicate. The vector below carries those cases.

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
# `name` is text supplied by whoever uploaded the file. It used to be reduced to
# a basename here; the worker now records a clean name, so anything that is not
# already its own basename is refused rather than repaired. The vector below
# carries ../, dir/, "", ".", "..", "/" and the reserved .curie- prefix.

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

# --- (i) the shared ADR 0205 vector -----------------------------------------
#
# Every case in tests/vectors/attachment-init-outcomes.json, through the
# rendered program. The docker driver runs the same file, so a case that changes
# here and not there fails one of the two.

PAST, FUTURE = 1_000_000_000, 2_000_000_000


def vector_ref(case_index, case):
    """(encoded ref, {url path -> entry}) for one vector case."""

    wire, paths = [], {}
    for index, item in enumerate(case["entries"]):
        body = item["body"].encode()
        path = f"/vector-{case_index}-{index}"
        served = body + b" tampered" if item["serve"] == "digest_mismatch" else body
        _Store.objects[path] = served
        if item["serve"].startswith("http_"):
            _Store.errors[path] = int(item["serve"].removeprefix("http_"))
        if item["serve"] == "stall":
            _Store.stalls.add(path)
        entry_wire = {
            "n": item["n"],
            "u": f"{BASE}{path}",
            "s": hashlib.sha256(body).hexdigest(),
            "b": len(body),
            "e": PAST if item["serve"] == "expired" else FUTURE,
            "m": "text/plain",
        }
        if "c" in item:
            entry_wire["c"] = item["c"]
        wire.append(entry_wire)
        paths[path] = item
    return encode(wire), paths


def is_current(item):
    return item.get("c", 1) == 1


def check_common(label, case, paths, requested):
    """The invariants every case carries, whatever its outcome."""

    fetched = [paths[path] for path in requested if path in paths]
    seen_earlier = False
    for item in fetched:
        if not is_current(item):
            seen_earlier = True
        elif seen_earlier:
            fail(
                f"{label}: current entry {item['n']!r} was fetched after an earlier "
                f"one (fetch order {[i['n'] for i in fetched]}). The current "
                "message's files come first, so a slow earlier file can never "
                "starve the ones the person just sent."
            )
    for item in fetched:
        if item["serve"] == "expired":
            fail(f"{label}: expired reference {item['n']!r} was fetched; it must be skipped unfetched")
    for row in case.get("status") or []:
        if row["reason"] == "deadline" and any(i["n"] == row["name"] for i in fetched):
            fail(
                f"{label}: {row['name']!r} was fetched although it was reached past "
                "the overall deadline; it must be recorded and skipped without a fetch"
            )


def hidden_entries():
    return sorted(child.name for child in MOUNT.iterdir() if child.name.startswith("."))


def read_status():
    path = MOUNT / STATUS_FILE
    if not path.is_file():
        return None
    return json.loads(path.read_text())


def by_name(rows):
    return sorted(rows, key=lambda row: row["name"])


def run_vector_case(case_index, case):
    label = f"vector {case['name']}"
    ref, paths = vector_ref(case_index, case)
    _Store.requested.clear()
    started = time.monotonic()
    completed = run(ref)
    elapsed = time.monotonic() - started
    requested = list(_Store.requested)

    if case["outcome"] == "fatal":
        if completed.returncode == 0:
            fail(
                f"{label}: the init container exited 0; the vector says this boot "
                f"is refused.\n    stdout={completed.stdout!r}\n    stderr={completed.stderr!r}"
            )
        if visible():
            fail(f"{label}: refused but left {sorted(visible())} visible in the mount")
        check_common(label, case, paths, requested)
        print(f"  ok: {label}: refused (exit {completed.returncode}), nothing visible")
        return

    if completed.returncode != 0:
        fail(
            f"{label}: the init container exited {completed.returncode}; the vector "
            f"says this boot proceeds.\n    stdout={completed.stdout!r}\n"
            f"    stderr={completed.stderr!r}"
        )
    expected_visible = {name: body.encode() for name, body in case["visible"].items()}
    if visible() != expected_visible:
        fail(f"{label}: the mount holds {sorted(visible())}, expected {sorted(expected_visible)} (bytes included)")
    # Only the status file may remain hidden: a leftover stage dir or a status
    # temp file means the write was not finished by a rename.
    if hidden_entries() != [STATUS_FILE]:
        fail(
            f"{label}: hidden entries {hidden_entries()} in the mount; expected only "
            f"{STATUS_FILE} (written to a temp name and renamed, stage dir removed)"
        )
    status = read_status()
    if not isinstance(status, dict) or set(status) != {"v", "files"} or status["v"] != VECTOR["status_version"]:
        fail(f"{label}: {STATUS_FILE} is {status!r}, expected {{'v': {VECTOR['status_version']}, 'files': [...]}}")
    for row in status["files"]:
        if set(row) != {"name", "status", "reason"}:
            fail(f"{label}: status row {row!r} must carry exactly name, status and reason")
    if by_name(status["files"]) != by_name(case["status"]):
        fail(f"{label}: status files {by_name(status['files'])} != expected {by_name(case['status'])}")
    if elapsed > FETCH_TIMEOUT + STALL:
        fail(f"{label}: took {elapsed:.1f}s; the {FETCH_TIMEOUT}s deadline did not bound the boot")
    check_common(label, case, paths, requested)
    print(f"  ok: {label}: {sorted(expected_visible)} visible, status recorded")


# Every case runs even after one fails, so a red run names the whole gap at once.
vector_failed = []
for case_index, case in enumerate(VECTOR["cases"]):
    try:
        run_vector_case(case_index, case)
    except SystemExit:
        vector_failed.append(case["name"])
if vector_failed:
    fail(f"{len(vector_failed)} of {len(VECTOR['cases'])} vector cases failed: {vector_failed}")

server.shutdown()

print()
print(
    "PASS: the rendered attachments-init program materializes verified bytes, "
    "and refuses -- writing nothing the runner could announce -- on a digest "
    "mismatch, an over-cap body, an expired reference, a redirect, a non-HTTP "
    "url and an unclean name, while an absent or empty reference is a clean "
    "no-op; and every case in the shared ADR 0205 vector -- exact names, current "
    "first and all-or-nothing, earlier files best effort with a recorded reason, "
    "digest mismatch always fatal -- comes out as the vector says."
)
PY
