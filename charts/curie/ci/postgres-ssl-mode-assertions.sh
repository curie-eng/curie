#!/usr/bin/env bash
#
# Render-assertion test for Postgres TLS mode on every DSN (#2431).
#
# postgres.deploy: false points the chart at a managed Postgres, but every DSN
# _helpers.tpl composes used to end at the database name. There was no value
# that asked for TLS, so on a store that enforces it (RDS ships rds.force_ssl=1)
# the install worked only because each driver's default happened to be
# "prefer". A parameter-group change or a driver default moving would drop to
# plaintext or refuse to connect with no render-time signal.
#
# The two driver families also disagree on the spelling, which is why an
# operator cannot fix this from postgres.auth.database:
#
#   api, worker (SQLAlchemy + asyncpg)  ->  ?ssl=require
#   Langfuse web + worker (Prisma)      ->  ?sslmode=require&sslaccept=accept_invalid_certs
#
# Prisma/quaint accepts only disable|prefer|require for sslmode. #2476 rendered
# ?sslmode=no-verify, which Prisma logs at debug and treats as prefer, so the
# Langfuse half did not enforce TLS (#2507). sslaccept=accept_invalid_certs is
# the no-CA posture; verify-full still needs a mounted CA (#2508).
#
# One more consumer: the api migrate init container calls asyncpg.connect() on
# the DSN directly. asyncpg's DSN parser knows sslmode, not ssl, so an ssl=
# query arg is forwarded to the server as an unknown setting and the API never
# leaves init. The probe must lift ssl into the connect kwarg.
#
# postgres.sslMode is threaded through ONE helper (curie.postgres.dsnParams)
# included from both curie.env.postgres and curie.langfuse.env. The bug class
# this chart has hit twice (#2052, #2327) is "two consumer groups read the
# same postgres.* field and only one of them was updated".
#
# Asserts:
#
#   1. default: DATABASE_URL on api, worker, migrate, and both Langfuse
#      containers has no TLS query parameter, byte-for-byte the pre-change
#      shape, so an existing install does not change on upgrade.
#   2. byo-plain (deploy=false + host, sslMode left default): still no TLS
#      query parameter. This is what makes assertion 3 non-vacuous: without
#      it, a template that emitted the suffix unconditionally would pass.
#   3. byo-require (deploy=false + host + sslMode=require): api/worker/migrate
#      DSNs end in ?ssl=require; both Langfuse DSNs end in
#      ?sslmode=require&sslaccept=accept_invalid_certs. Host still resolves to
#      the BYO host in the SAME render.
#   4. NEGATIVE CONTROL -- the in-chart guard: helm template
#      --set postgres.sslMode=require with postgres.deploy left true exits
#      non-zero and stderr names BOTH postgres.sslMode and postgres.deploy.
#      The in-chart Postgres StatefulSet serves no TLS listener.
#   5. NEGATIVE CONTROL -- invalid values: prefer, disable, verify-full, a
#      quoted "false", and 0 each fail the render and name postgres.sslMode.
#      Sprig `default` swallows false and 0, so these are read raw.
#   6. The migrate probe extracted from the byo-require render lifts ssl= out
#      of the DSN into the asyncpg.connect kwarg as the string "require" (not
#      True, which asyncpg maps to verify-full), then a fake driver fails
#      against a closed port with a connection error class.
#   7. The same extracted probe runs against a closed port with the installed
#      asyncpg driver (no fake), so a scheme or ssl-kwarg rejection cannot hide
#      behind the argument stub.
#   8. Prisma sslmode set: every rendered Langfuse DATABASE_URL's sslmode, if
#      present, is one of disable|prefer|require. A synthetic no-verify (the
#      #2476 spelling) and verify-full each fail that check, so a later
#      helper that reintroduces a non-Prisma value cannot pass by updating
#      assertion 3's expected string alone.
#
# Every render goes through --output-dir, never a stdout pipe: piping helm
# template in this environment silently truncates a large render at exit 0
# with empty stderr. Structural checks go through PyYAML rather than grep.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render() {
  local name="$1"
  shift
  RENDER_DIR="$TMP/$name"
  rm -rf "$RENDER_DIR"
  helm template rel "$CHART" --namespace default --output-dir "$RENDER_DIR" "$@" >/dev/null \
    || fail "helm template failed for render '$name'"
}

BYO_HOST="pg.acme.internal"

echo "=== Rendering (defaults) ==="
render default
DEFAULT_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering (byo-plain: deploy=false + host, sslMode left default) ==="
render byo-plain \
  --set postgres.deploy=false \
  --set-string postgres.host="$BYO_HOST"
BYO_PLAIN_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering (byo-require: deploy=false + host + sslMode=require) ==="
render byo-require \
  --set postgres.deploy=false \
  --set-string postgres.host="$BYO_HOST" \
  --set-string postgres.sslMode=require
BYO_REQUIRE_DIR="$RENDER_DIR/curie/templates"

DEFAULT_DIR="$DEFAULT_DIR" BYO_PLAIN_DIR="$BYO_PLAIN_DIR" \
BYO_REQUIRE_DIR="$BYO_REQUIRE_DIR" BYO_HOST="$BYO_HOST" \
python3 <<'PY'
import os
import sys
from urllib.parse import parse_qs, urlparse

import yaml

DEFAULT_DIR = os.environ["DEFAULT_DIR"]
BYO_PLAIN_DIR = os.environ["BYO_PLAIN_DIR"]
BYO_REQUIRE_DIR = os.environ["BYO_REQUIRE_DIR"]
BYO_HOST = os.environ["BYO_HOST"]

INCLUSTER_HOST = "rel-curie-postgres"
USER = "postgres"
DB = "postgres"
PASSWORD = "$(POSTGRES_PASSWORD)"

APP_BASE = f"postgresql+asyncpg://{USER}:{PASSWORD}@{{host}}:5432/{DB}"
LANGFUSE_BASE = f"postgresql://{USER}:{PASSWORD}@{{host}}:5432/{DB}"

failures = []
_docs_cache = {}


def load_docs(path):
    if path not in _docs_cache:
        if not os.path.isfile(path):
            _docs_cache[path] = []
        else:
            with open(path) as handle:
                _docs_cache[path] = [doc for doc in yaml.safe_load_all(handle) if doc]
    return _docs_cache[path]


def find_containers(obj, acc, key="containers"):
    if isinstance(obj, dict):
        found = obj.get(key)
        if isinstance(found, list):
            acc.extend(found)
        for value in obj.values():
            find_containers(value, acc, key)
    elif isinstance(obj, list):
        for item in obj:
            find_containers(item, acc, key)


def containers_named(manifest_path, name, *, init=False):
    acc = []
    for doc in load_docs(manifest_path):
        find_containers(doc, acc, "initContainers" if init else "containers")
    return [container for container in acc if isinstance(container, dict) and container.get("name") == name]


def database_url(containers, label):
    if len(containers) != 1:
        failures.append(f"found {len(containers)} container(s) matching {label!r}, expected exactly 1")
        return None
    entries = [entry for entry in (containers[0].get("env") or []) if entry.get("name") == "DATABASE_URL"]
    if len(entries) != 1:
        failures.append(f"{label}: DATABASE_URL rendered {len(entries)} time(s), expected exactly 1")
        return None
    value = entries[0].get("value")
    if not isinstance(value, str) or not value:
        failures.append(f"{label}: DATABASE_URL is {value!r}, expected a non-empty string")
        return None
    return value


def check_url(aid, containers, label, expected, ctx):
    actual = database_url(containers, f"{ctx}:{label}")
    if actual is None:
        return
    if actual != expected:
        failures.append(
            f"[{aid}] {ctx}: DATABASE_URL on {label!r} = {actual!r}, expected {expected!r}"
        )


def consumers(templates_dir, host):
    app = APP_BASE.format(host=host)
    langfuse = LANGFUSE_BASE.format(host=host)
    return [
        ("api", containers_named(f"{templates_dir}/api.yaml", "api"), app),
        ("worker", containers_named(f"{templates_dir}/worker.yaml", "worker"), app),
        (
            "schema-migrate",
            containers_named(f"{templates_dir}/schema-migrate.yaml", "schema-migrate"),
            app,
        ),
        (
            "langfuse-web",
            containers_named(f"{templates_dir}/langfuse.yaml", "langfuse-web"),
            langfuse,
        ),
        (
            "langfuse-worker",
            containers_named(f"{templates_dir}/langfuse.yaml", "langfuse-worker"),
            langfuse,
        ),
    ]


# ---- 1: default, no TLS query parameter, in-cluster host. -----------------
for label, containers, expected in consumers(DEFAULT_DIR, INCLUSTER_HOST):
    check_url("1", containers, label, expected, "default render")
_postgres_manifest = f"{DEFAULT_DIR}/postgres.yaml"
if not (os.path.isfile(_postgres_manifest) and os.path.getsize(_postgres_manifest) > 0):
    failures.append("[1] default render: templates/postgres.yaml did not render (or is empty)")

# ---- 2: byo-plain, still no TLS query parameter, BYO host. ----------------
for label, containers, expected in consumers(BYO_PLAIN_DIR, BYO_HOST):
    check_url("2", containers, label, expected, "byo-plain render")

# ---- 3: byo-require, per-driver suffix, BYO host. -------------------------
for label, containers, expected in consumers(BYO_REQUIRE_DIR, BYO_HOST):
    if label in {"api", "worker", "schema-migrate"}:
        expected = expected + "?ssl=require"
    else:
        expected = expected + "?sslmode=require&sslaccept=accept_invalid_certs"
    check_url("3", containers, label, expected, "byo-require render")

# Prisma/quaint sslmode values: https://www.prisma.io/docs/orm/overview/databases/postgresql
# sslmode=(disable|prefer|require). Unknown values (no-verify, verify-full, ...)
# are logged at debug and treated as prefer, so they cannot enforce TLS.
PRISMA_SSLMODES = frozenset({"disable", "prefer", "require"})


def prisma_sslmode_violations(url):
    modes = parse_qs(urlparse(url).query).get("sslmode", [])
    return [mode for mode in modes if mode not in PRISMA_SSLMODES]


for ctx, templates_dir in (
    ("default render", DEFAULT_DIR),
    ("byo-plain render", BYO_PLAIN_DIR),
    ("byo-require render", BYO_REQUIRE_DIR),
):
    for label, containers, _expected in consumers(templates_dir, "unused"):
        if label not in {"langfuse-web", "langfuse-worker"}:
            continue
        actual = database_url(containers, f"{ctx}:{label}")
        if actual is None:
            continue
        for mode in prisma_sslmode_violations(actual):
            failures.append(
                f"[8] {ctx}: {label} DATABASE_URL sslmode={mode!r} is outside "
                f"Prisma's {sorted(PRISMA_SSLMODES)}"
            )

for synthetic, label in (
    (
        "postgresql://postgres:$(POSTGRES_PASSWORD)@pg.acme.internal:5432/postgres?sslmode=no-verify",
        "no-verify",
    ),
    (
        "postgresql://postgres:$(POSTGRES_PASSWORD)@pg.acme.internal:5432/postgres?sslmode=verify-full",
        "verify-full",
    ),
):
    found = prisma_sslmode_violations(synthetic)
    if not found:
        failures.append(
            f"[8] prisma sslmode set check did not reject synthetic sslmode={label!r}"
        )

require_ok = prisma_sslmode_violations(
    "postgresql://postgres:x@pg.acme.internal:5432/postgres?sslmode=require&sslaccept=accept_invalid_certs"
)
if require_ok:
    failures.append(
        f"[8] prisma sslmode set check rejected a valid require DSN: {require_ok!r}"
    )

if failures:
    for message in failures:
        print(f"FAIL {message}", file=sys.stderr)
    print(f"{len(failures)} python-side assertion(s) failed", file=sys.stderr)
    sys.exit(1)

print("  [1] default: DATABASE_URL has no TLS query parameter on all five consumers: OK")
print("  [2] byo-plain: still no TLS query parameter (require is not unconditional): OK")
print("  [3] byo-require: ?ssl=require on asyncpg, ?sslmode=require&sslaccept=accept_invalid_certs on Prisma: OK")
print("  [8] Prisma sslmode is disable|prefer|require; synthetic no-verify and verify-full are rejected: OK")
PY

echo
echo "=== Rendering (guard: sslMode=require with postgres.deploy left at its default) ==="
GUARD_OUT="$(helm template rel "$CHART" --set-string postgres.sslMode=require 2>&1)" && {
  fail "[4] postgres.sslMode=require with postgres.deploy left true rendered successfully; expected a render-time refusal"
}
for needle in "postgres.sslMode" "postgres.deploy"; do
  if ! printf '%s' "$GUARD_OUT" | grep -qF "$needle"; then
    echo "FAIL: [4] the guard's refusal did not name '$needle'" >&2
    echo "  actual output:" >&2
    printf '%s\n' "$GUARD_OUT" | sed 's/^/    /' >&2
    exit 1
  fi
done
echo "  [4] negative control: sslMode=require + deploy=true is refused at render time, naming both keys: OK"

echo
echo "=== Rendering (guard: invalid sslMode values) ==="
refuse_invalid() {
  local flag="$1"
  local label="$2"
  local out
  out="$(helm template rel "$CHART" \
    --set postgres.deploy=false \
    --set-string postgres.host="$BYO_HOST" \
    $flag 2>&1)" && {
    fail "[5] $label rendered successfully; expected a render-time refusal naming postgres.sslMode"
  }
  if ! printf '%s' "$out" | grep -qF "postgres.sslMode"; then
    echo "FAIL: [5] $label refusal did not name postgres.sslMode" >&2
    echo "  actual output:" >&2
    printf '%s\n' "$out" | sed 's/^/    /' >&2
    exit 1
  fi
}

refuse_invalid "--set-string postgres.sslMode=prefer" "sslMode=prefer"
refuse_invalid "--set-string postgres.sslMode=disable" "sslMode=disable"
refuse_invalid "--set-string postgres.sslMode=verify-full" "sslMode=verify-full"
refuse_invalid "--set-string postgres.sslMode=false" "sslMode=false (quoted)"
refuse_invalid "--set postgres.sslMode=0" "sslMode=0"
echo "  [5] negative control: prefer/disable/verify-full/false/0 each refuse and name postgres.sslMode: OK"

echo
echo "=== Extracted migrate probe against a closed port (byo-require) ==="
BYO_REQUIRE_DIR="$BYO_REQUIRE_DIR" TMP="$TMP" python3 <<'PY'
import os
import pathlib
import socket
import subprocess
import sys
import textwrap

import yaml

templates_dir = os.environ["BYO_REQUIRE_DIR"]
tmp = pathlib.Path(os.environ["TMP"])


def die(message):
    print(f"FAIL: [6] {message}", file=sys.stderr)
    raise SystemExit(1)


docs = [
    doc
    for doc in yaml.safe_load_all(pathlib.Path(templates_dir, "schema-migrate.yaml").read_text())
    if doc
]
migrate = []
for doc in docs:
    spec = (
        (doc.get("spec") or {})
        .get("template", {})
        .get("spec", {})
    )
    for container in spec.get("containers") or []:
        if container.get("name") == "schema-migrate":
            migrate.append(container)
if len(migrate) != 1:
    die(f"expected exactly one schema-migrate container, found {len(migrate)}")

process = list(migrate[0].get("command") or []) + list(migrate[0].get("args") or [])
if len(process) < 3 or process[1] != "-c":
    die(f"migrate init is not a shell -c script: {process[:3]!r}")
script = process[2]
marker = "python -c '"
start = script.find(marker)
if start < 0:
    die("migrate init script has no python -c probe")
start += len(marker)
end = script.find("'", start)
if end < 0:
    die("migrate init python -c probe is not single-quote terminated")
probe_src = textwrap.dedent(script[start:end])
if "DATABASE_URL" not in probe_src or "asyncpg.connect" not in probe_src:
    die("extracted probe does not connect through DATABASE_URL with asyncpg")

closed = socket.socket()
closed.bind(("127.0.0.1", 0))
host, port = closed.getsockname()
closed.close()

fake = tmp / "fake-asyncpg"
fake.mkdir()
(fake / "asyncpg.py").write_text(
    """\
import socket
from urllib.parse import urlparse


class Connection:
    async def close(self):
        return None


async def connect(database_url, timeout=None, **kwargs):
    parsed = urlparse(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    socket.create_connection(
        (parsed.hostname, parsed.port or 5432),
        timeout=timeout if timeout is not None else 2,
    )
    return Connection()
"""
)

database_url = (
    f"postgresql+asyncpg://curie:not-a-secret@{host}:{port}/curie?ssl=require"
)
result = subprocess.run(
    [sys.executable, "-c", probe_src],
    env={
        **os.environ,
        "DATABASE_URL": database_url,
        "PYTHONPATH": str(fake),
    },
    capture_output=True,
    text=True,
    timeout=15,
    check=False,
)
output = (result.stdout or "") + (result.stderr or "")
if result.returncode == 0:
    die(f"probe succeeded against a closed port: {output!r}")
if "ConnectionRefusedError" not in output and "OSError" not in output:
    die(
        "probe against a closed port must fail with a connection error class, "
        f"got {output!r}"
    )
print(
    "  [6] extracted schema-migrate probe uses DATABASE_URL and fails "
    "against a closed port with a connection error class: OK"
)
PY

echo
echo "=== Extracted migrate probe with real asyncpg against a closed port ==="
REAL_PYTHON=""
if python3 -c 'import asyncpg' >/dev/null 2>&1; then
  REAL_PYTHON="$(command -v python3)"
elif command -v uv >/dev/null 2>&1 && (cd "$REPO_ROOT" && uv run python -c 'import asyncpg') >/dev/null 2>&1; then
  REAL_PYTHON="$(cd "$REPO_ROOT" && uv run python -c 'import sys; print(sys.executable)')"
else
  python3 -m venv "$TMP/asyncpg-venv" \
    || fail "[7] could not create a venv to install asyncpg"
  "$TMP/asyncpg-venv/bin/pip" install --quiet asyncpg \
    || fail "[7] could not install asyncpg into a throwaway venv"
  REAL_PYTHON="$TMP/asyncpg-venv/bin/python"
fi
BYO_REQUIRE_DIR="$BYO_REQUIRE_DIR" REAL_PYTHON="$REAL_PYTHON" python3 <<'PY'
import os
import pathlib
import socket
import subprocess
import sys
import textwrap

import yaml

templates_dir = os.environ["BYO_REQUIRE_DIR"]


def die(message):
    print(f"FAIL: [7] {message}", file=sys.stderr)
    raise SystemExit(1)


docs = [
    doc
    for doc in yaml.safe_load_all(pathlib.Path(templates_dir, "schema-migrate.yaml").read_text())
    if doc
]
migrate = []
for doc in docs:
    spec = (doc.get("spec") or {}).get("template", {}).get("spec", {})
    for container in spec.get("containers") or []:
        if container.get("name") == "schema-migrate":
            migrate.append(container)
if len(migrate) != 1:
    die(f"expected exactly one schema-migrate container, found {len(migrate)}")

process = list(migrate[0].get("command") or []) + list(migrate[0].get("args") or [])
script = process[2]
marker = "python -c '"
start = script.find(marker) + len(marker)
end = script.find("'", start)
probe_src = textwrap.dedent(script[start:end])

closed = socket.socket()
closed.bind(("127.0.0.1", 0))
host, port = closed.getsockname()
closed.close()

database_url = (
    f"postgresql+asyncpg://curie:not-a-secret@{host}:{port}/curie?ssl=require"
)
env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
env["DATABASE_URL"] = database_url
real_python = os.environ["REAL_PYTHON"]
result = subprocess.run(
    [real_python, "-c", probe_src],
    env=env,
    capture_output=True,
    text=True,
    timeout=15,
    check=False,
)
output = (result.stdout or "") + (result.stderr or "")
if result.returncode == 0:
    die(f"real asyncpg probe succeeded against a closed port: {output!r}")
if "IndentationError" in output or "SyntaxError" in output:
    die(f"extracted probe is not valid Python: {output!r}")
if "ClientConfigurationError" in output:
    die(f"real asyncpg rejected the lifted ssl kwarg: {output!r}")
if "postgresql+asyncpg" in output and "scheme" in output.lower():
    die(f"probe did not convert the SQLAlchemy scheme before asyncpg.connect: {output!r}")
if "ConnectionRefusedError" not in output and "OSError" not in output:
    die(
        "real asyncpg against a closed port must fail with a connection error class, "
        f"got {output!r}"
    )
print(
    "  [7] extracted schema-migrate probe with real asyncpg fails against a closed "
    "port with a connection error class: OK"
)
PY

echo
echo "PASS: postgres.sslMode renders a TLS parameter on every Postgres DSN"
echo "      (asyncpg ?ssl=require, Prisma ?sslmode=require&sslaccept=accept_invalid_certs), the default and"
echo "      BYO-plain renders stay suffix-free, invalid values and require+"
echo "      in-chart deploy are refused by name, and the schema-migrate probe"
echo "      connects through DATABASE_URL with asyncpg, including against a"
echo "      closed port with the real driver."
