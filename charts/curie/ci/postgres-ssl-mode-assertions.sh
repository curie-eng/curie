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
#   6. The retained legacy migrate probe lifts ssl= out of the DSN into the
#      real asyncpg.connect kwarg as the string "require" (not True), then
#      reports the actual closed-port connection error class.
#   7. Current migrate dispatch enters schema_compat before legacy readiness.
#      Its exact moved wait function uses the same real driver boundary and
#      preserves safe diagnostics. Removing its TLS argument must fail.
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
echo "=== Current and legacy migrate readiness with real asyncpg against a closed port ==="
REAL_PYTHON=""
if python3 -c 'import asyncpg' >/dev/null 2>&1; then
  REAL_PYTHON="$(command -v python3)"
elif command -v uv >/dev/null 2>&1 && (cd "$REPO_ROOT" && uv run python -c 'import asyncpg') >/dev/null 2>&1; then
  REAL_PYTHON="$(cd "$REPO_ROOT" && uv run python -c 'import sys; print(sys.executable)')"
else
  python3 -m venv "$TMP/asyncpg-venv" \
    || fail "[6/7] could not create a venv to install asyncpg"
  "$TMP/asyncpg-venv/bin/pip" install --quiet asyncpg \
    || fail "[6/7] could not install asyncpg into a throwaway venv"
  REAL_PYTHON="$TMP/asyncpg-venv/bin/python"
fi
BYO_REQUIRE_DIR="$BYO_REQUIRE_DIR" REAL_PYTHON="$REAL_PYTHON" \
REPO_ROOT="$REPO_ROOT" python3 <<'PY'
import os
import pathlib
import re
import socket
import subprocess
import sys
import textwrap

import yaml

templates_dir = pathlib.Path(os.environ["BYO_REQUIRE_DIR"])


def die(message):
    print(f"FAIL: [6/7] {message}", file=sys.stderr)
    raise SystemExit(1)


docs = [doc for doc in yaml.safe_load_all((templates_dir / "schema-migrate.yaml").read_text()) if doc]
migrate = []
for doc in docs:
    spec = (doc.get("spec") or {}).get("template", {}).get("spec", {})
    migrate.extend(item for item in spec.get("containers", []) if item.get("name") == "schema-migrate")
if len(migrate) != 1:
    die(f"expected exactly one schema-migrate container, found {len(migrate)}")
process = list(migrate[0].get("command") or []) + list(migrate[0].get("args") or [])
if len(process) != 3 or process[1] != "-c":
    die("schema-migrate must render a shell -c dispatch")
script = process[2]
dispatch = "exec python -m curie_api.schema_compat upgrade"
if dispatch not in script or "attempt=1" not in script:
    die("schema-migrate must dispatch current-image readiness and retain legacy readiness")
if script.index(dispatch) > script.index("attempt=1"):
    die("current-image readiness must enter schema_compat before the legacy shell probe")
probes = [
    textwrap.dedent(candidate)
    for candidate in re.findall(r"python -c '([^']*)'", script, re.DOTALL)
    if "DATABASE_URL" in candidate and "asyncpg.connect" in candidate
]
if len(probes) != 1:
    die(f"expected one retained legacy asyncpg readiness probe, found {len(probes)}")

# Observe the real driver boundary, then delegate unchanged. Wrong schemes,
# misplaced ssl settings, boolean TLS modes and changed timeouts must fail.
observer = r'''
import asyncio
import os
from urllib.parse import parse_qs, urlparse

import asyncpg

original_connect = asyncpg.connect

async def observe_connect(database_url, *args, **kwargs):
    print("REAL_CONNECT_BOUNDARY_ENTERED", flush=True)
    parsed = urlparse(database_url)
    assert parsed.scheme == "postgresql", "SQLAlchemy scheme reached asyncpg"
    assert "ssl" not in parse_qs(parsed.query), "ssl reached the server settings"
    if "ssl" not in kwargs:
        print("REAL_CONNECT_REJECTED_REASON=missing_ssl_kwarg", flush=True)
        raise ValueError("readiness_missing_ssl_kwarg")
    assert kwargs.get("ssl") == "require", "TLS must remain the require string"
    assert kwargs.get("timeout") == 2, "readiness connect timeout changed"
    print("REAL_CONNECT_ARGS_OK", flush=True)
    return await original_connect(database_url, *args, **kwargs)

asyncpg.connect = observe_connect
'''
current = r'''
import ast
import pathlib
import sys
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlunparse

# Execute the exact moved readiness function and its actual source constants.
# DATABASE_URL is a configuration input; the driver and stores are never faked.
source = pathlib.Path(os.environ["CURRENT_SCHEMA_SOURCE"])
tree = ast.parse(source.read_text())
functions = [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "wait_for_postgres"]
assert len(functions) == 1, "current readiness function is missing or ambiguous"
constants = {}
for node in tree.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in {
                "POSTGRES_ATTEMPTS", "POSTGRES_RETRY_S", "POSTGRES_CONNECT_TIMEOUT_S",
            }:
                constants[target.id] = ast.literal_eval(node.value)
assert constants == {
    "POSTGRES_ATTEMPTS": 60, "POSTGRES_RETRY_S": 2.0, "POSTGRES_CONNECT_TIMEOUT_S": 2.0,
}, "current readiness defaults changed"

if os.environ.get("SSL_NEGATIVE_CONTROL") == "1":
    class RemoveSslArgument(ast.NodeTransformer):
        removed = 0

        def visit_Assign(self, node):
            for target in node.targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "connect_kwargs"
                    and isinstance(target.slice, ast.Constant)
                    and target.slice.value == "ssl"
                ):
                    self.removed += 1
                    return ast.copy_location(ast.Pass(), node)
            return self.generic_visit(node)

    mutant = RemoveSslArgument()
    functions[0] = mutant.visit(functions[0])
    assert mutant.removed == 1, "negative control could not remove the TLS argument"
    print("SSL_MUTATION_APPLIED=1", flush=True)

namespace = {
    "asyncio": asyncio, "asyncpg": asyncpg, "sys": sys, "Any": Any,
    "urlparse": urlparse, "parse_qsl": parse_qsl, "urlencode": urlencode,
    "urlunparse": urlunparse,
    "get_settings": lambda: SimpleNamespace(database_url=os.environ["DATABASE_URL"]),
    **constants,
}
module = ast.fix_missing_locations(ast.Module(body=functions, type_ignores=[]))
exec(compile(module, str(source), "exec"), namespace)
# One real failure proves TLS/connect arguments; separate diagnostic ladder
# tests retain the complete 60-attempt wait contract checked above.
namespace["POSTGRES_ATTEMPTS"] = 1
raise SystemExit(asyncio.run(namespace["wait_for_postgres"]()))
'''

with socket.socket() as closed:
    # Keep this bound, unlistening endpoint owned until every child exits.
    closed.bind(("127.0.0.1", 0))
    host, port = closed.getsockname()
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    env["DATABASE_URL"] = f"postgresql+asyncpg://curie:not-a-secret@{host}:{port}/curie?ssl=require"
    env["CURRENT_SCHEMA_SOURCE"] = str(pathlib.Path(os.environ["REPO_ROOT"], "apps/api/src/curie_api/schema_compat.py"))

    def run(boundary, *, negative=False):
        result = subprocess.run(
            [os.environ["REAL_PYTHON"], "-c", observer + boundary],
            env={**env, "SSL_NEGATIVE_CONTROL": "1" if negative else "0"},
            capture_output=True, text=True, timeout=15, check=False,
        )
        output = (result.stdout or "") + (result.stderr or "")
        if result.returncode == 0:
            die("readiness unexpectedly succeeded against the owned closed port")
        return output

    for aid, label, boundary in ((6, "legacy shell", probes[0]), (7, "current Python", current)):
        output = run(boundary)
        if output.count("REAL_CONNECT_BOUNDARY_ENTERED") != 1 or output.count("REAL_CONNECT_ARGS_OK") != 1:
            die(f"{label} readiness did not preserve the real asyncpg TLS/connect arguments")
        if "ConnectionRefusedError" not in output and "OSError" not in output:
            die(f"{label} readiness did not report a real closed-port connection error class")
        for forbidden in ("IndentationError", "SyntaxError", "ClientConfigurationError", "not-a-secret"):
            if forbidden in output:
                die(f"{label} readiness emitted an invalid or credential-bearing diagnostic")
        print(f"  [{aid}] {label} readiness preserves ssl=require and real closed-port diagnostics: OK")

    negative = run(current, negative=True)
    if negative.count("SSL_MUTATION_APPLIED=1") != 1:
        die("negative control setup did not confirm exactly one TLS-argument mutation")
    if negative.count("REAL_CONNECT_BOUNDARY_ENTERED") != 1:
        die("negative control did not invoke the current readiness driver boundary exactly once")
    if negative.count("REAL_CONNECT_REJECTED_REASON=missing_ssl_kwarg") != 1:
        die("negative control did not fail for the precise missing TLS argument")
    if "REAL_CONNECT_ARGS_OK" in negative or "probe error class: ValueError" not in negative:
        die("missing TLS argument did not produce the current readiness failure diagnostic")
    print("  [7] negative control: missing current TLS argument is rejected: OK")
PY

echo
echo "PASS: postgres.sslMode renders a TLS parameter on every Postgres DSN"
echo "      (asyncpg ?ssl=require, Prisma ?sslmode=require&sslaccept=accept_invalid_certs), the default and"
echo "      BYO-plain renders stay suffix-free, invalid values and require+"
echo "      in-chart deploy are refused by name, and the schema-migrate probe"
echo "      connects through DATABASE_URL with asyncpg, including against a"
echo "      closed port with the real driver."
