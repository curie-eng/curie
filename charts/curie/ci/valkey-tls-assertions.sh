#!/usr/bin/env bash
#
# Render-assertion test for BYO Valkey over TLS (#2315).
#
# The chart's stated invariant (`charts/curie/CLAUDE.md`, "Every backing store
# follows the same toggle + BYO idiom") is that flipping `<store>.deploy` to
# false repoints EVERY consumer at the BYO `host`/`port`/`auth` fields on the
# same block. Valkey's block carried no TRANSPORT field at all, so the BYO
# branch could only ever produce a cleartext connection -- and every managed
# Redis-compatible service an operator would realistically bring is TLS-only or
# TLS-by-default. redis-py does not negotiate or downgrade, so the connection
# fails outright, taking down the api, worker, dispatcher, and both Langfuse
# Deployments at once.
#
# `valkey.tls` is threaded to all of it through ONE shared helper
# (`curie.valkey.tls`), included from both `curie.env.valkey` (api, worker,
# dispatcher, both upgrade-hook Jobs) and `curie.langfuse.env` (both Langfuse
# Deployments). The bug class this repo has hit twice (#2052, #2327) is exactly
# "two consumer groups read the same `valkey.*` field and only one of them was
# updated" -- silent and asymmetric: the render stays healthy, some consumers
# connect fine, and the rest fail closed with no failing manifest and no
# failing preflight to point at the cause.
#
# Asserts:
#
#   1. `byo-tls` (deploy=false + host + tls=true): VALKEY_TLS == "true" on the
#      api, worker and dispatcher containers, AND on both worker-upgrade-drain
#      hook Job containers (drain and release -- resolved by their `--mode`
#      arg, the way `upgrade-drain-assertions.sh` does, not by container name
#      alone); REDIS_TLS_ENABLED == "true" on both Langfuse Deployment
#      containers. All seven checked in ONE render, so a fix applied to one
#      include site and not its sibling fails this gate.
#   2. `byo-tls` transport/identity parity, in the SAME render: VALKEY_HOST /
#      REDIS_HOST still resolve to the BYO host, and VALKEY_PASSWORD /
#      REDIS_AUTH still resolve to their `secretKeyRef`. Prevents "fixing" TLS
#      by breaking the BYO host or credential path.
#   3. `byo-plain` (the same BYO shape, `valkey.tls` left at its default):
#      every one of the seven containers carries the var with the LITERAL
#      value "false" -- present, and false. This is what makes assertion 1
#      non-vacuous: without it, a template that emitted "true" unconditionally
#      would pass.
#   4. `default` no-regression: same as 3 on the default render, plus the
#      in-chart `templates/valkey.yaml` still renders. A BYO knob must not
#      disturb an install that never set it.
#   5. NEGATIVE CONTROL -- the guard: `helm template --set valkey.tls=true`
#      with `valkey.deploy` left at its default `true` exits non-zero, and its
#      stderr names BOTH `valkey.tls` and `valkey.deploy`. Asserting only the
#      exit code would pass against any unrelated template error. The in-chart
#      `valkey/valkey:8-alpine` StatefulSet serves no TLS listener, so
#      rendering TLS against it would break all seven consumers at once with a
#      perfectly healthy-looking manifest.
#   6. NEGATIVE CONTROL -- string coercion: `--set-string valkey.tls=false` on
#      a BYO render still yields "false" everywhere and does not trip the
#      guard. Go templates read a quoted `"false"` as truthy; this is the same
#      scar `security.allowDevDefaults` already carries (`_helpers.tpl:748`).
#   7. `byo-tls` renders NO in-chart valkey manifest (`[[ -s
#      "$DIR/valkey.yaml" ]]` is the `--output-dir` signal a template rendered
#      nothing, per the sibling scripts).
#
# Every render goes through `--output-dir`, never a stdout pipe: piping `helm
# template` in this environment silently truncates a large render at exit 0
# with empty stderr, which reads as a passing assertion against manifests that
# were never examined. Structural checks go through PyYAML rather than grep,
# for the reason `valkey-existing-secret-assertions.sh` gives -- a
# line-oriented reader mis-reads a requoted value or a reordered key.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

render() {
  local name="$1"
  shift
  RENDER_DIR="$TMP/$name"
  rm -rf "$RENDER_DIR"
  helm template rel "$CHART" --output-dir "$RENDER_DIR" "$@" >/dev/null \
    || fail "helm template failed for render '$name'"
}

BYO_HOST="redis.acme.internal"

# The dispatcher Deployment (curie.dispatcher.enabled) renders only when BOTH
# Slack tokens are present (issue #1759) -- a token-less default install skips
# it entirely rather than crash-looping. Every render below sets a placeholder
# pair so the dispatcher container is present to assert against; the values
# are inert (Socket Mode dials out, nothing in this script connects to Slack).
DISPATCHER_SLACK_ARGS=(
  --set-string dispatcher.slack.appToken=xapp-tls-assertions-placeholder
  --set-string dispatcher.slack.botToken=xoxb-assert
)

echo "=== Rendering (defaults) ==="
render default "${DISPATCHER_SLACK_ARGS[@]}"
DEFAULT_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering (byo-tls: deploy=false + host + tls=true) ==="
render byo-tls \
  "${DISPATCHER_SLACK_ARGS[@]}" \
  --set valkey.deploy=false \
  --set-string valkey.host="$BYO_HOST" \
  --set valkey.tls=true
BYO_TLS_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering (byo-plain: deploy=false + host, tls left default) ==="
render byo-plain \
  "${DISPATCHER_SLACK_ARGS[@]}" \
  --set valkey.deploy=false \
  --set-string valkey.host="$BYO_HOST"
BYO_PLAIN_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering (byo-string-false: --set-string valkey.tls=false) ==="
render byo-string-false \
  "${DISPATCHER_SLACK_ARGS[@]}" \
  --set valkey.deploy=false \
  --set-string valkey.host="$BYO_HOST" \
  --set-string valkey.tls=false
BYO_STRING_FALSE_DIR="$RENDER_DIR/curie/templates"

# ---------------------------------------------------------------------- 7
# A missing manifest file is --output-dir's signal that the whole template
# rendered nothing (the same check valkey-existing-secret-assertions.sh uses);
# an empty document would not be distinguishable.
if [[ -s "$BYO_TLS_DIR/valkey.yaml" ]]; then
  fail "[7] byo-tls (valkey.deploy=false + valkey.tls=true) still rendered templates/valkey.yaml"
fi
echo "  [7] byo-tls renders no in-chart valkey manifest: OK"

# -------------------------------------------------------------- 1, 2, 3, 4, 6
DEFAULT_DIR="$DEFAULT_DIR" BYO_TLS_DIR="$BYO_TLS_DIR" BYO_PLAIN_DIR="$BYO_PLAIN_DIR" \
BYO_STRING_FALSE_DIR="$BYO_STRING_FALSE_DIR" BYO_HOST="$BYO_HOST" \
python3 <<'PY'
import os
import sys

import yaml

DEFAULT_DIR = os.environ["DEFAULT_DIR"]
BYO_TLS_DIR = os.environ["BYO_TLS_DIR"]
BYO_PLAIN_DIR = os.environ["BYO_PLAIN_DIR"]
BYO_STRING_FALSE_DIR = os.environ["BYO_STRING_FALSE_DIR"]
BYO_HOST = os.environ["BYO_HOST"]

# `helm template rel <chart>` -> fullname `rel-curie`, so the chart's own
# Secret is `rel-curie-secrets` (same convention as
# valkey-existing-secret-assertions.sh). Hardcoded rather than derived: the
# BYO renders below never set valkey.existingSecret, so the password path must
# still fall back to this exact chart-managed Secret.
CHART_SECRET_NAME = "rel-curie-secrets"

failures = []


_docs_cache = {}


def load_docs(path):
    # Each manifest is queried several times per render (both hook modes,
    # both Langfuse containers, every assertion); parse it once.
    if path not in _docs_cache:
        if not os.path.isfile(path):
            _docs_cache[path] = []
        else:
            with open(path) as f:
                _docs_cache[path] = [d for d in yaml.safe_load_all(f) if d]
    return _docs_cache[path]


def find_containers(obj, acc):
    if isinstance(obj, dict):
        containers = obj.get("containers")
        if isinstance(containers, list):
            acc.extend(containers)
        for v in obj.values():
            find_containers(v, acc)
    elif isinstance(obj, list):
        for item in obj:
            find_containers(item, acc)


def all_containers(manifest_path):
    containers = []
    for d in load_docs(manifest_path):
        find_containers(d, containers)
    return [c for c in containers if isinstance(c, dict)]


def mode_of(container):
    """The value following `--mode` in a container's command, or None.

    Both worker-upgrade-drain hook Job containers live in the SAME manifest
    file and are told apart by which mode they run, not by container identity
    alone -- the same signal upgrade-drain-assertions.sh checks.
    """
    command = container.get("command") or []
    for i, arg in enumerate(command):
        if arg == "--mode" and i + 1 < len(command):
            return command[i + 1]
    return None


def by_name(manifest_path, name):
    return [c for c in all_containers(manifest_path) if c.get("name") == name]


def by_mode(manifest_path, mode):
    return [c for c in all_containers(manifest_path) if mode_of(c) == mode]


def resolve_env_entry(aid, containers, label, env_name, ctx):
    """The single env entry `env_name` on the single container matching `label`.

    Records a failure and returns None when either is not exactly one: a
    missing var and a var rendered twice are both drift, and a label matching
    zero or two containers means the manifest shape moved under the gate.
    """
    if len(containers) != 1:
        failures.append(
            f"[{aid}] {ctx}: found {len(containers)} container(s) matching "
            f"{label!r}, expected exactly 1"
        )
        return None
    entries = [e for e in (containers[0].get("env") or []) if e.get("name") == env_name]
    if len(entries) != 1:
        failures.append(
            f"[{aid}] {ctx}: {env_name} rendered {len(entries)} time(s) on "
            f"{label!r}, expected exactly 1"
        )
        return None
    return entries[0]


def check_literal(aid, containers, label, env_name, expected, ctx):
    entry = resolve_env_entry(aid, containers, label, env_name, ctx)
    if entry is None:
        return
    value = entry.get("value")
    if value != expected:
        failures.append(
            f"[{aid}] {ctx}: {env_name} on {label!r} = {value!r}, expected {expected!r}"
        )


def check_secret_ref(aid, containers, label, env_name, expected_name, expected_key, ctx):
    entry = resolve_env_entry(aid, containers, label, env_name, ctx)
    if entry is None:
        return
    ref = (entry.get("valueFrom") or {}).get("secretKeyRef") or {}
    if ref.get("name") != expected_name:
        failures.append(
            f"[{aid}] {ctx}: {env_name} secretKeyRef.name on {label!r} = "
            f"{ref.get('name')!r}, expected {expected_name!r}"
        )
    if ref.get("key") != expected_key:
        failures.append(
            f"[{aid}] {ctx}: {env_name} secretKeyRef.key on {label!r} = "
            f"{ref.get('key')!r}, expected {expected_key!r}"
        )


# The full TLS-carrying consumer set: every container the chart's two shared
# env helpers (curie.env.valkey, curie.langfuse.env) reach. Reused for
# assertions 1, 3, and 4 so a fix applied to one include site and not its
# sibling fails every one of them, not just the render it happened to touch.
def tls_consumers(templates_dir):
    return [
        ("api", "VALKEY_TLS", by_name(f"{templates_dir}/api.yaml", "api")),
        ("worker", "VALKEY_TLS", by_name(f"{templates_dir}/worker.yaml", "worker")),
        ("dispatcher", "VALKEY_TLS", by_name(f"{templates_dir}/dispatcher.yaml", "dispatcher")),
        (
            "upgrade-drain (drain)",
            "VALKEY_TLS",
            by_mode(f"{templates_dir}/worker-upgrade-drain.yaml", "drain"),
        ),
        (
            "upgrade-drain (release)",
            "VALKEY_TLS",
            by_mode(f"{templates_dir}/worker-upgrade-drain.yaml", "release"),
        ),
        (
            "langfuse-web",
            "REDIS_TLS_ENABLED",
            by_name(f"{templates_dir}/langfuse.yaml", "langfuse-web"),
        ),
        (
            "langfuse-worker",
            "REDIS_TLS_ENABLED",
            by_name(f"{templates_dir}/langfuse.yaml", "langfuse-worker"),
        ),
    ]


# ---- 1: byo-tls, all seven consumer containers carry TLS == "true". --------
for label, env_name, containers in tls_consumers(BYO_TLS_DIR):
    check_literal("1", containers, label, env_name, "true", "byo-tls render")

# ---- 2: byo-tls transport/identity parity, in the SAME render. ------------
check_literal("2", by_name(f"{BYO_TLS_DIR}/api.yaml", "api"), "api", "VALKEY_HOST", BYO_HOST, "byo-tls render")
check_literal("2", by_name(f"{BYO_TLS_DIR}/worker.yaml", "worker"), "worker", "VALKEY_HOST", BYO_HOST, "byo-tls render")
check_literal(
    "2", by_name(f"{BYO_TLS_DIR}/dispatcher.yaml", "dispatcher"), "dispatcher",
    "VALKEY_HOST", BYO_HOST, "byo-tls render",
)
check_literal(
    "2", by_name(f"{BYO_TLS_DIR}/langfuse.yaml", "langfuse-web"), "langfuse-web",
    "REDIS_HOST", BYO_HOST, "byo-tls render",
)
check_literal(
    "2", by_name(f"{BYO_TLS_DIR}/langfuse.yaml", "langfuse-worker"), "langfuse-worker",
    "REDIS_HOST", BYO_HOST, "byo-tls render",
)
check_secret_ref(
    "2", by_name(f"{BYO_TLS_DIR}/api.yaml", "api"), "api",
    "VALKEY_PASSWORD", CHART_SECRET_NAME, "valkeyPassword", "byo-tls render",
)
check_secret_ref(
    "2", by_name(f"{BYO_TLS_DIR}/worker.yaml", "worker"), "worker",
    "VALKEY_PASSWORD", CHART_SECRET_NAME, "valkeyPassword", "byo-tls render",
)
check_secret_ref(
    "2", by_name(f"{BYO_TLS_DIR}/dispatcher.yaml", "dispatcher"), "dispatcher",
    "VALKEY_PASSWORD", CHART_SECRET_NAME, "valkeyPassword", "byo-tls render",
)
check_secret_ref(
    "2", by_name(f"{BYO_TLS_DIR}/langfuse.yaml", "langfuse-web"), "langfuse-web",
    "REDIS_AUTH", CHART_SECRET_NAME, "valkeyPassword", "byo-tls render",
)
check_secret_ref(
    "2", by_name(f"{BYO_TLS_DIR}/langfuse.yaml", "langfuse-worker"), "langfuse-worker",
    "REDIS_AUTH", CHART_SECRET_NAME, "valkeyPassword", "byo-tls render",
)

# ---- 3: byo-plain no-regression: every container carries the LITERAL
#         "false" -- this is what makes assertion 1 non-vacuous. -----------
for label, env_name, containers in tls_consumers(BYO_PLAIN_DIR):
    check_literal("3", containers, label, env_name, "false", "byo-plain render")

# ---- 4: default no-regression, same shape as 3. ---------------------------
for label, env_name, containers in tls_consumers(DEFAULT_DIR):
    check_literal("4", containers, label, env_name, "false", "default render")
_default_valkey_manifest = f"{DEFAULT_DIR}/valkey.yaml"
if not (os.path.isfile(_default_valkey_manifest) and os.path.getsize(_default_valkey_manifest) > 0):
    failures.append("[4] default render: templates/valkey.yaml did not render (or is empty)")

# ---- 6: NEGATIVE CONTROL -- a quoted "false" from --set-string must not
#         read as truthy (the security.allowDevDefaults trap). -------------
for label, env_name, containers in tls_consumers(BYO_STRING_FALSE_DIR):
    check_literal(
        "6", containers, label, env_name, "false",
        "byo render with --set-string valkey.tls=false",
    )

if failures:
    for msg in failures:
        print(f"FAIL {msg}", file=sys.stderr)
    print(f"{len(failures)} python-side assertion(s) failed", file=sys.stderr)
    sys.exit(1)

print("  [1] byo-tls: VALKEY_TLS/REDIS_TLS_ENABLED == true on all seven consumer containers: OK")
print("  [2] byo-tls: VALKEY_HOST/REDIS_HOST and VALKEY_PASSWORD/REDIS_AUTH parity preserved: OK")
print("  [3] byo-plain: every consumer carries the literal \"false\": OK")
print("  [4] default: every consumer carries \"false\" and templates/valkey.yaml still renders: OK")
print("  [6] negative control: --set-string valkey.tls=false does not read as truthy: OK")
PY

echo
echo "=== Rendering (guard: valkey.tls=true with valkey.deploy left at its default) ==="
# ---------------------------------------------------------------------- 5
# NEGATIVE CONTROL, asserted by EXECUTING the rejected configuration, not by
# reading the helper: a guard that has never been seen refusing is a guard
# nobody has tested. The in-chart valkey/valkey:8-alpine StatefulSet serves no
# TLS listener, so rendering TLS against it would break all seven consumers at
# once with a perfectly healthy-looking manifest and no failing preflight.
GUARD_OUT="$(helm template rel "$CHART" --set valkey.tls=true 2>&1)" && {
  fail "[5] valkey.tls=true with valkey.deploy left true rendered successfully; expected a render-time refusal"
}
for needle in "valkey.tls" "valkey.deploy"; do
  if ! printf '%s' "$GUARD_OUT" | grep -qF "$needle"; then
    echo "FAIL: [5] the guard's refusal did not name '$needle'" >&2
    echo "  actual output:" >&2
    printf '%s\n' "$GUARD_OUT" | sed 's/^/    /' >&2
    exit 1
  fi
done
echo "  [5] negative control: valkey.tls=true + valkey.deploy=true is refused at render time, naming both keys: OK"

echo
echo "PASS: valkey.tls reaches every consumer (api, worker, dispatcher, both"
echo "      upgrade-hook Jobs, both Langfuse Deployments), the default and"
echo "      BYO-plain renders stay cleartext, and the deploy+tls guard refuses"
echo "      the one configuration that would break all of it silently."
