#!/usr/bin/env bash
#
# Render-assertion test for `<store>.existingSecret` reaching every Langfuse
# consumer (#2052 valkey, #2327 postgres + langfuse).
#
# The filename is historical: this started as the valkey/REDIS_AUTH gate and
# was widened in place rather than forked, because every case here is the same
# split -- one consumer group honouring `<store>.existingSecret` while
# `curie.langfuse.env` hardcoded the chart Secret -- and the helm-ci wiring
# already points at this path.
#
# The chart's stated invariant (charts/curie/CLAUDE.md, "Every backing store
# follows the same toggle + BYO idiom") is that flipping `<store>.deploy` to
# false repoints EVERY consumer at the BYO `host`/`port`/`auth`/`existingSecret`
# fields on the same block. Three stores had two consumer groups each and only
# one group honoured it: `curie.env.valkey` (api + worker) read
# `valkey.existingSecret | default <chart secret>` while `curie.langfuse.env`
# hardcoded the chart Secret for REDIS_AUTH (#2052), and `curie.env.postgres`
# read `postgres.existingSecret | default <chart secret>` while the same
# `curie.langfuse.env` block hardcoded it for POSTGRES_PASSWORD, SALT and
# ENCRYPTION_KEY (#2327).
#
# The consequence is silent and asymmetric, which is why it is worth a gate. On
# a BYO valkey install (`deploy=false` + `host` + `existingSecret`) the chart
# Secret still renders and still holds the chart-generated valkeyPassword, so
# nothing errors at template or install time. The api and worker authenticate
# against the real instance and stay healthy; only Langfuse presents the wrong
# password, so trace ingestion dies while every other component reports green.
# There is no failing manifest, no failing preflight, and no unhealthy pod to
# point at the cause -- exactly the shape of the rustfs endpoint bug already
# tombstoned in the same `curie.langfuse.env` block.
#
# Asserts:
#
#   1. DEFAULT render: REDIS_AUTH on BOTH langfuse containers resolves to the
#      chart's own Secret, key `valkeyPassword`. The no-regression case -- the
#      fix must not repoint an install that never set existingSecret.
#   2. `valkey.existingSecret=acme-valkey`: REDIS_AUTH on BOTH langfuse
#      containers resolves to `acme-valkey`. Both, because web and worker are
#      separate Deployments that each include the shared env helper; a fix
#      applied to one include site and not the other renders half a release.
#   3. Parity in the SAME render: VALKEY_PASSWORD on the api and worker
#      containers also resolves to `acme-valkey`. This is the pair that was
#      split, so both sides are asserted together -- asserting only the langfuse
#      half would let a future edit "fix" the split by breaking the app services
#      instead.
#   4. The realistic full BYO shape (`deploy=false` + `host` + `existingSecret`):
#      no valkey StatefulSet renders AND langfuse still resolves `acme-valkey`.
#      This is the exact supported configuration the bug broke, and it is not
#      the same render as (2) -- (2) keeps the in-chart valkey, so it alone
#      cannot prove the deploy=false path.
#   5. NEGATIVE CONTROL: under the BYO render, the chart Secret name must NOT
#      appear as the REDIS_AUTH secretKeyRef on either langfuse container.
#      Without this, assertions 2 and 4 would still pass against a template that
#      emitted REDIS_AUTH twice or fell back to the chart Secret, and the gate
#      would be vacuous.
#
#   6. `postgres.existingSecret=byo-postgres`: POSTGRES_PASSWORD resolves to
#      `byo-postgres` on BOTH langfuse containers AND, in the same render, on
#      the api and worker containers -- the split pair asserted together, so a
#      future edit cannot "fix" the split by breaking the app services instead.
#      With a negative control (the chart Secret must not back POSTGRES_PASSWORD
#      on either langfuse container) and a default-render no-regression case.
#      Before #2327 the api and worker authenticated against the real BYO
#      instance while both Langfuse Deployments presented the chart-generated
#      password and crash-looped at Prisma auth, every other component green.
#   7. `langfuse.existingSecret=byo-langfuse`: all five Langfuse-owned app keys
#      resolve to `byo-langfuse` -- SALT (`langfuseSalt`) and ENCRYPTION_KEY
#      (`langfuseEncryptionKey`) on BOTH containers via the shared env helper,
#      plus NEXTAUTH_SECRET, LANGFUSE_INIT_PROJECT_SECRET_KEY and
#      LANGFUSE_INIT_USER_PASSWORD written inline on the web Deployment only.
#      Two of the five are what #2327 fixed; the other three were already
#      correct and are asserted so a regression on either half fails the gate.
#      With the same negative control and no-regression pair for SALT and
#      ENCRYPTION_KEY. ENCRYPTION_KEY is the sharpest of the set: the operator's
#      `langfuseEncryptionKey` was silently unused, so a later regeneration of
#      the chart Secret left previously written encrypted columns undecryptable.
#
# Every render goes through `--output-dir`, never a stdout pipe: piping
# `helm template` in this environment silently truncates a large render at
# exit 0 with empty stderr, which reads as a passing assertion against manifests
# that were never examined. Structural checks go through PyYAML rather than
# grep, for the reason dispatcher-api-wiring-assertions.sh gives -- a
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

echo "=== Rendering Langfuse (defaults) ==="
render default
DEFAULT_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering Langfuse (valkey.existingSecret=acme-valkey) ==="
render byo --set valkey.existingSecret=acme-valkey
BYO_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering Langfuse (valkey.deploy=false + host + existingSecret) ==="
render byo-full \
  --set valkey.deploy=false \
  --set valkey.host=redis.acme.internal \
  --set valkey.existingSecret=acme-valkey
BYO_FULL_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering Langfuse (postgres.existingSecret=byo-postgres) ==="
render byo-postgres --set postgres.existingSecret=byo-postgres
BYO_PG_DIR="$RENDER_DIR/curie/templates"

echo "=== Rendering Langfuse (langfuse.existingSecret=byo-langfuse) ==="
render byo-langfuse --set langfuse.existingSecret=byo-langfuse
BYO_LF_DIR="$RENDER_DIR/curie/templates"

# ---------------------------------------------------------------------- 4a
# A missing manifest file is --output-dir's signal that the whole template
# rendered nothing (the same check direct-passthrough-existing-secret-
# assertions.sh uses); an empty document would not be distinguishable.
if [[ -s "$BYO_FULL_DIR/valkey.yaml" ]]; then
  fail "[4a] valkey.deploy=false still rendered templates/valkey.yaml"
fi
echo "  [4a] valkey.deploy=false renders no in-chart valkey manifest: OK"

# -------------------------------------------------- 1, 2, 3, 4b, 5, 6, 7
DEFAULT_DIR="$DEFAULT_DIR" BYO_DIR="$BYO_DIR" BYO_FULL_DIR="$BYO_FULL_DIR" \
BYO_PG_DIR="$BYO_PG_DIR" BYO_LF_DIR="$BYO_LF_DIR" \
python3 <<'PY'
import os
import sys

import yaml

DEFAULT_DIR = os.environ["DEFAULT_DIR"]
BYO_DIR = os.environ["BYO_DIR"]
BYO_FULL_DIR = os.environ["BYO_FULL_DIR"]
BYO_PG_DIR = os.environ["BYO_PG_DIR"]
BYO_LF_DIR = os.environ["BYO_LF_DIR"]

# `helm template rel <chart>` -> fullname `rel-curie`, so the chart's own
# Secret is `rel-curie-secrets`. Hardcoded rather than derived: the point of
# the negative control is that this exact name must NOT appear.
CHART_SECRET_NAME = "rel-curie-secrets"
BYO_SECRET_NAME = "acme-valkey"
BYO_PG_SECRET_NAME = "byo-postgres"
BYO_LF_SECRET_NAME = "byo-langfuse"

failures = []


def load_docs(path):
    if not os.path.isfile(path):
        return []
    with open(path) as f:
        return [d for d in yaml.safe_load_all(f) if d]


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


def find_env(manifest, container_name, env_name):
    """Return (secretKeyRef-or-None, match-count) for one env entry.

    Searches every container across every document in the file, so one helper
    covers langfuse.yaml's two Deployments (langfuse-web / langfuse-worker) as
    well as api.yaml and worker.yaml.
    """
    containers = []
    for d in load_docs(manifest):
        find_containers(d, containers)
    matched = [c for c in containers
               if isinstance(c, dict) and c.get("name") == container_name]
    entries = [e for c in matched for e in (c.get("env") or [])
               if e.get("name") == env_name]
    if len(entries) != 1:
        return None, len(entries)
    ref = (entries[0].get("valueFrom") or {}).get("secretKeyRef")
    return ref, 1


def check_ref(aid, manifest, container, env_name, expected_secret, expected_key, ctx):
    ref, n = find_env(manifest, container, env_name)
    if n == 0:
        failures.append(f"[{aid}] {ctx}: {env_name} did not render on container "
                        f"{container!r} in {manifest}")
        return
    if n > 1:
        failures.append(f"[{aid}] {ctx}: {env_name} rendered {n} times on container "
                        f"{container!r}, expected exactly 1")
        return
    if not ref:
        failures.append(f"[{aid}] {ctx}: {env_name} has no valueFrom.secretKeyRef "
                        "(an inline value would put this credential in the manifest)")
        return
    if ref.get("name") != expected_secret:
        failures.append(f"[{aid}] {ctx}: {env_name} secretKeyRef.name = "
                        f"{ref.get('name')!r}, expected {expected_secret!r}")
    if ref.get("key") != expected_key:
        failures.append(f"[{aid}] {ctx}: {env_name} secretKeyRef.key = "
                        f"{ref.get('key')!r}, expected {expected_key!r}")


# Paired with a literal id suffix so every per-container assertion id follows the
# same a/b convention as the single-container ones (3a/3b) in this file.
LANGFUSE_CONTAINERS = [("a", "langfuse-web"), ("b", "langfuse-worker")]

# ---- 1: default render still resolves to the chart's own Secret. ----
for suffix, c in LANGFUSE_CONTAINERS:
    check_ref(f"1{suffix}", f"{DEFAULT_DIR}/langfuse.yaml", c,
              "REDIS_AUTH", CHART_SECRET_NAME, "valkeyPassword",
              f"default render, {c}")

# ---- 2: valkey.existingSecret reaches BOTH langfuse containers. ----
for suffix, c in LANGFUSE_CONTAINERS:
    check_ref(f"2{suffix}", f"{BYO_DIR}/langfuse.yaml", c,
              "REDIS_AUTH", BYO_SECRET_NAME, "valkeyPassword",
              f"valkey.existingSecret set, {c}")

# ---- 3: parity with the app services in the SAME render. This is the pair
#         that was split, so both halves are asserted together. ----
check_ref("3a", f"{BYO_DIR}/api.yaml", "api", "VALKEY_PASSWORD",
          BYO_SECRET_NAME, "valkeyPassword", "valkey.existingSecret set, api")
check_ref("3b", f"{BYO_DIR}/worker.yaml", "worker", "VALKEY_PASSWORD",
          BYO_SECRET_NAME, "valkeyPassword", "valkey.existingSecret set, worker")

# ---- 4b: the realistic full BYO shape (deploy=false + host + existingSecret)
#          -- the exact supported configuration the bug broke. ----
for suffix, c in LANGFUSE_CONTAINERS:
    check_ref(f"4b{suffix}", f"{BYO_FULL_DIR}/langfuse.yaml", c,
              "REDIS_AUTH", BYO_SECRET_NAME, "valkeyPassword",
              f"valkey.deploy=false + host + existingSecret, {c}")

# ---- 5: NEGATIVE CONTROL. Proves the assertion catches the #2052 bug rather
#         than passing vacuously: under both BYO renders the chart Secret must
#         not back REDIS_AUTH on either langfuse container. ----
for label, d in (("byo", BYO_DIR), ("byo-full", BYO_FULL_DIR)):
    for _suffix, c in LANGFUSE_CONTAINERS:
        ref, n = find_env(f"{d}/langfuse.yaml", c, "REDIS_AUTH")
        if n == 1 and ref and ref.get("name") == CHART_SECRET_NAME:
            failures.append(
                f"[5] negative control ({label}, {c}): REDIS_AUTH still resolves to the "
                f"chart-managed Secret {CHART_SECRET_NAME!r} with valkey.existingSecret="
                f"{BYO_SECRET_NAME!r} set. This is issue #2052: the app services read the BYO "
                "Secret while Langfuse alone reads the chart one, so Langfuse presents the "
                "wrong password and trace ingestion dies silently with the rest of the "
                "release healthy.")

# ---- 6: postgres.existingSecret reaches Langfuse as well as the app services
#         (#2327). POSTGRES_PASSWORD/postgresPassword is the same env name and
#         key on all four containers -- `curie.env.postgres` (api + worker) and
#         `curie.langfuse.env` (web + worker) -- so the split is visible only by
#         asserting both groups in one render. ----
for suffix, c in LANGFUSE_CONTAINERS:
    check_ref(f"6{suffix}", f"{BYO_PG_DIR}/langfuse.yaml", c,
              "POSTGRES_PASSWORD", BYO_PG_SECRET_NAME, "postgresPassword",
              f"postgres.existingSecret set, {c}")

# ---- 6c/6d: the app-service half of that same render. Asserted alongside 6a/6b
#             so a future edit cannot "fix" the split by breaking these instead.
check_ref("6c", f"{BYO_PG_DIR}/api.yaml", "api", "POSTGRES_PASSWORD",
          BYO_PG_SECRET_NAME, "postgresPassword", "postgres.existingSecret set, api")
check_ref("6d", f"{BYO_PG_DIR}/worker.yaml", "worker", "POSTGRES_PASSWORD",
          BYO_PG_SECRET_NAME, "postgresPassword", "postgres.existingSecret set, worker")

# ---- 6e: NEGATIVE CONTROL for 6. Without it 6a/6b would still pass against a
#          template that emitted POSTGRES_PASSWORD twice or fell back to the
#          chart Secret, and the gate would be vacuous. ----
for _suffix, c in LANGFUSE_CONTAINERS:
    ref, n = find_env(f"{BYO_PG_DIR}/langfuse.yaml", c, "POSTGRES_PASSWORD")
    if n == 1 and ref and ref.get("name") == CHART_SECRET_NAME:
        failures.append(
            f"[6e] negative control ({c}): POSTGRES_PASSWORD still resolves to the "
            f"chart-managed Secret {CHART_SECRET_NAME!r} with postgres.existingSecret="
            f"{BYO_PG_SECRET_NAME!r} set. This is issue #2327: the api and worker "
            "authenticate against the real BYO instance while both Langfuse Deployments "
            "present the chart-generated password and crash-loop at Prisma auth, with "
            "every other component green.")

# ---- 6f: no-regression. An install that never set postgres.existingSecret must
#          still resolve to the chart's own Secret. ----
for suffix, c in LANGFUSE_CONTAINERS:
    check_ref(f"6f{suffix}", f"{DEFAULT_DIR}/langfuse.yaml", c,
              "POSTGRES_PASSWORD", CHART_SECRET_NAME, "postgresPassword",
              f"default render, {c}")

# ---- 7: langfuse.existingSecret backs all five Langfuse-owned app keys.
#         SALT and ENCRYPTION_KEY come from the shared `curie.langfuse.env`
#         include, so they must land on BOTH Deployments; the other three are
#         written inline in langfuse.yaml's web Deployment only. Verified
#         against templates/langfuse.yaml, not assumed. ----
LANGFUSE_SHARED_KEYS = [
    ({"langfuse-web": "7a", "langfuse-worker": "7b"}, "SALT", "langfuseSalt"),
    ({"langfuse-web": "7c", "langfuse-worker": "7d"}, "ENCRYPTION_KEY", "langfuseEncryptionKey"),
]
for aid_by_container, env_name, secret_key in LANGFUSE_SHARED_KEYS:
    for _suffix, c in LANGFUSE_CONTAINERS:
        check_ref(aid_by_container[c], f"{BYO_LF_DIR}/langfuse.yaml", c,
                  env_name, BYO_LF_SECRET_NAME, secret_key,
                  f"langfuse.existingSecret set, {c}")

# ---- 7e/7f/7g: the three web-only keys. These already honoured
#                langfuse.existingSecret before #2327 and are asserted so a
#                regression on either half of the block fails the gate. ----
LANGFUSE_WEB_ONLY_KEYS = [
    ("7e", "NEXTAUTH_SECRET", "langfuseNextauthSecret"),
    ("7f", "LANGFUSE_INIT_PROJECT_SECRET_KEY", "langfuseInitProjectSecretKey"),
    ("7g", "LANGFUSE_INIT_USER_PASSWORD", "langfuseInitUserPassword"),
]
for aid, env_name, secret_key in LANGFUSE_WEB_ONLY_KEYS:
    check_ref(aid, f"{BYO_LF_DIR}/langfuse.yaml", "langfuse-web",
              env_name, BYO_LF_SECRET_NAME, secret_key,
              "langfuse.existingSecret set, langfuse-web")

# ---- 7h: NEGATIVE CONTROL for the two keys #2327 fixed. ----
for env_name in ("SALT", "ENCRYPTION_KEY"):
    for _suffix, c in LANGFUSE_CONTAINERS:
        ref, n = find_env(f"{BYO_LF_DIR}/langfuse.yaml", c, env_name)
        if n == 1 and ref and ref.get("name") == CHART_SECRET_NAME:
            failures.append(
                f"[7h] negative control ({c}): {env_name} still resolves to the "
                f"chart-managed Secret {CHART_SECRET_NAME!r} with langfuse.existingSecret="
                f"{BYO_LF_SECRET_NAME!r} set. This is issue #2327: the operator's key is "
                "silently unused, and for ENCRYPTION_KEY a later regeneration of the chart "
                "Secret leaves previously written encrypted columns undecryptable.")

# ---- 7i/7j: no-regression for the same two keys on a default install. ----
LANGFUSE_SHARED_DEFAULTS = [
    ("7i", "SALT", "langfuseSalt"),
    ("7j", "ENCRYPTION_KEY", "langfuseEncryptionKey"),
]
for prefix, env_name, secret_key in LANGFUSE_SHARED_DEFAULTS:
    for suffix, c in LANGFUSE_CONTAINERS:
        check_ref(f"{prefix}{suffix}", f"{DEFAULT_DIR}/langfuse.yaml", c,
                  env_name, CHART_SECRET_NAME, secret_key,
                  f"default render, {c}")

if failures:
    for msg in failures:
        print(f"FAIL {msg}", file=sys.stderr)
    # 35 = 25 check_ref call sites + 10 negative-control loop iterations
    # ([5] 2 dirs x 2 containers = 4, [6e] 2 containers = 2, [7h] 2 env names x
    # 2 containers = 4). Bash-side [4a] is outside this count, hence "python-
    # side"; a single check_ref site can emit 2 failures (name and key both
    # wrong), so len(failures) is not capped at 35.
    print(f"{len(failures)} of 35 python-side assertions failed", file=sys.stderr)
    sys.exit(1)

print("  [1] default render: REDIS_AUTH -> chart Secret on web + worker: OK")
print("  [2] valkey.existingSecret: REDIS_AUTH -> BYO Secret on web + worker: OK")
print("  [3] same render: VALKEY_PASSWORD -> BYO Secret on api + worker: OK")
print("  [4b] deploy=false + host + existingSecret: langfuse -> BYO Secret: OK")
print("  [5] negative control: chart Secret never backs REDIS_AUTH under BYO: OK")
print("  [6] postgres.existingSecret: POSTGRES_PASSWORD -> BYO Secret on langfuse "
      "web + worker AND api + worker, with negative control and default no-regression: OK")
print("  [7] langfuse.existingSecret: SALT + ENCRYPTION_KEY (web + worker) and "
      "NEXTAUTH_SECRET + the two LANGFUSE_INIT_* keys (web) -> BYO Secret, with "
      "negative control and default no-regression: OK")
PY

echo
echo "PASS: valkey, postgres and langfuse existingSecret each reach every consumer,"
echo "      Langfuse included (#2052, #2327)."
