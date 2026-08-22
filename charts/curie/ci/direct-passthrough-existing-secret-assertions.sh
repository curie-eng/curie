#!/usr/bin/env bash
#
# Render-assertion test for the direct-passthrough credential escape (#1759).
#
# templates/secrets.yaml carries nine credential keys read straight from
# .Values with no BYO escape, except githubAppPrivateKey, which already has
# one (ADR-0092: api.githubAppExistingSecret + api.githubAppExistingSecretKey,
# see github-app-credential-assertions.sh). This proves the SAME shape lands
# for the other eight keys, and that two parity-seam gates the new escape
# would otherwise trip are fixed alongside it:
#
#   1-2. For each of the eight keys, a default render (no *ExistingSecret set)
#        still resolves to the chart's own Secret and its published key name,
#        and setting <field>ExistingSecret makes EVERY consumer of that key
#        resolve to the named Secret instead -- not just one consumer, for the
#        two keys with more than one (agentCredentials: agent-sandbox.yaml +
#        worker.yaml; slackBotToken: dispatcher.yaml + api.yaml + worker.yaml).
#   3.   For two of those keys, the *ExistingSecret wins even when a stale
#        inline value is left in place (mirrors github-app-credential-
#        assertions.sh assertion (f)).
#   4.   curie.dispatcher.enabled (_helpers.tpl) must treat an *ExistingSecret
#        as "token present" too, so a dispatcher wired entirely via BYO
#        secrets still deploys -- with a negative control proving the gate
#        still refuses when only one token is present by any means.
#   5.   The agentCredentials gate in agent-sandbox.yaml AND worker.yaml
#        (currently `if .Values.agentSandbox.runner.credentials`) must become
#        "credentials OR credentialsExistingSecret", in both places -- with a
#        negative control proving CURIE_CREDENTIALS still renders in neither
#        when both are empty.
#   6.   NOT asserted here: a BYO Secret missing the referenced key must fail
#        the pod loudly (CreateContainerConfigError). `helm template` cannot
#        exercise real Kubernetes admission, so that property is a cluster-
#        tier property, verified separately against a real cluster (the same
#        caveat secrets.yaml already documents for the otlpAuthHeader key).
#
# This script is expected to FAIL against the chart as it stands before the
# *ExistingSecret escape lands (assertions 2, 3, 4a and 5a/5b), and to pass
# once it does. Accumulates every failure instead of stopping at the first, so
# a single run reports the complete list.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

FAILED=0
ASSERTION_COUNT=30

# Render the whole chart (never `-s`/--show-only for a template that can
# render to nothing: under this environment's helm, `-s` on a template whose
# entire body is gated off errors "could not find template" instead of
# returning empty output, which cannot be told apart from a real invocation
# mistake). --output-dir also sidesteps the stdout-pipe truncation this ci/
# directory's own dispatcher-api-wiring-assertions.sh warns about for large
# renders; a missing manifest file is the reliable "this template rendered
# nothing" signal used throughout.
render() {
  local name="$1"
  shift
  local out="$TMP/$name"
  rm -rf "$out"
  local rc=0
  helm template curie "$CHART" -n curie --output-dir "$out" "$@" \
    >/dev/null 2>"$out.stderr" || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL [$name] helm template failed unexpectedly (rc=$rc): $*" >&2
    cat "$out.stderr" >&2
    FAILED=1
    return 1
  fi
  return 0
}

# ------------------------------------------------------------------------
# Render 1: the "default" path for all eight keys -- no *ExistingSecret set,
# every underlying field given a throwaway value (including both Slack tokens,
# so the dispatcher deploys at all).
# ------------------------------------------------------------------------
render default \
  --set agentSandbox.runner.credentials=default-agentcreds \
  --set worker.adapterCredentials.myadapter=default-adaptercreds \
  --set api.githubToken=default-githubtoken \
  --set sealing.privateKey=default-sealingkey \
  --set sealing.previousPrivateKey=default-prevsealingkey \
  --set dispatcher.slack.appToken=default-apptoken \
  --set dispatcher.slack.botToken=default-bottoken \
  --set dispatcher.slack.signingSecret=default-signingsecret

# ------------------------------------------------------------------------
# Render 2: the BYO path for all eight keys. Each field ALSO carries a stale
# inline value, so this render doubles as assertions 2 (BYO resolves
# correctly, every consumer) and 3 (BYO wins over a stale inline value).
# ------------------------------------------------------------------------
render byo \
  --set agentSandbox.runner.credentials=STALE-agentcreds \
  --set agentSandbox.runner.credentialsExistingSecret=byo-agentcreds \
  --set worker.adapterCredentials.myadapter=STALE-adaptercreds \
  --set worker.adapterCredentialsExistingSecret=byo-adaptercreds \
  --set api.githubToken=STALE-githubtoken \
  --set api.githubTokenExistingSecret=byo-githubtoken \
  --set sealing.privateKey=STALE-sealingkey \
  --set sealing.privateKeyExistingSecret=byo-sealingkey \
  --set sealing.previousPrivateKey=STALE-prevsealingkey \
  --set sealing.previousPrivateKeyExistingSecret=byo-prevsealingkey \
  --set dispatcher.slack.appToken=STALE-apptoken \
  --set dispatcher.slack.appTokenExistingSecret=byo-apptoken \
  --set dispatcher.slack.botToken=STALE-bottoken \
  --set dispatcher.slack.botTokenExistingSecret=byo-bottoken \
  --set dispatcher.slack.signingSecret=STALE-signingsecret \
  --set dispatcher.slack.signingSecretExistingSecret=byo-signingsecret

# ------------------------------------------------------------------------
# Renders for assertion 5 (agentCredentials gate): credentialsExistingSecret
# alone (plain field empty), and a bare render with both empty.
# ------------------------------------------------------------------------
render agentcred-pos \
  --set agentSandbox.runner.credentialsExistingSecret=my-agentcreds-secret

render bare

# ------------------------------------------------------------------------
# Assertions 1, 2, 3, 5a-5d: structural checks on the rendered env/secretKeyRef
# shape, via PyYAML rather than grep/awk -- a line-oriented reader silently
# mis-reads a requoted value or a reordered key (see the same reasoning in
# dispatcher-api-wiring-assertions.sh).
# ------------------------------------------------------------------------
DEFAULT_DIR="$TMP/default/curie/templates" \
BYO_DIR="$TMP/byo/curie/templates" \
AGENTCRED_POS_DIR="$TMP/agentcred-pos/curie/templates" \
BARE_DIR="$TMP/bare/curie/templates" \
python3 <<'PY'
import os
import sys

import yaml

DEFAULT_DIR = os.environ["DEFAULT_DIR"]
BYO_DIR = os.environ["BYO_DIR"]
AGENTCRED_POS_DIR = os.environ["AGENTCRED_POS_DIR"]
BARE_DIR = os.environ["BARE_DIR"]
SECRET = "curie-secrets"

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

    Searches every container across every document in the file, regardless of
    the owning object's kind (Deployment for api/worker/dispatcher,
    SandboxTemplate for agent-sandbox), so the same helper covers all four
    consumer templates.
    """
    docs = load_docs(manifest)
    containers = []
    for d in docs:
        find_containers(d, containers)
    matched = [c for c in containers if isinstance(c, dict) and c.get("name") == container_name]
    entries = [e for c in matched for e in (c.get("env") or []) if e.get("name") == env_name]
    if len(entries) != 1:
        return None, len(entries)
    ref = (entries[0].get("valueFrom") or {}).get("secretKeyRef")
    return ref, 1


def check_ref(assertion_id, manifest, container_name, env_name, expected_secret, expected_key, ctx):
    ref, n = find_env(manifest, container_name, env_name)
    if n == 0:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} did not render on container "
                         f"{container_name!r} in {manifest}")
        return
    if n > 1:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} rendered {n} times on container "
                         f"{container_name!r}, expected exactly 1")
        return
    if not ref:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} has no valueFrom.secretKeyRef "
                         "(an inline value would put this credential in the rendered manifest)")
        return
    if ref.get("name") != expected_secret:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} secretKeyRef.name = "
                         f"{ref.get('name')!r}, expected {expected_secret!r}")
    if ref.get("key") != expected_key:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} secretKeyRef.key = "
                         f"{ref.get('key')!r}, expected {expected_key!r}")


def check_present(assertion_id, manifest, container_name, env_name, ctx):
    ref, n = find_env(manifest, container_name, env_name)
    if n == 0:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} did not render on container "
                         f"{container_name!r} in {manifest} (expected present)")


def check_absent(assertion_id, manifest, container_name, env_name, ctx):
    ref, n = find_env(manifest, container_name, env_name)
    if n != 0:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} rendered on container "
                         f"{container_name!r} in {manifest} (expected ABSENT)")


# ---- 1: default render (no *ExistingSecret set) resolves to the chart's own
#         Secret and its published key name, in every consumer. ----
d = DEFAULT_DIR
check_ref("1a", f"{d}/agent-sandbox.yaml", "runner", "CURIE_CREDENTIALS",
          SECRET, "agentCredentials", "agentCredentials via agent-sandbox.yaml")
check_ref("1b", f"{d}/worker.yaml", "worker", "CURIE_CREDENTIALS",
          SECRET, "agentCredentials", "agentCredentials via worker.yaml")
check_ref("1c", f"{d}/worker.yaml", "worker", "CURIE_ADAPTER_CREDENTIALS",
          SECRET, "adapterCredentials", "adapterCredentials via worker.yaml")
check_ref("1d", f"{d}/api.yaml", "api", "GITHUB_TOKEN",
          SECRET, "githubToken", "githubToken via api.yaml")
check_ref("1e", f"{d}/worker.yaml", "worker", "CURIE_SEALING_PRIVATE_KEY",
          SECRET, "sealingPrivateKey", "sealingPrivateKey via worker.yaml")
check_ref("1f", f"{d}/worker.yaml", "worker", "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
          SECRET, "sealingPreviousPrivateKey", "sealingPreviousPrivateKey via worker.yaml")
check_ref("1g", f"{d}/dispatcher.yaml", "dispatcher", "SLACK_APP_TOKEN",
          SECRET, "slackAppToken", "slackAppToken via dispatcher.yaml")
check_ref("1h", f"{d}/dispatcher.yaml", "dispatcher", "SLACK_BOT_TOKEN",
          SECRET, "slackBotToken", "slackBotToken via dispatcher.yaml")
check_ref("1i", f"{d}/api.yaml", "api", "SLACK_BOT_TOKEN",
          SECRET, "slackBotToken", "slackBotToken via api.yaml")
check_ref("1j", f"{d}/worker.yaml", "worker", "SLACK_BOT_TOKEN",
          SECRET, "slackBotToken", "slackBotToken via worker.yaml")
check_ref("1k", f"{d}/dispatcher.yaml", "dispatcher", "SLACK_SIGNING_SECRET",
          SECRET, "slackSigningSecret", "slackSigningSecret via dispatcher.yaml")

# ---- 2: *ExistingSecret makes every consumer resolve to the named Secret
#         instead, winning over the stale inline value the same render also
#         carries (assertion 3 is folded in below under its own IDs). ----
b = BYO_DIR
check_ref("2a", f"{b}/agent-sandbox.yaml", "runner", "CURIE_CREDENTIALS",
          "byo-agentcreds", "agentCredentials", "agentCredentials via agent-sandbox.yaml")
check_ref("2b", f"{b}/worker.yaml", "worker", "CURIE_CREDENTIALS",
          "byo-agentcreds", "agentCredentials", "agentCredentials via worker.yaml")
check_ref("2c", f"{b}/worker.yaml", "worker", "CURIE_ADAPTER_CREDENTIALS",
          "byo-adaptercreds", "adapterCredentials", "adapterCredentials via worker.yaml")
check_ref("2d", f"{b}/api.yaml", "api", "GITHUB_TOKEN",
          "byo-githubtoken", "githubToken", "githubToken via api.yaml")
check_ref("2e", f"{b}/worker.yaml", "worker", "CURIE_SEALING_PRIVATE_KEY",
          "byo-sealingkey", "sealingPrivateKey", "sealingPrivateKey via worker.yaml")
check_ref("2f", f"{b}/worker.yaml", "worker", "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
          "byo-prevsealingkey", "sealingPreviousPrivateKey", "sealingPreviousPrivateKey via worker.yaml")
check_ref("2g", f"{b}/dispatcher.yaml", "dispatcher", "SLACK_APP_TOKEN",
          "byo-apptoken", "slackAppToken", "slackAppToken via dispatcher.yaml")
check_ref("2h", f"{b}/dispatcher.yaml", "dispatcher", "SLACK_BOT_TOKEN",
          "byo-bottoken", "slackBotToken", "slackBotToken via dispatcher.yaml")
check_ref("2i", f"{b}/api.yaml", "api", "SLACK_BOT_TOKEN",
          "byo-bottoken", "slackBotToken", "slackBotToken via api.yaml")
check_ref("2j", f"{b}/worker.yaml", "worker", "SLACK_BOT_TOKEN",
          "byo-bottoken", "slackBotToken", "slackBotToken via worker.yaml")
check_ref("2k", f"{b}/dispatcher.yaml", "dispatcher", "SLACK_SIGNING_SECRET",
          "byo-signingsecret", "slackSigningSecret", "slackSigningSecret via dispatcher.yaml")

# ---- 3: BYO wins over a stale inline value, named explicitly for the two
#         multi-consumer keys (mirrors github-app-credential-assertions.sh
#         assertion (f)). Reuses the byo render above -- every field there
#         already carries both a stale inline value and an *ExistingSecret. ----
check_ref("3a", f"{b}/worker.yaml", "worker", "CURIE_CREDENTIALS",
          "byo-agentcreds", "agentCredentials",
          "agentCredentialsExistingSecret must win over inline STALE-agentcreds (worker.yaml)")
check_ref("3b", f"{b}/dispatcher.yaml", "dispatcher", "SLACK_BOT_TOKEN",
          "byo-bottoken", "slackBotToken",
          "botTokenExistingSecret must win over inline STALE-bottoken (dispatcher.yaml)")

# ---- 5: the agentCredentials gate (agent-sandbox.yaml AND worker.yaml) must
#         also open on credentialsExistingSecret alone. ----
p = AGENTCRED_POS_DIR
check_present("5a", f"{p}/agent-sandbox.yaml", "runner", "CURIE_CREDENTIALS",
              "credentialsExistingSecret alone via agent-sandbox.yaml")
check_present("5b", f"{p}/worker.yaml", "worker", "CURIE_CREDENTIALS",
              "credentialsExistingSecret alone via worker.yaml")

# Negative control: both credentials and credentialsExistingSecret empty must
# still render CURIE_CREDENTIALS in NEITHER consumer (unchanged behaviour).
n = BARE_DIR
check_absent("5c", f"{n}/agent-sandbox.yaml", "runner", "CURIE_CREDENTIALS",
             "negative control: both empty, via agent-sandbox.yaml")
check_absent("5d", f"{n}/worker.yaml", "worker", "CURIE_CREDENTIALS",
             "negative control: both empty, via worker.yaml")

if failures:
    for msg in failures:
        print(f"FAIL {msg}", file=sys.stderr)
    print(f"{len(failures)} of 28 python-side assertions failed "
          "(assertions 1-3 and 5a-5d; 4a-4b run separately in bash)", file=sys.stderr)
    sys.exit(1)
print("OK: all 28 python-side assertions passed (1a-1k, 2a-2k, 3a-3b, 5a-5d)")
PY
[ $? -eq 0 ] || FAILED=1

# ------------------------------------------------------------------------
# Assertion 4: curie.dispatcher.enabled (_helpers.tpl) must treat an
# *ExistingSecret as "token present" too, so a dispatcher wired entirely via
# BYO secrets still deploys. A missing dispatcher.yaml manifest file (rather
# than an empty document) is --output-dir's signal that the whole template
# rendered nothing, matching dispatcher-api-wiring-assertions.sh's own check.
# ------------------------------------------------------------------------
render dispatcher-gate-pos \
  --set dispatcher.slack.appTokenExistingSecret=byo-apptoken \
  --set dispatcher.slack.botTokenExistingSecret=byo-bottoken
if [ -s "$TMP/dispatcher-gate-pos/curie/templates/dispatcher.yaml" ]; then
  echo "OK [4a] dispatcher deploys with both slack *ExistingSecret fields set and both plain tokens empty"
else
  echo "FAIL [4a] dispatcher.yaml did not render with both dispatcher.slack.appTokenExistingSecret and" \
       "dispatcher.slack.botTokenExistingSecret set (plain appToken/botToken left empty)." \
       "curie.dispatcher.enabled must treat an *ExistingSecret as a present token, or a dispatcher" \
       "configured entirely via BYO secrets never deploys." >&2
  FAILED=1
fi

# Negative control: only one of the two *ExistingSecret fields set (the other,
# and its plain counterpart, both empty) -- the gate must still refuse.
render dispatcher-gate-neg \
  --set dispatcher.slack.appTokenExistingSecret=byo-apptoken
if [ -s "$TMP/dispatcher-gate-neg/curie/templates/dispatcher.yaml" ]; then
  echo "FAIL [4b] dispatcher.yaml rendered with only appTokenExistingSecret set (botToken and" \
       "botTokenExistingSecret both empty). curie.dispatcher.enabled must still require BOTH tokens" \
       "present by some means -- this negative control proves the gate did not just become 'true'." >&2
  FAILED=1
else
  echo "OK [4b] dispatcher stays undeployed when only one Slack token is present by any means"
fi

# ---- 6 (note, not an assertion): a BYO Secret missing the referenced key
# must fail the pod loudly with CreateContainerConfigError. `helm template`
# never talks to the API server, so it cannot exercise that admission-time
# behaviour -- it is verified separately against a real cluster, the same
# caveat secrets.yaml already documents for the otlpAuthHeader key (#1563).

echo
if [ "$FAILED" -ne 0 ]; then
  echo "direct-passthrough-existing-secret assertions FAILED ($ASSERTION_COUNT assertions attempted)" >&2
  exit 1
fi
echo "direct-passthrough-existing-secret assertions passed ($ASSERTION_COUNT assertions)"
