#!/usr/bin/env bash
#
# Render-assertion test for the direct-passthrough credential escape (#1759).
#
# templates/secrets.yaml carries twelve credential keys read straight from
# .Values with no BYO escape, except githubAppPrivateKey, which already has
# one (ADR-0092: api.githubAppExistingSecret + api.githubAppExistingSecretKey,
# see github-app-credential-assertions.sh). This proves the SAME shape lands
# for the other eleven keys, and that two parity-seam gates the new escape
# would otherwise trip are fixed alongside it:
#
#   1-2. For each of the eleven keys, a default render (no *ExistingSecret set)
#        still resolves to the chart's own Secret and its published key name,
#        and setting <field>ExistingSecret makes EVERY consumer of that key
#        resolve to the named Secret instead -- not just one consumer, for the
#        two keys with more than one (agentCredentials: agent-sandbox.yaml +
#        worker.yaml; slackBotToken: dispatcher.yaml + api.yaml + worker.yaml).
#        Group 2's render also carries a stale inline value on every field, so
#        it already proves "BYO wins over stale" for all eleven, not just the
#        multi-consumer ones (mirrors github-app-credential-assertions.sh
#        assertion (f)).
#   4.   curie.dispatcher.enabled (_helpers.tpl) must treat an *ExistingSecret
#        as "token present" too, so a dispatcher wired entirely via BYO
#        secrets still deploys -- with a negative control proving the gate
#        still refuses when only one token is present by any means.
#   5.   The agentCredentials gate in agent-sandbox.yaml AND worker.yaml
#        (currently `if .Values.agentSandbox.runner.credentials`) must become
#        "credentials OR credentialsExistingSecret", in both places -- with a
#        negative control proving CURIE_CREDENTIALS still renders in neither
#        when both are empty.
#   7.   worker.yaml's checksum/adapter-credentials annotation must change
#        when adapterCredentialsExistingSecret changes (inline value held
#        constant), so switching which BYO Secret backs the credential still
#        rolls the worker Deployment.
#   8.   NOT asserted here: a BYO Secret missing the referenced key must fail
#        the pod loudly (CreateContainerConfigError). `helm template` cannot
#        exercise real Kubernetes admission, so that property is a cluster-
#        tier property, verified separately against a real cluster (the same
#        caveat secrets.yaml already documents for the otlpAuthHeader key).
#
#   9.   curie.adapterCredentials (_helpers.tpl) derives the worker's half of
#        the mail adapter's egress pair from mailAdapter.egressSecret, so both
#        ends of that shared credential always rotate together. That derivation
#        has a BYO branch: once mailAdapter.egressSecretExistingSecret is set,
#        the plain Helm value is unused and the (then required) external worker
#        map is the independent source of truth. So (9a) the default render
#        derives the adapter's entry from mailAdapter.egressSecret and the chart
#        Secret carries all three mail keys; (9b) the BYO render derives NO
#        entry for the adapter and the chart Secret carries NONE of the three
#        mail keys -- emitting either would put the very credential the escape
#        exists to keep out of helm values back into the chart Secret; (9c) the
#        equality check that normally refuses a worker entry disagreeing with
#        mailAdapter.egressSecret must NOT fire on that branch, where the plain
#        value means nothing; and (9d) a negative control with only the
#        *ExistingSecret removed must still fail the render WITH the equality
#        check's own diagnostic -- naming both configuration keys and neither
#        credential value -- proving 9c passes because of the BYO branch, and
#        not because the check was deleted or some unrelated gate refused first.
#
# Assertions 2, 4a, 5a/5b and 7 were written to FAIL against the chart as it
# stood before the *ExistingSecret escape landed for the original eight keys
# (#1759), and to pass once it did. The three mail keys and the
# adapterCredentials BYO branch (1l-1n, 2l-2n and 9) landed their escape later
# and were added here afterwards, so those are regression pins on shipped
# behaviour rather than a red-first gate. Accumulates every failure instead of
# stopping at the first, so a single run reports the complete list.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

FAILED=0
ASSERTION_COUNT=39

# mailAdapter.deploy=true fails closed without at least one AgentMail HTTPS
# CIDR (see the chart-level invariant in charts/curie/CLAUDE.md), so this flag
# rides along with every render below that turns the adapter on -- it is not
# an incidental value.
MAIL_HTTPS_CIDR="203.0.113.0/24"

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
# Render 1: the "default" path for all eleven keys -- no *ExistingSecret set,
# every underlying field given a throwaway value (including both Slack tokens,
# so the dispatcher deploys at all).
#
# The two mail flags that are not credentials are load-bearing, not noise:
# mail-adapter.yaml only renders when mailAdapter.deploy is true, and the
# mail-adapter egress rail fails CLOSED unless deploy=true is paired with at
# least one mailAdapter.agentmail.httpsCidrs entry. Drop either and this render
# stops producing the manifest assertions 1l-1n read.
# ------------------------------------------------------------------------
render default \
  --set agentSandbox.runner.credentials=default-agentcreds \
  --set worker.adapterCredentials.myadapter=default-adaptercreds \
  --set api.githubToken=default-githubtoken \
  --set sealing.privateKey=default-sealingkey \
  --set sealing.previousPrivateKey=default-prevsealingkey \
  --set dispatcher.slack.appToken=default-apptoken \
  --set dispatcher.slack.botToken=default-bottoken \
  --set dispatcher.slack.signingSecret=default-signingsecret \
  --set mailAdapter.deploy=true \
  --set-string "mailAdapter.agentmail.httpsCidrs[0]=${MAIL_HTTPS_CIDR}" \
  --set mailAdapter.channelToken=default-channeltoken \
  --set mailAdapter.egressSecret=default-egresssecret \
  --set mailAdapter.agentmail.apiKey=default-apikey

# ------------------------------------------------------------------------
# Render 2: the BYO path for all eleven keys. Each field ALSO carries a stale
# inline value, so this render doubles as assertions 2 (BYO resolves
# correctly, every consumer) and 3 (BYO wins over a stale inline value); the
# mail fields are set the same way so that "stale inline value on every field"
# property stays uniform across all eleven keys.
#
# Two couplings, both deliberate: the same fail-closed mail rail as render 1
# (deploy=true needs an mailAdapter.agentmail.httpsCidrs entry), and
# mailAdapter.egressSecretExistingSecret requires
# worker.adapterCredentialsExistingSecret -- which this render already sets for
# assertion 2c, so neither of those two --set flags is removable even though
# they read as belonging to unrelated keys.
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
  --set dispatcher.slack.signingSecretExistingSecret=byo-signingsecret \
  --set mailAdapter.deploy=true \
  --set-string "mailAdapter.agentmail.httpsCidrs[0]=${MAIL_HTTPS_CIDR}" \
  --set mailAdapter.channelToken=STALE-channeltoken \
  --set mailAdapter.channelTokenExistingSecret=byo-channeltoken \
  --set mailAdapter.egressSecret=STALE-egresssecret \
  --set mailAdapter.egressSecretExistingSecret=byo-egresssecret \
  --set mailAdapter.agentmail.apiKey=STALE-apikey \
  --set mailAdapter.agentmail.apiKeyExistingSecret=byo-apikey

# ------------------------------------------------------------------------
# Renders for assertion 5 (agentCredentials gate): credentialsExistingSecret
# alone (plain field empty), and a bare render with both empty.
# ------------------------------------------------------------------------
render agentcred-pos \
  --set agentSandbox.runner.credentialsExistingSecret=my-agentcreds-secret

render bare

# ------------------------------------------------------------------------
# Assertions 1, 2, 3, 5a-5d and 9a-9b: structural checks on the rendered
# env/secretKeyRef shape and on the chart Secret's own stringData, via PyYAML
# rather than grep/awk -- a line-oriented reader silently mis-reads a requoted
# value or a reordered key (see the same reasoning in
# dispatcher-api-wiring-assertions.sh).
# ------------------------------------------------------------------------
DEFAULT_DIR="$TMP/default/curie/templates" \
BYO_DIR="$TMP/byo/curie/templates" \
AGENTCRED_POS_DIR="$TMP/agentcred-pos/curie/templates" \
BARE_DIR="$TMP/bare/curie/templates" \
python3 <<'PY'
import json
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


def find_env_entry(manifest, container_name, env_name):
    """Return (whole-env-entry-or-None, match-count) for one env entry.

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
    return entries[0], 1


def find_env(manifest, container_name, env_name):
    """Return (secretKeyRef-or-None, match-count) for one env entry."""
    entry, n = find_env_entry(manifest, container_name, env_name)
    if entry is None:
        return None, n
    return (entry.get("valueFrom") or {}).get("secretKeyRef"), n


def check_ref(assertion_id, manifest, container_name, env_name, expected_secret, expected_key, ctx):
    entry, n = find_env_entry(manifest, container_name, env_name)
    if n == 0:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} did not render on container "
                         f"{container_name!r} in {manifest}")
        return
    if n > 1:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} rendered {n} times on container "
                         f"{container_name!r}, expected exactly 1")
        return
    # An inline `value:` sitting ALONGSIDE the secretKeyRef is the half-landed
    # escape (#2328): the ref looks right while the credential is still written
    # into the rendered manifest, `helm get manifest` and release history.
    # Kubernetes refuses both fields on one env entry at admission, but `helm
    # template` renders them happily -- so this script is the only gate that
    # sees it. The value itself is never printed.
    if "value" in entry:
        failures.append(f"[{assertion_id}] {ctx}: {env_name} carries an inline value alongside its "
                         "secretKeyRef, which puts the credential into the rendered manifest "
                         "(value not printed). Kubernetes rejects both fields at admission; "
                         "`helm template` does not, so only this assertion catches it.")
    ref = (entry.get("valueFrom") or {}).get("secretKeyRef")
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


def secret_string_data(render_dir):
    """stringData of the chart's OWN Secret in one render's secrets.yaml.

    Assertion 9 reads the emitted Secret rather than a consumer's env, because
    what it is proving is what the chart WRITES (a derived adapter entry, and
    which mail keys exist at all), not where a container reads it from. Both
    lookups go through here so the two branches cannot drift apart.
    """
    for doc in load_docs(f"{render_dir}/secrets.yaml"):
        if doc.get("kind") == "Secret" and (doc.get("metadata") or {}).get("name") == SECRET:
            return doc.get("stringData") or {}
    return {}


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
# The mail adapter is the third multi-key consumer template (after worker.yaml
# and dispatcher.yaml): all three mail keys resolve on its single container, so
# a per-key escape that only half-landed shows up as one of these three.
check_ref("1l", f"{d}/mail-adapter.yaml", "mail-adapter", "CURIE_CHANNEL_TOKEN",
          SECRET, "mailChannelToken", "mailChannelToken via mail-adapter.yaml")
check_ref("1m", f"{d}/mail-adapter.yaml", "mail-adapter", "CURIE_EGRESS_SECRET",
          SECRET, "mailEgressSecret", "mailEgressSecret via mail-adapter.yaml")
check_ref("1n", f"{d}/mail-adapter.yaml", "mail-adapter", "AGENTMAIL_API_KEY",
          SECRET, "mailAgentmailApiKey", "mailAgentmailApiKey via mail-adapter.yaml")

# ---- 2: *ExistingSecret makes every consumer resolve to the named Secret
#         instead, winning over the stale inline value the same render also
#         carries for every field -- so this group already IS the "wins over
#         stale" proof for all eleven keys, not just the multi-consumer
#         ones; a separate assertion 3 repeating 2b/2h byte-for-byte would add
#         no coverage of its own. ----
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
# Same three mail keys on the third multi-key consumer template, each pointed at
# its own BYO Secret over the stale inline value this render also carries.
check_ref("2l", f"{b}/mail-adapter.yaml", "mail-adapter", "CURIE_CHANNEL_TOKEN",
          "byo-channeltoken", "mailChannelToken", "mailChannelToken via mail-adapter.yaml")
check_ref("2m", f"{b}/mail-adapter.yaml", "mail-adapter", "CURIE_EGRESS_SECRET",
          "byo-egresssecret", "mailEgressSecret", "mailEgressSecret via mail-adapter.yaml")
check_ref("2n", f"{b}/mail-adapter.yaml", "mail-adapter", "AGENTMAIL_API_KEY",
          "byo-apikey", "mailAgentmailApiKey", "mailAgentmailApiKey via mail-adapter.yaml")

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

# ---- 9a-9b: the curie.adapterCredentials derivation (_helpers.tpl) and its BYO
#      branch, read off the chart's own Secret. adapterCredentials is a
#      JSON-encoded string, so it is parsed rather than substring-matched.
#      Failure messages name the credential and never print its value. ----
MAIL_SLUG = "mail-adapter"          # .Values.mailAdapter.adapterSlug
MAIL_KEYS = ("mailChannelToken", "mailEgressSecret", "mailAgentmailApiKey")

# Parsed once per render directory -- both the 9a and 9b branches below read
# these same stringData dicts rather than each re-opening and re-parsing
# secrets.yaml a second time.
default_sd = secret_string_data(DEFAULT_DIR)
byo_sd = secret_string_data(BYO_DIR)


def adapter_credentials(assertion_id, string_data, ctx):
    """Parsed adapterCredentials map, or None with a failure already recorded."""
    raw = string_data.get("adapterCredentials")
    if raw is None:
        failures.append(f"[{assertion_id}] {ctx}: the chart Secret has no adapterCredentials key")
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        failures.append(f"[{assertion_id}] {ctx}: adapterCredentials is not valid JSON "
                        "(curie.adapterCredentials must emit a toJson map)")
        return None
    if not isinstance(parsed, dict):
        failures.append(f"[{assertion_id}] {ctx}: adapterCredentials parsed to "
                        f"{type(parsed).__name__}, expected an object")
        return None
    return parsed


# Default (non-BYO) branch: the chart derives the worker's half of the egress
# pair from mailAdapter.egressSecret, so both ends rotate together, and the
# Secret carries all three mail credentials the adapter reads.
creds = adapter_credentials("9a", default_sd, "default render")
if creds is not None and creds.get(MAIL_SLUG) != "default-egresssecret":
    failures.append(f"[9a] default render: adapterCredentials[{MAIL_SLUG!r}] is missing or does "
                    "not equal mailAdapter.egressSecret (value not printed -- it is a live "
                    "egress credential). The worker's half must be derived from it, or the two "
                    "ends of the pair rotate independently and egress 401s.")
for key in MAIL_KEYS:
    if key not in default_sd:
        failures.append(f"[9a] default render: chart Secret is missing {key}, which the mail "
                        "adapter reads from it when no *ExistingSecret is set")

# BYO branch: with the adapter's egress credential externally sourced, the plain
# mailAdapter.egressSecret is unused and the required external worker map is the
# independent source of truth -- so the chart must neither derive the adapter's
# entry from that value nor emit any of the three mail keys. Emitting them would
# put the very credential this escape exists to keep OUT of helm values straight
# back into the chart Secret.
byo_creds = adapter_credentials("9b", byo_sd, "BYO render")
if byo_creds is not None and MAIL_SLUG in byo_creds:
    failures.append(f"[9b] BYO render: adapterCredentials still carries an entry for "
                    f"{MAIL_SLUG!r} with mailAdapter.egressSecretExistingSecret set. On the BYO "
                    "branch the plain mailAdapter.egressSecret is unused, and the external "
                    "worker map is the only source of truth.")
for key in MAIL_KEYS:
    if key in byo_sd:
        failures.append(f"[9b] BYO render: chart Secret still emits {key} even though its own "
                        "*ExistingSecret is set. That writes the externally managed credential "
                        "back into helm values-sourced chart state, which is exactly what the "
                        "escape exists to prevent.")

if failures:
    for msg in failures:
        print(f"FAIL {msg}", file=sys.stderr)
    print(f"{len(failures)} of 34 python-side assertions failed "
          "(assertions 1, 2, 5a-5d and 9a-9b; 4a-4b, 7 and 9c-9d run separately in bash)",
          file=sys.stderr)
    sys.exit(1)
print("OK: all 34 python-side assertions passed (1a-1n, 2a-2n, 5a-5d, 9a-9b)")
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

# ------------------------------------------------------------------------
# Assertion 7: worker.yaml's checksum/adapter-credentials annotation must
# change when adapterCredentialsExistingSecret changes, holding the inline
# adapterCredentials value constant -- otherwise switching (or clearing)
# which BYO Secret backs this credential never rolls the worker Deployment,
# silently narrowing the rollout guarantee this annotation exists for.
# ------------------------------------------------------------------------
render checksum-base \
  --set worker.adapterCredentials.myadapter=constant-value
render checksum-existing-secret \
  --set worker.adapterCredentials.myadapter=constant-value \
  --set worker.adapterCredentialsExistingSecret=my-adapter-secret
CHECKSUM_BASE="$(grep -o 'checksum/adapter-credentials: .*' "$TMP/checksum-base/curie/templates/worker.yaml")"
CHECKSUM_WITH_SECRET="$(grep -o 'checksum/adapter-credentials: .*' "$TMP/checksum-existing-secret/curie/templates/worker.yaml")"
if [ -z "$CHECKSUM_BASE" ] || [ -z "$CHECKSUM_WITH_SECRET" ]; then
  echo "FAIL [7] checksum/adapter-credentials annotation missing from a rendered worker.yaml" >&2
  FAILED=1
elif [ "$CHECKSUM_BASE" = "$CHECKSUM_WITH_SECRET" ]; then
  echo "FAIL [7] checksum/adapter-credentials did not change when" \
       "worker.adapterCredentialsExistingSecret was set (inline value held constant)." \
       "Switching which BYO Secret backs this credential must roll the worker Deployment." >&2
  FAILED=1
else
  echo "OK [7] checksum/adapter-credentials changes when adapterCredentialsExistingSecret changes"
fi

# 9c and 9d differ by exactly the two *ExistingSecret flags 9c appends below
# and nothing else -- that is precisely what makes 9d a control for 9c, so
# these two argument lists must not be allowed to drift apart.
EQUALITY_ARGS=(
  --set mailAdapter.deploy=true
  --set-string "mailAdapter.agentmail.httpsCidrs[0]=${MAIL_HTTPS_CIDR}"
  --set mailAdapter.channelToken=t
  --set mailAdapter.egressSecret=PLAIN-DIFFERENT
  --set mailAdapter.agentmail.apiKey=k
  --set worker.adapterCredentials.mail-adapter=SOMETHING-ELSE
)

# ------------------------------------------------------------------------
# Assertion 9c/9d: the curie.adapterCredentials equality check has a BYO
# branch. Normally the chart REFUSES a render where worker.adapterCredentials
# already carries an entry for the mail adapter's slug that disagrees with
# mailAdapter.egressSecret -- the two are one credential and a silent
# disagreement is a 401 at egress time that reads like a code bug. But once
# mailAdapter.egressSecretExistingSecret is set, the plain
# mailAdapter.egressSecret is unused: the external worker map is the
# independent source of truth, so there is nothing left to disagree WITH and
# the check must not fire. 9c proves the render succeeds on that branch; 9d is
# the negative control that the check itself still exists, and it pins the
# check's OWN diagnostic rather than "the render failed somehow" -- a bare
# non-zero exit would keep 9d green if helm were missing, a new fail-closed gate
# fired first, or a replacement fail() no longer implemented this check at all,
# which is exactly the false pass that would hollow out 9c.
# mail-adapter-wiring-assertions.sh assertion 13c is the sibling control of the
# same shape.
# ------------------------------------------------------------------------
# 9c invokes `helm template` directly rather than through render(): 9c's argv
# carries the two sentinel values (PLAIN-DIFFERENT and SOMETHING-ELSE) that 9d
# asserts must never appear in the chart's own refusal, so routing 9c through
# a helper that echoes its argv ("$*") on an unexpected failure would have this
# file print the very strings its sibling assertion exists to prove are never
# printed -- the pre-existing default and byo renders above also pass
# synthetic credential values through render(), so "this invocation's argv is
# nothing but credential values" was never what set 9c apart. Hardening
# render() itself never to echo raw argv is the deeper fix, deliberately left
# out of scope here: that helper is shared with eight assertions this ticket
# does not touch.
rm -rf "$TMP/adaptercreds-byo-branch"
helm template curie "$CHART" -n curie --output-dir "$TMP/adaptercreds-byo-branch" \
  "${EQUALITY_ARGS[@]}" \
  --set mailAdapter.egressSecretExistingSecret=byo-egresssecret \
  --set worker.adapterCredentialsExistingSecret=byo-adaptercreds \
  >/dev/null 2>"$TMP/adaptercreds-byo-branch.stderr"
BYO_BRANCH_RC=$?
if [ "$BYO_BRANCH_RC" -eq 0 ]; then
  echo "OK [9c] adapterCredentials equality check does not fire when the adapter's egress credential is externally sourced"
else
  echo "FAIL [9c] render was refused (rc=$BYO_BRANCH_RC) even though" \
       "mailAdapter.egressSecretExistingSecret is set. On the BYO branch the plain" \
       "mailAdapter.egressSecret is unused, so the equality check against" \
       "worker.adapterCredentials must not fire -- otherwise an operator who moved this" \
       "credential out of helm values can never render the chart again." \
       "(Neither the argv nor the captured stderr is echoed: both carry credential values.)" >&2
  FAILED=1
fi

# Negative control: the SAME disagreement with ONLY egressSecretExistingSecret
# removed must still FAIL the render, AND fail with this check's own diagnostic.
# Without that, 9c would also pass if the equality check had simply been deleted
# and something else refused the render. `helm template` is invoked directly
# rather than through render(), which reports any non-zero exit as a failure --
# here a non-zero exit is the expected result. The captured stderr is written
# under "$TMP" and never echoed: it is matched only for the two configuration
# key names the check's own fail() prints, and asserted NOT to contain either
# sentinel -- that absence is what pins fail()'s refusal to print either half of
# a live egress credential. Matching the key names rather than the whole
# sentence keeps ordinary rewording of the message out of CI's way.
NEG_STDERR="$TMP/adaptercreds-equality-neg.stderr"
helm template curie "$CHART" -n curie \
  "${EQUALITY_ARGS[@]}" \
  >/dev/null 2>"$NEG_STDERR"
NEG_RC=$?
if [ "$NEG_RC" -eq 0 ]; then
  echo "FAIL [9d] render SUCCEEDED with worker.adapterCredentials.mail-adapter disagreeing with" \
       "mailAdapter.egressSecret and no *ExistingSecret set. The equality check must still refuse" \
       "that combination -- otherwise 9c passes because the check is gone, not because the BYO" \
       "branch skips it." >&2
  FAILED=1
elif ! grep -qF 'worker.adapterCredentials.mail-adapter' "$NEG_STDERR" \
  || ! grep -qF 'mailAdapter.egressSecret' "$NEG_STDERR"; then
  echo "FAIL [9d] the render failed (rc=$NEG_RC) but its stderr does not name BOTH" \
       "worker.adapterCredentials.mail-adapter and mailAdapter.egressSecret, so this control" \
       "cannot tell the equality check's own refusal apart from an unrelated failure (a new" \
       "fail-closed gate, a schema rejection, a missing helm). Re-point the match at the" \
       "check's current diagnostic, or restore the check." >&2
  FAILED=1
elif grep -qF 'PLAIN-DIFFERENT' "$NEG_STDERR" || grep -qF 'SOMETHING-ELSE' "$NEG_STDERR"; then
  echo "FAIL [9d] the equality check's refusal printed one of the two disagreeing values." \
       "Both halves of the mail adapter's egress pair are live credentials and the chart's fail()" \
       "must name the configuration keys only -- an error string lands in CI logs, terminal" \
       "scrollback and helm's own output." >&2
  FAILED=1
else
  echo "OK [9d] negative control: the equality check still refuses a disagreement off the BYO branch, naming both keys and neither value"
fi

# ---- 8 (note, not an assertion): a BYO Secret missing the referenced key
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
