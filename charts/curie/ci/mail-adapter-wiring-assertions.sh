#!/usr/bin/env bash
#
# Render-assertion test for the mail adapter's chart wiring (#1515). Proves,
# with `helm template` alone (no cluster), that the first-party email channel
# deploys off, deploys correctly when asked for, receives every credential by
# reference, receives NO platform API key, cannot split the egress secret pair,
# and rolls its pods when any of its three credentials rotate.
#
#   1  Default install renders no mail-adapter Deployment and no Service.
#   2  mailAdapter.deploy=true renders both, and the Service port tracks
#      mailAdapter.service.port.
#   3  CURIE_API_URL derives the in-chart API Service, tracks api.service.port,
#      and mailAdapter.apiBaseUrl overrides it verbatim.
#   4  No CURIE_API_KEY env exists on the container AT ALL. The adapter holds no
#      platform key; this is what keeps curie.env.api from being "helpfully"
#      included later.
#   5  CURIE_CHANNEL_TOKEN, CURIE_EGRESS_SECRET and AGENTMAIL_API_KEY each arrive
#      by secretKeyRef to the chart Secret, never as inline literals.
#   6  The chart Secret carries mailChannelToken, mailEgressSecret and
#      mailAgentmailApiKey, and the worker's adapterCredentials entry for the
#      same slug renders alongside them in the same render.
#   7  priorityClassName is the platform class (asserted here, not in
#      render-assertions.sh, whose assertion 8 would break on the default render
#      where this Deployment deliberately does not exist).
#   8  CURIE_MAIL_ALLOWED_SENDERS renders from mailAdapter.allowedSenders, and an
#      EMPTY list renders an empty value rather than being omitted, so the
#      adapter's boot gate fires instead of the variable silently defaulting.
#   9  Every configured knob arrives under the exact name MailAdapterConfig
#      reads. A typo in an env NAME renders green and is then ignored at runtime.
#   10 replicas is 1, --set mailAdapter.replicas=3 does not change it (the knob
#      must not exist), and the rollout strategy is Recreate. All routing state
#      is process-local, and a rolling update runs two pods for the duration of
#      every upgrade.
#   11 checksum/mail-adapter-credentials tracks ALL THREE credentials, proven by
#      three independent one-credential pairs. A one-credential assertion passes
#      against a two-thirds-correct checksum, which is the bug.
#   12 The egress pair cannot diverge: derived from mailAdapter.egressSecret when
#      the operator writes only that half, unchanged when both halves agree, and
#      a hard `helm template` FAILURE naming both keys when they differ -- and
#      that failure message names the two KEYS without printing either live
#      credential VALUE into terminal scrollback or a CI log.
#   13 The worker's checksum/adapter-credentials sees the DERIVED entry, so
#      rotating mailAdapter.egressSecret actually rolls the workers.
#   14 The default render is untouched: with mailAdapter.deploy false the Secret's
#      adapterCredentials and the worker's checksum are byte-identical to what the
#      chart rendered before this change.
#   15 The build path exists: mail-adapter is a built cell in ci.yaml's `images`
#      job and in EVERY release.yaml matrix that defines a `name` list. An
#      include-only entry publishes nothing, and a build matrix without the
#      manifest-merge matrix publishes per-arch digests with no manifest or tag.
#
# NOTE ON `--output-dir`: copied deliberately from
# dispatcher-api-wiring-assertions.sh. In this environment `helm template` into a
# stdout pipe has been observed to truncate silently at ~41 lines while still
# exiting 0, which turns a rendered-fine env var into a reported-absent FALSE
# NEGATIVE. Rendering to a directory and reading the written files is the only
# trustworthy form here. Do not "fix" this back to a pipe.
#
# Fails loudly, naming the assertion and printing what rendered. Runnable locally
# and from CI.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CHART/../.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() {
  echo "ASSERTION FAILED: $1" >&2
  exit 1
}

# Release name `curie`, so curie.fullname is `curie` and the chart Secret is
# `curie-secrets` (same convention the dispatcher script asserts against).
RELEASE=curie
SECRET_NAME=curie-secrets
DEPLOY_NAME=curie-mail-adapter
SERVICE_NAME=curie-mail-adapter
SLUG=mail-adapter

# Turning the adapter on, plus the three credentials, is the render most
# assertions below want.
ON=(--set mailAdapter.deploy=true)
CREDS=(
  --set mailAdapter.channelToken=chn-assert-token
  --set mailAdapter.egressSecret=egress-assert-secret
  --set mailAdapter.agentmail.apiKey=am-assert-key
)

# ---------------------------------------------------------------------------
# Structural readers. Deliberately PyYAML over grep/awk, the same convention as
# read_key() in render-assertions.sh: a line-oriented reader silently mis-reads a
# requoted value, a reordered key, or a `valueFrom` that renders before `value`,
# and it fails as a FALSE PASS.
# ---------------------------------------------------------------------------

# field.py <rendered-dir> <kind> <name> [dotted.path]
#   exit 0 + prints the value, exit 1 if the path is missing,
#   exit 3 if no such object rendered.
FIELD_PY="$TMP/field.py"
cat > "$FIELD_PY" <<'PY'
import pathlib
import sys

import yaml

rendered, kind, name = sys.argv[1], sys.argv[2], sys.argv[3]
path = sys.argv[4] if len(sys.argv) > 4 else ""

found = []
for f in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(f.read_text()):
        if not isinstance(doc, dict):
            continue
        if doc.get("kind") == kind and (doc.get("metadata") or {}).get("name") == name:
            found.append(doc)

if not found:
    sys.stderr.write("no %s named %r rendered\n" % (kind, name))
    sys.exit(3)
if len(found) > 1:
    sys.stderr.write("%s %r rendered %d times\n" % (kind, name, len(found)))
    sys.exit(2)

node = found[0]
if path:
    for part in path.split("."):
        if isinstance(node, list):
            try:
                node = node[int(part)]
            except (ValueError, IndexError):
                sys.exit(1)
            continue
        if not isinstance(node, dict) or part not in node:
            sys.exit(1)
        node = node[part]
sys.stdout.write("" if node is None and path else str(node))
PY

# env.py <rendered-dir> <deployment-name> <env-name> [dotted.path]
#   exit 0 + prints the field, exit 1 if the env name (or the path under it) is
#   absent, exit 3 if the Deployment did not render or carries no containers.
ENV_PY="$TMP/env.py"
cat > "$ENV_PY" <<'PY'
import pathlib
import sys

import yaml

rendered, deployment, name = sys.argv[1], sys.argv[2], sys.argv[3]
path = sys.argv[4] if len(sys.argv) > 4 else "value"

docs = []
for f in sorted(pathlib.Path(rendered).rglob("*.yaml")):
    for doc in yaml.safe_load_all(f.read_text()):
        if (
            isinstance(doc, dict)
            and doc.get("kind") == "Deployment"
            and (doc.get("metadata") or {}).get("name") == deployment
        ):
            docs.append(doc)

if not docs:
    sys.stderr.write("no Deployment named %r rendered\n" % deployment)
    sys.exit(3)

containers = [
    c
    for d in docs
    for c in (((d.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
]
if not containers:
    sys.stderr.write("Deployment %r rendered no containers\n" % deployment)
    sys.exit(3)

entries = [e for c in containers for e in (c.get("env") or []) if e.get("name") == name]
if not entries:
    sys.exit(1)
if len(entries) > 1:
    sys.stderr.write("env %r appears %d times on %r\n" % (name, len(entries), deployment))
    sys.exit(2)

node = entries[0]
for part in path.split("."):
    if not isinstance(node, dict) or part not in node:
        sys.exit(1)
    node = node[part]
sys.stdout.write("" if node is None else str(node))
PY

field() { python3 "$FIELD_PY" "$@"; }
env_field() { python3 "$ENV_PY" "$@"; }

sha256_of() { python3 -c 'import hashlib,sys;sys.stdout.write(hashlib.sha256(sys.argv[1].encode()).hexdigest())' "$1"; }

# render <label> [helm args...] -> echoes the rendered directory
render() {
  local label="$1"
  shift
  local out="$TMP/render-$label"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --output-dir "$out" "$@" >/dev/null \
    || fail "$label: helm template exited non-zero (see the error above)"
  echo "$out"
}

# Assert one env entry carries an exact literal value.
assert_env_value() {
  # $1 = rendered dir, $2 = env name, $3 = expected value, $4 = why it matters
  local dir="$1" name="$2" want="$3" why="$4" got
  if ! got="$(env_field "$dir" "$DEPLOY_NAME" "$name" value)"; then
    fail "env '$name' is absent from the mail-adapter container (or carries no literal value). $why"
  fi
  [ "$got" = "$want" ] \
    || fail "env '$name' rendered '$got', expected '$want'. $why"
}

# Assert one env entry arrives by secretKeyRef, with no inline literal.
assert_env_secret_ref() {
  # $1 = rendered dir, $2 = env name, $3 = expected Secret key
  local dir="$1" name="$2" key="$3" got
  if ! got="$(env_field "$dir" "$DEPLOY_NAME" "$name" valueFrom.secretKeyRef.name)"; then
    fail "env '$name' does not arrive by valueFrom.secretKeyRef; an inline credential lands in 'helm get manifest' and in every rendered artifact CI uploads"
  fi
  [ "$got" = "$SECRET_NAME" ] \
    || fail "env '$name' secretKeyRef names Secret '$got', expected the chart Secret '$SECRET_NAME'"
  got="$(env_field "$dir" "$DEPLOY_NAME" "$name" valueFrom.secretKeyRef.key)"
  [ "$got" = "$key" ] \
    || fail "env '$name' secretKeyRef uses key '$got', expected '$key' (the key secrets.yaml renders; a different key silently reads an empty credential)"
  if env_field "$dir" "$DEPLOY_NAME" "$name" value >/dev/null 2>&1; then
    got="$(env_field "$dir" "$DEPLOY_NAME" "$name" value)"
    fail "env '$name' renders an inline literal value '$got'; the credential must come from the Secret by reference only"
  fi
}

# ---------------------------------------------------------------------------
# 1: a default install renders no mail adapter at all.
# ---------------------------------------------------------------------------
default_dir="$(render default)"
if field "$default_dir" Deployment "$DEPLOY_NAME" >/dev/null 2>&1; then
  fail "a default install rendered Deployment '$DEPLOY_NAME'; mailAdapter.deploy must default to false"
fi
if field "$default_dir" Service "$SERVICE_NAME" >/dev/null 2>&1; then
  fail "a default install rendered Service '$SERVICE_NAME'; mailAdapter.deploy must default to false"
fi

# ---------------------------------------------------------------------------
# 2: mailAdapter.deploy=true renders both objects, and the Service port tracks
#    mailAdapter.service.port rather than being hardcoded.
# ---------------------------------------------------------------------------
on_dir="$(render on "${ON[@]}" "${CREDS[@]}")"
field "$on_dir" Deployment "$DEPLOY_NAME" >/dev/null \
  || fail "mailAdapter.deploy=true rendered no Deployment named '$DEPLOY_NAME'"
field "$on_dir" Service "$SERVICE_NAME" >/dev/null \
  || fail "mailAdapter.deploy=true rendered no Service named '$SERVICE_NAME'; the worker has nowhere to deliver reply events"
actual="$(field "$on_dir" Service "$SERVICE_NAME" spec.ports.0.port)"
[ "$actual" = "8080" ] \
  || fail "Service port is '$actual', expected the mailAdapter.service.port default '8080'"

port_dir="$(render port "${ON[@]}" "${CREDS[@]}" --set mailAdapter.service.port=9091)"
actual="$(field "$port_dir" Service "$SERVICE_NAME" spec.ports.0.port)"
[ "$actual" = "9091" ] \
  || fail "with mailAdapter.service.port=9091 the Service port is '$actual'; the port is hardcoded in the template instead of read from the value"

# ---------------------------------------------------------------------------
# 3: CURIE_API_URL derivation. Unwired, the adapter falls back to its code
#    default (itself) and every turn it starts is never posted.
# ---------------------------------------------------------------------------
assert_env_value "$on_dir" CURIE_API_URL "http://curie-api:8000" \
  "The adapter must be told where the in-chart API Service is."
api_port_dir="$(render apiport "${ON[@]}" "${CREDS[@]}" --set api.service.port=9999)"
assert_env_value "$api_port_dir" CURIE_API_URL "http://curie-api:9999" \
  "The port must come from .Values.api.service.port, not a hardcoded 8000."
byo_dir="$(render byo "${ON[@]}" "${CREDS[@]}" --set mailAdapter.apiBaseUrl=http://byo-api.example:8080)"
assert_env_value "$byo_dir" CURIE_API_URL "http://byo-api.example:8080" \
  "mailAdapter.apiBaseUrl must override verbatim (the api.deploy=false path)."

# ---------------------------------------------------------------------------
# 4: NO CURIE_API_KEY on the container, at all. Hard security assertion.
# ---------------------------------------------------------------------------
set +e
env_field "$on_dir" "$DEPLOY_NAME" CURIE_API_KEY name >/dev/null 2>&1
rc=$?
set -e
if [ "$rc" -eq 3 ]; then
  fail "the mail-adapter Deployment did not render containers, so the CURIE_API_KEY assertion would pass vacuously"
fi
if [ "$rc" -ne 1 ]; then
  fail "the mail-adapter container carries a CURIE_API_KEY env entry. The adapter authenticates with its own channel token and must hold NO platform API key; this is almost certainly curie.env.api being included in the template"
fi

# ---------------------------------------------------------------------------
# 5: all three credentials arrive by secretKeyRef, never inline.
# ---------------------------------------------------------------------------
assert_env_secret_ref "$on_dir" CURIE_CHANNEL_TOKEN mailChannelToken
assert_env_secret_ref "$on_dir" CURIE_EGRESS_SECRET mailEgressSecret
assert_env_secret_ref "$on_dir" AGENTMAIL_API_KEY mailAgentmailApiKey

# ---------------------------------------------------------------------------
# 6: the Secret carries the three keys, and the worker's adapterCredentials
#    entry for the same slug renders in the SAME render, so both halves of the
#    egress pair are present at once.
# ---------------------------------------------------------------------------
actual="$(field "$on_dir" Secret "$SECRET_NAME" stringData.mailChannelToken)"
[ "$actual" = "chn-assert-token" ] \
  || fail "Secret key 'mailChannelToken' rendered '$actual', expected the configured 'chn-assert-token'"
actual="$(field "$on_dir" Secret "$SECRET_NAME" stringData.mailEgressSecret)"
[ "$actual" = "egress-assert-secret" ] \
  || fail "Secret key 'mailEgressSecret' rendered '$actual', expected the configured 'egress-assert-secret'"
actual="$(field "$on_dir" Secret "$SECRET_NAME" stringData.mailAgentmailApiKey)"
[ "$actual" = "am-assert-key" ] \
  || fail "Secret key 'mailAgentmailApiKey' rendered '$actual', expected the configured 'am-assert-key'"

creds_json="$(field "$on_dir" Secret "$SECRET_NAME" stringData.adapterCredentials)"
python3 -c '
import json, sys
raw, slug, want = sys.argv[1], sys.argv[2], sys.argv[3]
got = json.loads(raw).get(slug)
if got != want:
    sys.stderr.write("adapterCredentials[%r] is %r, expected %r (raw: %s)\n" % (slug, got, want, raw))
    sys.exit(1)
' "$creds_json" "$SLUG" "egress-assert-secret" \
  || fail "the worker half of the egress pair is missing or wrong; the worker will present nothing (or the wrong secret) and every reply delivery 401s"

# ---------------------------------------------------------------------------
# 7: priorityClassName is the platform class. The control plane must outrank
#    sandbox pods for node-pressure eviction.
# ---------------------------------------------------------------------------
actual="$(field "$on_dir" Deployment "$DEPLOY_NAME" spec.template.spec.priorityClassName || true)"
[ "$actual" = "curie-platform" ] \
  || fail "mail-adapter pod priorityClassName is '$actual', expected 'curie-platform' (.Values.priorityClasses.platform.name)"

# ---------------------------------------------------------------------------
# 8: CURIE_MAIL_ALLOWED_SENDERS renders, and an EMPTY list renders an EMPTY
#    VALUE rather than being omitted, so the adapter's boot gate fires instead of
#    the variable silently defaulting.
# ---------------------------------------------------------------------------
assert_env_value "$on_dir" CURIE_MAIL_ALLOWED_SENDERS "" \
  "An empty mailAdapter.allowedSenders must render the variable with an empty value, not omit it."
senders_dir="$(render senders "${ON[@]}" "${CREDS[@]}" --set 'mailAdapter.allowedSenders={ops@example.com,dev@example.com}')"
assert_env_value "$senders_dir" CURIE_MAIL_ALLOWED_SENDERS "ops@example.com,dev@example.com" \
  "The configured allow-list must reach the process comma-joined."

# ---------------------------------------------------------------------------
# 9: every configured knob arrives under the exact name MailAdapterConfig reads.
#    Non-default sentinels throughout: a template that hardcodes the default
#    passes an equals-the-default assertion while ignoring the operator.
# ---------------------------------------------------------------------------
knobs_dir="$(render knobs "${ON[@]}" "${CREDS[@]}" \
  --set mailAdapter.inbox=assert-inbox@example.com \
  --set mailAdapter.pollIntervalSeconds=37 \
  --set mailAdapter.ingressEnabled=false)"
assert_env_value "$knobs_dir" AGENTMAIL_INBOX "assert-inbox@example.com" \
  "mailAdapter.inbox must reach the process; a name typo renders green and is ignored at runtime."
assert_env_value "$knobs_dir" CURIE_MAIL_POLL_INTERVAL_SECONDS "37" \
  "mailAdapter.pollIntervalSeconds must reach the process as a quoted string."
assert_env_value "$knobs_dir" ADAPTER_INGRESS_ENABLED "false" \
  "mailAdapter.ingressEnabled must reach the process as a quoted string; an unquoted YAML bool is rejected by the API server."

# ---------------------------------------------------------------------------
# 10: one replica, no knob that changes it, and Recreate. Every routing map in
#     the adapter is process-local and the Service has no session affinity, so a
#     second pod answers turn.completed for a conversation it never saw.
# ---------------------------------------------------------------------------
actual="$(field "$on_dir" Deployment "$DEPLOY_NAME" spec.replicas)"
[ "$actual" = "1" ] \
  || fail "mail-adapter spec.replicas is '$actual', expected 1 (all routing state is process-local)"
replicas_dir="$(render replicas "${ON[@]}" "${CREDS[@]}" --set mailAdapter.replicas=3)"
actual="$(field "$replicas_dir" Deployment "$DEPLOY_NAME" spec.replicas)"
[ "$actual" = "1" ] \
  || fail "--set mailAdapter.replicas=3 rendered spec.replicas '$actual'; the replica count must be hardcoded 1 with NO values key, or the chart advertises a knob that silently breaks reply routing"
actual="$(field "$on_dir" Deployment "$DEPLOY_NAME" spec.strategy.type || true)"
[ "$actual" = "Recreate" ] \
  || fail "mail-adapter spec.strategy.type is '$actual', expected 'Recreate'; the default rolling update runs two pods at once for the duration of every upgrade, which is the same split-brain replicas:1 exists to prevent"

# ---------------------------------------------------------------------------
# 11: checksum/mail-adapter-credentials tracks ALL THREE credentials. Three
#     independent one-credential pairs: a checksum over the channel token alone
#     passes a one-credential assertion while rotating the egress secret or the
#     AgentMail key leaves the pod running on stale environment credentials.
# ---------------------------------------------------------------------------
ANNOTATION=spec.template.metadata.annotations.checksum/mail-adapter-credentials
base_sum="$(field "$on_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")" \
  || fail "the mail-adapter pod template carries no 'checksum/mail-adapter-credentials' annotation; rotating an expired token would leave the pod 401ing behind a green /healthz. It must be a POD TEMPLATE annotation, not a Deployment annotation, or it rolls nothing"
[ -n "$base_sum" ] \
  || fail "'checksum/mail-adapter-credentials' rendered empty"

assert_checksum_tracks() {
  # $1 = label, $2... = the single --set that differs from the baseline render
  local label="$1"
  shift
  local dir sum
  dir="$(render "sum-$label" "${ON[@]}" "${CREDS[@]}" "$@")"
  sum="$(field "$dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
  [ "$sum" != "$base_sum" ] \
    || fail "rotating $label did not change 'checksum/mail-adapter-credentials' (still '$base_sum'); that credential is not part of the hashed input, so a rotation leaves the pod running on the revoked value"
}
assert_checksum_tracks "mailAdapter.channelToken" --set mailAdapter.channelToken=chn-rotated
assert_checksum_tracks "mailAdapter.egressSecret" --set mailAdapter.egressSecret=egress-rotated
assert_checksum_tracks "mailAdapter.agentmail.apiKey" --set mailAdapter.agentmail.apiKey=am-rotated

# ---------------------------------------------------------------------------
# 12: the egress pair cannot diverge. (a) derived from mailAdapter.egressSecret
#     alone, (b) unchanged when the operator also writes an agreeing worker half,
#     (c) `helm template` FAILS, naming both keys, when they disagree. A mismatch
#     is not a preference: it is an operator who believes two different things,
#     and either choice ships an install whose reply path 401s.
# ---------------------------------------------------------------------------
pair_a_dir="$(render pair-a "${ON[@]}" --set mailAdapter.egressSecret=pair-secret)"
pair_a="$(field "$pair_a_dir" Secret "$SECRET_NAME" stringData.adapterCredentials)"
python3 -c '
import json, sys
got = json.loads(sys.argv[1]).get(sys.argv[2])
if got != sys.argv[3]:
    sys.stderr.write("adapterCredentials[%r] is %r, expected %r\n" % (sys.argv[2], got, sys.argv[3]))
    sys.exit(1)
' "$pair_a" "$SLUG" pair-secret \
  || fail "with only mailAdapter.egressSecret set, the chart did not derive worker.adapterCredentials['$SLUG']; the operator would have to write the worker half by hand"

pair_b_dir="$(render pair-b "${ON[@]}" --set mailAdapter.egressSecret=pair-secret \
  --set worker.adapterCredentials.mail-adapter=pair-secret)"
pair_b="$(field "$pair_b_dir" Secret "$SECRET_NAME" stringData.adapterCredentials)"
[ "$pair_b" = "$pair_a" ] \
  || fail "an AGREEING hand-written worker.adapterCredentials entry changed the rendered adapterCredentials from '$pair_a' to '$pair_b'; equal values must be accepted unchanged so a migration from hand-rolled manifests is not blocked"

set +e
mismatch_out="$(helm template "$RELEASE" "$CHART" "${ON[@]}" \
  --set mailAdapter.egressSecret=s1 \
  --set worker.adapterCredentials.mail-adapter=s2 2>&1)"
mismatch_rc=$?
set -e
if [ "$mismatch_rc" -eq 0 ]; then
  fail "a MISMATCHED egress pair (mailAdapter.egressSecret=s1, worker.adapterCredentials.$SLUG=s2) rendered successfully. The render must fail: whichever value wins, the install's reply path 401s and the operator believes otherwise"
fi
case "$mismatch_out" in
  *"worker.adapterCredentials"*) : ;;
  *) fail "the mismatch render failed, but its message never names 'worker.adapterCredentials.$SLUG'. Output was: $mismatch_out" ;;
esac
case "$mismatch_out" in
  *"mailAdapter.egressSecret"*) : ;;
  *) fail "the mismatch render failed, but its message never names 'mailAdapter.egressSecret' as the source of truth. Output was: $mismatch_out" ;;
esac

# (d) and it says all that WITHOUT printing either credential. The two values in
#     a mismatch are both live egress secrets, and this message goes to terminal
#     scrollback and to CI build logs, which are retained and frequently readable
#     far more widely than the values file. Distinctive sentinels here so the
#     absence check cannot pass by coincidence: a message that interpolates
#     either value contains the sentinel verbatim and fails this.
LEAK_A=zzsentinelegressalphazz
LEAK_B=zzsentinelworkerbetazz
set +e
leak_out="$(helm template "$RELEASE" "$CHART" "${ON[@]}" \
  --set mailAdapter.egressSecret="$LEAK_A" \
  --set worker.adapterCredentials.mail-adapter="$LEAK_B" 2>&1)"
leak_rc=$?
set -e
[ "$leak_rc" -ne 0 ] \
  || fail "the sentinel mismatch render succeeded; the divergence guard must still fail the render (see assertion 12c)"
case "$leak_out" in
  *"$LEAK_A"*) fail "the mismatch failure message printed the mailAdapter.egressSecret VALUE. A live credential must never be interpolated into an error that lands in CI logs; name the two configuration KEYS instead. Output was: $leak_out" ;;
esac
case "$leak_out" in
  *"$LEAK_B"*) fail "the mismatch failure message printed the worker.adapterCredentials.$SLUG VALUE. A live credential must never be interpolated into an error that lands in CI logs; name the two configuration KEYS instead. Output was: $leak_out" ;;
esac
case "$leak_out" in
  *"worker.adapterCredentials"*) : ;;
  *) fail "the sentinel mismatch message no longer names 'worker.adapterCredentials.$SLUG'; redacting the values must not cost the operator the two keys that disagree. Output was: $leak_out" ;;
esac
case "$leak_out" in
  *"mailAdapter.egressSecret"*) : ;;
  *) fail "the sentinel mismatch message no longer names 'mailAdapter.egressSecret'; redacting the values must not cost the operator the key that wins. Output was: $leak_out" ;;
esac

# ---------------------------------------------------------------------------
# 13: the worker's rotation trigger sees the DERIVED entry. Without this the
#     annotation is constant across the two renders and every worker keeps
#     presenting the revoked secret while the operator believes it is rotated.
# ---------------------------------------------------------------------------
WORKER_ANNOTATION=spec.template.metadata.annotations.checksum/adapter-credentials
rot1_dir="$(render worker-rot1 "${ON[@]}" --set mailAdapter.egressSecret=rot-one)"
rot2_dir="$(render worker-rot2 "${ON[@]}" --set mailAdapter.egressSecret=rot-two)"
rot1="$(field "$rot1_dir" Deployment curie-worker "$WORKER_ANNOTATION")" \
  || fail "the worker pod template carries no 'checksum/adapter-credentials' annotation"
rot2="$(field "$rot2_dir" Deployment curie-worker "$WORKER_ANNOTATION")"
[ "$rot1" != "$rot2" ] \
  || fail "rotating mailAdapter.egressSecret left the WORKER's 'checksum/adapter-credentials' unchanged ('$rot1'); the annotation still hashes .Values.worker.adapterCredentials instead of the coalesced map, so no worker restarts and every one keeps presenting the revoked secret"

# ---------------------------------------------------------------------------
# 14: the default render is untouched. This is what keeps the coalescing helper
#     from being a behavior change for every existing install.
# ---------------------------------------------------------------------------
actual="$(field "$default_dir" Secret "$SECRET_NAME" stringData.adapterCredentials)"
[ "$actual" = "{}" ] \
  || fail "with mailAdapter.deploy false the Secret's adapterCredentials rendered '$actual', expected the pre-existing '{}' byte for byte"
actual="$(field "$default_dir" Deployment curie-worker "$WORKER_ANNOTATION")"
expected="$(sha256_of '{}')"
[ "$actual" = "$expected" ] \
  || fail "with mailAdapter.deploy false the worker's checksum/adapter-credentials is '$actual', expected sha256('{}') = '$expected'. A mismatch means the value is double-encoded (include ... | toJson | sha256sum) or the helper emits stray whitespace, either of which rolls every worker on upgrade for no reason"

byo_creds_dir="$(render byo-creds --set worker.adapterCredentials.slack=hand-written)"
actual="$(field "$byo_creds_dir" Secret "$SECRET_NAME" stringData.adapterCredentials)"
[ "$actual" = '{"slack":"hand-written"}' ] \
  || fail "with the adapter off, a hand-written worker.adapterCredentials rendered '$actual', expected '{\"slack\":\"hand-written\"}' unchanged"
checksum="$(field "$byo_creds_dir" Deployment curie-worker "$WORKER_ANNOTATION")"
expected="$(sha256_of "$actual")"
[ "$checksum" = "$expected" ] \
  || fail "the worker checksum '$checksum' is not sha256 of the Secret value it is supposed to track ('$actual' hashes to '$expected'); the two have drifted apart"

# ---------------------------------------------------------------------------
# 15: the build path. The chart defaults to an image tag no workflow publishes
#     unless mail-adapter is a cell in ci.yaml's images matrix AND in every
#     release.yaml matrix that iterates a `name` list. An include-only entry
#     attaches a dockerfile to a cell that is never iterated and publishes
#     nothing; a build matrix without the manifest-merge matrix uploads per-arch
#     digests that nothing ever merges into a manifest or tag.
# ---------------------------------------------------------------------------
MATRIX_PY="$TMP/matrix.py"
cat > "$MATRIX_PY" <<'PY'
"""Assert `mail-adapter` is a built cell in the CI and release image matrices.

argv: <ci.yaml> <release.yaml> <image-name>
"""
import sys

import yaml

ci_path, release_path, image = sys.argv[1], sys.argv[2], sys.argv[3]
problems = []


def jobs(path):
    doc = yaml.safe_load(open(path)) or {}
    return (doc.get("jobs") or {}).items()


# ci.yaml: the `images` job is include-driven (name + dockerfile per cell), and
# an include entry IS the iteration there because no `name:` list exists.
ci_jobs = dict(jobs(ci_path))
images = ci_jobs.get("images")
if images is None:
    problems.append("ci.yaml has no `images` job at all")
else:
    include = ((images.get("strategy") or {}).get("matrix") or {}).get("include") or []
    names = [str(cell.get("name")) for cell in include if isinstance(cell, dict)]
    if not names:
        problems.append("ci.yaml `images` job has no matrix include entries; the check would be vacuous")
    elif image not in names:
        problems.append(
            "ci.yaml `images` job does not build %r (builds: %s); the image is never built on a PR"
            % (image, ", ".join(names))
        )

# release.yaml: iterate EVERY job whose matrix defines a `name` list rather than
# naming the two we know about, so a third matrix added later fails this too.
name_matrices = {}
for job_id, job in jobs(release_path):
    if not isinstance(job, dict):
        continue
    matrix = (job.get("strategy") or {}).get("matrix") or {}
    names = matrix.get("name")
    if isinstance(names, list):
        name_matrices[job_id] = [str(n) for n in names]

if len(name_matrices) < 2:
    problems.append(
        "release.yaml has %d job(s) with a `name` matrix list (%s); expected at least 2 (the per-arch build "
        "and the manifest merge). Either a matrix moved or this check has gone vacuous"
        % (len(name_matrices), ", ".join(sorted(name_matrices)) or "none")
    )

for job_id, names in sorted(name_matrices.items()):
    if image not in names:
        problems.append(
            "release.yaml job %r iterates %s and does not include %r. An `include:` entry alone attaches a "
            "dockerfile to a cell that never runs; a build matrix without the merge matrix publishes per-arch "
            "digests with no manifest and no tag" % (job_id, names, image)
        )

if problems:
    for p in problems:
        sys.stderr.write(p + "\n")
    sys.exit(1)

sys.stdout.write(
    "  ok: %r builds in ci.yaml `images` and in all %d release.yaml name matrices (%s)\n"
    % (image, len(name_matrices), ", ".join(sorted(name_matrices)))
)
PY

python3 "$MATRIX_PY" \
  "$REPO_ROOT/.github/workflows/ci.yaml" \
  "$REPO_ROOT/.github/workflows/release.yaml" \
  mail-adapter \
  || fail "the mail-adapter image is not wired into the standard build path; see the message above"

echo "OK: mail-adapter chart wiring and build-path render assertions passed (15 assertions)"
