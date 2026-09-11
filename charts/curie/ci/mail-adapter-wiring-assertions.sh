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
#   3  Byte-limit defaults and overrides render as exact base-10 integer strings.
#   4  CURIE_API_URL derives the in-chart API Service, tracks api.service.port,
#      and mailAdapter.apiBaseUrl overrides it verbatim.
#   5  No CURIE_API_KEY env exists on the container AT ALL. The adapter holds no
#      platform key; this is what keeps curie.env.api from being "helpfully"
#      included later.
#   6  CURIE_CHANNEL_TOKEN, CURIE_EGRESS_SECRET and AGENTMAIL_API_KEY each arrive
#      by secretKeyRef, never as inline literals. Each per-field existingSecret
#      overrides both the Secret name and key without making the reference
#      optional, so a missing external key fails closed.
#   7  The chart Secret carries mailChannelToken, mailEgressSecret and
#      mailAgentmailApiKey only for chart-managed values, omits each externally
#      sourced key, and renders the worker's adapterCredentials entry alongside
#      the chart-managed egress credential in the same render. An externally
#      sourced adapter egress credential requires the worker's external
#      credential map, omits the mail slug from the chart-managed map, and has a
#      value-safe render failure when that pair is split.
#   8  priorityClassName is the platform class (asserted here, not in
#      render-assertions.sh, whose assertion 8 would break on the default render
#      where this Deployment deliberately does not exist).
#   9  CURIE_MAIL_ALLOWED_SENDERS renders from mailAdapter.allowedSenders, and an
#      EMPTY list renders an empty value rather than being omitted, so the
#      adapter's boot gate fires instead of the variable silently defaulting.
#   10 Every configured knob arrives under the exact name MailAdapterConfig
#      reads. A typo in an env NAME renders green and is then ignored at runtime.
#   11 replicas is 1, --set mailAdapter.replicas=3 does not change it (the knob
#      must not exist), and the rollout strategy is Recreate. All routing state
#      is process-local, and a rolling update runs two pods for the duration of
#      every upgrade.
#   12 checksum/mail-adapter-credentials uses incoming Helm values for
#      chart-managed credentials and live Secret data for external sources,
#      with a stable source-ref fallback when no cluster read is available.
#   13 The chart-managed egress pair cannot diverge: derived from
#      mailAdapter.egressSecret when the operator writes only that half,
#      unchanged when both halves agree, and a hard `helm template` FAILURE
#      naming both keys when they differ -- without printing either live
#      credential VALUE into terminal scrollback or a CI log.
#   14 The worker's checksum/adapter-credentials sees the DERIVED entry, so
#      rotating mailAdapter.egressSecret actually rolls the workers.
#   15 The default render is untouched: with mailAdapter.deploy false the Secret's
#      adapterCredentials and the worker's checksum are byte-identical to what the
#      chart rendered before this change.
#   16 The build path exists: mail-adapter is a built cell in ci.yaml's `images`
#      job and in EVERY release.yaml matrix that defines a `name` list. An
#      include-only entry publishes nothing, and a build matrix without the
#      manifest-merge matrix publishes per-arch digests with no manifest or tag.
#   17 A BYO API (`api.deploy=false`) fails closed without an explicit API
#      egress CIDR and renders only that CIDR on the configured TCP port.
#   18 AgentMail and BYO-API CIDR controls reject equivalent default routes,
#      including IPv4/IPv6 /1 splits and alternate /0 spellings, while valid
#      IPv4 and IPv6 provider ranges still render.
#   19 The existing-claim preflight accepts exactly RWO or RWOP and rejects RWX
#      by executing the rendered hook command against a deterministic kubectl
#      fixture, rather than merely grepping its shell source.
#   20 The preflight Job opts out of token mounting and satisfies the Restricted
#      Pod Security Standard at both pod and container scope.
#   21 The container carries the SHARED OTLP env -- the same OTEL_EXPORTER_OTLP_*
#      names and values the other instrumented workloads get -- when the chart
#      collector is deployed, the external endpoint verbatim when it is not, and
#      NOTHING when telemetry is explicitly disabled. The adapter joined the
#      existing telemetry boundary; it did not acquire a private one.
#   22 The single egress policy gains a collector peer exactly when
#      otelCollector.deploy is true. The adapter is the ONLY first-party service
#      with an egress-restricting policy, so OTLP env without this peer exports
#      into a dropped connection while every render still looks green.
#   23 An EXTERNAL OTLP endpoint fails closed. The adapter's effective endpoint
#      is curie.otel.endpoint, so when that is not this release's in-chart
#      Collector the render REFUSES unless mailAdapter.otelEgress.httpsCidrs
#      declares a narrow peer, which then renders as one ipBlock rule on the
#      port derived from the endpoint URL. Overbroad peers, a port contradicting
#      the URL, and a stale key set while the in-chart Collector is deployed all
#      refuse; otelCollector.telemetryDisabled=true remains the escape hatch.
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
ON=(
  --set mailAdapter.deploy=true
  --set 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24'
)
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
  helm template "$RELEASE" "$CHART" --namespace default --output-dir "$out" "$@" >/dev/null \
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
  # $1 = rendered dir, $2 = env name, $3 = expected Secret name,
  # $4 = expected Secret key
  local dir="$1" name="$2" secret="$3" key="$4" got
  if ! got="$(env_field "$dir" "$DEPLOY_NAME" "$name" valueFrom.secretKeyRef.name)"; then
    fail "env '$name' does not arrive by valueFrom.secretKeyRef; an inline credential lands in 'helm get manifest' and in every rendered artifact CI uploads"
  fi
  [ "$got" = "$secret" ] \
    || fail "env '$name' secretKeyRef names Secret '$got', expected '$secret'"
  got="$(env_field "$dir" "$DEPLOY_NAME" "$name" valueFrom.secretKeyRef.key)"
  [ "$got" = "$key" ] \
    || fail "env '$name' secretKeyRef uses key '$got', expected '$key'; a different key leaves the required credential unavailable"
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
# 3: integer-valued env must be rendered as exact base-10 strings. YAML scientific
# notation (for example 1.048576e+06) reaches the container as text and is then
# rejected by the adapter's integer parser even though Helm accepted the value.
# ---------------------------------------------------------------------------
assert_env_value "$on_dir" CURIE_MAIL_MAX_BODY_BYTES "1048576" \
  "The default integer must render in base 10, never scientific notation."
assert_env_value "$on_dir" CURIE_MAIL_MAX_REPLY_BYTES "1048576" \
  "The default integer must render in base 10, never scientific notation."
assert_env_value "$on_dir" CURIE_MAIL_MAX_STATE_BYTES "268435456" \
  "The default integer must render in base 10, never scientific notation."
integer_override_dir="$(render integer-override "${ON[@]}" "${CREDS[@]}" \
  --set mailAdapter.maxStateBytes=314572800)"
assert_env_value "$integer_override_dir" CURIE_MAIL_MAX_STATE_BYTES "314572800" \
  "An explicit integer override must render as an exact base-10 string."

# ---------------------------------------------------------------------------
# 4: CURIE_API_URL derivation. Unwired, the adapter falls back to its code
#    default (itself) and every turn it starts is never posted.
# ---------------------------------------------------------------------------
assert_env_value "$on_dir" CURIE_API_URL "http://curie-api:8000" \
  "The adapter must be told where the in-chart API Service is."
api_port_dir="$(render apiport "${ON[@]}" "${CREDS[@]}" --set api.service.port=9999)"
assert_env_value "$api_port_dir" CURIE_API_URL "http://curie-api:9999" \
  "The port must come from .Values.api.service.port, not a hardcoded 8000."
byo_api_dir="$(render byo-api "${ON[@]}" "${CREDS[@]}" \
  --set api.deploy=false \
  --set ui.deploy=false \
  --set mailAdapter.apiBaseUrl=https://byo-api.example:8443 \
  --set 'mailAdapter.apiEgress.httpsCidrs[0]=198.51.100.0/24' \
  --set mailAdapter.apiEgress.port=8443)"
assert_env_value "$byo_api_dir" CURIE_API_URL "https://byo-api.example:8443" \
  "mailAdapter.apiBaseUrl must override verbatim (the api.deploy=false path)."

# ---------------------------------------------------------------------------
# 5: NO CURIE_API_KEY on the container, at all. Hard security assertion.
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
# 6: all three credentials arrive by secretKeyRef, never inline.
# ---------------------------------------------------------------------------
assert_env_secret_ref "$on_dir" CURIE_CHANNEL_TOKEN "$SECRET_NAME" mailChannelToken
assert_env_secret_ref "$on_dir" CURIE_EGRESS_SECRET "$SECRET_NAME" mailEgressSecret
assert_env_secret_ref "$on_dir" AGENTMAIL_API_KEY "$SECRET_NAME" mailAgentmailApiKey

# ---------------------------------------------------------------------------
# 7: the Secret carries the three keys, and the worker's adapterCredentials
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

# An externally sourced adapter egress credential cannot be copied into the
# worker's chart-managed JSON map because Helm cannot read the external Secret's
# credential data. Require the worker's external credential map and keep the
# render failure actionable without interpolating any supplied value.
PAIR_SECRET_VALUE=zzmailpairexternalsecretzz
PAIR_KEY_VALUE=zzmailpairexternalkeyzz
PAIR_CREDENTIAL_VALUE=zzmailpaircredentialzz
set +e
unpaired_egress_out="$(helm template "$RELEASE" "$CHART" "${ON[@]}" "${CREDS[@]}" \
  --set-string mailAdapter.egressSecretExistingSecret="$PAIR_SECRET_VALUE" \
  --set-string mailAdapter.egressSecretExistingSecretKey="$PAIR_KEY_VALUE" \
  --set-string mailAdapter.egressSecret="$PAIR_CREDENTIAL_VALUE" 2>&1)"
unpaired_egress_rc=$?
set -e
[ "$unpaired_egress_rc" -ne 0 ] \
  || fail "mailAdapter.egressSecretExistingSecret rendered without worker.adapterCredentialsExistingSecret; the paired credential sources must fail closed"
case "$unpaired_egress_out" in
  *"mailAdapter.egressSecretExistingSecret"*) : ;;
  *) fail "the split egress-source failure did not name mailAdapter.egressSecretExistingSecret; output was: $unpaired_egress_out" ;;
esac
case "$unpaired_egress_out" in
  *"worker.adapterCredentialsExistingSecret"*) : ;;
  *) fail "the split egress-source failure did not name worker.adapterCredentialsExistingSecret; output was: $unpaired_egress_out" ;;
esac
for supplied_value in "$PAIR_SECRET_VALUE" "$PAIR_KEY_VALUE" "$PAIR_CREDENTIAL_VALUE"; do
  case "$unpaired_egress_out" in
    *"$supplied_value"*) fail "the split egress-source failure printed a supplied value; name configuration keys without echoing their contents. Output was: $unpaired_egress_out" ;;
  esac
done

# Each credential can instead point at an operator-managed Secret. No such
# Secret exists during this clusterless render: the non-optional secretKeyRefs
# are therefore also the falsifiable missing-reference path. The chart must not
# silently keep a same-named key in its own Secret as a fallback.
EXTERNAL_REFS=(
  --set mailAdapter.channelTokenExistingSecret=mail-channel-source
  --set mailAdapter.channelTokenExistingSecretKey=channel-token
  --set mailAdapter.egressSecretExistingSecret=mail-egress-source
  --set mailAdapter.egressSecretExistingSecretKey=egress-token
  --set mailAdapter.agentmail.apiKeyExistingSecret=mail-provider-source
  --set mailAdapter.agentmail.apiKeyExistingSecretKey=provider-token
  --set worker.adapterCredentialsExistingSecret=mail-worker-source
  --set worker.adapterCredentialsExistingSecretKey=adapter-egress-map
)
external_dir="$(render external-secrets "${ON[@]}" "${CREDS[@]}" "${EXTERNAL_REFS[@]}")"
assert_env_secret_ref "$external_dir" CURIE_CHANNEL_TOKEN mail-channel-source channel-token
assert_env_secret_ref "$external_dir" CURIE_EGRESS_SECRET mail-egress-source egress-token
assert_env_secret_ref "$external_dir" AGENTMAIL_API_KEY mail-provider-source provider-token

for name in CURIE_CHANNEL_TOKEN CURIE_EGRESS_SECRET AGENTMAIL_API_KEY; do
  if env_field "$external_dir" "$DEPLOY_NAME" "$name" valueFrom.secretKeyRef.optional >/dev/null 2>&1; then
    fail "env '$name' makes its external secretKeyRef optional; a missing Secret or key must prevent the adapter container from starting"
  fi
done

field "$external_dir" Secret "$SECRET_NAME" >/dev/null \
  || fail "the chart Secret did not render, so existingSecret omission checks would pass vacuously"
for key in mailChannelToken mailEgressSecret mailAgentmailApiKey; do
  if field "$external_dir" Secret "$SECRET_NAME" "stringData.$key" >/dev/null 2>&1; then
    fail "chart Secret key '$key' still renders when its mail-adapter existingSecret is set; a Helm upgrade would keep managing or overwrite the externally sourced credential"
  fi
done

# The raw egress value remains set in CREDS above on purpose. Once the adapter's
# egress source is external, the chart-managed worker map must neither derive
# the mail slug from that unused value nor divergence-check it against the
# independently managed external map.
external_chart_creds="$(field "$external_dir" Secret "$SECRET_NAME" stringData.adapterCredentials)"
python3 -c '
import json, sys
if sys.argv[2] in json.loads(sys.argv[1]):
    raise SystemExit("external egress source leaked the mail slug into chart-managed adapterCredentials")
' "$external_chart_creds" "$SLUG" \
  || fail "worker adapterCredentials still contains '$SLUG' when mailAdapter.egressSecretExistingSecret is set; the required external worker map must be the only paired egress source"

# Even a stale plain worker entry must not be compared with the stale plain mail
# value on the external path. Neither map is consumed there; the two external
# references above are authoritative.
external_stale_values_dir="$(render external-stale-values "${ON[@]}" "${EXTERNAL_REFS[@]}" \
  --set mailAdapter.egressSecret=unused-mail-value \
  --set worker.adapterCredentials.mail-adapter=unused-worker-value)"
field "$external_stale_values_dir" Deployment "$DEPLOY_NAME" >/dev/null \
  || fail "external egress sources failed to render when unused plain mail and worker values differed; the chart must not divergence-validate unconsumed values"

# ---------------------------------------------------------------------------
# 8: priorityClassName is the platform class. The control plane must outrank
#    sandbox pods for node-pressure eviction.
# ---------------------------------------------------------------------------
actual="$(field "$on_dir" Deployment "$DEPLOY_NAME" spec.template.spec.priorityClassName || true)"
[ "$actual" = "curie-platform" ] \
  || fail "mail-adapter pod priorityClassName is '$actual', expected 'curie-platform' (.Values.priorityClasses.platform.name)"

# ---------------------------------------------------------------------------
# 9: CURIE_MAIL_ALLOWED_SENDERS renders, and an EMPTY list renders an EMPTY
#    VALUE rather than being omitted, so the adapter's boot gate fires instead of
#    the variable silently defaulting.
# ---------------------------------------------------------------------------
assert_env_value "$on_dir" CURIE_MAIL_ALLOWED_SENDERS "" \
  "An empty mailAdapter.allowedSenders must render the variable with an empty value, not omit it."
senders_dir="$(render senders "${ON[@]}" "${CREDS[@]}" --set 'mailAdapter.allowedSenders={ops@example.com,dev@example.com}')"
assert_env_value "$senders_dir" CURIE_MAIL_ALLOWED_SENDERS "ops@example.com,dev@example.com" \
  "The configured allow-list must reach the process comma-joined."

# ---------------------------------------------------------------------------
# 10: every configured knob arrives under the exact name MailAdapterConfig reads.
#    Non-default sentinels throughout: a template that hardcodes the default
#    passes an equals-the-default assertion while ignoring the operator.
# ---------------------------------------------------------------------------
knobs_dir="$(render knobs "${ON[@]}" "${CREDS[@]}" \
  --set mailAdapter.inbox=assert-inbox@example.com \
  --set mailAdapter.pollIntervalSeconds=37 \
  --set mailAdapter.maxPendingDeliveries=17 \
  --set mailAdapter.maxBodyBytes=4096 \
  --set mailAdapter.maxReplyBytes=8192 \
  --set mailAdapter.maxStateBytes=1048576 \
  --set mailAdapter.ingressEnabled=false)"
assert_env_value "$knobs_dir" AGENTMAIL_INBOX "assert-inbox@example.com" \
  "mailAdapter.inbox must reach the process; a name typo renders green and is ignored at runtime."
assert_env_value "$knobs_dir" CURIE_MAIL_POLL_INTERVAL_SECONDS "37" \
  "mailAdapter.pollIntervalSeconds must reach the process as a quoted string."
assert_env_value "$knobs_dir" ADAPTER_INGRESS_ENABLED "false" \
  "mailAdapter.ingressEnabled must reach the process as a quoted string; an unquoted YAML bool is rejected by the API server."
assert_env_value "$knobs_dir" CURIE_MAIL_MAX_PENDING_DELIVERIES "17" \
  "mailAdapter.maxPendingDeliveries must reach the durable admission bound."
assert_env_value "$knobs_dir" CURIE_MAIL_MAX_BODY_BYTES "4096" \
  "mailAdapter.maxBodyBytes must bound provider bodies before allocation/storage."
assert_env_value "$knobs_dir" CURIE_MAIL_MAX_REPLY_BYTES "8192" \
  "mailAdapter.maxReplyBytes must bound egress text before allocation/storage."
assert_env_value "$knobs_dir" CURIE_MAIL_MAX_STATE_BYTES "1048576" \
  "mailAdapter.maxStateBytes must cap SQLite pages/queued bytes."

# ---------------------------------------------------------------------------
# 11: one replica, no knob that changes it, and Recreate. Every routing map in
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
# 10b: durable single-writer storage and least-capability pod wiring. The root
# filesystem remains read-only; only the SQLite directory and /tmp are writable.
# ---------------------------------------------------------------------------
STRUCTURE_PY="$TMP/mail-structure.py"
cat > "$STRUCTURE_PY" <<'PY'
import pathlib
import sys

import yaml

rendered, expected_claim, expect_pvc, expected_cidr = sys.argv[1:]
docs = []
for path in pathlib.Path(rendered).rglob("*.yaml"):
    docs.extend(d for d in yaml.safe_load_all(path.read_text()) if isinstance(d, dict))


def fail(message):
    raise SystemExit(message)


deployments = [d for d in docs if d.get("kind") == "Deployment" and d.get("metadata", {}).get("name") == "curie-mail-adapter"]
if len(deployments) != 1:
    fail(f"expected one mail Deployment, found {len(deployments)}")
deployment = deployments[0]
pod = deployment["spec"]["template"]["spec"]
container = pod["containers"][0]

if pod.get("automountServiceAccountToken") is not False:
    fail("mail pod must set automountServiceAccountToken: false")
if pod.get("securityContext", {}).get("fsGroup") != 1000:
    fail("mail pod must own the RWO volume through fsGroup 1000")
if container.get("securityContext", {}).get("readOnlyRootFilesystem") is not True:
    fail("mail container root filesystem must remain read-only")
if container.get("readinessProbe", {}).get("httpGet", {}).get("path") != "/readyz":
    fail("mail readiness must use /readyz after durable startup")
if container.get("livenessProbe", {}).get("httpGet", {}).get("path") != "/healthz":
    fail("mail liveness must remain /healthz")

env = {entry["name"]: entry for entry in container.get("env", [])}
state_path = env.get("CURIE_MAIL_STATE_PATH", {}).get("value", "")
if not state_path.endswith(".sqlite3"):
    fail(f"CURIE_MAIL_STATE_PATH is absent or not a SQLite file: {state_path!r}")
if "CURIE_API_KEY" in env:
    fail("mail pod gained CURIE_API_KEY")

volumes = {volume["name"]: volume for volume in pod.get("volumes", [])}
mounts = {mount["name"]: mount for mount in container.get("volumeMounts", [])}
state_mounts = [
    (name, mount)
    for name, mount in mounts.items()
    if state_path.startswith(mount.get("mountPath", "").rstrip("/") + "/")
]
if len(state_mounts) != 1:
    fail(f"state path must sit under exactly one volume mount, got {state_mounts!r}")
state_volume = volumes.get(state_mounts[0][0], {})
pvcs = [
    d
    for d in docs
    if d.get("kind") == "PersistentVolumeClaim"
    and d.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
    == "mail-adapter-state"
]
if expected_claim == "__managed__":
    if len(pvcs) != 1:
        fail(f"managed state must render one PVC, found {len(pvcs)}")
    expected_claim = pvcs[0]["metadata"]["name"]
if state_volume.get("persistentVolumeClaim", {}).get("claimName") != expected_claim:
    fail(f"state volume does not mount expected claim {expected_claim!r}: {state_volume!r}")
tmp_mounts = [name for name, mount in mounts.items() if mount.get("mountPath") == "/tmp"]
if len(tmp_mounts) != 1 or "emptyDir" not in volumes.get(tmp_mounts[0], {}):
    fail("/tmp must be a writable emptyDir under the read-only root")

if (len(pvcs) == 1) != (expect_pvc == "yes"):
    fail(f"managed/BYO PVC rendering drifted: expect_pvc={expect_pvc}, found={len(pvcs)}")
if pvcs and pvcs[0].get("spec", {}).get("accessModes") != ["ReadWriteOnce"]:
    fail(f"managed mail state must be RWO: {pvcs[0].get('spec', {}).get('accessModes')!r}")

policies = [d for d in docs if d.get("kind") == "NetworkPolicy" and d.get("spec", {}).get("podSelector", {}).get("matchLabels", {}).get("app.kubernetes.io/component") == "mail-adapter"]
if len(policies) != 1:
    fail(f"expected one mail-only NetworkPolicy, found {len(policies)}")
policy = policies[0]
selector = policy["spec"]["podSelector"].get("matchLabels", {})
if selector.get("app.kubernetes.io/component") != "mail-adapter":
    fail(f"policy selector could include runner-sandbox: {selector!r}")
if policy["spec"].get("policyTypes") != ["Egress"]:
    fail(f"mail policy must be egress-only: {policy['spec'].get('policyTypes')!r}")
cidrs = {
    peer.get("ipBlock", {}).get("cidr")
    for rule in policy["spec"].get("egress", [])
    for peer in rule.get("to", [])
    if peer.get("ipBlock")
}
if expected_cidr not in cidrs:
    fail(f"configured AgentMail CIDR absent from policy: {sorted(cidrs)!r}")

for doc in docs:
    if doc.get("kind") in {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding"} and doc.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == "mail-adapter":
        fail(f"mail adapter must not gain Kubernetes RBAC: {doc['kind']} {doc['metadata'].get('name')}")
PY

python3 "$STRUCTURE_PY" "$on_dir" __managed__ yes 203.0.113.0/24 \
  || fail "managed durable mail storage/security wiring is incomplete"

byo_dir="$(render byo-state "${ON[@]}" "${CREDS[@]}" --set mailAdapter.persistence.existingClaim=mail-state-existing)"
python3 "$STRUCTURE_PY" "$byo_dir" mail-state-existing no 203.0.113.0/24 \
  || fail "existingClaim must mount the named same-namespace RWO claim without rendering a second PVC"

non_rwo_dir="$(render non-rwo-knob "${ON[@]}" "${CREDS[@]}" --set 'mailAdapter.persistence.accessModes[0]=ReadWriteMany')"
actual="$(python3 - "$non_rwo_dir" <<'PY'
import pathlib, sys, yaml
pvcs = [
    doc
    for path in pathlib.Path(sys.argv[1]).rglob("*.yaml")
    for doc in yaml.safe_load_all(path.read_text())
    if isinstance(doc, dict)
    and doc.get("kind") == "PersistentVolumeClaim"
    and doc.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
    == "mail-adapter-state"
]
if len(pvcs) != 1:
    raise SystemExit("expected exactly one managed mail PVC")
print(pvcs[0]["spec"]["accessModes"][0], end="")
PY
)"
[ "$actual" = "ReadWriteOnce" ] \
  || fail "mailAdapter.persistence.accessModes changed the managed claim to '$actual'; RWO is a correctness invariant, not an operator knob"

# The existingClaim hook is runtime policy, not documentation: execute the
# rendered command with a deterministic kubectl fixture. This makes RWO and
# RWOP positive paths independently observable and proves that RWX is refused.
preflight_command="$(field "$byo_dir" Job curie-mail-persistence-preflight spec.template.spec.containers.0.command.2)" \
  || fail "existingClaim rendered no executable mail persistence preflight"
FAKE_KUBECTL_DIR="$TMP/fake-kubectl"
mkdir -p "$FAKE_KUBECTL_DIR"
cat > "$FAKE_KUBECTL_DIR/kubectl" <<'SH'
#!/usr/bin/env sh
case "$*" in
  *accessModes*) printf '%s' "${FAKE_ACCESS_MODES:?}" ;;
  *volumeMode*) printf '%s' "${FAKE_VOLUME_MODE:-Filesystem}" ;;
  *) echo "unexpected kubectl invocation: $*" >&2; exit 64 ;;
esac
SH
chmod +x "$FAKE_KUBECTL_DIR/kubectl"

run_claim_preflight() {
  FAKE_ACCESS_MODES="$1" \
  FAKE_VOLUME_MODE=Filesystem \
  CLAIM=mail-state-existing \
  NAMESPACE=default \
  PATH="$FAKE_KUBECTL_DIR:$PATH" \
    /bin/sh -c "$preflight_command"
}

run_claim_preflight ReadWriteOnce >/dev/null \
  || fail "existingClaim preflight rejected ReadWriteOnce"
run_claim_preflight ReadWriteOncePod >/dev/null \
  || fail "existingClaim preflight rejected ReadWriteOncePod, the strict single-pod writer mode"
if run_claim_preflight ReadWriteMany >/dev/null 2>&1; then
  fail "existingClaim preflight accepted ReadWriteMany; only RWO and RWOP preserve the single-writer invariant"
fi

# Restricted Pod Security is checked structurally because admission would reject
# the hook before its claim command ever runs. Assert every required field,
# including the service-account-token opt-out that is easy to omit on a Job.
python3 - "$byo_dir" <<'PY' \
  || fail "existingClaim preflight Job is not Restricted-compatible"
import pathlib
import sys

import yaml

jobs = [
    doc
    for path in pathlib.Path(sys.argv[1]).rglob("*.yaml")
    for doc in yaml.safe_load_all(path.read_text())
    if isinstance(doc, dict)
    and doc.get("kind") == "Job"
    and doc.get("metadata", {}).get("name") == "curie-mail-persistence-preflight"
]
if len(jobs) != 1:
    raise SystemExit(f"expected one mail persistence preflight Job, found {len(jobs)}")

pod = jobs[0]["spec"]["template"]["spec"]
if pod.get("automountServiceAccountToken") is not False:
    raise SystemExit("preflight must set automountServiceAccountToken: false")
pod_security = pod.get("securityContext", {})
if pod_security.get("runAsNonRoot") is not True:
    raise SystemExit("preflight pod must set runAsNonRoot: true")
if pod_security.get("seccompProfile", {}).get("type") not in {"RuntimeDefault", "Localhost"}:
    raise SystemExit("preflight pod must select an allowed seccomp profile")

containers = pod.get("containers", [])
if len(containers) != 1:
    raise SystemExit(f"expected one preflight container, found {len(containers)}")
security = containers[0].get("securityContext", {})
if security.get("runAsNonRoot") is not True:
    raise SystemExit("preflight container must set runAsNonRoot: true")
if security.get("allowPrivilegeEscalation") is not False:
    raise SystemExit("preflight container must set allowPrivilegeEscalation: false")
if "ALL" not in security.get("capabilities", {}).get("drop", []):
    raise SystemExit("preflight container must drop ALL capabilities")
if security.get("seccompProfile", {}).get("type") not in {"RuntimeDefault", "Localhost"}:
    raise SystemExit("preflight container must select an allowed seccomp profile")
PY

set +e
missing_cidr_out="$(helm template "$RELEASE" "$CHART" --set mailAdapter.deploy=true "${CREDS[@]}" 2>&1)"
missing_cidr_rc=$?
set -e
[ "$missing_cidr_rc" -ne 0 ] \
  || fail "mailAdapter.deploy=true rendered with no AgentMail HTTPS CIDR; provider egress must fail closed"
case "$missing_cidr_out" in
  *"mailAdapter.agentmail.httpsCidrs"*) : ;;
  *) fail "missing-CIDR render failed without naming mailAdapter.agentmail.httpsCidrs; output was: $missing_cidr_out" ;;
esac

# ---------------------------------------------------------------------------
# 17: a BYO API is reachable only through an explicitly configured, narrow
#     NetworkPolicy peer. apiBaseUrl alone must never render a Ready pod whose
#     ingress remains pending forever because its API destination is denied.
# ---------------------------------------------------------------------------
assert_render_fails_named() {
  # $1 label, $2 expected configuration key in the error, remaining helm args.
  local label="$1" expected_key="$2" output rc
  shift 2
  set +e
  output="$(helm template "$RELEASE" "$CHART" "$@" 2>&1)"
  rc=$?
  set -e
  [ "$rc" -ne 0 ] \
    || fail "$label rendered successfully; this configuration must fail closed"
  case "$output" in
    *"$expected_key"*) : ;;
    *) fail "$label failed without naming '$expected_key'; output was: $output" ;;
  esac
}

assert_render_fails_named \
  "api.deploy=false without an API egress CIDR" \
  "mailAdapter.apiEgress.httpsCidrs" \
  "${ON[@]}" "${CREDS[@]}" \
  --set api.deploy=false \
  --set ui.deploy=false \
  --set mailAdapter.apiBaseUrl=https://byo-api.example:8443

byo_api_default_port_dir="$(render byo-api-default-port "${ON[@]}" "${CREDS[@]}" \
  --set api.deploy=false \
  --set ui.deploy=false \
  --set mailAdapter.apiBaseUrl=http://byo-api.example:8000 \
  --set 'mailAdapter.apiEgress.httpsCidrs[0]=198.51.100.128/25')"

python3 - "$byo_api_dir" "$byo_api_default_port_dir" <<'PY' \
  || fail "BYO API egress did not render exactly the configured narrow CIDR and TCP port"
import pathlib
import sys

import yaml

def assert_api_rule(rendered, expected_cidr, expected_port):
    policies = [
        doc
        for path in pathlib.Path(rendered).rglob("*.yaml")
        for doc in yaml.safe_load_all(path.read_text())
        if isinstance(doc, dict)
        and doc.get("kind") == "NetworkPolicy"
        and doc.get("metadata", {}).get("name") == "curie-mail-adapter-egress"
    ]
    if len(policies) != 1:
        raise SystemExit(f"expected one mail egress policy, found {len(policies)}")

    api_rules = []
    for rule in policies[0].get("spec", {}).get("egress", []):
        cidrs = {
            peer.get("ipBlock", {}).get("cidr")
            for peer in rule.get("to", [])
            if peer.get("ipBlock")
        }
        if expected_cidr in cidrs:
            api_rules.append((cidrs, rule.get("ports", [])))
    if len(api_rules) != 1:
        raise SystemExit(f"expected one BYO API CIDR rule, found {api_rules!r}")
    cidrs, ports = api_rules[0]
    if cidrs != {expected_cidr}:
        raise SystemExit(f"BYO API rule widened beyond the configured peer: {cidrs!r}")
    expected_ports = [{"protocol": "TCP", "port": expected_port}]
    if ports != expected_ports:
        raise SystemExit(
            f"BYO API rule did not preserve configured TCP port {expected_port}: {ports!r}"
        )


assert_api_rule(sys.argv[1], "198.51.100.0/24", 8443)
assert_api_rule(sys.argv[2], "198.51.100.128/25", 8000)
PY

# ---------------------------------------------------------------------------
# 18: equivalent default routes fail closed after CIDR parsing, not fragile
#     string comparison. The two /1 entries cover the same address family as
#     /0; whitespace and the expanded IPv6 zero address are equivalent /0s.
# ---------------------------------------------------------------------------
assert_render_fails_named \
  "IPv4 /1 split AgentMail route" \
  "mailAdapter.agentmail.httpsCidrs" \
  --set mailAdapter.deploy=true "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=0.0.0.0/1' \
  --set-string 'mailAdapter.agentmail.httpsCidrs[1]=128.0.0.0/1'
assert_render_fails_named \
  "IPv6 /1 split AgentMail route" \
  "mailAdapter.agentmail.httpsCidrs" \
  --set mailAdapter.deploy=true "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=::/1' \
  --set-string 'mailAdapter.agentmail.httpsCidrs[1]=8000::/1'
assert_render_fails_named \
  "whitespace-padded IPv4 default AgentMail route" \
  "mailAdapter.agentmail.httpsCidrs" \
  --set mailAdapter.deploy=true "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]= 0.0.0.0/0 '
assert_render_fails_named \
  "expanded IPv6 default AgentMail route" \
  "mailAdapter.agentmail.httpsCidrs" \
  --set mailAdapter.deploy=true "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=0:0:0:0:0:0:0:0/0'
assert_render_fails_named \
  "invalid AgentMail route" \
  "mailAdapter.agentmail.httpsCidrs" \
  --set mailAdapter.deploy=true "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=not-a-cidr'

# The BYO API list protects the same credential-bearing pod and must share the
# broad-route guard rather than becoming a second way to grant HTTPS everywhere.
assert_render_fails_named \
  "IPv4 /1 split BYO API route" \
  "mailAdapter.apiEgress.httpsCidrs" \
  "${ON[@]}" "${CREDS[@]}" \
  --set api.deploy=false \
  --set ui.deploy=false \
  --set mailAdapter.apiBaseUrl=https://byo-api.example:8443 \
  --set-string 'mailAdapter.apiEgress.httpsCidrs[0]=0.0.0.0/1' \
  --set-string 'mailAdapter.apiEgress.httpsCidrs[1]=128.0.0.0/1' \
  --set mailAdapter.apiEgress.port=8443

valid_cidrs_dir="$(render valid-provider-cidrs \
  --set mailAdapter.deploy=true "${CREDS[@]}" \
  --set-string 'mailAdapter.agentmail.httpsCidrs[0]=203.0.113.0/24' \
  --set-string 'mailAdapter.agentmail.httpsCidrs[1]=2001:db8::/48')"
python3 - "$valid_cidrs_dir" <<'PY' \
  || fail "valid narrow IPv4 and IPv6 AgentMail CIDRs did not render"
import pathlib
import sys

import yaml

cidrs = {
    peer.get("ipBlock", {}).get("cidr")
    for path in pathlib.Path(sys.argv[1]).rglob("*.yaml")
    for doc in yaml.safe_load_all(path.read_text())
    if isinstance(doc, dict) and doc.get("kind") == "NetworkPolicy"
    for rule in doc.get("spec", {}).get("egress", [])
    for peer in rule.get("to", [])
    if peer.get("ipBlock")
}
expected = {"203.0.113.0/24", "2001:db8::/48"}
if not expected.issubset(cidrs):
    raise SystemExit(f"missing valid provider CIDRs: expected={expected!r}, rendered={cidrs!r}")
PY

# ---------------------------------------------------------------------------
# 12: checksum/mail-adapter-credentials hashes the incoming values for
#     chart-managed credentials, because the live chart Secret still holds the
#     old data during an upgrade. External references instead hash live Secret
#     data and use a deterministic source-ref fallback when `helm template` has
#     no cluster to read.
# ---------------------------------------------------------------------------
ANNOTATION=spec.template.metadata.annotations.checksum/mail-adapter-credentials
base_sum="$(field "$on_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")" \
  || fail "the mail-adapter pod template carries no 'checksum/mail-adapter-credentials' annotation; rotating an expired token would leave the pod 401ing behind a green /healthz. It must be a POD TEMPLATE annotation, not a Deployment annotation, or it rolls nothing"
[ -n "$base_sum" ] \
  || fail "'checksum/mail-adapter-credentials' rendered empty"

repeat_dir="$(render checksum-repeat "${ON[@]}" "${CREDS[@]}")"
repeat_sum="$(field "$repeat_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$repeat_sum" = "$base_sum" ] \
  || fail "clusterless checksum fallback is not deterministic: identical renders produced '$base_sum' and '$repeat_sum'"

channel_rotation_dir="$(render checksum-channel-rotation "${ON[@]}" "${CREDS[@]}" \
  --set mailAdapter.channelToken=changed-channel-value)"
channel_rotation_sum="$(field "$channel_rotation_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$channel_rotation_sum" != "$base_sum" ] \
  || fail "rotating chart-managed mailAdapter.channelToken left the adapter checksum unchanged; an upgrade would keep the old environment value"

egress_rotation_dir="$(render checksum-egress-rotation "${ON[@]}" "${CREDS[@]}" \
  --set mailAdapter.egressSecret=changed-egress-value)"
egress_rotation_sum="$(field "$egress_rotation_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$egress_rotation_sum" != "$base_sum" ] \
  || fail "rotating chart-managed mailAdapter.egressSecret left the adapter checksum unchanged; an upgrade would keep the old environment value"

provider_rotation_dir="$(render checksum-provider-rotation "${ON[@]}" "${CREDS[@]}" \
  --set mailAdapter.agentmail.apiKey=changed-provider-value)"
provider_rotation_sum="$(field "$provider_rotation_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$provider_rotation_sum" != "$base_sum" ] \
  || fail "rotating chart-managed mailAdapter.agentmail.apiKey left the adapter checksum unchanged; an upgrade would keep the old environment value"

external_sum="$(field "$external_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$external_sum" != "$base_sum" ] \
  || fail "checksum fallback ignored the external Secret names/keys; changing the credential source would leave the pod on its prior secretKeyRefs"

external_raw_values_dir="$(render checksum-external-raw-values "${ON[@]}" "${EXTERNAL_REFS[@]}" \
  --set mailAdapter.channelToken=unused-channel-value \
  --set mailAdapter.egressSecret=unused-egress-value \
  --set mailAdapter.agentmail.apiKey=unused-provider-value)"
external_raw_values_sum="$(field "$external_raw_values_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$external_raw_values_sum" = "$external_sum" ] \
  || fail "external credential checksum changed with unused plain Helm values; existingSecret makes the referenced Secret the sole source"

external_source_dir="$(render checksum-external-source "${ON[@]}" "${CREDS[@]}" "${EXTERNAL_REFS[@]}" \
  --set mailAdapter.channelTokenExistingSecretKey=next-channel-token)"
external_source_sum="$(field "$external_source_dir" Deployment "$DEPLOY_NAME" "$ANNOTATION")"
[ "$external_source_sum" != "$external_sum" ] \
  || fail "external checksum fallback ignored a referenced key change; clusterless renders must track the source ref"

if ! grep -F 'lookup "v1" "Secret"' "$CHART/templates/mail-adapter.yaml" >/dev/null; then
  fail "mail-adapter checksum never looks up live Secret data; a Secret value rotated in place would not roll the adapter on the next Helm upgrade"
fi

# ---------------------------------------------------------------------------
# 13: the egress pair cannot diverge. (a) derived from mailAdapter.egressSecret
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
# 14: the worker's rotation trigger sees the DERIVED entry. Without this the
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
# 15: the default render is untouched. This is what keeps the coalescing helper
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
# 16: the build path. The chart defaults to an image tag no workflow publishes
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

# ---------------------------------------------------------------------------
# 21: the adapter carries the STANDARD OTLP env, and only when the shared
#     telemetry boundary says so. The two values below are the SAME pair
#     otel-collector-durability-assertions.sh pins for api/dispatcher/worker/
#     runner; asserting them here is what makes the adapter a fifth member of
#     that boundary rather than a workload with its own private wiring.
#     The set-wide gate that a sixth first-party workload cannot omit this env
#     is ci/instrumented-workload-assertions.sh (#2360).
# ---------------------------------------------------------------------------
assert_env_value "$on_dir" OTEL_EXPORTER_OTLP_ENDPOINT "http://curie-otel-collector:4318" \
  "The in-chart Collector Service must be derived exactly as it is for the other instrumented workloads; an adapter that exports nowhere leaves its records outside the shared, redacted OTLP path."
assert_env_value "$on_dir" OTEL_EXPORTER_OTLP_PROTOCOL "http/protobuf" \
  "The protocol must come from the shared helper default, not a per-workload literal."

# A chart-owned EXTERNAL endpoint must reach the adapter verbatim. A template
# that hardcoded the in-chart Service name would pass the assertion above and
# silently ignore the operator here.
otel_external_dir="$(render otel-external "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32')"
assert_env_value "$otel_external_dir" OTEL_EXPORTER_OTLP_ENDPOINT "https://otel.example.com:4318" \
  "otelCollector.endpoint must render verbatim on the adapter, exactly as it does on the other instrumented workloads."
assert_env_value "$otel_external_dir" OTEL_EXPORTER_OTLP_PROTOCOL "http/protobuf" \
  "The external-endpoint path must still carry the protocol; an endpoint with no protocol falls back to the SDK default and can disagree with the collector's receiver."

# The negative direction, and the one that proves membership of the shared
# boundary: with telemetry explicitly disabled the adapter must render NO
# OTEL_EXPORTER_OTLP_* env at all, which is what the other four do under the
# same values.
otel_disabled_dir="$(render otel-disabled "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.telemetryDisabled=true)"
# Non-vacuity floor. An absence check against a render that produced no
# mail-adapter container passes for the wrong reason; assert a known-present env
# first, then use the same rc -eq 3 guard assertion 5 uses.
assert_env_value "$otel_disabled_dir" CURIE_API_URL "http://curie-api:8000" \
  "The telemetry-disabled render must still be a real mail-adapter container, or the OTLP absence checks below pass vacuously."
for otel_name in OTEL_EXPORTER_OTLP_ENDPOINT OTEL_EXPORTER_OTLP_PROTOCOL OTEL_EXPORTER_OTLP_HEADERS; do
  set +e
  env_field "$otel_disabled_dir" "$DEPLOY_NAME" "$otel_name" name >/dev/null 2>&1
  rc=$?
  set -e
  if [ "$rc" -eq 3 ]; then
    fail "the telemetry-disabled render produced no mail-adapter container, so the '$otel_name' absence assertion would pass vacuously"
  fi
  if [ "$rc" -ne 1 ]; then
    fail "with otelCollector.telemetryDisabled=true the mail-adapter container still carries '$otel_name'. The adapter must sit inside the shared telemetry boundary: an operator who disabled telemetry for the release must not find one workload still exporting"
  fi
done

# ---------------------------------------------------------------------------
# 22: the collector egress peer exists exactly when otelCollector.deploy is
#     true. Read structurally, never by grep: this rule is a conditional entry
#     inside an existing list (Decision 3 keeps ONE policy object for this
#     credential-bearing pod), so a line reader cannot tell a present rule from
#     a reordered one, and an accidentally unconditional rule looks identical.
# ---------------------------------------------------------------------------
OTEL_EGRESS_PY="$TMP/mail-otel-egress.py"
cat > "$OTEL_EGRESS_PY" <<'PY'
"""Assert the mail adapter's collector egress peer in one rendered directory.

argv: <rendered-dir> <expect-collector-rule: yes|no> <expected-total-egress-rules>
      [<cidr-that-must-still-render>]
"""
import pathlib
import sys

import yaml

rendered, expect, expected_rules = sys.argv[1], sys.argv[2], int(sys.argv[3])
required_cidr = sys.argv[4] if len(sys.argv) > 4 else ""

# helm template with no -n renders into the `default` namespace, so that is the
# release namespace the peer must name. A rule that hardcoded another namespace
# would select nothing at runtime and drop every export.
NAMESPACE = "default"
EXPECTED_POD_SELECTOR = {
    "app.kubernetes.io/name": "curie",
    "app.kubernetes.io/instance": "curie",
    "app.kubernetes.io/component": "otel-collector",
}
EXPECTED_PORTS = [
    {"protocol": "TCP", "port": 4317},
    {"protocol": "TCP", "port": 4318},
]

docs = [
    doc
    for path in pathlib.Path(rendered).rglob("*.yaml")
    for doc in yaml.safe_load_all(path.read_text())
    if isinstance(doc, dict)
]

policies = [
    doc
    for doc in docs
    if doc.get("kind") == "NetworkPolicy"
    and doc.get("spec", {}).get("podSelector", {}).get("matchLabels", {}).get(
        "app.kubernetes.io/component"
    )
    == "mail-adapter"
]
if len(policies) != 1:
    raise SystemExit(
        f"expected exactly ONE NetworkPolicy selecting the mail adapter, found "
        f"{len(policies)} ({[p.get('metadata', {}).get('name') for p in policies]!r}). "
        "The collector peer is a conditional rule inside the existing egress "
        "policy, not a second policy object: an auditor must read one object to "
        "see everything this credential-bearing pod may reach."
    )
policy = policies[0]
egress = policy.get("spec", {}).get("egress") or []

collector_peers = []
for index, rule in enumerate(egress):
    for peer in rule.get("to") or []:
        pod_selector = peer.get("podSelector") or {}
        labels = pod_selector.get("matchLabels") or {}
        if labels.get("app.kubernetes.io/component") == "otel-collector":
            collector_peers.append((index, rule, peer))

if expect == "no":
    if collector_peers:
        raise SystemExit(
            "otelCollector.deploy is false but the mail adapter's egress policy "
            f"still carries a collector peer: {collector_peers[0][2]!r}. The rule "
            "must be gated on otelCollector.deploy, or the chart opens a peer for "
            "pods that do not exist."
        )
elif expect == "yes":
    if len(collector_peers) != 1:
        raise SystemExit(
            "otelCollector.deploy is true but the mail adapter's egress policy "
            f"carries {len(collector_peers)} collector peers, expected exactly 1. "
            "Without it the adapter's OTLP export is dropped by its own egress "
            "rail while the rendered env still looks correct. Rendered egress "
            f"rules: {egress!r}"
        )
    _, rule, peer = collector_peers[0]
    if (rule.get("to") or []).index(peer) != 0:
        raise SystemExit(f"collector peer is not to[0] of its rule: {rule!r}")
    namespace_selector = peer.get("namespaceSelector") or {}
    got_namespace = (namespace_selector.get("matchLabels") or {}).get(
        "kubernetes.io/metadata.name"
    )
    if got_namespace != NAMESPACE:
        raise SystemExit(
            f"collector peer namespaceSelector names {got_namespace!r}, expected "
            f"{NAMESPACE!r}. A podSelector with no namespaceSelector matches pods "
            "in the adapter's own namespace only by accident of this render, and a "
            "wrong namespace selects nothing at all."
        )
    got_labels = peer.get("podSelector", {}).get("matchLabels") or {}
    if got_labels != EXPECTED_POD_SELECTOR:
        raise SystemExit(
            f"collector peer podSelector is {got_labels!r}, expected "
            f"{EXPECTED_POD_SELECTOR!r} (curie.selectorLabels for the "
            "otel-collector component). A partial selector can match a different "
            "release's collector in the same namespace."
        )
    if rule.get("ports") != EXPECTED_PORTS:
        raise SystemExit(
            f"collector peer ports are {rule.get('ports')!r}, expected "
            f"{EXPECTED_PORTS!r} (otelCollector.service.grpcPort and .httpPort). "
            "The chart's own OTLP endpoint is the HTTP port, so omitting 4318 "
            "drops every export the default configuration makes."
        )
else:
    raise SystemExit(f"bad expect argument {expect!r}")

if len(egress) != expected_rules:
    raise SystemExit(
        f"the mail adapter's egress policy has {len(egress)} rules, expected "
        f"{expected_rules}. The rule set is DNS, the API peer, the AgentMail "
        "CIDRs, and -- only under otelCollector.deploy -- the collector. A count "
        f"drift means a destination was added or lost silently. Rules: {egress!r}"
    )

if required_cidr:
    cidrs = {
        peer.get("ipBlock", {}).get("cidr")
        for rule in egress
        for peer in rule.get("to") or []
        if peer.get("ipBlock")
    }
    if required_cidr not in cidrs:
        raise SystemExit(
            f"{required_cidr!r} no longer renders alongside the collector peer; "
            f"rendered CIDRs were {sorted(c for c in cidrs if c)!r}"
        )
PY

python3 "$OTEL_EGRESS_PY" "$on_dir" yes 4 203.0.113.0/24 \
  || fail "with the chart collector deployed, the mail adapter's egress policy does not admit it; see the message above"

# deploy=false: the rule must be GONE, and the policy must be back to exactly
# its three original destinations. A count assertion, so an accidentally
# unconditional rule fails here rather than rendering a peer for pods that were
# never deployed.
python3 "$OTEL_EGRESS_PY" "$otel_disabled_dir" no 3 203.0.113.0/24 \
  || fail "with the chart collector not deployed, the mail adapter's egress policy is not back to its three original rules; see the message above"
# Under the #2361 contract this render must ALSO declare its own OTLP peer, so
# the count is four: DNS, the API peer, AgentMail, and the declared external
# collector ipBlock. The intent of the assertion is unchanged -- the external
# collector must be reached as an operator-declared IP peer and must never be
# synthesized as an in-cluster podSelector peer for a Collector pod that
# otelCollector.deploy=false says does not exist.
python3 "$OTEL_EGRESS_PY" "$otel_external_dir" no 4 203.0.113.0/24 \
  || fail "an EXTERNAL otelCollector.endpoint must not synthesize an in-cluster collector peer; that destination is an IP the chart cannot know, and it must arrive as the declared mailAdapter.otelEgress ipBlock rule instead"

# The BYO-API render assertion 17 already exercises: the collector peer must
# coexist with the explicit BYO API CIDR rule rather than replacing it.
python3 "$OTEL_EGRESS_PY" "$byo_api_dir" yes 4 198.51.100.0/24 \
  || fail "with api.deploy=false the collector peer and the BYO API CIDR rule do not coexist; see the message above"

# ---------------------------------------------------------------------------
# 23: an EXTERNAL OTLP collector endpoint fails closed (#2361). The adapter is
#     the only first-party workload with an egress-restricting policy, and it
#     deliberately has no `mailAdapter.extraEnv`, so its effective OTLP endpoint
#     IS `curie.otel.endpoint`. When that endpoint is not this release's in-chart
#     Collector, the policy has no peer for it and every span, metric and log is
#     dropped by the pod's own rail while `helm template` stays green and the
#     rendered env still looks perfect -- exactly the silent-hole shape the
#     adjacent mailAdapter.apiEgress gate already refuses. These assertions pin
#     the reversal of that documented drop: refuse rather than document.
#
#     Read structurally, never by grep. The OTLP peer is an ipBlock rule in a
#     policy that ALREADY carries ipBlock rules (AgentMail on 443, and a BYO API
#     on its own port), so a line reader cannot tell which rule it is looking at
#     and would happily accept an OTLP "peer" that is really the AgentMail rule.
#
#     Every render below that pairs otelCollector.deploy=false with a non-empty
#     otelCollector.endpoint ALSO sets otelCollector.egress. That is not this
#     section's own gate: it is the pre-existing, unrelated #2317 runner-sandbox
#     refusal at templates/security-networkpolicy.yaml:386, which fires on that
#     exact combination and fails the render before the mail-adapter path under
#     test is ever reached. Without it these renders die on the runner's fail
#     closed message instead of exercising (or refusing for) the mail-adapter
#     reason this section asserts. Do not drop these flags as copy-paste noise.
# ---------------------------------------------------------------------------
OTLP_IPBLOCK_PY="$TMP/mail-otlp-ipblock.py"
cat > "$OTLP_IPBLOCK_PY" <<'PY'
"""Assert the mail adapter's EXTERNAL-OTLP ipBlock rule, structurally.

argv: <rendered-dir> <expect: rule|none> <expected-cidrs-csv|-> <expected-port|->
      <known-non-otlp-cidrs-csv>

The last argument is what makes this reader honest. The mail adapter's egress
policy legitimately carries other ipBlock rules (AgentMail on TCP 443, and, when
api.deploy is false, the BYO API peer on its configured port). Those are named
explicitly and excluded; whatever ipBlock rule is LEFT is the OTLP rule. That way
a template which "renders an OTLP peer" by accidentally widening the AgentMail
rule fails here instead of passing.
"""
import pathlib
import sys

import yaml

rendered, expect = sys.argv[1], sys.argv[2]
expected_cidrs = (
    {c for c in sys.argv[3].split(",") if c} if sys.argv[3] != "-" else set()
)
expected_port = int(sys.argv[4]) if sys.argv[4] != "-" else None
known_other = {c for c in sys.argv[5].split(",") if c} if len(sys.argv) > 5 else set()

policies = [
    doc
    for path in pathlib.Path(rendered).rglob("*.yaml")
    for doc in yaml.safe_load_all(path.read_text())
    if isinstance(doc, dict)
    and doc.get("kind") == "NetworkPolicy"
    and doc.get("spec", {}).get("podSelector", {}).get("matchLabels", {}).get(
        "app.kubernetes.io/component"
    )
    == "mail-adapter"
]
if len(policies) != 1:
    raise SystemExit(
        f"expected exactly ONE NetworkPolicy selecting the mail adapter, found "
        f"{len(policies)}. Decision 3 keeps a single policy object for this "
        "credential-bearing pod so an auditor reads one object to see every "
        "destination it may reach; a second object hides half the answer."
    )
egress = policies[0].get("spec", {}).get("egress") or []

ipblock_rules = []
for rule in egress:
    peers = rule.get("to") or []
    if not peers or not all(peer.get("ipBlock") for peer in peers):
        continue
    cidrs = {peer["ipBlock"].get("cidr") for peer in peers}
    ipblock_rules.append((cidrs, rule.get("ports") or []))

candidates = [(c, p) for c, p in ipblock_rules if not c <= known_other]

if expect == "none":
    if candidates:
        raise SystemExit(
            f"an OTLP ipBlock rule rendered where none may exist: {candidates!r} "
            f"(known non-OTLP CIDRs were {sorted(known_other)!r}). Either the "
            "in-chart Collector is deployed -- in which case the peer must be the "
            "pod-selector rule, not an IP the chart cannot know -- or telemetry is "
            "disabled, in which case the adapter exports nothing and must be "
            "granted nothing."
        )
elif expect == "rule":
    if len(candidates) != 1:
        raise SystemExit(
            f"expected exactly ONE external-OTLP ipBlock rule, found "
            f"{len(candidates)}: {candidates!r} (known non-OTLP CIDRs were "
            f"{sorted(known_other)!r}). Zero means the operator declared "
            "mailAdapter.otelEgress.httpsCidrs and the chart silently ignored it, "
            "leaving the export dropped; more than one means the rule set is "
            "widening past the single declared destination."
        )
    cidrs, ports = candidates[0]
    if cidrs != expected_cidrs:
        raise SystemExit(
            f"the external-OTLP rule reaches {sorted(cidrs)!r}, expected exactly "
            f"{sorted(expected_cidrs)!r}. A peer that is broader than what the "
            "operator declared is a new egress escape hatch on the pod that holds "
            "the channel token, the egress secret and the AgentMail API key."
        )
    want_ports = [{"protocol": "TCP", "port": expected_port}]
    if ports != want_ports:
        raise SystemExit(
            f"the external-OTLP rule opens {ports!r}, expected {want_ports!r}. The "
            "port is derived from the resolved endpoint URL (explicit port, else "
            "443 for https and 80 for http); getting it wrong renders a peer that "
            "looks configured and still drops every export."
        )
else:
    raise SystemExit(f"bad expect argument {expect!r}")
PY

# Zero-policy reader for the adapter-off case. `render` already proves the exit
# code; this proves the render is NOT empty, so "no mail-adapter policy" cannot
# pass because nothing rendered at all.
NO_MAIL_POLICY_PY="$TMP/mail-no-policy.py"
cat > "$NO_MAIL_POLICY_PY" <<'PY'
"""Assert zero mail-adapter NetworkPolicies in a NON-EMPTY render.

argv: <rendered-dir>
"""
import pathlib
import sys

import yaml

docs = [
    doc
    for path in pathlib.Path(sys.argv[1]).rglob("*.yaml")
    for doc in yaml.safe_load_all(path.read_text())
    if isinstance(doc, dict)
]
if len(docs) < 5:
    raise SystemExit(
        f"the render produced only {len(docs)} object(s); the absence assertion "
        "below would pass because nothing rendered, not because the gate is "
        "correctly scoped to mailAdapter.deploy."
    )
mail = [
    doc
    for doc in docs
    if doc.get("kind") == "NetworkPolicy"
    and doc.get("spec", {}).get("podSelector", {}).get("matchLabels", {}).get(
        "app.kubernetes.io/component"
    )
    == "mail-adapter"
]
if mail:
    raise SystemExit(
        f"mailAdapter.deploy is false but {len(mail)} mail-adapter NetworkPolicy "
        "object(s) rendered. The OTLP gate must live entirely inside the adapter's "
        "own conditional: an operator who never asked for the mail adapter must "
        "never be asked for its collector CIDRs, and must never receive a policy "
        "selecting pods that do not exist."
    )
PY

# Two-substring variant of assert_render_fails_named. The port-mismatch and
# endpoint-naming failures are only useful if the message names BOTH halves of
# the contradiction; a message that says "port mismatch" without printing the
# two ports sends the operator back to the values file to guess.
assert_render_fails_naming_both() {
  # $1 label, $2 first required substring, $3 second required substring,
  # remaining helm args.
  local label="$1" first="$2" second="$3" output rc
  shift 3
  set +e
  output="$(helm template "$RELEASE" "$CHART" "$@" 2>&1)"
  rc=$?
  set -e
  [ "$rc" -ne 0 ] \
    || fail "$label rendered successfully; this configuration must fail closed"
  case "$output" in
    *"$first"*) : ;;
    *) fail "$label failed without naming '$first'; an operator cannot act on a message that omits it. Output was: $output" ;;
  esac
  case "$output" in
    *"$second"*) : ;;
    *) fail "$label failed without naming '$second'; an operator cannot act on a message that omits it. Output was: $output" ;;
  esac
}

# 23a: the adapter is off. No policy, no demand for CIDRs, and a real render
#      behind the absence check.
otlp_adapter_off_dir="$(render otlp-adapter-off \
  --set mailAdapter.deploy=false \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318')"
python3 "$NO_MAIL_POLICY_PY" "$otlp_adapter_off_dir" \
  || fail "with mailAdapter.deploy=false an external collector endpoint must render cleanly and produce no mail-adapter policy; see the message above"

# 23b: the default, in-chart collector path is UNCHANGED. Assertion 22 above
#      already pinned the pod-selector peer and the rule count; what is added
#      here is that the chart did not ALSO invent an ipBlock for a collector it
#      can select by label. Two peers for one destination is a widening.
python3 "$OTLP_IPBLOCK_PY" "$on_dir" none - - 203.0.113.0/24 \
  || fail "the default in-chart collector render grew an OTLP ipBlock peer; the in-chart Collector is selectable by pod label and must never be reached by a hardcoded address as well"

# 23c: the key is read ONLY when the endpoint is external. A stale
#      mailAdapter.otelEgress.httpsCidrs left behind after an operator moved
#      back to the in-chart collector must be surfaced, not silently ignored --
#      the same refusal #2367 applies to the runner's override keys. Silently
#      ignored config is how an operator comes to believe a peer exists.
assert_render_fails_named \
  "in-chart collector WITH mailAdapter.otelEgress.httpsCidrs set" \
  "mailAdapter.otelEgress.httpsCidrs" \
  "${ON[@]}" "${CREDS[@]}" \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32'

# 23d: the headline gate. An external endpoint with no declared peer must refuse
#      and must name the RESOLVED endpoint, so the operator learns which address
#      needs a CIDR rather than being told a key is missing in the abstract.
assert_render_fails_naming_both \
  "external otelCollector.endpoint with no mailAdapter.otelEgress.httpsCidrs" \
  "mailAdapter.otelEgress.httpsCidrs" \
  "https://otel.example.com:4318" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318'

# 23e: the positive path. Four rules, an ipBlock peer on the port derived from
#      the URL, NO synthesized in-cluster collector peer (the chart cannot know
#      that pod exists), and the AgentMail rule still intact beside it.
otlp_external_ok_dir="$(render otlp-external-ok "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32')"
python3 "$OTEL_EGRESS_PY" "$otlp_external_ok_dir" no 4 203.0.113.0/24 \
  || fail "a declared external OTLP peer must add a FOURTH rule without synthesizing an in-cluster collector podSelector peer, and must not displace the AgentMail rule; see the message above"
python3 "$OTLP_IPBLOCK_PY" "$otlp_external_ok_dir" rule 192.0.2.40/32 4318 203.0.113.0/24 \
  || fail "the declared external OTLP peer did not render as a narrow ipBlock rule on the endpoint's port; see the message above"

# 23f: overbroad peers are refused by the SAME narrow-CIDR guard the AgentMail
#      and BYO API lists use. A /0 or a /1 half of the address space on this pod
#      is HTTPS-everywhere for a workload holding three live credentials, and
#      "it is only for telemetry" is not a smaller blast radius.
assert_render_fails_named \
  "default-route OTLP peer" \
  "mailAdapter.otelEgress.httpsCidrs" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string 'mailAdapter.otelEgress.httpsCidrs[0]=0.0.0.0/0'
assert_render_fails_named \
  "/1 half-of-the-internet OTLP peer" \
  "mailAdapter.otelEgress.httpsCidrs" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string 'mailAdapter.otelEgress.httpsCidrs[0]=128.0.0.0/1'

# 23g: an explicit port that CONTRADICTS the endpoint URL must refuse rather
#      than pick a winner. Whichever the chart chose silently, one of the two
#      operator statements would be quietly discarded -- and if the URL loses,
#      the rendered policy opens a port the collector is not listening on.
assert_render_fails_naming_both \
  "mailAdapter.otelEgress.port disagreeing with the endpoint URL port" \
  "4318" \
  "443" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string mailAdapter.otelEgress.port=443 \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32'

# 23h: an explicit port that AGREES is not a contradiction. The refusal above
#      must be a mismatch check, not a ban on ever setting the key.
otlp_port_agree_dir="$(render otlp-port-agree "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string mailAdapter.otelEgress.port=4318 \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32')"
python3 "$OTLP_IPBLOCK_PY" "$otlp_port_agree_dir" rule 192.0.2.40/32 4318 203.0.113.0/24 \
  || fail "an explicit mailAdapter.otelEgress.port that AGREES with the endpoint URL must render that port; see the message above"

# 23i: no explicit port anywhere. The scheme default (443 for https) is the only
#      port the collector can be on, and guessing 4318 there renders a peer that
#      drops every export.
#
#      The otelCollector.egress port below (4318) is the UNRELATED runner-sandbox
#      peer required by the #2317 gate, not the mail adapter's derived port --
#      the mail adapter's own peer is asserted separately below to be 443 (the
#      https scheme default). Do not read this 4318 as this assertion's target.
otlp_scheme_port_dir="$(render otlp-scheme-port "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32')"
python3 "$OTLP_IPBLOCK_PY" "$otlp_scheme_port_dir" rule 192.0.2.40/32 443 203.0.113.0/24 \
  || fail "an endpoint with no explicit port must derive the scheme default (443 for https); see the message above"

# 23j: the leftover-Service-DNS shape. `otelCollector.deploy=false` with an
#      endpoint that still names this release's collector Service is the most
#      dangerous case, because the host LOOKS in-chart while no such pod exists.
#      The gate must key off the EFFECTIVE endpoint plus deploy, not the
#      hostname, exactly as #2367 does for the runner.
assert_render_fails_named \
  "in-chart Service DNS left behind with otelCollector.deploy=false" \
  "mailAdapter.otelEgress.httpsCidrs" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=http://curie-otel-collector:4318 \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318'

# 23k: the documented escape hatch. telemetryDisabled resolves the endpoint to
#      empty, so there is nothing to reach and nothing to declare -- three rules
#      and no ipBlock beyond AgentMail. This is the answer the failure message
#      offers an operator who genuinely wants an unobservable adapter, so it has
#      to keep working.
python3 "$OTEL_EGRESS_PY" "$otel_disabled_dir" no 3 203.0.113.0/24 \
  || fail "otelCollector.telemetryDisabled=true must remain a complete escape from the OTLP peer requirement, at three rules; see the message above"
python3 "$OTLP_IPBLOCK_PY" "$otel_disabled_dir" none - - 203.0.113.0/24 \
  || fail "with telemetry disabled the adapter exports nothing, so it must be granted no OTLP ipBlock peer at all; see the message above"

# 23l: a bracketed IPv6 endpoint WITH a port. Port extraction used to be
#      skipped whenever the host contained "[", which left the URL port unseen,
#      fell through to the https scheme default, and opened 443 while the SDK
#      dialled 4318 -- a peer that looks configured and drops every export. The
#      rule must be on 4318.
otlp_v6_port_dir="$(render otlp-v6-port "${ON[@]}" "${CREDS[@]}" \
  --set-string 'otelCollector.endpoint=https://[2001:db8::1]:4318' \
  --set otelCollector.deploy=false \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string 'mailAdapter.otelEgress.httpsCidrs[0]=2001:db8::1/128')"
python3 "$OTLP_IPBLOCK_PY" "$otlp_v6_port_dir" rule 2001:db8::1/128 4318 203.0.113.0/24 \
  || fail "a bracketed IPv6 OTLP endpoint that names a port must open THAT port, not the https scheme default; see the message above"

# 23m: the same bracketed IPv6 endpoint with a DISAGREEING explicit port. The
#      bracket skip also disabled the mismatch refusal, so this shape used to
#      render silently. It must refuse, exactly as 23g does for a DNS host.
assert_render_fails_naming_both \
  "bracketed IPv6 endpoint port disagreeing with mailAdapter.otelEgress.port" \
  "4318" \
  "443" \
  "${ON[@]}" "${CREDS[@]}" \
  --set-string 'otelCollector.endpoint=https://[2001:db8::1]:4318' \
  --set otelCollector.deploy=false \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string mailAdapter.otelEgress.port=443 \
  --set-string 'mailAdapter.otelEgress.httpsCidrs[0]=2001:db8::1/128'

# 23n: a URL that omits its port still has a determined dial port. The mismatch
#      check used to compare the explicit key against a ":port" SUBSTRING, so a
#      portless https:// endpoint plus port=4318 passed unchallenged and opened
#      4318 while the exporter connected to 443. Compare against the resolved
#      dial port instead, and name both numbers.
assert_render_fails_naming_both \
  "explicit port against a portless https endpoint dialled on its scheme default" \
  "4318" \
  "443" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.deploy=false \
  --set otelCollector.endpoint=https://otel.example.com \
  --set 'otelCollector.egress[0].cidr=192.0.2.40/32' \
  --set 'otelCollector.egress[0].ports[0].protocol=TCP' \
  --set 'otelCollector.egress[0].ports[0].port=4318' \
  --set-string mailAdapter.otelEgress.port=4318 \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32'

# 23o: a stale CIDR left behind on a release that exports NOTHING. Both the
#      in-chart and the disabled release are "not external", but only one of
#      them has a pod-selector rule; curie.otel.validate forbids
#      telemetryDisabled with deploy=true, so quoting that rule here names a
#      rule that was never rendered. The remedy must say nothing is exported.
assert_render_fails_named \
  "telemetryDisabled with a stale mailAdapter.otelEgress.httpsCidrs" \
  "exports nothing at all" \
  "${ON[@]}" "${CREDS[@]}" \
  --set otelCollector.telemetryDisabled=true \
  --set otelCollector.deploy=false \
  --set 'mailAdapter.otelEgress.httpsCidrs[0]=192.0.2.40/32'

echo "OK: mail-adapter chart wiring and build-path render assertions passed (27 assertions)"
