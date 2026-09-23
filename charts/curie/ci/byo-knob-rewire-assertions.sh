#!/usr/bin/env bash
#
# Render assertions for the three BYO Secret knobs an External Secrets sync
# feeds (ADR 0163): installation.idExistingSecret,
# api.githubWebhookSecretExistingSecret, and
# agentSandbox.connectorExistingSecrets.
#
#   (a) Each knob rewires EVERY consumer secretKeyRef of its key to the BYO
#       Secret, and the chart Secret stops carrying that key.
#   (b) With every knob unset the consumers stay on the chart-managed Secret,
#       and the chart Secret still carries both keys.
#   (c) A BYO installation id the render cannot read (client-only upgrade)
#       leaves the drain hook fenced: it is told the identity is unobserved,
#       and a render with the knob keys absent (retained values) still works.
#       With the knob set, both drain hooks read CURIE_INSTALLATION_ID
#       through secretKeyRef, never a literal; without it the literal stays.
#       The lookup-dependent paths (identity read back, switch refusals, the
#       legacy bridge) need a cluster and are proven outside this script.
#   (d) Connector BYO validation fails closed: reserved key names, a missing
#       Secret name or key list, and an agent named in both maps.
#
# Optional: BASELINE_REF=<git ref> also renders the chart at that ref and
# requires the default render to equal it once the sealed-install random fields
# (installationId and the generated chart credentials) are normalized.
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

render() { helm template t "$CHART" -n t "$@"; }

python_check() {
    python3 - "$@" <<'PY'
import sys, yaml

mode, path = sys.argv[1], sys.argv[2]
docs = [d for d in yaml.safe_load_all(open(path)) if d]
failed = False

def fail(msg):
    global failed
    print(f"FAIL: {msg}", file=sys.stderr)
    failed = True

def refs(key_names):
    """Every secretKeyRef, anywhere in the render, whose env var consumes one of key_names."""
    out = []
    def walk(node, doc):
        if isinstance(node, dict):
            if "valueFrom" in node and isinstance(node["valueFrom"], dict):
                ref = node["valueFrom"].get("secretKeyRef")
                if ref and node.get("name") in key_names:
                    out.append((f"{doc.get('kind')}/{doc['metadata']['name']}", node["name"], ref))
            for v in node.values():
                walk(v, doc)
        elif isinstance(node, list):
            for v in node:
                walk(v, doc)
    for d in docs:
        walk(d, d)
    return out

def chart_secret():
    for d in docs:
        if d.get("kind") == "Secret" and d["metadata"]["name"] == "t-curie-secrets":
            return d.get("stringData") or {}
    fail("chart Secret t-curie-secrets did not render")
    return {}

def expect(env, want_name, want_key, minimum=1):
    found = refs({env})
    if len(found) < minimum:
        fail(f"{env}: expected at least {minimum} consumer, found {len(found)}")
    for where, _, ref in found:
        if ref.get("name") != want_name or ref.get("key") != want_key:
            fail(f"{env} in {where} reads {ref.get('name')}/{ref.get('key')}, expected {want_name}/{want_key}")
    return len(found)

if mode == "default":
    expect("GITHUB_WEBHOOK_SECRET", "t-curie-secrets", "githubWebhookSecret")
    expect("CURIE_INSTALLATION_ID", "t-curie-secrets", "installationId")
    data = chart_secret()
    for k in ("installationId", "githubWebhookSecret"):
        if not str(data.get(k, "")).strip():
            fail(f"default chart Secret lost {k}")
elif mode == "byo":
    n = expect("GITHUB_WEBHOOK_SECRET", "sm-webhook", "hmac")
    m = expect("CURIE_INSTALLATION_ID", "sm-identity", "id")
    data = chart_secret()
    # Provenance a later upgrade reads: without it a BYO release whose first
    # render could not see its Secret looks like a pre-installationId release.
    ann = next(d for d in docs if d.get("kind") == "Secret" and d["metadata"]["name"] == "t-curie-secrets")["metadata"].get("annotations") or {}
    if ann.get("curietech.ai/installation-id-source") != "byo":
        fail(f"chart Secret does not record the BYO identity source: {ann}")
    for k in ("installationId", "githubWebhookSecret"):
        if k in data:
            fail(f"chart Secret still carries {k} with its BYO knob set")
    # The per-agent template reads the BYO Secret for every listed key, and no
    # chart connector Secret renders for that agent.
    tmpl = [d for d in docs if d.get("kind") == "SandboxTemplate" and d["metadata"]["name"] == "t-curie-agent-acme-a-runner"]
    if len(tmpl) != 1:
        fail("BYO agent acme-a has no SandboxTemplate")
    got = {}
    for where, env, ref in refs({"TOKEN_A", "TOKEN_B"}):
        got[env] = (where, ref)
    for env in ("TOKEN_A", "TOKEN_B"):
        if env not in got:
            fail(f"{env} is not delivered to the acme-a sandbox")
            continue
        where, ref = got[env]
        if where != "SandboxTemplate/t-curie-agent-acme-a-runner" or ref.get("name") != "sm-acme-a" or ref.get("key") != env or ref.get("optional") is not False:
            fail(f"{env} in {where} reads {ref}, expected sm-acme-a/{env} optional false")
    if any(d.get("kind") == "Secret" and "acme-a" in d["metadata"]["name"] for d in docs):
        fail("a chart connector Secret rendered for a BYO agent")
    if not any(d.get("kind") == "SandboxWarmPool" and d["metadata"]["name"] == "t-curie-agent-acme-a-runner-pool" for d in docs):
        fail("BYO agent acme-a has no SandboxWarmPool")
    # The chart-valued agent next to it keeps its own chart Secret, template and pool.
    expect("TOKEN_C", "t-curie-agent-acme-b-connector-secrets", "TOKEN_C")
    for kind, name in (("Secret", "t-curie-agent-acme-b-connector-secrets"),
                       ("SandboxTemplate", "t-curie-agent-acme-b-runner"),
                       ("SandboxWarmPool", "t-curie-agent-acme-b-runner-pool")):
        if not any(d.get("kind") == kind and d["metadata"]["name"] == name for d in docs):
            fail(f"chart-valued agent acme-b lost its {kind}/{name}")
    print(f"ok: {n} webhook, {m} identity and 2 connector consumer refs rewired")
elif mode == "unobserved":
    hooks = [d for d in docs if d.get("kind") == "Job" and d["metadata"]["name"].endswith("-upgrade-drain")]
    if not hooks:
        fail("client-only upgrade rendered no drain hook")
    for h in hooks:
        for c in h["spec"]["template"]["spec"]["containers"]:
            if "--installation-id-observed=false" not in c.get("command", []):
                fail(f"{h['metadata']['name']} is not told the BYO identity is unobserved: {c.get('command')}")
elif mode in ("drain-byo", "drain-literal"):
    # Both upgrade-drain hook Jobs. A BYO (provider-synced) identity must reach
    # them by reference: a literal would land in every stored upgrade revision.
    hooks = [d for d in docs if d.get("kind") == "Job" and "upgrade-drain" in d["metadata"]["name"]]
    if len(hooks) != 2:
        fail(f"expected both upgrade-drain Jobs, found {[h['metadata']['name'] for h in hooks]}")
    for h in hooks:
        envs = [e for c in h["spec"]["template"]["spec"]["containers"] for e in c.get("env", []) if e.get("name") == "CURIE_INSTALLATION_ID"]
        if len(envs) != 1:
            fail(f"{h['metadata']['name']} carries {len(envs)} CURIE_INSTALLATION_ID entries")
            continue
        env = envs[0]
        if mode == "drain-byo":
            ref = (env.get("valueFrom") or {}).get("secretKeyRef") or {}
            if "value" in env:
                fail(f"{h['metadata']['name']} inlines the BYO installation id as a literal value")
            if ref.get("name") != "byo-installation" or ref.get("key") != "installationId":
                fail(f"{h['metadata']['name']} CURIE_INSTALLATION_ID reads {ref}, expected byo-installation/installationId")
        else:
            if "valueFrom" in env or not str(env.get("value", "")).strip():
                fail(f"{h['metadata']['name']} CURIE_INSTALLATION_ID is not a literal value without the knob: {env}")
sys.exit(1 if failed else 0)
PY
}

FAILED=0
fail() { echo "FAIL: $1" >&2; FAILED=1; }

# -- (b) default ---------------------------------------------------------------
render > "$TMP/default.yaml"
python_check default "$TMP/default.yaml" && echo "ok: unset knobs keep every consumer on the chart Secret" || FAILED=1

# -- (a) every knob set --------------------------------------------------------
BYO_ARGS=(
    --set-string installation.idExistingSecret=sm-identity
    --set-string installation.idExistingSecretKey=id
    --set-string api.githubWebhookSecretExistingSecret=sm-webhook
    --set-string api.githubWebhookSecretExistingSecretKey=hmac
    --set-string agentSandbox.connectorExistingSecrets.acme-a.existingSecret=sm-acme-a
    --set 'agentSandbox.connectorExistingSecrets.acme-a.keys={TOKEN_A,TOKEN_B}'
    --set-string agentSandbox.connectorSecrets.acme-b.TOKEN_C=chart-valued
)
render "${BYO_ARGS[@]}" > "$TMP/byo.yaml"
python_check byo "$TMP/byo.yaml" || FAILED=1

# -- (c) unreadable BYO identity stays fenced ----------------------------------
render --is-upgrade --set-string installation.idExistingSecret=sm-identity > "$TMP/unobserved.yaml"
python_check unobserved "$TMP/unobserved.yaml" && echo "ok: an unreadable BYO identity leaves the drain hook fenced" || FAILED=1

# -- (c1) the drain hooks read a BYO identity by reference --------------------
render --is-upgrade --set-string installation.idExistingSecret=byo-installation > "$TMP/drain-byo.yaml"
python_check drain-byo "$TMP/drain-byo.yaml" && echo "ok: both drain hooks read a BYO installation id through secretKeyRef" || FAILED=1
render --is-upgrade > "$TMP/drain-literal.yaml"
python_check drain-literal "$TMP/drain-literal.yaml" && echo "ok: without the knob the drain hooks keep the literal installation id" || FAILED=1

# -- (c2) retained values that predate the knobs -----------------------------
#
# `helm upgrade --reuse-values` replays values stored before these keys
# existed, and a nulled key is deleted by coalescing. Either way the render must
# fall back to the chart-managed Secret instead of dereferencing a nil map.
render --set installation=null --set api.githubWebhookSecretExistingSecret=null \
    --set api.githubWebhookSecretExistingSecretKey=null \
    --set agentSandbox.connectorExistingSecrets=null > "$TMP/retained.yaml" \
    || fail "a render with the knob keys absent failed"
python_check default "$TMP/retained.yaml" && echo "ok: absent knob keys keep every consumer on the chart Secret" || FAILED=1

# -- (d) connector BYO validation fails closed ---------------------------------
must_fail() {
    local label="$1" pattern="$2"; shift 2
    local out
    if out="$(render "$@" 2>&1)"; then
        fail "$label rendered instead of failing"
    elif ! grep -q -- "$pattern" <<<"$out"; then
        fail "$label failed without naming the cause: $(tail -1 <<<"$out")"
    else
        echo "ok: $label is refused"
    fi
}
must_fail "a reserved BYO connector key" "reserved" \
    --set-string agentSandbox.connectorExistingSecrets.demo.existingSecret=s \
    --set 'agentSandbox.connectorExistingSecrets.demo.keys={ANTHROPIC_API_KEY}'
must_fail "a BYO connector without a Secret name" "existingSecret is required" \
    --set 'agentSandbox.connectorExistingSecrets.demo.keys={TOKEN_A}'
must_fail "a BYO connector without keys" "keys must list" \
    --set-string agentSandbox.connectorExistingSecrets.demo.existingSecret=s
must_fail "an agent in both connector maps" "in both" \
    --set-string agentSandbox.connectorExistingSecrets.demo.existingSecret=s \
    --set 'agentSandbox.connectorExistingSecrets.demo.keys={TOKEN_A}' \
    --set-string agentSandbox.connectorSecrets.demo.TOKEN_A=x
# Paired control: a legitimate BYO connector renders.
render --set-string agentSandbox.connectorExistingSecrets.demo.existingSecret=s \
    --set 'agentSandbox.connectorExistingSecrets.demo.keys={TOKEN_A}' >/dev/null \
    || fail "a legitimate BYO connector failed to render"

# -- optional baseline equality ------------------------------------------------
if [[ -n "${BASELINE_REF:-}" ]]; then
    ROOT="$(git -C "$CHART" rev-parse --show-toplevel)"
    mkdir -p "$TMP/base"
    git -C "$ROOT" archive "$BASELINE_REF" charts/curie | tar -x -C "$TMP/base"
    normalize() {
        sed -E 's/^(  (installationId|postgresPassword|valkeyPassword|clickhousePassword|rustfsSecretKey|langfuseSalt|langfuseEncryptionKey|langfuseNextauthSecret|langfuseInitProjectSecretKey|langfuseInitUserPassword|otlpAuthHeader|apiKey|approvalChatAttesterSecret|internalWorkerToken|githubWebhookSecret)): ".*"$/\1: "<generated>"/' |
            awk '/- name: CURIE_INSTALLATION_ID$/ { print; if ((getline nxt) > 0) { sub(/value: ".*"$/, "value: \"<generated>\"", nxt); print nxt }; next } { print }'
    }
    for extra in "" "--set-string agentSandbox.connectorSecrets.acme-a.TOKEN_A=x"; do
        # shellcheck disable=SC2086
        helm template t "$TMP/base/charts/curie" -n t $extra | normalize > "$TMP/base.yaml"
        # shellcheck disable=SC2086
        render $extra | normalize > "$TMP/head.yaml"
        if diff -u "$TMP/base.yaml" "$TMP/head.yaml"; then
            echo "ok: render [${extra:-default}] equals $BASELINE_REF after normalizing generated fields"
        else
            fail "render [${extra:-default}] differs from $BASELINE_REF"
        fi
    done
fi

if ((FAILED)); then
    echo "BYO knob rewire assertions FAILED" >&2
    exit 1
fi
echo "BYO knob rewire assertions passed"
