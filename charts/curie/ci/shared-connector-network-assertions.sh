#!/usr/bin/env bash
#
# Render assertions for the root network policies of shared connector
# applications (ADR 0239, cp5, AC5).
#
# A shared application Pod carries app.kubernetes.io/part-of=<release>,
# app.kubernetes.io/component=shared-connector-application and
# curietech.ai/shared-application=<uuid> (the frozen render). The frozen render
# ships its own deny-ingress and database-egress policies; the root (this
# chart) supplies the rest, all behind one switch, sharedConnectors.enabled
# (default false).
#
# Asserts:
#
#   1. Off (default, explicit false, and the --is-upgrade render): no data-tier
#      peer and no policy names the shared component, no shared policy renders,
#      and the explicit-false render equals the default render byte for byte.
#   2. On: the Postgres and Valkey data-tier policies each gain exactly one
#      ingress rule whose only peer is the shared selector (part-of this
#      release, component shared-connector-application, and an Exists
#      expression on curietech.ai/shared-application; no namespaceSelector,
#      never mcp-connector), on that store's port only. RustFS and ClickHouse
#      gain nothing. Removing those two rules gives back the off render's data
#      tier exactly (the change is additive only), and the pre-existing
#      mcp-connector Valkey peer is untouched.
#   3. On: <fullname>-shared-app-allow-dns, <fullname>-shared-app-allow-grant-store
#      and one <fullname>-shared-<uuid>-allow-egress per applicationEgress key
#      render, Egress only, each selecting the shared selector (the per
#      application one adds curietech.ai/shared-application=<uuid>). DNS egress
#      equals the runner's DNS egress; grant-store egress is the in-chart Valkey
#      pods on 6379, or the grantStoreEgress ipBlocks with valkey.deploy=false;
#      application egress is exactly the configured entries. Nothing else
#      renders from the template.
#   3b. On: <fullname>-shared-app-deny-ingress renders (review r2), selecting
#      exactly the shared selector, policyTypes [Ingress], no ingress rule and
#      no egress, labelled component shared-connector-root so no worker
#      selector matches it. Absent with shared off (assertion 1). Negative
#      controls: an added ingress rule, an Egress policy type, a selector
#      without the Exists expression, the application component label, and
#      the policy missing are each caught.
#   4. On with the data tier off: the shared root policies still render and no
#      data-tier policy does.
#   5. Refused at render, naming sharedConnectors: a non-UUID or uppercase
#      applicationEgress key; entries below the egress floor (default route,
#      shorter than /8, cloud metadata, no ports, no cidr); valkey.deploy=false
#      with no grantStoreEgress; a non-boolean enabled. Each refusal has a
#      passing twin that differs only in the refused value.
#   6. --reuse-values: a chart copy whose values.yaml has no sharedConnectors
#      key renders with the data tier on and produces no shared object.
#      Negative control: the same copy plus a template that dereferences
#      .Values.sharedConnectors.enabled directly must fail, so the copy really
#      lacks the key.
#   7. Shared mode prerequisites: the API container carries
#      CURIE_INSTALLATION_ID_OBSERVED exactly once, equal to the observation the
#      upgrade drain hook is handed ("true" on install, "false" on a
#      client-only --is-upgrade render, which is the control that the env
#      follows the helper), and no Role or ClusterRole bound to the API grants
#      secrets.
#
# Every render layers values-dev.yaml first so generated credentials and the
# installation ID are deterministic and two renders can be compared byte for
# byte. Render to a directory and read the written files; a piped
# `helm template` has been observed to truncate silently while still exiting 0.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT

RELEASE=scn
NAMESPACE=scn-ns
FULLNAME=scn-curie
APP_A=4f0d6c2e-7a1b-4c3d-9e8f-0a1b2c3d4e5f
APP_B=9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

command -v helm >/dev/null || fail "helm is required"
python3 -c 'import yaml' 2>/dev/null || fail "python3 with PyYAML is required"

render_dir() {
  local name="$1" chart="$2"
  shift 2
  local out="$TMP/$name"
  mkdir -p "$out"
  helm template "$RELEASE" "$chart" --namespace "$NAMESPACE" -f "$chart/values-dev.yaml" \
    --output-dir "$out" "$@" >/dev/null
  printf '%s\n' "$out"
}

values_file() {
  local file="$TMP/values-$1.yaml"
  cat >"$file"
  printf '%s\n' "$file"
}

# must_fail_naming LABEL NEEDLE HELM_ARGS...: the render must fail, and its
# error must name NEEDLE, so a render failing for an unrelated reason cannot
# pass as the refusal under test.
must_fail_naming() {
  local label="$1" needle="$2"
  shift 2
  local slug="${label//[^A-Za-z0-9]/_}"
  local out="$TMP/fail-$slug"
  mkdir -p "$out"
  if helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" -f "$CHART/values-dev.yaml" \
    --output-dir "$out" "$@" >/dev/null 2>"$out.err"; then
    fail "${label}: render must fail"
  fi
  grep -qF -- "$needle" "$out.err" \
    || fail "${label}: render failed without naming ${needle}: $(cat "$out.err")"
  echo "  ok: ${label} is refused at render, naming ${needle}"
}

must_render() {
  local label="$1"
  shift
  local slug="${label//[^A-Za-z0-9]/_}"
  local out="$TMP/ok-$slug"
  mkdir -p "$out"
  helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" -f "$CHART/values-dev.yaml" \
    --output-dir "$out" "$@" >/dev/null 2>"$out.err" \
    || fail "${label}: the passing twin must render: $(cat "$out.err")"
}

# A sharedConnectors values file: enabled, data tier on, and the applicationEgress
# map given as YAML on stdin (indented two spaces under applicationEgress).
shared_values() {
  local name="$1" enabled="$2" extra="${3:-}"
  values_file "$name" <<EOF
security:
  dataTierNetworkPolicy:
    enabled: true
sharedConnectors:
  enabled: ${enabled}
${extra}
EOF
}

TIER_ON=(--set security.dataTierNetworkPolicy.enabled=true)
APP_EGRESS="  applicationEgress:
    ${APP_A}:
      - cidr: 10.20.0.7/32
        ports:
          - { protocol: TCP, port: 443 }
    ${APP_B}:
      - cidr: 10.30.0.0/24
        ports:
          - { protocol: TCP, port: 8443 }"

DEFAULT_OUT="$(render_dir default "$CHART" "${TIER_ON[@]}")" || fail "the default render must succeed"
OFF_OUT="$(render_dir off "$CHART" -f "$(shared_values off false)")" || fail "sharedConnectors.enabled=false must render"
UPGRADE_OFF_OUT="$(render_dir upgrade-off "$CHART" "${TIER_ON[@]}" --is-upgrade)" \
  || fail "a client-only upgrade render must succeed"
ON_OUT="$(render_dir on "$CHART" -f "$(shared_values on true "$APP_EGRESS")")" \
  || fail "sharedConnectors.enabled=true with applicationEgress must render"
NOVALKEY_OUT="$(render_dir novalkey "$CHART" -f "$(shared_values novalkey true "  grantStoreEgress:
    - cidr: 10.40.0.9/32
      ports:
        - { protocol: TCP, port: 6380 }")" --set valkey.deploy=false --set valkey.host=valkey.example.com)" \
  || fail "valkey.deploy=false with a grantStoreEgress entry must render"
NOTIER_OUT="$(render_dir notier "$CHART" --set sharedConnectors.enabled=true \
  --set security.dataTierNetworkPolicy.enabled=false)" \
  || fail "sharedConnectors.enabled=true with the data tier off must render"
UPGRADE_ON_OUT="$(render_dir upgrade-on "$CHART" "${TIER_ON[@]}" --is-upgrade \
  --set sharedConnectors.enabled=true)" || fail "a client-only upgrade render with shared on must succeed"

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import copy
import pathlib
import sys

import yaml

COMPONENT = "shared-connector-application"
APP_LABEL = "curietech.ai/shared-application"
STORES = {"postgres": 5432, "valkey": 6379}
OTHER_STORES = ("rustfs", "clickhouse")


def load(root):
    docs = []
    for path in sorted(pathlib.Path(root).rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if isinstance(doc, dict):
                doc["_file"] = path.name
                docs.append(doc)
    if not docs:
        raise SystemExit(f"{root}: Helm wrote no YAML documents")
    return docs


def policies(docs):
    return [d for d in docs if d.get("kind") == "NetworkPolicy"]


def shared_selector(release):
    return {
        "matchLabels": {
            "app.kubernetes.io/part-of": release,
            "app.kubernetes.io/component": COMPONENT,
        },
        "matchExpressions": [{"key": APP_LABEL, "operator": "Exists"}],
    }


def is_shared_peer(peer):
    labels = (peer.get("podSelector") or {}).get("matchLabels") or {}
    return labels.get("app.kubernetes.io/component") == COMPONENT


def names_shared(doc):
    return COMPONENT in yaml.safe_dump(doc) or "-shared-" in doc["metadata"]["name"]


def store_policies(docs, fullname, component):
    return [
        p for p in policies(docs)
        if p.get("_file") == "security-datatier-networkpolicy.yaml"
        and ((p["spec"].get("podSelector") or {}).get("matchLabels") or {}).get(
            "app.kubernetes.io/component"
        ) == component
    ]


def shared_rules(policy):
    return [
        rule for rule in policy["spec"].get("ingress") or []
        if any(is_shared_peer(peer) for peer in rule.get("from") or [])
    ]


def without_shared_rules(docs):
    out = []
    for doc in policies(docs):
        if doc.get("_file") != "security-datatier-networkpolicy.yaml":
            continue
        doc = copy.deepcopy(doc)
        if "ingress" in doc["spec"]:
            doc["spec"]["ingress"] = [
                r for r in doc["spec"]["ingress"]
                if not any(is_shared_peer(p) for p in r.get("from") or [])
            ]
        out.append(doc)
    return out


def check_off(docs, release, fullname):
    errors = []
    for doc in docs:
        if doc.get("_file") == "shared-connector-networkpolicy.yaml":
            errors.append(f"{doc['kind']} {doc['metadata']['name']} rendered with shared off")
    for policy in policies(docs):
        if names_shared(policy):
            errors.append(f"NetworkPolicy {policy['metadata']['name']} names the shared component with shared off")
    return errors


def check_data_tier(docs, off_docs, release, fullname):
    errors = []
    want_peer = {"podSelector": shared_selector(release)}
    for component, port in STORES.items():
        found = [
            (p["metadata"]["name"], r)
            for p in store_policies(docs, fullname, component)
            for r in shared_rules(p)
        ]
        if len(found) != 1:
            errors.append(f"{component}: expected one shared ingress rule, found {len(found)}")
            continue
        name, rule = found[0]
        if rule.get("from") != [want_peer]:
            errors.append(f"{name}: the shared rule's peers are {rule.get('from')!r}, want only {want_peer!r}")
        if rule.get("ports") != [{"protocol": "TCP", "port": port}]:
            errors.append(f"{name}: the shared rule's ports are {rule.get('ports')!r}, want TCP {port} only")
    for component in OTHER_STORES:
        for policy in store_policies(docs, fullname, component):
            if shared_rules(policy):
                errors.append(f"{policy['metadata']['name']}: {component} must not admit shared Pods")
    for policy in policies(docs):
        if policy.get("_file") != "security-datatier-networkpolicy.yaml":
            continue
        for rule in shared_rules(policy):
            text = yaml.safe_dump(rule)
            if "mcp-connector" in text or "namespaceSelector" in text:
                errors.append(f"{policy['metadata']['name']}: the shared rule names mcp-connector or another namespace")
    strip = lambda ds: sorted(  # noqa: E731
        (yaml.safe_dump({k: v for k, v in d.items() if k != "_file"}) for d in ds)
    )
    if strip(without_shared_rules(docs)) != strip(without_shared_rules(off_docs)):
        errors.append("the data tier changed beyond the two shared ingress rules")
    return errors


def select_shared(policy, release, app=None):
    selector = policy["spec"].get("podSelector") or {}
    labels = selector.get("matchLabels") or {}
    base = {"app.kubernetes.io/part-of": release, "app.kubernetes.io/component": COMPONENT}
    if app is None:
        return selector == shared_selector(release)
    if {k: v for k, v in labels.items() if k != APP_LABEL} != base or labels.get(APP_LABEL) != app:
        return False
    extra = selector.get("matchExpressions")
    return extra in (None, [], [{"key": APP_LABEL, "operator": "Exists"}])


def check_root(docs, release, fullname, mode, apps):
    errors = []
    rendered = {
        d["metadata"]["name"]: d for d in docs
        if d.get("_file") == "shared-connector-networkpolicy.yaml"
    }
    deny_name = f"{fullname}-shared-app-deny-ingress"
    want = {
        f"{fullname}-shared-app-allow-dns",
        f"{fullname}-shared-app-allow-grant-store",
        deny_name,
    }
    want |= {f"{fullname}-shared-{app}-allow-egress" for app in apps}
    if set(rendered) != want:
        errors.append(f"shared root policies are {sorted(rendered)}, want {sorted(want)}")
        return errors
    for name, doc in rendered.items():
        component = ((doc.get("metadata") or {}).get("labels") or {}).get(
            "app.kubernetes.io/component"
        )
        if component != "shared-connector-root":
            errors.append(f"{name}: component label {component!r}, want shared-connector-root")
        if doc.get("kind") != "NetworkPolicy":
            errors.append(f"{name} is a {doc.get('kind')}")
        elif name == deny_name:
            spec = doc["spec"]
            if not select_shared(doc, release):
                errors.append(f"{name}: selects {spec.get('podSelector')!r}")
            if spec.get("policyTypes") != ["Ingress"]:
                errors.append(f"{name}: policyTypes {spec.get('policyTypes')!r}, want ['Ingress']")
            if spec.get("ingress") not in (None, []) or "egress" in spec:
                errors.append(f"{name}: must allow no ingress and carry no egress")
        elif doc["spec"].get("policyTypes") != ["Egress"] or "ingress" in doc["spec"]:
            errors.append(f"{name}: must be Egress only, got {doc['spec'].get('policyTypes')!r}")
    runner_dns = [
        p for p in policies(docs) if p["metadata"]["name"] == f"{fullname}-runner-allow-dns"
    ]
    dns = rendered[f"{fullname}-shared-app-allow-dns"]
    if not select_shared(dns, release):
        errors.append(f"{dns['metadata']['name']}: selects {dns['spec'].get('podSelector')!r}")
    # The runner's DNS rule (security-networkpolicy.yaml) with the chart's
    # default dnsNamespace; when the runner rail renders too, the two match.
    want_dns = [{
        "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}}}],
        "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
    }]
    if dns["spec"].get("egress") != want_dns or (
        runner_dns and runner_dns[0]["spec"].get("egress") != want_dns
    ):
        errors.append(f"{dns['metadata']['name']}: egress {dns['spec'].get('egress')!r} is not the runner's DNS egress")
    grant = rendered[f"{fullname}-shared-app-allow-grant-store"]
    if not select_shared(grant, release):
        errors.append(f"{grant['metadata']['name']}: selects {grant['spec'].get('podSelector')!r}")
    if mode == "valkey":
        want_egress = [{
            "to": [{"podSelector": {"matchLabels": {
                "app.kubernetes.io/name": "curie",
                "app.kubernetes.io/instance": release,
                "app.kubernetes.io/component": "valkey",
            }}}],
            "ports": [{"protocol": "TCP", "port": 6379}],
        }]
    else:
        want_egress = [{"to": [{"ipBlock": {"cidr": "10.40.0.9/32"}}],
                        "ports": [{"protocol": "TCP", "port": 6380}]}]
    if grant["spec"].get("egress") != want_egress:
        errors.append(f"{grant['metadata']['name']}: egress {grant['spec'].get('egress')!r}, want {want_egress!r}")
    for app, (cidr, port) in apps.items():
        doc = rendered[f"{fullname}-shared-{app}-allow-egress"]
        if not select_shared(doc, release, app):
            errors.append(f"{doc['metadata']['name']}: selects {doc['spec'].get('podSelector')!r}")
        egress = doc["spec"].get("egress") or []
        if len(egress) != 1 or egress[0].get("ports") != [{"protocol": "TCP", "port": port}] or [
            (peer.get("ipBlock") or {}).get("cidr") for peer in egress[0].get("to") or []
        ] != [cidr]:
            errors.append(f"{doc['metadata']['name']}: egress {egress!r}, want {cidr} on TCP {port} only")
    for doc in rendered.values():
        text = yaml.safe_dump(doc["spec"].get("podSelector"))
        if "mcp-connector" in text or "runner-sandbox" in text:
            errors.append(f"{doc['metadata']['name']}: selects a non-shared Pod")
    return errors


def check_no_tier(docs, release, fullname):
    errors = []
    if any(p.get("_file") == "security-datatier-networkpolicy.yaml" for p in policies(docs)):
        errors.append("a data-tier policy rendered with the data tier off")
    return errors + check_root(docs, release, fullname, "valkey", {})


def env_values(docs, component, container, name):
    found = []
    for doc in docs:
        if doc.get("kind") != "Deployment":
            continue
        labels = (doc.get("metadata") or {}).get("labels") or {}
        if labels.get("app.kubernetes.io/component") != component:
            continue
        for c in doc["spec"]["template"]["spec"].get("containers") or []:
            if c.get("name") == container:
                found += [e.get("value") for e in c.get("env") or [] if e.get("name") == name]
    return found


def drain_observations(docs):
    seen = set()
    for doc in docs:
        text = yaml.safe_dump(doc)
        for token in ("--installation-id-observed=true", "--installation-id-observed=false"):
            if token in text:
                seen.add(token.rsplit("=", 1)[1])
    return seen


def check_prereq(docs, release, fullname, namespace, want):
    errors = []
    got = env_values(docs, "api", "api", "CURIE_INSTALLATION_ID_OBSERVED")
    if got != [want]:
        errors.append(f"the api container's CURIE_INSTALLATION_ID_OBSERVED is {got!r}, want [{want!r}]")
    drains = drain_observations(docs)
    if drains and drains != {want}:
        errors.append(f"the drain hook observes {sorted(drains)} but the API is handed {want!r}")
    account = f"{fullname}-api"
    for doc in docs:
        if doc.get("kind") not in ("RoleBinding", "ClusterRoleBinding"):
            continue
        if not any(
            s.get("kind") == "ServiceAccount" and s.get("name") == account
            for s in doc.get("subjects") or []
        ):
            continue
        ref = doc.get("roleRef") or {}
        for role in docs:
            if role.get("kind") == ref.get("kind") and role["metadata"]["name"] == ref.get("name"):
                for rule in role.get("rules") or []:
                    resources = rule.get("resources") or []
                    if "secrets" in resources or "*" in resources:
                        errors.append(f"{role['kind']} {role['metadata']['name']} grants the API {rule!r}")
    return errors


def main():
    mode, root, release, fullname, namespace, *args = sys.argv[1:]
    docs = load(root)
    if mode == "off":
        errors = check_off(docs, release, fullname)
    elif mode == "same":
        other = load(args[0])
        dump = lambda ds: [yaml.safe_dump(d) for d in ds]  # noqa: E731
        errors = [] if dump(docs) == dump(other) else ["the renders differ"]
    elif mode == "on":
        off_docs = load(args[0])
        apps = {}
        for spec in args[1:]:
            app, cidr, port = spec.split(",")
            apps[app] = (cidr, int(port))
        errors = check_data_tier(docs, off_docs, release, fullname)
        errors += check_root(docs, release, fullname, "valkey", apps)
    elif mode == "novalkey":
        errors = check_root(docs, release, fullname, "ipblock", {})
    elif mode == "notier":
        errors = check_no_tier(docs, release, fullname)
    elif mode == "prereq":
        errors = check_prereq(docs, release, fullname, namespace, args[0])
    elif mode == "controls":
        off_docs = load(args[0])
        errors = []
        # A shared peer added to RustFS, a widened port and an mcp-connector
        # shared peer must each be caught by the data-tier check.
        for label, change in (
            ("a shared peer on RustFS", "rustfs"),
            ("the Postgres shared rule on an extra port", "port"),
            ("the Postgres shared peer without the Exists expression", "exists"),
        ):
            mutated = copy.deepcopy(docs)
            post = [r for p in store_policies(mutated, fullname, "postgres") for r in shared_rules(p)]
            if not post:
                errors.append("controls need a passing render first")
                break
            if change == "rustfs":
                target = store_policies(mutated, fullname, "rustfs")[-1]
                target["spec"].setdefault("ingress", []).append(copy.deepcopy(post[0]))
            elif change == "port":
                post[0]["ports"].append({"protocol": "TCP", "port": 5433})
            else:
                post[0]["from"][0]["podSelector"].pop("matchExpressions", None)
            if not check_data_tier(mutated, off_docs, release, fullname):
                errors.append(f"negative control not caught: {label}")
    elif mode == "rootcontrols":
        errors = []
        deny_name = f"{fullname}-shared-app-deny-ingress"
        if check_root(docs, release, fullname, "valkey", {}):
            errors.append("root controls need a passing render first")
        for label, change in (
            ("an ingress rule on the root deny", "rule"),
            ("an Egress policy type on the root deny", "egress"),
            ("the root deny without the Exists expression", "exists"),
            ("the root deny labelled with the application component", "component"),
            ("the root deny missing", "missing"),
        ):
            if errors:
                break
            mutated = copy.deepcopy(docs)
            (deny,) = [d for d in mutated if d["metadata"]["name"] == deny_name]
            spec = deny["spec"]
            if change == "rule":
                spec["ingress"] = [{"from": [{"podSelector": {}}]}]
            elif change == "egress":
                spec["policyTypes"] = ["Ingress", "Egress"]
            elif change == "exists":
                spec["podSelector"].pop("matchExpressions", None)
            elif change == "component":
                deny["metadata"]["labels"]["app.kubernetes.io/component"] = COMPONENT
            else:
                mutated.remove(deny)
            if not check_root(mutated, release, fullname, "valkey", {}):
                errors.append(f"negative control not caught: {label}")
    else:
        raise SystemExit(f"unknown mode {mode!r}")
    for error in errors:
        print(f"  {error}", file=sys.stderr)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
PY

check() {
  python3 "$CHECKER" "$1" "$2" "$RELEASE" "$FULLNAME" "$NAMESPACE" "${@:3}"
}

echo "=== Assertion 1: shared off renders nothing shared and changes nothing ==="
check off "$DEFAULT_OUT" || fail "assertion 1 (default)"
check off "$OFF_OUT" || fail "assertion 1 (explicit false)"
check off "$UPGRADE_OFF_OUT" || fail "assertion 1 (client-only upgrade)"
check same "$OFF_OUT" "$DEFAULT_OUT" || fail "assertion 1: explicit false differs from the default render"
echo "  ok: no shared peer or policy, and explicit false equals the default render"

echo "=== Assertion 2 and 3: shared on adds the data-tier peers and the root egress policies ==="
check on "$ON_OUT" "$OFF_OUT" "${APP_A},10.20.0.7/32,443" "${APP_B},10.30.0.0/24,8443" \
  || fail "assertions 2 and 3 (valkey in chart)"
check novalkey "$NOVALKEY_OUT" || fail "assertion 3 (valkey.deploy=false with grantStoreEgress)"
check controls "$ON_OUT" "$OFF_OUT" || fail "assertion 2 negative controls"
check rootcontrols "$NOTIER_OUT" || fail "assertion 3b negative controls"
echo "  ok: the root deny-ingress selects every shared application Pod of this release and admits nothing; its negative controls are caught"
echo "  ok: Postgres and Valkey admit only shared Pods of this release on their own port; DNS, grant-store and per-application egress select only shared Pods"

echo "=== Assertion 4: the root policies do not depend on the data tier ==="
check notier "$NOTIER_OUT" || fail "assertion 4"
echo "  ok: with the data tier off the shared root policies render and no data-tier policy does"

echo "=== Assertion 5: invalid shared values are refused at render ==="
egress_with() {
  # One applicationEgress entry under key $1 with cidr $2 and the YAML ports
  # line $3 (empty for none).
  local key="$1" cidr="$2" ports="${3:-}"
  local body="  applicationEgress:
    ${key}:
      - cidr: ${cidr}"
  [[ -n "$ports" ]] && body+="
        ports:
          - ${ports}"
  shared_values "egress-${key}-${cidr//\//_}-${#ports}" true "$body"
}
GOOD_PORT="{ protocol: TCP, port: 443 }"
must_render "a canonical UUID key" -f "$(egress_with "$APP_A" 10.20.0.7/32 "$GOOD_PORT")"
must_fail_naming "a non-UUID applicationEgress key" "sharedConnectors" \
  -f "$(egress_with not-a-uuid 10.20.0.7/32 "$GOOD_PORT")"
must_fail_naming "an uppercase UUID applicationEgress key" "sharedConnectors" \
  -f "$(egress_with "${APP_A^^}" 10.20.0.7/32 "$GOOD_PORT")"
must_render "a /8 entry" -f "$(egress_with "$APP_A" 10.0.0.0/8 "$GOOD_PORT")"
for cidr in 0.0.0.0/0 10.0.0.0/4 169.254.169.254/32; do
  must_fail_naming "applicationEgress entry ${cidr}" "sharedConnectors" \
    -f "$(egress_with "$APP_A" "$cidr" "$GOOD_PORT")"
done
must_fail_naming "an applicationEgress entry without ports" "sharedConnectors" \
  -f "$(egress_with "$APP_A" 10.20.0.7/32)"
must_fail_naming "an applicationEgress entry without cidr" "sharedConnectors" \
  -f "$(shared_values nocidr true "  applicationEgress:
    ${APP_A}:
      - ports:
          - ${GOOD_PORT}")"
must_fail_naming "valkey.deploy=false with no grantStoreEgress" "sharedConnectors.grantStoreEgress" \
  -f "$(shared_values nogrant true)" --set valkey.deploy=false --set valkey.host=valkey.example.com
must_fail_naming "a grantStoreEgress entry below the floor" "sharedConnectors.grantStoreEgress" \
  -f "$(shared_values badgrant true "  grantStoreEgress:
    - cidr: 0.0.0.0/0
      ports:
        - ${GOOD_PORT}")" --set valkey.deploy=false --set valkey.host=valkey.example.com
must_render "valkey.deploy=false with shared off and no grantStoreEgress" --set valkey.deploy=false --set valkey.host=valkey.example.com
must_fail_naming "a string enabled" "sharedConnectors" --set-string sharedConnectors.enabled=true
must_render "a boolean enabled" --set sharedConnectors.enabled=true

echo "=== Assertion 6: a --reuse-values replay without the sharedConnectors key renders ==="
REUSE="$TMP/reuse-chart"
cp -R "$CHART" "$REUSE"
python3 - "$REUSE/values.yaml" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path).read()
# Drop the whole top-level block: its header line and every following line
# that is indented, blank or a comment, up to the next top-level key.
pattern = re.compile(r"^sharedConnectors:[^\n]*\n(?:(?:[ \t#][^\n]*)?\n)*", re.M)
out, count = pattern.subn("", text)
if count != 1:
    raise SystemExit(f"expected one top-level sharedConnectors block in values.yaml, found {count}")
open(path, "w").write(out)
PY
python3 -c '
import sys, yaml
if "sharedConnectors" in (yaml.safe_load(open(sys.argv[1])) or {}):
    raise SystemExit("the copy still has sharedConnectors")
' "$REUSE/values.yaml" || fail "assertion 6: could not remove sharedConnectors from the copy"
REUSE_OUT="$(render_dir reuse "$REUSE" "${TIER_ON[@]}")" \
  || fail "assertion 6: the chart must render with no sharedConnectors key (read it through | default dict)"
check off "$REUSE_OUT" || fail "assertion 6: the keyless render produced shared objects"
cat >"$REUSE/templates/zz-shared-nil-control.yaml" <<'EOF'
# negative control: a direct dereference of the absent key
{{- if .Values.sharedConnectors.enabled }}{{ end }}
EOF
if helm template "$RELEASE" "$REUSE" --namespace "$NAMESPACE" -f "$REUSE/values-dev.yaml" "${TIER_ON[@]}" \
  --output-dir "$TMP/reuse-control" >/dev/null 2>&1; then
  fail "assertion 6 negative control: a direct .Values.sharedConnectors.enabled rendered, so the copy did not lack the key"
fi
echo "  ok: renders without the key; a direct dereference in the same copy fails"

echo "=== Assertion 7: shared mode prerequisites on the API ==="
check prereq "$DEFAULT_OUT" true || fail "assertion 7 (install)"
check prereq "$UPGRADE_ON_OUT" false || fail "assertion 7 (client-only upgrade control)"
echo "  ok: the API observes the installation as the drain hook does, and holds no Secret access"

echo
echo "PASS: shared connector root policies are off by default, admit only this release's shared Pods to Postgres and Valkey, and give shared Pods only DNS, grant-store and their own configured egress"
