#!/usr/bin/env bash
#
# Render-assertion: per-run Docker daemons for factory repositories (ADR 0217
# decisions 4, 5 and 7). Every object lives in templates/docker-daemon.yaml and
# renders only when a repository runner enables docker.
#
# Proves:
#   (a) Default values, repositories without docker, and docker repositories
#       with worker.deploy=false render no docker-daemon object, no
#       <fullname>-docker-daemon-* name and no <fullname>-runner-allow-docker-daemon.
#   (b) Repository values (acme-corp/api docker with an allowlist,
#       acme-corp/web docker without one, acme-corp/ui no docker) render, in
#       the <release-namespace>-<fullname>-docker namespace: the privileged PSA
#       Namespace, the worker Role (exact rules) and RoleBinding, the
#       ResourceQuota and LimitRange from values, the default-deny, DNS,
#       public and per-repository NetworkPolicies (exact shapes, class label
#       repo-<sha256(path)[:16]>), the Rail 1 sandbox-to-daemon allow in the
#       release namespace, and three ValidatingAdmissionPolicies with Deny
#       bindings and failurePolicy Fail. createNamespace=false, a namespace
#       override, a port override and an allowlist-only fixture are covered.
#   (c) Structural CEL checks (no CEL evaluator is installed): every pods
#       policy rule's text is present (worker username, labels and egress
#       classes, dockerd name and image digest literal, userns and authz
#       plugin flags, privilege and capabilities, service account token,
#       emptyDir sizeLimit and secret volumes, host namespaces and ports,
#       resources, activeDeadlineSeconds) and every validation has a message.
#       The cluster proof covers CEL behaviour.
#   (d) templates/runner-resources-admission.yaml renders byte-identical with
#       and without every docker block and factory.docker.
#   (e) NEGATIVE: missing daemonImage, tag-only daemonImage, a 0.0.0.0/0
#       allowlist entry, port 0 and 65536, and missing quota.hard or
#       limitRange fail render, and stderr names the key. (Kubernetes < 1.30 is
#       not asserted here: runner-resources-admission.yaml already refuses it
#       for every render a docker repository needs.)
#   (f) A security.networkPolicy.clusterCidrs entry appears in the public
#       policy's excepts next to the metadata and private carve-outs.
#
# Runnable locally (from anywhere) and from CI. Fails loudly.
set -euo pipefail

[ "$#" -eq 0 ] || { echo "Usage: $0" >&2; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

DAEMON_IMAGE="registry.example.com/acme/curie-docker-daemon@sha256:$(printf '7%.0s' {1..64})"
CLUSTER_CIDR="198.18.0.0/15"

CHECKER="$TMP/check.py"
cat >"$CHECKER" <<'PY'
import hashlib
import json
import re
import sys
import yaml

mode, path = sys.argv[1], sys.argv[2]
args = sys.argv[3:]
docs = [d for d in yaml.safe_load_all(open(path)) if d]
FULL = "t-curie"
REL_NS = "default"
WORKER_SA = f"system:serviceaccount:{REL_NS}:{FULL}-worker"
SELECTOR = {"app.kubernetes.io/name": "curie", "app.kubernetes.io/instance": "t"}
DAEMON_SEL = dict(SELECTOR, **{"app.kubernetes.io/component": "docker-daemon"})
RUNNER_SEL = dict(SELECTOR, **{"app.kubernetes.io/component": "runner-sandbox"})


def fail(msg):
    sys.exit(f"{mode}: {msg}")


def docker_objects():
    out = []
    for d in docs:
        meta = d.get("metadata", {})
        name = meta.get("name", "")
        component = (meta.get("labels") or {}).get("app.kubernetes.io/component", "")
        if (component.startswith("docker-daemon") or name.startswith(f"{FULL}-docker-daemon")
                or name == f"{FULL}-runner-allow-docker-daemon"):
            out.append(f"{d['kind']}/{name}")
    return out


def find(kind, name):
    hits = [d for d in docs if d.get("kind") == kind and d["metadata"]["name"] == name]
    if len(hits) != 1:
        fail(f"expected exactly one {kind}/{name}, got {len(hits)}")
    return hits[0]


def in_ns(doc, ns):
    got = doc["metadata"].get("namespace")
    if got != ns:
        fail(f"{doc['kind']}/{doc['metadata']['name']} namespace {got!r}, want {ns!r}")


def norm_ports(ports):
    return sorted((p.get("protocol", "TCP"), int(p["port"])) for p in ports)


def strs(d):
    return {k: str(v) for k, v in (d or {}).items()}


def egress_class(repo_path):
    return "repo-" + hashlib.sha256(repo_path.encode()).hexdigest()[:16]


if mode == "absent":
    found = docker_objects()
    if found:
        fail(f"docker-daemon objects rendered without a docker repository: {found}")
    print(f"ok: {mode}")
    sys.exit(0)

if mode != "present":
    fail("unknown mode")

values = json.load(open(args[0]))
ns, image, port = args[1], args[2], int(args[3])
create_ns = args[4] == "true"
rail1 = args[5] == "true"
cluster_cidr = args[6] if len(args) > 6 else ""
docker_values = values["factory"].get("docker", {})
repos = [r for b in values["factory"]["bindings"] for r in b["repos"]]
docker_repos = [r for r in repos if r.get("runner") and r.get("docker", {}).get("enabled")]
allowlisted = {r["path"]: r["docker"]["egressAllowlist"] for r in docker_repos if r["docker"].get("egressAllowlist")}
public = any(not r["docker"].get("egressAllowlist") for r in docker_repos)
classes = (["public"] if public else []) + [egress_class(p) for p in sorted(allowlisted)]
non_classes = [egress_class(r["path"]) for r in repos if r["path"] not in allowlisted]

# 1. Namespace.
namespaces = [d for d in docs if d.get("kind") == "Namespace" and d["metadata"]["name"] == ns]
if not create_ns:
    if namespaces:
        fail(f"createNamespace=false still rendered Namespace/{ns}")
else:
    if len(namespaces) != 1:
        fail(f"expected Namespace/{ns}, got {len(namespaces)}")
    labels = namespaces[0]["metadata"].get("labels") or {}
    want = {"pod-security.kubernetes.io/enforce": "privileged",
            "pod-security.kubernetes.io/enforce-version": "latest",
            "app.kubernetes.io/component": "docker-daemon-namespace",
            "app.kubernetes.io/instance": "t"}
    for k, v in want.items():
        if labels.get(k) != v:
            fail(f"Namespace/{ns} label {k}={labels.get(k)!r}, want {v!r}")

# 2. Role and RoleBinding.
role_name = f"{FULL}-docker-daemon-worker"
role = find("Role", role_name)
in_ns(role, ns)
triples = set()
for rule in role.get("rules", []):
    if set(rule) - {"apiGroups", "resources", "verbs"}:
        fail(f"Role rule carries extra fields {sorted(set(rule) - {'apiGroups', 'resources', 'verbs'})}")
    for g in rule["apiGroups"]:
        for r in rule["resources"]:
            for v in rule["verbs"]:
                triples.add((g, r, v))
want_triples = set()
for g, r, verbs in (("", "pods", "create get list delete"), ("", "pods/log", "get"),
                    ("", "services", "create get list delete"), ("", "secrets", "create delete"),
                    ("networking.k8s.io", "networkpolicies", "create get list delete")):
    for v in verbs.split():
        want_triples.add((g, r, v))
if triples != want_triples:
    fail(f"Role rules differ: extra {sorted(triples - want_triples)} missing {sorted(want_triples - triples)}")
binding = find("RoleBinding", role_name)
in_ns(binding, ns)
if binding["roleRef"] != {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role_name}:
    fail(f"RoleBinding roleRef {binding['roleRef']}")
if binding.get("subjects") != [{"kind": "ServiceAccount", "name": f"{FULL}-worker", "namespace": REL_NS}]:
    fail(f"RoleBinding subjects {binding.get('subjects')}")

# 3. ResourceQuota and 4. LimitRange.
quota = find("ResourceQuota", f"{FULL}-docker-daemon")
in_ns(quota, ns)
if strs(quota["spec"].get("hard")) != strs(docker_values["quota"]["hard"]):
    fail(f"ResourceQuota hard {quota['spec'].get('hard')} != {docker_values['quota']['hard']}")
limits = find("LimitRange", f"{FULL}-docker-daemon")
in_ns(limits, ns)
lr = docker_values["limitRange"]
want_item = {"type": "Container", "max": strs(lr["max"]), "default": strs(lr["default"]),
             "defaultRequest": strs(lr["defaultRequest"])}
items = limits["spec"].get("limits", [])
if len(items) != 1 or {k: (strs(v) if isinstance(v, dict) else v) for k, v in items[0].items()} != want_item:
    fail(f"LimitRange limits {items} != [{want_item}]")

# 5. NetworkPolicies in the daemon namespace.
netpols = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "NetworkPolicy"
           and d["metadata"].get("namespace") == ns}
want_names = {f"{FULL}-docker-daemon-default-deny", f"{FULL}-docker-daemon-allow-dns"}
if public:
    want_names.add(f"{FULL}-docker-daemon-egress-public")
for p in allowlisted:
    want_names.add(f"{FULL}-docker-daemon-egress-{egress_class(p)}")
if set(netpols) != want_names:
    fail(f"daemon namespace NetworkPolicies {sorted(netpols)} != {sorted(want_names)}")
if netpols[f"{FULL}-docker-daemon-default-deny"]["spec"] != {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}:
    fail(f"default-deny spec {netpols[f'{FULL}-docker-daemon-default-deny']['spec']}")
dns = netpols[f"{FULL}-docker-daemon-allow-dns"]["spec"]
if dns.get("podSelector") != {"matchLabels": DAEMON_SEL} or dns.get("policyTypes") != ["Egress"] or "ingress" in dns:
    fail(f"allow-dns selector/policyTypes {dns}")
egress = dns.get("egress", [])
if (len(egress) != 1 or egress[0].get("to") != [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}}}]
        or norm_ports(egress[0].get("ports", [])) != [("TCP", 53), ("UDP", 53)]):
    fail(f"allow-dns egress {egress}")


def check_egress_policy(name, cls, entries, exact):
    spec = netpols[name]["spec"]
    if spec.get("podSelector") != {"matchLabels": dict(DAEMON_SEL, **{"curietech.ai/docker-egress": cls})}:
        fail(f"{name} podSelector {spec.get('podSelector')}")
    if spec.get("policyTypes") != ["Egress"] or "ingress" in spec:
        fail(f"{name} policyTypes {spec.get('policyTypes')}")
    rules = spec.get("egress", [])
    if len(rules) != len(entries):
        fail(f"{name} has {len(rules)} egress rules, want {len(entries)}")
    for rule, entry in zip(rules, entries):
        to = rule.get("to", [])
        if len(to) != 1 or set(to[0]) != {"ipBlock"} or to[0]["ipBlock"]["cidr"] != entry["cidr"]:
            fail(f"{name} rule {rule} does not target {entry['cidr']}")
        if norm_ports(rule.get("ports", [])) != norm_ports(entry["ports"]):
            fail(f"{name} {entry['cidr']} ports {rule.get('ports')} != {entry['ports']}")
        excepts = to[0]["ipBlock"].get("except", [])
        if len(excepts) != len(set(excepts)):
            fail(f"{name} {entry['cidr']} except has duplicates {excepts}")
        if exact is not None and excepts != exact.get(entry["cidr"], []):
            fail(f"{name} {entry['cidr']} except {excepts} != {exact.get(entry['cidr'], [])}")
        if entry["cidr"] == "0.0.0.0/0":
            need = {"169.254.0.0/16", "10.0.0.0/8", "100.64.0.0/10", "172.16.0.0/12", "192.168.0.0/16"}
            if cluster_cidr:
                need.add(cluster_cidr)  # (f)
            if not need <= set(excepts):
                fail(f"{name} 0.0.0.0/0 except {excepts} lacks {sorted(need - set(excepts))}")
        if entry["cidr"] == "::/0" and not {"fc00::/7", "fe80::/10"} <= set(excepts):
            fail(f"{name} ::/0 except {excepts} lacks the private IPv6 ranges")


web_ports = [{"protocol": "TCP", "port": 80}, {"protocol": "TCP", "port": 443}]
if public:
    check_egress_policy(f"{FULL}-docker-daemon-egress-public", "public",
                        [{"cidr": "0.0.0.0/0", "ports": web_ports}, {"cidr": "::/0", "ports": web_ports}], None)
for p, entries in allowlisted.items():
    check_egress_policy(f"{FULL}-docker-daemon-egress-{egress_class(p)}", egress_class(p), entries, {})

# 6. Rail 1 allow in the release namespace.
rail = [d for d in docs if d.get("kind") == "NetworkPolicy" and d["metadata"]["name"] == f"{FULL}-runner-allow-docker-daemon"]
if not rail1:
    if rail:
        fail("Rail 1 sandbox-to-daemon allow rendered with the sandbox or network policy disabled")
else:
    if len(rail) != 1:
        fail(f"expected one {FULL}-runner-allow-docker-daemon, got {len(rail)}")
    if rail[0]["metadata"].get("namespace", REL_NS) != REL_NS:
        fail(f"Rail 1 allow namespace {rail[0]['metadata'].get('namespace')}")
    spec = rail[0]["spec"]
    want_spec = {
        "podSelector": {"matchLabels": RUNNER_SEL},
        "policyTypes": ["Egress"],
        "egress": [{"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": ns}},
                            "podSelector": {"matchLabels": DAEMON_SEL}}],
                    "ports": [{"protocol": "TCP", "port": port}]}],
    }
    if spec != want_spec:
        fail(f"Rail 1 allow spec {json.dumps(spec, sort_keys=True)} != {json.dumps(want_spec, sort_keys=True)}")

# 7-9. Admission policies.
NS_SELECTOR = {"matchLabels": {"kubernetes.io/metadata.name": ns}}
SA_EXPR = f"request.userInfo.username == '{WORKER_SA}'"


def policy(suffix, want_rules, want_sa):
    name = f"{FULL}-docker-daemon-{suffix}"
    vap = find("ValidatingAdmissionPolicy", name)
    spec = vap["spec"]
    if spec.get("failurePolicy") != "Fail":
        fail(f"{name} failurePolicy {spec.get('failurePolicy')}")
    mc = spec.get("matchConstraints", {})
    if mc.get("namespaceSelector") != NS_SELECTOR:
        fail(f"{name} namespaceSelector {mc.get('namespaceSelector')}")
    got = set()
    for rule in mc.get("resourceRules", []):
        for g in rule["apiGroups"]:
            for r in rule["resources"]:
                for op in rule["operations"]:
                    got.add((g, r, op))
    if got != want_rules:
        fail(f"{name} resourceRules {sorted(got)} != {sorted(want_rules)}")
    conds = spec.get("matchConditions", [])
    if want_sa:
        if len(conds) != 1 or " ".join(conds[0]["expression"].split()) != SA_EXPR:
            fail(f"{name} matchConditions {conds} do not scope to the worker service account")
    elif conds:
        fail(f"{name} must match every requester, has matchConditions {conds}")
    validations = spec.get("validations", [])
    if not validations:
        fail(f"{name} has no validations")
    for v in validations:
        if not str(v.get("message", "")).strip():
            fail(f"{name} validation without a message: {v.get('expression')}")
    b = find("ValidatingAdmissionPolicyBinding", name)
    if b["spec"].get("policyName") != name or b["spec"].get("validationActions") != ["Deny"]:
        fail(f"{name} binding {b['spec']}")
    return " ".join(" ".join(v["expression"].split()) for v in validations), validations


pods_text, pods_validations = policy("pods", {("", "pods", "CREATE"), ("", "pods", "UPDATE")}, False)
objects_text, _ = policy("objects", {("", "services", "CREATE"), ("", "secrets", "CREATE"),
                                     ("networking.k8s.io", "networkpolicies", "CREATE")}, True)
cleanup_text, _ = policy("cleanup", {("networking.k8s.io", "networkpolicies", "DELETE")}, True)

# (c) structural CEL checks on the pods policy.
if len(pods_validations) < 10:
    fail(f"pods policy has {len(pods_validations)} validations, want at least 10")
need = {
    "creator": ["request.operation != 'CREATE'", SA_EXPR],
    "claim label": ["curietech.ai/sandbox-claim"],
    "managed-by": ["curietech.ai/managed-by", "'curie-sandbox-substrate'"],
    "selector labels": ["app.kubernetes.io/component", "'docker-daemon'", "app.kubernetes.io/name",
                        "'curie'", "app.kubernetes.io/instance", "'t'"],
    "egress label": ["curietech.ai/docker-egress"] + [f"'{c}'" for c in classes],
    "dockerd": ["'dockerd'", f"'{image}'", "initContainers", "ephemeralContainers"],
    "command override": ["command"],
    "args": ["args", "'--userns-remap=default'", "'--authorization-plugin=curie-authz'"],
    "privilege": ["privileged", "capabilities"],
    "service account token": ["automountServiceAccountToken", "== false", "serviceAccountToken", "serviceAccountName"],
    "volumes": ["volumes", "emptyDir", "sizeLimit", "secret"],
    "host access": ["hostNetwork", "hostPID", "hostIPC", "hostPort"],
    "resources": ["'cpu'", "'memory'", "'ephemeral-storage'", "requests", "limits"],
    "deadline": ["activeDeadlineSeconds"],
}
for rule, needles in need.items():
    for n in needles:
        if n not in pods_text:
            fail(f"pods policy ({rule}) lacks {n!r}")
for c in non_classes:
    if c in pods_text:
        fail(f"pods policy admits egress class {c} for a repository without an allowlist")
for n in ["curietech.ai/sandbox-claim", "'curie-sandbox-substrate'", "['Ingress']", "egress",
          "podSelector", "Opaque", "'kubernetes.io/tls'", "ClusterIP", "externalIPs"]:
    if n not in objects_text:
        fail(f"objects policy lacks {n!r}")
for n in ["oldObject.metadata.labels", "'curietech.ai/sandbox-claim'"]:
    if n not in cleanup_text:
        fail(f"cleanup policy lacks {n!r}")

# Every docker object lands in the daemon namespace or is cluster scoped/Rail 1.
for d in docs:
    name = d["metadata"]["name"]
    if name.startswith(f"{FULL}-docker-daemon") and d["kind"] in {"Role", "RoleBinding", "ResourceQuota", "LimitRange", "NetworkPolicy"}:
        in_ns(d, ns)
print(f"ok: {mode} ({ns})")
PY

render() { helm template t "$CHART" "$@"; }

expect_fail() {
  # $1: key the refusal must name; remaining: render args.
  local want="$1"; shift
  if render "$@" >"$TMP/neg.out" 2>"$TMP/neg.err"; then fail "render succeeded, expected failure naming $want"; fi
  grep -qF -- "$want" "$TMP/neg.err" || { cat "$TMP/neg.err" >&2; fail "refusal does not name $want"; }
}

python3 - "$TMP" "$DAEMON_IMAGE" "$CLUSTER_CIDR" <<'PY'
import copy, json, pathlib, sys

tmp, image, cluster_cidr = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
digest = lambda c: "sha256:" + c * 64
runner = {
    "tools": ["uv", "docker-cli"],
    "image": "registry.example.com/acme/repository-runner@" + digest("a"),
    "base": digest("d"),
    "platformDigests": {"linux/amd64": digest("e"), "linux/arm64": digest("f")},
}
quota = {
    "pods": "8", "requests.cpu": "8", "requests.memory": "16Gi", "requests.ephemeral-storage": "160Gi",
    "limits.cpu": "16", "limits.memory": "32Gi", "limits.ephemeral-storage": "160Gi",
    "services": "8", "secrets": "16", "persistentvolumeclaims": "0",
    "services.loadbalancers": "0", "services.nodeports": "0",
}
limit_range = {
    "max": {"cpu": "4", "memory": "8Gi", "ephemeral-storage": "40Gi"},
    "default": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "20Gi"},
    "defaultRequest": {"cpu": "500m", "memory": "1Gi", "ephemeral-storage": "10Gi"},
}
values = {
    "agentSandbox": {"runnerImages": {"factory": "registry.example.com/acme/agent-runner@" + digest("c")}},
    "security": {"networkPolicy": {"clusterCidrs": [cluster_cidr]}},
    "factory": {
        "sandboxEgress": {"factory": True},
        "docker": {"daemonImage": image},
        "bindings": [{"name": "acme", "repos": [
            {"path": "acme-corp/api", "runner": copy.deepcopy(runner), "docker": {
                "enabled": True,
                "egressAllowlist": [{"cidr": "203.0.113.0/24", "ports": [{"protocol": "TCP", "port": 443}]}],
            }},
            {"path": "acme-corp/web", "runner": copy.deepcopy(runner), "docker": {"enabled": True}},
            {"path": "acme-corp/ui", "runner": copy.deepcopy(runner)},
        ]}],
    },
}
json.dump(values, open(tmp / "docker-values.json", "w"))

# The checker compares against the chart defaults documented in values.yaml.
expected = copy.deepcopy(values)
expected["factory"]["docker"].update({"quota": {"hard": quota}, "limitRange": limit_range})
json.dump(expected, open(tmp / "docker-expected.json", "w"))

only_allowlist = copy.deepcopy(expected)
only_allowlist["factory"]["bindings"][0]["repos"][1]["docker"]["enabled"] = False
json.dump(only_allowlist, open(tmp / "allowlist-only-expected.json", "w"))

no_docker = copy.deepcopy(values)
del no_docker["factory"]["docker"]
for repo in no_docker["factory"]["bindings"][0]["repos"]:
    repo.pop("docker", None)
json.dump(no_docker, open(tmp / "no-docker-values.json", "w"))
PY

DEFAULT_NS="default-t-curie-docker"

# (a) nothing renders without a docker repository.
render >"$TMP/default.yaml" || fail "default render"
python3 "$CHECKER" absent "$TMP/default.yaml" || fail "(a) default render"
render -f "$TMP/no-docker-values.json" >"$TMP/no-docker.yaml" || fail "repositories without docker render"
python3 "$CHECKER" absent "$TMP/no-docker.yaml" || fail "(a) repositories without docker"
render -f "$TMP/docker-values.json" --set worker.deploy=false >"$TMP/no-worker.yaml" || fail "worker.deploy=false render"
python3 "$CHECKER" absent "$TMP/no-worker.yaml" || fail "(a) worker.deploy=false"

# (b) (c) (f) repository render.
render -f "$TMP/docker-values.json" >"$TMP/docker.yaml" || fail "docker repository render"
python3 "$CHECKER" present "$TMP/docker.yaml" "$TMP/docker-expected.json" "$DEFAULT_NS" "$DAEMON_IMAGE" 2376 true true "$CLUSTER_CIDR" \
  || fail "(b)(c)(f) docker repository render"

# (b) only allowlisted repositories: no public policy or class.
render -f "$TMP/docker-values.json" --set 'factory.bindings[0].repos[1].docker.enabled=false' >"$TMP/allowlist-only.yaml" || fail "allowlist-only render"
python3 "$CHECKER" present "$TMP/allowlist-only.yaml" "$TMP/allowlist-only-expected.json" "$DEFAULT_NS" "$DAEMON_IMAGE" 2376 true true "$CLUSTER_CIDR" \
  || fail "(b) allowlist-only render"

# (b) namespace override, createNamespace=false, port override, quota override.
python3 - "$TMP/docker-expected.json" "$TMP/override-expected.json" <<'PY'
import json, sys
v = json.load(open(sys.argv[1]))
v["factory"]["docker"]["quota"]["hard"]["pods"] = "3"
json.dump(v, open(sys.argv[2], "w"))
PY
render -f "$TMP/docker-values.json" --set-string factory.docker.namespace=acme-docker \
  --set factory.docker.createNamespace=false --set factory.docker.port=2443 \
  --set-string 'factory.docker.quota.hard.pods=3' >"$TMP/override.yaml" || fail "override render"
python3 "$CHECKER" present "$TMP/override.yaml" "$TMP/override-expected.json" acme-docker "$DAEMON_IMAGE" 2443 false true "$CLUSTER_CIDR" \
  || fail "(b) namespace, createNamespace, port and quota overrides"

# (b) Rail 1 allow follows the Rail 1 condition.
render -f "$TMP/docker-values.json" --set security.networkPolicy.enabled=false >"$TMP/no-netpol.yaml" || fail "network policy disabled render"
python3 - "$TMP/no-netpol.yaml" <<'PY' || fail "(b) Rail 1 allow rendered with network policy disabled"
import sys, yaml
names = [d["metadata"]["name"] for d in yaml.safe_load_all(open(sys.argv[1])) if d and d.get("kind") == "NetworkPolicy"]
sys.exit("t-curie-runner-allow-docker-daemon rendered" if "t-curie-runner-allow-docker-daemon" in names else 0)
PY

# (d) runner admission is byte-identical with and without docker.
render -f "$TMP/docker-values.json" -s templates/runner-resources-admission.yaml >"$TMP/admission-docker.yaml" || fail "runner admission docker render"
render -f "$TMP/no-docker-values.json" -s templates/runner-resources-admission.yaml >"$TMP/admission-plain.yaml" || fail "runner admission plain render"
cmp -s "$TMP/admission-docker.yaml" "$TMP/admission-plain.yaml" || { diff "$TMP/admission-plain.yaml" "$TMP/admission-docker.yaml" >&2 || true; fail "(d) runner admission changed with docker enabled"; }

# (e) refusals name the key.
expect_fail "factory.docker.daemonImage" -f "$TMP/docker-values.json" --set-string factory.docker.daemonImage=
expect_fail "factory.docker.daemonImage" -f "$TMP/docker-values.json" --set-string factory.docker.daemonImage=registry.example.com/acme/curie-docker-daemon:1.0
expect_fail "factory.docker.daemonImage" -f "$TMP/docker-values.json" --set-string "factory.docker.daemonImage=registry.example.com/acme/curie-docker-daemon@sha256:abc"
expect_fail "factory.bindings[0].repos[0].docker.egressAllowlist" -f "$TMP/docker-values.json" \
  --set-json 'factory.bindings[0].repos[0].docker.egressAllowlist=[{"cidr":"0.0.0.0/0","ports":[{"protocol":"TCP","port":443}]}]'
expect_fail "factory.docker.port" -f "$TMP/docker-values.json" --set factory.docker.port=0
expect_fail "factory.docker.port" -f "$TMP/docker-values.json" --set factory.docker.port=65536
expect_fail "factory.docker.quota.hard" -f "$TMP/docker-values.json" --set-json 'factory.docker.quota.hard=null'
expect_fail "factory.docker.limitRange" -f "$TMP/docker-values.json" --set-json 'factory.docker.limitRange=null'

echo "PASS: docker-daemon assertions"
