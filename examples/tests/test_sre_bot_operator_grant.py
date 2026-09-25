"""The SRE bot's opt-in operator grant, kubernetes-operator-access.yaml.

The default install keeps its `sre-demo` write ceiling. An operator who applies
the opt-in file binds the same ServiceAccount to one aggregated ClusterRole that
reaches every part of the cluster except Secret contents; each write still waits
for Curie's per-call approval, and RBAC stays the ceiling.
"""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "examples" / "sre-bot"
DEFAULT_ACCESS = BUNDLE / "manifests" / "kubernetes-access.yaml"
OPERATOR_ACCESS = BUNDLE / "manifests" / "kubernetes-operator-access.yaml"
SKILL = BUNDLE / "skills" / "sre-bot" / "SKILL.md"

OPERATOR_ROLE = "sre-bot-kubernetes-operator"
LABEL = "curie.dev/aggregate-to-sre-bot-operator"
SUBJECT = {"kind": "ServiceAccount", "name": "sre-bot-kubernetes", "namespace": "curie"}
RBAC_API = "rbac.authorization.k8s.io"

VERBS = frozenset(
    {"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}
)

# Top-level core resources a v1.36 API server serves, minus `secrets`.
CORE_RESOURCES = (
    "bindings",
    "componentstatuses",
    "configmaps",
    "endpoints",
    "events",
    "limitranges",
    "namespaces",
    "nodes",
    "persistentvolumeclaims",
    "persistentvolumes",
    "pods",
    "podtemplates",
    "replicationcontrollers",
    "resourcequotas",
    "serviceaccounts",
    "services",
)

# Core subresources as kube-apiserver serves them (the APIResourceList in
# kubernetes/kubernetes api/discovery/api__v1.json), minus the withheld ones in
# FORBIDDEN_RESOURCES. `kubectl api-resources` lists no subresources.
CORE_SUBRESOURCES = (
    "namespaces/finalize",
    "namespaces/status",
    "nodes/proxy",
    "nodes/status",
    "persistentvolumeclaims/status",
    "persistentvolumes/status",
    "pods/attach",
    "pods/binding",
    "pods/ephemeralcontainers",
    "pods/eviction",
    "pods/exec",
    "pods/log",
    "pods/portforward",
    "pods/resize",
    "pods/status",
    "replicationcontrollers/scale",
    "replicationcontrollers/status",
    "resourcequotas/status",
    "services/status",
)

# Built-in non-core groups. metrics.k8s.io is absent on purpose: metrics-server
# serves it through an APIService, so it is not part of Kubernetes itself.
BUILT_IN_GROUPS = (
    "admissionregistration.k8s.io",
    "apiextensions.k8s.io",
    "apiregistration.k8s.io",
    "apps",
    "authentication.k8s.io",
    "authorization.k8s.io",
    "autoscaling",
    "batch",
    "certificates.k8s.io",
    "coordination.k8s.io",
    "discovery.k8s.io",
    "events.k8s.io",
    "flowcontrol.apiserver.k8s.io",
    "networking.k8s.io",
    "node.k8s.io",
    "policy",
    RBAC_API,
    "resource.k8s.io",
    "scheduling.k8s.io",
    "storage.k8s.io",
)
READER_GROUPS = ("argoproj.io",)
# Curie's sandbox groups: a Sandbox or SandboxClaim spec carries per-run
# credentials, so the grant may clear one and never read one.
SANDBOX_GROUPS = ("agents.x-k8s.io", "extensions.agents.x-k8s.io")
SANDBOX_VERBS = frozenset({"delete", "deletecollection"})
INSTALLATION_GROUPS = ("crd.k8s.amazonaws.com", "networking.k8s.aws", "vpcresources.k8s.aws")

FORBIDDEN_RESOURCES = frozenset(
    {
        "secrets",
        "secrets/*",
        "serviceaccounts/*",
        "serviceaccounts/token",
        "pods/proxy",
        "services/proxy",
    }
)
# Core resources no entry may reach, however it is spelled (`*`, `*/token`).
WITHHELD_CORE = ("secrets", "serviceaccounts/token", "pods/proxy", "services/proxy")
RULE_KEYS = frozenset({"apiGroups", "resources", "verbs"})


def _documents() -> list[dict]:
    assert OPERATOR_ACCESS.is_file(), f"{OPERATOR_ACCESS} does not exist"
    return [doc for doc in yaml.safe_load_all(OPERATOR_ACCESS.read_text()) if doc is not None]


def _of_kind(documents: list[dict], kind: str) -> list[dict]:
    return [doc for doc in documents if doc.get("kind") == kind]


def _operator_role(documents: list[dict]) -> dict:
    matches = [
        doc
        for doc in _of_kind(documents, "ClusterRole")
        if doc.get("metadata", {}).get("name") == OPERATOR_ROLE
    ]
    assert len(matches) == 1, f"expected one ClusterRole {OPERATOR_ROLE}, found {len(matches)}"
    return matches[0]


def _components(documents: list[dict]) -> list[dict]:
    return [
        doc
        for doc in _of_kind(documents, "ClusterRole")
        if doc.get("metadata", {}).get("name") != OPERATOR_ROLE
    ]


def _every_rule(documents: list[dict]) -> list[dict]:
    return [rule for doc in documents for rule in (doc.get("rules") or [])]


def _resource_matches(entry: str, resource: str) -> bool:
    # RBAC matches a resource entry only as `*`, the exact `resource[/sub]`, or
    # `*/<sub>` (ResourceMatches, pkg/apis/rbac/v1/evaluation_helpers.go).
    # `pods/*` is compared literally and so grants no subresource at all.
    if entry in ("*", resource):
        return True
    _, slash, subresource = resource.partition("/")
    return bool(slash) and entry == f"*/{subresource}"


def _grants(rules: list[dict], group: str, resource: str, verb: str) -> bool:
    return any(
        not rule.get("resourceNames")
        and (group in rule.get("apiGroups", []) or "*" in rule.get("apiGroups", []))
        and (verb in rule.get("verbs", []) or "*" in rule.get("verbs", []))
        and any(_resource_matches(entry, resource) for entry in rule.get("resources", []))
        for rule in rules
    )


def _violations(rules: list[dict]) -> list[str]:
    """Every way a rule list crosses the operator grant's ceiling."""
    found = []
    for index, rule in enumerate(rules):
        where = f"rule {index} {rule!r}"
        if set(rule) != RULE_KEYS:
            found.append(f"{where}: keys must be exactly {sorted(RULE_KEYS)}")
        fields = {key: rule.get(key) for key in RULE_KEYS}
        if not all(
            isinstance(value, list) and value and all(isinstance(item, str) for item in value)
            for value in fields.values()
        ):
            found.append(f"{where}: apiGroups, resources and verbs must be non-empty string lists")
            continue
        groups, resources, verbs = fields["apiGroups"], fields["resources"], fields["verbs"]
        if "*" in groups:
            found.append(f"{where}: apiGroups '*'")
        if "" in groups and "*" in resources:
            found.append(f"{where}: every core resource")
        if FORBIDDEN_RESOURCES & set(resources):
            found.append(f"{where}: names {sorted(FORBIDDEN_RESOURCES & set(resources))}")
        reached = sorted(
            {
                withheld
                for entry in resources
                for withheld in WITHHELD_CORE
                if _resource_matches(entry, withheld)
            }
        )
        if "" in groups and reached:
            found.append(f"{where}: reaches {reached}")
        if set(verbs) - VERBS:
            found.append(f"{where}: verbs {sorted(set(verbs) - VERBS)}")
        if set(groups) & set(SANDBOX_GROUPS) and set(verbs) - SANDBOX_VERBS:
            found.append(f"{where}: sandbox groups take {sorted(set(verbs) - SANDBOX_VERBS)}")
    return found


# -- AC1: one aggregated role, one binding, nothing else ----------------------


def test_operator_grant_holds_only_cluster_roles_and_their_binding() -> None:
    documents = _documents()
    kinds = sorted({str(doc.get("kind")) for doc in documents})
    assert set(kinds) <= {"ClusterRole", "ClusterRoleBinding"}, (
        f"kinds {kinds}: the opt-in adds no Role, RoleBinding, Namespace, "
        "ServiceAccount or Secret; kubernetes-access.yaml owns the identity"
    )
    assert {doc.get("apiVersion") for doc in documents} == {f"{RBAC_API}/v1"}


def test_operator_role_aggregates_only_the_operator_label() -> None:
    documents = _documents()
    role = _operator_role(documents)
    # Any second selector, or a matchExpressions, would pull a role such as
    # `admin` (which reads Secrets) into the grant.
    assert role.get("aggregationRule") == {
        "clusterRoleSelectors": [{"matchLabels": {LABEL: "true"}}]
    }
    # The aggregation controller overwrites these, so any written here is dead text.
    assert not role.get("rules")
    nested = [
        doc["metadata"]["name"] for doc in _components(documents) if "aggregationRule" in doc
    ]
    assert nested == [], f"component roles must not aggregate others: {nested}"


def test_one_binding_ties_the_operator_role_to_the_bot_identity_only() -> None:
    bindings = _of_kind(_documents(), "ClusterRoleBinding")
    assert len(bindings) == 1, f"expected one ClusterRoleBinding, found {len(bindings)}"
    (binding,) = bindings
    assert binding["roleRef"] == {
        "apiGroup": RBAC_API,
        "kind": "ClusterRole",
        "name": OPERATOR_ROLE,
    }
    assert binding["subjects"] == [SUBJECT]


# -- AC2: what the component roles cover ---------------------------------------


def test_every_component_role_carries_the_aggregation_label() -> None:
    components = _components(_documents())
    assert components, "no component ClusterRole feeds the aggregated role"
    unlabelled = [
        doc["metadata"]["name"]
        for doc in components
        if (doc["metadata"].get("labels") or {}).get(LABEL) != "true"
    ]
    assert unlabelled == [], f'missing {LABEL}: "true" on {unlabelled}'


def _missing(
    rules: list[dict], group: str, resources: tuple[str, ...], verbs: frozenset = VERBS
) -> list[str]:
    return [
        f"{resource}:{verb}"
        for resource in resources
        for verb in sorted(verbs)
        if not _grants(rules, group, resource, verb)
    ]


def test_components_cover_every_core_resource_but_secrets() -> None:
    rules = _every_rule(_components(_documents()))
    assert _missing(rules, "", CORE_RESOURCES) == []


def test_components_cover_every_core_subresource_but_the_token() -> None:
    rules = _every_rule(_components(_documents()))
    assert _missing(rules, "", CORE_SUBRESOURCES) == []


@pytest.mark.parametrize("group", BUILT_IN_GROUPS + READER_GROUPS)
def test_components_cover_every_resource_in_each_group(group: str) -> None:
    rules = _every_rule(_components(_documents()))
    assert _missing(rules, group, ("*",)) == []


@pytest.mark.parametrize("group", SANDBOX_GROUPS)
def test_sandbox_groups_carry_exactly_delete_and_deletecollection(group: str) -> None:
    documents = _documents()
    granted = {
        verb
        for rule in _every_rule(documents)
        if group in rule.get("apiGroups", [])
        for verb in rule.get("verbs", [])
    }
    assert granted == SANDBOX_VERBS
    assert _missing(_every_rule(_components(documents)), group, ("*",), SANDBOX_VERBS) == []


def test_installation_cloud_groups_stay_out_of_upstream() -> None:
    named = {group for rule in _every_rule(_documents()) for group in rule.get("apiGroups", [])}
    assert not named & set(INSTALLATION_GROUPS)


# -- AC3: the ceiling, over every rule in the file ------------------------------


def test_no_rule_in_the_operator_grant_crosses_the_ceiling() -> None:
    documents = _documents()
    rules = _every_rule(documents)
    assert rules, "the operator grant carries no rules"
    assert _violations(rules) == []


def test_rbac_matching_has_no_per_resource_subresource_wildcard() -> None:
    def rule(resource: str) -> list[dict]:
        return [{"apiGroups": [""], "resources": [resource], "verbs": ["create"]}]

    assert not _grants(rule("pods/*"), "", "pods/exec", "create")
    assert not _grants(rule("pods"), "", "pods/exec", "create")
    assert _grants(rule("pods/exec"), "", "pods/exec", "create")
    assert _grants(rule("*/exec"), "", "pods/exec", "create")
    assert _grants(rule("*"), "", "pods/exec", "create")


INTENDED_SHAPE = [
    {
        "apiGroups": [""],
        "resources": ["serviceaccounts", "pods/exec", "nodes/proxy", "*/scale"],
        "verbs": sorted(VERBS),
    },
    {"apiGroups": ["apps", RBAC_API], "resources": ["*"], "verbs": sorted(VERBS)},
    {"apiGroups": list(SANDBOX_GROUPS), "resources": ["*"], "verbs": sorted(SANDBOX_VERBS)},
]


def test_the_guard_accepts_the_intended_shape() -> None:
    assert _violations(INTENDED_SHAPE) == []


@pytest.mark.parametrize(
    "rule",
    [
        pytest.param({"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]}, id="secrets"),
        pytest.param(
            {"apiGroups": ["apps"], "resources": ["secrets"], "verbs": ["get"]},
            id="secrets-named-in-another-group",
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["secrets/*"], "verbs": ["get"]},
            id="secrets-slash-star",
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["serviceaccounts/*"], "verbs": ["create"]},
            id="serviceaccounts-slash-star",
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["serviceaccounts/token"], "verbs": ["create"]},
            id="serviceaccount-token",
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["*/token"], "verbs": ["create"]}, id="any-token"
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["pods/proxy"], "verbs": ["get"]}, id="pods-proxy"
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["services/proxy"], "verbs": ["get"]},
            id="services-proxy",
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["*/proxy"], "verbs": ["get"]}, id="any-proxy"
        ),
        pytest.param(
            {"apiGroups": list(SANDBOX_GROUPS), "resources": ["*"], "verbs": ["get"]},
            id="sandbox-get",
        ),
        pytest.param(
            {
                "apiGroups": ["apps", "extensions.agents.x-k8s.io"],
                "resources": ["*"],
                "verbs": ["delete", "list"],
            },
            id="sandbox-list-beside-a-group",
        ),
        pytest.param(
            {"apiGroups": ["*"], "resources": ["deployments"], "verbs": ["get"]}, id="every-group"
        ),
        pytest.param({"apiGroups": [""], "resources": ["*"], "verbs": ["get"]}, id="every-core"),
        pytest.param(
            {"apiGroups": ["apps", ""], "resources": ["*"], "verbs": ["get"]},
            id="every-core-beside-a-group",
        ),
        pytest.param(
            {"apiGroups": [RBAC_API], "resources": ["clusterroles"], "verbs": ["escalate"]},
            id="escalate",
        ),
        pytest.param(
            {"apiGroups": [RBAC_API], "resources": ["clusterroles"], "verbs": ["bind"]}, id="bind"
        ),
        pytest.param(
            {"apiGroups": [""], "resources": ["serviceaccounts"], "verbs": ["impersonate"]},
            id="impersonate",
        ),
        pytest.param(
            {"apiGroups": ["certificates.k8s.io"], "resources": ["signers"], "verbs": ["approve"]},
            id="approve",
        ),
        pytest.param(
            {"apiGroups": ["certificates.k8s.io"], "resources": ["signers"], "verbs": ["sign"]},
            id="sign",
        ),
        pytest.param(
            {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["*"]},
            id="all-verbs",
        ),
        pytest.param(
            {
                "apiGroups": ["apps"],
                "resources": ["deployments"],
                "resourceNames": ["api"],
                "verbs": ["get"],
            },
            id="resource-names",
        ),
        pytest.param({"nonResourceURLs": ["*"], "verbs": ["get"]}, id="non-resource-urls"),
        pytest.param(
            {"apiGroups": [""], "resources": "secrets", "verbs": ["get"]},
            id="resources-as-a-string",
        ),
    ],
)
def test_the_guard_rejects_each_forbidden_shape(rule: dict) -> None:
    assert _violations([*INTENDED_SHAPE, rule]), f"guard let {rule!r} through"


# -- AC4: downstream groups join by label ---------------------------------------


def _leading_comment(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            lines.append(stripped.lstrip("#").strip())
        elif stripped:
            break
    return " ".join(lines)


def test_header_tells_an_operator_to_add_groups_with_a_labelled_role() -> None:
    assert OPERATOR_ACCESS.is_file(), f"{OPERATOR_ACCESS} does not exist"
    header = _leading_comment(OPERATOR_ACCESS.read_text())
    assert LABEL in header
    assert re.search(r"\blabel\b", header, re.IGNORECASE)
    assert "ClusterRole" in header


# -- AC6: the default install is unchanged ---------------------------------------


def test_default_install_keeps_its_sre_demo_ceiling_and_no_operator_grant() -> None:
    documents = [doc for doc in yaml.safe_load_all(DEFAULT_ACCESS.read_text()) if doc]
    assert [doc["metadata"]["namespace"] for doc in _of_kind(documents, "Role")] == ["sre-demo"]
    assert all(
        set(rule["verbs"]) <= {"get", "list", "watch"}
        for role in _of_kind(documents, "ClusterRole")
        for rule in role["rules"]
    )
    assert not [doc for doc in documents if doc["metadata"].get("name") == OPERATOR_ROLE]
    assert not [
        doc for doc in documents if LABEL in (doc["metadata"].get("labels") or {})
    ]
    assert not [
        doc
        for doc in documents
        if doc.get("roleRef", {}).get("name") == OPERATOR_ROLE
    ]


# -- AC7: SKILL.md names RBAC as the ceiling, not sre-demo ------------------------


def _skill_prose() -> str:
    text = SKILL.read_text()
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    return re.sub(
        r"^(`{3,}|~{3,})[^\n]*\n.*?^\1[^\n]*$", "", text, flags=re.DOTALL | re.MULTILINE
    )


def _flat(text: str) -> str:
    return " ".join(text.split())


@pytest.mark.parametrize(
    "phrase",
    [
        "still limits the approved call to workload operations in `sre-demo`",
        "The API server still refuses writes outside `sre-demo`",
        "inside `sre-demo`",
    ],
)
def test_skill_no_longer_states_sre_demo_as_a_fixed_ceiling(phrase: str) -> None:
    assert phrase not in _flat(_skill_prose())


def _paragraphs() -> list[str]:
    return [_flat(part) for part in re.split(r"\n\s*\n", _skill_prose())]


def test_skill_still_reports_a_403_as_the_ceiling_never_an_approval_problem() -> None:
    assert [
        part
        for part in _paragraphs()
        if "403" in part
        and "ceiling" in part
        and re.search(r"\bnever retr", part, re.IGNORECASE)
        and "approval problem" in part
    ]


def test_skill_tells_the_ceiling_by_the_operator_binding() -> None:
    assert [
        part
        for part in _paragraphs()
        if "ClusterRoleBinding" in part and OPERATOR_ROLE in part
    ]


# A sentence that puts the write ceiling at sre-demo must say when that holds.
FIXED_CEILING = re.compile(
    r"only (?:in|inside|within|to) `sre-demo`"
    r"|`sre-demo` only"
    r"|(?:outside|inside|within|limited to|confined to|restricted to) `sre-demo`"
    r"|operations in `sre-demo`"
)
CONDITION = re.compile(r"\b(?:default|unless|absent|if|operator grant)\b", re.IGNORECASE)


def _unconditional_ceilings(prose: str) -> list[str]:
    return [
        sentence
        for sentence in re.split(r"(?<=[.!?])\s+", _flat(prose))
        if FIXED_CEILING.search(sentence) and not CONDITION.search(sentence)
    ]


@pytest.mark.parametrize(
    "sentence",
    [
        "Kubernetes RBAC still limits the approved call to workload operations in `sre-demo`.",
        "The API server still refuses writes outside `sre-demo`, Secrets, and RBAC.",
        "Raw manifest updates can replace images inside `sre-demo`; show the effect.",
        "Writes succeed only in `sre-demo`.",
        "You can change workloads in `sre-demo` only.",
    ],
)
def test_the_fixed_ceiling_check_flags_an_unconditional_sentence(sentence: str) -> None:
    assert _unconditional_ceilings(sentence) == [sentence]


@pytest.mark.parametrize(
    "sentence",
    [
        "RBAC is the ceiling: by default it refuses writes outside `sre-demo`.",
        "Present means the operator grant applies, absent means writes succeed only in "
        "`sre-demo`.",
        "Workload operations in `sre-demo` by default, wider where it was applied.",
    ],
)
def test_the_fixed_ceiling_check_passes_a_conditional_sentence(sentence: str) -> None:
    assert _unconditional_ceilings(sentence) == []


def test_skill_states_no_unconditional_sre_demo_ceiling() -> None:
    assert _unconditional_ceilings(_skill_prose()) == []
