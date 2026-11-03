"""The measuring identity's least-privilege self-check, ADR 0238.

@spec MANAGED-CNI-5 @spec MANAGED-CNI-9. The measuring identity holds no
cluster-admin. Its rights are exactly the clause 5 manifest, and the verifier
proves it: a SubjectAccessReview showing any right beyond the manifest refuses
the campaign. The same check runs before the campaign, on every guard call and
before Readiness issuance.

The check has two halves, and either refuses:

* **Probes.** SubjectAccessReviews for the measuring ServiceAccount, with the
  groups the API server gives every ServiceAccount, over the clause 5 probe
  list: every verb on Secrets, ``nodes/proxy``, ``pods/exec``,
  ``pods/attach``, ``pods/portforward``, ``pods/ephemeralcontainers``, RBAC
  writes, ``impersonate``, NetworkPolicy writes in the protected and release
  namespaces, and pod creation in every namespace the manifest does not list.
  Every review must answer, and every answer must be a denial. Group-granted
  rights are caught here because the review carries the groups.
* **Bindings.** Every RoleBinding and ClusterRoleBinding naming the
  ServiceAccount as a subject, expanded through its Role or ClusterRole into
  atomic ``(namespace, group, resource, name, verb)`` rights, must equal the
  manifest's atoms exactly. A wildcard, an unreadable role or any extra or
  missing atom refuses.

The release namespace Role is part of the manifest only while one campaign
holds its binding (clause 8). ``release_allowed`` accepts the manifest with or
without that binding; at Readiness issuance it is ``False`` and the binding
must be gone.

This module reads nothing itself. ``MeasuringRbacReader`` is implemented by the
measuring identity's own bounded client, whose clause 5 rights include creating
SubjectAccessReviews and reading RBAC objects and Namespaces.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

__all__ = [
    "MeasuringRbacReader",
    "MeasuringRbacRefused",
    "MeasuringScope",
    "check_least_privilege",
    "manifest_atoms",
    "measuring_scope_document",
    "probe_attributes",
    "rbac_set_scope",
]

Atom = tuple[str, str, str, str, str]
_RBAC = "rbac.authorization.k8s.io"
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
_GROUP = re.compile(r"(?:[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?)?")
_ROLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,252}")
_RESOURCE = re.compile(r"[a-z0-9]+(?:/[a-z0-9]+)?")
_VERBS = frozenset({"create", "get", "list", "watch", "update", "patch", "delete"})
_ALL_VERBS = ("get", "list", "watch", "create", "update", "patch", "delete", "deletecollection")
_WRITE_VERBS = ("create", "update", "patch", "delete", "deletecollection")
_RBAC_WRITE_VERBS = (*_WRITE_VERBS, "escalate", "bind")
# Workload controllers: creating or changing one runs a pod the identity chose.
_CONTROLLERS = (
    ("apps", "daemonsets"),
    ("apps", "deployments"),
    ("apps", "statefulsets"),
    ("apps", "replicasets"),
    ("batch", "jobs"),
    ("batch", "cronjobs"),
    ("", "replicationcontrollers"),
)
# Kubernetes' default bootstrap grants to the groups every ServiceAccount token
# carries: discovery, health and issuer URLs, and reviews of its own access.
# A group binding may grant these and nothing else.
_AMBIENT_URLS = frozenset(
    {
        "/api",
        "/api/*",
        "/apis",
        "/apis/*",
        "/healthz",
        "/livez",
        "/openapi",
        "/openapi/*",
        "/readyz",
        "/version",
        "/version/",
        "/.well-known/openid-configuration",
        "/.well-known/openid-configuration/",
        "/openid/v1/jwks",
        "/openid/v1/jwks/",
    }
)
_AMBIENT: frozenset[Atom] = frozenset(
    {("", "url", url, "", "get") for url in _AMBIENT_URLS}
    | {
        ("", "authorization.k8s.io", "selfsubjectaccessreviews", "", "create"),
        ("", "authorization.k8s.io", "selfsubjectrulesreviews", "", "create"),
        ("", "authentication.k8s.io", "selfsubjectreviews", "", "create"),
    }
)
# The rendered RBAC set's measuring identity member (clause 6); the release
# binding is per campaign and comes from the permit instead.
_SCOPE_MEMBERS = (
    "service_account",
    "control_namespace",
    "protected_namespace",
    "probe_namespaces",
    "release_namespace",
    "channel_rules",
    "cni_configmaps",
    "provider_policies",
)
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_CONNECT_VERBS = ("get", "create")
_CONCURRENCY = 16

# Clause 5 manifest, by rule. Each entry is (apiGroup, resource, verbs).
_PROBE_RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("", "pods", ("create", "get", "list", "watch", "patch", "delete")),
    ("", "pods/log", ("get",)),
    ("", "pods/status", ("get",)),
    (
        "networking.k8s.io",
        "networkpolicies",
        ("create", "get", "list", "watch", "update", "patch", "delete"),
    ),
)
_PROTECTED_RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("", "pods", ("create", "get", "list", "watch", "delete")),
    ("", "pods/log", ("get",)),
    ("", "pods/status", ("get",)),
)
_RELEASE_RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("", "pods", ("create", "get", "list", "watch", "delete")),
    ("", "pods/log", ("get",)),
)
_READ = ("get", "list", "watch")
_CLUSTER_READS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("", "nodes", _READ),
    ("", "namespaces", _READ),
    ("", "pods", _READ),
    ("apps", "daemonsets", _READ),
    ("apps", "deployments", _READ),
    ("networking.k8s.io", "networkpolicies", _READ),
    ("admissionregistration.k8s.io", "validatingwebhookconfigurations", _READ),
    ("admissionregistration.k8s.io", "mutatingwebhookconfigurations", _READ),
    ("admissionregistration.k8s.io", "validatingadmissionpolicies", _READ),
    ("admissionregistration.k8s.io", "validatingadmissionpolicybindings", _READ),
    (_RBAC, "roles", _READ),
    (_RBAC, "rolebindings", _READ),
    (_RBAC, "clusterroles", _READ),
    (_RBAC, "clusterrolebindings", _READ),
    ("authorization.k8s.io", "subjectaccessreviews", ("create",)),
)
_CLUSTER_URLS = (("/version", "get"),)


class MeasuringRbacRefused(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Measuring identity scope refused")


def _require(value: bool) -> None:
    if not value:
        raise MeasuringRbacRefused() from None


def _role_name(value: object) -> str:
    """RBAC role names are path segments; bootstrap roles carry ``:``."""
    _require(type(value) is str and _ROLE.fullmatch(str(value)) is not None)
    return str(value)


def _name(value: object) -> str:
    _require(type(value) is str and _NAME.fullmatch(str(value)) is not None)
    return str(value)


@dataclass(frozen=True, slots=True)
class MeasuringScope:
    """The namespaces and names the bootstrap substituted into the clause 5 manifest."""

    service_account: str
    control_namespace: str
    protected_namespace: str
    probe_namespaces: tuple[str, ...]
    release_namespace: str
    # ``curie-release-probe-<campaign_id>``: the one release binding of this campaign.
    release_binding: str
    # The ADR 0236 request and reply objects in the control namespace.
    channel_rules: tuple[tuple[str, str, tuple[str, ...]], ...]
    # ConfigMaps the provider table names for the CNI and agent workloads.
    cni_configmaps: tuple[tuple[str, str], ...]
    # Provider policy collections of the provider table, read cluster-wide.
    provider_policies: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        for value in (
            self.service_account,
            self.control_namespace,
            self.protected_namespace,
            self.release_namespace,
            self.release_binding,
            *self.probe_namespaces,
        ):
            _name(value)
        _require(len(self.probe_namespaces) == 2)
        namespaces = {
            self.control_namespace,
            self.protected_namespace,
            self.release_namespace,
            *self.probe_namespaces,
        }
        _require(len(namespaces) == 5)
        _require(self.release_binding.startswith("curie-release-probe-"))
        _require(bool(self.channel_rules))
        for group, resource, verbs in self.channel_rules:
            _require(
                _GROUP.fullmatch(group) is not None and _RESOURCE.fullmatch(resource) is not None
            )
            _require(bool(verbs) and set(verbs) <= _VERBS)
        for namespace, name in self.cni_configmaps:
            _name(namespace)
            _name(name)
        for group, resource in self.provider_policies:
            _require(bool(group) and _GROUP.fullmatch(group) is not None)
            _require(_RESOURCE.fullmatch(resource) is not None and "/" not in resource)

    @property
    def user(self) -> str:
        return f"system:serviceaccount:{self.control_namespace}:{self.service_account}"

    @property
    def groups(self) -> tuple[str, ...]:
        """The groups the API server adds to every ServiceAccount token."""
        return (
            "system:serviceaccounts",
            f"system:serviceaccounts:{self.control_namespace}",
            "system:authenticated",
        )

    def pod_namespaces(self, *, release: bool) -> frozenset[str]:
        """Where the manifest lets the identity create pods."""
        base = {self.protected_namespace, *self.probe_namespaces}
        return frozenset(base | ({self.release_namespace} if release else set()))


def _atoms(namespace: str, rules: Sequence[tuple[str, str, tuple[str, ...]]]) -> set[Atom]:
    return {
        (namespace, group, resource, "", verb) for group, resource, verbs in rules for verb in verbs
    }


def manifest_atoms(scope: MeasuringScope, *, release: bool) -> frozenset[Atom]:
    """The clause 5 manifest as atomic rights; ``release`` adds the campaign's release Role."""
    atoms: set[Atom] = set()
    for namespace in scope.probe_namespaces:
        atoms |= _atoms(namespace, _PROBE_RULES)
    atoms |= _atoms(scope.protected_namespace, _PROTECTED_RULES)
    atoms |= _atoms(scope.control_namespace, scope.channel_rules)
    if release:
        atoms |= _atoms(scope.release_namespace, _RELEASE_RULES)
        atoms.add((scope.release_namespace, _RBAC, "rolebindings", scope.release_binding, "delete"))
    atoms |= _atoms("", _CLUSTER_READS)
    atoms |= _atoms("", [(group, resource, _READ) for group, resource in scope.provider_policies])
    atoms |= {
        (namespace, "", "configmaps", name, "get") for namespace, name in scope.cni_configmaps
    }
    atoms |= {("", "url", path, "", verb) for path, verb in _CLUSTER_URLS}
    return frozenset(atoms)


def _attributes(
    verb: str, group: str, resource: str, namespace: str = "", name: str = ""
) -> dict[str, str]:
    base, _, subresource = resource.partition("/")
    value = {"verb": verb, "group": group, "resource": base}
    optional = (("namespace", namespace), ("subresource", subresource), ("name", name))
    return value | {key: item for key, item in optional if item}


def probe_attributes(
    scope: MeasuringScope, namespaces: Sequence[str], *, release: bool
) -> tuple[dict[str, str], ...]:
    """The clause 5 probe list over every live namespace, plus cluster scope."""
    allowed_pods = scope.pod_namespaces(release=release)
    probes: list[dict[str, str]] = []
    for namespace in ("", *sorted(set(namespaces))):
        probes += [_attributes(verb, "", "secrets", namespace) for verb in _ALL_VERBS]
        for resource in ("pods/exec", "pods/attach", "pods/portforward"):
            probes += [_attributes(verb, "", resource, namespace) for verb in _CONNECT_VERBS]
        probes += [
            _attributes(verb, "", "pods/ephemeralcontainers", namespace)
            for verb in ("update", "patch")
        ]
        for resource in ("roles", "rolebindings", "clusterroles", "clusterrolebindings"):
            if namespace and resource.startswith("cluster"):
                continue
            probes += [_attributes(verb, _RBAC, resource, namespace) for verb in _RBAC_WRITE_VERBS]
        if namespace in (scope.protected_namespace, scope.release_namespace):
            probes += [
                _attributes(verb, "networking.k8s.io", "networkpolicies", namespace)
                for verb in _WRITE_VERBS
            ]
        if namespace not in allowed_pods:
            probes.append(_attributes("create", "", "pods", namespace))
        probes += [
            _attributes(verb, group, resource, namespace)
            for group, resource in _CONTROLLERS
            for verb in ("create", "update", "patch")
        ]
        probes.append(_attributes("create", "", "serviceaccounts/token", namespace))
    probes += [
        _attributes(verb, "", "nodes/proxy")
        for verb in ("get", "create", "update", "patch", "delete")
    ]
    probes += [
        _attributes("impersonate", "", resource)
        for resource in ("users", "groups", "serviceaccounts")
    ]
    probes += [
        _attributes("impersonate", "authentication.k8s.io", resource)
        for resource in ("uids", "userextras")
    ]
    return tuple(probes)


class MeasuringRbacReader(Protocol):
    """Reads under the measuring identity's own clause 5 rights."""

    async def review(
        self, user: str, groups: Sequence[str], attributes: Mapping[str, str]
    ) -> bool | None: ...

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        """``namespaces``, ``rolebindings`` or ``clusterrolebindings``, every page."""
        ...

    async def read_role(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None: ...


def _subject_form(binding: dict[str, Any], scope: MeasuringScope) -> str | None:
    """``direct`` when a subject names the identity, ``group`` for one of its groups."""
    form = None
    for subject in binding.get("subjects") or []:
        kind, name = subject.get("kind"), subject.get("name")
        if (
            kind == "ServiceAccount"
            and name == scope.service_account
            and subject.get("namespace", binding["metadata"].get("namespace"))
            == scope.control_namespace
        ) or (kind == "User" and name == scope.user):
            return "direct"
        if kind == "Group" and name in scope.groups:
            form = "group"
    return form


def _rule_atoms(namespace: str, rule: dict[str, Any]) -> set[Atom]:
    verbs = rule.get("verbs") or []
    _require(type(verbs) is list and bool(verbs) and "*" not in verbs)
    urls = rule.get("nonResourceURLs") or []
    if urls:
        # Non-resource rights exist only cluster-wide, and never with resources.
        _require(namespace == "" and not rule.get("resources") and not rule.get("apiGroups"))
        _require(all(url in _AMBIENT_URLS or "*" not in url for url in urls))
        return {("", "url", url, "", verb) for url in urls for verb in verbs}
    groups: list[str] = rule.get("apiGroups") or []
    resources: list[str] = rule.get("resources") or []
    _require(type(groups) is list and type(resources) is list and bool(resources))
    _require("*" not in groups and not any("*" in item for item in resources))
    names = rule.get("resourceNames") or [""]
    _require(type(names) is list and all(type(item) is str for item in names))
    return {
        (namespace, group, resource, name, verb)
        for group in groups
        for resource in resources
        for name in names
        for verb in verbs
    }


async def _bound_atoms(
    reader: MeasuringRbacReader, scope: MeasuringScope
) -> tuple[frozenset[Atom], frozenset[Atom]]:
    """The atoms bound to the identity directly, and those bound to its groups."""
    found: dict[str, set[Atom]] = {"direct": set(), "group": set()}
    for collection in ("rolebindings", "clusterrolebindings"):
        items = await reader.list_items(collection)
        _require(items is not None)
        assert items is not None
        for binding in items:
            form = _subject_form(binding, scope)
            if form is None:
                continue
            atoms = found[form]
            namespace = (
                binding["metadata"].get("namespace", "") if collection == "rolebindings" else ""
            )
            ref = binding["roleRef"]
            _require(ref.get("apiGroup") == _RBAC and ref.get("kind") in ("Role", "ClusterRole"))
            role = await reader.read_role(
                ref["kind"], namespace if ref["kind"] == "Role" else "", _role_name(ref["name"])
            )
            _require(role is not None)
            assert role is not None
            for rule in role.get("rules") or []:
                atoms |= _rule_atoms(namespace, rule)
    return frozenset(found["direct"]), frozenset(found["group"])


async def _reviews(
    reader: MeasuringRbacReader, scope: MeasuringScope, probes: Sequence[dict[str, str]]
) -> list[bool | None]:
    gate = asyncio.Semaphore(_CONCURRENCY)

    async def one(attributes: dict[str, str]) -> bool | None:
        async with gate:
            try:
                return await reader.review(scope.user, scope.groups, attributes)
            except Exception:  # noqa: BLE001 -- an unanswered review is unavailable.
                return None

    return list(await asyncio.gather(*(one(item) for item in probes)))


async def check_least_privilege(
    reader: MeasuringRbacReader, scope: MeasuringScope, *, release_allowed: bool
) -> str | None:
    """``None`` when the measuring identity holds exactly the manifest; else the reason.

    ``release_allowed`` accepts the campaign's release binding; without it the
    binding must be absent.
    """
    try:
        bound, grouped = await _bound_atoms(reader, scope)
    except Exception:  # noqa: BLE001 -- unreadable or unparseable bindings never pass.
        return "measuring_bindings_unavailable"
    if not grouped <= _AMBIENT:
        return "measuring_bindings_differ"
    shapes = [manifest_atoms(scope, release=False)]
    if release_allowed:
        shapes.append(manifest_atoms(scope, release=True))
    if bound not in shapes:
        return "measuring_bindings_differ"
    release = release_allowed and bound == shapes[-1]
    try:
        namespaces = await reader.list_items("namespaces")
    except Exception:  # noqa: BLE001 -- an unreadable namespace list is unavailable.
        namespaces = None
    if namespaces is None:
        return "measuring_review_unavailable"
    try:
        names = [_name(item["metadata"]["name"]) for item in namespaces]
    except (MeasuringRbacRefused, KeyError, TypeError):
        return "measuring_review_unavailable"
    results = await _reviews(reader, scope, probe_attributes(scope, names, release=release))
    if any(result is True for result in results):
        return "measuring_review_allowed"
    if any(result is not False for result in results):
        return "measuring_review_unavailable"
    return None


def measuring_scope_document(scope: MeasuringScope) -> dict[str, Any]:
    """The rendered RBAC set's ``measuring_identity`` member for ``scope``."""
    return {
        "service_account": scope.service_account,
        "control_namespace": scope.control_namespace,
        "protected_namespace": scope.protected_namespace,
        "probe_namespaces": list(scope.probe_namespaces),
        "release_namespace": scope.release_namespace,
        "channel_rules": [[g, r, list(v)] for g, r, v in scope.channel_rules],
        "cni_configmaps": [list(item) for item in scope.cni_configmaps],
        "provider_policies": [list(item) for item in scope.provider_policies],
    }


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [key for key, _value in pairs]
    _require(len(keys) == len(set(keys)))
    return dict(pairs)


def rbac_set_scope(rbac_set: bytes, *, rbac_sha256: str, campaign_id: str) -> MeasuringScope:
    """The measuring identity's scope, read from the RBAC set G's ``rbac_sha256`` names.

    Clause 6: the provisioner renders the installation's RBAC set, content
    addressed and covered by G. A caller therefore cannot point the check at
    another ServiceAccount or namespace set. The release binding is named by
    this campaign.
    """
    try:
        _require(type(rbac_set) is bytes and hashlib.sha256(rbac_set).hexdigest() == rbac_sha256)
        _require(_UUID.fullmatch(campaign_id) is not None)
        value = json.loads(rbac_set.decode("ascii"), object_pairs_hook=_no_duplicates)
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        _require(canonical.encode("ascii") == rbac_set)
        document = value["measuring_identity"]
        _require(type(document) is dict and sorted(document) == sorted(_SCOPE_MEMBERS))
        scope = MeasuringScope(
            service_account=document["service_account"],
            control_namespace=document["control_namespace"],
            protected_namespace=document["protected_namespace"],
            probe_namespaces=tuple(document["probe_namespaces"]),
            release_namespace=document["release_namespace"],
            release_binding="curie-release-probe-" + campaign_id,
            channel_rules=tuple((g, r, tuple(v)) for g, r, v in document["channel_rules"]),
            cni_configmaps=tuple((ns, name) for ns, name in document["cni_configmaps"]),
            provider_policies=tuple((g, r) for g, r in document["provider_policies"]),
        )
        _require(measuring_scope_document(scope) == document)
    except MeasuringRbacRefused:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError):
        raise MeasuringRbacRefused() from None
    return scope
