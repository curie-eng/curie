"""Measuring identity least-privilege self-check, @spec MANAGED-CNI-5 (ADR 0238).

A fake RBAC reader stands in for the API server: its SubjectAccessReviews
answer from the same bindings it lists, plus any group-granted rights a test
adds. The manifest-exact identity passes; every right beyond the manifest, any
missing manifest right, a wildcard, the release binding outside a campaign and
any unanswered review refuses.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
from collections.abc import Mapping, Sequence
from typing import Any

import pytest

NAMESPACES = ("control", "prot", "probe-a", "probe-foreign", "release", "kube-system", "default")
RBAC = "rbac.authorization.k8s.io"


def module() -> Any:
    name = "curie_protected_hooks.measuring_identity_rbac"
    assert importlib.util.find_spec(name) is not None, "least-privilege check not implemented"
    return importlib.import_module(name)


def scope(**changes: Any) -> Any:
    values: dict[str, Any] = {
        "service_account": "measuring",
        "control_namespace": "control",
        "protected_namespace": "prot",
        "probe_namespaces": ("probe-a", "probe-foreign"),
        "release_namespace": "release",
        "release_binding": "curie-release-probe-c1",
        "channel_rules": (("", "configmaps", ("create", "get", "list", "watch")),),
        "cni_configmaps": (("kube-system", "calico-config"),),
        "provider_policies": (("crd.projectcalico.org", "networkpolicies"),),
    }
    return module().MeasuringScope(**(values | changes))


def rule(groups: list[str], resources: list[str], verbs: list[str], **extra: Any) -> dict[str, Any]:
    return {"apiGroups": groups, "resources": resources, "verbs": verbs, **extra}


SA = [{"kind": "ServiceAccount", "name": "measuring", "namespace": "control"}]


def manifest(*, release: bool) -> list[tuple[str, str, str, list[dict[str, Any]]]]:
    """(binding kind, namespace, role name, rules) as an administrator would install them."""
    probe = [
        rule([""], ["pods"], ["create", "get", "list", "watch", "patch", "delete"]),
        rule([""], ["pods/log", "pods/status"], ["get"]),
        rule(
            ["networking.k8s.io"],
            ["networkpolicies"],
            ["create", "get", "list", "watch", "update", "patch", "delete"],
        ),
    ]
    read = ["get", "list", "watch"]
    roles: list[tuple[str, str, str, list[dict[str, Any]]]] = [
        ("Role", "probe-a", "curie-measuring", probe),
        ("Role", "probe-foreign", "curie-measuring", probe),
        (
            "Role",
            "prot",
            "curie-measuring",
            [
                rule([""], ["pods"], ["create", "get", "list", "watch", "delete"]),
                rule([""], ["pods/log", "pods/status"], ["get"]),
            ],
        ),
        (
            "Role",
            "control",
            "curie-measuring-channel",
            [
                rule([""], ["configmaps"], ["get", "list", "watch"]),
                rule([""], ["configmaps"], ["create", "get"]),
            ],
        ),
        (
            "Role",
            "kube-system",
            "curie-measuring-cni",
            [rule([""], ["configmaps"], ["get"], resourceNames=["calico-config"])],
        ),
        (
            "ClusterRole",
            "",
            "curie-measuring-reads",
            [
                rule([""], ["nodes", "namespaces"], read),
                rule([""], ["pods"], read),
                rule(["apps"], ["daemonsets", "deployments"], read),
                rule(["networking.k8s.io"], ["networkpolicies"], read),
                rule(["crd.projectcalico.org"], ["networkpolicies"], read),
                rule(
                    ["admissionregistration.k8s.io"],
                    [
                        "validatingwebhookconfigurations",
                        "mutatingwebhookconfigurations",
                        "validatingadmissionpolicies",
                        "validatingadmissionpolicybindings",
                    ],
                    read,
                ),
                rule(
                    [RBAC], ["roles", "rolebindings", "clusterroles", "clusterrolebindings"], read
                ),
                {"nonResourceURLs": ["/version"], "verbs": ["get"]},
            ],
        ),
        (
            "ClusterRole",
            "",
            "curie-measuring-reviews",
            [rule(["authorization.k8s.io"], ["subjectaccessreviews"], ["create"])],
        ),
    ]
    if release:
        roles.append(
            (
                "Role",
                "release",
                "curie-release-probe-c1",
                [
                    rule([""], ["pods"], ["create", "get", "list", "watch", "delete"]),
                    rule([""], ["pods/log"], ["get"]),
                    rule(
                        [RBAC],
                        ["rolebindings"],
                        ["delete"],
                        resourceNames=["curie-release-probe-c1"],
                    ),
                ],
            )
        )
    return roles


class Reader:
    """Answers reviews from its own bindings, like the API server's RBAC authorizer."""

    def __init__(self, roles: list[tuple[str, str, str, list[dict[str, Any]]]]) -> None:
        self.roles = roles
        self.group_grants: set[tuple[str, str, str, str]] = set()
        self.unanswered: set[tuple[str, str, str, str]] = set()
        self.unavailable: set[str] = set()
        self.extra_bindings: list[tuple[str, str, dict[str, Any]]] = []
        self.reviews: list[Mapping[str, str]] = []

    def _bindings(self, kind: str) -> list[dict[str, Any]]:
        items = []
        for role_kind, namespace, name, _rules in self.roles:
            binding_kind = "rolebindings" if role_kind == "Role" else "clusterrolebindings"
            if binding_kind != kind:
                continue
            meta = {"name": name} | ({"namespace": namespace} if namespace else {})
            items.append(
                {
                    "metadata": meta,
                    "roleRef": {"apiGroup": RBAC, "kind": role_kind, "name": name},
                    "subjects": SA,
                }
            )
        items += [
            binding for binding_kind, _ns, binding in self.extra_bindings if binding_kind == kind
        ]
        # An unrelated binding never counts.
        items.append(
            {
                "metadata": {"name": "other", "namespace": "default"},
                "roleRef": {"apiGroup": RBAC, "kind": "ClusterRole", "name": "admin"},
                "subjects": [{"kind": "ServiceAccount", "name": "other", "namespace": "default"}],
            }
        )
        return items

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        if collection in self.unavailable:
            return None
        if collection == "namespaces":
            return [{"metadata": {"name": name}} for name in NAMESPACES]
        return self._bindings(collection)

    async def read_role(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None:
        for role_kind, role_namespace, role_name, rules in self.roles:
            if (role_kind, role_namespace, role_name) == (kind, namespace, name):
                return {"rules": rules}
        return None

    def _granted(self, attributes: Mapping[str, str]) -> bool:
        resource = attributes["resource"] + (
            "/" + attributes["subresource"] if attributes.get("subresource") else ""
        )
        namespace, name = attributes.get("namespace", ""), attributes.get("name", "")
        for role_kind, role_namespace, _name, rules in self.roles:
            if role_kind == "Role" and role_namespace != namespace:
                continue
            for item in rules:
                if (
                    attributes["group"] in item.get("apiGroups", [])
                    and resource in item.get("resources", [])
                    and attributes["verb"] in item["verbs"]
                    and (not item.get("resourceNames") or name in item["resourceNames"])
                ):
                    return True
        return (namespace, attributes["group"], resource, attributes["verb"]) in self.group_grants

    async def review(
        self, user: str, groups: Sequence[str], attributes: Mapping[str, str]
    ) -> bool | None:
        assert user == "system:serviceaccount:control:measuring"
        assert "system:serviceaccounts:control" in groups
        self.reviews.append(attributes)
        key = (
            attributes.get("namespace", ""),
            attributes["group"],
            attributes["resource"]
            + ("/" + attributes["subresource"] if attributes.get("subresource") else ""),
            attributes["verb"],
        )
        if key in self.unanswered:
            return None
        return self._granted(attributes)


def check(reader: Reader, *, release_allowed: bool, **changes: Any) -> str | None:
    return asyncio.run(
        module().check_least_privilege(reader, scope(**changes), release_allowed=release_allowed)
    )


@pytest.mark.parametrize("release_allowed", [False, True])
def test_the_manifest_exact_identity_passes(release_allowed: bool) -> None:
    reader = Reader(manifest(release=False))
    assert check(reader, release_allowed=release_allowed) is None
    assert reader.reviews


def test_the_release_binding_is_allowed_only_while_a_campaign_holds_it() -> None:
    assert check(Reader(manifest(release=True)), release_allowed=True) is None
    # At Readiness issuance the binding must already be revoked.
    assert (
        check(Reader(manifest(release=True)), release_allowed=False) == "measuring_bindings_differ"
    )


def test_a_release_binding_for_another_campaign_refuses() -> None:
    reader = Reader(manifest(release=True))
    assert (
        check(reader, release_allowed=True, release_binding="curie-release-probe-c2")
        == "measuring_bindings_differ"
    )


@pytest.mark.parametrize(
    "extra",
    [
        ("Role", "control", "extra", [rule([""], ["secrets"], ["get"])]),
        ("Role", "prot", "extra", [rule(["networking.k8s.io"], ["networkpolicies"], ["create"])]),
        ("Role", "default", "extra", [rule([""], ["pods"], ["create"])]),
        ("ClusterRole", "", "extra", [rule([""], ["nodes/proxy"], ["get"])]),
        ("ClusterRole", "", "extra", [rule([""], ["configmaps"], ["get"])]),
        (
            "Role",
            "kube-system",
            "extra",
            [rule([""], ["configmaps"], ["get"], resourceNames=["other"])],
        ),
    ],
)
def test_an_extra_bound_right_refuses(extra: Any) -> None:
    reader = Reader([*manifest(release=False), extra])
    assert check(reader, release_allowed=True) == "measuring_bindings_differ"


def test_a_missing_manifest_right_refuses() -> None:
    roles = [item for item in manifest(release=False) if item[2] != "curie-measuring-reviews"]
    assert check(Reader(roles), release_allowed=True) == "measuring_bindings_differ"


@pytest.mark.parametrize(
    "wildcard",
    [
        rule([""], ["pods"], ["*"]),
        rule(["*"], ["pods"], ["get"]),
        rule([""], ["*"], ["get"]),
        {"nonResourceURLs": ["*"], "verbs": ["get"]},
    ],
)
def test_a_wildcard_rule_refuses(wildcard: dict[str, Any]) -> None:
    reader = Reader([*manifest(release=False), ("ClusterRole", "", "extra", [wildcard])])
    assert check(reader, release_allowed=True) == "measuring_bindings_unavailable"


@pytest.mark.parametrize(
    "grant",
    [
        ("kube-system", "", "secrets", "get"),
        ("", "", "secrets", "list"),
        ("probe-a", "", "pods/exec", "create"),
        ("prot", "", "pods/attach", "create"),
        ("probe-a", "", "pods/portforward", "create"),
        ("probe-a", "", "pods/ephemeralcontainers", "patch"),
        ("", "", "nodes/proxy", "get"),
        ("", RBAC, "clusterrolebindings", "create"),
        ("release", RBAC, "rolebindings", "bind"),
        ("", "", "serviceaccounts", "impersonate"),
        ("", "", "users", "impersonate"),
        ("", "", "groups", "impersonate"),
        ("release", "networking.k8s.io", "networkpolicies", "patch"),
        ("prot", "networking.k8s.io", "networkpolicies", "delete"),
        ("default", "", "pods", "create"),
        ("kube-system", "", "pods", "create"),
        ("control", "", "pods", "create"),
    ],
)
def test_a_group_granted_right_beyond_the_manifest_refuses(
    grant: tuple[str, str, str, str],
) -> None:
    # Bindings to a group carry no subject naming the identity; only the review sees them.
    reader = Reader(manifest(release=False))
    reader.group_grants.add(grant)
    assert check(reader, release_allowed=True) == "measuring_review_allowed"


def test_release_pod_creation_is_probed_once_the_binding_is_gone() -> None:
    reader = Reader(manifest(release=False))
    reader.group_grants.add(("release", "", "pods", "create"))
    assert check(reader, release_allowed=False) == "measuring_review_allowed"
    # While the campaign holds the release binding, release pod creation is the manifest.
    held = Reader(manifest(release=True))
    assert check(held, release_allowed=True) is None


def test_an_unanswered_review_refuses() -> None:
    reader = Reader(manifest(release=False))
    reader.unanswered.add(("kube-system", "", "secrets", "get"))
    assert check(reader, release_allowed=True) == "measuring_review_unavailable"


@pytest.mark.parametrize("collection", ["rolebindings", "clusterrolebindings"])
def test_unreadable_bindings_refuse(collection: str) -> None:
    reader = Reader(manifest(release=False))
    reader.unavailable.add(collection)
    assert check(reader, release_allowed=True) == "measuring_bindings_unavailable"


def test_unreadable_namespaces_refuse() -> None:
    reader = Reader(manifest(release=False))
    reader.unavailable.add("namespaces")
    assert check(reader, release_allowed=True) == "measuring_review_unavailable"


def test_an_unreadable_bound_role_refuses() -> None:
    reader = Reader(manifest(release=False))
    reader.extra_bindings.append(
        (
            "clusterrolebindings",
            "",
            {
                "metadata": {"name": "ghost"},
                "roleRef": {"apiGroup": RBAC, "kind": "ClusterRole", "name": "ghost"},
                "subjects": SA,
            },
        )
    )
    assert check(reader, release_allowed=True) == "measuring_bindings_unavailable"


def test_a_cluster_role_bound_in_one_namespace_is_scoped_to_it() -> None:
    reader = Reader(manifest(release=False))
    # A ClusterRole granting pod creation, bound by a RoleBinding in ``default``.
    reader.roles.append(("ClusterRole", "", "edit-lite", [rule([""], ["pods"], ["create"])]))
    reader.extra_bindings.append(
        (
            "rolebindings",
            "default",
            {
                "metadata": {"name": "edit-lite", "namespace": "default"},
                "roleRef": {"apiGroup": RBAC, "kind": "ClusterRole", "name": "edit-lite"},
                "subjects": [
                    {"kind": "ServiceAccount", "name": "measuring", "namespace": "control"}
                ],
            },
        )
    )
    # The ClusterRole's own (unbound) binding appears too; both are extra rights.
    assert check(reader, release_allowed=True) == "measuring_bindings_differ"


def test_the_probe_list_covers_clause_5() -> None:
    probes = module().probe_attributes(scope(), NAMESPACES, release=False)
    keys = {
        (p.get("namespace", ""), p["group"], p["resource"], p.get("subresource", ""), p["verb"])
        for p in probes
    }
    for namespace in ("", *NAMESPACES):
        for verb in (
            "get",
            "list",
            "watch",
            "create",
            "update",
            "patch",
            "delete",
            "deletecollection",
        ):
            assert (namespace, "", "secrets", "", verb) in keys
        assert (namespace, "", "pods", "exec", "create") in keys
        assert (namespace, RBAC, "rolebindings", "", "create") in keys
    assert ("", "", "nodes", "proxy", "get") in keys
    assert ("", RBAC, "clusterroles", "", "escalate") in keys
    for resource in ("users", "groups", "serviceaccounts"):
        assert ("", "", resource, "", "impersonate") in keys
    for namespace in ("prot", "release"):
        assert (namespace, "networking.k8s.io", "networkpolicies", "", "create") in keys
    # Pod creation is probed everywhere the manifest does not list it.
    created = {k[0] for k in keys if k[1:] == ("", "pods", "", "create")}
    assert created == {"", "control", "release", "kube-system", "default"}
    held = module().probe_attributes(scope(), NAMESPACES, release=True)
    created_while_held = {
        p.get("namespace", "")
        for p in held
        if (p["resource"], p.get("subresource", ""), p["verb"]) == ("pods", "", "create")
    }
    assert created_while_held == {
        "",
        "control",
        "kube-system",
        "default",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"probe_namespaces": ("probe-a",)},
        {"probe_namespaces": ("probe-a", "prot")},
        {"release_binding": "other-binding"},
        {"channel_rules": ()},
        {"channel_rules": (("", "configmaps", ("*",)),)},
        {"service_account": "Measuring"},
        {"provider_policies": (("", "networkpolicies"),)},
    ],
)
def test_an_invalid_scope_refuses(changes: dict[str, Any]) -> None:
    with pytest.raises(module().MeasuringRbacRefused):
        scope(**changes)


@pytest.mark.parametrize("collection", ["namespaces", "rolebindings"])
def test_a_reader_that_raises_refuses(collection: str) -> None:
    reader = Reader(manifest(release=False))

    async def raising(name: str) -> Any:
        if name == collection:
            raise RuntimeError("refused by the bounded client")
        return await Reader.list_items(reader, name)

    reader.list_items = raising  # type: ignore[method-assign]
    assert check(reader, release_allowed=True) in (
        "measuring_review_unavailable",
        "measuring_bindings_unavailable",
    )
