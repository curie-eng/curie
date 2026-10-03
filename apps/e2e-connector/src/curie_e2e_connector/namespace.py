"""Create and delete one run's namespace on the test cluster.

The test cluster's admission policy (issue #3243) is the authority for the
prefix and the owner label. This module still refuses a namespace that does
not belong to the signed run before it sends a delete, so a confused caller
never spends the delete on someone else's namespace.
"""

from __future__ import annotations

import ipaddress
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from curie_e2e_connector.contract import (
    EXPIRES_ANNOTATION,
    OWNER_LABEL,
    POD_SECURITY_LABEL,
    REFUSAL_MISCONFIGURED,
    REFUSAL_NO_RUN,
    REFUSAL_NOT_OWNED,
    RUN_LABEL,
    WORK_ITEM_LABEL,
)
from curie_e2e_connector.kube import ClusterApi, ClusterError

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_PREFIX = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?-$")
_LABEL_VALUE = re.compile(r"^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$")
_MAX_ALLOWS = 8
_MIN_TTL = 60
_MAX_TTL = 24 * 60 * 60


class Caller:
    def __init__(self, run: str, work_item: str) -> None:
        self.run = run
        self.work_item = work_item


class Install:
    def __init__(
        self,
        *,
        namespace_prefix: str,
        owner_label_key: str,
        owner_label_value: str,
        service_account: str,
        service_account_namespace: str,
        worker_cluster_role: str,
        ttl_seconds: int,
        pod_security: str,
    ) -> None:
        self.namespace_prefix = namespace_prefix
        self.owner_label_key = owner_label_key
        self.owner_label_value = owner_label_value
        self.service_account = service_account
        self.service_account_namespace = service_account_namespace
        self.worker_cluster_role = worker_cluster_role
        self.ttl_seconds = ttl_seconds
        self.pod_security = pod_security

    def validate(self) -> None:
        if not _PREFIX.fullmatch(self.namespace_prefix):
            raise ClusterError(f"{REFUSAL_MISCONFIGURED}: namespace prefix is not a DNS label")
        if len(self.namespace_prefix) > 27:
            raise ClusterError(f"{REFUSAL_MISCONFIGURED}: namespace prefix is too long")
        if self.owner_label_key != OWNER_LABEL:
            raise ClusterError(f"{REFUSAL_MISCONFIGURED}: owner label key is not {OWNER_LABEL}")
        if not _LABEL_VALUE.fullmatch(self.owner_label_value):
            raise ClusterError(f"{REFUSAL_MISCONFIGURED}: owner label value is empty or invalid")
        for name, value in (
            ("service account", self.service_account),
            ("service account namespace", self.service_account_namespace),
            ("worker cluster role", self.worker_cluster_role),
        ):
            if not value or "/" in value:
                raise ClusterError(f"{REFUSAL_MISCONFIGURED}: {name} is empty or invalid")
        if self.pod_security not in ("baseline", "restricted"):
            raise ClusterError(
                f"{REFUSAL_MISCONFIGURED}: pod security must be baseline or restricted"
            )
        if not _MIN_TTL <= self.ttl_seconds <= _MAX_TTL:
            raise ClusterError(f"{REFUSAL_MISCONFIGURED}: ttl is outside {_MIN_TTL}..{_MAX_TTL}")


def require_caller(run: str, work_item: str) -> Caller:
    if not run or not work_item:
        raise ClusterError(
            f"{REFUSAL_NO_RUN}: the caller token has no run and work item. "
            "env_create and env_destroy need the signed pair."
        )
    if _UUID.fullmatch(run) is None or _UUID.fullmatch(work_item) is None:
        raise ClusterError(
            f"{REFUSAL_NO_RUN}: run and work item must be lowercase hyphenated uuids"
        )
    return Caller(run, work_item)


def namespace_name(install: Install, caller: Caller) -> str:
    name = f"{install.namespace_prefix}{caller.run}"
    if len(name) > 63:
        raise ClusterError(f"{REFUSAL_MISCONFIGURED}: namespace name exceeds 63 characters")
    return name


def _labels(install: Install, caller: Caller) -> dict[str, str]:
    return {
        install.owner_label_key: install.owner_label_value,
        RUN_LABEL: caller.run,
        WORK_ITEM_LABEL: caller.work_item,
        POD_SECURITY_LABEL: install.pod_security,
    }


def _owned(install: Install, caller: Caller, metadata: dict[str, Any]) -> bool:
    labels = metadata.get("labels") or {}
    if not isinstance(labels, dict):
        return False
    return (
        labels.get(install.owner_label_key) == install.owner_label_value
        and labels.get(RUN_LABEL) == caller.run
        and labels.get(WORK_ITEM_LABEL) == caller.work_item
    )


def _refuse_not_owned() -> None:
    raise ClusterError(
        f"{REFUSAL_NOT_OWNED}: env_destroy deletes only the namespace this run created"
    )


def allow_policies(allow: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Default deny, plus one policy per rule the caller declared."""

    rules = allow or []
    if len(rules) > _MAX_ALLOWS:
        raise ClusterError(f"env_create accepts at most {_MAX_ALLOWS} allow rules")
    policies = [_default_deny()]
    dns_added = False
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise ClusterError(f"allow rule {index} must be an object")
        if rule.get("dns") is True:
            extra = set(rule) - {"dns"}
            if extra:
                raise ClusterError(f"allow rule {index} mixes dns with other fields")
            if not dns_added:
                policies.append(_dns_allow())
                dns_added = True
            continue
        cidr = rule.get("cidr")
        port = rule.get("port")
        protocol = rule.get("protocol", "TCP")
        extra = set(rule) - {"cidr", "port", "protocol"}
        if extra or not isinstance(cidr, str) or not isinstance(port, int):
            raise ClusterError(
                f"allow rule {index} needs cidr and port, or dns: true, and nothing else"
            )
        if protocol not in ("TCP", "UDP"):
            raise ClusterError(f"allow rule {index} protocol must be TCP or UDP")
        if not 1 <= port <= 65535:
            raise ClusterError(f"allow rule {index} port is out of range")
        try:
            ipaddress.ip_network(cidr, strict=True)
        except ValueError as exc:
            raise ClusterError(f"allow rule {index} cidr is not a network") from exc
        policies.append(_cidr_allow(index, cidr, port, str(protocol)))
    return policies


def _default_deny() -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": "default-deny"},
        "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]},
    }


def _dns_allow() -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": "allow-dns"},
        "spec": {
            "podSelector": {},
            "policyTypes": ["Egress"],
            "egress": [
                {
                    "ports": [
                        {"protocol": "UDP", "port": 53},
                        {"protocol": "TCP", "port": 53},
                    ]
                }
            ],
        },
    }


def _cidr_allow(index: int, cidr: str, port: int, protocol: str) -> dict[str, Any]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": f"allow-{index}"},
        "spec": {
            "podSelector": {},
            "policyTypes": ["Egress"],
            "egress": [
                {
                    "to": [{"ipBlock": {"cidr": cidr}}],
                    "ports": [{"protocol": protocol, "port": port}],
                }
            ],
        },
    }


def _quota() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {"name": "sandbox"},
        "spec": {
            "hard": {
                "pods": "20",
                "requests.cpu": "4",
                "requests.memory": "8Gi",
                "limits.cpu": "8",
                "limits.memory": "16Gi",
                "persistentvolumeclaims": "2",
                "services": "10",
                "count/jobs.batch": "20",
            }
        },
    }


def _limit_range() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "LimitRange",
        "metadata": {"name": "sandbox"},
        "spec": {
            "limits": [
                {
                    "type": "Container",
                    "default": {"cpu": "1", "memory": "512Mi"},
                    "defaultRequest": {"cpu": "100m", "memory": "128Mi"},
                    "max": {"cpu": "2", "memory": "2Gi"},
                }
            ]
        },
    }


def _role_binding(namespace: str, install: Install) -> dict[str, Any]:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "e2e-connector", "namespace": namespace},
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": install.service_account,
                "namespace": install.service_account_namespace,
            }
        ],
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": install.worker_cluster_role,
        },
    }


def _namespace_body(name: str, install: Install, caller: Caller, expires: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": _labels(install, caller),
            "annotations": {EXPIRES_ANNOTATION: expires},
        },
    }


def _expires(now: datetime, ttl: int) -> str:
    moment = now.astimezone(UTC) + timedelta(seconds=ttl)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _status(
    cluster: ClusterApi, method: str, path: str, body: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    code, payload = cluster.request(method, path, body)
    if code in (401, 403):
        raise ClusterError(f"the test cluster refused {method} {path} ({code})")
    return code, payload


def _ensure(cluster: ClusterApi, path: str, body: dict[str, Any]) -> None:
    code, _payload = _status(cluster, "POST", path, body)
    if code in (200, 201, 409):
        return
    raise ClusterError(f"the test cluster refused to create {body.get('kind')} ({code})")


def _children(
    cluster: ClusterApi, namespace: str, install: Install, policies: list[dict[str, Any]]
) -> None:
    _ensure(
        cluster,
        f"/apis/rbac.authorization.k8s.io/v1/namespaces/{namespace}/rolebindings",
        _role_binding(namespace, install),
    )
    _ensure(cluster, f"/api/v1/namespaces/{namespace}/resourcequotas", _quota())
    _ensure(cluster, f"/api/v1/namespaces/{namespace}/limitranges", _limit_range())
    for policy in policies:
        _ensure(
            cluster,
            f"/apis/networking.k8s.io/v1/namespaces/{namespace}/networkpolicies",
            policy,
        )


def create_environment(
    cluster: ClusterApi,
    install: Install,
    caller: Caller,
    allow: list[dict[str, Any]] | None,
    ttl_seconds: int | None,
    *,
    now: datetime,
) -> dict[str, Any]:
    install.validate()
    ttl = install.ttl_seconds if ttl_seconds is None else ttl_seconds
    if ttl > install.ttl_seconds or ttl < _MIN_TTL:
        raise ClusterError(
            f"ttl_seconds must be between {_MIN_TTL} and the installation cap {install.ttl_seconds}"
        )
    policies = allow_policies(allow)
    name = namespace_name(install, caller)
    expires = _expires(now, ttl)
    code, existing = _status(cluster, "GET", f"/api/v1/namespaces/{name}")
    if code == 200:
        metadata = existing.get("metadata") or {}
        if not isinstance(metadata, dict) or not _owned(install, caller, metadata):
            _refuse_not_owned()
        _children(cluster, name, install, policies)
        annotations = metadata.get("annotations") or {}
        stored = annotations.get(EXPIRES_ANNOTATION) if isinstance(annotations, dict) else None
        return {
            "namespace": name,
            "expires_at": stored or expires,
            "run": caller.run,
            "work_item": caller.work_item,
        }
    if code != 404:
        raise ClusterError(f"the test cluster did not answer a namespace read ({code})")
    created, _payload = _status(
        cluster,
        "POST",
        "/api/v1/namespaces",
        _namespace_body(name, install, caller, expires),
    )
    if created == 409:
        return create_environment(cluster, install, caller, allow, ttl, now=now)
    if created not in (200, 201):
        raise ClusterError(f"the test cluster refused the namespace ({created})")
    _children(cluster, name, install, policies)
    return {
        "namespace": name,
        "expires_at": expires,
        "run": caller.run,
        "work_item": caller.work_item,
    }


def destroy_environment(
    cluster: ClusterApi, install: Install, caller: Caller, namespace: str
) -> dict[str, Any]:
    install.validate()
    expected = namespace_name(install, caller)
    if namespace != expected or not namespace.startswith(install.namespace_prefix):
        _refuse_not_owned()
    code, existing = _status(cluster, "GET", f"/api/v1/namespaces/{namespace}")
    if code == 404:
        _refuse_not_owned()
    if code != 200:
        raise ClusterError(f"the test cluster did not answer a namespace read ({code})")
    metadata = existing.get("metadata") or {}
    if not isinstance(metadata, dict) or not _owned(install, caller, metadata):
        _refuse_not_owned()
    deleted, _payload = _status(cluster, "DELETE", f"/api/v1/namespaces/{namespace}")
    if deleted not in (200, 202):
        raise ClusterError(f"the test cluster refused the namespace delete ({deleted})")
    return {"namespace": namespace, "deleted": True}
