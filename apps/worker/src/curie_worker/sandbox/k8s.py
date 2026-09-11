"""Kubernetes access for the sandbox substrate.

``SandboxClient`` is the seam the substrate is written against; the
``KubernetesSandboxClient`` implementation drives the agent-sandbox v0.5.0 CRDs
(core group ``agents.x-k8s.io`` for ``Sandbox``, extensions group
``extensions.agents.x-k8s.io`` for ``SandboxClaim``) via the official client's
CustomObjectsApi. Unit tests use an in-memory fake of the protocol (the K8s
control plane is an external service); the real implementation is exercised by
the k8scratch e2e test.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from aci_protocol import BootEnv
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from ..attachments import ATTACHMENTS_REF_ENV
from ..workspace import WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV
from .types import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ClaimView,
    OperatingMode,
    QuotaRejection,
    SandboxView,
    filter_agent_child_env,
)

CORE_GROUP = "agents.x-k8s.io"
CORE_VERSION = "v1beta1"
EXT_GROUP = "extensions.agents.x-k8s.io"
EXT_VERSION = "v1beta1"

# Per-claim env with no containerName reaches only the FIRST main container (the
# agent-sandbox Overrides policy). The bundle ref must additionally reach the
# init containers that fetch and extract the bundle, or a Kubernetes runner boots
# an empty plugin dir. These names MUST match the init containers the chart's
# SandboxTemplate declares (charts/curie/templates/agent-sandbox.yaml).
#
# Named from the ONE declaration in ``aci_protocol.BootEnv`` (#488, ADR-0049),
# never retyped: this substrate is the same consumer as the boot contract, so a
# local literal would drift silently on a rename -- the sandbox would still boot
# and answer, with the bundle simply absent.
BUNDLE_REF_ENV = BootEnv.env_key("bundle_ref")
BUNDLE_INIT_CONTAINERS = ("bundle-fetch", "bundle-extract")
WORKSPACE_INIT_CONTAINERS = ("workspace-init",)
# The attachment lane's own init container (#2567): it redeems the minted
# one-object capabilities and materializes the files into the emptyDir the
# runner mounts. Named here for the same reason the two above are -- the
# Overrides policy leaves an init container alone unless the claim entry names
# it, so without this the container has no capability and the runner boots an
# empty attachment dir: the file silently never arrives.
ATTACHMENT_INIT_CONTAINERS = ("attachments-init",)

# The SandboxClaim env schema is value-only (no secretKeyRef), so anything put
# here is stored in plain text on the claim object. The model credential must NOT
# be persisted that way: the chart's SandboxTemplate injects CURIE_CREDENTIALS
# from the chart Secret (a secretKeyRef the Overrides policy leaves in place when
# the claim does not set it), so the Kubernetes runner still receives it without
# a plaintext copy on every claim. The Docker substrate has no Secret object and
# forwards it directly; this stripping is Kubernetes-only. Named from the BootEnv
# declaration for the same reason as BUNDLE_REF_ENV above, and with a sharper
# consequence: a local literal that drifted on a rename would stop matching the
# key it strips, persisting the model credential as plaintext in etcd.
CREDENTIALS_ENV = BootEnv.env_key("credentials_ref")

# Per-agent connector secrets (ADR-0009, #429) travel through the substrate-
# agnostic boot env by value. On this value-only claim CR they would be stored as
# plaintext in etcd -- the same leak the model-credential stripping above avoids.
# The binding marks which keys are connector secrets in this env var
# (comma-separated names); strip both the marker and every key it names off the
# claim. Their secretKeyRef delivery is the per-agent SandboxTemplate (#1488).
# Named from the BootEnv declaration like the two above:
# a local literal that drifted on a rename would stop matching the marker the
# binding writes, and every connector secret would be persisted as plaintext.
CONNECTOR_SECRET_KEYS_ENV = BootEnv.env_key("connector_secret_keys")


def _conditions_ready(status: dict[str, Any]) -> bool:
    for cond in status.get("conditions") or []:
        if cond.get("type") == "Ready":
            return bool(cond.get("status") == "True")
    return False


def _ready_condition(status: dict[str, Any]) -> tuple[str | None, str | None]:
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return None, None
    for condition in conditions:
        if not isinstance(condition, dict) or condition.get("type") != "Ready":
            continue
        reason = condition.get("reason")
        message = condition.get("message")
        return (
            reason if isinstance(reason, str) else None,
            message if isinstance(message, str) else None,
        )
    return None, None


def _resource_map(raw: str) -> dict[str, str] | None:
    values: dict[str, str] = {}
    for entry in raw.split(","):
        key, separator, value = entry.strip().partition("=")
        if (
            not separator
            or not key
            or not value
            or "=" in value
            or any(character.isspace() for character in key + value)
            or key in values
        ):
            return None
        values[key] = value
    return values or None


def _quota_rejection(status: dict[str, Any]) -> QuotaRejection | None:
    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return None
    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        if (
            condition.get("type") != "Ready"
            or condition.get("status") != "False"
            or condition.get("reason") != "ReconcilerError"
        ):
            continue
        message = condition.get("message")
        if not isinstance(message, str):
            continue

        _prefix, marker, details = message.partition("exceeded quota: ")
        if not marker or "exceeded quota: " in details:
            continue
        quota_name, marker, details = details.partition(", requested: ")
        if not marker or not quota_name or any(character.isspace() for character in quota_name):
            continue
        requested_raw, marker, details = details.partition(", used: ")
        if not marker:
            continue
        used_raw, marker, hard_raw = details.partition(", limited: ")
        if not marker:
            continue

        requested = _resource_map(requested_raw)
        used = _resource_map(used_raw)
        hard = _resource_map(hard_raw)
        if requested is None or used is None or hard is None:
            continue
        common = sorted(requested.keys() & used.keys() & hard.keys())
        if not common:
            continue
        resource = common[0]
        return QuotaRejection(
            quota_name=quota_name,
            resource=resource,
            requested=requested[resource],
            used=used[resource],
            hard=hard[resource],
        )
    return None


def _parse_timestamp(raw: object) -> datetime | None:
    """A cluster creation instant as tz-aware UTC, or None when unreadable.

    ``CustomObjectsApi`` hands back the raw deserialized JSON for a CRD rather
    than a typed model, so ``metadata.creationTimestamp`` is always the RFC3339
    string the API server emitted. Normalizing to aware UTC is not cosmetic:
    the reaper compares this against ``datetime.now(UTC)``, and a naive value
    on either side raises TypeError inside a maintenance tick whose caller
    swallows exceptions, which would silently stop reaping for good.

    An unreadable value returns None (unknown age, never reaped) rather than
    raising, so one malformed object cannot take down the whole tick.
    """

    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    # Naive is read as UTC, never as host-local: the API server always sends
    # an aware RFC3339 string, but if a naive value ever reached here,
    # interpreting it as local time would shift it by the host's UTC offset,
    # and the reaper compares this against datetime.now(UTC) to decide
    # whether a claim is past the grace and safe to delete.
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _claim_view(obj: dict[str, Any]) -> ClaimView:
    status = obj.get("status") or {}
    sandbox = (status.get("sandbox") or {}).get("name")
    ready_reason, ready_message = _ready_condition(status)
    return ClaimView(
        name=obj["metadata"]["name"],
        ready=_conditions_ready(status),
        sandbox_name=sandbox,
        created_at=_parse_timestamp(obj["metadata"].get("creationTimestamp")),
        quota_rejection=_quota_rejection(status),
        ready_reason=ready_reason,
        ready_message=ready_message,
    )


def _sandbox_view(obj: dict[str, Any]) -> SandboxView:
    status = obj.get("status") or {}
    return SandboxView(
        name=obj["metadata"]["name"],
        ready=_conditions_ready(status),
        service_fqdn=status.get("serviceFQDN") or None,
        operating_mode=str((obj.get("spec") or {}).get("operatingMode", "Running")),
    )


class KubernetesSandboxClient:
    """SandboxClient against a real cluster (kubeconfig or in-cluster auth)."""

    def __init__(
        self,
        namespace: str,
        *,
        kubeconfig: str | None = None,
    ) -> None:
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config(config_file=kubeconfig)
        self._api = k8s_client.CustomObjectsApi()
        self._namespace = namespace

    # -- SandboxClaim (extensions group) ------------------------------------

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        env = filter_agent_child_env(env)
        body: dict[str, Any] = {
            "apiVersion": f"{EXT_GROUP}/{EXT_VERSION}",
            "kind": "SandboxClaim",
            "metadata": {
                "name": name,
                "labels": {MANAGED_BY_LABEL: MANAGED_BY_VALUE, **(labels or {})},
            },
            "spec": {"warmPoolRef": {"name": pool}},
        }
        if env:
            # Unnamed entries land on the first main container (the runner). The
            # model credential and per-agent connector secrets are deliberately
            # excluded so no secret value is ever persisted in plain text on the
            # claim: the credential reaches the runner via the template's
            # secretKeyRef, and connector secrets are delivered the same way via
            # the per-agent template (#1488). The marker var naming the
            # connector-secret keys is stripped too.
            marker = env.get(CONNECTOR_SECRET_KEYS_ENV, "")
            stripped = {
                CREDENTIALS_ENV,
                CONNECTOR_SECRET_KEYS_ENV,
                WORKSPACE_REF_ENV,
                WORKSPACE_SHA256_ENV,
                ATTACHMENTS_REF_ENV,
            }
            stripped.update(k for k in marker.split(",") if k)
            entries: list[dict[str, str]] = [
                {"name": k, "value": v} for k, v in sorted(env.items()) if k not in stripped
            ]
            # The bundle ref must also reach the init containers, which the
            # Overrides policy does not touch without an explicit containerName.
            bundle_ref = env.get(BUNDLE_REF_ENV)
            if bundle_ref is not None:
                for container in BUNDLE_INIT_CONTAINERS:
                    entries.append(
                        {
                            "containerName": container,
                            "name": BUNDLE_REF_ENV,
                            "value": bundle_ref,
                        }
                    )
            # Workspace fetch/extract consumes only the short-lived exact-object
            # reference and digest. It receives no worker-auth, object-store, or
            # GitHub credential.
            workspace_ref = env.get(WORKSPACE_REF_ENV)
            workspace_sha256 = env.get(WORKSPACE_SHA256_ENV)
            if workspace_ref is not None:
                for container in WORKSPACE_INIT_CONTAINERS:
                    entries.append(
                        {
                            "containerName": container,
                            "name": WORKSPACE_REF_ENV,
                            "value": workspace_ref,
                        }
                    )
                    if workspace_sha256 is not None:
                        entries.append(
                            {
                                "containerName": container,
                                "name": WORKSPACE_SHA256_ENV,
                                "value": workspace_sha256,
                            }
                        )
            # The attachment capability reaches ONLY its own init container. An
            # unnamed entry would also hand the presigned URL to the model's own
            # process -- a capability the agent has no need for and, being a
            # plain env var, one it could echo into a channel.
            attachments_ref = env.get(ATTACHMENTS_REF_ENV)
            if attachments_ref is not None:
                for container in ATTACHMENT_INIT_CONTAINERS:
                    entries.append(
                        {
                            "containerName": container,
                            "name": ATTACHMENTS_REF_ENV,
                            "value": attachments_ref,
                        }
                    )
            body["spec"]["env"] = entries
        self._api.create_namespaced_custom_object(
            EXT_GROUP, EXT_VERSION, self._namespace, "sandboxclaims", body
        )

    def get_claim(self, name: str) -> ClaimView | None:
        obj = self._get(EXT_GROUP, EXT_VERSION, "sandboxclaims", name)
        return _claim_view(obj) if obj is not None else None

    def delete_claim(self, name: str) -> None:
        try:
            self._api.delete_namespaced_custom_object(
                EXT_GROUP, EXT_VERSION, self._namespace, "sandboxclaims", name
            )
        except k8s_client.ApiException as exc:
            if exc.status != 404:
                raise

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        result = self._api.list_namespaced_custom_object(
            EXT_GROUP,
            EXT_VERSION,
            self._namespace,
            "sandboxclaims",
            label_selector=label_selector,
        )
        return [_claim_view(item) for item in result.get("items", [])]

    # -- Sandbox (core group) ------------------------------------------------

    def get_sandbox(self, name: str) -> SandboxView | None:
        obj = self._get(CORE_GROUP, CORE_VERSION, "sandboxes", name)
        return _sandbox_view(obj) if obj is not None else None

    def set_sandbox_mode(self, name: str, mode: OperatingMode) -> None:
        self._api.patch_namespaced_custom_object(
            CORE_GROUP,
            CORE_VERSION,
            self._namespace,
            "sandboxes",
            name,
            {"spec": {"operatingMode": mode}},
        )

    # -- helpers --------------------------------------------------------------

    def _get(self, group: str, version: str, plural: str, name: str) -> dict[str, Any] | None:
        try:
            obj = self._api.get_namespaced_custom_object(
                group, version, self._namespace, plural, name
            )
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                return None
            raise
        return dict(obj)
