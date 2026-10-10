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

import re
import time
from datetime import UTC, datetime
from typing import Any

from aci_protocol import BootEnv
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from ..attachments import ATTACHMENTS_REF_ENV
from ..workspace import WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV
from .quota import quota_has_live_headroom, quota_rejection_is_valid
from .resources import prepare_resources_claim
from .types import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ClaimView,
    KubeTransientError,
    OperatingMode,
    QuotaRejection,
    SandboxTermination,
    SandboxView,
    filter_agent_child_env,
)

CORE_GROUP = "agents.x-k8s.io"
CORE_VERSION = "v1beta1"
EXT_GROUP = "extensions.agents.x-k8s.io"
EXT_VERSION = "v1beta1"

# Apiserver statuses a read may recover from on the next poll.
_TRANSIENT_STATUSES = frozenset({500, 502, 503, 504})

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
        return QuotaRejection(
            quota_name=quota_name,
            requested=requested,
            used=used,
            hard=hard,
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


def _safe_termination_reason(raw: object, *, fallback: str = "Terminated") -> str:
    # Pod and Event reasons are API data. Keep a single short diagnostic token;
    # never pass arbitrary event messages into replies or issue comments.
    if isinstance(raw, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", raw):
        return raw
    return fallback


def _safe_pod_message(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    # Kubernetes messages are untrusted free text. Extract only the shape
    # needed to explain the observed EmptyDir eviction; never publish a raw
    # message or rely on generic secret redaction to recognize every format.
    match = re.search(
        r'Usage of EmptyDir volume "([a-z0-9][a-z0-9.-]{0,62})" '
        r'exceeds the limit "([0-9]{1,12}(?:Ki|Mi|Gi|Ti|Pi|Ei)?)"',
        raw,
    )
    if match is None:
        return None
    volume, limit = match.groups()
    return f'Usage of EmptyDir volume "{volume}" exceeds the limit "{limit}"'


def _recent(moment: object, *, since: datetime) -> bool:
    return isinstance(moment, datetime) and moment.tzinfo is not None and moment >= since


def _pod_termination(pod: Any, *, since: datetime) -> SandboxTermination | None:
    status = getattr(pod, "status", None)
    phase = getattr(status, "phase", None)
    reason = getattr(status, "reason", None)
    if phase == "Failed" and reason == "Evicted":
        return SandboxTermination("Evicted", _safe_pod_message(getattr(status, "message", None)))

    # A graceful delete surfaces as a generic Error or Completed runner exit.
    # On a deleting pod only OOMKilled outranks the deletion itself.
    deleting = getattr(getattr(pod, "metadata", None), "deletion_timestamp", None) is not None
    deleted_detail: str | None = None
    for container in getattr(status, "container_statuses", None) or []:
        if getattr(container, "name", None) != "runner":
            continue
        terminated = getattr(getattr(container, "state", None), "terminated", None)
        if terminated is None:
            continue
        exit_code = getattr(terminated, "exit_code", None)
        detail = f"exit code {exit_code}" if isinstance(exit_code, int) else None
        if deleting and getattr(terminated, "reason", None) != "OOMKilled":
            deleted_detail = detail
            break
        return SandboxTermination(
            _safe_termination_reason(getattr(terminated, "reason", None)), detail
        )

    # A restarted runner can be Running by the time the drop is diagnosed.
    # Only OOMKilled is strong enough evidence in last_state: another old
    # termination may predate this turn and must not relabel a network drop.
    for container in getattr(status, "container_statuses", None) or []:
        if getattr(container, "name", None) != "runner":
            continue
        terminated = getattr(getattr(container, "last_state", None), "terminated", None)
        if getattr(terminated, "reason", None) == "OOMKilled" and _recent(
            getattr(terminated, "finished_at", None), since=since
        ):
            exit_code = getattr(terminated, "exit_code", None)
            detail = f"exit code {exit_code}" if isinstance(exit_code, int) else None
            return SandboxTermination("OOMKilled", detail)

    if deleting:
        return SandboxTermination("Deleted", deleted_detail)
    if phase in {"Failed", "Succeeded"}:
        return SandboxTermination(
            _safe_termination_reason(
                reason,
                fallback="Failed" if phase == "Failed" else "Completed",
            )
        )
    return None


def _event_termination(
    events: Any, *, pod_name: str, pod_uid: str | None, since: datetime
) -> SandboxTermination | None:
    # Event lists are name filtered at the API, then checked here as well.
    # A readable pod's UID prevents an old event for a reused name from being
    # attributed to this runner.
    matches: list[SandboxTermination] = []
    for event in getattr(events, "items", None) or []:
        involved = getattr(event, "involved_object", None)
        if (
            getattr(involved, "kind", None) != "Pod"
            or getattr(involved, "name", None) != pod_name
            or (pod_uid is not None and getattr(involved, "uid", None) != pod_uid)
        ):
            continue
        event_time = (
            getattr(getattr(event, "series", None), "last_observed_time", None)
            or getattr(event, "last_timestamp", None)
            or getattr(event, "event_time", None)
            or getattr(getattr(event, "metadata", None), "creation_timestamp", None)
        )
        if not _recent(event_time, since=since):
            continue
        reason = getattr(event, "reason", None)
        if reason in {"Evicted", "OOMKilled", "OOMKilling"}:
            matches.append(SandboxTermination(_safe_termination_reason(reason)))
    for preferred in ("Evicted", "OOMKilled", "OOMKilling"):
        for match in matches:
            if match.reason == preferred:
                return match
    return None


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
        configuration = k8s_client.Configuration.get_default_copy()
        # The installed client otherwise inherits urllib3's three retry
        # default. That would multiply every explicit request timeout by four
        # and make the pressure deletion envelope false.
        configuration.retries = 0
        api_client = k8s_client.ApiClient(configuration=configuration)
        self._api = k8s_client.CustomObjectsApi(api_client)
        self._core_api = k8s_client.CoreV1Api(api_client)
        self._namespace = namespace

    def _resources_pool(
        self,
        pool: str,
        agent_name: str | None,
        runner_resources: dict[str, Any],
    ) -> str:
        """Copy the chart template onto a worker-owned pool and return its name.

        The shared template is read and not written. A missing source template
        fails the claim instead of falling back to the chart size.
        """

        if not agent_name:
            raise ValueError("runner resources require an agent name")
        source_name = pool[: -len("-pool")] if pool.endswith("-pool") else pool
        try:
            source = self._api.get_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxtemplates",
                source_name,
            )
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                raise ValueError(f"source template {source_name} is missing") from exc
            raise
        templates = {source_name: source.get("spec") or {}}
        warm_pools: dict[str, Any] = {}
        owned_pool = prepare_resources_claim(
            pool, agent_name, runner_resources, templates, warm_pools
        )
        owned_template = owned_pool[: -len("-pool")]
        self._put_extension(
            "sandboxtemplates",
            owned_template,
            "SandboxTemplate",
            templates[owned_template],
        )
        self._put_extension(
            "sandboxwarmpools",
            owned_pool,
            "SandboxWarmPool",
            warm_pools[owned_pool],
        )
        return owned_pool

    def _put_extension(self, plural: str, name: str, kind: str, spec: dict[str, Any]) -> None:
        body = {
            "apiVersion": f"{EXT_GROUP}/{EXT_VERSION}",
            "kind": kind,
            "metadata": {"name": name},
            "spec": spec,
        }
        try:
            self._api.get_namespaced_custom_object(
                EXT_GROUP, EXT_VERSION, self._namespace, plural, name
            )
        except k8s_client.ApiException as exc:
            if exc.status != 404:
                raise
            self._api.create_namespaced_custom_object(
                EXT_GROUP, EXT_VERSION, self._namespace, plural, body
            )
            return
        self._api.patch_namespaced_custom_object(
            EXT_GROUP, EXT_VERSION, self._namespace, plural, name, body
        )

    # -- SandboxClaim (extensions group) ------------------------------------

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, Any] | None = None,
        agent_name: str | None = None,
    ) -> None:
        if runner_resources is not None:
            pool = self._resources_pool(pool, agent_name, runner_resources)
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

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> ClaimView | None:
        obj = self._get(
            EXT_GROUP,
            EXT_VERSION,
            "sandboxclaims",
            name,
            request_timeout_seconds=request_timeout_seconds,
        )
        return _claim_view(obj) if obj is not None else None

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        try:
            self._api.delete_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxclaims",
                name,
                _request_timeout=request_timeout_seconds,
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

    def warm_pool_exists(self, name: str) -> bool:
        """Whether this namespace already has the named SandboxWarmPool.

        A missing pool is false. Any other API error propagates so the caller
        can keep today's pool choice instead of failing the claim.
        """

        try:
            self._api.get_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxwarmpools",
                name,
                _request_timeout=5,
            )
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                return False
            raise
        return True

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None:
        obj = self._get(
            CORE_GROUP,
            CORE_VERSION,
            "sandboxes",
            name,
            request_timeout_seconds=request_timeout_seconds,
        )
        return _sandbox_view(obj) if obj is not None else None

    def quota_has_headroom(
        self,
        rejection: QuotaRejection,
        *,
        request_timeout_seconds: float,
    ) -> bool:
        if not quota_rejection_is_valid(rejection):
            return False
        try:
            quota = self._core_api.read_namespaced_resource_quota(
                rejection.quota_name,
                self._namespace,
                _request_timeout=request_timeout_seconds,
            )
        except Exception:  # noqa: BLE001 - unreadable quota state fails closed
            return False

        metadata = getattr(quota, "metadata", None)
        spec = getattr(quota, "spec", None)
        status = getattr(quota, "status", None)
        if (
            getattr(metadata, "name", None) != rejection.quota_name
            or getattr(metadata, "namespace", None) != self._namespace
        ):
            return False
        return quota_has_live_headroom(
            rejection,
            spec_hard=getattr(spec, "hard", None),
            status_hard=getattr(status, "hard", None),
            status_used=getattr(status, "used", None),
        )

    def pod_unschedulable(self, name: str, *, request_timeout_seconds: float) -> str | None:
        try:
            pod = self._core_api.read_namespaced_pod(
                name,
                self._namespace,
                _request_timeout=request_timeout_seconds,
            )
        except Exception:  # noqa: BLE001 - unknown pod state keeps the claim timeout
            return None
        status = getattr(pod, "status", None)
        for condition in getattr(status, "conditions", None) or []:
            if (
                getattr(condition, "type", None) == "PodScheduled"
                and getattr(condition, "status", None) == "False"
                and getattr(condition, "reason", None) == "Unschedulable"
            ):
                message = getattr(condition, "message", None)
                return message if isinstance(message, str) and message else "Unschedulable"
        return None

    def pod_log_tail(self, name: str, *, request_timeout_seconds: float) -> str | None:
        """Read the runner's last 8 KB, preferring its terminated instance."""

        deadline = time.monotonic() + request_timeout_seconds
        for previous in (True, False):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            response: Any = None
            try:
                response = self._core_api.read_namespaced_pod_log(
                    name,
                    self._namespace,
                    container="runner",
                    previous=previous,
                    tail_lines=200,
                    limit_bytes=8192,
                    _preload_content=False,
                    _request_timeout=remaining,
                )
                data = getattr(response, "data", response)
                if isinstance(data, bytes):
                    return data.decode("utf-8", errors="replace")
                return str(data)
            except k8s_client.ApiException as exc:
                if previous and exc.status == 400:
                    continue
                return None
            except Exception:  # noqa: BLE001 - diagnosis is best effort
                return None
            finally:
                for method_name in ("close", "release_conn"):
                    try:
                        method = getattr(response, method_name, None)
                        if callable(method):
                            method()
                    except Exception:  # noqa: BLE001 - response cleanup is best effort
                        pass
        return None

    def pod_termination(
        self, name: str, *, since: datetime, request_timeout_seconds: float
    ) -> SandboxTermination | None:
        """Read exact pod state and a bounded set of its events within one budget."""

        deadline = time.monotonic() + request_timeout_seconds
        pod: Any = None
        # A 404 is positive evidence the pod was deleted: the stream was
        # established against it. A 403 or a timeout proves nothing.
        pod_missing = False
        try:
            pod = self._core_api.read_namespaced_pod(
                name,
                self._namespace,
                _request_timeout=max(0.001, deadline - time.monotonic()),
            )
        except k8s_client.ApiException as exc:
            pod_missing = exc.status == 404
        except Exception:  # noqa: BLE001 - diagnosis is best effort
            pass
        # since is taken after the original pod was serving, and creation
        # timestamps truncate to the second, so a pod created at or after it is
        # a controller replacement. Its status and events describe another pod.
        if pod is not None and _recent(
            getattr(getattr(pod, "metadata", None), "creation_timestamp", None), since=since
        ):
            return SandboxTermination("Deleted", "replaced by a new pod with the same name")
        status_termination = _pod_termination(pod, since=since) if pod is not None else None
        pod_uid = getattr(getattr(pod, "metadata", None), "uid", None)
        event_termination: SandboxTermination | None = None
        if time.monotonic() < deadline:
            try:
                events = self._core_api.list_namespaced_event(
                    self._namespace,
                    field_selector=f"involvedObject.kind=Pod,involvedObject.name={name}",
                    limit=20,
                    _request_timeout=max(0.001, deadline - time.monotonic()),
                )
                event_termination = _event_termination(
                    events, pod_name=name, pod_uid=pod_uid, since=since
                )
            except Exception:  # noqa: BLE001 - pod state still provides evidence
                pass
        if status_termination is not None and status_termination.reason not in {
            "Failed",
            "Terminated",
            "Error",
        }:
            return status_termination
        if event_termination is not None and (
            event_termination.reason == "Evicted" or pod is None or status_termination is not None
        ):
            return event_termination
        if pod_missing:
            return SandboxTermination("Deleted")
        return status_termination

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

    def _get(
        self,
        group: str,
        version: str,
        plural: str,
        name: str,
        *,
        request_timeout_seconds: float,
    ) -> dict[str, Any] | None:
        try:
            obj = self._api.get_namespaced_custom_object(
                group,
                version,
                self._namespace,
                plural,
                name,
                _request_timeout=request_timeout_seconds,
            )
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                return None
            if exc.status in _TRANSIENT_STATUSES:
                raise KubeTransientError(f"kube API {exc.status} reading {plural}/{name}") from exc
            raise
        except Urllib3HTTPError as exc:
            # With retries=0 a read timeout or dropped connection surfaces as a
            # raw urllib3 error; typed here so it is a SandboxError (#4181).
            raise KubeTransientError(
                f"kube API transport error reading {plural}/{name}: {type(exc).__name__}"
            ) from exc
        return dict(obj)
