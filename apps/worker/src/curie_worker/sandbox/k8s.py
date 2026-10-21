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

import logging
import re
import socket
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from aci_protocol import BootEnv
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.stream import portforward
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from ..attachments import ATTACHMENTS_REF_ENV
from ..workspace import (
    REPO_ORIGIN_ENV,
    REPO_PATH_ENV,
    WORKSPACE_REF_ENV,
    WORKSPACE_SHA256_ENV,
)
from .claim_tokens import (
    CLAIM_LABEL,
    ClaimObjectNames,
    claim_object_names,
    claim_template_spec,
    executor_withheld_names,
    split_claim_tokens,
)
from .quota import quota_has_live_headroom, quota_rejection_is_valid
from .resources import prepare_resources_claim
from .types import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ClaimNameConflict,
    ClaimTimeoutError,
    ClaimView,
    DaemonObjectKind,
    DaemonObjectView,
    KubeTransientError,
    ListableDaemonKind,
    OperatingMode,
    QuotaRejection,
    SandboxError,
    SandboxTermination,
    SandboxView,
    ToolDaemonMount,
    filter_agent_child_env,
)

logger = logging.getLogger(__name__)

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
# The claim env each workspace init container receives by name. The origin and
# path are the trusted checkout origin (ADR 0197); the worker sets them from
# the API's credential, never from turn text.
WORKSPACE_INIT_ENV = (WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV, REPO_ORIGIN_ENV, REPO_PATH_ENV)
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

# Transport bound on every call a token-bearing create_claim makes. The per-claim
# template is a reaper candidate once it outlives the grace and has no claim, so
# the four calls from template create to claim create must sum (20 s) to well
# under REAP_GRACE_MARGIN_SECONDS (30 s); a stalled call fails the claim instead.
_CLAIM_PREPARATION_TIMEOUT_S = 5.0


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
        bundle_ref=_claim_bundle_ref(obj),
    )


def _claim_bundle_ref(obj: dict[str, Any]) -> str | None:
    """The runner's ``CURIE_BUNDLE_REF`` from the claim spec env, or None."""

    for entry in (obj.get("spec") or {}).get("env") or []:
        if (
            isinstance(entry, dict)
            and entry.get("name") == BUNDLE_REF_ENV
            and not entry.get("containerName")
            and isinstance(entry.get("value"), str)
        ):
            return str(entry["value"])
    return None


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


def _extension_body(kind: str, metadata: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """A full extensions-group object body of ``kind``."""

    return {
        "apiVersion": f"{EXT_GROUP}/{EXT_VERSION}",
        "kind": kind,
        "metadata": metadata,
        "spec": spec,
    }


def _claim_timeout(deadline: float) -> float:
    """One claim request's transport bound: the preparation cap, within ``deadline``."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ClaimTimeoutError("the claim's deadline passed before its next request")
    return min(_CLAIM_PREPARATION_TIMEOUT_S, remaining)


def _refuse_name_conflict(exc: BaseException, what: str) -> None:
    """Raise ClaimNameConflict for a 409 on a create at a claim-derived name."""

    if isinstance(exc, k8s_client.ApiException) and exc.status == 409:
        raise ClaimNameConflict(f"{what} already exists") from exc


def _aware(moment: object) -> datetime | None:
    if not isinstance(moment, datetime):
        return None
    return moment.astimezone(UTC) if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _daemon_view(kind: DaemonObjectKind, obj: Any) -> DaemonObjectView:
    metadata = obj.metadata
    return DaemonObjectView(
        kind=kind,
        name=str(metadata.name),
        uid=str(metadata.uid),
        claim=(metadata.labels or {}).get(CLAIM_LABEL),
        created_at=_aware(metadata.creation_timestamp),
        phase=getattr(getattr(obj, "status", None), "phase", None) if kind == "Pod" else None,
    )


# Port-forward connects still outstanding, so a hung API server cannot pile up
# threads behind repeated pings (each one fails closed instead).
_PORT_FORWARDS = threading.BoundedSemaphore(8)


def _owner_ref(api_version: str, kind: str, name: str, uid: str) -> dict[str, str]:
    """An ownerReference with no ``controller`` or ``blockOwnerDeletion``."""

    return {"apiVersion": api_version, "kind": kind, "name": name, "uid": uid}


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
        self._configuration = configuration
        self._api = k8s_client.CustomObjectsApi(api_client)
        self._core_api = k8s_client.CoreV1Api(api_client)
        self._networking_api = k8s_client.NetworkingV1Api(api_client)
        self._namespace = namespace

    def _resources_pool(
        self,
        pool: str,
        agent_name: str | None,
        runner_resources: dict[str, Any],
        deadline: float,
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
                _request_timeout=_claim_timeout(deadline),
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
            deadline,
        )
        self._put_extension(
            "sandboxwarmpools",
            owned_pool,
            "SandboxWarmPool",
            warm_pools[owned_pool],
            deadline,
        )
        return owned_pool

    def _put_extension(
        self, plural: str, name: str, kind: str, spec: dict[str, Any], deadline: float
    ) -> None:
        body = _extension_body(kind, {"name": name}, spec)
        try:
            self._api.get_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                plural,
                name,
                _request_timeout=_claim_timeout(deadline),
            )
        except k8s_client.ApiException as exc:
            if exc.status != 404:
                raise
            self._api.create_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                plural,
                body,
                _request_timeout=_claim_timeout(deadline),
            )
            return
        self._api.patch_namespaced_custom_object(
            EXT_GROUP,
            EXT_VERSION,
            self._namespace,
            plural,
            name,
            body,
            _request_timeout=_claim_timeout(deadline),
        )

    def _claim_scoped_pool(
        self,
        claim: str,
        pool: str,
        tokens: dict[str, str],
        runner_resources: dict[str, Any] | None,
        deadline: float,
        executor_secret_names: frozenset[str] | None = None,
        executor_withheld: frozenset[str] = frozenset(),
        tool_daemon: ToolDaemonMount | None = None,
    ) -> ClaimObjectNames:
        """Write the per-claim template, token Secret and pool; return their names.

        The source template is the one ``pool``'s ``sandboxTemplateRef`` names,
        so per-agent connector ``secretKeyRef`` entries (#1488) survive the copy.
        Every refusal (name too long, missing pool or template, no runner
        container, invalid resources) is raised before the first write. The
        Secret and pool are owned by the template, so deleting the template on
        a later failure removes all three.
        """

        names = claim_object_names(claim)
        source_name = self._pool_template_name(pool, deadline)
        source_spec = self._source_template_spec(
            source_name, request_timeout_seconds=_claim_timeout(deadline)
        )
        spec = claim_template_spec(
            source_spec,
            secret_name=names.secret,
            token_names=sorted(tokens),
            runner_resources=runner_resources,
            claim=claim,
            executor_secret_names=executor_secret_names,
            executor_withheld=executor_withheld,
            tool_daemon=tool_daemon,
        )
        labels = {MANAGED_BY_LABEL: MANAGED_BY_VALUE, CLAIM_LABEL: claim}
        try:
            created = self._api.create_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxtemplates",
                _extension_body(
                    "SandboxTemplate", {"name": names.template, "labels": labels}, spec
                ),
                _request_timeout=_claim_timeout(deadline),
            )
        except k8s_client.ApiException as exc:
            # Raised with no rollback: a template already at this name is not ours.
            _refuse_name_conflict(exc, f"SandboxTemplate {names.template}")
            raise
        try:
            # No controller or blockOwnerDeletion: either would need
            # ``finalizers`` RBAC the worker is not granted.
            owner = _owner_ref(
                f"{EXT_GROUP}/{EXT_VERSION}",
                "SandboxTemplate",
                names.template,
                created["metadata"]["uid"],
            )
            metadata = {"name": names.secret, "labels": labels, "ownerReferences": [owner]}
            # Token values and daemon client files (ADR 0217) are never logged;
            # ``stringData`` is the only place they are written.
            files = {} if tool_daemon is None else dict(tool_daemon.files)
            self._core_api.create_namespaced_secret(
                self._namespace,
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "type": "Opaque",
                    "metadata": metadata,
                    "stringData": {**tokens, **files},
                },
                _request_timeout=_claim_timeout(deadline),
            )
            self._api.create_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxwarmpools",
                _extension_body(
                    "SandboxWarmPool",
                    {**metadata, "name": names.pool},
                    {"replicas": 0, "sandboxTemplateRef": {"name": names.template}},
                ),
                _request_timeout=_claim_timeout(deadline),
            )
        except Exception:
            self._rollback_delete("sandboxtemplates", names.template)
            raise
        return names

    def _pool_template_name(self, pool: str, deadline: float) -> str:
        """The template ``pool``'s ``sandboxTemplateRef`` names; a missing pool refuses."""

        obj = self._get(
            EXT_GROUP,
            EXT_VERSION,
            "sandboxwarmpools",
            pool,
            request_timeout_seconds=_claim_timeout(deadline),
        )
        if obj is None:
            raise ValueError(f"source pool {pool} is missing")
        ref = (obj.get("spec") or {}).get("sandboxTemplateRef") or {}
        name = ref.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(f"source pool {pool} names no sandboxTemplateRef")
        return name

    def _source_template_spec(self, name: str, *, request_timeout_seconds: float) -> dict[str, Any]:
        """The spec of source template ``name``; a missing template refuses."""

        obj = self._get(
            EXT_GROUP,
            EXT_VERSION,
            "sandboxtemplates",
            name,
            request_timeout_seconds=request_timeout_seconds,
        )
        if obj is None:
            raise ValueError(f"source template {name} is missing")
        spec: dict[str, Any] = obj.get("spec") or {}
        return spec

    def _rollback_delete(self, plural: str, name: str) -> None:
        """Delete one object a failed claim created; never mask the original error.

        A failed delete is logged by exception class only and left to the
        reaper's per-claim template sweep.
        """

        try:
            self._api.delete_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                plural,
                name,
                propagation_policy="Background",
                _request_timeout=_CLAIM_PREPARATION_TIMEOUT_S,
            )
        except Exception as exc:  # noqa: BLE001 - a rollback delete is best effort
            if isinstance(exc, k8s_client.ApiException) and exc.status == 404:
                return
            logger.warning(
                "rollback delete of %s %s failed (%s)", plural, name, type(exc).__name__
            )

    # -- SandboxClaim (extensions group) ------------------------------------

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        deadline: float,
        runner_resources: dict[str, Any] | None = None,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
        tool_daemon: ToolDaemonMount | None = None,
    ) -> None:
        _claim_timeout(deadline)  # a spent deadline refuses before any write
        env = filter_agent_child_env(env)
        executor = executor_secret_names is not None
        withheld: frozenset[str] = frozenset()
        if executor:
            # @spec ACTION-EXECUTOR-5: no model credential, its env-key
            # declaration, or a name that declaration points at reaches an
            # executor runner, by claim or template.
            withheld = executor_withheld_names(env)
            env = {k: v for k, v in env.items() if k not in withheld}
        # Scoped tokens never ride the value-only claim (#3842): they go to a
        # per-claim Secret read by a per-claim template copy, which also
        # carries any runner resources override. A token-free claim keeps the
        # chart pool or the per-agent resources pool. An executor claim always
        # takes the per-claim path: claim env cannot remove what the pool
        # template carries, so only its own stripped copy can (ACTION-EXECUTOR-5).
        # A Docker-enabled claim takes it too: its daemon client files ride the
        # per-claim Secret, mounted read-only on the runner (ADR 0217).
        tokens, env = split_claim_tokens(env)
        claim_template: str | None = None
        if tokens or executor or tool_daemon is not None:
            claim_names = self._claim_scoped_pool(
                name,
                pool,
                tokens,
                runner_resources,
                deadline,
                executor_secret_names,
                withheld,
                tool_daemon,
            )
            pool = claim_names.pool
            claim_template = claim_names.template
        elif runner_resources is not None:
            pool = self._resources_pool(pool, agent_name, runner_resources, deadline)
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
            # connector-secret keys is stripped too. Scoped tokens were already
            # split off above and reach the runner through the per-claim
            # Secret (#3842).
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
            # reference and digest, plus the trusted origin and repository path
            # it checks the checkout against (ADR 0197). It receives no
            # worker-auth, object-store, or code host credential.
            workspace_ref = env.get(WORKSPACE_REF_ENV)
            if workspace_ref is not None:
                for container in WORKSPACE_INIT_CONTAINERS:
                    for key in WORKSPACE_INIT_ENV:
                        value = env.get(key)
                        if value is not None:
                            entries.append(
                                {"containerName": container, "name": key, "value": value}
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
        if claim_template is None:
            try:
                self._api.create_namespaced_custom_object(
                    EXT_GROUP,
                    EXT_VERSION,
                    self._namespace,
                    "sandboxclaims",
                    body,
                    _request_timeout=_claim_timeout(deadline),
                )
            except k8s_client.ApiException as exc:
                _refuse_name_conflict(exc, f"SandboxClaim {name}")
                raise
            return
        claim_created = False
        try:
            created = self._api.create_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxclaims",
                body,
                _request_timeout=_claim_timeout(deadline),
            )
            claim_created = True
            # Hand the template (and through it the Secret and pool) to the
            # claim, so garbage collection removes all three when the claim
            # goes by any path. Until this lands only the reaper's template
            # sweep covers them.
            owner = _owner_ref(
                f"{EXT_GROUP}/{EXT_VERSION}", "SandboxClaim", name, created["metadata"]["uid"]
            )
            self._api.patch_namespaced_custom_object(
                EXT_GROUP,
                EXT_VERSION,
                self._namespace,
                "sandboxtemplates",
                claim_template,
                {"metadata": {"ownerReferences": [owner]}},
                _request_timeout=_claim_timeout(deadline),
            )
        except Exception as exc:
            if claim_created:
                self._rollback_delete("sandboxclaims", name)
            self._rollback_delete("sandboxtemplates", claim_template)
            if not claim_created:
                _refuse_name_conflict(exc, f"SandboxClaim {name}")
            raise

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

    def reap_claim_templates(self, *, keep: set[str], created_before: datetime) -> list[str]:
        """Delete per-claim templates whose claim is gone; return their names.

        Only templates this substrate labelled for a claim are candidates, never
        the chart's. A template whose claim is in ``keep``, that is not older
        than ``created_before``, or whose age is unreadable is spared, the same
        fail-safe direction as claim reaping. Deleting one removes its token
        Secret and pool by ownerReference; a 404 means garbage collection got
        there first and counts as deleted.
        """

        result = self._api.list_namespaced_custom_object(
            EXT_GROUP,
            EXT_VERSION,
            self._namespace,
            "sandboxtemplates",
            label_selector=f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{CLAIM_LABEL}",
        )
        deleted: list[str] = []
        for item in result.get("items", []):
            metadata = item.get("metadata") or {}
            name = metadata.get("name")
            claim = (metadata.get("labels") or {}).get(CLAIM_LABEL)
            created_at = _parse_timestamp(metadata.get("creationTimestamp"))
            if (
                not name
                or not claim
                or claim in keep
                or created_at is None
                or created_at >= created_before
            ):
                continue
            # The claim inventory behind ``keep`` predates this sweep, so a
            # create_claim that finished since then has a live claim missing
            # from it. Re-read that one claim; only a 404 proves it is gone.
            try:
                live = self.get_claim(claim, request_timeout_seconds=_CLAIM_PREPARATION_TIMEOUT_S)
            except Exception as exc:  # noqa: BLE001 - an unreadable claim is spared
                logger.warning(
                    "claim recheck for template %s failed (%s); sparing it",
                    name,
                    type(exc).__name__,
                )
                continue
            if live is not None:
                continue
            try:
                self._api.delete_namespaced_custom_object(
                    EXT_GROUP,
                    EXT_VERSION,
                    self._namespace,
                    "sandboxtemplates",
                    name,
                    propagation_policy="Background",
                )
            except k8s_client.ApiException as exc:
                if exc.status != 404:
                    raise
            deleted.append(name)
        return deleted

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

    # -- tool daemon objects (ADR 0217) ----------------------------------------

    def _daemon_api(self, kind: DaemonObjectKind, verb: str) -> Any:
        api = self._networking_api if kind == "NetworkPolicy" else self._core_api
        plural = {"Pod": "pod", "Service": "service", "Secret": "secret"}.get(
            kind, "network_policy"
        )
        return getattr(api, f"{verb}_namespaced_{plural}")

    def create_daemon_object(
        self, namespace: str, manifest: Mapping[str, Any], *, request_timeout_seconds: float
    ) -> str:
        kind: DaemonObjectKind = manifest["kind"]
        try:
            created = self._daemon_api(kind, "create")(
                namespace, dict(manifest), _request_timeout=request_timeout_seconds
            )
        except k8s_client.ApiException as exc:
            _refuse_name_conflict(exc, f"{kind} {manifest['metadata']['name']} in {namespace}")
            raise
        return str(created.metadata.uid)

    def list_daemon_objects(
        self,
        namespace: str,
        kind: ListableDaemonKind,
        *,
        label_selector: str,
        request_timeout_seconds: float,
    ) -> list[DaemonObjectView]:
        result = self._daemon_api(kind, "list")(
            namespace, label_selector=label_selector, _request_timeout=request_timeout_seconds
        )
        return [_daemon_view(kind, item) for item in result.items or []]

    def delete_daemon_object(
        self,
        namespace: str,
        kind: DaemonObjectKind,
        name: str,
        *,
        uid: str,
        request_timeout_seconds: float,
    ) -> None:
        options = k8s_client.V1DeleteOptions(
            propagation_policy="Background", preconditions=k8s_client.V1Preconditions(uid=uid)
        )
        try:
            self._daemon_api(kind, "delete")(
                name, namespace, body=options, _request_timeout=request_timeout_seconds
            )
        except k8s_client.ApiException as exc:
            # Absent, or replaced since it was observed (the uid precondition's
            # 409): either way not ours to delete.
            if exc.status not in (404, 409):
                raise

    def get_daemon_pod(
        self, namespace: str, name: str, *, request_timeout_seconds: float
    ) -> DaemonObjectView | None:
        try:
            pod = self._core_api.read_namespaced_pod(
                name, namespace, _request_timeout=request_timeout_seconds
            )
        except k8s_client.ApiException as exc:
            if exc.status == 404:
                return None
            if exc.status in _TRANSIENT_STATUSES:
                raise KubeTransientError(f"kube API {exc.status} reading pod {name}") from exc
            raise
        except Urllib3HTTPError as exc:
            raise KubeTransientError(f"kube API transport error reading pod {name}") from exc
        return _daemon_view("Pod", pod)

    @contextmanager
    def open_daemon_stream(
        self, namespace: str, pod: str, port: int, *, timeout_seconds: float
    ) -> Iterator[socket.socket]:
        """A byte stream to ``pod:port`` through an API port-forward.

        ``portforward`` patches its ApiClient for the call's duration, so each
        stream gets a fresh one. Its websocket connect has no timeout, so it
        runs on a daemon thread waited on for ``timeout_seconds``; a connect
        that finishes after the caller gave up closes what it produced.
        """

        if not _PORT_FORWARDS.acquire(blocking=False):
            raise SandboxError("too many tool daemon port-forwards are still connecting")
        deadline = time.monotonic() + timeout_seconds
        api_client = k8s_client.ApiClient(configuration=self._configuration)
        lock = threading.Lock()
        done = threading.Event()
        state: dict[str, Any] = {}

        def connect() -> None:
            forward: Any = None
            error: Exception | None = None
            try:
                connector = k8s_client.CoreV1Api(api_client).connect_get_namespaced_pod_portforward
                forward = portforward(connector, pod, namespace, ports=str(port))
            except Exception as exc:  # noqa: BLE001 - handed to the waiting caller
                error = exc
            finally:
                _PORT_FORWARDS.release()
            with lock:
                abandoned = state.setdefault("abandoned", False)
                state.update(forward=forward, error=error)
            done.set()
            if abandoned:
                if forward is not None:
                    forward.close()
                api_client.close()

        threading.Thread(target=connect, name=f"daemon-portforward-{pod}", daemon=True).start()
        done.wait(timeout_seconds)
        with lock:
            connected = "error" in state
            state.setdefault("abandoned", True)
        if not connected:
            raise SandboxError(f"port-forward to {pod} did not connect within {timeout_seconds}s")
        forward = state["forward"]
        if forward is None:
            api_client.close()
            error = state["error"]
            raise SandboxError(f"port-forward to {pod} failed ({type(error).__name__})") from error
        sock: socket.socket | None = None
        try:
            sock = socket.socket(fileno=forward.socket(port).detach())
            sock.settimeout(max(0.001, deadline - time.monotonic()))
            yield sock
        finally:
            if sock is not None:
                sock.close()
            forward.close()
            api_client.close()

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
