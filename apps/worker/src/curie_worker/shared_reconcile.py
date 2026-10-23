"""Reconcile shared connector applications and their members' reach (ADR 0239, cp5).

Driven by ``connector_loop.ConnectorReconcileLoop.one_pass``, which records
what each agent's render said in a ``SharedObservation`` and then calls, in
order, ``ensure_application`` once per declared application,
``reconcile_reach`` and ``collect_garbage``. Every object applied is the
frozen render's output exactly as the API returned it (``render_application``
from the worker route, ``render_member_reach`` from the agent render), so the
API, the worker and the CLI cannot render differently.

The observation keeps three disjoint answers per pass:

1. Authoritative: a 200 render (its shared entries are granted and declared),
   an explicit refusal naming what the bundle declares (nothing granted, its
   applications still declared), or a malformed or undecidable refusal
   (nothing granted, nothing declared, and application GC is blocked for the
   pass because the agent's real declarations are unknown). An agent with no
   stored bundle is authoritative and empty.
2. Unresolved: a transport error, timeout, 5xx or any other exception. The
   agent's reach is left exactly as it is, and application GC is blocked.
3. Confirmed removals: reach of an agent no active deployment names.

So reach is pruned only on an authoritative answer, and application objects
are deleted only in a pass with GC unblocked and no authoritative agent
declaring the application. Secrets are never deleted.

An application's Pods never run without its ingress isolation. Its
NetworkPolicies are applied first, then its Service, and its Deployment only
once every policy applied. GC deletes the Deployment first, in the foreground,
and its Service and NetworkPolicies only in a pass that lists no Deployment of
that application: a failed delete, or a Deployment still terminating, keeps
them for a later pass.

``application_ready`` answers whether this process last observed an
application's Deployment ready. The agent reconcile reads it before this
pass's ``ensure_application`` runs, so a dedicated connector replaced by a
shared reference is retained until a pass after the first ready observation
(ADR 0239 Restore 6).

``ensure_application`` per application:

1. Reads the worker route's render and Secret plan. A disabled application
   applies nothing and deletes nothing.
2. The key Secret ``shared-key-<uuid>`` is created once from a key pair
   generated here, then read back by an exact list. An existing one (a create
   that answered 409) is listed, validated and adopted. A Secret is never
   patched, applied or replaced; one of the wrong shape is
   ``shared_key_secret_invalid`` and left as it is.
3. Only the public key is registered, and the API's answer must echo the
   application and recipient (``shared_recipient_mismatch``).
4. A bundled database's DSN Secret is created once the API holds a DSN sealed
   to the recipient; until then nothing is applied (``pending``). A DSN
   Secret that no longer matches the API's sealed DSN, after a rotation, is
   reported ``failed``/``dsn_secret_stale`` and never patched. A managed
   database's operator Secret, and the grant store and snapshot seal, are
   checked by metadata only.
5. The render's objects are server-side applied in that isolation order, and
   the database status is reported from the Deployment's observed readiness,
   only when it changes.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

import httpx
import nacl.public
from curie_connector_proxy.resolver.keys import KeyRing, strict_b64decode
from curie_internal.shared_declarations import (
    APPLICATION_LABEL,
    DSN_SECRET_KEY,
    KEY_SECRET_KEY,
    SECRET_ROLE_LABEL,
    dsn_secret_name,
    key_secret_name,
)
from kubernetes.client.exceptions import ApiException

from .connector_agent import SharedReach
from .shared_declarations_client import WORKER_TOKEN_HEADER, WORKER_TOKEN_MISSING
from .shared_k8s import SHARED_KINDS

logger = logging.getLogger(__name__)

AGENT_LABEL = "curietech.ai/agent-id"
COMPONENT_LABEL = "app.kubernetes.io/component"
PART_OF_LABEL = "app.kubernetes.io/part-of"
COMPONENT = "shared-connector-application"
ROLE_ENCRYPTION_KEY = "encryption-key"
ROLE_DATABASE_DSN = "database-dsn"
RECIPIENT_ALGORITHM = "x25519-sealed-box"
INTERNAL_PREFIX = "/v1/internal/shared-connectors"

KEY_SECRET_INVALID = "shared_key_secret_invalid"
DSN_SECRET_INVALID = "shared_dsn_secret_invalid"
RECIPIENT_MISMATCH = "shared_recipient_mismatch"
OPERATOR_SECRET_MISSING = "shared_operator_secret_missing"
RENDER_MALFORMED = "shared_application_render_malformed"
APPLY_FAILED = "shared_apply_failed"
DELETE_FAILED = "shared_delete_failed"
APPLICATION_DISABLED = "shared_application_disabled"
# Database status reasons (``STATUS_REASON_PATTERN``).
DSN_SECRET_STALE = "dsn_secret_stale"
DSN_SECRET_MISSING = "dsn_secret_missing"
DEPLOYMENT_UNAVAILABLE = "deployment_unavailable"

# The order an application's objects are applied in: no Pod before its policies.
_APPLY_ORDER = {"NetworkPolicy": 0, "Service": 1, "Deployment": 2}

# What the API answers a refusal with: a fixed code, never a value.
_CODE = re.compile(r"^[a-z][a-z0-9_.]{0,63}$")


class SharedReconcileRefused(Exception):
    """A shared application step refused. ``str(exc)`` is the code; it carries no value."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code

    def __str__(self) -> str:
        return self.code


# =============================================================================
# The API: the worker routes on the internal worker token
# =============================================================================


class SharedApplicationApi(Protocol):
    def render(self, application_id: str, *, namespace: str, release: str) -> dict[str, Any]: ...

    def register_recipient(self, application_id: str, body: dict[str, Any]) -> dict[str, Any]: ...

    def report_status(self, application_id: str, body: dict[str, Any]) -> dict[str, Any]: ...


class HttpSharedApplicationApi:
    """The worker routes under ``/v1/internal/shared-connectors``.

    Synchronous, as ``HttpManifestSource`` is. A 4xx carrying a fixed code
    raises ``SharedReconcileRefused``; any other non-2xx raises
    ``httpx.HTTPStatusError``, and a transport failure raises as httpx does.
    ``client`` is the transport; None builds one per call.
    """

    def __init__(
        self,
        *,
        api_base_url: str,
        internal_worker_token: str,
        client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base = api_base_url.rstrip("/") + INTERNAL_PREFIX
        self._headers = {WORKER_TOKEN_HEADER: internal_worker_token}
        self._client = client
        self._timeout = timeout

    def _call(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        body: Any = None,
    ) -> dict[str, Any]:
        url = self._base + path
        if self._client is not None:
            response = self._client.request(
                method, url, params=params, json=body, headers=self._headers
            )
        else:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.request(
                    method, url, params=params, json=body, headers=self._headers
                )
        if 400 <= response.status_code <= 499:
            try:
                detail = response.json().get("detail")
            except (ValueError, AttributeError):
                detail = None
            if isinstance(detail, str) and _CODE.fullmatch(detail):
                raise SharedReconcileRefused(detail)
        response.raise_for_status()
        answered: dict[str, Any] = response.json()
        return answered

    def render(self, application_id: str, *, namespace: str, release: str) -> dict[str, Any]:
        return self._call(
            "GET",
            f"/applications/{application_id}/render",
            params={"namespace": namespace, "release": release},
        )

    def register_recipient(self, application_id: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._call("POST", f"/applications/{application_id}/recipients", body=body)

    def report_status(self, application_id: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._call("PUT", f"/applications/{application_id}/database/status", body=body)


def _validate_registration_response(body: Any, *, application_id: str, recipient_id: str) -> None:
    """The API's answer names this application and this recipient, or it is refused."""

    if (
        not isinstance(body, Mapping)
        or str(body.get("application_id")) != application_id
        or body.get("recipient_id") != recipient_id
    ):
        raise SharedReconcileRefused(RECIPIENT_MISMATCH)


# =============================================================================
# The cluster surface (shared_k8s.KubernetesSharedClient)
# =============================================================================


class SharedClient(Protocol):
    def list_labelled(
        self, namespace: str, kind: str, selector: str | Mapping[str, str]
    ) -> list[dict[str, Any]]: ...

    def apply(self, namespace: str, obj: dict[str, Any]) -> None: ...

    def delete(self, namespace: str, kind: str, name: str) -> None: ...

    def create_secret(self, namespace: str, body: dict[str, Any]) -> str: ...

    def list_secret(
        self, namespace: str, name: str, labels: Mapping[str, str]
    ) -> dict[str, Any] | None: ...

    def secret_exists_metadata_only(self, namespace: str, name: str) -> bool: ...


# =============================================================================
# One pass's observation
# =============================================================================


@dataclass(frozen=True)
class AgentShared:
    """One agent's authoritative answer: the reach it is granted, the applications it declares."""

    granted: Mapping[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    declared: frozenset[str] = frozenset()


@dataclass
class SharedObservation:
    """What every agent's render said in one pass (the three disjoint sets)."""

    deployed: set[str] = field(default_factory=set)
    authoritative: dict[str, AgentShared] = field(default_factory=dict)
    unresolved: set[str] = field(default_factory=set)
    gc_blocked: bool = False

    def rendered(self, agent_id: str, shared: Sequence[SharedReach]) -> None:
        """A 200 render. Two aliases of one application are one member: one reach set."""

        granted: dict[str, tuple[dict[str, Any], ...]] = {}
        for entry in shared:
            granted.setdefault(entry.application, entry.reach)
        self.authoritative[agent_id] = AgentShared(granted=granted, declared=frozenset(granted))

    def refused(self, agent_id: str, declared: Sequence[str] | None) -> None:
        """A refused render: nothing granted. Naming no declarations blocks GC."""

        if declared is None:
            self.gc_blocked = True
        self.authoritative[agent_id] = AgentShared(declared=frozenset(declared or ()))

    def unresolvable(self, agent_id: str) -> None:
        self.unresolved.add(agent_id)
        self.gc_blocked = True

    def declared_applications(self) -> frozenset[str]:
        return frozenset().union(*(seen.declared for seen in self.authoritative.values()))


@dataclass
class SharedOutcome:
    """What one shared step did, folded into the pass summary."""

    applied: int = 0
    deleted: int = 0
    failures: list[str] = field(default_factory=list)
    status_reported: bool = False


# =============================================================================
# The reconciler
# =============================================================================


def _labels(obj: Mapping[str, Any]) -> dict[str, str]:
    return dict((obj.get("metadata") or {}).get("labels") or {})


def _name(obj: Mapping[str, Any]) -> str:
    return str((obj.get("metadata") or {}).get("name") or "")


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


class SharedReconciler:
    """Shared application objects, worker Secrets and member reach in one namespace.

    ``api`` is None without an internal worker token; the loop then treats
    every agent with shared content as undecidable and never calls
    ``ensure_application``.
    """

    def __init__(
        self,
        client: SharedClient,
        api: SharedApplicationApi | None,
        *,
        namespace: str,
        release: str,
    ) -> None:
        self._client = client
        self._api = api
        self._namespace = namespace
        self._release = release
        # The last database status reported per application, so a report is
        # sent only when it changes. In process only: a restarted worker
        # reports the current status once.
        self._reported: dict[str, tuple[str, str | None, int]] = {}
        # Applications whose Deployment the last ``ensure_application`` saw
        # ready. In process only: a restarted worker starts with none, so a
        # retained dedicated holder lasts one extra pass.
        self._ready_applications: set[str] = set()

    @property
    def ready(self) -> bool:
        """Whether applications can be reconciled: the worker routes need the token."""

        return self._api is not None

    @property
    def release(self) -> str:
        return self._release

    def application_ready(self, application_id: str) -> bool:
        """Whether the last ``ensure_application`` here observed the Deployment ready.

        False until then, and again after any later non-ready observation,
        refusal, disabled or unprovisioned render, transport failure, or GC.
        """

        return application_id in self._ready_applications

    # --- reach --------------------------------------------------------------------

    def reconcile_reach(self, observation: SharedObservation) -> SharedOutcome:
        """Apply each authoritative agent's granted reach and prune the rest of it.

        An unresolved agent's reach is not touched. Reach of an agent no
        active deployment names is a confirmed removal and is deleted.
        """

        outcome = SharedOutcome()
        selector = f"{COMPONENT_LABEL}={COMPONENT},{PART_OF_LABEL}={self._release},{AGENT_LABEL}"
        live: dict[str, list[str]] = {}
        for item in self._client.list_labelled(self._namespace, "NetworkPolicy", selector):
            live.setdefault(_labels(item).get(AGENT_LABEL, ""), []).append(_name(item))
        for agent_id, seen in sorted(observation.authoritative.items()):
            desired = {
                _name(obj): obj
                for application in sorted(seen.granted)
                for obj in seen.granted[application]
            }
            for obj in desired.values():
                self._apply(obj, outcome)
            for name in live.get(agent_id, []):
                if name not in desired:
                    self._delete("NetworkPolicy", name, outcome)
        for agent_id, names in sorted(live.items()):
            if agent_id in observation.deployed:
                continue
            for name in names:
                self._delete("NetworkPolicy", name, outcome)
        return outcome

    # --- application garbage ----------------------------------------------------------

    def collect_garbage(self, observation: SharedObservation) -> SharedOutcome:
        """Delete the objects of applications no authoritative agent declares.

        Only in a pass whose GC is unblocked: no agent unresolved and none
        undecidable. Secrets are never listed or deleted. The Deployments go
        first (foreground, so a terminating one stays listed); an
        application's Service and NetworkPolicies go only once a list after
        those deletes shows none of its Deployments, so its Pods never outlive
        their isolation.
        """

        outcome = SharedOutcome()
        if observation.gc_blocked:
            return outcome
        declared = observation.declared_applications()
        selector = (
            f"{COMPONENT_LABEL}={COMPONENT},{PART_OF_LABEL}={self._release},"
            f"{APPLICATION_LABEL},!{AGENT_LABEL}"
        )

        def undeclared(kind: str) -> list[tuple[str, dict[str, Any]]]:
            listed = self._client.list_labelled(self._namespace, kind, selector)
            return [
                (application, item)
                for item in listed
                if (application := _labels(item).get(APPLICATION_LABEL, "")) not in declared
            ]

        deployments = undeclared("Deployment")
        for application, item in deployments:
            self._forget(application)
            # Already terminating: the foreground delete is in progress.
            if not (item.get("metadata") or {}).get("deletionTimestamp"):
                self._delete("Deployment", _name(item), outcome)
        # Listed again: one whose Pods were gone is gone at once, while one
        # still terminating, or whose delete failed, holds its isolation.
        held = (
            {application for application, _ in undeclared("Deployment")} if deployments else set()
        )
        for kind in SHARED_KINDS:
            if kind == "Deployment":
                continue
            for application, item in undeclared(kind):
                self._forget(application)
                if application not in held:
                    self._delete(kind, _name(item), outcome)
        return outcome

    def _forget(self, application_id: str) -> None:
        self._reported.pop(application_id, None)
        self._ready_applications.discard(application_id)

    # --- one application -----------------------------------------------------------

    def ensure_application(self, application_id: str) -> SharedOutcome:
        """Converge one declared application. Raises only for transport failures."""

        outcome = SharedOutcome()
        # Ready only if this pass observes it so again.
        self._ready_applications.discard(application_id)
        try:
            self._ensure(application_id, outcome)
        except SharedReconcileRefused as refused:
            outcome.failures.append(refused.code)
        for failure in outcome.failures:
            logger.warning("shared application %s refused: %s", application_id, failure)
        return outcome

    def _ensure(self, application_id: str, outcome: SharedOutcome) -> None:
        api = self._api
        if api is None:
            raise SharedReconcileRefused(WORKER_TOKEN_MISSING)
        plan = api.render(application_id, namespace=self._namespace, release=self._release)
        try:
            enabled = plan["enabled"] is True
            objects: list[dict[str, Any]] = list(plan["objects"])
            secrets: Mapping[str, Any] = plan["secrets"]
            database: Mapping[str, Any] = secrets["database"]
            recipients = {str(recipient) for recipient in plan["recipients"]}
            operator_refs = [secrets["grant_store"], secrets["snapshot_seal"]]
            dsn_ref = database["secret"]
            revision = database["dsn_revision"]
        except (KeyError, TypeError):
            raise SharedReconcileRefused(RENDER_MALFORMED) from None
        if str(plan.get("application_id")) != application_id:
            raise SharedReconcileRefused(RENDER_MALFORMED)
        if not enabled:
            # Kept as it is: every member render refuses, so its reach is
            # pruned, and the resolver refuses calls.
            logger.debug("shared application %s: %s", application_id, APPLICATION_DISABLED)
            return

        public = self._ensure_key_secret(application_id)
        self._register(application_id, public, recipients)

        stale = False
        if database["mode"] == "bundled":
            sealed = database["sealed_dsn"]
            if sealed is None:
                # Not provisioned yet: no Deployment before its DSN Secret exists.
                return
            stale = self._ensure_dsn_secret(application_id, str(sealed)) == "stale"
        elif database["status"] is None:
            return
        elif not self._client.secret_exists_metadata_only(self._namespace, dsn_ref["name"]):
            self._report(application_id, "failed", DSN_SECRET_MISSING, revision, outcome)
            return
        for ref in operator_refs:
            if ref is not None and not self._client.secret_exists_metadata_only(
                self._namespace, str(ref["name"])
            ):
                raise SharedReconcileRefused(OPERATOR_SECRET_MISSING)

        # Policies, then the Service, then the Deployment, which waits for every
        # policy: no Pod of the application runs without its isolation. Any
        # other failure does not hold back the rest.
        applied = isolated = True
        for obj in sorted(objects, key=lambda obj: _APPLY_ORDER.get(str(obj.get("kind")), 1)):
            if obj.get("kind") == "Deployment" and not isolated:
                applied = False
                continue
            if not self._apply(obj, outcome):
                applied = False
                isolated = isolated and obj.get("kind") != "NetworkPolicy"
        if stale:
            self._report(application_id, "failed", DSN_SECRET_STALE, revision, outcome)
            return
        deployments = [_name(obj) for obj in objects if obj.get("kind") == "Deployment"]
        if not applied or len(deployments) != 1:
            return
        observed = self._readiness(application_id, deployments[0])
        if observed is not None:
            if observed[0] == "ready":
                self._ready_applications.add(application_id)
            self._report(application_id, observed[0], observed[1], revision, outcome)

    # --- Secrets ------------------------------------------------------------------

    def _secret_labels(self, application_id: str, role: str) -> dict[str, str]:
        return {
            APPLICATION_LABEL: application_id,
            SECRET_ROLE_LABEL: role,
            COMPONENT_LABEL: COMPONENT,
            PART_OF_LABEL: self._release,
        }

    def _create_once(
        self, name: str, labels: dict[str, str], key: str, content: Callable[[], str]
    ) -> dict[str, Any] | None:
        """The Secret as an exact list reads it, created first when the list misses it.

        None when even the readback misses it. A 409 is the same as a hit:
        what exists is read back and judged, never written.
        """

        found = self._client.list_secret(self._namespace, name, labels)
        if found is not None:
            return found
        body = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "namespace": self._namespace, "labels": dict(labels)},
            "type": "Opaque",
            "data": {key: _b64(content().encode("ascii"))},
        }
        self._client.create_secret(self._namespace, body)
        return self._client.list_secret(self._namespace, name, labels)

    def _secret_text(self, secret: Mapping[str, Any] | None, name: str, key: str) -> str | None:
        """The one data value of a worker Secret of the expected shape, decoded; else None."""

        if secret is None or _name(secret) != name or secret.get("type") != "Opaque":
            return None
        data = secret.get("data")
        if "stringData" in secret or not isinstance(data, Mapping) or set(data) != {key}:
            return None
        try:
            return strict_b64decode(str(data[key])).decode("ascii")
        except (ValueError, UnicodeDecodeError):
            return None

    def _ensure_key_secret(self, application_id: str) -> bytes:
        """The current raw public key the key Secret's private key derives."""

        name = key_secret_name(application_id)
        labels = self._secret_labels(application_id, ROLE_ENCRYPTION_KEY)
        secret = self._create_once(
            name,
            labels,
            KEY_SECRET_KEY,
            # One key line, current first: the resolver's key file format.
            lambda: _b64(bytes(nacl.public.PrivateKey.generate())) + "\n",
        )
        text = self._secret_text(secret, name, KEY_SECRET_KEY)
        if text is None:
            raise SharedReconcileRefused(KEY_SECRET_INVALID)
        try:
            ring = KeyRing.from_text(text)
        except ValueError:
            raise SharedReconcileRefused(KEY_SECRET_INVALID) from None
        return ring.public_key(ring.recipient_ids[0])

    def _register(self, application_id: str, public: bytes, recipients: set[str]) -> None:
        """Register the public key once; the private key never leaves the Secret."""

        recipient_id = hashlib.sha256(public).hexdigest()
        if recipient_id in recipients:
            return
        api = self._api
        assert api is not None
        answered = api.register_recipient(
            application_id,
            {
                "application_id": application_id,
                "recipient_id": recipient_id,
                "public_key": {"algorithm": RECIPIENT_ALGORITHM, "key": _b64(public)},
                # Computed from the Secret as read back, the key the resolver mounts.
                "readback": {"public_key_sha256": recipient_id},
            },
        )
        _validate_registration_response(
            answered, application_id=application_id, recipient_id=recipient_id
        )

    def _ensure_dsn_secret(self, application_id: str, sealed: str) -> Literal["current", "stale"]:
        """The bundled DSN Secret holds the API's sealed DSN, or reads ``stale``. Never patched."""

        name = dsn_secret_name(application_id)
        labels = self._secret_labels(application_id, ROLE_DATABASE_DSN)
        # The API's base64 sealed DSN, verbatim; the resolver opens it unchanged.
        secret = self._create_once(name, labels, DSN_SECRET_KEY, lambda: sealed)
        text = self._secret_text(secret, name, DSN_SECRET_KEY)
        if text is None:
            raise SharedReconcileRefused(DSN_SECRET_INVALID)
        held = "".join(text.split())
        try:
            strict_b64decode(held)
        except (ValueError, binascii.Error):
            raise SharedReconcileRefused(DSN_SECRET_INVALID) from None
        return "current" if held == "".join(sealed.split()) else "stale"

    # --- objects and status ----------------------------------------------------------

    def _apply(self, obj: dict[str, Any], outcome: SharedOutcome) -> bool:
        try:
            self._client.apply(self._namespace, obj)
        except ApiException as exc:
            outcome.failures.append(f"{APPLY_FAILED} {obj.get('kind')}/{_name(obj)} {exc.status}")
            return False
        outcome.applied += 1
        return True

    def _delete(self, kind: str, name: str, outcome: SharedOutcome) -> None:
        try:
            self._client.delete(self._namespace, kind, name)
        except ApiException as exc:
            if exc.status == 404:
                return
            outcome.failures.append(f"{DELETE_FAILED} {kind}/{name} {exc.status}")
            return
        outcome.deleted += 1

    def _readiness(self, application_id: str, name: str) -> tuple[str, str | None] | None:
        """``ready``, ``failed``/``deployment_unavailable``, or None while undecided.

        Read from the live Deployment's status at its current generation:
        Available, or Progressing False (its progress deadline passed).
        """

        listed = self._client.list_labelled(
            self._namespace,
            "Deployment",
            {
                APPLICATION_LABEL: application_id,
                COMPONENT_LABEL: COMPONENT,
                PART_OF_LABEL: self._release,
            },
        )
        live = next((item for item in listed if _name(item) == name), None)
        if live is None:
            return None
        status = live.get("status") or {}
        generation = (live.get("metadata") or {}).get("generation")
        observed = status.get("observedGeneration")
        if not isinstance(generation, int) or not isinstance(observed, int):
            return None
        if observed < generation:
            return None
        conditions = {
            str(condition.get("type")): str(condition.get("status"))
            for condition in status.get("conditions") or []
            if isinstance(condition, Mapping)
        }
        if conditions.get("Progressing") == "False":
            return "failed", DEPLOYMENT_UNAVAILABLE
        if conditions.get("Available") == "True":
            return "ready", None
        return None

    def _report(
        self,
        application_id: str,
        status: str,
        reason: str | None,
        revision: object,
        outcome: SharedOutcome,
    ) -> None:
        if not isinstance(revision, int):
            return
        report = (status, reason, revision)
        if self._reported.get(application_id) == report:
            return
        api = self._api
        assert api is not None
        body: dict[str, Any] = {"status": status, "dsn_revision": revision}
        if reason is not None:
            body["reason"] = reason
        api.report_status(application_id, body)
        self._reported[application_id] = report
        outcome.status_reported = True
