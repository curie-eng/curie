"""The guard: a fail-closed validating admission server for the workload namespace.

@spec PROTECTED-COMMISSIONING-8. The guard permits only the measured image,
service account, mounts, Secret references, network and ephemeral container
policy. For each request the API server sends under the control profile's
webhook configuration, this module answers allow only for:

- a Pod CREATE by the mediator whose object equals the measured template with
  its own slots, every container list included: regular, init and ephemeral
  containers are compared alike, and the template has exactly one runner and
  no init or ephemeral container;
- a Pod UPDATE by the mediator or the provisioner that removes the launch
  finalizer and changes nothing else but managed field bookkeeping;
- a Secret or Service CREATE by the mediator whose object has the launch
  mechanics' fixed shape under a slot name.

Everything else the webhook routes here refuses: ephemeral container, resize,
exec, attach and port-forward requests; any update of a Secret, Service or
the namespace; any change to ServiceAccounts, tokens, NetworkPolicies, Roles or
RoleBindings, which the provisioner creates before the webhook exists.

What this prevents and what it only detects: the API server consults the guard
before persisting a matched request, so an off-template workload in the
workload namespace is refused, not observed afterwards. The webhook
configuration itself cannot be guarded by any admission webhook, so its
removal or change, and any request made while it is absent, is detected by
reobservation of the control profile fact, never prevented here. The webhook
fails closed: if this server is unreachable, slow or answers anything but an
allow, the API server refuses.

Answers are not authority. The server authenticates no caller and holds no
Kubernetes or broker credential; only the API server, on its own request,
turns an answer into an admission decision.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import re
import ssl
from dataclasses import dataclass
from typing import Any, NoReturn

from aiohttp import web
from aiohttp.web_protocol import RequestHandler
from aiohttp.web_request import BaseRequest
from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding
from curie_protected_hooks.authority_records import _scalar

from .commissioning_control_profile import (
    GUARD_PATH,
    GUARD_SERVER_LABEL,
    GUARD_TARGET_PORT,
    ControlProfile,
    parse_control_profile,
    server_ca_digest,
)
from .commissioning_control_slots import (
    expected_pod,
    pod_slots,
    slot_for_name,
)
from .protected_kubernetes_projection import (
    _MANAGED_FIELDS,
    Projection,
    _defaults,
    _exact,
    parse_projection,
)
from .protected_launch_kubernetes import FINALIZER, secret_document, service_document
from .protected_launch_records import _decode, _text, _timestamp, canonical_json

MESSAGE = "Protected control admission refused"
CONFIG_DIR = "/etc/curie/protected-guard"
_CAP = 1048576
_VERSION = "admission.k8s.io/v1"
_REQUEST = {
    "uid",
    "kind",
    "resource",
    "subResource",
    "requestKind",
    "requestResource",
    "requestSubResource",
    "name",
    "namespace",
    "operation",
    "userInfo",
    "object",
    "oldObject",
    "dryRun",
    "options",
}
# Server bookkeeping the API server adds to a created object before validating
# admission. Each is bounded here; none can stand in for a measured field.
_CREATED = {"uid", "creationTimestamp", "managedFields", "generation", "resourceVersion"}
_SECRET_KEYS = ("runner-token", "history-token", "model-credential")


class ControlAdmissionInvalid(ValueError):
    """Fixed refusal, detached from the request and from any credential."""

    def __init__(self) -> None:
        super().__init__(MESSAGE)


def _invalid() -> NoReturn:
    error = ControlAdmissionInvalid()
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


def _require(condition: bool) -> None:
    if not condition:
        _invalid()


def _dict(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        _invalid()
    result: dict[str, Any] = value
    return result


@dataclass(frozen=True, slots=True, repr=False)
class GuardPolicy:
    """The profile and projection one guard enforces; bound, not authenticated."""

    profile: ControlProfile
    projection: Projection

    def __post_init__(self) -> None:
        try:
            _require(type(self.profile) is ControlProfile)
            _require(type(self.projection) is Projection)
            v, p = self.profile.as_dict(), self.projection.as_dict()
            _require(v["projection_digest"] == self.projection.digest)
            _require(v["workload_namespace"] == p["namespace"])
            _require(v["authority_domain_id"] == p["authority_domain_id"])
        except ControlAdmissionInvalid:
            raise
        except Exception:  # noqa: BLE001 - fixed refusal
            _invalid()

    def __repr__(self) -> str:
        return "<GuardPolicy>"


def _request(review: Any) -> dict[str, Any]:
    value = _dict(review)
    _require(value.keys() == {"apiVersion", "kind", "request"})
    _require(value["apiVersion"] == _VERSION and value["kind"] == "AdmissionReview")
    q = _dict(value["request"])
    _require(q.keys() <= _REQUEST)
    _require({"uid", "kind", "resource", "operation", "userInfo"} <= q.keys())
    _scalar(q["uid"], "uuid")
    user = _dict(q["userInfo"])
    _require(user.keys() <= {"username", "uid", "groups", "extra"})
    _text(user.get("username"), 512)
    # Dry-run requests are decided exactly like real ones: a dry run persists
    # nothing, and deciding it alike lets an observer measure the decision.
    _require(type(q.get("dryRun", False)) is bool)
    return q


def _resource(q: dict[str, Any]) -> tuple[str, str, str]:
    resource = _dict(q["resource"])
    _require(resource.keys() == {"group", "version", "resource"})
    group, version, name = resource["group"], resource["version"], resource["resource"]
    _require(all(type(x) is str for x in (group, version, name)) and version == "v1")
    sub = q.get("subResource", "")
    _require(type(sub) is str)
    for key, expected in (("requestResource", resource), ("requestSubResource", sub)):
        if key in q:
            _exact(q[key], expected)
    return group, name, sub


def _bookkeeping(meta: dict[str, Any]) -> None:
    """Bound the server-assigned metadata of an object being created."""
    if "uid" in meta:
        _scalar(meta["uid"], "uuid")
    if meta.get("creationTimestamp") is not None:
        _timestamp(meta["creationTimestamp"])
    if "generation" in meta:
        _require(type(meta["generation"]) is int and meta["generation"] == 1)
    _require(meta.get("resourceVersion", "") == "")
    managed = meta.get("managedFields", [])
    _require(type(managed) is list and len(managed) <= 16)
    for row in managed:
        _require(type(row) is dict and row.keys() <= _MANAGED_FIELDS)
        for key in row.keys() - {"fieldsV1", "time"}:
            _text(row[key])
        if "time" in row:
            _timestamp(row["time"])
        if "fieldsV1" in row:
            _require(type(row["fieldsV1"]) is dict)


def _created(obj: Any, expected: dict[str, Any]) -> dict[str, Any]:
    """A private copy of a created object with bookkeeping checked and removed."""
    value = _decode(canonical_json(obj), _CAP)
    _require(type(value) is dict and value.keys() <= expected.keys() | {"status"})
    meta = _dict(value.get("metadata"))
    _require(meta.keys() <= expected["metadata"].keys() | _CREATED)
    _bookkeeping(meta)
    for key in _CREATED:
        meta.pop(key, None)
    result: dict[str, Any] = value
    return result


def _containers(spec: dict[str, Any], template: dict[str, Any]) -> None:
    # Every container list is compared alike against the template's list.
    for key in ("containers", "initContainers", "ephemeralContainers"):
        observed = spec.get(key, [])
        _require(type(observed) is list)
        _require(len(observed) == len(template.get(key, [])))


def _pod_create(q: dict[str, Any], policy: GuardPolicy, user: str) -> None:
    v = policy.profile.as_dict()
    _require(user == v["principals"]["mediator"])
    _require(q.get("oldObject") is None)
    expected = expected_pod(policy.projection, pod_slots(q["object"]))
    _require(q.get("name", expected["metadata"]["name"]) == expected["metadata"]["name"])
    pod = _created(q["object"], expected)
    _require(pod.get("apiVersion") == "v1" and pod.get("kind") == "Pod")
    _exact(pod["metadata"], expected["metadata"])
    status = pod.pop("status", {})
    _require(status in ({}, {"phase": "Pending", "qosClass": "Guaranteed"}))
    spec = _dict(pod["spec"])
    _containers(spec, expected["spec"])
    _require("nodeName" not in spec)
    _defaults(spec)
    _exact(spec, expected["spec"])


def _pod_update(q: dict[str, Any], policy: GuardPolicy, user: str) -> None:
    v = policy.profile.as_dict()
    _require(user in (v["principals"]["mediator"], v["principals"]["provisioner"]))
    old = _decode(canonical_json(_dict(q.get("oldObject"))), _CAP)
    new = _decode(canonical_json(_dict(q["object"])), _CAP)
    old_meta, new_meta = _dict(old.get("metadata")), _dict(new.get("metadata"))
    _require(old_meta.get("finalizers") == [FINALIZER])
    _require(new_meta.get("finalizers", []) == [])
    for meta in (old_meta, new_meta):
        meta.pop("managedFields", None)
        meta.pop("finalizers", None)
    # Removing the only finalizer is the whole change; the slot must still be a
    # launch Pod so this path cannot release any other object.
    slot_for_name(old_meta.get("name"), "pod")
    _exact(new, old)


def _secret_create(q: dict[str, Any], policy: GuardPolicy, user: str) -> None:
    v = policy.profile.as_dict()
    _require(user == v["principals"]["mediator"])
    obj = _dict(q["object"])
    name = _dict(obj.get("metadata")).get("name")
    slot = slot_for_name(name, "secret")
    expected = secret_document(
        v["workload_namespace"], slot.secret_name, dict.fromkeys(_SECRET_KEYS, "x")
    )
    secret = _created(obj, expected)
    data = _dict(secret.pop("data", None))
    _require(data.keys() == set(_SECRET_KEYS))
    for value in data.values():
        _require(type(value) is str and 0 < len(value) <= 12000)
        _require(0 < len(base64.b64decode(value, validate=True)) <= 8192)
    expected.pop("data")
    _exact(secret, expected)


def _service_create(q: dict[str, Any], policy: GuardPolicy, user: str) -> None:
    v = policy.profile.as_dict()
    _require(user == v["principals"]["mediator"])
    obj = _dict(q["object"])
    slot = slot_for_name(_dict(obj.get("metadata")).get("name"), "service")
    expected = service_document(v["workload_namespace"], slot.service_name, slot.launch_id)
    service = _created(obj, expected)
    _require(service.pop("status", {}) in ({}, {"loadBalancer": {}}))
    _service_defaults(_dict(service["spec"]))
    _exact(service, expected)


def _service_defaults(spec: dict[str, Any]) -> None:
    """Only the API server's named Service defaults for a headless ClusterIP."""
    for key, value in (
        ("clusterIPs", ["None"]),
        ("sessionAffinity", "None"),
        ("ipFamilyPolicy", "SingleStack"),
        ("internalTrafficPolicy", "Cluster"),
    ):
        if key in spec:
            _require(spec.pop(key) == value)
    if "ipFamilies" in spec:
        families = spec.pop("ipFamilies")
        _require(families in (["IPv4"], ["IPv6"]))


def _decide(review: Any, policy: GuardPolicy) -> None:
    q = _request(review)
    group, resource, sub = _resource(q)
    operation, user = q["operation"], q["userInfo"]["username"]
    v = policy.profile.as_dict()
    if resource == "namespaces":
        _invalid()
    _require(q.get("namespace") == v["workload_namespace"])
    if (group, resource, sub, operation) == ("", "pods", "", "CREATE"):
        _pod_create(q, policy, user)
    elif (group, resource, sub, operation) == ("", "pods", "", "UPDATE"):
        _pod_update(q, policy, user)
    elif (group, resource, sub, operation) == ("", "secrets", "", "CREATE"):
        _secret_create(q, policy, user)
    elif (group, resource, sub, operation) == ("", "services", "", "CREATE"):
        _service_create(q, policy, user)
    else:
        _invalid()


def review(value: object, *, policy: GuardPolicy) -> dict[str, Any]:
    """One AdmissionReview response; refusal unless a rule above allows.

    The response echoes the request UID only when it is a canonical UUID.
    """
    uid = ""
    try:
        request = value.get("request") if type(value) is dict else None
        if type(request) is dict:
            candidate = request.get("uid")
            _scalar(candidate, "uuid")
            uid = str(candidate)
    except Exception:  # noqa: BLE001 - an unusable UID is never reflected
        uid = ""
    allowed = False
    try:
        _require(type(policy) is GuardPolicy and bool(uid))
        _decide(value, policy)
        allowed = True
    except Exception:  # noqa: BLE001 - every failure is the same refusal
        allowed = False
    response: dict[str, Any] = {"uid": uid, "allowed": allowed}
    if not allowed:
        response["status"] = {"code": 403, "message": MESSAGE}
    return {"apiVersion": _VERSION, "kind": "AdmissionReview", "response": response}


_QUIET = logging.Logger("protected-control-admission")
_QUIET.disabled = True


class _SafeHandler(RequestHandler):
    def handle_error(
        self,
        request: BaseRequest,
        status: int = 500,
        exc: BaseException | None = None,
        message: str | None = None,
    ) -> web.StreamResponse:
        # The parser embeds raw request bytes in its errors; never forward them.
        response = web.Response(status=403, text=MESSAGE)
        response.force_close()
        return response


class _GuardServer(web.Server):
    def __init__(self, original: web.Server) -> None:
        super().__init__(
            original.request_handler,
            request_factory=original.request_factory,
            handler_cancellation=original.handler_cancellation,
        )

    def __call__(self) -> RequestHandler:
        return _SafeHandler(
            self, loop=self._loop, access_log=_QUIET, logger=_QUIET, auto_decompress=False
        )


class _GuardRunner(web.AppRunner):
    async def _make_server(self) -> web.Server:
        return _GuardServer(await super()._make_server())


class ControlAdmissionServer:
    """HTTPS for the API server's admission calls; no other route, no logs.

    The serving certificate must chain to the profile's pinned CA, the same CA
    the rendered webhook configuration names as its ``caBundle``.
    """

    def __init__(
        self,
        policy: GuardPolicy,
        *,
        certificate_pem: bytes,
        key_file: str,
        certificate_file: str,
        ca_pem: bytes,
    ) -> None:
        try:
            _require(type(policy) is GuardPolicy)
            pinned = policy.profile.as_dict()["guard"]["server_ca_sha256"]
            _require(server_ca_digest(ca_pem) == pinned)
            certificate = x509.load_pem_x509_certificate(certificate_pem)
            _require(certificate.public_bytes(Encoding.PEM) == certificate_pem)
            certificate.verify_directly_issued_by(x509.load_pem_x509_certificate(ca_pem))
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(certificate_file, key_file)
            self.tls_context = context
            self._policy = policy
            self.application = web.Application(
                client_max_size=_CAP,
                middlewares=[self._refusals],
                handler_args={"auto_decompress": False},
            )
            self.application.router.add_route("*", "/{tail:.*}", self._handle)
        except Exception:  # noqa: BLE001 - setup never retains key or file errors
            _invalid()

    def runner(self) -> web.AppRunner:
        """The only supported runner: fixed parser errors and no access log."""
        return _GuardRunner(self.application, access_log=None)

    @web.middleware
    async def _refusals(self, request: web.Request, handler: Any) -> web.StreamResponse:
        try:
            async with asyncio.timeout(5):
                result: web.StreamResponse = await handler(request)
                return result
        except Exception:  # noqa: BLE001 - fixed output, nothing logged
            return web.Response(status=403, text=MESSAGE)

    async def _handle(self, request: web.Request) -> web.Response:
        _require(type(request.protocol) is _SafeHandler)
        _require(request.method == "POST" and request.rel_url.raw_path == GUARD_PATH)
        _require(request.headers.get("Content-Encoding", "identity") == "identity")
        _require(request.content_length is None or request.content_length <= _CAP)
        body = bytearray()
        async for part in request.content.iter_chunked(16384):
            _require(len(body) + len(part) <= _CAP)
            body.extend(part)
        value = _decode(bytes(body), _CAP)
        response = review(value, policy=self._policy)
        _require(bool(response["response"]["uid"]))
        return web.json_response(response)


def render_guard_deployment(profile: ControlProfile, *, config_secret: str) -> dict[str, Any]:
    """The guard's own workload: pinned image, no API credential, read-only.

    ``config_secret`` holds the profile, projection, CA and serving key pair
    under fixed keys; the guard reads nothing else.
    """
    try:
        _require(type(profile) is ControlProfile)
        v = profile.as_dict()
        _require(
            type(config_secret) is str
            and re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", config_secret) is not None
        )
        guard = v["guard"]
        labels = {GUARD_SERVER_LABEL: profile.digest[:32]}
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": guard["service_name"], "namespace": v["control_namespace"]},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": dict(labels)},
                "template": {
                    "metadata": {"labels": dict(labels)},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "enableServiceLinks": False,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "runAsGroup": 1000,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "guard",
                                "image": guard["image"],
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "python",
                                    "-m",
                                    "curie_worker.commissioning_control_admission",
                                ],
                                "ports": [{"name": "https", "containerPort": GUARD_TARGET_PORT}],
                                "readinessProbe": {
                                    "tcpSocket": {"port": GUARD_TARGET_PORT},
                                    "periodSeconds": 2,
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "volumeMounts": [
                                    {"name": "config", "mountPath": CONFIG_DIR, "readOnly": True}
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "config",
                                "secret": {"secretName": config_secret, "defaultMode": 0o440},
                            }
                        ],
                    },
                },
            },
        }
    except ControlAdmissionInvalid:
        raise
    except Exception:  # noqa: BLE001 - fixed refusal
        _invalid()


def _read(name: str, cap: int = 262144) -> bytes:
    fd = os.open(os.path.join(CONFIG_DIR, name), os.O_RDONLY | os.O_CLOEXEC)
    try:
        raw = os.read(fd, cap + 1)
    finally:
        os.close(fd)
    _require(0 < len(raw) <= cap)
    return raw


async def _serve() -> None:
    policy = GuardPolicy(
        parse_control_profile(_read("profile.json")), parse_projection(_read("projection.json"))
    )
    server = ControlAdmissionServer(
        policy,
        certificate_pem=_read("tls.crt"),
        certificate_file=os.path.join(CONFIG_DIR, "tls.crt"),
        key_file=os.path.join(CONFIG_DIR, "tls.key"),
        ca_pem=_read("ca.crt"),
    )
    runner = server.runner()
    await runner.setup()
    site = web.TCPSite(runner, port=GUARD_TARGET_PORT, ssl_context=server.tls_context)
    await site.start()
    await asyncio.Event().wait()


def main() -> None:
    """Serve the guard from the fixed configuration directory; no flags."""
    try:
        asyncio.run(_serve())
    except Exception:  # noqa: BLE001 - fixed exit, no configuration detail
        raise SystemExit(MESSAGE) from None


if __name__ == "__main__":
    main()


__all__ = [
    "CONFIG_DIR",
    "MESSAGE",
    "ControlAdmissionInvalid",
    "ControlAdmissionServer",
    "GuardPolicy",
    "render_guard_deployment",
    "review",
]
