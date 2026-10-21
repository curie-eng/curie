"""Signed per-call observation channel between control and provisioner, ADR 0236.

@spec PROTECTED-COMMISSIONING-5. The control process creates one immutable
request object per guard call. It carries a fresh nonce, the channel ID and the
exact digests ADR 0236 names. The provisioner signs a reply over that content
plus the request object's UID, and only after its own fresh reobservation
returns exactly the same digests. The signing key never leaves the provisioner:
the requester accepts only a pinned public key. A stale reply, a reply for
another request, channel or installation, or a changed binding cannot verify.
Any refusal permanently fences that end. Nothing here creates a successful
guard, acceptance, Qualification or Readiness by itself.

Precondition: only the provisioner may create ConfigMaps in the control
namespace (rendered control RBAC; candidate and ordinary principals are
observed to be denied). Any other principal can cause only a fail-closed
refusal: the signature binds the requester's own object UID and nonce, so a
planted or squatted object never yields a positive observation.

The ConfigMap calls use the bounded transport of ``KubernetesLaunchAPI``
directly. Its public methods are a closed launch allowlist (pods, services and
secrets), which this channel does not widen.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, NoReturn
from urllib.parse import quote
from uuid import UUID, uuid4

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from .protected_launch_kubernetes import KubernetesLaunchAPI

# ADR 0236, decision step 1: exactly these facts, each as a SHA-256 digest.
OBSERVATION_FACTS = (
    "permit",
    "manifest",
    "bootstrap",
    "control_profile",
    "projection",
    "primitive_completions",
    "network",
    "hostile_peer",
    "retained_peer",
    "installation",
    "cni",
    "artifact",
)
CHANNEL_LABEL = "curietech.ai/commissioning-observation"
KIND_LABEL = "curietech.ai/commissioning-observation-kind"
# Domain separation: this key's signatures mean only "this observation reply".
SIGNATURE_DOMAIN = b"curie.commissioning.observation-reply.v1\n"
_REQUEST, _REPLY = "observation-request-", "observation-reply-"
_MAX_BYTES = 16384
_DIGEST = re.compile(r"[0-9a-f]{64}")
_NAMESPACE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


class ObservationChannelRefused(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Commissioning observation channel refused")


def _refuse() -> NoReturn:
    raise ObservationChannelRefused() from None


def _require(value: bool) -> None:
    if not value:
        _refuse()


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode()


def _spki(key: Ed25519PublicKey) -> str:
    raw = key.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    return hashlib.sha256(raw).hexdigest()


def _facts(value: object) -> dict[str, str]:
    _require(isinstance(value, Mapping) and set(value) == set(OBSERVATION_FACTS))
    assert isinstance(value, Mapping)
    out = {}
    for name in OBSERVATION_FACTS:
        item = value[name]
        _require(type(item) is str and _DIGEST.fullmatch(item) is not None)
        out[name] = item
    return out


def _present(value: dict[str, Any] | None) -> dict[str, Any]:
    _require(type(value) is dict)
    assert value is not None
    return value


def _uuid(value: object) -> str:
    _require(type(value) is str)
    assert isinstance(value, str)
    _require(str(UUID(value)) == value)
    return value


def _namespace(value: object) -> str:
    _require(type(value) is str and _NAMESPACE.fullmatch(str(value)) is not None)
    return str(value)


def verify_reply(
    raw: bytes,
    signature: str,
    *,
    expected: Mapping[str, str],
    request_uid: str,
    key: Ed25519PublicKey,
) -> None:
    """Exact signed content: the request content plus this request object's UID."""
    try:
        _require(type(raw) is bytes and 0 < len(raw) <= _MAX_BYTES)
        _require(isinstance(key, Ed25519PublicKey) and type(signature) is str)
        key.verify(base64.b64decode(signature, validate=True), SIGNATURE_DOMAIN + raw)
        value = json.loads(raw)
        _require(value == dict(expected) | {"request_uid": request_uid})
        _require(canonical(value) == raw)
    except ObservationChannelRefused:
        raise
    except Exception:  # noqa: BLE001 -- signature/codec diagnostics never escape.
        _refuse()


def _delete_options(uid: str) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}}


class ObservationRequester:
    """Control process side. One fresh, signed provisioner reobservation per call."""

    def __init__(
        self,
        *,
        api: KubernetesLaunchAPI,
        namespace: str,
        channel: str,
        key: Ed25519PublicKey,
        pinned_spki_sha256: str,
        timeout_s: float = 30.0,
    ) -> None:
        _require(type(api) is KubernetesLaunchAPI)
        self._namespace = _namespace(namespace)
        self.channel = _uuid(channel)
        # A private key is never an Ed25519PublicKey: the signing key cannot enter here.
        _require(isinstance(key, Ed25519PublicKey) and _spki(key) == pinned_spki_sha256)
        _require(type(timeout_s) in (int, float) and 0 < timeout_s <= 120)
        self._api, self._key, self._timeout = api, key, timeout_s
        self._prefix = "/api/v1/namespaces/" + self._namespace + "/configmaps"
        self._gate = asyncio.Lock()
        self._failed = False
        self.calls = 0

    async def require(self, facts: Mapping[str, str]) -> None:
        """Return only after a verified reply bound to these exact digests."""
        _require(not self._failed)
        try:
            async with self._gate:
                _require(not self._failed)
                await self._require(_facts(facts))
        except BaseException:
            self._failed = True
            raise

    async def _require(self, facts: dict[str, str]) -> None:
        nonce = str(uuid4())
        expected = facts | {"nonce": nonce, "channel": self.channel}
        content = canonical(expected).decode()
        request = response = None
        try:
            async with asyncio.timeout(self._timeout):
                request = _present(
                    await self._api._request(
                        "POST",
                        self._prefix,
                        {
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "immutable": True,
                            "metadata": {
                                "name": _REQUEST + nonce,
                                "namespace": self._namespace,
                                "labels": {CHANNEL_LABEL: self.channel, KIND_LABEL: "request"},
                            },
                            "data": {"request": content},
                        },
                    )
                )
                # The signed UID must name the object the API stored exactly as sent.
                _require(request.get("immutable") is True)
                _require(request.get("data") == {"request": content})
                uid = _uuid(request["metadata"]["uid"])
                while response is None:
                    response = await self._api._request(
                        "GET", self._prefix + "/" + _REPLY + nonce, absent=True
                    )
                    if response is None:
                        await asyncio.sleep(0.05)
                _require(response.get("immutable") is True)
                data = response["data"]
                _require(type(data) is dict and data.keys() == {"observation", "signature"})
                _require(type(data["observation"]) is str)
                verify_reply(
                    data["observation"].encode(),
                    data["signature"],
                    expected=expected,
                    request_uid=uid,
                    key=self._key,
                )
                self.calls += 1
        except ObservationChannelRefused:
            raise
        except Exception:  # noqa: BLE001 -- includes timeout: never a positive observation.
            _refuse()
        finally:
            for item in (response, request):
                if item is not None:
                    try:
                        meta = item["metadata"]
                        await self._api._request(
                            "DELETE",
                            self._prefix + "/" + meta["name"],
                            _delete_options(meta["uid"]),
                            absent=True,
                        )
                    except Exception:  # noqa: BLE001 -- the responder's sweep owns leftovers.
                        pass


class ObservationResponder:
    """Provisioner side. Signs a request only after fresh reobservation matches it.

    ``facts`` are the premeasured digests this installation is bound to.
    ``reobserve`` is the trusted provisioner composition over its own producers;
    each call returns freshly observed digests and is never cached. ``register``
    records ownership of a reply object before it is created. A refused request
    fences permanently.
    """

    def __init__(
        self,
        *,
        api: KubernetesLaunchAPI,
        namespace: str,
        channel: str,
        key: Ed25519PrivateKey,
        facts: Mapping[str, str],
        reobserve: Callable[[], Awaitable[Mapping[str, str]]],
        register: Callable[[str], None] | None = None,
        maximum: int = 4096,
    ) -> None:
        _require(type(api) is KubernetesLaunchAPI)
        self._namespace = _namespace(namespace)
        self.channel = _uuid(channel)
        _require(isinstance(key, Ed25519PrivateKey) and callable(reobserve))
        _require(register is None or callable(register))
        _require(type(maximum) is int and 0 < maximum <= 65536)
        self._api, self._key = api, key
        self._facts, self._reobserve, self._register = _facts(facts), reobserve, register
        self._maximum = maximum
        self._prefix = "/api/v1/namespaces/" + self._namespace + "/configmaps"
        self._failed = False
        self.answered: list[dict[str, Any]] = []

    async def answer(self, request: dict[str, Any]) -> None:
        _require(not self._failed)
        try:
            await self._answer(request)
        except BaseException as error:
            self._failed = True
            if not isinstance(error, Exception):
                raise
            _refuse()

    async def _answer(self, request: dict[str, Any]) -> None:
        _require(len(self.answered) < self._maximum)
        meta = request["metadata"]
        _require(request.get("immutable") is True and meta.get("namespace") == self._namespace)
        _require(meta.get("labels", {}).get(CHANNEL_LABEL) == self.channel)
        uid = _uuid(meta["uid"])
        data = request["data"]
        _require(type(data) is dict and data.keys() == {"request"})
        raw = data["request"]
        _require(type(raw) is str and 0 < len(raw) <= _MAX_BYTES)
        value = json.loads(raw)
        _require(canonical(value) == raw.encode())
        nonce = _uuid(value.get("nonce"))
        _require(meta["name"] == _REQUEST + nonce)
        _require(value == self._facts | {"nonce": nonce, "channel": self.channel})
        started = asyncio.get_running_loop().time()
        _require(_facts(await self._reobserve()) == self._facts)
        observed = canonical(value | {"request_uid": uid})
        if self._register is not None:
            self._register(self._prefix + "/" + _REPLY + nonce)
        await self._api._request(
            "POST",
            self._prefix,
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "immutable": True,
                "metadata": {
                    "name": _REPLY + nonce,
                    "namespace": self._namespace,
                    "labels": {CHANNEL_LABEL: self.channel, KIND_LABEL: "reply"},
                },
                "data": {
                    "observation": observed.decode(),
                    "signature": base64.b64encode(
                        self._key.sign(SIGNATURE_DOMAIN + observed)
                    ).decode(),
                },
            },
        )
        self.answered.append(
            {
                "request_uid": uid,
                "nonce": nonce,
                "reobservation_seconds": round(asyncio.get_running_loop().time() - started, 3),
            }
        )

    async def serve(self, stop: asyncio.Event, *, deadline_s: float) -> None:
        """Finite loop; never answers from a cache. The caller joins it before teardown."""
        _require(isinstance(stop, asyncio.Event))
        _require(type(deadline_s) in (int, float) and 0 < deadline_s <= 3600)
        seen: set[str] = set()
        selector = CHANNEL_LABEL + "=" + self.channel + "," + KIND_LABEL + "=request"
        path = self._prefix + "?labelSelector=" + quote(selector, safe="")
        try:
            async with asyncio.timeout(deadline_s):
                while not stop.is_set():
                    collection = _present(await self._api._request("GET", path))
                    _require(not collection.get("metadata", {}).get("continue"))
                    rows = collection["items"]
                    _require(type(rows) is list and len(rows) <= 8)
                    for request in rows:
                        uid = request["metadata"]["uid"]
                        if uid in seen:
                            continue
                        seen.add(uid)
                        await self.answer(request)
                    try:
                        await asyncio.wait_for(stop.wait(), 0.05)
                    except TimeoutError:
                        pass
        except ObservationChannelRefused:
            raise
        except Exception:  # noqa: BLE001
            self._failed = True
            _refuse()

    async def sweep(self) -> int:
        """Delete this channel's leftover request and reply objects by exact UID."""
        selector = CHANNEL_LABEL + "=" + self.channel
        path = self._prefix + "?labelSelector=" + quote(selector, safe="")
        try:
            collection = _present(await self._api._request("GET", path))
            _require(not collection.get("metadata", {}).get("continue"))
            removed = 0
            for item in collection["items"]:
                meta = item["metadata"]
                _require(meta["labels"][CHANNEL_LABEL] == self.channel)
                await self._api._request(
                    "DELETE",
                    self._prefix + "/" + meta["name"],
                    _delete_options(_uuid(meta["uid"])),
                    absent=True,
                )
                removed += 1
            return removed
        except ObservationChannelRefused:
            raise
        except Exception:  # noqa: BLE001
            _refuse()
