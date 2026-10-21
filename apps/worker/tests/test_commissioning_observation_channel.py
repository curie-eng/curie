"""Signed per-call observation channel, ADR 0236 (@spec PROTECTED-COMMISSIONING-5).

A fake Kubernetes ConfigMap API behind the actual bounded HTTPS client, and real
Ed25519 signatures. These tests prove request/reply binding and refusal only; no
installation is observed and nothing here is campaign evidence.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from curie_worker.protected_launch_kubernetes import KubernetesLaunchAPI

NAMESPACE = "acme-control"
DOMAIN = "11111111-1111-4111-8111-111111111111"
FACTS = (
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
DIGESTS = {name: hashlib.sha256(name.encode()).hexdigest() for name in FACTS}
PREFIX = "/api/v1/namespaces/" + NAMESPACE + "/configmaps"


def product():
    import importlib

    return importlib.import_module("curie_worker.commissioning_observation_channel")


class Store:
    """Minimal ConfigMap API: create, get, label list and UID-preconditioned delete."""

    def __init__(self):
        self.objects, self.deleted, self.created = {}, [], []

    def handle(self, request):
        path = urlsplit(str(request.url)).path
        assert path.startswith(PREFIX)
        name = path[len(PREFIX) + 1 :] if len(path) > len(PREFIX) else None
        if request.method == "POST":
            value = json.loads(request.content)
            name = value["metadata"]["name"]
            if name in self.objects:
                return httpx.Response(409, json={})
            value["metadata"].update(uid=str(uuid4()), resourceVersion="1", namespace=NAMESPACE)
            self.objects[name] = value
            self.created.append(json.loads(json.dumps(value)))
            return httpx.Response(201, json=value)
        if request.method == "GET" and name:
            if name in self.objects:
                return httpx.Response(200, json=self.objects[name])
            return httpx.Response(404, json={})
        if request.method == "GET":
            selector = parse_qs(urlsplit(str(request.url)).query)["labelSelector"][0]
            wanted = dict(term.split("=", 1) for term in selector.split(","))
            items = [
                o
                for o in self.objects.values()
                if all(o["metadata"].get("labels", {}).get(k) == v for k, v in wanted.items())
            ]
            return httpx.Response(200, json={"metadata": {}, "items": items})
        if request.method == "DELETE":
            uid = json.loads(request.content)["preconditions"]["uid"]
            current = self.objects.get(name)
            if current is None:
                return httpx.Response(404, json={})
            assert current["metadata"]["uid"] == uid
            del self.objects[name]
            self.deleted.append(name)
            return httpx.Response(200, json={})
        raise AssertionError(request.method)


def api(store, handler=None):
    result = object.__new__(KubernetesLaunchAPI)
    result.authority_domain_id = DOMAIN
    result._origin = "https://kubernetes.example.com"
    result._client = httpx.AsyncClient(transport=httpx.MockTransport(handler or store.handle))
    return result


def pin(key):
    return hashlib.sha256(
        key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)
    ).hexdigest()


def pair(store, *, observed=None, timeout_s=2.0, register=None):
    """observed: callable returning the provisioner's fresh digests per call."""
    m = product()
    key = Ed25519PrivateKey.generate()
    channel = str(uuid4())
    calls = []

    async def reobserve():
        calls.append(1)
        return dict(DIGESTS) if observed is None else observed()

    requester = m.ObservationRequester(
        api=api(store),
        namespace=NAMESPACE,
        channel=channel,
        key=key.public_key(),
        pinned_spki_sha256=pin(key),
        timeout_s=timeout_s,
    )
    responder = m.ObservationResponder(
        api=api(store),
        namespace=NAMESPACE,
        channel=channel,
        key=key,
        facts=DIGESTS,
        reobserve=reobserve,
        register=register,
    )
    return requester, responder, calls, key


async def serving(responder, body):
    stop = asyncio.Event()
    task = asyncio.create_task(responder.serve(stop, deadline_s=30))
    try:
        return await body()
    finally:
        stop.set()
        await task


def signed(m, key, content):
    raw = m.canonical(content)
    return raw, base64.b64encode(key.sign(m.SIGNATURE_DOMAIN + raw)).decode()


def test_the_bound_fact_set_is_exactly_adr_0236s_list():
    assert product().OBSERVATION_FACTS == FACTS


def test_each_call_is_answered_only_after_fresh_reobservation_and_cleans_up():
    store = Store()
    requester, responder, calls, _ = pair(store)

    async def body():
        for _ in range(3):
            await requester.require(DIGESTS)

    asyncio.run(serving(responder, body))
    assert len(calls) == 3 and requester.calls == 3
    assert store.objects == {} and len(store.created) == 6  # three requests, three replies
    assert len({a["nonce"] for a in responder.answered}) == 3
    assert len({a["request_uid"] for a in responder.answered}) == 3


def test_request_object_is_immutable_and_carries_nonce_channel_and_exact_digests():
    m = product()
    store = Store()
    requester, responder, _, _ = pair(store)
    asyncio.run(serving(responder, lambda: requester.require(DIGESTS)))
    request = next(o for o in store.created if o["metadata"]["name"].startswith("observation-req"))
    assert request["immutable"] is True
    content = json.loads(request["data"]["request"])
    assert request["data"].keys() == {"request"}
    assert request["metadata"]["name"] == "observation-request-" + content["nonce"]
    assert content == DIGESTS | {"nonce": content["nonce"], "channel": requester.channel}
    assert request["metadata"]["labels"] == {
        m.CHANNEL_LABEL: requester.channel,
        m.KIND_LABEL: "request",
    }


@pytest.mark.parametrize(
    "facts",
    [
        {k: v for k, v in DIGESTS.items() if k != "cni"},
        DIGESTS | {"extra": "e" * 64},
        DIGESTS | {"nonce": str(uuid4())},
        DIGESTS | {"permit": "A" * 64},
        DIGESTS | {"permit": "a" * 63},
        DIGESTS | {"permit": 1},
    ],
    ids=["missing", "extra", "nonce", "uppercase", "short", "not-text"],
)
def test_requester_refuses_anything_but_the_exact_digest_set_before_any_object(facts):
    m = product()
    store = Store()
    requester, _, _, _ = pair(store)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(facts)

    asyncio.run(body())
    assert store.created == []


def test_binding_mismatch_is_never_signed_and_both_ends_fence():
    m = product()
    store = Store()
    requester, responder, calls, _ = pair(store, timeout_s=0.5)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS | {"permit": "d" * 64})
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)  # fenced after any refusal

    with pytest.raises(m.ObservationChannelRefused):
        asyncio.run(serving(responder, body))  # the provisioner fences too
    assert calls == [] and responder.answered == []
    assert store.objects == {}


def test_drift_seen_by_fresh_reobservation_publishes_no_reply():
    m = product()
    store = Store()
    requester, responder, calls, _ = pair(
        store, observed=lambda: DIGESTS | {"network": "f" * 64}, timeout_s=0.5
    )

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)

    with pytest.raises(m.ObservationChannelRefused):
        asyncio.run(serving(responder, body))
    assert calls == [1] and responder.answered == []
    assert not any(o["metadata"]["name"].startswith("observation-reply") for o in store.created)


def test_failed_reobservation_publishes_no_reply():
    m = product()
    store = Store()

    def broken():
        raise RuntimeError("installation drift observed")

    requester, responder, _, _ = pair(store, observed=broken, timeout_s=0.5)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)

    with pytest.raises(m.ObservationChannelRefused):
        asyncio.run(serving(responder, body))
    assert responder.answered == []
    assert not any(o["metadata"]["name"].startswith("observation-reply") for o in store.created)


def test_reply_ownership_is_registered_before_creation_and_a_failed_registration_refuses():
    m = product()
    store = Store()
    seen = []

    def register(path):
        seen.append((path, set(store.objects)))

    requester, responder, _, _ = pair(store, register=register)
    asyncio.run(serving(responder, lambda: requester.require(DIGESTS)))
    nonce = responder.answered[0]["nonce"]
    reply = "observation-reply-" + nonce
    assert seen == [(PREFIX + "/" + reply, {"observation-request-" + nonce})]

    def refuse(path):
        raise RuntimeError("ownership store unavailable")

    store = Store()
    requester, responder, _, _ = pair(store, register=refuse, timeout_s=0.5)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)

    with pytest.raises(m.ObservationChannelRefused):
        asyncio.run(serving(responder, body))
    assert not any(o["metadata"]["name"].startswith("observation-reply") for o in store.created)


def test_requester_refuses_an_unpinned_key_and_never_accepts_the_signing_key():
    m = product()
    key, other = Ed25519PrivateKey.generate(), Ed25519PrivateKey.generate()
    common = dict(api=api(Store()), namespace=NAMESPACE, channel=str(uuid4()))
    with pytest.raises(m.ObservationChannelRefused):
        m.ObservationRequester(key=key.public_key(), pinned_spki_sha256=pin(other), **common)
    with pytest.raises(m.ObservationChannelRefused):
        m.ObservationRequester(key=key, pinned_spki_sha256=pin(key), **common)


@pytest.mark.parametrize("namespace", ["", "Acme", "a/b", "../x", "a" * 64])
def test_unsafe_namespace_refuses_at_construction(namespace):
    m = product()
    key = Ed25519PrivateKey.generate()
    with pytest.raises(m.ObservationChannelRefused):
        m.ObservationRequester(
            api=api(Store()),
            namespace=namespace,
            channel=str(uuid4()),
            key=key.public_key(),
            pinned_spki_sha256=pin(key),
        )


@pytest.mark.parametrize(
    "attack",
    [
        "stale",
        "other-request",
        "other-channel",
        "tampered",
        "other-key",
        "no-domain",
        "not-canonical",
        "extra-field",
    ],
)
def test_replayed_tampered_or_foreign_replies_never_verify(attack):
    m = product()
    key = Ed25519PrivateKey.generate()
    channel, uid = str(uuid4()), str(uuid4())
    expected = DIGESTS | {"nonce": str(uuid4()), "channel": channel}
    raw, signature = signed(m, key, expected | {"request_uid": uid})
    m.verify_reply(raw, signature, expected=expected, request_uid=uid, key=key.public_key())
    kwargs = dict(expected=expected, request_uid=uid, key=key.public_key())
    if attack == "stale":
        kwargs["expected"] = expected | {"nonce": str(uuid4())}
    if attack == "other-request":
        kwargs["request_uid"] = str(uuid4())
    if attack == "other-channel":
        kwargs["expected"] = expected | {"channel": str(uuid4())}
    if attack == "tampered":
        raw = raw.replace(DIGESTS["permit"].encode(), b"0" * 64)
    if attack == "other-key":
        kwargs["key"] = Ed25519PrivateKey.generate().public_key()
    if attack == "no-domain":
        signature = base64.b64encode(key.sign(raw)).decode()
    if attack == "not-canonical":
        raw = json.dumps(expected | {"request_uid": uid}, indent=1).encode()
        signature = base64.b64encode(key.sign(m.SIGNATURE_DOMAIN + raw)).decode()
    if attack == "extra-field":
        raw, signature = signed(m, key, expected | {"request_uid": uid, "accepted": "yes"})
    with pytest.raises(m.ObservationChannelRefused):
        m.verify_reply(raw, signature, **kwargs)


def _request(m, channel, *, change=None):
    nonce = str(uuid4())
    value = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "immutable": True,
        "metadata": {
            "name": "observation-request-" + nonce,
            "uid": str(uuid4()),
            "namespace": NAMESPACE,
            "labels": {m.CHANNEL_LABEL: channel, m.KIND_LABEL: "request"},
        },
        "data": {"request": m.canonical(DIGESTS | {"nonce": nonce, "channel": channel}).decode()},
    }
    if change is not None:
        change(value)
    return value


@pytest.mark.parametrize(
    "change",
    [
        lambda v: v["metadata"]["labels"].update({product().CHANNEL_LABEL: str(uuid4())}),
        lambda v: v["metadata"].update(name="observation-request-" + str(uuid4())),
        lambda v: v.update(immutable=False),
        lambda v: v["metadata"].update(namespace="other-control"),
        lambda v: v["data"].update(extra="x"),
        lambda v: v["data"].update(request=json.dumps(json.loads(v["data"]["request"]), indent=1)),
        lambda v: v["data"].update(
            request=product()
            .canonical(json.loads(v["data"]["request"]) | {"channel": str(uuid4())})
            .decode()
        ),
    ],
    ids=[
        "other-channel-label",
        "name-not-nonce",
        "mutable",
        "other-namespace",
        "extra-data",
        "not-canonical",
        "other-channel-content",
    ],
)
def test_responder_rejects_malformed_or_foreign_requests_without_signing(change):
    m = product()
    store = Store()
    _, responder, calls, _ = pair(store)

    async def run():
        with pytest.raises(m.ObservationChannelRefused):
            await responder.answer(_request(m, responder.channel, change=change))
        with pytest.raises(m.ObservationChannelRefused):
            await responder.answer(_request(m, responder.channel))  # fenced

    asyncio.run(run())
    assert calls == [] and responder.answered == [] and store.created == []


def test_unanswered_request_times_out_removes_its_object_and_fences():
    m = product()
    store = Store()
    requester, responder, calls, _ = pair(store, timeout_s=0.2)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)

    asyncio.run(body())
    assert len(store.created) == 1 and store.objects == {}
    # A healthy provisioner afterwards does not revive the fenced requester.
    with pytest.raises(m.ObservationChannelRefused):
        asyncio.run(serving(responder, lambda: requester.require(DIGESTS)))
    assert len(store.created) == 1 and calls == []


@pytest.mark.parametrize(
    "change",
    [
        lambda o: o.update(immutable=False),
        lambda o: o["data"].update(
            request=product().canonical(DIGESTS | {"nonce": str(uuid4())}).decode()
        ),
    ],
    ids=["mutable", "other-content"],
)
def test_request_the_api_did_not_store_as_sent_refuses(change):
    m = product()
    store = Store()
    methods = []

    def mutate(request):
        methods.append(request.method)
        response = store.handle(request)
        if request.method == "POST":
            name = json.loads(request.content)["metadata"]["name"]
            change(store.objects[name])
            return httpx.Response(201, json=store.objects[name])
        return response

    requester, responder, calls, _ = pair(store, timeout_s=0.5)
    requester._api = api(store, mutate)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)

    asyncio.run(body())
    # Refused on the create response itself: no reply was ever awaited.
    assert methods == ["POST", "DELETE"]
    assert calls == [] and store.objects == {}


def test_squatted_reply_refuses_and_sweep_removes_leftovers():
    m = product()
    store = Store()
    requester, responder, _, key = pair(store, timeout_s=0.5)

    def squat(request):
        response = store.handle(request)
        name = json.loads(request.content)["metadata"]["name"] if request.method == "POST" else ""
        if name.startswith("observation-request-"):
            # A squatter pre-creates the reply name with a valid signature over
            # another request UID: never a positive observation.
            nonce = name[len("observation-request-") :]
            content = DIGESTS | {"nonce": nonce, "channel": requester.channel}
            raw, signature = signed(m, key, content | {"request_uid": str(uuid4())})
            store.objects["observation-reply-" + nonce] = {
                "metadata": {
                    "name": "observation-reply-" + nonce,
                    "uid": str(uuid4()),
                    "labels": {m.CHANNEL_LABEL: requester.channel, m.KIND_LABEL: "reply"},
                },
                "immutable": True,
                "data": {"observation": raw.decode(), "signature": signature},
            }
        return response

    requester._api = api(store, squat)

    async def body():
        with pytest.raises(m.ObservationChannelRefused):
            await requester.require(DIGESTS)

    asyncio.run(body())
    leftover = "observation-reply-" + str(uuid4())
    store.objects[leftover] = {
        "metadata": {
            "name": leftover,
            "uid": str(uuid4()),
            "labels": {m.CHANNEL_LABEL: responder.channel, m.KIND_LABEL: "reply"},
        }
    }
    assert asyncio.run(responder.sweep()) >= 1
    assert store.objects == {}
