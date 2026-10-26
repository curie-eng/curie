"""Socket-peer and Pod-UID launch identity of the provider gateway.

@spec PROTECTED-COMMISSIONING-8. Actual TLS over loopback and the actual
aiohttp connection factory; only the Kubernetes Pod reader is synthetic. The
cross Pod, ordinary worker, host path, retained connection and IP reuse
negatives are additionally observed on a disposable cluster (see the PR
evidence); a unit result is not that observation.
"""

from __future__ import annotations

import asyncio
import copy
import ssl
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp import web
from curie_worker import provider_identity as identity

from .protected_launch_endpoint_helpers import certificates

LAUNCH = "b0000000-0000-4000-8000-000000000001"
OTHER_LAUNCH = "b0000000-0000-4000-8000-000000000002"
POD_UID = "c0000000-0000-4000-8000-000000000001"
OTHER_UID = "c0000000-0000-4000-8000-000000000002"
NAMESPACE = "curie-launches"
POD = "pl-p-b0000000000040008000000000000001"
ABSENT = object()


def peer(**changes: Any) -> identity.LaunchPeer:
    value = {
        "authority": "commissioning",
        "launch_id": LAUNCH,
        "namespace": NAMESPACE,
        "pod_name": POD,
        "pod_uid": POD_UID,
        "peer_ip": "127.0.0.1",
    }
    value.update(changes)
    return identity.LaunchPeer(**value)


def pod(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "metadata": {"name": POD, "namespace": NAMESPACE, "uid": POD_UID},
        "spec": {"hostNetwork": False},
        "status": {
            "phase": "Running",
            "podIP": "127.0.0.1",
            "podIPs": [{"ip": "127.0.0.1"}],
            "hostIP": "192.0.2.10",
        },
    }
    for path, replacement in changes.items():
        target = value
        *parents, leaf = path.split("__")
        for name in parents:
            target = target[name]
        if replacement is ABSENT:
            target.pop(leaf, None)
        else:
            target[leaf] = replacement
    return value


class Pods:
    """Synthetic Kubernetes reader: the current object per (namespace, name)."""

    def __init__(self, *objects: dict[str, Any]) -> None:
        self.objects = {
            (item["metadata"]["namespace"], item["metadata"]["name"]): item for item in objects
        }
        self.reads = 0

    async def read(self, namespace: str, kind: str, name: str) -> dict[str, Any] | None:
        assert kind == "pods"
        self.reads += 1
        found = self.objects.get((namespace, name))
        return None if found is None else copy.deepcopy(found)


def refused() -> Any:
    return pytest.raises(identity.PeerIdentityRefused)


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


@pytest.mark.parametrize(
    "changes",
    [
        {"authority": "operator"},
        {"launch_id": "not-a-uuid"},
        {"launch_id": LAUNCH.upper()},
        {"namespace": "Upper"},
        {"pod_name": "x" * 64},
        {"pod_uid": "c0000000-0000-4000-8000-00000000000"},
        {"peer_ip": "127.000.000.001"},
        {"peer_ip": "fe80::1%lo0"},
        {"peer_ip": "::ffff:127.0.0.1"},
        {"peer_ip": 1},
    ],
)
def test_a_launch_peer_is_closed(changes: dict[str, Any]) -> None:
    with refused():
        peer(**changes)


def test_a_launch_peer_never_echoes_its_fields() -> None:
    assert LAUNCH not in repr(peer()) and POD_UID not in repr(peer())


@pytest.mark.parametrize(
    "changes",
    [
        {"metadata__uid": OTHER_UID},
        {"metadata__name": "other"},
        {"metadata__namespace": "other"},
        {"metadata__deletionTimestamp": "2026-10-21T00:00:00Z"},
        {"spec__hostNetwork": True},
        {"status__phase": "Pending"},
        {"status__phase": "Succeeded"},
        {"status__podIP": "127.0.0.2"},
        {"status__podIP": ABSENT},
        {"status__podIPs": [{"ip": "127.0.0.2"}]},
        {"status__hostIP": "127.0.0.1"},
    ],
)
def test_pod_facts_refuse_any_other_pod_state(changes: dict[str, Any]) -> None:
    with refused():
        run(identity.PodFacts(Pods(pod(**changes))).require(peer()))


def test_pod_facts_refuse_an_absent_pod_or_a_failing_reader() -> None:
    class Broken:
        async def read(self, *_args: Any) -> dict[str, Any] | None:
            raise RuntimeError("secret detail")

    with refused():
        run(identity.PodFacts(Pods()).require(peer()))
    with refused() as caught:
        run(identity.PodFacts(Broken()).require(peer()))
    assert "secret detail" not in str(caught.value)
    assert caught.value.__cause__ is None and caught.value.__context__ is None


def test_pod_facts_accept_the_exact_running_pod() -> None:
    run(identity.PodFacts(Pods(pod())).require(peer()))


def test_registration_requires_current_facts_and_a_launch_never_changes_identity() -> None:
    async def scenario() -> None:
        pods = Pods(pod())
        registry = identity.PeerRegistry(identity.PodFacts(pods))
        with refused():
            await registry.register(peer(pod_uid=OTHER_UID))
        await registry.register(peer())
        await registry.register(peer())  # an identical repeat is inert
        with refused():
            await registry.register(peer(authority="production"))
        with refused():
            await registry.register(peer(peer_ip="127.0.0.2"))

    run(scenario())


def _server_context(root: Path) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(root / "server.pem", root / "server.key")
    return context


transports: list[int] = []


@asynccontextmanager
async def serving(root: Path, registry: identity.PeerRegistry, *, tls: bool = True) -> Any:
    seen: list[identity.LaunchPeer] = []

    async def handler(request: web.Request) -> web.Response:
        # A refusal keeps the connection alive, so a retained connection is
        # really reused by the next request.
        transports.append(id(request.transport))
        try:
            owner = await registry.owner(request.transport)
        except identity.PeerIdentityRefused:
            return web.Response(status=403)
        seen.append(owner)
        return web.Response(text=owner.launch_id)

    application = web.Application()
    application.router.add_route("POST", "/", handler)
    runner = identity.PeerBoundRunner(application, registry)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=_server_context(root) if tls else None)
    await site.start()
    port = runner.addresses[0][1]
    try:
        yield f"{'https' if tls else 'http'}://127.0.0.1:{port}/", seen
    finally:
        await runner.cleanup()


def client(root: Path) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        verify=ssl.create_default_context(cafile=str(root / "ca.pem")), trust_env=False, timeout=5
    )


def test_identity_comes_from_the_tls_peer_never_from_http_fields(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        registry = identity.PeerRegistry(identity.PodFacts(Pods(pod())))
        await registry.register(peer())
        async with serving(root, registry) as (url, seen), client(root) as http:
            response = await http.post(
                url,
                headers={
                    "X-Forwarded-For": "192.0.2.99",
                    "X-Curie-Launch": OTHER_LAUNCH,
                    "Forwarded": "for=192.0.2.99",
                },
            )
            assert response.status_code == 200 and response.text == LAUNCH
            assert seen == [peer()]

    run(scenario())


def test_an_unregistered_peer_is_disconnected_before_any_request(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        registry = identity.PeerRegistry(identity.PodFacts(Pods(pod())))
        async with serving(root, registry) as (url, seen), client(root) as http:
            with pytest.raises(httpx.HTTPError):
                await http.post(url, headers={"X-Forwarded-For": "127.0.0.1"})
            assert seen == []

    run(scenario())


def test_a_plaintext_connection_never_gains_an_identity(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        registry = identity.PeerRegistry(identity.PodFacts(Pods(pod())))
        await registry.register(peer())
        async with serving(root, registry, tls=False) as (url, seen):
            async with httpx.AsyncClient(trust_env=False, timeout=5) as http:
                with pytest.raises(httpx.HTTPError):
                    await http.post(url)
            assert seen == []

    run(scenario())


def test_a_retained_connection_keeps_its_first_identity_and_refuses_after_withdrawal(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        pods = Pods(pod())
        registry = identity.PeerRegistry(identity.PodFacts(pods))
        await registry.register(peer())
        async with serving(root, registry) as (url, _seen), client(root) as http:
            assert (await http.post(url)).text == LAUNCH
            registry.withdraw(LAUNCH)
            response = await http.post(url)  # same pooled TLS connection
            assert response.status_code == 403
            assert transports[-1] == transports[-2]

    run(scenario())


def test_ip_reuse_never_moves_a_connection_or_a_new_peer_to_the_old_launch(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        pods = Pods(pod())
        registry = identity.PeerRegistry(identity.PodFacts(pods))
        await registry.register(peer())
        async with serving(root, registry) as (url, _seen):
            async with client(root) as retained:
                assert (await retained.post(url)).text == LAUNCH
                connection = transports[-1]
                # The first Pod is gone and an unrelated Pod now holds its IP.
                pods.objects.clear()
                replacement = pod(metadata__name="ordinary", metadata__uid=OTHER_UID)
                pods.objects[(NAMESPACE, "ordinary")] = replacement
                assert (await retained.post(url)).status_code == 403
                assert transports[-1] == connection
                async with client(root) as fresh:
                    assert (await fresh.post(url)).status_code == 403
                # A trusted registration of the new owner replaces the IP mapping;
                # the retained connection still carries the old UID and refuses.
                await registry.register(
                    peer(launch_id=OTHER_LAUNCH, pod_name="ordinary", pod_uid=OTHER_UID)
                )
                assert (await retained.post(url)).status_code == 403
                assert transports[-1] == connection
                async with client(root) as fresh:
                    assert (await fresh.post(url)).text == OTHER_LAUNCH

    run(scenario())


def test_a_replaced_address_owner_refuses_even_while_its_old_pod_still_reports_it(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        replacement = pod(metadata__name="ordinary", metadata__uid=OTHER_UID)
        pods = Pods(pod(), replacement)  # a stale observation: both report the address
        registry = identity.PeerRegistry(identity.PodFacts(pods))
        await registry.register(peer())
        async with serving(root, registry) as (url, _seen), client(root) as retained:
            assert (await retained.post(url)).text == LAUNCH
            connection = transports[-1]
            await registry.register(
                peer(launch_id=OTHER_LAUNCH, pod_name="ordinary", pod_uid=OTHER_UID)
            )
            assert (await retained.post(url)).status_code == 403
            assert transports[-1] == connection
            with refused():
                await registry.recheck(peer())

    run(scenario())


def test_an_ordinary_app_runner_never_resolves_an_owner(tmp_path: Path) -> None:
    async def scenario() -> None:
        root = certificates(tmp_path / "pki")
        registry = identity.PeerRegistry(identity.PodFacts(Pods(pod())))
        await registry.register(peer())

        async def handler(request: web.Request) -> web.Response:
            await registry.owner(request.transport)
            return web.Response(text="owner")

        application = web.Application()
        application.router.add_route("POST", "/", handler)
        runner = web.AppRunner(application)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=_server_context(root))
        await site.start()
        try:
            async with client(root) as http:
                response = await http.post(f"https://127.0.0.1:{runner.addresses[0][1]}/")
                assert response.status_code == 500
        finally:
            await runner.cleanup()

    run(scenario())
