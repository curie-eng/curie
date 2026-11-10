"""Socket-peer and Pod-UID launch identity of the provider gateway, @spec PROTECTED-COMMISSIONING-8.

Launch identity comes from the direct socket peer of a TLS connection and the
observed Pod UID bound to that peer when the connection is established, never
from HTTP fields or forwarded headers. A trusted in-process registrar (the
mediator that created the launch) registers each launch Pod after observing
it; there is no registration endpoint. A connection captures its owner once,
after the TLS handshake and before any HTTP byte, and keeps that owner for its
lifetime. Every request re-observes the same Pod: same UID, name and
namespace, running, not deleting, not on the host network, and still holding
the peer address. A withdrawn launch, a replaced IP owner or a vanished Pod
refuses on a retained connection instead of re-resolving it.

This identifies a peer; it authorizes nothing and is not evidence that the
cluster network prevents spoofing, host access or address reuse. Those
negatives are observed on the actual substrate; where they cannot be, the
provider gateway composition is unavailable.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import uuid
import weakref
from dataclasses import dataclass
from typing import Any, NoReturn, Protocol

from aiohttp import web
from aiohttp.web_protocol import RequestHandler

__all__ = [
    "AUTHORITIES",
    "LaunchPeer",
    "PeerBoundRunner",
    "PeerIdentityRefused",
    "PeerRegistry",
    "PodFacts",
    "PodReader",
    "peer_address",
]

AUTHORITIES = ("production", "commissioning")
_MAX_PEERS = 256
_MAX_CONNECTIONS = 1024
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


class PeerIdentityRefused(RuntimeError):
    """One fixed refusal; never carries a peer, Pod or reader detail."""

    def __init__(self) -> None:
        super().__init__("provider gateway peer identity refused")


def _refuse() -> NoReturn:
    error = PeerIdentityRefused()
    try:
        raise error from None
    finally:
        error.__cause__ = None
        error.__context__ = None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def _uuid(value: Any) -> None:
    _require(type(value) is str and len(value) == 36)
    try:
        _require(str(uuid.UUID(value)) == value)
    except ValueError:
        _refuse()


def _address(value: Any) -> str:
    _require(type(value) is str and "%" not in value)
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        _refuse()
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.compressed


@dataclass(frozen=True, slots=True, repr=False)
class LaunchPeer:
    """One launch Pod and the address it connects from; a value, never authority.

    ``authority`` is fixed by the registrar that created the launch and selects
    the closed authorizer; a request can never choose it.
    """

    authority: str
    launch_id: str
    namespace: str
    pod_name: str
    pod_uid: str
    peer_ip: str

    def __post_init__(self) -> None:
        _require(type(self.authority) is str and self.authority in AUTHORITIES)
        _uuid(self.launch_id)
        _uuid(self.pod_uid)
        for name in (self.namespace, self.pod_name):
            _require(type(name) is str and _DNS_LABEL.fullmatch(name) is not None)
        # The registrar records the compressed native form; an IPv4-mapped IPv6
        # spelling is normalized at capture, never registered.
        _require(type(self.peer_ip) is str and _address(self.peer_ip) == self.peer_ip)

    def __repr__(self) -> str:
        return "LaunchPeer()"


def peer_address(transport: Any) -> str:
    """The direct socket peer of an established TLS transport."""
    _require(transport is not None)
    _require(transport.get_extra_info("ssl_object") is not None)
    peer = transport.get_extra_info("peername")
    _require(type(peer) is tuple and len(peer) >= 2 and type(peer[0]) is str)
    return _address(peer[0])


class PodReader(Protocol):
    """A bounded Kubernetes read of one named object, absent as ``None``."""

    async def read(self, namespace: str, kind: str, name: str) -> dict[str, Any] | None: ...


class PeerFacts(Protocol):
    async def require(self, peer: LaunchPeer) -> None: ...


class PodFacts:
    """Current Kubernetes facts of the exact Pod behind a peer.

    The reader must be the gateway's own read-only Kubernetes identity; no
    caller-supplied object is trusted.
    """

    def __init__(self, reader: PodReader) -> None:
        _require(callable(getattr(reader, "read", None)))
        self._reader = reader

    def __repr__(self) -> str:
        return "PodFacts()"

    async def require(self, peer: LaunchPeer) -> None:
        try:
            _require(type(peer) is LaunchPeer)
            async with asyncio.timeout(5):
                pod = await self._reader.read(peer.namespace, "pods", peer.pod_name)
            _require(type(pod) is dict)
            assert pod is not None
            metadata, spec, status = pod["metadata"], pod["spec"], pod["status"]
            _require(metadata["uid"] == peer.pod_uid)
            _require(metadata["name"] == peer.pod_name)
            _require(metadata["namespace"] == peer.namespace)
            _require(metadata.get("deletionTimestamp") is None)
            _require(spec.get("hostNetwork", False) is False)
            _require(status["phase"] == "Running")
            _require(_address(status["podIP"]) == peer.peer_ip)
            ips = {_address(item["ip"]) for item in status.get("podIPs") or [status]}
            _require(peer.peer_ip in ips)
            _require(_address(status["hostIP"]) != peer.peer_ip)
        except Exception:  # noqa: BLE001 - no Pod or reader detail leaves this method.
            _refuse()


class PeerRegistry:
    """Registered launch peers and the owner each connection captured.

    One launch has exactly one peer for its lifetime; registering a new peer
    for an address replaces that address's previous owner, whose connections
    then refuse.
    """

    def __init__(self, facts: PeerFacts) -> None:
        _require(callable(getattr(facts, "require", None)))
        self._facts = facts
        self._by_launch: dict[str, LaunchPeer] = {}
        self._by_address: dict[str, LaunchPeer] = {}
        self._connections: weakref.WeakKeyDictionary[Any, LaunchPeer] = weakref.WeakKeyDictionary()

    def __repr__(self) -> str:
        return "PeerRegistry()"

    async def register(self, peer: LaunchPeer) -> None:
        """Trusted in-process registration after the registrar observed the Pod."""
        _require(type(peer) is LaunchPeer)
        known = self._by_launch.get(peer.launch_id)
        _require(known is None or known == peer)
        await self._facts.require(peer)
        _require(len(self._by_launch) < _MAX_PEERS or known is not None)
        previous = self._by_address.get(peer.peer_ip)
        if previous is not None and previous != peer:
            del self._by_launch[previous.launch_id]
        self._by_launch[peer.launch_id] = peer
        self._by_address[peer.peer_ip] = peer

    def withdraw(self, launch_id: str) -> None:
        """Forget one launch; its retained connections refuse from now on."""
        peer = self._by_launch.pop(launch_id, None)
        if peer is not None and self._by_address.get(peer.peer_ip) == peer:
            del self._by_address[peer.peer_ip]

    def capture(self, transport: Any) -> None:
        """Bind a new TLS connection to its address's current owner, or close it."""
        try:
            owner = self._by_address.get(peer_address(transport))
            _require(owner is not None and len(self._connections) < _MAX_CONNECTIONS)
            self._connections[transport] = owner  # type: ignore[assignment]
        except Exception:  # noqa: BLE001 - an unbound connection never reaches HTTP.
            transport.close()

    async def recheck(self, peer: LaunchPeer) -> None:
        """The peer is still registered as itself and its Pod facts still hold."""
        _require(type(peer) is LaunchPeer and self._by_launch.get(peer.launch_id) == peer)
        await self._facts.require(peer)

    async def owner(self, transport: Any) -> LaunchPeer:
        """The owner this connection captured, re-observed; never re-resolved."""
        try:
            original = self._connections.get(transport)
            _require(original is not None)
            assert original is not None
            _require(peer_address(transport) == original.peer_ip)
            await self.recheck(original)
            return original
        except Exception:  # noqa: BLE001 - one fixed refusal.
            _refuse()


class _PeerServer(web.Server):
    def __init__(self, original: web.Server, peers: PeerRegistry) -> None:
        super().__init__(
            original.request_handler,
            request_factory=original.request_factory,
            handler_cancellation=original.handler_cancellation,
            access_log=None,
            auto_decompress=False,
        )
        self._peers = peers

    def connection_made(self, handler: RequestHandler, transport: asyncio.Transport) -> None:
        # asyncio calls the protocol's connection_made after the TLS handshake
        # completes, so the capture precedes every HTTP byte.
        self._peers.capture(transport)
        super().connection_made(handler, transport)


class PeerBoundRunner(web.AppRunner):
    """The only runner whose connections can resolve an owner."""

    def __init__(self, application: web.Application, peers: PeerRegistry) -> None:
        _require(type(peers) is PeerRegistry)
        super().__init__(application, access_log=None)
        self._peers = peers

    async def _make_server(self) -> web.Server:
        return _PeerServer(await super()._make_server(), self._peers)
