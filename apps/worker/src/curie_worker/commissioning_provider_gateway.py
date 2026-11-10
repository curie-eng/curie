"""Provider gateway named by the execution configuration's model base URL.

@spec PROTECTED-COMMISSIONING-8 (and PROTECTED-COMMISSIONING-3 for the charge).
The gateway is a permanent installation component. It authorizes production
launches under production rules and commissioning launches only against a
running campaign journal, selected by the authenticated launch identity of
``provider_identity``, never by configuration or by a request field.

Each request passes, in order: the fixed route; the connection's captured
owner, re-observed; the closed Messages request profile measured from the
execution configuration (model, Messages backend, thinking disabled, output
token ceiling); the owner's closed authorizer; a second peer observation; and
only then the upstream forward of the original bytes. Remote attachments,
provider-managed tools and server-side compaction refuse before any charge.

A commissioning request is bound to its campaign, launch and request digest
and reserves and consumes one dispatch before forwarding. The reservation
charges the provider-enforced hard input ceiling at the worst input price
(uncached input, cache writes of either lifetime, cache reads) plus the
request's ``max_tokens`` at the output price, rounded up. No token count,
estimate or refund lowers it. A refused or ambiguous forward never returns its
consumed dispatch.

The upstream transport holds the fixed provider origin and credential; caller
headers are never forwarded. Nothing here qualifies an installation.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, NoReturn, Protocol, final

from aiohttp import web

from .protected_configuration import ProtectedExecutionConfig
from .protected_launch_registry import ProtectedLaunchRegistry
from .provider_identity import LaunchPeer, PeerBoundRunner, PeerRegistry

__all__ = [
    "MAX_BODY",
    "MESSAGES_PATH",
    "CampaignModelControl",
    "CommissioningModelAuthorizer",
    "GatewayProfile",
    "ProductionLaunches",
    "ProductionModelAuthorizer",
    "ProviderGateway",
    "ProviderGatewayRefused",
    "RegistryProductionLaunches",
    "UpstreamReply",
    "UpstreamTransport",
    "WorstCaseCharge",
    "WorstCasePrices",
    "validate_request",
]

MESSAGES_PATH = "/v1/messages"
MAX_BODY = 1_048_576
MAX_REPLY = 8_388_608
_MAX_INPUT_TOKENS = 10_000_000
_MAX_PRICE = 10**12
_DIGEST = re.compile(r"[0-9a-f]{64}")
_CONTENT_TYPES = ("application/json", "text/event-stream")
_TOP = frozenset(
    {
        "model",
        "messages",
        "system",
        "tools",
        "tool_choice",
        "thinking",
        "max_tokens",
        "stream",
        "metadata",
        "temperature",
        "stop_sequences",
    }
)
_REFUSAL = "provider gateway request refused"


class ProviderGatewayRefused(RuntimeError):
    """One fixed refusal; never carries a request, provider reply or credential."""

    def __init__(self) -> None:
        super().__init__(_REFUSAL)


def _refuse() -> NoReturn:
    error = ProviderGatewayRefused()
    try:
        raise error from None
    finally:
        # A transport exception can retain credential-bearing headers; detach
        # the whole graph, not only the displayed cause.
        error.__cause__ = None
        error.__context__ = None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def _count(value: Any, low: int, high: int) -> int:
    _require(type(value) is int and low <= value <= high)
    return value  # type: ignore[no-any-return]


@dataclass(frozen=True, slots=True)
class WorstCasePrices:
    """Measured provider prices in micro-USD per million tokens, for one model."""

    input_microusd_per_mtok: int
    cache_write_5m_microusd_per_mtok: int
    cache_write_1h_microusd_per_mtok: int
    cache_read_microusd_per_mtok: int
    output_microusd_per_mtok: int

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            _count(getattr(self, name), 1, _MAX_PRICE)

    @property
    def worst_input_microusd_per_mtok(self) -> int:
        """Any input token may be uncached, a cache write of either lifetime or a read."""
        return max(
            self.input_microusd_per_mtok,
            self.cache_write_5m_microusd_per_mtok,
            self.cache_write_1h_microusd_per_mtok,
            self.cache_read_microusd_per_mtok,
        )


@dataclass(frozen=True, slots=True)
class WorstCaseCharge:
    """One model call's worst case output tokens and spend; always one call."""

    output_tokens: int
    spend_microusd: int


@dataclass(frozen=True, slots=True, repr=False)
class GatewayProfile:
    """The measured model profile one installation's gateway enforces.

    Model and output ceiling come from the closed execution configuration,
    whose v1 grammar already fixes the Messages backend and disabled thinking.
    The hard input ceiling and prices are measured facts of that model,
    supplied by the installation; a caller-selected limit or a token count is
    never one.
    """

    execution_config_digest: str
    model: str
    max_output_tokens: int
    max_input_tokens: int
    prices: WorstCasePrices

    def __post_init__(self) -> None:
        _require(type(self.execution_config_digest) is str)
        _require(_DIGEST.fullmatch(self.execution_config_digest) is not None)
        _require(type(self.model) is str and 0 < len(self.model) <= 256)
        _count(self.max_output_tokens, 1, 1_000_000)
        _count(self.max_input_tokens, 1, _MAX_INPUT_TOKENS)
        _require(type(self.prices) is WorstCasePrices)

    def __repr__(self) -> str:
        return "GatewayProfile()"

    @classmethod
    def measured(
        cls,
        execution_config: ProtectedExecutionConfig,
        *,
        max_input_tokens: int,
        prices: WorstCasePrices,
    ) -> GatewayProfile:
        """Bind the gateway to one parsed execution configuration."""
        try:
            _require(type(execution_config) is ProtectedExecutionConfig)
            runner = execution_config.as_dict()["runner"]
            _require(runner["api_backend"] == "messages")
            _require(runner["thinking"] == "disabled")
            _require(runner["fake_model"] is False)
            return cls(
                execution_config.digest,
                runner["model"],
                runner["budget"]["max_output_tokens_per_run"],
                max_input_tokens,
                prices,
            )
        except Exception:  # noqa: BLE001 - one fixed refusal.
            _refuse()

    def charge(self, max_tokens: int) -> WorstCaseCharge:
        """The hard input ceiling plus ``max_tokens`` at worst case prices, rounded up."""
        _count(max_tokens, 1, self.max_output_tokens)
        numerator = (
            self.max_input_tokens * self.prices.worst_input_microusd_per_mtok
            + max_tokens * self.prices.output_microusd_per_mtok
        )
        return WorstCaseCharge(max_tokens, -(-numerator // 1_000_000))


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _constant(_value: str) -> NoReturn:
    _refuse()


def _text(value: Any, maximum: int = 256) -> None:
    _require(type(value) is str and 0 < len(value) <= maximum)


def _cache(block: dict[str, Any]) -> None:
    if "cache_control" in block:
        cache = block["cache_control"]
        _require(type(cache) is dict and cache.get("type") == "ephemeral")
        _require(cache.keys() <= {"type", "ttl"} and cache.get("ttl", "5m") in ("5m", "1h"))


def _text_block(block: Any) -> None:
    _require(type(block) is dict and block.get("type") == "text")
    _require(block.keys() <= {"type", "text", "cache_control"} and type(block.get("text")) is str)
    _cache(block)


def _text_content(value: Any) -> None:
    if type(value) is str:
        return
    _require(type(value) is list)
    for block in value:
        _text_block(block)


def _message(message: Any) -> None:
    _require(type(message) is dict and message.keys() == {"role", "content"})
    role, content = message["role"], message["content"]
    _require(role in ("user", "assistant"))
    if type(content) is str:
        return
    _require(type(content) is list and bool(content))
    for block in content:
        _require(type(block) is dict)
        kind = block.get("type")
        if kind == "text":
            _text_block(block)
        elif kind == "tool_use" and role == "assistant":
            _require(block.keys() <= {"type", "id", "name", "input", "cache_control"})
            _text(block.get("id"))
            _text(block.get("name"))
            _require(type(block.get("input")) is dict)
            _cache(block)
        elif kind == "tool_result" and role == "user":
            _require(
                block.keys() <= {"type", "tool_use_id", "content", "is_error", "cache_control"}
            )
            _text(block.get("tool_use_id"))
            _require("is_error" not in block or type(block["is_error"]) is bool)
            _text_content(block.get("content", ""))
            _cache(block)
        else:
            # Images, documents, files, URLs, search results, container uploads,
            # replayed thinking and every provider-executed tool block refuse:
            # each can fetch or execute outside the measured runner.
            _refuse()


def _tool(tool: Any) -> None:
    # Only custom tools run through the measured runner; provider-managed tool
    # types execute on the provider before any runner hook.
    _require(type(tool) is dict and tool.get("type", "custom") == "custom")
    _require(tool.keys() <= {"type", "name", "description", "input_schema", "cache_control"})
    _text(tool.get("name"))
    _require(type(tool.get("input_schema")) is dict)
    _require("description" not in tool or type(tool["description"]) is str)
    _cache(tool)


def validate_request(raw: bytes, profile: GatewayProfile) -> int:
    """The closed Messages request profile; returns the request's ``max_tokens``."""
    try:
        _require(type(profile) is GatewayProfile)
        _require(type(raw) is bytes and 0 < len(raw) <= MAX_BODY)
        value = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_pairs,
            parse_constant=_constant,
        )
        _require(type(value) is dict and value.keys() <= _TOP)
        _require({"model", "messages", "max_tokens", "thinking"} <= value.keys())
        _require(value["model"] == profile.model)
        _require(value["thinking"] == {"type": "disabled"})
        max_tokens = _count(value["max_tokens"], 1, profile.max_output_tokens)
        messages = value["messages"]
        _require(type(messages) is list and 0 < len(messages) <= 10_000)
        for message in messages:
            _message(message)
        if "system" in value:
            _text_content(value["system"])
        tools = value.get("tools", [])
        _require(type(tools) is list and len(tools) <= 128)
        for tool in tools:
            _tool(tool)
        if "tool_choice" in value:
            choice = value["tool_choice"]
            _require(type(choice) is dict and choice.get("type") in ("auto", "any", "none", "tool"))
            _require(choice.keys() <= {"type", "name", "disable_parallel_tool_use"})
            _require(("name" in choice) == (choice["type"] == "tool"))
            if "name" in choice:
                _text(choice["name"])
            _require(type(choice.get("disable_parallel_tool_use", False)) is bool)
        if "metadata" in value:
            metadata = value["metadata"]
            _require(type(metadata) is dict and metadata.keys() <= {"user_id"})
            if "user_id" in metadata:
                _text(metadata["user_id"])
        if "temperature" in value:
            temperature = value["temperature"]
            _require(type(temperature) in (int, float) and 0 <= temperature <= 1)
        if "stop_sequences" in value:
            stops = value["stop_sequences"]
            _require(type(stops) is list and len(stops) <= 16)
            for stop in stops:
                _text(stop)
        _require(type(value.get("stream", False)) is bool)
        return max_tokens
    except Exception:  # noqa: BLE001 - one fixed refusal.
        _refuse()


class ProductionLaunches(Protocol):
    """Production launch facts: a live production launch of this exact Pod."""

    async def require_launch(self, launch: LaunchPeer, *, execution_config_digest: str) -> None: ...


class CampaignModelControl(Protocol):
    """Campaign authority over the journal, implemented outside this package.

    ``require_launch`` refuses unless the journal is running and the launch's
    durable ownership is ``bound`` to exactly this Pod. ``consume`` binds the
    request to the campaign, launch and request digest, reserves the charge
    and consumes its dispatch; it is true only for the call that consumed it.
    """

    async def require_launch(
        self, launch_id: str, *, namespace: str, pod_name: str, pod_uid: str
    ) -> None: ...

    async def consume(
        self,
        operation_id: str,
        *,
        launch_id: str,
        pod_uid: str,
        request_sha256: str,
        output_tokens: int,
        spend_microusd: int,
    ) -> bool: ...


@final
class ProductionModelAuthorizer:
    """Production rules: a live production launch bound to this execution configuration."""

    __slots__ = ("_digest", "_launches")

    def __init__(self, launches: ProductionLaunches, profile: GatewayProfile) -> None:
        _require(callable(getattr(launches, "require_launch", None)))
        _require(type(profile) is GatewayProfile)
        self._launches = launches
        self._digest = profile.execution_config_digest

    def __repr__(self) -> str:
        return "ProductionModelAuthorizer()"

    async def authorize(
        self, peer: LaunchPeer, request_sha256: str, charge: WorstCaseCharge
    ) -> None:
        try:
            _require(type(peer) is LaunchPeer and peer.authority == "production")
            await self._launches.require_launch(peer, execution_config_digest=self._digest)
        except Exception:  # noqa: BLE001 - one fixed refusal.
            _refuse()


@final
class CommissioningModelAuthorizer:
    """Commissioning rules: a bound campaign launch and one consumed dispatch."""

    __slots__ = ("_control",)

    def __init__(self, control: CampaignModelControl) -> None:
        _require(callable(getattr(control, "require_launch", None)))
        _require(callable(getattr(control, "consume", None)))
        self._control = control

    def __repr__(self) -> str:
        return "CommissioningModelAuthorizer()"

    async def authorize(
        self, peer: LaunchPeer, request_sha256: str, charge: WorstCaseCharge
    ) -> None:
        try:
            _require(type(peer) is LaunchPeer and peer.authority == "commissioning")
            _require(type(charge) is WorstCaseCharge)
            _require(type(request_sha256) is str and _DIGEST.fullmatch(request_sha256) is not None)
            await self._control.require_launch(
                peer.launch_id,
                namespace=peer.namespace,
                pod_name=peer.pod_name,
                pod_uid=peer.pod_uid,
            )
            # A new operation per request: a retry is a new charge, never a replay.
            fresh = await self._control.consume(
                str(uuid.uuid4()),
                launch_id=peer.launch_id,
                pod_uid=peer.pod_uid,
                request_sha256=request_sha256,
                output_tokens=charge.output_tokens,
                spend_microusd=charge.spend_microusd,
            )
            _require(fresh is True)
        except Exception:  # noqa: BLE001 - one fixed refusal.
            _refuse()


@final
class RegistryProductionLaunches:
    """Production launch facts from the mediator's durable production registry.

    The launch must be ``running`` in this process's boot, own exactly the
    peer's Pod by name and observed UID, and be bound to the gateway's
    execution configuration digest.
    """

    __slots__ = ("_owner", "_registry")

    def __init__(self, registry: ProtectedLaunchRegistry, *, worker_principal_id: str) -> None:
        _require(type(registry) is ProtectedLaunchRegistry)
        _text(worker_principal_id)
        self._registry = registry
        self._owner = worker_principal_id

    def __repr__(self) -> str:
        return "RegistryProductionLaunches()"

    async def require_launch(self, launch: LaunchPeer, *, execution_config_digest: str) -> None:
        try:
            _require(type(launch) is LaunchPeer)
            row = await asyncio.to_thread(self._registry.get, self._owner, launch.launch_id)
            _require(row.state == "running")
            _require(row.objects.namespace == launch.namespace)
            _require(row.objects.pod_name == launch.pod_name)
            _require(row.objects.pod_uid == launch.pod_uid)
            _require(row.ownership.execution_config_digest == execution_config_digest)
        except Exception:  # noqa: BLE001 - one fixed refusal.
            _refuse()


@dataclass(frozen=True, slots=True, repr=False)
class UpstreamReply:
    status: int
    content_type: str
    body: bytes

    def __repr__(self) -> str:
        return "UpstreamReply()"


class UpstreamTransport(Protocol):
    """Fixed provider origin and credential; posts one Messages body.

    Callers choose neither host, path, headers nor credential.
    """

    async def post(self, raw: bytes) -> UpstreamReply: ...


class ProviderGateway:
    """The served gateway; no access log, no request or reply diagnostics."""

    def __init__(
        self,
        *,
        profile: GatewayProfile,
        peers: PeerRegistry,
        production: ProductionModelAuthorizer,
        commissioning: CommissioningModelAuthorizer | None,
        upstream: UpstreamTransport,
    ) -> None:
        _require(type(profile) is GatewayProfile and type(peers) is PeerRegistry)
        _require(type(production) is ProductionModelAuthorizer)
        _require(commissioning is None or type(commissioning) is CommissioningModelAuthorizer)
        _require(callable(getattr(upstream, "post", None)))
        self._profile = profile
        self._peers = peers
        self._authorizers: dict[str, ProductionModelAuthorizer | CommissioningModelAuthorizer] = {
            "production": production
        }
        if commissioning is not None:
            self._authorizers["commissioning"] = commissioning
        self._upstream = upstream
        self.application = web.Application(
            client_max_size=MAX_BODY,
            middlewares=[self._refusals],
            handler_args={"auto_decompress": False},
        )
        self.application.router.add_route("*", "/{tail:.*}", self._request)

    def __repr__(self) -> str:
        return "ProviderGateway()"

    def runner(self) -> PeerBoundRunner:
        """The owned connection factory; an ordinary runner resolves no owner."""
        return PeerBoundRunner(self.application, self._peers)

    @web.middleware
    async def _refusals(self, request: web.Request, handler: Any) -> web.StreamResponse:
        try:
            async with asyncio.timeout(65):
                response: web.StreamResponse = await handler(request)
                return response
        except Exception:  # noqa: BLE001 - a fixed response keeps no request graph.
            return web.Response(status=403, text=_REFUSAL)

    async def _request(self, request: web.Request) -> web.Response:
        _require(request.method == "POST" and request.path == MESSAGES_PATH)
        _require(request.rel_url.raw_query_string == "")
        _require(request.headers.get("Content-Encoding", "identity") == "identity")
        owner = await self._peers.owner(request.transport)
        _require(request.content_length is None or request.content_length <= MAX_BODY)
        collected = bytearray()
        async for chunk in request.content.iter_chunked(65536):
            _require(len(collected) + len(chunk) <= MAX_BODY)
            collected.extend(chunk)
        raw = bytes(collected)
        charge = self._profile.charge(validate_request(raw, self._profile))
        authorizer = self._authorizers.get(owner.authority)
        _require(authorizer is not None)
        assert authorizer is not None
        await authorizer.authorize(owner, hashlib.sha256(raw).hexdigest(), charge)
        # Re-observe the same Pod after the durable consumption; an ambiguous
        # forward has already spent its dispatch and is never retried here.
        await self._peers.recheck(owner)
        try:
            reply = await self._upstream.post(raw)
        except Exception:  # noqa: BLE001 - the upstream graph may hold credentials.
            _refuse()
        _require(type(reply) is UpstreamReply and reply.status == 200)
        _require(reply.content_type in _CONTENT_TYPES)
        _require(type(reply.body) is bytes and len(reply.body) <= MAX_REPLY)
        return web.Response(body=reply.body, status=200, content_type=reply.content_type)
