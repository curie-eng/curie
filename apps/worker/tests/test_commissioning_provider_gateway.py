"""Provider gateway: measured profile, closed requests and two closed authorizers.

@spec PROTECTED-COMMISSIONING-8. Actual TLS, actual aiohttp connection factory
and the actual closed execution configuration codec. The upstream provider,
the Kubernetes Pod reader, the production launch facts and the campaign
control are synthetic; the campaign control is proved against a real Valkey in
protected-hooks. No provider is called and no provider key exists here.
"""

from __future__ import annotations

import ast
import asyncio
import copy
import hashlib
import inspect
import json
import ssl
from contextlib import asynccontextmanager
from fractions import Fraction
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_worker import commissioning_provider_gateway as gateway
from curie_worker import provider_identity as identity
from curie_worker.protected_configuration import parse_execution_config

from .protected_configuration_vectors import canonical, execution_profile, manifest_for
from .protected_launch_endpoint_helpers import certificates
from .test_provider_identity import LAUNCH, NAMESPACE, POD, POD_UID, Pods, peer, pod

MODEL = "example-model"
PRICES = gateway.WorstCasePrices(
    input_microusd_per_mtok=3_000_000,
    cache_write_5m_microusd_per_mtok=3_750_000,
    cache_write_1h_microusd_per_mtok=6_000_000,
    cache_read_microusd_per_mtok=300_000,
    output_microusd_per_mtok=15_000_000,
)
MAX_INPUT = 200_000


def execution_config(**runner: Any) -> Any:
    value = execution_profile()
    value["runner"].update(runner)
    return parse_execution_config(canonical(value), manifest=manifest_for(value))


def profile() -> gateway.GatewayProfile:
    return gateway.GatewayProfile.measured(
        execution_config(), max_input_tokens=MAX_INPUT, prices=PRICES
    )


def request(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "model": MODEL,
        "max_tokens": 1024,
        "thinking": {"type": "disabled"},
        "messages": [{"role": "user", "content": "Summarize the incident."}],
    }
    value.update(changes)
    return value


def body(value: dict[str, Any] | None = None) -> bytes:
    return json.dumps(request() if value is None else value).encode()


def refused() -> Any:
    return pytest.raises(gateway.ProviderGatewayRefused)


def run(coroutine: Any) -> Any:
    return asyncio.run(coroutine)


# --- R3: the closed execution configuration names the gateway unchanged ---


@pytest.mark.parametrize(
    "url",
    [
        "https://provider-gateway.curie-protected.svc:8443",
        "https://provider-gateway.curie-protected.svc.cluster.local:8443",
        "https://10.96.0.20:8443",
    ],
)
def test_the_closed_execution_configuration_names_an_in_cluster_gateway(url: str) -> None:
    config = execution_config(model_base_url=url)
    assert config.as_dict()["runner"]["model_base_url"] == url


# --- measured profile ---


def test_the_profile_is_measured_from_the_execution_configuration() -> None:
    config = execution_config()
    measured = gateway.GatewayProfile.measured(config, max_input_tokens=MAX_INPUT, prices=PRICES)
    assert measured.model == MODEL
    assert measured.execution_config_digest == config.digest
    assert measured.max_output_tokens == 4096
    assert measured.max_input_tokens == MAX_INPUT
    assert "example-model" not in repr(measured)


def test_the_worst_case_charge_reserves_the_hard_input_ceiling_at_the_worst_input_price() -> None:
    measured = profile()
    charge = measured.charge(1024)
    # The 1h cache write is the worst input category: every input token may be one.
    expected = Fraction(MAX_INPUT * 6_000_000 + 1024 * 15_000_000, 1_000_000)
    assert charge == gateway.WorstCaseCharge(output_tokens=1024, spend_microusd=-(-expected // 1))
    # Rounding is always up, never down.
    tiny = gateway.GatewayProfile.measured(
        execution_config(),
        max_input_tokens=1,
        prices=gateway.WorstCasePrices(1, 1, 1, 1, 1),
    )
    assert tiny.charge(1).spend_microusd == 1
    with refused():
        measured.charge(4097)
    with refused():
        measured.charge(0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_input_tokens": 0},
        {"max_input_tokens": True},
        {"max_input_tokens": 10_000_001},
        {"prices": (1, 1, 1, 1, 1)},
    ],
)
def test_the_profile_refuses_unmeasured_bounds(kwargs: dict[str, Any]) -> None:
    arguments: dict[str, Any] = {"max_input_tokens": MAX_INPUT, "prices": PRICES} | kwargs
    with refused():
        gateway.GatewayProfile.measured(execution_config(), **arguments)


@pytest.mark.parametrize(
    "prices", [(0, 1, 1, 1, 1), (1, 1, 1, 1, 0), (1, 1, 1, 1, 1.5), (1, 1, 1, True, 1)]
)
def test_prices_are_positive_integers(prices: tuple[Any, ...]) -> None:
    with refused():
        gateway.WorstCasePrices(*prices)


def test_the_profile_refuses_anything_but_the_closed_parsed_configuration() -> None:
    config = execution_config()

    class Lookalike:
        digest = config.digest

        def as_dict(self) -> dict[str, Any]:
            return config.as_dict()

    with refused():
        gateway.GatewayProfile.measured(Lookalike(), max_input_tokens=MAX_INPUT, prices=PRICES)


# --- closed request profile ---


def test_a_text_and_custom_tool_request_is_admitted() -> None:
    value = request(
        system=[{"type": "text", "text": "Read only.", "cache_control": {"type": "ephemeral"}}],
        tools=[
            {
                "name": "read_log",
                "description": "Read one log window.",
                "input_schema": {"type": "object"},
                "cache_control": {"type": "ephemeral", "ttl": "1h"},
            }
        ],
        tool_choice={"type": "auto"},
        metadata={"user_id": "runner"},
        stream=True,
        temperature=0.5,
        stop_sequences=["END"],
        messages=[
            {"role": "user", "content": [{"type": "text", "text": "Look."}]},
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Reading."},
                    {"type": "tool_use", "id": "t1", "name": "read_log", "input": {"n": 1}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "t1",
                        "content": [{"type": "text", "text": "ok"}],
                        "is_error": False,
                    }
                ],
            },
        ],
    )
    assert gateway.validate_request(body(value), profile()) == 1024


def _image(source: dict[str, Any]) -> dict[str, Any]:
    return {"type": "image", "source": source}


REFUSED_REQUESTS: list[tuple[str, Any]] = [
    ("other model", lambda v: v.update(model="other-model")),
    ("thinking omitted", lambda v: v.pop("thinking")),
    ("thinking enabled", lambda v: v.update(thinking={"type": "enabled", "budget_tokens": 2048})),
    ("thinking adaptive", lambda v: v.update(thinking={"type": "adaptive"})),
    ("max_tokens omitted", lambda v: v.pop("max_tokens")),
    ("max_tokens above the measured ceiling", lambda v: v.update(max_tokens=4097)),
    ("max_tokens bool", lambda v: v.update(max_tokens=True)),
    ("max_tokens float", lambda v: v.update(max_tokens=1024.0)),
    ("server-side compaction", lambda v: v.update(context_management={"edits": []})),
    ("provider container", lambda v: v.update(container="c1")),
    ("remote MCP servers", lambda v: v.update(mcp_servers=[{"type": "url", "url": "x"}])),
    ("output config", lambda v: v.update(output_config={"effort": "high"})),
    ("service tier", lambda v: v.update(service_tier="auto")),
    ("empty messages", lambda v: v.update(messages=[])),
    ("system role message", lambda v: v.update(messages=[{"role": "system", "content": "x"}])),
    (
        "base64 image attachment",
        lambda v: v["messages"][0].update(
            content=[_image({"type": "base64", "media_type": "image/png", "data": "AA=="})]
        ),
    ),
    (
        "remote url attachment",
        lambda v: v["messages"][0].update(
            content=[_image({"type": "url", "url": "https://example.com/a.png"})]
        ),
    ),
    (
        "file attachment",
        lambda v: v["messages"][0].update(
            content=[{"type": "document", "source": {"type": "file", "file_id": "f1"}}]
        ),
    ),
    (
        "search result",
        lambda v: v["messages"][0].update(content=[{"type": "search_result", "source": "x"}]),
    ),
    (
        "container upload",
        lambda v: v["messages"][0].update(content=[{"type": "container_upload", "file_id": "f"}]),
    ),
    (
        "replayed thinking",
        lambda v: v.update(
            messages=[
                {"role": "user", "content": "x"},
                {
                    "role": "assistant",
                    "content": [{"type": "thinking", "thinking": "", "signature": "s"}],
                },
            ]
        ),
    ),
    (
        "server tool use replay",
        lambda v: v.update(
            messages=[
                {"role": "user", "content": "x"},
                {
                    "role": "assistant",
                    "content": [
                        {"type": "server_tool_use", "id": "s", "name": "web_search", "input": {}}
                    ],
                },
            ]
        ),
    ),
    (
        "tool use from the user",
        lambda v: v["messages"][0].update(
            content=[{"type": "tool_use", "id": "t", "name": "n", "input": {}}]
        ),
    ),
    (
        "tool result with an attachment",
        lambda v: v["messages"][0].update(
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": "t",
                    "content": [_image({"type": "url", "url": "https://example.com"})],
                }
            ]
        ),
    ),
    (
        "provider web search tool",
        lambda v: v.update(tools=[{"type": "web_search_20250305", "name": "web_search"}]),
    ),
    (
        "provider code execution tool",
        lambda v: v.update(tools=[{"type": "code_execution_20250825", "name": "code_execution"}]),
    ),
    ("provider bash tool", lambda v: v.update(tools=[{"type": "bash_20250124", "name": "bash"}])),
    (
        "provider tool shaped like a custom tool",
        lambda v: v.update(
            tools=[
                {
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "input_schema": {"type": "object"},
                }
            ]
        ),
    ),
    ("tool without schema", lambda v: v.update(tools=[{"name": "t"}])),
    (
        "two hour cache",
        lambda v: v["messages"][0].update(
            content=[
                {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": "2h"}}
            ]
        ),
    ),
    ("unknown tool choice", lambda v: v.update(tool_choice={"type": "web"})),
    ("metadata beyond user id", lambda v: v.update(metadata={"user_id": "r", "x": 1})),
    ("temperature above one", lambda v: v.update(temperature=2)),
    ("top_k sampling", lambda v: v.update(top_k=5)),
]


@pytest.mark.parametrize(
    "mutate", [case[1] for case in REFUSED_REQUESTS], ids=[case[0] for case in REFUSED_REQUESTS]
)
def test_requests_outside_the_closed_profile_refuse(mutate: Any) -> None:
    value = copy.deepcopy(request())
    mutate(value)
    with refused():
        gateway.validate_request(body(value), profile())


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"[]",
        b'{"model": "example-model", "model": "example-model"}',
        b'{"temperature": NaN}',
        b"\xff",
        b" " * (gateway.MAX_BODY + 1),
    ],
)
def test_malformed_bodies_refuse(raw: bytes) -> None:
    with refused():
        gateway.validate_request(raw, profile())


# --- the served gateway ---


class Upstream:
    """Synthetic fixed-origin provider: records exactly what would be forwarded."""

    def __init__(self, status: int = 200, content_type: str = "application/json") -> None:
        self.bodies: list[bytes] = []
        self.status = status
        self.content_type = content_type
        self.fail = False

    async def post(self, raw: bytes) -> gateway.UpstreamReply:
        self.bodies.append(raw)
        if self.fail:
            raise RuntimeError("upstream secret sk-example")
        return gateway.UpstreamReply(self.status, self.content_type, b'{"id":"msg"}')


class ProductionLaunches:
    def __init__(self) -> None:
        self.calls: list[tuple[identity.LaunchPeer, str]] = []
        self.live = True

    async def require_launch(
        self, launch: identity.LaunchPeer, *, execution_config_digest: str
    ) -> None:
        self.calls.append((launch, execution_config_digest))
        if not self.live:
            raise RuntimeError("not live")


class CampaignControl:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.fresh = True
        self.on_consume: Any = None

    async def require_launch(
        self, launch_id: str, *, namespace: str, pod_name: str, pod_uid: str
    ) -> None:
        self.calls.append(("require", (launch_id, namespace, pod_name, pod_uid)))

    async def consume(
        self,
        operation_id: str,
        *,
        launch_id: str,
        pod_uid: str,
        request_sha256: str,
        output_tokens: int,
        spend_microusd: int,
    ) -> bool:
        self.calls.append(
            ("consume", (launch_id, pod_uid, request_sha256, output_tokens, spend_microusd))
        )
        if self.on_consume is not None:
            self.on_consume()
        return self.fresh


class Served:
    def __init__(self, tmp: Path, authority: str = "commissioning") -> None:
        self.root = certificates(tmp / "pki")
        self.pods = Pods(pod())
        self.peers = identity.PeerRegistry(identity.PodFacts(self.pods))
        self.authority = authority
        self.upstream = Upstream()
        self.production = ProductionLaunches()
        self.control = CampaignControl()
        self.profile = profile()

    def build(self, *, commissioning: bool = True) -> gateway.ProviderGateway:
        return gateway.ProviderGateway(
            profile=self.profile,
            peers=self.peers,
            production=gateway.ProductionModelAuthorizer(self.production, self.profile),
            commissioning=(
                gateway.CommissioningModelAuthorizer(self.control) if commissioning else None
            ),
            upstream=self.upstream,
        )

    @asynccontextmanager
    async def serving(self, *, commissioning: bool = True) -> Any:
        await self.peers.register(peer(authority=self.authority))
        server = self.build(commissioning=commissioning)
        runner = server.runner()
        await runner.setup()
        site = gateway.web.TCPSite(runner, "127.0.0.1", 0, ssl_context=self.tls())
        await site.start()
        try:
            async with httpx.AsyncClient(
                base_url=f"https://127.0.0.1:{runner.addresses[0][1]}",
                verify=ssl.create_default_context(cafile=str(self.root / "ca.pem")),
                trust_env=False,
                timeout=5,
            ) as http:
                yield http
        finally:
            await runner.cleanup()

    def tls(self) -> Any:
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(self.root / "server.pem", self.root / "server.key")
        return context


def test_a_commissioning_launch_reserves_and_consumes_before_forwarding_original_bytes(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        served = Served(tmp_path)
        raw = body()
        async with served.serving() as http:
            response = await http.post(
                "/v1/messages",
                content=raw,
                headers={"x-api-key": "runner-key", "anthropic-beta": "compact-2026-01-12"},
            )
        assert response.status_code == 200 and response.content == b'{"id":"msg"}'
        assert response.headers["content-type"].startswith("application/json")
        assert served.upstream.bodies == [raw]
        charge = served.profile.charge(1024)
        assert served.control.calls == [
            ("require", (LAUNCH, NAMESPACE, POD, POD_UID)),
            (
                "consume",
                (LAUNCH, POD_UID, hashlib.sha256(raw).hexdigest(), 1024, charge.spend_microusd),
            ),
        ]
        assert served.production.calls == []

    run(scenario())


def test_a_replayed_or_stale_consumption_never_forwards(tmp_path: Path) -> None:
    async def scenario() -> None:
        served = Served(tmp_path)
        served.control.fresh = False
        async with served.serving() as http:
            response = await http.post("/v1/messages", content=body())
        assert response.status_code == 403 and served.upstream.bodies == []

    run(scenario())


def test_peer_facts_are_rechecked_after_consumption(tmp_path: Path) -> None:
    async def scenario() -> None:
        served = Served(tmp_path)
        served.control.on_consume = served.pods.objects.clear
        async with served.serving() as http:
            response = await http.post("/v1/messages", content=body())
        assert response.status_code == 403 and served.upstream.bodies == []
        assert [name for name, _ in served.control.calls] == ["require", "consume"]

    run(scenario())


def test_a_production_launch_is_authorized_under_production_rules(tmp_path: Path) -> None:
    async def scenario() -> None:
        served = Served(tmp_path, authority="production")
        raw = body()
        async with served.serving() as http:
            response = await http.post("/v1/messages", content=raw)
            served.production.live = False
            stale = await http.post("/v1/messages", content=raw)
        assert response.status_code == 200 and stale.status_code == 403
        assert served.upstream.bodies == [raw]
        assert (
            served.production.calls
            == [(peer(authority="production"), served.profile.execution_config_digest)] * 2
        )
        assert served.control.calls == []

    run(scenario())


def test_without_a_running_campaign_a_commissioning_launch_refuses(tmp_path: Path) -> None:
    async def scenario() -> None:
        served = Served(tmp_path)
        async with served.serving(commissioning=False) as http:
            response = await http.post("/v1/messages", content=body())
        assert response.status_code == 403
        assert served.upstream.bodies == [] and served.production.calls == []

    run(scenario())


@pytest.mark.parametrize(
    ("method", "path", "headers", "raw"),
    [
        ("GET", "/v1/messages", {}, None),
        ("POST", "/v1/messages/count_tokens", {}, "body"),
        ("POST", "/v1/messages?beta=true", {}, "body"),
        ("POST", "/v1/complete", {}, "body"),
        ("POST", "/v1/messages", {"Content-Encoding": "gzip"}, "body"),
        ("POST", "/v1/messages", {}, "attachment"),
        ("POST", "/v1/messages", {}, "oversize"),
    ],
)
def test_refusals_precede_any_charge_or_forward(
    tmp_path: Path, method: str, path: str, headers: dict[str, str], raw: str | None
) -> None:
    async def scenario() -> None:
        served = Served(tmp_path)
        content: bytes | None = None
        if raw == "body":
            content = body()
        elif raw == "attachment":
            value = request()
            value["messages"][0]["content"] = [
                _image({"type": "url", "url": "https://example.com/a.png"})
            ]
            content = body(value)
        elif raw == "oversize":
            content = b" " * (gateway.MAX_BODY + 1)
        async with served.serving() as http:
            response = await http.request(method, path, content=content, headers=headers)
        assert response.status_code in (403, 413)
        assert "example.com" not in response.text
        assert served.upstream.bodies == []
        assert [name for name, _ in served.control.calls if name == "consume"] == []

    run(scenario())


def test_an_upstream_failure_refuses_without_echoing_it(tmp_path: Path) -> None:
    async def scenario() -> None:
        served = Served(tmp_path)
        served.upstream.fail = True
        async with served.serving() as http:
            failed = await http.post("/v1/messages", content=body())
        served.upstream = Upstream(status=529)
        async with served.serving() as http:
            overloaded = await http.post("/v1/messages", content=body())
        assert failed.status_code == 403 and "sk-example" not in failed.text
        assert overloaded.status_code == 403

    run(scenario())


def test_authorizers_are_closed_and_selected_only_by_launch_identity(tmp_path: Path) -> None:
    served = Served(tmp_path)
    production = gateway.ProductionModelAuthorizer(served.production, served.profile)
    commissioning = gateway.CommissioningModelAuthorizer(served.control)
    for kwargs in (
        {"production": commissioning, "commissioning": None},
        {"production": production, "commissioning": production},
        {"production": production, "commissioning": None, "profile": object()},
        {"production": production, "commissioning": None, "peers": object()},
    ):
        arguments = {
            "profile": served.profile,
            "peers": served.peers,
            "upstream": served.upstream,
        } | kwargs
        with refused():
            gateway.ProviderGateway(**arguments)
    with refused():
        gateway.ProductionModelAuthorizer(served.production, object())  # type: ignore[arg-type]
    charge = served.profile.charge(1)
    with refused():
        run(production.authorize(peer(authority="commissioning"), "a" * 64, charge))
    with refused():
        run(commissioning.authorize(peer(authority="production"), "a" * 64, charge))


def test_the_gateway_imports_no_commissioning_record_or_launch_module() -> None:
    for module in (gateway, identity):
        tree = ast.parse(inspect.getsource(module))
        imported = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        assert not any("curie_protected_hooks" in name for name in imported), imported
        assert not any("commissioning_launch" in name for name in imported), imported
        assert not any("commissioning_registry" in name for name in imported), imported


# --- production rules over the actual production launch registry ---


def _running_registry(tmp_path: Path, execution_config_digest: str) -> tuple[Any, Any]:
    from . import test_protected_launch_registry as registry_vectors

    location = tmp_path.resolve() / "registry"
    location.mkdir(mode=0o700)
    registry = registry_vectors.open_registry(location)
    row = registry_vectors.create(
        registry,
        ownership=registry_vectors.ownership(execution_config_digest=execution_config_digest),
    )
    row = registry_vectors.objects(registry, row)
    row = registry.transition(registry_vectors.OWNER, row.launch_id, row.revision, "idle")
    started = registry.begin_operation(
        registry_vectors.OWNER,
        row.launch_id,
        row.revision,
        registry_vectors.OPERATION,
        "start",
        registry_vectors.DIGEST,
    )
    row = registry.finish_operation(
        registry_vectors.OWNER,
        row.launch_id,
        started.revision,
        registry_vectors.OPERATION,
        "applied",
        epoch=registry_vectors.EPOCH,
        state="running",
    )
    return registry, row


def test_registry_production_launches_require_the_running_launch_of_this_exact_pod(
    tmp_path: Path,
) -> None:
    from . import test_protected_launch_registry as registry_vectors

    measured = profile()
    registry, row = _running_registry(tmp_path, measured.execution_config_digest)
    with registry:
        launches = gateway.RegistryProductionLaunches(
            registry, worker_principal_id=registry_vectors.OWNER
        )
        exact = peer(
            authority="production",
            launch_id=row.launch_id,
            namespace=row.objects.namespace,
            pod_name=row.objects.pod_name,
            pod_uid=row.objects.pod_uid,
        )
        digest = measured.execution_config_digest
        run(launches.require_launch(exact, execution_config_digest=digest))
        for other in (
            peer(authority="production", launch_id=row.launch_id),
            peer(
                authority="production",
                launch_id="b0000000-0000-4000-8000-0000000000ff",
                namespace=row.objects.namespace,
                pod_name=row.objects.pod_name,
                pod_uid=row.objects.pod_uid,
            ),
        ):
            with refused():
                run(launches.require_launch(other, execution_config_digest=digest))
        with refused():
            run(launches.require_launch(exact, execution_config_digest="f" * 64))
        registry.transition(registry_vectors.OWNER, row.launch_id, row.revision, "retiring")
        with refused():
            run(launches.require_launch(exact, execution_config_digest=digest))
    with pytest.raises(gateway.ProviderGatewayRefused):
        gateway.RegistryProductionLaunches(object(), worker_principal_id="w")  # type: ignore[arg-type]
