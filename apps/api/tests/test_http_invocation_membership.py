"""Real ASGI ingress with owned, synthetic TLS directory facts.

@spec HTTP-INVOCATION-EXECUTION-2 HTTP-INVOCATION-EXECUTION-4/5/6/7/12/15.
Synthetic source freshness and kind declarations do not qualify a real directory.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import importlib.util
import json
import ssl
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import agent_role_policy, channel_token, principal_authority
from curie_api.config import get_settings
from curie_api.identity.http_credential_registry import build_provider_credential_registry
from curie_api.identity.http_transport import (
    HttpIdentityClient,
    HttpIdentityObservation,
    ProviderBearer,
)
from curie_api.main import create_app
from curie_api.models import (
    AgentChannel,
    ChannelIdentity,
    IdentityNamespace,
    Principal,
    ProviderInstallation,
)
from curie_api.schemas.http_identity import LookupRequestV1, NamespaceV1
from curie_internal import identity_context as codec
from curie_internal.keyspace import CHANNEL_KEY_PREFIX
from fastapi.testclient import TestClient
from principal_adoption_support import ISSUER, VECTORS, selection
from role_policy_support import change, run, seed
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

ACCOUNT = "T0EXAMPLE1"
SENDER = "U0EXAMPLE1"
BEARER = "SyntheticIngressProviderBearer"
SUBJECT = "SyntheticIngressSubject"
GROUP = "SyntheticRequiredGroup"
# @spec HTTP-INVOCATION-EXECUTION-21: synthetic Grid organisation and workspace.
GRID_ORG = "E0EXAMPLEORG"
GRID_WORKSPACE = "T0EXAMPLEHOME"


def _transport_harness() -> ModuleType:
    """@spec HTTP-INVOCATION-EXECUTION-12: reuse the independent public TLS owner."""
    path = Path(__file__).resolve().parents[3] / "tests/http_identity/conftest.py"
    spec = importlib.util.spec_from_file_location("http_membership_tls_support", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def synthetic_provider(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """@spec HTTP-INVOCATION-EXECUTION-12/15: own TLS, thread, tasks and certificates."""
    harness = _transport_harness()
    stop, ready = threading.Event(), threading.Event()
    provider = SimpleNamespace(member=True, requests=[], responses=[], errors=[])

    async def serve() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-12: all external observations are synthetic."""
        rig = harness.Rig(tmp_path)
        try:
            server_tls, provider.tls = rig.certificates()
            assert provider.tls.verify_mode == ssl.CERT_REQUIRED and provider.tls.check_hostname

            async def echo(_reader: Any, writer: Any) -> None:
                """@spec HTTP-INVOCATION-EXECUTION-4/15: echo actual request scope/version."""
                first, headers, raw = listener.requests[-1]
                request = json.loads(raw)
                assert first == b"POST /lookup HTTP/1.1"
                assert headers["authorization"] == "Bearer " + BEARER
                assert "cookie" not in headers
                # This owned source explicitly declares human kind and complete membership.
                response = copy.deepcopy(VECTORS["cases"][0]["response"])
                response.update(copy.deepcopy(request))
                stamp = (datetime.now(UTC) - timedelta(milliseconds=20)).isoformat()
                observation = {"as_of": stamp, "max_source_lag_ms": 5}
                response.update(
                    outcome="resolved",
                    sender_kind="human",
                    identity={
                        "issuer": request["scope"]["expected_issuer"],
                        "subject": SUBJECT,
                        "kind": "human",
                        "account_status": "active",
                    },
                    identity_observation=observation,
                    membership={
                        "complete": True,
                        "groups": [
                            {
                                "issuer": request["scope"]["expected_issuer"],
                                "id": GROUP if provider.member else "SyntheticOtherGroup",
                            },
                        ],
                        "observation": observation,
                    },
                )
                provider.requests.append(request)
                provider.responses.append(copy.deepcopy(response))
                writer.write(harness.wire(json.dumps(response).encode()))
                await writer.drain()

            listener = await rig.listen(handler=echo, tls=server_tls)
            provider.url = listener.url
            ready.set()
            while not stop.is_set():
                await asyncio.sleep(0.01)
        finally:
            await rig.cleanup()

    def start() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-12: propagate thread failures into fixture errors."""
        try:
            asyncio.run(serve())
        except BaseException as exc:  # noqa: BLE001 - relay owned thread failures to pytest.
            provider.errors.append(exc)
            ready.set()

    thread = threading.Thread(target=start, name="synthetic-ingress-provider")
    try:
        thread.start()
        assert ready.wait(5), "owned TLS provider did not start"
        assert not provider.errors
        yield provider
    finally:
        stop.set()
        thread.join(timeout=5)
        assert not thread.is_alive(), "owned TLS provider thread survived cleanup"
        assert not provider.errors, "owned TLS provider failed"


def _runtime_api() -> ModuleType:
    """@spec HTTP-INVOCATION-EXECUTION-15: missing features fail in the test body."""
    name = "curie_api.identity.http_invocation"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "HTTP invocation runtime is not implemented"
        raise
    for symbol in (
        "ReceiverRegistrationV1",
        "HttpInvocationRuntime",
        "build_http_invocation_runtime",
    ):
        assert hasattr(module, symbol), f"HTTP invocation runtime missing {symbol}"
    return module


def _scope(endpoint: str, *, grid: bool = False) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-4/5/15/21: real adopted scope, no legacy projections."""
    account = GRID_WORKSPACE if grid else ACCOUNT
    kind, key = ("slack_enterprise", GRID_ORG) if grid else ("slack_workspace", ACCOUNT)
    scope: dict[str, Any] = {
        "account": account,
        "address": "C" + uuid.uuid4().hex[:12].upper(),
        "receiver_id": str(uuid.uuid4()),
        "receiver_key": b"r" * 32,
        "context_key": b"c" * 32,
    }

    async def persist(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-4/5/15."""
        tenant, agent = await seed(factory)
        async with factory() as session, session.begin():
            adopted = await principal_authority.adopt_http_authority(
                session,
                tenant_id=tenant,
                expected_version=0,
                expected_lifecycle_generation=0,
                selection=selection(lookup_url=endpoint),
            )
            installation = ProviderInstallation(
                tenant_id=tenant,
                provider="slack",
                authority="",
                external_account_id=account,
                status="connected",
            )
            namespace = IdentityNamespace(
                tenant_id=tenant,
                provider="slack",
                authority="",
                kind=kind,
                key=key,
                status="active",
            )
            session.add_all([installation, namespace])
            await session.flush()
            identity = ChannelIdentity(
                tenant_id=tenant,
                provider="slack",
                name="synthetic-http-" + uuid.uuid4().hex,
                provider_installation_id=installation.id,
                status="active",
                attributes={},
            )
            session.add(identity)
            await session.flush()
            binding = AgentChannel(
                tenant_id=tenant,
                agent_id=agent,
                kind="slack",
                address=scope["address"],
                adapter=identity.name,
                allowed_callers=None,
            )
            session.add(binding)
            await session.flush()
            scope.update(
                tenant=str(tenant),
                agent=str(agent),
                binding=str(binding.id),
                identity=str(identity.id),
                installation=str(installation.id),
                adapter=identity.name,
                namespace=NamespaceV1(id=str(namespace.id), authority="", kind=kind, key=key),
                configuration=adopted.configuration,
            )
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(
                session,
                change=change(
                    tenant,
                    agent,
                    operation="migrate_role",
                    filter={
                        "entries": [
                            {"kind": "issuer_group", "group": {"issuer": ISSUER, "id": GROUP}}
                        ],
                    },
                ),
            )
            scope["role_version"] = result.version

    run(persist)
    return scope


def _runtime(module: ModuleType, scope: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-1/15: explicit operator registrations only."""
    providers = build_provider_credential_registry(
        ((scope["configuration"], ProviderBearer(BEARER)),)
    )
    registration = module.ReceiverRegistrationV1(
        schema_version=1,
        receiver_id=scope["receiver_id"],
        generation=1,
        tenant_id=scope["tenant"],
        channel_identity_id=scope["identity"],
        channel_identity_name=scope["adapter"],
        provider_installation_id=scope["installation"],
        namespace=scope["namespace"],
        installation_account=scope["account"],
        bot_user_id="U0EXAMPLEBOT",
        binding_ids=(scope["binding"],),
        human_native_ids=(SENDER,),
    )
    return module.build_http_invocation_runtime(
        providers,
        ((registration, scope["receiver_key"]),),
        (("synthetic-context", 1, scope["context_key"]),),
        signing_key=("synthetic-context", 1),
    )


def _credential(client: TestClient, scope: dict[str, Any]) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-2/15: use the real channel credential owner."""
    response = client.post(
        "/channels/token",
        headers={"X-API-Key": get_settings().api_key},
        json={
            "kind": "slack",
            "address": scope["address"],
            "adapter": scope["adapter"],
            "ttl_s": 60,
        },
    )
    assert response.status_code == 200, response.text
    token = response.json()["token"]
    claims = channel_token.verify_claims(
        token, get_settings().api_key, scope=channel_token.CHANNEL_ENQUEUE_SCOPE
    )
    assert claims is not None and claims.channel_id == scope["binding"]
    scope["generation"] = claims.generation
    return token


def _submission(scope: dict[str, Any], **proof_changes: Any) -> tuple[bytes, str]:
    """@spec HTTP-INVOCATION-EXECUTION-2/3: proof binds exact submitted bytes."""
    body = json.dumps(
        {
            "kind": "slack",
            "address": scope["address"],
            "adapter": scope["adapter"],
            "delivery_id": "synthetic-event-" + uuid.uuid4().hex,
            "conversation_id": scope["address"] + ":synthetic-thread",
            "author": SENDER,
            "text": "Synthetic member invocation",
            "reply_ref": "",
            "attachments": [],
        },
        separators=(",", ":"),
    ).encode()
    parsed = json.loads(body)
    proof = codec.ReceiverProofV1(
        schema_version=1,
        receiver_id=scope["receiver_id"],
        receiver_generation=1,
        channel_identity_id=scope["identity"],
        provider_installation_id=scope["installation"],
        binding_id=scope["binding"],
        binding_generation=scope["generation"],
        delivery_id=parsed["delivery_id"],
        native_channel_id=scope["address"],
        conversation_id=parsed["conversation_id"],
        body_sha256=hashlib.sha256(body).hexdigest(),
        slack=codec.ReceiverSlackEvidenceV1(
            delivery="app_mention",
            user_id=SENDER,
            event_user_team=None,
            event_team=ACCOUNT,
            interaction_user_team_id=None,
            is_ext_shared_channel=False,
            enterprise_ids=(),
        ),
    )
    if proof_changes:
        proof = codec.ReceiverProofV1.model_validate({**proof.model_dump(), **proof_changes})
    ring = codec.build_identity_keyring(
        "receiver", ((scope["receiver_id"], 1, scope["receiver_key"]),)
    )
    return body, codec.sign_receiver_proof(proof, ring)


def _keys(client: redis.Redis, binding: str) -> set[str]:
    """@spec HTTP-INVOCATION-EXECUTION-12/15: observe and clean only this binding's state."""
    return {
        key
        for key in client.scan_iter(match=CHANNEL_KEY_PREFIX + ":*")
        if binding in key.split(":")
    }


def _post(client: TestClient, token: str, body: bytes, proof: str) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-2/15: actual ASGI route and raw TurnIn body."""
    return client.post(
        "/channels/turns",
        content=body,
        headers={
            "X-API-Key": token,
            "X-Curie-Receiver-Proof": proof,
            "Content-Type": "application/json",
        },
    )


def test_owned_tls_source_echoes_fresh_request_scope(synthetic_provider: SimpleNamespace) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-12: qualify fixture independently of missing runtime."""
    provider = synthetic_provider

    async def exchange() -> None:
        """@spec HTTP-INVOCATION-EXECUTION-12: verified TLS, real public parser, no DB."""
        from curie_api.identity.http_transport import LookupTarget
        from curie_api.schemas.http_identity import FreshnessLimits

        request = LookupRequestV1.model_validate(copy.deepcopy(VECTORS["request"]))
        async with HttpIdentityClient(tls_context=provider.tls) as client:
            for member in (True, False):
                provider.member = member
                observation = await client.lookup(
                    target=LookupTarget(
                        provider.url,
                        request.scope.tenant_id,
                        request.scope.expected_issuer,
                        request.authority_version,
                    ),
                    bearer=ProviderBearer(BEARER),
                    request=request,
                    limits=FreshnessLimits(
                        max_identity_age_ms=2000, max_membership_age_ms=3000, max_clock_skew_ms=0
                    ),
                )
                assert isinstance(observation, HttpIdentityObservation)
                assert observation.facts.identity.kind == observation.facts.sender_kind == "human"
                assert observation.facts.groups[0].id == (
                    GROUP if member else "SyntheticOtherGroup"
                )
                assert json.loads(observation.response_bytes) == provider.responses[-1]
        assert client.close_status().state == "closed"
        assert provider.requests == [request.model_dump(mode="json")] * 2

    asyncio.run(exchange())


def test_current_http_member_queues_one_context_and_nonmember_writes_no_delivery_state(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-4/5/6/7/15: same receiver, credential and current group."""
    provider = synthetic_provider
    scope = _scope(provider.url)
    module = _runtime_api()
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    try:
        with TestClient(
            create_app(
                http_identity_runtime=_runtime(module, scope), http_identity_client=identity_client
            )
        ) as client:
            token = _credential(client, scope)
            body, proof = _submission(scope)
            assert _keys(valkey, scope["binding"]) == set() and valkey.xlen(runs_stream) == 0
            accepted = _post(client, token, body, proof)
            assert accepted.status_code == 200, accepted.text
            entries = valkey.xrange(runs_stream)
            assert len(entries) == 1
            stream_id, fields = entries[0]
            assert accepted.json()["stream_id"] == stream_id
            assert set(fields) == {"payload", "curie_identity_context_v1"}
            payload = fields["payload"]
            turn = QueuedTurn.model_validate_json(payload)
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),)
            )
            context = codec.open_invocation_context(fields["curie_identity_context_v1"], ring)
            assert isinstance(context, codec.InvocationContextV1)
            assert context.payload_sha256 == hashlib.sha256(payload.encode()).hexdigest()
            assert context.event_id == context.original_event_id == turn.event_id
            assert context.scope.model_dump() == {
                "tenant_id": scope["tenant"],
                "agent_id": scope["agent"],
                "binding_id": scope["binding"],
                "binding_generation": scope["generation"],
                "channel_identity_id": scope["identity"],
                "provider_installation_id": scope["installation"],
                "provider": "slack",
                "native_id": SENDER,
                "namespace": scope["namespace"].model_dump(),
            }
            assert context.receiver_id == scope["receiver_id"] and context.independent_human is True
            assert context.provider_version == scope["configuration"].version
            assert context.role_policy_version == scope["role_version"]
            assert context.canonical.subject == SUBJECT and context.canonical.issuer == ISSUER
            assert turn.author == SENDER and turn.reply_handle.placeholder is None
            assert turn.author_principal is not None and context.principal is not None
            assert turn.author_principal.model_dump() == context.principal.model_dump()
            with pytest.raises((AttributeError, TypeError, ValueError)):
                context.principal.principal_id = str(uuid.uuid4())

            async def original_association(factory: async_sessionmaker[AsyncSession]) -> None:
                """@spec HTTP-INVOCATION-EXECUTION-4/5/6: inspect durable provenance."""
                async with factory() as session:
                    row = await session.scalar(
                        select(Principal).where(
                            Principal.tenant_id == uuid.UUID(scope["tenant"]),
                            Principal.idp_issuer == ISSUER,
                            Principal.idp_subject == SUBJECT,
                        )
                    )
                    assert row is not None and row.type == "human" and row.status == "active"
                    assert turn.author_principal.principal_id == str(row.id)
                    assert turn.author_principal.authorization_version == row.authorization_version

            run(original_association)
            assert len(provider.requests) == 1
            observed = provider.requests[0]
            assert observed["authority_version"] == scope["configuration"].version
            assert observed["scope"]["tenant_id"] == scope["tenant"]
            assert observed["scope"]["sender"] == {
                "provider": "slack",
                "channel_identity_id": scope["identity"],
                "provider_installation_id": scope["installation"],
                "namespace": scope["namespace"].model_dump(),
                "native_id": SENDER,
            }
            previous = {key: valkey.get(key) for key in _keys(valkey, scope["binding"])}
            provider.member = False
            refused = _post(client, token, *_submission(scope))
            assert refused.status_code == 403 and refused.json() == {"detail": "caller_not_allowed"}
            assert len(provider.requests) == 2
            assert provider.requests[1]["request_id"] != observed["request_id"]
            assert provider.responses[1]["membership"]["complete"] is True
            assert provider.responses[1]["membership"]["groups"][0]["id"] != GROUP
            assert {key: valkey.get(key) for key in _keys(valkey, scope["binding"])} == previous
            assert valkey.xrange(runs_stream) == entries
        assert identity_client.close_status().state == "closed"
    finally:
        keys = _keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


@pytest.mark.parametrize(
    "tamper", ["body", "sender", "receiving_scope", "binding_generation", "mac"]
)
def test_tampered_receiver_request_refuses_before_provider_or_delivery_state(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,
    tamper: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/15: cryptography never substitutes for exact scope."""
    scope = _scope(synthetic_provider.url)
    module = _runtime_api()
    try:
        with TestClient(
            create_app(
                http_identity_runtime=_runtime(module, scope),
                http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
            )
        ) as client:
            token = _credential(client, scope)
            body, proof = _submission(scope)
            if tamper == "body":
                body = body.replace(
                    b"Synthetic member invocation", b"Synthetic modified invocation"
                )
            elif tamper == "sender":
                body, proof = _submission(
                    scope,
                    slack=codec.ReceiverSlackEvidenceV1(
                        delivery="app_mention",
                        user_id="U0EXAMPLE2",
                        event_user_team=None,
                        event_team=ACCOUNT,
                        interaction_user_team_id=None,
                        is_ext_shared_channel=False,
                        enterprise_ids=(),
                    ),
                )
            elif tamper == "receiving_scope":
                body, proof = _submission(scope, channel_identity_id=str(uuid.uuid4()))
            elif tamper == "binding_generation":
                body, proof = _submission(scope, binding_generation=scope["generation"] + 1)
            else:
                prefix, payload, signature = proof.split(".")
                proof = ".".join(
                    (prefix, payload, ("A" if signature[0] != "A" else "B") + signature[1:])
                )
            response = _post(client, token, body, proof)
            assert response.status_code == 401 and response.json() == {
                "detail": "invalid_http_receiver"
            }
            assert synthetic_provider.requests == []
            assert _keys(valkey, scope["binding"]) == set() and valkey.xlen(runs_stream) == 0
    finally:
        keys = _keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


def test_adopted_http_without_initialized_runtime_is_unavailable_before_lookup_or_state(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    hooks_client: TestClient,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-1/2/15: real valid proof cannot replace initialization."""
    scope = _scope(synthetic_provider.url)
    try:
        token = _credential(hooks_client, scope)
        response = _post(hooks_client, token, *_submission(scope))
        assert response.status_code == 503
        assert response.json() == {"detail": "identity_authority_unavailable"}
        assert synthetic_provider.requests == []
        assert _keys(valkey, scope["binding"]) == set() and valkey.xlen(runs_stream) == 0
    finally:
        keys = _keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


def _grid_evidence(**changes: Any) -> codec.ReceiverSlackEvidenceV1:
    """@spec HTTP-INVOCATION-EXECUTION-21: a synthetic Grid mention from a sibling workspace."""
    return codec.ReceiverSlackEvidenceV1.model_validate({
        "delivery": "app_mention", "user_id": SENDER, "event_user_team": "T0EXAMPLESIBLING",
        "event_team": GRID_WORKSPACE, "interaction_user_team_id": None,
        "is_ext_shared_channel": False, "enterprise_ids": (GRID_ORG,), **changes,
    })


def test_grid_workspace_member_queues_one_organisation_context_and_outsiders_refuse(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-2/15/21: synthetic Grid ids; no live Grid qualified."""
    provider = synthetic_provider
    scope = _scope(provider.url, grid=True)
    module = _runtime_api()
    try:
        with TestClient(
            create_app(
                http_identity_runtime=_runtime(module, scope),
                http_identity_client=HttpIdentityClient(tls_context=provider.tls),
            )
        ) as client:
            token = _credential(client, scope)
            for refused in (
                {"is_ext_shared_channel": True},
                {"is_ext_shared_channel": None},
                {"enterprise_ids": ()},
                {"enterprise_ids": ("E0EXAMPLEPARTNER",)},
                {"enterprise_ids": (GRID_ORG, "is_enterprise_install")},
            ):
                submission = _submission(scope, slack=_grid_evidence(**refused))
                response = _post(client, token, *submission)
                assert response.status_code == 401, refused
                assert response.json() == {"detail": "invalid_http_receiver"}
            assert provider.requests == []
            assert _keys(valkey, scope["binding"]) == set() and valkey.xlen(runs_stream) == 0
            accepted = _post(client, token, *_submission(scope, slack=_grid_evidence()))
            assert accepted.status_code == 200, accepted.text
            (entry,) = valkey.xrange(runs_stream)
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),)
            )
            context = codec.open_invocation_context(entry[1]["curie_identity_context_v1"], ring)
            assert isinstance(context, codec.InvocationContextV1)
            organisation = {
                "id": scope["namespace"].id, "authority": "", "kind": "slack_enterprise",
                "key": GRID_ORG,
            }
            assert context.scope.namespace.model_dump() == organisation
            assert context.scope.native_id == SENDER
            (observed,) = provider.requests
            assert observed["scope"]["sender"]["namespace"] == organisation
            assert observed["scope"]["sender"]["native_id"] == SENDER
            assert QueuedTurn.model_validate_json(entry[1]["payload"]).author == SENDER
    finally:
        keys = _keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)
