"""Protected binding against real Postgres, @spec PROTECTED-HOOK-LANE-6/7."""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
import uuid
from collections.abc import Mapping
from pathlib import Path

import pytest
from aci_protocol import BootEnv, QueuedTurn
from curie_protected_hooks.admission_records import parse_envelope, parse_intent
from curie_protected_hooks.worker_verification import Dispatchable
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.binding import ResolvedDeployment
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.protected_binding import (
    ProtectedBindingResolver,
    protected_delivery_scope,
)
from curie_worker.protected_configuration import parse_execution_config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

sys.path.insert(0, str(Path(__file__).parents[1]))
from protected_configuration_vectors import canonical, execution_profile, manifest_for  # noqa: E402
from test_protected_history import dispatchable  # noqa: E402

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")


class _Credentials:
    def __init__(self) -> None:
        self.history_binding: str | None = None
        self.expiry: int | None = None

    def model_credential(self, _reference: Mapping[str, str]) -> str:
        return "test-model-credential"

    def history_token(
        self,
        _reference: Mapping[str, str],
        *,
        agent_id: str,
        transcript_binding: str,
        expires_at: int,
    ) -> str:
        assert agent_id
        self.history_binding = transcript_binding
        self.expiry = expires_at
        return "test-history-token"


def _delivery(agent_id: str, *, address: str, profile: dict | None = None) -> Dispatchable:
    turn = QueuedTurn.model_validate(
        {
            "event_id": "event/protected",
            "conversation_id": "conversation/protected",
            "author": "hook/protected",
            "text": "test prompt",
            "received_at": "2026-10-08T00:00:00Z",
            "source": "webhook",
            "attachments": [],
            "hook_run": None,
            "tool_access": "read-only",
            "reply_handle": {
                "kind": "slack",
                "channel": address,
                "placeholder": None,
                "adapter": "default",
                "endpoint": None,
            },
        }
    )
    profile = profile or execution_profile(agent_id=agent_id)
    manifest = manifest_for(profile)
    original = dispatchable(
        channel=address, adapter="default", event=turn.event_id, conversation=turn.conversation_id
    )
    envelope = original.envelope.as_dict()
    envelope.update(
        manifest_digest=manifest.digest,
        execution_config_digest=manifest.as_dict()["execution_config_digest"],
        runner_image_digest=manifest.as_dict()["runner_image_digest"],
        payload_sha256=hashlib.sha256(turn.model_dump_json().encode()).hexdigest(),
    )
    intent = original.intent.as_dict()
    intent["identity"]["agent_id"] = agent_id
    intent.update(
        manifest_digest=manifest.digest,
        payload_sha256=envelope["payload_sha256"],
        envelope_sha256=hashlib.sha256(canonical(envelope)).hexdigest(),
    )
    return Dispatchable(turn, parse_envelope(canonical(envelope)), parse_intent(canonical(intent)))


def test_resolves_exact_inactive_history_and_renders_only_protected_boot() -> None:
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        agent_id, version_id, route_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        address = "C0EXAMPLE1"
        profile = execution_profile(agent_id=str(agent_id), version_id=str(version_id))
        manifest = manifest_for(profile)
        execution = parse_execution_config(canonical(profile), manifest=manifest)
        credentials = _Credentials()
        connected = False
        try:
            await pg_connect_or_skip(engine)
            connected = True
            async with engine.begin() as connection:
                await connection.execute(
                    text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
                    {"id": agent_id, "name": f"protected-test-{agent_id.hex}"},
                )
                await connection.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.agent_channels "
                        "(id, agent_id, kind, address, adapter) "
                        "VALUES (:id, :agent_id, 'slack', :address, 'default')"
                    ),
                    {"id": route_id, "agent_id": agent_id, "address": address},
                )
                await connection.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.agent_versions "
                        "(id, agent_id, version_label, bundle_ref, bundle_sha256, created_by) "
                        "VALUES (:id, :agent_id, :label, :ref, :sha, 'test')"
                    ),
                    {
                        "id": version_id,
                        "agent_id": agent_id,
                        "label": "v-protected",
                        "ref": "bundle/example-v1",
                        "sha": "f" * 64,
                    },
                )

            resolver = ProtectedBindingResolver(
                engine,
                db_schema=_SCHEMA,
                execution=execution,
                trusted_manifest=manifest,
                credentials=credentials,
            )
            delivery = _delivery(str(agent_id), address=address, profile=profile)
            async with protected_delivery_scope(delivery):
                resolved = await resolver.resolve("slack", "default", address)
                assert resolved is not None
                assert resolved.deployment_id is None
                assert resolved.bundle_ref == "bundle/example-v1"
                assert resolved.model == profile["runner"]["model"]
                assert resolved.secrets is None and resolved.behavior_packs is None
                assert await resolver.resolve("slack", "default", "C0EXAMPLE2") is None
                with pytest.raises(ValueError, match="does not match"):
                    resolver.boot_env(
                        resolved,
                        _thread_key_for(delivery.turn) + "-other",
                        kind="slack",
                        address=address,
                    )
                env = resolver.boot_env(
                    resolved,
                    _thread_key_for(delivery.turn),
                    kind="slack",
                    address=address,
                )

            assert env[BootEnv.env_key("fake_model")] == "0"
            assert env[BootEnv.env_key("max_turns")] == "8"
            assert env[profile["runner"]["model_env_key"]] == "test-model-credential"
            assert env[BootEnv.env_key("history_token")] == "test-history-token"
            assert BootEnv.env_key("memory_ref") not in env
            assert BootEnv.env_key("memory_token") not in env
            assert BootEnv.env_key("state_url") not in env
            assert BootEnv.env_key("state_token") not in env
            assert credentials.history_binding is not None
            assert credentials.history_binding.startswith("@protected:")
            assert credentials.expiry is not None
        finally:
            if connected:
                async with engine.begin() as connection:
                    await connection.execute(
                        text(f"DELETE FROM {_SCHEMA}.agent_channels WHERE id = :id"),
                        {"id": route_id},
                    )
                    await connection.execute(
                        text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"),
                        {"id": agent_id},
                    )
            await engine.dispose()

    asyncio.run(go())


def test_missing_delivery_context_refuses_resolution() -> None:
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        profile = execution_profile()
        execution = parse_execution_config(canonical(profile), manifest=manifest_for(profile))
        resolver = ProtectedBindingResolver(
            engine,
            db_schema=_SCHEMA,
            execution=execution,
            trusted_manifest=manifest_for(profile),
            credentials=_Credentials(),
        )
        try:
            with pytest.raises(ValueError, match="verified delivery context"):
                await resolver.resolve("slack", "default", "CEXAMPLE1")
        finally:
            await engine.dispose()

    asyncio.run(go())


@pytest.mark.parametrize(
    "field",
    [
        None,
        "execution_config_digest",
        "runner_image_digest",
        "bundle_digest",
        "manifest_digest",
        "agent_id",
    ],
)
def test_boot_uses_real_envelope_and_rejects_substituted_profile(field) -> None:
    """A verified delivery from another profile cannot use this credential provider."""

    async def go() -> None:
        profile = execution_profile()
        execution = parse_execution_config(canonical(profile), manifest=manifest_for(profile))
        credentials = _Credentials()
        engine = create_async_engine(_DB_URL)
        resolver = ProtectedBindingResolver(
            engine,
            db_schema=_SCHEMA,
            execution=execution,
            trusted_manifest=manifest_for(profile),
            credentials=credentials,
        )
        delivery = _delivery(profile["agent_id"], address="C0EXAMPLE1")
        envelope = delivery.envelope.as_dict()
        intent = delivery.intent.as_dict()
        if field == "agent_id":
            intent["identity"]["agent_id"] = "11111111-1111-4111-8111-111111111112"
        elif field is not None:
            envelope[field] = "sha256:" + "0" * 64 if field == "runner_image_digest" else "0" * 64
        delivery = Dispatchable(
            delivery.turn, parse_envelope(canonical(envelope)), parse_intent(canonical(intent))
        )
        resolved = ResolvedDeployment(
            agent_id=uuid.UUID(profile["agent_id"]),
            agent_name="acme-bot",
            version_id=uuid.UUID(profile["version_id"]),
            version_label="v1",
            bundle_ref="bundle/example-v1",
            max_usd_per_day=10.25,
            max_output_tokens_per_run=4096,
        )
        try:
            async with protected_delivery_scope(delivery):
                if field is None:
                    env = resolver.boot_env(
                        resolved,
                        _thread_key_for(delivery.turn),
                        kind="slack",
                        address="C0EXAMPLE1",
                    )
                    assert env[BootEnv.env_key("bundle_ref")] == "bundle/example-v1"
                    assert credentials.history_binding is not None
                else:
                    with pytest.raises(ValueError, match="execution profile"):
                        await resolver.resolve("slack", "default", "C0EXAMPLE1")
                    with pytest.raises(ValueError, match="execution profile"):
                        resolver.boot_env(
                            resolved,
                            _thread_key_for(delivery.turn),
                            kind="slack",
                            address="C0EXAMPLE1",
                        )
                    assert credentials.history_binding is None
                    assert credentials.expiry is None
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_resolver_rejects_profile_from_a_different_manifest() -> None:
    """An independently valid codec profile cannot silently bind to another manifest."""

    async def go() -> None:
        profile = execution_profile()
        execution = parse_execution_config(canonical(profile), manifest=manifest_for(profile))
        other = execution_profile()
        other["runner"]["max_turns"] = 9
        engine = create_async_engine(_DB_URL)
        try:
            with pytest.raises(ValueError, match="execution profile"):
                ProtectedBindingResolver(
                    engine,
                    db_schema=_SCHEMA,
                    execution=execution,
                    trusted_manifest=manifest_for(other),
                    credentials=_Credentials(),
                )
        finally:
            await engine.dispose()

    asyncio.run(go())


def _classifying_resolver(engine) -> ProtectedBindingResolver:
    """@spec HTTP-INVOCATION-EXECUTION-18: the production protected owner, no delivery."""
    profile = execution_profile()
    return ProtectedBindingResolver(
        engine,
        db_schema=_SCHEMA,
        execution=parse_execution_config(canonical(profile), manifest=manifest_for(profile)),
        trusted_manifest=manifest_for(profile),
        credentials=_Credentials(),
    )


@pytest.mark.parametrize("recognized", [False, True], ids=["context-free", "recognized"])
def test_protected_owner_is_an_explicitly_local_classifier(recognized: bool) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-18: protected turns are not HTTP invocations."""
    from types import SimpleNamespace

    from curie_worker.kernel import identity
    from queue_fixtures import qevent

    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            resolver = _classifying_resolver(engine)
            assert await resolver.identity_binding("slack", "default", "CEXAMPLE1") is None
            turn = qevent("protected synthetic turn", channel="CEXAMPLE1")
            carry = identity.HttpTurnCarry(
                turn, "synthetic-context" if recognized else None,
                hashlib.sha256(b"synthetic").hexdigest() if recognized else None,
            )
            token = identity.CURRENT.set(carry)
            try:
                kernel = SimpleNamespace(_binding=resolver, _identity_authority=None)
                if recognized:
                    with pytest.raises(identity.HttpIdentityRefused):
                        await identity.prepare(kernel)
                else:
                    await identity.prepare(kernel)
                    assert carry.legacy is True and carry.scope is None
            finally:
                identity.CURRENT.reset(token)
        finally:
            await engine.dispose()

    asyncio.run(go())
