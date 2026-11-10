"""The mediated worker validates binding without possessing runner credentials."""

import asyncio
import uuid

import pytest
from curie_worker import protected_binding
from curie_worker.binding import ResolvedDeployment
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.protected_configuration import parse_execution_config
from sqlalchemy.ext.asyncio import create_async_engine

from .test_protected_binding import _delivery, canonical, execution_profile, manifest_for


@pytest.mark.parametrize("wrong_thread", [False, True])
def test_mediated_boot_requires_exact_binding_without_credentials(wrong_thread):
    async def run():
        cls = getattr(protected_binding, "ProtectedMediatedBindingResolver", None)
        assert cls is not None, "mediated worker binding is missing"
        profile = execution_profile()
        manifest = manifest_for(profile)
        engine = create_async_engine("postgresql+asyncpg://postgres:postgres@127.0.0.1:1/postgres")
        resolver = cls(
            engine, db_schema="curie", trusted_manifest=manifest,
            execution=parse_execution_config(canonical(profile), manifest=manifest),
        )
        delivery = _delivery(profile["agent_id"], address="C0EXAMPLE1")
        resolved = ResolvedDeployment(
            agent_id=uuid.UUID(profile["agent_id"]), agent_name="acme-bot",
            version_id=uuid.UUID(profile["version_id"]), version_label="v1",
            bundle_ref="bundle/example-v1", max_usd_per_day=10.25,
            max_output_tokens_per_run=4096,
        )
        try:
            async with protected_binding.protected_delivery_scope(delivery):
                key = "wrong/thread" if wrong_thread else _thread_key_for(delivery.turn)
                if wrong_thread:
                    with pytest.raises(ValueError):
                        resolver.boot_env(resolved, key, kind="slack", address="C0EXAMPLE1")
                else:
                    packs = resolver.packs_for(resolved)
                    assert not packs.nav.enabled
                    assert resolver.boot_env(
                        resolved, key, kind="slack", address="C0EXAMPLE1"
                    ) == {}
                    with pytest.raises(ValueError):
                        resolver.boot_env(
                            resolved, key, kind="slack", address="C0EXAMPLE1",
                            caller_run="untrusted/run",
                        )
        finally:
            await engine.dispose()

    asyncio.run(run())
