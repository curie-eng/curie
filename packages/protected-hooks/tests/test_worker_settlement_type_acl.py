"""Actual TLS broker TYPE checks keep original proofs read-only.

@spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6.
"""

import asyncio

import pytest
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import verification_acl_rules
from redis.exceptions import NoPermissionError, ResponseError

from . import worker_broker as wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker

PROOF_KEYS = (
    "protected:admission:event:event-example",
    "protected:admission:intent:" + "a" * 64,
    "protected:admission:state:" + "a" * 64,
    "protected:admission:commit:" + "a" * 64,
    "protected:admission:binding:event-example",
    "protected:control:selection:33333333-3333-4333-8333-333333333333",
)
READ_KEYS = ("protected:lane:worker:type-example", *PROOF_KEYS)
FORBIDDEN_KEYS = (
    *PROOF_KEYS,
    "protected:source:11111111-1111-4111-8111-111111111111:incident",
    "foreign:type-example",
    "curie:worker:type-example",
)


@pytest.mark.parametrize("key", READ_KEYS)
@pytest.mark.parametrize("operation", ["direct", "lua"])
def test_actual_pinned_worker_allows_direct_and_lua_type_only_on_existing_families(
    owned_worker, key, operation
):
    broker = owned_worker.broker
    broker.command("SET", key, "SYNTHETIC-PROOF")
    owned_worker.rules = verification_acl_rules("worker")
    owned_worker.install()

    async def go():
        client = await AuthenticatedWorkerClient.connect(
            broker.manifest(), owned_worker.credential(), broker.ca_pem
        )
        try:
            if operation == "direct":
                assert await client.lane().runs.type(key) == "string"
            else:
                assert (
                    await client.lane().runs.eval("return redis.call('TYPE',KEYS[1]).ok", 1, key)
                    == "string"
                )
            assert await client.lane().runs.get(key) == "SYNTHETIC-PROOF"
        finally:
            await client.close()

    asyncio.run(go())
    assert broker.command("GET", key) == b"SYNTHETIC-PROOF"


@pytest.mark.parametrize("key", FORBIDDEN_KEYS)
def test_type_read_grants_do_not_permit_direct_or_lua_proof_source_control_foreign_writes(
    owned_worker, key
):
    broker = owned_worker.broker
    broker.command("SET", key, "SYNTHETIC-PROOF")
    owned_worker.rules = verification_acl_rules("worker")
    owned_worker.install()

    async def go():
        client = await AuthenticatedWorkerClient.connect(
            broker.manifest(), owned_worker.credential(), broker.ca_pem
        )
        try:
            for command in ("SET", "DEL"):
                args = (key, "replacement") if command == "SET" else (key,)
                with pytest.raises(NoPermissionError):
                    await client.lane().runs.execute_command(command, *args)
                script = (
                    "return redis.call('SET',KEYS[1],'replacement')"
                    if command == "SET"
                    else "return redis.call('DEL',KEYS[1])"
                )
                with pytest.raises(ResponseError, match="[Pp]ermission|NOPERM|[Aa][Cc][Ll]"):
                    await client.lane().runs.eval(script, 1, key)
        finally:
            await client.close()

    asyncio.run(go())
    assert broker.command("GET", key) == b"SYNTHETIC-PROOF"


def test_type_read_grants_leave_admin_and_admission_quota_mutation_broker_denied(owned_worker):
    broker = owned_worker.broker
    quota = "protected:admission:quota"
    broker.command("ZADD", quota, 1, "a" * 64)
    owned_worker.rules = verification_acl_rules("worker")
    owned_worker.install()

    async def go():
        client = await AuthenticatedWorkerClient.connect(
            broker.manifest(), owned_worker.credential(), broker.ca_pem
        )
        try:
            with pytest.raises(NoPermissionError):
                await client.lane().runs.execute_command("CONFIG", "GET", "maxmemory")
            with pytest.raises(NoPermissionError):
                await client.lane().runs.zrem(quota, "a" * 64)
            with pytest.raises(ResponseError, match="[Pp]ermission|NOPERM|[Aa][Cc][Ll]"):
                await client.lane().runs.eval(
                    "return redis.call('ZREM',KEYS[1],ARGV[1])", 1, quota, "a" * 64
                )
        finally:
            await client.close()

    asyncio.run(go())
    assert broker.command("ZSCORE", quota, "a" * 64) is not None


@pytest.mark.parametrize(
    "case",
    [
        "lease",
        "generation",
        "consumer",
        "proof",
        "type",
        "terminal-prefix",
        "resolution-prefix",
        "graveyard",
    ],
)
def test_settlement_lua_mechanics_use_same_owned_tls_broker(pworker_service, case):
    """Synthetic transaction mechanics only; not authenticated settlement qualification."""
    import importlib.util
    from pathlib import Path
    from urllib.parse import quote

    path = (
        Path(__file__).resolve().parents[3]
        / "apps/worker/tests/protected_settlement"
        / "test_lua_fences.py"
    )
    spec = importlib.util.spec_from_file_location("settlement_mechanics", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    broker = pworker_service
    endpoint = wb.FixtureSecret(
        "rediss://provisioner:"
        + quote(broker.admin.connection_pool.connection_kwargs["password"], safe="")
        + "@127.0.0.1:"
        + str(broker.port)
        + "/0?ssl_cert_reqs=required&ssl_ca_certs="
        + quote(str(broker.private / "ca.crt"), safe="")
        + "&ssl_check_hostname=true"
    )
    if case in {"lease", "generation", "consumer", "proof", "type"}:
        module.test_wrong_fence_proof_or_type_writes_nothing(endpoint, case)
    elif case == "terminal-prefix":
        module.test_terminal_prefix_survives_repair_and_replay_preserves_generation(endpoint)
    elif case == "resolution-prefix":
        module.test_resolution_prefix_repairs_without_recreating_outbox(endpoint)
    else:
        module.test_clear_generation_race_retains_owed_and_graveyard_precedes_resolution(endpoint)
