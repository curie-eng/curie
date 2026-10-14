"""Owned proxy handles cannot be constructed into positive authority."""

import asyncio

import pytest
from curie_worker.protected_launch_client import LaunchClientError, _budget
from curie_worker.protected_runner import ProtectedRunnerClient
from curie_worker.protected_substrate import ProtectedSandboxSubstrate


def test_spent_budget_never_becomes_unbounded_timeout():
    for value in (0, -1, float("nan"), float("inf")):
        with pytest.raises(LaunchClientError):
            _budget(value)
    assert _budget(12) == 5
    assert _budget(0.1) == 0.1


def test_generic_steer_never_needs_proof_or_transport():
    async def go():
        runner = object.__new__(ProtectedRunnerClient)
        with pytest.raises(LaunchClientError):
            await runner.steer("https://runner.example.com", object(), token="private")

    asyncio.run(go())


def test_substrate_refuses_unowned_handle_without_network():
    substrate = object.__new__(ProtectedSandboxSubstrate)
    substrate._handles = {}
    with pytest.raises(LaunchClientError):
        substrate.capability_for(object())


def test_copied_capability_never_selects_authenticated_owned_target():
    from types import SimpleNamespace

    from curie_worker.protected_launch_client import ProtectedLaunchClient
    from curie_worker.protected_launch_records import WorkerLaunchCapability

    original = WorkerLaunchCapability(
        "https://mediator.example.com:443",
        "77777777-7777-4777-8777-777777777777",
        "worker-example",
        "A" * 43,
    )
    copy = WorkerLaunchCapability(
        original.origin,
        original.launch_id,
        original.worker_principal_id,
        original.control_capability,
    )
    client = object.__new__(ProtectedLaunchClient)
    client._owned = {original.launch_id: SimpleNamespace(capability=original)}
    with pytest.raises(LaunchClientError):
        client._target(copy)
    with pytest.raises(LaunchClientError):
        client._target(object())


def test_explicit_wrapper_never_forwards_generic_url_or_bearer():
    runner = object.__new__(ProtectedRunnerClient)
    substrate = object.__new__(ProtectedSandboxSubstrate)
    substrate._handles = {}
    runner._substrate = substrate
    with pytest.raises(LaunchClientError):
        runner._capability("https://runner.example.com", None)
    with pytest.raises(LaunchClientError):
        runner._capability("https://runner.example.com", "private-marker")


@pytest.mark.parametrize("principal", ["worker/example", "worker@example", "worker+example"])
def test_launch_tls_config_accepts_exact_provisioned_opaque_ref_grammar(principal):
    from curie_worker.protected_launch_client import LaunchTLSConfig

    config = LaunchTLSConfig(
        "https://mediator.example.com:443",
        "ca",
        "mediator.example.com",
        "a" * 64,
        "client",
        "key",
        principal,
    )
    assert config.worker_principal_id == principal


@pytest.mark.parametrize("principal", ["", "/worker", "worker\n", "worker example", "é", "w" * 257])
def test_launch_tls_config_refuses_invalid_principal_before_transport(principal):
    from curie_worker.protected_launch_client import LaunchTLSConfig

    with pytest.raises(LaunchClientError):
        LaunchTLSConfig(
            "https://mediator.example.com:443",
            "ca",
            "mediator.example.com",
            "a" * 64,
            "client",
            "key",
            principal,
        )
