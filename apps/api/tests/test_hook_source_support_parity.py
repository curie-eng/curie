"""Probe ``supported`` and parity with ingress admission, @spec PROTECTED-HOOK-SOURCE-9.

The support probe answers 200 ``supported`` with the runtime members exactly
when, in one request, the committed protected row computes, the agent has no
source bindings (step 0), the runtime files are valid, one control reader
session yields ``accept`` and ``enqueue.json`` is valid and bound to the
manifest (step 12). The parity test drives the same seeded broker states
through the probe over HTTP and through a real signed delivery and asserts
the delivery is accepted exactly when the probe answered ``supported``,
outside the stated per delivery exclusions. See ``_protected_ingress_harness``.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from _protected_ingress_harness import (
    _broker,
    deliver,
    enqueue_file,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    private_entries,
    provision,
    seed_row,
    signed,
)
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    BODIES,
    GENERATION,
    expected,
    post_support,
    scoped_secret,
    support_db,
    support_headers,
)
from test_hook_source_support_broker import STEPS, Runtime, runtime_members

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service

# Faults on the control reader principal alone: the probe reads with it and
# ingress does not, so they are the stated availability difference, not a
# tuple difference (@spec PROTECTED-HOOK-SOURCE-9).
READER_ONLY = {"2-wrong-password", "2-reader-disabled", "2-control-read-refused", "2-time-refused"}


def run(scenario: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    asyncio.run(asyncio.wait_for(scenario(), 60))


async def probe(client: Any, agent: str, requested: str | None = None) -> Any:
    """Signed support probe under the current scoped key, @spec PROTECTED-HOOK-SOURCE-9."""
    body = BODIES[requested]
    return await post_support(
        client,
        agent,
        body,
        support_headers(
            scoped_secret(agent),
            requested=requested,
            body=body,
            delivery="support-" + secrets.token_hex(4),
        ),
    )


def supported(rt: Runtime, requested: str | None) -> dict[str, Any]:
    """The 200 DTO, @spec PROTECTED-HOOK-SOURCE-9."""
    want = expected(requested, "read-only", str(GENERATION), "supported")
    want.update(runtime_members(rt), supported=True)
    return want


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_fully_valid_tuple_answers_200_supported_with_runtime_members(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, requested: str | None
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            response = await probe(client, agent, requested)
            assert response.status_code == 200, response.text
            assert response.json() == supported(rt, requested)
            for value in (broker.enqueue.username, broker.enqueue.password):
                assert value not in response.text

    run(scenario)


def _enqueue_missing(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.files["enqueue.json"] = None


def _enqueue_unbound(rt: Runtime, broker: Any) -> None:
    """A stale file after a provisioner rotation, @spec PROTECTED-HOOK-SOURCE-6/9."""
    rt.files["enqueue.json"] = enqueue_file(
        broker, {"id": "credential/example-enqueue", "generation": "2"}
    )


def _enqueue_invalid(rt: Runtime, broker: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    rt.files["enqueue.json"] = b"{}"


@pytest.mark.parametrize(
    "mutate",
    [_enqueue_missing, _enqueue_unbound, _enqueue_invalid],
    ids=["missing", "unbound", "invalid"],
)
def test_step_12_enqueue_file_reports_runtime_unavailable_with_members(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutate: Any
) -> None:
    """The probe parses ``enqueue.json`` after step 11 and never connects with it.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-6.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            mutate(rt, broker)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            response = await probe(client, agent)
            assert response.status_code == 503, response.text
            want = expected(None, "read-only", str(GENERATION), "runtime_unavailable")
            want.update(runtime_members(rt))
            assert response.json() == want

    run(scenario)


def test_closed_admission_reports_runtime_unavailable_with_members(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            rt.selection["admission_open"] = False
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            response = await probe(client, agent)
            assert response.status_code == 503, response.text
            want = expected(None, "read-only", str(GENERATION), "runtime_unavailable")
            want.update(runtime_members(rt))
            assert response.json() == want

    run(scenario)


async def _bind(client: Any, agent: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    from test_hook_source_ingress_admission import bind_sources

    await bind_sources(client, agent, "other-hook")


@pytest.mark.parametrize("runtime", ["valid", "unset"])
def test_step_0_source_bindings_answer_configuration_unsupported_on_probe_and_ingress(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runtime: str
) -> None:
    """Source bindings decide before the runtime files; no runtime members; ingress agrees.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with ingress_app(tmp_path, monkeypatch, configure=runtime == "valid") as (
            _app,
            client,
            agent,
            directory,
        ):
            rt = provision(broker, agent, directory)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            await _bind(client, agent)
            response = await probe(client, agent)
            assert response.status_code == 503, response.text
            assert response.json() == expected(
                None, "read-only", str(GENERATION), "configuration_unsupported"
            )
            delivered = await deliver(client, agent, signed(scoped_secret(agent)))
            assert delivered.status_code == 503, delivered.text
            assert delivered.json() == {"detail": "configuration_unsupported"}

    run(scenario)


# Every probe step of the base contract plus the two new ones, as seeded states.
PARITY: list[tuple[str, Callable[[Runtime, Any], None]]] = [
    (case[0], case[1]) for case in STEPS if case[0] not in READER_ONLY
] + [
    ("0-valid", lambda rt, broker: None),
    ("12-enqueue-missing", _enqueue_missing),
    ("12-enqueue-unbound", _enqueue_unbound),
]


@pytest.mark.parametrize(
    "mutate,valid", [(c[1], c[0] == "0-valid") for c in PARITY], ids=[c[0] for c in PARITY]
)
def test_delivery_is_accepted_exactly_when_the_probe_answered_supported(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutate: Any, valid: bool
) -> None:
    """Parity over the same seeded broker states, probe first, then one signed delivery.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-8 @spec PROTECTED-HOOK-LANE-4.
    """
    broker = ingress_broker

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
            rt = provision(broker, agent, directory)
            mutate(rt, broker)
            await asyncio.to_thread(seed_row, agent, rt)
            install(broker, rt)
            answered = await probe(client, agent)
            assert answered.status_code == (200 if valid else 503), answered.text
            delivered = await deliver(client, agent, signed(scoped_secret(agent)))
            accepted = (
                delivered.status_code == 200
                and delivered.json().get("acceptance_status") == "accepted"
            )
            assert accepted == (answered.status_code == 200), (
                f"probe {answered.status_code} {answered.json().get('reason')} but delivery "
                f"{delivered.status_code} {delivered.text}"
            )
            assert len(private_entries(broker)) == (1 if accepted else 0)
            if not accepted:
                assert delivered.status_code == 503, delivered.text

    run(scenario)
