"""The hook-approval proof rig keeps replies offline through the default identity.

``charts/curie/ci/hook-approval-proof.py`` cannot import ``channel_protocol``
or ``aci_protocol`` -- they need pydantic and a newer Python than the bare
``python3`` the rig runs under -- so it reproduces the rule
``_thread_key_for`` applies instead of calling it, and checks its own offline
wiring by hand. The rig's own suite is an opt-in live-cluster rung
(``CURIE_E2E_HOOK_APPROVAL=1``) that cannot run here, so this pins both at the
unit level (ADR-0168 decisions 3 and 4).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from aci_protocol import QueuedTurn, ReplyHandle
from curie_worker.kernel import _thread_key_for

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[3] / "charts" / "curie" / "ci" / "hook-approval-proof.py"
)


def _load_proof_module():
    spec = importlib.util.spec_from_file_location(
        "curie_hook_approval_proof_under_test", _SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_rigs_thread_key_matches_the_workers_builder() -> None:
    proof = _load_proof_module()
    turn = QueuedTurn(
        event_id="EvSIM-hook-approval-proof",
        conversation_id="hook:abc",
        author="U0EXAMPLE1",
        text="ping",
        reply_handle=ReplyHandle(
            kind="slack",
            channel=proof.CHANNEL,
            placeholder="p-1",
            endpoint=None,
            adapter=proof.ROUTE_IDENTITY,
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )
    assert proof.ROUTE_IDENTITY == "default"
    assert proof._thread_key("hook:abc") == _thread_key_for(turn) == "slack:C0EXAMPLE1:hook%3Aabc"


def test_the_rig_requires_the_workers_own_slack_origin_to_be_offline() -> None:
    proof = _load_proof_module()
    offline = {
        "SLACK_API_BASE_URL": {"name": "SLACK_API_BASE_URL", "value": proof.OFFLINE_ENDPOINT}
    }
    proof.check_offline_slack_origin(offline)
    for env in (
        {},
        {"SLACK_API_BASE_URL": {"value": ""}},
        {"SLACK_API_BASE_URL": {"valueFrom": {"secretKeyRef": {}}}},
        {**offline, "CURIE_SLACK_TRUSTED_ORIGINS": {"value": "http://elsewhere.test"}},
        {**offline, "CURIE_SLACK_TRUSTED_ORIGINS": {"valueFrom": {"secretKeyRef": {}}}},
    ):
        with pytest.raises(proof.ProofError):
            proof.check_offline_slack_origin(env)
