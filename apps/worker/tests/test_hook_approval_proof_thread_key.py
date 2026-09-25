"""The hook-approval proof rig's hand-built thread key matches the worker's.

``charts/curie/ci/hook-approval-proof.py`` cannot import ``channel_protocol``
or ``aci_protocol`` -- they need pydantic and a newer Python than the bare
``python3`` the rig runs under -- so it reproduces the rule
``_thread_key_for`` applies instead of calling it. The rig's own suite is an
opt-in live-cluster rung (``CURIE_E2E_HOOK_APPROVAL=1``) that cannot run here,
so this pins the two builders' outputs equal at the unit level for the rig's
own route: a Slack turn whose adapter is the rig's custom offline transport
(ADR-0168 decision 4).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
    conversation_id = "hook:abc"

    turn = QueuedTurn(
        event_id="EvSIM-hook-approval-proof",
        conversation_id=conversation_id,
        author="U0EXAMPLE1",
        text="ping",
        reply_handle=ReplyHandle(
            kind="slack",
            channel=proof.CHANNEL,
            placeholder="p-1",
            endpoint=proof.OFFLINE_ENDPOINT,
            adapter=proof.OFFLINE_ADAPTER,
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )

    assert proof._thread_key(conversation_id) == _thread_key_for(turn)
