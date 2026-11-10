"""Exact-image custody proof, gated by CURIE_PERSONAL_LIVE=1.

The caller supplies the scratch Docker context, throwaway Kubernetes context,
fresh namespace and source-built CURIE_BIN. CURIE_PERSONAL_PROOF_PHASE=synthetic
exercises all unpaid observations without a grant. The default live phase also
requires a privately loaded CLAUDE_CODE_OAUTH_TOKEN, its stat-only private input
and a stable CURIE_PERSONAL_PROOF_RUN_ID for the durable two-session ledger.
No live failure is retried. Local stdio MCP inheritance remains unproven.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator

import pytest
from personal_token_cluster_support import (
    ClusterProof,
    HeldSecret,
    ProofCheckError,
    validate_inputs,
)

__tracebackhide__ = True
_GRANT = HeldSecret(os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", ""))


@pytest.fixture
def personal_cluster(request: pytest.FixtureRequest) -> Iterator[ClusterProof]:
    __tracebackhide__ = True
    if os.environ.get("CURIE_PERSONAL_LIVE") != "1":
        _GRANT.clear()
        pytest.skip("set CURIE_PERSONAL_LIVE=1 for the owned cluster proof")
    phase = os.environ.get("CURIE_PERSONAL_PROOF_PHASE", "live")
    validate_inputs(_GRANT, phase)
    proof = ClusterProof(_GRANT, phase)

    def finish() -> None:
        __tracebackhide__ = True
        try:
            proof.cleanup()
        finally:
            reporter = request.config.pluginmanager.get_plugin("terminalreporter")
            if reporter is not None:
                reporter.write_line(json.dumps(proof.evidence, sort_keys=True))

    request.addfinalizer(finish)
    try:
        proof.setup()
    except Exception as exc:  # noqa: BLE001 - provider errors must not expose private input
        proof.evidence["failure"] = (
            str(exc) if isinstance(exc, ProofCheckError) else type(exc).__name__
        )
        raise RuntimeError(
            "personal cluster proof setup failed; private details withheld"
        ) from None
    yield proof


def test_personal_token_custody_on_exact_candidate(personal_cluster: ClusterProof) -> None:
    __tracebackhide__ = True
    try:
        personal_cluster.run()
    except Exception as exc:  # noqa: BLE001 - provider errors must not expose private input
        personal_cluster.evidence["failure"] = (
            str(exc) if isinstance(exc, ProofCheckError) else type(exc).__name__
        )
        raise AssertionError(
            "personal cluster custody proof failed; private details withheld"
        ) from None
