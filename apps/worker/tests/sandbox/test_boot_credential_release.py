"""#3823: deleting a claimed sandbox reports the boot credential it holds."""

from __future__ import annotations

import time
import uuid

from curie_worker.binding import HISTORY_TOKEN_ENV
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.substrate import SandboxSubstrate
from curie_worker.sandbox.types import SubstrateConfig
from curie_worker.sandbox_token import mint

from .conftest import FakeSandboxClient


def test_release_reports_the_boot_credential(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    reported: list[tuple[str, str]] = []
    substrate.set_boot_credential_revoker(
        lambda agent, cred: reported.append((agent, cred)) or True
    )
    agent = "22222222-2222-4222-8222-222222222222"
    cred = uuid.uuid4().hex
    token = mint(
        "api-key",
        agent=agent,
        scope="state",
        exp=int(time.time()) + 600,
        claims={"binding": "slack:C0EXAMPLE1", "memory": "read", "cred": cred},
    )
    handle = substrate.claim("thread-1", env={HISTORY_TOKEN_ENV: token})
    assert handle.state_credential_id == cred
    assert handle.state_credential_agent == agent
    assert substrate.release("thread-1")
    assert reported == [(agent, cred)]
    # A second release has no route left to report.
    assert not substrate.release("thread-1")
    assert reported == [(agent, cred)]
    assert affinity.claim_credential(handle.claim_name) is None


def test_a_failed_report_is_retried_once_the_claim_is_gone(
    fake_k8s: FakeSandboxClient, affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = SandboxSubstrate(fake_k8s, affinity, config)
    calls = {"n": 0}

    def revoker(agent: str, cred: str) -> bool:
        del agent, cred
        calls["n"] += 1
        return calls["n"] >= 2

    substrate.set_boot_credential_revoker(revoker)
    agent = "33333333-3333-4333-8333-333333333333"
    cred = uuid.uuid4().hex
    token = mint(
        "api-key",
        agent=agent,
        scope="state",
        exp=int(time.time()) + 600,
        claims={"binding": "slack:C0EXAMPLE1", "memory": "read", "cred": cred},
    )
    handle = substrate.claim("thread-retry", env={HISTORY_TOKEN_ENV: token})
    assert substrate.release("thread-retry")
    assert calls["n"] == 1
    assert affinity.claim_credential(handle.claim_name) == (agent, cred)
    substrate.reap_orphans()
    assert calls["n"] == 2
    assert affinity.claim_credential(handle.claim_name) is None
