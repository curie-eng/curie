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
    substrate.set_boot_credential_revoker(lambda agent, cred: reported.append((agent, cred)))
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
