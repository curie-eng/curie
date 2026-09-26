"""Keep the foreign-repository fixture tree out of this repository's collection.

``fixtures/repo_toolchain/tests/test_rates.py`` is a **deliberately failing**
stand-in for a foreign repository's own test suite: the red -> green -> red proof
in ``test_repo_toolchain_proof.py`` depends on it failing against the seeded
defect. It is executed only inside the runner container, against the fixture's
own virtualenv, and must never be imported or run by this repository's pytest.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _zero_probe_retry_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep real probe-failure tests off the retry backoff sleeps (#2945).

    The boot capability probe retries a failed dial with a bounded backoff;
    tests that dial a genuinely failing server (a missing stdio command)
    would otherwise sleep the backoff on every attempt. Only the retry tests
    assert on the backoff itself and override this in their own body.
    ``raising=False`` keeps this a no-op against a source without the retry,
    so the fix-pin gate stays intact.
    """

    from curie_runner import mcp_tool_capability

    monkeypatch.setattr(
        mcp_tool_capability, "_PROBE_RETRY_BACKOFF_SECONDS", 0.0, raising=False
    )


collect_ignore_glob = ["fixtures/**"]
