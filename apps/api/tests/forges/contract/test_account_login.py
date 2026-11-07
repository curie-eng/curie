"""A code host account's current login, read by its immutable id (#5414).

Factory Slack steering maps a linked person to their code host account by the
numeric id an operator recorded, then needs that account's login now to ask the
host for write permission. Only GitHub can answer this today (ADR 0198 names
"GitHub: the host, numeric user id"); every other adapter declares the optional
operation unsupported and raises, so the caller refuses instead of guessing.

GitHub shape: https://docs.github.com/en/rest/users/users#get-a-user-using-their-id
(``GET /user/{account_id}`` answers the account object with ``id`` and ``login``).
"""

from __future__ import annotations

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import (
    OPTIONAL_OPERATIONS,
    CapabilityTier,
    Operation,
    Support,
    supports,
    tier,
)
from curie_api.forges.errors import Unsupported
from forge_fakes.contract_harness import AdapterHarness


def test_account_login_is_an_optional_code_host_operation() -> None:
    assert Operation.ACCOUNT_LOGIN.value == "code_host.account_login"
    assert Operation.ACCOUNT_LOGIN in OPTIONAL_OPERATIONS
    assert tier(Operation.ACCOUNT_LOGIN) is CapabilityTier.OPTIONAL


def test_only_a_github_code_host_declares_account_login(harness: AdapterHarness) -> None:
    declared = harness.code_host.capabilities[Operation.ACCOUNT_LOGIN]
    if harness.code_host.kind == types.GITHUB:
        assert declared is Support.SUPPORTED
    else:
        assert declared is Support.UNSUPPORTED


@pytest.mark.anyio
async def test_github_answers_the_login_only_for_the_matching_account_id(
    harness: AdapterHarness,
) -> None:
    code_host, repository = harness.code_host, harness.repository
    if not supports(code_host.capabilities, Operation.ACCOUNT_LOGIN):
        with pytest.raises(Unsupported) as refused:
            await code_host.account_login(repository, "6601")
        assert refused.value.operation is Operation.ACCOUNT_LOGIN
        return
    seed = harness.seed_account  # type: ignore[attr-defined]
    seed("6601", "octo-writer")
    # The payload names another account than the one asked for: never trusted.
    seed("6603", "octo-impostor", payload_id=6604)
    before = harness.write_count()

    assert await code_host.account_login(repository, "6601") == "octo-writer"
    assert await code_host.account_login(repository, "6603") is None
    # An account the host does not know is no login, not an error.
    assert await code_host.account_login(repository, "6609") is None
    assert harness.write_count() == before
