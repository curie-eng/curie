"""Service-free tests for the channel-identity scope guard (#5246).

``scopes`` is a list of provider permission names (``chat:write``), never a
credential, but the column has no grammar to anchor a known prefix to -- the
same position ``attributes`` is in -- so every supplied scope passes the same
finite deny the attributes bag gets, on create and on update, through the real
production guard (:func:`curie_api.channel_identities.check_scopes`). A
refusal is a plain ``IdentityInvalid`` whose message never carries the
submitted value and which chains nothing.

These are the guard-level pair of the authenticated route regressions in
``test_channel_identities.py::test_scope_credential_values_are_refused_without_persistence``:
they need no service, database or credential, so the factory runner executes
them directly; the POST/PATCH persistence controls live there and run against
real Postgres in CI.
"""

from __future__ import annotations

import pytest
from curie_api.channel_identities import IdentityInvalid, check_scopes

# Synthetic shapes the attributes deny already refuses; none is a real
# credential. Every entry must match ``_ATTRIBUTES_DENY_RE``.
# gitleaks:allow -- fabricated test fixtures, never real credentials
PASTED = [
    "xoxb-123-SECRET",
    "xoxe.xoxp-1-abc",
    "xapp-1-abc",
    "ghp_abc123def456",
    "sk-ant-api03-abc",
    "sk_live_abc123",
    "rk_live_abc123",
    "lin_api_abc123",
    "AIzaSyA1b2c3D4e5F6g7",
    "AKIA1234567890ABCDEF",
    "ASIA1234567890ABCDEF",
    "0123456789abcdef" * 2,
    "eyJhbGciOiJIUzI1NiJ9.e30.sig",
]

# Ordinary permission names, including the empty list: omitted scopes mean no
# scopes, and an explicitly empty list is the same write.
ORDINARY = [
    ["chat:write"],
    ["chat:write", "users:read", "commands"],
    ["im:history", "app_mentions:read", "dnd:read"],
    [],
]


@pytest.mark.parametrize("scopes", ORDINARY, ids=["single", "many", "dotted", "empty"])
def test_guard_accepts_ordinary_and_empty_scope_lists(scopes: list[str]) -> None:
    # No raise: a scope list of ordinary names, and no scopes at all, stay valid.
    assert check_scopes(scopes) is None


@pytest.mark.parametrize("scope", PASTED)
def test_guard_refuses_a_credential_shaped_scope(scope: str) -> None:
    with pytest.raises(IdentityInvalid) as exc_info:
        check_scopes([scope])
    # The refusal is scope-related, carries no echo and chains no input.
    assert "scope" in str(exc_info.value)
    assert scope not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.parametrize("scope", PASTED)
def test_guard_refuses_a_credential_shape_anywhere_in_a_mixed_list(scope: str) -> None:
    with pytest.raises(IdentityInvalid) as exc_info:
        check_scopes(["chat:write", scope, "users:read"])
    assert scope not in str(exc_info.value)
    assert exc_info.value.__cause__ is None