"""Protected history stays separate from ordinary state credentials.

@spec PROTECTED-HOOK-LANE-7. Real API and Postgres; no backend mocks. The
existing legacy compatibility window applies only to ordinary transcripts.
"""

from __future__ import annotations

from typing import Any

import pytest
from channel_protocol import scoped_conversation_id
from curie_api.config import get_settings
from curie_internal.sandbox_token import mint
from test_state_transcript_scope import (
    _FAR_FUTURE,
    BINDING_A,
    CHANNEL_A,
    HISTORY,
    SECRET,
    _agent_with_two_channels,
    _binding_url,
    _legacy,
    _reader,
    _seed,
    _slack_key,
    _thread_rows,
    _url,
)

DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
PROTECTED_BINDING = "@protected:" + DIGEST_A
EXECUTION_KEY = "protected:33333333-3333-4333-8333-333333333333:1:" + "c" * 64
PROTECTED_KEY = scoped_conversation_id("@protected", DIGEST_A, EXECUTION_KEY)


def protected_token(aid: str, digest: str = DIGEST_A) -> dict[str, str]:
    """Explicit modern loader credential, @spec PROTECTED-HOOK-LANE-7."""
    return _reader(aid, "@protected:" + digest)


def app_token(aid: str) -> dict[str, str]:
    """The bundle token cannot reach reserved history, @spec PROTECTED-HOOK-LANE-7."""
    return {
        "X-API-Key": mint(get_settings().api_key, agent=aid, scope="state.app", exp=_FAR_FUTURE)
    }


def perform(client: Any, url: str, headers: dict[str, str], verb: str):
    """Each history verb uses its actual API contract, @spec PROTECTED-HOOK-LANE-7."""
    if verb == "get":
        return client.get(url, headers=headers)
    if verb == "put":
        return client.put(url, json={"value": HISTORY}, headers=headers)
    if verb == "append":
        return client.post(url + "/append", json={"item": {"role": "assistant"}}, headers=headers)
    return client.delete(url, headers=headers)


def test_protected_loader_and_platform_keep_complete_history_cycle(
    client: Any, auth_headers: dict[str, str], clean_db: None
):
    """Scoped protected history remains usable, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    url = _url(aid, PROTECTED_KEY)
    for headers in (protected_token(aid), auth_headers):
        for verb in ("put", "get", "append", "delete"):
            response = perform(client, url, headers, verb)
            assert response.status_code in (200, 204), (verb, response.status_code, response.text)
        assert client.get(url, headers=auth_headers).status_code == 404


@pytest.mark.parametrize("principal", ["ordinary", "unbound", "legacy", "app", "other-protected"])
@pytest.mark.parametrize("verb", ["get", "put", "append", "delete"])
def test_other_state_principals_cannot_read_or_modify_protected_history(
    client: Any, auth_headers: dict[str, str], clean_db: None, principal: str, verb: str
):
    """All verbs refuse before touching the protected record, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    url = _url(aid, PROTECTED_KEY)
    _seed(client, auth_headers, url, SECRET)
    headers = {
        "ordinary": _reader(aid, BINDING_A),
        "unbound": _reader(aid, None),
        "legacy": _legacy(aid),
        "app": app_token(aid),
        "other-protected": protected_token(aid, DIGEST_B),
    }[principal]
    refused = perform(client, url, headers, verb)
    assert refused.status_code == 403, (principal, verb, refused.status_code, refused.text)
    assert "a DM secret" not in refused.text
    stored = client.get(url, headers=auth_headers)
    assert stored.status_code == 200
    assert stored.json()["value"] == SECRET


@pytest.mark.parametrize("verb", ["get", "put", "append", "delete"])
def test_protected_loader_cannot_reach_ordinary_history(
    client: Any, auth_headers: dict[str, str], clean_db: None, verb: str
):
    """History isolation is symmetric, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    url = _url(aid, _slack_key(CHANNEL_A))
    _seed(client, auth_headers, url, SECRET)
    response = perform(client, url, protected_token(aid), verb)
    assert response.status_code == 403, response.text
    assert client.get(url, headers=auth_headers).json()["value"] == SECRET


@pytest.mark.parametrize(
    "key",
    [
        # Decodes to the protected kind but is not a canonical scoped key.
        f"@protected:{DIGEST_A}:conversation",
        f"%40p%72otected:{DIGEST_A}:conversation",
        f"%40protected:{DIGEST_A}",
        f"%40protected:{DIGEST_A}:conversation:extra:extra",
        f"%40protected%3A{DIGEST_A}%3Aconversation",
    ],
)
@pytest.mark.parametrize("verb", ["get", "put", "append", "delete"])
def test_malformed_protected_shaped_keys_cannot_enter_legacy_compatibility_path(
    client: Any, auth_headers: dict[str, str], clean_db: None, key: str, verb: str
):
    """Structural parse failure must not reopen legacy access, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    url = _url(aid, key)
    _seed(client, auth_headers, url, SECRET)
    refused = perform(client, url, _legacy(aid), verb)
    assert refused.status_code == 403, (key, verb, refused.status_code, refused.text)
    assert client.get(url, headers=auth_headers).json()["value"] == SECRET


def test_legacy_listing_hides_protected_keys_without_changing_ordinary_compatibility(
    client: Any, auth_headers: dict[str, str], clean_db: None
):
    """Legacy lists ordinary history only, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    ordinary = {_slack_key(CHANNEL_A), "old-unscoped-history"}
    protected = {PROTECTED_KEY, f"%40p%72otected:{DIGEST_A}:conversation"}
    for key in ordinary | protected:
        _seed(client, auth_headers, _url(aid, key), HISTORY)
    listing_url = f"/agents/{aid}/state/transcript"
    legacy = client.get(listing_url, headers=_legacy(aid))
    assert legacy.status_code == 200
    assert {row["key"] for row in legacy.json()} == ordinary
    own = client.get(listing_url, headers=protected_token(aid))
    assert own.status_code == 200
    assert {row["key"] for row in own.json()} == {PROTECTED_KEY}
    platform = client.get(listing_url, headers=auth_headers)
    assert {row["key"] for row in platform.json()} == ordinary | protected
    for key in ordinary:
        response = client.get(_url(aid, key), headers=_legacy(aid))
        assert response.status_code == 200


@pytest.mark.parametrize("principal", ["ordinary", "legacy", "protected"])
@pytest.mark.parametrize("verb", ["get", "put", "append", "delete"])
def test_ordinary_binding_path_cannot_launder_a_protected_transcript_key(
    client: Any, auth_headers: dict[str, str], clean_db: None, principal: str, verb: str
):
    """Path binding and protected key must both enforce isolation, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    url = _binding_url(aid, "slack", CHANNEL_A, PROTECTED_KEY)
    headers = {"ordinary": _reader(aid), "legacy": _legacy(aid), "protected": protected_token(aid)}[
        principal
    ]
    response = perform(client, url, headers, verb)
    assert response.status_code == 403, (principal, verb, response.status_code, response.text)
    assert _thread_rows(aid, PROTECTED_KEY) == 0


def test_denied_legacy_read_cannot_adopt_a_protected_pre_identity_row(
    client: Any, auth_headers: dict[str, str], clean_db: None
):
    """No unauthorized read performs pre-identity adoption, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    old = scoped_conversation_id("@protected", DIGEST_A, EXECUTION_KEY)
    new = scoped_conversation_id("@protected", DIGEST_A, EXECUTION_KEY, identity="protected-worker")
    _seed(client, auth_headers, _url(aid, old), SECRET)
    denied = client.get(_url(aid, new), headers=_legacy(aid))
    assert denied.status_code == 403, denied.text
    assert _thread_rows(aid, new) == 0
    assert client.get(_url(aid, old), headers=auth_headers).json()["value"] == SECRET


def test_legacy_binding_listing_cannot_launder_protected_history(
    client: Any, auth_headers: dict[str, str], clean_db: None
):
    """Binding lists preserve only ordinary legacy reach, @spec PROTECTED-HOOK-LANE-7."""
    aid = _agent_with_two_channels(client, auth_headers)
    ordinary = _slack_key(CHANNEL_A)
    for key in (ordinary, PROTECTED_KEY):
        _seed(client, auth_headers, _binding_url(aid, "slack", CHANNEL_A, key), HISTORY)
    url = f"/agents/{aid}/state/bindings/slack/{CHANNEL_A}/transcript"
    response = client.get(url, headers=_legacy(aid))
    assert response.status_code == 200
    assert {row["key"] for row in response.json()} == {ordinary}
    platform = client.get(url, headers=auth_headers)
    assert {row["key"] for row in platform.json()} == {ordinary, PROTECTED_KEY}
