"""Behavior-packs content is size-capped (NOT YET IMPLEMENTED).

A write whose serialized behavior-packs JSON exceeds `Settings.
behavior_packs_max_bytes` (default 64 KiB) must be rejected with 413, on BOTH
write paths: `PUT /agents/{id}/behavior-packs` and `POST /agents` create (when
the create body includes `behavior_packs`). Mirrors the already-implemented
`state_max_value_bytes` cap (`routers/state.py`'s `_enforce_caps`), which
measures `len(json.dumps(value, separators=(",", ":")).encode("utf-8"))`.

These tests currently FAIL (200/201 instead of 413) until the cap is added.
"""

from typing import Any

from curie_api.config import get_settings


def _create_agent(client: Any, auth_headers: dict[str, str], **body: Any) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "packs-bot", "slack_channel": "C000PACKS1", **body},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _oversized_greeting_reply(default_cap_bytes: int = 64 * 1024) -> dict[str, Any]:
    """A behavior-packs body whose serialized JSON clearly exceeds the default
    64 KiB cap: an oversized `greeting.reply` string is the easiest lever."""
    return {
        "greeting": {
            "enabled": True,
            "phrases": ["hi"],
            "reply": "x" * (default_cap_bytes + 1024),
        }
    }


def test_put_oversized_behavior_packs_rejected_413(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    resp = client.put(
        f"/agents/{agent['id']}/behavior-packs",
        json=_oversized_greeting_reply(),
        headers=auth_headers,
    )
    assert resp.status_code == 413, resp.text


def test_create_agent_with_oversized_behavior_packs_rejected_413(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    resp = client.post(
        "/agents",
        json={
            "name": "packs-bot-oversized",
            "slack_channel": "C000PACKS2",
            "behavior_packs": _oversized_greeting_reply(),
        },
        headers=auth_headers,
    )
    assert resp.status_code == 413, resp.text


def test_put_behavior_packs_within_cap_ok(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    # Non-trivial (a few KB) but comfortably under the 64 KiB default cap, so a
    # correctly-sized cap must not false-positive reject it.
    config = {
        "load": {"enabled": True, "lines": ["Working on it!"] * 20},
        "tips": {"enabled": True, "tips": ["Ask me for the top 5"] * 20},
        "greeting": {
            "enabled": True,
            "phrases": ["hey", "hello"],
            "reply": "hello! " * 200,
        },
        "help": {"enabled": True, "phrases": ["help"], "reply": "here is help"},
        "nav": {"enabled": True, "hub_label": "Help", "hub_command": "help"},
    }
    resp = client.put(f"/agents/{agent['id']}/behavior-packs", json=config, headers=auth_headers)
    assert resp.status_code == 200, resp.text

    resp = client.get(f"/agents/{agent['id']}/behavior-packs", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    got = resp.json()
    assert got["load"]["lines"] == ["Working on it!"] * 20
    assert got["greeting"]["reply"] == "hello! " * 200
    assert got["nav"]["hub_command"] == "help"


def test_put_behavior_packs_boundary_of_small_overridden_cap(
    client: Any, auth_headers: dict[str, str], clean_db: None, monkeypatch: Any
) -> None:
    """Override the cap to a small value so the test does not depend on the
    64 KiB default magnitude: a body over the small cap is 413, a body under
    it is accepted. `get_settings` is `lru_cache`-d, so the cache must be
    cleared after the env var change (and restored on teardown) or the
    override never takes effect / leaks into other tests.
    """
    monkeypatch.setenv("BEHAVIOR_PACKS_MAX_BYTES", "1000")
    get_settings.cache_clear()
    try:
        agent = _create_agent(client, auth_headers)

        # Comfortably under the 1000-byte cap (serialized total ~870 bytes).
        under = {"greeting": {"enabled": True, "phrases": [], "reply": "a" * 600}}
        resp = client.put(f"/agents/{agent['id']}/behavior-packs", json=under, headers=auth_headers)
        assert resp.status_code == 200, resp.text

        # Comfortably over the 1000-byte cap (serialized total ~1170 bytes).
        over = {"greeting": {"enabled": True, "phrases": [], "reply": "a" * 900}}
        resp = client.put(f"/agents/{agent['id']}/behavior-packs", json=over, headers=auth_headers)
        assert resp.status_code == 413, resp.text
    finally:
        get_settings.cache_clear()
