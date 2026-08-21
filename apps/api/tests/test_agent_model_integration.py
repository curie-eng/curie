"""Per-agent model selection round-trips through the API against real Postgres.

The `model` column is forwarded as CURIE_MODEL at sandbox boot by the worker
(#254); the API's job is to store, expose, and update it. Create-with-model, the
null default, PATCH-to-set, and PATCH-leaves-unchanged.
"""

from typing import Any


def _create_agent(
    client: Any, auth_headers: dict[str, str], **body: Any
) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "model-bot", "channel": {"kind": "slack", "address": "CMODEL001"}, **body},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def test_agent_defaults_to_null_model(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert agent["model"] is None

    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] is None


def test_create_with_model_persists(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, model="glm-5.2")
    assert agent["model"] == "glm-5.2"

    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "glm-5.2"


def test_patch_sets_model(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"model": "kimi-k2.1"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "kimi-k2.1"


def test_patch_without_model_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, model="deepseek-v4")
    # A binding write must not clear the model. Since ADR-0116 the binding is
    # written through its own subresource, so this is no longer a PATCH field
    # that could be co-cleared by a partial-update bug -- but the response still
    # carries the whole agent, and a handler that rebuilt it from the binding
    # write alone would drop the override just as silently.
    resp = client.patch(
        f"/agents/{agent['id']}/channels",
        params={"kind": "slack", "address": "CMODEL001"},
        json={"kind": "slack", "address": "CMOVED001"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["channels"] == [{"kind": "slack", "address": "CMOVED001"}]
    assert body["model"] == "deepseek-v4"


def test_an_empty_model_is_refused_and_the_error_points_at_null(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #1355: the twin of the thinking case, on the adjacent line. An empty string
    # is NOT an alternate reset -- apply_model_env reads
    # `override if override is not None else config.model`, so "" is not None and
    # wins the ternary, then `if model:` is falsy and CURIE_MODEL is never
    # emitted. The SDK then falls back to its own built-in default instead of the
    # operator's platform model, and on the BYO lane it dials a custom endpoint
    # asking for a built-in Anthropic model id. Refuse it, and say what to send.
    agent = _create_agent(client, auth_headers, model="kimi-k2")
    resp = client.patch(
        f"/agents/{agent['id']}", json={"model": ""}, headers=auth_headers
    )
    assert resp.status_code == 422, resp.text
    assert "null" in resp.text, f"the error must point at the real reset: {resp.text}"

    # And the refusal left the stored value alone.
    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.json()["model"] == "kimi-k2"

    # Create is refused identically -- same field, same nonsense, same answer.
    resp = client.post(
        "/agents",
        json={
            "name": "empty-model",
            "channel": {"kind": "slack", "address": "CMODEL011"},
            "model": "",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text


def test_a_whitespace_only_model_is_refused_on_both_paths(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Whitespace is worse than empty: it passes the falsy check downstream, so it
    # is stored AND forwarded as a garbage model id the harness cannot resolve.
    # The same predicate that catches "" has to catch it.
    resp = client.post(
        "/agents",
        json={
            "name": "ws-model",
            "channel": {"kind": "slack", "address": "CMODEL012"},
            "model": "   ",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text

    agent = _create_agent(client, auth_headers, model="kimi-k2")
    resp = client.patch(
        f"/agents/{agent['id']}", json={"model": "  "}, headers=auth_headers
    )
    assert resp.status_code == 422, resp.text


def test_model_and_thinking_clear_through_the_same_gesture(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Refusal parity no longer lives here: it hardcoded ("model", "thinking")
    # against this one endpoint, which is exactly why #1349 could add a third
    # unguarded override on EvalTriggerRequest without reddening anything.
    # test_nullable_override_parity.py now walks every routed request body
    # reflectively and owns that half (#1389). What stays is the endpoint-level
    # half a schema test cannot see: both fields clear to the platform default
    # through the SAME gesture, one PATCH with explicit JSON null.
    agent = _create_agent(client, auth_headers, model="kimi-k2", thinking="adaptive")
    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"model": None, "thinking": None},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] is None
    assert resp.json()["thinking"] is None


def test_a_padded_override_is_stored_trimmed_on_both_paths(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #1392: the API is the gate every client passes through, so normalization
    # belongs here rather than in each surface. The console trimmed and the CLI
    # did not, so one paste stored two different values; a padded model id is
    # forwarded as CURIE_MODEL and rejected by the provider at the agent's next
    # turn, far from the request that stored it.
    agent = _create_agent(client, auth_headers, model="  kimi-k2  ", thinking="\tadaptive\n")
    assert agent["model"] == "kimi-k2"
    assert agent["thinking"] == "adaptive"

    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"model": " glm-5.2 ", "thinking": " enabled:2000 "},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "glm-5.2"
    assert resp.json()["thinking"] == "enabled:2000"

    # Stored, not merely echoed: a later read returns the normalized value.
    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.json()["model"] == "glm-5.2"
    assert resp.json()["thinking"] == "enabled:2000"


def test_trimming_does_not_soften_the_blank_refusal(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The obvious way to get this wrong: strip first, then test emptiness, which
    # turns a whitespace-only value into "" and stores it -- the exact defect
    # #1355 closed. The refusal must still fire before normalization.
    for blank in ("", "   ", "\t\n"):
        resp = client.post(
            "/agents",
            json={
                "name": f"blank-{len(blank)}x",
                "channel": {"kind": "slack", "address": "CBLANK001"},
                "model": blank,
            },
            headers=auth_headers,
        )
        assert resp.status_code == 422, f"{blank!r} must still be refused: {resp.text}"
