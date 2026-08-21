"""Per-agent thinking depth round-trips through the API against real Postgres.

The sibling of `test_agent_model_integration`, deliberately: `thinking` is the
per-agent half of the two-layer operator control ADR-0098 defines, and it is
stored, exposed and updated exactly the way `model` is (#1182). Create-with,
the null default, PATCH-to-set, and PATCH-leaves-unchanged.

The null default carries the meaning here: NULL is not "thinking off", it is
"this agent expresses no opinion", which falls through to the worker's
`CURIE_THINKING` and, if that is unset too, to sending the runner nothing at all.
"""

from typing import Any


def _create_agent(client: Any, auth_headers: dict[str, str], **body: Any) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": "thinking-bot", "channel": {"kind": "slack", "address": "CTHINK001"}, **body},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def test_agent_defaults_to_null_thinking(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers)
    assert agent["thinking"] is None

    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] is None


def test_create_with_thinking_persists(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, thinking="disabled")
    assert agent["thinking"] == "disabled"

    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] == "disabled"


def test_patch_sets_thinking(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    agent = _create_agent(client, auth_headers)
    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"thinking": "enabled:2000"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] == "enabled:2000"


def test_patch_without_thinking_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Omitted means "unchanged", the same convention `model` follows -- a write
    # that moves an agent's binding must not silently clear its thinking depth.
    # The binding write is the channels subresource since ADR-0116; the subject
    # of this test is the untouched override beside it, which is why the
    # assertion is on `thinking` and not on the binding.
    agent = _create_agent(client, auth_headers, thinking="adaptive")
    resp = client.patch(
        f"/agents/{agent['id']}/channels",
        params={"kind": "slack", "address": "CTHINK001"},
        json={"kind": "slack", "address": "CTHINK002"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] == "adaptive"
    assert resp.json()["channels"] == [{"kind": "slack", "address": "CTHINK002"}]


def test_the_api_stores_the_value_verbatim_and_does_not_validate_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Deliberate: the vocabulary belongs to the runner (`curie_runner.thinking`),
    # not to the persistence layer, so that swapping the harness is not a schema
    # change. The consequence is that a typo is caught at sandbox boot with a
    # message naming the vocabulary, not at write time -- which is the same
    # bargain `model` already makes (the API does not know which model ids a
    # provider accepts either).
    agent = _create_agent(client, auth_headers, thinking="not-a-real-value")
    assert agent["thinking"] == "not-a-real-value"


# --- clearing the override back to the platform default (#1310) ---------------
# The distinction these pin is not cosmetic: before this, setting `thinking` was
# a one-way door. PATCH with null "succeeded" and changed nothing, so an operator
# who pinned an agent to `disabled` had no way, through the API, to put it back
# on the platform default.


def test_patch_with_explicit_null_clears_the_override(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, thinking="disabled")
    assert agent["thinking"] == "disabled"

    resp = client.patch(
        f"/agents/{agent['id']}",
        json={"thinking": None},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] is None, "explicit null must clear the override"

    # And it is durable, not just echoed back by the write response.
    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] is None


def test_omitting_thinking_still_leaves_it_untouched(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The other half of the distinction. If this regressed, every write that
    # renamed a channel would silently wipe the agent's thinking depth. The
    # binding write is the channels subresource since ADR-0116, and it returns
    # the whole agent, so the override still has to survive the response it
    # rebuilds.
    agent = _create_agent(client, auth_headers, thinking="adaptive")
    resp = client.patch(
        f"/agents/{agent['id']}/channels",
        params={"kind": "slack", "address": "CTHINK001"},
        json={"kind": "slack", "address": "CTHINK009"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["thinking"] == "adaptive"


def test_the_model_override_clears_the_same_way(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # `model` is the same seam with the same defect; fixing thinking beside it
    # and leaving this broken on an adjacent line is the drift the repo's
    # parity-seam rule exists to stop.
    agent = _create_agent(client, auth_headers, model="glm-5.2")
    assert agent["model"] == "glm-5.2"

    resp = client.patch(
        f"/agents/{agent['id']}", json={"model": None}, headers=auth_headers
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] is None

    # Omission still leaves it alone.
    agent2 = _create_agent(
        client,
        auth_headers,
        name="model-untouched",
        channel={"kind": "slack", "address": "CTHINK010"},
        model="kimi-k2.1",
    )
    resp = client.patch(
        f"/agents/{agent2['id']}", json={"thinking": "adaptive"}, headers=auth_headers
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "kimi-k2.1"


def test_an_empty_thinking_is_refused_and_the_error_points_at_null(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # An empty string is NOT an alternate reset: it reaches the worker falsy, so
    # no boot key is emitted at all and the MODEL's default wins -- silently
    # skipping the platform default the operator configured. Refuse it, and say
    # what to send instead.
    agent = _create_agent(client, auth_headers, thinking="adaptive")
    resp = client.patch(
        f"/agents/{agent['id']}", json={"thinking": ""}, headers=auth_headers
    )
    assert resp.status_code == 422, resp.text
    assert "null" in resp.text, f"the error must point at the real reset: {resp.text}"

    # And the refusal left the stored value alone.
    resp = client.get(f"/agents/{agent['id']}", headers=auth_headers)
    assert resp.json()["thinking"] == "adaptive"

    # Create is refused identically -- same field, same nonsense, same answer.
    resp = client.post(
        "/agents",
        json={
            "name": "empty-thinking",
            "channel": {"kind": "slack", "address": "CTHINK011"},
            "thinking": "   ",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
