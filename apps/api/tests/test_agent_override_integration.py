"""Per-agent operator override round-trips through the API against real Postgres.

Consolidates the three per-field override suites: `model` (#254, forwarded as
CURIE_MODEL at sandbox boot), `thinking` (ADR-0098's per-agent half of the
two-layer operator control, #1182), and `execution_deadline_seconds` (#3071).
All nullable overrides share the same three-way PATCH contract: omitted leaves the field
unchanged, an explicit JSON null clears it back to the platform default, and a
value sets it. `model` and `thinking` additionally share a blank-string refusal
contract (#1355, #1392): an empty or whitespace-only string is refused with a
422 pointing the caller at the real reset, both on create and on PATCH, and a
padded value is stored trimmed rather than accepted verbatim.

Schema-level reflection across every routed request body that carries a
nullable override lives in `test_nullable_override_parity.py` (#1389); it
walks the live app and asserts blank-refusal on every such field without a
client or a database. This file is the endpoint-level layer that reflection
cannot see: live HTTP calls against real Postgres, the interaction between an
override and the `/agents/{id}/channels` binding write (ADR-0118), and
durability of a write across a subsequent GET.
"""

from typing import Any

import pytest


def _create_agent(
    client: Any, auth_headers: dict[str, str], *, name: str, address: str, **body: Any
) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": address}, **body},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


def _patch(client: Any, auth_headers: dict[str, str], agent_id: str, body: dict[str, Any]) -> Any:
    return client.patch(f"/agents/{agent_id}", json=body, headers=auth_headers)


def _read(client: Any, auth_headers: dict[str, str], agent_id: str) -> dict[str, Any]:
    resp = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    return resp.json()  # type: ignore[no-any-return]


# --- shared shapes: model and thinking are the same nullable-string override ---


@pytest.mark.parametrize("field", ["model", "reviewer_model", "thinking"])
def test_agent_defaults_to_null(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str
) -> None:
    agent = _create_agent(client, auth_headers, name=f"{field}-bot", address=f"C{field.upper()}DEF")
    assert agent[field] is None
    assert _read(client, auth_headers, agent["id"])[field] is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("model", "glm-5.2", id="model"),
        pytest.param("reviewer_model", "claude-opus-5-5", id="reviewer_model"),
        pytest.param("thinking", "disabled", id="thinking"),
    ],
)
def test_create_with_value_persists(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str, value: str
) -> None:
    agent = _create_agent(
        client, auth_headers, name=f"{field}-bot", address=f"C{field.upper()}PER", **{field: value}
    )
    assert agent[field] == value
    assert _read(client, auth_headers, agent["id"])[field] == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("model", "kimi-k2.1", id="model"),
        pytest.param("reviewer_model", "claude-opus-5-5", id="reviewer_model"),
        pytest.param("thinking", "enabled:2000", id="thinking"),
    ],
)
def test_patch_sets_value(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str, value: str
) -> None:
    agent = _create_agent(client, auth_headers, name=f"{field}-bot", address=f"C{field.upper()}SET")
    resp = _patch(client, auth_headers, agent["id"], {field: value})
    assert resp.status_code == 200, resp.text
    assert resp.json()[field] == value


@pytest.mark.parametrize(
    ("field", "seed"),
    [
        pytest.param("model", "deepseek-v4", id="model"),
        pytest.param("reviewer_model", "claude-opus-5-5", id="reviewer_model"),
        pytest.param("thinking", "adaptive", id="thinking"),
    ],
)
def test_patch_without_field_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str, seed: str
) -> None:
    # A binding write must not clear the override. Since ADR-0118 the binding is
    # written through its own subresource, so this is no longer a PATCH field that
    # could be co-cleared by a partial-update bug -- but the response still carries
    # the whole agent, and a handler that rebuilt it from the binding write alone
    # would drop the override just as silently.
    address = f"C{field.upper()}CHN1"
    moved = f"C{field.upper()}CHN2"
    agent = _create_agent(
        client, auth_headers, name=f"{field}-bot", address=address, **{field: seed}
    )
    resp = client.patch(
        f"/agents/{agent['id']}/channels",
        params={"kind": "slack", "address": address},
        json={"kind": "slack", "address": moved},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["channels"] == [
        {
            "kind": "slack",
            "address": moved,
            "adapter": "default",
            "allowed_callers": None,
        }
    ]
    assert body[field] == seed


@pytest.mark.parametrize(
    ("field", "seed"),
    [
        pytest.param("model", "kimi-k2", id="model"),
        pytest.param("reviewer_model", "claude-opus-5-5", id="reviewer_model"),
        pytest.param("thinking", "adaptive", id="thinking"),
    ],
)
def test_an_empty_value_is_refused_and_the_error_points_at_null(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str, seed: str
) -> None:
    # #1355: an empty string is NOT an alternate reset. Downstream code reads
    # `override if override is not None else default`, so "" is not None and wins
    # the ternary, then a truthiness check on the result is falsy and the override
    # is silently skipped rather than applied or cleared. Refuse it, and say what
    # to send instead.
    agent = _create_agent(
        client, auth_headers, name=f"{field}-bot", address=f"C{field.upper()}EMP1", **{field: seed}
    )
    resp = _patch(client, auth_headers, agent["id"], {field: ""})
    assert resp.status_code == 422, resp.text
    assert "null" in resp.text, f"the error must point at the real reset: {resp.text}"

    # And the refusal left the stored value alone.
    assert _read(client, auth_headers, agent["id"])[field] == seed

    # Create is refused identically -- same field, same nonsense, same answer.
    resp = client.post(
        "/agents",
        json={
            "name": f"empty-{field}",
            "channel": {"kind": "slack", "address": f"C{field.upper()}EMP2"},
            field: "",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize("field", ["model", "reviewer_model", "thinking"])
def test_a_whitespace_only_value_is_refused_on_both_paths(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str
) -> None:
    # Whitespace is worse than empty: it passes a falsy check downstream, so it is
    # stored AND forwarded as a garbage value the harness cannot resolve. The same
    # predicate that catches "" has to catch it.
    resp = client.post(
        "/agents",
        json={
            "name": f"ws-{field}",
            "channel": {"kind": "slack", "address": f"C{field.upper()}WS1"},
            field: "   ",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text

    agent = _create_agent(
        client,
        auth_headers,
        name=f"{field}-bot",
        address=f"C{field.upper()}WS2",
        **{field: "kimi-k2"},
    )
    resp = _patch(client, auth_headers, agent["id"], {field: "  "})
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    ("field", "seed"),
    [
        pytest.param("model", "kimi-k2", id="model"),
        pytest.param("reviewer_model", "claude-opus-5-5", id="reviewer_model"),
        pytest.param("thinking", "disabled", id="thinking"),
    ],
)
def test_patch_with_explicit_null_clears_the_override(
    client: Any, auth_headers: dict[str, str], clean_db: None, field: str, seed: str
) -> None:
    # #1310: before this, setting the override was a one-way door. PATCH with null
    # "succeeded" and changed nothing, so an operator who pinned an agent had no
    # way, through the API, to put it back on the platform default.
    agent = _create_agent(
        client, auth_headers, name=f"{field}-bot", address=f"C{field.upper()}CLR", **{field: seed}
    )
    assert agent[field] == seed

    resp = _patch(client, auth_headers, agent["id"], {field: None})
    assert resp.status_code == 200, resp.text
    assert resp.json()[field] is None, "explicit null must clear the override"

    # And it is durable, not just echoed back by the write response.
    assert _read(client, auth_headers, agent["id"])[field] is None


# --- cross-field tests: genuinely different shapes, kept dedicated ------------


def test_model_and_thinking_clear_through_the_same_gesture(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Refusal parity does not live here: test_nullable_override_parity.py walks
    # every routed request body reflectively and owns that half (#1389). What
    # stays is the endpoint-level half a schema test cannot see: both fields
    # clear to the platform default through the SAME gesture, one PATCH with
    # explicit JSON null.
    agent = _create_agent(
        client,
        auth_headers,
        name="both-bot",
        address="CBOTHCLR1",
        model="kimi-k2",
        thinking="adaptive",
    )
    resp = _patch(client, auth_headers, agent["id"], {"model": None, "thinking": None})
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] is None
    assert resp.json()["thinking"] is None


def test_patching_thinking_leaves_model_untouched_when_omitted(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # `model` and `thinking` are independent overrides carried by the same PATCH
    # body; a handler that rebuilds the whole agent from one field must not drop
    # the other one simply because this write did not mention it.
    agent = _create_agent(
        client, auth_headers, name="model-untouched", address="COMIT0001", model="kimi-k2.1"
    )
    resp = _patch(client, auth_headers, agent["id"], {"thinking": "adaptive"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "kimi-k2.1"


def test_a_padded_override_is_stored_trimmed_on_both_paths(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # #1392: the API is the gate every client passes through, so normalization
    # belongs here rather than in each surface. The console trimmed and the CLI
    # did not, so one paste stored two different values; a padded model id is
    # forwarded as CURIE_MODEL and rejected by the provider at the agent's next
    # turn, far from the request that stored it.
    agent = _create_agent(
        client,
        auth_headers,
        name="padded-bot",
        address="CPADDED01",
        model="  kimi-k2  ",
        thinking="\tadaptive\n",
    )
    assert agent["model"] == "kimi-k2"
    assert agent["thinking"] == "adaptive"

    resp = _patch(
        client, auth_headers, agent["id"], {"model": " glm-5.2 ", "thinking": " enabled:2000 "}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "glm-5.2"
    assert resp.json()["thinking"] == "enabled:2000"

    # Stored, not merely echoed: a later read returns the normalized value.
    read = _read(client, auth_headers, agent["id"])
    assert read["model"] == "glm-5.2"
    assert read["thinking"] == "enabled:2000"


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


def test_the_api_stores_thinking_verbatim_and_does_not_validate_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Deliberate: the vocabulary belongs to the runner (`curie_runner.thinking`),
    # not to the persistence layer, so that swapping the harness is not a schema
    # change. The consequence is that a typo is caught at sandbox boot with a
    # message naming the vocabulary, not at write time -- the same bargain
    # `model` already makes, since the API does not know which model ids a
    # provider accepts either.
    agent = _create_agent(
        client, auth_headers, name="thinking-bot", address="CVERBATIM", thinking="not-a-real-value"
    )
    assert agent["thinking"] == "not-a-real-value"


# --- execution_deadline_seconds: an integer override with bounds, not a ------
# --- blank-refusal string (#3071) --------------------------------------------


def _create_deadline_agent(client: Any, auth_headers: dict[str, str]) -> dict[str, Any]:
    return _create_agent(client, auth_headers, name="deadline-bot", address="CDEAD0001")


def test_agent_defaults_to_null_execution_deadline(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_deadline_agent(client, auth_headers)
    assert agent["execution_deadline_seconds"] is None
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] is None


def test_patch_sets_and_explicit_null_clears_execution_deadline(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_deadline_agent(client, auth_headers)
    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": 90})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] == 90
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] == 90

    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": None})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] is None
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] is None


def test_patch_omitting_execution_deadline_leaves_it_unchanged(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_deadline_agent(client, auth_headers)
    assert (
        _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": 90}).status_code
        == 200
    )
    resp = _patch(client, auth_headers, agent["id"], {"thinking": "adaptive"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] == 90
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] == 90


@pytest.mark.parametrize("value", [60, 10800], ids=["lower-bound", "upper-bound"])
def test_patch_accepts_the_bounds(
    client: Any, auth_headers: dict[str, str], clean_db: None, value: int
) -> None:
    agent = _create_deadline_agent(client, auth_headers)
    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": value})
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_deadline_seconds"] == value


@pytest.mark.parametrize(
    "value",
    [59, 10801, 0, -1],
    ids=["below-lower-bound", "above-upper-bound", "zero", "negative"],
)
def test_patch_rejects_out_of_range_and_keeps_the_stored_value(
    client: Any, auth_headers: dict[str, str], clean_db: None, value: int
) -> None:
    agent = _create_deadline_agent(client, auth_headers)
    assert (
        _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": 90}).status_code
        == 200
    )
    resp = _patch(client, auth_headers, agent["id"], {"execution_deadline_seconds": value})
    assert resp.status_code == 422, resp.text
    assert _read(client, auth_headers, agent["id"])["execution_deadline_seconds"] == 90



def test_reviewer_model_omission_and_clear_preserve_the_implementer_override(
    client: Any, auth_headers: dict[str, str], clean_db: None,
) -> None:
    agent = _create_agent(
        client, auth_headers, name="acme-reviewer-override", address="C0EXAMPLE1",
        model="acme-implementer-model", reviewer_model="acme-reviewer-model",
    )
    renamed = _patch(client, auth_headers, agent["id"], {"name": "acme-renamed"})
    assert renamed.status_code == 200, renamed.text
    assert _read(client, auth_headers, agent["id"])["reviewer_model"] == "acme-reviewer-model"
    cleared = _patch(client, auth_headers, agent["id"], {"reviewer_model": None})
    assert cleared.status_code == 200, cleared.text
    stored = _read(client, auth_headers, agent["id"])
    assert stored["reviewer_model"] is None
    assert stored["model"] == "acme-implementer-model"
