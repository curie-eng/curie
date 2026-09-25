"""The API reads the chart's Slack identities to validate a binding (ADR-0168 decision 1).

Until ADR-0155's `provider_installations` exists (#2909), the declared
identities are exactly what the chart rendered into `CURIE_SLACK_IDENTITIES`.
`get_settings()` is cached, so every case clears the cache after setting the
environment, and the fixture clears it again on the way out so no later test
reads this one's declaration.
"""

import json
import re
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from aci_protocol.slack_identities import IDENTITY_NAME_PATTERN
from curie_api.config import Settings, get_settings
from curie_api.identities import declared_identities, refuse_undeclared
from curie_api.schemas import _CHANNEL_KIND, ChannelBindingWrite
from fastapi.testclient import TestClient
from pydantic import ValidationError

TWO_IDENTITIES = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": "SLACK_SIGNING_SECRET",
        },
        {
            "name": "second",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__0",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__0",
            "signing_secret_env": None,
        },
    ]
)


@pytest.fixture
def declare(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str | None], None]]:
    def _declare(value: str | None) -> None:
        if value is None:
            monkeypatch.delenv("CURIE_SLACK_IDENTITIES", raising=False)
        else:
            monkeypatch.setenv("CURIE_SLACK_IDENTITIES", value)
        get_settings.cache_clear()

    yield _declare
    get_settings.cache_clear()


def test_with_no_declaration_slack_declares_only_the_default_app(
    declare: Callable[[str | None], None],
) -> None:
    declare(None)

    assert get_settings().slack_identities == ()
    assert declared_identities("slack") == frozenset({"default"})


def test_the_declared_names_are_the_slack_identities(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    assert declared_identities("slack") == frozenset({"default", "second"})


def test_a_declared_identity_is_admitted_and_an_undeclared_one_is_refused(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    refuse_undeclared("slack", "second")
    refuse_undeclared("slack", "default")
    with pytest.raises(ValueError, match="'third'.*'default', 'second'"):
        refuse_undeclared("slack", "third")


def test_without_a_declaration_a_second_name_is_still_refused(
    declare: Callable[[str | None], None],
) -> None:
    declare(None)

    with pytest.raises(ValueError, match="'second'.*'default'"):
        refuse_undeclared("slack", "second")


def test_a_binding_write_may_name_a_declared_identity(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    written = ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="second")

    assert written.adapter == "second"


def test_a_binding_write_naming_an_undeclared_identity_is_refused(
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    with pytest.raises(ValidationError, match="'third'"):
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="third")


@pytest.mark.parametrize(
    "name",
    ["sales", "sales-eu", "a1-b2-c3", "sales--eu", "-sales", "sales-", "sales_eu", "Sales", "a"],
)
def test_every_name_the_declaration_admits_is_one_a_binding_can_name(name: str) -> None:
    """The declaration's name rule is the binding schema's `adapter` rule, or
    stricter: a declared name no binding could carry is a dead identity."""

    if re.fullmatch(IDENTITY_NAME_PATTERN, name):
        assert _CHANNEL_KIND.fullmatch(name), name
    if name in {"sales--eu", "-sales", "sales-", "Sales"}:
        assert not re.fullmatch(IDENTITY_NAME_PATTERN, name), name


def test_other_kinds_are_still_not_enumerable(declare: Callable[[str | None], None]) -> None:
    declare(TWO_IDENTITIES)

    assert declared_identities("email") is None


def test_a_malformed_declaration_fails_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """A declaration the chart would never render refuses boot, not a later write."""

    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", json.dumps([{"name": "second"}]))

    with pytest.raises(ValidationError, match="CURIE_SLACK_IDENTITIES"):
        Settings()


# --- a declared name meets the database ----------------------------------------
#
# The write schema admits a declared identity, but 0024's
# `agent_channels_route_pair_ck` still refuses a Slack row naming one with no
# endpoint. These drive real HTTP against the real Postgres, so the refusal is
# the database's own, and pin that it reaches the caller as a named 422 rather
# than a 500.

UNSTORABLE = "cannot be stored until the database admits it"
ISSUE_3146 = "https://github.com/curie-eng/curie/issues/3146"


def _refused_as_unstorable(response: Any) -> None:
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert isinstance(detail, str), detail
    assert UNSTORABLE in detail and ISSUE_3146 in detail, detail


def _create_agent(client: TestClient, headers: dict[str, str], name: str, address: str) -> str:
    created = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": address}},
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def test_adding_a_binding_naming_a_declared_identity_is_a_named_422(
    client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    declare: Callable[[str | None], None],
) -> None:
    agent_id = _create_agent(client, auth_headers, "declared-add", "C0EXAMPLE1")
    declare(TWO_IDENTITIES)

    added = client.post(
        f"/agents/{agent_id}/channels",
        json={"kind": "slack", "address": "C0EXAMPLE2", "adapter": "second"},
        headers=auth_headers,
    )

    _refused_as_unstorable(added)
    fetched = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert [c["address"] for c in fetched.json()["channels"]] == ["C0EXAMPLE1"]


def test_moving_a_binding_onto_a_declared_identity_is_a_named_422(
    client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    declare: Callable[[str | None], None],
) -> None:
    agent_id = _create_agent(client, auth_headers, "declared-move", "C0EXAMPLE1")
    declare(TWO_IDENTITIES)

    moved = client.patch(
        f"/agents/{agent_id}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE1"},
        json={"kind": "slack", "address": "C0EXAMPLE1", "adapter": "second"},
        headers=auth_headers,
    )

    _refused_as_unstorable(moved)
    fetched = client.get(f"/agents/{agent_id}", headers=auth_headers)
    assert fetched.json()["channels"] == [
        {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default"}
    ]


def test_creating_an_agent_bound_to_a_declared_identity_is_a_named_422(
    client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    declare: Callable[[str | None], None],
) -> None:
    declare(TWO_IDENTITIES)

    created = client.post(
        "/agents",
        json={
            "name": "declared-create",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "second"},
        },
        headers=auth_headers,
    )

    _refused_as_unstorable(created)
    listed = client.get("/agents", headers=auth_headers)
    assert "declared-create" not in [agent["name"] for agent in listed.json()]
