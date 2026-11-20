"""The API's operator-named connector ServiceAccounts.

Contract: docs/interfaces/connector-host/INTERFACE.md, "A connector's own
ServiceAccount". The chart renders `connectorServiceAccounts` as JSON into
`CURIE_CONNECTOR_SERVICE_ACCOUNTS`; the API refuses to boot on a list it cannot
render from, rather than failing every connector deploy later.

Pure: each case builds `Settings()` from the env it set, with no fixtures.
"""

from __future__ import annotations

import json

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

_ENV = "CURIE_CONNECTOR_SERVICE_ACCOUNTS"


@pytest.fixture(autouse=True)
def _no_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ENV, raising=False)


def _entry(**overrides: object) -> dict[str, object]:
    return {
        "agent": "acme-bot",
        "connector": "storage",
        "serviceAccountName": "acme-bot-storage",
        **overrides,
    }


def test_unset_names_no_service_account() -> None:
    assert Settings().connector_service_accounts_for("acme-bot") == {}


@pytest.mark.parametrize("value", ["", "  ", "[]"])
def test_an_empty_list_names_no_service_account(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # The chart renders `[]` when the value is unset.
    monkeypatch.setenv(_ENV, value)
    assert Settings().connector_service_accounts_for("acme-bot") == {}


def test_entries_are_returned_for_their_own_agent_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        _ENV,
        json.dumps(
            [
                _entry(),
                _entry(connector="search", serviceAccountName="acme-bot-search"),
                _entry(agent="acme-other", serviceAccountName="acme-other-storage"),
            ]
        ),
    )
    settings = Settings()
    assert settings.connector_service_accounts_for("acme-bot") == {
        "storage": "acme-bot-storage",
        "search": "acme-bot-search",
    }
    assert settings.connector_service_accounts_for("acme-other") == {
        "storage": "acme-other-storage"
    }
    assert settings.connector_service_accounts_for("acme-third") == {}


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("{not json", id="malformed_json"),
        pytest.param(json.dumps({"acme-bot/storage": "acme-bot-storage"}), id="not_a_list"),
        pytest.param(json.dumps(["acme-bot-storage"]), id="entry_not_an_object"),
        pytest.param(
            json.dumps([{"agent": "acme-bot", "connector": "storage"}]), id="missing_key"
        ),
        pytest.param(json.dumps([_entry(namespace="acme-ns")]), id="unknown_key"),
        pytest.param(json.dumps([_entry(agent="")]), id="empty_agent"),
        pytest.param(json.dumps([_entry(connector=" ")]), id="blank_connector"),
        pytest.param(json.dumps([_entry(serviceAccountName="")]), id="empty_account"),
        pytest.param(json.dumps([_entry(serviceAccountName=7)]), id="account_not_a_string"),
        pytest.param(json.dumps([_entry(serviceAccountName="Acme-Bot")]), id="uppercase_account"),
        pytest.param(json.dumps([_entry(serviceAccountName="acme_bot")]), id="underscore_account"),
        pytest.param(json.dumps([_entry(serviceAccountName="acme-")]), id="trailing_dash"),
        pytest.param(json.dumps([_entry(serviceAccountName="a" * 254)]), id="account_too_long"),
        pytest.param(
            json.dumps([_entry(), _entry(serviceAccountName="acme-bot-other")]),
            id="duplicate_pair",
        ),
    ],
)
def test_the_api_refuses_to_boot_on_a_mapping_it_cannot_render(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv(_ENV, value)
    with pytest.raises(ValidationError, match=_ENV):
        Settings()


def test_a_dotted_subdomain_account_name_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    # Kubernetes validates a ServiceAccount name as a DNS-1123 subdomain.
    monkeypatch.setenv(_ENV, json.dumps([_entry(serviceAccountName="acme.bot-storage")]))
    assert Settings().connector_service_accounts_for("acme-bot") == {
        "storage": "acme.bot-storage"
    }
