"""Registered Slack sender derivation for Enterprise Grid workspace receivers.

@spec HTTP-INVOCATION-EXECUTION-21.
Every organisation, workspace, user and channel id here is synthetic. No real
Grid delivery or auth.test answer was observed; live Grid qualification is a
Discovery waiver on #4890.
"""

from __future__ import annotations

import importlib
import uuid
from typing import Any

import pytest
from curie_api.identity.slack import AUTH_TEST_KEY, SlackEvidence, derive

ORG = "E0EXAMPLEORG"
OTHER_ORG = "E0EXAMPLEPARTNER"
WORKSPACE = "T0EXAMPLEHOME"
SIBLING = "T0EXAMPLESIBLING"
USER = "W0EXAMPLEPERSON"
GRID = {"namespace_kind": "slack_enterprise", "namespace_authority": "", "namespace_key": ORG}


def owner() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-21: missing owner fails in the test body."""
    module = importlib.import_module("curie_api.identity.slack")
    assert hasattr(module, "derive_registered_sender"), "registered sender owner is missing"
    return module.derive_registered_sender


def mention(**changes: Any) -> SlackEvidence:
    """@spec HTTP-INVOCATION-EXECUTION-21: a synthetic Grid app_mention."""
    return SlackEvidence.model_validate({
        "delivery": "app_mention", "user_id": USER, "event_user_team": None,
        "event_team": WORKSPACE, "interaction_user_team_id": None,
        "is_ext_shared_channel": False, "enterprise_ids": [ORG], **changes,
    })


def answer(delivery: str = "block_actions", **changes: Any) -> SlackEvidence:
    """@spec HTTP-INVOCATION-EXECUTION-21: a synthetic Grid interaction."""
    return SlackEvidence.model_validate({
        "delivery": delivery, "user_id": USER, "interaction_user_team_id": WORKSPACE,
        "enterprise_ids": [ORG], **changes,
    })


def grid(evidence: SlackEvidence, **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-21."""
    return owner()(evidence, **{**GRID, "installation_account": WORKSPACE, **changes})


ORDINARY_CASES = [
    {"delivery": "app_mention", "user_id": "U0EXAMPLEONE", "event_team": "T0EXAMPLEONE",
     "is_ext_shared_channel": False},
    {"delivery": "app_mention", "user_id": "U0EXAMPLEONE", "event_team": "T0EXAMPLEOTHER",
     "is_ext_shared_channel": False},
    {"delivery": "app_mention", "user_id": "U0EXAMPLEONE", "event_team": "T0EXAMPLEONE",
     "is_ext_shared_channel": True},
    {"delivery": "app_mention", "user_id": "U0EXAMPLEONE", "event_team": "T0EXAMPLEONE"},
    {"delivery": "app_mention", "user_id": "U0EXAMPLEONE", "event_user_team": "T0EXAMPLEONE",
     "event_team": "T0EXAMPLEOTHER"},
    {"delivery": "app_mention", "user_id": "U0EXAMPLEONE", "event_team": "T0EXAMPLEONE",
     "is_ext_shared_channel": False, "enterprise_ids": [ORG]},
    {"delivery": "message", "user_id": "U0EXAMPLEONE", "event_team": "T0EXAMPLEONE",
     "is_ext_shared_channel": False},
    {"delivery": "block_actions", "user_id": "U0EXAMPLEONE",
     "interaction_user_team_id": "T0EXAMPLEONE"},
    {"delivery": "view_submission", "user_id": "U0EXAMPLEONE"},
    {"delivery": "app_mention", "user_id": " ", "event_team": "T0EXAMPLEONE",
     "is_ext_shared_channel": False},
]


@pytest.mark.parametrize("fields", ORDINARY_CASES)
def test_ordinary_registration_returns_exactly_the_existing_derivation(
    fields: dict[str, Any],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: non-Grid behaviour is unchanged."""
    evidence = SlackEvidence.model_validate(fields)
    expected = derive(
        evidence,
        identity_attributes={AUTH_TEST_KEY: {
            "team_id": "T0EXAMPLEONE", "enterprise_id": None, "is_enterprise_install": False,
        }},
        installation_authority="",
        installation_account="T0EXAMPLEONE",
    )
    assert owner()(
        evidence, namespace_kind="slack_workspace", namespace_authority="",
        namespace_key="T0EXAMPLEONE", installation_account="T0EXAMPLEONE",
    ) == expected


def test_existing_derivation_still_refuses_every_grid_signal() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: local resolution keeps refusing Grid."""
    for evidence in (mention(), answer()):
        assert derive(
            evidence,
            identity_attributes={AUTH_TEST_KEY: {
                "team_id": WORKSPACE, "enterprise_id": ORG, "is_enterprise_install": False,
            }},
            installation_authority="",
            installation_account=WORKSPACE,
        ) == "subject_unidentified"


@pytest.mark.parametrize("team_fields", [
    {"event_team": WORKSPACE},
    {"event_user_team": SIBLING, "event_team": WORKSPACE},
    {"event_user_team": ORG, "event_team": None},
    {"event_user_team": None, "event_team": None},
])
def test_grid_mention_resolves_in_the_organisation_from_any_member_workspace(
    team_fields: dict[str, Any],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: team fields are neither namespace nor check."""
    assert grid(mention(**team_fields)) == (ORG, USER)


def test_grid_answer_resolves_from_the_registered_workspace() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21."""
    assert grid(answer("block_actions")) == (ORG, USER)
    assert grid(answer("view_submission")) == (ORG, USER)


@pytest.mark.parametrize(("evidence", "reason"), [
    (mention(is_ext_shared_channel=True), "subject_unidentified"),
    (mention(is_ext_shared_channel=None), "subject_unidentified"),
    (mention(enterprise_ids=[]), "subject_unidentified"),
    (mention(enterprise_ids=["is_enterprise_install"]), "subject_unidentified"),
    (mention(enterprise_ids=[ORG, "is_enterprise_install"]), "subject_unidentified"),
    (mention(enterprise_ids=[OTHER_ORG]), "namespace_conflict"),
    (mention(enterprise_ids=[ORG, OTHER_ORG]), "namespace_conflict"),
    (mention(user_id=None), "subject_unidentified"),
    (mention(user_id=""), "subject_unidentified"),
    (mention(user_id="  "), "subject_unidentified"),
    (mention(delivery="message"), "subject_unidentified"),
    (answer(interaction_user_team_id=None), "subject_unidentified"),
    (answer(interaction_user_team_id=SIBLING), "namespace_conflict"),
    (answer(enterprise_ids=[]), "subject_unidentified"),
    (answer(enterprise_ids=[OTHER_ORG]), "namespace_conflict"),
    (answer(enterprise_ids=["is_enterprise_install"]), "subject_unidentified"),
])
def test_grid_refusals_fail_closed_with_named_reasons(evidence: SlackEvidence, reason: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21: outside organisations and gaps never resolve."""
    assert grid(evidence) == reason


@pytest.mark.parametrize("changes", [
    {"namespace_authority": "https://slack.example.test"},
    {"namespace_key": WORKSPACE},
    {"namespace_kind": "slack_organisation"},
    {"namespace_kind": "email_domain"},
    {"namespace_kind": "slack_workspace"},
])
def test_unsupported_registration_shapes_never_resolve(changes: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21."""
    assert grid(mention(), **changes) == "subject_unidentified"
    assert grid(answer(), **changes) == "subject_unidentified"


def _registration(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-15/21: operator declaration, synthetic ids."""
    return {
        "schema_version": 1, "receiver_id": str(uuid.uuid4()), "generation": 1,
        "tenant_id": str(uuid.uuid4()), "channel_identity_id": str(uuid.uuid4()),
        "channel_identity_name": "synthetic-grid-receiver",
        "provider_installation_id": str(uuid.uuid4()),
        "namespace": {"id": str(uuid.uuid4()), "authority": "", "kind": "slack_enterprise",
                      "key": ORG},
        "installation_account": WORKSPACE, "bot_user_id": "U0EXAMPLEBOT",
        "binding_ids": (str(uuid.uuid4()),), "human_native_ids": (USER,), **changes,
    }


def test_receiver_registration_accepts_only_the_grid_workspace_shape() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/21."""
    from curie_api.identity.http_invocation import ReceiverRegistrationV1

    accepted = ReceiverRegistrationV1.model_validate(_registration())
    assert accepted.namespace.kind == "slack_enterprise" and accepted.namespace.key == ORG
    ordinary = _registration(namespace={
        "id": str(uuid.uuid4()), "authority": "", "kind": "slack_workspace", "key": WORKSPACE,
    })
    assert ReceiverRegistrationV1.model_validate(ordinary).namespace.key == WORKSPACE
    for namespace in (
        {"authority": "", "kind": "slack_enterprise", "key": WORKSPACE},
        {"authority": "https://slack.example.test", "kind": "slack_enterprise", "key": ORG},
        {"authority": "", "kind": "slack_workspace", "key": ORG},
        {"authority": "", "kind": "slack_organisation", "key": ORG},
    ):
        with pytest.raises(ValueError):
            ReceiverRegistrationV1.model_validate(
                _registration(namespace={"id": str(uuid.uuid4()), **namespace})
            )
