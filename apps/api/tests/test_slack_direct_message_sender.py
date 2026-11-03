"""Registered sender attribution for one-to-one direct messages to a receiver's bot.

@spec HTTP-INVOCATION-EXECUTION-26.
Every workspace, organisation and user id here is synthetic. No real direct
message delivery was captured: its team fields remain unmeasured (ADR 0263).
"""

from __future__ import annotations

from typing import Any

import pytest
from curie_api.identity.slack import SlackEvidence, derive_registered_sender

HOME = "T0EXAMPLEHOME"
PARTNER = "T0EXAMPLEPARTNER"
STRANGER = "T0EXAMPLESTRANGER"
ORG = "E0EXAMPLEORG"
OTHER_ORG = "E0EXAMPLEPARTNERORG"
USER = "U0EXAMPLEPERSON"
ORDINARY = {
    "namespace_kind": "slack_workspace", "namespace_authority": "", "namespace_key": HOME,
    "installation_account": HOME,
}
GRID = {
    "namespace_kind": "slack_enterprise", "namespace_authority": "", "namespace_key": ORG,
    "installation_account": HOME,
}


def direct(**changes: Any) -> SlackEvidence:
    """@spec HTTP-INVOCATION-EXECUTION-26: a plain one-to-one direct message."""
    return SlackEvidence.model_validate({
        "delivery": "message", "user_id": USER, "event_user_team": HOME, "event_team": HOME,
        "interaction_user_team_id": None, "is_ext_shared_channel": False, "enterprise_ids": [],
        "channel_type": "im", **changes,
    })


def ordinary(evidence: SlackEvidence, **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-26."""
    return derive_registered_sender(evidence, **{**ORDINARY, **changes})


def grid(evidence: SlackEvidence, **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-26."""
    return derive_registered_sender(evidence, **{**GRID, **changes})


@pytest.mark.parametrize("changes", [
    {},
    {"event_user_team": None},
    {"event_team": None},
    {"event_source_team": HOME},
])
def test_ordinary_plain_direct_message_resolves_in_the_own_namespace(
    changes: dict[str, Any],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: every present team field is the installation."""
    assert ordinary(direct(**changes)) == (HOME, USER)


def test_grid_plain_direct_message_resolves_in_the_organisation() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: team fields are neither namespace nor check."""
    evidence = direct(
        enterprise_ids=[ORG], event_user_team="T0EXAMPLESIBLING", event_team=None,
    )
    assert grid(evidence) == (ORG, USER)
    assert grid(direct(enterprise_ids=[ORG], event_user_team=None, event_team=None)) == (
        ORG, USER,
    )


@pytest.mark.parametrize(("changes", "reason"), [
    ({"user_id": None}, "subject_unidentified"),
    ({"user_id": "  "}, "subject_unidentified"),
    ({"channel_type": "mpim"}, "subject_unidentified"),
    ({"channel_type": "channel"}, "subject_unidentified"),
    ({"channel_type": "group"}, "subject_unidentified"),
    ({"channel_type": None}, "subject_unidentified"),
    ({"is_ext_shared_channel": True}, "subject_unidentified"),
    ({"is_ext_shared_channel": None}, "subject_unidentified"),
    ({"event_user_team": None, "event_team": None}, "subject_unidentified"),
    ({"event_user_team": None, "event_team": None, "event_source_team": HOME},
     "subject_unidentified"),
    ({"event_user_team": STRANGER}, "namespace_conflict"),
    ({"event_team": STRANGER}, "namespace_conflict"),
    ({"event_source_team": STRANGER}, "namespace_conflict"),
    ({"enterprise_ids": [ORG]}, "subject_unidentified"),
    ({"enterprise_ids": ["is_enterprise_install"]}, "subject_unidentified"),
])
def test_ordinary_direct_message_refusals(changes: dict[str, Any], reason: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: positive statements only, absence never permits."""
    assert ordinary(direct(**changes)) == reason


@pytest.mark.parametrize(("changes", "reason"), [
    ({"user_id": None}, "subject_unidentified"),
    ({"channel_type": "mpim"}, "subject_unidentified"),
    ({"channel_type": None}, "subject_unidentified"),
    ({"is_ext_shared_channel": True}, "subject_unidentified"),
    ({"is_ext_shared_channel": None}, "subject_unidentified"),
    ({"enterprise_ids": []}, "subject_unidentified"),
    ({"enterprise_ids": [ORG, "is_enterprise_install"]}, "subject_unidentified"),
    ({"enterprise_ids": [OTHER_ORG]}, "namespace_conflict"),
    ({"enterprise_ids": [ORG, OTHER_ORG]}, "namespace_conflict"),
])
def test_grid_direct_message_refusals(changes: dict[str, Any], reason: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: ADR 0252's enterprise checks still apply."""
    assert grid(direct(**{"enterprise_ids": [ORG], **changes})) == reason


def test_trusted_partner_namespaces_never_widen_a_direct_message() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: ADR 0261 entries are not read for a DM."""
    partner = direct(event_user_team=PARTNER, event_team=HOME, event_source_team=PARTNER)
    assert ordinary(partner, trusted_workspaces=(PARTNER,)) == "namespace_conflict"
    assert ordinary(direct(event_user_team=PARTNER, event_team=None),
                    trusted_workspaces=(PARTNER,)) == "namespace_conflict"
    assert ordinary(direct(enterprise_ids=[OTHER_ORG], event_user_team=PARTNER),
                    trusted_organisations=(OTHER_ORG,)) == "subject_unidentified"
    assert grid(direct(enterprise_ids=[ORG], event_user_team=PARTNER),
                trusted_workspaces=(PARTNER,)) == (ORG, USER)
    # Trusting does not change the own member's result either.
    assert ordinary(direct(), trusted_workspaces=(PARTNER,)) == (HOME, USER)


def test_a_mention_carrying_a_channel_type_is_refused() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: only a message delivery may carry it."""
    mention = direct(delivery="app_mention", event_user_team=None)
    assert ordinary(mention) == "subject_unidentified"
    assert ordinary(direct(delivery="app_mention", event_user_team=None, channel_type=None)) == (
        HOME, USER,
    )


def test_absent_channel_type_keeps_existing_evidence_bytes() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: absent stays absent in the API evidence model."""
    evidence = direct(delivery="app_mention", channel_type=None)
    assert "channel_type" not in evidence.model_dump(mode="json")
    assert direct().model_dump(mode="json")["channel_type"] == "im"
