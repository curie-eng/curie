"""Registered Slack Connect senders resolved from operator-trusted partner namespaces.

@spec HTTP-INVOCATION-EXECUTION-24.
Every organisation, workspace and user id here is synthetic. No real Slack
Connect delivery or external-workspace interaction was observed.
"""

from __future__ import annotations

import importlib
import uuid
from typing import Any

import pytest
from curie_api.identity.slack import SlackEvidence

HOME = "T0EXAMPLEHOME"
PARTNER = "T0EXAMPLEPARTNER"
PARTNER_MEMBER_WORKSPACE = "T0EXAMPLEPARTNERWS"
STRANGER = "T0EXAMPLESTRANGER"
SIBLING = "T0EXAMPLESIBLING"
ORG = "E0EXAMPLEORG"
PARTNER_ORG = "E0EXAMPLEPARTNER"
OTHER_ORG = "E0EXAMPLEOTHER"
USER = "U0EXAMPLEPERSON"
INSTALL = "is_enterprise_install"


def owner() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-24: missing owner fails in the test body."""
    module = importlib.import_module("curie_api.identity.slack")
    return module.derive_registered_sender


def evidence(**fields: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: synthetic evidence, validated inside each test."""
    return {"user_id": USER, **fields}


def connect(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: a mention in a channel marked externally shared."""
    return evidence(**{
        "delivery": "app_mention", "event_team": HOME, "is_ext_shared_channel": True,
        "enterprise_ids": [], **changes,
    })


def answer(delivery: str = "block_actions", **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: an interaction with the documented user.team_id."""
    return evidence(**{
        "delivery": delivery, "interaction_user_team_id": HOME, "enterprise_ids": [], **changes,
    })


def ordinary(value: dict[str, Any], **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-24: ordinary registration trusting a workspace and org."""
    return owner()(SlackEvidence.model_validate(value), **{
        "namespace_kind": "slack_workspace", "namespace_authority": "", "namespace_key": HOME,
        "installation_account": HOME, "trusted_workspaces": frozenset({PARTNER}),
        "trusted_organisations": frozenset({PARTNER_ORG}), **changes,
    })


def grid(value: dict[str, Any], **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-24: Grid workspace registration trusting a workspace."""
    return owner()(SlackEvidence.model_validate(value), **{
        "namespace_kind": "slack_enterprise", "namespace_authority": "", "namespace_key": ORG,
        "installation_account": HOME, "trusted_workspaces": frozenset({PARTNER}),
        "trusted_organisations": frozenset(), **changes,
    })


# --- decision 2 and 3: ordinary registration mentions -------------------------------------


@pytest.mark.parametrize(("value", "expected"), [
    (connect(event_user_team=HOME, event_source_team=HOME), (HOME, USER)),
    (connect(event_user_team=HOME), (HOME, USER)),
    # event.team is never read once user_team is present: it can name the receiver.
    (connect(event_user_team=HOME, event_team=PARTNER), (HOME, USER)),
    (connect(), (HOME, USER)),
    (connect(event_user_team=PARTNER, event_source_team=PARTNER), (PARTNER, USER)),
    (connect(event_user_team=PARTNER, event_team=HOME), (PARTNER, USER)),
    (connect(event_user_team=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[PARTNER_ORG]),
     (PARTNER_ORG, USER)),
    (connect(event_user_team=PARTNER_MEMBER_WORKSPACE, event_source_team=PARTNER_MEMBER_WORKSPACE,
             enterprise_ids=[PARTNER_ORG, PARTNER_ORG]), (PARTNER_ORG, USER)),
])
def test_ordinary_connect_mention_resolves_in_the_home_teams_namespace(
    value: dict[str, Any], expected: tuple[str, str],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: own, trusted workspace or trusted organisation."""
    assert ordinary(value) == expected


@pytest.mark.parametrize(("value", "reason"), [
    (connect(user_id=None), "subject_unidentified"),
    (connect(user_id=""), "subject_unidentified"),
    (connect(user_id="  "), "subject_unidentified"),
    (connect(event_user_team=PARTNER, event_source_team=STRANGER), "namespace_conflict"),
    (connect(event_user_team=HOME, event_source_team=PARTNER), "namespace_conflict"),
    (connect(event_source_team=PARTNER), "subject_unidentified"),
    (connect(event_source_team=HOME), "subject_unidentified"),
    (connect(event_team=None), "subject_unidentified"),
    (connect(event_team=PARTNER), "namespace_conflict"),
    (connect(event_user_team=STRANGER), "namespace_conflict"),
    (connect(event_user_team=PARTNER_ORG), "namespace_conflict"),
    (connect(event_user_team=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[PARTNER_ORG, INSTALL]),
     "subject_unidentified"),
    (connect(event_user_team=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[INSTALL]),
     "subject_unidentified"),
    (connect(event_user_team=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[OTHER_ORG]),
     "namespace_conflict"),
    (connect(event_user_team=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[PARTNER_ORG, OTHER_ORG]),
     "namespace_conflict"),
    (connect(event_user_team=HOME, enterprise_ids=[PARTNER_ORG]), "namespace_conflict"),
    (connect(enterprise_ids=[PARTNER_ORG]), "namespace_conflict"),
    # An absent shared flag is not a Connect channel and a partner is never attributed.
    (connect(is_ext_shared_channel=None, event_user_team=PARTNER, event_team=None),
     "namespace_conflict"),
    (connect(delivery="message", event_user_team=PARTNER, event_team=None),
     "namespace_conflict"),
])
def test_ordinary_connect_mention_refusals_name_their_reason(
    value: dict[str, Any], reason: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    assert ordinary(value) == reason


def test_ordinary_registration_trusting_no_organisation_refuses_every_enterprise_signal() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    for value in (
        connect(event_user_team=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[PARTNER_ORG]),
        connect(event_user_team=PARTNER, enterprise_ids=[PARTNER_ORG]),
        answer(interaction_user_team_id=PARTNER_ORG, enterprise_ids=[PARTNER_ORG]),
    ):
        assert ordinary(value, trusted_organisations=frozenset()) == "subject_unidentified"


def test_ordinary_registration_trusting_nothing_attributes_only_its_own_connect_members() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: ADR 0261 decision 2 without any trust list."""
    bare = {"trusted_workspaces": frozenset(), "trusted_organisations": frozenset()}
    assert ordinary(connect(), **bare) == (HOME, USER)
    assert ordinary(connect(event_user_team=HOME), **bare) == (HOME, USER)
    assert ordinary(connect(event_user_team=PARTNER), **bare) == "namespace_conflict"
    assert owner()(
        SlackEvidence.model_validate(connect()), namespace_kind="slack_workspace",
        namespace_authority="", namespace_key=HOME, installation_account=HOME,
    ) == (HOME, USER)


# --- decision 4: answers ------------------------------------------------------------------


@pytest.mark.parametrize("delivery", ["block_actions", "view_submission"])
def test_ordinary_answers_resolve_from_the_installation_or_a_trusted_key(delivery: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    assert ordinary(answer(delivery)) == (HOME, USER)
    assert ordinary(answer(delivery, interaction_user_team_id=PARTNER)) == (PARTNER, USER)
    assert ordinary(answer(
        delivery, interaction_user_team_id=PARTNER_ORG, enterprise_ids=[PARTNER_ORG],
    )) == (PARTNER_ORG, USER)


@pytest.mark.parametrize(("value", "reason"), [
    (answer(interaction_user_team_id=STRANGER), "namespace_conflict"),
    (answer(interaction_user_team_id=PARTNER_ORG), "namespace_conflict"),
    (answer(interaction_user_team_id=None), "subject_unidentified"),
    (answer(user_id=None), "subject_unidentified"),
    (answer(interaction_user_team_id=PARTNER_MEMBER_WORKSPACE, enterprise_ids=[PARTNER_ORG]),
     "namespace_conflict"),
    (answer(interaction_user_team_id=PARTNER, enterprise_ids=[PARTNER_ORG]), "namespace_conflict"),
    (answer(interaction_user_team_id=HOME, enterprise_ids=[PARTNER_ORG]), "namespace_conflict"),
    (answer(interaction_user_team_id=OTHER_ORG, enterprise_ids=[OTHER_ORG]), "namespace_conflict"),
    (answer(interaction_user_team_id=PARTNER_ORG, enterprise_ids=[PARTNER_ORG, INSTALL]),
     "subject_unidentified"),
    (answer("view_submission", interaction_user_team_id=STRANGER), "namespace_conflict"),
])
def test_ordinary_answer_refusals_name_their_reason(value: dict[str, Any], reason: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    assert ordinary(value) == reason


# --- decision 3 and 4: Grid workspace registrations ---------------------------------------


def grid_connect(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: a Connect mention in a channel hosted by the org."""
    return connect(**{"enterprise_ids": [ORG], **changes})


def grid_answer(delivery: str = "block_actions", **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    return answer(delivery, **{"enterprise_ids": [ORG], **changes})


@pytest.mark.parametrize(("value", "expected"), [
    (grid_connect(is_ext_shared_channel=False, event_user_team=SIBLING), (ORG, USER)),
    (grid_connect(event_user_team=HOME), (ORG, USER)),
    (grid_connect(event_user_team=HOME, event_source_team=HOME), (ORG, USER)),
    (grid_connect(), (ORG, USER)),
    (grid_connect(event_user_team=PARTNER, event_source_team=PARTNER), (PARTNER, USER)),
    (grid_connect(event_user_team=PARTNER), (PARTNER, USER)),
])
def test_grid_connect_mention_resolves_own_members_and_trusted_partners(
    value: dict[str, Any], expected: tuple[str, str],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: ADR 0252 checks first, then the home team."""
    assert grid(value) == expected


@pytest.mark.parametrize(("value", "reason"), [
    (grid_connect(event_user_team=SIBLING), "namespace_conflict"),
    (grid_connect(event_user_team=STRANGER), "namespace_conflict"),
    (grid_connect(event_user_team=PARTNER, event_source_team=STRANGER), "namespace_conflict"),
    (grid_connect(event_source_team=PARTNER), "subject_unidentified"),
    (grid_connect(event_team=None), "subject_unidentified"),
    (grid_connect(event_team=PARTNER), "namespace_conflict"),
    (grid_connect(event_user_team=PARTNER, enterprise_ids=[]), "subject_unidentified"),
    (grid_connect(event_user_team=PARTNER, enterprise_ids=[OTHER_ORG]), "namespace_conflict"),
    (grid_connect(event_user_team=PARTNER, enterprise_ids=[ORG, INSTALL]),
     "subject_unidentified"),
    (grid_connect(event_user_team=PARTNER, is_ext_shared_channel=None), "subject_unidentified"),
    (grid_connect(user_id=None), "subject_unidentified"),
])
def test_grid_connect_mention_refusals_name_their_reason(
    value: dict[str, Any], reason: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    assert grid(value) == reason


@pytest.mark.parametrize(("value", "expected"), [
    (grid_answer(), (ORG, USER)),
    (grid_answer("view_submission"), (ORG, USER)),
    (grid_answer(interaction_user_team_id=PARTNER), (PARTNER, USER)),
    (grid_answer("view_submission", interaction_user_team_id=PARTNER), (PARTNER, USER)),
    # The one modal shape without an enterprise id (ADR 0261 decision 4).
    (grid_answer("view_submission", interaction_user_team_id=PARTNER, enterprise_ids=[]),
     (PARTNER, USER)),
    (grid_answer(interaction_user_team_id=STRANGER), "namespace_conflict"),
    (grid_answer(interaction_user_team_id=SIBLING), "namespace_conflict"),
    (grid_answer("view_submission", interaction_user_team_id=HOME, enterprise_ids=[]),
     "subject_unidentified"),
    (grid_answer("view_submission", interaction_user_team_id=STRANGER, enterprise_ids=[]),
     "subject_unidentified"),
    (grid_answer(interaction_user_team_id=PARTNER, enterprise_ids=[]), "subject_unidentified"),
    (grid_answer("view_submission", interaction_user_team_id=PARTNER, enterprise_ids=[INSTALL]),
     "subject_unidentified"),
    (grid_answer("view_submission", interaction_user_team_id=None, enterprise_ids=[]),
     "subject_unidentified"),
    (grid_answer("view_submission", user_id=None, interaction_user_team_id=PARTNER,
                 enterprise_ids=[]), "subject_unidentified"),
    (grid_answer(interaction_user_team_id=PARTNER, enterprise_ids=[OTHER_ORG]),
     "namespace_conflict"),
])
def test_grid_answers_follow_the_same_set(value: dict[str, Any], expected: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    assert grid(value) == expected


def test_grid_view_submission_exception_needs_the_partner_to_be_trusted() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    value = grid_answer("view_submission", interaction_user_team_id=PARTNER, enterprise_ids=[])
    assert grid(value, trusted_workspaces=frozenset()) == "subject_unidentified"
    assert grid(grid_connect(event_user_team=PARTNER), trusted_workspaces=frozenset()) == (
        "namespace_conflict"
    )


@pytest.mark.parametrize("changes", [
    {"trusted_organisations": frozenset({PARTNER_ORG})},
    {"trusted_workspaces": frozenset({HOME})},
    {"trusted_workspaces": frozenset({ORG})},
])
def test_unsupported_trust_shapes_never_resolve(changes: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: registration validation is not the only guard."""
    assert grid(grid_connect(event_user_team=PARTNER), **changes) == "subject_unidentified"
    assert ordinary(connect(), **{
        "trusted_workspaces": frozenset({HOME}),
    }) == "subject_unidentified"


# --- decision 1: the registration ----------------------------------------------------------


def entry(kind: str, key: str, **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: one trusted namespace descriptor."""
    return {"id": str(uuid.uuid4()), "authority": "", "kind": kind, "key": key, **changes}


def registration(*, grid_scope: bool = False, **changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: operator declaration, synthetic ids."""
    kind, key = ("slack_enterprise", ORG) if grid_scope else ("slack_workspace", HOME)
    return {
        "schema_version": 1, "receiver_id": str(uuid.uuid4()), "generation": 1,
        "tenant_id": str(uuid.uuid4()), "channel_identity_id": str(uuid.uuid4()),
        "channel_identity_name": "synthetic-connect-receiver",
        "provider_installation_id": str(uuid.uuid4()),
        "namespace": {"id": str(uuid.uuid4()), "authority": "", "kind": kind, "key": key},
        "installation_account": HOME, "bot_user_id": "U0EXAMPLEBOT",
        "binding_ids": (str(uuid.uuid4()),), "human_native_ids": (USER,), **changes,
    }


def model() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    from curie_api.identity.http_invocation import ReceiverRegistrationV1

    return ReceiverRegistrationV1


def test_registration_without_trusted_namespaces_is_unchanged() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: existing operator files keep decoding."""
    declared = model().model_validate(registration())
    assert declared.trusted_namespaces == ()
    assert declared.namespace_for_key(HOME) == declared.namespace
    assert declared.namespace_for_key(PARTNER) is None
    assert declared.recorded_namespace(declared.namespace) == declared.namespace


def test_registration_helpers_select_own_or_trusted_namespaces_exactly() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    workspace = entry("slack_workspace", PARTNER)
    organisation = entry("slack_enterprise", PARTNER_ORG)
    declared = model().model_validate(registration(trusted_namespaces=(workspace, organisation)))
    own = declared.namespace
    assert declared.namespace_for_key(HOME) == own
    assert declared.namespace_for_key(PARTNER).model_dump() == workspace
    assert declared.namespace_for_key(PARTNER_ORG).model_dump() == organisation
    assert declared.namespace_for_key(STRANGER) is None
    assert declared.recorded_namespace(own) == own
    assert declared.recorded_namespace(declared.trusted_namespaces[0]).model_dump() == workspace
    for changed in (
        {**workspace, "id": str(uuid.uuid4())}, {**workspace, "key": STRANGER},
        {**workspace, "kind": "slack_enterprise"}, {**workspace, "authority": "x"},
    ):
        namespace_type = type(own)
        assert declared.recorded_namespace(namespace_type.model_validate(changed)) is None

    grid_declared = model().model_validate(registration(
        grid_scope=True, trusted_namespaces=(workspace,),
    ))
    assert grid_declared.namespace_for_key(ORG) == grid_declared.namespace
    assert grid_declared.namespace_for_key(PARTNER).model_dump() == workspace
    assert grid_declared.namespace_for_key(HOME) is None


@pytest.mark.parametrize(("grid_scope", "entries"), [
    (False, (entry("slack_workspace", PARTNER, authority="https://slack.example.test"),)),
    (False, (entry("slack_organisation", PARTNER_ORG),)),
    (False, (entry("email_domain", "example.com"),)),
    (True, (entry("slack_enterprise", PARTNER_ORG),)),
    (False, (entry("slack_workspace", PARTNER), entry("slack_workspace", PARTNER))),
    (False, (entry("slack_workspace", PARTNER), entry("slack_enterprise", PARTNER))),
    (False, (entry("slack_workspace", HOME),)),
    (True, (entry("slack_workspace", HOME),)),
    (True, (entry("slack_workspace", ORG),)),
    (False, tuple(entry("slack_workspace", f"T0EXAMPLE{index:04d}") for index in range(257))),
])
def test_registration_refuses_invalid_trusted_namespaces(
    grid_scope: bool, entries: tuple[dict[str, Any], ...],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24: the existing fixed registration refusal."""
    with pytest.raises(ValueError):
        model().model_validate(registration(grid_scope=grid_scope, trusted_namespaces=entries))


def test_registration_refuses_duplicate_ids_and_the_own_namespace_id() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    shared = str(uuid.uuid4())
    with pytest.raises(ValueError, match="invalid_receiver_registration"):
        model().model_validate(registration(trusted_namespaces=(
            entry("slack_workspace", PARTNER, id=shared),
            entry("slack_workspace", STRANGER, id=shared),
        )))
    declared = registration()
    with pytest.raises(ValueError, match="invalid_receiver_registration"):
        model().model_validate({**declared, "trusted_namespaces": (
            entry("slack_workspace", PARTNER, id=declared["namespace"]["id"]),
        )})


def test_registration_accepts_the_bound_of_256_entries() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    entries = tuple(entry("slack_workspace", f"T0EXAMPLE{index:04d}") for index in range(256))
    assert len(model().model_validate(
        registration(trusted_namespaces=entries),
    ).trusted_namespaces) == 256


# --- decision 6: API evidence -------------------------------------------------------------


def test_api_evidence_accepts_source_team_and_omits_it_when_absent() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24."""
    present = SlackEvidence.model_validate(
        evidence(delivery="app_mention", event_source_team=PARTNER),
    )
    assert present.event_source_team == PARTNER
    assert present.model_dump(mode="json")["event_source_team"] == PARTNER
    absent = SlackEvidence.model_validate(evidence(delivery="app_mention"))
    assert "event_source_team" not in absent.model_dump(mode="json")
