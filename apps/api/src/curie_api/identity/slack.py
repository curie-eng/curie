"""Slack's sender field mapping (#2910, ADR 0201 decisions 1 and 2).

ADR 0198 decision 6 sets the properties: sender-bound, canonical,
connection-scoped authority, fail-closed. ADR 0201 leaves which payload fields
satisfy them to this module, its fixture tests and the channel-ingress
interface note. The mapping, from the 2026-10-05 capture of an ordinary
non-Grid workspace:

- **Interactions** (``block_actions``, ``view_submission``) use the
  documented ``user.team_id``.
- **Events** use the documented ``user_team`` when present; an ``event.team``
  that disagrees with it is ``namespace_conflict``.
- Otherwise an event uses ``event.team`` only where the context establishes
  the sender's team by itself: an ``app_mention`` (the only delivery type it
  was observed on), in a channel positively marked not externally shared, on
  an installation with positive non-Grid ``auth.test`` evidence, and only when
  it equals the receiving installation's team. That team is a consistency
  check, never a fallback.
- **Any Enterprise Grid signal** refuses, on every path, since canonical ids
  under Grid cannot be confirmed from these payloads.

The envelope ``team_id``, ``source_team``, ``authorizations`` and enterprise
ids are never a sender team. Nothing is normalised: an id that differs only in
case or whitespace is a different id.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field

INTERACTION_DELIVERIES = frozenset({"block_actions", "view_submission"})
OBSERVED_EVENT_TEAM_DELIVERIES = frozenset({"app_mention"})
# Reserved in ChannelIdentity.attributes: only report_slack_identity writes it.
AUTH_TEST_KEY = "slack_auth_test"

SUBJECT_UNIDENTIFIED = "subject_unidentified"
NAMESPACE_CONFLICT = "namespace_conflict"

# Bounds on what one delivery's evidence may carry, far above any Slack id.
MAX_EVIDENCE_LENGTH = 256
MAX_ENTERPRISE_IDS = 32
EvidenceId = Annotated[str, Field(max_length=MAX_EVIDENCE_LENGTH)]


class SlackEvidence(BaseModel):
    """The identity-bearing fields of one Slack delivery, as the dispatcher read them.

    Strict, so a string ``"false"`` is never taken as the positive statement
    ``is_ext_shared_channel: false``; extra fields are refused, so a field this
    mapping does not use cannot be sent as if it were evidence.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    # event.type, or the interaction payload's type.
    delivery: str = Field(max_length=MAX_EVIDENCE_LENGTH)
    user_id: str | None = Field(default=None, max_length=MAX_EVIDENCE_LENGTH)
    event_user_team: str | None = Field(default=None, max_length=MAX_EVIDENCE_LENGTH)
    event_team: str | None = Field(default=None, max_length=MAX_EVIDENCE_LENGTH)
    interaction_user_team_id: str | None = Field(default=None, max_length=MAX_EVIDENCE_LENGTH)
    # The Events API envelope's flag; None when the envelope lacks it.
    is_ext_shared_channel: bool | None = None
    # Every enterprise signal seen anywhere in the payload.
    enterprise_ids: list[EvidenceId] = Field(default_factory=list, max_length=MAX_ENTERPRISE_IDS)


def _auth_test(identity_attributes: dict[str, Any]) -> dict[str, Any] | None:
    stored = identity_attributes.get(AUTH_TEST_KEY)
    return stored if isinstance(stored, dict) else None


def _grid_signal(evidence: SlackEvidence, auth_test: dict[str, Any] | None) -> bool:
    if evidence.enterprise_ids:
        return True
    if auth_test is None:
        return False
    return (
        auth_test.get("enterprise_id") is not None or auth_test.get("is_enterprise_install") is True
    )


def _positively_non_grid(
    auth_test: dict[str, Any] | None, installation_authority: str, installation_account: str
) -> bool:
    """auth.test said, for THIS installation, that it is not Enterprise Grid.

    An ordinary workspace's ``auth.test`` omits ``enterprise_id`` (Slack sends
    it only within an Enterprise organization), so an absent or null
    ``enterprise_id`` counts; ``enterprise_id_present`` is recorded but not
    required. ``is_enterprise_install`` was observed present, so it must be
    the strict bool ``False``: absent, null or a non-bool is not evidence, nor
    is evidence for another team (stale since a reattach).
    """

    if auth_test is None:
        return False
    return (
        auth_test.get("enterprise_id") is None
        and auth_test.get("is_enterprise_install") is False
        and auth_test.get("team_id") == installation_account
        and installation_authority == ""
    )


def derive(
    evidence: SlackEvidence,
    *,
    identity_attributes: dict[str, Any],
    installation_authority: str,
    installation_account: str,
) -> tuple[str, str] | str:
    """Return ``(sender_team, native_id)``, or the reason there is none."""

    user_id = evidence.user_id
    if not user_id or not user_id.strip():
        return SUBJECT_UNIDENTIFIED
    auth_test = _auth_test(identity_attributes)
    if _grid_signal(evidence, auth_test):
        return SUBJECT_UNIDENTIFIED

    if evidence.delivery in INTERACTION_DELIVERIES:
        team = evidence.interaction_user_team_id
        return (team, user_id) if team else SUBJECT_UNIDENTIFIED

    if evidence.event_user_team:
        if evidence.event_team and evidence.event_team != evidence.event_user_team:
            # Slack Connect semantics are unobserved: fail closed.
            return NAMESPACE_CONFLICT
        return (evidence.event_user_team, user_id)

    # The context-established path (ADR 0201 decision 2).
    if evidence.delivery not in OBSERVED_EVENT_TEAM_DELIVERIES:
        return SUBJECT_UNIDENTIFIED
    if evidence.is_ext_shared_channel is not False:
        return SUBJECT_UNIDENTIFIED
    if not _positively_non_grid(auth_test, installation_authority, installation_account):
        return SUBJECT_UNIDENTIFIED
    team = evidence.event_team
    if not team:
        return SUBJECT_UNIDENTIFIED
    if team != installation_account:
        return NAMESPACE_CONFLICT
    return (team, user_id)
