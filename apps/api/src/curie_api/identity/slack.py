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
- **Any Enterprise Grid signal** refuses, on every path of ``derive``, since
  canonical ids under Grid cannot be confirmed from these payloads. Only an
  operator-registered HTTP receiver's Grid workspace path resolves Grid
  senders, through ``derive_registered_sender`` (ADR 0252).
- **Slack Connect** mentions (``is_ext_shared_channel`` exactly true) resolve
  only on that registered path, from an established home team, in the
  registration's own namespace or an operator-trusted partner namespace
  (ADR 0261); ``derive`` is unchanged.

The envelope ``team_id``, ``source_team``, ``authorizations`` and enterprise
ids are never a sender team. Nothing is normalised: an id that differs only in
case or whitespace is a different id.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    model_serializer,
)

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
    # The event's source_team; only an operator-registered receiver reads it
    # (ADR 0261). @spec HTTP-INVOCATION-EXECUTION-24.
    event_source_team: str | None = Field(default=None, max_length=MAX_EVIDENCE_LENGTH)

    @model_serializer(mode="wrap")
    def _omit_absent_source_team(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo,
    ) -> dict[str, Any]:
        """Absent stays absent, so existing evidence serialises unchanged.

        @spec HTTP-INVOCATION-EXECUTION-24.
        """
        data: dict[str, Any] = handler(self)
        if self.event_source_team is None:
            data.pop("event_source_team", None)
        return data


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


SLACK_WORKSPACE_KIND = "slack_workspace"
SLACK_ENTERPRISE_KIND = "slack_enterprise"
# What the dispatcher's copier records for an organisation-wide install.
ENTERPRISE_INSTALL_SIGNAL = "is_enterprise_install"


_REASONS = frozenset({SUBJECT_UNIDENTIFIED, NAMESPACE_CONFLICT})


def _connect_home_team(evidence: SlackEvidence, installation_account: str) -> str:
    """The sender's home team in a Slack Connect mention, or the refusal reason.

    ADR 0261 decision 2: ``user_team`` names the home team and a present
    ``source_team`` must agree with it; ``event.team`` is read only when both
    are absent, and then it must be the receiving installation.
    @spec HTTP-INVOCATION-EXECUTION-24.
    """
    user_team, source_team = evidence.event_user_team, evidence.event_source_team
    if user_team is not None:
        if source_team is not None and source_team != user_team:
            return NAMESPACE_CONFLICT
        return user_team
    if source_team is not None or not evidence.event_team:
        return SUBJECT_UNIDENTIFIED
    if evidence.event_team != installation_account:
        return NAMESPACE_CONFLICT
    return installation_account


def _ordinary_namespace(
    evidence: SlackEvidence, home: str, *, installation_account: str,
    workspaces: Collection[str], organisations: Collection[str], answer: bool,
) -> str:
    """The namespace key for an established home team, or the refusal reason.

    ADR 0261 decisions 3 and 4 on an ordinary workspace registration. An
    answer's ``user.team_id`` must itself be the trusted organisation's key.
    @spec HTTP-INVOCATION-EXECUTION-24.
    """
    signals = evidence.enterprise_ids
    if not signals:
        if home == installation_account or home in workspaces:
            return home
        return NAMESPACE_CONFLICT
    if not organisations or ENTERPRISE_INSTALL_SIGNAL in signals:
        return SUBJECT_UNIDENTIFIED
    organisation = signals[0]
    if any(signal != organisation for signal in signals) or organisation not in organisations:
        return NAMESPACE_CONFLICT
    if home == installation_account or (answer and home != organisation):
        return NAMESPACE_CONFLICT
    return organisation


def _derive_ordinary(
    evidence: SlackEvidence, *, installation_account: str,
    workspaces: Collection[str], organisations: Collection[str],
) -> tuple[str, str] | str:
    """@spec HTTP-INVOCATION-EXECUTION-21/24."""
    connect = (
        evidence.delivery in OBSERVED_EVENT_TEAM_DELIVERIES
        and evidence.is_ext_shared_channel is True
    )
    trusting = bool(workspaces or organisations)
    if connect or (trusting and evidence.delivery in INTERACTION_DELIVERIES):
        user_id = evidence.user_id
        if not user_id or not user_id.strip():
            return SUBJECT_UNIDENTIFIED
        if connect:
            home = _connect_home_team(evidence, installation_account)
        else:
            home = evidence.interaction_user_team_id or SUBJECT_UNIDENTIFIED
        if home in _REASONS:
            return home
        key = _ordinary_namespace(
            evidence, home, installation_account=installation_account,
            workspaces=workspaces, organisations=organisations, answer=not connect,
        )
        return key if key in _REASONS else (key, user_id)
    result = derive(
        evidence,
        identity_attributes={AUTH_TEST_KEY: {
            "team_id": installation_account, "enterprise_id": None,
            "is_enterprise_install": False,
        }},
        installation_authority="",
        installation_account=installation_account,
    )
    # Outside a Connect mention or an answer, trust never widens derive: only
    # the installation's own team can be the sender's namespace there.
    if trusting and isinstance(result, tuple) and result[0] != installation_account:
        return NAMESPACE_CONFLICT
    return result


def _derive_grid(
    evidence: SlackEvidence, *, organisation: str, installation_account: str,
    workspaces: Collection[str],
) -> tuple[str, str] | str:
    """ADR 0252 decisions 5 and 6 and ADR 0261 decisions 3 and 4.

    @spec HTTP-INVOCATION-EXECUTION-21/24. The context establishes the
    organisation; payload enterprise ids only check it, and Grid team fields
    can name a workspace or the organisation.
    """

    user_id = evidence.user_id
    if not user_id or not user_id.strip():
        return SUBJECT_UNIDENTIFIED
    signals = evidence.enterprise_ids
    if not signals:
        # A trusted partner's modal submission may carry no enterprise id at
        # all; no other delivery without one resolves (ADR 0261 decision 4).
        team = evidence.interaction_user_team_id
        if evidence.delivery == "view_submission" and team is not None and team in workspaces:
            return (team, user_id)
        return SUBJECT_UNIDENTIFIED
    if ENTERPRISE_INSTALL_SIGNAL in signals:
        return SUBJECT_UNIDENTIFIED
    if any(signal != organisation for signal in signals):
        return NAMESPACE_CONFLICT
    if evidence.delivery in OBSERVED_EVENT_TEAM_DELIVERIES:
        if evidence.is_ext_shared_channel is False:
            return (organisation, user_id)
        if evidence.is_ext_shared_channel is not True:
            return SUBJECT_UNIDENTIFIED
        home = _connect_home_team(evidence, installation_account)
    elif evidence.delivery in INTERACTION_DELIVERIES:
        home = evidence.interaction_user_team_id or SUBJECT_UNIDENTIFIED
    else:
        return SUBJECT_UNIDENTIFIED
    if home in _REASONS:
        return home
    if home == installation_account:
        return (organisation, user_id)
    if home in workspaces:
        return (home, user_id)
    return NAMESPACE_CONFLICT


def derive_registered_sender(
    evidence: SlackEvidence,
    *,
    namespace_kind: str,
    namespace_authority: str,
    namespace_key: str,
    installation_account: str,
    trusted_workspaces: Collection[str] = (),
    trusted_organisations: Collection[str] = (),
) -> tuple[str, str] | str:
    """Return ``(namespace key, native id)`` for an operator-registered receiver.

    @spec HTTP-INVOCATION-EXECUTION-21/24. An ordinary registration that trusts
    nothing is exactly :func:`derive` with positive non-Grid evidence for its
    own account, except that a Slack Connect mention from its own members is
    attributed (ADR 0261); a Grid workspace registration resolves in its
    organisation (ADR 0252). A returned key is the registration's own key or
    the key of one of its trusted partner namespaces.
    """

    if namespace_authority != "":
        return SUBJECT_UNIDENTIFIED
    own = {namespace_key, installation_account}
    if any(key in own for key in (*trusted_workspaces, *trusted_organisations)):
        return SUBJECT_UNIDENTIFIED
    if namespace_kind == SLACK_WORKSPACE_KIND:
        if namespace_key != installation_account:
            return SUBJECT_UNIDENTIFIED
        return _derive_ordinary(
            evidence, installation_account=installation_account,
            workspaces=trusted_workspaces, organisations=trusted_organisations,
        )
    if (
        namespace_kind == SLACK_ENTERPRISE_KIND and namespace_key != installation_account
        and not trusted_organisations
    ):
        return _derive_grid(
            evidence, organisation=namespace_key, installation_account=installation_account,
            workspaces=trusted_workspaces,
        )
    return SUBJECT_UNIDENTIFIED
