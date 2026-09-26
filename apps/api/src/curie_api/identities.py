"""Which identities an installation declares (ADR-0168 decisions 1 and 3).

Slack's are the chart's list, read from `CURIE_SLACK_IDENTITIES`, until
ADR-0155's `provider_installations` exists (#2909). An out-of-process adapter's
name cannot be listed by the API, so other kinds keep the slug rule the write
schema already applies.
"""

import logging
import os
from collections.abc import Mapping

from aci_protocol.slack_identities import declared_slack_identity_names
from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND

from .config import Settings, get_settings

logger = logging.getLogger(__name__)


def declared_identities(kind: str) -> frozenset[str] | None:
    """The identities a binding of ``kind`` may name, or None when not enumerable."""

    if kind == SLACK_KIND:
        return declared_slack_identity_names(get_settings().slack_identities)
    return None


def refuse_undeclared(kind: str, identity: str | None) -> None:
    """Raise if ``identity`` is not one this installation declares for ``kind``.

    Shared by `ChannelBindingWrite` and `PublicationCreate`, which both need
    the same check. Call with the RESOLVED
    identity (``route_identity(kind, adapter)``), never the raw column: an
    omitted Slack adapter means the default app, and comparing the raw
    ``None`` here would refuse the common case of not naming one at all. A
    no-op for a kind `declared_identities` cannot yet enumerate.
    """

    declared = declared_identities(kind)
    if declared is None or identity in declared:
        return
    names = ", ".join(repr(name) for name in sorted(declared))
    raise ValueError(
        f"{kind} identity {identity!r} is not declared by this installation, "
        f"which declares {names}."
    )


def slack_bot_tokens(
    settings: Settings, *, environ: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Each Slack identity's bot token, keyed by name (ADR-0168 decision 5).

    ``default`` comes from ``SLACK_BOT_TOKEN`` and is absent when that is
    blank, the normal Slack-free install. A stock install reads no other
    environment. A named identity whose token is blank is logged and left out.
    """

    tokens: dict[str, str] = {}
    if settings.slack_bot_token:
        tokens[DEFAULT_IDENTITY] = settings.slack_bot_token
    if not settings.slack_identities:
        return tokens
    env: Mapping[str, str] = os.environ if environ is None else environ
    for declared in settings.slack_identities:
        if declared.name == DEFAULT_IDENTITY:
            continue
        token = env.get(declared.bot_token_env) or ""
        if not token.strip():
            logger.error(
                "Slack identity %s cannot resolve approver groups: %s is empty",
                declared.name,
                declared.bot_token_env,
            )
            continue
        tokens[declared.name] = token
    return tokens
