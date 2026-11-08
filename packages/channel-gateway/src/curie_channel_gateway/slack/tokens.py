"""The bot token each Slack identity speaks with in this gateway (ADR-0168 decision 5).

`aci_protocol.slack_identities` parses which identities the chart declares and
the env name holding each bot token. This module reads those tokens once, at
boot, and names the identity a Slack route's calls authenticate as.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping

from aci_protocol.slack_identities import SLACK_IDENTITIES_ENV, SlackIdentities
from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND, slack_speaking_identity
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SlackProviderConfig(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore", hide_input_in_errors=True, populate_by_name=True
    )
    slack_bot_token: str = Field(default="", repr=False)
    slack_identities: SlackIdentities = Field(default=(), validation_alias=SLACK_IDENTITIES_ENV)


logger = logging.getLogger(__name__)


def slack_bot_tokens(
    config: SlackProviderConfig, *, environ: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Each identity's bot token, keyed by identity name.

    ``default`` is always present, blank or not, so a stock install sends the
    token it always sent and reads no other environment. A named identity
    whose token is blank is logged and left out, so a turn addressed to it is
    refused rather than answered by another bot.
    """

    tokens = {DEFAULT_IDENTITY: config.slack_bot_token}
    if not config.slack_identities:
        return tokens
    env: Mapping[str, str] = os.environ if environ is None else environ
    for declared in config.slack_identities:
        if declared.name == DEFAULT_IDENTITY:
            continue
        token = env.get(declared.bot_token_env) or ""
        if not token.strip():
            logger.error(
                "Slack identity %s cannot reply from this gateway: %s is empty",
                declared.name,
                declared.bot_token_env,
            )
            continue
        tokens[declared.name] = token
    return tokens


def token_identity(adapter: str | None, endpoint: str | None) -> str:
    """The identity whose bot token a Slack route's calls carry.

    The rule is ``aci_protocol.turn.slack_speaking_identity``, shared with the
    API so the two cannot disagree about a route.
    """

    return slack_speaking_identity(SLACK_KIND, adapter, endpoint)
