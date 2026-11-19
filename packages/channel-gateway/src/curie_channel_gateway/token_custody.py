"""Credential custody checks for applications that only consume gateway ports."""

import os
from collections.abc import Mapping

from aci_protocol.slack_identities import SlackIdentities
from pydantic import TypeAdapter, ValidationError


def refuse_slack_tokens(values: object) -> object:
    supplied = values if isinstance(values, Mapping) else {}
    token_names: set[str] = set()
    try:
        declared = TypeAdapter(SlackIdentities).validate_python(
            os.environ.get("CURIE_SLACK_IDENTITIES", "[]")
        )
        for identity in declared:
            token_names.update((identity.bot_token_env, identity.app_token_env))
    except ValidationError:
        pass
    candidates = [*os.environ.items(), *supplied.items()]
    for name, value in candidates:
        if not isinstance(value, str) or not value:
            continue
        if (
            name in token_names
            or ("SLACK" in str(name).upper() and "TOKEN" in str(name).upper())
            or value.startswith(("xoxb-", "xoxp-", "xapp-"))
        ):
            raise ValueError("Slack tokens belong only in the channel gateway or dispatcher")
    return values
