"""The test installation declaration (ADR 0202 decision 1).

An installation's operator may declare it a test installation and list the bots
allowed to drive it. The chart renders ``testInstallation.enabled`` and
``testInstallation.drivers`` into the API and the dispatcher as the two
variables below, and both services read them through this one module, so the
two cannot disagree about what a driver entry is or which secrets are
published.

A driver entry is a channel id, a Slack bot id and that bot's user id,
validated like a ``dispatcher.threadedBotAllowlist`` pair. A sibling driver
(another identity of this installation, ADR 0168) also names the one agent it
serves. Whether an entry is a sibling is known only at the dispatcher's
preflight, from ``auth.test``, so ``agent`` is optional here.

The Helm helper ``charts/curie/templates/_test-installation.tpl`` repeats the
entry rules and the three published defaults so a bad value fails the render;
``charts/curie/ci/test-installation-assertions.sh`` feeds its rendered value
through the API and dispatcher settings to hold the two in step.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

TEST_INSTALLATION_ENABLED_ENV = "CURIE_TEST_INSTALLATION_ENABLED"
TEST_INSTALLATION_DRIVERS_ENV = "CURIE_TEST_INSTALLATION_DRIVERS"

# The secrets this repository publishes as defaults (values-dev.yaml,
# compose.dev.yaml and the services' own dev defaults), by the variable each
# service reads. Anyone reading the repository holds them, so a test
# installation, which admits bot-driven actions, may not run on one.
PUBLISHED_DEFAULT_API_KEY = "curie-dev-key"
PUBLISHED_DEFAULT_INTERNAL_WORKER_TOKEN = "curie-dev-worker-token"
PUBLISHED_DEFAULT_APPROVAL_CHAT_ATTESTER_SECRET = "curie-dev-approval-chat-attester"

_CHANNEL_ID = re.compile(r"^[CG][A-Z0-9]+$")
_BOT_ID = re.compile(r"^B[A-Z0-9]+$")
_BOT_USER_ID = re.compile(r"^U[A-Z0-9]+$")
_REQUIRED = ("channel_id", "bot_id", "bot_user_id")
_OPTIONAL = ("agent",)


@dataclass(frozen=True)
class DeclaredDriver:
    """One bot the operator lists as allowed to drive this installation."""

    channel_id: str
    bot_id: str
    bot_user_id: str
    # The one agent a sibling driver's identity serves; None for a driver
    # that runs in another installation.
    agent: str | None = None


def parse_drivers(value: object) -> tuple[DeclaredDriver, ...]:
    """Parse the drivers list from its JSON wire form, or an already decoded list.

    Raises ``ValueError`` naming the entry and field on any malformed input,
    so a bad declaration refuses boot instead of admitting a different set.
    """

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{TEST_INSTALLATION_DRIVERS_ENV} is not JSON: {exc.msg}") from None
    if isinstance(value, tuple) and all(isinstance(entry, DeclaredDriver) for entry in value):
        return value
    if not isinstance(value, list):
        raise ValueError(
            f"{TEST_INSTALLATION_DRIVERS_ENV} must be a JSON list of driver entries"
        )
    drivers: list[DeclaredDriver] = []
    for index, entry in enumerate(value):
        where = f"testInstallation.drivers[{index}]"
        if not isinstance(entry, Mapping):
            raise ValueError(f"{where} must be a mapping")
        unknown = sorted(set(entry) - set(_REQUIRED) - set(_OPTIONAL))
        if unknown:
            raise ValueError(f"{where} has unknown keys: {', '.join(map(str, unknown))}")
        for key, pattern in (
            ("channel_id", _CHANNEL_ID),
            ("bot_id", _BOT_ID),
            ("bot_user_id", _BOT_USER_ID),
        ):
            field = entry.get(key)
            if not isinstance(field, str) or not field:
                raise ValueError(f"{where}.{key} is required")
            if not pattern.match(field):
                raise ValueError(f"{where}.{key} does not match {pattern.pattern}")
        agent = entry.get("agent")
        if agent is not None and (not isinstance(agent, str) or not agent.strip()):
            raise ValueError(f"{where}.agent must be a nonblank string when present")
        drivers.append(
            DeclaredDriver(
                channel_id=entry["channel_id"],
                bot_id=entry["bot_id"],
                bot_user_id=entry["bot_user_id"],
                agent=agent,
            )
        )
    return tuple(drivers)


def refuse_published_defaults(enabled: bool, secrets: Mapping[str, str]) -> None:
    """Refuse boot when the declaration is on and a held secret is published.

    ``secrets`` maps the variable a service reads to the value it holds; a
    service passes only the secrets it holds. Raises ``ValueError`` naming
    every offender, never a value.
    """

    if not enabled:
        return
    published = {
        "API_KEY": PUBLISHED_DEFAULT_API_KEY,
        "CURIE_API_KEY": PUBLISHED_DEFAULT_API_KEY,
        "CURIE_INTERNAL_WORKER_TOKEN": PUBLISHED_DEFAULT_INTERNAL_WORKER_TOKEN,
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET": PUBLISHED_DEFAULT_APPROVAL_CHAT_ATTESTER_SECRET,
    }
    offenders = [name for name, value in secrets.items() if value == published[name]]
    if offenders:
        raise ValueError(
            f"{TEST_INSTALLATION_ENABLED_ENV}=true but these secrets are still the "
            f"published default: {', '.join(offenders)}. A test installation admits "
            "bot-driven actions, so it may not boot on a secret anyone reading this "
            "repository holds (ADR 0202). Set real values."
        )
