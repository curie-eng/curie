"""The one strict parse of ``connectors.yaml`` the API and the worker share (ADR 0239).

An entry carrying ``shared`` is validated with the frozen ``ConnectorSpec``,
whose ``exclusive_shared`` refuses any mixing with dedicated fields, and is held
to the frozen dedicated name rule. Every other entry, plus ``runner``, goes
through the frozen ``validate_connectors`` exactly as before. A refusal is a
list of ``(code, message)`` pairs and never an empty connector set, so no reader
can turn a malformed declaration into "declares nothing".

Pure: it imports only ``plugin_format``'s public names and the standard
library, so both readers agree by construction. The worker-managed Secret
names the API pins at registration and the worker creates are here too, for
the same reason.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from plugin_format.connectors import (
    RESERVED_CONNECTOR_NAMES,
    ConnectorsFile,
    ConnectorSpec,
    validate_connectors,
)
from plugin_format.deploy_targets import restrict_connectors

SHARED_KEY = "shared"

# The worker-managed Secrets of one application: a key Secret holding the
# resolver's X25519 private keys and, for bundled databases, a DSN Secret
# holding the DSN sealed to them. Registration requires exactly these names
# and data keys, and the admission policy admits only these shapes.
KEY_SECRET_PREFIX = "shared-key-"
KEY_SECRET_KEY = "private-keys"
DSN_SECRET_PREFIX = "shared-dsn-"
DSN_SECRET_KEY = "sealed-dsn"
APPLICATION_LABEL = "curietech.ai/shared-application"
SECRET_ROLE_LABEL = "curietech.ai/shared-secret"

INVALID_CODE = "connectors.shared_invalid"
BAD_NAME_CODE = "connectors.bad_name"
RESERVED_NAME_CODE = "connectors.reserved_name"

# Every code a declaration or resolution refusal carries on the wire. The
# worker tells a shared refusal from any other 4xx by this set.
SHARED_REFUSAL_CODES: frozenset[str] = frozenset(
    {
        INVALID_CODE,
        BAD_NAME_CODE,
        RESERVED_NAME_CODE,
        "shared_installation_unavailable",
        "shared_installation_invalid",
        "shared_installation_unobserved",
        "shared_application_unknown",
        "shared_application_disabled",
        "shared_application_not_member",
        "shared_application_root_mismatch",
        "shared_member_selector_invalid",
        "shared_application_invalid",
    }
)

# The frozen dedicated rule: the name becomes a Kubernetes object and a DNS label.
_NAME_MAX = 40
_NAME_RE = re.compile(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?")


def key_secret_name(application: str) -> str:
    return KEY_SECRET_PREFIX + application


def dsn_secret_name(application: str) -> str:
    return DSN_SECRET_PREFIX + application


def valid_connector_name(name: object) -> bool:
    return isinstance(name, str) and len(name) <= _NAME_MAX and bool(_NAME_RE.fullmatch(name))


@dataclass(frozen=True)
class SharedEntry:
    """One shared reference: the bundle's connector name and the application UUID."""

    name: str
    application: str


@dataclass(frozen=True)
class Declarations:
    """A parsed ``connectors.yaml``: the dedicated subset and the shared entries by name."""

    dedicated: ConnectorsFile = field(default_factory=ConnectorsFile)
    shared: tuple[SharedEntry, ...] = ()

    def restricted(self, allowlist: frozenset[str] | None) -> Declarations:
        """Both halves narrowed to a deploy target's allowlist; None keeps everything."""

        if allowlist is None:
            return self
        return Declarations(
            dedicated=restrict_connectors(self.dedicated, allowlist),
            shared=tuple(entry for entry in self.shared if entry.name in allowlist),
        )

    def names(self) -> frozenset[str]:
        return frozenset(self.dedicated.connectors) | {entry.name for entry in self.shared}


def _is_shared_entry(entry: object) -> bool:
    return isinstance(entry, Mapping) and SHARED_KEY in entry


def has_shared(data: Any) -> bool:
    """Whether parsed ``connectors.yaml`` content declares any shared entry."""

    connectors = data.get("connectors") if isinstance(data, Mapping) else None
    return isinstance(connectors, Mapping) and any(
        _is_shared_entry(entry) for entry in connectors.values()
    )


def dedicated_subset(data: dict[str, Any]) -> dict[str, Any]:
    """The same mapping with every shared entry removed; nothing else changes."""

    connectors = data.get("connectors")
    if not isinstance(connectors, Mapping):
        return data
    return {
        **data,
        "connectors": {
            name: entry for name, entry in connectors.items() if not _is_shared_entry(entry)
        },
    }


def _name_errors(name: object) -> list[tuple[str, str]]:
    where = f"connectors.{name}"
    errors: list[tuple[str, str]] = []
    if not valid_connector_name(name):
        errors.append(
            (
                BAD_NAME_CODE,
                f"{where}: a connector name becomes a Kubernetes object and DNS label, so it "
                "must be lowercase alphanumeric or dashes, start and end alphanumeric, and be "
                f"at most {_NAME_MAX} characters",
            )
        )
    if name in RESERVED_CONNECTOR_NAMES:
        errors.append(
            (
                RESERVED_NAME_CODE,
                f"{where}: `{name}` is reserved for Curie's own platform MCP server; "
                "rename the connector",
            )
        )
    return errors


def parse_declarations(data: Any) -> tuple[Declarations | None, list[tuple[str, str]]]:
    """Parse ``connectors.yaml`` content strictly; ``(None, errors)`` on any refusal.

    No file is an empty declaration. Content that is not a mapping of
    connectors is the frozen validator's to judge, verbatim.
    """

    if data is None:
        return Declarations(), []
    connectors = data.get("connectors") if isinstance(data, dict) else None
    if not isinstance(connectors, Mapping):
        parsed, frozen = validate_connectors(data)
        if frozen or parsed is None:
            return None, frozen
        return Declarations(dedicated=parsed), []

    errors: list[tuple[str, str]] = []
    shared: list[SharedEntry] = []
    for name, entry in connectors.items():
        if not _is_shared_entry(entry):
            continue
        errors.extend(_name_errors(name))
        try:
            spec = ConnectorSpec.model_validate(entry)
        except ValueError as exc:
            errors.append((INVALID_CODE, f"connectors.{name}: " + str(exc)[:400]))
            continue
        if spec.shared is None:  # pragma: no cover -- exclusive_shared refuses a null
            errors.append((INVALID_CODE, f"connectors.{name}: shared must be nonnull"))
            continue
        shared.append(SharedEntry(name=str(name), application=spec.shared.application))

    dedicated, frozen = validate_connectors(dedicated_subset(data))
    errors.extend(frozen)
    if errors or dedicated is None:
        return None, errors
    return Declarations(
        dedicated=dedicated, shared=tuple(sorted(shared, key=lambda entry: entry.name))
    ), []
