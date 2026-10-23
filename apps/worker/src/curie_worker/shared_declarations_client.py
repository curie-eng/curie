"""The worker's own reader of bundle connector declarations (ADR 0239, cp5).

The worker reads a stored bundle's ``connectors.yaml`` with the parser the API
uses (``curie_internal.shared_declarations``), so the two agree by
construction. A declaration that cannot be read or does not parse raises
``ConnectorDeclarationRefused`` with its code; it is never read as "declares
nothing". Only a bundle with no ``connectors.yaml`` declares nothing.

Shared entries are first narrowed to the acting agent's ``deploy.yaml``
target, read strictly (a bad file refuses rather than reading as "every
connector runs"), and what remains is resolved through the API's
``POST /v1/internal/shared-connectors/resolve`` on the internal worker token.
The API is the one authority on applications and memberships; the worker
holds no copy of either. Every refusal is a typed exception carrying its code.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml
from curie_internal.shared_declarations import (
    SHARED_REFUSAL_CODES,
    Declarations,
    SharedEntry,
    parse_declarations,
)
from plugin_format.connectors import CONNECTORS_FILE
from plugin_format.deploy_targets import connectors_for_agent, validate_deploy_targets
from plugin_format.validate import DEPLOY_FILE
from plugin_format.yaml_loader import DuplicateKeyError, safe_load_unique

RESOLVE_PATH = "/v1/internal/shared-connectors/resolve"
WORKER_TOKEN_HEADER = "X-Curie-Worker-Token"

UNREADABLE = "connectors.unreadable"
WORKER_TOKEN_MISSING = "shared_worker_token_missing"
ACTION_UNSUPPORTED = "shared_connector_action_unsupported"
RESOLVE_MALFORMED = "shared_resolve_malformed"


class ConnectorDeclarationRefused(Exception):
    """A bundle's connector declarations refused. ``str(exc)`` is the code; it carries no value."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code

    def __str__(self) -> str:
        return self.code


class SharedDeclarationRefused(ConnectorDeclarationRefused):
    """A shared entry could not be resolved for this agent."""


class SharedConnectorUnsupported(SharedDeclarationRefused):
    """The worker's reader cannot act through a shared connector (cp5 has no shared executor)."""


@dataclass(frozen=True)
class ResolvedEntry:
    name: str
    application: str
    enabled: bool


def shared_refusal(response: httpx.Response) -> tuple[str, tuple[str, ...] | None] | None:
    """A 4xx that is a shared declaration refusal: its code and declared applications.

    A declaration refusal is recognised by its shape, ``{"detail": {"code",
    ..., "declared"}}``, whatever its code (frozen ``deploy.*`` and dedicated
    ``connectors.*`` codes come back in it too). ``declared`` is None when the
    refusal names no applications: the declaration did not parse, or the
    installation itself refused. A bare ``{"detail": code}`` counts only when
    the code is a shared one, and then names no applications. Anything else is
    None, so the caller's own status handling applies unchanged.
    """

    if not 400 <= response.status_code <= 499:
        return None
    try:
        body = response.json()
    except ValueError:
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, str):
        return (detail, None) if detail in SHARED_REFUSAL_CODES else None
    if not isinstance(detail, dict):
        return None
    code = detail.get("code")
    if not isinstance(code, str) or not code:
        return None
    declared = detail.get("declared")
    if not isinstance(declared, list) or not all(_uuid_text(item) for item in declared):
        return code, None
    return code, tuple(str(item) for item in declared)


def _uuid_text(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


class SharedResolveClient:
    """Resolves an agent's shared entries through the API on the internal worker token.

    Synchronous, as ``HttpManifestSource`` is: its callers run in a worker
    thread. ``client`` is the transport; None builds one per call.
    """

    def __init__(
        self,
        *,
        base_url: str,
        internal_worker_token: str,
        client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._url = base_url.rstrip("/") + RESOLVE_PATH
        self._token = internal_worker_token
        self._client = client
        self._timeout = timeout

    def resolve(self, agent_id: str, entries: Sequence[SharedEntry]) -> tuple[ResolvedEntry, ...]:
        """Every entry resolved for ``agent_id``, or the first refusal, typed with its code.

        A blank worker token refuses before any request. A non-2xx that is
        not a shared refusal raises ``httpx.HTTPStatusError``.
        """

        if not self._token.strip():
            raise SharedDeclarationRefused(WORKER_TOKEN_MISSING)
        body = {
            "agent_id": agent_id,
            "entries": [
                {"name": entry.name, "application": entry.application} for entry in entries
            ],
        }
        headers = {WORKER_TOKEN_HEADER: self._token}
        if self._client is not None:
            response = self._client.post(self._url, json=body, headers=headers)
        else:
            with httpx.Client(timeout=self._timeout) as client:
                response = client.post(self._url, json=body, headers=headers)
        refused = shared_refusal(response)
        if refused is not None:
            raise SharedDeclarationRefused(refused[0])
        response.raise_for_status()
        return _resolved(response.json(), agent_id, entries)


def _resolved(
    body: Any, agent_id: str, entries: Sequence[SharedEntry]
) -> tuple[ResolvedEntry, ...]:
    """The answer, held to exactly the entries asked about for exactly this agent."""

    asked = {entry.name: entry.application for entry in entries}
    items = body.get("entries") if isinstance(body, Mapping) else None
    if (
        not isinstance(body, Mapping)
        or str(body.get("agent_id")) != agent_id
        or not isinstance(items, list)
    ):
        raise SharedDeclarationRefused(RESOLVE_MALFORMED)
    resolved: list[ResolvedEntry] = []
    for item in items:
        if not isinstance(item, Mapping) or not isinstance(item.get("enabled"), bool):
            raise SharedDeclarationRefused(RESOLVE_MALFORMED)
        name, application = item.get("name"), item.get("application")
        if not isinstance(name, str) or not isinstance(application, str):
            raise SharedDeclarationRefused(RESOLVE_MALFORMED)
        if asked.get(name) != application:
            raise SharedDeclarationRefused(RESOLVE_MALFORMED)
        resolved.append(ResolvedEntry(name=name, application=application, enabled=item["enabled"]))
    if sorted(entry.name for entry in resolved) != sorted(asked):
        raise SharedDeclarationRefused(RESOLVE_MALFORMED)
    return tuple(resolved)


def _load(path: Path, unreadable: str, duplicate: str) -> Any:
    try:
        return safe_load_unique(path.read_text(encoding="utf-8"))
    except DuplicateKeyError:
        raise ConnectorDeclarationRefused(duplicate) from None
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        raise ConnectorDeclarationRefused(unreadable) from None


def read_bundle_declarations(root: Path, *, agent_name: str) -> Declarations:
    """An extracted bundle's declarations, narrowed to ``agent_name``'s deploy target.

    No ``connectors.yaml`` is an empty declaration. An unreadable file (bad
    UTF-8, invalid YAML, a duplicate key) refuses ``connectors.unreadable``,
    and a declaration that does not parse refuses with its first code. With
    shared entries, ``deploy.yaml`` is read strictly and any of its errors
    refuses with its code, so a shared alias the agent's target excludes is
    dropped before anything resolves it.
    """

    path = root / CONNECTORS_FILE
    if not path.is_file():
        return Declarations()
    declarations, errors = parse_declarations(_load(path, UNREADABLE, UNREADABLE))
    if declarations is None:
        raise ConnectorDeclarationRefused(errors[0][0] if errors else UNREADABLE)
    if not declarations.shared:
        return declarations
    deploy = root / DEPLOY_FILE
    if not deploy.is_file():
        return declarations
    targets, target_errors = validate_deploy_targets(
        _load(deploy, "deploy.unreadable", "deploy.duplicate_target")
    )
    if target_errors or targets is None:
        raise ConnectorDeclarationRefused(
            target_errors[0][0] if target_errors else "deploy.invalid"
        )
    return declarations.restricted(connectors_for_agent(targets, agent_name))
