"""Shared connector declarations in stored bundles, resolved against the store (ADR 0239).

``read_declarations`` is the strict reader every post-intake path uses: a
``connectors.yaml`` that does not parse raises ``DeclarationRefused`` with its
code and is never read as an empty connector set. ``resolve`` checks each
shared entry against the shared store in one read transaction: the
installation is usable, the application exists in it and is enabled, and the
deploying agent is a member under exactly the bundle's connector name.

A declaration or resolution refusal answers ``{"detail": {"code",
"application", "connector", "declared"}}`` and is never cached. ``declared``
lists every shared application the parsed bundle declares, so the worker can
tell a refusal that names its applications from one whose declarations are
unknown: it is null when the declaration itself did not parse and for an
installation-level refusal, where nothing about the bundle was decided.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from curie_internal.shared_declarations import Declarations, SharedEntry, parse_declarations
from fastapi import HTTPException, status
from plugin_format import bundle_root
from plugin_format.connectors import CONNECTORS_FILE
from plugin_format.deploy_targets import (
    DeployTargetsFile,
    connectors_for_agent,
    validate_deploy_targets,
)
from plugin_format.shared_connector_render import SharedApplication
from plugin_format.validate import DEPLOY_FILE
from plugin_format.yaml_loader import DuplicateKeyError, safe_load_unique
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import SharedConnectorApplication, SharedConnectorMembership
from .store import NO_STORE, SharedRefusal, usable_installation, with_lock


class DeclarationRefused(SharedRefusal):
    """A declaration or resolution refusal, in its dict form and never cached."""

    def __init__(
        self,
        status_code: int,
        code: str,
        *,
        application: str | None = None,
        connector: str | None = None,
        declared: Iterable[str] | None = None,
    ) -> None:
        self.code = code
        self.application = application
        self.connector = connector
        self.declared = None if declared is None else sorted(set(declared))
        # A ``SharedRefusal`` for every catcher, with the dict body in place of
        # the bare code.
        HTTPException.__init__(
            self,
            status_code,
            detail={
                "code": code,
                "application": application,
                "connector": connector,
                "declared": self.declared,
            },
            headers=dict(NO_STORE),
        )

    def naming(self, declared: Iterable[str]) -> DeclarationRefused:
        """The same refusal, naming the applications the bundle declares."""

        return DeclarationRefused(
            self.status_code,
            self.code,
            application=self.application,
            connector=self.connector,
            declared=declared,
        )

    def __str__(self) -> str:
        if self.connector is None:
            return self.code
        return f"{self.code}: shared connector {self.connector!r} (application {self.application})"


def declared_applications(decls: Declarations) -> list[str]:
    return sorted({entry.application for entry in decls.shared})


def read_declarations(root: Path) -> Declarations:
    """A stored bundle's ``connectors.yaml``, strictly; empty only when the file is absent."""

    path = bundle_root(root) / CONNECTORS_FILE
    if not path.is_file():
        return Declarations()
    unprocessable = status.HTTP_422_UNPROCESSABLE_CONTENT
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except DuplicateKeyError:
        raise DeclarationRefused(unprocessable, "connectors.duplicate_connector") from None
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        raise DeclarationRefused(unprocessable, "connectors.unreadable") from None
    declarations, errors = parse_declarations(data)
    if declarations is None:
        raise DeclarationRefused(unprocessable, errors[0][0])
    return declarations


def read_targets_strict(root: Path) -> DeployTargetsFile | None:
    """A stored bundle's ``deploy.yaml``; None only when the file is absent.

    For bundles with shared entries, where ``bundles.read_deploy_targets``'s
    None on a bad file would read as "every connector runs".
    """

    path = bundle_root(root) / DEPLOY_FILE
    if not path.is_file():
        return None
    unprocessable = status.HTTP_422_UNPROCESSABLE_CONTENT
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except DuplicateKeyError:
        raise DeclarationRefused(unprocessable, "deploy.duplicate_target") from None
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        raise DeclarationRefused(unprocessable, "deploy.unreadable") from None
    parsed, errors = validate_deploy_targets(data)
    if errors or parsed is None:
        raise DeclarationRefused(unprocessable, errors[0][0] if errors else "deploy.invalid")
    return parsed


def for_agent(root: Path, agent_name: str) -> tuple[Declarations, list[str]]:
    """The declarations ``agent_name``'s target runs, and every application the bundle declares.

    Parse, then restrict to the agent's target, then (the caller) resolve what
    remains: an agent whose target excludes a shared alias needs no membership.
    """

    decls = read_declarations(root)
    declared = declared_applications(decls)
    if not decls.shared:
        return decls, declared
    try:
        targets = read_targets_strict(root)
    except DeclarationRefused as refused:
        raise refused.naming(declared) from None
    return decls.restricted(connectors_for_agent(targets, agent_name)), declared


@dataclass(frozen=True)
class ResolvedShared:
    name: str
    application: SharedApplication
    enabled: bool


async def resolve(
    session: AsyncSession,
    agent_id: uuid.UUID,
    entries: Sequence[SharedEntry],
    *,
    declared: Iterable[str] | None = None,
) -> tuple[ResolvedShared, ...]:
    """Every entry against the store, in name order, stopping at the first refusal.

    ``declared`` is what a refusal names; it defaults to the entries' own
    applications.
    """

    ordered = sorted(entries, key=lambda entry: entry.name)
    if not ordered:
        return ()
    named = list(declared) if declared is not None else [e.application for e in ordered]
    try:
        installation = usable_installation()
    except SharedRefusal as refused:
        raise DeclarationRefused(refused.status_code, str(refused.detail)) from None

    conflict = status.HTTP_409_CONFLICT
    resolved: list[ResolvedShared] = []
    for entry in ordered:

        def refusal(status_code: int, code: str, entry: SharedEntry = entry) -> DeclarationRefused:
            return DeclarationRefused(
                status_code,
                code,
                application=entry.application,
                connector=entry.name,
                declared=named,
            )

        application_id = uuid.UUID(entry.application)
        # FOR SHARE, the lock order's first row: a disable waits for this read.
        application: SharedConnectorApplication | None = await session.scalar(
            with_lock(
                select(SharedConnectorApplication).where(
                    SharedConnectorApplication.id == application_id
                ),
                "share",
            )
        )
        # Unknown and another installation's application share one code.
        if application is None or application.installation_id != installation:
            raise refusal(conflict, "shared_application_unknown")
        if not application.enabled:
            raise refusal(conflict, "shared_application_disabled")
        member = await session.scalar(
            select(SharedConnectorMembership.alias).where(
                SharedConnectorMembership.application_id == application_id,
                SharedConnectorMembership.agent_id == agent_id,
                SharedConnectorMembership.alias == entry.name,
            )
        )
        if member is None:
            raise refusal(conflict, "shared_application_not_member")
        try:
            spec = SharedApplication.model_validate_json(json.dumps(application.spec))
        except ValueError:
            raise refusal(
                status.HTTP_500_INTERNAL_SERVER_ERROR, "shared_application_invalid"
            ) from None
        resolved.append(ResolvedShared(name=entry.name, application=spec, enabled=True))
    return tuple(resolved)


def entries_body(resolved: Sequence[ResolvedShared]) -> list[dict[str, Any]]:
    return [
        {"name": item.name, "application": item.application.application, "enabled": item.enabled}
        for item in resolved
    ]
