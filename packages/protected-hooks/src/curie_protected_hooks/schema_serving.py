"""@spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.resources import files
from types import MappingProxyType

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

_SCHEMA_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")
_TEXT_TYPES = frozenset({"text", "varchar", "bpchar"})
_REQUIRED: dict[str, dict[str, frozenset[str]]] = {
    "agents": {"id": frozenset({"uuid"}), "hook_generation": frozenset({"int4"})},
    "hook_source_policies": {
        "agent_id": frozenset({"uuid"}), "hook": _TEXT_TYPES,
        "generation": frozenset({"int8"}), "operation_id": frozenset({"uuid"}),
        "mode": _TEXT_TYPES, "tool_access": _TEXT_TYPES, "runtime_id": _TEXT_TYPES,
        "qualification_id": _TEXT_TYPES, "bundle_digest": _TEXT_TYPES,
        "legacy_generation": frozenset({"int8"}), "updated_at": frozenset({"timestamptz"}),
    },
    "hook_source_operations": {
        "agent_id": frozenset({"uuid"}), "hook": _TEXT_TYPES,
        "operation_id": frozenset({"uuid"}), "intent_sha256": _TEXT_TYPES,
        "status": _TEXT_TYPES, "generation": frozenset({"int8"}),
        "attempted_at": frozenset({"timestamptz"}),
    },
}


class SchemaServingUnavailable(RuntimeError):
    """@spec PROTECTED-HOOK-SOURCE-2."""

    def __init__(self, code: str) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        self.code = code
        message = (
            "schema_below_min: database schema is below application min"
            if code == "schema_below_min" else code
        )
        super().__init__(message)


@dataclass(frozen=True)
class AppWindow:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    schema_min: str
    schema_head: str

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if not self.schema_min or not self.schema_head:
            raise ValueError("schema_min and schema_head must be non-empty")


@dataclass(frozen=True)
class ServingMetadata:
    """@spec PROTECTED-HOOK-SOURCE-2."""

    window: AppWindow
    parents: Mapping[str, tuple[str, ...]]

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if (
            not isinstance(self.window, AppWindow)
            or type(self.window.schema_min) is not str
            or type(self.window.schema_head) is not str
            or not isinstance(self.parents, Mapping)
            or not self.parents
        ):
            raise SchemaServingUnavailable("schema_metadata_invalid")
        copied: dict[str, tuple[str, ...]] = {}
        for revision, links in self.parents.items():
            if (
                type(revision) is not str or not revision
                or type(links) is not tuple
                or any(type(parent) is not str or not parent for parent in links)
                or len(set(links)) != len(links)
            ):
                raise SchemaServingUnavailable("schema_metadata_invalid")
            copied[revision] = tuple(links)
        if (
            self.window.schema_min not in copied
            or self.window.schema_head not in copied
            or any(parent not in copied for links in copied.values() for parent in links)
        ):
            raise SchemaServingUnavailable("schema_metadata_invalid")
        children: dict[str, list[str]] = {revision: [] for revision in copied}
        counts = {revision: len(links) for revision, links in copied.items()}
        for revision, links in copied.items():
            for parent in links:
                children[parent].append(revision)
        ready = [revision for revision, count in counts.items() if count == 0]
        visited = 0
        while ready:
            parent = ready.pop()
            visited += 1
            for child in children[parent]:
                counts[child] -= 1
                if counts[child] == 0:
                    ready.append(child)
        heads = {revision for revision, links in children.items() if not links}
        if visited != len(copied) or heads != {self.window.schema_head}:
            raise SchemaServingUnavailable("schema_metadata_invalid")
        object.__setattr__(self, "parents", MappingProxyType(copied))

    @property
    def known_revisions(self) -> frozenset[str]:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        return frozenset(self.parents)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SchemaServingUnavailable("schema_metadata_invalid")
        result[key] = value
    return result


def parse_metadata(raw: bytes) -> ServingMetadata:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    try:
        if type(raw) is not bytes or not raw or len(raw) > 65536:
            raise SchemaServingUnavailable("schema_metadata_invalid")
        payload = json.loads(raw, object_pairs_hook=_unique_object)
        if type(payload) is not dict or set(payload) != {
            "schema_min", "schema_head", "revision_parents"
        }:
            raise SchemaServingUnavailable("schema_metadata_invalid")
        if (
            type(payload["schema_min"]) is not str or not payload["schema_min"]
            or type(payload["schema_head"]) is not str or not payload["schema_head"]
            or type(payload["revision_parents"]) is not dict
        ):
            raise SchemaServingUnavailable("schema_metadata_invalid")
        parents = {}
        for revision, links in payload["revision_parents"].items():
            if type(links) is not list:
                raise SchemaServingUnavailable("schema_metadata_invalid")
            parents[revision] = tuple(links)
        return ServingMetadata(AppWindow(payload["schema_min"], payload["schema_head"]), parents)
    except SchemaServingUnavailable:
        raise
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise SchemaServingUnavailable("schema_metadata_invalid") from None


def load_metadata() -> ServingMetadata:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    try:
        raw = files("curie_protected_hooks").joinpath("schema_serving.json").read_bytes()
    except (OSError, ValueError):
        raise SchemaServingUnavailable("schema_metadata_invalid") from None
    return parse_metadata(raw)


def load_window() -> AppWindow:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    return load_metadata().window


def can_serve(
    current: str | None,
    window: AppWindow,
    known_revisions: Iterable[str],
    parents: Mapping[str, tuple[str, ...]] | None = None,
) -> bool:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    if current is None:
        return False
    if current not in set(known_revisions):
        return True
    if current in (window.schema_min, window.schema_head):
        return True
    if parents is None:
        try:
            parents = load_metadata().parents
        except SchemaServingUnavailable:
            return False
    seen: set[str] = set()
    revision: str | None = current
    while revision is not None and revision not in seen:
        if revision == window.schema_min:
            return True
        seen.add(revision)
        links = parents.get(revision, ())
        revision = links[0] if links else None
    return False


async def _source_structure(connection: AsyncConnection) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    try:
        for table, required in _REQUIRED.items():
            columns = ",".join(f'"{column}"' for column in required)
            await connection.execute(text(f'SELECT {columns} FROM curie."{table}" WHERE FALSE'))
            rows = await connection.execute(text(
                "SELECT a.attname,t.typname FROM pg_catalog.pg_attribute a "
                "JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
                "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "JOIN pg_catalog.pg_type t ON t.oid=a.atttypid "
                "JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace "
                "WHERE n.nspname='curie' AND c.relname=:table "
                "AND tn.nspname='pg_catalog' AND a.attnum>0 AND NOT a.attisdropped"
            ), {"table": table})
            actual: dict[str, str] = {row[0]: row[1] for row in rows}
            if any(actual.get(column, "") not in types for column, types in required.items()):
                raise SchemaServingUnavailable("schema_structure_unavailable")
    except SQLAlchemyError:
        raise SchemaServingUnavailable("schema_structure_unavailable") from None


async def _observe_servable(engine: AsyncEngine, *, metadata_schema: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    if type(metadata_schema) is not str or not _SCHEMA_IDENTIFIER.fullmatch(metadata_schema):
        raise SchemaServingUnavailable("schema_metadata_invalid")
    metadata = load_metadata()
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                quoted = connection.dialect.identifier_preparer.quote_identifier(metadata_schema)
                try:
                    result = await connection.execute(text(
                        f'SELECT version_num FROM {quoted}."alembic_version" LIMIT 2'
                    ))
                    rows = result.all()
                except SQLAlchemyError:
                    raise SchemaServingUnavailable("schema_revision_unavailable") from None
                if len(rows) != 1 or type(rows[0][0]) is not str or not rows[0][0]:
                    raise SchemaServingUnavailable("schema_revision_unavailable")
                current = rows[0][0]
                if not can_serve(
                    current, metadata.window, metadata.known_revisions, metadata.parents
                ):
                    raise SchemaServingUnavailable("schema_below_min")
                await _source_structure(connection)
                return current
    except SchemaServingUnavailable:
        raise
    except Exception:  # noqa: BLE001  Credential-bearing transport errors must stay redacted.
        raise SchemaServingUnavailable("schema_probe_unavailable") from None


async def assert_servable(engine: AsyncEngine, *, metadata_schema: str) -> str:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    try:
        async with asyncio.timeout(30):
            return await _observe_servable(engine, metadata_schema=metadata_schema)
    except TimeoutError:
        raise SchemaServingUnavailable("schema_probe_timeout") from None
