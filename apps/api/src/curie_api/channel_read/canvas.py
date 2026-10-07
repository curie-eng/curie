"""Canvas list, read and cell edit on the agent's bound channels (ADR 0200, #3819).

Every operation repeats the channel read capability's checks (generation, then
the deployment digest), then requires its own grant from the claims, then
checks the request's shape, all before any provider call. The bound check
needs the provider's metadata: where the canvas is shared is learned from one
metadata lookup, which is the only provider call before a refusal; no download
and no edit precedes it. An edit replaces one table cell a read of the same
canvas recorded in the same logical turn, and is audited before and after the
provider call. Every successful operation costs one of the turn's pages.
Nothing here logs or stores retrieved canvas content or a credential.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import TypeVar

import httpx
from aci_protocol.turn import SLACK_KIND
from channel_protocol import ChannelCapability
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import Settings
from ..identities import slack_bot_tokens
from ..models import AgentChannel, ChannelCanvasEdit
from ..schemas.channel_read import (
    CanvasCell,
    CanvasParagraph,
    CanvasSummary,
    CanvasTable,
    ChannelCanvasRequest,
    ChannelCanvasResult,
    ChannelSelector,
)
from .canvas_sections import CanvasSections
from .errors import ChannelReadRefused
from .ledger import ChannelReadLedger
from .provider_guard import ProviderGuard
from .readers import (
    BindingRoute,
    CanvasCellRecord,
    CanvasDocument,
    CanvasFile,
    CanvasReader,
    canvas_reader_for,
)
from .service import authorized_binding, check_authority, reserve_page
from .slack_canvas import (
    CANVASES_EDIT,
    CONVERSATIONS_INFO,
    FILES_DOWNLOAD,
    FILES_INFO,
    FILES_LIST,
    SlackCanvasReader,
    cell_text_problem,
)
from .token import ChannelReadClaims

T = TypeVar("T")

_GRANT_OF = {"list": "canvasList", "read": "canvasRead", "edit": "canvasEdit"}
# The fields each operation takes besides ``operation``.
_FIELDS = {
    "list": frozenset({"channel"}),
    "read": frozenset({"kind", "canvas_id"}),
    "edit": frozenset({"kind", "canvas_id", "section_id", "text"}),
}
_OPTIONAL = frozenset({"channel", "kind"})


def canvas_readers(settings: Settings, http: httpx.AsyncClient) -> Mapping[str, CanvasReader]:
    """Every surface with canvases, keyed by binding kind."""

    return {SLACK_KIND: SlackCanvasReader(http, slack_bot_tokens(settings))}


def _invalid_identifier(message: str) -> ChannelReadRefused:
    return ChannelReadRefused(422, "invalid_identifier", message)


def _rfc3339(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, UTC).isoformat().replace("+00:00", "Z")


class _Provider:
    """Per operation provider bookkeeping: one attempt charged before the
    first call, and a cooldown per credential and method around every call."""

    def __init__(
        self, guard: ProviderGuard, reader: CanvasReader, claims: ChannelReadClaims
    ) -> None:
        self._guard = guard
        self._reader = reader
        self._claims = claims
        self._charged = False

    async def call(
        self,
        identity: str,
        method: str,
        call: Callable[[], Awaitable[T]],
        *,
        before_call: Callable[[], Awaitable[None]] | None = None,
    ) -> T:
        """``before_call`` runs after every guard check, as the last await
        before the provider call."""

        identity_key = self._reader.identity_key(identity)
        remaining = await self._guard.cooldown_remaining(identity_key, method)
        if remaining is not None:
            raise ChannelReadRefused(
                429,
                "provider_rate_limited",
                "the provider is rate limiting canvas calls; retry later",
                retry_after=remaining,
            )
        if not self._charged:
            if not await self._guard.charge_attempt(self._claims.agent, self._claims.turn):
                raise ChannelReadRefused(
                    429,
                    "attempt_budget_exhausted",
                    "this turn has used its provider read attempts",
                )
            self._charged = True
        if before_call is not None:
            await before_call()
        try:
            return await call()
        except ChannelReadRefused as refused:
            if refused.code == "channel_read.provider_rate_limited":
                await self._guard.cool_down(identity_key, method, refused.retry_after)
            raise


@dataclass(frozen=True)
class _KindBindings:
    kind: str
    # address -> the routes of that address's rows
    routes: dict[str, list[BindingRoute]]

    @property
    def all_routes(self) -> list[BindingRoute]:
        return [route for routes in self.routes.values() for route in routes]


async def _kind_bindings(
    session: AsyncSession, claims: ChannelReadClaims, kind: str | None
) -> _KindBindings:
    if kind is None:
        if claims.default is None:
            raise ChannelReadRefused(
                400, "kind_required", "this turn has no default channel; name a kind"
            )
        kind = claims.default.kind
    rows = (
        await session.scalars(
            select(AgentChannel).where(
                AgentChannel.agent_id == claims.agent, AgentChannel.kind == kind
            )
        )
    ).all()
    if not rows:
        raise ChannelReadRefused(403, "not_bound", "the agent has no binding of that kind")
    routes: dict[str, list[BindingRoute]] = {}
    for row in rows:
        routes.setdefault(row.address, []).append(
            BindingRoute(adapter=row.adapter, endpoint=row.endpoint)
        )
    return _KindBindings(kind=kind, routes=routes)


def _check_fields(body: ChannelCanvasRequest) -> None:
    """A field that does not belong to the operation, or a missing one, is named."""

    allowed = _FIELDS[body.operation]
    for name in ("channel", "kind", "canvas_id", "section_id", "text"):
        present = getattr(body, name) is not None
        if present and name not in allowed:
            raise _invalid_identifier(f"{name} does not belong to a canvas {body.operation}")
        if not present and name in allowed and name not in _OPTIONAL:
            raise _invalid_identifier(f"a canvas {body.operation} needs {name}")


def _cell_records(document: CanvasDocument) -> list[CanvasCellRecord]:
    return [
        cell
        for table in document.tables
        for row in (table.header, *table.rows)
        for cell in row
        if cell.section_id is not None
    ]


def _cell(record: CanvasCellRecord) -> CanvasCell:
    return CanvasCell(section_id=record.section_id, text=record.text, truncated=record.truncated)


async def _list(
    *,
    claims: ChannelReadClaims,
    body: ChannelCanvasRequest,
    session: AsyncSession,
    ledger: ChannelReadLedger,
    guard: ProviderGuard,
    readers: Mapping[str, CanvasReader],
) -> ChannelCanvasResult:
    binding = await authorized_binding(session, claims, body.channel)
    reader = canvas_reader_for(readers, binding.kind, ChannelCapability.CANVAS_READ)
    if reader is None:
        raise ChannelReadRefused(
            409, "capability_unsupported", f"{binding.kind} channels have no canvases"
        )
    identity = reader.identity(binding.routes)
    if not reader.has_identity(identity):
        raise ChannelReadRefused(
            503, "provider_unconfigured", "no provider credential serves this binding"
        )
    await reserve_page(ledger, claims)
    try:
        provider = _Provider(guard, reader, claims)
        member = await provider.call(
            identity,
            CONVERSATIONS_INFO,
            lambda: reader.is_member(identity=identity, channel=binding.address),
        )
        if not member:
            raise _not_member()
        page = await provider.call(
            identity,
            FILES_LIST,
            lambda: reader.list(identity=identity, channel=binding.address),
        )
    except BaseException:
        await ledger.release(claims.agent, claims.turn)
        raise
    return ChannelCanvasResult(
        operation="list",
        canvases=[
            CanvasSummary(id=c.id, title=c.title, created=_rfc3339(c.created))
            for c in page.canvases
        ],
        has_more=page.has_more,
    )


def _not_member() -> ChannelReadRefused:
    return ChannelReadRefused(403, "not_member", "the app is not a member of that channel")


@dataclass(frozen=True)
class _Located:
    file: CanvasFile
    address: str
    identity: str


async def _locate_bound(
    provider: _Provider,
    reader: CanvasReader,
    bindings: _KindBindings,
    canvas_id: str,
) -> _Located:
    """Metadata, then the bound check, the type, and membership: no content call
    happens unless the canvas is shared into a binding the app is a member of."""

    identity = reader.identity(bindings.all_routes)
    found = await provider.call(
        identity, FILES_INFO, lambda: reader.locate(identity=identity, canvas_id=canvas_id)
    )
    if found is None:
        raise ChannelReadRefused(404, "canvas_not_found", "the provider has no such canvas")
    matched = sorted(found.shared_in & bindings.routes.keys())
    if not matched:
        raise ChannelReadRefused(
            403,
            "canvas_not_bound",
            "the canvas is not shared into a channel this agent is bound to",
        )
    if not found.is_canvas:
        raise ChannelReadRefused(422, "not_a_canvas", "that file is not a canvas")
    configured = False
    for address in matched:
        own = reader.identity(bindings.routes[address])
        if not reader.has_identity(own):
            continue
        configured = True
        try:
            member = await provider.call(
                own,
                CONVERSATIONS_INFO,
                partial(reader.is_member, identity=own, channel=address),
            )
        except ChannelReadRefused as refused:
            if refused.code != "channel_read.not_member":
                raise
            member = False
        if member:
            return _Located(file=found, address=address, identity=own)
    if not configured:
        raise ChannelReadRefused(
            503, "provider_unconfigured", "no provider credential serves this binding"
        )
    raise _not_member()


async def authorize_and_canvas(
    *,
    claims: ChannelReadClaims,
    body: ChannelCanvasRequest,
    session: AsyncSession,
    ledger: ChannelReadLedger,
    guard: ProviderGuard,
    sections: CanvasSections,
    settings: Settings,
    readers: Mapping[str, CanvasReader],
) -> ChannelCanvasResult:
    """One canvas operation, refused in the ADR 0200 order or performed."""

    await check_authority(session, ledger, claims)
    op = body.operation
    if _GRANT_OF[op] not in claims.grants:
        raise ChannelReadRefused(
            403, f"canvas_{op}_not_granted", f"the bundle does not grant canvas {op}"
        )
    _check_fields(body)
    if op == "list":
        return await _list(
            claims=claims,
            body=body,
            session=session,
            ledger=ledger,
            guard=guard,
            readers=readers,
        )

    bindings = await _kind_bindings(session, claims, body.kind)
    canvas_id = body.canvas_id
    assert canvas_id is not None  # _check_fields requires it
    known = readers.get(bindings.kind)
    if known is not None and not known.valid_canvas_id(canvas_id):
        raise _invalid_identifier("canvas_id is not a canvas id")
    section_id = body.section_id
    text = body.text
    if op == "edit":
        assert section_id is not None and text is not None  # _check_fields requires them
        if known is not None and not known.valid_section_id(section_id):
            raise _invalid_identifier("section_id is not a canvas section id")
        if (problem := cell_text_problem(text)) is not None:
            raise ChannelReadRefused(422, "cell_text_invalid", problem)
        if not await sections.was_read(claims.agent, claims.turn, canvas_id, section_id):
            raise ChannelReadRefused(
                409,
                "section_not_read",
                "edit only a cell a read of this canvas returned in this turn",
            )
    capability = ChannelCapability.CANVAS_EDIT if op == "edit" else ChannelCapability.CANVAS_READ
    reader = canvas_reader_for(readers, bindings.kind, capability)
    if reader is None:
        raise ChannelReadRefused(
            409, "capability_unsupported", f"{bindings.kind} channels cannot {op} canvases"
        )
    if not reader.has_identity(reader.identity(bindings.all_routes)):
        raise ChannelReadRefused(
            503, "provider_unconfigured", "no provider credential serves this binding"
        )
    await reserve_page(ledger, claims)
    try:
        provider = _Provider(guard, reader, claims)
        located = await _locate_bound(provider, reader, bindings, canvas_id)
        own = located.identity
        document = await provider.call(
            own, FILES_DOWNLOAD, lambda: reader.document(identity=own, file=located.file)
        )
        if op == "read":
            await sections.record(
                claims.agent,
                claims.turn,
                canvas_id,
                (cell.section_id for cell in _cell_records(document) if cell.section_id),
            )
            return ChannelCanvasResult(
                operation="read",
                canvas_id=canvas_id,
                title=located.file.title,
                tables=[
                    CanvasTable(
                        header=[_cell(c) for c in table.header],
                        rows=[[_cell(c) for c in row] for row in table.rows],
                    )
                    for table in document.tables
                ],
                paragraphs=[
                    CanvasParagraph(section_id=p.section_id, text=p.text, truncated=p.truncated)
                    for p in document.paragraphs
                ],
            )
        assert section_id is not None and text is not None
        await _edit(
            provider=provider,
            reader=reader,
            session=session,
            ledger=ledger,
            claims=claims,
            kind=bindings.kind,
            located=located,
            document=document,
            section_id=section_id,
            text=text,
        )
    except BaseException:
        await ledger.release(claims.agent, claims.turn)
        raise
    return ChannelCanvasResult(
        operation="edit", canvas_id=canvas_id, section_id=section_id, edited=True
    )


async def _edit(
    *,
    provider: _Provider,
    reader: CanvasReader,
    session: AsyncSession,
    ledger: ChannelReadLedger,
    claims: ChannelReadClaims,
    kind: str,
    located: _Located,
    document: CanvasDocument,
    section_id: str,
    text: str,
) -> None:
    """Audit the attempt, replace the cell, then settle only a definite answer."""

    cell = next((c for c in _cell_records(document) if c.section_id == section_id), None)
    if cell is None:
        raise ChannelReadRefused(
            409, "section_not_editable", "that section is no longer a single editable table cell"
        )

    async def recheck_authority() -> None:
        await check_authority(session, ledger, claims)
        await authorized_binding(
            session, claims, ChannelSelector(kind=kind, address=located.address)
        )

    # Authority may have been withdrawn during the lookup and download awaits.
    await recheck_authority()
    row = ChannelCanvasEdit(
        id=uuid.uuid4(),
        agent_id=claims.agent,
        deployment_id=claims.deployment,
        turn=claims.turn,
        kind=kind,
        channel_address=located.address,
        canvas_id=located.file.id,
        section_id=section_id,
        before_text=cell.text,
        after_text=text,
        status="attempted",
    )
    session.add(row)
    await session.commit()
    own = located.identity
    try:
        await provider.call(
            own,
            CANVASES_EDIT,
            lambda: reader.replace_cell(
                identity=own, canvas_id=located.file.id, section_id=section_id, text=text
            ),
            # And again during the audit commit and the cooldown check, so the
            # edit is authorized by the last await before Slack sees it.
            before_call=recheck_authority,
        )
    except ChannelReadRefused as refused:
        if refused.code != "channel_read.edit_outcome_unknown":
            row.status = "failed"
            row.error_code = refused.code
            row.completed_at = datetime.now(UTC)
            await session.commit()
        raise
    row.status = "applied"
    row.completed_at = datetime.now(UTC)
    await session.commit()
