"""Authenticated HTTP ingress for Curie's neutral reply events."""

import secrets
from typing import Protocol

from channel_protocol import ReplyAck, ReplyEvent
from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import TypeAdapter, ValidationError

_EVENT: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)


class ReplyService(Protocol):
    async def deliver(self, event: ReplyEvent) -> ReplyAck: ...


def create_reply_app(service: ReplyService, adapter_secret: str) -> FastAPI:
    app = FastAPI(title="Curie Discord adapter", docs_url=None, redoc_url=None)

    @app.post("/replies", response_model=ReplyAck)
    async def replies(
        request: Request,
        # Matches the worker's own name for this header exactly
        # (`ADAPTER_SECRET_HEADER` in `apps/worker/src/curie_worker/reply_sink.py`)
        # -- that is the side that actually sends it, so it is the name that
        # governs. A prior mismatch here (`X-Curie-Adapter-Key`) meant every
        # reply delivery 401'd unconditionally: each side's own unit tests
        # passed in isolation because each asserted its OWN (different) name,
        # and only a genuine cross-service round trip ever exercised both at
        # once.
        x_curie_adapter_secret: str | None = Header(default=None),
    ) -> ReplyAck:
        supplied = x_curie_adapter_secret or ""
        if not secrets.compare_digest(supplied, adapter_secret):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid credential")
        try:
            event = _EVENT.validate_json(await request.body())
        except ValidationError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, exc.errors()) from exc
        return await service.deliver(event)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
