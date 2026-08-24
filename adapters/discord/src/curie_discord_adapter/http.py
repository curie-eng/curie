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
        x_curie_adapter_key: str | None = Header(default=None),
    ) -> ReplyAck:
        supplied = x_curie_adapter_key or ""
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
