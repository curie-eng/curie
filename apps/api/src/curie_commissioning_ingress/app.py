"""Explicit HTTP constructor for the commissioning ingress, @spec PROTECTED-COMMISSIONING-2/4.

The ordinary API never mounts this application; the commissioning process
composition constructs it with its ingress. The request carries only a body,
the signing headers and the source in the path: no field selects a phase,
reply URL, credential, policy generation or target. Every refusal is one fixed
answer that names no step, signature, payload or control fact.
"""

from __future__ import annotations

from curie_api import hook_signing
from curie_protected_hooks.commissioning_campaign_records import CampaignAdmission
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

from .ingress import MAX_PAYLOAD_BYTES, CommissioningIngress

_HEADERS = (
    hook_signing.TIMESTAMP_HEADER,
    hook_signing.DELIVERY_HEADER,
    hook_signing.SIGNATURE_HEADER,
)
# The body bound plus one byte, so the ingress itself refuses an oversized payload.
_READ_LIMIT = MAX_PAYLOAD_BYTES + 1


def create_commissioning_app(ingress: CommissioningIngress) -> FastAPI:
    """One POST route over ``ingress``; no docs, schema or other route."""
    if type(ingress) is not CommissioningIngress:
        raise ValueError("commissioning ingress required")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.post("/v1/commissioning/hooks/{source_id}")
    async def receive(source_id: str, request: Request) -> Response:
        admission: CampaignAdmission | None = None
        try:
            if not request.url.query and all(
                len(request.headers.getlist(name)) == 1 for name in _HEADERS
            ):
                raw = bytearray()
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw) > _READ_LIMIT:
                        break
                admission = await ingress.admit(
                    source_id,
                    body=bytes(raw[:_READ_LIMIT]),
                    timestamp=request.headers[_HEADERS[0]],
                    delivery_id=request.headers[_HEADERS[1]],
                    signature=request.headers[_HEADERS[2]],
                )
        except Exception:  # noqa: BLE001  One fixed refusal excludes signatures, payloads and control facts.
            admission = None
        if admission is None:
            raise HTTPException(403, "Commissioning ingress refused")
        return Response(admission.canonical_bytes, status_code=202, media_type="application/json")

    return app
