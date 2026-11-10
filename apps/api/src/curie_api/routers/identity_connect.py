"""Private browser confirmation for a sender-bound account-connect capability."""

import hmac
import secrets
from collections.abc import Callable, Coroutine
from html import escape
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.routing import APIRoute

from ..approval_auth import enforce_console_cookie_origin
from ..config import get_settings
from ..deps import SessionDep
from ..identity import connect
from ..models import IdentityNamespace

CONNECT_COOKIE = "__Host-curie_identity_connect"
CSRF_COOKIE = "__Host-curie_identity_connect_csrf"
PROOF_COOKIE = "__Host-curie_identity_connect_proof"
PRIVATE_HEADERS = {
    "Cache-Control": "private, no-store",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": (
        "default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
}


class PrivateConnectRoute(APIRoute):
    """Keep validation/parser errors private as well as successful pages."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def private_handler(request: Request) -> Response:
            try:
                response = await handler(request)
            except RequestValidationError:
                return JSONResponse(
                    {"detail": "invalid account confirmation"},
                    status_code=400,
                    headers=PRIVATE_HEADERS,
                )
            except HTTPException as exc:
                exc.headers = {**(exc.headers or {}), **PRIVATE_HEADERS}
                raise
            response.headers.update(PRIVATE_HEADERS)
            return response

        return private_handler


router = APIRouter(
    prefix="/identity/connect", tags=["identity-connect"], route_class=PrivateConnectRoute
)


def public_api_prefix() -> str:
    """Trusted proxy prefix from the configured OIDC callback, never a return URL."""
    path = urlsplit(get_settings().oidc_redirect_uri).path
    suffix = "/console/oidc/callback"
    return path[: -len(suffix)] if path.endswith(suffix) else ""


def public_path(request: Request, path: str) -> str:
    return public_api_prefix() + path


def connect_path(request: Request, token: str) -> str:
    return public_path(request, f"/identity/connect/{token}")


def set_browser_proof_cookie(response: Response, proof: str) -> None:
    response.set_cookie(
        PROOF_COOKIE, proof, max_age=600, path="/", secure=True, httponly=True, samesite="lax"
    )


def clear_connect_cookies(response: Response) -> None:
    for name in (CONNECT_COOKIE, CSRF_COOKIE, PROOF_COOKIE):
        response.delete_cookie(name, path="/", secure=True, httponly=True, samesite="lax")


def refused() -> HTTPException:
    return HTTPException(400, "invalid or expired account connection", headers=PRIVATE_HEADERS)


@router.get("/{token}", response_class=HTMLResponse, name="identity_connect_page")
async def connect_page(token: str, request: Request, session: SessionDep) -> HTMLResponse:
    try:
        row = await connect.live_token(session, token)
    except connect.ConnectRefused:
        raise refused() from None
    namespace = await session.get(IdentityNamespace, row.identity_namespace_id)
    assert namespace is not None  # live_token checked it
    account = f"{namespace.provider} user {row.provider_native_id} in workspace {namespace.key}"
    principal = await connect.browser_principal(
        session, token=token, proof=request.cookies.get(PROOF_COOKIE, "")
    )
    if principal is not None and (
        principal.tenant_id != row.tenant_id or principal.idp_issuer != row.idp_issuer
    ):
        raise refused()
    csrf = secrets.token_urlsafe(32)
    if principal is None:
        login = escape(public_path(request, "/console/oidc/login"), quote=True)
        action = f'<p><a href="{login}">Sign in with your organization</a></p>'
    else:
        who = escape(principal.display_name or principal.idp_subject)
        target = escape(connect_path(request, token), quote=True)
        action = (
            f"<p>Connect this account to {who}?</p>"
            f'<form method="post" action="{target}/confirm">'
            f'<input type="hidden" name="csrf" value="{csrf}">'
            '<button type="submit">Confirm account connection</button></form>'
        )
    response = HTMLResponse(
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>Connect account</title>'
        f"<h1>Connect account</h1><p>{escape(account)}</p>{action}"
        "<p>Your original request will not run. After connecting, send it again.</p></html>",
        headers=PRIVATE_HEADERS,
    )
    for name, value in ((CONNECT_COOKIE, token), (CSRF_COOKIE, csrf)):
        response.set_cookie(
            name, value, max_age=600, path="/", secure=True, httponly=True, samesite="lax"
        )
    return response


@router.post("/{token}/confirm", response_class=HTMLResponse)
async def confirm_connection(
    token: str,
    request: Request,
    session: SessionDep,
    csrf: Annotated[str, Form(min_length=43, max_length=43)],
) -> HTMLResponse:
    enforce_console_cookie_origin(request)
    bound = request.cookies.get(CONNECT_COOKIE, "")
    expected = request.cookies.get(CSRF_COOKIE, "")
    if (
        not bound
        or not expected
        or not hmac.compare_digest(token.encode(), bound.encode())
        or not hmac.compare_digest(csrf.encode(), expected.encode())
    ):
        raise refused()
    principal = await connect.browser_principal(
        session, token=token, proof=request.cookies.get(PROOF_COOKIE, ""), lock=True
    )
    if principal is None:
        raise HTTPException(401, "sign in required", headers=PRIVATE_HEADERS)
    try:
        await connect.confirm(session, token=token, principal=principal)
        await session.commit()
    except connect.ConnectRefused:
        await session.rollback()
        raise refused() from None
    response = HTMLResponse(
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>Account connected</title>'
        "<h1>Account connected</h1><p>Return to your chat and send your request again.</p></html>",
        headers=PRIVATE_HEADERS,
    )
    clear_connect_cookies(response)
    return response
