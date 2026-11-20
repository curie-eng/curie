"""Private browser confirmation for a sender-bound account-connect capability."""

import hmac
import secrets
from collections.abc import Callable, Coroutine, Mapping
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..approval_auth import enforce_console_cookie_origin
from ..config import get_settings
from ..deps import SessionDep
from ..identity import connect
from ..models import IdentityNamespace
from .identity_connect_pages import (
    REFUSALS,
    STYLE_SRC,
    ConfirmForm,
    SignIn,
    SlackLabels,
    render_connect_page,
    render_connected_page,
    render_refusal_page,
)

CONNECT_COOKIE = "__Host-curie_identity_connect"
CSRF_COOKIE = "__Host-curie_identity_connect_csrf"
PROOF_COOKIE = "__Host-curie_identity_connect_proof"
PRIVATE_HEADERS = {
    "Cache-Control": "private, no-store",
    # Fetch sets a non-GET Origin to the value null under no-referrer, which
    # the console origin check rejects. strict-origin keeps the real Origin on
    # a same-scheme POST and sends only the origin, never the token path, as Referer.
    # https://fetch.spec.whatwg.org/#append-a-request-origin-header
    "Referrer-Policy": "strict-origin",
    "Content-Security-Policy": (
        # The one inline stylesheet is allowed by its hash, nothing else.
        f"default-src 'none'; style-src {STYLE_SRC}; form-action 'self'; "
        "frame-ancestors 'none'; base-uri 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
}


def wants_html(request: Request) -> bool:
    """True when Accept names the text/html media range itself with a nonzero q.

    `*/*` and parameters that merely contain the text do not count: only a
    browser that asks for a page gets one.
    """
    for part in request.headers.get("accept", "").split(","):
        media, *params = (piece.strip() for piece in part.split(";"))
        if media.lower() != "text/html":
            continue
        quality = 1.0
        for param in params:
            name, _, value = param.partition("=")
            if name.strip().lower() == "q":
                try:
                    quality = float(value)
                except ValueError:
                    quality = 0.0
        if quality > 0:
            return True
    return False


def refusal_page(status: int, headers: Mapping[str, str] | None = None) -> HTMLResponse:
    return HTMLResponse(
        render_refusal_page(status),
        status_code=status,
        headers={**(headers or {}), **PRIVATE_HEADERS},
    )


class PrivateConnectRoute(APIRoute):
    """Keep validation/parser errors private as well as successful pages.

    A browser (Accept includes text/html) gets a 400, 401 or 403 as a styled
    page; every other client keeps the JSON detail.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def private_handler(request: Request) -> Response:
            try:
                response = await handler(request)
            except RequestValidationError:
                if wants_html(request):
                    return refusal_page(400)
                return JSONResponse(
                    {"detail": "invalid account confirmation"},
                    status_code=400,
                    headers=PRIVATE_HEADERS,
                )
            except StarletteHTTPException as exc:
                # The base class: a malformed multipart body raises Starlette's,
                # not FastAPI's subclass, and must stay private too.
                if exc.status_code in REFUSALS and wants_html(request):
                    return refusal_page(exc.status_code, exc.headers)
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
    personal = get_settings().factory_model_auth_mode == "personal"
    slack = (
        SlackLabels(row.slack_display_name, row.slack_handle, row.slack_workspace_name)
        if personal and namespace.provider == "slack"
        else None
    )
    principal = await connect.browser_principal(
        session, token=token, proof=request.cookies.get(PROOF_COOKIE, "")
    )
    if principal is not None and (
        principal.tenant_id != row.tenant_id or principal.idp_issuer != row.idp_issuer
    ):
        raise refused()
    csrf = secrets.token_urlsafe(32)
    action: SignIn | ConfirmForm
    if principal is None:
        action = SignIn(public_path(request, "/console/oidc/login"))
    else:
        action = ConfirmForm(
            who=principal.display_name or principal.idp_subject,
            target=connect_path(request, token),
            csrf=csrf,
        )
    response = HTMLResponse(
        render_connect_page(
            provider=namespace.provider,
            user_id=row.provider_native_id,
            workspace_id=namespace.key,
            slack=slack,
            action=action,
        ),
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
) -> Response:
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
    response: Response
    if get_settings().factory_model_auth_mode == "personal":
        response = RedirectResponse(
            "/settings/claude-plan", status_code=303, headers=PRIVATE_HEADERS
        )
    else:
        response = HTMLResponse(render_connected_page(), headers=PRIVATE_HEADERS)
    clear_connect_cookies(response)
    return response
