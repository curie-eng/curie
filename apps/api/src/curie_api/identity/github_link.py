"""One-use GitHub account proof for a Console principal (ADR 0281)."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import HTTPException

from ..config import Settings
from ..forges.registry import forges_registry

CALLBACK = "/identity/me/github-link/callback"
TTL_SECONDS = 600


@dataclass(frozen=True)
class Configuration:
    host: str
    web: str
    api: str
    callback: str


def personal_linking_enabled(settings: Settings) -> bool:
    return (
        settings.factory_model_auth_mode == "personal"
        and bool(settings.oidc_issuer)
        and urlsplit(settings.oidc_redirect_uri).scheme == "https"
    )


def configuration(settings: Settings) -> Configuration | None:
    if not personal_linking_enabled(settings):
        return None
    if (
        not settings.factory_github_app_client_id
        or not settings.factory_github_app_client_secret.get_secret_value()
    ):
        return None
    host = forges_registry(settings).github_code_host()
    if host is None:
        return None
    redirect = urlsplit(settings.oidc_redirect_uri)
    prefix = redirect.path.removesuffix("/console/oidc/callback")
    if prefix == redirect.path:
        return None
    callback = urlunsplit((redirect.scheme, redirect.netloc, prefix + CALLBACK, "", ""))
    return Configuration(
        host.config.host, host.html_base, host.config.api_url.rstrip("/"), callback
    )


def require_configuration(settings: Settings) -> Configuration:
    configured = configuration(settings)
    if configured is None:
        raise HTTPException(404, "Not Found")
    return configured


class ProviderRefused(Exception):
    pass


async def verified_account(settings: Settings, config: Configuration, code: str) -> tuple[str, str]:
    """Read only the account identity, then revoke before returning its proof."""
    verify: ssl.SSLContext | bool = True
    if settings.code_host_ca_bundle:
        verify = ssl.create_default_context(cafile=settings.code_host_ca_bundle)
    async with httpx.AsyncClient(verify=verify, timeout=15, follow_redirects=False) as client:
        try:
            response = await client.post(
                config.web + "/login/oauth/access_token",
                headers={"Accept": "application/json"},
                data={
                    "client_id": settings.factory_github_app_client_id,
                    "client_secret": settings.factory_github_app_client_secret.get_secret_value(),
                    "code": code,
                    "redirect_uri": config.callback,
                },
            )
            response.raise_for_status()
            payload = response.json()
            token = payload.get("access_token") if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise ProviderRefused()
            try:
                user = await client.get(
                    config.api + "/user", headers={"Authorization": f"Bearer {token}"}
                )
                user.raise_for_status()
                account = user.json()
            finally:
                revoked = await client.request(
                    "DELETE",
                    config.api
                    + "/applications/"
                    + settings.factory_github_app_client_id
                    + "/token",
                    auth=(
                        settings.factory_github_app_client_id,
                        settings.factory_github_app_client_secret.get_secret_value(),
                    ),
                    json={"access_token": token},
                )
                revoked.raise_for_status()
            native_id = account.get("id") if isinstance(account, dict) else None
            login = account.get("login") if isinstance(account, dict) else None
            if (
                type(native_id) is not int
                or native_id <= 0
                or not isinstance(login, str)
                or not login
                or len(login) > 256
            ):
                raise ProviderRefused()
            return str(native_id), login
        except (httpx.HTTPError, ValueError, ProviderRefused):
            raise ProviderRefused() from None
