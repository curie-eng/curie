"""The Bitbucket Data Center REST transport under ``rest/api/latest`` (#4401 decision 13).

Every request carries the access token as a Bearer header and an explicit 15
second timeout, and never ``If-None-Match``. Failures surface only as the
classes in `curie_api.forges.errors`, each with a fixed reason; no error or log
record carries a response body, a request header or the token.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Collection, Mapping
from typing import Any

import httpx

from curie_api.forges.config import header_safe
from curie_api.forges.errors import NotFound, Unauthorized, Unavailable

logger = logging.getLogger(__name__)

TIMEOUT_S = 15.0
# Listings longer than this many pages are not trusted to be complete.
MAX_PAGES = 50
# What a rate-limited answer says about when to come back; logged, never slept on.
_RATE_LIMIT_HEADERS = (
    "Retry-After",
    "X-RateLimit-Limit",
    "X-RateLimit-Interval-Seconds",
    "X-RateLimit-Fill-Rate",
)


# The REST API every call uses, and the build status API whose commit listing
# is the only one that names every build key on a commit (`code_host.observe_ci`).
REST_API = "/rest/api/latest"
BUILD_STATUS_API = "/rest/build-status/latest"


class DcTransport:
    """Requests to one Bitbucket Data Center as one access token."""

    def __init__(self, client: httpx.AsyncClient, *, entry: str, api_url: str, token: str) -> None:
        self._client = client
        # The configured entry's name, so a log line tells two entries apart.
        self.entry = entry
        self.root = api_url.rstrip("/")
        self._token = token

    def __repr__(self) -> str:
        return f"DcTransport(entry={self.entry!r}, root={self.root!r})"

    async def request(
        self,
        method: str,
        path: str,
        *,
        what: str,
        params: Mapping[str, str | int] | None = None,
        json: Any = None,
        allow: Collection[int] = (),
        api: str = REST_API,
    ) -> httpx.Response:
        """One request to ``path`` under ``api``. A status in ``allow`` is
        returned to the caller; any other non-2xx answer raises."""

        if not header_safe(self._token):
            # httpx would refuse the header and quote it in its own debug log.
            logger.warning(
                "Bitbucket Data Center %r %s %s at %s has a token unfit for a header",
                self.entry,
                method,
                what,
                self.root,
            )
            raise Unavailable("transport")
        headers = {"Accept": "application/json", "Authorization": f"Bearer {self._token}"}
        response = None
        try:
            response = await self._client.request(
                method,
                f"{self.root}{api}{path}",
                params=params,
                json=json,
                headers=headers,
                timeout=TIMEOUT_S,
                follow_redirects=False,
            )
        except httpx.HTTPError as error:
            failure = type(error).__name__
        finally:
            headers.clear()
        if response is None:
            # Raised outside the handler: the httpx error's text can quote the
            # Authorization header, so neither it nor its traceback is kept.
            logger.warning(
                "Bitbucket Data Center %r %s %s at %s failed in transport: %s",
                self.entry,
                method,
                what,
                self.root,
                failure,
            )
            raise Unavailable("transport")
        status = response.status_code
        if status in allow or 200 <= status < 300:
            return response
        if status in (429, 503):
            _log_rate_limit(status, what, response.headers)
            raise Unavailable("rate_limited" if status == 429 else "unavailable")
        logger.info("Bitbucket Data Center refused %s %s with %s", method, what, status)
        if status in (401, 403):
            raise Unauthorized(what)
        if status == 404:
            raise NotFound(what)
        raise Unavailable(what)

    async def json(
        self,
        method: str,
        path: str,
        *,
        what: str,
        params: Mapping[str, str | int] | None = None,
        json: Any = None,
        api: str = REST_API,
    ) -> dict[str, Any]:
        """One JSON object, or `Unavailable` when the answer is not one."""

        response = await self.request(method, path, what=what, params=params, json=json, api=api)
        return object_of(response, what)

    async def put_versioned(self, path: str, *, what: str, json: Any) -> dict[str, Any]:
        """PUT ``json``, which carries the version the caller read. A version
        someone else moved on answers 409, raised as ``Unavailable("stale_version")``
        so the caller retries with a fresh read."""

        response = await self.request("PUT", path, what=what, json=json, allow=(409,))
        if response.status_code == 409:
            raise Unavailable("stale_version")
        return object_of(response, what)

    async def pages(
        self,
        path: str,
        *,
        what: str,
        limit: int,
        params: Mapping[str, str] | None = None,
        api: str = REST_API,
        strict: bool = False,
    ) -> list[dict[str, Any]]:
        """Every value of a paged listing, following ``isLastPage`` and
        ``nextPageStart`` for at most `MAX_PAGES` pages."""

        values: list[dict[str, Any]] = []
        async for page in self.iter_pages(
            path, what=what, limit=limit, params=params, api=api, strict=strict
        ):
            values.extend(page)
        return values

    async def iter_pages(
        self,
        path: str,
        *,
        what: str,
        limit: int,
        params: Mapping[str, str] | None = None,
        api: str = REST_API,
        strict: bool = False,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """The pages of one listing in order, so a caller may stop early. A
        failed page, or a listing longer than `MAX_PAGES`, raises `Unavailable`.
        A value that is not an object is skipped, or with ``strict`` raises
        `Unavailable`, for a caller that must not decide without it."""

        start = 0
        for _ in range(MAX_PAGES):
            try:
                page = await self.json(
                    "GET",
                    path,
                    what=what,
                    params={**(params or {}), "start": start, "limit": limit},
                    api=api,
                )
            except NotFound:
                # A listing that vanishes mid-way is a failed page, not a missing one.
                if start == 0:
                    raise
                raise Unavailable("pagination") from None
            values = page.get("values")
            if not isinstance(values, list):
                raise Unavailable("pagination")
            if strict and not all(isinstance(value, dict) for value in values):
                raise Unavailable(what)
            yield [value for value in values if isinstance(value, dict)]
            if page.get("isLastPage") is True:
                return
            following = page.get("nextPageStart")
            if type(following) is not int or following <= start:
                raise Unavailable("pagination")
            start = following
        # A partial listing could hide the item asked for; decide nothing.
        raise Unavailable("pagination")


def object_of(response: httpx.Response, what: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        raise Unavailable(what) from None
    if not isinstance(payload, dict):
        raise Unavailable(what)
    return payload


def _log_rate_limit(status: int, what: str, headers: httpx.Headers) -> None:
    said = " ".join(f"{name}={headers[name]}" for name in _RATE_LIMIT_HEADERS if name in headers)
    logger.warning(
        "Bitbucket Data Center answered %s reading %s; not retrying now (%s)",
        status,
        what,
        said or "no rate limit headers",
    )
