"""The Bitbucket Cloud REST 2.0 transport (#4401 decision 13).

Every request carries the access token as a Bearer header and an explicit 15
second timeout, and never ``If-None-Match``: a 304 still counts against the
token's hourly budget. Failures surface only as the classes in
`curie_api.forges.errors`, each with a fixed reason; no error or log record
carries a response body, a request header or the token.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Sequence
from typing import Any
from urllib.parse import urlsplit

import httpx

from curie_api.forges.config import header_safe
from curie_api.forges.errors import NotFound, Unauthorized, Unavailable

logger = logging.getLogger(__name__)

TIMEOUT_S = 15.0
# Listings longer than this many pages are not trusted to be complete.
MAX_PAGES = 50
# What a rate-limited answer says about when to come back; logged, never slept on.
_RATE_LIMIT_PREFIX = "x-ratelimit-"

Params = Sequence[tuple[str, str]]


class CloudTransport:
    """Requests to Bitbucket Cloud as one access token."""

    def __init__(self, client: httpx.AsyncClient, *, entry: str, api_url: str, token: str) -> None:
        self._client = client
        # The configured entry's name, so a log line tells two entries apart.
        self.entry = entry
        self.root = api_url.rstrip("/")
        self._token = token

    def __repr__(self) -> str:
        return f"CloudTransport(entry={self.entry!r}, root={self.root!r})"

    async def request(
        self,
        method: str,
        url: str,
        *,
        what: str,
        params: Params | None = None,
        json: Any = None,
        allow: Collection[int] = (),
    ) -> httpx.Response:
        """One request to ``url`` (absolute, under `root`). A status in
        ``allow`` is returned to the caller; any other non-2xx answer raises."""

        if not header_safe(self._token):
            # httpx would refuse the header and quote it in its own debug log.
            logger.warning(
                "Bitbucket Cloud %r %s %s at %s has a token unfit for a header",
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
                url,
                params=list(params) if params is not None else None,
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
                "Bitbucket Cloud %r %s %s at %s failed in transport: %s",
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
        logger.info("Bitbucket Cloud refused %s %s with %s", method, what, status)
        if status in (401, 403):
            raise Unauthorized(what)
        if status == 404:
            raise NotFound(what)
        raise Unavailable(what)

    async def json(
        self,
        method: str,
        url: str,
        *,
        what: str,
        params: Params | None = None,
        json: Any = None,
    ) -> dict[str, Any]:
        """One JSON object, or `Unavailable` when the answer is not one."""

        response = await self.request(method, url, what=what, params=params, json=json)
        return object_of(response, what)

    async def pages(
        self, url: str, *, what: str, params: Params, strict: bool = False
    ) -> list[dict[str, Any]]:
        """Every value of a paged listing, following the absolute ``next``
        link for at most `MAX_PAGES` pages. A failed page, a ``next`` off this
        API, or a listing longer than `MAX_PAGES` raises `Unavailable`. A value
        that is not an object is skipped, or with ``strict`` raises
        `Unavailable`, for a caller that must not decide without it."""

        values: list[dict[str, Any]] = []
        following: str | None = url
        query: Params | None = params
        for index in range(MAX_PAGES):
            assert following is not None
            try:
                page = await self.json("GET", following, what=what, params=query)
            except NotFound:
                # A listing that vanishes mid-way is a failed page, not a missing one.
                if index == 0:
                    raise
                raise Unavailable("pagination") from None
            except Unavailable as failed:
                if index == 0:
                    raise
                raise Unavailable("pagination") from failed
            listed = page.get("values")
            if not isinstance(listed, list):
                raise Unavailable("pagination")
            if strict and not all(isinstance(value, dict) for value in listed):
                raise Unavailable(what)
            values.extend(value for value in listed if isinstance(value, dict))
            following = page.get("next")
            if following is None:
                return values
            if not isinstance(following, str) or not self._own(following):
                # Never send the token to another origin.
                raise Unavailable("pagination")
            # The `next` link carries the whole query.
            query = None
        # A partial listing could hide the item asked for; decide nothing.
        raise Unavailable("pagination")

    def _own(self, url: str) -> bool:
        root, there = urlsplit(self.root), urlsplit(url)
        return (there.scheme, there.netloc) == (root.scheme, root.netloc) and (
            there.path.startswith(f"{root.path}/")
        )


def object_of(response: httpx.Response, what: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        raise Unavailable(what) from None
    if not isinstance(payload, dict):
        raise Unavailable(what)
    return payload


def _log_rate_limit(status: int, what: str, headers: httpx.Headers) -> None:
    said = " ".join(
        f"{name}={value}"
        for name, value in headers.items()
        if name.lower() == "retry-after" or name.lower().startswith(_RATE_LIMIT_PREFIX)
    )
    logger.warning(
        "Bitbucket Cloud answered %s reading %s; not retrying now (%s)",
        status,
        what,
        said or "no rate limit headers",
    )
