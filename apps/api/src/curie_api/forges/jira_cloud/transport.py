"""The Jira Cloud REST v3 transport (#4358 decisions 2, 7, 9 and 22).

Every request carries HTTP basic auth with the bot's Atlassian account email
and API token (https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis/).
The header is built per request from the operation's `TokenSnapshot` (read at
the first request, so a replaced token file is used by the next operation,
#4630) and never stored, logged, or put in an exception: a transport failure
is raised outside the handler with only the name of the operation, so no cause
or context keeps an HTTP library error that echoes the request's headers into
its own text. The log names the API root, the operation and the error class
only.

Status map (decision 9, the port contract): 429 and 503 are `Unavailable` and
are never slept on, whatever ``Retry-After`` says, so the poll pass keeps its
cursor and comes back on its own schedule; 401 and 403 are `Unauthorized`; 404
is `NotFound`; any other non-2xx answer, a transport error or a body that is
not JSON is `Unavailable`.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import secrets
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

import httpx

from curie_api.forges.errors import NotFound, Unauthorized, Unavailable

if TYPE_CHECKING:
    from curie_api.forges.credential_providers import TokenSnapshot

logger = logging.getLogger(__name__)

API_PREFIX = "/rest/api/3"
# A listing that has not ended after this many pages is not trusted to end.
MAX_PAGES = 1000
# Keys `JiraTransport.credential_fingerprint`; never leaves the process.
_FINGERPRINT_KEY = secrets.token_bytes(32)


class Forbidden(Unauthorized):
    """A 403: the credential is valid but may not do this. Callers that count a
    refused write as done (decision 16) tell it from a 401 by this class."""


class JiraTransport:
    """One Jira Cloud API base (classic site or scoped gateway) and its credential."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        entry: str,
        api_url: str,
        username: str,
        token: TokenSnapshot,
    ) -> None:
        self._client = client
        # The configured entry's name, so a log line tells two entries apart.
        self.entry = entry
        self._api = api_url.rstrip("/") + API_PREFIX
        self._username = username
        self._token = token

    def __repr__(self) -> str:
        return f"JiraTransport(entry={self.entry!r}, api={self._api!r})"

    def _authorization(self) -> str:
        pair = f"{self._username}:{self._token.value()}".encode()
        return "Basic " + base64.b64encode(pair).decode()

    def credential_fingerprint(self) -> str:
        """A keyed digest of this operation's credential, so a cached identity
        can tell the token it was verified with from a replacement. The key is
        random per process, so the digest names no token outside it."""

        return hmac.new(
            _FINGERPRINT_KEY, self._authorization().encode(), hashlib.sha256
        ).hexdigest()

    async def request(
        self,
        method: str,
        path: str,
        *,
        what: str,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
    ) -> Any:
        """The JSON answer to ``method`` on ``path`` under ``/rest/api/3``, or
        None for an empty 2xx body. ``what`` names the operation in errors."""

        headers = {"Accept": "application/json", "Authorization": self._authorization()}
        response = None
        try:
            response = await self._client.request(
                method,
                f"{self._api}{path}",
                params=dict(params or {}),
                json=json,
                headers=headers,
                follow_redirects=False,
            )
        except httpx.HTTPError as error:
            failure = type(error).__name__
        if response is None:
            logger.warning(
                "Jira %r %s %s at %s failed in transport: %s",
                self.entry,
                method,
                what,
                self._api,
                failure,
            )
            raise Unavailable(what)
        return _answer(response, what)

    async def anonymous_get(self, url: str, *, what: str) -> Any:
        """GET an absolute URL with no credential (the site's ``tenant_info``)."""

        response = None
        try:
            response = await self._client.get(
                url, headers={"Accept": "application/json"}, follow_redirects=False
            )
        except httpx.HTTPError as error:
            failure = type(error).__name__
        if response is None:
            logger.warning(
                "Jira %r GET %s at %s failed in transport: %s", self.entry, what, self._api, failure
            )
            raise Unavailable(what)
        return _answer(response, what)

    async def get(self, path: str, *, what: str, params: Mapping[str, Any] | None = None) -> Any:
        return await self.request("GET", path, what=what, params=params)

    async def post(
        self, path: str, *, what: str, json: Any, params: Mapping[str, Any] | None = None
    ) -> Any:
        return await self.request("POST", path, what=what, params=params, json=json)

    async def put(
        self, path: str, *, what: str, json: Any, params: Mapping[str, Any] | None = None
    ) -> Any:
        return await self.request("PUT", path, what=what, params=params, json=json)

    # Listings -------------------------------------------------------------

    async def search_jql(
        self, jql: str, fields: list[str], *, page_size: int
    ) -> list[dict[str, Any]]:
        """Every issue the JQL matches, across every ``nextPageToken`` page.

        https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-post
        Any page that fails raises, so a partial listing is never returned.
        """

        issues: list[dict[str, Any]] = []
        token: str | None = None
        for _ in range(MAX_PAGES):
            body: dict[str, Any] = {"jql": jql, "fields": fields, "maxResults": page_size}
            if token is not None:
                body["nextPageToken"] = token
            page = await self.post("/search/jql", what="search", json=body)
            batch = page.get("issues") if isinstance(page, dict) else None
            if not isinstance(batch, list):
                raise Unavailable("search")
            issues.extend(issue for issue in batch if isinstance(issue, dict))
            next_token = page.get("nextPageToken")
            if page.get("isLast") is True or not isinstance(next_token, str) or not next_token:
                return issues
            token = next_token
        raise Unavailable("search")

    async def changelog(
        self, issue_id: str, *, page_size: int, strict: bool = False
    ) -> list[dict[str, Any]]:
        """The issue's whole changelog, oldest first, across every page.

        https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-changelog-get
        A history that is not an object is skipped, or with ``strict`` raises
        `Unavailable`, for a caller that must not decide without it.
        """

        histories: list[dict[str, Any]] = []
        start = 0
        for _ in range(MAX_PAGES):
            page = await self.get(
                f"/issue/{issue_id}/changelog",
                what="changelog",
                params={"startAt": start, "maxResults": page_size},
            )
            values = page.get("values") if isinstance(page, dict) else None
            if not isinstance(values, list):
                raise Unavailable("changelog")
            if strict and not all(isinstance(value, dict) for value in values):
                raise Unavailable("changelog")
            histories.extend(value for value in values if isinstance(value, dict))
            if page.get("isLast") is True:
                return histories
            if not values:
                raise Unavailable("changelog")
            start += len(values)
        raise Unavailable("changelog")

    async def comments(
        self,
        issue_id: str,
        *,
        newest_first: bool,
        page_size: int,
        until: Callable[[dict[str, Any]], bool] | None = None,
        limit: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """The issue's comments in creation order (or newest first) and the total.

        https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
        ``until`` is a predicate on a comment: reading stops at the first one it
        holds for (newest first, nothing older can follow). ``limit`` stops after
        that many comments. Without either, every page is read or it raises.
        """

        comments: list[dict[str, Any]] = []
        start = 0
        for _ in range(MAX_PAGES):
            page = await self.get(
                f"/issue/{issue_id}/comment",
                what="comments",
                params={
                    "orderBy": "-created" if newest_first else "created",
                    "startAt": start,
                    "maxResults": page_size,
                },
            )
            values = page.get("comments") if isinstance(page, dict) else None
            total = page.get("total") if isinstance(page, dict) else None
            if not isinstance(values, list) or not isinstance(total, int):
                raise Unavailable("comments")
            for value in values:
                if not isinstance(value, dict):
                    continue
                if until is not None and until(value):
                    return comments, total
                comments.append(value)
                if limit is not None and len(comments) >= limit:
                    return comments, total
            start += len(values)
            if start >= total:
                return comments, total
            if not values:
                raise Unavailable("comments")
        raise Unavailable("comments")


def _answer(response: httpx.Response, what: str) -> Any:
    status = response.status_code
    if status == 401:
        raise Unauthorized(what)
    if status == 403:
        raise Forbidden(what)
    if status == 404:
        raise NotFound(what)
    if not 200 <= status < 300:
        # 429 and 503 land here too: never slept on, whatever Retry-After says.
        raise Unavailable(what)
    if not response.content:
        return None
    try:
        return response.json()
    except ValueError:
        raise Unavailable(what) from None
