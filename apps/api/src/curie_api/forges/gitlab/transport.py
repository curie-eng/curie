"""The GitLab REST v4 transport every GitLab adapter call goes through (#4399 decisions 5 to 7).

One `GitLabTransport` per adapter instance owns:

1. authentication: the ``PRIVATE-TOKEN`` header, never ``Authorization``;
2. the 15 second timeout and no redirects on every request;
3. the version gate (decision 5): the first call reads ``GET /version``, and a
   server below 17.0 refuses every call with
   ``Unavailable("server_version_unsupported")`` without another request;
4. the rate limit gate (decision 7): a 429 holds every request until
   ``Retry-After`` (60 seconds when absent) and ``RateLimit-Remaining: 0``
   holds them until ``RateLimit-Reset``, each raising
   ``Unavailable("rate_limited")`` without sending;
5. the status map (decision 6) and paged listings that follow ``x-next-page``
   and fail closed.

Errors carry fixed reason codes only, raised ``from None``: never the token, a
header, a URL query or response text. Logs name the method and status only.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

import httpx

from curie_api.forges.config import header_safe
from curie_api.forges.errors import NotFound, Unauthorized, Unavailable
from curie_api.forges.types import Actor

logger = logging.getLogger(__name__)

TIMEOUT_S = 15.0
PER_PAGE = 100
MIN_VERSION = (17, 0)
# A listing longer than this many pages is not trusted to be complete.
MAX_PAGES = 50
# Seconds to hold requests after a 429 that names no Retry-After (decision 7).
DEFAULT_RETRY_AFTER_S = 60
# Developer: the lowest role that may push and start a run (decision 11).
DEVELOPER_ACCESS = 30

TokenSource = Callable[[], Awaitable[str]]
Params = Mapping[str, str | int]

_VERSION = re.compile(r"\s*(\d+)\.(\d+)")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def encode_segment(value: str) -> str:
    """One path segment as GitLab addresses it: every reserved byte escaped."""

    return quote(value, safe="")


def parse_time(value: Any) -> datetime | None:
    """A GitLab timestamp (``2026-10-01T12:00:00.000Z``) as an aware UTC time."""

    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def render_time(moment: datetime) -> str:
    """A time as GitLab renders it: UTC, milliseconds, ``Z``."""

    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def user_id(user: Any) -> str | None:
    """The numeric id of an embedded GitLab user, as text."""

    if isinstance(user, dict) and type(user.get("id")) is int:
        return str(user["id"])
    return None


def actor_of(user: Any) -> Actor | None:
    uid = user_id(user)
    if uid is None:
        return None
    assert isinstance(user, dict)
    login = user.get("username")
    return Actor(uid, login if isinstance(login, str) and login else uid)


def _seconds(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return max(0, int(value.strip()))
    except ValueError:
        return None


class GitLabTransport:
    """Requests to one GitLab API root as one token."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        entry: str,
        api_url: str,
        token: TokenSource,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._client = client
        # The configured entry's name, so a log line tells two entries apart.
        self.entry = entry
        self.root = api_url.rstrip("/")
        self._token = token
        self._clock = clock
        self._wall = wall
        self._ready = False
        self._unsupported = False
        self._not_before: float | None = None
        self._own: Actor | None = None
        self._bots: dict[str, bool] = {}
        # The server's clock from the last answer's ``Date`` header.
        self.server_date: datetime | None = None

    def __repr__(self) -> str:
        return f"GitLabTransport(entry={self.entry!r}, root={self.root!r})"

    # Gates ------------------------------------------------------------------

    def _check_rate(self) -> None:
        if self._not_before is not None:
            if self._clock() < self._not_before:
                raise Unavailable("rate_limited")
            self._not_before = None

    def _hold(self, seconds: float) -> None:
        until = self._clock() + max(0.0, seconds)
        if self._not_before is None or until > self._not_before:
            self._not_before = until

    async def ensure_ready(self) -> None:
        """Decision 5: read the version once; refuse a server below 17.0 for good."""

        if self._unsupported:
            raise Unavailable("server_version_unsupported")
        self._check_rate()
        if self._ready:
            return
        response = await self._send("GET", "/version", params=None, json=None)
        if response.status_code != 200:
            self._raise_for(response.status_code)
        try:
            payload = response.json()
        except ValueError:
            raise Unavailable("server_version_unreadable") from None
        reported = payload.get("version") if isinstance(payload, dict) else None
        matched = _VERSION.match(reported) if isinstance(reported, str) else None
        if matched is None:
            raise Unavailable("server_version_unreadable")
        if (int(matched.group(1)), int(matched.group(2))) < MIN_VERSION:
            self._unsupported = True
            logger.error(
                "GitLab server version %s is below 17.0, the oldest the factory supports",
                reported,
            )
            raise Unavailable("server_version_unsupported")
        self._ready = True

    # Requests ---------------------------------------------------------------

    async def _send(
        self, method: str, path: str, *, params: Params | None, json: Any
    ) -> httpx.Response:
        self._check_rate()
        token = await self._token()
        if not header_safe(token):
            # httpx would refuse the header and quote it in its own debug log.
            logger.warning(
                "GitLab %r %s at %s has a token unfit for a header", self.entry, method, self.root
            )
            raise Unavailable("transport")
        headers = {"PRIVATE-TOKEN": token, "Accept": "application/json"}
        response = None
        try:
            response = await self._client.request(
                method,
                f"{self.root}{path}",
                params=params,
                json=json,
                headers=headers,
                timeout=TIMEOUT_S,
                follow_redirects=False,
            )
        except httpx.TimeoutException as error:
            # A request that never connected was not sent; one that timed out
            # later may have been.
            sent = not isinstance(error, httpx.ConnectTimeout | httpx.PoolTimeout)
            reason, failure = "timeout" if sent else "transport", type(error).__name__
        except httpx.HTTPError as error:
            reason, failure = "transport", type(error).__name__
        finally:
            headers.clear()
        if response is None:
            # Raised outside the handler: the httpx error's text can quote the
            # PRIVATE-TOKEN header, so neither it nor its traceback is kept.
            logger.warning(
                "GitLab %r %s at %s failed in transport: %s", self.entry, method, self.root, failure
            )
            raise Unavailable(reason)
        self._observe(response)
        if response.status_code == 429:
            wait = _seconds(response.headers.get("retry-after"))
            self._hold(DEFAULT_RETRY_AFTER_S if wait is None else wait)
            logger.warning("GitLab answered %s with 429; holding requests", method)
            raise Unavailable("rate_limited")
        return response

    def _observe(self, response: httpx.Response) -> None:
        date = response.headers.get("date")
        if date:
            try:
                self.server_date = parsedate_to_datetime(date).astimezone(UTC)
            except (TypeError, ValueError):
                pass
        if response.headers.get("ratelimit-remaining", "").strip() == "0":
            reset = _seconds(response.headers.get("ratelimit-reset"))
            wait = DEFAULT_RETRY_AFTER_S if reset is None else reset - self._wall().timestamp()
            self._hold(wait)

    @staticmethod
    def _raise_for(status: int) -> None:
        if status == 401:
            raise Unauthorized("unauthorized")
        if status == 403:
            raise Unauthorized("forbidden")
        if status == 404:
            raise NotFound("not_found")
        if status >= 500:
            raise Unavailable("server_error")
        raise Unavailable("rejected")

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Params | None = None,
        json: Any = None,
        allow: Collection[int] = (),
    ) -> httpx.Response:
        """One request to ``api_url + path``. A status in ``allow`` is returned to
        the caller; any other non-2xx answer raises its port error."""

        await self.ensure_ready()
        response = await self._send(method, path, params=params, json=json)
        status = response.status_code
        if 200 <= status < 300 or status in allow:
            return response
        logger.info("GitLab answered %s with %s", method, status)
        self._raise_for(status)
        raise AssertionError("unreachable")

    async def get(self, path: str, params: Params | None = None) -> Any:
        """The decoded JSON of ``GET api_url + path``."""

        return decode(await self.request("GET", path, params=params))

    async def get_object(self, path: str, params: Params | None = None) -> dict[str, Any]:
        found = await self.get(path, params)
        if not isinstance(found, dict):
            raise Unavailable("malformed")
        return found

    async def get_text(self, path: str) -> str:
        return (await self.request("GET", path)).text

    async def write(self, method: str, path: str, *, json: Any) -> dict[str, Any]:
        found = decode(await self.request(method, path, json=json))
        if not isinstance(found, dict):
            raise Unavailable("malformed")
        return found

    async def pages(
        self, path: str, params: Params | None = None
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """The pages of one listing in order, asking `PER_PAGE` items each and
        following ``x-next-page``. A failed page, or more than `MAX_PAGES`,
        raises `Unavailable`, so no caller decides on a partial listing."""

        page = 1
        for _ in range(MAX_PAGES):
            asked = {**(params or {}), "per_page": PER_PAGE, "page": page}
            try:
                response = await self.request("GET", path, params=asked)
            except NotFound:
                if page == 1:
                    raise
                raise Unavailable("pagination") from None
            values = decode(response)
            if not isinstance(values, list):
                raise Unavailable("pagination")
            yield [value for value in values if isinstance(value, dict)]
            following = response.headers.get("x-next-page", "").strip()
            if not following:
                return
            if not following.isdigit() or int(following) <= page:
                raise Unavailable("pagination")
            page = int(following)
        raise Unavailable("pagination")

    async def list_all(self, path: str, params: Params | None = None) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        async for page in self.pages(path, params):
            values.extend(page)
        return values

    # Identities -------------------------------------------------------------

    async def own_user(self) -> Actor:
        """The token's own user (``GET /user``), read once."""

        if self._own is None:
            own = actor_of(await self.get("/user"))
            if own is None:
                raise Unavailable("user")
            self._own = own
        return self._own

    async def is_bot(self, uid: str) -> bool:
        """Whether user ``uid`` is a bot (``GET /users/:id``), cached per user.
        GitLab's event payloads carry no bot flag (decision 10)."""

        if uid not in self._bots:
            if not uid.isdigit():
                return True
            try:
                user = await self.get_object(f"/users/{uid}")
            except NotFound:
                # A deleted user cannot act; treat it as no person.
                self._bots[uid] = True
            else:
                self._bots[uid] = user.get("bot") is True
        return self._bots[uid]

    async def is_person(self, user: Any) -> Actor | None:
        """The actor when a person other than the token's own user, else None."""

        actor = actor_of(user)
        if actor is None or actor.id == (await self.own_user()).id:
            return None
        return None if await self.is_bot(actor.id) else actor

    async def can_write(self, project_id: str, uid: str) -> bool:
        """Decision 11: Developer (30) or higher on the project, inherited
        membership included, and an active user. Not a member is False."""

        if not uid.isdigit():
            return False
        try:
            member = await self.get_object(f"/projects/{project_id}/members/all/{uid}")
        except NotFound:
            return False
        level = member.get("access_level")
        return type(level) is int and level >= DEVELOPER_ACCESS and member.get("state") == "active"


def decode(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        raise Unavailable("malformed") from None
