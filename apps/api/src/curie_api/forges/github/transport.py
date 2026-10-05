"""The shared GitHub REST transport: headers, single reads and paged listings."""

from datetime import datetime
from typing import Any

import httpx

from curie_api.github_review_events import FeedbackIgnored, FeedbackUnavailable


def parse_time(value: Any) -> datetime | None:
    """A GitHub ISO-8601 timestamp as an aware datetime, or None."""

    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def github_headers(token: str) -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def get_github_json(
    client: httpx.AsyncClient,
    *,
    api: str,
    token: str,
    path: str,
    refusal: str,
) -> dict[str, Any]:
    """Read one GitHub JSON object. Claimed webhook URLs are never fetched."""

    try:
        response = await client.get(
            f"{api}{path}",
            headers=github_headers(token),
            follow_redirects=False,
        )
    except httpx.HTTPError:
        raise FeedbackUnavailable(refusal) from None
    # A 404 can conceal missing App permissions; it cannot distinguish a
    # deleted resource from a temporary inability to prove current authority.
    if response.status_code in {401, 403, 404, 429} or response.status_code >= 500:
        raise FeedbackUnavailable(refusal)
    if response.status_code != 200:
        raise FeedbackIgnored(refusal)
    try:
        result = response.json()
    except ValueError:
        raise FeedbackIgnored(refusal) from None
    if not isinstance(result, dict):
        raise FeedbackIgnored(refusal)
    return result


def repository_identity_matches(value: Any, *, repository_id: int, repo_full_name: str) -> bool:
    return (
        isinstance(value, dict)
        and type(value.get("id")) is int
        and value["id"] == repository_id
        and isinstance(value.get("full_name"), str)
        and value["full_name"].casefold() == repo_full_name.casefold()
    )


_PER_PAGE = 100
# Listings longer than this many pages are not trusted to be complete.
_MAX_PAGES = 50


class Unavailable(Exception):
    """GitHub could not answer; try the repository again next pass."""


async def get_all(
    client: httpx.AsyncClient, *, api: str, token: str, path: str, params: dict[str, Any]
) -> list[Any]:
    """Every page of one listing, in GitHub's order, or Unavailable."""

    items: list[Any] = []
    for page in range(1, _MAX_PAGES + 1):
        try:
            response = await client.get(
                f"{api}{path}",
                params={**params, "per_page": _PER_PAGE, "page": page},
                headers=github_headers(token),
                follow_redirects=False,
            )
        except httpx.HTTPError:
            raise Unavailable(path) from None
        if response.status_code != 200:
            raise Unavailable(path)
        try:
            result = response.json()
        except ValueError:
            raise Unavailable(path) from None
        if not isinstance(result, list):
            raise Unavailable(path)
        items.extend(result)
        if len(result) < _PER_PAGE:
            return items
    # A partial listing could hide the newest label event; decide nothing.
    raise Unavailable(path)


class PollUnavailable(Exception):
    """GitHub could not answer; leave this repository's cursor where it is."""


async def list_pages(
    client: httpx.AsyncClient,
    *,
    api: str,
    token: str,
    path: str,
    params: dict[str, Any],
    etag: str | None,
) -> tuple[list[Any] | None, str | None]:
    """Pages of one listing. None means the first page was not modified."""

    headers = github_headers(token)
    if etag:
        headers["If-None-Match"] = etag
    items: list[Any] = []
    seen_etag = etag
    for page in range(1, _MAX_PAGES + 1):
        try:
            response = await client.get(
                f"{api}{path}",
                params={**params, "per_page": _PER_PAGE, "page": page},
                headers=headers,
                follow_redirects=False,
            )
        except httpx.HTTPError:
            raise PollUnavailable(path) from None
        if page == 1 and response.status_code == 304:
            return None, response.headers.get("etag") or etag
        if response.status_code != 200:
            raise PollUnavailable(path)
        try:
            result = response.json()
        except ValueError:
            raise PollUnavailable(path) from None
        if not isinstance(result, list):
            raise PollUnavailable(path)
        if page == 1:
            seen_etag = response.headers.get("etag") or etag
        items.extend(result)
        if len(result) < _PER_PAGE:
            return items, seen_etag
        headers = github_headers(token)
    raise PollUnavailable(path)
