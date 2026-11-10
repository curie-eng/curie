"""A read-only MCP connector for GitHub activity across repositories over a window.

Why this exists
---------------
A program manager agent runs on a schedule and answers one question each time:
what moved since I last looked? That means merged pull requests, issues opened
and closed, and milestone movement, across several repositories at once, bounded
by the time of its previous run.

The off-the-shelf server in `examples/github-issues` does not fit that shape.
It reads one repository per call, so a five-repo summary is fifteen calls the
model has to remember to make, and it has no notion of a window, so "since last
Monday" is the model filtering timestamps by eye. Its token also sits in the
sandbox, which is the wrong place for a credential that a prompt-injectable
agent never needs to see.

Shape
-----
Same contract as every other hosted connector here: this process holds the
token, and the sandbox learns a URL and nothing else. The bundle delivers the
token as a `from_secret` reference, so the renderer derives no Bearer header
into the sandbox; the only copy outside the Secret is this process's
environment. One tool, `repository_activity`, reads every listed repository for
one half-open window and returns the four lists per repository.

Why a write is refused three times over
---------------------------------------
* **This process can only GET.** There is one network helper, `_get`, and it
  calls `httpx.get`. No other method is reachable from any tool, so there is no
  code path that turns a request into a write.
* **The bundle's toolPolicy allows only this tool.** A tool name the policy does
  not match fails closed, so even a connector that grew a write tool by mistake
  would not reach the agent.
* **The credential is read only.** It is minted with issues, pull requests, and
  metadata read permissions, so GitHub itself answers 403 to a write made with
  it. That is the real boundary; the other two keep a write from being
  attempted at all.

`readOnlyHint` on the tool is a hint to the model, not one of the three. It is
also not a safety proof on its own: a tool can return a credential and still be
a read. Nothing here returns configuration, and the token is never echoed, not
in a result and not in an error: every diagnostic built from upstream text (an
error body, a non JSON body, a transport exception) passes through `_redact`.

Closures are history, not current state
---------------------------------------
An issue closed inside the window and reopened since reads as open on the issues
endpoint, and one closed again later carries the later `closed_at`. So
`closed_issues` merges the row's `closed_at` with in-window `closed` events from
the events feed, one entry per issue at its latest in-window closure.
"""

# NOTE: no `from __future__ import annotations` here, deliberately. It turns
# every annotation into a string, and the MCP server introspects tool signatures
# with `issubclass(param.annotation, Context)`, which then raises
# `TypeError: issubclass() arg 1 must be a class` at import time, before the
# server ever binds a port. Python 3.12 parses `list[str] | None` natively, so
# the import buys nothing here and costs the whole process.

import logging
import os
import re
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

log = logging.getLogger("github-activity-mcp")

API_URL = (os.environ.get("GITHUB_API_URL") or "https://api.github.com").rstrip("/")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
TIMEOUT = float(os.environ.get("GITHUB_TIMEOUT_SECONDS") or "30")
PER_PAGE = 100


def _page_cap(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError:
        return 10
    return max(1, value)


# A CEILING ON PAGES PER ENDPOINT PER REPOSITORY, and it is never silent.
#
# The issues endpoint is bounded by `since`, but a busy repository can still
# return thousands of rows touched in a long window, and the events feed has no
# `since` filter at all. Ten pages of 100 is a large week for most repositories.
# When the cap is hit while GitHub still offers a next page, the endpoint is
# named in `truncated`, because a quietly short list reads as a quiet week.
MAX_PAGES = _page_cap(os.environ.get("GITHUB_MAX_PAGES") or "10")

# owner/name, each part GitHub's own character set. Anything else is refused
# before a URL is built: "acme/api/issues" would otherwise address a different
# endpoint with the operator's token.
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")

mcp = MCPServer("github")

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=True)


def _valid_repo(name: str) -> bool:
    if not _REPO_RE.match(name):
        return False
    return all(part.strip(".") for part in name.split("/"))


def _allowlist() -> list[str]:
    raw = os.environ.get("GITHUB_REPOSITORIES", "")
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


def _format(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_window_bound(value: str, name: str) -> datetime:
    text = value.strip()
    if not text:
        raise ToolError(
            f"{name} is required, as an ISO 8601 timestamp such as 2026-09-01T00:00:00Z."
        )
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ToolError(
            f"{name} {value!r} is not an ISO 8601 timestamp. Pass one such as 2026-09-01T00:00:00Z."
        ) from exc
    if moment.tzinfo is None:
        raise ToolError(
            f"{name} {value!r} has no UTC offset, so it could mean several instants. "
            "Add Z or an offset such as -04:00."
        )
    return moment.astimezone(UTC)


def _parse_github_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _in_window(value: Any, since: datetime, until: datetime) -> bool:
    moment = _parse_github_time(value)
    return moment is not None and since <= moment < until


def _resolve_repositories(repositories: list[str] | None) -> list[str]:
    allowed = _allowlist()
    for entry in allowed:
        if not _valid_repo(entry):
            raise ToolError(
                f"GITHUB_REPOSITORIES contains {entry!r}, which is not owner/name. "
                "The operator has to fix the connector configuration."
            )
    asked = [name.strip() for name in (repositories or []) if name.strip()]
    if not asked:
        if not allowed:
            raise ToolError(
                "no repositories to read: pass repositories as a list of owner/name, "
                "because this connector has no configured default set."
            )
        asked = allowed

    for name in asked:
        if not _valid_repo(name):
            raise ToolError(f"{name!r} is not a repository name. Pass owner/name, e.g. acme/api.")
    if allowed:
        permitted = {entry.lower() for entry in allowed}
        refused = [name for name in asked if name.lower() not in permitted]
        if refused:
            raise ToolError(
                f"{', '.join(refused)} is not in this connector's configured repositories "
                f"({', '.join(allowed)}). Nothing was read."
            )

    unique: dict[str, str] = {}
    for name in asked:
        unique.setdefault(name.lower(), name)
    return list(unique.values())


def _redact(text: str) -> str:
    """Strip the token from any text that came from upstream.

    GitHub, a proxy in front of it, or an httpx exception string can all echo
    the credential back. Every diagnostic built from such text goes through
    here, before truncation, so a cut can never leave a partial token behind.
    """

    if TOKEN:
        text = text.replace(TOKEN, "[redacted]")
    return text


def _upstream_excerpt(r: httpx.Response) -> str:
    return _redact(r.text)[:300]


def _is_throttled(r: httpx.Response) -> bool:
    """Primary or secondary rate limit, as opposed to a permission refusal.

    The primary limit answers 403 or 429 with x-ratelimit-remaining 0. A
    secondary limit answers 403 or 429 with quota still remaining and a message
    naming the rate limit. Retry-After alone does not mark a 403 as throttling,
    since a permission refusal may carry it too; it only states the wait once
    throttling is otherwise established.
      https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
    """

    if r.status_code == 429:
        return True
    if r.headers.get("x-ratelimit-remaining") == "0":
        return True
    return "rate limit" in r.text.lower()


def _get(url: str, params: dict[str, Any] | None, repo: str) -> httpx.Response:
    """GET one GitHub URL. This is the only network call in the process.

    Every failure is raised as a `ToolError` rather than returned, so a refused
    read never reaches the agent looking like an empty one. The message is a
    sentence the model can repeat, says whether a retry can help, and never
    carries the token: any upstream text in it passes through `_redact`.
    """

    if not TOKEN:
        raise ToolError("not configured: GITHUB_TOKEN is not set on the connector.")
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "curie-github-activity-connector",
    }
    try:
        r = httpx.get(url, params=params, headers=headers, timeout=TIMEOUT)
    except httpx.TimeoutException as exc:
        raise ToolError(
            f"GitHub did not respond within {TIMEOUT:g}s while reading {repo}. "
            "Try again, or narrow the window."
        ) from exc
    except httpx.HTTPError as exc:
        # `from None`: the chained exception carries the unredacted string.
        raise ToolError(
            f"could not reach GitHub while reading {repo}: {_redact(str(exc))}"
        ) from None

    if r.status_code == 401:
        raise ToolError(
            "GitHub rejected the connector's token (401). It is missing, expired, or "
            "revoked. This will not fix itself on retry."
        )
    if r.status_code in (403, 429):
        if _is_throttled(r):
            retry_after = r.headers.get("retry-after")
            wait = (
                f"Wait {retry_after}s before retrying."
                if retry_after and retry_after.isdigit()
                else "Wait for the limit to reset before retrying."
            )
            raise ToolError(
                f"GitHub rate limited the connector's token ({r.status_code}) while reading "
                f"{repo}. {wait}"
            )
        raise ToolError(
            f"GitHub refused the connector's token for {repo} ({r.status_code}). The "
            "credential lacks read access to issues, pull requests, or metadata there. "
            "This will not fix itself on retry."
        )
    if r.status_code == 404:
        raise ToolError(
            f"GitHub returned 404 for {repo}. Either it does not exist or the connector's "
            "token cannot see it; GitHub answers 404 rather than 403 for a private "
            "repository outside the token's reach."
        )
    if r.status_code >= 400:
        raise ToolError(
            f"GitHub returned {r.status_code} while reading {repo}: {_upstream_excerpt(r)}"
        )
    return r


def _paginate(
    repo: str,
    endpoint: str,
    params: dict[str, Any],
    stop: Callable[[list[dict[str, Any]]], bool] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Read one endpoint, following Link rel="next", up to MAX_PAGES pages.

    Returns the rows and whether the read was cut short by the page cap. A read
    that ends because GitHub has no next page, or because `stop` says the window
    is covered, is complete; only a cap reached with a next page still on offer
    is truncation.
    """

    url: str | None = f"{API_URL}/repos/{repo}/{endpoint}"
    page_params: dict[str, Any] | None = params
    rows: list[dict[str, Any]] = []
    pages = 0
    while url is not None:
        if pages >= MAX_PAGES:
            return rows, True
        r = _get(url, page_params, repo)
        pages += 1
        try:
            body = r.json()
        except ValueError:
            raise ToolError(
                f"GitHub returned non JSON for {repo} {endpoint}: {_upstream_excerpt(r)}"
            ) from None
        if not isinstance(body, list):
            raise ToolError(f"GitHub returned an unexpected shape for {repo} {endpoint}.")
        page_rows = [row for row in body if isinstance(row, dict)]
        rows.extend(page_rows)
        if stop is not None and stop(page_rows):
            return rows, False
        url = _next_link(r)
        # The next link already carries every query parameter GitHub wants.
        page_params = None
    return rows, False


def _next_link(r: httpx.Response) -> str | None:
    target = r.links.get("next", {}).get("url")
    if not target:
        return None
    # Follow a link only back to the configured API. The token rides on every
    # request, so a next link to any other host would hand it over.
    if not target.startswith(f"{API_URL}/"):
        raise ToolError(
            "GitHub returned a pagination link outside the configured API URL. "
            "Refusing to follow it with the connector's token."
        )
    return target


def _login(user: Any) -> str | None:
    if isinstance(user, dict):
        login = user.get("login")
        if isinstance(login, str):
            return login
    return None


def _milestone_title(milestone: Any) -> str | None:
    if isinstance(milestone, dict):
        title = milestone.get("title")
        if isinstance(title, str):
            return title
    return None


def _issues_since(since: datetime) -> str:
    """The `since` sent to the issues endpoint: one second before the window.

    GitHub returns rows updated AFTER `since`, but the window is inclusive, so a
    row touched exactly at `since` and never again would be dropped upstream.
    Starting a second early and keeping the local [since, until) filter keeps it.
    """

    floor = since.replace(microsecond=0) - timedelta(seconds=1)
    return floor.strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_repository(repo: str, since: datetime, until: datetime) -> dict[str, Any]:
    truncated: list[str] = []

    issues, cut = _paginate(
        repo,
        "issues",
        {
            "state": "all",
            "since": _issues_since(since),
            "sort": "updated",
            "direction": "desc",
            "per_page": PER_PAGE,
        },
    )
    if cut:
        truncated.append("issues")

    merged: list[dict[str, Any]] = []
    opened: list[dict[str, Any]] = []
    # One closure per issue number, at its latest in-window closure. Seeded from
    # the rows' closed_at, then completed from `closed` events below, because a
    # row shows current state only.
    closed: dict[Any, dict[str, Any]] = {}
    pull_numbers: set[Any] = set()
    for row in issues:
        pull = row.get("pull_request")
        common = {
            "number": row.get("number"),
            "title": row.get("title"),
            "author": _login(row.get("user")),
        }
        link = {
            "url": row.get("html_url"),
            "milestone": _milestone_title(row.get("milestone")),
        }
        if isinstance(pull, dict):
            pull_numbers.add(row.get("number"))
            if _in_window(pull.get("merged_at"), since, until):
                merged.append(common | {"merged_at": pull.get("merged_at")} | link)
            continue
        if _in_window(row.get("created_at"), since, until):
            opened.append(common | {"created_at": row.get("created_at")} | link)
        if _in_window(row.get("closed_at"), since, until):
            closed[row.get("number")] = (
                common
                | {"closed_at": row.get("closed_at"), "state_reason": row.get("state_reason")}
                | link
            )

    # The events feed is newest first and has no `since` filter, so once a page
    # reaches back past `since` the window is covered and paging stops.
    def _covered(page: list[dict[str, Any]]) -> bool:
        for event in page:
            moment = _parse_github_time(event.get("created_at"))
            if moment is not None and moment < since:
                return True
        return False

    events, cut = _paginate(repo, "issues/events", {"per_page": PER_PAGE}, stop=_covered)
    if cut:
        truncated.append("events")

    changes: list[dict[str, Any]] = []
    for event in events:
        kind = event.get("event")
        if kind not in ("milestoned", "demilestoned", "closed"):
            continue
        if not _in_window(event.get("created_at"), since, until):
            continue
        issue = event.get("issue") if isinstance(event.get("issue"), dict) else {}
        if kind == "closed":
            _record_closure(closed, pull_numbers, event, issue)
            continue
        changes.append(
            {
                "number": issue.get("number"),
                "kind": "pull_request" if "pull_request" in issue else "issue",
                "title": issue.get("title"),
                "event": kind,
                "milestone": _milestone_title(event.get("milestone")),
                "actor": _login(event.get("actor")),
                "at": event.get("created_at"),
            }
        )

    milestones_raw, cut = _paginate(repo, "milestones", {"state": "all", "per_page": PER_PAGE})
    if cut:
        truncated.append("milestones")

    milestones: list[dict[str, Any]] = []
    for milestone in milestones_raw:
        if _in_window(milestone.get("closed_at"), since, until):
            change = "closed"
        elif _in_window(milestone.get("created_at"), since, until):
            change = "created"
        else:
            continue
        milestones.append(
            {
                "number": milestone.get("number"),
                "title": milestone.get("title"),
                "state": milestone.get("state"),
                "created_at": milestone.get("created_at"),
                "closed_at": milestone.get("closed_at"),
                "due_on": milestone.get("due_on"),
                "change": change,
            }
        )

    return {
        "repository": repo,
        "merged_pull_requests": merged,
        "opened_issues": opened,
        "closed_issues": list(closed.values()),
        "milestone_changes": changes,
        "milestones": milestones,
        "truncated": truncated,
    }


def _record_closure(
    closed: dict[Any, dict[str, Any]],
    pull_numbers: set[Any],
    event: dict[str, Any],
    issue: dict[str, Any],
) -> None:
    """Fold one in-window `closed` event into the per-issue closures.

    Pull requests are skipped: a closed PR is either merged (listed elsewhere)
    or abandoned (not listed). An issue already recorded keeps its title and
    author and moves to this closure only if it is later than the one recorded.
    The closure's state_reason comes from the event; the issue row's current
    state_reason describes only its current closure.
    """

    number = issue.get("number")
    if number is None or "pull_request" in issue or number in pull_numbers:
        return
    at = event.get("created_at")
    moment = _parse_github_time(at)
    known = closed.get(number)
    if known is not None:
        recorded = _parse_github_time(known.get("closed_at"))
        if recorded is not None and moment is not None and moment <= recorded:
            return
        closed[number] = known | {"closed_at": at, "state_reason": event.get("state_reason")}
        return
    closed[number] = {
        "number": number,
        "title": issue.get("title"),
        "author": _login(issue.get("user")),
        "closed_at": at,
        "state_reason": event.get("state_reason"),
        "url": issue.get("html_url"),
        "milestone": _milestone_title(issue.get("milestone")),
    }


@mcp.tool(annotations=READ_ONLY)
def repository_activity(
    since: str,
    until: str = "",
    repositories: list[str] | None = None,
) -> dict[str, Any]:
    """Read what moved in GitHub repositories during one time window.

    For each repository, returns pull requests merged, issues opened, issues
    closed, issues and pull requests added to or removed from a milestone, and
    milestones created or closed, all within `since` (inclusive) to `until`
    (exclusive).

    `since` and `until` are ISO 8601 timestamps with an offset, e.g.
    `2026-09-01T00:00:00Z` or `2026-08-31T20:00:00-04:00`. Omit `until` to read
    up to now. For a recurring summary, pass the previous run's `until` as this
    run's `since` so nothing falls between runs.

    `repositories` is a list of `owner/name`. Omit it to read the connector's
    configured set; a repository outside that set is refused.

    Each repository carries a `truncated` list. When it names an endpoint
    (`issues`, `events`, or `milestones`), that endpoint had more pages than
    the connector reads, so its lists are incomplete: say so, and narrow the
    window, rather than reporting the short list as the whole story.

    Pull requests closed without merging are not listed, and pull requests never
    appear among the issues.
    """

    since_at = _parse_window_bound(since, "since")
    until_at = _parse_window_bound(until, "until") if until.strip() else datetime.now(UTC)
    if since_at >= until_at:
        raise ToolError(
            f"the window is empty: since {_format(since_at)} is not before until "
            f"{_format(until_at)}."
        )
    repos = _resolve_repositories(repositories)

    return {
        "window": {"since": _format(since_at), "until": _format(until_at)},
        "repositories": [_read_repository(repo, since_at, until_at) for repo in repos],
    }


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)
    if not TOKEN:
        # Refuse to start rather than serve a tool that fails on every call. A
        # connector that answers "not configured" to everything looks healthy to
        # Kubernetes and is useless to the agent.
        log.error("refusing to start: missing GITHUB_TOKEN")
        return 1
    allowed = _allowlist()
    log.info(
        "github activity connector reading %s via %s",
        ", ".join(allowed) if allowed else "repositories named per call",
        API_URL,
    )
    mcp.run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
