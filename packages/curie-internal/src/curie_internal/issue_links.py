"""Issue-shaped links in a Slack message, parsed once for every consumer (#5380).

The dispatcher (whether a root mention earns a receipt), the worker (whether a
turn is a kickoff candidate) and the API (which single issue a kickoff names)
all read links through this one function, so the three cannot disagree about
what a message links to. The function only finds and shapes links. Which host
is the configured tracker, and whether the agent serves the repository, is the
API's decision.

Slack wraps every link it detects as ``<URL>`` or ``<URL|label>``; a bare
``http(s)://`` URL is read too. The scan does not special-case the angle form:
a URL ends at the first character a URL cannot contain unescaped (whitespace,
``<``, ``>``, ``|``, a double quote, a backtick and the like), so the target of
``<URL|label>`` and any URL inside its label are both found. A label that
names a different issue than its target therefore reads as two links, which
the API refuses as ``link_multiple`` instead of trusting either.

A URL is a candidate when one of its path segments is ``issues``, ``pull``,
``pulls`` or ``merge_requests`` (case-insensitively, so a near miss refuses
rather than falling through to ordinary chat). A candidate is ``issue`` or
``pull`` only for exactly ``/{owner}/{repo}/issues/{number}`` or
``/{owner}/{repo}/pull[s]/{number}``, optionally followed by one trailing
slash, a query or a fragment; every other candidate is ``malformed``. Owner and
repository take GitHub's name characters (letters, digits, ``.``, ``_``,
``-``). The number is a positive decimal without a leading zero that fits a
signed 32-bit integer, so an unbounded value never reaches a database column.

Equal links (same host, path, kind and number) collapse to the first one, in
order. At most ``MAX_CANDIDATES`` candidates are read; when the text holds
more, one ``malformed`` overflow entry is appended so the result always holds
more than one entry and the API answers ``link_multiple`` rather than silently
judging a truncated list. The function never raises.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

IssueLinkKind = Literal["issue", "pull", "malformed"]

MAX_CANDIDATES = 32

# RFC 3986 unreserved, reserved and percent characters. Anything else ends the
# URL, which is what stops a Slack ``<URL|label>`` token at ``|`` and ``>``.
_URL = re.compile(r"https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+", re.IGNORECASE)
_CANDIDATE_SEGMENTS = frozenset({"issues", "pull", "pulls", "merge_requests"})
_KINDS: dict[str, IssueLinkKind] = {"issues": "issue", "pull": "pull", "pulls": "pull"}
_NAME = re.compile(r"[A-Za-z0-9._-]+")
_NUMBER = re.compile(r"[1-9][0-9]{0,9}")
_MAX_NUMBER = 2**31 - 1


@dataclass(frozen=True)
class IssueLink:
    """One issue-shaped link, as written in the message."""

    host: str
    owner: str
    repo: str
    kind: IssueLinkKind
    number: int | None

    @property
    def path(self) -> str:
        """The repository path, ``owner/repo``."""

        return f"{self.owner}/{self.repo}"


# The entry appended when a message holds more candidates than are read.
_OVERFLOW = IssueLink(host="", owner="", repo="", kind="malformed", number=None)


def _split(url: str) -> tuple[str, str]:
    """The lowercased netloc and the path of ``url``; never raises."""

    rest = url.split("://", 1)[1]
    end = len(rest)
    for mark in "/?#":
        index = rest.find(mark)
        if index != -1:
            end = min(end, index)
    netloc, after = rest[:end], rest[end:]
    for mark in "?#":
        index = after.find(mark)
        if index != -1:
            after = after[:index]
    return netloc.lower(), after


def _classify(url: str) -> IssueLink | None:
    """The link ``url`` names, or None when it is not issue-shaped at all."""

    host, path = _split(url)
    segments = path.split("/")[1:] if path.startswith("/") else []
    if not any(segment.casefold() in _CANDIDATE_SEGMENTS for segment in segments):
        return None
    owner = segments[0] if segments else ""
    repo = segments[1] if len(segments) > 1 else ""
    core = segments[:-1] if len(segments) == 5 and segments[4] == "" else segments
    if (
        len(core) == 4
        and core[2] in _KINDS
        and _NAME.fullmatch(owner)
        and _NAME.fullmatch(repo)
        and _NUMBER.fullmatch(core[3])
        and int(core[3]) <= _MAX_NUMBER
    ):
        return IssueLink(
            host=host, owner=owner, repo=repo, kind=_KINDS[core[2]], number=int(core[3])
        )
    return IssueLink(host=host, owner=owner, repo=repo, kind="malformed", number=None)


def find_issue_links(text: str) -> tuple[IssueLink, ...]:
    """Every distinct issue-shaped link in ``text``, in order; see the module docstring."""

    if not isinstance(text, str):
        return ()
    links: list[IssueLink] = []
    seen: set[IssueLink] = set()
    candidates = 0
    for match in _URL.finditer(text):
        link = _classify(match.group(0))
        if link is None:
            continue
        candidates += 1
        if candidates > MAX_CANDIDATES:
            links.append(_OVERFLOW)
            break
        if link not in seen:
            seen.add(link)
            links.append(link)
    return tuple(links)


__all__ = ["MAX_CANDIDATES", "IssueLink", "IssueLinkKind", "find_issue_links"]
