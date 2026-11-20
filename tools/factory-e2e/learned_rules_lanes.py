"""Ticket routing, recording split and scrubbing for the learned rules scenario.

Three tickets run on one install and their first user messages differ only by
an issue URL, which the model server's key scrubs away. This module reads the
ticket from the raw request instead. A label objective names the ticket's issue
(``/issues/<n>``); a revision objective opens with a feedback URL on the ticket's
pull request (``/pull/<m>#<fragment>``), so the pull request is mapped back to
the ticket it closes through its stub record.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import scripted_lanes as lanes

TICKETS: dict[str, int] = {"a": 3815, "b": 3921, "c": 3922}
_BY_NUMBER = {number: letter for letter, number in TICKETS.items()}
_ISSUE_OR_PULL = re.compile(r"/(?:issues|pull)/(\d+)")
_ISSUE = re.compile(r"/issues/(\d+)")
# The factory's closing reference is `Closes #<n>` (GitHubTracker.closing_reference).
_CLOSES = re.compile(r"\b[Cc]loses #(\d+)\b")
# The factory's branch is `curie/publication-<hex>` and carries no issue number;
# a branch that does (`.../issue-<n>` or `.../issues/<n>`) must agree with the body.
_BRANCH_ISSUE = re.compile(r"(?:^|[/-])issues?[-/](\d+)(?:$|[/-])")


def transcript_paths(ticket_a: Path) -> dict[str, Path]:
    match = re.fullmatch(r"(.+)-a\.json", ticket_a.name)
    if match is None:
        raise lanes.LaneError(f"{ticket_a.name} is not named <stem>-a.json")
    return {
        "a": ticket_a,
        "b": ticket_a.with_name(f"{match.group(1)}-b.json"),
        "c": ticket_a.with_name(f"{match.group(1)}-c.json"),
    }


def ticket_of_pull(stub: Any, number: int) -> int | None:
    pull = stub._get("pull", number)
    if not isinstance(pull, dict):
        return None
    closes = {int(n) for n in _CLOSES.findall(str(pull.get("body") or ""))} & set(_BY_NUMBER)
    if len(closes) != 1:
        return None
    (ticket,) = closes
    head = pull.get("head")
    ref = head.get("ref") if isinstance(head, dict) else None
    if isinstance(ref, str):
        named = _BRANCH_ISSUE.search(ref)
        if named is not None and int(named.group(1)) != ticket:
            return None
    return ticket


def issue_identity(stub: Any) -> Callable[[dict[str, Any]], int | None]:
    def identity(request: dict[str, Any]) -> int | None:
        text = lanes.first_user_text(request)
        issue = _ISSUE.search(text)
        if issue is not None and int(issue.group(1)) in _BY_NUMBER:
            return int(issue.group(1))
        for found in _ISSUE_OR_PULL.finditer(text):
            return ticket_of_pull(stub, int(found.group(1)))
        return None

    return identity


def ticket_tag(
    identity: Callable[[dict[str, Any]], int | None],
) -> Callable[[dict[str, Any]], str | None]:
    def tag(request: dict[str, Any]) -> str | None:
        return _BY_NUMBER.get(identity(request) or 0)

    return tag


def build_router(
    model_module: Any,
    transcripts: dict[str, Path],
    identity: Callable[[dict[str, Any]], int | None],
) -> lanes.LaneRouter:
    router = lanes.LaneRouter({}, issue_of=identity)
    for letter, number in TICKETS.items():
        exchanges = model_module.load_transcript(transcripts[letter])
        router.add(
            lanes.Lane(f"learned-rules-{letter}", lanes.IMPLEMENTATION, number, list(exchanges))
        )
    return router


def split_recording(
    exchanges: list[dict[str, Any]], tags: list[str | None]
) -> dict[str, list[dict[str, Any]]]:
    if len(exchanges) != len(tags):
        raise lanes.LaneError("recorded exchanges and ticket tags differ in length")
    split: dict[str, list[dict[str, Any]]] = {letter: [] for letter in TICKETS}
    for exchange, tag in zip(exchanges, tags, strict=True):
        if tag not in split:
            raise lanes.LaneError(f"a recorded exchange has no known ticket tag ({tag!r})")
        split[tag].append(exchange)
    return split


def scrub_recording(
    exchanges: list[dict[str, Any]], replacements: dict[str, str]
) -> list[dict[str, Any]]:
    def clean(value: Any) -> Any:
        if isinstance(value, str):
            for old, new in replacements.items():
                value = value.replace(old, new)
            return value
        if isinstance(value, list):
            return [clean(item) for item in value]
        if isinstance(value, dict):
            return {key: clean(item) for key, item in value.items()}
        return value

    return list(clean(copy.deepcopy(exchanges)))


def write_transcripts(
    model_module: Any, split: dict[str, list[dict[str, Any]]], paths: dict[str, Path]
) -> None:
    for letter, exchanges in split.items():
        model_module.dump_transcript(paths[letter], exchanges)
