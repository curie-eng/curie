"""Kind-rung factory scenario assertions (#3814).

The fixture is a small Python tree, not this repository. Its declared check
is ``python -m unittest``, and its edits live under ``unitconv/``. Replaying
the #3521 hardcoded preflight or the #3617 Curie path prefixes drops those
facts and the assertions fail. Those legacy filters exist only so the test
can show the failure. They are not the product path.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

FIXTURE_CHECK = ["python", "-m", "unittest", "discover", "-s", "unitconv/tests", "-v"]
FIXTURE_CHANGED_PATHS = [
    "unitconv/convert.py",
    "unitconv/tests/test_convert.py",
]
# The Slack channel the factory agent is bound to, and the text its root post starts with.
SLACK_CHANNEL = "C0EXAMPLE7"
SLACK_ROOT_PREFIX = "Factory run: acme-corp/acme-bot #3815"
# The prefixes #3617 treated as the only editable tree.
_CURIE_PREFIXES = ("apps/", "cli/", "runner/", "packages/", "charts/")


class ScenarioAssertionError(AssertionError):
    """The scripted factory scenario did not observe the fixture."""


def assert_preflight_outcome(outcome: str) -> None:
    """The declared check ran. ``unavailable`` means it did not."""

    if outcome not in {"passed", "failed"}:
        raise ScenarioAssertionError(
            f"preflight reported {outcome!r}; expected passed or failed"
        )


def assert_publication_paths(paths: list[str]) -> None:
    """The publication request carries the fixture paths the tools changed."""

    missing = [path for path in FIXTURE_CHANGED_PATHS if path not in paths]
    if missing:
        raise ScenarioAssertionError(
            "publication request is missing fixture paths: " + ", ".join(missing)
        )


def legacy_3521_preflight(command: list[str]) -> str:
    """The old hardcoded Curie preflight. Not used by the product."""

    if command[:3] == ["uv", "run", "pytest"]:
        return "passed"
    return "unavailable"


def legacy_3617_paths(paths: list[str]) -> list[str]:
    """The old Curie prefix filter. Not used by the product."""

    return [
        path
        for path in paths
        if any(path.startswith(prefix) for prefix in _CURIE_PREFIXES)
    ]


def _field(post: Any, name: str) -> Any:
    return post.get(name) if isinstance(post, Mapping) else getattr(post, name, None)


def assert_slack_progress(
    posts: Iterable[Any], channel: str, pull_request_url: str
) -> dict[str, int]:
    """The factory progress card: one root, one card with an update, one ready reply.

    ``posts`` are the Slack Web API calls a recorder saw, as objects or mappings
    with ``method``, ``channel``, ``text``, ``thread_ts`` and ``ts``. Posts in
    other threads are ignored. Returns the counts the run records.
    """

    posts = list(posts)
    in_channel = [post for post in posts if _field(post, "channel") == channel]
    roots = [
        post
        for post in in_channel
        if _field(post, "method") == "chat.postMessage"
        and not _field(post, "thread_ts")
        and str(_field(post, "text") or "").startswith(SLACK_ROOT_PREFIX)
    ]
    if len(roots) != 1:
        raise ScenarioAssertionError(f"expected one Slack root post, saw {len(roots)}")
    root_ts = _field(roots[0], "ts")
    thread = [
        post
        for post in in_channel
        if _field(post, "method") == "chat.postMessage" and _field(post, "thread_ts") == root_ts
    ]
    if not thread:
        raise ScenarioAssertionError("the Slack root post has no card in its thread")
    card = thread[0]
    updates = [
        post
        for post in in_channel
        if _field(post, "method") == "chat.update" and _field(post, "ts") == _field(card, "ts")
    ]
    if not updates:
        raise ScenarioAssertionError("the Slack card was never updated")
    ready = [post for post in thread[1:] if pull_request_url in str(_field(post, "text") or "")]
    if len(ready) != 1:
        raise ScenarioAssertionError(
            f"expected one ready reply in the root thread naming {pull_request_url}, "
            f"saw {len(ready)}"
        )
    extra = len(thread) - 1 - len(ready)
    if extra:
        raise ScenarioAssertionError(
            f"expected one card in the root thread, saw {1 + extra} posts besides the ready reply"
        )
    return {"root": len(roots), "card": 1, "ready": len(ready)}
