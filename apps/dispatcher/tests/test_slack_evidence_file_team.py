"""The sender team of a file-bearing Slack message.

@spec HTTP-INVOCATION-EXECUTION-28. Every id here is synthetic. No real
file-bearing delivery is captured; the shape is Slack's documented file object.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest
from curie_api.identity.slack import SlackEvidence, derive_registered_sender
from curie_dispatcher.identity import slack_evidence

USER = "U0EXAMPLE1"
OTHER_USER = "U0EXAMPLE2"
TEAM = "T0EXAMPLE1"
OTHER_TEAM = "T0EXAMPLE2"
SENTINEL = object()


def file(user: Any = USER, user_team: Any = TEAM, **extra: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-28: a synthetic Slack file object."""
    out: dict[str, Any] = {"id": "F0EXAMPLE1", "name": "report.txt", **extra}
    if user is not SENTINEL:
        out["user"] = user
    if user_team is not SENTINEL:
        out["user_team"] = user_team
    return out


def body(files: Any = SENTINEL, **event_changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-28: an app_mention with no team on its event."""
    event: dict[str, Any] = {
        "type": "app_mention", "user": USER, "channel": "C0EXAMPLE1", "text": "hi",
        "ts": "1700.0001", **event_changes,
    }
    if files is not SENTINEL:
        event["files"] = files
    return {
        "type": "event_callback", "team_id": TEAM, "is_ext_shared_channel": False,
        "event": event, "authorizations": [{"enterprise_id": None, "team_id": TEAM}],
    }


def unfilled(payload: dict[str, Any]) -> dict[str, Any] | None:
    """What the copier returned before: the same payload without any files."""
    bare = copy.deepcopy(payload)
    bare["event"].pop("files", None)
    return slack_evidence(bare)


@pytest.mark.parametrize("files", [
    [file()],
    [file(), file(id="F0EXAMPLE2")],
    [file() for _ in range(100)],
])
def test_the_senders_own_files_fill_the_team(files: list[dict[str, Any]]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-28."""
    payload = body(files)
    evidence = slack_evidence(payload)
    assert evidence == {**(unfilled(payload) or {}), "event_user_team": TEAM}
    assert evidence is not None
    assert evidence["event_team"] is None


@pytest.mark.parametrize("files", [
    [file(user=OTHER_USER)],
    [file(), file(user=OTHER_USER)],
    [file(), file(user_team=OTHER_TEAM)],
    [file(user_team=SENTINEL)],
    [file(), file(user_team=SENTINEL)],
    [file(user_team="   ")],
    [file(user_team="")],
    [file(user_team=None)],
    [file(user_team=7)],
    [file(user=SENTINEL)],
    [file(user="")],
    [file(), "F0EXAMPLE1"],
    [],
    [file() for _ in range(101)],
    {"0": file()},
    "F0EXAMPLE1",
    None,
], ids=[
    "other-users-file", "one-file-shared-by-another", "two-teams", "file-without-team",
    "mixed-with-and-without-team", "blank-team", "empty-team", "null-team",
    "non-string-team", "file-without-user", "empty-file-user", "non-dict-file",
    "empty-files", "101-files", "files-is-dict", "files-is-string", "files-is-null",
])
def test_other_shapes_leave_the_evidence_as_it_was(files: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-28."""
    payload = body(files)
    assert slack_evidence(payload) == unfilled(payload)


@pytest.mark.parametrize("event_changes", [
    {"team": OTHER_TEAM},
    {"user_team": OTHER_TEAM},
    {"user_team": TEAM, "team": OTHER_TEAM},
    {"team": ""},
    {"user_team": None},
])
def test_an_event_that_names_a_team_keeps_its_own_value(event_changes: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-28: the fill never overrides or adds to a native team."""
    payload = body([file(user_team="T0EXAMPLE9")], **event_changes)
    assert slack_evidence(payload) == unfilled(payload)


@pytest.mark.parametrize("user", ["", None, 7])
def test_an_event_without_a_usable_user_is_not_filled(user: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-28."""
    payload = body([file(user=user)], user=user)
    assert slack_evidence(payload) == unfilled(payload)


def test_nothing_else_is_copied_from_files() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-28."""
    payload = body([file(enterprise_id="E0EXAMPLE1", team="T0EXAMPLE9")])
    evidence = slack_evidence(payload)
    assert evidence == {**(unfilled(payload) or {}), "event_user_team": TEAM}
    assert evidence is not None
    assert evidence["enterprise_ids"] == []
    assert evidence["event_team"] is None


def registered(evidence: dict[str, Any] | None) -> tuple[str, str] | str:
    assert evidence is not None
    return derive_registered_sender(
        SlackEvidence.model_validate(evidence),
        namespace_kind="slack_workspace", namespace_authority="", namespace_key=TEAM,
        installation_account=TEAM,
    )


def test_the_filled_team_resolves_and_the_unfilled_stays_unidentified() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-28: an ordinary registration."""
    payload = body([file()])
    assert registered(slack_evidence(payload)) == (TEAM, USER)
    assert registered(unfilled(payload)) == "subject_unidentified"
    assert registered(slack_evidence(body([file(user=OTHER_USER)]))) == "subject_unidentified"
