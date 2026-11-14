"""Assertions of the scripted ``triage-to-pr`` scenario (#5588 decision 14).

The scenario drives the installed platform with only the model, GitHub and
Slack HTTP boundaries scripted, then reads the outcome from outside: the
GitHub stub's issues, comments, events, pull requests and branches, the Slack
recorder's posts, the model lanes, and read-only SQL. Each function below takes
those plain observations and raises ``TriageAssertionError`` when one of the
decision's ten points (or the refused handoff, or the findings ending) does
not hold. They read no prompt or skill text.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import factory_e2e as fe

TRIAGE_LABEL = "triage"
# The dark factory agent of the existing scenario serves the configured
# tracker label; the triage declaration names it as its implementation label.
IMPLEMENTATION_LABEL = "factory"
BASE_LABEL = "base:main"
TRIAGED_LABEL = "curie-factory:triaged"
SLACK_CHANNEL = "C0EXAMPLE7"
# Seeded source issues: a reproducible defect, the same defect for the refused
# handoff, and a report that does not reproduce.
SOURCE_TICKET = 3901
SOURCE_REFUSED = 3902
SOURCE_FINDINGS = 3903
REFUSED_LABELLER = {"id": 6603, "login": "triager", "type": "User"}
REFUSAL_REASON = "authority_revoked"
INTAKE_NOTICE_PREFIX = "curie-factory-intake-"
EVIDENCE_NAME = "scripted-triage-to-pr.json"
# The success terminus factory_e2e's issue-to-pull-request judges require of
# an execution request, and the check conclusions they count as green.
IMPLEMENTATION_SUCCESS_STATUS = "completed"
GREEN_CHECK_CONCLUSIONS = fe.FIRST_HEAD_GREEN


class TriageAssertionError(AssertionError):
    """The triage-to-pr scenario observed something decision 14 forbids."""


def linked_marker(request_id: str) -> str:
    return f"curie-linked-issue:execution-request:{request_id}"


def result_marker(request_id: str) -> str:
    return f"curie-tracker-result:{request_id}"


def _labels(issue: Mapping[str, Any]) -> set[str]:
    return {label["name"] for label in issue.get("labels", [])}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise TriageAssertionError(message)


def body_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def outputs_of(issues: Iterable[Mapping[str, Any]], request_id: str) -> list[Mapping[str, Any]]:
    marker = linked_marker(request_id)
    return [issue for issue in issues if marker in (issue.get("body") or "")]


def result_comments_of(
    comments: Iterable[Mapping[str, Any]], source: int, request_id: str
) -> list[Mapping[str, Any]]:
    marker = result_marker(request_id)
    return [
        comment
        for comment in comments
        if comment["issue_number"] == source and marker in comment["body"]
    ]


def assert_one_selection(
    work_items: Sequence[Mapping[str, Any]],
    requests: Sequence[Mapping[str, Any]],
    lane: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Point 1: one human selection, one source WorkItem, one triage model session."""

    _require(len(work_items) == 1, f"expected one source WorkItem, found {len(work_items)}")
    _require(len(requests) == 1, f"expected one source request, found {len(requests)}")
    request = requests[0]
    _require(request.get("completion_contract") == "tracker_result", "source is not triage")
    _require(bool(request.get("model_session_reserved_at")), "no triage model session started")
    ordinals = list(lane.get("served_ordinals", []))
    _require(ordinals.count(0) == 1, f"triage sessions started {ordinals.count(0)} times")
    _require(len(ordinals) == len(set(ordinals)), "a triage turn was answered twice")
    return request


def assert_reproduction_ran(
    lane: Mapping[str, Any], main_revision: str, receipt: Mapping[str, Any]
) -> None:
    """Point 2: the reproduction ran in the sandbox at the reported revision.

    The lane substituted the commit the sandbox's own command printed, and the
    platform recorded that same commit as the ticket's revision.
    """

    _require(
        list(lane.get("observed_revisions", [])) == [main_revision],
        "the reproduction did not print the source base revision",
    )
    _require(receipt.get("revision") == main_revision, "the ticket names another revision")


def assert_one_output(
    issues: Sequence[Mapping[str, Any]],
    comments: Sequence[Mapping[str, Any]],
    source: int,
    request_id: str,
    receipt: Mapping[str, Any],
    bot_login: str,
) -> Mapping[str, Any]:
    """Point 3: one output issue and one marked result comment, read back as delivered."""

    outputs = outputs_of(issues, request_id)
    _require(len(outputs) == 1, f"expected one linked output issue, found {len(outputs)}")
    output = outputs[0]
    results = result_comments_of(comments, source, request_id)
    _require(len(results) == 1, f"expected one marked result comment, found {len(results)}")
    _require(output["user"]["login"] == bot_login, "the output was not created by the App")
    _require(BASE_LABEL in _labels(output), "the output lost the source base label")
    _require(TRIAGE_LABEL not in _labels(output), "the output carries the triage label")
    _require(
        receipt.get("output_body_sha256") == body_sha256(output["body"]),
        "the output body differs from the delivered digest",
    )
    _require(receipt.get("output_url") == output["html_url"], "the receipt names another issue")
    _require(
        str(receipt.get("result_comment_id")) == str(results[0]["id"]),
        "the receipt names another result comment",
    )
    return output


def assert_no_triage_publication(
    pulls: Sequence[Mapping[str, Any]],
    branches: Sequence[str],
    triage_publications: int,
    sources: Iterable[int],
) -> None:
    """Point 4: no triage branch or pull request was published."""

    _require(triage_publications == 0, "a triage request recorded a publication")
    heads = {pull["head"]["ref"] for pull in pulls}
    stray = sorted(set(branches) - heads - {"main"})
    _require(not stray, f"branches without a pull request: {stray}")
    for source in sources:
        closing = [pull for pull in pulls if f"#{source}" in (pull.get("body") or "")]
        _require(not closing, f"a pull request names triage source #{source}")


def assert_handoff_admitted(
    work_items: Sequence[Mapping[str, Any]],
    requests: Sequence[Mapping[str, Any]],
    implementation_agent: str,
    source_request: Mapping[str, Any],
    output_events: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    """Point 5: one separate implementation WorkItem under the delegation, no human action."""

    _require(len(work_items) == 1, f"expected one output WorkItem, found {len(work_items)}")
    _require(len(requests) == 1, f"expected one output request, found {len(requests)}")
    work_item, request = work_items[0], requests[0]
    _require(str(work_item["agent_id"]) == implementation_agent, "the wrong agent was admitted")
    _require(work_item.get("trigger_label") == IMPLEMENTATION_LABEL, "wrong frozen label")
    _require(request.get("completion_contract") == "implementation", "output is not implemented")
    _require(request["id"] != source_request["id"], "the source request was reused")
    _require(
        request.get("requester") == source_request.get("requester"),
        "the implementation is not attributed to the original requester",
    )
    actors = {event["actor"]["type"] for event in output_events}
    _require(actors <= {"Bot"}, "a person acted on the output issue")
    added = [
        event
        for event in output_events
        if event["event"] == "labeled" and event["label"]["name"] == IMPLEMENTATION_LABEL
    ]
    _require(len(added) == 1, f"implementation label added {len(added)} times")
    return request


def assert_implementation_terminus(
    request: Mapping[str, Any],
    source_request: Mapping[str, Any],
    pulls: Sequence[Mapping[str, Any]],
    final_comments: Sequence[Mapping[str, Any]],
    output_number: int,
    checks_by_head: Mapping[str, Sequence[Mapping[str, Any]]],
) -> Mapping[str, Any]:
    """Point 6: fresh bounds, then the normal pull request and check terminus.

    ``checks_by_head`` maps a head commit to the check runs the code host
    reports on it. The pull request's head must carry at least one, and every
    one must have completed green.
    """

    # Both timestamps come from one json_build_object, so they share one
    # ISO 8601 rendering and compare as strings.
    _require(bool(request.get("execution_deadline")), "the implementation has no deadline")
    _require(
        str(request["execution_deadline"]) > str(source_request.get("execution_deadline") or ""),
        "the implementation did not get its own, later deadline",
    )
    _require(
        str(request["created_at"]) > str(source_request.get("created_at") or ""),
        "the implementation request predates the triage request",
    )
    closing = [pull for pull in pulls if f"#{output_number}" in (pull.get("body") or "")]
    _require(len(closing) == 1, f"expected one pull request for the output, found {len(closing)}")
    _require(len(final_comments) == 1, "expected one final comment on the output")
    _require(closing[0]["html_url"] in final_comments[0]["body"], "final comment omits the PR")
    status = request.get("status")
    _require(
        status == IMPLEMENTATION_SUCCESS_STATUS,
        f"the implementation request ended {status!r}, expected {IMPLEMENTATION_SUCCESS_STATUS!r}",
    )
    head = str((closing[0].get("head") or {}).get("sha") or "")
    _require(bool(head), "the pull request has no head commit")
    checks = [c for c in checks_by_head.get(head, []) if c.get("head_sha") == head]
    _require(bool(checks), "the pull request head has no check runs")
    red = sorted(
        str(c.get("name"))
        for c in checks
        if c.get("status") != "completed" or c.get("conclusion") not in GREEN_CHECK_CONCLUSIONS
    )
    _require(not red, f"pull request checks did not pass: {red}")
    return closing[0]


def assert_no_replay_effects(
    before: Mapping[str, int],
    after: Mapping[str, int],
    output_comments: Sequence[Mapping[str, Any]],
) -> None:
    """Point 7: replay, an API restart and bot label events created nothing more."""

    _require(dict(before) == dict(after), f"replay changed the outcome: {before} -> {after}")
    notices = [c for c in output_comments if INTAKE_NOTICE_PREFIX in c["body"]]
    _require(not notices, "the factory's own label drew an intake notice")


def assert_source_preserved(before: Mapping[str, Any], after: Mapping[str, Any]) -> None:
    """Point 8: title, body, human labels and open state are untouched."""

    for key in ("title", "body", "state"):
        _require(before[key] == after[key], f"the source issue {key} changed")
    _require(after["state"] == "open", "the source issue was closed")
    missing = _labels(before) - _labels(after)
    _require(not missing, f"human labels were removed: {sorted(missing)}")
    added = _labels(after) - _labels(before)
    _require(added == {TRIAGED_LABEL}, f"unexpected source labels: {sorted(added)}")


def triage_lines(posts: Iterable[Mapping[str, Any]], ticket_url: str) -> list[Mapping[str, Any]]:
    prefix = f"Triage published a ticket: {ticket_url}."
    return [
        post
        for post in posts
        if post["method"] == "chat.postMessage" and (post.get("text") or "").startswith(prefix)
    ]


def assert_slack_line(
    posts: Sequence[Mapping[str, Any]],
    thread: Mapping[str, Any],
    ticket_url: str,
    disposition: str,
) -> None:
    """Point 9: the source thread holds the result line and its disposition, once."""

    lines = triage_lines(posts, ticket_url)
    _require(len(lines) == 1, f"expected one Slack triage line, found {len(lines)}")
    line = lines[0]
    expected = f"Triage published a ticket: {ticket_url}. {disposition}"
    _require(line.get("text") == expected, f"Slack line {line.get('text')!r} != {expected!r}")
    _require(line.get("channel") == thread.get("channel"), "line posted to another channel")
    _require(
        bool(thread.get("root_ts")) and line.get("thread_ts") == thread["root_ts"],
        "line is not in the source WorkItem's thread",
    )


def assert_runtime_cleared(
    sandbox_claims: int, publication_resources: int, cleanup: Mapping[str, Any]
) -> None:
    """Point 10: every runtime resource the scenario created is gone."""

    _require(sandbox_claims == 0, f"{sandbox_claims} sandbox claims remain")
    _require(publication_resources == 0, f"{publication_resources} publication resources remain")
    _require(cleanup.get("cleanup_verified") is True, "owned cluster teardown was not verified")


def assert_refused_handoff(
    issues: Sequence[Mapping[str, Any]],
    comments: Sequence[Mapping[str, Any]],
    source: int,
    request_id: str,
    output_work_items: Sequence[Mapping[str, Any]],
    posts: Sequence[Mapping[str, Any]],
    thread: Mapping[str, Any],
    lane: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Ending 2: ticket published, no implementation, refusal on the source and Slack."""

    outputs = outputs_of(issues, request_id)
    _require(len(outputs) == 1, f"expected one refused output issue, found {len(outputs)}")
    output = outputs[0]
    _require(output["state"] == "open", "the refused ticket was closed")
    _require(
        not _labels(output) & {IMPLEMENTATION_LABEL, TRIAGE_LABEL},
        "the refused ticket carries a trigger label",
    )
    _require(not output_work_items, "a refused handoff admitted implementation")
    _require(list(lane.get("served_ordinals", [])).count(0) == 1, "triage restarted")
    statuses = [
        c
        for c in comments
        if c["issue_number"] == source and "Implementation not started:" in c["body"]
    ]
    _require(len(statuses) == 1, "the source status comment does not name the refusal")
    _require(REFUSAL_REASON in statuses[0]["body"], "the refusal names another reason")
    lines = triage_lines(posts, output["html_url"])
    _require(len(lines) == 1, f"expected one refused Slack line, found {len(lines)}")
    text = lines[0].get("text") or ""
    _require(
        text.startswith(
            f"Triage published a ticket: {output['html_url']}. Implementation not started:"
        )
        and REFUSAL_REASON in text,
        f"refused Slack line is {text!r}",
    )
    _require(lines[0].get("thread_ts") == thread.get("root_ts"), "refusal in another thread")
    return output


def assert_findings_ending(
    issues: Sequence[Mapping[str, Any]],
    comments: Sequence[Mapping[str, Any]],
    source: int,
    request_id: str,
    receipt: Mapping[str, Any],
    implementation_requests: int,
) -> None:
    """Findings ending: a result comment, no output issue, no implementation request."""

    _require(not outputs_of(issues, request_id), "findings published an output issue")
    _require(receipt.get("result_kind") == "findings", "the result is not findings")
    _require(receipt.get("reason") == "not_reproduced", "findings name another reason")
    _require(not receipt.get("output_issue_id"), "findings recorded an output issue")
    results = result_comments_of(comments, source, request_id)
    _require(len(results) == 1, f"expected one findings comment, found {len(results)}")
    _require(implementation_requests == 0, "findings admitted an implementation request")
