"""Offline proof of the scripted ``jira-one-ticket`` scenario's judging (#5791 AC 7).

The Jira stub holds a ticket whose factory comments were written by the
production Jira adapter (`JiraMarkedComments.upsert_marked`) with the
production bodies (`factory_questions.compose_body`, the status lines of
`factory_notices`), and whose pull request reference is the adapter's own
(`publication_references`). ``one_ticket_jira`` turns what the stub reports into
the records ``one_ticket_scenario`` judges; each test then breaks one fact the
scenario owes and requires the shared assertions to refuse it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import ssl
import subprocess
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api import factory_questions
from curie_api.factory_comment_text import status_marker
from curie_api.factory_notices import ANSWER_RECEIVED_LINE, INVESTIGATING_LINE
from curie_api.forges.config import ForgesConfig
from curie_api.forges.jira_cloud import adf
from curie_api.forges.registry import build_registry
from curie_api.forges.types import JIRA_CLOUD, ReplyTarget, RepositoryRef, TrackerIssueRef

TOOLS = Path(__file__).resolve().parents[1]
REPO_ROOT = TOOLS.parents[1]
sys.path.insert(0, str(TOOLS))
sys.path.insert(0, str(REPO_ROOT / "apps/api/tests"))

import jira_one_ticket_run as run  # noqa: E402
import one_ticket_jira as ojt  # noqa: E402
import one_ticket_run  # noqa: E402
import one_ticket_scenario as ots  # noqa: E402
import scripted_kind as sk  # noqa: E402
from forge_fakes import jira as fake_jira  # noqa: E402

BOT = ojt.BOT_ACCOUNT_ID
LABELLER = ojt.LABELLER["account_id"]
OUTSIDER = ojt.OUTSIDER["account_id"]
ASKING = uuid.UUID("00000000-0000-4000-8000-000000000001")
SECOND = uuid.UUID("00000000-0000-4000-8000-000000000002")
QUESTIONS = ["Which spelling of Kelvin?", "Should negative Kelvin be rejected?"]


def _ca(workdir: Path) -> tuple[Path, Path]:
    cert, key = workdir / "ca.pem", workdir / "ca.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=one-ticket-jira-test-ca",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    return cert, key


@contextmanager
def served(workdir: Path) -> Iterator[run.JiraStubProcess]:
    cert, key = _ca(workdir)
    stub = run.JiraStubProcess(workdir / "stub", "127.0.0.1", cert, key, python=(sys.executable,))
    try:
        stub.start({"PATH": "/usr/bin:/bin"}, timeout=60)
        yield stub
    finally:
        teardown = stub.close()
    assert teardown["exited"] and not teardown["listening"], teardown


def _tracker(stub: run.JiraStubProcess, client: httpx.AsyncClient) -> Any:
    """The production tracker for the stub site, as the scenario's install builds it."""

    document = fake_jira.jira_document(
        api_url=stub.api_url, start_authority={"type": "allowlist", "accounts": [LABELLER]}
    )
    document["trackers"][0]["host"] = stub.netloc
    registry = build_registry(ForgesConfig.model_validate(document))
    connection = registry.binding_tracker(fake_jira.TRACKER_NAME)
    return connection.for_binding(client, registry.binding(fake_jira.BINDING_NAME))


async def _factory_writes(stub: run.JiraStubProcess, issue: TrackerIssueRef) -> dict[str, Any]:
    """What the platform writes on the ticket, through the production adapter."""

    context = ssl.create_default_context(cafile=str(stub.ca_cert))
    async with httpx.AsyncClient(verify=context) as client:
        tracker = _tracker(stub, client)
        comments = tracker.marked_comments
        target = ReplyTarget.on_issue(issue)
        status = status_marker(ASKING)
        await comments.upsert_marked(target, status, INVESTIGATING_LINE)
        questions = factory_questions.compose_body(1, "Two things decide the change.", QUESTIONS)
        await comments.upsert_marked(target, factory_questions.marker_for(ASKING), questions)
        await comments.upsert_marked(target, status, f"{INVESTIGATING_LINE}\n\nQuestions asked.")
        await tracker.set_state_label(issue, add=ots.QUESTIONS_LABEL, remove=())
        repository = RepositoryRef("github", "github.com", "4401", "acme-corp/acme-bot")
        references = await tracker.publication_references(issue, repository)
    return {"references": references}


async def _answered(stub: run.JiraStubProcess, issue: TrackerIssueRef) -> None:
    context = ssl.create_default_context(cafile=str(stub.ca_cert))
    async with httpx.AsyncClient(verify=context) as client:
        comments = _tracker(stub, client).marked_comments
        await comments.upsert_marked(
            ReplyTarget.on_issue(issue), status_marker(SECOND), f"{ANSWER_RECEIVED_LINE}\n\nDone."
        )


def _ticket(stub: run.JiraStubProcess) -> dict[str, Any]:
    """The scenario's ticket on the stub: labelled, asked, ignored, answered."""

    control = stub.client()
    for person in (ojt.LABELLER, ojt.OUTSIDER):
        control.add_user(person)
    title, body = one_ticket_run.issue_title_and_body()
    created = control.create_issue(title, body, LABELLER)
    issue_id, key = created["id"], created["key"]
    baseline = [record["number"] for record in ojt.issue_records(control.state())]
    label_at = ots.instant(control.add_label(issue_id, ots.LABEL, LABELLER)["at"])
    issue = TrackerIssueRef(JIRA_CLOUD, stub.netloc, ojt.CLOUD_ID, issue_id, key)
    written = asyncio.run(_factory_writes(stub, issue))
    outsider = control.add_comment(issue_id, run.OUTSIDER_TEXT, OUTSIDER)
    answer = control.add_comment(issue_id, one_ticket_run.ANSWER_TEXT, LABELLER)
    asyncio.run(_answered(stub, issue))
    answer_at = ots.instant(answer["at"])
    return {
        "issue_id": issue_id,
        "key": key,
        "url": ojt.browse_url(stub.base_url, key),
        "baseline": baseline,
        "label_at": label_at,
        "answer": answer,
        "answer_at": answer_at,
        "outsider": outsider,
        "references": written["references"],
        "requests": [
            {
                "id": str(ASKING),
                "status": "completed",
                "terminal_cause": "questions_asked",
                "completion_contract": "implementation",
                "created_at": label_at.isoformat(),
            },
            {
                "id": str(SECOND),
                "status": "completed",
                "terminal_cause": None,
                "completion_contract": "implementation",
                "created_at": (answer_at + timedelta(seconds=20)).isoformat(),
            },
        ],
        "receipts": [
            {"comment_key": str(answer["id"]), "outcome": "admitted", "request_id": str(SECOND)},
            {"comment_key": str(outsider["id"]), "outcome": "not_authorized", "request_id": None},
        ],
        "state": control.state(),
        "control": control,
    }


def _pull(body: str) -> dict[str, Any]:
    return {"number": 7, "body": body, "html_url": "https://example.test/pull/7"}


def test_the_stub_records_pass_every_shared_assertion(tmp_path: Path) -> None:
    with served(tmp_path) as stub:
        ticket = _ticket(stub)
    state, issue_id = ticket["state"], ticket["issue_id"]
    records = ojt.comment_records(state, issue_id)
    question = ots.assert_one_question_comment(records, int(issue_id), BOT)
    assert question["body"].startswith(f"<!-- {factory_questions.MARKER_PREFIX}")
    issue = ojt.issue_record(ojt.issue_of(state, issue_id))
    asking = ots.assert_questions_state(issue, ticket["requests"][:1])
    investigating = ojt.line_instant(state, issue_id, str(ASKING), INVESTIGATING_LINE, BOT)
    ots.assert_first_response(ticket["label_at"], investigating)
    received = ojt.line_instant(state, issue_id, str(SECOND), ANSWER_RECEIVED_LINE, BOT)
    second = ots.assert_answer_admitted(
        ticket["requests"], str(asking["id"]), ticket["answer_at"], received
    )
    ots.assert_answer_receipts(ticket["receipts"], str(ticket["answer"]["id"]), str(second["id"]))
    body = f"Implements the ticket.\n\n{ticket['references'][0]}\n"
    ojt.assert_one_jira_pull_request([_pull(body)], ticket["key"], ticket["url"], [])
    ots.assert_no_factory_issues(ticket["baseline"], ojt.issue_records(state))


def test_a_second_factory_created_issue_is_refused(tmp_path: Path) -> None:
    with served(tmp_path) as stub:
        ticket = _ticket(stub)
        ticket["control"].create_issue("Follow-up", "Made by the factory.", BOT)
        state = ticket["control"].state()
    with pytest.raises(ots.OneTicketAssertionError, match="the factory created issues"):
        ots.assert_no_factory_issues(ticket["baseline"], ojt.issue_records(state))


def test_a_pull_request_body_without_the_jira_key_is_refused(tmp_path: Path) -> None:
    with served(tmp_path) as stub:
        ticket = _ticket(stub)
    key, url = ticket["key"], ticket["url"]
    # The adapter's reference names both; the GitHub closing form names neither.
    assert ticket["references"][0] == f"Jira: {key} {url}"
    # The last names the key but links another ticket whose URL extends this one.
    wrong = (
        f"Closes #{ticket['issue_id']}",
        f"Refs {key}",
        f"Jira: {key}0 {url}0",
        f"{key} {url}0",
    )
    for body in wrong:
        with pytest.raises(ots.OneTicketAssertionError, match=f"does not name {key}"):
            ojt.assert_one_jira_pull_request([_pull(body)], key, url, [])


def test_a_request_the_outsider_comment_created_is_refused(tmp_path: Path) -> None:
    with served(tmp_path) as stub:
        ticket = _ticket(stub)
    receipts = [
        dict(ticket["receipts"][0]),
        {
            "comment_key": str(ticket["outsider"]["id"]),
            "outcome": "admitted",
            "request_id": "00000000-0000-4000-8000-000000000003",
        },
    ]
    with pytest.raises(ots.OneTicketAssertionError, match="requests started by comments"):
        ots.assert_answer_receipts(receipts, str(ticket["answer"]["id"]), str(SECOND))


def test_a_second_request_later_than_the_bound_after_the_answer_is_refused(
    tmp_path: Path,
) -> None:
    with served(tmp_path) as stub:
        ticket = _ticket(stub)
    state, issue_id = ticket["state"], ticket["issue_id"]
    late = [dict(request) for request in ticket["requests"]]
    late[1]["created_at"] = (
        ticket["answer_at"] + timedelta(seconds=ots.RESPONSE_SECONDS + 1)
    ).isoformat()
    received = ojt.line_instant(state, issue_id, str(SECOND), ANSWER_RECEIVED_LINE, BOT)
    with pytest.raises(ots.OneTicketAssertionError, match="the second request"):
        ots.assert_answer_admitted(late, str(ASKING), ticket["answer_at"], received)


def test_a_person_comment_is_not_the_factory_status_comment(tmp_path: Path) -> None:
    """The marker lives in the entity property the adapter writes, not in text."""

    with served(tmp_path) as stub:
        ticket = _ticket(stub)
        control = ticket["control"]
        control.add_comment(ticket["issue_id"], INVESTIGATING_LINE, LABELLER)
        state = control.state()
    finals = ojt.final_comments(
        state,
        ticket["issue_id"],
        str(SECOND),
        [{"execution_request_id": str(SECOND), "comment_id": "0", "finalized_at": "t"}],
        BOT,
    )
    assert finals == []
    people = [
        record
        for record in ojt.comment_records(state, ticket["issue_id"])
        if record["user"]["login"] == LABELLER
    ]
    assert people and not any(record["body"].startswith("<!--") for record in people)


def test_adf_text_reads_the_production_questions_body_as_numbered_items() -> None:
    body = factory_questions.compose_body(1, "Two things decide the change.", QUESTIONS)
    text = ojt.adf_text(adf.from_markdown(body))
    assert [line for line in text.splitlines() if line[:3] in {"1. ", "2. "}] == [
        f"1. {QUESTIONS[0]}",
        f"2. {QUESTIONS[1]}",
    ]


def test_the_transcript_is_rekeyed_on_the_jira_channel_only() -> None:
    exchanges = [
        {"lane_key": "frame\nchannel: github\nrest", "ordinal": 0, "response": {"x": 1}},
        {"lane_key": "other lane", "ordinal": 0, "response": {"y": 2}},
    ]
    rekeyed = run.transcript_for_jira(exchanges)
    assert rekeyed == [
        {"lane_key": "frame\nchannel: jira\nrest", "ordinal": 0, "response": {"x": 1}},
        exchanges[1],
    ]
    with pytest.raises(run.JiraOneTicketError, match="no GitHub channel frame"):
        run.transcript_for_jira(exchanges[1:])
    recorded = json.loads(one_ticket_run.TRANSCRIPT.read_text())["exchanges"]
    assert any("\nchannel: jira\n" in e["lane_key"] for e in run.transcript_for_jira(recorded))


def test_the_jira_tracker_values_render_a_valid_forges_config() -> None:
    values = sk.scripted_values(
        model_base_url="http://10.0.0.9:8080",
        github_api="https://10.0.0.9:9443/api/v3",
        clone_base="https://10.0.0.9:9443",
        ca_configmap="stub-ca",
        runner_image="registry.example/curie-runner:local",
    )
    sk.add_jira_tracker(
        values, api_url=f"https://10.0.0.9:9555/ex/jira/{ojt.CLOUD_ID}", host="10.0.0.9:9555"
    )
    factory = values["factory"]
    tracker = factory["trackers"][-1]
    assert (tracker["kind"], tracker["host"], tracker["label"]) == (
        "jira_cloud",
        "10.0.0.9:9555",
        ots.LABEL,
    )
    assert tracker["credential"] == {
        "type": "static_token",
        "username": ojt.BOT_EMAIL,
        "existingSecret": sk.JIRA_SECRET_NAME,
        "existingSecretKey": "token",
    }
    (binding,) = factory["bindings"]
    assert binding["tracker"] == sk.JIRA_TRACKER
    assert binding["trackerScopeId"] == ojt.CLOUD_ID
    assert binding["startAuthority"] == {"type": "allowlist", "accounts": [LABELLER]}


def test_the_scripted_driver_validates_the_jira_scenario() -> None:
    args = argparse.Namespace(scenario=sk.JIRA_SCENARIO, record=False, transcript="unused")
    sk.validate_configuration(args)
    args.record = True
    with pytest.raises(sk.fe.ConfigError, match="only the publication scenario"):
        sk.validate_configuration(args)
