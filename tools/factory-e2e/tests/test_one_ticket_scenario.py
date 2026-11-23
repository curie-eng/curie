"""Offline proof of the scripted ``one-ticket`` scenario (#5788).

The assertion functions reject each broken shape; the scenario's comment
helpers write the shape the GitHub stub serves to the real poll intake, which
is read here through the production tracker and the production poll rules; and
the scripted lanes serve the two sessions of the transcript in order.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib.util
import json
import re
import ssl
import sys
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api import factory_poll_intake
from curie_api.forges.github.comments import static_token
from curie_api.forges.github.tracker import GitHubTracker
from curie_api.github_review_events import FeedbackIgnored, label_actor

TOOLS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS))

import one_ticket_run as run  # noqa: E402
import one_ticket_scenario as ots  # noqa: E402
import scripted_lanes as lanes  # noqa: E402

REPO_ROOT = TOOLS.parents[1]
STUB = REPO_ROOT / "tools/github-stub/github_stub.py"
MODEL_SCRIPT = REPO_ROOT / "tools/model-script/model_script.py"
REPRODUCTION_GATE = REPO_ROOT / "examples/dark-factory/hooks/reproduction_gate.py"
DECISIONS_SECTION = REPO_ROOT / "examples/dark-factory/hooks/decisions_section.py"
SEED_TREE = TOOLS / "fixtures/unitconv"
RECORDING = REPO_ROOT / "tools/github-stub/recordings/curie-pr-3400.json"
REPOSITORY = "acme-corp/acme-bot"
BOT_LOGIN = "example-app[bot]"
T0 = ots.instant("2026-11-19T12:00:00Z")


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


GITHUB = _load("one_ticket_github_stub", STUB)

# Assertions ------------------------------------------------------------------


def _comment(number: int, body: str, login: str = BOT_LOGIN) -> dict[str, Any]:
    return {"issue_number": number, "body": body, "user": {"login": login}}


def _questions(body: str = "1. Which spelling?\n2. Which sign?\n") -> dict[str, Any]:
    return _comment(ots.ISSUE, f"<!-- {ots.QUESTIONS_MARKER}abc -->\n{body}")


def _request(
    sequence: int, status: str, *, cause: str | None = None, created: str = "2026-11-19T12:00:00Z"
) -> dict[str, Any]:
    return {
        "id": f"00000000-0000-4000-8000-00000000000{sequence}",
        "sequence": sequence,
        "status": status,
        "terminal_cause": cause,
        "execution_kind": "implementation",
        "created_at": created,
    }


def test_each_flow_point_passes_on_the_valid_shape() -> None:
    ots.assert_first_response(T0, T0 + timedelta(seconds=ots.RESPONSE_SECONDS))
    ots.assert_one_question_comment([_questions()], ots.ISSUE, BOT_LOGIN)
    issue = {"labels": [{"name": ots.QUESTIONS_LABEL}]}
    asking = _request(1, "completed", cause="questions_asked")
    assert ots.assert_questions_state(issue, [asking]) is asking
    ots.assert_ignored_comments(1, 1)
    second = _request(2, "completed", created="2026-11-19T12:03:00Z")
    ots.assert_answer_admitted(
        [asking, second], str(asking["id"]), T0, T0 + timedelta(seconds=ots.RESPONSE_SECONDS)
    )
    pull = {"number": 1, "body": f"Closes #{ots.ISSUE}", "html_url": "https://example.test/pull/1"}
    assert ots.assert_one_pull_request([pull], ots.ISSUE, []) is pull
    ots.assert_no_factory_issues([3815, ots.ISSUE], [{"number": 3815}, {"number": ots.ISSUE}])


def test_a_second_factory_created_issue_is_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="created issues: \\[3999\\]"):
        ots.assert_no_factory_issues([3815, ots.ISSUE], [{"number": 3815}, {"number": 3999}])


def test_a_pull_request_without_the_closing_reference_is_refused() -> None:
    bare = {"number": 1, "body": f"Fixes the ticket #{ots.ISSUE}", "html_url": "u"}
    with pytest.raises(ots.OneTicketAssertionError, match="closing #3911"):
        ots.assert_one_pull_request([bare], ots.ISSUE, [])
    other = {"number": 1, "body": "Closes #39110", "html_url": "u"}
    with pytest.raises(ots.OneTicketAssertionError, match="closing #3911"):
        ots.assert_one_pull_request([other], ots.ISSUE, [])


def test_a_second_request_after_the_bot_comment_is_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="request count: 1 -> 2"):
        ots.assert_ignored_comments(1, 2)


def test_a_missing_question_marker_is_refused() -> None:
    plain = _comment(ots.ISSUE, "1. Which spelling?\n2. Which sign?\n")
    with pytest.raises(ots.OneTicketAssertionError, match="found 0"):
        ots.assert_one_question_comment([plain], ots.ISSUE, BOT_LOGIN)


def test_question_items_other_than_one_and_two_are_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="items 1 and 2"):
        ots.assert_one_question_comment([_questions("1. Only one?\n")], ots.ISSUE, BOT_LOGIN)
    with pytest.raises(ots.OneTicketAssertionError, match="items 1 and 2"):
        ots.assert_one_question_comment([_questions("1. A?\n2. B?\n3. C?\n")], ots.ISSUE, BOT_LOGIN)


def test_a_missing_investigating_line_is_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="was never seen"):
        ots.assert_first_response(T0, None)
    with pytest.raises(ots.OneTicketAssertionError, match="took 181 seconds"):
        ots.assert_first_response(T0, T0 + timedelta(seconds=181))


def test_an_answer_received_line_later_than_the_bound_is_refused() -> None:
    asking = _request(1, "completed", cause="questions_asked")
    second = _request(2, "running", created="2026-11-19T12:00:10Z")
    with pytest.raises(ots.OneTicketAssertionError, match="Answer received"):
        ots.assert_answer_admitted(
            [asking, second], str(asking["id"]), T0, T0 + timedelta(seconds=181)
        )


def test_a_second_request_later_than_the_bound_after_the_answer_is_refused() -> None:
    asking = _request(1, "completed", cause="questions_asked")
    late = _request(2, "running", created="2026-11-19T12:03:01Z")
    with pytest.raises(ots.OneTicketAssertionError, match="the second request took 181 seconds"):
        ots.assert_answer_admitted([asking, late], str(asking["id"]), T0, T0)
    with pytest.raises(ots.OneTicketAssertionError, match="found 1"):
        ots.assert_answer_admitted([asking], str(asking["id"]), T0, T0)


def test_a_question_ending_that_left_a_request_running_is_refused() -> None:
    issue = {"labels": [{"name": ots.QUESTIONS_LABEL}]}
    with pytest.raises(ots.OneTicketAssertionError, match="still running"):
        ots.assert_questions_state(issue, [_request(1, "running")])
    with pytest.raises(ots.OneTicketAssertionError, match="lacks the"):
        ots.assert_questions_state(
            {"labels": []}, [_request(1, "completed", cause="questions_asked")]
        )


def test_a_third_request_at_the_end_is_refused() -> None:
    items = [{"id": "w"}]
    pull = {"html_url": "https://example.test/pull/1"}
    final = [{"body": "Completed: https://example.test/pull/1"}]
    requests = [_request(1, "completed"), _request(2, "completed")]
    ots.assert_finished(items, requests, final, pull)
    with pytest.raises(ots.OneTicketAssertionError, match="two requests at the end, found 3"):
        ots.assert_finished(items, [*requests, _request(3, "waiting")], final, pull)


def test_the_wrong_work_item_shape_is_refused() -> None:
    item = {"id": "w", "agent_id": "a", "trigger_label": ots.LABEL, "cancelled_at": None}
    assert ots.assert_one_work_item([item], "a") is item
    with pytest.raises(ots.OneTicketAssertionError, match="found 2"):
        ots.assert_one_work_item([item, item], "a")
    with pytest.raises(ots.OneTicketAssertionError, match="found 0"):
        ots.assert_one_work_item([], "a")
    with pytest.raises(ots.OneTicketAssertionError, match="another agent"):
        ots.assert_one_work_item([item], "b")
    with pytest.raises(ots.OneTicketAssertionError, match="another label"):
        ots.assert_one_work_item([{**item, "trigger_label": "other"}], "a")
    with pytest.raises(ots.OneTicketAssertionError, match="cancelled"):
        ots.assert_one_work_item([{**item, "cancelled_at": "2026-11-19T12:00:00Z"}], "a")


def test_two_question_comments_are_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="found 2"):
        ots.assert_one_question_comment([_questions(), _questions()], ots.ISSUE, BOT_LOGIN)


def test_a_question_comment_from_someone_else_is_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="not posted by the App"):
        ots.assert_one_question_comment(
            [{**_questions(), "user": {"login": "someone"}}], ots.ISSUE, BOT_LOGIN
        )


def test_an_extra_pull_request_is_refused() -> None:
    pull = {"number": 2, "body": f"Closes #{ots.ISSUE}", "html_url": "u"}
    other = {"number": 3, "body": "Unrelated", "html_url": "v"}
    with pytest.raises(ots.OneTicketAssertionError, match="found 2"):
        ots.assert_one_pull_request([pull, other], ots.ISSUE, [1])
    with pytest.raises(ots.OneTicketAssertionError, match="found 0"):
        ots.assert_one_pull_request([], ots.ISSUE, [])
    with pytest.raises(ots.OneTicketAssertionError, match="found 2"):
        ots.assert_one_pull_request([pull, {**pull, "number": 4}], ots.ISSUE, [])


def test_an_older_pull_request_edited_to_close_the_issue_cannot_stand_in() -> None:
    older = {"number": 1, "body": f"Closes #{ots.ISSUE}", "html_url": "u"}
    unrelated = {"number": 2, "body": "Unrelated", "html_url": "v"}
    with pytest.raises(ots.OneTicketAssertionError, match="not one closing"):
        ots.assert_one_pull_request([older, unrelated], ots.ISSUE, [1])


def test_a_pull_request_opened_before_the_answer_is_refused() -> None:
    ots.assert_no_new_pulls([{"number": 1}], [1])
    with pytest.raises(ots.OneTicketAssertionError, match="before the answer: \\[2\\]"):
        ots.assert_no_new_pulls([{"number": 1}, {"number": 2}], [1])


def test_a_line_seen_before_its_cause_is_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="before its cause"):
        ots.assert_first_response(T0, T0 - timedelta(seconds=1))
    asking = _request(1, "completed", cause="questions_asked")
    second = _request(2, "running", created="2026-11-19T12:00:10Z")
    with pytest.raises(ots.OneTicketAssertionError, match="before its cause"):
        ots.assert_answer_admitted(
            [asking, second], str(asking["id"]), T0, T0 - timedelta(seconds=5)
        )


def test_a_status_line_is_placed_by_the_comment_that_carried_it_first() -> None:
    marker = "<!-- curie-execution-request:r1 -->"
    line = ots.INVESTIGATING_LINE
    history = [
        {"at": "2026-11-19T12:00:00Z", "body": f"{marker}\nQueued."},
        {"at": "2026-11-19T12:00:02Z", "body": f"{marker}\n{line}\nworking"},
        {"at": "2026-11-19T12:00:09Z", "body": f"{marker}\nDone."},
    ]
    found = ots.first_line_instant(history, marker, line)
    assert found == ots.instant("2026-11-19T12:00:02Z")
    # The line later dropped from the body is still placed; another request's
    # comment, or a body without the marker, never is.
    assert ots.first_line_instant(history, "<!-- curie-execution-request:r2 -->", line) is None
    bare = [{"at": "2026-11-19T12:00:00Z", "body": line}]
    assert ots.first_line_instant(bare, marker, line) is None


def _receipt(comment: int, outcome: str, request: str | None) -> dict[str, Any]:
    return {"comment_key": str(comment), "outcome": outcome, "request_id": request}


def test_the_answer_receipt_names_the_second_request() -> None:
    ots.assert_answer_receipts([_receipt(6, "admitted", "req2")], "6", "req2")


def test_an_answer_that_did_not_start_the_second_request_is_refused() -> None:
    not_admitted = [_receipt(6, "not_after_question", None)]
    with pytest.raises(ots.OneTicketAssertionError, match="not admitted"):
        ots.assert_answer_receipts(not_admitted, "6", "req2")
    elsewhere = [_receipt(6, "admitted", "other")]
    with pytest.raises(ots.OneTicketAssertionError, match="another request"):
        ots.assert_answer_receipts(elsewhere, "6", "req2")
    with pytest.raises(ots.OneTicketAssertionError, match="no receipt for the answer"):
        ots.assert_answer_receipts([], "6", "req2")


def test_a_request_started_by_a_comment_other_than_the_answer_is_refused() -> None:
    extra = [_receipt(6, "admitted", "req2"), _receipt(7, "queued_follow_up", "req3")]
    with pytest.raises(ots.OneTicketAssertionError, match="started by comments"):
        ots.assert_answer_receipts(extra, "6", "req2")


# Handoff: the stub's comment payload through the production intake ---------


@pytest.fixture
def stub(tmp_path: Path) -> Iterator[Any]:
    issue_file = tmp_path / "issue.md"
    issue_file.write_text("Seed issue.\n")
    server = GITHUB.GithubStub(
        tmp_path, json.loads(RECORDING.read_text()), seed_tree=SEED_TREE, issue_file=issue_file
    )
    try:
        server.start()
        issue = server._issue(ots.ISSUE, "Fixture", "Body", [ots.LABEL])
        server._put("issue", ots.ISSUE, issue)
        yield server
    finally:
        server.close()


def test_the_stub_keeps_every_body_a_comment_held_in_order(stub: Any) -> None:
    comment = stub.append_comment(ots.ISSUE, "Investigating.", author=GITHUB.HUMAN)
    stub.edit_comment_body(comment["id"], "Done.")

    history = stub.comment_history(comment["id"])

    assert [revision["body"] for revision in history] == ["Investigating.", "Done."]
    assert stub._get("comment", comment["id"])["body"] == "Done."
    assert ots.instant(history[1]["at"]) >= ots.instant(history[0]["at"])


def _tracker(stub: Any, client: httpx.AsyncClient) -> GitHubTracker:
    async def own_login() -> str:
        return BOT_LOGIN

    return GitHubTracker(
        client,
        api=f"{stub.base_url}/api/v3",
        html_base=stub.base_url,
        repo_full_name=REPOSITORY,
        repository_id=1,
        token=static_token("example-installation-token"),
        label=ots.LABEL,
        mention="curie-factory-bot",
        app_id="51",
        own_login=own_login,
        page_size=100,
    )


async def _listed(stub: Any, since: str) -> tuple[GitHubTracker, list[dict[str, Any]]]:
    context = ssl.create_default_context(cafile=str(stub.ca_file))
    async with httpx.AsyncClient(verify=context, trust_env=False) as client:
        tracker = _tracker(stub, client)
        listed, _ = await tracker.issue_comments_since(since, None)
        assert listed is not None
        return tracker, [c for c in listed if isinstance(c, dict)]


def _intake_verdicts(stub: Any, since: str) -> dict[int, tuple[bool, str]]:
    """For each listed comment: (edited per the poll pass, the answer-actor verdict)."""

    async def decide() -> dict[int, tuple[bool, str]]:
        context = ssl.create_default_context(cafile=str(stub.ca_file))
        async with httpx.AsyncClient(verify=context, trust_env=False) as client:
            tracker = _tracker(stub, client)
            listed, _ = await tracker.issue_comments_since(since, None)
            assert listed is not None
            threshold = ots.instant(since)
            app_slug = await tracker.app_slug()
            verdicts: dict[int, tuple[bool, str]] = {}
            for comment in listed:
                notice = factory_poll_intake._answer_notice(
                    comment, tracker, installation_id=5501, mention=False, app_slug=app_slug
                )
                assert notice is not None, "the listed comment names no issue comment"
                try:
                    actor = label_actor(
                        {
                            "actor": comment.get("user"),
                            "performed_via_github_app": comment.get("performed_via_github_app"),
                        },
                        app_id=tracker.app_id,
                    )
                    permission = await tracker.label_permission(
                        dataclasses.replace(notice, sender_id=actor[0], sender_login=actor[1])
                    )
                    verdict = f"answer:{actor[1]}:{permission}"
                except FeedbackIgnored as exc:
                    verdict = f"refused:{exc.code}"
                verdicts[int(comment["id"])] = (
                    factory_poll_intake._edited(comment, threshold),
                    verdict,
                )
            return verdicts

    return asyncio.run(decide())


class _NoFacts:
    """The facts exchange the poll pass builds; it has nothing owed in this proof."""

    def __init__(self, *_args: Any) -> None: ...

    async def recover_owed(self) -> None: ...

    async def observe(self, *_args: Any, **_kwargs: Any) -> bool:
        return False

    async def deliver_owed(self) -> None: ...


def _offered_to_the_answer_intake(
    stub: Any, monkeypatch: pytest.MonkeyPatch, since: str
) -> list[int]:
    """Run the production poll pass over the stub and return the comments it offered as answers.

    Everything the pass reads from GitHub is the stub's real output through the
    production tracker. The seams that need a database are replaced: the facts
    exchange, the open-question and owned-pull lookups, and the answer intake
    itself, which records the comment it is handed.
    """

    offered: list[int] = []

    async def record(
        _maker: Any, _settings: Any, _tracker: Any, _notice: Any, comment: Any
    ) -> bool:
        offered.append(int(comment["id"]))
        return True

    async def numbers(*_args: Any) -> set[int]:
        return {ots.ISSUE}

    async def nothing(*_args: Any, **_kwargs: Any) -> list[int]:
        return []

    async def no_retries(*_args: Any, **_kwargs: Any) -> set[int]:
        return set()

    monkeypatch.setattr(factory_poll_intake, "FactsExchange", _NoFacts)
    monkeypatch.setattr(factory_poll_intake, "drain_answer_retries", no_retries)
    monkeypatch.setattr(factory_poll_intake, "_issue_numbers", numbers)
    monkeypatch.setattr(factory_poll_intake, "_answering_issue_numbers", numbers)
    monkeypatch.setattr(factory_poll_intake, "open_pulls", nothing)
    monkeypatch.setattr(factory_poll_intake, "_admit_answer", record)
    monkeypatch.setattr(factory_poll_intake, "repository_ref", lambda *_a, **_k: "repository")

    async def poll() -> None:
        context = ssl.create_default_context(cafile=str(stub.ca_file))
        async with httpx.AsyncClient(verify=context, trust_env=False) as client:
            cursor = factory_poll_intake.Cursor(comments_since=ots.instant(since))
            await factory_poll_intake._admit_mentions(
                object(),  # type: ignore[arg-type]
                object(),  # type: ignore[arg-type]
                client,
                cursor,
                _tracker(stub, client),
                now=ots.instant(since),
                api=f"{stub.base_url}/api/v3",
                token="example-installation-token",
                repo_path=REPOSITORY,
                installation_id=5501,
                repository={},
                verified=None,  # type: ignore[arg-type]
                serves=True,
            )

    asyncio.run(poll())
    return offered


def test_the_production_poll_pass_offers_the_stubs_fresh_answer_and_never_the_edited_one(
    stub: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The handoff: the stub's own comment output goes through ``_admit_mentions``.

    The fresh answer is offered to the answer intake; the same text edited in
    place after posting, which the stub stamps with a later ``updated_at``, is
    refused by the pass and never offered. With the pass disabled nothing is
    offered and the first assertion fails.
    """

    answer = stub.append_comment(ots.ISSUE, run.ANSWER_TEXT, author=GITHUB.HUMAN)
    edited = stub.append_comment(ots.ISSUE, "Placeholder.", author=GITHUB.HUMAN)
    stub.edit_comment_body(edited["id"], run.ANSWER_TEXT)
    since = (ots.instant(answer["created_at"]) - timedelta(seconds=30)).isoformat()

    offered = _offered_to_the_answer_intake(stub, monkeypatch, since)

    assert offered == [answer["id"]]
    assert edited["id"] not in offered


def test_the_production_poll_pass_refuses_an_answer_posted_before_its_threshold(
    stub: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    answer = stub.append_comment(ots.ISSUE, run.ANSWER_TEXT, author=GITHUB.HUMAN)
    after = (ots.instant(answer["created_at"]) + timedelta(seconds=5)).isoformat()

    offered = _offered_to_the_answer_intake(stub, monkeypatch, after)

    assert offered == []


def test_the_helper_comment_is_read_as_an_answer_by_the_production_poll_rules(stub: Any) -> None:
    stub.add_user(run.READER, "read")
    before = "2020-01-01T00:00:00Z"
    answer = stub.append_comment(ots.ISSUE, run.ANSWER_TEXT, author=GITHUB.HUMAN)
    reader = stub.append_comment(ots.ISSUE, "Me too.", author=run.READER)
    other = stub.append_comment(ots.ISSUE, "Automated.", author=run.OTHER_BOT)
    assert answer["created_at"] == answer["updated_at"]

    verdicts = _intake_verdicts(stub, before)

    assert verdicts[answer["id"]] == (False, "answer:octocat:None")
    assert verdicts[reader["id"]] == (False, "answer:onlooker:actor_no_permission")
    assert verdicts[other["id"]] == (False, "refused:actor_bot")


def test_a_comment_the_stub_marks_edited_is_refused_by_the_production_poll_pass(
    stub: Any,
) -> None:
    """The named counterexample: an answer whose ``updated_at`` is later than its ``created_at``.

    ``add_issue_comment`` stamps the two timestamps from separate clock reads
    and a later body edit moves ``updated_at`` on. The poll pass treats either
    as an edit and never as an answer, so a scenario that used that shape for
    its answer would wait forever for a second request.
    """

    answer = stub.append_comment(ots.ISSUE, run.ANSWER_TEXT, author=GITHUB.HUMAN)
    edited = stub.append_comment(ots.ISSUE, "Placeholder.", author=GITHUB.HUMAN)
    stub.edit_comment_body(edited["id"], run.ANSWER_TEXT)

    verdicts = _intake_verdicts(stub, "2020-01-01T00:00:00Z")

    assert verdicts[answer["id"]][0] is False
    assert verdicts[edited["id"]][0] is True


def test_the_helper_comment_is_refused_when_it_predates_the_poll_threshold(stub: Any) -> None:
    """Liveness of the same rule from the other side: a threshold after the post is an edit."""

    answer = stub.append_comment(ots.ISSUE, run.ANSWER_TEXT, author=GITHUB.HUMAN)
    later = (ots.instant(answer["created_at"]) + timedelta(seconds=5)).isoformat()

    assert _intake_verdicts(stub, later)[answer["id"]][0] is True


# Scripted lanes -------------------------------------------------------------


def _frame(issue: int) -> str:
    return (
        "[platform-sender curie-sender-boundary]\nevent: job\nchannel: github\n"
        "person: none\nrole: scheduled run\n[user-message curie-sender-boundary]\n"
        f"https://192.0.2.1:8443/acme-corp/acme-bot/issues/{issue}\n"
        "[end-user-message curie-sender-boundary]"
    )


def _turn(issue: int, ordinal: int) -> bytes:
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": [{"type": "text", "text": _frame(issue)}]}
    ]
    for index in range(ordinal):
        messages.append({"role": "assistant", "content": [{"type": "text", "text": str(index)}]})
        messages.append({"role": "user", "content": [{"type": "text", "text": "ok"}]})
    return json.dumps(
        {"system": [{"type": "text", "text": "Dark factory."}], "messages": messages}
    ).encode()


def _tools(exchange: dict[str, Any] | None) -> list[str]:
    assert exchange is not None
    return [b["name"] for b in exchange["message"]["content"] if b.get("type") == "tool_use"]


@pytest.fixture
def served() -> Iterator[tuple[Any, lanes.LaneRouter]]:
    model = _load("one_ticket_model_script", MODEL_SCRIPT)
    script = model.ModelScript(REPO_ROOT / "tools/model-script/transcripts/unitconv-issue.json")
    router = lanes.LaneRouter({})
    router.add(run.build_lane(model))
    router.install(script, model)
    yield script, router
    script._server.server_close()


def test_the_transcript_serves_the_asking_session_then_the_publishing_one(
    served: tuple[Any, lanes.LaneRouter],
) -> None:
    script, router = served

    # Session 1 ends on its ask: the platform stops the turn once the questions
    # are posted and the worker does not re-prompt it (#5785), so its last
    # exchange is the ask. Session 2 then starts again from ordinal 0.
    first = [script._match("POST", "/v1/messages", _turn(ots.ISSUE, n)) for n in range(4)]
    second = [script._match("POST", "/v1/messages", _turn(ots.ISSUE, n)) for n in range(19)]

    assert "mcp__curie__ask_questions" in _tools(first[3])
    assert first[3]["message"]["stop_reason"] == "tool_use"
    assert "mcp__curie__ask_questions" not in _tools(second[0])
    assert all("mcp__curie__publish_changes" not in _tools(e) for e in first)
    assert "mcp__curie__publish_changes" in _tools(second[18])
    assert all("mcp__curie__ask_questions" not in _tools(e) for e in second)
    assert router.unrouted == []
    served_ordinals = router.evidence()["one-ticket"]["served_ordinals"]
    assert served_ordinals == [*range(4), *range(19)]


def test_the_asked_questions_are_exactly_two_and_the_publication_closes_the_issue() -> None:
    model = _load("one_ticket_model_script_shape", MODEL_SCRIPT)
    exchanges = model.load_transcript(run.TRANSCRIPT)
    calls = [
        block
        for exchange in exchanges
        for block in exchange["message"]["content"]
        if block.get("type") == "tool_use"
    ]
    (ask,) = [c for c in calls if c["name"] == "mcp__curie__ask_questions"]
    assert len(ask["input"]["questions"]) == 2
    (publish,) = [c for c in calls if c["name"] == "mcp__curie__publish_changes"]
    assert f"Closes #{ots.ISSUE}" in publish["input"]["body"]
    assert "3815" not in json.dumps(exchanges)


def test_the_github_lane_publication_cites_the_answer_its_labeller_posted() -> None:
    """Session 2's body cites the answer the GitHub lane admits, by its author.

    The lane's one authorized answer is ``ANSWER_TEXT``, posted by the stub's
    labeller, and a GitHub ticket read names a comment's author by login. The
    body must carry that citation, which is also what the dark factory's
    reproduction gate requires once the ticket carries the factory's questions,
    inside a Decisions section the review gate's grammar accepts. The runner
    honours those gates' denies, so a body either refuses would be denied in
    the run and the model asked for an exchange the transcript does not hold.
    """

    reproduction = _load("one_ticket_reproduction_gate", REPRODUCTION_GATE)
    decisions = _load("one_ticket_decisions_section", DECISIONS_SECTION)
    model = _load("one_ticket_model_script_gates", MODEL_SCRIPT)
    (body,) = run.publication_bodies(model.load_transcript(run.TRANSCRIPT))

    assert run.ANSWER_AUTHOR == GITHUB.HUMAN["login"]
    citation = f"Answered on the ticket by {GITHUB.HUMAN['login']}: {run.ANSWER_TEXT}"
    assert run.answer_citation(GITHUB.HUMAN["login"]) == citation
    assert body.splitlines().count(citation) == 1
    assert [
        line for line in body.splitlines() if line.startswith("Answered on the ticket by ")
    ] == [citation]
    assert reproduction.cited(body)
    assert len(decisions.parse_decisions_section(body)) == 2


def test_the_publication_scenario_routes_a_fixture_issue_it_never_labelled_to_the_fallback(
    served: tuple[Any, lanes.LaneRouter],
) -> None:
    script, router = served
    recorded = script._match("POST", "/v1/messages", _turn(3815, 0))
    assert _tools(recorded) == ["Skill"]
    assert router.unrouted == []


def test_the_scenario_observation_names_only_the_fixture_issue() -> None:
    sql = run.observation_sql(ots.ISSUE)
    assert f"tracker_issue_id = '{ots.ISSUE}'" in sql
    assert sql.count("tracker_issue_id") == 4


def test_the_scenario_observation_reads_only_columns_the_head_schema_has() -> None:
    from curie_api.models import Base

    sql = run.observation_sql(ots.ISSUE)
    columns = {table.name: set(table.columns.keys()) for table in Base.metadata.tables.values()}
    aliases = dict((alias, table) for table, alias in re.findall(r"curie\.(\w+) (\w)\b", sql))
    references = set(re.findall(r"\b([a-z])\.(\w+)", sql))
    assert set(aliases) == {alias for alias, _ in references}
    missing = sorted(f"{aliases[a]}.{c}" for a, c in references if c not in columns[aliases[a]])
    assert missing == []
