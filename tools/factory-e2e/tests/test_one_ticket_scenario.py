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
        "completion_contract": "implementation",
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
    pull = {"body": f"Closes #{ots.ISSUE}", "html_url": "https://example.test/pull/1"}
    assert ots.assert_one_pull_request([pull], ots.ISSUE, 0) is pull
    ots.assert_no_factory_issues([3815, ots.ISSUE], [{"number": 3815}, {"number": ots.ISSUE}])


def test_a_second_factory_created_issue_is_refused() -> None:
    with pytest.raises(ots.OneTicketAssertionError, match="created issues: \\[3999\\]"):
        ots.assert_no_factory_issues([3815, ots.ISSUE], [{"number": 3815}, {"number": 3999}])


def test_a_pull_request_without_the_closing_reference_is_refused() -> None:
    bare = {"body": f"Fixes the ticket #{ots.ISSUE}", "html_url": "u"}
    with pytest.raises(ots.OneTicketAssertionError, match="closing #3911"):
        ots.assert_one_pull_request([bare], ots.ISSUE, 0)
    other = {"body": "Closes #39110", "html_url": "u"}
    with pytest.raises(ots.OneTicketAssertionError, match="closing #3911"):
        ots.assert_one_pull_request([other], ots.ISSUE, 0)


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
        ots.assert_one_question_comment(
            [_questions("1. A?\n2. B?\n3. C?\n")], ots.ISSUE, BOT_LOGIN
        )


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
        ots.assert_questions_state({"labels": []}, [_request(1, "completed", cause="questions_asked")])


def test_a_follow_up_request_at_the_end_is_refused() -> None:
    items = [{"id": "w"}]
    pull = {"html_url": "https://example.test/pull/1"}
    final = [{"body": "Completed: https://example.test/pull/1"}]
    requests = [_request(1, "completed"), _request(2, "completed")]
    ots.assert_finished(items, requests, final, pull)
    with pytest.raises(ots.OneTicketAssertionError, match="two requests at the end, found 3"):
        ots.assert_finished(items, [*requests, _request(3, "waiting")], final, pull)


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
            verdicts: dict[int, tuple[bool, str]] = {}
            for comment in listed:
                notice = factory_poll_intake._answer_notice(
                    comment, tracker, installation_id=5501, mention=False
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
                        dataclasses.replace(
                            notice, sender_id=actor[0], sender_login=actor[1]
                        )
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

    first = [script._match("POST", "/v1/messages", _turn(ots.ISSUE, n)) for n in range(6)]
    second = [script._match("POST", "/v1/messages", _turn(ots.ISSUE, n)) for n in range(18)]

    assert "mcp__curie__ask_questions" in _tools(first[4])
    assert first[5]["message"]["stop_reason"] == "end_turn"
    assert all("mcp__curie__publish_changes" not in _tools(e) for e in first)
    assert "mcp__curie__publish_changes" in _tools(second[17])
    assert all("mcp__curie__ask_questions" not in _tools(e) for e in second)
    assert router.unrouted == []
    served_ordinals = router.evidence()["one-ticket"]["served_ordinals"]
    assert served_ordinals == [*range(6), *range(18)]


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
    assert sql.count("tracker_issue_id") == 3
