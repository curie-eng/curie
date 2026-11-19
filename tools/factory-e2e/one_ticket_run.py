"""Drive the ``one-ticket`` scenario on the scripted command's shared install.

It runs after the issue-to-pull-request scenario, on the same cluster, GitHub
stub, model endpoint and API. One human label goes on one fresh issue, and the
flow the platform owes that issue is observed from outside:

1. the factory says it is investigating, and session 1 asks two questions on
   the issue with the ``ask_questions`` tool;
2. a comment from a person without write permission and a comment from a bot
   start nothing;
3. the plain answer from the labeller starts the second request, which says it
   received the answer and ends in one pull request that closes the issue;
4. a second plain comment from the labeller, posted with the answer, starts
   nothing more.

Every observation is external: GitHub stub state, the model lanes and read-only
SQL. ``one_ticket_scenario`` holds the assertions.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import factory_e2e as fe
import one_ticket_scenario as ots
import scripted_lanes as lanes

REPO_ROOT = Path(__file__).resolve().parents[2]
TRANSCRIPT = REPO_ROOT / "tools/model-script/transcripts/one-ticket.json"
ISSUE_FILE = REPO_ROOT / "tools/factory-e2e/fixtures/one-ticket-issue.md"
REPOSITORY = "acme-corp/acme-bot"
# One read of the issue comments per intake pass.
COMMENT_LIST_PATH = f"/repos/{REPOSITORY}/issues/comments"
# A comment is judged ignored only after this many intake passes read the
# tracker's comments since it was posted.
QUIET_POLLS = 3
READER = {"id": 6610, "login": "onlooker", "type": "User"}
OTHER_BOT = {"id": 6611, "login": "helper-bot[bot]", "type": "Bot"}
ANSWER_TEXT = "Accept `k` and also `kelvin`. Refuse a Kelvin value below zero with an error."
FOLLOW_UP_TEXT = "Thanks for asking, that is all from me."


class OneTicketError(RuntimeError):
    """The one-ticket scenario could not be driven to an observation."""


def build_lane(model_module: Any) -> lanes.Lane:
    """The factory agent's two sessions on the fixture issue, in the order they run."""

    exchanges = model_module.load_transcript(TRANSCRIPT)
    return lanes.Lane("one-ticket", lanes.IMPLEMENTATION, ots.ISSUE, list(exchanges))


def issue_title_and_body() -> tuple[str, str]:
    """The fixture issue: first line the title, the rest the body."""

    text = ISSUE_FILE.read_text()
    title, _, body = text.partition("\n")
    return title.removeprefix("# ").strip(), body.lstrip("\n")


def observation_sql(issue: int) -> str:
    """One read-only JSON observation of the ticket's durable records."""

    number = int(issue)
    return (
        "SELECT json_build_object("
        "'work_items',(SELECT coalesce(json_agg(json_build_object("
        "'id',w.id,'agent_id',w.agent_id,'trigger_label',w.trigger_label,"
        "'cancelled_at',w.cancelled_at)),'[]'::json) "
        f"FROM curie.work_items w WHERE w.tracker_issue_id = '{number}'),"
        "'requests',(SELECT coalesce(json_agg(json_build_object("
        "'id',r.id,'sequence',r.sequence,'status',r.status,"
        "'terminal_cause',r.terminal_cause,'completion_contract',r.completion_contract,"
        "'reply_target_kind',r.reply_target_kind,'created_at',r.created_at,"
        "'terminal_at',r.terminal_at) ORDER BY r.sequence),'[]'::json) "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id=r.work_item_id "
        f"WHERE w.tracker_issue_id = '{number}'),"
        "'rounds',(SELECT coalesce(json_agg(json_build_object("
        "'state',q.state,'rounds_used',q.rounds_used)),'[]'::json) "
        "FROM curie.factory_question_rounds q JOIN curie.work_items w ON w.id=q.work_item_id "
        f"WHERE w.tracker_issue_id = '{number}'))"
    )


def now() -> datetime:
    return datetime.now(UTC)


class OneTicket:
    """One run of the scenario against a ScriptedRun's live install."""

    def __init__(self, driver: Any, router: lanes.LaneRouter):
        self.driver = driver
        self.stub = driver.stub
        self.router = router
        self.evidence: dict[str, Any] = {"scenario": "one-ticket", "result": "failed"}
        self.sightings: dict[str, datetime] = {}
        self.baseline_issues: list[int] = []
        self.baseline_pulls = 0

    # Observation -----------------------------------------------------------

    def check_boundaries(self) -> None:
        if self.driver.model.unexpected:
            raise OneTicketError(f"unexpected model request: {self.driver.model.unexpected[0]}")
        if self.router.unrouted:
            raise OneTicketError(f"unrouted model request: {self.router.unrouted[0]}")
        if self.stub.unknown_requests:
            raise OneTicketError("unsupported stub request")

    def observe(self) -> dict[str, Any]:
        observed: dict[str, Any] = self.driver.sql(observation_sql(ots.ISSUE))
        return observed

    def comments(self) -> list[dict[str, Any]]:
        return [c for c in self.stub._rows("comment") if c["issue_number"] == ots.ISSUE]

    def issue(self) -> dict[str, Any]:
        found = self.stub._get("issue", ots.ISSUE)
        if found is None:
            raise OneTicketError("the fixture issue is gone")
        return found

    def sight(self, name: str, line: str, bot_login: str) -> None:
        """Stamp the first moment the factory's own comment carried ``line``.

        A status comment is edited in place and drops its lead line once its
        run ends, so the line is only ever observable while it is live.
        """

        if name in self.sightings:
            return
        for comment in self.comments():
            if comment["user"]["login"] == bot_login and line in comment["body"]:
                self.sightings[name] = now()
                return

    def wait(self, what: str, ready: Any, github: Any) -> Any:
        """Poll until ``ready()`` answers truthy, stamping the visible status lines."""

        deadline = self.driver.deadline
        assert deadline is not None
        bot = github.BOT["login"]
        while time.monotonic() < deadline:
            self.check_boundaries()
            self.sight("investigating", ots.INVESTIGATING_LINE, bot)
            self.sight("answer_received", ots.ANSWER_RECEIVED_LINE, bot)
            found = ready()
            if found:
                return found
            time.sleep(1)
        raise OneTicketError(f"{what} did not happen inside the command bound")

    def comment_reads(self) -> int:
        return sum(
            1
            for entry in self.stub.request_log
            if entry["method"] == "GET" and entry["path"].endswith(COMMENT_LIST_PATH)
        )

    def wait_for_intake_passes(self, since: int, github: Any) -> None:
        """Wait until the intake has listed the comments QUIET_POLLS times since ``since``.

        Nothing happening is only evidence once the intake has looked.
        """

        poll = float(self.driver.tracker_poll_seconds)
        bound = time.monotonic() + 4 * QUIET_POLLS * poll
        if self.driver.deadline is not None:
            bound = min(bound, self.driver.deadline)

        def looked() -> bool:
            if time.monotonic() >= bound:
                raise OneTicketError(
                    f"the intake listed comments {self.comment_reads() - since} times, "
                    f"expected {QUIET_POLLS}"
                )
            return self.comment_reads() - since >= QUIET_POLLS

        self.wait("the intake passes", looked, github)

    # Steps -----------------------------------------------------------------

    def label(self, github: Any) -> datetime:
        """One human label on one fresh issue; returns the label event's instant."""

        title, body = issue_title_and_body()
        self.baseline_issues = [int(row["number"]) for row in self.stub._rows("issue")]
        self.baseline_pulls = len(self.stub._rows("pull"))
        issue = self.stub._issue(ots.ISSUE, title, body, [ots.LABEL])
        issue["user"] = github.HUMAN
        self.stub._put("issue", ots.ISSUE, issue)
        self.stub._label_event(ots.ISSUE, ots.LABEL, "labeled", github.HUMAN)
        events = [e for e in self.stub._rows("event") if e["issue_number"] == ots.ISSUE]
        return ots.instant(events[-1]["created_at"])

    def asked(self, github: Any) -> dict[str, Any]:
        """Wait for session 1's questions and for its request to finish asking."""

        def ready() -> dict[str, Any] | None:
            if not ots.question_comments(self.comments(), ots.ISSUE):
                return None
            observed = self.observe()
            requests = observed["requests"]
            if len(requests) != 1 or requests[0]["status"] in ots.LIVE_STATUSES:
                return None
            if ots.QUESTIONS_LABEL not in ots.labels_of(self.issue()):
                return None
            return observed

        return dict(self.wait("the questions ending", ready, github))

    def answered(self, github: Any, asking: str) -> dict[str, Any]:
        """Wait for the second request to end with its final comment on the ticket."""

        def ready() -> dict[str, Any] | None:
            observed = self.observe()
            requests = observed["requests"]
            if len(requests) < 2 or str(requests[1]["id"]) == asking:
                return None
            if requests[1]["status"] in ots.LIVE_STATUSES:
                return None
            if not self.final_comments(str(requests[1]["id"])):
                return None
            return observed

        return dict(self.wait("the answered run", ready, github))

    def final_comments(self, request_id: str) -> list[dict[str, Any]]:
        """The request's own status comment once its run ended."""

        marker = f"<!-- curie-execution-request:{request_id} -->"
        return [c for c in self.comments() if marker in c["body"] and fe._NOTICE_FINAL in c["body"]]

    def execute(self, github: Any) -> None:
        bot = github.BOT["login"]
        agent = str(self.driver.evidence["agent_id"])
        self.stub.add_user(READER, "read")
        self.stub.add_user(OTHER_BOT, "write")
        label_at = self.label(github)
        self.evidence.update(
            issue=ots.ISSUE,
            labeller={"id": github.HUMAN["id"], "login": github.HUMAN["login"]},
            reader={"id": READER["id"], "login": READER["login"]},
            other_bot={"id": OTHER_BOT["id"], "login": OTHER_BOT["login"]},
        )

        # Session 1 asks.
        observed = self.asked(github)
        item = ots.assert_one_work_item(observed["work_items"], agent)
        ots.assert_first_response(label_at, self.sightings.get("investigating"))
        ots.assert_one_question_comment(self.comments(), ots.ISSUE, bot)
        asking = ots.assert_questions_state(self.issue(), observed["requests"])
        self.evidence.update(work_item_id=str(item["id"]), asking_request_id=str(asking["id"]))

        # Comments that are not answers start nothing.
        since = self.comment_reads()
        self.stub.append_comment(ots.ISSUE, "I would also like this.", author=READER)
        self.stub.append_comment(ots.ISSUE, "Automated notice.", author=OTHER_BOT)
        self.wait_for_intake_passes(since, github)
        ots.assert_ignored_comments(len(observed["requests"]), len(self.observe()["requests"]))

        # The labeller's plain answer, and a second plain comment beside it.
        answer = self.stub.append_comment(ots.ISSUE, ANSWER_TEXT, author=github.HUMAN)
        follow_up = self.stub.append_comment(ots.ISSUE, FOLLOW_UP_TEXT, author=github.HUMAN)
        answer_at = ots.instant(answer["created_at"])
        observed = self.answered(github, str(asking["id"]))
        second = ots.assert_answer_admitted(
            observed["requests"],
            str(asking["id"]),
            answer_at,
            self.sightings.get("answer_received"),
        )

        # Session 2 publishes, and the follow-up starts nothing.
        since = self.comment_reads()
        self.wait_for_intake_passes(since, github)
        observed = self.observe()
        pulls = [self.stub._fresh_pull(row) for row in self.stub._rows("pull")]
        pull = ots.assert_one_pull_request(pulls, ots.ISSUE, self.baseline_pulls)
        ots.assert_no_factory_issues(self.baseline_issues, self.stub._rows("issue"))
        final = self.final_comments(str(second["id"]))
        ots.assert_finished(observed["work_items"], observed["requests"], final, pull)
        self.evidence.update(
            second_request_id=str(second["id"]),
            answer_comment_id=answer["id"],
            follow_up_comment_id=follow_up["id"],
            pull_request=pull["html_url"],
            investigating_seconds=(self.sightings["investigating"] - label_at).total_seconds(),
            answer_received_seconds=(self.sightings["answer_received"] - answer_at).total_seconds(),
            second_request_seconds=(ots.instant(second["created_at"]) - answer_at).total_seconds(),
            lanes=self.router.evidence(),
        )
        self.evidence["result"] = "passed"
