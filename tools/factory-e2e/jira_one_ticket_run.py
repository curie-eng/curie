"""Drive the ``jira-one-ticket`` scenario on the scripted command's install (#5791).

The install has one Jira Cloud tracker, the Jira stub (``tools/jira-stub``), and
the GitHub stub as the code host. One human label goes on one Jira issue, and
the flow the platform owes that issue is observed from outside:

1. the factory says it is investigating, and session 1 asks two questions in
   one Jira comment marked ``curie-factory-questions:``; the issue carries the
   ``curie-factory:questions`` label;
2. a plain comment from an account outside the start allowlist starts nothing;
3. the labeller's plain comment starts the second request, which says it
   received the answer and ends in one pull request on the GitHub stub whose
   body names the Jira key and its browse URL;
4. the Jira stub holds no issue the factory created.

Every observation is external: the Jira stub's state, the GitHub stub's pull
requests and read-only SQL. ``one_ticket_jira`` turns the Jira records into the
shapes ``one_ticket_scenario`` judges, and holds the Jira-only assertions.
"""

from __future__ import annotations

import json
import os
import secrets
import selectors
import signal
import socket
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import IO, Any

import one_ticket_jira as ojt
import one_ticket_run
import one_ticket_scenario as ots

REPO_ROOT = Path(__file__).resolve().parents[2]
STUB = REPO_ROOT / "tools/jira-stub/jira_stub.py"
TOKEN_ENV = "CURIE_JIRA_STUB_CONTROL_TOKEN"
# The agent's channel kind on a Jira binding, which the runner names in the
# platform frame of every turn it starts for the agent.
CHANNEL_KIND = "jira"
# ``uv run`` may first build the workspace environment.
START_SECONDS = 900
OUTSIDER_TEXT = "I would also like this."
# Past the last recorded check run, as the publication scenario advances it.
CHECK_REPLAY_SECONDS = 1800


class JiraOneTicketError(RuntimeError):
    """The Jira one-ticket scenario could not be driven to an observation."""


def transcript_for_jira(exchanges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The one-ticket transcript as the factory agent's Jira turns key it.

    The recording names the GitHub channel in the platform frame of each
    factory turn; a Jira-bound agent's frame names ``jira``. Nothing else in
    the transcript changes.
    """

    old, new = "\nchannel: github\n", f"\nchannel: {CHANNEL_KIND}\n"
    if not any(old in exchange["lane_key"] for exchange in exchanges):
        raise JiraOneTicketError("the transcript has no GitHub channel frame to key on Jira")
    return [
        {**exchange, "lane_key": exchange["lane_key"].replace(old, new)} for exchange in exchanges
    ]


class JiraStubProcess:
    """The Jira stub as a subprocess of the driver, bound to ``host``."""

    def __init__(
        self,
        workdir: Path,
        host: str,
        ca_cert: Path,
        ca_key: Path,
        *,
        python: tuple[str, ...] = ("uv", "run", "--frozen", "python"),
    ) -> None:
        """``python`` runs the stub; the default is the workspace environment."""

        self.python = python
        self.workdir = workdir
        self.host = host
        self.ca_cert = ca_cert
        self.ca_key = ca_key
        self.token = secrets.token_hex(24)
        self.port: int | None = None
        self.process: subprocess.Popen[str] | None = None
        self._log: IO[str] | None = None

    @property
    def netloc(self) -> str:
        authority = f"[{self.host}]" if ":" in self.host else self.host
        return f"{authority}:{self.port}"

    @property
    def base_url(self) -> str:
        return f"https://{self.netloc}"

    @property
    def api_url(self) -> str:
        """The scoped API gateway form, ``https://<gateway>/ex/jira/<cloudId>``."""

        return f"{self.base_url}/ex/jira/{ojt.CLOUD_ID}"

    def client(self) -> ojt.JiraStubClient:
        return ojt.JiraStubClient(self.base_url, self.ca_cert, self.token)

    def start(self, env: dict[str, str], timeout: float = START_SECONDS) -> None:
        if self.process is not None:
            raise JiraOneTicketError("the Jira stub already started")
        self.workdir.mkdir(parents=True, exist_ok=True)
        self._log = (self.workdir / "jira-stub.log").open("w")
        environment = dict(env)
        environment[TOKEN_ENV] = self.token
        self.process = subprocess.Popen(
            [
                *self.python,
                str(STUB),
                "serve",
                "--host",
                self.host,
                "--ca-cert",
                str(self.ca_cert),
                "--ca-key",
                str(self.ca_key),
                "--workdir",
                str(self.workdir / "tls"),
            ],
            cwd=REPO_ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=self._log,
            text=True,
            # Its own process group: ``uv run`` starts the server as a child, and
            # teardown has to stop the server, not only the wrapper.
            start_new_session=True,
        )
        assert self.process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(self.process.stdout, selectors.EVENT_READ)
        try:
            if not selector.select(timeout=timeout):
                raise JiraOneTicketError("the Jira stub did not start")
            line = self.process.stdout.readline()
        finally:
            selector.close()
        try:
            self.port = int(json.loads(line)["port"])
        except (ValueError, KeyError, TypeError):
            raise JiraOneTicketError("the Jira stub exited before it was ready") from None

    def listening(self) -> bool:
        if self.port is None:
            return False
        with socket.socket(socket.AF_INET6 if ":" in self.host else socket.AF_INET) as sock:
            sock.settimeout(1)
            # Linux can connect a socket to itself when its ephemeral source
            # port reuses the closed listener's port.
            return sock.connect_ex((self.host, self.port)) == 0 and (
                sock.getsockname() != sock.getpeername()
            )

    def group_alive(self) -> bool:
        """Whether any process of the stub's process group still exists."""

        if self.process is None:
            return False
        try:
            os.killpg(self.process.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _stop_group(self, sig: signal.Signals, seconds: float) -> bool:
        assert self.process is not None
        try:
            os.killpg(self.process.pid, sig)
        except ProcessLookupError:
            pass
        bound = time.monotonic() + seconds
        while time.monotonic() < bound:
            self.process.poll()
            if not self.group_alive():
                return True
            time.sleep(0.05)
        return False

    def close(self) -> dict[str, Any]:
        """Stop the stub's whole process group and report what is left.

        ``exited`` holds only when no process of the group remains, so a server
        that outlived its wrapper is reported, not hidden by the wrapper's exit.
        """

        teardown: dict[str, Any] = {"started": self.process is not None, "port": self.port}
        if self.process is not None:
            if self.process.poll() is None or self.group_alive():
                if not self._stop_group(signal.SIGTERM, 15):
                    self._stop_group(signal.SIGKILL, 15)
            self.process.poll()
            if self.process.stdout is not None:
                self.process.stdout.close()
            teardown["returncode"] = self.process.returncode
        if self._log is not None:
            self._log.close()
        teardown["exited"] = self.process is None or (
            self.process.returncode is not None and not self.group_alive()
        )
        teardown["listening"] = self.listening()
        return teardown


class JiraOneTicket:
    """One run of the scenario against a ScriptedRun's live install."""

    def __init__(self, driver: Any, stub: JiraStubProcess) -> None:
        self.driver = driver
        self.github = driver.stub
        self.stub = stub
        self.jira = stub.client()
        self.evidence: dict[str, Any] = {"scenario": "jira-one-ticket", "result": "failed"}
        self.issue_id = ""

    # Observation -----------------------------------------------------------

    def state(self) -> dict[str, Any]:
        return self.jira.state()

    def check_boundaries(self, state: dict[str, Any] | None = None) -> None:
        if self.driver.model.unexpected:
            # Only the count: an audit entry carries the model's input text.
            raise JiraOneTicketError(
                f"{len(self.driver.model.unexpected)} unexpected model requests"
            )
        if self.github.unknown_requests:
            raise JiraOneTicketError("unsupported GitHub stub request")
        unrouted = (state or self.state())["unrouted"]
        if unrouted:
            raise JiraOneTicketError(f"requests no Jira route serves: {unrouted[:3]}")

    def observe(self) -> dict[str, Any]:
        observed: dict[str, Any] = self.driver.sql(
            one_ticket_run.observation_sql(int(self.issue_id))
        )
        observed["notices"] = self.driver.sql(ojt.notices_sql(self.issue_id))
        return observed

    def wait(self, what: str, ready: Any) -> Any:
        """Poll until ``ready(state)`` answers truthy."""

        deadline = self.driver.deadline
        assert deadline is not None
        while time.monotonic() < deadline:
            state = self.state()
            self.check_boundaries(state)
            found = ready(state)
            if found:
                return found
            time.sleep(1)
        raise JiraOneTicketError(f"{what} did not happen inside the command bound")

    def reads_since(self, state: dict[str, Any], after: datetime) -> int:
        """Intake passes that read this ticket's comments after ``after``.

        The stub records each poll comment read it served (``comment_reads``);
        one pass reads the ticket's comments once, after its label search.
        """

        return sum(
            1
            for read in state["comment_reads"]
            if read["issue"] == self.issue_id and ots.instant(read["at"]) > after
        )

    def wait_for_intake_passes(self, after: datetime) -> None:
        """Wait until QUIET_POLLS intake passes have read the ticket's comments after ``after``."""

        poll = float(self.driver.tracker_poll_seconds)
        bound = time.monotonic() + 4 * one_ticket_run.QUIET_POLLS * poll
        if self.driver.deadline is not None:
            bound = min(bound, self.driver.deadline)

        def looked(state: dict[str, Any]) -> bool:
            passes = self.reads_since(state, after)
            if time.monotonic() >= bound:
                raise JiraOneTicketError(
                    f"the intake read the ticket's comments {passes} times, "
                    f"expected {one_ticket_run.QUIET_POLLS}"
                )
            return passes >= one_ticket_run.QUIET_POLLS

        self.wait("the intake passes", looked)

    def asked(self, baseline_pulls: list[int]) -> dict[str, Any]:
        """Wait for session 1's questions and for its request to finish asking."""

        issue = int(self.issue_id)

        def ready(state: dict[str, Any]) -> dict[str, Any] | None:
            if ots.new_pulls(self.github._rows("pull"), baseline_pulls):
                raise JiraOneTicketError(
                    "session 1 opened a pull request instead of ending on its questions"
                )
            if not ots.question_comments(ojt.comment_records(state, self.issue_id), issue):
                return None
            observed = self.observe()
            requests = observed["requests"]
            if len(requests) != 1 or requests[0]["status"] in ots.LIVE_STATUSES:
                return None
            labels = ojt.issue_of(state, self.issue_id)["labels"]
            if ots.QUESTIONS_LABEL not in labels:
                return None
            return observed

        return dict(self.wait("the questions ending", ready))

    def answered(self, asking: str, baseline_pulls: list[int]) -> dict[str, Any]:
        """Wait for the second request to end with its final status comment.

        The GitHub stub replays recorded check runs on its own clock. Once
        session 2's pull request exists, that clock moves past the recording,
        as the publication scenario does, so the platform's CI gate sees the
        checks finish rather than waiting out the request's deadline.
        """

        advanced = False

        def ready(state: dict[str, Any]) -> dict[str, Any] | None:
            nonlocal advanced
            if not advanced and ots.new_pulls(self.github._rows("pull"), baseline_pulls):
                self.github.advance(CHECK_REPLAY_SECONDS)
                advanced = True
                self.evidence["check_clock_advanced"] = True
            observed = self.observe()
            requests = observed["requests"]
            if len(requests) < 2 or str(requests[1]["id"]) == asking:
                return None
            if requests[1]["status"] in ots.LIVE_STATUSES:
                return None
            final = ojt.final_comments(
                state,
                self.issue_id,
                str(requests[1]["id"]),
                observed["notices"],
                ojt.BOT_ACCOUNT_ID,
            )
            return observed if final else None

        return dict(self.wait("the answered run", ready))

    # Steps -----------------------------------------------------------------

    def execute(self) -> None:
        try:
            self.drive()
        finally:
            try:
                self.evidence.update(self.outcome())
            except (OSError, ValueError, KeyError, ojt.JiraStubError) as exc:
                # A stub or install that stopped answering must not hide why the drive failed.
                self.evidence["outcome_error"] = str(exc)

    def outcome(self) -> dict[str, Any]:
        """What the ticket's records show at the end, pass or fail.

        Request statuses and the factory's own comments as a reader sees them,
        so a failed run says where the flow stopped. Every value is fixture
        text or platform output about the fixture.
        """

        state = self.state()
        found: dict[str, Any] = {"jira_requests": state["requests"]}
        if not self.issue_id:
            return found
        observed = self.observe()
        found["requests"] = [
            {key: request.get(key) for key in ("id", "sequence", "status", "terminal_cause")}
            for request in observed["requests"]
        ]
        found["status_comments"] = observed["notices"]
        found["bot_comments"] = [
            {"id": comment["id"], "text": comment["body"][:2000]}
            for comment in ojt.comment_records(state, self.issue_id)
            if comment["user"]["login"] == ojt.BOT_ACCOUNT_ID
        ]
        found["labels"] = ojt.issue_of(state, self.issue_id)["labels"]
        return found

    def drive(self) -> None:
        bot = ojt.BOT_ACCOUNT_ID
        labeller = ojt.LABELLER["account_id"]
        agent = str(self.driver.evidence["agent_id"])
        for person in (ojt.LABELLER, ojt.OUTSIDER):
            self.jira.add_user(person)
        self.evidence.update(labeller=ojt.LABELLER, outsider=ojt.OUTSIDER)

        # One human label on one fresh Jira issue.
        baseline_pulls = [int(row["number"]) for row in self.github._rows("pull")]
        title, body = one_ticket_run.issue_title_and_body()
        created = self.jira.create_issue(title, body, labeller)
        self.issue_id, key = created["id"], created["key"]
        url = ojt.browse_url(self.stub.base_url, key)
        # The ticket itself is the one issue the factory must not add to.
        baseline_issues = [record["number"] for record in ojt.issue_records(self.state())]
        label_at = ots.instant(self.jira.add_label(self.issue_id, ots.LABEL, labeller)["at"])
        self.evidence.update(issue_id=self.issue_id, issue_key=key)

        # Session 1 asks.
        observed = self.asked(baseline_pulls)
        state = self.state()
        item = ots.assert_one_work_item(observed["work_items"], agent)
        asking = ots.assert_questions_state(
            ojt.issue_record(ojt.issue_of(state, self.issue_id)), observed["requests"]
        )
        investigating_at = ojt.line_instant(
            state, self.issue_id, str(asking["id"]), ots.INVESTIGATING_LINE, bot
        )
        ots.assert_first_response(label_at, investigating_at)
        ots.assert_one_question_comment(
            ojt.comment_records(state, self.issue_id), int(self.issue_id), bot
        )
        ots.assert_no_new_pulls(self.github._rows("pull"), baseline_pulls)
        self.evidence.update(work_item_id=str(item["id"]), asking_request_id=str(asking["id"]))

        # A plain comment from outside the start allowlist starts nothing.
        outsider = self.jira.add_comment(self.issue_id, OUTSIDER_TEXT, ojt.OUTSIDER["account_id"])
        self.wait_for_intake_passes(ots.instant(outsider["at"]))
        ots.assert_ignored_comments(len(observed["requests"]), len(self.observe()["requests"]))

        # The labeller's plain answer, the one authorized answer.
        answer = self.jira.add_comment(self.issue_id, one_ticket_run.ANSWER_TEXT, labeller)
        answer_at = ots.instant(answer["at"])
        observed = self.answered(str(asking["id"]), baseline_pulls)
        state = self.state()
        received_at = None
        if len(observed["requests"]) > 1:
            received_at = ojt.line_instant(
                state,
                self.issue_id,
                str(observed["requests"][1]["id"]),
                ots.ANSWER_RECEIVED_LINE,
                bot,
            )
        second = ots.assert_answer_admitted(
            observed["requests"], str(asking["id"]), answer_at, received_at
        )
        ots.assert_answer_receipts(observed["receipts"], str(answer["id"]), str(second["id"]))

        # Session 2 publishes, and nothing else starts.
        self.wait_for_intake_passes(one_ticket_run.now())
        observed = self.observe()
        state = self.state()
        pulls = [self.github._fresh_pull(row) for row in self.github._rows("pull")]
        pull = ojt.assert_one_jira_pull_request(pulls, key, url, baseline_pulls)
        ots.assert_no_factory_issues(baseline_issues, ojt.issue_records(state))
        final = ojt.final_comments(
            state, self.issue_id, str(second["id"]), observed["notices"], bot
        )
        ots.assert_finished(observed["work_items"], observed["requests"], final, pull)
        assert investigating_at is not None and received_at is not None
        self.evidence.update(
            second_request_id=str(second["id"]),
            answer_comment_id=answer["id"],
            pull_request=pull["number"],
            work_item_count=len(observed["work_items"]),
            execution_request_count=len(observed["requests"]),
            pull_request_count=len(ots.new_pulls(pulls, baseline_pulls)),
            factory_created_issue_count=len(state["issues"]) - len(baseline_issues),
            investigating_seconds=(investigating_at - label_at).total_seconds(),
            answer_received_seconds=(received_at - answer_at).total_seconds(),
            second_request_seconds=(ots.instant(second["created_at"]) - answer_at).total_seconds(),
        )
        self.evidence["result"] = "passed"
