"""Pure judge and evidence sanitizer of the ``live-github-chain`` scenario (#5808).

Decision 7 of #5808 lists eight beats (a to h) with bounds. Decision 8 sets
the spend and attempt caps, decision 9 the evidence sanitizer, decision 10 the
teardown verdict. This module judges all of it from already fetched records and
returns one verdict per beat with reasons. It does no network, subprocess,
cluster or file access: the driver (``live_github_chain.py``, a later part of
#5808) fetches the GitHub API JSON, the CLI JSON output, the install records
and the timings, passes them in as plain data and writes the verdicts out
through :func:`sanitize_evidence`. The split follows ``triage_run.py`` (driver)
and ``triage_scenario.py`` (judge).

The one public entry point is :func:`judge_chain`. It takes one record::

    {
      "attempt": 1,
      "identities": {"app_login", "writer_id", "reader_id", "repository_id",
                     "repository_url", "default_branch"},
      "agent": {"tenant_id", "memory_rule_proposals"},
      "preflight": {"open_items", "work_items"},
      "session_probe": {"writer": {...}, "reader": {...}},
      "beat_a" ... "beat_h": per beat records, see each ``_judge_beat_*``,
      "usage": {"samples": [{"label", "usage"}, ...]},
      "teardown": {"verified", "namespace_gone", ...},
    }

Timestamps are ISO 8601 strings taken from the API that reported them (GitHub
for comments, labels, reviews and merges; the install's records for requests),
so every bound compares two clocks that agree. A missing or malformed section
fails its own beat with the reason; it never raises.

Record shapes that come from features not merged when this module was written
are each isolated in one small named constant or parser below, marked
``ASSUMED``, so the driver part can adjust it in one place:
:data:`TOOL_NOTES_KEY` (#5794, #5796), :data:`RULES_KEY`,
:func:`refusal_code`, :func:`accepted_rule_of` (#5797), :func:`work_item_rules_of`
and :data:`LEARNED_RULES_LINE` (#5798), :func:`deploy_refusal_codes` (#5804)
and :data:`ACTIVE_REQUEST_STATES`.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# --- Bounds of decisions 7 and 8 (seconds unless named) ---------------------
STATUS_COMMENT_WITHIN = 180
QUESTIONS_WITHIN = 1800
QUIET_AFTER_QUESTIONS = 60
RESTART_WITHIN = 180
PULL_REQUEST_WITHIN = 3600
PROMOTION_WITHIN = 300
CLI_WITHIN = 60
UNBOUND_QUIET_WINDOW = 180
SPEND_CAP_USD = 50.0
MAX_ATTEMPTS = 2

# --- Texts the beats name ----------------------------------------------------
STATUS_COMMENT_PREFIX = "Investigating this ticket."
QUESTIONS_MARKER = "curie-factory-questions:"
QUESTIONS_LABEL = "curie-factory:questions"
ANSWERED_PREFIX = "Answered on the ticket by "
LEARNED_PREFIX = "Learned rules:"
REQUESTER_PREFIX = "Requester preferences:"
# Fixture texts, equal to the files under fixtures/live-github-chain/ (a test
# pins that), so the judge needs no file access.
ANSWER_TEXT = "Show 2 decimal places in the CLI output."
REVIEW_RULE_TEXT = (
    "Use `self.assertAlmostEqual(..., places=6)` for every floating point result; "
    "every float assertion in this repository's tests does that."
)
# ASSUMED (#5798 decision 9): the platform written pull request line for one
# rule numbered R1, with the digest captured.
LEARNED_RULES_LINE = re.compile(r"^Learned rules: in force; rules:R1; sha256:([0-9a-f]{64})$")
# ASSUMED (#5796 decisions 5 and 11): the tool trajectory is the run's recorded
# tool notes on the execution request, each a tool name or a mapping with one.
TOOL_NOTES_KEY = "tool_notes"
PROPOSE_RULE_TOOL = "propose_rule"
# ASSUMED (#5796 decision 6, #5797 decision 1): the listed rules key of the
# ``curie cluster memory --rules --json`` output, and the rule states.
RULES_KEY = "rules"
PROPOSED_STATE = "proposed"
ACCEPTED_STATE = "accepted"
REMOVED_STATE = "removed"
# ASSUMED (#5797 decision 3): the CLI refusal codes of a reader's accept.
READER_REFUSAL_CODES = frozenset(
    {"memory_rules.write_access_required", "memory_rules.write_access_unverifiable"}
)
# ASSUMED (#5804 decision 3): the stable bundle refusal code.
BUNDLE_RETIRED_CODE = "bundle.result_declaration_retired"
# ASSUMED: request states that mean a run is queued or in progress.
ACTIVE_REQUEST_STATES = frozenset({"queued", "running"})
WRITE_PERMISSIONS = frozenset({"write", "admin"})
TEARDOWN_KEYS = (
    "verified",
    "namespace_gone",
    "session_files_deleted",
    "code_host_links_removed",
    "dex_deleted",
    "issues_closed",
)

BEAT_ORDER = (
    "preflight",
    "session_probe",
    "beat_a",
    "beat_b",
    "beat_c",
    "beat_d",
    "beat_e",
    "beat_f",
    "beat_g",
    "beat_h",
    "spend",
    "attempts",
    "teardown",
)


# --- Verdicts ------------------------------------------------------------------
@dataclass(frozen=True)
class BeatVerdict:
    beat: str
    passed: bool
    reasons: tuple[str, ...]
    measures: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ChainVerdict:
    attempt: int | None
    beats: tuple[BeatVerdict, ...]

    @property
    def passed(self) -> bool:
        return all(beat.passed for beat in self.beats)

    def evidence(self) -> dict[str, Any]:
        """The verdicts as JSON data; pass it through :func:`sanitize_evidence`."""

        return {
            "attempt": self.attempt,
            "passed": self.passed,
            "beats": [
                {
                    "beat": beat.beat,
                    "passed": beat.passed,
                    "reasons": list(beat.reasons),
                    "measures": dict(beat.measures),
                }
                for beat in self.beats
            ],
        }


class _Beat:
    def __init__(self, name: str) -> None:
        self.name = name
        self.reasons: list[str] = []
        self.measures: dict[str, float] = {}

    def check(self, condition: bool, reason: str) -> bool:
        if not condition:
            self.reasons.append(reason)
        return bool(condition)

    def elapsed(self, key: str, start: Any, end: Any) -> float:
        seconds = (parse_time(end) - parse_time(start)).total_seconds()
        self.measures[key] = seconds
        return seconds

    def within(self, key: str, start: Any, end: Any, bound: float, reason: str) -> bool:
        seconds = self.elapsed(key, start, end)
        return self.check(0 <= seconds <= bound, f"{reason} ({seconds:.0f}s, bound {bound:.0f}s)")

    def verdict(self) -> BeatVerdict:
        return BeatVerdict(self.name, not self.reasons, tuple(self.reasons), dict(self.measures))


# --- Small named parsers: each record shape that may change lives in one ------
def parse_time(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp {value!r} has no zone")
    return parsed


def revision_request_id(repository_id: Any, pull_number: Any, review_id: Any) -> str:
    """#5794 decision 2.2: the request id of one submitted review."""

    name = f"curie-review:{repository_id}:{pull_number}:{review_id}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def review_url(repository_url: str, pull_number: Any, review_id: Any) -> str:
    """#5794 decision 2.5: the URL a review keyed request and its rule carry."""

    return f"{repository_url.rstrip('/')}/pull/{pull_number}#pullrequestreview-{review_id}"


def request_tool_names(request: Mapping[str, Any]) -> list[str]:
    """ASSUMED (#5796): the tool names of a request's recorded tool notes."""

    names = []
    for note in request.get(TOOL_NOTES_KEY) or []:
        if isinstance(note, Mapping):
            note = note.get("tool") or note.get("name") or ""
        names.append(str(note))
    return names


def called_propose_rule(request: Mapping[str, Any]) -> bool:
    return any(
        name == PROPOSE_RULE_TOOL or name.endswith(f"__{PROPOSE_RULE_TOOL}")
        for name in request_tool_names(request)
    )


def rules_of(output: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """ASSUMED (#5797): the rules of ``curie cluster memory <agent> --rules --json``."""

    return list(output[RULES_KEY])


def refusal_code(output: Mapping[str, Any]) -> str:
    """ASSUMED (#5797 decision 7): a refusal prints ``{"error","fix"}``; code is in ``error``."""

    found = re.search(r"memory_rules\.[a-z_]+", str(output.get("error", "")))
    return found.group(0) if found else ""


def accepted_rule_of(output: Mapping[str, Any]) -> Mapping[str, Any]:
    """ASSUMED (#5797): the action's JSON is the changed rule item (plus ``applies_from``)."""

    return output


def principal_of(actor: Mapping[str, Any] | None) -> str:
    """ASSUMED (#5797 decision 1): ``accepted_by`` is ``{principal_id, display_name}``."""

    return str((actor or {}).get("principal_id", ""))


def work_item_rules_of(work_item: Mapping[str, Any]) -> Mapping[str, Any]:
    """ASSUMED (#5798 decision 2): ``memory_rules`` of ``curie cluster work-items <id> --json``."""

    rules: Mapping[str, Any] = work_item["memory_rules"]
    return rules


def deploy_refusal_codes(output: Any) -> set[str]:
    """ASSUMED (#5804 decision 3): every ``code`` of a deploy refusal's JSON, at any depth."""

    codes: set[str] = set()
    if isinstance(output, Mapping):
        if isinstance(output.get("code"), str):
            codes.add(output["code"])
        for value in output.values():
            codes |= deploy_refusal_codes(value)
    elif isinstance(output, list):
        for value in output:
            codes |= deploy_refusal_codes(value)
    return codes


def added_text_of(added_lines: Sequence[str]) -> str:
    return "\n".join(added_lines)


def almost_equal_calls(added_text: str) -> list[str]:
    """Each ``assertAlmostEqual(`` call in the added text, through its closing paren."""

    calls = []
    for found in re.finditer(r"assertAlmostEqual\(", added_text):
        depth, end = 1, found.end()
        while end < len(added_text) and depth:
            depth += {"(": 1, ")": -1}.get(added_text[end], 0)
            end += 1
        calls.append(added_text[found.start() : end])
    return calls


def _lines_starting(body: str, prefix: str) -> list[str]:
    return [line for line in (body or "").splitlines() if line.lstrip("-*• ").startswith(prefix)]


def _closes(body: str, issue: int) -> bool:
    return re.search(rf"\bCloses #{issue}(?!\d)", body or "") is not None


def _by_app(item: Mapping[str, Any], ids: Mapping[str, Any]) -> bool:
    return bool(item["user"]["login"] == ids["app_login"])


# --- Beats ----------------------------------------------------------------------
def _judge_preflight(b: _Beat, rec: Mapping[str, Any]) -> None:
    pre = rec["preflight"]
    b.check(not pre["open_items"], "the fixture repository has an open issue or pull request")
    b.check(not pre["work_items"], "the agent already has a work item")


def _judge_session_probe(b: _Beat, rec: Mapping[str, Any]) -> None:
    probe, tenant = rec["session_probe"], rec["agent"]["tenant_id"]
    for role in ("writer", "reader"):
        side = probe[role]
        b.check(
            side["session_kind"] == "principal", f"the {role} session is not a principal session"
        )
        b.check(side["principal_status"] == 200, f"the {role} principal read is not 200")
        b.check(side["tenant_id"] == tenant, f"the {role} principal is in another tenant")
    b.check(
        probe["writer"]["principal_id"] != probe["reader"]["principal_id"],
        "the writer and reader sessions are one principal",
    )


def _judge_beat_a(b: _Beat, rec: Mapping[str, Any]) -> None:
    a, ids = rec["beat_a"], rec["identities"]
    b.check(rec["agent"]["memory_rule_proposals"] is True, "rule proposals were not turned on")
    label_at = a["labelled_at"]
    statuses = [
        c for c in a["comments"] if _by_app(c, ids) and c["body"].startswith(STATUS_COMMENT_PREFIX)
    ]
    if b.check(bool(statuses), "no investigating status comment"):
        b.within(
            "status_seconds",
            label_at,
            min((c["created_at"] for c in statuses), key=parse_time),
            STATUS_COMMENT_WITHIN,
            "the investigating comment came late",
        )
    questions = [c for c in a["comments"] if _by_app(c, ids) and QUESTIONS_MARKER in c["body"]]
    if not b.check(len(questions) == 1, f"expected one questions comment, found {len(questions)}"):
        return
    question = questions[0]
    b.within(
        "questions_seconds",
        label_at,
        question["created_at"],
        QUESTIONS_WITHIN,
        "the questions came late",
    )
    numbers = [int(n) for n in re.findall(r"(?m)^\s*(\d+)\.\s", question["body"])]
    b.check(
        bool(numbers) and numbers == list(range(1, len(numbers) + 1)),
        "the questions are not numbered from 1.",
    )
    b.check(QUESTIONS_LABEL in a["issue_labels"], "the issue lacks the questions label")
    quiet = a["quiet_snapshot"]
    b.check(
        b.elapsed("quiet_wait_seconds", question["created_at"], quiet["observed_at"])
        >= QUIET_AFTER_QUESTIONS,
        "the work item was read before the quiet period elapsed",
    )
    active = [r["id"] for r in quiet["requests"] if r["status"] in ACTIVE_REQUEST_STATES]
    b.check(not active, "a request is still queued or running after the questions")


def _judge_beat_b(b: _Beat, rec: Mapping[str, Any]) -> None:
    beat, ids = rec["beat_b"], rec["identities"]
    issue = rec["beat_a"]["issue_number"]
    answer = beat["answer"]
    b.check(answer["user"]["id"] == ids["writer_id"], "the answer was not posted by the writer")
    b.check(answer["body"].strip() == ANSWER_TEXT, "the answer text is not the fixture answer")
    b.check("@" not in answer["body"], "the answer mentions an account")
    restarts = [
        r
        for r in beat["requests"]
        if parse_time(r["created_at"]) >= parse_time(answer["created_at"])
    ]
    if b.check(bool(restarts), "no new request after the answer"):
        b.within(
            "restart_seconds",
            answer["created_at"],
            min((r["created_at"] for r in restarts), key=parse_time),
            RESTART_WITHIN,
            "the restart came late",
        )
    pulls = beat["open_pulls"]
    if b.check(len(pulls) == 1, f"expected one open pull request, found {len(pulls)}"):
        pull = pulls[0]
        b.within(
            "pull_request_seconds",
            answer["created_at"],
            pull["created_at"],
            PULL_REQUEST_WITHIN,
            "the pull request came late",
        )
        b.check(_closes(pull["body"], issue), f"the pull request lacks `Closes #{issue}`")
        b.check(
            bool(_lines_starting(pull["body"], ANSWERED_PREFIX)),
            "the pull request lacks an `Answered on the ticket by ` line",
        )
    others = [i["number"] for i in beat["issues"] if _by_app(i, ids)]
    b.check(not others, f"the App opened issues: {others}")


def _judge_beat_c(b: _Beat, rec: Mapping[str, Any]) -> None:
    c, ids = rec["beat_c"], rec["identities"]
    number = c["pull_number"]
    post, body = c["post"], c["post"]["body"]
    b.check(
        post["method"] == "POST"
        and re.fullmatch(rf"/?repos/[^/]+/[^/]+/pulls/{number}/reviews", post["path"]) is not None,
        "the feedback was not posted as a pull request review",
    )
    b.check(body["commit_id"] == c["pull_before"]["head"]["sha"], "the review names another head")
    b.check(body["event"] == "COMMENT", "the review event is not COMMENT")
    b.check(body["body"] == "", "the review has a body")
    posted = body["comments"]
    read = c["review_comments"]
    b.check(len(posted) == 1, f"the posted review carries {len(posted)} inline comments")
    if not b.check(len(read) == 1, f"the review reads back {len(read)} inline comments"):
        return
    comment = read[0]
    b.check(comment["body"].strip() == REVIEW_RULE_TEXT, "the inline comment is not the rule text")
    b.check("@" not in comment["body"], "the inline comment mentions an account")
    b.check(
        comment["line"] in c["diff_added_lines"].get(comment["path"], []),
        "the inline comment is not on a line the diff adds",
    )
    review = c["review"]
    b.check(review["state"] == "COMMENTED", "the review is not in state COMMENTED")
    b.check(bool(review["submitted_at"]), "the review is not submitted")
    b.check(review["user"]["id"] == ids["writer_id"], "the review was not written by the actor")
    submitted = review["submitted_at"]
    after = [r for r in c["requests"] if parse_time(r["created_at"]) >= parse_time(submitted)]
    if not b.check(len(after) == 1, f"expected one revision request, found {len(after)}"):
        return
    request = after[0]
    b.within(
        "revision_seconds",
        submitted,
        request["created_at"],
        PULL_REQUEST_WITHIN,
        "the revision came late",
    )
    expected = revision_request_id(ids["repository_id"], number, review["id"])
    b.check(request["id"] == expected, "the request id is not derived from the review id")
    objective = str(request["objective"])
    b.check(
        comment["body"] in objective and comment["path"] in objective,
        "the revision objective lacks the inline comment text or path",
    )
    before, now = c["pull_before"]["head"], c["pull_after"]["head"]
    b.check(
        now["ref"] == before["ref"]
        and set(c["branches"]) <= {ids["default_branch"], before["ref"]},
        "the revision is on a different branch",
    )
    b.check(now["sha"] != before["sha"], "no new commit on the pull request branch")
    b.within(
        "commit_seconds",
        submitted,
        now["committed_at"],
        PULL_REQUEST_WITHIN,
        "the commit came late",
    )
    b.check(called_propose_rule(request), "the revision trajectory has no propose_rule")


def _judge_beat_d(b: _Beat, rec: Mapping[str, Any]) -> None:
    d, ids = rec["beat_d"], rec["identities"]
    b.check(not rules_of(d["rules_before_merge"]), "a rule is listed before the merge")
    b.check(d["merged_by_id"] == ids["writer_id"], "the pull request was not merged by the writer")
    after = d["rules_after_merge"]
    b.within(
        "promotion_seconds",
        d["merged_at"],
        after["observed_at"],
        PROMOTION_WITHIN,
        "the rule was listed late",
    )
    rules = rules_of(after["output"])
    if not b.check(len(rules) == 1, f"expected one rule after the merge, found {len(rules)}"):
        return
    c = rec["beat_c"]
    expected = review_url(ids["repository_url"], c["pull_number"], c["review"]["id"])
    b.check(rules[0]["state"] == PROPOSED_STATE, "the rule is not proposed")
    b.check(rules[0]["source_url"] == expected, "the rule source is not the submitted review URL")


def _judge_beat_e(b: _Beat, rec: Mapping[str, Any]) -> None:
    e = rec["beat_e"]
    b.check(
        e["reader_permission"] not in WRITE_PERMISSIONS, "the reader can write to the repository"
    )
    reader, writer = e["reader_command"], e["writer_command"]
    b.check(reader["exit_code"] == 1, "the reader's accept did not exit 1")
    b.check(
        refusal_code(reader["output"]) in READER_REFUSAL_CODES,
        "the reader's accept is not refused for write access",
    )
    b.check(
        e["rule_after_reader"]["version"] == e["rule_before"]["version"],
        "the rule version changed under the reader's accept",
    )
    b.check(writer["exit_code"] == 0, "the writer's accept did not exit 0")
    rule = accepted_rule_of(writer["output"])
    b.check(rule.get("state") == ACCEPTED_STATE, "the writer's accept did not end accepted")
    b.check(
        principal_of(rule.get("accepted_by")) == rec["session_probe"]["writer"]["principal_id"],
        "the rule was not accepted by the writer principal",
    )
    for name, command in (("reader", reader), ("writer", writer)):
        seconds = float(command["elapsed_seconds"])
        b.measures[f"{name}_command_seconds"] = seconds
        b.check(seconds <= CLI_WITHIN, f"the {name} command took {seconds:.0f}s")


def _judge_beat_f(b: _Beat, rec: Mapping[str, Any]) -> None:
    f = rec["beat_f"]
    issue = f["issue_number"]
    b.check(f["instruction_set"] is True, "the conflicting instruction was not set")
    pulls = [p for p in f["open_pulls"] if _closes(p["body"], issue)]
    if not b.check(len(pulls) == 1, f"expected one pull request for ticket B, found {len(pulls)}"):
        return
    pull = pulls[0]
    b.within(
        "pull_request_seconds",
        f["labelled_at"],
        pull["created_at"],
        PULL_REQUEST_WITHIN,
        "the pull request came late",
    )
    lines = _lines_starting(pull["body"], LEARNED_PREFIX)
    if b.check(len(lines) == 1, f"expected one {LEARNED_PREFIX} line, found {len(lines)}"):
        found = LEARNED_RULES_LINE.match(lines[0])
        if b.check(
            found is not None, "the learned rules line is not `in force; rules:R1; sha256:`"
        ):
            assert found is not None
            b.check(
                found.group(1) == work_item_rules_of(f["work_item"])["sha256"],
                "the line digest differs from the work item's",
            )
        all_lines = pull["body"].splitlines()
        at = all_lines.index(lines[0])
        b.check(
            at + 1 < len(all_lines) and all_lines[at + 1].startswith(REQUESTER_PREFIX),
            "the line is not immediately before the Requester preferences line",
        )
    calls = almost_equal_calls(added_text_of(f["diff_added_lines"]))
    if b.check(bool(calls), "the diff adds no assertAlmostEqual( call"):
        loose = [call for call in calls if re.search(r"\bplaces\s*=\s*6\b", call) is None]
        b.check(not loose, f"{len(loose)} added assertAlmostEqual( calls lack places=6")


def _judge_beat_g(b: _Beat, rec: Mapping[str, Any]) -> None:
    g = rec["beat_g"]
    removal = g["remove_command"]
    b.check(removal["exit_code"] == 0, "the writer's removal did not exit 0")
    b.check(
        accepted_rule_of(removal["output"]).get("state") == REMOVED_STATE,
        "the removal did not end removed",
    )
    seconds = float(removal["elapsed_seconds"])
    b.measures["remove_command_seconds"] = seconds
    b.check(seconds <= CLI_WITHIN, f"the removal took {seconds:.0f}s")
    pulls = [p for p in g["open_pulls"] if _closes(p["body"], g["issue_number"])]
    if b.check(len(pulls) == 1, f"expected one pull request for ticket C, found {len(pulls)}"):
        b.within(
            "pull_request_seconds",
            g["labelled_at"],
            pulls[0]["created_at"],
            PULL_REQUEST_WITHIN,
            "the pull request came late",
        )
        b.check(
            not _lines_starting(pulls[0]["body"], LEARNED_PREFIX),
            "the pull request still carries a learned rules line",
        )
    b.check(
        work_item_rules_of(g["work_item"])["state"] == "absent",
        "ticket C's work item does not read memory_rules.state absent",
    )


def _judge_beat_h(b: _Beat, rec: Mapping[str, Any]) -> None:
    h, ids = rec["beat_h"], rec["identities"]
    b.check(
        b.elapsed("quiet_wait_seconds", h["labelled_at"], h["observed_at"]) >= UNBOUND_QUIET_WINDOW,
        "the unbound issue was read before the quiet window elapsed",
    )
    b.check(not h["work_items"], "the unbound label produced a work item")
    b.check(
        not [c for c in h["comments"] if _by_app(c, ids)], "the App commented on the unbound issue"
    )
    b.check(
        not [e for e in h["label_events"] if e["actor"]["login"] == ids["app_login"]],
        "the App labelled the unbound issue",
    )
    deploy = h["retired_bundle"]
    b.check(deploy["exit_code"] != 0, "the retired declaration bundle deployed")
    b.check(
        BUNDLE_RETIRED_CODE in deploy_refusal_codes(deploy["output"]),
        f"the deploy refusal is not {BUNDLE_RETIRED_CODE}",
    )
    b.check(
        deploy["version_before"] == deploy["version_after"],
        "the in force version changed after the refused deploy",
    )


def _judge_spend(b: _Beat, rec: Mapping[str, Any]) -> None:
    usages = [s["usage"] for s in rec["usage"]["samples"]]
    if not b.check(
        len(usages) >= 2 and all(isinstance(u, (int, float)) for u in usages),
        "the model usage was not measured before and after",
    ):
        return
    delta = float(usages[-1]) - float(usages[0])
    b.measures["delta_usd"] = delta
    b.check(delta < SPEND_CAP_USD, f"the usage delta {delta:.2f} USD reached {SPEND_CAP_USD:.0f}")


def _judge_attempts(b: _Beat, rec: Mapping[str, Any]) -> None:
    attempt = rec["attempt"]
    b.measures["attempt"] = float(attempt)
    b.check(1 <= attempt <= MAX_ATTEMPTS, f"attempt {attempt} is outside 1..{MAX_ATTEMPTS}")


def _judge_teardown(b: _Beat, rec: Mapping[str, Any]) -> None:
    teardown = rec["teardown"]
    missing = [key for key in TEARDOWN_KEYS if teardown.get(key) is not True]
    b.check(not missing, f"teardown is not verified: {missing}")


_JUDGES: dict[str, Callable[[_Beat, Mapping[str, Any]], None]] = {
    "preflight": _judge_preflight,
    "session_probe": _judge_session_probe,
    "beat_a": _judge_beat_a,
    "beat_b": _judge_beat_b,
    "beat_c": _judge_beat_c,
    "beat_d": _judge_beat_d,
    "beat_e": _judge_beat_e,
    "beat_f": _judge_beat_f,
    "beat_g": _judge_beat_g,
    "beat_h": _judge_beat_h,
    "spend": _judge_spend,
    "attempts": _judge_attempts,
    "teardown": _judge_teardown,
}
_SECTIONS = {"spend": "usage", "attempts": "attempt"}


def judge_beat(name: str, record: Mapping[str, Any]) -> BeatVerdict:
    """Judge one beat; the driver calls this to stop early (preflight, probe, spend)."""

    beat = _Beat(name)
    section = _SECTIONS.get(name, name)
    if section not in record:
        beat.reasons.append(f"the record has no {section!r} section")
        return beat.verdict()
    try:
        _JUDGES[name](beat, record)
    except (KeyError, TypeError, ValueError, AttributeError, IndexError) as error:
        beat.reasons.append(f"the record is malformed: {type(error).__name__} {error}")
    return beat.verdict()


def judge_chain(record: Mapping[str, Any]) -> ChainVerdict:
    """Judge every beat and bound of decisions 7, 8 and 10, in order."""

    beats = tuple(judge_beat(name, record) for name in BEAT_ORDER)
    attempt = record.get("attempt")
    return ChainVerdict(attempt if isinstance(attempt, int) else None, beats)


# --- Evidence sanitizer (decision 9) ---------------------------------------------
ROLE_PLACEHOLDERS: Mapping[str, str] = {
    "fixture-repo": "<fixture-repo>",
    "writer": "<writer>",
    "reader": "<reader>",
    "app": "<app>",
    "namespace": "<namespace>",
    "context": "<context>",
    "registry": "<registry>",
    "code-host": "<code-host>",
}


class EvidenceLeak(ValueError):
    """Evidence still holds a raw identifier. Names roles and paths, never the value."""

    def __init__(self, leaks: Sequence[tuple[str, str]]) -> None:
        self.leaks = tuple(leaks)
        detail = ", ".join(f"{role} at {path}" for role, path in self.leaks)
        super().__init__(f"evidence still contains raw identifiers: {detail}")


Identifiers = Mapping[str, Iterable["str | int"]]


def _tokens(identifiers: Identifiers) -> list[tuple[str, str]]:
    """``(raw, role)`` pairs, longest raw first, so a URL's owner/name beats its parts."""

    pairs: dict[str, str] = {}
    for role, raws in identifiers.items():
        if role not in ROLE_PLACEHOLDERS:
            raise ValueError(f"unknown identifier role {role!r}")
        for raw in raws:
            text = str(raw)
            if not text.strip():
                raise ValueError(f"empty identifier for role {role!r}")
            pairs.setdefault(text.lower(), role)
    return sorted(pairs.items(), key=lambda pair: (-len(pair[0]), pair[0]))


def _pattern(token: str) -> str:
    # A numeric id matches as a whole number, never inside a longer one.
    return rf"(?<!\d){re.escape(token)}(?!\d)" if token.isdigit() else re.escape(token)


def _compiled(identifiers: Identifiers) -> tuple[re.Pattern[str] | None, dict[str, str]]:
    tokens = _tokens(identifiers)
    if not tokens:
        return None, {}
    groups = {f"t{i}": role for i, (_, role) in enumerate(tokens)}
    body = "|".join(f"(?P<t{i}>{_pattern(raw)})" for i, (raw, _) in enumerate(tokens))
    return re.compile(body, re.IGNORECASE), groups


def _replace(text: str, pattern: re.Pattern[str] | None, groups: Mapping[str, str]) -> str:
    if pattern is None:
        return text
    return pattern.sub(lambda m: ROLE_PLACEHOLDERS[groups[str(m.lastgroup)]], text)


def _walk(value: Any, leaf: Callable[[Any], Any], key: Callable[[str], str]) -> Any:
    if isinstance(value, Mapping):
        return {key(str(k)): _walk(v, leaf, key) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_walk(v, leaf, key) for v in value]
    return leaf(value)


def sanitize_evidence(evidence: Any, identifiers: Identifiers) -> Any:
    """Replace every raw identifier with its role placeholder, then verify none remains.

    Strings, mapping keys and whole numbers are matched (case-insensitively for
    text, as a whole number for ids) wherever they sit: inside URLs, in nested
    JSON, in lists. Raises :class:`EvidenceLeak` if the result still holds one.
    """

    pattern, groups = _compiled(identifiers)

    def leaf(value: Any) -> Any:
        if value is None or isinstance(value, (bool, float)):
            return value
        text = _replace(str(value), pattern, groups)
        # A number that names no identifier stays a number.
        return value if isinstance(value, int) and text == str(value) else text

    cleaned = _walk(evidence, leaf, lambda k: _replace(k, pattern, groups))
    refuse_leaks(cleaned, identifiers)
    return cleaned


def sanitize_log(text: str, identifiers: Identifiers) -> str:
    """The same sanitizer for one log line or block the driver prints."""

    cleaned = sanitize_evidence(text, identifiers)
    assert isinstance(cleaned, str)
    return cleaned


def refuse_leaks(value: Any, identifiers: Identifiers) -> None:
    """Raise :class:`EvidenceLeak` when ``value`` (text or JSON data) holds a raw identifier.

    Placeholders are masked first, so a role word inside ``<writer>`` is not a leak.
    Call it on the serialized text just before writing the evidence file.
    """

    pattern, groups = _compiled(identifiers)
    if pattern is None:
        return
    leaks: list[tuple[str, str]] = []

    def masked(text: str) -> str:
        for placeholder in ROLE_PLACEHOLDERS.values():
            text = text.replace(placeholder, "")
        return text

    def scan(text: str, path: str) -> None:
        for found in pattern.finditer(masked(text)):
            leaks.append((groups[str(found.lastgroup)], path))

    def visit(item: Any, path: str) -> None:
        if isinstance(item, Mapping):
            for k, v in item.items():
                scan(str(k), f"{path}/<key>")
                # A key that leaks must not reach the error text through the path.
                visit(v, f"{path}/{'*' if pattern.search(masked(str(k))) else k}")
        elif isinstance(item, (list, tuple)):
            for index, v in enumerate(item):
                visit(v, f"{path}[{index}]")
        elif item is not None and not isinstance(item, (bool, float)):
            scan(str(item), path)

    visit(value, "$")
    if leaks:
        raise EvidenceLeak(sorted(set(leaks)))


def serialize_evidence(evidence: Any, identifiers: Identifiers) -> str:
    """Sanitize, serialize and re-check the text: the only form the driver may write."""

    text = json.dumps(sanitize_evidence(evidence, identifiers), indent=2, sort_keys=True)
    refuse_leaks(text, identifiers)
    return text + "\n"
