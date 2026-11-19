"""Offline proof of the live-github-chain judge and evidence sanitizer (#5808, part 1).

Every case goes through ``judge_chain``, the entry point the live driver calls,
on a fully passing record set (``fixtures/live-github-chain/record-pass.json``,
all identifiers obviously fake) with one broken shape applied. Record shapes of
features that were not merged when this was written (#5794, #5796, #5797,
#5798, #5800, #5804) are marked ASSUMED in the judge module.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import live_github_chain_judge as judge
import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "live-github-chain"
PASSING: dict[str, Any] = json.loads((FIXTURES / "record-pass.json").read_text())
BODY_F = PASSING["beat_f"]["open_pulls"][0]["body"]
LINE = next(x for x in BODY_F.splitlines() if x.startswith("Learned rules:"))
REQUESTER_LINE = next(x for x in BODY_F.splitlines() if x.startswith("Requester preferences:"))


def at(minutes: int, seconds: int = 0) -> str:
    return f"2030-01-01T{minutes // 60:02d}:{minutes % 60:02d}:{seconds:02d}Z"


def apply(record: dict[str, Any], path: str, value: Any) -> None:
    *parents, last = path.split("/")
    node: Any = record
    for token in parents:
        node = node[int(token)] if isinstance(node, list) else node[token]
    if isinstance(node, list):
        index = int(last)
        node[index] = value(node[index]) if callable(value) else value
    else:
        node[last] = value(node[last]) if callable(value) else value


def broken(*edits: tuple[str, Any]) -> dict[str, Any]:
    record = copy.deepcopy(PASSING)
    for path, value in edits:
        apply(record, path, value)
    return record


def failed_beats(record: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    return {b.beat: b.reasons for b in judge.judge_chain(record).beats if not b.passed}


def with_body(body: str) -> tuple[str, Any]:
    return ("beat_f/open_pulls/0/body", body)


def drop_line(body: str, prefix: str) -> str:
    return "\n".join(x for x in body.splitlines() if not x.startswith(prefix)) + "\n"


OUTSIDE_DECISIONS = (
    "Closes #11\n\n## Decisions\n\nNone.\n\n## Notes\n\n"
    "Answered on the ticket by fixture-writer: print 2 decimal places.\n"
)


def second_revision(requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    extra = {**requests[-1], "id": "request-extra", "created_at": at(31)}
    return [*requests, extra]


def two_rules(output: dict[str, Any]) -> dict[str, Any]:
    return {**output, "rules": [output["rules"][0], {**output["rules"][0], "id": "rule-id-2"}]}


# (case id, edits, failing beat, fragment of its reason)
CASES: list[tuple[str, list[tuple[str, Any]], str, str]] = [
    ("open-issue", [("preflight/open_items", [{"number": 1}])], "preflight", "open issue"),
    (
        "open-pull-request",
        [("preflight/open_items", [{"number": 2, "pull_request": {}}])],
        "preflight",
        "open issue or pull",
    ),
    ("existing-work-item", [("preflight/work_items", [{"id": "w1"}])], "preflight", "work item"),
    (
        "operator-session",
        [("session_probe/reader/session_kind", "operator")],
        "session_probe",
        "not a principal session",
    ),
    (
        "one-principal-for-both",
        [("session_probe/reader/principal_id", "principal-writer")],
        "session_probe",
        "one principal",
    ),
    (
        "other-tenant",
        [("session_probe/reader/tenant_id", "tenant-other")],
        "session_probe",
        "another tenant",
    ),
    (
        "principal-read-not-200",
        [("session_probe/writer/principal_status", 401)],
        "session_probe",
        "not 200",
    ),
    (
        "investigating-late",
        [("beat_a/comments/0/created_at", at(4))],
        "beat_a",
        "investigating comment came late",
    ),
    ("no-investigating", [("beat_a/comments/0/body", "Working.")], "beat_a", "no investigating"),
    (
        "questions-late",
        [("beat_a/comments/1/created_at", at(31))],
        "beat_a",
        "questions came late",
    ),
    (
        "questions-not-numbered-from-one",
        [("beat_a/comments/1/body", "curie-factory-questions:11\n\n2. How many places?\n")],
        "beat_a",
        "numbered from 1.",
    ),
    (
        "no-question-comment",
        [("beat_a/comments/1/body", "A plain remark.")],
        "beat_a",
        "expected one questions comment",
    ),
    (
        "no-questions-label",
        [("beat_a/issue_labels", ["factory"])],
        "beat_a",
        "questions label",
    ),
    (
        "request-queued-after-questions",
        [("beat_a/quiet_snapshot/requests/0/status", "queued")],
        "beat_a",
        "queued or running",
    ),
    (
        "request-running-after-questions",
        [("beat_a/quiet_snapshot/requests/0/status", "running")],
        "beat_a",
        "queued or running",
    ),
    (
        "quiet-snapshot-too-early",
        [("beat_a/quiet_snapshot/observed_at", at(5, 30))],
        "beat_a",
        "before the quiet period",
    ),
    (
        "proposals-off",
        [("agent/memory_rule_proposals", False)],
        "beat_a",
        "rule proposals were not turned on",
    ),
    (
        "restart-late",
        [("beat_b/requests/1/created_at", at(13, 30))],
        "beat_b",
        "restart came late",
    ),
    (
        "no-restart",
        [("beat_b/requests", lambda rs: rs[:1])],
        "beat_b",
        "no new request",
    ),
    (
        "pull-request-late",
        [("beat_b/open_pulls/0/created_at", at(72))],
        "beat_b",
        "pull request came late",
    ),
    (
        "no-closes",
        [("beat_b/open_pulls/0/body", lambda s: s.replace("Closes #11", "Fixes #11"))],
        "beat_b",
        "Closes #11",
    ),
    (
        "closes-another-issue-with-same-prefix",
        [("beat_b/open_pulls/0/body", lambda s: s.replace("Closes #11", "Closes #110"))],
        "beat_b",
        "Closes #11",
    ),
    (
        "no-answered-line",
        [("beat_b/open_pulls/0/body", lambda s: drop_line(s, "Answered on the ticket by "))],
        "beat_b",
        "Decisions section lacks",
    ),
    (
        "answered-line-outside-the-decisions-section",
        [("beat_b/open_pulls/0/body", OUTSIDE_DECISIONS)],
        "beat_b",
        "Decisions section lacks",
    ),
    (
        "two-pull-requests",
        [("beat_b/open_pulls", lambda ps: [*ps, {**ps[0], "number": 99}])],
        "beat_b",
        "expected one open pull request",
    ),
    (
        "second-factory-issue",
        [
            (
                "beat_b/issues",
                lambda xs: [*xs, {"number": 12, "user": {"login": "fixture-app[bot]"}}],
            )
        ],
        "beat_b",
        "App opened issues",
    ),
    (
        "answer-mentions-the-factory",
        [("beat_b/answer/body", "@fixture-app " + judge.ANSWER_TEXT)],
        "beat_b",
        "mentions",
    ),
    (
        "feedback-as-conversation-comment",
        [("beat_c/post/path", "/repos/fixture-owner/fixture-repo/issues/12/comments")],
        "beat_c",
        "not posted as a pull request review",
    ),
    (
        "review-names-another-head",
        [("beat_c/post/body/commit_id", "c" * 40)],
        "beat_c",
        "another head",
    ),
    (
        "review-has-a-body",
        [("beat_c/post/body/body", "Please see inline.")],
        "beat_c",
        "has a body",
    ),
    (
        "review-event-not-comment",
        [("beat_c/post/body/event", "REQUEST_CHANGES")],
        "beat_c",
        "not COMMENT",
    ),
    (
        "posted-review-with-zero-inline-comments",
        [("beat_c/post/body/comments", [])],
        "beat_c",
        "posted review carries 0",
    ),
    (
        "posted-review-with-two-inline-comments",
        [("beat_c/post/body/comments", lambda cs: [*cs, *cs])],
        "beat_c",
        "posted review carries 2",
    ),
    (
        "read-back-review-with-zero-inline-comments",
        [("beat_c/review_comments", [])],
        "beat_c",
        "reads back 0",
    ),
    (
        "read-back-review-with-two-inline-comments",
        [("beat_c/review_comments", lambda cs: [*cs, *cs])],
        "beat_c",
        "reads back 2",
    ),
    (
        "posted-comment-differs-from-the-read-back-comment",
        [("beat_c/post/body/comments/0/path", "unitconv/other.py")],
        "beat_c",
        "differ",
    ),
    (
        "posted-comment-is-not-the-rule-text",
        [("beat_c/post/body/comments/0/body", "Rename this.")],
        "beat_c",
        "posted inline comment is not the rule text",
    ),
    (
        "inline-comment-not-on-an-added-line",
        [("beat_c/diff_added_lines", {"unitconv/tests/test_convert.py": [1, 2]})],
        "beat_c",
        "line the diff adds",
    ),
    (
        "inline-comment-mentions-the-factory",
        [("beat_c/review_comments/0/body", "@fixture-app " + judge.REVIEW_RULE_TEXT)],
        "beat_c",
        "mentions",
    ),
    ("review-pending", [("beat_c/review/state", "PENDING")], "beat_c", "not in state COMMENTED"),
    ("review-not-submitted", [("beat_c/review/submitted_at", None)], "beat_c", "not submitted"),
    (
        "review-by-someone-else",
        [("beat_c/review/user/id", 1)],
        "beat_c",
        "not written by the actor",
    ),
    (
        "zero-revision-requests",
        [("beat_c/requests", lambda rs: rs[:-1])],
        "beat_c",
        "found 0",
    ),
    (
        "two-revision-requests",
        [("beat_c/requests", second_revision)],
        "beat_c",
        "found 2",
    ),
    (
        "request-id-not-derived-from-the-review",
        [("beat_c/requests/2/id", "request-3")],
        "beat_c",
        "not derived from the review id",
    ),
    (
        "objective-lacks-the-comment",
        [("beat_c/requests/2/objective", "Fix it.")],
        "beat_c",
        "objective lacks",
    ),
    (
        "revision-on-another-branch",
        [("beat_c/pull_after/head/ref", "curie/other")],
        "beat_c",
        "different branch",
    ),
    (
        "revision-opened-a-stray-branch",
        [("beat_c/branches", lambda bs: [*bs, "curie/stray"])],
        "beat_c",
        "different branch",
    ),
    (
        "no-new-commit",
        [("beat_c/pull_after/head/sha", "a" * 40)],
        "beat_c",
        "no new commit",
    ),
    (
        "no-propose-rule",
        [("beat_c/requests/2/tool_notes", ["Read", "Edit"])],
        "beat_c",
        "no propose_rule",
    ),
    (
        "rule-listed-before-merge",
        [("beat_d/rules_before_merge/rules", [{"id": "rule-id-0"}])],
        "beat_d",
        "before the merge",
    ),
    (
        "zero-rules-after-merge",
        [("beat_d/rules_after_merge/output/rules", [])],
        "beat_d",
        "found 0",
    ),
    (
        "two-rules-after-merge",
        [("beat_d/rules_after_merge/output", two_rules)],
        "beat_d",
        "found 2",
    ),
    (
        "rule-listed-late",
        [("beat_d/rules_after_merge/observed_at", at(52, 10))],
        "beat_d",
        "listed late",
    ),
    (
        "rule-source-is-the-inline-comment",
        [
            (
                "beat_d/rules_after_merge/output/rules/0/source_url",
                lambda u: u.split("#")[0] + "#discussion_r5",
            )
        ],
        "beat_d",
        "not the submitted review URL",
    ),
    (
        "rule-already-accepted",
        [("beat_d/rules_after_merge/output/rules/0/state", "accepted")],
        "beat_d",
        "not proposed",
    ),
    (
        "merged-by-another-account",
        [("beat_d/merged_by_id", 1)],
        "beat_d",
        "not merged by the writer",
    ),
    (
        "reader-accept-succeeded",
        [
            ("beat_e/reader_command/exit_code", 0),
            ("beat_e/reader_command/output", {"state": "accepted", "version": 2}),
            ("beat_e/rule_after_reader/version", 2),
        ],
        "beat_e",
        "did not exit 1",
    ),
    (
        "reader-refused-for-another-reason",
        [("beat_e/reader_command/output/error", "memory_rules.stale_version")],
        "beat_e",
        "not refused for write access",
    ),
    (
        "reader-accept-changed-the-version",
        [("beat_e/rule_after_reader/version", 2)],
        "beat_e",
        "version changed",
    ),
    (
        "reader-can-write",
        [("beat_e/reader_permission", "write")],
        "beat_e",
        "reader can write",
    ),
    (
        "reader-is-admin",
        [("beat_e/reader_permission", "admin")],
        "beat_e",
        "reader can write",
    ),
    (
        "writer-accept-failed",
        [
            ("beat_e/writer_command/exit_code", 1),
            ("beat_e/writer_command/output", {"error": "memory_rules.stale_version"}),
        ],
        "beat_e",
        "did not exit 0",
    ),
    (
        "accepted-by-another-principal",
        [("beat_e/writer_command/output/accepted_by/principal_id", "principal-reader")],
        "beat_e",
        "not accepted by the writer principal",
    ),
    (
        "reader-command-slow",
        [("beat_e/reader_command/elapsed_seconds", 61)],
        "beat_e",
        "reader command took",
    ),
    (
        "no-learned-rules-line",
        [with_body(drop_line(BODY_F, "Learned rules:"))],
        "beat_f",
        "found 0",
    ),
    (
        "two-learned-rules-lines",
        [with_body(BODY_F.replace(REQUESTER_LINE, LINE + "\n" + REQUESTER_LINE))],
        "beat_f",
        "found 2",
    ),
    (
        "digest-differs-from-the-work-item",
        [("beat_f/work_item/memory_rules/sha256", "e" * 64)],
        "beat_f",
        "digest differs",
    ),
    (
        "line-after-requester-preferences",
        [with_body(BODY_F.replace(LINE + "\n" + REQUESTER_LINE, REQUESTER_LINE + "\n" + LINE))],
        "beat_f",
        "immediately before",
    ),
    (
        "line-not-adjacent-to-requester-preferences",
        [with_body(BODY_F.replace(LINE + "\n", LINE + "\nAn extra line.\n"))],
        "beat_f",
        "immediately before",
    ),
    (
        "learned-line-says-followed",
        [with_body(BODY_F.replace("in force", "followed"))],
        "beat_f",
        "not `in force",
    ),
    (
        "no-assert-almost-equal-added",
        [
            (
                "beat_f/diff_added_lines",
                ["    def test_delisle(self):", "        self.assertEqual(1, 1)"],
            )
        ],
        "beat_f",
        "adds no assertAlmostEqual",
    ),
    (
        "assert-almost-equal-without-places",
        [
            (
                "beat_f/diff_added_lines",
                ["        self.assertAlmostEqual(convert(1, 'c', 'de'), 150.0)"],
            )
        ],
        "beat_f",
        "lack places=6",
    ),
    (
        "assert-almost-equal-with-places-5",
        [("beat_f/diff_added_lines", ["        self.assertAlmostEqual(f(1), 2.0, places=5)"])],
        "beat_f",
        "lack places=6",
    ),
    (
        "assert-almost-equal-with-places-60",
        [("beat_f/diff_added_lines", ["        self.assertAlmostEqual(f(1), 2.0, places=60)"])],
        "beat_f",
        "lack places=6",
    ),
    (
        "one-call-without-places-among-good-ones",
        [
            (
                "beat_f/diff_added_lines",
                lambda ls: [*ls, "        self.assertAlmostEqual(g(2), 3.0)"],
            )
        ],
        "beat_f",
        "lack places=6",
    ),
    (
        "conflicting-instruction-not-set",
        [("beat_f/instruction_set", False)],
        "beat_f",
        "instruction was not set",
    ),
    (
        "ticket-b-pull-request-late",
        [("beat_f/open_pulls/0/created_at", at(130))],
        "beat_f",
        "pull request came late",
    ),
    (
        "ticket-c-pull-request-carries-the-line",
        [
            (
                "beat_g/open_pulls/1/body",
                lambda s: s.replace("Requester preferences:", LINE + "\nRequester preferences:"),
            )
        ],
        "beat_g",
        "still carries a learned rules line",
    ),
    (
        "ticket-c-work-item-still-has-rules",
        [("beat_g/work_item/memory_rules/state", "selected")],
        "beat_g",
        "state absent",
    ),
    (
        "removal-failed",
        [("beat_g/remove_command/exit_code", 1)],
        "beat_g",
        "did not exit 0",
    ),
    (
        "removal-ended-accepted",
        [("beat_g/remove_command/output/state", "accepted")],
        "beat_g",
        "did not end removed",
    ),
    (
        "no-pull-request-for-ticket-c",
        [("beat_g/open_pulls", lambda ps: ps[:1])],
        "beat_g",
        "found 0",
    ),
    (
        "unbound-label-made-a-work-item",
        [("beat_h/work_items", [{"id": "w9"}])],
        "beat_h",
        "produced a work item",
    ),
    (
        "app-commented-on-the-unbound-issue",
        [
            (
                "beat_h/comments",
                [
                    {
                        "id": 9,
                        "body": "hi",
                        "created_at": at(121),
                        "user": {"login": "fixture-app[bot]"},
                    }
                ],
            )
        ],
        "beat_h",
        "commented",
    ),
    (
        "app-labelled-the-unbound-issue",
        [
            (
                "beat_h/label_events",
                lambda events: [
                    *events,
                    {
                        "event": "labeled",
                        "label": {"name": "curie-factory:queued"},
                        "actor": {"login": "fixture-app[bot]", "id": 9000003},
                        "created_at": at(121),
                    },
                ],
            )
        ],
        "beat_h",
        "labelled",
    ),
    (
        "unbound-quiet-window-too-short",
        [("beat_h/observed_at", at(121, 30))],
        "beat_h",
        "quiet window",
    ),
    (
        "retired-bundle-deployed",
        [("beat_h/retired_bundle/exit_code", 0)],
        "beat_h",
        "deployed",
    ),
    (
        "retired-bundle-refused-for-another-code",
        [("beat_h/retired_bundle/output", {"issues": [{"code": "bundle.invalid"}]})],
        "beat_h",
        "bundle.result_declaration_retired",
    ),
    (
        "in-force-version-changed",
        [("beat_h/retired_bundle/version_after", "v4")],
        "beat_h",
        "in force version changed",
    ),
    (
        "usage-delta-at-the-cap",
        [("usage/samples/8/usage", 60.0)],
        "spend",
        "reached 50",
    ),
    (
        "usage-not-measured",
        [("usage/samples/8/usage", None)],
        "spend",
        "not measured at",
    ),
    ("third-attempt", [("attempt", 3)], "attempts", "outside 1..2"),
    ("attempt-zero", [("attempt", 0)], "attempts", "outside 1..2"),
    (
        "request-still-active-at-the-60-second-deadline",
        [
            ("beat_a/quiet_snapshot/requests/0/finished_at", at(7)),
            ("beat_a/quiet_snapshot/observed_at", at(7, 10)),
        ],
        "beat_a",
        "60 seconds after the questions",
    ),
    (
        "snapshot-holds-another-work-items-request",
        [("beat_a/quiet_snapshot/requests/0/work_item_id", "work-item-z")],
        "beat_a",
        "another work item",
    ),
    (
        "restart-belongs-to-another-work-item",
        [("beat_b/requests/1/work_item_id", "work-item-z")],
        "beat_b",
        "no new request",
    ),
    (
        "answer-is-on-another-issue",
        [("beat_b/answer/issue_number", 5)],
        "beat_b",
        "not on ticket A",
    ),
    (
        "ticket-a-history-missing-at-the-restart",
        [("beat_b/requests", lambda rs: rs[1:])],
        "beat_b",
        "history of ticket A is missing",
    ),
    (
        "ticket-a-history-missing-at-the-review",
        [("beat_c/requests", lambda rs: rs[1:])],
        "beat_c",
        "history of ticket A is missing",
    ),
    (
        "review-on-another-pull-request",
        [("beat_b/open_pulls/0/number", 99)],
        "beat_c",
        "another pull request than ticket A's",
    ),
    (
        "review-head-is-not-ticket-as-pull-head",
        [("beat_b/open_pulls/0/head/sha", "d" * 40)],
        "beat_c",
        "not ticket A's pull head",
    ),
    (
        "review-posted-on-another-repository",
        [("beat_c/post/path", "/repos/other-owner/other-repo/pulls/12/reviews")],
        "beat_c",
        "fixture repository",
    ),
    (
        "review-read-back-from-another-pull-request",
        [("beat_c/review/pull_request_url", lambda u: u.replace("pulls/12", "pulls/99"))],
        "beat_c",
        "another pull request or review",
    ),
    (
        "inline-comment-of-another-review",
        [("beat_c/review_comments/0/pull_request_review_id", 1)],
        "beat_c",
        "another pull request or review",
    ),
    (
        "rule-label-is-not-r1",
        [("beat_d/rules_after_merge/output/rules/0/label", "R2")],
        "beat_d",
        "not R1",
    ),
    (
        "acted-on-rule-is-not-the-promoted-rule",
        [
            ("beat_e/rule_before/id", "rule-id-9"),
            ("beat_e/reader_command/rule_id", "rule-id-9"),
            ("beat_e/writer_command/rule_id", "rule-id-9"),
            ("beat_e/writer_command/output/id", "rule-id-9"),
            ("beat_g/remove_command/rule_id", "rule-id-9"),
            ("beat_g/remove_command/output/id", "rule-id-9"),
        ],
        "beat_e",
        "not the promoted rule",
    ),
    (
        "reader-accept-names-another-rule",
        [("beat_e/reader_command/rule_id", "rule-id-9")],
        "beat_e",
        "reader's accept names another rule",
    ),
    (
        "writer-accept-names-another-rule",
        [("beat_e/writer_command/rule_id", "rule-id-9")],
        "beat_e",
        "writer's accept names another rule",
    ),
    (
        "writer-accept-changed-another-rule",
        [("beat_e/writer_command/output/id", "rule-id-9")],
        "beat_e",
        "changed another rule",
    ),
    (
        "removal-names-another-rule",
        [("beat_g/remove_command/rule_id", "rule-id-9")],
        "beat_g",
        "another rule than the accepted one",
    ),
    (
        "removal-changed-another-rule",
        [("beat_g/remove_command/output/id", "rule-id-9")],
        "beat_g",
        "removal changed another rule",
    ),
    (
        "removal-by-the-reader",
        [("beat_g/remove_command/output/removed_by/principal_id", "principal-reader")],
        "beat_g",
        "not removed by the writer principal",
    ),
    (
        "removal-after-ticket-c-was-labelled",
        [("beat_g/remove_command/output/removed_at", at(95))],
        "beat_g",
        "after ticket C was labelled",
    ),
    (
        "commented-out-assert-almost-equal",
        [("beat_f/diff_added_lines", ["        # self.assertAlmostEqual(1, 2, places=6)"])],
        "beat_f",
        "adds no assertAlmostEqual",
    ),
    (
        "places-six-only-in-a-message",
        [("beat_f/diff_added_lines", ['        self.assertAlmostEqual(1, 2, msg="places=6")'])],
        "beat_f",
        "lack places=6",
    ),
    (
        "places-six-only-in-a-trailing-comment",
        [("beat_f/diff_added_lines", ["        self.assertAlmostEqual(1, 2)  # places=6"])],
        "beat_f",
        "lack places=6",
    ),
    (
        "places-six-only-in-a-nested-call",
        [("beat_f/diff_added_lines", ["        self.assertAlmostEqual(round(x, places=6), 2)"])],
        "beat_f",
        "lack places=6",
    ),
    (
        "unbound-label-never-applied",
        [("beat_h/label_events", [])],
        "beat_h",
        "applied 0 times",
    ),
    (
        "bound-label-applied-instead-of-the-unbound-one",
        [("beat_h/label_events/0/label/name", "factory")],
        "beat_h",
        "applied 0 times",
    ),
    (
        "label-is-not-an-unbound-label",
        [("beat_h/label", "factory"), ("beat_h/label_events/0/label/name", "factory")],
        "beat_h",
        "not an unbound factory label",
    ),
    (
        "intermediate-usage-sample-over-the-cap",
        [("usage/samples/4/usage", 70.0)],
        "spend",
        "reached 50",
    ),
    (
        "no-usage-sample-after-a-beat",
        [("usage/samples", lambda ss: [s for s in ss if s["label"] != "after_c"])],
        "spend",
        "not measured at: ['after_c']",
    ),
    *[
        (f"teardown-{key}-unverified", [(f"teardown/{key}", False)], "teardown", key)
        for key in judge.TEARDOWN_KEYS
    ],
]


def test_judge_accepts_a_fully_passing_record_set() -> None:
    verdict = judge.judge_chain(copy.deepcopy(PASSING))
    assert [b.beat for b in verdict.beats] == list(judge.BEAT_ORDER)
    assert {b.beat: b.reasons for b in verdict.beats if b.reasons} == {}
    assert verdict.passed
    assert verdict.attempt == 1
    assert verdict.beats[2].measures["status_seconds"] == 20


@pytest.mark.parametrize(
    ("edits", "beat", "reason"), [c[1:] for c in CASES], ids=[c[0] for c in CASES]
)
def test_judge_rejects_each_broken_shape(
    edits: list[tuple[str, Any]], beat: str, reason: str
) -> None:
    failures = failed_beats(broken(*edits))
    assert beat in failures, f"{beat} passed"
    assert any(reason in text for text in failures[beat]), failures[beat]
    assert set(failures) == {beat}, f"another beat failed too: {failures}"


@pytest.mark.parametrize(
    "edits",
    [
        [("beat_a/comments/0/created_at", at(3))],  # exactly the 180 s bound
        [("beat_e/reader_command/output/error", "memory_rules.write_access_unverifiable: x")],
        [("beat_e/reader_permission", "read"), ("beat_e/reader_command/elapsed_seconds", 60)],
        [("usage/samples/8/usage", 59.99)],
        [("attempt", 2)],
        [("beat_g/remove_command/elapsed_seconds", 61)],
        [("beat_a/quiet_snapshot/requests/0/finished_at", at(6))],
        [("beat_f/diff_added_lines", ['self.assertAlmostEqual(f(")"), 2, places=6)  # places=7'])],
        [
            (
                "beat_f/diff_added_lines",
                ["self.assertAlmostEqual(", "    f(1),", "    2,", "    places=6,", ")"],
            )
        ],
        [("beat_b/open_pulls/0/body", lambda s: s.replace("Closes #11", "Closes #11."))],
        [
            (
                "beat_h/retired_bundle/output",
                {"a": [{"b": {"code": "bundle.result_declaration_retired"}}]},
            )
        ],
        [("beat_c/requests/2/tool_notes", [{"tool": "propose_rule"}])],
        [("beat_f/diff_added_lines", ["self.assertAlmostEqual(f(1), (2.0), places = 6)"])],
    ],
    ids=[
        "bound-is-inclusive",
        "unverifiable-refusal",
        "cli-bound-inclusive",
        "usage-just-under",
        "second-attempt",
        "removal-has-no-time-bound",
        "request-finished-at-the-deadline",
        "paren-in-a-string-and-a-comment",
        "call-split-over-lines",
        "closes-with-punctuation",
        "refusal-code-at-any-depth",
        "tool-note-as-mapping",
        "nested-parens-and-spacing",
    ],
)
def test_judge_still_accepts_the_valid_neighbour_of_each_bound(
    edits: list[tuple[str, Any]],
) -> None:
    assert failed_beats(broken(*edits)) == {}


def test_a_missing_or_malformed_section_fails_its_beat_without_raising() -> None:
    record = broken()
    del record["beat_c"]
    record["beat_d"]["merged_at"] = "not a time"
    failures = failed_beats(record)
    assert "no 'beat_c' section" in failures["beat_c"][0]
    assert "malformed" in failures["beat_d"][0]
    assert failed_beats(broken(("teardown", {}))) == {"teardown": failures_of_empty_teardown()}


def failures_of_empty_teardown() -> tuple[str, ...]:
    return (f"teardown is not verified: {list(judge.TEARDOWN_KEYS)}",)


def test_the_driver_can_judge_one_beat_to_stop_before_spending() -> None:
    assert judge.judge_beat("preflight", PASSING).passed
    stopped = judge.judge_beat("preflight", broken(("preflight/open_items", [{"number": 7}])))
    assert not stopped.passed


def test_verdict_evidence_is_plain_json() -> None:
    evidence = judge.judge_chain(broken(("attempt", 3))).evidence()
    assert json.loads(json.dumps(evidence)) == evidence
    assert evidence["passed"] is False and evidence["attempt"] == 3


def test_fixture_texts_equal_the_judges_constants() -> None:
    assert (FIXTURES / "answer-a.md").read_text().strip() == judge.ANSWER_TEXT
    assert (FIXTURES / "review-rule.md").read_text().strip() == judge.REVIEW_RULE_TEXT
    assert (FIXTURES / "requester-instructions.md").read_text().strip() == (
        "Use `assertEqual` for every assertion."
    )
    assert "decimal" not in (FIXTURES / "issue-a.md").read_text().lower()


# --- Sanitizer ---------------------------------------------------------------------
IDENTIFIERS: dict[str, list[str | int]] = {
    "fixture-repo": ["demo-owner/demo-repo", "demo-owner", "demo-repo", 9100001],
    "writer": ["fixture-writer", 9000001],
    "reader": ["fixture-reader", 9000002],
    "app": ["fixture-app", "fixture-app[bot]", 9000003, 7777777],
    "namespace": ["ns-fake-1"],
    "context": ["ctx-fake"],
    "registry": ["registry.example.invalid:5000"],
    "code-host": ["https://code-host.example.invalid", "code-host.example.invalid"],
}
RAW_VALUES = [str(v) for values in IDENTIFIERS.values() for v in values]


def test_sanitizer_replaces_every_identifier_in_urls_and_nested_json() -> None:
    evidence = {
        "url": "https://code-host.example.invalid/demo-owner/demo-repo/pull/12#pullrequestreview-7",
        "nested": {"reviews": [{"login": "Fixture-Writer", "user": {"id": 9000001}}]},
        "reader": ["fixture-reader", 9000002, {"fixture-reader": "x"}],
        "app": "fixture-app[bot] installed as 7777777 (app id 9000003)",
        "where": "ns-fake-1 on ctx-fake pushes registry.example.invalid:5000/img:tag",
        "unchanged": [1, 2.5, True, None, "pull/12"],
    }
    cleaned = judge.sanitize_evidence(evidence, IDENTIFIERS)
    assert cleaned == {
        "url": "<code-host>/<fixture-repo>/pull/12#pullrequestreview-7",
        "nested": {"reviews": [{"login": "<writer>", "user": {"id": "<writer>"}}]},
        "reader": ["<reader>", "<reader>", {"<reader>": "x"}],
        "app": "<app> installed as <app> (app id <app>)",
        "where": "<namespace> on <context> pushes <registry>/img:tag",
        "unchanged": [1, 2.5, True, None, "pull/12"],
    }
    text = json.dumps(cleaned)
    assert not [raw for raw in RAW_VALUES if raw.lower() in text.lower()]


def test_sanitizer_matches_numeric_ids_only_as_whole_numbers() -> None:
    cleaned = judge.sanitize_evidence(
        {"a": "19000011 and 9000001 and 90000012 and user/9000001/x"}, IDENTIFIERS
    )
    assert cleaned == {"a": "19000011 and <writer> and 90000012 and user/<writer>/x"}


def test_sanitizer_replaces_the_longest_identifier_first() -> None:
    cleaned = judge.sanitize_evidence("fixture-app[bot] and fixture-app", IDENTIFIERS)
    assert cleaned == "<app> and <app>"


def test_sanitizer_refuses_evidence_that_still_holds_a_raw_identifier() -> None:
    for leaky in (
        {"url": "https://x.example/demo-owner/demo-repo"},
        {"deep": [{"deeper": {"login": "fixture-reader"}}]},
        {"id": 9000002},
        "log line naming ns-fake-1",
        {"fixture-writer": "key leaks"},
    ):
        with pytest.raises(judge.EvidenceLeak) as error:
            judge.refuse_leaks(leaky, IDENTIFIERS)
        assert not [raw for raw in RAW_VALUES if raw in str(error.value)], (
            "the error echoes a value"
        )
    with pytest.raises(judge.EvidenceLeak, match="reader"):
        judge.refuse_leaks(json.dumps({"login": "fixture-reader"}), IDENTIFIERS)


def test_sanitizer_accepts_text_that_only_holds_placeholders() -> None:
    judge.refuse_leaks(
        {"a": "<writer> merged on <fixture-repo>"}, {"writer": ["writer"], "fixture-repo": ["repo"]}
    )
    ids = {"app": ["app"], "writer": ["writer"]}
    assert judge.sanitize_evidence("the app and the writer", ids) == "the <app> and the <writer>"


def test_serialized_evidence_has_no_raw_identifier() -> None:
    record = broken(("beat_a/issue_labels", ["factory-fixture-writer"]))
    evidence = {
        "record_hint": record["beat_a"]["issue_labels"],
        **judge.judge_chain(record).evidence(),
    }
    text = judge.serialize_evidence(evidence, IDENTIFIERS)
    assert "factory-<writer>" in text
    assert not [raw for raw in RAW_VALUES if raw.lower() in text.lower()]
    with pytest.raises(judge.EvidenceLeak):
        judge.refuse_leaks(text + "fixture-reader", IDENTIFIERS)


def test_sanitize_log_applies_the_same_replacement() -> None:
    assert judge.sanitize_log("pushed to ctx-fake as fixture-writer", IDENTIFIERS) == (
        "pushed to <context> as <writer>"
    )


@pytest.mark.parametrize(
    "identifiers",
    [{"nobody": ["x"]}, {"writer": [""]}, {"writer": ["  "]}],
    ids=["unknown-role", "empty", "blank"],
)
def test_sanitizer_refuses_unusable_identifier_sets(identifiers: dict[str, list[str]]) -> None:
    with pytest.raises(ValueError):
        judge.sanitize_evidence({"a": "b"}, identifiers)


def test_the_judge_module_is_pure() -> None:
    source = (Path(judge.__file__)).read_text()
    for forbidden in (
        "import subprocess",
        "import socket",
        "import urllib",
        "import requests",
        "open(",
    ):
        assert forbidden not in source


def test_sanitizer_replaces_percent_encoded_identifiers_in_urls_and_queries() -> None:
    evidence = {
        "url": "https://code-host.example.invalid/demo-owner/demo-repo/pull/1"
        "?author=Fixture%2dWriter&uid=%39%30%30%30%30%30%31&q=fixture%2Dreader%20x",
        "nested": [{"next": "https%3A%2F%2Fcode-host.example.invalid%2Fdemo-owner%2Fdemo-repo"}],
        "whole": "id=%39%30%30%30%30%30%310 and %349000001",
    }
    cleaned = judge.sanitize_evidence(evidence, IDENTIFIERS)
    assert cleaned == {
        "url": "<code-host>/<fixture-repo>/pull/1?author=<writer>&uid=<writer>&q=<reader>%20x",
        "nested": [{"next": "<code-host>%2F<fixture-repo>"}],
        "whole": "id=%39%30%30%30%30%30%310 and %349000001",
    }


def test_sanitizer_refuses_percent_encoded_leftovers() -> None:
    for leaky in (
        "https://x.example/p?login=Fixture%2dWriter",
        {"q": [{"uid": "%39%30%30%30%30%30%32"}]},
        "path=demo%2downer",
    ):
        with pytest.raises(judge.EvidenceLeak):
            judge.refuse_leaks(leaky, IDENTIFIERS)
