"""Offline contract of the live one-ticket scenario on Jira Cloud (#5793).

The live run needs a Jira Cloud site, a real model and a scratch cluster, so it
is proved by running it. These pin what must hold before then: the scenario's
registration and refusals (``-k registration``), the beat judge and its bounds
over the real Jira REST and usage-line shapes (``-k judge``), and the
anonymizer, the evidence serializer and ``check-clean`` (``-k anonymize``).
Every identity below is invented; nothing touches the network.
"""

from __future__ import annotations

import ast
import base64
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import factory_e2e as fe
import pytest

otl = fe.load_one_ticket_live()

HERE = Path(__file__).resolve().parent
MODULE = HERE.parent / "one_ticket_live.py"
REPO_ROOT = HERE.parents[2]
SHA = "c" * 40

# --- invented identities, one of each kind --------------------------------------------

REPO = "acme-labs/widget-factory-e2e"
APP_ID = 913377
APP_SLUG = "acme-factory-app"
INSTALLATION_ID = 55443322
REPOSITORY_ID = 987654321
ACTOR = "ada-actor"
OPERATOR = "olly-operator"
SITE = "acme-test.atlassian.net"
CLOUD_ID = "33333333-3333-4333-8333-333333333333"
PROJECT = "ACME"
LABELLER_EMAIL = "labeller@example.com"
LABELLER_ACCOUNT = "557058:aaaaaaaa-0000-4000-8000-0000000000a1"
LABELLER_NAME = "Ada Labeller"
BOT_EMAIL = "factory-bot@example.com"
BOT_ACCOUNT = "712020:bbbbbbbb-0000-4000-8000-0000000000b0"
BOT_NAME = "Acme Factory Bot"
CONTEXT = "kind-acme-scratch"
REGISTRY = "registry.acme.example:5000/factory"
BINARY = "/opt/acme-tools/bin/curie"
HOME = "/home/acme-user"
STATUS_ID = "10001"
ISSUE_KEY = f"{PROJECT}-12"


def _environ(tmp_path: Path) -> dict[str, str]:
    app = tmp_path / "app"
    app.mkdir(parents=True, exist_ok=True)
    (app / "app.json").write_text(
        json.dumps({"id": APP_ID, "slug": APP_SLUG, "installation_id": INSTALLATION_ID})
    )
    (app / "app.pem").write_text("not a real key\n")
    (app / "webhook_secret").write_text("not-the-dev-default\n")
    (tmp_path / "labeler-token").write_text("labeller-fixture-token\n")
    (tmp_path / "bot-token").write_text("bot-fixture-token\n")
    facts = {
        "site": SITE,
        "cloud_id": CLOUD_ID,
        "project_key": PROJECT,
        "labeler_email": LABELLER_EMAIL,
        "labeler_token_file": "labeler-token",
        "bot_email": BOT_EMAIL,
        "bot_account_id": BOT_ACCOUNT,
        "bot_token_file": "bot-token",
    }
    env_file = tmp_path / "jira.env"
    env_file.write_text("".join(f"{k}={v}\n" for k, v in facts.items()))
    return {
        "CURIE_FACTORY_REPO": REPO,
        "CURIE_FACTORY_APP_DIR": str(app),
        "CURIE_FACTORY_ACTOR_GH_USER": ACTOR,
        "CURIE_FACTORY_OPERATOR_LOGIN": OPERATOR,
        "CURIE_FACTORY_JIRA_ENV_FILE": str(env_file),
        "CURIE_FACTORY_KUBE_CONTEXT": CONTEXT,
        "CURIE_FACTORY_LAYER_REGISTRY": REGISTRY,
        "CURIE_BIN": BINARY,
        "HOME": HOME,
    }


def _fetch_myself(actor: str) -> Mapping[str, Any]:
    if actor == "labeler":
        return {
            "accountId": LABELLER_ACCOUNT,
            "displayName": LABELLER_NAME,
            "emailAddress": LABELLER_EMAIL,
        }
    return {"accountId": BOT_ACCOUNT, "displayName": BOT_NAME, "emailAddress": BOT_EMAIL}


def _no_login(role: str) -> str:
    raise AssertionError(f"no GitHub login read is needed for {role}")


def _identities(tmp_path: Path) -> Any:
    ids = otl.load_identities(
        _environ(tmp_path), fetch_myself=_fetch_myself, github_login=_no_login
    )
    otl.add_identity(ids, "fixture-repo", REPOSITORY_ID)
    otl.add_identity(ids, "status", STATUS_ID)
    return ids


@pytest.fixture(autouse=True)
def _isolated_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    """No sanitizer a test installs outlives it."""

    monkeypatch.setattr(fe, "LOG_FILTERS", [])
    monkeypatch.setattr(otl, "_INSTALLED_FILTERS", [])


# --- registration -----------------------------------------------------------------------


def test_registration_loop_forge_fixes_jira_cloud() -> None:
    assert fe.LOOP_SCENARIOS["one-ticket"] == "jira_cloud"
    assert fe.loop_forge("one-ticket", "jira_cloud") == "jira_cloud"
    assert fe.loop_forge("one-ticket", None) == "jira_cloud"


def test_registration_loop_forge_refuses_another_forge() -> None:
    with pytest.raises(fe.ConfigError, match="one-ticket drives the jira_cloud forge"):
        fe.loop_forge("one-ticket", "github")


def test_registration_forge_flag_refusal_names_every_loop_scenario() -> None:
    with pytest.raises(fe.ConfigError) as refused:
        fe.loop_forge("issue-to-pr", "github")
    assert str(refused.value) == "--forge applies only to forge-loop, github-loop and one-ticket"


def test_registration_parse_args_carries_the_answer_file() -> None:
    args = fe.parse_args(
        ["run", "--scenario", "one-ticket", "--forge", "jira_cloud", "--answer-file", "x"]
    )
    assert args.scenario == "one-ticket"
    assert args.forge == "jira_cloud"
    assert args.answer_file == Path("x")


def test_registration_scenario_is_bound_and_opens_no_seed_issue() -> None:
    assert fe.SCENARIOS["one-ticket"] is not None
    assert fe.resolve_scenario("one-ticket") is fe.SCENARIOS["one-ticket"]
    assert fe.scenario_opens_seed_issue("one-ticket") is False
    hooks = fe.scenario_hooks("one-ticket")
    assert hooks.before_run is not None and hooks.finish is not None
    assert hooks.private_binary is True


def _refuse_any_install(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_install(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("nothing may be configured or installed before this refusal")

    monkeypatch.setattr(fe, "load_config", no_install)
    monkeypatch.setattr(fe, "Preflight", no_install)
    monkeypatch.setattr(fe, "_resolve_candidate", no_install)
    monkeypatch.setattr(fe, "resolve_curie_binary", no_install)


@pytest.mark.parametrize(
    ("argv", "named"),
    [
        (["run", "--scenario", "one-ticket", "--forge", "jira_cloud"], "--answer-file"),
        (["run", "--scenario", "one-ticket"], "--answer-file"),
        (
            ["run", "--scenario", "forge-loop", "--forge", "github", "--answer-file", "a.md"],
            "--answer-file applies only to one-ticket",
        ),
        (
            ["run", "--scenario", "issue-to-pr", "--answer-file", "a.md"],
            "--answer-file applies only to one-ticket",
        ),
    ],
)
def test_registration_main_refuses_before_anything_is_installed(
    argv: list[str],
    named: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _refuse_any_install(monkeypatch)
    assert fe.main(argv) == fe.EXIT_CONFIG
    assert named in capsys.readouterr().err


@pytest.mark.parametrize(
    ("text", "named"),
    [
        ("", "is empty"),
        ("   \n", "is empty"),
        ("@ada-labeller round half up, two places.", "mentions someone"),
        ("Round half up. cc [~accountid:557058:x]", "mentions someone"),
    ],
)
def test_registration_main_refuses_an_answer_that_is_not_plain(
    text: str,
    named: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _refuse_any_install(monkeypatch)
    answer = tmp_path / "answer.md"
    answer.write_text(text)
    argv = ["run", "--scenario", "one-ticket", "--answer-file", str(answer)]
    assert fe.main(argv) == fe.EXIT_CONFIG
    assert named in capsys.readouterr().err


class _Reached(Exception):
    """main got past the Preflight construction and its scenario options."""


def test_registration_main_reaches_the_install_with_the_answer_and_the_jira_ticket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = _environ(tmp_path)
    for key in (
        "CURIE_FACTORY_KUBE_CONTEXT",
        "CURIE_FACTORY_APP_DIR",
        "CURIE_FACTORY_LAYER_REGISTRY",
    ):
        monkeypatch.setenv(key, env[key])
    monkeypatch.setenv("CURIE_FACTORY_ACTOR_TOKEN", "actor-token")
    monkeypatch.setenv("CURIE_FACTORY_REPO", REPO)
    monkeypatch.setenv("CURIE_FACTORY_MODEL_API_KEY", "placeholder-model-key")
    monkeypatch.setattr(fe, "_resolve_candidate", lambda *_a, **_k: SHA)
    built: list[Any] = []

    class FakePreflight:
        def __init__(self, *_a: Any, **kwargs: Any) -> None:
            self.kwargs = kwargs
            self.evidence: dict[str, Any] = {}
            self.scenario_options: dict[str, Any] = {}
            built.append(self)

    def stop(*_a: Any, **_k: Any) -> None:
        raise _Reached

    monkeypatch.setattr(fe, "Preflight", FakePreflight)
    monkeypatch.setattr(fe.signal, "signal", stop)
    answer = REPO_ROOT / "tools/factory-e2e/fixtures/one-ticket-answer.md"
    with pytest.raises(_Reached):
        fe.main(["run", "--scenario", "one-ticket", "--answer-file", str(answer)])
    (preflight,) = built
    assert preflight.evidence["forge"] == "jira_cloud"
    assert preflight.kwargs["issue_spec"] == fe.parse_issue_file(fe.ONE_TICKET_ISSUE_FILE)
    assert preflight.scenario_options["answer_text"] == answer.read_text().strip()


def test_registration_fixtures_leave_the_decision_open_and_answer_it_plainly() -> None:
    title, body = fe.parse_issue_file(fe.ONE_TICKET_ISSUE_FILE)
    assert "rounding" in title
    assert "not decided here" in body
    assert "unitconv/tests/test_convert.py" in body
    answer = fe.read_answer_file(REPO_ROOT / "tools/factory-e2e/fixtures/one-ticket-answer.md")
    assert "half up" in answer and "two decimal places" in answer
    assert "\n" not in answer


# --- the judge ----------------------------------------------------------------------------


def _passing_obs() -> dict[str, Any]:
    return {
        "investigating_s": 25.0,
        "question_s": 420.0,
        "settled": True,
        "settled_s": 430.0,
        "pre_answer_read": "ok",
        "live_before_answer": [],
        "answer_s": 435.0,
        "second_request_s": 445.0,
        "answer_received_s": 450.0,
        "pull_requests": [
            {"number": 7, "opened_s": 1600.0, "names_key": True, "has_citation": True}
        ],
        "answered_run_ended": True,
        "merge_ok": True,
        "merged_s": 2000.0,
        "merged_notice_s": 2040.0,
        "status_id_unchanged": True,
        "request_count": 2,
        "usage": {"kind": "total", "total": "4.20"},
        "factory_created_issues": 0,
    }


def test_judge_passes_a_run_where_every_beat_holds() -> None:
    assert otl.judge(_passing_obs()) == []


def _second_pull(obs: dict[str, Any]) -> None:
    obs["pull_requests"].append(dict(obs["pull_requests"][0], number=8))


BROKEN: dict[str, tuple[dict[str, Any] | Any, str]] = {
    "no-investigating": (
        {"investigating_s": None},
        "no 'Investigating this ticket.' status line within 180 s of labelling",
    ),
    "investigating-late": (
        {"investigating_s": 181.0},
        "no 'Investigating this ticket.' status line within 180 s of labelling",
    ),
    "no-question": ({"question_s": None}, "no question comment within 1800 s of labelling"),
    "question-late": ({"question_s": 1801.0}, "no question comment within 1800 s of labelling"),
    "never-settled": (
        {"settled": False},
        "the question ending was never observed settled (the questions label on the issue "
        "and no live request in the same read)",
    ),
    "answer-before-settlement": (
        {"settled_s": 440.0},
        "the answer was posted before the question ending was observed settled",
    ),
    "live-between-settlement-and-answer": (
        {"live_before_answer": ["queued"]},
        "a request was live between the settlement and the answer: queued",
    ),
    "pre-answer-read-unreadable": (
        {"pre_answer_read": "unreadable"},
        "the work item could not be read immediately before the answer",
    ),
    "pre-answer-read-no-request": (
        {"pre_answer_read": "no_request"},
        "the work item showed no request immediately before the answer",
    ),
    "pre-answer-read-missing": (
        {"pre_answer_read": None},
        "the work item was not read immediately before the answer",
    ),
    "answered-run-never-ended": (
        {"answered_run_ended": False, "merge_ok": False},
        f"the answered run did not end within {otl.ENDING_SECONDS} s of the pull request; "
        "not merged",
    ),
    "second-request-late": (
        {"second_request_s": 616.0},
        "the second request came 181 s after the answer, over 180 s",
    ),
    "second-request-missing": ({"second_request_s": None}, "no second request after the answer"),
    "no-answer-received": (
        {"answer_received_s": None},
        "no 'Answer received.' line within 180 s of the answer",
    ),
    "answer-received-late": (
        {"answer_received_s": 616.0},
        "no 'Answer received.' line within 180 s of the answer",
    ),
    "second-pull-request": (_second_pull, "2 pull requests opened, expected exactly 1"),
    "no-pull-request": ({"pull_requests": []}, "0 pull requests opened, expected exactly 1"),
    "pull-request-late": (
        {"pull_requests": [{"opened_s": 4036.0, "names_key": True, "has_citation": True}]},
        "the pull request opened 3601 s after the answer, over 3600 s after the answer",
    ),
    "body-without-key": (
        {"pull_requests": [{"opened_s": 900.0, "names_key": False, "has_citation": True}]},
        "the pull request body does not name the Jira key",
    ),
    "body-without-citation": (
        {"pull_requests": [{"opened_s": 900.0, "names_key": True, "has_citation": False}]},
        "the pull request body has no line starting 'Answered on the ticket by '",
    ),
    "merge-failed": ({"merge_ok": False}, "the merge failed"),
    "no-merged-notice": (
        {"merged_notice_s": None},
        "no 'Pull request merged:' line within 600 s of the merge",
    ),
    "merged-notice-late": (
        {"merged_notice_s": 2601.0},
        "no 'Pull request merged:' line within 600 s of the merge",
    ),
    "status-id-changed": (
        {"status_id_unchanged": False},
        "the Jira status id changed between labelling and the merged notice",
    ),
    "status-id-unreadable": (
        {"status_id_unchanged": None},
        "the Jira status id could not be read before labelling and after the merge",
    ),
    "third-request": ({"request_count": 3}, "3 execution requests, over the bound of 2"),
    "request-count-unreadable": ({"request_count": None}, "the request count could not be read"),
    "factory-created-an-issue": (
        {"factory_created_issues": 1},
        "the factory account created 1 Jira issues, expected 0",
    ),
    "factory-created-issues-unreadable": (
        {"factory_created_issues": None},
        "the Jira issues the factory account created could not be read",
    ),
}


@pytest.mark.parametrize("case", sorted(BROKEN))
def test_judge_rejects_each_broken_shape(case: str) -> None:
    change, message = BROKEN[case]
    obs = _passing_obs()
    if callable(change):
        change(obs)
    else:
        obs.update(change)
    failures = otl.judge(obs)
    assert message in failures, failures


# Literal lines `usage_line` (apps/api/src/curie_api/factory_usage.py) produces; the
# drift test below rebuilds each from the real function.
USAGE_TOTAL = (
    "Usage: implementer 1,234,567 tokens ($12.34), reviewers 45,000 tokens ($0.56), "
    "total $12.90 (estimated from openrouter prices as of 2026-11-01)"
)
USAGE_THOUSANDS = (
    "Usage: implementer 98,000,000 tokens ($1,234.56), total $1,234.56 "
    "(estimated from openrouter prices as of 2026-11-01)"
)
USAGE_AT_LEAST = (
    "Usage: implementer 900,000 tokens ($3.10), total at least $3.10 "
    "(estimated from openrouter prices as of 2026-11-01), usage not reported for 1 run"
)
USAGE_UNKNOWN = "Usage: implementer 12,000 tokens (cost unknown), total cost unknown"
USAGE_NOT_REPORTED = "Usage: not reported for 1 run"


def test_judge_usage_lines_are_the_ones_usage_line_writes() -> None:
    usage = pytest.importorskip("curie_api.factory_usage")
    schema = pytest.importorskip("curie_api.schemas.workitems")
    source = [
        schema.WorkItemUsagePriceSource(
            source="openrouter", as_of=datetime(2026, 11, 1, tzinfo=UTC)
        )
    ]

    def line(**kwargs: Any) -> str:
        fields: dict[str, Any] = {
            "work_item_id": uuid.uuid4(),
            "models": [],
            "requests": [],
            "price_sources": source,
            "requests_without_usage": 0,
            **kwargs,
        }
        return str(usage.usage_line(schema.WorkItemUsageOut(**fields)))

    role = schema.WorkItemUsageRole
    assert USAGE_TOTAL == line(
        total_tokens=1_279_567,
        estimated_cost_usd=Decimal("12.90"),
        cost_complete=True,
        roles={
            "implementer": role(tokens=1_234_567, estimated_cost_usd=Decimal("12.34")),
            "reviewer": role(tokens=45_000, estimated_cost_usd=Decimal("0.56")),
        },
    )
    assert USAGE_THOUSANDS == line(
        total_tokens=98_000_000,
        estimated_cost_usd=Decimal("1234.56"),
        cost_complete=True,
        roles={"implementer": role(tokens=98_000_000, estimated_cost_usd=Decimal("1234.56"))},
    )
    assert USAGE_AT_LEAST == line(
        total_tokens=900_000,
        estimated_cost_usd=Decimal("3.10"),
        cost_complete=False,
        requests_without_usage=1,
        roles={"implementer": role(tokens=900_000, estimated_cost_usd=Decimal("3.10"))},
    )
    assert USAGE_UNKNOWN == line(
        total_tokens=12_000,
        estimated_cost_usd=None,
        cost_complete=False,
        price_sources=[],
        roles={"implementer": role(tokens=12_000, estimated_cost_usd=None, cost_complete=False)},
    )
    assert USAGE_NOT_REPORTED == line(
        total_tokens=0,
        estimated_cost_usd=None,
        cost_complete=False,
        requests_without_usage=1,
        roles={},
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (USAGE_TOTAL, ("total", Decimal("12.90"))),
        (USAGE_THOUSANDS, ("total", Decimal("1234.56"))),
        (USAGE_AT_LEAST, ("at_least", Decimal("3.10"))),
        (USAGE_UNKNOWN, ("unknown", None)),
        (USAGE_NOT_REPORTED, ("missing", None)),
        ("Factory status: completed", ("missing", None)),
        (
            f"Some text\n{USAGE_TOTAL}\nPull request merged: https://example.com/pr/7",
            ("total", Decimal("12.90")),
        ),
    ],
)
def test_judge_usage_total_classifies_each_line(text: str, expected: tuple[str, Any]) -> None:
    assert otl.usage_total(text) == expected


def _with_usage(line: str) -> dict[str, Any]:
    kind, total = otl.usage_total(line)
    obs = _passing_obs()
    obs["usage"] = {"kind": kind, "total": None if total is None else str(total)}
    return obs


@pytest.mark.parametrize(
    ("line", "message"),
    [
        (
            "Usage: implementer 9,000,000 tokens ($25.01), total $25.01",
            "the estimated cost is $25.01, over the 25 USD bound",
        ),
        (
            "Usage: implementer 9,000,000 tokens ($30.00), total at least $30.00, "
            "usage not reported for 1 run",
            "the estimated cost is at least $30.00, over the 25 USD bound",
        ),
        (USAGE_UNKNOWN, "the usage line reads 'total cost unknown'"),
        (USAGE_NOT_REPORTED, "the merged status comment carries no usage total"),
        ("Factory status: completed", "the merged status comment carries no usage total"),
    ],
)
def test_judge_rejects_a_cost_over_or_outside_the_bound(line: str, message: str) -> None:
    assert message in otl.judge(_with_usage(line))


@pytest.mark.parametrize(
    "line",
    [
        "Usage: implementer 900,000 tokens ($3.10), total at least $3.10, "
        "usage not reported for 1 run",
        "Usage: implementer 9,000,000 tokens ($25.00), total $25.00",
    ],
)
def test_judge_passes_a_cost_within_the_bound(line: str) -> None:
    assert otl.judge(_with_usage(line)) == []


# Handoff (b): the builders over the real Jira REST shapes, into the judge.


def _issue(status_id: str, name: str, labels: list[str]) -> dict[str, Any]:
    """A ``GET /rest/api/3/issue/{id}?fields=status,labels`` response."""

    return {
        "expand": "renderedFields,names,schema,operations,editmeta,changelog",
        "id": "10042",
        "self": "https://api.atlassian.com/ex/jira/x/rest/api/3/issue/10042",
        "key": "ACME-12",
        "fields": {
            "labels": labels,
            "status": {
                "self": f"https://api.atlassian.com/ex/jira/x/rest/api/3/status/{status_id}",
                "description": "",
                "iconUrl": "https://example.atlassian.net/",
                "name": name,
                "id": status_id,
                "statusCategory": {
                    "self": "https://api.atlassian.com/ex/jira/x/rest/api/3/statuscategory/2",
                    "id": 2,
                    "key": "new",
                    "colorName": "blue-gray",
                    "name": "To Do",
                },
            },
        },
    }


def _paragraph(*inline: dict[str, Any]) -> dict[str, Any]:
    return {"type": "paragraph", "content": list(inline)}


def _text(text: str, href: str | None = None) -> dict[str, Any]:
    node: dict[str, Any] = {"type": "text", "text": text}
    if href is not None:
        node["marks"] = [{"type": "link", "attrs": {"href": href}}]
    return node


def _comment(comment_id: str, marker: str | None, *blocks: dict[str, Any]) -> dict[str, Any]:
    """One value of ``POST /comment/list?expand=properties``, in the recorded shape of
    tools/factory-e2e/fixtures/jira/comments.json (properties as a key/value list)."""

    properties = (
        [{"key": "curie.factory", "value": {"marker": marker, "body_sha256": "0" * 64}}]
        if marker
        else []
    )
    return {
        "self": f"https://example.atlassian.net/rest/api/3/issue/10042/comment/{comment_id}",
        "id": comment_id,
        "author": {"accountId": "712020:bot", "accountType": "app", "displayName": "Bot"},
        "body": {"type": "doc", "version": 1, "content": list(blocks)},
        "created": "2026-11-20T09:00:00.000+0000",
        "updated": "2026-11-20T09:05:00.000+0000",
        "properties": properties,
    }


PR_URL = "https://github.com/acme/fixture/pull/7"


def _comment_page() -> dict[str, Any]:
    return {
        "startAt": 0,
        "maxResults": 3,
        "total": 3,
        "isLast": True,
        "values": [
            _comment(
                "10100",
                "curie-execution-request:11111111-1111-4111-8111-111111111111",
                _paragraph(
                    _text(
                        "Investigating this ticket. The factory will open a pull request or "
                        "post its questions here."
                    )
                ),
                _paragraph(_text("Status: Running")),
            ),
            _comment(
                "10101",
                "curie-factory-questions:11111111-1111-4111-8111-111111111111",
                _paragraph(_text("Before I can build this, two questions:")),
                {
                    "type": "orderedList",
                    "content": [
                        {
                            "type": "listItem",
                            "content": [_paragraph(_text("Which rounding mode?"))],
                        },
                        {"type": "listItem", "content": [_paragraph(_text("Default precision?"))]},
                    ],
                },
            ),
            _comment(
                "10102",
                "curie-execution-request:22222222-2222-4222-8222-222222222222",
                _paragraph(_text("Answer received. The factory is working on this ticket again.")),
                _paragraph(_text("Result: completed"), {"type": "hardBreak"}, _text("Cause: none")),
                _paragraph(_text(USAGE_AT_LEAST)),
                _paragraph(_text("Pull request merged: "), _text(PR_URL, href=PR_URL)),
            ),
        ],
    }


def _obs_from_shapes(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    obs = _passing_obs()
    seen: dict[str, bool] = {}
    for comment in _comment_page()["values"]:
        marker = otl.comment_marker(comment) or ""
        text = otl.comment_text(comment)
        if marker.startswith(otl.STATUS_MARKER_PREFIX):
            seen["investigating"] = seen.get("investigating") or otl.has_line(
                text, otl.INVESTIGATING_LINE
            )
            seen["received"] = seen.get("received") or otl.has_line(text, otl.ANSWER_RECEIVED_LINE)
            if otl.has_line(text, otl.MERGED_LINE):
                kind, total = otl.usage_total(text)
                obs["usage"] = {"kind": kind, "total": None if total is None else str(total)}
                seen["merged"] = True
        if marker.startswith(otl.QUESTIONS_MARKER_PREFIX):
            seen["question"] = True
    for name, key in (
        ("investigating", "investigating_s"),
        ("question", "question_s"),
        ("received", "answer_received_s"),
        ("merged", "merged_notice_s"),
    ):
        if not seen.get(name):
            obs[key] = None
    first, last = otl.status_id(before), otl.status_id(after)
    obs["status_id_unchanged"] = None if first is None or last is None else first == last
    return obs


def test_judge_handoff_reads_real_jira_shapes_and_catches_a_same_category_status_change() -> None:
    before = _issue("10001", "To Do", [])
    after = _issue("10002", "Backlog", ["curie-factory"])
    after["fields"]["status"]["statusCategory"] = {"key": "new"}
    obs = _obs_from_shapes(before, after)
    # A category comparison would pass this pair: both are "new".
    assert before["fields"]["status"]["statusCategory"]["key"] == "new"
    assert after["fields"]["status"]["statusCategory"]["key"] == "new"
    assert obs["usage"] == {"kind": "at_least", "total": "3.10"}
    assert otl.judge(obs) == ["the Jira status id changed between labelling and the merged notice"]


def test_judge_handoff_passes_the_same_shapes_with_an_unchanged_status() -> None:
    before = _issue("10001", "To Do", [])
    obs = _obs_from_shapes(before, _issue("10001", "To Do", ["curie-factory"]))
    assert otl.judge(obs) == []


def test_judge_builders_read_markers_text_and_labels() -> None:
    values = _comment_page()["values"]
    assert [otl.comment_marker(c) for c in values] == [
        "curie-execution-request:11111111-1111-4111-8111-111111111111",
        "curie-factory-questions:11111111-1111-4111-8111-111111111111",
        "curie-execution-request:22222222-2222-4222-8222-222222222222",
    ]
    text = otl.comment_text(values[2])
    assert f"Pull request merged: {PR_URL}" in text.splitlines()
    assert "Cause: none" in text.splitlines()
    assert otl.comment_marker({"properties": [{"key": "other", "value": {"marker": "x"}}]}) is None
    assert otl.comment_marker({"properties": []}) is None
    assert otl.status_id({"fields": {}}) is None
    assert otl.issue_labels(_issue("1", "x", ["curie-factory:questions"])) == [
        "curie-factory:questions"
    ]
    # The status line is a sentence followed by more words; a prefix of a word is not it.
    assert otl.has_line("Investigating this ticket. More.", otl.INVESTIGATING_LINE)
    assert not otl.has_line("Investigating this ticket.Later", otl.INVESTIGATING_LINE)


def test_judge_pull_request_body_rules() -> None:
    body = (
        "Adds rounding.\n\nJira: ACME-12 https://example.atlassian.net/browse/ACME-12\n\n"
        "Answered on the ticket by Ada: round half up."
    )
    assert otl.names_key(body, "ACME-12")
    assert otl.has_citation(body)
    assert not otl.names_key("Jira: ACME-120", "ACME-12")
    assert not otl.names_key("Jira: XACME-12", "ACME-12")
    assert not otl.has_citation("The answer was Answered on the ticket by Ada")
    assert otl.live_statuses(
        {"requests": [{"status": "completed"}, {"status": "queued"}, {"status": "running"}]}
    ) == ["queued", "running"]


# --- anonymize ----------------------------------------------------------------------------


def _raw_values(ids: Mapping[str, list[Any]]) -> list[str]:
    return sorted({str(value) for values in ids.values() for value in values})


def _evidence() -> dict[str, Any]:
    return {
        "kube_context": CONTEXT,
        "actor_login": ACTOR.upper(),
        "fixture_repository_id": REPOSITORY_ID,
        "jira_issue_key": ISSUE_KEY,
        "jira": {"project_key": PROJECT, "binding": "jira"},
        "steps": [
            {"step": "Jira issue opened and labelled by the labeler", "key": ISSUE_KEY},
            {"step": "browse", "url": f"https://{SITE}/browse/{ISSUE_KEY}"},
        ],
        REPO: {"nested": [OPERATOR, (f"{APP_SLUG}[bot]", APP_ID), str(INSTALLATION_ID)]},
        "app": {"slug": APP_SLUG.upper(), "id": str(APP_ID), "repo_name": "widget-factory-e2e"},
        "gateway": f"https://api.atlassian.com/ex/jira/{CLOUD_ID}/rest/api/3/myself",
        "accounts": [
            LABELLER_ACCOUNT,
            LABELLER_NAME,
            LABELLER_EMAIL,
            BOT_ACCOUNT,
            BOT_EMAIL,
            BOT_NAME,
        ],
        "status": {"before": STATUS_ID, "as_int": int(STATUS_ID)},
        "registry": f"{REGISTRY}/runner:cand-abc",
        "registry_host_only": "registry.acme.example:5000",
        "binary": f"{BINARY} --version",
        "home": f"{HOME}/.config/curie",
        "log": f"[factory-e2e 10:00:00] labelled {ISSUE_KEY} as {LABELLER_EMAIL} on {SITE}",
        "owner": "Acme-Labs",
        "plain": {"count": 12, "seconds": 435.5, "ok": True},
    }


def test_anonymize_replaces_every_identity_kind_in_a_nested_evidence_object(tmp_path: Path) -> None:
    ids = _identities(tmp_path)
    assert set(ids) == {
        "fixture-repo",
        "app",
        "installation",
        "actor",
        "operator",
        "site",
        "cloud",
        "project",
        "labeller",
        "bot",
        "context",
        "registry",
        "binary",
        "home",
        "status",
    }
    evidence = _evidence()
    cleaned = otl.anonymize(evidence, ids)
    dumped = json.dumps(cleaned)
    for raw in _raw_values(ids):
        assert raw not in dumped, raw
    assert otl.find_identities(dumped, ids) == []
    assert cleaned["jira_issue_key"] == "project-12"
    assert cleaned["actor_login"] == "actor"
    assert cleaned["fixture_repository_id"] == "fixture-repo"
    assert cleaned["fixture-repo"] == {"nested": ["operator", ("app", "app"), "installation"]}
    assert cleaned["status"] == {"before": "status", "as_int": "status"}
    assert cleaned["plain"] == {"count": 12, "seconds": 435.5, "ok": True}
    assert cleaned["steps"][1]["url"] == "https://site/browse/project-12"
    # The input is not changed.
    assert evidence["jira_issue_key"] == ISSUE_KEY


def test_anonymize_matching_rules(tmp_path: Path) -> None:
    ids = _identities(tmp_path)
    # Logins match case-insensitively; the project key only as written.
    assert otl.anonymize("by Ada-Actor and OLLY-OPERATOR", ids) == "by actor and operator"
    assert otl.anonymize("ACME-12, acme-12 and ACMEWIDGET", ids) == (
        "project-12, acme-12 and ACMEWIDGET"
    )
    # A short project key is not replaced inside a longer word.
    assert otl.anonymize("XACME and ACMEX stay", ids) == "XACME and ACMEX stay"
    # Numeric ids only as whole digit runs.
    assert otl.anonymize(f"{REPOSITORY_ID} 9{REPOSITORY_ID} {REPOSITORY_ID}0", ids) == (
        f"fixture-repo 9{REPOSITORY_ID} {REPOSITORY_ID}0"
    )
    assert otl.anonymize(REPOSITORY_ID + 1, ids) == REPOSITORY_ID + 1
    # A value under 2 characters is never an identity.
    assert otl.anonymize("x y", {"site": ["x"]}) == "x y"


def test_anonymize_log_lines_through_the_installed_filter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ids = _identities(tmp_path)
    p = _preflight(tmp_path, monkeypatch)
    otl.install_sanitizer(p, ids)
    line = _evidence()["log"]
    fe.log(line)
    err = capsys.readouterr().err
    assert "labelled project-12 as labeller on site" in err
    assert otl.find_identities(err, ids) == []
    # A second install replaces the first filter rather than stacking.
    otl.install_sanitizer(p, ids)
    assert len(fe.LOG_FILTERS) == 1


def test_anonymize_the_outcome_summary(tmp_path: Path) -> None:
    ids = _identities(tmp_path)
    obs = _passing_obs()
    obs.update(model_usage_before=10.0, model_usage_after=14.25, verdict="passed")
    evidence = {"candidate_commit": SHA, "result": f"passed for {ACTOR} on {SITE}", "scenario": obs}
    summary = otl.outcome_summary(evidence, ids)
    assert otl.find_identities(summary, ids) == []
    lines = summary.splitlines()
    assert lines[0] == f"candidate commit: {SHA}"
    assert lines[1] == "scenario result: passed for actor on site"
    assert lines[2] == "requests: 2; pull requests: 1; factory-created Jira issues: 0"
    assert "labelling to investigating 25" in lines[3]
    assert "answer to second request 10" in lines[3]
    assert "answer to pull request 1165" in lines[3]
    assert "merge to merged notice 40" in lines[3]
    assert lines[4] == "cost: total $4.20 (bound 25 USD)"
    assert lines[5] == "model key usage: before 10.00 USD, after 14.25 USD"


def test_anonymize_check_clean_exits_1_on_the_raw_file_and_prints_clean_on_the_anonymized_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    environ = _environ(tmp_path)
    ids = _identities(tmp_path)
    raw = tmp_path / "raw.json"
    raw.write_text(json.dumps(_evidence(), indent=2, default=list))
    clean = tmp_path / "clean.json"
    clean.write_text(json.dumps(otl.anonymize(_evidence(), ids), indent=2))

    def run(path: Path) -> tuple[int, str]:
        code = otl.main(
            ["check-clean", str(path)],
            environ=environ,
            fetch_myself=_fetch_myself,
            github_login=_no_login,
        )
        return code, capsys.readouterr().out

    code, out = run(raw)
    assert code == 1
    assert out.startswith("identities found: ")
    roles = out.removeprefix("identities found: ").strip().split(", ")
    assert {"labeller", "bot", "project", "site", "fixture-repo", "home"} <= set(roles)
    for value in _raw_values(ids):
        assert value not in out
    assert run(clean) == (0, "clean\n")


def test_anonymize_check_clean_fails_closed_without_naming_a_value(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    environ = _environ(tmp_path)
    target = tmp_path / "clean.txt"
    target.write_text("nothing here\n")
    missing = {k: v for k, v in environ.items() if k != "CURIE_FACTORY_JIRA_ENV_FILE"}
    assert (
        otl.main(
            ["check-clean", str(target)],
            environ=missing,
            fetch_myself=_fetch_myself,
            github_login=_no_login,
        )
        == 2
    )
    assert "CURIE_FACTORY_JIRA_ENV_FILE" in capsys.readouterr().err

    def refused(_actor: str) -> Mapping[str, Any]:
        return {}

    assert (
        otl.main(
            ["check-clean", str(target)],
            environ=environ,
            fetch_myself=refused,
            github_login=_no_login,
        )
        == 2
    )
    err = capsys.readouterr().err
    assert "/myself" in err
    for value in (SITE, CLOUD_ID, LABELLER_EMAIL, HOME):
        assert value not in err
    gone = tmp_path / "gone.txt"
    assert (
        otl.main(
            ["check-clean", str(gone)],
            environ=environ,
            fetch_myself=_fetch_myself,
            github_login=_no_login,
        )
        == 2
    )


def test_anonymize_module_imports_only_the_standard_library() -> None:
    tree = ast.parse(MODULE.read_text())
    top: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            top.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top.add(node.module.split(".")[0])
    assert top - {"__future__"} <= set(sys.stdlib_module_names), top


def test_anonymize_check_clean_runs_under_an_isolated_interpreter(tmp_path: Path) -> None:
    # -I: no repository module is importable; the missing Jira env file refuses
    # before any account read, so no network is reached.
    env = {"PATH": os.environ.get("PATH", ""), "CURIE_FACTORY_OPERATOR_LOGIN": OPERATOR}
    done = subprocess.run(
        [sys.executable, "-I", str(MODULE), "check-clean", str(tmp_path / "x.json")],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
        check=False,
        timeout=60,
    )
    assert done.returncode == 2, done.stderr
    assert "CURIE_FACTORY_JIRA_ENV_FILE" in done.stderr


# Handoff (a): the real Preflight, the real write path, then check-clean.


def _preflight(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> fe.Preflight:
    environ = _environ(tmp_path)
    monkeypatch.setenv("HOME", HOME)
    monkeypatch.setenv("CURIE_BIN", BINARY)
    config_env = {
        "CURIE_FACTORY_KUBE_CONTEXT": CONTEXT,
        "CURIE_FACTORY_APP_DIR": environ["CURIE_FACTORY_APP_DIR"],
        "CURIE_FACTORY_REPO": REPO,
        "CURIE_FACTORY_ACTOR_TOKEN": "actor-token",
        "CURIE_FACTORY_LAYER_REGISTRY": REGISTRY,
        "CURIE_FACTORY_OPERATOR_LOGIN": OPERATOR,
    }
    config = fe.load_config(config_env, context=None)
    config = fe.dataclasses.replace(config, curie_bin=BINARY)
    p = fe.Preflight(
        config,
        repo_root=REPO_ROOT,
        candidate=SHA,
        namespace="test-factory-unit",
        evidence_path=tmp_path / "evidence" / "one-ticket-live.json",
        admission_timeout=1,
        scenario_name="one-ticket",
    )
    return p


def _run_path_evidence(p: fe.Preflight) -> None:
    """What the real run path writes into the evidence before it is serialized."""

    p.evidence["actor_login"] = ACTOR  # check_app
    p.evidence["fixture_repository_id"] = REPOSITORY_ID  # check_app
    p.step("App JWT and actor token verified", actor_login=ACTOR)
    p.evidence["jira"] = {"project_key": PROJECT, "binding": "jira"}  # _run_quickstart
    p.evidence["jira_issue_key"] = ISSUE_KEY  # open_and_label_ticket
    p.step("Jira issue opened and labelled by the labeler", key=ISSUE_KEY)
    p.step("labeller account", start_account=LABELLER_ACCOUNT, email=LABELLER_EMAIL)


def _install_like_before_run(p: fe.Preflight, tmp_path: Path) -> None:
    first = otl.config_identities(p)
    loaded = otl.load_identities(
        _environ(tmp_path), fetch_myself=_fetch_myself, github_login=_no_login
    )
    otl.install_sanitizer(p, otl._merged(first, loaded))
    p.repository_id = REPOSITORY_ID
    otl.add_run_identities(p)


def _check_clean(path: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> tuple[int, str]:
    code = otl.main(
        ["check-clean", str(path)],
        environ=_environ(tmp_path),
        fetch_myself=_fetch_myself,
        github_login=_no_login,
    )
    return code, capsys.readouterr().out


def test_anonymize_handoff_real_preflight_evidence_is_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    p = _preflight(tmp_path, monkeypatch)
    try:
        _install_like_before_run(p, tmp_path)
        _run_path_evidence(p)
        capsys.readouterr()
        p.write_evidence()
        assert p.evidence_refused is False
        written = json.loads(p.evidence_path.read_text())
        assert written["jira_issue_key"] == "project-12"
        assert written["kube_context"] == "context"
        assert written["fixture_repository_id"] == "fixture-repo"
        assert _check_clean(p.evidence_path, tmp_path, capsys) == (0, "clean\n")
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def test_anonymize_handoff_plain_json_names_the_labeller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    p = _preflight(tmp_path, monkeypatch)
    try:
        _run_path_evidence(p)
        p.evidence_serializer = None
        p.write_evidence()
        code, out = _check_clean(p.evidence_path, tmp_path, capsys)
        assert code == 1
        assert "labeller" in out.removeprefix("identities found: ").strip().split(", ")
        assert LABELLER_ACCOUNT not in out
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def test_anonymize_handoff_a_serializer_that_misses_a_value_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _preflight(tmp_path, monkeypatch)
    real = otl.anonymize

    def misses_the_labeller(value: Any, identities: Mapping[str, Any]) -> Any:
        return real(value, {k: v for k, v in identities.items() if k != "labeller"})

    try:
        _install_like_before_run(p, tmp_path)
        _run_path_evidence(p)
        monkeypatch.setattr(otl, "anonymize", misses_the_labeller)
        p.write_evidence()
        assert p.evidence_refused is True
        text = p.evidence_path.read_text()
        assert LABELLER_ACCOUNT not in text and LABELLER_EMAIL not in text
        assert "labeller" in json.loads(text)["error"]
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def test_anonymize_finish_hook_adds_the_run_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _preflight(tmp_path, monkeypatch)
    try:
        otl.install_sanitizer(p, otl.config_identities(p))
        p.repository_id = REPOSITORY_ID
        p.evidence["actor_login"] = ACTOR
        assert otl.finish(p, []) is True
        ids = p.scenario_options["identities"]
        assert REPOSITORY_ID in ids["fixture-repo"] and ACTOR in ids["actor"]
        assert fe.filtered(f"{ACTOR} {REPOSITORY_ID}") == "actor fixture-repo"
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


# --- the Jira driver's additions ----------------------------------------------------------


class _FakeJira:
    """The few Jira Cloud REST v3 routes the one-ticket driver adds, as fe.http_json."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.search_status = 200
        self.search_issues: list[dict[str, Any]] = []
        self.label_status = 204

    def __call__(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        timeout: float = 30,
    ) -> tuple[int, Any]:
        path = url.split("/rest/api/3", 1)[1]
        auth = (headers or {}).get("Authorization", "")
        email = base64.b64decode(auth.removeprefix("Basic ")).decode().split(":", 1)[0]
        self.calls.append({"method": method, "path": path, "as": email, "body": body})
        route = path.split("?", 1)[0]
        if method == "POST" and route == "/issue":
            return 201, {"id": "10042", "key": ISSUE_KEY}
        if method == "PUT" and route == "/issue/10042":
            return self.label_status, None
        if method == "GET" and route == "/issue/10042":
            return 200, _issue(STATUS_ID, "To Do", ["curie-factory"])
        if method == "POST" and route == "/issue/10042/comment":
            return 201, {"id": "10200"}
        if method == "GET" and route == f"/issue/{ISSUE_KEY}/comment":
            listed = [
                {"id": c["id"], "author": {"accountId": BOT_ACCOUNT}}
                for c in _comment_page()["values"]
            ] + [{"id": "10300", "author": {"accountId": LABELLER_ACCOUNT}}]
            return 200, {"startAt": 0, "maxResults": 100, "total": 4, "comments": listed}
        if method == "POST" and route == "/comment/list":
            assert "expand=properties" in path
            return 200, _comment_page()
        if method == "POST" and route == "/search/jql":
            if self.search_status != 200:
                return self.search_status, {"errorMessages": ["bad"]}
            return 200, {"issues": self.search_issues, "isLast": True}
        raise AssertionError(f"unexpected Jira call {method} {path}")


def _driver(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Any, _FakeJira, fe.Preflight]:
    p = _preflight(tmp_path, monkeypatch)
    environ = _environ(tmp_path)
    env_file = Path(environ["CURIE_FACTORY_JIRA_ENV_FILE"])
    monkeypatch.setenv("CURIE_FACTORY_JIRA_ENV_FILE", str(env_file))
    fake = _FakeJira()
    monkeypatch.setattr(fe, "http_json", fake)
    fe.load_forge_entries()
    module = sys.modules["factory_e2e_forges.jira_cloud"]
    return module.JiraCloudDriver(p, github=object()), fake, p


def test_driver_before_label_runs_after_create_and_before_the_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, fake, p = _driver(tmp_path, monkeypatch)
    items = {"items": [{"id": "wi-1", "tracker": {"kind": "jira_cloud", "issue_id": "10042"}}]}
    p.api = lambda *_a, **_k: (200, items)  # type: ignore[method-assign]
    seen: list[tuple[str, str, int]] = []

    def before_label(issue_id: str, key: str) -> None:
        seen.append((issue_id, key, len(fake.calls)))

    try:
        assert driver.open_and_label_ticket("t", "b", 0.0, before_label=before_label) == "wi-1"
        assert seen == [("10042", ISSUE_KEY, 1)]
        assert [c["method"] for c in fake.calls] == ["POST", "PUT"]
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def test_driver_reads_and_writes_the_one_ticket_scenario_adds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, fake, p = _driver(tmp_path, monkeypatch)
    try:
        status, issue = driver.issue_fields("10042")
        assert status == 200 and otl.status_id(issue) == STATUS_ID
        assert fake.calls[-1]["path"] == "/issue/10042?fields=status,labels"
        assert fake.calls[-1]["as"] == LABELLER_EMAIL

        assert driver.post_comment("10042", "Round half up.")[0] == 201
        assert fake.calls[-1]["body"] == {
            "body": {
                "type": "doc",
                "version": 1,
                "content": [
                    {"type": "paragraph", "content": [{"type": "text", "text": "Round half up."}]}
                ],
            }
        }
        assert fake.calls[-1]["as"] == LABELLER_EMAIL

        comments = driver.bot_comments(ISSUE_KEY)
        assert [otl.comment_marker(c) for c in comments][1].startswith("curie-factory-questions:")
        listed = [c for c in fake.calls if c["path"].startswith("/comment/list")][-1]
        assert listed["body"] == {"ids": [10100, 10101, 10102]}

        assert driver.factory_created_issues(fe.time.time() - 125) == 0
        search = fake.calls[-1]
        assert search["path"] == "/search/jql"
        assert search["body"] == {
            "jql": f'project = "{PROJECT}" AND creator = "{BOT_ACCOUNT}" AND created >= -4m',
            "fields": ["created"],
            "maxResults": 100,
        }
        fake.search_status = 400
        assert driver.factory_created_issues(fe.time.time()) is None
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def _search_hit(issue_id: str, created: str) -> dict[str, Any]:
    """One ``issues`` entry of ``POST /rest/api/3/search/jql`` with ``fields: ["created"]``."""

    return {
        "expand": "renderedFields,names,schema,operations,editmeta,changelog",
        "id": issue_id,
        "self": f"https://api.atlassian.com/ex/jira/x/rest/api/3/issue/{issue_id}",
        "key": f"{PROJECT}-{issue_id[-2:]}",
        "fields": {"created": created},
    }


def test_driver_factory_created_issues_counts_only_issues_created_since_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, fake, p = _driver(tmp_path, monkeypatch)
    since = datetime(2026, 11, 20, 8, 30, tzinfo=UTC).timestamp()
    try:
        fake.search_issues = [
            # 08:15:30Z, inside the rounded JQL window but before the run.
            _search_hit("10040", "2026-11-20T08:15:30.123+0000"),
            # 04:45:10-0400 is 08:45:10Z: after the run once the offset is applied.
            _search_hit("10041", "2026-11-20T04:45:10.000-0400"),
        ]
        assert driver.factory_created_issues(since) == 1
        # Exactly at the start counts; an unparseable or missing created counts.
        fake.search_issues = [
            _search_hit("10042", "2026-11-20T08:30:00.000+0000"),
            _search_hit("10043", "yesterday"),
            {"id": "10044", "key": f"{PROJECT}-44", "fields": {}},
        ]
        assert driver.factory_created_issues(since) == 3
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def test_anonymize_issue_id_is_an_identity_before_the_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver, fake, p = _driver(tmp_path, monkeypatch)
    ids = otl.load_identities(
        _environ(tmp_path), fetch_myself=_fetch_myself, github_login=_no_login
    )
    otl.install_sanitizer(p, ids)
    monkeypatch.setattr(p, "loop_driver", lambda: driver)
    fake.label_status = 500
    try:
        run = otl._LiveRun(p, clock=lambda: 1000.0, sleep=_no_sleep)
        with pytest.raises(fe.PreflightFailed) as refused:
            run.open_ticket()
        # The driver's own label failure names the numeric issue id.
        assert "10042" in str(refused.value)
        shown = fe.filtered(str(refused.value))
        assert "10042" not in shown and "issue" in shown, shown
        assert p.scenario_options["identities"]["issue"] == ["10042"]
        assert p.scenario_options["identities"]["status"] == [STATUS_ID]
    finally:
        fe.shutil.rmtree(p.workdir, ignore_errors=True)


def test_anonymize_load_identities_names_the_restore_url(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    url = "https://hooks.acme.example/restore"
    environ = {**_environ(tmp_path), "CURIE_FACTORY_WEBHOOK_RESTORE_URL": url}
    ids = otl.load_identities(environ, fetch_myself=_fetch_myself, github_login=_no_login)
    assert ids["restore-url"] == [url]
    target = tmp_path / "evidence.json"
    target.write_text(json.dumps({"restore": url}))
    code = otl.main(
        ["check-clean", str(target)],
        environ=environ,
        fetch_myself=_fetch_myself,
        github_login=_no_login,
    )
    assert (code, capsys.readouterr().out) == (1, "identities found: restore-url\n")


# --- drive end to end against fakes, on an injected clock ---------------------------------


def _no_sleep(_seconds: float) -> None:
    raise AssertionError("this beat must not wait")


class _Clock:
    """Time that moves only when the driver sleeps."""

    def __init__(self) -> None:
        self.now = 1_790_000_000.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat()


REQUEST_1 = "11111111-1111-4111-8111-111111111111"
REQUEST_2 = "22222222-2222-4222-8222-222222222222"


class _World:
    """The factory and Jira as the driver sees them, scripted against the clock.

    ``final_read`` picks what every work item read after the first settled one
    returns: ``"ok"`` (the real detail), ``"unreadable"`` (None) or ``"live"``
    (a queued request). ``second_run_ends`` False keeps the answered run running.
    """

    forge = "jira_cloud"
    pairing = "jira_cloud+github"

    def __init__(self, clock: _Clock, *, final_read: str = "ok", second_run_ends: bool = True):
        self.clock = clock
        self.final_read = final_read
        self.second_run_ends = second_run_ends
        self.labelled: float | None = None
        self.answered: float | None = None
        self.merged: float | None = None
        self.settled_reads = 0
        self.posted: list[tuple[str, str]] = []
        self.merges: list[Any] = []

    def since(self, at: float | None, seconds: float) -> bool:
        return at is not None and self.clock() >= at + seconds

    # -- the driver --

    def open_and_label_ticket(
        self, title: str, body: str, since: float, *, before_label: Any = None
    ) -> str:
        assert title and body
        before_label("10042", ISSUE_KEY)
        self.labelled = self.clock()
        return "wi-1"

    def pin_fixture_base(self) -> None:
        pass

    def issue_fields(self, issue_id: str) -> tuple[int, dict[str, Any]]:
        assert issue_id == "10042"
        labels = ["curie-factory"]
        if self.since(self.labelled, 405) and self.answered is None:
            labels.append(otl.QUESTIONS_LABEL)
        return 200, _issue(STATUS_ID, "To Do", labels)

    def bot_comments(self, key: str) -> list[dict[str, Any]]:
        assert key == ISSUE_KEY
        values = []
        if self.since(self.labelled, 20):
            values.append(
                _comment(
                    "10100",
                    f"curie-execution-request:{REQUEST_1}",
                    _paragraph(_text("Investigating this ticket. The factory will open a pull ")),
                )
            )
        if self.since(self.labelled, 400):
            values.append(
                _comment(
                    "10101",
                    f"curie-factory-questions:{REQUEST_1}",
                    _paragraph(_text("Which rounding mode, and what default precision?")),
                )
            )
        if self.since(self.answered, 15):
            blocks = [
                _paragraph(_text("Answer received. The factory is working on this ticket again."))
            ]
            if self.since(self.merged, 30):
                blocks += [
                    _paragraph(_text(USAGE_AT_LEAST)),
                    _paragraph(_text("Pull request merged: "), _text(PR_URL, href=PR_URL)),
                ]
            values.append(_comment("10102", f"curie-execution-request:{REQUEST_2}", *blocks))
        return values

    def post_comment(self, issue_id: str, text: str) -> tuple[int, Any]:
        self.posted.append((issue_id, text))
        self.answered = self.clock()
        return 201, {"id": "10200"}

    def changes_opened(self) -> list[int]:
        return [7] if self.since(self.answered, 1100) else []

    def merge(self, change: Any) -> dict[str, Any]:
        self.merges.append(change)
        self.merged = self.clock()
        return {"merged": True}

    def factory_created_issues(self, since_epoch: float) -> int:
        return 0

    # -- the platform --

    def detail(self) -> dict[str, Any] | None:
        assert self.labelled is not None
        first = "completed" if self.since(self.labelled, 405) else "running"
        requests = [{"sequence": 1, "status": first, "created_at": _iso(self.labelled)}]
        if self.answered is None and first == "completed":
            self.settled_reads += 1
            if self.settled_reads > 1 and self.final_read == "unreadable":
                return None
            if self.settled_reads > 1 and self.final_read == "live":
                requests.append({"sequence": 2, "status": "queued", "created_at": _iso(0)})
        if self.since(self.answered, 8):
            assert self.answered is not None
            ended = self.second_run_ends and self.since(self.answered, 1200)
            requests.append(
                {
                    "sequence": 2,
                    "status": "completed" if ended else "running",
                    "created_at": _iso(self.answered + 8),
                }
            )
        detail: dict[str, Any] = {"id": "wi-1", "requests": requests}
        if self.since(self.answered, 1100):
            detail["pr"] = {"number": 7, "url": "https://github.com/acme/fixture/pull/7"}
        return detail

    def pull(self) -> dict[str, Any]:
        assert self.answered is not None
        return {
            "number": 7,
            "created_at": _iso(self.answered + 1100),
            "body": (
                f"Adds rounding.\n\nJira: {ISSUE_KEY}\n\n"
                f"Answered on the ticket by {LABELLER_NAME}: round half up."
            ),
        }


class _FakePreflight:
    """The Preflight surface ``drive`` reads."""

    def __init__(self, world: _World, ids: Any) -> None:
        self.world = world
        self.config = fe.dataclasses.make_dataclass("Config", ["repo"])(REPO)
        self.evidence: dict[str, Any] = {"actor_login": ACTOR}
        self.scenario_options: dict[str, Any] = {
            "answer_text": "Round half up, two decimal places by default.",
            "identities": ids,
        }
        self.issue_spec = ("Add a rounding option", "The rounding mode is not decided here.")
        self.labelled_at = 0.0
        self.repository_id = REPOSITORY_ID
        self.usage_before = 10.0
        self.evidence_serializer: Any = None
        self.detail_reads = 0

    def loop_driver(self) -> _World:
        return self.world

    def work_item_detail(self, work_item_id: str) -> dict[str, Any] | None:
        assert work_item_id == "wi-1"
        self.detail_reads += 1
        return self.world.detail()

    def as_actor(self, method: str, path: str) -> tuple[int, Any]:
        assert (method, path) == ("GET", f"/repos/{REPO}/pulls/7")
        return 200, self.world.pull()

    def model_usage(self) -> float:
        return 14.25


def _drive(tmp_path: Path, **world: Any) -> tuple[_FakePreflight, _World, _Clock]:
    clock = _Clock()
    w = _World(clock, **world)
    p = _FakePreflight(w, _identities(tmp_path))
    try:
        otl.drive(p, clock=clock, sleep=clock.sleep)
    except fe.PreflightFailed:
        pass
    return p, w, clock


def test_judge_drive_end_to_end_passes_on_the_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fe, "_await_ending", _no_await_ending)
    p, world, _clock = _drive(tmp_path)
    obs = p.evidence["scenario"]
    assert obs["verdict"] == "passed", obs.get("failures")
    assert otl.judge(obs) == []
    assert obs["pre_answer_read"] == "ok" and obs["answered_run_ended"] is True
    assert world.posted == [("10042", "Round half up, two decimal places by default.")]
    assert len(world.merges) == 1
    assert obs["usage"] == {"kind": "at_least", "total": "3.10"}
    assert obs["request_count"] == 2 and obs["status_id_unchanged"] is True
    # The run's identities, the issue id among them, are in the sanitizer.
    assert p.scenario_options["identities"]["issue"] == ["10042"]
    text = p.evidence_serializer(p.evidence)
    assert otl.find_identities(text, p.scenario_options["identities"]) == []


def _no_await_ending(*_a: Any, **_k: Any) -> tuple[dict[str, Any], bool]:
    """The harness wait gives up; the driver must not merge on it (its deadline is hours)."""

    return {}, False


@pytest.mark.parametrize(
    ("final_read", "message"),
    [
        ("unreadable", "the work item could not be read immediately before the answer"),
        ("live", "a request was live between the settlement and the answer: queued"),
    ],
)
def test_judge_drive_never_answers_when_the_final_read_is_not_clean(
    final_read: str, message: str, tmp_path: Path
) -> None:
    p, world, _clock = _drive(tmp_path, final_read=final_read)
    obs = p.evidence["scenario"]
    assert world.posted == [] and world.merges == []
    assert obs["answer_s"] is None and obs["settled"] is True
    assert obs["verdict"] == "failed"
    assert message in obs["failures"], obs["failures"]
    if final_read == "unreadable":
        assert obs["live_before_answer"] == []
        # The settled read, the bounded retries, then the request count read.
        assert world.settled_reads == 1 + otl.PRE_ANSWER_READ_ATTEMPTS + 1


def test_judge_drive_does_not_merge_a_run_that_never_ends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(fe, "_await_ending", _no_await_ending)
    p, world, clock = _drive(tmp_path, second_run_ends=False)
    obs = p.evidence["scenario"]
    assert world.merges == []
    assert obs["answered_run_ended"] is False and obs["merge_ok"] is False
    assert (
        f"the answered run did not end within {otl.ENDING_SECONDS} s of the pull request; "
        "not merged"
    ) in obs["failures"]
    assert world.answered is not None
    # The wait is the module's own bound after the pull request, not the harness's.
    assert clock.now <= world.answered + 1100 + otl.ENDING_SECONDS + 2 * otl.POLL_SECONDS
