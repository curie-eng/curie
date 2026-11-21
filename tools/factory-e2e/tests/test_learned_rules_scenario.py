"""Offline proof of the scripted ``learned-rules`` scenario (#5799).

Criterion 1: the judge refuses evidence missing any step, steps out of order,
and each named observation violated; its readers parse what the runner and API
producers really write. Criterion 9: the session probe runs inside
``ScriptedRun.execute`` after ``deploy`` and before ticket A's label, and each
wrong session stops the run with no model request served or proxied and ticket
A unlabelled. Criterion 8a end: a revision's real objective on a pull request
the factory opened on the real GitHub stub routes to the ticket it closes.
"""

from __future__ import annotations

import json
import ssl
import subprocess
import urllib.parse
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import factory_e2e as fe
import learned_rules_lanes as lrl
import learned_rules_run as lrr
import learned_rules_scenario as lrs
import personal_two_owner_support as pto
import pytest
import scripted_kind as sk
from curie_api import memory_rules_context as api_rules
from curie_api import requester_instruction_context as api_requester
from curie_api.factory_reply_target import feedback_url
from curie_api.github_factory_review import _objective
from curie_api.github_review_events import UnverifiedFeedback
from curie_api.github_review_store import feedback_provenance
from curie_runner.memory_rules_context import rules_section
from curie_runner.requester_context import requester_section
from curie_runner.sender_frame import frame_user_turn

REPO_ROOT = Path(__file__).resolve().parents[3]
RECORDING = REPO_ROOT / "tools/github-stub/recordings/curie-pr-3400.json"
SEED_TREE = REPO_ROOT / "tools/factory-e2e/fixtures/unitconv"
REPO = "acme-corp/acme-bot"
DIGEST = "d" * 64
REQUESTER_DIGEST = "e" * 64
REPLY_URL = "https://stub.example.test/acme-corp/acme-bot/pull/7#issuecomment-41"
RULE_ID = "11111111-2222-4333-8444-555555555555"
TENANT = "aaaaaaaa-0000-4000-8000-000000000001"
WRITER = "aaaaaaaa-0000-4000-8000-0000000000a1"
READER = "aaaaaaaa-0000-4000-8000-0000000000a2"


# A passing record -------------------------------------------------------------


def _capture(sequence: int, issue: int, *, implementer: bool, section: bool) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "issue": issue,
        "ordinal": 0,
        "implementer": implementer,
        "mounted": False,
        "open": 100 if section else None,
        "close": 900 if section else None,
        "requester": 1000 if implementer else None,
        "rule_line": section,
        "requester_text": implementer and issue == lrs.TICKET_B,
        "system_chars": 5000,
    }


_G_ISSUES = (
    *((n, lrs.TICKET_A) for n in range(1, 11)),
    *((n, lrs.TICKET_B) for n in range(11, 14)),
    *((n, lrs.TICKET_C) for n in range(14, 16)),
)


def _g_capture(sequence: int, issue: int) -> dict[str, Any]:
    return {"sequence": sequence, "issue": issue, "ordinal": sequence, "key": f"key-{sequence}"}


def _served(sequence: int, issue: int) -> dict[str, Any]:
    letter = next(k for k, v in lrs.TICKETS.items() if v == issue)
    return {"lane": lrs.LANES[letter], "ordinal": sequence, "key": f"key-{sequence}"}


def _passing() -> dict[str, Any]:
    requester_line = f"Requester preferences: applied; sha256:{REQUESTER_DIGEST}"
    return {
        "steps": [
            {
                "step": "0",
                "session_kinds": ["principal", "principal"],
                "principal_ids": [WRITER, READER],
                "principal_tenants": [TENANT, TENANT],
                "agent_tenant": TENANT,
                "model_requests": {"captured": 0, "served": 0, "unexpected": 0, "proxied": 0},
            },
            {
                "step": "a",
                "memory_rule_proposals": True,
                "labelled": True,
                "pull_request": 7,
                "closes": [lrs.TICKET_A],
            },
            {
                "step": "b",
                "comment": {
                    "id": 41,
                    "pull_request": 7,
                    "author": lrs.WRITER_LOGIN,
                    "author_association": "COLLABORATOR",
                    "body": (lrr.REVIEW_COMMENT.read_text()).strip(),
                },
                "revision": {
                    "request_id": "r2",
                    "status": "completed",
                    "reply_target_kind": "pull_request",
                    "reply_target_url": REPLY_URL,
                    "final_notice": True,
                },
                "propose_ordinals": [3],
                "revision_captures": [
                    {"sequence": 9, "issue": lrs.TICKET_A, "ordinal": 3, "mounted": True},
                    {"sequence": 10, "issue": lrs.TICKET_A, "ordinal": 4, "mounted": True},
                ],
                "rules_before_merge": [],
            },
            {
                "step": "c",
                "merged": True,
                "source_matches_reply_target": True,
                "rules": [
                    {
                        "id": RULE_ID,
                        "label": "R1",
                        "state": "proposed",
                        # Created as a candidate at 1; the promotion bumped it.
                        "version": 2,
                        "source_url": REPLY_URL,
                        "text": lrs.RULE_TEXT,
                    }
                ],
            },
            {
                "step": "d",
                "rule_id": RULE_ID,
                "expected_version": 2,
                "reader": {"exit": 1, "codes": [lrs.WRITE_ACCESS_REQUIRED]},
                "writer": {"exit": 0, "codes": [], "state": "accepted", "version": 3},
            },
            {
                "step": "e",
                "coding_instructions_exit": 0,
                "labeller": lrs.WRITER_LOGIN,
                "pull_request": 8,
                "closes": [lrs.TICKET_B],
                "work_item_memory_rules": {"state": "selected", "count": 1, "sha256": DIGEST},
                "window": [11, 13],
                "captures": [
                    _capture(11, lrs.TICKET_B, implementer=True, section=True),
                    _capture(12, lrs.TICKET_B, implementer=True, section=True),
                    _capture(13, lrs.TICKET_B, implementer=False, section=False),
                ],
                "annotations": [
                    {"index": 6, "line": f"Learned rules: in force; rules:R1; sha256:{DIGEST}"},
                    {"index": 7, "line": requester_line},
                ],
                "added_asserts": [
                    "self.assertAlmostEqual(convert(0, 'f', 'r'), 459.67, places=6)",
                    "self.assertAlmostEqual(convert(491.67, 'r', 'c'), 0.0, places=6)",
                ],
            },
            {
                "step": "f",
                "remove_exit": 0,
                "expected_version": 3,
                "state_after": "removed",
                "labeller": lrs.WRITER_LOGIN,
                "pull_request": 9,
                "closes": [lrs.TICKET_C],
                "work_item_memory_rules": {"state": "absent", "count": 0},
                "window": [14, 15],
                "captures": [
                    _capture(14, lrs.TICKET_C, implementer=True, section=False),
                    _capture(15, lrs.TICKET_C, implementer=False, section=False),
                ],
                "annotations": [{"index": 4, "line": requester_line}],
                "added_asserts": [],
            },
            {
                "step": "g",
                "record": False,
                "lanes": {
                    **{
                        name: {"issue": lrs.TICKETS[letter], "complete": True}
                        for letter, name in lrs.LANES.items()
                    },
                    "unrouted": [],
                },
                "captures": [_g_capture(n, issue) for n, issue in _G_ISSUES],
                "served": [_served(n, issue) for n, issue in _G_ISSUES],
            },
        ]
    }


def _step(evidence: dict[str, Any], name: str) -> dict[str, Any]:
    return next(step for step in evidence["steps"] if step["step"] == name)


def test_the_passing_record_passes() -> None:
    lrs.assert_chain(_passing(), record=False)


def test_a_recorded_run_passes_on_its_per_ticket_counts() -> None:
    evidence = _passing()
    step = _step(evidence, "g")
    del step["lanes"]
    step.update(record=True, recorded_counts={"a": 12, "b": 7, "c": 5}, untagged=0)
    lrs.assert_chain(evidence, record=True)
    with pytest.raises(lrs.LearnedRulesAssertionError, match="other mode"):
        lrs.assert_chain(evidence, record=False)
    step["recorded_counts"]["c"] = 0
    with pytest.raises(lrs.LearnedRulesAssertionError, match="ticket c recorded no exchange"):
        lrs.assert_chain(evidence, record=True)


@pytest.mark.parametrize("missing", lrs.STEPS)
def test_a_missing_step_is_refused(missing: str) -> None:
    evidence = _passing()
    evidence["steps"] = [step for step in evidence["steps"] if step["step"] != missing]
    with pytest.raises(lrs.LearnedRulesAssertionError, match=f"steps missing: \\['{missing}'\\]"):
        lrs.assert_chain(evidence, record=False)


@pytest.mark.parametrize("first", range(len(lrs.STEPS) - 1))
def test_steps_out_of_order_are_refused(first: int) -> None:
    evidence = _passing()
    steps = evidence["steps"]
    steps[first], steps[first + 1] = steps[first + 1], steps[first]
    with pytest.raises(lrs.LearnedRulesAssertionError, match="steps out of order"):
        lrs.assert_chain(evidence, record=False)


def _set(step: str, *path: Any, value: Any) -> Callable[[dict[str, Any]], None]:
    def mutate(evidence: dict[str, Any]) -> None:
        target: Any = _step(evidence, step)
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return mutate


def _section_after_requester(evidence: dict[str, Any]) -> None:
    capture = _step(evidence, "e")["captures"][0]
    capture["close"], capture["requester"] = 1000, 900


def _learned_line_not_adjacent(evidence: dict[str, Any]) -> None:
    _step(evidence, "e")["annotations"][1]["index"] = 9


def _requester_line_first(evidence: dict[str, Any]) -> None:
    lines = _step(evidence, "e")["annotations"]
    lines[0]["index"], lines[1]["index"] = 7, 6


def _bare_assert(evidence: dict[str, Any]) -> None:
    _step(evidence, "e")["added_asserts"].append(
        "self.assertAlmostEqual(convert(1, 'r', 'k'), 0.5)"
    )


def _c_carries_section(evidence: dict[str, Any]) -> None:
    _step(evidence, "f")["captures"][0].update(open=100, close=900)


def _c_carries_line(evidence: dict[str, Any]) -> None:
    _step(evidence, "f")["annotations"].insert(
        0, {"index": 3, "line": f"Learned rules: in force; rules:R1; sha256:{DIGEST}"}
    )


def _capture_on_wrong_ticket(evidence: dict[str, Any]) -> None:
    _step(evidence, "g")["captures"][11]["issue"] = lrs.TICKET_C


def _ticket_capture_outside_its_run(evidence: dict[str, Any]) -> None:
    _step(evidence, "g")["captures"][0]["issue"] = lrs.TICKET_B


def _capture_on_no_ticket(evidence: dict[str, Any]) -> None:
    _step(evidence, "g")["captures"][0]["issue"] = None


VIOLATIONS: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
    "operator session": (
        _set("0", "session_kinds", value=["principal", "operator"]),
        "session kinds",
    ),
    "one principal": (_set("0", "principal_ids", value=[WRITER, WRITER]), "one principal"),
    "foreign tenant": (
        _set("0", "principal_tenants", value=[TENANT, READER]),
        "outside the agent's tenant",
    ),
    "model request before the probe": (
        _set("0", "model_requests", "served", value=1),
        "before the probe",
    ),
    "proposals off": (_set("a", "memory_rule_proposals", value=False), "proposals on"),
    "A unlabelled": (_set("a", "labelled", value=False), "not labelled"),
    "A closes another ticket": (_set("a", "closes", value=[3921]), "closes another"),
    "comment without mention": (
        _set("b", "comment", "body", value=lrs.RULE_TEXT),
        "does not mention",
    ),
    "comment by another author": (_set("b", "comment", "author", value="octocat"), "writer's"),
    "comment on another PR": (_set("b", "comment", "pull_request", value=8), "A's pull request"),
    "revision not completed": (_set("b", "revision", "status", value="failed"), "complete"),
    "no propose_rule served": (_set("b", "propose_ordinals", value=[]), "no propose_rule"),
    "tool not mounted": (
        _set("b", "revision_captures", 1, "mounted", value=False),
        "tool mounted",
    ),
    "rules listed before merge": (
        _set("b", "rules_before_merge", value=[{"id": RULE_ID, "state": "proposed"}]),
        "before the merge",
    ),
    "not merged": (_set("c", "merged", value=False), "not merged"),
    "two rules": (
        lambda e: _step(e, "c")["rules"].append(dict(_step(e, "c")["rules"][0])),
        "expected one rule",
    ),
    "rule not proposed": (_set("c", "rules", 0, "state", value="candidate"), "not proposed"),
    "rule not R1": (_set("c", "rules", 0, "label", value="R2"), "not R1"),
    "source_url on another PR": (
        lambda e: [
            _step(e, "c")["rules"][0].update(source_url=REPLY_URL.replace("/7#", "/8#")),
            _step(e, "b")["revision"].update(reply_target_url=REPLY_URL.replace("/7#", "/8#")),
        ],
        "not A's pull request",
    ),
    "source_url not the reply target": (
        _set("c", "rules", 0, "source_url", value=REPLY_URL + "0"),
        "reply target",
    ),
    "reader accept exit 0": (_set("d", "reader", "exit", value=0), "reader's accept exited 0"),
    "reader refusal unnamed": (_set("d", "reader", "codes", value=[]), "does not name"),
    "writer accept failed": (_set("d", "writer", "exit", value=1), "writer's accept"),
    "rule not accepted": (_set("d", "writer", "state", value="proposed"), "accepted"),
    "raw source differs from the raw reply target": (
        _set("c", "source_matches_reply_target", value=False),
        "unscrubbed source",
    ),
    "no reviewer prompt of B": (
        _set(
            "e",
            "captures",
            value=[_capture(11, lrs.TICKET_B, implementer=True, section=True)],
        ),
        "no reviewer prompt of B",
    ),
    "places=60 passing as six": (
        _set(
            "e",
            "added_asserts",
            value=["self.assertAlmostEqual(convert(0, 'f', 'r'), 459.67, places=60)"],
        ),
        "without places=6",
    ),
    "decision on a fixed version": (_set("d", "expected_version", value=1), "proposed rule's"),
    "accept not bumping the version": (_set("d", "writer", "version", value=2), "move by one"),
    "proposed rule without a version": (_set("c", "rules", 0, "version", value=None), "version"),
    "requester document not set": (
        _set("e", "coding_instructions_exit", value=1),
        "requester document",
    ),
    "B labelled by another": (_set("e", "labeller", value="octocat"), "labelled by the writer"),
    "B rules not selected": (
        _set("e", "work_item_memory_rules", "state", value="absent"),
        "B's rules are absent",
    ),
    "missing section": (
        _set("e", "captures", 1, "open", value=None),
        "lacks the learned rules section",
    ),
    "missing rule line": (_set("e", "captures", 0, "rule_line", value=False), "R1: <rule text>"),
    "section after the requester section": (_section_after_requester, "does not end before"),
    "reviewer prompt carrying the section": (
        _set("e", "captures", 2, "open", value=100),
        "reviewer prompts carry",
    ),
    "no implementer prompt": (
        _set("e", "captures", value=[_capture(13, lrs.TICKET_B, implementer=False, section=False)]),
        "no implementer prompt",
    ),
    "digest mismatch": (
        _set("e", "work_item_memory_rules", "sha256", value="f" * 64),
        "work item's digest",
    ),
    "Learned line not immediately before Requester line": (
        _learned_line_not_adjacent,
        "immediately before",
    ),
    "Requester line before Learned line": (_requester_line_first, "immediately before"),
    "no added assertAlmostEqual": (_set("e", "added_asserts", value=[]), "adds no"),
    "assertAlmostEqual without places=6": (_bare_assert, "without places=6"),
    "removal failed": (_set("f", "remove_exit", value=1), "remove failed"),
    "remove on a stale version": (_set("f", "expected_version", value=2), "accepted rule's"),
    "rule not removed": (_set("f", "state_after", value="accepted"), "removed"),
    "C rules not absent": (
        _set("f", "work_item_memory_rules", "state", value="selected"),
        "not absent",
    ),
    "C carrying the section": (_c_carries_section, "C's prompts carry"),
    "C carrying the Learned line": (_c_carries_line, "C's pull request body"),
    "incomplete lane": (_set("g", "lanes", "learned-rules-b", "complete", value=False), "complete"),
    "absent lane": (
        lambda e: _step(e, "g")["lanes"].pop("learned-rules-c"),
        "learned-rules-c is absent",
    ),
    "non-empty unrouted": (_set("g", "lanes", "unrouted", value=[{"issue": 4000}]), "unrouted"),
    "capture of B on C's number": (_capture_on_wrong_ticket, "names #3922"),
    "capture naming B outside B's run": (_ticket_capture_outside_its_run, "outside its run"),
    "capture on no ticket": (_capture_on_no_ticket, "on no ticket"),
}


@pytest.mark.parametrize("name", list(VIOLATIONS))
def test_each_named_observation_is_refused(name: str) -> None:
    mutate, message = VIOLATIONS[name]
    evidence = _passing()
    mutate(evidence)
    with pytest.raises(lrs.LearnedRulesAssertionError, match=message):
        lrs.assert_chain(evidence, record=False)


# The readers on what the producers write --------------------------------------


def test_prompt_positions_read_the_runner_sections_in_their_order() -> None:
    rule = lrs.RULE_TEXT
    learned = rules_section(1, DIGEST, f"R1: {rule}")
    requester = requester_section(
        "sel-1", REQUESTER_DIGEST, "Use `assertEqual`.", learned_rules=True
    )
    prompt = "\n\n".join(("Bundle prompt.", "Model line.", learned, requester))
    found = lrs.prompt_positions(prompt, rule)
    assert found["open"] is not None and found["close"] is not None
    assert found["requester"] is not None and found["close"] < found["requester"]
    assert found["rule_line"] is True

    swapped = lrs.prompt_positions("\n\n".join(("Bundle prompt.", requester, learned)), rule)
    assert swapped["requester"] < swapped["close"]
    absent = lrs.prompt_positions("\n\n".join(("Bundle prompt.", requester)), rule)
    assert absent["open"] is None and absent["rule_line"] is False
    other = lrs.prompt_positions(prompt, "Use tabs.")
    assert other["rule_line"] is False


def test_annotation_lines_read_the_api_publication_lines() -> None:
    body = api_requester.annotate("Adds Rankine.\n\nCloses #3921", REQUESTER_DIGEST)
    body = api_rules.annotate(body, api_rules.AppliedRules(rule_numbers=(1,), sha256=DIGEST))
    lines = lrs.annotation_lines(body)
    step = _step(_passing(), "e")
    step["annotations"] = lines
    lrs.assert_ticket_b(step)
    without = lrs.annotation_lines(api_requester.annotate("Adds Delisle.", REQUESTER_DIGEST))
    assert [item["line"].split(":", 1)[0] for item in without] == ["Requester preferences"]


def test_added_assert_lines_read_only_added_lines() -> None:
    diff = "\n".join(
        (
            "--- a/unitconv/tests/test_convert.py",
            "+++ b/unitconv/tests/test_convert.py",
            "@@ -1,3 +1,5 @@",
            "         self.assertAlmostEqual(convert(100, 'c', 'f'), 212.0)",
            "-        self.assertAlmostEqual(convert(32, 'f', 'c'), 0.0)",
            "+        self.assertAlmostEqual(convert(32, 'f', 'c'), 0.0, places=6)",
            "+        self.assertEqual(convert(1, 'm', 'm'), 1)",
        )
    )
    assert lrs.added_assert_lines(diff) == [
        "self.assertAlmostEqual(convert(32, 'f', 'c'), 0.0, places=6)"
    ]


def test_a_call_with_whitespace_before_the_parenthesis_is_counted_and_read() -> None:
    spaced = "+        self.assertAlmostEqual (convert(32, 'f', 'c'), 0.0, places=6)"
    diff = "\n".join(("+++ b/t.py", spaced))
    assert lrs.added_assert_lines(diff) == [spaced[1:].strip()]
    assert lrs.places_six(spaced[1:].strip()) is True
    assert lrs.places_six("self.assertAlmostEqual (a, b, places=5)") is False


def test_propose_ordinals_find_the_tool_call_in_served_answers() -> None:
    exchanges = [
        {"lane_key": "k", "ordinal": 0, "message": {"content": [{"type": "text", "text": "x"}]}},
        {
            "lane_key": "k",
            "ordinal": 2,
            "message": {"content": [{"type": "tool_use", "name": lrs.PROPOSE_TOOL, "input": {}}]},
        },
    ]
    assert lrs.propose_ordinals(exchanges) == [2]


# Criterion 9 and the call order, through ScriptedRun.execute ------------------


class _Stop(RuntimeError):
    """Raised where the real run would wait for ticket A's final comment."""


AGENT_ID = "bbbbbbbb-0000-4000-8000-000000000001"
API_ORIGIN = "http://127.0.0.1:18000"
DEX_PORT = 15556


def _fake_opener(issuer: str, redirect_uri: str) -> lrr.Opener:
    """Dex and the API's login hops, with one session value per email."""

    def open_url(
        method: str, url: str, headers: Any, body: bytes | None
    ) -> tuple[int, list[tuple[str, str]], bytes]:
        parts = urllib.parse.urlsplit(url)
        if url == API_ORIGIN + pto.LOGIN_PATH:
            return (
                302,
                [
                    ("Location", f"{issuer}/auth?client_id=curie-console"),
                    ("Set-Cookie", f"{pto.STATE_COOKIE}=state-value; Path=/; HttpOnly"),
                ],
                b"",
            )
        if parts.port == DEX_PORT and method == "GET":
            form = '<form method="post" action="/dex/auth/local/login"><input name="password">'
            return 200, [], form.encode()
        if parts.port == DEX_PORT and method == "POST":
            login = urllib.parse.parse_qs((body or b"").decode())["login"][0]
            role = login.split("@", 1)[0]
            return 303, [("Location", f"{redirect_uri}?code={role}&state=s")], b""
        if parts.path == pto.CALLBACK_PATH:
            assert headers["Cookie"] == f"{pto.STATE_COOKIE}=state-value"
            role = urllib.parse.parse_qs(parts.query)["code"][0]
            return 303, [("Set-Cookie", f"{pto.SESSION_COOKIE}=session-{role}")], b""
        raise AssertionError(f"unexpected login hop {method} {url}")

    return open_url


class _Api:
    """The installed API's answers to the probe, links and the agent read."""

    def __init__(self, run: lrr.LearnedRulesRun, sessions: dict[str, dict[str, Any]]) -> None:
        self.run = run
        self.sessions = sessions
        self.calls: list[tuple[str, str, bool]] = []
        self.links: list[dict[str, Any]] = []

    def labelled(self) -> bool:
        return _labelled(self.run)

    def __call__(
        self, method: str, url: str, *, headers: dict[str, str], body: Any = None
    ) -> tuple[int, Any]:
        path = url.removeprefix(API_ORIGIN)
        self.calls.append((method, path, self.labelled()))
        cookie = headers.get("Cookie", "")
        role = cookie.split("=session-", 1)[1] if "=session-" in cookie else None
        if role is None:
            assert headers == {"X-API-Key": "platform-key"}
        if path == f"/agents/{AGENT_ID}/access-policy":
            return 200, {"agent_id": AGENT_ID, "tenant_id": TENANT}
        if path == "/console/session" and role is not None:
            return 200, {"kind": self.sessions[role]["kind"]}
        if path == "/console/principal" and role is not None:
            session = self.sessions[role]
            return 200, {"id": session["id"], "tenant_id": session["tenant_id"]}
        if path == f"/identity/admin/{TENANT}/code-host-links" and method == "POST":
            self.links.append(body)
            return 201, {"link_id": str(len(self.links))}
        if path == f"/agents/{AGENT_ID}":
            return 200, {"id": AGENT_ID, "memory_rule_proposals": True}
        raise AssertionError(f"unexpected API call {method} {path}")


def _transcripts(tmp_path: Path) -> Path:
    first = tmp_path / "transcripts" / "chain-a.json"
    first.parent.mkdir()
    for path in lrl.transcript_paths(first).values():
        path.write_text(json.dumps({"version": 1, "exchanges": []}))
    return first


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """A LearnedRulesRun whose cluster, registry, Helm, kubectl and CLI are faked,
    with the real GitHub stub and the real model endpoint on loopback."""

    # A local engine: the in-process servers bind the kind gateway the fake reports.
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "CURIE_LOCAL_STUB_HOST"):
        monkeypatch.delenv(name, raising=False)
    args = fe.parse_args(
        ["scripted", "--scenario", "learned-rules", "--transcript", str(_transcripts(tmp_path))]
    )
    run = lrr.LearnedRulesRun(args)
    timeline: list[str] = []
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.setattr(sk.tempfile, "mkdtemp", lambda **_kwargs: str(work))
    monkeypatch.setattr(sk.kind_images, "import_budget", lambda *_args: 0)
    monkeypatch.setattr(sk.kind_images, "load_images", lambda *_args: None)
    monkeypatch.setattr(run, "prerequisites", lambda: None)
    monkeypatch.setattr(run, "check_disk", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run, "start_registry", lambda: (5000, "localhost:5000"))

    def command(*argv: str, **_kwargs: Any) -> str:
        words = [Path(argv[0]).name, *argv[1:]]
        if words[:2] == ["docker", "inspect"]:
            return json.dumps(
                [{"NetworkSettings": {"Networks": {"kind": {"Gateway": "127.0.0.1"}}}}]
            )
        if words[:2] == ["docker", "run"]:
            return json.dumps({"signing": "s", "verify": "v", "sealing": "x"})
        if words[:2] == ["openssl", "genrsa"]:
            Path(argv[argv.index("-out") + 1]).write_text("key")
        if words[0] == "helm" and "install" in words:
            timeline.append("helm install")
        if words[0] == "kubectl" and "apply" in words and "dex.json" in words[-1]:
            timeline.append("dex applied")
        if words[0] == "kubectl" and "networkpolicy" in words:
            return json.dumps(
                {"items": [{"metadata": {"namespace": "curie-pub"}, "spec": {"podSelector": {}}}]}
            )
        if words[0] == "kubectl" and "deployment/curie-worker" in words:
            return "curie-pub"
        if "overrides" in words:
            timeline.append("proposals on")
        return ""

    monkeypatch.setattr(run, "command", command)
    monkeypatch.setattr(run, "command_result", lambda *argv, **_kwargs: (0, ""))

    def deploy(_registry_ref: str) -> None:
        timeline.append("deploy")
        run.env.update(CURIE_API_URL=API_ORIGIN, CURIE_API_KEY="platform-key")
        run.evidence["agent_id"] = AGENT_ID

    def start_dex_forward() -> None:
        timeline.append("dex forward")
        run.dex_port = DEX_PORT

    def wait_for_final() -> None:
        values = json.loads((work / "values.json").read_text())
        run.evidence["installed_api_env"] = values["api"]["extraEnv"]
        timeline.append("wait for final")
        raise _Stop("ticket A is labelled")

    monkeypatch.setattr(run, "deploy", deploy)
    monkeypatch.setattr(run, "start_dex_forward", start_dex_forward)
    monkeypatch.setattr(run, "wait_for_final", wait_for_final)
    run.opener = _fake_opener(run.issuer, run.redirect_uri)
    try:
        yield run, timeline
    finally:
        run.cleanup()


def _sessions(**changes: dict[str, Any]) -> dict[str, dict[str, Any]]:
    sessions = {
        "writer": {"kind": "principal", "id": WRITER, "tenant_id": TENANT},
        "reader": {"kind": "principal", "id": READER, "tenant_id": TENANT},
    }
    for role, change in changes.items():
        sessions[role].update(change)
    return sessions


def _label_events(run: lrr.LearnedRulesRun) -> list[dict[str, Any]]:
    return [
        e
        for e in run.stub._rows("event")
        if e["issue_number"] == lrs.TICKET_A and e["event"] == "labeled"
    ]


def _labelled(run: lrr.LearnedRulesRun) -> bool:
    """Ticket A carries its label: the stub seeds one past label event, and
    ``execute`` clears the issue's labels until the install is ready."""

    issue = run.stub._get("issue", lrs.TICKET_A)
    labels = {label["name"] for label in issue["labels"]}
    return lrr.LABEL in labels or len(_label_events(run)) > 1


def test_execute_probes_after_deploy_and_before_ticket_a_is_labelled(harness: Any) -> None:
    run, timeline = harness
    api = _Api(run, _sessions())
    run.http = api
    with pytest.raises(_Stop):
        run.execute()

    assert timeline == [
        "dex applied",
        "helm install",
        "deploy",
        "dex forward",
        "proposals on",
        "wait for final",
    ]
    assert api.calls and not any(labelled for _method, _path, labelled in api.calls)
    assert [path for _method, path, _labelled in api.calls][:5] == [
        f"/agents/{AGENT_ID}/access-policy",
        "/console/session",
        "/console/principal",
        "/console/session",
        "/console/principal",
    ]
    assert _labelled(run) and len(_label_events(run)) == 2
    probe = run.chain["steps"][0]
    assert probe["step"] == "0"
    lrs.assert_probe(probe)
    assert [(link["principal_id"], link["account_id"]) for link in api.links] == [
        (WRITER, "6630"),
        (READER, "6631"),
    ]
    assert {link["host"] for link in api.links} == {f"127.0.0.1:{run.stub.port}"}
    for role in ("writer", "reader"):
        path = run.session_files[role]
        assert path.read_text() == f"session-{role}\n"
        assert path.stat().st_mode & 0o777 == 0o600
        assert f"session-{role}" in run.secrets
    env = {entry["name"]: entry for entry in run.evidence["installed_api_env"]}
    assert env["CURIE_OIDC_ISSUER"]["value"] == run.issuer
    assert env["CURIE_OIDC_CLIENT_SECRET"]["valueFrom"]["secretKeyRef"]["name"] == lrr.OIDC_SECRET
    assert env["CURIE_OIDC_ADMIT_ALL_AUTHENTICATED"]["value"] == "true"
    assert "session-writer" not in json.dumps(run.evidence)


@pytest.mark.parametrize(
    ("sessions", "message"),
    [
        (_sessions(reader={"kind": "operator"}), "not a principal session"),
        (_sessions(reader={"id": WRITER}), "one principal"),
        (_sessions(reader={"tenant_id": READER}), "outside the agent's tenant"),
    ],
    ids=["operator-session", "one-principal", "foreign-tenant"],
)
def test_a_wrong_session_stops_the_run_before_any_model_request_or_label(
    harness: Any, sessions: dict[str, dict[str, Any]], message: str
) -> None:
    run, timeline = harness
    api = _Api(run, sessions)
    run.http = api
    with pytest.raises(lrr.LearnedRulesError, match=message):
        run.execute()

    assert "wait for final" not in timeline and "proposals on" not in timeline
    assert not _labelled(run), "ticket A was labelled"
    assert not api.links
    assert run.model_requests() == {"captured": 0, "served": 0, "unexpected": 0, "proxied": 0}
    assert run.model is not None and run.model.unexpected == []
    assert run.chain["steps"] == []


# Criterion 8a end: the revision objective routes through the real stub --------


@pytest.fixture
def stub(tmp_path: Path) -> Iterator[Any]:
    github = sk._module("learned_rules_github_stub", REPO_ROOT / "tools/github-stub/github_stub.py")
    issue_file = tmp_path / "issue.md"
    issue_file.write_text("Seed issue.\n")
    server = github.GithubStub(
        tmp_path / "github",
        json.loads(RECORDING.read_text()),
        seed_tree=SEED_TREE,
        issue_file=issue_file,
    )
    server.start()
    try:
        yield server
    finally:
        server.close()


def _open_pull(stub: Any, branch: str, body: str) -> int:
    """Open a pull request the way the factory's publication does: POST pulls."""

    stub._run(
        "git",
        "--git-dir",
        str(stub._repository),
        "update-ref",
        f"refs/heads/{branch}",
        stub.main_revision(),
    )
    request = Request(
        f"{stub.base_url}/api/v3/repos/{REPO}/pulls",
        data=json.dumps(
            {"title": "Add a unit", "head": branch, "base": "main", "body": body}
        ).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer example"},
        method="POST",
    )
    context = ssl.create_default_context(cafile=str(stub.ca_file))
    with urlopen(request, context=context, timeout=30) as response:
        assert response.status == 201
        return int(json.load(response)["number"])


def _revision_request(base: str, pull: int) -> dict[str, Any]:
    url = feedback_url(base, REPO, pull, "issuecomment-41")
    feedback = UnverifiedFeedback(
        delivery_id=uuid.UUID(int=7),
        event="issue_comment",
        installation_id=5501,
        repository_id=1,
        repo_full_name=REPO,
        pr_number=pull,
        feedback_id=41,
        sender_id=6630,
        sender_login="rule-writer",
        body=lrr.REVIEW_COMMENT.read_text().strip(),
        url=url,
        created_at=datetime(2026, 11, 20, tzinfo=UTC),
        head_sha=None,
        commit_sha=None,
        author_association="COLLABORATOR",
    )
    objective = _objective(
        feedback_provenance(feedback), url, {"pull_request_body": "Adds Kelvin."}
    )
    text = frame_user_turn("job", "", objective, "github")
    return {"messages": [{"role": "user", "content": [{"type": "text", "text": text}]}]}


def test_a_revision_on_the_factory_pull_request_routes_to_the_ticket_it_closes(stub: Any) -> None:
    identity = lrl.issue_identity(stub)
    branch = "curie/publication-" + "0" * 32
    ticket_a = _open_pull(stub, branch, "Adds Kelvin conversions.\n\nCloses #3815")
    other = _open_pull(stub, branch + "1", "Adds Rankine conversions.\n\nCloses #3921")
    unrelated = _open_pull(stub, branch + "2", "A person's refactor.")

    assert identity(_revision_request(stub.base_url, ticket_a)) == lrs.TICKET_A
    assert identity(_revision_request(stub.base_url, other)) == lrs.TICKET_B
    assert identity(_revision_request(stub.base_url, unrelated)) is None


# The chain stops at a broken link before the next ticket's model calls ------


def _chain_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, writer_code: str | None):
    """A LearnedRulesRun whose steps a to c replay the passing evidence and whose
    decisions answer like the API: a stale version is refused, an accept bumps it."""

    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "CURIE_LOCAL_STUB_HOST"):
        monkeypatch.delenv(name, raising=False)
    args = fe.parse_args(
        ["scripted", "--scenario", "learned-rules", "--transcript", str(_transcripts(tmp_path))]
    )
    run = lrr.LearnedRulesRun(args)
    passing = {step["step"]: step for step in _passing()["steps"]}
    pull_a = passing["a"]["pull_request"]
    rule = dict(passing["c"]["rules"][0])
    calls: list[tuple[str, ...]] = []
    run.evidence["pull_request"] = {"number": pull_a, "body": f"Closes #{lrs.TICKET_A}"}
    run.proposals_on = True
    run.session_files = {role: tmp_path / role for role in ("writer", "reader")}
    run.env.update(CURIE_API_URL=API_ORIGIN, CURIE_API_KEY="platform-key")
    monkeypatch.setattr(run, "label_events", lambda _number: 1)

    def revise(_github: Any, _pull: int) -> str:
        run.chain["steps"].append(passing["b"])
        # The raw URL names the live stub; the stored step holds the scrubbed one.
        return REPLY_URL.replace("https://stub.example.test", "http://192.0.2.10:18080")

    def promote(_pull: int, _reply_url: str) -> dict[str, Any]:
        run.chain["steps"].append(passing["c"])
        return dict(rule)

    def decide(verb: str, rule_id: str, version: int, role: str) -> tuple[int, str]:
        calls.append((verb, role, str(version)))
        if version != rule["version"]:
            return 1, json.dumps({"error": {"code": "memory_rules.stale_version"}})
        if role == "reader":
            return 1, json.dumps({"error": {"code": lrs.WRITE_ACCESS_REQUIRED}})
        if writer_code is not None:
            return 1, json.dumps({"error": {"code": writer_code}})
        rule.update(state={"accept": "accepted", "remove": "removed"}[verb], version=version + 1)
        return 0, ""

    def ticket(_github: Any, number: int, _text: str) -> dict[str, Any]:
        calls.append(("ticket", str(number)))
        raise _Stop(f"ticket {number} would make model calls")

    monkeypatch.setattr(run, "revise", revise)
    monkeypatch.setattr(run, "promote", promote)
    monkeypatch.setattr(run, "decide", decide)
    monkeypatch.setattr(run, "list_rules", lambda: [dict(rule)])
    monkeypatch.setattr(run, "command_result", lambda *_argv, **_kwargs: (0, ""))
    monkeypatch.setattr(run, "run_ticket", ticket)
    return run, calls


def test_decisions_pass_the_listed_version_and_ticket_b_follows_the_accept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run, calls = _chain_run(tmp_path, monkeypatch, writer_code=None)
    with pytest.raises(_Stop, match=f"ticket {lrs.TICKET_B}"):
        run.run_chain(object())
    assert calls == [("accept", "reader", "2"), ("accept", "writer", "2"), ("ticket", "3921")]
    decided = _step({"steps": run.chain["steps"]}, "d")
    assert decided["writer"] == {"exit": 0, "codes": [], "state": "accepted", "version": 3}
    run.cleanup()


def test_a_refused_accept_stops_the_chain_before_ticket_b_is_labelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run, calls = _chain_run(tmp_path, monkeypatch, writer_code="memory_rules.stale_version")
    with pytest.raises(lrr.LearnedRulesError, match="writer's accept exited 1"):
        run.run_chain(object())
    assert ("ticket", str(lrs.TICKET_B)) not in calls
    decided = _step({"steps": run.chain["steps"]}, "d")
    assert decided["writer"]["codes"] == ["memory_rules.stale_version"]
    run.cleanup()


# The browser hold: one proposed rule kept up for an external Console check ---


class _Clock:
    """``learned_rules_run.time`` with a clock that each sleep advances."""

    def __init__(self, on_sleep: Callable[[int], None] | None = None) -> None:
        self.now = 1000.0
        self.sleeps = 0
        self.on_sleep = on_sleep

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        self.now += seconds
        if self.on_sleep is not None:
            self.on_sleep(self.sleeps)


CANDIDATE = "c" * 40


def _held_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hold: int | None
) -> tuple[lrr.LearnedRulesRun, list[tuple[str, ...]]]:
    run, calls = _chain_run(tmp_path, monkeypatch, writer_code=None)
    run.args.hold_for_browser = hold
    run.args.candidate = CANDIDATE
    run.workdir = tmp_path / "run"
    run.workdir.mkdir()
    run.env["KUBECONFIG"] = str(run.workdir / "kubeconfig")
    run.deadline = 1000.0 + sk.BOUND_SECONDS
    ports = {lrr.BROWSER_API_SERVICE: 8000, lrr.BROWSER_UI_SERVICE: 8080}
    monkeypatch.setattr(run, "service_port", lambda name: ports[name])
    return run, calls


def test_hold_for_browser_keeps_one_proposed_rule_until_released(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The production chain with the hold flag: after the merge proposed the
    # rule, the run writes the hold file, decides nothing and opens no ticket
    # until the release file appears, then ends held for teardown.
    run, calls = _held_run(tmp_path, monkeypatch, hold=600)
    assert run.workdir is not None
    release = run.workdir / "browser-hold.release"
    monkeypatch.setattr(lrr, "time", _Clock(lambda n: release.touch() if n == 3 else None))
    monkeypatch.setattr(run, "observe", lambda: None)
    run.run_scenarios(object())

    assert calls == [], "the hold decided a rule or opened a ticket"
    assert run.chain["result"] == "held"
    hold = run.workdir / "browser-hold.json"
    assert hold.stat().st_mode & 0o777 == 0o600
    held = json.loads(hold.read_text())
    passing = {step["step"]: step for step in _passing()["steps"]}
    assert held["rule"] == {
        key: passing["c"]["rules"][0].get(key) for key in ("id", "version", "state")
    }
    assert held["rule"]["state"] == "proposed"
    assert held["release_file"] == str(release)
    assert held["services"] == {"curie-api": 8000, "curie-ui": 8080}
    assert set(held["session_files"]) == {"writer", "reader"}
    assert held["session_cookie"] == pto.SESSION_COOKIE
    assert held["candidate"] == CANDIDATE
    step = _step({"steps": run.chain["steps"]}, "hold")
    assert step["held_seconds"] == 3 * lrr.POLL_SECONDS
    assert step["rules_after_hold"][0]["state"] == "proposed"
    run.cleanup()


def test_hold_for_browser_without_a_release_fails_and_decides_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run, calls = _held_run(tmp_path, monkeypatch, hold=10)
    monkeypatch.setattr(lrr, "time", _Clock())
    with pytest.raises(lrr.LearnedRulesError, match="without a release"):
        run.run_chain(object())
    assert calls == []
    run.cleanup()


def test_hold_for_browser_refuses_a_release_first_seen_after_its_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The release appears during the poll that carries the hold past its
    # bound (2 s with a 1 s bound): it came too late, so the run fails.
    run, calls = _held_run(tmp_path, monkeypatch, hold=1)
    assert run.workdir is not None
    release = run.workdir / "browser-hold.release"
    monkeypatch.setattr(lrr, "time", _Clock(lambda n: release.touch() if n == 1 else None))
    with pytest.raises(lrr.LearnedRulesError, match="without a release in time"):
        run.run_chain(object())
    assert release.exists()
    assert calls == []
    run.cleanup()


def test_without_the_hold_the_chain_goes_on_to_the_decisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Liveness of the default: no hold flag, the reader and writer decide.
    run, calls = _held_run(tmp_path, monkeypatch, hold=None)
    with pytest.raises(_Stop):
        run.run_chain(object())
    assert calls[:2] == [("accept", "reader", "2"), ("accept", "writer", "2")]
    assert run.workdir is not None
    assert not (run.workdir / "browser-hold.json").exists()
    run.cleanup()


@pytest.mark.parametrize(
    "argv",
    [
        ["--scenario", "publication", "--hold-for-browser", "60"],
        ["--scenario", "learned-rules", "--hold-for-browser", "0"],
        ["--scenario", "learned-rules", "--hold-for-browser", "7201"],
        ["--scenario", "learned-rules", "--record", "--hold-for-browser", "60"],
    ],
)
def test_hold_for_browser_is_refused_outside_a_learned_rules_replay(
    argv: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_FACTORY_RECORD_KEY", "not-a-key")
    args = fe.parse_args(["scripted", *argv])
    with pytest.raises(fe.ConfigError, match="hold-for-browser"):
        sk.validate_configuration(args)


def _browser_args(*extra: str) -> Any:
    return fe.parse_args(
        ["scripted", "--scenario", "learned-rules", "--hold-for-browser", "60", *extra]
    )


@pytest.mark.parametrize(
    ("extra", "state", "refusal"),
    [
        (["--candidate", CANDIDATE], (CANDIDATE, []), "needs --build-images"),
        (["--build-images"], (CANDIDATE, []), "needs --candidate"),
        (["--build-images", "--candidate", "main"], ("main", []), "needs --candidate"),
        (
            ["--build-images", "--candidate", CANDIDATE],
            ("d" * 40, []),
            "not the --candidate",
        ),
        (
            ["--build-images", "--candidate", CANDIDATE],
            (CANDIDATE, ["apps/ui/src/App.tsx"]),
            "1 changed or untracked paths",
        ),
        (["--build-images", "--candidate", CANDIDATE], (None, []), "could not read"),
    ],
    ids=[
        "reused-local-images",
        "no-candidate",
        "candidate-not-a-commit-id",
        "checkout-at-another-commit",
        "checkout-with-a-changed-file",
        "checkout-unreadable",
    ],
)
def test_hold_for_browser_is_pinned_to_a_fresh_build_of_the_candidate(
    extra: list[str],
    state: tuple[str | None, list[str]],
    refusal: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sk, "checkout_state", lambda _root: state)
    with pytest.raises(fe.ConfigError, match=refusal):
        sk.validate_configuration(_browser_args(*extra))


def test_hold_for_browser_accepts_a_fresh_build_of_a_clean_candidate_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sk, "checkout_state", lambda _root: (CANDIDATE, []))
    sk.validate_configuration(
        _browser_args(
            "--build-images",
            "--candidate",
            CANDIDATE,
            "--transcript",
            str(_transcripts(tmp_path)),
        )
    )


def test_checkout_state_reads_head_and_untracked_paths(tmp_path: Path) -> None:
    tmp_path = tmp_path / "checkout"
    tmp_path.mkdir()
    env = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "PATH": "/usr/bin:/bin"}
    ident = ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.com"]

    def run(*argv: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(tmp_path), *argv], capture_output=True, text=True, check=True, env=env
        )

    run("init", "--quiet")
    (tmp_path / "tracked.txt").write_text("one\n")
    run("add", "tracked.txt")
    run(*ident, "commit", "--quiet", "-m", "base")
    head = run("rev-parse", "HEAD").stdout.strip()
    assert sk.checkout_state(tmp_path) == (head, [])
    (tmp_path / "untracked.txt").write_text("context\n")
    assert sk.checkout_state(tmp_path) == (head, ["untracked.txt"])
    assert sk.checkout_state(tmp_path / "missing") == (None, [])


class _Engine:
    """A Docker engine and kind node as the scripted build and import call them:
    each build gives its tag a new image id, and an import records the id the
    imported tag named at that moment."""

    SNAPSHOTTER = '[plugins."io.containerd.grpc.v1.cri".containerd]\nsnapshotter = "overlayfs"\n'

    def __init__(self) -> None:
        self.tags: dict[str, str] = {}
        self.imported: dict[str, str] = {}
        self.builds = 0

    def build(self, tag: str) -> None:
        self.builds += 1
        self.tags[tag] = f"sha256:{self.builds:064x}"

    def __call__(self, *argv: str, timeout: float = 900) -> str:
        if argv[:2] == ("docker", "build"):
            self.build(argv[argv.index("-t") + 1])
            return ""
        if argv[1:3] == ("build", "--tag"):
            self.build(argv[3])
            return ""
        if argv[:4] == ("docker", "image", "inspect", "--format"):
            return self.tags[argv[5]] + "\n"
        if argv[:3] == ("docker", "image", "inspect"):
            return json.dumps([{"Size": 1, "Id": self.tags[ref]} for ref in argv[3:]])
        if argv[:2] == ("docker", "version"):
            return "linux/amd64\n"
        if argv[:3] == ("kind", "get", "nodes"):
            return "fixture-control-plane\n"
        if argv[:2] == ("docker", "exec") and "config" in argv:
            return self.SNAPSHOTTER
        if argv[0] == "bash" and "kind-image-import" in argv:
            self.imported[argv[-1]] = self.tags[argv[-1]]
            return ""
        raise AssertionError(f"unexpected engine call {argv[:3]}")


def _built_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[lrr.LearnedRulesRun, _Engine]:
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "CURIE_LOCAL_STUB_HOST"):
        monkeypatch.delenv(name, raising=False)
    args = fe.parse_args(
        [
            "scripted",
            "--scenario",
            "learned-rules",
            "--build-images",
            "--transcript",
            str(_transcripts(tmp_path)),
        ]
    )
    run = lrr.LearnedRulesRun(args)
    engine = _Engine()
    run.curie = "curie"
    monkeypatch.setattr(run, "command", engine)
    monkeypatch.setattr(run, "check_disk", lambda *_a, **_k: None)
    return run, engine


def test_a_build_on_the_shared_tags_between_build_and_import_changes_nothing_imported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Another checkout builds the shared tags on the same engine after this
    # run built its images and before it imports them. The import, the install
    # values and the recorded ids are still this run's own builds.
    run, engine = _built_run(tmp_path, monkeypatch)
    run.build_candidate_images()
    built = dict(run.built_image_ids)
    for ref in (*sk.IMAGES, sk.GATEWAY_IMAGE):
        engine.build(ref)
    run.load_candidate_images()
    assert engine.imported == built
    assert set(built) == set(run.image_refs)
    assert all(ref.endswith(f":run-{run.run_id}") for ref in built)
    assert run.evidence["candidate_images"] == run.evidence["loaded_images"] == built
    values = {component: {"image": {"tag": "local"}} for component in run.built_components()}
    run.pin_built_images(values)
    assert {values[c]["image"]["tag"] for c in values} == {f"run-{run.run_id}"}
    assert run.runner_ref == f"curie-runner:run-{run.run_id}"
    assert set(run.created_images) == set(built)


def test_an_image_changed_after_its_build_is_not_imported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run, engine = _built_run(tmp_path, monkeypatch)
    run.build_candidate_images()
    engine.build(f"curie-ui:run-{run.run_id}")
    with pytest.raises(sk.ScriptedScenarioError, match="changed after their build"):
        run.load_candidate_images()
    assert engine.imported == {}


# Steps d to f on real-shaped inputs ------------------------------------------
#
# The live chain stores every step scrubbed while the driver reads raw values
# (the stub's real host and port in URLs). This drives accept, ticket B and
# ticket C through the real driver code against the real stub, the real
# capture writer and the product's own section and annotation renderers, so a
# raw-versus-scrubbed or shape mismatch fails here instead of in a paid run.

IMPLEMENTER_SYSTEM = "You are Claude Code, Anthropic's official CLI for Claude."
REQUESTER_TEXT = lrr.REQUESTER_FILE.read_text()
SELECTION = "cccccccc-0000-4000-8000-000000000001"


def _rules_canonical(text: str) -> str:
    from curie_internal.memory_rules import canonical_text

    return canonical_text([(1, text)])


class _Factory:
    """Stands in for the install between a label and the final notice: the runner's
    Messages requests reach the real capture writer, and a branch and pull request
    land on the real stub with the body the API annotates."""

    def __init__(self, run: lrr.LearnedRulesRun, stub: Any, model: Any, rule_text: str) -> None:
        self.run, self.stub, self.model, self.rule_text = run, stub, model, rule_text
        self.rules_in_force = True
        self.items: dict[int, dict[str, Any]] = {}
        # Counterexamples: what the install would deliver if a link were wrong.
        self.requester_text = REQUESTER_TEXT
        self.reviewer_system = True
        self.b_asserts = ["self.assertAlmostEqual(convert(0, 'f', 'r'), 459.67, places=6)"]

    def request(self, number: int, *, implementer: bool) -> None:
        issue_url = f"{self.stub.base_url}/{REPO}/issues/{number}"
        system: list[dict[str, Any]] = [{"type": "text", "text": IMPLEMENTER_SYSTEM}]
        if implementer:
            if self.rules_in_force:
                canonical = _rules_canonical(self.rule_text)
                system.append({"type": "text", "text": rules_section(1, DIGEST, canonical)})
            system.append(
                {
                    "type": "text",
                    "text": requester_section(
                        SELECTION,
                        REQUESTER_DIGEST,
                        self.requester_text,
                        learned_rules=self.rules_in_force,
                    ),
                }
            )
            first = frame_user_turn("job", "", f"Implement {issue_url}", "github")
        else:
            first = f"<system-reminder>\nReview the change for {issue_url}\n</system-reminder>"
        body = {
            "model": "claude",
            "tools": [{"name": "Read"}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": first}]}],
        }
        if implementer or self.reviewer_system is True:
            body["system"] = system
        elif isinstance(self.reviewer_system, str):
            body["system"] = [{"type": "text", "text": self.reviewer_system}]
        self.model._capture(json.dumps(body).encode())

    def publish(self, number: int, asserts: list[str]) -> None:
        branch = f"curie/publication-{number:032x}"
        work = self.run.workdir / f"clone-{number}"
        git = ["git", "-c", "user.name=factory", "-c", "user.email=factory@example.test"]
        self.stub._run("git", "clone", "-q", str(self.stub._repository), str(work))
        test = work / "tests" / f"test_issue_{number}.py"
        test.parent.mkdir(exist_ok=True)
        test.write_text("".join(f"        {line}\n" for line in asserts))
        self.stub._run(*git, "-C", str(work), "add", "-A")
        self.stub._run(*git, "-C", str(work), "commit", "-qm", f"Issue {number}")
        self.stub._run(*git, "-C", str(work), "push", "-q", "origin", f"HEAD:refs/heads/{branch}")
        body = api_requester.annotate(f"Adds the units.\n\nCloses #{number}", REQUESTER_DIGEST)
        if self.rules_in_force:
            body = api_rules.annotate(body, api_rules.AppliedRules((1,), DIGEST))
        _post_pull(self.stub, branch, body)
        self.items[number] = (
            {"state": "selected", "count": 1, "rule_numbers": [1], "sha256": DIGEST}
            if self.rules_in_force
            else {"state": "absent", "count": 0, "rule_numbers": [], "sha256": None}
        )


def _post_pull(stub: Any, branch: str, body: str) -> int:
    request = Request(
        f"{stub.base_url}/api/v3/repos/{REPO}/pulls",
        data=json.dumps(
            {"title": "Add units", "head": branch, "base": "main", "body": body}
        ).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer example"},
        method="POST",
    )
    context = ssl.create_default_context(cafile=str(stub.ca_file))
    with urlopen(request, context=context, timeout=30) as response:
        assert response.status == 201
        return int(json.load(response)["number"])


def _drive_steps_d_to_f(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stub: Any, **faults: Any
) -> tuple[lrr.LearnedRulesRun, dict[str, Any]]:
    """Run accept, ticket B and ticket C through the real driver code against the
    real stub; ``faults`` set attributes of the install stand-in before it runs."""

    from types import SimpleNamespace

    # The stub module is loaded by path, so read its accounts from the class's module.
    github = SimpleNamespace(RULE_WRITER=type(stub).__init__.__globals__["RULE_WRITER"])
    model_module = sk._module("learned_rules_model_script", lrr.MODEL_SCRIPT)
    run, calls = _chain_run(tmp_path, monkeypatch, writer_code=None)
    run.stub = stub
    run.workdir = tmp_path / "work"
    run.workdir.mkdir()
    run.capture_dir = run.workdir / "model-captures"
    run.identity = lrl.issue_identity(stub)
    run.deadline = __import__("time").monotonic() + 60
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"version": 1, "exchanges": []}))
    model = model_module.ModelScript(empty, capture_dir=run.capture_dir, issue_of=run.identity)
    rule_text = lrs.RULE_TEXT
    factory = _Factory(run, stub, model, rule_text)
    for name, value in faults.items():
        setattr(factory, name, value)
    raw_source = f"{stub.base_url}/{REPO}/pull/7#issuecomment-41"
    listed = {"id": RULE_ID, "label": "R1", "state": "proposed", "version": 2}
    listed.update(source_url=raw_source, text=rule_text)

    def decide(verb: str, rule_id: str, version: int, role: str) -> tuple[int, str]:
        if version != listed["version"]:
            return 1, json.dumps({"error": {"code": "memory_rules.stale_version"}})
        if role == "reader":
            return 1, json.dumps({"error": {"code": lrs.WRITE_ACCESS_REQUIRED}})
        listed.update(state={"accept": "accepted", "remove": "removed"}[verb], version=version + 1)
        factory.rules_in_force = verb == "accept"
        return 0, ""

    def label(_github: Any, number: int) -> None:
        lrr.LearnedRulesRun.label_ticket(run, github, number)
        factory.request(number, implementer=True)
        factory.request(number, implementer=False)
        factory.request(number, implementer=True)
        factory.publish(number, factory.b_asserts if number == lrs.TICKET_B else [])

    def sql(query: str) -> list[Any]:
        for number in (lrs.TICKET_B, lrs.TICKET_C):
            if query == lrr.requests_sql(number):
                return [{"id": f"request-{number}", "status": "completed"}]
            if query == lrr.work_items_sql(number):
                return [f"item-{number}"]
        raise AssertionError(f"unexpected SQL {query[:80]}")

    def api(method: str, path: str, **_kwargs: Any) -> tuple[int, Any]:
        number = int(path.rsplit("-", 1)[1])
        return 200, {"memory_rules": factory.items[number]}

    monkeypatch.setattr(run, "decide", decide)
    monkeypatch.setattr(run, "list_rules", lambda: [dict(listed)])
    monkeypatch.setattr(run, "label_ticket", label)
    monkeypatch.setattr(run, "run_ticket", lambda *args: lrr.LearnedRulesRun.run_ticket(run, *args))
    monkeypatch.setattr(run, "sql", sql)
    monkeypatch.setattr(run, "api", api)
    monkeypatch.setattr(run, "final_notice", lambda _request_id: {"id": 1})
    monkeypatch.setattr(run, "check_boundaries", lambda: None)

    def lanes_step() -> None:
        raise _Stop("lanes")

    monkeypatch.setattr(run, "lanes_step", lanes_step)
    try:
        with pytest.raises(_Stop, match="lanes"):
            run.run_chain(github)
    finally:
        run.stub = None
        run.cleanup()
    return run, {step["step"]: step for step in run.chain["steps"]}


def test_steps_d_to_f_pass_the_judge_on_raw_host_inputs_through_the_real_stub(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stub: Any
) -> None:
    run, steps = _drive_steps_d_to_f(tmp_path, monkeypatch, stub)
    assert set(steps) == {"a", "b", "c", "d", "e", "f"}
    rule = lrs.assert_promoted(steps["c"], 7, REPLY_URL)
    accepted = lrs.assert_decided(steps["d"], rule)
    lrs.assert_ticket_b(steps["e"])
    lrs.assert_ticket_c(steps["f"], accepted)
    assert all(c["issue"] == lrs.TICKET_B for c in steps["e"]["captures"])
    stored = json.dumps(run.chain["steps"])
    assert stub.base_url not in stored and f"{stub.host}:{stub.port}" not in stored


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        # The section renders but carries another document than the one the writer set.
        ({"requester_text": "Use tabs for indentation."}, "conflicting requester document"),
        # A reviewer request whose system prompt the capture writer stores as "".
        ({"reviewer_system": False}, "hold no system prompt"),
        # A reviewer request whose only system text is whitespace.
        ({"reviewer_system": "  \n\t "}, "hold no system prompt"),
        # A valid call written with whitespace before the parenthesis is still read.
        (
            {
                "b_asserts": [
                    "self.assertAlmostEqual (convert(0, 'f', 'r'), 459.67, places=5)",
                ]
            },
            "without places=6",
        ),
        # The keyword says five; a trailing comment spells six.
        (
            {
                "b_asserts": [
                    "self.assertAlmostEqual(convert(0, 'f', 'r'), 459.67, places=5)"
                    "  # previously places=6"
                ]
            },
            "without places=6",
        ),
    ],
    ids=[
        "other-requester-document",
        "empty-reviewer-prompt",
        "whitespace-reviewer-prompt",
        "places-five-with-a-space-before-the-parenthesis",
        "places-six-in-a-comment",
    ],
)
def test_ticket_b_counterexamples_through_the_real_stub_and_capture_writer_are_refused(
    fault: dict[str, Any],
    message: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stub: Any,
) -> None:
    # The driver judges step e as it lands, so the run stops there with the judge's words.
    with pytest.raises(lrr.LearnedRulesError, match=message):
        _drive_steps_d_to_f(tmp_path, monkeypatch, stub, **fault)


def _tie_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    drop_capture: int | None = None,
    foreign_capture: int | None = None,
) -> tuple[lrr.LearnedRulesRun, Any]:
    """Two requests per ticket served by the real router and captured by the real
    capture writer; the driver's own step g then reads both."""

    from types import SimpleNamespace

    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "CURIE_LOCAL_STUB_HOST"):
        monkeypatch.delenv(name, raising=False)
    model_module = sk._module("learned_rules_tie_model_script", lrr.MODEL_SCRIPT)
    identity = lrl.issue_identity(SimpleNamespace(_get=lambda *_args: None))
    bodies: list[tuple[int, bytes]] = []
    transcripts = lrl.transcript_paths(tmp_path / "chain-a.json")
    for letter, number in lrl.TICKETS.items():
        exchanges = []
        for kind in ("implement", "review"):
            first = f"{kind} https://stub.example.test/{REPO}/issues/{number}"
            body = {
                "system": [{"type": "text", "text": f"{kind} system"}],
                "messages": [{"role": "user", "content": [{"type": "text", "text": first}]}],
            }
            raw = json.dumps(body).encode()
            key = model_module.request_key(body)
            message = {"type": "message", "content": []}
            exchanges.append({"lane_key": key[0], "ordinal": key[1], "message": message})
            bodies.append((number, raw))
        model_module.dump_transcript(transcripts[letter], exchanges)
    args = fe.parse_args(
        ["scripted", "--scenario", "learned-rules", "--transcript", str(transcripts["a"])]
    )
    run = lrr.LearnedRulesRun(args)
    run.workdir = tmp_path / "work"
    run.capture_dir = run.workdir / "model-captures"
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"version": 1, "exchanges": []}))
    model = model_module.ModelScript(empty, capture_dir=run.capture_dir, issue_of=identity)
    run.router = lrl.build_router(model_module, transcripts, identity)
    run.router.install(model, model_module, fallback=False)
    for index, (_number, raw) in enumerate(bodies):
        if index != drop_capture:
            model._capture(raw)
        assert model._match("POST", "/v1/messages", raw) is not None
    if foreign_capture is not None:
        # The capture of this request names another ticket than the lane that served it.
        path = sorted(run.capture_dir.glob("*.json"))[foreign_capture]
        row = json.loads(path.read_text())
        row["issue"] = lrs.TICKET_C
        path.write_text(json.dumps(row))
    return run, model_module


def _step_g(run: lrr.LearnedRulesRun) -> dict[str, Any]:
    run.lanes_step()
    return next(step for step in run.chain["steps"] if step["step"] == "g")


def test_every_served_request_has_its_capture_on_the_serving_lanes_ticket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run, _ = _tie_run(tmp_path, monkeypatch)
    lrs.assert_lanes(_step_g(run), {}, record=False)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"drop_capture": 3}, "served ordinal 0 with no capture of that request on #3921"),
        ({"foreign_capture": 3}, "no capture of that request on #3921"),
    ],
    ids=["dropped-capture", "capture-on-another-ticket"],
)
def test_a_dropped_or_misfiled_capture_is_refused_per_request(
    changes: dict[str, Any], message: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run, _ = _tie_run(tmp_path, monkeypatch, **changes)
    with pytest.raises(lrs.LearnedRulesAssertionError, match=message):
        lrs.assert_lanes(_step_g(run), {}, record=False)


def _secret_scan_rule(rule_id: str) -> Any:
    import re
    import tomllib

    config = tomllib.loads((REPO_ROOT / ".gitleaks.toml").read_text())
    [rule] = [rule for rule in config["rules"] if rule["id"] == rule_id]
    return re.compile(rule["regex"])


def test_recording_scrub_leaves_nothing_the_secret_scan_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stub: Any
) -> None:
    """A plan review prompt names the ticket owner-qualified; the committed
    transcript must not, or the repository's secret scan refuses the push."""

    args = fe.parse_args(
        ["scripted", "--scenario", "learned-rules", "--transcript", str(_transcripts(tmp_path))]
    )
    run = lrr.LearnedRulesRun(args)
    run.stub = stub
    prompt = (
        f"ISSUE LINK: {stub.base_url}/{REPO}/issues/3815\n\n"
        f"ISSUE TEXT (ticket {REPO}#3815):\n# Add Kelvin temperature conversions"
    )
    exchange = {"lane_key": prompt, "ordinal": 0, "message": {"content": [{"text": prompt}]}}
    try:
        [scrubbed] = lrl.scrub_recording([exchange], run.replacements())
    finally:
        run.stub = None
        run.cleanup()
    text = json.dumps(scrubbed)
    assert _secret_scan_rule("cross-repo-issue-ref").search(json.dumps(exchange))
    assert not _secret_scan_rule("cross-repo-issue-ref").search(text)
    assert "(ticket <repository>#3815)" in text and stub.base_url not in text
