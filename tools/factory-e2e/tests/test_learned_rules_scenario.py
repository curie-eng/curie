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
    }


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
                "rules": [
                    {
                        "id": RULE_ID,
                        "label": "R1",
                        "state": "proposed",
                        "version": 1,
                        "source_url": REPLY_URL,
                        "text": lrs.RULE_TEXT,
                    }
                ],
            },
            {
                "step": "d",
                "rule_id": RULE_ID,
                "reader": {"exit": 1, "codes": [lrs.WRITE_ACCESS_REQUIRED]},
                "writer": {"exit": 0, "state": "accepted"},
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
                "captures": [
                    *({"sequence": n, "issue": lrs.TICKET_A} for n in range(1, 11)),
                    *({"sequence": n, "issue": lrs.TICKET_B} for n in range(11, 14)),
                    *({"sequence": n, "issue": lrs.TICKET_C} for n in range(14, 16)),
                ],
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
    objective = _objective(feedback, url, {"pull_request_body": "Adds Kelvin."})
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
