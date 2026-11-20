"""Assertions of the scripted ``learned-rules`` scenario (#5799).

The scenario drives one install through the whole learned rule chain: a
reviewer's mentioned comment on ticket A's pull request admits a revision that
proposes a rule, the merge promotes it, a person who can write to the
repository accepts it through the CLI, ticket B's implementer is told the rule
above a conflicting requester preference and its pull request names it, and
after removal ticket C carries none of it. The driver (``learned_rules_run``)
records one step per link, in order, under ``evidence["learned_rules"]``; the
functions below take that record and raise ``LearnedRulesAssertionError`` when a
step is missing, out of order, or one of its observations does not hold. They
do no IO.

Step keys, in order:

0. the session probe before any model request;
a. proposals on, ticket A labelled, its pull request open;
b. the writer's mentioned comment admitted a revision that called
   ``propose_rule`` with the tool mounted, and no rule is listed yet;
c. the merge made one ``proposed`` rule R1 whose source is the revision's
   reply target on A's pull request;
d. the read only principal is refused the acceptance, the writer accepts;
e. ticket B: selected rules, the implementer section above the requester
   section, no reviewer copy, the publication line, and the added assertions;
f. after removal, ticket C carries no learned rules;
g. every ticket lane is complete and every captured prompt is on its ticket.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

STEPS = ("0", "a", "b", "c", "d", "e", "f", "g")
EVIDENCE_KEY = "learned_rules"
REPOSITORY = "acme-corp/acme-bot"
TICKET_A = 3815
TICKET_B = 3921
TICKET_C = 3922
TICKETS = {"a": TICKET_A, "b": TICKET_B, "c": TICKET_C}
LANES = {letter: f"learned-rules-{letter}" for letter in TICKETS}
MENTION = "@curie-factory-bot"
RULE_TEXT = (
    "Use `self.assertAlmostEqual(..., places=6)` for every floating point result; "
    "every float assertion in this repository's tests does that."
)
PROPOSE_TOOL = "mcp__curie__propose_rule"
WRITER_LOGIN = "rule-writer"
WRITE_ACCESS_REQUIRED = "memory_rules.write_access_required"
# runner/src/curie_runner/memory_rules_context.py and requester_context.py.
SECTION_OPEN = "<<<curie-learned-rules "
SECTION_CLOSE = "<<<end-curie-learned-rules sha256="
REQUESTER_OPEN = "<<<curie-requester-preferences"
REQUESTER_CLOSE = "<<<end-curie-requester-preferences"
# apps/api/src/curie_api/memory_rules_context.py (applied_line, annotate).
LEARNED_PREFIX = "Learned rules:"
REQUESTER_PREFIX = "Requester preferences:"
REQUESTER_APPLIED = re.compile(r"^Requester preferences: applied; sha256:[0-9a-f]{64}$")
SENDER_FRAME = "[platform-sender"
ASSERT_CALL = "assertAlmostEqual("
PLACES = "places=6"
_CODE = re.compile(r"memory_rules\.[a-z_]+")


class LearnedRulesAssertionError(AssertionError):
    """The learned rules chain observed something it forbids, or skipped a step."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LearnedRulesAssertionError(message)


# Pure readers the driver uses to turn raw observations into step evidence ------


def prompt_positions(
    system: str, rule_text: str | None, requester_text: str | None = None
) -> dict[str, Any]:
    """Where a system prompt opens and closes the learned rules section and the
    requester section, whether the section carries ``R1: <rule_text>``, and
    whether the requester section carries ``requester_text``."""

    opened = system.find(SECTION_OPEN)
    closed = system.find(SECTION_CLOSE)
    requester = system.find(REQUESTER_OPEN)
    rule_line = False
    if opened >= 0 and closed > opened and rule_text:
        rule_line = f"\nR1: {rule_text}\n" in system[opened:closed]
    requester_closed = system.find(REQUESTER_CLOSE, requester) if requester >= 0 else -1
    requester_line = False
    if requester >= 0 and requester_closed > requester and requester_text:
        requester_line = requester_text.strip() in system[requester:requester_closed]
    return {
        "open": opened if opened >= 0 else None,
        "close": closed if closed >= 0 else None,
        "requester": requester if requester >= 0 else None,
        "rule_line": rule_line,
        "requester_text": requester_line,
        "system_chars": len(system),
    }


def annotation_lines(body: str | None) -> list[dict[str, Any]]:
    """The pull request body's learned rules and requester lines, with their line index."""

    return [
        {"index": index, "line": line}
        for index, line in enumerate((body or "").splitlines())
        if line.startswith((LEARNED_PREFIX, REQUESTER_PREFIX))
    ]


def added_assert_lines(diff: str) -> list[str]:
    """Every added line of a unified diff that calls ``assertAlmostEqual(``."""

    return [
        line[1:].strip()
        for line in diff.splitlines()
        if line.startswith("+") and not line.startswith("+++") and ASSERT_CALL in line
    ]


def places_six(line: str) -> bool:
    """Whether the line is one call whose ``places`` keyword argument is the integer 6.

    Read from the parsed call, so a comment, a string or another keyword that
    spells ``places=6`` does not count, and ``places=60`` or ``places=6.5`` is not six.
    """

    try:
        tree = ast.parse(line.strip())
    except SyntaxError:
        return False
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == ASSERT_CALL.rstrip("(")
    ]
    if not calls:
        return False
    for call in calls:
        values = [kw.value for kw in call.keywords if kw.arg == "places"]
        if len(values) != 1:
            return False
        value = values[0]
        if not (isinstance(value, ast.Constant) and type(value.value) is int and value.value == 6):
            return False
    return True


def tool_use_names(message: Mapping[str, Any]) -> list[str]:
    content = message.get("content")
    if not isinstance(content, list):
        return []
    return [
        str(block.get("name"))
        for block in content
        if isinstance(block, dict) and block.get("type") == "tool_use"
    ]


def propose_ordinals(exchanges: Iterable[Mapping[str, Any]]) -> list[int]:
    """The ordinals of the answers that call ``propose_rule``."""

    return [
        int(exchange["ordinal"])
        for exchange in exchanges
        if PROPOSE_TOOL in tool_use_names(exchange.get("message") or {})
    ]


def error_codes(output: str) -> list[str]:
    return sorted(set(_CODE.findall(output)))


def closes(body: str | None) -> list[int]:
    return sorted({int(n) for n in re.findall(r"\b[Cc]loses #(\d+)\b", body or "")})


# The judge -------------------------------------------------------------------


def _steps(evidence: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    steps = evidence.get("steps")
    if not isinstance(steps, list):
        raise LearnedRulesAssertionError("the evidence holds no steps")
    names = [str(step.get("step")) if isinstance(step, Mapping) else None for step in steps]
    missing = [name for name in STEPS if name not in names]
    _require(not missing, f"steps missing: {missing}")
    _require(names == list(STEPS), f"steps out of order: {names}")
    return {str(step["step"]): step for step in steps}


def assert_probe(step: Mapping[str, Any]) -> None:
    """0: two principal sessions, two principals, both in the agent's tenant, no model call."""

    kinds = list(step.get("session_kinds") or [])
    _require(kinds == ["principal", "principal"], f"session kinds were {kinds}")
    principals = [str(p) for p in step.get("principal_ids") or [] if p]
    _require(len(principals) == 2, "the probe did not read two principals")
    _require(principals[0] != principals[1], "both sessions resolved one principal")
    tenant = step.get("agent_tenant")
    tenants = list(step.get("principal_tenants") or [])
    _require(bool(tenant), "the agent's tenant was not read")
    _require(
        len(tenants) == 2 and all(t == tenant for t in tenants),
        "a principal is outside the agent's tenant",
    )
    counts = step.get("model_requests")
    _require(isinstance(counts, Mapping), "the probe recorded no model request count")
    assert isinstance(counts, Mapping)
    for key in ("captured", "served", "unexpected", "proxied"):
        _require(counts.get(key) == 0, f"{counts.get(key)} model requests {key} before the probe")


def assert_ticket_a(step: Mapping[str, Any]) -> int:
    """a: proposals on, ticket A labelled, and one pull request that closes it."""

    _require(step.get("memory_rule_proposals") is True, "the agent read lacks proposals on")
    _require(step.get("labelled") is True, "ticket A was not labelled")
    pull = step.get("pull_request")
    _require(isinstance(pull, int), "ticket A opened no pull request")
    _require(step.get("closes") == [TICKET_A], "ticket A's pull request closes another ticket")
    return int(pull)  # type: ignore[arg-type]


def assert_revision(step: Mapping[str, Any], pull_a: int) -> str:
    """b: the mentioned comment admitted a revision that proposed with the tool mounted."""

    comment = step.get("comment") or {}
    _require(comment.get("pull_request") == pull_a, "the comment is not on A's pull request")
    _require(comment.get("author") == WRITER_LOGIN, "the comment is not the writer's")
    _require(
        comment.get("author_association") == "COLLABORATOR", "the comment is not a collaborator's"
    )
    body = str(comment.get("body") or "")
    _require(MENTION in body, "the comment does not mention the factory")
    _require(RULE_TEXT in body, "the comment does not state the rule")
    revision = step.get("revision") or {}
    _require(bool(revision.get("request_id")), "no revision request was admitted")
    _require(revision.get("status") == "completed", "the revision did not complete")
    _require(revision.get("final_notice") is True, "the revision posted no final notice")
    url = revision.get("reply_target_url")
    _require(isinstance(url, str) and bool(url), "the revision stored no reply target URL")
    served = [int(k) for k in step.get("propose_ordinals") or []]
    _require(bool(served), "lane A served no propose_rule call")
    mounted = {
        int(c["ordinal"])
        for c in step.get("revision_captures") or []
        if c.get("mounted") and c.get("issue") == TICKET_A and c.get("ordinal") is not None
    }
    _require(
        any(k + 1 in mounted for k in served),
        "no follow-up of the propose_rule call shows the tool mounted",
    )
    rules = list(step.get("rules_before_merge") or [])
    _require(not rules, f"{len(rules)} rules were listed before the merge")
    return str(url)


def assert_promoted(step: Mapping[str, Any], pull_a: int, reply_url: str) -> Mapping[str, Any]:
    """c: the merge made one proposed rule R1 sourced from the revision's reply target."""

    _require(step.get("merged") is True, "ticket A's pull request was not merged")
    rules = list(step.get("rules") or [])
    _require(len(rules) == 1, f"expected one rule after the merge, found {len(rules)}")
    rule = rules[0]
    _require(rule.get("state") == "proposed", f"the rule is {rule.get('state')}, not proposed")
    _require(rule.get("label") == "R1", f"the rule is labelled {rule.get('label')}, not R1")
    version = rule.get("version")
    _require(
        isinstance(version, int) and not isinstance(version, bool) and version >= 1,
        f"the proposed rule has no version: {version!r}",
    )
    # Compared before scrubbing, so a source that differs only in the stub's
    # address (a dropped port) cannot pass as the same scrubbed URL.
    _require(
        step.get("source_matches_reply_target") is True,
        "the rule's unscrubbed source is not the revision's unscrubbed reply target",
    )
    source = rule.get("source_url")
    _require(source == reply_url, "the rule's source is not the revision's reply target")
    path = urlsplit(str(source)).path
    _require(path == f"/{REPOSITORY}/pull/{pull_a}", "the rule's source is not A's pull request")
    _require(bool(rule.get("text")), "the rule has no text")
    return dict(rule)


def assert_decided(step: Mapping[str, Any], rule: Mapping[str, Any]) -> Mapping[str, Any]:
    """d: the read only principal is refused; the writer accepts.

    Both decisions pass the version the list read for the proposed rule, and the
    accept bumps it by one. Returns the accepted rule's id and version.
    """

    _require(step.get("rule_id") == rule.get("id"), "another rule was decided")
    _require(
        step.get("expected_version") == rule.get("version"),
        "the decision did not pass the proposed rule's version",
    )
    reader = step.get("reader") or {}
    _require(reader.get("exit") == 1, f"the reader's accept exited {reader.get('exit')}")
    _require(
        WRITE_ACCESS_REQUIRED in (reader.get("codes") or []),
        f"the reader's refusal does not name {WRITE_ACCESS_REQUIRED}",
    )
    writer = step.get("writer") or {}
    _require(writer.get("exit") == 0, f"the writer's accept exited {writer.get('exit')}")
    _require(writer.get("state") == "accepted", "the rule did not read accepted")
    _require(
        writer.get("version") == int(rule["version"]) + 1,
        "the accepted rule's version did not move by one",
    )
    return {"id": rule.get("id"), "version": writer.get("version")}


def _captures(step: Mapping[str, Any], issue: int) -> tuple[list[Mapping[str, Any]], list[Any]]:
    captures = [c for c in step.get("captures") or [] if c.get("issue") == issue]
    implementer = [c for c in captures if c.get("implementer")]
    others = [c for c in captures if not c.get("implementer")]
    _require(bool(implementer), f"no implementer prompt of #{issue} was captured")
    return implementer, others


def _ticket(step: Mapping[str, Any], issue: int) -> None:
    _require(step.get("labeller") == WRITER_LOGIN, f"#{issue} was not labelled by the writer")
    _require(isinstance(step.get("pull_request"), int), f"#{issue} opened no pull request")
    _require(step.get("closes") == [issue], f"#{issue}'s pull request closes another ticket")


def assert_ticket_b(step: Mapping[str, Any]) -> None:
    """e: B's implementer is told R1 above the requester section and its PR names it."""

    _require(step.get("coding_instructions_exit") == 0, "the requester document was not set")
    _ticket(step, TICKET_B)
    rules = step.get("work_item_memory_rules") or {}
    _require(rules.get("state") == "selected", f"B's rules are {rules.get('state')}")
    _require(rules.get("count") == 1, f"B selected {rules.get('count')} rules")
    digest = rules.get("sha256")
    _require(isinstance(digest, str) and bool(digest), "B's work item carries no digest")
    implementer, others = _captures(step, TICKET_B)
    for capture in implementer:
        _require(
            capture.get("open") is not None and capture.get("close") is not None,
            "B's implementer prompt lacks the learned rules section",
        )
        _require(capture.get("rule_line") is True, "B's section lacks the line R1: <rule text>")
        requester = capture.get("requester")
        _require(requester is not None, "B's implementer prompt lacks the requester section")
        _require(
            capture["close"] < requester,
            "B's learned rules section does not end before the requester section",
        )
        _require(
            capture.get("requester_text") is True,
            "B's requester section does not carry the conflicting requester document",
        )
    _require(bool(others), "no reviewer prompt of B was captured, so none was checked")
    empty = [c.get("sequence") for c in others if not int(c.get("system_chars") or 0)]
    _require(
        not empty, f"B's reviewer captures hold no system prompt, so none was checked: {empty}"
    )
    copied = [c.get("sequence") for c in others if c.get("open") is not None]
    _require(not copied, f"B's reviewer prompts carry the learned rules section: {copied}")
    lines = list(step.get("annotations") or [])
    learned = [item for item in lines if str(item["line"]).startswith(LEARNED_PREFIX)]
    requester_lines = [item for item in lines if str(item["line"]).startswith(REQUESTER_PREFIX)]
    _require(len(learned) == 1, f"B's body carries {len(learned)} learned rules lines")
    _require(len(requester_lines) == 1, "B's body carries no single requester line")
    _require(
        learned[0]["line"] == f"{LEARNED_PREFIX} in force; rules:R1; sha256:{digest}",
        "B's learned rules line does not name R1 with the work item's digest",
    )
    _require(
        bool(REQUESTER_APPLIED.match(str(requester_lines[0]["line"]))),
        "B's requester line does not read applied",
    )
    _require(
        learned[0]["index"] + 1 == requester_lines[0]["index"],
        "B's learned rules line is not immediately before the requester line",
    )
    added = list(step.get("added_asserts") or [])
    _require(bool(added), "B's diff adds no assertAlmostEqual( call")
    bare = [line for line in added if not places_six(line)]
    _require(not bare, f"B's diff adds assertAlmostEqual( without {PLACES}: {bare}")


def assert_ticket_c(step: Mapping[str, Any], accepted: Mapping[str, Any]) -> None:
    """f: after removal, C carries no learned rules anywhere."""

    _require(
        step.get("expected_version") == accepted.get("version"),
        "the remove did not pass the accepted rule's version",
    )
    _require(step.get("remove_exit") == 0, "the writer's remove failed")
    _require(step.get("state_after") == "removed", "the rule did not read removed")
    _ticket(step, TICKET_C)
    rules = step.get("work_item_memory_rules") or {}
    _require(rules.get("state") == "absent", f"C's rules are {rules.get('state')}, not absent")
    implementer, others = _captures(step, TICKET_C)
    carrying = [c.get("sequence") for c in [*implementer, *others] if c.get("open") is not None]
    _require(not carrying, f"C's prompts carry the learned rules section: {carrying}")
    lines = [
        item
        for item in step.get("annotations") or []
        if str(item["line"]).startswith(LEARNED_PREFIX)
    ]
    _require(not lines, "C's pull request body carries a learned rules line")


def _window(step: Mapping[str, Any], issue: int) -> tuple[int, int]:
    window = step.get("window")
    _require(
        isinstance(window, Sequence) and len(window) == 2,
        f"#{issue} recorded no capture window",
    )
    assert isinstance(window, Sequence)
    return int(window[0]), int(window[1])


def _tie_captures_to_lanes(
    step: Mapping[str, Any], captures: Sequence[Mapping[str, Any]], *, record: bool
) -> None:
    """Every request a lane served has the capture of that request on the lane's ticket.

    ``served`` lists one row per answered request (the lane that answered it, the
    answer's ordinal and a digest of the request's lane key); each is matched to
    one capture by key and ordinal that names the lane's ticket.
    A replay also owes the converse: a capture no lane answered is refused. A
    recording proxies requests the upstream may have refused, so only the first
    direction binds it.
    """

    served = list(step.get("served") or [])
    _require(bool(served), "no served request was listed, so no capture was tied to a lane")
    remaining = list(captures)
    for row in served:
        lane = str(row.get("lane"))
        _require(lane in LANES.values(), f"a request was served by an unknown lane: {lane}")
        issue = TICKETS[lane.rsplit("-", 1)[-1]]
        found = next(
            (
                c
                for c in remaining
                if c.get("key") == row.get("key")
                and c.get("ordinal") == row.get("ordinal")
                and c.get("issue") == issue
            ),
            None,
        )
        _require(
            found is not None,
            f"lane {lane} served ordinal {row.get('ordinal')} with no capture of that request "
            f"on #{issue}",
        )
        assert found is not None
        remaining.remove(found)
    if not record:
        _require(
            not remaining,
            f"captures no lane served: {[c.get('sequence') for c in remaining]}",
        )


def assert_lanes(
    step: Mapping[str, Any],
    windows: Mapping[int, tuple[int, int]],
    *,
    record: bool,
) -> None:
    """g: every lane complete, nothing unrouted, every capture on its own ticket."""

    _require(step.get("record") is record, "the evidence is from the other mode")
    if record:
        counts = step.get("recorded_counts") or {}
        for letter in TICKETS:
            _require(int(counts.get(letter) or 0) > 0, f"ticket {letter} recorded no exchange")
        _require(step.get("untagged") == 0, "a recorded exchange has no ticket tag")
    else:
        lanes = step.get("lanes") or {}
        for letter, name in LANES.items():
            lane = lanes.get(name)
            _require(isinstance(lane, Mapping), f"lane {name} is absent")
            assert isinstance(lane, Mapping)
            _require(lane.get("complete") is True, f"lane {name} is not complete")
            _require(lane.get("issue") == TICKETS[letter], f"lane {name} is on another issue")
        unrouted = list(lanes.get("unrouted") or [])
        _require(not unrouted, f"{len(unrouted)} model requests were unrouted")
    captures = list(step.get("captures") or [])
    _require(bool(captures), "no model request was captured")
    known = set(TICKETS.values())
    for capture in captures:
        sequence, issue = int(capture["sequence"]), capture.get("issue")
        _require(issue in known, f"capture {sequence} is on no ticket ({issue})")
        inside = [ticket for ticket, (lo, hi) in windows.items() if lo <= sequence <= hi]
        if inside:
            _require(issue == inside[0], f"capture {sequence} in #{inside[0]}'s run names #{issue}")
        else:
            _require(issue not in windows, f"capture {sequence} names #{issue} outside its run")
    _tie_captures_to_lanes(step, captures, record=record)


def assert_chain(evidence: Mapping[str, Any], *, record: bool) -> None:
    """Every step, in order, and every observation the chain owes."""

    steps = _steps(evidence)
    assert_probe(steps["0"])
    pull_a = assert_ticket_a(steps["a"])
    reply_url = assert_revision(steps["b"], pull_a)
    rule = assert_promoted(steps["c"], pull_a, reply_url)
    accepted = assert_decided(steps["d"], rule)
    assert_ticket_b(steps["e"])
    assert_ticket_c(steps["f"], accepted)
    windows = {TICKET_B: _window(steps["e"], TICKET_B), TICKET_C: _window(steps["f"], TICKET_C)}
    assert_lanes(steps["g"], windows, record=record)
