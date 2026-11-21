"""Pure judge and evidence sanitizer of the ``live-github-chain`` scenario (#5808).

Decision 7 of #5808 lists eight beats (a to h) with bounds. Decision 8 sets the
spend and attempt caps, decision 9 the evidence sanitizer, decision 10 the
teardown verdict. This module judges all of it from already fetched records and
returns one verdict per beat with reasons. It does no network, subprocess,
cluster or file access beyond importing the default bundle's Decisions grammar
(``examples/dark-factory/hooks/decisions_section.py``): the driver
(``live_github_chain.py``, a later part of #5808) fetches the GitHub API JSON,
the CLI JSON output, the install records and the timings, passes them in as
plain data and writes the verdicts out through :func:`sanitize_evidence`. The
driver does the I/O and this module judges.

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
and :data:`ACTIVE_REQUEST_STATES`. Requests carry the product's own
``execution_requests`` keys (the ``request_view`` names): ``work_item_id``,
``terminal_at`` (None while active) and ``terminal_cause``, so the judge can tie
evidence to ticket A's work item, read the requests active at the 60 second mark
and see that the question request ended :data:`QUESTIONS_ASKED`; usage samples are labelled
:data:`SPEND_SAMPLE_LABELS`; label events use GitHub's timeline shape.

Fields the driver part added to the record, each read as GitHub returns it:

1. ``beat_b["ticket_comments"]``: ticket A's issue comments, read after the pull
   request opened. One App comment must hold :data:`ANSWER_RECEIVED_LINE` and be
   created or updated (status comments are edited in place) at or after the
   answer. ``beat_b["open_pulls"][i]["html_url"]`` is the pull request's URL.
2. ``beat_c["pull_comments"]``: the pull request's conversation comments. One App
   comment created or updated at or after the review's ``submitted_at`` must name
   the revision head ``beat_c["pull_after"]["head"]["sha"]`` in full.
3. ``beat_c["revision_checks"]``: ``{"sha", "check_runs", "status"}``, the check
   runs (every page, collected to ``total_count``) and the combined status of
   that head as GitHub returns them. Green is every check run read, every one
   ``completed``, at least one with conclusion ``success`` and none with a
   conclusion other than ``success``, ``skipped`` or ``neutral``; and either no
   commit status at all or every commit status ``success``.
4. ``beat_d["issue_after_merge"]`` (ticket A, read after the merge: ``state`` and
   ``closed_at``) must be closed within :data:`PROMOTION_WITHIN` of the merge, and
   ``beat_d["ticket_comments_after_merge"]`` must hold an App comment with
   :data:`MERGED_NOTICE_PREFIX` and the pull request URL.
5. ``beat_f["diff_added_lines"]`` must add no float assertion other than
   ``assertAlmostEqual(..., places=6)``: see :func:`float_equality_assertions`.
"""

from __future__ import annotations

import importlib.util
import io
import json
import re
import tokenize
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType
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
# The questions comment's marker as the GitHub adapter posts it: the asking
# request's UUID after the prefix (``curie_api.factory_questions.marker_for``),
# in a hidden HTML comment (``GitHubMarkedComments.embed``); a test pins both.
QUESTIONS_MARKER_LINE = re.compile(
    "<!-- "
    + re.escape(QUESTIONS_MARKER)
    + r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) -->"
)
QUESTIONS_LABEL = "curie-factory:questions"


def _decisions_grammar() -> ModuleType:
    """The default bundle's Decisions grammar (``hooks/decisions_section.py``).

    Its review gate refuses a publication whose Decisions section does not
    parse, and its reproduction gate reads answer citations with the same
    boundary, so the judge reads the section through that module instead of a
    heading rule of its own.
    """

    path = Path(__file__).resolve().parents[2] / "examples/dark-factory/hooks/decisions_section.py"
    spec = importlib.util.spec_from_file_location("factory_decisions_grammar", path)
    if spec is None or spec.loader is None:  # pragma: no cover -- a source checkout has it
        raise ImportError(f"cannot load the Decisions grammar from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DECISIONS_GRAMMAR = _decisions_grammar()
DECISIONS_HEADING = "## Decisions"
ANSWERED_PREFIX: str = DECISIONS_GRAMMAR.CITATION_PREFIX
# Mirror of curie_api.factory_questions.ANSWER_RECEIVED_LINE (a test pins equality):
# the first line of the status comment of a request an answer started.
ANSWER_RECEIVED_LINE = "Answer received. The factory is working on this ticket again."
# Mirror of the line curie_api.factory_notices writes once the pull request merged.
MERGED_NOTICE_PREFIX = "Pull request merged: "
# Check run conclusions (GitHub REST check runs): ``success`` is green; a run
# ``skipped`` or ``neutral`` did not execute a check, so it neither satisfies
# nor fails the head; every other conclusion fails it.
PASSING_CONCLUSION = "success"
NEUTRAL_CONCLUSIONS = frozenset({"skipped", "neutral"})
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
RULE_LABEL = "R1"
ACCEPTED_STATE = "accepted"
REMOVED_STATE = "removed"
# ASSUMED (#5797 decision 3): the CLI refusal codes of a reader's accept.
READER_REFUSAL_CODES = frozenset(
    {"memory_rules.write_access_required", "memory_rules.write_access_unverifiable"}
)
# ASSUMED (#5804 decision 3): the stable bundle refusal code.
BUNDLE_RETIRED_CODE = "bundle.result_declaration_retired"
# Request states that mean a run is queued or in progress: the live states of
# ``execution_requests_status_ck`` (models.py); ``waiting`` is queued on capacity.
ACTIVE_REQUEST_STATES = frozenset({"queued", "waiting", "running", "cancellation_requested"})
# Mirror of curie_api.factory_questions.QUESTIONS_ASKED (a test pins equality):
# the terminal cause of a ``completed`` request that posted questions.
QUESTIONS_ASKED = "questions_asked"
# The keys of one request record the judge reads, each a column of
# ``execution_requests`` and a key of ``request_view`` (a test pins both).
REQUEST_KEYS = ("id", "work_item_id", "status", "created_at", "terminal_at", "terminal_cause")
UNBOUND_LABEL_PREFIX = "factory-unbound-"
# One model usage sample before beat (a) and one after each beat (decision 8).
SPEND_SAMPLE_LABELS = ("before_a", *(f"after_{beat}" for beat in "abcdefgh"))
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
    """The tool names a request's turns recorded in the thread transcript.

    The driver reads them from the stored transcript of the work item's thread
    (``live_github_chain.request_tool_calls``); a stored rule proposal row is
    no record of the call.
    """

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
    """The changed rule of an accept, reject or remove's JSON.

    #5852 (``cli/src/commands/memory_rules.rs``) prints the API's
    ``MemoryRuleDecisionOut``: ``{"rule": {...}, "applies_from": "next_ticket"}``.
    """

    rule: Mapping[str, Any] = output["rule"]
    return rule


def principal_of(actor: Mapping[str, Any] | None) -> str:
    """ASSUMED (#5797 decision 1): ``accepted_by`` is ``{principal_id, display_name}``."""

    return str((actor or {}).get("principal_id", ""))


def work_item_rules_of(work_item: Mapping[str, Any]) -> Mapping[str, Any]:
    """ASSUMED (#5798 decision 2): ``memory_rules`` of ``curie cluster work-items <id> --json``."""

    rules: Mapping[str, Any] = work_item["memory_rules"]
    return rules


_BUNDLE_CODE = re.compile(r"(?<![\w.])bundle\.[a-z][a-z0-9_]*(?![\w.])")


def deploy_refusal_codes(output: Any) -> set[str]:
    """Every refusal code of a deploy's ``--json`` output, at any depth.

    ASSUMED (#5804 decision 3) for the code itself. The CLI renders an API
    refusal of the bundle upload inside the ``error`` string of its
    ``{"error", "fix"}`` envelope (``cli/src/api.rs`` ``expect_ok``:
    ``"{what} failed with {status}: {body}"``; ``cli/src/exit.rs``
    ``error_json``), so a ``bundle.<code>`` inside any string value counts too,
    as does a ``code`` key of structured output.
    """

    codes: set[str] = set()
    if isinstance(output, Mapping):
        if isinstance(output.get("code"), str):
            codes.add(output["code"])
        for value in output.values():
            codes |= deploy_refusal_codes(value)
    elif isinstance(output, list):
        for value in output:
            codes |= deploy_refusal_codes(value)
    elif isinstance(output, str):
        codes |= set(_BUNDLE_CODE.findall(output))
    return codes


def added_text_of(added_lines: Sequence[str]) -> str:
    return "\n".join(added_lines)


def _code_tokens(added_text: str) -> list[tokenize.TokenInfo]:
    """Python tokens of added lines, comments and strings kept as single tokens.

    The lines are a diff fragment, so indentation is dropped and an unfinished
    statement at the end is tolerated: the tokens read so far are returned.
    """

    source = "\n".join(line.strip() for line in added_text.splitlines()) + "\n"
    tokens: list[tokenize.TokenInfo] = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            tokens.append(token)
    except (tokenize.TokenError, IndentationError, SyntaxError):
        pass
    return tokens


def almost_equal_call_flags(added_text: str) -> list[bool]:
    """One flag per ``assertAlmostEqual(`` call in the added code: does it pass ``places=6``?

    A call in a comment or a string is not a call, and ``places=6`` counts only
    as a keyword argument of that call (``msg="places=6"`` does not).
    """

    tokens = [t for t in _code_tokens(added_text) if t.type not in _IGNORED_TOKENS]
    flags: list[bool] = []
    for index, token in enumerate(tokens[:-1]):
        if token.type != tokenize.NAME or token.string != "assertAlmostEqual":
            continue
        if tokens[index + 1].string != "(" or (index and tokens[index - 1].string == "def"):
            continue
        depth = 1
        arguments: list[list[tokenize.TokenInfo]] = []
        current: list[tokenize.TokenInfo] = []
        for inner in tokens[index + 2 :]:
            if inner.type == tokenize.OP and inner.string in {"(", "[", "{"}:
                depth += 1
            elif inner.type == tokenize.OP and inner.string in {")", "]", "}"}:
                depth -= 1
                if depth == 0:
                    break
            if depth == 1 and inner.type == tokenize.OP and inner.string == ",":
                arguments.append(current)
                current = []
            else:
                current.append(inner)
        arguments.append(current)
        flags.append(any(_is_places_six(argument) for argument in arguments))
    return flags


_IGNORED_TOKENS = {
    tokenize.COMMENT,
    tokenize.NL,
    tokenize.NEWLINE,
    tokenize.INDENT,
    tokenize.DEDENT,
    tokenize.ENDMARKER,
}
_NON_CODE_TOKENS = _IGNORED_TOKENS - {tokenize.NEWLINE}


_EQUALITY_ASSERTS = frozenset({"assertEqual", "assertEquals", "assertNotEqual"})


def _is_float_literal(token: tokenize.TokenInfo) -> bool:
    if token.type != tokenize.NUMBER:
        return False
    text = token.string.lower()
    return not text.startswith(("0x", "0o", "0b")) and ("." in text or "e" in text)


def _call_arguments(tokens: Sequence[tokenize.TokenInfo]) -> list[tokenize.TokenInfo]:
    """The tokens inside the parentheses that open at ``tokens[0]``, nested ones included."""

    depth = 0
    inside: list[tokenize.TokenInfo] = []
    for token in tokens:
        if token.type == tokenize.OP and token.string in {"(", "[", "{"}:
            depth += 1
            if depth == 1:
                continue
        elif token.type == tokenize.OP and token.string in {")", "]", "}"}:
            depth -= 1
            if depth == 0:
                break
        inside.append(token)
    return inside


def float_equality_assertions(added_text: str) -> list[str]:
    """Each added float assertion that is not ``assertAlmostEqual``, as its call name.

    An ``assertEqual(``, ``assertEquals(`` or ``assertNotEqual(`` call with a float
    literal among its arguments (a number with a ``.`` or an exponent), and a bare
    ``assert`` statement comparing with ``==`` or ``!=`` to a float literal, break
    the review rule. So does such a call or statement naming a variable the added
    code assigns a float literal (``expected = 33.0``, a unary minus included)
    or a converter call (``actual = convert(100, "c", "de")``), or calling the
    fixture's converter, which returns a float (``convert(0, "c", "de")``,
    ``unitconv.convert(...)`` or a name imported from it). Comments and strings
    never count.
    """

    # NEWLINE is kept: it ends a bare ``assert`` statement.
    tokens = [t for t in _code_tokens(added_text) if t.type not in _NON_CODE_TOKENS]
    functions, modules = _converter_names(tokens)

    def converter_call(argument: Sequence[tokenize.TokenInfo], i: int) -> bool:
        return _is_converter_call(argument, i, functions, modules)

    float_names = _float_assigned_names(tokens) | _converter_assigned_names(tokens, converter_call)

    def floaty(argument: Sequence[tokenize.TokenInfo]) -> bool:
        return any(
            _is_float_literal(t)
            or (
                t.type == tokenize.NAME
                and t.string in float_names
                and not (i and argument[i - 1].string == ".")
            )
            or (t.type == tokenize.NAME and converter_call(argument, i))
            for i, t in enumerate(argument)
        )

    found: list[str] = []
    for index, token in enumerate(tokens):
        if token.type != tokenize.NAME:
            continue
        if token.string in _EQUALITY_ASSERTS:
            following = tokens[index + 1 : index + 2]
            if not following or following[0].string != "(":
                continue
            if index and tokens[index - 1].string == "def":
                continue
            if floaty(_call_arguments(tokens[index + 1 :])):
                found.append(token.string)
        elif token.string == "assert":
            statement: list[tokenize.TokenInfo] = []
            for inner in tokens[index + 1 :]:
                if inner.type == tokenize.NEWLINE:
                    break
                statement.append(inner)
            compares = any(t.type == tokenize.OP and t.string in {"==", "!="} for t in statement)
            if compares and floaty(statement):
                found.append("assert")
    return found


def _float_assigned_names(tokens: Sequence[tokenize.TokenInfo]) -> set[str]:
    """Names a statement of the added code assigns a float literal: ``x = 33.0``, ``x = -1e3``."""

    names: set[str] = set()
    for index, token in enumerate(tokens):
        starts = (
            index == 0
            or tokens[index - 1].type == tokenize.NEWLINE
            or (tokens[index - 1].string == ";")
        )
        if token.type != tokenize.NAME or not starts:
            continue
        rest = [t.string for t in tokens[index + 1 : index + 2]]
        if rest != ["="]:
            continue
        value = list(tokens[index + 2 : index + 4])
        if value and value[0].type == tokenize.OP and value[0].string in {"-", "+"}:
            value = value[1:] + list(tokens[index + 4 : index + 5])
        if not value or not _is_float_literal(value[0]):
            continue
        after = value[1:2]
        if (
            not after
            or after[0].type in {tokenize.NEWLINE, tokenize.ENDMARKER}
            or (after[0].string == ";")
        ):
            names.add(token.string)
    return names


def _converter_assigned_names(
    tokens: Sequence[tokenize.TokenInfo],
    converter_call: Callable[[Sequence[tokenize.TokenInfo], int], bool],
) -> set[str]:
    """Names a statement of the added code binds to a converter call.

    ``actual = convert(100, "c", "de")`` and ``actual = unitconv.convert(...)``
    bind ``actual`` to the converter's float result.
    """

    names: set[str] = set()
    for index, token in enumerate(tokens):
        starts = (
            index == 0
            or tokens[index - 1].type == tokenize.NEWLINE
            or (tokens[index - 1].string == ";")
        )
        if token.type != tokenize.NAME or not starts:
            continue
        if [t.string for t in tokens[index + 1 : index + 2]] != ["="]:
            continue
        value = list(tokens[index + 2 : index + 6])
        # The callee is the value's first name, or ``module.convert``.
        dotted = len(value) > 2 and value[1].string == "."
        if value and value[0].type == tokenize.NAME and converter_call(value, 2 if dotted else 0):
            names.add(token.string)
    return names


def _is_converter_call(
    argument: Sequence[tokenize.TokenInfo], i: int, functions: set[str], modules: set[str]
) -> bool:
    """Whether ``argument[i]`` opens a call of the fixture converter."""

    if i + 1 >= len(argument) or argument[i + 1].string != "(":
        return False
    dotted = i > 0 and argument[i - 1].string == "."
    if not dotted:
        return argument[i].string in functions
    owner = argument[i - 2].string if i > 1 else ""
    return argument[i].string == CONVERTER_FUNCTION and owner in modules


# The fixture's converter (``fixtures/unitconv/unitconv/convert.py``): ``convert``
# returns a float, so an exact equality on its result is a float assertion.
CONVERTER_PACKAGE = "unitconv"
CONVERTER_FUNCTION = "convert"


def _converter_names(tokens: Sequence[tokenize.TokenInfo]) -> tuple[set[str], set[str]]:
    """The names that call the fixture converter, and the names of its modules.

    ``convert`` and the package ``unitconv`` always count; an added
    ``from unitconv[.convert] import convert [as c]`` adds ``c``, and an added
    ``import unitconv[.convert] as m`` adds ``m`` as a module.
    """

    functions = {CONVERTER_FUNCTION}
    modules = {CONVERTER_PACKAGE, CONVERTER_FUNCTION}
    strings = [t.string for t in tokens]
    for index, word in enumerate(strings):
        if word not in {"from", "import"}:
            continue
        if index + 1 >= len(strings) or strings[index + 1] != CONVERTER_PACKAGE:
            continue
        end = index + 1
        while end < len(tokens) and tokens[end].type != tokenize.NEWLINE:
            end += 1
        statement = strings[index + 1 : end]
        if word == "import":
            if "as" in statement:
                modules.add(statement[statement.index("as") + 1])
            continue
        if "import" not in statement:
            continue
        imported = [
            name
            for name in statement[statement.index("import") + 1 :]
            if name not in {"(", ")", ","}
        ]
        for at, name in enumerate(imported):
            if name != CONVERTER_FUNCTION or (at and imported[at - 1] == "as"):
                continue
            alias = (
                imported[at + 2] if at + 2 < len(imported) and imported[at + 1] == "as" else name
            )
            # ``from unitconv import convert as c`` may name the module or the function.
            functions.add(alias)
            modules.add(alias)
    return functions, modules


def _is_places_six(argument: Sequence[tokenize.TokenInfo]) -> bool:
    shape = [(t.type, t.string) for t in argument]
    return shape == [(tokenize.NAME, "places"), (tokenize.OP, "="), (tokenize.NUMBER, "6")]


def section_lines(body: str, heading: str) -> list[str]:
    """Lines under a markdown heading named ``heading``, up to the next heading."""

    lines, inside = [], False
    for line in (body or "").splitlines():
        found = re.match(r"^\s{0,3}#{1,6}\s*(.*?)\s*#*\s*$", line)
        if found:
            inside = found.group(1).lower() == heading.lower()
        elif inside:
            lines.append(line)
    return lines


def answer_citations(body: Any) -> tuple[list[str], str | None]:
    """The answer citations of a publication's Decisions section, and why it is
    refused when the bundle's grammar refuses it.

    The section is the one the grammar parses: the lines after the single
    exact ``## Decisions`` line, up to the next ``## `` line or the exact
    ``---`` line the platform writes before its own text. A citation below that
    line is outside the section and does not count.
    """

    try:
        DECISIONS_GRAMMAR.parse_decisions_section(body)
    except ValueError as exc:
        return [], str(exc)
    lines = str(body).splitlines()
    start = lines.index(DECISIONS_HEADING) + 1
    citations: list[str] = []
    for line in lines[start:]:
        if line.startswith("## ") or line == DECISIONS_GRAMMAR.SECTION_END:
            break
        if line.startswith(ANSWERED_PREFIX) and line[len(ANSWERED_PREFIX) :].strip():
            citations.append(line)
    return citations, None


def _lines_starting(body: str, prefix: str) -> list[str]:
    return [line for line in (body or "").splitlines() if line.lstrip("-*• ").startswith(prefix)]


def _closes(body: str, issue: int) -> bool:
    return re.search(rf"\bCloses #{issue}(?!\d)", body or "") is not None


def _by_app(item: Mapping[str, Any], ids: Mapping[str, Any]) -> bool:
    return bool(item["user"]["login"] == ids["app_login"])


def _touched_at(comment: Mapping[str, Any]) -> datetime:
    """When a comment last read as it does: ``updated_at`` (edited in place) or ``created_at``."""

    return parse_time(comment.get("updated_at") or comment["created_at"])


def _app_comments_since(
    comments: Sequence[Mapping[str, Any]], ids: Mapping[str, Any], since: Any, text: str
) -> list[Mapping[str, Any]]:
    """App comments holding ``text`` that were created or updated at or after ``since``."""

    start = parse_time(since)
    return [
        c
        for c in comments
        if _by_app(c, ids) and text in str(c["body"]) and _touched_at(c) >= start
    ]


def checks_green(checks: Mapping[str, Any], sha: str) -> list[str]:
    """Why a head's checks are not green, or nothing when they are.

    ``check_runs`` is the body of ``GET /repos/{r}/commits/{sha}/check-runs``
    (https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference)
    and ``status`` the body of ``GET /repos/{r}/commits/{sha}/status``
    (https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference).
    """

    reasons: list[str] = []
    if checks["sha"] != sha:
        reasons.append("the checks were read for another commit than the revision head")
    runs = list(checks["check_runs"]["check_runs"])
    total = checks["check_runs"]["total_count"]
    if len(runs) != total:
        reasons.append(f"read {len(runs)} of {total} check runs of the revision head")
    if not runs:
        reasons.append("the revision head has no check run")
    pending = [r["name"] for r in runs if r["status"] != "completed"]
    if pending:
        reasons.append(f"check runs not completed on the revision head: {pending}")
    completed = [r for r in runs if r["status"] == "completed"]
    failing = [
        r["name"]
        for r in completed
        if r["conclusion"] != PASSING_CONCLUSION and r["conclusion"] not in NEUTRAL_CONCLUSIONS
    ]
    if failing:
        reasons.append(f"check runs not passing on the revision head: {failing}")
    if not [r for r in completed if r["conclusion"] == PASSING_CONCLUSION]:
        reasons.append("no check run on the revision head executed with a success conclusion")
    status = checks["status"]
    statuses = list(status["statuses"])
    if status["total_count"] == 0:
        if statuses:
            reasons.append("the combined status lists commit statuses but counts none")
    elif status["state"] != "success" or any(s["state"] != "success" for s in statuses):
        reasons.append(f"the combined status of the revision head is {status['state']}")
    return reasons


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


def _mine(requests: Sequence[Mapping[str, Any]], work_item: str) -> list[Mapping[str, Any]]:
    """The requests of ticket A's work item; another ticket's requests never count."""

    return [r for r in requests if r["work_item_id"] == work_item]


def _active_at(request: Mapping[str, Any], moment: Any) -> bool:
    """A request is active from ``created_at`` until ``terminal_at`` (None: still)."""

    if parse_time(request["created_at"]) > parse_time(moment):
        return False
    terminal = request["terminal_at"]
    return terminal is None or parse_time(terminal) > parse_time(moment)


def asking_request_id(body: str) -> str | None:
    """The UUID of the request the questions comment's marker names, or None."""

    found = QUESTIONS_MARKER_LINE.search(body or "")
    return found.group(1) if found else None


def _request_keys_missing(requests: Sequence[Mapping[str, Any]]) -> list[str]:
    """The :data:`REQUEST_KEYS` some request record lacks, in order."""

    return [key for key in REQUEST_KEYS if any(key not in r for r in requests)]


def _judge_beat_a(b: _Beat, rec: Mapping[str, Any]) -> None:
    a, ids = rec["beat_a"], rec["identities"]
    b.check(rec["agent"]["memory_rule_proposals"] is True, "rule proposals were not turned on")
    label_at = a["labelled_at"]
    # The status comment is edited in place (``factory_notices._sync_comment``),
    # so its investigating lead is judged from the first read that showed it,
    # not from the later snapshot, where the body may already be terminal.
    seen = a["investigating"]
    lead = seen["comment"] if isinstance(seen, Mapping) else None
    if b.check(
        lead is not None and _by_app(lead, ids) and lead["body"].startswith(STATUS_COMMENT_PREFIX),
        "no investigating status comment was observed",
    ):
        assert lead is not None
        b.within(
            "status_seconds",
            label_at,
            lead["created_at"],
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
    missing = _request_keys_missing(quiet["requests"])
    if not b.check(not missing, f"a quiet snapshot request lacks {missing}"):
        return
    requests = _mine(quiet["requests"], a["work_item_id"])
    b.check(
        len(requests) == len(quiet["requests"]), "the snapshot holds another work item's request"
    )
    if b.check(bool(requests), "the quiet snapshot holds no request of ticket A"):
        # The questions comment is bound to the request its marker names.
        asker_id = asking_request_id(question["body"])
        askers = [r for r in requests if str(r["id"]) == asker_id]
        if b.check(len(askers) == 1, "the questions comment's marker names no request of ticket A"):
            asker = askers[0]
            b.check(
                asker["status"] == "completed" and asker["terminal_cause"] == QUESTIONS_ASKED,
                "ticket A's question request did not end completed with cause questions_asked "
                f"(it ended {asker['status']} with cause {asker['terminal_cause']})",
            )
            later = [
                r["id"]
                for r in requests
                if parse_time(r["created_at"]) > parse_time(asker["created_at"])
            ]
            b.check(not later, "a request of ticket A started after its question request")
    deadline = parse_time(question["created_at"]) + timedelta(seconds=QUIET_AFTER_QUESTIONS)
    b.check(
        b.elapsed("quiet_wait_seconds", question["created_at"], quiet["observed_at"])
        >= QUIET_AFTER_QUESTIONS,
        "the work item was read before the quiet period elapsed",
    )
    b.check(
        not [r["id"] for r in requests if _active_at(r, deadline)],
        "a request was still queued or running 60 seconds after the questions",
    )
    active = [r["id"] for r in requests if r["status"] in ACTIVE_REQUEST_STATES]
    b.check(not active, "a request is still queued or running after the questions")


def _judge_beat_b(b: _Beat, rec: Mapping[str, Any]) -> None:
    beat, ids, a = rec["beat_b"], rec["identities"], rec["beat_a"]
    issue = a["issue_number"]
    answer = beat["answer"]
    b.check(answer["issue_number"] == issue, "the answer is not on ticket A")
    b.check(answer["user"]["id"] == ids["writer_id"], "the answer was not posted by the writer")
    b.check(answer["body"].strip() == ANSWER_TEXT, "the answer text is not the fixture answer")
    b.check("@" not in answer["body"], "the answer mentions an account")
    requests = _mine(beat["requests"], a["work_item_id"])
    earlier = {r["id"] for r in a["quiet_snapshot"]["requests"]}
    b.check(earlier <= {r["id"] for r in requests}, "the request history of ticket A is missing")
    restarts = [
        r for r in requests if parse_time(r["created_at"]) >= parse_time(answer["created_at"])
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
        citations, refused = answer_citations(pull["body"])
        if b.check(refused is None, f"the Decisions section does not parse: {refused}"):
            b.check(
                bool(citations),
                "the Decisions section lacks an `Answered on the ticket by ` line",
            )
    others = [i["number"] for i in beat["issues"] if _by_app(i, ids)]
    b.check(not others, f"the App opened issues: {others}")
    acks = _app_comments_since(
        beat["ticket_comments"], ids, answer["created_at"], ANSWER_RECEIVED_LINE
    )
    b.check(bool(acks), "no App comment on ticket A acknowledges the answer")


def _judge_beat_c(b: _Beat, rec: Mapping[str, Any]) -> None:
    c, ids, a = rec["beat_c"], rec["identities"], rec["beat_a"]
    number = c["pull_number"]
    post, body = c["post"], c["post"]["body"]
    pull = rec["beat_b"]["open_pulls"][0]
    b.check(number == pull["number"], "the review is on another pull request than ticket A's")
    b.check(c["pull_before"]["head"] == pull["head"], "the review head is not ticket A's pull head")
    path = rf"/?repos/{re.escape(ids['repository_path'])}/pulls/{number}/reviews"
    b.check(
        post["method"] == "POST" and re.fullmatch(path, post["path"]) is not None,
        "the feedback was not posted as a pull request review on the fixture repository",
    )
    b.check(body["commit_id"] == c["pull_before"]["head"]["sha"], "the review names another head")
    b.check(body["event"] == "COMMENT", "the review event is not COMMENT")
    b.check(body["body"] == "", "the review has a body")
    posted = body["comments"]
    read = c["review_comments"]
    b.check(len(posted) == 1, f"the posted review carries {len(posted)} inline comments")
    b.check(len(read) == 1, f"the review reads back {len(read)} inline comments")
    if len(posted) != 1 or len(read) != 1:
        return
    comment, sent = read[0], posted[0]
    b.check(
        sent["body"].strip() == REVIEW_RULE_TEXT, "the posted inline comment is not the rule text"
    )
    b.check(
        (sent["path"], sent["line"], sent["body"])
        == (comment["path"], comment["line"], comment["body"]),
        "the posted and the read back inline comment differ",
    )
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
    b.check(
        review["pull_request_url"].endswith(f"/{ids['repository_path']}/pulls/{number}")
        and comment["pull_request_review_id"] == review["id"],
        "the review or its comment belongs to another pull request or review",
    )
    submitted = review["submitted_at"]
    requests = _mine(c["requests"], a["work_item_id"])
    earlier = {r["id"] for r in rec["beat_b"]["requests"]}
    b.check(earlier <= {r["id"] for r in requests}, "the request history of ticket A is missing")
    after = [r for r in requests if parse_time(r["created_at"]) >= parse_time(submitted)]
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
    replies = _app_comments_since(c["pull_comments"], ids, submitted, now["sha"])
    b.check(bool(replies), "no App reply on the pull request names the revision commit")
    for reason in checks_green(c["revision_checks"], now["sha"]):
        b.check(False, reason)


def _judge_beat_d(b: _Beat, rec: Mapping[str, Any]) -> None:
    d, ids = rec["beat_d"], rec["identities"]
    _judge_merge_closure(b, rec)
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
    b.check(rules[0]["label"] == RULE_LABEL, f"the rule is not {RULE_LABEL}")


def _judge_merge_closure(b: _Beat, rec: Mapping[str, Any]) -> None:
    """Ticket A closed with the merge and carries the merge notice (decision 7.4)."""

    d, ids = rec["beat_d"], rec["identities"]
    issue = d["issue_after_merge"]
    if b.check(issue["state"] == "closed", "ticket A is not closed after the merge"):
        b.within(
            "issue_closed_seconds",
            d["merged_at"],
            issue["closed_at"],
            PROMOTION_WITHIN,
            "ticket A was not closed with the merge",
        )
    pull_url = rec["beat_b"]["open_pulls"][0]["html_url"]
    notices = _app_comments_since(
        d["ticket_comments_after_merge"], ids, d["merged_at"], MERGED_NOTICE_PREFIX + pull_url
    )
    b.check(bool(notices), "no App comment on ticket A names the merged pull request")


def _judge_beat_e(b: _Beat, rec: Mapping[str, Any]) -> None:
    e = rec["beat_e"]
    rule_id = e["rule_before"]["id"]
    promoted = [r["id"] for r in rules_of(rec["beat_d"]["rules_after_merge"]["output"])]
    b.check(not promoted or rule_id in promoted, "the rule acted on is not the promoted rule")
    b.check(
        e["reader_permission"] not in WRITE_PERMISSIONS, "the reader can write to the repository"
    )
    reader, writer = e["reader_command"], e["writer_command"]
    b.check(reader["rule_id"] == rule_id, "the reader's accept names another rule")
    b.check(writer["rule_id"] == rule_id, "the writer's accept names another rule")
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
    b.check(rule.get("id") == rule_id, "the writer's accept changed another rule")
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
    added = added_text_of(f["diff_added_lines"])
    flags = almost_equal_call_flags(added)
    if b.check(bool(flags), "the diff adds no assertAlmostEqual( call"):
        b.check(all(flags), f"{flags.count(False)} added assertAlmostEqual( calls lack places=6")
    conflicting = float_equality_assertions(added)
    b.check(not conflicting, f"the diff adds float assertions without places=6: {conflicting}")


def _judge_beat_g(b: _Beat, rec: Mapping[str, Any]) -> None:
    g = rec["beat_g"]
    removal = g["remove_command"]
    rule_id = rec["beat_e"]["rule_before"]["id"]
    b.check(removal["rule_id"] == rule_id, "the removal names another rule than the accepted one")
    b.check(removal["exit_code"] == 0, "the writer's removal did not exit 0")
    rule = accepted_rule_of(removal["output"])
    b.check(rule.get("id") == rule_id, "the removal changed another rule")
    b.check(rule.get("state") == REMOVED_STATE, "the removal did not end removed")
    b.check(
        principal_of(rule.get("removed_by")) == rec["session_probe"]["writer"]["principal_id"],
        "the rule was not removed by the writer principal",
    )
    b.check(
        b.elapsed("removed_before_ticket_c_seconds", rule["removed_at"], g["labelled_at"]) >= 0,
        "the rule was removed after ticket C was labelled",
    )
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
    label = str(h["label"])
    b.check(label.startswith(UNBOUND_LABEL_PREFIX), "the label is not an unbound factory label")
    applied = [
        e
        for e in h["label_events"]
        if e["event"] == "labeled"
        and e["label"]["name"] == label
        and e["actor"]["id"] == ids["writer_id"]
    ]
    if not b.check(len(applied) == 1, f"the unbound label was applied {len(applied)} times"):
        return
    b.check(
        b.elapsed("quiet_wait_seconds", applied[0]["created_at"], h["observed_at"])
        >= UNBOUND_QUIET_WINDOW,
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
    # The serving version is the one the worker boots: the agent's active
    # deployment, prod first, then the newest (``live_github_chain.serving_version``).
    before, after = deploy["version_before"], deploy["version_after"]
    b.check(bool(before), "no serving version before the refused deploy")
    b.check(bool(after), "no serving version after the refused deploy")
    b.check(before == after, "the in force version changed after the refused deploy")


def _judge_spend(b: _Beat, rec: Mapping[str, Any]) -> None:
    samples = {s["label"]: s["usage"] for s in rec["usage"]["samples"]}
    missing = [
        label for label in SPEND_SAMPLE_LABELS if not isinstance(samples.get(label), (int, float))
    ]
    if not b.check(not missing, f"the model usage was not measured at: {missing}"):
        return
    base = float(samples[SPEND_SAMPLE_LABELS[0]])
    deltas = {label: float(samples[label]) - base for label in SPEND_SAMPLE_LABELS}
    over = [label for label, delta in deltas.items() if delta >= SPEND_CAP_USD]
    b.measures["delta_usd"] = max(deltas.values())
    b.check(not over, f"the usage delta reached {SPEND_CAP_USD:.0f} USD at {over}")


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
    "binary": "<curie-bin>",
    "cloudflared": "<cloudflared>",
    "restore-url": "<restore-url>",
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


def _char_pattern(char: str) -> str:
    """One character, literally or percent-encoded (``%2d``, ``%39``), as in URLs and queries."""

    encoded = "".join(f"%{byte:02x}" for byte in char.encode())
    options = [re.escape(char), encoded]
    if char == " ":
        options.append(r"\+")
    return "(?:" + "|".join(options) + ")"


def _pattern(token: str) -> str:
    body = "".join(_char_pattern(char) for char in token)
    # A numeric id matches as a whole number, never inside a longer one, written
    # in digits or as percent-encoded digits.
    return rf"(?<!\d)(?<!%3\d){body}(?!\d)(?!%3\d)" if token.isdigit() else body


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
