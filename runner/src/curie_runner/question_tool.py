"""The ``ask_questions`` tool (#5785): a factory run asks a person on its own ticket.

A run that cannot build its ticket without answers from a person posts its
summary and numbered questions on the ticket it is working. The platform posts
them and this session then ends; the answers arrive as a later run. A ticket
has three question rounds in all.

This module owns the schema, the bounds check and the handler and stays free
of the SDK (ADR 0140); `curie_runner.harness.claude.question_tool` wraps the
handler as the SDK tool. The post, the body decoding and the refusal code
reading are the helpers `curie_runner.completion_contract` already owns, so the two
tools share one copy.

The bounds are checked here before any request, so a malformed call costs no
round and reaches no endpoint.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from typing import Any

import aiohttp

from .completion_contract import (
    _CODE_RE,
    ResultHandler,
    _code,
    _decode,
    _error,
    _ok,
    _post,
    _TooLarge,
)

logger = logging.getLogger(__name__)

SUMMARY_MAX_CHARS = 4_000
QUESTIONS_MAX = 10
QUESTION_MAX_CHARS = 1_000
ROUNDS_LIMIT = 3
ROUNDS_EXHAUSTED_CODE = "question_rounds_exhausted"
INVALID_QUESTIONS_CODE = "invalid_questions"

_QUESTIONS_TIMEOUT_SECONDS = 45
_QUESTIONS_MAX_BYTES = 65_536
# Every line break form a renderer or Python honors, CRLF first so it stays
# one break; the API normalizes the same set before its blank-line rule.
_LINE_BREAK = re.compile(r"\r\n|[\r\n\v\f\x85\u2028\u2029]")

QUESTIONS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "What the investigation found, including a reproduction result "
            "when there is one: 1 to 4,000 characters, not only whitespace.",
        },
        "questions": {
            "type": "array",
            "description": "The questions for the person, 1 to 10 of them.",
            "minItems": 1,
            "maxItems": 10,
            "items": {
                "type": "string",
                "description": "One question: 1 to 1,000 characters, not only whitespace, "
                "and no blank line inside it.",
            },
        },
    },
    "required": ["summary", "questions"],
    "additionalProperties": False,
}

QUESTIONS_DESCRIPTION = (
    "Ask a person questions on this run's own ticket. Call this only when the "
    "ticket cannot be built without answers from a person. Give a summary of "
    "what the investigation found, including a reproduction result when there "
    "is one, and 1 to 10 questions that the platform posts as a numbered list. "
    "The platform posts them on this run's own ticket and this session then "
    "ends: make no further changes and do not open a pull request. A run may "
    "ask once and never after it published."
)


def _visible(text: str) -> str:
    """``text`` as the API posts it: each line break a newline, with no control
    (tab kept) or invisible format character, trimmed. A line holding only a
    form feed or a zero-width space is blank once posted, so it is blank here."""

    kept = (
        char
        for char in _LINE_BREAK.sub("\n", text)
        if char in "\n\t" or unicodedata.category(char) not in ("Cc", "Cf", "Cs")
    )
    return "".join(kept).strip()


def question_input_issues(args: Any) -> list[str]:
    """The bounds ``args`` violates, as plain sentences; empty when it is valid."""

    if not isinstance(args, dict):
        return ["the input must be an object with summary and questions"]
    issues: list[str] = []
    extra = sorted(str(key) for key in args if key not in {"summary", "questions"})
    if extra:
        issues.append("only summary and questions are allowed, found other keys")
    summary = args.get("summary")
    if not isinstance(summary, str):
        issues.append("summary must be a string")
    elif not _visible(summary):
        issues.append("summary must not be empty or only whitespace")
    elif len(summary) > SUMMARY_MAX_CHARS:
        issues.append(f"summary must be at most {SUMMARY_MAX_CHARS:,} characters")
    questions = args.get("questions")
    if not isinstance(questions, list):
        issues.append("questions must be a list of strings")
        return issues
    if not questions:
        issues.append(f"questions must hold 1 to {QUESTIONS_MAX} entries, found none")
    elif len(questions) > QUESTIONS_MAX:
        issues.append(f"questions must hold 1 to {QUESTIONS_MAX} entries")
    for number, question in enumerate(questions[:QUESTIONS_MAX], start=1):
        if not isinstance(question, str):
            issues.append(f"question {number} must be a string")
        elif not _visible(question):
            issues.append(f"question {number} must not be empty or only whitespace")
        elif len(question) > QUESTION_MAX_CHARS:
            issues.append(f"question {number} must be at most {QUESTION_MAX_CHARS:,} characters")
        elif not all(line.strip() for line in _visible(question).split("\n")):
            issues.append(f"question {number} must not contain a blank line")
    return issues


def _render_posted(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return _error("The questions answer could not be read. Retry shortly.")
    number = payload.get("round")
    limit = payload.get("rounds_limit")
    if type(number) is not int or number < 1:
        return _error("The questions answer could not be read. Retry shortly.")
    if type(limit) is not int or limit < 1:
        limit = ROUNDS_LIMIT
    return _ok(
        f"The questions are posted on the ticket as round {number} of {limit}. "
        "This session ends now: stop working, make no further changes and end "
        "the turn without opening a pull request."
    )


def questions_handler(url: str, token: str) -> ResultHandler:
    """The handler of ``ask_questions``, closed over the route and capability.

    Calls run one at a time. The route lets a retry of a round still
    ``posting`` write again (the lost-response path), so two parallel calls
    from one turn could both reach the tracker; serialized, the second sees
    the posted round and replays it.
    """

    single_flight = asyncio.Lock()

    async def ask_questions(args: dict[str, Any]) -> dict[str, Any]:
        async with single_flight:
            return await _ask(args)

    async def _ask(args: dict[str, Any]) -> dict[str, Any]:
        issues = question_input_issues(args)
        if issues:
            return _error(
                "The questions were not posted, because the input is outside its bounds: "
                + "; ".join(issues)
                + ". Correct it and call again."
            )
        try:
            status, raw = await _post(
                url,
                token,
                {"summary": args["summary"], "questions": list(args["questions"])},
                timeout=_QUESTIONS_TIMEOUT_SECONDS,
                limit=_QUESTIONS_MAX_BYTES,
            )
        except (aiohttp.ClientError, TimeoutError, OSError, _TooLarge) as exc:
            # Never render a transport diagnostic: it can name the endpoint.
            logger.warning("ask_questions transport failure: %s", type(exc).__name__)
            return _error("The questions could not be posted right now. Retry shortly.")
        try:
            payload = _decode(raw)
        except ValueError:
            payload = None
        if status == 200:
            return _render_posted(payload)
        if status == 409:
            code = _code(payload)
            if code == ROUNDS_EXHAUSTED_CODE:
                return _error(
                    f"This ticket's {ROUNDS_LIMIT} question rounds are used, so no more "
                    "questions can be asked. End now with a final message that starts "
                    "with `Could not complete:` and says what is missing."
                )
            if code == INVALID_QUESTIONS_CODE:
                # The pre-check passed, so the platform's own bound on the
                # posted comment refused it: redaction or line breaks made it
                # too long.
                return _error(
                    "The questions were refused because the posted comment would be too "
                    "long. Shorten the summary and the questions, use fewer line breaks, "
                    "and leave out tokens and credentials, then call again."
                )
            if code is not None and _CODE_RE.match(code):
                return _error(f"The questions were refused: {code}.")
            return _error("The questions were refused.")
        if status == 503:
            return _error("The tracker could not take the questions right now. Retry shortly.")
        return _error(f"The questions were refused (status {status}).")

    return ask_questions
