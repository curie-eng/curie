"""Promote a Langfuse trace into an anonymized eval case (#259).

One click in the Runs view turns a real conversation into a runnable eval case
conforming to the frozen eval-case format (ADR-0019,
``apps/worker/schema/eval-cases.schema.json``): ``{id, input, grader}`` with a
deterministic ``{kind, expected, case_sensitive}`` grader. The models here are a
deliberate API-side mirror of the worker's frozen ``EvalCase``/``Grader`` shape
(the API never imports the worker package); the shape must not drift from that
schema.

Anonymization is the load-bearing part: the promoted case is derived from a real
Slack conversation, so emails, Slack ids, phone numbers, and credential-shaped
tokens are redacted out of both the input prompt and the expected output before
the case is emitted. The redaction is intentionally conservative -- it never
invents content, only masks recognizable identifiers with a stable placeholder.
"""

from __future__ import annotations

import json
import re
from typing import Any

from .schemas import EvalCaseOut, GraderOut

# The expected-output snippet is capped so a promoted grader keys off a salient
# line rather than an entire multi-paragraph answer (which would rarely match on
# re-run). The human refines it after promotion; this is a runnable starting
# point, not a final assertion.
_EXPECTED_MAX_LEN = 200

# Redaction patterns, applied in order. Emails and tokens are matched before the
# generic phone/number patterns so a digit-bearing email or key is masked whole
# rather than partially. Each maps a recognizable identifier to a stable
# placeholder so the anonymized text stays readable.
_REDACTIONS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "<email>"),
    # Slack-ish OAuth / bot tokens and API keys (xoxb-, sk-ant-, sk-...).
    (re.compile(r"\b(?:xox[bpasr]|sk(?:-[A-Za-z]+)?)-[A-Za-z0-9-]{6,}\b"), "<token>"),
    # Slack object ids: users U/W, channels C/G, DMs D. Uppercase alnum, >=8.
    (re.compile(r"\b[UWCGD][A-Z0-9]{7,}\b"), "<slack-id>"),
    # Phone numbers: an optional +, then 9+ digits possibly split by -, space, ().
    (re.compile(r"\+?\d(?:[\d\s().-]{7,})\d"), "<phone>"),
]


def redact(text: str) -> str:
    """Mask recognizable PII / secrets in ``text`` with stable placeholders."""
    for pattern, placeholder in _REDACTIONS:
        text = pattern.sub(placeholder, text)
    return text


def _coerce_text(value: Any) -> str:
    """Flatten a Langfuse input/output payload into a single text string.

    Handles the common shapes: a bare string; a chat-style list of
    ``{"role", "content"}`` messages (the last user message for an input, the
    last message for an output); a ``{"content": ...}`` dict; else a compact JSON
    dump so nothing is silently dropped.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        # Prefer the last message that carries text content.
        for msg in reversed(value):
            if isinstance(msg, dict) and msg.get("content"):
                return _coerce_text(msg["content"])
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        for key in ("content", "text", "output", "input", "value"):
            if value.get(key):
                return _coerce_text(value[key])
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _last_user_input(value: Any) -> str:
    """The last user-authored message from a chat-style input, else all of it."""
    if isinstance(value, list):
        for msg in reversed(value):
            if isinstance(msg, dict) and msg.get("role") == "user" and msg.get("content"):
                return _coerce_text(msg["content"])
    return _coerce_text(value)


def extract_io(trace: dict[str, Any], observations: list[dict[str, Any]]) -> tuple[str, str]:
    """Best-effort (input, output) text for a trace.

    Prefers the trace-level ``input``/``output`` fields; falls back to the first
    observation carrying an input (the prompt) and the last carrying an output
    (the answer) when the trace itself does not surface them.
    """
    input_text = _last_user_input(trace.get("input"))
    output_text = _coerce_text(trace.get("output"))

    if not input_text:
        for obs in observations:
            if isinstance(obs, dict) and obs.get("input"):
                input_text = _last_user_input(obs["input"])
                break
    if not output_text:
        for obs in reversed(observations):
            if isinstance(obs, dict) and obs.get("output"):
                output_text = _coerce_text(obs["output"])
                break
    return input_text, output_text


def _expected_snippet(output: str) -> str:
    """A salient, capped snippet of the (already-redacted) output for the grader.

    The first non-empty line, trimmed to the length cap. Empty when there is no
    output -- a ``contains`` grader on ``""`` trivially passes, which keeps the
    case runnable while signalling the human to fill in a real assertion.
    """
    for line in output.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:_EXPECTED_MAX_LEN]
    return output.strip()[:_EXPECTED_MAX_LEN]


def trace_to_eval_case(
    trace_id: str, trace: dict[str, Any], observations: list[dict[str, Any]]
) -> EvalCaseOut:
    """Build an anonymized, runnable eval case from a trace and its observations."""
    raw_input, raw_output = extract_io(trace, observations)
    anon_input = redact(raw_input).strip()
    anon_output = redact(raw_output)
    return EvalCaseOut(
        id=f"promoted-{trace_id}",
        input=anon_input,
        grader=GraderOut(kind="contains", expected=_expected_snippet(anon_output)),
    )
