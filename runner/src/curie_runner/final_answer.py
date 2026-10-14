"""Bounded, turn-local publication authorization (ADR 0227).

This observes provider-neutral tool records; it neither executes tools nor grants permission.
The validator owns semantics. Only exact call/result provenance authorizes text.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

FINAL_ANSWER_HEADER = "X-Curie-Final-Answer"
FINAL_ANSWER_CLASSIFICATION = "final-answer-validation"
FINAL_ANSWER_DIAGNOSTIC = "run failed: final answer validation required"
_MAX_JSON_BYTES = 64_000
_TOOL_NAME = re.compile(r"[A-Za-z0-9_-]{1,256}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class FinalAnswerPolicy:
    tool: str

    @classmethod
    def parse(cls, value: object) -> FinalAnswerPolicy:
        if (
            not isinstance(value, dict)
            or set(value) != {"version", "tool"}
            or type(value.get("version")) is not int
            or value["version"] != 1
            or not isinstance(value.get("tool"), str)
            or not _TOOL_NAME.fullmatch(value["tool"])
        ):
            raise ValueError("finalAnswer requires version 1 and an exact SDK tool name")
        return cls(tool=value["tool"])


def _json_bytes(value: object) -> bytes:
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(encoded) > _MAX_JSON_BYTES:
        raise ValueError("oversized validation payload")
    return encoded


def strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate keys rather than letting JSON's last value grant access."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _envelope(content: object) -> dict[str, Any]:
    if isinstance(content, str):
        if len(content.encode("utf-8")) > _MAX_JSON_BYTES:
            raise ValueError("oversized validation payload")
    else:
        _json_bytes(content)
    if isinstance(content, list):
        if (
            len(content) != 1
            or not isinstance(content[0], dict)
            or content[0].get("type") != "text"
        ):
            raise ValueError("expected exactly one MCP text block")
        content = content[0].get("text")
    if isinstance(content, str):
        content = json.loads(content, object_pairs_hook=strict_json_object)
    if not isinstance(content, dict) or set(content) != {"version", "draftSha256", "text"}:
        raise ValueError("invalid validation envelope")
    _json_bytes(content)
    if (
        type(content["version"]) is not int
        or content["version"] != 1
        or not isinstance(content["draftSha256"], str)
        or not _DIGEST.fullmatch(content["draftSha256"])
        or not isinstance(content["text"], str)
        or not content["text"]
    ):
        raise ValueError("invalid validation envelope")
    return content


@dataclass(frozen=True)
class ValidationCall:
    """One observed tool invocation; hosted tools cannot validate bundle text."""

    id: str
    name: str
    input: object
    hosted: bool = False


@dataclass(frozen=True)
class ValidationResult:
    """One correlated tool result, already extracted by the harness boundary."""

    call_id: str
    content: object
    is_error: bool | None = None
    hosted: bool = False


@dataclass
class FinalAnswerValidation:
    policy: FinalAnswerPolicy
    _seen: set[str] = field(default_factory=set, repr=False)
    _outstanding: set[str] = field(default_factory=set, repr=False)
    _candidate: str | None = field(default=None, repr=False)
    _digest: str | None = field(default=None, repr=False)
    _approved: str | None = field(default=None, repr=False)
    _resolved: bool = field(default=False, repr=False)
    _suspended: int = field(default=0, repr=False)

    @contextmanager
    def suspended(self) -> Iterator[None]:
        """Withhold authorization while steered input is being injected.

        Anything observed before the injection settles may belong to the
        superseded request, so settling (or cancellation) invalidates again.
        """
        self.invalidate()
        self._suspended += 1
        try:
            yield
        finally:
            self._suspended -= 1
            self.invalidate()

    def invalidate(self) -> None:
        # Keep seen/outstanding identities: delayed results cannot resurrect a
        # steered/superseded candidate, and repeated call IDs are ambiguous.
        self._candidate = self._digest = self._approved = None
        self._resolved = False

    def accepts(self, text: str) -> bool:
        return not self._suspended and self._approved is not None and text == self._approved

    def observe_calls(self, calls: Sequence[ValidationCall], *, nested: bool) -> None:
        """Observe all calls from one assistant message as an atomic group."""
        if nested:
            for call in calls:
                if call.id == self._candidate:
                    self.invalidate()
                self._seen.add(call.id)
            return
        eligible = len(calls) == 1 and not self._outstanding
        if calls:
            self.invalidate()
        for call in calls:
            fresh = call.id not in self._seen
            self._seen.add(call.id)
            self._outstanding.add(call.id)
            if (
                eligible
                and fresh
                and not self._suspended
                and not call.hosted
                and call.name == self.policy.tool
            ):
                try:
                    if not isinstance(call.input, dict):
                        continue
                    _json_bytes(call.input)
                    draft = call.input.get("draft")
                    if not isinstance(draft, str) or not draft:
                        continue
                    digest = hashlib.sha256(draft.encode("utf-8")).hexdigest()
                except (ValueError, TypeError, UnicodeError, RecursionError):
                    continue
                self._candidate, self._digest = call.id, digest

    def observe_results(self, results: Sequence[ValidationResult], *, nested: bool) -> None:
        for result in results:
            if result.hosted:
                # Hosted results retire provider work, never authorize bundle
                # text. Nested hosted work cannot retire top-level calls.
                if not nested:
                    if result.call_id == self._candidate:
                        self.invalidate()
                    self._outstanding.discard(result.call_id)
                continue
            if nested:
                if result.call_id == self._candidate:
                    self.invalidate()
                continue
            if result.call_id not in self._outstanding:
                self.invalidate()
                continue
            self._outstanding.remove(result.call_id)
            if result.call_id != self._candidate:
                continue
            if self._resolved or result.is_error is True:
                self.invalidate()
                continue
            self._resolved = True
            try:
                value = _envelope(result.content)
            except (ValueError, TypeError, UnicodeError, RecursionError):
                self.invalidate()
                continue
            if value["draftSha256"] != self._digest:
                self.invalidate()
                continue
            self._approved = value["text"]
