"""The offline session a blocked verification preflight stands in with (#3873).

When a declared check could not run in the sandbox and declares no
``delegated_to``, the runner never builds the model session. This session
answers every turn, including the worker's early-stop continuation (#3128),
with the same ``Could not complete:`` explanation and a zero-token success
result, so the existing early_stop terminus delivers the explanation, the usage
line and the cleanup. It is offline: it spawns no CLI, makes no network call
and makes no tool call. It has no ``options`` attribute, since no model is
configured behind it.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock


class PreflightBlockedSession:
    """A ``ModelSession`` that explains a blocked preflight and never runs a model."""

    def __init__(self, text: str, *, model: str | None) -> None:
        self._text = text
        self._model = model or ""
        self._interrupted = False

    async def connect(self) -> None:
        return None

    async def query(self, text: str) -> None:
        self._interrupted = False

    async def interrupt(self) -> None:
        self._interrupted = True

    async def close(self) -> None:
        return None

    async def receive_turn(self) -> AsyncIterator[Any]:
        if self._interrupted:
            return
        # The configured model names the zero row, so the usage report is
        # complete; with no model there is no row to report.
        yield AssistantMessage(
            content=[TextBlock(text=self._text)],
            model=self._model,
            usage={"input_tokens": 0, "output_tokens": 0},
        )
        if self._interrupted:
            return
        yield ResultMessage(
            subtype="success",
            duration_ms=0,
            duration_api_ms=0,
            is_error=False,
            num_turns=1,
            session_id="preflight-blocked",
            result=self._text,
            usage={"input_tokens": 0, "output_tokens": 0},
            # A fresh id per turn, so two turns of one request never share one.
            uuid=str(uuid.uuid4()),
        )
