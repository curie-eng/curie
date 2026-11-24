"""Scripted Messages responses derived from the current runner turn's Slack reply."""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from typing import Any

from scripted_lanes import LaneRouter

ANCHOR = "Supported: temperature (c, f), length (km, mi, m, ft)."
INSTRUCTION = "Insert the line Run note {} immediately before the Supported: line in README.md."
_NONCE = r"([a-f0-9]{32})"
_GUIDANCE = re.compile(re.escape(INSTRUCTION).replace(r"\{\}", _NONCE))
_REVIEW = re.compile(r"Check the steering fixture (plan|diff) for " + _NONCE + r"\.")
_FRAME = re.compile(
    r"\[user-message (curie-sender-boundary(?:-\d+)?)\]\n(.*?)\n\[end-user-message \1\]",
    re.DOTALL,
)


def _texts(content: Any) -> list[str]:
    if isinstance(content, str):
        return [content]
    return [
        block["text"]
        for block in content or []
        if isinstance(block, dict)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
    ]


def _tool(name: str, **args: Any) -> dict[str, Any]:
    if name in {"Agent", "Bash"}:
        args["run_in_background"] = False
    return {"type": "tool_use", "name": name, "input": args}


def _feedback_body(current: str) -> str | None:
    for line in current.splitlines():
        if not line.startswith("{"):
            continue
        try:
            reply = json.loads(line)
        except ValueError:
            continue
        if (
            isinstance(reply, dict)
            and reply.get("kind") == "slack_mention"
            and isinstance(reply.get("body"), str)
        ):
            return str(reply["body"])
    return None


def _review(stage: str, nonce: str) -> dict[str, Any]:
    return _tool(
        "Agent",
        description=f"Review the {stage}",
        prompt=f"Check the steering fixture {stage} for {nonce}.",
        subagent_type=f"dark-factory:{stage}-reviewer",
    )


def _last_results(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest = next((m for m in reversed(messages) if m.get("role") in {"assistant", "user"}), {})
    if latest.get("role") != "user":
        return []
    content = latest.get("content")
    return (
        [
            block
            for block in content
            if isinstance(block, dict) and block.get("type") == "tool_result"
        ]
        if isinstance(content, list)
        else []
    )


def _answer(nonce: str, ordinal: int, stage: str, block: dict[str, Any]) -> dict[str, Any]:
    if block["type"] == "tool_use":
        block["id"] = f"toolu_{stage}_{nonce}_{ordinal}"
    return {
        "message": {
            "id": f"msg_{stage}_{nonce}_{ordinal}",
            "type": "message",
            "role": "assistant",
            "model": "scripted",
            "content": [block],
            "stop_reason": "tool_use" if block["type"] == "tool_use" else "end_turn",
        }
    }


@dataclass(frozen=True)
class FixtureEdit:
    path: str
    before: str
    after: str
    added_line: str


class SteeringRouter(LaneRouter):
    def __init__(self) -> None:
        super().__init__({})
        self.initial_call = threading.Event()
        self.release_initial = threading.Event()
        self.received: list[str] = []

    def instruction_key(self, body: str) -> str | None:
        match = _GUIDANCE.fullmatch(body.removeprefix("<@U0EXAMPLE9> "))
        return match.group(1) if match else None

    def edit_for(self, key: str) -> FixtureEdit:
        line = f"Run note {key}"
        return FixtureEdit("README.md", ANCHOR, f"{line}\n{ANCHOR}", line)

    def match(self, method: str, path: str, body: bytes) -> dict[str, Any] | None:
        if method != "POST" or path != "/v1/messages":
            return None
        request = self._parse(body)
        messages = request.get("messages") or []
        result = _last_results(messages)
        result_text = "\n".join(t for block in result for t in _texts(block.get("content")))
        for i in reversed(range(len(messages))):
            msg = messages[i]
            if not isinstance(msg, dict) or msg.get("role") != "user":
                continue
            text = "\n".join(_texts(msg.get("content")))
            ordinal = sum(message.get("role") == "assistant" for message in messages[i + 1 :])
            # Reviewer prompts are emitted by our own parent model, never by a tool result.
            if review := _REVIEW.search(text):
                stage, nonce = review.groups()
                edit = self.edit_for(nonce)
                if ordinal == 0:
                    block = (
                        _tool("Read", file_path="/workspace/" + edit.path)
                        if stage == "plan"
                        else _tool(
                            "Bash",
                            command=f"git diff HEAD -- {edit.path}",
                            description="Read the fixture patch",
                        )
                    )
                elif ordinal == 1 and result and all(not b.get("is_error") for b in result):
                    verified = (
                        edit.before in result_text
                        if stage == "plan"
                        else (
                            [
                                line[1:]
                                for line in result_text.splitlines()
                                if line.startswith("+") and not line.startswith("+++")
                            ]
                            == [edit.added_line]
                        )
                    )
                    verdict = "APPROVE" if verified else "CHANGES"
                    block = {
                        "type": "text",
                        "text": f"REVIEWER: {stage}-reviewer\nVERDICT: {verdict}",
                    }
                else:
                    return None
                return _answer(nonce, ordinal, stage, block)
            if frame := _FRAME.search(text):
                current = frame.group(2)
                feedback = _feedback_body(current)
                nonce = self.instruction_key(feedback) if feedback is not None else None
                if nonce is None:
                    # The initial job uses the original issue transcript. A revision
                    # missing its expected input must not borrow that transcript.
                    if "/pull/" in current:
                        return None
                    break
                if ordinal and (not result or any(b.get("is_error") for b in result)):
                    return None
                edit = self.edit_for(nonce)
                blocks = [
                    _tool("Skill", skill="dark-factory:implement-issue", args="3815"),
                    _tool(
                        "mcp__curie__get_issue",
                        owner="acme-corp",
                        repo="acme-bot",
                        issue_number=3815,
                    ),
                    _review("plan", nonce),
                    _tool(
                        "mcp__curie__report_progress",
                        phase="implement",
                        note="The fixture change is ready to check.",
                    ),
                    _tool("Read", file_path="/workspace/" + edit.path),
                    _tool(
                        "Edit",
                        file_path="/workspace/" + edit.path,
                        old_string=edit.before,
                        new_string=edit.after,
                    ),
                    _tool(
                        "Bash",
                        command="python3 -m unittest discover -s unitconv/tests -v",
                        description="Run the declared repository check",
                    ),
                    _review("diff", nonce),
                    _tool(
                        "mcp__curie__publish_changes",
                        title="Add Kelvin temperature conversions (#3815)",
                        body="Add Kelvin support and update the README.\n\nCloses #3815\n\n"
                        "## Decisions\n\n"
                        "None: the change follows directly from the acceptance criteria.",
                    ),
                ]
                if ordinal >= len(blocks):
                    return None
                if ordinal == 0:
                    self.received.append(nonce)
                return _answer(nonce, ordinal, "execution", blocks[ordinal])
        response = super().match(method, path, body)
        if response is not None and not self.initial_call.is_set():
            self.initial_call.set()
            if not self.release_initial.wait(180):
                return None
        return response
