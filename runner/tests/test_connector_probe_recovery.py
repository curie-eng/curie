"""Turn-start recovery of a transiently failed declared connector (#2634).

Kept in its own module on purpose: the fix-pin gate reverts the source change
and runs this test against the old runner, so every module-level import here
must already exist there. Symbols the fix introduces are resolved inside the
test body, making the pinned test fail at runtime rather than at collection.
"""

from __future__ import annotations

import logging

import anyio
import pytest
from aci_protocol import ErrorEvent, Event, SessionStatus, parse_ndjson
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import mcp_tool_capability as capability_module
from curie_runner.fake import FakeModelSession, default_turn
from curie_runner.session import SessionRunner


def test_transient_probe_failure_recovers_on_a_later_turn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # AC3 (#2634): the boot probe and the turn-1 re-probe hit a transient
    # network error; the turn-2 re-probe succeeds. Driven through the real
    # probe and re-probe helpers with only the network dial faked.
    planted = "ghp-not-a-real-token-PLACEHOLDER"
    calls: list[str] = []

    async def flaky(*_args: object, **_kwargs: object) -> tuple[int, bool, frozenset[str]]:
        calls.append("dial")
        if len(calls) <= 2:
            raise OSError(f"connection reset dialing with Bearer {planted}")
        return 1, True, frozenset()

    monkeypatch.setattr("curie_runner.mcp_tool_capability._probe_server", flaky)
    derived = {
        "github": {
            "type": "http",
            "url": "http://127.0.0.1:9/mcp",
            "headers": {"Authorization": "Bearer ${GITHUB_TOKEN}"},
        }
    }
    env = {"GITHUB_TOKEN": planted}
    boot = anyio.run(capability_module.probe_mcp_tool_capability, None, derived, env)
    assert [f.reason for f in boot.connector_failures] == ["probe_failed"]

    # The bug under test: without a turn-start re-probe a boot probe failure is
    # sticky for the life of the process.
    reprobe_connector_failures = getattr(capability_module, "reprobe_connector_failures", None)
    assert reprobe_connector_failures is not None, "boot probe failure is never re-probed"

    from curie_runner.hooks import build_gated_pre_tool_use_hooks
    from curie_runner.mcp_tool_capability import ConnectorAvailability

    availability = ConnectorAvailability(boot.connector_failures)

    async def reprobe(failures):
        return await reprobe_connector_failures(failures, derived, env)

    fake = FakeModelSession(default_turn)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        connector_failures=boot.connector_failures,
        connector_reprobe=reprobe,
        connector_availability=availability,
    )
    hooks = build_gated_pre_tool_use_hooks(None, availability)
    assert hooks is not None
    callback = hooks["PreToolUse"][0].hooks[0]
    notice = boot.connector_failures[0].caller_message()

    async def go() -> tuple[list, dict, list, dict]:
        await runner.start()
        first = parse_ndjson(
            "".join(
                [
                    line
                    async for line in runner.run_inbound(
                        Event(type="message", text="go", user="U", ts="1")
                    )
                ]
            )
        )
        first_decision = await callback({"tool_name": "mcp__github__search"}, None, None)
        second = parse_ndjson(
            "".join(
                [
                    line
                    async for line in runner.run_inbound(
                        Event(type="message", text="again", user="U", ts="2")
                    )
                ]
            )
        )
        second_decision = await callback({"tool_name": "mcp__github__search"}, None, None)
        return first, first_decision, second, second_decision

    # Only the turn-time re-probe logs are pinned here: the re-probe logs the
    # exception class, never its text, which carries the header value.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        first, first_decision, second, second_decision = anyio.run(go)

    assert calls == ["dial", "dial", "dial"]
    assert fake.queries == ["go", "again"]

    assert not any(isinstance(e, ErrorEvent) for e in first)
    assert first[-1].status == SessionStatus.DONE
    assert first[-1].text.startswith(notice)
    assert first_decision["hookSpecificOutput"]["permissionDecision"] == "deny"

    assert not any(e.type == "error" for e in second)
    assert second[-1].status == SessionStatus.DONE
    assert second[-1].text == "all done"
    assert availability.failures == ()
    assert second_decision == {}
    assert any("error_class=OSError" in r.getMessage() for r in caplog.records)
    for record in caplog.records:
        assert planted not in record.getMessage()
