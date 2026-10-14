"""ADR 0227: publication authorization, using the installed SDK message shapes.

SDK 0.2.163's types.py was inspected locally: AssistantMessage and UserMessage
carry parent_tool_use_id; ToolResultBlock carries tool_use_id/content/is_error.
These are synthetic protocol fixtures, not provider or live deployment proof.
"""

import hashlib
import json
from pathlib import Path

import anyio
import pytest
from aci_protocol import Event
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import AssistantMessage, TextBlock, ToolResultBlock, ToolUseBlock, UserMessage
from claude_agent_sdk.types import ServerToolResultBlock, ServerToolUseBlock
from curie_runner import RunTracer, SideEffectClassifier, create_app, plugin
from curie_runner.fake import FakeModelSession, _result, approval_turn
from curie_runner.mcp_tool_capability import ConnectorCapabilityFailure
from curie_runner.session import SessionRunner

TOOL = "mcp__answer_checks__finalize_answer"
DRAFT = "draft 한글"
APPROVED = "  approved 👩🏽‍💻 e\u0301\n"
EVENT = Event(type="message", text="question", user="U", ts="1")


def policy(tmp_path: Path):
    (tmp_path / "curie.bundle.json").write_text(
        json.dumps({"finalAnswer": {"version": 1, "tool": TOOL}})
    )
    return plugin.load_bundle_final_answer(str(tmp_path))


def call(call_id="check", name=TOOL, draft=DRAFT, *, parent=None):
    return AssistantMessage(
        content=[ToolUseBlock(id=call_id, name=name, input={"draft": draft})],
        model="synthetic",
        parent_tool_use_id=parent,
    )


def envelope(**changes):
    return {
        "version": 1,
        "draftSha256": hashlib.sha256(DRAFT.encode()).hexdigest(),
        "text": APPROVED,
        **changes,
    }


def result(content=None, *, call_id="check", parent=None, error=False):
    return UserMessage(
        content=[
            ToolResultBlock(
                tool_use_id=call_id,
                content=json.dumps(envelope()) if content is None else content,
                is_error=error,
            )
        ],
        parent_tool_use_id=parent,
    )


def runner_for(script, *, final_answer=None, **kwargs):
    fake = FakeModelSession(lambda: list(script), truncate_on_interrupt=False)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        max_usd_per_day=None,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="final-answer-test",
        held_secrets=frozenset(),
        final_answer=final_answer,
        **kwargs,
    )
    return runner, fake


def run(script, final_answer):
    async def exercise():
        runner, _ = runner_for(script, final_answer=final_answer)
        await runner.start()
        try:
            return [json.loads(line) async for line in runner.run_turn(EVENT)]
        finally:
            await runner.close()

    return anyio.run(exercise)


def assert_refused(frames):
    assert frames[-1]["status"] == "classified-failure"
    assert frames[-1]["text"] != APPROVED
    assert any(f.get("classification") == "final-answer-validation" for f in frames)
    assert not any(f["type"] == "text_delta" for f in frames)


def test_missing_validation_never_publishes_candidate(tmp_path):
    frames = run(
        [
            AssistantMessage(content=[TextBlock(text=APPROVED)], model="synthetic"),
            _result(text=APPROVED),
        ],
        policy(tmp_path),
    )
    assert_refused(frames)


@pytest.mark.parametrize("content", [None, [{"type": "text", "text": json.dumps(envelope())}]])
def test_validated_exact_text_is_the_only_answer(tmp_path, content):
    frames = run([call(), result(content), _result(text=APPROVED)], policy(tmp_path))
    assert frames[-1]["status"] == "done"
    assert frames[-1]["text"] == APPROVED
    assert not any(f["type"] == "text_delta" for f in frames)


@pytest.mark.parametrize(
    "script",
    [
        [call(), result(), _result(text=APPROVED.strip())],
        [call(), result(error=True), _result(text=APPROVED)],
        [call(), result(parent="nested"), _result(text=APPROVED)],
        [call(parent="nested"), result(), _result(text=APPROVED)],
        [call(), result(call_id="unknown"), _result(text=APPROVED)],
        [result(), _result(text=APPROVED)],
        [call(), result(), result(), _result(text=APPROVED)],
        [call(), result(), call("later", "Read"), _result(text=APPROVED)],
        [
            call(),
            result(),
            call("later"),
            result(call_id="later", error=True),
            _result(text=APPROVED),
        ],
        [call("other", "Read"), call(), result(), _result(text=APPROVED)],
        [
            AssistantMessage(
                content=[call().content[0], call("other", "Read").content[0]], model="synthetic"
            ),
            result(),
            _result(text=APPROVED),
        ],
        [call(), result(), call(), result(), _result(text=APPROVED)],
        [call(draft=""), result(), _result(text=APPROVED)],
        [call(draft="x" * 64001), result(), _result(text=APPROVED)],
        [call(), result(json.dumps(envelope(text="x" * 64001))), _result(text=APPROVED)],
        [call(), result(json.dumps(envelope(version=True))), _result(text=APPROVED)],
        [call(), result(json.dumps(envelope(extra="forbidden"))), _result(text=APPROVED)],
        [call(), result(json.dumps(envelope(draftSha256="0" * 64))), _result(text=APPROVED)],
        [call(), result('{"version":1,"version":1}'), _result(text=APPROVED)],
        [
            call(),
            result(
                [
                    {"type": "text", "text": json.dumps(envelope())},
                    {"type": "text", "text": "extra"},
                ]
            ),
            _result(text=APPROVED),
        ],
        [call(), result(), _result(text="")],
        [call(), result()],
    ],
)
def test_invalid_validation_cannot_authorize_answer(tmp_path, script):
    assert_refused(run(script, policy(tmp_path)))


def test_unconstrained_turn_preserves_stream_and_final():
    frames = run(
        [
            AssistantMessage(content=[TextBlock(text="preview")], model="synthetic"),
            _result(text=APPROVED),
        ],
        None,
    )
    assert any(f["type"] == "text_delta" for f in frames)
    assert frames[-1]["text"] == APPROVED


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"version": True, "tool": TOOL},
        {"version": 2, "tool": TOOL},
        {"version": 1, "tool": ""},
        {"version": 1, "tool": "*"},
        {"version": 1, "tool": TOOL, "extra": 1},
    ],
)
def test_invalid_policy_is_refused_at_sidecar_load(tmp_path, value):
    (tmp_path / "curie.bundle.json").write_text(json.dumps({"finalAnswer": value}))
    with pytest.raises(plugin.PluginBundleError):
        plugin.load_bundle_web_search_enabled(str(tmp_path))


def test_policy_coexists_with_web_search_and_absence_defaults(tmp_path):
    assert plugin.load_bundle_final_answer(str(tmp_path)) is None
    assert policy(tmp_path).tool == TOOL
    assert plugin.load_bundle_web_search_enabled(str(tmp_path)) is True


@pytest.mark.parametrize("header,status", [(None, 400), ("2", 400), ("1", 200)])
def test_http_requires_support_before_accepting_constrained_turn(tmp_path, header, status):
    async def exercise():
        runner, fake = runner_for(
            [call(), result(), _result(text=APPROVED)], final_answer=policy(tmp_path)
        )
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            headers = {} if header is None else {"X-Curie-Final-Answer": header}
            response = await client.post("/v1/event", json=EVENT.model_dump(), headers=headers)
            assert response.status == status
            await response.read()
            assert bool(fake.queries) is (status == 200)
            assert response.headers.get("X-Curie-Final-Answer") == ("1" if status == 200 else None)

    anyio.run(exercise)


def test_turn_validation_does_not_leak_between_turns(tmp_path):
    async def exercise():
        scripts = iter([[call(), result(), _result(text=APPROVED)], [_result(text=APPROVED)]])
        runner, fake = runner_for([], final_answer=policy(tmp_path))
        fake._script_factory = lambda: next(scripts)
        await runner.start()
        try:
            first = [json.loads(line) async for line in runner.run_turn(EVENT)]
            second = [json.loads(line) async for line in runner.run_turn(EVENT)]
            assert first[-1]["status"] == "done"
            assert_refused(second)
        finally:
            await runner.close()

    anyio.run(exercise)


@pytest.mark.parametrize("action", ["interrupt", "timeout", "steer-pending", "steer-approved"])
def test_control_actions_cannot_publish_stale_candidate(tmp_path, action):
    async def exercise():
        class ControlledSession(FakeModelSession):
            async def receive_turn(self):
                async for message in super().receive_turn():
                    yield message
                    trigger = (
                        isinstance(message, AssistantMessage)
                        if action == "steer-pending"
                        else isinstance(message, UserMessage)
                    )
                    if trigger:
                        if action == "interrupt":
                            await runner.interrupt()
                        elif action == "timeout":
                            assert await runner.timeout("epoch-current")
                        else:
                            assert await runner.steer("changed request")

        fake = ControlledSession(
            lambda: [call(), result(), _result(text=APPROVED)], truncate_on_interrupt=False
        )
        runner = SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            max_usd_per_day=None,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="final-answer-test",
            held_secrets=frozenset(),
            final_answer=policy(tmp_path),
        )
        await runner.start()
        try:
            frames = [
                json.loads(line)
                async for line in runner.run_turn(EVENT, turn_epoch="epoch-current")
            ]
            if action.startswith("steer"):
                assert_refused(frames)
            else:
                assert frames[-1]["status"] == (
                    "idle-awaiting-input" if action == "interrupt" else "classified-failure"
                )
                assert frames[-1]["text"] == (
                    "run interrupted" if action == "interrupt" else "run timed out"
                )
        finally:
            await runner.close()

    anyio.run(exercise)


def test_connector_notice_cannot_decorate_approved_answer(tmp_path):
    async def exercise():
        runner, _ = runner_for(
            [call(), result(), _result(text=APPROVED)],
            final_answer=policy(tmp_path),
            connector_failures=(
                ConnectorCapabilityFailure(
                    connector="github", credential_names=(), reason="empty_expansion"
                ),
            ),
        )
        await runner.start()
        try:
            frames = [json.loads(line) async for line in runner.run_turn(EVENT)]
            assert frames[-1]["text"] == APPROVED
        finally:
            await runner.close()

    anyio.run(exercise)


def test_redaction_cannot_silently_rewrite_approved_answer(tmp_path):
    async def exercise():
        fake = FakeModelSession(lambda: [call(), result(), _result(text=APPROVED)])
        runner = SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            max_usd_per_day=None,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="final-answer-test",
            held_secrets=frozenset({"approved"}),
            final_answer=policy(tmp_path),
        )
        await runner.start()
        try:
            assert_refused([json.loads(line) async for line in runner.run_turn(EVENT)])
        finally:
            await runner.close()

    anyio.run(exercise)


def test_provider_hosted_tool_invalidates_approval(tmp_path):
    # These block constructors also come from locally inspected SDK 0.2.163.
    hosted = AssistantMessage(
        model="synthetic",
        content=[
            ServerToolUseBlock(id="search", name="web_search", input={"query": "new evidence"}),
            ServerToolResultBlock(tool_use_id="search", content={"result": "new evidence"}),
        ],
    )
    assert_refused(run([call(), result(), hosted, _result(text=APPROVED)], policy(tmp_path)))


@pytest.mark.parametrize("valid", [False, True])
def test_only_validated_success_enters_history(tmp_path, valid):
    async def exercise():
        class History:
            def __init__(self):
                self.records = []

            async def load(self):
                return self.records

            async def append(self, record):
                self.records.append(record)
                return False

        history = History()
        diagnostic = AssistantMessage(model="synthetic", content=[TextBlock(text="internal note")])
        script = [diagnostic, *([call(), result()] if valid else []), _result(text=APPROVED)]
        runner, _ = runner_for(script, final_answer=policy(tmp_path), history_store=history)
        await runner.start()
        try:
            frames = [json.loads(line) async for line in runner.run_turn(EVENT)]
            assert len(history.records) == int(valid)
            if valid:
                assert frames[-1]["text"] == APPROVED
                assert history.records[0].assistant == APPROVED
                assert "internal note" in str(history.records[0].messages)
            else:
                assert_refused(frames)
        finally:
            await runner.close()

    anyio.run(exercise)


def test_approval_pause_keeps_existing_status(tmp_path):
    frames = run(approval_turn("Please approve the action"), policy(tmp_path))
    assert frames[-1]["status"] == "awaiting-approval"
    assert not any(f.get("classification") == "final-answer-validation" for f in frames)


def test_provider_failure_outranks_validation(tmp_path):
    frames = run([_result(text="provider failed", is_error=True)], policy(tmp_path))
    assert frames[-1]["status"] == "classified-failure"
    assert not any(f.get("classification") == "final-answer-validation" for f in frames)


@pytest.mark.parametrize("header,status", [(None, 200), ("1", 200), ("2", 400)])
def test_unconstrained_http_never_claims_exact_output(header, status):
    async def exercise():
        runner, _ = runner_for([_result(text="ordinary")])
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            headers = {} if header is None else {"X-Curie-Final-Answer": header}
            response = await client.post("/v1/event", json=EVENT.model_dump(), headers=headers)
            assert response.status == status
            assert "X-Curie-Final-Answer" not in response.headers
            await response.read()

    anyio.run(exercise)


def test_actual_fake_boot_loads_bundle_policy(tmp_path):
    from curie_runner.__main__ import build_runner
    from curie_runner.config import RunnerConfig

    policy(tmp_path)
    manifest = tmp_path / ".claude-plugin"
    manifest.mkdir()
    (manifest / "plugin.json").write_text('{"name":"acme-bot"}')
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(tmp_path),
            "CURIE_SESSION_ID": "synthetic-session",
            "CURIE_SANDBOX_ID": "synthetic-sandbox",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}',
        }
    )
    runner = build_runner(config, fake_model=True)
    assert runner.final_answer_required


def test_validation_classification_survives_platform_allowlist():
    from curie_runner.translate import map_error_classification

    assert map_error_classification("final-answer-validation") == "final-answer-validation"


def test_validated_text_fallback_still_requires_exact_approval(tmp_path):
    script = [
        call(),
        result(),
        AssistantMessage(model="synthetic", content=[TextBlock(text=APPROVED)]),
        _result(text=""),
    ]
    frames = run(script, policy(tmp_path))
    assert frames[-1]["status"] == "done"
    assert frames[-1]["text"] == APPROVED
    assert not any(f["type"] == "text_delta" for f in frames)


def test_unicode_normalization_is_not_exact_approval(tmp_path):
    assert_refused(
        run([call(), result(), _result(text=APPROVED.replace("e\u0301", "é"))], policy(tmp_path))
    )


def test_completed_provider_hosted_tool_allows_fresh_validation(tmp_path):
    hosted = AssistantMessage(
        model="synthetic",
        content=[
            ServerToolUseBlock(id="search", name="web_search", input={"query": "evidence"}),
            ServerToolResultBlock(tool_use_id="search", content={"result": "evidence"}),
        ],
    )
    frames = run([hosted, call(), result(), _result(text=APPROVED)], policy(tmp_path))
    assert frames[-1]["status"] == "done"
    assert frames[-1]["text"] == APPROVED


def test_unresolved_provider_hosted_tool_prevents_validation(tmp_path):
    hosted = AssistantMessage(
        model="synthetic",
        content=[ServerToolUseBlock(id="search", name="web_search", input={"query": "evidence"})],
    )
    assert_refused(run([hosted, call(), result(), _result(text=APPROVED)], policy(tmp_path)))


@pytest.mark.parametrize(
    "subtype,classification",
    [("error_max_budget_usd", "budget-exceeded"), ("error_max_turns", "max-turns")],
)
def test_budget_and_turn_limit_keep_precedence(tmp_path, subtype, classification):
    frames = run([_result(subtype=subtype, is_error=True, text="limit reached")], policy(tmp_path))
    assert frames[-1]["status"] == "classified-failure"
    assert any(f.get("classification") == classification for f in frames)
    assert not any(f.get("classification") == "final-answer-validation" for f in frames)


def test_auth_failure_keeps_precedence(tmp_path):
    failure = AssistantMessage(
        model="synthetic", content=[TextBlock(text="auth rejected")], error="authentication_failed"
    )
    frames = run([failure, _result(is_error=True)], policy(tmp_path))
    assert frames[-1]["status"] == "classified-failure"
    assert any(f.get("classification") == "model-credential-rejected" for f in frames)
    assert not any(f.get("classification") == "final-answer-validation" for f in frames)


def test_executor_cannot_advertise_final_answer_support():
    from curie_runner.server import create_executor_app

    async def exercise():
        async with TestClient(TestServer(create_executor_app({}))) as client:
            response = await client.post(
                "/v1/event", json=EVENT.model_dump(), headers={"X-Curie-Final-Answer": "1"}
            )
            assert response.status == 409
            assert "X-Curie-Final-Answer" not in response.headers
            await response.read()

    anyio.run(exercise)


@pytest.mark.parametrize("second", ["1", "2"])
def test_duplicate_support_headers_are_refused_before_model(tmp_path, second):
    async def exercise():
        runner, fake = runner_for([_result(text="unused")], final_answer=policy(tmp_path))
        await runner.start()
        async with TestClient(TestServer(create_app(runner))) as client:
            response = await client.post(
                "/v1/event",
                json=EVENT.model_dump(),
                # aiohttp's client coalesces equal case-varied headers; the
                # same spelling preserves two actual HTTP header fields.
                headers=[("X-Curie-Final-Answer", "1"), ("X-Curie-Final-Answer", second)],
            )
            assert response.status == 400
            assert not fake.queries
            assert "X-Curie-Final-Answer" not in response.headers
            await response.read()

    anyio.run(exercise)


@pytest.mark.parametrize("cancel", [False, True])
def test_steer_injection_in_flight_cannot_publish_old_context_answer(tmp_path, cancel):
    """Old-context validation that lands while a steer is still being injected."""

    async def exercise():
        entered = anyio.Event()
        release = anyio.Event()
        steered: list[bool] = []

        class BlockingSteerSession(FakeModelSession):
            steer_queries = 0

            async def query(self, text: str) -> None:
                self.steer_queries += 1
                if self.steer_queries > 1:
                    entered.set()
                    await release.wait()
                await super().query(text)

            async def receive_turn(self):
                tg.start_soon(steer)
                await entered.wait()
                async for message in super().receive_turn():
                    yield message

        async def steer():
            with anyio.CancelScope() as scope:
                steer_scope.append(scope)
                steered.append(await runner.steer("changed request"))

        steer_scope: list[anyio.CancelScope] = []
        fake = BlockingSteerSession(
            lambda: [call(), result(), _result(text=APPROVED)], truncate_on_interrupt=False
        )
        runner = SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            max_usd_per_day=None,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="final-answer-test",
            held_secrets=frozenset(),
            final_answer=policy(tmp_path),
        )
        await runner.start()
        try:
            async with anyio.create_task_group() as tg:
                frames = [json.loads(line) async for line in runner.run_turn(EVENT)]
                # The old answer settled while the steer was still injecting.
                assert_refused(frames)
                if cancel:
                    steer_scope[0].cancel()
                release.set()
            assert steered == ([] if cancel else [True])
        finally:
            await runner.close()

    anyio.run(exercise)


def test_validation_suspension_blocks_and_then_invalidates(tmp_path):
    from curie_runner.final_answer import FinalAnswerValidation, ValidationCall, ValidationResult

    validation = FinalAnswerValidation(policy(tmp_path))
    digest_ok = json.dumps(envelope())
    validation.observe_calls([ValidationCall("a", TOOL, {"draft": DRAFT})], nested=False)
    with validation.suspended():
        with validation.suspended():  # concurrent steers nest
            validation.observe_results([ValidationResult("a", digest_ok)], nested=False)
            assert not validation.accepts(APPROVED)
            validation.observe_calls([ValidationCall("b", TOOL, {"draft": DRAFT})], nested=False)
            validation.observe_results([ValidationResult("b", digest_ok)], nested=False)
            assert not validation.accepts(APPROVED)
        assert not validation.accepts(APPROVED)
    # Settling invalidates anything observed during injection.
    assert not validation.accepts(APPROVED)
    validation.observe_calls([ValidationCall("c", TOOL, {"draft": DRAFT})], nested=False)
    validation.observe_results([ValidationResult("c", digest_ok)], nested=False)
    assert validation.accepts(APPROVED)
