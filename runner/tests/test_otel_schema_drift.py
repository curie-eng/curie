"""CI drift gate on the closed OTel attribute schema (ADR-0076 Stone 5, #891).

Mirrors ADR-0074's committed-artifact-plus-CI-gate discipline for the CLI's
``--json`` schemas, adapted for this Python-only surface: ``SpanAttributeKey``
(``curie_telemetry_schema``) is the single source of truth in code, this file's committed
mirror (``runner/schema/otel-attributes.schema.json``) is the reviewed
artifact, and the tests below fail loudly if the two diverge -- catching a
new attribute key, a bumped ``SCHEMA_VERSION``, or a value-TYPE change to an
existing key (ADR-0076 decision 2; #934) that lands without an accompanying
schema update. A real emitted-attribute contract test closes the loop by
proving a live run's span attributes never escape the committed set, and
that each emitted value's runtime type matches what was committed.

Regenerate the committed file after a deliberate schema change by running, from
the repo root: import ``json``, ``SCHEMA_VERSION``, ``SpanAttributeKey``, and
``SPAN_ATTRIBUTE_VALUE_TYPES`` from ``curie_runner.otel``, then write
``{"$id": "https://schemas.curietech.ai/runner/otel-attributes/v1.json",
"schema_version": SCHEMA_VERSION, "keys": {member.value:
SPAN_ATTRIBUTE_VALUE_TYPES[member] for member in sorted(SpanAttributeKey,
key=lambda m: m.value)}}`` as indented JSON to
``runner/schema/otel-attributes.schema.json`` (keys sorted by name for a
stable diff).
"""

import ast
import json
from pathlib import Path

import anyio
from aci_protocol import Event, OtelConfig
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import (
    RunTracer,
    SideEffectClassifier,
    build_tracer_provider,
)
from curie_runner import (
    SpanAttributeKey as RunnerSpanAttributeKey,
)
from curie_runner.fake import FakeModelSession
from curie_runner.otel import SCHEMA_VERSION, SPAN_ATTRIBUTE_VALUE_TYPES
from curie_runner.session import SessionRunner
from curie_telemetry_schema import SpanAttributeKey
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "otel-attributes.schema.json"
_REPO_ROOT = Path(__file__).parents[2]
_ENUM_SOURCE = (
    _REPO_ROOT
    / "packages"
    / "telemetry-schema"
    / "src"
    / "curie_telemetry_schema"
    / "__init__.py"
)


def _production_python_modules() -> list[Path]:
    source_dirs = (
        _REPO_ROOT / "runner" / "src",
        *sorted(_REPO_ROOT.glob("apps/*/src")),
        *sorted(_REPO_ROOT.glob("packages/*/src")),
    )
    return sorted(
        module
        for source_dir in source_dirs
        for module in source_dir.rglob("*.py")
        if module != _ENUM_SOURCE
    )


def _display_module_path(module: Path) -> Path:
    try:
        return module.relative_to(_REPO_ROOT)
    except ValueError:
        return module


def _open_coded_span_attribute_values(
    modules: list[Path] | None = None,
) -> list[tuple[Path, int, str, str]]:
    # The bare "model" value is a common domain field, while "service.name" is
    # the standard resource/metric dimension owned by the shared telemetry
    # package. Neither literal can identify a runner span-schema consumer drift.
    members_by_value = {
        member.value: member
        for member in SpanAttributeKey
        if member not in {SpanAttributeKey.MODEL, SpanAttributeKey.SERVICE_NAME}
    }
    violations: list[tuple[Path, int, str, str]] = []
    modules_to_scan = _production_python_modules() if modules is None else modules

    assert modules_to_scan, "The OTel literal scan must inspect at least one production module."

    for module in modules_to_scan:
        tree = ast.parse(module.read_text(), filename=str(module))
        docstring_nodes = {
            id(owner.body[0].value)
            for owner in ast.walk(tree)
            if isinstance(
                owner,
                (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            )
            and owner.body
            and isinstance(owner.body[0], ast.Expr)
            and isinstance(owner.body[0].value, ast.Constant)
            and isinstance(owner.body[0].value.value, str)
        }
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if id(node) in docstring_nodes:
                continue
            member = members_by_value.get(node.value)
            if member is not None:
                violations.append(
                    (
                        _display_module_path(module),
                        node.lineno,
                        node.value,
                        member.name,
                    )
                )

    return sorted(violations)


def test_literal_scan_reports_the_exact_enum_key_in_an_injected_module(tmp_path: Path) -> None:
    module = tmp_path / "consumer.py"
    module.write_text('attribute = "curie.sandbox_id"\n')

    assert _open_coded_span_attribute_values([module]) == [
        (module, 1, "curie.sandbox_id", "CURIE_SANDBOX_ID")
    ]

_ADR_0076_V1_KEYS = {
    "curie.sandbox_id": "str",
    "curie.session_id": "str",
    "gen_ai.approval.decision": "str",
    "gen_ai.operation.name": "str",
    "gen_ai.request.model": "str",
    "gen_ai.tool.name": "str",
    "gen_ai.usage.cache_creation_input_tokens": "int",
    "gen_ai.usage.cache_read_input_tokens": "int",
    "gen_ai.usage.input_tokens": "int",
    "gen_ai.usage.output_tokens": "int",
    "langfuse.session.id": "str",
    "langfuse.trace.name": "str",
    "langfuse.user.id": "str",
    "model": "str",
    "schema.version": "str",
    "service.name": "str",
}

_PHASE_V1_ADDITIONS = {
    "curie.generation.round": "int",
    "curie.phase": "str",
    "curie.phase.end_kind": "str",
    "curie.phase.start_kind": "str",
    "curie.terminal.cause": "str",
    "curie.terminal.status": "str",
    "curie.tool.call.index": "int",
    "curie.generation.ttft_ms": "int",
    "curie.tool.outcome": "str",
    # #3128: generation content and result-only usage scope.
    "langfuse.observation.input": "str",
    "langfuse.observation.output": "str",
    "curie.usage.scope": "str",
}


def _committed_schema() -> dict:
    return json.loads(_SCHEMA_PATH.read_text())


def _assert_value_type(committed: dict, key: str, value: object, where: str = "") -> None:
    """Assert an emitted attribute's runtime type matches the committed type.

    redact_span_attribute (redact.py:152-164) scrubs strings but passes every
    other value through untouched, so a committed ``"int"`` key (the
    ``gen_ai.usage.*`` token counts) stays an int on the wire -- this checks the
    type actually emitted, catching a call-site retype (e.g. emitting ``model``
    as an int) that forgot to update the declaration, not merely the type
    redaction happened to preserve.
    """
    expected_type = committed[key]
    assert type(value).__name__ == expected_type, (
        f"{where}attribute {key!r} emitted as {type(value).__name__!r} but the "
        f"committed schema declares {expected_type!r}"
    )


def test_committed_schema_matches_the_enum() -> None:
    committed_keys = _committed_schema()["keys"]
    code_keys = {member.value: SPAN_ATTRIBUTE_VALUE_TYPES[member] for member in SpanAttributeKey}
    assert code_keys == committed_keys, (
        "SpanAttributeKey (curie_telemetry_schema) and the committed schema "
        f"({_SCHEMA_PATH}) have diverged -- a key was added, removed, or "
        "RETYPED on one side but not the other (a retyped value, e.g. an "
        "attribute switching from str to int, trips this gate too). "
        "Regenerate the committed file (see this module's docstring) and "
        "commit it alongside the code change."
    )


def test_phase_keys_are_an_additive_typed_v1_extension() -> None:
    committed = _committed_schema()
    committed_keys = committed["keys"]

    assert committed["schema_version"] == "v1"
    assert {
        key: committed_keys.get(key) for key in _ADR_0076_V1_KEYS
    } == _ADR_0076_V1_KEYS
    assert {
        key: committed_keys.get(key) for key in _PHASE_V1_ADDITIONS
    } == _PHASE_V1_ADDITIONS
    assert committed_keys == _ADR_0076_V1_KEYS | _PHASE_V1_ADDITIONS
    assert set(committed_keys.values()) == {"str", "int"}


def test_runner_reexports_the_shared_span_attribute_key_enum() -> None:
    assert RunnerSpanAttributeKey is SpanAttributeKey


def test_production_consumers_resolve_span_attribute_keys_through_the_enum() -> None:
    violations = _open_coded_span_attribute_values()

    assert not violations, (
        "Production consumers must resolve OTel span attribute keys through "
        "SpanAttributeKey instead of restating its values:\n"
        + "\n".join(
            f"{path}:{line}: {literal!r} must use "
            f"SpanAttributeKey.{member}.value"
            for path, line, literal, member in violations
        )
    )


def test_declared_value_types_cover_exactly_the_enum() -> None:
    # Forces a new SpanAttributeKey member to also declare its value type,
    # and forces every declared type to stay within the two types the runner
    # actually emits today (see redact.py:152-164).
    assert set(SPAN_ATTRIBUTE_VALUE_TYPES) == set(SpanAttributeKey), (
        "SPAN_ATTRIBUTE_VALUE_TYPES (otel.py) must declare a value type for "
        "every SpanAttributeKey member, no more and no fewer."
    )
    assert set(SPAN_ATTRIBUTE_VALUE_TYPES.values()) <= {"str", "int"}, (
        "SPAN_ATTRIBUTE_VALUE_TYPES values must be one of {'str', 'int'} -- "
        "the only value types the runner emits today."
    )


def test_committed_schema_version_matches_the_code() -> None:
    committed_version = _committed_schema()["schema_version"]
    assert committed_version == SCHEMA_VERSION, (
        "SCHEMA_VERSION (otel.py) and the committed schema's schema_version "
        f"({_SCHEMA_PATH}) disagree. Per ADR-0076, bump both together only "
        "for a removed/renamed/retyped key -- a new optional key is additive "
        "and must not bump either."
    )


def test_a_real_run_only_emits_attributes_within_the_committed_schema() -> None:
    # The contract half: proves the enum's closure actually holds on the wire,
    # not just in the committed inventory. Drives a real turn (permission gate
    # + tool call + usage, the FakeModelSession default script) and asserts
    # every attribute key on every emitted span is in the committed set, and
    # that each emitted value's runtime type matches the committed type for
    # its key -- catching a call-site retype (e.g. emitting `model` as an int)
    # that forgot to update the declaration.
    committed = _committed_schema()["keys"]
    committed_keys = set(committed)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    runner = SessionRunner(
        session_factory=FakeModelSession,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        session_id="s1",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        async for _ in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            pass

    anyio.run(go)

    spans = exporter.get_finished_spans()
    assert spans, "expected at least one emitted span to check"
    for span in spans:
        emitted_keys = set(span.attributes)
        assert emitted_keys <= committed_keys, (
            f"span {span.name!r} emitted attribute key(s) outside the "
            f"committed schema: {emitted_keys - committed_keys}"
        )
        for key, value in span.attributes.items():
            _assert_value_type(committed, key, value, f"span {span.name!r} ")


def test_every_declared_key_emits_its_committed_value_type() -> None:
    # The real-run contract test above only exercises 9 of the 16 declared keys
    # (the FakeModelSession default script never threads a session/sandbox id,
    # an approval decision, or the prompt-cache usage fields through). This test
    # closes that gap by driving every declared key through its real setter --
    # direct compatibility setters for usage/tools, an opt-in payload-free fake
    # boundary for TTFT, and build_tracer_provider for stable resource keys -- so
    # a call-site retype of any previously-unexercised key (that also forgot to
    # update SPAN_ATTRIBUTE_VALUE_TYPES) fails here instead of slipping past.
    committed = _committed_schema()["keys"]
    emitted: dict[str, object] = {}

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    provider._curie_sandbox_id = "sandbox-abc"  # type: ignore[attr-defined]
    tracer = RunTracer(provider)

    with tracer.run_span(
        trace_name="t",
        model="fake-model",
        session_id="s1",
        user_id="U1",
        approval_decision="approved",
    ) as span:
        span.record_usage(
            {
                "input_tokens": 1,
                "output_tokens": 2,
                "cache_read_input_tokens": 3,
                "cache_creation_input_tokens": 4,
            }
        )
        span.tool_span("Bash")

    runner = SessionRunner(
        session_factory=lambda: FakeModelSession(emit_partial_boundaries=True),
        ceiling=0,
        tracer=tracer,
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )

    async def go() -> None:
        await runner.start()
        try:
            async for _ in runner.run_turn(
                Event(type="message", text="go", user="U0EXAMPLE1", ts="1")
            ):
                pass
        finally:
            await runner.close()

    anyio.run(go)

    # #3128: a provider that reports usage only on the ResultMessage stamps the
    # turn total on the final generation with curie.usage.scope. Its own
    # provider: closing the runner above shuts the shared one down.
    result_exporter = InMemorySpanExporter()
    result_provider = TracerProvider()
    result_provider.add_span_processor(SimpleSpanProcessor(result_exporter))
    result_only = SessionRunner(
        session_factory=lambda: FakeModelSession(
            script_factory=lambda: [
                AssistantMessage(
                    content=[TextBlock(text="hi")], model="fake-model", usage=None
                ),
                ResultMessage(
                    subtype="success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=False,
                    num_turns=1,
                    session_id="sdk-session",
                    result="hi",
                    usage={"input_tokens": 5, "output_tokens": 1},
                ),
            ]
        ),
        ceiling=0,
        tracer=RunTracer(result_provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )

    async def go_result_only() -> None:
        await result_only.start()
        try:
            async for _ in result_only.run_turn(
                Event(type="message", text="go", user="U0EXAMPLE1", ts="2")
            ):
                pass
        finally:
            await result_only.close()

    anyio.run(go_result_only)

    for finished in [*exporter.get_finished_spans(), *result_exporter.get_finished_spans()]:
        if finished.attributes:
            emitted.update(finished.attributes)

    otel = OtelConfig(endpoint="http://localhost:24318")
    resource_provider = build_tracer_provider(otel, "s1", "sandbox-abc")
    assert resource_provider is not None
    # The shared stable resource carries only service identity and the schema;
    # session/sandbox correlation was exercised on the root span above.
    resource_attrs = resource_provider.resource.attributes
    emitted.update({key: value for key, value in resource_attrs.items() if key in committed})
    resource_provider.shutdown()

    exercised_keys = set(emitted)
    missing = set(committed) - exercised_keys
    assert not missing, f"declared key(s) not exercised by this test: {sorted(missing)}"

    for key, value in emitted.items():
        _assert_value_type(committed, key, value)
