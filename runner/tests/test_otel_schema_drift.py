"""CI drift gate on the closed OTel attribute schema (ADR-0076 Stone 5, #891).

Mirrors ADR-0074's committed-artifact-plus-CI-gate discipline for the CLI's
``--json`` schemas, adapted for this Python-only surface: ``SpanAttributeKey``
(``otel.py``) is the single source of truth in code, this file's committed
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

import json
from pathlib import Path

import anyio
from aci_protocol import Event, OtelConfig
from curie_runner import RunTracer, SideEffectClassifier, build_tracer_provider
from curie_runner.fake import FakeModelSession
from curie_runner.otel import SCHEMA_VERSION, SPAN_ATTRIBUTE_VALUE_TYPES, SpanAttributeKey
from curie_runner.session import SessionRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

_SCHEMA_PATH = Path(__file__).parent.parent / "schema" / "otel-attributes.schema.json"

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
        "SpanAttributeKey (otel.py) and the committed schema "
        f"({_SCHEMA_PATH}) have diverged -- a key was added, removed, or "
        "RETYPED on one side but not the other (a retyped value, e.g. an "
        "attribute switching from str to int, trips this gate too). "
        "Regenerate the committed file (see this module's docstring) and "
        "commit it alongside the code change."
    )


def test_production_telemetry_keeps_the_existing_adr_0076_v1_schema() -> None:
    committed = _committed_schema()
    assert committed["schema_version"] == "v1"
    assert committed["keys"] == _ADR_0076_V1_KEYS


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
    # the 12 span-level keys via RunTracer.run_span/record_usage/tool_span, and
    # the stable resource keys via build_tracer_provider -- so a call-site
    # retype of any previously-unexercised key (that also forgot
    # to update SPAN_ATTRIBUTE_VALUE_TYPES) fails here instead of slipping past.
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

    for finished in exporter.get_finished_spans():
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
