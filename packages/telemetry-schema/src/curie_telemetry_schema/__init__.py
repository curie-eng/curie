"""Shared telemetry attribute key schema."""

from enum import StrEnum


class SpanAttributeKey(StrEnum):
    """The closed set of keys the runner may attach to a span or resource.

    ADR-0076 decision 1. Str-mixin so a member is usable anywhere a plain
    attribute-value string is expected (e.g. dict keys, f-strings), but every
    ``set_attribute``/``Resource.create`` call site should pass a member here
    rather than a literal, so an unlisted key is a construction-time error.
    """

    TRACE_NAME = "langfuse.trace.name"
    SESSION_ID = "langfuse.session.id"
    USER_ID = "langfuse.user.id"
    # ADR-0076 Stone 3 (#889, epic #512): the resolved terminal decision
    # (approved/rejected/expired) of the approval a resume turn is resuming
    # from, threaded in from the worker's authority-free CURIE_APPROVAL_DECISION
    # boot-env fact. Closes the "did an approval get requested" gap ADR-0038
    # named open, on the existing span stream.
    APPROVAL_DECISION = "gen_ai.approval.decision"
    REQUEST_MODEL = "gen_ai.request.model"
    MODEL = "model"
    USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
    USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
    USAGE_CACHE_READ_INPUT_TOKENS = "gen_ai.usage.cache_read_input_tokens"
    USAGE_CACHE_CREATION_INPUT_TOKENS = "gen_ai.usage.cache_creation_input_tokens"
    TOOL_NAME = "gen_ai.tool.name"
    OPERATION_NAME = "gen_ai.operation.name"
    # #3128: generation input and output, redacted then clipped. Never tool
    # arguments or tool results. Langfuse maps these to observation input/output.
    OBSERVATION_INPUT = "langfuse.observation.input"
    OBSERVATION_OUTPUT = "langfuse.observation.output"
    # #3128: "turn" when a result-only provider's ResultMessage total is stamped
    # on the final generation; "unrecorded" on the root when no generation was
    # active to carry it. Absent means per-generation usage.
    USAGE_SCOPE = "curie.usage.scope"
    PHASE = "curie.phase"
    PHASE_START_KIND = "curie.phase.start_kind"
    PHASE_END_KIND = "curie.phase.end_kind"
    TERMINAL_CAUSE = "curie.terminal.cause"
    TERMINAL_STATUS = "curie.terminal.status"
    GENERATION_TTFT_MS = "curie.generation.ttft_ms"
    GENERATION_ROUND = "curie.generation.round"
    TOOL_CALL_INDEX = "curie.tool.call.index"
    TOOL_OUTCOME = "curie.tool.outcome"
    SERVICE_NAME = "service.name"
    CURIE_SESSION_ID = "curie.session_id"
    CURIE_SANDBOX_ID = "curie.sandbox_id"
    SCHEMA_VERSION_KEY = "schema.version"


__all__ = ["SpanAttributeKey"]
