"""The ACI session protocol and NDJSON event types.

This package is the single source of truth for the Agent Container Interface
(ACI) contract (docs/reference/detailed-architecture.md section 0). Every lane
compiles against it across three languages (Pydantic here, generated TypeScript
and Rust from the committed JSON Schema). The wire is strict producers, tolerant
consumers: constructing an event with an unknown field is an error, but a
consumer decoding the wire ignores fields it does not model. The version is
semver; a consumer accepts a compatible wire version (same major.minor under 0.x)
via ``is_compatible``. A contract change bumps PROTOCOL_VERSION per the change
class and regenerates the committed schema and types.
"""

from .conformance import ConformanceReport, Producer, run_conformance
from .events import (
    ADOPTION_CREDENTIAL_MAX_CHARS,
    OUTBOUND_EVENT_TYPES,
    READER_CONTEXT,
    ErrorEvent,
    Event,
    Final,
    InboundMessage,
    Interrupt,
    OutboundEvent,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
    parse_adoption_credential,
)
from .ndjson import (
    ProtocolVersionError,
    dump_ndjson,
    iter_ndjson,
    parse_eval_job,
    parse_inbound,
    parse_ndjson,
    parse_ndjson_line,
    parse_queued_turn,
    to_inbound_json,
    to_ndjson_line,
)
from .reference import reference_producer
from .service_config import (
    DEAD_LETTER_STREAM_ENV,
    EVAL_CONSUMER_GROUP_DEFAULT,
    EVAL_STREAM_DEFAULT,
    RUNS_STREAM_DEFAULT,
    STREAM_ENV,
    STREAM_PAYLOAD_FIELD,
    WORKER_GROUP_DEFAULT,
    derive_dead_letter_stream_name,
)
from .session import BootEnv, Budget, OtelConfig, SessionConfig
from .session import Producer as EnvProducer
from .turn import QueuedTurn, ReplyHandle, TurnSource
from .version import PROTOCOL_VERSION, is_compatible
from .wire import ApprovalRequest, EvalJob, EvalReport, GateKind

__version__ = "0.0.0"

__all__ = [
    "PROTOCOL_VERSION",
    "is_compatible",
    "__version__",
    # session setup
    "SessionConfig",
    "Budget",
    "OtelConfig",
    # the platform boot-env superset (#488, ADR-0049)
    "BootEnv",
    "EnvProducer",
    # queue turn payload (the ingress job the worker consumes)
    "QueuedTurn",
    "ReplyHandle",
    "TurnSource",
    # eval/approval queue payloads (the API <-> worker seam)
    "EvalJob",
    "EvalReport",
    "ApprovalRequest",
    "GateKind",
    # shared transport literals (defaults + the stream payload field)
    "RUNS_STREAM_DEFAULT",
    "STREAM_ENV",
    "WORKER_GROUP_DEFAULT",
    "EVAL_STREAM_DEFAULT",
    "EVAL_CONSUMER_GROUP_DEFAULT",
    "STREAM_PAYLOAD_FIELD",
    # the shared dead-letter graveyard derivation both lanes call (#668)
    "DEAD_LETTER_STREAM_ENV",
    "derive_dead_letter_stream_name",
    # inbound
    "Event",
    "Interrupt",
    "InboundMessage",
    "ADOPTION_CREDENTIAL_MAX_CHARS",
    "parse_adoption_credential",
    # outbound
    "TextDelta",
    "ToolNote",
    "Final",
    "ErrorEvent",
    "SideEffectFlag",
    "OutboundEvent",
    "OUTBOUND_EVENT_TYPES",
    "SessionStatus",
    # ndjson
    "to_ndjson_line",
    "dump_ndjson",
    "parse_ndjson_line",
    "parse_ndjson",
    "iter_ndjson",
    "parse_inbound",
    "to_inbound_json",
    "parse_queued_turn",
    "parse_eval_job",
    "READER_CONTEXT",
    "ProtocolVersionError",
    # conformance
    "run_conformance",
    "ConformanceReport",
    "Producer",
    "reference_producer",
]
