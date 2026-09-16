"""Curie runner: a claude-agent-sdk streaming session server speaking ACI v0.1.

Productizes the PT-2/PT-E prototypes into a long-lived, single-session HTTP server
that implements the frozen ACI contract from ``packages/aci-protocol``: it accepts
inbound event/interrupt frames, streams outbound NDJSON (text_delta / tool_note /
final / error / side_effect_flag) with protocol-version enforcement, honors the
ACI SessionConfig from the environment, enforces the per-run output-token budget,
flags non-idempotent tool calls, loads a validated plugin bundle, exports gen_ai
OTel spans to the collector, and rehydrates from a history ref on start.
"""

from .adapter import ClaudeAgentSession, ModelSession, build_options
from .budget import BUDGET_CLASSIFICATION, BudgetTracker
from .config import RunnerConfig
from .conformance import conformance_producer
from .fake import FakeModelSession
from .harness import HarnessContribution, discover_contributions, resolve_harness
from .hooks import load_bundle_hooks
from .memory import (
    ConsolidationResult,
    MemoryError,
    MemoryRecord,
    MemoryStore,
    NullMemoryStore,
    Provenance,
    StateApiMemoryStore,
    SupportsReplace,
    consolidate_memory,
    consolidate_records,
    format_memory_preamble,
    merge_provenance,
    resolve_memory,
)
from .otel import SCHEMA_VERSION, RunTracer, SpanAttributeKey, build_tracer_provider
from .plugin import (
    PluginBundleError,
    load_bundle_system_prompt,
    load_bundle_web_search_enabled,
    load_plugins,
)
from .server import create_app
from .session import SessionRunner
from .side_effects import (
    CLAUDE_READONLY_TOOLS,
    DEFAULT_IDEMPOTENT_TOOLS,
    PLATFORM_IDEMPOTENT_TOOLS,
    SideEffectClassifier,
)

__version__ = "0.0.0"

__all__ = [
    "ModelSession",
    "ClaudeAgentSession",
    "build_options",
    "BudgetTracker",
    "BUDGET_CLASSIFICATION",
    "RunnerConfig",
    "conformance_producer",
    "FakeModelSession",
    "RunTracer",
    "build_tracer_provider",
    "SpanAttributeKey",
    "SCHEMA_VERSION",
    "PluginBundleError",
    "load_plugins",
    "load_bundle_system_prompt",
    "load_bundle_web_search_enabled",
    "load_bundle_hooks",
    "create_app",
    "SessionRunner",
    "SideEffectClassifier",
    "DEFAULT_IDEMPOTENT_TOOLS",
    "CLAUDE_READONLY_TOOLS",
    "PLATFORM_IDEMPOTENT_TOOLS",
    "MemoryStore",
    "MemoryRecord",
    "Provenance",
    "NullMemoryStore",
    "StateApiMemoryStore",
    "MemoryError",
    "resolve_memory",
    "format_memory_preamble",
    "SupportsReplace",
    "ConsolidationResult",
    "consolidate_memory",
    "consolidate_records",
    "merge_provenance",
    "HarnessContribution",
    "discover_contributions",
    "resolve_harness",
]
