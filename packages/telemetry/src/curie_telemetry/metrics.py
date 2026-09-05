"""Manifest gated low cardinality operational metrics."""

from __future__ import annotations

import math
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Final

from opentelemetry.sdk.metrics import MeterProvider

from .context import _normalize_trace_context

_SERVICE_NAMES: Final = [
    "curie-api",
    "curie-dispatcher",
    "curie-worker",
    "curie-runner",
]
_TURN_SOURCES: Final = ["api", "dispatcher", "worker", "runner", "local", "eval"]
_TURN_OUTCOMES: Final = [
    "done",
    "awaiting_approval",
    "budget_halted",
    "classified_failure",
    "side_effect_halted",
    "idle",
    "interrupted",
    # "deadline_halted" (#2278): the worker lifecycle's wall-clock delivery
    # deadline (ADR-0131), distinct from model-spend ``budget_halted``.
    # ``record_metric`` raises on an out-of-domain outcome, so omitting this
    # value crashes terminal completion after the turn has already settled
    # and leaves the stream entry pending.
    "deadline_halted",
]


def _definition(
    instrument_type: str,
    unit: str,
    description: str,
    monotonic: bool,
    attributes: Mapping[str, list[str]],
) -> dict[str, Any]:
    domains = {key: list(values) for key, values in attributes.items()}
    bound = math.prod(len(values) for values in domains.values())
    return {
        "type": instrument_type,
        "unit": unit,
        "description": description,
        "monotonic": monotonic,
        "attributes": domains,
        "cardinality_bound": bound,
    }


_TURN_ATTRIBUTES = {
    "service.name": _SERVICE_NAMES,
    "source": _TURN_SOURCES,
    "outcome": ["accepted"],
}
_TURN_COMPLETED_ATTRIBUTES = {
    "service.name": _SERVICE_NAMES,
    "source": _TURN_SOURCES,
    "outcome": _TURN_OUTCOMES,
}
_HISTORY_CACHE_ATTRIBUTES = {
    "service.name": ["curie-runner"],
    "source": ["runner"],
    "cache_hit": ["true", "false"],
}
_QUEUE_ATTRIBUTES = {
    "service.name": ["curie-api", "curie-dispatcher", "curie-worker"],
    "source": ["api", "dispatcher", "worker", "local", "eval"],
    "outcome": ["success", "failure", "pending", "ack", "retry", "dead-letter"],
}
_QUEUE_RETRY_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "source": ["worker", "eval"],
    # "runner-timeout" (#2011): the runner streaming budget expiring is its own
    # retry cause, distinct from the generic "runner-error". It MUST be declared
    # here -- ``record_metric`` raises on an out-of-domain attribute value, so a
    # retryable classification missing from this allowlist crashes the worker on
    # the retry emission rather than silently dropping a point.
    # "workspace-error" (#2004): a managed-workspace preparation failure before
    # the turn was ever accepted, named apart from "runner-error" for the same
    # reason -- and, being retryable, subject to the same crash-on-omission.
    "retry_class": [
        "redelivery",
        "rate-limit",
        "runner-error",
        "runner-timeout",
        "workspace-error",
    ],
}
_THREAD_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "source": ["worker"],
    "outcome": ["acquired", "contended", "timeout", "start", "steer", "finish-race", "observed"],
}
_SANDBOX_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "operation": ["claim", "resume", "release", "suspend", "cleanup"],
    "outcome": [
        "claimed",
        "reused",
        "resumed",
        "released",
        "suspended",
        "failed",
        "orphan-cleaned",
        "observed",
    ],
}
_SANDBOX_INVENTORY_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "operation": ["observe"],
    "outcome": ["observed"],
}
_RUNNER_RPC_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "operation": ["event", "steer", "interrupt", "reset", "status", "timeout"],
    "role": ["client"],
    "outcome": ["success", "failure", "conflict", "timeout"],
}
_APPROVAL_ATTRIBUTES = {
    "service.name": ["curie-api", "curie-worker"],
    "operation": ["request", "resolve", "expire", "suspend", "resume", "observe"],
    "outcome": [
        "requested",
        "resolved",
        "expired",
        "suspended",
        "resumed",
        "pending",
        "observed",
        "failure",
    ],
}
_APPROVAL_PENDING_ATTRIBUTES = {
    "service.name": ["curie-api"],
    "operation": ["observe"],
    "outcome": ["pending"],
}
_REPLY_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "operation": ["update", "post"],
    "role": ["client"],
    "outcome": ["success", "failure", "best-effort", "retry", "rate-limited"],
}
_REPLY_RETRY_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "operation": ["update", "post"],
    "role": ["client"],
    "retry_class": ["block-fallback", "rate-limit", "transport-fallback"],
}
_HTTP_OPERATIONS = [
    "/actions",
    "/actions/{action_id}",
    "/actions/{action_id}/audit",
    "/actions/{action_id}/complete",
    "/actions/{action_id}/undo",
    "/agents",
    "/agents/{agent_id}",
    "/agents/{agent_id}/behavior-packs",
    "/agents/{agent_id}/budget",
    "/agents/{agent_id}/channels",
    "/agents/{agent_id}/cost",
    "/agents/{agent_id}/kill",
    "/agents/{agent_id}/memory",
    "/agents/{agent_id}/memory/{index}",
    "/agents/{agent_id}/memory/{index}/provenance",
    "/agents/{agent_id}/resume",
    "/agents/{agent_id}/state",
    "/agents/{agent_id}/state/bindings/{kind}/{address}",
    "/agents/{agent_id}/state/bindings/{kind}/{address}/{namespace}",
    "/agents/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}",
    "/agents/{agent_id}/state/bindings/{kind}/{address}/{namespace}/{key}/append",
    "/agents/{agent_id}/state/{namespace}",
    "/agents/{agent_id}/state/{namespace}/{key}",
    "/agents/{agent_id}/state/{namespace}/{key}/append",
    "/agents/{agent_id}/threads/{thread_key}/reset",
    "/agents/{agent_id}/versions",
    "/agents/{agent_id}/versions/{version_id}/bundle",
    "/agents/{agent_id}/versions/{version_id}/connectors",
    "/agents/{agent_id}/versions/{version_id}/files",
    "/approvals",
    "/approvals/principals/operator",
    "/approvals/{approval_id}",
    "/approvals/{approval_id}/audit",
    "/approvals/{approval_id}/resolve",
    "/channels/token",
    "/channels/turns",
    "/cluster-message-replies/{reply_ref}",
    "/config",
    "/console/login-codes",
    "/console/session",
    "/deploy-targets/list",
    "/deploy-targets/resolve",
    "/deployments",
    "/deployments/{deployment_id}",
    "/evals/matrix",
    "/evals/report",
    "/evals/trigger",
    "/git-flow/routing-check",
    "/github/webhook",
    "/health",
    "/hooks/{agent_id}/{hook}",
    "/langfuse/traces",
    "/langfuse/traces/{trace_id}",
    "/langfuse/traces/{trace_id}/eval-case",
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
    "/observability/metrics/series",
    "/observability/metrics/summary",
    "/observability/runners",
    "/observability/runners/{namespace}/{pod}/logs",
    "/publications",
    "/publications/{publication_id}",
    "/v1/internal/cluster-message-replies/{reply_ref}",
    "/v1/internal/publications",
    "/v1/internal/publications/lineage",
    "/v1/internal/publications/{publication_id}/lineage",
    "/v1/internal/publications/{publication_id}/credential",
    "/v1/internal/workspaces/{deployment_id}/credential",
    "/v1/internal/workspaces/{deployment_id}/selection",
    "unmatched",
]
_HTTP_ATTRIBUTES = {
    "service.name": ["curie-api"],
    "operation": _HTTP_OPERATIONS,
    "role": ["server"],
    "source": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "OTHER"],
    "outcome": ["1xx", "2xx", "3xx", "4xx", "5xx"],
}
_HTTP_ACTIVE_ATTRIBUTES = {
    key: values for key, values in _HTTP_ATTRIBUTES.items() if key != "outcome"
}
_BACKGROUND_ATTRIBUTES = {
    "service.name": ["curie-api", "curie-worker"],
    "operation": [
        "resume-reconciler",
        "approval-sweeper",
        "graveyard-watcher",
        "commit-poller",
        "connector-reconciler",
    ],
    "role": ["background"],
    "outcome": ["success", "failure", "observed"],
}
_BACKGROUND_AGE_ATTRIBUTES = {
    key: values for key, values in _BACKGROUND_ATTRIBUTES.items() if key != "outcome"
}
_EVAL_ATTRIBUTES = {
    "service.name": ["curie-worker"],
    "source": ["eval"],
    "outcome": ["success", "failure", "plumbing"],
}


_METRICS: dict[str, dict[str, Any]] = {
    "curie.turn.accepted": _definition(
        "counter", "{turn}", "Turns accepted for processing.", True, _TURN_ATTRIBUTES
    ),
    "curie.turn.completed": _definition(
        "counter", "{turn}", "Turns reaching a terminal result.", True, _TURN_COMPLETED_ATTRIBUTES
    ),
    "curie.turn.duration": _definition(
        "histogram", "s", "End to end turn duration.", False, _TURN_COMPLETED_ATTRIBUTES
    ),
    "curie.history.resume.cache_read": _definition(
        "histogram",
        "{token}",
        "Provider cache read input tokens on the first turn after structured replay.",
        False,
        _HISTORY_CACHE_ATTRIBUTES,
    ),
    "curie.queue.enqueue": _definition(
        "counter", "{message}", "Messages enqueued.", True, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.process": _definition(
        "counter", "{message}", "Messages processed.", True, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.settle": _definition(
        "counter", "{message}", "Message settlement outcomes.", True, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.retry": _definition(
        "counter",
        "{retry}",
        "Bounded queue redelivery and in-process turn retries.",
        True,
        _QUEUE_RETRY_ATTRIBUTES,
    ),
    "curie.queue.dead_letter": _definition(
        "counter", "{message}", "Messages dead lettered.", True, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.wait.duration": _definition(
        "histogram", "s", "Queue wait duration.", False, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.process.duration": _definition(
        "histogram", "s", "Queue processing duration.", False, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.message.age": _definition(
        "histogram", "s", "Message age at processing.", False, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.pending": _definition(
        "gauge", "{message}", "Pending consumer group messages.", False, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.lag": _definition(
        "gauge", "{message}", "Consumer group lag.", False, _QUEUE_ATTRIBUTES
    ),
    "curie.queue.depth": _definition(
        "gauge", "{message}", "Stream depth.", False, _QUEUE_ATTRIBUTES
    ),
    "curie.thread.lock.wait.duration": _definition(
        "histogram", "s", "Thread lock acquisition duration.", False, _THREAD_ATTRIBUTES
    ),
    "curie.thread.route": _definition(
        "counter", "{decision}", "Thread routing decisions.", True, _THREAD_ATTRIBUTES
    ),
    "curie.thread.route.active": _definition(
        "gauge", "{route}", "Active thread routes.", False, _THREAD_ATTRIBUTES
    ),
    "curie.sandbox.lifecycle": _definition(
        "counter", "{operation}", "Sandbox lifecycle outcomes.", True, _SANDBOX_ATTRIBUTES
    ),
    "curie.sandbox.claim.duration": _definition(
        "histogram", "s", "Sandbox claim duration.", False, _SANDBOX_ATTRIBUTES
    ),
    "curie.sandbox.resume.duration": _definition(
        "histogram", "s", "Sandbox resume duration.", False, _SANDBOX_ATTRIBUTES
    ),
    "curie.sandbox.release.duration": _definition(
        "histogram", "s", "Sandbox release duration.", False, _SANDBOX_ATTRIBUTES
    ),
    "curie.sandbox.active": _definition(
        "gauge", "{sandbox}", "Active sandboxes.", False, _SANDBOX_INVENTORY_ATTRIBUTES
    ),
    "curie.sandbox.suspended": _definition(
        "gauge", "{sandbox}", "Suspended sandboxes.", False, _SANDBOX_INVENTORY_ATTRIBUTES
    ),
    "curie.sandbox.cleanup": _definition(
        "counter", "{sandbox}", "Failed and orphan sandbox cleanup.", True, _SANDBOX_ATTRIBUTES
    ),
    "curie.runner.rpc.request.duration": _definition(
        "histogram", "s", "Runner RPC request duration.", False, _RUNNER_RPC_ATTRIBUTES
    ),
    "curie.runner.rpc.result": _definition(
        "counter", "{request}", "Runner RPC outcomes.", True, _RUNNER_RPC_ATTRIBUTES
    ),
    "curie.approval.lifecycle": _definition(
        "counter", "{approval}", "Approval lifecycle outcomes.", True, _APPROVAL_ATTRIBUTES
    ),
    "curie.approval.pending": _definition(
        "gauge", "{approval}", "Pending approvals.", False, _APPROVAL_PENDING_ATTRIBUTES
    ),
    "curie.approval.pending.age": _definition(
        "gauge", "s", "Age of the oldest pending approval.", False, _APPROVAL_PENDING_ATTRIBUTES
    ),
    "curie.reply.delivery": _definition(
        "counter", "{reply}", "Reply delivery outcomes.", True, _REPLY_ATTRIBUTES
    ),
    "curie.reply.update.duration": _definition(
        "histogram", "s", "Reply update duration.", False, _REPLY_ATTRIBUTES
    ),
    "curie.reply.post.duration": _definition(
        "histogram", "s", "Reply post duration.", False, _REPLY_ATTRIBUTES
    ),
    "curie.reply.retry": _definition(
        "counter", "{retry}", "Reply delivery retries.", True, _REPLY_RETRY_ATTRIBUTES
    ),
    "curie.http.server.request": _definition(
        "counter", "{request}", "HTTP server requests.", True, _HTTP_ATTRIBUTES
    ),
    "curie.http.server.request.duration": _definition(
        "histogram", "s", "HTTP server request duration.", False, _HTTP_ATTRIBUTES
    ),
    "curie.http.server.active": _definition(
        "up_down_counter",
        "{request}",
        "Active HTTP server requests.",
        False,
        _HTTP_ACTIVE_ATTRIBUTES,
    ),
    "curie.background.loop": _definition(
        "counter", "{run}", "Background loop pass outcomes.", True, _BACKGROUND_ATTRIBUTES
    ),
    "curie.background.last_success.age": _definition(
        "gauge",
        "s",
        "Age of the last successful background pass.",
        False,
        _BACKGROUND_AGE_ATTRIBUTES,
    ),
    "curie.eval.process": _definition(
        "counter", "{job}", "Eval processing outcomes.", True, _EVAL_ATTRIBUTES
    ),
}

_provider: MeterProvider | None = None
_instruments: dict[str, Any] = {}


def declared_metric_manifest() -> dict[str, Any]:
    return {
        "schema_version": "v1",
        "metrics": deepcopy(_METRICS),
    }


def configure_meter_provider(provider: MeterProvider) -> MeterProvider:
    """Create the declared instruments once for the supplied provider."""

    global _provider, _instruments
    if provider is _provider:
        return provider
    meter = provider.get_meter("curie-telemetry")
    instruments: dict[str, Any] = {}
    for name, definition in _METRICS.items():
        factory = {
            "counter": meter.create_counter,
            "up_down_counter": meter.create_up_down_counter,
            "histogram": meter.create_histogram,
            "gauge": meter.create_gauge,
        }[definition["type"]]
        instruments[name] = factory(
            name,
            unit=definition["unit"],
            description=definition["description"],
        )
    _provider = provider
    _instruments = instruments
    return provider


def record_metric(
    name: str,
    value: float = 1,
    *,
    attributes: Mapping[str, str] | None = None,
) -> None:
    """Validate and record one declared point."""

    definition = _METRICS.get(name)
    if definition is None:
        raise ValueError(f"undeclared metric {name!r}")
    supplied = dict(attributes or {})
    domains: dict[str, list[str]] = definition["attributes"]
    unknown = set(supplied) - set(domains)
    if unknown:
        raise ValueError(f"undeclared attribute for {name}: {sorted(unknown)!r}")
    missing = set(domains) - set(supplied)
    if missing:
        raise ValueError(f"missing declared attribute for {name}: {sorted(missing)!r}")
    for key, item in supplied.items():
        if item not in domains[key]:
            raise ValueError(f"attribute {key!r} value {item!r} is outside its declared domain")
    if not math.isfinite(float(value)):
        raise ValueError("metric value must be finite")
    instrument = _instruments.get(name)
    if instrument is None:
        return
    context = _normalize_trace_context()
    instrument_type = definition["type"]
    if instrument_type in {"counter", "up_down_counter"}:
        instrument.add(value, supplied, context=context)
    elif instrument_type == "histogram":
        instrument.record(value, supplied, context=context)
    else:
        instrument.set(value, supplied, context=context)
