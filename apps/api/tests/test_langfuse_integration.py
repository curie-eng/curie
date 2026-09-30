"""Real Langfuse integration: seed a trace via OTLP, read it back through the proxy.

Exercises the production ingest path (app -> OTel Collector -> Langfuse) and the
read path (proxy -> Langfuse observations API -> reconstructed tree). Skips when
the dev stack is not reachable so the unit suite stays runnable standalone.
"""

import os
import time
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import SpanKind

COLLECTOR_ENDPOINT = os.environ.get(
    "TEST_OTEL_COLLECTOR_ENDPOINT", "http://localhost:24318/v1/traces"
)
COLLECTOR_PROBE = COLLECTOR_ENDPOINT.removesuffix("/v1/traces")
LANGFUSE_HOST = (
    os.environ.get("TEST_LANGFUSE_HOST")
    or os.environ.get("LANGFUSE_HOST")
    or get_settings().langfuse_host
)


def _stack_up() -> bool:
    try:
        httpx.get(f"{LANGFUSE_HOST}/api/public/health", timeout=2.0).raise_for_status()
        httpx.get(COLLECTOR_PROBE, timeout=2.0)
    except Exception:
        return False
    return True


def _emit_three_level_trace() -> str:
    provider = TracerProvider(
        resource=Resource.create({"service.name": "b1-integration"})
    )
    provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=COLLECTOR_ENDPOINT))
    )
    tracer = provider.get_tracer("b1-integration")
    with tracer.start_as_current_span("agent.run", kind=SpanKind.SERVER) as root:
        root.set_attribute("langfuse.trace.name", "b1-integration-demo")
        trace_id = format(root.get_span_context().trace_id, "032x")
        with tracer.start_as_current_span("llm.generation") as gen:
            gen.set_attribute("gen_ai.request.model", "claude-opus-4-8")
            gen.set_attribute("model", "claude-opus-4-8")
            gen.set_attribute("gen_ai.usage.input_tokens", 1200)
            gen.set_attribute("gen_ai.usage.output_tokens", 88)
            with tracer.start_as_current_span("execute_tool") as tool_a:
                tool_a.set_attribute("gen_ai.tool.name", "search_repo")
            with tracer.start_as_current_span("execute_tool") as tool_b:
                tool_b.set_attribute("gen_ai.tool.name", "write_file")
    provider.shutdown()
    return trace_id


def _max_depth(nodes: list[dict[str, Any]]) -> int:
    if not nodes:
        return 0
    return 1 + max(_max_depth(n.get("children", [])) for n in nodes)


@pytest.mark.skipif(not _stack_up(), reason="dev compose stack not reachable")
def test_proxy_returns_reconstructed_tree_for_seeded_trace(
    client: TestClient, auth_headers: dict[str, str],
) -> None:
    trace_id = _emit_three_level_trace()

    deadline = time.time() + 60
    body: dict[str, Any] | None = None
    while time.time() < deadline:
        resp = client.get(
            f"/langfuse/traces/{trace_id}", headers=auth_headers
        )
        if resp.status_code == 200 and _max_depth(resp.json()["tree"]) >= 3:
            body = resp.json()
            break
        time.sleep(2)

    assert body is not None, "seeded trace never reached the proxy with depth >= 3"
    assert _max_depth(body["tree"]) >= 3
    # The model-bearing span maps to a GENERATION somewhere in the tree.
    flat: list[dict[str, Any]] = []

    def _walk(nodes: list[dict[str, Any]]) -> None:
        for node in nodes:
            flat.append(node)
            _walk(node.get("children", []))

    _walk(body["tree"])
    assert any(n["type"] == "GENERATION" for n in flat)


@pytest.mark.skipif(not _stack_up(), reason="dev compose stack not reachable")
@pytest.mark.parametrize("decision", ["approved", None])
def test_proxy_reads_approval_on_non_root_observation(
    client: TestClient, auth_headers: dict[str, str], decision: str | None,
) -> None:
    provider = TracerProvider(
        resource=Resource.create({"service.name": "approval-correlation-integration"})
    )
    provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=COLLECTOR_ENDPOINT))
    )
    tracer = provider.get_tracer("approval-correlation-integration")
    try:
        with tracer.start_as_current_span("curie.queue.enqueue") as root:
            trace_id = format(root.get_span_context().trace_id, "032x")
            with tracer.start_as_current_span("agent.run") as resumed:
                if decision is not None:
                    resumed.set_attribute("gen_ai.approval.decision", decision)
            with tracer.start_as_current_span("curie.reply.update"):
                pass
    finally:
        provider.shutdown()

    deadline = time.time() + 60
    body: dict[str, Any] | None = None
    while time.time() < deadline:
        resp = client.get(f"/langfuse/traces/{trace_id}", headers=auth_headers)
        if resp.status_code == 200:
            candidate = resp.json()
            tree = candidate["tree"]
            if (
                len(tree) == 1
                and tree[0]["name"] == "curie.queue.enqueue"
                and {child["name"] for child in tree[0]["children"]}
                == {"agent.run", "curie.reply.update"}
                and candidate["approval_decision"] == decision
            ):
                body = candidate
                break
        time.sleep(2)
    assert body is not None, "correlated approval metadata did not reach the exact proxy read"
    assert body["approval_decision"] == decision
