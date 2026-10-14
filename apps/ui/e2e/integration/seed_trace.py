"""Seed one OTLP trace into the dev Langfuse for the H1b integration E2E.

Emits a three-level span tree (agent.run -> llm.generation -> execute_tool x2)
to the OTel Collector, exactly like apps/api's own langfuse integration test.
Prints the trace id on stdout so the Playwright spec can wait for it to surface
through the API proxy. Run via `uv run python` so the workspace otel deps load.

The collector endpoint defaults to the documented single-stack port; set
CURIE_OTLP_TRACES_ENDPOINT to target a private stack's collector.
"""

import os
import sys

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import SpanKind

COLLECTOR_ENDPOINT = os.environ.get(
    "CURIE_OTLP_TRACES_ENDPOINT", "http://localhost:24318/v1/traces"
)
TRACE_NAME = "h1b-ui-wire-demo"


def main() -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": "h1b-ui-wire"}))
    provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=COLLECTOR_ENDPOINT))
    )
    tracer = provider.get_tracer("h1b-ui-wire")
    with tracer.start_as_current_span("agent.run", kind=SpanKind.SERVER) as root:
        root.set_attribute("langfuse.trace.name", TRACE_NAME)
        trace_id = format(root.get_span_context().trace_id, "032x")
        with tracer.start_as_current_span("llm.generation") as gen:
            gen.set_attribute("gen_ai.request.model", "claude-opus-4-8")
            gen.set_attribute("model", "claude-opus-4-8")
            gen.set_attribute("gen_ai.usage.input_tokens", 1200)
            gen.set_attribute("gen_ai.usage.output_tokens", 88)
            with tracer.start_as_current_span("execute_tool") as tool_a:
                tool_a.set_attribute("gen_ai.tool.name", "salesforce.get_deal")
            with tracer.start_as_current_span("execute_tool") as tool_b:
                tool_b.set_attribute("gen_ai.tool.name", "post_to_slack")
    provider.shutdown()
    sys.stdout.write(trace_id + "\n")


if __name__ == "__main__":
    main()
