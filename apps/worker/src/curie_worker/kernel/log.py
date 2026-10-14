from __future__ import annotations

import logging

from curie_telemetry import operation_span, record_metric

logger = logging.getLogger("curie_worker.kernel")

# One telemetry binding. Call sites use log.record_metric and log.operation_span.
__all__ = ["logger", "operation_span", "record_metric"]
