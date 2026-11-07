"""Read one Prometheus instant query, in the shape a remediation verifier samples.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-17. One tool over
streamable HTTP at ``/mcp``:

* ``query_value {query}`` (read-only) runs ``GET /api/v1/query`` against the
  one configured Prometheus and replies with structured content.

Why this exists
---------------
ADR 0203 remediation verifies every action through a read-only tool of a
connector other than the acting one (``verifier_not_independent``). The
platform's runner applies one RFC 6901 pointer to the reply's structured
content and the API applies one comparator to the scalar it finds
(docs/operations.md, "Automated remediation"). This connector is the read side
for the SRE example's opt-in scaler: it holds no Kubernetes credential and
cannot write anything.

The reply
---------
``{"resultType": <type>, "data": ...}``, built for the documented pointers:

* a vector is a list, ordered by the canonical JSON of each series' labels so
  ``/data/0`` names the same series however Prometheus ordered its answer:
  ``{"resultType": "vector", "data": [{"metric": {...}, "value": [<unix time>,
  "<sample>"]}, ...]}``. ``/data/0/value/1`` is the first series' sample, a
  decimal string the ordering comparators read as a number. An empty vector
  leaves that pointer absent, which only the ``absent`` comparator satisfies;
  ``... or vector(0)`` turns "no series" into a number.
* a scalar is ``{"resultType": "scalar", "data": {"value": "<sample>",
  "time": <unix time>}}``, so ``/data/value`` is the sample.

A query that answers more than one series and is read at ``/data/0`` reads
whichever series sorts first: aggregate (``max``, ``sum``) to one series.

Failing closed
--------------
Everything else is a tool error, which the platform records as an unsuccessful
sample: no ``METRICS_URL``, an empty or oversized query, a transport error, a
timeout, a redirect (never followed), a non-2xx status, a body past
``METRICS_MAX_RESPONSE_BYTES``, a body that is not JSON, a ``status`` other
than ``success``, a range (``matrix``) or ``string`` result, a malformed
sample, and more than ``METRICS_MAX_SERIES`` series. A range result is refused
because its latest sample cannot be addressed by one pointer.

``NaN`` and ``±Inf`` samples are passed through as Prometheus writes them; they
are not decimal strings, so no ordering comparator is ever satisfied by one.

Nothing here logs the query, a sample or the token.
"""

# NOTE: no `from __future__ import annotations`, deliberately, as in the other
# connectors: MCPServer introspects tool signatures and string annotations break
# that at import time.

import json
import logging
import math
import os
import sys
from typing import Any, Final
from urllib.parse import urlsplit

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import StrictStr

log = logging.getLogger("metrics")

QUERY_PATH: Final = "/api/v1/query"
MAX_QUERY_CHARS: Final = 4096
INSTANT_TYPES: Final = frozenset({"vector", "scalar"})


class ConfigError(ValueError):
    """A configuration variable is malformed. The message never carries the token."""


def _positive_number(env: dict[str, str], name: str, default: str) -> float:
    raw = env.get(name, default).strip()
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{name} is not a number") from None
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{name} is not a positive number")
    return value


def _positive_integer(env: dict[str, str], name: str, default: str) -> int:
    raw = env.get(name, default).strip()
    if not raw.isdigit() or int(raw) <= 0:
        raise ConfigError(f"{name} is not a positive integer")
    return int(raw)


class Settings:
    """Where to read and how much, read once at start.

    An empty ``METRICS_URL`` is not an error at start: every call is refused
    instead, so a declared but unconfigured connector fails closed and says why
    on each call. A URL that is not ``http`` or ``https`` refuses to start.
    """

    def __init__(self, env: dict[str, str]) -> None:
        url = env.get("METRICS_URL", "").strip()
        if url:
            parts = urlsplit(url)
            if parts.scheme not in ("http", "https") or not parts.netloc:
                raise ConfigError("METRICS_URL is not an http or https URL")
        self.url = url.rstrip("/")
        self.token = env.get("METRICS_BEARER_TOKEN", "").strip()
        self.timeout = _positive_number(env, "METRICS_TIMEOUT_SECONDS", "10")
        self.max_series = _positive_integer(env, "METRICS_MAX_SERIES", "50")
        self.max_response_bytes = _positive_integer(env, "METRICS_MAX_RESPONSE_BYTES", "262144")


# --------------------------------------------------------------------------- #
# Reading Prometheus.
# --------------------------------------------------------------------------- #


def _sample(value: Any, where: str) -> tuple[float, str]:
    """``[<unix time>, "<sample>"]``, or ``ToolError``."""

    if (
        not isinstance(value, list)
        or len(value) != 2
        or not isinstance(value[0], int | float)
        or isinstance(value[0], bool)
        or not isinstance(value[1], str)
    ):
        raise ToolError(f"Prometheus answered a malformed {where} sample")
    return value[0], value[1]


def _labels_key(series: dict[str, Any]) -> str:
    return json.dumps(series["metric"], sort_keys=True, separators=(",", ":"))


def _refuse_error(body: dict[str, Any]) -> None:
    """Raise for a Prometheus error object, naming only its ``errorType``.

    The ``error`` text can quote the query, so it is never repeated.
    """

    error_type = body.get("errorType")
    kind = error_type if isinstance(error_type, str) and error_type.isidentifier() else "error"
    raise ToolError(f"Prometheus refused the query ({kind})")


def _reply(body: Any, max_series: int) -> dict[str, Any]:
    """The structured reply for one Prometheus answer, or ``ToolError``."""

    if not isinstance(body, dict):
        raise ToolError("Prometheus did not answer a JSON object")
    if body.get("status") != "success":
        _refuse_error(body)
    data = body.get("data")
    if not isinstance(data, dict):
        raise ToolError("Prometheus answered no data")
    result_type = data.get("resultType")
    result = data.get("result")
    if result_type not in INSTANT_TYPES:
        raise ToolError(
            "the query's result is not a vector or a scalar; a range or string "
            "result has no single sample a verifier can read"
        )
    if result_type == "scalar":
        time, sample = _sample(result, "scalar")
        return {"resultType": "scalar", "data": {"value": sample, "time": time}}
    if not isinstance(result, list):
        raise ToolError("Prometheus answered a malformed vector")
    if len(result) > max_series:
        raise ToolError(
            f"the query answered {len(result)} series, more than this connector's "
            f"limit of {max_series}; aggregate it to fewer"
        )
    series: list[dict[str, Any]] = []
    for item in result:
        if not isinstance(item, dict) or not isinstance(item.get("metric"), dict):
            raise ToolError("Prometheus answered a malformed vector series")
        metric = item["metric"]
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in metric.items()):
            raise ToolError("Prometheus answered a malformed vector series")
        time, sample = _sample(item.get("value"), "vector")
        series.append({"metric": metric, "value": [time, sample]})
    series.sort(key=_labels_key)
    return {"resultType": "vector", "data": series}


def _read(settings: Settings, query: str) -> Any:
    """The decoded JSON body of one instant query, or ``ToolError``."""

    headers = {"Accept": "application/json"}
    if settings.token:
        headers["Authorization"] = f"Bearer {settings.token}"
    try:
        with httpx.Client(timeout=settings.timeout, follow_redirects=False) as client:
            with client.stream(
                "GET", settings.url + QUERY_PATH, params={"query": query}, headers=headers
            ) as response:
                status = response.status_code
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > settings.max_response_bytes:
                        raise ToolError(
                            "Prometheus answered more than this connector's limit of "
                            f"{settings.max_response_bytes} bytes"
                        )
                    chunks.append(chunk)
    except httpx.TimeoutException:
        raise ToolError(f"Prometheus did not answer within {settings.timeout:g}s") from None
    except httpx.HTTPError as exc:
        raise ToolError(f"could not reach Prometheus: {type(exc).__name__}") from None
    raw = b"".join(chunks)
    if 300 <= status < 400:
        raise ToolError(f"Prometheus answered a redirect ({status}); redirects are not followed")
    try:
        body = json.loads(raw, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        if status >= 400:
            raise ToolError(f"Prometheus answered {status}") from None
        raise ToolError("Prometheus did not answer JSON") from None
    if status >= 400:
        if isinstance(body, dict) and body.get("status") == "error":
            # Prometheus's own error object names the kind.
            _refuse_error(body)
        raise ToolError(f"Prometheus answered {status}")
    return body


def _reject_constant(constant: str) -> Any:
    raise ValueError(constant)


# --------------------------------------------------------------------------- #
# The server.
# --------------------------------------------------------------------------- #

READ_ONLY = ToolAnnotations(
    read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False
)


def build(settings: Settings) -> MCPServer:
    """The MCP server for one configuration."""

    mcp = MCPServer("metrics")

    @mcp.tool(annotations=READ_ONLY)
    def query_value(query: StrictStr) -> dict[str, Any]:
        """Run one PromQL instant query and return its value.

        Read-only. The reply is a JSON object: ``resultType`` and ``data``. A
        vector's ``data`` is a list of ``{"metric", "value": [<unix time>,
        "<sample>"]}`` ordered by labels, so ``/data/0/value/1`` is the first
        series' sample; a scalar's is ``{"value": "<sample>", "time"}``. Aggregate
        to one series (``max(...)``, ``sum(...)``), and use ``... or vector(0)``
        when "no series" should read as zero. A range selector result, an error
        or more series than the connector allows is a tool error.
        """

        if not settings.url:
            raise ToolError("METRICS_URL is not set: this connector refuses every call")
        if not query.strip():
            raise ToolError("query is empty")
        if len(query) > MAX_QUERY_CHARS:
            raise ToolError(f"query is longer than {MAX_QUERY_CHARS} characters")
        try:
            reply = _reply(_read(settings, query), settings.max_series)
        except ToolError as exc:
            log.info("query_value refused: %s", exc)
            raise
        log.info("query_value answered a %s", reply["resultType"])
        return reply

    return mcp


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)
    # httpx logs every request URL at INFO, and the URL carries the query.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        settings = Settings(dict(os.environ))
    except ConfigError as exc:
        log.error("refusing to start: %s", exc)
        return 1
    log.info(
        "metrics: url %s, token %s, timeout %gs, at most %d series and %d bytes",
        "set" if settings.url else "unset",
        "held" if settings.token else "absent",
        settings.timeout,
        settings.max_series,
        settings.max_response_bytes,
    )
    build(settings).run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
