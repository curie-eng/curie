"""Langfuse read proxy for the Runs view.

The UI reads traces through this API rather than talking to Langfuse directly
(detailed-architecture.md section 8, decision 4: proxy through our API for a
single auth domain). The observation tree is reconstructed from the flat
observation list via parentObservationId linkage, the PT-4-proven pattern (see
prototypes/observability/read_tree.py).
"""

import json
from datetime import datetime
from itertools import batched
from typing import Any

import httpx
from curie_telemetry_schema import SpanAttributeKey

from curie_api.schemas.observability import ObservationNode

from .config import Settings

# When filtering the trace list by an agent token, scan this many recent traces
# and keep the matches. Langfuse's list API has no substring filter, so the
# match is applied client-side over a bounded, newest-first scan.
_TRACE_SCAN_LIMIT = 500

# Langfuse's public list API caps page size at 100 and 400s anything larger, so
# every list request is clamped to this and paginated to gather more.
_MAX_PAGE_SIZE = 100

# Langfuse v4 ("langfuse_api_version = v4") has no trace read. A trace is
# rebuilt from its observations via Observations API v2, which is
# cursor-paginated, newest-first and totally ordered, so no `orderBy` is needed.
_V2_OBSERVATIONS = "/api/public/v2/observations"
_V2_METRICS = "/api/public/v2/metrics"

# v2 returns only the `core` and `basic` groups unless asked, so each read names
# the groups its consumers use. Trace summaries need tags (`trace_context`)
# and latency (`metrics`). Trees and eval-case promotion need model and usage.
_V2_OBSERVATION_FIELDS = "core,basic,time,io,metadata,model,usage"
_V2_TRACE_PARAMS = {
    "fields": "core,basic,time,io,metadata,metrics,trace_context",
    # Free-form strings the eval matrix reads, including its case/version keys.
    "expandMetadata": "case_id,version,model,detail,stream_id",
}
# Keep batches of trace IDs comfortably within HTTP request URL limits.
_V2_TRACE_BATCH_SIZE = 25

_V2_PAGE_BOUND = 50
# Pages read for one trace's roots; a trace has few logical roots.
_V2_TRACE_PAGE_BOUND = 10


def _v2_condition(
    kind: str, column: str, operator: str, value: Any
) -> dict[str, Any]:
    return {"type": kind, "column": column, "operator": operator, "value": value}


_ROOT_ONLY = _v2_condition("boolean", "isRootObservation", "=", True)


def _decode_io(value: Any) -> Any:
    """Restore JSON input/output that v2 returns as raw strings.

    Observations API v2 always returns `input`/`output` as the stored string,
    where the v3 API parsed JSON. Consumers (eval-case extraction, the matrix)
    expect the parsed value, and a non-JSON string stays as it is.
    """

    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _normalize_observation(obs: dict[str, Any]) -> dict[str, Any]:
    return {**obs, "input": _decode_io(obs.get("input")), "output": _decode_io(obs.get("output"))}


def _trace_from_observation(obs: dict[str, Any]) -> dict[str, Any]:
    """Shape the representative observation as a v3 trace dict.

    Callers keep reading `id`, `name`, `timestamp`, `input`, `output`,
    `metadata`, `tags` and `latency` (seconds, as before) unchanged.
    """

    return {
        "id": obs.get("traceId"),
        "name": obs.get("traceName") or obs.get("name"),
        "timestamp": obs.get("startTime"),
        "input": _decode_io(obs.get("input")),
        "output": _decode_io(obs.get("output")),
        "metadata": obs.get("metadata"),
        "tags": obs.get("tags") or [],
        "release": obs.get("release"),
        "version": obs.get("version"),
        "userId": obs.get("userId"),
        "sessionId": obs.get("sessionId"),
        "environment": obs.get("environment"),
        "public": obs.get("public"),
        "bookmarked": obs.get("bookmarked"),
        "latency": obs.get("latency"),
        "createdAt": obs.get("createdAt"),
        "updatedAt": obs.get("updatedAt"),
    }


def _trace_observation_order(obs: dict[str, Any]) -> tuple[bool, str]:
    return obs.get("isRootObservation") is not True, str(obs.get("startTime") or "")


def _traces_by_id(observations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Prefer each trace's earliest logical root, or its earliest exported row."""
    chosen: dict[str, dict[str, Any]] = {}
    for obs in observations:
        trace_id = obs.get("traceId")
        if not isinstance(trace_id, str):
            continue
        held = chosen.get(trace_id)
        if held is None or _trace_observation_order(obs) < _trace_observation_order(held):
            chosen[trace_id] = obs
    return {trace_id: _trace_from_observation(obs) for trace_id, obs in chosen.items()}


def to_v2_metrics_query(query: dict[str, Any]) -> dict[str, Any]:
    """Rewrite a v3 Metrics API query for Metrics API v2.

    v2 has no `traces` view. A per-trace aggregate becomes the same aggregate
    over root observations (`isRootObservation = true`), and the trace `name`
    column becomes `traceName`. Observation-view queries pass through as they
    are. A trace with several logical roots counts once per root.
    """

    if query.get("view") != "traces":
        return query

    def column(name: str) -> str:
        return "traceName" if name == "name" else name

    rewritten = {**query, "view": "observations"}
    rewritten["filters"] = [
        {**flt, "column": column(flt["column"])} for flt in query.get("filters") or []
    ] + [dict(_ROOT_ONLY)]
    if query.get("dimensions"):
        rewritten["dimensions"] = [
            {**dim, "field": column(dim["field"])} for dim in query["dimensions"]
        ]
    return rewritten


def matching_traces(
    traces: list[dict[str, Any]], name_contains: str, limit: int
) -> list[dict[str, Any]]:
    """Keep the traces whose `name` contains the token, capped at `limit`.

    The scan is newest-first (Langfuse returns traces most-recent-first), so the
    cap keeps the most recent matching runs.
    """

    out = [
        t
        for t in traces
        if isinstance(t.get("name"), str) and name_contains in t["name"]
    ]
    return out[:limit]


# The OTel resource attribute the runner stamps (runner/otel.py) so a trace is
# attributable to the concrete sandbox that served it.
_SANDBOX_ATTR = SpanAttributeKey.CURIE_SANDBOX_ID.value

# The OTel span attribute the runner stamps on the root agent.run span (ADR-0076
# Stone 3, #889) when a turn resumes a resolved approval.
_APPROVAL_DECISION_ATTR = SpanAttributeKey.APPROVAL_DECISION.value

# The OTel span attribute the runner stamps on each execute_tool span
# (runner/otel.py). Surfaced on the node because a trace consumer has to be able
# to tell WHICH tool ran, not merely that some tool ran: an assertion that an
# execute_tool observation exists still passes when the span for the one tool
# under test is the one that went missing.
_TOOL_NAME_ATTR = SpanAttributeKey.TOOL_NAME.value


def _probe_attr(bag: Any, key: str, *, bare_key: str | None = None) -> str | None:
    """Return a non-empty string attribute from one attribute bag, or None.

    Checks the bag itself plus the common keys Langfuse uses to carry OTel
    span/resource attributes (``metadata`` / ``resourceAttributes`` /
    ``resource``, each optionally wrapping an ``attributes`` sub-dict).
    ``bare_key``, when given, is accepted as a shorter alias (e.g. the bare
    ``sandbox_id`` for ``curie.sandbox_id``). Non-string / empty values are
    ignored.
    """

    if not isinstance(bag, dict):
        return None
    candidates: list[dict[str, Any]] = [bag]
    for nest in ("metadata", "resourceAttributes", "resource"):
        nested = bag.get(nest)
        if isinstance(nested, dict):
            candidates.append(nested)
            inner = nested.get("attributes")
            if isinstance(inner, dict):
                candidates.append(inner)
    for cand in candidates:
        hit = cand.get(key) or (cand.get(bare_key) if bare_key else None)
        if isinstance(hit, str) and hit.strip():
            return hit
    return None


def hoist_sandbox_id(
    trace: dict[str, Any], observations: list[dict[str, Any]]
) -> str | None:
    """Lift the runner's sandbox id out of a trace, or None when absent.

    Checks, in order, the trace-level resource/metadata attributes then the
    first observation's resource attributes, so the id resolves regardless of
    whether Langfuse surfaces the OTel resource attr on the trace or only on an
    observation. Only a present, non-empty attribute is returned -- no value is
    invented.
    """

    hit = _probe_attr(trace, _SANDBOX_ATTR, bare_key="sandbox_id")
    if hit:
        return hit
    if observations:
        return _probe_attr(observations[0], _SANDBOX_ATTR, bare_key="sandbox_id")
    return None


def hoist_approval_decision(
    trace: dict[str, Any], observations: list[dict[str, Any]]
) -> str | None:
    """Lift the runner's approval-gate decision out of a trace, or None.

    Prefer trace metadata, then inspect each observation in response order.
    The resumed ``agent.run`` span carries the attribute (ADR-0076 Stone 3,
    #889), but a correlated trace begins at dispatcher ingress and includes
    reply spans, so the annotated observation need not be first. Absent for
    the ordinary case (no approval gate was resumed); no value invented.
    """

    hit = _probe_attr(trace, _APPROVAL_DECISION_ATTR)
    if hit:
        return hit
    for observation in observations:
        hit = _probe_attr(observation, _APPROVAL_DECISION_ATTR)
        if hit:
            return hit
    return None


def build_tree(observations: list[dict[str, Any]]) -> list[ObservationNode]:
    """Reconstruct the nested observation tree from a flat observation list.

    Nodes are linked by parentObservationId; any observation whose parent is
    missing from the set (or absent) is treated as a root. Children are ordered
    by startTime so the tree renders in execution order.
    """

    children: dict[str | None, list[dict[str, Any]]] = {}
    for obs in observations:
        children.setdefault(obs.get("parentObservationId"), []).append(obs)
    known_ids = {obs["id"] for obs in observations}

    def node_for(obs: dict[str, Any]) -> ObservationNode:
        kids = sorted(
            children.get(obs["id"], []), key=lambda o: o.get("startTime") or ""
        )
        return ObservationNode(
            id=obs["id"],
            type=obs.get("type", ""),
            name=obs.get("name"),
            startTime=obs.get("startTime"),
            model=obs.get("model"),
            usageDetails=obs.get("usageDetails"),
            toolName=_probe_attr(obs, _TOOL_NAME_ATTR),
            children=[node_for(k) for k in kids],
        )

    roots = [
        obs
        for obs in observations
        if not obs.get("parentObservationId")
        or obs.get("parentObservationId") not in known_ids
    ]
    roots.sort(key=lambda o: o.get("startTime") or "")
    return [node_for(root) for root in roots]


class LangfuseReadLimitExceeded(RuntimeError):
    """A complete trace read exceeds the configured pagination bound."""


class LangfuseTraceNotFound(LookupError):
    """No trace row is readable yet (the v4 counterpart of a trace-GET 404)."""


class LangfuseClient:
    """Thin async client over Langfuse's public read API.

    With `langfuse_api_version = "v3"` (the default) it calls the legacy traces,
    observations and metrics endpoints. With `"v4"` it calls Observations API v2
    and Metrics API v2 and returns the same shapes, so callers do not change.
    """

    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self._base = settings.langfuse_host.rstrip("/")
        self._auth = (settings.langfuse_public_key, settings.langfuse_secret_key)
        self._client = client
        self._v4 = settings.langfuse_api_version == "v4"

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        resp = await self._client.get(
            f"{self._base}{path}", params=params, auth=self._auth
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data

    async def _get_all(
        self,
        path: str,
        params: dict[str, Any],
        max_items: int | None = None,
        max_pages: int = 50,
        require_complete: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch pages of a paginated list endpoint (data[] + meta.totalPages).

        Page size is clamped to Langfuse's hard cap of 100 (it 400s anything
        larger). Stops once `max_items` are gathered (if set) or pages run out.
        """

        items: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            body = await self._get(
                path, {**params, "page": page, "limit": _MAX_PAGE_SIZE}
            )
            meta = body.get("meta") or {}
            if require_complete and int(meta.get("totalPages", page)) > max_pages:
                raise LangfuseReadLimitExceeded("complete trace read exceeds page bound")
            items.extend(body.get("data", []))
            if max_items is not None and len(items) >= max_items:
                return items[:max_items]
            meta = body.get("meta") or {}
            if page >= int(meta.get("totalPages", page)):
                break
        return items

    async def _get_cursor_all(
        self,
        params: dict[str, Any],
        max_pages: int = _V2_PAGE_BOUND,
        require_complete: bool = False,
        max_traces: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch bounded pages, optionally stopping after discovery of enough trace IDs."""
        items: list[dict[str, Any]] = []
        trace_ids: set[str] = set()
        cursor: str | None = None
        for _ in range(max_pages):
            page_params = {**params, "limit": _MAX_PAGE_SIZE}
            if cursor:
                page_params["cursor"] = cursor
            body = await self._get(_V2_OBSERVATIONS, page_params)
            page = body.get("data", [])
            items.extend(page)
            cursor = (body.get("meta") or {}).get("cursor")
            if not cursor:
                return items
            if max_traces is not None:
                trace_ids.update(
                    row["traceId"] for row in page if isinstance(row.get("traceId"), str)
                )
                if len(trace_ids) >= max_traces:
                    return items
        if require_complete:
            raise LangfuseReadLimitExceeded("complete trace read exceeds page bound")
        return items

    async def _list_traces_v4(
        self, limit: int, name_contains: str | None
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        params = {"fields": "core"}
        if name_contains is not None:
            params["filter"] = json.dumps(
                [_v2_condition("string", "traceName", "contains", name_contains)]
            )
        # The first match per trace determines recent activity rank. Fetch the
        # summary separately because an earlier root can be on a later page.
        activity = await self._get_cursor_all(params, max_traces=limit)
        trace_ids = list(
            dict.fromkeys(row["traceId"] for row in activity if isinstance(row.get("traceId"), str))
        )[:limit]
        traces: dict[str, dict[str, Any]] = {}
        for batch in batched(trace_ids, _V2_TRACE_BATCH_SIZE, strict=False):
            traces.update(await self._read_v4_trace_summaries(batch))
        return [traces[trace_id] for trace_id in trace_ids if trace_id in traces]

    async def _read_v4_trace_summaries(
        self, trace_ids: tuple[str, ...]
    ) -> dict[str, dict[str, Any]]:
        def trace_filter(ids: tuple[str, ...]) -> dict[str, str]:
            if len(ids) == 1:
                return {"traceId": ids[0]}
            condition = _v2_condition("stringOptions", "traceId", "any of", ids)
            return {"filter": json.dumps([condition])}

        roots = await self._get_cursor_all(
            {**_V2_TRACE_PARAMS, **trace_filter(trace_ids), "isRootObservation": "true"},
            max_pages=_V2_TRACE_PAGE_BOUND if len(trace_ids) == 1 else _V2_PAGE_BOUND,
            require_complete=True,
        )
        traces = _traces_by_id(roots)
        rootless_ids = tuple(trace_id for trace_id in trace_ids if trace_id not in traces)
        if rootless_ids:
            rows = await self._get_cursor_all(
                {**_V2_TRACE_PARAMS, **trace_filter(rootless_ids)}, require_complete=True
            )
            traces.update(_traces_by_id(rows))
        return traces

    async def list_traces(
        self, limit: int, name_contains: str | None = None
    ) -> list[dict[str, Any]]:
        if self._v4:
            return await self._list_traces_v4(limit, name_contains)
        if name_contains is None:
            return await self._get_all("/api/public/traces", {}, max_items=limit)
        # Filter to one agent's traces. Langfuse's list API has no substring
        # filter, so scan the most recent traces and match `name contains` here.
        scanned = await self._get_all(
            "/api/public/traces", {}, max_items=_TRACE_SCAN_LIMIT
        )
        return matching_traces(scanned, name_contains, limit)

    async def get_trace(self, trace_id: str) -> dict[str, Any]:
        if self._v4:
            traces = await self._read_v4_trace_summaries((trace_id,))
            if trace_id not in traces:
                raise LangfuseTraceNotFound(trace_id)
            return traces[trace_id]
        return await self._get(f"/api/public/traces/{trace_id}", {})

    async def get_observations(self, trace_id: str) -> list[dict[str, Any]]:
        if self._v4:
            rows = await self._get_cursor_all(
                {
                    "traceId": trace_id,
                    "fields": _V2_OBSERVATION_FIELDS,
                    # Keep the attributes the probes read below untruncated.
                    "expandMetadata": ",".join(
                        (_SANDBOX_ATTR, _APPROVAL_DECISION_ATTR, _TOOL_NAME_ATTR)
                    ),
                },
                require_complete=True,
            )
            return [_normalize_observation(row) for row in rows]
        body = await self._get(
            "/api/public/observations", {"traceId": trace_id, "limit": 100}
        )
        observations: list[dict[str, Any]] = body.get("data", [])
        return observations

    async def list_traces_by_tags(
        self,
        tags: list[str],
        *,
        from_timestamp: datetime | None = None,
        fields: str | None = None,
        order_by: str | None = None,
        require_complete: bool = False,
    ) -> list[dict[str, Any]]:
        """Every trace carrying all of the given tags (e.g. suite:<name>).

        `fields` and `order_by` are Traces API (v3) parameters. Under v4 they
        are ignored: the trace fields come from the v2 groups above, and the v2
        cursor order keeps observations with tied start times across pages.
        Trace rows are ordered by the selected observation's start time.
        """

        if self._v4:
            conditions = [_v2_condition("arrayOptions", "tags", "all of", tags)]
            if from_timestamp is not None:
                conditions.append(
                    _v2_condition(
                        "datetime",
                        "startTime",
                        ">=",
                        from_timestamp.isoformat(timespec="milliseconds"),
                    )
                )
            observations = await self._get_cursor_all(
                {**_V2_TRACE_PARAMS, "filter": json.dumps(conditions)},
                require_complete=require_complete,
            )
            return sorted(
                _traces_by_id(observations).values(),
                key=lambda trace: str(trace.get("timestamp") or ""),
                reverse=True,
            )

        params: dict[str, Any] = {"tags": tags}
        if from_timestamp is not None:
            params["fromTimestamp"] = from_timestamp.isoformat(timespec="milliseconds")
        if fields is not None:
            params["fields"] = fields
        if order_by is not None:
            params["orderBy"] = order_by
        return await self._get_all(
            "/api/public/traces", params, require_complete=require_complete
        )

    async def query_metrics(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Run a Langfuse Metrics API query and return its data rows.

        The query is the Langfuse metrics query object (view/metrics/dimensions/
        filters/timeDimension/fromTimestamp/toTimestamp); it is sent url-encoded
        as the ``query`` parameter. Rows key each metric as
        ``<aggregation>_<measure>`` (e.g. ``count_count``, ``sum_totalCost``).
        """

        if self._v4:
            body = await self._get(
                _V2_METRICS, {"query": json.dumps(to_v2_metrics_query(query))}
            )
        else:
            body = await self._get("/api/public/metrics", {"query": json.dumps(query)})
        rows: list[dict[str, Any]] = body.get("data", [])
        return rows
