"""Langfuse read proxy for the Runs view.

The UI reads traces through this API rather than talking to Langfuse directly
(detailed-architecture.md section 8, decision 4: proxy through our API for a
single auth domain). The observation tree is reconstructed from the flat
observation list via parentObservationId linkage, the PT-4-proven pattern (see
prototypes/observability/read_tree.py).
"""

import json
from typing import Any

import httpx

from .config import Settings
from .schemas import ObservationNode

# When filtering the trace list by an agent token, scan this many recent traces
# and keep the matches. Langfuse's list API has no substring filter, so the
# match is applied client-side over a bounded, newest-first scan.
_TRACE_SCAN_LIMIT = 500

# Langfuse's public list API caps page size at 100 and 400s anything larger, so
# every list request is clamped to this and paginated to gather more.
_MAX_PAGE_SIZE = 100


def matching_traces(
    traces: list[dict[str, Any]], name_contains: str, limit: int
) -> list[dict[str, Any]]:
    """Keep the traces whose `name` contains the token, capped at `limit`.

    The scan is newest-first (Langfuse returns traces most-recent-first), so the
    cap keeps the most recent matching runs.
    """

    out = [t for t in traces if isinstance(t.get("name"), str) and name_contains in t["name"]]
    return out[:limit]


# The OTel resource attribute the runner stamps (runner/otel.py) so a trace is
# attributable to the concrete sandbox that served it.
_SANDBOX_ATTR = "curie.sandbox_id"

# The OTel span attribute the runner stamps on the root agent.run span (ADR-0076
# Stone 3, #889) when a turn resumes a resolved approval.
_APPROVAL_DECISION_ATTR = "gen_ai.approval.decision"


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


def hoist_sandbox_id(trace: dict[str, Any], observations: list[dict[str, Any]]) -> str | None:
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

    Mirrors ``hoist_sandbox_id``'s trace-then-first-observation probe. The
    attribute is stamped on the root ``agent.run`` span rather than as a
    provider-wide resource attribute (ADR-0076 Stone 3, #889), so it is
    ordinarily trace-level; the observation fallback stays for the same
    reason ``hoist_sandbox_id`` keeps it -- Langfuse's OTLP ingestion doesn't
    guarantee which level surfaces a given span attribute. Absent for the
    ordinary case (no approval gate was resumed this turn); no value invented.
    """

    hit = _probe_attr(trace, _APPROVAL_DECISION_ATTR)
    if hit:
        return hit
    if observations:
        return _probe_attr(observations[0], _APPROVAL_DECISION_ATTR)
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
        kids = sorted(children.get(obs["id"], []), key=lambda o: o.get("startTime") or "")
        return ObservationNode(
            id=obs["id"],
            type=obs.get("type", ""),
            name=obs.get("name"),
            startTime=obs.get("startTime"),
            model=obs.get("model"),
            usageDetails=obs.get("usageDetails"),
            children=[node_for(k) for k in kids],
        )

    roots = [
        obs
        for obs in observations
        if not obs.get("parentObservationId") or obs.get("parentObservationId") not in known_ids
    ]
    roots.sort(key=lambda o: o.get("startTime") or "")
    return [node_for(root) for root in roots]


class LangfuseClient:
    """Thin async client over Langfuse's public read API."""

    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self._base = settings.langfuse_host.rstrip("/")
        self._auth = (settings.langfuse_public_key, settings.langfuse_secret_key)
        self._client = client

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        resp = await self._client.get(f"{self._base}{path}", params=params, auth=self._auth)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data

    async def _get_all(
        self,
        path: str,
        params: dict[str, Any],
        max_items: int | None = None,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch pages of a paginated list endpoint (data[] + meta.totalPages).

        Page size is clamped to Langfuse's hard cap of 100 (it 400s anything
        larger). Stops once `max_items` are gathered (if set) or pages run out.
        """

        items: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            body = await self._get(path, {**params, "page": page, "limit": _MAX_PAGE_SIZE})
            items.extend(body.get("data", []))
            if max_items is not None and len(items) >= max_items:
                return items[:max_items]
            meta = body.get("meta") or {}
            if page >= int(meta.get("totalPages", page)):
                break
        return items

    async def list_traces(
        self, limit: int, name_contains: str | None = None
    ) -> list[dict[str, Any]]:
        if name_contains is None:
            return await self._get_all("/api/public/traces", {}, max_items=limit)
        # Filter to one agent's traces. Langfuse's list API has no substring
        # filter, so scan the most recent traces and match `name contains` here.
        scanned = await self._get_all("/api/public/traces", {}, max_items=_TRACE_SCAN_LIMIT)
        return matching_traces(scanned, name_contains, limit)

    async def get_trace(self, trace_id: str) -> dict[str, Any]:
        return await self._get(f"/api/public/traces/{trace_id}", {})

    async def get_observations(self, trace_id: str) -> list[dict[str, Any]]:
        body = await self._get("/api/public/observations", {"traceId": trace_id, "limit": 100})
        observations: list[dict[str, Any]] = body.get("data", [])
        return observations

    async def list_traces_by_tags(self, tags: list[str]) -> list[dict[str, Any]]:
        """Every trace carrying all of the given tags (e.g. suite:<name>)."""

        return await self._get_all("/api/public/traces", {"tags": tags})

    async def query_metrics(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Run a Langfuse Metrics API query and return its data rows.

        The query is the Langfuse metrics query object (view/metrics/dimensions/
        filters/timeDimension/fromTimestamp/toTimestamp); it is sent url-encoded
        as the ``query`` parameter. Rows key each metric as
        ``<aggregation>_<measure>`` (e.g. ``count_count``, ``sum_totalCost``).
        """

        body = await self._get("/api/public/metrics", {"query": json.dumps(query)})
        rows: list[dict[str, Any]] = body.get("data", [])
        return rows
