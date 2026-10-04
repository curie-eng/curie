"""Token usage and estimated cost per factory run (#3223).

Pinned interface (the implementation must provide), all in
``curie_api.factory_usage`` unless noted:

- ``Price``: per-token Decimal rates ``prompt``, ``completion``, ``cache_read``,
  ``cache_write`` plus ``source: str`` and ``as_of: datetime`` (keyword ctor).
- ``PriceBook`` protocol: ``async price(model: str) -> Price | None``.
- ``get_price_book``: a FastAPI dependency the usage ingress route resolves its
  price book through. Tests inject a fake with
  ``app.dependency_overrides[get_price_book] = lambda: fake``. No network.
- ``async work_item_usage(session, work_item_id)`` backs the read route.
- ``POST /v1/work-item-progress/{request_id}/usage`` (``work_item.progress``
  token, 201 on record and on replay). Body:
  ``{"turn_id", "primary_model", "models": [{"model", "input_tokens",
  "cached_input_tokens", "cache_write_tokens", "output_tokens"}]}``.
- ``GET /work-items/{id}/usage`` (platform key) returns ``work_item_id``,
  ``total_tokens``, ``estimated_cost_usd``, ``cost_complete``, ``roles``
  (``{"implementer": {"tokens", "estimated_cost_usd"}, "reviewer": {...}}``),
  ``models`` (per model: ``model``, ``role``, the four token counts,
  ``estimated_cost_usd``), ``requests`` (``request_id``, ``tokens``,
  ``estimated_cost_usd``), ``pr_number``, ``pr_url``, ``price_sources``
  (``source``, ``as_of``). Money is a JSON number or decimal string.
- Table ``curie.execution_request_model_usage`` (see the plan).
- The terminal status comment carries one ``Usage:`` line above FINAL_MARKER.

Admission and terminal comment plumbing reuse test_factory_terminus.py.
"""

from __future__ import annotations

import sys
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner.usage_report import UsageReporter

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api import sandbox_token
from curie_api.config import get_settings
from curie_api.factory_notices import FINAL_MARKER
from curie_api.factory_usage import Price, get_price_book
from test_factory_status_comment import _admit, _execute, _finish_failed, _marked
from test_factory_terminus import (  # noqa: F401  (fixtures)
    _reconcile,
    _request,
    _rows,
    _start_running,
    admitted,
    comments,
)

pytestmark = pytest.mark.usefixtures("clean_db")

IMPLEMENTER = "example-org/implementer-model-PLACEHOLDER"
REVIEWER = "example-org/reviewer-model-PLACEHOLDER"
UNPRICED = "example-org/unpriced-model-PLACEHOLDER"
SOURCE = "https://prices.example.com/api/v1/models"
AS_OF = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
USAGE_LINE = (
    "Usage: implementer 1,500,000 tokens ($2.00), reviewers 300,000 tokens ($0.40), "
    f"total $2.40 (estimated from {SOURCE} prices as of 2026-09-01)"
)


class _FakePriceBook:
    """Fabricated rates: 1 USD per million prompt tokens, 2 per million completion."""

    def __init__(self) -> None:
        self.asked: list[str] = []

    async def price(self, model: str) -> Price | None:
        self.asked.append(model)
        if model not in {IMPLEMENTER, REVIEWER}:
            return None
        return Price(
            prompt=Decimal("0.000001"),
            completion=Decimal("0.000002"),
            cache_read=Decimal("0.0000001"),
            cache_write=Decimal("0.000003"),
            source=SOURCE,
            as_of=AS_OF,
        )


@pytest.fixture
def priced(admitted: Any) -> Any:  # noqa: F811
    client, github, sink = admitted
    book = _FakePriceBook()
    client.app.dependency_overrides[get_price_book] = lambda: book
    yield client, github, sink, book
    client.app.dependency_overrides.pop(get_price_book, None)


def _token(request_id: uuid.UUID, *, scope: str = "work_item.progress") -> str:
    return sandbox_token.mint(
        get_settings().api_key,
        agent=str(request_id),
        scope=scope,
        exp=int(time.time()) + 3600,
    )


def _entry(model: str, inp: int, out: int, cached: int = 0, write: int = 0) -> dict[str, Any]:
    return {
        "model": model,
        "input_tokens": inp,
        "cached_input_tokens": cached,
        "cache_write_tokens": write,
        "output_tokens": out,
    }


def _usage(
    client: Any,
    request_id: uuid.UUID,
    models: list[dict[str, Any]],
    *,
    turn_id: str = "turn-1",
    primary: str | None = IMPLEMENTER,
    token: str | None = None,
    path_id: uuid.UUID | None = None,
) -> Any:
    return client.post(
        f"/v1/work-item-progress/{path_id or request_id}/usage",
        headers={"X-API-Key": token if token is not None else _token(request_id)},
        json={"turn_id": turn_id, "primary_model": primary, "models": models},
    )


def _stored(request_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT turn_id, model, role, input_tokens, cached_input_tokens, "
        "cache_write_tokens, output_tokens, estimated_cost_usd, price_source, price_as_of "
        "FROM curie.execution_request_model_usage "
        "WHERE execution_request_id = :id ORDER BY model",
        {"id": request_id},
    )


def _money(value: Any) -> Decimal:
    return Decimal(str(value))


STANDARD = [_entry(IMPLEMENTER, 1_000_000, 500_000), _entry(REVIEWER, 200_000, 100_000)]


# --- authentication -------------------------------------------------------------


def test_wrong_credentials_are_401_and_store_nothing(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9601)
    wrong = {
        "state scope": _token(request_id, scope="state"),
        "another request": _token(uuid.uuid4()),
        "platform key": get_settings().api_key,
        "empty": "",
    }
    for name, token in wrong.items():
        response = _usage(client, request_id, STANDARD, token=token)
        assert response.status_code == 401, (name, response.text)
    other = _usage(client, request_id, STANDARD, path_id=uuid.uuid4())
    assert other.status_code == 401, other.text
    assert _stored(request_id) == []


# --- record, role split, pricing, dedup -----------------------------------------


def test_a_report_is_stored_per_model_with_role_and_price(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9602)

    response = _usage(
        client,
        request_id,
        [_entry(IMPLEMENTER, 1_000_000, 500_000, cached=100_000, write=10_000),
         _entry(REVIEWER, 200_000, 100_000)],
    )

    assert response.status_code == 201, response.text
    rows = {row["model"]: row for row in _stored(request_id)}
    assert set(rows) == {IMPLEMENTER, REVIEWER}
    impl = rows[IMPLEMENTER]
    assert impl["role"] == "implementer"
    assert (impl["input_tokens"], impl["cached_input_tokens"]) == (1_000_000, 100_000)
    assert (impl["cache_write_tokens"], impl["output_tokens"]) == (10_000, 500_000)
    # 1.00 prompt + 0.01 cache read + 0.03 cache write + 1.00 completion
    assert impl["estimated_cost_usd"] == Decimal("2.040000")
    assert impl["price_source"] == SOURCE
    assert impl["price_as_of"] == AS_OF
    assert rows[REVIEWER]["role"] == "reviewer"
    assert rows[REVIEWER]["estimated_cost_usd"] == Decimal("0.400000")


def test_a_replayed_turn_is_a_no_op(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9603)
    assert _usage(client, request_id, STANDARD, turn_id="turn-a").status_code == 201
    replay = _usage(client, request_id, STANDARD, turn_id="turn-a")
    assert replay.status_code == 201, replay.text
    assert len(_stored(request_id)) == 2
    assert _usage(client, request_id, STANDARD, turn_id="turn-b").status_code == 201
    assert len(_stored(request_id)) == 4


def test_an_unknown_model_keeps_tokens_with_no_cost_and_no_source(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9604)
    response = _usage(
        client,
        request_id,
        [_entry(IMPLEMENTER, 1000, 10), _entry(UNPRICED, 700, 70)],
    )
    assert response.status_code == 201, response.text
    rows = {row["model"]: row for row in _stored(request_id)}
    unpriced = rows[UNPRICED]
    assert (unpriced["input_tokens"], unpriced["output_tokens"]) == (700, 70)
    assert unpriced["role"] == "reviewer"
    assert unpriced["estimated_cost_usd"] is None
    assert unpriced["price_source"] is None
    assert unpriced["price_as_of"] is None
    assert rows[IMPLEMENTER]["estimated_cost_usd"] is not None


def test_a_single_model_with_no_primary_is_the_implementer(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9605)
    response = _usage(client, request_id, [_entry(REVIEWER, 10, 1)], primary=None)
    assert response.status_code == 201, response.text
    assert [row["role"] for row in _stored(request_id)] == ["implementer"]


@pytest.mark.parametrize(
    "models",
    [
        [_entry(f"example-org/m{i}", 1, 1) for i in range(21)],
        [_entry(IMPLEMENTER, 10**12 + 1, 1)],
        [_entry(IMPLEMENTER, -1, 1)],
    ],
)
def test_out_of_bounds_reports_are_refused(priced: Any, models: list[dict[str, Any]]) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9606)
    response = _usage(client, request_id, models)
    assert response.status_code == 422, response.text
    assert _stored(request_id) == []


# --- aggregation and the read route -----------------------------------------------


def _second_request(work_item_id: uuid.UUID, first: uuid.UUID) -> uuid.UUID:
    """A CI-fix round: a second request on the same work item."""

    second = uuid.uuid4()
    _execute(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, requester, "
        "reply_kind, reply_address, reply_conversation_id) "
        "SELECT :id, work_item_id, 2, 'waiting', clock_timestamp() + interval '30 seconds', "
        "objective, requester, reply_kind, reply_address, reply_conversation_id "
        "FROM curie.execution_requests WHERE id = :first",
        {"id": second, "first": first},
    )
    _execute(
        "UPDATE curie.work_items SET next_sequence = 3 WHERE id = :id", {"id": work_item_id}
    )
    return second


def test_usage_sums_across_rounds_of_one_work_item_only(priced: Any) -> None:
    client, github, sink, _book = priced
    first = _admit(client, github, sink, 9607)
    work_item_id = _request(9607)["work_item_id"]
    epoch = _start_running(first)
    assert _usage(client, first, STANDARD).status_code == 201
    _finish_failed(client, first, epoch, "ci_failed")
    second = _second_request(work_item_id, first)
    assert _usage(client, second, STANDARD, turn_id="turn-round-2").status_code == 201

    stranger = _admit(client, github, sink, 9608)
    assert _usage(client, stranger, STANDARD).status_code == 201

    headers = {"X-API-Key": get_settings().api_key}
    response = client.get(f"/work-items/{work_item_id}/usage", headers=headers)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["work_item_id"] == str(work_item_id)
    assert body["total_tokens"] == 2 * 1_800_000
    assert _money(body["estimated_cost_usd"]) == Decimal("4.80")
    assert body["cost_complete"] is True
    assert body["roles"]["implementer"]["tokens"] == 3_000_000
    assert _money(body["roles"]["implementer"]["estimated_cost_usd"]) == Decimal("4.00")
    assert body["roles"]["reviewer"]["tokens"] == 600_000
    assert _money(body["roles"]["reviewer"]["estimated_cost_usd"]) == Decimal("0.80")
    by_model = {entry["model"]: entry for entry in body["models"]}
    assert by_model[IMPLEMENTER]["role"] == "implementer"
    assert by_model[IMPLEMENTER]["input_tokens"] == 2_000_000
    assert by_model[REVIEWER]["output_tokens"] == 200_000
    assert {entry["request_id"] for entry in body["requests"]} == {str(first), str(second)}
    assert all(entry["tokens"] == 1_800_000 for entry in body["requests"])
    assert [entry["source"] for entry in body["price_sources"]] == [SOURCE]
    assert "pr_number" in body and "pr_url" in body


def test_usage_with_an_unpriced_model_is_not_cost_complete(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9609)
    work_item_id = _request(9609)["work_item_id"]
    _usage(client, request_id, [_entry(IMPLEMENTER, 1000, 10), _entry(UNPRICED, 5, 5)])
    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["cost_complete"] is False
    assert body["total_tokens"] == 1020


def test_a_zero_token_row_for_an_unpriced_model_is_stored_unpriced_and_costs_zero(
    priced: Any,
) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9640)
    work_item_id = _request(9640)["work_item_id"]
    _start_running(request_id)

    response = _usage(client, request_id, [_entry(UNPRICED, 0, 0)], primary=UNPRICED)

    assert response.status_code == 201, response.text
    (row,) = _stored(request_id)
    assert row["role"] == "implementer"
    assert (row["input_tokens"], row["output_tokens"]) == (0, 0)
    assert row["estimated_cost_usd"] is None
    assert row["price_source"] is None
    assert row["price_as_of"] is None
    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["total_tokens"] == 0
    assert body["requests_without_usage"] == 0
    assert body["cost_complete"] is True
    assert _money(body["estimated_cost_usd"]) == Decimal(0)


def test_a_nonzero_unpriced_row_stays_incomplete_beside_a_zero_unpriced_row(
    priced: Any,
) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9641)
    work_item_id = _request(9641)["work_item_id"]
    _start_running(request_id)
    assert _usage(client, request_id, [_entry(UNPRICED, 0, 0)], primary=UNPRICED).status_code == 201
    assert (
        _usage(
            client, request_id, [_entry(UNPRICED, 5, 5)], turn_id="turn-2", primary=UNPRICED
        ).status_code
        == 201
    )

    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["total_tokens"] == 10
    assert body["cost_complete"] is False


def test_the_blocked_run_comment_shows_the_explanation_and_a_complete_zero_usage_line(
    priced: Any,
) -> None:
    explanation = (
        "Could not complete: in-sandbox verification is unavailable before "
        "implementation, so no model round ran. Check units_rs: package_registry."
    )
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9642)
    _reconcile()
    epoch = _start_running(request_id)
    assert _usage(client, request_id, [_entry(UNPRICED, 0, 0)], primary=UNPRICED).status_code == 201
    _finish_failed(client, request_id, epoch, "early_stop", detail=explanation)
    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert explanation in body
    assert "Cause: early_stop" in body
    lines = body.splitlines()
    assert "Usage: implementer 0 tokens ($0.00), total $0.00" in lines
    assert body.index("Usage: implementer 0 tokens") < body.index(FINAL_MARKER)


def test_the_read_route_needs_the_platform_key_and_404s_unknown_items(priced: Any) -> None:
    client, github, sink, _book = priced
    _admit(client, github, sink, 9610)
    work_item_id = _request(9610)["work_item_id"]
    headers = {"X-API-Key": get_settings().api_key}
    assert client.get(f"/work-items/{uuid.uuid4()}/usage", headers=headers).status_code == 404
    assert client.get(f"/work-items/{work_item_id}/usage").status_code == 401
    empty = client.get(f"/work-items/{work_item_id}/usage", headers=headers)
    assert empty.status_code == 200, empty.text
    assert empty.json()["total_tokens"] == 0


# --- the terminal status comment ---------------------------------------------------


def test_the_terminal_comment_states_the_split_above_the_final_marker(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9611)
    _reconcile()
    epoch = _start_running(request_id)
    assert _usage(client, request_id, STANDARD).status_code == 201
    _finish_failed(client, request_id, epoch, "runner_escalated")
    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    lines = body.splitlines()
    assert USAGE_LINE in lines
    assert body.index(USAGE_LINE) < body.index(FINAL_MARKER)
    cause = [line for line in lines if line.startswith("Cause: ")]
    assert cause == ["Cause: runner_escalated"]


def test_an_unpriced_role_says_cost_unknown(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9612)
    _reconcile()
    epoch = _start_running(request_id)
    _usage(client, request_id, [_entry(IMPLEMENTER, 1_000_000, 500_000), _entry(UNPRICED, 5, 5)])
    _finish_failed(client, request_id, epoch, "runner_escalated")
    _reconcile()

    (comment,) = _marked(sink, request_id)
    (usage,) = [line for line in comment["body"].splitlines() if line.startswith("Usage:")]
    assert "implementer 1,500,000 tokens ($2.00)" in usage
    assert "reviewers 10 tokens (cost unknown)" in usage


def test_a_run_with_no_usage_says_so(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9613)
    _reconcile()
    epoch = _start_running(request_id)
    _finish_failed(client, request_id, epoch, "runner_escalated")
    _reconcile()

    (comment,) = _marked(sink, request_id)
    (usage,) = [line for line in comment["body"].splitlines() if line.startswith("Usage:")]
    assert usage == "Usage: not reported for 1 run"
    assert "Cause: runner_escalated" in comment["body"]
    assert FINAL_MARKER in comment["body"]


# --- role from the runner (#3223 review) --------------------------------------------
#
# Pinned: each ``models`` entry may carry ``role`` ("implementer" | "reviewer").
# When present it is authoritative over the primary_model comparison, and the
# dedup key is (request, turn_id, model, role).


def _roled(model: str, role: str, inp: int, out: int) -> dict[str, Any]:
    return {**_entry(model, inp, out), "role": role}


def test_an_explicit_role_wins_over_the_primary_model(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9620)
    response = _usage(
        client,
        request_id,
        [_roled(IMPLEMENTER, "reviewer", 10, 1), _roled(REVIEWER, "implementer", 20, 2)],
    )
    assert response.status_code == 201, response.text
    rows = {row["model"]: row["role"] for row in _stored(request_id)}
    assert rows == {IMPLEMENTER: "reviewer", REVIEWER: "implementer"}


def test_an_unknown_role_is_refused(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9621)
    response = _usage(client, request_id, [_roled(IMPLEMENTER, "judge", 10, 1)])
    assert response.status_code == 422, response.text
    assert _stored(request_id) == []


def test_one_model_in_two_roles_is_stored_twice_and_aggregated_by_role(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9622)
    work_item_id = _request(9622)["work_item_id"]
    _reconcile()
    epoch = _start_running(request_id)
    models = [
        _roled(IMPLEMENTER, "implementer", 1_000_000, 500_000),
        _roled(IMPLEMENTER, "reviewer", 200_000, 100_000),
    ]
    assert _usage(client, request_id, models).status_code == 201
    replay = _usage(client, request_id, models)
    assert replay.status_code == 201, replay.text
    stored = sorted((row["model"], row["role"]) for row in _stored(request_id))
    assert stored == [(IMPLEMENTER, "implementer"), (IMPLEMENTER, "reviewer")]

    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["roles"]["implementer"]["tokens"] == 1_500_000
    assert body["roles"]["reviewer"]["tokens"] == 300_000
    by = {(entry["model"], entry["role"]): entry for entry in body["models"]}
    assert set(by) == {(IMPLEMENTER, "implementer"), (IMPLEMENTER, "reviewer")}

    _finish_failed(client, request_id, epoch, "runner_escalated")
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert USAGE_LINE in comment["body"].splitlines()


# --- a missing report is not a complete zero (#3223 review) --------------------------
#
# Pinned: GET /work-items/{id}/usage returns ``requests_without_usage``, the count
# of the work item's requests with ``started_at`` set and no usage rows;
# ``cost_complete`` is false when it is > 0. The terminal Usage line then ends
# with ", usage not reported for N run" (N == 1) / "runs" (N > 1), placed after
# the price source parenthetical.

MISSING_SUFFIX = ", usage not reported for 1 run"


def test_a_started_request_with_no_usage_is_not_a_complete_zero(priced: Any) -> None:
    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9623)
    work_item_id = _request(9623)["work_item_id"]
    _start_running(request_id)
    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["total_tokens"] == 0
    assert body["requests_without_usage"] == 1
    assert body["cost_complete"] is False


def test_a_never_started_request_is_not_counted_as_missing(priced: Any) -> None:
    client, github, sink, _book = priced
    _admit(client, github, sink, 9624)
    work_item_id = _request(9624)["work_item_id"]
    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["requests_without_usage"] == 0
    assert body["cost_complete"] is True


def test_the_terminal_line_names_runs_that_reported_no_usage(priced: Any) -> None:
    client, github, sink, _book = priced
    first = _admit(client, github, sink, 9625)
    work_item_id = _request(9625)["work_item_id"]
    _reconcile()
    epoch = _start_running(first)
    _finish_failed(client, first, epoch, "ci_failed")
    second = _second_request(work_item_id, first)
    epoch2 = _start_running(second)
    assert _usage(client, second, STANDARD).status_code == 201

    headers = {"X-API-Key": get_settings().api_key}
    body = client.get(f"/work-items/{work_item_id}/usage", headers=headers).json()
    assert body["requests_without_usage"] == 1
    assert body["cost_complete"] is False

    _finish_failed(client, second, epoch2, "runner_escalated")
    _reconcile()
    (comment,) = _marked(sink, second)
    (usage,) = [line for line in comment["body"].splitlines() if line.startswith("Usage:")]
    assert usage.endswith(MISSING_SUFFIX), usage
    assert "total at least $2.40" in usage


@pytest.mark.parametrize("include_reviewer_totals", [True, False])
def test_a_follow_up_turn_stores_only_its_increment_and_keeps_the_reviewer_role(
    priced: Any, include_reviewer_totals: bool,
) -> None:
    """Two turns of one request store the running model_usage once.

    The runner posts each turn's own slice. The stored rows and the terminal
    Usage line must sum to that slice, with the reviewer model still reviewer
    after a follow-up that never sees the reviewer again.
    """

    captured: list[dict[str, Any]] = []

    def _sdk_usage(inp: int, out: int) -> dict[str, int]:
        return {
            "input_tokens": inp,
            "output_tokens": out,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }

    def _model_usage(inp: int, out: int) -> dict[str, int]:
        return {
            "inputTokens": inp,
            "outputTokens": out,
            "cacheReadInputTokens": 0,
            "cacheCreationInputTokens": 0,
        }

    def _assistant(
        model: str, usage: dict[str, int], *, parent: str | None = None
    ) -> AssistantMessage:
        return AssistantMessage(
            content=[TextBlock(text="x")],
            model=model,
            parent_tool_use_id=parent,
            usage=usage,
        )

    def _result(**overrides: Any) -> ResultMessage:
        fields: dict[str, Any] = {
            "subtype": "success",
            "duration_ms": 1,
            "duration_api_ms": 1,
            "is_error": False,
            "num_turns": 1,
            "session_id": "sdk-session-PLACEHOLDER",
            "result": "done",
        }
        fields.update(overrides)
        return ResultMessage(**fields)

    async def drive() -> None:
        app = web.Application()

        async def usage(request: web.Request) -> web.Response:
            captured.append(await request.json())
            return web.json_response({"recorded": True}, status=201)

        app.router.add_post("/v1/work-item-progress/{request_id}/usage", usage)
        server = TestServer(app)
        await server.start_server()
        try:
            url = str(server.make_url("/v1/work-item-progress/example-request/usage"))
            reporter = UsageReporter(url, "sbx.example-usage-token.signature")
            reporter.observe(_assistant(IMPLEMENTER, _sdk_usage(1_000_000, 500_000)))
            reporter.observe(
                _assistant(REVIEWER, _sdk_usage(200_000, 100_000), parent="toolu_example")
            )
            reviewer_totals = (
                {REVIEWER: _model_usage(200_000, 100_000)} if include_reviewer_totals else {}
            )
            await reporter.report(
                _result(
                    model_usage={
                        IMPLEMENTER: _model_usage(1_000_000, 500_000),
                        **reviewer_totals,
                    },
                    uuid="turn-a",
                ),
                IMPLEMENTER,
            )
            reporter.observe(_assistant(IMPLEMENTER, _sdk_usage(500_000, 200_000)))
            await reporter.report(
                _result(
                    model_usage={
                        IMPLEMENTER: _model_usage(1_500_000, 700_000),
                        **reviewer_totals,
                    },
                    uuid="turn-b",
                ),
                IMPLEMENTER,
            )
        finally:
            await server.close()

    anyio.run(drive)
    assert len(captured) == 2

    client, github, sink, _book = priced
    request_id = _admit(client, github, sink, 9630)
    _reconcile()
    epoch = _start_running(request_id)
    for body in captured:
        response = client.post(
            f"/v1/work-item-progress/{request_id}/usage",
            headers={"X-API-Key": _token(request_id)},
            json=body,
        )
        assert response.status_code == 201, response.text
    rows = _stored(request_id)
    by_turn = {(row["turn_id"], row["role"], row["model"]): row for row in rows}
    assert set(by_turn) == {
        ("turn-a", "implementer", IMPLEMENTER),
        ("turn-a", "reviewer", REVIEWER),
        ("turn-b", "implementer", IMPLEMENTER),
    }
    assert by_turn[("turn-b", "implementer", IMPLEMENTER)]["input_tokens"] == 500_000
    assert by_turn[("turn-b", "implementer", IMPLEMENTER)]["output_tokens"] == 200_000
    assert by_turn[("turn-a", "reviewer", REVIEWER)]["output_tokens"] == 100_000
    assert sum(row["output_tokens"] for row in rows) == 800_000

    _finish_failed(client, request_id, epoch, "runner_escalated")
    _reconcile()
    (comment,) = _marked(sink, request_id)
    (usage,) = [line for line in comment["body"].splitlines() if line.startswith("Usage:")]
    assert usage == (
        "Usage: implementer 2,200,000 tokens ($2.90), reviewers 300,000 tokens ($0.40), "
        f"total $3.30 (estimated from {SOURCE} prices as of 2026-09-01)"
    )
