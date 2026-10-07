"""Nomination route and parser (automated remediation task 6).

@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7
@spec AUTOMATED-REMEDIATION-1.

Pins the API half of AUTOMATED-REMEDIATION-6 and all of -7 of
docs/superpowers/specs/2026-10-07-automated-remediation.md through the real
route, real Postgres and the real protected admission broker:

* ``POST /v1/internal/remediation/nominations`` under the internal worker token
  (``X-Curie-Worker-Token``), whose body is exactly ``{"event_id", "block"}``.
  ``block`` is the raw text the protected worker extracted (the fenced block,
  fences included, as the shared vector's ``submitted`` field).
* ``agent_id``, ``hook``, the admitted generation, the thread and the reply
  handle are resolved from the protected binding keyed by ``event_id``, never
  from the request; a request carrying any of them is ``422`` and writes nothing.
* An event with no binding is ``not_protected_event``; with
  ``CURIE_REMEDIATION_ENABLED`` off every submission is ``remediation_disabled``;
  both write no row.
* Idempotent per ``event_id``: the first accepted submission wins, a
  byte-identical replay returns the same answer, and different bytes are
  ``nomination_conflict``.
* One ``remediation_nominations`` row per entry (one row for a malformed block),
  with the target key of AUTOMATED-REMEDIATION-10; the parse refusals
  ``nomination_malformed``, ``unknown_action``, ``nomination_duplicate`` and
  ``arguments_schema_mismatch`` end the row ``refused`` with no approval or
  execution, and every submitted case of ``tests/vectors/remediation-nomination.json``
  is driven through the route.

The binding is the one a delivery creates: every event here is a signed
protected delivery admitted through the real hook route onto the owned
disposable TLS broker (``_protected_ingress_harness``), not a hand-written key.
The deliveries are admitted before the remediation policy is bound, so the
admitted generation is absent (AUTOMATED-REMEDIATION-4: "a delivery admitted
before any policy existed"); a positive admitted generation needs task 5's
envelope field and is proved there.

Refusals follow the policy routes' body, ``{"detail": {"code": ...}}``. Shapes
are recorded in ``.projects/plans/task-remediation-nomination.tests.md``.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from _migration_support import sql_dicts
from _protected_ingress_harness import (
    _broker,
    deliver,
    event_id,
    ingress_app,
    ingress_broker_fixture,  # noqa: F401  (fixture)
    install,
    provision,
    seed_row,
    signed,
)
from curie_api import approval_principal
from curie_api.config import get_settings
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    HOOK,
    scoped_secret,
    support_db,
)

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service
TIMEOUT = 90

ROUTE = "/v1/internal/remediation/nominations"
WORKER_HEADER = "X-Curie-Worker-Token"
PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
REMEDIATION_SETTING = "CURIE_REMEDIATION_ENABLED"
EXECUTOR_SETTING = "CURIE_ACTION_EXECUTOR_ENABLED"
OPERATOR = "U0EXAMPLE1"
CHANNEL = "C0EXAMPLE1"
ROUTE_NAME = "sre-oncall"
CONNECTOR = "k8s"
PARSE_REFUSALS = {
    "nomination_malformed",
    "unknown_action",
    "nomination_duplicate",
    "arguments_schema_mismatch",
}

_VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "remediation-nomination.json"
    ).read_text("utf-8")
)
_SUBMITTED = [case for case in _VECTOR["cases"] if case["submitted"] is not None]

# ---------------------------------------------------------------------------
# The policy (AUTOMATED-REMEDIATION-2) names the vector's action with the
# vector's values, plus a second target so two entries have distinct target
# keys (AUTOMATED-REMEDIATION-10).
# ---------------------------------------------------------------------------

_READ: dict[str, Any] = {
    "connector": "prometheus",
    "tool": "query",
    "arguments": {"query": "sum(rate(http_requests_errors_total[5m]))"},
    "pointer": "/data/result/0/value/1",
}
SCALE_ACTION: dict[str, Any] = {
    "name": "scale-out-api",
    "kind": "remediate",
    "connector": CONNECTOR,
    "tool": "scale_deployment",
    "arguments": {
        "namespace": {"type": "string", "allowed": ["example-ns"]},
        "deployment": {"type": "string", "allowed": ["example-api", "example-worker"]},
        "replicas": {"type": "integer", "minimum": 2, "maximum": 6},
    },
    "target": {"argument": "deployment", "allowed": ["example-api", "example-worker"]},
    "reversibility": "reversible",
    "precondition": {**_READ, "comparator": "gt", "value": 0.5},
    "verifier": {
        **_READ,
        "comparator": "lt",
        "value": 0.05,
        "settle_seconds": 60,
        "deadline_seconds": 600,
        "interval_seconds": 30,
        "consecutive": 2,
    },
    "automatic": False,
    "qualification": None,
}
POLICY: dict[str, Any] = {
    "route": ROUTE_NAME,
    "limits": {
        "per_policy_per_hour": 3,
        "per_incident_per_target": 1,
        "incident_window_seconds": 3600,
        "approval_ttl_seconds": 14400,
    },
    "actions": [SCALE_ACTION],
}
POLICY_ACTIONS = {action["name"] for action in POLICY["actions"]}


def canonical(value: Any) -> str:
    """The executor's canonical argument text, as the shared vector freezes it."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def entry(
    action: str = "scale-out-api",
    *,
    deployment: str = "example-api",
    replicas: Any = 4,
    reason: str | None = "error ratio above threshold",
    **extra: Any,
) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "namespace": "example-ns",
        "deployment": deployment,
        "replicas": replicas,
        **extra,
    }
    item: dict[str, Any] = {"action": action, "arguments": arguments}
    if reason is not None:
        item["reason"] = reason
    return item


def block(*entries: dict[str, Any]) -> str:
    """A fenced block exactly as the worker submits it (AUTOMATED-REMEDIATION-5)."""

    body = json.dumps({"version": 1, "nominations": list(entries)})
    return f"{_VECTOR['opening_fence']}\n{body}\n{_VECTOR['closing_fence']}\n"


# ---------------------------------------------------------------------------
# Real stores: a protected delivery per event, a bound remediation policy.
# ---------------------------------------------------------------------------


def worker_headers() -> dict[str, str]:
    return {WORKER_HEADER: get_settings().internal_worker_token}


def admin_headers() -> dict[str, str]:
    """Platform key plus an ADR 0106 operator principal: a valid policy writer."""

    token = approval_principal.mint(
        get_settings().api_key,
        subject=OPERATOR,
        kind="operator",
        scope=approval_principal.APPROVE_SCOPE,
        exp=int(time.time()) + 300,
    )
    return {"X-API-Key": get_settings().api_key, PRINCIPAL_HEADER: token}


def remediation(monkeypatch: pytest.MonkeyPatch, *, enabled: bool) -> None:
    """``CURIE_REMEDIATION_ENABLED`` (AUTOMATED-REMEDIATION-1), the executor on."""

    monkeypatch.setenv(EXECUTOR_SETTING, "true")
    if enabled:
        monkeypatch.setenv(REMEDIATION_SETTING, "true")
    else:
        monkeypatch.delenv(REMEDIATION_SETTING, raising=False)
    get_settings.cache_clear()


class Stage:
    """One app, one agent with a protected hook, its broker and runtime."""

    def __init__(self, client: httpx.AsyncClient, agent: str, broker: Any) -> None:
        self.client = client
        self.agent = agent
        self.broker = broker
        self.generation: int | None = None
        self._deliveries = 0

    async def protected_event(self) -> str:
        """A signed protected delivery admitted onto the broker; its event id."""

        self._deliveries += 1
        delivery = f"delivery-{self._deliveries}"
        response = await deliver(
            self.client, self.agent, signed(scoped_secret(self.agent), delivery=delivery)
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["acceptance_status"] == "accepted", body
        assert body["event_id"] == event_id(self.agent, delivery)
        assert self.broker.command("EXISTS", "protected:admission:binding:" + body["event_id"])
        return str(body["event_id"])

    async def bind(self, policy: dict[str, Any] | None = None) -> int:
        """Bind the remediation policy through its real route (task 3)."""

        response = await self.client.put(
            f"/agents/{self.agent}/hooks/{HOOK}/remediation-policy",
            json={
                "expected_generation": str(self.generation or 0),
                "operation_id": str(uuid.uuid4()),
                "policy": copy.deepcopy(POLICY if policy is None else policy),
            },
            headers=admin_headers(),
        )
        assert response.status_code == 200, response.text
        self.generation = int(response.json()["generation"])
        return self.generation

    async def submit(
        self, event: str, text: str, *, headers: dict[str, str] | None = None, **extra: Any
    ) -> httpx.Response:
        return await self.client.post(
            ROUTE,
            json={"event_id": event, "block": text, **extra},
            headers=worker_headers() if headers is None else headers,
        )


@asynccontextmanager
async def staged(
    broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Stage]:
    """Real app, a protected hook source with its runtime, an explicit approval route."""

    async with ingress_app(tmp_path, monkeypatch) as (_app, client, agent, directory):
        rt = provision(broker, agent, directory)
        await asyncio.to_thread(seed_row, agent, rt)
        install(broker, rt)
        patched = await client.patch(
            f"/agents/{agent}",
            json={
                "approval_routes": {
                    ROUTE_NAME: {
                        "resolution": {"kind": "slack", "address": CHANNEL},
                        "approvers": {"users": [OPERATOR]},
                    }
                }
            },
            headers={"X-API-Key": get_settings().api_key},
        )
        assert patched.status_code == 200, patched.text
        yield Stage(client, agent, broker)


def run(scenario: Any, timeout: float = TIMEOUT) -> None:
    asyncio.run(asyncio.wait_for(scenario(), timeout))


def _rows(event: str | None = None) -> list[dict[str, Any]]:
    """The ``remediation_nominations`` rows (AUTOMATED-REMEDIATION-7), oldest first."""

    where, params = ("WHERE event_id = :event_id", {"event_id": event}) if event else ("", {})
    return sql_dicts(
        "SELECT id, agent_id, hook, event_id, admitted_generation, current_generation, "
        "action, kind, arguments, arguments_sha256, target, reason, state, refusal_code, "
        "approval_id, execution_id, verification_outcome, created_at, decided_at "
        f"FROM curie.remediation_nominations {where} ORDER BY created_at, id",
        params,
    )


def _side_effect_counts() -> dict[str, int]:
    """Approvals and executions a refused nomination must not create."""

    counts = sql_dicts(
        "SELECT (SELECT count(*) FROM curie.approvals) AS approvals, "
        "(SELECT count(*) FROM curie.action_executions) AS executions"
    )[0]
    return {key: int(value) for key, value in counts.items()}


async def rows(event: str | None = None) -> list[dict[str, Any]]:
    """``_rows`` off the event loop (``sql_dicts`` runs its own loop)."""

    return await asyncio.to_thread(_rows, event)


async def side_effect_counts() -> dict[str, int]:
    return await asyncio.to_thread(_side_effect_counts)


def arguments_text(value: Any) -> str | None:
    """The stored canonical arguments, whether kept as text or as JSON."""

    if value is None:
        return None
    return value if isinstance(value, str) else canonical(value)


def target_text(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def code(response: httpx.Response) -> str | None:
    body = response.json()
    detail = body.get("detail") if isinstance(body, dict) else None
    assert isinstance(detail, dict), f"refusal carries no coded detail: {response.text}"
    return detail.get("code")


def assert_refused_row(row: dict[str, Any], refusal: str) -> None:
    assert row["state"] == "refused", row
    assert row["refusal_code"] == refusal, row
    assert row["approval_id"] is None and row["execution_id"] is None, row
    assert row["verification_outcome"] is None, row


# ---------------------------------------------------------------------------
# Rows resolved from the binding, one per entry, with the target key
# ---------------------------------------------------------------------------


def test_a_block_writes_one_row_per_entry_resolved_from_the_binding(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-1

    With remediation on, a block of two well-formed entries for a protected
    event writes two rows whose agent, hook and event come from the binding the
    delivery created, whose admitted generation is absent (delivered before
    the policy) and whose current generation is the bound one; arguments are
    canonical with their digest, the reason is stored, and each row's target
    key names the connector and the literal target value, distinct per target.
    No parse refusal, approval or execution.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            generation = await stage.bind()
            before = await side_effect_counts()
            first = entry(deployment="example-api", replicas=4)
            second = entry(deployment="example-worker", replicas=3, reason=None)

            response = await stage.submit(event, block(first, second))

            assert response.status_code == 200, response.text
            assert response.json()["event_id"] == event
            found = await rows(event)
            assert len(found) == 2, found
            for row, item in zip(found, (first, second), strict=True):
                assert str(row["agent_id"]) == stage.agent
                assert row["hook"] == HOOK
                assert row["event_id"] == event
                assert row["admitted_generation"] is None
                assert int(row["current_generation"]) == generation
                assert row["action"] == "scale-out-api"
                assert row["kind"] == "remediate"
                text = canonical(item["arguments"])
                assert arguments_text(row["arguments"]) == text
                assert row["arguments_sha256"] == hashlib.sha256(text.encode()).hexdigest()
                assert row["reason"] == item.get("reason")
                assert row["state"] != "refused" and row["refusal_code"] is None, row
                assert row["approval_id"] is None and row["execution_id"] is None
                assert row["created_at"] is not None
                target = target_text(row["target"])
                assert CONNECTOR in target, target
                assert canonical(item["arguments"]["deployment"]) in target, target
            assert target_text(found[0]["target"]) != target_text(found[1]["target"])
            assert await side_effect_counts() == before

    run(scenario)


# ---------------------------------------------------------------------------
# Idempotent per event_id
# ---------------------------------------------------------------------------


def test_a_byte_identical_replay_returns_the_first_answer_and_adds_nothing(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-6: the first accepted submission wins."""

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()
            text = block(entry())

            first = await stage.submit(event, text)
            assert first.status_code == 200, first.text
            written = await rows(event)
            assert len(written) == 1

            replay = await stage.submit(event, text)

            assert replay.status_code == first.status_code, replay.text
            assert replay.json() == first.json()
            assert await rows(event) == written

    run(scenario)


@pytest.mark.parametrize(
    "second",
    [
        pytest.param(lambda: block(entry(replicas=5)), id="other_arguments"),
        pytest.param(lambda: block(entry(), entry(deployment="example-worker")), id="more"),
        pytest.param(lambda: block(entry()).rstrip("\n"), id="same_entries_other_bytes"),
        pytest.param(lambda: "```curie-remediation\nnot json\n```\n", id="malformed"),
    ],
)
def test_different_bytes_for_the_same_event_are_refused_nomination_conflict(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, second: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-6: a retried turn cannot add nominations.

    Any second submission whose bytes differ, even by a trailing newline, is
    refused ``nomination_conflict`` and leaves the first rows as they were.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()
            first = await stage.submit(event, block(entry()))
            assert first.status_code == 200, first.text
            written = await rows(event)

            response = await stage.submit(event, second())

            assert response.status_code == 409, response.text
            assert code(response) == "nomination_conflict"
            assert await rows(event) == written

    run(scenario)


def test_a_malformed_first_submission_also_wins(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7

    A malformed block is still the event's accepted submission: a later
    well-formed one is ``nomination_conflict`` and adds no row.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()
            first = await stage.submit(event, "```curie-remediation\n{\n```\n")
            assert first.status_code == 200, first.text
            written = await rows(event)
            assert len(written) == 1
            assert_refused_row(written[0], "nomination_malformed")

            response = await stage.submit(event, block(entry()))

            assert response.status_code == 409, response.text
            assert code(response) == "nomination_conflict"
            assert await rows(event) == written

    run(scenario)


# ---------------------------------------------------------------------------
# Submission refusals: not_protected_event, remediation_disabled, auth, shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("which", ["never_delivered", "binding_removed"])
def test_an_event_without_a_binding_is_refused_not_protected_event(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, which: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-6

    The API resolves the event through its protected binding: an event id of
    the protected shape that was never delivered, and a delivered event whose
    binding key is gone, are both ``not_protected_event`` with no row.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            if which == "never_delivered":
                await stage.protected_event()
                event = event_id(stage.agent, "delivery-never-sent")
            else:
                event = await stage.protected_event()
                ingress_broker.command("DEL", "protected:admission:binding:" + event)
            await stage.bind()

            response = await stage.submit(event, block(entry()))

            assert response.status_code == 404, response.text
            assert code(response) == "not_protected_event"
            assert await rows() == []

    run(scenario)


def test_remediation_off_refuses_remediation_disabled_and_writes_nothing(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-1 @spec AUTOMATED-REMEDIATION-8 (check 1)

    With the setting off, a well-formed submission for a protected event under
    a bound policy is ``remediation_disabled`` and creates no nomination,
    approval or execution.
    """

    remediation(monkeypatch, enabled=False)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()
            before = await side_effect_counts()

            response = await stage.submit(event, block(entry()))

            assert response.status_code == 409, response.text
            assert code(response) == "remediation_disabled"
            assert await rows() == []
            assert await side_effect_counts() == before

    run(scenario)


@pytest.mark.parametrize(
    "field,value",
    [
        ("agent_id", "00000000-0000-4000-8000-000000000001"),
        ("hook", "other-hook"),
        ("admitted_generation", "1"),
        ("current_generation", "1"),
        ("generation", "1"),
        ("conversation_id", "example-conversation"),
        ("reply_handle", {"kind": "slack", "address": CHANNEL}),
    ],
)
def test_a_request_carrying_a_resolved_field_is_refused_and_does_not_claim_the_event(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
) -> None:
    """@spec AUTOMATED-REMEDIATION-6

    The route accepts only ``event_id`` and the block. A request also carrying
    the agent, hook, a generation, the thread or a reply handle is ``422`` and
    writes nothing; the same event's clean submission then succeeds with the
    binding's agent and hook, not the refused request's.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()

            refused = await stage.submit(event, block(entry()), **{field: value})

            assert refused.status_code == 422, refused.text
            assert await rows() == []
            accepted = await stage.submit(event, block(entry()))
            assert accepted.status_code == 200, accepted.text
            found = await rows(event)
            assert len(found) == 1
            assert str(found[0]["agent_id"]) == stage.agent
            assert found[0]["hook"] == HOOK

    run(scenario)


@pytest.mark.parametrize("credential", ["none", "platform_key", "wrong_token"])
def test_the_route_requires_the_internal_worker_token(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, credential: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-6: the internal worker token, never the platform key."""

    remediation(monkeypatch, enabled=True)
    headers = {
        "none": {},
        "platform_key": {"X-API-Key": get_settings().api_key},
        "wrong_token": {WORKER_HEADER: "example-not-the-worker-token"},
    }[credential]

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()

            response = await stage.submit(event, block(entry()), headers=headers)

            assert response.status_code == 401, response.text
            assert await rows() == []

    run(scenario)


# ---------------------------------------------------------------------------
# Parse and validation refusals (AUTOMATED-REMEDIATION-7)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "item,refusal",
    [
        pytest.param(entry("drain-node"), "unknown_action", id="unknown_action"),
        pytest.param(entry(force=True), "arguments_schema_mismatch", id="key_outside_the_schema"),
        pytest.param(entry(replicas="4"), "arguments_schema_mismatch", id="string_for_integer"),
        pytest.param(entry(replicas=4.5), "arguments_schema_mismatch", id="float_for_integer"),
        pytest.param(entry(replicas=True), "arguments_schema_mismatch", id="bool_for_integer"),
        pytest.param(
            entry(deployment=["example-api"]), "arguments_schema_mismatch", id="list_for_string"
        ),
    ],
)
def test_each_validation_refusal_writes_a_refused_row_with_no_approval_or_execution(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    item: dict[str, Any],
    refusal: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-7

    An entry naming no policy action, or whose arguments carry a key outside
    the action's schema or a value of the wrong type, ends ``refused`` with its
    code, keeps its action and canonical arguments, and asks nobody.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()
            before = await side_effect_counts()

            response = await stage.submit(event, block(item))

            assert response.status_code == 200, response.text
            found = await rows(event)
            assert len(found) == 1, found
            assert_refused_row(found[0], refusal)
            assert found[0]["action"] == item["action"]
            assert arguments_text(found[0]["arguments"]) == canonical(item["arguments"])
            assert await side_effect_counts() == before

    run(scenario)


@pytest.mark.parametrize(
    "item",
    [
        pytest.param(entry(replicas=9), id="above_the_range"),
        pytest.param(entry(replicas=1), id="below_the_range"),
        pytest.param(entry(deployment="example-other"), id="target_outside_the_list"),
    ],
)
def test_a_right_typed_value_out_of_bounds_is_not_a_parse_refusal(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, item: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-7

    A value of the right type outside the allowed range or target list is not
    refused: it is left for admission (an approval request, tasks 9 and 10)
    with exactly its arguments.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()

            response = await stage.submit(event, block(item))

            assert response.status_code == 200, response.text
            found = await rows(event)
            assert len(found) == 1, found
            assert found[0]["state"] != "refused", found[0]
            assert found[0]["refusal_code"] not in PARSE_REFUSALS
            assert arguments_text(found[0]["arguments"]) == canonical(item["arguments"])

    run(scenario)


def test_one_block_mixing_entries_writes_one_row_per_entry_with_its_own_outcome(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7

    A refusal ends only its own entry: a well-formed entry, its duplicate, an
    unknown action and a schema mismatch in one block give four rows.
    """

    remediation(monkeypatch, enabled=True)

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            event = await stage.protected_event()
            await stage.bind()
            items = [
                entry(),
                entry(reason="same call again"),
                entry("drain-node"),
                entry(force=True),
            ]

            response = await stage.submit(event, block(*items))

            assert response.status_code == 200, response.text
            found = await rows(event)
            outcomes = sorted(
                (row["action"], arguments_text(row["arguments"]), row["refusal_code"])
                for row in found
            )
            expected = sorted(
                [
                    ("scale-out-api", canonical(items[0]["arguments"]), None),
                    ("scale-out-api", canonical(items[1]["arguments"]), "nomination_duplicate"),
                    ("drain-node", canonical(items[2]["arguments"]), "unknown_action"),
                    (
                        "scale-out-api",
                        canonical(items[3]["arguments"]),
                        "arguments_schema_mismatch",
                    ),
                ],
                key=lambda t: (t[0], t[1], t[2] or ""),
            )
            assert sorted(outcomes, key=lambda t: (t[0], t[1], t[2] or "")) == expected
            for row in found:
                if row["refusal_code"] is not None:
                    assert_refused_row(row, row["refusal_code"])

    run(scenario)


def test_every_submitted_vector_case_is_decided_through_the_route(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-5 @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-26

    Each submitted case of the shared nomination vector, sent as its own
    protected event: an invalid block writes exactly one ``refused``
    ``nomination_malformed`` row; a valid block writes one row per frozen entry
    with the frozen action, canonical arguments, digest and reason, refused
    ``nomination_duplicate`` where the vector says so, ``unknown_action`` for an
    action the policy does not declare, and no parse refusal otherwise.
    """

    remediation(monkeypatch, enabled=True)
    assert len(_SUBMITTED) < 64, "more cases than the fixture broker's admission quota"

    async def scenario() -> None:
        async with staged(ingress_broker, tmp_path, monkeypatch) as stage:
            events = [await stage.protected_event() for _ in _SUBMITTED]
            await stage.bind()
            mismatches: list[str] = []
            for case, event in zip(_SUBMITTED, events, strict=True):
                response = await stage.submit(event, case["submitted"])
                if response.status_code != 200:
                    mismatches.append(f"{case['name']}: {response.status_code} {response.text}")
                    continue
                found = await rows(event)
                expected = case["parse"]
                if expected["result"] == "malformed":
                    if len(found) != 1 or (
                        found[0]["state"],
                        found[0]["refusal_code"],
                    ) != ("refused", _VECTOR["malformed_code"]):
                        mismatches.append(f"{case['name']}: {found}")
                    continue
                want = [
                    (
                        item["action"],
                        item["arguments"],
                        item["arguments_sha256"],
                        item["reason"],
                        item["refusal"]
                        or (None if item["action"] in POLICY_ACTIONS else "unknown_action"),
                    )
                    for item in expected["entries"]
                ]
                got = [
                    (
                        row["action"],
                        arguments_text(row["arguments"]),
                        row["arguments_sha256"],
                        row["reason"],
                        row["refusal_code"],
                    )
                    for row in found
                ]
                if sorted(got, key=repr) != sorted(want, key=repr):
                    mismatches.append(f"{case['name']}: got {got!r:.400} want {want!r:.400}")
            assert not mismatches, "\n".join(mismatches)

    run(scenario, timeout=TIMEOUT * 2)
