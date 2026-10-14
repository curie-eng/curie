"""Tuning and prevention kinds (automated remediation plan task 16).

@spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-25

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the maintainer
ruling of 2026-10-07: tuning stops at the nomination and the approval card; an
approved tuning request ends refused ``tune_execution_not_automated`` with no
write call; automated rule-owner change requests need a later Draft ADR.

Every row is driven through its real producer, as in
``test_remediation_admission.py`` (whose rig this reuses): signed protected
deliveries onto the owned broker, the real nomination route, the real policy
routes, the executor's claim and sample routes, the real approval resolve route,
and the worker's remediation card loop with the real Slack renderer and a fake
Slack client. Postgres and Valkey are real.

Surface these tests fix (``.projects/plans/task-remediation-tuning.tests.md``):

* AR-24: a ``prevent`` nomination asks (``approval_requested``,
  ``not_automatic``) and creates no execution; approved, it executes once under
  the approval authority and is verified. ``automatic`` true on ``prevent`` and
  on ``tune`` is refused ``kind_not_automatic`` at the policy route and creates
  no generation.
* AR-25 tune action shape (closed): ``name``, ``kind`` ``tune``, ``connector``
  and ``tool`` (the rule owner's write), ``rules`` (an object keyed by the
  closed set of rule identifiers; each rule declares ``current``, a map from a
  change field to the declared read of that field's current value, and
  ``evidence``, a map from an evidence name to a declared read; a declared read
  here is ``connector``, ``tool``, ``arguments``, ``pointer``, no comparator),
  ``change`` (a closed map from ``threshold``, ``for_duration``, ``group_by``,
  ``dedupe``, ``retire`` to that field's allowed values or range; ``retire``
  declares ``duplicate_of``, rule identifiers from ``rules``), ``automatic``,
  ``qualification``. No ``arguments``, ``target``, ``reversibility``,
  ``precondition`` or ``verifier``.
* A tune nomination is ``{"action", "rule", "field", "value", "reason"?}``; for
  ``retire`` the value is ``{"duplicate_of": <rule>}``. A rule outside
  ``rules``, a field outside ``change``, or a ``duplicate_of`` outside the
  declared set is refused ``arguments_schema_mismatch`` at the route.
* A tune nomination asks (``not_automatic``); the approval binds
  ``mcp__<rule owner connector>__<tool>`` and the canonical change. Its card is
  posted only once the nominated rule's declared reads (``read`` executions
  under ``remediation:<nomination id>:``, claimed and sampled as any read) have
  ended, and shows the platform-rendered diff (the current value read, then the
  proposed value) and every evidence name with its read value; the model's
  ``reason`` (and any figure in it) appears only as unverified model text.
* A recorded series of deliveries nominating the same change yields exactly one
  approval and one card; every nomination of it points at that approval.
* Approving it answers ``409`` ``{"code": "tune_execution_not_automated"}``,
  records the approval ``approved`` and ends every nomination of the request
  ``refused`` with ``refusal_code`` ``tune_execution_not_automated``: no forward
  execution, no ledger row, nothing claimable but reads, and a reconciliation
  pass creates nothing. Rejecting it ends the nominations ``rejected`` and
  creates nothing.

Every identifier is a placeholder.
"""

from __future__ import annotations

import copy
import importlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

import pytest
from _protected_ingress_harness import (
    _broker,
    ingress_broker_fixture,  # noqa: F401  (fixture)
)
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_hook_source_support import (  # noqa: F401  (support_db is a fixture)
    HOOK,
    support_db,
)
from test_remediation_admission import (
    ACT_CONNECTOR,
    ACT_IMAGE,
    LIMITS,
    READ_CONNECTOR,
    READ_IMAGE,
    TARGETS,
    World,
    _approver,
    all_forwards,
    approved_forward,
    assert_approval,
    assert_nothing_executes,
    assert_requested,
    claim,
    entry,
    executions_of,
    ledger_rows,
    nomination,
    policy,
    q,
    report,
    run,
    run_forward,
    world,
)
from test_remediation_nomination_routes import CHANNEL, ROUTE_NAME, admin_headers

pytestmark = pytest.mark.usefixtures("support_db")
admission_service = _broker.admission_service

# --------------------------------------------------------------------------- #
# The tune action (AUTOMATED-REMEDIATION-25)
# --------------------------------------------------------------------------- #

TUNE_ACTION_NAME = "tune-alert-rule"
RULES_CONNECTOR = "example-rules"
RULES_DIGEST = "sha256:" + "a1" * 32
RULES_IMAGE = f"ghcr.io/example/rules-mcp@{RULES_DIGEST}"
RULES_WRITE_TOOL = "update_rule"
RULES_READ_TOOL = "get_rule"
EVIDENCE_TOOL = "query_value"
# Two alert rules firing for one underlying condition (issue #4144's series).
RULE_SLOW = "example-claim-slow"
RULE_OVER_N = "example-claim-over-n"
UNDECLARED_RULE = "example-disk-full"

EVIDENCE_NAMES = (
    "fire_count",
    "duration_p50_seconds",
    "resolved_without_action_ratio",
    "overlap_ratio",
)
# What each declared read observes, by its (unique) pointer. Values are chosen
# so none is a substring of another or of any other text on the card.
CURRENT_THRESHOLD = 37.5
PROPOSED_THRESHOLD = 90
OBSERVED: dict[str, dict[str, Any]] = {
    RULE_SLOW: {
        "threshold": CURRENT_THRESHOLD,
        "fire_count": 61,
        "duration_p50_seconds": 212,
        "resolved_without_action_ratio": 0.927,
        "overlap_ratio": 0.881,
    },
    RULE_OVER_N: {
        "threshold": 44.5,
        "fire_count": 58,
        "duration_p50_seconds": 198,
        "resolved_without_action_ratio": 0.911,
        "overlap_ratio": 0.874,
    },
}
# The model's own figure and its own "diff": shown only as unverified text.
MODEL_FIGURE = "999"
MODEL_REASON = f"fires and resolves within minutes, fired {MODEL_FIGURE} times; diff: 1 -> 2"


def _current_pointer(rule: str, field: str) -> str:
    return f"/rules/{rule}/{field}"


def _evidence_pointer(rule: str, name: str) -> str:
    return f"/evidence/{rule}/{name}"


def _rule_reads(rule: str) -> dict[str, Any]:
    return {
        "current": {
            "threshold": {
                "connector": RULES_CONNECTOR,
                "tool": RULES_READ_TOOL,
                "arguments": {"rule": rule},
                "pointer": _current_pointer(rule, "threshold"),
            }
        },
        "evidence": {
            name: {
                "connector": READ_CONNECTOR,
                "tool": EVIDENCE_TOOL,
                "arguments": {"query": f'example_alert_{name}{{rule="{rule}"}}'},
                "pointer": _evidence_pointer(rule, name),
            }
            for name in EVIDENCE_NAMES
        },
    }


TUNE_ACTION: dict[str, Any] = {
    "name": TUNE_ACTION_NAME,
    "kind": "tune",
    "connector": RULES_CONNECTOR,
    "tool": RULES_WRITE_TOOL,
    "rules": {RULE_SLOW: _rule_reads(RULE_SLOW), RULE_OVER_N: _rule_reads(RULE_OVER_N)},
    "change": {
        "threshold": {"type": "number", "minimum": 5, "maximum": 300},
        "for_duration": {"type": "integer", "minimum": 60, "maximum": 1800},
        "retire": {"duplicate_of": [RULE_SLOW]},
    },
    "automatic": False,
    "qualification": None,
}
READ_VALUES: dict[str, Any] = {
    (_current_pointer(rule, key) if key == "threshold" else _evidence_pointer(rule, key)): value
    for rule, values in OBSERVED.items()
    for key, value in values.items()
}


def tune_policy(**action: Any) -> dict[str, Any]:
    declared = copy.deepcopy(TUNE_ACTION)
    declared.update(action)
    return {"route": ROUTE_NAME, "limits": dict(LIMITS), "actions": [declared]}


def tune_entry(
    rule: str = RULE_SLOW,
    field: str = "threshold",
    value: Any = PROPOSED_THRESHOLD,
    reason: str | None = MODEL_REASON,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "action": TUNE_ACTION_NAME,
        "rule": rule,
        "field": field,
        "value": value,
    }
    if reason is not None:
        item["reason"] = reason
    return item


def retire_entry() -> dict[str, Any]:
    """Retire the rule that duplicates another, as the model would nominate it."""

    return tune_entry(
        RULE_OVER_N,
        "retire",
        {"duplicate_of": RULE_SLOW},
        reason=f"same condition as the other claim alert, fired {MODEL_FIGURE} times",
    )


def connectors_yaml() -> str:
    """The acting ``k8s``, the evidence reader, and the rule owner."""

    return (
        "connectors:\n"
        f"  {ACT_CONNECTOR}:\n    image: {ACT_IMAGE}\n"
        "    secrets:\n      - name: SNAPSHOT_SEALING_KEY\n"
        "        from_secret: k8s-restorer-seal\n"
        f"  {READ_CONNECTOR}:\n    image: {READ_IMAGE}\n"
        "    secrets:\n      - METRICS_TOKEN\n"
        f"  {RULES_CONNECTOR}:\n    image: {RULES_IMAGE}\n"
        "    secrets:\n      - RULES_TOKEN\n"
    )


# A recorded alert series: two rules for one condition, FIRING and RESOLVED,
# replayed as protected deliveries. Each turn nominates the same retirement.
SERIES: list[bytes] = [
    json.dumps({"status": status, "alertname": rule, "fingerprint": f"fp-{rule}"}).encode()
    for status, rule in (
        ("firing", RULE_SLOW),
        ("firing", RULE_OVER_N),
        ("resolved", RULE_SLOW),
        ("resolved", RULE_OVER_N),
        ("firing", RULE_SLOW),
        ("firing", RULE_OVER_N),
    )
]


# --------------------------------------------------------------------------- #
# Rows and the worker
# --------------------------------------------------------------------------- #


async def generations() -> int:
    return int((await q("SELECT count(*) AS n FROM curie.remediation_policy_generations"))[0]["n"])


async def put_policy(w: World, document: dict[str, Any]) -> Any:
    return await w.stage.client.put(
        f"/agents/{w.stage.agent}/hooks/{HOOK}/remediation-policy",
        json={
            "expected_generation": str(w.hooks.generation.get(HOOK, 0)),
            "operation_id": str(uuid.uuid4()),
            "policy": copy.deepcopy(document),
        },
        headers=admin_headers(),
    )


def detail_code(response: Any) -> str | None:
    try:
        detail = response.json().get("detail")
    except ValueError:
        return None
    return detail.get("code") if isinstance(detail, dict) else None


async def drain_reads(w: World) -> list[dict[str, Any]]:
    """Claim every due execution as the worker would and answer each declared read.

    Only reads may ever be claimable for a tune nomination: a claimed execution
    of any other kind, or a read whose pointer no declared read names, fails.
    """

    taken: list[dict[str, Any]] = []
    for _ in range(200):
        claimed = await claim(w.stage)
        if claimed.status_code == 204:
            return taken
        assert claimed.status_code == 200, claimed.text
        body = dict(claimed.json())
        (row,) = await q(
            "SELECT * FROM curie.action_executions WHERE id = :id", {"id": uuid.UUID(body["id"])}
        )
        assert row["kind"] == "read", f"only reads are claimable for a tune: {row}"
        assert row["tool"] != RULES_WRITE_TOOL, row
        assert row["pointer"] in READ_VALUES, f"an undeclared read was scheduled: {row}"
        reported = await report(w.stage, body, READ_VALUES[row["pointer"]])
        assert reported.status_code == 200, reported.text
        taken.append(row)
    raise AssertionError("reads never stopped being claimable")


class _CardMemory:
    def __init__(self) -> None:
        self.remembered: list[tuple[str, dict[str, Any]]] = []

    async def remember(self, approval_id: str, **fields: Any) -> None:
        self.remembered.append((approval_id, fields))


_NOT_TEXT = frozenset({"value", "action_id", "block_id", "url"})


def _strings(value: Any) -> list[str]:
    """Every displayed string of a Slack post (button values and ids excluded)."""

    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for k, item in value.items() if k not in _NOT_TEXT for s in _strings(item)]
    if isinstance(value, list):
        return [s for item in value for s in _strings(item)]
    return []


async def deliver_cards(times: int = 1) -> list[dict[str, Any]]:
    """The worker's remediation card loop over this database, real Slack renderer,
    fake Slack client; returns what was posted."""

    cards = importlib.import_module("curie_worker.remediation_cards")
    from curie_worker.slack_sink import SlackReplyAdapter

    posts: list[dict[str, Any]] = []

    async def chat_post_message(**kwargs: Any) -> dict[str, Any]:
        posts.append(kwargs)
        return {"ok": True, "channel": kwargs["channel"], "ts": f"1700000000.{len(posts):06d}"}

    sink = SlackReplyAdapter("xoxb-test")
    sink._client_for(None).chat_postMessage = chat_post_message  # type: ignore[method-assign]
    engine = create_async_engine(get_settings().database_url)
    try:
        loop = cards.RemediationCardLoop(
            store=cards.PostgresRemediationCardStore(
                engine, schema="curie", lease_owner="worker-a"
            ),
            replies=sink,
            card_store=_CardMemory(),
        )
        for _ in range(times):
            await loop.deliver_pending_card()
    finally:
        await engine.dispose()
    return posts


def card_text(post: dict[str, Any]) -> str:
    return "\n".join(_strings(post.get("blocks")) + [post.get("text") or ""])


_FENCED = re.compile(r"```.*?```", re.DOTALL)


def outside_model_text(text: str) -> str:
    """The card with the fenced (unverified model text) blocks removed."""

    return _FENCED.sub("", text)


def model_blocks(text: str) -> str:
    return "\n".join(_FENCED.findall(text))


async def tune_approvals() -> list[dict[str, Any]]:
    return await q(
        "SELECT * FROM curie.approvals WHERE purpose = 'remediation' ORDER BY created_at, id"
    )


async def resolve(w: World, approval_id: Any, decision: str) -> Any:
    return await w.stage.client.post(
        f"/approvals/{approval_id}/resolve", json={"decision": decision}, headers=_approver()
    )


async def tune_world_bound(w: World) -> None:
    await w.bind(tune_policy())


def tune_world(ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    return world(ingress_broker, tmp_path, monkeypatch, connectors=connectors_yaml())


async def asked_tune(w: World, item: dict[str, Any] | None = None) -> dict[str, Any]:
    """One tune nomination, its reads answered; the row after."""

    (row,) = await w.nominate(item or tune_entry())
    await drain_reads(w)
    return await nomination(row["id"])


# =========================================================================== #
# AUTOMATED-REMEDIATION-24: prevent always asks, and is verified when approved
# =========================================================================== #


def test_a_prevent_nomination_passing_every_bound_asks_and_never_executes(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-24: "its nominations always become approval
    requests": an in-bounds ``prevent`` nomination under an armed, current
    policy becomes an approval request, never a read or an execution. Check 5
    (``not_automatic``) decides before check 6, so no qualification record bears
    on it, and a ``prevent`` action can never be written automatic (below).
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(kind="prevent", automatic=False))
            row = await w.one(entry(TARGETS[0]))
            assert row["kind"] == "prevent", row
            assert_approval(row, "not_automatic")
            await assert_requested(row)
            await assert_nothing_executes(row)
            assert await all_forwards() == []

    run(scenario)


def test_an_approved_prevent_nomination_executes_once_and_is_verified(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-16
    @spec AUTOMATED-REMEDIATION-18: approved, a ``prevent`` nomination yields one
    approval-authority forward execution, executed and verified like any action.
    """

    async def scenario() -> None:
        async with world(ingress_broker, tmp_path, monkeypatch) as w:
            await w.bind(policy(kind="prevent", automatic=False))
            asked = await w.one(entry(TARGETS[0]))
            assert_approval(asked, "not_automatic")
            forward = await approved_forward(w.stage, asked)
            await run_forward(w.stage, asked, forward, "verified")

            done = await nomination(asked["id"])
            assert done["verification_outcome"] == "verified", done
            assert done["state"] == "finished", done
            assert len(await executions_of(asked["id"], "forward")) == 1
            assert await ledger_rows() == 1

    run(scenario)


@pytest.mark.parametrize("kind", ["prevent", "tune"])
def test_automatic_true_on_prevent_or_tune_is_refused_kind_not_automatic(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-24: "A policy write with ``automatic`` true on a
    ``prevent`` or ``tune`` action is refused ``kind_not_automatic``", through the
    real route, creating no generation. The tune action is in its
    AUTOMATED-REMEDIATION-25 shape, so the refusal is the kind's and not a shape
    refusal.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            document = (
                policy(kind="prevent", automatic=True)
                if kind == "prevent"
                else tune_policy(automatic=True)
            )
            before = await generations()
            refused = await put_policy(w, document)
            assert refused.status_code == 422, refused.text
            assert detail_code(refused) == "kind_not_automatic", refused.text
            assert await generations() == before

    run(scenario)


# =========================================================================== #
# AUTOMATED-REMEDIATION-25: the tune action and its nominations
# =========================================================================== #


def test_a_tune_action_in_its_declared_shape_binds(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: a ``tune`` action declares the rule owner's
    connector and tool, the closed set of rules with their declared reads, and a
    closed change schema; written with ``automatic`` false it binds, and reads
    back as written.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            written = await put_policy(w, tune_policy())
            assert written.status_code == 200, written.text
            read = await w.stage.client.get(
                f"/agents/{w.stage.agent}/hooks/{HOOK}/remediation-policy",
                headers={"X-API-Key": get_settings().api_key},
            )
            assert read.status_code == 200, read.text
            assert read.json()["policy"]["actions"] == [TUNE_ACTION], read.text

    run(scenario)


TUNE_SHAPE_REFUSALS = [
    (
        "change-field-outside-the-five",
        {"change": {**TUNE_ACTION["change"], "severity": {"type": "string", "allowed": ["x"]}}},
        "policy_unknown_key",
    ),
    (
        "retire-duplicate-of-outside-the-rules",
        {"change": {**TUNE_ACTION["change"], "retire": {"duplicate_of": [UNDECLARED_RULE]}}},
        "policy_document_invalid",
    ),
]


@pytest.mark.parametrize(
    "override,code",
    [case[1:] for case in TUNE_SHAPE_REFUSALS],
    ids=[case[0] for case in TUNE_SHAPE_REFUSALS],
)
def test_a_tune_change_schema_outside_the_closed_set_is_refused(
    ingress_broker: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    override: dict[str, Any],
    code: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: ``field`` is one of ``threshold``,
    ``for_duration``, ``group_by``, ``dedupe``, ``retire``, and ``retire``'s
    ``duplicate_of`` names a rule "from the same set"; anything else is refused
    and creates no generation.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            refused = await put_policy(w, tune_policy(**override))
            assert refused.status_code == 422, refused.text
            assert detail_code(refused) == code, refused.text
            assert await generations() == 0

    run(scenario)


UNDECLARED = [
    ("undeclared-rule", tune_entry(UNDECLARED_RULE)),
    ("undeclared-field", tune_entry(field="severity", value="page")),
    (
        "retire-duplicate-of-undeclared",
        tune_entry(RULE_OVER_N, "retire", {"duplicate_of": UNDECLARED_RULE}),
    ),
]


@pytest.mark.parametrize(
    "item", [case[1] for case in UNDECLARED], ids=[case[0] for case in UNDECLARED]
)
def test_a_tune_nomination_naming_an_undeclared_rule_or_field_is_refused(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, item: dict[str, Any]
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: "a nomination naming an undeclared rule or
    field is refused ``arguments_schema_mismatch``": a refused row, no approval,
    no read and no execution.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            await tune_world_bound(w)
            (row,) = await w.nominate(item)
            assert row["state"] == "refused", row
            assert row["refusal_code"] == "arguments_schema_mismatch", row
            assert row["approval_id"] is None, row
            await assert_nothing_executes(row)
            assert await tune_approvals() == []

    run(scenario)


def test_a_tune_nomination_asks_with_the_change_bound_and_reads_only_what_it_declares(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25 @spec AUTOMATED-REMEDIATION-24: a well-formed
    tune nomination is never automatic; it asks, bound to the rule owner's call
    and the canonical change, and the only executions it creates are the
    nominated rule's declared reads (no other rule's, nothing of another kind).
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            await tune_world_bound(w)
            (row,) = await w.nominate(tune_entry())
            assert row["kind"] == "tune", row
            reads = await drain_reads(w)
            row = await nomination(row["id"])

            assert_approval(row, "not_automatic")
            (approval,) = await tune_approvals()
            assert approval["id"] == row["approval_id"]
            assert approval["granted_tool"] == f"mcp__{RULES_CONNECTOR}__{RULES_WRITE_TOOL}"
            assert approval["granted_arguments"] == {
                "field": "threshold",
                "rule": RULE_SLOW,
                "value": PROPOSED_THRESHOLD,
            }
            declared = {
                _current_pointer(RULE_SLOW, "threshold"),
                *(_evidence_pointer(RULE_SLOW, name) for name in EVIDENCE_NAMES),
            }
            assert {r["pointer"] for r in reads} == declared, reads
            assert all(r["idempotency_key"].startswith(f"remediation:{row['id']}:") for r in reads)
            assert {r["kind"] for r in await executions_of(row["id"])} == {"read"}

    run(scenario)


def test_the_tune_card_shows_the_platform_rendered_diff_and_the_declared_read_evidence(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: "The platform renders the diff from the
    structured change and the rule's current definition read through the
    declared read; the model never supplies diff text. The evidence on the card
    ... comes only from reads the action declares; any model-supplied figure is
    shown as unverified model text."

    No card is posted while the declared reads are outstanding. The posted card
    names the rule and field with the read current value before the proposed
    one, every evidence name with its read value, and keeps the model's own
    figure and "diff" inside the unverified model text only.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            await tune_world_bound(w)
            await w.nominate(tune_entry())
            assert await deliver_cards() == [], "a card was posted before its evidence was read"

            await drain_reads(w)
            posts = await deliver_cards(times=2)
            assert len(posts) == 1, posts
            assert posts[0]["channel"] == CHANNEL
            text = card_text(posts[0])
            platform = outside_model_text(text)

            diff_lines = [
                line
                for line in platform.splitlines()
                if RULE_SLOW in line
                and "threshold" in line
                and str(CURRENT_THRESHOLD) in line
                and str(PROPOSED_THRESHOLD) in line
            ]
            assert diff_lines, f"no platform-rendered diff line in:\n{text}"
            line = diff_lines[0]
            assert line.index(str(CURRENT_THRESHOLD)) < line.index(str(PROPOSED_THRESHOLD)), line

            for name in EVIDENCE_NAMES:
                assert name in platform, name
                assert str(OBSERVED[RULE_SLOW][name]) in platform, name
            # The other rule's evidence was never read for this nomination.
            for name in EVIDENCE_NAMES:
                assert str(OBSERVED[RULE_OVER_N][name]) not in text, name

            assert "unverified" in text.lower()
            assert MODEL_FIGURE in model_blocks(text)
            assert MODEL_FIGURE not in platform
            assert "1 -> 2" not in platform

    run(scenario)


def test_a_recorded_series_with_a_duplicate_rule_raises_exactly_one_tuning_request(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25 @spec AUTOMATED-REMEDIATION-15: "a recorded
    alert series with a duplicate rule, replayed as deliveries, produces exactly
    one tuning approval request whose card shows the platform-rendered diff and
    the declared-read evidence."

    Six deliveries of two rules for one condition each nominate retiring the
    duplicate: one approval, every nomination on it, one card naming the
    retirement and its duplicate with the retired rule's evidence.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            await tune_world_bound(w)
            rows = []
            for body in SERIES:
                (row,) = await w.nominate(retire_entry(), body=body)
                rows.append(row)
            await drain_reads(w)

            approvals = await tune_approvals()
            assert len(approvals) == 1, approvals
            approval = approvals[0]
            assert approval["granted_arguments"] == {
                "field": "retire",
                "rule": RULE_OVER_N,
                "value": {"duplicate_of": RULE_SLOW},
            }
            for row in rows:
                after = await nomination(row["id"])
                assert after["state"] == "approval_requested", after
                assert after["approval_id"] == approval["id"], after
            assert await all_forwards() == []

            posts = await deliver_cards(times=3)
            assert len(posts) == 1, posts
            text = card_text(posts[0])
            platform = outside_model_text(text)
            assert any(
                RULE_OVER_N in line and "retire" in line.lower() and RULE_SLOW in line
                for line in platform.splitlines()
            ), f"no platform-rendered retirement line in:\n{text}"
            for name in EVIDENCE_NAMES:
                assert name in platform, name
                assert str(OBSERVED[RULE_OVER_N][name]) in platform, name
            assert MODEL_FIGURE not in platform
            for body in SERIES:
                assert body.decode() not in text

    run(scenario)


async def _asked_series(w: World) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    await tune_world_bound(w)
    rows = [(await w.nominate(retire_entry(), body=body))[0] for body in SERIES[:3]]
    await drain_reads(w)
    (approval,) = await tune_approvals()
    return rows, approval


def test_approving_a_tuning_request_ends_tune_execution_not_automated_with_no_write(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25 (maintainer ruling 2026-10-07): "an approved
    ``tune`` request ends ``refused`` with ``tune_execution_not_automated``, with
    no write call, and the receipt says so."

    The approver's decision is recorded (``approved``); the route answers the
    code; every nomination of the request ends ``refused`` naming the code
    (``refusal_code``), with no execution and a decision time; no forward
    execution or ledger row exists, nothing but reads was ever claimable, and a
    reconciliation pass creates nothing either.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            rows, approval = await _asked_series(w)
            ledger_before = await ledger_rows()

            resolved = await resolve(w, approval["id"], "approved")
            assert resolved.status_code == 409, resolved.text
            assert detail_code(resolved) == "tune_execution_not_automated", resolved.text

            (after,) = await q(
                "SELECT * FROM curie.approvals WHERE id = :id", {"id": approval["id"]}
            )
            assert after["status"] == "approved", after
            for row in rows:
                ended = await nomination(row["id"])
                assert ended["state"] == "refused", ended
                assert ended["refusal_code"] == "tune_execution_not_automated", ended
                assert ended["execution_id"] is None, ended
                assert ended["verification_outcome"] is None, ended
                assert ended["decided_at"] is not None, ended

            reconcile = importlib.import_module("curie_api.remediation_approvals")
            engine = create_async_engine(get_settings().database_url)
            try:
                async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                    await reconcile.reconcile_remediation_approvals(session)
            finally:
                await engine.dispose()

            assert await all_forwards() == []
            assert (
                await q(
                    "SELECT id FROM curie.action_executions WHERE tool = :tool",
                    {"tool": RULES_WRITE_TOOL},
                )
                == []
            )
            assert await ledger_rows() == ledger_before == 0
            assert await drain_reads(w) == []

    run(scenario)


def test_rejecting_a_tuning_request_changes_nothing(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: "Rejection or expiry leaves the rule
    untouched and records it": every nomination ends ``rejected`` and nothing
    executes.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            rows, approval = await _asked_series(w)

            resolved = await resolve(w, approval["id"], "rejected")
            assert resolved.status_code == 200, resolved.text
            for row in rows:
                after = await nomination(row["id"])
                assert after["state"] == "rejected", after
                assert after["decided_at"] is not None, after
            assert await all_forwards() == []
            assert await ledger_rows() == 0
            assert await drain_reads(w) == []

    run(scenario)


def test_a_field_without_a_current_read_renders_only_the_platform_bound_change(
    ingress_broker: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec AUTOMATED-REMEDIATION-25: a nomination whose reason carries its own
    diff and figures for a field with no current-value read still gets no model
    text outside the unverified block: the card's diff names the proposed
    ``for_duration`` value the platform bound, and the model's figure stays
    unverified.
    """

    async def scenario() -> None:
        async with tune_world(ingress_broker, tmp_path, monkeypatch) as w:
            await tune_world_bound(w)
            asked = await asked_tune(w, tune_entry(field="for_duration", value=600))
            assert_approval(asked, "not_automatic")
            (post,) = await deliver_cards(times=2)
            text = card_text(post)
            platform = outside_model_text(text)
            assert any(
                RULE_SLOW in line and "for_duration" in line and "600" in line
                for line in platform.splitlines()
            ), text
            assert MODEL_FIGURE not in platform
            assert "1 -> 2" not in platform

    run(scenario)
