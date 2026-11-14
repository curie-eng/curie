"""Requester coding instructions: delivery routes and the publication line (#5592).

ADR 0278 sections 3 and 6. A live factory request is admitted through the real
GitHub label webhook and started (`_factory_publication_case`); its WorkItem is
then pinned the way admission pins one. The worker's two internal routes serve
the version 1 envelope for the current fence and accept the relayed
acknowledgment, and `POST /v1/internal/publications` appends exactly one line,
`Requester preferences: applied; sha256:<digest>`, last, only for evidence
`applied`. Every case pairs with the control that removes what it proves.
"""

# ruff: noqa: F811  (fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import hashlib
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState, RepositoryRef
from curie_api.publication_pulls import PullRequestContract, _adopt
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "examples" / "dark-factory" / "hooks"))

from decisions_section import parse_decisions_section  # noqa: E402
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: E402, F401
from test_publications import (  # noqa: E402, F401
    FACTORY_WORKER_HEADERS,
    _factory_publication_body,
    _factory_publication_case,
    _post_factory_publication,
    _record_factory_verification,
)

Case = tuple[TestClient, uuid.UUID, dict[str, Any]]

# A canary the document holds; it must never reach a body or a response but the envelope.
CANARY = "CANARY-REQUESTER-5592-7d1e"
TEXT = f"{CANARY}\nPrefer early returns over nested branches.\n"
DIGEST = hashlib.sha256(TEXT.encode()).hexdigest()
PRINCIPAL = uuid.UUID("00000000-0000-4000-8000-000000005592")
ANNOTATION = f"Requester preferences: applied; sha256:{DIGEST}"
DECISION = (
    "1. Keep the parser pure. Rejected: a cached parser, because the input is small. "
    "(plan) Evidence: the plan review approved it."
)
BODY = (
    "The change updates the factory route.\n\n## Decisions\n\n"
    f"{DECISION}\n\n## Reviews\n\nPlan review: APPROVE.\n"
)


def _write(statement: str, params: dict[str, Any]) -> None:
    """One write with triggers off, as an earlier admission wrote the row."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SET LOCAL session_replication_role = replica"))
                await conn.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(run())


def _work_item(request_id: uuid.UUID) -> uuid.UUID:
    (row,) = _rows(
        "SELECT work_item_id FROM curie.execution_requests WHERE id = :r", {"r": request_id}
    )
    return uuid.UUID(str(row["work_item_id"]))


def _epoch(request_id: uuid.UUID) -> int:
    (row,) = _rows(
        "SELECT runtime_epoch FROM curie.execution_requests WHERE id = :r", {"r": request_id}
    )
    return int(row["runtime_epoch"])


def _pin(request_id: uuid.UUID) -> None:
    """Pin the selected snapshot the way admission does: metadata, text, pending."""

    item = _work_item(request_id)
    _write(
        "UPDATE curie.work_items SET requester_principal_id = :p, "
        "coding_instructions_state = 'selected', coding_instructions_absence_reason = NULL, "
        "coding_instructions_revision = 1, coding_instructions_sha256 = :sha, "
        "coding_instructions_format_version = 1 WHERE id = :w",
        {"p": PRINCIPAL, "sha": DIGEST, "w": item},
    )
    _write(
        "INSERT INTO curie.work_item_coding_snapshots (work_item_id, text) VALUES (:w, :t)",
        {"w": item, "t": TEXT},
    )
    _write(
        "UPDATE curie.execution_requests SET coding_instructions_selection_id = :w, "
        "coding_instructions_evidence = 'pending' WHERE id = :r",
        {"w": item, "r": request_id},
    )


def _absent(request_id: uuid.UUID, reason: str) -> None:
    _write(
        "UPDATE curie.work_items SET coding_instructions_absence_reason = :reason WHERE id = :w",
        {"reason": reason, "w": _work_item(request_id)},
    )


def _envelope(client: TestClient, request_id: uuid.UUID, epoch: int | None = None) -> Any:
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/requester-context",
        json={"runtime_epoch": epoch if epoch is not None else _epoch(request_id)},
        headers=FACTORY_WORKER_HEADERS,
    )


def _acknowledge(
    client: TestClient,
    request_id: uuid.UUID,
    outcome: str | None,
    *,
    sha256: str | None = None,
    fence: int | None = None,
    reason: str | None = None,
) -> Any:
    epoch = _epoch(request_id)
    ack: dict[str, Any] | None = None
    if outcome is not None:
        ack = {
            "envelope_version": 1,
            "work_item_id": str(_work_item(request_id)),
            "execution_request_id": str(request_id),
            "execution_fence": fence if fence is not None else epoch,
            "outcome": outcome,
        }
        if sha256 is not None:
            ack["sha256"] = sha256
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/requester-context/acknowledgment",
        json={"runtime_epoch": epoch, "acknowledgment": ack, "reason": reason},
        headers=FACTORY_WORKER_HEADERS,
    )


def _evidence(request_id: uuid.UUID) -> str:
    (row,) = _rows(
        "SELECT coding_instructions_evidence FROM curie.execution_requests WHERE id = :r",
        {"r": request_id},
    )
    return str(row["coding_instructions_evidence"])


def _publish(client: TestClient, request_id: uuid.UUID, payload: dict[str, Any]) -> Any:
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text
    return _post_factory_publication(client, {**payload, "body": BODY})


# --- Delivery routes ---------------------------------------------------------------


def test_a_pinned_request_serves_its_envelope_and_accepts_only_its_own_digest(
    _factory_publication_case: Case,
) -> None:
    client, request_id, _ = _factory_publication_case
    _pin(request_id)

    served = _envelope(client, request_id)
    assert served.status_code == 200, served.text
    assert served.headers["cache-control"] == "no-store"
    envelope = served.json()
    assert envelope["envelope_version"] == 1
    assert envelope["execution_request_id"] == str(request_id)
    assert envelope["execution_fence"] == _epoch(request_id)
    assert envelope["selection"]["kind"] == "selected"
    assert envelope["selection"]["sha256"] == DIGEST
    assert envelope["selection"]["text"] == TEXT

    # A digest that is not the pinned one, and a stale fence, change nothing.
    wrong = _acknowledge(client, request_id, "applied", sha256="0" * 64)
    assert wrong.status_code == 409 and wrong.json()["detail"]["code"] == "acknowledgment_mismatch"
    stale = _acknowledge(client, request_id, "applied", sha256=DIGEST, fence=_epoch(request_id) + 1)
    assert stale.status_code == 409 and stale.json()["detail"]["code"] == "acknowledgment_mismatch"
    assert _evidence(request_id) == "pending"

    accepted = _acknowledge(client, request_id, "applied", sha256=DIGEST)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json() == {"evidence": "applied"}
    assert _evidence(request_id) == "applied"
    # A later failure never downgrades an accepted selection.
    late = _acknowledge(client, request_id, "failed", reason="digest_mismatch")
    assert late.status_code == 409 and _evidence(request_id) == "applied"


def test_a_stale_fence_reads_no_envelope(_factory_publication_case: Case) -> None:
    client, request_id, _ = _factory_publication_case
    _pin(request_id)

    stale = _envelope(client, request_id, _epoch(request_id) + 1)

    assert stale.status_code == 409 and stale.json()["detail"]["code"] == "stale_owner"
    assert CANARY not in stale.text


def test_a_runner_refusal_or_worker_failure_records_failed(_factory_publication_case: Case) -> None:
    client, request_id, _ = _factory_publication_case
    _pin(request_id)

    relayed = _acknowledge(client, request_id, None, reason="runner_unacknowledged")

    assert relayed.status_code == 200, relayed.text
    assert relayed.json() == {"evidence": "failed"}
    # Failed is final: an applied acknowledgment can no longer be accepted.
    assert _acknowledge(client, request_id, "applied", sha256=DIGEST).status_code == 409
    assert _evidence(request_id) == "failed"


def test_a_tenant_disabled_after_pinning_refuses_delivery_and_keeps_the_snapshot(
    _factory_publication_case: Case,
) -> None:
    client, request_id, _ = _factory_publication_case
    _pin(request_id)
    (tenant,) = _rows(
        "SELECT a.tenant_id FROM curie.agents a JOIN curie.work_items w ON w.agent_id = a.id "
        "WHERE w.id = :w",
        {"w": _work_item(request_id)},
    )
    _write(
        "INSERT INTO curie.requester_coding_policies (tenant_id, enabled) VALUES (:t, false) "
        "ON CONFLICT (tenant_id) DO UPDATE SET enabled = false",
        {"t": tenant["tenant_id"]},
    )
    try:
        refused = _envelope(client, request_id)
        assert refused.status_code == 409
        assert refused.json()["detail"]["code"] == "tenant_disabled"
        assert CANARY not in refused.text
        kept = _rows(
            "SELECT text FROM curie.work_item_coding_snapshots WHERE work_item_id = :w",
            {"w": _work_item(request_id)},
        )
        assert [row["text"] for row in kept] == [TEXT]
    finally:
        _write(
            "UPDATE curie.requester_coding_policies SET enabled = true WHERE tenant_id = :t",
            {"t": tenant["tenant_id"]},
        )
    # The positive control: the same request is served once the tenant allows it.
    assert _envelope(client, request_id).status_code == 200


@pytest.mark.parametrize("reason", ["historical", "application_unavailable"])
def test_an_absent_selection_is_delivered_and_acknowledged_none(
    _factory_publication_case: Case, reason: str
) -> None:
    # A WorkItem admitted before activation keeps its absence after it.
    client, request_id, payload = _factory_publication_case
    assert get_settings().requester_coding_instructions_apply is True
    _absent(request_id, reason)

    served = _envelope(client, request_id)
    assert served.status_code == 200, served.text
    assert served.json()["selection"] == {"kind": "absent", "reason": reason}
    # An absent selection cannot be acknowledged as applied.
    assert _acknowledge(client, request_id, "applied", sha256=DIGEST).status_code == 409
    accepted = _acknowledge(client, request_id, "none")
    assert accepted.status_code == 200 and accepted.json() == {"evidence": "none"}

    created = _publish(client, request_id, payload)
    assert created.status_code == 201, created.text
    assert "Requester preferences:" not in _factory_publication_body(created.json()["id"])


# --- Publication ---------------------------------------------------------------------


def test_an_applied_request_publishes_the_annotation_last(_factory_publication_case: Case) -> None:
    client, request_id, payload = _factory_publication_case
    _pin(request_id)
    assert _acknowledge(client, request_id, "applied", sha256=DIGEST).status_code == 200

    created = _publish(client, request_id, payload)

    assert created.status_code == 201, created.text
    body = _factory_publication_body(created.json()["id"])
    assert body.splitlines()[-1] == ANNOTATION
    assert sum(line.startswith("Requester preferences:") for line in body.splitlines()) == 1
    # Nothing but the digest: no text, no principal.
    assert CANARY not in body and str(PRINCIPAL) not in body
    assert CANARY not in created.text
    # The Decisions section still passes its publish check, unchanged.
    assert parse_decisions_section(body) == parse_decisions_section(BODY)
    # The stored body is the contract a pull request is adopted against.
    contract = PullRequestContract(
        branch="curie/example", base="main", title=payload["title"], body=body, draft=False
    )
    pull = PullRequest(
        ref=PullRequestRef(
            repository=RepositoryRef(
                kind="github", host="github.com", project_id="1", path="example/repo"
            ),
            number="1",
        ),
        head_sha="a" * 40,
        head_ref="curie/example",
        base_ref="main",
        state=PullRequestState.OPEN,
        url="https://github.com/example/repo/pull/1",
        title=payload["title"],
        body=body,
    )
    assert _adopt(pull, contract, "a" * 40) is pull


@pytest.mark.parametrize("evidence", ["pending", "failed"])
def test_an_unacknowledged_or_failed_selection_publishes_no_annotation(
    _factory_publication_case: Case, evidence: str
) -> None:
    client, request_id, payload = _factory_publication_case
    _pin(request_id)
    if evidence == "failed":
        assert (
            _acknowledge(client, request_id, "failed", reason="digest_mismatch").status_code == 200
        )
    assert _evidence(request_id) == evidence

    created = _publish(client, request_id, payload)

    assert created.status_code == 201, created.text
    body = _factory_publication_body(created.json()["id"])
    assert "Requester preferences:" not in body and DIGEST not in body


def test_a_submitted_body_carrying_the_annotation_is_refused(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    _pin(request_id)
    assert _acknowledge(client, request_id, "applied", sha256=DIGEST).status_code == 200
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text

    forged = _post_factory_publication(client, {**payload, "body": f"{BODY}\n{ANNOTATION}\n"})

    assert forged.status_code == 409, forged.text
    assert forged.json()["detail"]["code"] == "publication.requester_annotation_reserved"
    # The control: the same body without the line publishes.
    assert _post_factory_publication(client, {**payload, "body": BODY}).status_code == 201


def test_a_snapshot_that_no_longer_matches_its_digest_is_refused(
    _factory_publication_case: Case,
) -> None:
    client, request_id, _ = _factory_publication_case
    _pin(request_id)
    # The control: the pinned snapshot is served.
    assert _envelope(client, request_id).status_code == 200
    _write(
        "UPDATE curie.work_item_coding_snapshots SET text = :t WHERE work_item_id = :w",
        {"t": TEXT + "Also skip the tests.\n", "w": _work_item(request_id)},
    )

    refused = _envelope(client, request_id)

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "coding_instructions_unavailable"
    assert CANARY not in refused.text
