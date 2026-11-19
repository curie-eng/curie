"""The platform written learned rules line on a factory pull request (#5798).

A live factory request is admitted through the real GitHub label webhook and
started (`_factory_publication_case`); its WorkItem is then pinned the way
admission pins one, and the worker relays the runner's acknowledgment through
the real internal route. `POST /v1/internal/publications` writes exactly one
line, `Learned rules: in force; rules:R1,R4; sha256:<digest>`, only for evidence
`applied`, immediately before the `Requester preferences:` line, which stays
last. A submitted body that carries the line is refused before anything is
published. Every case pairs with the control that removes what it proves.
"""

# ruff: noqa: F401, F811  (fixtures are imported, then requested)

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_internal.memory_rules import canonical_text, rules_sha256
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_requester_instruction_publication as requester  # noqa: E402
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: E402
from test_publications import (  # noqa: E402
    FACTORY_WORKER_HEADERS,
    _factory_publication_body,
    _factory_publication_case,
    _post_factory_publication,
    _record_factory_verification,
)
from test_requester_instruction_publication import (  # noqa: E402
    BODY,
    _epoch,
    _publish,
    _work_item,
    _write,
)

Case = tuple[TestClient, uuid.UUID, dict[str, Any]]

CANARY = "CANARY-LEARNED-5798-pub"
RULES = [(1, f"Prefer early returns. {CANARY}"), (4, "Name tests after behavior.")]
TEXT = canonical_text(RULES)
DIGEST = rules_sha256(TEXT)
LINE = f"Learned rules: in force; rules:R1,R4; sha256:{DIGEST}"


def _pin(request_id: uuid.UUID) -> None:
    """Pin two rules the way admission does: metadata, snapshot, pending."""

    item = _work_item(request_id)
    _write(
        "UPDATE curie.work_items SET memory_rules_state = 'selected', memory_rules_count = 2, "
        "memory_rules_sha256 = :sha, memory_rules_format_version = 1 WHERE id = :w",
        {"sha": DIGEST, "w": item},
    )
    _write(
        "INSERT INTO curie.work_item_memory_rule_snapshots "
        "(work_item_id, rule_numbers, text, sha256) VALUES (:w, ARRAY[1, 4]::bigint[], :t, :sha)",
        {"w": item, "t": TEXT, "sha": DIGEST},
    )
    _write(
        "UPDATE curie.execution_requests SET memory_rules_evidence = 'pending' WHERE id = :r",
        {"r": request_id},
    )


def _acknowledge(
    client: TestClient, request_id: uuid.UUID, outcome: str | None, **extra: Any
) -> Any:
    epoch = _epoch(request_id)
    ack: dict[str, Any] | None = None
    if outcome is not None:
        ack = {
            "envelope_version": 1,
            "work_item_id": str(_work_item(request_id)),
            "execution_request_id": str(request_id),
            "execution_fence": epoch,
            "outcome": outcome,
            **extra,
        }
    return client.post(
        f"/v1/internal/work-items/requests/{request_id}/memory-rules-context/acknowledgment",
        json={"runtime_epoch": epoch, "acknowledgment": ack, "reason": None},
        headers=FACTORY_WORKER_HEADERS,
    )


def _evidence(request_id: uuid.UUID) -> str:
    (row,) = _rows(
        "SELECT memory_rules_evidence FROM curie.execution_requests WHERE id = :r",
        {"r": request_id},
    )
    return str(row["memory_rules_evidence"])


def _learned_lines(body: str) -> list[str]:
    return [line for line in body.splitlines() if line.startswith("Learned rules:")]


def test_an_applied_pin_names_its_rule_numbers_and_digest(_factory_publication_case: Case) -> None:
    client, request_id, payload = _factory_publication_case
    _pin(request_id)
    applied = _acknowledge(client, request_id, "applied", sha256=DIGEST)
    assert applied.status_code == 200 and applied.json() == {"evidence": "applied"}

    created = _publish(client, request_id, payload)

    assert created.status_code == 201, created.text
    body = _factory_publication_body(created.json()["id"])
    assert _learned_lines(body) == [LINE]
    # With no requester line, the learned rules line is last, after the body.
    assert body.splitlines()[-1] == LINE
    assert body.index(BODY.rstrip()) < body.index(LINE)
    # Numbers and digest only: no rule text, no "followed".
    assert CANARY not in body and "Prefer early returns" not in body
    assert "followed" not in LINE
    assert CANARY not in created.text


def test_both_annotations_put_learned_rules_immediately_before_the_requester_line(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    _pin(request_id)
    requester._pin(request_id)
    assert _acknowledge(client, request_id, "applied", sha256=DIGEST).status_code == 200
    assert (
        requester._acknowledge(client, request_id, "applied", sha256=requester.DIGEST).status_code
        == 200
    )

    created = _publish(client, request_id, payload)

    assert created.status_code == 201, created.text
    lines = _factory_publication_body(created.json()["id"]).splitlines()
    assert lines[-2:] == [LINE, requester.ANNOTATION]
    assert _learned_lines("\n".join(lines)) == [LINE]
    assert sum(line.startswith("Requester preferences:") for line in lines) == 1


@pytest.mark.parametrize("evidence", ["pending", "failed", "none"])
def test_no_line_unless_the_runner_acknowledged_the_pin(
    _factory_publication_case: Case, evidence: str
) -> None:
    client, request_id, payload = _factory_publication_case
    if evidence == "none":
        # An absent pin is acknowledged none.
        assert _acknowledge(client, request_id, "none").json() == {"evidence": "none"}
    else:
        _pin(request_id)
        if evidence == "failed":
            assert _acknowledge(client, request_id, "failed").json() == {"evidence": "failed"}
    assert _evidence(request_id) == evidence

    created = _publish(client, request_id, payload)

    assert created.status_code == 201, created.text
    body = _factory_publication_body(created.json()["id"])
    assert _learned_lines(body) == [] and DIGEST not in body


@pytest.mark.parametrize(
    "forged", [LINE, "Learned rules: in force; rules:R9; sha256:" + "0" * 64, "Learned rules:"]
)
def test_a_submitted_body_carrying_the_line_is_refused_and_nothing_is_published(
    _factory_publication_case: Case, forged: str
) -> None:
    client, request_id, payload = _factory_publication_case
    _pin(request_id)
    assert _acknowledge(client, request_id, "applied", sha256=DIGEST).status_code == 200
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text
    before = _rows("SELECT id FROM curie.publications")

    refused = _post_factory_publication(client, {**payload, "body": f"{BODY}\n{forged}\n"})

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.learned_rules_annotation_reserved"
    assert _rows("SELECT id FROM curie.publications") == before
    # The control: the same body without the line publishes, with the platform line.
    created = _post_factory_publication(client, {**payload, "body": BODY})
    assert created.status_code == 201, created.text
    assert _learned_lines(_factory_publication_body(created.json()["id"])) == [LINE]


def test_a_line_that_only_mentions_learned_rules_mid_line_is_not_reserved(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text

    created = _post_factory_publication(
        client, {**payload, "body": f"{BODY}\nSee the Learned rules: section above.\n"}
    )

    assert created.status_code == 201, created.text
