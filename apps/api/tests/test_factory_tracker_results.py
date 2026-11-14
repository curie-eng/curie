"""Tracker result proposals and their exactly-once delivery (ADR 0277 decision 4, #5585).

Criterion 3: the typed proposal against ``tests/vectors/tracker-result-proposal.json``.
Criterion 5: delivery on real Postgres. The source WorkItem is admitted through
a signed GitHub issues webhook against create_app() and made running through
`workitem_dispatch.acquire` and `start`, as test_factory_terminus does, then
marked a tracker result request with its session reserved (#5586). The
tracker is the in-memory adapter keyed to that WorkItem's issue. Grants are
ORM inserts using the production `source_fingerprint`, `output_identity_for`
and `source_content_sha256`. Time moves only by SQL on this file's rows.
Criterion 6: the storage boundary, by planted sentinels.

#5586 criterion 2 drives the runner-facing routes over HTTP with the issue
read capability the internal mint route issues, against a tracker result
request whose deployed version carries a result policy; only the tracker is
substituted (the in-memory adapter, through
`factory_tracker_result_runtime.work_item_tracker`).
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import Settings, get_settings
from curie_api.factory_tracker_result_delivery import (
    LISTING_SETTLE_SECONDS,
    AttemptError,
    DeliveryOutcome,
    DeliveryState,
    RefusalReason,
    TrackerResultRefused,
    TrackerResultStorageError,
    deliver,
    due_deliveries,
    expire_overdue,
    output_identity_for,
    result_marker,
    source_fingerprint,
    submit_result,
)
from curie_api.factory_tracker_result_runtime import source_content_sha256
from curie_api.factory_tracker_results import (
    FindingsReason,
    PlatformFindings,
    ProposalInvalid,
    ResultProposal,
    parse_proposal,
    proposal_content,
    proposal_sha256,
)
from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.errors import Unauthorized, Unavailable
from curie_api.forges.memory import InMemoryTracker, all_supported
from curie_api.forges.types import ReplyTarget, SourceContent, TrackerIssueRef
from curie_api.models import (
    ExecutionRequest,
    FactoryTrackerResultArtifact,
    FactoryTrackerResultGrant,
    WorkItem,
)
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from forge_fakes import factory_wake
from forge_fakes.github import LABEL, REPO_ID, GitHubAPI, _issue_event, _post
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "tracker-result-proposal.json"
REVISION = "3f2a9c0d4b5e6f708192a3b4c5d6e7f8091a2b3c"
SOURCE_TITLE = "Export drops a row"
SOURCE_BODY = "Reported symptom."
POLICY_DIGEST = "ab" * 32


# Criterion 3: the proposal ------------------------------------------------------


def _vector() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert set(data) == {"comment", "valid", "invalid"}
    return data


def _filled(case: dict[str, Any]) -> Any:
    proposal = json.loads(json.dumps(case["proposal"]))
    fill = case.get("fill")
    if fill is not None:
        (key,) = fill["path"]
        proposal[key] = fill["char"] * fill["length"]
    return proposal


def _independent_sha256(proposal: dict[str, Any]) -> str:
    present = {key: value for key, value in proposal.items() if value is not None}
    canonical = json.dumps(present, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def test_every_valid_vector_parses_with_its_digest() -> None:
    cases = _vector()["valid"]
    assert {case["proposal"]["kind"] for case in cases} == {"ticket", "findings"}
    for case in cases:
        proposal = parse_proposal(case["proposal"])
        assert proposal.kind == case["proposal"]["kind"], case["name"]
        assert proposal_sha256(proposal) == case["proposal_sha256"], case["name"]
        assert _independent_sha256(case["proposal"]) == case["proposal_sha256"], case["name"]
        # Key order does not move the digest.
        reordered = dict(reversed(list(case["proposal"].items())))
        assert proposal_sha256(parse_proposal(reordered)) == case["proposal_sha256"]


def test_every_invalid_vector_reports_exactly_its_issues() -> None:
    names = set()
    for case in _vector()["invalid"]:
        names.add(case["name"])
        with pytest.raises(ProposalInvalid) as refused:
            parse_proposal(_filled(case))
        got = sorted([issue.field, issue.code] for issue in refused.value.issues)
        assert got == sorted(case["issues"]), case["name"]
    # The cases criterion 3 names.
    assert {
        "submitted_budget_exhausted",
        "destination_repository",
        "destination_labels",
        "destination_agent",
        "destination_assignee",
        "destination_destination",
        "unknown_field",
        "oversized_body",
        "revision_39_hex",
        "revision_uppercase",
    } <= names


def test_a_refusal_never_carries_submitted_text() -> None:
    planted = "plantedvaluewombat"
    raw = {
        "kind": "findings",
        "reason": f"budget_exhausted {planted}",
        "tried": planted * 300,
        "missing": f"{planted}\x00",
        "revision": planted,
        f"{planted} key": planted,
        "destination": planted,
    }
    with pytest.raises(ProposalInvalid) as refused:
        parse_proposal(raw)
    rendered = repr(refused.value.issues) + str(refused.value) + repr(refused.value.args)
    assert planted not in rendered
    assert refused.value.__cause__ is None and refused.value.__context__ is None
    assert {issue.code for issue in refused.value.issues} == {
        "value",
        "length",
        "text",
        "revision",
        "unknown_field",
    }


def test_an_identifier_shaped_key_is_never_echoed() -> None:
    sentinel = f"SENTINEL_KEY_{uuid.uuid4().hex[:4]}"
    raw = {
        "kind": "findings",
        "reason": "not_reproduced",
        "tried": sentinel,
        "missing": "m",
        sentinel: sentinel,
    }
    nested = {
        "kind": "ticket",
        "title": "t",
        "body": "b",
        "reproduction": {"command": "c", "observed": "o", "revision": REVISION, sentinel: 1},
    }
    for proposal, field in ((raw, "*"), (nested, "reproduction.*")):
        with pytest.raises(ProposalInvalid) as refused:
            parse_proposal(proposal)
        assert [(issue.field, issue.code) for issue in refused.value.issues] == [
            (field, "unknown_field")
        ]
        rendered = repr(refused.value.issues) + str(refused.value) + repr(refused.value.args)
        assert sentinel not in rendered


def test_budget_exhausted_is_platform_only() -> None:
    platform = PlatformFindings(
        kind="findings", reason=FindingsReason.BUDGET_EXHAUSTED, tried="t", missing="m"
    )
    assert platform.model_dump(mode="json")["reason"] == "budget_exhausted"
    with pytest.raises(ProposalInvalid) as refused:
        parse_proposal(platform.model_dump(mode="json"))
    assert [(issue.field, issue.code) for issue in refused.value.issues] == [("reason", "value")]


@pytest.mark.parametrize(
    ("field", "path"),
    [
        ("title", "title"),
        ("body", "body"),
        ("tried", "tried"),
        ("command", "reproduction.command"),
    ],
)
def test_a_lone_surrogate_is_a_proposal_issue_not_an_encode_error(field: str, path: str) -> None:
    # A JSON "\ud800" escape decodes to a lone surrogate, which UTF-8 cannot
    # encode, so the proposal digest would raise UnicodeEncodeError (review).
    ticket: dict[str, Any] = {
        "kind": "ticket",
        "title": "t",
        "body": "b",
        "reproduction": {"command": "c", "observed": "o", "revision": REVISION},
    }
    findings: dict[str, Any] = {
        "kind": "findings",
        "reason": "not_reproduced",
        "tried": "t",
        "missing": "m",
    }
    raw = findings if field == "tried" else ticket
    if field == "command":
        raw["reproduction"]["command"] = "run \ud800"
    else:
        raw[field] = "x \ud800"
    with pytest.raises(ProposalInvalid) as refused:
        parse_proposal(raw)
    # Pydantic's strict string check refuses it before any digest is taken.
    assert [(issue.field, issue.code) for issue in refused.value.issues] == [(path, "type")]
    # Liveness: the same proposal without the surrogate parses and digests.
    if field == "command":
        raw["reproduction"]["command"] = "run \u00e9"
    else:
        raw[field] = "x \u00e9"
    assert len(proposal_sha256(parse_proposal(raw))) == 64


def test_the_delivery_deadline_setting_is_a_positive_finite_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "CURIE_FACTORY_TRACKER_RESULT_DELIVERY_DEADLINE_SECONDS"
    monkeypatch.delenv(name, raising=False)
    assert Settings().factory_tracker_result_delivery_deadline_seconds == 900
    for value in ("0", "-1", "inf", "86401"):
        monkeypatch.setenv(name, value)
        with pytest.raises(ValidationError):
            Settings()
    monkeypatch.setenv(name, "60")
    assert Settings().factory_tracker_result_delivery_deadline_seconds == 60


# Delivery setup -----------------------------------------------------------------


def _run[T](work: Callable[[async_sessionmaker[AsyncSession]], Awaitable[T]]) -> T:
    """Run service code on this thread's own loop and engine.

    A `BaseException` raised inside (a modelled process death) propagates
    after the engine is disposed.
    """

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            return await work(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _exec(statement: str, params: dict[str, Any] | None = None) -> None:
    async def work(maker: async_sessionmaker[AsyncSession]) -> None:
        async with maker() as session:
            await session.execute(text(statement), params or {})
            await session.commit()

    _run(work)


def _label(client: Any, github: GitHubAPI, number: int) -> None:
    github.issue_number = number
    github.advance_label_event(number)
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"


def _start_running(client: Any, request_id: uuid.UUID) -> int:
    async def go() -> int:
        async with client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            started = await start(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                claim_name=f"claim-{request_id.hex[:12]}",
                sandbox_name=f"sbx-{request_id.hex[:12]}",
                context=acquired.context,
            )
            assert not isinstance(started, DispatchConflict), started
            return int(started.runtime_epoch)

    return int(client.portal.call(go))


@dataclass
class _Source:
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    epoch: int
    tracker: InMemoryTracker
    issue: TrackerIssueRef
    grant_id: uuid.UUID


def _grant(request_id: uuid.UUID, kinds: tuple[str, ...] = ("findings", "ticket")) -> uuid.UUID:
    async def work(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID:
        async with maker() as session:
            request = await session.get(ExecutionRequest, request_id)
            assert request is not None
            work_item = await session.get(WorkItem, request.work_item_id)
            assert work_item is not None
            grant = FactoryTrackerResultGrant(
                id=uuid.uuid4(),
                work_item_id=work_item.id,
                execution_request_id=request.id,
                source_fingerprint=source_fingerprint(work_item, request),
                repository_id=work_item.repository_project_id,
                allowed_result_kinds=sorted(kinds),
                output_identity=output_identity_for(request.id),
                implementation_label="factory-implement" if "ticket" in kinds else None,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                source_content_sha256=source_content_sha256(
                    SourceContent(SOURCE_TITLE, SOURCE_BODY)
                ),
            )
            session.add(grant)
            await session.commit()
            return grant.id

    return _run(work)


def _source(
    admitted: Any, number: int, *, kinds: tuple[str, ...] = ("findings", "ticket")
) -> _Source:
    client, github, _sink = admitted
    _label(client, github, number)
    (row,) = _rows(
        "SELECT r.id AS request_id, w.id AS work_item_id, w.tracker_kind, w.tracker_host, "
        "w.tracker_scope_id, w.tracker_issue_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )
    epoch = _start_running(client, row["request_id"])
    # A tracker result request whose one session is reserved (#5586).
    _exec(
        "UPDATE curie.execution_requests SET completion_contract = 'tracker_result', "
        "result_policy_digest = :digest, model_session_reserved_at = clock_timestamp() "
        "WHERE id = :id",
        {"id": row["request_id"], "digest": POLICY_DIGEST},
    )
    tracker = InMemoryTracker(
        kind=row["tracker_kind"], host=row["tracker_host"], scope_id=row["tracker_scope_id"]
    )
    issue = tracker.seed_issue(SOURCE_TITLE, SOURCE_BODY, issue_id=row["tracker_issue_id"])
    return _Source(
        request_id=row["request_id"],
        work_item_id=row["work_item_id"],
        epoch=epoch,
        tracker=tracker,
        issue=issue,
        grant_id=_grant(row["request_id"], kinds),
    )


def _ticket(**over: Any) -> ResultProposal:
    raw: dict[str, Any] = {
        "kind": "ticket",
        "title": "Export drops the last row",
        "body": "## Problem\n\nThe export omits the final row.",
        "reproduction": {
            "command": "uv run pytest tests/test_export.py -q",
            "observed": "1 failed: expected 3 rows, got 2",
            "revision": REVISION,
        },
    }
    raw.update(over)
    return parse_proposal(raw)


def _findings(**over: Any) -> ResultProposal:
    raw: dict[str, Any] = {
        "kind": "findings",
        "reason": "not_reproduced",
        "tried": "Ran the export three times on the sample data.",
        "missing": "A dataset that shows the dropped row.",
        "revision": REVISION,
    }
    raw.update(over)
    return parse_proposal(raw)


def _proposal(kind: str) -> ResultProposal:
    return _ticket() if kind == "ticket" else _findings()


def _submit(
    src: _Source,
    proposal: ResultProposal,
    *,
    grant_id: uuid.UUID | None = None,
    request_id: uuid.UUID | None = None,
    epoch: int | None = None,
    tracker: InMemoryTracker | None = None,
    settings: Settings | None = None,
) -> uuid.UUID:
    async def work(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID:
        async with maker() as session:
            return await submit_result(
                session,
                grant_id=grant_id or src.grant_id,
                execution_request_id=request_id or src.request_id,
                runtime_epoch=src.epoch if epoch is None else epoch,
                proposal=proposal,
                tracker=tracker or src.tracker,
                settings=settings or get_settings(),
            )

    return _run(work)


def _deliver(src: _Source, delivery_id: uuid.UUID, *, owner: str = "deliverer") -> DeliveryOutcome:
    async def work(maker: async_sessionmaker[AsyncSession]) -> DeliveryOutcome:
        return await deliver(
            maker, delivery_id, tracker=src.tracker, settings=get_settings(), owner=owner
        )

    return _run(work)


def _delivery(delivery_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT state, attempts, last_error_code, failure_code, output_issue_id, "
        "attempt_owner, submitted_at, deadline_at, create_attempted_at "
        "FROM curie.factory_tracker_result_deliveries WHERE id = :id",
        {"id": delivery_id},
    )
    return dict(row)


def _count(table: str, delivery_id: uuid.UUID | None = None) -> int:
    where = "" if delivery_id is None else " WHERE delivery_id = :id"
    (row,) = _rows(
        f"SELECT count(*) AS n FROM curie.{table}{where}",
        {} if delivery_id is None else {"id": delivery_id},
    )
    return int(row["n"])


def _deliveries() -> int:
    (row,) = _rows("SELECT count(*) AS n FROM curie.factory_tracker_result_deliveries")
    return int(row["n"])


def _comments(src: _Source) -> list[str]:
    target = ReplyTarget.on_issue(src.issue)
    return [body for author, body in src.tracker.marked_comments.comments(target)]


def _own_issues(src: _Source) -> list[TrackerIssueRef]:
    return src.tracker.issues(author=src.tracker.identity)


def _age_lease(delivery_id: uuid.UUID) -> None:
    _exec(
        "UPDATE curie.factory_tracker_result_deliveries "
        "SET attempt_lease_expires_at = clock_timestamp() - interval '1 second' "
        "WHERE id = :id",
        {"id": delivery_id},
    )


def _make_due(delivery_id: uuid.UUID) -> None:
    """Move the retry backoff past, as waiting for it would."""

    _exec(
        "UPDATE curie.factory_tracker_result_deliveries "
        "SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": delivery_id},
    )


def _age_deadline(delivery_id: uuid.UUID) -> None:
    _exec(
        "UPDATE curie.factory_tracker_result_deliveries "
        "SET submitted_at = clock_timestamp() - interval '2 seconds', "
        "deadline_at = clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": delivery_id},
    )


def _assert_delivered_once(src: _Source, delivery_id: uuid.UUID, kind: str) -> None:
    row = _delivery(delivery_id)
    assert (row["state"], row["failure_code"], row["attempt_owner"]) == ("delivered", None, None)
    assert _count("factory_tracker_result_receipts", delivery_id) == 1
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0
    comments = _comments(src)
    assert len(comments) == 1
    assert result_marker(src.request_id) in comments[0]
    (receipt,) = _rows(
        "SELECT result_kind, reason, revision, result_comment_id, output_issue_id, "
        "output_url, output_body_sha256 FROM curie.factory_tracker_result_receipts "
        "WHERE delivery_id = :id",
        {"id": delivery_id},
    )
    assert receipt["result_kind"] == kind
    assert receipt["revision"] == REVISION
    if kind == "ticket":
        (issue,) = _own_issues(src)
        assert len(src.tracker.sent_issue_bodies) == 1
        assert receipt["output_issue_id"] == issue.issue_id == row["output_issue_id"]
        assert receipt["output_url"] == src.tracker.issue_url(issue)
        assert (
            receipt["output_body_sha256"]
            == hashlib.sha256(src.tracker.issue_body(issue).encode()).hexdigest()
        )
        assert receipt["reason"] is None
        # The planted reproduction values and the link, nothing about wording.
        assert "uv run pytest tests/test_export.py -q" in comments[0]
        assert "1 failed: expected 3 rows, got 2" in comments[0]
        assert receipt["output_url"] in comments[0]
    else:
        assert _own_issues(src) == []
        assert receipt["output_issue_id"] is None
        assert receipt["reason"] == "not_reproduced"
        assert "Ran the export three times on the sample data." in comments[0]
        assert "A dataset that shows the dropped row." in comments[0]


# Criterion 5: exactly once ----------------------------------------------------------


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_duplicate_submission_delivers_once(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9301)
    proposal = _proposal(kind)
    first = _submit(src, proposal)
    assert _submit(src, proposal) == first
    assert _deliveries() == 1
    assert src.tracker.ledger.writes == 0
    outcome = _deliver(src, first)
    assert (outcome.state, outcome.attempted) == (DeliveryState.DELIVERED, True)
    _assert_delivered_once(src, first, kind)
    writes = src.tracker.ledger.writes
    # Delivered: the same submission returns the same id and a second attempt writes nothing.
    assert _submit(src, proposal) == first
    again = _deliver(src, first)
    assert (again.state, again.attempted) == (DeliveryState.DELIVERED, False)
    assert src.tracker.ledger.writes == writes
    # Another proposal under the same grant is a conflict.
    other = _ticket(title="Another title") if kind == "ticket" else _findings(missing="Other.")
    with pytest.raises(TrackerResultRefused) as refused:
        _submit(src, other)
    assert refused.value.reason is RefusalReason.PROPOSAL_CONFLICT
    _assert_delivered_once(src, first, kind)


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_mention_in_submitted_text_never_reaches_the_tracker_live(
    admitted: Any, kind: str
) -> None:
    # Review finding: model text holding "@login" was posted as live GitHub
    # text under the App's identity and notified that account.
    src = _source(admitted, 9392)
    mention = "@octocat"
    if kind == "ticket":
        proposal = parse_proposal(
            {
                "kind": "ticket",
                "title": f"Ping {mention}",
                "body": f"Seen by {mention} and team@example.com.",
                "reproduction": {
                    "command": f"echo {mention}",
                    "observed": f"printed {mention}",
                    "revision": REVISION,
                },
            }
        )
    else:
        proposal = _findings(tried=f"Asked {mention}.", missing=f"Word from {mention}.")
    delivery_id = _submit(src, proposal)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    written = list(_comments(src))
    if kind == "ticket":
        (issue,) = _own_issues(src)
        written.append(src.tracker.issue_body(issue))
        written.extend(src.tracker.sent_issue_bodies)
    assert written
    for posted in written:
        assert mention not in posted
    # The text is still there for a reader, only the mention is broken.
    assert any("@\u200boctocat" in posted for posted in written)


# Review finding: model text in the source comment rendered live Markdown and
# reordered the App's own lines with bidi controls.
_CONTROLS = ("\u202e", "\u200d", "\u0085", "\u2066", "\u200f", "\x1b")
_ORDINARY = "Ordinary words stay verbatim."
_HOSTILE = (
    f"{_ORDINARY}\n"
    "Override\u202e here, joiner\u200d here, C1\u0085 here, isolate\u2066 here, "
    "mark\u200f here, escape\x1b[31m here.\n"
    "[x](https://evil.example)\n"
    "![i](https://evil.example/i.png)\n"
    "<https://evil.example>\n"
    '<a href="https://evil.example">a</a>\n'
    "```\n"
    "[y](https://evil.example/after-inner-fence)\n"
    "```\n"
    "````"
)


def _fenced_line_numbers(markdown: str) -> set[int]:
    """The lines a CommonMark backtick fence holds; every fence must close."""

    inside: set[int] = set()
    opener = 0
    for number, line in enumerate(markdown.split("\n")):
        run = len(line) - len(line.lstrip("`"))
        if opener == 0:
            if run >= 3 and "`" not in line[run:]:
                opener = run
        elif run >= opener and not line[run:].strip():
            opener = 0
        else:
            inside.add(number)
    assert opener == 0, "a fence was left open"
    return inside


def _assert_inert(posted: str) -> None:
    for control in _CONTROLS:
        assert control not in posted
    lines = posted.split("\n")
    inside = _fenced_line_numbers(posted)
    live = [
        line
        for number, line in enumerate(lines)
        if ("evil.example" in line or line == "```") and number not in inside
    ]
    assert live == []
    # Every hostile form still reached the comment, inside a fence.
    for form in ("[x](", "![i](", "<https://evil", "<a href=", "after-inner-fence"):
        assert any(form in lines[number] for number in inside)
    assert any(lines[number] == _ORDINARY for number in inside)


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_model_text_in_a_published_comment_is_inert(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9393)
    if kind == "ticket":
        proposal = _ticket(
            title="Export\u202e drops\u200d the\u0085 row",
            body=_HOSTILE,
            reproduction={"command": _HOSTILE, "observed": _HOSTILE, "revision": REVISION},
        )
    else:
        proposal = _findings(tried=_HOSTILE, missing=_HOSTILE)
    delivery_id = _submit(src, proposal)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    (comment,) = _comments(src)
    _assert_inert(comment)
    inside = _fenced_line_numbers(comment)
    fenced = [line for number, line in enumerate(comment.split("\n")) if number in inside]
    # Both model fields, each wholly fenced.
    assert fenced.count(_ORDINARY) == 2
    if kind == "ticket":
        # A ticket body is model Markdown by design: no control survives, and
        # the title is plain text without them too.
        (issue,) = _own_issues(src)
        body = src.tracker.issue_body(issue)
        title = asyncio.run(src.tracker.read_source_content(issue)).title
        for control in _CONTROLS:
            assert control not in body
            assert control not in title
        assert _ORDINARY in body
        assert title == "Export drops the row"


class _Restart(BaseException):
    """The process dies; not an `Exception`, so nothing in the service catches it."""


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_restart_between_the_issue_write_and_the_comment_write_delivers_once(
    admitted: Any, kind: str
) -> None:
    src = _source(admitted, 9302)
    delivery_id = _submit(src, _proposal(kind))
    src.tracker.write_faults.abort_before_next(Operation.UPSERT_MARKED, _Restart())
    with pytest.raises(_Restart):
        _deliver(src, delivery_id, owner="first-process")
    assert _comments(src) == []
    row = _delivery(delivery_id)
    assert (row["state"], row["attempt_owner"]) == ("owed", "first-process")
    if kind == "ticket":
        # The issue was written and recorded before the comment.
        (issue,) = _own_issues(src)
        assert row["output_issue_id"] == issue.issue_id
    # The dead attempt's lease still holds.
    held = _deliver(src, delivery_id, owner="second-process")
    assert (held.state, held.attempted) == (DeliveryState.OWED, False)
    _age_lease(delivery_id)
    outcome = _deliver(src, delivery_id, owner="second-process")
    assert outcome.state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, kind)
    assert _delivery(delivery_id)["attempts"] == 2


def test_a_lost_create_response_reconciles_to_the_one_issue(admitted: Any) -> None:
    src = _source(admitted, 9303)
    delivery_id = _submit(src, _ticket())
    # The answer is lost and the adapter's in-call relist confirms the issue.
    src.tracker.write_faults.lose_next_response(Operation.CREATE_LINKED_ISSUE)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")
    assert _delivery(delivery_id)["attempts"] == 1


def _due() -> list[uuid.UUID]:
    async def work(maker: async_sessionmaker[AsyncSession]) -> list[uuid.UUID]:
        async with maker() as session:
            return await due_deliveries(session)

    return _run(work)


def _unconfirmed_create(src: _Source, delivery_id: uuid.UUID) -> TrackerIssueRef:
    """A create whose answer is lost while the listing lags: written, unconfirmed."""

    src.tracker.hide_new_from_listing = True
    src.tracker.write_faults.lose_next_response(Operation.CREATE_LINKED_ISSUE)
    lost = _deliver(src, delivery_id)
    assert (lost.state, lost.last_error_code) == (DeliveryState.OWED, "output_unconfirmed")
    (issue,) = _own_issues(src)
    row = _delivery(delivery_id)
    assert row["output_issue_id"] is None and row["create_attempted_at"] is not None
    return issue


def test_an_unconfirmed_create_waits_for_the_listing_to_settle(admitted: Any) -> None:
    src = _source(admitted, 9305)
    delivery_id = _submit(src, _ticket())
    assert _due() == [delivery_id]
    issue = _unconfirmed_create(src, delivery_id)
    assert LISTING_SETTLE_SECONDS == 60
    (row,) = _rows(
        "SELECT next_attempt_at - updated_at >= interval '60 seconds' AS from_attempt, "
        "next_attempt_at > clock_timestamp() + interval '50 seconds' AS still_out "
        "FROM curie.factory_tracker_result_deliveries WHERE id = :id",
        {"id": delivery_id},
    )
    assert row == {"from_attempt": True, "still_out": True}
    assert _due() == []
    # The claim refuses the row inside the settle window too.
    writes = src.tracker.ledger.writes
    early = _deliver(src, delivery_id)
    assert (early.state, early.attempted) == (DeliveryState.OWED, False)
    assert src.tracker.ledger.writes == writes and _delivery(delivery_id)["attempts"] == 1
    _make_due(delivery_id)
    assert _due() == [delivery_id]
    src.tracker.show_in_listing(issue)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")


@pytest.mark.parametrize("appears", [False, True])
def test_an_unconfirmed_create_is_never_sent_again(admitted: Any, appears: bool) -> None:
    src = _source(admitted, 9306)
    delivery_id = _submit(src, _ticket())
    issue = _unconfirmed_create(src, delivery_id)
    writes = src.tracker.ledger.writes
    # Past every settle window the listing still omits it; retries only reconcile.
    for _ in range(3):
        _make_due(delivery_id)
        retry = _deliver(src, delivery_id)
        assert (retry.state, retry.last_error_code) == (DeliveryState.OWED, "output_unconfirmed")
        assert _own_issues(src) == [issue] and len(src.tracker.sent_issue_bodies) == 1
        assert src.tracker.ledger.writes == writes and _comments(src) == []
        assert _delivery(delivery_id)["create_attempted_at"] is not None
    if appears:
        src.tracker.show_in_listing(issue)
        _make_due(delivery_id)
        assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
        _assert_delivered_once(src, delivery_id, "ticket")
        return
    _age_deadline(delivery_id)
    failed = _deliver(src, delivery_id)
    assert (failed.state, failed.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert _own_issues(src) == [issue]
    assert _count("factory_tracker_result_receipts", delivery_id) == 0


def test_a_process_death_after_the_create_request_never_creates_again(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = _source(admitted, 9307)
    delivery_id = _submit(src, _ticket())
    src.tracker.hide_new_from_listing = True
    real_create = src.tracker.create_linked_issue

    async def dies_after_the_write(*args: Any, **kwargs: Any) -> Any:
        await real_create(*args, **kwargs)
        raise _Restart

    monkeypatch.setattr(src.tracker, "create_linked_issue", dies_after_the_write)
    with pytest.raises(_Restart):
        _deliver(src, delivery_id, owner="dead-process")
    monkeypatch.setattr(src.tracker, "create_linked_issue", real_create)
    (issue,) = _own_issues(src)
    row = _delivery(delivery_id)
    assert row["create_attempted_at"] is not None and row["output_issue_id"] is None
    _age_lease(delivery_id)
    retry = _deliver(src, delivery_id)
    assert (retry.state, retry.last_error_code) == (DeliveryState.OWED, "output_unconfirmed")
    assert _own_issues(src) == [issue] and len(src.tracker.sent_issue_bodies) == 1
    src.tracker.show_in_listing(issue)
    _make_due(delivery_id)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")


@pytest.mark.parametrize("refusal", ["unauthorized", "unprocessable"])
def test_a_definite_refusal_clears_the_mark_and_a_retry_creates_once(
    admitted: Any, refusal: str
) -> None:
    src = _source(admitted, 9308)
    delivery_id = _submit(src, _ticket())
    error: Unauthorized | Unavailable = (
        Unauthorized("credential refused") if refusal == "unauthorized" else Unavailable(refusal)
    )
    src.tracker.write_faults.refuse_next_write(Operation.CREATE_LINKED_ISSUE, error)
    refused = _deliver(src, delivery_id)
    expected_code = "tracker_unauthorized" if refusal == "unauthorized" else "tracker_unavailable"
    assert (refused.state, refused.last_error_code) == (DeliveryState.OWED, expected_code)
    assert _own_issues(src) == [] and src.tracker.ledger.writes == 0
    assert _delivery(delivery_id)["create_attempted_at"] is None
    _make_due(delivery_id)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_lost_comment_response_delivers_one_comment(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9304)
    delivery_id = _submit(src, _proposal(kind))
    src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    lost = _deliver(src, delivery_id)
    assert (lost.state, lost.last_error_code) == (DeliveryState.OWED, "tracker_unavailable")
    assert len(_comments(src)) == 1
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 1
    _make_due(delivery_id)
    outcome = _deliver(src, delivery_id)
    assert outcome.state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, kind)


# Criterion 5: fences ----------------------------------------------------------------


_SUBMISSION_FENCES = [
    "source_fingerprint_changed",
    "stale_epoch",
    "lapsed_heartbeat",
    "cancelled",
    "grant_for_another_request",
    "grant_not_found",
    "grant_scope_mismatch",
    "grant_expired",
    "result_kind_not_allowed",
    "tracker_mismatch",
    "tracker_operation_unsupported",
    "closes_source",
]


@pytest.mark.parametrize("fence", _SUBMISSION_FENCES)
def test_a_failed_fence_refuses_submission_and_writes_nothing(admitted: Any, fence: str) -> None:
    kinds = ("findings",) if fence == "result_kind_not_allowed" else ("findings", "ticket")
    src = _source(admitted, 9311, kinds=kinds)
    params = {"work_item": src.work_item_id, "request": src.request_id, "grant": src.grant_id}
    bad: dict[str, Any] = {}
    (admitted_with,) = _rows(
        "SELECT trigger_label FROM curie.work_items WHERE id = :work_item", params
    )
    proposal = _ticket()
    other_tracker: InMemoryTracker | None = None
    expected = {
        "stale_epoch": RefusalReason.OWNER_NOT_LIVE,
        "lapsed_heartbeat": RefusalReason.OWNER_NOT_LIVE,
        "grant_for_another_request": RefusalReason.GRANT_REQUEST_MISMATCH,
    }.get(fence) or RefusalReason(fence)
    if fence == "source_fingerprint_changed":
        # A relabel to another trigger moves the admission facts.
        _exec(
            "UPDATE curie.work_items SET trigger_label = 'other-trigger' WHERE id = :work_item",
            params,
        )
    elif fence == "stale_epoch":
        bad["epoch"] = src.epoch + 1
    elif fence == "lapsed_heartbeat":
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :request",
            params,
        )
    elif fence == "cancelled":
        _exec(
            "UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :work_item",
            params,
        )
    elif fence == "grant_for_another_request":
        other = _source(admitted, 9312)
        bad.update(request_id=other.request_id, epoch=other.epoch)
    elif fence == "grant_not_found":
        bad["grant_id"] = uuid.uuid4()
    elif fence == "grant_scope_mismatch":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET repository_id = '999999' "
            "WHERE id = :grant",
            params,
        )
    elif fence == "grant_expired":
        _exec(
            "UPDATE curie.factory_tracker_result_grants "
            "SET expires_at = clock_timestamp() - interval '1 second' WHERE id = :grant",
            params,
        )
    elif fence == "result_kind_not_allowed":
        pass
    elif fence == "tracker_mismatch":
        other_tracker = InMemoryTracker(
            kind=src.tracker.kind, host="tracker.other.example", scope_id=src.tracker.scope_id
        )
        bad["tracker"] = other_tracker
    elif fence == "tracker_operation_unsupported":
        capabilities = all_supported(TRACKER_OPERATIONS)
        capabilities[Operation.CREATE_LINKED_ISSUE] = Support.UNSUPPORTED
        other_tracker = InMemoryTracker(
            kind=src.tracker.kind,
            host=src.tracker.host,
            scope_id=src.tracker.scope_id,
            capabilities=capabilities,
        )
        other_tracker.seed_issue("report", "symptom", issue_id=src.issue.issue_id)
        bad["tracker"] = other_tracker
    elif fence == "closes_source":
        bad["proposal"] = _ticket(body=f"Fixes #{src.issue.issue_id} for good.")
    with pytest.raises(TrackerResultRefused) as refused:
        _submit(src, bad.pop("proposal", proposal), **bad)
    assert refused.value.reason is expected
    assert _deliveries() == 0
    assert _count("factory_tracker_result_artifacts") == 0
    assert src.tracker.ledger.writes == 0
    if other_tracker is not None:
        assert other_tracker.ledger.writes == 0

    # Liveness control: the same setup with the fence satisfied delivers.
    if fence == "source_fingerprint_changed":
        _exec(
            "UPDATE curie.work_items SET trigger_label = :label WHERE id = :work_item",
            {**params, "label": admitted_with["trigger_label"]},
        )
    elif fence == "lapsed_heartbeat":
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() + interval '60 seconds' WHERE id = :request",
            params,
        )
    elif fence == "cancelled":
        _exec("UPDATE curie.work_items SET cancelled_at = NULL WHERE id = :work_item", params)
    elif fence == "grant_scope_mismatch":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET repository_id = :repo "
            "WHERE id = :grant",
            {**params, "repo": str(REPO_ID)},
        )
    elif fence == "grant_expired":
        _exec(
            "UPDATE curie.factory_tracker_result_grants "
            "SET expires_at = clock_timestamp() + interval '1 hour' WHERE id = :grant",
            params,
        )
    elif fence == "result_kind_not_allowed":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET allowed_result_kinds = "
            "'[\"findings\", \"ticket\"]'::jsonb, implementation_label = 'factory-implement' "
            "WHERE id = :grant",
            params,
        )
    delivery_id = _submit(src, proposal)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")


_ATTEMPT_FENCES = [
    "none",
    "source_fingerprint_changed",
    "stale_epoch",
    "lapsed_heartbeat",
    "cancelled",
    "grant_for_another_request",
    "grant_scope_mismatch",
]


@pytest.mark.parametrize("fence", _ATTEMPT_FENCES)
def test_a_fence_failing_at_attempt_time_settles_failed_and_writes_nothing(
    admitted: Any, fence: str
) -> None:
    src = _source(admitted, 9321)
    delivery_id = _submit(src, _ticket())
    params = {"work_item": src.work_item_id, "request": src.request_id, "grant": src.grant_id}
    expected = fence
    if fence == "source_fingerprint_changed":
        _exec(
            "UPDATE curie.work_items SET trigger_label = 'other-trigger' WHERE id = :work_item",
            params,
        )
    elif fence == "stale_epoch":
        expected = AttemptError.REQUEST_SETTLING.value
        _exec(
            "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 "
            "WHERE id = :request",
            params,
        )
    elif fence == "lapsed_heartbeat":
        expected = AttemptError.REQUEST_SETTLING.value
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :request",
            params,
        )
    elif fence == "cancelled":
        _exec(
            "UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :work_item",
            params,
        )
    elif fence == "grant_for_another_request":
        expected = "grant_request_mismatch"
        other = _source(admitted, 9322)
        _exec(
            "DELETE FROM curie.factory_tracker_result_grants WHERE id = :other",
            {"other": other.grant_id},
        )
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET execution_request_id = :other "
            "WHERE id = :grant",
            {**params, "other": other.request_id},
        )
    elif fence == "grant_scope_mismatch":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET repository_id = '999999' "
            "WHERE id = :grant",
            params,
        )
    outcome = _deliver(src, delivery_id)
    if fence == "none":
        assert outcome.state is DeliveryState.DELIVERED
        _assert_delivered_once(src, delivery_id, "ticket")
        return
    if expected == AttemptError.REQUEST_SETTLING:
        # A running request without its live owner is settling: the attempt
        # defers, writes nothing and keeps the delivery owed (#5586).
        assert (outcome.state, outcome.last_error_code) == (DeliveryState.OWED, expected)
        assert src.tracker.ledger.writes == 0
        assert _own_issues(src) == [] and _comments(src) == []
        assert _count("factory_tracker_result_artifacts", delivery_id) == 1
        return
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, expected)
    assert src.tracker.ledger.writes == 0
    assert _own_issues(src) == [] and _comments(src) == []
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    # Failed is final.
    assert _deliver(src, delivery_id).attempted is False


def test_two_owned_issues_with_the_marker_fail_ambiguous_and_create_nothing(
    admitted: Any,
) -> None:
    src = _source(admitted, 9331)
    embedded = src.tracker.embed_linked_marker(output_identity_for(src.request_id))
    for _ in range(2):
        src.tracker.seed_issue("earlier", f"text\n\n{embedded}\n", author=src.tracker.identity)
    delivery_id = _submit(src, _ticket())
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, "output_ambiguous")
    assert len(_own_issues(src)) == 2
    assert src.tracker.ledger.writes == 0 and _comments(src) == []
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


# Criterion 5: read-back and deadline -----------------------------------------------------


def test_an_issue_read_back_mismatch_is_not_delivered_and_then_fails_at_the_deadline(
    admitted: Any,
) -> None:
    src = _source(admitted, 9341)
    delivery_id = _submit(src, _ticket())
    src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    assert _deliver(src, delivery_id).last_error_code == "tracker_unavailable"
    (issue,) = _own_issues(src)
    # A person edits the output issue; the marker stays its last line.
    src.tracker.edit_issue_body(issue, "edited by a person\n\n" + src.tracker.issue_body(issue))
    _make_due(delivery_id)
    mismatch = _deliver(src, delivery_id)
    assert (mismatch.state, mismatch.last_error_code) == (DeliveryState.OWED, "readback_mismatch")
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert len(_own_issues(src)) == 1
    writes = src.tracker.ledger.writes
    _age_deadline(delivery_id)
    failed = _deliver(src, delivery_id)
    assert (failed.state, failed.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert src.tracker.ledger.writes == writes
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_a_comment_read_back_that_misses_is_not_delivered(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = _source(admitted, 9342)
    delivery_id = _submit(src, _findings())
    comments = src.tracker.marked_comments
    real_upsert, real_find = comments.upsert_marked, comments.find_marked
    upserted: list[bool] = []

    async def upsert(target: ReplyTarget, marker: str, body: str) -> Any:
        result = await real_upsert(target, marker, body)
        upserted.append(True)
        return result

    async def find(target: ReplyTarget, marker: str) -> Any:
        # The read-back after the write sees no comment.
        return None if upserted else await real_find(target, marker)

    monkeypatch.setattr(comments, "upsert_marked", upsert)
    monkeypatch.setattr(comments, "find_marked", find)
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.last_error_code) == (DeliveryState.OWED, "readback_mismatch")
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    monkeypatch.setattr(comments, "upsert_marked", real_upsert)
    monkeypatch.setattr(comments, "find_marked", real_find)
    _make_due(delivery_id)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "findings")


def test_past_the_deadline_a_delivery_is_recorded_failed(admitted: Any) -> None:
    attempted = _source(admitted, 9351)
    sixty = get_settings().model_copy(
        update={"factory_tracker_result_delivery_deadline_seconds": 60}
    )
    first = _submit(attempted, _ticket(), settings=sixty)
    row = _delivery(first)
    assert row["deadline_at"] - row["submitted_at"] == timedelta(seconds=60)
    _age_deadline(first)
    outcome = _deliver(attempted, first)
    assert (outcome.state, outcome.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert attempted.tracker.ledger.writes == 0
    assert _count("factory_tracker_result_artifacts", first) == 0

    # The sweep settles an owed row nothing attempted; a row inside its deadline stays owed.
    unattempted = _source(admitted, 9352)
    second = _submit(unattempted, _findings())
    live = _source(admitted, 9353)
    third = _submit(live, _findings())
    _age_deadline(second)
    assert _run(lambda maker: expire_overdue(maker)) == 1
    assert (_delivery(second)["state"], _delivery(second)["failure_code"]) == (
        "failed",
        "delivery_deadline_exceeded",
    )
    assert _count("factory_tracker_result_artifacts", second) == 0
    assert _delivery(third)["state"] == "owed"
    assert _deliver(live, third).state is DeliveryState.DELIVERED


# Criterion 6: storage boundary -------------------------------------------------------


def _tables_holding(sentinels: list[str]) -> dict[str, list[str]]:
    """Every table in the curie schema whose rows, as text, hold a sentinel."""

    async def work(maker: async_sessionmaker[AsyncSession]) -> dict[str, list[str]]:
        async with maker() as session:
            tables = (
                await session.scalars(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'curie' AND table_type = 'BASE TABLE'"
                    )
                )
            ).all()
            assert "factory_tracker_result_deliveries" in tables
            params = {f"s{index}": sentinel for index, sentinel in enumerate(sentinels)}
            probes = ", ".join(
                f"coalesce(bool_or(position(:s{index} in t::text) > 0), false)"
                for index in range(len(sentinels))
            )
            found: dict[str, list[str]] = {}
            for table in tables:
                hits = (
                    await session.execute(text(f'SELECT {probes} FROM curie."{table}" t'), params)
                ).one()
                held = [sentinel for sentinel, hit in zip(sentinels, hits, strict=True) if hit]
                if held:
                    found[str(table)] = held
            return found

    return _run(work)


def test_no_row_and_no_log_line_holds_submitted_text(
    admitted: Any, caplog: pytest.LogCaptureFixture
) -> None:
    # The root logger at the production level (Settings.log_level); no logger is
    # excluded and none is forced lower than production runs it.
    caplog.set_level(get_settings().log_level.upper())
    planted = {
        field: f"planted{field}{uuid.uuid4().hex[:8]}"
        for field in ("title", "body", "command", "observed", "tried", "missing")
    }
    ticket_src = _source(admitted, 9361)
    findings_src = _source(admitted, 9362)
    ticket = _ticket(
        title=f"Title {planted['title']}",
        body=f"Body {planted['body']}",
        reproduction={
            "command": f"run {planted['command']}",
            "observed": f"saw {planted['observed']}",
            "revision": REVISION,
        },
    )
    findings = _findings(tried=f"tried {planted['tried']}", missing=f"lacks {planted['missing']}")
    delivered = _submit(ticket_src, ticket)
    failed = _submit(findings_src, findings)
    # Sealed: the artifact is the one place the text lives.
    assert set(_tables_holding(list(planted.values()))) == {"factory_tracker_result_artifacts"}

    assert _deliver(ticket_src, delivered).state is DeliveryState.DELIVERED
    findings_src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    assert _deliver(findings_src, failed).last_error_code == "tracker_unavailable"
    _age_deadline(failed)
    assert _deliver(findings_src, failed).failure_code == "delivery_deadline_exceeded"

    # The tracker holds the content; the rows and the logs do not.
    (issue,) = _own_issues(ticket_src)
    assert planted["body"] in ticket_src.tracker.issue_body(issue)
    assert planted["command"] in _comments(ticket_src)[0]
    assert planted["observed"] in _comments(ticket_src)[0]
    assert planted["tried"] in _comments(findings_src)[0]
    assert _count("factory_tracker_result_artifacts") == 0
    assert _tables_holding(list(planted.values())) == {}
    logged = caplog.text + "".join(record.getMessage() for record in caplog.records)
    assert any(record.name == "curie_api.factory_tracker_results" for record in caplog.records)
    for value in planted.values():
        assert value not in logged


def test_a_failed_artifact_write_raises_a_code_only_error(
    admitted: Any, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(get_settings().log_level.upper())
    planted = f"plantedtried{uuid.uuid4().hex[:8]}"
    src = _source(admitted, 9371)
    seam = "curie_api.factory_tracker_result_delivery.proposal_content"

    def unstorable(proposal: ResultProposal) -> dict[str, object]:
        # JSONB refuses NUL, so the artifact INSERT fails with its content bound.
        return {**proposal_content(proposal), "tried": f"{planted}\x00"}

    monkeypatch.setattr(seam, unstorable)
    with pytest.raises(TrackerResultStorageError) as failed:
        _submit(src, _findings(tried=f"tried {planted}"))
    error = failed.value
    assert error.code == "artifact_write_failed"
    assert error.__cause__ is None and error.__context__ is None
    assert planted not in f"{error}{error!r}{error.args!r}"
    assert _deliveries() == 0 and _count("factory_tracker_result_artifacts") == 0
    assert src.tracker.ledger.writes == 0
    assert planted not in caplog.text + "".join(r.getMessage() for r in caplog.records)
    # Liveness control: the same proposal stores and delivers without the fault.
    monkeypatch.setattr(seam, proposal_content)
    delivery_id = _submit(src, _findings())
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_valid_proposal_round_trips_through_the_stored_artifact(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9372)
    body = "b" * 60_000
    proposal = _ticket(body=body) if kind == "ticket" else _findings(reason="already_fixed")
    delivery_id = _submit(src, proposal)

    async def stored(maker: async_sessionmaker[AsyncSession]) -> Any:
        async with maker() as session:
            artifact = await session.get(FactoryTrackerResultArtifact, delivery_id)
            assert artifact is not None
            return artifact.content

    content = _run(stored)
    # Strict mode reads the stored JSON strings back (``reason`` included).
    assert parse_proposal(content) == proposal
    assert proposal_sha256(parse_proposal(content)) == proposal_sha256(proposal)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    if kind == "ticket":
        (issue,) = _own_issues(src)
        assert body in src.tracker.issue_body(issue)
    else:
        assert "already_fixed" in _comments(src)[0]


# F1: rechecks immediately before each tracker write ------------------------------


async def _sql_now(statements: list[tuple[str, dict[str, Any]]]) -> None:
    """Run SQL from inside an attempt, between two tracker calls."""

    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.begin() as connection:
            for statement, params in statements:
                await connection.execute(text(statement), params)
    finally:
        await engine.dispose()


def _after(
    monkeypatch: pytest.MonkeyPatch,
    owner: object,
    name: str,
    hook: Callable[[], Awaitable[None]],
) -> None:
    """Run ``hook`` right after ``owner.name`` returns, once."""

    real = getattr(owner, name)
    ran: list[bool] = []

    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        result = await real(*args, **kwargs)
        if not ran:
            ran.append(True)
            await hook()
        return result

    monkeypatch.setattr(owner, name, wrapped)


_AGE_DEADLINE = (
    "UPDATE curie.factory_tracker_result_deliveries "
    "SET submitted_at = clock_timestamp() - interval '2 seconds', "
    "deadline_at = clock_timestamp() - interval '1 second' WHERE id = :delivery"
)


def _crossing(
    condition: str, src: _Source, delivery_id: uuid.UUID, other: _Source | None
) -> tuple[list[tuple[str, dict[str, Any]]], str]:
    """The SQL that crosses ``condition`` and the failure code it must end in."""

    params: dict[str, Any] = {
        "work_item": src.work_item_id,
        "request": src.request_id,
        "grant": src.grant_id,
        "delivery": delivery_id,
    }
    if condition == "source_fingerprint_changed":
        sql = ["UPDATE curie.work_items SET trigger_label = 'other-trigger' WHERE id = :work_item"]
        return [(statement, params) for statement in sql], condition
    if condition == "stale_epoch":
        sql = [
            "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 "
            "WHERE id = :request"
        ]
        return [(statement, params) for statement in sql], AttemptError.REQUEST_SETTLING.value
    if condition == "lapsed_heartbeat":
        sql = [
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :request"
        ]
        return [(statement, params) for statement in sql], AttemptError.REQUEST_SETTLING.value
    if condition == "cancelled":
        sql = ["UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :work_item"]
        return [(statement, params) for statement in sql], condition
    if condition == "grant_scope_mismatch":
        sql = [
            "UPDATE curie.factory_tracker_result_grants SET repository_id = '999999' "
            "WHERE id = :grant"
        ]
        return [(statement, params) for statement in sql], condition
    if condition == "grant_for_another_request":
        assert other is not None
        moved = {**params, "other_grant": other.grant_id, "other_request": other.request_id}
        return [
            ("DELETE FROM curie.factory_tracker_result_grants WHERE id = :other_grant", moved),
            (
                "UPDATE curie.factory_tracker_result_grants "
                "SET execution_request_id = :other_request WHERE id = :grant",
                moved,
            ),
        ], "grant_request_mismatch"
    assert condition == "deadline"
    return [(_AGE_DEADLINE, params)], "delivery_deadline_exceeded"


_BETWEEN_WRITES = [
    "source_fingerprint_changed",
    "stale_epoch",
    "lapsed_heartbeat",
    "cancelled",
    "grant_scope_mismatch",
    "grant_for_another_request",
    "deadline",
]


@pytest.mark.parametrize("condition", _BETWEEN_WRITES)
def test_a_condition_crossed_between_the_issue_and_the_comment_posts_no_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, condition: str
) -> None:
    src = _source(admitted, 9381)
    other = _source(admitted, 9382) if condition == "grant_for_another_request" else None
    delivery_id = _submit(src, _ticket())
    statements, code = _crossing(condition, src, delivery_id, other)

    async def cross() -> None:
        await _sql_now(statements)

    _after(monkeypatch, src.tracker, "create_linked_issue", cross)
    outcome = _deliver(src, delivery_id)
    assert len(_own_issues(src)) == 1
    assert _comments(src) == []
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    if code == AttemptError.REQUEST_SETTLING:
        # Deferred, not failed: the comment waits for the request to settle.
        assert (outcome.state, outcome.last_error_code) == (DeliveryState.OWED, code)
        assert _count("factory_tracker_result_artifacts", delivery_id) == 1
        return
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, code)
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


@pytest.mark.parametrize("condition", ["deadline", "cancelled", "lapsed_heartbeat"])
def test_a_condition_crossed_after_the_comment_is_never_recorded_delivered(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, condition: str
) -> None:
    src = _source(admitted, 9383)
    delivery_id = _submit(src, _findings())
    statements, code = _crossing(condition, src, delivery_id, None)

    async def cross() -> None:
        await _sql_now(statements)

    _after(monkeypatch, src.tracker.marked_comments, "upsert_marked", cross)
    outcome = _deliver(src, delivery_id)
    assert len(_comments(src)) == 1
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    if code == AttemptError.REQUEST_SETTLING:
        assert (outcome.state, outcome.last_error_code) == (DeliveryState.OWED, code)
        assert _count("factory_tracker_result_artifacts", delivery_id) == 1
        return
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, code)
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_the_sweep_fails_an_attempt_in_flight_and_the_late_attempt_records_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = _source(admitted, 9384)
    delivery_id = _submit(src, _ticket())
    swept: list[int] = []

    async def sweep() -> None:
        await _sql_now([(_AGE_DEADLINE, {"delivery": delivery_id})])
        engine = create_async_engine(get_settings().database_url)
        try:
            swept.append(await expire_overdue(async_sessionmaker(engine, expire_on_commit=False)))
        finally:
            await engine.dispose()

    _after(monkeypatch, src.tracker, "create_linked_issue", sweep)
    outcome = _deliver(src, delivery_id)
    # The sweep settled the row although this attempt held its lease.
    assert swept == [1]
    assert (outcome.state, outcome.failure_code, outcome.last_error_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
        None,
    )
    row = _delivery(delivery_id)
    # The late attempt recorded nothing: no output, no lease, one attempt.
    assert (row["output_issue_id"], row["attempt_owner"], row["attempts"]) == (None, None, 1)
    assert _comments(src) == []
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_the_sweep_settles_an_overdue_row_whose_lease_is_still_held(admitted: Any) -> None:
    src = _source(admitted, 9385)
    delivery_id = _submit(src, _findings())
    src.tracker.write_faults.abort_before_next(Operation.UPSERT_MARKED, _Restart())
    with pytest.raises(_Restart):
        _deliver(src, delivery_id, owner="dead-process")
    assert _delivery(delivery_id)["attempt_owner"] == "dead-process"
    _age_deadline(delivery_id)
    assert _run(lambda maker: expire_overdue(maker)) == 1
    row = _delivery(delivery_id)
    assert (row["state"], row["failure_code"], row["attempt_owner"]) == (
        "failed",
        "delivery_deadline_exceeded",
        None,
    )
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0
    assert _deliver(src, delivery_id).attempted is False
    assert _comments(src) == []


# F2: a recorded output is never recreated -------------------------------------------


def test_a_recorded_output_whose_marker_is_gone_is_never_recreated(admitted: Any) -> None:
    src = _source(admitted, 9391)
    delivery_id = _submit(src, _ticket())
    src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    assert _deliver(src, delivery_id).last_error_code == "tracker_unavailable"
    (issue,) = _own_issues(src)
    assert _delivery(delivery_id)["output_issue_id"] == issue.issue_id
    # A person removes the marker line from the output issue.
    src.tracker.edit_issue_body(issue, "rewritten by a person, marker removed")
    _make_due(delivery_id)
    missing = _deliver(src, delivery_id)
    assert (missing.state, missing.last_error_code) == (DeliveryState.OWED, "output_missing")
    assert _own_issues(src) == [issue]
    assert len(src.tracker.sent_issue_bodies) == 1
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    _age_deadline(delivery_id)
    failed = _deliver(src, delivery_id)
    assert (failed.state, failed.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert _own_issues(src) == [issue]


# #5586 criterion 2: the runner-facing routes over HTTP ------------------------------

WORKER_HEADERS = {"X-Curie-Worker-Token": "factory-terminus-worker"}
MINT_URL = "/v1/internal/work-items/issue-read/context"
CONTRACT_URL = "/work-items/completion-contract"
SUBMIT_URL = "/work-items/tracker-result"
FINDINGS: dict[str, Any] = {
    "kind": "findings",
    "reason": "not_reproduced",
    "tried": "Ran the export three times on the sample data.",
    "missing": "A dataset that shows the dropped row.",
    "revision": REVISION,
}
TICKET: dict[str, Any] = {
    "kind": "ticket",
    "title": "Export drops the last row",
    "body": "## Problem\n\nThe export omits the final row.",
    "reproduction": {
        "command": "uv run pytest tests/test_export.py -q",
        "observed": "1 failed: expected 3 rows, got 2",
        "revision": REVISION,
    },
}


@dataclass
class _Triage:
    client: Any
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    tracker: InMemoryTracker
    issue: TrackerIssueRef
    capability: str
    context: Any
    epoch: int = 0


def _declare_policy(work_item_id: uuid.UUID, kinds: tuple[str, ...]) -> None:
    """The agent's deployed version carries a result policy, and admission
    selected the tracker result contract from it."""

    policy = {
        "results": sorted(kinds),
        "implementation_label": "factory-implement" if "ticket" in kinds else None,
    }
    _exec(
        "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by, "
        "result_policy_schema_version, result_policy_digest, result_policy) "
        "SELECT :id, agent_id, 'triage-v1', 'test', 1, :digest, CAST(:policy AS jsonb) "
        "FROM curie.work_items WHERE id = :work_item",
        {
            "id": uuid.uuid4(),
            "digest": POLICY_DIGEST,
            "policy": json.dumps(policy),
            "work_item": work_item_id,
        },
    )
    _exec(
        "UPDATE curie.execution_requests SET completion_contract = 'tracker_result', "
        "result_policy_digest = :digest WHERE work_item_id = :work_item",
        {"digest": POLICY_DIGEST, "work_item": work_item_id},
    )


def _triage(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    number: int,
    *,
    tracker_result: bool = True,
    kinds: tuple[str, ...] = ("findings", "ticket"),
    labels: tuple[str, ...] = (),
) -> _Triage:
    """A labelled issue admitted through the webhook and acquired by a worker,
    with the capability the worker mints for its sandbox. Not started yet."""

    from curie_api import factory_tracker_result_runtime

    client, github, _sink = admitted
    _label(client, github, number)
    (row,) = _rows(
        "SELECT r.id AS request_id, w.id AS work_item_id, w.tracker_kind, w.tracker_host, "
        "w.tracker_scope_id, w.tracker_issue_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )
    if tracker_result:
        _declare_policy(row["work_item_id"], kinds)
    tracker = InMemoryTracker(
        kind=row["tracker_kind"], host=row["tracker_host"], scope_id=row["tracker_scope_id"]
    )
    issue = tracker.seed_issue(
        SOURCE_TITLE, SOURCE_BODY, issue_id=row["tracker_issue_id"], labels=labels
    )
    monkeypatch.setattr(
        factory_tracker_result_runtime,
        "work_item_tracker",
        lambda _settings, _client, _work_item: tracker,
    )
    request_id = row["request_id"]

    async def go() -> Any:
        async with client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            return acquired.context

    context = client.portal.call(go)
    minted = client.post(
        MINT_URL, headers=WORKER_HEADERS, json={"execution_request_id": str(request_id)}
    )
    assert minted.status_code == 200, minted.text
    return _Triage(
        client=client,
        request_id=request_id,
        work_item_id=row["work_item_id"],
        tracker=tracker,
        issue=issue,
        capability=minted.json()["capability"],
        context=context,
    )


def _begin(triage: _Triage) -> None:
    async def go() -> int:
        async with triage.client.app.state.sessionmaker() as session:
            started = await start(
                session,
                triage.request_id,
                owner="factory-owner",
                generation=1,
                claim_name=f"claim-{triage.request_id.hex[:12]}",
                sandbox_name=f"sbx-{triage.request_id.hex[:12]}",
                context=triage.context,
            )
            assert not isinstance(started, DispatchConflict), started
            return int(started.runtime_epoch)

    triage.epoch = int(triage.client.portal.call(go))


def _contract(triage: _Triage) -> Any:
    return triage.client.post(CONTRACT_URL, headers={"X-Curie-Issue-Read": triage.capability})


def _post_result(triage: _Triage, proposal: Any) -> Any:
    return triage.client.post(
        SUBMIT_URL, headers={"X-Curie-Issue-Read": triage.capability}, json={"proposal": proposal}
    )


def _finish(triage: _Triage, outcome: str, cause: str) -> Any:
    from curie_api.workitem_dispatch import finish

    async def go() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            return await finish(
                session,
                triage.request_id,
                runtime_epoch=triage.epoch,
                outcome=outcome,  # type: ignore[arg-type]
                cause=cause,
                detail=None,
                ci_fix_round=None,
            )

    return triage.client.portal.call(go)


def _code_of(response: Any) -> str:
    detail = response.json()["detail"]
    assert isinstance(detail, dict), detail
    return str(detail["code"])


def _request(triage: _Triage) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT status, terminal_cause, model_session_reserved_at, execution_deadline, "
        "execution_attempts FROM curie.execution_requests WHERE id = :id",
        {"id": triage.request_id},
    )
    return dict(row)


def _tracker_result_rows(triage: _Triage) -> tuple[int, int, int]:
    """Grants, deliveries and artifacts the request holds."""

    params = {"id": triage.request_id}
    (grants,) = _rows(
        "SELECT count(*) AS n FROM curie.factory_tracker_result_grants "
        "WHERE execution_request_id = :id",
        params,
    )
    (deliveries,) = _rows(
        "SELECT count(*) AS n FROM curie.factory_tracker_result_deliveries "
        "WHERE execution_request_id = :id",
        params,
    )
    (artifacts,) = _rows(
        "SELECT count(*) AS n FROM curie.factory_tracker_result_artifacts a "
        "JOIN curie.factory_tracker_result_deliveries d ON d.id = a.delivery_id "
        "WHERE d.execution_request_id = :id",
        params,
    )
    return int(grants["n"]), int(deliveries["n"]), int(artifacts["n"])


def _triage_comments(triage: _Triage) -> list[str]:
    target = ReplyTarget.on_issue(triage.issue)
    return [body for _author, body in triage.tracker.marked_comments.comments(target)]


def _running_triage(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> _Triage:
    """Acquired, its session reserved through the route, then started."""

    triage = _triage(admitted, monkeypatch, number)
    reserved = _contract(triage)
    assert reserved.status_code == 200, reserved.text
    _begin(triage)
    return triage


def test_a_submission_is_delivered_once_and_completes_the_request_without_a_pull_request(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _triage(admitted, monkeypatch, 9501)
    # The runner asks at session start, before the worker's start grant.
    contract = _contract(triage)
    assert contract.status_code == 200, contract.text
    assert contract.headers["cache-control"] == "no-store"
    assert contract.json() == {
        "completion_contract": "tracker_result",
        "result_kinds": ["findings", "ticket"],
        "session_usd_allowance": 5.0,
    }
    assert _request(triage)["model_session_reserved_at"] is not None
    _begin(triage)
    deadline = _request(triage)["execution_deadline"]
    (grant,) = _rows(
        "SELECT expires_at, source_content_sha256, output_identity, allowed_result_kinds "
        "FROM curie.factory_tracker_result_grants WHERE execution_request_id = :id",
        {"id": triage.request_id},
    )
    # The start transition bounds the grant by the execution deadline.
    assert grant["expires_at"] == deadline + timedelta(
        seconds=get_settings().factory_tracker_result_delivery_deadline_seconds
    )
    assert grant["source_content_sha256"] == source_content_sha256(
        SourceContent(SOURCE_TITLE, SOURCE_BODY)
    )
    assert grant["output_identity"] == output_identity_for(triage.request_id)
    assert grant["allowed_result_kinds"] == ["findings", "ticket"]

    submitted = _post_result(triage, FINDINGS)
    assert submitted.status_code == 200, submitted.text
    body = submitted.json()
    assert (body["result_kind"], body["state"], body["ticket_url"]) == (
        "findings",
        "delivered",
        None,
    )
    comments = _triage_comments(triage)
    assert len(comments) == 1
    assert result_marker(triage.request_id) in comments[0]
    assert body["result_comment_url"].startswith(triage.tracker.issue_url(triage.issue))

    # The worker finishes the delivered turn; no pull request exists.
    finished = _finish(triage, "completed", "tracker_result")
    assert not isinstance(finished, DispatchConflict), finished
    request = _request(triage)
    assert (request["status"], request["terminal_cause"]) == ("completed", "completed")
    (publications,) = _rows(
        "SELECT count(*) AS n FROM curie.publications WHERE execution_request_id = :id",
        {"id": triage.request_id},
    )
    assert publications["n"] == 0
    assert _own_issues_of(triage) == []


def _own_issues_of(triage: _Triage) -> list[TrackerIssueRef]:
    return triage.tracker.issues(author=triage.tracker.identity)


def test_a_second_submission_is_refused_result_already_submitted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running_triage(admitted, monkeypatch, 9502)
    assert _post_result(triage, FINDINGS).json()["state"] == "delivered"
    for proposal in (FINDINGS, {**FINDINGS, "reason": "already_fixed"}):
        again = _post_result(triage, proposal)
        assert again.status_code == 409, again.text
        assert _code_of(again) == "result_already_submitted"
    assert _tracker_result_rows(triage) == (1, 1, 0)
    assert len(_triage_comments(triage)) == 1


def test_the_contract_route_reserves_exactly_one_session(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running_triage(admitted, monkeypatch, 9503)
    reserved_at = _request(triage)["model_session_reserved_at"]
    # A second runner process for the same request starts no model.
    second = _contract(triage)
    assert second.status_code == 409, second.text
    assert _code_of(second) == "session_already_started"
    assert _request(triage)["model_session_reserved_at"] == reserved_at
    assert _tracker_result_rows(triage)[0] == 1


def test_an_implementation_capability_gets_its_contract_and_cannot_submit(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _triage(admitted, monkeypatch, 9504, tracker_result=False)
    contract = _contract(triage)
    assert contract.status_code == 200, contract.text
    assert contract.json() == {
        "completion_contract": "implementation",
        "result_kinds": [],
        "session_usd_allowance": None,
    }
    # Asking again changes nothing for an implementation request.
    assert _contract(triage).status_code == 200
    _begin(triage)
    for proposal in (FINDINGS, TICKET):
        refused = _post_result(triage, proposal)
        assert refused.status_code == 409, refused.text
        assert _code_of(refused) == "not_tracker_result"
    assert _request(triage)["model_session_reserved_at"] is None
    assert _tracker_result_rows(triage) == (0, 0, 0)
    assert triage.tracker.ledger.writes == 0


def test_a_submission_before_the_session_is_reserved_is_refused(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _triage(admitted, monkeypatch, 9505)
    _begin(triage)
    refused = _post_result(triage, FINDINGS)
    assert refused.status_code == 409, refused.text
    assert _code_of(refused) == "session_not_started"
    assert _tracker_result_rows(triage) == (0, 0, 0)
    # Liveness: once reserved, the same submission is delivered.
    assert _contract(triage).status_code == 200
    assert _post_result(triage, FINDINGS).json()["state"] == "delivered"


@pytest.mark.parametrize("change", ["title", "body", "cancelled", "stale_owner"])
def test_a_changed_source_a_cancelled_work_item_or_a_stale_owner_is_refused(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from curie_api.workitem_dispatch import cancel

    triage = _running_triage(admitted, monkeypatch, 9510)
    params = {"id": triage.request_id, "work_item": triage.work_item_id}
    expected = "source_fingerprint_changed"
    if change == "title":
        triage.tracker.edit_issue_title(triage.issue, "Export drops two rows")
    elif change == "body":
        triage.tracker.edit_issue_body(triage.issue, "Reported symptom, edited.")
    elif change == "cancelled":
        (row,) = _rows("SELECT version FROM curie.work_items WHERE id = :work_item", params)

        async def go() -> Any:
            async with triage.client.app.state.sessionmaker() as session:
                return await cancel(
                    session, work_item_id=triage.work_item_id, expected_version=row["version"]
                )

        triage.client.portal.call(go)
        # Cancelling the WorkItem revoked its unused grant.
        (grant,) = _rows(
            "SELECT expires_at <= clock_timestamp() AS expired "
            "FROM curie.factory_tracker_result_grants WHERE execution_request_id = :id",
            params,
        )
        assert grant["expired"] is True
        expected = "grant_expired"
    else:
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id",
            params,
        )
        expected = "owner_not_live"
    refused = _post_result(triage, FINDINGS)
    assert refused.status_code == 409, refused.text
    assert _code_of(refused) == expected
    assert _tracker_result_rows(triage) == (1, 0, 0)
    assert triage.tracker.ledger.writes == 0
    assert _triage_comments(triage) == []


def test_a_new_comment_on_the_source_changes_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running_triage(admitted, monkeypatch, 9511)
    target = ReplyTarget.on_issue(triage.issue)
    triage.tracker.marked_comments.seed_comment(target, triage.tracker.identity, "status")
    submitted = _post_result(triage, FINDINGS)
    assert submitted.status_code == 200, submitted.text
    assert submitted.json()["state"] == "delivered"


def test_an_invalid_proposal_is_422_with_codes_only_and_writes_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running_triage(admitted, monkeypatch, 9512)
    planted = "plantedinvalidwombat"
    refused = _post_result(triage, {**FINDINGS, "reason": planted, planted: planted})
    assert refused.status_code == 422, refused.text
    detail = refused.json()["detail"]
    assert detail["code"] == "proposal_invalid"
    assert sorted((issue["field"], issue["code"]) for issue in detail["issues"]) == [
        ("*", "unknown_field"),
        ("reason", "value"),
    ]
    assert planted not in refused.text
    assert _tracker_result_rows(triage) == (1, 0, 0)


def test_a_tracker_read_failure_at_the_contract_route_writes_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _triage(admitted, monkeypatch, 9513)
    real = triage.tracker.read_source_content

    async def unavailable(issue: TrackerIssueRef) -> SourceContent:
        raise Unavailable("issue")

    monkeypatch.setattr(triage.tracker, "read_source_content", unavailable)
    failed = _contract(triage)
    assert failed.status_code == 503, failed.text
    assert _code_of(failed) == "tracker_unavailable"
    assert _request(triage)["model_session_reserved_at"] is None
    assert _tracker_result_rows(triage) == (0, 0, 0)
    # Liveness: the read answers again and the reservation goes through.
    monkeypatch.setattr(triage.tracker, "read_source_content", real)
    assert _contract(triage).status_code == 200
    assert _tracker_result_rows(triage)[0] == 1


def test_a_capability_for_another_issue_is_refused_on_both_routes(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_api.issue_read_token import mint, verify_claims

    triage = _triage(admitted, monkeypatch, 9514)
    claims = verify_claims(triage.capability, get_settings().api_key)
    assert claims is not None
    forged = mint(get_settings().api_key, claims.model_copy(update={"tracker_issue_id": "999999"}))
    for url, body in ((CONTRACT_URL, None), (SUBMIT_URL, {"proposal": FINDINGS})):
        refused = triage.client.post(url, headers={"X-Curie-Issue-Read": forged}, json=body)
        assert refused.status_code == 409, refused.text
        assert _code_of(refused) == "invalid_context"
    unauthenticated = triage.client.post(CONTRACT_URL)
    assert unauthenticated.status_code == 401
    assert _request(triage)["model_session_reserved_at"] is None
    assert _tracker_result_rows(triage) == (0, 0, 0)


# Revision 1 item 5: delivery after the session ends --------------------------------


def test_an_owed_delivery_fails_the_request_and_a_later_delivery_keeps_it_failed(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_api.workitem_reconciler import WorkItemReconciler

    triage = _running_triage(admitted, monkeypatch, 9520)
    triage.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    submitted = _post_result(triage, FINDINGS)
    assert submitted.status_code == 200, submitted.text
    assert submitted.json()["state"] == "owed"
    finished = _finish(triage, "completed", "tracker_result")
    assert not isinstance(finished, DispatchConflict), finished
    assert (_request(triage)["status"], _request(triage)["terminal_cause"]) == (
        "failed",
        "tracker_result_undelivered",
    )
    (delivery,) = _rows(
        "SELECT id FROM curie.factory_tracker_result_deliveries WHERE execution_request_id = :id",
        {"id": triage.request_id},
    )
    _make_due(delivery["id"])
    reconciler = WorkItemReconciler(
        triage.client.app.state.sessionmaker, triage.client.app.state.valkey, get_settings()
    )
    triage.client.portal.call(reconciler._deliver_tracker_results)
    assert _delivery(delivery["id"])["state"] == "delivered"
    assert len(_triage_comments(triage)) == 1
    # A later delivery records facts; it never rewrites the execution's history.
    assert (_request(triage)["status"], _request(triage)["terminal_cause"]) == (
        "failed",
        "tracker_result_undelivered",
    )


# Revision 1 item 6: content fingerprint at every attempt --------------------------


@pytest.mark.parametrize("when", ["before_first_attempt", "between_retries"])
def test_a_source_edit_after_submission_fails_the_delivery_and_writes_nothing_more(
    admitted: Any, when: str
) -> None:
    src = _source(admitted, 9530)
    delivery_id = _submit(src, _findings())
    if when == "between_retries":
        src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
        assert _deliver(src, delivery_id).last_error_code == "tracker_unavailable"
        _make_due(delivery_id)
    writes = src.tracker.ledger.writes
    src.tracker.edit_issue_title(src.issue, "Export drops every row")
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (
        DeliveryState.FAILED,
        "source_fingerprint_changed",
    )
    assert src.tracker.ledger.writes == writes
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_a_grant_without_a_content_digest_never_delivers(admitted: Any) -> None:
    src = _source(admitted, 9531)
    delivery_id = _submit(src, _findings())
    _exec(
        "UPDATE curie.factory_tracker_result_grants SET source_content_sha256 = NULL "
        "WHERE id = :grant",
        {"grant": src.grant_id},
    )
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (
        DeliveryState.FAILED,
        "source_fingerprint_changed",
    )
    assert src.tracker.ledger.writes == 0


# Revision 1 item 7: the first submission is atomic --------------------------------


@pytest.mark.parametrize("second", ["identical", "different", "exhaustion"])
def test_racing_submissions_or_exhaustion_seal_exactly_one_delivery(
    admitted: Any, second: str
) -> None:
    from curie_api import factory_tracker_result_runtime as runtime
    from curie_api.factory_tracker_result_delivery import seal_platform_findings

    src = _source(admitted, 9540)
    target = runtime.SubmissionTarget(
        grant_id=src.grant_id,
        source_content_sha256=source_content_sha256(SourceContent(SOURCE_TITLE, SOURCE_BODY)),
    )

    async def work(maker: async_sessionmaker[AsyncSession]) -> list[object]:
        async def submit(proposal: ResultProposal) -> object:
            async with maker() as session:
                try:
                    return await runtime.seal_submission(
                        session,
                        request_id=src.request_id,
                        target=target,
                        proposal=proposal,
                        tracker=src.tracker,
                        settings=get_settings(),
                    )
                except runtime.RuntimeRefused as refused:
                    return refused.code

        async def exhaust() -> object:
            async with maker() as session:
                # The lifecycle holds the WorkItem and request locks when it seals.
                work_item = await session.get(WorkItem, src.work_item_id, with_for_update=True)
                request = await session.get(ExecutionRequest, src.request_id, with_for_update=True)
                assert request is not None and work_item is not None
                sealed = await seal_platform_findings(
                    session,
                    request=request,
                    work_item=work_item,
                    limit="budget",
                    settings=get_settings(),
                )
                await session.commit()
                return sealed

        other = {
            "identical": submit(_findings()),
            "different": submit(_findings(reason="already_fixed")),
            "exhaustion": exhaust(),
        }[second]
        return list(await asyncio.gather(submit(_findings()), other))

    results = _run(work)
    assert _deliveries() == 1
    winners = [result for result in results if isinstance(result, uuid.UUID)]
    assert len(winners) == 1
    assert [result for result in results if not isinstance(result, uuid.UUID)] in (
        ["result_already_submitted"],
        [None],
    )


# Revision 1 item 8: base labels and the related-source line ---------------------------


def test_a_published_ticket_carries_the_source_base_label_and_no_trigger_label(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _triage(admitted, monkeypatch, 9550, labels=("base:release-2", LABEL, "bug"))
    assert _contract(triage).status_code == 200
    _begin(triage)
    submitted = _post_result(triage, TICKET)
    assert submitted.status_code == 200, submitted.text
    body = submitted.json()
    assert body["state"] == "delivered"
    (published,) = _own_issues_of(triage)
    assert body["ticket_url"] == triage.tracker.issue_url(published)
    assert triage.tracker.labels(published) == frozenset({"base:release-2"})
    lines = [line for line in triage.tracker.issue_body(published).splitlines() if line.strip()]
    assert lines[-2].startswith("Related to ")
    (receipt,) = _rows(
        "SELECT output_labels, result_comment_url FROM curie.factory_tracker_result_receipts "
        "WHERE execution_request_id = :id",
        {"id": triage.request_id},
    )
    assert receipt["output_labels"] == ["base:release-2"]
    assert receipt["result_comment_url"] == body["result_comment_url"]


# Revision 2 item 2: attempt-time fence 4 for both origins ----------------------------

_SETTLING = {
    "live_owner": ([], "delivered"),
    "expired_heartbeat": (
        [
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id"
        ],
        "request_settling",
    ),
    "wrong_epoch": (
        ["UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 WHERE id = :id"],
        "request_settling",
    ),
    "deadline_teardown": (
        [
            "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
            "terminal_cause = 'execution_deadline', "
            "cancellation_requested_at = clock_timestamp() WHERE id = :id"
        ],
        "request_settling",
    ),
    "user_cancellation": (
        [
            "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
            "terminal_cause = 'issue_cancelled', "
            "cancellation_requested_at = clock_timestamp() WHERE id = :id"
        ],
        "cancelled",
    ),
    "terminal_expired": (
        [
            "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
            "terminal_cause = 'execution_deadline', "
            "cancellation_requested_at = clock_timestamp() WHERE id = :id",
            "UPDATE curie.execution_requests SET status = 'expired', "
            "terminal_at = clock_timestamp(), termination_observation = 'runtime gone' "
            "WHERE id = :id",
        ],
        "delivered",
    ),
    "terminal_failed": (
        [
            "UPDATE curie.execution_requests SET status = 'failed', "
            "terminal_at = clock_timestamp(), terminal_cause = 'budget_exceeded' "
            "WHERE id = :id"
        ],
        "delivered",
    ),
}


@pytest.mark.parametrize("origin", ["submitted", "platform"])
@pytest.mark.parametrize("condition", list(_SETTLING))
def test_attempt_time_owner_fence_proceeds_defers_or_fails(
    admitted: Any, origin: str, condition: str
) -> None:
    from curie_api.factory_tracker_result_delivery import seal_platform_findings

    src = _source(admitted, 9560)
    if origin == "submitted":
        delivery_id = _submit(src, _findings())
    else:

        async def seal(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID | None:
            async with maker() as session:
                request = await session.get(ExecutionRequest, src.request_id)
                work_item = await session.get(WorkItem, src.work_item_id)
                assert request is not None and work_item is not None
                sealed = await seal_platform_findings(
                    session,
                    request=request,
                    work_item=work_item,
                    limit="execution_deadline",
                    settings=get_settings(),
                )
                await session.commit()
                return sealed

        sealed_id = _run(seal)
        assert sealed_id is not None
        delivery_id = sealed_id
    statements, expected = _SETTLING[condition]
    for statement in statements:
        _exec(statement, {"id": src.request_id})
    outcome = _deliver(src, delivery_id)
    if expected == "delivered":
        assert outcome.state is DeliveryState.DELIVERED
        assert len(_comments(src)) == 1
        return
    assert src.tracker.ledger.writes == 0 and _comments(src) == []
    if expected == "request_settling":
        assert (outcome.state, outcome.last_error_code) == (DeliveryState.OWED, expected)
        assert _count("factory_tracker_result_artifacts", delivery_id) == 1
    else:
        assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, expected)
        assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_an_implementation_request_never_delivers_even_with_a_planted_grant(
    admitted: Any,
) -> None:
    src = _source(admitted, 9561)
    delivery_id = _submit(src, _findings())
    _exec(
        "UPDATE curie.execution_requests SET completion_contract = 'implementation', "
        "result_policy_digest = NULL, model_session_reserved_at = NULL WHERE id = :id",
        {"id": src.request_id},
    )
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, "not_tracker_result")
    assert src.tracker.ledger.writes == 0
