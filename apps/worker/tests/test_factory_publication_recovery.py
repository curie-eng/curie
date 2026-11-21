"""A failed factory publication is recovered from its retained candidate (#5429).

Boundary. The recovery request is driven through the real kernel entry point
for a WorkItem execute wake: the real reconciler pass publishes the wake, the
real stream consumer delivers it, and the kernel acquires and starts the
request through the real served API, claims a sandbox on the managed
workspace, and boots the production runner app (``curie_runner.server``)
over a scripted model. The candidate is read back through the real
``CandidateReader`` from an owned bucket and the publication is created
through the real ``ApprovalClient`` against the served API. Only the model,
the code host, the tracker surface and the Kubernetes control plane are
doubles (``apps/worker/tests/factory_retry_fixtures.py``).

The predecessor is the factory's own failed publication: it is acquired and
started through the dispatch routes, its workspace snapshot is read from the
production runner snapshot route over the claim's real base archive, the real
capture retains it, and it finishes ``approval_create_failed`` with a clean
safety fact exactly as the kernel's publication pause reports a create it
could not complete. The API then admits the recovery successor itself.

Real services come from the environment the factory retry harness reads
(Postgres, Valkey and S3); a missing prerequisite skips locally and fails
under CI there. Every owned resource (database, bucket, stream keys, runner
servers, scratch and checkout directories) is removed and its absence
asserted by the harness and by this module.

Injected faults sit outside the changed code: an httpx transport that loses
the first publication create response after the API committed it, and SQL
that moves the WorkItem's base or expires the candidate after the recovery
was admitted.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import shutil
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from aiohttp.test_utils import TestServer
from channel_protocol.work_item_events import execute_event_id
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import RunTracer, SideEffectClassifier, workspace_snapshot
from curie_runner.completion_contract import BoundContract, bind_completion_contract
from curie_runner.fake import FakeModelSession
from curie_runner.server import bind_status_attestation
from curie_runner.server import create_app as create_runner_app
from curie_runner.session import SessionRunner
from curie_worker.approvals import ApprovalClient
from curie_worker.binding import FactoryWake
from curie_worker.kernel.publication_recovery import RECOVERY_REFUSED_CAUSE
from curie_worker.publication_candidate_capture import PublicationCandidateCapture
from curie_worker.publication_candidate_storage import (
    CandidateObjectStore,
    CandidateReader,
    CandidateWriter,
)
from curie_worker.publication_candidates import PublicationCandidateClient
from curie_worker.runner_client import RunnerClient, RunnerWorkspaceSnapshot
from curie_worker.workitem_dispatch import WorkItemRun

from apps.worker.tests import factory_retry_fixtures
from apps.worker.tests.factory_retry_fixtures import (
    UNOBSERVED_LIMITS,
    FactoryRetryCase,
    Item,
    factory_retry,
    factory_retry_database,
)
from apps.worker.tests.start_wait_fixtures import ADDRESS, OPERATOR_KEY, REPOSITORY, WORKER_TOKEN
from apps.worker.tests.test_workspace_recovery_storage import _extract

__all__ = ["factory_retry", "factory_retry_database"]

TITLE = "Convert kelvin in the unit converter"
BODY = "Adds kelvin conversion and documents it."
SNAPSHOT_TOKEN = "recovery-snapshot-runner-token"
CANDIDATE_CAP = 1 << 30
BASE_ARCHIVE_LIMIT = 64 << 20
ADDED_PATH = "unitconv/kelvin.py"
OTHER_BASE = "0123456789abcdef0123456789abcdef01234567"
PUBLICATION_ROUTE = "/v1/internal/publications"
_SETTLED = ("status", "terminal_at", "terminal_cause", "version", "retry_safety")


@pytest.fixture
def candidate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """Publication candidates on for the served API, and a worker scratch root.

    Requested before ``factory_retry`` so the API and the kernel config read it.
    """

    scratch = tmp_path / "worker-scratch"
    scratch.mkdir()
    for key, value in {
        "CURIE_PUBLICATION_CANDIDATES_ENABLED": "true",
        "CURIE_PUBLICATION_CANDIDATE_MAX_BYTES": str(CANDIDATE_CAP),
        "S3_ENDPOINT_URL": os.environ.get("TEST_S3_ENDPOINT_URL")
        or os.environ.get("S3_ENDPOINT_URL", "http://127.0.0.1:29000"),
        "CURIE_WORKSPACE_SCRATCH_ROOT": str(scratch),
        # The capture declares this limit; the API refuses any other.
        "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": str(BASE_ARCHIVE_LIMIT),
    }.items():
        monkeypatch.setenv(key, value)
    yield scratch
    # Every candidate read and validation cleans its private scratch.
    assert list(scratch.iterdir()) == []
    shutil.rmtree(scratch)
    assert not scratch.exists()


class _LoseFirstPublication(httpx.AsyncBaseTransport):
    """Loses the first successful publication create response after the API committed it."""

    def __init__(self) -> None:
        self._inner = httpx.AsyncHTTPTransport()
        self.creates = 0
        self.dropped = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        if request.method == "POST" and request.url.path == PUBLICATION_ROUTE:
            self.creates += 1
            if self.dropped == 0 and response.status_code in (200, 201):
                await response.aread()
                self.dropped += 1
                raise httpx.ReadError("injected loss of the create response", request=request)
        return response

    async def aclose(self) -> None:
        await self._inner.aclose()


@dataclass
class _Retained:
    """A failed publication whose candidate is retained and handed to its recovery."""

    item: Item
    predecessor_epoch: int
    settled: dict[str, Any]
    snapshot: RunnerWorkspaceSnapshot
    base_sha: str
    base_checkout: Path
    candidate_id: uuid.UUID
    successor_id: uuid.UUID


class _Owned:
    """Clients and servers one test opens; closed in reverse."""

    def __init__(self) -> None:
        self.clients: list[httpx.AsyncClient] = []
        self.transports: list[httpx.AsyncBaseTransport] = []
        self.servers: list[TestServer] = []
        self.runner_clients: list[RunnerClient] = []
        self.paths: list[Path] = []

    async def close(self) -> None:
        for runner_client in self.runner_clients:
            await runner_client.close()
        for server in self.servers:
            await server.close()
        for client in self.clients:
            await client.aclose()
        for transport in self.transports:
            await transport.aclose()
        for path in self.paths:
            shutil.rmtree(path, ignore_errors=True)
            assert not path.exists(), f"owned directory {path} survived"


def _wire(case: FactoryRetryCase, owned: _Owned, *, lose_first_create: bool) -> Any:
    """Give the case's kernel the candidate reader and, optionally, a lossy creator."""

    reader_transport = httpx.AsyncHTTPTransport()
    owned.transports.append(reader_transport)
    case.h.kernel._publication_candidate_reader = CandidateReader(
        ledger=PublicationCandidateClient(
            api_base_url=case.api_url, worker_token=WORKER_TOKEN, client=case.http
        ),
        store=CandidateObjectStore(client=case.s3, bucket=case.bucket),
        transport=reader_transport,
        reference_ttl_seconds=60,
    )
    if not lose_first_create:
        return None
    lossy = _LoseFirstPublication()
    client = httpx.AsyncClient(base_url=case.api_url, transport=lossy)
    owned.clients.append(client)
    case.h.kernel._publication_creator = ApprovalClient(
        api_base_url=case.api_url,
        api_key=OPERATOR_KEY,
        client=client,
        worker_token=WORKER_TOKEN,
        read_timeout_s=5,
    )
    return lossy


async def _snapshot(
    owned: _Owned, root: Path, archive: bytes, base_sha: str, origin: str
) -> RunnerWorkspaceSnapshot:
    """The production runner snapshot of the agent's edits over the claim base."""

    checkout = root / "checkout"
    owned.paths.append(checkout)
    _extract(archive, checkout)
    (checkout / "README.md").write_text("# unitconv\n\nConverts celsius, fahrenheit and kelvin.\n")
    (checkout / ADDED_PATH).write_text(
        "def to_kelvin(celsius: float) -> float:\n    return celsius + 273.15\n"
    )
    session = FakeModelSession(script_factory=lambda: [])
    runner = bind_status_attestation(
        SessionRunner(
            max_usd_per_day=None,
            held_secrets=frozenset(),
            session_factory=lambda: session,
            ceiling=10_000,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="publication-recovery-predecessor",
        ),
        session_id="session-acme-recovery",
        sandbox_id="sandbox-acme-recovery",
        cwd=str(checkout),
    )
    server = TestServer(
        create_runner_app(
            runner,
            token=SNAPSHOT_TOKEN,
            snapshotter=workspace_snapshot.workspace_snapshotter(
                checkout,
                trusted_origin=origin,
                repository_path=REPOSITORY,
                proposal=lambda: (TITLE, BODY),
            ),
        )
    )
    await server.start_server()
    owned.servers.append(server)
    runner_client = RunnerClient(total_timeout_s=60.0)
    owned.runner_clients.append(runner_client)
    snapshot = await runner_client.snapshot(
        f"http://127.0.0.1:{server.port}", token=SNAPSHOT_TOKEN, base_sha=base_sha
    )
    assert snapshot.base_sha == base_sha
    assert set(snapshot.changed_paths) == {"README.md", ADDED_PATH}
    return snapshot


async def _retained_failure(case: FactoryRetryCase, owned: _Owned, root: Path) -> _Retained:
    """The predecessor: started, captured, finished as a lost publication create."""

    # The case's own admitted WorkItem. Its request is due from admission, so
    # any other WorkItem would leave it for the first reconciler pass to wake
    # into a model turn on the runner meant for the recovery.
    item = case.item
    claimed = await asyncio.to_thread(
        case.workspace.claim_or_resume_with_handle,
        thread_key=item.thread,
        deployment_id=case.deployment_id,
        env={},
    )
    prepared = claimed.prepared
    # The trusted base the factory resolved at admission (ADR 0186).
    await case.execute(
        "UPDATE curie.work_items SET base_commit = :base, base_branch = 'main', "
        "base_source = 'default' WHERE id = :id",
        {"base": prepared.base_sha, "id": item.work_item_id},
    )
    generation = int((await case.row(item.request_id))["dispatch_generation"])
    owner = case.h.config.consumer_name
    wake = FactoryWake(
        kind="slack", address=ADDRESS, adapter="default", conversation_id=item.conversation
    )
    grant = await case.dispatch.acquire(
        item.request_id, owner=owner, generation=generation, wake=wake, context=None
    )
    assert (grant.execution_kind, grant.recovery_candidate_id) == ("implementation", None)
    run = WorkItemRun(
        client=case.dispatch,
        request_id=item.request_id,
        owner=owner,
        grant=grant,
        wake=wake,
        event_id=execute_event_id(item.request_id, generation),
        thread_key=item.thread,
        on_stop=case.h.kernel._stop_owned_work_item,
        on_stale=case.h.kernel._abandon_stale_work_item,
    )
    assert not run.is_publication_recovery
    await run.start(
        claim_name=claimed.handle.claim_name,
        sandbox_name=claimed.handle.sandbox_name,
        delivered_limits=UNOBSERVED_LIMITS,
    )
    assert run.runtime_epoch is not None
    archive = b"".join(case.objects.get_stream(prepared.object_key))
    snapshot = await _snapshot(owned, root, archive, prepared.base_sha, prepared.repo_origin)
    # The trusted base the recovery runtime mounts; its boot preflight runs here.
    base_checkout = root / "base-checkout"
    owned.paths.append(base_checkout)
    _extract(archive, base_checkout)

    ledger = PublicationCandidateClient(
        api_base_url=case.api_url, worker_token=WORKER_TOKEN, client=case.http
    )
    owned.paths.append(root / "candidate-scratch")
    captured = await PublicationCandidateCapture(
        ledger=ledger,
        writer=CandidateWriter(
            ledger=ledger,
            store=CandidateObjectStore(client=case.s3, bucket=case.bucket),
            scratch_root=root / "candidate-scratch",
            base_archive_limit_bytes=BASE_ARCHIVE_LIMIT,
        ),
        coordinator=case.workspace,
        budget_seconds=60.0,
    ).capture(
        run, snapshot, prepared, dedupe_key=f"capture-{uuid.uuid4().hex}", budget_seconds=60.0
    )
    assert captured.status == "complete" and captured.candidate_id is not None, captured

    # The kernel's publication pause after a create it could not complete.
    await case.h.kernel._finish_or_settle(
        run,
        outcome="failed",
        cause="approval_create_failed",
        detail="publication create failed: injected",
        ci_fix_round=None,
        retry_safety="no_effects",
        retry_condition=None,
        exhausted_volume=None,
    )
    assert run.finished
    # The delivery ``finally`` of kernel/lifecycle.py for this run.
    await run.close()
    await case.h.kernel._release_work_item_sandbox(run)

    predecessor = await case.row(item.request_id)
    successor = await case.successor(item.request_id)
    assert successor is not None and successor["status"] == "waiting", successor
    assert successor["execution_kind"] == "publication_recovery"
    assert successor["recovery_candidate_id"] == captured.candidate_id
    assert await case.used(item.work_item_id) == 1
    return _Retained(
        item=item,
        predecessor_epoch=run.runtime_epoch,
        settled={key: predecessor[key] for key in _SETTLED},
        snapshot=snapshot,
        base_sha=prepared.base_sha,
        base_checkout=base_checkout,
        candidate_id=captured.candidate_id,
        successor_id=successor["id"],
    )


async def _publications(case: FactoryRetryCase, request_id: uuid.UUID) -> list[dict[str, Any]]:
    return await case.rows(
        "SELECT id, approval_id, status, base_sha, patch_bytes, changed_paths, title, body "
        "FROM curie.publications WHERE execution_request_id = :id",
        {"id": request_id},
    )


async def _drop_candidates(case: FactoryRetryCase) -> None:
    """Delete the candidate rows of this case's WorkItems, children first.

    The candidate ledger restricts deleting a WorkItem it still references, so
    the case's agent teardown can only cascade once these owned rows are gone.
    The bucket and its objects belong to the case and go with it.
    """

    owned = {"ids": [item.work_item_id for item in case.items]}
    for table in (
        "publication_candidate_readers",
        "publication_candidate_objects",
        "publication_candidates",
        "publication_candidate_groups",
    ):
        await case.execute(
            f"DELETE FROM curie.{table} WHERE work_item_id = ANY(:ids)",
            owned,
        )
    for table in ("publication_candidate_groups", "publication_candidate_objects"):
        assert (
            await case.rows(
                f"SELECT 1 FROM curie.{table} WHERE work_item_id = ANY(:ids)",
                owned,
            )
            == []
        )


def _no_finish(case: FactoryRetryCase, request_id: uuid.UUID) -> bool:
    return not [call for call in case.calls_to("finish", request_id) if call.status == 200]


# -- a, b, c: the retained candidate is published once, with no model turn ------------------


def test_recovery_publishes_the_retained_candidate_once_without_a_model_turn(
    candidate_env: Path, factory_retry: FactoryRetryCase, tmp_path: Path
) -> None:
    case = factory_retry

    async def exercise() -> None:
        owned = _Owned()
        try:
            retained = await _retained_failure(case, owned, tmp_path)
            lossy = _wire(case, owned, lose_first_create=True)
            (runner,) = await case.real_runners(1, preflight_workspace=retained.base_checkout)

            await case.publish_wake(retained.successor_id)
            # The delivery acks only after the publication pause created it.
            await case.settle()

            (publication,) = await _publications(case, retained.successor_id)
            # The exact retained bytes, against the fresh trusted base.
            assert bytes(publication["patch_bytes"]) == retained.snapshot.patch
            assert sorted(publication["changed_paths"]) == sorted(retained.snapshot.changed_paths)
            assert publication["base_sha"] == retained.base_sha
            # The retained proposal; the API appends its verification disclosure
            # for a Python change exactly as on any other publication.
            body, separator, disclosure = publication["body"].partition("\n\n")
            assert (publication["title"], body, separator) == (TITLE, BODY, "\n\n")
            assert "in-sandbox verification" in disclosure, disclosure
            # The lost create response was retried under the same dedupe key.
            assert (lossy.dropped, lossy.creates) == (1, 2)
            approvals = await case.rows(
                "SELECT id FROM curie.approvals WHERE id = :id", {"id": publication["approval_id"]}
            )
            assert len(approvals) == 1

            # The recovery booted its runner and never opened a turn.
            assert runner.turns == 0
            assert "/v1/status" in runner.paths
            assert case.h.runner.opened == []
            started = [
                call for call in case.calls_to("start", retained.successor_id) if call.status == 200
            ]
            assert len(started) == 1
            row = await case.row(retained.successor_id)
            assert row["status"] == "running" and row["runtime_epoch"] >= 1, row
            assert row["runtime_claim_name"] is not None
            assert _no_finish(case, retained.successor_id)
            # One allowance slot; the terminal predecessor is never rewritten.
            assert await case.used(retained.item.work_item_id) == 1
            predecessor = await case.row(retained.item.request_id)
            assert {key: predecessor[key] for key in _SETTLED} == retained.settled
            assert len(await case.requests(retained.item.work_item_id)) == 2
        finally:
            await owned.close()
            await _drop_candidates(case)

    case.run(exercise())


# -- d: a refused recovery is terminal, names its code, and publishes nothing ---------------


@pytest.mark.parametrize(
    ("factory_retry", "variant", "code"),
    [
        ({}, "base_changed", "recovery_base_changed"),
        ({}, "candidate_expired", "recovery_candidate_unavailable"),
        ({"publication_protected_paths": (ADDED_PATH,)}, "protected_path", "validation_refused"),
    ],
    indirect=["factory_retry"],
)
def test_a_refused_recovery_finishes_terminal_with_its_code(
    candidate_env: Path,
    factory_retry: FactoryRetryCase,
    tmp_path: Path,
    variant: str,
    code: str,
) -> None:
    case = factory_retry

    async def exercise() -> None:
        owned = _Owned()
        try:
            retained = await _retained_failure(case, owned, tmp_path)
            _wire(case, owned, lose_first_create=False)
            (runner,) = await case.real_runners(1, preflight_workspace=retained.base_checkout)
            if variant == "base_changed":
                await case.execute(
                    "UPDATE curie.work_items SET base_commit = :base WHERE id = :id",
                    {"base": OTHER_BASE, "id": retained.item.work_item_id},
                )
            elif variant == "candidate_expired":
                await case.execute(
                    "UPDATE curie.publication_candidates "
                    "SET expires_at = clock_timestamp() - interval '1 second' WHERE id = :id",
                    {"id": retained.candidate_id},
                )

            await case.publish_wake(retained.successor_id)
            finish = await case.finished(retained.successor_id)
            await case.settle()

            assert finish.facts == {
                "cause": RECOVERY_REFUSED_CAUSE,
                "retry_safety": "no_effects",
                "retry_condition": None,
            }
            assert finish.body["detail"] == code
            row = await case.row(retained.successor_id)
            assert (row["status"], row["terminal_cause"]) == ("failed", RECOVERY_REFUSED_CAUSE)
            assert await _publications(case, retained.successor_id) == []
            assert runner.turns == 0
            assert case.h.runner.opened == []
            # Terminal: no further successor and no further allowance spent.
            assert await case.successor(retained.successor_id) is None
            assert await case.used(retained.item.work_item_id) == 1
            predecessor = await case.row(retained.item.request_id)
            assert {key: predecessor[key] for key in _SETTLED} == retained.settled
        finally:
            await owned.close()
            await _drop_candidates(case)

    case.run(exercise())


# -- e: a CI fix on a recovery request is an ordinary bounded workflow turn ----------------


def _checks_fixed() -> list[Any]:
    """A model turn that ends cleanly, as the CI fix runner's model answers it."""

    return [
        AssistantMessage(content=[TextBlock(text="The checks are fixed.")], model="fake-model"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="fake-session",
            result="The checks are fixed.",
            usage=None,
        ),
    ]


def test_a_ci_fix_on_a_recovery_request_starts_the_model_with_the_ordinary_contract(
    candidate_env: Path,
    factory_retry: FactoryRetryCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADR 0269 point 5: only the recovery execute wake is model free.

    The recovery publishes its candidate with no model turn; a CI fix round on
    the same running request then cold-claims a fresh sandbox, whose runner
    reads the ordinary implementation contract bound to that request and runs
    its model.
    """

    case = factory_retry
    mints: list[tuple[uuid.UUID, bool]] = []
    contracts: list[BoundContract] = []
    work_items = case.h.kernel._work_items
    assert work_items is not None
    mint = work_items.issue_read_context

    async def recorded_mint(request_id: uuid.UUID, *, publication_recovery: bool) -> Any:
        mints.append((request_id, publication_recovery))
        return await mint(request_id, publication_recovery=publication_recovery)

    async def recorded_bind(issue_read: Any) -> BoundContract:
        bound = await bind_completion_contract(issue_read)
        contracts.append(bound)
        return bound

    monkeypatch.setattr(work_items, "issue_read_context", recorded_mint)
    monkeypatch.setattr(factory_retry_fixtures, "bind_completion_contract", recorded_bind)

    async def exercise() -> None:
        owned = _Owned()
        try:
            retained = await _retained_failure(case, owned, tmp_path)
            _wire(case, owned, lose_first_create=False)
            recovering, fixing = await case.real_runners(
                2, script=_checks_fixed, preflight_workspace=retained.base_checkout
            )

            await case.publish_wake(retained.successor_id)
            await case.settle()
            (publication,) = await _publications(case, retained.successor_id)
            assert recovering.turns == 0
            assert mints == [(retained.successor_id, True)]
            (recovery,) = contracts
            assert recovery.contract.publication_recovery
            assert recovery.contract.execution is None

            # The publication's outcome reached durable history, as after its
            # approval and Job; this harness runs neither, so SQL stands in.
            await case.execute(
                "UPDATE curie.publications SET status = 'succeeded', "
                "outcome_history_ready_at = clock_timestamp() WHERE id = :id",
                {"id": publication["id"]},
            )
            recovered = dataclasses.replace(retained.item, request_id=retained.successor_id)
            case.enqueue(case.ci_fix_turn(recovered, round_=2))
            await case.finished(retained.successor_id)
            await case.settle()

            # The CI fix minted an ordinary capability for the same request,
            # its runner read the bounded workflow contract bound to that
            # request, and the model ran.
            assert mints == [(retained.successor_id, True), (retained.successor_id, False)]
            _, ci_fix = contracts
            assert ci_fix.contract.kind == "implementation"
            assert ci_fix.contract.execution is not None
            assert ci_fix.contract.execution.execution_request_id == str(retained.successor_id)
            assert fixing.turns >= 1
            assert recovering.turns == 0
            (published,) = await _publications(case, retained.successor_id)
            assert published["id"] == publication["id"]
        finally:
            await owned.close()
            await _drop_candidates(case)

    case.run(exercise())
