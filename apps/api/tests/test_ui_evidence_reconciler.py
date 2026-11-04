"""The API half of UI evidence: upload routes, storage and the reconciler (AC2, AC3).

Every case drives the real worker-token routes, the real Postgres rows, the
real `UiEvidenceReconciler` and the real GitHub code host adapter. Only GitHub
is faked: `GitHubRepositoryFake` replays recorded GitHub shapes, and its
comments, refs and raw route are the observations. The publication path the
evidence rides on is the real one too (create, approve, the lineage PATCH),
and every case proves the code pull request result is exactly what it was
before any evidence work ran.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import struct
import sys
import uuid
import zlib
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.forges.capabilities import Operation, Support
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.ui_evidence_comment import MAX_COMMENT_CHARS, render_evidence, ui_evidence_marker
from curie_api.ui_evidence_reconciler import UiEvidenceReconciler
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (
    APP_ID,
    PRIVATE_KEY,
    REPO,
    GitHubRepositoryFake,
    fake_installation_tokens,
)
from test_publications import (  # noqa: F401 - publication_stack is a fixture
    BASE_SHA,
    WORKER_HEADERS,
    _claim_publication_lease,
    _create_deployment,
    _create_publication,
    _execute,
    _mark_outcome_history_ready,
    _patch_lineage,
    _publication_payload,
    _resolve,
    _rows,
    publication_stack,
)
from test_ui_evidence_corpus import corpus_cases, load_corpus, resolve_index, stand_in_checkout

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
# The candidate tree C every pushed head carries unless a case says otherwise,
# and two other object ids that are neither P nor C.
CANDIDATE_TREE = "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00"
OTHER_OID = "0be70be70be70be70be70be70be70be70be70be7"
BASE_TREE = "ba5e0000ba5e0000ba5e0000ba5e0000ba5e0000"
_IMAGE_URL = re.compile(
    r"!\[[^\]]*\]\((https://github\.com/"
    + re.escape(REPO)
    + r"/raw/([0-9a-f]{40})/(before|after)/(\d{4})\.png)\)"
)


def png_bytes(seed: str, *, width: int = 2, height: int = 2) -> bytes:
    """A valid PNG whose pixels depend on ``seed``, so distinct seeds differ."""

    rgb = hashlib.sha256(seed.encode()).digest()[:3]
    raw = b"".join(b"\x00" + rgb * width for _ in range(height))

    def chunk(kind: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(kind + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", crc)

    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return (
        PNG_SIGNATURE
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


# --- the stack ------------------------------------------------------------------


@dataclass
class Stack:
    client: TestClient
    fake: GitHubRepositoryFake
    http: httpx.AsyncClient
    # One deployment per test: every thread is a new conversation on it, as the
    # publication suite does (a second agent on the same channel is refused).
    deployment: dict[str, Any] | None = None


@pytest.fixture
def _evidence_env(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    # Zero disables the background loop, so each test drives `run_once` itself.
    monkeypatch.setenv("UI_EVIDENCE_RECONCILE_INTERVAL_S", "0")
    # The GitHub App the fake authors every comment as (`CONTRACT_APP_ID`), so
    # marked-comment ownership finds the evidence comment. Parametrized False,
    # the install has no App id and only the operator token.
    if getattr(request, "param", True):
        monkeypatch.setenv("GITHUB_APP_ID", APP_ID)
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", PRIVATE_KEY)
        fake_installation_tokens(monkeypatch)
    else:
        monkeypatch.setenv("GITHUB_APP_ID", "")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "")


@pytest.fixture
def stack(
    _evidence_env: None,
    publication_stack: tuple[TestClient, str],  # noqa: F811
    clean_db: None,
) -> Iterator[Stack]:
    client, _ = publication_stack
    fake = GitHubRepositoryFake()
    # P is the base every first publication names; it is the base branch tip.
    fake.seed_commit(BASE_SHA, parents=[], message="Base", tree=BASE_TREE)
    fake.branches[fake.default_branch] = BASE_SHA
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handle))
    # Every GitHub read the API makes goes to the fake, never the network.
    original = client.app.state.http_client
    client.app.state.http_client = http
    try:
        yield Stack(client, fake, http)
    finally:
        client.app.state.http_client = original
        asyncio.run(http.aclose())


@contextmanager
def app_github(client: TestClient, http: httpx.AsyncClient) -> Iterator[None]:
    """The API's own GitHub client is ``http`` for the duration."""

    original = client.app.state.http_client
    client.app.state.http_client = http
    try:
        yield
    finally:
        client.app.state.http_client = original


def reconciler(
    sessionmaker: async_sessionmaker[Any], http: httpx.AsyncClient
) -> UiEvidenceReconciler:
    """One reconciler instance: one process, one lease owner."""

    return UiEvidenceReconciler(sessionmaker, get_settings(), http, interval_seconds=10)


def reconcile(http: httpx.AsyncClient, *, passes: int = 1) -> None:
    """Run ``passes`` passes of one fresh reconciler instance."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            instance = reconciler(async_sessionmaker(engine, expire_on_commit=False), http)
            for _ in range(passes):
                await instance.run_once()
        finally:
            await engine.dispose()

    asyncio.run(run())


# --- the publication path -----------------------------------------------------


@dataclass
class Revision:
    publication: dict[str, Any]
    deployment: dict[str, Any]
    conversation_id: str
    images: dict[int, bytes] = field(default_factory=dict)
    head_sha: str = ""
    pr_number: int = 0
    pr_url: str = ""

    @property
    def id(self) -> str:
        return str(self.publication["id"])

    @property
    def lineage_id(self) -> uuid.UUID:
        return uuid.UUID(str(self.publication["lineage_id"]))

    @property
    def branch(self) -> str:
        return str(self.publication["branch"])


def create_revision(
    stack: Stack,
    auth_headers: dict[str, str],
    *,
    previous: Revision | None = None,
) -> Revision:
    """A pending publication: the first of a new thread, or the next on ``previous``'s."""

    if stack.deployment is None:
        stack.deployment = _create_deployment(stack.client, auth_headers)
    deployment = stack.deployment
    conversation_id = previous.conversation_id if previous else f"evidence-{uuid.uuid4().hex[:8]}"
    payload = _publication_payload(
        deployment["id"],
        conversation_id=conversation_id,
        base_sha=previous.head_sha if previous else BASE_SHA,
        dedupe_key=f"{conversation_id}-{uuid.uuid4().hex[:6]}",
    )
    status, publication = _create_publication(stack.client, payload)
    assert status == 201, publication
    return Revision(publication, deployment, conversation_id)


def put_index(client: TestClient, publication_id: str, index: Mapping[str, Any]) -> httpx.Response:
    return client.put(
        f"/v1/internal/publications/{publication_id}/ui-evidence",
        json=dict(index),
        headers=WORKER_HEADERS,
    )


def put_image(
    client: TestClient,
    publication_id: str,
    ordinal: int,
    content: bytes,
    *,
    digest: str | None = None,
) -> httpx.Response:
    return client.put(
        f"/v1/internal/publications/{publication_id}/ui-evidence/images/{ordinal}",
        content=content,
        headers={
            **WORKER_HEADERS,
            "Content-Type": "image/png",
            "X-Curie-Evidence-Sha256": digest if digest is not None else sha256(content),
        },
    )


def seal(client: TestClient, publication_id: str) -> httpx.Response:
    return client.post(
        f"/v1/internal/publications/{publication_id}/ui-evidence/seal",
        json={},
        headers=WORKER_HEADERS,
    )


def code(response: httpx.Response) -> str | None:
    """The refusal code of an API error body, wherever FastAPI nests it."""

    body = response.json()
    detail = body.get("detail", body) if isinstance(body, dict) else body
    if isinstance(detail, dict):
        return detail.get("code")
    return None


def image_entry(
    ordinal: int,
    state: str,
    content: bytes | None,
    *,
    label: str = "Home page",
    path: str | None = None,
    refusal: str | None = None,
    bound: bool = True,
) -> dict[str, Any]:
    return {
        "ordinal": ordinal,
        "path": path or f"{state}/home.png",
        "label": label,
        "state": state,
        "size": len(content) if content is not None else None,
        "sha256": sha256(content) if content is not None else None,
        "refusal": refusal,
        "bound": bound,
    }


def complete_index(
    base_sha: str,
    images: Mapping[int, tuple[str, bytes]],
    *,
    candidate_tree: str = CANDIDATE_TREE,
) -> dict[str, Any]:
    """A complete, fully bound worker-to-API index (plan 5.2)."""

    return {
        "version": 1,
        "manifest_state": "valid",
        "manifest_refusal": None,
        "status": "complete",
        "reason": None,
        "base_sha": base_sha,
        "manifest_base_sha": base_sha,
        "manifest_head_sha": None,
        "candidate_dirty": True,
        "manifest_candidate_tree": candidate_tree,
        "candidate_tree": candidate_tree,
        "head_tree": None,
        "warnings": [],
        "images": [
            image_entry(ordinal, state, content) for ordinal, (state, content) in images.items()
        ],
    }


def upload(
    stack: Stack, revision: Revision, index: Mapping[str, Any], images: Mapping[int, bytes]
) -> None:
    """The worker's relay: index, each fetched image in ordinal order, seal."""

    created = put_index(stack.client, revision.id, index)
    assert created.status_code == 201, created.text
    for ordinal in sorted(images):
        stored = put_image(stack.client, revision.id, ordinal, images[ordinal])
        assert stored.status_code == 201, stored.text
    sealed = seal(stack.client, revision.id)
    assert sealed.status_code == 200, sealed.text
    revision.images.update(images)


def lineage_version(lineage_id: uuid.UUID) -> tuple[int, str | None]:
    row = _rows(
        "SELECT version, head_sha FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": lineage_id},
    )[0]
    return int(row["version"]), row["head_sha"]


def approve_and_push(
    stack: Stack,
    auth_headers: dict[str, str],
    revision: Revision,
    *,
    head_tree: str | None = CANDIDATE_TREE,
    previous: Revision | None = None,
) -> Revision:
    """Approve through the public API, then stand in for the unchanged push Job:
    the head commit, branch and pull request land on GitHub, and the result is
    recorded through the real lineage PATCH route."""

    approved = _resolve(stack.client, auth_headers, revision.publication["approval_id"])
    assert approved.status_code == 200, approved.text
    parent = previous.head_sha if previous else BASE_SHA
    head = hashlib.sha1(f"head of {revision.id}".encode()).hexdigest()
    stack.fake.seed_commit(
        head,
        parents=[parent],
        message=f"Publish the repository changes\n\nCurie-Revision: {revision.id}\n",
        tree=head_tree,
    )
    stack.fake.branches[revision.branch] = head
    number = (
        previous.pr_number
        if previous
        else stack.fake.seed_pull(
            revision.branch, stack.fake.default_branch, title="Publish", body="Approved."
        )
    )
    pr_url = f"https://github.com/{REPO}/pull/{number}"
    version, prior_head = lineage_version(revision.lineage_id)
    owner = "api-test-worker"
    lease = (_claim_publication_lease(revision.id, owner), owner)
    with app_github(stack.client, stack.http):
        advanced = _patch_lineage(
            stack.client, revision.id, version, prior_head, head, "open", number, pr_url, lease
        )
    assert advanced.status_code == 200, advanced.text
    _mark_outcome_history_ready(revision.id)
    revision.head_sha, revision.pr_number, revision.pr_url = head, number, pr_url
    return revision


def published_with_evidence(
    stack: Stack,
    auth_headers: dict[str, str],
    *,
    head_tree: str | None = CANDIDATE_TREE,
    previous: Revision | None = None,
    candidate_tree: str = CANDIDATE_TREE,
) -> Revision:
    """A succeeded publication whose worker uploaded a complete before/after pair."""

    revision = create_revision(stack, auth_headers, previous=previous)
    base = previous.head_sha if previous else BASE_SHA
    before = png_bytes(f"before {revision.id}")
    after = png_bytes(f"after {revision.id}")
    index = complete_index(
        base, {0: ("before", before), 1: ("after", after)}, candidate_tree=candidate_tree
    )
    upload(stack, revision, index, {0: before, 1: after})
    return approve_and_push(stack, auth_headers, revision, head_tree=head_tree, previous=previous)


# --- observations -------------------------------------------------------------


def marked_comments(stack: Stack, revision: Revision) -> list[dict[str, Any]]:
    marker = f"<!-- curie-ui-evidence:{revision.lineage_id} -->"
    return [
        comment
        for comment in stack.fake.comments
        if comment["_number"] == revision.pr_number and marker in comment["body"]
    ]


def only_comment(stack: Stack, revision: Revision) -> str:
    found = marked_comments(stack, revision)
    assert len(found) == 1, [comment["body"] for comment in found]
    return str(found[0]["body"])


def image_urls(body: str) -> dict[int, tuple[str, str, str]]:
    """ordinal -> (url, evidence commit, state) for every image the body shows."""

    return {int(m[4]): (m[1], m[2], m[3]) for m in _IMAGE_URL.finditer(body)}


def raw(stack: Stack, url: str) -> bytes | None:
    response = stack.fake.handle(httpx.Request("GET", url))
    return response.content if response.status_code == 200 else None


def code_pr(stack: Stack, revision: Revision) -> dict[str, Any]:
    """Everything the code pull request is: the publication result, the lineage
    record and the pull request on GitHub. Evidence work must change none of it."""

    row = _rows(
        "SELECT p.status, p.result_url, p.version AS publication_version, "
        "p.updated_at AS publication_updated_at, l.pr_number, l.pr_url, l.head_sha, "
        "l.status AS lineage_status, l.latest_revision, l.version AS lineage_version, "
        "l.updated_at AS lineage_updated_at "
        "FROM curie.publications p JOIN curie.thread_publication_lineages l "
        "ON l.id = p.lineage_id WHERE p.id = :id",
        {"id": uuid.UUID(revision.id)},
    )[0]
    pull = stack.fake.pulls.get(revision.pr_number)
    return {
        **row,
        "pull": json.loads(json.dumps(pull)) if pull is not None else None,
        "branch_head": stack.fake.branches.get(revision.branch),
    }


def evidence_row(revision: Revision) -> dict[str, Any] | None:
    rows = _rows(
        "SELECT status, outcome, error, evidence_commit_sha, comment_written_at "
        "FROM curie.publication_ui_evidence WHERE publication_id = :id",
        {"id": uuid.UUID(revision.id)},
    )
    return rows[0] if rows else None


def stored_contents(revision: Revision) -> dict[int, bytes | None]:
    return {
        int(row["ordinal"]): row["content"]
        for row in _rows(
            "SELECT ordinal, content FROM curie.publication_ui_evidence_images "
            "WHERE publication_id = :id",
            {"id": uuid.UUID(revision.id)},
        )
    }


def expire_leases() -> None:
    """Every evidence lease lapses, as it does once its holder has died."""

    for table in ("publication_ui_evidence", "publication_lineage_ui_evidence"):
        _execute(
            f"UPDATE curie.{table} SET lease_expires_at = now() - interval '1 second' "
            "WHERE lease_expires_at IS NOT NULL"
        )


def expire_publish_window(revision: Revision) -> None:
    _execute(
        "UPDATE curie.publication_ui_evidence "
        "SET publish_deadline_at = now() - interval '1 second' WHERE publication_id = :id",
        {"id": uuid.UUID(revision.id)},
    )
    expire_leases()


def declare_github_without_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    """The code host declares evidence hosting unsupported, as GitLab and both
    Bitbucket kinds do; every other GitHub behaviour is the real adapter's."""

    original = GitHubCodeHost.__init__

    def init(self: GitHubCodeHost, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        self.capabilities = {
            **self.capabilities,
            Operation.STORE_EVIDENCE: Support.UNSUPPORTED,
            Operation.DELETE_EVIDENCE: Support.UNSUPPORTED,
        }

    monkeypatch.setattr(GitHubCodeHost, "__init__", init)


def assert_shows(stack: Stack, revision: Revision, body: str, shown: set[int]) -> str:
    """The body shows exactly ``shown``, each URL serving the uploaded bytes from
    one evidence commit that the lineage's ref names; returns that commit."""

    urls = image_urls(body)
    assert set(urls) == shown, body
    commits = {commit for _url, commit, _state in urls.values()}
    assert len(commits) == 1, commits
    (commit,) = commits
    for ordinal, (url, _commit, _state) in urls.items():
        assert raw(stack, url) == revision.images[ordinal]
    assert stack.fake.evidence_ref(str(revision.lineage_id)) == commit
    # A parentless commit outside every branch.
    assert stack.fake.commits[commit]["parents"] == []
    assert commit not in stack.fake.branches.values()
    return commit


# --- AC2: the shared corpus at the API boundary -------------------------------

_GIT_DATA_WRITES = r"^(POST|PATCH) /repos/[^ ]+/git/(blobs|trees|commits|refs)"
_COMMENT_WRITES = r"^(POST|PATCH) /repos/[^ ]+/issues/(\d+/comments|comments/\d+)$"


def _case_upload(case: Mapping[str, Any]) -> tuple[dict[str, Any], dict[int, bytes]]:
    """The case's index with every placeholder resolved (P is the publication
    base, C the candidate the pushed head carries), and the exact fixture bytes
    of every image the worker fetched: bound and unrefused."""

    checkout = stand_in_checkout(case, base=BASE_SHA, candidate_tree=CANDIDATE_TREE)
    index = resolve_index(case, checkout)
    images = {
        int(entry["ordinal"]): checkout.originals[entry["path"]]
        for entry in index["images"]
        if entry.get("refusal") is None and entry.get("bound") is True
    }
    return index, images


@contextmanager
def failing_image_storage(ordinal: int) -> Iterator[None]:
    """Postgres refuses to store image ``ordinal``'s bytes, a real storage fault
    on the API's own write; every other row is written as usual."""

    _execute(
        "CREATE OR REPLACE FUNCTION curie.test_refuse_evidence_bytes() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected storage fault'; END $$"
    )
    _execute(
        "CREATE TRIGGER test_refuse_evidence_bytes BEFORE INSERT OR UPDATE "
        "ON curie.publication_ui_evidence_images FOR EACH ROW "
        f"WHEN (NEW.ordinal = {int(ordinal)} AND NEW.content IS NOT NULL) "
        "EXECUTE FUNCTION curie.test_refuse_evidence_bytes()"
    )
    try:
        yield
    finally:
        _execute(
            "DROP TRIGGER IF EXISTS test_refuse_evidence_bytes "
            "ON curie.publication_ui_evidence_images"
        )
        _execute("DROP FUNCTION IF EXISTS curie.test_refuse_evidence_bytes()")


def _upload_case(
    stack: Stack, revision: Revision, case: Mapping[str, Any], kind: str | None
) -> None:
    index, images = _case_upload(case)
    if kind == "upload_rejection":
        # The API refuses the index PUT with a 4xx (here: a base that is not the
        # publication's), and the worker sends nothing else.
        refused = put_index(stack.client, revision.id, {**index, "base_sha": OTHER_OID})
        assert refused.status_code == 409, refused.text
        assert code(refused) == "ui_evidence.base_mismatch"
        return
    created = put_index(stack.client, revision.id, index)
    assert created.status_code == 201, created.text
    for ordinal in sorted(images):
        if kind == "storage" and ordinal == 1:
            # Ordinal 1's bytes fail to store on every attempt; ordinal 0 stores.
            with failing_image_storage(1):
                for _attempt in range(3):
                    failed = put_image(stack.client, revision.id, 1, images[1])
                    assert failed.status_code == 503, failed.text
                    assert code(failed) == "ui_evidence.storage_unavailable"
            continue
        stored = put_image(stack.client, revision.id, ordinal, images[ordinal])
        assert stored.status_code == 201, stored.text
        revision.images[ordinal] = images[ordinal]
    assert seal(stack.client, revision.id).status_code == 200


def _drive_case(stack: Stack, revision: Revision, fault: Mapping[str, Any]) -> None:
    kind = fault.get("kind") if fault.get("boundary") == "api" else None
    if kind in {"comment", "publish_timeout"}:
        if kind == "comment":
            stack.fake.fail_writes(_COMMENT_WRITES, 500, count=10_000)
        else:
            # Hosting the images never completes before the publish budget ends.
            stack.fake.fail_writes(_GIT_DATA_WRITES, 503, count=10_000)
        reconcile(stack.http)
        # The 120 s publish window runs out with the fault still in place.
        expire_publish_window(revision)
        reconcile(stack.http, passes=2)
        return
    reconcile(stack.http, passes=2)


@pytest.mark.parametrize("case", corpus_cases(), ids=lambda case: str(case["id"]))
def test_corpus_case_at_the_api(
    case: dict[str, Any],
    stack: Stack,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = case.get("api")
    if expected is None:
        pytest.skip(f"{case['id']} has no API expectation")
    fault = case.get("fault") or {}
    kind = fault.get("kind")
    if kind == "host_unsupported":
        declare_github_without_evidence(monkeypatch)
    texts = load_corpus()["texts"]

    revision = create_revision(stack, auth_headers)
    if case.get("index") is not None:
        _upload_case(stack, revision, case, kind)
    approve_and_push(stack, auth_headers, revision)
    pull_request = code_pr(stack, revision)

    _drive_case(stack, revision, fault)

    assert code_pr(stack, revision) == pull_request
    found = marked_comments(stack, revision)
    if expected["comment"] == "none":
        assert found == []
        if case.get("index") is None or kind == "upload_rejection":
            assert evidence_row(revision) is None
            assert not stack.fake.blobs
        return
    body = only_comment(stack, revision)
    shown = {int(ordinal) for ordinal in expected.get("shown") or []}
    if shown:
        assert_shows(stack, revision, body, shown)
    else:
        assert image_urls(body) == {}
    if expected.get("outcome"):
        assert f"**Outcome:** {expected['outcome']}" in body
    for ordinal, refusal in (expected.get("not_shown") or {}).items():
        assert int(ordinal) not in image_urls(body)
        assert texts[refusal] in body, (refusal, body)
    if expected["comment"] in {"evidence", "outcome"}:
        assert f"`{revision.head_sha}`" in body
        assert f"`{BASE_SHA}`" in body
    if expected["comment"] == "unsupported":
        assert "This code host cannot show UI evidence" in body
        assert not stack.fake.blobs
        assert stack.fake.evidence_ref(str(revision.lineage_id)) is None
    # Never an image that was not stored and bound.
    for ordinal in image_urls(body):
        assert ordinal in revision.images


# --- AC2: the upload routes -----------------------------------------------------


def test_index_upload_is_idempotent_and_refuses_a_different_index(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    revision = create_revision(stack, auth_headers)
    before, after = png_bytes("idempotent before"), png_bytes("idempotent after")
    index = complete_index(BASE_SHA, {0: ("before", before), 1: ("after", after)})

    created = put_index(stack.client, revision.id, index)
    assert created.status_code == 201, created.text
    assert created.json() == {"status": "receiving", "expected": [0, 1]}
    assert created.headers["cache-control"] == "no-store"
    stored = _rows(
        "SELECT index_sha256 FROM curie.publication_ui_evidence WHERE publication_id = :id",
        {"id": uuid.UUID(revision.id)},
    )

    replayed = put_index(stack.client, revision.id, index)
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["status"] == "receiving"

    different = json.loads(json.dumps(index))
    different["images"][1]["label"] = "Settings page"
    conflict = put_index(stack.client, revision.id, different)
    assert conflict.status_code == 409
    assert code(conflict) == "ui_evidence.index_conflict"
    # The first index stands.
    assert (
        _rows(
            "SELECT index_sha256 FROM curie.publication_ui_evidence WHERE publication_id = :id",
            {"id": uuid.UUID(revision.id)},
        )
        == stored
    )
    assert len(stored_contents(revision)) == 2

    other = create_revision(stack, auth_headers)
    mismatch = put_index(stack.client, other.id, complete_index(OTHER_OID, {}))
    assert mismatch.status_code == 409
    assert code(mismatch) == "ui_evidence.base_mismatch"
    # Liveness: the same publication takes an index naming its own base.
    assert put_index(stack.client, other.id, complete_index(BASE_SHA, {})).status_code == 201

    missing = put_index(stack.client, str(uuid.uuid4()), index)
    assert missing.status_code == 404
    unauthenticated = stack.client.put(
        f"/v1/internal/publications/{revision.id}/ui-evidence", json=index
    )
    assert unauthenticated.status_code == 401


def test_image_upload_refuses_digest_mismatch_and_sealed_rows(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    revision = create_revision(stack, auth_headers)
    before, after = png_bytes("upload before"), png_bytes("upload after")
    index = complete_index(BASE_SHA, {0: ("before", before), 1: ("after", after)})
    index["images"].append(
        image_entry(2, "after", None, path="after/notes.png", refusal="not_png", bound=False)
    )
    created = put_index(stack.client, revision.id, index)
    assert created.status_code == 201, created.text
    assert created.json()["expected"] == [0, 1]

    wrong_header = put_image(stack.client, revision.id, 0, before, digest=sha256(after))
    assert wrong_header.status_code == 409
    assert code(wrong_header) == "ui_evidence.digest_mismatch"
    wrong_body = put_image(stack.client, revision.id, 0, after, digest=sha256(before))
    assert wrong_body.status_code == 409
    assert code(wrong_body) == "ui_evidence.digest_mismatch"
    assert stored_contents(revision)[0] is None

    # Liveness: the entry's own bytes are stored, and a replay of them is a 200.
    stored = put_image(stack.client, revision.id, 0, before)
    assert stored.status_code == 201, stored.text
    assert stored.json() == {"ordinal": 0, "stored": True}
    assert put_image(stack.client, revision.id, 0, before).status_code == 200
    assert stored_contents(revision)[0] == before

    refused = put_image(stack.client, revision.id, 2, before)
    assert refused.status_code == 409
    assert code(refused) == "ui_evidence.image_not_expected"
    assert put_image(stack.client, revision.id, 9, before).status_code == 404

    sealed = seal(stack.client, revision.id)
    assert sealed.status_code == 200, sealed.text
    assert sealed.json() == {"status": "ready", "stored": [0], "missing": [1]}
    late = put_image(stack.client, revision.id, 1, after)
    assert late.status_code == 409
    assert code(late) == "ui_evidence.sealed"
    assert stored_contents(revision)[1] is None
    again = seal(stack.client, revision.id)
    assert again.status_code == 200
    assert again.json()["status"] == "ready"


def test_terminal_rows_clear_image_bytes(stack: Stack, auth_headers: dict[str, str]) -> None:
    published = published_with_evidence(stack, auth_headers)
    # Liveness: stored bytes are kept until the row is terminal.
    assert all(content is not None for content in stored_contents(published).values())

    denied = create_revision(stack, auth_headers)
    content = png_bytes("denied before")
    upload(stack, denied, complete_index(BASE_SHA, {0: ("before", content)}), {0: content})
    refused = _resolve(
        stack.client, auth_headers, denied.publication["approval_id"], decision="rejected"
    )
    assert refused.status_code == 200, refused.text

    reconcile(stack.http)

    assert evidence_row(published)["status"] == "published"
    assert set(stored_contents(published).values()) == {None}
    assert evidence_row(denied)["status"] == "abandoned"
    assert set(stored_contents(denied).values()) == {None}
    # The denied publication has no pull request and no evidence on GitHub.
    assert content not in stack.fake.blobs.values()


# --- AC3: twice, restart, stale -------------------------------------------------


def test_running_the_reconciler_twice_keeps_one_comment_and_the_pull_request(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    revision = published_with_evidence(stack, auth_headers)
    pull_request = code_pr(stack, revision)

    reconcile(stack.http)
    body = only_comment(stack, revision)
    commit = assert_shows(stack, revision, body, {0, 1})
    assert f"**Pull request head:** `{revision.head_sha}` (revision 1)" in body
    assert f"**Base:** `{BASE_SHA}`" in body
    assert "**Outcome:** complete" in body
    writes = stack.fake.writes

    reconcile(stack.http, passes=2)
    reconcile(stack.http)

    assert only_comment(stack, revision) == body
    assert stack.fake.evidence_ref(str(revision.lineage_id)) == commit
    assert stack.fake.writes == writes
    assert code_pr(stack, revision) == pull_request
    assert evidence_row(revision)["status"] == "published"


class _Crash(BaseException):
    """The process dies: nothing in the reconciler may catch this."""


def test_a_restart_mid_publish_resumes_without_a_second_comment(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    revision = published_with_evidence(stack, auth_headers)
    pull_request = code_pr(stack, revision)

    def dies_on_the_comment(request: httpx.Request) -> httpx.Response:
        if re.search(_COMMENT_WRITES, f"{request.method} {request.url.path}"):
            raise _Crash
        return stack.fake.handle(request)

    crashing = httpx.AsyncClient(transport=httpx.MockTransport(dies_on_the_comment))
    try:
        with pytest.raises(_Crash):
            reconcile(crashing)
    finally:
        asyncio.run(crashing.aclose())

    stored_at_crash = stack.fake.evidence_ref(str(revision.lineage_id))
    assert stored_at_crash is not None
    assert marked_comments(stack, revision) == []
    writes = stack.fake.writes

    # A new process while the dead one's lease is still live writes nothing.
    reconcile(stack.http)
    assert marked_comments(stack, revision) == []
    assert stack.fake.writes == writes

    expire_leases()
    reconcile(stack.http)

    body = only_comment(stack, revision)
    assert assert_shows(stack, revision, body, {0, 1}) == stored_at_crash
    assert stack.fake.evidence_ref(str(revision.lineage_id)) == stored_at_crash
    assert code_pr(stack, revision) == pull_request
    assert evidence_row(revision)["status"] == "published"
    reconcile(stack.http)
    assert len(marked_comments(stack, revision)) == 1


def test_a_later_revision_without_evidence_writes_the_stale_notice_and_deletes_the_ref(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    first = published_with_evidence(stack, auth_headers)
    reconcile(stack.http)
    assert_shows(stack, first, only_comment(stack, first), {0, 1})

    second = create_revision(stack, auth_headers, previous=first)
    approve_and_push(stack, auth_headers, second, head_tree=OTHER_OID, previous=first)
    pull_request = code_pr(stack, second)

    # Liveness: inside the upload window a missing upload may still arrive, so
    # the earlier comment (which names its own head, not this one) stands.
    reconcile(stack.http)
    held = only_comment(stack, first)
    assert f"`{first.head_sha}` (revision 1)" in held
    assert stack.fake.evidence_ref(str(first.lineage_id)) is not None

    _execute(
        "UPDATE curie.publications SET created_at = created_at - interval '5 minutes' "
        "WHERE id = :id",
        {"id": uuid.UUID(second.id)},
    )
    reconcile(stack.http)

    body = only_comment(stack, second)
    assert (
        f"No UI evidence exists for the current pull request head `{second.head_sha}` "
        "(revision 2). Evidence from an earlier revision was removed."
    ) in body
    assert image_urls(body) == {}
    assert stack.fake.evidence_ref(str(first.lineage_id)) is None
    assert code_pr(stack, second) == pull_request
    reconcile(stack.http)
    assert only_comment(stack, second) == body


def test_stale_evidence_of_an_older_revision_is_superseded_not_published(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    first = published_with_evidence(stack, auth_headers)
    # Revision 2 lands before the reconciler ever saw revision 1's evidence.
    second_tree = "5ec0d00d5ec0d00d5ec0d00d5ec0d00d5ec0d00d"
    second = published_with_evidence(
        stack,
        auth_headers,
        previous=first,
        head_tree=second_tree,
        candidate_tree=second_tree,
    )
    pull_request = code_pr(stack, second)

    reconcile(stack.http, passes=2)

    assert evidence_row(first)["status"] == "superseded"
    body = only_comment(stack, second)
    assert f"**Pull request head:** `{second.head_sha}` (revision 2)" in body
    assert f"**Pull request head:** `{first.head_sha}`" not in body
    assert_shows(stack, second, body, {0, 1})
    for content in first.images.values():
        assert content not in stack.fake.blobs.values()
    assert set(stored_contents(first).values()) == {None}
    assert code_pr(stack, second) == pull_request


@pytest.mark.parametrize("head_tree", [OTHER_OID, None], ids=["other-tree", "no-tree-field"])
def test_stale_head_tree_marks_after_images_unavailable(
    stack: Stack, auth_headers: dict[str, str], head_tree: str | None
) -> None:
    # The pushed head's tree is not the captured candidate, or GitHub's commit
    # payload carries no tree at all: the after image is never shown as current.
    revision = published_with_evidence(stack, auth_headers, head_tree=head_tree)
    pull_request = code_pr(stack, revision)

    reconcile(stack.http)

    body = only_comment(stack, revision)
    assert_shows(stack, revision, body, {0})
    assert load_corpus()["texts"]["head_changed"] in body
    assert "**Outcome:** partial" in body
    assert revision.images[1] not in stack.fake.blobs.values()
    assert code_pr(stack, revision) == pull_request


@pytest.mark.parametrize("merged", [True, False], ids=["merged", "closed"])
def test_merged_or_closed_lineage_deletes_the_evidence_ref(
    stack: Stack, auth_headers: dict[str, str], merged: bool
) -> None:
    revision = published_with_evidence(stack, auth_headers)
    reconcile(stack.http)
    body = only_comment(stack, revision)
    commit = assert_shows(stack, revision, body, {0, 1})
    # Liveness: an open pull request keeps its evidence ref.
    reconcile(stack.http)
    assert stack.fake.evidence_ref(str(revision.lineage_id)) == commit

    # The lineage reconciler's record of the pull request ending.
    stack.fake.set_pull_state(revision.pr_number, merged=merged)
    _execute(
        "UPDATE curie.thread_publication_lineages SET status = :status WHERE id = :id",
        {"status": "merged" if merged else "closed", "id": revision.lineage_id},
    )
    reconcile(stack.http)

    assert stack.fake.evidence_ref(str(revision.lineage_id)) is None
    assert only_comment(stack, revision) == body
    writes = stack.fake.writes
    reconcile(stack.http)
    assert stack.fake.writes == writes


def _dies_after(stack: Stack, pattern: str) -> httpx.AsyncClient:
    """GitHub applies the first matching write, then the process dies before
    it hears the answer."""

    def handler(request: httpx.Request) -> httpx.Response:
        response = stack.fake.handle(request)
        if re.search(pattern, f"{request.method} {request.url.path}"):
            raise _Crash
        return response

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _crash_once(client: httpx.AsyncClient) -> None:
    try:
        with pytest.raises(_Crash):
            reconcile(client)
    finally:
        asyncio.run(client.aclose())


def lineage_evidence(revision: Revision) -> dict[str, Any]:
    rows = _rows(
        "SELECT ref_commit_sha, ref_pending, comment_revision, comment_kind, comment_pending "
        "FROM curie.publication_lineage_ui_evidence WHERE lineage_id = :id",
        {"id": revision.lineage_id},
    )
    assert len(rows) == 1
    return rows[0]


def test_a_head_pushed_after_the_recorded_one_marks_after_images_unavailable(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    # Someone pushed to the pull request branch after Curie recorded its head;
    # the recorded head's tree is still the candidate, the live head is not.
    revision = published_with_evidence(stack, auth_headers)
    stack.fake.branches[stack.fake.pulls[revision.pr_number]["head"]] = "f" * 40

    reconcile(stack.http)

    body = only_comment(stack, revision)
    assert_shows(stack, revision, body, {0})
    assert load_corpus()["texts"]["head_changed"] in body
    assert revision.images[1] not in stack.fake.blobs.values()


@pytest.mark.parametrize("merged", [True, False], ids=["merged", "closed"])
def test_a_ref_stored_before_a_crash_is_deleted_when_the_lineage_ends(
    stack: Stack, auth_headers: dict[str, str], merged: bool
) -> None:
    revision = published_with_evidence(stack, auth_headers)
    # The ref is written on GitHub, then the process dies before recording it.
    _crash_once(_dies_after(stack, r"^(POST|PATCH) /repos/[^ ]+/git/refs"))
    assert stack.fake.evidence_ref(str(revision.lineage_id)) is not None
    held = lineage_evidence(revision)
    assert held["ref_commit_sha"] is None and held["ref_pending"] is True

    stack.fake.set_pull_state(revision.pr_number, merged=merged)
    _execute(
        "UPDATE curie.thread_publication_lineages SET status = :status WHERE id = :id",
        {"status": "merged" if merged else "closed", "id": revision.lineage_id},
    )
    expire_leases()
    reconcile(stack.http)

    assert stack.fake.evidence_ref(str(revision.lineage_id)) is None
    cleaned = lineage_evidence(revision)
    assert cleaned["ref_commit_sha"] is None and cleaned["ref_pending"] is False


def test_a_crash_after_the_comment_records_neither_half_and_restart_settles_both(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    first = published_with_evidence(stack, auth_headers)
    _crash_once(_dies_after(stack, _COMMENT_WRITES))
    assert len(marked_comments(stack, first)) == 1
    # The comment and its record land in one commit: neither half is recorded.
    assert evidence_row(first)["comment_written_at"] is None
    assert lineage_evidence(first)["comment_revision"] is None

    expire_leases()
    reconcile(stack.http)
    assert_shows(stack, first, only_comment(stack, first), {0, 1})
    assert evidence_row(first)["status"] == "published"
    assert lineage_evidence(first)["comment_revision"] == 1

    # So a later revision without evidence still gets the current-head notice.
    second = create_revision(stack, auth_headers, previous=first)
    approve_and_push(stack, auth_headers, second, head_tree=OTHER_OID, previous=first)
    _execute(
        "UPDATE curie.publications SET created_at = created_at - interval '5 minutes' "
        "WHERE id = :id",
        {"id": uuid.UUID(second.id)},
    )
    reconcile(stack.http)
    body = only_comment(stack, second)
    assert f"No UI evidence exists for the current pull request head `{second.head_sha}`" in body
    assert image_urls(body) == {}
    assert stack.fake.evidence_ref(str(first.lineage_id)) is None


def test_a_crash_after_the_comment_then_a_newer_revision_without_evidence_gets_the_notice(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    first = published_with_evidence(stack, auth_headers)
    _crash_once(_dies_after(stack, _COMMENT_WRITES))
    assert_shows(stack, first, only_comment(stack, first), {0, 1})
    held = lineage_evidence(first)
    assert held["comment_revision"] is None and held["comment_pending"] is True

    # Revision 2 lands with no evidence before the reconciler ever recovers.
    second = create_revision(stack, auth_headers, previous=first)
    approve_and_push(stack, auth_headers, second, head_tree=OTHER_OID, previous=first)
    pull_request = code_pr(stack, second)
    _execute(
        "UPDATE curie.publications SET created_at = created_at - interval '5 minutes' "
        "WHERE id = :id",
        {"id": uuid.UUID(second.id)},
    )
    expire_leases()
    reconcile(stack.http, passes=2)

    body = only_comment(stack, second)
    assert f"No UI evidence exists for the current pull request head `{second.head_sha}`" in body
    assert image_urls(body) == {}
    assert stack.fake.evidence_ref(str(first.lineage_id)) is None
    assert evidence_row(first)["status"] == "superseded"
    settled = lineage_evidence(first)
    assert settled["comment_revision"] == 2 and settled["comment_pending"] is False
    assert code_pr(stack, second) == pull_request
    writes = stack.fake.writes
    reconcile(stack.http)
    assert stack.fake.writes == writes


def test_an_index_with_no_image_stored_by_the_deadline_gets_a_failed_outcome(
    stack: Stack, auth_headers: dict[str, str]
) -> None:
    # The worker sent the index and died before any image or the seal.
    revision = create_revision(stack, auth_headers)
    before, after = png_bytes("never stored before"), png_bytes("never stored after")
    index = complete_index(BASE_SHA, {0: ("before", before), 1: ("after", after)})
    assert put_index(stack.client, revision.id, index).status_code == 201
    approve_and_push(stack, auth_headers, revision)
    pull_request = code_pr(stack, revision)

    # Liveness: inside the upload window the missing images may still arrive.
    reconcile(stack.http)
    assert marked_comments(stack, revision) == []

    _execute(
        "UPDATE curie.publication_ui_evidence "
        "SET upload_deadline_at = now() - interval '1 second' WHERE publication_id = :id",
        {"id": uuid.UUID(revision.id)},
    )
    reconcile(stack.http)

    body = only_comment(stack, revision)
    assert "**Outcome:** failed" in body
    assert body.count(load_corpus()["texts"]["not_stored"]) == 2
    assert image_urls(body) == {}
    assert not stack.fake.blobs
    assert code_pr(stack, revision) == pull_request


@pytest.mark.parametrize("_evidence_env", [True, False], ids=["app", "no-app"], indirect=True)
def test_without_a_github_app_id_no_evidence_comment_is_written(
    _evidence_env: None, stack: Stack, auth_headers: dict[str, str], request: pytest.FixtureRequest
) -> None:
    # Ownership of a marked comment is the App id that posted it; without one
    # the reconciler cannot tell its own comment from a pasted marker, so it
    # records the row unsupported and writes nothing. The App variant is the
    # liveness half: the same publication gets its comment.
    app = request.node.callspec.params["_evidence_env"]
    revision = published_with_evidence(stack, auth_headers)
    pull_request = code_pr(stack, revision)
    writes = stack.fake.writes

    reconcile(stack.http)

    row = evidence_row(revision)
    assert row is not None
    if app:
        assert_shows(stack, revision, only_comment(stack, revision), {0, 1})
        assert row["status"] == "published"
    else:
        assert marked_comments(stack, revision) == []
        assert row["status"] == "unsupported"
        assert "comment_ownership_unavailable" in {row["outcome"], row["error"]}
        assert stack.fake.writes == writes
        assert not stack.fake.blobs
        assert set(stored_contents(revision).values()) == {None}
    assert code_pr(stack, revision) == pull_request


def test_the_marker_is_the_lineage_marker() -> None:
    lineage_id = uuid.uuid4()
    assert ui_evidence_marker(lineage_id) == f"curie-ui-evidence:{lineage_id}"


@dataclass(frozen=True)
class _Entry:
    ordinal: int
    state: str | None
    label: str | None
    path: str | None


def test_many_long_path_images_show_as_many_as_fit_and_count_the_rest() -> None:
    # 300 small images with 200-character paths: listing every image not shown
    # on its own line could never fit, but images plus a counted remainder do.
    entries = [
        _Entry(n, "before" if n < 150 else "after", f"screen {n}", f"{n:04d}/" + "p" * 195)
        for n in range(300)
    ]
    raw = f"https://github.com/{REPO}/raw/{'a' * 40}"
    urls = {entry.ordinal: f"{raw}/{entry.state}/{entry.ordinal:04d}.png" for entry in entries}
    rendered = render_evidence(
        marker=ui_evidence_marker(uuid.uuid4()),
        manifest_state="valid",
        manifest_status="complete",
        reason=None,
        head_sha="b" * 40,
        revision=1,
        base_sha="c" * 40,
        entries=entries,
        urls=urls,
        refusals={},
    )

    shown = len(_IMAGE_URL.findall(rendered.body))
    assert 100 < shown < 300
    assert len(rendered.body) <= MAX_COMMENT_CHARS
    assert rendered.outcome == "partial"
    assert re.search(r"\d+ more images are not shown\.", rendered.body)
