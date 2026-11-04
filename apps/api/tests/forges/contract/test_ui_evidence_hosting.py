"""UI evidence hosting through the port (ADR 0197, contract item 11).

`CodeHost.store_evidence` hosts image files outside every branch under one ref
named by a key and returns each file's image URL at the stored commit;
`CodeHost.delete_evidence` removes that ref. A host that declares either
operation unsupported raises `Unsupported` and writes nothing. Every
observation is made from outside the port, through the harness.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest
from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.errors import Unsupported
from curie_api.forges.types import EvidenceFile, StoredEvidence
from forge_fakes.contract_harness import AdapterHarness

STAMP = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
LINEAGE_ID = "00000000-0000-4000-8000-000000005336"
MESSAGE = "Curie UI evidence for publication 7d1f4b8e-0c55-4c5f-9a7e-0e3b1d2c4a66\n"


def _png(tag: bytes) -> bytes:
    # The port hosts bytes as given; PNG validation is the runner's and API's.
    return b"\x89PNG\r\n\x1a\n" + tag


def _file(path: str, content: bytes) -> EvidenceFile:
    return EvidenceFile(path, content, hashlib.sha256(content).hexdigest())


BEFORE = _png(b"before-home")
AFTER = _png(b"after-home")
FILES = [_file("before/0000.png", BEFORE), _file("after/0001.png", AFTER)]


def _hosting(harness: AdapterHarness) -> bool:
    capabilities = harness.code_host.capabilities
    return supports(capabilities, Operation.STORE_EVIDENCE) and supports(
        capabilities, Operation.DELETE_EVIDENCE
    )


async def _store(harness: AdapterHarness, files: list[EvidenceFile]) -> StoredEvidence:
    return await harness.code_host.store_evidence(
        harness.repository, key=LINEAGE_ID, files=files, message=MESSAGE, stamp=STAMP
    )


@pytest.mark.anyio
async def test_storing_the_same_evidence_twice_gives_the_same_commit_and_urls(
    harness: AdapterHarness,
) -> None:
    if not _hosting(harness):
        pytest.skip(f"{harness.name} declares no evidence hosting")

    first = await _store(harness, FILES)
    again = await _store(harness, FILES)

    assert again.commit_sha == first.commit_sha
    assert dict(again.urls) == dict(first.urls)
    assert set(first.urls) == {"before/0000.png", "after/0001.png"}
    assert harness.evidence_ref(LINEAGE_ID) == first.commit_sha
    assert harness.evidence_raw(first.urls["before/0000.png"]) == BEFORE
    assert harness.evidence_raw(first.urls["after/0001.png"]) == AFTER
    # Every URL is pinned to the stored commit, never to a moving name.
    assert all(first.commit_sha in url for url in first.urls.values())


@pytest.mark.anyio
async def test_storing_different_evidence_under_the_same_key_moves_the_ref(
    harness: AdapterHarness,
) -> None:
    if not _hosting(harness):
        pytest.skip(f"{harness.name} declares no evidence hosting")

    first = await _store(harness, FILES)
    replacement = _png(b"after-home-revision-two")
    second = await _store(harness, [_file("after/0000.png", replacement)])

    assert second.commit_sha != first.commit_sha
    assert second.ref == first.ref
    assert harness.evidence_ref(LINEAGE_ID) == second.commit_sha
    assert harness.evidence_raw(second.urls["after/0000.png"]) == replacement
    # Only one ref exists for the key: one delete removes it, the next finds none.
    assert await harness.code_host.delete_evidence(harness.repository, key=LINEAGE_ID) is True
    assert await harness.code_host.delete_evidence(harness.repository, key=LINEAGE_ID) is False


@pytest.mark.anyio
async def test_delete_reports_true_then_false(harness: AdapterHarness) -> None:
    if not _hosting(harness):
        pytest.skip(f"{harness.name} declares no evidence hosting")
    # Liveness: deleting a key that was never stored is a valid call, not an error.
    assert await harness.code_host.delete_evidence(harness.repository, key="never-stored") is False

    await _store(harness, FILES)
    assert harness.evidence_ref(LINEAGE_ID) is not None

    assert await harness.code_host.delete_evidence(harness.repository, key=LINEAGE_ID) is True
    assert harness.evidence_ref(LINEAGE_ID) is None
    assert await harness.code_host.delete_evidence(harness.repository, key=LINEAGE_ID) is False


@pytest.mark.anyio
async def test_an_unsupported_host_raises_and_writes_nothing(harness: AdapterHarness) -> None:
    capabilities = harness.code_host.capabilities
    if _hosting(harness):
        pytest.skip(f"{harness.name} hosts evidence")
    before = harness.write_count()

    if not supports(capabilities, Operation.STORE_EVIDENCE):
        with pytest.raises(Unsupported) as refused:
            await _store(harness, FILES)
        assert refused.value.operation is Operation.STORE_EVIDENCE
    if not supports(capabilities, Operation.DELETE_EVIDENCE):
        with pytest.raises(Unsupported) as refused:
            await harness.code_host.delete_evidence(harness.repository, key=LINEAGE_ID)
        assert refused.value.operation is Operation.DELETE_EVIDENCE

    assert harness.write_count() == before
    assert harness.evidence_ref(LINEAGE_ID) is None


@pytest.mark.anyio
async def test_read_commit_reports_the_tree_when_the_host_carries_one(
    harness: AdapterHarness,
) -> None:
    code_host, repository = harness.code_host, harness.repository
    # A commit whose payload carries no tree stays readable; its tree is unknown.
    bare = harness.push_head("factory/evidence-bare")
    assert (await code_host.read_commit(repository, bare)).tree is None

    tree = hashlib.sha1(b"tree for the evidence contract").hexdigest()
    with_tree = harness.push_head_with_tree("factory/evidence-tree", tree)
    if with_tree is None:
        pytest.skip(f"{harness.name} reports no commit tree")

    commit = await code_host.read_commit(repository, with_tree)

    assert commit.sha == with_tree
    assert commit.tree == tree
