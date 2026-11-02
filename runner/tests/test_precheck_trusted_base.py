"""The revision precheck anchors its capture at the prepared head (#5230).

``PublicationContext.expected_head`` is P. A local commit on top of P is a
publishable change, not a moved checkout; only a HEAD that is not P or a
bounded local descendant of it keeps the ``head_changed:`` refusal.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Any

import anyio
from aci_protocol import PublicationContext
from curie_runner.publication_precheck import PublicationPrecheck

REPO = "acme-corp/acme-bot"
GITHUB = "https://github.com"
API = "https://api.example.com"
TITLE = "Update documentation"
BODY = "Explain the requested documentation changes.\n"
IDENTITY = (
    "-c",
    "user.name=Acme Bot",
    "-c",
    "user.email=acme-bot@example.com",
    "-c",
    "commit.gpgsign=false",
)
# The refusal text a network-disabled precheck gives an empty patch.
EMPTY_PATCH_UNAVAILABLE = "precheck_unavailable: Publication could not be verified."


def _git(repo: Path, *args: str, stdin: bytes | None = None) -> str:
    result = subprocess.run(
        ["git", *args], cwd=repo, input=stdin, check=True, capture_output=True
    )
    return result.stdout.decode("utf-8", errors="replace").strip()


def _commit(repo: Path, path: str, data: bytes, message: str) -> str:
    (repo / path).write_bytes(data)
    _git(repo, "add", path)
    _git(repo, *IDENTITY, "commit", "--quiet", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _prepared(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "workspace"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "remote", "add", "origin", f"{GITHUB}/{REPO}.git")
    return repo, _commit(repo, "README.md", b"Original documentation.\n", "Prepared head")


def _local_history(repo: Path, parent: str, count: int, *, branch: str) -> None:
    stream = bytearray()
    for index in range(1, count + 1):
        message = b"Local step %d\n" % index
        stream += b"commit refs/heads/%s\n" % branch.encode()
        stream += b"committer Acme Bot <acme-bot@example.com> %d +0000\n" % (
            1_700_000_000 + index
        )
        stream += b"data %d\n%s" % (len(message), message)
        if index == 1:
            stream += b"from %s\n" % parent.encode()
        if index == count:
            content = b"Documentation after %d local commits.\n" % count
            stream += b"M 100644 inline README.md\ndata %d\n%s" % (len(content), content)
        stream += b"\n"
    _git(repo, "fast-import", "--quiet", stdin=bytes(stream))
    _git(repo, "checkout", "--quiet", branch)


def _context(expected_head: str) -> PublicationContext:
    payload: dict[str, Any] = {
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "deployment_id": "22222222-2222-4222-8222-222222222222",
        "work_item_id": "33333333-3333-4333-8333-333333333333",
        "execution_request_id": "44444444-4444-4444-8444-444444444444",
        "lineage_id": "55555555-5555-4555-8555-555555555555",
        "runtime_epoch": 2,
        "lineage_version": 3,
        "conversation_id": "slack:T0EXAMPLE1:C0EXAMPLE1:123.45",
        "queued_event_id": "publicationevent1",
        "expected_head": expected_head,
        "precheck_url": f"{API}/publications/precheck",
        "capability": "ppc.test.publicationcapability",
        "observed_title": TITLE,
        "observed_body_sha256": hashlib.sha256(BODY.encode()).hexdigest(),
        "observed_at": "2026-09-25T10:00:00Z",
    }
    return PublicationContext.model_validate(payload)


def _decide(repo: Path, expected_head: str) -> str | None:
    """One fresh precheck decision with the network disabled.

    A nonempty patch is decided locally, so no metadata provider is needed; an
    empty patch takes the existing unavailable refusal instead of a remote read.
    """

    precheck = PublicationPrecheck(
        repo,
        API,
        network_enabled=False,
        repo_origin=GITHUB,
        repo_path=REPO,
    )
    precheck.bind(_context(expected_head))
    decision: list[str | None] = []

    async def go() -> None:
        decision.append(await precheck.decide("publish-1", {"title": TITLE, "body": BODY}))

    anyio.run(go)
    return decision[0]


def test_a_local_commit_on_the_prepared_head_is_a_publishable_change(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    head = _commit(repo, "README.md", b"Committed documentation.\n", "Agent change")
    assert head != prepared
    assert _git(repo, "status", "--porcelain") == ""

    assert _decide(repo, prepared) is None


def test_an_unrelated_head_keeps_the_head_changed_refusal(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _git(repo, "checkout", "--quiet", "--orphan", "unrelated")
    _commit(repo, "README.md", b"Unrelated documentation.\n", "Unrelated root")

    refusal = _decide(repo, prepared)

    assert refusal is not None
    assert refusal.startswith("head_changed:")


def test_local_history_over_256_commits_keeps_the_head_changed_refusal(
    tmp_path: Path,
) -> None:
    repo, prepared = _prepared(tmp_path)

    _local_history(repo, prepared, 256, branch="history-256")
    assert _decide(repo, prepared) is None

    _local_history(repo, prepared, 257, branch="history-257")
    refusal = _decide(repo, prepared)
    assert refusal is not None
    assert refusal.startswith("head_changed:")


def test_a_change_committed_then_reverted_takes_the_empty_patch_path(tmp_path: Path) -> None:
    repo, prepared = _prepared(tmp_path)
    _commit(repo, "README.md", b"Committed documentation.\n", "Agent change")
    _commit(repo, "README.md", b"Original documentation.\n", "Revert agent change")
    assert _git(repo, "rev-parse", "HEAD") != prepared

    refusal = _decide(repo, prepared)

    assert refusal is not None
    assert not refusal.startswith("head_changed:")
    assert refusal.startswith(EMPTY_PATCH_UNAVAILABLE)
