"""The in-memory code host's ancestry answer (#5094).

The memory forge answers ``is_ancestor`` from its own commit table, following
every parent. It proves "not an ancestor" only over a well-formed graph: a
missing commit, a commit stored under another sha, or a parent that is not a
commit sha is Unavailable, never False. Merges and forks are seeded straight
into the table, since ``push`` only extends one branch.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest
from curie_api.forges.capabilities import CODE_HOST_OPERATIONS, Operation, Support
from curie_api.forges.errors import Unavailable, Unsupported
from curie_api.forges.memory import InMemoryCodeHost
from curie_api.forges.types import Commit, RepositoryRef


def _sha(number: int) -> str:
    return f"{number:040x}"


ROOT, SIDE, MAINLINE, MERGE, FORK = (_sha(9000 + n) for n in range(5))


def _host(
    capabilities: Mapping[Operation, Support] | None = None,
) -> tuple[InMemoryCodeHost, RepositoryRef]:
    """A root, a side branch merged back into the mainline, and a fork off the root."""

    host = InMemoryCodeHost(capabilities=capabilities)
    repository = host.seed_repository("acme/widgets")
    for sha, parents in (
        (ROOT, ()),
        (SIDE, (ROOT,)),
        (MAINLINE, (ROOT,)),
        (MERGE, (MAINLINE, SIDE)),
        (FORK, (ROOT,)),
    ):
        host._commits[sha] = Commit(sha, parents, f"commit {sha[-4:]}")
    return host, repository


def _is_ancestor(host: InMemoryCodeHost, repository: RepositoryRef, a: str, d: str) -> bool:
    return asyncio.run(host.is_ancestor(repository, a, d))


def test_memory_is_ancestor_of_identical_shas_is_true() -> None:
    host, repository = _host()

    assert _is_ancestor(host, repository, MERGE, MERGE) is True
    # Equal shas need no graph at all.
    assert _is_ancestor(host, repository, _sha(1), _sha(1)) is True


@pytest.mark.parametrize(
    ("ancestor", "descendant", "expected"),
    [
        (ROOT, MAINLINE, True),
        (SIDE, MERGE, True),
        (ROOT, MERGE, True),
        (MERGE, ROOT, False),
        (FORK, MERGE, False),
    ],
    ids=("ahead", "merge-parent", "through-merge", "behind", "diverged"),
)
def test_memory_is_ancestor_follows_every_parent(
    ancestor: str, descendant: str, expected: bool
) -> None:
    host, repository = _host()

    assert _is_ancestor(host, repository, ancestor, descendant) is expected


def test_memory_is_ancestor_follows_a_pushed_branch() -> None:
    host = InMemoryCodeHost()
    repository = host.seed_repository("acme/widgets")
    first = host.push(repository, "main", "first")
    second = host.push(repository, "main", "second")

    assert asyncio.run(host.is_ancestor(repository, first, second)) is True
    assert asyncio.run(host.is_ancestor(repository, second, first)) is False


@pytest.mark.parametrize("defect", ["missing-commit", "wrong-sha", "bad-parent"])
def test_memory_is_ancestor_on_a_malformed_graph_is_unavailable_not_false(defect: str) -> None:
    host, repository = _host()
    if defect == "missing-commit":
        del host._commits[MAINLINE]
    elif defect == "wrong-sha":
        host._commits[MAINLINE] = Commit(_sha(1), (ROOT,), "stored under another sha")
    else:
        host._commits[MAINLINE] = Commit(MAINLINE, ("not-a-sha",), "bad parent")

    with pytest.raises(Unavailable):
        _is_ancestor(host, repository, FORK, MERGE)
    # Liveness twin: the same question answers once the graph is whole.
    host._commits[MAINLINE] = Commit(MAINLINE, (ROOT,), "mainline")
    assert _is_ancestor(host, repository, FORK, MERGE) is False


def test_memory_is_ancestor_of_an_unknown_descendant_is_unavailable() -> None:
    host, repository = _host()

    with pytest.raises(Unavailable):
        _is_ancestor(host, repository, ROOT, _sha(1))


def test_memory_is_ancestor_is_gated_by_its_declared_operation() -> None:
    declared = dict.fromkeys(CODE_HOST_OPERATIONS, Support.SUPPORTED)
    declared[Operation.IS_ANCESTOR] = Support.UNSUPPORTED
    host, repository = _host(declared)

    with pytest.raises(Unsupported):
        _is_ancestor(host, repository, ROOT, MERGE)
    assert InMemoryCodeHost().capabilities[Operation.IS_ANCESTOR] is Support.SUPPORTED
