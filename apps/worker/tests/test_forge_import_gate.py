"""The worker holds no forge code (ADR 0197, "Two ports" item 6, #3831).

No worker module may import ``curie_api.forges`` at all, ports included, nor
carry a GitHub REST literal: pull request, branch and commit reads move behind
internal API endpoints and the worker receives a credential and origin as data.

``ALLOWLIST`` holds exactly today's offenders and may only shrink: a listed
file that no longer offends fails the gate until its entry is removed.
"""

from __future__ import annotations

from pathlib import Path

from curie_test_support.forge_imports import scan_source, scan_tree, worker_import_rule

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKER_SOURCE = REPO_ROOT / "apps" / "worker" / "src"

ALLOWLIST: frozenset[str] = frozenset(
    {
        "apps/worker/src/curie_worker/config.py",
        "apps/worker/src/curie_worker/publication_clients.py",
        "apps/worker/src/curie_worker/publication_k8s.py",
    }
)


def _offenders() -> dict[str, list[str]]:
    found = scan_tree(REPO_ROOT, WORKER_SOURCE, forbidden_import=worker_import_rule)
    return {
        path: [f"{offence.line}: {offence.reason}" for offence in offences]
        for path, offences in found.items()
    }


def test_no_new_worker_module_reaches_a_forge() -> None:
    new = {path: lines for path, lines in _offenders().items() if path not in ALLOWLIST}
    assert new == {}, "the worker asks the API; it imports no forge code"


def test_the_allowlist_holds_no_entry_that_no_longer_offends() -> None:
    stale = sorted(ALLOWLIST - set(_offenders()))
    assert stale == [], "remove these from ALLOWLIST; it may only shrink"


def _scan(source: str) -> list[str]:
    return [
        offence.reason
        for offence in scan_source(
            source,
            path="example.py",
            module="curie_worker.example",
            forbidden_import=worker_import_rule,
        )
    ]


def test_the_scanner_refuses_even_the_ports_in_the_worker() -> None:
    assert _scan("from curie_api.forges import ports\n") == ["imports curie_api.forges"]
    assert _scan("import curie_api.forges.types\n") == ["imports curie_api.forges.types"]
    assert _scan('API = "https://api.github.com"\n') == ["GitHub literal 'api.github.com'"]


def test_the_scanner_passes_other_api_imports() -> None:
    assert _scan("from curie_api.config import Settings\nimport httpx\n") == []
