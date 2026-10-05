"""The API reaches forges only through the ports (ADR 0197, #3831).

Outside ``apps/api/src/curie_api/forges`` no module may import an adapter
package (``curie_api.forges.<kind>``) or carry a GitHub REST literal. The
ports, value types and in-memory adapters are importable from anywhere.

``ALLOWLIST`` holds exactly today's offenders. It may only shrink: a listed
file that no longer offends fails the gate until its entry is removed, and the
#3831 acceptance criterion is an empty list.
"""

from __future__ import annotations

from pathlib import Path

from curie_test_support.forge_imports import (
    adapter_packages,
    api_import_rule,
    scan_source,
    scan_tree,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
API_SOURCE = REPO_ROOT / "apps" / "api" / "src"
FORGES = API_SOURCE / "curie_api" / "forges"

ALLOWLIST: frozenset[str] = frozenset(
    {
        "apps/api/src/curie_api/commitpoller.py",
        "apps/api/src/curie_api/config.py",
        "apps/api/src/curie_api/factory_base.py",
        "apps/api/src/curie_api/factory_ci.py",
        "apps/api/src/curie_api/factory_label_reconcile.py",
        "apps/api/src/curie_api/factory_notices.py",
        "apps/api/src/curie_api/factory_poll_intake.py",
        "apps/api/src/curie_api/github_app.py",
        "apps/api/src/curie_api/github_checks.py",
        "apps/api/src/curie_api/github_factory.py",
        "apps/api/src/curie_api/github_factory_events.py",
        "apps/api/src/curie_api/github_factory_review.py",
        "apps/api/src/curie_api/github_review_store.py",
        "apps/api/src/curie_api/github_review_truth.py",
        "apps/api/src/curie_api/issue_read.py",
        "apps/api/src/curie_api/publication_authority.py",
        "apps/api/src/curie_api/publication_truth.py",
        "apps/api/src/curie_api/routers/publications.py",
        "apps/api/src/curie_api/routers/work_item_outcomes.py",
        "apps/api/src/curie_api/workitems/lifecycle.py",
    }
)


def _offenders() -> dict[str, list[str]]:
    found = scan_tree(
        REPO_ROOT,
        API_SOURCE,
        forbidden_import=api_import_rule(adapter_packages(FORGES)),
        skip=lambda path: FORGES in path.parents,
    )
    return {
        path: [f"{offence.line}: {offence.reason}" for offence in offences]
        for path, offences in found.items()
    }


def test_no_new_module_reaches_a_forge_adapter() -> None:
    offenders = _offenders()
    new = {path: lines for path, lines in offenders.items() if path not in ALLOWLIST}
    assert new == {}, "use the ports in curie_api.forges instead"


def test_the_allowlist_holds_no_entry_that_no_longer_offends() -> None:
    stale = sorted(ALLOWLIST - set(_offenders()))
    assert stale == [], "remove these from ALLOWLIST; it may only shrink"


def test_the_adapter_packages_are_found() -> None:
    # If this set were empty the import rule would forbid nothing.
    assert "github" in adapter_packages(FORGES)


def _scan(source: str, module: str = "curie_api.example") -> list[str]:
    rule = api_import_rule(frozenset({"github"}))
    return [
        offence.reason
        for offence in scan_source(source, path="example.py", module=module, forbidden_import=rule)
    ]


def test_the_scanner_catches_each_offence_form() -> None:
    assert _scan("import curie_api.forges.github.ci\n") == ["imports curie_api.forges.github.ci"]
    assert _scan("from curie_api.forges import github\n") == ["imports curie_api.forges.github"]
    # A relative import resolves against the scanned module's package.
    assert _scan(
        "from ..forges.github.transport import github_headers\n",
        module="curie_api.routers.example",
    ) == ["imports curie_api.forges.github.transport"]
    assert _scan('BASE = "https://api.github.com"\n') == [
        "GitHub literal 'api.github.com'",
    ]
    assert _scan('H = {"X-GitHub-Api-Version": "2022-11-28"}\n') == [
        "GitHub literal 'x-github-api-version'"
    ]
    assert _scan('def f(n):\n    return f"https://github.com/factory/label/{n}"\n') == [
        "GitHub literal 'https://github.com'"
    ]
    assert _scan('ACCEPT = "application/vnd.github+json"\n') == [
        "GitHub literal 'application/vnd.github'"
    ]


def test_the_scanner_passes_the_ports_and_prose() -> None:
    clean = (
        '"""Mentions https://github.com/curie-eng/curie/issues/3831 in prose."""\n'
        "from curie_api.forges import capabilities, ports, types\n"
        "from curie_api.forges.identity import notice_request_id\n"
        "from curie_api.forges.memory import InMemoryTracker\n"
        "# https://api.github.com in a comment is not code\n"
        'DOCS = "https://docs.github.com/en/webhooks"\n'
    )
    assert _scan(clean) == []
