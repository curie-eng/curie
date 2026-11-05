"""Installed schema serving owner, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import importlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[3]
RESOURCE = Path("packages/protected-hooks/src/curie_protected_hooks/schema_serving.json")


def shared() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    assert importlib.util.find_spec("curie_protected_hooks.schema_serving") is not None, (
        "SOURCE-2 installed application-independent serving decision is missing"
    )
    return importlib.import_module("curie_protected_hooks.schema_serving")


def graph_payload() -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    script = ScriptDirectory(str(ROOT / "apps/api/alembic"))
    candidate = json.loads((ROOT / "cli/src/application_schema_windows.json").read_text())[
        "candidate"
    ]
    graph = {}
    for revision in script.walk_revisions():
        parent = revision.down_revision
        graph[revision.revision] = (
            list(parent) if isinstance(parent, (list, tuple)) else ([parent] if parent else [])
        )
    return dict(**candidate, revision_parents=graph)


@pytest.mark.parametrize(
    "current,expected",
    [
        (None, False),
        ("0075", False),
        ("0076", False),
        ("0077", True),
        # #2911: 0101 tenant-scopes the core tables the app reads, so the
        # revisions below it are refused. #4397's trigger label expand (0105)
        # is the minimum: this image reads its columns, so 0103 is refused.
        ("0082", False),
        ("0100", False),
        ("0101", False),
        ("0102", False),
        ("0103", False),
        ("0108", False),
        ("0122", False),
        ("0123", False),
        ("0130", False),
        # @spec HTTP-PRINCIPAL-ADOPTION-V1-2/9: authority ORM reads require 0132.
        ("0131", False),
        ("0136", False),
        # @spec AGENT-ROLE-PERSISTENCE-V1-9: policy ORM requires 0133.
        ("0132", False),
        # @spec HTTP-APPROVAL-CONTINUITY-5: approval ORM requires 0137.
        ("0133", False),
        # @spec HTTP-INVOCATION-EXECUTION-21: Grid reports write the 0139 namespace kind.
        ("0137", False),
        # @spec AUTOMATED-REMEDIATION-15: 0138 only widens a CHECK below the floor.
        ("0138", False),
        ("0139", False),
        # #5067: the stable deadline backport alone has none of the feature tables.
        ("0140", False),
        ("0141", False),
        ("0143", False),
