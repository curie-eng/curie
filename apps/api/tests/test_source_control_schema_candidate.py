"""Ledger candidate prerequisite, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import json
import re
import tomllib
import uuid
from pathlib import Path

import pytest
import yaml
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.schema_compat import current_revision
from curie_protected_hooks.schema_serving import load_window
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
CATALOG = ROOT / "cli/src/application_schema_windows.json"


def catalog() -> dict:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    return json.loads(CATALOG.read_text())


@pytest.mark.parametrize("resource", ["api", "cli"])
def test_candidate_requires_deploy_notices_after_ledger_schema(resource: str) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    window = (
        json.loads((ROOT / "apps/api/src/curie_api/schema_compat.json").read_text())
        if resource == "api"
        else catalog()["candidate"]
    )
    # The next-only migrations follow the immutable published 0.12.2 chain.
    # The minimum tracks the newest schema the app reads (#2911's tenant scope);
    # The join requires both start diagnostics (0123) and attachment catalogs (0130).
    # @spec AGENT-ROLE-PERSISTENCE-V1-9: policy ORM reads require 0133.
    # @spec HTTP-APPROVAL-CONTINUITY-5: approval ORM reads require the 0137 column.
    # @spec AUTOMATED-REMEDIATION-15: 0138 only widens a CHECK.
    # @spec HTTP-INVOCATION-EXECUTION-21: Grid reports write the 0139 namespace kind.
    # Workspace recovery reads require the 0141 outcome ledger.
    # The 0145 join requires both the stable deadline backport (0140) and 0144.
