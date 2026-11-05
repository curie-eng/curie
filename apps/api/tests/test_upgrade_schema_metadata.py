"""The chart carries a generated copy of the API's authoritative schema graph."""

import hashlib
import json
import shutil
import uuid
from pathlib import Path

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    run_script,
    sql_rows,
    stamped_revision,
)
from alembic import command
from alembic.script import ScriptDirectory

REPO = Path(__file__).resolve().parents[3]
ARTIFACT = Path("charts/curie/files/schema-compat.json")


def test_packaged_schema_metadata_matches_api_authority() -> None:
    metadata = json.loads((REPO / ARTIFACT).read_text())
    api = REPO / "apps/api/src/curie_api"
    window = json.loads((api / "schema_compat.json").read_text())
    kinds = json.loads((api / "revision_kinds.json").read_text())
