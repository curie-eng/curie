"""Grid organisation namespace schema candidate, @spec HTTP-INVOCATION-EXECUTION-21."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "apps/api/alembic/versions/0139_slack_enterprise_namespaces.py"
