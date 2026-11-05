"""Database/application compatibility window (#2300).

The planner, serve check, expand rollback, irreversible refusal, redacted
output, and crash/retry resume are the behavior this module pins. Production
code lives in ``curie_api.schema_compat``; this file never migrates by calling
``alembic upgrade head`` from an API pod startup path.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from _migration_support import ALEMBIC_DIR, IsolatedMigrationDb, alembic_config, sql_rows
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from curie_api import schema_compat
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.schema_compat import (
    KIND_CONTRACT,
    KIND_EXPAND,
    KIND_IRREVERSIBLE,
    AppWindow,
    apply_upgrade,
    assert_servable,
    can_serve,
    current_revision,
    load_kinds,
    load_window,
    plan_upgrade,
    render_decision,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

CONTRACT = "0041"
# @spec PROTECTED-HOOK-SOURCE-2 and DEPLOY-NOTICE-RELEASE-1.
# Agent reads require deploy notifications after the published source-control ledger,
# the Agent ORM and resolver read the tenant-scope columns of 0101 (#2911), and
# factory status replies read the typed reply target columns of 0102, and
# WorkItems are keyed by their tracker issue (0103 contract, #3831), and
# channel and WorkItem reads carry the trigger label of 0105 (#4361).
# The 0108 join requires both stable reply selectors (0106) and identity links (0107).
# The join requires both start diagnostics (0123) and attachment catalogs (0130).
# The API writes the twelve-hour execution deadlines only 0136 and 0140 admit (#5067).
# @spec AGENT-ROLE-PERSISTENCE-V1-9: policy ORM reads require the 0133 column.
# @spec HTTP-APPROVAL-CONTINUITY-5: approval ORM reads require the 0137 column.
# @spec HTTP-INVOCATION-EXECUTION-21: Grid reports write the 0139 namespace kind.
# Factory base transition ORM reads require the 0144 transition table.
# The 0145 join requires both the stable deadline backport (0140) and 0144.
