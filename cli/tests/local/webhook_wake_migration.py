"""Exercise the installed migration assets against an owned database."""

import json
import os
import subprocess
import uuid
from urllib.parse import quote

import asyncpg
from curie_api.config import get_settings
from sqlalchemy.engine import make_url


async def qualify():
    current = make_url(get_settings().database_url)
    name = "test_adr0274_wake_migration"
    admin = await asyncpg.connect(
        current.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    created = False
    database = None
    try:
        if await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name):
            raise RuntimeError("migration database already exists; preserve its owner")
        await admin.execute(f'CREATE DATABASE "{name}"')
        created = True
        target = current.set(database=name)
        env = {**os.environ, "DATABASE_URL": target.render_as_string(hide_password=False)}

        def migrate(action, revision, *, refused=False):
            result = subprocess.run(
                ["python3", "-m", "alembic", "-c", "/app/alembic.ini", action, revision],
                env=env,
                cwd="/app",
                capture_output=True,
                text=True,
                timeout=180,
            )
            detail = result.stderr.replace(
                target.render_as_string(hide_password=False), "[database]"
            )
            if target.password:
                detail = detail.replace(target.password, "[credential]")
                detail = detail.replace(quote(target.password, safe=""), "[credential]")
            detail = "\n".join(detail.splitlines()[-20:])
            if refused:
                if result.returncode == 0:
                    raise AssertionError("installed migration allowed a lossy downgrade")
                reason = "cannot downgrade while webhook hook runs or wake keys exist"
                if reason not in result.stderr:
                    raise RuntimeError(f"installed downgrade failed for another reason: {detail}")
            elif result.returncode:
                raise RuntimeError(f"installed migration {action} {revision} failed: {detail}")

        migrate("upgrade", "0148")
        database = await asyncpg.connect(
            target.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        agent, version = uuid.uuid4(), uuid.uuid4()
        await database.execute(
            "INSERT INTO curie.agents(id,name) VALUES($1,'acme-wake-migration')", agent
        )
        await database.execute(
            "INSERT INTO curie.agent_versions(id,agent_id,version_label,created_by) "
            "VALUES($1,$2,'v1','qualification')",
            version,
            agent,
        )

        async def insert(source):
            row = uuid.uuid4()
            await database.execute(
                "INSERT INTO curie.hook_runs "
                "(id,agent_id,name,slot_utc,version_id,source,outcome,started_at,ended_at) "
                "VALUES($1,$2,$3,clock_timestamp(),$4,$3,'ran',now(),now())",
                row,
                agent,
                source,
                version,
            )
            return row

        await insert("schedule")
        await insert("manual")
        history = "SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.hook_runs r"
        before = await database.fetchval(history)
        migrate("upgrade", "0149")
        assert (
            await database.fetchval(
                "SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.hook_runs r"
            )
            == before
        )
        webhook = await insert("webhook")
        migrate("downgrade", "0148", refused=True)
        assert await database.fetchval("SELECT version_num FROM curie.alembic_version") == "0149"
        assert (
            await database.fetchval("SELECT source FROM curie.hook_runs WHERE id=$1", webhook)
            == "webhook"
        )
        await database.execute("DELETE FROM curie.hook_runs WHERE id=$1", webhook)
        await database.execute(
            "INSERT INTO curie.hook_wake_keys(agent_id,hook) VALUES($1,'queue-ready')", agent
        )
        migrate("downgrade", "0148", refused=True)
        await database.execute("DELETE FROM curie.hook_wake_keys")
        migrate("downgrade", "0148")
        assert (
            await database.fetchval(
                "SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM curie.hook_runs r"
            )
            == before
        )
        print(
            json.dumps(
                {
                    "history_preserved": True,
                    "webhook_accepted": True,
                    "lossy_downgrades_refused": 2,
                    "clean_downgrade": "0148",
                }
            )
        )
    finally:
        if database is not None:
            await database.close()
        if created:
            await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
            assert not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", name)
        await admin.close()
