"""The binding subresource's row lock, proven DETERMINISTICALLY (#1525).

`test_agent_channels_subresource.py` races two app instances, which is the right
shape for the invariants it asserts but leaves the lock itself only probabilis-
tically covered: deleting `.with_for_update()` from `crud.lock_agent_bindings`
keeps every one of those tests green, because the losing request rarely lands
inside the sub-millisecond window. These two hold the contended state open with
a second connection instead, so the mechanism is exercised on every run:

- the handler BLOCKS while another transaction holds its binding rows;
- the locked read returns the row as it is once the lock is granted, not the
  copy the session loaded before it waited. Without
  `execution_options(populate_existing=True)` the ORM hands back the stale
  identity-map object and `generation += 1` is computed from a value the winner
  already superseded -- a lost update that the FOR UPDATE alone does not stop.

Both were verified to fail with their mechanism removed and pass with it.
"""

import asyncio
import threading
import time
from typing import Any

import asyncpg
from curie_api.config import get_settings
from sqlalchemy import make_url


def _connect_args() -> dict[str, Any]:
    url = make_url(get_settings().database_url)
    return {
        "user": url.username,
        "password": url.password,
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def test_a_patch_blocks_while_another_transaction_holds_the_row(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    created = client.post(
        "/agents",
        json={"name": "lockproof", "channel": {"kind": "slack", "address": "C0LOCKED01"}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    done: list[Any] = []

    def _move() -> None:
        done.append(
            client.patch(
                f"/agents/{agent_id}/channels",
                params={"kind": "slack", "address": "C0LOCKED01"},
                json={"kind": "slack", "address": "C0LOCKED02"},
                headers=auth_headers,
            )
        )

    async def run() -> None:
        conn = await asyncpg.connect(**_connect_args())
        tx = conn.transaction()
        await tx.start()
        await conn.fetch(
            "SELECT id FROM curie.agent_channels WHERE agent_id = $1 FOR UPDATE",
            agent_id,
        )
        worker = threading.Thread(target=_move)
        worker.start()
        time.sleep(2.0)
        assert not done, "the handler did NOT block: it is not taking the row lock"
        await tx.rollback()
        await conn.close()
        worker.join(timeout=10)
        assert done and done[0].status_code == 200, done

    asyncio.run(run())


def test_the_locked_read_sees_a_generation_written_while_it_waited(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    created = client.post(
        "/agents",
        json={"name": "stalelock", "channel": {"kind": "slack", "address": "C0STALE001"}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    done: list[Any] = []

    def _move() -> None:
        done.append(
            client.patch(
                f"/agents/{agent_id}/channels",
                params={"kind": "slack", "address": "C0STALE001"},
                json={"kind": "slack", "address": "C0STALE002"},
                headers=auth_headers,
            )
        )

    async def run() -> None:
        conn = await asyncpg.connect(**_connect_args())
        tx = conn.transaction()
        await tx.start()
        await conn.fetch(
            "SELECT id FROM curie.agent_channels WHERE agent_id = $1 FOR UPDATE",
            agent_id,
        )
        worker = threading.Thread(target=_move)
        worker.start()
        # The handler has read the agent (generation 0 into its identity map)
        # and is now waiting on the lock.
        time.sleep(1.5)
        assert not done, done
        await conn.execute(
            "UPDATE curie.agent_channels SET generation = 5 WHERE agent_id = $1",
            agent_id,
        )
        await tx.commit()
        await conn.close()
        worker.join(timeout=10)
        assert done and done[0].status_code == 200, done

        check = await asyncpg.connect(**_connect_args())
        generation = await check.fetchval(
            "SELECT generation FROM curie.agent_channels WHERE agent_id = $1", agent_id
        )
        await check.close()
        assert generation == 6, (
            f"generation is {generation}: the locked read handed back the STALE "
            "identity-map row, so the increment was computed from a value the "
            "winner had already superseded"
        )

    asyncio.run(run())
