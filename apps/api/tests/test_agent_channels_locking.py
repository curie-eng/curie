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
  already superseded -- a lost update that the FOR UPDATE alone does not stop;
- a broken lock CYCLE answers 409 and not 500. Locking one agent's rows cannot
  order two callers moving GLOBALLY unique pairs in opposite directions, so
  Postgres aborts one of them with `40P01`;
- the duplicate-pair recovery runs inside the STILL-OPEN outer transaction. The
  savepoint is what keeps the row locks alive across the failed write, and a
  plain `session.rollback()` there would answer the 409 from an unlocked world.

Every one was verified to fail with its mechanism removed and pass with it.
"""

import asyncio
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

import asyncpg
from curie_api import crud
from curie_api.config import get_settings
from sqlalchemy import make_url
from sqlalchemy.ext.asyncio import AsyncSession

LOCK_BINDINGS = "SELECT id FROM curie.agent_channels WHERE agent_id = $1 FOR UPDATE"


def _connect_args() -> dict[str, Any]:
    url = make_url(get_settings().database_url)
    return {
        "user": url.username,
        "password": url.password,
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def _create_agent(client: Any, auth_headers: dict[str, str], name: str, address: str) -> str:
    created = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": address}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id: str = created.json()["id"]
    return agent_id


def test_a_patch_blocks_while_another_transaction_holds_the_row(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    created = client.post(
        "/agents",
        json={"name": "lockproof", "channel": {"kind": "slack", "address": "C0EXAMPLE5"}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    done: list[Any] = []

    def _move() -> None:
        done.append(
            client.patch(
                f"/agents/{agent_id}/channels",
                params={"kind": "slack", "address": "C0EXAMPLE5"},
                json={"kind": "slack", "address": "C0EXAMPLE6"},
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
        json={"name": "stalelock", "channel": {"kind": "slack", "address": "C0EXAMPLE7"}},
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = created.json()["id"]
    done: list[Any] = []

    def _move() -> None:
        done.append(
            client.patch(
                f"/agents/{agent_id}/channels",
                params={"kind": "slack", "address": "C0EXAMPLE7"},
                json={"kind": "slack", "address": "C0EXAMPLE8"},
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


def test_a_broken_lock_cycle_answers_a_retryable_409_not_a_500(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """`lock_agent_bindings` locks ONE agent's rows; the pair is GLOBALLY unique.

    Two callers moving their own agents onto each other's pairs therefore each
    hold a lock the other needs, and Postgres breaks the cycle by aborting one
    of them with `40P01`. Untranslated that reaches the caller as a 500 for a
    race it could simply retry.

    Made deterministic rather than raced, so it proves the translation on every
    run instead of on a lucky one:

    - a GATE connection holds the mover's binding rows, parking the handler in
      `lock_agent_bindings` at a known point;
    - a RIVAL connection takes the contested `(kind, address)` pair first and
      leaves it uncommitted, so the handler's own write must queue behind it;
    - the rival THEN queues behind the handler for the mover's rows. Postgres
      aborts whichever backend closes the cycle last, so the handler must be
      last -- which is why the gate exists at all, and why the rival's
      `deadlock_timeout` is raised out of the way.
    """

    mover = _create_agent(client, auth_headers, "cycle-mover", "C0EXAMPLE1")
    opponent = _create_agent(client, auth_headers, "cycle-opponent", "C0EXAMPLE2")
    contested = "C0EXAMPLE3"
    done: list[Any] = []

    def _move() -> None:
        done.append(
            client.patch(
                f"/agents/{mover}/channels",
                params={"kind": "slack", "address": "C0EXAMPLE1"},
                json={"kind": "slack", "address": contested},
                headers=auth_headers,
            )
        )

    async def run() -> None:
        gate = await asyncpg.connect(**_connect_args())
        rival = await asyncpg.connect(**_connect_args())
        try:
            gate_tx = gate.transaction()
            await gate_tx.start()
            await gate.fetch(LOCK_BINDINGS, mover)

            # Raised so the rival never runs the deadlock check itself: the
            # backend that detects the cycle is the one that dies, and this
            # test is about what the HANDLER does when it is the victim.
            await rival.execute("SET deadlock_timeout = '1h'")
            rival_tx = rival.transaction()
            await rival_tx.start()
            await rival.execute(
                "UPDATE curie.agent_channels SET address = $2 WHERE agent_id = $1",
                opponent,
                contested,
            )

            worker = threading.Thread(target=_move)
            worker.start()
            time.sleep(1.5)
            assert not done, "the handler did not park on the gate's lock"

            waiting = asyncio.create_task(rival.fetch(LOCK_BINDINGS, mover))
            await asyncio.sleep(1.5)
            assert not waiting.done(), "the rival did not queue behind the handler"

            await gate_tx.rollback()
            worker.join(timeout=30)
            await asyncio.wait_for(waiting, timeout=30)
            await rival_tx.rollback()
        finally:
            await gate.close()
            await rival.close()

        assert done, "the handler never returned: the cycle was never broken"
        assert done[0].status_code == 409, done[0].text
        assert "retry" in done[0].text, done[0].text
        # The write did not land: the victim's whole transaction was discarded.
        read = client.get(f"/agents/{mover}", headers=auth_headers)
        assert read.status_code == 200, read.text
        assert [b["address"] for b in read.json()["channels"]] == ["C0EXAMPLE1"]

    asyncio.run(run())


def test_the_conflict_lookup_runs_with_the_row_locks_still_held(
    client: Any, auth_headers: dict[str, str], clean_db: None, monkeypatch: Any
) -> None:
    """The SAVEPOINT recovery, asserted as lock state rather than as a status.

    A duplicate-pair write is answered by rolling the savepoint back and then
    asking who owns the pair, all inside the still-open outer transaction --
    which is the only reason the answer is trustworthy: the agent's rows are
    still locked, so ownership cannot move between the failed write and the
    sentence describing it. Swap `begin_nested` for a plain
    `session.rollback()` and every status-shaped assertion stays green while
    the locks are gone.

    So this one observes from a SECOND connection, at the one instant that
    distinguishes them: `FOR UPDATE NOWAIT` against the mover's rows, taken
    while the owner lookup runs. `crud.agent_id_for_pair` is wrapped, not
    replaced -- the real function still answers the request; the wrapper only
    marks the moment, because that moment is inside a handler and has no other
    external edge.
    """

    mover = _create_agent(client, auth_headers, "savepoint-mover", "C0EXAMPLE1")
    _create_agent(client, auth_headers, "savepoint-owner", "C0EXAMPLE2")
    observed: list[str] = []
    real: Callable[..., Any] = crud.agent_id_for_pair

    async def probe_then_answer(
        session: AsyncSession, kind: str, address: str
    ) -> uuid.UUID | None:
        probe = await asyncpg.connect(**_connect_args())
        try:
            await probe.fetch(f"{LOCK_BINDINGS} NOWAIT", mover)
            observed.append("released")
        except asyncpg.exceptions.LockNotAvailableError:
            observed.append("held")
        finally:
            await probe.close()
        answer: uuid.UUID | None = await real(session, kind, address)
        return answer

    monkeypatch.setattr(crud, "agent_id_for_pair", probe_then_answer)

    refused = client.patch(
        f"/agents/{mover}/channels",
        params={"kind": "slack", "address": "C0EXAMPLE1"},
        json={"kind": "slack", "address": "C0EXAMPLE2"},
        headers=auth_headers,
    )

    assert refused.status_code == 409, refused.text
    assert observed == ["held"], (
        f"the owner lookup ran with the mover's rows {observed}: the failed "
        "write was recovered by ending the outer transaction, not by rolling "
        "back to the savepoint, so the 409 describes a world that was free to "
        "move between the conflict and the answer"
    )
