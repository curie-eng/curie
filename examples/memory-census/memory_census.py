"""@spec MC001: read-only metadata snapshot; see docs/CONTRACT.md."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

# @spec MC001 c1-c3
QUERY = sa.text("""
SELECT coalesce(a.name, '<gone>') AS agent, s.key,
       octet_length(s.value::text) AS bytes
FROM curie.workflow_state_entries s
LEFT JOIN curie.agents a ON a.id = s.agent_id
WHERE s.namespace = 'memory'
ORDER BY agent, s.key
""")


async def main() -> None:
    """@spec MC001 c1-c4; buffer all metadata before publishing the snapshot."""
    dsn = os.environ["DATABASE_URL"].strip()
    if not dsn:
        raise ValueError("missing database configuration")
    if dsn.startswith("postgresql://"):
        dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(dsn)
    try:
        async with engine.connect() as connection:
            rows = (await connection.execute(QUERY)).fetchall()
    finally:
        await engine.dispose()
    counts: Counter[str] = Counter()
    snapshot = []
    for agent, key, byte_count in rows:
        counts[agent] += 1
        snapshot.append({"memory_census": "entry", "agent": agent, "key": key, "bytes": byte_count})
    snapshot.extend(
        {"memory_census": "agent", "agent": agent, "rows": counts[agent]}
        for agent in sorted(counts)
    )
    snapshot.append({"memory_census": "total", "rows": len(rows), "agents": len(counts)})
    output = "\n".join(json.dumps(row) for row in snapshot)
    print(output)


def cli() -> int:
    """@spec MC001 c4: never expose a database exception message or URL."""
    try:
        asyncio.run(main())
    except Exception:  # noqa: BLE001 - fixed diagnostics at the executable boundary
        error = (
            "collection_error"
            if os.environ.get("DATABASE_URL", "").strip()
            else "configuration_error"
        )
        print(json.dumps({"error": error}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(cli())
