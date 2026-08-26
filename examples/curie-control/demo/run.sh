#!/usr/bin/env bash
# Boot a disposable control-plane and play the scripted conversation.
#
# Everything the demo shows is a real HTTP response from a real API against a
# real Postgres. The database is created and dropped here, so this never touches
# a shared dev database and is reproducible from a clean machine.
#
#   bash examples/curie-control/demo/run.sh [--fast]
#
# Requires: the compose Postgres up (`curie local up --minimal`), and `uv`.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

DB="curie_control_demo"
PORT="${CURIE_DEMO_PORT:-28099}"
API="http://localhost:${PORT}"
KEY="demo-platform-key"
PG="postgresql+asyncpg://postgres:postgres@localhost:25432/${DB}"

cleanup() {
  [[ -n "${API_PID:-}" ]] && kill "$API_PID" 2>/dev/null || true
  uv run python - <<PY 2>/dev/null || true
import asyncio, asyncpg
async def drop():
    c = await asyncpg.connect(user="postgres", password="postgres",
                              host="localhost", port=25432, database="postgres")
    await c.execute("DROP DATABASE IF EXISTS ${DB} WITH (FORCE)")
    await c.close()
asyncio.run(drop())
PY
}
trap cleanup EXIT

printf "booting a disposable control plane...\n"

uv run python - >/dev/null <<PY
import asyncio, os
import asyncpg
from alembic import command
from alembic.config import Config

async def reset():
    c = await asyncpg.connect(user="postgres", password="postgres",
                              host="localhost", port=25432, database="postgres")
    await c.execute("DROP DATABASE IF EXISTS ${DB} WITH (FORCE)")
    await c.execute("CREATE DATABASE ${DB}")
    await c.close()

asyncio.run(reset())
cfg = Config("apps/api/alembic.ini")
cfg.set_main_option("script_location", "apps/api/alembic")
cfg.set_main_option("sqlalchemy.url", "${PG}")
os.environ["DATABASE_URL"] = "${PG}"
command.upgrade(cfg, "head")
PY

DATABASE_URL="$PG" API_KEY="$KEY" CONTROL_AGENT="curie-control" \
  CONTROL_OPERATORS="U_ALEX" VALKEY_HOST=localhost VALKEY_PORT=26379 \
  VALKEY_PASSWORD="${VALKEY_PASSWORD:-valkeypass}" \
  S3_ENDPOINT_URL=http://localhost:29000 S3_ACCESS_KEY=rustfs S3_SECRET_KEY=rustfssecret \
  uv run uvicorn curie_api.main:app --port "$PORT" --log-level error >/dev/null 2>&1 &
API_PID=$!

for _ in $(seq 1 40); do
  curl -fsS -m 1 "$API/health" >/dev/null 2>&1 && break
  sleep 0.25
done

SEED="$(uv run python examples/curie-control/demo/seed.py --api "$API" --key "$KEY" 2>/dev/null)"
AGENT_ID="$(printf '%s' "$SEED" | uv run python -c 'import json,sys; print(json.load(sys.stdin)["agent_id"])')"
OLD_VERSION="$(printf '%s' "$SEED" | uv run python -c 'import json,sys; print(json.load(sys.stdin)["old_version"])')"

# Blank the setup noise so a recording opens on the conversation. Tolerant of
# an unset TERM (a pipe, CI) rather than failing the run over cosmetics.
clear 2>/dev/null || true
uv run python examples/curie-control/demo/chat_demo.py \
  --api "$API" --key "$KEY" \
  --agent-id "$AGENT_ID" --old-version "$OLD_VERSION" "$@"
