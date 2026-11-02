# Local verification reference

Run these from the repo root unless noted. CI (`.github/workflows/ci.yaml`) runs
the same commands.

**Python (all packages, from root):**
Run this local Python CI baseline. For concurrent jobs, first apply
[Concurrent local verification](#concurrent-local-verification) below: this
block is the default port reference, not a requirement to share its resources.
Do not use `--profile full` for startup because it can start stale application images.
Set `base=next` when your worktree targets `next`.

```bash
(
  set -e
  base=${base:-main}
  if [[ -n "${COMPOSE_FILE:-}" && -z "${COMPOSE_PROJECT_NAME:-}" ]]; then
    echo "Set COMPOSE_PROJECT_NAME with a custom COMPOSE_FILE" >&2
    exit 2
  fi
  if [[ -z "${COMPOSE_PROJECT_NAME:-}" ]]; then
    export COMPOSE_PROJECT_NAME=curie-implement-baseline
    baseline_default=1
  else
    baseline_default=0
  fi
  compose_base="$PWD/compose.dev.yaml"
  compose_file_list=${COMPOSE_FILE:-$compose_base}
  IFS=: read -r -a compose_files <<< "$compose_file_list"
  compose=(docker compose -p "$COMPOSE_PROJECT_NAME")
  for compose_file in "${compose_files[@]}"; do
    [[ "$compose_file" = /* && -f "$compose_file" ]] || {
      echo "COMPOSE_FILE must contain existing absolute paths" >&2
      exit 2
    }
    compose+=(-f "$compose_file")
  done
  if (( baseline_default )); then
    baseline_lock=/tmp/curie-implement-baseline.lock
  else
    [[ "$COMPOSE_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || {
      echo "COMPOSE_PROJECT_NAME is not safe for a lock path" >&2
      exit 2
    }
    baseline_lock="/tmp/curie-implement-${COMPOSE_PROJECT_NAME}.lock"
    : "${CURIE_RELEASED_UPGRADE_POSTGRES_HOST:?set the isolated Postgres host}"
    : "${CURIE_RELEASED_UPGRADE_POSTGRES_PORT:?set the isolated Postgres port}"
    export CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT="$COMPOSE_PROJECT_NAME"
    export CURIE_RELEASED_UPGRADE_COMPOSE_FILES="$compose_file_list"
    export CURIE_RELEASED_UPGRADE_INTEGRATION=1
    export TEST_DATABASE_URL="postgresql+asyncpg://postgres:postgres@${CURIE_RELEASED_UPGRADE_POSTGRES_HOST}:${CURIE_RELEASED_UPGRADE_POSTGRES_PORT}/postgres"
    export DATABASE_URL="$TEST_DATABASE_URL"
  fi
  export TEST_VALKEY_HOST=${TEST_VALKEY_HOST:-127.0.0.1}
  export TEST_VALKEY_PORT=${TEST_VALKEY_PORT:-26379}
  export VALKEY_HOST="$TEST_VALKEY_HOST"
  export VALKEY_PORT="$TEST_VALKEY_PORT"
  export TEST_S3_ENDPOINT_URL=${TEST_S3_ENDPOINT_URL:-http://127.0.0.1:29000}
  export S3_ENDPOINT_URL="$TEST_S3_ENDPOINT_URL"
  export TEST_LANGFUSE_HOST=${TEST_LANGFUSE_HOST:-${LANGFUSE_HOST:-http://127.0.0.1:23000}}
  export LANGFUSE_HOST="$TEST_LANGFUSE_HOST"
  export TEST_OTEL_COLLECTOR_ENDPOINT=${TEST_OTEL_COLLECTOR_ENDPOINT:-http://127.0.0.1:24318/v1/traces}
  exec 9>"$baseline_lock"
  if ! flock -n 9; then
    echo "Another local Python CI baseline is already running"
    exit 1
  fi
  wire_lock=$(mktemp /tmp/curie-implement-baseline-wire.lock.XXXXXX)
  cleanup() {
    "${compose[@]}" --profile full down -v
    rm -f "$wire_lock"
  }
  trap cleanup EXIT
  uv lock --check
  uv sync
  uv run python scripts/check-alembic-revisions.py
  case "$base" in
    main) other_train=next ;;
    next) other_train=main ;;
    *) other_train= ;;
  esac
  if [[ -n "$other_train" ]]; then
    git fetch --no-tags origin "$other_train"
    uv run python scripts/check-alembic-revisions.py --other-train-ref "origin/$other_train"
  else
    echo "cross-train lineage skipped: base $base is not a train branch"
  fi
  uv run python scripts/check-schema-window.py
  uv run python scripts/check-contract-migration-timing.py
  uv run ruff check .
  uv run mypy
  uv run lint-imports
  bash scripts/check-docs.sh
  bash scripts/check-wire-tolerance.sh
  COMPOSE_PROJECT_NAME="$COMPOSE_PROJECT_NAME" COMPOSE_FILE="$compose_file_list" \
    python3 scripts/wait-for-langfuse.py --start --timeout-seconds 480
  git fetch --force --tags origin refs/heads/main:refs/remotes/origin/main
  uv run python scripts/check-released-upgrade.py --self-test
  uv run python scripts/check-released-upgrade.py
  (cd apps/api && uv run alembic upgrade head)
  git fetch --no-tags origin "$base" || true
  git show "origin/$base:packages/aci-protocol/schema/wire.lock" > "$wire_lock" 2>/dev/null || true
  uv run python -m aci_protocol.wire_lock --check-base "$wire_lock"
  uv run pytest -q
)
```

Linked worktrees share their repository's Git shallow metadata. Never use a
shallow fetch in this checkout or one of its linked worktrees; if a shallow
fetch is needed, use an independent clone instead.

Occupied default ports are a reason to isolate the job, not by themselves a
failed precondition. Preserve the existing owner and follow the instructions
below before deciding that local verification is blocked.

### Concurrent local verification

This applies to every local implementation job, including Bonus Drain. Agents
are authorized to create disposable Compose overrides, endpoint files and test
launchers inside their own worktree without asking permission. Keep them under
the gitignored `.projects/` directory. Do not add infrastructure features to the
product just to run a job. Direct Compose invocation for this disposable setup
is an exception to the CLI entry point guidance in `CLAUDE.md`.

1. Choose a unique Compose project for this execution, such as
   `curie-check-<issue>-<run-suffix>`. Inventory running containers, published
   ports and network ownership first. Use the same project, absolute base
   Compose path and override files for config, startup, exec, logs and teardown.
   A project name alone does not change published ports or explicitly named
   networks.
2. Generate a private override for every service you will start. Replace each
   published port list with `ports: !override [...]`, binding to `127.0.0.1`.
   This requires Compose 2.24.4 or later; see the
   [Compose merge reference](https://docs.docker.com/reference/compose-file/merge/#replace-value).
   Plain Compose list merging can retain the original occupied binding. Either
   choose unused custom ports and retry allocation on a bind collision, or let
   Docker allocate ports and discover them with `docker compose port` before
   starting consumers. Do not assume an unused port probe reserves the port.
   Keep chosen ports below 49152: short-lived local listeners take ports in
   the macOS ephemeral range (49152-65535), and one that lands on the local
   reply stub port (`CURIE_LOCAL_STUB_PORT`) makes `curie local message`
   refuse to start (#4823).
   For a full Python baseline, replace every published backing service port:

   ```yaml
   services:
     postgres:
       ports: !override ["127.0.0.1:35432:5432"]
     valkey:
       ports: !override ["127.0.0.1:36379:6379"]
     clickhouse:
       ports: !override ["127.0.0.1:38123:8123", "127.0.0.1:39009:9000"]
     rustfs:
       ports: !override ["127.0.0.1:39000:9000", "127.0.0.1:39001:9001"]
     langfuse-web:
       ports: !override ["127.0.0.1:33000:3000"]
     otel-collector:
       ports: !override ["127.0.0.1:34317:4317", "127.0.0.1:34318:4318", "127.0.0.1:38888:8888"]
   networks:
     curie_runner:
       name: "${COMPOSE_PROJECT_NAME}_runner"
   ```

   These example ports must also be checked for occupancy. Inspect the resolved
   `docker compose -p "$COMPOSE_PROJECT_NAME" -f compose.dev.yaml -f <override> config`
   before startup; confirm that no enabled service retains a shared binding and
   the runner network belongs to this project.
3. Point every host consumer at the chosen endpoints. Inspect the actual test
   fixtures and settings: common variables include `DATABASE_URL`,
   `TEST_DATABASE_URL`, `VALKEY_HOST`, `VALKEY_PORT`, `TEST_VALKEY_PORT`,
   `S3_ENDPOINT_URL`, `TEST_S3_ENDPOINT_URL`, `LANGFUSE_HOST`, `CURIE_API_URL`,
   `CURIE_API_TARGET` and `PW_PORT`. Export only settings the consumer reads.
   Bridge containers keep their internal service addresses. The host network
   worker needs explicit override entries for its database, Valkey, S3, API
   and worker OTel endpoints, plus `CURIE_DOCKER_NETWORK` matching the private
   runner network. Shell exports do not replace literal Compose environment
   entries. Use private image tags when building or running workers. The Docker
   substrate copies validated bundle, workspace, and attachment inputs through
   the Docker API into claim-owned named volumes. Workspaces and temporary
   directories need no shared filesystem or matching daemon-host paths.
4. Export the full isolated contract before the baseline. The baseline consumes
   `COMPOSE_PROJECT_NAME` and `COMPOSE_FILE`, keeps the default shared lock only
   when it selected the default project itself, and uses a project scoped lock
   for every caller supplied project. It passes the same Compose files and
   project to the upgrade gate through
   `CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT` and
   `CURIE_RELEASED_UPGRADE_COMPOSE_FILES`; supply the matching loopback
   `CURIE_RELEASED_UPGRADE_POSTGRES_HOST` and
   `CURIE_RELEASED_UPGRADE_POSTGRES_PORT`. Also export matching
   `TEST_VALKEY_HOST`, `TEST_VALKEY_PORT`, `TEST_S3_ENDPOINT_URL`,
   `TEST_LANGFUSE_HOST`, and `TEST_OTEL_COLLECTOR_ENDPOINT`. The baseline derives
   runtime consumer variables from those values, and the readiness helper reads
   `TEST_LANGFUSE_HOST` for its web endpoint.
   For example:

   ```bash
   export COMPOSE_PROJECT_NAME=curie-check-2751-a
   export COMPOSE_FILE="$PWD/compose.dev.yaml:$PWD/.projects/2751/compose.yaml"
   export CURIE_RELEASED_UPGRADE_POSTGRES_HOST=127.0.0.1
   export CURIE_RELEASED_UPGRADE_POSTGRES_PORT=35432
   export TEST_VALKEY_PORT=36379
   export TEST_S3_ENDPOINT_URL=http://127.0.0.1:39000
   export TEST_LANGFUSE_HOST=http://127.0.0.1:33000
   export TEST_OTEL_COLLECTOR_ENDPOINT=http://127.0.0.1:34318/v1/traces
   ```

   Record resolved mappings, candidate identity, and exact commands without
   credential values. The gate rejects incomplete or mismatched configuration
   before database mutation. Keep all baseline assertions unchanged.
5. Do not silently replace a required ladder with focused tests or weaken a
   gate. Preserve other jobs and run each cleanup with its exact Compose array.
   The Python baseline, released upgrade gate, CLI local lifecycle, connectors,
   and E2E ladder all consume the same isolation contract: `COMPOSE_PROJECT_NAME`,
   ordered `COMPOSE_FILE` (candidate `compose.dev.yaml` then a private override),
   and matching host endpoints. Do not force project `curie` or a single
   `compose.dev.yaml` when those values are set. Example local CLI / ladder run:

   ```bash
   export COMPOSE_PROJECT_NAME=curie-check-2780-a
   export COMPOSE_FILE="$PWD/compose.dev.yaml:$PWD/.projects/2780/compose.yaml"
   export CURIE_API_URL=http://127.0.0.1:38000
   export VALKEY_HOST=127.0.0.1 VALKEY_PORT=36379
   export S3_ENDPOINT_URL=http://127.0.0.1:39000
   export CURIE_DOCKER_NETWORK=curie-check-2780-a_runner
   export CURIE_LOCAL_STUB_PORT=18155
   export CURIE_LOCAL_POSTGRES_HOST=127.0.0.1 CURIE_LOCAL_POSTGRES_PORT=35432
   export CURIE_LOCAL_IMAGE_TAG=dev-2780-a
   export CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:34318
   export CURIE_LOCAL_OTEL_METRICS_URL=http://127.0.0.1:38888/metrics
   curie local up --build
   CURIE_E2E_TIERS=local curie dev e2e-ladder
   curie local down
   ```

   Incomplete isolation is a usage error. Teardown uses the same project, files,
   and labels; never a global `curie*` name sweep.
6. Install cleanup before startup and remove only this execution's containers,
   networks, volumes and spawned runners. Identify resources by the project
   label and recorded container IDs, never a global `curie*` name sweep. On a
   partial startup failure, clean the owned partial stack before reallocating.
   Verify that owned resources are gone and other jobs remain running.

Existing task contracts still govern scope and required evidence. A running job
or older worktree does not automatically acquire newer instructions; read this
section from the updated base when resuming. Do not edit a live claimed Bonus
Drain task or launch a second copy to apply the guidance.


## The dev stack: compose.dev.yaml

The compose stack now has two profiles. `full` brings up the whole backing
stack (Postgres + Valkey + Langfuse v3 + ClickHouse + RustFS + OTel Collector).
`core` brings up the smaller local product loop (Postgres + Valkey + RustFS +
API + worker). Every backend integration test and UI E2E runs against `full`.

```bash
docker compose --profile full -f compose.dev.yaml up -d   # full stack
OTEL_EXPORTER_OTLP_ENDPOINT= docker compose --profile core -f compose.dev.yaml up -d   # 7-service minimal stack (no Langfuse/ClickHouse/OTel/UI); blank endpoint avoids a DNS retry against the absent otel-collector
docker compose -f compose.dev.yaml ps        # check health
docker compose -f compose.dev.yaml down      # stop, KEEP volumes (fast restart)
docker compose -f compose.dev.yaml down -v   # stop and WIPE volumes (throwaway)
```

**Clean up after yourself: this is not optional.** If you bring the local
stack up, you MUST take it down when you are done. This box does not have the
RAM to leave the full stack idling, and this keeps happening: stacks get left
running across sessions and starve the machine. Before you end a session in
which you ran `curie local up` / `docker compose ... up`, run `curie local
down` (or `docker compose -f compose.dev.yaml down`) and confirm with
`docker ps` that none of this run's containers are still up. Use the same private
project and overrides used for startup. Also remove stray runner containers this
run spawned, identified by recorded IDs or ownership labels; preserve other
jobs' containers. The thread that brought the
stack up owns tearing it down; a blocked or crashed test agent never cleans up
after itself, so do not assume someone else will.

**Do stack testing from a worktree, not the main checkout.** If a local test
requires code edits, make them in a git worktree cut from the release train base
selected below and land them as a PR. Never edit `main` or `next` in place to
make a local run work. Read only runs against the current tree are fine; the
moment you need to change code, cut a worktree.

Add `--profile slack` through `curie local up --slack` to start the optional dispatcher for real Slack.

Host ports (non-default host ports to avoid local collisions):

| Service | Host port |
|---|---|
| Langfuse UI | http://localhost:23000 |
| Postgres | localhost:25432 |
| Valkey | localhost:26379 |
| ClickHouse | HTTP 28123, native 29009 |
| RustFS | S3 29000, console 29001 |
| OTel Collector | gRPC 24317, HTTP 24318 |

Config lives in `.env.example` (copy to the gitignored `.env` to override; the
stack runs on the baked defaults without one). Load-bearing facts:

1. **ClickHouse:** `compose.dev.yaml` stays on `:24.8.14.39` so an SSE4.2-only
  developer host can still boot the local stack (Langfuse `3.225.5` still
  applies on the 24.8 line). The Helm chart default is `:25.12.11.4`, required
  by the Langfuse pin's ClickHouse migrations from 39 onward; AVX is a
  chart-default requirement. `preflights.avxCheck` fails an AVX-less node
  unless the operator pins a tag in `clickhouse.sse42SafeTags`. Do not move the
  two chart pins independently (#2210). Both sides name a patch build, not a
  `24.8` / `25.12` alias -- upstream republishes those (#2319).
2. **Postgres and Valkey are one pin, not two:** `postgres.image` is
  digest-pinned (`postgres:16.15-alpine@sha256:cf78e766...`, because
  PostgreSQL's version is two components) and `valkey.image` is
  `valkey/valkey:8.1.10-alpine`. `charts/curie/values.yaml`, `compose.dev.yaml`,
  the rust job's Valkey service in `.github/workflows/ci.yaml`, and
  `charts/curie/ci/postgres-readiness-delay.Dockerfile` must all name the same
  build; `compose/tests/test_generate_release_compose.py` asserts it.
  `postgres.podSecurityContext.fsGroup` is `70`, the `postgres` gid inside the
  pinned image, asserted against the running image by
  `charts/curie/ci/postgres-fsgroup-assertions.sh` (#2319).
3. **Langfuse OTLP ingest is HTTP-only** (gRPC is silently unsupported). Services
  may emit OTLP over gRPC or HTTP to the OTel Collector (4317/4318); the
  collector always exports to Langfuse over HTTP. Send app traces to the
  collector, not directly to Langfuse.
4. **Langfuse is bootstrapped headless** with a fixed dev project (`curie-dev`)
  and keys `pk-lf-curie-dev` / `sk-lf-curie-dev`, so the OTel path
  authenticates on first boot with no manual key-minting. Read traces back via
  `curl -u pk-lf-curie-dev:sk-lf-curie-dev http://localhost:23000/api/public/...`. <!-- gitleaks:allow -->

