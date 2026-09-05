# Curie API image (FastAPI + uvicorn).
#
# BUILD FROM THE REPO ROOT, not from apps/api/, because the app resolves against
# the uv workspace (packages/*, uv.lock at the root):
#
#   docker build -f apps/api/Dockerfile -t curie-api .
#
# The per-Dockerfile ignore (apps/api/Dockerfile.dockerignore) trims the root
# context. runner/Dockerfile is the sibling reference for the workspace build.
#
# Multi-stage: the builder resolves the frozen workspace with uv and installs the
# `curie-api` member (and only its deps) as non-editable wheels into /app/.venv;
# the runtime carries just that venv plus the Alembic migration tree so the chart
# can run `alembic upgrade head` from the same image.
#
# Third-party dependencies install in their own layer ahead of the sources; see
# the dependency-layer comment in the builder.
FROM python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285 AS builder
RUN pip install --no-cache-dir uv==0.10.7
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
# Dependency layer: the lockfile plus every workspace member's pyproject.toml
# (uv needs all of them to resolve --frozen), and nothing else, so this layer
# is invalidated only by a lockfile or manifest change -- never by a source
# edit. --no-install-workspace installs the member's locked third-party
# dependencies without building any workspace member.
COPY pyproject.toml uv.lock ./
COPY packages/aci-protocol/pyproject.toml packages/aci-protocol/
COPY packages/channel-protocol/pyproject.toml packages/channel-protocol/
COPY packages/plugin-format/pyproject.toml packages/plugin-format/
COPY packages/telemetry-schema/pyproject.toml packages/telemetry-schema/
COPY packages/test-support/pyproject.toml packages/test-support/
COPY packages/telemetry/pyproject.toml packages/telemetry/
COPY apps/api/pyproject.toml apps/api/
COPY apps/dispatcher/pyproject.toml apps/dispatcher/
COPY adapters/discord/pyproject.toml adapters/discord/
COPY apps/mail-adapter/pyproject.toml apps/mail-adapter/
COPY apps/worker/pyproject.toml apps/worker/
COPY runner/pyproject.toml runner/
COPY tools/doclint/pyproject.toml tools/doclint/
COPY tools/wire-tolerance-gate/pyproject.toml tools/wire-tolerance-gate/
RUN uv sync --frozen --no-dev --no-editable --no-install-workspace --package curie-api
# Sources last: this sync only builds and installs the workspace members the
# package depends on; the third-party wheels above are already in the venv.
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package curie-api

FROM python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285
# git is a RUNTIME dependency: the git-flow engine (gitflow.py) shells out to
# `git clone --mirror` / `git archive` to build a bundle from a pushed sha. The
# slim base does not ship it, so without this a push webhook fails
# `git.archive_failed` and no deploy is ever built.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 1000 app && useradd -u 1000 -g app -m -d /home/app app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
# Migration tree so an init container can run `alembic -c alembic.ini upgrade head`
# (env.py reads DATABASE_URL via Settings and imports models from the installed
# package; script_location=alembic resolves relative to alembic.ini here).
COPY --from=builder --chown=app:app /app/apps/api/alembic /app/alembic
COPY --from=builder --chown=app:app /app/apps/api/alembic.ini /app/alembic.ini
ENV PATH="/app/.venv/bin:$PATH"
USER 1000
EXPOSE 8000
CMD ["uvicorn", "curie_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
