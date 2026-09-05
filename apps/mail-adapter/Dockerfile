# Curie mail adapter image (AgentMail email channel).
#
# BUILD FROM THE REPO ROOT (uv workspace); see apps/dispatcher/Dockerfile and
# apps/api/Dockerfile for the shared pattern:
#
#   docker build -f apps/mail-adapter/Dockerfile -t curie-mail-adapter .
#
# Unlike the dispatcher this process serves inbound HTTP (the neutral reply wire
# plus /healthz on CURIE_MAIL_PORT), so the chart gives it an HTTP liveness probe
# rather than a heartbeat-file exec probe.
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
RUN uv sync --frozen --no-dev --no-editable --no-install-workspace --package curie-mail-adapter
# Sources last: this sync only builds and installs the workspace members the
# package depends on; the third-party wheels above are already in the venv.
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package curie-mail-adapter

FROM python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285
RUN groupadd -g 1000 app && useradd -u 1000 -g app -m -d /home/app app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
USER 1000
CMD ["python", "-m", "curie_mail_adapter"]
