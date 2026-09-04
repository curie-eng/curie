# Curie dispatcher image (Slack Bolt, Socket Mode).
#
# BUILD FROM THE REPO ROOT (uv workspace); see runner/Dockerfile and
# apps/api/Dockerfile for the shared pattern:
#
#   docker build -f apps/dispatcher/Dockerfile -t curie-dispatcher .
#
# Socket Mode means no inbound HTTP: the process dials Slack out, so there is no
# server port. The chart uses a heartbeat-file exec liveness probe (a daemon
# thread touches a file the probe checks for freshness), not an HTTP probe.
FROM python:3.14.7-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 AS builder
RUN pip install --no-cache-dir uv==0.10.7
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package curie-dispatcher

FROM python:3.14.7-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
RUN groupadd -g 1000 app && useradd -u 1000 -g app -m -d /home/app app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
USER 1000
CMD ["python", "-m", "curie_dispatcher"]
