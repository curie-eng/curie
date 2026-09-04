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
FROM python:3.14.7-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 AS builder
RUN pip install --no-cache-dir uv==0.10.7
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package curie-mail-adapter

FROM python:3.14.7-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
RUN groupadd -g 1000 app && useradd -u 1000 -g app -m -d /home/app app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
USER 1000
CMD ["python", "-m", "curie_mail_adapter"]
