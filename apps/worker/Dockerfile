# Curie worker image (concurrency kernel + sandbox substrate + eval runner).
#
# BUILD FROM THE REPO ROOT (uv workspace); see runner/Dockerfile and
# apps/api/Dockerfile for the shared pattern:
#
#   docker build -f apps/worker/Dockerfile -t curie-worker .
#
# The worker drives the Kubernetes API to claim/manage sandboxes, so its
# Deployment carries a ServiceAccount + RBAC (see the chart). No inbound HTTP.
FROM python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285 AS builder
RUN pip install --no-cache-dir uv==0.10.7
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package curie-worker

FROM python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 app \
    && useradd -u 1000 -g app -m -d /home/app app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
USER 1000
CMD ["python", "-m", "curie_worker"]
