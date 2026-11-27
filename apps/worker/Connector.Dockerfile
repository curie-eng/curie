FROM python:3.15.1-slim@sha256:533b0b2f9b5d46fd097091cf826f27902f1d6c3bca714b6dc74551356aa97a51 AS builder
RUN pip install --no-cache-dir uv==0.10.7
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY . .
RUN uv sync --frozen --no-dev --no-editable --package curie-worker

FROM python:3.15.1-slim@sha256:533b0b2f9b5d46fd097091cf826f27902f1d6c3bca714b6dc74551356aa97a51
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 connector \
    && useradd --uid 65532 --gid 65532 --no-create-home connector
WORKDIR /opt/connector
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER 65532:65532
ENTRYPOINT ["/usr/bin/tini", "--", "python3", "-m", "curie_connector_proxy.hosting"]
