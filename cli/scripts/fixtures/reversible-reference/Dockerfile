# The reference reversible connector (plan task 7). The e2e ladder copies this
# directory into a scratch bundle and builds it with `curie build`, like the
# mcp-receipt fixture beside it; nothing here is run by hand. Build it for the
# architectures the bundle's connectors.yaml declares, as the example
# connectors do.
#
# SNAPSHOT_SEALING_KEY must reach this pod as a SecretRef (ACTION-EXECUTOR-16).
# REFERENCE_STORE_PATH names the disposable record store; the server refuses to
# start without it until the cluster tier's Deployment target lands.
FROM python:3.12-slim

# uid 65532 (nonroot) matches the other connectors.
RUN useradd --uid 65532 --create-home --shell /usr/sbin/nologin nonroot

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["python", "-u", "server.py"]
