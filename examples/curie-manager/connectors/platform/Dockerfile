# The curie-manager `platform` connector: the Curie platform API as MCP tools.
# Built multi-arch by `curie build --plugin-dir <bundle>`, which reads
# `build.platforms` in the bundle's connectors.yaml. Nothing here is run by hand.
FROM python:3.12-slim

# uid 65532 (nonroot) matches the other connectors, so the same securityContext
# works without a special case.
RUN useradd --uid 65532 --create-home --shell /usr/sbin/nologin nonroot

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["python", "-u", "server.py"]
