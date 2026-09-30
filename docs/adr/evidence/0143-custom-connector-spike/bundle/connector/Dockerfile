# The connector image. Built and pushed by `curie build --plugin-dir <bundle>
# --registry <ref>`, which records the registry digest in connectors.lock.yaml.
FROM python:3.12-slim

# uid 65532 matches the securityContext the platform renders (runAsUser 65532,
# readOnlyRootFilesystem). A different uid here would still run, as 65532.
RUN useradd --uid 65532 --create-home --shell /usr/sbin/nologin nonroot

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

USER 65532:65532
EXPOSE 8000
# CMD, not ENTRYPOINT. `connectors.yaml` `args` become the container's `args`,
# which replace CMD and are APPENDED to an ENTRYPOINT. Measured in the ADR's
# evidence: an ENTRYPOINT image silently ignores the declared process.
CMD ["python", "-u", "server.py"]
