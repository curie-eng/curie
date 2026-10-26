# The opt-in scaler connector. Build it multi-arch, as the other connectors in
# this bundle are built: `build.platforms` in the opt-in declaration
# (connectors/scaler/opt-in.yaml) names both architectures, and one
# `curie build --plugin-dir <bundle> --registry <ref>` builds what it declares.
#
# SNAPSHOT_SEALING_KEY must reach this pod as a SecretRef (ACTION-EXECUTOR-16),
# and the kubeconfig for manifests/scaler-access.yaml as a file.
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
