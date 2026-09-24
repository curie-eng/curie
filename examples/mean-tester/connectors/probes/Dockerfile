# Multi-arch for the same reason as examples/sre-bot/connectors/tempo: the
# builder and the node are often different architectures.
FROM python:3.12-slim
RUN useradd --uid 65532 --create-home --shell /usr/sbin/nologin nonroot
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mean_tester_probes ./mean_tester_probes
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["python", "-u", "-m", "mean_tester_probes.server"]
