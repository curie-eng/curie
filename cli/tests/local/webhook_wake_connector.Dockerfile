ARG WORKER_IMAGE
FROM ${WORKER_IMAGE}
COPY cli/tests/local/webhook_wake_connector.py /app/webhook_wake_connector.py
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["python3", "/app/webhook_wake_connector.py"]
