FROM python:3.12-slim

WORKDIR /srv
COPY cli/scripts/fixtures/mcp-receipt/server.py /srv/mcp_receipt.py
COPY charts/curie/ci/cron-approval-receipt.py /srv/server.py
USER 65534:65534
EXPOSE 8000
ENTRYPOINT ["python", "-u", "/srv/server.py"]
