# Throwaway proof image. Chromium is installed at build time because a factory
# sandbox has no general network egress and a read-only root filesystem.
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        chromium \
        fonts-liberation \
        git \
        python3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 runner \
    && useradd --uid 1000 --gid 1000 --home-dir /home/runner --create-home --shell /usr/sbin/nologin runner

COPY prove.py /opt/prove.py
USER 1000:1000
ENV HOME=/home/runner
CMD ["python3", "/opt/prove.py"]
