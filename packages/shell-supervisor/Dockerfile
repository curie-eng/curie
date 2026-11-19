# Static-pool harness shell: the unchanged runner image plus its trusted
# supervisor as PID 1. Build from the repo root against a built runner image:
#
#   docker build -f packages/shell-supervisor/Dockerfile \
#     --build-arg RUNNER_IMAGE=<runner image reference> -t curie-shell .
#
# The supervisor runs as root so it can run the runner child as uid 1000, stop
# its whole process group, and wipe what that uid wrote; the child never runs
# as root (config.py refuses uid 0). Port 8443 is the shell's only ingress; the
# runner's own port 8080 must not be reachable from outside the task.
ARG RUNNER_IMAGE=curie-runner
FROM ${RUNNER_IMAGE}

USER 0:0
COPY packages/shell-supervisor /app/packages/shell-supervisor
# --no-deps: aiohttp, its one dependency, is already in the runner venv.
# Outside its claim directory the runner child may write only the scratch
# roots the supervisor empties at every retirement (/tmp, /var/tmp, /dev/shm,
# /run/lock, /dev/mqueue). The runner image's own home belongs to uid 1000, so
# it is handed to root here; each claim gets a fresh HOME instead. The
# supervisor checks the whole filesystem at startup and refuses to serve if
# anything else is writable by the child, or if any setuid or setgid file
# remains (the child also runs with no_new_privs).
RUN /app/.venv/bin/pip install --no-cache-dir --no-deps /app/packages/shell-supervisor \
    && mkdir -p /var/lib/curie-shell \
    && chmod 0711 /var/lib/curie-shell \
    && chown -R 0:0 /home/runner \
    && chmod -R go-w /home/runner \
    && find / -xdev -type f -perm /6000 -exec chmod ug-s {} +

EXPOSE 8443
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
    CMD ["python", "-m", "curie_shell_supervisor.healthcheck"]
CMD ["python", "-m", "curie_shell_supervisor"]
