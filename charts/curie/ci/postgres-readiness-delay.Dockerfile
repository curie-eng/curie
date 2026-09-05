# Must equal charts/curie/values.yaml `postgres.image` -- the delayed fixture
# only proves the readiness gate if it is the shipped image plus a sleep
# (#2319; asserted by compose/tests/test_generate_release_compose.py).
ARG POSTGRES_BASE_IMAGE=postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685
FROM ${POSTGRES_BASE_IMAGE}

# Give the Langfuse pod a deterministic window in which to prove that its web
# process remains behind the chart's readiness gate. Keep this at 20 seconds:
# the chart's Postgres liveness probe starts at 20 seconds, runs every 10 seconds,
# and restarts after its third failure (roughly 40 seconds). A longer artificial
# entrypoint delay can therefore turn the dependency fixture itself into a
# restart loop. Kubernetes `command`
# on the gate overrides this entrypoint, so only the real Postgres container
# pays the proven-safe delay.
ENV POSTGRES_READINESS_DELAY_SECONDS=20
ENTRYPOINT ["sh", "-ec", "sleep \"${POSTGRES_READINESS_DELAY_SECONDS}\"; exec /usr/local/bin/docker-entrypoint.sh \"$@\"", "--"]
CMD ["postgres"]
