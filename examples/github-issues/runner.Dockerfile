# Layers this bundle's stdio MCP server onto the platform runner (ADR 0173).
# The base is a build argument, never a tag written in this file. The platform
# image ends as uid 1000, and a global install has to write /usr/local, so the
# layer is root for that one command and then drops back.
ARG CURIE_RUNNER_IMAGE
FROM ${CURIE_RUNNER_IMAGE}

USER root
RUN npm install -g @modelcontextprotocol/server-github@2025.4.8
USER 1000:1000
