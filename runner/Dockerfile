# Curie runner image. Productizes prototypes/runner/Dockerfile (PT-E) into the
# full ACI v0.1 runner.
#
# BUILD FROM THE REPO ROOT, not from runner/, because the runner compiles against
# the frozen workspace packages (packages/aci-protocol, packages/plugin-format)
# and shared telemetry packages (packages/telemetry and telemetry-schema):
#
#   docker build -f runner/Dockerfile -t curie-runner .
#
# The claude-agent-sdk supplies and drives its bundled platform `claude`
# executable. The image carries Node and npm only for pinned MCP packages. With
# CURIE_FAKE_MODEL=1 the container answers synthetic events offline (no CLI or
# model call), which is how the container smoke round trips without a credential.
# The frozen workspace packages require the Python base below.
# Node comes from the official image rather than NodeSource's apt repo. That
# repo's setup script (`deb.nodesource.com/setup_22.x`) started returning 403 to
# everyone on 2026-07-27, which broke every runner-image build repo-wide: curl
# failed, nodejs never installed, and the next line died with `npm: not found`.
# Copying from the official image removes the third-party apt repo from the build
# path entirely, pins the version by tag plus digest so a rebuild at this commit
# is the same image (#2320), and is architecture-correct by construction (Docker
# resolves the matching platform from the multi-arch index, so an arm64 dev box
# and an amd64 CI runner both work -- a hardcoded tarball URL would not).
# Floating tags would still pick up upstream OS security patches without a
# commit; Dependabot's docker ecosystem raises those base moves as PRs instead.
FROM node:22.23.2-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5 AS node

FROM python:3.13.15-slim-bookworm@sha256:ed86c82274b3c69b52fb5820f358f0bd7df0b603332063cb5c6e32bd220c3e6e

# Only the Node bits, never all of /usr/local: the python base keeps its own
# interpreter there, so a blanket copy would clobber it.
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules

RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

# Off-the-shelf MCP servers pre-installed at build time so a bundle that
# declares one starts it from the on-disk binary with NO runtime network fetch
# (the runner rootfs is read-only at runtime, so an `npx`-style fetch would fail
# anyway). Bundles reference these by the package's bin name, e.g.
# `"command": "mcp-server-github"` -- see examples/github-issues. Add a line here
# to bless another authed third-party MCP server for the same shape.
RUN npm install -g @modelcontextprotocol/server-github@2025.4.8

WORKDIR /app

# The five source trees the runner needs: the two frozen workspace packages, the
# shared telemetry implementation and schema, and the runner itself. Copied by path so the image is
# reproducible from the lockfile above them (root pyproject/uv.lock are the
# workspace source of truth).
COPY uv.lock ./uv.lock
COPY packages/aci-protocol ./packages/aci-protocol
COPY packages/plugin-format ./packages/plugin-format
COPY packages/telemetry ./packages/telemetry
COPY packages/telemetry-schema ./packages/telemetry-schema
COPY runner ./runner

# One venv. Install every local project without dependencies, then install the
# full registry dependency closure exported from the tested workspace uv.lock.
# The local packages are installed by path first so the
# workspace requirements resolve to the copied trees, not same-named PyPI
# distributions.
RUN python3 -m venv /app/.venv \
    && /app/.venv/bin/pip install --no-cache-dir --upgrade pip \
    && /app/.venv/bin/pip install --no-cache-dir --no-deps \
        ./packages/aci-protocol \
        ./packages/plugin-format \
        ./packages/telemetry \
        ./packages/telemetry-schema \
    && /app/.venv/bin/pip install --no-cache-dir --no-deps ./runner \
    && python3 runner/export_dependency_pins.py < uv.lock > /tmp/runner-dependency-pins.txt \
    && /app/.venv/bin/pip install --no-cache-dir --no-deps -r /tmp/runner-dependency-pins.txt

# Run as a non-root user. The claude-agent-sdk spawns
# `claude --dangerously-skip-permissions` (permission_mode=bypassPermissions),
# and Claude Code refuses that flag under uid 0 ("cannot be used with root/sudo
# privileges"), so a root image fails every real-model boot. uid:gid 1000:1000
# matches the A2 chart hardening (runAsUser/runAsGroup/fsGroup 1000) and HOME is
# /home/runner (one of the chart's writablePaths). The venv contains the SDK and
# its bundled runtime in root owned, world readable paths, so uid 1000 imports
# the dependencies and executes the CLI without owning them. HOME is the only
# writable path the runtime needs; under the A2 read-only rootfs the chart mounts
# emptyDirs at /tmp and /home/runner, and nothing here writes elsewhere.
RUN groupadd --gid 1000 runner \
    && useradd --uid 1000 --gid 1000 --home-dir /home/runner --create-home --shell /usr/sbin/nologin runner \
    && chown -R 1000:1000 /home/runner
ENV HOME=/home/runner

ENV PATH="/app/.venv/bin:$PATH"
ENV CURIE_RUNNER_PORT=8080
EXPOSE 8080

USER 1000:1000
CMD ["python", "-m", "curie_runner"]
