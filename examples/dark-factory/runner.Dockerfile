# Layers this bundle's repository toolchains onto the platform runner.
# The base is a build argument, never a tag written in this file. The platform
# image already supplies Python 3.13 and Node 22. External images resolve for
# the target platform, so their binaries match both amd64 and arm64 builds.
ARG CURIE_RUNNER_IMAGE
FROM ${CURIE_RUNNER_IMAGE}

USER root
# Copy exact multiarch releases without adding another base image. Rust's
# installed toolchain is readable from the immutable root filesystem; Cargo's
# registry and git cache live under the writable home mount at runtime.
COPY --from=ghcr.io/astral-sh/uv:0.10.7@sha256:edd1fd89f3e5b005814cc8f777610445d7b7e3ed05361f9ddfae67bebfe8456a /uv /uvx /usr/local/bin/
COPY --from=rust:1.95.0-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 /usr/local/cargo /usr/local/cargo
COPY --from=rust:1.95.0-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 /usr/local/rustup /usr/local/rustup
# Cargo.lock includes aws-lc-sys and ring, which compile native code.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g --ignore-scripts pnpm@9.15.9
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=${HOME}/.cargo
ENV UV_CACHE_DIR=${HOME}/.cache/uv
ENV PNPM_HOME=${HOME}/.local/share/pnpm
ENV PATH="/usr/local/cargo/bin:${PATH}"
RUN rustup component add rustfmt clippy
# Claude Code asks for up to 64000 output tokens per Opus request by default,
# and OpenRouter reserves credit for the whole ceiling, so a low balance refused
# every reviewer call (#3935). A verdict needs a few thousand tokens. The CLI
# has no per-subagent ceiling, so this caps every request in the sandbox, the
# factory model's included.
ENV CLAUDE_CODE_MAX_OUTPUT_TOKENS=16000
USER 1000:1000
