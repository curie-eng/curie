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
RUN set -eu; \
    version=0.9.146; \
    case "$(dpkg --print-architecture)" in \
        amd64) target=x86_64-unknown-linux-gnu; sha256=682c21b777c333e96fd532e114d3a5a894e0729ab88d94c0a9f20f8419695428 ;; \
        arm64) target=aarch64-unknown-linux-gnu; sha256=b2e33d7c72de7ade0ff7b3a948ac37516b24f8a836b7a8870c1f634a94be9de9 ;; \
        *) echo "unsupported cargo-nextest architecture" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${version}/cargo-nextest-${version}-${target}.tar.gz" -o /tmp/cargo-nextest.tar.gz; \
    echo "${sha256}  /tmp/cargo-nextest.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/cargo-nextest.tar.gz -C /usr/local/cargo/bin cargo-nextest; \
    rm /tmp/cargo-nextest.tar.gz
# Official Helm archives, the same v3.16.4 pin as the CLI chart-consumer CI.
# Checksums are the published .sha256sum values, verified against the bytes.
RUN set -eu; \
    version=3.16.4; \
    arch="$(dpkg --print-architecture)"; \
    case "${arch}" in \
        amd64) sha256=fc307327959aa38ed8f9f7e66d45492bb022a66c3e5da6063958254b9767d179 ;; \
        arm64) sha256=d3f8f15b3d9ec8c8678fbf3280c3e5902efabe5912e2f9fcf29107efbc8ead69 ;; \
        *) echo "unsupported helm architecture: ${arch}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://get.helm.sh/helm-v${version}-linux-${arch}.tar.gz" -o /tmp/helm.tar.gz; \
    echo "${sha256}  /tmp/helm.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/helm.tar.gz -C /tmp "linux-${arch}/helm"; \
    install -m 0755 "/tmp/linux-${arch}/helm" /usr/local/bin/helm; \
    rm -rf /tmp/helm.tar.gz "/tmp/linux-${arch}"
USER 1000:1000
