# The per-run Docker daemon a Docker-enabled factory run reaches (ADR 0217
# decision 4): stock dind with the curie-authz authorization plugin in front of
# every API call, user namespace remapping and a private cgroup namespace
# forced on by the entrypoint, and the BuildKit network.host,
# security.insecure and device entitlements turned off in daemon.json.
#
# The builder stage runs on the build platform and cross compiles a static musl
# binary for the target arch with rust-lld, so a multi-arch build needs no
# emulation: only the final stage, which copies files, is per target platform.

FROM --platform=$BUILDPLATFORM rust:1.99.1-alpine3.24@sha256:b1d5dd5a22a09404d3e798c0cf051b689077360374eb505fa5bb93c479693d87 AS build
ARG TARGETARCH
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) t=x86_64-unknown-linux-musl ;; \
      arm64) t=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    RUSTFLAGS="-C linker=rust-lld -C target-feature=+crt-static" \
      cargo build --locked --release --target "$t"; \
    cp "target/$t/release/curie-docker-authz" /curie-docker-authz

FROM docker:29.9.2-dind@sha256:9e79b879b177fbd72c34376a3dd3d91747da5db49e1f560f76aea59561471977
COPY --from=build /curie-docker-authz /usr/local/bin/curie-docker-authz
COPY daemon.json /etc/docker/daemon.json
COPY --chmod=0755 entrypoint.sh /usr/local/bin/curie-docker-daemon-entrypoint
ENTRYPOINT ["curie-docker-daemon-entrypoint"]
# The entrypoint adds --authorization-plugin=curie-authz --userns-remap=default.
CMD ["dockerd"]
