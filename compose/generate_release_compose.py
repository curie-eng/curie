#!/usr/bin/env python3
"""Generate the self-contained release compose file from compose.dev.yaml.

compose.dev.yaml is the single source of truth for the local stack. The release
asset (`compose.release.yaml`, shipped to `curie local up` on a release binary)
must not depend on a repo checkout, so this script derives it from the dev file
via three ordered text transforms:

  T1  Replace the curie-worker build overlay (`build: {context, dockerfile}`)
      with a pinned `image: ghcr.io/curie-eng/curie-worker-local:latest`, since
      the release stack cannot build the worker-local overlay from source.

  T2  Inline otel/collector-config.yaml as a top-level `configs:` block (a literal
      scalar, re-indented 6 spaces, with `${env:` escaped to `$${env:` so compose
      does not try to interpolate the collector's own env references), and repoint
      the otel-collector service from the host bind-mount to that config while
      retaining its task-owned persistent-queue volume.

  T3  Pin every `ghcr.io/curie-eng/curie-*:latest` image tag to the release
      version (this also pins the worker-local image introduced by T1). Also
      collapses the `:${CURIE_BASE_TAG:-latest}` override form (issue #698)
      to the same literal pin, since the release asset has no shell to resolve
      that override in.

Each transform locates its anchor explicitly and raises ValueError if it is
missing: this runs unattended at publish time, so a silent no-op would ship a
broken release asset. Fail loud instead.
"""

import argparse
import re
import textwrap
from pathlib import Path

WORKER_BUILD_BLOCK = """    build:
      context: compose
      dockerfile: worker-local.Dockerfile
      # Threads CURIE_BASE_TAG through to the overlay's own ARG BASE_TAG
      # (compose/worker-local.Dockerfile), which pins its `FROM
      # ghcr.io/curie-eng/curie-worker:${BASE_TAG}` base. Without this the
      # arg was never wired, so the Dockerfile silently fell back to its own
      # `latest` default regardless of what the api/migrate override above was
      # set to. Same variable, same default, so one override can drive all
      # three services uniformly.
      args:
        BASE_TAG: ${CURIE_BASE_TAG:-latest}
"""
WORKER_IMAGE_LINE = "    image: ghcr.io/curie-eng/curie-worker-local:latest\n"

CONFIGS_ANCHOR = "x-core-profiles: &core_profiles [core, full]"

OTEL_VOLUME_BLOCK = """    volumes:
      - ./otel/collector-config.yaml:/etc/otel/collector-config.yaml:ro
      - otel_collector_storage:/var/lib/otelcol/storage
"""
OTEL_CONFIGS_REF = """    configs:
      - source: otel_collector_config
        target: /etc/otel/collector-config.yaml
    volumes:
      - otel_collector_storage:/var/lib/otelcol/storage
"""

# Matches the plain `:latest` pin AND compose.dev.yaml's
# `:${CURIE_BASE_TAG:-latest}` override form (issue #698: curie-api and
# curie-migrate's `image:` refs carry the override so CI/local runs can
# repoint them at a locally built tag with no registry auth). The release
# asset has no shell to resolve that override in, so either form collapses to
# a plain, literal `:<version>` pin here -- same outcome the dev file's own
# unset default already produces.
CURIE_LATEST_RE = re.compile(
    r"(ghcr\.io/curie-eng/curie-[a-z-]+):(?:latest|\$\{CURIE_BASE_TAG:-latest\})"
)

# The other override shape, added by #1915: an image whose WHOLE reference is a
# defaulted variable, `${CURIE_UI_IMAGE:-ghcr.io/curie-eng/curie-ui:latest}`.
# Those images (ui, dispatcher, runner) are not on CURIE_BASE_TAG, because that
# variable means "the platform images this caller built" and CI sets it while
# building only api and worker.
#
# T3's pattern matches the ref INSIDE the braces, which would pin the tag and
# leave the `${...}` wrapper in place -- and the release asset has no shell to
# resolve it, so compose would try to pull an image literally named `${...}`.
# Unwrapping first collapses the form to the plain ref T3 then pins, which is the
# same outcome the dev file's own unset default already produces.
CURIE_DEFAULTED_IMAGE_RE = re.compile(
    r"\$\{[A-Z_]+:-(ghcr\.io/curie-eng/curie-[a-z-]+:latest)\}"
)

DEV_COMPOSE = Path("compose.dev.yaml")
OTEL_CONFIG = Path("otel/collector-config.yaml")


def generate(dev_text: str, otel_text: str, version: str) -> str:
    """Apply transforms T1, T2, T3 in order and return the release compose text."""
    text = dev_text

    # T1: worker build overlay -> pinned worker-local image.
    if WORKER_BUILD_BLOCK not in text:
        raise ValueError(
            "T1: curie-worker build overlay block not found in compose.dev.yaml"
        )
    text = text.replace(WORKER_BUILD_BLOCK, WORKER_IMAGE_LINE, 1)

    # T2a: inline the collector config as a top-level configs block.
    if CONFIGS_ANCHOR not in text:
        raise ValueError(f"T2: anchor line not found: {CONFIGS_ANCHOR!r}")
    body = textwrap.indent(otel_text.replace("${env:", "$${env:"), "      ")
    configs_block = "configs:\n  otel_collector_config:\n    content: |\n" + body
    text = text.replace(CONFIGS_ANCHOR, configs_block + "\n" + CONFIGS_ANCHOR, 1)

    # T2b: repoint otel-collector from the host bind-mount to the inlined config.
    if OTEL_VOLUME_BLOCK not in text:
        raise ValueError(
            "T2: otel-collector host bind-mount block not found in compose.dev.yaml"
        )
    text = text.replace(OTEL_VOLUME_BLOCK, OTEL_CONFIGS_REF, 1)

    # T3a: unwrap a defaulted whole-reference override to its plain ref, so T3b
    # pins it like any other and no `${...}` survives into the release asset.
    text = CURIE_DEFAULTED_IMAGE_RE.sub(r"\1", text)

    # T3b: pin every curie-* image tag to the release version (worker-local too).
    text = CURIE_LATEST_RE.sub(rf"\1:{version}", text)

    return text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate compose.release.yaml from compose.dev.yaml."
    )
    parser.add_argument("--version", default="latest", help="release version to pin image tags to")
    args = parser.parse_args()

    dev_text = DEV_COMPOSE.read_text()
    otel_text = OTEL_CONFIG.read_text()
    result = generate(dev_text, otel_text, args.version)
    print(result, end="")


if __name__ == "__main__":
    main()
