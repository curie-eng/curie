"""Contract tests for the compose release generator (compose/generate_release_compose.py).

The generator turns compose.dev.yaml into a self-contained release compose file via
three text transforms: (1) replace the curie-worker build overlay with a pinned
worker-local image, (2) inline otel/collector-config.yaml as a top-level `configs:`
block (re-indented 6 spaces, `${env:` escaped to `$${env:`) and repoint the
otel-collector service at it, and (3) pin every ghcr curie-* image tag to the
release version. These tests assert on those transforms and on invariants preserved
from the dev stack. They deliberately do NOT compare byte-for-byte against the
hand-maintained compose.release.yaml, which has drifted from dev.
"""

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "compose" / "generate_release_compose.py"
DEV_PATH = REPO_ROOT / "compose.dev.yaml"
OTEL_PATH = REPO_ROOT / "otel" / "collector-config.yaml"
# Artifacts that must carry the SAME data-tier image pins as compose (#2319).
CHART_VALUES_PATH = REPO_ROOT / "charts" / "curie" / "values.yaml"
CI_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "ci.yaml"
READINESS_DOCKERFILE_PATH = (
    REPO_ROOT / "charts" / "curie" / "ci" / "postgres-readiness-delay.Dockerfile"
)
READINESS_RUNTIME_SCRIPT_PATH = (
    REPO_ROOT
    / "charts"
    / "curie"
    / "ci"
    / "runtime"
    / "langfuse-postgres-readiness-runtime.sh"
)

DEV_TEXT = DEV_PATH.read_text()
OTEL_TEXT = OTEL_PATH.read_text()

CURIE_LATEST_RE = re.compile(r"ghcr\.io/curie-eng/curie-[a-z-]+:latest")
CURIE_IMAGE_RE = re.compile(r"ghcr\.io/curie-eng/curie-[a-z-]+:(\S+)")
# `${env:` not preceded by a `$` -> an UNescaped collector-config reference.
UNESCAPED_ENV_RE = re.compile(r"(?<!\$)\$\{env:")


def load_generate():
    """Import the standalone generator script by path (compose/ is not on sys.path)."""
    spec = importlib.util.spec_from_file_location("generate_release_compose", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.generate


def run_cli(*args):
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def service_names(text):
    """Extract the set of service keys (2-space-indented `  name:` under `services:`)."""
    names = set()
    in_services = False
    for line in text.splitlines():
        if re.match(r"^services:\s*$", line):
            in_services = True
            continue
        if in_services and re.match(r"^\S", line):
            break  # next top-level key (e.g. `volumes:`)
        if in_services:
            m = re.match(r"^ {2}([A-Za-z0-9_-]+):\s*$", line)
            if m:
                names.add(m.group(1))
    return names


def service_block(text, name):
    """Return the text of a single service block, header through last body line."""
    out = []
    capturing = False
    for line in text.splitlines():
        if re.match(rf"^ {{2}}{re.escape(name)}:\s*$", line):
            capturing = True
            out.append(line)
            continue
        if capturing:
            # A new 2-space-indented header/comment or a top-level key ends the block.
            if re.match(r"^ {2}\S", line) or re.match(r"^\S", line):
                break
            out.append(line)
    return "\n".join(out)


def test_worker_build_overlay_becomes_pinned_image():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="9.9.9")

    worker = service_block(out, "curie-worker")
    assert worker, "curie-worker service block not found in generated output"
    assert "image: ghcr.io/curie-eng/curie-worker-local:9.9.9" in worker
    assert "build:" not in worker
    assert "worker-local.Dockerfile" not in worker
    assert "worker-local.Dockerfile" not in out


def test_otel_config_is_inlined_and_escaped():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="9.9.9")

    # A new top-level configs block holds the collector config as a literal scalar.
    assert re.search(r"^configs:\s*$", out, re.MULTILINE)
    assert "otel_collector_config:" in out
    assert "content: |" in out

    # The inlined content is the collector config re-indented 6 spaces with the
    # `${env:` interpolation escaped to `$${env:` (compose interpolation escape).
    expected_block = textwrap.indent(OTEL_TEXT.replace("${env:", "$${env:"), "      ")
    assert expected_block in out

    # The escaped auth line is present, and NO unescaped `${env:` remains anywhere.
    assert "$${env:LANGFUSE_OTLP_AUTH_HEADER}" in out
    assert UNESCAPED_ENV_RE.search(out) is None


def test_otel_collector_references_config_not_host_mount():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="9.9.9")

    collector = service_block(out, "otel-collector")
    assert collector, "otel-collector service block not found in generated output"
    assert "source: otel_collector_config" in collector
    assert "target: /etc/otel/collector-config.yaml" in collector
    # The host bind-mount of the config file is gone.
    assert "./otel/collector-config.yaml" not in out


def test_curie_images_pinned_non_curie_untouched():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="9.9.9")

    # Every curie-* image is pinned to the release version; none left at :latest.
    assert CURIE_LATEST_RE.search(out) is None
    tags = CURIE_IMAGE_RE.findall(out)
    assert tags, "expected at least one ghcr curie-* image in the output"
    assert all(tag == "9.9.9" for tag in tags)

    # Non-curie images are never rewritten.
    assert (
        "image: postgres:16.15-alpine@sha256:"
        "cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685" in out
    )
    assert "image: otel/opentelemetry-collector-contrib:0.119.0" in out


def test_data_tier_pins_agree_across_artifacts():
    """The data-tier image pins are the same bytes everywhere they are named (#2319).

    Postgres and Valkey are one pin repeated across four artifacts: a chart that
    pins one build while compose, the rust job's Valkey service, or the readiness
    fixture's base image pin another is a stack nobody has actually tested
    together. ClickHouse is the ONE documented exception (#2210): compose stays on
    the SSE4.2-safe 24.8 line so an AVX-less developer host can boot, while the
    chart needs the AVX-only 25.12 line for the Langfuse migration set. That split
    is asserted as a split -- compose must sit on a tag the chart itself lists as
    SSE4.2-safe -- so it stays deliberate rather than becoming accidental drift.
    """
    values = yaml.safe_load(CHART_VALUES_PATH.read_text())
    workflow = yaml.safe_load(CI_WORKFLOW_PATH.read_text())

    # Postgres: chart == compose (dev and release) == readiness fixture base.
    chart_postgres = values["postgres"]["image"]
    for label, doc in compose_docs():
        assert doc["services"]["postgres"]["image"] == chart_postgres, (
            f"{label} postgres image must match the chart's postgres.image "
            f"{chart_postgres!r}"
        )

    dockerfile_args = re.findall(
        r"^ARG\s+POSTGRES_BASE_IMAGE=(\S+)\s*$",
        READINESS_DOCKERFILE_PATH.read_text(),
        re.MULTILINE,
    )
    assert dockerfile_args == [chart_postgres], (
        f"{READINESS_DOCKERFILE_PATH.name} POSTGRES_BASE_IMAGE default "
        f"{dockerfile_args!r} must be the chart's postgres.image {chart_postgres!r}"
    )

    runtime_script_pins = re.findall(
        r'^CHART_DEFAULT_POSTGRES_IMAGE="([^"]+)"\s*$',
        READINESS_RUNTIME_SCRIPT_PATH.read_text(),
        re.MULTILINE,
    )
    assert runtime_script_pins == [chart_postgres], (
        f"{READINESS_RUNTIME_SCRIPT_PATH.name} CHART_DEFAULT_POSTGRES_IMAGE "
        f"{runtime_script_pins!r} must be the chart's postgres.image {chart_postgres!r}"
    )

    # Valkey: chart == compose (dev and release) == the rust job's service container.
    chart_valkey = values["valkey"]["image"]
    for label, doc in compose_docs():
        assert doc["services"]["valkey"]["image"] == chart_valkey, (
            f"{label} valkey image must match the chart's valkey.image {chart_valkey!r}"
        )
    workflow_valkey = workflow["jobs"]["rust"]["services"]["valkey"]["image"]
    assert workflow_valkey == chart_valkey, (
        f"ci.yaml jobs.rust.services.valkey.image {workflow_valkey!r} must be the "
        f"chart's valkey.image {chart_valkey!r} so the valkey_or_skip tests cover "
        "the shipped default"
    )

    # ClickHouse: the documented split, each side on a full patch build.
    safe_tags = [str(tag) for tag in values["clickhouse"]["sse42SafeTags"]]
    for label, doc in compose_docs():
        compose_clickhouse_tag = doc["services"]["clickhouse"]["image"].rsplit(":", 1)[1]
        assert re.fullmatch(r"\d+(?:\.\d+){3}", compose_clickhouse_tag), (
            f"{label} ClickHouse tag {compose_clickhouse_tag!r} must be a "
            "four-component patch build, not a moving 24.8 alias"
        )
        assert any(compose_clickhouse_tag.startswith(f"{tag}.") for tag in safe_tags), (
            f"{label} ClickHouse {compose_clickhouse_tag!r} must be on one of the "
            f"chart's SSE4.2-safe lines {safe_tags!r} -- that is the documented "
            "reason it differs from the chart default (#2210)"
        )


def test_rustfs_and_aws_bucket_bootstrap_preserve_the_s3_consumer_contract():
    """The bundled store is RustFS and clients speak generic S3 APIs."""
    for label, doc in compose_docs():
        services = doc["services"]
        assert "minio" not in services, f"{label} still exposes the retired object store"
        assert "minio-init" not in services, f"{label} still exposes the retired bootstrap client"

        rustfs = services.get("rustfs")
        assert rustfs is not None, f"{label} must expose the bundled RustFS server"
        assert rustfs.get("image") == "rustfs/rustfs:1.0.0-beta.12"
        rustfs_env = env_map(rustfs)
        for key in (
            "RUSTFS_VOLUMES",
            "RUSTFS_ADDRESS",
            "RUSTFS_CONSOLE_ADDRESS",
            "RUSTFS_ACCESS_KEY",
            "RUSTFS_SECRET_KEY",
        ):
            assert key in rustfs_env, f"{label} RustFS server is missing {key}"
        healthcheck = rustfs.get("healthcheck", {})
        health_command = " ".join(str(part) for part in healthcheck.get("test", []))
        assert re.search(r"/health(?=$|[\s\"'])", health_command), (
            f"{label} RustFS healthcheck must call /health, got {health_command!r}"
        )

        bootstrap = services.get("rustfs-init")
        assert bootstrap is not None, f"{label} must bootstrap the Langfuse bucket through RustFS"
        assert bootstrap.get("image") == "amazon/aws-cli:2.32.6"
        assert (
            bootstrap.get("depends_on", {}).get("rustfs", {}).get("condition")
            == "service_healthy"
        )
        bootstrap_command = str(bootstrap.get("entrypoint", ""))
        assert "aws " in bootstrap_command and "s3" in bootstrap_command
        assert "http://rustfs:9000" in bootstrap_command
        assert "mc " not in bootstrap_command

        for consumer in ("langfuse-web", "langfuse-worker"):
            consumer_env = env_map(services[consumer])
            assert consumer_env["LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT"] == "http://rustfs:9000"
            assert consumer_env["LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT"] == "http://rustfs:9000"
            assert consumer_env["LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE"] == "true"
            assert consumer_env["LANGFUSE_S3_MEDIA_UPLOAD_FORCE_PATH_STYLE"] == "true"


def test_invariants_preserved_from_dev():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="9.9.9")

    assert "x-core-profiles: &core_profiles [core, full]" in out
    assert "x-full-profiles: &full_profiles [full]" in out
    assert out.count("profiles: *core_profiles") == 8
    assert out.count("profiles: *full_profiles") == 6

    # No service is added or dropped by the transforms.
    assert service_names(out) == service_names(DEV_TEXT)


def test_default_version_latest_leaves_latest_tags():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="latest")

    worker = service_block(out, "curie-worker")
    assert "image: ghcr.io/curie-eng/curie-worker-local:latest" in worker
    assert "build:" not in worker

    tags = CURIE_IMAGE_RE.findall(out)
    assert tags
    assert all(tag == "latest" for tag in tags)


def test_cli_prints_generated_yaml():
    result = run_cli("--version", "9.9.9")
    assert result.returncode == 0, result.stderr
    out = result.stdout

    assert "image: ghcr.io/curie-eng/curie-worker-local:9.9.9" in out
    assert CURIE_LATEST_RE.search(out) is None
    assert re.search(r"^configs:\s*$", out, re.MULTILINE)
    assert "$${env:LANGFUSE_OTLP_AUTH_HEADER}" in out
    assert service_names(out) == service_names(DEV_TEXT)


def test_cli_default_version_is_latest():
    result = run_cli()
    assert result.returncode == 0, result.stderr
    out = result.stdout

    assert "image: ghcr.io/curie-eng/curie-worker-local:latest" in out
    tags = CURIE_IMAGE_RE.findall(out)
    assert tags
    assert all(tag == "latest" for tag in tags)


# --- Dispatcher <-> API wiring (#442) ---------------------------------------
#
# The dispatcher resolves Slack approval clicks by calling the platform API. It
# must therefore be told where the API is. Unwired, it falls back to its code
# default http://localhost:8000, which inside its own bridge-network container
# is the dispatcher itself, and every Approve click dead-ends.
#
# These assert the resolved VALUE, not the presence of a key: both "absent" and
# "wired to the wrong host" must fail. They run against the dev document (the
# source of truth) and the generated release document (the shipped asset), since
# the generator copies env through untouched and a guard on only one of the two
# leaves the other free to drift.


def env_map(spec):
    """Service `environment:` as a dict, normalizing compose's two forms.

    The dispatcher and API use map form (`KEY: value`); the worker uses list
    form (`- KEY=value`). Both are valid compose and both appear in this file.
    """
    env = spec.get("environment", {})
    if isinstance(env, dict):
        return {key: "" if value is None else str(value) for key, value in env.items()}
    out = {}
    for item in env:
        key, sep, value = str(item).partition("=")
        out[key] = value if sep else ""
    return out


SHELL_DEFAULT_RE = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*:?-(.*)\}$")
SHELL_INTERPOLATION_RE = re.compile(
    r"^\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?P<colon>:?)-(?P<default>.*)\}$"
)


def resolve_shell_default(value):
    """Resolve compose's `${VAR:-default}` / `${VAR-default}` forms to the default
    an operator gets with nothing exported in their shell.

    `env_map` returns the raw literal from compose.dev.yaml, so a var written as
    `${OTEL_EXPORTER_OTLP_ENDPOINT-http://otel-collector:4318}` comes back as
    that literal string, not the resolved endpoint. The acceptance criterion
    here is about what a plain `curie local up` does with no shell overrides,
    so the default inside the wrapper is the value under test, not the wrapper. A
    plain literal (no `${...}` wrapper) passes through untouched.

    Both forms appear on purpose and resolve identically for THIS helper's
    question (nothing exported), so both are accepted: `:-` substitutes the
    default when the var is unset OR empty, while `-` substitutes only when it is
    UNSET. The endpoint uses the `-` form specifically so `curie local up
    --minimal` can suppress it with an explicit empty override; under `:-` an
    empty value could never mean "no endpoint". Still intentionally narrow: no
    `${VAR}` or `${VAR:?err}` forms. Nested defaults recurse.
    """
    if value is None:
        return None
    match = SHELL_DEFAULT_RE.match(value)
    return resolve_shell_default(match.group(1)) if match else value


def resolve_shell_interpolation(value, environ):
    """Resolve the two Compose default operators used by this contract."""
    match = SHELL_INTERPOLATION_RE.match(value or "")
    if not match:
        return value
    name = match.group("name")
    if name not in environ:
        return resolve_shell_interpolation(match.group("default"), environ)
    resolved = environ[name]
    if match.group("colon") and resolved == "":
        return resolve_shell_interpolation(match.group("default"), environ)
    return resolved


def compose_docs():
    """The dev document and the generated release document, parsed and labelled."""
    generate = load_generate()
    return [
        ("compose.dev.yaml", yaml.safe_load(DEV_TEXT)),
        ("compose.release.yaml", yaml.safe_load(generate(DEV_TEXT, OTEL_TEXT, version="9.9.9"))),
    ]


def test_runner_network_excludes_data_tier():
    """The dedicated `curie_runner` network carries only the runner's
    documented dependencies, never the data tier (#631).

    A hardened runner joins `curie_runner` (CURIE_DOCKER_NETWORK). Membership
    of that network is the local mirror of the K8s data-tier NetworkPolicy: the
    stores (postgres/valkey/rustfs/clickhouse) must NOT be on it, so a
    trusted-but-buggy bundle cannot reach their embedded credentials by service
    name, while otel-collector (telemetry), ollama (local model), and curie-api
    (state) must be, so the documented flows still resolve.
    """
    runner_net = "curie_runner"
    data_tier = {"postgres", "valkey", "rustfs", "clickhouse"}
    required_members = {"otel-collector", "ollama", "curie-api"}
    for label, doc in compose_docs():
        # The network is declared with an explicit, project-independent name so
        # `--network curie_runner` resolves regardless of the compose project.
        networks = doc.get("networks") or {}
        assert runner_net in networks, f"{label}: {runner_net} network not declared"
        assert (networks[runner_net] or {}).get("name") == runner_net, (
            f"{label}: {runner_net} must pin an explicit `name:` so the worker's "
            f"--network {runner_net} resolves regardless of compose project name"
        )

        def members_of(svc, _doc=doc):
            nets = _doc["services"][svc].get("networks") or []
            # networks may be a list or a mapping; normalize to a set of names.
            return set(nets) if isinstance(nets, list) else set(nets.keys())

        for store in data_tier & set(doc["services"]):
            assert runner_net not in members_of(store), (
                f"{label}: data-tier service {store!r} is ON the {runner_net} "
                f"network; a runner could reach the store's credentials directly"
            )
        for dep in required_members & set(doc["services"]):
            assert runner_net in members_of(dep), (
                f"{label}: {dep!r} is NOT on the {runner_net} network; a hardened "
                f"runner cannot resolve its documented dependency by name"
            )


def test_dispatcher_api_base_url_is_in_network():
    """The dispatcher points at the API by compose service name, not localhost.

    `http://curie-api:8000` is the in-network form the UI already uses
    (`CURIE_API_TARGET`). The published host port (28000) is correct only for
    the host-networked worker and is unreachable from the dispatcher's bridge
    network.
    """
    for label, doc in compose_docs():
        env = env_map(doc["services"]["curie-dispatcher"])
        assert env.get("CURIE_API_URL") == "http://curie-api:8000", (
            f"{label}: curie-dispatcher CURIE_API_URL is "
            f"{env.get('CURIE_API_URL')!r}; the dispatcher cannot reach the "
            f"API and Slack approval clicks dead-end"
        )


def test_dispatcher_api_key_matches_the_api():
    """The dispatcher authenticates with the key the API actually accepts.

    Asserted as a relationship between the two services rather than against the
    literal dev key, so rotating the key on one side without the other fails
    here instead of at click time with a 401.
    """
    for label, doc in compose_docs():
        dispatcher = env_map(doc["services"]["curie-dispatcher"])
        api = env_map(doc["services"]["curie-api"])

        assert "CURIE_API_KEY" in dispatcher, (
            f"{label}: curie-dispatcher has no CURIE_API_KEY; its auth to the "
            f"API is an accident of two defaults agreeing"
        )
        assert dispatcher["CURIE_API_KEY"] == api["API_KEY"], (
            f"{label}: curie-dispatcher CURIE_API_KEY "
            f"{dispatcher['CURIE_API_KEY']!r} != curie-api API_KEY "
            f"{api['API_KEY']!r}; approval resolve calls will be rejected"
        )


def test_approval_chat_attester_secret_is_independent_and_minimally_distributed():
    """Slack approval attestation uses its own HMAC key on both compose forms.

    The API key authenticates administrators and must not also let its holder
    forge a Slack click. Only the attestation producer (dispatcher) and verifier
    (API) receive this key; the worker and browser UI must not.
    """
    env_name = "CURIE_APPROVAL_CHAT_ATTESTER_SECRET"
    for label, doc in compose_docs():
        services = doc["services"]
        dispatcher = env_map(services["curie-dispatcher"])
        api = env_map(services["curie-api"])

        assert dispatcher.get(env_name), f"{label}: dispatcher has no {env_name}"
        assert api.get(env_name), f"{label}: API has no {env_name}"
        assert dispatcher[env_name] == api[env_name], (
            f"{label}: dispatcher and API use different chat attestation keys"
        )
        assert api[env_name] != api["API_KEY"], (
            f"{label}: chat attestation key reuses the platform API key"
        )
        for service_name in ("curie-worker", "curie-ui"):
            assert env_name not in env_map(services[service_name]), (
                f"{label}: {service_name} must not receive the chat attestation key"
            )


def test_dispatcher_depends_on_api_healthy():
    """The dispatcher waits for the API to be healthy before it starts.

    The API block already publishes a healthcheck (the UI depends on it the same
    way), so this is the ordering guarantee that keeps the dispatcher's boot
    preflight a backstop rather than a race.
    """
    for label, doc in compose_docs():
        depends = doc["services"]["curie-dispatcher"].get("depends_on", {})
        assert isinstance(depends, dict), (
            f"{label}: curie-dispatcher depends_on is list form, which carries "
            f"no condition; the API dependency needs service_healthy"
        )
        entry = depends.get("curie-api")
        assert isinstance(entry, dict) and entry.get("condition") == "service_healthy", (
            f"{label}: curie-dispatcher does not depend on curie-api with "
            f"condition service_healthy (got {entry!r})"
        )


def test_worker_api_base_url_stays_host_local():
    """Regression guard: the worker's localhost:28000 is CORRECT. Do not "fix" it.

    #442 names the worker's `CURIE_API_URL=http://localhost:28000` as the
    defect. It is not. The worker runs `network_mode: host`, so the published
    host port is exactly right for it, and rewriting this line to the in-network
    form breaks the worker. This test passes today and must keep passing.
    """
    for label, doc in compose_docs():
        worker = doc["services"]["curie-worker"]
        assert worker.get("network_mode") == "host", (
            f"{label}: curie-worker is no longer host-networked; the premise of "
            f"its localhost:28000 API URL has changed"
        )
        env = env_map(worker)
        assert env.get("CURIE_API_URL") == "http://localhost:28000", (
            f"{label}: curie-worker CURIE_API_URL is "
            f"{env.get('CURIE_API_URL')!r}, expected http://localhost:28000 "
            f"(host-networked: the published port is the correct form here)"
        )


def collector_http_port():
    """The port the shipped collector actually listens on for OTLP/HTTP.

    Read from the collector's own config rather than hardcoded, so moving the
    receiver port without repointing the worker fails here instead of shipping a
    worker aimed at a closed port. The collector serves OTLP over both gRPC
    (4317) and HTTP (4318); the worker's endpoint is an `http://` URL, so the
    http receiver is the one it must match.
    """
    protocols = yaml.safe_load(OTEL_TEXT)["receivers"]["otlp"]["protocols"]
    return protocols["http"]["endpoint"].rsplit(":", 1)[1]


def test_worker_traces_to_shipped_collector_by_default():
    """The worker exports traces to the collector this file ships, by default.

    #545: `curie local up` boots otel-collector + Langfuse, but the deployed
    local tier exported ZERO traces because curie-worker was never given
    OTEL_EXPORTER_OTLP_ENDPOINT, and CURIE_DOCKER_NETWORK defaulted to empty
    so spawned sandbox containers could not resolve otel-collector by name.
    Both must default to values that work with no manual flags, matching the
    documented manual recipe (README.md).

    This pins the DEFAULT (full-profile) `curie local up`. `--minimal` selects
    the `core` profile, which starts no collector, and suppresses the endpoint by
    exporting it empty -- see `up_minimal_suppresses_otel_endpoint` in
    cli/src/local.rs, which is where the profile choice lives.
    """
    expected_runner = f"http://otel-collector:{collector_http_port()}"
    expected_worker = "http://127.0.0.1:24318"
    for label, doc in compose_docs():
        assert "otel-collector" in doc["services"], (
            f"{label}: otel-collector service not found in the compose document"
        )
        env = env_map(doc["services"]["curie-worker"])
        otel_endpoint = resolve_shell_default(env.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
        assert otel_endpoint == expected_worker, (
            f"{label}: curie-worker OTEL_EXPORTER_OTLP_ENDPOINT resolves to "
            f"{otel_endpoint!r}, expected {expected_worker!r}; the host-network "
            "worker cannot resolve a Compose service name"
        )
        runner_endpoint = resolve_shell_default(
            env.get("CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT")
        )
        assert runner_endpoint == expected_runner, (
            f"{label}: runner OTLP endpoint resolves to {runner_endpoint!r}, "
            f"expected {expected_runner!r} on the isolated runner network"
        )
        docker_network = resolve_shell_default(env.get("CURIE_DOCKER_NETWORK"))
        assert docker_network == "curie_runner", (
            f"{label}: curie-worker CURIE_DOCKER_NETWORK resolves to "
            f"{docker_network!r}, expected curie_runner (#631): the dedicated, "
            "data-tier-free runner network onto which otel-collector, ollama, and "
            "curie-api are multi-homed so a hardened runner resolves its "
            "documented dependencies by name without reaching the stores"
        )


@pytest.mark.parametrize("service_name", ["curie-api", "curie-dispatcher"])
def test_platform_services_export_to_shipped_collector_by_default(service_name):
    """Every long-lived Python service gets the standard OTLP endpoint.

    A worker-only default leaves API request/commit-poll spans and dispatcher
    ingress spans as disconnected stderr diagnostics.  Assert the raw Compose
    interpolation as well as its unset-shell result: the single-dash form is
    load-bearing because an explicitly empty value is the supported no-endpoint
    control for the core/minimal profile.

    The same assertions run against the generated release document so the
    shipped compose cannot silently lose telemetry wiring during generation.
    """
    expected = f"http://otel-collector:{collector_http_port()}"
    for label, doc in compose_docs():
        env = env_map(doc["services"][service_name])
        raw = env.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        assert raw is not None, f"{label}: {service_name} endpoint is absent"
        assert resolve_shell_interpolation(raw, {}) == expected, (
            f"{label}: {service_name} OTEL_EXPORTER_OTLP_ENDPOINT resolves to "
            f"{resolve_shell_interpolation(raw, {})!r} with the variable unset, "
            f"expected {expected!r}"
        )


@pytest.mark.parametrize(
    ("overrides", "worker_endpoint", "runner_endpoint"),
    [
        ({}, "http://127.0.0.1:24318", "http://otel-collector:4318"),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": ""}, "", ""),
        (
            {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.example.com:4318"},
            "http://collector.example.com:4318", "http://collector.example.com:4318",
        ),
        (
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.example.com:4318",
                "CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT": "",
            },
            "", "http://collector.example.com:4318",
        ),
        (
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4318",
                "CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:24318",
            },
            "http://127.0.0.1:24318", "http://otel-collector:4318",
        ),
    ],
)
def test_worker_endpoint_split_executes_compose_interpolation(
    tmp_path, overrides, worker_endpoint, runner_endpoint
):
    """Use actual Compose interpolation, including explicit-empty negative paths."""
    for label, doc in compose_docs():
        worker_env = env_map(doc["services"]["curie-worker"])
        selected = {
            key: worker_env[key]
            for key in ("OTEL_EXPORTER_OTLP_ENDPOINT", "CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT")
        }
        config = tmp_path / label
        config.write_text(yaml.safe_dump({
            "services": {"worker": {"image": "example-worker", "environment": selected}}
        }))
        clean_env = {key: value for key, value in os.environ.items() if key not in (
            "OTEL_EXPORTER_OTLP_ENDPOINT", "CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT"
        )}
        result = subprocess.run(
            ["docker", "compose", "--env-file", "/dev/null", "-f", str(config),
             "config", "--format", "json"],
            env={**clean_env, **overrides}, capture_output=True, text=True, check=True,
        )
        rendered = json.loads(result.stdout)["services"]["worker"]["environment"]
        assert rendered["OTEL_EXPORTER_OTLP_ENDPOINT"] == worker_endpoint
        assert rendered["CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT"] == runner_endpoint


@pytest.mark.parametrize("service_name", ["curie-api", "curie-dispatcher"])
def test_platform_service_endpoint_explicit_empty_disables_export(service_name):
    """The minimal/core path can explicitly suppress the full-stack default.

    Compose `${VAR-default}` keeps an exported empty string, whereas
    `${VAR:-default}` replaces it with the collector endpoint.  Exercise that
    semantic independently of Docker availability so this remains a fast,
    durable regression for both development and generated release compose.
    """

    for label, doc in compose_docs():
        raw = env_map(doc["services"][service_name]).get("OTEL_EXPORTER_OTLP_ENDPOINT")
        assert raw is not None, f"{label}: {service_name} endpoint is absent"
        resolved = resolve_shell_interpolation(raw, {"OTEL_EXPORTER_OTLP_ENDPOINT": ""})
        assert resolved == "", (
            f"{label}: {service_name} resolves an explicitly empty endpoint to "
            f"{resolved!r}; `${{VAR:-default}}` would silently re-enable export "
            "during core/minimal no-endpoint mode"
        )


# --- Durable Collector reception (#1818, #1819) ---------------------------

COLLECTOR_SELF_METRICS_0_119 = {
    "otelcol_receiver_accepted_spans",
    "otelcol_receiver_accepted_log_records",
    "otelcol_receiver_accepted_metric_points",
    "otelcol_exporter_sent_spans",
    "otelcol_exporter_sent_log_records",
    "otelcol_exporter_sent_metric_points",
    "otelcol_exporter_send_failed_spans",
    "otelcol_exporter_send_failed_log_records",
    "otelcol_exporter_send_failed_metric_points",
    "otelcol_exporter_enqueue_failed_spans",
    "otelcol_exporter_enqueue_failed_log_records",
    "otelcol_exporter_enqueue_failed_metric_points",
    "otelcol_exporter_queue_size",
    "otelcol_exporter_queue_capacity",
}


def _assert_component_references_resolve(config, label):
    """Pin the Collector's runtime graph, not merely YAML key presence."""
    components = {
        "receivers": set(config.get("receivers", {})),
        "processors": set(config.get("processors", {})),
        "exporters": set(config.get("exporters", {})),
    }
    pipelines = config.get("service", {}).get("pipelines", {})
    assert set(pipelines) == {"traces", "logs", "metrics"}, (
        f"{label}: Collector must receive all three OTLP signals; got {sorted(pipelines)}"
    )
    for signal, pipeline in pipelines.items():
        for component_type in ("receivers", "processors", "exporters"):
            for reference in pipeline.get(component_type, []):
                assert reference in components[component_type], (
                    f"{label}: {signal} pipeline references undefined "
                    f"{component_type[:-1]} {reference!r}"
                )

        processors = pipeline.get("processors", [])
        assert "memory_limiter" in processors and "batch" in processors, (
            f"{label}: {signal} pipeline must contain memory_limiter and batch; "
            f"got {processors!r}"
        )
        assert processors.index("memory_limiter") < processors.index("batch"), (
            f"{label}: {signal} pipeline must limit memory before batching; "
            f"got {processors!r}"
        )

    extensions = config.get("extensions", {})
    for reference in config.get("service", {}).get("extensions", []):
        assert reference in extensions, (
            f"{label}: service.extensions references undefined extension {reference!r}"
        )
    assert "file_storage" in extensions, f"{label}: file_storage extension is not declared"
    enabled_extensions = config.get("service", {}).get("extensions", [])
    assert "file_storage" in enabled_extensions, (
        f"{label}: file_storage is declared but absent from service.extensions"
    )
    directory = extensions["file_storage"].get("directory")
    assert isinstance(directory, str) and directory.startswith("/"), (
        f"{label}: file_storage.directory must be an absolute durable path, got {directory!r}"
    )
    return directory


def _assert_network_exporters_are_bounded(config, label):
    """Every exporter which crosses a network gets retry plus a disk queue."""
    for name, exporter in config.get("exporters", {}).items():
        exporter_type = name.split("/", 1)[0]
        if exporter_type not in {"otlp", "otlphttp"}:
            continue

        retry = exporter.get("retry_on_failure", {})
        assert retry.get("enabled") is True, f"{label}: {name} retry_on_failure is not enabled"
        assert retry.get("max_interval"), f"{label}: {name} retry max_interval is not bounded"
        assert retry.get("max_elapsed_time") not in (None, "0", "0s"), (
            f"{label}: {name} retry max_elapsed_time must be finite and non-zero"
        )

        queue = exporter.get("sending_queue", {})
        assert queue.get("enabled") is True, f"{label}: {name} sending_queue is not enabled"
        assert queue.get("storage") == "file_storage", (
            f"{label}: {name} queue must persist through file_storage"
        )
        size = queue.get("queue_size")
        assert isinstance(size, int) and 0 < size <= 100_000, (
            f"{label}: {name} queue_size must be a finite positive bound, got {size!r}"
        )


def _collector_storage_mount(service, directory, label):
    mounts = service.get("volumes", [])
    for mount in mounts:
        if isinstance(mount, str):
            parts = mount.split(":")
            if len(parts) >= 2 and directory == parts[1]:
                return parts[0]
        elif mount.get("target") == directory:
            return mount.get("source")
    raise AssertionError(
        f"{label}: Collector file_storage path {directory!r} is not backed by a volume"
    )


def test_dev_collector_receives_every_signal_with_durable_bounded_delivery():
    config = yaml.safe_load(OTEL_TEXT)
    storage_directory = _assert_component_references_resolve(config, "otel/collector-config.yaml")
    _assert_network_exporters_are_bounded(config, "otel/collector-config.yaml")

    # Compose is the explicit development opt-in. Production Helm omits debug;
    # keeping this assertion here makes an accidental production-style no-op in
    # the local feedback loop visible.
    assert "debug" in config["exporters"]
    for signal, pipeline in config["service"]["pipelines"].items():
        assert "debug" in pipeline["exporters"], (
            f"otel/collector-config.yaml: dev {signal} pipeline omitted debug exporter"
        )

    doc = yaml.safe_load(DEV_TEXT)
    collector = doc["services"]["otel-collector"]
    volume_name = _collector_storage_mount(
        collector, storage_directory, "compose.dev.yaml"
    )
    assert volume_name in (doc.get("volumes") or {}), (
        f"compose.dev.yaml: Collector storage volume {volume_name!r} is not declared"
    )
    assert "127.0.0.1:28888:8888" in collector.get("ports", []), (
        "compose.dev.yaml: Collector self-metrics must be reachable from the "
        "developer host without exposing unauthenticated queue/loss state to the LAN"
    )


def test_release_generator_keeps_collector_config_and_storage_in_lockstep():
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="9.9.9")
    release = yaml.safe_load(out)
    inlined = release["configs"]["otel_collector_config"]["content"]
    expected = OTEL_TEXT.replace("${env:", "$${env:")
    assert inlined == expected, "release compose did not inline the complete Collector config"

    config = yaml.safe_load(inlined.replace("$${env:", "${env:"))
    storage_directory = _assert_component_references_resolve(
        config, "generated compose.release.yaml"
    )
    _assert_network_exporters_are_bounded(config, "generated compose.release.yaml")
    collector = release["services"]["otel-collector"]
    volume_name = _collector_storage_mount(
        collector, storage_directory, "generated compose.release.yaml"
    )
    assert volume_name in (release.get("volumes") or {}), (
        f"generated compose.release.yaml: storage volume {volume_name!r} is not declared"
    )


def test_collector_0_119_self_metric_names_remain_pinned():
    """Names consumed by chart-runtime-e2e for the pinned 0.119.0 image."""
    harness = (REPO_ROOT / "scripts" / "chart-runtime-e2e.sh").read_text()
    missing = sorted(name for name in COLLECTOR_SELF_METRICS_0_119 if name not in harness)
    assert not missing, (
        "chart runtime harness no longer checks the Collector 0.119 self-metric "
        f"contract: missing {missing}"
    )


# --- Local-tier connector scope (#1690, ADR 0113 Stream B2) -----------------
#
# `config.py`'s `connector_release` / `connector_namespace` already read
# CURIE_RELEASE / CURIE_NAMESPACE (both default empty), and `binding.py`
# already forwards them into the sandbox boot env unchanged -- that plumbing
# was built for the cluster tier (#1118), where Helm supplies `.Release.Name`
# / `.Release.Namespace`. There is no Helm release at skill/local tier, so
# nothing has ever set these two on curie-worker: a plain `curie local up`
# leaves both empty, `binding.py` emits no connector scope at all, and the
# runner logs "declared but not exercisable in this tier" (#1093) for every
# hosted connector even once a build-form connector container is started
# beside it on `curie_runner`. This is the synthetic scope that makes the
# Docker network alias `connector_render.service_dns(...)` computes for the
# local connector container and the cluster Service DNS the same string.


def test_worker_carries_a_local_tier_connector_scope():
    """`curie-worker` must resolve a non-empty CURIE_RELEASE and CURIE_NAMESPACE
    with no manual export, in both the dev document and the generated release
    document -- the generator copies env through untouched, and a guard on only
    one of the two leaves the other free to drift (the same reasoning as the
    OTEL/CURIE_DOCKER_NETWORK pin above).
    """
    for label, doc in compose_docs():
        env = env_map(doc["services"]["curie-worker"])
        release = resolve_shell_default(env.get("CURIE_RELEASE"))
        namespace = resolve_shell_default(env.get("CURIE_NAMESPACE"))
        assert release, (
            f"{label}: curie-worker CURIE_RELEASE resolves to {release!r}; "
            "config.py's connector_release stays empty and binding.py emits no "
            "connector scope for the sandbox to mount, so every hosted "
            "connector local tier starts is unreachable"
        )
        assert namespace, (
            f"{label}: curie-worker CURIE_NAMESPACE resolves to {namespace!r}; "
            "config.py's connector_namespace stays empty for the same reason"
        )


def assert_init_containers_adopted(compose_text, label):
    """Assert every one-shot init container is adopted via
    `service_completed_successfully` in every profile combo `curie local up`
    can activate.

    `docker compose up --wait` treats a one-shot init container's clean exit(0)
    as a failure unless some service in the up-set depends on it with
    `condition: service_completed_successfully`. `curie local up` activates a
    base profile (core or full), optionally + `local-model`, optionally +
    `slack` -> 8 combos. Every one-shot init started in a combo must be adopted
    by a long-running service that is itself started in that same combo.
    """
    doc = yaml.safe_load(compose_text)
    services = doc["services"]

    combos = []
    for base in ({"core"}, {"full"}):
        for with_model in (False, True):
            for with_slack in (False, True):
                combo = set(base)
                if with_model:
                    combo.add("local-model")
                if with_slack:
                    combo.add("slack")
                combos.append(frozenset(combo))

    def is_started(spec, combo):
        profiles = spec.get("profiles")
        if not profiles:
            return True
        return bool(set(profiles) & combo)

    def is_oneshot(spec):
        return spec.get("restart") == "no"

    def adopts(spec, init):
        """True if this (long-running) service depends on `init` with
        condition service_completed_successfully."""
        depends = spec.get("depends_on")
        if not isinstance(depends, dict):
            # list form carries no condition, or depends_on absent -> no adoption
            return False
        entry = depends.get(init)
        return (
            isinstance(entry, dict) and entry.get("condition") == "service_completed_successfully"
        )

    violations = []
    for combo in combos:
        started = {name: spec for name, spec in services.items() if is_started(spec, combo)}
        for init, init_spec in started.items():
            if not is_oneshot(init_spec):
                continue
            adopted = any(
                other != init and not is_oneshot(other_spec) and adopts(other_spec, init)
                for other, other_spec in started.items()
            )
            if not adopted:
                violations.append((sorted(combo), init))

    assert not violations, (
        f"{label}: one-shot init container(s) unadopted by any "
        f"service_completed_successfully dependency in an activatable profile "
        f"combo: "
        + "; ".join(
            f"init '{init}' unadopted in profiles {profiles}" for profiles, init in violations
        )
    )


def test_dev_compose_init_containers_adopted():
    assert_init_containers_adopted(DEV_TEXT, "compose.dev.yaml")


def test_release_compose_init_containers_adopted():
    generate = load_generate()
    release_text = generate(DEV_TEXT, OTEL_TEXT, version="1.2.3")
    assert_init_containers_adopted(release_text, "compose.release.yaml")


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")
def test_generated_compose_validates_with_docker(tmp_path):
    generate = load_generate()
    out = generate(DEV_TEXT, OTEL_TEXT, version="latest")

    compose_file = tmp_path / "compose.release.yaml"
    compose_file.write_text(out)

    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "config", "-q"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
