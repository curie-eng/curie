"""Real local fix pin for #2982: help never prints the CURIE_API_KEY value.

A private Compose API is keyed with a synthetic sentinel. The same sentinel in
CURIE_API_KEY must stay out of every affected `--help` page, and must still be
the credential the CLI sends to that API.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import subprocess
import uuid
from dataclasses import dataclass

import pytest

REPO = pathlib.Path(__file__).parents[3]
SENTINEL = f"sentinel-2982-{uuid.uuid4().hex}"
HELP_PATHS = (("local", "message"), ("cluster", "message"), ("doctor",))


def _run(argv, *, env=None, cwd=REPO, timeout=300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout, check=False
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode:
        raise RuntimeError(
            f"{purpose} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


@pytest.fixture(scope="session")
def source_artifacts(tmp_path_factory: pytest.TempPathFactory):
    owned = tmp_path_factory.mktemp("api-key-help-artifacts")
    binary_value = os.environ.get("CURIE_BIN")
    if binary_value:
        binary = pathlib.Path(binary_value).resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError(f"CURIE_BIN is not an executable file: {binary}")
    else:
        _require(
            _run(["cargo", "build", "--locked"], cwd=REPO / "cli", timeout=1200),
            "building the source CLI",
        )
        metadata = _run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"], cwd=REPO / "cli"
        )
        target = pathlib.Path(
            json.loads(_require(metadata, "reading the Cargo target"))["target_directory"]
        )
        binary = owned / "curie"
        shutil.copy2(target / "debug/curie", binary)
        binary.chmod(0o755)

    supplied_image = os.environ.get("CURIE_LOCAL_DEPLOY_API_IMAGE")
    api_image = supplied_image or f"curie-2982-api:{uuid.uuid4().hex[:12]}"
    if supplied_image:
        _require(_run(["docker", "image", "inspect", api_image]), "finding the API image")
        yield binary, api_image
        return
    _require(
        _run(["docker", "build", "-f", "apps/api/Dockerfile", "-t", api_image, "."], timeout=1200),
        "building the source API image",
    )
    try:
        yield binary, api_image
    finally:
        _require(_run(["docker", "image", "rm", "-f", api_image]), f"removing {api_image}")


@dataclass
class Stack:
    binary: pathlib.Path
    api_url: str
    env: dict[str, str]


@pytest.fixture
def stack(tmp_path: pathlib.Path, source_artifacts):
    binary, api_image = source_artifacts
    project = f"curie-2982-{uuid.uuid4().hex[:10]}"
    override = tmp_path / "compose.override.yaml"
    override.write_text(
        f"""services:
  postgres:
    ports: !override ["127.0.0.1::5432"]
  valkey:
    ports: !override ["127.0.0.1::6379"]
  rustfs:
    ports: !override ["127.0.0.1::9000", "127.0.0.1::9001"]
  curie-migrate:
    image: {api_image}
    pull_policy: never
  curie-api:
    image: {api_image}
    pull_policy: never
    environment:
      API_KEY: {SENTINEL}
      GITHUB_REVIEW_INGRESS_ENABLED: "false"
      GITHUB_APP_ID: ""
      GITHUB_APP_PRIVATE_KEY: ""
      SLACK_BOT_TOKEN: ""
      OTEL_EXPORTER_OTLP_ENDPOINT: ""
      OTEL_EXPORTER_OTLP_PROTOCOL: ""
    ports: !override ["127.0.0.1::8000"]
"""
    )
    env = os.environ.copy()
    env.update({"COMPOSE_PROJECT_NAME": project, "CURIE_LOCAL_IMAGE_TAG": f"test-{project}"})
    for name in ("CURIE_API_KEY", "CURIE_API_URL", "COMPOSE_FILE"):
        env.pop(name, None)
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(REPO / "compose.dev.yaml"),
        "-f",
        str(override),
        "--profile",
        "core",
    ]

    def teardown() -> None:
        _require(
            _run(compose + ["down", "-v", "--remove-orphans"], env=env),
            "stopping the private stack",
        )
        left = _run(
            ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"]
        )
        volumes = _run(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ]
        )
        if (
            _require(left, "listing containers").strip()
            or _require(volumes, "listing volumes").strip()
        ):
            raise RuntimeError(f"private project {project} left resources behind")

    try:
        _require(
            _run(
                compose
                + [
                    "up",
                    "-d",
                    "--wait",
                    "--wait-timeout",
                    "240",
                    "postgres",
                    "valkey",
                    "rustfs-perms",
                    "rustfs",
                    "rustfs-init",
                    "curie-migrate",
                    "curie-api",
                ],
                env=env,
                timeout=300,
            ),
            "starting the private API stack",
        )
        port = _require(
            _run(compose + ["port", "curie-api", "8000"], env=env), "finding the API port"
        )
        yield Stack(binary, f"http://127.0.0.1:{port.strip().rsplit(':', 1)[1]}", env)
    finally:
        teardown()


def test_help_hides_the_api_key_that_still_authenticates(stack: Stack) -> None:
    env = dict(stack.env, CURIE_API_KEY=SENTINEL, CURIE_API_URL=stack.api_url)

    for path in HELP_PATHS:
        result = _run([str(stack.binary), *path, "--help"], env=env)
        text = result.stdout + result.stderr
        assert result.returncode == 0, f"{path} --help failed\n{text}"
        assert SENTINEL not in text, f"{' '.join(path)} --help printed CURIE_API_KEY"
        assert "--api-key" in text and "CURIE_API_KEY" in text, (
            f"{path} help lost --api-key\n{text}"
        )

    # The env key authenticates: listing agents succeeds and the unknown
    # agent is reported as missing, with the key itself never printed.
    missing = f"no-such-agent-{uuid.uuid4().hex[:8]}"
    accepted = _run([str(stack.binary), "local", "versions", missing], env=env)
    accepted_text = accepted.stdout + accepted.stderr
    assert accepted.returncode != 0
    assert "401" not in accepted_text, accepted_text
    assert f'no agent found matching "{missing}"' in accepted_text, accepted_text
    assert SENTINEL not in accepted_text

    # Negative control: without the env key the CLI falls back to its default,
    # which this API rejects, so the pass above came from CURIE_API_KEY.
    env_without_key = {k: v for k, v in env.items() if k != "CURIE_API_KEY"}
    rejected = _run([str(stack.binary), "local", "versions", missing], env=env_without_key)
    rejected_text = rejected.stdout + rejected.stderr
    assert rejected.returncode != 0
    assert "401 Unauthorized" in rejected_text, rejected_text
