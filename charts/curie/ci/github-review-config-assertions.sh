#!/usr/bin/env bash
# Render and rejection assertions for GitHub review-feedback ingress (#2275).
set -euo pipefail

CHART="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$CHART" <<'PY'
import pathlib
import subprocess
import sys

import yaml


chart = pathlib.Path(sys.argv[1])


def render(*args: str, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [
            "helm",
            "template",
            "curie",
            str(chart),
            "--show-only",
            "templates/api.yaml",
            *args,
        ],
        capture_output=True,
        text=True,
    )
    if expect_success:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0, "invalid review configuration rendered successfully"
    return result


def api_environment(output: str) -> dict[str, dict[str, object]]:
    deployments = [
        document
        for document in yaml.safe_load_all(output)
        if document and document.get("kind") == "Deployment"
    ]
    assert len(deployments) == 1, f"expected one API Deployment, found {len(deployments)}"
    containers = deployments[0]["spec"]["template"]["spec"]["containers"]
    api_containers = [container for container in containers if container["name"] == "api"]
    assert len(api_containers) == 1, f"expected one API container, found {len(api_containers)}"
    return {entry["name"]: entry for entry in api_containers[0].get("env", [])}


def assert_rendered_values(result: subprocess.CompletedProcess[str], enabled: str, interval: str) -> None:
    environment = api_environment(result.stdout)
    expected = {
        "GITHUB_REVIEW_INGRESS_ENABLED": enabled,
        "GITHUB_REVIEW_RECONCILER_INTERVAL_S": interval,
    }
    for name, value in expected.items():
        assert environment.get(name) == {"name": name, "value": value}, (
            f"{name} rendered as {environment.get(name)!r}, expected the first-class value {value!r}"
        )


# Defaults are explicit and inert in the workload that consumes them.
assert_rendered_values(render(), "false", "5")

# An operator override reaches the same API environment entries.
assert_rendered_values(
    render(
        "--set",
        "api.githubReviewIngressEnabled=true",
        "--set-json",
        "api.githubReviewReconcilerIntervalSeconds=2.5",
        "--set-string",
        "api.githubAppId=12345",
        "--set-string",
        "api.githubAppPrivateKey=example-private-key",
        "--set-string",
        "api.githubWebhookSecret=example-review-hmac-secret",
    ),
    "true",
    "2.5",
)

# Exercise Helm's values-schema gate through the real render consumer. Helm
# reports the same schema location as a dotted path in v3 and a JSON pointer in
# v4, so accept those two exact shapes while still requiring the named field.
for arguments, field in (
    (("--set-string", "api.githubReviewIngressEnabled=true"), "githubReviewIngressEnabled"),
    (("--set", "api.githubReviewReconcilerIntervalSeconds=0"), "githubReviewReconcilerIntervalSeconds"),
    (("--set", "api.githubReviewReconcilerIntervalSeconds=-1"), "githubReviewReconcilerIntervalSeconds"),
    (("--set-string", "api.githubReviewReconcilerIntervalSeconds=5"), "githubReviewReconcilerIntervalSeconds"),
):
    failure = render(*arguments, expect_success=False)
    diagnostic = failure.stdout + failure.stderr
    exact_paths = (f"api.{field}", f"/api/{field}")
    assert any(path in diagnostic for path in exact_paths), (
        f"schema refusal did not name {exact_paths}: {diagnostic}"
    )

# Both first-class entries stay reserved even while ingress is disabled.
for name, replacement in (
    ("GITHUB_REVIEW_INGRESS_ENABLED", "api.githubReviewIngressEnabled"),
    ("GITHUB_REVIEW_RECONCILER_INTERVAL_S", "api.githubReviewReconcilerIntervalSeconds"),
):
    failure = render(
        "--set",
        f"api.extraEnv[0].name={name}",
        "--set-string",
        "api.extraEnv[0].value=conflict",
        expect_success=False,
    )
    diagnostic = failure.stdout + failure.stderr
    assert "api.extraEnv" in diagnostic and name in diagnostic and replacement in diagnostic, diagnostic

print("github-review-config-assertions: render, schema, and reserved-env guards passed")
PY
