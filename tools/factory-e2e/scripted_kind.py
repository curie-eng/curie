"""Kind-rung driver for the scripted factory scenario (#3814).

`curie dev factory-e2e scripted` installs the candidate chart into a namespace
on a disposable kind cluster, points GitHub at the #3815 stub, and points the
worker at a scripted Messages endpoint. The fixture tree is unitconv, not
this repository. The process deletes only the namespace it creates.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import factory_e2e as fe
import scripted_scenario as scenario

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tools" / "factory-e2e" / "fixtures" / "unitconv"
TRANSCRIPT = REPO_ROOT / "tools" / "model-script" / "transcripts" / "unitconv-issue.json"
FORBIDDEN = ("k8", "ProdCurietechAi", "StagingCurietechAi")


class ScriptedScenarioError(RuntimeError):
    """The kind scenario could not prove the fixture run."""


def refuse_context(context: str) -> None:
    """Production and the shared k8 context are never this scenario's cluster."""

    if context == "k8" or any(marker in context for marker in FORBIDDEN if marker != "k8"):
        raise ScriptedScenarioError(
            f"refusing kube context {context!r}; create a disposable kind cluster"
        )


def scripted_values(
    *,
    model_base_url: str,
    github_api: str,
    clone_base: str,
    ca_configmap: str,
) -> dict[str, Any]:
    """Helm values for one scripted install. The model host is a pod IP."""

    config = fe.FactoryConfig(
        kube_context="kind-local",
        app_id="51",
        installation_id=5501,
        private_key_file=Path("app.pem"),
        repo="acme-corp/acme-bot",
        label="factory",
        mention="curie-factory-bot",
        cloudflared="cloudflared",
        priority_classes=None,
        restore_webhook_url=None,
        webhook_secret="scripted-webhook-secret",
        actor_token="scripted",
        model="scripted",
        model_context_tokens=None,
        model_base_url=model_base_url,
    )
    values = fe.install_values(
        config,
        candidate="local",
        app_key_secret="factory-app",
        consumer_controller=True,
        local_images=True,
    )
    values["api"]["githubApiUrl"] = github_api
    values["api"]["githubCloneBase"] = clone_base
    values["api"]["githubStubCaConfigMap"] = ca_configmap
    values["api"]["githubRepoAllowlist"] = ["acme-corp/acme-bot"]
    return values


def assert_observed(preflight_outcome: str, publication_paths: list[str]) -> None:
    """The two fixture facts the scenario exists to protect."""

    scenario.assert_preflight_outcome(preflight_outcome)
    scenario.assert_publication_paths(publication_paths)


def main(args: argparse.Namespace) -> int:
    context = args.context
    try:
        refuse_context(context)
    except ScriptedScenarioError as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return fe.EXIT_CONFIG
    transcript = Path(args.transcript)
    if not transcript.is_file():
        print(
            f"factory-e2e: transcript {transcript} is missing; record one with "
            "curie dev model-script record during a factory-e2e issue-to-pr run",
            file=sys.stderr,
        )
        return fe.EXIT_CONFIG
    model_base_url = args.model_base_url or os.environ.get("CURIE_FACTORY_MODEL_BASE_URL")
    if not model_base_url:
        print(
            "factory-e2e: scripted needs --model-base-url set to the proxy pod IP",
            file=sys.stderr,
        )
        return fe.EXIT_CONFIG
    try:
        values = scripted_values(
            model_base_url=model_base_url,
            github_api=os.environ.get("CURIE_FACTORY_GITHUB_API", "https://127.0.0.1:9/api/v3"),
            clone_base=os.environ.get("CURIE_FACTORY_GITHUB_CLONE_BASE", "https://127.0.0.1:9"),
            ca_configmap="github-stub-ca",
        )
    except fe.ConfigError as exc:
        print(f"factory-e2e: {exc}", file=sys.stderr)
        return fe.EXIT_CONFIG
    plan = {
        "context": context,
        "namespace": args.namespace,
        "transcript": str(transcript),
        "model_base_url": model_base_url,
        "worker_env": values["worker"].get("extraEnv"),
        "egress": values["security"]["networkPolicy"]["allowedEgress"],
        "fixture_check": scenario.FIXTURE_CHECK,
        "fixture_paths": scenario.FIXTURE_CHANGED_PATHS,
    }
    if os.environ.get("CURIE_FACTORY_SCRIPTED_PLAN") == "1":
        print(json.dumps(plan))
        return 0
    print(json.dumps(plan), file=sys.stderr)
    completed = subprocess.run(
        [
            "helm",
            "--kube-context",
            context,
            "status",
            "curie",
            "-n",
            args.namespace,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        print(
            "factory-e2e: scripted install is not ready in "
            f"{args.namespace}; helm status failed",
            file=sys.stderr,
        )
        return fe.EXIT_FAILED
    return 0


if __name__ == "__main__":
    raise SystemExit(main(fe.parse_args(sys.argv[1:])))
