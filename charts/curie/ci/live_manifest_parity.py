#!/usr/bin/env python3
"""Compare a Helm target worker env to the live Deployment and Chart.yaml version.

The 2026-09-04 v0.8.4 to v0.8.5 soak left Helm's retained target carrying
CURIE_RUNNER_TOTAL_TIMEOUT_S while the live worker omitted it, and a retained
worker.extraEnv duplicate made Kubernetes reject the worker patch. helm upgrade
exiting zero is not enough: the target env must be unique, present live, and
the release VERSION must equal charts/curie/Chart.yaml.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from collections import Counter
from pathlib import Path

import yaml

TIMEOUT_ENV = "CURIE_RUNNER_TOTAL_TIMEOUT_S"


def _fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def load_docs(path: Path) -> list[dict]:
    text = path.read_text()
    docs = [doc for doc in yaml.safe_load_all(text) if isinstance(doc, dict)]
    if not docs:
        _fail(f"{path} contained no YAML objects")
    return docs


def _is_worker_deploy(doc: dict) -> bool:
    if doc.get("kind") != "Deployment":
        return False
    labels = (doc.get("metadata") or {}).get("labels") or {}
    if labels.get("app.kubernetes.io/component") == "worker":
        return True
    name = str((doc.get("metadata") or {}).get("name") or "")
    return name.endswith("-worker")


def worker_deploy_from_docs(docs: list[dict], source: str) -> dict:
    deploys = [doc for doc in docs if _is_worker_deploy(doc)]
    labeled = [
        doc
        for doc in deploys
        if ((doc.get("metadata") or {}).get("labels") or {}).get(
            "app.kubernetes.io/component"
        )
        == "worker"
    ]
    chosen = labeled or deploys
    if len(chosen) != 1:
        _fail(f"{source} expected exactly one worker Deployment, found {len(chosen)}")
    return chosen[0]


def worker_env(deploy: dict, source: str) -> list[tuple[str, str | None]]:
    containers = (
        ((deploy.get("spec") or {}).get("template") or {}).get("spec") or {}
    ).get("containers") or []
    named = [c for c in containers if c.get("name") == "worker"]
    if len(named) != 1:
        _fail(f"{source} expected exactly one container named worker, found {len(named)}")
    entries: list[tuple[str, str | None]] = []
    for env in named[0].get("env") or []:
        name = env.get("name")
        if not name:
            _fail(f"{source} worker env entry is missing name")
        entries.append((str(name), env.get("value")))
    return entries


def parse_chart_version(path: Path) -> str:
    for line in path.read_text().splitlines():
        if line.startswith("version:"):
            return line.split(":", 1)[1].strip().strip('"').strip("'")
    _fail(f"{path} has no version:")
    raise AssertionError("unreachable")


def parse_helm_version(path: Path) -> str:
    for line in path.read_text().splitlines():
        if line.startswith("VERSION:"):
            return line.split(":", 1)[1].strip()
    _fail(f"{path} has no VERSION: line from helm get metadata")
    raise AssertionError("unreachable")


def assert_unique(entries: list[tuple[str, str | None]], source: str) -> dict[str, str | None]:
    counts = Counter(name for name, _value in entries)
    duplicates = sorted(name for name, count in counts.items() if count != 1)
    if duplicates:
        _fail(
            f"{source} worker env has duplicate names {duplicates}; "
            "each name must appear exactly once"
        )
    return dict(entries)


def verify(
    *,
    helm_manifest: Path,
    live_deploy: Path,
    helm_metadata: Path,
    chart_yaml: Path,
    expect_env: str = TIMEOUT_ENV,
) -> None:
    helm_docs = load_docs(helm_manifest)
    live_docs = load_docs(live_deploy)
    helm_worker = worker_deploy_from_docs(helm_docs, "helm target")
    live_worker = worker_deploy_from_docs(live_docs, "live")
    helm_env = assert_unique(worker_env(helm_worker, "helm target"), "helm target")
    live_env = assert_unique(worker_env(live_worker, "live"), "live")

    if expect_env not in helm_env:
        _fail(f"helm target worker env is missing {expect_env}")
    if expect_env not in live_env:
        _fail(
            f"live worker omits {expect_env} present on the helm target "
            f"(live names: {sorted(live_env)})"
        )
    if helm_env[expect_env] != live_env[expect_env]:
        _fail(
            f"{expect_env} helm target value {helm_env[expect_env]!r} "
            f"does not match live {live_env[expect_env]!r}"
        )

    missing_live = sorted(name for name in helm_env if name not in live_env)
    if missing_live:
        _fail(f"live worker omits helm target env names {missing_live}")

    chart_version = parse_chart_version(chart_yaml)
    helm_version = parse_helm_version(helm_metadata)
    if helm_version != chart_version:
        _fail(
            f"helm release VERSION {helm_version!r} did not converge to "
            f"chart version {chart_version!r}"
        )

    print(
        f"live-manifest parity: {expect_env}={live_env[expect_env]!r} exactly once; "
        f"target version {helm_version} converged"
    )


def _write(path: Path, text: str) -> Path:
    path.write_text(text)
    return path


def _deploy_yaml(env: list[dict[str, str]]) -> str:
    doc = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "curie-worker",
            "labels": {"app.kubernetes.io/component": "worker"},
        },
        "spec": {
            "template": {
                "spec": {"containers": [{"name": "worker", "env": env}]},
            }
        },
    }
    return yaml.safe_dump(doc, sort_keys=False)


def self_test() -> None:
    timeout = [{"name": TIMEOUT_ENV, "value": "600"}]
    with tempfile.TemporaryDirectory() as raw:
        tmp = Path(raw)
        chart = _write(tmp / "Chart.yaml", 'apiVersion: v2\nname: curie\nversion: 0.8.5\n')
        metadata = _write(
            tmp / "metadata.txt",
            "NAME: curie\nCHART: curie\nVERSION: 0.8.5\nAPP_VERSION: 0.8.5\n",
        )
        helm = _write(tmp / "helm.yaml", _deploy_yaml(timeout))
        live = _write(tmp / "live.yaml", _deploy_yaml(timeout))
        verify(
            helm_manifest=helm,
            live_deploy=live,
            helm_metadata=metadata,
            chart_yaml=chart,
        )

        missing = _write(
            tmp / "live-missing.yaml",
            _deploy_yaml([{"name": "CURIE_DELIVERY_BUDGET_S", "value": "600"}]),
        )
        try:
            verify(
                helm_manifest=helm,
                live_deploy=missing,
                helm_metadata=metadata,
                chart_yaml=chart,
            )
        except SystemExit:
            pass
        else:
            _fail("self-test: live missing timeout unexpectedly passed")

        duplicate = _write(
            tmp / "live-dup.yaml",
            _deploy_yaml(timeout + [{"name": TIMEOUT_ENV, "value": "1700"}]),
        )
        try:
            verify(
                helm_manifest=helm,
                live_deploy=duplicate,
                helm_metadata=metadata,
                chart_yaml=chart,
            )
        except SystemExit:
            pass
        else:
            _fail("self-test: duplicate live timeout unexpectedly passed")

        skew = _write(
            tmp / "metadata-skew.txt",
            "NAME: curie\nCHART: curie\nVERSION: 0.0.0-not-the-chart\n",
        )
        try:
            verify(
                helm_manifest=helm,
                live_deploy=live,
                helm_metadata=skew,
                chart_yaml=chart,
            )
        except SystemExit:
            pass
        else:
            _fail("self-test: version skew unexpectedly passed")

    print("live-manifest parity self-test: negatives rejected, matching fixtures passed")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--helm-manifest", type=Path)
    parser.add_argument("--live-deploy", type=Path)
    parser.add_argument("--helm-metadata", type=Path)
    parser.add_argument("--chart-yaml", type=Path)
    parser.add_argument("--expect-env", default=TIMEOUT_ENV)
    args = parser.parse_args(argv)
    if args.self_test:
        self_test()
        return
    missing = [
        name
        for name in ("helm_manifest", "live_deploy", "helm_metadata", "chart_yaml")
        if getattr(args, name) is None
    ]
    if missing:
        _fail("missing required paths: " + ", ".join(missing))
    verify(
        helm_manifest=args.helm_manifest,
        live_deploy=args.live_deploy,
        helm_metadata=args.helm_metadata,
        chart_yaml=args.chart_yaml,
        expect_env=args.expect_env,
    )


if __name__ == "__main__":
    main()
