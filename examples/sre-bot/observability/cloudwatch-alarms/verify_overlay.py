"""Render-only guard for @spec SRE-CW-7 SRE-CW-8; no cluster qualification."""

from __future__ import annotations

import fnmatch
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import cast

import yaml

OBS = Path(__file__).resolve().parent.parent


# @spec SRE-CW-7.
def render(*overlays: str) -> dict[str, str]:
    command = [
        "helm",
        "template",
        "prometheus",
        "prometheus-community/prometheus",
        "--version",
        "29.27.0",
        "--namespace",
        "observability",
    ]
    for overlay in overlays:
        command += ["-f", str(OBS / overlay)]
    manifest = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    configs = [
        doc["data"]
        for doc in yaml.safe_load_all(manifest)
        if doc and doc["kind"] == "ConfigMap" and "prometheus.yml" in doc.get("data", {})
    ]
    assert len(configs) == 1, "expected exactly one Prometheus server ConfigMap"
    return cast(dict[str, str], configs[0])


# @spec SRE-CW-7.
def rule_files(config: dict[str, str]) -> list[str]:
    return cast(list[str], yaml.safe_load(config["prometheus.yml"])["rule_files"])


# @spec SRE-CW-7.
def check_rule_loading(config: dict[str, str]) -> None:
    executable = os.environ.get("PROMTOOL") or shutil.which("promtool")
    if not executable:
        raise RuntimeError("promtool is required to check rendered rule-file loading")
    with tempfile.TemporaryDirectory(prefix="cloudwatch-rules-") as temporary:
        directory = Path(temporary)
        for name, content in config.items():
            if name != "prometheus.yml":
                (directory / name).write_text(content)
        # Keep rendered file patterns, replacing only the container mount path.
        # Scrape credentials belong to the running cluster, not this rule check.
        rendered = yaml.safe_load(config["prometheus.yml"])
        paths = [path.removeprefix("/etc/config/") for path in rendered["rule_files"]]
        (directory / "prometheus.yml").write_text(yaml.safe_dump({"rule_files": paths}))
        subprocess.run(
            [executable, "check", "config", "prometheus.yml"],
            cwd=directory,
            check=True,
            capture_output=True,
            text=True,
        )


# @spec SRE-CW-7 SRE-CW-8.
def main() -> None:
    defaults = render("prometheus-values.yaml")
    heartbeat = render("prometheus-values.yaml", "alertmanager-heartbeat.yaml")
    for with_heartbeat in (False, True):
        overlays = ["prometheus-values.yaml"]
        if with_heartbeat:
            overlays += ["alertmanager-heartbeat.yaml"]
        combined = render(*overlays, "prometheus-cloudwatch.yaml")
        expected = rule_files(heartbeat if with_heartbeat else defaults)
        assert all(
            any(fnmatch.fnmatchcase(path, pattern) for pattern in rule_files(combined))
            for path in expected
        ), "rule file dropped"
        assert "/etc/config/cloudwatch_rules.yml" in rule_files(combined)
        assert combined["alerting_rules.yml"] == defaults["alerting_rules.yml"]
        assert ("heartbeat_rules.yml" in combined) == with_heartbeat
        if with_heartbeat:
            assert combined["heartbeat_rules.yml"] == heartbeat["heartbeat_rules.yml"]
        assert len(yaml.safe_load(combined["cloudwatch_rules.yml"])["groups"][0]["rules"]) == 2
        check_rule_loading(combined)
    assert "cloudwatch_rules.yml" not in defaults, "default install enables CloudWatch"
    assert "/etc/config/cloudwatch_rules.yml" not in rule_files(defaults)
    reader = list(yaml.safe_load_all((OBS / "cloudwatch-alarms.yaml").read_text()))[0]
    signer = list(yaml.safe_load_all((OBS / "alert-signer.yaml").read_text()))[0]
    assert (
        reader["spec"]["template"]["spec"]["containers"][0]["image"]
        == (signer["spec"]["template"]["spec"]["containers"][0]["image"])
    ), "reader image differs from the existing stdlib example pin"
    print("PASS: default rules preserved; heartbeat and CloudWatch load together only when enabled")


if __name__ == "__main__":
    main()
