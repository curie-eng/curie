"""The factory voice switch mounts only the operator's model claim."""

import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("claim", ["", "voice-model"])
def test_voice_configuration_and_model_mount(claim: str) -> None:
    args = ["--set-string", f"factory.voiceModelClaim={claim}"] if claim else []
    result = subprocess.run(
        [
            "helm", "template", "curie", str(CHART),
            "--show-only", "templates/api.yaml", "--show-only", "templates/dispatcher.yaml",
            "--set", "dispatcher.slack.appToken=xapp-example",
            "--set", "dispatcher.slack.botToken=xoxb-example", *args,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    pods = {}
    for doc in yaml.safe_load_all(result.stdout):
        if doc["kind"] == "Deployment":
            pod = doc["spec"]["template"]["spec"]
            for container in pod["containers"]:
                pods[container["name"]] = (container, pod)
    for name in ("api", "dispatcher"):
        container, _ = pods[name]
        enabled = [v["value"] for v in container["env"] if v["name"] == "FACTORY_VOICE_ENABLED"]
        assert enabled == [str(bool(claim)).lower()]
    api, pod = pods["api"]
    mounts = [v for v in api.get("volumeMounts", []) if v["name"] == "factory-voice-model"]
    volumes = [v for v in pod.get("volumes", []) if v["name"] == "factory-voice-model"]
    if claim:
        assert mounts == [
            {"name": "factory-voice-model", "mountPath": "/var/run/curie/voice", "readOnly": True}
        ]
        assert volumes == [
            {
                "name": "factory-voice-model",
                "persistentVolumeClaim": {"claimName": claim, "readOnly": True},
            }
        ]
    else:
        assert mounts == volumes == []
