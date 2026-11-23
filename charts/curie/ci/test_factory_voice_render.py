"""The factory voice switch mounts only the operator's model claim."""

import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("claim", ["", "voice-model"])
def test_api_voice_configuration_and_mount(claim: str) -> None:
    args = ["--set-string", f"factory.voiceModelClaim={claim}"] if claim else []
    result = subprocess.run(
        ["helm", "template", "curie", str(CHART), "--show-only", "templates/api.yaml", *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    deployment = next(
        doc for doc in yaml.safe_load_all(result.stdout) if doc["kind"] == "Deployment"
    )
    pod = deployment["spec"]["template"]["spec"]
    api = next(c for c in pod["containers"] if c["name"] == "api")
    enabled = [v["value"] for v in api["env"] if v["name"] == "FACTORY_VOICE_ENABLED"]
    assert enabled == [str(bool(claim)).lower()]
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
