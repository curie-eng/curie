"""Execute default and opt-in Helm consumers; no cluster acceptance claim."""

import json
import subprocess
import sys

import yaml

chart = sys.argv[1]
marker = "00000000-0000-4000-8000-000000000001"


def render(values):
    result = subprocess.run(
        ["helm", "template", "acme-bot", chart, "-f", "-"],
        input=json.dumps(values),
        text=True,
        capture_output=True,
        check=False,
    )
    return result


def jobs(result):
    assert result.returncode == 0, result.stderr
    return {
        doc["metadata"]["labels"].get("app.kubernetes.io/component"): doc
        for doc in yaml.safe_load_all(result.stdout)
        if isinstance(doc, dict) and doc.get("kind") == "Job"
    }


def enabled():
    return {"upgradeRecovery": {"enabled": True, "operationId": marker}}


base = jobs(render({}))
opted = jobs(render(enabled()))
for component, events in [
    ("upgrade-drain", "pre-upgrade,pre-rollback"),
    ("schema-migrate", "post-install,pre-upgrade,pre-rollback"),
    ("upgrade-drain-release", "post-upgrade,post-rollback"),
]:
    normal, recovery = base[component], opted[component]
    assert "rollback" not in normal["metadata"]["annotations"]["helm.sh/hook"]
    assert (
        normal["metadata"]["annotations"]["helm.sh/hook-delete-policy"]
        == "before-hook-creation,hook-succeeded"
    )
    assert recovery["metadata"]["annotations"]["helm.sh/hook"] == events
    assert (
        recovery["metadata"]["annotations"]["helm.sh/hook-delete-policy"] == "before-hook-creation"
    )
    assert recovery["metadata"]["labels"]["curie-upgrade-operation"] == marker
    assert recovery["spec"]["template"]["metadata"]["labels"]["curie-upgrade-operation"] == marker
    # The opt-in changes lifecycle metadata only, never executable phase code.
    assert normal["spec"]["template"]["spec"] == recovery["spec"]["template"]["spec"]
    for name in ("activeDeadlineSeconds", "backoffLimit"):
        assert normal["spec"][name] == recovery["spec"][name]
for values in [
    {"upgradeRecovery": {"enabled": "true", "operationId": marker}},
    {"upgradeRecovery": "true"},
    {"upgradeRecovery": {"enabled": True}},
    {"upgradeRecovery": {"enabled": True, "operationId": "inherited-not-a-uuid"}},
    dict(enabled(), worker={"deploy": False}),
    dict(enabled(), worker={"upgradeDrain": {"enabled": False}}),
    dict(enabled(), api={"migrate": {"enabled": False}}),
    dict(enabled(), api={"deploy": False}),
]:
    denied = render(values)
    assert denied.returncode != 0, values
    assert "upgradeRecovery" in denied.stderr, denied.stderr
assert jobs(render({"upgradeRecovery": {"enabled": False, "operationId": marker}})) == base
print(
    "PASS: recovery opt-in, all three retained phase witnesses, "
    "default parity and eight refusing consumers"
)
