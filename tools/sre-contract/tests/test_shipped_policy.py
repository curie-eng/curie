"""Execute the shipped bundle guard, then remove each restored supported read."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
READS = (
    "grafana/query_loki_logs",
    "grafana/alerting_manage_rules",
    "tempo/search_traces",
    "tempo/get_trace",
    "tempo/list_trace_tags",
    "tempo/list_trace_tag_values",
    "self-upgrade/latest_release",
)


def test_shipped_supported_reads_and_each_missing_grant(tmp_path):
    bundle = tmp_path / "sre-bot"
    shutil.copytree(ROOT / "examples/sre-bot", bundle)
    manifest = bundle / ".claude-plugin/plugin.json"
    healthy = manifest.read_text()

    def check():
        return subprocess.run(
            [sys.executable, str(ROOT / "tools/sre-contract/check.py"), "--bundle", str(bundle)],
            capture_output=True,
            text=True,
            timeout=30,
        )

    baseline = check()
    assert baseline.returncode == 0, baseline.stderr
    for canonical in READS:
        broken = json.loads(healthy)
        broken["toolPolicy"]["allow"].remove(canonical)
        manifest.write_text(json.dumps(broken))
        result = check()
        assert result.returncode == 1
        assert f"{canonical}: expected allow, got deny" in result.stderr
        manifest.write_text(healthy)
        restored = check()
        assert restored.returncode == 0, restored.stderr
