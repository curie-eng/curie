"""Plan-gated branch rules on the factory-e2e GitHub stub.

``plan_gated_branch_rules`` is an instance attribute. ``GithubStub._rest`` keeps
its signature, and both subclasses that override ``_rest`` still reach the new
answer.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest
from c4_admitted_http import tracker_stub_type
from c4_dynamic_base import dynamic_stub_type
from curie_api.forges.github.code_host import PLAN_GATED_BRANCH_RULES_MESSAGE

ROOT = Path(__file__).resolve().parents[3]
RULES = "/repos/acme-corp/acme-bot/rules/branches/main"


def _github_module() -> Any:
    source = ROOT / "tools/github-stub/github_stub.py"
    spec = importlib.util.spec_from_file_location("github_stub_plan_gate", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(params=["trackers", "dynamic"], ids=["c4_admitted_http", "c4_dynamic_base"])
def rules_stub(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    module = _github_module()
    kind = (
        tracker_stub_type(module, redact=lambda text: text)
        if request.param == "trackers"
        else dynamic_stub_type(module, redact=lambda text: text)
    )
    recording = json.loads((ROOT / "tools/github-stub/recordings/curie-pr-3400.json").read_text())
    return kind(
        tmp_path / "github",
        recording,
        seed_tree=ROOT / "tools/factory-e2e/fixtures/unitconv",
        issue_file=ROOT / "tools/factory-e2e/fixtures/unitconv-issue.md",
    )


def test_branch_rules_stay_absent_until_the_plan_gate_flag(rules_stub: Any) -> None:
    status, body = rules_stub._rest("GET", RULES, {}, None)
    assert status == 404
    assert body == {"message": "Not Found"}
    assert "private" not in rules_stub._repository_row()

    rules_stub.plan_gated_branch_rules = True
    status, body = rules_stub._rest("GET", RULES, {}, None)
    assert status == 403
    assert body["message"] == PLAN_GATED_BRANCH_RULES_MESSAGE
    assert rules_stub._repository_row()["private"] is True
