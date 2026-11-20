"""Routes the live chain needs that are still in open pull requests (#5808).

Each fixture under ``fixtures/live-github-chain/pending/`` is copied from a
pull request head. The handoff tests feed it through the driver's builder into
the judge. The parity tests compare it with the product once that pull request
has merged, and skip with ``pending #NNNN`` until then. Every test name carries
``pending`` so the coordinator can rerun exactly these after a merge.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

import live_github_chain as lgc
import live_github_chain_judge as judge
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "live-github-chain"
PENDING = FIXTURES / "pending"
PASSING: dict[str, Any] = json.loads((FIXTURES / "record-pass.json").read_text())

RULES_VECTOR = PENDING / "memory-rules-responses.json"
OVERRIDES = PENDING / "overrides-response.json"
WORK_ITEM_RULES = PENDING / "work-item-memory-rules.json"


def _load(path: Path) -> Any:
    return json.loads(path.read_text())


def _record_with(beat: str, section: dict[str, Any]) -> dict[str, Any]:
    record = copy.deepcopy(PASSING)
    record[beat] = section
    return record


def _vector_rule_list() -> dict[str, Any]:
    """The #5852 list response, narrowed to its first rule as the promoted R1."""

    listing = _load(RULES_VECTOR)["list"]
    c = PASSING["beat_c"]
    review = f"{PASSING['identities']['repository_url']}/pull/{c['pull_number']}"
    rule = {
        **listing["rules"][0],
        "state": "proposed",
        "source_url": f"{review}#pullrequestreview-{c['review']['id']}",
    }
    return {**listing, "rules": [rule]}


# --------------------------------------------------------------------------
# #5852: curie cluster memory --rules [--accept|--remove] --json
# --------------------------------------------------------------------------


def test_pending_5852_rule_list_feeds_beat_d_into_the_judge() -> None:
    section = copy.deepcopy(PASSING["beat_d"])
    built = lgc.beat_d_record(
        rules_before={**_load(RULES_VECTOR)["list"], "rules": []},
        pull_merged={
            "merged_at": section["merged_at"],
            "merged_by": {"id": section["merged_by_id"]},
        },
        rules_after=_vector_rule_list(),
        observed_at=section["rules_after_merge"]["observed_at"],
        issue_after=section["issue_after_merge"],
        ticket_comments=section["ticket_comments_after_merge"],
    )
    assert judge.judge_beat("beat_d", _record_with("beat_d", built)).passed
    accepted = {
        **_vector_rule_list(),
        "rules": [{**_vector_rule_list()["rules"][0], "state": "accepted"}],
    }
    built["rules_after_merge"]["output"] = accepted
    refused = judge.judge_beat("beat_d", _record_with("beat_d", built))
    assert "the rule is not proposed" in refused.reasons


def _beat_e(accept: dict[str, Any], reader_exit: int = 1) -> dict[str, Any]:
    rule = _load(RULES_VECTOR)["list"]["rules"][0]
    refusal = {
        "error": "memory_rules.write_access_required: You cannot write to this repository.",
        "fix": "Ask a maintainer of the repository to decide this rule.",
    }
    return lgc.beat_e_record(
        reader_permission={"permission": "read"},
        rule_before=rule,
        reader=(reader_exit, json.dumps(refusal), 1.0),
        rule_after_reader=rule,
        writer=(0, json.dumps(accept), 1.0),
    )


def _rule_record_for_vector() -> dict[str, Any]:
    """The passing record re-keyed to the vector's rule id and accepting principal."""

    accept = _load(RULES_VECTOR)["accept"]
    record = copy.deepcopy(PASSING)
    record["beat_d"]["rules_after_merge"]["output"] = _vector_rule_list()
    record["session_probe"]["writer"]["principal_id"] = accept["rule"]["accepted_by"][
        "principal_id"
    ]
    return record


def test_pending_5852_accept_output_feeds_beat_e_into_the_judge() -> None:
    accept = _load(RULES_VECTOR)["accept"]
    record = _rule_record_for_vector()
    record["beat_e"] = _beat_e(accept)
    assert judge.judge_beat("beat_e", record).passed
    # Counterexample: the pre-#5852 bare rule object the judge once assumed.
    record["beat_e"] = _beat_e(accept["rule"])
    assert not judge.judge_beat("beat_e", record).passed
    # Counterexample: a reader accept that went through.
    record["beat_e"] = _beat_e(accept, reader_exit=0)
    assert not judge.judge_beat("beat_e", record).passed


def test_pending_5852_vector_matches_the_product() -> None:
    product = REPO_ROOT / "tests" / "vectors" / "memory-rules-responses.json"
    if not product.exists():
        pytest.skip(
            "pending #5852: tests/vectors/memory-rules-responses.json is not on this checkout"
        )
    assert _load(product) == _load(RULES_VECTOR)


def test_pending_5852_refusal_codes_match_the_product() -> None:
    source = REPO_ROOT / "apps" / "api" / "src" / "curie_api" / "routers" / "memory_rules.py"
    text = source.read_text() if source.exists() else ""
    if "write_access_required" not in text:
        pytest.skip("pending #5852: the memory_rules refusal codes are not on this checkout")
    for code in judge.READER_REFUSAL_CODES:
        assert f'"{code.split(".", 1)[1]}"' in text, code


# --------------------------------------------------------------------------
# #5857: overrides --memory-rule-proposals and the propose_rule tool
# --------------------------------------------------------------------------


def test_pending_5857_overrides_output_reads_as_proposals_on() -> None:
    fixture = _load(OVERRIDES)
    assert lgc.PROPOSALS_FIELD in fixture["required"]
    assert lgc.overrides_proposals(0, json.dumps(fixture["output"])) is True
    off = {**fixture["output"], lgc.PROPOSALS_FIELD: False}
    assert lgc.overrides_proposals(0, json.dumps(off)) is False
    # Counterexamples: today's v3.1 output has no such field, and a failed read.
    legacy = {k: v for k, v in fixture["output"].items() if k != lgc.PROPOSALS_FIELD}
    assert lgc.overrides_proposals(0, json.dumps(legacy)) is None
    assert lgc.overrides_proposals(1, json.dumps(fixture["output"])) is None


def test_pending_5857_proposal_rows_become_tool_notes_the_judge_reads() -> None:
    c = PASSING["beat_c"]
    rows = [
        {
            "id": r["id"],
            "work_item_id": r["work_item_id"],
            "status": "completed",
            "created_at": r["created_at"],
            "terminal_at": r["finished_at"],
            "objective": r["objective"],
        }
        for r in c["requests"]
    ]
    revision = c["requests"][-1]["id"]
    section = {**copy.deepcopy(c), "requests": lgc.request_records(rows, [revision])}
    assert judge.judge_beat("beat_c", _record_with("beat_c", section)).passed
    section["requests"] = lgc.request_records(rows, [])
    refused = judge.judge_beat("beat_c", _record_with("beat_c", section))
    assert not refused.passed
    assert _load(OVERRIDES)["propose_rule_tool"] == judge.PROPOSE_RULE_TOOL


def test_pending_5857_overrides_schema_matches_the_product() -> None:
    schema = _load(REPO_ROOT / "cli" / "schema" / "overrides.schema.json")
    fixture = _load(OVERRIDES)
    if schema["$id"] != fixture["schema_id"]:
        pytest.skip(f"pending #5857: overrides schema is {schema['$id']} on this checkout")
    result = next(entry for entry in schema["oneOf"] if "required" in entry)
    assert result["required"] == fixture["required"]
    assert set(fixture["output"]) == set(result["properties"])


def test_pending_5857_propose_rule_tool_name_matches_the_runner() -> None:
    source = (REPO_ROOT / "runner" / "src" / "curie_runner" / "tool_names.py").read_text()
    found = re.search(r'^PROPOSE_RULE_TOOL(?:: Final)? = "([^"]+)"', source, re.MULTILINE)
    if found is None:
        pytest.skip("pending #5857: runner tool_names.PROPOSE_RULE_TOOL is not on this checkout")
    assert found.group(1) == _load(OVERRIDES)["propose_rule_tool"] == judge.PROPOSE_RULE_TOOL


def test_pending_5796_proposal_query_reads_columns_that_exist_now(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api.models import ExecutionRequest, MemoryRule

    seen: list[str] = []

    def fake_sql(_p: Any, query: str) -> list[dict[str, Any]]:
        seen.append(query)
        return [{"id": "00000000-0000-4000-8000-000000000009"}]

    monkeypatch.setattr(lgc, "sql_json", fake_sql)
    work_item = "00000000-0000-4000-8000-000000000001"
    assert lgc.proposal_request_ids(Any, work_item) == ["00000000-0000-4000-8000-000000000009"]
    assert MemoryRule.__tablename__ == "memory_rules"
    assert ExecutionRequest.__tablename__ == "execution_requests"
    assert "source_request_id" in MemoryRule.__table__.columns
    assert "work_item_id" in ExecutionRequest.__table__.columns
    assert "FROM memory_rules r JOIN execution_requests e" in seen[0]
    assert f"e.work_item_id = '{work_item}'" in seen[0]
    with pytest.raises(lgc.fe.PreflightFailed):
        lgc.proposal_request_ids(Any, "x' OR '1'='1")


# --------------------------------------------------------------------------
# #5859: the work item memory_rules field and the Learned rules line
# --------------------------------------------------------------------------


def _beat_f(memory_rules: dict[str, Any], body: str) -> dict[str, Any]:
    f = PASSING["beat_f"]
    patch = "@@ -1,0 +1,4 @@\n" + "\n".join(f"+{line}" for line in f["diff_added_lines"])
    return lgc.beat_f_record(
        issue_number=f["issue_number"],
        instruction_set=True,
        labelled_at=f["labelled_at"],
        open_pulls=[{**f["open_pulls"][0], "body": body}],
        work_item={"memory_rules": memory_rules},
        files=[{"filename": "unitconv/tests/test_convert.py", "patch": patch}],
    )


def test_pending_5859_selected_rules_and_annotated_body_feed_beat_f() -> None:
    fixture = _load(WORK_ITEM_RULES)
    assert judge.LEARNED_RULES_LINE.match(fixture["applied_line"])
    section = _beat_f(fixture["selected"], fixture["pull_body_annotated"])
    assert judge.judge_beat("beat_f", _record_with("beat_f", section)).passed
    # Counterexamples: the body before annotation, and an absent pin.
    for memory_rules, body in (
        (fixture["selected"], fixture["pull_body_before"]),
        (fixture["absent"], fixture["pull_body_annotated"]),
    ):
        refused = judge.judge_beat("beat_f", _record_with("beat_f", _beat_f(memory_rules, body)))
        assert not refused.passed


def test_pending_5859_absent_rules_feed_beat_g() -> None:
    fixture = _load(WORK_ITEM_RULES)
    section = copy.deepcopy(PASSING["beat_g"])
    section["work_item"] = {"memory_rules": fixture["absent"]}
    assert judge.judge_beat("beat_g", _record_with("beat_g", section)).passed
    section["work_item"] = {"memory_rules": fixture["selected"]}
    assert not judge.judge_beat("beat_g", _record_with("beat_g", section)).passed


def test_pending_5859_learned_line_and_metadata_match_the_product() -> None:
    try:
        from curie_api import memory_rules_context as context
        from curie_api.schemas.workitems import MemoryRulesMetadata  # type: ignore[attr-defined]
    except ImportError:
        pytest.skip("pending #5859: curie_api.memory_rules_context is not on this checkout")
    fixture = _load(WORK_ITEM_RULES)
    applied = context.AppliedRules(rule_numbers=(1,), sha256=fixture["selected"]["sha256"])
    assert context.applied_line(applied) == fixture["applied_line"]
    assert context.annotate(fixture["pull_body_before"], applied) == fixture["pull_body_annotated"]
    assert list(MemoryRulesMetadata.model_fields) == fixture["metadata_fields"]
    for example in ("selected", "absent"):
        assert MemoryRulesMetadata.model_validate(fixture[example]).model_dump() == fixture[example]


# --------------------------------------------------------------------------
# #5804: the retired result declaration refusal (no open pull request yet)
# --------------------------------------------------------------------------


def test_pending_5804_retired_code_matches_the_product() -> None:
    sources = (REPO_ROOT / "apps" / "api" / "src" / "curie_api").rglob("*.py")
    if not any(judge.BUNDLE_RETIRED_CODE in path.read_text() for path in sources):
        pytest.skip(f"pending #5804: {judge.BUNDLE_RETIRED_CODE} is not on this checkout")
    assert judge.BUNDLE_RETIRED_CODE == "bundle.result_declaration_retired"
