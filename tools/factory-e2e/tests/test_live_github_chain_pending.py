"""Routes the live chain needs, first copied from open pull requests (#5808).

Each fixture under ``fixtures/live-github-chain/pending/`` was copied from a
pull request head. The handoff tests feed it through the driver's builder into
the judge. The parity tests compare it with the product. Every pull request
they waited on has merged into the integration branch, so none of them skips
any more: a product change that drifts from a fixture or from the judge's
assumed constant fails here. Every test name keeps ``pending`` so the
coordinator can rerun exactly these.
"""

from __future__ import annotations

import copy
import io
import json
import re
import shutil
import tarfile
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
    assert _load(product) == _load(RULES_VECTOR)


def test_pending_5852_refusal_codes_match_the_product() -> None:
    source = REPO_ROOT / "apps" / "api" / "src" / "curie_api" / "routers" / "memory_rules.py"
    text = source.read_text()
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


def _turn(ts: str, *tools: str) -> dict[str, Any]:
    """A stored transcript turn, from the runner's producer (``TurnRecord.to_dict``)."""

    from curie_runner.history import ConversationMessage, TurnRecord

    calls = [
        {"type": "tool_use", "id": f"toolu_{i}", "name": name, "input": {}}
        for i, name in enumerate(tools)
    ]
    messages = (
        ConversationMessage(role="user", content="the turn's prompt"),
        ConversationMessage(role="assistant", content=calls or [{"type": "text", "text": "ok"}]),
    )
    return TurnRecord(user="the turn's prompt", assistant="ok", ts=ts, messages=messages).to_dict()


def test_pending_5857_recorded_propose_rule_call_becomes_a_tool_note_the_judge_reads() -> None:
    # The runner records the tool under its MCP name (#5857's head); the
    # transcript turn of the revision request carries it, and nothing else does.
    c = PASSING["beat_c"]
    rows = [
        {
            "id": r["id"],
            "work_item_id": r["work_item_id"],
            "status": "completed",
            "created_at": r["created_at"],
            "terminal_at": r["terminal_at"],
            "terminal_cause": r["terminal_cause"],
            "objective": r["objective"],
        }
        for r in c["requests"]
    ]
    name = _load(OVERRIDES)["propose_rule_mcp_name"]

    def section(*tools: str) -> dict[str, Any]:
        transcript = [_turn("2030-01-01T00:38:55+00:00", *tools)]
        calls = lgc.request_tool_calls(rows, [transcript])
        return {**copy.deepcopy(c), "requests": lgc.request_records(rows, calls)}

    assert judge.judge_beat("beat_c", _record_with("beat_c", section(name))).passed
    refused = judge.judge_beat("beat_c", _record_with("beat_c", section("Edit")))
    assert "the revision trajectory has no propose_rule" in refused.reasons
    assert _load(OVERRIDES)["propose_rule_tool"] == judge.PROPOSE_RULE_TOOL


def test_pending_5857_overrides_schema_matches_the_product() -> None:
    schema = _load(REPO_ROOT / "cli" / "schema" / "overrides.schema.json")
    fixture = _load(OVERRIDES)
    assert schema["$id"] == fixture["schema_id"]
    result = next(entry for entry in schema["oneOf"] if "required" in entry)
    assert result["required"] == fixture["required"]
    assert set(fixture["output"]) == set(result["properties"])


def test_pending_5857_propose_rule_tool_name_matches_the_runner() -> None:
    source = (REPO_ROOT / "runner" / "src" / "curie_runner" / "tool_names.py").read_text()
    found = re.search(r'^PROPOSE_RULE_TOOL(?:: Final)? = "([^"]+)"', source, re.MULTILINE)
    assert found is not None, "runner tool_names.PROPOSE_RULE_TOOL is gone"
    assert found.group(1) == _load(OVERRIDES)["propose_rule_tool"] == judge.PROPOSE_RULE_TOOL


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
    from curie_api import memory_rules_context as context
    from curie_api.schemas.workitems import MemoryRulesMetadata

    fixture = _load(WORK_ITEM_RULES)
    applied = context.AppliedRules(rule_numbers=(1,), sha256=fixture["selected"]["sha256"])
    assert context.applied_line(applied) == fixture["applied_line"]
    assert context.annotate(fixture["pull_body_before"], applied) == fixture["pull_body_annotated"]
    assert list(MemoryRulesMetadata.model_fields) == fixture["metadata_fields"]
    for example in ("selected", "absent"):
        assert MemoryRulesMetadata.model_validate(fixture[example]).model_dump() == fixture[example]


# --------------------------------------------------------------------------
# #5804: the retired result declaration refusal
# --------------------------------------------------------------------------


class _Deployed:
    """The two Preflight fields ``retired_bundle_copy`` reads."""

    def __init__(self, deployed_bundle: Path | None, workdir: Path) -> None:
        self.deployed_bundle = deployed_bundle
        self.workdir = workdir


def _tar_gz(root: Path) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(root.rglob("*")):
            archive.add(path, arcname=str(path.relative_to(root)), recursive=False)
    return buffer.getvalue()


def _upload_refusal(bundle: Path) -> tuple[int, str]:
    """The bundle through the upload route's own validation, as the CLI renders it.

    ``deploy.validate_archive`` is what ``PUT /agents/{id}/versions/{id}/bundle``
    (``routers/bundles.py`` ``upload_bundle``) runs before it stores anything;
    a refusal becomes that route's 422 body, which the CLI's ``expect_ok``
    carries inside its ``{"error", "fix"}`` envelope with exit 1.
    """

    from curie_api import deploy
    from curie_api.config import Settings

    try:
        deploy.validate_archive(_tar_gz(bundle), Settings())
    except deploy.BundleInvalid as exc:
        body = {"detail": {"detail": "bundle failed validation", "errors": exc.errors}}
        error = f"bundle upload failed with 422 Unprocessable Entity: {json.dumps(body)}"
        return 1, json.dumps({"error": error, "fix": "Fix the bundle and deploy again."})
    return 0, json.dumps({"deployed": True})


def _retired_h(deploy_result: tuple[int, str]) -> dict[str, Any]:
    h = PASSING["beat_h"]
    retired = h["retired_bundle"]
    return lgc.beat_h_record(
        label=h["label"],
        observed_at=h["observed_at"],
        work_items=h["work_items"],
        comments=h["comments"],
        events=h["label_events"],
        deploy=deploy_result,
        version_before=retired["version_before"],
        version_after=retired["version_after"],
    )


def test_pending_5804_the_products_retired_refusal_feeds_beat_h(tmp_path: Path) -> None:
    # The driver's own retired copy of the default bundle, through the API's
    # upload validation, into the driver's record and the judge.
    deployed = tmp_path / "deployed"
    shutil.copytree(REPO_ROOT / "examples" / "dark-factory", deployed)
    retired = lgc.retired_bundle_copy(_Deployed(deployed, tmp_path))  # type: ignore[arg-type]
    refusal = _upload_refusal(retired)
    assert refusal[0] == 1
    section = _retired_h(refusal)
    assert judge.BUNDLE_RETIRED_CODE in judge.deploy_refusal_codes(
        section["retired_bundle"]["output"]
    )
    assert judge.judge_beat("beat_h", _record_with("beat_h", section)).passed
    # Counterexample: the same bundle without the file is not refused for it.
    clean = _upload_refusal(deployed)
    refused = judge.judge_beat("beat_h", _record_with("beat_h", _retired_h(clean)))
    assert f"the deploy refusal is not {judge.BUNDLE_RETIRED_CODE}" in refused.reasons


# --------------------------------------------------------------------------
# The revision reply and the merge notice: what the API's tests assert
# --------------------------------------------------------------------------


def _function_source(path: Path, name: str) -> str:
    """The text of one top level function of ``path``, or "" when it is absent."""

    text = path.read_text() if path.exists() else ""
    found = re.search(rf"^(?:async )?def {re.escape(name)}\(", text, re.MULTILINE)
    if found is None:
        return ""
    rest = text[found.end() :]
    end = re.search(r"^(?:@|(?:async )?def |class )", rest, re.MULTILINE)
    return text[found.start() : found.end() + (end.start() if end else len(rest))]


@pytest.mark.parametrize("notice", ["revision_reply", "checks_line", "merge_notice"])
def test_pending_notice_assertions_match_the_api_tests(notice: str) -> None:
    # The driver tests build these posts from the lines copied here; once a
    # copied line no longer reads the same in its source, re-copy it.
    entry = _load(FIXTURES / "notices" / "api-asserted-posts.json")[notice]
    path, _, name = entry["source"].partition("::")
    source = _function_source(REPO_ROOT / path, name)
    missing = [line for line in entry["asserted_lines"] if line not in source]
    assert source and not missing, (
        f"{entry['source']} no longer asserts {missing or 'anything'}; re-copy "
        "fixtures/live-github-chain/notices/api-asserted-posts.json"
    )


# --------------------------------------------------------------------------
# #5794 (merged as #5869): the review keyed request id and the source URL
# --------------------------------------------------------------------------


def test_pending_5794_review_request_id_matches_the_product() -> None:
    # The judge's beat (c) key against the producer the admission path calls.
    from curie_api.forges.github.review_snapshot import event_request_id, review_request_id

    c = PASSING["beat_c"]
    repository_id = PASSING["identities"]["repository_id"]
    pull, review = c["pull_number"], c["review"]["id"]
    assert judge.revision_request_id(repository_id, pull, review) == str(
        review_request_id(int(repository_id), int(pull), int(review))
    )
    # Counterexample: the per-event key the product used before #5794.
    old = event_request_id(int(repository_id), "pull_request_review", int(review))
    assert judge.revision_request_id(repository_id, pull, review) != str(old)


def test_pending_5794_review_source_url_matches_the_product() -> None:
    # The URL admission stores for a review keyed request, which the rule's
    # source_url carries: the product's builder, with the fragment the
    # admission path passes it.
    from curie_api.factory_reply_target import feedback_url

    admission = REPO_ROOT / "apps" / "api" / "src" / "curie_api" / "github_factory_review.py"
    source = _function_source(admission, "admit_parsed_feedback")
    assert 'f"pullrequestreview-{review.id}"' in source
    c = PASSING["beat_c"]
    base, path = "https://github.com", "example-org/unitconv"
    pull, review = c["pull_number"], c["review"]["id"]
    product = feedback_url(base, path, int(pull), f"pullrequestreview-{review}")
    assert judge.review_url(f"{base}/{path}", pull, review) == product
    # Counterexample: the inline comment's URL is not the review's.
    comment = feedback_url(base, path, int(pull), f"discussion_r{review}")
    assert judge.review_url(f"{base}/{path}", pull, review) != comment
