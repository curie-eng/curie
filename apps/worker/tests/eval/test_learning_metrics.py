"""Behavioral contracts for CLI learning observations and classification."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest
from curie_worker.eval.learning_metrics import (
    CaseStatus,
    DecisionResult,
    EvidenceError,
    LearningClass,
    Mutation,
    StateSnapshot,
    Thresholds,
    ToolEvidence,
    diff_state,
    match_tool_results,
    parse_cli_snapshot,
    score_state,
    summarize,
)

DURABLE = frozenset(
    {
        LearningClass.SKILL,
        LearningClass.MEMORY_FACT,
        LearningClass.INSTRUCTION,
        LearningClass.SCHEDULE,
    }
)
DEFAULT_THRESHOLDS = Thresholds()
FIXTURES = Path(__file__).parents[2] / "src/curie_worker/eval/fixtures"
CASE_IDS = tuple(f"F{family}-P{paraphrase}" for family in range(1, 9) for paraphrase in range(1, 4))
FAMILY_CLASS = {
    1: LearningClass.SKILL,
    2: LearningClass.INSTRUCTION,
    3: LearningClass.MEMORY_FACT,
    4: LearningClass.SCHEDULE,
    5: LearningClass.SCHEDULE,
    6: LearningClass.CLARIFY,
    7: LearningClass.NOTHING,
    8: LearningClass.SKILL,
}


def _fact(**changes):
    return {
        "id": "fact-example",
        "scope": "agent",
        "channel": None,
        "statement": "The sample reference is violet-73.",
        "author": "U0EXAMPLE1",
        "stated_at": "2026-01-01T00:00:00Z",
        **changes,
    }


def _skill(**changes):
    return {
        "id": "skill-example",
        "slug": "sample-ordering",
        "revision": 1,
        "description": "Reorder three words using the saved convention.",
        "author": "U0EXAMPLE1",
        "updated_at": "2026-01-01T00:00:00Z",
        **changes,
    }


def _schedule(**changes):
    return {
        "id": "schedule-example",
        "revision": 1,
        "state": "active",
        "cron": None,
        "run_at": "2026-01-01T10:00:00Z",
        "timezone": "Etc/UTC",
        "prompt": "Post sample-message in the requesting channel.",
        "created_by": "U0EXAMPLE1",
        **changes,
    }


def _snapshot(*, facts=(), skills=(), schedules=(), available=DURABLE):
    return StateSnapshot(
        skills=tuple(skills), facts=tuple(facts), schedules=tuple(schedules), available=available
    )


def _parse(*, facts=(), skills=(), schedules=(), available=DURABLE):
    return parse_cli_snapshot(
        agent="acme-bot",
        skills=list(skills),
        memory={"agent": "acme-bot", "entries": [], "facts": list(facts)},
        schedules={"agent_schedules": list(schedules)},
        available=available,
    )


def _predicate(name, kind, target, **arguments):
    if name in {"same_id", "revision_increased", "unchanged_other_stores"}:
        arguments = {"value": True, **arguments}
    return {"name": name, "class": kind.value, "target": target, **arguments}


def _score(before, after, *predicates):
    return score_state(before, after, {"predicates": list(predicates)})


def _mutation(kind=LearningClass.MEMORY_FACT, item_id=None):
    identities = {
        LearningClass.SKILL: "skill-example",
        LearningClass.MEMORY_FACT: "fact-example",
        LearningClass.INSTRUCTION: "instruction-example",
        LearningClass.SCHEDULE: "schedule-example",
    }
    identity = identities[kind] if item_id is None else item_id
    if kind is LearningClass.SKILL:
        row = _skill(id=identity)
    elif kind is LearningClass.SCHEDULE:
        row = _schedule(id=identity)
    elif kind is LearningClass.INSTRUCTION:
        row = _fact(id=identity, kind="instruction", archived=False)
    else:
        row = _fact(id=identity)
    return Mutation(kind=kind, item_id=identity, operation="create", before=None, after=row)


def _result(base="F3-P1", *, repetition=1, expected=None, predicted=None, **changes):
    kind = expected or FAMILY_CLASS[int(base[1])]
    observed = frozenset({kind}) if predicted is None else frozenset(predicted)
    return DecisionResult(
        case_id=f"{base}-R{repetition}",
        decision_id=changes.pop("decision_id", "teach"),
        expected=kind,
        predicted=observed,
        status=changes.pop("status", CaseStatus.PASS),
        mutations=changes.pop(
            "mutations", tuple(_mutation(value) for value in observed if value in DURABLE)
        ),
        attempts=changes.pop("attempts", ()),
        distractor=changes.pop("distractor", kind is LearningClass.NOTHING),
        correction_duplicates=changes.pop("correction_duplicates", 0),
        detail=changes.pop("detail", "Serialized observation."),
        **changes,
    )


def _inventory(repetitions=1):
    return [
        _result(base, repetition=rep, **({"decision_id": "correct"} if base[1] == "8" else {}))
        for rep in range(1, repetitions + 1)
        for base in CASE_IDS
    ]


def _summary(rows, *, repetitions=1, thresholds=DEFAULT_THRESHOLDS):
    return summarize(
        rows, expected_case_ids=CASE_IDS, repetitions=repetitions, thresholds=thresholds
    )


def test_six_classes_and_canonical_thresholds():
    assert {value.value for value in LearningClass} == {
        "skill",
        "memory-fact",
        "instruction",
        "schedule",
        "clarify",
        "nothing",
    }
    assert Thresholds() == Thresholds(
        precision=0.9,
        recall=0.8,
        falsewrite=0.05,
        correction_duplicates=0,
        heldout_control_improvement=0.0,
        scheduled_deliveries=1,
    )


def test_cli_readers_keep_canonical_rows_and_only_legacy_memory_defaults():
    fact = _fact(statement="Always close replies with violet-73.")
    payload = {
        "agent": "acme-bot",
        "entries": [{"index": 0, "content": "old append log"}],
        "facts": [fact],
    }
    original = deepcopy(payload)
    snapshot = parse_cli_snapshot(
        agent="acme-bot",
        skills=[],
        memory=payload,
        schedules={"agent_schedules": []},
        available=DURABLE,
    )
    assert snapshot.facts == ({**fact, "kind": "fact", "archived": False},)
    assert payload == original
    fact["statement"] = "Changed caller input."
    assert snapshot.facts[0]["statement"] == "Always close replies with violet-73."
    assert not snapshot.skills
    assert not snapshot.schedules


def test_instruction_kind_is_typed_and_archived_rows_remain_observable():
    snapshot = _parse(
        facts=[
            _fact(),
            _fact(id="instruction-example", kind="instruction", archived=True),
        ]
    )
    assert snapshot.facts[0]["kind"] == "fact"
    assert snapshot.facts[1]["kind"] == "instruction"
    assert snapshot.facts[1]["archived"] is True
    empty = _snapshot()
    active = _predicate("active_count", LearningClass.INSTRUCTION, "instruction-example", value=0)
    assert _score(empty, snapshot, active).passed is True


def test_skills_are_a_top_level_array_and_schedules_use_only_agent_schedules():
    skill, schedule = _skill(), _schedule()
    snapshot = _parse(skills=[skill], schedules=[schedule])
    assert snapshot.skills == (skill,)
    assert snapshot.schedules == (schedule,)
    with pytest.raises(EvidenceError):
        parse_cli_snapshot(
            agent="acme-bot",
            skills={"skills": [skill]},
            memory={"agent": "acme-bot", "entries": [], "facts": []},
            schedules={"agent_schedules": []},
            available=DURABLE,
        )
    with pytest.raises(EvidenceError):
        parse_cli_snapshot(
            agent="acme-bot",
            skills=[],
            memory={"agent": "acme-bot", "entries": [], "facts": []},
            schedules={"schedules": [{"hooks": [schedule]}]},
            available=DURABLE,
        )


def test_capability_absence_must_be_admitted_before_missing_input():
    available = frozenset({LearningClass.MEMORY_FACT})
    snapshot = parse_cli_snapshot(
        agent="acme-bot",
        skills=None,
        memory={"agent": "acme-bot", "entries": [], "facts": [_fact()]},
        schedules=None,
        available=available,
    )
    assert snapshot.available == available
    assert len(snapshot.facts) == 1
    assert snapshot.skills == snapshot.schedules == ()
    with pytest.raises(EvidenceError):
        parse_cli_snapshot(
            agent="acme-bot", skills=None, memory=None, schedules=None, available=available
        )


@pytest.mark.parametrize("invalid", [LearningClass.CLARIFY, LearningClass.NOTHING])
def test_available_contains_only_durable_capabilities(invalid):
    with pytest.raises(EvidenceError):
        _parse(available=DURABLE | {invalid})


@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", None),
        ("kind", "preference"),
        ("kind", 1),
        ("archived", None),
        ("archived", "false"),
        ("archived", 0),
        ("scope", None),
        ("scope", "global"),
        ("scope", {}),
        ("statement", None),
        ("statement", 12),
        ("author", []),
        ("id", ""),
        ("channel", "C0EXAMPLE1"),
    ],
)
def test_malformed_present_fact_fields_fail_closed(field, value):
    with pytest.raises(EvidenceError):
        _parse(facts=[_fact(**{field: value})])


@pytest.mark.parametrize(
    "memory",
    [
        None,
        [],
        "not-json",
        {"agent": "acme-bot", "entries": []},
        {"agent": "another-agent", "entries": [], "facts": []},
        {"agent": "acme-bot", "entries": [], "facts": None},
        {"agent": "acme-bot", "entries": [], "facts": {}},
        {"agent": "acme-bot", "entries": [], "facts": [42]},
    ],
)
def test_required_memory_shape_never_becomes_empty_success(memory):
    with pytest.raises(EvidenceError):
        parse_cli_snapshot(
            agent="acme-bot",
            skills=[],
            memory=memory,
            schedules={"agent_schedules": []},
            available=DURABLE,
        )


@pytest.mark.parametrize(
    "schedules",
    [
        None,
        [],
        {"agent_schedules": None},
        {"agent_schedules": {}},
        {"agent_schedules": [42]},
        {"agent_schedules": [_schedule(created_by=[])]},
    ],
)
def test_malformed_schedule_collection_is_error(schedules):
    with pytest.raises(EvidenceError):
        parse_cli_snapshot(
            agent="acme-bot",
            skills=[],
            memory={"agent": "acme-bot", "entries": [], "facts": []},
            schedules=schedules,
            available=DURABLE,
        )


@pytest.mark.parametrize("collection", ["facts", "skills", "schedules"])
def test_duplicate_immutable_identities_fail_closed(collection):
    row = {"facts": _fact, "skills": _skill, "schedules": _schedule}[collection]()
    with pytest.raises(EvidenceError):
        _parse(**{collection: [row, deepcopy(row)]})


def test_row_missing_predicate_field_is_preserved_but_unadmitted_scoring_errors():
    row = _fact()
    del row["author"]
    snapshot = _parse(facts=[row])
    assert "author" not in snapshot.facts[0]
    with pytest.raises(EvidenceError):
        _score(
            _snapshot(),
            snapshot,
            _predicate(
                "author_equals", LearningClass.MEMORY_FACT, "fact-example", value="U0EXAMPLE1"
            ),
        )
    assert (
        _score(
            _snapshot(),
            _parse(facts=[_fact()]),
            _predicate(
                "author_equals", LearningClass.MEMORY_FACT, "fact-example", value="U0EXAMPLE1"
            ),
        ).passed
        is True
    )


def test_missing_identity_is_not_fabricated_or_silently_dropped():
    row = _fact()
    del row["id"]
    snapshot = _parse(facts=[row])
    assert len(snapshot.facts) == 1
    assert "id" not in snapshot.facts[0]
    with pytest.raises(EvidenceError):
        diff_state(_snapshot(), snapshot)


def test_timestamp_and_order_only_changes_are_not_learning():
    first = _fact()
    second = _fact(id="fact-other", statement="The sample color is blue.")
    assert (
        diff_state(
            _parse(facts=[first, second]),
            _parse(facts=[{**second, "stated_at": "2026-01-02T00:00:00Z"}, first]),
        )
        == ()
    )


def test_actual_state_differences_keep_every_store_and_immutable_identity():
    before = _parse(facts=[_fact()], skills=[_skill()])
    changed = _fact(statement="The sample reference is green-29.")
    after = _parse(
        facts=[changed, _fact(id="fact-new")],
        skills=[_skill(description="Use the revised word order.", revision=2)],
        schedules=[_schedule()],
    )
    observed = {(item.kind, item.item_id, item.operation) for item in diff_state(before, after)}
    assert observed == {
        (LearningClass.MEMORY_FACT, "fact-example", "update"),
        (LearningClass.MEMORY_FACT, "fact-new", "create"),
        (LearningClass.SKILL, "skill-example", "update"),
        (LearningClass.SCHEDULE, "schedule-example", "create"),
    }
    deleted = diff_state(after, _snapshot())
    assert {item.operation for item in deleted} == {"delete"}
    assert {item.item_id for item in deleted} == {
        "fact-example",
        "fact-new",
        "skill-example",
        "schedule-example",
    }


def test_archive_retains_typed_instruction_and_is_not_an_active_creation():
    before = _parse(facts=[_fact(kind="instruction", archived=False)])
    after = _parse(facts=[_fact(kind="instruction", archived=True)])
    mutations = diff_state(before, after)
    assert len(mutations) == 1
    assert mutations[0].kind is LearningClass.INSTRUCTION
    assert mutations[0].operation == "archive"
    assert (
        _score(
            before,
            after,
            _predicate("active_count", LearningClass.INSTRUCTION, "fact-example", value=0),
        ).passed
        is True
    )


def test_same_slug_new_skill_id_is_replacement_not_correction():
    before = _parse(skills=[_skill()])
    after = _parse(skills=[_skill(id="skill-replacement", revision=2)])
    assert {item.operation for item in diff_state(before, after)} == {"delete", "create"}
    assert (
        _score(before, after, _predicate("same_id", LearningClass.SKILL, "sample-ordering")).passed
        is False
    )


def test_same_id_correction_and_revision_are_behavioral():
    before = _parse(skills=[_skill()])
    after = _parse(skills=[_skill(description="Reorder words second, third, first.", revision=2)])
    predicates = [
        _predicate("same_id", LearningClass.SKILL, "sample-ordering"),
        _predicate("revision_increased", LearningClass.SKILL, "sample-ordering"),
        _predicate("active_count", LearningClass.SKILL, "sample-ordering", value=1),
        _predicate("updated_count", LearningClass.SKILL, "sample-ordering", value=1),
    ]
    assert _score(before, after, *predicates).passed is True
    duplicate = _parse(skills=[*after.skills, _skill(id="skill-duplicate")])
    assert _score(before, duplicate, *predicates).passed is False
    assert _score(before, before, predicates[1]).passed is False


@pytest.mark.parametrize(
    "name,field,expected,wrong",
    [
        ("statement_equals", "statement", "The sample reference is violet-73.", "Obsolete."),
        ("author_equals", "author", "U0EXAMPLE1", "U0EXAMPLE2"),
        ("scope_equals", "scope", "agent", "channel"),
        (
            "channel_equals",
            "channel",
            {"kind": "slack", "address": "C0EXAMPLE1"},
            {"kind": "slack", "address": "C0EXAMPLE2"},
        ),
    ],
)
def test_supported_fact_predicates_have_passing_and_failing_counterparts(
    name, field, expected, wrong
):
    scope = "channel" if field == "channel" else "agent"
    good = _fact(**{"scope": scope, field: expected})
    bad = {**good, field: wrong}
    if field == "scope":
        bad["channel"] = {"kind": "slack", "address": "C0EXAMPLE1"}
    check = _predicate(name, LearningClass.MEMORY_FACT, "fact-example", value=expected)
    assert _score(_snapshot(), _parse(facts=[good]), check).passed is True
    assert _score(_snapshot(), _parse(facts=[bad]), check).passed is False


def test_missing_skill_body_is_not_invented_for_an_unadmitted_content_predicate():
    after = _parse(skills=[_skill()])
    assert "body" not in after.skills[0]
    check = _predicate(
        "body_contains_steps",
        LearningClass.SKILL,
        "sample-ordering",
        steps=["Read three words", "third, first, second", "teal-42"],
    )
    with pytest.raises(EvidenceError):
        _score(_snapshot(), after, check)


def test_pinned_schedule_and_unchanged_predicates_reject_wrong_or_extra_writes():
    before = _snapshot()
    schedule = _schedule()
    check = _predicate(
        "schedule_matches",
        LearningClass.SCHEDULE,
        "schedule-example",
        value={
            "state": "active",
            "cron": None,
            "run_at": schedule["run_at"],
            "timezone": "Etc/UTC",
            "prompt": schedule["prompt"],
            "created_by": "U0EXAMPLE1",
        },
    )
    assert _score(before, _parse(schedules=[schedule]), check).passed is True
    assert (
        _score(before, _parse(schedules=[_schedule(timezone="Europe/Paris")]), check).passed
        is False
    )
    no_fact = _predicate("unchanged_other_stores", LearningClass.MEMORY_FACT, "*")
    assert _score(before, _parse(skills=[_skill()]), no_fact).passed is True
    assert _score(before, _parse(skills=[_skill()], facts=[_fact()]), no_fact).passed is False


@pytest.mark.parametrize(
    "expectation",
    [
        {},
        {"predicates": []},
        {"predicates": None},
        {"predicates": [{"name": "unknown", "class": "memory-fact", "target": "fact-example"}]},
        {
            "predicates": [
                {"name": "active_count", "class": "memory-fact", "target": "fact-example"}
            ]
        },
        {
            "predicates": [
                {
                    "name": "active_count",
                    "class": "memory-fact",
                    "target": "fact-example",
                    "value": True,
                }
            ]
        },
        {
            "predicates": [
                {
                    "name": "active_count",
                    "class": "memory-fact",
                    "target": "fact-example",
                    "value": -1,
                }
            ]
        },
        {
            "predicates": [
                {
                    "name": "author_equals",
                    "class": "memory-fact",
                    "target": "fact-example",
                    "value": None,
                }
            ]
        },
    ],
)
def test_unadmitted_or_malformed_predicates_never_manufacture_pass(expectation):
    with pytest.raises(EvidenceError):
        score_state(_snapshot(), _snapshot(), expectation)


def _messages(*, result=True, error=False):
    messages = [
        {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "call-example",
                    "name": "memory_write",
                    "input": {"statement": "The sample reference is violet-73."},
                }
            ],
        }
    ]
    if result:
        messages.append(
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-example",
                        "content": "denied" if error else "saved",
                        "is_error": error,
                    }
                ],
            }
        )
    return messages


@pytest.mark.parametrize("has_result,is_error", [(True, False), (True, True), (False, False)])
def test_tool_attempts_are_preserved_without_fabricating_writes(has_result, is_error):
    attempts = match_tool_results(_messages(result=has_result, error=is_error))
    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt.call_id == "call-example"
    assert attempt.name == "memory_write"
    assert attempt.matched is has_result
    assert attempt.is_error is (is_error if has_result else None)
    assert diff_state(_snapshot(), _snapshot()) == ()
    assert len(diff_state(_snapshot(), _parse(facts=[_fact()]))) == 1


def test_tool_names_in_prose_are_not_execution_evidence():
    assert (
        match_tool_results(
            [{"role": "assistant", "content": "I called memory_write and saved it."}]
        )
        == ()
    )


@pytest.mark.parametrize("defect", ["duplicate-call", "duplicate-result", "before-call", "orphan"])
def test_tool_pairs_require_ordered_unique_matching_ids(defect):
    messages = _messages()
    if defect == "duplicate-call":
        messages.insert(1, deepcopy(messages[0]))
    elif defect == "duplicate-result":
        messages.append(deepcopy(messages[1]))
    elif defect == "before-call":
        messages.reverse()
    else:
        messages[1]["content"][0]["tool_use_id"] = "unknown-call"
    with pytest.raises(EvidenceError):
        match_tool_results(messages)


def test_complete_inventory_has_six_correct_one_vs_rest_counts():
    summary = _summary(_inventory())
    expected_support = {
        "skill": 6,
        "memory-fact": 3,
        "instruction": 3,
        "schedule": 6,
        "clarify": 3,
        "nothing": 3,
    }
    assert set(summary["per_class"]) == set(expected_support)
    for kind, support in expected_support.items():
        row = summary["per_class"][kind]
        assert (row["tp"], row["fp"], row["fn"], row["support"]) == (support, 0, 0, support)
        assert row["precision"] == row["recall"] == 1.0
    assert summary["gates"]["precision"]["status"] == "PASS"
    assert summary["gates"]["recall"]["status"] == "PASS"
    assert summary["coverage"]["status"] == "PASS"
    for kind, support in expected_support.items():
        assert summary["confusion"][kind][kind] == support


def test_wrong_class_and_extra_classes_keep_fp_and_fn():
    rows = _inventory()
    rows[6] = replace(
        rows[6],
        predicted=frozenset({LearningClass.SKILL}),
        status=CaseStatus.FAIL,
        mutations=(_mutation(LearningClass.SKILL),),
    )
    rows[7] = replace(
        rows[7],
        predicted=frozenset({LearningClass.MEMORY_FACT, LearningClass.SCHEDULE}),
        mutations=(_mutation(), _mutation(LearningClass.SCHEDULE)),
    )
    summary = _summary(rows)
    assert summary["per_class"]["memory-fact"]["tp"] == 2
    assert summary["per_class"]["memory-fact"]["fn"] == 1
    assert summary["per_class"]["skill"]["fp"] == 1
    assert summary["per_class"]["schedule"]["fp"] == 1
    assert summary["per_class"]["schedule"]["tp"] == 6
    assert summary["confusion"]["memory-fact"]["skill"] == 1
    assert summary["confusion"]["memory-fact"]["schedule"] == 1
    assert summary["confusion"]["memory-fact"]["memory-fact"] == 2


@pytest.mark.parametrize("kind", list(LearningClass))
def test_zero_prediction_denominator_is_null_and_available_failure_stays_in_support(kind):
    rows = [
        replace(row, predicted=frozenset(), mutations=(), status=CaseStatus.ERROR)
        if row.expected is kind
        else row
        for row in _inventory()
    ]
    summary = _summary(rows)
    result = summary["per_class"][kind.value]
    assert result["tp"] == 0
    assert result["fp"] == 0
    assert result["fn"] == result["support"] > 0
    assert result["precision"] is None
    assert result["recall"] == 0.0
    assert result["errors"] == result["support"]


def test_expected_unavailable_rows_enter_no_classification_count_and_keep_raw_writes():
    rows = _inventory()
    eligible = [
        row for row in rows if row.expected not in {LearningClass.SKILL, LearningClass.INSTRUCTION}
    ]
    unavailable = [
        replace(
            row,
            status=CaseStatus.NOT_AVAILABLE,
            predicted=frozenset({LearningClass.MEMORY_FACT}),
            mutations=(_mutation(),),
        )
        for row in rows
        if row not in eligible
    ]
    summary = _summary([*eligible, *unavailable])
    fact = summary["per_class"]["memory-fact"]
    assert (fact["tp"], fact["fp"], fact["fn"], fact["support"]) == (3, 0, 0, 3)
    skill = summary["per_class"]["skill"]
    assert (skill["tp"], skill["fp"], skill["fn"], skill["support"]) == (0, 0, 0, 0)
    assert skill["precision"] is skill["recall"] is None
    assert skill["unavailable"] == 6
    assert summary["coverage"]["status"] == "NOT_AVAILABLE"
    assert sum(summary["confusion"]["skill"].values()) == 0
    assert summary["confusion"]["memory-fact"]["memory-fact"] == 3
    assert summary["unavailable_off_target_writes"]["skill"]["memory-fact"] == 6
    assert summary["unavailable_off_target_writes"]["instruction"]["memory-fact"] == 3
    for gate in ("precision", "recall"):
        assert summary["gates"][gate] == {"status": "NOT_AVAILABLE", "value": None}
    assert summary["per_class"]["nothing"]["tp"] == 3
    assert summary["per_class"]["clarify"]["tp"] == 3
    assert summary["distractor"]["evaluated"] == 3


@pytest.mark.parametrize("invented", [LearningClass.CLARIFY, LearningClass.NOTHING])
def test_unavailable_no_write_never_acquires_semantic_no_action_prediction(invented):
    rows = _inventory()
    rows[0] = replace(
        rows[0], status=CaseStatus.NOT_AVAILABLE, predicted=frozenset({invented}), mutations=()
    )
    with pytest.raises(EvidenceError):
        _summary(rows)


def test_unclear_semantics_do_not_erase_an_observed_unexpected_write():
    rows = _inventory()
    rows[15] = replace(
        rows[15],
        predicted=frozenset({LearningClass.MEMORY_FACT}),
        mutations=(_mutation(),),
        status=CaseStatus.FAIL,
    )
    summary = _summary(rows)
    assert summary["per_class"]["clarify"]["fn"] == 1
    assert summary["per_class"]["memory-fact"]["fp"] == 1


def _balanced_rows(*, tp, fp, fn):
    rows = _inventory()
    rows = [
        replace(row, predicted=frozenset(), mutations=(), status=CaseStatus.FAIL)
        if row.expected is LearningClass.MEMORY_FACT
        else row
        for row in rows
    ]
    for index in range(tp):
        rows.append(_result(decision_id=f"fact-hit-{index}"))
    for index in range(fn - 3):
        rows.append(
            _result(
                predicted=(), mutations=(), status=CaseStatus.FAIL, decision_id=f"fact-miss-{index}"
            )
        )
    for index in range(fp):
        rows.append(
            _result(
                "F1-P1",
                decision_id=f"extra-fact-{index}",
                predicted={LearningClass.SKILL, LearningClass.MEMORY_FACT},
            )
        )
    return rows


@pytest.mark.parametrize(
    "tp,fp,fn,precision_status,recall_status",
    [
        (36, 4, 9, "PASS", "PASS"),
        (35, 4, 9, "FAIL", "FAIL"),
        (36, 5, 9, "FAIL", "PASS"),
        (36, 4, 10, "PASS", "FAIL"),
    ],
)
def test_precision_point_nine_and_recall_point_eight_boundaries(
    tp, fp, fn, precision_status, recall_status
):
    summary = _summary(_balanced_rows(tp=tp, fp=fp, fn=fn))
    fact = summary["per_class"]["memory-fact"]
    assert (fact["tp"], fact["fp"], fact["fn"]) == (tp, fp, fn)
    assert fact["precision"] == pytest.approx(tp / (tp + fp))
    assert fact["recall"] == pytest.approx(tp / (tp + fn))
    assert summary["gates"]["precision"]["status"] == precision_status
    assert summary["gates"]["recall"]["status"] == recall_status


def test_custom_thresholds_change_boundary_without_changing_raw_counts():
    rows = _balanced_rows(tp=36, fp=5, fn=10)
    default = _summary(rows)
    custom = _summary(rows, thresholds=Thresholds(precision=36 / 41, recall=36 / 46))
    assert default["per_class"] == custom["per_class"]
    assert custom["gates"]["precision"]["status"] == "PASS"
    assert custom["gates"]["recall"]["status"] == "PASS"


def test_distractor_attempts_and_actual_false_writes_have_different_denominators():
    rows = _inventory()
    attempt = ToolEvidence("call-example", "memory_write", {}, "denied", True, True)
    rows[18] = replace(rows[18], attempts=(attempt,), status=CaseStatus.FAIL)
    summary = _summary(rows)
    assert summary["distractor"]["falsewrite_rate"] == 0.0
    assert summary["distractor"]["attempted_write_rate"] == pytest.approx(1 / 3)
    assert summary["gates"]["falsewrite"]["status"] == "PASS"
    rows[18] = replace(
        rows[18],
        predicted=frozenset({LearningClass.MEMORY_FACT}),
        mutations=(_mutation(),),
        attempts=(),
    )
    summary = _summary(rows)
    assert summary["distractor"]["falsewrite_count"] == 1
    assert summary["distractor"]["falsewrite_rate"] == pytest.approx(1 / 3)
    assert summary["gates"]["falsewrite"]["status"] == "FAIL"


def test_falsewrite_point_zero_five_is_inclusive_and_counts_decisions_once():
    rows = _inventory()
    rows.extend(_result("F7-P1", decision_id=f"distractor-{index}") for index in range(17))
    rows[-1] = replace(
        rows[-1],
        predicted=frozenset({LearningClass.MEMORY_FACT, LearningClass.SKILL}),
        mutations=(_mutation(), _mutation(LearningClass.SKILL)),
        status=CaseStatus.FAIL,
    )
    summary = _summary(rows)
    assert summary["distractor"]["evaluated"] == 20
    assert summary["distractor"]["falsewrite_count"] == 1
    assert summary["distractor"]["falsewrite_rate"] == 0.05
    assert summary["gates"]["falsewrite"]["status"] == "PASS"
    assert (
        _summary(rows[1:] + [rows[0]], thresholds=Thresholds(falsewrite=0.049))["gates"][
            "falsewrite"
        ]["status"]
        == "FAIL"
    )


def test_duplicate_correction_gate_defaults_to_zero_and_can_be_configured():
    rows = _inventory()
    assert _summary(rows)["gates"]["correction_duplicates"]["status"] == "PASS"
    rows[-1] = replace(rows[-1], correction_duplicates=1, status=CaseStatus.FAIL)
    assert _summary(rows)["correction_duplicates"] == 1
    assert _summary(rows)["gates"]["correction_duplicates"]["status"] == "FAIL"
    assert (
        _summary(rows, thresholds=Thresholds(correction_duplicates=1))["gates"][
            "correction_duplicates"
        ]["status"]
        == "PASS"
    )


@pytest.mark.parametrize(
    "defect",
    [
        "missing-case",
        "duplicate-decision",
        "wrong-family",
        "missing-repetition",
        "reused-instance",
        "changed-expected",
    ],
)
def test_expected_inventory_and_n_repetitions_are_mandatory(defect):
    rows = _inventory(2)
    if defect == "missing-case":
        rows.pop()
    elif defect == "duplicate-decision":
        rows.append(rows[0])
    elif defect == "wrong-family":
        rows[-1] = replace(rows[-1], case_id="F9-P3-R2")
    elif defect == "missing-repetition":
        rows = rows[:24]
    elif defect == "reused-instance":
        rows[-1] = replace(rows[-1], case_id="F8-P3-R1")
    else:
        rows[-1] = replace(rows[-1], expected=LearningClass.NOTHING)
    with pytest.raises(EvidenceError):
        _summary(rows, repetitions=2)


def test_repetition_metrics_cannot_pool_away_a_failure():
    rows = _inventory(2)
    rows[18] = replace(
        rows[18],
        predicted=frozenset({LearningClass.MEMORY_FACT}),
        mutations=(_mutation(),),
        status=CaseStatus.FAIL,
    )
    summary = _summary(rows, repetitions=2, thresholds=Thresholds(falsewrite=0.2))
    assert summary["distractor"]["falsewrite_rate"] == pytest.approx(1 / 6)
    assert summary["repetitions"][0]["gates"]["falsewrite"]["status"] == "FAIL"
    assert summary["repetitions"][1]["gates"]["falsewrite"]["status"] == "PASS"
    assert summary["gates"]["falsewrite"]["status"] == "FAIL"


@pytest.mark.parametrize("repetitions", [0, -1, True])
def test_invalid_repetition_count_is_rejected(repetitions):
    with pytest.raises(EvidenceError):
        _summary(_inventory(), repetitions=repetitions)


def test_committed_inventory_has_every_primary_expected_class_before_live_scoring():
    corpus = json.loads((FIXTURES / "learning-cases.json").read_text())
    assert corpus["version"] == 1
    assert [family["id"] for family in corpus["families"]] == [f"F{n}" for n in range(1, 9)]
    cases = [case for family in corpus["families"] for case in family["scenarios"]]
    assert tuple(case["id"] for case in cases) == CASE_IDS
    for case in cases:
        assert len(case["messages"]) >= 1
        primary = case.get("primary_message", 0)
        decisions = [row for row in case["decisions"] if row["message"] == primary]
        assert len(decisions) == 1
        assert decisions[0]["expected"] == FAMILY_CLASS[int(case["id"][1])].value
        assert decisions[0]["predicates"]
    for case in cases[15:21]:
        assert any(row["class"] == "memory-fact" for row in case["decisions"][0]["predicates"])


def test_empty_available_stores_are_valid_observations_without_inferred_learning():
    snapshot = _parse()
    assert snapshot == _snapshot()
    assert diff_state(snapshot, snapshot) == ()
    checks = [_predicate("unchanged_other_stores", kind, "*") for kind in DURABLE]
    assert _score(snapshot, snapshot, *checks).passed is True


def test_pinned_recurring_schedule_accepts_null_run_at_without_aliases():
    row = _schedule(cron="40 9 * * 1-5", run_at=None)
    snapshot = _parse(schedules=[row])
    assert snapshot.schedules == (row,)
    check = _predicate(
        "schedule_matches",
        LearningClass.SCHEDULE,
        row["id"],
        value={
            "cron": "40 9 * * 1-5",
            "run_at": None,
            "state": "active",
            "timezone": "Etc/UTC",
            "prompt": row["prompt"],
            "created_by": "U0EXAMPLE1",
        },
    )
    assert _score(_snapshot(), snapshot, check).passed is True
    assert _score(_snapshot(), _parse(schedules=[_schedule()]), check).passed is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", True),
        ("revision", "1"),
        ("state", None),
        ("state", True),
        ("cron", 10),
        ("run_at", 10),
        ("timezone", []),
        ("prompt", None),
        ("created_by", {}),
    ],
)
def test_malformed_present_pinned_schedule_fields_fail_closed(field, value):
    with pytest.raises(EvidenceError):
        _parse(schedules=[_schedule(**{field: value})])


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", True),
        ("revision", "1"),
        ("description", None),
        ("updated_at", []),
        ("slug", []),
        ("author", None),
    ],
)
def test_malformed_present_pinned_skill_fields_fail_closed(field, value):
    with pytest.raises(EvidenceError):
        _parse(skills=[_skill(**{field: value})])


def test_missing_schedule_channel_is_not_invented_for_an_unadmitted_predicate():
    snapshot = _parse(schedules=[_schedule()])
    assert "channel" not in snapshot.schedules[0]
    check = _predicate(
        "channel_equals", LearningClass.SCHEDULE, "schedule-example", value="C0EXAMPLE1"
    )
    with pytest.raises(EvidenceError):
        _score(_snapshot(), snapshot, check)


def test_delete_and_create_count_predicates_use_persisted_identity_changes():
    before = _parse(facts=[_fact()])
    after = _parse(facts=[_fact(id="fact-replacement")])
    checks = [
        _predicate("deleted_count", LearningClass.MEMORY_FACT, "fact-example", value=1),
        _predicate("created_count", LearningClass.MEMORY_FACT, "fact-replacement", value=1),
    ]
    assert _score(before, after, *checks).passed is True
    assert _score(before, before, *checks).passed is False


def test_same_id_fact_correction_keeps_one_current_statement_and_no_obsolete_identity():
    before = _parse(facts=[_fact()])
    corrected = _fact(statement="The sample reference is green-29.")
    after = _parse(facts=[corrected])
    checks = [
        _predicate("same_id", LearningClass.MEMORY_FACT, "fact-example"),
        _predicate("updated_count", LearningClass.MEMORY_FACT, "fact-example", value=1),
        _predicate(
            "statement_equals",
            LearningClass.MEMORY_FACT,
            "fact-example",
            value=corrected["statement"],
        ),
    ]
    assert _score(before, after, *checks).passed is True
    duplicate = _parse(facts=[_fact(), {**corrected, "id": "fact-duplicate"}])
    assert _score(before, duplicate, *checks).passed is False


def test_unavailable_raw_write_counts_mutations_and_excludes_duplicate_metric():
    rows = _inventory()
    rows[0] = replace(
        rows[0],
        status=CaseStatus.NOT_AVAILABLE,
        predicted=frozenset({LearningClass.MEMORY_FACT}),
        mutations=(_mutation(item_id="fact-one"), _mutation(item_id="fact-two")),
        correction_duplicates=7,
    )
    summary = _summary(rows)
    assert summary["unavailable_off_target_writes"]["skill"]["memory-fact"] == 2
    assert summary["per_class"]["memory-fact"]["fp"] == 0
    assert summary["correction_duplicates"] == 0


def test_whole_row_unavailable_with_zero_writes_never_infers_nothing():
    rows = _inventory()
    rows[0] = replace(rows[0], status=CaseStatus.NOT_AVAILABLE, predicted=frozenset(), mutations=())
    summary = _summary(rows)
    assert summary["per_class"]["nothing"]["tp"] == 3
    assert summary["per_class"]["nothing"]["fp"] == 0
    assert summary["per_class"]["skill"]["support"] == 5
    assert summary["per_class"]["skill"]["fn"] == 0
    assert summary["per_class"]["skill"]["unavailable"] == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("precision", -0.1),
        ("precision", 1.1),
        ("precision", float("nan")),
        ("recall", float("inf")),
        ("falsewrite", -0.1),
        ("heldout_control_improvement", 1.1),
        ("correction_duplicates", -1),
        ("scheduled_deliveries", 0),
    ],
)
def test_invalid_thresholds_cannot_reach_metric_acceptance(field, value):
    with pytest.raises(EvidenceError):
        _summary(_inventory(), thresholds=replace(Thresholds(), **{field: value}))


@pytest.mark.parametrize("value", [-1, True, 1.5])
def test_invalid_correction_duplicate_counts_are_not_silently_coerced(value):
    rows = _inventory()
    rows[-1] = replace(rows[-1], correction_duplicates=value)
    with pytest.raises(EvidenceError):
        _summary(rows)


def test_summary_does_not_invent_auxiliary_gates_from_primary_decisions():
    summary = _summary(_inventory())
    assert set(summary["gates"]) == {"precision", "recall", "falsewrite", "correction_duplicates"}


def test_unsupported_prediction_cannot_create_a_seventh_confusion_class():
    rows = _inventory()
    rows[0] = replace(rows[0], predicted=frozenset({"unrecognized"}))
    with pytest.raises(EvidenceError):
        _summary(rows)


def test_expected_case_inventory_itself_rejects_duplicates():
    with pytest.raises(EvidenceError):
        summarize(_inventory(), expected_case_ids=(*CASE_IDS, CASE_IDS[0]), repetitions=1)


def test_append_log_entries_cannot_substitute_for_canonical_facts():
    snapshot = parse_cli_snapshot(
        agent="acme-bot",
        skills=[],
        memory={
            "agent": "acme-bot",
            "entries": [{"index": 0, "content": "Always close replies with violet-73."}],
            "facts": [],
        },
        schedules={"agent_schedules": []},
        available=DURABLE,
    )
    assert snapshot.facts == ()
    assert diff_state(_snapshot(), snapshot) == ()
    assert snapshot.available == DURABLE


# The three row shapes below were recorded from a live learning-eval run against the pinned CLI
# and anonymized. They differ from the fixture text only in Markdown marks, a trailing period,
# and extra prompt wording around the token.
REAL_SKILL_BODY = (
    "1. Read the three input words in their given order.\n"
    "2. Place the third word first, the first word second, and the second word last.\n"
    "3. Return `rm-db6ac1` followed by those three words, separated by colons."
)
REAL_STEPS = [
    "Read the three input words in their given order.",
    "Place the third word first, the first word second, and the second word last.",
    "Return rm-db6ac1 followed by those three words, separated by colons.",
]
REAL_FACT = "cabinet-7bb2f8 is violet-153e9e."
REAL_PROMPT = (
    'Post the exact text "tok-7be4bc" as a message in the Slack channel where this '
    "schedule was created. Post only that text, with no additional commentary."
)


def _real_schedule(**changes):
    row = {
        "prompt": REAL_PROMPT,
        "cron": "40 9 * * 1-5",
        "run_at": None,
        "created_by": "U-curie-message",
        "destination": {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default"},
    }
    return _schedule(**{**row, **changes})


def _real_schedule_check(**changes):
    value = {
        "state": "active",
        "cron": "40 9 * * 1-5",
        "run_at": None,
        "timezone": "Etc/UTC",
        "prompt": "tok-7be4bc",
        "created_by": "U-curie-message",
        **changes,
    }
    return _predicate("schedule_matches", LearningClass.SCHEDULE, "schedule-example", value=value)


def _steps_check(steps=REAL_STEPS):
    return _predicate("body_contains_steps", LearningClass.SKILL, "sample-ordering", steps=steps)


def _body_score(body, steps=REAL_STEPS):
    after = _parse(skills=[_skill(body=body)])
    return _score(_snapshot(), after, _steps_check(steps)).passed


def test_real_skill_body_with_markdown_marks_and_list_markers_passes_steps():
    assert _body_score(REAL_SKILL_BODY) is True


def test_real_fact_statement_with_trailing_period_passes_statement_equals():
    check = _predicate(
        "statement_equals",
        LearningClass.MEMORY_FACT,
        "fact-example",
        value="cabinet-7bb2f8 is violet-153e9e",
    )
    after = _parse(facts=[_fact(statement=REAL_FACT)])
    assert _score(_snapshot(), after, check).passed is True


def test_real_schedule_row_passes_when_prompt_contains_the_token():
    after = _parse(schedules=[_real_schedule()])
    assert _score(_snapshot(), after, _real_schedule_check()).passed is True


def test_skill_body_missing_a_step_fails():
    body = "\n".join(REAL_SKILL_BODY.splitlines()[:2])
    assert _body_score(body) is False


def test_skill_body_reordering_steps_without_nonces_is_left_to_the_judge():
    # Steps one and two carry no nonce, so their order imposes no deterministic constraint.
    first, second, third = REAL_SKILL_BODY.splitlines()
    assert _body_score("\n".join([second, first, third])) is True


def test_skill_body_with_wrong_marker_fails():
    assert _body_score(REAL_SKILL_BODY.replace("rm-db6ac1", "rm-000000")) is False


def test_fact_statement_with_wrong_value_fails():
    check = _predicate(
        "statement_equals",
        LearningClass.MEMORY_FACT,
        "fact-example",
        value="cabinet-7bb2f8 is violet-153e9e",
    )
    after = _parse(facts=[_fact(statement="cabinet-7bb2f8 is violet-000000.")])
    assert _score(_snapshot(), after, check).passed is False


def test_schedule_prompt_lacking_the_token_fails():
    prompt = "Post the exact text as a message in the Slack channel where this was created."
    after = _parse(schedules=[_real_schedule(prompt=prompt)])
    assert _score(_snapshot(), after, _real_schedule_check()).passed is False


def test_schedule_with_wrong_cron_fails():
    after = _parse(schedules=[_real_schedule(cron="0 9 * * 1-5")])
    assert _score(_snapshot(), after, _real_schedule_check()).passed is False


def test_summary_marks_correction_duplicates_not_available_without_an_evaluated_correction():
    rows = [
        _result(base, status=CaseStatus.NOT_AVAILABLE, predicted=())
        if base[1] == "8"
        else _result(base)
        for base in CASE_IDS
    ]
    gate = _summary(rows)["gates"]["correction_duplicates"]
    assert gate == {"status": "NOT_AVAILABLE", "value": None}


def test_summary_keeps_numeric_correction_duplicates_with_one_evaluated_correction():
    rows = [
        _result(base, status=CaseStatus.NOT_AVAILABLE, predicted=())
        if base[1] == "8" and base != "F8-P1"
        else _result(base, **({"decision_id": "correct"} if base[1] == "8" else {}))
        for base in CASE_IDS
    ]
    gate = _summary(rows)["gates"]["correction_duplicates"]
    assert gate["status"] == "PASS"
    assert gate["value"] == 0


# The rows below were recorded from a live learning-eval run (run 7) and anonymized. They
# preserve the fixture meaning while rewording it, so verbatim containment cannot grade them.
F1_P2_LINES = [
    "1. Read the unit tag and integer quantity from the input.",
    "2. Choose the route by unit tag: for kg choose route-1ecd41; for cm choose route-d0d301. "
    "Leave the quantity unchanged.",
    "3. Return the selected route, a slash, the quantity, a slash, and rt-c5cab1 "
    "(format: <route>/<quantity>/rt-c5cab1).",
]
F1_P2_STEPS = [
    "Read the unit tag and integer quantity.",
    "For kg choose route-1ecd41; for cm choose route-d0d301; leave the quantity unchanged.",
    "Return the selected route, a slash, the quantity, a slash, and rt-c5cab1.",
]
F8_P2_STEPS = ["second word", "third word", "first word"]
F8_P2_BODY = "Place the second word before the third, then the first word last."
F3_P3_STATEMENT = "The reference value for cabinet-573d74 is violet-743263."


def _f3_score(statement):
    check = _predicate(
        "statement_equals",
        LearningClass.MEMORY_FACT,
        "fact-example",
        value="cabinet-573d74 is violet-743263",
    )
    after = _parse(facts=[_fact(statement=statement)])
    return _score(_snapshot(), after, check).passed


def test_real_f1_p2_reworded_body_passes_token_anchored_steps():
    assert _body_score("\n".join(F1_P2_LINES), F1_P2_STEPS) is True


def test_real_f8_p2_reworded_correction_body_passes_token_anchored_steps():
    assert _body_score(F8_P2_BODY, F8_P2_STEPS) is True


def test_real_f3_p3_reworded_statement_passes_statement_equals():
    assert _f3_score(F3_P3_STATEMENT) is True


def test_f1_p2_body_with_a_wrong_route_marker_fails():
    body = "\n".join(F1_P2_LINES).replace("route-1ecd41", "route-999999")
    assert _body_score(body, F1_P2_STEPS) is False


def test_f1_p2_body_missing_plain_wording_but_keeping_nonces_is_left_to_the_judge():
    # Plain (non-nonce) wording is the judge's call, not a token-coverage threshold.
    body = "\n".join(F1_P2_LINES).replace(" Leave the quantity unchanged.", "")
    assert _body_score(body, F1_P2_STEPS) is True


def test_f1_p2_body_with_nonce_bearing_steps_reversed_fails():
    first, second, third = F1_P2_LINES
    assert _body_score("\n".join([first, third, second]), F1_P2_STEPS) is False


def test_f8_p2_body_without_nonces_imposes_no_deterministic_step_constraint():
    # F8-P2 steps carry no nonce, so no unique-token anchor can fail the body.
    body = "Place the second word before the first word last."
    assert _body_score(body, F8_P2_STEPS) is True


def test_f3_p3_statement_with_a_different_value_fails():
    assert _f3_score("The reference value for cabinet-573d74 is violet-000000.") is False


def test_f3_p3_statement_holding_both_obsolete_and_new_values_fails():
    statement = "The reference value for cabinet-573d74 is violet-743263, not violet-111111."
    assert _f3_score(statement) is False


def _f8_rows(*decisions):
    rows = []
    for base in CASE_IDS:
        if base[1] != "8":
            rows.append(_result(base))
            continue
        decision_id, duplicates, status = decisions[int(base[-1]) - 1]
        rows.append(
            _result(
                base,
                decision_id=decision_id,
                correction_duplicates=duplicates,
                status=status,
                predicted=() if status is CaseStatus.NOT_AVAILABLE else None,
            )
        )
    return rows


def test_correction_gate_counts_only_f8_correct_decisions_not_seed_rows():
    rows = _f8_rows(
        ("seed", 5, CaseStatus.PASS),
        ("correct", 1, CaseStatus.PASS),
        ("correct", 0, CaseStatus.PASS),
    )
    gate = _summary(rows)["gates"]["correction_duplicates"]
    assert gate == {"status": "FAIL", "value": 1}


def test_correction_gate_is_not_available_when_only_seed_rows_were_evaluated():
    rows = _f8_rows(
        ("seed", 2, CaseStatus.PASS),
        ("correct", 0, CaseStatus.NOT_AVAILABLE),
        ("correct", 0, CaseStatus.NOT_AVAILABLE),
    )
    gate = _summary(rows)["gates"]["correction_duplicates"]
    assert gate == {"status": "NOT_AVAILABLE", "value": None}


# Paraphrase tolerance (review R1-2): nonces are the only deterministic anchor ------------

NONCE_STEPS = [
    "Look up route-1ecd41 for every kg load.",
    "Send each cm load through route-d0d301 instead.",
    "Finish the reply with rt-c5cab1.",
]
NONCE_PARAPHRASE = (
    "Kilogram shipments use route-1ecd41. Centimetre shipments go via route-d0d301. "
    "Append rt-c5cab1 at the very end."
)


def test_contains_in_order_accepts_a_paraphrase_that_keeps_every_nonce_in_order():
    from curie_worker.eval.learning_metrics import _contains_in_order

    assert _contains_in_order(NONCE_PARAPHRASE, NONCE_STEPS) is True
    assert _body_score(NONCE_PARAPHRASE, NONCE_STEPS) is True


def test_contains_in_order_rejects_a_body_missing_one_nonce():
    from curie_worker.eval.learning_metrics import _contains_in_order

    body = NONCE_PARAPHRASE.replace(" Centimetre shipments go via route-d0d301.", "")
    assert _contains_in_order(body, NONCE_STEPS) is False


def test_contains_in_order_rejects_nonces_out_of_step_order():
    from curie_worker.eval.learning_metrics import _contains_in_order

    body = (
        "Centimetre shipments go via route-d0d301. Kilogram shipments use route-1ecd41. "
        "Append rt-c5cab1 at the very end."
    )
    assert _contains_in_order(body, NONCE_STEPS) is False


def test_contains_in_order_step_without_a_nonce_imposes_no_constraint():
    from curie_worker.eval.learning_metrics import _contains_in_order

    steps = ["Read the three input words in their given order.", "Return rm-db6ac1 first."]
    assert _contains_in_order("Lead the output with rm-db6ac1.", steps) is True
    assert _contains_in_order("Something unrelated entirely.", ["Read the words."]) is True


def test_contains_in_order_still_requires_a_text_body():
    from curie_worker.eval.learning_metrics import _contains_in_order

    with pytest.raises(EvidenceError):
        _contains_in_order(None, NONCE_STEPS)


@pytest.mark.parametrize(
    "statement",
    [
        "The value of cabinet-abc123 is violet-def456",
        "violet-def456 is what cabinet-abc123 holds now.",
        "For cabinet-abc123, use violet-def456.",
    ],
)
def test_statement_agrees_with_any_wording_that_holds_every_expected_nonce(statement):
    from curie_worker.eval.learning_metrics import _statement_agrees

    assert _statement_agrees(statement, "cabinet-abc123 is violet-def456") is True


@pytest.mark.parametrize(
    "statement",
    [
        "cabinet-abc123 is violet-def456, formerly amber-123abc.",
        "cabinet-abc123 is amber-123abc",
        "cabinet-abc123 is set",
        "cabinet-abc1234 is violet-def456",
    ],
    ids=["competing-nonce", "wrong-nonce", "missing-nonce", "nonce-not-a-word"],
)
def test_statement_disagrees_on_a_missing_or_competing_nonce(statement):
    from curie_worker.eval.learning_metrics import _statement_agrees

    assert _statement_agrees(statement, "cabinet-abc123 is violet-def456") is False


@pytest.mark.parametrize(
    "statement,agrees",
    [
        ("Note: the deploy window is Friday.", True),
        ("The **deploy window** is friday", True),
        ("the deploy window is fridays", False),
        ("deploy window friday", False),
    ],
)
def test_statement_without_expected_nonces_keeps_word_bounded_containment(statement, agrees):
    from curie_worker.eval.learning_metrics import _statement_agrees

    assert _statement_agrees(statement, "the deploy window is friday") is agrees
