"""The Slack mention receipt contract, judged against the frozen vector (#5414, #5380).

``tests/vectors/slack-mention-receipt.json`` holds a version 1 receipt in the
exact bytes the dispatcher stored before version 2 existed, a version 2
(declared ADR 0202 test driver) receipt, and two ``kickoff_request_id`` cases.
The dispatcher's producer test delivers the same envelopes through Socket Mode
and compares the bytes it stores; this file holds the model and the reader to
them. The reader test uses the real Valkey from compose.dev.yaml.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_internal.slack_mention_receipt import (
    MalformedMentionReceiptError,
    SlackMentionReceipt,
    kickoff_request_id,
    read_receipt,
    receipt_key,
    write_receipt,
)
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW, connect_or_skip
from pydantic import ValidationError
from redis.asyncio import Redis as AsyncRedis

_VECTOR = Path(__file__).parents[3] / "tests" / "vectors" / "slack-mention-receipt.json"
_FROZEN: dict[str, Any] = json.loads(_VECTOR.read_text(encoding="utf-8"))
_RECEIPTS: dict[str, dict[str, Any]] = {entry["name"]: entry for entry in _FROZEN["receipts"]}
_RECEIPT_KEYS = {"name", "slack_identity", "declared_driver", "envelope", "serialized"}


def _fields(name: str) -> dict[str, Any]:
    fields: dict[str, Any] = json.loads(_RECEIPTS[name]["serialized"])
    return fields


@pytest.fixture
def valkey() -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.close()


def test_the_vector_has_exactly_the_shape_its_readers_parse() -> None:
    assert set(_FROZEN) == {"comment", "receipts", "kickoff_request_ids"}
    assert set(_RECEIPTS) == {"person_thread_reply", "declared_driver_root"}
    for entry in _FROZEN["receipts"]:
        assert set(entry) == _RECEIPT_KEYS
    for case in _FROZEN["kickoff_request_ids"]:
        assert set(case) == {"channel_identity", "delivery_id", "request_id"}


@pytest.mark.parametrize("name", sorted(_RECEIPTS))
def test_every_frozen_receipt_round_trips_to_its_exact_bytes(name: str) -> None:
    serialized = _RECEIPTS[name]["serialized"]

    parsed = SlackMentionReceipt.model_validate_json(serialized)

    assert parsed.model_dump_json() == serialized
    assert SlackMentionReceipt.model_validate(json.loads(serialized)) == parsed
    # The worker hands the API ``model_dump(mode="json")``; it must parse back.
    assert SlackMentionReceipt.model_validate(parsed.model_dump(mode="json")) == parsed


def test_a_version_1_receipt_never_serializes_a_driver_field() -> None:
    """Released workers and APIs forbid unknown keys, so version 1 must not grow one."""

    receipt = SlackMentionReceipt.model_validate(_fields("person_thread_reply"))

    assert receipt.version == 1
    assert receipt.driver_bot_id is None
    assert "driver_bot_id" not in json.loads(receipt.model_dump_json())
    assert "driver_bot_id" not in receipt.model_dump(mode="json")
    assert "driver_bot_id" not in receipt.model_dump()


def test_a_version_2_receipt_names_the_declared_driver() -> None:
    entry = _RECEIPTS["declared_driver_root"]
    driver = entry["declared_driver"]

    receipt = SlackMentionReceipt.model_validate_json(entry["serialized"])

    assert receipt.version == 2
    assert receipt.driver_bot_id == driver["bot_id"]
    assert receipt.user_id == driver["bot_user_id"]
    assert receipt.evidence["user_id"] == driver["bot_user_id"]
    assert receipt.root_ts == receipt.message_ts
    assert json.loads(receipt.model_dump_json())["driver_bot_id"] == driver["bot_id"]


@pytest.mark.parametrize(
    "changes",
    [
        {"version": 2},
        {"version": 2, "driver_bot_id": None},
        {"driver_bot_id": "B0EXAMPLE1"},
        {"version": 3},
        {"version": "2", "driver_bot_id": "B0EXAMPLE1"},
        {"version": 2, "driver_bot_id": "U0EXAMPLE1"},
        {"version": 2, "driver_bot_id": ""},
        {"version": 2, "driver_bot_id": "b0example1"},
        {"version": 2, "driver_bot_id": "B0EXAMPLE1\n"},
        {"version": 2, "driver_bot_id": 1},
    ],
    ids=[
        "v2_without_driver",
        "v2_with_null_driver",
        "v1_with_driver",
        "unknown_version",
        "string_version",
        "user_id_as_driver",
        "blank_driver",
        "lowercase_driver",
        "driver_with_newline",
        "non_string_driver",
    ],
)
def test_version_and_driver_must_agree(changes: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        SlackMentionReceipt.model_validate({**_fields("person_thread_reply"), **changes})


def test_the_agreeing_forms_of_both_versions_are_accepted() -> None:
    """Liveness for the agreement refusals above."""

    person = SlackMentionReceipt.model_validate(_fields("person_thread_reply"))
    driver = SlackMentionReceipt.model_validate(
        {**_fields("person_thread_reply"), "version": 2, "driver_bot_id": "B0EXAMPLE2"}
    )

    assert (person.version, person.driver_bot_id) == (1, None)
    assert (driver.version, driver.driver_bot_id) == (2, "B0EXAMPLE2")


@pytest.mark.parametrize(
    "case",
    _FROZEN["kickoff_request_ids"],
    ids=[case["channel_identity"] for case in _FROZEN["kickoff_request_ids"]],
)
def test_kickoff_request_id_matches_the_frozen_vector(case: dict[str, str]) -> None:
    request_id = kickoff_request_id(case["channel_identity"], case["delivery_id"])

    assert isinstance(request_id, uuid.UUID)
    assert str(request_id) == case["request_id"]


def test_kickoff_request_ids_differ_by_identity_and_delivery() -> None:
    ids = {
        kickoff_request_id("default", "Ev0EXAMPLE1"),
        kickoff_request_id("acme-factory", "Ev0EXAMPLE1"),
        kickoff_request_id("default", "Ev0EXAMPLE2"),
    }

    assert len(ids) == 3
    assert kickoff_request_id("default", "Ev0EXAMPLE1") == kickoff_request_id(
        "default", "Ev0EXAMPLE1"
    )


def test_the_reader_reads_both_versions_and_refuses_a_disagreeing_receipt(
    valkey: redis.Redis,
) -> None:
    """``read_receipt`` (the worker's reader) through real Valkey."""

    token = uuid.uuid4().hex[:10].upper()
    person = SlackMentionReceipt.model_validate(
        {**_fields("person_thread_reply"), "delivery_id": f"Ev0EXAMPLE{token}A"}
    )
    driver = SlackMentionReceipt.model_validate(
        {**_fields("declared_driver_root"), "delivery_id": f"Ev0EXAMPLE{token}B"}
    )
    forged_id = f"Ev0EXAMPLE{token}C"
    forged = {**_fields("person_thread_reply"), "delivery_id": forged_id}
    forged["driver_bot_id"] = "B0EXAMPLE1"
    keys = [receipt_key(person.delivery_id), receipt_key(driver.delivery_id)]
    keys.append(receipt_key(forged_id))

    async def read(delivery_id: str) -> SlackMentionReceipt | None:
        client = AsyncRedis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
        try:
            return await read_receipt(client, delivery_id)
        finally:
            await client.aclose()

    try:
        assert write_receipt(valkey, person, ttl_s=60)
        assert write_receipt(valkey, driver, ttl_s=60)
        valkey.set(receipt_key(forged_id), json.dumps(forged), ex=60)

        assert asyncio.run(read(person.delivery_id)) == person
        assert asyncio.run(read(driver.delivery_id)) == driver
        with pytest.raises(MalformedMentionReceiptError):
            asyncio.run(read(forged_id))
    finally:
        valkey.delete(*keys)
