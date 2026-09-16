"""The API observer reads the real worker's terminal key configuration."""

import asyncio
import json
import uuid

import pytest
import redis.asyncio as redis
from curie_api.config import Settings
from curie_worker.config import WorkerConfig


@pytest.mark.parametrize("override", [None, "KEY_PREFIX", "CURIE_KEY_PREFIX"])
def test_review_terminal_prefix_matches_actual_worker_environment(
    monkeypatch: pytest.MonkeyPatch, override: str | None
) -> None:
    monkeypatch.delenv("KEY_PREFIX", raising=False)
    monkeypatch.delenv("CURIE_KEY_PREFIX", raising=False)
    if override:
        monkeypatch.setenv(override, "test:review-terminal")
    expected = "test:review-terminal" if override == "KEY_PREFIX" else "curie:worker"
    assert WorkerConfig().key_prefix == expected
    assert Settings(_env_file=None).worker_key_prefix == expected


@pytest.mark.parametrize("decode_responses", [False, True])
def test_graveyard_observer_reads_actual_client_bytes_and_decoded_fields(
    decode_responses: bool,
) -> None:
    from curie_api.github_review_terminal import read_review_dead_letter
    from curie_test_support.valkey import (
        VALKEY_HOST,
        VALKEY_PORT,
        VALKEY_PW,
        connect_or_skip,
    )

    probe = connect_or_skip()
    probe.close()

    async def exercise() -> None:
        settings = Settings(runs_stream=f"test:review-bytes:{uuid.uuid4()}")
        stream = settings.dead_letter_stream_name()
        valkey = redis.Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None,
            decode_responses=decode_responses, socket_connect_timeout=2,
        )
        turn = {"event_id": "github-feedback-example", "conversation_id": "review-original"}
        try:
            assert await read_review_dead_letter(
                valkey, settings, stream_id="1-0", turn=turn, cursor=None,
            ) == (False, None)
            unrelated = await valkey.xadd(stream, {
                "dl_original_id": "2-0", "payload": json.dumps(turn),
            })
            unrelated_id = unrelated.decode() if isinstance(unrelated, bytes) else unrelated
            assert await read_review_dead_letter(
                valkey, settings, stream_id="1-0", turn=turn, cursor=None,
            ) == (False, unrelated_id)
            matched = await valkey.xadd(stream, {
                "dl_original_id": "1-0", "payload": json.dumps(turn),
            })
            matched_id = matched.decode() if isinstance(matched, bytes) else matched
            assert await read_review_dead_letter(
                valkey, settings, stream_id="1-0", turn=turn, cursor=unrelated_id,
            ) == (True, matched_id)
            # Removing real evidence must restore unknown, never infer completion.
            await valkey.delete(stream)
            assert await read_review_dead_letter(
                valkey, settings, stream_id="1-0", turn=turn, cursor=unrelated_id,
            ) == (False, unrelated_id)
        finally:
            await valkey.delete(stream)
            await valkey.aclose()

    asyncio.run(exercise())
