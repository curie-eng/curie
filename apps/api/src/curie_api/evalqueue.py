"""Eval-job fan-out seam (K1): the API enqueues, a worker consumer runs the Job.

On a dev-branch push the git-flow deploy enqueues one eval job per new version
onto the Valkey stream ``curie:evals`` (the shared default; override with
``CURIE_EVAL_STREAM``, which the worker consumer reads too). Wire encoding
mirrors the dispatcher's ``curie:runs`` seam exactly: a single ``payload``
stream field holds the model as ``model_dump_json()``, so fields can evolve
without reshaping the stream.

This module is the PRODUCER only. The consumer reads the stream and runs
``python -m curie_worker.eval`` for the version, decoding the ``payload`` field
with the shared tolerant reader ``aci_protocol.parse_eval_job``. See
docs/eval-fanout.md for the written contract.

The payload model is ``aci_protocol.EvalJob`` (#492): one declaration shared with
the worker and guarded by the schema-compat gate, rather than the copy each lane
used to hand-mirror. The Valkey encoding stays here -- transport is a producer
detail and stays out of the model, per ``turn.py``'s principle.
"""

from datetime import UTC, datetime
from typing import Any, cast

import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, EvalJob, parse_eval_job

from .config import get_settings


def to_stream_fields(job: EvalJob) -> dict[str, str]:
    return {STREAM_PAYLOAD_FIELD: job.model_dump_json()}


def from_stream_fields(fields: dict[str, str]) -> EvalJob:
    # Reader side of the seam, so it takes the sanctioned tolerant decode: an
    # unknown field from a newer producer is ignored, not rejected.
    return parse_eval_job(fields[STREAM_PAYLOAD_FIELD])


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


class EvalQueue:
    def __init__(self, client: redis.Redis, stream: str | None = None) -> None:
        # The eval stream is declared once, on the settings object (#3565), and
        # resolved at construction so the CURIE_EVAL_STREAM override the worker
        # consumer also reads is picked up when the queue is built.
        self._client = client
        self._stream = stream if stream is not None else get_settings().eval_stream

    async def enqueue(self, request: EvalJob) -> str:
        # redis-py types the fields map as an invariant broad union; cast to
        # satisfy the stub (same pattern as the dispatcher's enqueue).
        fields = cast(dict[Any, Any], to_stream_fields(request))
        stream_id = await self._client.xadd(self._stream, fields)
        return stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)
