"""One candidate collection pass that dies after object deletion and before finalize.

Run by ``test_candidate_collector_killed_before_finalize`` as a separate process.
The parent passes the served API, the S3 endpoint, the owned bucket and the
collector identity through the environment. The process claims through the real
candidate ledger, deletes through the real candidate store, and is killed with
``os._exit`` at the first finalize call, so the ledger is left holding a live
claim for an object that is already gone.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import httpx
from curie_worker.publication_candidate_storage import (
    CandidateObjectStore,
    build_candidate_collector,
)
from curie_worker.publication_candidates import PublicationCandidateClient
from curie_worker.workspace_recovery_storage import build_recovery_s3_client

EXIT_BEFORE_FINALIZE = 17
EXIT_NOTHING_FINALIZED = 3


class _DiesBeforeFinalize(PublicationCandidateClient):
    async def finalize_collection(self, payload: dict[str, Any]) -> dict[str, Any]:
        # Injected fault: the worker process is killed after it deleted the
        # claimed object and before the ledger heard about it. os._exit skips
        # every finally block, like SIGKILL or an OOM kill would.
        sys.stdout.flush()
        os._exit(EXIT_BEFORE_FINALIZE)


async def _main() -> int:
    env = os.environ
    store = CandidateObjectStore(
        client=build_recovery_s3_client(
            endpoint_url=env["CANDIDATE_S3_ENDPOINT"],
            access_key=env["CANDIDATE_S3_ACCESS_KEY"],
            secret_key=env["CANDIDATE_S3_SECRET_KEY"],
            region="us-east-1",
        ),
        bucket=env["CANDIDATE_BUCKET"],
    )
    async with httpx.AsyncClient(timeout=30.0) as http:
        ledger = _DiesBeforeFinalize(
            api_base_url=env["CANDIDATE_API_URL"],
            worker_token=env["CANDIDATE_WORKER_TOKEN"],
            client=http,
        )
        collector = build_candidate_collector(
            ledger=ledger, store=store, collector_id=env["CANDIDATE_COLLECTOR_ID"]
        )
        await collector.collect_once()
    return EXIT_NOTHING_FINALIZED


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
