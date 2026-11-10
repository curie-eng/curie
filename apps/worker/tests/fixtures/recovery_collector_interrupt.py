"""One recovery collection pass that dies after object deletion and before finalize.

Run by ``test_recovery_gc_races`` as a separate process. The parent passes the
owned API, store and collector identity through the environment. The process
claims through the real ledger, deletes through the real store, and is killed
with ``os._exit`` at the first finalize call, so the ledger is left holding
live claims for objects that are already gone.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import httpx
from curie_worker.workspace_recovery import WorkspaceRecoveryClient
from curie_worker.workspace_recovery_storage import (
    RecoveryCollector,
    RecoveryObjectStore,
    build_recovery_s3_client,
)

EXIT_BEFORE_FINALIZE = 17
EXIT_NOTHING_FINALIZED = 3


class _DiesBeforeFinalize(WorkspaceRecoveryClient):
    async def finalize_collection(self, payload: dict[str, Any]) -> dict[str, Any]:
        # Injected fault: the worker process is killed after it deleted the
        # claimed objects and before the ledger heard about it. os._exit skips
        # every finally block, like SIGKILL or an OOM kill would.
        sys.stdout.flush()
        os._exit(EXIT_BEFORE_FINALIZE)


async def _main() -> int:
    env = os.environ
    store = RecoveryObjectStore(
        client=build_recovery_s3_client(
            endpoint_url=env["RECOVERY_S3_ENDPOINT"],
            access_key=env["RECOVERY_S3_ACCESS_KEY"],
            secret_key=env["RECOVERY_S3_SECRET_KEY"],
            region="us-east-1",
        ),
        bucket=env["RECOVERY_BUCKET"],
    )
    async with httpx.AsyncClient(timeout=30.0) as http:
        ledger = _DiesBeforeFinalize(
            api_base_url=env["RECOVERY_API_URL"],
            worker_token=env["RECOVERY_WORKER_TOKEN"],
            client=http,
        )
        collector = RecoveryCollector(
            ledger=ledger, store=store, collector_id=env["RECOVERY_COLLECTOR_ID"]
        )
        await collector.collect_once()
    return EXIT_NOTHING_FINALIZED


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
