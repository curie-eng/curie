"""Suite-wide xdist scheduling for the required CI run.

CI runs `pytest -n 4 --dist loadgroup`. Every test is grouped by its own file,
which schedules exactly like `--dist loadfile`, except the files below. They
share process-external state that no fixture namespaces, so they are pinned to
one group and never overlap each other on different workers:

- fixed Valkey keys: the thread reset sets (`THREAD_RESET_SET`,
  `THREAD_RESET_INFLIGHT_SET`) are frozen shared names, and a consumer in one
  file drains or deletes another file's requests;
- the shared Langfuse project: exact aggregate comparisons read ClickHouse
  while other files ingest traces into the same project.

A serial run is unaffected: without xdist the marker is inert.
"""

from __future__ import annotations

import pytest

SHARED_LIVE_STATE_GROUP = "shared-live-state"

SHARED_LIVE_STATE_FILES = frozenset(
    {
        # Valkey thread reset sets
        "apps/worker/tests/kernel/test_consumer.py",
        "apps/worker/tests/kernel/test_otel_runtime.py",
        "apps/worker/tests/test_thread_reset_vector.py",
        "apps/api/tests/test_thread_reset_vector.py",
        "apps/api/tests/test_control_integration.py",
        # Live Langfuse readers and trace writers
        "apps/api/tests/test_metrics_integration.py",
        "apps/api/tests/test_langfuse_integration.py",
        "apps/api/tests/test_evals_integration.py",
        "apps/api/tests/test_promote_eval_case.py",
        "apps/api/tests/test_runs_proxy.py",
        "apps/worker/tests/eval/test_recorder.py",
        "apps/worker/tests/eval/test_stream.py",
        "runner/tests/test_otel.py",
        "runner/tests/test_otel_schema_drift.py",
    }
)


# tryfirst: xdist's loadgroup reads the group into the node id in its own
# modifyitems hook, so the marker has to exist before that hook runs.
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        path = item.path.relative_to(config.rootpath).as_posix()
        group = SHARED_LIVE_STATE_GROUP if path in SHARED_LIVE_STATE_FILES else path
        item.add_marker(pytest.mark.xdist_group(group))
