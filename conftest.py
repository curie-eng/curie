"""Suite-wide xdist scheduling for the required CI run.

CI runs `pytest -n 4 --dist loadgroup`. Every test is grouped by its own file,
which schedules exactly like `--dist loadfile`, except the files below. They
share process-external state that no fixture namespaces, so they are pinned to
one group and never overlap each other on different workers:

- fixed Valkey keys: the thread reset sets are frozen shared names that any
  running consumer drains, and the default eval stream is read by exact length;
- the shared Langfuse project: exact aggregate comparisons read ClickHouse
  while other files ingest traces into the same project.

A serial run is unaffected: without xdist the marker is inert.
"""

from __future__ import annotations

import pytest

SHARED_LIVE_STATE_GROUP = "shared-live-state"

SHARED_LIVE_STATE_FILES = frozenset(
    {
        # Valkey thread reset sets: every file that runs a real consumer, since
        # each turn and maintenance tick drains them (curie_worker/consumer.py)
        "apps/worker/tests/kernel/test_attachment_claim.py",
        "apps/worker/tests/kernel/test_completion_outbox.py",
        "apps/worker/tests/kernel/test_consumer.py",
        "apps/worker/tests/kernel/test_consumer_dead_letter.py",
        "apps/worker/tests/kernel/test_deleted_mail_completion.py",
        "apps/worker/tests/kernel/test_delivery_ownership.py",
        "apps/worker/tests/kernel/test_github_review_queue.py",
        "apps/worker/tests/kernel/test_kernel.py",
        "apps/worker/tests/kernel/test_long_turn_evidence.py",
        "apps/worker/tests/kernel/test_mixed_version.py",
        "apps/worker/tests/kernel/test_otel_runtime.py",
        "apps/worker/tests/kernel/test_turn_not_started.py",
        "apps/worker/tests/kernel/test_upgrade_drain.py",
        "apps/worker/tests/test_thread_reset_vector.py",
        "apps/api/tests/test_thread_reset_vector.py",
        "apps/api/tests/test_control_integration.py",
        "apps/api/tests/test_github_review_events.py",
        # The default eval stream (curie:evals): exact length reads and producers
        "apps/api/tests/test_evalqueue_integration.py",
        "apps/api/tests/test_evals_trigger_integration.py",
        "apps/api/tests/test_gitflow_integration.py",
        "apps/worker/tests/test_upgrade_drain.py",
        "apps/worker/tests/sandbox/test_e2e_resilience.py",
        "apps/worker/tests/test_config.py",
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
