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

CI also splits the suite across parallel jobs with `--ci-shard INDEX/COUNT`.
Shards are cut along the same groups, so a group never straddles two jobs, and
every shard collects the whole suite before deselecting what is not its own, so
the shards partition exactly the unfiltered collection. Groups are assigned
greedily, largest first, to the shard with the fewest tests so far; the
assignment depends only on the collected node ids, so every shard (and every
xdist worker inside one) computes the same split.
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
        "apps/api/tests/test_control_integration.py",
        "apps/api/tests/test_github_review_events.py",
        # GitHub review fixtures share fixed delivery identities, whose Valkey
        # idempotency keys collide across these files
        "apps/api/tests/test_github_review_binding_scope.py",
        "apps/api/tests/test_github_review_pending_outbox.py",
        "apps/api/tests/test_github_review_sender_authority.py",
        "apps/api/tests/test_github_review_terminal.py",
        # Diffs a global relay key glob before and after each call
        "apps/api/tests/test_cluster_message_results.py",
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
    groups: dict[str, list[pytest.Item]] = {}
    for item in items:
        path = item.path.relative_to(config.rootpath).as_posix()
        group = SHARED_LIVE_STATE_GROUP if path in SHARED_LIVE_STATE_FILES else path
        item.add_marker(pytest.mark.xdist_group(group))
        groups.setdefault(group, []).append(item)

    shard = config.getoption("ci_shard")
    if shard is None:
        return
    index, count = shard
    owner = assign_shards({group: len(members) for group, members in groups.items()}, count)
    keep = [item for item in items if owner[_group_of(item, groups)] == index]
    deselected = [item for item in items if owner[_group_of(item, groups)] != index]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = keep


def _group_of(item: pytest.Item, groups: dict[str, list[pytest.Item]]) -> str:
    for marker in item.iter_markers("xdist_group"):
        name = marker.args[0]
        assert isinstance(name, str)
        return name
    raise AssertionError(f"{item.nodeid} has no xdist group")


def assign_shards(group_sizes: dict[str, int], count: int) -> dict[str, int]:
    """Map each group to a 1-based shard, balancing test counts greedily."""
    loads = [0] * count
    owner: dict[str, int] = {}
    for group, size in sorted(group_sizes.items(), key=lambda entry: (-entry[1], entry[0])):
        target = min(range(count), key=lambda shard: (loads[shard], shard))
        loads[target] += size
        owner[group] = target + 1
    return owner


def _parse_shard(value: str) -> tuple[int, int]:
    index_text, separator, count_text = value.partition("/")
    if not separator or not index_text.isdigit() or not count_text.isdigit():
        raise pytest.UsageError(f"--ci-shard must be INDEX/COUNT, got {value!r}")
    index, count = int(index_text), int(count_text)
    if not 1 <= index <= count:
        raise pytest.UsageError(f"--ci-shard index must be within 1..{count}, got {value!r}")
    return index, count


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--ci-shard",
        dest="ci_shard",
        default=None,
        type=_parse_shard,
        metavar="INDEX/COUNT",
        help="Run only this shard of the suite, split along xdist groups.",
    )
