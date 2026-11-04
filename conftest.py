"""Suite-wide xdist scheduling for the required CI run.

CI runs `pytest -n 4 --dist loadgroup`. By default tests are grouped by file,
which schedules exactly like `--dist loadfile`, except the files below. They
share process-external state that no fixture namespaces, so they are pinned to
one group and never overlap each other on different workers:

- fixed Valkey keys: the thread reset sets are frozen shared names that any
  running consumer drains, and the default eval stream is read by exact length;
- the shared Langfuse project: exact aggregate comparisons read ClickHouse
  while other files ingest traces into the same project.
- the protected source runner peer: its closed projection fixes port 8080,
  so those files share a separate scheduling group.

A serial run is unaffected: without xdist the marker is inert.

Files whose cases own all process-external state may opt into per-test groups
with ``pytestmark = pytest.mark.xdist_independent``. This also distributes their
cases across CI shards. The shared-state file lists above take precedence over
the marker. Unmarked files keep their file group.

CI also splits the suite across parallel jobs with `--ci-shard INDEX/COUNT`.
Shards are cut along the same groups, so a group never straddles two jobs, and
every shard collects the whole suite before deselecting what is not its own, so
the shards partition exactly the unfiltered collection. Groups are assigned
greedily, largest first, to the shard with the fewest tests so far; the
assignment depends only on the collected node ids, so every shard (and every
xdist worker inside one) computes the same split.

A pull request shard may boot fewer services than the full dev stack. Shards
1..FULL boot the full tier (Langfuse, ClickHouse, the OTel collector), shards
1..STORAGE boot at least the storage tier (RustFS), and the rest boot only the
core tier (Postgres and Valkey). `--ci-shard-tiers STORAGE,FULL` tells the
split about those limits: each group's tier is the highest service tier of its
files, as `tools/e2e-ci-selection/pytest_impact.py` classifies them statically,
and groups are placed highest tier first, then largest first, onto the least
loaded shard that boots their tier. Without the option, or when both limits
equal the shard count, the split is exactly the one described above.

A misclassified file would then skip for want of a service rather than fail.
When `CURIE_CI_STACK_TIER` is `core` or `storage`, a skip (of a test or of a
whole module at collection) whose reason names a service that tier did not
boot is reported as a failure naming the tier, so lost coverage is loud. The
`full` tier, or no value, leaves every skip alone.
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from collections.abc import Generator, Iterable, Mapping
from pathlib import Path
from types import ModuleType

import pytest

pytest_plugins = ["curie_test_support.duration_budget"]

SHARED_LIVE_STATE_GROUP = "shared-live-state"
PROTECTED_RUNNER_GROUP = "protected-source-runner-port"
PROTECTED_RUNNER_FILES = frozenset(
    {
        # The closed protected projection fixes the external runner peer at 8080.
        # These source fixtures share that host port, while owning separate brokers.
        "apps/api/tests/test_guarded_nomination_api_integration.py",
        "apps/worker/tests/protected_settlement/test_actual_source.py",
        "apps/worker/tests/protected_settlement/test_terminal_launch_gate.py",
        "apps/worker/tests/test_commissioning_cleanup.py",
        "apps/worker/tests/test_commissioning_launch.py",
        "apps/worker/tests/test_commissioning_record_refusal.py",
        "apps/worker/tests/test_protected_consumer_recovery.py",
        "apps/worker/tests/test_protected_entry_runtime.py",
        "apps/worker/tests/test_protected_launch_server_integration.py",
        "apps/worker/tests/test_protected_original_cap.py",
        "apps/worker/tests/test_protected_pending_cleanup.py",
        "apps/worker/tests/test_protected_primary_runtime.py",
        "apps/worker/tests/test_protected_recovery_source.py",
        "apps/worker/tests/test_protected_retirement_server.py",
        "apps/worker/tests/test_protected_terminal_copy_cleanup.py",
    }
)

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
        "apps/api/tests/test_migration_0043_github_reviews.py",
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


TIER_NAMES = ("core", "storage", "full")
STACK_TIER_ENV = "CURIE_CI_STACK_TIER"
PYTEST_IMPACT = Path(__file__).resolve().parent / "tools" / "e2e-ci-selection" / "pytest_impact.py"


def _service_pattern(*names: str) -> re.Pattern[str]:
    # Word-ish: no letter or digit may touch either end, so "s3" never matches
    # inside another word, while plurals and snake_case or kebab-case spellings
    # ("buckets", "object_store", "OTEL_EXPORTER") still match.
    alternatives = "|".join(r"[\s_-]?".join(map(re.escape, name.split())) for name in names)
    return re.compile(rf"(?<![a-z0-9])(?:{alternatives})(?:e?s)?(?![a-z0-9])", re.IGNORECASE)


_FULL_ONLY_SERVICES = ("langfuse", "clickhouse", "collector", "otel", "compose stack", "dev stack")
# Services each reduced tier did not boot; a skip naming one of them is a failure.
UNBOOTED_SERVICES = {
    "core": _service_pattern("rustfs", "s3", "bucket", "object store", *_FULL_ONLY_SERVICES),
    "storage": _service_pattern(*_FULL_ONLY_SERVICES),
}


# tryfirst: xdist's loadgroup reads the group into the node id in its own
# modifyitems hook, so the marker has to exist before that hook runs.
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    groups: dict[str, list[pytest.Item]] = {}
    for item in items:
        path = item.path.relative_to(config.rootpath).as_posix()
        group = SHARED_LIVE_STATE_GROUP if path in SHARED_LIVE_STATE_FILES else path
        if path in PROTECTED_RUNNER_FILES:
            group = PROTECTED_RUNNER_GROUP
        elif path not in SHARED_LIVE_STATE_FILES and item.get_closest_marker("xdist_independent"):
            group = item.nodeid
        item.add_marker(pytest.mark.xdist_group(group))
        groups.setdefault(group, []).append(item)

    shard = config.getoption("ci_shard")
    tier_limits = config.getoption("ci_shard_tiers")
    if shard is None:
        if tier_limits is not None:
            raise pytest.UsageError("--ci-shard-tiers requires --ci-shard")
        return
    index, count = shard
    sizes = {group: len(members) for group, members in groups.items()}
    if tier_limits is None or tier_limits == (count, count):
        owner = assign_shards(sizes, count)
    else:
        storage, full = tier_limits
        if not full <= storage <= count:
            raise pytest.UsageError(
                f"--ci-shard-tiers needs FULL <= STORAGE <= {count}, got {storage},{full}"
            )
        owner = assign_shards(
            sizes,
            count,
            group_tiers=_group_tiers(config.rootpath, groups),
            tier_limits=tier_limits,
        )
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


def assign_shards(
    group_sizes: dict[str, int],
    count: int,
    *,
    group_tiers: Mapping[str, int] | None = None,
    tier_limits: tuple[int, int] | None = None,
) -> dict[str, int]:
    """Map each group to a 1-based shard, balancing test counts greedily.

    With `tier_limits` (STORAGE, FULL), a group of service tier 1 may only land
    on shards 1..STORAGE and one of tier 2 only on 1..FULL; groups are placed
    highest tier first so the limited shards are filled by the groups that need
    them. `group_tiers` defaults every unlisted group to tier 0.
    """
    loads = [0] * count
    owner: dict[str, int] = {}
    if tier_limits is None:
        for group, size in sorted(group_sizes.items(), key=lambda entry: (-entry[1], entry[0])):
            target = min(range(count), key=lambda shard: (loads[shard], shard))
            loads[target] += size
            owner[group] = target + 1
        return owner
    tiers = group_tiers or {}
    limits = (count, *tier_limits)
    for group, size in sorted(
        group_sizes.items(),
        key=lambda entry: (-tiers.get(entry[0], 0), -entry[1], entry[0]),
    ):
        tier = tiers.get(group, 0)
        limit = min(limits[tier], count)
        if limit <= 0:
            raise pytest.UsageError(
                f"xdist group {group!r} needs the {TIER_NAMES[tier]} tier (tier {tier}), "
                f"but --ci-shard-tiers {tier_limits[0]},{tier_limits[1]} boots it on no shard"
            )
        target = min(range(limit), key=lambda shard: (loads[shard], shard))
        loads[target] += size
        owner[group] = target + 1
    return owner


def _load_pytest_impact() -> ModuleType:
    name = "curie_root_conftest_pytest_impact"
    loaded = sys.modules.get(name)
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location(name, PYTEST_IMPACT)
    if spec is None or spec.loader is None:
        raise pytest.UsageError(f"--ci-shard-tiers cannot load {PYTEST_IMPACT}")
    module = importlib.util.module_from_spec(spec)
    # Dataclasses resolve their module through sys.modules while the class body runs.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _group_tiers(root: Path, groups: Mapping[str, Iterable[pytest.Item]]) -> dict[str, int]:
    files = {
        group: sorted({item.path.relative_to(root).as_posix() for item in members})
        for group, members in groups.items()
    }
    tiers_for = _load_pytest_impact().tiers_for
    file_tiers: dict[str, int] = tiers_for(
        root, sorted({f for paths in files.values() for f in paths})
    )
    return {
        group: max((file_tiers.get(path, 0) for path in paths), default=0)
        for group, paths in files.items()
    }


def _parse_shard(value: str) -> tuple[int, int]:
    index_text, separator, count_text = value.partition("/")
    if not separator or not index_text.isdigit() or not count_text.isdigit():
        raise pytest.UsageError(f"--ci-shard must be INDEX/COUNT, got {value!r}")
    index, count = int(index_text), int(count_text)
    if not 1 <= index <= count:
        raise pytest.UsageError(f"--ci-shard index must be within 1..{count}, got {value!r}")
    return index, count


def _parse_shard_tiers(value: str) -> tuple[int, int]:
    storage_text, separator, full_text = value.partition(",")
    if not separator or not storage_text.isdigit() or not full_text.isdigit():
        raise pytest.UsageError(f"--ci-shard-tiers must be STORAGE,FULL, got {value!r}")
    return int(storage_text), int(full_text)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--ci-shard",
        dest="ci_shard",
        default=None,
        type=_parse_shard,
        metavar="INDEX/COUNT",
        help="Run only this shard of the suite, split along xdist groups.",
    )
    parser.addoption(
        "--ci-shard-tiers",
        dest="ci_shard_tiers",
        default=None,
        type=_parse_shard_tiers,
        metavar="STORAGE,FULL",
        help=(
            "Shards 1..STORAGE boot RustFS and 1..FULL boot Langfuse; keep every group "
            "on a shard that boots the services its files need."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "xdist_independent: cases own their external state and may run on separate workers/shards",
    )
    tier = os.environ.get(STACK_TIER_ENV, "")
    if tier and tier not in TIER_NAMES:
        raise pytest.UsageError(
            f"{STACK_TIER_ENV} must be one of {', '.join(TIER_NAMES)}, got {tier!r}"
        )


def _guard_skip(report: pytest.CollectReport | pytest.TestReport) -> None:
    """Turn a skip for a service this shard's tier did not boot into a failure."""
    if not report.skipped or hasattr(report, "wasxfail"):
        return
    tier = os.environ.get(STACK_TIER_ENV, "")
    pattern = UNBOOTED_SERVICES.get(tier)
    if pattern is None:
        return
    longrepr = report.longrepr
    reason = longrepr[2] if isinstance(longrepr, tuple) else str(longrepr)
    reason = reason.removeprefix("Skipped: ")
    if not pattern.search(reason):
        return
    report.outcome = "failed"
    report.longrepr = (
        f"{STACK_TIER_ENV}={tier}: this shard booted only the {tier} tier, and this skip "
        f"names a service that tier does not boot: {reason!r}. A skip here would silently "
        "drop coverage; classify the file for a higher service tier in "
        "tools/e2e-ci-selection/pytest_impact.py."
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    _guard_skip(report)
    return report


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(
    collector: pytest.Collector,
) -> Generator[None, pytest.CollectReport, pytest.CollectReport]:
    report = yield
    _guard_skip(report)
    return report
