"""#4939: the runner enforces the factory agent's 250000 output-token allowance.

CURIE_BUDGET is built the way the worker writes it: ``BindingResolver.budget_for``
(apps/worker/src/curie_worker/binding.py) returns an ``aci_protocol.Budget``,
and ``BootEnv.render_worker`` (packages/aci-protocol/src/aci_protocol/session.py)
serializes it with ``budget.model_dump_json()``. The runner then parses it with
``RunnerConfig.from_env`` and hands ``config.ceiling`` to ``SessionRunner``
(runner/src/curie_runner/__main__.py), which builds one ``BudgetTracker`` per
turn from that ceiling.
"""

from aci_protocol import Budget
from curie_runner import BudgetTracker
from curie_runner.config import RunnerConfig


def _tracker(max_output_tokens_per_run: int) -> BudgetTracker:
    worker_budget = Budget(
        max_output_tokens_per_run=max_output_tokens_per_run,
        max_usd_per_day=5.0,
    ).model_dump_json()
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": "/plugin",
            "CURIE_SESSION_ID": "s",
            "CURIE_SANDBOX_ID": "b",
            "CURIE_BUDGET": worker_budget,
        }
    )
    assert config.max_usd_per_day == 5.0
    return BudgetTracker(ceiling=config.ceiling)


def test_factory_budget_allows_up_to_250000_output_tokens() -> None:
    tracker = _tracker(250000)
    tracker.set_total({"output_tokens": 249999})
    assert not tracker.exceeded
    tracker.set_total({"output_tokens": 250000})
    assert not tracker.exceeded
    tracker.set_total({"output_tokens": 250001})
    assert tracker.exceeded


def test_generic_budget_halts_past_100000_output_tokens() -> None:
    tracker = _tracker(100000)
    tracker.set_total({"output_tokens": 100000})
    assert not tracker.exceeded
    tracker.set_total({"output_tokens": 100001})
    assert tracker.exceeded
