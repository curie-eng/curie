"""Per-run budget accounting for the runner.

The ACI budget (``CURIE_BUDGET``) carries ``max_output_tokens_per_run`` and
``max_usd_per_day``. This module enforces the per-run **output token ceiling**:
the runner accumulates output tokens as the SDK reports usage and halts the turn
with a classified-failure final once the ceiling is crossed. The daily USD cap is
handed to the SDK natively (``ClaudeAgentOptions.max_budget_usd``); it is a
process/session ceiling the harness enforces, not a per-turn concern here.

Enforcement granularity is the SDK loop boundary: usage is read from each message
the SDK yields (assistant messages when they carry usage, and always the terminal
result), so the halt lands at the first boundary where cumulative output crosses
the ceiling, not mid-token. That is the tightest guarantee the SDK's usage
reporting allows and is sufficient for the product behavior (bounding spend, not
byte-exact truncation).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

# The classification string carried on the error event and used by consumers
# (F1's retry rules) to recognise a budget halt versus a model failure.
BUDGET_CLASSIFICATION = "budget-exceeded"


def output_tokens(usage: Mapping[str, Any] | None) -> int:
    """Extract the output-token count from an SDK usage mapping.

    Returns 0 for a missing usage block or a missing/non-integer field, so a
    turn with no usage reporting simply never trips the ceiling rather than
    erroring.
    """

    if not usage:
        return 0
    value = usage.get("output_tokens")
    return value if isinstance(value, int) else 0


@dataclass
class BudgetTracker:
    """Accumulates output tokens for one run and reports ceiling crossings.

    Two SDK usage sources report the same tokens and must not be double counted:
    ``AssistantMessage.usage`` is the per-message output as the turn streams, and
    ``ResultMessage.usage`` is the SDK's authoritative aggregate for the whole
    turn (equal to the sum of the per-response values). So streaming usage is
    *summed once per message id* (for mid-turn enforcement) while the terminal
    total *replaces* rather than adds -- ``used`` is the max of the two, which is
    correct whether the SDK reports per-message usage, only a terminal total,
    or both.
    """

    ceiling: int
    _incremental: int = 0
    _total: int = 0
    _seen_message_ids: set[str] = field(default_factory=set, init=False, repr=False)

    def add_increment(self, usage: Mapping[str, Any] | None, message_id: str | None = None) -> None:
        """Count a response once per run; messages without a usable id still add.

        The SDK may emit several assistant content blocks with the same
        response id and usage. Each turn owns a fresh tracker and seen set.
        """

        if isinstance(message_id, str) and message_id:
            if message_id in self._seen_message_ids:
                return
            self._seen_message_ids.add(message_id)
        self._incremental += output_tokens(usage)

    def set_total(self, usage: Mapping[str, Any] | None) -> None:
        """Record the terminal result's authoritative turn total (does not add)."""

        self._total = max(self._total, output_tokens(usage))

    @property
    def used(self) -> int:
        return max(self._incremental, self._total)

    @property
    def exceeded(self) -> bool:
        """True once output tokens exceed the ceiling (<=0 ceiling = unbounded)."""

        return self.ceiling > 0 and self.used > self.ceiling
