"""Per-model token pricing for attributing a dollar cost to an eval turn (#390).

The eval runner knows a turn's token usage (carried on the ACI ``final`` event)
and the model it ran under; this module is the third input -- the price of those
tokens. Pricing lives here, on the consumer side, not on the wire: the ACI wire
carries raw usage, and what a token costs is a billing concern the recorder/matrix
own, not a property of the protocol.

The table is deny-by-default: a model id with no entry prices to ``None`` (cost
unknown) rather than to a guessed number, so a run under an unpriced model simply
falls out of the matrix's cost rollup instead of polluting it. That is also why
the fake-model path never gets a cost -- ``CURIE_FAKE_MODEL`` is not a real
model and is deliberately absent from the table.

Prices are USD per one million tokens, split input/output, matching Anthropic's
published list pricing (https://www.anthropic.com/pricing, list as of 2026-07).
Keep this list-price-only: eval cost is a coarse comparison signal across models,
not a billing ledger, so it does not model prompt-cache discounts, batch pricing,
or per-region multipliers.
"""

from __future__ import annotations

from typing import NamedTuple


class _Price(NamedTuple):
    """USD per one million tokens, input and output."""

    input_per_mtok: float
    output_per_mtok: float


# Keyed by a canonical model-family substring so a fully-qualified id -- a bare
# ``claude-opus-4-8`` or a platform-prefixed/dated ``anthropic.claude-opus-4-1-...``
# -- resolves to the same price via containment (see ``_price_for``). Ordered
# longest-key-first there so a more specific family wins over a prefix of it.
_PRICES: dict[str, _Price] = {
    # Anthropic list pricing, USD per 1M tokens (input / output).
    "claude-fable-5": _Price(10.0, 50.0),
    "claude-mythos-5": _Price(10.0, 50.0),
    "claude-opus-4-8": _Price(5.0, 25.0),
    "claude-opus-4-7": _Price(5.0, 25.0),
    "claude-opus-4-6": _Price(5.0, 25.0),
    "claude-opus-4-5": _Price(5.0, 25.0),
    "claude-opus-4-1": _Price(15.0, 75.0),
    "claude-opus-4": _Price(15.0, 75.0),
    "claude-sonnet-5": _Price(3.0, 15.0),
    "claude-sonnet-4-6": _Price(3.0, 15.0),
    "claude-sonnet-4-5": _Price(3.0, 15.0),
    "claude-sonnet-4": _Price(3.0, 15.0),
    "claude-haiku-4-5": _Price(1.0, 5.0),
    "claude-haiku-5": _Price(1.0, 5.0),
}

# Longest key first: ``claude-opus-4-8`` must be tried before ``claude-opus-4``
# so a specific family is never shadowed by a shorter prefix of itself.
_PRICE_KEYS: tuple[str, ...] = tuple(sorted(_PRICES, key=len, reverse=True))


def _price_for(model: str | None) -> _Price | None:
    """The price for ``model``, or ``None`` when the model is unknown/unset.

    Matches by family containment so an exact id, a platform-prefixed id
    (``anthropic.claude-...``), or a dated snapshot all resolve to one entry. An
    unrecognized id returns ``None`` -- cost stays unknown rather than guessed.
    """
    if not model:
        return None
    for key in _PRICE_KEYS:
        if key in model:
            return _PRICES[key]
    return None


def cost_usd(
    model: str | None,
    input_tokens: int | None,
    output_tokens: int | None,
) -> float | None:
    """Dollar cost of one turn's token usage under ``model``, or ``None``.

    Returns ``None`` -- cost unknown, keeping the turn out of the matrix's cost
    rollup rather than counting it as free -- when the model is unpriced, or when
    neither token count was reported (an unpriced model, the fake tier, or a
    provider that reported no usage). A reported count that is ``None`` is treated
    as zero once at least one side is known, so a turn with only output tokens
    still prices.
    """
    price = _price_for(model)
    if price is None or (input_tokens is None and output_tokens is None):
        return None
    return (
        (input_tokens or 0) * price.input_per_mtok
        + (output_tokens or 0) * price.output_per_mtok
    ) / 1_000_000
