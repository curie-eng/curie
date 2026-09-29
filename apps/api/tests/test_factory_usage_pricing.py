"""Pure price matching and the cost formula (#3223). No database, no network.

Pinned interface in ``curie_api.factory_usage``:

- ``match_price(models_payload: dict, model: str) -> dict[str, Decimal] | None``
  reads an OpenRouter-shaped ``{"data": [{"id", "pricing": {...}}]}`` payload and
  returns per-token rates keyed ``prompt``, ``completion``, ``cache_read``,
  ``cache_write``. Exact id first, else a unique match on the normalized name
  (lowercase, ``provider/`` dropped, ``.`` -> ``-``). Ambiguous or none -> None.
  Missing cache rates fall back to the prompt rate.
- ``estimate_cost(price, *, input_tokens, cached_input_tokens,
  cache_write_tokens, output_tokens) -> Decimal`` where ``price`` is a ``Price``.

The payload below is fabricated; the rates are placeholders, not published prices.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from curie_api.factory_usage import Price, estimate_cost, match_price

PAYLOAD = {
    "data": [
        {
            "id": "example-lab/alpha-4.5",
            "pricing": {
                "prompt": "0.000003",
                "completion": "0.000015",
                "input_cache_read": "0.0000003",
                "input_cache_write": "0.00000375",
            },
        },
        {
            "id": "example-lab/beta-1",
            "pricing": {"prompt": "0.000001", "completion": "0.000002"},
        },
        {"id": "vendor-a/gamma-2", "pricing": {"prompt": "0.000005", "completion": "0.00001"}},
        {"id": "vendor-b/gamma-2", "pricing": {"prompt": "0.000006", "completion": "0.00002"}},
    ]
}


def test_an_exact_id_matches() -> None:
    assert match_price(PAYLOAD, "example-lab/alpha-4.5") == {
        "prompt": Decimal("0.000003"),
        "completion": Decimal("0.000015"),
        "cache_read": Decimal("0.0000003"),
        "cache_write": Decimal("0.00000375"),
    }


def test_a_normalized_name_matches_uniquely() -> None:
    rates = match_price(PAYLOAD, "Alpha-4-5")
    assert rates is not None
    assert rates["prompt"] == Decimal("0.000003")


def test_missing_cache_rates_fall_back_to_the_prompt_rate() -> None:
    rates = match_price(PAYLOAD, "example-lab/beta-1")
    assert rates is not None
    assert rates["cache_read"] == Decimal("0.000001")
    assert rates["cache_write"] == Decimal("0.000001")


def test_an_ambiguous_or_unknown_name_matches_nothing() -> None:
    assert match_price(PAYLOAD, "gamma-2") is None
    assert match_price(PAYLOAD, "example-lab/delta-9") is None
    assert match_price({"data": []}, "example-lab/alpha-4.5") is None


def test_the_cost_formula_prices_each_token_kind_at_its_rate() -> None:
    price = Price(
        prompt=Decimal("0.000001"),
        completion=Decimal("0.000002"),
        cache_read=Decimal("0.0000001"),
        cache_write=Decimal("0.000003"),
        source="https://prices.example.com/api/v1/models",
        as_of=datetime(2026, 9, 1, tzinfo=UTC),
    )
    cost = estimate_cost(
        price,
        input_tokens=1_000_000,
        cached_input_tokens=100_000,
        cache_write_tokens=10_000,
        output_tokens=500_000,
    )
    assert cost == Decimal("2.04")
