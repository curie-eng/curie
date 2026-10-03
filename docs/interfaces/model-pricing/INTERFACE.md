---
seam: Model pricing / PriceBook
kind: CLEAN
impls: 1 price source (OpenRouterPriceBook) + disabled fallback
grade: not separately graded
vision_row: null
epics: ["#3223", "#3923"]
order: 25
---

# INTERFACE: Model pricing / PriceBook

> Part of the Curie swappable seam catalog. See the [seam index](../../interfaces.md).

<!-- BEGIN GENERATED: header (curie dev docs-lint) -->
> **Kind:** CLEAN &nbsp;·&nbsp; **Implementations today:** 1 price source (OpenRouterPriceBook) + disabled fallback &nbsp;·&nbsp; **Swap-readiness grade:** not separately graded
<!-- END GENERATED: header -->

**Kind legend:** CLEAN = a real `Protocol` or typed port class. SOFT = swap through config or wire without a code interface. NONE = not built yet.

## The black line

Factory usage accounting asks a price book for model rates through `PriceBook`
(`apps/api/src/curie_api/factory_usage.py::PriceBook`). The accounting path owns
token records and cost calculation; the price book owns rate lookup and its
provenance. This typed pricing boundary is independent of the runner's model
provider credential and wire selection. Choosing a pricing source does not
choose the model endpoint.

## Current contract

The port has one async operation, `price(model: str) -> Price | None`
(`apps/api/src/curie_api/factory_usage.py::PriceBook.price`). `Price` is an
immutable value containing `Decimal` rates in USD per token for `prompt`,
`completion`, `cache_read`, and `cache_write`, plus the source string and
`as_of` timestamp (`apps/api/src/curie_api/factory_usage.py::Price`). `None`
means no usable price, not a free model or zero cost.

`get_price_book`
(`apps/api/src/curie_api/factory_usage.py::get_price_book`)
selects the implementation from `Settings.factory_price_source_url`
(`apps/api/src/curie_api/config.py::Settings`), configured by
`FACTORY_PRICE_SOURCE_URL`. The default is
`https://openrouter.ai/api/v1/models`. A nonempty trimmed URL selects
`OpenRouterPriceBook`
(`apps/api/src/curie_api/factory_usage.py::OpenRouterPriceBook`), with one
cached instance per URL in the API process. An empty URL selects `_NoPriceBook`
(`apps/api/src/curie_api/factory_usage.py::_NoPriceBook`), whose lookup always
returns `None`.

The current source expects an OpenRouter shaped JSON object with a `data` list.
Each model entry supplies an `id` and a `pricing` object. `match_price`
(`apps/api/src/curie_api/factory_usage.py::match_price`) uses an exact model ID
first, otherwise a unique normalized name. Normalization removes the provider
prefix, folds case, and replaces dots with hyphens. Ambiguous or absent matches
return no price. Prompt and completion rates must be finite nonnegative
numbers; absent or invalid cache rates use the prompt rate
(`apps/api/src/curie_api/factory_usage.py::_rates`).

`OpenRouterPriceBook` caches a successful price list for six hours. Fetch or
parse failure produces no price and postpones the next attempt for five
minutes. The source URL and successful fetch time become the price provenance.
The implementation uses an async lock to serialize refreshes and a ten second
HTTP timeout (`apps/api/src/curie_api/factory_usage.py::OpenRouterPriceBook`).

The usage route injects `PriceBook` into `record_usage`
(`apps/api/src/curie_api/routers/factory_status.py::report_work_item_usage`,
`apps/api/src/curie_api/factory_usage.py::record_usage`). For each model it
computes the estimate from reported input, cached input, cache write, and
output token counts using `estimate_cost`
(`apps/api/src/curie_api/factory_usage.py::estimate_cost`). It stores the
estimate rounded to six decimal places alongside rate provenance. A missing
price preserves the token counts with no estimate or provenance. The unique
request, turn, and model record makes replay a no op.

## Implementations today

One production price source ships, `OpenRouterPriceBook`, plus `_NoPriceBook`
for disabled pricing. A different configured URL can supply the same model
list shape. A source with a different payload would need an implementation of
the existing `PriceBook` contract and composition at `get_price_book`; no
registration or arbitrary adapter loading is built.

`apps/api/tests/test_factory_usage.py` uses a price book double to exercise
usage persistence and estimates. It is test coverage of the port, not another
production price source.

## Known leakage

The port's value is provider independent, but the configured implementation
parses OpenRouter field names and model naming conventions. The exact or
unique normalized match is a lookup policy, not proof that a provider billed
those rates. Cache rate fallback and cached public prices make this an estimate.
It does not replace provider invoices.

Selection is URL based and the concrete instance cache lives in one API
process. The API records the price available at report time. A replay preserves
the existing usage row instead of repricing it after a catalog refresh
(`apps/api/src/curie_api/factory_usage.py::record_usage`).

## Cross-links

1. **Related work:** #3223 implements factory usage and estimated cost; #3923
   catalogs the pricing boundary.
2. **Vision doc:** [architecture-vision.md](../../architecture-vision.md).
   Model pricing is not one of its six graded jobs.
3. **Related seam:** [Model provider](../model-provider/INTERFACE.md) describes
   model wire and credential selection. Pricing has its own entry because its
   typed port, API owner, and source selection differ from that config seam.
