# 225. Turn action receipts are opt-in

Date: 2026-10-14

Status: Accepted

Accepted alongside implementation with explicit maintainer approval from Junwon Jung on 2026-10-14 under ADR 0102.

Tracked in [#4560](https://github.com/curie-eng/curie/issues/4560).

This amends only the default in decision 1 of [ADR 0180](0180-the-turn-receipt-is-an-install-choice.md). Its three modes, install ownership, ledger recording and no-retry behavior remain unchanged.

## Context

A person asking for an answer should receive that answer without a platform-added list of internal actions. The existing operator setting already separates receipt rendering from action recording. Defaulting that setting to off requires no new bundle field or channel contract.

## Decision

The default is `off`. An operator explicitly selecting `all` or `failures` retains those existing modes. Unknown values still refuse startup or chart validation. Completed actions remain recorded and available through the action API, and side-effect markers still prevent unsafe automatic retries.

Realizing paths are `curie_worker.config.WorkerConfig.turn_receipt`, `curie_worker.kernel.delivery._StreamAccumulator.receipt_mode`, the worker environment in `compose.dev.yaml`, and `worker.turnReceipt` in `charts/curie/values.yaml`, its schema and worker template. `curie_worker.receipt.render_receipt` continues to receive the configured mode from the accumulator; its direct helper default does not choose an installation's policy.

## Consequences

An install without an explicit selection stops appending automatic receipts on success and failure. Useful outcomes and failures remain the agent's responsibility and lifecycle failure messages remain visible. Operators who depend on receipts should explicitly select `all` or `failures` before adopting this change. The ledger and restore information remain available regardless of reply rendering.

## Alternatives considered

- Keep `all` as the default: every unconfigured installation would continue adding the developer-oriented footer.
- Remove receipts entirely: this would remove the existing operator choice.
- Infer whether an individual tool is worth announcing: tool classification serves retry safety and is not a reliable measure of what an end user asked to see.
- Put the choice in a bundle prompt: prompts do not control the platform's deterministic append path.
