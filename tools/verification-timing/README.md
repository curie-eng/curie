# verification-timing

A lightweight, offline summary of how much wall-clock time the verification
phases of an `/implement` run actually consumed. It reads the run-state files
this repository already writes and reports what was measured.

It is **not** an observability product, **not** a monitoring backend, and it
performs **no** historical scan of its own. It has no runtime dependency, adds
nothing to the shipped product, and reads only the files you hand it.

## The optional `timings` block

Each entry of `e2e.evidence` in a run-state file may carry an optional
`timings` object. It is backward compatible: a run-state written without it
loads fine and reports every phase as unknown.

```json
{
  "tier": "skill",
  "command": "uv run pytest tests/test_example.py",
  "commit": "0000000000000000000000000000000000000000",
  "timings": {
    "startup": { "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:00:30Z" },
    "tests": { "seconds": 44.0 },
    "cleanup": { "seconds": 3.5 },
    "retries": { "count": 1, "seconds": 12.0 },
    "external_wait": null
  }
}
```

A phase records either `seconds`, or a `started_at`/`completed_at` pair, or
both (they must agree to within one second, or the record is rejected). The
five phases are:

- **startup** — bringing the environment up before any test executes: image
  pulls, stack boot, dependency install, fixture provisioning.
- **tests** — executing the test command itself, and nothing else.
- **cleanup** — tearing the environment back down afterwards.
- **retries** — time spent re-running work that had already been attempted.
  `count` is the number of retries and is aggregated separately from duration.
- **external_wait** — blocked on something outside the run: a queued CI runner,
  a third-party endpoint, a rate limit.

`startup` is recorded separately from `tests` on purpose. Conflating the two is
precisely what makes a savings claim unfalsifiable — a change that only moves
work from one bucket to the other looks identical to a change that removes the
work, and nobody can tell the difference from a single combined number.

## Observed-only

Missing historical timing **remains unknown**. The summarizer never backfills a
guess: an unmeasured phase is reported as `{"seconds": null, "provenance":
"unknown"}` and is excluded from every count, sum, median and percentile.
Provenance is `timestamps` when the duration came from a timestamp pair,
`seconds` when it came from a declared duration, and `unknown` when there was
no measurement at all.

Because unknown phases are counted as unknown rather than as zero, a summary
over sparse data is honest about its own sample size: `observed_count` and
`unknown_count` sit next to each other in every phase total, and a phase nobody
measured reports `null` totals rather than a confident `0`.

A malformed record — a negative duration, a non-numeric duration, a completion
before its start, an unrecognised phase name — aborts the load naming the
offending file and phase. It is never skipped silently, because a quietly
dropped row is indistinguishable from a fast one.

## Running it

Summarize one file, or every `*.state.json` in a directory:

```bash
uv run python tools/verification-timing/summarize_timings.py path/to/run.state.json
```

The tool prints a deterministic JSON summary (`json.dumps(..., sort_keys=True,
indent=2)`) and exits 0, or exits 2 and prints nothing to stdout if any input
record is malformed.

Its test suite:

```bash
uv run pytest tools/verification-timing/tests
```

## Three-stage evidence contract

This tool is stage 1 of a staged pilot:

- **Stage 1 (this change)** — measurement and reporting support. Focused checks
  only.
- **Stage 2** — a development-only approval integration slice. Focused checks
  only.
- **Stage 3** — the structured development command, the complete failure
  scenarios, and the FULL assembled verification for all three diffs together,
  run against the final combined candidate.

Stages 1 and 2 carry an explicit verification **debt**. Each one runs focused
test-first checks plus review, and neither is independently merge-ready on its
own. The full applicable repository baseline and the union of the required
tiers are owed by stage 3, run against the assembled candidate.

This staging is a specific, maintainer-authorized exception for this one pilot.
It changes no repository-wide verification mandate: the tier rules in
`AGENTS.md` remain in force, and a fake result never substitutes for a required
real tier.
