# tier-diff demo

Everything needed to reproduce the recorded demo, and the honest boundary around
what it proves.

## What is real and what is scripted

The **diff engine is real**: `curie_worker.eval.tierdiff` plus its renderers, with
tests in `apps/worker/tests/eval/test_tierdiff.py`.

The **artifacts are produced by the real eval runner**. `generate.py` starts a
scripted runner over real HTTP and drives
`curie_worker.eval.runner.EvalRunner` against it, so the graders, the sampling
reduction, the result construction, and the new trajectory capture are all
production code paths. Only the model's replies are scripted, which is what a
fixture is.

The **artifacts are not cluster runs**. No Kubernetes was involved, and the
`cluster` tier's egress denial text was scripted rather than emitted by a real
network policy. Every artifact says so in its own `provenance` field, on every
tier, so a reader who never opens this file cannot mistake one for a real cluster
run.

## Regenerate the artifacts

```bash
uv run --directory apps/worker python demo/tier-diff/generate.py
```

Writes `artifacts/deployed-v0.7.0.json` and three repeats of
`artifacts/candidate-v0.7.1-rc.1-runN.json`. Deterministic: rerunning overwrites
with identical content apart from latency numbers.

## The four shots

The `curie` on PATH is likely a released build without this verb. Put this
branch's binary in front of it for the current shell only:

```bash
export PATH="$PWD/cli/target/debug:$PATH"
curie --version   # expect 0.7.0-rc.2
```

Run from the repository root. Artifact paths are resolved against the directory
you are standing in, so these work from anywhere.

```bash
# 1. The deployed bundle against itself. Nothing to report, and the report is quiet.
curie dev tier-diff \
  --deployed demo/tier-diff/artifacts/deployed-v0.7.0.json \
  --candidate demo/tier-diff/artifacts/deployed-v0.7.0.json

# 2. What a code review would show for the same change.
git diff --stat HEAD~1 -- examples/

# 3. The candidate against the deployed bundle, three repeats.
curie dev tier-diff \
  --deployed demo/tier-diff/artifacts/deployed-v0.7.0.json \
  --candidate demo/tier-diff/artifacts/candidate-v0.7.1-rc.1-run1.json \
  --candidate demo/tier-diff/artifacts/candidate-v0.7.1-rc.1-run2.json \
  --candidate demo/tier-diff/artifacts/candidate-v0.7.1-rc.1-run3.json

# 4. The same report as a pull request comment.
curie dev tier-diff \
  --deployed demo/tier-diff/artifacts/deployed-v0.7.0.json \
  --candidate demo/tier-diff/artifacts/candidate-v0.7.1-rc.1-run1.json \
  --candidate demo/tier-diff/artifacts/candidate-v0.7.1-rc.1-run2.json \
  --candidate demo/tier-diff/artifacts/candidate-v0.7.1-rc.1-run3.json \
  --format markdown
```

## What each row in shot 3 demonstrates

| Row | Demonstrates |
| --- | --- |
| `escalate_to_human` ROUTE CHANGED | A change no pass-rate diff can see: the case passes in both bundles, and the agent stopped calling `refund` |
| `quarterly_report` TIER DISAGREEMENT | The tier axis, plus a named cause and a pasteable remedy |
| `sync_crm_record` TIER DISAGREEMENT | The honesty valve: a cause the signature table does not know stays `unclassified` |
| `summarize_thread` FLAKY | A difference that did not reproduce is reported as suite instability, not as a change |
| 3 quiet rows | Unchanged and tier-consistent rows are counted, not listed |

## Reset

Nothing to reset. The commands are read-only and the artifacts are committed, so a
mistyped take costs nothing but a retake.
