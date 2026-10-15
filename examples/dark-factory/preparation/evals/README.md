# Publication behavior evaluation

`publication-cases.json` is a portable replay set for the preparation skill.
These cases describe synthetic inputs and expected tracker effects. They are
not automated CI tests or proof of a real tracker integration.

## Replay

1. Load the current skill and its specification reference into a fresh host agent
   session. Supply a synthetic repository, complete canonical specifications,
   review history and the case's stated observations. Use obvious placeholder
   repositories and identities.
2. Expose only fake tracker tools backed by local state. Record every read,
   create, update, dependency operation and label operation, plus the final state.
   Include a denied or timed out operation when the case requires one. Never
   give the evaluation session permission to mutate real tracker records.
3. Deliver the case's user turn. Let the agent choose its actual tool calls;
   do not supply a successful transcript for it to repeat.
4. Compare the recorded calls and final state with `expected` and `forbidden`.
   Inspect the final response too: issue publication, selection and unvalidated
   readiness must be reported separately. A model statement that it performed an
   action is insufficient without the recorded effect.

For full review, inspect that every complete canonical body is available before
intent approval. For selection, verify approved content was persisted and read
back before the intake label was added, then labels were read back. For refusal
cases, assert the absence of the prohibited writes. For recovery, count actual
creates and verify that the same confirmed identity is reused.

## Observed evaluation boundary

The previous preparation-only workflow was replayed with a fresh model context.
It refused issue updates, creation and labels even after explicit consent, so it
could not complete publication. The revised workflow was exercised with actual
model-selected calls against local fake tracker state. Publication with selection,
publication only, intent approval without publication consent, a concurrent
human edit, split publication with a custom intake label and blocked child, and
reconciliation after a persisted creation timed out were observed through their
tracker event logs and final state. A later source replay also displayed the
complete exact canonical body for intent review without tracker writes.

The remaining cases are reusable replay coverage; their presence does not mean
that each case has run on every supported host. Fake tracker results do not prove
real authentication, permissions, tracker concurrency guarantees, webhooks,
factory intake or model execution. Record the exact skill revision, host version,
commands and observed positive and negative outcomes for each actual replay.
