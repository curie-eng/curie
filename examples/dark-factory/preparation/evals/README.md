# Preparation behavior evaluation

`publication-cases.json` is a portable replay set for the preparation skill.
These cases describe synthetic inputs, clarification behavior and tracker effects. They are
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

For clarification cases, materialize the supplied `repository_files` in an
isolated checkout and provide the `user_turn` without the expected answers or
grading criteria. The agent must inspect actual files rather than a summary of
their contents. Capture its next questions and subsequent complete draft. Count
independent decisions, not numbered bullets: unrelated choices bundled into one
bullet still count separately. Use `follow_up` as a later developer turn when
present and inspect the resulting specification for the changed decision.

Check the clear case for zero clarification questions, the ambiguous case for
questions that address its material choices with concrete behavior alternatives,
and the repository case for inspection instead of asking about documented facts.
Do not grade a lower question count as better by itself. Check full review and
corrections in the actual canonical body, separately from report contents. These
are host-agent observations; a JSON fixture or prompt substring assertion is not
proof that the behavior ran.

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

The ambiguity-driven revision was also exercised in fresh offline preparation
contexts. A fully specified export request produced no behavioral questions and
presented the complete specification for review. An underspecified request
produced questions about material alternatives and retained unresolved choices in
its provisional draft. A subsequent developer correction changed both Decisions
and Acceptance criteria in the complete body without authorizing publication.
These observations exercise the instructions, not every client, an optimal
question count, or improved factory completion.
