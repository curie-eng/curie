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

For verification-feasibility cases, materialize the sandbox configuration, workflow
and tests as raw files. Let the agent inspect them and run permitted baseline probes.
Check the actual per-criterion classification, evidence and full draft for preserved
intent. Check that missing coverage becomes explicit tests to add and that approved
external qualification remains visible without blocking selection. Inspect tracker
logs to prove unresolved gaps preventing verification of the agreed implementation
receive no intake label, including after explicit start consent. A supported sandbox route
and a supported observable CI route are positive controls. Host-only success,
secret-dependent CI with no factory observation path, a new test that fails only
for the missing feature, and changed environment evidence exercise the boundaries.
The post-PR cases check that a scripted environment-heavy scenario prompts an
explicit developer choice, while focused factory tests remain required and
approved human evaluation remains a merge gate with PR-head evidence.
Publish-only must remain available without pretending gaps are resolved. Do not
count these fixtures as observed runs; record fresh host results separately.

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

The simplified intake workflow was replayed in a fresh host context with no CLI,
repository pin or compatibility endpoint. A synthetic existing issue received the
exact reviewed canonical body; the fake tracker log showed read, update, read,
one configured label add and final readback. No input prefix was added. No real
tracker or factory run was performed. This tests publication behavior, not the
impact on accepted factory outcomes.

The install-only revision was replayed again in a fresh context after removing
managed update and repository resolution instructions. The fake tracker received
one update and one label addition with readbacks, the complete reviewed body was
preserved. No managed version command or real external operation ran in that replay.

The earlier verification-feasibility revision was exercised in fresh contexts using raw
synthetic repository files, trusted sandbox configuration and local fake tracker
state. A Python standard-library task retained proposed tests as sandbox-verifiable
and completed approved publication and selection. A live-cloud requirement with
missing credentials/network and a staging CI requirement with no factory result
observation path both retained their original outcomes, completed approved
publication and received no intake label. Their baseline runners exited zero with
live tests skipped; the agents did not treat those skips as integration evidence.
The coordinator checked exact published bodies, final labels and tracker operation
logs. These are observations of preparation decisions and local fake effects,
not proof of a real factory environment, deployed enforcement or improved outcomes.

The later testable-scope revision supersedes the blanket external-route gate. Its
cases distinguish approved deferred qualification from a live check retained as
mandatory acceptance and from a missing runner needed to test the implementation.
Fresh host contexts exercised all three boundaries using raw repository files
and local fake tracker tools. With explicit scope approval, a storage-adapter
issue required new automated tests, kept live delivery as visible deferred
qualification, and completed publication plus one observed intake label addition.
When the developer retained live delivery as mandatory acceptance, publication
completed but selection was withheld. An unavailable proprietary runner needed to
verify core semantics likewise prevented selection while approved publication
completed. Exact final bodies, open state, readbacks, one update per issue and
label operations were checked. No product implementation, real external service,
deployed factory execution or outcome improvement was demonstrated.

## Existing-material readiness cases

The `readiness-*` cases include raw tracker snapshots, local skill outputs and
later approval turns. Materialize `readiness_repository_files` as an isolated Git
repository; replace the synthetic base SHA consistently with its actual fixture
commit before replay. Keep the source issue's structure intact. Deliver user turns
only at their stated stage. Withhold `expected` from the executing context and
check the actual artifacts and tracker operation log afterwards.

Exercise a sufficient externally prepared issue, one material ambiguity, a local
internal plan with corrected discussion decisions, approved deferred qualification,
a missing core runner and an active selected source-edit request. Count repeated
questions and body writes, not just final issue headings. The sufficient unchanged
case should produce no body update; the selected active case should produce no
source mutation without observed safe coordination. A supplied CI result or image
configuration is not a live sandbox observation. These fixtures are replay inputs,
not proof that a model or deployed factory executed them.

### Readiness-first observations

Fresh synthetic replays exercised all six readiness cases. A sufficient external
issue retained its exact title/body and settled decisions, received no update and
one approved intake label with readback. An ambiguous existing issue received one
behavioral clarification and stayed local. An internal plan retained its corrected
discussion decision, created one approved issue and remained publish-only.

Further reviewer-followed replays observed approved deferred qualification being
published and selected, a missing core runner with unresolved intent being
published explicitly as a draft without selection, and an active selected issue
receiving no source update or label toggle without observed safe coordination.
The last case exposed an instruction gap; the reference was tightened and the
observed RUNNING state rechecked against that explicit rule. Complete artifacts
and before/after fake-tracker operation logs were inspected. Host fixture tests
passed, but neither those runs nor supplied image/configuration evidence establish
a live sandbox, real tracker permissions or deployed factory admission. The
boundary replays were reviewer-followed and knew their expected effects; they are
not blinded model evaluations or evidence of improved factory completion.
