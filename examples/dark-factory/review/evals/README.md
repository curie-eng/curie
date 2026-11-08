# PR review workflow replays

Use `replay-cases.json` with a fresh host agent loaded with the installed skill and both
references. Materialize raw synthetic PR files, diff, linked issue, CI state and
repository instructions. Expose fake authenticated host tools backed by local
state, logging every read, test execution, comment, review, merge and issue update.
Supply the case's user turns and changing state without the grading expectations.
Do not permit real network writes or give the model a successful transcript.

Assess the actual walkthrough, commands, observed results and recorded tool
effects against each case. Test scenarios should refer to real fixture assertions;
instruction text matching alone does not establish behavior. A merge must enforce
the supplied head precondition. Inject write timeouts after persistence and a head
change between initial inspection and publication to exercise reconciliation.

These are opt-in host-agent evaluations, not `curie skill eval` suites or automated CI tests. Record the skill
revision, host, raw fixture, operation log and final observations for each replay.
Synthetic receipts establish decisions and fake effects only; they do not prove
real client discovery, tracker permissions, factory admission or branch protection.
