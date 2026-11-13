# Factory triage

This example investigates a rough report on the base the platform resolved for
its source issue. It executes a reproduction and submits one tracker result:
a linked implementation ticket for a matching reproduced defect with settled
intent, or findings explaining why no ticket can be proposed. It implements no
fix, opens no pull request and holds no tracker credential.

Render a bundle with an explicit implementation target:

```bash
curie example factory-triage render --out ./triage-bundle --implementation-label curie-factory
```

The output directory must be new or empty. `--implementation-label` is required;
there is no default implementation target. `--json` emits one result object.
The committed declaration uses `curie-factory` only as an example. Choose a
different agent's implementation trigger label on the same repository, distinct
from this triage agent's label. Publication records the target but does not apply
that trigger label or start implementation.

Add the rendered bundle to an existing install:

```bash
curie factory quickstart --agent acme-triage --label acme-triage-intake --plugin-dir ./triage-bundle
```

Quickstart uses the existing install and its factory code host. This bundle uses
the dark factory runner toolchain and adds no runner image family. Supply the
same built runner layer used by an implementation agent when configuring the
install. Rendering itself is offline and touches no cluster.

The default execution limits are 1800 seconds, 5 USD per model session and one
session, with no continuation. Configure the factory selection accordingly; the
bundle honors the execution deadline supplied by the platform. Model budget
limits are platform settings, not enforced by the example's prompt.

## Reproduction gate

The shell recorder stores session observations in the workspace under
`.curie/factory-triage`. Each completed command records its exit status, up to
32 KiB of output, checkout commit and tracked-file cleanliness. The last 128
commands are retained. The gate checks a ticket against that evidence and the
checkout at submission. Its observed result must quote at least one recorded
nonempty output line verbatim. Untracked probe files are allowed; tracked source
patches must be reverted before the baseline probe and before submission.

The hook refuses unsupported tickets, protected-path edits, direct publication,
background reproduction and unknown mutation tools. Its file lock covers both
checking and saving evidence. Invalid input, unreadable state, internal exceptions
and an eight-second deadline produce a deny decision with successful hook exit,
because the runner treats hook errors and timeouts as nonblocking.

Findings remain available when reproduction state is missing or corrupt. The
submitted reasons are `not_reproduced`, `already_fixed`,
`product_decision_needed` and `capability_unavailable`. A passing probe alone
never establishes `already_fixed`; an unavailable service never establishes a
product failure. Reports cannot authorize credential exposure or bundle edits.

The hook verifies that a command and quoted output were observed. Matching the
report, resolving intent and writing a viable implementation specification are
agent judgments, exercised by graded evals. Publication of a ticket is not a
platform certification that it is viable. Review the ticket before selecting it
for a separate implementation run.

## Verification

```bash
uv run pytest examples/tests/test_factory_triage_bundle.py examples/tests/test_factory_triage_hooks.py examples/tests/test_factory_triage_evals.py -q
```

Eval inputs include issue, repository and diff evidence inline, so they can run
without a checkout or issue tool. Candidate and evidence-removal control replies
are recorded with prompt hashes; fixed counterexamples test grader sensitivity.
Offline hook tests drive the shipped configuration against a real temporary git
checkout. Live provider and factory publication proof are separate requirements.
