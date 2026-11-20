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

This bundle runs without its reproduction gate until the bundle is removed.
The gate, its tests and the policy replay evals moved to the default factory
bundle, [`examples/dark-factory`](../dark-factory/README.md), whose
`investigate` phase now carries the investigation guidance. Nothing here
checks that a submitted ticket quotes an observed command; review a ticket
before selecting it for a separate implementation run.

## Verification

```bash
uv run pytest examples/tests/test_factory_triage_bundle.py -q
```

The bundle test validates the bundle and its result declaration through the
deployed artifact readers. Live provider and factory publication proof are
separate requirements.
