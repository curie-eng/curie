# 204. The factory checks its environment before it admits a ticket

Date: 2026-10-06

Status: Draft

This Draft proposes to amend [ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
(Accepted). It adds an environment half to that ADR's "can it start" question
and routes an environment failure to its internal `unknown` verdict, never to a
ticket rejection. Everything else in ADR 0199 stands. If accepted, ADR 0199
gains the back link that
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
allows.

## Context

ADR 0199 gates admission on the ticket: whether its blockers merged, its paths
exist, its decisions are made, and its criteria are verifiable with what the
runner capability manifest declares. It assumes the environment the manifest
describes is actually working. Nothing checks that.

A staging round on 2026-10-05 and 2026-10-06 ran three well formed, small
tickets through a v0.12.1 installation. All three were written to the ADR 0199
shape. None ended in a passing run, and none of the causes were in the ticket:

1. **Model credit.** The provider key had reached its own spend limit. The
   provider answered HTTP 403 "Key limit exceeded", every run failed at its
   first model call, and Curie reported a rejected credential.
2. **Toolchain.** The dark factory runner image ships Cargo without rustfmt or
   clippy. `examples/dark-factory/runner.Dockerfile` copies a toolchain
   installed with the minimal profile. A run published a PR whose only red
   check was formatting, and its CI fix round could not run `cargo fmt`.
3. **Registry egress.** `agentSandbox.registryEgress` lists CIDR ranges,
   because a NetworkPolicy cannot name a host (`charts/curie/values.yaml`). The
   installation's list covered two of the CDN's ranges, the registry resolved
   to a third, and a run could not fetch the locked Cargo graph, so a
   criterion's verification could not run.
4. **Base branch health.** The base branch's scheduled dependency audit had
   been red for hours on an advisory already fixed on the other release line.
   Every PR into it inherited two failing required checks.
5. **Sandbox claims.** A CI fix round failed to claim a sandbox three times
   (`ClaimTimeoutError`, pod Running but not Ready), and the run escalated.

Each failure was found only after a sandbox was claimed and the repository
cloned, and most only after a full implementation. Together the runs reported
at least $4.18 to $4.52 each, against about $1 for comparable runs on
2026-10-01. Per model usage shows why: the implementer read 16 to 17 million
input tokens per run, about three times the earlier average, much of it CI fix
rounds and retries spent on these failures.

Every one of these conditions was true before the ticket was labelled, was
shared by every ticket the installation would admit, and was cheap to observe.
ADR 0199 decision 6 already defines the right disposition for a condition that
is not the ticket's fault: an internal `unknown` that creates no WorkItem,
blames no one on the issue, retries with backoff, and alerts the operator.

## Decision

**Admission checks the environment before the ticket. An environment check
that fails or cannot be read makes the verdict `unknown` under ADR 0199
decision 6, with a reason that names the check. It is never a rejection, it
never labels or comments on the issue, and it never spends a sandbox.**

### 1. Environment checks run first and are shared across tickets

1. The environment checks run inside the ADR 0199 gate, on the fresh admission
   branch, after base resolution and before the ticket's start checks.
2. Their results are per agent and per base, not per ticket. The gate caches
   each result with its observation time and reuses it for every ticket in the
   same intake pass. A pass with fifty labelled tickets reads the environment
   once.
3. An environment failure stops evaluation for every ticket on that agent and
   base. No ticket is classified, and no model classification call is made.

### 2. The four environment checks

1. **Model credit.** The agent's model credential has at least the
   deployment's minimum remaining credit, default 10 USD. For a provider whose
   API reports a key's remaining limit and the account balance (OpenRouter's
   key endpoint does both), both must be at least the minimum. For a provider
   with no such endpoint, the check is recorded as not applicable and passes.
2. **Base branch health.** No required check on the resolved base commit has a
   failing conclusion, read from the code host through the CodeHost port (ADR
   0197). Pending required checks pass. A base whose required checks fail
   would fail every PR built on it.
3. **Runner toolchain.** The runner capability manifest (ADR 0199 decision 3)
   lists every executable the repository's verification declaration routes to
   the sandbox, including formatters and linters such as `cargo fmt` and
   `cargo clippy`. `curie build` fails when probing the built image for a
   declared executable fails, so a manifest can never claim a tool the image
   lacks, and a bundle that declares a check whose tool is missing does not
   build.
4. **Sandbox canary.** The agent has a passing canary result younger than 60
   minutes (decision 3).

### 3. The sandbox canary

1. The platform runs one canary per factory agent every 30 minutes and once
   after each deploy of that agent.
2. A canary claims a sandbox from the agent's own template, so it uses the
   agent's image, resources, quota, and NetworkPolicies. It runs no model.
3. It records pass only if all of these hold:
   1. the claim binds within the runner's normal claim timeout;
   2. every manifest executable runs its version command and exits 0;
   3. a TLS connection succeeds to every registry host the manifest declares.
      The bundle declares the hosts its build needs (for the dark factory:
      `index.crates.io`, `static.crates.io`, `pypi.org`,
      `files.pythonhosted.org`, `registry.npmjs.org`), and `curie build`
      writes them into the manifest.
4. It releases the sandbox and stores the result, its observation time, the
   failing sub-check, and the claim's pod events when the claim did not bind.
   Keeping those events is deliberate: a claim timeout's own message has
   misdescribed its cause before, and the pod is usually gone by the time
   anyone looks.

### 4. Failure handling and visibility

1. An environment failure yields `unknown` with reason codes
   `env_model_credit`, `env_base_red`, `env_toolchain`, `env_canary_claim`,
   `env_canary_tool`, or `env_canary_egress`, plus the failing evidence (the
   remaining credit, the red check names and base commit, the missing
   executable, or the unreachable host).
2. As ADR 0199 decision 6 requires, the issue gets no label and no comment, and
   intake retries with capped exponential backoff. The canary schedule is
   independent of intake backoff, so a repaired environment is noticed within
   30 minutes.
3. The operator sees the reason in the admission read in the CLI and in a
   metric. An environment failure older than the deployment's alert threshold
   raises the ADR 0199 operator alert once per reason.
4. A run that fails after a passing environment check, for a cause the checks
   cover, records an environment miss against the check, alongside ADR 0199's
   gate miss metric.

### 5. What does not change

1. ADR 0199's ticket checks, model call, rejection outcome, fingerprinting,
   and re-admission rules are unchanged. A ticket held for its environment is
   re-evaluated on the next intake pass after the environment passes, with no
   edit or relabel, because it was never rejected.
2. In-run handling of a check that turns red on the base after admission is not
   decided here.
3. The runtime preflight block (`runner/src/curie_runner/verification.py`)
   stays the fallback for drift after admission.

## Consequences

1. A broken environment stops intake at the cost of cached reads and one
   sandbox claim per agent every 30 minutes, instead of a claimed sandbox,
   a clone, and up to a full implementation per labelled ticket.
2. Run failures for environment causes leave the run statistics, so a failed
   run means a run problem, and the cost per run reflects real work.
3. The operator gets one alert naming the cause, instead of a needs-human card
   per ticket that has to be read to find it.
4. A red base branch halts factory admission on that base until it is fixed.
   That is intended: no PR built on it could pass its required checks.
5. The canary adds a scheduled sandbox claim per factory agent and a result
   table. On a small cluster it competes with runs for quota; it claims at most
   one sandbox per agent at a time.
6. Bundles gain a registry host declaration, and `curie build` probes declared
   executables and hosts. A bundle that declares a check whose tool is missing
   stops building, which surfaces the rustfmt class of failure at build time.

## Alternatives considered

### Reject the ticket on an environment failure

Rejected. ADR 0199 re-admits a rejected ticket only on an edit or a relabel. An
environment failure is not the ticket's fault, so a rejection would put a
misleading reason on every labelled issue and force a person to touch each one
after the operator fixed one thing.

### Check the environment inside the run

The runner already blocks on a failed boot preflight. Rejected as the primary
check. It costs a sandbox claim, an image pull, and a clone per ticket, ends as
a failed run, and cannot see model credit or base health before the model has
already been called.

### Fix each paper cut and add no check

Rejected as sufficient, though each is being fixed. The failures were five
different causes in one round. With enough independent conditions, some
condition is always false. The value is a single place that proves the
environment works before money is spent.

### Probe registry egress from the API

Rejected. The API pod runs under different NetworkPolicies than the runner, so
an API side probe proves nothing about the sandbox. Only a claim from the
agent's own template tests the path a run uses.

## Implementation

Not authorized while Draft. On acceptance, the work splits into: the four
environment checks and their reason codes in the ADR 0199 gate; the canary
scheduler, result table, and pod event capture; the manifest's registry host
declaration and `curie build` probes; and the CLI admission read, metric, and
alert for environment reasons.
