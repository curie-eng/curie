# Startability observation contract

The optional observer at `observability/startability/observer.py` reads a
snapshot of channel bindings and the resources the worker and dispatcher use.
It reports whether the observed configuration permits the start of each binding.
It does not send a message, call a model, request a sandbox claim, change a
resource, or resolve an approval. It is separate from the default SRE bot bundle
and installer.

The observation cannot establish credential validity, operator pause state,
sandbox availability or a successful claim, provider reachability, execution,
recovery, or end-to-end stability. A positive result means only that the checks
below found no configuration obstacle in the snapshot. Reads are not atomic;
resources can change after observation.

## Acceptance criteria

### STARTABILITY-1: Binding and deployment snapshot

Read `curie.agents` joined to `curie.agent_channels`, using `c.adapter` as the
binding identity. Deployment presence is the worker's active-deployment join:
`curie.deployments` joined to `curie.agent_versions` on version and agent, with
`d.status = 'active'`. Do not add a bundle-reference condition. Read only agent
secret key names, never agent secret values. Observe bound agents only; each
binding is a separate result and a binding with no active deployment fails.
Zero bindings produces a valid zero total, not a claim about unbound agents.

### STARTABILITY-2: Worker judgment

The pure `judge` entry point accepts binding rows, worker environment values,
credential states, pool-to-template names, and template secret-reference names.
Production uses the candidate worker's actual `claim_warm_pool`,
`inject_connector_secrets`, `is_reserved_boot_env_name`, and
`BootEnv.env_key('connector_secret_keys')`. Build the marker from secret key names
with empty placeholders through the actual injection helper, including its
connector-only and sealing-key withholding; never read agent secret values.
Strip whitespace from comma-separated worker pool lists. Preserve an explicitly
blank `CURIE_WARM_POOL` instead of replacing it with the absent-variable default.
After the claim helper chooses the generic pool, match the consumer's discovery
of an existing per-agent pool in the snapshot, using the actual
`agent_warm_pool_name` helper. An absent derived pool preserves the generic
choice; a connector-secret refusal remains a refusal. Never copy the helpers'
routing, derivation, or secret-name policy into the observer. A worker refusal,
missing selected pool, missing selected template, missing or ambiguous named
runner container in the selected template, or missing connector secret reference
fails only affected bindings. Check reference presence only;
do not claim that referenced sandbox Secrets exist or are usable.

### STARTABILITY-3: Dispatcher identity declarations

`identity_lanes` maps the literal or resolved `CURIE_SLACK_IDENTITIES` JSON
declaration's `name`, `app_token_env`, and `bot_token_env` fields. Parse with the
actual protocol `SlackIdentities` parser used by the dispatcher. Absent, blank, and empty-list
declarations use the platform's default `SLACK_APP_TOKEN` and `SLACK_BOT_TOKEN`.
A Slack binding requires both declared lanes. An undeclared
named identity fails rather than deriving an indexed credential name from its
identity. Reject every declaration the actual dispatcher rejects, including
invalid or unbounded identity names, invalid credential variable names, missing default
identity, shared credential variables, extra fields, and duplicate identities.
Non-Slack bindings do not depend on Slack credentials. Retain the optional
legacy identity-name lane derivation for callers that explicitly omit the
`identity_lanes` argument to the pure judge; the live collector always passes
the resolved declaration.

### STARTABILITY-4: Value-free credential observations

`credential_states` reduces the app and bot variables to `(bool, reason)`.
Both literal values and referenced Secret keys must be nonblank. A missing
variable, missing key, blank value, or unreadable credential Secret fails its
lane. Read only credential Secrets referenced by the selected dispatcher and
cache each Secret per observation. Reasons identify the variable/reference
and bounded exception type, never credential values or exception text.
Ignore unrelated dispatcher environment variables and Secrets. Presence is
not an authentication test.

### STARTABILITY-5: Explicit resource scope and image comparison

The command requires all of `--worker-namespace`, `--worker-deployment`,
`--worker-container`, `--dispatcher-namespace`, `--dispatcher-deployment`,
`--dispatcher-container`, `--sandbox-namespace`, `--runner-container`, and
`--check-image`. Values are installation inputs, with no example-installation
defaults. Empty inputs are configuration errors before external reads.
The read-only database DSN comes from nonblank `DATABASE_URL`; accept
`postgresql://` by selecting the asyncpg driver and accept
`postgresql+asyncpg://` directly. Do not print the DSN.

Use in-cluster Kubernetes configuration. Read exactly the specified worker and
dispatcher Deployments and require one matching container in each. Compare
the supplied observer image with the worker container image before collecting
bindings or judging. Missing or unequal image strings fail collection.
An equal string is a necessary comparison, not proof that a mutable tag points
to identical bytes; operators should supply the same immutable worker image.

Read SandboxWarmPools and SandboxTemplates only in the supplied sandbox
namespace, using `extensions.agents.x-k8s.io/v1beta1`. Record whether each
template has exactly one named runner container and read secret references only
from that container. A malformed unselected template does not fail collection.
Resolve relevant explicit
environment `value` and `valueFrom.secretKeyRef` / `configMapKeyRef` entries
in the owning Deployment namespace: worker pool-selection variables and the
dispatcher declaration plus its credential variables. A failure reading
worker configuration or the identity declaration fails collection. Relevant
`envFrom` configuration is unsupported and fails collection rather than
silently using guessed configuration. It is not sufficient to test helpers
without invoking this collector through the command.

### STARTABILITY-6: Structured output and failed collection

Keep the existing JSON-lines schema: one `agent_readiness: 'binding'` object
with `agent`, `kind`, `address`, `identity`, integer `ready` (0 or 1), and
`reason` per binding; then exactly one final `agent_readiness: 'total'` object
with integer `bindings`, `not_ready`, and distinct bound `agents` counts.
A successful collection exits 0 even when `not_ready` is nonzero. Do not emit
secret-name lists or credential values. Buffer results until collection and
judgment complete so an incomplete read cannot produce a success total.

Configuration, image, database, or general Kubernetes collection failure exits
1 and emits exactly one `agent_readiness: 'error'` JSON line, without binding
or total lines. Its `reason` is a fixed, bounded diagnostic and exception type
only. Raw database/SQLAlchemy errors, URLs, passwords, Kubernetes response
bodies, and arbitrary exception text must never appear on stdout or stderr.
Argument usage failures follow the same structured error contract.

### STARTABILITY-7: Pure and command tests

The behavioral tests live at `examples/tests/test_startability_observer.py` in
the existing root collection tree; this example does not change the root
dependency manifest to add a new collection path.


Pin the judgment against real current worker and protocol helpers, without
stubbing or replacing the worker routing implementation. Exercise the actual
command in subprocesses against fixture-backed Kubernetes and database clients.
Cover positive and negative collection, declaration values held in external
references, credential absence/blankness/read failure, scoped resource reads,
image incompatibility, deployment absence, missing pool/template/reference,
missing and duplicate named runner containers for an agent with no connector
secrets,
reserved and connector-only withheld names, empty-list and dispatcher-invalid
declarations, blank worker pool configuration, discovery of an existing
unlisted per-agent pool and its selected template, multiple bindings, and zero
bindings. Test error redaction
with a recognizable dummy value in exception text and the DSN. These boundary
replays prove command wiring, not real Kubernetes/Postgres integration.

### STARTABILITY-8: Runtime evidence gate

Before calling the observer implemented and verified, run its real command
against a disposable namespace and isolated Postgres with the actual candidate
worker image, real resource/Secret reads, and real binding rows. Record candidate
identity, exact command with private inputs kept private, output, and teardown.
Demonstrate a positive binding, a missing/blank credential binding, an
incompatible-image error with no total, and a failed read with no total. Do not
substitute boundary replays for this evidence. No model call or Slack delivery
is required because this observer makes neither. Image publication,
installation, scheduling, metric ingestion, alert delivery, and production
activation require separate evidence and are outside this contract.

## Operator inputs

Run the file inside the candidate worker image with its existing Python
dependencies. Supply the database DSN through an existing secret-management
mechanism; never put it in the argument list or paste it into evidence.
For an anonymous installation, the resource arguments take this form:

```text
python observer.py \
  --worker-namespace acme-platform --worker-deployment acme-worker \
  --worker-container worker \
  --dispatcher-namespace acme-platform --dispatcher-deployment acme-dispatcher \
  --dispatcher-container dispatcher \
  --sandbox-namespace acme-sandboxes --runner-container runner \
  --check-image ghcr.io/acme-corp/worker@sha256:<immutable-digest>
```

Grant only database SELECT and namespaced Kubernetes reads needed for the
selected Deployments, referenced configuration, and sandbox pool/template
resources. The observer needs credential-Secret reads; the SRE bot's general
Kubernetes connector deliberately does not have that grant. Review and provide
the observer identity separately. This document installs no RBAC or scheduler.
