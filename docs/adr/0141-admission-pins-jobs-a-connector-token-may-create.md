# 141. Admission pins the Jobs a connector token may create

Date: 2026-09-03

Status: Draft

Implements [#2175](https://github.com/curie-eng/curie/issues/2175). Closes the
escalation [PR #2163](https://github.com/curie-eng/curie/pull/2163) disclosed:
a leaked `sre-bot-upgrader` token can select the `curie-platform-upgrader`
ServiceAccount. [#2122](https://github.com/curie-eng/curie/pull/2122)'s claim
that the platform-upgrade credential is isolated to a short-lived Job rests on
this hole being closed.

This Draft does not authorize implementation
([ADR-0085](0085-acceptance-not-implementation-authorizes-an-adr.md), as
amended by [ADR-0102](0102-accepted-alongside-implementation-with-explicit-approval.md)).
The YAML under `examples/sre-bot/manifests/upgrade-job-admission.yaml` is a
sketch of the realizing path, not an applied control.

## Context

`upgrade_platform` is a zero-argument connector tool that creates a Job from an
operator-written, suspended CronJob. The Job runs as `curie-platform-upgrader`,
whose Role is namespace-admin in all but name: it must, because `helm upgrade`
rewrites the release's Secrets, workloads, and RBAC. The connector identity
(`sre-bot-upgrader`) is deliberately weaker. It may `get` two named CronJobs
and `create`/`list` Jobs. It does not hold the Helm credential. The sandbox
never sees either kubeconfig.

That isolation is real on the connector surface and false at the Kubernetes
API. `create` on `jobs` cannot take `resourceNames`: a create has no name yet.
RBAC also does not restrict `spec.template.spec.serviceAccountName`. A holder
of the connector's long-lived token can POST an arbitrary Job whose pod runs
as `curie-platform-upgrader` with an arbitrary image and command. The
connector cannot construct that body; a token holder bypasses the connector.

PR #2163 documented the path and listed three deployment-side options:
constrain ServiceAccount choice with admission, separate the identities by
namespace, or stop using a long-lived connector token. None of those is
installed. `curie example sre-bot install --platform-upgrade` still applies
both Roles together.

A fourth option, "do not install the upgrade connector", remains the
fail-closed default (`PLATFORM_UPGRADE_CRONJOB` empty, CronJob and identity
absent). It is not a mitigation of an install that has already chosen to
enable the path.

## Attack path

1. An operator installs `manifests/upgrade-role.yaml` beside
   `manifests/platform-upgrade-role.yaml` in the release namespace (the
   `--platform-upgrade` installer path).
2. The self-upgrade connector pod holds a static kubeconfig for
   `sre-bot-upgrader` (`sre-bot-upgrader-token`). That token does not expire
   with the pod.
3. The token leaks: file mount, process dump, a future tool that reads the
   kubeconfig, backup of the Secret, or any other path that copies a bearer
   token out of the connector container.
4. The holder POSTs a Job in the release namespace with
   `spec.template.spec.serviceAccountName: curie-platform-upgrader` and a
   container image and command they chose.
5. The API server authorizes the create against the connector Role. Nothing
   in RBAC asks which ServiceAccount the Job selected.
6. The Job's pod receives a projected `curie-platform-upgrader` token and
   inherits that account's namespace-wide powers, including `get` on every
   Secret in the release namespace (the platform API key and the model
   credential among them) and rewrite of every workload the release owns.

The same Job-create grant can also mount arbitrary Secrets via `secretKeyRef`
on a pod that uses the default ServiceAccount. That is the older, already
documented blast radius of namespace-wide Job create. Selecting
`curie-platform-upgrader` is worse: it adds Helm-level rewrite, not only
secret theft.

## Decision

**The ceiling Kubernetes RBAC cannot express is enforced by admission.** A
request authenticated as the upgrade-connector ServiceAccount may create a
Job only when that Job is a verbatim instantiation of a CronJob the same
identity is already allowed to `get`. "Verbatim" means the security-critical
fields of the pod spec -- ServiceAccount, containers (name, image, command,
args, env, volumeMounts), volumes, host namespaces, and initContainers --
equal the live CronJob's `jobTemplate`. The live CronJob is the source of
truth, fetched as the admission policy's `params`; the policy does not
duplicate those fields.

**ServiceAccount-choice-only is not this control.** An allowlist that
includes `curie-platform-upgrader` still lets the leaked token run an
arbitrary image as that account. An allowlist that excludes it breaks the
legitimate `upgrade_platform` path, whose template *does* select that
account. The pin is the whole template, not the name of the account.

**Installing both identities without this admission (or an equivalent
admission control that enforces the same field pin) is forbidden.** The
fail-closed alternative is to leave `PLATFORM_UPGRADE_CRONJOB` empty and not
apply `platform-upgrade-role.yaml`. Kubernetes 1.30 is the floor for the
in-tree `ValidatingAdmissionPolicy` sketch; on a cluster that cannot admit
that API, the equivalent is some other admission plugin that enforces the
same comparison, not a skip.

**The realizing sketch is
`examples/sre-bot/manifests/upgrade-job-admission.yaml`.** It is not wired
into `curie example sre-bot install` and must not be applied from this Draft.
Once this ADR is Accepted, the installer path that applies both Roles must
apply this policy in the same step, or refuse.

## Consequences

- [#2122](https://github.com/curie-eng/curie/pull/2122)'s isolation claim
  becomes true at the API, not only at the connector: a leaked connector
  token can still create Jobs, but only Jobs that already exist as operator
  templates.
- Editing a CronJob template (image tag, env, command) updates what admission
  will accept, because the policy reads the live object. Drift between a
  hard-coded allowlist and the CronJob cannot reopen the hole or block a
  legitimate start.
- The connector's Python "copy the template verbatim" path stays defense in
  depth for the MCP surface. It is no longer the only ceiling.
- Operators on Kubernetes < 1.30, or with `ValidatingAdmissionPolicy`
  disabled, cannot enable `--platform-upgrade` until they provide an
  equivalent control. That is a documented incompatibility, not a silent
  fail-open.
- A future "button" connector that creates Jobs in a namespace that also
  holds a more privileged ServiceAccount is the same decision: admit by
  template pin, or do not give that connector Job create.

## Alternatives considered

1. **Admission that only constrains `serviceAccountName`.** Rejected as a
   false mitigation. Allowing `curie-platform-upgrader` preserves the
   escalation (arbitrary command as that account). Denying it breaks the
   legitimate Job. PR #2163 named this option; it is not sufficient.

2. **Separate namespaces.** Rejected as the primary control. A Job can only
   use a ServiceAccount in its own namespace, so the legitimate upgrade Job
   must be created in the release namespace where `curie-platform-upgrader`
   lives. Moving Job create out of that namespace requires a new trigger
   (a controller, a CR, or a cross-namespace impersonation) and is a
   different architecture. It remains available as a later, larger change
   if admission proves too brittle.

3. **No long-lived connector token** (projected, bound ServiceAccount
   tokens; drop the static kubeconfig Secret). Rejected as the primary
   control. A projected token is still valid for the life of the connector
   Deployment. It bounds persistence after the pod is deleted; it does not
   stop the escalation while the connector is running. Worth doing later as
   defense in depth. Out of scope here, and this Draft does not change
   credential handling.

4. **A Job-factory controller** that holds Job create, while the connector
   only patches a namespaced trigger the controller watches. Correct by
   construction, and closer to [ADR-0007](0007-adopt-not-build-boundaries.md)'s
   "do not build" line being crossed than in-tree admission is. Rejected for
   this decision: we do not add a controller to express a constraint
   `ValidatingAdmissionPolicy` already can. Revisit if the template-pin
   policy cannot be made correct under API defaulting.

5. **Grant the connector `update` on the named CronJobs** and unsuspend or
   rewrite the schedule instead of creating a Job. Rejected: `update` on the
   CronJob includes rewriting `jobTemplate`, which is a wider grant than Job
   create.

6. **Leave the hole documented and unmitigated.** Rejected. #2122's
   credential-isolation claim is load-bearing for enabling platform upgrade
   at all. A documented hole is not isolation.

## What this deliberately does not cover

- **The platform-upgrader identity while a legitimate Job runs.** A human
  who approved `upgrade_platform` still starts a ninety-second
  namespace-admin Job. Admission does not shrink that Role. Recovery from a
  migrating upgrade remains restore-from-backup, not `helm rollback`.
- **Prompt injection against the zero-argument MCP tool.** The connector
  still copies the CronJob verbatim; approval still gates who may press the
  button. This ADR is about a leaked token bypassing that surface.
- **Other Job- or Pod-creating identities in the release namespace**
  (cluster-admin, Helm itself, a mis-scoped general Kubernetes connector).
  The policy matches `system:serviceaccount:<ns>:sre-bot-upgrader` only.
- **Clusters that cannot run the in-tree policy.** They do not get a weaker
  substitute that fail-opens. They keep the path uninstalled.
- **The static kubeconfigs of the other sre-bot connectors**, and rotating
  or projecting `sre-bot-upgrader-token`. Credential handling does not
  change in this Draft.
- **RBAC on `jobs/create`.** There is still no `resourceNames` for a create.
  This ADR does not pretend otherwise.
- **Applying the sketch to any cluster, and wiring it into the example
  installer.** Those are implementation, and implementation waits on
  acceptance.
