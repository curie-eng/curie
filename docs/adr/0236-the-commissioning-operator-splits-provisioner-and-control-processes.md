# 236. The commissioning operator splits provisioner and control processes

Date: 2026-10-17

Status: Draft

Tracked in [#4557](https://github.com/curie-eng/curie/issues/4557).
This realizes the operator qualification command required by
[ADR 0224](0224-protected-runtime-commissioning-precedes-installation-acceptance.md).
It does not change the commissioning record grammar or authorize
implementation while Draft.

## Context

ADR 0224 requires one operator-facing command to own provisioning, a bounded
campaign, evidence capture, revocation, cleanup and an independent decision.
The runtime commissioning specification (`PROTECTED-COMMISSIONING-5`) adds that
the launch server has no production composition, that every required
observation must pass, and that Installation A cannot qualify Installation B.

The preventive measurements come from two places that cannot share a process.

- **In-cluster producers.** These are the control admission server, the
  primitive probe registry, the scripted SDK primitive, the retained-peer probe,
  the campaign registry, the mediator and the issuer session. They must hold the
  first-owner TLS transports and the registry rows of the workloads they launch.
- **Provisioner producers.** These are the control observer, the effective-RBAC
  and CNI observations, the network and hostile-peer probes, and Node/control-Pod
  identity. They need credentials and node access that no in-cluster control
  principal should hold.

`CampaignGuard` must re-observe both sets on every call. A fixture
implementation or a stored success row cannot stand in for that.

## Decision

### Two processes, one signed observation channel

The operator runs as a **provisioner process** and a **control process**.

The provisioner process:
- creates the installation from its pinned profile, projection and images;
- registers ownership of every resource before it is created;
- holds the provisioner signing key that the control profile pins;
- keeps its own producers in memory.

The control process runs in the owned control Pod. It keeps the in-cluster
producers in memory and constructs the `CampaignGuard`.

On each guard call the control process:
1. Creates an immutable request object in the control namespace. The object
   carries a fresh nonce and exact digests of the permit, manifest, bootstrap,
   control profile, projection, primitive completions, network, hostile and
   retained-peer facts, installation, CNI and artifact.
2. Waits for a reply object.
3. Accepts the reply only if it carries a valid signature over exactly that
   content plus the request object's UID.

The provisioner signs a reply only after fresh reobservation by its own
producers. A stale reply, a reply for another request or a different
installation, or a changed binding cannot verify. The signing key never enters
the control Pod.

### The decision is the provisioner's, from this installation only

The independent verifier runs in the provisioner process. It accepts only
when all of the following hold:
- every required case in `PROTECTED-COMMISSIONING-5` has a passing observation
  from this installation's own producers or from signed channel replies bound
  to this installation;
- the final tuple equals the premeasured one, covering the manifest, broker
  session, guard, bootstrap, images and execution digest;
- revocation has been observed;
- the original-workload termination and absence have been observed.

Any missing, unavailable, skipped, timed-out or fake-model observation refuses.
Only after acceptance does the verifier construct the existing Qualification and
Readiness publishers, whose credentials are distinct from the issuer's.

### Network producers are selected per CNI

The provisioner measures preventive network enforcement with a producer for the
installation's actual CNI. The local Cilium producer is the first one.

An installation whose CNI has no producer refuses before the campaign starts. A
producer for a managed cloud CNI requires its own realization, including:
- source-identity and spoof measurement;
- retained-connection refusal after the original workload is replaced.

Until such a producer exists, the command qualifies only disposable local
installations.

### Command surface

The first command is a `curie dev` subcommand for disposable local
installations, because it needs a source checkout, a local container runtime
and a local cluster. A `curie cluster` form follows once a production CNI
producer exists. Neither form accepts a caller-supplied observation, success
record or phase flag.

## Consequences

The proven local composition becomes source code rather than private test
drivers. A qualifying campaign stays installation-bound, because the request UID
and installation digests are part of every signed reply.

Guard latency is one provisioner round trip per call. Each round trip includes
effective-RBAC reobservation, measured at about one to two seconds locally.
Callers that poll the guard must budget for that latency.

Live qualification remains blocked until a producer exists for the target CNI.
That outcome is explicit rather than hidden.

## Alternatives rejected

- **A single in-cluster process.** The control Pod would have to hold provisioner
  credentials and the signing key and attest its own observations, so the
  verifier would not be independent of the process the campaign runs in.
- **A provisioner-only process using remote calls.** It cannot keep the
  first-owner TLS transports and launch registry rows that the primitive and
  peer measurements depend on.
- **Importing an evidence file or a success record.** The specification forbids
  caller-supplied successful observations, and a file cannot prove freshness.
- **Caching guard results between calls.** A cached result is stale evidence and
  violates per-call reobservation.
- **Treating local Cilium measurement as cloud CNI evidence.** Different
  enforcement engines can fail differently, and one installation cannot qualify
  another.
