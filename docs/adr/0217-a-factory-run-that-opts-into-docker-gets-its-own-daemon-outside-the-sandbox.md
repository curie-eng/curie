# 217. A factory run that opts into Docker gets its own daemon outside the sandbox

Date: 2026-10-12

Status: Accepted

Accepted 2026-10-12 with explicit maintainer approval from Brian Conn,
given in the design review that produced it, before implementation.

This ADR partially supersedes the rejection of a Docker daemon for factory runs
in [ADR 0176](0176-a-factory-run-may-test-end-to-end-in-a-namespace-it-owns-on-a-separate-cluster.md)
(its "Docker in Docker" and privileged sandbox alternatives). It also partially
amends [ADR 0199](0199-the-factory-admits-only-tickets-it-can-start-and-finish.md)
where it states the runner has no container runtime, for repositories opted in
here. It builds on [ADR 0216](0216-a-factory-repository-binding-selects-its-own-runner-image-built-from-a-tool-catalog.md)
for the per repository binding and the Docker CLI catalog entry. The agent
sandbox envelope of [ADR 0059](0059-sandbox-is-a-bounded-resource-envelope.md)
and [ADR 0054](0054-local-docker-runner-hardening.md) is unchanged. ADR 0176's
hosted end to end connector remains the path for deploy proofs on a test
cluster.

## Context

Many repositories verify with Docker: a compose stack, a container built and
exec'd into, a ladder that runs an image. A factory run cannot run those
checks. Its sandbox runs as non-root with every capability dropped, a read only
root, RuntimeDefault seccomp, and usually gVisor. The runner admission policy
refuses privileged templates.

A spike on 2026-10-12 tried every in-pod option under that envelope.

1. Rootless dockerd and rootless BuildKit both fail to start.
2. BuildKit builds only with seccomp and AppArmor unconfined, default
   capabilities and privilege escalation allowed. Even then it cannot run
   containers.
3. A privileged sidecar would put a model-controlled privileged container inside
   the sandbox pod, and that pod holds the run's credentials.

Probes on k3s showed what a separate per-run daemon pod reached over
`DOCKER_HOST` can do:

1. It gives build, run, exec and compose.
2. It also showed the risks. A privileged daemon let the model run `docker run
   --userns=host --privileged` with full capabilities and write the daemon's
   root filesystem through a host bind mount. With open egress, containers it
   started reached the cluster API server.
3. `--userns-remap` alone did not stop the first two. A small authorization
   plugin did. With default-deny egress, containers could not reach the
   cluster API, the node, other services or cloud metadata, and image pulls
   still worked.

Comparable products give agents Docker either inside a VM or through a
privileged nested daemon. None gives a hardened, non-root pod a daemon of its
own.

## Decision

**When the operator opts a repository into Docker, each of its runs gets a
private Docker daemon in a pod outside the agent sandbox. The daemon runs in a
dedicated namespace on the factory cluster and is reached over mutual TLS. It
lives and dies with the run's sandbox claim. The sandbox itself stays
unprivileged.**

1. **Opt-in per repository, by the operator.** A repository binding sets
   `docker.enabled`. Its runner image must include the Docker CLI catalog entry
   (ADR 0216). Configuration refuses `docker.enabled` without it.
2. **One daemon per sandbox claim.** The worker creates the daemon pod, a
   headless Service, the run's NetworkPolicies and its certificate Secret in
   the daemon namespace just before it creates the claim. It waits for the
   daemon alongside the claim's bind. Each object carries the claim's name and
   the worker's managed-by label. The daemon lives as long as the claim:
   1. A factory work item releases its claim at its terminal state, and the
      daemon goes with it.
   2. An ordinary thread keeps both while its sandbox is retained.
   3. A retained claim is reused only if its daemon answers a ping. Otherwise
      the worker retires the claim and its daemon together and claims fresh,
      so a sandbox is never handed out with a dead Docker endpoint.
3. **Explicit cleanup, reconciled.** The claim and the daemon live in different
   namespaces, so no owner reference ties them together.
   1. The worker deletes the daemon's objects by claim label wherever it
      retires a claim.
   2. The worker's orphan sweep deletes any daemon objects whose claim does not
      exist and that are older than the claim timeout. This also covers a crash
      between creating the daemon and creating the claim.
   3. `activeDeadlineSeconds`, set from the claim's maximum lifetime rather
      than one run's deadline, stops a daemon whose worker is gone until the
      sweep removes its objects.
4. **A hardened privileged daemon.** The daemon container may be privileged. It
   is the only privileged container Curie creates, and only in the daemon
   namespace. It runs with `--userns-remap` and a Curie authorization plugin.
   1. The plugin is an allowlist per API endpoint and field, not a deny list.
      It admits what the Docker CLI, compose and Curie's own CLI use:
      1. ping, version, info and events;
      2. container create, start, stop, kill, restart, wait, attach, resize,
         exec, logs, top, stats, inspect, list, remove, and archive get, put
         and head (what `docker cp` uses);
      3. image pull, build (including the build session), inspect, list, tag
         and remove;
      4. network create on the default bridge driver with no driver options,
         plus network inspect, list, connect, disconnect and remove;
      5. volume create on the local driver with no driver options, plus volume
         inspect, list and remove.
      The authorization plugin's tests derive this list from those consumers.
   2. It refuses everything else. That includes host user, PID, IPC, UTS and
      network namespaces, privileged mode, added capabilities, devices,
      sysctls, host bind mounts, volume or network driver options, unconfined
      security options, privileged exec, and plugin management.
   3. It fails closed on any request body it cannot parse.
5. **Admission enforces the shape.** A new admission policy, separate from the
   runner policy and matching only the daemon namespace, admits only a daemon
   pod that:
   1. uses the chart's pinned daemon and plugin digests;
   2. runs with both flags;
   3. has no ServiceAccount token, host path, host namespace or host port;
   4. has bounded storage and resources.
   The runner policy is not loosened.
6. **Mutual TLS, with a per-run certificate.** The worker mints a certificate
   authority, server certificate and client certificate for each run. The
   client certificate reaches the sandbox through the claim's existing per-run
   Secret. It opens only that run's daemon and dies with it. This is a narrow
   exception to "the sandbox holds no credential", accepted because it grants
   nothing beyond the run's own tool.
7. **Network.**
   1. The run's sandbox may reach its own daemon only.
   2. The daemon accepts traffic only from that sandbox.
   3. Containers the daemon starts get egress on ports 80 and 443 to the
      internet, excluding the cluster, private and metadata ranges. The
      operator may set a per repository allowlist instead.
   4. Opting a repository into Docker therefore widens that repository's
      egress from named services to the public internet.
8. **A remote daemon contract.** `DOCKER_HOST` points at the daemon, and a
   second variable names the host where published ports answer.
   1. Workspace bind mounts do not work: the daemon cannot see the sandbox's
      files. Builds upload their context. Data moves with `docker cp` or named
      volumes.
   2. The runner's tool manifest tells the model this contract when Docker is
      enabled.
   3. Curie's own tooling that assumes a local daemon gains a remote mode. That
      covers the CLI's snapshot transport and endpoint handling, not only
      scripts.

## Consequences

1. Repositories that verify with containers can be finished by the factory.
   Docker-only checks no longer have to be delegated to CI.
2. The install now creates privileged pods. Pod Security on the daemon
   namespace must be `privileged`, so GKE Autopilot and clusters that forbid
   privileged pods cannot offer Docker. A hardened runtime class (sysbox, Kata)
   could later make the daemon unprivileged without changing this contract.
3. A model that escapes the daemon's controls holds root on the node. The
   authorization plugin and user namespace remapping are the controls, and the
   escape tests are part of verification. An operator who wants more isolation
   pins daemon pods to a dedicated, tainted node pool.
4. The sandbox holds one more secret, the per-run client certificate.
5. Each Docker-enabled run costs a second pod and its storage, inside the
   daemon namespace's quota.
6. A repository opted into Docker can reach the public internet from its
   containers. Data in the sandbox, including private source, can leave by
   that path. Labelling a ticket already needs write access, so the realistic
   vector is injected issue or repository text.
7. A late workspace handoff (ADR 0136) or suspend replaces the claim, and with
   it the daemon and any running containers.

## Alternatives considered

### A daemon inside the sandbox pod

Rejected. Rootless shapes fail under the envelope, and the shapes that work
need the envelope relaxed for the pod that holds the run's credentials.

### Rootless BuildKit or Kaniko only

Rejected. They build images but cannot run containers or compose, which is
what repository checks need.

### Kubernetes user namespaces for an in-pod daemon

Deferred. Not testable on the reference k3s node, and it still needs root
inside the namespace. It could replace the privileged daemon later.

### ADR 0176's hosted connector as the Docker path

Rejected for this need. It offers deploy, run-a-Job and logs on a separate
cluster, not a Docker API. It has no exec and no compose, and it cannot build
an unpublished commit. It stays the deploy proof path.

### The separate test cluster as the daemon's home

Rejected as the default. It would need a second cluster for every install that
wants Docker. An operator can isolate daemons with a node pool instead.

### Registry-only egress through a mirror

Rejected as the default. It breaks every Dockerfile step that fetches packages
and requires the operator to run a mirror. The per repository allowlist covers
installs that need it.

## Implementation

Acceptance authorizes implementation in follow-up pull requests into `next`.
No code realizes this decision yet.

Note: The ping transport is an API port-forward using the claim certificate.
Maintainer approval 2026-10-21.
