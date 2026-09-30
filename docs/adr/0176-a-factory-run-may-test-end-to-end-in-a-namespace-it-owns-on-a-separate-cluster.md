# 176. A factory run may test end to end in a namespace it owns on a separate cluster

Date: 2026-09-25

Status: Accepted

Accepted 2026-09-25 with explicit maintainer approval from Brian Conn, recorded
on the publishing pull request.

This ADR builds on [ADR 0086](0086-bundles-declare-connectors-the-platform-hosts-them.md)
(bundles declare connectors, the platform hosts them),
[ADR 0075](0075-the-agent-proxy-credential-and-egress-boundary.md) (credentials
and egress leave the sandbox), [ADR 0059](0059-sandbox-is-a-bounded-resource-envelope.md)
(the sandbox is a bounded resource envelope),
[ADR 0171](0171-a-factory-run-may-take-three-hours-and-is-bounded-by-time-not-turns.md)
(a factory run is bounded by time) and
[ADR 0173](0173-a-bundle-may-layer-its-own-runner-image.md) (a bundle may layer
its own runner image). It relies on the owner and consumer split proposed in
Draft [ADR 0129](0129-one-release-owns-a-clusters-shared-singletons.md). It
supersedes nothing. It records a direction; nothing implements it yet.

## Context

The dark factory (`examples/dark-factory`) runs coding work in an agent sandbox
pod. That pod runs as non root, drops every capability, denies privilege
escalation and uses the `RuntimeDefault` seccomp profile. It has no Docker
daemon. Its egress reaches the model providers and the GitHub API and nothing
else, and no in cluster service is provided for it to test against.

Dogfooding v0.10.0 on staging on 2026-09-25 took a real issue (#3013) through
the factory to a pull request (#3173). It showed that the agent can run unit
tests and nothing more. The kernel tests that need Postgres and Valkey, a
compose stack, a kind cluster and the parity ladders all fail to start. #2901
records the same gap for the workspace sandbox in general, and #3083 tracked
the package registry egress half of it.

Today the only end to end signal a factory run gets is the post publication CI
gate (`apps/api/src/curie_api/factory_ci.py`, `gate`). It reports only after
the agent believes it is done and has published. Each round costs 20 to 40
minutes of wall clock, and the wait itself is a fixed 1200 seconds that is
shorter than many repositories' CI (#3162). Three rounds is the cap. A factory
that learns about a broken deploy only from CI spends most of its three hour
budget (ADR 0171) waiting.

Maintainers do not work this way. Before they publish, they run the stack
locally and on real clusters, in a namespace they create and tear down. A
software factory needs the agent to do the same.

The sandbox cannot simply be given the means to do it:

- **The sandbox is not a credential boundary.** On 2026-08-20 a remote dev
  test cloned with a tokened URL. Git persisted the token in `.git/config`, and
  the model read it and used it to push a branch and open a pull request on its
  own. Separately, #2525 shows that a tool deny list leaves the state
  credential readable from a shell tool. Anything placed in the sandbox is
  available to the model.
- **The sandbox is a bounded envelope** (ADR 0059). It is sized for one agent,
  not for a Postgres, a Valkey, an API, a worker and a nested sandbox.
- **The factory's own cluster is already short of room.** #3169 and #2949
  record factory work that the quota admits but no node can schedule.

ADR 0086 already names the pattern that fits: the credential lives in a
platform hosted connector and the sandbox holds none. It calls itself the
substrate for scoped Kubernetes access, which #1096 left as an open design
question. This ADR answers that question for end to end testing.

## Decision

**A factory bundle may declare a platform hosted end to end connector. The
connector, not the sandbox, holds the credential for a separate test cluster.
Each run gets a fresh namespace there that the run owns, that the platform
reaps, and that is bounded like a sandbox. Post publication CI stays as the
independent check.**

1. **A declared, platform hosted connector.** A factory bundle declares the
   end to end connector in its connectors file, as ADR 0086 provides. It gives
   the run tools with capabilities such as `env_create`, `image_build`,
   `deploy`, `run`, `logs` and `events`, and `env_destroy`. These names are
   illustrative. This ADR fixes the capabilities and the boundaries below, not
   the tool schema, which belongs to the implementation issue.
2. **The credential stays in the connector.** The test cluster credential and
   the registry push credential live only in the hosted connector, never in the
   sandbox. The model gets tools, not a kubeconfig. This is the ADR 0075
   principle applied to a new credential, and it holds for the reasons in the
   Context: whatever the sandbox holds, the model can use.
3. **A separate test cluster, a fresh namespace per run.** End to end work runs
   on a test cluster that is not the cluster running the factory, so its load
   cannot starve the factory or other agents. Each environment is a fresh
   namespace owned by one run and labelled with the run and work item ids. The
   connector creates it with a `ResourceQuota`, a `LimitRange`, a default deny
   `NetworkPolicy` plus only the rules the test needs, and a TTL.
4. **The connector acts only in what it created; the platform reaps.**
   - The connector's identity can create namespaces with its own name prefix
     and label, and can act only inside namespaces carrying that label and
     prefix. It holds no cluster admin grant and no cluster scoped write.
   - Teardown does not depend on the agent calling `env_destroy`. A platform
     reaper deletes every environment whose TTL has passed or whose run has
     reached a terminal state. It deletes the namespaced objects that stall a
     namespace delete (sandbox claims, jobs, persistent volume claims) before
     the namespace itself.
5. **Cluster scoped objects are a shared layer. A change to one falls back to
   CI only, and the pull request says so.**
   - The CRDs, the PriorityClasses and the agent sandbox controller are
     installed on the test cluster once, by its owner release, and each run's
     namespace is a consumer of them in the sense of ADR 0129.
   - A run cannot change that layer. When a deploy's manifests contain a
     cluster scoped object, the connector refuses it with a named reason rather
     than applying it or silently skipping it.
   - The run then publishes with the post publication CI gate as its only end
     to end signal, and the pull request body states that the change alters a
     cluster scoped object and was not proven end to end in the run.
   - A dedicated ephemeral cluster per run is the recorded alternative for this
     case (see Alternatives). It is left for a later ADR once there is evidence
     of how often factory work touches the shared layer.
6. **Images are built without Docker in the sandbox.** `image_build` builds from
   the run's workspace commit, using a daemonless builder (such as BuildKit or
   kaniko) running as a job in the run's namespace on the test cluster. It
   pushes to a registry the connector holds the credential for, and returns
   digests. Deploys name images by digest, never by tag.
7. **No room means wait, inside the run's own deadline.**
   - When the test cluster has no room, `env_create` defers and the run waits.
     It does not fail. This follows the rule that work without room waits in
     the queue (#3169).
   - Time spent waiting for and using an environment counts against the run's
     execution deadline (ADR 0171). No new clock is added.
   - Each installation sets a cap on concurrent environments. The cap is the
     first admission check, before any cluster capacity check.
8. **The CI gate stays.** In run end to end testing happens before
   publication. The post publication CI gate remains the independent check,
   and its verdict still decides whether the request completes.

## Consequences

- The test cluster carries real load and real cost. Operators size it, and the
  concurrency cap in decision 7 is what keeps it from being oversubscribed.
- Image builds will dominate environment time. Build caching on the test
  cluster matters more than any other tuning.
- Every run pushes images. The registry needs a retention and garbage
  collection policy keyed on the same run labels the reaper uses.
- The reaper is a new platform component with its own failure mode: a reaper
  that stops running leaks namespaces and quota. It needs its own health signal
  and alert, named in the implementation issue (#3245).
- The connector's identity is a new privileged principal. Its role, its
  namespace prefix rule and its refusal of cluster scoped objects need a
  security review before the connector ships (#3243).
- An installation without a test cluster configured has no end to end
  connector. Its factory runs behave as they do today, with CI as the only
  end to end signal.
- The runner side tooling a factory needs to drive these tools (uv, pnpm, a Rust
  toolchain, the repository's test runners) arrives through ADR 0173, whose
  implementation is #3170. This ADR depends on it.
- Target release train: v0.11.0.

## Alternatives considered

- **Docker in Docker, or a privileged sandbox.** Rejected. It needs privileges
  the sandbox deliberately drops, and a privileged pod running model directed
  code is a node compromise away from every other tenant on that node.
- **A sysbox or Kata `runtimeClass` so the sandbox can run containers.**
  Plausible later, and not rejected on principle. It weakens the isolation story
  the security rails tell today, adds a node level runtime dependency, and still
  does not give the run a real Kubernetes API to deploy to.
- **Give the sandbox a scoped kubeconfig directly.** Rejected. The sandbox is not
  a credential boundary: a token in the sandbox is a token the model holds, and
  scoping it only limits what a leaked token can do. The hosted connector keeps
  it out of reach entirely.
- **Run end to end environments in the factory's own cluster.** Rejected. That
  cluster already runs short of room (#3169, #2949), and end to end load there
  would starve the factory and every other agent on it.
- **Rely only on post publication CI.** Rejected as the only signal, kept as the
  independent check (decision 8). Its feedback arrives after the agent believes
  it is done, at 20 to 40 minutes a round, with three rounds at most.
- **Bake Postgres and Valkey into the runner image.** Useful and complementary:
  with ADR 0173 a bundle can do this to run the fast test tiers that need a
  database. It is not end to end testing, because nothing is deployed, so it
  does not replace this decision.
- **A dedicated ephemeral cluster per run** (for decision 5). Deferred rather
  than rejected. It is the only way to prove a change to a CRD, a PriorityClass
  or the controller in the run, but it costs a cluster boot per run and a much
  wider identity for the connector. It is the path if falling back to CI proves
  too common.

## Tracking

#3242 tracks the implementation. Each ticket is one pull request against `next`:
- #3243, the connector identity on the test cluster and its security review
  (decision 4);
- #3244, the connector, its bundle declaration and a bounded `env_create`
  (decisions 1 to 3);
- #3245, the platform reaper with its health signal and alert (decision 4);
- #3246, daemonless image builds and registry retention (decision 6);
- #3247, deploy by digest and the refusal of cluster scoped objects
  (decisions 1 and 5);
- #3248, the per installation cap and waiting inside the run deadline
  (decision 7);
- #3249, the dark factory adoption, the pull request note when only CI
  proves a change, and the acceptance run (decisions 5 and 8).
