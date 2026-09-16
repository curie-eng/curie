# Documentation

Read the living docs below for how the system works today. The pre-build design
and de-risking documents were removed after the MVP shipped and are preserved in
git history (`git log -- docs/`).

## Living documentation

* [`agent-install.md`](agent-install.md): an unattended release install, verification, and result reporting procedure for a coding agent.
- [`agents.md`](agents.md): the verification contract for an agent driving
  Curie. The exact commands that prove an outcome, what to automate versus what
  to ask a human for, and the rule that a file existing or a string appearing in
  output is never evidence.
- [`onboarding.md`](onboarding.md): the local dev onboarding path for a new
  engineer, from a fresh clone to a verified local turn with no Slack and no
  cluster.
- [`vision.md`](vision.md): the north star. What Curie is, who it is for, and
  what it could become. The document we hold new features against. Read this to
  understand *why* the project exists before *how* it works.
- [`../ARCHITECTURE.md`](../ARCHITECTURE.md): the as-built architecture reference
  (the deep "what talks to what," with path/symbol citations). Start here for
  detail.
- [`diagrams/`](diagrams/): four focused, presentation-grade flow docs, each a
  single clean diagram with narration. Start here to explain the system to
  someone:
  - [`diagrams/message-flow.md`](diagrams/message-flow.md): how a message comes
    in and a reply goes out (the core loop).
  - [`diagrams/kubernetes.md`](diagrams/kubernetes.md): the cluster and how a
    sandbox pod is built.
  - [`diagrams/aci.md`](diagrams/aci.md): the agent container interface, the
    frozen contract between the worker and the agent in the box.
  - [`diagrams/seams.md`](diagrams/seams.md): the seam overlay — where the black
    lines are, i.e. every port you could cut and what is plugged into it today.
- [`architecture.md`](architecture.md): a redirect stub pointing at the root
  `ARCHITECTURE.md` above.
- [`architecture-vision.md`](architecture-vision.md): the forward-looking
  "swappable jobs around an opinionated core" framing, covering where each
  adopted component's swap seam lives.
- [`interfaces.md`](interfaces.md): the swappable-seam catalog (INTERFACE.md per
  black line) with the swap-readiness grade table.
- [`approvals.md`](approvals.md): the human-in-the-loop plane. How a turn pauses
  for a human decision, how a route is declared and bound to a channel, who is
  allowed to resolve it, and the resume turn your skill has to handle.
- [`thinking.md`](thinking.md): what a reasoning model costs you and how to
  change it — the measured latency gap, the two operator layers that turn
  thinking down, and the OpenRouter parameter that looks like it works and does
  not.
- [`behavior-packs.md`](behavior-packs.md): per-agent, opt-in UX touches (load
  lines, capability tips, greeting/help) applied around a turn — why packs are
  declarative data rather than code, and what is wired today.
- [`operations.md`](operations.md): running a cluster install, plus
  operator-facing findings from early installs.
- [`guides/repository-toolchain-in-the-managed-sandbox.md`](guides/repository-toolchain-in-the-managed-sandbox.md):
  the supported recipe for installing a repository's dependencies and running
  its checks inside a managed sandbox, including the measured fail-closed
  live-registry refusal (~368s on unconfigured pip; the runner image now
  ships `/etc/pip.conf` with `retries = 0`).
- [`release-verification.md`](release-verification.md): what every release asset
  carries (checksums, signature, provenance, SBOM) and how to verify one before
  you run it, plus the patch-release rule that the cut names its defect trigger
  and a live proof.
- [`design/multi-dev-shared-cluster.md`](design/multi-dev-shared-cluster.md): the
  design pass for many developers iterating against one shared cluster (isolation,
  targeting, ambient context), answering epic #44's deferred open questions.
- [`slack-local-runbook.md`](slack-local-runbook.md): connect your own Slack
  app to the local compose stack to exercise real mentions and threads without a
  cluster.
- [`pillars.md`](pillars.md): the eight agent-development pillars issues, PRs,
  and discussions are labeled against, and how to apply the labels.

Forward-looking work is planned and tracked in
[GitHub issues](https://github.com/curie-eng/curie/issues), with larger
journeys filed as `epic`-labeled issues.
- [`adr/`](adr/): Architecture Decision Records, the load-bearing choices and
  the live evidence behind them.
