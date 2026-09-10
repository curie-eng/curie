# Deciding what belongs in a connector

Connectors are the platform's answer to "the sandbox must not hold this". What
they are *for* is well covered by [ADR-0075](../adr/0075-the-agent-proxy-credential-and-egress-boundary.md)
puts the credential on the far side of the sandbox boundary, and
[ADR-0086](../adr/0086-bundles-declare-connectors-the-platform-hosts-them.md)
makes a bundle declare one and the platform run it.

What none of them says is what should stay **out**. This guide is that half: a
test for whether a piece of work belongs in a connector at all, and where to
spend a boundary when you only get to spend it once.

## 1. The three-way test

For each piece of work, decide which of three things it is.

| It is a… | Example | Where it goes |
|---|---|---|
| **Credential** | an API token, a client secret, a kubeconfig | Connector |
| **Capability** | writing to a storage location, patching a Deployment, sending mail | Connector |
| **Calculation** | mapping a value into a template, formatting a report, parsing a request | **The sandbox.** A skill and the code it calls |

The first two are what a connector exists for. The third is not, and putting it
in one is using a security mechanism as a code-organisation mechanism.

**It costs more than it looks.** A connector is a separate image: a build, a
pushed artifact, a pinned digest ([ADR-0113](../adr/0113-bundles-declare-connector-build-inputs-and-tiers-deliver-pinned-images.md)),
a redeploy on every change, and a second place to read logs when something goes
wrong. Configuration that changes weekly becomes a rebuild every week.

**And it buys nothing you did not already have.** The usual argument for moving
computation into a connector is determinism. The same inputs must produce the
same output every run. But determinism is a property of the code, not of the
process it runs in. Same function, same input, same answer, whichever side of the
boundary it sits on. Moving a calculation across the line does not make it more
correct; unit tests do that, and they run identically either way.

## 2. When a calculation *does* belong in a connector

One case, and it is the case the SRE bot example is built around: when the
calculation is the only thing bounding the capability.

`examples/sre-bot/connectors/k8s-write/server.py` builds its patch body itself,
a constant with a timestamp filled in, and exposes no parameter through which a
caller reaches an image, a command or a replica count. If that body-building
moved into the sandbox, a caller could supply the body, and a restart tool would
become an arbitrary-write tool. The computation *is* the ceiling.

**The test:** if this code moved into the sandbox, could a caller reach the
capability without it?

- **Yes**: it stays in the connector. It is part of the ceiling, not a helper.
- **No**: the tool surface already bounds the capability, and the computation
  can live in the sandbox where it is cheaper to change and easier to debug.

## 3. The four layers

When you do put a boundary in a connector, know which layer is holding it.
Listed strongest first, because the weakest is the one people assume is doing the
work. `examples/sre-bot/README.md` develops this at length for one connector.

| # | Layer | What it actually stops |
|---|---|---|
| 1 | **The tool surface** | There is no parameter through which a caller reaches the thing. |
| 2 | **The allowlist inside the connector** | Checked before the request is built. |
| 3 | **The credential's own permissions** | Bounds a *compromised connector*. |
| 4 | **The approval gate** | A control on the agent, not on the credential. |

**Scope layers 1 to 3 as if the gate were absent.** If the credential leaked the
gate would be irrelevant, and a human can approve the wrong thing. A gate is a
prompt to a person, and people click. Layer 4 is a good control and a bad
ceiling.

## 4. When the credential cannot express the boundary

This is the case that decides most designs, and it is easy to miss because the
instinct, *grant the narrowest credential and let the platform enforce it*, is
right, and then turns out not to be available.

Sometimes the boundary you want cannot be written as a permission. Then layer 3
is simply not on offer, and the code has to carry it. That is not a workaround.
It is layer 1, which is stronger anyway.

**The worked example in this repository is Kubernetes RBAC.** `kubectl rollout
restart` is not a distinct permission; it is a PATCH of the pod template. So
`patch` on deployments is the *same grant* as `set image`, `set env`, and
replacing the container command. RBAC grants verbs on resources and cannot say
"patch only this one field". `examples/sre-bot/manifests/write-role.yaml` says so
in its own comments, and it is why that connector constructs its patch rather
than forwarding one.

**The shape generalises.** Object stores and document platforms commonly grant
per-bucket or per-site, not per-prefix or per-folder. If two locations that must
be treated differently live under one grant, no credential distinguishes them and
the connector's own path check is the only thing that does.

**Two rules for that situation:**

1. **Measure before you assume it.** Whether a given provider can express your
   boundary is a claim about a dependency, and this repository's method is that
   documentation is not an observation. Probe it against a real account: attempt
   the operation you believe is forbidden and record what came back. Either answer
   improves the design. A refusal means the grant can carry the boundary and you
   should let it, and a success means your code is the only thing there is and the
   evidence should say so plainly.
2. **Keep the code layer regardless.** Two layers beat one. If the grant turns out
   to enforce the boundary too, the code check becomes defence in depth rather
   than dead weight, and it is what survives a credential being widened by someone
   who did not read this page.

## 5. Is there a human downstream?

Boundaries are not free, and you rarely get to put one everywhere. This is how to
choose.

An agent is prompt-injectable by construction: it reads logs, tickets, mail and
dashboard titles that other people wrote. So assume, for each thing it can do,
that it can be made to do it wrongly. Then ask: **is there a human between that
and the consequence?**

- **Yes**: a draft in a review queue, a suggested change, a summary a person
  reads. The review is the control. A connector boundary here buys little, and
  the real controls are unit tests and the review itself.
- **No**: a published document, a cluster change, a sent message, a payment.
  This is what a boundary is for. Spend it here.

A concrete pattern: a generate-then-review-then-approve workflow, where approval
is a person moving a file from one location to another. A wrong value in a draft
is caught by the review. That is what the review is. A write into the *approved*
location is not caught by anything, because it skipped the step that would have
caught it. So the boundary goes on the approved location and nowhere else, and
the code producing the draft can live in the sandbox.

**The corollary matters as much.** If the human step is ever removed, every "yes"
above becomes a "no" and the calculation changes. An agent that publishes without
review needs the boundaries an agent that drafts for review does not. Treat "a
person always reviews this" as a load-bearing assumption, and revisit the design
when it stops being true rather than after.

## 6. Rules a connector follows

Each of these has cost somebody a day.

**Expose the fixed endpoint.** Connector containers must listen on port `8000`.
HTTP MCP connectors serve `/mcp` where applicable. The `args` in
`connectors.yaml` pass through verbatim, so configure the image to listen on
that port and serve that path.

**Fail closed on missing configuration.** An empty allowlist means refuse to
start, never "everything permitted". A connector that comes up with no ceiling
looks healthy and is wrong.

**Declare the mode, never infer it.** A connector with a fake and a real mode
reads an explicit variable. Inferring "real" from the presence of credentials
means a typo'd secret name starts it in fake mode, where it accepts calls,
reports success and does nothing. A fake does not fail. It answers.

**An empty result is not a success.** A readable-but-empty folder, a query
matching no rows, a search finding nothing: each is its own status, distinct from
`ok`. Handed `{"status": "ok", "items": []}`, a model will report "there are none"
as a fact. A confident "nothing" is worse than an error, because the caller
believes it and stops looking.

**Keep failures distinguishable.** Not-authenticated (the credential),
not-authorized (the grant on it), not-found, empty, and refused (your own ceiling
a request that was never sent) mean five different fixes. Collapsing 401 and 403
costs an hour every time; an operator widening a permission to fix a *refused* is
changing the wrong thing entirely.

**State the ceiling in two places on purpose.** The grant, and the connector's own
allowlist. A ceiling stated once is a ceiling that moves when somebody edits the
other place.

**Never let a secret reach a returned message.** Error text goes to logs and into
a model's context. Do not echo values, and think twice about echoing your
configured scope, which is a map of what this credential can reach.

**Annotate tools honestly.** A gate is armed by exact tool name, so a write tool
annotated `readOnlyHint` is a write with nothing in front of it. And audit what a
read tool *returns*, not just its annotation: a tool that hands back a kubeconfig
is annotated read-only correctly, because reading a credential is a read.

**Do not hand a policy decision back to the caller.** Whether a conflicting write
is refused or replaces what is there is operator configuration, not a tool
argument. A per-call `overwrite=true` states the decision and then gives it to a
model that is reading text a stranger wrote.
