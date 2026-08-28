---
name: using-curie
description: How to drive the Curie harness -- the parity ladder, tier decision logic, landmines, and recovery steps. Invoke when running curie commands, authoring or evaluating a bundle, or debugging a divergence between skill, local, and cluster tiers.
---

# Curie harness primer

Curie is a harness: it guarantees that a bundle behaving in a local chat behaves identically as a deployed local process and again on Kubernetes. You author the skill; the harness owns deployment parity.

You are a coding agent driving this harness. This primer carries only what you cannot derive from the command tree or your training data. Read it before you start.

## Verify-first (before every command)

Your training data is stale. Confirm a command exists before you run it -- never invoke one you have not seen in the manifest or --help. A file existing, a string appearing in output, or a command not erroring is never evidence -- report success only when a command exits 0 and the stated fact holds in its --json payload. The full verification contract is docs/agents.md in the Curie repository (https://github.com/curie-eng/curie/blob/main/docs/agents.md).

    curie schema
    curie skill --help

## The parity ladder (the core loop)

One immutable bundle and one evals/cases.json, run at three tiers. Climb only as far as you need. A tier-to-tier divergence is the harness catching a real environment bug, not your skill's logic.

    # 0. Scaffold a bundle (Claude Code plugin shape + evals/cases.json seed)
    curie init deal-desk
    #   Scaffold a bundle (Claude Code plugin shape) with an evals/cases.json seed.
    # 1. skill tier: the runner alone, offline, fake model -- the fastest loop
    curie skill up --fake-model
    #   Boot the runner alone, offline, no credential -- the fastest authoring loop.
    curie skill check
    #   Prove the bundle's MCP tools actually load -- offline, no credential. A green --fake-model does NOT cover this (the fake model never calls MCP tools).
    curie skill eval
    #   Run evals/cases.json in-process. This is the promotion gate -- but only under a real credential, where the cases are graded. Under --fake-model it reports plumbing_ok: the fake returns one canned reply whatever the input, so nothing is graded and a green here says only that the turn completed.
    curie skill message "hello"
    #   Drive one synthetic turn and stream the reply.
    curie skill down
    #   Stop and remove the runner.
    # 2. local tier: the same bundle through the full platform (compose), zero Slack
    curie local up
    #   Bring up the full platform via compose (queue, worker, sandbox), still zero Slack.
    curie local deploy
    #   Push the identical bundle to the local platform API.
    curie local message "hello"
    #   Drive the real product loop end to end -- the path a Slack mention would take.
    curie local eval
    #   Re-run the SAME evals/cases.json through the local tier -- the per-tier parity gate. A pass here that failed at skill (or vice versa) is the harness catching an environment bug.
    # 3. cluster tier: the same bundle on Kubernetes (a Helm release)
    curie cluster up
    #   Install the release on Kubernetes via Helm.
    curie cluster deploy
    #   Ship the same bundle to the cluster.
    curie cluster message "hello"
    #   Drive the same loop on the cluster.
    curie cluster eval
    #   Re-assert the SAME evals/cases.json on the cluster with the same grader -- the per-tier parity gate at the tier that matters most before prod.

The eval file never changes across tiers. If `skill eval` is green but a deployed message misbehaves, the bundle is fine and the environment is not -- that gap is the signal the harness exists to surface.

## When / which

- **skill tier vs bundle skill artifact**
  The `skill` command names the runner only tier. A bundle skill is an artifact at `skills/<name>/SKILL.md`.

- **skill vs local vs cluster**
  skill is the runner only (offline, no platform, no Slack) -- the tightest loop. local puts the full platform in front of the identical runner via compose. cluster is the same on Kubernetes. Pick the lightest tier that answers your question; promote only to reproduce a divergence.

- **skill vs MCP server**
  A skill is a prompt+tools capability inside the bundle; an MCP server is an external tool surface the skill calls. Add a skill for behavior the agent performs; add an MCP server to give it a new tool.

- **authoring a bundle: interview, not prompts**
  init never prompts interactively. To author a new bundle, interview the human, write an agent-spec.json (name, description, skills, connectors, evals), then run `curie init --from-spec agent-spec.json` -- the CLI scaffolds the bundle deterministically from that spec.

- **how an eval gates promotion**
  evals/cases.json is the contract. `curie skill eval` must be green before you deploy -- green under a real credential, since the fake model grades nothing and only reports plumbing_ok; `curie local eval` and `curie cluster eval` re-run the SAME cases with the SAME grader at each tier (the per-tier parity gate), so a suite that is green at skill can be re-asserted verbatim once deployed. Merging to main promotes to prod (git flow is the deploy model). Never promote a bundle whose evals are red.

## Approvals (a turn can end without a reply)

A turn can end without a reply: it pauses for a human decision and resumes once someone resolves it. The platform owns the durable record, the authorization, and the resume; your skill only raises the request and handles the resume turn. Approval records live in the platform, so this plane exists at the local and cluster tiers, not at skill.

- **Raising one from a skill**
  Call the built-in request_approval tool (it appears as mcp__curie__request_approval) with a one-line summary and, when your instructions name one, a route. It executes nothing: it marks the turn, which then ends awaiting-approval. Tell the user the request is pending and end your turn. The other trigger is configuration rather than the model: a tool named in the bundle's approvalPolicy is denied before it runs and ends the turn the same way.

- **Declaring a route, then binding it**
  A bundle declares routes in .claude-plugin/plugin.json under approvalPolicy.gates[], each entry {gate: <tool name>, route: <route name>}. An operator then binds each route per agent with `approvals <AGENT> --route <name>=<channel>` (and optionally `--route-approvers <name>=users:U1,U2` or `<name>=group:S1`). `channel` is WHERE the card posts; `approvers` is WHO may act on it. A write REPLACES the whole route map, so name every route it should keep.

- **Who may resolve is independent of where the card is**
  approvers.users (an explicit Slack user list) beats approvers.group (a Slack user group) beats the card channel's members, which is the zero-setup default when no approvers block is declared. users and group ignore the click channel entirely, so a card can sit in a room everyone reads while a narrow set decides. Every check runs server-side: the buttons are visible to all, and a refused click gets a private reason.

- **A decision can carry a reason**
  Approve and Reject open a short dialog for an OPTIONAL note before the decision lands; cancelling it resolves nothing. A note that is left reaches the requester on its own -- the API stores it and the resume turn below interpolates it -- so a skill does not collect or forward one itself.

- **The resume turn is yours to handle**
  A resolution wakes the session with a platform-authored turn whose text starts `[approval resolved]` (or `[approval expired]`), naming the decision, who made it, and any note they left. A skill with no instruction for that prefix silently drops the verdict, and nothing in the command tree tells you the turn exists.

- **Driving it with no workspace connected**
  --list reports each pending record's id, summary, and the route it named; --resolve settles one as a named actor. Resolution is once-only: the first authorized resolver wins and a later one is told who won.

    curie local approvals <AGENT> --list
    curie local approvals <AGENT> --resolve <id> --as <user> --actor-channel <channel>

## Landmines (non-discoverable)

- **Self-approval is refused, from any channel**
  The platform blocks the author of the turn that raised a request from resolving it, under every approver set. Testing an approval flow therefore needs a second actor: pass `--as <other-user>` on the resolve.

- **A group route with no bot token on the API refuses every click**
  `--route-approvers <name>=group:<S...>` makes the API resolve Slack user-group membership, which needs SLACK_BOT_TOKEN with the `usergroups:read` scope on the API process. Without it the lookup is undetermined and the authorizer fails CLOSED: every resolution reports `could not verify approver group membership`; the refusal does not name the missing token. Bind `users:` instead, or set the token (the scope needs the Slack app reinstalled).

- **Silence after a request usually means an unbound route**
  A route the bundle names but the agent's approval_routes never bound is escalated to a human rather than widened to the requesting channel, so no card appears anywhere and the turn still reports the request as pending. Bind the route with `--route <name>=<channel>` and re-run the turn; check that before suspecting the gate.

- **Fake vs live model is symmetric across skill and local**
  `curie skill up` and `curie local up` both run the real model when a credential is present and the fake model otherwise; `curie skill up --fake-model` or CURIE_FAKE_MODEL=1 forces the offline fake at either tier.

- **A real model in-cluster needs its provider's egress opened**
  The runner sandbox is default-deny egress, so `curie cluster up` with a credential is still sealed and the model unreachable until you pass --allow-egress-host <provider> (anthropic/openrouter/zhipu/moonshot/deepseek). Zhipu, Moonshot, and DeepSeek also need their matching base URL in worker runtime configuration; a web-fetching skill additionally needs --allow-web-egress <CIDR> for its hosts.

- **The skill runner executes an immutable snapshot taken at `skill up`**
  A `SKILL.md` or `.mcp.json` edit does not reach a running runner: re-run `curie skill up --replace` and confirm the new `bundle_digest` in `curie skill status --json`. `evals/cases.json` IS read live from source, so the contract can update while the behavior stays frozen at the boot snapshot.

- **secretKeyRef env vars resolve once, at pod start**
  A connect that rotates a secret must also roll the pod; `curie cluster comms` does this for you.

- **You wire the non-secret setup; the human supplies only secrets and browser-made apps**
  When you are asked to get a bundle running, do the plumbing yourself -- source the bundle's dotenv, run `curie local up`, `curie local deploy`, and `curie local comms --slack`, then tear down -- rather than handing the human a shell checklist to copy-paste. Two parts are irreducibly manual and stay with the human: supplying the actual secret VALUES (never type a credential or API key yourself) and creating an external app in a browser to mint its tokens (e.g. the Slack app). Automate everything between. If a credential already lives in the environment or the bundle's own `.env`, use it instead of asking for it to be exported.

- **A real-model `cluster up` fails closed without a gVisor runtime**
  On a cluster with no `runsc` RuntimeClass, the default `security.gvisor.mode=auto` renders a blocking enforcement preflight for a real (non-fake) model, so `curie cluster up` fails closed instead of running runner pods on the host kernel. Install anyway with `curie cluster up --set security.gvisor.mode=off` (no kernel isolation, knowingly), use `--fake-model` (the sealed path skips the preflight), or install runsc on the nodes. This is the security posture, not a bug.

## Error -> recovery

- **"platform API ... unreachable" on a local deploy or message**
  The stack is down. Run `curie local up`, then retry.

- **`curie cluster up` hangs ~2 min then dies with `job curie-preflight-gvisor failed: DeadlineExceeded`**
  A real-model install on a cluster with no `runsc` RuntimeClass fails closed under `security.gvisor.mode=auto`. Opt out with `curie cluster up --set security.gvisor.mode=off`, or use `--fake-model`, or install runsc on the nodes.

- **"(no response)" or an empty reply**
  You are on the fake model (--fake-model, or a sealed install). Provide a credential to go live; on cluster, also open the provider egress with --allow-egress-host <provider>. Zhipu, Moonshot, and DeepSeek also need their matching base URL in worker runtime configuration or the model stays unreachable.

- **the agent answers but never calls your MCP tools**
  Run `curie skill check` -- a RED verdict names the server that failed to load and how to fix the declaration.

- **a resolve is refused with "self-approval is blocked"**
  You are the author of the turn that raised it, which no approver set overrides. Resolve as a different actor with `--as <user>`.

- **a resolve returns 409 already resolved**
  Someone settled it first; resolution is once-only and the loser is told who won. Read the decision off `--list` (or the card) rather than retrying -- a retry cannot change it.

- **a resolve returns 410 expired**
  The record aged out before anyone acted, and the turn already resumed with an `[approval expired]` prefix. Nothing is recoverable on that record: drive the gated action again to raise a fresh one.

- **a resolve is refused with "you are not an approver"**
  The route's approver set does not admit that actor from that channel. With no approvers block the card channel's members are the set, so pass `--actor-channel` with the channel that route is bound to. A null `card_channel` is from an older row or a direct API write that omitted the field, so use the requesting channel.

- **a resolve is refused with "could not verify approvers"**
  The declared approvers block is malformed, so the platform cannot determine who may resolve. Correct its `users` or `group` value, then replace the complete route map.

- **a resolve is refused with "could not verify approver group membership"**
  Slack group membership could not be verified, so the platform fails CLOSED. The refusal does not name the missing token. Check the API's SLACK_BOT_TOKEN, its `usergroups:read` scope and reinstallation, and Slack availability.

- **a command "does not exist"**
  You trusted training over the manifest. Re-run `curie schema` and use the confirmed spelling.

When anything is uncertain, confirm it against `curie schema` before you act.
