//! `curie guide`: the self-describing harness primer (ADR-0021, issue #322).
//!
//! Emits a compact, self-contained "how to drive this harness" document for a
//! coding agent, modeled on `SKILL.md`: ordered by what the agent needs first,
//! roughly 100 lines, carrying only non-discoverable knowledge -- the parity
//! ladder, the when/which decision logic, the landmines, and verify-first. It
//! deliberately omits a directory tour the agent could derive itself (ADR-0021
//! decision 2: restate-the-obvious guidance measurably hurts).
//!
//! One data model ([`primer`]) is the single source of truth; the Markdown
//! default and the `--json` structured variant both render from it, so a command
//! printed by one is byte-identical in the other and the drift test in
//! `cli/tests/guide.rs` can validate the printed commands against the CLI's own
//! `curie schema` manifest. Every printed `curie ...` invocation is a real
//! command path -- prose refers to the product as "Curie".

use anyhow::Result;
use serde::Serialize;

use crate::ui;

/// The whole primer as data. Serialized directly for `--json`; rendered to
/// Markdown for the default. Fields are ordered as the agent reads them.
#[derive(Serialize)]
pub struct Primer {
    pub harness: &'static str,
    pub summary: &'static str,
    pub verify_first: VerifyFirst,
    pub parity_ladder: Vec<Rung>,
    pub decision_logic: Vec<Decision>,
    pub approvals: Approvals,
    pub landmines: Vec<Landmine>,
    pub recovery: Vec<Recovery>,
}

/// The human-in-the-loop plane. It earns a section of its own rather than a
/// landmine because none of it is derivable: the command tree shows one
/// `approvals` verb whose flags read as tool-gate config, while the plane behind
/// it spans a bundle declaration, an operator route binding, three approver sets
/// resolved server-side, and a resume turn the skill itself must handle.
#[derive(Serialize)]
pub struct Approvals {
    pub summary: &'static str,
    pub facts: Vec<ApprovalFact>,
    pub commands: Vec<&'static str>,
}

#[derive(Serialize)]
pub struct ApprovalFact {
    pub title: &'static str,
    pub detail: &'static str,
}

#[derive(Serialize)]
pub struct VerifyFirst {
    pub why: &'static str,
    pub commands: Vec<&'static str>,
}

/// One rung of the parity ladder: a real, runnable command and why to run it.
#[derive(Serialize)]
pub struct Rung {
    pub tier: &'static str,
    pub command: &'static str,
    pub purpose: &'static str,
}

#[derive(Serialize)]
pub struct Decision {
    pub question: &'static str,
    pub answer: &'static str,
}

#[derive(Serialize)]
pub struct Landmine {
    pub title: &'static str,
    pub detail: &'static str,
}

#[derive(Serialize)]
pub struct Recovery {
    pub symptom: &'static str,
    pub fix: &'static str,
}

/// The authored primer. Content lives here so Markdown and JSON never diverge.
pub fn primer() -> Primer {
    Primer {
        harness: "curie",
        summary: "Curie is a harness: it guarantees that a bundle behaving in a local chat \
                  behaves identically as a deployed local process and again on Kubernetes. You \
                  author the skill; the harness owns deployment parity.",
        verify_first: VerifyFirst {
            why: "Your training data is stale. Confirm a command exists before you run it -- \
                  never invoke one you have not seen in the manifest or --help. A file \
                  existing, a string appearing in output, or a command not erroring is never \
                  evidence -- report success only when a command exits 0 and the stated fact \
                  holds in its --json payload. The full verification contract is docs/agents.md \
                  in the Curie repository \
                  (https://github.com/curie-eng/curie/blob/main/docs/agents.md).",
            commands: vec!["curie schema", "curie skill --help"],
        },
        parity_ladder: vec![
            Rung {
                tier: "init",
                command: "curie init deal-desk",
                purpose: "Scaffold a bundle (Claude Code plugin shape) with an evals/cases.json seed.",
            },
            Rung {
                tier: "skill",
                command: "curie skill up --fake-model",
                purpose: "Boot the runner alone, offline, no credential -- the fastest authoring loop.",
            },
            Rung {
                tier: "skill",
                command: "curie skill check",
                purpose: "Prove the bundle's MCP tools actually load -- offline, no credential. \
                          A green --fake-model does NOT cover this (the fake model never calls \
                          MCP tools).",
            },
            Rung {
                tier: "skill",
                command: "curie skill eval",
                purpose: "Run evals/cases.json in-process. This is the promotion gate -- but only \
                          under a real credential, where the cases are graded. Under --fake-model \
                          it reports plumbing_ok: the fake returns one canned reply whatever the \
                          input, so nothing is graded and a green here says only that the turn \
                          completed.",
            },
            Rung {
                tier: "skill",
                command: "curie skill message \"hello\"",
                purpose: "Drive one synthetic turn and stream the reply.",
            },
            Rung {
                tier: "skill",
                command: "curie skill down",
                purpose: "Stop and remove the runner.",
            },
            Rung {
                tier: "local",
                command: "curie local up",
                purpose: "Bring up the full platform via compose (queue, worker, sandbox), still zero Slack.",
            },
            Rung {
                tier: "local",
                command: "curie local deploy",
                purpose: "Push the identical bundle to the local platform API.",
            },
            Rung {
                tier: "local",
                command: "curie local message \"hello\"",
                purpose: "Drive the real product loop end to end -- the path a Slack mention would take.",
            },
            Rung {
                tier: "local",
                command: "curie local eval",
                purpose: "Re-run the SAME evals/cases.json through the local tier -- the per-tier parity gate. A pass here that failed at skill (or vice versa) is the harness catching an environment bug.",
            },
            Rung {
                tier: "cluster",
                command: "curie apply --init",
                purpose: "Write a starter curie.yaml, then `curie apply` / `curie diff --context`.",
            },
            Rung {
                tier: "cluster",
                command: "curie cluster deploy",
                purpose: "Ship the same bundle to the cluster.",
            },
            Rung {
                tier: "cluster",
                command: "curie cluster message \"hello\"",
                purpose: "Drive the same loop on the cluster.",
            },
            Rung {
                tier: "cluster",
                command: "curie cluster eval",
                purpose: "Re-assert the SAME evals/cases.json on the cluster with the same grader -- the per-tier parity gate at the tier that matters most before prod.",
            },
        ],
        decision_logic: vec![
            Decision {
                question: "skill tier vs bundle skill artifact",
                answer: "The `skill` command names the runner only tier. A bundle skill is an \
                         artifact at `skills/<name>/SKILL.md`.",
            },
            Decision {
                question: "skill vs local vs cluster",
                answer: "skill is the runner only (offline, no platform, no Slack) -- the tightest \
                         loop. local puts the full platform in front of the identical runner via \
                         compose. cluster is the same on Kubernetes. Pick the lightest tier that \
                         answers your question; promote only to reproduce a divergence.",
            },
            Decision {
                question: "skill vs MCP server",
                answer: "A skill is a prompt+tools capability inside the bundle; an MCP server is an \
                         external tool surface the skill calls. Add a skill for behavior the agent \
                         performs; add an MCP server to give it a new tool.",
            },
            Decision {
                question: "authoring a bundle: interview, not prompts",
                answer: "init never prompts interactively. To author a new bundle, interview the \
                         human, write an agent-spec.json (name, description, skills, connectors, \
                         evals), then run `curie init --from-spec agent-spec.json` -- the CLI \
                         scaffolds the bundle deterministically from that spec.",
            },
            Decision {
                question: "how an eval gates promotion",
                answer: "evals/cases.json is the contract. `curie skill eval` must be green before \
                         you deploy -- green under a real credential, since the fake model grades \
                         nothing and only reports plumbing_ok; \
                         `curie local eval` and `curie cluster eval` re-run the SAME \
                         cases with the SAME grader at each tier (the per-tier parity gate), so a \
                         suite that is green at skill can be re-asserted verbatim once deployed. \
                         Merging to main promotes to prod (git flow is the deploy model). Never \
                         promote a bundle whose evals are red.",
            },
        ],
        approvals: Approvals {
            summary: "A turn can end without a reply: it pauses for a human decision and \
                      resumes once someone resolves it. The platform owns the durable record, \
                      the authorization, and the resume; your skill only raises the request and \
                      handles the resume turn. Approval records live in the platform, so this \
                      plane exists at the local and cluster tiers, not at skill.",
            facts: vec![
                ApprovalFact {
                    title: "Raising one from a skill",
                    detail: "Call the built-in request_approval tool only when it is available \
                             (it appears as mcp__curie__request_approval) and a step genuinely \
                             needs sign-off. It takes a one-line summary and, when your \
                             instructions name one, a route. It executes nothing: it marks the \
                             turn, which then ends awaiting-approval. If the tool is absent, \
                             explain that the bundle cannot perform the action; do not fabricate \
                             a remediation request. The runner omits it for a surface with no MCP \
                             tools or only tools explicitly readOnlyHint=true, unless an actionable \
                             approval gate exists. readOnlyHint controls advertisement only, not \
                             authorization or tool execution. The other trigger is configuration \
                             rather than the model: a tool named in the bundle's approvalPolicy is \
                             denied before it runs and ends the turn the same way.",
                },
                ApprovalFact {
                    title: "Declaring a route, then binding it",
                    detail: "A bundle declares routes in .claude-plugin/plugin.json under \
                             approvalPolicy.gates[], each entry {gate: <tool name>, route: \
                             <route name>}. An operator then binds each route per agent with \
                             `approvals <AGENT> --route-resolution <name>=<channel>` (and optionally \
                             `--route-approvers <name>=users:U1,U2` or `<name>=group:S1`). Declare \
                             a notification in the strict `--routes-from` JSON map. \
                             `resolution` is the single verified Slack card where an identity \
                             acts; `notification` is a text-only ping with interaction=None, \
                             and `approvers` is WHO may act. A notification directs humans to \
                             the configured approval channel without disclosing its identifier \
                             and carries no resolving affordance. Slack uses \
                             its configured transport; a non-Slack notification needs both \
                             endpoint and adapter in --routes-from. Resolution on another \
                             channel remains a future extension requiring a scoped credential. \
                             A write REPLACES the whole route map, so name every route it should keep.",
                },
                ApprovalFact {
                    title: "Who may resolve is independent of where the card is",
                    detail: "approvers.users (an explicit Slack user list) beats approvers.group \
                             (a Slack user group) beats the card channel's members, which is the \
                             zero-setup default when no approvers block is declared. users and \
                             group ignore the click channel entirely, so a card can sit in a \
                             room everyone reads while a narrow set decides. The API authorizes \
                             an authenticated principal against that selected set; it never \
                             trusts a caller-selected name or channel. Every check runs \
                             server-side: the buttons are visible to all, and a refused click \
                             gets a private reason.",
                },
                ApprovalFact {
                    title: "Terminal and Console identity are bootstrapped explicitly",
                    detail: "An administrator can mint a reusable operator token with \
                             `approvals <AGENT> --mint-operator-principal <SUBJECT>`, then export \
                             its one-time result as CURIE_APPROVAL_PRINCIPAL_TOKEN before running \
                             --resolve. A terminal principal carries no channel evidence, so it \
                             works only when the route uses an explicit-user approver list. \
                             `--mint-console-login-code <SUBJECT>` instead creates a subject-bound \
                             one-time browser login: the Console can use an explicit-user list or \
                             a Slack user group the server verifies for that subject, but never \
                             channel membership. For a channel-backed route, use the authenticated \
                             card; neither bootstrap mode may invent membership.",
                },
                ApprovalFact {
                    title: "Authorized requesters may self-confirm",
                    detail: "An ordinary gate is a confirmation, not an implicit two-person \
                             policy. The authenticated requester may self-confirm when that same \
                             principal belongs to the configured approver set; matching the author \
                             neither grants membership nor vetoes it. A workflow requiring \
                             separation of duties needs a distinct policy.",
                },
                ApprovalFact {
                    title: "A decision can carry a reason",
                    detail: "Approve and Reject open a short dialog for an OPTIONAL note \
                             before the decision lands; cancelling it resolves nothing. A \
                             note that is left reaches the requester on its own -- the API \
                             stores it and the resume turn below interpolates it -- so a \
                             skill does not collect or forward one itself.",
                },
                ApprovalFact {
                    title: "The resume turn is yours to handle",
                    detail: "A resolution wakes the session with a platform-authored turn whose \
                             text starts `[approval resolved]` (or `[approval expired]`), naming \
                             the decision, who made it, and any note they left. A skill with no \
                             instruction for that prefix silently drops the verdict, and nothing \
                             in the command tree tells you the turn exists.",
                },
                ApprovalFact {
                    title: "Driving it with no workspace connected",
                    detail: "--list reports each pending record's id, summary, and the route it \
                             named; --resolve settles one as the authenticated principal from \
                             CURIE_APPROVAL_PRINCIPAL_TOKEN. Resolution is \
                             once-only: the first authorized resolver wins and a later one is \
                             told who won.",
                },
            ],
            commands: vec![
                "curie local approvals <AGENT> --list",
                "curie local approvals <AGENT> --mint-operator-principal <SUBJECT>",
                "curie local approvals <AGENT> --resolve <id>",
            ],
        },
        landmines: vec![
            Landmine {
                title: "A terminal principal has no channel membership",
                detail: "CURIE_APPROVAL_PRINCIPAL_TOKEN proves its subject but carries no channel. It can resolve only an explicit-user route that includes that subject. Use the authenticated card for the default card-channel set or a Slack group set.",
            },
            Landmine {
                title: "A group route with no bot token on the API refuses every click",
                detail: "`--route-approvers <name>=group:<S...>` makes the API resolve Slack user-group membership, which needs SLACK_BOT_TOKEN with the `usergroups:read` scope on the API process. Without it the lookup is undetermined and the authorizer fails CLOSED: every resolution reports `could not verify approver group membership`; the refusal does not name the missing token. Bind `users:` instead, or set the token (the scope needs the Slack app reinstalled).",
            },
            Landmine {
                title: "Silence after a request usually means an unbound route",
                detail: "A route the bundle names but the agent's approval_routes never bound is escalated to a human rather than widened to the requesting channel, so no card appears anywhere and the turn still reports the request as pending. Bind its verified Slack resolution with `--route-resolution <name>=<channel>` and re-run the turn; check that before suspecting the gate.",
            },
            Landmine {
                title: "Fake vs live model is symmetric across skill and local",
                detail: "`curie skill up` and `curie local up` both run the real model when a credential is present and the fake model otherwise; `curie skill up --fake-model` or CURIE_FAKE_MODEL=1 forces the offline fake at either tier.",
            },
            Landmine {
                title: "Ambiguous provider credentials need explicit cluster egress",
                detail: "Direct `curie cluster up` infers anthropic from sk-ant- and openrouter from sk-or- when --allow-egress-host is absent. Other credential shapes stay sealed until you pass --allow-egress-host <provider> (zhipu/moonshot/deepseek) or a raw range. An explicit provider list that omits the detected provider is an error. Zhipu, Moonshot, and DeepSeek also need their matching base URL in worker runtime configuration; a web fetching skill additionally needs --allow-web-egress <CIDR> for its hosts.",
            },
            Landmine {
                title: "The skill runner executes an immutable snapshot taken at `skill up`",
                detail: "A `SKILL.md` or `.mcp.json` edit does not reach a running runner: re-run `curie skill up` and confirm the new `bundle_digest` in `curie skill status --json`. A verified same-directory runner is replaced automatically; `--replace` still forces a restart. `evals/cases.json` IS read live from source, so the contract can update while the behavior stays frozen at the boot snapshot.",
            },
            Landmine {
                title: "secretKeyRef env vars resolve once, at pod start",
                detail: "A connect that rotates a secret must also roll the pod; `curie cluster comms` does this for you.",
            },
            Landmine {
                title: "You wire the non-secret setup; the human supplies only secrets and browser-made apps",
                detail: "When you are asked to get a bundle running, do the plumbing yourself -- source the bundle's dotenv, run `curie local up`, `curie local deploy`, and `curie local comms --slack`, then tear down -- rather than handing the human a shell checklist to copy-paste. Two parts are irreducibly manual and stay with the human: supplying the actual secret VALUES (never type a credential or API key yourself) and creating an external app in a browser to mint its tokens (e.g. the Slack app). Automate everything between. If a credential already lives in the environment or the bundle's own `.env`, use it instead of asking for it to be exported.",
            },
            Landmine {
                title: "Missing gVisor is inferred only from admission",
                detail: "A real model install first keeps the gVisor chart default. When admission reports exactly that RuntimeClass gvisor is not found, direct `curie cluster up` shows that attempt as retrying, applies security.gvisor.mode=off, prints the override, and retries once. Other failures remain closed. Explicit auto or require modes contradict the detected result and are errors.",
            },
        ],
        recovery: vec![
            Recovery {
                symptom: "\"platform API ... unreachable\" on a local deploy or message",
                fix: "Check the API URL and follow the local status guidance. Run `curie local up` only if local services are absent, then retry.",
            },
            Recovery {
                symptom: "`curie cluster up` fails immediately when `curie-preflight-gvisor` reports `FailedCreate`: `RuntimeClass \"gvisor\" not found`",
                fix: "Plain `curie cluster up` now infers security.gvisor.mode=off from this exact admission result and retries once. If you explicitly set auto or require, remove that setting to accept the detected posture, or install runsc on the nodes.",
            },
            Recovery {
                symptom: "`curie cluster up` hangs ~2 min then dies with `job curie-preflight-gvisor failed: DeadlineExceeded`",
                fix: "The Kubernetes Event watch was unavailable, so Helm reported the later deadline. A real-model install on a cluster with no `runsc` RuntimeClass still fails closed. Opt out with `curie cluster up --set security.gvisor.mode=off`, or use `--fake-model`, or install runsc on the nodes.",
            },
            Recovery {
                symptom: "\"(no response)\" or an empty reply",
                fix: "You are on the fake model (--fake-model, or a sealed install). Provide a credential to go live. On cluster, sk-ant- and sk-or- infer their provider egress; other providers need --allow-egress-host <provider>. Zhipu, Moonshot, and DeepSeek also need their matching base URL in worker runtime configuration or the model stays unreachable.",
            },
            Recovery {
                symptom: "the agent answers but never calls your MCP tools",
                fix: "Run `curie skill check` -- a RED verdict names the server that failed to load and how to fix the declaration.",
            },
            Recovery {
                symptom: "a resolve says CURIE_APPROVAL_PRINCIPAL_TOKEN is missing",
                fix: "Mint a reusable subject-bound token with `<tier> approvals <AGENT> --mint-operator-principal <SUBJECT>`, export the one-time result as CURIE_APPROVAL_PRINCIPAL_TOKEN, and retry. It is eligible only for explicit-user routes; use the authenticated card for channel/group routes.",
            },
            Recovery {
                symptom: "a resolve returns 409 already resolved",
                fix: "Someone settled it first; resolution is once-only and the loser is told who won. Read the decision off `--list` (or the card) rather than retrying -- a retry cannot change it.",
            },
            Recovery {
                symptom: "a resolve returns 410 expired",
                fix: "The record aged out before anyone acted, and the turn already resumed with an `[approval expired]` prefix. Nothing is recoverable on that record: drive the gated action again to raise a fresh one.",
            },
            Recovery {
                symptom: "a resolve is refused with \"you are not an approver\"",
                fix: "The route's selected approver set does not admit the authenticated principal. A terminal principal is eligible only for an explicit-user list containing its subject. A Console principal can also satisfy a Slack user group through the server-side membership lookup, but neither principal can satisfy channel membership; use the authenticated card for that default set. A null `card_channel` is from an older row or a direct API write that omitted the field, so the requesting channel is the card location.",
            },
            Recovery {
                symptom: "a resolve is refused with \"could not verify approvers\"",
                fix: "The declared approvers block is malformed, so the platform cannot determine who may resolve. Correct its `users` or `group` value, then replace the complete route map.",
            },
            Recovery {
                symptom: "a resolve is refused with \"could not verify approver group membership\"",
                fix: "Slack group membership could not be verified, so the platform fails CLOSED. The refusal does not name the missing token. Check the API's SLACK_BOT_TOKEN, its `usergroups:read` scope and reinstallation, and Slack availability.",
            },
            Recovery {
                symptom: "a command \"does not exist\"",
                fix: "You trusted training over the manifest. Re-run `curie schema` and use the confirmed spelling.",
            },
        ],
    }
}

fn tier_caption(tier: &'static str) -> &'static str {
    match tier {
        "init" => "0. Scaffold a bundle (Claude Code plugin shape + evals/cases.json seed)",
        "skill" => "1. skill tier: the runner alone, offline, fake model -- the fastest loop",
        "local" => "2. local tier: the same bundle through the full platform (compose), zero Slack",
        "cluster" => "3. cluster tier: the same bundle on Kubernetes (a Helm release)",
        other => other,
    }
}

/// Render the primer as Markdown. Every `curie ...` token printed here is a
/// real command; the drift test enforces that against the live manifest.
fn render_markdown(p: &Primer) -> String {
    let mut s = String::new();
    s.push_str("# Curie harness primer\n\n");
    s.push_str(p.summary);
    s.push_str(
        "\n\nYou are a coding agent driving this harness. This primer carries only what you \
         cannot derive from the command tree or your training data. Read it before you \
         start.\n\n",
    );

    s.push_str("## Verify-first (before every command)\n\n");
    s.push_str(p.verify_first.why);
    s.push_str("\n\n");
    for c in &p.verify_first.commands {
        s.push_str(&format!("    {c}\n"));
    }
    s.push('\n');

    s.push_str("## The parity ladder (the core loop)\n\n");
    s.push_str(
        "One immutable bundle and one evals/cases.json, run at three tiers. Climb only as far \
         as you need. A tier-to-tier divergence is the harness catching a real environment \
         bug, not your skill's logic.\n\n",
    );
    let mut last_tier = "";
    for r in &p.parity_ladder {
        if r.tier != last_tier {
            s.push_str(&format!("    # {}\n", tier_caption(r.tier)));
            last_tier = r.tier;
        }
        s.push_str(&format!("    {}\n", r.command));
        s.push_str(&format!("    #   {}\n", r.purpose));
    }
    s.push('\n');
    s.push_str(
        "The eval file never changes across tiers. If `skill eval` is green but a deployed \
         message misbehaves, the bundle is fine and the environment is not -- that gap is the \
         signal the harness exists to surface.\n\n",
    );

    s.push_str("## When / which\n\n");
    for d in &p.decision_logic {
        s.push_str(&format!("- **{}**\n  {}\n\n", d.question, d.answer));
    }

    s.push_str("## Approvals (a turn can end without a reply)\n\n");
    s.push_str(p.approvals.summary);
    s.push_str("\n\n");
    for f in &p.approvals.facts {
        s.push_str(&format!("- **{}**\n  {}\n\n", f.title, f.detail));
    }
    for c in &p.approvals.commands {
        s.push_str(&format!("    {c}\n"));
    }
    s.push('\n');

    s.push_str("## Landmines (non-discoverable)\n\n");
    for l in &p.landmines {
        s.push_str(&format!("- **{}**\n  {}\n\n", l.title, l.detail));
    }

    s.push_str("## Error -> recovery\n\n");
    for r in &p.recovery {
        s.push_str(&format!("- **{}**\n  {}\n\n", r.symptom, r.fix));
    }

    s.push_str("When anything is uncertain, confirm it against `curie schema` before you act.\n");
    s
}

/// The primer rendered to Markdown. One seam for callers that need the same
/// authored body the `curie guide` default prints -- the scaffold's harness
/// skill renders from this so the two can never diverge (D2 anti-drift).
pub fn primer_markdown() -> String {
    render_markdown(&primer())
}

/// `curie guide`: print the primer. Markdown to stdout by default; the global
/// `--json` flag prints the structured variant to stdout via the shared
/// machine-output path (any human text would go to stderr).
pub fn run() -> Result<()> {
    ui::ui().emit(&GuideOutput { primer: primer() });
    Ok(())
}

/// Output of `curie guide` (#474): the primer, structured under `--json` and
/// rendered as markdown otherwise, routed through the one `Ui::emit` point.
struct GuideOutput {
    primer: Primer,
}

impl ui::CliOutput for GuideOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.primer).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn render(&self, ui: &ui::Ui) {
        ui.payload_plain(&render_markdown(&self.primer));
    }
}
