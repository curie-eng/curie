# 287. Learned rules are a memory kind that applies only after a person accepts it

Date: 2026-11-18

Status: Accepted

Accepted before implementation under the normal path of
[`docs/adr/AGENTS.md`](AGENTS.md), not the coordinated exception of
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).
The repository owner approved this decision for acceptance on 2026-11-18,
recorded in the issue #5784. The realizing issues are #5795 to #5801 and
#5808, as mapped under "Realizing code paths". It partially amends ADRs 0167, 0188, 0278, 0119,
0162, 0187 and 0145, precisely as listed under "Accepted ADRs amended", under
the back-link rule in
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).

This decision targets `next` and authorizes the implementation named under
"Realizing code paths". It belongs to the epic #5783.

## Context

A reviewer comment on a factory pull request often states a rule that holds for
the whole repository: how errors are reported, which helper to use, what a test
must cover. Today that rule is applied once, in the revision that acts on the
comment, and is lost for the next ticket. The reviewer repeats it, or the next
pull request repeats the mistake.

The platform already has memory
([ADR 0167](0167-agent-and-channel-memory-are-written-by-the-agent-guided-by-editable-guidance.md)):
facts the agent writes with its own tools and editable guidance, loaded into the
prompt at session start, with the sandbox write path scoped by
[ADR 0188](0188-the-sandbox-memory-credential-is-scoped-to-its-own-channel.md).
Facts are untyped values with no state and no acceptance record, and they reach
prompts without a person deciding they should. That is acceptable for facts. It
is not acceptable for an instruction that shapes every later run, because the
text behind it is a reviewer comment, which is untrusted input.

The requester's personal coding instructions
([ADR 0278](0278-requester-coding-instructions.md)) already establish a
pinned, bounded, digest-recorded prompt section that ranks below platform,
bundle and repository rules, is delivered to the implementer only, and is
recorded on the pull request by the platform and not the model. A rule learned
from review is the repository's convention rather than a person's preference,
so it needs the same delivery discipline, a rank above personal preferences,
and a person's acceptance. The factory's own pull request lineage and the
execution scoped issue read capability
([ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md))
supply the identities such a proposal needs without trusting the model for any
of them. The unmerged twin memory design (ADR 0284 on the branch
`epic/self-improving`) shows a typed memory kind with an append only revision
log; it is prior art, not a dependency.

## Decision

1. A learned rule is a memory kind of its own, stored by the platform in its own table (`memory_rules`), owned by the agent that learned it, beside facts and guidance rather than inside the fact rows. Reason: facts are untyped JSON values with no state, no acceptance record and nothing admission can pin, and the relational guarantees a rule needs (states, one per pull request, an accepted-by person, a per scope cap) would otherwise be rebuilt by hand on every write path.
2. A rule has one of three scopes: `repository` (keyed by code host kind, host and immutable project id), `binding` (keyed by memory's own binding scope string) and `agent`. A factory rule is always `repository` scoped and never per developer. Reason: the project id survives renames and is the same whichever tracker the ticket came from; a binding address is not the repository for a ticket from a non-GitHub tracker.
3. A rule's lifecycle is `candidate`, `proposed`, `accepted`, then `removed`; `proposed` can instead become `rejected`; a `candidate` can instead become `superseded` or `discarded`. Only `accepted` rules are ever delivered to a model. Reason: nothing a model or a commenter writes applies until a person decides.
4. The factory decides that a reviewer comment states a general rule inside the revision run that acts on that comment, through a platform tool (`propose_rule`). Each execution request may store at most one hidden candidate, so a pull request revised several times can hold several candidates; at most one rule per pull request is ever promoted to a visible proposal. Reason: that run already holds the comment, the diff and the repository, so no second model call is needed, and it runs under the same model grant on service and personal installs.
5. A factory proposal is written as a hidden `candidate` tied to the execution request and its pull request lineage. When the pull request merges, in the same transaction as the terminal lineage write, the platform promotes the newest candidate of that lineage to `proposed` with the next rule number in its scope and marks every older candidate `superseded`; when it closes unmerged, it marks every candidate `discarded`. Superseded and discarded rows and their `supersede` and `discard` audit events are kept and never listed or delivered. Reason: a person sees a rule only for a change the repository actually took, at most one per pull request, and decides after the merge as the product flow shows.
6. The factory proposal travels on the execution scoped issue read capability of ADR 0187 (a sibling route of `/work-items/issue-read`), and the API derives the agent, scope, source request, source pull request, source comment URL and source person from the capability and the stored request, never from the model. The model supplies only the rule sentence and why it is general. Reason: the capability already names the execution, and a new runner boot switch would change a frozen contract.
7. A non-factory agent proposes through the same tool on the ADR 0188 per turn write credential, into its own binding scope or agent scope, at most one per turn, and its proposal is `proposed` at once because there is no pull request to wait for. Reason: any agent can learn standing rules the same way, with the same acceptance gate.
8. Proposals require an operator owned agent switch (`memory_rule_proposals`, off by default). Accepted rules apply whatever its value. Reason: an operator controls whether an agent may propose; turning proposals off must not silently drop rules people accepted.
9. Only a signed-in person acts on a rule: accept, reject and remove require a principal console session; the platform key and every sandbox credential are refused. For a `repository` rule the person's linked code host account must have write access to that repository, read from the code host at the time of the action, failing closed when the link, the account or the code host answer is missing. For `binding` and `agent` rules any active human principal of the agent's tenant decides, the same authority that already edits that agent's memory in the console. Reason: the people who can merge a repository own its conventions; an attacker with only comment rights cannot make a rule apply.
10. Every state change writes one audit event with the actor kind (agent, platform, principal), the actor and the time, in the same transaction, and every action is a compare-and-set on the rule's version. Reason: who decided and when is part of the record, and concurrent decisions must not overwrite each other.
11. At most 16 rules are accepted per scope, enforced when a rule is accepted. A rule is one line of at most 240 UTF-8 bytes with no CR, LF or NUL, and contains neither reserved line prefix (`Learned rules:`, `Requester preferences:`) nor the prompt section delimiters. Oversized or invalid input is refused, never truncated. Reason: a bounded, stable prompt section and a line that cannot forge platform text.
12. Accepted repository rules are resolved and pinned on the work item at its first admission, with a snapshot of the canonical text (`R<n>: <text>` lines) and its SHA-256, and every execution request of that work item uses that pin. Accept and remove therefore affect the next ticket, never a running one. Reason: a repair, resume or revision of one ticket must not change rules mid flight, and the pull request line must name exactly what was in force.
13. Pinned rules are delivered to the implementer only, by the worker as a versioned envelope beside the requester envelope of ADR 0278, verified by the runner against the bound execution and the digest, and acknowledged before any model work; a failed delivery fails the request before the model starts. Reviewer sessions never receive them. Reason: reviewers judge the repository contract independently, as ADR 0278 section 5 established.
14. Non-factory turns, which have no work item, load accepted agent and own binding rules once at session start through the read only boot credential. Reason: the same memory kind serves agents outside the factory without an admission step they do not have.
15. Order of authority, strongest first: platform safety, operator controls and mandatory bundle rules; the repository's own instruction files (every applicable scoped `AGENTS.md`, `CONTRIBUTING.md` and workspace `CLAUDE.md`); accepted learned rules for the repository; the requester's personal coding instructions; remembered facts, which are data. A learned rule outranks a conflicting requester instruction. The prompt section sits after the bundle prompt and model line and before the requester section, and states this order. A rule cannot add tools, skills, credentials, network egress, grants or budget, cannot change acceptance criteria, and cannot remove review or publication gates. Reason: a repository rule accepted by its maintainers is the team's convention, like a project file winning over a personal one.
16. The platform, never the model, writes one pull request line when the pinned rules were acknowledged as applied: `Learned rules: in force; rules:R<n>,R<m>; sha256:<digest>`, immediately before the `Requester preferences:` line. It names rule numbers and the digest, never rule text, and says "in force", never "followed". A submitted body carrying a line with that prefix is refused. Reason: the line records what applied without claiming model compliance or publishing rule text.
17. No new per-developer feature. The ADR 0278 requester coding instructions stay as they are, except that they now rank below accepted learned rules.

### Accepted ADRs amended (each gets only the ADR 0045 back link line; the amendment is stated here)

1. ADR 0167 (agent and channel memory), sections "The tools" and "What the platform stores and loads": memory gains a third kind, standing rules, stored outside the fact rows, with per item human acceptance and a `repository` scope; the tool list gains `propose_rule`. Facts, guidance and their tools are unchanged.
2. ADR 0188 (the sandbox memory credential), decision 1 "Scoped to its own agent and its own channel" and decision 4 "Authors are stamped by the API, from a per-turn credential": the per turn write credential may propose one rule per turn in its own binding or agent scope, with author and turn stamped by the API; it may never accept, reject, remove or read pending rules, and the read only boot credential may read only accepted rules of its own binding and agent scope.
3. ADR 0278 (requester coding instructions), section 4 "Preferences cannot override platform, bundle or repository rules": accepted learned rules are inserted between repository guidance and requester preferences. Section 6 "Record application through trusted execution and publication metadata": a second platform line, `Learned rules:`, placed before the `Requester preferences:` line, sourced from its own acknowledged evidence. Section 5 applies to learned rules unchanged.
4. ADR 0119 (prompt prefix rebuild), decisions 1 and 2: an execution prefix additionally contains the learned rules section, stable for one work item (pinned) or one session (loaded at start), and absent from reviewer context. No cache hit is claimed.
5. ADR 0162 (work items own durable execution identity), its Decision paragraph that the WorkItem "stores no issue body, acceptance criteria, dependency graph, readiness judgment, task verdict, or model judgment": a work item may carry a pin and a child snapshot of person accepted rule text. The snapshot is product memory, not issue content and not a judgment about the ticket.
6. ADR 0187 (the factory polls GitHub and the platform reads the issue), section "The platform reads the issue for the bundle": the execution scoped issue read capability also authorizes one sibling write, `/work-items/rule-proposal`, which stores a rule candidate and never tracker content.
7. ADR 0145 (a labelled issue is a backlog item), note on "Reading the ticket is the bundle's job" and "There is no polling loop and no task table": unchanged in substance. A rule is platform memory, not a task record or a copy of the ticket; the platform still parses no issue body.

## Consequences

1. Rule text stored by the platform is new product text written by a model and accepted by a person. It is not tracker content, not a copy of the ticket and not a task record, so ADRs 0145, 0162 and 0187 keep their stance that the platform stores no tracker content.
2. The reviewer comment body is never copied onto the rule. The rule keeps a link to the source comment and the platform recorded identity of the person who wrote it; reading the original means following the link.
3. An attacker with comment rights cannot make a rule apply: a comment only becomes a revision under the existing feedback authority, the model writes at most a candidate, the candidate is visible only after a merge, and nothing applies without an accept by a person whose linked account can write to the repository at that moment.
4. A judgment made inside the revision run is revealed only after the merge, as at most one proposed rule per pull request; candidates from earlier revisions of the same pull request are superseded, not shown. A pull request closed unmerged leaves no visible rule; its discarded candidates remain only as audit rows.
5. Every revision run of an agent with proposals on carries one more tool definition, and every pinned run carries one more bounded prompt section of at most 16 lines.
6. Removing a rule never changes a running ticket; the person who removes it sees "applies from the next ticket".
7. Acceptance of repository rules works for GitHub code hosts only until other code hosts have account linking. A repository on a host without an identity namespace (every host except GitHub today) can propose rules, but no one can accept them; the action is refused as unverifiable rather than allowed.
8. In service mode a person gets the code host link that acceptance needs from an operator or, where enabled, from self service linking ([ADR 0281](0281-personal-mode-refusals-offer-self-service-account-linking.md)).
9. Model compliance with a rule is a behaviour to evaluate, not a guarantee; executable authorization and publication checks stay authoritative.
10. Channel and agent facts (ADR 0167) still reach prompts without acceptance, exactly as before; this ADR changes nothing about facts. Only standing rules need a person's acceptance.
11. Write access for a repository rule is checked against the repository's immutable id: the platform resolves the scope's project id on the code host and confirms the id before reading the person's permission, so a renamed repository whose old path now names another repository grants nothing.

## Alternatives rejected

1. A factory-only rules table outside memory. Rejected: other agents (finance, IT and the like) could not reuse it, and learning would be entangled with the factory.
2. Writing rules into repository or bundle files by pull request. Rejected: the bundle usually does not live in the repository the factory works on and serves more than one repository, and a pull request per rule turns acceptance into a code review of prose.
3. Agent-written facts with no approval, as ADR 0167 facts work. Rejected for rules: a reviewer comment is untrusted input, and an instruction that shapes every later run must not apply because a model chose to save it.
4. A platform-started model call after the merge that rereads the pull request and proposes a rule. Rejected: it is the first completion the platform would start outside a sandbox, needs its own usage role, retries and a service model credential, and cannot run on personal plan installs.
5. Proposing per developer rules from a comment author's taste. Rejected by the product owner: the factory's learnings are the factory's, for everyone working in the repository.
6. Reusing the standing instruction kind of the unmerged twin memory design (ADR 0284 on `epic/self-improving`). Rejected as code: it applies an instruction as soon as a served user sets it and has no per item acceptance; its typed kind and append only revision log are cited as prior art.

## Realizing code paths

1. Storage, pins and the domain module: #5795 (`memory_rules`, `memory_rule_events`, work item pin columns and `work_item_memory_rule_snapshots`, `execution_requests.memory_rules_evidence`, `packages/curie-internal/src/curie_internal/memory_rules.py`).
2. Factory proposal, promotion, supersede and discard: #5796 (`/work-items/rule-proposal`, the `rule_proposal` field of the completion contract, `propose_rule`, `settle_lineage_rule_candidates` in both terminal lineage writers, the agent switch).
3. Acceptance with the write access check by repository id and audit: #5797 (`/agents/{agent_id}/memory/rules` routes, `curie cluster memory --rules`).
4. Pin, delivery, prompt section, reviewer exclusion and the pull request line: #5798.
5. The scripted chain proof with a recorded real model: #5799 (scenario `learned-rules` of `curie dev factory-e2e scripted`).
6. Console Memory page: #5800.
7. Non-factory proposals and session start load: #5801.
8. The live proof on a real GitHub repository with a live model: #5808 (scenario `live-github-chain` of `curie dev factory-e2e run`).
