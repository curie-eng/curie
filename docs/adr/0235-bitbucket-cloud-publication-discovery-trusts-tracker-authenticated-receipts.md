# 235. Bitbucket Cloud publication discovery trusts tracker-authenticated receipts

Date: 2026-10-19

Status: Draft

## Context

After factory storage loss, a still-labelled Jira issue can open a duplicate
Bitbucket Cloud pull request. [#4629](https://github.com/curie-eng/curie/issues/4629),
implemented by [#4653](https://github.com/curie-eng/curie/pull/4653), suppresses
duplicates on GitHub, GitLab and Bitbucket Data Center. Cloud remains a gap:
`apps/api/src/curie_api/forges/bitbucket_cloud/code_host.py` lists publication
branches with `owner_verified=False`, and `factory_rediscovery.py` discards them.
Repository, project and workspace access tokens cannot identify themselves
through `GET /2.0/user`, which answers 403. Data Center instead reads its
authenticated numeric identity from the `X-AUSERID` response header and compares
it with the PR author. That mechanism does not establish Cloud identity.

A repository writer can copy a publication branch name, issue marker or PR
body. Treating those as factory ownership would let that writer suppress work.
Learning the author of an arbitrary discovered PR would make the same mistake.
The decision concerns evidence of publication provenance, not permission to
write to the repository or resume lost work.

[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
keeps tracker and code-host identities separate. Jira's tracker verifies its
site scope and configured account before writing marked status comments and
remote links. `forges/jira_cloud/comments.py` selects comments by that account
and a `curie.factory` property; its body digest is a change detector, not a
signature. `forges/jira_cloud/tracker.py` writes a remote link keyed by PR URL,
but that write does not prove that a later remote-link read authenticates its
creator or last editor. Neither existing surface is yet a recovery receipt.

[Draft ADR 0233](0233-factory-publication-recovery-requires-explicit-operator-adoption.md)
separates automatic duplicate suppression from explicit operator adoption of
lost execution lineage. [ADR 0234](0234-factory-owned-pull-requests-accept-authorized-feedback-without-a-mention.md)
requires owned lineage for review continuation; discovery alone supplies none.
[#4747](https://github.com/curie-eng/curie/issues/4747), tracked for v0.13.2,
deliberately implements only the documented Cloud limitation and issue-scoped
operator-confirmation refusal. A lookalike can cause that conservative refusal,
but must not become a verified factory publication.

## Decision

**For Bitbucket Cloud paired with Jira, recommend a tracker-authenticated
publication receipt as evidence for automatic duplicate suppression. The
receipt names one immutable issue and one immutable repository and PR, and is
written by the verified factory tracker account only after the platform proves
its own publication. It grants no continuation authority. Missing or
unverifiable evidence retains operator-confirmation refusal.**

Upon acceptance this would amend ADR 0233's discovery correlation rule for
Bitbucket Cloud only: independently verified tracker provenance may establish
that a particular PR is a factory publication without identifying the current
code-host token's account. It does not supersede ADR 0233's explicit adoption,
exact-head baseline, cancellation or bounded continuation requirements. It
preserves ADR 0197's two ports and separate identity namespaces, and ADR 0234's
owned-lineage requirement. This Draft changes no Accepted ADR and authorizes
neither implementation nor acceptance of ADR 0233.

### What proves a publication

1. Create a versioned receipt in a dedicated marked Jira comment, separate
   from mutable progress prose. It binds tracker kind, immutable scope and
   issue id; code-host kind, immutable repository UUID and PR id; validated PR
   URL; publication branch, base and creation head; and the author account id
   observed in the authenticated PR creation response. Names and issue keys
   remain display data. The receipt attests to this PR only, not to every PR
   sharing that author. Another installation using the same tracker account
   shares this trust domain; the receipt cannot recover the old owning agent.
2. Write only from a platform publication operation whose surviving local
   approval and execution records and successful authenticated creation response
   prove the association. Merely finding a PR with matching branch, body and
   head, including after an uncertain create response, cannot mint this receipt.
   Such cases need evidence repair or explicit operator confirmation. Never
   issue a receipt from arbitrary tracker text, a discovered candidate, or the
   rediscovery refusal comment itself. Existing generic status comments do not
   acquire this stronger meaning retrospectively.
3. Recovery authenticates Jira's current site scope and configured account,
   reads the complete relevant comment set, and re-reads the selected comment
   with its current body, properties, author and last editor. Require the
   verified account as both author and editor, a supported receipt schema,
   consistent content and properties, and the exact immutable issue identity.
   A marker or stored digest alone proves nothing. Reject malformed, missing,
   contradictory or unverifiable provenance. Tracker administrators and anyone
   able to act as the factory tracker account are trusted in this design;
   it does not defend against their compromise or concealed edits.
4. Read the named PR directly through CodeHost, resolving the configured
   repository by UUID and validating the URL against that returned object.
   Confirm repository and PR ids and the recorded author. Do not follow
   arbitrary URLs from comments or infer ownership from matching display names.
   Branch and body are discovery hints, not authority; their subsequent edits
   cannot redirect the receipt to another issue or PR. A changed head remains
   subject to adoption's separate exact-head checks.
5. A verified open PR suppresses duplicate admission and publication and reports
   its link. Conflicting receipts or multiple open matches report ambiguity.
   An unreadable or partial tracker or forge read defers without advancing the
   cursor. With no valid receipt, an issue-matching open publication candidate
   keeps #4747's confirmation refusal; absence of a receipt never upgrades an
   unverified candidate to clear. This decision adds no terminal-trigger replay
   policy and leaves other hosts' verified rediscovery unchanged.
6. Remote links may locate candidates, but are not ownership evidence unless
   the provider exposes verifiable author and edit provenance equivalent to
   the receipt contract. The existing URL-keyed remote link is insufficient.
   A marked status comment qualifies only through the new receipt protocol,
   not because its current prose happens to contain a PR URL.

### Loss, replacement and tampering

1. **Total local storage loss:** the receipt and current PR survive externally.
   Reinstalled configuration must independently restore the same Jira scope,
   trusted tracker account and allowed repository. No local learned identity
   or signing secret is needed. The result suppresses duplicates only; no
   WorkItem, approval, feedback cursor or execution budget is reconstructed.
2. **Token replacement:** replacing a Jira token for the same verified account
   preserves receipt verification. Changing that account makes old receipts
   untrusted until an authenticated operator explicitly re-establishes their
   provenance; do not infer equivalence or accept a list of former accounts
   from tracker content. Replacing a Bitbucket token, even if its author
   identity changes, does not invalidate a receipt for an older PR: compare
   its recorded author with that PR, never with a newly learned global author.
   Current read access and repository binding must still verify.
3. **Key rotation:** this option introduces no publication signing key and
   does not depend on the release signing key. Credential rotation follows
   the account rules above. Rotating release keys has no effect on receipts.
4. **Repository writer forgery:** copied branches, bodies, commit trailers,
   URLs or comments by another Jira account cannot produce a valid receipt.
   A writer can still provoke #4747's visible confirmation refusal or mutate a
   genuine publication; neither grants continuation or changes the attested
   PR identity. Compromise of the trusted tracker account is a separate threat.
5. **Deleted or edited tracker evidence:** deletion, missing properties,
   inconsistent content, an untrusted last editor or unreadable edit provenance
   removes automatic proof and retains confirmation refusal for a surviving
   candidate. Do not repair evidence by copying a remote link or rewriting the
   receipt from that candidate. A valid update by the trusted tracker account
   must follow the same publication-proof rules. Jira retention and edit
   permissions become operational prerequisites, not an immutable audit log.

## Consequences

1. Automatic suppression can survive local storage loss without granting a
   repository writer ownership authority or creating a shared signing secret.
   Trust expands to authenticated Jira receipt provenance and the tracker
   account's custody. Tracker administrators remain inside that boundary.
2. Older PRs without receipts and PRs created before a failed receipt write
   still need operator confirmation. Publication and receipt writing span two
   services and cannot be atomic. Persist retries while storage survives and
   expose a pending or failed receipt; never claim recovery coverage until
   the external receipt has been read back and verified.
3. Extra tracker reads consume rate-limit budget. Partial pagination, identity
   failures and permission loss become visible deferrals, not an empty queue.
   The operator sees whether a candidate is verified, ambiguous, awaiting
   confirmation or unreadable, together with its validated link and reason.
4. Suppression is not adoption. Restoring feedback handling or CI repair still
   needs ADR 0233's explicit operator grant, current configuration and exact
   head proof. A receipt cannot recover cancellation, denied approvals, agent
   ownership, consumed budgets or whether previous feedback was handled.

## Alternatives considered

1. **Learn the publishing author once from a PR the factory created.** A
   successful authenticated creation response can establish that PR's author
   without `GET /2.0/user`; learning from a discovered PR cannot. A local record
   disappears with total storage loss. Backing it up separately shifts the
   recovery requirement to another store, and an account may publish for
   several installations. Token replacement may change the author, so accepting
   a new one must not silently bless older candidates. There is no signing-key
   rotation dependency, and tracker comment deletion is irrelevant if the
   record survives elsewhere. An ordinary writer cannot forge provider author
   identity, but author equality alone does not prove this issue association
   when that author's editable PR body is changed. Retain the observed author
   as receipt data, not as the sole global ownership rule.
2. **Trust the factory's existing tracker status comment or remote link.**
   External evidence survives local loss and same-account token replacement,
   needs no release key, and a repository-only writer cannot impersonate its
   Jira author. However the current marked-comment reader does not verify
   last-editor provenance, and a remote-link write through a verified account
   does not authenticate a later read. Edits or deletion can remove or redirect
   the association. Recommend the constrained receipt protocol above; reject
   treating any old bot-authored URL or URL-keyed link as sufficient proof.
3. **Sign a PR-body marker or commit trailer with a release key.** A signature
   over immutable issue, repository and PR identities could survive storage
   loss and token replacement if its verification keys survive. A repository
   writer could delete the marker but could not forge it; binding the PR id
   prevents copying it onto another PR, and tracker edits would be irrelevant.
   Rotation needs retained historical verification keys and an explicit
   revocation policy: compromised keys cannot remain recovery authority merely
   because a signature verifies. PR ids are known only after creation, adding
   another non-atomic write. A public release contains no private signing
   authority, and distributing a release private key to installations would
   let any holder forge receipts for every installation. An online signer or
   separately backed-up installation key and trust anchor adds a new service
   or disaster-recovery dependency. Reject coupling publication provenance to
   release signing and introducing that key custody for this gap.
4. **Keep refusing and require explicit operator adoption only.** This is the
   safe fallback after total loss, account or token changes, missing keys and
   edited or deleted tracker evidence. Branch and body forgery grants no
   authority, though it can force operator attention. #4747's confirmation
   refusal is the interim behavior; confirmation is not itself ADR 0233's
   bounded continuation grant. Keep this path for unprovable cases, rather
   than require a human for every valid externally attested publication.

## Realizing paths and validation

The proposed implementation belongs at `publication_pulls.py` and the
publication completion routes, with durable receipt-write retry state beside
publication persistence; `factory_notices.py` exposes delivery failure without
turning ordinary progress text into authority. Extend `forges/ports.py` and
`forges/types.py` for typed provenance observations, and
`forges/jira_cloud/tracker.py`, `comments.py` and `identity.py` for receipt
writing and strict read verification. Keep Cloud PR and author validation in
`forges/bitbucket_cloud/code_host.py`. Combine independently authenticated
observations in `factory_rediscovery.py`, consumed by
`factory_tracker_intake.py` and the pre-publication guard. All paths are under
`apps/api/src/curie_api/`; these are proposed responsibilities, not implemented
behavior. Do not set `owner_verified=True` from a branch or PR body alone.

#4747 remains separately tracked interim implementation and does not implement
this proof. Before acceptance, name a separate implementation issue for the
receipt protocol and its operator projections; neither #4747 nor this Draft
authorizes that work. ADR 0233 adoption remains separate work as well.

That implementation must drive the real publication and intake boundaries
through API, worker, Postgres and Valkey, with external-provider fixtures and
required live Jira and Bitbucket Cloud proof. Show that a created PR, verified
receipt and emptied local storage suppress another PR at both guards. Controls
must include a writer's copied branch and body; a copied receipt under another
author; edited, deleted, malformed and missing evidence; partial pagination;
rate limits; wrong scope or repository; multiple open PRs; uncertain creation
and failed receipt writes; same-account token replacement and changed accounts;
and a changed PR head that suppresses duplication but cannot resume work.
Verify that rediscovery writes no receipt or execution authority and that the
other hosts' behavior is unchanged. No runtime proof is claimed by this Draft.
