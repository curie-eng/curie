# 281. Personal-mode refusals offer self-service account linking

Date: 2026-11-16

Status: Accepted

Accepted with maintainer approval from TheConnMan, recorded as an approving
review on the publishing pull request
([#5721](https://github.com/curie-eng/curie/pull/5721)).

This ADR builds on
[ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md),
[ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md),
[ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
and
[ADR 0268](0268-personal-subscription-execution-and-acceptance-contract.md).
It partially amends ADR 0268 decision 2, where that decision says no automatic
identity linking is implied, and ADR 0220 section 3, where only a tenant with a
directory-synced identity provider may issue a connect token. It realizes
ADR 0198 decision 8's `provider_event_verified` source for personal-mode Slack
and GitHub accounts. Realizing issues: #5718 and #5719.

## Context

A personal subscription (ADR 0268, epic #5370) runs factory work on the
initiating person's own Claude plan. Admission needs that person as a principal,
so the person's Slack account and GitHub account must each carry an active
identity link to the principal before a Slack mention or a trigger label can
start a run.

The refusals that answer an unlinked person exist today:

1. A personal Slack kickoff (`apps/api/src/curie_api/factory_slack_kickoff.py`)
   refuses `sender_unverified` when `identity.service.resolve_principal` does
   not resolve the sender, and `enrollment_inactive` when the resolved
   principal's enrollment is not active at a positive generation.
2. A GitHub label admission in personal mode refuses `personal_owner_required`
   when no owner is known (`apps/api/src/curie_api/personal_execution.py`).
   The issue comment `apps/api/src/curie_api/factory_intake_notices.py` writes
   for that code tells the reader that the operator links the account, or a
   linked person applies the label.

Only an operator can create those links. `POST
/identity/admin/{tenant_id}/code-host-links` in
`apps/api/src/curie_api/routers/identity_admin.py`, behind the platform key,
writes an `admin_mapped` GitHub link in the host's `github_host` namespace. No
API creates a Slack link at all. The live drivers seed one `admin_mapped` by SQL
as setup, as `tools/factory-e2e/README.md` records under "Personal Slack kickoff
acceptance".

The ADR 0220 section 3 connect flow exists in
`apps/api/src/curie_api/identity/connect.py` and
`apps/api/src/curie_api/routers/identity_connect.py`. `issue_for_sender` issues
a token only when resolution answers `no_link`. The token is single use, lives
10 minutes (`TOKEN_TTL`), is stored as a SHA-256 hash, and is bound to the
sender's namespace and native id. Issuance per account is spaced one minute
apart (`PROMPT_SPACING`) and capped at five live tokens (`MAX_ACCOUNT_TOKENS`).
The connect page sets `__Host-` prefixed, `Secure` cookies, and linking happens
only through a CSRF-bound `POST /identity/connect/{token}/confirm`. Confirm
refuses `account_already_linked` when the account already has an active link,
writes a `provider_event_verified` link, and records an
`identity.account_connected` audit row. The page names the account only by its
native ids, in the form "slack user U... in workspace T...".

`connect.configured_provider` gates all of this. It returns a provider only when
the tenant has governed email domains, a `managed_by` directory adapter, and at
least one `idp_group` team with a completed snapshot: a directory-synced
identity provider. Without one, `issue_for_sender` returns no token and the
button is never offered.

A personal-mode installation usually has no directory. Its Console signs people
in with plain OIDC (`CURIE_OIDC_ISSUER`), and `resolve_principal` in
`apps/api/src/curie_api/crud/console.py` creates the principal at first sign-in,
keyed by the issuer and subject. Such a tenant can never offer the connect
button, so every personal-mode user waits for an operator.

Four earlier decisions bound the answer:

1. [ADR 0268](0268-personal-subscription-execution-and-acceptance-contract.md)
   decision 2 reuses ADR 0166 principals and links and says "No new identity
   namespace or automatic identity linking is implied."
2. [ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md)
   decision 8 makes link creation the trust boundary: resolution trusts any
   link it finds, so a link needs evidence that the provider account is the
   one being linked and that the principal is entitled to it. Its two sources
   are `provider_event_verified` (a signed-in principal proves control of the
   provider account in the same flow) and `admin_mapped`. Its decision 6 says
   how a Slack sender's namespace and native id are derived from an event.
3. [ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md)
   says resolution never guesses, and never matches on email or display name.
4. [ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
   section 2 makes accounts the identity provider reports take precedence over
   connected links, section 3 defines the connect button, and its alternative
   5 rejects typed link codes.

Letting a person create their own link changes who may create a link and on
what evidence. That is the boundary ADR 0198 decision 8 sets, so it needs this
decision before #5718 and #5719 build it.

## Decision

**In personal mode, a refusal caused by a missing link offers the person a way
to link the account themselves. A Slack account is linked through the ADR 0220
connect flow, issued under the Console OIDC issuer when the tenant has no
directory. A GitHub account is linked through the factory GitHub App's user
authorization. Each link needs the person's explicit action and proof of control
of the account, links only an account no one holds, and records
`provider_event_verified`. The person can unlink their own accounts.**

### 1. Scope

These paths exist only when the installation runs
`FACTORY_MODEL_AUTH_MODE=personal` and Console OIDC is enabled with an `https`
redirect origin. The connect flow's cookies are `__Host-` prefixed and `Secure`,
and the GitHub callback carries an authorization code, so neither is offered
over plain HTTP. Service mode is unchanged: it issues no token and offers no
link under this ADR.

### 2. Slack: a refused sender gets a connect token

A personal Slack kickoff refused `sender_unverified` for a Slack account with no
active link (resolution reason `no_link`) issues an ADR 0220 section 3 connect
token. The token keeps every existing property: single use, 10 minutes, stored
hashed, bound to the sender's namespace and native id as ADR 0198 decision 6
derives them, and subject to the existing per-account spacing and cap.

When the tenant has no directory-synced identity provider, the token's issuer is
the Console OIDC issuer, so the person who opens it signs in through the same
OIDC sign-in the Console already uses. A tenant with a directory-synced provider
keeps today's rule and today's issuer. An account the identity provider reports
for a person still takes precedence over a connected link for the same account
(ADR 0220 section 2).

### 3. Slack: the confirmation names the account and links only an unlinked one

The confirmation page names the Slack account in words a person recognizes: the
Slack display name and the workspace name captured when the token is issued,
with the native ids beside them. It also shows this sentence, verbatim:

"Confirm only if this is your own Slack account. Anyone using it will start runs
on your Claude plan."

Linking needs an explicit confirm through the existing CSRF-bound POST. The link
is created only if that Slack account has no active link at confirm time. Its
source is `provider_event_verified` (ADR 0198 decision 8), and the existing audit
row records it with the acting principal.

The no-active-link rule is first come only, and that matters. Without it, a
second principal holding a token could silently take an account another
principal already holds, and that account's mentions would start billing a
different plan with no action from the first holder. With it, an account that
is linked stays linked to its principal until the current holder or an operator
revokes the link (decision 5); only then can it be linked again. The active-link
uniqueness of ADR 0198 decision 5 enforces the same rule in the database.

### 4. GitHub: the person links through the GitHub App's user authorization

A signed-in principal links a GitHub account through the factory GitHub App's
user authorization code flow.

1. The principal starts the flow from a Console session
   ([ADR 0083](0083-console-sessions-and-cli-minted-login-codes.md)). Curie
   creates a `state` held server-side, single use, valid 10 minutes, and bound to
   the principal session that started it.
2. GitHub returns the browser to Curie's callback with a code and the `state`.
   The callback requires the same principal session that started the flow;
   otherwise it refuses and creates no link.
3. Curie exchanges the code for a user token, reads the numeric user id from
   `GET /user`, revokes the user token at once, and never stores it. The token is
   used for that one read only.
4. The link is created in that host's `github_host` namespace, keyed by the
   numeric id (ADR 0198 decision 5), only if that GitHub account has no active
   link on that host. Its source is `provider_event_verified`, with an audit
   row.

GitHub is not exposed to the link-forwarding attack decision 7 accepts for
Slack, because the `state` is bound to the principal that started the flow and
the callback checks it. A victim who opens an authorization URL the attacker
started fails at the callback: the `state` belongs to the attacker's session,
not the victim's. An attacker who tricks a victim into authorizing (login CSRF)
cannot complete the link in the attacker's session, because GitHub delivers the
code to the victim's browser, which does not carry the attacker's session, and
the `state` and code are single use. A person who starts their own flow
authorizes the GitHub account their own browser is signed in to, which GitHub's
authorization page names. The user token is short-lived in Curie's custody
because it is revoked as soon as the id is read.

### 5. A person may unlink their own accounts

A principal may revoke its own active Slack and GitHub links from the Console.
Revocation follows ADR 0198 decision 5: the row keeps its evidence and gains
`revoked_at`, and the account may then be linked again. Operator revoke through
`POST /identity/admin/{tenant_id}/links/{link_id}/revoke` is unchanged.

Unlinking stops new admissions through that account. A run already admitted
keeps its owner under ADR 0268 decision 2; a person who wants such runs stopped
disconnects the enrollment, which ADR 0268 decision 3 says stops that owner's
runtimes.

### 6. Refusals point to the fix

1. A personal Slack kickoff refused for an unlinked sender (`sender_unverified`
   with `no_link`) or an inactive enrollment (`enrollment_inactive`) answers the
   sender privately, with an ephemeral message carrying a "Connect your Claude
   plan" button. For an unlinked sender the button opens the connect page for
   the token decision 2 issued. For a linked sender whose enrollment is
   inactive there is nothing to link, and the button opens the Console's Claude
   plan page.
2. A GitHub label refusal comment for an unlinked labeller
   (`personal_owner_required`) or an inactive enrollment (`enrollment_inactive`)
   links the Console's Claude plan page with no token, because issue comments
   are public and anyone who read a token there could use it. The person signs
   in there and links the account through decision 4 or enrolls a plan.

### 7. Accepted risk: Slack link-forwarding

The Slack connect token is bound to the Slack account, not to a principal. The
proof of control is the Slack event itself, which arrives before anyone has
signed in to Curie, so there is no principal session to bind the token to at
issue. Whoever opens the link, signs in and confirms becomes the principal the
account is linked to.

That allows link-forwarding. An attacker mentions the bot, receives their own
connect link privately, and sends it to a victim. If the victim signs in and
confirms, the attacker's Slack account is bound to the victim's principal, and
the attacker's mentions then start runs on the victim's Claude plan. This ADR
accepts that risk.

The attack has limits. It cannot bind the victim's Slack account to the
attacker: a token for the victim's account is only ever shown privately to the
victim, and the no-active-link rule refuses any account already linked. It
cannot read the victim's grant, which stays inside the ADR 0268 decision 4
custody boundary; a link carries start authority, not token bytes. The attacker
also needs a Slack account in a namespace the tenant holds, so it is someone
who can already message the bot.

The mitigations are decision 3's wording, which names the attacker's Slack
account in recognizable words beside the warning sentence; the explicit
confirm; the no-active-link rule; single use and the 10 minute expiry; and the
per-account rate limit on issuance. The recovery is Console unlink (decision 5).
The audit row shows the link, its `provider_event_verified` source and the
confirming principal, and the Console lists the principal's active links, so an
unfamiliar Slack account is visible to the person it bills.

## Consequences

1. #5718 builds Slack self-linking and its refusal surface: decision 2's
   issuance in personal mode under the Console OIDC issuer when the tenant has
   no directory-synced provider; capture of the Slack display name and
   workspace name at issue, stored with the token; decision 3's confirmation
   page and sentence; decision 6 item 1's ephemeral answer with the
   "Connect your Claude plan" button; and decision 6 item 2's comment pointing
   at the Console's Claude plan page.
2. #5719 builds GitHub self-linking and unlink: decision 4's flow and decision
   5's Console revoke of the principal's own Slack and GitHub links.
3. Operators keep the `admin_mapped` route and operator revoke. Self-service
   adds a source of links in personal mode; it removes none.
4. The SQL seeding of the Slack link in the live drivers can later be replaced
   by the decision 2 and 3 flow.
5. Directory-synced tenants and service mode behave as before.
6. Negative: the link-forwarding risk of decision 7 is accepted, not removed. A
   person who confirms a link they did not start pays for another person's runs
   until they unlink it.
7. Negative: the factory GitHub App must have user authorization enabled with a
   registered callback URL, and Curie needs the App's client credentials to
   exchange and revoke user tokens. An installation without them cannot offer
   decision 4.
8. Negative: personal mode now needs a second `https` origin requirement. Besides
   the Console OIDC redirect, the GitHub App's callback URL must be an `https`
   URL on the Console's origin.
9. Negative: issuing a Slack token now reads the sender's display name and the
   workspace name from Slack, one more provider call on the refusal path.

## Alternatives considered

1. **Operator-only linking.** Rejected. It blocks self-service: every new
   personal-mode user waits for an operator before their first run, which is
   the problem this decision exists to remove.
2. **Match the Slack or GitHub email to the principal's email.** Rejected.
   ADR 0166 forbids matching on email or display name, and a chat or GitHub
   profile email is user-editable unless an identity provider controls it
   (ADR 0220 alternative 7).
3. **Slack's own OpenID sign-in as a second identity provider.** Rejected. A
   principal is keyed by one issuer and subject, so a Slack sign-in creates or
   finds a different principal rather than proving that the Console principal
   controls the Slack account. It would add a second sign-in to configure and
   would still need a link between the two.
4. **Typed link codes.** Rejected for the reason ADR 0220 alternative 5 gives:
   the friction of pasting a code, when a button that opens a sign-in proves the
   same two facts.
5. **Link a label actor automatically on first use.** Rejected. A label event
   proves which GitHub account applied the label, but nothing proves that any
   principal controls that account. ADR 0198 decision 8 needs evidence on both
   sides, and this has none on the principal's.
6. **Keep the GitHub user token.** Rejected. Curie needs the numeric id once.
   A stored user token, with its refresh token, is an unneeded long-lived
   secret that could act on GitHub as the person.
7. **Relax the directory gate for every tenant.** Rejected as wider than the
   need. Only personal mode needs self-service linking; directory-synced tenants
   already have ADR 0220's model, and service mode has no per-person billing at
   stake.

## Reference

1. Issue: #5712.
2. Realizing issues: #5718 (Slack self-linking and the refusal surfaces) and
   #5719 (GitHub self-linking and unlink).
3. Epic: #5370.
4. Related ADRs:
   [ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md)
   (back links on ADRs 0268 and 0220),
   [ADR 0083](0083-console-sessions-and-cli-minted-login-codes.md),
   [ADR 0166](0166-tenant-boundary-and-principal-identity-land-together.md),
   [ADR 0198](0198-identity-links-live-in-provider-identity-namespaces.md),
   [ADR 0220](0220-one-identity-provider-decides-who-may-invoke-and-approve-on-every-channel.md)
   and
   [ADR 0268](0268-personal-subscription-execution-and-acceptance-contract.md).
