# 260. HTTP identity admits an email sender only on a signed email receiver proof

Date: 2026-11-02

Status: Draft

## Context

An email binding is fed by a mail adapter outside Curie. The adapter receives
a message, applies its own message authentication, and posts the turn to the
channel ingress with a channel token and an `author` field naming the sender
address. On the legacy path the API takes `author` as given: whoever holds a
channel token for the binding speaks for any sender.

Under ADR 0254 decision 2, an email binding whose agent's invocation role is
migrated or filtered requires HTTP identity. The ingress then demands a
receiver proof, and only the Slack dispatcher can sign one, so every email to
such an agent is refused. An agent cannot apply the same identity-provider
rules to email that it applies to Slack.

The tempting fix is to accept the channel token as the email source's
authority and derive the sender from `author`. That makes the sender as
strong as the weakest holder of a channel token. A channel token is minted by
any holder of the platform key, so any such holder could name any address in
a trusted domain and receive that person's identity facts and permissions.
The Slack path does not have this weakness: its sender comes from evidence
signed by a receiver key that the platform key cannot produce. Email needs
the same separation.

## Decision

### 1. An email receiver is registered with its own key

The protected API runtime file gains an optional list of email receiver
registrations. Each registration names:

- its tenant and the email binding ids it covers;
- an operator-chosen stable channel identity id and provider installation id,
  because email bindings have no channel identity or installation rows;
- one to sixteen trusted sender domains, each a namespace descriptor of kind
  `email_domain` with a lowercase key;
- an email receiver key: an operator-provisioned signing key, distinct from
  the platform key, every channel token and every Slack receiver key, held
  only by the API and the mail adapter.

Keys, domains and ids are unique across the file. A defective list refuses
API startup like any other runtime defect.

### 2. The mail adapter signs what it authenticated

For each message, the adapter signs an email receiver proof over:

- the registration and the email binding id;
- the sender address after lowercasing, exactly one address with one `@`;
- the message's native delivery id (its `Message-ID`);
- the SHA-256 of the exact turn body bytes it submits.

The proof travels with the turn. The adapter signs only after its own message
authentication passes; how it authenticates mail stays outside Curie.

### 3. The API verifies the proof before it reads a sender

For an email binding that requires HTTP identity, the ingress requires a valid
channel token for the binding, exactly one email registration listing it, and
a proof that verifies under that registration's key. The proof's binding,
address and body digest must equal the submitted binding, the lowercased
`author` and the received body bytes. Any mismatch, or a missing proof, is an
invalid receiver refusal before any provider lookup. A channel token alone,
whoever minted it, never names an email sender.

The domain after the `@` must be one of the registration's trusted domains.
An unlisted domain is a caller refusal before any provider lookup, so an
address outside the trusted domains is never sent to the provider.

### 4. Everything after the sender is the shared path

The sender is `{provider: "email", channel_identity_id,
provider_installation_id, namespace, native_id: <lowercased address>}`. The
same current-authority composition, role evaluation, sealed context, worker
current check and delivery receipts apply as for a Slack sender. The native
delivery id is the delivery identity, so a resubmitted message is a duplicate,
not a second turn. The sealed context records the email registration; the
worker reconstructs the sender only from that registration and refuses a
namespace it does not list.

### 5. An installation may register email receivers only

The runtime file must register at least one receiver of either kind. A file
with email receivers and an empty Slack receiver list is valid, and so is the
reverse. A file with neither is refused.

### 6. What does not change

Email bindings of agents that do not need HTTP facts keep the legacy path,
and a registration listing an email binding does not by itself make that
binding require HTTP identity. Approval answers by email reply are not added.
A Slack receiver proof on an email turn is refused.

## Consequences

Holding the platform key no longer lets a caller name an email sender on a
binding that requires HTTP identity. The residual trust sits with the email
receiver key and the adapter that holds it, the same shape as the Slack
receiver key.

Operators provision and rotate one more key per email receiver, through the
same coordinated process replacement as every other registration change.

The mail adapter changes: it must hold the receiver key and sign each
submission. An adapter that does not keeps working for agents on the legacy
path and is refused for agents that need HTTP facts.

## Alternatives considered

1. **Trust the channel token's holder to have authenticated the sender.**
   Rejected: anyone who can mint a channel token, which includes every
   platform key holder, could name any sender in a trusted domain.
2. **Verify message authentication results (SPF, DKIM, DMARC) in the API.**
   Moves mail parsing and DNS-dependent checks into the API and still needs a
   way to prove the headers belong to the submitted turn. A signed proof from
   the adapter that already authenticates mail is smaller and matches the
   Slack receiver.
3. **Sign only the sender address.** A captured proof could then be replayed
   with a different body. Binding the body digest and the native delivery id
   ties the proof to one message.
4. **Reuse a Slack receiver key for email.** One compromise would reach both
   channels, and a Slack key's scope is a Slack installation, not a mail
   domain.
5. **Keep requiring at least one Slack receiver.** Blocks an installation
   that uses HTTP identity only for email, for no security gain.
