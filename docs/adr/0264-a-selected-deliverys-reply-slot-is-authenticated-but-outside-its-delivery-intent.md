# 264. A selected delivery's reply slot is authenticated but outside its delivery intent

Date: 2026-11-02

Status: Draft

## Context

On the legacy path the dispatcher acknowledges a mention by posting a
placeholder reply and gives the worker that message as the slot to edit into
the answer. The selected HTTP receiver submits with an empty reply slot, so a
person who mentions an agent under HTTP identity sees nothing until the worker
posts its answer as a new message.

Adding the placeholder to the selected path meets two delivery facts. The
receiver's request is signed, and the API records a receipt for each native
delivery with a digest of its intent, so a repeat with the same intent is a
duplicate and a different intent under the same delivery id is a conflict.
And one delivery can be sent more than once for two different reasons:

- the dispatcher retries an identical request after losing the API's
  response. The first attempt may already have been accepted, with this
  attempt's placeholder as its slot;
- Slack redelivers the event. The dispatcher handles it as new work and posts
  a fresh placeholder, so this attempt's slot differs from the accepted one.

If the slot is part of the signed intent, a redelivery has a different intent
and is refused as a conflict, so the person sees an error for a turn that is
running. If the dispatcher deletes its placeholder whenever the API answers
duplicate, a retry of an accepted request deletes the very slot the worker
will edit, and the answer has nowhere to land.

## Decision

### 1. The selected path posts a placeholder

After the receiver's local checks pass and before submission, the dispatcher
posts the same placeholder the legacy path posts and submits its message
timestamp as `reply_ref`.

### 2. The reply slot is authenticated, not intent

`reply_ref` stays inside the signed request, so the API knows the receiver
named it. The delivery intent digest that a receipt records is computed by
the API over the immutable delivery only: receiver, binding, native delivery
id, sender evidence, text and attachments. It excludes `reply_ref`. A
redelivery of the same event is therefore the same intent.

The API carries a nonempty `reply_ref` into the queued turn as its reply
placeholder, inside the sealed payload like every other field, so the worker
edits the answer into it exactly as on the legacy path. An empty `reply_ref`
keeps the fresh-reply path.

### 3. A duplicate names the accepted slot

The receipt records the accepted turn's `reply_ref`. A duplicate response
returns it. A different intent under the same delivery id is still a
conflict.

### 4. The dispatcher deletes only a placeholder the accepted turn does not own

On a duplicate, the dispatcher compares the returned slot with its own:

- equal: this attempt's placeholder is the accepted turn's slot, so it is
  kept;
- different: the accepted turn already has a slot, so this attempt's
  placeholder is deleted;
- absent, or any response that is not a duplicate with a slot (pending,
  unavailable, timeout): the placeholder is kept. A placeholder is never
  deleted without proof that another one owns the turn.

A refusal replaces the placeholder with the fixed notice, as on the legacy
path.

## Consequences

A person sees the same acknowledgement under HTTP identity as on the legacy
path. A Slack redelivery is recognised as the same delivery and leaves one
placeholder and one answer. A lost response followed by a retry leaves the
accepted turn's slot in place.

An attempt that is kept without proof may leave a stray placeholder when the
outcome is never learned. That is the safe direction: a stray acknowledgement
is visible and harmless, while deleting the accepted slot loses the answer.

The receipt grows by one field and the duplicate response by one field. An
older dispatcher that ignores the returned slot keeps its current behaviour.

## Alternatives considered

1. **Keep the selected path without a placeholder.** Leaves HTTP identity
   with a worse experience than the legacy path for no security reason.
2. **Sign the slot as part of the intent.** Rejected: every Slack redelivery
   becomes a conflict and is shown as a refusal.
3. **Delete the placeholder on every duplicate.** Rejected: a retry of an
   accepted request deletes the slot the worker edits.
4. **Let the worker fall back to a new message when its slot is gone.**
   Hides the deletion rather than preventing it, and still leaves the person
   with a vanished acknowledgement.
5. **Deduplicate redeliveries in the dispatcher before posting.** Needs
   shared state across dispatcher replicas, and the API's receipt already is
   that state.
