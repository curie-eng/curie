# 265. A file-bearing Slack message's sender team may come from the sender's own files

Date: 2026-11-02

Status: Draft

## Context

HTTP identity attributes a Slack sender only when the payload names the
sender's team, or, on a Grid receiver, the organisation. A message that
carries files can arrive with neither `user_team` nor `team` on its event,
while each file object names its uploader (`user`) and the uploader's team
(`user_team`). No such delivery has been captured in this repository yet;
the shape is what Slack's file objects document, and it has to be confirmed
by a capture before this ADR is accepted.

If it holds, a person who attaches a file is refused as `subject_unidentified` where
the same person without the file is admitted. The information needed is in
the payload, but only on the files.

## Decision

### 1. Fill the team only from the sender's own files

The shared Slack evidence copier fills the event's sender team from the files
only when all of these hold:

- the event names a nonempty user;
- the event carries neither `user_team` nor `team`;
- `files` is a nonempty list of at most 100 objects;
- every file's `user` equals the event's user;
- every file carries the same nonempty `user_team`.

The filled value is that common `user_team`. Otherwise the evidence is
exactly what it was before.

### 2. Nothing else changes

Nothing else is copied from the files, and enterprise signals are collected
as before. Every attribution rule (ordinary, Grid, Slack Connect, direct
message) reads a filled team exactly as it reads a native one, so trust,
namespace and refusal reasons are unchanged. An event that already names a
team keeps its own value whatever its files say. Every consumer of the shared
copier, from admission to the selected receiver, sees the same evidence.

## Consequences

A person who attaches their own file is attributed as they are without it.
A file shared by someone else, files from two teams, or a file without a team
cannot supply one, so the rule cannot attribute a sender from another
person's evidence.

If a captured delivery shows a different shape, the rule still only fills
when every condition holds and otherwise leaves the refusal in place.

## Alternatives considered

1. **Refuse file-bearing messages without a team.** Keeps today's behaviour,
   where attaching a file makes an admitted person look unidentified.
2. **Use any file's `user_team`.** A message can carry a file shared by
   someone else, whose team is not the sender's.
3. **Look the sender's team up from Slack with a users API call.** Adds a
   network call and a token scope to every unattributed delivery, and reads
   current state rather than what the delivery said.
