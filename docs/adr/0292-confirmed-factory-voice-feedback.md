# 292. A voice draft requires confirmation before it can steer factory work

Date: 2026-11-23

Status: Draft

## Context

Factory feedback can start a revision on an existing WorkItem. Speech recognition can lose an identifier or reverse the meaning of an instruction by dropping a negation. The person who submitted a recording needs to see the words, correct them and confirm the target before those words enter execution.

Slack file access and GitHub attachment access have different credentials, source identities and audiences. A transcript can disclose as much as the original recording. Its visibility must follow the original message throughout recognition, editing and delivery.

## Decision

1. The source adapter verifies the author, the conversation, the attachment and the WorkItem under the ordinary feedback policy. It streams bounded bytes to the API. The API hashes the bytes and reserves a pending draft in PostgreSQL. It retains the identifiers and hashes needed to re-read the source. The adapter can acknowledge intake before recognition.
2. A source-specific background consumer leases a pending draft and reopens its source through the adapter. Core checks the current bytes against the stored digest. A bounded child process decodes supported local media and runs the pinned English whisper.cpp model on one CPU thread. The operator supplies the weights. The API verifies their digest. Recognition defaults off. The accepted media pairs are PCM WAV, MP3, MP4/AAC and WebM/Opus. Support for a container says nothing about which events a provider delivers.
3. The adapter shows the resulting text and versioned draft code in the original conversation under current authority. An edit returns a receipt with the draft ID and schedules durable delivery of the replacement text and version. The author may reject the draft or confirm the displayed version. A verified change to the source invalidates the draft. Expiry defaults to 15 minutes from intake and survives edits. A periodic purge removes expired transcripts and source details even when the feature is turned off.
4. On confirmation, the adapter re-reads the canonical source and the confirming event before core locks the draft. Core checks the actor, audience, target, media, version and expiry before calling the existing steering ingress. The ingress rechecks database authority in the caller's transaction. A successful transaction records the steering receipt and closes the draft together. A retained tombstone scopes replays to the original actor, tenant, audience and version. The receipt distinguishes queued guidance from applied guidance.
5. The local worker reads the model by verified path and audio by pipe. Decoding, inference and callback failures return bounded reason codes. Operators can enable recognition by mounting a pre-populated read-only model claim in the API. The product shares neither weights nor raw audio through its images, queues or notification outboxes.

## Slack

The Slack source adapter owns this part of the design.

Socket Mode binds a clip to the receiving app and workspace, original message, sender and file. The worker queues a marker and keeps media and edits out of turn text. The API uses the signed gateway for a fresh thread read and bounded file retrieval, and displays the transcript only in the verified original thread. Native event shape and download fields with an app mention still require provider verification.

## Consequences

The operator must provision the pinned weights and budget CPU and memory for the API child. The child uses one CPU, a 2 GiB address-space limit and a 180-second wall deadline including a 30-second decode limit. The purge scans once per minute and retains terminal keys and confirmation receipts indefinitely. Downloads, retries and edits cannot compound into multiple work admissions for one original message. A callback can be repeated after a lost delivery acknowledgement. Source adapters deduplicate a prompt by its draft and version where the provider supports it.

The WAV fixtures and local transcodes exercise decoding, pauses, silence, negation and identifiers. These fixtures establish observed behavior for the supplied voices. Provider ingress and the resulting change on the original PR require separate evidence.

## Alternatives

Calling a hosted transcription API would send audio outside the installation and introduce a metered dependency. Treating spoken words as immediately actionable would allow a recognition error to spend a revision before the author saw it. Keeping audio in the database would simplify retries but would also create a second recording beyond the provider's own retention policy. Re-reading a small, bounded source gives the worker a fresh authority and content check on retry.

## Acceptance

Maintainer approval is pending. On acceptance, record the maintainer, date and approval reference. The implementation lives in `apps/api/src/curie_api/factory_voice.py` (drafts and admission), `apps/api/src/curie_api/factory_voice_child.py` (offline recognition) and `charts/curie/templates/api.yaml` (model mount). [Implementation PR](https://github.com/curie-eng/curie/pull/5938)
