# 205. An attachment belongs to its thread, and every boot rebuilds the thread's files

Date: 2026-10-06

Status: Accepted

Accepted 2026-10-06 with explicit maintainer approval from Junwon Jung
(jw3329), who owns the attachment lane, given in the implementation planning
for [#4079](https://github.com/curie-eng/curie/issues/4079). The realizing code
paths are named under Implementation.

This ADR partially amends [ADR 0153](0153-a-channel-port-turn-carries-its-attachments.md)
(Accepted), back-linked there under
[ADR 0045](0045-the-status-line-is-the-mutable-part-of-an-immutable-adr.md).
ADR 0153's transport, its adapter fetch route, and its all-or-nothing rule for
the files a message carries all stand. What changes is how long a file stays
reachable: until now it reached only the sandbox that booted for the message
carrying it. Under this ADR it reaches every sandbox that boots for the thread.

## Context

A person attaches a file to a message, and the agent reads it and asks a
question. The person answers in the same thread, and the agent can no longer
find the file. A live deployment hit exactly this on 2026-10-05. Its history
still said the file was at `/attachments/<name>`, because the runner records
that notice on the message that carried the file
([#3691](https://github.com/curie-eng/curie/issues/3691)), but the directory
was empty.

The cause is a mismatch between two lifetimes:

1. **The bytes live as long as one sandbox.** The worker resolves a message's
   files only on the turn that carries them, parks them for one hour so a retry
   can fetch them again, and hands the boot a five minute capability. The
   attachments init container writes them into an `emptyDir`. A later sandbox
   for the same thread is given nothing.
2. **The conversation lives as long as the thread.** The transcript is keyed by
   agent, binding and thread ([ADR 0170](0170-conversation-transcripts-live-in-their-own-per-thread-table.md)),
   survives any number of sandboxes, and expires after 30 idle days or when the
   thread's WorkItem ends.

That mismatch used to be hidden, because a follow-up usually reused the warm
sandbox that already held the files. Since the turn budget check from
[#3823](https://github.com/curie-eng/curie/issues/3823), most follow-ups boot a
replacement. Idle reap, approval suspend and resume, and pod failure always
did. In every one of those cases the history names a file that is not on disk.

[#4086](https://github.com/curie-eng/curie/pull/4086) is the stable line's
stopgap. A text-only turn re-mints the thread's newest parked set while it is
still retained. It does not cover a thread that attached two different files,
and it does not cover a thread that resumes after the one hour retention.
Nothing that keys on the parked bytes can, because they are deliberately short
lived.

## Decision

1. **The thread records every file the agent was given.** Once a turn's
   resolved files are installed on the sandbox that serves it, the worker
   appends one reference per file to the thread's attachment ledger. A turn
   that is refused after its files resolve (the route changed, a reply was
   still running) records nothing, and its discard removes anything it wrote.
   The append is idempotent per event and file id, so a redelivered turn does
   not record a file twice. A reference records the channel's file id, the
   name, the best-effort mime type and size, the sha256 of the bytes that
   were parked, the order it arrived in, its on-disk name (decision 4), and
   the route it came from: the reply handle's kind, adapter, and bot
   identity. It never records an endpoint, a URL, or bytes.
2. **The ledger has the transcript's lifetime, and only the worker writes
   it.** It is API-owned thread state keyed exactly like the transcript
   (agent, binding scope, thread key). Every path that removes or expires a
   thread's transcript removes its references with it: WorkItem completion,
   idle expiry, explicit delete, and agent deletion. It follows the
   transcript's ADR 0168 copy-forward from a pre-identity key the same way.
   Because transcript expiry is lazy, a ledger read returns nothing for a
   thread whose transcript has expired. Reads and appends go through
   `/v1/internal` routes behind the internal worker token. No state route
   exposes the ledger, and no sandbox credential can reach it, so an agent
   cannot add a file id for the worker to fetch with the bot token. It is not
   stored inside the transcript value, because the runner rewrites that value
   whenever it compacts.
3. **Every boot for the thread rebuilds the whole set.** Whatever starts the
   sandbox (a new claim, a suspended resume, a turn budget or workspace
   handoff, or a file-carrying turn's fresh claim), the worker reads the
   thread's ledger and materializes all of it in arrival order, with the
   current message's files last. The current message is the one the boot is
   for. An approval resume and a hook or cron turn have no current files.
   An adopted warm sandbox is unchanged: it ignores claim env and already
   holds the files.
4. **A file's on-disk name is fixed when it is recorded.** The name is
   cleaned with the init container's own rules and disambiguated against
   every name already in the ledger, then stored on the reference. The init
   container and the docker substrate write exactly that name and never
   rename. Omitting a file never changes another file's name, and a name is
   never reused, so the path a notice gave for a file is its path on every
   later boot.
5. **Bytes stay short-lived and are fetched again when needed.** The parked
   object store remains a cache with the existing retention. A reference
   whose bytes are still parked is re-minted. One whose bytes have lapsed is
   fetched again with the same bounded, credential-holding fetch the carrying
   turn used, and its digest must match the recorded sha256. The route for
   that fetch is resolved from the agent's bindings as they are now, never
   from a recorded endpoint: Slack `files.info` with the recorded identity's
   current bot token, or the adapter's current endpoint and secret for
   `GET /attachments/{id}`. If no current binding routes that kind and
   adapter to this agent, the file is unavailable. Nothing about who holds
   the channel credential changes
   ([ADR 0075](0075-the-agent-proxy-credential-and-egress-boundary.md)). The sandbox still never
   reaches the channel.
6. **Only the carrying message's files are all-or-nothing.** A message that
   carries files still resolves them all or refuses the turn, as ADR 0153
   decides, because the person just sent them. An earlier file is best
   effort: deleted at the channel, permission revoked, an adapter 404, a
   digest that no longer matches, a rate limit, or a fetch that does not fit
   in the turn's remaining time all make it unavailable. It is omitted from
   the boot and named to the agent as unavailable. It never fails the boot.
   Capabilities are signed only after every fetch finishes, so a slow fetch
   cannot age one out before the sandbox redeems it. A failed ledger read on
   a text-only turn boots without earlier files and tells the agent so. The
   attachments init container applies the same rule to an earlier file's
   reference it can no longer redeem, which is what a restarted pod meets
   once its five minute capability has expired. It stays all-or-nothing for
   the current message's references, and a digest mismatch at init stays
   fatal, because that is an integrity failure.
7. **The thread's set is bounded.** A per-thread file count and total byte
   budget, both configurable, cap what one boot materializes. The budget must
   fit inside the sandbox's attachment volume and the init container's fetch
   timeout. Over the budget, the newest files are kept and the omitted ones
   are named to the agent as omitted.
8. **The runner is told which files are this message's, and the disk wins.**
   The worker passes the runner an attachment manifest as an optional boot
   env key: each file's on-disk name, whether it arrived on the current
   message or earlier, and which earlier files are unavailable or omitted.
   The init container writes the outcome of each reference to a hidden
   status file in the volume. The runner reconciles the manifest against the
   status file and the disk, and a file that is not on disk is reported
   missing whatever the manifest says. The system prompt preamble lists the
   files on disk and names the missing ones. The per-message notice
   ([#3691](https://github.com/curie-eng/curie/issues/3691)) names only the
   current message's files, which closes
   [#4081](https://github.com/curie-eng/curie/issues/4081). The key is a
   runner-local boot input outside the frozen `SessionConfig`, so adding it
   is a patch version bump. The runner's pod template and the docker
   substrate each declare it, as claim env injection requires.
9. **An adapter serves what it announced.** A channel-port adapter should keep
   answering `GET /attachments/{id}` for an id it put on a turn for as long as
   it retains the message. A 404 means the file is unavailable, which
   decision 6 handles. This is the adapter obligation ADR 0153 left unstated.

What a thread can hold is unchanged: only files that reached the worker on a
turn. A file posted in a channel thread without mentioning the agent never
becomes a turn, and this ADR does not change that.

## Consequences

- A follow-up in a thread sees every file the thread was sent, at the path its
  history names, whatever sandbox it boots on and however long after the
  upload, for as long as the transcript lives.
- Anyone who can address the agent in that thread can have it read an earlier
  file, as they already could while a warm sandbox held it. What changes is
  how long: up to the transcript's 30 idle days instead of one sandbox's
  life. A file deleted at the channel stops reaching new boots once its
  parked copy lapses, at most one retention period later.
- A cold boot of a thread with files does more work: one ledger read, and a
  channel re-fetch for each file whose parked bytes have lapsed. The
  per-thread budget bounds that work, and the parked cache absorbs the common
  case of a follow-up within the hour. A text-only boot of a thread that
  never carried a file costs one ledger read.
- Most boots of a thread with files now carry an attachment capability, so the
  init container's tolerance for an expired earlier reference (decision 6) is
  what keeps a restarted pod from failing on a file it no longer needs.
- The per-turn `max_files` cap still applies to one message. The per-thread
  budget is separate.
- The worker writes a second kind of thread state through the API, so API
  unavailability during a file-carrying turn now fails that turn before the
  claim, the same way a failed resolve does today.
- The #4086 stopgap's `carry` is replaced by the ledger read and is removed when
  this lands on the stable line.

## Alternatives considered

- **Carry the newest parked set (#4086).** Kept on the stable line as the
  stopgap. Rejected as the decision because it loses every file but the newest
  and every file after one hour, and the history still names them.
- **Keep the bytes as long as the transcript.** Removes the re-fetch, but makes
  the platform the long-term custodian of every file anyone ever attached, on a
  retention clock nobody chose for files. The channel already keeps the
  original. Re-fetching keeps the custody where it was.
- **Let the sandbox fetch a file when the agent asks for it.** Lazy and cheap,
  but it needs either the channel credential or a new egress path inside the
  sandbox. ADR 0075 and ADR 0153 both rejected that boundary crossing.
- **Re-read the Slack thread on every turn.** The dispatcher could list the
  thread's messages and collect their files. It is Slack-only, so channel-port
  adapters would get nothing. It spends a history call on every turn, and it
  would pick up files posted without the agent being addressed, which is a
  privacy change this ADR does not want to make in passing.
- **Store the references inside the transcript.** Same lifetime for free, but
  the runner owns that value and rewrites it on compaction, so the worker
  would race the runner for a field it does not own.

## Implementation

Acceptance authorizes implementation, tracked in
[#4079](https://github.com/curie-eng/curie/issues/4079). The realizing code
paths are:

- the thread attachment table, its expiry, deletion and pre-identity
  copy-forward beside the transcript's, and the internal read and append
  routes behind the worker token in `apps/api`;
- the ledger client, the boot-time rebuild with cache, re-fetch, per-file
  unavailability and the per-thread budget in `apps/worker`'s attachment lane
  and kernel claim path;
- fixed on-disk names, best-effort redemption of an earlier file's expired or
  unfetchable reference, and the hidden status file in the attachments init
  container (`charts/curie/templates/agent-sandbox.yaml`) and in the docker
  substrate's equivalent;
- the manifest boot env key in `aci_protocol`'s `BootEnv` and the runner's
  preamble and notice in `runner/src/curie_runner`.
