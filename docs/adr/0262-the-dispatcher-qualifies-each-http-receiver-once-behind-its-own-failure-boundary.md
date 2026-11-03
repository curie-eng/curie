# 262. The dispatcher qualifies each HTTP receiver once behind its own failure boundary

Date: 2026-11-02

Status: Accepted

Accepted 2026-11-02 under ADR 0102 coordinated acceptance with explicit
approval by Junwon Jung (jw3329), including merging alongside its realizing
code path in one combined pull request. The realizing code path is
`curie_dispatcher.http_receiver_initializer.build_selected_receivers` and
`curie_dispatcher.run.build_identity_connections`. The CODEOWNERS approving review by TheConnMan was
not obtained before merge.

## Context

At startup the dispatcher builds one selected HTTP receiver for each
registration in its protected receiver file. Each receiver is qualified
against Slack: the identity must have been admitted by preflight, and the
identity's own `auth.test` must report the registered installation (and, for
a Grid receiver, the registered organisation). The realization criteria
today stop the whole dispatcher when any one receiver fails, rather than
start that identity on its legacy path.

Two problems follow. A dispatcher serves many identities, so one receiver's
transient `auth.test` timeout or one stale registration takes down every
agent, including those on unrelated identities. And the qualification runs in
more than one place: once while building the receiver set, and again when
each Slack app is constructed. Any isolation added to one stage leaves the
other able to abort the process.

## Decision

### 1. Qualify once

Each selected receiver is qualified exactly once per process start, and the
qualified result (the identity's reported installation, organisation and bot
ids) is what the app for that identity is built from. App construction does
not repeat the network qualification.

### 2. One failure boundary per receiver, around every stage

Every stage of starting one receiver, from matching its registration to an
admitted identity, through `auth.test` and its comparison, to constructing its
runtime and app, runs inside one boundary for that receiver. A failure at any
stage, including a timeout, excludes that receiver and its identity and is
logged as an error naming only the channel identity and the stage. The other
receivers and every unregistered identity start.

### 3. An excluded identity starts nothing

An identity whose receiver was excluded opens no Slack connection on either
path. Its registration still makes the API refuse that identity's
channel-token turns (ADR 0254), so a legacy connection would only produce
refusals while adding a second path that must never admit. Its agents are
unavailable until the next process start qualifies the receiver.

### 4. A defective file still stops startup

A receiver file that cannot be read or parsed, or whose entries repeat a
channel identity, still stops startup. Then no entry can be trusted to be
what the operator wrote, and excluding entries would guess at intent.

## Consequences

One receiver's failure costs only its own identity's agents. The process
stays up for everyone else, and the log names which identity failed and at
which stage, without Slack or secret bytes.

An excluded receiver does not recover by itself; a process restart retries
it. Automatic re-qualification with backoff is not part of this decision.

## Alternatives considered

1. **Keep stopping the dispatcher on any receiver failure.** Turns a single
   identity's transient Slack error into an outage for every agent.
2. **Start an excluded identity on its legacy path.** The API refuses its
   turns anyway, so this adds a connection whose only effect is refusals, and
   relies on one layer where two are available.
3. **Isolate only the first qualification.** Leaves the second qualification
   during app construction able to abort the whole process; the boundary has
   to cover every stage, which qualifying once makes simple.
4. **Qualify at install time instead of at every start.** Needs every bot
   token at install, checks one moment rather than each start, and cannot see
   a workspace that later moves into an organisation.
