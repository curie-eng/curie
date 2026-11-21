# Personal Claude subscription

A personal subscription is your own Claude plan. Factory work you start runs on
that plan. Curie stores the token and uses it only for a run you own.

This page is the developer path: create a token, enroll, and start a run.
Operator setup stays in the sections linked below. Do not repeat those settings
here. The command reference is
[Personal Claude subscription](../../cli/README.md#personal-claude-subscription).

## Before you enroll

Your operator has to finish these steps first. The links are the configuration.
This page does not repeat it.

1. The installation is Kubernetes, in personal mode on the Kubernetes sandbox
   substrate. The operator selects the mode with
   `curie cluster factory --model-auth-mode personal`, which sets the chart
   value `factory.modelAuthMode`; `curie cluster status` shows it as
   `factory model auth mode: personal`. An installation that set
   `FACTORY_MODEL_AUTH_MODE` through `api.extraEnv` and `worker.extraEnv`
   must remove those entries and set `factory.modelAuthMode` in the same
   upgrade; the chart README section "Factory model auth mode" shows how. See [Admitting a labelled GitHub issue](../operations.md#admitting-a-labelled-github-issue).
2. `personalClaude.grantNamespace` names an existing namespace used only for
   personal grant custody. An empty value makes enrollment answer
   `custody_unconfigured`. See
   [Personal Claude enrollment](../operations.md#personal-claude-enrollment).
3. The factory is on, Slack is connected, and the agent is bound to the channel
   you will use. That binding's GitHub surface serves the repository. See
   [Factory Slack kickoff](../operations.md#factory-slack-kickoff).
4. Your Slack account is linked to your principal. Starting a run uses that
   link. Steering a run also needs a linked GitHub account with write access.
   See [Factory Slack steering](../operations.md#factory-slack-steering).
5. You can sign in to the Console in a browser. The session cookie is sent only
   over HTTPS, or over HTTP on loopback. See
   [curie cluster status](../operations.md#curie-cluster-status).

## Create the token

Run `claude setup-token` yourself. Curie does not run it, and it does not ask
for a password or a saved browser profile.

Save only the token that command prints into a regular file named `token.txt`.
Set the file mode to 0600 or 0400. One trailing newline is ignored. The value
must be 1 to 4096 printable ASCII characters with no spaces. That check is
shape only. It does not say the token is valid or unexpired.

Do not pass the token as an argument, an environment variable, or on stdin.
Do not put a real token in a ticket, a document, or a shell history you share.

## Sign in and enroll

There are two ways to enroll. The Console page is the first. The CLI is the
second, for scripts and terminals.

### In the Console

Sign in to the Console with your organization, then open
`<Console origin>/settings/claude-plan`. The same page is Settings, then Claude
plan. A login-code session cannot use it; the page says so and links to
organization sign-in.

1. The page shows one status line: "Not connected", "Connected", or
   "Disconnected". When connected, a muted line under it names the enrollment
   generation ("Generation N"). While a disconnect is still finishing, a line
   names the runs and stored tokens left to clean up, and the page refreshes
   every 5 seconds until both are zero.
2. Paste the token that `claude setup-token` printed into the box and choose
   Connect (no plan yet), Replace (connected), or Reconnect (disconnected).
   The page sends the version it last read, so a change made in another window
   or the CLI answers "Your plan changed in another window" and the page
   reloads the current state.
3. Disconnect asks for confirmation. It stops your runs and deletes the stored
   token. It does not revoke the token with Anthropic; revoke it in your Claude
   account settings.

The token is checked against the same rule as the CLI, sent once in the request
body, and cleared from the page as soon as the request finishes. The page never
shows it again and keeps it in no browser storage. Each button shows the
matching `curie cluster subscription` command.

### With the CLI

The CLI uses your Console session cookie. It has no `--api-key`, and these
commands do not read a platform key.

When organization sign-in is enabled, the login screen shows "Sign in with
your organization". That sign-in lands you in the Console, already signed
in. If the return path was `/settings/claude-plan`, you land on that page.

Sign in to the Console in a browser. Copy only the value of the
`__Host-curie_console_session` cookie into a regular file named `session.txt`,
mode 0600 or 0400. Copying that cookie by hand is a known limit. See
[Known limitations](#known-limitations).

The version numbers below are an example that starts from a first enrollment.
Use the version `status` prints.

```bash
chmod 600 session.txt token.txt
curie cluster subscription enroll --session-file session.txt --token-file token.txt
curie cluster subscription status --session-file session.txt
curie cluster subscription replace --session-file session.txt --token-file token.txt --expected-version 1
curie cluster subscription disconnect --session-file session.txt --expected-version 2
curie cluster subscription reconnect --session-file session.txt --token-file token.txt --expected-version 3
curie cluster --context <context> subscription status --session-file session.txt
```

`enroll` creates generation 1 when you have no enrollment. `status` prints one
of these lines:

```text
personal Claude enrollment: active, generation 1, version 1
personal Claude enrollment: absent
```

An inactive enrollment uses the same sentence with `inactive`. When teardown is
still running, a second line names the pending runtime and custody counts. A
disconnect is finished when both counts are zero. The operator page defines the
counts.

`replace` requires an active enrollment. It stops runtimes that are using the
old grant and does not cancel their work items. `disconnect` makes the
enrollment inactive and stops its runtimes. It does not revoke the token in
the provider's own settings, and it does not reverse billing. Repeating
disconnect at the inactive version changes nothing. `reconnect` requires an
inactive enrollment and supplies a new token.

`replace`, `reconnect`, and disconnecting an active enrollment each increase
the generation and the version by one.

With `--json`, stdout is one object. The shape is the
[subscription status schema](../../cli/schema/subscription.schema.json). It
never includes the token, the cookie, or the custody location.

Pass `--namespace` and `--release` when the operator did not use the chart
defaults `curie` and `curie`. Without `--api-url`, the CLI reaches the release
API through a loopback port-forward. A direct `--api-url` must be HTTPS unless
it is loopback, and it must be the same host and port as the Console that
issued the session. The environment variable for that URL is `CURIE_API_URL`.
The environment variable for the namespace is `CURIE_NAMESPACE`.

## Commands and flags

| Flag | Applies to | What it does |
| --- | --- | --- |
| `--session-file PATH` | every verb | Required. Regular file, mode 0600 or 0400, holding only the cookie value. |
| `--token-file PATH` | `enroll`, `replace`, `reconnect` | Required. Regular file, mode 0600 or 0400, holding only the token. |
| `--api-url URL` | every verb | Optional. Defaults to the release API over loopback. |
| `--namespace NAME` | every verb | Optional. Default `curie`. |
| `--release NAME` | every verb | Optional. Default `curie`. |
| `--dry-run` | every verb | Checks the files the verb requires, prints the plan, and sends nothing. |
| `--expected-version N` | `replace`, `reconnect`, `disconnect` | Optional integer, 1 or greater. See [The version check](#the-version-check). |
| `--context <context>` | `curie cluster` | Kubernetes context. Defaults to the current context, pinned for the command. |
| `--json` | `curie` | One JSON object on stdout. Human text goes to stderr. |
| `--quiet`, `-q` | `curie` | Payload only. Progress and diagnostics are suppressed. |
| `--debug` | `curie` | Verbose plumbing. The token and cookie are still omitted. |
| `--color VALUE` | `curie` | `auto` (default), `always`, or `never`. |

There is no `--api-key` on these commands.

Exit 2 means a bad file, a bad token, or a request the API rejects as invalid
(`invalid_enrollment_request`). Exit 1 means a refused or expired session, a
rejected origin, no enrollment on a write, a version or state conflict, or
`custody_unconfigured`. Exit 3 means the API is unreachable, or custody is
briefly unavailable (`custody_unavailable`, `enrollment_busy`), or an API
rollout is in progress (`personal_rollout_in_progress`).

During an API rollout, enroll, replace, and reconnect are held and the previous
enrollment is unchanged. Disconnect is not held. Retry the held verb after the
rollout finishes.

## Start a run

In a public or private channel where the factory app is bound, mention the app
with exactly one GitHub issue link. Post at the channel root, or in a thread
that no factory run owns. A direct message does not start a run.

```text
@curie https://github.com/acme-corp/acme-fixture/issues/7
```

Slack's `<URL>` and `<URL|label>` forms both count. A label that names a
second issue is a second link, and the mention is refused.

Your Slack identity selects your principal. That principal's active enrollment
selects the grant and the generation the run is bound to. The installation's
GitHub App reads the issue. A linked GitHub account is not required to start.

If your Slack account is unlinked or your enrollment is inactive, Curie sends
you a private message with a **Connect your Claude plan** button before any
model work. The public placeholder becomes:

```text
Not started. I sent you a private message about what to do next.
```

For an unlinked account, the button opens the existing account confirmation
page. It shows your Slack display name, @handle, user id and workspace name
and id when Slack supplied the names. Confirm only your own account: anyone
using it can start runs on your Claude plan. Sign in, confirm, then connect
your plan on the Console's `/settings/claude-plan` page and mention the app
again. An already linked account opens that plan page directly. When secure
Console OIDC is unavailable or the token issuance limit is reached, the private
message has no button and directs you to the operator.

If the run is admitted, the app's first reply in that thread becomes the run's
progress card. It names the issue and that you started it, lists the run's
steps as they are done, and is edited in place until the run finishes or stops.
When the pull request is open, or when the run needs you, the thread also gets
one short reply that mentions you:

```text
@you your pull request is ready for review: https://github.com/acme-corp/acme-fixture/pull/12
```

The thread does not need `factory_slack_channel`. See
[Factory Slack progress](../operations.md#factory-slack-progress).

To ask for a change after the run has started, mention the app in that run's
thread. Only you can steer it, and only while your enrollment is still active
at that WorkItem's generation. Steering also needs one linked GitHub account
with write access. Another person, even one with write access and an enrollment
of their own, is refused before any model work:

```text
This message was refused as a revision.
Any earlier attempt of this message is unconfirmed here; the run thread shows any new revision.
Outcome: admission_refused (owner_mismatch)
```

A sender with no active enrollment gets the same headline and
`Outcome: admission_refused (enrollment_inactive)`. The full rules, including
a declared test driver on a test installation, are in
[Factory Slack kickoff](../operations.md#factory-slack-kickoff).

## Other ways in

1. Adding the factory label with `factory.modelAuthMode` set to `personal` admits a
   WorkItem when the labeller is linked to a principal whose personal Claude
   enrollment is active. The WorkItem owner is that principal at the
   enrollment's generation. An unlinked labeller is refused
   `personal_owner_required`. A linked labeller with no active enrollment is
   refused `enrollment_inactive`. Each refusal is answered by an issue comment
   and, when a factory Slack channel is configured, one channel post. When
   the secure Console origin is configured, the comment links to the Claude
   plan page and the channel post has a **Connect your Claude plan** button.
   These public links contain no account-connect token.
   Mentioning the app on an issue that has no WorkItem is refused
   `not_admitted`. Review feedback with no WorkItem is ignored. A WorkItem
   admitted as service before the switch refuses its tracker mentions, review
   feedback, and Slack steering with `personal_owner_required`. No owner is
   invented for it.
2. Ordinary Slack chat that is not a kickoff is refused before a claim or a
   model call: "This installation runs personal subscriptions, and this request
   has no owner to run it as (personal_owner_required). Nothing was started."
3. A webhook hook has a reply target and no owner. The worker posts the sentence
   in item 2, then finishes the delivery. It does not take a claim or call a model.
4. A cron hook with no reply target is targetless. The worker logs
   `personal_owner_required` and posts no reply, then finishes the delivery.
   A cron hook that names a reply target posts the sentence in item 2, then
   finishes the same way.
5. An eval has no owner. The worker logs `personal_owner_required` and reports
   an empty result. It does not post that sentence, and it does not read the
   bundle or take a claim.

## The version check

`--expected-version` is a compare-and-set. A concurrent replace, reconnect, or
disconnect fails with `stale_version` instead of changing a newer enrollment.
The current CLI and API agreement is
[pull request 5547](https://github.com/curie-eng/curie/pull/5547).

1. On the CLI the flag is optional for `replace`, `reconnect`, and `disconnect`.
   When you set it, the CLI sends that version and does not read first. When
   you omit it, the CLI reads your enrollment and sends the version that read
   returned. If the read finds no enrollment, the command exits 1, tells you
   to run `curie cluster subscription enroll` first, and sends no write.
2. On the API, `expected_version` is null only for enroll. Replace and
   reconnect require the integer in the PUT body. Disconnect requires it as a
   query parameter. A missing or badly shaped value is HTTP 422
   `invalid_enrollment_request`. The error does not include the token or the
   cookie.
3. HTTP 409 `stale_version` means someone changed the enrollment after the
   version you sent. Run `curie cluster subscription status` and retry with
   the version it prints.
4. Replace and reconnect answer HTTP 409 `enrollment_absent` when no
   enrollment exists. Disconnect answers HTTP 404 `enrollment_absent` in that
   case. The other 409 codes are `enrollment_exists` (enroll when one already
   exists), `enrollment_inactive` (replace while disconnected),
   `enrollment_active` (reconnect while still connected), and
   `staging_conflict` (custody staging did not settle). Run status and retry
   the verb that fits.

## Limits

Only individual Claude Pro and Max plans are supported. Team and Enterprise
plans are not supported. Enrollment does not read the plan. Any token with
the accepted shape is stored, including a token from a plan this installation
does not support.

Personal mode is Kubernetes only. When `CURIE_MODEL_AUTH_MODE` or
`FACTORY_MODEL_AUTH_MODE` in the CLI's own environment is exactly `personal`,
these commands exit 2 before any Docker, compose, network, volume, or image
call, including `--dry-run`:

1. `curie skill up`
2. `curie skill eval`
3. `curie local up`
4. `curie local rebuild`

The error starts with `personal_mode_requires_kubernetes`. It names the
variable, says the mode runs only on Kubernetes (`curie cluster ...`), and
says it does not run in Docker-backed `skill` or `local` runs. Unset that
variable, or run on a cluster with `curie cluster up`.

`CURIE_MODEL_AUTH_MODE` is checked first. The value must be the exact word
`personal`. A different case or surrounding spaces does not select the mode.
Other `skill` and `local` commands are not refused by this check.

On a cluster, `factory.modelAuthMode` is the only switch. The chart renders it
as `FACTORY_MODEL_AUTH_MODE` into both the API and the worker, and refuses the
variable in `api.extraEnv` or `worker.extraEnv`. A personal release whose
`worker.extraEnv` sets `CURIE_SANDBOX_SUBSTRATE` to anything but the literal
`kubernetes`, compared with whitespace trimmed and case ignored (a `valueFrom`
entry or an empty value included), is refused by `curie cluster factory --model-auth-mode personal` and by the
chart render, before anything is applied. A worker started in personal mode
on another substrate anyway refuses to start with
`personal_mode_requires_kubernetes: FACTORY_MODEL_AUTH_MODE=personal needs CURIE_SANDBOX_SUBSTRATE=kubernetes`.

## Known limitations

1. Enrollment still needs the Console session cookie copied into
   `--session-file`. There is no CLI sign-in for that session yet.
   [Issue 5559](https://github.com/curie-eng/curie/issues/5559) tracks that
   limit. This guide does not describe a future command.
2. Live acceptance runs do not yet affirm exact usage capture. That concerns
   the operator who runs the acceptance driver, not day-to-day use of an
   enrolled subscription.
   [Issue 5640](https://github.com/curie-eng/curie/issues/5640) tracks that
   limit.
3. Curie does not warn before a Claude plan expires. Expiry is not knowable.
   Enrollment checks only that the token is 1 to 4096 printable characters,
   and it stores no expiry. When a run fails because the plan was rejected,
   and the enrollment is still active at that run's generation, Curie sends
   one Slack direct message for that generation. The message has a button to
   the Claude plan page. It does not send another for the same generation,
   and it sends none after you disconnect or replace the enrollment.

## Link your GitHub account

Open **Settings > Claude plan > Linked accounts** in the Console and choose
**Link GitHub**. Authorize the factory GitHub App with the account you use to
apply factory labels. If the button is absent, ask the operator to configure
the GitHub App OAuth client. An account already linked to someone else must be
unlinked there before it can be linked to you.

The same section lists your Slack and GitHub accounts. Choose **Unlink** and
confirm to stop new runs through an account. Runs already admitted keep their
owner; disconnect your Claude plan when those runs must also stop.
