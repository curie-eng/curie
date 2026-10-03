---
name: github-activity
description: Report what happened across GitHub repositories over a time window, read only. Invoke when the user asks what merged, which issues were opened or closed, or what moved between milestones since a date or since the last report, across one or more repositories.
---

# GitHub activity over a window

## When to run
The user wants a summary of repository activity since a point in time: merged
pull requests, issues opened and closed, and milestone movement, usually across
several repositories at once. Typical asks are "what merged since Monday" or
"what changed since my last update".

## How to answer
1. Work out the window. `since` is the start as ISO 8601 (for example
   `2026-09-28T00:00:00Z`): the last run time when this is a recurring report,
   otherwise the date the user named. `until` is optional and defaults to now.
2. Call `repository_activity` on the `github` server with `since`, and `until`
   when the user gave an end. Pass `repositories` (a list of `owner/name`) only
   when the user named specific ones; otherwise the configured set is used. If
   the tool says no repositories are configured, ask which to read.
3. Report per repository, in this order, leaving out empty sections:
   * merged pull requests: number, title, author, merge time;
   * issues opened: number, title, author;
   * issues closed in the window, even if reopened since: number, title, and
     whether completed or not planned when GitHub reports it;
   * milestone movement: items added to or removed from a milestone, and
     milestones created or closed in the window.
   If every list is empty for a repository and nothing was truncated, say it
   was quiet in one line.
4. Read `truncated`. When it is non-empty for a repository, say plainly that
   the window was too busy to read in full for that repository, name what was
   cut off, and suggest a narrower window (a later `since` or an earlier
   `until`). Never describe a truncated repository as quiet or complete.

## What this bundle cannot do
It cannot write to GitHub. There is no tool for commenting, labeling, closing,
reopening, assigning, or moving an issue or pull request to a milestone, and
the credential behind it is read only. When asked for any of those, say this
bundle is read only and cannot make the change; offer the details the person
would need to make it themselves.
