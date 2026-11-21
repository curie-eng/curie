---
name: factory-environment-maintenance
description: Review Software Factory failures across GitHub, GitLab, Jira, and Bitbucket bindings; propose and verify repository runner image or sandbox configuration repairs. Use for periodic admin maintenance or repeated needs-human outcomes.
---

# Maintain factory repository environments

Run on an admin-chosen schedule, such as weekly, and after a repeated failure
cluster. The schedule invokes this local skill with the target factory and
time window; the skill itself performs no background polling.

Read the configured factory bindings first. Treat each work item as a tracker
issue plus a separate code-host repository; Jira issues may correspond to
Bitbucket or GitHub pull requests. Use provider tools or APIs available to the
operator for the actual binding. Do not assume GitHub labels, issue numbers, or
PR URLs exist on every provider.
GitHub uses issues and pull requests; GitLab uses issues and merge requests;
Jira Cloud issues pair with the bound GitHub or Bitbucket Cloud/DC pull
requests. Preserve each provider's immutable project and issue identity.

## Review failures

1. Read recent `curie cluster work-items --json` outcomes (or the local tier
   when appropriate), then inspect failed items and their tracker notices,
   run details, and code-host CI or review history. State the time window and
   count. The current work-items list is bounded; if `truncated` is true,
   continue through the provider's failure history when available and report
   any remaining coverage gap. Never claim to have examined all failures from
   a truncated page. Deduplicate retries of one root cause. An inaccessible
   record is unknown, not successful.
2. Group failures by cause and affected repository. Distinguish missing or
   wrong-version executables from blocked registry egress, unavailable
   services, credentials, sandbox capacity, CI/base health, publication
   restrictions, and incomplete task instructions. Correlate with the selected
   repository image digest and the exact failing command. Do not propose an
   image rebuild for a network-policy or credential failure.
3. For an image cause, compare the operator-owned repository-runner spec with
   the selected image's `/etc/curie/tool-manifest.json`, then propose a catalog
   entry or custom Dockerfile/probe change. For other causes, name the owning
   configuration or ticket-preparation instruction and its administrator.
   Show affected examples, expected recurrence reduction, build or operational
   cost, a negative control, and rollback digest. Keep private evidence in a
   private admin record; use anonymous descriptions on public upstream surfaces.
4. Present a prioritized, reviewable set of changes. Do not modify a binding,
   push an image, widen egress, relabel issues, or retry work from this review
   alone. Approval of a specific candidate permits the setup skill's build,
   probe, sandbox validation, and deployment path. After deployment, compare
   new failures with the prior window and record whether the cause recurred.

If work-item outcomes omit the command or sandbox evidence needed to classify
a failure, report that observability gap instead of guessing. An image update
does not automatically make an old failed issue eligible for retry; follow the
configured tracker's explicit retry or selection policy.

After a Curie CLI upgrade, rerun `curie factory skills install --agent <client>`
to update an exact shipped copy; review local edits before replacing them.
