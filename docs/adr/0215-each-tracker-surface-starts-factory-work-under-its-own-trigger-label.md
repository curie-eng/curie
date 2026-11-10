# 215. Each tracker surface starts factory work under its own trigger label

Date: 2026-10-12

Status: Accepted

Accepted with explicit maintainer approval from Brian Conn, given on
2026-10-09 for the label routing content of
[#4361](https://github.com/curie-eng/curie/issues/4361) and on 2026-10-11 for
the typed binding and per surface label routing of
[#4397](https://github.com/curie-eng/curie/issues/4397), alongside
implementation under the coordinated exception of
[ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).

This ADR partly supersedes
[ADR 0214](0214-one-factory-agent-serves-every-github-repository-bound-to-it.md)
decision 4 and its restatement in consequence 4: two surfaces on one
repository are now allowed when their effective trigger labels differ. It
partially amends
[ADR 0161](0161-signed-github-issue-events-admit-one-work-item.md), where an
install has one factory label;
[ADR 0187](0187-the-factory-polls-github-and-the-platform-reads-the-issue.md),
where the poller lists issues carrying "the configured label"; and
[ADR 0168](0168-one-installation-hosts-several-bot-identities.md) decision 3,
where the route key is `(kind, adapter, address)` and a route-less row
excludes another agent's route on the pair. Everything else in those ADRs
stands. It realizes the adapter construction and binding configuration of
[ADR 0197](0197-the-factory-reaches-code-hosts-and-trackers-through-two-ports.md)
without amending it.

Tracked in [#4397](https://github.com/curie-eng/curie/issues/4397), which
carries [#4361](https://github.com/curie-eng/curie/issues/4361) as amended by
its decision 10.

## Context

ADR 0197 put the factory behind a Tracker port and a CodeHost port, and
ADR 0214 made an agent's `github=<owner/name>` channel binding the thing that
makes it serve a repository. Two gaps remained.

1. Nothing built a Tracker or a CodeHost from the typed forge model, so the
   factory still read the old GitHub settings, and a GitLab or Jira adapter
   had no place to plug in and no way for an agent to serve its locations.
2. An install had one factory trigger label. The webhook path, the poller and
   the label reconciler all read that one value, and the route key
   `agent_channels_route_key` on `(kind, address, adapter)` refused a second
   `github=` row on the same repository. So an operator could not run a full
   factory agent and a simpler one on one repository, one started by
   `curie-factory` and the other by `curie-factory-fast`.

Most of the factory already works per issue. An issue is one WorkItem with
one agent (ADR 0197, identity 1), review feedback follows
`lineage.binding_id`, a mention acts only on an issue that already has a
WorkItem, and each tracker instance already takes its own label. What was
missing is a way to say which label belongs to which agent.

## Decision

**Chart config declares typed code hosts, trackers and named bindings. An
agent serves a tracker location through a channel surface, and each surface
carries the label that starts work through it. Overlap is one location with
one effective label.**

### Typed config and tracker surfaces

1. The chart declares `factory.codeHosts`, `factory.trackers` and
   `factory.bindings`, delivered to the API as `CURIE_FORGES`. A binding names
   a tracker and carries its credentials, start allowlist or group,
   repository aliases, component map and review feedback allowlist. One
   registry builds a Tracker and a CodeHost per configured entry, and every
   factory code path gets its tracker, code host, credential, label and
   mention login from that registry. The old GitHub factory settings are
   removed, not aliased.
2. An agent serves a tracker location through a channel surface:
   1. `github=<owner/name>`, unchanged from ADR 0214. No configured binding is
      needed; the one GitHub tracker serves it.
   2. `gitlab=<group/subgroup/project>`, a project path of any depth, served
      by the configured binding whose GitLab tracker lists that path.
   3. `jira=<binding name>`, served by the named binding with a Jira Cloud
      tracker.
3. Surface resolution for `gitlab=` and `jira=` exists now, so an adapter
   pull request adds no routing code. A surface whose location no configured
   tracker serves is refused `surface_unconfigured` (422) before it is stored.
4. Each forge kind registers from its own package. The `gitlab`,
   `jira_cloud`, `bitbucket_cloud` and `bitbucket_dc` packages register their
   kind as not installed. A config entry naming a kind that is not installed
   is refused at config load with an error naming the kind and the entry,
   never skipped. Because no tracker of such a kind can be configured, a
   `gitlab=` or `jira=` surface write in a build without that adapter is
   refused `surface_unconfigured`.

### The trigger label lives on the surface

1. `agent_channels.trigger_label` is a nullable column on every tracker
   surface kind (`github`, `gitlab`, `jira`). A CHECK refuses it on any other
   kind. It follows the tracker label rule (1 to 50 characters, no
   whitespace) and may not start with a reserved label family: the
   `curie-factory:` state labels, `base:` or `repo:`.
2. A surface with no label serves its tracker's configured `label`.
   `effective_trigger_label` is the only reader: the surface's own label, else
   the configured label of the tracker that serves it, else none. No
   migration backfill is needed, so existing installs keep working with no
   action.
3. A label is set only when a surface is added. A move keeps the stored
   label, and a patch naming one is refused. To change it, remove the surface
   and add it again.
4. The API wire adds optional `trigger_label` to `ChannelBindingWrite` and
   `ChannelBindingOut`. The CLI adds `--label` to
   `curie cluster surfaces <agent> --add` and `curie local surfaces <agent>
   --add` through one shared handler. `--label` with a kind that is not a
   tracker surface exits non-zero before any request. Listings show the label,
   and `--json` carries `trigger_label`.

### Overlap

1. Overlap is the same tracker location with the same effective label. It is
   refused on one agent or on several, under the pair advisory lock, with a
   409 that names the other agent.
2. An agent holds at most one surface per tracker location, because the agent
   scoped readers (admission, channel reads, lineages) resolve one row per
   agent and location.
3. Different labels on one location are allowed, on different agents.
4. `agent_channels_route_key` widens to `(kind, address, adapter,
   trigger_label)`, still NULLS NOT DISTINCT. The index cannot compare
   effective labels (a NULL label and an explicit label equal to the tracker
   label are different keys), so the API enforces the rule above.
5. Tracker surfaces are exempt from the route-less pair exclusivity of
   ADR 0168 decision 3. Their work routes by the WorkItem's agent, never by
   the pair alone, and the overlap rule already refuses the only collision
   left. Every other kind keeps that exclusivity unchanged.

### Admission, cancellation and the rest of #4361

1. Admission for label L on location R uses the surface on R whose effective
   label is L. No such surface refuses `binding_missing`. If two surfaces
   resolve to L at run time (the tracker label was changed to equal an
   explicit label), L admits nothing on R, refuses `binding_missing`, and logs
   a warning naming both agents. The webhook path picks the surface by the
   `label.name` of the `issues.labeled` or `issues.unlabeled` event.
2. Admission is refused, with an upserted marked status comment, in this
   order:
   1. `work_item_other_agent`: the issue already has a WorkItem whose agent is
      not L's agent, whether or not that agent still has a surface on R. The
      comment says the issue belongs to
      the run that agent's label started and that a new issue is needed to run
      it under L.
   2. `label_conflict`: the issue's current labels, from the `verify_current`
      re-read, include two or more effective labels bound on R. The comment
      names them and asks to keep exactly one. A run already admitted keeps
      running.

   The outcome depends on the labels present at admission, not on event
   order.
3. `work_items.trigger_label` records the label that admitted the WorkItem.
   It is written at admission and at readmission for every WorkItem; one
   admitted through a surface with no label stores the then effective tracker
   label. This column goes beyond #4361's text so that cancellation never
   needs a binding lookup (ADR 0161).
4. Removing label L cancels the issue's WorkItem only when L equals
   `work_items.trigger_label`; removing any other label returns
   `label_not_admitting` and leaves the WorkItem alone. A WorkItem admitted
   before the column existed (NULL) was admitted when the configured tracker
   label was the only install label, so it compares L with that label alone;
   no surface is consulted, so removing or moving its agent's surface does not
   change what cancels it. Closing the issue cancels as before. A tracker
   label change does not strand running work: removing the old label still
   cancels it.
5. The poller makes one pass per repository, with one repository read and one
   installation token. Inside it, one labelled issue listing runs per
   effective label, each under the ETag key `issues:<label>`. The old `issues`
   key, and the key of any label no surface binds any more, is dropped, which
   costs one unconditional listing once. Mention and review polling run once
   per repository. The label reconciler walks the same (repository, label)
   pairs. A repository whose last surface was removed but which still has open
   WorkItems gets the cancellation pass only, with no listing or admission.
   Repositories outside `GITHUB_REPO_ALLOWLIST` are skipped.
6. Mentions keep the one install wide mention login. A mention on an issue
   with a WorkItem routes to that WorkItem's agent's surface on R; a mention
   on an issue without one is ignored.
7. Review feedback on a factory pull request keeps following
   `lineage.binding_id`, so it reaches the agent whose label started the run.
8. The `curie-factory:*` state labels stay unnamespaced, because one issue has
   at most one WorkItem and so one writer.
9. Per agent budget, concurrency and execution deadline stay per agent.
10. The migration adds both columns, the CHECK and the widened key. Its
    downgrade refuses, naming each location, while any `(kind, address,
    adapter)` holds two rows; otherwise it restores the old key and drops the
    CHECK and both columns.

## Realizing code paths

1. Registry and config: `apps/api/src/curie_api/forges/registry.py`
   (`register`, `register_not_installed`, `check_installed`,
   `build_registry`), `apps/api/src/curie_api/forges/loader.py`, and the
   reserved kind packages `apps/api/src/curie_api/forges/gitlab/`,
   `forges/jira_cloud/`, `forges/bitbucket_cloud/` and
   `forges/bitbucket_dc/`.
2. Surfaces: `apps/api/src/curie_api/forges/surfaces.py`
   (`resolve_surface`, `effective_trigger_label`), the label and address
   rules in `apps/api/src/curie_api/schemas/channels.py`
   (`valid_trigger_label`), and the write paths in
   `apps/api/src/curie_api/routers/agents.py`.
3. Overlap: `refuse_label_overlap` and `refuse_routeless_pair_sharing` in
   `apps/api/src/curie_api/crud/channels.py`.
4. Label routing: `resolve_binding`, `bound_labels` and `agent_binding` in
   `apps/api/src/curie_api/forges/github/binding.py`; `admit_notice`,
   `admitting_label` and the label refusals in
   `apps/api/src/curie_api/github_factory.py`; `parse_factory_event` in
   `apps/api/src/curie_api/github_factory_events.py`; the per label listing
   and cancellation pass in `apps/api/src/curie_api/factory_poll_intake.py`;
   and `open_work_item_repositories` and the (repository, label) walk in
   `apps/api/src/curie_api/factory_label_reconcile.py`.
5. Migration: `apps/api/alembic/versions/0105_agent_channel_trigger_label.py`.
6. CLI: the `--label` flag and listing in `cli/src/commands/channels.rs`.
7. Tests: `apps/api/tests/forges/test_registry.py`,
   `apps/api/tests/test_trigger_label_binding.py`,
   `apps/api/tests/test_trigger_label_migration.py`,
   `apps/api/tests/test_factory_label_routing.py`,
   `apps/api/tests/test_factory_label_routing_poll.py`,
   `apps/api/tests/test_factory_multi_label.py`, and
   `cli/tests/surfaces_trigger_label.rs`.

## Consequences

1. Two agents can serve one repository under different labels, for example a
   full factory agent with no label and a simpler one with
   `curie-factory-fast`, and each admits only its own issues.
2. A GitLab or Jira adapter pull request replaces only its own package. The
   surface address shapes, resolution, label routing and overlap rules are
   already in place.
3. A tracker location with two surfaces costs one labelled listing per label
   per poll pass, so polling cost grows with the number of labels, not the
   number of agents alone.
4. Changing the configured tracker label so it equals an explicit label on
   the same location stops admission under that label there, with a warning,
   until the operator removes one of the two surfaces.
5. A label that admitted a WorkItem keeps cancelling it after a tracker label
   change or after its surface is removed, because the WorkItem stores it.
6. The unscoped channel route lookups keep answering 409 `AmbiguousRoute` for
   two agents on one tracker location. No factory caller uses them; factory
   work routes by the WorkItem's agent.
7. Moving an issue's WorkItem between agents is not possible. A new issue is
   needed to run the same work under another label.
8. A downgrade below 0105 keeps a lone explicitly labelled surface and
   discards its label. The predecessor then routes issues carrying the
   install's configured tracker label at that location to that surface's
   agent, which previously admitted only its own label.
9. Rolling the application back without downgrading the schema leaves the
   widened key in place, so where labelled surfaces share a location the old
   application can find two rows on one route, which it was never written to
   expect. Remove all but one surface on each such location before rolling
   back.

## Alternatives considered

1. **One factory label per install, one agent per repository.** Rejected. It
   is the limit this decision removes, and running two agents on one
   repository otherwise needs a second install.
2. **Put the label in config bindings instead of on the surface.** Rejected.
   The surface is what makes an agent serve a location (ADR 0214), so the
   label that tells two agents apart belongs on the same row; a config side
   map would drift from the binding table.
3. **Backfill `trigger_label` with the install label.** Rejected. The tracker
   label lives in the API's forge config, which the schema migrate Job does
   not have, and NULL already means that label.
4. **Cancel on removal of any bound label.** Rejected. Removing one agent's
   label would cancel a run another agent's label started.
5. **Find the admitting label by resolving the binding at cancellation.**
   Rejected. Cancellation must not require the binding to exist (ADR 0161),
   and a tracker label change would strand running work.
6. **Namespace the state labels per agent.** Rejected. One issue has at most
   one WorkItem, so it has one writer.
7. **Write separate ADRs for the binding wiring and the label routing.**
   Rejected. Both define what a tracker surface is and how it routes, and one
   record keeps them from disagreeing.
