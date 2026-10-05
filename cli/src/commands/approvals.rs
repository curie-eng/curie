//! `<tier> approvals`: list, resolve, recover, and bind approval routes against the platform API.

use super::*;

/// The pending-list / resolve flags for `local approvals` (#506). Defaulted so
/// the skill/cluster tiers, which keep only the gate view/set surface, pass an
/// empty value.
#[derive(Default)]
pub struct ApprovalCmd {
    pub list: bool,
    pub resolve: Option<String>,
    pub reject: bool,
    pub note: Option<String>,
    /// Administrative subject for a reusable operator-principal mint.
    pub mint_operator_principal: Option<String>,
    /// Administrative subject for a single-use console login-code mint.
    pub mint_console_login_code: Option<String>,
    /// `--route-resolution NAME=CHANNEL`, repeatable.
    pub route_resolution: Vec<String>,
    /// `--route-approvers NAME=users:U1,U2` or `NAME=group:S1`, repeatable.
    pub route_approvers: Vec<String>,
    /// `--routes-from FILE`: the whole binding map as JSON.
    pub routes_from: Option<PathBuf>,
    pub list_routes: bool,
    pub clear_routes: bool,
    /// `--report-identity`: the installation-wide approval identity report.
    pub report_identity: bool,
    /// `--recover <APPROVAL_ID>`: administratively reject one approval under
    /// the installation-wide recovery grant (#2753).
    pub recover: Option<String>,
    /// Operator free text recorded on the recovery audit row. Required by
    /// `--recover`.
    pub reason: Option<String>,
    /// The CALLER's idempotency key for a recovery operation. Required, never
    /// generated: the server keys replay absorption on it.
    pub recovery_key: Option<String>,
}

// --- Approval route bindings (#1052) -----------------------------------------
//
// The verified resolution card, optional text-only notification, and who may
// resolve live as separate fields in the agent's `approval_routes` map. Until
// this verb existed the only way to write one was a hand-rolled
// `PATCH /agents/{id}`, against the repo's own "one entry point: curie
// <command>" rule.
//
// Two properties shape the code below.
//
// The write is a FULL REPLACEMENT, exactly as `--gate` already is for
// `approval_required_tools`. That is the field's semantics on `AgentUpdate`, and
// a merge would make the route inputs unable to express removal.
//
// Every parse and shape error is collected BEFORE any HTTP call. A partial write
// of a binding map is a silently widened (or silently narrowed) approver set,
// which is the failure ADR-0034 fails closed against, so a malformed entry
// anywhere aborts the whole invocation with nothing sent.

/// Slack ID shapes, mirroring the authoritative validators in
/// `apps/api/src/curie_api/schemas/channels.py`. The API is the gate for every caller and
/// re-checks all of these; these exist only so a typo is answered locally with a
/// fix hint instead of a round trip (the same split the API's own
/// `validate_channel_binding` docstring describes).
pub(super) static SLACK_USERGROUP_ID: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^S[A-Z0-9]{7,}$").expect("usergroup id re"));
pub(super) static SLACK_USER_ID: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^[UW][A-Z0-9]{7,}$").expect("user id re"));
/// One bare email address, mirroring the API's `_EMAIL_CALLER`: no display name,
/// list, or wildcard (ADR-0177 amendment approver emails, ADR 0175 callers).
pub(super) static BARE_EMAIL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r#"^[^@\s<>,;"()*]+@[^@\s<>,;"()*]+$"#).expect("bare email re")
});
pub(super) static CHANNEL_KIND: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| {
        regex::Regex::new(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$").expect("channel kind re")
    });

/// Split `NAME=VALUE` once, rejecting an empty half.
///
/// Splits at the FIRST `=` so a value containing one is preserved; a route name
/// containing `=` is not expressible, which is fine because a route name is a
/// manifest identifier matched verbatim against `approvalPolicy`.
pub(super) fn split_route_arg<'a>(flag: &str, raw: &'a str) -> Result<(&'a str, &'a str)> {
    let (name, value) = raw.split_once('=').ok_or_else(|| {
        crate::exit::usage(format!(
            "{flag} {raw:?} is not NAME=VALUE; pass e.g. {flag} deal_desk=C0EXAMPLE1"
        ))
    })?;
    let (name, value) = (name.trim(), value.trim());
    if name.is_empty() {
        return Err(crate::exit::usage(format!(
            "{flag} {raw:?} has an empty route name; the name must match a route the \
             bundle manifest's approvalPolicy declares"
        )));
    }
    if value.is_empty() {
        return Err(crate::exit::usage(format!(
            "{flag} {raw:?} has an empty value"
        )));
    }
    Ok((name, value))
}

/// Parse one `--route-approvers NAME=KIND:VALUES` value into its approver set.
pub(super) fn parse_route_approvers(value: &str) -> Result<crate::api::ApprovalApprovers> {
    let (kind, rest) = value.split_once(':').ok_or_else(|| {
        crate::exit::usage(format!(
            "--route-approvers value {value:?} is not KIND:VALUES; pass \
             users:U0123ABCD,U0456DEFG or group:S0123ABCD"
        ))
    })?;
    match kind.trim() {
        "users" => {
            let users: Vec<String> = rest
                .split(',')
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
                .collect();
            if users.is_empty() {
                // The API refuses an empty list for the same reason: as silent
                // config, "nobody may approve" means the request can only expire.
                return Err(crate::exit::usage(
                    "--route-approvers users: needs at least one Slack user ID",
                ));
            }
            for user in &users {
                if !SLACK_USER_ID.is_match(user) {
                    return Err(crate::exit::CliError::usage(format!(
                        "approvers user {user:?} is not a Slack user ID"
                    ))
                    .with_fix(
                        "pass the user ID (e.g. U0123ABCD, or W0123ABCD on enterprise \
                         grid), not a @handle or a display name: a member's ID is under \
                         their profile's More menu, Copy member ID",
                    )
                    .into());
                }
            }
            Ok(crate::api::ApprovalApprovers {
                group: None,
                users: Some(users),
                emails: None,
            })
        }
        "group" => {
            let group = rest.trim().to_string();
            if !SLACK_USERGROUP_ID.is_match(&group) {
                // Naming the C-prefix case explicitly: a channel ID here is the
                // likely mistake, and it is the one that would otherwise look
                // plausible enough to debug for a while.
                return Err(crate::exit::CliError::usage(format!(
                    "approvers group {group:?} is not a Slack user-group ID"
                ))
                .with_fix(
                    "pass a user-group ID (e.g. S0123ABCD), not a @handle or a name; a \
                     C-prefixed value is a CHANNEL, not a user group. List group IDs via \
                     the Slack usergroups.list API",
                )
                .into());
            }
            Ok(crate::api::ApprovalApprovers {
                group: Some(group),
                users: None,
                emails: None,
            })
        }
        other => Err(crate::exit::usage(format!(
            "--route-approvers kind {other:?} is not recognized; pass `users:` or `group:`"
        ))),
    }
}

/// Build the binding map a write should send from a strict file plus overrides.
///
/// `--routes-from` seeds the map and the repeatable flags apply on top, so a
/// committed file can be spot-overridden on the command line. Every error is a
/// usage error raised before the caller opens a connection.
pub(super) fn build_route_bindings(
    route_resolution: &[String],
    route_approvers: &[String],
    routes_from: Option<&PathBuf>,
) -> Result<BTreeMap<String, crate::api::ApprovalRouteBindingWrite>> {
    let mut bindings: BTreeMap<String, crate::api::ApprovalRouteBindingWrite> = BTreeMap::new();

    if let Some(path) = routes_from {
        let text = std::fs::read_to_string(path).map_err(|e| {
            crate::exit::usage(format!(
                "--routes-from {}: {e}; the file must be JSON shaped \
                 {{\"<route>\": {{\"resolution\": \
                 {{\"kind\": \"slack\", \"address\": \"C0EXAMPLE1\"}}}}}}",
                path.display()
            ))
        })?;
        // Decode in two steps rather than straight into the binding map, so an
        // unknown key can be reported against the ROUTE that carries it. Serde's
        // own error names the key but not which entry of the map it came from,
        // and "unknown field `approver`" with no route is a poor thing to hand
        // someone holding a twenty-route file.
        let raw: BTreeMap<String, serde_json::Value> =
            serde_json::from_str(&text).map_err(|e| {
                crate::exit::usage(format!(
                    "--routes-from {}: {e}; expected JSON shaped \
                     {{\"<route>\": {{\"resolution\": \
                     {{\"kind\": \"slack\", \"address\": \"C0EXAMPLE1\"}}, \
                     \"approvers\": {{\"group\": \"S0123ABCD\"}}}}}}",
                    path.display()
                ))
            })?;
        for (name, value) in raw {
            // RouteBindingInput is the strict, operator-file twin of the
            // response-side type: it refuses an unknown key instead of dropping
            // it (#1072). A dropped `approver` would leave authority falling
            // back to the whole resolution-card channel.
            let input: crate::api::RouteBindingInput =
                serde_json::from_value(value).map_err(|e| {
                    anyhow::Error::from(
                        crate::exit::CliError::usage(format!(
                            "--routes-from {}: route {name:?}: {e}",
                            path.display()
                        ))
                        .with_fix(
                            "a route binding requires `resolution: {kind: \"slack\", \
                             address: \"C...\"}` or `resolution: {mode: \
                             \"requesting_surface\"}` and accepts optional `notification` and \
                             `approvers` blocks, and nothing else. The retired `channel` key \
                             is not accepted",
                        ),
                    )
                })?;
            let binding: crate::api::ApprovalRouteBindingWrite = input.into();
            if let Some(approvers) = &binding.approvers {
                validate_parsed_approvers(&name, approvers)?;
            }
            bindings.insert(name, binding);
        }
    }

    for raw in route_resolution {
        let (name, channel) = split_route_arg("--route-resolution", raw)?;
        bindings
            .entry(name.to_string())
            .and_modify(|b| b.resolution = crate::api::ApprovalResolutionWrite::slack(channel))
            .or_insert_with(|| crate::api::ApprovalRouteBindingWrite {
                resolution: crate::api::ApprovalResolutionWrite::slack(channel),
                notification: None,
                approvers: None,
            });
    }

    for raw in route_approvers {
        let (name, value) = split_route_arg("--route-approvers", raw)?;
        let approvers = parse_route_approvers(value)?;
        // Approvers narrow an EXISTING binding; without a resolution there is
        // no verified card surface. Refuse rather than invent one.
        let binding = bindings.get_mut(name).ok_or_else(|| {
            anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "--route-approvers {name:?} names a route with no resolution"
                ))
                .with_fix(format!(
                    "add --route-resolution {name}=<CHANNEL> to the same invocation (or name the \
                     route in --routes-from): a write replaces the whole route map, so \
                     every route it should keep must be present"
                )),
            )
        })?;
        binding.approvers = Some(approvers);
    }

    // Overrides can invalidate a previously valid file binding (for example,
    // by moving resolution onto its notification target), so validate only the
    // complete final map. Nothing reaches HTTP unless every binding survives.
    for (name, binding) in &bindings {
        validate_resolution_target(name, &binding.resolution)?;
        if let Some(notification) = &binding.notification {
            let crate::api::ApprovalResolutionWrite::Fixed(resolution) = &binding.resolution else {
                // ADR-0177 decision 1, mirroring the API: the card already
                // joins the conversation that asked, so there is nobody further
                // to notify.
                return Err(crate::exit::usage(format!(
                    "route {name:?}: a requesting_surface resolution cannot carry a \
                     notification; the card is already shown in the conversation that asked"
                )));
            };
            validate_notification_target(name, notification)?;
            reject_identical_targets(name, resolution, notification)?;
        }
        if let Some(approvers) = &binding.approvers {
            validate_parsed_approvers(name, approvers)?;
            // ADR-0177 amendment, mirroring the API: only a requesting_surface route shows
            // its card in an email thread. On a fixed Slack target an address
            // can never be verified, so the list could only admit nobody.
            if approvers.emails.is_some()
                && matches!(
                    binding.resolution,
                    crate::api::ApprovalResolutionWrite::Fixed(_)
                )
            {
                return Err(crate::exit::CliError::usage(format!(
                    "route {name:?}: approvers emails need a requesting_surface resolution"
                ))
                .with_fix(
                    "write the route's resolution as {\"mode\": \"requesting_surface\"} in \
                     --routes-from, or list Slack users instead: a fixed target shows its \
                     card in Slack, where an email address cannot be verified",
                )
                .into());
            }
        }
    }

    Ok(bindings)
}

/// A fixed target stays Slack-only; the only way off Slack is the
/// `requesting_surface` mode, which shows the card in the conversation that
/// asked (ADR-0177). Mirrors the API's `ApprovalRouteBinding.resolution`.
pub(super) fn validate_resolution_target(
    route: &str,
    resolution: &crate::api::ApprovalResolutionWrite,
) -> Result<()> {
    let target = match resolution {
        crate::api::ApprovalResolutionWrite::Fixed(target) => target,
        crate::api::ApprovalResolutionWrite::RequestingSurface(target) => {
            if target.mode != "requesting_surface" {
                return Err(crate::exit::usage(format!(
                    "route {route:?}: resolution mode {:?} is unsupported; the only mode is \
                     \"requesting_surface\"",
                    target.mode
                )));
            }
            return Ok(());
        }
    };
    if target.kind != "slack" {
        return Err(crate::exit::usage(format!(
            "route {route:?}: resolution kind {:?} is unsupported; only slack can carry \
             a verified resolver identity",
            target.kind
        )));
    }
    validate_route_channel(route, &target.address)
}

/// Validate the notification identity and its independent transport route.
pub(super) fn validate_notification_target(
    route: &str,
    target: &crate::api::NotificationTargetWrite,
) -> Result<()> {
    if !CHANNEL_KIND.is_match(&target.kind) {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification kind {:?} is not a lowercase channel-kind slug",
            target.kind
        )));
    }
    if target.kind == "slack" {
        validate_route_channel(route, &target.address)?;
    } else if target.address.is_empty() || target.address.chars().any(char::is_whitespace) {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification address must be non-empty and contain no whitespace"
        )));
    }
    if target.kind == "slack" {
        if target.endpoint.is_some() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: a Slack notification names its identity in adapter and takes \
                 no endpoint"
            )));
        }
    } else if target.endpoint.is_none() || target.adapter.is_none() {
        return Err(crate::exit::CliError::usage(format!(
            "route {route:?}: non-Slack notification kind {:?} requires both endpoint and adapter",
            target.kind
        ))
        .with_fix(
            "put the complete notification object in --routes-from, including an absolute \
             endpoint URL and adapter name",
        )
        .into());
    }
    if let Some(adapter) = &target.adapter {
        if !CHANNEL_KIND.is_match(adapter) {
            return Err(crate::exit::usage(format!(
                "route {route:?}: notification adapter is not a lowercase slug"
            )));
        }
    }
    if let Some(endpoint) = &target.endpoint {
        let parsed = reqwest::Url::parse(endpoint).map_err(|_| {
            crate::exit::usage(format!(
                "route {route:?}: notification endpoint must be an absolute http(s) URL with a host"
            ))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: notification endpoint must be an absolute http(s) URL with a host"
            )));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: notification endpoint must not contain userinfo; use adapter credentials"
            )));
        }
    }
    Ok(())
}

pub(super) fn reject_identical_targets(
    route: &str,
    resolution: &crate::api::ApprovalResolutionTargetWrite,
    notification: &crate::api::NotificationTargetWrite,
) -> Result<()> {
    if resolution.kind == notification.kind && resolution.address == notification.address {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification must differ from resolution; a duplicate target \
             is not a second notification surface"
        )));
    }
    Ok(())
}

/// Render route bindings for a dry-run without exposing credential-bearing
/// adapter URL paths, queries, or fragments. The origin is enough to identify
/// the transport destination; the real PATCH retains the complete endpoint.
pub(super) fn route_bindings_plan_json(
    bindings: &BTreeMap<String, crate::api::ApprovalRouteBindingWrite>,
) -> String {
    let mut display = bindings.clone();
    for binding in display.values_mut() {
        let Some(endpoint) = binding
            .notification
            .as_mut()
            .and_then(|target| target.endpoint.as_mut())
        else {
            continue;
        };
        *endpoint = reqwest::Url::parse(endpoint)
            .map(|url| url.origin().ascii_serialization())
            .unwrap_or_else(|_| "<redacted>".to_string());
    }
    serde_json::to_string(&display).unwrap_or_else(|_| "<unserializable>".to_string())
}

/// Channel-shape check for one route binding, with the route named in the error.
pub(super) fn validate_route_channel(route: &str, channel: &str) -> Result<()> {
    if !crate::credcheck::looks_like_slack_channel_id(channel) {
        return Err(crate::exit::CliError::usage(format!(
            "route {route:?}: {channel:?} is not a Slack channel ID. Real Slack events \
             carry the ID and the worker routes on it, so a #name binding never \
             receives messages"
        ))
        .with_fix(
            "pass the channel ID (e.g. C0EXAMPLE1): find it in the channel's About tab, \
             or at the end of the channel URL (.../archives/C0EXAMPLE1)",
        )
        .into());
    }
    Ok(())
}

/// Re-run the flag-path approver checks over a `--routes-from` block, so the two
/// input forms cannot disagree about what a valid binding is.
pub(super) fn validate_parsed_approvers(
    route: &str,
    approvers: &crate::api::ApprovalApprovers,
) -> Result<()> {
    if approvers.group.is_none() && approvers.users.is_none() && approvers.emails.is_none() {
        return Err(crate::exit::usage(format!(
            "route {route:?}: an approvers block must declare group, users or emails; omit \
             the block entirely to keep card-channel membership"
        )));
    }
    if let Some(emails) = &approvers.emails {
        if emails.is_empty() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: approvers emails, when present, must contain at least \
                 one address"
            )));
        }
        for email in emails {
            if !BARE_EMAIL.is_match(email) {
                return Err(crate::exit::usage(format!(
                    "route {route:?}: approvers email {email:?} is not one bare email address \
                     (e.g. approver@example.com, with no display name or wildcard)"
                )));
            }
        }
    }
    if let Some(group) = &approvers.group {
        if !SLACK_USERGROUP_ID.is_match(group) {
            return Err(crate::exit::usage(format!(
                "route {route:?}: approvers group {group:?} is not a Slack user-group ID \
                 (e.g. S0123ABCD); a C-prefixed value is a channel, not a user group"
            )));
        }
    }
    if let Some(users) = &approvers.users {
        if users.is_empty() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: approvers users, when present, must contain at least \
                 one user ID"
            )));
        }
        for user in users {
            if !SLACK_USER_ID.is_match(user) {
                return Err(crate::exit::usage(format!(
                    "route {route:?}: approvers user {user:?} is not a Slack user ID \
                     (e.g. U0123ABCD, or W0123ABCD on enterprise grid)"
                )));
            }
        }
    }
    Ok(())
}

/// Output of `<tier> approvals <agent>`: the dry-run plan, the gate list (empty
/// vec == "no tools gated"), the pending records, or a resolved record (#506).
/// Owns its data so it outlives the `ApiClient`.
///
/// `manifest_unreadable` carries the third gate-list state (#607): `gated_tools`
/// alone cannot distinguish "the deployed bundle manifest declares no gates" from
/// "the manifest could not be read at all" -- both used to arrive as an empty vec
/// and render as the affirmative "calls run without approval". `Some(reason)`
/// means the manifest lookup failed, so the list is what we could see rather than
/// what is armed. Always `None` on the set path (`--gate`/`--clear`).
pub enum ApprovalsOutput {
    DryRun(crate::ui::DryRunPlan),
    Gates {
        agent: String,
        gated_tools: Vec<String>,
        manifest_unreadable: Option<String>,
    },
    Pending {
        agent: String,
        records: Vec<crate::api::ApprovalRecord>,
        /// Current route bindings fetched with the agent for this list read.
        /// These are display context, never a snapshot of resolve authority.
        routes: BTreeMap<String, crate::api::ApprovalRouteBindingResponse>,
        /// `true` when `records.len()` hit the server's page-size cap
        /// (`ApiClient::APPROVALS_LIST_LIMIT`), meaning more pending approvals
        /// may exist beyond what was fetched (#670). Always present (never
        /// conditionally omitted), per the repo's superset-JSON convention.
        truncated: bool,
    },
    Resolved {
        record: crate::api::ApprovalRecord,
    },
    /// The installation-wide approval identity report (#2753). Forwarded as the
    /// server rendered it: it is diagnostic evidence plus the declaration
    /// skeleton the operator fills in, so a field this CLI does not know about
    /// must still reach the operator rather than be silently dropped.
    IdentityReport {
        report: serde_json::Value,
    },
    /// One approval administratively rejected under the recovery grant. Carries
    /// the route's RECORDED outcome (`ApprovalRecoveryOut`), not an ordinary
    /// approval record: the recovery route answers with the administrative
    /// facts, and a record shape would not decode at all.
    Recovered {
        outcome: crate::api::ApprovalRecoveryOutcome,
    },
    /// One-time delivery of a reusable operator credential. The token is never
    /// persisted and is emitted only from this explicit mint result.
    OperatorPrincipal {
        delivery: crate::api::OperatorPrincipalDelivery,
    },
    /// One-time delivery of a subject-bound console login code.
    ConsoleLoginCode {
        delivery: crate::api::ConsoleLoginCodeDelivery,
    },
    /// The agent's approval route bindings, read back after `--list-routes` or
    /// after a write. `routes` empty means no route is bound, so any route the
    /// bundle names escalates to a human rather than posting a card.
    Routes {
        agent: String,
        routes: BTreeMap<String, crate::api::ApprovalRouteBindingResponse>,
    },
}

pub(super) fn approval_record_json(r: &crate::api::ApprovalRecord) -> serde_json::Value {
    serde_json::json!({
        "id": r.id,
        "author": r.author,
        "route": r.route,
        "gate_kind": r.gate_kind,
        "granted_tool": r.granted_tool,
        "status": r.status,
        "conversation_id": r.conversation_id,
        "summary": r.summary,
        "expires_at": r.expires_at,
        "resolved_by": r.resolved_by,
        // #1078: the one field --resolve cannot be driven without. Null only
        // for an older row or a direct API write that omitted this field.
        "card_channel": r.card_channel,
    })
}

pub(super) fn pending_approval_record_json(
    record: &crate::api::ApprovalRecord,
    routes: &BTreeMap<String, crate::api::ApprovalRouteBindingResponse>,
) -> serde_json::Value {
    let mut row = approval_record_json(record);
    let current_route_approvers = record
        .route
        .as_ref()
        .and_then(|route| routes.get(route))
        .and_then(|binding| binding.approvers.as_ref());
    // This reports the route configuration observed when listing. The API
    // reads the binding again when an actor attempts to resolve the approval.
    row["current_route_approvers"] = serde_json::json!(current_route_approvers);
    row
}

impl crate::ui::CliOutput for ApprovalsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ApprovalsOutput::DryRun(plan) => plan.to_json(),
            ApprovalsOutput::Gates {
                agent,
                gated_tools,
                manifest_unreadable,
            } => serde_json::json!({
                "agent": agent,
                "gated_tools": gated_tools,
                "manifest_unreadable": manifest_unreadable,
            }),
            ApprovalsOutput::Pending {
                agent,
                records,
                routes,
                truncated,
            } => serde_json::json!({
                "agent": agent,
                "pending": records.iter().map(|record| pending_approval_record_json(record, routes)).collect::<Vec<_>>(),
                "count": records.len(),
                "truncated": truncated,
            }),
            ApprovalsOutput::Resolved { record } => serde_json::json!({
                "resolved": approval_record_json(record),
            }),
            ApprovalsOutput::IdentityReport { report } => serde_json::json!({
                "identity_report": report,
            }),
            ApprovalsOutput::Recovered { outcome } => serde_json::json!({
                "recovered": {
                    "approval_id": outcome.approval_id,
                    "status": outcome.status,
                    "recovery_key": outcome.recovery_key,
                    "reason": outcome.reason,
                    "actor": outcome.actor,
                    "recovered_at": outcome.recovered_at,
                },
            }),
            ApprovalsOutput::OperatorPrincipal { delivery } => serde_json::json!({
                "operator_principal": {
                    "token": delivery.token,
                    "subject": delivery.subject,
                    "expires_at": delivery.expires_at,
                },
            }),
            ApprovalsOutput::ConsoleLoginCode { delivery } => serde_json::json!({
                "console_login_code": {
                    "code": delivery.code,
                    "subject": delivery.subject,
                    "expires_at": delivery.expires_at,
                },
            }),
            ApprovalsOutput::Routes { agent, routes } => serde_json::json!({
                "agent": agent,
                "routes": routes,
                "count": routes.len(),
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ApprovalsOutput::DryRun(plan) => plan.render(ui),
            ApprovalsOutput::Gates {
                agent,
                gated_tools,
                manifest_unreadable,
            } => {
                ui.payload(&approvals_summary_line(
                    agent,
                    gated_tools,
                    manifest_unreadable.as_deref(),
                ));
                for tool in gated_tools {
                    ui.kv("gated", tool);
                }
            }
            ApprovalsOutput::Pending {
                agent,
                records,
                routes,
                truncated,
            } => {
                if records.is_empty() {
                    ui.payload(&format!("{agent}: no pending approvals"));
                } else {
                    ui.payload(&format!("{agent} — {} pending approval(s):", records.len()));
                    for r in records {
                        let tool =
                            crate::render::action_label(r.granted_tool.as_deref().unwrap_or(""));
                        let display = crate::approval_wording::approval_display(r);
                        let route = r.route.as_deref().unwrap_or("(requesting channel)");
                        // A null card_channel means an older row or a direct API
                        // write that omitted the field, for which the requesting
                        // channel applies (#1431); it must NOT render as "null",
                        // "none" or "-", which would state the wrong fact.
                        // The server picks approvers from `card_channel or
                        // reply_channel`, and in Python only the empty string
                        // is falsy, so an empty card_channel is absent too and
                        // must show the same requesting-channel meaning as the
                        // resolve hint in message.rs. A whitespace-only channel
                        // is truthy in Python and is NOT absent, so it is still
                        // printed verbatim here; do not trim it.
                        let card = r
                            .card_channel
                            .as_deref()
                            .filter(|c| !c.is_empty())
                            .unwrap_or("(requesting channel)");
                        let approvers = match r.route.as_deref() {
                            Some(route) => routes.get(route).map_or_else(
                                || {
                                    "route binding missing; restore it before resolution"
                                        .to_string()
                                },
                                describe_approvers,
                            ),
                            None => format!("members of {card} (routeless default)"),
                        };
                        ui.kv(
                            &r.id,
                            &format!(
                                "{}: {} [action: {tool}, route: {route}, channel: {card}, current route approvers: {approvers}, by: {}]",
                                display, r.conversation_id, r.author
                            ),
                        );
                    }
                }
                if *truncated {
                    ui.payload(&format!(
                        "this list is capped at {} (the server max) and more pending approvals \
                         may exist; resolve some and re-run to see the rest",
                        crate::api::ApiClient::APPROVALS_LIST_LIMIT
                    ));
                }
            }
            ApprovalsOutput::Resolved { record } => {
                ui.payload(&format!(
                    "approval {} -> {} (by {})",
                    record.id,
                    record.status,
                    record.resolved_by.as_deref().unwrap_or("?")
                ));
            }
            ApprovalsOutput::IdentityReport { report } => {
                let approvals = report
                    .get("approvals")
                    .and_then(|value| value.as_array())
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                if approvals.is_empty() {
                    ui.payload("no pending approval row was reported");
                } else {
                    ui.payload(&format!("{} pending approval row(s):", approvals.len()));
                    for row in approvals {
                        let id = row.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                        // The observations live in `facts`, a string array, NOT
                        // in boolean columns. Reading booleans hid
                        // `card_identity_missing` and
                        // `reply_identity_unreconstructable` -- the two
                        // obligations an operator has to recover -- while
                        // surfacing the unrelated `has_reply_placeholder` flag.
                        let facts: Vec<&str> = row
                            .get("facts")
                            .and_then(|value| value.as_array())
                            .map(|values| values.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        // A row with no fact is REPORTED, not dropped: the
                        // reporter excludes nothing, and a renderer that hid
                        // the healthy rows would answer a different question.
                        let facts = if facts.is_empty() {
                            "(no identity fact observed)".to_string()
                        } else {
                            facts.join(", ")
                        };
                        ui.kv(id, &facts);
                    }
                }
                let declarations = report
                    .get("declarations")
                    .and_then(|value| value.as_array())
                    .map(Vec::len)
                    .unwrap_or(0);
                ui.payload(&format!(
                    "declarations carries {declarations} entr(y/ies) to fill in and feed back \
                     to the upgrade; re-run with --json to capture it"
                ));
            }
            ApprovalsOutput::Recovered { outcome } => {
                ui.payload(&format!(
                    "approval {} administratively recovered -> {} by {} (this act is audited)",
                    outcome.approval_id,
                    outcome.status,
                    outcome.actor.as_deref().unwrap_or("?")
                ));
            }
            ApprovalsOutput::OperatorPrincipal { delivery } => {
                ui.note(&format!(
                    "minted an operator approval principal for {} (expires {}); assign the \
                     token to CURIE_APPROVAL_PRINCIPAL_TOKEN without pasting it into shell history",
                    delivery.subject, delivery.expires_at
                ));
                // This explicit mint result is the token's one delivery. Keep
                // it off progress/error output and do not cache it.
                ui.payload_plain(&delivery.token);
            }
            ApprovalsOutput::ConsoleLoginCode { delivery } => {
                ui.note(&format!(
                    "minted a console login code for {} (expires {})",
                    delivery.subject, delivery.expires_at
                ));
                // The browser exchanges this plaintext once; the CLI never
                // stores or repeats it.
                ui.payload_plain(&delivery.code);
            }
            ApprovalsOutput::Routes { agent, routes } => {
                if routes.is_empty() {
                    // Not a neutral empty list: with nothing bound, a route the
                    // bundle names is escalated to a human instead of posting a
                    // card, which is the state operators most often mistake for
                    // "approvals are broken".
                    ui.payload(&format!(
                        "{agent}: no approval routes bound. Any route the bundle's \
                         approvalPolicy names will escalate to a human rather than post \
                         a card, since there is no channel to post it to"
                    ));
                } else {
                    ui.payload(&format!(
                        "{agent} — {} approval route(s) bound:",
                        routes.len()
                    ));
                    for (name, binding) in routes {
                        let resolution = match &binding.resolution {
                            crate::api::ApprovalResolutionResponse::Fixed(target) => format!(
                                "resolution {}:{} (verified interactive card)",
                                target.kind, target.address
                            ),
                            crate::api::ApprovalResolutionResponse::RequestingSurface(_) => {
                                "resolution requesting_surface (the card is shown in the \
                                 conversation that asked)"
                                    .to_string()
                            }
                        };
                        ui.kv(name, &resolution);
                        let notification = binding
                            .notification
                            .as_ref()
                            .map(|target| format!("{}:{} (text-only)", target.kind, target.address))
                            .unwrap_or_else(|| "(none)".to_string());
                        ui.kv("", &format!("  notification: {notification}"));
                        ui.kv("", &format!("  approvers: {}", describe_approvers(binding)));
                    }
                }
            }
        }
    }
}

/// One line naming who may resolve a route's approvals, including the default.
pub(super) fn describe_approvers(binding: &crate::api::ApprovalRouteBindingResponse) -> String {
    match &binding.approvers {
        None => match &binding.resolution {
            crate::api::ApprovalResolutionResponse::Fixed(target) => format!(
                "members of {}:{} (the default: no approvers block declared)",
                target.kind, target.address
            ),
            crate::api::ApprovalResolutionResponse::RequestingSurface(_) => {
                "the asking channel's members in Slack, and nobody on any other channel \
                 (the default: no approvers block declared)"
                    .to_string()
            }
        },
        Some(a) => {
            let slack = describe_slack_approvers(a);
            match &a.emails {
                Some(emails) => format!("{slack}; on email, {}", emails.join(", ")),
                None => slack,
            }
        }
    }
}

/// The Slack half of an approvers block: who may answer a card shown in Slack.
pub(super) fn describe_slack_approvers(a: &crate::api::ApprovalApprovers) -> String {
    match (&a.users, &a.group) {
        // Mirror the API's precedence in the wording rather than hiding it:
        // `users` wins over `group`, so a binding carrying both must not read
        // as though the group also decides.
        (Some(users), Some(group)) => format!(
            "users {} (an explicit list wins over group {group}; the click channel is ignored)",
            users.join(", ")
        ),
        (Some(users), None) => {
            format!("users {} (the click channel is ignored)", users.join(", "))
        }
        (None, Some(group)) => {
            format!("members of Slack user group {group} (the click channel is ignored)")
        }
        (None, None) if a.emails.is_some() => "nobody in Slack".to_string(),
        (None, None) => "unreadable: the block declares neither users nor group".to_string(),
    }
}

/// The human summary line for `<tier> approvals`' gate view.
///
/// Four lines for three states, because whether gates were found is orthogonal to
/// whether we could read the manifest that declares them. The `unreadable` arm is
/// the one that matters: an unanswered lookup must not borrow the vocabulary of an
/// answered one. Reporting "no tools are gated (calls run without approval)"
/// because the deployment list request errored tells the reader the runner will
/// not pause, which is a claim this command never checked -- and the reader acts on
/// it. Same reasoning as the skill tier's `gates_summary_line`, where the unseen
/// source is the boot-time env override rather than the deployed manifest.
///
/// The unreadable-with-gates arm is not redundant: gates from the platform's
/// `approval_required_tools` field are real, but presenting them without the
/// caveat implies the list is the whole set.
pub(super) fn approvals_summary_line(
    agent: &str,
    gated_tools: &[String],
    unreadable: Option<&str>,
) -> String {
    match (gated_tools.is_empty(), unreadable) {
        (true, None) => format!("{agent}: no tools are gated (calls run without approval)"),
        (false, None) => format!("{agent} — {} gated tool(s):", gated_tools.len()),
        (true, Some(reason)) => format!(
            "{agent}: the deployed bundle manifest could not be read ({reason}), so whether it \
             gates any tool is unknown. The platform's approval_required_tools field lists none, \
             which is not the same as nothing being gated"
        ),
        (false, Some(reason)) => format!(
            "{agent} — {} gated tool(s), and this list may be incomplete: the deployed bundle \
             manifest could not be read ({reason}), so any gate it declares is not shown:",
            gated_tools.len()
        ),
    }
}

/// Mint a Console login code bound to the supplied subject.
pub async fn console_login(
    api_url: &str,
    resolved_api_key: &str,
    subject: &str,
    dry_run: bool,
) -> Result<ApprovalsOutput> {
    if subject.trim().is_empty() {
        return Err(crate::exit::usage(
            "the principal subject must not be blank",
        ));
    }
    if dry_run {
        return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/console/login-codes subject={subject:?} (the code is returned only by a real run)",
                api_url.trim_end_matches('/')
            )],
        }));
    }
    let client = ApiClient::with_resolved_key(api_url, resolved_api_key)?;
    Ok(ApprovalsOutput::ConsoleLoginCode {
        delivery: client.mint_console_login_code(subject).await?,
    })
}

/// `<tier> approvals <agent> [--gate TOOL]... [--clear]`: view or set the tool
/// names whose calls pause for human approval. No flags => show current gates.
pub async fn approvals(
    opts: AgentActionOpts,
    gate: Vec<String>,
    clear: bool,
    cmd: ApprovalCmd,
) -> Result<ApprovalsOutput> {
    let gate_mode = clear || !gate.is_empty();

    // The split target/approver flags address the agent's approval route map.
    // Handled ahead of every other branch because it is a distinct
    // object from both the tool gates and the pending records, and mixing it with
    // either in one invocation would make the write's replace-the-whole-map
    // semantics ambiguous.
    let route_write = !cmd.route_resolution.is_empty()
        || !cmd.route_approvers.is_empty()
        || cmd.routes_from.is_some()
        || cmd.clear_routes;

    // --- Administrative recovery (#2753) -------------------------------------
    //
    // Handled ahead of EVERY other branch, including the route and mint blocks,
    // because each of those refuses with a message naming only its own flags. A
    // recovery verb combined with one of them has to be refused by a message
    // that names BOTH sides, otherwise the operator is told to drop a flag they
    // did not think they were using.
    //
    // These verbs address the durable approval STORE under an
    // installation-wide grant, not this agent's gates, routes or pending list.
    // The agent argument is positional on the verb and is deliberately not used
    // to scope them: the report is installation-wide and a recovery is
    // id-addressed, so resolving an agent first would add a lookup that can fail
    // for reasons unrelated to the act being performed.
    let recovery_verbs: Vec<&str> = [
        cmd.report_identity.then_some("--report-identity"),
        cmd.recover.as_ref().map(|_| "--recover"),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !recovery_verbs.is_empty() {
        if recovery_verbs.len() > 1 {
            return Err(crate::exit::usage(format!(
                "{} are separate administrative acts against the same approval row; \
                 combining them in one invocation would make the durable outcome \
                 ambiguous. Run one per invocation",
                recovery_verbs.join(" and "),
            )));
        }
        let verb = recovery_verbs[0];
        let conflicts: Vec<&str> = [
            cmd.list.then_some("--list"),
            cmd.resolve.as_ref().map(|_| "--resolve"),
            cmd.list_routes.then_some("--list-routes"),
            cmd.clear_routes.then_some("--clear-routes"),
            (!cmd.route_resolution.is_empty()).then_some("--route-resolution"),
            (!cmd.route_approvers.is_empty()).then_some("--route-approvers"),
            cmd.routes_from.as_ref().map(|_| "--routes-from"),
            (!gate.is_empty()).then_some("--gate"),
            clear.then_some("--clear"),
            cmd.mint_operator_principal
                .as_ref()
                .map(|_| "--mint-operator-principal"),
            cmd.mint_console_login_code
                .as_ref()
                .map(|_| "--mint-console-login-code"),
        ]
        .into_iter()
        .flatten()
        .collect();
        if !conflicts.is_empty() {
            return Err(crate::exit::usage(format!(
                "{verb} is an administrative act on the durable approval store; it cannot \
                 be combined with {} (tool gates, route bindings, and the ordinary \
                 pending-record verbs address different objects). Run them as separate \
                 invocations",
                conflicts.join(", "),
            )));
        }

        if cmd.report_identity {
            if opts.dry_run {
                return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                    lines: vec![format!(
                        "GET {}/approvals/identity-report  (installation-wide; no agent \
                         lookup, no principal, nothing mutated)",
                        opts.api_url
                    )],
                }));
            }
            let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
            return Ok(ApprovalsOutput::IdentityReport {
                report: client.approval_identity_report().await?,
            });
        }

        let approval_id = cmd
            .recover
            .as_deref()
            .expect("--recover is the only mutating recovery verb left to reach here");

        // Every input is validated BEFORE any network call and before the
        // principal is even read, so a malformed administrative act can never
        // half-happen and never reaches the API for it to reject.
        let reason = cmd
            .reason
            .as_deref()
            .filter(|reason| !reason.trim().is_empty())
            .ok_or_else(|| {
                crate::exit::usage(format!(
                    "{verb} requires a non-empty --reason. It is written verbatim to the \
                     durable audit row, which is the only record of why this approval was \
                     administratively disposed of"
                ))
            })?;
        // Deliberately NOT generated, defaulted or decorated: the server's
        // idempotency is keyed on this value, so a CLI-chosen one would turn
        // every retry into a fresh administrative act.
        let recovery_key = cmd
            .recovery_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .ok_or_else(|| {
                crate::exit::usage(format!(
                    "{verb} requires --recovery-key. The key is caller-supplied so that \
                     retrying the identical command is absorbed by the server as one act; \
                     the CLI will not invent one"
                ))
            })?;

        if opts.dry_run {
            let line = format!(
                "POST {}/approvals/{approval_id}/recover disposition=rejected \
                 recovery_key={recovery_key:?} (attributed to the principal read from \
                 CURIE_APPROVAL_PRINCIPAL_TOKEN at execution)",
                opts.api_url
            );
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![line],
            }));
        }

        let principal_token = std::env::var("CURIE_APPROVAL_PRINCIPAL_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                crate::exit::usage(format!(
                    "{verb} requires CURIE_APPROVAL_PRINCIPAL_TOKEN, which names WHO acted \
                     in the audit row (the platform key authorizes the act but identifies \
                     nobody). Mint a reusable operator credential with `curie \
                     <local|cluster> approvals <AGENT> --mint-operator-principal <SUBJECT>`, \
                     export the one-time result, and retry"
                ))
            })?;
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        return Ok(ApprovalsOutput::Recovered {
            outcome: client
                .recover_approval(approval_id, reason, recovery_key, &principal_token)
                .await?,
        });
    }

    let mint_subject = match (
        cmd.mint_operator_principal.as_deref(),
        cmd.mint_console_login_code.as_deref(),
    ) {
        (Some(_), Some(_)) => {
            return Err(crate::exit::usage(
                "--mint-operator-principal and --mint-console-login-code are separate \
                 administrative actions; run one per invocation",
            ));
        }
        (Some(subject), None) => Some(("operator", subject)),
        (None, Some(subject)) => Some(("console", subject)),
        (None, None) => None,
    };
    if let Some((kind, subject)) = mint_subject {
        if subject.trim().is_empty() {
            return Err(crate::exit::usage(
                "the principal subject must not be blank",
            ));
        }
        if gate_mode
            || cmd.list
            || cmd.resolve.is_some()
            || route_write
            || cmd.list_routes
            || cmd.reject
            || cmd.note.is_some()
        {
            return Err(crate::exit::usage(
                "principal bootstrap cannot be combined with gate, route, pending-list, \
                 resolution, reject, or note flags; run it as a separate invocation",
            ));
        }
        if opts.dry_run {
            let endpoint = if kind == "operator" {
                "approvals/principals/operator"
            } else {
                "console/login-codes"
            };
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "POST {}/{endpoint} subject={subject:?} (one-time credential is returned only by a real run)",
                    opts.api_url
                )],
            }));
        }
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        return if kind == "operator" {
            Ok(ApprovalsOutput::OperatorPrincipal {
                delivery: client.mint_operator_principal(subject).await?,
            })
        } else {
            Ok(ApprovalsOutput::ConsoleLoginCode {
                delivery: client.mint_console_login_code(subject).await?,
            })
        };
    }

    if route_write || cmd.list_routes {
        if gate_mode || cmd.list || cmd.resolve.is_some() {
            return Err(crate::exit::usage(
                "the route-binding flags (--route-resolution/--route-approvers/--routes-from/\
                 --list-routes/--clear-routes) address the agent's approval ROUTES; they \
                 cannot be combined with --gate/--clear (tool gates) or --list/--resolve \
                 (pending records). Run them as separate invocations",
            ));
        }
        if route_write && cmd.list_routes {
            return Err(crate::exit::usage(
                "--list-routes reads; drop it to write, or run it as a second invocation",
            ));
        }
        if cmd.clear_routes
            && (!cmd.route_resolution.is_empty()
                || !cmd.route_approvers.is_empty()
                || cmd.routes_from.is_some())
        {
            return Err(crate::exit::usage(
                "--clear-routes cannot be combined with --route-resolution/\
                 --route-approvers/--routes-from \
                 (clear removes every binding)",
            ));
        }

        // Parse and validate EVERYTHING before any network call, so a malformed
        // entry can never leave a half-written binding map behind.
        let bindings = if cmd.clear_routes {
            BTreeMap::new()
        } else {
            build_route_bindings(
                &cmd.route_resolution,
                &cmd.route_approvers,
                cmd.routes_from.as_ref(),
            )?
        };

        if opts.dry_run {
            let action = if route_write {
                format!(
                    "PATCH {}/agents/<id> approval_routes={} (a FULL REPLACEMENT of the map)",
                    opts.api_url,
                    // Deliberately not valid JSON on serialization failure. "{}" is the
                    // real clear payload, so a fallback that looked like "{}" would
                    // misreport a full revocation.
                    route_bindings_plan_json(&bindings)
                )
            } else {
                format!(
                    "GET {}/agents/<id> (show approval route bindings)",
                    opts.api_url
                )
            };
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "{action}  (would resolve agent {:?} first)",
                    opts.agent
                )],
            }));
        }

        let ui = crate::ui::ui();
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        let agent = client.find_agent(&opts.agent).await?;
        let agent = if route_write {
            // #2448: advisory pre-check against every active deployment's
            // declared routes. Fail-open; the platform API remains the gate.
            let (declaring, unreadable) = active_declared_routes(&client, &agent.id).await;
            for reason in &unreadable {
                ui.warn(&format!(
                    "could not check this route write against every active deployment \
                     ({reason}); sending it anyway. The platform API still refuses a write \
                     that removes a declared route"
                ));
            }
            if let Some(err) = route_write_refusal(&agent.name, &bindings, &declaring) {
                return Err(err);
            }
            let cl = ui.checklist();
            let step = cl.step(&format!("updating approval routes for {}", agent.name));
            match client.set_approval_routes(&agent.id, &bindings).await {
                Ok(updated) => {
                    step.done("updated");
                    updated
                }
                Err(err) => {
                    step.fail("failed");
                    return Err(err);
                }
            }
        } else {
            agent
        };
        return Ok(ApprovalsOutput::Routes {
            agent: agent.name,
            routes: agent.approval_routes.unwrap_or_default(),
        });
    }

    // --resolve <id>: resolve one live approval record as the authenticated
    // principal from CURIE_APPROVAL_PRINCIPAL_TOKEN (#1531, ADR-0106). It is
    // id-scoped, not gate config, so it is mutually exclusive with --gate/--clear/
    // --list. Approve by default; --reject rejects.
    if let Some(approval_id) = cmd.resolve {
        if gate_mode || cmd.list {
            return Err(crate::exit::usage(
                "--resolve cannot be combined with --gate/--clear/--list",
            ));
        }
        let decision = if cmd.reject { "rejected" } else { "approved" };
        if opts.dry_run {
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "POST {}/approvals/{approval_id}/resolve decision={decision} \
                     (authenticated principal read from CURIE_APPROVAL_PRINCIPAL_TOKEN at execution)",
                    opts.api_url
                )],
            }));
        }
        let principal_token = std::env::var("CURIE_APPROVAL_PRINCIPAL_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                crate::exit::usage(
                    "--resolve requires CURIE_APPROVAL_PRINCIPAL_TOKEN. Mint a reusable \
                     operator credential with `curie <local|cluster> approvals <AGENT> \
                     --mint-operator-principal <SUBJECT>`, export the one-time result, and retry",
                )
            })?;
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        let record = client
            .resolve_approval(
                &approval_id,
                decision,
                &principal_token,
                cmd.note.as_deref(),
            )
            .await?;
        return Ok(ApprovalsOutput::Resolved { record });
    }

    // --list: the agent's pending approval records (#506).
    if cmd.list {
        if gate_mode {
            return Err(crate::exit::usage(
                "--list cannot be combined with --gate/--clear",
            ));
        }
        if opts.dry_run {
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "GET {}/approvals?status_filter=pending&agent_id=<id>&limit={}  (would resolve agent {:?} first)",
                    opts.api_url,
                    crate::api::ApiClient::APPROVALS_LIST_LIMIT,
                    opts.agent
                )],
            }));
        }
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        let agent = client.find_agent(&opts.agent).await?;
        let records = client.list_pending_approvals(&agent.id).await?;
        let truncated = records.len() >= crate::api::ApiClient::APPROVALS_LIST_LIMIT;
        return Ok(ApprovalsOutput::Pending {
            agent: agent.name,
            records,
            routes: agent.approval_routes.unwrap_or_default(),
            truncated,
        });
    }

    if clear && !gate.is_empty() {
        return Err(crate::exit::usage(
            "--clear cannot be combined with --gate (clear removes all gates)",
        ));
    }
    let setting = clear || !gate.is_empty();
    if opts.dry_run {
        let action = if setting {
            format!(
                "PATCH {}/agents/<id> approval_required_tools={:?}",
                opts.api_url, gate
            )
        } else {
            format!("GET {}/agents/<id> (show current gates)", opts.api_url)
        };
        return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "{action}  (would resolve agent {:?} first)",
                opts.agent
            )],
        }));
    }
    let ui = crate::ui::ui();
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let (gates, unreadable) = if setting {
        let cl = ui.checklist();
        let step = cl.step(&format!("updating approval gates for {}", agent.name));
        match client.set_approval_tools(&agent.id, &gate).await {
            Ok(updated) => {
                step.done("updated");
                (updated.approval_required_tools.unwrap_or_default(), None)
            }
            Err(err) => {
                step.fail("failed");
                return Err(err);
            }
        }
    } else {
        // Report what the runner actually arms (#546): the UNION of the platform's
        // mutable `approval_required_tools` field (delivered as
        // CURIE_APPROVAL_REQUIRED_TOOLS) AND the in-force deployed bundle
        // manifest's `approvalPolicy` gates. Reading only the API field reported an
        // empty set while a manifest gate was armed and blocking.
        //
        // When that manifest half cannot be read, the union is only the field half
        // and the report says so (#607) rather than passing a partial answer off as
        // the effective set.
        let mut gated = agent.approval_required_tools.clone().unwrap_or_default();
        match deployed_manifest_gate_names(&client, &agent.id).await? {
            ManifestGates::Readable(names) => {
                for name in names {
                    if !gated.contains(&name) {
                        gated.push(name);
                    }
                }
                (gated, None)
            }
            ManifestGates::Unreadable(reason) => (gated, Some(reason)),
        }
    };
    Ok(ApprovalsOutput::Gates {
        agent: agent.name,
        gated_tools: gates,
        manifest_unreadable: unreadable,
    })
}
