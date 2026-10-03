//! `<tier> channels` and `<tier> callers`: channel bindings and caller allowlists.

use super::*;

/// What one `curie <tier> surfaces <agent>` invocation does to the agent's
/// binding set: nothing (list), add one pair, or remove one pair.
///
/// Exactly one mutation per invocation, never a batch: the API has no batch
/// endpoint, so a half-applied run would leave the operator guessing what took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelChange {
    /// No flags: read the agent's bindings and write nothing.
    List,
    Add {
        kind: String,
        address: String,
        endpoint: Option<String>,
        adapter: Option<String>,
    },
    Remove {
        kind: String,
        address: String,
        /// Which identity's binding to drop (ADR-0168 decision 3). Omitted
        /// keeps today's selection: the default Slack identity, or the single
        /// row on a non-Slack pair.
        adapter: Option<String>,
    },
}

impl ChannelChange {
    /// Resolve the `--add` / `--remove` flag pair into one intent.
    ///
    /// clap already refuses the two together (`conflicts_with`), so this parses
    /// whichever arrived. Usage errors are raised here, before any I/O, so a
    /// mistyped pair costs no network round trip.
    ///
    /// Args:
    ///   add: the `--add KIND=ADDRESS` value, if passed.
    ///   remove: the `--remove KIND=ADDRESS` value, if passed.
    ///   adapter: `--adapter`, valid alongside either -- a Slack identity name
    ///     on `--add` (with no `--endpoint`) or the identity to drop on
    ///     `--remove` (ADR-0168 decision 3). Meaningless with neither, since
    ///     there is no write and no removal selector for it to name.
    ///
    /// Returns:
    ///   The intent, or a usage error when the pair is malformed.
    pub fn resolve(
        add: Option<String>,
        remove: Option<String>,
        endpoint: Option<String>,
        adapter: Option<String>,
    ) -> Result<Self> {
        match (add, remove) {
            (Some(spec), _) => {
                let (kind, address) = parse_channel_pair(&spec)?;
                if kind == "slack" && endpoint.is_some() {
                    return Err(crate::exit::usage(
                        "--endpoint on a Slack binding: a Slack route names its identity with \
                         --adapter and takes no endpoint (ADR-0168 decision 3)"
                            .to_string(),
                    ));
                }
                // A non-Slack reply route needs BOTH endpoint and adapter:
                // clap only enforces `--endpoint` requires `--adapter`, not
                // the other way, so `--adapter` alone on a non-Slack kind
                // reaches here and must be refused before any I/O -- the API
                // would refuse it too, but only after a round trip.
                if kind != "slack" && adapter.is_some() && endpoint.is_none() {
                    return Err(crate::exit::usage(format!(
                        "--adapter on a non-Slack kind ({kind}) also needs --endpoint; \
                         a Slack identity needs no endpoint, but a {kind} reply route does"
                    )));
                }
                Ok(ChannelChange::Add {
                    kind,
                    address,
                    endpoint,
                    adapter,
                })
            }
            (None, Some(spec)) => {
                let (kind, address) = parse_channel_pair(&spec)?;
                Ok(ChannelChange::Remove {
                    kind,
                    address,
                    adapter,
                })
            }
            (None, None) => {
                if adapter.is_some() {
                    return Err(crate::exit::usage(
                        "--adapter needs --add or --remove".to_string(),
                    ));
                }
                Ok(ChannelChange::List)
            }
        }
    }
}

/// Split `KIND=ADDRESS` on the FIRST `=` only. A kind may not contain one; an
/// address may (an email- or URL-shaped address for a non-Slack ingress is the
/// whole reason bindings went channel-neutral), so everything after the first
/// separator is the address, `=` included.
pub(super) fn parse_channel_pair(spec: &str) -> Result<(String, String)> {
    let malformed = || {
        crate::exit::usage(format!(
            "--add/--remove takes KIND=ADDRESS (e.g. slack=C0EXAMPLE1), got {spec:?}. \
             The kind is never inferred: a binding names the ingress explicitly"
        ))
    };
    let (kind, address) = spec.split_once('=').ok_or_else(malformed)?;
    if kind.is_empty() || address.is_empty() {
        return Err(malformed());
    }
    Ok((kind.to_string(), address.to_string()))
}

/// Output of `<tier> surfaces <agent>`: the dry-run plan, or the agent's
/// binding set as the API stored it. Owns its data so it outlives the
/// `ApiClient`.
///
/// `channels` carries the PAIRS, not bare addresses, so an agent consumer reads
/// the kind without guessing it. `changed` distinguishes a list from a
/// mutation, so a consumer can tell "this is what it is" from "this is what it
/// now is" without diffing.
#[derive(Debug)]
pub enum ChannelsOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        channels: Vec<crate::api::ChannelBinding>,
        changed: bool,
    },
}

pub(super) const CHANNEL_BINDING_NEVER_RESOLVES_WARNING: &str =
    "mentions match on the channel ID, not the name, so this binding never resolves";

#[derive(Serialize)]
pub(super) struct ChannelBindingPresentation<'a> {
    kind: &'a str,
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapter: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<&'static str>,
}

pub(super) fn channel_binding_never_resolves(kind: &str, address: &str) -> bool {
    kind == "slack" && address.trim_start().starts_with('#')
}

/// The binding's identity, when it is worth printing: present and not the
/// default Slack identity every pre-ADR install already reads as (ADR-0168
/// decision 3). Naming that one on every row would make single-identity
/// output noisier for no new information.
pub(super) fn channel_binding_named_identity(binding: &crate::api::ChannelBinding) -> Option<&str> {
    binding.named_adapter()
}

pub(super) fn channel_binding_presentation(
    binding: &crate::api::ChannelBinding,
) -> ChannelBindingPresentation<'_> {
    ChannelBindingPresentation {
        kind: &binding.kind,
        address: &binding.address,
        adapter: channel_binding_named_identity(binding),
        warning: channel_binding_never_resolves(&binding.kind, &binding.address)
            .then_some(CHANNEL_BINDING_NEVER_RESOLVES_WARNING),
    }
}

impl crate::ui::CliOutput for ChannelsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ChannelsOutput::DryRun(plan) => plan.to_json(),
            ChannelsOutput::Done {
                agent,
                channels,
                changed,
            } => {
                let surfaces = channels
                    .iter()
                    .map(channel_binding_presentation)
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "agent": agent,
                    // Serialize each CLI presentation row wholesale. The raw
                    // API mirror remains unchanged while this output adds its
                    // optional, derived warning without hand-projecting fields.
                    "surfaces": serde_json::to_value(surfaces)
                        .unwrap_or(serde_json::Value::Null),
                    "changed": changed,
                })
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ChannelsOutput::DryRun(plan) => plan.render(ui),
            ChannelsOutput::Done {
                agent,
                channels,
                changed,
            } => {
                let verb = if *changed { " now" } else { "" };
                let bound = if channels.is_empty() {
                    "none".to_string()
                } else {
                    channels
                        .iter()
                        .map(|c| match channel_binding_named_identity(c) {
                            Some(identity) => format!("{}:{} ({identity})", c.kind, c.address),
                            None => format!("{}:{}", c.kind, c.address),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                ui.payload(&format!("surfaces for {agent}{verb}: {bound}"));
                for channel in channels {
                    if channel_binding_never_resolves(&channel.kind, &channel.address) {
                        ui.warn(&format!(
                            "{}:{}: {CHANNEL_BINDING_NEVER_RESOLVES_WARNING}",
                            channel.kind, channel.address
                        ));
                    }
                }
            }
        }
    }
}

/// `curie <tier> surfaces <agent> [--add KIND=ADDRESS | --remove KIND=ADDRESS]`.
///
/// With no flags this LISTS: one `GET`-resolved agent, no write. With one flag
/// it adds or removes exactly that binding, then reports the set as the API
/// holds it, so the operator sees what took rather than what was intended.
///
/// Args:
///   opts: api url/key, the agent name or id, and the dry-run flag.
///   change: the intent already parsed from the flag pair.
///
/// Returns:
///   The agent's bindings, or the dry-run plan.
///
/// Named `channel_bindings` rather than `channels` after the verb: the
/// emit-parity gate's reachability walk follows a `to_json` body's bare
/// identifiers to same-named free functions (`cli/tests/support/emit_parity.rs`),
/// and `channels` is now a field identifier several unrelated bodies mention,
/// so a free fn by that name gets pulled into their keysets and reports
/// omissions on other verbs as stale.
pub async fn channel_bindings(
    opts: AgentActionOpts,
    change: ChannelChange,
) -> Result<ChannelsOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        let plan = match &change {
            ChannelChange::List => format!(
                "GET {}/agents  (read-only: would resolve agent {:?} and print its surfaces)",
                opts.api_url, opts.agent
            ),
            ChannelChange::Add {
                kind,
                address,
                endpoint,
                adapter,
            } => {
                // Three reply-route shapes (ADR-0168 decision 3): a
                // non-Slack route (endpoint + adapter together), a named
                // Slack identity (adapter alone), or the implicit default
                // nothing names.
                let reply_route = if kind != "slack" && endpoint.is_some() && adapter.is_some() {
                    "configured".to_string()
                } else if let Some(adapter) = adapter {
                    format!("identity {adapter}")
                } else {
                    "implicit".to_string()
                };
                format!(
                        "POST {}/agents/<id>/channels  {{\"kind\":\"{kind}\",\"address\":\"{address}\"}}  \
                         (would resolve agent {:?} first; reply route: {reply_route})",
                        opts.api_url, opts.agent,
                    )
            }
            ChannelChange::Remove {
                kind,
                address,
                adapter,
            } => {
                let selector = match adapter {
                    Some(adapter) => format!("kind={kind}&address={address}&adapter={adapter}"),
                    None => format!("kind={kind}&address={address}"),
                };
                format!(
                    "DELETE {}/agents/<id>/channels?{selector}  \
                         (would resolve agent {:?} first)",
                    opts.api_url, opts.agent
                )
            }
        };
        return Ok(ChannelsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let (kind, address, adding) = match &change {
        ChannelChange::List => {
            // find_agent already carries the bindings, so there is nothing
            // further to fetch and nothing to write.
            return Ok(ChannelsOutput::Done {
                agent: agent.name,
                channels: agent.channels,
                changed: false,
            });
        }
        ChannelChange::Add { kind, address, .. } => (kind, address, true),
        ChannelChange::Remove { kind, address, .. } => (kind, address, false),
    };
    let cl = ui.checklist();
    let verb = if adding { "adding" } else { "removing" };
    let step = cl.step(&format!(
        "{verb} {kind}:{address} on {name}",
        name = agent.name
    ));
    let saved = if adding {
        let (endpoint, adapter) = match &change {
            ChannelChange::Add {
                endpoint, adapter, ..
            } => (endpoint.as_deref(), adapter.as_deref()),
            _ => (None, None),
        };
        client
            .add_agent_channel(&agent.id, kind, address, endpoint, adapter)
            .await
    } else {
        // The DELETE answers 204 with no body, so the remaining set comes from
        // a fresh read rather than from locally subtracting the pair -- the CLI
        // reports what the API holds, never what it assumes it holds.
        let adapter = match &change {
            ChannelChange::Remove { adapter, .. } => adapter.as_deref(),
            _ => None,
        };
        match client
            .remove_agent_channel(&agent.id, kind, address, adapter)
            .await
        {
            Ok(()) => client.get_agent(&agent.id).await,
            Err(err) => Err(err),
        }
    };
    let saved = match saved {
        Ok(saved) => {
            step.done(if adding { "added" } else { "removed" });
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(ChannelsOutput::Done {
        agent: saved.name,
        channels: saved.channels,
        changed: true,
    })
}

/// What one `curie <tier> callers <agent> --surface KIND=ADDRESS` invocation
/// does to that surface's caller list (ADR 0175): show it, replace it, or clear
/// it so everyone may talk to the bot again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallersChange {
    /// No `--set` and no `--clear`: read the list and write nothing.
    Show,
    /// `--set`: replace the list with exactly these ids.
    Set(Vec<String>),
    /// `--clear`: remove the list, so everyone may talk to the bot.
    Clear,
}

impl CallersChange {
    /// Resolve `--set` / `--clear` into one intent, before any I/O.
    ///
    /// clap already refuses the two together (`conflicts_with`). This mirrors
    /// only the API's kind-free rules (convention: validate at the API, mirror
    /// in the CLI): an entry must be non-empty with no whitespace. The id shape
    /// per kind (a Slack U/W/B id, one bare email address) stays the API's
    /// call, because only the API knows the selected binding's kind for sure.
    ///
    /// Args:
    ///   set: the `--set` ids, comma separated or repeated; empty when absent.
    ///   clear: whether `--clear` was passed.
    ///
    /// Returns:
    ///   The intent, or a usage error naming the malformed entry.
    pub fn resolve(set: Vec<String>, clear: bool) -> Result<Self> {
        if clear {
            return Ok(CallersChange::Clear);
        }
        if set.is_empty() {
            return Ok(CallersChange::Show);
        }
        for id in &set {
            if id.is_empty() || id.chars().any(char::is_whitespace) {
                return Err(crate::exit::usage(format!(
                    "--set takes exact caller ids, comma separated, with no spaces \
                     (e.g. --set U0123ABCD,U0456EFGH or --set person@example.com); got {id:?}"
                )));
            }
        }
        Ok(CallersChange::Set(set))
    }
}

/// Output of `<tier> callers <agent>`: the dry-run plan, or the surface as the
/// API holds it after this invocation. `allowed_callers` is `None` when the
/// surface carries no list (everyone may talk to the bot), which is emitted as
/// JSON `null` rather than omitted, so a consumer never mistakes "open" for "not
/// reported". `changed` distinguishes a show from a write.
#[derive(Debug)]
pub enum CallersOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        surface: crate::api::ChannelBinding,
        changed: bool,
    },
}

impl crate::ui::CliOutput for CallersOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            CallersOutput::DryRun(plan) => plan.to_json(),
            CallersOutput::Done {
                agent,
                surface,
                changed,
            } => serde_json::json!({
                "agent": agent,
                "kind": surface.kind,
                "address": surface.address,
                "adapter": surface.adapter,
                "allowed_callers": surface.allowed_callers,
                "changed": changed,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            CallersOutput::DryRun(plan) => plan.render(ui),
            CallersOutput::Done {
                agent,
                surface,
                changed,
            } => {
                let route = match channel_binding_named_identity(surface) {
                    Some(identity) => format!("{}:{} ({identity})", surface.kind, surface.address),
                    None => format!("{}:{}", surface.kind, surface.address),
                };
                let verb = if *changed { " now" } else { "" };
                let who = match &surface.allowed_callers {
                    None => "everyone (no caller list)".to_string(),
                    Some(ids) => ids.join(", "),
                };
                ui.payload(&format!("callers for {agent} on {route}{verb}: {who}"));
            }
        }
    }
}

/// The surface `(kind, address, adapter)` names among one agent's bindings.
///
/// Matched as the API's `_binding_for` matches: the pair always, and the
/// identity only when `adapter` names one, compared on the identity the read
/// side reports (a Slack binding with none reads back `default`).
pub(super) fn find_surface<'a>(
    channels: &'a [crate::api::ChannelBinding],
    kind: &str,
    address: &str,
    adapter: Option<&str>,
) -> Option<&'a crate::api::ChannelBinding> {
    channels.iter().find(|binding| {
        binding.kind == kind
            && binding.address == address
            && adapter.is_none_or(|wanted| binding.adapter.as_deref() == Some(wanted))
    })
}

/// `curie <tier> callers <agent> --surface KIND=ADDRESS [--set IDS | --clear]`.
///
/// With neither flag this SHOWS the surface's caller list, from the agent read
/// alone. With `--set` it replaces the list, and with `--clear` it removes it,
/// through `PUT /agents/{id}/channels/callers` (ADR 0175), and reports the
/// surface as the API stored it. The write leaves the binding's generation
/// alone, so an adapter's channel token keeps working.
///
/// Args:
///   opts: api url/key, the agent name or id, and the dry-run flag.
///   surface: the `--surface KIND=ADDRESS` value.
///   adapter: `--adapter`, the identity that selects one of several routes on
///     a pair (ADR-0168 decision 3); omitted selects as the API does.
///   change: the intent already parsed from `--set` / `--clear`.
///
/// Returns:
///   The surface and its caller list, or the dry-run plan.
pub async fn channel_callers(
    opts: AgentActionOpts,
    surface: &str,
    adapter: Option<String>,
    change: CallersChange,
) -> Result<CallersOutput> {
    let (kind, address) = parse_channel_pair(surface)?;
    if opts.dry_run {
        let selector = match &adapter {
            Some(adapter) => format!("kind={kind}&address={address}&adapter={adapter}"),
            None => format!("kind={kind}&address={address}"),
        };
        let plan = match &change {
            CallersChange::Show => format!(
                "GET {}/agents  (read-only: would resolve agent {:?} and print the caller list \
                 of {kind}:{address})",
                opts.api_url, opts.agent
            ),
            CallersChange::Set(ids) => format!(
                "PUT {}/agents/<id>/channels/callers?{selector}  {}  (would resolve agent {:?} first)",
                opts.api_url,
                serde_json::json!({ "allowed_callers": ids }),
                opts.agent
            ),
            CallersChange::Clear => format!(
                "PUT {}/agents/<id>/channels/callers?{selector}  {{\"allowed_callers\":null}}  \
                 (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            ),
        };
        return Ok(CallersOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let not_bound = || {
        crate::exit::CliError::usage(format!(
            "agent {} has no {kind}:{address} surface{}",
            agent.name,
            adapter
                .as_deref()
                .map(|a| format!(" as {a:?}"))
                .unwrap_or_default()
        ))
        .with_fix(format!(
            "List the agent's surfaces with `curie <tier> surfaces {}` and pass one of them \
             as --surface KIND=ADDRESS.",
            agent.name
        ))
    };
    let callers = match &change {
        CallersChange::Show => {
            let found = find_surface(&agent.channels, &kind, &address, adapter.as_deref())
                .cloned()
                .ok_or_else(not_bound)?;
            return Ok(CallersOutput::Done {
                agent: agent.name.clone(),
                surface: found,
                changed: false,
            });
        }
        CallersChange::Set(ids) => Some(ids.as_slice()),
        CallersChange::Clear => None,
    };
    let ui = crate::ui::ui();
    let cl = ui.checklist();
    let verb = if callers.is_some() {
        "setting"
    } else {
        "clearing"
    };
    let step = cl.step(&format!(
        "{verb} the caller list of {kind}:{address} on {}",
        agent.name
    ));
    let saved = match client
        .set_channel_callers(&agent.id, &kind, &address, adapter.as_deref(), callers)
        .await
    {
        Ok(saved) => {
            step.done(if callers.is_some() { "set" } else { "cleared" });
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    let stored = find_surface(&saved.channels, &kind, &address, adapter.as_deref())
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "the API accepted the caller list but its answer carries no {kind}:{address} surface"
            )
        })?;
    Ok(CallersOutput::Done {
        agent: saved.name,
        surface: stored,
        changed: true,
    })
}

#[cfg(test)]
mod channels_tests {
    use super::ChannelChange;

    #[test]
    fn channel_change_parses_kind_and_address_on_first_equals() {
        // KIND=ADDRESS splits on the FIRST `=` only. A kind may not contain
        // one; an address may -- an email-shaped or URL-shaped address for a
        // non-Slack ingress is the whole reason bindings went channel-neutral.
        // Splitting on the last `=`, or rejecting the second one, would make
        // those addresses unbindable through the CLI.
        let change =
            ChannelChange::resolve(Some("slack=C0EXAMPLE1".into()), None, None, None).unwrap();
        assert_eq!(
            change,
            ChannelChange::Add {
                kind: "slack".into(),
                address: "C0EXAMPLE1".into(),
                endpoint: None,
                adapter: None,
            }
        );

        let odd =
            ChannelChange::resolve(Some("email=ops+a=b@example.com".into()), None, None, None)
                .unwrap();
        assert_eq!(
            odd,
            ChannelChange::Add {
                kind: "email".into(),
                address: "ops+a=b@example.com".into(),
                endpoint: None,
                adapter: None,
            },
            "everything after the first `=` is the address, `=` included"
        );

        // The same rule on the remove side: one parser, both flags.
        let removed =
            ChannelChange::resolve(None, Some("slack=C0EXAMPLE2".into()), None, None).unwrap();
        assert_eq!(
            removed,
            ChannelChange::Remove {
                kind: "slack".into(),
                address: "C0EXAMPLE2".into(),
                adapter: None,
            }
        );

        // Neither flag is an inspect, not an error: `channels <agent>` lists.
        assert_eq!(
            ChannelChange::resolve(None, None, None, None).unwrap(),
            ChannelChange::List
        );
    }

    #[test]
    fn channel_change_resolve_carries_the_adapter_on_add_and_remove() {
        // ADR-0168 decision 3: --adapter names the identity on --add (a Slack
        // identity, with no --endpoint) and selects which identity's binding
        // --remove drops.
        let add = ChannelChange::resolve(
            Some("slack=C0EXAMPLE1".into()),
            None,
            None,
            Some("default".into()),
        )
        .unwrap();
        assert_eq!(
            add,
            ChannelChange::Add {
                kind: "slack".into(),
                address: "C0EXAMPLE1".into(),
                endpoint: None,
                adapter: Some("default".into()),
            }
        );

        let remove = ChannelChange::resolve(
            None,
            Some("slack=C0EXAMPLE1".into()),
            None,
            Some("default".into()),
        )
        .unwrap();
        assert_eq!(
            remove,
            ChannelChange::Remove {
                kind: "slack".into(),
                address: "C0EXAMPLE1".into(),
                adapter: Some("default".into()),
            }
        );
    }

    #[test]
    fn channel_change_rejects_a_bare_adapter_with_no_add_or_remove() {
        // `--adapter` with neither `--add` nor `--remove` names nothing: there
        // is no write for it to name an identity on, and no removal for it to
        // select. It refuses before any I/O rather than reading as the
        // read-only `List`, which would drop the flag without a word.
        let err = ChannelChange::resolve(None, None, None, Some("default".into())).unwrap_err();
        let (class, _fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(err.to_string().contains("--adapter"), "{err}");
        assert!(err.to_string().contains("--add"), "{err}");
        assert!(err.to_string().contains("--remove"), "{err}");

        // Plain `List` (no adapter at all) is unaffected.
        assert_eq!(
            ChannelChange::resolve(None, None, None, None).unwrap(),
            ChannelChange::List
        );
    }

    #[test]
    fn channel_change_rejects_a_non_slack_adapter_with_no_endpoint() {
        // ADR-0168 decision 3: a non-Slack ingress still needs BOTH endpoint
        // and adapter for its reply route. Clap's `--endpoint requires
        // --adapter` only covers one direction; `--adapter` alone on a
        // non-Slack kind must be refused here, before the round trip the API
        // would otherwise spend refusing it.
        let err = ChannelChange::resolve(
            Some("discord=111111111111111111".into()),
            None,
            None,
            Some("discord-main".into()),
        )
        .unwrap_err();
        let (class, _fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(err.to_string().contains("--endpoint"), "{err}");

        // The same pairing on a Slack kind is exactly the bare-identity case
        // and must still succeed.
        assert!(ChannelChange::resolve(
            Some("slack=C0EXAMPLE1".into()),
            None,
            None,
            Some("second-bot".into())
        )
        .is_ok());

        // Both endpoint and adapter together on a non-Slack kind is that
        // kind's reply route and must still succeed.
        assert!(ChannelChange::resolve(
            Some("discord=111111111111111111".into()),
            None,
            Some("https://discord-adapter.example.com/replies".into()),
            Some("discord-main".into()),
        )
        .is_ok());
    }

    // @spec ADR-0168 d3
    #[test]
    fn a_slack_binding_takes_no_endpoint() {
        let err = ChannelChange::resolve(
            Some("slack=C0EXAMPLE1".into()),
            None,
            Some("http://127.0.0.1:1".into()),
            Some("proof-offline".into()),
        )
        .unwrap_err();
        let (class, _fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(err.to_string().contains("--endpoint"), "{err}");
    }

    #[test]
    fn channel_change_rejects_a_bare_address_with_no_kind() {
        // `--add C0EXAMPLE1` is the mistake this catches. Defaulting the kind
        // to "slack" would be the silent-wrong-thing: the operator learns the
        // kind is optional, and the first non-Slack ingress binds to the wrong
        // one. The error must exit USAGE, before any network call.
        let err = ChannelChange::resolve(Some("C0EXAMPLE1".into()), None, None, None).unwrap_err();
        let (class, _fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(err.to_string().contains("KIND=ADDRESS"), "{err}");

        // An empty kind or an empty address is the same mistake wearing a
        // separator, and must not slip through as a half-empty pair.
        for bad in ["=C0EXAMPLE1", "slack=", "="] {
            assert!(
                ChannelChange::resolve(Some(bad.into()), None, None, None).is_err(),
                "{bad:?} must not resolve to a binding"
            );
        }
    }
}
