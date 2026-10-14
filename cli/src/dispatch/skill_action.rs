//! Dispatch for `curie skill`: the local runner tier.

use super::*;

/// Run one parsed `skill` subcommand.
pub(super) async fn run(action: SkillAction) -> Result<()> {
    match action {
        SkillAction::Up {
            plugin_dir,
            image,
            port,
            name,
            fake_model,
            network,
            otel_endpoint,
            budget,
            model,
            local_model,
            pull_model,
            secret,
            env_file,
            replace,
        } => {
            let image = skill_runner_image(&plugin_dir, image.as_deref())?;
            commands::start(StartOpts {
                plugin_dir,
                image,
                port,
                name,
                fake_model,
                network,
                otel_endpoint,
                budget,
                model,
                local_model,
                pull_model,
                secret,
                env_file,
                replace,
            })
            .await
        }
        SkillAction::Check {
            plugin_dir,
            image,
            timeout,
        } => {
            let image = skill_runner_image(&plugin_dir, image.as_deref())?;
            commands::check(plugin_dir, image, timeout).await
        }
        SkillAction::Approvals {
            plugin_dir,
            gate,
            clear,
            list,
            resolve,
            route_resolution,
            route_approvers,
            routes_from,
            list_routes,
            clear_routes,
            recovery:
                ApprovalRecoveryArgs {
                    report_identity,
                    recover,
                    ..
                },
            ..
        } => {
            // Answered, not absent (ADR-0041, ADR-0077): the durable
            // list/resolve the local+cluster tiers have does not exist here,
            // so the verb reports why and exits 4 rather than erroring like a
            // typo. Gate config (view/set/clear) is unchanged.
            //
            // The route flags decline with their OWN reason (#1052): a pending
            // record is absent because this tier keeps no durable store, but a
            // route binding is absent because it is per-agent platform config
            // and this tier has no agent. Same wrong answer, different fix.
            let routes_asked = !route_resolution.is_empty()
                || !route_approvers.is_empty()
                || routes_from.is_some()
                || list_routes
                || clear_routes;
            //
            // The recovery verbs decline FIRST and with their own reason
            // (#2753): they address the durable store's administrative
            // surface, and answering them with the route or list reason
            // would point the operator at the wrong absent thing.
            if report_identity || recover.is_some() {
                Err(commands::skill_approvals_recovery_unavailable())
            } else if routes_asked {
                Err(commands::skill_approval_routes_unavailable())
            } else if list || resolve.is_some() {
                Err(commands::skill_approvals_list_unavailable())
            } else {
                emit(commands::skill_approvals(plugin_dir, gate, clear).await?)
            }
        }
        // Answered, not absent: the concept does not exist at this tier, so
        // the verb reports why and exits 4 (issue #459, ADR-0041).
        SkillAction::Versions => Err(commands::skill_versions_unavailable()),
        SkillAction::Memory => Err(commands::skill_memory_unavailable()),
        SkillAction::WorkItems { .. } => Err(commands::skill_work_items_unavailable()),
        SkillAction::Schedules { .. } => Err(commands::skill_schedules_unavailable()),
        SkillAction::Hook { action } => match action {
            SkillHookAction::Schedule => Err(commands::skill_hook_record_unavailable("schedule")),
            SkillHookAction::Record => Err(commands::skill_hook_record_unavailable("record")),
            SkillHookAction::Fire {
                name,
                plugin_dir,
                url,
            } => emit(commands::skill_hook_fire(&plugin_dir, &name, url).await?),
        },
        SkillAction::Observability { .. } => Err(commands::skill_observability_unavailable()),
        SkillAction::Down { name } => commands::stop(name, std::path::Path::new(".")).await,
        SkillAction::Status { url } => commands::status(url).await,
        SkillAction::Message {
            text,
            user,
            event_type,
            url,
            r#continue,
        } => emit(commands::send(&text, &user, event_type.into(), url, r#continue).await?),
        SkillAction::Eval {
            cases,
            case_id,
            url,
            model,
            secret,
            image,
            sampling,
        } => {
            let image = if model.is_empty() {
                artifacts::resolve_image(
                    image.as_deref(),
                    artifacts::Channel::current(),
                    artifacts::version(),
                )
            } else {
                let saved = curie::state::load(std::path::Path::new("."))?;
                let bundle_dir = saved
                    .as_ref()
                    .and_then(|state| state.bundle_snapshot_dir.as_deref())
                    .or_else(|| saved.as_ref().map(|state| state.plugin_dir.as_str()))
                    .unwrap_or(".");
                skill_runner_image(std::path::Path::new(bundle_dir), image.as_deref())?
            };
            commands::eval(
                cases,
                case_id,
                url,
                model,
                secret,
                image,
                sampling.config()?,
            )
            .await
        }
        SkillAction::EvalInit { out, force } => {
            curie::eval_init::run(curie::eval_init::EvalInitOpts { out, force })
        }
    }
}
