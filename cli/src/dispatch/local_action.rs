//! Dispatch for `curie local`: the compose stack tier.

use super::*;

/// Run one parsed `local` subcommand.
pub(super) async fn run(action: LocalAction) -> Result<()> {
    match action {
        LocalAction::Up {
            project,
            files,
            dry_run,
            minimal,
            model,
            local_model,
            pull_model,
            slack,
            env_file,
            build,
        } => {
            let mut opts = bind_local_opts(project, files, dry_run, build).await?;
            opts.minimal = minimal;
            opts.local_model = local_model;
            opts.pull_model = pull_model;
            opts.slack = slack;
            opts.model_mode = local::model_mode_from_env();
            opts.env_file = env_file;
            emit(local::up(opts, model).await?)
        }
        LocalAction::Rebuild {
            service,
            project,
            files,
            dry_run,
            minimal,
            model,
            local_model,
            slack,
            env_file,
        } => {
            let mut opts = bind_local_opts(project, files, dry_run, false).await?;
            opts.minimal = minimal;
            opts.local_model = local_model;
            opts.slack = slack;
            opts.model_mode = local::model_mode_from_env();
            opts.env_file = env_file;
            emit(
                local::rebuild(local::LocalRebuildOpts {
                    common: opts,
                    service,
                    model,
                })
                .await?,
            )
        }
        LocalAction::Down {
            project,
            files,
            wipe,
            yes,
            dry_run,
        } => {
            let opts = bind_local_opts(project, files, dry_run, false).await?;
            emit(
                local::down(LocalDownOpts {
                    common: opts,
                    wipe,
                    yes,
                })
                .await?,
            )
        }
        LocalAction::Status {
            project,
            files,
            dry_run,
        } => {
            let opts = bind_local_opts(project, files, dry_run, false).await?;
            emit(local::status(opts).await?)
        }
        LocalAction::Comms {
            slack,
            disconnect,
            minimal,
            model,
            app_token,
            bot_token,
            project,
            files,
            dry_run,
        } => {
            comms::require_provider(slack)?;
            let mut model_opts = bind_local_opts(project, files, dry_run, false).await?;
            model_opts.minimal = minimal;
            model_opts.slack = true;
            model_opts.model_mode = local::model_mode_from_env();
            // #749: fall back to Slack tokens persisted via `curie secrets
            // set` when neither a flag nor an env var supplied one, so
            // `--slack` needs no per-session re-export. Precedence: flag/env
            // > saved vault.
            let app_token =
                comms::resolve_local_slack_token("SLACK_APP_TOKEN", &app_token, disconnect)?;
            let bot_token =
                comms::resolve_local_slack_token("SLACK_BOT_TOKEN", &bot_token, disconnect)?;
            let model_credentials = local::apply_credential_plan(&mut model_opts, crate::ui::ui())?;
            // #1925: `comms connect` recreates the worker and dispatcher --
            // and, via `depends_on`, the api and migrate behind them. Derive
            // the running stack's tag here, alongside the credential plan
            // this same throwaway `LocalOpts` already exists to resolve.
            local::resolve_stack_image_env(&mut model_opts).await;
            // #3557: both directions recreate the api, which must come back
            // on the credentials the running stack uses: the stored ones,
            // or none for a stack that predates the store. Only `local up`
            // adopts CURIE_API_KEY or generates.
            let stack_secret_env =
                curie::local_stack_keys::running_stack_secret_env(model_opts.project())?;
            emit(
                comms::local_comms(LocalCommsOpts {
                    project: model_opts.project().to_string(),
                    files: model_opts.files().to_vec(),
                    stub_port: model_opts.resources.stub_port,
                    dry_run,
                    app_token,
                    bot_token,
                    disconnect,
                    model_mode: model_opts.model_mode,
                    model_credentials,
                    model,
                    minimal,
                    stack_image_env: model_opts.stack_image_env,
                    stack_secret_env,
                })
                .await?,
            )
        }
        LocalAction::Message {
            text,
            channel,
            agent,
            thread,
            r#continue,
            valkey_password,
            api_url,
            api_key,
            user,
            stream,
            timeout_secs,
            dry_run,
        } => {
            let state = if r#continue {
                match load_turn(&std::env::current_dir()?)? {
                    Some(state) => Some(state),
                    None => anyhow::bail!(
                        "no previous turn recorded in .curie/last-turn.json; run a message without --continue first"
                    ),
                }
            } else {
                None
            };
            let resolved = apply_continue(
                TurnVerb::Local,
                CliTurnArgs {
                    channel,
                    thread,
                    namespace: None,
                    release: None,
                    chart: None,
                    listen_host: None,
                    timeout_secs,
                    api_url,
                    api_key,
                    agent,
                },
                state,
                // Empty is unset (#540), so the recorded-env bail below still
                // fires when $CURIE_API_KEY is exported blank.
                std::env::var("CURIE_API_KEY")
                    .ok()
                    .filter(|v| !v.is_empty()),
            )?;
            message::message(MessageOpts {
                text,
                channel: resolved.channel,
                agent: resolved.agent,
                thread: resolved.thread,
                namespace: "curie".into(),
                release: "curie".into(),
                chart: "charts/curie".into(),
                listen_host: None,
                listen_port: message::DEFAULT_LISTEN_PORT,
                valkey_local_port: message::DEFAULT_VALKEY_LOCAL_PORT,
                valkey_password,
                api_local_port: message::DEFAULT_API_LOCAL_PORT,
                api_key: resolved.api_key,
                user,
                stream,
                timeout_secs: resolved.timeout_secs,
                dry_run,
                local: true,
                api_url: resolved.api_url,
            })
            .await
        }
        LocalAction::Eval {
            cases,
            case_id,
            channel,
            agent,
            valkey_password,
            api_url,
            api_key,
            user,
            stream,
            timeout_secs,
            model,
            concurrency,
            sampling,
            dry_run,
        } => {
            message::eval(message::EvalOpts {
                cases,
                case_ids: case_id,
                channel,
                agent,
                namespace: "curie".into(),
                release: "curie".into(),
                listen_host: None,
                listen_port: message::DEFAULT_LISTEN_PORT,
                valkey_local_port: message::DEFAULT_VALKEY_LOCAL_PORT,
                valkey_password,
                api_local_port: message::DEFAULT_API_LOCAL_PORT,
                api_key,
                user,
                stream,
                timeout_secs,
                dry_run,
                local: true,
                api_url,
                models: model,
                concurrency,
                sampling: sampling.config()?,
            })
            .await
        }
        LocalAction::Deploy {
            plugin_dir,
            agent,
            target,
            identity,
            api_url,
            api_key,
            slack_channel,
            repo,
            workspace,
            no_workspace,
            env,
            label,
            secret,
        } => {
            let local_api_url = api_url.clone();
            let result = commands::deploy(DeployOpts {
                // The local tier has no release to read delivery off, so it
                // is not assessed and says nothing -- distinct from an
                // assessed-but-undetermined `Some(Unknown)` (#2496).
                delivery: None,
                plugin_dir,
                agent,
                target,
                identity,
                api_url,
                api_key,
                slack_channel,
                repo,
                workspace: commands::WorkspaceIntent::from_flags(workspace, no_workspace),
                tier: commands::DeployTier::Local,
                env,
                label,
                secret,
                // `local deploy` offers `--secret`, so enforce the
                // declared-secrets policy gate (#464).
                secret_binding_supported: true,
                connect_hint: "the platform API is unreachable.".to_string(),
            })
            .await;
            emit(local::with_deploy_unreachable_hint(result, &local_api_url).await?)
        }
        LocalAction::Console {
            action:
                LocalConsoleAction::Login {
                    subject,
                    api_url,
                    dry_run,
                },
        } => {
            let api_key = if dry_run {
                String::new()
            } else {
                api::resolve_local_api_key(&api_url, message::DEFAULT_API_KEY)
            };
            emit(commands::console_login(&api_url, &api_key, &subject, dry_run).await?)
        }
        LocalAction::Versions { target } => emit(commands::versions(target.into()).await?),
        LocalAction::Hooks { action } => match action {
            LocalHooksAction::Show { target } => emit(commands::hooks_show(target.into()).await?),
            LocalHooksAction::Configure { target, file } => {
                emit(commands::hooks_configure(target.into(), &file).await?)
            }
            LocalHooksAction::Secret { target } => {
                emit(commands::hooks_secret(target.into()).await?)
            }
        },
        LocalAction::WorkItems {
            id,
            agent,
            api_url,
            api_key,
            dry_run,
        } => emit({
            let id = commands::validated_work_item_id(id)?;
            commands::work_items(commands::WorkItemsOpts {
                api_url,
                api_key,
                id,
                agent,
                dry_run,
                tier: "local",
            })
            .await?
        }),
        LocalAction::Schedules {
            agent,
            pause,
            resume,
            api_url,
            api_key,
            dry_run,
        } => emit(
            commands::schedules(commands::SchedulesOpts {
                api_url,
                api_key,
                agent,
                pause,
                resume,
                dry_run,
            })
            .await?,
        ),
        LocalAction::Hook { action } => {
            let LocalHookAction::Fire {
                agent,
                name,
                wait_secs,
                api_url,
                api_key,
                dry_run,
            } = action;
            emit(
                commands::hook_fire(commands::HookFireOpts {
                    api_url,
                    api_key,
                    agent,
                    name,
                    dry_run,
                    wait_secs,
                })
                .await?,
            )
        }
        LocalAction::Memory {
            target,
            add,
            delete,
            channel,
            guidance,
        } => match (guidance.action(), add, delete) {
            (Some(action), _, _) => emit_memory_guidance(target.into(), action).await,
            (None, None, Some(id)) => {
                emit(commands::memory_delete(target.into(), id, channel).await?)
            }
            (None, None, None) => emit(commands::memory(target.into(), channel).await?),
            (None, Some(content), _) => {
                emit(commands::memory_add(target.into(), content, "local").await?)
            }
        },
        LocalAction::Approvals {
            target,
            gate,
            clear,
            list,
            resolve,
            reject,
            note,
            mint_operator_principal,
            mint_console_login_code,
            route_resolution,
            route_approvers,
            routes_from,
            list_routes,
            clear_routes,
            recovery:
                ApprovalRecoveryArgs {
                    report_identity,
                    recover,
                    reason,
                    recovery_key,
                },
        } => emit(
            commands::approvals(
                target.into(),
                gate,
                clear,
                commands::ApprovalCmd {
                    list,
                    resolve,
                    reject,
                    note,
                    mint_operator_principal,
                    mint_console_login_code,
                    route_resolution,
                    route_approvers,
                    routes_from,
                    list_routes,
                    clear_routes,
                    report_identity,
                    recover,
                    reason,
                    recovery_key,
                },
            )
            .await?,
        ),
        LocalAction::Observability { query, open } => match query {
            None => emit(commands::observability(open).await?),
            Some(_) if open => Err(curie::exit::usage(
                "--open cannot be combined with an observability query",
            )),
            Some(query) => emit_boxed(run_local_observability_query(query).await?),
        },
        LocalAction::Overrides {
            agent,
            model,
            clear_model,
            thinking,
            clear_thinking,
            execution_deadline,
            clear_execution_deadline,
            runner_resources,
            clear_runner_resources,
            memory_writes,
            api_url,
            api_key,
            dry_run,
        } => emit(
            commands::overrides_with_memory_writes(
                AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                },
                commands::OverrideChange::resolve("model", model, clear_model)?,
                commands::OverrideChange::resolve("thinking", thinking, clear_thinking)?,
                commands::OverrideChange::resolve_execution_deadline(
                    execution_deadline,
                    clear_execution_deadline,
                )?,
                commands::OverrideChange::resolve_runner_resources(
                    runner_resources,
                    clear_runner_resources,
                )?,
                commands::memory_writes_flag(memory_writes.as_deref()),
            )
            .await?,
        ),
        LocalAction::PublicationPolicy {
            agent,
            policy,
            draft,
            no_draft,
            branch_prefix,
            clear_branch_prefix,
            api_url,
            api_key,
            dry_run,
        } => emit(
            commands::publication_policy(
                AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                },
                policy,
                draft,
                no_draft,
                branch_prefix,
                clear_branch_prefix,
            )
            .await?,
        ),
        LocalAction::Surfaces {
            target,
            add,
            endpoint,
            adapter,
            remove,
        } => emit(
            commands::channel_bindings(
                target.into(),
                commands::ChannelChange::resolve(add, remove, endpoint, adapter)?,
            )
            .await?,
        ),
        LocalAction::Callers {
            target,
            surface,
            adapter,
            set,
            clear,
        } => emit(
            commands::channel_callers(
                target.into(),
                &surface,
                adapter,
                commands::CallersChange::resolve(set, clear)?,
            )
            .await?,
        ),
        LocalAction::Budget {
            agent,
            limit,
            output_tokens,
            api_url,
            api_key,
            dry_run,
        } => emit(
            commands::budget(
                AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                },
                limit,
                output_tokens,
            )
            .await?,
        ),
        LocalAction::Kill {
            agent,
            api_url,
            api_key,
            yes,
            dry_run,
        } => emit(
            commands::kill(
                AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                },
                yes,
            )
            .await?,
        ),
        LocalAction::Resume {
            agent,
            api_url,
            api_key,
            dry_run,
        } => emit(
            commands::resume(AgentActionOpts {
                api_url,
                api_key,
                agent,
                dry_run,
            })
            .await?,
        ),
        LocalAction::ResetThread {
            agent,
            thread_key,
            api_url,
            api_key,
            yes,
            dry_run,
        } => emit(
            commands::reset_thread(
                AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                },
                thread_key,
                yes,
            )
            .await?,
        ),
        LocalAction::Delete {
            agent,
            api_url,
            api_key,
            yes,
            dry_run,
        } => emit(
            commands::delete(
                AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                },
                yes,
            )
            .await?,
        ),
    }
}
