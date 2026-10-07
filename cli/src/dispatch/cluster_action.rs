//! Dispatch for `curie cluster`: the Kubernetes tier.

use super::*;

/// Run one parsed `cluster` subcommand.
pub(super) async fn run(action: ClusterAction, context: Option<String>) -> Result<()> {
    let target = curie::kube_context::pin_for_cluster_command(context.as_deref())?;
    if let Some(target) = &target {
        ui::ui().note(&format!(
            "Kubernetes context: {} (cluster {})",
            target.context, target.cluster
        ));
    }
    match action {
        ClusterAction::LintValues {
            files,
            namespace,
            release,
        } => emit(
            ops::lint_values(
                CommonOpts {
                    namespace,
                    release,
                    dry_run: false,
                },
                files,
            )
            .await?,
        ),
        ClusterAction::Up {
            namespace,
            release,
            chart,
            no_expose,
            adopt,
            fake_model,
            model,
            local_model,
            allow_egress_host,
            allow_web_egress,
            github_token,
            clear_github_token,
            set,
            dev,
            dry_run,
            forward_only,
            e2e_connector_identity,
        } => {
            let mut set = set;
            if forward_only {
                set.push("api.migrate.forwardOnly=true".to_string());
            }
            if e2e_connector_identity {
                set.push(E2E_CONNECTOR_IDENTITY_SET.to_string());
            }
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_artifact(resolved, dry_run, "chart").await?;
            // Only the shell credential is a change request. The saved one
            // is adopted during completion when the release records no
            // model credential of its own (#3848).
            let (credentials, saved_credentials) = if fake_model || local_model.is_some() {
                (None, None)
            } else {
                let credentials =
                    ops::resolve_up_credentials(fake_model, ops::explicit_model_credential_env());
                let saved_credentials = credentials
                    .is_none()
                    .then(ops::saved_model_credential)
                    .flatten();
                (credentials, saved_credentials)
            };
            emit(
                ops::up(
                    UpOpts {
                        retained_mail_values: None,
                        // Resolved by ops::up from the recorded release values.
                        retained_runner_values: None,
                        saved_credentials,
                        common: CommonOpts {
                            namespace,
                            release,
                            dry_run,
                        },
                        chart,
                        no_expose,
                        set,
                        set_string: vec![],
                        allow_egress_host,
                        // Populated by ops::up (resolve named providers to host
                        // routes on a live run); empty here so the pure builder and
                        // --dry-run start clean.
                        resolved_egress_cidrs: vec![],
                        allow_web_egress,
                        fake_model,
                        credentials,
                        local_model,
                        // Default `agentSandbox.runner.model` from the shell
                        // `CURIE_MODEL` (None when unset/empty) for cross-tier
                        // parity with `local up` (#361).
                        model: model.or_else(|| {
                            std::env::var("CURIE_MODEL").ok().filter(|s| !s.is_empty())
                        }),
                        // Populated by ops::up (generate on fresh install / reuse on
                        // upgrade); empty here so the pure builder starts clean.
                        secrets: vec![],
                        // Resolved by ops::up from the flags below plus the
                        // value the release already recorded; Untouched here so
                        // the pure builder starts clean.
                        github_token: ops::GithubTokenPlan::Untouched,
                        dev,
                        adopt,
                    },
                    github_token,
                    clear_github_token,
                )
                .await?,
            )
        }
        ClusterAction::Down {
            namespace,
            release,
            yes,
            dry_run,
        } => emit(
            ops::down(DownOpts {
                common: CommonOpts {
                    namespace,
                    release,
                    dry_run,
                },
                yes,
            })
            .await?,
        ),
        ClusterAction::Rollback {
            revision,
            allow_failed_revision,
            live_schema_revision,
            namespace,
            release,
            yes,
            dry_run,
        } => emit(
            ops::rollback(RollbackOpts {
                common: CommonOpts {
                    namespace,
                    release,
                    dry_run,
                },
                revision,
                allow_failed_revision,
                yes,
                disable_schema_gate: false,
                live_schema_revision,
            })
            .await?,
        ),
        ClusterAction::Upgrade {
            to,
            namespace,
            release,
            chart,
            yes,
            dry_run,
            forward_only,
        } => {
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                &to,
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_upgrade_chart(resolved, dry_run).await?;
            emit(
                ops::upgrade(UpgradeOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    to,
                    chart,
                    yes,
                    forward_only,
                })
                .await?,
            )
        }
        ClusterAction::Status {
            namespace,
            release,
            dry_run,
        } => emit(
            ops::status(
                CommonOpts {
                    namespace,
                    release,
                    dry_run,
                },
                target.is_some(),
            )
            .await?,
        ),
        ClusterAction::Observability {
            query,
            namespace,
            release,
            dry_run,
            open,
        } => match query {
            None => emit(
                ops::observability(
                    CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    open,
                )
                .await?,
            ),
            Some(_) if open => Err(curie::exit::usage(
                "--open cannot be combined with an observability query",
            )),
            Some(_) if dry_run => Err(curie::exit::usage(
                "--dry-run applies to bare cluster observability discovery, not API queries",
            )),
            Some(query) => {
                emit_boxed(run_cluster_observability_query(query, namespace, release).await?)
            }
        },
        ClusterAction::MigrateStore {
            phase,
            namespace,
            release,
            chart,
            bucket,
            keep_staging,
            dry_run,
        } => {
            use curie::migrate_store as ms;
            let common = curie::ops::CommonOpts {
                namespace,
                release,
                dry_run,
            };
            let out = match phase.as_deref() {
                Some("import") => ms::run_import(&common, &bucket, keep_staging).await?,
                other => {
                    let resolved = artifacts::resolve_chart(
                        chart.as_deref(),
                        artifacts::Channel::current(),
                        artifacts::version(),
                        artifacts::cache_root,
                        std::path::Path::new("charts/curie").is_dir(),
                    )?;
                    let chart = materialize_artifact(resolved, dry_run, "chart").await?;
                    if other == Some("export") {
                        ms::run_export(&common, &chart, &bucket).await?
                    } else {
                        ms::run_auto(&common, &chart, &bucket).await?
                    }
                }
            };
            emit(out)
        }
        ClusterAction::Comms {
            slack,
            disconnect,
            app_token,
            bot_token,
            namespace,
            release,
            chart,
            dry_run,
        } => {
            comms::require_provider(slack)?;
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_artifact(resolved, dry_run, "chart").await?;
            emit(
                comms::comms(CommsOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    chart,
                    app_token,
                    bot_token,
                    disconnect,
                })
                .await?,
            )
        }
        ClusterAction::GithubApp {
            app_id,
            private_key,
            existing_secret,
            existing_secret_key,
            clone_base,
            disconnect,
            namespace,
            release,
            chart,
            dry_run,
        } => {
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_artifact(resolved, dry_run, "chart").await?;
            emit(
                crate_github_app::github_app(
                    crate_github_app::GithubAppOpts {
                        common: CommonOpts {
                            namespace,
                            release,
                            dry_run,
                        },
                        chart,
                        app_id,
                        private_key_path: private_key,
                        existing_secret,
                        existing_secret_key,
                        disconnect,
                    },
                    &clone_base,
                )
                .await?,
            )
        }
        ClusterAction::Factory {
            intake,
            repos,
            label,
            mention,
            card_base_url,
            webhook_secret_file,
            github_api_egress,
            disable,
            app_id,
            private_key_file,
            org,
            timeout,
            runner_image,
            namespace,
            release,
            chart,
            dry_run,
        } => {
            let runner_binding = runner_image
                .as_deref()
                .map(parse_runner_image_binding)
                .transpose()?;
            let webhook_secret = if disable {
                None
            } else {
                curie::factory_intake::resolve_webhook_secret(webhook_secret_file.as_deref())?
            };
            let resolved = artifacts::resolve_chart(
                chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_artifact(resolved, dry_run, "chart").await?;
            emit_boxed(
                curie::factory_intake::factory_intake(curie::factory_intake::FactoryIntakeOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    chart,
                    repos,
                    label,
                    mention,
                    card_base_url,
                    webhook_secret,
                    github_api_egress,
                    disable,
                    timeout_seconds: timeout,
                    app_id,
                    private_key_file,
                    org,
                    intake,
                    runner_binding,
                })
                .await?,
            )
        }
        ClusterAction::Message {
            text,
            channel,
            agent,
            thread,
            r#continue,
            namespace,
            release,
            chart,
            listen_host,
            listen_port,
            valkey_local_port,
            valkey_password,
            api_local_port,
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
                TurnVerb::Cluster,
                CliTurnArgs {
                    channel,
                    thread,
                    namespace,
                    release,
                    chart,
                    listen_host,
                    timeout_secs,
                    api_url: None,
                    // The cluster tier has no dev default to bind (#786), so
                    // hand `apply_continue` the sentinel it compares against
                    // when nothing was supplied; the real value is resolved
                    // below, once the release is known.
                    api_key: api_key
                        .clone()
                        .unwrap_or_else(|| message::DEFAULT_API_KEY.to_string()),
                    agent,
                },
                state,
                // Empty is unset (#540), so the recorded-env bail below still
                // fires when $CURIE_API_KEY is exported blank.
                std::env::var("CURIE_API_KEY")
                    .ok()
                    .filter(|v| !v.is_empty()),
            )?;
            let resolved_chart = artifacts::resolve_chart(
                resolved.chart.as_deref(),
                artifacts::Channel::current(),
                artifacts::version(),
                artifacts::cache_root,
                std::path::Path::new("charts/curie").is_dir(),
            )?;
            let chart = materialize_artifact(resolved_chart, dry_run, "chart").await?;
            // `cluster up` randomizes both credentials per release, so an
            // omitted flag reads the release's own Secret rather than the
            // dev sentinel that 401s / fails Valkey auth on a real install
            // (#786). Explicit flag or env still wins.
            let api_key = message::resolve_cluster_credential(
                api_key,
                dry_run,
                message::DEFAULT_API_KEY,
                || ops::discover_api_key(&resolved.namespace, &resolved.release),
            )
            .await?;
            let valkey_password = message::resolve_cluster_credential(
                valkey_password,
                dry_run,
                message::DEFAULT_VALKEY_PASSWORD,
                || ops::discover_valkey_password(&resolved.namespace, &resolved.release),
            )
            .await?;
            message::message(MessageOpts {
                text,
                channel: resolved.channel,
                agent: resolved.agent,
                thread: resolved.thread,
                namespace: resolved.namespace,
                release: resolved.release,
                chart,
                listen_host: resolved.listen_host,
                listen_port,
                valkey_local_port,
                valkey_password,
                api_local_port,
                api_key,
                user,
                stream,
                timeout_secs: resolved.timeout_secs,
                dry_run,
                local: false,
                api_url: None,
            })
            .await
        }
        ClusterAction::Eval {
            cases,
            case_id,
            channel,
            agent,
            namespace,
            release,
            listen_host,
            listen_port,
            valkey_local_port,
            valkey_password,
            api_local_port,
            api_key,
            user,
            stream,
            timeout_secs,
            model,
            concurrency,
            sampling,
            dry_run,
        } => {
            message::validate_eval_models(&model)?;
            // `cluster up` randomizes both credentials per release, so an
            // omitted flag reads the release's own Secret rather than the
            // dev sentinel that 401s / fails Valkey auth on a real install
            // (#790, mirroring the #786 fix for `cluster message`). Explicit
            // flag or env still wins.
            let api_key = message::resolve_cluster_credential(
                api_key,
                dry_run,
                message::DEFAULT_API_KEY,
                || ops::discover_api_key(&namespace, &release),
            )
            .await?;
            let valkey_password = message::resolve_cluster_credential(
                valkey_password,
                dry_run,
                message::DEFAULT_VALKEY_PASSWORD,
                || ops::discover_valkey_password(&namespace, &release),
            )
            .await?;
            message::eval(message::EvalOpts {
                cases,
                case_ids: case_id,
                channel,
                agent,
                namespace,
                release,
                listen_host,
                listen_port,
                valkey_local_port,
                valkey_password,
                api_local_port,
                api_key,
                user,
                stream,
                timeout_secs,
                dry_run,
                local: false,
                api_url: None,
                models: model,
                concurrency,
                sampling: sampling.config()?,
            })
            .await
        }
        ClusterAction::Deploy {
            plugin_dir,
            agent,
            target,
            identity,
            all_targets,
            api_url,
            namespace,
            release,
            chart,
            api_key,
            slack_channel,
            repo,
            workspace,
            no_workspace,
            env,
            label,
            secret,
            api_local_port,
        } => {
            if workspace {
                commands::warn_if_empty_github_repo_allowlist(&namespace, &release).await;
            }
            let api_key = commands::normalize_deploy_api_key(api_key);
            // ADR-0057 (supersedes ADR-0024's deploy transport): with no
            // explicit --api-key, discover the release's strong Secret key;
            // an explicit key wins.
            let key_auto_discovered = commands::deploy_needs_key_discovery(api_key.as_deref());
            let api_key = if key_auto_discovered {
                ops::discover_api_key(&namespace, &release).await?
            } else {
                api_key.expect("explicit key present when discovery not needed")
            };
            // An explicit --api-url / CURIE_API_URL direct-dials the given
            // URL. Otherwise self-plumb a kubectl port-forward to the
            // release's api service so the discovered strong key travels only
            // over the loopback tunnel, never over the cleartext UI /api
            // NodePort proxy. Hold the child until after deploy returns;
            // kill_on_drop tears it down on every exit path.
            // 0 (the clap default) requests a kernel-assigned port, so a
            // squatted 8123 and two concurrent deploys are both structurally
            // impossible; an explicit --api-local-port stays an exact
            // override and still gets #1739's occupied-port refusal (#1533
            // symptom 2, the verb #1740 did not reach).
            let local_port = api_local_port;
            let _deploy_pf;
            // `Some` iff this deploy self-plumbed, carrying the fullname the
            // tunnel actually forwards to. That one value decides the
            // transport, names the Service in the diagnostics below, and
            // tells the unreachable hint which recovery to describe: the
            // auto path really opened a tunnel; the explicit path
            // direct-dialed and did not.
            let tunnel = commands::deploy_api_tunnel(
                api_url.as_deref(),
                &namespace,
                &release,
                local_port,
                message::API_REMOTE_PORT,
            )
            .await;
            let api_url = match &tunnel {
                Some((fullname, pf_cmd)) => {
                    let (deploy_pf, effective_port) =
                        message::start_port_forward(pf_cmd, local_port, "deploy api").await?;
                    _deploy_pf = Some(deploy_pf);
                    // svc/<release>-api serves the platform API at ROOT, so the
                    // base URL has NO /api suffix (the /api in ADR-0024 was only
                    // because the request went through the UI pod).
                    let base_url = format!("http://127.0.0.1:{effective_port}");
                    // The tunnel is TCP-alive, but a squatted local port and a
                    // Service name that resolved to the wrong workload are both
                    // TCP-alive too -- `start_port_forward`'s bind and readiness
                    // checks cannot tell them from the real API. Reading the
                    // UNAUTHENTICATED /health is the only check that can, and it
                    // is the compensating control for name resolution falling
                    // back to the chart rule.
                    //
                    // Placed here, at tunnel setup, deliberately: `--all-targets`
                    // posts several bundles over this one tunnel dev-before-prod
                    // (#1279), so verifying once BEFORE the target loop is what
                    // keeps a refusal a clean refusal instead of a half-deployed
                    // repository.
                    //
                    // #705: the auto-discovered strong release key is already in
                    // hand and this endpoint is not yet proven to be Curie, so the
                    // probe carries no key and a 401/403 is never retried with one.
                    //
                    // #1908: this returns `Err` rather than exiting, so the scope
                    // unwinds, `_deploy_pf` drops, and `kill_on_drop` reaps the
                    // kubectl child instead of orphaning it with its port held.
                    if let Err(observed) = api::verify_is_curie_api(&base_url).await {
                        let api_svc = fullname.resource("api");
                        return Err(anyhow::Error::from(
                            curie::exit::CliError::failure(format!(
                                "refusing to deploy: the endpoint behind the self-plumbed \
                                 tunnel is not the Curie API. {observed}. `cluster deploy` \
                                 forwarded svc/{api_svc} in namespace {namespace} to \
                                 127.0.0.1:{effective_port} and posted nothing. Re-run with \
                                 --api-local-port <port> to bind a different local port, or \
                                 pass --api-url <url> to dial the API directly without a \
                                 tunnel."
                            ))
                            .with_fix(
                                "re-run with --api-local-port <port>, or pass --api-url <url> \
                                 to dial the platform API directly",
                            ),
                        ));
                    }
                    base_url
                }
                None => {
                    _deploy_pf = None;
                    let url = api_url.expect("explicit url when no port-forward");
                    // #705: the strong auto-discovered key must never egress
                    // cleartext. An explicit non-loopback `http://` --api-url
                    // would leak it on the wire, and no loopback tunnel is in
                    // play here to protect it. Refuse rather than warn, unless
                    // the operator opted in with an explicit --api-key.
                    if key_auto_discovered && api::is_insecure_endpoint(&url) {
                        bail!(
                            "refusing to send the auto-discovered release key over cleartext \
                             HTTP to {url}: the strong key would leak on the wire. Pass \
                             --api-key explicitly to acknowledge, use an https:// URL, or omit \
                             --api-url to reach the release over the loopback port-forward."
                        );
                    }
                    url
                }
            };
            let connect_hint = match tunnel.as_ref() {
                // Self-plumbed: name the Service the tunnel ACTUALLY used,
                // as resolved from the cluster, so the operator can look up
                // the same object the CLI did.
                Some((fullname, _)) => {
                    let api_svc = fullname.resource("api");
                    format!(
                        "the platform API at {api_url} is unreachable. `cluster deploy` self-plumbs a kubectl port-forward to svc/{api_svc}; confirm the release is healthy with `curie cluster status`, or pass --api-url to dial the API directly."
                    )
                }
                // Explicit --api-url: no Service was contacted and none was
                // resolved, so this hint names the chart's no-override rule
                // rather than triggering a discovery round-trip on a path
                // that is pinned cluster-offline. Under an override install
                // the printed name may differ from the rendered one; the
                // recovery it suggests (omit --api-url) resolves it live.
                None => {
                    let api_svc = ops::chart_fullname(&release).resource("api");
                    format!(
                        "the platform API at {api_url} (from --api-url/CURIE_API_URL) is unreachable. `cluster deploy` dialed it directly with no port-forward; confirm that URL is reachable and the release is healthy with `curie cluster status`, or omit --api-url to self-plumb a loopback port-forward to svc/{api_svc}."
                    )
                }
            };
            // --all-targets onboards a repository in one invocation.
            // The list comes from the API, not a Rust YAML parse: ADR-0089
            // keeps exactly one parser for this file, and a second could
            // disagree with it about where a deploy lands.
            // The Bearer secret names this bundle's hosted connectors
            // declare (#2503, #2559). Read once here, from the same
            // connectors.yaml the deploy packs, so both cluster paths bind
            // the sandbox with only the name the derived header expands.
            let connector_env_secret_names = curie::connector_build::hosted_env_secret_names(
                &curie::connector_build::load(&plugin_dir)?,
            );
            // The runner layer digest the lock records (#3260); None
            // clears an earlier value on the release.
            let runner_image = curie::connector_build::locked_runner_image(&plugin_dir)?;
            // A layer built on another platform runner is one the worker
            // may not serve (#3218, ADR 0173 decision 5): refuse it before
            // anything is posted, naming the `curie build` that fixes it.
            if runner_image.is_some() {
                curie::cluster_secrets::check_layered_runner_base(
                    &ops::CommonOpts {
                        namespace: namespace.clone(),
                        release: release.clone(),
                        dry_run: false,
                    },
                    &plugin_dir,
                )
                .await?;
            }

            let targets: Vec<Option<String>> = if all_targets {
                let path = plugin_dir.join("deploy.yaml");
                let content = std::fs::read_to_string(&path).map_err(|err| {
                    curie::exit::usage(format!(
                        "--all-targets needs a deploy.yaml in the bundle, but {} could \
                         not be read: {err}",
                        path.display()
                    ))
                })?;
                let listed = api::ApiClient::new(&api_url, &api_key)?
                    .list_deploy_targets(&content)
                    .await?;
                if listed.targets.is_empty() {
                    bail!(
                        "deploy.yaml declares no targets, so --all-targets has nothing to \
                         deploy. Declare at least one (ADR-0089), or drop the flag and pass \
                         --agent/--env/--slack-channel."
                    );
                }
                ui::ui().note(&format!(
                    "onboarding {} target(s): {}",
                    listed.targets.len(),
                    listed
                        .targets
                        .iter()
                        .map(|t| t.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                listed.targets.into_iter().map(|t| Some(t.name)).collect()
            } else {
                vec![target]
            };

            // ONE observation for the whole invocation (#2496). The release
            // is the same for every `--all-targets` entry, so assessing per
            // target would be N helm subprocesses for one answer. Read
            // before activation and printed after: an operator who edits
            // helm values mid-deploy sees a stale line, and `curie doctor`
            // is the authority on the current state.
            // Only a deploy that BINDS a repository can make a delivery
            // claim, and the line is printed inside that same block, so a
            // deploy without `--repo` must not pay for the helm read.
            let delivery = match &repo {
                None => None,
                Some(_) => Some(curie::delivery::assess(
                    &curie::doctor::observe_delivery(&ops::CommonOpts {
                        namespace: namespace.clone(),
                        release: release.clone(),
                        dry_run: false,
                    })
                    .await,
                )),
            };

            if all_targets {
                let first_target = targets
                    .first()
                    .cloned()
                    .flatten()
                    .expect("all target entries always have a target name");
                // Deliberately NOT joined with the delivery observation
                // above (#2496). Awaiting the bind here keeps a bind failure
                // isolated: nothing else is in flight when it returns `Err`.
                // Joining them lets a bind failure overlap the still-running
                // helm subprocess of `observe_delivery`, and `connectors::run`
                // is not `kill_on_drop`, so cancelling the parent in that
                // window leaks a `kubectl` child that sequential ordering
                // would never have spawned. The wall-clock saving is not
                // worth either.
                let connector_target =
                    match curie::connectors::bind_current_cluster(&namespace, &release).await {
                        Ok(target) => target,
                        Err(err) => {
                            let payload = commands::all_targets_deploy_failure_json(
                                &first_target,
                                &[],
                                None,
                                &err,
                            );
                            return Err(curie::exit::with_json_payload(err, payload));
                        }
                    };
                let app_name = match curie::connectors::discover_app_name(&connector_target).await {
                    Ok(app_name) => app_name,
                    Err(err) => {
                        let payload = commands::all_targets_deploy_failure_json(
                            &first_target,
                            &[],
                            None,
                            &err,
                        );
                        return Err(curie::exit::with_json_payload(err, payload));
                    }
                };

                // Resolve every target and every connector credential before
                // activating the first deployment. Preparation may create
                // agents and versions, but it does not POST a deployment.
                let mut prepared_targets = Vec::new();
                for target in targets {
                    let target = target.expect("all target entries always have a target name");
                    let prepared_deploy = match commands::prepare_deploy(DeployOpts {
                        delivery: delivery.clone(),
                        plugin_dir: plugin_dir.clone(),
                        agent: agent.clone(),
                        target: Some(target.clone()),
                        identity: None,
                        api_url: api_url.clone(),
                        api_key: api_key.clone(),
                        slack_channel: slack_channel.clone(),
                        repo: repo.clone(),
                        workspace: commands::WorkspaceIntent::from_flags(workspace, no_workspace),
                        tier: commands::DeployTier::Cluster,
                        env,
                        label: label.clone(),
                        secret: Vec::new(),
                        secret_binding_supported: false,
                        connect_hint: connect_hint.clone(),
                    })
                    .await
                    {
                        Ok(prepared) => prepared,
                        Err(err) => {
                            let payload =
                                commands::all_targets_deploy_failure_json(&target, &[], None, &err);
                            return Err(curie::exit::with_json_payload(err, payload));
                        }
                    };
                    let connector_version = ConnectorVersion {
                        agent_id: prepared_deploy.agent_id(),
                        agent_name: prepared_deploy.agent_name(),
                        version_id: prepared_deploy.version_id(),
                    };
                    let prepared_connectors = match prepare_connectors(
                        &api_url,
                        &api_key,
                        &namespace,
                        &release,
                        &app_name,
                        connector_version,
                        connector_target.clone(),
                    )
                    .await
                    {
                        Ok(prepared) => prepared,
                        Err(err) => {
                            let payload =
                                commands::all_targets_deploy_failure_json(&target, &[], None, &err);
                            return Err(curie::exit::with_json_payload(err, payload));
                        }
                    };
                    prepared_targets.push((target, prepared_deploy, prepared_connectors));
                }

                // Activate and reconcile in the API's declared target order.
                let mut completed = Vec::new();
                for (target, prepared_deploy, prepared_connectors) in prepared_targets {
                    let deployed = match commands::deploy_prepared(prepared_deploy).await {
                        Ok(deployed) => deployed,
                        Err(err) => {
                            let payload = commands::all_targets_deploy_failure_json(
                                &target, &completed, None, &err,
                            );
                            return Err(curie::exit::with_json_payload(err, payload));
                        }
                    };

                    // #2503: bind the connector's own declared secret
                    // names alongside the explicit `--secret` set, with the
                    // cluster-scoped values this plan already resolved
                    // (#1913), so a hosted connector's derived Bearer header
                    // expands in the sandbox. Binding precedes the apply that
                    // consumes `prepared_connectors`.
                    if let Err(err) = bind_and_apply_cluster_connectors(
                        &namespace,
                        &release,
                        chart.as_deref(),
                        &deployed.agent_name,
                        &secret,
                        &connector_env_secret_names,
                        runner_image.clone(),
                        prepared_connectors,
                    )
                    .await
                    {
                        let payload = commands::all_targets_deploy_failure_json(
                            &target,
                            &completed,
                            Some(&deployed),
                            &err,
                        );
                        return Err(curie::exit::with_json_payload(err, payload));
                    }

                    completed.push(commands::AllTargetsDeployResult {
                        target,
                        result: deployed,
                    });
                }
                emit(commands::AllTargetsDeployOutput { results: completed })
            } else {
                let target = targets
                    .into_iter()
                    .next()
                    .expect("the target list is never empty");
                let deployed = commands::deploy(DeployOpts {
                    delivery,
                    plugin_dir: plugin_dir.clone(),
                    agent: agent.clone(),
                    target,
                    identity: identity.clone(),
                    api_url: api_url.clone(),
                    api_key: api_key.clone(),
                    slack_channel: slack_channel.clone(),
                    repo: repo.clone(),
                    workspace: commands::WorkspaceIntent::from_flags(workspace, no_workspace),
                    env,
                    label: label.clone(),
                    secret: secret.clone(),
                    secret_binding_supported: true,
                    connect_hint: connect_hint.clone(),
                    tier: commands::DeployTier::Cluster,
                })
                .await?;

                // Stand up whatever the bundle's connectors.yaml declares
                // (ADR-0086, #1063). After the deploy, so the objects exist
                // before the next turn reaches for them; the credentials here
                // are the CONNECTOR's, resolved locally and written straight to
                // a K8s Secret, which is a different path from the sandbox
                // secret delivery #440 tracks. The connector's own declared
                // Bearer name is bound into the sandbox too (#2503, #2559),
                // unioned with the explicit `--secret` set, so the runner
                // can expand the derived header and then drop the name.
                // Resolved before binding, applied after it.
                let prepared_connectors = prepare_cluster_connectors(
                    &api_url,
                    &api_key,
                    &namespace,
                    &release,
                    &deployed.agent_id,
                    &deployed.agent_name,
                    &deployed.version_id,
                )
                .await?;

                bind_and_apply_cluster_connectors(
                    &namespace,
                    &release,
                    chart.as_deref(),
                    &deployed.agent_name,
                    &secret,
                    &connector_env_secret_names,
                    runner_image,
                    prepared_connectors,
                )
                .await?;
                emit(deployed)
            }
        }
        ClusterAction::Kill {
            agent,
            conn,
            yes,
            dry_run,
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
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
            )
        }
        ClusterAction::Console {
            action:
                ClusterConsoleAction::Login {
                    subject,
                    api_url,
                    namespace,
                    release,
                    dry_run,
                },
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(
                ClusterConn {
                    api_url,
                    api_key: None,
                    namespace,
                    release,
                },
                dry_run,
            )
            .await?;
            emit(commands::console_login(&api_url, &api_key, &subject, dry_run).await?)
        }
        ClusterAction::Resume {
            agent,
            conn,
            dry_run,
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::resume(AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                })
                .await?,
            )
        }
        ClusterAction::Overrides {
            agent,
            model,
            clear_model,
            reviewer_model,
            clear_reviewer_model,
            thinking,
            clear_thinking,
            execution_deadline,
            clear_execution_deadline,
            runner_resources,
            clear_runner_resources,
            memory_writes,
            conn,
            dry_run,
        } => {
            // Resolve the flag pairs BEFORE discovering the connection: a
            // contradictory invocation is a usage error, and making the
            // operator wait on a cluster lookup to be told so is worse than
            // telling them immediately.
            let model = commands::OverrideChange::resolve("model", model, clear_model)?;
            let reviewer_model = commands::OverrideChange::resolve(
                "reviewer-model",
                reviewer_model,
                clear_reviewer_model,
            )?;
            let thinking = commands::OverrideChange::resolve("thinking", thinking, clear_thinking)?;
            let execution_deadline = commands::OverrideChange::resolve_execution_deadline(
                execution_deadline,
                clear_execution_deadline,
            )?;
            let runner_resources = commands::OverrideChange::resolve_runner_resources(
                runner_resources,
                clear_runner_resources,
            )?;
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::overrides_with_memory_writes(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    model,
                    reviewer_model,
                    thinking,
                    execution_deadline,
                    runner_resources,
                    commands::memory_writes_flag(memory_writes.as_deref()),
                )
                .await?,
            )
        }
        ClusterAction::PublicationPolicy {
            agent,
            policy,
            draft,
            no_draft,
            branch_prefix,
            clear_branch_prefix,
            conn,
            dry_run,
        } => {
            let (api_url, api_key, _port_forward) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
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
            )
        }
        ClusterAction::Surfaces {
            agent,
            add,
            endpoint,
            adapter,
            remove,
            conn,
            dry_run,
        } => {
            // Resolve the flag pair BEFORE discovering the connection, for
            // the same reason `Overrides` does: a mistyped pair must not
            // cost a cluster lookup, nor be reported as a connection
            // failure instead of the usage error it is.
            let change = commands::ChannelChange::resolve(add, remove, endpoint, adapter)?;
            let (api_url, api_key, _port_forward) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::channel_bindings(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    change,
                )
                .await?,
            )
        }
        ClusterAction::Callers {
            agent,
            surface,
            adapter,
            set,
            clear,
            conn,
            dry_run,
        } => {
            // Resolved before the connection for the same reason as
            // `Surfaces`: a malformed id must be a usage error, not a
            // cluster lookup that then fails for an unrelated reason.
            let change = commands::CallersChange::resolve(set, clear)?;
            let (api_url, api_key, _port_forward) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::channel_callers(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
                    &surface,
                    adapter,
                    change,
                )
                .await?,
            )
        }
        ClusterAction::ChannelToken {
            agent,
            kind,
            address,
            ttl,
            show_exp,
            conn,
            dry_run,
        } => {
            let namespace = conn.namespace.clone();
            let release = conn.release.clone();
            let (api_url, api_key, _port_forward) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                crate_channel_token::channel_token(crate_channel_token::ChannelTokenOpts {
                    common: CommonOpts {
                        namespace,
                        release,
                        dry_run,
                    },
                    api_url,
                    api_key,
                    agent,
                    kind,
                    address,
                    ttl,
                    show_exp,
                })
                .await?,
            )
        }
        ClusterAction::Budget {
            agent,
            limit,
            output_tokens,
            conn,
            dry_run,
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
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
            )
        }
        ClusterAction::ResetThread {
            agent,
            thread_key,
            conn,
            yes,
            dry_run,
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
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
            )
        }
        ClusterAction::WorkItems {
            id,
            agent,
            conn,
            dry_run,
        } => {
            // Validate before any network access, through the centralized
            // structured error path (cli/CLAUDE.md error contract).
            let id = commands::validated_work_item_id(id)?;
            // `_cluster_api_pf` is the port-forward guard; it must live for
            // the whole call.
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::work_items(commands::WorkItemsOpts {
                    api_url,
                    api_key,
                    id,
                    agent,
                    dry_run,
                    tier: "cluster",
                })
                .await?,
            )
        }
        ClusterAction::Schedules {
            agent,
            pause,
            resume,
            conn,
            dry_run,
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::schedules(commands::SchedulesOpts {
                    api_url,
                    api_key,
                    agent,
                    pause,
                    resume,
                    dry_run,
                })
                .await?,
            )
        }
        ClusterAction::Hook { action, conn } => {
            let dry_run = match &action {
                ClusterHookAction::Fire { dry_run, .. }
                | ClusterHookAction::Record { dry_run, .. } => *dry_run,
            };
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(
                ClusterConn {
                    api_url: conn.api_url,
                    api_key: conn.api_key,
                    namespace: conn.namespace,
                    release: conn.release,
                },
                dry_run,
            )
            .await?;
            match action {
                ClusterHookAction::Fire {
                    agent,
                    name,
                    wait_secs,
                    dry_run,
                } => emit(
                    commands::hook_fire(commands::HookFireOpts {
                        api_url,
                        api_key,
                        agent,
                        name,
                        dry_run,
                        wait_secs,
                        tier: "cluster",
                    })
                    .await?,
                ),
                ClusterHookAction::Record {
                    agent,
                    name,
                    id,
                    dry_run,
                } => emit(
                    commands::hook_record(commands::HookRecordOpts {
                        api_url,
                        api_key,
                        agent,
                        name,
                        run_id: id,
                        dry_run,
                    })
                    .await?,
                ),
            }
        }
        ClusterAction::Delete {
            agent,
            conn,
            yes,
            dry_run,
        } => {
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
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
            )
        }
        ClusterAction::Versions { target } => {
            let ClusterAgentTarget {
                agent,
                conn,
                dry_run,
            } = target;
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::versions(AgentActionOpts {
                    api_url,
                    api_key,
                    agent,
                    dry_run,
                })
                .await?,
            )
        }
        ClusterAction::Hooks { action } => {
            let (target, file, verb) = match action {
                ClusterHooksAction::Show { target } => (target, None, "show"),
                ClusterHooksAction::Configure { target, file } => (target, Some(file), "configure"),
                ClusterHooksAction::Secret { target } => (target, None, "secret"),
            };
            let ClusterAgentTarget {
                agent,
                conn,
                dry_run,
            } = target;
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            let opts = AgentActionOpts {
                api_url,
                api_key,
                agent,
                dry_run,
            };
            match verb {
                "show" => emit(commands::hooks_show(opts).await?),
                "configure" => emit(
                    commands::hooks_configure(opts, &file.expect("configure supplies a file"))
                        .await?,
                ),
                "secret" => emit(commands::hooks_secret(opts).await?),
                _ => unreachable!(),
            }
        }
        ClusterAction::Memory {
            target,
            add,
            delete,
            channel,
            guidance,
        } => {
            let ClusterAgentTarget {
                agent,
                conn,
                dry_run,
            } = target;
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            let opts = AgentActionOpts {
                api_url,
                api_key,
                agent,
                dry_run,
            };
            match (guidance.action(), add, delete) {
                (Some(action), _, _) => emit_memory_guidance(opts, action).await,
                (None, None, Some(id)) => emit(commands::memory_delete(opts, id, channel).await?),
                (None, None, None) => emit(commands::memory(opts, channel).await?),
                (None, Some(content), _) => {
                    emit(commands::memory_add(opts, content, "cluster").await?)
                }
            }
        }
        // @spec ACTION-EXECUTOR-23
        ClusterAction::Actions { verb } => {
            let (verb, conn) = verb.into_parts();
            // Validated before the connection is resolved, so a malformed id
            // or a missing principal never reaches discovery or the API.
            let verb = verb.validate()?;
            // `_cluster_api_pf` is the port-forward guard; it must live for
            // the whole call.
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, false).await?;
            emit(
                commands::actions(
                    commands::ActionsOpts {
                        api_url,
                        api_key,
                        tier: "cluster",
                    },
                    verb,
                )
                .await?,
            )
        }
        ClusterAction::Approvals {
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
        } => {
            let ClusterAgentTarget {
                agent,
                conn,
                dry_run,
            } = target;
            let (api_url, api_key, _cluster_api_pf) = resolve_cluster_conn(conn, dry_run).await?;
            emit(
                commands::approvals(
                    AgentActionOpts {
                        api_url,
                        api_key,
                        agent,
                        dry_run,
                    },
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
            )
        }
    }
}
