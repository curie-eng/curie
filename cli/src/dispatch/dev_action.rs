//! Dispatch for `curie dev` contributor commands.

use super::*;

/// Run one parsed `dev` subcommand.
pub(super) async fn run(action: DevAction) -> Result<()> {
    match action {
        DevAction::Preflight {
            fast,
            base,
            dry_run,
            pr_body,
            title,
        } => emit(
            commands::dev_preflight(fast, &base, dry_run, pr_body.as_deref(), title.as_deref())
                .await?,
        ),
        DevAction::ModelScript { args } => {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            commands::dev_script("cli/scripts/model-script.sh", &args).await
        }
        DevAction::ModelCredit => curie::openrouter_credit::model_credit().await,
        DevAction::Hooks { action } => match action {
            HooksAction::Install => commands::dev_hooks_install(),
        },
        DevAction::Contracts => commands::dev_script("scripts/check-contracts.sh", &[]).await,
        DevAction::ChartCheck => commands::dev_chart_check().await,
        DevAction::VerifyFixPin { change, selector } => {
            commands::dev_script(
                "cli/scripts/verify-fix-pin.sh",
                &[change.as_str(), selector.as_str()],
            )
            .await
        }
        DevAction::E2e => commands::dev_script("cli/scripts/e2e.sh", &[]).await,
        DevAction::E2eLadder => commands::dev_script("cli/scripts/e2e-ladder.sh", &[]).await,
        DevAction::TwoReleaseApprovalE2e => {
            commands::dev_script("cli/scripts/two-release-approval-e2e.sh", &[]).await
        }
        DevAction::FactoryE2e { args } => {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            commands::dev_script("cli/scripts/factory-e2e.sh", &args).await
        }
        DevAction::GithubStub { args } => {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            commands::dev_script("cli/scripts/github-stub.sh", &args).await
        }
        DevAction::E2eCiSelection {
            path,
            base,
            head,
            push,
        } => commands::dev_e2e_ci_selection(&path, base.as_deref(), head.as_deref(), push).await,
        DevAction::ChartRuntimeE2e { force } => {
            let args: &[&str] = if force { &["--force"] } else { &[] };
            commands::dev_script("scripts/chart-runtime-e2e.sh", args).await
        }
        DevAction::RecoveryDrill {
            surface,
            scenario,
            bound_seconds,
            force,
        } => {
            let bound = bound_seconds.to_string();
            let mut args: Vec<&str> = vec![
                "--surface",
                surface.as_str(),
                "--scenario",
                scenario.as_str(),
                "--bound-seconds",
                bound.as_str(),
            ];
            if force {
                args.push("--force");
            }
            if ui::ui().json() {
                args.push("--json");
            }
            commands::dev_script("cli/scripts/recovery-drill.sh", &args).await
        }
        DevAction::LeaseExpiryClusterProof {
            force,
            keep,
            self_test,
        } => {
            let mut args: Vec<&str> = Vec::new();
            if force {
                args.push("--force");
            }
            if keep {
                args.push("--keep");
            }
            if self_test {
                args.push("--self-test");
            }
            if ui::ui().json() {
                args.push("--json");
            }
            commands::dev_script("cli/scripts/lease-expiry-cluster-proof.sh", &args).await
        }
        DevAction::UpgradeDrill {
            scenario,
            also_predecessor,
            force,
            keep,
            self_test,
        } => {
            let mut args: Vec<&str> = vec!["--scenario", scenario.as_str()];
            if also_predecessor {
                args.push("--also-predecessor");
            }
            if force {
                args.push("--force");
            }
            if keep {
                args.push("--keep");
            }
            if self_test {
                args.push("--self-test");
            }
            if ui::ui().json() {
                args.push("--json");
            }
            commands::dev_script("cli/scripts/upgrade-drill.sh", &args).await
        }
        DevAction::ClusterUpgradeMatrix {
            scenario,
            force,
            keep,
            self_test,
        } => {
            let mut args: Vec<&str> = vec!["--scenario", scenario.as_str()];
            if force {
                args.push("--force");
            }
            if keep {
                args.push("--keep");
            }
            if self_test {
                args.push("--self-test");
            }
            if ui::ui().json() {
                args.push("--json");
            }
            commands::dev_script("cli/scripts/cluster-upgrade-matrix.sh", &args).await
        }
        DevAction::DocsLint => commands::dev_script("scripts/check-docs.sh", &[]).await,
        DevAction::PluginCompat => {
            commands::dev_script("scripts/check-plugin-compat.sh", &[]).await
        }
        DevAction::AgentSkills => commands::dev_script("scripts/check-agent-skills.sh", &[]).await,
        DevAction::EvalFalsifiability => {
            commands::dev_script("cli/scripts/eval-falsifiability.sh", &[]).await
        }
        DevAction::FieldParity => {
            commands::dev_script("cli/scripts/check-field-parity.sh", &[]).await
        }
        DevAction::EmitParity => {
            commands::dev_script("cli/scripts/check-emit-parity.sh", &[]).await
        }
        DevAction::VerbParity => {
            commands::dev_script("cli/scripts/check-verb-parity.sh", &[]).await
        }
        DevAction::SchemaBaseline => {
            commands::dev_script("cli/scripts/refresh-schema-baseline.sh", &[]).await
        }
        DevAction::NetpolCheck => {
            commands::dev_script("scripts/check-netpol-enforcement.sh", &[]).await
        }
        DevAction::VersionCheck => {
            commands::dev_script("scripts/check-version-consistency.sh", &[]).await
        }
        DevAction::WireTolerance => {
            commands::dev_script("scripts/check-wire-tolerance.sh", &[]).await
        }
        DevAction::RestoreDrill {
            check_backup,
            supplied_config,
            negative,
        } => {
            let mut args = Vec::new();
            if let Some(path) = check_backup {
                args.push("--check-backup".to_string());
                args.push(path.display().to_string());
            }
            if let Some(path) = supplied_config {
                args.push("--supplied-config".to_string());
                args.push(path.display().to_string());
            }
            if let Some(component) = negative {
                args.push("--negative".to_string());
                args.push(component);
            }
            let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
            commands::dev_script("cli/scripts/restore-drill.sh", &arg_refs).await
        }
        DevAction::BumpVersion { version, dry_run } => {
            commands::bump_version(&version, dry_run).await
        }
        DevAction::ReleaseAccept { ledger, self_test } => {
            curie::release_accept::run(ledger.as_deref(), self_test)
        }
    }
}
