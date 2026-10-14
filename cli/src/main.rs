//! The `curie` binary: `init`, `skill <up|down|status|message|eval|approvals>`
//! for a local runner, `local <up|down|status|message|deploy>` for the compose stack,
//! and `cluster <up|down|status|comms|message|deploy>` for Kubernetes and the
//! platform API. Task I1; contracts are frozen in packages/aci-protocol and
//! packages/plugin-format.

mod args;
mod dispatch;

use clap::{CommandFactory, FromArgMatches};
use curie::ui::{self, Ui};

use args::Cli;
use dispatch::{resolve_cluster_command_target, run, ClusterTargetSources};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(hint) = curie::retired_hint(&args) {
        eprintln!("{hint}");
        std::process::exit(curie::exit::ExitClass::Usage.code());
    }

    if let Some(err) = curie::message::reject_agent_named_message(&args) {
        // Clap's default extra-positional error fires before `Ui` exists, so
        // this intercept has to emit the ADR-0021 `{error, fix}` payload (and
        // the human Error:/Fix: pair) itself. `--json` is global and may sit
        // anywhere in argv.
        let json = args.iter().any(|arg| arg == "--json");
        let (class, fix) = curie::exit::classify(&err);
        if json {
            let payload = curie::exit::error_json(&err);
            if let Ok(line) = serde_json::to_string(&payload) {
                println!("{line}");
            }
        } else {
            eprintln!("Error: {err}");
            if let Some(fix) = fix {
                eprintln!("Fix: {fix}");
            }
        }
        std::process::exit(class.code());
    }

    let matches = Cli::command().get_matches();
    let cluster_target_sources = ClusterTargetSources::from_matches(&matches);
    let mut cli = Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    ui::init(Ui::from_process(cli.color, cli.debug, cli.quiet, cli.json));
    // main never returns Err (which would give anyhow's default exit 1 and skip
    // classification). Run the command, then map any error to a semantic exit
    // code: the JSON payload goes to stdout under --json. Otherwise the human
    // presentation goes to stderr, debug receives the full cause chain, and the
    // class picks the exit code.
    let result = match resolve_cluster_command_target(cli.command.as_mut(), cluster_target_sources)
    {
        Ok(()) => run(cli.command).await,
        Err(error) => Err(error),
    };
    if let Err(err) = result {
        let (class, _fix) = curie::exit::classify(&err);
        if ui::ui().json() {
            let payload = curie::exit::wrapped_json_payload(&err)
                .unwrap_or_else(|| curie::exit::error_json(&err));
            ui::ui().emit_json(&payload);
        } else {
            let (message, remedy) = curie::exit::present_error(&err);
            eprintln!("Error: {message}");
            if let Some(remedy) = remedy {
                eprintln!("Fix: {remedy}");
            }
            ui::ui().plumbing(&format!("{err:#}"));
        }
        std::process::exit(class.code());
    }
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
