//! `curie cluster access`: reach the Console and Langfuse of a release whose
//! services are `ClusterIP`, through loopback `kubectl port-forward` sessions
//! that live only while the command runs.
//!
//! Nothing is published on the host until an operator runs this command, and
//! nothing stays published after it ends: the forwards are children of this
//! process, bound to `127.0.0.1`, and are stopped and reaped on Ctrl-C,
//! SIGTERM, or when any one of them dies.

use std::time::Duration;

use anyhow::{bail, Context, Result};

use super::verbs::{
    api_suffix_path, dry_run_fullname, fetch_service, forward_local_port, parse_service, svc_cmd,
};
use super::{plain, release_fullname, require_on_path, CommonOpts, OpsCommand, ReleaseFullname};
use crate::ui::{CliOutput, DryRunPlan, Ui};

/// How long a forward gets to exit after it is told to stop.
const STOP_GRACE: Duration = Duration::from_secs(5);
/// How long a forward that has exited waits for a shutdown signal that may
/// still be on its way, so a Ctrl-C that stopped it is not reported as a
/// failure of the forward.
const SIGNAL_RACE_GRACE: Duration = Duration::from_millis(300);

/// One service `cluster access` can forward, in the order it is listed.
struct Surface {
    /// The chart component suffix (`<fullname>-<suffix>`).
    suffix: &'static str,
    label: &'static str,
    /// The Console opens on its API-backed view.
    api: bool,
    /// The flag that moves its local port.
    port_flag: &'static str,
}

const SURFACES: [Surface; 2] = [
    Surface {
        suffix: "ui",
        label: "Console",
        api: true,
        port_flag: "--ui-port",
    },
    Surface {
        suffix: "langfuse-web",
        label: "Langfuse",
        api: false,
        port_flag: "--langfuse-port",
    },
];

/// The flags of `cluster access` beyond the release.
#[derive(Debug, Clone)]
pub struct AccessOpts {
    pub common: CommonOpts,
    /// Local port for the Console. Default: derived like `cluster status`.
    pub ui_port: Option<u16>,
    /// Local port for Langfuse. Default: derived like `cluster status`.
    pub langfuse_port: Option<u16>,
}

/// One forward the command will run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Forward {
    suffix: &'static str,
    label: &'static str,
    port_flag: &'static str,
    service: String,
    local: u16,
    remote: u16,
    path: &'static str,
}

impl Forward {
    fn url(&self) -> String {
        format!("http://localhost:{}{}", self.local, self.path)
    }
}

/// Why a surface has nothing to forward. A skipped surface is reported, not
/// fatal, so a release without Langfuse still gives access to the Console.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Skipped {
    NotFound(String),
    Unreadable(String),
}

impl Skipped {
    fn describe(&self, label: &str) -> String {
        match self {
            Skipped::NotFound(service) => format!("{label}: service {service} not found"),
            Skipped::Unreadable(service) => format!("{label}: could not read service {service}"),
        }
    }
}

/// Decide one surface's forward from the service JSON kubectl returned (`None`
/// when kubectl could not read it). Pure, so every outcome is unit tested
/// with no cluster.
fn plan_forward(
    surface: &Surface,
    service: &str,
    svc_json: Option<&str>,
    local_override: Option<u16>,
) -> std::result::Result<Forward, Skipped> {
    let json = svc_json.ok_or_else(|| Skipped::NotFound(service.to_string()))?;
    let Some((_, _, remote)) = parse_service(json) else {
        return Err(Skipped::Unreadable(service.to_string()));
    };
    if remote == 0 {
        return Err(Skipped::Unreadable(service.to_string()));
    }
    Ok(Forward {
        suffix: surface.suffix,
        label: surface.label,
        port_flag: surface.port_flag,
        service: service.to_string(),
        local: local_override.unwrap_or_else(|| forward_local_port(remote)),
        remote,
        path: api_suffix_path(surface.api),
    })
}

/// The `kubectl port-forward` for one forward: the builder `cluster status`
/// hints and the API reads share, pinned to the IPv4 loopback address so
/// nothing but this host can reach it.
fn forward_command(namespace: &str, fullname: &ReleaseFullname, forward: &Forward) -> OpsCommand {
    let mut command = crate::message::port_forward_command(
        namespace,
        fullname,
        forward.suffix,
        forward.local,
        forward.remote,
    );
    // Right after `-n <namespace> port-forward`.
    command.args.insert(3, plain("--address"));
    command.args.insert(4, plain("127.0.0.1"));
    command
}

fn local_port_conflict(forwards: &[Forward]) -> Option<u16> {
    let (first, rest) = forwards.split_first()?;
    rest.iter()
        .find(|other| other.local == first.local)
        .map(|other| other.local)
}

/// What `cluster access` reports once every forward is open.
#[derive(Debug)]
pub struct ClusterAccessOutput {
    forwards: Vec<Forward>,
    skipped: Vec<String>,
}

impl CliOutput for ClusterAccessOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "forwards": self.forwards.iter().map(|forward| serde_json::json!({
                "name": forward.label,
                "url": forward.url(),
                "service": forward.service,
                "local_port": forward.local,
                "remote_port": forward.remote,
            })).collect::<Vec<_>>(),
            "skipped": self.skipped,
        })
    }

    fn render(&self, ui: &Ui) {
        for forward in &self.forwards {
            ui.kv(forward.label, &ui.url(&forward.url()));
        }
        for skipped in &self.skipped {
            ui.kv("skipped", skipped);
        }
    }
}

/// `cluster access --dry-run`: the commands that would run. The service ports
/// are read from the cluster at runtime, so the forward lines use the chart
/// defaults and say so.
pub fn access_plan(opts: &AccessOpts) -> DryRunPlan {
    let fullname = dry_run_fullname(&opts.common.release);
    let mut lines = vec![
        "# The service ports are read from the cluster at runtime; the port-forward lines below show the chart defaults."
            .to_string(),
    ];
    for surface in &SURFACES {
        lines.push(svc_cmd(&opts.common, &fullname, surface.suffix).display());
    }
    for (surface, (remote, override_port)) in SURFACES
        .iter()
        .zip([(80_u16, opts.ui_port), (3000, opts.langfuse_port)])
    {
        let forward = Forward {
            suffix: surface.suffix,
            label: surface.label,
            port_flag: surface.port_flag,
            service: fullname.resource(surface.suffix),
            local: override_port.unwrap_or_else(|| forward_local_port(remote)),
            remote,
            path: api_suffix_path(surface.api),
        };
        lines.push(forward_command(&opts.common.namespace, &fullname, &forward).display());
    }
    DryRunPlan { lines }
}

/// Run the forwards in the foreground until Ctrl-C (or SIGTERM), then stop
/// and reap every one. Returns an error, after stopping the rest, if a forward
/// dies before that.
pub async fn access(opts: AccessOpts) -> Result<()> {
    require_on_path("kubectl")?;
    let common = &opts.common;
    let fullname = release_fullname(&common.namespace, &common.release).await;
    let (ui_json, langfuse_json) = tokio::join!(
        fetch_service(common, &fullname, SURFACES[0].suffix),
        fetch_service(common, &fullname, SURFACES[1].suffix),
    );
    let mut forwards = Vec::new();
    let mut skipped = Vec::new();
    for (surface, (json, local)) in SURFACES
        .iter()
        .zip([(ui_json, opts.ui_port), (langfuse_json, opts.langfuse_port)])
    {
        match plan_forward(
            surface,
            &fullname.resource(surface.suffix),
            json.as_deref(),
            local,
        ) {
            Ok(forward) => forwards.push(forward),
            Err(reason) => skipped.push(reason.describe(surface.label)),
        }
    }
    if forwards.is_empty() {
        bail!(
            "no Curie service to reach in namespace {} for release {}: {}. Check `curie cluster status`; the release may not be installed, or the namespace or release name may differ.",
            common.namespace,
            common.release,
            skipped.join("; ")
        );
    }
    if let Some(port) = local_port_conflict(&forwards) {
        bail!(
            "the Console and Langfuse cannot both use local port {port}; pass --ui-port or --langfuse-port"
        );
    }

    let mut shutdown = shutdown_signal()?;
    let mut running: Vec<(Forward, tokio::process::Child)> = Vec::new();
    for forward in forwards {
        let command = forward_command(&common.namespace, &fullname, &forward);
        let started = tokio::select! {
            started = crate::message::start_port_forward(&command, forward.local, forward.label) => started,
            _ = shutdown.changed() => {
                stop_all(running).await;
                return Ok(());
            }
        };
        match started {
            Ok((child, _)) => running.push((forward, child)),
            Err(error) => {
                let failure = format!(
                    "could not forward the {} to localhost:{}: {error:#} Pass {} to use another local port.",
                    forward.label, forward.local, forward.port_flag
                );
                stop_all(running).await;
                bail!(failure);
            }
        }
    }

    crate::ui::ui().emit(&ClusterAccessOutput {
        forwards: running.iter().map(|(forward, _)| forward.clone()).collect(),
        skipped,
    });
    crate::ui::ui().note(
        "Forwarding on 127.0.0.1 only, for as long as this command runs. Press Ctrl-C to stop.",
    );

    let outcome = wait_for_end(&mut running, &mut shutdown).await;
    stop_all(running).await;
    outcome
}

/// Wait for a shutdown signal (Ok) or for any forward to exit on its own (Err).
async fn wait_for_end(
    running: &mut [(Forward, tokio::process::Child)],
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    if running.is_empty() {
        return Ok(());
    }
    let exited = async {
        let waits = running.iter_mut().map(|(forward, child)| {
            Box::pin(async move {
                let status = child.wait().await;
                (forward.label, status)
            })
        });
        futures_util::future::select_all(waits).await.0
    };
    tokio::select! {
        _ = shutdown.changed() => Ok(()),
        (label, status) = exited => {
            if tokio::time::timeout(SIGNAL_RACE_GRACE, shutdown.changed()).await.is_ok() {
                return Ok(());
            }
            let status = status
                .map(|status| status.to_string())
                .unwrap_or_else(|error| error.to_string());
            bail!("the {label} port-forward ended ({status}); stopping the other forwards")
        }
    }
}

/// Stop every forward and wait for it, so none outlives this command.
async fn stop_all(running: Vec<(Forward, tokio::process::Child)>) {
    for (forward, mut child) in running {
        if child.start_kill().is_err() {
            // Already exited; the wait below reaps it.
        }
        match tokio::time::timeout(STOP_GRACE, child.wait()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => crate::ui::ui().warn(&format!(
                "could not confirm the {} port-forward stopped: {error}",
                forward.label
            )),
            Err(_) => crate::ui::ui().warn(&format!(
                "the {} port-forward did not stop within {STOP_GRACE:?}",
                forward.label
            )),
        }
    }
}

/// A receiver that changes when this process gets SIGINT or SIGTERM. The
/// signal arrives on a helper thread that lives for the rest of the process.
#[cfg(unix)]
fn shutdown_signal() -> Result<tokio::sync::watch::Receiver<bool>> {
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ])
    .context("installing the Ctrl-C handler")?;
    std::thread::Builder::new()
        .name("curie-cluster-access-signal".to_string())
        .spawn(move || {
            if signals.forever().next().is_some() && sender.send(true).is_err() {
                // The command already ended.
            }
        })
        .context("starting the Ctrl-C handler")?;
    Ok(receiver)
}

#[cfg(not(unix))]
fn shutdown_signal() -> Result<tokio::sync::watch::Receiver<bool>> {
    bail!("cluster access needs a unix signal handler to stop its forwards cleanly")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::testsupport::{fullname, NODEPORT_SVC};

    const CLUSTERIP_UI: &str = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
    const CLUSTERIP_LANGFUSE: &str = r#"{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}"#;

    fn plan(surface: &Surface, json: Option<&str>, local: Option<u16>) -> Result<Forward, Skipped> {
        plan_forward(surface, "curie-svc", json, local)
    }

    #[test]
    fn a_clusterip_console_forwards_to_the_port_status_hints_name() {
        let forward = plan(&SURFACES[0], Some(CLUSTERIP_UI), None).unwrap();
        assert_eq!((forward.local, forward.remote), (18080, 80));
        assert_eq!(forward.url(), "http://localhost:18080/?api=1");
    }

    #[test]
    fn langfuse_forwards_to_its_own_port_with_no_path() {
        let forward = plan(&SURFACES[1], Some(CLUSTERIP_LANGFUSE), None).unwrap();
        assert_eq!((forward.local, forward.remote), (3000, 3000));
        assert_eq!(forward.url(), "http://localhost:3000");
    }

    #[test]
    fn a_nodeport_service_is_forwarded_too() {
        let forward = plan(&SURFACES[0], Some(NODEPORT_SVC), None).unwrap();
        assert_eq!((forward.local, forward.remote), (18080, 80));
    }

    #[test]
    fn a_flag_moves_the_local_port_only() {
        let forward = plan(&SURFACES[0], Some(CLUSTERIP_UI), Some(9090)).unwrap();
        assert_eq!((forward.local, forward.remote), (9090, 80));
        assert_eq!(forward.url(), "http://localhost:9090/?api=1");
    }

    #[test]
    fn a_service_that_is_missing_or_unreadable_is_skipped_with_its_name() {
        assert_eq!(
            plan(&SURFACES[1], None, None),
            Err(Skipped::NotFound("curie-svc".into()))
        );
        assert_eq!(
            plan(&SURFACES[1], Some("not json"), None),
            Err(Skipped::Unreadable("curie-svc".into()))
        );
        assert_eq!(
            plan(
                &SURFACES[1],
                Some(r#"{"spec":{"type":"ClusterIP","ports":[{}]}}"#),
                None
            ),
            Err(Skipped::Unreadable("curie-svc".into()))
        );
        assert_eq!(
            Skipped::NotFound("curie-langfuse-web".into()).describe("Langfuse"),
            "Langfuse: service curie-langfuse-web not found"
        );
    }

    /// The forward is the shared port-forward builder, bound to loopback.
    #[test]
    fn the_forward_command_binds_the_ipv4_loopback_only() {
        let forward = plan(&SURFACES[0], Some(CLUSTERIP_UI), None).unwrap();
        let command = forward_command("curie", &fullname(), &forward);
        assert_eq!(
            command.display(),
            "kubectl -n curie port-forward --address 127.0.0.1 svc/curie-ui 18080:80"
        );
        let langfuse = plan(&SURFACES[1], Some(CLUSTERIP_LANGFUSE), None).unwrap();
        assert_eq!(
            forward_command("curie", &fullname(), &langfuse).display(),
            "kubectl -n curie port-forward --address 127.0.0.1 svc/curie-langfuse-web 3000:3000"
        );
        assert!(!command.display().contains("0.0.0.0"));
    }

    #[test]
    fn two_forwards_may_not_share_a_local_port() {
        let ui = plan(&SURFACES[0], Some(CLUSTERIP_UI), Some(3000)).unwrap();
        let langfuse = plan(&SURFACES[1], Some(CLUSTERIP_LANGFUSE), None).unwrap();
        assert_eq!(local_port_conflict(&[ui, langfuse.clone()]), Some(3000));
        let ui = plan(&SURFACES[0], Some(CLUSTERIP_UI), None).unwrap();
        assert_eq!(local_port_conflict(&[ui, langfuse]), None);
    }

    #[test]
    fn the_dry_run_lists_the_reads_then_the_loopback_forwards() {
        let plan = access_plan(&AccessOpts {
            common: CommonOpts {
                namespace: "curie".into(),
                release: "curie".into(),
                dry_run: true,
            },
            ui_port: None,
            langfuse_port: Some(13000),
        });
        let text = plan.lines.join("\n");
        assert!(
            text.contains("kubectl get svc curie-ui -n curie -o json"),
            "{text}"
        );
        assert!(
            text.contains("kubectl get svc curie-langfuse-web -n curie -o json"),
            "{text}"
        );
        assert!(
            text.contains(
                "kubectl -n curie port-forward --address 127.0.0.1 svc/curie-ui 18080:80"
            ),
            "{text}"
        );
        assert!(text.contains("svc/curie-langfuse-web 13000:3000"), "{text}");
    }

    #[test]
    fn the_report_lists_each_forward_and_each_skipped_surface() {
        let output = ClusterAccessOutput {
            forwards: vec![plan(&SURFACES[0], Some(CLUSTERIP_UI), None).unwrap()],
            skipped: vec!["Langfuse: service curie-langfuse-web not found".into()],
        };
        let json = output.to_json();
        assert_eq!(json["forwards"][0]["name"], "Console");
        assert_eq!(json["forwards"][0]["url"], "http://localhost:18080/?api=1");
        assert_eq!(json["forwards"][0]["local_port"], 18080);
        assert_eq!(
            json["skipped"][0],
            "Langfuse: service curie-langfuse-web not found"
        );
    }
}
