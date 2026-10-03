//! `curie init`, `skill check`, and `curie try`.

use super::*;

/// The versioned report emitted by `curie_runner.check`.
#[derive(Debug, Deserialize, Serialize)]
pub struct CheckReport {
    pub check: String,
    pub version: u64,
    pub plugin_dir: String,
    pub declared: Vec<DeclaredServer>,
    /// Opaque pass-through of the runner's registered-server list. Never read by
    /// the human render (only round-tripped through `--json`), so it is kept as
    /// raw JSON: it round-trips losslessly and can never fail `parse_check_report`
    /// on a future tool/server shape.
    pub registered: Vec<serde_json::Value>,
    pub matches: Vec<CheckMatch>,
    pub verdict: String,
    pub reasons: Vec<String>,
    pub hints: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeclaredServer {
    pub name: String,
    pub source: String,
    pub form: String,
    /// True when the server carries a credential (env/headers) the credential-free
    /// offline check never exercised. `#[serde(default)]` keeps older reports that
    /// predate the field parsing (they default to false).
    #[serde(default)]
    pub authed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckMatch {
    pub declared: String,
    pub registered: Option<String>,
    pub connected: bool,
    pub tool_count: u64,
}

/// Parse the frozen runner to CLI check report contract.
pub fn parse_check_report(stdout: &str) -> Result<CheckReport> {
    let report: CheckReport = serde_json::from_str(stdout)
        .context("runner check output is not valid JSON for the check report contract")?;
    if report.version != 1 {
        bail!(
            "runner check report contract version {} is unsupported; expected version 1",
            report.version
        );
    }
    Ok(report)
}

/// Map a runner check verdict to the CLI semantic exit contract.
pub fn check_outcome(report: &CheckReport) -> std::result::Result<(), crate::exit::CliError> {
    match report.verdict.as_str() {
        "green" => Ok(()),
        "red" => Err(crate::exit::CliError {
            message: "MCP load check reported red".into(),
            fix: None,
            class: crate::exit::ExitClass::Failure,
        }
        // A structurally bad bundle is `invalid_bundle` (the runner's `run_check`
        // rejects it at step 1), so every remaining red cause is a runtime one:
        // a declared server that never registered or failed to start, one that
        // registered zero tools, one that needs a credential the offline check
        // never forwards, or MCP init exceeding the deadline. The printed
        // `reason:` lines say which, so point at them rather than guess.
        .with_fix(
            "read the printed reason(s): fix the server's command/args, forward its credential with curie skill up --secret <NAME>, or raise --timeout if MCP init ran long",
        )),
        "invalid_bundle" => {
            // Bundle validation, not an MCP load failure. A reason that already
            // starts with the bundle headline is kept as the message.
            let joined = report.reasons.join("; ");
            let message = if report.reasons.is_empty() {
                "invalid plugin bundle".to_string()
            } else if joined.starts_with("invalid plugin bundle") {
                joined
            } else {
                format!("invalid plugin bundle: {joined}")
            };
            Err(crate::exit::CliError::usage(message).with_fix(
                "correct the invalid bundle declaration named in the error and run curie skill check again",
            ))
        }
        verdict => Err(crate::exit::CliError {
            message: format!("MCP load check reported unknown verdict '{verdict}'"),
            fix: None,
            class: crate::exit::ExitClass::Failure,
        }),
    }
}

/// Run the offline MCP load check for a plugin bundle.
pub async fn check(plugin_dir: PathBuf, image: String, timeout_s: u64) -> Result<()> {
    let requested_dir = plugin_dir.display().to_string();
    let plugin_dir = plugin_dir.canonicalize().map_err(|err| {
        crate::exit::CliError::usage(format!("plugin dir not found: {requested_dir}: {err}"))
    })?;
    read_manifest(&plugin_dir).map_err(|err| {
        crate::exit::CliError::usage(format!("plugin dir is not a usable bundle: {err}"))
    })?;

    let spec = CheckSpec {
        image,
        plugin_dir: plugin_dir.display().to_string(),
        timeout_s,
    };
    let (status, stdout, stderr) = docker::docker_capture(&spec.run_args()).await?;
    // A container that DID run and produced parseable JSON is data (a
    // green/red/invalid verdict) regardless of its exit code. Only when the
    // stdout is NOT a valid report is this a real docker failure -- surface the
    // captured stderr (e.g. "Cannot connect to the Docker daemon") so the true
    // cause is visible instead of being dropped. Stays a plain Failure (exit 1);
    // Transient/exit 3 is reserved for reqwest connect/timeout errors (#323).
    let report = parse_check_report(&stdout).map_err(|err| {
        anyhow::anyhow!(
            "runner check output violated the check report contract: {err}; \
             docker exited {status}; stdout: {stdout}; stderr: {stderr}"
        )
    })?;

    let ui = crate::ui::ui();
    ui.emit(&CheckOutput { report: &report });
    // The runner owns bundle validation. Only report a declared cron after it
    // confirms that the bundle is structurally valid, while preserving the MCP
    // verdict as the command's eventual outcome.
    if report.verdict != "invalid_bundle" {
        if let Ok(Some(warning)) = cron_trigger_warning_from_bundle(&plugin_dir) {
            ui.warn(&warning);
        }
    }
    check_outcome(&report).map_err(anyhow::Error::from)
}

/// Output of `skill check` (#474): the MCP-load report, structured under `--json`
/// and rendered line-by-line otherwise, routed through the one `Ui::emit` point.
/// Borrows the report so the caller can still pass it to `check_outcome`.
pub(super) struct CheckOutput<'a> {
    report: &'a CheckReport,
}

impl crate::ui::CliOutput for CheckOutput<'_> {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self.report).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let report = self.report;
        let mut lines = vec![format!("declared: {}", report.declared.len())];
        lines.extend(report.matches.iter().map(|entry| {
            format!(
                "match: {} -> {} (connected: {}, tools: {})",
                entry.declared,
                entry.registered.as_deref().unwrap_or("none"),
                entry.connected,
                entry.tool_count
            )
        }));
        lines.push(format!("verdict: {}", report.verdict));
        lines.extend(
            report
                .reasons
                .iter()
                .map(|reason| format!("reason: {reason}")),
        );
        lines.extend(report.hints.iter().map(|hint| format!("hint: {hint}")));
        ui.payload_plain(&lines.join("\n"));
    }
}

pub fn init(
    name: Option<String>,
    dir: Option<PathBuf>,
    from_spec: Option<PathBuf>,
    adopt: Option<PathBuf>,
) -> Result<()> {
    let ui = crate::ui::ui();

    // Spec-file path (ADR-0021 decision 5): fully non-interactive. The bundle
    // name comes from the spec, never a prompt.
    if let Some(spec_path) = from_spec {
        let body = std::fs::read_to_string(&spec_path)
            .with_context(|| format!("reading spec file {}", spec_path.display()))?;
        let spec = crate::spec::parse(&body)?;
        // A positional name is allowed only if it matches the spec's name; a
        // mismatch is an authoring error, not a silent override.
        if let Some(positional) = &name {
            if positional != &spec.name {
                bail!(
                    "positional name {:?} does not match the spec name {:?}; \
                     the bundle name comes from the spec -- omit the name or make them match",
                    positional,
                    spec.name
                );
            }
        }
        let dir = dir.unwrap_or_else(|| PathBuf::from(&spec.name));
        let created = scaffold_from_spec(&dir, &spec)?;
        report_scaffold(
            ui,
            spec.name.clone(),
            Some(spec_path.clone()),
            format!(
                "initialized plugin bundle '{}' in {} (from spec {})",
                spec.name,
                dir.display(),
                spec_path.display()
            ),
            created,
            &dir,
        );
        return Ok(());
    }

    // Adopt an existing directory (#745, ADR-0071): scaffold the plugin skeleton
    // INTO <dir> alongside whatever is already there, deriving the name from the
    // directory unless an explicit NAME overrides it. The logic port is the
    // operator's (docs/adopting-a-bundle.md); this only lays the skeleton.
    if let Some(adopt_dir) = adopt {
        if !adopt_dir.is_dir() {
            bail!(
                "--adopt {}: not a directory. Point it at the existing bundle to adopt.",
                adopt_dir.display()
            );
        }
        let name = match name {
            Some(name) => name,
            None => derive_plugin_name(&adopt_dir).ok_or_else(|| {
                anyhow::anyhow!(
                    "could not derive a kebab-case plugin name from {}; pass one explicitly: \
                     curie init <name> --adopt {}",
                    adopt_dir.display(),
                    adopt_dir.display()
                )
            })?,
        };
        let created = scaffold(&adopt_dir, &name)?;
        report_scaffold(
            ui,
            name.clone(),
            None,
            format!(
                "adopted {} as plugin bundle '{name}' -- scaffolded the skeleton alongside \
                 your existing files. Port your agent's logic into skills/{name}/SKILL.md and \
                 .mcp.json (see docs/adopting-a-bundle.md), then run `curie skill up`.",
                adopt_dir.display()
            ),
            created,
            &adopt_dir,
        );
        return Ok(());
    }

    let name = match name {
        Some(name) => name,
        None => bail!("provide a plugin NAME, --from-spec <path>, or --adopt <dir>"),
    };
    let dir = dir.unwrap_or_else(|| PathBuf::from(&name));
    let created = scaffold(&dir, &name)?;
    report_scaffold(
        ui,
        name.clone(),
        None,
        format!("initialized plugin bundle '{name}' in {}", dir.display()),
        created,
        &dir,
    );
    Ok(())
}

/// Report a freshly scaffolded bundle through the one success-path decision point
/// (`Ui::emit`, issue #485): under `--json` emit one structured `InitOutput`
/// object to stdout; otherwise render the success line, a `created` note per
/// written path, and the `Next:` hint on stderr (byte-identical to before).
/// Shared by both `init` branches so the only per-branch difference is the
/// success message text and whether a spec sourced the bundle.
pub(super) fn report_scaffold(
    ui: &crate::ui::Ui,
    name: String,
    from_spec: Option<PathBuf>,
    success_msg: String,
    created: Vec<PathBuf>,
    dir: &Path,
) {
    ui.emit(&InitOutput {
        name,
        dir: dir.to_path_buf(),
        from_spec,
        created,
        success_msg,
    });
}

/// The result of `curie init` (both the plain-name and `--from-spec` branches),
/// carried through `Ui::emit`. Under `--json` an agent gets the bundle name, the
/// directory, the spec source (null for the plain-name path), the list of created
/// paths, and the next-step command -- never empty stdout (issue #485). Owns its
/// data so `to_json`/`render` outlive the scaffold call.
pub struct InitOutput {
    pub name: String,
    pub dir: PathBuf,
    pub from_spec: Option<PathBuf>,
    pub created: Vec<PathBuf>,
    pub success_msg: String,
}

impl InitOutput {
    /// The copy-pasteable next-step command. The dir is shell-quoted (only when
    /// it carries a special char -- a kebab bundle name stays bare) so a path
    /// with a space yields a valid `cd`, not a broken two-token one. Shared by
    /// `to_json` and `render` so the machine and human forms never drift.
    fn next_command(&self) -> String {
        format!(
            "cd {} && curie skill up",
            crate::ops::shell_quote(&self.dir.display().to_string())
        )
    }
}

impl crate::ui::CliOutput for InitOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "initialized": true,
            "name": self.name,
            "dir": self.dir.display().to_string(),
            "from_spec": self.from_spec.as_ref().map(|p| p.display().to_string()),
            "created": self
                .created
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>(),
            "next": self.next_command(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.success(&self.success_msg);
        for path in &self.created {
            ui.note(&format!("created {}", path.display()));
        }
        ui.note(&format!("Next: {}", self.next_command()));
    }
}

/// Scaffold and drive the existing local skill path for one first reply.
pub async fn try_first_run(keep: bool, image: String) -> Result<SkillMessageOutput> {
    const DEMO_NAME: &str = "curie-demo";
    const DEMO_PROMPT: &str = "hello, are you there?";

    let ui = crate::ui::ui();
    let dir = if keep {
        PathBuf::from(DEMO_NAME)
    } else {
        std::env::temp_dir().join(format!("curie-try-{}", uuid::Uuid::new_v4()))
    };

    if keep {
        refuse_unowned_nonempty_dir(&dir)?;
    }

    if let Err(err) = scaffold(&dir, DEMO_NAME) {
        if !keep && dir.exists() {
            if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                ui.warn(&format!(
                    "could not remove incomplete demo at {}: {cleanup_err}",
                    dir.display()
                ));
            }
        }
        return Err(err);
    }
    ui.note(&format!("scaffolded demo at {}", dir.display()));

    let mut credential_name = None;
    let mut discovery_error = None;
    for name in MODEL_CREDENTIAL_ENV_NAMES {
        if env_credential_present(name) {
            credential_name = Some(name);
            break;
        }
        match crate::secrets::is_saved(name) {
            Ok(true) => {
                credential_name = Some(name);
                break;
            }
            Ok(false) => {}
            Err(err) => {
                discovery_error = Some(err);
                break;
            }
        }
    }
    if let Some(err) = discovery_error {
        if !keep {
            if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                ui.warn(&format!(
                    "could not remove temporary demo at {}: {cleanup_err}",
                    dir.display()
                ));
            }
        }
        return Err(err);
    }
    let fake_model = credential_name.is_none();
    if let Some(name) = credential_name {
        ui.note(&format!("using discovered model credential {name}"));
    } else {
        ui.note("no model credential found; using the scripted fake model");
    }

    let started = start(StartOpts {
        plugin_dir: dir.clone(),
        image,
        port: DEFAULT_PORT,
        name: docker::RUNNER_CONTAINER_LOCAL.to_string(),
        fake_model,
        network: None,
        otel_endpoint: None,
        budget: DEFAULT_BUDGET.to_string(),
        model: None,
        local_model: None,
        pull_model: false,
        secret: Vec::new(),
        env_file: None,
        replace: false,
    })
    .await;
    if let Err(err) = started {
        if !keep {
            if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                ui.warn(&format!(
                    "could not remove temporary demo at {}: {cleanup_err}",
                    dir.display()
                ));
            }
        }
        return Err(err);
    }

    let message = send(
        DEMO_PROMPT,
        crate::message::DEFAULT_USER,
        EventType::Message,
        Some(format!("http://localhost:{DEFAULT_PORT}")),
        true,
    )
    .await;
    let teardown = stop(None, &dir).await;

    if let Err(cleanup_err) = teardown {
        if let Err(message_err) = &message {
            ui.warn(&format!("demo message failed: {message_err}"));
        }
        let message = format!(
            "could not tear down the demo at {}: {cleanup_err}",
            dir.display()
        );
        let remedy = format!(
            "recover with: cd {} && curie skill down",
            crate::ops::shell_quote(&dir.display().to_string())
        );
        let payload = serde_json::json!({ "error": &message, "fix": &remedy });
        let failure = crate::exit::CliError::failure(message.clone())
            .with_fix(remedy.clone())
            .into();
        return Err(crate::exit::operator_context(
            crate::exit::with_json_payload(failure, payload),
            message,
            Some(remedy),
        ));
    }

    if keep {
        let next = if fake_model {
            "cd curie-demo && curie skill up --fake-model"
        } else {
            "cd curie-demo && curie skill up"
        };
        ui.note(&format!("kept ./curie-demo; next: {next}"));
    } else if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
        if let Err(message_err) = &message {
            ui.warn(&format!("demo message failed: {message_err}"));
        }
        return Err(crate::exit::CliError::failure(format!(
            "could not remove temporary demo at {}: {cleanup_err}",
            dir.display()
        ))
        .into());
    } else {
        ui.note(&format!("removed temporary demo at {}", dir.display()));
    }

    message
}
