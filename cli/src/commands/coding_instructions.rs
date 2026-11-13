use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::api::{
    CodingInstructionsApplication, CodingInstructionsStatus, CodingInstructionsWrite,
    OwnerApiClient,
};
use crate::exit::CliError;
use crate::ui::{CliOutput, DryRunPlan, Ui};

const MAX_INPUT_BYTES: u64 = 131_072;

pub enum CodingInstructionsVerb {
    Get,
    Set {
        file: PathBuf,
        expected_revision: i64,
    },
    Clear {
        expected_revision: i64,
    },
}

pub struct CodingInstructionsOpts {
    pub verb: CodingInstructionsVerb,
    pub session_file: PathBuf,
    pub api_url: Option<String>,
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

#[derive(Debug)]
pub enum CodingInstructionsOutput {
    Document(CodingInstructionsStatus),
    DryRun(DryRunPlan),
}

impl CliOutput for CodingInstructionsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Document(status) => serde_json::to_value(status).unwrap_or_default(),
            Self::DryRun(plan) => {
                let mut value = plan.to_json();
                value["application"] =
                    serde_json::to_value(CodingInstructionsApplication::Unavailable)
                        .unwrap_or_default();
                value
            }
        }
    }

    fn render(&self, ui: &Ui) {
        match self {
            Self::Document(status) => {
                ui.payload(&format!(
                    "coding instructions: revision {}, {} bytes, tenant {}",
                    status.revision,
                    status.byte_length,
                    if status.tenant_enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                ));
                ui.payload("application to factory runs: unavailable");
                if let Some(text) = &status.text {
                    ui.payload(&terminal_text(text));
                }
            }
            Self::DryRun(plan) => plan.render(ui),
        }
    }
}

fn terminal_text(text: &str) -> String {
    let mut escaped = String::new();
    for character in text.chars() {
        if character.is_control() && !matches!(character, '\n' | '\t') {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    escaped
}

fn instruction_file_error(message: impl Into<String>) -> anyhow::Error {
    let fix = "pass --file with a readable regular UTF-8 file; an empty file clears the document";
    CliError::usage(format!("{}; {fix}", message.into()))
        .with_fix(fix)
        .into()
}

fn read_instruction_file(path: &Path) -> Result<String> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(path)
        .map_err(|_| instruction_file_error("cannot open the instruction file"))?;
    let metadata = file
        .metadata()
        .map_err(|_| instruction_file_error("cannot inspect the instruction file"))?;
    if !metadata.file_type().is_file() {
        return Err(instruction_file_error(
            "the instruction file is not a regular file",
        ));
    }
    if metadata.len() > MAX_INPUT_BYTES {
        return Err(instruction_file_error(
            "the instruction file exceeds the 131072-byte transport limit",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_INPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| instruction_file_error("cannot read the instruction file"))?;
    if bytes.len() as u64 > MAX_INPUT_BYTES {
        return Err(instruction_file_error(
            "the instruction file exceeds the 131072-byte transport limit",
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| instruction_file_error("the instruction file is not valid UTF-8"))
}

fn dry_run_plan(opts: &CodingInstructionsOpts, api_url: &str) -> DryRunPlan {
    let mut lines = Vec::new();
    let base = if opts.api_url.is_none() {
        lines.push(format!(
            "open a loopback kubectl port-forward to release {} in namespace {}",
            opts.release, opts.namespace
        ));
        "http://127.0.0.1:<tunnel port>".to_string()
    } else {
        api_url.trim_end_matches('/').to_string()
    };
    lines.push(format!(
        "authenticate with the Console session cookie from {} (value not shown)",
        opts.session_file.display()
    ));
    let url = format!("{base}/coding-instructions");
    lines.push(match &opts.verb {
        CodingInstructionsVerb::Get => format!("GET {url}"),
        CodingInstructionsVerb::Set {
            file,
            expected_revision,
        } => format!(
            "PUT {url} expected_revision={expected_revision} text=<from {}, not shown>",
            file.display()
        ),
        CodingInstructionsVerb::Clear { expected_revision } => {
            format!("DELETE {url}?expected_revision={expected_revision}")
        }
    });
    lines.push("application to factory runs: unavailable".to_string());
    DryRunPlan { lines }
}

pub async fn coding_instructions(opts: CodingInstructionsOpts) -> Result<CodingInstructionsOutput> {
    let cookie = super::subscription::read_session_cookie(&opts.session_file)?;
    let write = match &opts.verb {
        CodingInstructionsVerb::Set {
            file,
            expected_revision,
        } => Some(CodingInstructionsWrite {
            text: read_instruction_file(file)?,
            expected_revision: *expected_revision,
        }),
        CodingInstructionsVerb::Get | CodingInstructionsVerb::Clear { .. } => None,
    };
    let (api_url, _tunnel) = super::resolve_cluster_api(
        opts.api_url.clone(),
        &opts.namespace,
        &opts.release,
        opts.dry_run,
    )
    .await?;
    if opts.dry_run {
        return Ok(CodingInstructionsOutput::DryRun(dry_run_plan(
            &opts, &api_url,
        )));
    }
    let client = OwnerApiClient::new(&api_url, cookie)?;
    let status = match opts.verb {
        CodingInstructionsVerb::Get => client.get_coding_instructions().await?,
        CodingInstructionsVerb::Set { .. } => {
            client
                .put_coding_instructions(&write.expect("set reads its instruction file"))
                .await?
        }
        CodingInstructionsVerb::Clear { expected_revision } => {
            client.delete_coding_instructions(expected_revision).await?
        }
    };
    Ok(CodingInstructionsOutput::Document(status))
}
