//! `curie cluster subscription`: a person's own personal Claude enrollment
//! (#5377, ADR 0268).
//!
//! The only credential is the caller's Console session cookie, read from a
//! private file. The release's platform key is never discovered, read or sent:
//! the API is reached through [`super::resolve_cluster_api`], which has no key
//! path. The grant token is read from a second private file, validated against
//! the shared token rule, and leaves this process only inside the PUT body.
//! Neither value reaches argv, the environment, stdout, stderr, `--debug`, an
//! error or a dry-run plan, and nothing here reads stdin.

use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;

use crate::api::{
    CredentialShapeError, EnrollmentIntent, EnrollmentState, GrantToken, OwnerApiClient,
    PersonalEnrollmentStatus, PersonalEnrollmentWrite, SessionCookie, PERSONAL_TOKEN_MAX_LEN,
    SESSION_COOKIE_MAX_LEN,
};
use crate::exit::CliError;
use crate::ui::{CliOutput, DryRunPlan, Ui};

/// The owner enrollment route every verb calls.
const ENROLLMENT_PATH: &str = "/personal-claude/enrollment";

/// One `curie cluster subscription` verb.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionVerb {
    Status,
    Enroll,
    Replace,
    Reconnect,
    Disconnect,
}

impl SubscriptionVerb {
    fn intent(self) -> Option<EnrollmentIntent> {
        match self {
            Self::Enroll => Some(EnrollmentIntent::Enroll),
            Self::Replace => Some(EnrollmentIntent::Replace),
            Self::Reconnect => Some(EnrollmentIntent::Reconnect),
            Self::Status | Self::Disconnect => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Enroll => "enroll",
            Self::Replace => "replace",
            Self::Reconnect => "reconnect",
            Self::Disconnect => "disconnect",
        }
    }

    fn past_tense(self) -> &'static str {
        match self {
            Self::Status => "read",
            Self::Enroll => "enrolled",
            Self::Replace => "replaced",
            Self::Reconnect => "reconnected",
            Self::Disconnect => "disconnected",
        }
    }
}

/// A parsed `curie cluster subscription` invocation.
pub struct SubscriptionOpts {
    pub verb: SubscriptionVerb,
    pub session_file: PathBuf,
    /// Required for enroll, replace and reconnect; unused otherwise.
    pub token_file: Option<PathBuf>,
    /// The CAS version for replace, reconnect and disconnect. `None` reads the
    /// current version first.
    pub expected_version: Option<i64>,
    pub api_url: Option<String>,
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

/// The `--json` result of `curie cluster subscription`
/// (`cli/schema/subscription.schema.json`).
#[derive(Debug)]
pub enum SubscriptionOutput {
    /// `status`: the enrollment as the API reports it.
    Status(PersonalEnrollmentStatus),
    /// A write verb's result, with the version it wrote at.
    Changed {
        action: SubscriptionVerb,
        previous_version: Option<i64>,
        enrollment: PersonalEnrollmentStatus,
    },
    DryRun(DryRunPlan),
}

/// The JSON of [`SubscriptionOutput::Changed`]. Serialized wholesale so no
/// field of the API answer can be dropped by a hand projection.
#[derive(Serialize)]
struct ChangedJson<'a> {
    action: SubscriptionVerb,
    changed: bool,
    previous_version: Option<i64>,
    #[serde(flatten)]
    enrollment: &'a PersonalEnrollmentStatus,
}

fn changed(previous_version: Option<i64>, enrollment: &PersonalEnrollmentStatus) -> bool {
    previous_version.is_none() || enrollment.version != previous_version
}

impl CliOutput for SubscriptionOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::Status(status) => serde_json::to_value(status).unwrap_or_default(),
            Self::Changed {
                action,
                previous_version,
                enrollment,
            } => serde_json::to_value(ChangedJson {
                action: *action,
                changed: changed(*previous_version, enrollment),
                previous_version: *previous_version,
                enrollment,
            })
            .unwrap_or_default(),
            Self::DryRun(plan) => plan.to_json(),
        }
    }

    fn render(&self, ui: &Ui) {
        match self {
            Self::Status(status) => {
                ui.payload(&describe(status));
                if status.state == EnrollmentState::Absent {
                    ui.note("run `curie cluster subscription enroll` to enroll a grant");
                }
                render_teardown(ui, status);
            }
            Self::Changed {
                action,
                previous_version,
                enrollment,
            } => {
                if changed(*previous_version, enrollment) {
                    ui.payload(&format!(
                        "{}: {}",
                        action.past_tense(),
                        describe(enrollment)
                    ));
                } else {
                    ui.payload(&format!(
                        "already {} at version {}; nothing changed",
                        action.past_tense(),
                        previous_version.unwrap_or_default()
                    ));
                }
                render_teardown(ui, enrollment);
            }
            Self::DryRun(plan) => plan.render(ui),
        }
    }
}

fn describe(status: &PersonalEnrollmentStatus) -> String {
    let state = match status.state {
        EnrollmentState::Absent => return "personal Claude enrollment: absent".to_string(),
        EnrollmentState::Active => "active",
        EnrollmentState::Inactive => "inactive",
    };
    let number =
        |value: Option<i64>| value.map_or_else(|| "unknown".to_string(), |v| v.to_string());
    format!(
        "personal Claude enrollment: {state}, generation {}, version {}",
        number(status.generation),
        number(status.version)
    )
}

fn render_teardown(ui: &Ui, status: &PersonalEnrollmentStatus) {
    let teardown = &status.teardown;
    if teardown.pending_runtimes > 0 || teardown.pending_custody_cleanup > 0 {
        ui.payload(&format!(
            "pending teardown: {} runtime(s) still stopping, {} custody cleanup(s)",
            teardown.pending_runtimes, teardown.pending_custody_cleanup
        ));
    }
}

/// Read a private credential file: open without blocking, `fstat` the open
/// handle, refuse anything but a regular file that only its owner can read or
/// write, then read at most `max_len` bytes plus one trailing newline and trim
/// exactly that newline (`\n` or `\r\n`). Every refusal is a usage error that
/// names the file and never its content.
fn read_private_file(path: &Path, what: &str, max_len: usize) -> Result<Vec<u8>> {
    let shown = path.display();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(path)
        .map_err(|error| {
            usage_with_fix(
                format!("cannot open the {what} {shown}: {error}"),
                format!("pass --{}-file with a readable file", file_flag(what)),
            )
        })?;
    let metadata = file
        .metadata()
        .map_err(|error| CliError::usage(format!("cannot inspect the {what} {shown}: {error}")))?;
    if !metadata.file_type().is_file() {
        return Err(usage_with_fix(
            format!("the {what} {shown} is not a regular file"),
            format!("pass --{}-file with a regular file", file_flag(what)),
        ));
    }
    let mode = metadata.mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(usage_with_fix(
            format!("the {what} {shown} is accessible to group or others (mode {mode:04o})"),
            format!("run `chmod 600 {shown}`, then retry"),
        ));
    }
    let limit = max_len + 2;
    let mut content = Vec::with_capacity(limit.min(8192));
    file.take(limit as u64 + 1)
        .read_to_end(&mut content)
        .map_err(|error| CliError::usage(format!("cannot read the {what} {shown}: {error}")))?;
    if content.len() > limit {
        return Err(refused(what, path, CredentialShapeError::TooLong));
    }
    if content.ends_with(b"\r\n") {
        content.truncate(content.len() - 2);
    } else if content.ends_with(b"\n") {
        content.truncate(content.len() - 1);
    }
    if content.is_empty() {
        return Err(refused(what, path, CredentialShapeError::Empty));
    }
    Ok(content)
}

fn file_flag(what: &str) -> &'static str {
    if what == "token file" {
        "token"
    } else {
        "session"
    }
}

fn refused(what: &str, path: &Path, reason: CredentialShapeError) -> anyhow::Error {
    let fix = if what == "token file" {
        "write only the token printed by `claude setup-token` to the file, with mode 0600"
    } else {
        "write only the __Host-curie_console_session cookie value to the file, with mode 0600"
    };
    usage_with_fix(
        format!("the {what} {} is refused: {reason}", path.display()),
        fix.to_string(),
    )
}

/// A usage error whose message also carries the fix, since the human error
/// path prints only the message.
fn usage_with_fix(message: String, fix: String) -> anyhow::Error {
    CliError::usage(format!("{message}; {fix}"))
        .with_fix(fix)
        .into()
}

pub(super) fn read_session_cookie(path: &Path) -> Result<SessionCookie> {
    let content = read_private_file(path, "session file", SESSION_COOKIE_MAX_LEN)?;
    SessionCookie::parse(&content).map_err(|reason| refused("session file", path, reason))
}

fn read_grant_token(path: &Path) -> Result<GrantToken> {
    let content = read_private_file(path, "token file", PERSONAL_TOKEN_MAX_LEN)?;
    GrantToken::parse(&content).map_err(|reason| refused("token file", path, reason))
}

/// The offline plan: what a real run would send, with every credential named
/// only by the file it comes from.
fn dry_run_plan(opts: &SubscriptionOpts, api_url: &str) -> DryRunPlan {
    let mut lines = Vec::new();
    let base = if opts.api_url.is_none() {
        lines.push(format!(
            "open a loopback kubectl port-forward to the API of release {} in namespace {} \
             (no platform key is read)",
            opts.release, opts.namespace
        ));
        "http://127.0.0.1:<tunnel port>".to_string()
    } else {
        api_url.trim_end_matches('/').to_string()
    };
    let url = format!("{base}{ENROLLMENT_PATH}");
    lines.push(format!(
        "authenticate with the Console session cookie from {} (value not shown)",
        opts.session_file.display()
    ));
    let version = match (opts.verb, opts.expected_version) {
        (SubscriptionVerb::Status | SubscriptionVerb::Enroll, _) => String::new(),
        (_, Some(version)) => version.to_string(),
        (_, None) => {
            lines.push(format!(
                "GET {url} to read the current version (no --expected-version given)"
            ));
            "<version from that read>".to_string()
        }
    };
    let token = opts
        .token_file
        .as_deref()
        .map(|path| format!("<from {}, not shown>", path.display()))
        .unwrap_or_default();
    lines.push(match opts.verb {
        SubscriptionVerb::Status => format!("GET {url}"),
        SubscriptionVerb::Enroll => {
            format!("PUT {url} intent=enroll expected_version=null token={token}")
        }
        SubscriptionVerb::Replace | SubscriptionVerb::Reconnect => format!(
            "PUT {url} intent={} expected_version={version} token={token}",
            opts.verb.name()
        ),
        SubscriptionVerb::Disconnect => format!("DELETE {url}?expected_version={version}"),
    });
    DryRunPlan { lines }
}

/// The version a replace, reconnect or disconnect writes at: the one given, or
/// the one a fresh status read reports.
async fn write_version(
    client: &OwnerApiClient,
    verb: SubscriptionVerb,
    expected_version: Option<i64>,
) -> Result<i64> {
    if let Some(version) = expected_version {
        return Ok(version);
    }
    let status = client.get_personal_enrollment().await?;
    match (status.state, status.version) {
        (EnrollmentState::Absent, _) | (_, None) => Err(CliError::failure(format!(
            "subscription {} failed: no personal Claude enrollment exists for this person; \
             run `curie cluster subscription enroll` first",
            verb.name()
        ))
        .with_fix("run `curie cluster subscription enroll` first")
        .into()),
        (_, Some(version)) => Ok(version),
    }
}

/// `curie cluster subscription <verb>`.
pub async fn subscription(opts: SubscriptionOpts) -> Result<SubscriptionOutput> {
    let cookie = read_session_cookie(&opts.session_file)?;
    let token = match (opts.verb.intent(), opts.token_file.as_deref()) {
        (Some(_), Some(path)) => Some(read_grant_token(path)?),
        (Some(_), None) => {
            return Err(crate::exit::usage(format!(
                "subscription {} requires --token-file",
                opts.verb.name()
            )))
        }
        (None, _) => None,
    };
    let (api_url, _tunnel) = super::resolve_cluster_api(
        opts.api_url.clone(),
        &opts.namespace,
        &opts.release,
        opts.dry_run,
    )
    .await?;
    if opts.dry_run {
        return Ok(SubscriptionOutput::DryRun(dry_run_plan(&opts, &api_url)));
    }
    let client = OwnerApiClient::new(&api_url, cookie)?;
    let verb = opts.verb;
    match (verb.intent(), token) {
        (None, _) if verb == SubscriptionVerb::Status => Ok(SubscriptionOutput::Status(
            client.get_personal_enrollment().await?,
        )),
        (None, _) => {
            let version = write_version(&client, verb, opts.expected_version).await?;
            Ok(SubscriptionOutput::Changed {
                action: verb,
                previous_version: Some(version),
                enrollment: client.delete_personal_enrollment(version).await?,
            })
        }
        (Some(intent), Some(token)) => {
            let previous_version = match intent {
                EnrollmentIntent::Enroll => None,
                EnrollmentIntent::Replace | EnrollmentIntent::Reconnect => {
                    Some(write_version(&client, verb, opts.expected_version).await?)
                }
            };
            let enrollment = client
                .put_personal_enrollment(&PersonalEnrollmentWrite {
                    intent,
                    expected_version: previous_version,
                    token,
                })
                .await?;
            Ok(SubscriptionOutput::Changed {
                action: verb,
                previous_version,
                enrollment,
            })
        }
        (Some(_), None) => Err(crate::exit::usage(format!(
            "subscription {} requires --token-file",
            verb.name()
        ))),
    }
}
