//! `curie <local|cluster> remediation-qualification`: record a qualification
//! and run its verifier (AUTOMATED-REMEDIATION-22, -23).
//!
//! `record` writes a qualification record and `start-run` starts one verifier
//! run against a literal member of the action's allowed targets; both are
//! recorded under the ADR-0106 operator principal in
//! CURIE_APPROVAL_PRINCIPAL_TOKEN, which is never printed. `show-run` reads a
//! run and needs none. Every input check runs in the verb's `validate`, before
//! the dispatcher resolves the connection and before any request, the agent
//! lookup included.

use std::path::PathBuf;

use anyhow::Result;

use super::remediation_policy::{api_client, validated_hook, RemediationPolicyOpts};
use super::remediation_receipts::canonical_uuid;
use crate::api::{RemediationQualificationOut, RemediationVerifierRunOut};
use crate::api_requests::{
    RemediationQualificationWrite, RemediationVerifierRunStart, TargetLiteral,
};
use crate::exit::CliError;

const WORST_CASE_MAXIMUM: usize = 2000;
const ACTION_MAXIMUM: usize = 128;

// @spec AUTOMATED-REMEDIATION-22
/// The verb a `remediation-qualification` invocation runs.
pub enum RemediationQualificationVerb {
    Record {
        agent: String,
        qualification_id: String,
        hook: String,
        action: String,
        generation: String,
        evidence_file: PathBuf,
        worst_case: String,
    },
    StartRun {
        agent: String,
        qualification_id: String,
        hook: String,
        action: String,
        target: String,
    },
    ShowRun {
        agent: String,
        qualification_id: String,
        run_id: String,
    },
}

enum Validated {
    Record {
        qualification_id: String,
        body: RemediationQualificationWrite,
    },
    StartRun {
        qualification_id: String,
        body: RemediationVerifierRunStart,
    },
    ShowRun {
        qualification_id: String,
        run_id: String,
    },
}

// @spec AUTOMATED-REMEDIATION-22
/// A verb whose inputs passed [`RemediationQualificationVerb::validate`].
pub struct ValidatedQualification {
    agent: String,
    verb: Validated,
    principal: Option<String>,
}

fn usage(message: String, fix: &str) -> anyhow::Error {
    anyhow::Error::from(CliError::usage(message).with_fix(fix.to_string()))
}

fn validated_action(raw: &str) -> Result<String> {
    if raw.is_empty() || raw.len() > ACTION_MAXIMUM {
        return Err(usage(
            format!("--action must be 1 to {ACTION_MAXIMUM} characters, got {raw:?}"),
            "name an action the hook's policy declares",
        ));
    }
    Ok(raw.to_string())
}

/// A canonical decimal policy generation.
fn validated_generation(raw: &str) -> Result<String> {
    let canonical = (raw == "0" || (!raw.starts_with('0') && !raw.is_empty()))
        && raw.len() <= 19
        && raw.bytes().all(|b| b.is_ascii_digit())
        && raw.parse::<i64>().is_ok();
    if canonical {
        Ok(raw.to_string())
    } else {
        Err(usage(
            format!("--generation must be a canonical decimal generation, got {raw:?}"),
            "pass the policy generation the action was qualified under (`remediation-policy show`)",
        ))
    }
}

/// The evidence file: one JSON object, passed on unchanged.
fn validated_evidence(
    file: &std::path::Path,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let text = std::fs::read_to_string(file).map_err(|error| {
        usage(
            format!("cannot read the evidence file {}: {error}", file.display()),
            "pass --evidence-file the path of a JSON object of execution and run ids",
        )
    })?;
    match serde_json::from_str::<serde_json::Value>(&text) {
        Ok(serde_json::Value::Object(evidence)) => Ok(evidence),
        Ok(_) => Err(usage(
            format!("the evidence in {} must be a JSON object", file.display()),
            "pass --evidence-file a JSON object",
        )),
        Err(error) => Err(usage(
            format!("the evidence in {} is not JSON: {error}", file.display()),
            "pass --evidence-file a JSON object",
        )),
    }
}

fn validated_worst_case(raw: &str) -> Result<String> {
    if raw.trim().is_empty() || raw.chars().count() > WORST_CASE_MAXIMUM {
        return Err(usage(
            format!("--worst-case must be 1 to {WORST_CASE_MAXIMUM} characters and not blank"),
            "state the worst case of the action in a sentence",
        ));
    }
    Ok(raw.to_string())
}

/// A JSON number or boolean literal is sent as that type; anything else is a
/// string, so the target matches a member of the action's `target.allowed`.
fn target_literal(raw: &str) -> TargetLiteral {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(serde_json::Value::Bool(value)) => TargetLiteral::Bool(value),
        Ok(serde_json::Value::Number(number)) => match (number.as_i64(), number.as_f64()) {
            (Some(integer), _) => TargetLiteral::Integer(integer),
            (None, Some(float)) => TargetLiteral::Number(float),
            (None, None) => TargetLiteral::Text(raw.to_string()),
        },
        _ => TargetLiteral::Text(raw.to_string()),
    }
}

// @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23
/// The ADR-0106 operator principal a qualification write is recorded under.
/// Never echoed.
fn write_principal_token() -> Result<String> {
    super::approvals::approval_principal_token(|| {
        anyhow::Error::from(
            CliError::usage(
                "remediation qualification writes require CURIE_APPROVAL_PRINCIPAL_TOKEN, which \
                 names WHO recorded the qualification (the platform key authorizes the write \
                 but identifies nobody)",
            )
            .with_fix(
                "mint a reusable operator credential with `curie <local|cluster> approvals \
                 <AGENT> --mint-operator-principal <SUBJECT>`, export the one-time result as \
                 CURIE_APPROVAL_PRINCIPAL_TOKEN, and retry",
            ),
        )
    })
}

impl RemediationQualificationVerb {
    // @spec AUTOMATED-REMEDIATION-22
    /// Check the invocation's own inputs and, for a write, the principal. Pure
    /// apart from reading the evidence file: no network, no cluster.
    pub fn validate(self) -> Result<ValidatedQualification> {
        match self {
            RemediationQualificationVerb::Record {
                agent,
                qualification_id,
                hook,
                action,
                generation,
                evidence_file,
                worst_case,
            } => {
                let body = RemediationQualificationWrite {
                    action: validated_action(&action)?,
                    evidence: validated_evidence(&evidence_file)?,
                    generation: validated_generation(&generation)?,
                    hook: validated_hook(&hook)?,
                    worst_case: validated_worst_case(&worst_case)?,
                };
                let qualification_id = canonical_uuid(&qualification_id, "the qualification id")?;
                Ok(ValidatedQualification {
                    agent,
                    verb: Validated::Record {
                        qualification_id,
                        body,
                    },
                    principal: Some(write_principal_token()?),
                })
            }
            RemediationQualificationVerb::StartRun {
                agent,
                qualification_id,
                hook,
                action,
                target,
            } => {
                let body = RemediationVerifierRunStart {
                    action: validated_action(&action)?,
                    hook: validated_hook(&hook)?,
                    target: target_literal(&target),
                };
                let qualification_id = canonical_uuid(&qualification_id, "the qualification id")?;
                Ok(ValidatedQualification {
                    agent,
                    verb: Validated::StartRun {
                        qualification_id,
                        body,
                    },
                    principal: Some(write_principal_token()?),
                })
            }
            RemediationQualificationVerb::ShowRun {
                agent,
                qualification_id,
                run_id,
            } => Ok(ValidatedQualification {
                agent,
                verb: Validated::ShowRun {
                    qualification_id: canonical_uuid(&qualification_id, "the qualification id")?,
                    run_id: canonical_uuid(&run_id, "the run id")?,
                },
                principal: None,
            }),
        }
    }
}

// @spec AUTOMATED-REMEDIATION-22
/// What the API answered a `remediation-qualification` verb with.
pub struct RemediationQualificationOutput(Answer);

enum Answer {
    Qualification(Box<RemediationQualificationOut>),
    Run(Box<RemediationVerifierRunOut>),
}

impl crate::ui::CliOutput for RemediationQualificationOutput {
    fn to_json(&self) -> serde_json::Value {
        match &self.0 {
            Answer::Qualification(record) => serde_json::to_value(record),
            Answer::Run(run) => serde_json::to_value(run),
        }
        .unwrap_or(serde_json::Value::Null)
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let line = |key: &str, value: &str| ui.payload_plain(&format!("{key:<14} {value}"));
        let dash = |value: &Option<String>| value.clone().unwrap_or_else(|| "-".to_string());
        match &self.0 {
            Answer::Qualification(record) => {
                line("qualification", &record.id);
                line("agent", &record.agent_id);
                line("hook", &dash(&record.hook));
                line("action", &dash(&record.action));
                line("generation", &dash(&record.generation));
                line(
                    "connector",
                    &format!("{}.{}", record.connector, record.tool),
                );
                line("reversibility", &record.reversibility);
                line("recorded by", &record.recorded_by);
                line("created", &record.created_at);
            }
            Answer::Run(run) => {
                line("run", &run.id);
                line("qualification", &run.qualification_id);
                line("hook", &run.hook);
                line("action", &run.action);
                line("target", &run.target.to_string());
                line("generation", &run.generation);
                line("started by", &run.started_by);
                line("started", &run.started_at);
                line("outcome", &dash(&run.outcome));
                line("decided", &dash(&run.decided_at));
            }
        }
    }
}

// @spec AUTOMATED-REMEDIATION-22
/// `<tier> remediation-qualification <verb>`: send the request and print what
/// the API answered.
pub async fn remediation_qualification(
    opts: RemediationPolicyOpts,
    validated: ValidatedQualification,
) -> Result<RemediationQualificationOutput> {
    let ValidatedQualification {
        agent,
        verb,
        principal,
    } = validated;
    let client = api_client(&opts)?;
    let agent_id = client.find_agent(&agent).await?.id;
    let principal = principal.unwrap_or_default();
    Ok(RemediationQualificationOutput(match verb {
        Validated::Record {
            qualification_id,
            body,
        } => Answer::Qualification(Box::new(
            client
                .put_remediation_qualification(&agent_id, &qualification_id, &body, &principal)
                .await?,
        )),
        Validated::StartRun {
            qualification_id,
            body,
        } => Answer::Run(Box::new(
            client
                .start_remediation_verifier_run(&agent_id, &qualification_id, &body, &principal)
                .await?,
        )),
        Validated::ShowRun {
            qualification_id,
            run_id,
        } => Answer::Run(Box::new(
            client
                .get_remediation_verifier_run(&agent_id, &qualification_id, &run_id)
                .await?,
        )),
    }))
}
