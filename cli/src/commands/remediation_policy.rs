//! `curie <local|cluster> remediation-policy`: the operator surface over a
//! protected hook's remediation policy (AUTOMATED-REMEDIATION-3).
//!
//! `show` reads the current generation; `apply`, `arm`, `disarm` and `remove`
//! each write a new generation under compare-and-swap, with an idempotency
//! key, as the ADR-0106 operator principal from CURIE_APPROVAL_PRINCIPAL_TOKEN.
//! `apply` runs the mirrored document validator
//! ([`crate::remediation_policy`]) first, so a document the API would refuse
//! is refused here with the API's code and path before any request.
//!
//! Every input check runs in [`RemediationPolicyVerb::validate`], before the
//! dispatcher resolves the connection: at the cluster tier that resolution
//! reads Helm state and opens a port-forward, none of which a bad document or
//! a missing principal should trigger.

use std::path::PathBuf;

use anyhow::Result;

use crate::api::{ApiClient, RemediationPolicyGeneration};
use crate::api_requests::{RemediationPolicyMutation, RemediationPolicyWrite};
use crate::exit::CliError;

// @spec AUTOMATED-REMEDIATION-3
/// The CAS pair every write carries: the generation the operator read, and
/// the operation id that makes a retry the same intent.
pub struct PolicyWriteArgs {
    pub expected_generation: String,
    pub operation_id: Option<String>,
}

// @spec AUTOMATED-REMEDIATION-3
/// The verb a `remediation-policy` invocation runs, as the dispatcher hands it
/// over.
pub enum RemediationPolicyVerb {
    Show {
        agent: String,
        hook: String,
    },
    Apply {
        agent: String,
        hook: String,
        file: PathBuf,
        write: PolicyWriteArgs,
    },
    Arm {
        agent: String,
        hook: String,
        write: PolicyWriteArgs,
    },
    Disarm {
        agent: String,
        hook: String,
        write: PolicyWriteArgs,
    },
    Remove {
        agent: String,
        hook: String,
        write: PolicyWriteArgs,
    },
}

// @spec AUTOMATED-REMEDIATION-3
/// The connection a `remediation-policy` verb uses, already resolved for its
/// tier.
pub struct RemediationPolicyOpts {
    pub api_url: String,
    pub api_key: String,
    /// "local" or "cluster".
    pub tier: &'static str,
}

// @spec AUTOMATED-REMEDIATION-3
/// Output of `<tier> remediation-policy <verb>`: the committed generation the
/// API answered with, passed through as one JSON object under `--json`. A
/// write adds the `operation_id` it sent, so the operator can replay it.
pub struct RemediationPolicyOutput {
    pub verb: &'static str,
    pub policy: RemediationPolicyGeneration,
    /// The idempotency key a write carried (passed or minted); `None` for `show`.
    pub operation_id: Option<String>,
}

impl crate::ui::CliOutput for RemediationPolicyOutput {
    fn to_json(&self) -> serde_json::Value {
        let mut value = serde_json::to_value(&self.policy).unwrap_or(serde_json::Value::Null);
        if let (Some(id), Some(object)) = (&self.operation_id, value.as_object_mut()) {
            object.insert("operation_id".to_string(), serde_json::json!(id));
        }
        value
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let line = |key: &str, value: &str| ui.payload_plain(&format!("{key:<12} {value}"));
        let policy = &self.policy;
        line("agent", &policy.agent_id);
        line("hook", &policy.hook);
        line("generation", &policy.generation);
        line("active", if policy.active { "yes" } else { "no" });
        line("armed", if policy.armed { "yes" } else { "no" });
        line("bound by", &policy.bound_by);
        line("updated", &policy.updated_at);
        if let Some(id) = &self.operation_id {
            line("operation", id);
        }
        if self.verb == "show" && policy.active {
            let actions = policy.policy["actions"]
                .as_array()
                .map(|actions| {
                    actions
                        .iter()
                        .filter_map(|action| action["name"].as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            line("route", policy.policy["route"].as_str().unwrap_or_default());
            line("actions", &actions);
        }
    }
}

/// `[a-z0-9][a-z0-9._-]{0,62}`, the API's hook name.
fn validated_hook(raw: &str) -> Result<String> {
    let bytes = raw.as_bytes();
    let ok = !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes[1..].iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        });
    if ok {
        Ok(raw.to_string())
    } else {
        Err(crate::exit::usage(format!(
            "hook must match [a-z0-9][a-z0-9._-]{{0,62}}, got {raw:?}"
        )))
    }
}

/// A canonical decimal generation (`0` binds a hook with no policy), at most
/// 2^63 - 1, as the API reads it.
fn validated_generation(raw: &str) -> Result<String> {
    let canonical = (raw == "0" || (!raw.starts_with('0') && !raw.is_empty()))
        && raw.len() <= 19
        && raw.bytes().all(|b| b.is_ascii_digit())
        && raw.parse::<i64>().is_ok();
    if canonical {
        Ok(raw.to_string())
    } else {
        Err(anyhow::Error::from(
            CliError::usage(format!(
                "--expected-generation must be a canonical decimal generation, got {raw:?}"
            ))
            .with_fix(
                "pass the generation `remediation-policy show` prints, or 0 to bind a hook \
                 with no policy",
            ),
        ))
    }
}

/// A canonical (lowercase, hyphenated) UUID, or a freshly minted one.
fn validated_operation_id(raw: Option<String>) -> Result<String> {
    match raw {
        None => Ok(uuid::Uuid::new_v4().to_string()),
        Some(raw) => match uuid::Uuid::parse_str(&raw) {
            Ok(parsed) if parsed.hyphenated().to_string() == raw => Ok(raw),
            _ => Err(crate::exit::usage(format!(
                "--operation-id must be a lowercase hyphenated UUID, got {raw:?}"
            ))),
        },
    }
}

fn validated_write(write: PolicyWriteArgs) -> Result<RemediationPolicyMutation> {
    Ok(RemediationPolicyMutation {
        expected_generation: validated_generation(&write.expected_generation)?,
        operation_id: validated_operation_id(write.operation_id)?,
    })
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
/// Read and validate the policy file with the API's own rules. An unreadable
/// file and a refused document are both usage errors (exit 2).
fn validated_document(
    file: &std::path::Path,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let text = std::fs::read_to_string(file).map_err(|error| {
        anyhow::Error::from(
            CliError::usage(format!(
                "cannot read the policy file {}: {error}",
                file.display()
            ))
            .with_fix("pass --file the path of a JSON policy document"),
        )
    })?;
    let refused = |refusal: crate::remediation_policy::PolicyRefusal| {
        anyhow::Error::from(
            CliError::usage(format!(
                "the policy document in {} is refused: {refusal}",
                file.display()
            ))
            .with_fix(
                "correct the document at the named path; the platform refuses it with the \
                 same code",
            ),
        )
    };
    let document = crate::remediation_policy::parse_policy_text(&text).map_err(refused)?;
    crate::remediation_policy::validate_policy_document(&document).map_err(refused)?;
    match document {
        serde_json::Value::Object(document) => Ok(document),
        // The validator refuses a non-object document, so this is unreachable.
        _ => Err(crate::exit::usage(
            "the policy document must be a JSON object",
        )),
    }
}

// @spec AUTOMATED-REMEDIATION-3
/// The ADR-0106 operator principal every policy write is recorded under.
/// Never echoed.
fn write_principal_token() -> Result<String> {
    super::approvals::approval_principal_token(|| {
        anyhow::Error::from(
            CliError::usage(
                "remediation policy writes require CURIE_APPROVAL_PRINCIPAL_TOKEN, which names \
                 WHO bound the policy (the platform key authorizes the write but identifies \
                 nobody)",
            )
            .with_fix(
                "mint a reusable operator credential with `curie <local|cluster> approvals \
                 <AGENT> --mint-operator-principal <SUBJECT>`, export the one-time result as \
                 CURIE_APPROVAL_PRINCIPAL_TOKEN, and retry",
            ),
        )
    })
}

enum Validated {
    Show,
    Apply { body: RemediationPolicyWrite },
    Arm(RemediationPolicyMutation),
    Disarm(RemediationPolicyMutation),
    Remove(RemediationPolicyMutation),
}

// @spec AUTOMATED-REMEDIATION-3
/// A `remediation-policy` verb whose inputs passed
/// [`RemediationPolicyVerb::validate`]. Only this type reaches
/// [`remediation_policy`], so no request is made for a malformed invocation.
pub struct ValidatedRemediationPolicy {
    agent: String,
    hook: String,
    verb: Validated,
    principal: Option<String>,
}

impl RemediationPolicyVerb {
    // @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
    /// Check the invocation's own inputs: the hook name, the CAS pair, the
    /// policy document and, for a write, the principal. Pure apart from
    /// reading the policy file: no network, no cluster, no key discovery.
    pub fn validate(self) -> Result<ValidatedRemediationPolicy> {
        let (agent, hook, verb) = match self {
            RemediationPolicyVerb::Show { agent, hook } => (agent, hook, Validated::Show),
            RemediationPolicyVerb::Apply {
                agent,
                hook,
                file,
                write,
            } => {
                let cas = validated_write(write)?;
                let policy = validated_document(&file)?;
                (
                    agent,
                    hook,
                    Validated::Apply {
                        body: RemediationPolicyWrite {
                            expected_generation: cas.expected_generation,
                            operation_id: cas.operation_id,
                            policy,
                        },
                    },
                )
            }
            RemediationPolicyVerb::Arm { agent, hook, write } => {
                (agent, hook, Validated::Arm(validated_write(write)?))
            }
            RemediationPolicyVerb::Disarm { agent, hook, write } => {
                (agent, hook, Validated::Disarm(validated_write(write)?))
            }
            RemediationPolicyVerb::Remove { agent, hook, write } => {
                (agent, hook, Validated::Remove(validated_write(write)?))
            }
        };
        let hook = validated_hook(&hook)?;
        let principal = match verb {
            Validated::Show => None,
            _ => Some(write_principal_token()?),
        };
        Ok(ValidatedRemediationPolicy {
            agent,
            hook,
            verb,
            principal,
        })
    }
}

// @spec AUTOMATED-REMEDIATION-3
/// `<tier> remediation-policy <verb>`: read the hook's current generation, or
/// write a new one and print it.
pub async fn remediation_policy(
    opts: RemediationPolicyOpts,
    validated: ValidatedRemediationPolicy,
) -> Result<RemediationPolicyOutput> {
    let ValidatedRemediationPolicy {
        agent,
        hook,
        verb,
        principal,
    } = validated;
    let operation_id = match &verb {
        Validated::Show => None,
        Validated::Apply { body } => Some(body.operation_id.clone()),
        Validated::Arm(cas) | Validated::Disarm(cas) | Validated::Remove(cas) => {
            Some(cas.operation_id.clone())
        }
    };
    match send(opts, agent, hook, verb, principal).await {
        Ok((verb, policy)) => Ok(RemediationPolicyOutput {
            verb,
            policy,
            operation_id,
        }),
        Err(error) => Err(match operation_id {
            Some(id) => with_operation_id(error, &id),
            None => error,
        }),
    }
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
/// A failed write names the operation id it carried, keeping its exit class.
/// A transient failure may have committed before its answer was lost, so its
/// fix says to rerun with that id: the API replays a committed write for the
/// same id instead of refusing the rerun as stale.
fn with_operation_id(error: anyhow::Error, id: &str) -> anyhow::Error {
    let (class, fix) = crate::exit::classify(&error);
    let fix = if class == crate::exit::ExitClass::Transient {
        Some(format!(
            "rerun the same command with --operation-id {id}: if the write committed before \
             the failure, the platform replays it rather than refusing it as stale"
        ))
    } else {
        fix
    };
    anyhow::Error::from(CliError {
        message: format!("{error:#} (operation id {id})"),
        fix,
        class,
    })
}

/// Resolve the agent and send the verb's request.
async fn send(
    opts: RemediationPolicyOpts,
    agent: String,
    hook: String,
    verb: Validated,
    principal: Option<String>,
) -> Result<(&'static str, RemediationPolicyGeneration)> {
    // A cluster tunnel is loopback too; its key is already the release's, so
    // it must not trigger local key discovery.
    let client = if opts.tier == "cluster" {
        ApiClient::with_resolved_key(&opts.api_url, &opts.api_key)?
    } else {
        ApiClient::new(&opts.api_url, &opts.api_key)?
    };
    let agent_id = client.find_agent(&agent).await?.id;
    let principal = principal.unwrap_or_default();
    let (verb, policy) = match verb {
        Validated::Show => (
            "show",
            client.get_remediation_policy(&agent_id, &hook).await?,
        ),
        Validated::Apply { body } => (
            "apply",
            client
                .put_remediation_policy(&agent_id, &hook, &body, &principal)
                .await?,
        ),
        Validated::Arm(cas) => (
            "arm",
            client
                .arm_remediation_policy(&agent_id, &hook, &cas, &principal)
                .await?,
        ),
        Validated::Disarm(cas) => (
            "disarm",
            client
                .disarm_remediation_policy(&agent_id, &hook, &cas, &principal)
                .await?,
        ),
        Validated::Remove(cas) => (
            "remove",
            client
                .remove_remediation_policy(&agent_id, &hook, &cas, &principal)
                .await?,
        ),
    };
    Ok((verb, policy))
}
