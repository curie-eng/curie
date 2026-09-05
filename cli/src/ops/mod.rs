//! `curie cluster up | cluster upgrade | cluster status | cluster rollback | cluster down`: the operator
//! day-1 lifecycle, wrapping the Helm chart and `kubectl` the way linkerd or
//! cilium wrap theirs -- a deliberately thin CLI over the chart, which stays the
//! source of truth. Every verb shells out to the `helm`/`kubectl` binaries; the
//! CLI never re-derives what a values file already declares.
//!
//! Each verb builds its command lines as a pure function returning
//! [`OpsCommand`] vectors; the executor (or the `--dry-run` printer) consumes
//! them. That split keeps the argv construction unit-testable with no cluster
//! and gives one place to mask secrets before anything is printed.

mod command;
mod convergence;
mod providers;
mod up;
mod upgrade;
mod verbs;

pub use command::*;
pub use providers::*;
pub use up::*;
pub use upgrade::*;
pub use verbs::*;

/// Fixtures shared by more than one submodule's `mod tests`. They live
/// here, once, so the four test modules cannot drift apart on a default.
#[cfg(test)]
mod testsupport {
    use super::*;

    pub(super) fn common() -> CommonOpts {
        CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        }
    }

    /// The default release's resolved fullname, for the builders that now take
    /// one. `curie` contains the chart name, so this is byte-identical to the
    /// names these tests have always asserted -- see
    /// `chart_fullname_tests::the_default_release_is_a_byte_identical_no_op`.
    pub(super) fn fullname() -> ReleaseFullname {
        chart_fullname("curie")
    }

    // A fixture whose release differs from its namespace, so an assertion on the
    // ownership label VALUE unambiguously locks it to the release (not the ns).
    pub(super) fn common_distinct_release() -> CommonOpts {
        CommonOpts {
            namespace: "agent-ns".into(),
            release: "prod-release".into(),
            dry_run: false,
        }
    }

    /// Parse a `kubectl label namespace <ns> k=v [k=v ...] --overwrite` stamp
    /// into the label map it would actually set on that namespace.
    pub(super) fn parse_stamped_labels(
        cmd: &OpsCommand,
    ) -> std::collections::BTreeMap<String, String> {
        let line = cmd.display();
        let mut parts = line.split_whitespace();
        assert_eq!(parts.next(), Some("kubectl"), "{line}");
        assert_eq!(parts.next(), Some("label"), "{line}");
        assert_eq!(parts.next(), Some("namespace"), "{line}");
        parts.next().expect("the target namespace arg");
        parts
            .take_while(|tok| !tok.starts_with("--"))
            .map(|tok| {
                let (k, v) = tok.split_once('=').unwrap_or_else(|| {
                    panic!("every stamped label must be key=value, got {tok:?}: {line}")
                });
                (k.to_string(), v.to_string())
            })
            .collect()
    }

    /// Parse the `-l` selector out of a `kubectl delete namespace -l <sel>
    /// --ignore-not-found` sweep into the `key=value` terms it REQUIRES (a
    /// comma-joined kubectl selector is a conjunction: all terms must match).
    pub(super) fn parse_selector_terms(cmd: &OpsCommand) -> Vec<(String, String)> {
        let line = cmd.display();
        let toks: Vec<&str> = line.split_whitespace().collect();
        let at = toks.iter().position(|t| *t == "-l").expect("a -l selector");
        toks[at + 1]
            .split(',')
            .map(|term| {
                let (k, v) = term.split_once('=').unwrap_or_else(|| {
                    panic!("every selector term must be key=value, got {term:?}: {line}")
                });
                (k.to_string(), v.to_string())
            })
            .collect()
    }

    /// Whether a sweep selector's required terms are all satisfied by the labels
    /// a namespace actually carries, i.e. whether that sweep would delete it.
    pub(super) fn selector_matches(
        terms: &[(String, String)],
        labels: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        terms
            .iter()
            .all(|(k, v)| labels.get(k).map(String::as_str) == Some(v.as_str()))
    }

    // -----------------------------------------------------------------------
    // Observability twin (issue #460): the pure discovery core that both
    // `cluster status` and `cluster observability` build on. Only the kubectl
    // boundary is mocked -- by feeding the service JSON strings kubectl returns.
    // -----------------------------------------------------------------------

    /// The NodePort service fixture kubectl returns for an exposed service.
    pub(super) const NODEPORT_SVC: &str =
        r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;

    /// The ClusterIP service fixture kubectl returns for a `--no-expose` install.
    pub(super) const CLUSTERIP_SVC: &str =
        r#"{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}"#;

    /// The API-base row must NEVER name `--api-url`: `cluster observability`
    /// has no such flag (only --namespace/--release/--dry-run/--open), so the
    /// hint inherited from `cluster deploy`'s error vocabulary is dead here.
    pub(super) fn assert_no_api_url_hint(ep: &crate::observability::Endpoint) {
        let note = ep.note.as_deref().unwrap_or("");
        assert!(
            !note.contains("--api-url"),
            "`cluster observability` has no --api-url flag; dead hint in: {note}"
        );
    }

    // -----------------------------------------------------------------------
    // `api.githubToken` as a private durable cluster input (#1124)
    //
    // The API's OUTBOUND GitHub credential (git-flow bundle clone + eval commit
    // status, #1058/#1097/#1109). Every assertion below reads a user-visible
    // outcome -- what `display()` renders, what `argv()` carries after
    // materialization, what the `--dry-run --json` plan serializes to -- never an
    // internal `CmdArg` shape, so renaming any of the machinery breaks nothing.
    // -----------------------------------------------------------------------

    /// One sentinel everywhere, so a leak is unambiguous in any output form and
    /// its masked prefix (`mask_secret` keeps 8 chars) is `ghp-SENT***`.
    pub(super) const GH_SENTINEL: &str = "ghp-SENTINEL-1124-leak-canary";

    /// The masked form the operator SHOULD see: enough prefix to recognise the
    /// credential is applied, not enough to use it.
    pub(super) const GH_MASKED: &str = "api.githubToken=ghp-SENT***";

    /// A `cluster up` carrying nothing but the GitHub credential plan, so each
    /// assertion below reads exactly one variable.
    pub(super) fn up_with_github_token(plan: GithubTokenPlan) -> Vec<OpsCommand> {
        up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: plan,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        })
    }

    /// Every values file a materialized command hands helm, in argv order.
    ///
    /// Plural on purpose: a real sealed `cluster up` emits more than one
    /// `SecretValuesFile` (the model credential, the GitHub credential, and the
    /// generated/preserved chart secrets are three separate args), so a helper
    /// that read only the FIRST `-f` would silently assert against the model
    /// credential file while believing it was reading the token's.
    pub(super) fn secret_values_file_bodies(cmd: &OpsCommand) -> Vec<String> {
        let argv = cmd.argv();
        let bodies: Vec<String> = argv
            .iter()
            .enumerate()
            .filter(|(_, a)| a.as_str() == "-f")
            .map(|(i, _)| {
                std::fs::read_to_string(&argv[i + 1]).expect("reading the secret values file")
            })
            .collect();
        assert!(
            !bodies.is_empty(),
            "no -f values file in the materialized argv: {argv:?}"
        );
        bodies
    }
}
