//! Declared application schema ranges for `curie cluster rollback` (#2296).
//!
//! v0.8.x API images run `alembic upgrade head` at startup and refuse a live
//! database revision they do not know. Helm `deployed`/`superseded` is not
//! enough: a status-eligible older application can still be unable to start.
//! This catalog is the CLI-side declaration of each released application's
//! schema range. The v0.9.0 replacement (#2300) ships the window inside the
//! API image; this module is the fail-closed repair for already-released
//! v0.8.x images that do not.

use std::sync::OnceLock;

use regex::Regex;
use serde::Deserialize;

const CATALOG_JSON: &str = include_str!("application_schema_windows.json");

#[derive(Debug, Deserialize)]
struct Catalog {
    revisions: Vec<String>,
    candidate: Window,
    windows: std::collections::BTreeMap<String, Window>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Window {
    pub schema_min: String,
    pub schema_head: String,
    #[serde(default)]
    pub artifact_identity_ambiguous: bool,
}

fn catalog() -> &'static Catalog {
    static CATALOG: OnceLock<Catalog> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(CATALOG_JSON).expect("application_schema_windows.json is valid")
    })
}

/// Strip an optional `v` prefix so Helm `app_version` and catalog keys match.
pub fn normalize_app_version(raw: &str) -> String {
    raw.trim().trim_start_matches('v').trim().to_string()
}

/// Chart field `curie-0.8.4` -> `0.8.4`. `None` when the field is not that shape.
pub fn version_from_chart(chart: &str) -> Option<String> {
    let rest = chart.trim().strip_prefix("curie-")?;
    let first = rest.chars().next()?;
    if first.is_ascii_digit() {
        Some(normalize_app_version(rest))
    } else {
        None
    }
}

pub fn window_for(app_version: &str) -> Option<Window> {
    catalog()
        .windows
        .get(&normalize_app_version(app_version))
        .cloned()
}

/// The schema window of this source tree before it is published as a version.
pub fn source_candidate_window() -> Window {
    catalog().candidate.clone()
}

fn revision_index(revision: &str) -> Option<usize> {
    catalog().revisions.iter().position(|item| item == revision)
}

/// Build the serving window declared by retained candidate metadata.
/// Both bounds must be known catalog revisions so an unrecognized value can
/// never expand rollback eligibility.
pub fn candidate_window(schema_min: &str, schema_head: &str) -> Result<Window, String> {
    let Some(min_idx) = revision_index(schema_min) else {
        return Err(format!(
            "candidate schema minimum revision {schema_min} is not in the application schema catalog"
        ));
    };
    let Some(head_idx) = revision_index(schema_head) else {
        return Err(format!(
            "candidate schema head revision {schema_head} is not in the application schema catalog"
        ));
    };
    if min_idx > head_idx {
        return Err(format!(
            "candidate schema minimum revision {schema_min} is after its head revision {schema_head}"
        ));
    }
    Ok(Window {
        schema_min: schema_min.to_string(),
        schema_head: schema_head.to_string(),
        artifact_identity_ambiguous: false,
    })
}

/// Live revision is inside the application's declared range on the linear
/// catalog chain. Unknown live revisions (newer than the catalog) are outside.
pub fn live_in_window(live: &str, window: &Window) -> bool {
    let Some(live_idx) = revision_index(live) else {
        return false;
    };
    let Some(min_idx) = revision_index(&window.schema_min) else {
        return false;
    };
    let Some(head_idx) = revision_index(&window.schema_head) else {
        return false;
    };
    live_idx >= min_idx && live_idx <= head_idx
}

type VersionKey = (u64, u64, u64, u8, u64);

fn version_key(version: &str) -> Option<VersionKey> {
    let normalized = normalize_app_version(version);
    let (core, release_candidate) = match normalized.split_once("-rc.") {
        Some((core, rc)) => (core, Some(rc)),
        None => (normalized.as_str(), None),
    };
    let parse_number = |part: &str| {
        if part.is_empty()
            || !part.bytes().all(|byte| byte.is_ascii_digit())
            || (part.len() > 1 && part.starts_with('0'))
        {
            return None;
        }
        part.parse::<u64>().ok()
    };
    let mut parts = core.split('.');
    let major = parse_number(parts.next()?)?;
    let minor = parse_number(parts.next()?)?;
    let patch = parse_number(parts.next()?)?;
    if parts.next().is_some() {
        return None;
    }
    let (stable, rc) = match release_candidate {
        Some(value) => (0, parse_number(value)?),
        None => (1, 0),
    };
    Some((major, minor, patch, stable, rc))
}

/// Catalog head of the first catalogued version released after `app_version`.
/// A build carrying `app_version` was cut before that release, so it cannot
/// declare a later schema head. `None` when no later version is catalogued;
/// callers pass a catalogued version, so an unparseable one never reaches the
/// open bound. Version order stands in for release order on one schema chain:
/// a patch cut after a later line was catalogued is refused, which fails
/// closed with a fail forward hint.
pub fn next_release_head(app_version: &str) -> Option<String> {
    let key = version_key(app_version)?;
    catalog()
        .windows
        .iter()
        .filter_map(|(version, window)| Some((version_key(version)?, window)))
        .filter(|(candidate, _)| *candidate > key)
        .min_by_key(|(candidate, _)| *candidate)
        .map(|(_, window)| window.schema_head.clone())
}

/// `candidate` sorts strictly after `current`, or `current` is `None`. An
/// unparseable `candidate` never outranks; an unparseable `current` is
/// outranked by any parseable one.
pub fn version_outranks(candidate: &str, current: Option<&str>) -> bool {
    let Some(key) = version_key(candidate) else {
        return false;
    };
    current.is_none_or(|current| Some(key) > version_key(current))
}

/// ADR 0142 bounds on a build's own declared window: the version is catalogued
/// and not marked `artifact_identity_ambiguous`, the declared head is strictly
/// later than the catalog head, and it is no later than the next catalogued
/// release's head (open when none). Requiring a later head keeps every
/// published release judged by its catalog window. The upper bound admits only
/// a build cut between its release and the next one, so a chart packaged from a
/// newer tree under an older version stays out.
pub fn declared_window_within_release(app_version: &str, declared: &Window) -> bool {
    let Some(catalog) = window_for(app_version) else {
        return false;
    };
    if catalog.artifact_identity_ambiguous {
        return false;
    }
    // candidate_window refuses a min after its head, so this orders the heads.
    let later_head = declared.schema_head != catalog.schema_head
        && candidate_window(&catalog.schema_head, &declared.schema_head).is_ok();
    let before_next_release = next_release_head(app_version)
        .is_none_or(|next_head| candidate_window(&declared.schema_head, &next_head).is_ok());
    later_head && before_next_release
}

/// A Helm history build whose retained manifest declares its own ADR 0142
/// schema window.
#[derive(Debug, Clone)]
pub struct DeclaredBuild {
    pub app_version: String,
    pub helm_revision: u32,
    pub window: Window,
}

/// Newest catalogued application version in `candidates` whose window contains
/// `live`. That is the fail-forward target a refused rollback must name.
pub fn newest_fail_forward<'a>(
    candidates: impl IntoIterator<Item = &'a str>,
    live: &str,
) -> Option<String> {
    let mut best: Option<String> = None;
    for raw in candidates {
        let version = normalize_app_version(raw);
        if version.is_empty() {
            continue;
        }
        let Some(key) = version_key(&version) else {
            continue;
        };
        let Some(window) = window_for(&version) else {
            continue;
        };
        if !live_in_window(live, &window) {
            continue;
        }
        match &best {
            None => best = Some(version),
            Some(current) if Some(key) > version_key(current) => {
                best = Some(version);
            }
            Some(_) => {}
        }
    }
    best
}

#[derive(Debug)]
enum FailForward<'a> {
    Catalog(String),
    Declared(&'a DeclaredBuild),
}

/// The fail-forward target: [`newest_fail_forward`] over `history_apps`, or a
/// newer declared build that passes [`declared_window_within_release`] and
/// whose window contains `live`. A catalog match wins a version tie because it
/// needs no qualifier; among declared builds of one version the highest Helm
/// revision wins.
fn select_fail_forward<'a>(
    history_apps: &[String],
    declared: &'a [DeclaredBuild],
    live: &str,
) -> Option<FailForward<'a>> {
    let catalog = newest_fail_forward(history_apps.iter().map(String::as_str), live);
    let mut best: Option<&DeclaredBuild> = None;
    for build in declared {
        if !live_in_window(live, &build.window)
            || !declared_window_within_release(&build.app_version, &build.window)
        {
            continue;
        }
        let replaces = match best {
            None => version_key(&build.app_version).is_some(),
            Some(current) => {
                version_outranks(&build.app_version, Some(&current.app_version))
                    || (version_key(&build.app_version) == version_key(&current.app_version)
                        && build.helm_revision > current.helm_revision)
            }
        };
        if replaces {
            best = Some(build);
        }
    }
    match (catalog, best) {
        (Some(version), Some(build)) if !version_outranks(&build.app_version, Some(&version)) => {
            Some(FailForward::Catalog(version))
        }
        (_, Some(build)) => Some(FailForward::Declared(build)),
        (Some(version), None) => Some(FailForward::Catalog(version)),
        (None, None) => None,
    }
}

#[derive(Debug)]
pub struct SchemaRefusal {
    pub message: String,
    pub fix: String,
}

fn fail_forward_fix(forward: Option<FailForward<'_>>, live: &str) -> String {
    match forward {
        Some(FailForward::Catalog(version)) => format!(
            "fail forward to application {version}, which can start against revision {live}"
        ),
        // The published artifact of this version may not start; only the build
        // that revision installed declared a window containing `live`.
        Some(FailForward::Declared(build)) => format!(
            "fail forward to application {} using the build Helm revision {} installed, which can start against revision {live}",
            normalize_app_version(&build.app_version),
            build.helm_revision
        ),
        None => format!(
            "no catalogued application version in this release history can start against revision {live}; stay on the current revision"
        ),
    }
}

/// Check that `target_app` can start against `live` using its resolved artifact
/// window. `history_apps` and `declared` supply fail forward candidates from
/// Helm history.
pub fn check_target_schema(
    target_app: &str,
    window: &Window,
    live: &str,
    history_apps: &[String],
    declared: &[DeclaredBuild],
) -> Result<(), SchemaRefusal> {
    let target = normalize_app_version(target_app);
    if live_in_window(live, window) {
        return Ok(());
    }
    Err(SchemaRefusal {
        message: format!(
            "refusing rollback to application {target}: live database revision {live} is outside its declared schema range (head {})",
            window.schema_head
        ),
        fix: fail_forward_fix(select_fail_forward(history_apps, declared, live), live),
    })
}

/// Message for an application version that has no static catalog entry. A dry
/// run without a live revision has this message but no fail-forward fix.
pub fn missing_target_window_message(target_app: &str) -> String {
    let target = normalize_app_version(target_app);
    format!("refusing rollback to application {target}: no declared schema range for that version")
}

/// Refusal for an application version that has no static catalog entry.
pub fn missing_target_window_refusal(
    target_app: &str,
    live: &str,
    history_apps: &[String],
    declared: &[DeclaredBuild],
) -> SchemaRefusal {
    SchemaRefusal {
        message: missing_target_window_message(target_app),
        fix: fail_forward_fix(select_fail_forward(history_apps, declared, live), live),
    }
}

/// Last non-log token of `alembic current` stdout: `0039 (head)` -> `0039`.
pub fn parse_alembic_current_output(stdout: &str) -> Option<String> {
    for line in stdout.lines().rev() {
        let line = line.trim();
        if line.is_empty()
            || line.starts_with("INFO ")
            || line.starts_with("DEBUG ")
            || line.starts_with("WARNING ")
        {
            continue;
        }
        let token = line.split_whitespace().next()?;
        if !token.is_empty() && token.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Some(token.to_string());
        }
    }
    None
}

/// Strip connection strings and password-shaped tokens from probe output so a
/// refusal never carries database contents or credentials.
pub fn redact_probe_text(text: &str) -> String {
    static PATTERNS: OnceLock<[Regex; 3]> = OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        [
            Regex::new(r"(?i)(?:postgres(?:ql)?(?:\+asyncpg)?)://\S+").expect("dsn regex"),
            Regex::new(r"(?i)database_url=\S+").expect("database_url regex"),
            Regex::new(r"(?i)password=\S+").expect("password regex"),
        ]
    });
    let mut out = text.to_string();
    for re in patterns {
        out = re.replace_all(&out, "<redacted>").into_owned();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn migration_graph(
        versions: &std::path::Path,
    ) -> Result<std::collections::BTreeMap<String, Vec<String>>, String> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cli is not the repo root");
        let output = std::process::Command::new("python3")
            .args([
                "-c",
                r#"
import json
import runpy
import sys
from pathlib import Path

read_revision = runpy.run_path(sys.argv[1])["_revision_fields"]
graph = {}
for path in sorted(Path(sys.argv[2]).glob("*.py")):
    revision, parents = read_revision(path)
    if revision is None:
        if path.name != "__init__.py":
            raise ValueError(f"migration has no literal revision: {path.name}")
        continue
    if revision in graph:
        raise ValueError(f"duplicate Alembic revision: {revision}")
    graph[revision] = parents
print(json.dumps(graph))
"#,
            ])
            .arg(root.join("scripts/check-schema-window.py"))
            .arg(versions)
            .output()
            .map_err(|error| format!("cannot inspect migration graph: {error}"))?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("migration graph is invalid JSON: {error}"))
    }

    #[test]
    fn migration_graph_reads_tuple_parents_and_refuses_duplicate_ids() {
        let directory = tempfile::tempdir().expect("migration sources");
        std::fs::write(
            directory.path().join("base.py"),
            "revision = 'base'\ndown_revision = None\n",
        )
        .expect("base migration");
        std::fs::write(
            directory.path().join("stable.py"),
            "revision: str = 'stable'\ndown_revision: str | None = 'base'\n",
        )
        .expect("stable migration");
        std::fs::write(
            directory.path().join("feature.py"),
            "revision: str = 'feature'\ndown_revision: str | None = 'base'\n",
        )
        .expect("feature migration");
        std::fs::write(
            directory.path().join("merge.py"),
            "revision: str = 'merge'\ndown_revision: tuple[str, str] = (\n 'feature',\n 'stable',\n)\n",
        )
        .expect("merge migration");
        let graph = migration_graph(directory.path()).expect("both merge parents are readable");
        assert_eq!(graph["merge"], ["feature", "stable"]);
        let parents: std::collections::BTreeSet<&String> = graph.values().flatten().collect();
        let heads: Vec<&str> = graph
            .keys()
            .filter(|revision| !parents.contains(revision))
            .map(String::as_str)
            .collect();
        assert_eq!(heads, ["merge"]);
        std::fs::write(
            directory.path().join("duplicate.py"),
            "revision = 'stable'\n",
        )
        .expect("duplicate migration");
        let error = migration_graph(directory.path()).expect_err("duplicate IDs are invalid");
        assert!(error.contains("duplicate Alembic revision: stable"));
    }

    #[test]
    fn next_release_head_is_the_head_of_the_next_catalogued_version() {
        assert_eq!(next_release_head("0.10.0").as_deref(), Some("0058"));
        assert_eq!(next_release_head("v0.10.0-rc.1").as_deref(), Some("0057"));
        assert_eq!(next_release_head("0.10.3").as_deref(), Some("0070"));
        assert_eq!(next_release_head("0.12.1").as_deref(), Some("0081"));
        assert_eq!(next_release_head("not-a-version"), None);
    }

    #[test]
    fn newest_catalogued_version_has_no_next_release_head() {
        let newest = catalog()
            .windows
            .keys()
            .max_by_key(|version| version_key(version))
            .expect("the catalog has released windows");
        assert_eq!(
            next_release_head(newest),
            None,
            "a main build carrying the newest release must keep an open upper bound"
        );
    }

    #[test]
    fn v084_cannot_start_against_0039() {
        let window = window_for("0.8.4").expect("0.8.4 is catalogued");
        assert_eq!(window.schema_head, "0038");
        assert!(!live_in_window("0039", &window));
        assert!(live_in_window("0038", &window));
        assert!(live_in_window("0001", &window));
    }

    #[test]
    fn v085_can_start_against_0039() {
        let window = window_for("0.8.5").expect("0.8.5 is catalogued");
        assert_eq!(window.schema_head, "0039");
        assert!(live_in_window("0039", &window));
        assert!(live_in_window("0038", &window));
    }

    #[test]
    fn incident_refusal_names_fail_forward_v085() {
        let history = ["0.8.4".to_string(), "0.8.5".to_string()];
        let window = window_for("0.8.4").expect("0.8.4 window");
        let err =
            check_target_schema("0.8.4", &window, "0039", &history, &[]).expect_err("incompatible");
        assert!(err.message.contains("0039") && err.message.contains("0.8.4"));
        assert!(err.message.contains("0038"));
        assert!(err.fix.contains("0.8.5"), "{}", err.fix);
        assert!(!err.message.contains("postgresql://") && !err.fix.contains("postgresql://"));
    }

    #[test]
    fn compatible_v085_to_v085_is_allowed() {
        let window = window_for("0.8.5").expect("0.8.5 window");
        check_target_schema("0.8.5", &window, "0039", &["0.8.5".to_string()], &[])
            .expect("compatible");
    }

    #[test]
    fn unknown_app_version_is_refused() {
        let err = missing_target_window_refusal(
            "0.7.3",
            "0039",
            &["0.7.3".to_string(), "0.8.5".to_string()],
            &[],
        );
        assert!(err.message.contains("0.7.3"));
        assert!(err.fix.contains("0.8.5"));
    }

    fn declared(app_version: &str, helm_revision: u32, min: &str, head: &str) -> DeclaredBuild {
        DeclaredBuild {
            app_version: app_version.to_string(),
            helm_revision,
            window: candidate_window(min, head).expect("declared bounds are catalogued"),
        }
    }

    fn fix_for(history: &[&str], builds: &[DeclaredBuild], live: &str) -> String {
        let history: Vec<String> = history.iter().map(|app| app.to_string()).collect();
        missing_target_window_refusal("sha-4498fa0", live, &history, builds).fix
    }

    #[test]
    fn declared_window_within_release_refuses_ambiguous_and_uncatalogued_versions() {
        // 0.10.2 is catalogued 0060..0062; 0.10.3's head 0063 bounds its builds.
        assert!(declared_window_within_release(
            "0.10.2",
            &candidate_window("0045", "0063").unwrap()
        ));
        assert!(!declared_window_within_release(
            "0.10.2",
            &candidate_window("0045", "0062").unwrap()
        ));
        assert!(!declared_window_within_release(
            "0.10.1",
            &candidate_window("0045", "0063").unwrap()
        ));
        // 0.8.9 would pass the head bounds (0039 < 0044 <= 0.9.0's 0044) but its
        // artifact identity is ambiguous.
        assert!(window_for("0.8.9").unwrap().artifact_identity_ambiguous);
        assert!(!declared_window_within_release(
            "0.8.9",
            &candidate_window("0001", "0044").unwrap()
        ));
        assert!(!declared_window_within_release(
            "sha-4498fa0",
            &candidate_window("0108", "0120").unwrap()
        ));
    }

    #[test]
    fn fail_forward_tie_prefers_the_catalog_match() {
        let builds = [declared("0.10.2", 4, "0045", "0063")];
        assert_eq!(
            fix_for(&["0.10.1", "0.10.2"], &builds, "0060"),
            "fail forward to application 0.10.2, which can start against revision 0060"
        );
    }

    #[test]
    fn fail_forward_names_a_newer_declared_build_over_a_lower_catalog_match() {
        assert_eq!(
            fix_for(&["0.10.1", "0.10.2"], &[], "0058"),
            "fail forward to application 0.10.1, which can start against revision 0058"
        );
        let builds = [
            declared("0.10.2", 4, "0045", "0063"),
            declared("0.10.2", 6, "0045", "0063"),
            declared("0.10.2", 5, "0045", "0063"),
        ];
        assert_eq!(
            fix_for(&["0.10.1", "0.10.2"], &builds, "0058"),
            "fail forward to application 0.10.2 using the build Helm revision 6 installed, which can start against revision 0058"
        );
    }

    #[test]
    fn fail_forward_ignores_declared_builds_outside_their_release_bounds() {
        let lower = "fail forward to application 0.10.1, which can start against revision 0058";
        // Head equal to the catalog head: the published artifact's own metadata.
        let published = [declared("0.10.2", 4, "0045", "0062")];
        assert_eq!(fix_for(&["0.10.1", "0.10.2"], &published, "0058"), lower);
        // Head past the next catalogued release's head (0.10.3, 0063).
        let too_new = [declared("0.10.2", 4, "0045", "0070")];
        assert_eq!(fix_for(&["0.10.1", "0.10.2"], &too_new, "0058"), lower);
        // Window that does not contain live.
        let excludes = [declared("0.10.2", 4, "0060", "0063")];
        assert_eq!(fix_for(&["0.10.1", "0.10.2"], &excludes, "0058"), lower);
        // Ambiguous and uncatalogued versions are never named.
        let unnamed = [
            declared("0.8.9", 4, "0001", "0044"),
            declared("sha-4498fa0", 5, "0108", "0120"),
        ];
        assert_eq!(
            fix_for(&[], &unnamed, "0044"),
            "no catalogued application version in this release history can start against revision 0044; stay on the current revision"
        );
    }

    #[test]
    fn unknown_live_revision_is_outside_every_window() {
        let window = window_for("0.8.6").unwrap();
        assert!(!live_in_window("0099", &window));
        assert!(
            !live_in_window("0040", &window),
            "0.8.6's declared window ends at 0039; 0040 is this tree's later head"
        );
    }

    #[test]
    fn alembic_current_parses_head_annotation() {
        let stdout =
            "INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.\n0039 (head)\n";
        assert_eq!(
            parse_alembic_current_output(stdout).as_deref(),
            Some("0039")
        );
        assert_eq!(
            parse_alembic_current_output("0038\n").as_deref(),
            Some("0038")
        );
        assert_eq!(parse_alembic_current_output("INFO only\n"), None);
    }

    #[test]
    fn probe_redaction_strips_a_dsn() {
        let redacted = redact_probe_text(
            "could not connect to postgresql://curie:secret-password@postgres:5432/curie",
        );
        assert!(!redacted.contains("secret-password"));
        assert!(!redacted.contains("postgresql://"));
        assert!(redacted.contains("<redacted>"));
    }

    #[test]
    fn version_from_chart_reads_curie_chart_field() {
        assert_eq!(version_from_chart("curie-0.8.4").as_deref(), Some("0.8.4"));
        assert_eq!(version_from_chart("other-1.0.0"), None);
    }

    #[test]
    fn released_v090_and_v091_remain_at_0044_and_v091_refuses_0046() {
        let v090 = window_for("0.9.0").expect("0.9.0 is catalogued");
        let v091 = window_for("0.9.1").expect("0.9.1 is catalogued");
        assert_eq!(v090.schema_head, "0044");
        assert_eq!(v091.schema_head, "0044");

        let err = check_target_schema(
            "0.9.1",
            &v091,
            "0046",
            &["0.9.1".to_string(), "0.10.0".to_string()],
            &[],
        )
        .expect_err("published 0.9.1 must refuse live revision 0046");
        assert!(err.message.contains("0.9.1"));
        assert!(err.message.contains("0046"));
        assert!(err.message.contains("0044"));
    }

    #[test]
    fn released_v092_accepts_adapter_0045_and_refuses_later_live_revisions() {
        let v092 = window_for("0.9.2").expect("0.9.2 is catalogued");
        assert_eq!(v092.schema_min, "0045");
        assert_eq!(v092.schema_head, "0045");

        check_target_schema(
            "0.9.2",
            &v092,
            "0045",
            &["0.9.2".to_string(), "0.10.0".to_string()],
            &[],
        )
        .expect("published 0.9.2 accepts its adapter schema");

        for live in ["0046", "0047", "0048", "0049", "0050"] {
            let err = check_target_schema(
                "0.9.2",
                &v092,
                live,
                &["0.9.2".to_string(), "0.10.0".to_string()],
                &[],
            )
            .expect_err("published 0.9.2 must refuse later live revisions");
            assert!(err.message.contains("0.9.2"));
            assert!(err.message.contains(live));
            assert!(err.message.contains("0045"));
        }
    }

    #[test]
    fn released_v100_and_v101_keep_their_0058_window() {
        let n = window_for("0.10.0").expect("0.10.0 is catalogued");
        let n1 = window_for("0.10.1").expect("0.10.1 is catalogued");
        assert_eq!(n.schema_min, "0045");
        assert_eq!(n.schema_head, "0058");
        assert_eq!(n.schema_min, n1.schema_min);
        assert_eq!(n.schema_head, n1.schema_head);
        assert_eq!(n1.schema_head, "0058");
        check_target_schema(
            "0.10.0",
            &n,
            "0058",
            &["0.10.0".to_string(), "0.10.1".to_string()],
            &[],
        )
        .expect("N+1 to N is the same schema window");
        let err = check_target_schema(
            "0.8.7",
            &window_for("0.8.7").expect("0.8.7 window"),
            &n.schema_head,
            &["0.8.7".to_string(), "0.10.0".to_string()],
            &[],
        )
        .expect_err("0.8.7 cannot start on the released 0.10.0 head");
        assert!(
            err.message.contains("0.8.7") && err.message.contains("schema"),
            "{}",
            err.message
        );
    }

    // @spec PROTECTED-HOOK-SOURCE-2: require the ledger without rewriting prior windows.
    #[test]
    fn candidate_window_tracks_the_catalog_without_changing_released_windows() {
        // @spec DEPLOY-NOTICE-RELEASE-1.
        // @spec AGENT-ROLE-PERSISTENCE-V1-9: current policy reads require 0133.
        let candidate = source_candidate_window();
        // @spec HTTP-APPROVAL-CONTINUITY-5: approval ORM reads require 0137.
        // @spec AUTOMATED-REMEDIATION-15: 0138 only widens a CHECK.
        // @spec HTTP-INVOCATION-EXECUTION-21: Grid reports write the 0139 namespace kind.
        // Factory ORM reads require the request base columns introduced by 0141.
        assert_eq!(candidate.schema_min, "0141");
        assert_eq!(candidate.schema_head, "0141");
        assert_eq!(
            candidate.schema_head.as_str(),
            catalog().revisions.last().unwrap()
        );
        let current = candidate_window(&candidate.schema_min, &candidate.schema_head)
            .expect("candidate bounds are catalogued and ordered");
        // #2911: the Agent ORM and resolver read 0101's tenant-scope columns, so
        // every revision the window admitted below it is no longer live.
        for below in [
            "0082", "0083", "0084", "0085", "0086", "0087", "0088", "0089", "0090", "0091a",
            "0092a", "0093a", "0094", "0095", "0096", "0097", "0098", "0099", "0100", "0093",
            "0101", "0102", "0103",
        ] {
            assert!(!live_in_window(below, &current), "{below}");
        }
        assert!(!live_in_window("0108", &current));
        assert!(!live_in_window("0120", &current));
        assert!(!live_in_window("0121", &current));
        assert!(!live_in_window("0122", &current));
        assert!(!live_in_window("0123", &current));
        assert!(!live_in_window("0130", &current));
        assert!(!live_in_window("0131", &current));
        assert!(!live_in_window("0136", &current));
        // @spec AGENT-ROLE-PERSISTENCE-V1-9: policy ORM requires 0133.
        assert!(!live_in_window("0132", &current));
        // @spec HTTP-APPROVAL-CONTINUITY-5: approval ORM requires 0137.
        assert!(!live_in_window("0133", &current));
        // @spec HTTP-INVOCATION-EXECUTION-21: Grid reports write the 0139 namespace kind.
        assert!(!live_in_window("0137", &current));
        assert!(!live_in_window("0138", &current));
        assert!(!live_in_window("0139", &current));
        assert!(live_in_window("0141", &current));
        assert!(!live_in_window("0075", &current));
        assert!(!live_in_window("0076", &current));
        let retained = window_for("0.12.0").expect("published foundation remains catalogued");
        assert_eq!(retained.schema_min, "0070");
        assert_eq!(retained.schema_head, "0073");
        assert!(live_in_window("0070", &retained));
        assert!(live_in_window("0071", &retained));
        assert!(live_in_window("0072", &retained));
        assert!(live_in_window("0073", &retained));
        assert!(!live_in_window("0075", &retained));
        assert!(!live_in_window("0069", &retained));

        let released = window_for("0.12.2").expect("published patch is catalogued");
        assert_eq!(released.schema_min, "0076");
        assert_eq!(released.schema_head, "0081");
        let published_stable = window_for("0.12.3").expect("published stable is catalogued");
        assert_eq!(published_stable.schema_min, "0076");
        assert_eq!(published_stable.schema_head, "0093");
        for revision in ["0081", "0091", "0092", "0093"] {
            assert!(live_in_window(revision, &published_stable));
        }
        for revision in ["0082", "0091a", "0098", "0099"] {
            assert!(!live_in_window(revision, &published_stable));
        }
        // v0.13.0 is published: its window stays at the 0108 join while the
        // candidate advances to agent access policies (0121, #4585).
        let candidate_release = window_for("0.13.0").expect("published 0.13.0 is catalogued");
        assert_eq!(candidate_release.schema_min, "0108");
        assert_eq!(candidate_release.schema_head, "0108");
        assert!(live_in_window("0108", &candidate_release));
        assert!(!live_in_window("0120", &candidate_release));
        assert!(!live_in_window("0100", &candidate_release));
        assert!(!live_in_window("0075", &candidate_release));
        let published =
            window_for("0.12.1").expect("released source control window remains catalogued");
        assert_eq!(published.schema_min, "0076");
        assert_eq!(published.schema_head, "0076");
        assert!(live_in_window("0076", &published));
        assert!(!live_in_window("0077", &published));
        let stable = window_for("0.12.0").expect("released window remains catalogued");
        assert_eq!(stable.schema_min, "0070");
        assert_eq!(stable.schema_head, "0073");
        assert!(live_in_window("0073", &stable));
        assert!(!live_in_window("0075", &stable));

        assert_eq!(window_for("0.10.1").unwrap().schema_head, "0058");
        let prior = window_for("0.10.2").expect("0.10.2 remains catalogued");
        assert_eq!(prior.schema_min, "0060");
        assert_eq!(prior.schema_head, "0062");
        let current = window_for("0.10.3").expect("0.10.3 remains catalogued");
        assert_eq!(current.schema_min, "0063");
        assert_eq!(current.schema_head, "0063");
        let released = window_for("0.11.0").expect("0.11.0 remains catalogued");
        assert_eq!(released.schema_min, "0070");
        assert_eq!(released.schema_head, "0070");
        for version in ["0.12.0", "0.12.0-rc.1"] {
            let released = window_for(version).expect("released window remains catalogued");
            assert_eq!(released.schema_min, "0070");
            assert_eq!(released.schema_head, "0073");
        }
    }

    // @spec PROTECTED-HOOK-SOURCE-2: new candidate metadata is distinct from prior windows.
    #[test]
    fn source_control_candidate_0122_requires_ledger_schema() {
        let window = window_for("0.12.2").expect("source control candidate is catalogued");
        assert_eq!(window.schema_min, "0076");
        assert_eq!(window.schema_head, "0081");
        assert!(live_in_window("0076", &window));
        assert!(live_in_window("0080", &window));
        assert!(live_in_window("0081", &window));
        assert!(!live_in_window("0075", &window));
    }

    #[test]
    fn unreleased_patch_093_keeps_the_092_schema_window() {
        let previous = window_for("0.9.2").expect("0.9.2 schema window");
        let patch = window_for("0.9.3").expect("0.9.3 schema window");
        assert_eq!(patch.schema_min, previous.schema_min);
        assert_eq!(patch.schema_head, previous.schema_head);
        assert_eq!(patch.schema_min, "0045");
        assert_eq!(patch.schema_head, "0045");
    }

    #[test]
    fn catalog_head_matches_this_tree() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("cli is not the repo root");
        let chart =
            std::fs::read_to_string(root.join("charts/curie/Chart.yaml")).expect("Chart.yaml");
        let app_version = chart
            .lines()
            .find_map(|line| {
                line.strip_prefix("appVersion:")
                    .map(|rest| normalize_app_version(rest.trim().trim_matches('"')))
            })
            .expect("appVersion");
        let chart_window = window_for(&app_version)
            .unwrap_or_else(|| panic!("catalog missing window for {app_version}"));
        assert!(
            !chart_window.artifact_identity_ambiguous,
            "Chart.yaml appVersion {app_version} must have one unambiguous artifact identity"
        );
        // A registered (released) appVersion keeps its released window while the
        // candidate moves ahead; scripts/check-schema-window.py owns comparing
        // that window to the release tag. Only an unregistered appVersion must
        // still equal the candidate.
        let atlas: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(root.join("docs/architecture-atlas/versions.json"))
                .expect("architecture atlas versions.json"),
        )
        .expect("versions.json is valid JSON");
        let release_id = format!("v{app_version}");
        let registered = atlas["versions"]
            .as_array()
            .expect("versions.json versions array")
            .iter()
            .any(|entry| entry["id"].as_str() == Some(release_id.as_str()));
        if !registered {
            assert_eq!(
                chart_window.schema_min,
                catalog().candidate.schema_min,
                "unregistered Chart.yaml appVersion {app_version} minimum must match the candidate"
            );
            assert_eq!(
                chart_window.schema_head,
                catalog().candidate.schema_head,
                "unregistered Chart.yaml appVersion {app_version} head must match the candidate"
            );
        }

        let versions = root.join("apps/api/alembic/versions");
        let graph = migration_graph(&versions).expect("migration graph has unique literal IDs");
        for id in graph.keys() {
            assert!(
                catalog().revisions.iter().any(|item| item == id),
                "catalog revisions missing alembic id {id}"
            );
        }
        let down_of: std::collections::BTreeSet<&String> = graph.values().flatten().collect();
        let heads: Vec<&String> = graph.keys().filter(|id| !down_of.contains(id)).collect();
        assert_eq!(heads.len(), 1, "expected one alembic head, got {heads:?}");
        let tree_head = heads[0];
        assert!(
            catalog().revisions.iter().any(|item| item == tree_head),
            "catalog revisions missing this tree's alembic head {tree_head}"
        );
        assert_eq!(
            catalog().candidate.schema_head,
            *tree_head,
            "candidate window head must match this tree's Alembic head {tree_head}"
        );
    }
}
