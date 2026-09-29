//! Which image a connector runs, and which containers a redeploy removes.
//!
//! Two defects, one seam each, both pinned here without a Docker daemon.
//!
//! **Selection.** `connectors.lock.yaml` records what a declared `build:`
//! resolved to, so under ADR 0113 it is the identity of a `build:` connector and
//! of nothing else. Both emitters -- the skill tier's `docker run` and the local
//! tier's compose overlay -- used to consult the lock FIRST, unconditionally, so
//! a connector whose declaration had since switched from `build:` to `image:`
//! silently kept running the last source build while its author read the image
//! they had declared. `connector_build::resolved_image` is now the one rule and
//! both emitters route through it; the Python cluster render already gated on
//! the same condition (`connector_lock.apply_lock`: `if spec.build is None`).
//!
//! **Reconcile.** `local deploy` only ever ADDED services: compose starts what
//! the overlay names and leaves everything else alone, so a connector a new
//! bundle version dropped or renamed kept serving the runner an endpoint the
//! bundle no longer declares. The reap that fixes it is the scope decision, and
//! the scope is the whole risk -- the local tier shares ONE compose project
//! (`curie`) with the api/worker stack and with every other locally deployed
//! agent, so `--remove-orphans` would remove the platform and a project-label
//! sweep would remove another agent's connectors. The containers are therefore
//! labeled with their agent and their object name at creation, in both emitters,
//! and the reap selects on all three labels and then removes only what the new
//! desired set does not name.

use std::collections::{BTreeMap, BTreeSet};

use curie::commands::skill_connector_plan;
use curie::connector_build::{
    compose_overlay, object_name, resolved_image, ConnectorBuildDecl, ConnectorLockEntryDecl,
    ConnectorLockFileDecl, ConnectorScope, ConnectorSpecDecl, ConnectorsFileDecl, Delivery,
    LOCK_VERSION,
};
use curie::docker::{
    agent_connectors_argv, connector_identity_labels, connector_project_label, connectors_to_reap,
    parse_agent_connectors, ConnectorStartSpec, RunningConnector, CONNECTOR_AGENT_LABEL_KEY,
    CONNECTOR_COMPONENT_LABEL, CONNECTOR_OBJECT_LABEL_KEY,
};
use curie::exit::{classify, ExitClass};
use tempfile::TempDir;

/// What the author declares TODAY.
const DECLARED_IMAGE: &str = "ghcr.io/acme-corp/sre-bot-tempo:v2";
/// What a build recorded in the lock BEFORE the declaration changed.
const LOCKED_IMAGE: &str = "ghcr.io/acme-corp/sre-bot-tempo@sha256:\
                            2222222222222222222222222222222222222222222222222222222222222222";

const AGENT: &str = "sre-bot";
const RELEASE: &str = "curie";
const PROJECT: &str = "curie";

// ─── Fixtures ────────────────────────────────────────────────────────────────

fn scope() -> ConnectorScope {
    ConnectorScope {
        release: RELEASE.to_string(),
        agent: AGENT.to_string(),
        namespace: "default".to_string(),
    }
}

fn image_spec() -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        image: Some(DECLARED_IMAGE.to_string()),
        ..Default::default()
    }
}

fn build_spec() -> ConnectorSpecDecl {
    ConnectorSpecDecl {
        build: Some(ConnectorBuildDecl {
            context: "connectors/tempo".to_string(),
            dockerfile: "Dockerfile".to_string(),
            platforms: vec!["linux/amd64".to_string()],
        }),
        ..Default::default()
    }
}

/// A lock entry for `connector`, as `curie build` writes one.
fn lock_for(connector: &str) -> ConnectorLockFileDecl {
    ConnectorLockFileDecl {
        version: LOCK_VERSION,
        connectors: BTreeMap::from([(
            connector.to_string(),
            ConnectorLockEntryDecl {
                image: LOCKED_IMAGE.to_string(),
                delivery: Delivery::Registry,
                platforms: vec!["linux/amd64".to_string()],
                source_digest: "sha256:\
                                3333333333333333333333333333333333333333333333333333333333333333"
                    .to_string(),
            },
        )]),
        ..Default::default()
    }
}

fn empty_lock() -> ConnectorLockFileDecl {
    ConnectorLockFileDecl {
        version: LOCK_VERSION,
        connectors: BTreeMap::new(),
        ..Default::default()
    }
}

fn declaring(connector: &str, spec: ConnectorSpecDecl) -> ConnectorsFileDecl {
    ConnectorsFileDecl {
        connectors: BTreeMap::from([(connector.to_string(), spec)]),
        ..Default::default()
    }
}

// ─── The shared selection rule ───────────────────────────────────────────────

/// The defect: a lock entry outliving the `build:` that produced it.
///
/// The declaration is the author's statement of what to run. A lock entry left
/// behind by an earlier `build:` form of the same connector is stale by
/// construction -- nothing rebuilds it, nothing invalidates it, and consulting
/// it first means the bundle runs an image no one reading `connectors.yaml` can
/// see.
#[test]
fn a_stale_lock_entry_never_hijacks_an_image_connector() {
    let image = resolved_image("tempo", &image_spec(), &lock_for("tempo"))
        .expect("an `image:` connector always has an image");
    assert_eq!(
        image, DECLARED_IMAGE,
        "the declared image wins over a lock entry no current `build:` produced"
    );
}

/// The other half of the rule, and the reason it is not simply "ignore the
/// lock": a `build:` connector has no image of its own to run.
#[test]
fn a_build_connector_resolves_to_its_locked_image() {
    let image = resolved_image("tempo", &build_spec(), &lock_for("tempo"))
        .expect("a locked `build:` connector resolves");
    assert_eq!(image, LOCKED_IMAGE);
}

/// Unchanged behavior, restated so the gate above cannot be "fixed" by
/// dropping the lock consultation altogether.
#[test]
fn a_build_connector_with_no_lock_entry_names_curie_build() {
    let error = resolved_image("tempo", &build_spec(), &empty_lock())
        .expect_err("an unbuilt `build:` connector has nothing to run");
    let (class, _fix) = classify(&error);
    assert_eq!(
        class,
        ExitClass::Usage,
        "no retry of the same argv clears an unbuilt connector: {error:#}"
    );
    let text = format!("{error:#}");
    assert!(
        text.contains("curie build"),
        "the refusal must name the command that clears it: {text}"
    );
    assert!(text.contains("tempo"), "{text}");
}

// ─── Both emitters route through it ──────────────────────────────────────────

/// The local tier. The overlay IS the local tier's image decision: whatever it
/// writes into `image:` is what compose starts.
#[test]
fn the_local_overlay_starts_the_declared_image_despite_a_stale_lock_entry() {
    let dir = TempDir::new().expect("tempdir");
    let overlay = compose_overlay(
        &lock_for("tempo"),
        &declaring("tempo", image_spec()),
        &scope(),
        PROJECT,
        dir.path(),
    )
    .expect("the overlay renders");
    let service = &overlay["services"][object_name(RELEASE, AGENT, "tempo")];
    assert_eq!(service["image"], DECLARED_IMAGE, "{overlay}");
}

/// A `build:` connector's overlay still carries the locked digest, so the fix
/// above did not cost the source-build path its image.
#[test]
fn the_local_overlay_starts_the_locked_image_for_a_build_connector() {
    let dir = TempDir::new().expect("tempdir");
    let overlay = compose_overlay(
        &lock_for("tempo"),
        &declaring("tempo", build_spec()),
        &scope(),
        PROJECT,
        dir.path(),
    )
    .expect("the overlay renders");
    let service = &overlay["services"][object_name(RELEASE, AGENT, "tempo")];
    assert_eq!(service["image"], LOCKED_IMAGE, "{overlay}");
}

/// The skill tier. `skill_connector_plan` is what `start_skill_connectors`
/// iterates, and it hands each container its image -- the starter computes none
/// of its own, so the inline lock-first copy this replaces cannot come back
/// without deleting this call.
#[test]
fn the_skill_plan_starts_the_declared_image_despite_a_stale_lock_entry() {
    let plan = skill_connector_plan(&declaring("tempo", image_spec()), &lock_for("tempo"))
        .expect("the plan resolves");
    assert_eq!(
        plan,
        vec![("tempo".to_string(), DECLARED_IMAGE.to_string())]
    );
}

#[test]
fn the_skill_plan_starts_the_locked_image_for_a_build_connector() {
    let plan = skill_connector_plan(&declaring("tempo", build_spec()), &lock_for("tempo"))
        .expect("the plan resolves");
    assert_eq!(plan, vec![("tempo".to_string(), LOCKED_IMAGE.to_string())]);
}

/// Refused whole, before the first container starts, rather than half-started.
#[test]
fn the_skill_plan_refuses_a_build_connector_with_no_lock_entry() {
    let mut decl = declaring("tempo", build_spec());
    decl.connectors
        .insert("kubernetes".to_string(), image_spec());
    let error = skill_connector_plan(&decl, &empty_lock())
        .expect_err("an unbuilt `build:` connector refuses the boot");
    let (class, _fix) = classify(&error);
    assert_eq!(class, ExitClass::Usage, "{error:#}");
    assert!(format!("{error:#}").contains("curie build"), "{error:#}");
}

/// A connector Curie does not host has no image to resolve, and demanding one
/// would refuse a bundle that works.
#[test]
fn the_skill_plan_skips_a_connector_curie_does_not_host() {
    let remote = ConnectorSpecDecl {
        url: Some("https://mcp.example.com/mcp".to_string()),
        ..Default::default()
    };
    let plan =
        skill_connector_plan(&declaring("remote", remote), &empty_lock()).expect("nothing to run");
    assert!(plan.is_empty(), "{plan:?}");
}

// ─── The identity labels both emitters write ─────────────────────────────────

/// Teardown resolves containers by label, never by a generated file, so a
/// container that is not labeled at creation can never be reconciled. Both
/// emitters, one pair of labels.
#[test]
fn both_emitters_label_a_connector_with_its_agent_and_object_name() {
    let dir = TempDir::new().expect("tempdir");
    let object = object_name(RELEASE, AGENT, "tempo");

    let argv = ConnectorStartSpec::from_declaration(
        "tempo",
        &image_spec(),
        DECLARED_IMAGE,
        &scope(),
        "curie-skill-net",
        "curie-skill-abc123",
        dir.path(),
        &BTreeMap::new(),
    )
    .expect("the start spec renders")
    .run_args();
    let labels: Vec<String> = argv
        .windows(2)
        .filter(|w| w[0] == "--label")
        .map(|w| w[1].clone())
        .collect();
    assert!(
        labels.contains(&format!("{CONNECTOR_AGENT_LABEL_KEY}={AGENT}")),
        "{labels:?}"
    );
    assert!(
        labels.contains(&format!("{CONNECTOR_OBJECT_LABEL_KEY}={object}")),
        "{labels:?}"
    );

    let overlay = compose_overlay(
        &empty_lock(),
        &declaring("tempo", image_spec()),
        &scope(),
        PROJECT,
        dir.path(),
    )
    .expect("the overlay renders");
    let service_labels = &overlay["services"][&object]["labels"];
    assert_eq!(
        service_labels[CONNECTOR_AGENT_LABEL_KEY], AGENT,
        "{overlay}"
    );
    assert_eq!(
        service_labels[CONNECTOR_OBJECT_LABEL_KEY], object,
        "{overlay}"
    );
}

// ─── The reap's scope ────────────────────────────────────────────────────────

/// The selector is the safety property. All three filters, and nothing that
/// reaches beyond them: this compose project also holds the api/worker stack
/// and every other locally deployed agent's connectors.
#[test]
fn the_reap_selects_one_agents_connectors_in_a_shared_compose_project() {
    let argv = agent_connectors_argv(PROJECT, AGENT);
    let joined = argv.join(" ");
    assert!(
        argv.contains(&format!("label={CONNECTOR_COMPONENT_LABEL}")),
        "without the component filter the reap can see the api/worker stack: {joined}"
    );
    assert!(
        argv.contains(&format!("label={}", connector_project_label(PROJECT))),
        "{joined}"
    );
    assert!(
        argv.contains(&format!("label={CONNECTOR_AGENT_LABEL_KEY}={AGENT}")),
        "without the agent filter the reap can see another agent's connectors: {joined}"
    );
    assert!(
        !joined.contains("--remove-orphans"),
        "--remove-orphans in this project removes the platform itself: {joined}"
    );
    assert!(
        joined.contains(CONNECTOR_OBJECT_LABEL_KEY),
        "the listing must report the object name the desired set is compared against: {joined}"
    );
}

/// The selector and the emitters must agree on the label, or the reap silently
/// selects nothing and every dropped connector leaks.
#[test]
fn the_reap_filters_on_the_label_the_emitters_write() {
    let written = connector_identity_labels(AGENT, "ignored");
    let (key, value) = &written[0];
    assert!(
        agent_connectors_argv(PROJECT, AGENT).contains(&format!("label={key}={value}")),
        "{written:?}"
    );
}

/// The decision itself: dropped connectors go, declared ones stay.
#[test]
fn a_redeploy_removes_only_the_connectors_the_bundle_dropped() {
    let running = vec![
        RunningConnector {
            id: "aaa".to_string(),
            object: object_name(RELEASE, AGENT, "tempo"),
        },
        RunningConnector {
            id: "bbb".to_string(),
            object: object_name(RELEASE, AGENT, "kubernetes"),
        },
    ];
    let desired = BTreeSet::from([object_name(RELEASE, AGENT, "tempo")]);
    assert_eq!(
        connectors_to_reap(&running, &desired),
        vec!["bbb".to_string()],
        "the connector the new bundle still declares must keep running"
    );
}

/// The zero case, which is the one the early return used to skip entirely.
#[test]
fn a_redeploy_to_zero_connectors_removes_every_one_of_this_agents() {
    let running = vec![
        RunningConnector {
            id: "aaa".to_string(),
            object: object_name(RELEASE, AGENT, "tempo"),
        },
        RunningConnector {
            id: "bbb".to_string(),
            object: object_name(RELEASE, AGENT, "kubernetes"),
        },
    ];
    assert_eq!(
        connectors_to_reap(&running, &BTreeSet::new()),
        vec!["aaa".to_string(), "bbb".to_string()]
    );
}

/// An unchanged redeploy removes nothing, so a `local deploy` that changed no
/// connector does not restart them.
#[test]
fn an_unchanged_redeploy_removes_nothing() {
    let running = vec![RunningConnector {
        id: "aaa".to_string(),
        object: object_name(RELEASE, AGENT, "tempo"),
    }];
    let desired = BTreeSet::from([object_name(RELEASE, AGENT, "tempo")]);
    assert!(connectors_to_reap(&running, &desired).is_empty());
}

/// The listing's shape, including a container an older CLI started before the
/// object label existed: it matches no desired name and is removed, and the
/// bring-up that follows recreates it labeled if the bundle still declares it.
#[test]
fn a_container_with_no_object_label_is_listed_and_reaped() {
    let object = object_name(RELEASE, AGENT, "tempo");
    let listing = format!("aaa\t{object}\nbbb\t\n");
    let running = parse_agent_connectors(&listing);
    assert_eq!(
        running,
        vec![
            RunningConnector {
                id: "aaa".to_string(),
                object: object.clone(),
            },
            RunningConnector {
                id: "bbb".to_string(),
                object: String::new(),
            },
        ]
    );
    assert_eq!(
        connectors_to_reap(&running, &BTreeSet::from([object])),
        vec!["bbb".to_string()]
    );
}
