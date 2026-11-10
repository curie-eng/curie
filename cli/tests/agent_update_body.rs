// The typed `PATCH /agents/{id}` body (issue #3834) serializes the three
// states the API distinguishes. The source of the distinction is
// `apps/api/src/curie_api/routers/agents.py` `update_agent`, which reads
// `data.model_fields_set`: an ABSENT key leaves the field unchanged, an
// explicit JSON `null` clears the override back to the platform default, and a
// value sets it. Reading `None` alone cannot tell absent from null, so the CLI
// mirror carries clearable fields as `Option<Option<T>>`:
//   None           -> key omitted (unchanged)
//   Some(None)     -> `null`      (clear)
//   Some(Some(v))  -> value       (set)
// Non-nullable PATCH fields are `Option<T>`: omitted when `None`, never `null`.

use curie::api_requests::AgentUpdate;
use serde_json::json;

fn wire(body: &AgentUpdate) -> serde_json::Value {
    serde_json::to_value(body).expect("AgentUpdate serializes")
}

#[test]
fn default_update_is_an_empty_object() {
    assert_eq!(wire(&AgentUpdate::default()), json!({}));
}

#[test]
fn model_absent_null_and_value_are_distinct_on_the_wire() {
    let absent = AgentUpdate {
        model: None,
        ..Default::default()
    };
    assert_eq!(wire(&absent), json!({}));

    let clear = AgentUpdate {
        model: Some(None),
        ..Default::default()
    };
    assert_eq!(wire(&clear), json!({"model": null}));

    let set = AgentUpdate {
        model: Some(Some("openrouter/some-model".into())),
        ..Default::default()
    };
    assert_eq!(wire(&set), json!({"model": "openrouter/some-model"}));
}

#[test]
fn thinking_absent_null_and_value_are_distinct_on_the_wire() {
    let absent = AgentUpdate {
        thinking: None,
        ..Default::default()
    };
    assert_eq!(wire(&absent), json!({}));

    let clear = AgentUpdate {
        thinking: Some(None),
        ..Default::default()
    };
    assert_eq!(wire(&clear), json!({"thinking": null}));

    let set = AgentUpdate {
        thinking: Some(Some("high".into())),
        ..Default::default()
    };
    assert_eq!(wire(&set), json!({"thinking": "high"}));
}

#[test]
fn execution_deadline_absent_null_and_value_are_distinct_on_the_wire() {
    let absent = AgentUpdate {
        execution_deadline_seconds: None,
        ..Default::default()
    };
    assert_eq!(wire(&absent), json!({}));

    let clear = AgentUpdate {
        execution_deadline_seconds: Some(None),
        ..Default::default()
    };
    assert_eq!(wire(&clear), json!({"execution_deadline_seconds": null}));

    let set = AgentUpdate {
        execution_deadline_seconds: Some(Some(900)),
        ..Default::default()
    };
    assert_eq!(wire(&set), json!({"execution_deadline_seconds": 900}));
}

#[test]
fn three_states_compose_in_one_body() {
    // Each field keeps its own state: clear one, set another, leave a third.
    let body = AgentUpdate {
        model: Some(None),
        thinking: Some(Some("low".into())),
        execution_deadline_seconds: None,
        ..Default::default()
    };
    assert_eq!(wire(&body), json!({"model": null, "thinking": "low"}));
}

#[test]
fn non_nullable_publication_draft_is_omitted_or_valued_never_null() {
    // The API refuses an explicit null `publication_draft`
    // (`_reject_null_publication_switches` in schemas/agents.py), so the mirror
    // is `Option<bool>`: absent when None, the bool when set, never `null`.
    let absent = AgentUpdate {
        publication_draft: None,
        ..Default::default()
    };
    let absent_wire = wire(&absent);
    assert_eq!(absent_wire, json!({}));
    assert!(absent_wire.get("publication_draft").is_none());

    let set = AgentUpdate {
        publication_draft: Some(false),
        ..Default::default()
    };
    assert_eq!(wire(&set), json!({"publication_draft": false}));
}
