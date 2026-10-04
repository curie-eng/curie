// Field-parity gate (issue #691): every CLI struct that mirrors a platform API
// model must carry that model's fields, and every deliberate omission must be
// declared and justified in `cli/api-mirrors.json`. This binary asserts that
// invariant on the real tree and drives the real comparator over drifted
// fixtures to prove it rejects each violation class by execution (AC5).
//
// ─── Shared comparator contract (Stream A implements this VERBATIM) ──────────
// The helper lives at `cli/tests/support/field_parity.rs`, reached below via a
// `#[path = ...]` include. Its ENTIRE public surface is one pure function plus
// one enum. Copy both exactly; tests assert on `Violation` variants + payload,
// never on message strings.
//
//   pub fn violations(
//       rust_src: &str,                  // contents of the Rust source to walk
//       openapi: &serde_json::Value,     // parsed OpenAPI doc (components.schemas)
//       manifest: &serde_json::Value,    // parsed api-mirrors.json manifest
//   ) -> Vec<Violation>;
//
//   #[derive(Debug, Clone, PartialEq, Eq)]
//   pub enum Violation {
//       /// Schema defines a property the struct neither carries nor allowlists.
//       MissingField { struct_name: String, schema: String, field: String },
//       /// Struct carries a wire field the schema does not define.
//       UnknownField { struct_name: String, schema: String, field: String },
//       /// A `Deserialize` struct in the source is in neither `mirrors` nor `non_mirrors`.
//       UndeclaredStruct { struct_name: String },
//       /// A manifest entry names a struct the source walk never found.
//       StructNotFound { struct_name: String },
//       /// A dishonest omission: struct actually carries the field, the schema no
//       /// longer has it, or the omission's `why` is blank/missing.
//       StaleOmission { struct_name: String, schema: String, field: String },
//       /// A manifest `schema` is absent from `components.schemas`.
//       SchemaNotFound { struct_name: String, schema: String },
//       /// The schema (object-level `allOf`/`anyOf`) or the struct (a
//       /// `#[serde(flatten)]` field) cannot be decomposed field-by-field.
//       UnsupportedShape { struct_name: String, schema: String },
//       /// Two `Deserialize` structs share one bare name; the manifest keys by
//       /// name, so only the first is ever field-checked.
//       DuplicateStruct { struct_name: String },
//       /// A manifest entry lacks a required key (`struct`/`schema`), so its
//       /// struct would silently escape field comparison.
//       MalformedManifestEntry { detail: String },
//       // ── Issue #3834 additions ──
//       /// Required Rust field for an API field that is optional or nullable.
//       OptionalityMismatch { struct_name: String, schema: String, field: String },
//       /// A platform send with an untyped or `json!` body, or not in `requests`.
//       UnclassifiedRequest { function: String },
//       /// The declared operation's requestBody schema is not the mirror's schema.
//       RequestSchemaMismatch { function: String, .. },
//       /// Source-derived method or normalized path differs from the declaration.
//       RequestRouteMismatch { function: String, .. },
//       /// Method, `format!` URL literal, or turbofish body cannot be extracted.
//       UnverifiableRequest { function: String },
//   }
//
//   pub fn request_violations(
//       api_src: &str,                   // cli/src/api.rs (the sends)
//       requests_src: &str,              // cli/src/api_requests.rs (the bodies)
//       openapi: &serde_json::Value,
//       manifest: &serde_json::Value,    // `request_mirrors` + `requests`
//   ) -> Vec<Violation>;
//
// Precedence rules Stream A MUST honor so each fixture triggers one variant:
//  - A struct with any `#[serde(flatten)]` field => emit `UnsupportedShape` for
//    that struct and do NOT run field comparison on it.
//  - An object-level `allOf`/`anyOf` schema => `UnsupportedShape`; skip field
//    comparison (a naive read would see zero required fields and pass silently).
//  - An omission entry suppresses `MissingField` for its field regardless of the
//    `why` content; a blank/missing `why` is reported as `StaleOmission` (NOT
//    `MissingField`).
//  - Wire name governs coverage: `#[serde(rename = "x")]` maps the field to `x`;
//    `rename_all` applies the container transform; a bare field uses its ident.
//  - The walk recognizes both derive spellings (`Deserialize` and
//    `serde::Deserialize`) and recurses into items nested in fn/impl bodies.
// ─────────────────────────────────────────────────────────────────────────────

#[path = "support/field_parity.rs"]
mod field_parity;

use field_parity::{violations, Violation};
use serde_json::Value;

// ─── Loaders ─────────────────────────────────────────────────────────────────

fn repo_text(rel: &str) -> String {
    let path = format!("{}/../{}", env!("CARGO_MANIFEST_DIR"), rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn repo_json(rel: &str) -> Value {
    let raw = repo_text(rel);
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {rel}: {e}"))
}

fn fixture_text(name: &str) -> String {
    let path = format!(
        "{}/tests/data/field-parity/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

fn fixture_json(name: &str) -> Value {
    let raw = fixture_text(name);
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"))
}

/// The drifted fixture triple that drives every rejection case.
fn fixtures() -> (String, Value, Value) {
    (
        fixture_text("drifted-api.rs"),
        fixture_json("drifted-openapi.json"),
        fixture_json("drifted-mirrors.json"),
    )
}

// ─── Payload matchers (variant + payload, never message strings) ─────────────

fn has_missing_field(vs: &[Violation], struct_name: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::MissingField { struct_name: s, field: f, .. }
            if s == struct_name && f == field)
    })
}

fn has_unknown_field(vs: &[Violation], struct_name: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::UnknownField { struct_name: s, field: f, .. }
            if s == struct_name && f == field)
    })
}

fn has_stale_omission(vs: &[Violation], struct_name: &str, field: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::StaleOmission { struct_name: s, field: f, .. }
            if s == struct_name && f == field)
    })
}

fn has_undeclared_struct(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter()
        .any(|v| matches!(v, Violation::UndeclaredStruct { struct_name: s } if s == struct_name))
}

fn has_struct_not_found(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter()
        .any(|v| matches!(v, Violation::StructNotFound { struct_name: s } if s == struct_name))
}

fn has_schema_not_found(vs: &[Violation], struct_name: &str, schema: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::SchemaNotFound { struct_name: s, schema: sc }
            if s == struct_name && sc == schema)
    })
}

fn has_unsupported_shape(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter().any(
        |v| matches!(v, Violation::UnsupportedShape { struct_name: s, .. } if s == struct_name),
    )
}

fn has_duplicate_struct(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter()
        .any(|v| matches!(v, Violation::DuplicateStruct { struct_name: s } if s == struct_name))
}

fn has_malformed_manifest_entry(vs: &[Violation], needle: &str) -> bool {
    vs.iter().any(
        |v| matches!(v, Violation::MalformedManifestEntry { detail } if detail.contains(needle)),
    )
}

fn mentions_struct(vs: &[Violation], struct_name: &str) -> bool {
    vs.iter().any(|v| match v {
        Violation::MissingField { struct_name: s, .. }
        | Violation::UnknownField { struct_name: s, .. }
        | Violation::UndeclaredStruct { struct_name: s }
        | Violation::StructNotFound { struct_name: s }
        | Violation::StaleOmission { struct_name: s, .. }
        | Violation::SchemaNotFound { struct_name: s, .. }
        | Violation::DuplicateStruct { struct_name: s }
        | Violation::UnsupportedShape { struct_name: s, .. }
        | Violation::OptionalityMismatch { struct_name: s, .. } => s == struct_name,
        // Request-binding variants are keyed by function, not struct.
        _ => false,
    })
}

// ─── Real-tree assertions (AC1, AC3) ─────────────────────────────────────────

#[test]
fn real_tree_has_no_field_parity_violations() {
    let src = repo_text("cli/src/api.rs");
    let openapi = repo_json("apps/api/openapi.json");
    let manifest = repo_json("cli/api-mirrors.json");

    let vs = violations(&src, &openapi, &manifest);
    assert!(
        vs.is_empty(),
        "cli/src/api.rs has drifted from apps/api/openapi.json. Each entry below \
         is fixed by either adding the field to the struct or declaring the \
         omission (with a justification) in cli/api-mirrors.json:\n{vs:#?}"
    );
}

#[test]
fn approval_routes_declare_separate_response_and_write_mirrors() {
    let manifest = repo_json("cli/api-mirrors.json");
    let mirrors = manifest["mirrors"].as_array().expect("mirrors array");
    let schema_for = |name: &str| {
        mirrors
            .iter()
            .find(|entry| entry["struct"] == name)
            .and_then(|entry| entry["schema"].as_str())
    };

    assert_eq!(
        schema_for("ApprovalRouteBindingResponse"),
        Some("ApprovalRouteBindingOut"),
        "the tolerant/redacted GET model needs its own API mirror"
    );
    assert_eq!(
        schema_for("ApprovalRouteBindingWrite"),
        Some("ApprovalRouteBinding"),
        "the strict PATCH model must mirror the full stored binding"
    );
    assert_eq!(
        schema_for("NotificationTargetWrite"),
        Some("ApprovalNotificationTarget"),
        "notification endpoint and adapter belong only to the write mirror"
    );
}

#[test]
fn version_struct_carries_the_full_version_out() {
    // The issue's named drift: `Version` must cover `agent_id` and `bundle_ref`.
    // Checked independently of the generic sweep so a manifest mistake cannot
    // hide it.
    let src = repo_text("cli/src/api.rs");
    let openapi = repo_json("apps/api/openapi.json");
    let manifest = repo_json("cli/api-mirrors.json");

    let vs = violations(&src, &openapi, &manifest);
    let version_violations: Vec<&Violation> = vs
        .iter()
        .filter(|v| match v {
            Violation::MissingField { struct_name, .. }
            | Violation::UnknownField { struct_name, .. }
            | Violation::UndeclaredStruct { struct_name }
            | Violation::StructNotFound { struct_name }
            | Violation::StaleOmission { struct_name, .. }
            | Violation::SchemaNotFound { struct_name, .. }
            | Violation::DuplicateStruct { struct_name }
            | Violation::UnsupportedShape { struct_name, .. }
            | Violation::OptionalityMismatch { struct_name, .. } => struct_name == "Version",
            _ => false,
        })
        .collect();
    assert!(
        version_violations.is_empty(),
        "Version does not fully mirror VersionOut (expected agent_id + bundle_ref \
         covered, zero omissions):\n{version_violations:#?}"
    );
}

// ─── Guard-rejects-a-violating-input demonstrations (AC5) ────────────────────

#[test]
fn rejects_a_struct_missing_a_required_schema_field() {
    // case 1
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_missing_field(&vs, "MissingFieldMirror", "dropped"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_stale_allowlist_entry() {
    // case 2: an omission for a field the struct actually carries.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_stale_omission(&vs, "StaleCarriesMirror", "beta"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_allowlist_entry_for_a_field_the_schema_no_longer_has() {
    // case 3
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_stale_omission(&vs, "StaleShrunkMirror", "delta"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_undeclared_deserialize_struct() {
    // case 4: D2's teeth — a Deserialize struct in neither list.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_undeclared_struct(&vs, "UndeclaredMirror"), "{vs:#?}");
}

#[test]
fn rejects_a_struct_field_absent_from_the_schema() {
    // case 5: a CLI field with no API field behind it is always a bug.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_unknown_field(&vs, "UnknownFieldMirror", "phantom"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_missing_schema() {
    // case 6: a named schema absent from components.schemas -> not a skip.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_schema_not_found(&vs, "MissingSchemaMirror", "NoSuchSchema"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_unsupported_schema_shape() {
    // case 7: an object-level allOf schema must fail closed, never pass silently.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_unsupported_shape(&vs, "AllOfMirror"), "{vs:#?}");
}

#[test]
fn rejects_serde_flatten() {
    // case 8: flatten makes the wire-name mapping non-local -> UnsupportedShape.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_unsupported_shape(&vs, "FlattenMirror"), "{vs:#?}");
}

#[test]
fn rejects_an_empty_omission_justification() {
    // case 9: an omission with a blank `why` is not a justified omission.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_stale_omission(&vs, "BlankWhyMirror", "q"), "{vs:#?}");
}

#[test]
fn honors_serde_rename_on_both_sides() {
    // case 10: paired positive/negative — proves the gate reads the WIRE name.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    // Positive: the renamed field covers the schema -> no violation for it.
    assert!(
        !mentions_struct(&vs, "RenamePositive"),
        "renamed field should satisfy coverage:\n{vs:#?}"
    );
    // Negative: the same ident without the rename leaves `fooBar` uncovered.
    assert!(
        has_missing_field(&vs, "RenameNegative", "fooBar"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_an_undeclared_deserialize_struct_inside_a_fn_body() {
    // case 11: the nested-item walk proof — a `serde::Deserialize` struct declared
    // inside an impl-method body, absent from the manifest.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_undeclared_struct(&vs, "NestedUndeclared"), "{vs:#?}");
}

#[test]
fn rejects_a_manifest_entry_for_a_struct_the_source_does_not_have() {
    // case 12: the symmetric fail-closed twin of case 6 — a dangling manifest
    // entry makes a walk defect observable instead of silently shrinking the set.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_struct_not_found(&vs, "GhostStruct"), "{vs:#?}");
}

#[test]
fn rejects_a_skip_deserializing_field_as_non_coverage() {
    // case 13: `#[serde(skip_deserializing)]` drops the field from the wire (the
    // decoder fills it from Default), so it must NOT satisfy coverage for the
    // schema property it names -> MissingField.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_missing_field(&vs, "SkipDeserMirror", "discarded"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_a_duplicated_struct_name() {
    // case 14: two `Deserialize` structs share one bare name; the manifest keys by
    // name so the second is never field-checked -> DuplicateStruct, fail-closed.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(has_duplicate_struct(&vs, "DupNameMirror"), "{vs:#?}");
}

#[test]
fn rejects_a_manifest_entry_missing_a_required_key() {
    // case 15: a `mirrors` entry lacking `schema` would silently skip field
    // comparison for its struct -> MalformedManifestEntry, fail-closed.
    let (src, oa, mf) = fixtures();
    let vs = violations(&src, &oa, &mf);
    assert!(
        has_malformed_manifest_entry(&vs, "MalformedEntryMirror"),
        "{vs:#?}"
    );
}

#[test]
fn rejects_required_cli_fields_for_optional_or_nullable_api_fields() {
    let src = "#[derive(Deserialize)] struct Mirror { omitted: String, nullable: String }";
    let openapi = serde_json::json!({"components":{"schemas":{"Model":{
        "type":"object", "properties":{
            "omitted":{"type":"string"},
            "nullable":{"anyOf":[{"type":"string"},{"type":"null"}]}
        }, "required":["nullable"]
    }}}});
    let manifest = serde_json::json!({"mirrors":[{"struct":"Mirror","schema":"Model"}]});
    let vs = violations(src, &openapi, &manifest);
    for field in ["omitted", "nullable"] {
        assert!(vs.iter().any(|v| matches!(v, Violation::OptionalityMismatch {
            struct_name, field: found, ..
        } if struct_name == "Mirror" && found == field)), "{vs:#?}");
    }
}

#[test]
fn honors_option_default_and_referenced_nullability() {
    let src = r#"#[derive(Deserialize)] struct Mirror {
        omitted: Option<String>,
        #[serde(default)] count: u32,
        nullable: Option<String>,
    }"#;
    let openapi = serde_json::json!({"components":{"schemas":{
        "NullableText":{"anyOf":[{"type":"string"},{"type":"null"}]},
        "Model":{"type":"object", "properties":{
            "omitted":{"type":"string"}, "count":{"type":"integer"},
            "nullable":{"$ref":"#/components/schemas/NullableText"}
        }, "required":["nullable"]}
    }}});
    let manifest = serde_json::json!({"mirrors":[{"struct":"Mirror","schema":"Model"}]});
    assert!(violations(src, &openapi, &manifest).is_empty());
}

#[test]
fn serde_default_does_not_accept_api_null() {
    let src = "#[derive(Deserialize)] struct Mirror { #[serde(default)] value: String }";
    let openapi = serde_json::json!({"components":{"schemas":{"Model":{
        "type":"object", "properties":{"value":{"type":["string","null"]}},
        "required":["value"]
    }}}});
    let manifest = serde_json::json!({"mirrors":[{"struct":"Mirror","schema":"Model"}]});
    assert!(violations(src, &openapi, &manifest).iter().any(|v| matches!(v,
        Violation::OptionalityMismatch { field, .. } if field == "value")));
}

#[test]
fn real_tree_request_bodies_match_openapi_operations() {
    let vs = field_parity::request_violations(
        &repo_text("cli/src/api.rs"),
        &repo_text("cli/src/api_requests.rs"),
        &repo_json("apps/api/openapi.json"),
        &repo_json("cli/api-mirrors.json"),
    );
    assert!(vs.is_empty(), "CLI request bodies drifted from OpenAPI: {vs:#?}");
}

// ─── Request-gate fixtures (Revision 2: binding sends to HTTP operations) ────
//
// `request_violations(api_src, requests_src, openapi, manifest)` reads every
// platform send in `api_src` in the production shape
// `self.http.<method>(format!("{}<path>", self.base_url, ..)).header(..).json::<T>(&body)`
// and binds it to the manifest's `requests` entry for the enclosing fn. The
// source-derived method and path (placeholders normalized to `{}` on both
// sides) must equal the declared method and path, and that OpenAPI operation's
// requestBody schema must equal the `request_mirrors` schema for the struct.

/// An OpenAPI document with one operation (`method` on `path`) whose JSON
/// requestBody references `components.schemas.<body_schema>`.
fn request_openapi(path: &str, method: &str, body_schema: &str, schemas: Value) -> Value {
    let mut operation = serde_json::Map::new();
    operation.insert(
        method.to_string(),
        serde_json::json!({"requestBody":{"content":{"application/json":{
            "schema":{"$ref": format!("#/components/schemas/{body_schema}")}
        }}}}),
    );
    let mut paths = serde_json::Map::new();
    paths.insert(path.to_string(), Value::Object(operation));
    serde_json::json!({"paths": Value::Object(paths), "components": {"schemas": schemas}})
}

fn request_manifest(function: &str, method: &str, path: &str, schema: &str) -> Value {
    serde_json::json!({
        "request_mirrors": [{"struct": "Body", "schema": schema}],
        "requests": [{"function": function, "struct": "Body", "method": method, "path": path}]
    })
}

fn create_schema() -> Value {
    serde_json::json!({"Create":{"type":"object",
        "properties":{"name":{"type":"string"}},"required":["name"]}})
}

fn has_route_mismatch(vs: &[Violation], function_name: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::RequestRouteMismatch { function, .. } if function == function_name)
    })
}

fn has_unverifiable_request(vs: &[Violation], function_name: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::UnverifiableRequest { function } if function == function_name)
    })
}

fn has_unclassified_request(vs: &[Violation], function_name: &str) -> bool {
    vs.iter().any(|v| {
        matches!(v, Violation::UnclassifiedRequest { function } if function == function_name)
    })
}

#[test]
fn request_gate_rejects_missing_required_fields_and_unclassified_sends() {
    let requests = "#[derive(Serialize, Deserialize)] struct Body { typo: String }";
    let src = r#"impl Client {
        fn create(&self) {
            self.http
                .post(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(&body);
        }
        fn new_send(&self) {
            self.http
                .post(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json(&body);
        }
    }"#;
    let openapi = request_openapi("/items", "post", "Create", create_schema());
    let manifest = request_manifest("create", "post", "/items", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(has_missing_field(&vs, "Body", "name"), "{vs:#?}");
    assert!(has_unknown_field(&vs, "Body", "typo"), "{vs:#?}");
    assert!(has_unclassified_request(&vs, "new_send"), "{vs:#?}");
    // The declared, real-shape send is bound correctly; only its body drifted.
    assert!(!has_route_mismatch(&vs, "create"), "{vs:#?}");
    assert!(!has_unverifiable_request(&vs, "create"), "{vs:#?}");
}

#[test]
fn request_gate_rejects_wrong_operation_schema_and_dynamic_json() {
    let requests = "#[derive(Serialize, Deserialize)] struct Body { name: String }";
    let mut schemas = create_schema();
    schemas["Other"] = serde_json::json!({"type":"object",
        "properties":{"name":{"type":"string"}},"required":["name"]});
    let openapi = request_openapi("/items", "post", "Create", schemas);
    // Declared mirror schema `Other` is not the operation's requestBody `Create`.
    let manifest = request_manifest("create", "post", "/items", "Other");
    let src = r#"impl Client {
        fn create(&self) {
            self.http
                .post(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(&body);
        }
    }"#;
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(
        vs.iter().any(|v| matches!(v, Violation::RequestSchemaMismatch { function, .. }
            if function == "create")),
        "{vs:#?}"
    );

    // A `json!` body on a declared platform send is rejected even with the
    // manifest pointing at the correct schema.
    let manifest = request_manifest("create", "post", "/items", "Create");
    let src = r#"impl Client {
        fn create(&self) {
            self.http
                .post(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json(&json!({"name": name}));
        }
    }"#;
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(has_unclassified_request(&vs, "create"), "{vs:#?}");
}

#[test]
fn request_gate_rejects_a_send_whose_method_differs_from_the_declaration() {
    // Production mutated post -> put; manifest and OpenAPI still say post.
    let requests = "#[derive(Serialize, Deserialize)] struct Body { name: String }";
    let src = r#"impl Client {
        fn create(&self) {
            self.http
                .put(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(&body);
        }
    }"#;
    let openapi = request_openapi("/items", "post", "Create", create_schema());
    let manifest = request_manifest("create", "post", "/items", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(has_route_mismatch(&vs, "create"), "{vs:#?}");
}

#[test]
fn request_gate_rejects_a_send_whose_path_differs_from_the_declaration() {
    // Production mutated the URL literal; manifest and OpenAPI still say /items.
    let requests = "#[derive(Serialize, Deserialize)] struct Body { name: String }";
    let src = r#"impl Client {
        fn create(&self) {
            self.http
                .post(format!("{}/things", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(&body);
        }
    }"#;
    let openapi = request_openapi("/items", "post", "Create", create_schema());
    let manifest = request_manifest("create", "post", "/items", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(has_route_mismatch(&vs, "create"), "{vs:#?}");
}

#[test]
fn request_gate_normalizes_path_placeholders_and_passes_a_correct_send() {
    // Liveness: `{item}` in the source and `{item_id}` in OpenAPI are the same
    // segment once both normalize to `{}`; a correct typed body is clean.
    let requests = "#[derive(Serialize, Deserialize)] struct Body { name: String }";
    let src = r#"impl Client {
        fn update(&self, item: &str, body: &Body) {
            self.http
                .patch(format!("{}/items/{item}", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(body);
        }
    }"#;
    let openapi = request_openapi("/items/{item_id}", "patch", "Create", create_schema());
    let manifest = request_manifest("update", "patch", "/items/{item_id}", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(vs.is_empty(), "a correct real-shape send must pass: {vs:#?}");
}

#[test]
fn request_gate_rejects_an_opaque_url_as_unverifiable() {
    // The URL is not a `format!` literal, so method+path cannot be bound to the
    // declaration. Unknown shapes are rejected, never trusted.
    let requests = "#[derive(Serialize, Deserialize)] struct Body { name: String }";
    let src = r#"impl Client {
        fn create(&self, url: String, body: &Body) {
            self.http
                .post(url)
                .header("X-API-Key", &self.api_key)
                .json::<Body>(body);
        }
    }"#;
    let openapi = request_openapi("/items", "post", "Create", create_schema());
    let manifest = request_manifest("create", "post", "/items", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(has_unverifiable_request(&vs, "create"), "{vs:#?}");
}

#[test]
fn request_gate_rejects_a_required_cli_field_for_an_optional_api_field() {
    // A required (non-Option) Rust request field always sends the key, so it
    // cannot honestly mirror an API field the request may omit.
    let requests = "#[derive(Debug, Default, Serialize)] struct Body { name: String, note: String }";
    let src = r#"impl Client {
        fn create(&self, body: &Body) {
            self.http
                .post(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(body);
        }
    }"#;
    let schemas = serde_json::json!({"Create":{"type":"object","properties":{
        "name":{"type":"string"},
        "note":{"type":"string"}
    },"required":["name"]}});
    let openapi = request_openapi("/items", "post", "Create", schemas);
    let manifest = request_manifest("create", "post", "/items", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(
        vs.iter().any(|v| matches!(v, Violation::OptionalityMismatch { struct_name, field, .. }
            if struct_name == "Body" && field == "note")),
        "{vs:#?}"
    );
    // The required-on-both-sides field is not flagged.
    assert!(
        !vs.iter().any(|v| matches!(v, Violation::OptionalityMismatch { field, .. }
            if field == "name")),
        "{vs:#?}"
    );
}

#[test]
fn request_gate_accepts_skipped_option_and_three_state_nullable_fields() {
    // Liveness: `Option<T>` + skip_serializing_if mirrors an optional field;
    // `Option<Option<T>>` + skip_serializing_if mirrors an optional NULLABLE
    // field (absent = unchanged, null = clear, value = set).
    let requests = r#"#[derive(Debug, Default, Serialize)] struct Body {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<Option<String>>,
    }"#;
    let src = r#"impl Client {
        fn create(&self, body: &Body) {
            self.http
                .post(format!("{}/items", self.base_url))
                .header("X-API-Key", &self.api_key)
                .json::<Body>(body);
        }
    }"#;
    let schemas = serde_json::json!({"Create":{"type":"object","properties":{
        "name":{"type":"string"},
        "note":{"type":"string"},
        "model":{"anyOf":[{"type":"string"},{"type":"null"}]}
    },"required":["name"]}});
    let openapi = request_openapi("/items", "post", "Create", schemas);
    let manifest = request_manifest("create", "post", "/items", "Create");
    let vs = field_parity::request_violations(src, requests, &openapi, &manifest);
    assert!(vs.is_empty(), "optional and nullable request fields must pass: {vs:#?}");
}
