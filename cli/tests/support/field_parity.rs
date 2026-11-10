//! Field-parity comparator for the `--json` gate (issue #691) and the typed
//! request gate (issue #3834).
//!
//! Pure, side-effect-free core of the gate: given the text of a Rust source
//! (`cli/src/api.rs` on the real tree, a fixture otherwise), a parsed OpenAPI
//! doc, and a parsed `api-mirrors.json` manifest, return every way a mirror
//! struct has drifted from the API model it declares it mirrors. The gate test
//! (`cli/tests/api_field_parity.rs`) asserts on the returned `Violation`
//! variants; this module never prints or panics on drift.
//!
//! The module is shared by three test binaries (`api_field_parity`,
//! `plugin_format_field_parity`, `api_emit_parity`), each of which uses a
//! different subset of it; `request_violations` carries the one `dead_code`
//! allowance, for the binaries that run only the response gate.
//!
//! Design notes (kept here, out of the terse return):
//!
//! * The response inventory is EVERY `Deserialize`-deriving struct in the
//!   source, found via a `syn::visit::Visit` walk. `visit` (not a flat
//!   `File.items` loop) is what reaches structs declared inside fn / impl-method
//!   bodies — the real tree has exactly one, `serde::Deserialize struct
//!   BundleFiles` inside `ApiClient::bundle_files`. The walk recognizes both the
//!   bare `Deserialize` and qualified `serde::Deserialize` derive spellings (last
//!   path segment == `Deserialize`), and deliberately does NOT match `Serialize`.
//! * Wire name governs coverage, not the Rust ident: `#[serde(rename="x")]`,
//!   container `#[serde(rename_all=...)]` are applied; skips are directional:
//!   response mirrors drop a field on `skip` or `skip_deserializing`, request
//!   mirrors on `skip` or `skip_serializing`, and the other directional skip keeps
//!   the field on the wire; `#[serde(alias=...)]` is an ADDITIONAL accepted name so
//!   it never counts as coverage. `#[serde(default)]` and `skip_serializing_if`
//!   are irrelevant to coverage; they matter only to optionality (below).
//! * Fail-closed shapes: a `#[serde(flatten)]` field or an object-level
//!   `allOf`/`anyOf`/`oneOf` schema cannot be decomposed field-by-field, so the
//!   struct yields `UnsupportedShape` and is NOT read as zero-required (which
//!   would silently pass). On request mirrors the same holds for a kept field
//!   whose `skip_serializing_if` is anything but exactly `"Option::is_none"` on an
//!   `Option<..>` field: the gate cannot know when such a key is omitted.
//! * An omission entry always suppresses `MissingField` for its field; a
//!   dishonest omission (blank/missing `why`, or a field the struct actually
//!   carries, or a field the schema no longer defines) additionally yields
//!   `StaleOmission`.
//!
//! Optionality (issue #3834). An API property is OPTIONAL when it is absent
//! from the schema's `required` list, and NULLABLE when its schema admits
//! `null`: an `anyOf`/`oneOf` member that is nullable, a `type` of `"null"` or a
//! `type` array containing it, `nullable: true`, or a `$ref` to a nullable
//! component (followed recursively). A Rust field is REQUIRED when it is not an
//! `Option<..>` and carries no serde knob that relaxes it.
//!
//! * Response mirrors (`Deserialize`): `Option<T>` accepts both omission and
//!   null. `#[serde(default)]` (field or container) accepts omission but NOT
//!   null. `serde_json::Value` accepts null but not omission. A field that cannot
//!   accept what the API may legally send (an omitted optional key, or a null)
//!   is `OptionalityMismatch`: decoding a valid API response would fail.
//! * Request mirrors (`Serialize`): a field may OMIT its key only through
//!   `skip_serializing_if`, and may SEND null when it is `Option<T>` without that
//!   skip, `Option<Option<T>>` (the PATCH three-state: `None` omits, `Some(None)`
//!   sends null, `Some(Some(v))` sends the value), or `serde_json::Value`. serde
//!   `default` affects decoding only, so it never relaxes a request field.
//!   `OptionalityMismatch` when a required Rust field mirrors an optional or
//!   nullable API field (the key is always sent, so the struct cannot honestly
//!   express the API's optional shape), when the field can send null to a
//!   non-nullable API field, or when it can omit a key the API requires.
//!
//! Request binding (issue #3834, `request_violations`). Every platform send in
//! the API client source must be a typed body on the production shape
//! `self.http.<method>(format!("{}<path>", self.base_url, ..)) ... .json::<T>(..)`.
//! From the SAME method chain as each one-argument `.json(..)` call the gate
//! reads the HTTP method ident, the `format!` literal and the turbofish body
//! type; `{...}` placeholders normalize to `{}` on both the source literal and
//! the OpenAPI path. The manifest's `requests` entry for the enclosing fn must
//! declare that method and path, name that struct, and the OpenAPI operation's
//! JSON requestBody schema must equal the struct's `request_mirrors` schema.
//!
//! * An untyped `.json(&value)` or a `json!` body is `UnclassifiedRequest`, as is
//!   a typed send whose fn has no `requests` entry.
//! * A send whose method, `format!` URL literal or `self.base_url` prefix cannot
//!   be read in that shape is `UnverifiableRequest`: unknown shapes are rejected,
//!   never trusted.
//! * A method or normalized path that differs from the declaration, a declared
//!   operation OpenAPI does not define, or a `requests` entry whose fn holds no
//!   send is `RequestRouteMismatch`.
//! * A turbofish struct other than the declared one, or an operation whose
//!   requestBody schema is not the declared mirror schema, is
//!   `RequestSchemaMismatch`.
//! * Zero-argument `.json()` calls decode responses and are not sends; items
//!   under `#[cfg(test)]` are not platform sends.
//! * Request mirror structs are the `Serialize`-deriving structs of the request
//!   source (`cli/src/api_requests.rs`), each of which must be declared in
//!   `request_mirrors`; a declared struct not found there is looked up among the
//!   API source's `Serialize` structs (a shared request/response DTO such as
//!   `BudgetConfig`). Request mirrors are field-compared exactly like response
//!   mirrors, with the request optionality rules above.

use std::collections::BTreeSet;

use serde_json::Value;
use syn::punctuated::Punctuated;
use syn::visit::Visit;

/// A single way a mirror struct has drifted from its declared API schema. The
/// gate test matches on these variants and their payload, never on messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// Schema defines a property the struct neither carries nor allowlists.
    MissingField {
        struct_name: String,
        schema: String,
        field: String,
    },
    /// Struct carries a wire field the schema does not define.
    UnknownField {
        struct_name: String,
        schema: String,
        field: String,
    },
    /// A `Deserialize` struct in the source is in neither `mirrors` nor `non_mirrors`
    /// (or a request source `Serialize` struct is not in `request_mirrors`).
    UndeclaredStruct { struct_name: String },
    /// A manifest entry names a struct the source walk never found.
    StructNotFound { struct_name: String },
    /// A dishonest omission: struct actually carries the field, the schema no
    /// longer has it, or the omission's `why` is blank/missing.
    StaleOmission {
        struct_name: String,
        schema: String,
        field: String,
    },
    /// A manifest `schema` is absent from `components.schemas`.
    SchemaNotFound { struct_name: String, schema: String },
    /// The schema (object-level `allOf`/`anyOf`) or the struct (a
    /// `#[serde(flatten)]` field) cannot be decomposed field-by-field.
    UnsupportedShape { struct_name: String, schema: String },
    /// The inventory holds more than one struct with this bare name; the
    /// manifest keys by name, so the second is never field-checked.
    DuplicateStruct { struct_name: String },
    /// A manifest entry is missing a required key, so its struct (or send)
    /// would silently escape comparison.
    MalformedManifestEntry { detail: String },
    /// The Rust field's optionality cannot honestly express the API field's:
    /// see the module doc for the response and request rules.
    OptionalityMismatch {
        struct_name: String,
        schema: String,
        field: String,
    },
    /// A platform send with an untyped or `json!` body, or not in `requests`.
    UnclassifiedRequest { function: String },
    /// The declared operation's requestBody schema is not the mirror's schema,
    /// or the send's turbofish names a different struct than the declaration.
    RequestSchemaMismatch {
        function: String,
        declared: String,
        found: String,
    },
    /// Source-derived method or normalized path differs from the declaration,
    /// the declared operation is not in OpenAPI, or the fn holds no send.
    RequestRouteMismatch {
        function: String,
        declared: String,
        found: String,
    },
    /// Method, `format!` URL literal, or turbofish body cannot be extracted.
    UnverifiableRequest { function: String },
}

/// Which serde derive a struct inventory selects, and therefore which
/// optionality rules apply to its fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// Response mirrors: the struct decodes what the API sends.
    Deserialize,
    /// Request mirrors: the struct encodes what the CLI sends.
    Serialize,
}

impl Direction {
    fn derive_name(self) -> &'static str {
        match self {
            Direction::Deserialize => "Deserialize",
            Direction::Serialize => "Serialize",
        }
    }
}

/// One wire field of a collected struct, with the serde facts optionality needs.
pub(crate) struct WireField {
    pub(crate) wire: String,
    /// `Option` nesting depth: 0 plain, 1 `Option<T>`, 2 `Option<Option<T>>`.
    pub(crate) option_depth: u8,
    /// `serde_json::Value`, which decodes `null` as `Value::Null`.
    pub(crate) json_value: bool,
    /// Field-level or container-level `#[serde(default)]`.
    pub(crate) has_default: bool,
    /// Field-level `skip_serializing_if`.
    pub(crate) skip_serializing_if: bool,
}

/// One struct recovered from the source: its bare name, the set of wire field
/// names (rename/rename_all/skip applied), the per-field serde facts, and
/// whether it has a `#[serde(flatten)]` field (which makes it undecomposable).
///
/// `pub(crate)` (rather than private): the emit-hop gate (#699,
/// `support/emit_parity.rs`) reuses this exact struct inventory as the source
/// of truth for a mirror struct's OWN fields, one hop upstream of its check.
pub(crate) struct CollectedStruct {
    pub(crate) name: String,
    pub(crate) wire_fields: BTreeSet<String>,
    pub(crate) fields: Vec<WireField>,
    pub(crate) has_flatten: bool,
    /// Request side only: a `skip_serializing_if` on a kept field that is not
    /// exactly `Option::is_none` on an `Option<..>` field, so the gate cannot
    /// tell when the key is omitted.
    pub(crate) has_unsupported_skip_if: bool,
}

/// serde attributes distilled from a field's (or container's) `#[serde(...)]`
/// attrs. Only the knobs that affect wire-name coverage or optionality are
/// captured.
#[derive(Default)]
struct SerdeAttrs {
    rename: Option<String>,
    rename_all: Option<String>,
    skip: bool,
    skip_serializing: bool,
    skip_deserializing: bool,
    flatten: bool,
    default: bool,
    /// The predicate string literal of `skip_serializing_if`, when present
    /// (empty when the value is not a string literal).
    skip_serializing_if: Option<String>,
}

impl SerdeAttrs {
    /// Does this field vanish from the wire in `direction`? `skip` drops both;
    /// `skip_serializing` only the request side; `skip_deserializing` only the
    /// response side.
    fn dropped_in(&self, direction: Direction) -> bool {
        self.skip
            || match direction {
                Direction::Serialize => self.skip_serializing,
                Direction::Deserialize => self.skip_deserializing,
            }
    }
}

fn parse_serde_attrs(attrs: &[syn::Attribute]) -> SerdeAttrs {
    let mut out = SerdeAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        // Errors are ignored: any serde attr shape this codebase does not use
        // would simply leave `out` at its earlier state; the real tree only uses
        // rename / rename_all / skip / flatten / default / skip_serializing_if /
        // alias / deny_unknown_fields / untagged, all handled below.
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                out.skip = true;
            } else if meta.path.is_ident("skip_serializing") {
                out.skip_serializing = true;
            } else if meta.path.is_ident("skip_deserializing") {
                out.skip_deserializing = true;
            } else if meta.path.is_ident("flatten") {
                out.flatten = true;
            } else if meta.path.is_ident("rename") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                out.rename = Some(lit.value());
            } else if meta.path.is_ident("rename_all") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                out.rename_all = Some(lit.value());
            } else if meta.path.is_ident("default") {
                // Bare `default` or `default = "path"`: both relax decoding.
                out.default = true;
                if meta.input.peek(syn::Token![=]) {
                    let _: syn::Expr = meta.value()?.parse()?;
                }
            } else if meta.path.is_ident("skip_serializing_if") {
                let expr: syn::Expr = meta.value()?.parse()?;
                out.skip_serializing_if = Some(match expr {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(lit),
                        ..
                    }) => lit.value(),
                    _ => String::new(),
                });
            } else if meta.input.peek(syn::Token![=]) {
                // Any other value-bearing key (alias = "..") — consume and
                // ignore. `alias` deliberately does NOT count as coverage.
                let _: syn::Expr = meta.value()?.parse()?;
            }
            Ok(())
        });
    }
    out
}

/// Does any `#[derive(...)]` on this item name `derive` (bare or
/// `serde::<derive>`)? Matches on the last path segment, so `Serialize` never
/// counts for `Deserialize` and vice versa.
fn derives(attrs: &[syn::Attribute], derive: &str) -> bool {
    for attr in attrs {
        if !attr.path().is_ident("derive") {
            continue;
        }
        let mut found = false;
        let _ = attr.parse_nested_meta(|meta| {
            if let Some(seg) = meta.path.segments.last() {
                if seg.ident == derive {
                    found = true;
                }
            }
            Ok(())
        });
        if found {
            return true;
        }
    }
    false
}

/// serde's `rename_all` word-splitting is on the snake_case field ident.
fn apply_rename_all(rule: &str, ident: &str) -> String {
    // Rust fields arrive snake_case; split on `_` into words.
    let words: Vec<&str> = ident.split('_').filter(|w| !w.is_empty()).collect();
    let capitalize = |w: &str| {
        let mut c = w.chars();
        match c.next() {
            Some(f) => f.to_ascii_uppercase().to_string() + &c.as_str().to_ascii_lowercase(),
            None => String::new(),
        }
    };
    match rule {
        "lowercase" => ident.to_ascii_lowercase(),
        "UPPERCASE" => ident.to_ascii_uppercase(),
        "PascalCase" => words.iter().map(|w| capitalize(w)).collect(),
        "camelCase" => {
            let mut it = words.iter();
            let first = it
                .next()
                .map(|w| w.to_ascii_lowercase())
                .unwrap_or_default();
            first + &it.map(|w| capitalize(w)).collect::<String>()
        }
        "snake_case" => words.join("_"),
        "SCREAMING_SNAKE_CASE" => words
            .iter()
            .map(|w| w.to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join("_"),
        "kebab-case" => words.join("-"),
        "SCREAMING-KEBAB-CASE" => words
            .iter()
            .map(|w| w.to_ascii_uppercase())
            .collect::<Vec<_>>()
            .join("-"),
        // Unknown rule: leave the ident untouched rather than guess.
        _ => ident.to_string(),
    }
}

/// The single generic type argument of a path type's last segment, if any.
fn sole_type_arg(seg: &syn::PathSegment) -> Option<&syn::Type> {
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

/// How many `Option<..>` layers wrap this type (references are looked through).
fn option_depth(ty: &syn::Type) -> u8 {
    match ty {
        syn::Type::Reference(r) => option_depth(&r.elem),
        syn::Type::Paren(p) => option_depth(&p.elem),
        syn::Type::Path(p) => match p.path.segments.last() {
            Some(seg) if seg.ident == "Option" => {
                1 + sole_type_arg(seg).map(option_depth).unwrap_or(0)
            }
            _ => 0,
        },
        _ => 0,
    }
}

/// Is this type `serde_json::Value` (bare `Value` or qualified)?
fn is_json_value(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Reference(r) => is_json_value(&r.elem),
        syn::Type::Path(p) => p
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "Value" && seg.arguments.is_none()),
        _ => false,
    }
}

/// Walks a parsed Rust file collecting every struct deriving the selected
/// serde trait, including those nested in fn / impl-method bodies (the reason
/// this is a `Visit` walk and not a `File.items` loop).
struct StructCollector {
    direction: Direction,
    structs: Vec<CollectedStruct>,
}

impl<'ast> Visit<'ast> for StructCollector {
    fn visit_item_struct(&mut self, node: &'ast syn::ItemStruct) {
        if derives(&node.attrs, self.direction.derive_name()) {
            let container = parse_serde_attrs(&node.attrs);
            let mut wire_fields = BTreeSet::new();
            let mut fields = Vec::new();
            let mut has_flatten = false;
            let mut has_unsupported_skip_if = false;
            if let syn::Fields::Named(named) = &node.fields {
                for field in &named.named {
                    let fs = parse_serde_attrs(&field.attrs);
                    if fs.flatten {
                        has_flatten = true;
                        continue;
                    }
                    if fs.dropped_in(self.direction) {
                        continue;
                    }
                    let depth = option_depth(&field.ty);
                    if self.direction == Direction::Serialize {
                        if let Some(predicate) = &fs.skip_serializing_if {
                            if predicate != "Option::is_none" || depth == 0 {
                                has_unsupported_skip_if = true;
                            }
                        }
                    }
                    let Some(ident) = field.ident.as_ref() else {
                        continue;
                    };
                    let ident = ident.to_string();
                    let wire = if let Some(rename) = fs.rename {
                        rename
                    } else if let Some(rule) = &container.rename_all {
                        apply_rename_all(rule, &ident)
                    } else {
                        ident
                    };
                    wire_fields.insert(wire.clone());
                    fields.push(WireField {
                        wire,
                        option_depth: depth,
                        json_value: is_json_value(&field.ty),
                        has_default: fs.default || container.default,
                        skip_serializing_if: fs.skip_serializing_if.is_some(),
                    });
                }
            }
            self.structs.push(CollectedStruct {
                name: node.ident.to_string(),
                wire_fields,
                fields,
                has_flatten,
                has_unsupported_skip_if,
            });
        }
        // Continue the default traversal so structs nested inside THIS struct's
        // context (and, at the file level, inside fn/impl bodies) are still seen.
        syn::visit::visit_item_struct(self, node);
    }
}

fn walk_structs_for(rust_src: &str, direction: Direction) -> Vec<CollectedStruct> {
    let file = match syn::parse_file(rust_src) {
        Ok(f) => f,
        // A source we cannot parse yields no structs; the real-tree test would
        // then fail loudly on the manifest's dangling entries (StructNotFound),
        // which is the correct fail-closed signal.
        Err(_) => return Vec::new(),
    };
    let mut collector = StructCollector {
        direction,
        structs: Vec::new(),
    };
    collector.visit_file(&file);
    collector.structs
}

/// Every `Deserialize`-deriving struct in `rust_src` (the response inventory).
pub(crate) fn walk_structs(rust_src: &str) -> Vec<CollectedStruct> {
    walk_structs_for(rust_src, Direction::Deserialize)
}

/// Extracts `e[key]` as a wire string, e.g. a manifest entry's declared
/// struct/schema name. Reused for the declared-name set, the malformed-entry
/// scan, the StructNotFound scan, and the per-mirror field comparison.
fn entry_str<'a>(e: &'a Value, key: &str) -> Option<&'a str> {
    e.get(key).and_then(|v| v.as_str())
}

/// A manifest list by key, empty when absent.
fn manifest_list<'a>(manifest: &'a Value, key: &str) -> &'a [Value] {
    manifest
        .get(key)
        .and_then(|m| m.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// Property names of a schema object, empty if it declares none.
fn schema_props(schema: &Value) -> BTreeSet<String> {
    schema
        .get("properties")
        .and_then(|p| p.as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}

/// The schema's `required` property names, empty if it declares none.
fn schema_required(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// An object-level composition keyword the gate refuses to decompose.
fn is_composed_schema(schema: &Value) -> bool {
    schema.get("allOf").is_some() || schema.get("anyOf").is_some() || schema.get("oneOf").is_some()
}

/// Does this (property) schema admit JSON `null`? Follows `$ref` into
/// `components.schemas`; `depth` bounds a cyclic reference.
fn schema_nullable(schema: &Value, schemas: &serde_json::Map<String, Value>, depth: usize) -> bool {
    if depth > 16 {
        return false;
    }
    if schema.get("nullable").and_then(Value::as_bool) == Some(true) {
        return true;
    }
    match schema.get("type") {
        Some(Value::String(t)) if t == "null" => return true,
        Some(Value::Array(types)) if types.iter().any(|t| t == "null") => return true,
        _ => {}
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(members) = schema.get(key).and_then(Value::as_array) {
            if members
                .iter()
                .any(|m| schema_nullable(m, schemas, depth + 1))
            {
                return true;
            }
        }
    }
    if let Some(target) = schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|r| r.strip_prefix("#/components/schemas/"))
    {
        if let Some(resolved) = schemas.get(target) {
            return schema_nullable(resolved, schemas, depth + 1);
        }
    }
    false
}

/// Whether one Rust field honestly expresses one API property's optionality,
/// under the rules for `direction` (see the module doc).
fn optionality_matches(
    field: &WireField,
    api_optional: bool,
    api_nullable: bool,
    direction: Direction,
) -> bool {
    match direction {
        Direction::Deserialize => {
            let accepts_omission = field.option_depth > 0 || field.has_default;
            let accepts_null = field.option_depth > 0 || field.json_value;
            (accepts_omission || !api_optional) && (accepts_null || !api_nullable)
        }
        Direction::Serialize => {
            let can_omit = field.skip_serializing_if;
            let can_send_null = (field.option_depth >= 1 && !field.skip_serializing_if)
                || field.option_depth >= 2
                || field.json_value;
            let required = field.option_depth == 0 && !can_omit && !field.json_value;
            let required_for_optional = required && (api_optional || api_nullable);
            let null_for_non_nullable = can_send_null && !api_nullable;
            let omits_required = can_omit && !api_optional;
            !(required_for_optional || null_for_non_nullable || omits_required)
        }
    }
}

/// Field-compare one declared mirror entry `m` against its found struct `s`,
/// appending every drift to `out`. Shared by the response and request gates.
fn compare_mirror(
    out: &mut Vec<Violation>,
    m: &Value,
    s: &CollectedStruct,
    schemas: &serde_json::Map<String, Value>,
    direction: Direction,
) {
    let (Some(struct_name), Some(schema_name)) = (entry_str(m, "struct"), entry_str(m, "schema"))
    else {
        return;
    };

    let Some(schema) = schemas.get(schema_name) else {
        out.push(Violation::SchemaNotFound {
            struct_name: struct_name.to_string(),
            schema: schema_name.to_string(),
        });
        return;
    };

    if s.has_flatten || s.has_unsupported_skip_if || is_composed_schema(schema) {
        out.push(Violation::UnsupportedShape {
            struct_name: struct_name.to_string(),
            schema: schema_name.to_string(),
        });
        return;
    }

    let props = schema_props(schema);
    let required = schema_required(schema);
    let wire = &s.wire_fields;

    // Validate omissions. An omission always suppresses MissingField for its
    // field; a dishonest one additionally yields StaleOmission.
    let mut suppressed: BTreeSet<String> = BTreeSet::new();
    if let Some(omissions) = m.get("omissions").and_then(|o| o.as_array()) {
        for om in omissions {
            let Some(field) = om.get("field").and_then(|v| v.as_str()) else {
                continue;
            };
            suppressed.insert(field.to_string());
            let why = om.get("why").and_then(|v| v.as_str()).unwrap_or("").trim();
            let dishonest = why.is_empty()      // blank/missing justification
                || wire.contains(field)         // struct actually carries it
                || !props.contains(field); // schema no longer defines it
            if dishonest {
                out.push(Violation::StaleOmission {
                    struct_name: struct_name.to_string(),
                    schema: schema_name.to_string(),
                    field: field.to_string(),
                });
            }
        }
    }

    // MissingField: a schema property neither carried nor suppressed.
    for p in &props {
        if !wire.contains(p) && !suppressed.contains(p) {
            out.push(Violation::MissingField {
                struct_name: struct_name.to_string(),
                schema: schema_name.to_string(),
                field: p.clone(),
            });
        }
    }

    // UnknownField: a wire field with no schema property behind it. No
    // allowlist path — a CLI field with no API field is always a bug.
    for w in wire {
        if !props.contains(w) {
            out.push(Violation::UnknownField {
                struct_name: struct_name.to_string(),
                schema: schema_name.to_string(),
                field: w.clone(),
            });
        }
    }

    // OptionalityMismatch: a carried field whose Rust optionality cannot
    // express the API property's.
    let properties = schema.get("properties").and_then(Value::as_object);
    for field in &s.fields {
        let Some(prop) = properties.and_then(|p| p.get(&field.wire)) else {
            continue;
        };
        let api_optional = !required.contains(&field.wire);
        let api_nullable = schema_nullable(prop, schemas, 0);
        if !optionality_matches(field, api_optional, api_nullable, direction) {
            out.push(Violation::OptionalityMismatch {
                struct_name: struct_name.to_string(),
                schema: schema_name.to_string(),
                field: field.wire.clone(),
            });
        }
    }
}

/// `components.schemas` of an OpenAPI doc, empty when absent.
fn components_schemas(openapi: &Value) -> &serde_json::Map<String, Value> {
    static EMPTY: std::sync::OnceLock<serde_json::Map<String, Value>> = std::sync::OnceLock::new();
    openapi
        .get("components")
        .and_then(|c| c.get("schemas"))
        .and_then(|s| s.as_object())
        .unwrap_or_else(|| EMPTY.get_or_init(serde_json::Map::new))
}

/// DuplicateStruct: the manifest keys by bare struct name, so two same-named
/// structs (legal Rust — e.g. two fn-body-local structs) collapse to one
/// manifest key and only the FIRST is ever field-checked. Fail closed: flag
/// each duplicated name once.
fn push_duplicates(out: &mut Vec<Violation>, structs: &[CollectedStruct]) {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut flagged_dup: BTreeSet<&str> = BTreeSet::new();
    for s in structs {
        let name = s.name.as_str();
        if !seen.insert(name) && flagged_dup.insert(name) {
            out.push(Violation::DuplicateStruct {
                struct_name: name.to_string(),
            });
        }
    }
}

/// MalformedManifestEntry for a mirrors-shaped list: an entry lacking `struct`
/// or `schema` would otherwise be silently skipped in field comparison (and,
/// if it has a struct, suppress that struct's UndeclaredStruct), letting the
/// struct escape checking. Fail closed: flag it.
fn push_malformed_mirrors(out: &mut Vec<Violation>, list: &str, mirrors: &[Value]) {
    for m in mirrors {
        let has_struct = entry_str(m, "struct");
        let has_schema = entry_str(m, "schema");
        if has_struct.is_none() || has_schema.is_none() {
            let detail = match (has_struct, has_schema) {
                (Some(st), None) => format!("{list} entry for struct {st:?} missing schema"),
                (None, Some(sc)) => format!("{list} entry missing struct (schema {sc:?})"),
                _ => format!("{list} entry missing struct and schema"),
            };
            out.push(Violation::MalformedManifestEntry { detail });
        }
    }
}

/// Compare the mirror structs in `rust_src` against the schemas they declare in
/// `manifest`, using `openapi`'s `components.schemas`. Pure: same inputs, same
/// output, so the fixtures drive it identically to the real tree.
pub fn violations(rust_src: &str, openapi: &Value, manifest: &Value) -> Vec<Violation> {
    let mut out = Vec::new();

    let structs = walk_structs(rust_src);
    let found_names: BTreeSet<&str> = structs.iter().map(|s| s.name.as_str()).collect();
    push_duplicates(&mut out, &structs);

    let mirrors = manifest_list(manifest, "mirrors");
    let non_mirrors = manifest_list(manifest, "non_mirrors");

    // Every declared struct name, across both lists.
    let declared: BTreeSet<String> = mirrors
        .iter()
        .chain(non_mirrors.iter())
        .filter_map(|e| entry_str(e, "struct").map(String::from))
        .collect();

    push_malformed_mirrors(&mut out, "mirrors", mirrors);
    for e in non_mirrors {
        if entry_str(e, "struct").is_none() {
            out.push(Violation::MalformedManifestEntry {
                detail: "non_mirrors entry missing struct".to_string(),
            });
        }
    }

    // UndeclaredStruct: an inventoried struct in neither list (D2's teeth).
    for s in &structs {
        if !declared.contains(&s.name) {
            out.push(Violation::UndeclaredStruct {
                struct_name: s.name.clone(),
            });
        }
    }

    // StructNotFound: a manifest entry (either list) naming a struct the walk
    // never found — the fail-closed twin that makes a walk defect observable.
    for entry in mirrors.iter().chain(non_mirrors.iter()) {
        if let Some(name) = entry_str(entry, "struct") {
            if !found_names.contains(name) {
                out.push(Violation::StructNotFound {
                    struct_name: name.to_string(),
                });
            }
        }
    }

    let schemas = components_schemas(openapi);
    for m in mirrors {
        let Some(struct_name) = entry_str(m, "struct") else {
            continue;
        };
        // No struct found for this entry -> already reported as StructNotFound.
        if let Some(s) = structs.iter().find(|s| s.name.as_str() == struct_name) {
            compare_mirror(&mut out, m, s, schemas, Direction::Deserialize);
        }
    }

    out
}

// ─── Request binding (issue #3834) ───────────────────────────────────────────

/// What the gate could read off one one-argument `.json(..)` call.
enum SendShape {
    /// No turbofish, or a `json!` body.
    Untyped,
    /// Method, URL literal or body type could not be read in the production shape.
    Unverifiable,
    /// The production shape: HTTP method ident, the path (literal minus its
    /// leading `{}` base-url placeholder), and the turbofish body struct.
    Typed {
        method: String,
        path: String,
        body: String,
    },
}

/// One platform send: the enclosing fn and its shape.
struct Send {
    function: String,
    shape: SendShape,
}

/// `{...}` placeholders collapse to `{}` so a source `{agent_id}` and an
/// OpenAPI `{agent_id}` (or any other name) compare equal per segment.
fn normalize_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut chars = path.chars();
    while let Some(c) = chars.next() {
        if c == '{' {
            for inner in chars.by_ref() {
                if inner == '}' {
                    break;
                }
            }
            out.push_str("{}");
        } else {
            out.push(c);
        }
    }
    out
}

/// Is `expr` exactly `self.<member>`?
fn is_self_field(expr: &syn::Expr, member: &str) -> bool {
    let syn::Expr::Field(field) = expr else {
        return false;
    };
    let syn::Member::Named(name) = &field.member else {
        return false;
    };
    name == member && matches!(&*field.base, syn::Expr::Path(p) if p.path.is_ident("self"))
}

/// Is this expression (through `&` and parens) a `json!` macro?
fn is_json_macro(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Reference(r) => is_json_macro(&r.expr),
        syn::Expr::Paren(p) => is_json_macro(&p.expr),
        syn::Expr::Macro(m) => m
            .mac
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == "json"),
        _ => false,
    }
}

/// The path a `format!("{}<path>", self.base_url, ..)` URL argument names, or
/// `None` when the argument is not that exact shape.
fn format_url_path(expr: &syn::Expr) -> Option<String> {
    let syn::Expr::Macro(m) = expr else {
        return None;
    };
    if !m.mac.path.is_ident("format") {
        return None;
    }
    let args = m
        .mac
        .parse_body_with(Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated)
        .ok()?;
    let mut args = args.iter();
    let syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(literal),
        ..
    }) = args.next()?
    else {
        return None;
    };
    if !is_self_field(args.next()?, "base_url") {
        return None;
    }
    literal.value().strip_prefix("{}").map(str::to_string)
}

/// Read one `.json(arg)` call's shape from its own method chain.
fn send_shape(call: &syn::ExprMethodCall) -> SendShape {
    let body = call.turbofish.as_ref().and_then(|tf| {
        tf.args.iter().find_map(|arg| match arg {
            syn::GenericArgument::Type(syn::Type::Path(p)) => {
                p.path.segments.last().map(|seg| seg.ident.to_string())
            }
            _ => None,
        })
    });
    let Some(body) = body else {
        return SendShape::Untyped;
    };
    if call.args.first().is_some_and(is_json_macro) {
        return SendShape::Untyped;
    }
    // Walk the receiver chain (`.header(..)`, `.query(..)`, ..) down to the
    // `self.http.<method>(url)` call that starts it.
    let mut receiver = &*call.receiver;
    loop {
        let syn::Expr::MethodCall(link) = receiver else {
            return SendShape::Unverifiable;
        };
        if is_self_field(&link.receiver, "http") {
            let Some(path) = link.args.first().and_then(format_url_path) else {
                return SendShape::Unverifiable;
            };
            return SendShape::Typed {
                method: link.method.to_string().to_ascii_lowercase(),
                path,
                body,
            };
        }
        receiver = &link.receiver;
    }
}

fn is_cfg_test(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Ident>()
                .is_ok_and(|ident| ident == "test")
    })
}

/// Collects every one-argument `.json(..)` call with its enclosing fn name,
/// skipping `#[cfg(test)]` modules and fns.
#[derive(Default)]
struct SendCollector {
    function: Vec<String>,
    sends: Vec<Send>,
}

impl<'ast> Visit<'ast> for SendCollector {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if !is_cfg_test(&node.attrs) {
            syn::visit::visit_item_mod(self, node);
        }
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        if is_cfg_test(&node.attrs) {
            return;
        }
        self.function.push(node.sig.ident.to_string());
        syn::visit::visit_item_fn(self, node);
        self.function.pop();
    }

    fn visit_impl_item_fn(&mut self, node: &'ast syn::ImplItemFn) {
        if is_cfg_test(&node.attrs) {
            return;
        }
        self.function.push(node.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, node);
        self.function.pop();
    }

    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        if node.method == "json" && node.args.len() == 1 {
            self.sends.push(Send {
                function: self.function.last().cloned().unwrap_or_default(),
                shape: send_shape(node),
            });
        }
        syn::visit::visit_expr_method_call(self, node);
    }
}

fn collect_sends(api_src: &str) -> Vec<Send> {
    let Ok(file) = syn::parse_file(api_src) else {
        return Vec::new();
    };
    let mut collector = SendCollector::default();
    collector.visit_file(&file);
    collector.sends
}

/// The JSON requestBody `$ref` schema name of `method` on the OpenAPI path that
/// normalizes to `path`. Outer `None`: no such operation. Inner `None`: the
/// operation has no named JSON requestBody schema.
fn operation_body_schema(openapi: &Value, method: &str, path: &str) -> Option<Option<String>> {
    let wanted = normalize_path(path);
    let paths = openapi.get("paths").and_then(Value::as_object)?;
    let operation = paths
        .iter()
        .filter(|(p, _)| normalize_path(p) == wanted)
        .find_map(|(_, item)| item.get(method))?;
    Some(
        operation
            .pointer("/requestBody/content/application~1json/schema/$ref")
            .and_then(Value::as_str)
            .and_then(|r| r.strip_prefix("#/components/schemas/"))
            .map(str::to_string),
    )
}

/// Bind every platform send in `api_src` to its declared OpenAPI operation and
/// field-compare the request mirror structs in `requests_src` (see the module
/// doc for the exact contract). Pure: same inputs, same output.
#[allow(dead_code)] // only api_field_parity runs the request gate
pub fn request_violations(
    api_src: &str,
    requests_src: &str,
    openapi: &Value,
    manifest: &Value,
) -> Vec<Violation> {
    let mut out = Vec::new();
    let schemas = components_schemas(openapi);

    let request_structs = walk_structs_for(requests_src, Direction::Serialize);
    let api_serialize_structs = walk_structs_for(api_src, Direction::Serialize);
    push_duplicates(&mut out, &request_structs);

    let request_mirrors = manifest_list(manifest, "request_mirrors");
    let requests = manifest_list(manifest, "requests");
    push_malformed_mirrors(&mut out, "request_mirrors", request_mirrors);
    for r in requests {
        let missing: Vec<&str> = ["function", "struct", "method", "path"]
            .into_iter()
            .filter(|key| entry_str(r, key).is_none())
            .collect();
        if !missing.is_empty() {
            out.push(Violation::MalformedManifestEntry {
                detail: format!("requests entry {r} missing {}", missing.join(", ")),
            });
        }
    }

    // Request mirror declaration and field comparison.
    let declared: BTreeSet<&str> = request_mirrors
        .iter()
        .filter_map(|e| entry_str(e, "struct"))
        .collect();
    for s in &request_structs {
        if !declared.contains(s.name.as_str()) {
            out.push(Violation::UndeclaredStruct {
                struct_name: s.name.clone(),
            });
        }
    }
    for m in request_mirrors {
        let Some(struct_name) = entry_str(m, "struct") else {
            continue;
        };
        let found = request_structs
            .iter()
            .chain(api_serialize_structs.iter())
            .find(|s| s.name == struct_name);
        match found {
            Some(s) => compare_mirror(&mut out, m, s, schemas, Direction::Serialize),
            None => out.push(Violation::StructNotFound {
                struct_name: struct_name.to_string(),
            }),
        }
    }

    // Send binding.
    let sends = collect_sends(api_src);
    for send in &sends {
        let function = send.function.clone();
        let (method, path, body) = match &send.shape {
            SendShape::Untyped => {
                out.push(Violation::UnclassifiedRequest { function });
                continue;
            }
            SendShape::Unverifiable => {
                out.push(Violation::UnverifiableRequest { function });
                continue;
            }
            SendShape::Typed { method, path, body } => (method, path, body),
        };
        let Some(entry) = requests
            .iter()
            .find(|r| entry_str(r, "function") == Some(send.function.as_str()))
        else {
            out.push(Violation::UnclassifiedRequest { function });
            continue;
        };
        let (Some(decl_struct), Some(decl_method), Some(decl_path)) = (
            entry_str(entry, "struct"),
            entry_str(entry, "method"),
            entry_str(entry, "path"),
        ) else {
            // Already reported as MalformedManifestEntry.
            continue;
        };
        let decl_method = decl_method.to_ascii_lowercase();

        if *method != decl_method || normalize_path(path) != normalize_path(decl_path) {
            out.push(Violation::RequestRouteMismatch {
                function: function.clone(),
                declared: format!("{} {decl_path}", decl_method.to_ascii_uppercase()),
                found: format!("{} {path}", method.to_ascii_uppercase()),
            });
        }
        if body != decl_struct {
            out.push(Violation::RequestSchemaMismatch {
                function: function.clone(),
                declared: decl_struct.to_string(),
                found: body.clone(),
            });
        }

        let mirror_schema = request_mirrors
            .iter()
            .find(|m| entry_str(m, "struct") == Some(decl_struct))
            .and_then(|m| entry_str(m, "schema"));
        match operation_body_schema(openapi, &decl_method, decl_path) {
            None => out.push(Violation::RequestRouteMismatch {
                function: function.clone(),
                declared: format!("{} {decl_path}", decl_method.to_ascii_uppercase()),
                found: "no such OpenAPI operation".to_string(),
            }),
            Some(op_schema) => {
                if op_schema.as_deref() != mirror_schema || mirror_schema.is_none() {
                    out.push(Violation::RequestSchemaMismatch {
                        function: function.clone(),
                        declared: mirror_schema.map(str::to_string).unwrap_or_else(|| {
                            format!("no request_mirrors entry for {decl_struct}")
                        }),
                        found: op_schema
                            .unwrap_or_else(|| "no JSON requestBody schema".to_string()),
                    });
                }
            }
        }
    }

    // A `requests` entry whose fn holds no send binds nothing: fail closed.
    let send_functions: BTreeSet<&str> = sends.iter().map(|s| s.function.as_str()).collect();
    for r in requests {
        let (Some(function), Some(method), Some(path)) = (
            entry_str(r, "function"),
            entry_str(r, "method"),
            entry_str(r, "path"),
        ) else {
            continue;
        };
        if !send_functions.contains(function) {
            out.push(Violation::RequestRouteMismatch {
                function: function.to_string(),
                declared: format!("{} {path}", method.to_ascii_uppercase()),
                found: "no platform send in this fn".to_string(),
            });
        }
    }

    out
}
