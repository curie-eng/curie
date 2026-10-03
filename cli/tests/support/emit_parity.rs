//! Emit-hop comparator for the `--json` gate (issue #699).
//!
//! One hop downstream of the struct-level field-parity gate (`support/field_parity.rs`,
//! issue #691). That gate proves a `cli/src/api.rs` mirror struct carries every
//! field its API schema declares. It says nothing about the SECOND hop: a
//! `CliOutput::to_json` impl in a `cli/src/commands/` module (or a sibling file) can
//! still hand-project that struct into a `serde_json::json!` literal and drop
//! one of its fields on the floor. The proof case was `VersionsOutput::to_json`
//! dropping `Version::id` (fixed inline in #691's PR).
//!
//! ## Scope (deliberately narrower than the struct-level gate)
//!
//! The struct-level gate requires EVERY `Deserialize` struct in `api.rs` to be
//! declared (its `UndeclaredStruct` violation is what gives it that reach) --
//! that is tractable because "every `Deserialize`-deriving struct" is a
//! syntactic property syn can enumerate exhaustively.
//!
//! "Every place a mirror struct gets hand-projected into a `json!` literal" is
//! NOT a syntactic property: proving it exhaustively would mean resolving, for
//! an arbitrary closure/iterator-adapter chain, the concrete type flowing
//! through it (e.g. that `versions.iter().map(|v| json!({..}))`'s `v` is a
//! `&crate::api::Version`) -- real type inference, which a syntax-only `syn`
//! walk cannot give us. That was the risk this issue's own text flagged and
//! asked for a spike on.
//!
//! So this gate takes the tractable half of the proposed shape and stops
//! there: `cli/api-mirrors.json` gets a new `emits` array, each entry a
//! human-DECLARED `(output, struct)` pair (analogous to `mirrors`' declared
//! struct/schema pairs), and the gate mechanically proves that declared pair
//! honest -- every wire field of `struct` (per the SAME struct inventory
//! `field_parity::walk_structs` already builds from `api.rs`) appears in at
//! least one `json!` object literal reachable from `output`'s `to_json`, or is
//! covered by a declared, justified omission (the same allowlist shape as
//! `mirrors`' omissions). It does NOT discover new `(output, struct)` pairs on
//! its own the way `UndeclaredStruct` does for the struct hop -- a future
//! projection of a mirror struct that nobody declares an `emits` entry for is a
//! gap this gate cannot see. That gap is the honest cost of this hop being one
//! step further from syntax and one step closer to semantics; closing it fully
//! would need a real type-checker, not a heavier `syn` walk.
//!
//! ## Reachability: how "at least one `json!` literal" is found
//!
//! A `to_json` body is scanned for `json!`/`serde_json::json!` invocations by
//! walking its RAW token stream (not the `syn::Expr` tree): a macro argument is
//! opaque to syn (it is never parsed into an `Expr`), so the only way to see
//! inside `json!({..})` at all is to look at its tokens directly. The walk is
//! purely token-shaped -- find an `ident "json"` followed by `!` followed by a
//! `Group`, and both recurse into that group (in case a `json!` call sits
//! nested inside another one's tokens, e.g. inline rather than as a sibling
//! statement) and descend into every OTHER group along the way (so a `json!`
//! call inside a closure, a `.map(...)`, or a match arm is still found, since
//! all of those still show up as ordinary token groups). Each `{...}`-shaped
//! `json!` argument is then read as a flat object literal: top-level,
//! comma-separated `"key": value` entries (nested groups inside a value are
//! opaque and never split on their internal commas, since a `TokenTree::Group`
//! is one token). A non-object argument (`json!(x)`, `json!([..])`) yields no
//! keys and is not an error -- plenty of `to_json` bodies delegate to a
//! `serde::Serialize` value wholesale (`serde_json::to_value`) or to a shared
//! builder fn, which cannot drop a field by construction and needs no `emits`
//! entry at all.
//!
//! A `to_json` body also often delegates a struct's projection to a named free
//! function (e.g. `records.iter().map(approval_record_json)`), so the walk
//! additionally collects every bare identifier reachable from the body and, for
//! each one that resolves to a top-level `fn` found anywhere in the scanned
//! sources, pulls that function's own reachable `json!` literals in too
//! (fixpoint, cycle-guarded by name). Only free functions are followed --
//! `self.method()` helpers are not, a scope limit noted rather than hidden.
//!
//! The comparator requires a single keyset (not a union across several) to
//! cover every non-omitted wire field, so two unrelated `json!` literals in the
//! same `to_json` cannot accidentally "cover" a struct between them.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use serde_json::Value;
use syn::visit::Visit;

use crate::field_parity::{self, CollectedStruct};

/// A single way a declared `emits` projection has drifted. Mirrors the shape of
/// `field_parity::Violation`: the gate test matches on these variants and their
/// payload, never on message strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitViolation {
    /// No single `json!` object literal reachable from `output`'s `to_json`
    /// covers this non-omitted wire field of `struct_name`.
    MissingField {
        output: String,
        struct_name: String,
        field: String,
    },
    /// A declared omission is dishonest: blank/missing `why`, the struct
    /// (per the struct-level inventory) does not actually carry the field, or
    /// the field turns out to be covered by a reachable `json!` literal anyway.
    StaleOmission {
        output: String,
        struct_name: String,
        field: String,
    },
    /// An `emits` entry names an output with no `impl (crate::ui::)?CliOutput`
    /// found for it anywhere in the scanned sources.
    OutputNotFound { output: String },
    /// An `emits` entry names a struct absent from the struct-level inventory.
    StructNotFound { output: String, struct_name: String },
    /// An `emits` entry is missing a required key (`output`/`struct`), so its
    /// pair would silently escape comparison.
    MalformedManifestEntry { detail: String },
}

/// Every `CliOutput` impl's `to_json` body found in the scanned sources (output
/// type name -> its `to_json` block, re-tokenized), plus every top-level `fn`
/// found (by name, since a delegated projection is often a free function) for
/// the reachability fixpoint to follow.
#[derive(Default)]
struct OutputCollector {
    outputs: BTreeMap<String, TokenStream>,
    fns: BTreeMap<String, Vec<TokenStream>>,
}

impl<'ast> Visit<'ast> for OutputCollector {
    fn visit_item_impl(&mut self, node: &'ast syn::ItemImpl) {
        if let Some((path, _)) = &node.trait_ {
            let is_cli_output = path
                .segments
                .last()
                .map(|s| s.ident == "CliOutput")
                .unwrap_or(false);
            if is_cli_output {
                if let syn::Type::Path(tp) = node.self_ty.as_ref() {
                    if let Some(seg) = tp.path.segments.last() {
                        let name = seg.ident.to_string();
                        for item in &node.items {
                            if let syn::ImplItem::Fn(f) = item {
                                if f.sig.ident == "to_json" {
                                    // First definition wins; a duplicate output
                                    // name across files would be surprising
                                    // enough on its own to warrant a hand look,
                                    // and is out of scope for this gate.
                                    self.outputs
                                        .entry(name.clone())
                                        .or_insert_with(|| f.block.to_token_stream());
                                }
                            }
                        }
                    }
                }
            }
        }
        syn::visit::visit_item_impl(self, node);
    }

    fn visit_item_fn(&mut self, node: &'ast syn::ItemFn) {
        self.fns
            .entry(node.sig.ident.to_string())
            .or_default()
            .push(node.block.to_token_stream());
        syn::visit::visit_item_fn(self, node);
    }
}

/// Every bare identifier reachable in `tokens`, descending into every group.
/// Used only to find candidate free-fn names a `to_json` body might reference
/// by value (`.map(approval_record_json)`) or by call
/// (`approval_record_json(r)`) -- a name that happens not to resolve to a
/// known `fn` (a local variable, a type, a keyword-shaped ident) is simply
/// never looked up, so over-collecting here is harmless.
fn bare_idents(tokens: TokenStream) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    collect_idents(tokens, &mut out);
    out
}

fn collect_idents(tokens: TokenStream, out: &mut BTreeSet<String>) {
    for tt in tokens {
        match tt {
            TokenTree::Ident(id) => {
                out.insert(id.to_string());
            }
            TokenTree::Group(g) => collect_idents(g.stream(), out),
            _ => {}
        }
    }
}

/// Split a token stream on TOP-LEVEL commas only -- a comma inside a nested
/// group (a `{}`/`()`/`[]` in a value expression) is part of that group's one
/// `TokenTree` and is never seen as a separate token here, so it can't be
/// mistaken for an object-literal separator.
fn split_top_level_commas(tokens: TokenStream) -> Vec<TokenStream> {
    let mut chunks = Vec::new();
    let mut current: Vec<TokenTree> = Vec::new();
    for tt in tokens {
        if let TokenTree::Punct(p) = &tt {
            if p.as_char() == ',' {
                chunks.push(current.drain(..).collect::<TokenStream>());
                continue;
            }
        }
        current.push(tt);
    }
    if !current.is_empty() {
        chunks.push(current.into_iter().collect());
    }
    chunks
}

/// A string-literal token's value, or `None` if `text` is not a quoted string
/// (e.g. a number, or an interpolated/computed key this gate cannot read
/// statically -- skipped rather than guessed).
fn strip_str_literal(text: &str) -> Option<String> {
    if text.len() >= 2 && text.starts_with('"') && text.ends_with('"') {
        Some(text[1..text.len() - 1].to_string())
    } else {
        None
    }
}

/// If `tokens` is exactly one `{...}`-delimited group, read it as a flat
/// object literal: each top-level comma-separated entry is `"key" : value`;
/// `key` is collected, `value` is not interpreted further (key COVERAGE is all
/// this gate checks, never the value expression). Returns `None` when `tokens`
/// is not a single brace group (e.g. `json!(x)`, `json!([..])`) -- not an
/// object literal, so it carries no keys to check.
fn object_literal_keys(tokens: TokenStream) -> Option<BTreeSet<String>> {
    let toks: Vec<TokenTree> = tokens.into_iter().collect();
    if toks.len() != 1 {
        return None;
    }
    let TokenTree::Group(g) = &toks[0] else {
        return None;
    };
    if g.delimiter() != Delimiter::Brace {
        return None;
    }
    let mut keys = BTreeSet::new();
    for chunk in split_top_level_commas(g.stream()) {
        let mut it = chunk.into_iter();
        let Some(TokenTree::Literal(lit)) = it.next() else {
            continue;
        };
        let Some(key) = strip_str_literal(&lit.to_string()) else {
            continue;
        };
        let has_colon = matches!(it.next(), Some(TokenTree::Punct(p)) if p.as_char() == ':');
        if has_colon {
            keys.insert(key);
        }
    }
    Some(keys)
}

/// Recursively find every `json!`/`serde_json::json!`-shaped invocation
/// reachable in `tokens` (any qualifying path prefix -- only the segment right
/// before the `!` is checked), returning one keyset per `{...}`-shaped
/// argument found (a non-object argument contributes no keyset, not an
/// error). Descends into every group along the way, both to find a `json!`
/// call nested inside another expression's tokens and to find one hidden
/// inside a closure, `.map(...)`, or match arm.
fn find_json_object_keysets(tokens: TokenStream) -> Vec<BTreeSet<String>> {
    let mut out = Vec::new();
    let toks: Vec<TokenTree> = tokens.into_iter().collect();
    let mut i = 0;
    while i < toks.len() {
        if let TokenTree::Ident(id) = &toks[i] {
            if id == "json" && i + 2 < toks.len() {
                if let TokenTree::Punct(bang) = &toks[i + 1] {
                    if bang.as_char() == '!' {
                        if let TokenTree::Group(g) = &toks[i + 2] {
                            match object_literal_keys(g.stream()) {
                                Some(keys) => out.push(keys),
                                None => out.extend(find_json_object_keysets(g.stream())),
                            }
                            i += 3;
                            continue;
                        }
                    }
                }
            }
        }
        if let TokenTree::Group(g) = &toks[i] {
            out.extend(find_json_object_keysets(g.stream()));
        }
        i += 1;
    }
    out
}

/// Every `json!` object-literal keyset reachable from `output`'s `to_json`,
/// including through free functions it references by name (fixpoint,
/// cycle-guarded).
fn reachable_keysets(
    output: &str,
    outputs: &BTreeMap<String, TokenStream>,
    fns: &BTreeMap<String, Vec<TokenStream>>,
) -> Vec<BTreeSet<String>> {
    let Some(body) = outputs.get(output) else {
        return Vec::new();
    };
    let mut keysets = find_json_object_keysets(body.clone());
    let mut seen_fns: BTreeSet<String> = BTreeSet::new();
    let mut frontier: Vec<TokenStream> = vec![body.clone()];
    while let Some(tokens) = frontier.pop() {
        for name in bare_idents(tokens) {
            if seen_fns.contains(&name) {
                continue;
            }
            if let Some(bodies) = fns.get(&name) {
                seen_fns.insert(name);
                for b in bodies {
                    keysets.extend(find_json_object_keysets(b.clone()));
                    frontier.push(b.clone());
                }
            }
        }
    }
    keysets
}

/// Compare each declared `emits` entry in `manifest` against the `to_json`
/// bodies found in `cli_srcs` (every non-`api.rs` CLI source file) and the
/// struct inventory found in `api_src` (`cli/src/api.rs`, the same walk
/// `field_parity::violations` uses). Pure: same inputs, same output.
pub fn violations(
    api_src: &str,
    cli_srcs: &[(&str, &str)],
    manifest: &Value,
) -> Vec<EmitViolation> {
    let mut out = Vec::new();

    let structs = field_parity::walk_structs(api_src);
    let struct_index: BTreeMap<&str, &CollectedStruct> =
        structs.iter().map(|s| (s.name.as_str(), s)).collect();

    let mut collector = OutputCollector::default();
    for (_, src) in cli_srcs {
        // A file this gate cannot parse contributes nothing; a manifest entry
        // whose output only lives there then fails closed via OutputNotFound
        // below, the same fail-closed shape field_parity's walk relies on.
        if let Ok(file) = syn::parse_file(src) {
            collector.visit_file(&file);
        }
    }

    let empty: Vec<Value> = Vec::new();
    let entries: &[Value] = manifest
        .get("emits")
        .and_then(|v| v.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&empty);

    for entry in entries {
        let output = entry.get("output").and_then(|v| v.as_str());
        let struct_name = entry.get("struct").and_then(|v| v.as_str());
        let (Some(output), Some(struct_name)) = (output, struct_name) else {
            out.push(EmitViolation::MalformedManifestEntry {
                detail: format!("emits entry missing output or struct: {entry}"),
            });
            continue;
        };

        if !collector.outputs.contains_key(output) {
            out.push(EmitViolation::OutputNotFound {
                output: output.to_string(),
            });
            continue;
        }
        let Some(s) = struct_index.get(struct_name) else {
            out.push(EmitViolation::StructNotFound {
                output: output.to_string(),
                struct_name: struct_name.to_string(),
            });
            continue;
        };

        let keysets = reachable_keysets(output, &collector.outputs, &collector.fns);

        let mut suppressed: BTreeSet<String> = BTreeSet::new();
        if let Some(omissions) = entry.get("omissions").and_then(|o| o.as_array()) {
            for om in omissions {
                let Some(field) = om.get("field").and_then(|v| v.as_str()) else {
                    continue;
                };
                suppressed.insert(field.to_string());
                let why = om.get("why").and_then(|v| v.as_str()).unwrap_or("").trim();
                let covered_anyway = keysets.iter().any(|ks| ks.contains(field));
                let dishonest = why.is_empty() || !s.wire_fields.contains(field) || covered_anyway;
                if dishonest {
                    out.push(EmitViolation::StaleOmission {
                        output: output.to_string(),
                        struct_name: struct_name.to_string(),
                        field: field.to_string(),
                    });
                }
            }
        }

        let non_omitted: BTreeSet<&String> = s
            .wire_fields
            .iter()
            .filter(|f| !suppressed.contains(f.as_str()))
            .collect();

        // A SINGLE keyset must cover every non-omitted field -- coverage
        // scattered across several unrelated `json!` literals in the same
        // `to_json` does not count, since that would let two coincidentally
        // matching key names in unrelated objects "cover" each other.
        let best = keysets.iter().max_by_key(|ks| {
            non_omitted
                .iter()
                .filter(|f| ks.contains(f.as_str()))
                .count()
        });
        let missing: Vec<&String> = match best {
            Some(ks) => non_omitted
                .iter()
                .filter(|f| !ks.contains(f.as_str()))
                .copied()
                .collect(),
            None => non_omitted.iter().copied().collect(),
        };
        for field in missing {
            out.push(EmitViolation::MissingField {
                output: output.to_string(),
                struct_name: struct_name.to_string(),
                field: field.clone(),
            });
        }
    }

    out
}
