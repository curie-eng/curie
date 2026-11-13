//! Enum-variant inventory from source, for the `MessageOutcomeOutput`
//! schema-coverage gate (issue #955).
//!
//! Same shape as the sibling AST gates (`support/schema_inventory.rs`,
//! `support/emit_parity.rs`): a pure, side-effect-free `syn` walk over the text
//! of a `cli/src` file. The caller reads the file and asserts; this module never
//! prints or touches the filesystem. It DOES panic, with a specific message, on
//! the three inputs it cannot honestly enumerate (unparseable source, a second
//! same-named enum, a `#[cfg]`-gated declaration) -- see `variant_names`.
//!
//! ## What gives it teeth
//!
//! "Every variant of `enum T`" is a *syntactic* property a `syn` walk enumerates
//! exhaustively, so it is a source of truth a developer cannot forget to update:
//! adding a variant to the enum IS updating it. That is exactly what #955 needs
//! -- a hand-written list of expected variants in the test would go stale the
//! moment a sixth variant lands, which is the drift the gate exists to catch.
//!
//! The corollary is that UNDER-reporting the variant set is the gate's worst
//! failure: a set missing a variant makes the coverage comparison vacuously
//! green. Every input this walk cannot enumerate faithfully therefore fails
//! loudly rather than returning a quietly-short set.

use std::collections::BTreeSet;

use syn::visit::Visit;

/// Collects the variant identifiers of every `enum` whose name matches `target`,
/// including enums nested in module or fn bodies. That reach comes from
/// `Visit::visit_file`'s own default walk over the parsed file (which needs the
/// `full` + `visit` features the sibling walks already rely on), not from this
/// visitor's `visit_item_enum` override recursing into itself below -- no item
/// can nest inside an enum body, so that call can never reach another
/// `ItemEnum`. It is kept anyway, matching the sibling walks' convention
/// (`schema_inventory.rs`, `emit_parity.rs`) of always continuing the default
/// walk from an override. The `visit_item_mod` override adds no reach either:
/// it only records which enclosing modules are `#[cfg]`-gated while continuing
/// that same default walk.
struct VariantCollector<'a> {
    target: &'a str,
    names: BTreeSet<String>,
    /// How many `enum <target>` declarations the walk saw. More than one and the
    /// name no longer identifies a single set (see `variant_names`).
    declarations: usize,
    /// Declarations (`Enum`), variants (`Enum::Variant`), or enclosing modules
    /// carrying a `#[cfg]`.
    cfg_gated: BTreeSet<String>,
    /// Names of the `#[cfg]`-gated modules enclosing the walk's current
    /// position, innermost last. A declaration inside one exists only under that
    /// cfg exactly as a declaration carrying its own `#[cfg]` does, so it is the
    /// same failure -- and it is the likely real-world shape, since `#[cfg(test)]
    /// mod tests` is where a second same-named enum usually lives.
    cfg_mods: Vec<String>,
}

/// Does this item carry a `#[cfg(...)]` attribute directly, i.e. does its very
/// existence depend on the compilation configuration? Enclosing `#[cfg]`s are
/// tracked separately, by `cfg_mods`.
fn is_cfg_gated(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident("cfg"))
}

impl<'ast> Visit<'ast> for VariantCollector<'_> {
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        let gated = is_cfg_gated(&node.attrs);
        if gated {
            self.cfg_mods.push(node.ident.to_string());
        }
        syn::visit::visit_item_mod(self, node);
        if gated {
            self.cfg_mods.pop();
        }
    }

    fn visit_item_enum(&mut self, node: &'ast syn::ItemEnum) {
        if node.ident == self.target {
            self.declarations += 1;
            if is_cfg_gated(&node.attrs) {
                self.cfg_gated.insert(self.target.to_string());
            }
            if let Some(module) = self.cfg_mods.last() {
                self.cfg_gated
                    .insert(format!("mod {module} (enclosing enum {})", self.target));
            }
            for variant in &node.variants {
                if is_cfg_gated(&variant.attrs) {
                    self.cfg_gated
                        .insert(format!("{}::{}", self.target, variant.ident));
                }
                self.names.insert(variant.ident.to_string());
            }
        }
        syn::visit::visit_item_enum(self, node);
    }
}

/// Every variant identifier declared by `enum <target>` in `src`.
///
/// An absent enum yields an empty set -- the caller asserts the set is non-empty
/// before using it, so a genuinely missing enum reads as a gate failure rather
/// than as vacuous coverage. The three inputs that would instead produce a
/// *plausible-looking but short* set panic here, each with its own message, so
/// they can never be mistaken for the absent-enum case:
///
/// * `src` does not parse as Rust (the old walk silently returned nothing,
///   which the caller then misreported as "no enum found");
/// * two `enum <target>` declarations exist (their variants would union into a
///   set matching neither, e.g. one real and one inside `#[cfg(test)] mod tests`);
/// * the enum, one of its variants, or a module enclosing the declaration is
///   `#[cfg]`-gated. This walk reads source text and does not model conditional
///   compilation, while the consuming no-wildcard `match` is compiled under one
///   specific cfg -- so the two sides would disagree about what exists.
///
/// The cfg detection is exactly those three positions: an attribute spelled
/// `#[cfg(...)]` on the enum, on a variant, or on an enclosing `mod`. It does
/// NOT see a `#[cfg]` on any other enclosing item (a gated `fn` whose body
/// declares the enum), nor one applied indirectly through `#[cfg_attr(...)]`.
/// Those shapes would be enumerated as if unconditional. Every failure mode
/// above, and the module-enclosure case in particular, is proven by execution
/// in `cli/tests/json_contract.rs`.
pub fn variant_names(src: &str, target: &str) -> BTreeSet<String> {
    let file = syn::parse_file(src).unwrap_or_else(|e| {
        panic!(
            "enum_variants: the source searched for `enum {target}` does not parse as Rust \
             ({e}); the #955 coverage gate cannot derive a variant set from it. This is a \
             PARSE failure, not an absent enum"
        )
    });
    variant_names_in_file(&file, target)
}

/// Enumerate a parsed source with the same duplicate and cfg checks.
pub fn variant_names_in_file(file: &syn::File, target: &str) -> BTreeSet<String> {
    let mut collector = VariantCollector {
        target,
        names: BTreeSet::new(),
        declarations: 0,
        cfg_gated: BTreeSet::new(),
        cfg_mods: Vec::new(),
    };
    collector.visit_file(file);

    assert!(
        collector.declarations <= 1,
        "enum_variants: {} declarations of `enum {target}` in one source; their variants would \
         union into a set that describes no single enum, so the #955 coverage gate would compare \
         against a fiction. Keep exactly one declaration of the gated enum name",
        collector.declarations
    );
    assert!(
        collector.cfg_gated.is_empty(),
        "enum_variants: {:?} carry a `#[cfg(...)]` attribute; this walk reads source text and \
         does not model conditional compilation, while the #955 coverage gate's no-wildcard \
         `match` is compiled under one specific cfg -- the two would disagree about which \
         variants exist. Un-gate the variant, or teach both sides the same cfg",
        collector.cfg_gated
    );

    collector.names
}
