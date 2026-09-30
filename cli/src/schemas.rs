//! Embedded, versioned JSON Schemas for every agent-facing `--json` result
//! (issue #634).
//!
//! The CLI's primary consumer is a coding agent (ADR-0021). Every result it
//! emits under `--json` maps to a committed schema under `cli/schema/` with an
//! explicit version identity (the `/vN` segment of the schema's `$id`). Those
//! files are embedded into the binary by `build.rs`, so a released `curie`
//! ships its own schemas and can print them with no source checkout -- the
//! documented discovery path (`curie schema-index [NAME]`). `cli/schema/index.json`
//! is the inventory that maps each result family to its schema and version, and
//! the contract test `cli/tests/schema_inventory.rs` fails CI if a new result
//! family lands without one. The compatibility policy (additive vs breaking;
//! breaking implies a new version) lives in
//! `docs/adr/0074-versioned-json-schemas-for-cli-results.md`.

include!(concat!(env!("OUT_DIR"), "/schemas_embedded.rs"));

/// The schema inventory index JSON (`cli/schema/index.json`), embedded.
pub fn index() -> &'static str {
    SCHEMA_INDEX
}

/// The contents of the named schema, looked up by short name (e.g. `"kill"`),
/// full file name (`"kill.schema.json"`), or the `<name>.schema.json` form.
/// `None` when no such schema is embedded.
pub fn schema(name: &str) -> Option<&'static str> {
    let short = name.strip_suffix(".schema.json").unwrap_or(name);
    SCHEMAS
        .iter()
        .find(|(s, file, _)| *s == short || *file == name)
        .map(|(_, _, contents)| *contents)
}

/// Every embedded schema's short name, sorted.
pub fn names() -> Vec<&'static str> {
    SCHEMAS.iter().map(|(s, _, _)| *s).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_is_embedded_and_valid_json() {
        let v: serde_json::Value =
            serde_json::from_str(index()).expect("embedded index.json is valid JSON");
        assert!(v["results"].as_array().is_some_and(|a| !a.is_empty()));
    }

    #[test]
    fn every_embedded_schema_is_valid_json_and_looks_up() {
        assert!(!SCHEMAS.is_empty(), "build.rs embedded at least one schema");
        for (short, file, contents) in SCHEMAS {
            serde_json::from_str::<serde_json::Value>(contents)
                .unwrap_or_else(|e| panic!("embedded {file} is valid JSON: {e}"));
            assert_eq!(
                schema(short),
                Some(*contents),
                "lookup by short name {short}"
            );
            assert_eq!(schema(file), Some(*contents), "lookup by file name {file}");
        }
    }

    #[test]
    fn a_known_schema_and_an_unknown_one() {
        assert!(schema("kill").is_some(), "kill schema is embedded");
        assert!(schema("kill.schema.json").is_some());
        assert!(
            schema("curie-yaml").is_some(),
            "installation input schema is embedded"
        );
        assert!(schema("no-such-result").is_none());
    }
}
