//! The one place an untrusted error becomes payload text.
//!
//! A bundle defect is a `diagnostics` entry at exit 0 on stdout, in the command
//! an agent is told to run before reporting success and to paste into a
//! transcript, a CI log or an issue comment. Every upstream producer of that
//! text -- `serde_json`, `jsonschema`, the probe container, the platform API --
//! renders the OFFENDING VALUE into its own message, so a token mistyped into a
//! manifest field, an eval suite or an MCP block would be echoed straight back
//! through a `reason`. Collapsing an MCP row to four fields buys nothing if the
//! diagnostic beside it reprints the same `env` block verbatim.
//!
//! The rule every helper here implements: a diagnostic says WHAT was wrong and
//! WHERE -- a JSON pointer, a type name, a line and column, an array index --
//! and never WHAT THE CONTENT WAS. A pointer is also more useful to an agent
//! than the echoed text it replaces.
//!
//! Two things this module deliberately does not do. It never formats an
//! untrusted error's `Display` (that is the echo itself), and it never scrubs a
//! finished string for secret-looking substrings -- a denylist over
//! attacker-shaped text is not a boundary. A new diagnostic builds its `reason`
//! from these helpers plus its own static prose; interpolating an upstream error
//! into one is the defect this module exists to make unnecessary.

/// A JSON parse failure as its POSITION only. Serde's syntax-error catalogue
/// happens not to quote the source span today, but a payload string must not
/// rest on that; the line and column are the whole useful part anyway.
pub fn json_syntax(err: &serde_json::Error) -> String {
    format!(
        "a JSON syntax error at line {}, column {}",
        err.line(),
        err.column()
    )
}

/// The JSON type of a value, named for "found X, expected Y" prose. Types
/// are derived facts; the value they describe never leaves this function.
pub fn json_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

/// One `jsonschema` violation as its two LOCATIONS: where in the instance,
/// and which constraint of the committed schema it failed.
///
/// `ValidationError`'s `Display` serializes the failing instance into the
/// message -- `PluginManifest.mcpServers` is `anyOf [string, object, null]`,
/// so declaring servers as an array (a routine authoring mistake) makes it
/// print the whole array including any `env` block inside it. Both
/// `Location`s are structural: property names and array indices on the
/// instance side, the committed schema's own path on the other.
pub fn schema_violation(err: &jsonschema::ValidationError<'_>) -> String {
    format!(
        "the value at {} does not satisfy the schema constraint at {}",
        pointer(err.instance_path()),
        pointer(err.schema_path())
    )
}

/// Render a JSON pointer, naming the empty pointer rather than emitting the
/// empty string into the middle of a sentence.
fn pointer(location: impl std::fmt::Display) -> String {
    let rendered = location.to_string();
    if rendered.is_empty() {
        "the document root".to_string()
    } else {
        rendered
    }
}
