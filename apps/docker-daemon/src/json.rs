//! A duplicate-preserving JSON tree and struct-field lookup that folds keys
//! the way Go's encoding/json does.
//!
//! Go matches object keys to struct fields case-insensitively with Unicode
//! simple folding (U+212A KELVIN SIGN folds to `k`, U+017F LONG S to `s`),
//! and the last of two matching keys wins. A reader that keeps only one key
//! (serde_json's `Value`) can see a different value than dockerd does, so
//! every struct-shaped object the policy inspects is refused when two of its
//! keys fold equal.

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use std::collections::HashSet;
use std::fmt;

pub enum Node {
    Null,
    Bool(bool),
    Number,
    String(String),
    Array(Vec<Node>),
    Object(Vec<(String, Node)>),
}

/// A policy refusal produced while reading a body: rule id plus detail.
pub struct Refusal {
    pub rule: &'static str,
    pub detail: String,
}

pub type Res<T> = Result<T, Refusal>;

pub fn refuse(rule: &'static str, detail: impl Into<String>) -> Refusal {
    Refusal {
        rule,
        detail: detail.into(),
    }
}

impl Node {
    /// Parse one complete JSON document. serde_json rejects trailing data and
    /// invalid UTF-8, and its default recursion limit (128) stays on, so a
    /// deeply nested body is an error rather than a stack overflow.
    pub fn parse(bytes: &[u8]) -> Result<Node, String> {
        serde_json::from_slice(bytes).map_err(|e| e.to_string())
    }

    fn kind(&self) -> &'static str {
        match self {
            Node::Null => "null",
            Node::Bool(_) => "bool",
            Node::Number => "number",
            Node::String(_) => "string",
            Node::Array(_) => "array",
            Node::Object(_) => "object",
        }
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(NodeVisitor)
    }
}

struct NodeVisitor;

impl<'de> Visitor<'de> for NodeVisitor {
    type Value = Node;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_unit<E>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }
    fn visit_bool<E>(self, b: bool) -> Result<Node, E> {
        Ok(Node::Bool(b))
    }
    fn visit_i64<E>(self, _: i64) -> Result<Node, E> {
        Ok(Node::Number)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Node, E> {
        Ok(Node::Number)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Node, E> {
        Ok(Node::Number)
    }
    fn visit_str<E>(self, s: &str) -> Result<Node, E> {
        Ok(Node::String(s.to_owned()))
    }
    fn visit_string<E>(self, s: String) -> Result<Node, E> {
        Ok(Node::String(s))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Node, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Node::Array(items))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
        let mut pairs = Vec::new();
        while let Some(pair) = map.next_entry::<String, Node>()? {
            pairs.push(pair);
        }
        Ok(Node::Object(pairs))
    }
}

/// Go's fold for keys that can match an ASCII field name: the two non-ASCII
/// letters whose simple fold orbit reaches ASCII, then ASCII lowercase.
fn fold(key: &str) -> String {
    key.chars()
        .map(|c| match c {
            '\u{212A}' => 'k',
            '\u{17F}' => 's',
            c => c.to_ascii_lowercase(),
        })
        .collect()
}

fn wrong_type(what: &str, want: &str, got: &Node) -> Refusal {
    refuse(
        "body-unparseable",
        format!("{what} must be {want}, got {}", got.kind()),
    )
}

/// The keys of one struct-shaped object, folded and checked for collisions.
pub struct Fields<'a> {
    what: String,
    items: Vec<(String, &'a Node)>,
}

impl<'a> Fields<'a> {
    /// Read `node` as a struct. Null is `None`. Two keys that fold equal are
    /// `field-ambiguous`; with an allowlist, any other key is `field-unknown`.
    pub fn of(node: &'a Node, what: &str, allowed: Option<&[&str]>) -> Res<Option<Self>> {
        let pairs = match node {
            Node::Null => return Ok(None),
            Node::Object(pairs) => pairs,
            other => return Err(wrong_type(what, "an object", other)),
        };
        let mut items: Vec<(String, &'a Node)> = Vec::with_capacity(pairs.len());
        let mut seen = HashSet::with_capacity(pairs.len());
        for (key, value) in pairs {
            let folded = fold(key);
            if !seen.insert(folded.clone()) {
                return Err(refuse(
                    "field-ambiguous",
                    format!("{what} has two keys that Go reads as the same field ({key:?})"),
                ));
            }
            if let Some(allowed) = allowed {
                if !allowed.iter().any(|a| a.eq_ignore_ascii_case(&folded)) {
                    return Err(refuse(
                        "field-unknown",
                        format!("{what} key {key:?} is not allowlisted"),
                    ));
                }
            }
            items.push((folded, value));
        }
        Ok(Some(Fields {
            what: what.to_owned(),
            items,
        }))
    }

    pub fn what(&self, name: &str) -> String {
        format!("{}.{name}", self.what)
    }

    /// The value of field `name`; null counts as absent.
    pub fn get(&self, name: &str) -> Option<&'a Node> {
        self.items
            .iter()
            .find(|(k, _)| name.eq_ignore_ascii_case(k))
            .map(|(_, v)| *v)
            .filter(|v| !matches!(v, Node::Null))
    }

    pub fn present(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn sub(&self, name: &str, allowed: Option<&[&str]>) -> Res<Option<Fields<'a>>> {
        match self.get(name) {
            None => Ok(None),
            Some(node) => Fields::of(node, &self.what(name), allowed),
        }
    }

    /// The field's value, `default` when absent, `extract`ed from the
    /// expected variant, else a `wrong_type` refusal naming `want`.
    fn typed<T>(
        &self,
        name: &str,
        default: T,
        want: &str,
        extract: impl FnOnce(&'a Node) -> Option<T>,
    ) -> Res<T> {
        match self.get(name) {
            None => Ok(default),
            Some(node) => extract(node).ok_or_else(|| wrong_type(&self.what(name), want, node)),
        }
    }

    pub fn bool(&self, name: &str) -> Res<bool> {
        self.typed(name, false, "a bool", |n| match n {
            Node::Bool(b) => Some(*b),
            _ => None,
        })
    }

    pub fn str(&self, name: &str) -> Res<&'a str> {
        self.typed(name, "", "a string", |n| match n {
            Node::String(s) => Some(s.as_str()),
            _ => None,
        })
    }

    pub fn list(&self, name: &str) -> Res<&'a [Node]> {
        self.typed(name, &[], "an array", |n| match n {
            Node::Array(items) => Some(items.as_slice()),
            _ => None,
        })
    }

    pub fn strs(&self, name: &str) -> Res<Vec<&'a str>> {
        self.list(name)?
            .iter()
            .map(|n| match n {
                Node::String(s) => Ok(s.as_str()),
                other => Err(wrong_type(&self.what(name), "an array of strings", other)),
            })
            .collect()
    }

    /// A map-shaped field (keys are data, not collision checked).
    pub fn map(&self, name: &str) -> Res<&'a [(String, Node)]> {
        self.typed(name, &[], "an object", |n| match n {
            Node::Object(pairs) => Some(pairs.as_slice()),
            _ => None,
        })
    }
}
