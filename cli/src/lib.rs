//! Curie CLI library: everything behind the `curie` binary.
//!
//! The CLI speaks only the frozen contracts: ACI frames over HTTP/NDJSON to a
//! local runner container (via the generated `curie-aci-protocol` crate) and
//! the platform API's committed OpenAPI surface. Task I1.

pub mod api;
pub mod artifacts;
pub mod bundle;
pub mod channel;
pub mod chat;
pub mod commands;
pub mod comms;
pub mod discover;
pub mod docker;
pub mod eval_init;
pub mod evals;
pub mod exit;
pub mod guide;
pub mod info;
pub mod interactive;
pub mod local;
pub mod message;
pub mod ndjson;
pub mod observability;
pub mod ops;
pub mod queue;
pub mod recipes;
pub mod redact;
pub mod render;
pub mod retired;
pub mod runner;
pub mod scaffold;
pub mod schema;
pub mod schemas;
pub mod secrets;
pub mod slack;
pub mod spec;
pub mod state;
pub mod ui;

pub use retired::retired_hint;
