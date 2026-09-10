//! Curie CLI library: everything behind the `curie` binary.
//!
//! The CLI speaks only the frozen contracts: ACI frames over HTTP/NDJSON to a
//! local runner container (via the generated `curie-aci-protocol` crate) and
//! the platform API's committed OpenAPI surface. Task I1.

pub mod api;
pub mod artifacts;
pub mod bundle;
pub mod channel;
pub mod channel_token;
pub mod chat;
pub mod cluster_secrets;
pub mod commands;
pub mod comms;
pub mod completion_outbox;
pub mod connector_build;
pub mod connectors;
pub mod credcheck;
pub mod delivery;
pub mod discover;
pub mod docker;
pub mod doctor;
pub mod eval_init;
pub mod eval_sampling;
pub mod evals;
pub mod examples;
pub mod exit;
pub mod github_app;
pub mod guide;
pub mod installation;
pub mod interactive;
pub mod local;
pub mod mail_channel;
pub mod message;
pub mod migrate_store;
pub mod modelpin;
pub mod ndjson;
pub mod observability;
pub mod ops;
pub mod queue;
pub mod recipes;
pub mod render;
pub mod retired;
pub mod runner;
pub mod scaffold;
pub mod schema;
pub mod schema_window;
pub mod schemas;
pub mod seal;
pub mod sealing;
pub mod secrets;
pub mod slack;
pub mod spec;
pub mod state;
pub mod ui;
pub(crate) mod worker_claims;

pub use retired::retired_hint;

#[cfg(test)]
pub(crate) static PROCESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
