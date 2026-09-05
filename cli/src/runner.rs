//! HTTP client for the runner's ACI channel.
//!
//! Speaks the frozen contract only: inbound frames are the generated
//! `InboundMessage`, the `/v1/event` response is an NDJSON stream of
//! `OutboundEvent` frames (version-enforced at deserialization).

use std::time::Duration;

use anyhow::{bail, Context, Result};
use curie_aci_protocol::{EventType, InboundMessage, OutboundEvent};
use futures_util::StreamExt;

use crate::ndjson::{parse_outbound, LineSplitter};

pub struct RunnerClient {
    base_url: String,
    http: reqwest::Client,
}

impl RunnerClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    pub async fn healthy(&self) -> bool {
        matches!(
            self.http
                .get(format!("{}/healthz", self.base_url))
                .send()
                .await,
            Ok(resp) if resp.status().is_success()
        )
    }

    /// Poll `/healthz` until the runner answers or the deadline passes.
    pub async fn wait_healthy(&self, deadline: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if self.healthy().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        bail!(
            "runner at {} did not become healthy within {:?}",
            self.base_url,
            deadline
        )
    }

    pub async fn status(&self) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/status", self.base_url))
            .send()
            .await
            .map_err(|error| self.request_error(error, format!("GET {}/status", self.base_url)))?;
        if !resp.status().is_success() {
            bail!("GET /status returned {}", resp.status());
        }
        resp.json().await.context("decoding /status body")
    }

    /// Discard the runner's conversation so the next turn starts fresh (#550).
    ///
    /// `curie skill eval` calls this between cases to enforce per-case
    /// isolation: a case must not answer from an earlier case's history instead
    /// of actually invoking its tools. Not an ACI wire frame -- a runner control
    /// route like `/status`, so it takes no body. A 409 (a turn is still active)
    /// surfaces as an error; the eval loop is sequential, so a turn is never live
    /// at reset time.
    pub async fn reset(&self) -> Result<()> {
        let resp = self
            .http
            .post(format!("{}/v1/reset", self.base_url))
            .send()
            .await
            .map_err(|error| {
                self.request_error(error, format!("POST {}/v1/reset", self.base_url))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("POST /v1/reset returned {status}: {}", body.trim());
        }
        Ok(())
    }

    /// Open a turn: POST the event frame, stream back the outbound events.
    ///
    /// `on_event` fires per frame as it arrives (live streaming to the
    /// terminal); the full ordered list is returned for callers that assert on
    /// the turn (evals). The turn must terminate in a `final` frame.
    pub async fn send_event(
        &self,
        event_type: EventType,
        text: &str,
        user: &str,
        mut on_event: impl FnMut(&OutboundEvent),
    ) -> Result<Vec<OutboundEvent>> {
        let frame = InboundMessage::Event {
            r#type: event_type,
            text: text.to_string(),
            user: user.to_string(),
            ts: slack_ts(),
            session_id: None,
            history_ref: None,
            adoption_credential: None,
        };
        let resp = self
            .http
            .post(format!("{}/v1/event", self.base_url))
            .json(&frame)
            .send()
            .await
            .map_err(|error| {
                self.request_error(error, format!("POST {}/v1/event", self.base_url))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("POST /v1/event returned {status}: {}", body.trim());
        }

        let mut events = Vec::new();
        let mut splitter = LineSplitter::default();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("reading NDJSON stream")?;
            for line in splitter.push(&chunk) {
                if line.trim().is_empty() {
                    continue;
                }
                let event = parse_outbound(&line)?;
                on_event(&event);
                events.push(event);
            }
        }
        if let Some(tail) = splitter.finish() {
            if !tail.trim().is_empty() {
                let event = parse_outbound(&tail)?;
                on_event(&event);
                events.push(event);
            }
        }

        if !matches!(events.last(), Some(OutboundEvent::Final { .. })) {
            bail!(
                "stream ended without a final frame ({} events)",
                events.len()
            );
        }
        Ok(events)
    }

    fn request_error(&self, error: reqwest::Error, operation: String) -> anyhow::Error {
        let unreachable = error.is_connect() || error.is_timeout();
        let source = anyhow::Error::from(error).context(operation);
        if unreachable {
            crate::exit::operator_context(
                source,
                format!(
                    "The local runner at {} could not be reached.",
                    self.base_url
                ),
                Some("Run `curie skill status` to check the local runner.".to_string()),
            )
        } else {
            let remedy = if self.base_url.contains("://") {
                "Pass an absolute runner URL beginning with `http://` or `https://`.".to_string()
            } else {
                format!("Pass the absolute runner URL `http://{}`.", self.base_url)
            };
            crate::exit::operator_context(
                source,
                format!("The configured runner URL `{}` is invalid.", self.base_url),
                Some(remedy),
            )
        }
    }
}

/// A Slack-style event timestamp: `<unix seconds>.<microseconds>`.
fn slack_ts() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the epoch");
    format!("{}.{:06}", now.as_secs(), now.subsec_micros())
}

#[cfg(test)]
mod tests {
    use super::slack_ts;

    #[test]
    fn slack_ts_has_the_wire_shape() {
        let ts = slack_ts();
        let (secs, micros) = ts.split_once('.').expect("dot separator");
        assert!(secs.parse::<u64>().is_ok());
        assert_eq!(micros.len(), 6);
        assert!(micros.parse::<u32>().is_ok());
    }
}
