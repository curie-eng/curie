//! NDJSON stream handling for the ACI outbound channel.
//!
//! The runner answers `POST /v1/event` with a stream of newline-delimited JSON
//! frames, each one an `OutboundEvent` from the frozen contract. The splitter
//! reassembles lines across arbitrary chunk boundaries; parsing delegates to the
//! generated crate, whose deserializer enforces the protocol version.

use anyhow::{Context, Result};
use curie_aci_protocol::OutboundEvent;

/// Reassembles complete lines from a chunked byte stream.
#[derive(Default)]
pub struct LineSplitter {
    buf: Vec<u8>,
}

impl LineSplitter {
    /// Feed a chunk; returns every complete line it closed (without the `\n`).
    pub fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(chunk);
        let mut lines = Vec::new();
        while let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
            let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
            line.pop(); // the \n
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            lines.push(String::from_utf8_lossy(&line).into_owned());
        }
        lines
    }

    /// Any trailing bytes after the stream ends (a final line with no `\n`).
    pub fn finish(self) -> Option<String> {
        if self.buf.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(&self.buf).into_owned())
        }
    }
}

/// Parse one NDJSON line into a frozen-contract outbound event.
///
/// A protocol-version mismatch or an unknown frame shape is a hard error: the
/// contract is versioned and the CLI must not silently reinterpret frames.
pub fn parse_outbound(line: &str) -> Result<OutboundEvent> {
    serde_json::from_str(line).with_context(|| format!("invalid ACI outbound frame: {line}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use curie_aci_protocol::{SessionStatus, PROTOCOL_VERSION};

    #[test]
    fn splits_lines_across_chunk_boundaries() {
        let mut splitter = LineSplitter::default();
        assert_eq!(splitter.push(b"{\"a\":1}\n{\"b\""), vec!["{\"a\":1}"]);
        assert_eq!(splitter.push(b":2}\n"), vec!["{\"b\":2}"]);
        assert_eq!(splitter.finish(), None);
    }

    #[test]
    fn strips_carriage_returns_and_reports_trailing_bytes() {
        let mut splitter = LineSplitter::default();
        assert_eq!(splitter.push(b"one\r\ntail"), vec!["one"]);
        assert_eq!(splitter.finish(), Some("tail".to_string()));
    }

    #[test]
    fn parses_a_final_frame() {
        let line = format!(
            "{{\"type\":\"final\",\"version\":\"{PROTOCOL_VERSION}\",\"text\":\"done\",\"status\":\"done\"}}"
        );
        let event = parse_outbound(&line).unwrap();
        assert_eq!(
            event,
            OutboundEvent::Final {
                version: PROTOCOL_VERSION.to_string(),
                text: "done".to_string(),
                status: SessionStatus::Done,
                approval_summary: None,
                approval_route: None,
                approval_gate_kind: None,
                approval_granted_tool: None,
                input_tokens: None,
                output_tokens: None,
                adoption_applied: None,
            }
        );
    }

    #[test]
    fn rejects_an_off_version_frame() {
        let line = r#"{"type":"final","version":"9.9.9","text":"x","status":"done"}"#;
        assert!(parse_outbound(line).is_err());
    }
}
