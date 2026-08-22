//! Terminal rendering: per-event output lines and the boxed `skill up` summary.
//!
//! The boxed summary follows the design canon (claude-design-prompt.md): a
//! Supabase-style rounded box listing the local bot URL, the emulator and eval
//! commands, and the version line.

use curie_aci_protocol::{OutboundEvent, SessionStatus};

/// Human-readable session status, matching the wire vocabulary.
pub fn status_str(status: &SessionStatus) -> &'static str {
    match status {
        SessionStatus::Done => "done",
        SessionStatus::IdleAwaitingInput => "idle-awaiting-input",
        SessionStatus::ClassifiedFailure => "classified-failure",
        SessionStatus::AwaitingApproval => "awaiting-approval",
    }
}

/// One piece of a streamed turn, tagged by which stream it belongs on so the
/// caller can route it: `Token` is agent answer text (raw payload -> stdout),
/// `Note` is tool/side-effect chatter (dim diagnostics -> stderr), `Fail` is an
/// error line (red diagnostics -> stderr), and `Status` is the final trailer
/// (dim diagnostics -> stderr).
pub enum TurnPart {
    Token(String),
    Note(String),
    Fail(String),
    Status(String),
}

/// Classifies the outbound events of one turn into stream-tagged parts.
///
/// Text deltas are answer tokens. Tool notes and side-effect flags are notes.
/// Error events are failures. The final frame becomes a status trailer, except
/// when nothing was streamed before it and it carries text: then the answer is
/// returned as a `Token` (so a turn that only yields a final still shows its
/// answer to stdout) and the caller appends the status trailer itself.
#[derive(Default)]
pub struct TurnPrinter {
    streamed_text: bool,
}

impl TurnPrinter {
    /// The stream-tagged part for one event, if any.
    pub fn part_for(&mut self, event: &OutboundEvent) -> Option<TurnPart> {
        match event {
            OutboundEvent::TextDelta { text, .. } => {
                self.streamed_text = true;
                Some(TurnPart::Token(text.clone()))
            }
            OutboundEvent::ToolNote { text, tool, .. } => match tool {
                Some(tool) => Some(TurnPart::Note(format!("  -> [{tool}] {text}"))),
                None => Some(TurnPart::Note(format!("  -> {text}"))),
            },
            OutboundEvent::SideEffectFlag { tool, detail, .. } => {
                let tool = tool.as_deref().unwrap_or("unknown tool");
                let detail = detail
                    .as_deref()
                    .map(|d| format!(": {d}"))
                    .unwrap_or_default();
                Some(TurnPart::Note(format!(
                    "  !  side effect via {tool}{detail}"
                )))
            }
            OutboundEvent::ErrorEvent {
                message,
                classification,
                ..
            } => {
                let class = classification
                    .as_deref()
                    .map(|c| format!(" [{c}]"))
                    .unwrap_or_default();
                Some(TurnPart::Fail(format!("error{class}: {message}")))
            }
            OutboundEvent::Final { text, status, .. } => {
                if self.streamed_text || text.is_empty() {
                    Some(TurnPart::Status(format!(
                        "-- final ({})",
                        status_str(status)
                    )))
                } else {
                    // Nothing streamed: surface the final answer as a token; the
                    // caller prints it to stdout then adds the status trailer.
                    Some(TurnPart::Token(text.clone()))
                }
            }
        }
    }
}

/// Render the boxed environment summary the design specifies for `curie skill up`.
pub fn boxed_summary(title: &str, rows: &[(&str, String)]) -> String {
    let label_width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    let body: Vec<String> = rows
        .iter()
        .map(|(label, value)| format!("  {label:<label_width$}   {value}"))
        .collect();
    // Inner width: the character count between the two side bars. The title
    // head occupies "- <title> " (3 + title) on the top rule, plus at least a
    // couple of trailing rule characters so the box always closes with a rule.
    let inner = body
        .iter()
        .map(String::len)
        .max()
        .unwrap_or(0)
        .max(title.len() + 5)
        + 2;

    let mut out = String::new();
    out.push('\u{256d}'); // rounded top-left
    out.push_str(&format!("\u{2500} {title} "));
    out.push_str(&"\u{2500}".repeat(inner.saturating_sub(title.len() + 3)));
    out.push('\u{256e}');
    out.push('\n');
    for line in &body {
        out.push('\u{2502}');
        out.push_str(&format!("{line:<inner$}"));
        out.push('\u{2502}');
        out.push('\n');
    }
    out.push('\u{2570}');
    out.push_str(&"\u{2500}".repeat(inner));
    out.push('\u{256f}');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use curie_aci_protocol::PROTOCOL_VERSION;

    fn v() -> String {
        PROTOCOL_VERSION.to_string()
    }

    /// The text of a `Token`/`Note`/`Fail`/`Status` part, for assertions.
    fn part_text(part: Option<TurnPart>) -> String {
        match part.expect("a part") {
            TurnPart::Token(s) | TurnPart::Note(s) | TurnPart::Fail(s) | TurnPart::Status(s) => s,
        }
    }

    #[test]
    fn streams_deltas_as_tokens_then_final_is_a_status_without_repeating_text() {
        let mut printer = TurnPrinter::default();
        let delta = OutboundEvent::TextDelta {
            version: v(),
            text: "all done".into(),
        };
        let final_frame = OutboundEvent::Final {
            version: v(),
            text: "all done".into(),
            status: SessionStatus::Done,
            approval_summary: None,
            approval_route: None,
            approval_gate_kind: None,
            approval_granted_tool: None,
            input_tokens: None,
            output_tokens: None,
        };
        // A delta routes to stdout as a raw token.
        assert!(matches!(printer.part_for(&delta), Some(TurnPart::Token(t)) if t == "all done"));
        // After streaming, the final only contributes the status trailer.
        assert!(
            matches!(printer.part_for(&final_frame), Some(TurnPart::Status(s)) if s == "-- final (done)")
        );
    }

    #[test]
    fn returns_final_text_as_a_token_when_nothing_was_streamed() {
        let mut printer = TurnPrinter::default();
        let final_frame = OutboundEvent::Final {
            version: v(),
            text: "quiet answer".into(),
            status: SessionStatus::IdleAwaitingInput,
            approval_summary: None,
            approval_route: None,
            approval_gate_kind: None,
            approval_granted_tool: None,
            input_tokens: None,
            output_tokens: None,
        };
        // The caller prints this token to stdout, then appends the status trailer.
        assert!(
            matches!(printer.part_for(&final_frame), Some(TurnPart::Token(t)) if t == "quiet answer")
        );
    }

    #[test]
    fn an_empty_final_is_a_status_even_without_streaming() {
        let mut printer = TurnPrinter::default();
        let final_frame = OutboundEvent::Final {
            version: v(),
            text: String::new(),
            status: SessionStatus::Done,
            approval_summary: None,
            approval_route: None,
            approval_gate_kind: None,
            approval_granted_tool: None,
            input_tokens: None,
            output_tokens: None,
        };
        assert!(
            matches!(printer.part_for(&final_frame), Some(TurnPart::Status(s)) if s == "-- final (done)")
        );
    }

    #[test]
    fn routes_tool_notes_side_effects_and_errors_to_note_and_fail() {
        let mut printer = TurnPrinter::default();
        let note = OutboundEvent::ToolNote {
            version: v(),
            text: "running echo hi".into(),
            tool: Some("Bash".into()),
        };
        let flag = OutboundEvent::SideEffectFlag {
            version: v(),
            tool: Some("Bash".into()),
            detail: None,
            // Optional on the wire, but a Rust struct-variant literal has to
            // name every field, so an additive ACI field is free for readers
            // and not free for constructors.
            arguments: None,
            result: None,
        };
        let error = OutboundEvent::ErrorEvent {
            version: v(),
            message: "boom".into(),
            classification: Some("budget".into()),
        };
        // Tool note and side-effect flag are diagnostics -> Note (stderr).
        assert!(matches!(printer.part_for(&note), Some(TurnPart::Note(_))));
        assert_eq!(
            part_text(printer.part_for(&note)),
            "  -> [Bash] running echo hi"
        );
        assert_eq!(
            part_text(printer.part_for(&flag)),
            "  !  side effect via Bash"
        );
        // Error events route to Fail (red stderr).
        assert!(matches!(
            printer.part_for(&error),
            Some(TurnPart::Fail(f)) if f == "error [budget]: boom"
        ));
    }

    #[test]
    fn boxed_summary_lines_are_flush() {
        let rows = [
            ("Local bot", "http://localhost:7245".to_string()),
            (
                "Slack emulator",
                "curie skill message \"<message>\"".to_string(),
            ),
            ("Eval runner", "curie skill eval".to_string()),
            ("Version", "dev @ 4f2c91a".to_string()),
        ];
        let boxed = boxed_summary("curie dev environment", &rows);
        let lines: Vec<&str> = boxed.lines().collect();
        assert_eq!(lines.len(), rows.len() + 2);
        let width = lines[0].chars().count();
        for line in &lines {
            assert_eq!(line.chars().count(), width, "misaligned line: {line}");
        }
        assert!(lines[0].contains("curie dev environment"));
        assert!(lines[1].contains("Local bot"));
        assert!(lines[1].contains("http://localhost:7245"));
    }
}
