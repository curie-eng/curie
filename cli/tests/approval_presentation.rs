use curie::api::ApprovalRecord;
use curie::approval_wording::approval_display;
use serde_json::json;

#[test]
fn approval_displays_keep_exact_records_and_requested_values() {
    let value = json!({
        "id":"approval-example", "author":"U0EXAMPLE1", "status":"pending",
        "conversation_id":"thread-example", "gate_kind":"permission",
        "granted_tool":"mcp__acme__file_attachment",
        "summary":"Tool call awaiting approval: mcp__acme__file_attachment {\"file_name\":\"example.pdf\"}",
        "display_summary":"Approve file attachment. File name: example.pdf"
    });
    let record: ApprovalRecord = serde_json::from_value(value).unwrap();
    assert_eq!(
        approval_display(&record),
        "Approve file attachment. File name: example.pdf"
    );
    assert_eq!(
        record.granted_tool.as_deref(),
        Some("mcp__acme__file_attachment")
    );
    assert!(record.summary.contains("mcp__acme__file_attachment"));
}

#[test]
fn legacy_approval_values_remain_visible_without_raw_tool_names() {
    let record: ApprovalRecord = serde_json::from_value(json!({
        "id":"approval-example", "author":"U0EXAMPLE1", "status":"pending",
        "conversation_id":"thread-example",
        "summary":"Tool call awaiting approval: mcp__acme__file_attachment {\"file_name\":\"example.pdf\"}"
    })).unwrap();
    assert_eq!(
        approval_display(&record),
        "Approve file attachment. File name: example.pdf"
    );
    assert!(record.granted_tool.is_none());
}

#[test]
fn truncated_approval_requires_review_instead_of_hiding_missing_details() {
    let record: ApprovalRecord = serde_json::from_value(json!({
        "id":"approval-example", "author":"U0EXAMPLE1", "status":"pending",
        "conversation_id":"thread-example",
        "summary":"Tool call awaiting approval: mcp__acme__file_attachment {\"file_name\":"
    }))
    .unwrap();
    assert_eq!(approval_display(&record), "Approve file attachment. Details are incomplete; review the original request before approving.");
}

#[test]
fn nested_document_keys_are_data_and_granted_tool_remains_exact() {
    let tool = "mcp__acme__file_attachment";
    let nested = json!({ "mcp__acme__file_attachment": "draft", "example.pdf": "literal", "customer_id": "keep", "record_values": ["1", 1, true, null, {"customer_id":"keep"}, "문서"] });
    let summary = format!(
        "Tool call awaiting approval: {tool} {}",
        json!({"file_contents": nested})
    );
    let record: ApprovalRecord = serde_json::from_value(json!({
        "id":"approval-example", "author":"U0EXAMPLE1", "status":"pending",
        "conversation_id":"thread-example", "granted_tool":tool,
        "summary":summary
    }))
    .unwrap();
    let display = approval_display(&record);
    let prefix = "Approve file attachment. File contents: ";
    assert_eq!(
        display,
        format!("{prefix}{}", serde_json::to_string(&nested).unwrap())
    );
    let shown: serde_json::Value =
        serde_json::from_str(display.strip_prefix(prefix).unwrap()).unwrap();
    assert_eq!(shown, nested);
    assert_eq!(record.granted_tool.as_deref(), Some(tool));
    assert_eq!(record.summary, summary);
}
