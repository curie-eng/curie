import { describe, expect, it } from "vitest";
import vectors from "../../../../tests/vectors/user-action-wording.json";
import { actionLabel, approvalSummary } from "./actionWording";
describe("plain action presentation", () => {
  it.each(vectors.vectors)("matches the shared label for $tool", ({ tool, label }) => {
    expect(actionLabel(tool)).toBe(label);
  });
  it("keeps approval values when reading an old API response", () => {
    expect(approvalSummary('Tool call awaiting approval: mcp__acme__file_attachment {"file_name":"example.pdf"}')).toBe("Approve file attachment. File name: example.pdf");
  });
  it("does not describe a truncated request as complete", () => {
    expect(approvalSummary('Tool call awaiting approval: mcp__acme__file_attachment {"file_name":')).toContain("Details are incomplete");
  });
  it("preserves nested document keys while naming the root field", () => {
    const nested = { mcp__acme__file_attachment: "draft", "example.pdf": "literal", customer_id: "keep" };
    const args = { file_contents: nested };
    const before = JSON.stringify(args);
    const display = approvalSummary(`Tool call awaiting approval: mcp__acme__file_attachment ${before}`);
    expect(display).toMatch(/^Approve file attachment\. File contents: /);
    for (const [key, value] of Object.entries(nested)) {
      expect(display).toContain(key);
      expect(display).toContain(value);
    }
    expect(display).not.toContain("Customer id: keep");
    expect(JSON.stringify(args)).toBe(before);
  });

});
