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
});
