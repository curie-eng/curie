import { describe, expect, it } from "vitest";
import { initialState, reducer } from "./store";
import type { AppState, PromotedEvalCase } from "./types";

const fresh = (): AppState => initialState();

describe("reducer state machine", () => {
  it("opens a factory trace in observability", () => {
    const s = reducer(fresh(), { type: "openFactoryTrace", id: "trace-1" });
    expect(s.nav).toBe("observability");
    expect(s.obsTab).toBe("traces");
    expect(s.traceOpen).toBe("trace-1");
  });

  it("go navigates and clears transient sub-views", () => {
    let s = reducer(fresh(), { type: "openTrace", id: "tr_8f3k21" });
    s = reducer(s, { type: "viewTraces", agentId: "agent-1" });
    s = reducer(s, { type: "go", nav: "agents" });
    expect(s.nav).toBe("agents");
    expect(s.agentDetail).toBeNull();
    expect(s.traceOpen).toBeNull();
    expect(s.tracesAgentId).toBeNull();
    expect(s.logsPod).toBeNull();
  });

  it("confettiFire lands on overview and fires confetti exactly once", () => {
    let s = reducer(fresh(), { type: "deployStart" });
    expect(s.deploying).toBe(true);
    s = reducer(s, { type: "confettiFire" });
    expect(s.deploying).toBe(false);
    expect(s.nav).toBe("overview");
    expect(s.modal).toBeNull();
    expect(s.confetti).toBe(true);
    expect(s.confettiDone).toBe(true);

    // A second deploy does not re-fire confetti.
    s = reducer(s, { type: "confettiDone" });
    s = reducer(s, { type: "deployStart" });
    s = reducer(s, { type: "confettiFire" });
    expect(s.confetti).toBe(false);
  });

  it("deploy validation failure surfaces issues and stops the spinner", () => {
    let s = reducer(fresh(), { type: "deployStart" });
    s = reducer(s, { type: "deployFailedValidation", issues: [{ code: "x", message: "bad", location: "skill.md" }] });
    expect(s.deploying).toBe(false);
    expect(s.deployIssues?.length).toBe(1);
    expect(s.deployError).toBeNull();
    s = reducer(s, { type: "clearDeployErrors" });
    expect(s.deployIssues).toBeNull();
  });

  it("deploy failure surfaces a message", () => {
    const s = reducer(reducer(fresh(), { type: "deployStart" }), { type: "deployFailed", message: "boom" });
    expect(s.deploying).toBe(false);
    expect(s.deployError).toBe("boom");
    expect(s.deployIssues).toBeNull();
  });

  it("addPromotedEvalCase prepends newest-first and dedupes by id", () => {
    const a: PromotedEvalCase = { id: "e1", input: "hi", grader: { kind: "exact", expected: "ok", case_sensitive: false } };
    const b: PromotedEvalCase = { id: "e2", input: "yo", grader: { kind: "contains", expected: "y", case_sensitive: false } };
    const a2: PromotedEvalCase = { id: "e1", input: "hi again", grader: { kind: "exact", expected: "ok", case_sensitive: false } };
    let s = reducer(fresh(), { type: "addPromotedEvalCase", evalCase: a });
    s = reducer(s, { type: "addPromotedEvalCase", evalCase: b });
    s = reducer(s, { type: "addPromotedEvalCase", evalCase: a2 });
    expect(s.promotedEvalCases.map((c) => c.id)).toEqual(["e1", "e2"]);
    expect(s.promotedEvalCases[0].input).toBe("hi again");
  });

  it("setEnv toggles the environment", () => {
    const s = reducer(fresh(), { type: "setEnv", env: "dev" });
    expect(s.env).toBe("dev");
  });

  it("viewTraces jumps to the traces tab filtered to an agent", () => {
    const s = reducer(fresh(), { type: "viewTraces", agentId: "agent-123" });
    expect(s.nav).toBe("observability");
    expect(s.obsTab).toBe("traces");
    expect(s.tracesAgentId).toBe("agent-123");
    expect(s.traceOpen).toBeNull();
  });

  it("openLogs jumps to the logs tab preselected to the serving sandbox", () => {
    const s = reducer(fresh(), { type: "openLogs", sandboxId: "sbx-42" });
    expect(s.nav).toBe("observability");
    expect(s.obsTab).toBe("logs");
    expect(s.logsPod).toBe("sbx-42");
    expect(s.traceOpen).toBeNull();
  });

  it("a manual tab switch or nav clears the sandbox-logs prefill", () => {
    let s = reducer(fresh(), { type: "openLogs", sandboxId: "sbx-42" });
    s = reducer(s, { type: "setObsTab", tab: "traces" });
    expect(s.logsPod).toBeNull();
    s = reducer(reducer(fresh(), { type: "openLogs", sandboxId: "sbx-9" }), { type: "go", nav: "agents" });
    expect(s.logsPod).toBeNull();
  });

  it("changing tab or nav clears the agent trace filter", () => {
    let s = reducer(fresh(), { type: "viewTraces", agentId: "agent-123" });
    s = reducer(s, { type: "setObsTab", tab: "metrics" });
    expect(s.tracesAgentId).toBeNull();
    s = reducer(reducer(fresh(), { type: "viewTraces", agentId: "agent-9" }), { type: "go", nav: "agents" });
    expect(s.tracesAgentId).toBeNull();
  });
});
