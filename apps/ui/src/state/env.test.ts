import { describe, expect, it } from "vitest";
import { agentIdsForEnv, hiddenAgentIdsForEnv } from "./env";
import type { DeploymentOut, Environment } from "../api/client";

// Deployment builder mirroring the fixtures used in hooks.test.ts / WiredVersions.test.ts.
const dep = (
  agent_id: string,
  environment: Environment,
  status = "active",
): DeploymentOut => ({
  id: `dep-${agent_id}-${environment}-${status}`,
  agent_id,
  version_id: `ver-${agent_id}-${environment}`,
  environment,
  commit_sha: null,
  status,
  deployed_at: "2026-07-08T00:00:00Z",
  workspace_enabled: false,
});

describe("agentIdsForEnv (client-side env scoping)", () => {
  it("returns the agent_ids with an ACTIVE deployment in the target env", () => {
    const deployments = [
      dep("a1", "prod"),
      dep("a2", "prod"),
      dep("a3", "dev"),
    ];
    const prod = agentIdsForEnv(deployments, "prod");
    expect(prod).toBeInstanceOf(Set);
    expect(prod.has("a1")).toBe(true);
    expect(prod.has("a2")).toBe(true);
    // a3 lives only in dev, so it is NOT in the prod set.
    expect(prod.has("a3")).toBe(false);
    expect(prod.size).toBe(2);
  });

  it("an agent deployed only to dev is absent from the prod set and present in the dev set", () => {
    const deployments = [dep("dev-only", "dev")];
    expect(agentIdsForEnv(deployments, "prod").has("dev-only")).toBe(false);
    expect(agentIdsForEnv(deployments, "dev").has("dev-only")).toBe(true);
  });

  it("an agent deployed to both envs appears in both sets", () => {
    const deployments = [dep("both", "prod"), dep("both", "dev")];
    expect(agentIdsForEnv(deployments, "prod").has("both")).toBe(true);
    expect(agentIdsForEnv(deployments, "dev").has("both")).toBe(true);
  });

  it("ignores non-active deployments (superseded/stopped do not count)", () => {
    const deployments = [
      dep("a1", "prod", "superseded"),
      dep("a2", "prod", "stopped"),
      dep("a3", "prod", "active"),
    ];
    const prod = agentIdsForEnv(deployments, "prod");
    expect(prod.has("a1")).toBe(false);
    expect(prod.has("a2")).toBe(false);
    expect(prod.has("a3")).toBe(true);
    expect(prod.size).toBe(1);
  });

  it("returns an empty set when there are no deployments", () => {
    const empty = agentIdsForEnv([], "prod");
    expect(empty).toBeInstanceOf(Set);
    expect(empty.size).toBe(0);
  });
});

describe("hiddenAgentIdsForEnv (client-side env visibility)", () => {
  it("does NOT hide an undeployed agent (no active deployments anywhere)", () => {
    // An agent with no deployment records at all is not represented here, and an
    // agent whose only deployment is non-active must not be hidden either.
    const deployments = [dep("never-deployed", "dev", "stopped")];
    expect(hiddenAgentIdsForEnv(deployments, "prod").has("never-deployed")).toBe(false);
    expect(hiddenAgentIdsForEnv([], "prod").size).toBe(0);
  });

  it("does NOT hide an agent active in the selected env", () => {
    const deployments = [dep("here", "prod")];
    expect(hiddenAgentIdsForEnv(deployments, "prod").has("here")).toBe(false);
  });

  it("HIDES an agent active only in the other env", () => {
    const deployments = [dep("dev-only", "dev")];
    const hiddenInProd = hiddenAgentIdsForEnv(deployments, "prod");
    expect(hiddenInProd.has("dev-only")).toBe(true);
    // ...but it is visible in its own env.
    expect(hiddenAgentIdsForEnv(deployments, "dev").has("dev-only")).toBe(false);
  });

  it("does NOT hide an agent active in both envs", () => {
    const deployments = [dep("both", "prod"), dep("both", "dev")];
    expect(hiddenAgentIdsForEnv(deployments, "prod").has("both")).toBe(false);
    expect(hiddenAgentIdsForEnv(deployments, "dev").has("both")).toBe(false);
  });

  it("a non-active other-env deployment does NOT hide the agent", () => {
    // superseded/stopped deployments in the other env are not live, so they
    // neither place the agent in that env nor hide it from this one.
    const deployments = [dep("stale", "dev", "superseded"), dep("stale", "dev", "stopped")];
    expect(hiddenAgentIdsForEnv(deployments, "prod").has("stale")).toBe(false);
  });
});

