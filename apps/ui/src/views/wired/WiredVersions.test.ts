import { describe, expect, it } from "vitest";
import { buildRows } from "./WiredVersions";
import type { VersionOut, DeploymentOut } from "../../api/client";

const v = (id: string, created_at: string): VersionOut => ({
  id,
  agent_id: "a1",
  version_label: id,
  bundle_ref: "r",
  bundle_sha256: "s",
  commit_sha: null,
  created_by: "ui",
  created_at,
});

const d = (version_id: string, environment: "prod" | "dev", deployed_at: string, status = "active"): DeploymentOut => ({
  id: `dep-${version_id}-${environment}`,
  agent_id: "a1",
  version_id,
  environment,
  commit_sha: null,
  status,
  deployed_at,
  workspace_enabled: false,
});

describe("buildRows (versions joined with deployments)", () => {
  it("emits one row per deployment and keeps undeployed versions", () => {
    const versions = [v("v1", "2026-07-01T00:00:00Z"), v("v2", "2026-07-02T00:00:00Z")];
    const deployments = [d("v1", "prod", "2026-07-03T00:00:00Z"), d("v1", "dev", "2026-07-04T00:00:00Z")];
    const rows = buildRows(versions, deployments);
    // v1 has two deployments (prod + dev), v2 has none -> one bare row.
    expect(rows).toHaveLength(3);
    const undeployed = rows.filter((r) => r.deployment === null);
    expect(undeployed.map((r) => r.version.id)).toEqual(["v2"]);
  });

  it("orders newest activity first (deployed_at, else created_at)", () => {
    const versions = [v("v1", "2026-07-01T00:00:00Z"), v("v2", "2026-07-05T00:00:00Z")];
    const deployments = [d("v1", "prod", "2026-07-09T00:00:00Z")];
    const rows = buildRows(versions, deployments);
    // v1's deployment (07-09) outranks v2's created_at (07-05).
    expect(rows[0].version.id).toBe("v1");
    expect(rows[0].deployment?.environment).toBe("prod");
    expect(rows[1].version.id).toBe("v2");
    expect(rows[1].deployment).toBeNull();
  });
});
