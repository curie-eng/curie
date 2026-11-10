import { describe, expect, it } from "vitest";
import { pickActiveVersion } from "./hooks";
import type { VersionOut, DeploymentOut } from "./client";

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

describe("pickActiveVersion", () => {
  it("prefers the prod active deployment over a newer dev one (matches the worker)", () => {
    const versions = [v("v1", "2026-07-01"), v("v2", "2026-07-02")];
    const deployments = [
      d("v1", "prod", "2026-07-01T00:00:00Z"),
      d("v2", "dev", "2026-07-02T00:00:00Z"), // newer, but dev
    ];
    expect(pickActiveVersion(versions, deployments)).toBe("v1");
  });

  it("picks the newest active deployment within the same environment", () => {
    const versions = [v("v1", "2026-07-01"), v("v2", "2026-07-02")];
    const deployments = [
      d("v1", "prod", "2026-07-01T00:00:00Z"),
      d("v2", "prod", "2026-07-03T00:00:00Z"),
    ];
    expect(pickActiveVersion(versions, deployments)).toBe("v2");
  });

  it("ignores non-active deployments and stale version refs", () => {
    const versions = [v("v2", "2026-07-02")];
    const deployments = [
      d("v1", "prod", "2026-07-05T00:00:00Z", "superseded"), // not active
      d("v9", "prod", "2026-07-06T00:00:00Z"), // active but version not present
      d("v2", "prod", "2026-07-04T00:00:00Z"),
    ];
    expect(pickActiveVersion(versions, deployments)).toBe("v2");
  });

  it("falls back to the newest version when there are no active deployments", () => {
    const versions = [v("v1", "2026-07-01"), v("v2", "2026-07-03")];
    expect(pickActiveVersion(versions, [])).toBe("v2");
  });

  it("returns null when there are no versions", () => {
    expect(pickActiveVersion([], [])).toBeNull();
  });
});
