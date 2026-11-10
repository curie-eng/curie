// @vitest-environment node

import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const generator = fileURLToPath(new URL("../../scripts/gen-api-types.mjs", import.meta.url));
const source = fileURLToPath(new URL("../../../api/openapi.json", import.meta.url));

// Each case runs the generator over the whole OpenAPI document up to four times.
describe("generated API type gate", { timeout: 30_000 }, () => {
  let directory: string;
  let schemaPath: string;
  let outputPath: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "curie-ui-api-types-"));
    schemaPath = join(directory, "openapi.json");
    outputPath = join(directory, "api.ts");
    writeFileSync(schemaPath, readFileSync(source));
  });

  afterEach(() => {
    rmSync(directory, { recursive: true, force: true });
  });

  function run(check = false) {
    return spawnSync(process.execPath, [
      generator,
      "--schema", schemaPath,
      "--output", outputPath,
      ...(check ? ["--check"] : []),
    ], { encoding: "utf8" });
  }

  it("accepts current types and leaves the committed artifact unchanged", () => {
    expect(run().status).toBe(0);
    const generated = readFileSync(outputPath, "utf8");
    expect(run(true).status).toBe(0);
    expect(readFileSync(outputPath, "utf8")).toBe(generated);
  });

  it("rejects a changed API response field until types are regenerated", () => {
    expect(run().status).toBe(0);
    const generated = readFileSync(outputPath, "utf8");
    const schema = JSON.parse(readFileSync(schemaPath, "utf8"));
    schema.components.schemas.AgentOut.properties.verification_note = { type: "string" };
    schema.components.schemas.AgentOut.required.push("verification_note");
    writeFileSync(schemaPath, JSON.stringify(schema));

    const stale = run(true);
    expect(stale.status).toBe(1);
    expect(stale.stderr).toContain("API types are stale");
    expect(readFileSync(outputPath, "utf8")).toBe(generated);

    expect(run().status).toBe(0);
    expect(readFileSync(outputPath, "utf8")).toContain("verification_note: string");
    expect(run(true).status).toBe(0);
  });

  it("rejects a missing generated artifact", () => {
    const missing = run(true);
    expect(missing.status).toBe(1);
    expect(missing.stderr).toContain("API types are missing");
  });
});
