// Secrets, delegated to `curie secrets` so the desktop app never becomes a
// second place credentials live.
//
// Two details matter. The value is handed to the CLI through an environment
// variable and `--from-env`, never as an argv token, because argv is world
// readable in `ps` on every platform this ships to. And nothing ever reads a
// value back: `list` returns names, which is all the CLI itself will return.

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { randomBytes } from "node:crypto";

import { findCli, searchPath, defaultCwd, runOnce } from "./cli.js";

const execFileAsync = promisify(execFile);

export async function list(): Promise<string[]> {
  const { stdout, code } = await runOnce(["secrets", "list", "--json"]);
  if (code !== 0) return [];
  try {
    const parsed = JSON.parse(stdout) as { secrets?: string[] };
    return parsed.secrets ?? [];
  } catch {
    return [];
  }
}

export async function set(name: string, value: string): Promise<void> {
  const cli = findCli();
  if (!cli) throw new Error("curie is not on PATH");
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`${name} is not a valid secret name`);
  }
  // A random carrier name so a concurrent set cannot pick up the wrong value,
  // and so the variable never collides with something already in the env.
  const carrier = `CURIE_DESKTOP_SECRET_${randomBytes(8).toString("hex").toUpperCase()}`;
  await execFileAsync(cli, ["secrets", "set", name, "--from-env", carrier], {
    cwd: defaultCwd(),
    env: { ...process.env, PATH: searchPath(), [carrier]: value },
    timeout: 15_000,
  });
}

export async function unset(name: string): Promise<void> {
  const { code, stderr } = await runOnce(["secrets", "unset", name]);
  if (code !== 0) throw new Error(stderr || `could not remove ${name}`);
}
