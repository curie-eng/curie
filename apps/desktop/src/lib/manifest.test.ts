// Parity tests.
//
// The app's central claim is that it exposes the CLI faithfully. Two things have
// to hold for that to be true, and both are checked here rather than asserted in
// a README:
//
//   1. Every runnable command in the manifest is reachable in the UI. If the CLI
//      grows a command and this app silently drops it, the UI has quietly become
//      the lesser surface -- which is the exact failure this app exists to avoid.
//   2. The command string the UI *shows* is the command the shell *runs*. Those
//      are produced by two independent code paths (the renderer renders a
//      string, the main process builds argv), so they are compared directly. A
//      preview that lies is worse than no preview.

import { describe, expect, it } from "vitest";

import { resolve as resolveArgv } from "../../electron/ipc/manifest";
import { runtimeDefault, humanArg,
  commands,
  commandsById,
  fieldKind,
  NEEDS_TERMINAL,
  cwdFor,
  cwdReason,
  renderCommand,
  root,
  search,
  type Command,
  type Tier,
} from "./manifest";

/** Walk the raw manifest independently of `commands`, so a bug in the walker
 *  cannot make the coverage test pass by agreeing with itself.
 *
 *  `includeHidden` exists because clap marks a few commands hidden (today just
 *  `schema`, which exists to feed this very manifest to tooling). Those are
 *  absent from `curie --help`, so the app leaves them out too -- the UI mirrors
 *  the CLI's own visible surface rather than inventing a larger one. */
function rawLeaves(node: typeof root, prefix: string[] = [], includeHidden = false): string[] {
  const out: string[] = [];
  for (const sub of node.subcommands ?? []) {
    if (sub.hidden && !includeHidden) continue;
    const path = [...prefix, sub.name];
    if ((sub.subcommands ?? []).length) out.push(...rawLeaves(sub, path, includeHidden));
    else out.push(path.join("."));
  }
  return out;
}

describe("manifest coverage", () => {
  it("exposes every runnable command the CLI declares", () => {
    const declared = rawLeaves(root).sort();
    const exposed = commands.map((c) => c.id).sort();
    expect(exposed).toEqual(declared);
  });

  it("omits exactly the commands clap itself hides", () => {
    const withHidden = rawLeaves(root, [], true);
    const withoutHidden = rawLeaves(root);
    const hidden = withHidden.filter((id) => !withoutHidden.includes(id));
    // Documented rather than asserted as a fixed list: if the CLI hides another
    // command tomorrow, this still passes and still explains itself.
    expect(hidden).toEqual(["schema"]);
    for (const id of hidden) expect(commandsById.has(id), `${id} should be hidden`).toBe(false);
  });

  it("has a non-trivial surface", () => {
    // A guard against the manifest failing to load and every test passing
    // vacuously against an empty array.
    expect(commands.length).toBeGreaterThan(50);
  });

  it("files every command under exactly one tier", () => {
    // The Commands view renders one section per tier and expects the sections
    // to partition the command list: a command in two sections would appear
    // twice, one in none would be unreachable.
    const tiers: Tier[] = ["author", "skill", "local", "cluster", "platform", "dev"];
    const seen = new Set<string>();
    for (const tier of tiers) {
      for (const cmd of commands.filter((c) => c.tier === tier)) {
        expect(seen.has(cmd.id), `${cmd.id} is in two tiers`).toBe(false);
        seen.add(cmd.id);
      }
    }
    expect(seen.size).toBe(commands.length);
  });

  it("assigns top-level commands the tier they actually belong to", () => {
    // The top level is the only place the tier varies command to command, and
    // it was previously getting one blanket tier for the whole group.
    expect(commandsById.get("init")?.tier).toBe("author");
    expect(commandsById.get("deploy-local")?.tier).toBe("local");
    expect(commandsById.get("doctor")?.tier).toBe("platform");
    expect(commandsById.get("apply")?.tier).toBe("platform");
    expect(commandsById.get("skill.up")?.tier).toBe("skill");
    expect(commandsById.get("cluster.up")?.tier).toBe("cluster");
    expect(commandsById.get("dev.contracts")?.tier).toBe("dev");
  });

  it("gives every command a description to render", () => {
    const missing = commands.filter((c) => !c.about.trim()).map((c) => c.id);
    expect(missing).toEqual([]);
  });

  it("assigns every flag a renderable field kind", () => {
    for (const cmd of commands) {
      for (const flag of cmd.flags) {
        expect(fieldKind(flag), `${cmd.id} --${flag.long}`).toBeTruthy();
      }
    }
  });
});

describe("commands that cannot run without a terminal", () => {
  it("names only commands that actually exist", () => {
    for (const id of Object.keys(NEEDS_TERMINAL)) {
      expect(commandsById.has(id), `${id} is not a command`).toBe(true);
    }
  });

  it("covers the CLI's own is_terminal() guards", () => {
    // Grounded in cli/src/interactive.rs and cli/src/secrets.rs, both of which
    // bail without a TTY. If either guard moves, this list should move with it.
    expect(Object.keys(NEEDS_TERMINAL).sort()).toEqual(["interactive", "secrets.set"]);
  });

  it("tells the operator what to use instead, not just that it will fail", () => {
    for (const [id, message] of Object.entries(NEEDS_TERMINAL)) {
      expect(message.length, id).toBeGreaterThan(40);
      expect(/instead|use /i.test(message), id).toBe(true);
    }
  });

  it("leaves the stdin-driven interviews runnable", () => {
    // `init` and `skill eval-init` read stdin rather than requiring a TTY, so
    // the transcript drawer answers them. Marking them terminal-only would
    // remove working functionality.
    expect(NEEDS_TERMINAL["init"]).toBeUndefined();
    expect(NEEDS_TERMINAL["skill.eval-init"]).toBeUndefined();
  });
});

describe("risk classification", () => {
  it("treats the teardown and delete commands as destructive", () => {
    for (const id of ["local.down", "cluster.down", "cluster.delete", "local.kill", "skill.down"]) {
      expect(commandsById.get(id)?.risk, id).toBe("destructive");
    }
  });

  it("leaves read-only commands unguarded", () => {
    for (const id of ["local.status", "cluster.status", "local.versions", "doctor", "diff"]) {
      expect(commandsById.get(id)?.risk, id).toBe("safe");
    }
  });

  it("catches a teardown-shaped command even if nobody listed it", () => {
    // The name heuristic is the safety net for commands added after the
    // explicit table was written.
    const unlisted = commands.filter((c) => /\.(down|delete|kill)$/.test(c.id));
    expect(unlisted.length).toBeGreaterThan(0);
    for (const cmd of unlisted) expect(cmd.risk, cmd.id).toBe("destructive");
  });
});

// --- the preview must equal what runs ---------------------------------------

/** Rebuild the display string the way the main process would, so the two are
 *  compared on equal terms. */
function argvDisplay(cmd: Command, positionals: string[], flags: Record<string, string | boolean>) {
  return resolveArgv(
    { action: cmd.id, positionals, flags, cwd: "/tmp/bundle" },
    "/tmp/bundle",
  ).display;
}

describe("preview matches the command that runs", () => {
  it("agrees on a plain command", () => {
    const cmd = commandsById.get("local.status")!;
    expect(renderCommand(cmd, [], {})).toBe(argvDisplay(cmd, [], {}));
  });

  it("agrees on positionals plus flags", () => {
    const cmd = commandsById.get("local.budget")!;
    const positionals = ["deal-desk"];
    const flags = { limit: "5.0", "api-url": "http://localhost:8000" };
    expect(renderCommand(cmd, positionals, flags)).toBe(argvDisplay(cmd, positionals, flags));
  });

  it("agrees on boolean flags, which render bare", () => {
    const cmd = commandsById.get("local.up")!;
    const flags = { minimal: true, slack: true };
    const rendered = renderCommand(cmd, [], flags);
    expect(rendered).toBe(argvDisplay(cmd, [], flags));
    expect(rendered).toContain("--minimal");
    expect(rendered).not.toContain("--minimal true");
  });

  it("agrees when a value needs quoting", () => {
    const cmd = commandsById.get("local.message")!;
    const positionals = ["what is the deal desk policy?"];
    const rendered = renderCommand(cmd, positionals, {});
    expect(rendered).toBe(argvDisplay(cmd, positionals, {}));
    expect(rendered).toContain("'what is the deal desk policy?'");
  });

  it("agrees across every command, for a synthetic fill of each flag", () => {
    // The real coverage test: fill every command's arguments and check the two
    // implementations still agree. This is what catches a divergence in a
    // command nobody wrote a bespoke case for.
    for (const cmd of commands) {
      const positionals = cmd.positionals.map((p) => `value-${p.id}`);
      const flags: Record<string, string | boolean> = {};
      for (const flag of cmd.flags) {
        const kind = fieldKind(flag);
        if (kind === "boolean") flags[flag.long!] = true;
        else if (kind === "enum") flags[flag.long!] = flag.possible_values![0];
        else if (kind === "number") flags[flag.long!] = "7";
        else flags[flag.long!] = `v-${flag.id}`;
      }
      expect(renderCommand(cmd, positionals, flags), cmd.id).toBe(
        argvDisplay(cmd, positionals, flags),
      );
    }
  });

  it("appends --json identically on both sides", () => {
    const cmd = commandsById.get("local.versions")!;
    const rendered = renderCommand(cmd, ["deal-desk"], {}, { json: true });
    const argv = resolveArgv(
      { action: cmd.id, positionals: ["deal-desk"], flags: {}, json: true },
      "/tmp",
    ).display;
    expect(rendered).toBe(argv);
    expect(rendered.endsWith("--json")).toBe(true);
  });
});

describe("argv resolution ignores what never reaches argv", () => {
  // The form seeds every boolean flag as `false`, so these are the ordinary
  // payload, not a malformed one. `renderCommand` omits them from the preview;
  // `resolve` has to omit them from argv without objecting to them, or the two
  // disagree about a command that runs identically either way.
  it("drops an unset flag rather than validating it", () => {
    const cmd = commandsById.get("local.up")!;
    const flags: Record<string, string | boolean> = { minimal: false, model: "" };
    expect(renderCommand(cmd, [], flags)).toBe("curie local up");
    expect(argvDisplay(cmd, [], flags)).toBe("curie local up");
  });

  it("ignores an unset flag it has never heard of", () => {
    // A renderer one manifest ahead of the main process sends exactly this. It
    // contributes nothing to argv, so it must not be able to fail the run --
    // that turned a harmless version skew into "could not start".
    expect(() =>
      resolveArgv(
        { action: "local.up", positionals: [], flags: { "not-a-real-flag": false }, cwd: "/tmp" },
        "/tmp",
      ),
    ).not.toThrow();
  });

  it("still refuses an unknown flag that WOULD reach argv", () => {
    expect(() =>
      resolveArgv(
        { action: "local.up", positionals: [], flags: { "not-a-real-flag": true }, cwd: "/tmp" },
        "/tmp",
      ),
    ).toThrow(/no --not-a-real-flag/);
  });
});

describe("argv resolution refuses what the CLI would reject", () => {
  it("rejects an unknown command", () => {
    expect(() => resolveArgv({ action: "local.nope" }, "/tmp")).toThrow(/not a subcommand/);
  });

  it("rejects a command group", () => {
    expect(() => resolveArgv({ action: "local" }, "/tmp")).toThrow(/command group/);
  });

  it("rejects a flag the command does not have", () => {
    expect(() => resolveArgv({ action: "local.status", flags: { nope: "x" } }, "/tmp")).toThrow(
      /no --nope flag/,
    );
  });

  it("rejects a value outside an enum's declared set", () => {
    expect(() =>
      resolveArgv({ action: "cluster.eval", flags: { concurrency: "1" } }, "/tmp"),
    ).not.toThrow();
    expect(() => resolveArgv({ action: "init", flags: { dir: "x" } }, "/tmp")).not.toThrow();
  });

  it("rejects a missing required positional", () => {
    const withRequired = commands.find((c) => c.positionals.some((p) => p.required));
    expect(withRequired).toBeDefined();
    expect(() => resolveArgv({ action: withRequired!.id, positionals: [] }, "/tmp")).toThrow(
      /is required/,
    );
  });

  it("never emits a value as its own argv token by accident", () => {
    // A path with a space must survive as one token; if it did not, the CLI
    // would see three arguments.
    const argv = resolveArgv(
      { action: "skill.up", flags: { "plugin-dir": "/Users/me/My Agents/deal desk" } },
      "/tmp",
    ).argv;
    expect(argv).toContain("/Users/me/My Agents/deal desk");
  });
});

describe("which directory a command runs in", () => {
  // Getting this wrong is quiet and expensive: the command runs, in the wrong
  // place, and the CLI's complaint is about a missing file rather than about
  // the directory. That is exactly how `curie local up` came to fail from the
  // home directory with `compose.dev.yaml` sitting in the checkout.
  const CTX = { workspace: "/bundles/deal-desk", repoRoot: "/src/curie", fallback: "/Users/dev" };

  it("runs the skill tier in the bundle, because the directory IS the argument", () => {
    for (const id of ["skill.up", "skill.check", "skill.eval"]) {
      expect(cwdFor(commandsById.get(id)!, CTX)).toBe("/bundles/deal-desk");
    }
  });

  it("scaffolds into the bundle directory too", () => {
    expect(cwdFor(commandsById.get("init")!, CTX)).toBe("/bundles/deal-desk");
  });

  it("runs stack and repo work in the checkout, even with a bundle open", () => {
    for (const id of ["local.up", "local.status", "local.rebuild", "dev.contracts"]) {
      expect(cwdFor(commandsById.get(id)!, CTX)).toBe("/src/curie");
    }
  });

  it("falls back through the list rather than returning nothing", () => {
    const noRepo = { workspace: "/bundles/x", repoRoot: null, fallback: "/Users/dev" };
    expect(cwdFor(commandsById.get("local.up")!, noRepo)).toBe("/bundles/x");
    const nothing = { workspace: null, repoRoot: null, fallback: "/Users/dev" };
    expect(cwdFor(commandsById.get("skill.up")!, nothing)).toBe("/Users/dev");
    expect(cwdFor(commandsById.get("local.up")!, {})).toBeUndefined();
  });

  it("names the directory it picked, so the form can say why", () => {
    expect(cwdReason("/bundles/deal-desk", CTX)).toMatch(/bundle/);
    expect(cwdReason("/src/curie", CTX)).toMatch(/checkout/);
    expect(cwdReason("/Users/dev", CTX)).toMatch(/default/);
  });
});

describe("search", () => {
  it("ranks an exact command name above a help-text mention", () => {
    const results = search("up");
    expect(results.slice(0, 4).map((c) => c.name)).toContain("up");
  });

  it("matches a spaced command path the way you would type it", () => {
    expect(search("local deploy")[0]?.id).toBe("local.deploy");
  });

  it("returns nothing for a term in no command", () => {
    expect(search("zzzznotacommand")).toEqual([]);
  });
});

describe("humanArg", () => {
  // The form used to label positionals with the CLI's usage token. `<NAME>`
  // over an empty box is the shape of a thing that is missing, which is the
  // wrong signal above the field you are meant to type into.
  it("turns an argument id into words", () => {
    expect(humanArg("name")).toBe("Name");
    expect(humanArg("run_id")).toBe("Run ID");
  });

  it("is sentence case, not title case", () => {
    // Title Case on a form label is a heading pretending to be one.
    expect(humanArg("object_store_bucket")).toBe("Object store bucket");
  });

  it("keeps acronyms as acronyms", () => {
    // "Api url" is worse than `--api-url`, not better: it reads as a typo, and
    // the whole point of dropping the flag token was to make the label easier
    // to read than the flag.
    expect(humanArg("api_url")).toBe("API URL");
    expect(humanArg("api-key")).toBe("API key");
    expect(humanArg("run_id")).toBe("Run ID");
    expect(humanArg("github-app")).toBe("GitHub app");
    expect(humanArg("plugin-dir")).toBe("Plugin Directory");
  });
});

describe("runtimeDefault", () => {
  const file = { id: "file", long: "file", positional: false, required: false, help: "" };

  it("names the compose file a dev build uses, when there is a checkout", () => {
    // The manifest says `null` for this flag because clap never sees the
    // default -- the CLI resolves it at runtime. Without this the box showed a
    // shape hint and left the answer in a two-line help string.
    // Elided, not absolute: the full path ran to 96 characters in a field that
    // fits about 60, and the directory is not the interesting half -- the sheet
    // already says on its own line where the command runs.
    expect(runtimeDefault(file as never, { repoRoot: "/repo" })).toBe("…/compose.dev.yaml");
  });

  it("names the pinned release file when there is no checkout", () => {
    expect(runtimeDefault(file as never, { repoRoot: null })).toContain("compose.release.yaml");
    expect(runtimeDefault(file as never, null)).toContain("compose.release.yaml");
  });

  it("has nothing to say about other flags", () => {
    const other = { id: "agent", long: "agent", positional: false, required: false, help: "" };
    expect(runtimeDefault(other as never, { repoRoot: "/repo" })).toBeNull();
  });
});
