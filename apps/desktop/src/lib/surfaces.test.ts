// Placement tests.
//
// `manifest.test.ts` proves the app can *reach* every command. That was always
// true and was never the complaint: the Commands view is generated from the
// manifest, so it is complete by construction. What it does not prove is that
// any command is reachable the way a person actually works -- from the screen
// they are already on, on a control that says what it does.
//
// These tests make the placement map a contract rather than a comment. A command
// added to the CLI fails here until someone decides where in the app it belongs,
// and a surface cannot claim a command that does not exist.

import { describe, expect, it } from "vitest";

import { ROUTES } from "../bridge/app";
import { commands, commandsById } from "./manifest";
import {
  SURFACES,
  homeOf,
  placedIds,
  placementsOf,
  resolve,
  surfacesById,
  surfacesFor,
} from "./surfaces";

describe("every command has a home", () => {
  it("places every runnable command on at least one surface", () => {
    const unplaced = commands.filter((c) => !placedIds.has(c.id)).map((c) => c.id);
    expect(
      unplaced,
      "these commands are only reachable by searching the Commands list; give each one a surface in src/lib/surfaces.ts",
    ).toEqual([]);
  });

  it("offers no command the CLI does not have", () => {
    const phantom = [...placedIds].filter((id) => !commandsById.has(id));
    expect(phantom, "a surface names a command that is not in the manifest").toEqual([]);
  });

  it("resolves every action to a real command, so no control renders dead", () => {
    for (const surface of SURFACES) {
      expect(resolve(surface).length, `${surface.id} drops actions`).toBe(surface.actions.length);
    }
  });

  it("gives every command exactly one home, and names it", () => {
    for (const cmd of commands) {
      const home = homeOf(cmd.id);
      expect(home, `${cmd.id} has no home`).toBeDefined();
      const first = placementsOf(cmd.id)[0];
      expect(first.surface.id).toBe(home!.surface.id);
      expect(first.action.id).toBe(home!.action.id);
      expect(home!.action.label.length, `${cmd.id} has an empty label`).toBeGreaterThan(0);
    }
  });
});

describe("the surfaces themselves", () => {
  it("has a unique id per surface", () => {
    expect(surfacesById.size).toBe(SURFACES.length);
  });

  it("puts every surface on a route that exists", () => {
    for (const surface of SURFACES) {
      expect(ROUTES, `${surface.id} is on an unknown route`).toContain(surface.route);
    }
  });

  it("never lists the same command twice on one surface", () => {
    for (const surface of SURFACES) {
      const ids = surface.actions.map((a) => a.id);
      expect(new Set(ids).size, `${surface.id} repeats a command`).toBe(ids.length);
    }
  });

  it("writes a label in the operator's words, not the command's", () => {
    // A control reading `local.reset-thread` or "down" is the CLI with a border
    // drawn round it. What is allowed is a plain noun the operator already uses
    // in context -- "Memory" on an agent's own row says everything -- so the
    // check is on the *shape* of the label rather than on whether it happens to
    // share a word with the command: no dotted id, no kebab-case, no bare
    // imperative CLI verb, and a capital at the front like every other label in
    // the app.
    const JARGON = new Set([
      "up",
      "down",
      "init",
      "apply",
      "seal",
      "diff",
      "doctor",
      "build",
      "install",
      "update",
      "eval",
      "eval-init",
      "check",
      "deploy",
      "rebuild",
      "comms",
      "reset-thread",
      "migrate-store",
      "github-app",
      "list-agents",
      "deploy-local",
      "schema-index",
      "interactive",
    ]);
    for (const surface of SURFACES) {
      for (const action of surface.actions) {
        const label = action.label;
        const where = `${surface.id}/${action.id}`;
        expect(label, `${where}: label is the command id`).not.toContain(".");
        expect(label, `${where}: label is kebab-case`).not.toMatch(/^[a-z]+(-[a-z]+)+$/);
        expect(label[0], `${where}: label does not start with a capital`).toBe(
          label[0].toUpperCase(),
        );
        expect(
          JARGON.has(label.toLowerCase()),
          `${where}: "${label}" is the CLI's verb, not the operator's words`,
        ).toBe(false);
      }
    }
  });

  it("gives every surface a blurb saying what the group is for", () => {
    for (const surface of SURFACES) {
      expect(surface.blurb.length, `${surface.id} has no blurb`).toBeGreaterThan(20);
    }
  });

  it("says where on the route the controls actually are", () => {
    // A route name is not directions. Half these groups live inside something
    // you have to open first -- a row, a sheet, a bundle -- and "Overview" does
    // not tell anybody that the agent commands are on a row. So the phrase has
    // to read as a place, which at minimum means it is not just the route name
    // repeated back.
    for (const surface of SURFACES) {
      expect(surface.where.length, `${surface.id} has no directions`).toBeGreaterThan(15);
      expect(surface.where.toLowerCase(), `${surface.id} just repeats its route`).not.toBe(
        surface.route,
      );
    }
  });

  it("marks the destructive commands with a destructive tone", () => {
    // The confirm step is the real guard (CommandForm asks whichever way you
    // arrive), so this is about the control *looking* like what it does before
    // it is pressed.
    for (const surface of SURFACES) {
      for (const action of surface.actions) {
        const cmd = commandsById.get(action.id)!;
        if (cmd.risk !== "destructive") continue;
        expect(action.tone, `${surface.id}/${action.id} is destructive but reads as ordinary`).toBe(
          "danger",
        );
      }
    }
  });

  it("never puts a danger tone on a command that is not destructive", () => {
    for (const surface of SURFACES) {
      for (const action of surface.actions) {
        if (action.tone !== "danger") continue;
        const cmd = commandsById.get(action.id)!;
        expect(cmd.risk, `${surface.id}/${action.id} shouts about a safe command`).not.toBe("safe");
      }
    }
  });
});

describe("the routes that host surfaces", () => {
  it("puts something on each of the routes the map claims", () => {
    const hosting = new Set(SURFACES.map((s) => s.route));
    for (const route of hosting) {
      expect(surfacesFor(route).length, `${route} hosts nothing`).toBeGreaterThan(0);
    }
  });

  it("does not file anything on the Commands route", () => {
    // Commands is the reference, and a reference is where you end up when the
    // map fails you. If a command's home were "the Commands list", the map
    // would be describing the problem rather than fixing it.
    expect(SURFACES.filter((s) => s.route === "commands")).toEqual([]);
  });
});

// --- and that each surface is actually on screen -----------------------------
//
// The tests above prove the map covers the CLI. They cannot prove the map is
// *rendered* -- and a surface nobody renders is exactly the failure this whole
// exercise is about, one level up: instead of a command with no home, a home
// with no door. It happened once already, to `build.author`, and nothing caught
// it but opening the app.
//
// So: every surface id has to appear in the renderer's own source. It is a crude
// check, deliberately -- it cannot tell a rendered group from a mention -- but it
// is the one that fails when a surface is declared and then forgotten, which is
// the mistake that actually gets made.

describe("every surface is rendered somewhere", () => {
  it("names each surface id in a view", () => {
    // `import.meta.glob` rather than reading the tree with node:fs: this file is
    // compiled by the renderer's own tsconfig, which has no Node types, and the
    // bundler resolving the pattern is the same thing that would resolve a real
    // import -- so the corpus is exactly the renderer's source.
    const modules = import.meta.glob("../**/*.{ts,tsx}", {
      eager: true,
      query: "?raw",
      import: "default",
    }) as Record<string, string>;

    // `import.meta.glob` keys are relative to THIS file, so the map next door is
    // `./surfaces.ts` and not `../lib/surfaces.ts`. Matching the wrong one left
    // the map in its own corpus and every surface trivially "found" -- the check
    // passed while proving nothing. Match on the basename.
    const sources = Object.entries(modules)
      .filter(([path]) => {
        const file = path.split("/").pop()!;
        return file !== "surfaces.ts" && !/\.test\.tsx?$/.test(file);
      })
      .map(([, src]) => src);

    // Guard against the glob matching nothing and the assertion passing
    // vacuously against an empty corpus.
    expect(sources.length).toBeGreaterThan(10);

    const orphans = SURFACES.filter((s) => !sources.some((src) => src.includes(`"${s.id}"`))).map(
      (s) => s.id,
    );
    expect(
      orphans,
      "these surfaces are declared but no view mentions them, so their commands have a home on paper only",
    ).toEqual([]);
  });
});
