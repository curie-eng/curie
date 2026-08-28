import { describe, expect, it } from "vitest";

import { shortPath, DASH, ago, bytes, count, duration, percent, stripAnsi, titleize, usd } from "./format";

const ESC = String.fromCharCode(27);
const BEL = String.fromCharCode(7);

describe("unknown values", () => {
  it("render as a dash, never as zero", () => {
    // The whole point: a resource monitor that prints 0% for "could not measure"
    // is lying, and every formatter in this module has to agree about that.
    for (const fn of [bytes, percent, usd, count, duration]) {
      expect(fn(null)).toBe(DASH);
      expect(fn(undefined)).toBe(DASH);
      expect(fn(NaN)).toBe(DASH);
    }
    expect(ago(null)).toBe(DASH);
  });

  it("still renders a real zero as zero", () => {
    expect(bytes(0)).toBe("0 B");
    expect(percent(0)).toBe("0.0%");
    expect(usd(0)).toBe("$0.00");
    expect(count(0)).toBe("0");
  });
});

describe("bytes", () => {
  it("scales in binary units", () => {
    expect(bytes(1024)).toBe("1.0 KB");
    expect(bytes(1024 * 1024 * 1.5)).toBe("1.5 MB");
  });

  it("drops the decimal where it would be noise", () => {
    expect(bytes(512)).toBe("512 B");
    expect(bytes(1024 * 400)).toBe("400 KB");
  });
});

describe("usd", () => {
  it("keeps sub-cent amounts visible instead of rounding them to zero", () => {
    // Per-run agent spend is frequently a fraction of a cent; "$0.00" would make
    // a real cost look free.
    expect(usd(0.0004)).toBe("$0.0004");
    expect(usd(4.2)).toBe("$4.20");
  });
});

describe("duration", () => {
  it("picks a unit a human can read at a glance", () => {
    expect(duration(420)).toBe("420ms");
    expect(duration(4200)).toBe("4.2s");
    expect(duration(95_000)).toBe("1m 35s");
    expect(duration(3_900_000)).toBe("1h 5m");
  });
});

describe("ago", () => {
  it("describes recent times relatively", () => {
    expect(ago(Date.now() - 5_000)).toBe("just now");
    expect(ago(Date.now() - 5 * 60_000)).toBe("5m ago");
    expect(ago(Date.now() - 3 * 3_600_000)).toBe("3h ago");
  });

  it("switches to a date past a week, where 'Nd ago' stops helping", () => {
    const old = Date.now() - 40 * 86_400_000;
    expect(ago(old)).toBe(new Date(old).toLocaleDateString());
  });

  it("accepts an ISO string, which is what the API returns", () => {
    expect(ago(new Date(Date.now() - 120_000).toISOString())).toBe("2m ago");
  });
});

describe("stripAnsi", () => {
  it("removes colour sequences a nested tool emitted despite NO_COLOR", () => {
    expect(stripAnsi(`${ESC}[32mready${ESC}[0m`)).toBe("ready");
  });

  it("removes OSC title sequences", () => {
    expect(stripAnsi(`${ESC}]0;installing${BEL}done`)).toBe("done");
  });

  it("leaves ordinary text alone", () => {
    expect(stripAnsi("curie local up --minimal")).toBe("curie local up --minimal");
  });
});

describe("titleize", () => {
  it("turns a command id into a heading", () => {
    expect(titleize("local.reset-thread")).toBe("Reset thread");
    expect(titleize("up")).toBe("Up");
  });
});

describe("shortPath", () => {
  const home = "/Users/alexrao";

  it("abbreviates the home directory the way every shell does", () => {
    expect(shortPath("/Users/alexrao/code/x.yaml", home)).toBe("~/code/x.yaml");
  });

  it("leaves a path outside home alone", () => {
    expect(shortPath("/etc/hosts", home)).toBe("/etc/hosts");
  });

  it("elides the MIDDLE, keeping the filename and its directory", () => {
    // Truncating the end would drop the filename, which is the one piece
    // nobody can reconstruct from the rest.
    const long =
      "/Users/alexrao/curietech/agentos/.claude/worktrees/commands-tab-ui-ux-5b3aeb/compose.dev.yaml";
    const out = shortPath(long, home);
    expect(out.length).toBeLessThanOrEqual(56);
    expect(out).toContain("…");
    expect(out.endsWith("commands-tab-ui-ux-5b3aeb/compose.dev.yaml")).toBe(true);
    expect(out.startsWith("~")).toBe(true);
  });

  it("spends the space it has rather than keeping a fixed number of segments", () => {
    // A rule that always keeps exactly one parent throws away room on a short
    // path and still overflows on a long one.
    const out = shortPath("/Users/alexrao/a/b/c/d/e/f/g/h/i/file.yaml", home, 56);
    expect(out.length).toBeLessThanOrEqual(56);
    expect(out.endsWith("file.yaml")).toBe(true);
    expect(out.split("/").length).toBeGreaterThan(3);
  });

  it("falls back to the filename when even one parent will not fit", () => {
    // The leading separator survives: "/…/c.yaml" still says the path is
    // absolute, which "…/c.yaml" does not.
    const out = shortPath("/a/" + "b".repeat(60) + "/c.yaml", null, 20);
    expect(out).toBe("/…/c.yaml");
  });

  it("keeps Windows separators", () => {
    expect(shortPath("C:\\src\\curie\\compose.dev.yaml", null, 44)).toBe(
      "C:\\src\\curie\\compose.dev.yaml",
    );
  });
});
