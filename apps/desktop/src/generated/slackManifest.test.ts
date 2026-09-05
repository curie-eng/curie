// The Slack manifest the app hands out must be the one the dispatcher expects.
//
// A scope added to `apps/dispatcher/slack-app-manifest.yaml` and not regenerated
// here would have the app installing a Slack app the dispatcher cannot use, and
// it would fail at the API call hours later rather than at install time.

import { describe, expect, it } from "vitest";

import { SLACK_APP_MANIFEST } from "./slackManifest";

// Drift against `apps/dispatcher/slack-app-manifest.yaml` is caught in CI by
// regenerating and diffing, the same way the command manifest is: the source
// lives outside this package and Vite will not resolve an import across that
// boundary. What is asserted here is the part a diff cannot judge -- that the
// generated string is actually usable as a Slack manifest.

describe("the generated Slack app manifest", () => {
  it("still carries what the dispatcher connects with", () => {
    // Socket Mode and app_mention are the two that turn this from a Slack app
    // into a Curie one; without either, an install succeeds and nothing works.
    expect(SLACK_APP_MANIFEST).toMatch(/socket_mode_enabled:\s*true/);
    expect(SLACK_APP_MANIFEST).toMatch(/- app_mention$/m);
    expect(SLACK_APP_MANIFEST).toMatch(/- app_mentions:read$/m);
    expect(SLACK_APP_MANIFEST).toMatch(/- chat:write$/m);
  });

  it("is pasteable as-is, with no leading commentary", () => {
    expect(SLACK_APP_MANIFEST.startsWith("display_information:")).toBe(true);
  });
});
