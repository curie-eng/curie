// Whether this agent is actually running anywhere, and the way to make it so.
//
// Build could tell you an agent was "Ready to deploy" and then never mention
// deployment again. The word "ready" is about the FILES -- it means the bundle
// would load -- and it was the only badge on the screen, so an agent that had
// never been deployed looked indistinguishable from one that had. The Canvas
// and Resources views were telling the truth and Build was silent, which reads
// as the two disagreeing.
//
// The app already knows: `app.agents` is what the platform reports it is
// running. Nothing was comparing the two.

import { useState, type CSSProperties } from "react";
import { channelLabel } from "../lib/channels";

import { useApp } from "../bridge/app";
import { bridge } from "../bridge/bridge";
import { SLACK_APP_MANIFEST } from "../generated/slackManifest";
import { deployedAs } from "../lib/deployment";
import { RunButton } from "./Actions";
import { ACCENT, F, R, STATUS, T } from "../tokens";
import { Button, CopyButton, Group, LiveRing, Mono, SectionHeader, Well } from "../primitives";

export function Deployment({ bundleName }: { readonly bundleName: string }) {
  const app = useApp();
  const reachable = !!app.api?.reachable;
  const live = reachable ? deployedAs(app.agents, bundleName) : undefined;

  return (
    <section>
      <SectionHeader>Where this one is running</SectionHeader>
      <Group style={{ padding: "12px 14px", display: "flex", gap: 12, alignItems: "flex-start" }}>
        <span
          style={{
            flex: "none",
            width: 16,
            height: 16,
            marginTop: 2,
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
          }}
        >
          {live ? (
            <LiveRing />
          ) : (
            <span
              aria-hidden
              style={{
                width: 9,
                height: 9,
                borderRadius: 999,
                background: reachable ? T.quaternary : STATUS.warn,
              }}
            />
          )}
        </span>

        <div style={{ flex: 1, minWidth: 0, display: "grid", gap: 3 }}>
          <div style={{ ...F.headline }}>
            {!reachable
              ? "Nowhere yet — Curie is not running"
              : live
                ? "Running now"
                : "Not put to work yet"}
          </div>
          <div style={{ ...F.footnote, color: T.tertiary, lineHeight: 1.55 }}>
            {!reachable ? (
              "Start Curie from the Overview and this will say where this agent is running."
            ) : live ? (
              <>
                Answering as <strong style={{ color: T.secondary }}>{live.name}</strong>
                {live.model ? ` on ${live.model}` : ""}
                {channelLabel(live) ? ` · ${channelLabel(live)}` : ""}. Sending it again replaces it
                with what is on disk now.
              </>
            ) : (
              <>
                Nothing is answering as <strong style={{ color: T.secondary }}>{bundleName}</strong>{" "}
                yet. Sending it makes a version and points a live agent at it — the Canvas and
                Resources views will show it once it is there.
              </>
            )}
          </div>
        </div>

        <span style={{ flex: "none", display: "flex", gap: 7, alignItems: "center" }}>
          {live ? (
            <Button
              size="sm"
              onClick={() => app.navigate("overview", live.name)}
              title="Find it on the Overview, where its settings and controls are"
            >
              Open it
            </Button>
          ) : null}
          {reachable ? (
            <RunButton id="local.deploy" tone={live ? undefined : "primary"}>
              {live ? "Send it again" : "Put it to work"}
            </RunButton>
          ) : null}
        </span>
      </Group>

      {live ? <SlackInstall agentName={live.name} /> : null}
    </section>
  );
}

/** The same fact, small enough for an agent's row in the list. Absent when
 *  nothing is running, because a badge on every row saying "no" is noise on a
 *  list where most rows will say it.
 *
 *  A dot, not the word. It was a `live` pill, which cost about thirty-four
 *  pixels of a column a hundred and sixty-eight wide and took them from the
 *  agent's NAME -- the one thing a row in a switcher exists to show. Presence is
 *  the whole encoding here: there is a mark or there is not, which is a
 *  distinction that survives being four pixels across in a way a word does not,
 *  and the hue is confirmation rather than the signal. The title says it in
 *  words for anyone who points at it, and the pane beside this list spells the
 *  same fact out in full. */
export function DeployedDot({ bundleName }: { readonly bundleName: string }) {
  const app = useApp();
  if (!app.api?.reachable || !deployedAs(app.agents, bundleName)) return null;
  return (
    <span
      title={`${bundleName} is running on the platform`}
      style={{
        flex: "none",
        width: 6,
        height: 6,
        borderRadius: R.pill,
        background: ACCENT,
      }}
    />
  );
}


/**
 * Getting a deployed agent into a Slack workspace.
 *
 * A deployed agent answers nothing until a Slack app exists and the dispatcher
 * holds its two tokens. That was documented in the repo and nowhere in this app,
 * so "Running now" was true and useless -- the operator had finished the part
 * the app could see and had no idea there were four more steps.
 *
 * The manifest is generated from `apps/dispatcher/slack-app-manifest.yaml`, not
 * retyped, so the scopes here cannot drift from the ones the dispatcher actually
 * needs. A missing scope does not fail at install; it fails at an API call much
 * later, which is the worst way to find out.
 *
 * Collapsed by default. It is four steps of setup you do once per workspace, and
 * a wall of YAML standing open under every deployed agent forever would be worse
 * than the omission it fixes.
 */
function SlackInstall({ agentName }: { readonly agentName: string }) {
  const [open, setOpen] = useState(false);
  const step: CSSProperties = { ...F.footnote, color: T.secondary, lineHeight: 1.6 };

  return (
    <Group style={{ marginTop: 10, padding: "11px 14px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ ...F.callout, color: T.primary }}>Answer in Slack</div>
          <div style={{ ...F.footnote, color: T.tertiary, marginTop: 2 }}>
            {agentName} is deployed. To reach it from Slack, your workspace needs an app that
            matches what Curie expects.
          </div>
        </div>
        <Button size="sm" onClick={() => setOpen((v) => !v)} aria-expanded={open}>
          {open ? "Hide" : "Show me how"}
        </Button>
      </div>

      {open ? (
        <div style={{ marginTop: 12, display: "grid", gap: 14 }}>
          <div>
            <div style={{ ...F.callout, color: T.primary, marginBottom: 4 }}>
              1 · Create the app from this
            </div>
            <div style={step}>
              At{" "}
              <a
                href="https://api.slack.com/apps"
                onClick={(e) => {
                  e.preventDefault();
                  void bridge().shell.openExternal("https://api.slack.com/apps");
                }}
                style={{ color: ACCENT }}
              >
                api.slack.com/apps
              </a>
              , choose <strong>Create New App</strong> → <strong>From a manifest</strong>, pick your
              workspace, and paste this in.
            </div>
            <div style={{ display: "flex", justifyContent: "flex-end", margin: "6px 0 4px" }}>
              <CopyButton text={SLACK_APP_MANIFEST} label="Copy the manifest" />
            </div>
            <Well style={{ maxHeight: 190, overflow: "auto", whiteSpace: "pre", fontSize: 11 }}>
              {SLACK_APP_MANIFEST}
            </Well>
          </div>

          <div>
            <div style={{ ...F.callout, color: T.primary, marginBottom: 4 }}>
              2 · Collect two tokens
            </div>
            <div style={step}>
              An <strong>App-Level Token</strong> with the <Mono>connections:write</Mono> scope —
              it starts <Mono>xapp-</Mono>. Then <strong>Install to Workspace</strong> and copy the{" "}
              <strong>Bot User OAuth Token</strong>, which starts <Mono>xoxb-</Mono>.
            </div>
          </div>

          <div>
            <div style={{ ...F.callout, color: T.primary, marginBottom: 4 }}>
              3 · Invite it to a channel
            </div>
            <div style={step}>
              In Slack, invite the bot to the channel it should answer in, then copy that channel&apos;s
              ID. Send {agentName} again with the channel filled in and it will answer there.
            </div>
          </div>

          <div>
            <div style={{ ...F.callout, color: T.primary, marginBottom: 4 }}>
              4 · Hand Curie the tokens
            </div>
            <div style={step}>
              This is the only step that leaves your machine — the tokens go to the running stack,
              which holds the Slack connection open.
            </div>
            <div style={{ marginTop: 7 }}>
              <RunButton id="local.comms" tone="primary">
                Connect Slack
              </RunButton>
            </div>
          </div>
        </div>
      ) : null}
    </Group>
  );
}
