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

import { useApp } from "../bridge/app";
import { deployedAs } from "../lib/deployment";
import { RunButton } from "./Actions";
import { ACCENT, F, R, STATUS, T } from "../tokens";
import { Button, Group, LiveRing, SectionHeader } from "../primitives";

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
                {live.channel?.kind ? ` · ${live.channel.kind}` : ""}. Sending it again replaces it
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
