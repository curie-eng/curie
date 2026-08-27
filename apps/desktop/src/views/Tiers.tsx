// Tiers: the parity ladder as a place, not as a prefix on a command name.
//
// Half this app's commands are `local ...` or `cluster ...`, and until this view
// existed the only thing that said so was the word at the front of a monospace
// string in a list. The ladder is the product's central idea -- the same verbs
// against a bigger deployment each rung up -- so it deserves a screen where each
// rung says what it costs, whether it is running here, and what you can do to it.
//
// Everything on it is rendered from `src/lib/surfaces.ts`. This file supplies
// only what the map cannot know: whether this machine can reach Docker, how many
// containers are actually up, whether the API answers, and the prose that
// explains a rung to somebody meeting it for the first time.

import { useApp } from "../bridge/app";
import { useResources } from "../bridge/resources";
import { surfacesById } from "../lib/surfaces";
import { Actions } from "./Actions";
import { ACCENT, F, HUE, LINE, R, S, STATUS, T, tint } from "../tokens";
import { Badge, Group, Mono, SectionHeader } from "../primitives";

/** What each rung actually is, in one honest sentence about cost and reach. The
 *  manifest describes commands; nothing in it describes a *tier*. */
interface Rung {
  readonly surfaceId: string;
  readonly color: string;
  /** The one-line trade: what you get, and what it costs to get it. */
  readonly costs: string;
  readonly reach: string;
}

const RUNGS: readonly Rung[] = [
  {
    surfaceId: "tiers.skill",
    color: ACCENT,
    costs: "One container. Seconds to start.",
    reach: "This directory only — no agents, no versions, no memory.",
  },
  {
    surfaceId: "tiers.local",
    color: STATUS.info,
    costs: "Eight or so containers. A minute to start.",
    reach: "The whole platform, on this machine, with real agents you can deploy to.",
  },
  {
    surfaceId: "tiers.cluster",
    color: HUE.violet,
    costs: "A Helm release on a real cluster.",
    reach: "The same platform other people can reach.",
  },
];

export function Tiers() {
  const app = useApp();
  const res = useResources();

  const runners = res.samples.filter((s) => s.role === "runner" && s.state === "running").length;
  const stack = res.samples.filter((s) => !!s.service && s.state === "running").length;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18, maxWidth: 860 }}>
      <Explainer />

      {RUNGS.map((rung) => {
        const surface = surfacesById.get(rung.surfaceId)!;
        const live =
          rung.surfaceId === "tiers.skill"
            ? runners
              ? `${runners} runner${runners === 1 ? "" : "s"} up`
              : null
            : rung.surfaceId === "tiers.local"
              ? stack
                ? `${stack} service${stack === 1 ? "" : "s"} up`
                : null
              : app.api?.reachable && !app.api.baseUrl.includes("localhost")
                ? "API answering"
                : null;

        return (
          <Actions
            key={rung.surfaceId}
            surface={surface}
            right={
              live ? (
                <Badge color={rung.color} filled>
                  {live}
                </Badge>
              ) : (
                <span style={{ ...F.footnote, color: T.quaternary }}>nothing running here</span>
              )
            }
          >
            <div
              style={{
                marginTop: 11,
                paddingTop: 10,
                borderTop: `1px solid ${LINE.separator}`,
                display: "grid",
                gridTemplateColumns: "auto 1fr",
                gap: "3px 12px",
                ...F.footnote,
                color: T.quaternary,
              }}
            >
              <span>Costs</span>
              <span style={{ color: T.tertiary }}>{rung.costs}</span>
              <span>Reaches</span>
              <span style={{ color: T.tertiary }}>{rung.reach}</span>
            </div>
          </Actions>
        );
      })}

      <Actions surface={surfacesById.get("tiers.declarative")!}>
        <div style={{ ...F.footnote, color: T.quaternary, marginTop: 10, lineHeight: 1.55 }}>
          A <Mono style={{ fontSize: 10 }}>curie.yaml</Mono> describes the release you want;{" "}
          <Mono style={{ fontSize: 10 }}>apply</Mono> converges the cluster to it. Read the diff
          first — it is the only one of the three that changes nothing.
        </div>
      </Actions>

      <Actions surface={surfacesById.get("tiers.examples")!} />
    </div>
  );
}

/** The one paragraph that makes the three groups below read as one idea rather
 *  than as three unrelated toolbars. */
function Explainer() {
  return (
    <div
      style={{
        background: tint(ACCENT, 0.07),
        border: `1px solid ${LINE.separator}`,
        borderRadius: R.group,
        padding: "12px 14px",
      }}
    >
      <div style={{ ...F.headline, marginBottom: 4 }}>The same agent, three deployments</div>
      <div style={{ ...F.callout, color: T.secondary, lineHeight: 1.6 }}>
        A bundle runs unchanged at every rung. Going up costs more to start and reaches further, and
        the verbs keep their names — <Mono style={{ fontSize: 11 }}>message</Mono>,{" "}
        <Mono style={{ fontSize: 11 }}>eval</Mono>, <Mono style={{ fontSize: 11 }}>deploy</Mono>,{" "}
        <Mono style={{ fontSize: 11 }}>kill</Mono> — so what you learn on one rung is what you know
        on the next.
      </div>
    </div>
  );
}

/** The whole ladder as one compact strip, for the Overview. Not a duplicate of
 *  the view: it says which rung is live and gets you there, and offers no
 *  commands of its own. */
export function LadderStrip() {
  const app = useApp();
  const res = useResources();
  const runners = res.samples.some((s) => s.role === "runner" && s.state === "running");
  const stack = res.samples.some((s) => !!s.service && s.state === "running");
  const cluster = !!app.api?.reachable && !app.api.baseUrl.includes("localhost");

  const rungs: [string, boolean, string][] = [
    ["Skill", runners, ACCENT],
    ["Local", stack, STATUS.info],
    ["Cluster", cluster, HUE.violet],
  ];

  return (
    <section>
      <SectionHeader>Where things are running</SectionHeader>
      {/* Slim: three pills and a link do not need a card's worth of padding,
          and the band of empty space between them was reading as a mistake. */}
      <Group style={{ padding: "7px 12px" }}>
        <button
          onClick={() => app.navigate("tiers")}
          title="Open the tiers view"
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            width: "100%",
            border: "none",
            background: "transparent",
            padding: 0,
            cursor: "default",
            color: "inherit",
          }}
        >
          {rungs.map(([label, on, color], i) => (
            <span key={label} style={{ display: "inline-flex", alignItems: "center", gap: 10 }}>
              {i > 0 ? <span style={{ color: T.quaternary, fontSize: 10 }}>›</span> : null}
              <span
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  padding: "3px 9px",
                  borderRadius: R.pill,
                  // `S.control` and not `S.well`: a chip sits ON a card, so it
                  // wants the standard control fill, which is a step lighter
                  // than the surface behind it. `well` is the RECESSED surface --
                  // the darkest thing in the dark palette -- and putting the
                  // dimmest ink on it made an idle tier genuinely unreadable.
                  background: on ? tint(color, 0.16) : S.control,
                  ...F.caption,
                  // Idle is a real state, not a disabled control. `quaternary`
                  // is the placeholder level and says "you cannot use this".
                  color: on ? T.primary : T.secondary,
                }}
              >
                {label}
                <span style={{ ...F.footnote, color: on ? color : T.tertiary }}>
                  {on ? "live" : "idle"}
                </span>
              </span>
            </span>
          ))}
          <span style={{ flex: 1 }} />
          <span style={{ ...F.footnote, color: T.tertiary }}>Open tiers ›</span>
        </button>
      </Group>
    </section>
  );
}
